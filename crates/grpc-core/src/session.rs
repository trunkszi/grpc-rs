//! Synchronous HTTP/2 gRPC sessions with bounded per-stream receive and send queues.
//!
//! Receive budgets cover metadata, message descriptors and reserved partial envelopes.
//! Native HTTP/2 storage, temporary copies and queued IncomingRequest metadata are separate.
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    io,
    marker::PhantomData,
    rc::Rc,
    time::Duration,
};

use nghttp2::{
    DataPoll, DataProvider, DataSource, HeaderField, SessionEvent, SessionOptions, SettingsEntry,
    types::{FrameType, SettingsId},
};

use crate::{
    compression::{CompressionAlgorithm, CompressionConfig},
    framing::{AlignedMessageBuf, LengthPrefixedFrame},
    metadata::{Metadata, format_grpc_timeout, parse_grpc_timeout},
    slot_map::{EndpointRole, Headers, SlotMapError, StreamLimits, StreamMessage, StreamSlotMap},
    status::{Code, Status, decode_grpc_message},
    transport::SyncTransport,
};

/// Relative request deadline represented by the grpc-timeout header.
pub type Timeout = Duration;
/// Client role marker.
pub enum Client {}
/// Server role marker.
pub enum Server {}
/// Client RPC engine.
pub type GrpcClientSession = GrpcSession<Client>;
/// Server RPC engine.
pub type GrpcServerSession = GrpcSession<Server>;
const ACCEPT: &str = "zstd,lz4,gzip,deflate,snappy,identity";
/// HTTP/2 settings and application-owned stream limits.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub max_concurrent_streams: u32,
    pub initial_window_size: u32,
    pub max_frame_size: u32,
    pub max_header_list_size: u32,
    pub max_inbound_message_size: usize,
    pub per_stream_byte_budget: usize,
    pub max_active_streams: usize,
    pub compression: CompressionConfig,
    pub user_agent: String,
    pub content_type: String,
    pub scheme: String,
}
#[rustfmt::skip]
impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100, initial_window_size: 1024 * 1024,
            max_frame_size: 16_384, max_header_list_size: 64 * 1024,
            max_inbound_message_size: 16 * 1024 * 1024, per_stream_byte_budget: 16 * 1024 * 1024,
            max_active_streams: 1024, compression: CompressionConfig::default(),
            user_agent: "grpc-rs/0.1.0".into(), scheme: "http".into(), content_type: "application/grpc".into(),
        }
    }
}
/// A validated request header block; message bodies are consumed separately.
#[derive(Debug)]
pub struct IncomingRequest {
    pub stream_id: i32,
    pub path: String,
    pub content_type: String,
    pub authority: String,
    pub timeout: Option<Timeout>,
    pub request_encoding: CompressionAlgorithm,
    pub accept_encoding: String,
    pub initial_metadata: Metadata,
}
#[derive(Default)]
struct Outbound {
    bytes: VecDeque<u8>,
    eof: bool,
    deferred: bool,
    finished: bool,
    trailers: bool,
}
struct Source {
    queue: Rc<RefCell<Outbound>>,
    chunk: Vec<u8>,
}
#[rustfmt::skip]
impl DataSource for Source {
    fn read(&mut self, max_len: usize) -> DataPoll<'_> {
        let mut queue = self.queue.borrow_mut();
        self.chunk.clear();
        if !queue.bytes.is_empty() {
            let count = max_len.min(queue.bytes.len());
            self.chunk.extend(queue.bytes.drain(..count));
            return DataPoll::Copy(&self.chunk);
        }
        if queue.eof {
            queue.finished = true;
            if queue.trailers { DataPoll::EofWithTrailers } else { DataPoll::Eof }
        } else {
            queue.deferred = true;
            DataPoll::Deferred
        }
    }
}
#[rustfmt::skip]
struct Stream {
    decoder: LengthPrefixedFrame,
    encoding: CompressionAlgorithm,
    outgoing_encoding: CompressionAlgorithm,
    outgoing: Rc<RefCell<Outbound>>,
    headers: Headers, header_bytes: usize,
    initial: bool, sent_initial: bool, terminal: bool, closed: bool, end_stream_serialized: bool,
    failure: Option<Status>, failure_taken: bool, http_status: u16,
    pending_trailers: Option<Vec<(String, String)>>,
    prefix: Vec<u8>, remaining: usize, reserved: usize,
}
enum Native {
    Client(nghttp2::ClientSession),
    Server(nghttp2::ServerSession),
}
macro_rules! native { ($self:expr, $method:ident($($arg:expr),*)) => { match &mut $self.native { Native::Client(s) => s.$method($($arg),*), Native::Server(s) => s.$method($($arg),*) } }; }
/// Single-threaded session. Closed slots remain readable until explicitly removed.
#[rustfmt::skip]
pub struct GrpcSession<Role> {
    native: Native, config: SessionConfig, slots: StreamSlotMap,
    streams: BTreeMap<i32, Stream>,
    incoming: VecDeque<IncomingRequest>,
    wire: Vec<u8>, transport_pending: Vec<u8>, transport_offset: usize, transport_flushed: bool,
    failed: Option<Status>, draining: bool, role: PhantomData<Role>,
}
#[rustfmt::skip]
fn internal(error: impl std::fmt::Display) -> Status { Status::internal(error.to_string()) }
#[rustfmt::skip]
fn resource() -> Status { Status::resource_exhausted("per-stream byte budget exceeded") }
#[rustfmt::skip]
fn slot_error(error: SlotMapError) -> Status {
    match error { SlotMapError::MemoryLimit { .. } | SlotMapError::MessageLimit { .. } => Status::resource_exhausted(error.to_string()), _ => internal(error) }
}
#[rustfmt::skip]
fn fields(headers: &[(String, String)]) -> Vec<HeaderField<'_>> {
    headers.iter().map(|(k, v)| HeaderField::new(k.as_bytes(), v.as_bytes())).collect()
}
#[rustfmt::skip]
fn metadata(headers: &Headers) -> Result<Metadata, Status> {
    let mut result = Metadata::new();
    for (key, value) in headers {
        if key.starts_with(':') || matches!(key.as_str(), "content-type" | "te" | "user-agent") || key.starts_with("grpc-") {
            continue;
        }
        result.append_header(key, std::str::from_utf8(value).map_err(internal)?).map_err(internal)?;
    }
    Ok(result)
}
#[rustfmt::skip]
fn validate_content_type(value: &str) -> Result<(), Status> {
    let media = value.split(';').next().unwrap_or("");
    let suffix = media.strip_prefix("application/grpc+");
    if !value.bytes().all(|b| (32..127).contains(&b)) || !(media == "application/grpc" || suffix.is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)))) { return Err(Status::invalid_argument("invalid gRPC content-type")); }
    Ok(())
}
#[rustfmt::skip]
fn validate_metadata(metadata: &Metadata) -> Result<(), Status> {
    if metadata.iter().any(|(key, _)| matches!(key, "content-type" | "te" | "user-agent")) { return Err(Status::invalid_argument("application metadata contains protocol header")); }
    Ok(())
}
#[rustfmt::skip]
fn value<'a>(headers: &'a Headers, key: &str) -> Result<Option<&'a str>, Status> {
    let mut values = headers.iter().filter(|(k, _)| k == key);
    let result = values.next().map(|(_, v)| std::str::from_utf8(v).map_err(internal)).transpose()?;
    if values.next().is_some() { return Err(Status::internal(format!("duplicate {key}"))); }
    Ok(result)
}
#[rustfmt::skip]
macro_rules! constructor {
    ($role:ty, $variant:ident, $native:ty, $endpoint:expr) => {
        impl GrpcSession<$role> {
            /// Creates an engine and queues its initial HTTP/2 SETTINGS.
            /// # Errors
            /// Rejects invalid settings and native initialization failure.
            pub fn new(config: SessionConfig) -> Result<Self, Status> {
                if config.max_active_streams == 0
                    || config.initial_window_size > i32::MAX as u32
                    || !(16_384..=16_777_215).contains(&config.max_frame_size)
                    || !matches!(config.scheme.as_str(), "http" | "https")
                {
                    return Err(Status::invalid_argument("invalid HTTP/2 session configuration"));
                }
                validate_content_type(&config.content_type)?;
                let mut options = SessionOptions::new().map_err(internal)?;
                options.set_no_auto_window_update(true).set_peer_max_concurrent_streams(config.max_concurrent_streams).set_max_send_header_block_length(config.max_header_list_size as usize);
                let mut native = <$native>::new_default(Some(&options), None).map_err(internal)?;
                native
                    .submit_settings(&[
                        SettingsEntry::new(SettingsId::MAX_CONCURRENT_STREAMS, config.max_concurrent_streams),
                        SettingsEntry::new(SettingsId::INITIAL_WINDOW_SIZE, config.initial_window_size),
                        SettingsEntry::new(SettingsId::MAX_FRAME_SIZE, config.max_frame_size),
                        SettingsEntry::new(SettingsId::MAX_HEADER_LIST_SIZE, config.max_header_list_size),
                    ])
                    .map_err(internal)?;
                let slots = StreamSlotMap::with_role(StreamLimits { max_buffered_bytes: config.per_stream_byte_budget, max_queued_messages: 1024 }, $endpoint);
                Ok(Self {
                    native: Native::$variant(native), config, slots,
                    streams: BTreeMap::new(), incoming: VecDeque::new(),
                    wire: Vec::new(), transport_pending: Vec::new(),
                    transport_offset: 0, transport_flushed: true, failed: None, draining: false, role: PhantomData,
                })
            }
        }
    };
}
constructor!(Client, Client, nghttp2::ClientSession, EndpointRole::Client);
constructor!(Server, Server, nghttp2::ServerSession, EndpointRole::Server);
#[rustfmt::skip]
impl GrpcClientSession {
    /// Starts a POST request; false END_STREAM enables incremental message sending.
    /// # Errors
    /// Rejects stream limits, invalid metadata/timeout or native submission failure.
    pub fn start_request(&mut self, authority: &str, path: &str, metadata: &Metadata, timeout: Option<Timeout>, encoding: CompressionAlgorithm, end_stream: bool) -> Result<i32, Status> {
        let content_type = self.config.content_type.clone();
        self.start_request_with_content_type(authority, path, metadata, timeout, encoding, end_stream, &content_type)
    }
    /// Starts a request with a codec media type independently of other RPCs.
    /// # Errors
    /// Rejects invalid media types, metadata, stream limits or native submission failure.
    #[expect(clippy::too_many_arguments, reason = "The request carries routing, metadata, deadline, encoding and codec identity.")]
    pub fn start_request_with_content_type(&mut self, authority: &str, path: &str, metadata: &Metadata, timeout: Option<Timeout>, encoding: CompressionAlgorithm, end_stream: bool, content_type: &str) -> Result<i32, Status> {
        validate_content_type(content_type)?;
        self.check_capacity()?;
        if authority.is_empty() || !path.starts_with('/') { return Err(Status::invalid_argument("invalid RPC authority or path")); }
        let mut headers = vec![
            (":method".into(), "POST".into()),
            (":scheme".into(), self.config.scheme.clone()),
            (":authority".into(), authority.into()),
            (":path".into(), path.into()),
            ("te".into(), "trailers".into()),
            ("content-type".into(), content_type.into()),
            ("user-agent".into(), self.config.user_agent.clone()),
            ("grpc-accept-encoding".into(), ACCEPT.into()),
        ];
        if encoding != CompressionAlgorithm::Identity {
            headers.push(("grpc-encoding".into(), encoding.as_str().into()));
        }
        if let Some(timeout) = timeout {
            headers.push(("grpc-timeout".into(), format_grpc_timeout(timeout).map_err(internal)?));
        }
        validate_metadata(metadata)?;
        headers.extend(metadata.to_headers());
        self.check_headers(&headers)?;
        let queue = Rc::new(RefCell::new(Outbound::default()));
        let provider = (!end_stream).then(|| DataProvider::new(Source { queue: Rc::clone(&queue), chunk: Vec::new() }));
        let id = native!(self, submit_request(&fields(&headers), provider)).map_err(internal)?;
        self.open(id, queue, encoding)?;
        if end_stream {
            self.slots.mark_local_end_stream(id).map_err(slot_error)?;
            self.stream_mut(id)?.outgoing.borrow_mut().eof = true;
        }
        Ok(id)
    }
    /// Ends the request body after all queued messages.
    /// # Errors
    /// Rejects unknown, reset or already half-closed streams.
    pub fn half_close(&mut self, id: i32) -> Result<(), Status> {
        self.check_send(id)?;
        self.stream_mut(id)?.outgoing.borrow_mut().eof = true;
        self.slots.mark_local_end_stream(id).map_err(slot_error)?;
        self.resume(id)
    }
}
#[rustfmt::skip]
impl GrpcServerSession {
    /// Transfers the next received request's routing information.
    pub fn pop_incoming_request(&mut self) -> Option<IncomingRequest> { self.incoming.pop_front() }
    /// Sends response headers and installs an incremental body source.
    /// # Errors
    /// Rejects duplicate headers, ended streams and native submission failure.
    pub fn send_initial_metadata(&mut self, id: i32, metadata: &Metadata, encoding: CompressionAlgorithm) -> Result<(), Status> { self.send_initial_metadata_with_content_type(id, metadata, encoding, &self.config.content_type.clone()) }
    /// Starts a response using the codec selected for this RPC's route.
    /// # Errors
    /// Rejects invalid media types, duplicate headers, ended streams or native failure.
    pub fn send_initial_metadata_with_content_type(&mut self, id: i32, metadata: &Metadata, encoding: CompressionAlgorithm, content_type: &str) -> Result<(), Status> { self.begin_response(id, metadata, encoding, content_type) }
    /// Completes a response, emitting trailers only when its queued DATA has drained.
    /// # Errors
    /// Rejects duplicate completion, invalid streams, budgets or native failure.
    pub fn finish_response(&mut self, id: i32, status: &Status, metadata: &Metadata) -> Result<(), Status> {
        self.check_send(id)?;
        let mut headers = vec![("grpc-status".into(), status.code().as_i32().to_string())];
        if !status.message().is_empty() {
            headers.push(("grpc-message".into(), status.encoded_message()));
        }
        validate_metadata(metadata)?;
        headers.extend(metadata.to_headers());
        if !self.stream_mut(id)?.sent_initial {
            headers.insert(0, ("content-type".into(), self.config.content_type.clone()));
            headers.insert(0, (":status".into(), "200".into()));
            self.check_headers(&headers)?;
            native!(self, submit_response(id, &fields(&headers), None)).map_err(internal)?;
        } else {
            self.check_headers(&headers)?;
            let stream = self.stream_mut(id)?;
            let mut queue = stream.outgoing.borrow_mut();
            queue.eof = true;
            queue.trailers = true;
            stream.pending_trailers = Some(headers);
            drop(queue);
            self.resume(id)?;
        }
        self.slots.mark_local_end_stream(id).map_err(slot_error)
    }
}
#[rustfmt::skip]
impl<Role> GrpcSession<Role> {
    fn begin_response(&mut self, id: i32, metadata: &Metadata, encoding: CompressionAlgorithm, content_type: &str) -> Result<(), Status> {
        validate_content_type(content_type)?;
        self.check_send(id)?;
        if self.stream_mut(id)?.sent_initial { return Err(Status::failed_precondition("response headers already sent")); }
        let mut headers = vec![(":status".into(), "200".into()), ("content-type".into(), content_type.into()), ("grpc-accept-encoding".into(), ACCEPT.into())];
        if encoding != CompressionAlgorithm::Identity {
            headers.push(("grpc-encoding".into(), encoding.as_str().into()));
        }
        validate_metadata(metadata)?;
        headers.extend(metadata.to_headers());
        self.check_headers(&headers)?;
        let queue = Rc::clone(&self.stream_mut(id)?.outgoing);
        native!(self, submit_response(id, &fields(&headers), Some(DataProvider::new(Source { queue, chunk: Vec::new() })))).map_err(internal)?;
        let stream = self.stream_mut(id)?;
        stream.sent_initial = true;
        stream.outgoing_encoding = encoding;
        Ok(())
    }

    /// Queues GOAWAY at the last processed peer stream; existing RPCs may finish.
    /// # Errors
    /// Returns native submission or a previous connection failure.
    pub fn submit_goaway(&mut self, error_code: u32) -> Result<(), Status> {
        self.check_live()?;
        let last = native!(self, last_proc_stream_id());
        native!(self, submit_goaway(last, error_code, &[])).map_err(internal)?;
        self.draining = true; Ok(())
    }
    /// Whether the native connection can accept additional inbound bytes.
    pub fn want_read(&self) -> bool { self.failed.is_none() && match &self.native { Native::Client(s) => s.want_read(), Native::Server(s) => s.want_read() } }
    /// Whether native frames or partially written transport bytes need sending.
    pub fn want_write(&self) -> bool { !self.transport_flushed || self.transport_offset < self.transport_pending.len() || self.streams.values().any(|s| s.pending_trailers.is_some() && s.outgoing.borrow().finished) || match &self.native { Native::Client(s) => s.want_write(), Native::Server(s) => s.want_write() } }
    /// The most restrictive peer GOAWAY boundary and its HTTP/2 error code.
    pub fn goaway_received(&self) -> Option<(i32, u32)> { self.slots.goaway() }
    /// Whether terminal output was serialized and tracked transport writes/flushes drained.
    pub fn stream_outbound_complete(&self, id: i32) -> bool { self.transport_flushed && self.transport_offset == self.transport_pending.len() && self.streams.get(&id).is_some_and(|s| s.closed || s.end_stream_serialized) }
    /// Whether local or peer GOAWAY has stopped admission of new RPCs.
    pub fn is_draining(&self) -> bool { self.draining || self.slots.goaway().is_some() }
    fn client(&self) -> bool { matches!(self.native, Native::Client(_)) }
    fn check_live(&self) -> Result<(), Status> { self.failed.clone().map_or(Ok(()), Err) }
    fn check_capacity(&self) -> Result<(), Status> {
        self.check_live()?;
        if self.is_draining() { return Err(Status::unavailable("connection is draining")); }
        if self.streams.len() >= self.config.max_active_streams { return Err(Status::resource_exhausted("active stream limit exceeded")); }
        Ok(())
    }
    fn check_headers(&self, headers: &[(String, String)]) -> Result<(), Status> {
        let bytes = headers.iter().try_fold(0usize, |n, (k, v)| n.checked_add(k.len() + v.len() + 32)).ok_or_else(resource)?;
        if bytes > self.config.max_header_list_size as usize || bytes > self.config.per_stream_byte_budget { return Err(resource()); }
        Ok(())
    }
    fn stream_mut(&mut self, id: i32) -> Result<&mut Stream, Status> { self.streams.get_mut(&id).ok_or_else(|| Status::not_found("unknown stream")) }
    fn open(&mut self, id: i32, queue: Rc<RefCell<Outbound>>, encoding: CompressionAlgorithm) -> Result<(), Status> {
        self.check_capacity()?;
        self.slots.open_stream(id).map_err(slot_error)?;
        self.streams.insert(
            id,
            Stream {
                decoder: LengthPrefixedFrame::new(self.config.max_inbound_message_size.min(self.config.per_stream_byte_budget)),
                encoding: CompressionAlgorithm::Identity,
                outgoing_encoding: encoding,
                outgoing: queue,
                headers: vec![], header_bytes: 0, initial: false, sent_initial: false, terminal: false, closed: false, end_stream_serialized: false,
                failure: None, failure_taken: false, http_status: 200, pending_trailers: None,
                prefix: vec![], remaining: 0, reserved: 0,
            },
        );
        Ok(())
    }
    fn check_send(&self, id: i32) -> Result<(), Status> {
        self.check_live()?;
        let stream = self.streams.get(&id).ok_or_else(|| Status::not_found("unknown stream"))?;
        if let Some(error) = &stream.failure { return Err(error.clone()); }
        let slot = self.slots.get(id).ok_or_else(|| Status::not_found("unknown stream"))?;
        if slot.local_end_stream() { return Err(Status::failed_precondition("sending half already closed")); }
        Ok(())
    }
    fn resume(&mut self, id: i32) -> Result<(), Status> {
        let deferred = self.stream_mut(id)?.outgoing.borrow().deferred;
        if deferred {
            native!(self, resume_data(id)).map_err(internal)?;
            self.stream_mut(id)?.outgoing.borrow_mut().deferred = false;
        }
        Ok(())
    }
    /// Queues one framed message, preserving the stream's negotiated encoding.
    /// # Errors
    /// Rejects ended streams, mismatched encodings, compression failure or byte excess.
    pub fn send_message(&mut self, id: i32, payload: &[u8], encoding: CompressionAlgorithm) -> Result<(), Status> {
        self.check_send(id)?;
        if !self.client() && !self.stream_mut(id)?.sent_initial {
            self.begin_response(id, &Metadata::new(), encoding, &self.config.content_type.clone())?;
        }

        if encoding != CompressionAlgorithm::Identity && encoding != self.stream_mut(id)?.outgoing_encoding { return Err(Status::invalid_argument("message encoding differs from stream encoding")); }
        if payload.len() > self.config.per_stream_byte_budget { return Err(resource()); }
        let compressed = CompressionConfig { algorithm: encoding, ..self.config.compression }.compress(payload)?;
        let frame = AlignedMessageBuf::new(&compressed.data, compressed.compressed)?;
        let budget = self.config.per_stream_byte_budget;
        let stream = self.stream_mut(id)?;
        let mut queue = stream.outgoing.borrow_mut();
        if queue.bytes.len().checked_add(frame.wire_bytes().len()).is_none_or(|n| n > budget) { return Err(resource()); }
        queue.bytes.extend(frame.wire_bytes());
        drop(queue);
        self.resume(id)
    }
    /// Resets a stream and releases its queued outbound body.
    /// Completed NO_ERROR peer closure needs no additional local upload reset.
    /// # Errors
    /// Rejects unknown streams or native reset submission failure.
    pub fn cancel_stream(&mut self, id: i32, error_code: u32) -> Result<(), Status> {
        self.check_read(id)?;
        if error_code == 0 && self.streams.get(&id).is_some_and(|s| s.closed && s.terminal) { return Ok(()); }
        native!(self, submit_rst_stream(id, error_code)).map_err(internal)?;
        self.fail_stream(id, Status::cancelled("stream cancelled"), error_code)
    }
    fn fail_stream(&mut self, id: i32, error: Status, code: u32) -> Result<(), Status> {
        let stream = self.stream_mut(id)?;
        if stream.failure.is_some() { return Ok(()); }
        stream.failure = Some(error);
        stream.decoder.reset();
        stream.reserved = 0;
        stream.prefix.clear();
        stream.headers.clear();
        stream.pending_trailers = None;
        stream.outgoing.borrow_mut().bytes.clear();
        self.incoming.retain(|r| r.stream_id != id);
        self.slots.record_rst_stream(id, code).map_err(slot_error)
    }
    /// Feeds received HTTP/2 bytes and dispatches every resulting callback event.
    /// # Errors
    /// Returns native connection errors; malformed RPC streams are reset individually.
    pub fn mem_recv(&mut self, bytes: &[u8]) -> Result<usize, Status> {
        self.check_live()?;
        let mut count = 0;
        for chunk in bytes.chunks(16 * 1024) {
            let n = native!(self, mem_recv(chunk)).map_err(internal)?;
            count += n;
            self.events()?;
            if n < chunk.len() {
                break;
            }
        }
        Ok(count)
    }
    /// Produces the next HTTP/2 wire chunk, valid until the next mutable session call.
    /// # Errors
    /// Returns native connection or callback-processing errors.
    pub fn mem_send(&mut self) -> Result<&[u8], Status> {
        if self.transport_offset < self.transport_pending.len() { return Err(Status::failed_precondition("transport output must drain before mem_send")); }
        self.check_live()?;
        self.submit_ready_trailers()?;
        self.wire = native!(self, mem_send()).map_err(internal)?.to_vec();
        self.events()?;
        if self.wire.is_empty() {
            self.submit_ready_trailers()?;
            self.wire = native!(self, mem_send()).map_err(internal)?.to_vec();
            self.events()?;
        }
        Ok(&self.wire)
    }
    fn submit_ready_trailers(&mut self) -> Result<(), Status> {
        let ready: Vec<_> = self.streams.iter().filter(|(_, s)| s.pending_trailers.is_some() && s.outgoing.borrow().finished).map(|(id, _)| *id).collect();
        for id in ready {
            if let Some(headers) = self.stream_mut(id)?.pending_trailers.take() {
                native!(self, submit_trailer(id, &fields(&headers))).map_err(internal)?;
            }
        }
        Ok(())
    }
    /// Flushes wire output, retaining partially written chunks across WouldBlock.
    /// # Errors
    /// Returns transport or session failures; WouldBlock means retry later.
    pub fn flush_to_transport<T: SyncTransport>(&mut self, transport: &mut T) -> Result<(), Status> {
        loop {
            if self.transport_offset == self.transport_pending.len() {
                self.transport_pending = self.mem_send()?.to_vec();
                self.transport_offset = 0;
                if !self.transport_pending.is_empty() { self.transport_flushed = false; }
                if self.transport_pending.is_empty() {
                    break;
                }
            }
            match transport.write(&self.transport_pending[self.transport_offset..]) {
                Ok(0) => return Err(Status::unavailable("transport write returned zero")),
                Ok(n) => self.transport_offset += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(Status::unavailable(e.to_string())),
            }
        }
        match transport.flush() {
            Ok(()) => { self.transport_flushed = true; Ok(()) },
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => { self.transport_flushed = false; Ok(()) },
            Err(e) => Err(Status::unavailable(e.to_string())),
        }
    }
    /// Flushes outgoing data, reads once and routes inbound bytes.
    /// # Errors
    /// Returns transport EOF/errors or session errors. WouldBlock returns zero.
    pub fn step_transport<T: SyncTransport>(&mut self, transport: &mut T, buf: &mut [u8]) -> Result<usize, Status> {
        if buf.is_empty() { return Err(Status::invalid_argument("receive buffer is empty")); }
        self.flush_to_transport(transport)?;
        let n = loop {
            match transport.read(buf) {
                Ok(0) => return Err(Status::unavailable("transport EOF")),
                Ok(n) => break n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(0),
                Err(e) => return Err(Status::unavailable(e.to_string())),
            }
        };
        let mut used = 0;
        while used < n {
            let count = self.mem_recv(&buf[used..n])?;
            if count == 0 { return Err(Status::internal("HTTP/2 receive made no progress")); }
            used += count;
        }
        self.flush_to_transport(transport)?;
        Ok(n)
    }
    /// Transfers custom initial metadata, preserving stream lifecycle history.
    /// # Errors
    /// Returns unknown/reset stream or metadata decoding errors.
    pub fn pop_initial_metadata(&mut self, id: i32) -> Result<Option<Metadata>, Status> {
        self.check_read(id)?;
        self.slots.pop_initial_metadata(id).map_err(slot_error)?.map(|h| metadata(&h)).transpose()
    }
    /// Transfers one decompressed message into eight-byte-aligned storage.
    /// # Errors
    /// Returns unknown/reset stream or allocation errors.
    pub fn pop_message(&mut self, id: i32) -> Result<Option<AlignedMessageBuf>, Status> {
        self.check_read(id)?;
        self.slots.pop_message(id).map_err(slot_error)?.map(|m| AlignedMessageBuf::new(&m.payload, false)).transpose()
    }
    /// Whether the peer's request or response half has ended.
    /// # Errors
    /// Returns unknown/reset stream errors.
    pub fn is_remote_end_stream(&self, id: i32) -> Result<bool, Status> {
        self.check_read(id)?;
        Ok(self.slots.get(id).is_some_and(|s| s.remote_end_stream()))
    }
    fn check_read(&self, id: i32) -> Result<(), Status> {
        let s = self.streams.get(&id).ok_or_else(|| Status::not_found("unknown stream"))?;
        s.failure.clone().map_or(Ok(()), Err)
    }
    /// Takes terminal status and custom trailing metadata exactly once.
    /// # Errors
    /// Returns unknown/reset stream or invalid metadata errors.
    pub fn take_trailers(&mut self, id: i32) -> Result<Option<(Status, Metadata)>, Status> {
        if let Some(error) = self.stream_mut(id)?.failure.clone() {
            if self.stream_mut(id)?.failure_taken { return Ok(None); }
            self.stream_mut(id)?.failure_taken = true;
            return Ok(Some((error, Metadata::new())));
        }
        self.slots
            .take_trailers(id)
            .map_err(slot_error)?
            .map(|h| {
                let code = value(&h, "grpc-status")?.and_then(|v| v.parse::<i32>().ok()).map_or(Code::Unknown, Code::from_i32);
                let message = decode_grpc_message(value(&h, "grpc-message")?.unwrap_or(""));
                Ok((Status::new(code, message), metadata(&h)?))
            })
            .transpose()
    }
    /// Releases a closed stream and all unread events.
    /// # Errors
    /// Rejects unknown or still-open streams.
    pub fn remove_closed_stream(&mut self, id: i32) -> Result<(), Status> {
        if !self.stream_mut(id)?.closed { return Err(Status::failed_precondition("native stream is not closed")); }
        if self.slots.get(id).is_some() {
            self.slots.remove_closed_stream(id).map_err(slot_error)?;
        }
        self.streams.remove(&id);
        self.incoming.retain(|r| r.stream_id != id);
        Ok(())
    }
    fn events(&mut self) -> Result<(), Status> {
        while let Some(event) = native!(self, next_event()) {
            let id = match &event {
                SessionEvent::BeginHeaders { stream_id, .. } | SessionEvent::Header { stream_id, .. } | SessionEvent::DataChunk { stream_id, .. } | SessionEvent::FrameRecv { stream_id, .. } | SessionEvent::FrameNotSend { stream_id, .. } => {
                    Some(*stream_id)
                }
                _ => None,
            };
            if id.is_some_and(|id| self.streams.get(&id).is_some_and(|s| s.failure.is_some())) {
                continue;
            }
            if let Err(error) = self.event(event) {
                if let Some(id) = id.filter(|id| *id > 0) {
                    native!(self, submit_rst_stream(id, 2)).map_err(internal)?;
                    if self.streams.contains_key(&id) {
                        self.fail_stream(id, error, 2)?;
                    }
                } else {
                    self.failed = Some(error.clone());
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    fn event(&mut self, event: SessionEvent) -> Result<(), Status> {
        match event {
            SessionEvent::FrameSend { stream_id, frame_type, flags } if flags & 1 != 0 && matches!(frame_type, FrameType::HEADERS | FrameType::DATA) => {
                if let Some(stream) = self.streams.get_mut(&stream_id) { stream.end_stream_serialized = true; }
            }
            SessionEvent::BeginHeaders { stream_id: id, .. } => {
                if !self.streams.contains_key(&id) {
                    if self.client() { return Err(Status::internal("unsolicited server stream")); }
                    self.open(id, Rc::new(RefCell::new(Outbound::default())), CompressionAlgorithm::Identity)?;
                }
                let stream = self.stream_mut(id)?;
                stream.headers.clear();
                stream.header_bytes = 0;
            }
            SessionEvent::Header { stream_id: id, name, value, .. } => {
                let budget = self.config.per_stream_byte_budget;
                let max = self.config.max_header_list_size as usize;
                let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
                let stream = self.stream_mut(id)?;
                let size = name.len().checked_add(value.len()).and_then(|n| n.checked_add(32)).ok_or_else(resource)?;
                let total = stream.header_bytes.checked_add(size).ok_or_else(resource)?;
                if total > max || retained.checked_add(total).and_then(|n| n.checked_add(stream.reserved)).is_none_or(|n| n > budget) { return Err(resource()); }
                stream.header_bytes = total;
                stream.headers.push((String::from_utf8(name).map_err(internal)?, value));
            }
            SessionEvent::DataChunk { stream_id: id, data, .. } => {
                native!(self, consume(id, data.len())).map_err(internal)?;
                self.data(id, &data)?;
            }
            SessionEvent::FrameRecv { stream_id: id, frame_type, flags } if id > 0 => {
                if frame_type == FrameType::HEADERS {
                    self.headers(id, flags & 1 != 0)?;
                }
                if flags & 1 != 0 && (frame_type == FrameType::HEADERS || frame_type == FrameType::DATA) {
                    self.end(id)?;
                }
            }
            SessionEvent::StreamClose { stream_id: id, error_code } => {
                if let Some(stream) = self.streams.get_mut(&id) { stream.closed = true; }
                if self.streams.contains_key(&id) && !self.streams[&id].terminal && self.streams[&id].failure.is_none() {
                    if error_code.raw() != 0 {
                        self.fail_stream(id, Status::new(if error_code.raw() == 8 { Code::Cancelled } else { Code::Unavailable }, format!("HTTP/2 reset {}", error_code.raw())), error_code.raw())?;
                    } else if self.client() {
                        self.end(id)?;
                    } else if self.slots.get(id).is_some_and(|s| !s.remote_end_stream() || !s.local_end_stream()) {
                        self.fail_stream(id, Status::cancelled("peer closed unfinished request"), 0)?;
                    }
                }
                if self.client() && error_code.raw() == 0 && self.slots.get(id).is_some_and(|s| s.reset_error_code().is_none()) { self.slots.record_rst_stream(id, 0).map_err(slot_error)?; }
            }
            SessionEvent::GoAway { last_stream_id, error_code, .. } => {
                for id in self.slots.apply_goaway(last_stream_id, error_code.raw()).map_err(slot_error)? {
                    let stream = self.stream_mut(id)?;
                    stream.failure = Some(Status::unavailable("stream excluded by GOAWAY"));
                    stream.outgoing.borrow_mut().bytes.clear();
                    stream.pending_trailers = None;
                    stream.decoder.reset();
                    stream.reserved = 0;
                    self.incoming.retain(|r| r.stream_id != id);
                }
            }
            SessionEvent::FrameNotSend { stream_id, lib_error_code, .. } => {
                return Err(Status::unavailable(format!("frame for {stream_id} rejected: {lib_error_code}")));
            }
            SessionEvent::Error { code, .. } => return Err(internal(code)),
            _ => {}
        }
        Ok(())
    }
    fn headers(&mut self, id: i32, end: bool) -> Result<(), Status> {
        let headers = std::mem::take(&mut self.stream_mut(id)?.headers);
        self.stream_mut(id)?.header_bytes = 0;
        let first = !self.stream_mut(id)?.initial;
        if first {
            if self.client() {
                let status = value(&headers, ":status")?.unwrap_or("").parse::<u16>().map_err(internal)?;
                if (100..200).contains(&status) && !end { return Ok(()); }
                self.stream_mut(id)?.http_status = status;
            } else if value(&headers, ":method")? != Some("POST") || value(&headers, "te")? != Some("trailers") {
                return Err(Status::internal("invalid gRPC request headers"));
            }
            if (!self.client() || self.stream_mut(id)?.http_status == 200 || value(&headers, "grpc-status")?.is_some())
                && !value(&headers, "content-type")?.is_some_and(|v| v.starts_with("application/grpc"))
            {
                return Err(Status::internal("invalid gRPC content-type"));
            }
            let encoding = CompressionAlgorithm::from_encoding(value(&headers, "grpc-encoding")?.unwrap_or("identity"))?;
            if !self.client() {
                self.incoming.push_back(IncomingRequest {
                    stream_id: id, content_type: value(&headers, "content-type")?.unwrap_or("").into(),
                    path: value(&headers, ":path")?.ok_or_else(|| Status::internal("missing :path"))?.into(),
                    authority: value(&headers, ":authority")?.unwrap_or("").into(),
                    timeout: value(&headers, "grpc-timeout")?.map(parse_grpc_timeout).transpose().map_err(internal)?,
                    request_encoding: encoding,
                    accept_encoding: value(&headers, "grpc-accept-encoding")?.unwrap_or("identity").into(),
                    initial_metadata: metadata(&headers)?,
                });
            }
            self.stream_mut(id)?.encoding = encoding;
            self.stream_mut(id)?.initial = true;
            if !(self.client() && end && value(&headers, "grpc-status")?.is_some()) {
                self.slots.push_initial_metadata(id, headers.clone()).map_err(slot_error)?;
            }
        }
        if self.client() && end {
            if let Some(code) = value(&headers, "grpc-status")? {
                let n = code.parse::<i32>().map_err(internal)?;
                if !(0..=16).contains(&n) { return Err(Status::internal("invalid grpc-status")); }
            } else {
                return self.missing_trailers(id);
            }
            self.stream_mut(id)?.decoder.finish()?;
            self.slots.push_trailing_metadata(id, headers).map_err(slot_error)?;
            self.stream_mut(id)?.terminal = true;
        } else if !first {
            return Err(Status::internal("unexpected nonterminal header block"));
        }
        Ok(())
    }
    fn missing_trailers(&mut self, id: i32) -> Result<(), Status> {
        let http = self.stream_mut(id)?.http_status;
        let status = if http == 200 { Status::unknown("missing grpc-status trailer") } else { Status::from_http_status(http) };
        self.slots
            .push_trailing_metadata(id, vec![("grpc-status".into(), status.code().as_i32().to_string().into_bytes()), ("grpc-message".into(), status.encoded_message().into_bytes())])
            .map_err(slot_error)?;
        self.stream_mut(id)?.terminal = true;
        Ok(())
    }
    fn end(&mut self, id: i32) -> Result<(), Status> {
        self.stream_mut(id)?.decoder.finish()?;
        if self.client() && !self.stream_mut(id)?.terminal {
            self.missing_trailers(id)?;
        }
        if !self.slots.get(id).is_some_and(|s| s.remote_end_stream()) {
            self.slots.mark_remote_end_stream(id).map_err(slot_error)?;
        }
        Ok(())
    }
    fn data(&mut self, id: i32, data: &[u8]) -> Result<(), Status> {
        let budget = self.config.per_stream_byte_budget;
        let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
        let client = self.client();
        let stream = self.stream_mut(id)?;
        if client && stream.http_status != 200 { return Ok(()); }
        if !stream.initial { return Err(Status::internal("DATA before initial headers")); }
        let mut completed = 0usize;
        for &byte in data {
            if stream.remaining > 0 {
                stream.remaining -= 1;
                if stream.remaining == 0 {
                    completed = completed.checked_add(stream.reserved).ok_or_else(resource)?;
                    stream.reserved = 0;
                }
            } else {
                stream.prefix.push(byte);
                if stream.prefix.len() == 5 {
                    let n = u32::from_be_bytes([stream.prefix[1], stream.prefix[2], stream.prefix[3], stream.prefix[4]]) as usize;
                    stream.reserved = n.checked_add(5 + std::mem::size_of::<StreamMessage>()).ok_or_else(resource)?;
                    stream.remaining = n;
                    stream.prefix.clear();
                    if n == 0 {
                        completed = completed.checked_add(stream.reserved).ok_or_else(resource)?;
                        stream.reserved = 0;
                    }
                }
            }
            if retained.checked_add(completed).and_then(|n| n.checked_add(stream.reserved)).and_then(|n| n.checked_add(stream.prefix.len())).is_none_or(|n| n > budget) { return Err(resource()); }
        }
        let frames = stream.decoder.push(data)?;
        let encoding = stream.encoding;
        for frame in frames {
            let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
            let remaining = self.stream_mut(id)?.reserved;
            let available = budget.saturating_sub(retained).saturating_sub(remaining).saturating_sub(5 + std::mem::size_of::<StreamMessage>());
            let config = CompressionConfig { algorithm: encoding, max_decoding_message_size: self.config.compression.max_decoding_message_size.min(self.config.max_inbound_message_size).min(available), ..self.config.compression };
            let payload = config.decompress(frame.payload(), frame.is_compressed())?;
            self.slots.push_message(id, StreamMessage { compressed: false, payload }).map_err(slot_error)?;
        }
        Ok(())
    }
}
