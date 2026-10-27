//! Synchronous HTTP/2 gRPC sessions with bounded per-stream receive and send queues.
//!
//! Receive budgets cover metadata, message descriptors and reserved partial envelopes.
//! Native HTTP/2 storage, temporary copies and queued IncomingRequest metadata are separate.
mod receive;
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    io,
    marker::PhantomData,
    rc::Rc,
    time::Duration,
};

use receive::*;

use crate::{
    compression::{CompressionAlgorithm, CompressionConfig},
    framing::{AlignedMessageBuf, LengthPrefixedFrame},
    http2::{
        DataPoll, DataProvider, DataSource, FrameType, HeaderField, Http2Role, Http2Session,
        Http2Settings, SessionEvent,
    },
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
impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100,
            initial_window_size: 1024 * 1024,
            max_frame_size: 16_384,
            max_header_list_size: 64 * 1024,
            max_inbound_message_size: 16 * 1024 * 1024,
            per_stream_byte_budget: 16 * 1024 * 1024,
            max_active_streams: 1024,
            compression: CompressionConfig::default(),
            user_agent: "grpc-rs/0.1.0".into(),
            scheme: "http".into(),
            content_type: "application/grpc".into(),
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
            if queue.trailers {
                DataPoll::EofWithTrailers
            } else {
                DataPoll::Eof
            }
        } else {
            queue.deferred = true;
            DataPoll::Deferred
        }
    }
}
struct Stream {
    decoder: LengthPrefixedFrame,
    encoding: CompressionAlgorithm,
    outgoing_encoding: CompressionAlgorithm,
    outgoing: Rc<RefCell<Outbound>>,
    headers: Headers,
    header_bytes: usize,
    state: StreamHsm,
    failure: Option<Status>,
    failure_taken: bool,
    http_status: u16,
    pending_trailers: Option<Vec<(String, String)>>,
    prefix: [u8; 5],
    prefix_len: usize,
    remaining: usize,
    reserved: usize,
}
/// Single-threaded session. Closed slots remain readable until explicitly removed.
pub struct GrpcSession<Role> {
    protocol: Http2Session,
    config: SessionConfig,
    slots: StreamSlotMap,
    streams: BTreeMap<i32, Stream>,
    incoming: VecDeque<IncomingRequest>,
    wire: Vec<u8>,
    transport_pending: Vec<u8>,
    transport_offset: usize,
    transport_flushed: bool,
    failure: Option<SessionFailure>,
    phase: SessionPhase,
    role: PhantomData<Role>,
}
macro_rules! constructor {
    ($role:ty, $variant:ident, $endpoint:expr) => {
        impl GrpcSession<$role> {
            /// Creates an engine and queues its initial HTTP/2 SETTINGS.
            /// # Errors
            /// Rejects invalid settings and protocol initialization failure.
            pub fn new(config: SessionConfig) -> Result<Self, Status> {
                if config.max_active_streams == 0
                    || config.initial_window_size > i32::MAX as u32
                    || !(16_384..=16_777_215).contains(&config.max_frame_size)
                    || !matches!(config.scheme.as_str(), "http" | "https")
                {
                    return Err(Status::invalid_argument(
                        "invalid HTTP/2 session configuration",
                    ));
                }
                validate_content_type(&config.content_type)?;
                let protocol = Http2Session::new(
                    Http2Role::$variant,
                    Http2Settings {
                        max_concurrent_streams: config.max_concurrent_streams,
                        initial_window_size: config.initial_window_size,
                        max_frame_size: config.max_frame_size,
                        max_header_list_size: config.max_header_list_size,
                        enable_push: false,
                        ..Http2Settings::default()
                    },
                )
                .map_err(internal)?;
                let slots = StreamSlotMap::with_role(
                    StreamLimits {
                        max_buffered_bytes: config.per_stream_byte_budget,
                        max_queued_messages: 1024,
                    },
                    $endpoint,
                );
                Ok(Self {
                    protocol,
                    config,
                    slots,
                    streams: BTreeMap::new(),
                    incoming: VecDeque::new(),
                    wire: Vec::new(),
                    transport_pending: Vec::new(),
                    transport_offset: 0,
                    transport_flushed: true,
                    failure: None,
                    phase: SessionPhase::Active,
                    role: PhantomData,
                })
            }
        }
    };
}
constructor!(Client, Client, EndpointRole::Client);
constructor!(Server, Server, EndpointRole::Server);
impl GrpcClientSession {
    /// Starts a POST request; false END_STREAM enables incremental message sending.
    /// # Errors
    /// Rejects stream limits, invalid metadata/timeout or protocol submission failure.
    pub fn start_request(
        &mut self,
        authority: &str,
        path: &str,
        metadata: &Metadata,
        timeout: Option<Timeout>,
        encoding: CompressionAlgorithm,
        end_stream: bool,
    ) -> Result<i32, Status> {
        let content_type = self.config.content_type.clone();
        self.start_request_with_content_type(
            authority,
            path,
            metadata,
            timeout,
            encoding,
            end_stream,
            &content_type,
        )
    }
    /// Starts a request with a codec media type independently of other RPCs.
    /// # Errors
    /// Rejects invalid media types, metadata, stream limits or protocol submission failure.
    #[expect(
        clippy::too_many_arguments,
        reason = "The request carries routing, metadata, deadline, encoding and codec identity."
    )]
    pub fn start_request_with_content_type(
        &mut self,
        authority: &str,
        path: &str,
        metadata: &Metadata,
        timeout: Option<Timeout>,
        encoding: CompressionAlgorithm,
        end_stream: bool,
        content_type: &str,
    ) -> Result<i32, Status> {
        validate_content_type(content_type)?;
        self.check_capacity()?;
        if authority.is_empty() || !path.starts_with('/') {
            return Err(Status::invalid_argument("invalid RPC authority or path"));
        }
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
            headers.push((
                "grpc-timeout".into(),
                format_grpc_timeout(timeout).map_err(internal)?,
            ));
        }
        validate_metadata(metadata)?;
        headers.extend(metadata.to_headers());
        self.check_headers(&headers)?;
        let queue = Rc::new(RefCell::new(Outbound::default()));
        let provider = (!end_stream).then(|| {
            DataProvider::new(Source {
                queue: Rc::clone(&queue),
                chunk: Vec::new(),
            })
        });
        let id = self
            .protocol
            .submit_request(&fields(&headers), provider)
            .map_err(internal)?;
        self.open(id, queue, encoding)?;
        self.stream_mut(id)?.state.send_headers();
        if end_stream {
            self.slots.mark_local_end_stream(id).map_err(slot_error)?;
            self.stream_mut(id)?.state.queue_end();
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
        self.stream_mut(id)?.state.queue_end();
        self.resume(id)
    }
}
impl GrpcServerSession {
    /// Releases safely closed streams that were never handed to a request consumer.
    /// Dispatched streams retain their terminal state until explicitly removed.
    ///
    /// # Errors
    /// Returns a stream-removal error if its closure state is inconsistent.
    pub fn reap_closed_undispatched_streams(&mut self) -> Result<usize, Status> {
        let closed: Vec<_> = self
            .streams
            .iter()
            .filter(|(id, stream)| {
                !stream.state.dispatched()
                    && stream.state.closed()
                    && self.stream_outbound_complete(**id)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in &closed {
            self.remove_closed_stream(*id)?;
        }
        Ok(closed.len())
    }
    /// Transfers the next received request's routing information.
    pub fn pop_incoming_request(&mut self) -> Option<IncomingRequest> {
        let request = self.incoming.pop_front()?;
        if let Some(stream) = self.streams.get_mut(&request.stream_id) {
            stream.state.dispatch();
        }
        Some(request)
    }
    /// Sends response headers and installs an incremental body source.
    /// # Errors
    /// Rejects duplicate headers, ended streams and protocol submission failure.
    pub fn send_initial_metadata(
        &mut self,
        id: i32,
        metadata: &Metadata,
        encoding: CompressionAlgorithm,
    ) -> Result<(), Status> {
        self.send_initial_metadata_with_content_type(
            id,
            metadata,
            encoding,
            &self.config.content_type.clone(),
        )
    }
    /// Starts a response using the codec selected for this RPC's route.
    /// # Errors
    /// Rejects invalid media types, duplicate headers, ended streams or protocol failure.
    pub fn send_initial_metadata_with_content_type(
        &mut self,
        id: i32,
        metadata: &Metadata,
        encoding: CompressionAlgorithm,
        content_type: &str,
    ) -> Result<(), Status> {
        self.begin_response(id, metadata, encoding, content_type)
    }
    /// Completes a response, emitting trailers only when its queued DATA has drained.
    /// # Errors
    /// Rejects duplicate completion, invalid streams, budgets or protocol failure.
    pub fn finish_response(
        &mut self,
        id: i32,
        status: &Status,
        metadata: &Metadata,
    ) -> Result<(), Status> {
        self.check_send(id)?;
        let mut headers = vec![("grpc-status".into(), status.code().as_i32().to_string())];
        if !status.message().is_empty() {
            headers.push(("grpc-message".into(), status.encoded_message()));
        }
        validate_metadata(metadata)?;
        headers.extend(metadata.to_headers());
        if !self.stream_mut(id)?.state.headers_sent() {
            headers.insert(0, ("content-type".into(), self.config.content_type.clone()));
            headers.insert(0, (":status".into(), "200".into()));
            self.check_headers(&headers)?;
            self.protocol
                .submit_response(id, &fields(&headers), None)
                .map_err(internal)?;
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
        self.stream_mut(id)?.state.dispatch();
        self.slots.mark_local_end_stream(id).map_err(slot_error)?;
        self.stream_mut(id)?.state.queue_end();
        Ok(())
    }
}
impl<Role> GrpcSession<Role> {
    fn begin_response(
        &mut self,
        id: i32,
        metadata: &Metadata,
        encoding: CompressionAlgorithm,
        content_type: &str,
    ) -> Result<(), Status> {
        validate_content_type(content_type)?;
        self.check_send(id)?;
        if self.stream_mut(id)?.state.headers_sent() {
            return Err(Status::failed_precondition("response headers already sent"));
        }
        let mut headers = vec![
            (":status".into(), "200".into()),
            ("content-type".into(), content_type.into()),
            ("grpc-accept-encoding".into(), ACCEPT.into()),
        ];
        if encoding != CompressionAlgorithm::Identity {
            headers.push(("grpc-encoding".into(), encoding.as_str().into()));
        }
        validate_metadata(metadata)?;
        headers.extend(metadata.to_headers());
        self.check_headers(&headers)?;
        let queue = Rc::clone(&self.stream_mut(id)?.outgoing);
        self.protocol
            .submit_response(
                id,
                &fields(&headers),
                Some(DataProvider::new(Source {
                    queue,
                    chunk: Vec::new(),
                })),
            )
            .map_err(internal)?;
        let stream = self.stream_mut(id)?;
        stream.state.send_headers();
        stream.state.dispatch();
        stream.outgoing_encoding = encoding;
        Ok(())
    }

    /// Queues GOAWAY at the last processed peer stream; existing RPCs may finish.
    /// # Errors
    /// Returns protocol submission or a previous connection failure.
    pub fn submit_goaway(&mut self, error_code: u32) -> Result<(), Status> {
        self.check_live()?;
        let last = self.protocol.last_proc_stream_id();
        self.protocol
            .submit_goaway(last, error_code, &[])
            .map_err(internal)?;
        self.phase = SessionPhase::Draining;
        Ok(())
    }
    /// Whether the protocol connection can accept additional inbound bytes.
    pub fn want_read(&self) -> bool {
        self.phase != SessionPhase::Failed && self.protocol.want_read()
    }
    /// Whether protocol frames or partially written transport bytes need sending.
    pub fn want_write(&self) -> bool {
        !self.transport_flushed
            || self.transport_offset < self.transport_pending.len()
            || self
                .streams
                .values()
                .any(|s| s.pending_trailers.is_some() && s.outgoing.borrow().finished)
            || self.protocol.want_write()
    }
    /// The most restrictive peer GOAWAY boundary and its HTTP/2 error code.
    pub fn goaway_received(&self) -> Option<(i32, u32)> {
        self.slots.goaway()
    }
    /// Whether terminal output was serialized and tracked transport writes/flushes drained.
    pub fn stream_outbound_complete(&self, id: i32) -> bool {
        self.transport_flushed
            && self.transport_offset == self.transport_pending.len()
            && self
                .streams
                .get(&id)
                .is_some_and(|s| s.state.outbound_phase() == OutboundPhase::Closed)
    }
    /// Whether local or peer GOAWAY has stopped admission of new RPCs.
    pub fn is_draining(&self) -> bool {
        self.phase == SessionPhase::Draining
            || self.slots.goaway().is_some()
            || self
                .failure
                .as_ref()
                .is_some_and(|failure| failure.phase == SessionPhase::Draining)
    }
    fn client(&self) -> bool {
        self.protocol.role() == Http2Role::Client
    }
    fn check_capacity(&self) -> Result<(), Status> {
        self.check_live()?;
        if self.is_draining() {
            return Err(Status::unavailable("connection is draining"));
        }
        if self.streams.len() >= self.config.max_active_streams {
            return Err(Status::resource_exhausted("active stream limit exceeded"));
        }
        Ok(())
    }
    fn check_headers(&self, headers: &[(String, String)]) -> Result<(), Status> {
        let bytes = headers
            .iter()
            .try_fold(0usize, |n, (k, v)| n.checked_add(k.len() + v.len() + 32))
            .ok_or_else(resource)?;
        if bytes > self.config.max_header_list_size as usize
            || bytes > self.config.per_stream_byte_budget
        {
            return Err(resource());
        }
        Ok(())
    }
    fn stream_mut(&mut self, id: i32) -> Result<&mut Stream, Status> {
        self.streams
            .get_mut(&id)
            .ok_or_else(|| Status::not_found("unknown stream"))
    }
    fn open(
        &mut self,
        id: i32,
        queue: Rc<RefCell<Outbound>>,
        encoding: CompressionAlgorithm,
    ) -> Result<(), Status> {
        self.check_capacity()?;
        self.slots.open_stream(id).map_err(slot_error)?;
        self.streams.insert(
            id,
            Stream {
                decoder: LengthPrefixedFrame::new(
                    self.config
                        .max_inbound_message_size
                        .min(self.config.per_stream_byte_budget),
                ),
                encoding: CompressionAlgorithm::Identity,
                outgoing_encoding: encoding,
                outgoing: queue,
                headers: vec![],
                header_bytes: 0,
                state: StreamHsm::default(),
                failure: None,
                failure_taken: false,
                http_status: 200,
                pending_trailers: None,
                prefix: [0; 5],
                prefix_len: 0,
                remaining: 0,
                reserved: 0,
            },
        );
        Ok(())
    }
    fn check_send(&self, id: i32) -> Result<(), Status> {
        self.check_live()?;
        let stream = self
            .streams
            .get(&id)
            .ok_or_else(|| Status::not_found("unknown stream"))?;
        if let Some(error) = &stream.failure {
            return Err(error.clone());
        }
        let slot = self
            .slots
            .get(id)
            .ok_or_else(|| Status::not_found("unknown stream"))?;
        if slot.local_end_stream() {
            return Err(Status::failed_precondition("sending half already closed"));
        }
        Ok(())
    }
    fn resume(&mut self, id: i32) -> Result<(), Status> {
        let deferred = self.stream_mut(id)?.outgoing.borrow().deferred;
        if deferred {
            self.protocol.resume_data(id).map_err(internal)?;
            self.stream_mut(id)?.outgoing.borrow_mut().deferred = false;
        }
        Ok(())
    }
    /// Queues one framed message, preserving the stream's negotiated encoding.
    /// # Errors
    /// Rejects ended streams, mismatched encodings, compression failure or byte excess.
    pub fn send_message(
        &mut self,
        id: i32,
        payload: &[u8],
        encoding: CompressionAlgorithm,
    ) -> Result<(), Status> {
        self.check_send(id)?;
        if !self.client() && !self.stream_mut(id)?.state.headers_sent() {
            self.begin_response(
                id,
                &Metadata::new(),
                encoding,
                &self.config.content_type.clone(),
            )?;
        }

        if encoding != CompressionAlgorithm::Identity
            && encoding != self.stream_mut(id)?.outgoing_encoding
        {
            return Err(Status::invalid_argument(
                "message encoding differs from stream encoding",
            ));
        }
        if payload.len() > self.config.per_stream_byte_budget {
            return Err(resource());
        }
        let compressed = CompressionConfig {
            algorithm: encoding,
            ..self.config.compression
        }
        .compress(payload)?;
        let frame = AlignedMessageBuf::new(&compressed.data, compressed.compressed)?;
        let budget = self.config.per_stream_byte_budget;
        let stream = self.stream_mut(id)?;
        let mut queue = stream.outgoing.borrow_mut();
        if queue
            .bytes
            .len()
            .checked_add(frame.wire_bytes().len())
            .is_none_or(|n| n > budget)
        {
            return Err(resource());
        }
        queue.bytes.extend(frame.wire_bytes());
        drop(queue);
        self.resume(id)
    }
    /// Resets a stream and releases its queued outbound body.
    /// Completed NO_ERROR peer closure needs no additional local upload reset.
    /// # Errors
    /// Rejects unknown streams or protocol reset submission failure.
    pub fn cancel_stream(&mut self, id: i32, error_code: u32) -> Result<(), Status> {
        self.check_read(id)?;
        if error_code == 0
            && self
                .streams
                .get(&id)
                .is_some_and(|s| s.state.closed() && s.state.terminal())
        {
            return Ok(());
        }
        self.protocol
            .submit_rst_stream(id, error_code)
            .map_err(internal)?;
        self.fail_stream(id, Status::cancelled("stream cancelled"), error_code)
    }
    fn fail_stream(&mut self, id: i32, error: Status, code: u32) -> Result<(), Status> {
        let stream = self.stream_mut(id)?;
        if stream.failure.is_some() {
            return Ok(());
        }
        stream.failure = Some(error);
        stream.decoder.reset();
        stream.reserved = 0;
        stream.prefix_len = 0;
        stream.headers.clear();
        stream.pending_trailers = None;
        stream.outgoing.borrow_mut().bytes.clear();
        self.incoming.retain(|r| r.stream_id != id);
        self.slots.record_rst_stream(id, code).map_err(slot_error)
    }
    /// Feeds received HTTP/2 bytes and dispatches every resulting callback event.
    /// # Errors
    /// Returns protocol connection errors; malformed RPC streams are reset individually.
    pub fn mem_recv(&mut self, bytes: &[u8]) -> Result<usize, Status> {
        self.check_live()?;
        let mut count = 0;
        for chunk in bytes.chunks(16 * 1024) {
            let n = self.protocol.mem_recv(chunk).map_err(internal)?;
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
    /// Returns protocol connection or callback-processing errors.
    pub fn mem_send(&mut self) -> Result<&[u8], Status> {
        if self.transport_offset < self.transport_pending.len() {
            return Err(Status::failed_precondition(
                "transport output must drain before mem_send",
            ));
        }
        self.check_live()?;
        self.submit_ready_trailers()?;
        self.wire = self.protocol.mem_send().map_err(internal)?.to_vec();
        self.events()?;
        if self.wire.is_empty() {
            self.submit_ready_trailers()?;
            self.wire = self.protocol.mem_send().map_err(internal)?.to_vec();
            self.events()?;
        }
        Ok(&self.wire)
    }
    fn submit_ready_trailers(&mut self) -> Result<(), Status> {
        let ready: Vec<_> = self
            .streams
            .iter()
            .filter(|(_, s)| s.pending_trailers.is_some() && s.outgoing.borrow().finished)
            .map(|(id, _)| *id)
            .collect();
        for id in ready {
            if let Some(headers) = self.stream_mut(id)?.pending_trailers.take() {
                self.protocol
                    .submit_trailer(id, &fields(&headers))
                    .map_err(internal)?;
            }
        }
        Ok(())
    }
    /// Flushes wire output, retaining partially written chunks across WouldBlock.
    /// # Errors
    /// Returns transport or session failures; WouldBlock means retry later.
    pub fn flush_to_transport<T: SyncTransport>(
        &mut self,
        transport: &mut T,
    ) -> Result<(), Status> {
        loop {
            if self.transport_offset == self.transport_pending.len() {
                self.transport_pending = self.mem_send()?.to_vec();
                self.transport_offset = 0;
                if !self.transport_pending.is_empty() {
                    self.transport_flushed = false;
                }
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
            Ok(()) => {
                self.transport_flushed = true;
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.transport_flushed = false;
                Ok(())
            }
            Err(e) => Err(Status::unavailable(e.to_string())),
        }
    }
    /// Flushes outgoing data, reads once and routes inbound bytes.
    /// # Errors
    /// Returns transport EOF/errors or session errors. WouldBlock returns zero.
    pub fn step_transport<T: SyncTransport>(
        &mut self,
        transport: &mut T,
        buf: &mut [u8],
    ) -> Result<usize, Status> {
        if buf.is_empty() {
            return Err(Status::invalid_argument("receive buffer is empty"));
        }
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
            if count == 0 {
                return Err(Status::internal("HTTP/2 receive made no progress"));
            }
            used += count;
        }
        self.flush_to_transport(transport)?;
        Ok(n)
    }
}
