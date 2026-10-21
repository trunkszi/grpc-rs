//! Caller-thread client channels with shared HTTP/2 sessions and typed RPC streams.
use std::{cell::RefCell, collections::BTreeSet, io, rc::Rc, time::Instant};

use grpc_core::{
    Code, Codec, CompressionAlgorithm, GrpcClientSession, Metadata, SessionConfig, Status, Timeout,
    transport::{SyncTransport, TcpTransport},
};

use crate::stream::{
    BidiStreamingCall, ClientStreamingCall, ServerStreamingCall, StreamDriver, StreamError,
};

/// Connection defaults applied before starting each RPC.
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    pub authority: String,
    pub scheme: String,
    pub default_timeout: Option<Timeout>,
    pub default_compression: CompressionAlgorithm,
    pub session_config: SessionConfig,
}
impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            authority: "localhost".into(),
            scheme: "http".into(),
            default_timeout: None,
            default_compression: CompressionAlgorithm::Identity,
            session_config: SessionConfig::default(),
        }
    }
}
impl ChannelConfig {
    /// Selects the default outgoing compression (Zstd uses level one).
    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.default_compression = compression;
        self
    }
}
/// Metadata and overrides for one RPC.
#[derive(Clone, Debug, Default)]
pub struct CallOptions {
    pub metadata: Metadata,
    pub timeout: Option<Timeout>,
    pub compression: Option<CompressionAlgorithm>,
}
struct ChannelInner<T: SyncTransport> {
    session: GrpcClientSession,
    transport: T,
    buffer: Vec<u8>,
    config: ChannelConfig,
    cleanup_error: Option<Status>,
    pending_reap: BTreeSet<i32>,
}
impl<T: SyncTransport> ChannelInner<T> {
    fn pump(&mut self, wait: bool, deadline: Option<Instant>) -> Result<(), Status> {
        let remaining = deadline
            .map(|end| {
                end.checked_duration_since(Instant::now())
                    .filter(|n| !n.is_zero())
                    .ok_or_else(|| Status::deadline_exceeded("RPC deadline elapsed"))
            })
            .transpose()?;
        self.transport.set_nonblocking(!wait).map_err(io_status)?;
        let result = (|| {
            if wait {
                self.transport
                    .set_read_timeout(remaining)
                    .map_err(io_status)?;
                self.transport
                    .set_write_timeout(remaining)
                    .map_err(io_status)?;
            }
            self.session
                .step_transport(&mut self.transport, &mut self.buffer)
                .map(|_| ())
        })();
        let restored = self.transport.set_nonblocking(true).map_err(io_status);
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(Status::deadline_exceeded("RPC deadline elapsed"));
        }
        result.and(restored)?;
        self.reap()
    }
    fn cleanup(&mut self, id: i32) {
        let reset = self.session.cancel_stream(id, 8);
        self.pending_reap.insert(id);
        if let Err(error) = self.flush() {
            self.cleanup_error = Some(error);
        } else if self.pending_reap.contains(&id)
            && let Err(error) = reset
        {
            self.cleanup_error = Some(error);
        }
    }
    fn reap(&mut self) -> Result<(), Status> {
        let ids: Vec<_> = self.pending_reap.iter().copied().collect();
        for id in ids {
            match self.session.remove_closed_stream(id) {
                Ok(()) => {
                    self.pending_reap.remove(&id);
                }
                Err(error) if error.code() == Code::NotFound => {
                    self.pending_reap.remove(&id);
                }
                Err(error) if error.code() == Code::FailedPrecondition => {}
                Err(error) => {
                    self.cleanup_error = Some(error.clone());
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Status> {
        self.session.flush_to_transport(&mut self.transport)?;
        self.reap()
    }
}
fn io_status(error: io::Error) -> Status {
    Status::unavailable(error.to_string())
}
fn codec_status(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("codec failure: {error}"))
}

/// Shared, single-threaded connection. Clones share one HTTP/2 session.
///
/// Connections stay on their owning OS thread.
/// ```compile_fail
/// use grpc::{SyncChannel, core::transport::MemoryPipe};
/// fn require_send<T: Send>() {}
/// require_send::<SyncChannel<MemoryPipe>>();
/// ```
/// ```compile_fail
/// use grpc::{SyncChannel, core::transport::MemoryPipe};
/// fn require_sync<T: Sync>() {}
/// require_sync::<SyncChannel<MemoryPipe>>();
/// ```
pub struct SyncChannel<T: SyncTransport> {
    inner: Rc<RefCell<ChannelInner<T>>>,
}
impl<T: SyncTransport> Clone for SyncChannel<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}
impl<T: SyncTransport + 'static> SyncChannel<T> {
    /// Creates a channel using standard connection defaults.
    /// # Errors
    /// Returns invalid configuration or transport/session initialization failures.
    pub fn new(transport: T) -> Result<Self, Status> {
        Self::with_config(transport, ChannelConfig::default())
    }
    /// Creates a nonblocking session; blocking RPC methods wait on the caller thread.
    /// # Errors
    /// Returns invalid authority/scheme, transport or session initialization errors.
    pub fn with_config(mut transport: T, mut config: ChannelConfig) -> Result<Self, Status> {
        if config.authority.is_empty()
            || config
                .authority
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        {
            return Err(Status::invalid_argument("invalid channel authority"));
        }
        config.session_config.scheme.clone_from(&config.scheme);
        let session = GrpcClientSession::new(config.session_config.clone())?;
        transport.set_nonblocking(true).map_err(io_status)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(ChannelInner {
                session,
                transport,
                buffer: vec![0; 16 * 1024],
                config,
                cleanup_error: None,
                pending_reap: BTreeSet::new(),
            })),
        })
    }
    fn start<C: Codec>(
        &mut self,
        path: &str,
        codec: C,
        options: CallOptions,
        content_type: &str,
    ) -> Result<ChannelStreamDriver<T, C>, Status> {
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant channel access"))?;
        inner.reap()?;
        if let Some(error) = &inner.cleanup_error {
            return Err(error.clone());
        }
        let timeout = options.timeout.or(inner.config.default_timeout);
        let deadline = timeout
            .map(|duration| {
                Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| Status::invalid_argument("RPC deadline overflow"))
            })
            .transpose()?;
        if timeout.is_some_and(|duration| duration.is_zero()) {
            return Err(Status::deadline_exceeded("RPC deadline elapsed"));
        }
        let encoding = options
            .compression
            .unwrap_or(inner.config.default_compression);
        let authority = inner.config.authority.clone();
        let id = inner.session.start_request_with_content_type(
            &authority,
            path,
            &options.metadata,
            timeout,
            encoding,
            false,
            content_type,
        )?;
        Ok(ChannelStreamDriver {
            inner: Rc::clone(&self.inner),
            id,
            codec,
            encoding,
            deadline,
            initial: None,
            trailing: None,
            complete: false,
            closed: false,
            buffer: Vec::new(),
        })
    }
    /// Performs a unary RPC, returning its response and both metadata collections.
    ///
    /// A mutable codec also supports stateful serializers and generated service adapters.
    /// # Errors
    /// Returns transport, codec, deadline, remote status or response cardinality errors.
    pub fn unary<C, Req, Resp>(
        &mut self,
        path: &str,
        req: &Req,
        codec: &mut C,
        options: CallOptions,
    ) -> Result<(Resp, Metadata, Metadata), Status>
    where
        C: Codec<EncodeItem = Req, DecodeItem = Resp>,
    {
        let mut payload = Vec::new();
        codec.encode(req, &mut payload).map_err(codec_status)?;
        let mut driver = self.start(path, BytesCodec, options, C::CONTENT_TYPE)?;
        driver.send(&payload).map_err(status_only)?;
        driver.half_close().map_err(status_only)?;
        let mut response = None;
        loop {
            match driver.try_recv_next() {
                Ok(Some(bytes)) => {
                    if response.replace(bytes).is_some() {
                        driver.cancel();
                        return Err(Status::internal("multiple unary responses"));
                    }
                }
                Ok(None) => break,
                Err(StreamError::WouldBlock) => driver.wait_ready().map_err(status_only)?,
                Err(error) => return Err(status_only(error)),
            }
        }
        let bytes = response.ok_or_else(|| Status::internal("missing unary response"))?;
        let response = codec.decode(bytes.payload()).map_err(codec_status)?;
        Ok((
            response,
            driver.initial.take().unwrap_or_default(),
            driver.trailing.take().unwrap_or_default(),
        ))
    }
    /// Performs a unary call with default call options.
    /// # Errors
    /// Returns the same failures as [`Self::unary`].
    pub fn call_unary<C, Req, Resp>(
        &mut self,
        path: &str,
        req: &Req,
        codec: &mut C,
    ) -> Result<Resp, Status>
    where
        C: Codec<EncodeItem = Req, DecodeItem = Resp>,
    {
        self.unary(path, req, codec, CallOptions::default())
            .map(|(response, _, _)| response)
    }
    /// Starts one request followed by a response stream.
    /// # Errors
    /// Returns setup, encoding or request submission failures.
    pub fn server_streaming<C, Req, Resp>(
        &mut self,
        path: &str,
        req: &Req,
        codec: C,
        options: CallOptions,
    ) -> Result<ServerStreamingCall<Resp>, Status>
    where
        C: Codec<EncodeItem = Req, DecodeItem = Resp> + 'static,
        Req: 'static,
        Resp: 'static,
    {
        let mut driver = self.start(path, codec, options, C::CONTENT_TYPE)?;
        driver.send(req).map_err(status_only)?;
        driver.half_close().map_err(status_only)?;
        Ok(ServerStreamingCall::new(driver))
    }
    /// Starts a request stream with one terminal response.
    /// # Errors
    /// Returns invalid options or session submission failures.
    pub fn client_streaming<C, Req, Resp>(
        &mut self,
        path: &str,
        codec: C,
        options: CallOptions,
    ) -> Result<ClientStreamingCall<Req, Resp>, Status>
    where
        C: Codec<EncodeItem = Req, DecodeItem = Resp> + 'static,
        Req: 'static,
        Resp: 'static,
    {
        Ok(ClientStreamingCall::new(self.start(
            path,
            codec,
            options,
            C::CONTENT_TYPE,
        )?))
    }
    /// Starts independent request and response streams.
    /// # Errors
    /// Returns invalid options or session submission failures.
    pub fn bidi_streaming<C, Req, Resp>(
        &mut self,
        path: &str,
        codec: C,
        options: CallOptions,
    ) -> Result<BidiStreamingCall<Req, Resp>, Status>
    where
        C: Codec<EncodeItem = Req, DecodeItem = Resp> + 'static,
        Req: 'static,
        Resp: 'static,
    {
        Ok(BidiStreamingCall::new(self.start(
            path,
            codec,
            options,
            C::CONTENT_TYPE,
        )?))
    }
}
impl SyncChannel<TcpTransport> {
    /// Connects a TCP channel with authority derived from the peer address.
    /// # Errors
    /// Returns connection, address or session initialization errors.
    pub fn connect(address: impl std::net::ToSocketAddrs) -> Result<Self, Status> {
        let transport = TcpTransport::connect(address).map_err(io_status)?;
        let authority = transport.peer_endpoint().map_err(io_status)?;
        Self::with_config(
            transport,
            ChannelConfig {
                authority,
                ..ChannelConfig::default()
            },
        )
    }
    /// Connects and verifies an HTTP/2 TLS session before creating the channel.
    /// # Errors
    /// Returns invalid server name, TCP, certificate, ALPN or handshake errors.
    pub fn connect_tls(
        address: impl std::net::ToSocketAddrs,
        config: &grpc_core::tls::TlsClientConfig,
        server_name: &str,
    ) -> Result<SyncChannel<impl SyncTransport + 'static>, Status> {
        let name = server_name.to_owned().try_into().map_err(codec_status)?;
        let socket = TcpTransport::connect(address).map_err(io_status)?;
        let mut tls =
            grpc_core::tls::TlsTransport::client(socket, config, name).map_err(io_status)?;
        tls.handshake().map_err(io_status)?;
        SyncChannel::with_config(
            tls,
            ChannelConfig {
                authority: server_name.into(),
                scheme: "https".into(),
                ..ChannelConfig::default()
            },
        )
    }
}
#[cfg(unix)]
impl SyncChannel<grpc_core::transport::UnixTransport> {
    /// Connects a Unix-domain channel.
    /// # Errors
    /// Returns socket connection or session initialization errors.
    pub fn connect_unix(path: impl AsRef<std::path::Path>) -> Result<Self, Status> {
        Self::new(grpc_core::transport::UnixTransport::connect(path).map_err(io_status)?)
    }
}
fn status_only(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::unavailable("RPC operation would block"),
    }
}
struct BytesCodec;
impl Codec for BytesCodec {
    type EncodeItem = Vec<u8>;
    type DecodeItem = grpc_core::AlignedMessageBuf;
    type Error = Status;
    const CONTENT_TYPE: &'static str = "application/grpc";
    fn encode(&mut self, item: &Vec<u8>, destination: &mut Vec<u8>) -> Result<(), Self::Error> {
        destination.extend_from_slice(item);
        Ok(())
    }
    fn decode(&mut self, payload: &[u8]) -> Result<Self::DecodeItem, Self::Error> {
        grpc_core::AlignedMessageBuf::new(payload, false)
    }
}
/// Typed streaming driver over one shared caller-thread channel connection.
pub struct ChannelStreamDriver<T: SyncTransport, C: Codec> {
    inner: Rc<RefCell<ChannelInner<T>>>,
    id: i32,
    codec: C,
    encoding: CompressionAlgorithm,
    deadline: Option<Instant>,
    initial: Option<Metadata>,
    trailing: Option<Metadata>,
    complete: bool,
    closed: bool,
    buffer: Vec<u8>,
}
impl<T: SyncTransport, C: Codec> ChannelStreamDriver<T, C> {
    fn check_deadline(&self) -> Result<(), StreamError> {
        if self.deadline.is_some_and(|end| Instant::now() >= end) {
            Err(Status::deadline_exceeded("RPC deadline elapsed").into())
        } else {
            Ok(())
        }
    }
    fn receive(&mut self) -> Result<Option<C::DecodeItem>, StreamError> {
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant channel access"))?;
        if self.initial.is_none() {
            self.initial = inner.session.pop_initial_metadata(self.id)?;
        }
        if let Some(message) = inner.session.pop_message(self.id)? {
            return self
                .codec
                .decode(message.payload())
                .map(Some)
                .map_err(|error| codec_status(error).into());
        }
        if let Some((status, trailers)) = inner.session.take_trailers(self.id)? {
            self.trailing = Some(trailers);
            if !self.closed {
                inner.session.cancel_stream(self.id, 0)?;
                self.closed = true;
            }
            inner.pending_reap.insert(self.id);
            inner.flush()?;
            self.complete = true;
            return if status.code() == Code::Ok {
                Ok(None)
            } else {
                Err(status.into())
            };
        }
        Err(StreamError::WouldBlock)
    }
}
impl<T: SyncTransport, C: Codec> StreamDriver for ChannelStreamDriver<T, C>
where
    C::EncodeItem: Sized,
{
    type Send = C::EncodeItem;
    type Receive = C::DecodeItem;
    fn send(&mut self, message: &Self::Send) -> Result<(), StreamError> {
        self.check_deadline()?;
        if self.closed || self.complete {
            return Err(Status::failed_precondition("request sending half is closed").into());
        }
        self.buffer.clear();
        self.codec
            .encode(message, &mut self.buffer)
            .map_err(codec_status)?;
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant channel access"))?;
        inner
            .session
            .send_message(self.id, &self.buffer, self.encoding)?;
        inner.flush()?;
        Ok(())
    }
    fn half_close(&mut self) -> Result<(), StreamError> {
        self.check_deadline()?;
        if self.closed {
            return Ok(());
        }
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant channel access"))?;
        inner.session.half_close(self.id)?;
        self.closed = true;
        inner.flush()?;
        Ok(())
    }
    fn try_recv_next(&mut self) -> Result<Option<Self::Receive>, StreamError> {
        self.check_deadline()?;
        if self.complete {
            return Ok(None);
        }
        match self.receive() {
            Err(StreamError::WouldBlock) => {}
            result => return result,
        }
        self.inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant channel access"))?
            .pump(false, self.deadline)?;
        self.receive()
    }
    fn wait_ready(&mut self) -> Result<(), StreamError> {
        self.check_deadline()?;
        self.inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant channel access"))?
            .pump(true, self.deadline)?;
        Ok(())
    }
    fn finish(&mut self, _: &Status, _: &Metadata) -> Result<(), StreamError> {
        Err(Status::failed_precondition("client cannot finish server response").into())
    }
    fn initial_metadata(&self) -> Option<&Metadata> {
        self.initial.as_ref()
    }
    fn trailing_metadata(&self) -> Option<&Metadata> {
        self.trailing.as_ref()
    }
    fn cancel(&mut self) {
        if self.complete {
            return;
        }
        if let Ok(mut inner) = self.inner.try_borrow_mut() {
            inner.cleanup(self.id);
        }
        self.complete = true;
    }
}

impl<T: SyncTransport, C: Codec> Drop for ChannelStreamDriver<T, C> {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        if let Ok(mut inner) = self.inner.try_borrow_mut() {
            inner.cleanup(self.id);
        }
    }
}
