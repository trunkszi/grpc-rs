//! Caller-thread HTTP/2 service routing and typed request/response streams.
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    time::{Duration, Instant},
};

use grpc_core::{
    Codec, Metadata, Request, Response, Status,
    compression::CompressionAlgorithm,
    session::{GrpcServerSession, IncomingRequest, SessionConfig},
    transport::SyncTransport,
};

use crate::stream::{RequestStream, ResponseSink, StreamDriver, StreamError, server_streams};

fn normalized_content_type(value: &str) -> &str {
    if value == "application/grpc+proto" {
        "application/grpc"
    } else {
        value
    }
}
fn io_status(error: io::Error) -> Status {
    Status::unavailable(error.to_string())
}
fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking stream operation remained pending"),
    }
}
struct ServerInner<T> {
    session: GrpcServerSession,
    transport: T,
    buffer: Vec<u8>,
    pending_incoming: VecDeque<(Instant, IncomingRequest)>,
    pending_reap: BTreeSet<i32>,
    reset_reap: BTreeSet<i32>,
    closed: bool,
    draining: bool,
}
impl<T: SyncTransport> ServerInner<T> {
    fn drive(&mut self, blocking: bool, deadline: Option<Instant>) -> Result<usize, Status> {
        let remaining = |deadline: Option<Instant>| -> Result<Option<Duration>, Status> {
            let timeout = deadline.map(|at| at.saturating_duration_since(Instant::now()));
            if timeout.is_some_and(|duration| duration.is_zero()) {
                Err(Status::deadline_exceeded("request deadline elapsed"))
            } else {
                Ok(timeout)
            }
        };
        remaining(deadline)?;
        self.transport.set_nonblocking(true).map_err(io_status)?;
        self.flush()?;
        let mut result = loop {
            match self.transport.read(&mut self.buffer) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    remaining(deadline)?;
                }
                result => break result,
            }
        };
        if blocking && matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
            self.transport.set_nonblocking(false).map_err(io_status)?;
            let waited = (|| -> Result<_, Status> {
                loop {
                    let mut timeout = remaining(deadline)?;
                    if self.session.want_write() || !self.pending_reap.is_empty() {
                        let quantum = Duration::from_millis(1);
                        timeout = Some(timeout.map_or(quantum, |left| left.min(quantum)));
                    }
                    self.transport
                        .set_read_timeout(timeout)
                        .map_err(io_status)?;
                    match self.transport.read(&mut self.buffer) {
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        result => return Ok(result),
                    }
                }
            })();
            self.transport.set_nonblocking(true).map_err(io_status)?;
            result = waited?;
        }
        let count = match result {
            Ok(0) => {
                remaining(deadline)?;
                self.closed = true;
                return Ok(0);
            }
            Ok(count) => count,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                remaining(deadline)?;
                return Ok(0);
            }
            Err(error) => {
                remaining(deadline)?;
                return Err(io_status(error));
            }
        };
        let mut offset = 0;
        while offset < count {
            let used = self.session.mem_recv(&self.buffer[offset..count])?;
            if used == 0 {
                return Err(Status::internal("HTTP/2 receive made no progress"));
            }
            offset += used;
            while let Some(request) = self.session.pop_incoming_request() {
                self.pending_incoming.push_back((Instant::now(), request));
            }
        }
        self.flush()?;
        remaining(deadline)?;
        Ok(count)
    }
    fn flush(&mut self) -> Result<(), Status> {
        self.transport.set_nonblocking(true).map_err(io_status)?;
        self.session.flush_to_transport(&mut self.transport)?;
        self.session.reap_closed_undispatched_streams()?;
        let ready: Vec<_> = self
            .pending_reap
            .iter()
            .copied()
            .filter(|id| self.session.stream_outbound_complete(*id))
            .collect();
        for id in ready {
            if !self.reset_reap.contains(&id) {
                match self.session.is_remote_end_stream(id) {
                    Ok(false) => {
                        self.session.cancel_stream(id, 0)?;
                        self.reset_reap.insert(id);
                        self.session.flush_to_transport(&mut self.transport)?;
                    }
                    Ok(true) => {}
                    Err(_) => {
                        self.reset_reap.insert(id);
                    }
                }
            }
            match self.session.remove_closed_stream(id) {
                Ok(()) => {}
                Err(error) if error.code() == grpc_core::Code::FailedPrecondition => continue,
                Err(error) => return Err(error),
            }
            self.pending_reap.remove(&id);
            self.reset_reap.remove(&id);
        }
        Ok(())
    }
}

/// Raw payload codec for custom service dispatchers.
#[derive(Default)]
pub struct PayloadCodec;
impl Codec for PayloadCodec {
    type EncodeItem = Vec<u8>;
    type DecodeItem = Vec<u8>;
    type Error = Status;
    const CONTENT_TYPE: &'static str = "application/grpc";
    fn encode(&mut self, item: &Vec<u8>, destination: &mut Vec<u8>) -> Result<(), Status> {
        destination.extend_from_slice(item);
        Ok(())
    }
    fn decode(&mut self, payload: &[u8]) -> Result<Vec<u8>, Status> {
        Ok(payload.to_vec())
    }
}

/// Custom dispatcher receiving raw request payloads and a raw response sink.
/// Implementations can bridge generated service methods using their descriptors.
pub trait ServiceHandler {
    /// Runs one routed RPC. The server sends terminal status after this returns.
    /// # Errors
    /// Returns the RPC status to send to the caller.
    fn handle(
        &mut self,
        requests: RequestStream<Vec<u8>>,
        responses: &mut ResponseSink<Vec<u8>>,
    ) -> Result<(), Status>;
}
impl<F> ServiceHandler for F
where
    F: FnMut(RequestStream<Vec<u8>>, &mut ResponseSink<Vec<u8>>) -> Result<(), Status>,
{
    fn handle(
        &mut self,
        requests: RequestStream<Vec<u8>>,
        responses: &mut ResponseSink<Vec<u8>>,
    ) -> Result<(), Status> {
        self(requests, responses)
    }
}

/// Typed stream driver sharing a single caller-thread server connection.
pub struct ServerStreamDriver<T: SyncTransport, C: Codec> {
    inner: Rc<RefCell<ServerInner<T>>>,
    codec: Rc<RefCell<C>>,
    id: i32,
    deadline: Option<Instant>,
    initial: Metadata,
    trailers: Option<Metadata>,
    encoding: CompressionAlgorithm,
    response_initial_sent: bool,
    complete: Rc<Cell<bool>>,
}
impl<T: SyncTransport, C: Codec> ServerStreamDriver<T, C> {
    fn check_deadline(&self) -> Result<(), StreamError> {
        if !self.deadline.is_some_and(|at| Instant::now() >= at) {
            return Ok(());
        }
        let status = Status::deadline_exceeded("request deadline elapsed");
        if !self.complete.get() {
            let mut inner = self
                .inner
                .try_borrow_mut()
                .map_err(|_| Status::failed_precondition("reentrant server operation"))?;
            inner.transport.set_nonblocking(true).map_err(io_status)?;
            inner
                .session
                .finish_response(self.id, &status, &Metadata::new())?;
            self.complete.set(true);
            inner.pending_reap.insert(self.id);
            inner.flush()?;
        }
        Err(status.into())
    }
}
impl<T: SyncTransport, C: Codec> StreamDriver for ServerStreamDriver<T, C>
where
    C::EncodeItem: Sized,
{
    type Send = C::EncodeItem;
    type Receive = C::DecodeItem;
    fn send(&mut self, message: &Self::Send) -> Result<(), StreamError> {
        self.check_deadline()?;
        let mut bytes = Vec::new();
        self.codec
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant codec"))?
            .encode(message, &mut bytes)
            .map_err(|error| Status::internal(error.to_string()))?;
        self.check_deadline()?;
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant server operation"))?;
        inner.transport.set_nonblocking(true).map_err(io_status)?;
        if !self.response_initial_sent {
            inner.session.send_initial_metadata_with_content_type(
                self.id,
                &Metadata::new(),
                self.encoding,
                C::CONTENT_TYPE,
            )?;
            self.response_initial_sent = true;
        }
        inner.session.send_message(self.id, &bytes, self.encoding)?;
        inner.flush()?;
        Ok(())
    }
    fn half_close(&mut self) -> Result<(), StreamError> {
        Err(Status::failed_precondition("server responses close with final status").into())
    }
    fn try_recv_next(&mut self) -> Result<Option<Self::Receive>, StreamError> {
        self.check_deadline()?;
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant server operation"))?;
        for attempt in 0..2 {
            if let Some(message) = inner.session.pop_message(self.id)? {
                drop(inner);
                let value = self
                    .codec
                    .try_borrow_mut()
                    .map_err(|_| Status::failed_precondition("reentrant codec"))?
                    .decode(message.payload())
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                self.check_deadline()?;
                return Ok(Some(value));
            }
            if inner.session.is_remote_end_stream(self.id)? {
                return Ok(None);
            }
            if attempt == 0 {
                inner.drive(false, self.deadline)?;
            }
        }
        if inner.closed {
            Err(Status::unavailable("transport EOF during request").into())
        } else {
            Err(StreamError::WouldBlock)
        }
    }
    fn wait_ready(&mut self) -> Result<(), StreamError> {
        self.check_deadline()?;
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant server operation"))?;
        inner.drive(true, self.deadline)?;
        if inner.closed {
            Err(Status::unavailable("transport EOF during request").into())
        } else {
            Ok(())
        }
    }
    fn finish(&mut self, status: &Status, trailers: &Metadata) -> Result<(), StreamError> {
        self.check_deadline()?;
        if self.complete.get() {
            return Err(Status::failed_precondition("response already finished").into());
        }
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| Status::failed_precondition("reentrant server operation"))?;
        inner.transport.set_nonblocking(true).map_err(io_status)?;
        inner.session.finish_response(self.id, status, trailers)?;
        self.complete.set(true);
        inner.pending_reap.insert(self.id);
        self.trailers = Some(trailers.clone());
        inner.flush()?;
        Ok(())
    }
    fn initial_metadata(&self) -> Option<&Metadata> {
        Some(&self.initial)
    }
    fn trailing_metadata(&self) -> Option<&Metadata> {
        self.trailers.as_ref()
    }
    fn cancel(&mut self) {
        if self.deadline.is_some_and(|at| Instant::now() >= at) {
            match self.check_deadline() {
                Err(_) if self.complete.get() => return,
                Err(_) | Ok(()) => {}
            }
        }
        if !self.complete.get()
            && let Ok(mut inner) = self.inner.try_borrow_mut()
        {
            match inner.session.cancel_stream(self.id, 8) {
                Ok(()) => {
                    self.complete.set(true);
                    inner.pending_reap.insert(self.id);
                    inner.reset_reap.insert(self.id);
                }
                Err(error)
                    if matches!(
                        error.code(),
                        grpc_core::Code::Cancelled | grpc_core::Code::FailedPrecondition
                    ) =>
                {
                    self.complete.set(true);
                    inner.pending_reap.insert(self.id);
                    inner.reset_reap.insert(self.id);
                }
                Err(error) if error.code() == grpc_core::Code::NotFound => self.complete.set(true),
                Err(_) => inner.closed = true,
            }
        }
    }
}

type DriverState<T, C> = (ServerStreamDriver<T, C>, Rc<Cell<bool>>);

type Route<T> = Box<dyn FnMut(IncomingRequest, Rc<RefCell<ServerInner<T>>>) -> Result<(), Status>>;
/// Synchronous single-connection server with path-based service registration.
pub struct SyncServer<T: SyncTransport> {
    inner: Rc<RefCell<ServerInner<T>>>,
    routes: BTreeMap<String, Route<T>>,
}
impl<T: SyncTransport + 'static> SyncServer<T> {
    /// Creates a server with bounded default HTTP/2 session settings.
    /// # Errors
    /// Returns invalid settings or native initialization failures.
    pub fn new(transport: T) -> Result<Self, Status> {
        Self::with_config(transport, SessionConfig::default())
    }
    /// Creates a connection with explicit HTTP/2 and message limits.
    /// # Errors
    /// Returns invalid settings or native initialization failures.
    pub fn with_config(transport: T, config: SessionConfig) -> Result<Self, Status> {
        Ok(Self {
            inner: Rc::new(RefCell::new(ServerInner {
                session: GrpcServerSession::new(config)?,
                transport,
                buffer: vec![0; 16 * 1024],
                pending_incoming: VecDeque::new(),
                pending_reap: BTreeSet::new(),
                reset_reap: BTreeSet::new(),
                closed: false,
                draining: false,
            })),
            routes: BTreeMap::new(),
        })
    }
    fn insert(&mut self, path: &str, handler: Route<T>) -> Result<(), Status> {
        if !path.starts_with('/')
            || path
                .rsplit_once('/')
                .is_none_or(|(service, method)| service.len() < 2 || method.is_empty())
        {
            return Err(Status::invalid_argument(
                "RPC path must contain service and method",
            ));
        }
        if self.routes.contains_key(path) {
            return Err(Status::already_exists("RPC path is already registered"));
        }
        self.routes.insert(path.to_string(), handler);
        Ok(())
    }
    fn streams<C: Codec>(
        request: IncomingRequest,
        inner: Rc<RefCell<ServerInner<T>>>,
        codec: Rc<RefCell<C>>,
    ) -> Result<DriverState<T, C>, Status> {
        let content_type = request.content_type.split(';').next().unwrap_or("").trim();
        if normalized_content_type(content_type) != normalized_content_type(C::CONTENT_TYPE) {
            return Err(Status::unimplemented(
                "request content type does not match registered codec",
            ));
        }
        let deadline = request
            .timeout
            .map(|timeout| {
                Instant::now()
                    .checked_add(timeout)
                    .ok_or_else(|| Status::invalid_argument("request deadline exceeds clock range"))
            })
            .transpose()?;
        let encoding = if request
            .accept_encoding
            .split(',')
            .any(|name| name.trim() == request.request_encoding.as_str())
        {
            request.request_encoding
        } else {
            CompressionAlgorithm::Identity
        };
        let complete = Rc::new(Cell::new(false));
        Ok((
            ServerStreamDriver {
                inner,
                codec,
                id: request.stream_id,
                deadline,
                initial: request.initial_metadata,
                trailers: None,
                encoding,
                response_initial_sent: false,
                complete: Rc::clone(&complete),
            },
            complete,
        ))
    }
    fn finalize<Resp>(
        sink: &mut ResponseSink<Resp>,
        complete: &Cell<bool>,
        result: Result<(), Status>,
    ) -> Result<(), Status> {
        if complete.get() {
            return Ok(());
        }
        let status = match result {
            Ok(()) => Status::ok(""),
            Err(status) => status,
        };
        let result = sink.finish(status, Metadata::new()).map_err(stream_status);
        if complete.get() { Ok(()) } else { result }
    }
    /// Registers a unary handler with exactly one inbound and outbound message.
    /// # Errors
    /// Rejects invalid paths or duplicate registrations.
    pub fn register_unary<C, Req, Resp, F>(
        &mut self,
        path: &str,
        codec: C,
        mut handler: F,
    ) -> Result<(), Status>
    where
        C: Codec<EncodeItem = Resp, DecodeItem = Req> + 'static,
        Req: 'static,
        Resp: 'static,
        F: FnMut(Request<Req>) -> Result<Response<Resp>, Status> + 'static,
    {
        let codec = Rc::new(RefCell::new(codec));
        self.insert(
            path,
            Box::new(move |request, inner| {
                let metadata = request.initial_metadata.clone();
                let (mut driver, complete) =
                    Self::streams(request, Rc::clone(&inner), Rc::clone(&codec))?;
                driver.response_initial_sent = true;
                let id = driver.id;
                let encoding = driver.encoding;
                let (mut requests, mut sink) = server_streams(driver);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let value = requests
                        .recv_next()
                        .map_err(stream_status)?
                        .ok_or_else(|| Status::invalid_argument("unary request is missing"))?;
                    if requests.recv_next().map_err(stream_status)?.is_some() {
                        return Err(Status::invalid_argument(
                            "unary request has multiple messages",
                        ));
                    }
                    let response = handler(Request::from_parts(metadata, value))?;
                    let (metadata, value) = response.into_parts();
                    inner
                        .borrow_mut()
                        .session
                        .send_initial_metadata_with_content_type(
                            id,
                            &metadata,
                            encoding,
                            C::CONTENT_TYPE,
                        )?;
                    sink.send(&value).map_err(stream_status)
                }))
                .unwrap_or_else(|_| Err(Status::internal("service handler panicked")));
                Self::finalize(&mut sink, &complete, result)
            }),
        )
    }
    /// Registers a single-request streaming-response handler.
    /// The server sends OK trailers if the handler has not already finished its sink.
    /// # Errors
    /// Rejects invalid paths or duplicate registrations.
    pub fn register_server_streaming<C, Req, Resp, F>(
        &mut self,
        path: &str,
        codec: C,
        mut handler: F,
    ) -> Result<(), Status>
    where
        C: Codec<EncodeItem = Resp, DecodeItem = Req> + 'static,
        Req: 'static,
        Resp: 'static,
        F: FnMut(Request<Req>, &mut ResponseSink<Resp>) -> Result<(), Status> + 'static,
    {
        let codec = Rc::new(RefCell::new(codec));
        self.insert(
            path,
            Box::new(move |request, inner| {
                let metadata = request.initial_metadata.clone();
                let (driver, complete) = Self::streams(request, inner, Rc::clone(&codec))?;
                let (mut requests, mut sink) = server_streams(driver);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let value = requests
                        .recv_next()
                        .map_err(stream_status)?
                        .ok_or_else(|| Status::invalid_argument("request is missing"))?;
                    if requests.recv_next().map_err(stream_status)?.is_some() {
                        return Err(Status::invalid_argument("request has multiple messages"));
                    }
                    handler(Request::from_parts(metadata, value), &mut sink)
                }))
                .unwrap_or_else(|_| Err(Status::internal("service handler panicked")));
                Self::finalize(&mut sink, &complete, result)
            }),
        )
    }
    /// Registers a streaming-request handler returning exactly one response.
    /// # Errors
    /// Rejects invalid paths or duplicate registrations.
    pub fn register_client_streaming<C, Req, Resp, F>(
        &mut self,
        path: &str,
        codec: C,
        mut handler: F,
    ) -> Result<(), Status>
    where
        C: Codec<EncodeItem = Resp, DecodeItem = Req> + 'static,
        Req: 'static,
        Resp: 'static,
        F: FnMut(RequestStream<Req>) -> Result<Response<Resp>, Status> + 'static,
    {
        let codec = Rc::new(RefCell::new(codec));
        self.insert(
            path,
            Box::new(move |request, inner| {
                let (mut driver, complete) =
                    Self::streams(request, Rc::clone(&inner), Rc::clone(&codec))?;
                driver.response_initial_sent = true;
                let id = driver.id;
                let encoding = driver.encoding;
                let (requests, mut sink) = server_streams(driver);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let (metadata, value) = handler(requests)?.into_parts();
                    inner
                        .borrow_mut()
                        .session
                        .send_initial_metadata_with_content_type(
                            id,
                            &metadata,
                            encoding,
                            C::CONTENT_TYPE,
                        )?;
                    sink.send(&value).map_err(stream_status)
                }))
                .unwrap_or_else(|_| Err(Status::internal("service handler panicked")));
                Self::finalize(&mut sink, &complete, result)
            }),
        )
    }
    /// Registers a bidirectional streaming handler.
    /// # Errors
    /// Rejects invalid paths or duplicate registrations.
    pub fn register_bidi_streaming<C, Req, Resp, F>(
        &mut self,
        path: &str,
        codec: C,
        mut handler: F,
    ) -> Result<(), Status>
    where
        C: Codec<EncodeItem = Resp, DecodeItem = Req> + 'static,
        Req: 'static,
        Resp: 'static,
        F: FnMut(RequestStream<Req>, &mut ResponseSink<Resp>) -> Result<(), Status> + 'static,
    {
        let codec = Rc::new(RefCell::new(codec));
        self.insert(
            path,
            Box::new(move |request, inner| {
                let (driver, complete) = Self::streams(request, inner, Rc::clone(&codec))?;
                let (requests, mut sink) = server_streams(driver);
                let result = catch_unwind(AssertUnwindSafe(|| handler(requests, &mut sink)))
                    .unwrap_or_else(|_| Err(Status::internal("service handler panicked")));
                Self::finalize(&mut sink, &complete, result)
            }),
        )
    }
    /// Registers a raw-payload dispatcher, including generated-service adapters.
    /// # Errors
    /// Rejects invalid paths or duplicate registrations.
    pub fn register_service_handler<H: ServiceHandler + 'static>(
        &mut self,
        path: &str,
        mut handler: H,
    ) -> Result<(), Status> {
        self.register_bidi_streaming(path, PayloadCodec, move |requests, sink| {
            handler.handle(requests, sink)
        })
    }
    fn dispatch(&mut self) -> Result<bool, Status> {
        let request = self.inner.borrow_mut().pending_incoming.pop_front();
        let Some((received, mut request)) = request else {
            return Ok(false);
        };
        request.timeout = request
            .timeout
            .map(|timeout| timeout.saturating_sub(received.elapsed()));
        let id = request.stream_id;
        if request.timeout.is_some_and(|timeout| timeout.is_zero()) {
            let mut inner = self.inner.borrow_mut();
            inner.session.finish_response(
                id,
                &Status::deadline_exceeded("request deadline elapsed"),
                &Metadata::new(),
            )?;
            inner.pending_reap.insert(id);
            inner.flush()?;
            return Ok(true);
        }
        if let Some(handler) = self.routes.get_mut(&request.path) {
            let result = handler(request, Rc::clone(&self.inner));
            if let Err(status) = result {
                let mut inner = self.inner.borrow_mut();
                if inner
                    .session
                    .finish_response(id, &status, &Metadata::new())
                    .is_err()
                {
                    inner.session.cancel_stream(id, 2)?;
                    inner.reset_reap.insert(id);
                }
                inner.pending_reap.insert(id);
            }
        } else {
            let mut inner = self.inner.borrow_mut();
            inner.session.finish_response(
                id,
                &Status::unimplemented("RPC path is not registered"),
                &Metadata::new(),
            )?;
            inner.pending_reap.insert(id);
        }
        self.inner.borrow_mut().flush()?;
        Ok(true)
    }
    /// Advances nonblocking socket I/O and dispatches at most one synchronous handler.
    /// A dispatched handler may wait for its request body on the calling thread.
    /// # Errors
    /// Returns native HTTP/2 or transport failures.
    pub fn poll_once(&mut self) -> Result<bool, Status> {
        self.inner
            .borrow_mut()
            .transport
            .set_nonblocking(true)
            .map_err(io_status)?;
        if self.inner.borrow().closed {
            return Ok(false);
        }
        if self.dispatch()? {
            return Ok(true);
        }
        let progress = self.inner.borrow_mut().drive(false, None)? > 0;
        Ok(self.dispatch()? || progress)
    }
    /// Serves until peer EOF or local graceful shutdown.
    /// # Errors
    /// Returns connection-level HTTP/2, transport or handler completion failures.
    pub fn serve_connection(&mut self) -> Result<(), Status> {
        loop {
            if self.inner.borrow().closed {
                return Ok(());
            }
            if self.dispatch()? {
                continue;
            }
            {
                let mut inner = self.inner.borrow_mut();
                if inner.draining || inner.session.goaway_received().is_some() {
                    inner.transport.set_nonblocking(true).map_err(io_status)?;
                    inner.flush()?;
                    if !inner.session.want_write() && inner.pending_reap.is_empty() {
                        inner.closed = true;
                        return Ok(());
                    }
                }
                inner.drive(true, None)?;
            }
        }
    }
    /// Queues GOAWAY and attempts a nonblocking flush.
    /// Continue polling or serving to drain accepted requests and pending output.
    /// # Errors
    /// Returns HTTP/2 submission or transport failures.
    pub fn shutdown_with_goaway(&mut self, error_code: u32) -> Result<(), Status> {
        let mut inner = self.inner.borrow_mut();
        inner.session.submit_goaway(error_code)?;
        inner.draining = true;
        inner.transport.set_nonblocking(true).map_err(io_status)?;
        inner.flush()?;
        Ok(())
    }
}
