//! Synchronous streaming calls over a caller-thread I/O driver.
//! Drivers handle transport, framing and codecs; handles enforce RPC lifecycle
//! and cardinality without creating threads or an executor.
use std::{cell::RefCell, rc::Rc};

use grpc_core::{Metadata, Status};
/// A pending operation or a terminal RPC failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StreamError {
    /// No message or transport capacity is available without waiting.
    #[error("stream would block")]
    WouldBlock,
    /// Terminal local, transport or remote gRPC status.
    #[error("{0}")]
    Status(#[from] Status),
}
type Result<T> = std::result::Result<T, StreamError>;
fn invalid(message: &str) -> StreamError {
    Status::failed_precondition(message).into()
}
/// Typed, single-threaded transport seam for one RPC.
///
/// `WouldBlock` means no message, send, half-close or finish was committed; a
/// retry must not duplicate a frame. All methods except `wait_ready` are bounded
/// nonblocking steps. `wait_ready` waits for actual I/O readiness or a deadline
/// on the caller thread; it must not busy-spin. Errors other than `WouldBlock`
/// terminate the call. Before returning client response EOF, validate grpc-status
/// and expose trailers (including an empty collection); non-OK status is an error.
/// Request EOF on a server only half-closes its inbound direction.
///
/// Metadata remains available after cancellation. `cancel` releases local stream
/// resources without waiting, cannot panic, and does not promise peer acknowledgement.
pub trait StreamDriver {
    type Send;
    type Receive;
    /// Attempts to commit one outgoing message.
    /// # Errors
    /// Returns pending without accepting the message, or a terminal failure.
    fn send(&mut self, message: &Self::Send) -> Result<()>;
    /// Attempts to half-close outgoing messages.
    /// # Errors
    /// Returns pending without closing, or a terminal failure.
    fn half_close(&mut self) -> Result<()>;
    /// Receives a message, validated EOF, or a pending/terminal error.
    /// # Errors
    /// Returns `WouldBlock` while pending; all other errors are terminal.
    fn try_recv_next(&mut self) -> Result<Option<Self::Receive>>;
    /// Waits for readiness needed by the last pending operation.
    /// # Errors
    /// Returns a terminal transport/deadline failure; must not return `WouldBlock`.
    fn wait_ready(&mut self) -> Result<()>;
    /// Attempts server completion, including non-OK trailers-only responses.
    /// # Errors
    /// Returns pending without committing completion, or a terminal failure.
    fn finish(&mut self, status: &Status, trailers: &Metadata) -> Result<()>;
    fn initial_metadata(&self) -> Option<&Metadata>;
    fn trailing_metadata(&self) -> Option<&Metadata>;
    fn cancel(&mut self);
}
struct State<D: StreamDriver> {
    driver: D,
    error: Option<StreamError>,
    receive_closed: bool,
    send_closed: bool,
    complete: bool,
    client: bool,
}
impl<D: StreamDriver> State<D> {
    fn fail(&mut self, error: StreamError) -> StreamError {
        self.error = Some(error.clone());
        if !self.complete {
            self.driver.cancel();
        }
        self.complete = true;
        error
    }
    fn check(&self) -> Result<()> {
        self.error.clone().map_or(Ok(()), Err)
    }
    fn accept<T>(&mut self, result: Result<T>) -> Result<T> {
        match result {
            Err(StreamError::WouldBlock) => Err(StreamError::WouldBlock),
            Err(e) => Err(self.fail(e)),
            Ok(value) => Ok(value),
        }
    }
    fn poll(&mut self) -> Result<Option<D::Receive>> {
        self.check()?;
        if self.receive_closed {
            return Ok(None);
        }
        let result = self.driver.try_recv_next();
        let message = self.accept(result)?;
        if message.is_none() {
            if self.client && self.driver.trailing_metadata().is_none() {
                return Err(
                    self.fail(Status::unknown("response ended without validated trailers").into())
                );
            }
            self.receive_closed = true;
            if self.client {
                if !self.send_closed {
                    self.driver.cancel();
                }
                (self.complete, self.send_closed) = (true, true);
            }
        }
        Ok(message)
    }
    fn send(&mut self, value: &D::Send) -> Result<()> {
        self.check()?;
        if self.send_closed {
            return Err(invalid("send direction is closed"));
        }
        let result = self.driver.send(value);
        self.accept(result)
    }
    fn close(&mut self) -> Result<()> {
        self.check()?;
        if self.send_closed {
            return Ok(());
        }
        let result = self.driver.half_close();
        self.accept(result)?;
        self.send_closed = true;
        Ok(())
    }
    fn finish(&mut self, status: &Status, trailers: &Metadata) -> Result<()> {
        self.check()?;
        if self.complete {
            return Err(invalid("RPC is already finished"));
        }
        let result = self.driver.finish(status, trailers);
        self.accept(result)?;
        (self.complete, self.send_closed, self.receive_closed) = (true, true, true);
        Ok(())
    }
    fn wait(&mut self) -> Result<()> {
        self.check()?;
        match self.driver.wait_ready() {
            Ok(()) => Ok(()),
            Err(StreamError::WouldBlock) => {
                Err(self.fail(Status::internal("driver wait made no progress").into()))
            }
            Err(e) => Err(self.fail(e)),
        }
    }
    fn cancel(&mut self) {
        if !self.complete {
            self.fail(Status::cancelled("call cancelled").into());
        }
    }
}
impl<D: StreamDriver> Drop for State<D> {
    fn drop(&mut self) {
        if !self.complete {
            self.driver.cancel();
        }
    }
}
trait Control {
    fn fail(&mut self, error: StreamError) -> Result<()>;
    fn wait(&mut self) -> Result<()>;
    fn metadata(&self, trailing: bool) -> Result<Option<Metadata>>;
    fn cancel(&mut self) -> Result<()>;
}
trait Receive<T>: Control {
    fn poll(&mut self) -> Result<Option<T>>;
}
trait Send<T>: Control {
    fn send(&mut self, value: &T) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn finish(&mut self, status: &Status, trailers: &Metadata) -> Result<()>;
}
struct Handle<D: StreamDriver>(Rc<RefCell<State<D>>>);
impl<D: StreamDriver> Handle<D> {
    fn with<T>(&self, f: impl FnOnce(&mut State<D>) -> Result<T>) -> Result<T> {
        let mut state = self
            .0
            .try_borrow_mut()
            .map_err(|_| invalid("reentrant stream operation"))?;
        f(&mut state)
    }
}
impl<D: StreamDriver> Control for Handle<D> {
    fn fail(&mut self, error: StreamError) -> Result<()> {
        self.with(|state| {
            state.fail(error);
            Ok(())
        })
    }
    fn wait(&mut self) -> Result<()> {
        self.with(State::wait)
    }
    fn metadata(&self, trailing: bool) -> Result<Option<Metadata>> {
        let state = self
            .0
            .try_borrow()
            .map_err(|_| invalid("reentrant metadata access"))?;
        Ok(if trailing {
            state.driver.trailing_metadata()
        } else {
            state.driver.initial_metadata()
        }
        .cloned())
    }
    fn cancel(&mut self) -> Result<()> {
        self.with(|state| {
            state.cancel();
            Ok(())
        })
    }
}
impl<D: StreamDriver> Receive<D::Receive> for Handle<D> {
    fn poll(&mut self) -> Result<Option<D::Receive>> {
        self.with(State::poll)
    }
}
impl<D: StreamDriver> Send<D::Send> for Handle<D> {
    fn send(&mut self, value: &D::Send) -> Result<()> {
        self.with(|s| s.send(value))
    }
    fn close(&mut self) -> Result<()> {
        self.with(State::close)
    }
    fn finish(&mut self, status: &Status, trailers: &Metadata) -> Result<()> {
        self.with(|s| s.finish(status, trailers))
    }
}
type Shared<D> = Rc<RefCell<State<D>>>;
fn shared<D: StreamDriver>(driver: D, client: bool, send_closed: bool) -> Shared<D> {
    Rc::new(RefCell::new(State {
        driver,
        error: None,
        receive_closed: false,
        send_closed,
        complete: false,
        client,
    }))
}
fn blocking<T, C: Control + ?Sized>(
    control: &mut C,
    mut step: impl FnMut(&mut C) -> Result<T>,
) -> Result<T> {
    loop {
        match step(control) {
            Err(StreamError::WouldBlock) => control.wait()?,
            result => return result,
        }
    }
}
macro_rules! control_methods {
    ($($field:ident).+) => {
        /// Copies received initial metadata, if available.
        /// # Errors
        /// Rejects reentrant access from a driver callback.
        pub fn initial_metadata(&self) -> Result<Option<Metadata>> {
            self.$($field).+.metadata(false)
        }
        /// Copies terminal metadata, including on a remote failure.
        /// # Errors
        /// Rejects reentrant access from a driver callback.
        pub fn trailing_metadata(&self) -> Result<Option<Metadata>> {
            self.$($field).+.metadata(true)
        }
        /// Cancels local resources without waiting for peer acknowledgement.
        /// # Errors
        /// Rejects reentrant access from a driver callback.
        pub fn cancel(&mut self) -> Result<()> { self.$($field).+.cancel() }
    };
}
macro_rules! send_methods {
    ($message:ty, $($field:ident).+) => {
        /// Sends a message, waiting for transport capacity.
        /// # Errors
        /// Returns a closed-direction or terminal failure.
        pub fn send(&mut self, value: &$message) -> Result<()> {
            blocking(self.$($field).+.as_mut(), |sender| sender.send(value))
        }
        /// Attempts a send; pending means the message was not accepted.
        /// # Errors
        /// Returns pending, closed-direction or terminal failure.
        pub fn try_send(&mut self, value: &$message) -> Result<()> { self.$($field).+.send(value) }
    };
}
macro_rules! close_methods {
    ($($field:ident).+) => {
        /// Idempotently half-closes requests while allowing further responses.
        /// # Errors
        /// Returns a terminal failure.
        pub fn half_close(&mut self) -> Result<()> {
            blocking(self.$($field).+.as_mut(), |sender| sender.close())
        }
        /// Attempts request half-close without waiting.
        /// # Errors
        /// Returns pending or terminal failure.
        pub fn try_half_close(&mut self) -> Result<()> { self.$($field).+.close() }
    };
}
/// Receiving half of a server-streaming or bidirectional client call.
pub struct ServerStreamingCall<T> {
    receiver: Box<dyn Receive<T>>,
}
impl<T: 'static> ServerStreamingCall<T> {
    /// Wraps a call whose unary request was sent and half-closed by its driver.
    pub fn new<D: StreamDriver<Receive = T> + 'static>(driver: D) -> Self {
        Self {
            receiver: Box::new(Handle(shared(driver, true, true))),
        }
    }
    /// Performs one nonblocking receive attempt.
    /// # Errors
    /// Returns pending or the sticky terminal failure; EOF is `Ok(None)`.
    pub fn try_recv_next(&mut self) -> Result<Option<T>> {
        self.receiver.poll()
    }
    /// Receives the next message, waiting for readiness on the caller thread.
    /// # Errors
    /// Returns a terminal RPC, transport or deadline failure.
    pub fn recv_next(&mut self) -> Result<Option<T>> {
        blocking(self.receiver.as_mut(), |receiver| receiver.poll())
    }
    /// Drains and returns remaining messages through validated completion.
    /// # Errors
    /// Returns a terminal failure, including missing successful trailers.
    pub fn finish(&mut self) -> Result<Vec<T>> {
        let mut messages = Vec::new();
        while let Some(message) = self.recv_next()? {
            messages.push(message);
        }
        Ok(messages)
    }
    control_methods!(receiver);
}
/// Inbound server requests, created together with a response sink by [`server_streams`].
/// Request EOF half-closes inbound messages without closing the response sink.
pub struct RequestStream<Req>(ServerStreamingCall<Req>);
impl<Req: 'static> RequestStream<Req> {
    /// Wraps inbound requests without treating request EOF as response completion.
    pub fn new<D: StreamDriver<Receive = Req> + 'static>(driver: D) -> Self {
        Self(ServerStreamingCall {
            receiver: Box::new(Handle(shared(driver, false, false))),
        })
    }
}
impl<Req> std::ops::Deref for RequestStream<Req> {
    type Target = ServerStreamingCall<Req>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<Req> std::ops::DerefMut for RequestStream<Req> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
/// Sending half of a server response stream.
pub struct ResponseSink<T> {
    sender: Box<dyn Send<T>>,
}
impl<T> ResponseSink<T> {
    send_methods!(T, sender);
    /// Attempts completion without waiting or consuming trailers on backpressure.
    /// # Errors
    /// Returns pending, a terminal error or an already-finished error.
    pub fn try_finish(&mut self, status: &Status, trailers: &Metadata) -> Result<()> {
        self.sender.finish(status, trailers)
    }
    /// Sends final status and trailers, including a trailers-only error response.
    /// # Errors
    /// Returns a terminal failure or rejects an already finished RPC.
    pub fn finish(&mut self, status: Status, trailers: Metadata) -> Result<()> {
        blocking(self.sender.as_mut(), |sender| {
            sender.finish(&status, &trailers)
        })
    }
    control_methods!(sender);
}
/// Creates server halves sharing a driver; only the last unfinished half's drop cancels.
pub fn server_streams<D: StreamDriver + 'static>(
    driver: D,
) -> (RequestStream<D::Receive>, ResponseSink<D::Send>) {
    let state = shared(driver, false, false);
    (
        RequestStream(ServerStreamingCall {
            receiver: Box::new(Handle(Rc::clone(&state))),
        }),
        ResponseSink {
            sender: Box::new(Handle(state)),
        },
    )
}
/// Independent request and response directions for a bidirectional client call.
pub struct BidiStreamingCall<Req, Resp> {
    incoming: ServerStreamingCall<Resp>,
    outgoing: ResponseSink<Req>,
}
impl<Req: 'static, Resp: 'static> BidiStreamingCall<Req, Resp> {
    pub fn new<D: StreamDriver<Send = Req, Receive = Resp> + 'static>(driver: D) -> Self {
        let state = shared(driver, true, false);
        Self {
            incoming: ServerStreamingCall {
                receiver: Box::new(Handle(Rc::clone(&state))),
            },
            outgoing: ResponseSink {
                sender: Box::new(Handle(state)),
            },
        }
    }
    send_methods!(Req, outgoing.sender);
    close_methods!(outgoing.sender);
    /// Receives the next response without blocking.
    /// # Errors
    /// Returns pending or sticky terminal failure.
    pub fn try_recv_next(&mut self) -> Result<Option<Resp>> {
        self.incoming.try_recv_next()
    }
    /// Waits for the next response.
    /// # Errors
    /// Returns terminal failure.
    pub fn recv_next(&mut self) -> Result<Option<Resp>> {
        self.incoming.recv_next()
    }
    /// Half-closes requests and returns all remaining responses through completion.
    /// # Errors
    /// Returns terminal failure.
    pub fn finish(&mut self) -> Result<Vec<Resp>> {
        self.half_close()?;
        self.incoming.finish()
    }
    control_methods!(incoming.receiver);
}
/// Client-streaming requests with exactly one response after successful completion.
pub struct ClientStreamingCall<Req, Resp> {
    call: BidiStreamingCall<Req, Resp>,
    response: Option<Resp>,
    delivered: bool,
}
impl<Req: 'static, Resp: 'static> ClientStreamingCall<Req, Resp> {
    pub fn new<D: StreamDriver<Send = Req, Receive = Resp> + 'static>(driver: D) -> Self {
        Self {
            call: BidiStreamingCall::new(driver),
            response: None,
            delivered: false,
        }
    }
    send_methods!(Req, call.outgoing.sender);
    close_methods!(call.outgoing.sender);
    /// Advances completion by at most two receive steps, preserving a pending response.
    /// # Errors
    /// Returns pending, terminal error, or response cardinality failure.
    pub fn try_finish(&mut self) -> Result<Resp> {
        if self.delivered {
            return Err(invalid("response already delivered"));
        }
        let result = self.poll_finish();
        if let Err(error) = &result
            && *error != StreamError::WouldBlock
        {
            self.call.incoming.receiver.fail(error.clone())?;
        }
        result
    }
    fn poll_finish(&mut self) -> Result<Resp> {
        self.call.try_half_close()?;
        if self.response.is_none() {
            match self.call.try_recv_next()? {
                Some(message) => self.response = Some(message),
                None => return Err(Status::internal("missing client-streaming response").into()),
            }
        }
        match self.call.try_recv_next()? {
            Some(_) => Err(Status::internal("multiple client-streaming responses").into()),
            None => {
                self.delivered = true;
                self.response
                    .take()
                    .ok_or_else(|| Status::internal("missing client-streaming response").into())
            }
        }
    }
    /// Polls the single response through validated final status.
    /// # Errors
    /// Returns the same pending or terminal errors as [`Self::try_finish`].
    pub fn try_recv_next(&mut self) -> Result<Option<Resp>> {
        (!self.delivered).then(|| self.try_finish()).transpose()
    }
    /// Waits for the single response, then returns EOF on subsequent reads.
    /// # Errors
    /// Returns the terminal errors from [`Self::finish`].
    pub fn recv_next(&mut self) -> Result<Option<Resp>> {
        (!self.delivered).then(|| self.finish()).transpose()
    }
    /// Waits for exactly one response and a successful final status.
    /// # Errors
    /// Returns terminal failure or response cardinality failure.
    pub fn finish(&mut self) -> Result<Resp> {
        loop {
            match self.try_finish() {
                Err(StreamError::WouldBlock) => self.call.incoming.receiver.wait()?,
                result => return result,
            }
        }
    }
    control_methods!(call.incoming.receiver);
}
#[cfg(test)]
mod unit_lifecycle_scenario {
    use std::collections::VecDeque;

    use super::*;
    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
    #[derive(Default)]
    struct Journal {
        ops: Vec<&'static str>,
        accepted: Vec<i32>,
        finished: Vec<(Status, Metadata)>,
    }
    struct Script {
        incoming: VecDeque<Result<Option<i32>>>,
        sends: VecDeque<Result<()>>,
        closes: VecDeque<Result<()>>,
        finishes: VecDeque<Result<()>>,
        waits: VecDeque<Result<()>>,
        initial: Metadata,
        trailers: Option<Metadata>,
        send_trailers: bool,
        log: Rc<RefCell<Journal>>,
    }
    impl Script {
        fn new(events: Vec<Result<Option<i32>>>) -> (Self, Rc<RefCell<Journal>>) {
            let log = Rc::new(RefCell::new(Journal::default()));
            (
                Self {
                    incoming: events.into(),
                    sends: VecDeque::new(),
                    closes: VecDeque::new(),
                    finishes: VecDeque::new(),
                    waits: VecDeque::new(),
                    initial: Metadata::new(),
                    trailers: None,
                    send_trailers: true,
                    log: Rc::clone(&log),
                },
                log,
            )
        }
        fn attempt(queue: &mut VecDeque<Result<()>>) -> Result<()> {
            queue.pop_front().transpose().map(|_| ())
        }
        fn record(&self, operation: &'static str) {
            self.log.borrow_mut().ops.push(operation);
        }
    }
    impl StreamDriver for Script {
        type Send = i32;
        type Receive = i32;
        fn send(&mut self, value: &i32) -> Result<()> {
            self.record("send");
            Self::attempt(&mut self.sends)?;
            self.log.borrow_mut().accepted.push(*value);
            Ok(())
        }
        fn half_close(&mut self) -> Result<()> {
            self.record("close");
            Self::attempt(&mut self.closes)
        }
        fn try_recv_next(&mut self) -> Result<Option<i32>> {
            self.record("recv");
            let result = self
                .incoming
                .pop_front()
                .ok_or_else(|| invalid("script exhausted"))?;
            if self.send_trailers && matches!(&result, Ok(None) | Err(StreamError::Status(_))) {
                self.trailers = Some(Metadata::new());
            }
            result
        }
        fn wait_ready(&mut self) -> Result<()> {
            self.record("wait");
            Self::attempt(&mut self.waits)
        }
        fn finish(&mut self, status: &Status, trailers: &Metadata) -> Result<()> {
            self.record("finish");
            Self::attempt(&mut self.finishes)?;
            self.log
                .borrow_mut()
                .finished
                .push((status.clone(), trailers.clone()));
            Ok(())
        }
        fn initial_metadata(&self) -> Option<&Metadata> {
            Some(&self.initial)
        }
        fn trailing_metadata(&self) -> Option<&Metadata> {
            self.trailers.as_ref()
        }
        fn cancel(&mut self) {
            self.record("cancel");
        }
    }
    #[test]
    fn server_stream_pending_metadata_and_completion() -> TestResult {
        let (mut driver, log) =
            Script::new(vec![Err(StreamError::WouldBlock), Ok(Some(7)), Ok(None)]);
        driver.initial.append_ascii("tag", "a")?;
        driver.initial.append_ascii("tag", "b")?;
        driver.initial.append_binary("raw-bin", &[0, 255])?;
        let initial = driver.initial.clone();
        let mut call = ServerStreamingCall::new(driver);
        assert_eq!(call.try_recv_next(), Err(StreamError::WouldBlock));
        assert_eq!(log.borrow().ops, ["recv"]);
        assert_eq!(call.initial_metadata()?, Some(initial.clone()));
        assert_eq!(call.trailing_metadata()?, None);
        assert_eq!(call.recv_next()?, Some(7));
        assert_eq!(call.recv_next()?, None);
        assert_eq!(call.try_recv_next()?, None);
        assert_eq!(call.initial_metadata()?, Some(initial));
        assert_eq!(call.trailing_metadata()?, Some(Metadata::new()));
        drop(call);
        assert_eq!(log.borrow().ops, ["recv", "recv", "recv"]);
        Ok(())
    }
    #[test]
    fn blocking_receive_waits_and_failure_is_sticky() -> TestResult {
        let failure = StreamError::from(Status::unavailable("offline"));
        let (driver, log) = Script::new(vec![
            Err(StreamError::WouldBlock),
            Ok(Some(3)),
            Err(failure.clone()),
            Ok(Some(9)),
        ]);
        let mut call = ServerStreamingCall::new(driver);
        assert_eq!(call.recv_next()?, Some(3));
        assert_eq!(log.borrow().ops, ["recv", "wait", "recv"]);
        assert_eq!(call.recv_next(), Err(failure.clone()));
        assert_eq!(call.try_recv_next(), Err(failure));
        assert_eq!(call.trailing_metadata()?, Some(Metadata::new()));
        call.cancel()?;
        drop(call);
        assert_eq!(log.borrow().ops, ["recv", "wait", "recv", "recv", "cancel"]);
        Ok(())
    }
    #[test]
    fn client_response_waits_for_status_and_enforces_cardinality() -> TestResult {
        let (driver, log) = Script::new(vec![Ok(Some(8)), Err(StreamError::WouldBlock), Ok(None)]);
        let mut call = ClientStreamingCall::new(driver);
        assert_eq!(call.try_recv_next(), Err(StreamError::WouldBlock));
        assert_eq!(call.recv_next()?, Some(8));
        assert_eq!(call.try_recv_next()?, None);
        assert!(call.try_finish().is_err());
        drop(call);
        assert_eq!(log.borrow().ops, ["close", "recv", "recv", "recv"]);
        for events in [
            vec![Ok(None)],
            vec![Ok(Some(1)), Ok(Some(2))],
            vec![Ok(Some(1)), Err(Status::unavailable("late status").into())],
        ] {
            let (driver, _) = Script::new(events);
            let mut call = ClientStreamingCall::new(driver);
            let first = call.try_finish();
            assert!(matches!(first, Err(StreamError::Status(_))));
            assert_eq!(call.try_finish(), first);
            assert_eq!(call.try_send(&9).err(), first.clone().err());
            assert_eq!(call.try_half_close().err(), first.err());
        }
        Ok(())
    }
    #[test]
    fn blocking_client_finish_waits_only_for_pending_io() -> TestResult {
        let (mut driver, log) =
            Script::new(vec![Err(StreamError::WouldBlock), Ok(Some(4)), Ok(None)]);
        driver.closes.push_back(Err(StreamError::WouldBlock));
        let mut call = ClientStreamingCall::new(driver);
        call.send(&1)?;
        assert_eq!(call.finish()?, 4);
        assert_eq!(log.borrow().accepted, [1]);
        assert_eq!(
            log.borrow().ops.join(","),
            "send,close,wait,close,recv,wait,recv,recv"
        );
        Ok(())
    }
    #[test]
    fn bidi_backpressure_half_close_preserves_responses() -> TestResult {
        let (mut driver, log) = Script::new(vec![Ok(Some(5)), Ok(Some(6)), Ok(None)]);
        driver.sends = vec![Err(StreamError::WouldBlock); 2].into();
        driver.sends.push_back(Ok(()));
        driver.closes = vec![Err(StreamError::WouldBlock), Ok(())].into();
        let mut call = BidiStreamingCall::new(driver);
        assert_eq!(call.try_send(&4), Err(StreamError::WouldBlock));
        assert!(log.borrow().accepted.is_empty());
        assert_eq!(call.recv_next()?, Some(5));
        call.send(&4)?;
        assert_eq!(call.try_half_close(), Err(StreamError::WouldBlock));
        call.half_close()?;
        call.half_close()?;
        assert!(call.try_send(&99).is_err());
        assert_eq!(call.finish()?, vec![6]);
        drop(call);
        assert_eq!(log.borrow().accepted, [4]);
        assert_eq!(
            log.borrow().ops.join(","),
            "send,recv,send,wait,send,close,close,recv,recv"
        );
        let (driver, log) = Script::new(vec![Ok(None)]);
        let mut call = BidiStreamingCall::new(driver);
        assert_eq!(call.try_recv_next()?, None);
        drop(call);
        assert_eq!(log.borrow().ops, ["recv", "cancel"]);
        Ok(())
    }
    #[test]
    fn server_halves_finish_and_last_drop_control_lifetime() -> TestResult {
        let (mut driver, log) = Script::new(vec![Ok(None)]);
        driver.finishes = vec![Err(StreamError::WouldBlock); 2].into();
        driver.finishes.push_back(Ok(()));
        let (mut requests, mut responses) = server_streams(driver);
        assert_eq!(requests.try_recv_next()?, None);
        drop(requests);
        responses.send(&2)?;
        let status = Status::permission_denied("denied");
        let mut trailers = Metadata::new();
        trailers.append_ascii("reason", "policy")?;
        assert_eq!(
            responses.try_finish(&status, &trailers),
            Err(StreamError::WouldBlock)
        );
        responses.finish(status.clone(), trailers.clone())?;
        assert!(responses.try_send(&3).is_err());
        assert!(responses.finish(Status::ok(""), Metadata::new()).is_err());
        drop(responses);
        assert_eq!(log.borrow().finished, [(status, trailers)]);
        assert_eq!(
            log.borrow().ops,
            ["recv", "send", "finish", "finish", "wait", "finish"]
        );
        let (driver, log) = Script::new(vec![Ok(None)]);
        let mut requests = RequestStream::new(driver);
        assert_eq!(requests.recv_next()?, None);
        drop(requests);
        assert_eq!(log.borrow().ops, ["recv", "cancel"]);
        let (driver, log) = Script::new(vec![]);
        let (requests, responses) = server_streams(driver);
        drop(responses);
        assert!(log.borrow().ops.is_empty());
        drop(requests);
        assert_eq!(log.borrow().ops, ["cancel"]);
        Ok(())
    }
    #[test]
    fn cancellation_and_invalid_wait_are_terminal() -> TestResult {
        let (driver, log) = Script::new(vec![Ok(Some(1))]);
        let mut call = BidiStreamingCall::new(driver);
        call.half_close()?;
        call.cancel()?;
        call.cancel()?;
        assert!(call.try_send(&1).is_err());
        assert!(call.try_recv_next().is_err());
        assert!(call.initial_metadata()?.is_some());
        drop(call);
        assert_eq!(log.borrow().ops, ["close", "cancel"]);
        let (mut driver, log) = Script::new(vec![Ok(None), Ok(Some(9))]);
        driver.send_trailers = false;
        let mut call = ServerStreamingCall::new(driver);
        let failure = call.recv_next();
        assert!(
            matches!(&failure, Err(StreamError::Status(s)) if s.code() == grpc_core::Code::Unknown)
        );
        assert_eq!(call.try_recv_next(), failure);
        drop(call);
        assert_eq!(log.borrow().ops, ["recv", "cancel"]);
        let (mut driver, log) = Script::new(vec![Err(StreamError::WouldBlock)]);
        driver.waits.push_back(Err(StreamError::WouldBlock));
        let mut call = ServerStreamingCall::new(driver);
        assert!(matches!(call.recv_next(), Err(StreamError::Status(_))));
        drop(call);
        assert_eq!(log.borrow().ops, ["recv", "wait", "cancel"]);
        Ok(())
    }
}
