//! Capacity ownership and absolute deadlines under controlled transport failures.
use std::{
    cell::RefCell,
    io::{self, Read, Write},
    rc::Rc,
    thread,
    time::Duration,
};

use grpc::{
    Codec, Metadata, Response, SessionConfig, Status, SyncServer,
    core::{
        Code, CompressionAlgorithm, GrpcClientSession,
        transport::{MemoryPipe, SyncTransport},
    },
};
type TestResult = anyhow::Result<()>;
struct Bytes;
impl Codec for Bytes {
    type EncodeItem = Vec<u8>;
    type DecodeItem = Vec<u8>;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = "application/grpc";
    fn encode(&mut self, message: &Vec<u8>, output: &mut Vec<u8>) -> io::Result<()> {
        output.extend_from_slice(message);
        Ok(())
    }
    fn decode(&mut self, input: &[u8]) -> io::Result<Vec<u8>> {
        Ok(input.to_vec())
    }
}
fn wire(client: &mut GrpcClientSession) -> Result<Vec<u8>, Status> {
    let mut wire = Vec::new();
    loop {
        let chunk = client.mem_send()?;
        if chunk.is_empty() {
            return Ok(wire);
        }
        wire.extend_from_slice(chunk);
    }
}
fn receive(client: &mut GrpcClientSession, pipe: &mut MemoryPipe) -> anyhow::Result<()> {
    let mut bytes = [0; 16384];
    loop {
        match pipe.read(&mut bytes) {
            Ok(0) => return Ok(()),
            Ok(count) => {
                client.mem_recv(&bytes[..count])?;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}
#[test]
fn integration_cancelled_before_dispatch_releases_single_slot() -> TestResult {
    let (mut pipe, transport) = MemoryPipe::pair();
    pipe.set_nonblocking(true)?;
    let mut server = SyncServer::with_config(
        transport,
        SessionConfig {
            max_active_streams: 1,
            ..Default::default()
        },
    )?;
    server.register_unary("/limits.Capacity/Call", Bytes, |request| {
        Ok(Response::new(request.into_parts().1))
    })?;
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    for _ in 0..80 {
        let id = client.start_request(
            "localhost",
            "/limits.Capacity/Call",
            &Metadata::new(),
            None,
            CompressionAlgorithm::Identity,
            false,
        )?;
        let mut batch = wire(&mut client)?;
        client.cancel_stream(id, 8)?;
        batch.extend(wire(&mut client)?);
        // Both HEADERS and reset are available in the same server receive batch.
        pipe.write_all(&batch)?;
        assert!(server.poll_once()?);
        receive(&mut client, &mut pipe)?;
        client.remove_closed_stream(id)?;
    }
    let id = client.start_request(
        "localhost",
        "/limits.Capacity/Call",
        &Metadata::new(),
        None,
        CompressionAlgorithm::Identity,
        false,
    )?;
    client.send_message(id, b"healthy", CompressionAlgorithm::Identity)?;
    client.half_close(id)?;
    pipe.write_all(&wire(&mut client)?)?;
    assert!(server.poll_once()?);
    receive(&mut client, &mut pipe)?;
    assert_eq!(
        client.pop_message(id)?.expect("healthy response").payload(),
        b"healthy"
    );
    assert_eq!(
        client
            .take_trailers(id)?
            .expect("healthy trailers")
            .0
            .code(),
        Code::Ok
    );
    Ok(())
}
struct InterruptedRead {
    pipe: MemoryPipe,
    blocking: bool,
    interrupted: bool,
    timeout: Option<Duration>,
    observed: Rc<RefCell<Vec<Duration>>>,
}
impl Read for InterruptedRead {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.blocking
            && let Some(timeout) = self.timeout.filter(|d| *d > Duration::from_millis(50))
        {
            self.observed.borrow_mut().push(timeout);
            if !self.interrupted {
                self.interrupted = true;
                thread::sleep(Duration::from_millis(35));
                return Err(io::ErrorKind::Interrupted.into());
            }
        }
        self.pipe.read(bytes)
    }
}
impl Write for InterruptedRead {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.pipe.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.pipe.flush()
    }
}
impl SyncTransport for InterruptedRead {
    fn set_nonblocking(&mut self, value: bool) -> io::Result<()> {
        self.blocking = !value;
        self.pipe.set_nonblocking(value)
    }
    fn set_read_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
        self.timeout = value;
        self.pipe.set_read_timeout(value)
    }
    fn set_write_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
        self.pipe.set_write_timeout(value)
    }
    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        self.pipe.read_timeout()
    }
    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        self.pipe.write_timeout()
    }
    fn peer_endpoint(&self) -> io::Result<String> {
        self.pipe.peer_endpoint()
    }
}
#[test]
fn integration_interrupted_read_reinstalls_remaining_deadline() -> TestResult {
    let (pipe, transport) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<Vec<Duration>, Status> {
        let observed = Rc::new(RefCell::new(Vec::new()));
        let mut server = SyncServer::new(InterruptedRead {
            pipe: transport,
            blocking: false,
            interrupted: false,
            timeout: None,
            observed: Rc::clone(&observed),
        })?;
        server.register_bidi_streaming("/limits.Deadline/Call", Bytes, |mut requests, _sink| {
            while requests
                .recv_next()
                .map_err(|e| Status::internal(e.to_string()))?
                .is_some()
            {}
            Ok(())
        })?;
        server.serve_connection()?;
        Ok(observed.take())
    });
    let result = (|| -> TestResult {
        let mut pipe = pipe;
        pipe.set_read_timeout(Some(Duration::from_secs(2)))?;
        let mut client = GrpcClientSession::new(SessionConfig::default())?;
        let id = client.start_request(
            "localhost",
            "/limits.Deadline/Call",
            &Metadata::new(),
            Some(Duration::from_millis(300)),
            CompressionAlgorithm::Identity,
            false,
        )?;
        client.send_message(id, &[1], CompressionAlgorithm::Identity)?;
        pipe.write_all(&wire(&mut client)?)?;
        let mut bytes = [0; 16384];
        loop {
            let count = pipe.read(&mut bytes)?;
            assert_ne!(count, 0, "server must send deadline status before closing");
            client.mem_recv(&bytes[..count])?;
            if let Some((status, _)) = client.take_trailers(id)? {
                assert_eq!(status.code(), Code::DeadlineExceeded);
                break;
            }
        }
        Ok(())
    })();
    let served = worker.join().expect("server must not panic");
    result?;
    let values = served?;
    assert!(
        values.len() >= 2,
        "must observe interrupted read and its retry: {values:?}"
    );
    assert!(
        values[1] + Duration::from_millis(20) < values[0],
        "retry timeout must exclude time already consumed: {values:?}"
    );
    Ok(())
}
