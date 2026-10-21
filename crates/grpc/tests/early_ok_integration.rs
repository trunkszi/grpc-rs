//! Early successful responses retain wire ordering while request uploads stay open.
use std::{collections::BTreeSet, io, thread, time::Duration};

use grpc::{
    CallOptions, ChannelConfig, Codec, Metadata, Status, SyncChannel, SyncServer,
    core::transport::{MemoryPipe, SyncTransport, TcpTransport, TcpTransportListener},
};

type TestResult = anyhow::Result<()>;
struct Bytes;
impl Codec for Bytes {
    type EncodeItem = Vec<u8>;
    type DecodeItem = Vec<u8>;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = "application/grpc";
    fn encode(&mut self, value: &Vec<u8>, out: &mut Vec<u8>) -> io::Result<()> {
        out.extend_from_slice(value);
        Ok(())
    }
    fn decode(&mut self, input: &[u8]) -> io::Result<Vec<u8>> {
        Ok(input.to_vec())
    }
}
// Partial writes and retry boundaries expose trailers submitted but not yet flushed.
struct Fragmented<T> {
    transport: T,
    pause: bool,
    flush_pause: bool,
    wire: Vec<u8>,
    ended: BTreeSet<i32>,
}
impl<T: io::Read> io::Read for Fragmented<T> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.transport.read(bytes)
    }
}
impl<T: io::Write> io::Write for Fragmented<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.pause {
            self.pause = false;
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.pause = true;
        let count = self.transport.write(&bytes[..bytes.len().min(257)])?;
        self.wire.extend_from_slice(&bytes[..count]);
        while self.wire.len() >= 9 {
            let length = u32::from_be_bytes([0, self.wire[0], self.wire[1], self.wire[2]]) as usize;
            if self.wire.len() < length + 9 {
                break;
            }
            let id = i32::from_be_bytes(self.wire[5..9].try_into().expect("frame ID")) & i32::MAX;
            if self.wire[3] == 1 && self.wire[4] & 1 != 0 {
                self.ended.insert(id);
            }
            if self.wire[3] == 3 && self.wire[9..9 + length] == [0; 4] && !self.ended.contains(&id)
            {
                return Err(io::Error::other(
                    "NO_ERROR reset preceded response END_STREAM",
                ));
            }
            self.wire.drain(..9 + length);
        }
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flush_pause = !self.flush_pause;
        if self.flush_pause {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.transport.flush()
    }
}
impl<T: SyncTransport> SyncTransport for Fragmented<T> {
    fn set_nonblocking(&mut self, value: bool) -> io::Result<()> {
        self.transport.set_nonblocking(value)
    }
    fn set_read_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
        self.transport.set_read_timeout(value)
    }
    fn set_write_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
        self.transport.set_write_timeout(value)
    }
    fn peer_endpoint(&self) -> io::Result<String> {
        self.transport.peer_endpoint()
    }
}
fn early_ok<C, S>(client: C, server: S) -> TestResult
where
    C: SyncTransport + 'static,
    S: SyncTransport + Send + 'static,
{
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(Fragmented {
            transport: server,
            pause: false,
            flush_pause: false,
            wire: Vec::new(),
            ended: BTreeSet::new(),
        })?;
        server.register_bidi_streaming("/test.Early/Ok", Bytes, |_requests, sink| {
            sink.send(&vec![42; 4096])
                .map_err(|e| Status::internal(e.to_string()))?;
            let mut trailers = Metadata::new();
            trailers
                .append_ascii("finished", "yes")
                .map_err(|e| Status::internal(e.to_string()))?;
            sink.finish(Status::ok(""), trailers)
                .map_err(|e| Status::internal(e.to_string()))
        })?;
        server.serve_connection()
    });
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            client,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(3)),
                ..Default::default()
            },
        )?;
        for _ in 0..24 {
            let mut call =
                channel.bidi_streaming("/test.Early/Ok", Bytes, CallOptions::default())?;
            call.send(&vec![7; 32])?;
            // The request is intentionally not half-closed before successful response EOF.
            assert_eq!(call.recv_next()?, Some(vec![42; 4096]));
            assert_eq!(call.recv_next()?, None);
            let trailers = call.trailing_metadata()?.expect("validated OK trailers");
            assert!(
                trailers
                    .to_headers()
                    .contains(&("finished".into(), "yes".into()))
            );
        }
        Ok(())
    })();
    let served = worker.join().expect("server must not panic");
    result?;
    // Closing a client with unread terminal reset frames can reset a TCP/Unix socket.
    // All response payloads and OK trailers above must already have been validated.
    if let Err(status) = served {
        assert_eq!(status.code(), grpc::core::Code::Unavailable);
        let message = status.message().to_ascii_lowercase();
        assert!(
            message.contains("connection reset by peer") || message.contains("broken pipe"),
            "{status}"
        );
    }
    Ok(())
}
#[test]
fn integration_early_ok_memory_partial_writes() -> TestResult {
    let (client, server) = MemoryPipe::with_capacity(1024)?;
    early_ok(client, server)
}
#[test]
fn integration_early_ok_tcp_partial_writes() -> TestResult {
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let client = TcpTransport::connect(listener.local_addr()?)?;
    let (server, _) = listener.accept()?;
    early_ok(client, server)
}
#[cfg(unix)]
#[test]
fn integration_early_ok_unix_partial_writes() -> TestResult {
    use grpc::core::transport::{UnixTransport, UnixTransportListener};
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.0));
        }
    }
    let path = std::env::temp_dir().join(format!("grpc-early-ok-{}", std::process::id()));
    std::fs::create_dir(&path)?;
    let directory = Directory(path);
    let path = directory.0.join("socket");
    let listener = UnixTransportListener::bind(&path)?;
    let client = UnixTransport::connect(&path)?;
    let (server, _) = listener.accept()?;
    early_ok(client, server)
}
