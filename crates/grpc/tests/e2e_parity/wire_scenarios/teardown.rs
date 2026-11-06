//! Response rejection and graceful completion under controlled peer teardown.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use super::*;

#[derive(Default)]
struct ResponseTrace {
    rejected: AtomicBool,
    data_published: AtomicBool,
    trailers_sent: AtomicBool,
}
struct ResponseGate {
    pipe: MemoryPipe,
    trace: Arc<ResponseTrace>,
}
impl Read for ResponseGate {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.pipe.read(bytes)
    }
}
impl Write for ResponseGate {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.pipe.write(bytes)?;
        if bytes.len() >= 9 && bytes[3] == 0 {
            // Publish the DATA before allowing the server to finish its response.
            self.trace.data_published.store(true, Ordering::Release);
            let deadline = Instant::now() + Duration::from_secs(5);
            while !self.trace.rejected.load(Ordering::Acquire) {
                if Instant::now() >= deadline {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                thread::yield_now();
            }
        }
        if count == bytes.len() && bytes.len() >= 9 && bytes[3] == 1 && bytes[4] & 1 != 0 {
            self.trace.trailers_sent.store(true, Ordering::Release);
        }
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.pipe.flush()
    }
}
impl SyncTransport for ResponseGate {
    fn set_nonblocking(&mut self, value: bool) -> io::Result<()> {
        self.pipe.set_nonblocking(value)
    }
    fn set_read_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
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
fn e2e_compressed_response_exceeding_decoded_limit_is_resource_exhausted() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    let trace = Arc::new(ResponseTrace::default());
    let observed = Arc::clone(&trace);
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(ResponseGate {
            pipe: server,
            trace: Arc::clone(&observed),
        })?;
        register::<_, WireCodec>(&mut server)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !observed.rejected.load(Ordering::Acquire) {
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("rejection scenario stalled"));
            }
            if !server.poll_once()? {
                thread::yield_now();
            }
        }
        server.shutdown_with_goaway(0)?;
        server.serve_connection()
    });
    let mut config = ChannelConfig {
        default_timeout: Some(Duration::from_secs(5)),
        ..ChannelConfig::default()
    };
    config.session_config.compression.max_decoding_message_size = 128;
    let mut channel = SyncChannel::with_config(client, config)?;
    let result = (|| -> TestResult {
        let error = channel
            .unary(
                ORDER_SERVICE_PLACE_ORDER.path,
                &"large".repeat(1000),
                &mut WireCodec::default(),
                options(metadata(), Some(CompressionAlgorithm::Zstd)),
            )
            .expect_err("decompressed response exceeds 128 bytes");
        anyhow::ensure!(
            error.code() == Code::ResourceExhausted,
            "unexpected status: {error}"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !trace.data_published.load(Ordering::Acquire) {
            anyhow::ensure!(Instant::now() < deadline, "response DATA was not published");
            thread::yield_now();
        }
        anyhow::ensure!(
            !trace.trailers_sent.load(Ordering::Acquire),
            "trailers must still be gated"
        );
        Ok(())
    })();
    // The channel owns the peer until terminal response output has drained.
    trace.rejected.store(true, Ordering::Release);
    let served = worker.join().expect("server panicked");
    drop(channel);
    result?;
    served?;
    assert!(trace.trailers_sent.load(Ordering::Acquire));
    Ok(())
}

#[test]
fn e2e_backpressured_rpc_drains_through_goaway_before_server_exit() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<(), Status> {
        let blocked = Rc::new(std::cell::Cell::new(false));
        let gate = Rc::clone(&blocked);
        let mut server = SyncServer::new(Recorded {
            inner: Rc::new(RefCell::new(server)),
            blocked: gate,
            trace: Rc::new(RefCell::new(Trace::default())),
        })?;
        let response_gate = Rc::clone(&blocked);
        let completed = Rc::new(std::cell::Cell::new(false));
        let observed = Rc::clone(&completed);
        let mut generated = OrderServiceServer::new(OrderBook, ());
        server.register_server_streaming(
            ORDER_SERVICE_PLACE_ORDER.path,
            WireCodec::default(),
            move |request, sink| {
                response_gate.set(true);
                sink.send(generated.place_order(request)?.get_ref())
                    .map_err(stream_status)?;
                sink.finish(Status::ok(""), trailers())
                    .map_err(stream_status)?;
                observed.set(true);
                Ok(())
            },
        )?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !completed.get() {
            assert!(std::time::Instant::now() < deadline, "RPC did not complete");
            if !server.poll_once()? {
                thread::yield_now();
            }
        }
        // The response and END_STREAM trailers are queued behind transport backpressure.
        assert!(blocked.get());
        server.shutdown_with_goaway(0)?;
        blocked.set(false);
        server.serve_connection()
    });
    let trace = Rc::new(RefCell::new(Trace::default()));
    let client = Rc::new(RefCell::new(client));
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            Recorded {
                inner: Rc::clone(&client),
                blocked: Rc::default(),
                trace: Rc::clone(&trace),
            },
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                ..ChannelConfig::default()
            },
        )?;
        let (response, _, trailing) = channel.unary(
            ORDER_SERVICE_PLACE_ORDER.path,
            &"drain".to_string(),
            &mut WireCodec::default(),
            options(metadata(), None),
        )?;
        assert_eq!(response, "accepted:drain");
        assert_eq!(trailing, trailers());
        Ok(())
    })();
    let served = worker.join().expect("server panicked");
    result?;
    served?;
    client.borrow_mut().set_nonblocking(false)?;
    let mut tail = Vec::new();
    client.borrow_mut().read_to_end(&mut tail)?;
    trace.borrow_mut().received.extend_from_slice(&tail);
    let received = wire(&trace.borrow().received);
    assert_eq!(received.goaway, Some((1, 0)));
    assert_eq!(header(&received, 1, b"grpc-status"), b"0");
    Ok(())
}
