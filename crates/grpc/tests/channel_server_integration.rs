//! Real HTTP/2 integration through bounded memory pipes and loopback TCP.

use std::{io, thread, time::Duration};

use grpc::{
    Codec, Request, Response,
    channel::{CallOptions, ChannelConfig, SyncChannel},
    core::{
        Code, Metadata, Status,
        compression::CompressionAlgorithm,
        transport::{MemoryPipe, SyncTransport, TcpTransport, TcpTransportListener},
    },
    server::SyncServer,
    stream::StreamError,
};
type TestResult = Result<(), Box<dyn std::error::Error>>;
#[derive(Clone, Default)]
struct IntegerCodec;
impl Codec for IntegerCodec {
    type EncodeItem = u32;
    type DecodeItem = u32;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = "application/grpc+test";
    fn encode(&mut self, value: &u32, destination: &mut Vec<u8>) -> io::Result<()> {
        destination.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }
    fn decode(&mut self, payload: &[u8]) -> io::Result<u32> {
        Ok(u32::from_be_bytes(payload.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "integer payload must be four bytes",
            )
        })?))
    }
}
fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking operation unexpectedly pending"),
    }
}
fn metadata() -> Metadata {
    let mut values = Metadata::new();
    values.append_ascii("tag", "first").expect("valid tag");
    values.append_ascii("tag", "second").expect("valid tag");
    values
        .append_binary("raw-bin", &[0, 255, 128])
        .expect("valid binary metadata");
    values
}
fn register<T: SyncTransport + 'static>(server: &mut SyncServer<T>) -> Result<(), Status> {
    server.register_unary("/test.Echo/Unary", IntegerCodec, |request| {
        Ok(Response::from_parts(
            request.metadata().clone(),
            request.get_ref() + 1,
        ))
    })?;
    server.register_server_streaming("/test.Echo/Server", IntegerCodec, |request, sink| {
        sink.send(request.get_ref()).map_err(stream_status)?;
        sink.send(&(request.get_ref() + 1)).map_err(stream_status)?;
        sink.finish(Status::ok(""), metadata())
            .map_err(stream_status)
    })?;
    server.register_client_streaming("/test.Echo/Client", IntegerCodec, |mut requests| {
        let mut sum = 0;
        while let Some(value) = requests.recv_next().map_err(stream_status)? {
            sum += value;
        }
        Ok(Response::new(sum))
    })?;
    server.register_bidi_streaming("/test.Echo/Bidi", IntegerCodec, |mut requests, sink| {
        while let Some(value) = requests.recv_next().map_err(stream_status)? {
            sink.send(&(value * 2)).map_err(stream_status)?;
        }
        Ok(())
    })?;
    server.register_bidi_streaming("/test.Echo/Early", IntegerCodec, |_requests, sink| {
        sink.send(&77).map_err(stream_status)?;
        Ok(())
    })?;
    server.register_unary("/test.Echo/Denied", IntegerCodec, |_| {
        Err(Status::permission_denied("policy rejection"))
    })?;
    server.register_unary("/test.Echo/Panic", IntegerCodec, |_| {
        panic!("controlled handler panic")
    })?;
    Ok(())
}
fn spawn_server<T: SyncTransport + Send + 'static>(
    transport: T,
) -> thread::JoinHandle<Result<(), Status>> {
    thread::spawn(move || {
        let mut server = SyncServer::new(transport)?;
        register(&mut server)?;
        server.serve_connection()
    })
}
fn exercise_all_cardinalities<T: SyncTransport + 'static>(transport: T) -> TestResult {
    let config = ChannelConfig {
        default_timeout: Some(Duration::from_secs(5)),
        ..ChannelConfig::default()
    };
    let mut channel = SyncChannel::with_config(transport, config)?;
    for algorithm in [
        CompressionAlgorithm::Identity,
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Deflate,
        CompressionAlgorithm::Snappy,
        CompressionAlgorithm::Lz4,
    ] {
        let (value, initial, _trailing) = channel.unary(
            "/test.Echo/Unary",
            &41,
            &mut IntegerCodec,
            CallOptions {
                metadata: metadata(),
                compression: Some(algorithm),
                ..CallOptions::default()
            },
        )?;
        assert_eq!(value, 42);
        assert_eq!(initial, metadata());
    }
    let mut stream = channel.server_streaming(
        "/test.Echo/Server",
        &7,
        IntegerCodec,
        CallOptions::default(),
    )?;
    assert_eq!(stream.recv_next()?, Some(7));
    assert_eq!(stream.finish()?, [8]);
    assert_eq!(stream.trailing_metadata()?, Some(metadata()));
    drop(stream);
    let mut stream =
        channel.client_streaming("/test.Echo/Client", IntegerCodec, CallOptions::default())?;
    stream.send(&2)?;
    stream.send(&3)?;
    stream.send(&5)?;
    assert_eq!(stream.finish()?, 10);
    drop(stream);
    let mut stream =
        channel.bidi_streaming("/test.Echo/Bidi", IntegerCodec, CallOptions::default())?;
    // Receiving before half-close proves the response is not batch buffered.
    stream.send(&3)?;
    assert_eq!(stream.recv_next()?, Some(6));
    stream.send(&4)?;
    assert_eq!(stream.recv_next()?, Some(8));
    assert!(stream.finish()?.is_empty());
    drop(stream);
    let mut abandoned =
        channel.bidi_streaming("/test.Echo/Bidi", IntegerCodec, CallOptions::default())?;
    abandoned.send(&17)?;
    assert_eq!(abandoned.recv_next()?, Some(34));
    drop(abandoned);
    assert_eq!(
        channel.call_unary("/test.Echo/Unary", &19, &mut IntegerCodec)?,
        20
    );
    for (path, expected) in [
        ("/test.Echo/Denied", Code::PermissionDenied),
        ("/test.Echo/Panic", Code::Internal),
        ("/test.Echo/Missing", Code::Unimplemented),
    ] {
        let error = channel
            .call_unary(path, &1, &mut IntegerCodec)
            .expect_err("remote RPC must fail");
        assert_eq!(error.code(), expected);
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &9, &mut IntegerCodec)?,
            10,
            "a rejected RPC must not poison the connection"
        );
    }
    Ok(())
}
#[test]
fn integration_memory_pipe_all_cardinalities_compression_metadata_and_failures() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    let worker = spawn_server(server);
    let result = exercise_all_cardinalities(client);
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}
#[test]
fn integration_tcp_all_cardinalities_compression_metadata_and_failures() -> TestResult {
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let client = TcpTransport::connect(listener.local_addr()?)?;
    let (server, _) = listener.accept()?;
    let worker = spawn_server(server);
    let result = exercise_all_cardinalities(client);
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}
#[test]
fn integration_deadline_without_peer_progress_is_bounded() -> TestResult {
    let (client, _silent_peer) = MemoryPipe::pair();
    let mut channel = SyncChannel::new(client)?;
    let error = channel
        .unary(
            "/test.Echo/Unary",
            &1,
            &mut IntegerCodec,
            CallOptions {
                timeout: Some(Duration::from_millis(20)),
                ..CallOptions::default()
            },
        )
        .expect_err("silent peer must hit the call deadline");
    assert_eq!(error.code(), Code::DeadlineExceeded);
    Ok(())
}
#[grpc::macros::grpc_service(package = "test")]
trait GeneratedEcho {
    fn reflect(request: u32) -> u32;
}
struct EchoHandler;
impl GeneratedEcho for EchoHandler {
    fn reflect(&mut self, request: Request<u32>) -> Result<Response<u32>, Status> {
        Ok(Response::new(request.get_ref() + 100))
    }
}
impl GeneratedEchoChannel<IntegerCodec> for SyncChannel<MemoryPipe> {
    fn reflect(
        &mut self,
        codec: &mut IntegerCodec,
        method: &'static grpc::MethodDescriptor,
        compression: &Option<CompressionAlgorithm>,
        request: Request<u32>,
    ) -> Result<Response<u32>, Status> {
        let (metadata, request) = request.into_parts();
        let (response, initial, _) = self.unary(
            method.path,
            &request,
            codec,
            CallOptions {
                metadata,
                compression: *compression,
                ..CallOptions::default()
            },
        )?;
        Ok(Response::from_parts(initial, response))
    }
}
#[test]
fn integration_generated_client_and_server_adapters_execute_real_frames() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(server)?;
        let mut generated = GeneratedEchoServer::new(EchoHandler, IntegerCodec);
        server.register_unary(GENERATED_ECHO_REFLECT.path, IntegerCodec, move |request| {
            generated.reflect(request)
        })?;
        server.serve_connection()
    });
    let channel = SyncChannel::with_config(
        client,
        ChannelConfig {
            default_timeout: Some(Duration::from_secs(5)),
            ..ChannelConfig::default()
        },
    )?;
    let mut generated = GeneratedEchoClient::new(channel, IntegerCodec)
        .with_compression(CompressionAlgorithm::Zstd);
    let result = generated.reflect(Request::new(23));
    drop(generated);
    let served = worker.join().expect("server worker must not panic");
    assert_eq!(*result?.get_ref(), 123);
    served?;
    Ok(())
}

#[test]
fn integration_server_rejects_expired_wire_deadline_before_handler() -> TestResult {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use grpc::core::session::{GrpcClientSession, SessionConfig};
    let (mut client, transport) = MemoryPipe::pair();
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    let called = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&called);
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(transport)?;
        server.register_unary("/test.Echo/Expired", IntegerCodec, move |_| {
            observed.store(true, Ordering::SeqCst);
            Ok(Response::new(99))
        })?;
        server.serve_connection()
    });
    let mut session = GrpcClientSession::new(SessionConfig::default())?;
    let id = session.start_request(
        "localhost",
        "/test.Echo/Expired",
        &Metadata::new(),
        Some(Duration::ZERO),
        CompressionAlgorithm::Identity,
        false,
    )?;
    session.send_message(id, &1_u32.to_be_bytes(), CompressionAlgorithm::Identity)?;
    session.half_close(id)?;
    let mut buffer = [0; 8192];
    let result = (|| -> Result<Status, Status> {
        loop {
            session.step_transport(&mut client, &mut buffer)?;
            if let Some((status, _)) = session.take_trailers(id)? {
                return Ok(status);
            }
        }
    })();
    drop(client);
    let served = worker.join().expect("server worker must not panic");
    assert_eq!(result?.code(), Code::DeadlineExceeded);
    assert!(!called.load(Ordering::SeqCst));
    served?;
    Ok(())
}

#[test]
fn integration_stream_capacity_reclaims_completed_and_cancelled_calls() -> TestResult {
    use grpc::core::session::SessionConfig;
    let (client, server) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::with_config(
            server,
            SessionConfig {
                max_active_streams: 2,
                ..SessionConfig::default()
            },
        )?;
        register(&mut server)?;
        server.serve_connection()
    });
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            client,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                session_config: SessionConfig {
                    max_active_streams: 2,
                    ..SessionConfig::default()
                },
                ..ChannelConfig::default()
            },
        )?;
        for value in 0..20 {
            assert_eq!(
                channel.call_unary("/test.Echo/Unary", &value, &mut IntegerCodec)?,
                value + 1
            );
            let mut abandoned =
                channel.bidi_streaming("/test.Echo/Bidi", IntegerCodec, CallOptions::default())?;
            abandoned.send(&value)?;
            assert_eq!(abandoned.recv_next()?, Some(2 * value));
            drop(abandoned);
        }
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &100, &mut IntegerCodec)?,
            101
        );
        Ok(())
    })();
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}

struct WrongContentCodec;
impl Codec for WrongContentCodec {
    type EncodeItem = u32;
    type DecodeItem = u32;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = "application/grpc+wrong-content";
    fn encode(&mut self, value: &u32, destination: &mut Vec<u8>) -> io::Result<()> {
        IntegerCodec.encode(value, destination)
    }
    fn decode(&mut self, payload: &[u8]) -> io::Result<u32> {
        IntegerCodec.decode(payload)
    }
}

#[test]
fn integration_wrong_content_type_rejected_before_codec_and_connection_recovers() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    let worker = thread::spawn(move || {
        let mut server = SyncServer::with_config(
            server,
            grpc::SessionConfig {
                max_active_streams: 1,
                ..Default::default()
            },
        )?;
        register(&mut server)?;
        server.serve_connection()
    });
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            client,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                ..ChannelConfig::default()
            },
        )?;
        for _ in 0..8 {
            let error = channel
                .call_unary("/test.Echo/Unary", &5, &mut WrongContentCodec)
                .expect_err("codec content type mismatch must fail");
            assert_eq!(error.code(), Code::Unimplemented);
            assert_eq!(
                channel.call_unary("/test.Echo/Unary", &5, &mut IntegerCodec)?,
                6
            );
        }
        Ok(())
    })();
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}

#[test]
fn integration_graceful_shutdown_emits_goaway_and_serve_returns() -> TestResult {
    use grpc::core::session::{GrpcClientSession, SessionConfig};
    let (mut client, server) = MemoryPipe::pair();
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(server)?;
        server.shutdown_with_goaway(0)?;
        server.serve_connection()
    });
    // Only receive here: the peer may close after flushing GOAWAY, so avoid an
    // unrelated settings-ACK write to the deliberately closed transport.
    use std::io::Read;
    let mut session = GrpcClientSession::new(SessionConfig::default())?;
    let mut buffer = [0; 8192];
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        while session.goaway_received().is_none() {
            let count = client.read(&mut buffer)?;
            if count == 0 {
                return Err("peer closed without GOAWAY".into());
            }
            let mut used = 0;
            while used < count {
                let consumed = session.mem_recv(&buffer[used..count])?;
                if consumed == 0 {
                    return Err("HTTP/2 decoder made no progress".into());
                }
                used += consumed;
            }
        }
        assert_eq!(session.goaway_received(), Some((0, 0)));
        Ok(())
    })();
    drop(client);
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}

struct DropCallCodec {
    abandoned: std::rc::Rc<std::cell::RefCell<Option<grpc::BidiStreamingCall<u32, u32>>>>,
}
impl Codec for DropCallCodec {
    type EncodeItem = u32;
    type DecodeItem = u32;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = IntegerCodec::CONTENT_TYPE;
    fn encode(&mut self, value: &u32, destination: &mut Vec<u8>) -> io::Result<()> {
        IntegerCodec.encode(value, destination)
    }
    fn decode(&mut self, payload: &[u8]) -> io::Result<u32> {
        drop(self.abandoned.borrow_mut().take());
        IntegerCodec.decode(payload)
    }
}

#[test]
fn integration_codec_callback_drops_other_call_without_leaking_stream_capacity() -> TestResult {
    use std::{cell::RefCell, rc::Rc};

    use grpc::core::session::SessionConfig;
    let (client, transport) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::with_config(
            transport,
            SessionConfig {
                max_active_streams: 2,
                ..SessionConfig::default()
            },
        )?;
        register(&mut server)?;
        server.serve_connection()
    });
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            client,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                session_config: SessionConfig {
                    max_active_streams: 2,
                    ..SessionConfig::default()
                },
                ..ChannelConfig::default()
            },
        )?;
        let abandoned = Rc::new(RefCell::new(Some(channel.bidi_streaming(
            "/test.Echo/Early",
            IntegerCodec,
            CallOptions::default(),
        )?)));
        let mut responses = channel.server_streaming(
            "/test.Echo/Server",
            &10,
            DropCallCodec {
                abandoned: Rc::clone(&abandoned),
            },
            CallOptions::default(),
        )?;
        assert_eq!(responses.finish()?, [10, 11]);
        assert!(abandoned.borrow().is_none());
        drop(responses);
        let pending =
            channel.bidi_streaming("/test.Echo/Early", IntegerCodec, CallOptions::default())?;
        // This second live slot must fit: the callback released the older call.
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &20, &mut IntegerCodec)?,
            21
        );
        drop(pending);
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &30, &mut IntegerCodec)?,
            31
        );
        Ok(())
    })();
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}

struct SlowCodec {
    encode_delay: bool,
    decode_delay: bool,
    entered: std::rc::Rc<std::cell::Cell<bool>>,
}
impl Codec for SlowCodec {
    type EncodeItem = u32;
    type DecodeItem = u32;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = IntegerCodec::CONTENT_TYPE;
    fn encode(&mut self, value: &u32, destination: &mut Vec<u8>) -> io::Result<()> {
        if self.encode_delay {
            self.entered.set(true);
            thread::sleep(Duration::from_millis(70));
        }
        IntegerCodec.encode(value, destination)
    }
    fn decode(&mut self, payload: &[u8]) -> io::Result<u32> {
        if self.decode_delay {
            self.entered.set(true);
            thread::sleep(Duration::from_millis(70));
        }
        IntegerCodec.decode(payload)
    }
}

#[test]
fn integration_unary_deadline_includes_codec_encode_and_decode_work() -> TestResult {
    use std::{cell::Cell, rc::Rc};
    let (client, transport) = MemoryPipe::pair();
    let worker = spawn_server(transport);
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            client,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                ..ChannelConfig::default()
            },
        )?;
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &0, &mut IntegerCodec)?,
            1
        );
        for encode_delay in [true, false] {
            let entered = Rc::new(Cell::new(false));
            let mut codec = SlowCodec {
                encode_delay,
                decode_delay: !encode_delay,
                entered: Rc::clone(&entered),
            };
            let error = channel
                .unary(
                    "/test.Echo/Unary",
                    &2,
                    &mut codec,
                    CallOptions {
                        timeout: Some(Duration::from_millis(10)),
                        ..CallOptions::default()
                    },
                )
                .expect_err("codec work must not extend an expired RPC deadline");
            assert!(
                entered.get(),
                "the delayed codec branch must actually execute"
            );
            assert_eq!(error.code(), Code::DeadlineExceeded);
            assert_eq!(
                channel.call_unary("/test.Echo/Unary", &3, &mut IntegerCodec)?,
                4
            );
        }
        Ok(())
    })();
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}

#[test]
fn integration_streaming_deadline_includes_codec_decode_and_send_work() -> TestResult {
    use std::{cell::Cell, rc::Rc};
    let (client, transport) = MemoryPipe::pair();
    let worker = spawn_server(transport);
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            client,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                ..ChannelConfig::default()
            },
        )?;
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &0, &mut IntegerCodec)?,
            1
        );
        let decoded = Rc::new(Cell::new(false));
        let mut responses = channel.server_streaming(
            "/test.Echo/Server",
            &7,
            SlowCodec {
                encode_delay: false,
                decode_delay: true,
                entered: Rc::clone(&decoded),
            },
            CallOptions {
                timeout: Some(Duration::from_millis(10)),
                ..CallOptions::default()
            },
        )?;
        let error = responses
            .recv_next()
            .expect_err("slow decoder must not return late success");
        assert!(decoded.get());
        assert!(
            matches!(error, StreamError::Status(status) if status.code() == Code::DeadlineExceeded)
        );
        drop(responses);
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &1, &mut IntegerCodec)?,
            2
        );
        let encoded = Rc::new(Cell::new(false));
        let mut requests = channel.bidi_streaming(
            "/test.Echo/Bidi",
            SlowCodec {
                encode_delay: true,
                decode_delay: false,
                entered: Rc::clone(&encoded),
            },
            CallOptions {
                timeout: Some(Duration::from_millis(10)),
                ..CallOptions::default()
            },
        )?;
        let error = requests
            .send(&9)
            .expect_err("slow encoder must not return late success");
        assert!(encoded.get());
        assert!(
            matches!(error, StreamError::Status(status) if status.code() == Code::DeadlineExceeded)
        );
        drop(requests);
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &2, &mut IntegerCodec)?,
            3
        );
        Ok(())
    })();
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}

struct ByteVecCodec;
impl Codec for ByteVecCodec {
    type EncodeItem = Vec<u8>;
    type DecodeItem = Vec<u8>;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = "application/grpc+bytes";
    fn encode(&mut self, value: &Vec<u8>, destination: &mut Vec<u8>) -> io::Result<()> {
        destination.extend_from_slice(value);
        Ok(())
    }
    fn decode(&mut self, payload: &[u8]) -> io::Result<Vec<u8>> {
        Ok(payload.to_vec())
    }
}

#[test]
fn integration_duplex_backpressure_drains_both_directions_on_tiny_pipe() -> TestResult {
    let (client, transport) = MemoryPipe::with_capacity(1024)?;
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(transport)?;
        server.register_bidi_streaming(
            "/test.Bytes/Duplex",
            ByteVecCodec,
            |mut requests, sink| {
                for expected in [1_u8, 2] {
                    let message = requests
                        .recv_next()
                        .map_err(stream_status)?
                        .ok_or_else(|| Status::internal("missing duplex request"))?;
                    if message != vec![expected; 4096] {
                        return Err(Status::invalid_argument("incorrect duplex payload"));
                    }
                    sink.send(&message).map_err(stream_status)?;
                }
                if requests.recv_next().map_err(stream_status)?.is_some() {
                    return Err(Status::invalid_argument("unexpected third duplex request"));
                }
                Ok(())
            },
        )?;
        server.serve_connection()
    });
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::new(client)?;
        let mut call = channel.bidi_streaming(
            "/test.Bytes/Duplex",
            ByteVecCodec,
            CallOptions {
                timeout: Some(Duration::from_secs(2)),
                ..CallOptions::default()
            },
        )?;
        // Both sends precede any receive; neither pipe direction can hold a frame.
        call.send(&vec![1; 4096])?;
        call.send(&vec![2; 4096])?;
        call.half_close()?;
        assert_eq!(call.recv_next()?, Some(vec![1; 4096]));
        assert_eq!(call.recv_next()?, Some(vec![2; 4096]));
        assert_eq!(call.recv_next()?, None);
        Ok(())
    })();
    let served = worker.join().expect("server worker must not panic");
    result?;
    served?;
    Ok(())
}
