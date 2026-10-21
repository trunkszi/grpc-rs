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
    let worker = spawn_server(server);
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            client,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                ..ChannelConfig::default()
            },
        )?;
        let error = channel
            .call_unary("/test.Echo/Unary", &5, &mut WrongContentCodec)
            .expect_err("codec content type mismatch must fail");
        assert_eq!(error.code(), Code::Unimplemented);
        assert_eq!(
            channel.call_unary("/test.Echo/Unary", &5, &mut IntegerCodec)?,
            6
        );
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
