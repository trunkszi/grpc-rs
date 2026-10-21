//! End-to-end RPC scenarios through generated adapters, HTTP/2 and synchronous transports.
#![cfg(any(feature = "protobuf", feature = "fory", feature = "capnp"))]
use std::{
    cell::RefCell,
    collections::BTreeMap,
    io::{self, Read, Write},
    rc::Rc,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use grpc::{
    CallOptions, ChannelConfig, Codec, CompressionAlgorithm, Metadata, MethodDescriptor, Request,
    Response, Status, SyncChannel, SyncServer,
    core::{
        Code, LengthPrefixedFrame,
        transport::{MemoryPipe, SyncTransport, TcpTransport, TcpTransportListener, http2},
    },
    stream::StreamError,
};
type TestResult = anyhow::Result<()>;
fn metadata() -> Metadata {
    let mut values = Metadata::new();
    values.append_ascii("x-desk", "hft").unwrap();
    values.append_binary("order-bin", &[0, 255, 128]).unwrap();
    values
}
fn trailers() -> Metadata {
    let mut values = Metadata::new();
    values.append_ascii("execution-id", "fill-42").unwrap();
    values.append_binary("audit-bin", &[1, 0, 254]).unwrap();
    values
}
#[grpc::macros::grpc_service(package = "hft")]
trait OrderService {
    fn place_order(request: String) -> String;
    #[server_streaming]
    fn subscribe_depth(request: String) -> String;
    #[client_streaming]
    fn upload_fills(request: String) -> String;
    #[bidi_streaming]
    fn order_stream(request: String) -> String;
}
struct OrderBook;
impl OrderService for OrderBook {
    fn place_order(&mut self, request: Request<String>) -> Result<Response<String>, Status> {
        assert_eq!(request.metadata(), &metadata());
        Ok(Response::new(format!("accepted:{}", request.get_ref())))
    }
    fn subscribe_depth(
        &mut self,
        request: Request<String>,
    ) -> Result<Response<Vec<String>>, Status> {
        Ok(Response::new(
            (1..=3)
                .map(|sequence| format!("{}:depth:{sequence}", request.get_ref()))
                .collect(),
        ))
    }
    fn upload_fills(&mut self, request: Request<Vec<String>>) -> Result<Response<String>, Status> {
        let mut total = 0u64;
        for quantity in request.get_ref() {
            total = total
                .checked_add(
                    quantity
                        .parse::<u64>()
                        .map_err(|_| Status::invalid_argument("fill must be a quantity"))?,
                )
                .ok_or_else(|| Status::out_of_range("fill total overflow"))?;
        }
        Ok(Response::new(format!("filled:{total}")))
    }
    fn order_stream(
        &mut self,
        request: Request<Vec<String>>,
    ) -> Result<Response<Vec<String>>, Status> {
        Ok(Response::new(
            request
                .get_ref()
                .iter()
                .map(|order| format!("ack:{order}"))
                .collect(),
        ))
    }
}
fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking stream remained pending"),
    }
}
fn options(metadata: Metadata, compression: Option<CompressionAlgorithm>) -> CallOptions {
    CallOptions {
        metadata,
        compression,
        ..CallOptions::default()
    }
}
// Generated stubs use Vec envelopes for streaming; this adapter connects them to
// the live channel and interleaves each bidi send with its corresponding reply.
impl<T: SyncTransport + 'static, C> OrderServiceChannel<C> for SyncChannel<T>
where
    C: Codec<EncodeItem = String, DecodeItem = String> + Default + 'static,
{
    fn place_order(
        &mut self,
        codec: &mut C,
        method: &'static MethodDescriptor,
        compression: &Option<CompressionAlgorithm>,
        request: Request<String>,
    ) -> Result<Response<String>, Status> {
        let (metadata, request) = request.into_parts();
        let (value, initial, trailing) = self.unary(
            method.path,
            &request,
            codec,
            options(metadata, *compression),
        )?;
        assert_eq!(trailing, trailers());
        Ok(Response::from_parts(initial, value))
    }
    fn subscribe_depth(
        &mut self,
        _: &mut C,
        method: &'static MethodDescriptor,
        compression: &Option<CompressionAlgorithm>,
        request: Request<String>,
    ) -> Result<Response<Vec<String>>, Status> {
        let (metadata, request) = request.into_parts();
        let mut stream = self.server_streaming(
            method.path,
            &request,
            C::default(),
            options(metadata, *compression),
        )?;
        let values = stream.finish().map_err(stream_status)?;
        assert_eq!(
            stream.trailing_metadata().map_err(stream_status)?,
            Some(trailers())
        );
        Ok(Response::new(values))
    }
    fn upload_fills(
        &mut self,
        _: &mut C,
        method: &'static MethodDescriptor,
        compression: &Option<CompressionAlgorithm>,
        request: Request<Vec<String>>,
    ) -> Result<Response<String>, Status> {
        let (metadata, requests) = request.into_parts();
        let mut stream =
            self.client_streaming(method.path, C::default(), options(metadata, *compression))?;
        for request in requests {
            stream.send(&request).map_err(stream_status)?;
        }
        Ok(Response::new(stream.finish().map_err(stream_status)?))
    }
    fn order_stream(
        &mut self,
        _: &mut C,
        method: &'static MethodDescriptor,
        compression: &Option<CompressionAlgorithm>,
        request: Request<Vec<String>>,
    ) -> Result<Response<Vec<String>>, Status> {
        let (metadata, requests) = request.into_parts();
        let mut stream =
            self.bidi_streaming(method.path, C::default(), options(metadata, *compression))?;
        let mut responses = Vec::new();
        for request in requests {
            stream.send(&request).map_err(stream_status)?;
            responses.push(
                stream
                    .recv_next()
                    .map_err(stream_status)?
                    .ok_or_else(|| Status::internal("missing order acknowledgement"))?,
            );
        }
        responses.extend(stream.finish().map_err(stream_status)?);
        Ok(Response::new(responses))
    }
}
fn register<T: SyncTransport + 'static, C>(server: &mut SyncServer<T>) -> Result<(), Status>
where
    C: Codec<EncodeItem = String, DecodeItem = String> + Default + 'static,
{
    let mut unary = OrderServiceServer::new(OrderBook, ());
    // A one-message sink preserves unary cardinality while exposing terminal metadata.
    server.register_server_streaming(
        ORDER_SERVICE_PLACE_ORDER.path,
        C::default(),
        move |request, sink| {
            let value = unary.place_order(request)?;
            sink.send(value.get_ref()).map_err(stream_status)?;
            sink.finish(Status::ok(""), trailers())
                .map_err(stream_status)
        },
    )?;
    let mut depth = OrderServiceServer::new(OrderBook, ());
    server.register_server_streaming(
        ORDER_SERVICE_SUBSCRIBE_DEPTH.path,
        C::default(),
        move |request, sink| {
            for value in depth.subscribe_depth(request)?.get_ref() {
                sink.send(value).map_err(stream_status)?;
            }
            sink.finish(Status::ok(""), trailers())
                .map_err(stream_status)
        },
    )?;
    let mut fills = OrderServiceServer::new(OrderBook, ());
    server.register_client_streaming(
        ORDER_SERVICE_UPLOAD_FILLS.path,
        C::default(),
        move |mut stream| {
            let mut values = Vec::new();
            while let Some(value) = stream.recv_next().map_err(stream_status)? {
                values.push(value);
            }
            fills.upload_fills(Request::new(values))
        },
    )?;
    let mut bidi = OrderServiceServer::new(OrderBook, ());
    server.register_bidi_streaming(
        ORDER_SERVICE_ORDER_STREAM.path,
        C::default(),
        move |mut stream, sink| {
            while let Some(value) = stream.recv_next().map_err(stream_status)? {
                let early = value == "early";
                for value in bidi.order_stream(Request::new(vec![value]))?.get_ref() {
                    sink.send(value).map_err(stream_status)?;
                }
                if early {
                    return sink
                        .finish(Status::ok(""), trailers())
                        .map_err(stream_status);
                }
            }
            Ok(())
        },
    )?;
    server.register_unary("/hft.OrderService/Denied", C::default(), |_| {
        Err(Status::permission_denied("risk 50%: reject\n策略"))
    })?;
    Ok(())
}
#[cfg(feature = "capnp")]
#[derive(Default)]
struct CapnpText(
    grpc::CapnpCodec<
        grpc_codec_capnp::backend::text::Owned,
        grpc_codec_capnp::backend::text::Owned,
    >,
);
#[cfg(feature = "capnp")]
impl Codec for CapnpText {
    type EncodeItem = String;
    type DecodeItem = String;
    type Error = grpc_codec_capnp::backend::Error;
    const CONTENT_TYPE: &'static str = "application/grpc+capnp";
    fn encode(&mut self, value: &String, out: &mut Vec<u8>) -> Result<(), Self::Error> {
        let mut message = grpc_codec_capnp::backend::message::TypedBuilder::new_default();
        message.set_root(value.as_str().into())?;
        self.0.encode(&message, out)
    }
    fn decode(&mut self, payload: &[u8]) -> Result<String, Self::Error> {
        assert_eq!(
            payload.as_ptr() as usize % 8,
            0,
            "received Capnp payload alignment"
        );
        let reader = self.0.read_aligned(payload)?;
        let root = reader.get()?;
        let bytes = root.as_bytes();
        assert!(bytes.as_ptr() >= payload.as_ptr());
        assert!(bytes.as_ptr_range().end <= payload.as_ptr_range().end);
        Ok(root.to_str()?.to_string())
    }
}
#[derive(Default)]
struct Trace {
    sent: Vec<u8>,
    received: Vec<u8>,
}
struct Recorded<T> {
    inner: Rc<RefCell<T>>,
    blocked: Rc<std::cell::Cell<bool>>,
    trace: Arc<Mutex<Trace>>,
}
impl<T: SyncTransport> Read for Recorded<T> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.borrow_mut().read(out)?;
        self.trace
            .lock()
            .unwrap()
            .received
            .extend_from_slice(&out[..n]);
        Ok(n)
    }
}
impl<T: SyncTransport> Write for Recorded<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.blocked.get() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = self.inner.borrow_mut().write(bytes)?;
        self.trace
            .lock()
            .unwrap()
            .sent
            .extend_from_slice(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.borrow_mut().flush()
    }
}
impl<T: SyncTransport> SyncTransport for Recorded<T> {
    fn set_nonblocking(&mut self, value: bool) -> io::Result<()> {
        self.inner.borrow_mut().set_nonblocking(value)
    }
    fn set_read_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
        self.inner.borrow_mut().set_read_timeout(value)
    }
    fn set_write_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
        self.inner.borrow_mut().set_write_timeout(value)
    }
    fn peer_endpoint(&self) -> io::Result<String> {
        self.inner.borrow().peer_endpoint()
    }
}
type HeaderBlock = Vec<(Vec<u8>, Vec<u8>)>;
struct Wire {
    headers: BTreeMap<i32, HeaderBlock>,
    header_flags: BTreeMap<i32, Vec<u8>>,
    data: BTreeMap<i32, Vec<u8>>,
    goaway: Option<(i32, u32)>,
}
fn wire(bytes: &[u8]) -> Wire {
    let mut result = Wire {
        headers: BTreeMap::new(),
        header_flags: BTreeMap::new(),
        data: BTreeMap::new(),
        goaway: None,
    };
    let mut inflater = http2::HpInflater::new().unwrap();
    let mut offset = if bytes.starts_with(b"PRI * HTTP/2.0") {
        24
    } else {
        0
    };
    while offset < bytes.len() {
        let h = &bytes[offset..offset + 9];
        let len = (usize::from(h[0]) << 16) | (usize::from(h[1]) << 8) | usize::from(h[2]);
        let id = i32::from_be_bytes(h[5..9].try_into().unwrap()) & 0x7fff_ffff;
        let payload = &bytes[offset + 9..offset + 9 + len];
        match h[3] {
            1 => {
                result.header_flags.entry(id).or_default().push(h[4]);
                assert_eq!(h[4] & 4, 4, "bounded test metadata fits one HEADERS block");
                for field in inflater.inflate_block(payload).unwrap() {
                    result
                        .headers
                        .entry(id)
                        .or_default()
                        .push((field.name, field.value));
                }
            }
            0 => result
                .data
                .entry(id)
                .or_default()
                .extend_from_slice(payload),
            7 => {
                result.goaway = Some((
                    i32::from_be_bytes(payload[..4].try_into().unwrap()),
                    u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                ))
            }
            _ => {}
        }
        offset += 9 + len;
    }
    result
}
fn header<'a>(wire: &'a Wire, id: i32, name: &[u8]) -> &'a [u8] {
    &wire.headers[&id]
        .iter()
        .find(|(key, _)| key == name)
        .expect("wire header")
        .1
}
fn exercise<T, S, C>(client: T, server: S) -> TestResult
where
    T: SyncTransport + 'static,
    S: SyncTransport + Send + 'static,
    C: Codec<EncodeItem = String, DecodeItem = String> + Default + 'static,
{
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(server)?;
        register::<_, C>(&mut server)?;
        server.serve_connection()
    });
    let trace = Arc::new(Mutex::new(Trace::default()));
    let result = client_calls::<_, C>(Rc::new(RefCell::new(client)), Arc::clone(&trace), "http");
    let served = worker.join().expect("server thread panicked");
    result?;
    served?;
    check_trace(&trace);
    Ok(())
}
fn check_trace(trace: &Arc<Mutex<Trace>>) {
    let trace = trace.lock().unwrap();
    let sent = wire(&trace.sent);
    let received = wire(&trace.received);
    assert_eq!(header(&sent, 1, b"x-desk"), b"hft");
    assert_eq!(header(&sent, 1, b"order-bin"), b"AP+A");
    let timeout = grpc::core::parse_grpc_timeout(
        std::str::from_utf8(header(&sent, 1, b"grpc-timeout")).unwrap(),
    )
    .unwrap();
    assert!(timeout > Duration::ZERO && timeout <= Duration::from_secs(5));
    assert_eq!(header(&received, 1, b"execution-id"), b"fill-42");
    assert_eq!(header(&received, 1, b"grpc-status"), b"0");
    for (id, code) in [(11, b"7".as_slice()), (13, b"12".as_slice())] {
        assert_eq!(
            received.header_flags[&id],
            [5],
            "one END_STREAM HEADERS block"
        );
        assert_eq!(header(&received, id, b":status"), b"200");
        assert_eq!(header(&received, id, b"grpc-status"), code);
        assert!(
            !received.data.contains_key(&id),
            "trailers-only status cannot carry response DATA"
        );
    }
    assert_eq!(
        header(&received, 11, b"grpc-message"),
        b"risk%2050%25%3A%20reject%0A%E7%AD%96%E7%95%A5"
    );
}
fn client_calls<T: SyncTransport + 'static, C>(
    client: Rc<RefCell<T>>,
    trace: Arc<Mutex<Trace>>,
    scheme: &str,
) -> TestResult
where
    C: Codec<EncodeItem = String, DecodeItem = String> + Default + 'static,
{
    let channel = SyncChannel::with_config(
        Recorded {
            inner: client,
            blocked: Rc::default(),
            trace: Arc::clone(&trace),
        },
        ChannelConfig {
            scheme: scheme.into(),
            default_timeout: Some(Duration::from_secs(5)),
            ..ChannelConfig::default()
        },
    )?;
    let mut generated = OrderServiceClient::new(channel, ())
        .with_codec::<C>()
        .with_compression(CompressionAlgorithm::Zstd);
    let long = "BUY:42:".repeat(150);
    assert_eq!(
        generated
            .place_order(Request::from_parts(metadata(), long.clone()))?
            .get_ref(),
        &format!("accepted:{long}")
    );
    assert_eq!(
        generated
            .subscribe_depth(Request::new("BTCUSDT".into()))?
            .get_ref(),
        &["BTCUSDT:depth:1", "BTCUSDT:depth:2", "BTCUSDT:depth:3"]
    );
    assert_eq!(
        generated
            .upload_fills(Request::new(vec!["3".into(), "5".into(), "7".into()]))?
            .get_ref(),
        "filled:15"
    );
    assert_eq!(
        generated
            .order_stream(Request::new(vec!["BUY:1".into(), "SELL:2".into()]))?
            .get_ref(),
        &["ack:BUY:1", "ack:SELL:2"]
    );
    let mut channel = generated.transport().clone();
    drop(generated);
    let mut early = channel.bidi_streaming(
        ORDER_SERVICE_ORDER_STREAM.path,
        C::default(),
        CallOptions::default(),
    )?;
    early.send(&"early".to_string())?;
    assert_eq!(early.recv_next()?, Some("ack:early".into()));
    assert_eq!(
        early.recv_next()?,
        None,
        "server OK before client half-close"
    );
    assert_eq!(early.trailing_metadata()?, Some(trailers()));
    drop(early);
    for (path, code, message) in [
        (
            "/hft.OrderService/Denied",
            Code::PermissionDenied,
            "risk 50%: reject\n策略",
        ),
        (
            "/hft.OrderService/Missing",
            Code::Unimplemented,
            "unknown method",
        ),
    ] {
        let error = channel
            .call_unary(path, &"x".to_string(), &mut C::default())
            .expect_err("trailers-only failure");
        assert_eq!(error.code(), code);
        if code == Code::PermissionDenied {
            assert_eq!(error.message(), message);
        }
    }
    for algorithm in [
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Deflate,
        CompressionAlgorithm::Snappy,
        CompressionAlgorithm::Identity,
    ] {
        for count in [10, 1024] {
            let request = "x".repeat(count);
            let mut encoded = Vec::new();
            C::default()
                .encode(&request, &mut encoded)
                .map_err(|e| io::Error::other(e.to_string()))?;
            let (response, _, _) = channel.unary(
                ORDER_SERVICE_PLACE_ORDER.path,
                &request,
                &mut C::default(),
                options(metadata(), Some(algorithm)),
            )?;
            assert_eq!(response, format!("accepted:{request}"));
            let snapshot = trace.lock().unwrap();
            let sent = wire(&snapshot.sent);
            let id = *sent.headers.keys().next_back().unwrap();
            let messages = LengthPrefixedFrame::new(1024 * 1024).push(&sent.data[&id])?;
            assert_eq!(messages.len(), 1);
            assert_eq!(
                messages[0].is_compressed(),
                encoded.len() >= 256 && algorithm != CompressionAlgorithm::Identity
            );
            if messages[0].is_compressed() {
                assert_eq!(
                    header(&sent, id, b"grpc-encoding"),
                    algorithm.as_str().as_bytes()
                );
                assert_eq!(
                    algorithm.decompress(messages[0].payload(), 1024 * 1024)?,
                    encoded
                );
            } else {
                assert_eq!(messages[0].payload(), encoded);
            }
        }
    }
    let (value, _, _) = channel.unary(
        ORDER_SERVICE_PLACE_ORDER.path,
        &"after-error".to_string(),
        &mut C::default(),
        options(metadata(), None),
    )?;
    assert_eq!(value, "accepted:after-error");
    Ok(())
}

#[test]
#[cfg(feature = "protobuf")]
fn e2e_memory_protobuf_generated_all_shapes_and_early_ok() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    exercise::<_, _, grpc::ProstCodec<String, String>>(client, server)
}
#[test]
#[cfg(feature = "fory")]
fn e2e_memory_fory_generated_all_shapes_and_early_ok() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    exercise::<_, _, grpc::ForyCodec<String>>(client, server)
}
#[test]
#[cfg(feature = "capnp")]
fn e2e_memory_capnp_aligned_borrowed_all_shapes_and_early_ok() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    exercise::<_, _, CapnpText>(client, server)
}
#[cfg(feature = "fory")]
type WireCodec = grpc::ForyCodec<String>;
#[cfg(all(not(feature = "fory"), feature = "protobuf"))]
type WireCodec = grpc::ProstCodec<String, String>;
#[cfg(all(not(feature = "fory"), not(feature = "protobuf"), feature = "capnp"))]
type WireCodec = CapnpText;

#[test]
fn e2e_tcp_generated_all_shapes_and_nodelay() -> TestResult {
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let client = TcpTransport::connect(listener.local_addr()?)?;
    let (server, _) = listener.accept()?;
    assert!(client.get_ref().nodelay()? && server.get_ref().nodelay()?);
    exercise::<_, _, WireCodec>(client, server)
}
#[cfg(unix)]
#[test]
fn e2e_unix_generated_all_shapes() -> TestResult {
    use grpc::core::transport::{UnixTransport, UnixTransportListener};
    let dir = std::env::temp_dir().join(format!("grpc-hft-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("order.sock");
    let listener = UnixTransportListener::bind(&path)?;
    let client = UnixTransport::connect(&path)?;
    let (server, _) = listener.accept()?;
    let result = exercise::<_, _, WireCodec>(client, server);
    drop(listener);
    std::fs::remove_file(path)?;
    std::fs::remove_dir(dir)?;
    result
}

#[test]
fn e2e_tls13_h2_loopback_generated_all_shapes() -> TestResult {
    use grpc::core::tls::{TlsClientConfig, TlsServerConfig, TlsTransport};
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = CertificateDer::from(identity.cert.der().to_vec());
    let key = PrivatePkcs8KeyDer::from(identity.key_pair.serialize_der()).into();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.clone())?;
    let client_config = TlsClientConfig::new(roots)?;
    let server_config = TlsServerConfig::new(vec![cert], key)?;
    assert_eq!(client_config.get_ref().alpn_protocols, [b"h2".to_vec()]);
    assert_eq!(server_config.get_ref().alpn_protocols, [b"h2".to_vec()]);
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let mut socket = TcpTransport::connect(listener.local_addr()?)?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    socket.set_write_timeout(Some(Duration::from_secs(5)))?;
    let worker = thread::spawn(move || -> Result<(), Status> {
        let (mut socket, _) = listener
            .accept()
            .map_err(|e| Status::unavailable(e.to_string()))?;
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| Status::unavailable(e.to_string()))?;
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut tls = TlsTransport::server(socket, &server_config)
            .map_err(|e| Status::unavailable(e.to_string()))?;
        tls.handshake()
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut server = SyncServer::new(tls)?;
        register::<_, WireCodec>(&mut server)?;
        server.serve_connection()
    });
    let mut tls = TlsTransport::client(socket, &client_config, "localhost".to_owned().try_into()?)?;
    tls.handshake()?;
    assert_eq!(tls.negotiated_alpn(), Some(b"h2".as_slice()));
    assert_eq!(
        tls.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
    let tls = Rc::new(RefCell::new(tls));
    let trace = Arc::new(Mutex::new(Trace::default()));
    let result = client_calls::<_, WireCodec>(Rc::clone(&tls), Arc::clone(&trace), "https");
    let closed = (|| -> io::Result<()> {
        let mut transport = tls.borrow_mut();
        transport.set_nonblocking(false)?;
        transport.set_write_timeout(Some(Duration::from_secs(5)))?;
        transport.close_notify()
    })();
    drop(tls);
    let served = worker.join().expect("TLS server panicked");
    result?;
    closed?;
    served?;
    check_trace(&trace);
    Ok(())
}

#[test]
fn e2e_compressed_response_exceeding_decoded_limit_is_resource_exhausted() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(server)?;
        register::<_, WireCodec>(&mut server)?;
        server.serve_connection()
    });
    let result = (|| -> TestResult {
        let mut config = ChannelConfig {
            default_timeout: Some(Duration::from_secs(5)),
            ..ChannelConfig::default()
        };
        config.session_config.compression.max_decoding_message_size = 128;
        let mut channel = SyncChannel::with_config(client, config)?;
        let error = channel
            .unary(
                ORDER_SERVICE_PLACE_ORDER.path,
                &"large".repeat(1000),
                &mut WireCodec::default(),
                options(metadata(), Some(CompressionAlgorithm::Zstd)),
            )
            .expect_err("decompressed response exceeds 128 bytes");
        assert_eq!(error.code(), Code::ResourceExhausted);
        Ok(())
    })();
    let served = worker.join().expect("server panicked");
    result?;
    served?;
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
            trace: Arc::new(Mutex::new(Trace::default())),
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
    let trace = Arc::new(Mutex::new(Trace::default()));
    let client = Rc::new(RefCell::new(client));
    let result = (|| -> TestResult {
        let mut channel = SyncChannel::with_config(
            Recorded {
                inner: Rc::clone(&client),
                blocked: Rc::default(),
                trace: Arc::clone(&trace),
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
    trace.lock().unwrap().received.extend_from_slice(&tail);
    let received = wire(&trace.lock().unwrap().received);
    assert_eq!(received.goaway, Some((1, 0)));
    assert_eq!(header(&received, 1, b"grpc-status"), b"0");
    Ok(())
}
