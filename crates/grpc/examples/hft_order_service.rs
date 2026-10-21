//! Generated HFT service adapters over real TCP, with three codecs and Zstd.
//! String payloads use Protobuf StringValue, Fory string, or Cap'n Proto text.
use std::{
    thread,
    time::{Duration, Instant},
};

use grpc::{
    CallOptions, ChannelConfig, Codec, CompressionAlgorithm, ForyCodec, Metadata, MethodDescriptor,
    ProstCodec, Request, Response, Status, SyncChannel, SyncServer,
    core::transport::{TcpTransport, TcpTransportListener},
    stream::StreamError,
};
use grpc_codec_capnp::{
    CapnpCodec,
    backend::{message::TypedBuilder, text},
};

type ExampleResult = Result<(), Box<dyn std::error::Error>>;

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
impl<C> OrderServiceChannel<C> for SyncChannel<TcpTransport>
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
        let (value, initial, _) = self.unary(
            method.path,
            &request,
            codec,
            options(metadata, *compression),
        )?;
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
        Ok(Response::new(stream.finish().map_err(stream_status)?))
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
fn register<C>(server: &mut SyncServer<TcpTransport>) -> Result<(), Status>
where
    C: Codec<EncodeItem = String, DecodeItem = String> + Default + 'static,
{
    let mut unary = OrderServiceServer::new(OrderBook, ());
    server.register_unary(
        ORDER_SERVICE_PLACE_ORDER.path,
        C::default(),
        move |request| unary.place_order(request),
    )?;
    let mut depth = OrderServiceServer::new(OrderBook, ());
    server.register_server_streaming(
        ORDER_SERVICE_SUBSCRIBE_DEPTH.path,
        C::default(),
        move |request, sink| {
            for snapshot in depth.subscribe_depth(request)?.get_ref() {
                sink.send(snapshot).map_err(stream_status)?;
            }
            Ok(())
        },
    )?;
    let mut fills = OrderServiceServer::new(OrderBook, ());
    server.register_client_streaming(
        ORDER_SERVICE_UPLOAD_FILLS.path,
        C::default(),
        move |mut stream| {
            let mut requests = Vec::new();
            while let Some(fill) = stream.recv_next().map_err(stream_status)? {
                requests.push(fill);
            }
            fills.upload_fills(Request::new(requests))
        },
    )?;
    let mut bidi = OrderServiceServer::new(OrderBook, ());
    server.register_bidi_streaming(
        ORDER_SERVICE_ORDER_STREAM.path,
        C::default(),
        move |mut stream, sink| {
            while let Some(order) = stream.recv_next().map_err(stream_status)? {
                for acknowledgement in bidi.order_stream(Request::new(vec![order]))?.get_ref() {
                    sink.send(acknowledgement).map_err(stream_status)?;
                }
            }
            Ok(())
        },
    )?;
    Ok(())
}

/// Bridges the typed Cap'n Proto text codec to the service's string domain.
#[derive(Default)]
struct CapnpTextCodec(CapnpCodec<text::Owned, text::Owned>);
impl Codec for CapnpTextCodec {
    type EncodeItem = String;
    type DecodeItem = String;
    type Error = grpc_codec_capnp::backend::Error;
    const CONTENT_TYPE: &'static str = CapnpCodec::<text::Owned, text::Owned>::CONTENT_TYPE;
    fn encode(&mut self, value: &String, destination: &mut Vec<u8>) -> Result<(), Self::Error> {
        let mut message = TypedBuilder::<text::Owned>::new_default();
        message.set_root(value.as_str().into())?;
        self.0.encode(&message, destination)
    }
    fn decode(&mut self, payload: &[u8]) -> Result<String, Self::Error> {
        let message = self.0.decode(payload)?;
        Ok(message.get()?.to_str()?.to_string())
    }
}

fn run<C>(label: &str) -> ExampleResult
where
    C: Codec<EncodeItem = String, DecodeItem = String> + Default + 'static,
{
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> Result<(), Status> {
        let (transport, _) = listener
            .accept()
            .map_err(|error| Status::unavailable(error.to_string()))?;
        assert!(
            transport
                .get_ref()
                .nodelay()
                .map_err(|error| Status::unavailable(error.to_string()))?
        );
        let mut server = SyncServer::new(transport)?;
        register::<C>(&mut server)?;
        server.serve_connection()
    });
    let outcome = (|| -> ExampleResult {
        let transport = TcpTransport::connect(address)?;
        assert!(transport.get_ref().nodelay()?);
        let channel = SyncChannel::with_config(
            transport,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                ..ChannelConfig::default()
            },
        )?;
        let mut client = OrderServiceClient::new(channel, ())
            .with_codec::<C>()
            .with_compression(CompressionAlgorithm::Zstd);
        // Above the 256-byte compression threshold for every wire codec.
        let order = format!("BUY:42:{}", "symbol-context;".repeat(24));
        let start = Instant::now();
        assert_eq!(
            client.place_order(Request::new(order.clone()))?.get_ref(),
            &format!("accepted:{order}")
        );
        let unary = start.elapsed();
        let start = Instant::now();
        assert_eq!(
            client
                .subscribe_depth(Request::new("ACME".to_string()))?
                .get_ref(),
            &["ACME:depth:1", "ACME:depth:2", "ACME:depth:3"]
        );
        let depth = start.elapsed();
        let start = Instant::now();
        assert_eq!(
            client
                .upload_fills(Request::new(vec!["3".into(), "5".into(), "7".into()]))?
                .get_ref(),
            "filled:15"
        );
        let fills = start.elapsed();
        let start = Instant::now();
        assert_eq!(
            client
                .order_stream(Request::new(vec!["BUY:1".into(), "SELL:2".into()]))?
                .get_ref(),
            &["ack:BUY:1", "ack:SELL:2"]
        );
        println!(
            "{label}+Zstd TCP_NODELAY: accepted order; depth=3 fills=15 bidi=2; latency us \
             unary={} depth={} fills={} bidi={}",
            unary.as_micros(),
            depth.as_micros(),
            fills.as_micros(),
            start.elapsed().as_micros()
        );
        Ok(())
    })();
    let served = server.join().map_err(|_| "server thread panicked")?;
    outcome?;
    served?;
    Ok(())
}
fn main() -> ExampleResult {
    run::<ProstCodec<String, String>>("Protobuf")?;
    run::<ForyCodec<String>>("Fory")?;
    run::<CapnpTextCodec>("Capnp")
}
