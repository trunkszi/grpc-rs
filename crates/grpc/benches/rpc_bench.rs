//! Individual caller-thread RPC shapes over loopback TCP with typed codec validation.
use std::{thread, time::Duration};

use anyhow::{Context, Result, ensure};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use grpc::{
    CallOptions, ChannelConfig, Codec, CompressionAlgorithm, Request, Response, Status,
    StreamError, SyncChannel, SyncServer,
    core::transport::{TcpTransport, TcpTransportListener},
};
#[path = "../examples/support/order/capnp_family.rs"]
mod capnp_family;
#[path = "../examples/support/order/families.rs"]
mod families;

mod order {
    pub mod protobuf {
        include!(concat!(env!("OUT_DIR"), "/protobuf/order.rs"));
        include!(concat!(env!("OUT_DIR"), "/protobuf/order_grpc.rs"));
    }
    // Compiler-generated serializers include convenience methods without rustdoc.
    #[allow(dead_code, clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub mod fory {
        include!(concat!(env!("OUT_DIR"), "/fory/fory/order.rs"));
        include!(concat!(env!("OUT_DIR"), "/fory/order_grpc.rs"));
    }
    // Native capability bindings are generated too; gRPC uses the lowered service.
    #[allow(dead_code, clippy::expl_impl_clone_on_copy, clippy::unimplemented)]
    pub mod order_capnp {
        include!(concat!(env!("OUT_DIR"), "/capnp/order_capnp.rs"));
    }
}
use order::{fory, order_capnp, protobuf};

trait Family: 'static {
    type Order: 'static;
    type Ack: 'static;
    type DepthRequest: 'static;
    type Depth: 'static;
    type Fill: 'static;
    type Summary: 'static;
    type Orders: Codec<EncodeItem = Self::Order, DecodeItem = Self::Ack> + 'static;
    type Acks: Codec<EncodeItem = Self::Ack, DecodeItem = Self::Order> + 'static;
    type DepthRequests: Codec<EncodeItem = Self::DepthRequest, DecodeItem = Self::Depth> + 'static;
    type Depths: Codec<EncodeItem = Self::Depth, DecodeItem = Self::DepthRequest> + 'static;
    type Fills: Codec<EncodeItem = Self::Fill, DecodeItem = Self::Summary> + 'static;
    type Summaries: Codec<EncodeItem = Self::Summary, DecodeItem = Self::Fill> + 'static;
    fn orders() -> Result<Self::Orders, Status>;
    fn acks() -> Result<Self::Acks, Status>;
    fn depth_requests() -> Result<Self::DepthRequests, Status>;
    fn depths() -> Result<Self::Depths, Status>;
    fn fills() -> Result<Self::Fills, Status>;
    fn summaries() -> Result<Self::Summaries, Status>;
    fn order(id: u64) -> Self::Order;
    fn order_fields(order: &Self::Order) -> Result<(u64, u64), Status>;
    fn ack(id: u64, quantity: u64) -> Self::Ack;
    fn ack_fields(ack: &Self::Ack) -> Result<(u64, bool, u64), Status>;
    fn depth_request() -> Self::DepthRequest;
    fn depth_count(request: &Self::DepthRequest) -> Result<u32, Status>;
    fn depth(level: u32) -> Self::Depth;
    fn depth_fields(value: &Self::Depth) -> Result<(i64, u64, u32), Status>;
    fn fill(quantity: u64) -> Self::Fill;
    fn fill_quantity(fill: &Self::Fill) -> Result<u64, Status>;
    fn summary(total: u64, count: u32) -> Self::Summary;
    fn summary_fields(summary: &Self::Summary) -> Result<(u64, u32), Status>;
    fn paths() -> [&'static str; 4];
    fn register(server: &mut SyncServer<TcpTransport>) -> Result<(), Status>;
}
fn status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}
fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking RPC remained pending"),
    }
}
fn options() -> CallOptions {
    CallOptions {
        compression: Some(CompressionAlgorithm::Zstd),
        ..CallOptions::default()
    }
}
struct Book<F>(std::marker::PhantomData<F>);
impl<F: Family> Book<F> {
    fn place(&mut self, order: Request<F::Order>) -> Result<Response<F::Ack>, Status> {
        let (id, quantity) = F::order_fields(order.get_ref())?;
        if quantity == 0 {
            return Err(Status::invalid_argument("order quantity must be positive"));
        }
        Ok(Response::new(F::ack(id, quantity)))
    }
    fn depth(
        &mut self,
        request: Request<F::DepthRequest>,
    ) -> Result<Response<Vec<F::Depth>>, Status> {
        let count = F::depth_count(request.get_ref())?;
        if count > 32 {
            return Err(Status::resource_exhausted(
                "depth request exceeds 32 levels",
            ));
        }
        Ok(Response::new((1..=count).map(F::depth).collect()))
    }
    fn fills(&mut self, request: Request<Vec<F::Fill>>) -> Result<Response<F::Summary>, Status> {
        let mut total = 0u64;
        for fill in request.get_ref() {
            total = total
                .checked_add(F::fill_quantity(fill)?)
                .ok_or_else(|| Status::out_of_range("fill quantity overflow"))?;
        }
        let count = request
            .get_ref()
            .len()
            .try_into()
            .map_err(|_| Status::out_of_range("fill count overflow"))?;
        Ok(Response::new(F::summary(total, count)))
    }
    fn orders(&mut self, request: Request<Vec<F::Order>>) -> Result<Response<Vec<F::Ack>>, Status> {
        let mut acks = Vec::new();
        for order in request.get_ref() {
            let (id, quantity) = F::order_fields(order)?;
            acks.push(F::ack(id, quantity));
        }
        Ok(Response::new(acks))
    }
}
macro_rules! service_family {
    ($family:ty, $schema:ident, $place:ident, $depth:ident, $fills:ident, $orders:ident) => {
        impl $schema::OrderService for super::Book<$family> {
            fn $place(
                &mut self,
                r: grpc::Request<<$family as super::Family>::Order>,
            ) -> Result<grpc::Response<<$family as super::Family>::Ack>, grpc::Status> {
                self.place(r)
            }
            fn $depth(
                &mut self,
                r: grpc::Request<<$family as super::Family>::DepthRequest>,
            ) -> Result<grpc::Response<Vec<<$family as super::Family>::Depth>>, grpc::Status> {
                self.depth(r)
            }
            fn $fills(
                &mut self,
                r: grpc::Request<Vec<<$family as super::Family>::Fill>>,
            ) -> Result<grpc::Response<<$family as super::Family>::Summary>, grpc::Status> {
                self.fills(r)
            }
            fn $orders(
                &mut self,
                r: grpc::Request<Vec<<$family as super::Family>::Order>>,
            ) -> Result<grpc::Response<Vec<<$family as super::Family>::Ack>>, grpc::Status> {
                self.orders(r)
            }
        }
    };
}
pub(crate) use service_family;
macro_rules! register_family {
    ($family:ty, $schema:ident, $server:ident, $place:ident, $depth:ident, $fills:ident, $orders:ident) => {{
        let paths = <$family as super::Family>::paths();
        let book = || super::Book::<$family>(std::marker::PhantomData);
        let mut unary = $schema::OrderServiceServer::new(book(), ());
        $server.register_unary(paths[0], <$family>::acks()?, move |r| unary.$place(r))?;
        let mut depth = $schema::OrderServiceServer::new(book(), ());
        $server.register_server_streaming(paths[1], <$family>::depths()?, move |r, sink| {
            for value in depth.$depth(r)?.get_ref() {
                sink.send(value).map_err(super::stream_status)?;
            }
            Ok(())
        })?;
        let mut fills = $schema::OrderServiceServer::new(book(), ());
        $server.register_client_streaming(
            paths[2],
            <$family>::summaries()?,
            move |mut stream| {
                let mut values = Vec::new();
                while let Some(value) = stream.recv_next().map_err(super::stream_status)? {
                    values.push(value);
                }
                fills.$fills(grpc::Request::new(values))
            },
        )?;
        let mut orders = $schema::OrderServiceServer::new(book(), ());
        $server.register_bidi_streaming(
            paths[3],
            <$family>::acks()?,
            move |mut stream, sink| {
                while let Some(value) = stream.recv_next().map_err(super::stream_status)? {
                    for ack in orders.$orders(grpc::Request::new(vec![value]))?.get_ref() {
                        sink.send(ack).map_err(super::stream_status)?;
                    }
                }
                Ok(())
            },
        )?;
        Ok(())
    }};
}
pub(crate) use register_family;

// Serializer registration is cold setup; owned stream cleanup remains part of each RPC.
enum PreparedRpc<F: Family> {
    Orders(F::Orders),
    Depth(F::DepthRequests),
    Fills(F::Fills),
}
fn prepare<F: Family>(shape: usize) -> Result<PreparedRpc<F>> {
    Ok(match shape {
        0 | 3 => PreparedRpc::Orders(F::orders()?),
        1 => PreparedRpc::Depth(F::depth_requests()?),
        2 => PreparedRpc::Fills(F::fills()?),
        _ => unreachable!("fixed RPC shape"),
    })
}
fn rpc<F: Family>(
    channel: &mut SyncChannel<TcpTransport>,
    shape: usize,
    codec: PreparedRpc<F>,
) -> Result<()> {
    let paths = F::paths();
    match (shape, codec) {
        (0, PreparedRpc::Orders(mut codec)) => {
            let id = (1u64 << 54) + 7;
            let (ack, _, _) = channel.unary(paths[0], &F::order(id), &mut codec, options())?;
            ensure!(
                F::ack_fields(&ack)? == (id, true, 17),
                "unary response mismatch"
            );
        }
        (1, PreparedRpc::Depth(codec)) => {
            let levels = channel
                .server_streaming(paths[1], &F::depth_request(), codec, options())?
                .finish()?;
            ensure!(levels.len() == 3, "server stream count mismatch");
            for (i, level) in levels.iter().enumerate() {
                ensure!(
                    F::depth_fields(level)? == (-99 + i as i64, 20, i as u32 + 1),
                    "depth mismatch"
                );
            }
        }
        (2, PreparedRpc::Fills(codec)) => {
            let mut stream = channel.client_streaming(paths[2], codec, options())?;
            for quantity in [3, 5, 7] {
                stream.send(&F::fill(quantity))?;
            }
            ensure!(
                F::summary_fields(&stream.finish()?)? == (15, 3),
                "client stream mismatch"
            );
        }
        (3, PreparedRpc::Orders(codec)) => {
            let mut stream = channel.bidi_streaming(paths[3], codec, options())?;
            for id in [11, 13] {
                stream.send(&F::order(id))?;
                let ack = stream.recv_next()?.context("missing bidi response")?;
                ensure!(
                    F::ack_fields(&ack)? == (id, true, 17),
                    "bidi response mismatch"
                );
            }
            ensure!(stream.finish()?.is_empty(), "unexpected bidi response");
        }
        _ => unreachable!("fixed RPC shape"),
    }
    Ok(())
}
fn family<F: Family>(c: &mut Criterion, name: &str) -> Result<()> {
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let transport = TcpTransport::connect(listener.local_addr()?)?;
    let server = thread::spawn(move || -> Result<(), Status> {
        let (transport, _) = listener.accept().map_err(status)?;
        let mut server = SyncServer::new(transport)?;
        F::register(&mut server)?;
        server.serve_connection()
    });
    {
        let mut channel = SyncChannel::with_config(
            transport,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                ..ChannelConfig::default()
            },
        )?;
        let mut group = c.benchmark_group(format!("rpc/{name}/zstd"));
        for (shape, name) in [
            "unary",
            "server_streaming",
            "client_streaming",
            "bidi_streaming",
        ]
        .into_iter()
        .enumerate()
        {
            rpc::<F>(&mut channel, shape, prepare::<F>(shape)?)?;
            group.bench_function(name, |b| {
                b.iter_batched(
                    || prepare::<F>(shape).expect("codec preparation"),
                    |codec| rpc::<F>(&mut channel, shape, codec).expect("RPC benchmark"),
                    BatchSize::SmallInput,
                )
            });
        }
        group.finish();
    }
    server
        .join()
        .map_err(|_| anyhow::anyhow!("benchmark server panicked"))??;
    Ok(())
}
fn rpcs(c: &mut Criterion) {
    family::<families::Protobuf>(c, "protobuf").expect("protobuf benchmark");
    family::<families::Fory>(c, "fory").expect("fory benchmark");
    family::<capnp_family::Capnp>(c, "capnp").expect("capnp benchmark");
}
criterion_group! { name = benches; config = Criterion::default().sample_size(20); targets = rpcs }
criterion_main!(benches);
