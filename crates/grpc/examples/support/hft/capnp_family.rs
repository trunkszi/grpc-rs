//! Directional codecs retain generated Cap'n Proto roots as typed builders.
use capnp::{message::TypedBuilder, traits::Owned};
use grpc::{Codec, Status, SyncServer, core::transport::TcpTransport};
use grpc_codec_capnp::CapnpCodec;

use super::{Family, hft_order_capnp as wire};

pub(super) struct BuilderCodec<E: Owned, D: Owned>(CapnpCodec<E, D>);
impl<E: Owned, D: Owned> Codec for BuilderCodec<E, D> {
    type EncodeItem = TypedBuilder<E>;
    type DecodeItem = TypedBuilder<D>;
    type Error = capnp::Error;
    const CONTENT_TYPE: &'static str = CapnpCodec::<E, D>::CONTENT_TYPE;
    fn encode(
        &mut self,
        value: &Self::EncodeItem,
        destination: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        self.0.encode(value, destination)
    }
    fn decode(&mut self, payload: &[u8]) -> Result<Self::DecodeItem, Self::Error> {
        let reader = self.0.decode(payload)?;
        let mut builder = TypedBuilder::<D>::new_default();
        builder.set_root(reader.get()?)?;
        Ok(builder)
    }
}
// The service's message names resolve to generated, owned typed roots.
type OrderRequest = TypedBuilder<wire::order_request::Owned>;
type OrderAck = TypedBuilder<wire::order_ack::Owned>;
type DepthRequest = TypedBuilder<wire::depth_request::Owned>;
type DepthLevel = TypedBuilder<wire::depth_level::Owned>;
type FillReport = TypedBuilder<wire::fill_report::Owned>;
type FillSummary = TypedBuilder<wire::fill_summary::Owned>;
include!(concat!(env!("OUT_DIR"), "/capnp/hft_order_grpc.rs"));

pub(super) struct Capnp;
super::service_family!(
    Capnp,
    order_service,
    placeOrder,
    subscribeDepth,
    uploadFills,
    orderStream
);
impl Family for Capnp {
    type Order = OrderRequest;
    type Ack = OrderAck;
    type DepthRequest = DepthRequest;
    type Depth = DepthLevel;
    type Fill = FillReport;
    type Summary = FillSummary;
    type Orders = BuilderCodec<wire::order_request::Owned, wire::order_ack::Owned>;
    type Acks = BuilderCodec<wire::order_ack::Owned, wire::order_request::Owned>;
    type DepthRequests = BuilderCodec<wire::depth_request::Owned, wire::depth_level::Owned>;
    type Depths = BuilderCodec<wire::depth_level::Owned, wire::depth_request::Owned>;
    type Fills = BuilderCodec<wire::fill_report::Owned, wire::fill_summary::Owned>;
    type Summaries = BuilderCodec<wire::fill_summary::Owned, wire::fill_report::Owned>;
    fn orders() -> Result<Self::Orders, Status> {
        Ok(BuilderCodec(CapnpCodec::new()))
    }
    fn acks() -> Result<Self::Acks, Status> {
        Ok(BuilderCodec(CapnpCodec::new()))
    }
    fn depth_requests() -> Result<Self::DepthRequests, Status> {
        Ok(BuilderCodec(CapnpCodec::new()))
    }
    fn depths() -> Result<Self::Depths, Status> {
        Ok(BuilderCodec(CapnpCodec::new()))
    }
    fn fills() -> Result<Self::Fills, Status> {
        Ok(BuilderCodec(CapnpCodec::new()))
    }
    fn summaries() -> Result<Self::Summaries, Status> {
        Ok(BuilderCodec(CapnpCodec::new()))
    }
    fn order(id: u64) -> Self::Order {
        let mut message = OrderRequest::new_default();
        {
            let mut root = message.init_root();
            root.set_client_order_id(id);
            root.set_instrument_id(42);
            root.set_quantity(17);
            root.set_price_ticks(-12345);
        }
        message
    }
    fn order_fields(value: &Self::Order) -> Result<(u64, u64), Status> {
        let root = value.get_root_as_reader().map_err(super::status)?;
        if root.get_instrument_id() != 42 || root.get_price_ticks() != -12345 {
            return Err(Status::invalid_argument(
                "order instrument or ticks mismatch",
            ));
        }
        Ok((root.get_client_order_id(), root.get_quantity()))
    }
    fn ack(id: u64, quantity: u64) -> Self::Ack {
        let mut message = OrderAck::new_default();
        {
            let mut root = message.init_root();
            root.set_client_order_id(id);
            root.set_accepted(true);
            root.set_quantity(quantity);
        }
        message
    }
    fn ack_fields(value: &Self::Ack) -> Result<(u64, bool, u64), Status> {
        let root = value.get_root_as_reader().map_err(super::status)?;
        Ok((
            root.get_client_order_id(),
            root.get_accepted(),
            root.get_quantity(),
        ))
    }
    fn depth_request() -> Self::DepthRequest {
        let mut message = DepthRequest::new_default();
        {
            let mut root = message.init_root();
            root.set_instrument_id(42);
            root.set_levels(3);
        }
        message
    }
    fn depth_count(value: &Self::DepthRequest) -> Result<u32, Status> {
        Ok(value
            .get_root_as_reader()
            .map_err(super::status)?
            .get_levels())
    }
    fn depth(level: u32) -> Self::Depth {
        let mut message = DepthLevel::new_default();
        {
            let mut root = message.init_root();
            root.set_level(level);
            root.set_quantity(20);
            root.set_price_ticks(-100 + i64::from(level));
        }
        message
    }
    fn depth_fields(value: &Self::Depth) -> Result<(i64, u64, u32), Status> {
        let root = value.get_root_as_reader().map_err(super::status)?;
        Ok((
            root.get_price_ticks(),
            root.get_quantity(),
            root.get_level(),
        ))
    }
    fn fill(quantity: u64) -> Self::Fill {
        let mut message = FillReport::new_default();
        {
            let mut root = message.init_root();
            root.set_client_order_id(11);
            root.set_quantity(quantity);
        }
        message
    }
    fn fill_quantity(value: &Self::Fill) -> Result<u64, Status> {
        Ok(value
            .get_root_as_reader()
            .map_err(super::status)?
            .get_quantity())
    }
    fn summary(total: u64, count: u32) -> Self::Summary {
        let mut message = FillSummary::new_default();
        {
            let mut root = message.init_root();
            root.set_total_quantity(total);
            root.set_fill_count(count);
        }
        message
    }
    fn summary_fields(value: &Self::Summary) -> Result<(u64, u32), Status> {
        let root = value.get_root_as_reader().map_err(super::status)?;
        Ok((root.get_total_quantity(), root.get_fill_count()))
    }
    fn paths() -> [&'static str; 4] {
        [
            order_service::ORDER_SERVICE_PLACE_ORDER.path,
            order_service::ORDER_SERVICE_SUBSCRIBE_DEPTH.path,
            order_service::ORDER_SERVICE_UPLOAD_FILLS.path,
            order_service::ORDER_SERVICE_ORDER_STREAM.path,
        ]
    }
    fn register(server: &mut SyncServer<TcpTransport>) -> Result<(), Status> {
        super::register_family!(
            Capnp,
            order_service,
            server,
            placeOrder,
            subscribeDepth,
            uploadFills,
            orderStream
        )
    }
}
