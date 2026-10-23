use grpc::{Status, SyncServer, core::transport::TcpTransport};

use super::{Family, fory, protobuf};

fn fory_codec<E: ::fory::Serializer, D: ::fory::Serializer>()
-> Result<grpc::ForyCodec<E, D>, Status> {
    let mut backend = ::fory::Fory::builder()
        .xlang(true)
        .track_ref(true)
        .compatible(true)
        .build();
    fory::register_types(&mut backend).map_err(super::status)?;
    Ok(grpc::ForyCodec::with_fory(backend))
}
pub(super) struct Protobuf;
super::service_family!(
    Protobuf,
    protobuf_service,
    PlaceOrder,
    SubscribeDepth,
    UploadFills,
    OrderStream
);
use protobuf::order_service as protobuf_service;
impl Family for Protobuf {
    type Order = protobuf::OrderRequest;
    type Ack = protobuf::OrderAck;
    type DepthRequest = protobuf::DepthRequest;
    type Depth = protobuf::DepthLevel;
    type Fill = protobuf::FillReport;
    type Summary = protobuf::FillSummary;
    type Orders = grpc::ProstCodec<Self::Order, Self::Ack>;
    type Acks = grpc::ProstCodec<Self::Ack, Self::Order>;
    type DepthRequests = grpc::ProstCodec<Self::DepthRequest, Self::Depth>;
    type Depths = grpc::ProstCodec<Self::Depth, Self::DepthRequest>;
    type Fills = grpc::ProstCodec<Self::Fill, Self::Summary>;
    type Summaries = grpc::ProstCodec<Self::Summary, Self::Fill>;
    fn orders() -> Result<Self::Orders, Status> {
        Ok(grpc::ProstCodec::new())
    }
    fn acks() -> Result<Self::Acks, Status> {
        Ok(grpc::ProstCodec::new())
    }
    fn depth_requests() -> Result<Self::DepthRequests, Status> {
        Ok(grpc::ProstCodec::new())
    }
    fn depths() -> Result<Self::Depths, Status> {
        Ok(grpc::ProstCodec::new())
    }
    fn fills() -> Result<Self::Fills, Status> {
        Ok(grpc::ProstCodec::new())
    }
    fn summaries() -> Result<Self::Summaries, Status> {
        Ok(grpc::ProstCodec::new())
    }
    fn order(id: u64) -> Self::Order {
        protobuf::OrderRequest {
            client_order_id: id,
            instrument_id: 42,
            quantity: 17,
            price_ticks: -12345,
        }
    }
    fn order_fields(value: &Self::Order) -> Result<(u64, u64), Status> {
        if value.instrument_id != 42 || value.price_ticks != -12345 {
            return Err(Status::invalid_argument(
                "order instrument or ticks mismatch",
            ));
        }
        Ok((value.client_order_id, value.quantity))
    }
    fn ack(id: u64, quantity: u64) -> Self::Ack {
        protobuf::OrderAck {
            client_order_id: id,
            accepted: true,
            quantity,
        }
    }
    fn ack_fields(value: &Self::Ack) -> Result<(u64, bool, u64), Status> {
        Ok((value.client_order_id, value.accepted, value.quantity))
    }
    fn depth_request() -> Self::DepthRequest {
        protobuf::DepthRequest {
            instrument_id: 42,
            levels: 3,
        }
    }
    fn depth_count(value: &Self::DepthRequest) -> Result<u32, Status> {
        Ok(value.levels)
    }
    fn depth(level: u32) -> Self::Depth {
        protobuf::DepthLevel {
            price_ticks: -100 + i64::from(level),
            quantity: 20,
            level,
        }
    }
    fn depth_fields(value: &Self::Depth) -> Result<(i64, u64, u32), Status> {
        Ok((value.price_ticks, value.quantity, value.level))
    }
    fn fill(quantity: u64) -> Self::Fill {
        protobuf::FillReport {
            client_order_id: 11,
            quantity,
        }
    }
    fn fill_quantity(value: &Self::Fill) -> Result<u64, Status> {
        Ok(value.quantity)
    }
    fn summary(total_quantity: u64, fill_count: u32) -> Self::Summary {
        protobuf::FillSummary {
            total_quantity,
            fill_count,
        }
    }
    fn summary_fields(value: &Self::Summary) -> Result<(u64, u32), Status> {
        Ok((value.total_quantity, value.fill_count))
    }
    fn paths() -> [&'static str; 4] {
        [
            protobuf_service::ORDER_SERVICE_PLACE_ORDER.path,
            protobuf_service::ORDER_SERVICE_SUBSCRIBE_DEPTH.path,
            protobuf_service::ORDER_SERVICE_UPLOAD_FILLS.path,
            protobuf_service::ORDER_SERVICE_ORDER_STREAM.path,
        ]
    }
    fn register(server: &mut SyncServer<TcpTransport>) -> Result<(), Status> {
        super::register_family!(
            Protobuf,
            protobuf_service,
            server,
            PlaceOrder,
            SubscribeDepth,
            UploadFills,
            OrderStream
        )
    }
}
pub(super) struct Fory;
super::service_family!(
    Fory,
    fory_service,
    PlaceOrder,
    SubscribeDepth,
    UploadFills,
    OrderStream
);
use fory::order_service as fory_service;
impl Family for Fory {
    type Order = fory::OrderRequest;
    type Ack = fory::OrderAck;
    type DepthRequest = fory::DepthRequest;
    type Depth = fory::DepthLevel;
    type Fill = fory::FillReport;
    type Summary = fory::FillSummary;
    type Orders = grpc::ForyCodec<Self::Order, Self::Ack>;
    type Acks = grpc::ForyCodec<Self::Ack, Self::Order>;
    type DepthRequests = grpc::ForyCodec<Self::DepthRequest, Self::Depth>;
    type Depths = grpc::ForyCodec<Self::Depth, Self::DepthRequest>;
    type Fills = grpc::ForyCodec<Self::Fill, Self::Summary>;
    type Summaries = grpc::ForyCodec<Self::Summary, Self::Fill>;
    fn orders() -> Result<Self::Orders, Status> {
        fory_codec()
    }
    fn acks() -> Result<Self::Acks, Status> {
        fory_codec()
    }
    fn depth_requests() -> Result<Self::DepthRequests, Status> {
        fory_codec()
    }
    fn depths() -> Result<Self::Depths, Status> {
        fory_codec()
    }
    fn fills() -> Result<Self::Fills, Status> {
        fory_codec()
    }
    fn summaries() -> Result<Self::Summaries, Status> {
        fory_codec()
    }
    fn order(id: u64) -> Self::Order {
        fory::OrderRequest {
            client_order_id: id,
            instrument_id: 42,
            quantity: 17,
            price_ticks: -12345,
        }
    }
    fn order_fields(value: &Self::Order) -> Result<(u64, u64), Status> {
        if value.instrument_id != 42 || value.price_ticks != -12345 {
            return Err(Status::invalid_argument(
                "order instrument or ticks mismatch",
            ));
        }
        Ok((value.client_order_id, value.quantity))
    }
    fn ack(id: u64, quantity: u64) -> Self::Ack {
        fory::OrderAck {
            client_order_id: id,
            accepted: true,
            quantity,
        }
    }
    fn ack_fields(value: &Self::Ack) -> Result<(u64, bool, u64), Status> {
        Ok((value.client_order_id, value.accepted, value.quantity))
    }
    fn depth_request() -> Self::DepthRequest {
        fory::DepthRequest {
            instrument_id: 42,
            levels: 3,
        }
    }
    fn depth_count(value: &Self::DepthRequest) -> Result<u32, Status> {
        Ok(value.levels)
    }
    fn depth(level: u32) -> Self::Depth {
        fory::DepthLevel {
            price_ticks: -100 + i64::from(level),
            quantity: 20,
            level,
        }
    }
    fn depth_fields(value: &Self::Depth) -> Result<(i64, u64, u32), Status> {
        Ok((value.price_ticks, value.quantity, value.level))
    }
    fn fill(quantity: u64) -> Self::Fill {
        fory::FillReport {
            client_order_id: 11,
            quantity,
        }
    }
    fn fill_quantity(value: &Self::Fill) -> Result<u64, Status> {
        Ok(value.quantity)
    }
    fn summary(total_quantity: u64, fill_count: u32) -> Self::Summary {
        fory::FillSummary {
            total_quantity,
            fill_count,
        }
    }
    fn summary_fields(value: &Self::Summary) -> Result<(u64, u32), Status> {
        Ok((value.total_quantity, value.fill_count))
    }
    fn paths() -> [&'static str; 4] {
        [
            fory_service::ORDER_SERVICE_PLACE_ORDER.path,
            fory_service::ORDER_SERVICE_SUBSCRIBE_DEPTH.path,
            fory_service::ORDER_SERVICE_UPLOAD_FILLS.path,
            fory_service::ORDER_SERVICE_ORDER_STREAM.path,
        ]
    }
    fn register(server: &mut SyncServer<TcpTransport>) -> Result<(), Status> {
        super::register_family!(
            Fory,
            fory_service,
            server,
            PlaceOrder,
            SubscribeDepth,
            UploadFills,
            OrderStream
        )
    }
}
