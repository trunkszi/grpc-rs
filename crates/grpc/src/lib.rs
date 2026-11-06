//! Synchronous gRPC channels, service dispatch and caller-thread streaming.
//!
//! Sessions own HTTP/2 protocol state; transports supply blocking readiness and
//! codecs encode individual messages. No executor or background thread is created.

pub mod channel;
pub mod server;
pub mod stream;

pub use channel::{CallOptions, ChannelConfig, ChannelStreamDriver, SyncChannel};
#[cfg(feature = "capnp")]
pub use grpc_codec_capnp::CapnpCodec;
#[cfg(feature = "fory")]
pub use grpc_codec_fory::ForyCodec;
#[cfg(feature = "protobuf")]
pub use grpc_codec_prost::ProstCodec;
/// Codec-independent request, response, session and transport contracts.
pub use grpc_core as core;
pub use grpc_core::{
    Codec, CompressionAlgorithm, Metadata, MethodDescriptor, Request, Response, RpcCardinality,
    SessionConfig, Status, Timeout,
};
/// Service-definition procedural macro namespace.
pub use grpc_macros as macros;
pub use server::{ServerStreamDriver, ServiceHandler, SyncServer};
pub use stream::{
    BidiStreamingCall, ClientStreamingCall, RequestStream, ResponseSink, ServerStreamingCall,
    StreamDriver, StreamError, server_streams,
};
