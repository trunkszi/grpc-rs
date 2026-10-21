//! Public facade for the synchronous gRPC framework.
//!
//! Core contracts and serialization backends are available. RPC execution and
//! service generation remain under construction.

pub mod channel;
pub mod server;
pub mod stream;

#[cfg(feature = "capnp")]
pub use grpc_codec_capnp::CapnpCodec;
#[cfg(feature = "fory")]
pub use grpc_codec_fory::ForyCodec;
#[cfg(feature = "protobuf")]
pub use grpc_codec_prost::ProstCodec;
/// Codec-independent request, response, and method contracts.
pub use grpc_core as core;
pub use grpc_core::{Codec, MethodDescriptor, Request, Response, RpcCardinality};
/// Service-definition procedural macro namespace.
pub use grpc_macros as macros;
