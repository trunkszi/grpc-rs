//! ForyCodec integration scaffold for the synchronous gRPC framework.
//!
//! The serialization backend is available for schema types. The codec adapter
//! does not yet implement the core `Codec` contract.

/// Serialization backend used by this adapter.
pub use fory as backend;

/// Marker reserving the ForyCodec adapter API.
#[derive(Clone, Copy, Debug, Default)]
pub struct ForyCodec;

impl ForyCodec {
    /// HTTP content type for messages encoded with this format.
    pub const CONTENT_TYPE: &'static str = "application/grpc+fory";
}
