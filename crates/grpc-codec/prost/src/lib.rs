//! ProstCodec integration scaffold for the synchronous gRPC framework.
//!
//! The serialization backend is available for schema types. The codec adapter
//! does not yet implement the core `Codec` contract.

/// Serialization backend used by this adapter.
pub use prost as backend;

/// Marker reserving the ProstCodec adapter API.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProstCodec;

impl ProstCodec {
    /// HTTP content type for messages encoded with this format.
    pub const CONTENT_TYPE: &'static str = "application/grpc";
}
