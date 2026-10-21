//! CapnpCodec integration scaffold for the synchronous gRPC framework.
//!
//! The serialization backend is available for schema types. The codec adapter
//! does not yet implement the core `Codec` contract.

/// Serialization backend used by this adapter.
pub use capnp as backend;

/// Marker reserving the CapnpCodec adapter API.
#[derive(Clone, Copy, Debug, Default)]
pub struct CapnpCodec;

impl CapnpCodec {
    /// HTTP content type for messages encoded with this format.
    pub const CONTENT_TYPE: &'static str = "application/grpc+capnp";
}
