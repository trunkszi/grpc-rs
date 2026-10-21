//! Synchronous, codec-independent gRPC contracts.
//!
//! Message envelopes and codec interfaces are available independently of a transport.
//! Wire framing, sessions, compression, and transport modules reserve their public
//! namespaces; they do not yet implement an RPC engine.

pub mod codec;
pub mod compression;
pub mod framing;
pub mod metadata;
pub mod method;
pub mod request;
pub mod response;
pub mod session;
pub mod slot_map;
pub mod status;
pub mod transport;

pub use codec::Codec;
pub use metadata::{Metadata, MetadataError, MetadataValue};
pub use method::{MethodDescriptor, RpcCardinality};
pub use request::Request;
pub use response::Response;
