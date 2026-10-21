//! Synchronous, codec-independent gRPC contracts.
//!
//! Message envelopes and codec interfaces are available independently of a transport.
//! Aligned message framing, bounded compression, statuses and metadata support
//! transport-independent RPC message handling.

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
pub mod tls;
pub mod transport;

pub use codec::Codec;
pub use compression::{CompressionAlgorithm, CompressionConfig};
pub use framing::{AlignedMessageBuf, LengthPrefixedFrame};
pub use metadata::{
    Metadata, MetadataError, MetadataValue, format_grpc_timeout, parse_grpc_timeout,
};
pub use method::{MethodDescriptor, RpcCardinality};
pub use request::Request;
pub use response::Response;
pub use status::{Code, Status};
