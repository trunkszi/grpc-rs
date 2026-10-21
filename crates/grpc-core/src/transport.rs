//! Reserved for synchronous memory, socket, and TLS transports.
//! No transport implementation is provided yet.

/// Synchronous HTTP/2 backend used by transport and session implementations.
pub use nghttp2 as http2;
