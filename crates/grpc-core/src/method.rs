//! Static RPC routing and cardinality descriptions.

/// Message multiplicity in each direction of an RPC.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RpcCardinality {
    /// One request and one response.
    Unary,
    /// One request and a stream of responses.
    ServerStreaming,
    /// A stream of requests and one response.
    ClientStreaming,
    /// Independent request and response streams.
    BidiStreaming,
}

/// Static routing information emitted by service definitions.
///
/// `path` is the canonical, case-sensitive HTTP/2 path in the form
/// `/package.Service/Method`. The service definition owns its validity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MethodDescriptor {
    /// Canonical HTTP/2 request path.
    pub path: &'static str,
    /// Request and response message multiplicity.
    pub cardinality: RpcCardinality,
}

impl MethodDescriptor {
    /// Describes a method using its canonical path and message multiplicity.
    #[must_use]
    pub const fn new(path: &'static str, cardinality: RpcCardinality) -> Self {
        Self { path, cardinality }
    }
}
