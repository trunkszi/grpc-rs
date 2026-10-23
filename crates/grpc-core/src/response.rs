//! Response messages and their initial application metadata.

use crate::Metadata;

/// A response message with initial application metadata.
///
/// This envelope does not represent terminal gRPC status or trailing metadata.
/// Those belong to the RPC stream lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Response<T> {
    metadata: Metadata,
    message: T,
}

impl<T> Response<T> {
    /// Wraps a message with empty initial metadata.
    #[must_use]
    pub fn new(message: T) -> Self {
        Self::from_parts(Metadata::new(), message)
    }

    /// Combines initial application metadata and a message.
    #[must_use]
    pub const fn from_parts(metadata: Metadata, message: T) -> Self {
        Self { metadata, message }
    }

    /// Borrows the initial application metadata.
    #[must_use]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Mutably borrows the initial application metadata.
    pub const fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Borrows the response message.
    #[must_use]
    pub const fn get_ref(&self) -> &T {
        &self.message
    }

    /// Mutably borrows the response message.
    pub const fn get_mut(&mut self) -> &mut T {
        &mut self.message
    }

    /// Consumes the envelope, returning initial metadata and the message.
    #[must_use]
    pub fn into_parts(self) -> (Metadata, T) {
        (self.metadata, self.message)
    }

    /// Transforms the message while preserving initial application metadata.
    #[must_use]
    pub fn map<U>(self, transform: impl FnOnce(T) -> U) -> Response<U> {
        Response::from_parts(self.metadata, transform(self.message))
    }
}
