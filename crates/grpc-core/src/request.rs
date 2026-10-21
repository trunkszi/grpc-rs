//! Request messages and their application metadata.

use crate::Metadata;

/// An outbound or decoded request with application metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request<T> {
    metadata: Metadata,
    message: T,
}

impl<T> Request<T> {
    /// Wraps a message with empty metadata.
    #[must_use]
    pub fn new(message: T) -> Self {
        Self::from_parts(Metadata::new(), message)
    }

    /// Combines application metadata and a message.
    #[must_use]
    pub const fn from_parts(metadata: Metadata, message: T) -> Self {
        Self { metadata, message }
    }

    /// Borrows the application metadata.
    #[must_use]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Mutably borrows the application metadata.
    pub const fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Borrows the message.
    #[must_use]
    pub const fn get_ref(&self) -> &T {
        &self.message
    }

    /// Mutably borrows the message.
    pub const fn get_mut(&mut self) -> &mut T {
        &mut self.message
    }

    /// Consumes the envelope, returning application metadata and the message.
    #[must_use]
    pub fn into_parts(self) -> (Metadata, T) {
        (self.metadata, self.message)
    }

    /// Transforms the message while preserving application metadata.
    #[must_use]
    pub fn map<U>(self, transform: impl FnOnce(T) -> U) -> Request<U> {
        Request::from_parts(self.metadata, transform(self.message))
    }
}
