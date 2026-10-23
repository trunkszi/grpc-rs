//! Serialization contracts for individual gRPC message payloads.

/// Encodes and decodes message payloads independently of gRPC wire framing.
///
/// A codec may retain reusable serialization state. Implementations do not need
/// to be thread safe. Payloads exclude the five-byte gRPC frame prefix, and the
/// transport is responsible for compression and message size limits.
pub trait Codec {
    /// Message type accepted by the encoder.
    type EncodeItem: ?Sized;
    /// Owned message type returned by the decoder.
    type DecodeItem;
    /// Serialization or deserialization failure.
    type Error: std::error::Error;

    /// HTTP content type identifying this serialization format.
    const CONTENT_TYPE: &'static str;

    /// Appends one serialized message to `destination`.
    ///
    /// The caller may reuse the buffer between messages. An implementation may
    /// leave partially appended bytes on failure; callers must discard them.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined error when the message cannot be encoded.
    fn encode(
        &mut self,
        item: &Self::EncodeItem,
        destination: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Decodes exactly one message payload.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined error when the payload is invalid for
    /// this codec, including unsupported trailing bytes where applicable.
    fn decode(&mut self, payload: &[u8]) -> Result<Self::DecodeItem, Self::Error>;
}
