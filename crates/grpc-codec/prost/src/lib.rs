//! Protocol Buffers serialization for gRPC message payloads using Prost.
//!
//! Messages are appended directly to the caller's buffer, without a staging
//! buffer, a Protobuf length delimiter, or a gRPC frame header. Decoding follows
//! Protobuf rules, including acceptance of well-formed unknown fields.

use std::marker::PhantomData;

use grpc_core::codec::Codec;
/// Serialization backend used by this adapter.
pub use prost as backend;
use prost::Message;

/// Encodes `E` and decodes `D` as individual Protobuf payloads.
///
/// The directions may use different message types, as is usual for RPC requests
/// and responses. The adapter holds no message data between calls; callers may
/// reuse both the codec and the outbound buffer across messages.
#[derive(Debug)]
pub struct ProstCodec<E, D> {
    message_types: PhantomData<fn(E) -> D>,
}

impl<E, D> ProstCodec<E, D> {
    /// HTTP content type emitted for Protobuf messages.
    pub const CONTENT_TYPE: &'static str = "application/grpc";

    /// Creates a codec for the specified outbound and inbound message types.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            message_types: PhantomData,
        }
    }

    /// Accepts the default gRPC media type and its explicit Protobuf variant.
    ///
    /// Media types are compared case-insensitively, ignoring surrounding HTTP
    /// whitespace and optional media type parameters.
    #[must_use]
    pub fn accepts_content_type(content_type: &str) -> bool {
        let media_type = content_type
            .split_once(';')
            .map_or(content_type, |(media_type, _)| media_type)
            .trim_matches([' ', '\t']);
        media_type.eq_ignore_ascii_case(Self::CONTENT_TYPE)
            || media_type.eq_ignore_ascii_case("application/grpc+proto")
    }
}

impl<E, D> Default for ProstCodec<E, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Message, D: Message + Default> Codec for ProstCodec<E, D> {
    type EncodeItem = E;
    type DecodeItem = D;
    type Error = ProstCodecError;

    const CONTENT_TYPE: &'static str = Self::CONTENT_TYPE;

    fn encode(&mut self, item: &E, destination: &mut Vec<u8>) -> Result<(), Self::Error> {
        item.encode(destination).map_err(ProstCodecError::Encode)
    }

    fn decode(&mut self, payload: &[u8]) -> Result<D, Self::Error> {
        D::decode(payload).map_err(ProstCodecError::Decode)
    }
}

/// A Protobuf serialization failure, preserving the backend error as its source.
#[derive(Debug, thiserror::Error)]
pub enum ProstCodecError {
    /// The outbound message could not be encoded into the destination.
    #[error("Protobuf encoding failed: {0}")]
    Encode(#[from] prost::EncodeError),
    /// The inbound payload is not a valid message of the requested type.
    #[error("Protobuf decoding failed: {0}")]
    Decode(#[from] prost::DecodeError),
}

#[cfg(test)]
mod unit {
    use std::error::Error;

    use super::*;

    #[test]
    fn scenario_encode_appends_wire_payload_to_reusable_buffer() {
        let mut codec = ProstCodec::<String, String>::new();
        let mut destination = Vec::with_capacity(64);
        destination.extend_from_slice(&[0xaa, 0xbb]);
        let pointer = destination.as_ptr();
        let capacity = destination.capacity();

        codec
            .encode(&String::from("hello"), &mut destination)
            .unwrap();

        assert_eq!(destination, b"\xaa\xbb\x0a\x05hello");
        assert_eq!(destination.as_ptr(), pointer);
        assert_eq!(destination.capacity(), capacity);
        assert_eq!(codec.decode(&destination[2..]).unwrap(), "hello");

        destination.clear();
        codec
            .encode(&String::from("next"), &mut destination)
            .unwrap();
        assert_eq!(destination, b"\x0a\x04next");
        assert_eq!(destination.as_ptr(), pointer);
        assert_eq!(codec.decode(&destination).unwrap(), "next");
    }

    #[test]
    fn scenario_encode_grows_destination_without_capacity_requirement() {
        let mut codec = ProstCodec::<Vec<u8>, Vec<u8>>::new();
        let value = vec![0xff; 512];
        let mut destination = Vec::new();
        codec.encode(&value, &mut destination).unwrap();
        assert_eq!(destination, value.encode_to_vec());
        assert_eq!(codec.decode(&destination).unwrap(), value);
    }

    #[test]
    fn scenario_request_and_response_types_are_independent() {
        let mut codec = ProstCodec::<String, u32>::default();
        let mut destination = Vec::new();
        codec
            .encode(&String::from("request"), &mut destination)
            .unwrap();
        assert_eq!(destination, b"\x0a\x07request");
        assert_eq!(codec.decode(&[0x08, 0x96, 0x01]).unwrap(), 150);
    }

    #[test]
    fn scenario_empty_message_has_empty_payload() {
        let mut codec = ProstCodec::<(), ()>::new();
        let mut destination = vec![0xaa];
        codec.encode(&(), &mut destination).unwrap();
        assert_eq!(destination, [0xaa]);
        codec.decode(&[]).unwrap();
    }

    #[test]
    fn scenario_decode_preserves_protobuf_unknown_field_semantics() {
        let mut codec = ProstCodec::<String, String>::new();
        assert_eq!(codec.decode(b"\x0a\x02ok\x10\x01").unwrap(), "ok");
        assert_eq!(codec.decode(b"\x10\x01").unwrap(), "");
    }

    #[test]
    fn scenario_unknown_wire_types_do_not_hide_later_known_fields() {
        let mut codec = ProstCodec::<String, String>::new();
        let payload = b"\x0a\x03old\x11abcdefgh\x1dijkl\x22\x03xyz\x28\x96\x01\x0a\x03new";
        assert_eq!(codec.decode(payload).unwrap(), "new");

        for truncated_unknown in [
            &b"\x11abcdefg"[..],
            &b"\x1dabc"[..],
            &b"\x22\x04xyz"[..],
            &b"\x28\x80"[..],
        ] {
            let mut payload = b"\x0a\x02ok".to_vec();
            payload.extend_from_slice(truncated_unknown);
            assert!(codec.decode(&payload).is_err());
        }
    }

    #[test]
    fn scenario_decode_rejects_malformed_payloads_and_trailing_bytes() {
        let mut codec = ProstCodec::<String, String>::new();
        for payload in [
            &b"\x0a\x05short\x00"[..],
            &b"\x0a\x05bad"[..],
            &b"\x0a\x01\xff"[..],
            &b"\x08\x01"[..],
            &b"\x80"[..],
        ] {
            let error = codec.decode(payload).unwrap_err();
            assert!(matches!(error, ProstCodecError::Decode(_)));
            assert!(error.source().unwrap().is::<prost::DecodeError>());
        }
    }

    #[test]
    fn scenario_decode_failure_does_not_contaminate_later_messages() {
        let mut codec = ProstCodec::<String, String>::new();
        assert!(codec.decode(b"\x0a\x05bad").is_err());
        assert_eq!(codec.decode(b"\x0a\x02ok").unwrap(), "ok");
        assert_eq!(codec.decode(&[]).unwrap(), "");
    }

    #[test]
    fn scenario_encode_errors_preserve_backend_details() {
        let mut empty: &mut [u8] = &mut [];
        let backend_error = String::from("value").encode(&mut empty).unwrap_err();
        let error = ProstCodecError::from(backend_error);
        let ProstCodecError::Encode(inner) = &error else {
            panic!("expected an encoding error");
        };
        assert_eq!(inner.required_capacity(), 7);
        assert_eq!(inner.remaining(), 0);
        assert!(error.source().unwrap().is::<prost::EncodeError>());
        assert!(error.to_string().contains(&backend_error.to_string()));
    }

    #[test]
    fn scenario_decode_errors_preserve_backend_details() {
        let backend_error = String::decode(&b"\x00"[..]).unwrap_err();
        let description = backend_error.to_string();
        let error = ProstCodecError::from(backend_error);
        assert!(matches!(error, ProstCodecError::Decode(_)));
        assert!(error.source().unwrap().is::<prost::DecodeError>());
        assert!(error.to_string().contains(&description));
    }

    #[test]
    fn scenario_content_type_accepts_only_protobuf_media_types() {
        for value in [
            "application/grpc",
            "application/grpc+proto",
            "Application/GRPC+Proto",
            " application/grpc\t; charset=utf-8",
        ] {
            assert!(ProstCodec::<String, String>::accepts_content_type(value));
        }
        for value in [
            "",
            "application/grpc+capnp",
            "application/grpc-web+proto",
            "application/grpc+proto-extra",
            "application/grpcsuffix",
            "application/grpc, application/json",
            "application/grpc\r\n",
        ] {
            assert!(!ProstCodec::<String, String>::accepts_content_type(value));
        }
    }
}
