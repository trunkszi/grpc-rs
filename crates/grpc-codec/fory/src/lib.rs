//! Apache Fory payload serialization with a reusable, codec-owned context.
//!
//! Register all message types on a [`backend::Fory`] before passing it to
//! [`ForyCodec::with_fory`]. Registration IDs, schemas and Fory configuration
//! must agree with the peer. Fory freezes registration on the first operation.
//! gRPC framing, compression and transport message limits are handled separately.

use std::marker::PhantomData;

/// Serialization backend, including schema derives and registration APIs.
pub use fory as backend;
use fory::{Fory, Reader, Serializer};
use grpc_core::Codec;

/// Encodes with serializer `E` and decodes with serializer `D`.
///
/// For ordinary messages derived with [`backend::ForyStruct`], the message type
/// is also its serializer. Separate serializers can target external types via
/// [`backend::Serializer::Target`]. `D` defaults to `E` for symmetric messages.
/// The same Fory instance and its cached type metadata serve every call.
///
/// ```
/// use grpc_codec_fory::{
///     ForyCodec,
///     backend::{Fory, ForyStruct},
/// };
/// use grpc_core::Codec;
///
/// #[derive(Debug, PartialEq, ForyStruct)]
/// struct Message {
///     text: String,
/// }
/// let mut fory = Fory::builder().build();
/// fory.register::<Message>(100)?;
/// let mut codec = ForyCodec::<Message>::with_fory(fory);
/// let mut payload = Vec::new();
/// codec.encode(
///     &Message {
///         text: "hello".into(),
///     },
///     &mut payload,
/// )?;
/// assert_eq!(codec.decode(&payload)?.text, "hello");
/// # Ok::<(), grpc_codec_fory::backend::Error>(())
/// ```
pub struct ForyCodec<E, D = E> {
    fory: Fory,
    message_types: PhantomData<fn(E) -> D>,
}

impl<E, D> ForyCodec<E, D> {
    /// HTTP content type for Fory-encoded gRPC messages.
    pub const CONTENT_TYPE: &'static str = "application/grpc+fory";

    /// Creates a codec using Fory's default configuration and built-in types.
    ///
    /// Use [`Self::with_fory`] for registered structures or custom serializers.
    pub fn new() -> Self {
        Self::with_fory(Fory::builder().build())
    }

    /// Takes ownership of a configured Fory instance with completed registrations.
    pub fn with_fory(fory: Fory) -> Self {
        Self {
            fory,
            message_types: PhantomData,
        }
    }
}

impl<E, D> Default for ForyCodec<E, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Serializer, D: Serializer> Codec for ForyCodec<E, D> {
    type EncodeItem = E::Target;
    type DecodeItem = D::Target;
    type Error = backend::Error;

    const CONTENT_TYPE: &'static str = Self::CONTENT_TYPE;

    fn encode(
        &mut self,
        item: &Self::EncodeItem,
        destination: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        self.fory.serialize_to_with::<E>(destination, item)?;
        Ok(())
    }

    fn decode(&mut self, payload: &[u8]) -> Result<Self::DecodeItem, Self::Error> {
        let mut reader = Reader::new(payload);
        let message = self.fory.deserialize_from_with::<D>(&mut reader)?;
        if reader.get_cursor() != payload.len() {
            return Err(backend::Error::invalid_data(
                "trailing bytes after Fory gRPC message",
            ));
        }
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use backend::ForyStruct;

    use super::*;

    #[derive(Debug, PartialEq, ForyStruct)]
    struct Request {
        id: i64,
        text: String,
        values: Vec<i32>,
    }

    #[derive(Debug, PartialEq, ForyStruct)]
    struct Reply {
        accepted: bool,
        text: String,
    }

    #[derive(Debug, PartialEq, ForyStruct)]
    struct Envelope {
        request: Request,
        reply: Option<Reply>,
    }

    #[derive(Debug, PartialEq)]
    struct ExternalValue(i32);

    struct CheckedSerializer;

    impl Serializer for CheckedSerializer {
        type Target = ExternalValue;

        fn write_data(
            value: &ExternalValue,
            context: &mut backend::WriteContext,
        ) -> Result<(), backend::Error> {
            context.writer.write_i32(value.0);
            if value.0 < 0 {
                return Err(backend::Error::encode_error("negative value rejected"));
            }
            Ok(())
        }

        fn read_data(context: &mut backend::ReadContext) -> Result<ExternalValue, backend::Error> {
            Ok(ExternalValue(context.reader.read_i32()?))
        }

        fn default_value(_: &mut backend::ReadContext) -> Result<ExternalValue, backend::Error> {
            Ok(ExternalValue(0))
        }
    }

    fn configured() -> Fory {
        let mut fory = Fory::builder().build();
        fory.register::<Request>(100).unwrap();
        fory.register::<Reply>(101).unwrap();
        fory
    }

    fn request(id: i64, text: &str) -> Request {
        Request {
            id,
            text: text.into(),
            values: vec![i32::MIN, 0, i32::MAX],
        }
    }

    #[test]
    fn unit_scenario_roundtrip_structs_and_append_to_reused_buffer() {
        let mut codec = ForyCodec::<Request>::with_fory(configured());
        assert_eq!(
            <ForyCodec<Request> as Codec>::CONTENT_TYPE,
            "application/grpc+fory"
        );
        let mut buffer = Vec::with_capacity(4096);
        for message in [request(i64::MIN, ""), request(i64::MAX, "你好 🦀")] {
            buffer.clear();
            buffer.extend_from_slice(b"prefix");
            let start = buffer.len();
            codec.encode(&message, &mut buffer).unwrap();
            assert_eq!(&buffer[..start], b"prefix");
            assert_eq!(codec.decode(&buffer[start..]).unwrap(), message);
            assert_eq!(buffer.capacity(), 4096);
        }
    }

    #[test]
    fn unit_scenario_directional_request_response_matches_backend_wire_bytes() {
        let mut client = ForyCodec::<Request, Reply>::with_fory(configured());
        let mut server = ForyCodec::<Reply, Request>::with_fory(configured());
        let mut bytes = Vec::new();
        let message = request(42, "request");
        client.encode(&message, &mut bytes).unwrap();
        assert_eq!(bytes, configured().serialize(&message).unwrap());
        assert_eq!(server.decode(&bytes).unwrap(), message);
        let reply = Reply {
            accepted: true,
            text: "done".into(),
        };
        bytes.clear();
        server.encode(&reply, &mut bytes).unwrap();
        assert_eq!(client.decode(&bytes).unwrap(), reply);
    }

    #[test]
    fn unit_scenario_rejects_truncation_and_recovers_same_context() {
        let mut codec = ForyCodec::<Request>::with_fory(configured());
        let message = request(7, "valid after failure");
        let mut bytes = Vec::new();
        codec.encode(&message, &mut bytes).unwrap();
        for end in 0..bytes.len() {
            assert!(codec.decode(&bytes[..end]).is_err(), "prefix length {end}");
            assert_eq!(codec.decode(&bytes).unwrap(), message);
        }
        assert!(codec.decode(&[0xff; 32]).is_err());
        assert_eq!(codec.decode(&bytes).unwrap(), message);
    }

    #[test]
    fn unit_scenario_rejects_garbage_and_concatenated_messages() {
        let mut codec = ForyCodec::<Request>::with_fory(configured());
        let message = request(8, "one payload only");
        let mut bytes = Vec::new();
        codec.encode(&message, &mut bytes).unwrap();
        let mut concatenated = bytes.clone();
        concatenated.extend_from_slice(&bytes);
        assert!(
            codec
                .decode(&concatenated)
                .unwrap_err()
                .to_string()
                .contains("trailing bytes")
        );
        let mut garbage = bytes.clone();
        garbage.push(0);
        assert!(codec.decode(&garbage).is_err());
        assert_eq!(codec.decode(&bytes).unwrap(), message);
    }

    #[test]
    fn unit_scenario_registration_is_preserved_across_calls_and_owner_move() {
        let mut codec = ForyCodec::<Request>::with_fory(configured());
        let mut bytes = Vec::new();
        for id in 0..32 {
            let message = request(id, "cached schema");
            bytes.clear();
            codec.encode(&message, &mut bytes).unwrap();
            assert_eq!(codec.decode(&bytes).unwrap(), message);
        }
        // Registration is frozen by the existing instance's first operation.
        assert!(codec.fory.register::<Request>(100).is_err());
        let mut moved = Box::new(codec);
        assert_eq!(moved.decode(&bytes).unwrap(), request(31, "cached schema"));
    }

    #[test]
    fn unit_scenario_missing_registration_is_reported_without_poisoning_builtins() {
        let mut codec = ForyCodec::<Request, i32>::new();
        let mut bytes = Vec::new();
        assert!(
            codec
                .encode(&request(1, "unregistered"), &mut bytes)
                .is_err()
        );
        // The caller discards partial bytes after an encoding failure.
        let valid = Fory::builder().build().serialize(&17_i32).unwrap();
        assert_eq!(codec.decode(&valid).unwrap(), 17);
    }

    #[test]
    fn unit_scenario_custom_registered_serializer_recovers_after_partial_write_error() {
        let mut fory = Fory::builder().build();
        fory.register_serializer::<CheckedSerializer>(120).unwrap();
        let mut codec = ForyCodec::<CheckedSerializer>::with_fory(fory);
        let mut bytes = Vec::new();
        for value in [0, 17, i32::MAX] {
            assert!(codec.encode(&ExternalValue(-1), &mut bytes).is_err());
            bytes.clear();
            codec.encode(&ExternalValue(value), &mut bytes).unwrap();
            assert_eq!(codec.decode(&bytes).unwrap(), ExternalValue(value));
            bytes.clear();
        }
    }

    #[test]
    fn unit_scenario_nested_optional_messages_and_wrong_type_recovery() {
        let mut fory = configured();
        fory.register::<Envelope>(102).unwrap();
        let mut codec = ForyCodec::<Envelope>::with_fory(fory);
        let wrong_type = configured().serialize(&request(0, "wrong root")).unwrap();
        let mut bytes = Vec::new();
        for reply in [
            None,
            Some(Reply {
                accepted: false,
                text: "嵌套".into(),
            }),
        ] {
            assert!(codec.decode(&wrong_type).is_err());
            let envelope = Envelope {
                request: request(-1, "nested"),
                reply,
            };
            bytes.clear();
            codec.encode(&envelope, &mut bytes).unwrap();
            assert_eq!(codec.decode(&bytes).unwrap(), envelope);
        }
    }

    #[test]
    fn unit_scenario_independent_codecs_retain_their_configuration() {
        let mut native = Fory::builder().xlang(false).build();
        native.register::<Request>(100).unwrap();
        let mut native_codec = ForyCodec::<Request>::with_fory(native);
        let mut default_codec = ForyCodec::<String>::default();
        let mut native_bytes = Vec::new();
        let mut default_bytes = Vec::new();
        for id in 0..8 {
            let message = request(id, "local mode");
            native_bytes.clear();
            default_bytes.clear();
            native_codec.encode(&message, &mut native_bytes).unwrap();
            default_codec
                .encode(&"built-in".into(), &mut default_bytes)
                .unwrap();
            assert_eq!(native_codec.decode(&native_bytes).unwrap(), message);
            assert_eq!(default_codec.decode(&default_bytes).unwrap(), "built-in");
        }
    }
}
