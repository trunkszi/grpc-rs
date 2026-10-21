//! Apache Fory payload serialization with reusable backend contexts.
//!
//! Register all message types on a [`backend::Fory`] before passing it to
//! [`ForyCodec::with_fory`]. Registration IDs, schemas and Fory configuration
//! must agree with the peer. Fory freezes registration on the first operation.
//! gRPC framing, compression and transport message limits are handled separately.

use std::{cell::Cell, marker::PhantomData, sync::OnceLock};

/// Serialization backend, including schema derives and registration APIs.
pub use fory as backend;
use fory::{Fory, Reader, Serializer};
use grpc_core::Codec;

static DEFAULT_FORY: OnceLock<Fory> = OnceLock::new();

thread_local! {
    static OPERATION_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static DEFAULT_POISONED: Cell<bool> = const { Cell::new(false) };
}

enum Backend {
    SharedDefault,
    Configured(Box<Fory>),
}

impl Backend {
    fn instance(&self) -> &Fory {
        match self {
            Self::SharedDefault => DEFAULT_FORY.get_or_init(|| Fory::builder().build()),
            Self::Configured(fory) => fory,
        }
    }
}

struct OperationGuard {
    shared_default: bool,
    completed: bool,
}

impl OperationGuard {
    fn enter(shared_default: bool) -> Result<Self, backend::Error> {
        if shared_default && DEFAULT_POISONED.get() {
            return Err(backend::Error::invalid_data(
                "default Fory context is poisoned by a serializer panic on this thread",
            ));
        }
        if OPERATION_ACTIVE.replace(true) {
            return Err(backend::Error::invalid_data(
                "recursive Fory codec operation is unsupported",
            ));
        }
        Ok(Self {
            shared_default,
            completed: false,
        })
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if self.shared_default && !self.completed {
            DEFAULT_POISONED.set(true);
        }
        OPERATION_ACTIVE.set(false);
    }
}

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
    fory: Backend,
    message_types: PhantomData<fn(E) -> D>,
}

impl<E, D> ForyCodec<E, D> {
    /// HTTP content type for Fory-encoded gRPC messages.
    pub const CONTENT_TYPE: &'static str = "application/grpc+fory";

    /// Creates a codec using Fory's default configuration and built-in types.
    ///
    /// All default codecs share one immutable backend, retaining at most one
    /// read and write context per thread across repeated creation and dropping.
    /// A serializer panic poisons default operations on that thread permanently;
    /// subsequent operations return an error without reentering dirty contexts.
    /// Use [`Self::with_fory`] for registered structures or custom serializers.
    pub fn new() -> Self {
        Self {
            fory: Backend::SharedDefault,
            message_types: PhantomData,
        }
    }

    /// Takes ownership of a configured Fory instance with completed registrations.
    ///
    /// For untrusted RPC input, keep Fory's string validation and resource limits
    /// enabled. Options disabling validation require trusted payloads, as documented
    /// by the backend. Custom serializers must not recursively invoke backend
    /// serialization or deserialization while using a supplied context. Discard
    /// the codec if a serializer panics; backend context recovery applies to
    /// errors returned as `Result`, not unwinding. The backend retains configured
    /// instances' contexts until thread exit, so reuse configured codecs rather
    /// than repeatedly constructing them on a long-lived thread.
    pub fn with_fory(fory: Fory) -> Self {
        Self {
            fory: Backend::Configured(Box::new(fory)),
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
        // The backend's borrowed-buffer writer is not detached if a custom
        // serializer panics. Its owned writer keeps cleanup independent of the
        // caller's Vec; only successfully serialized payloads are appended.
        let mut guard = OperationGuard::enter(matches!(self.fory, Backend::SharedDefault))?;
        let result = self.fory.instance().serialize_with::<E>(item);
        guard.completed = true;
        let payload = result?;
        destination.extend_from_slice(&payload);
        Ok(())
    }

    fn decode(&mut self, payload: &[u8]) -> Result<Self::DecodeItem, Self::Error> {
        let mut reader = Reader::new(payload);
        let mut guard = OperationGuard::enter(matches!(self.fory, Backend::SharedDefault))?;
        let result = self.fory.instance().deserialize_from_with::<D>(&mut reader);
        guard.completed = true;
        let message = result?;
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

    struct DefaultProbeSerializer;

    impl Serializer for DefaultProbeSerializer {
        type Target = i32;

        fn write_data(
            value: &i32,
            context: &mut backend::WriteContext,
        ) -> Result<(), backend::Error> {
            i32::write_data(value, context)?;
            assert!(*value >= 0, "default writer panic");
            if *value == 42 {
                let mut nested = ForyCodec::<i32>::new();
                let error = nested.encode(&1, &mut Vec::new()).unwrap_err();
                assert!(error.to_string().contains("recursive"));
            }
            Ok(())
        }

        fn read_data(context: &mut backend::ReadContext) -> Result<i32, backend::Error> {
            let value = i32::read_data(context)?;
            assert!(value >= 0, "default reader panic");
            Ok(value)
        }

        fn write_type_info(context: &mut backend::WriteContext) -> Result<(), backend::Error> {
            i32::write_type_info(context)
        }

        fn read_type_info(context: &mut backend::ReadContext) -> Result<(), backend::Error> {
            i32::read_type_info(context)
        }
    }

    #[test]
    fn unit_scenario_default_backend_reused_across_types_lifetimes_and_threads() {
        fn require_send_sync<T: Send + Sync>() {}
        require_send_sync::<ForyCodec<String>>();
        let anchor = ForyCodec::<i32>::new();
        for _ in 0..256 {
            let mut codec = ForyCodec::<String>::new();
            assert!(std::ptr::eq(anchor.fory.instance(), codec.fory.instance()));
            let mut bytes = Vec::new();
            codec
                .encode(&"reused backend".to_owned(), &mut bytes)
                .unwrap();
            assert!(codec.decode(&[]).is_err());
            assert_eq!(codec.decode(&bytes).unwrap(), "reused backend");
        }
        std::thread::spawn(move || {
            let mut moved = anchor;
            let local = ForyCodec::<String>::new();
            assert!(std::ptr::eq(moved.fory.instance(), local.fory.instance()));
            let mut bytes = Vec::new();
            moved.encode(&17, &mut bytes).unwrap();
            assert_eq!(moved.decode(&bytes).unwrap(), 17);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn unit_scenario_recursive_default_codec_rejected_without_poisoning_context() {
        let mut codec = ForyCodec::<DefaultProbeSerializer>::new();
        let mut bytes = Vec::new();
        codec.encode(&42, &mut bytes).unwrap();
        assert_eq!(codec.decode(&bytes).unwrap(), 42);
        bytes.clear();
        codec.encode(&17, &mut bytes).unwrap();
        assert_eq!(codec.decode(&bytes).unwrap(), 17);
    }

    #[test]
    fn unit_scenario_default_serializer_panic_poison_is_thread_local_and_permanent() {
        for decoding in [false, true] {
            std::thread::spawn(move || {
                let mut ordinary = ForyCodec::<i32>::new();
                let mut negative = Vec::new();
                ordinary.encode(&-1, &mut negative).unwrap();
                let mut codec = ForyCodec::<DefaultProbeSerializer>::new();
                let mut prefix = b"owned prefix".to_vec();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if decoding {
                        codec.decode(&negative).unwrap();
                    } else {
                        codec.encode(&-1, &mut prefix).unwrap();
                    }
                }));
                assert!(result.is_err());
                assert_eq!(prefix, b"owned prefix");
                drop(codec);
                let mut replacement = ForyCodec::<i32>::new();
                assert!(
                    replacement
                        .encode(&1, &mut prefix)
                        .unwrap_err()
                        .to_string()
                        .contains("poisoned")
                );
                assert!(
                    ordinary
                        .decode(&negative)
                        .unwrap_err()
                        .to_string()
                        .contains("poisoned")
                );
                std::thread::spawn(|| {
                    let mut fresh = ForyCodec::<i32>::new();
                    let mut bytes = Vec::new();
                    fresh.encode(&7, &mut bytes).unwrap();
                    assert_eq!(fresh.decode(&bytes).unwrap(), 7);
                })
                .join()
                .unwrap();
            })
            .join()
            .unwrap();
        }
    }

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
        let Backend::Configured(fory) = &mut codec.fory else {
            panic!("expected configured backend");
        };
        assert!(fory.register::<Request>(100).is_err());
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

    #[test]
    fn integration_scenario_noncanonical_root_headers_do_not_poison_context() {
        let mut codec = ForyCodec::<String>::new();
        let message = "valid after malformed root".to_owned();
        let mut valid = Vec::new();
        codec.encode(&message, &mut valid).unwrap();
        assert_eq!(codec.decode(&valid).unwrap(), message);
        for header in u8::MIN..=u8::MAX {
            if header == valid[0] {
                continue;
            }
            let mut invalid = valid.clone();
            invalid[0] = header;
            assert!(codec.decode(&invalid).is_err(), "root header {header:#04x}");
            assert_eq!(codec.decode(&valid).unwrap(), message);
        }
    }

    #[test]
    fn integration_scenario_cross_language_mode_mismatch_recovers_both_codecs() {
        let mut native = ForyCodec::<String>::with_fory(Fory::builder().xlang(false).build());
        let mut cross_language =
            ForyCodec::<String>::with_fory(Fory::builder().xlang(true).build());
        let message = "same type, different wire configuration".to_owned();
        let mut native_bytes = Vec::new();
        let mut cross_language_bytes = Vec::new();
        native.encode(&message, &mut native_bytes).unwrap();
        cross_language
            .encode(&message, &mut cross_language_bytes)
            .unwrap();
        assert_eq!(native.decode(&native_bytes).unwrap(), message);
        assert_eq!(
            cross_language.decode(&cross_language_bytes).unwrap(),
            message
        );
        assert!(native.decode(&cross_language_bytes).is_err());
        assert!(cross_language.decode(&native_bytes).is_err());
        assert_eq!(native.decode(&native_bytes).unwrap(), message);
        assert_eq!(
            cross_language.decode(&cross_language_bytes).unwrap(),
            message
        );
    }

    #[test]
    fn integration_scenario_partial_custom_write_preserves_preceding_payload() {
        let mut fory = Fory::builder().build();
        fory.register_serializer::<CheckedSerializer>(120).unwrap();
        let mut codec = ForyCodec::<CheckedSerializer>::with_fory(fory);
        let mut bytes = b"prefix".to_vec();
        let first_start = bytes.len();
        codec.encode(&ExternalValue(17), &mut bytes).unwrap();
        let first_end = bytes.len();
        let preceding = bytes.clone();
        assert!(codec.encode(&ExternalValue(-1), &mut bytes).is_err());
        assert_eq!(bytes, preceding);
        codec.encode(&ExternalValue(42), &mut bytes).unwrap();
        assert_eq!(&bytes[..first_start], b"prefix");
        assert_eq!(
            codec.decode(&bytes[first_start..first_end]).unwrap(),
            ExternalValue(17)
        );
        assert_eq!(
            codec.decode(&bytes[first_end..]).unwrap(),
            ExternalValue(42)
        );
    }

    struct ReadCheckedSerializer;

    impl Serializer for ReadCheckedSerializer {
        type Target = ExternalValue;

        fn write_data(
            value: &ExternalValue,
            context: &mut backend::WriteContext,
        ) -> Result<(), backend::Error> {
            context.writer.write_i32(value.0);
            Ok(())
        }

        fn read_data(context: &mut backend::ReadContext) -> Result<ExternalValue, backend::Error> {
            let value = context.reader.read_i32()?;
            if value < 0 {
                return Err(backend::Error::invalid_data("negative body rejected"));
            }
            Ok(ExternalValue(value))
        }
    }

    #[test]
    fn integration_scenario_custom_body_and_trailing_errors_allow_valid_error_valid() {
        let mut fory = Fory::builder().build();
        fory.register_serializer::<ReadCheckedSerializer>(121)
            .unwrap();
        let mut codec = ForyCodec::<ReadCheckedSerializer>::with_fory(fory);
        let mut invalid_body = Vec::new();
        codec.encode(&ExternalValue(-1), &mut invalid_body).unwrap();
        let mut valid = Vec::new();
        for value in [0, 17, i32::MAX] {
            valid.clear();
            codec.encode(&ExternalValue(value), &mut valid).unwrap();
            assert_eq!(codec.decode(&valid).unwrap(), ExternalValue(value));
            let error = codec.decode(&invalid_body).unwrap_err();
            assert!(error.to_string().contains("negative body rejected"));
            assert_eq!(codec.decode(&valid).unwrap(), ExternalValue(value));
            let mut trailing = valid.clone();
            trailing.extend_from_slice(&invalid_body);
            let error = codec.decode(&trailing).unwrap_err();
            assert!(error.to_string().contains("trailing bytes"));
            assert_eq!(codec.decode(&valid).unwrap(), ExternalValue(value));
        }
    }

    struct PanickingSerializer;

    impl Serializer for PanickingSerializer {
        type Target = ExternalValue;

        fn write_data(
            value: &ExternalValue,
            context: &mut backend::WriteContext,
        ) -> Result<(), backend::Error> {
            context.writer.write_i32(value.0);
            panic!("custom serializer panicked after writing its body");
        }

        fn read_data(context: &mut backend::ReadContext) -> Result<ExternalValue, backend::Error> {
            Ok(ExternalValue(context.reader.read_i32()?))
        }
    }

    #[test]
    fn integration_scenario_custom_writer_panic_keeps_caller_buffer_owned() {
        std::thread::spawn(|| {
            let mut fory = Fory::builder().build();
            fory.register_serializer::<PanickingSerializer>(122)
                .unwrap();
            let mut codec = ForyCodec::<PanickingSerializer>::with_fory(fory);
            let mut bytes = b"caller-owned prefix".to_vec();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                codec.encode(&ExternalValue(17), &mut bytes).unwrap();
            }));
            assert!(result.is_err());
            assert_eq!(bytes, b"caller-owned prefix");
            // The caller buffer is gone before thread-local backend contexts are dropped.
            drop(bytes);
        })
        .join()
        .unwrap();
    }
}
