//! Typed Cap'n Proto payloads for gRPC.
//!
//! [`Codec::decode`] copies segments into owned, aligned storage. For payloads
//! held in an 8-byte aligned transport buffer, [`CapnpCodec::read_aligned`]
//! borrows segments directly. Readers validate nested pointers lazily as callers
//! access fields, subject to the configured traversal and nesting limits.

use std::marker::PhantomData;

/// Serialization backend used by this adapter.
pub use capnp as backend;
use capnp::{
    message::{ReaderOptions, TypedBuilder, TypedReader},
    serialize::{NoAllocSliceSegments, OwnedSegments},
    traits::Owned,
};
use grpc_core::codec::Codec;

/// An owned, typed message returned by [`Codec::decode`].
pub type OwnedMessage<T> = TypedReader<OwnedSegments, T>;

/// A typed reader whose segments borrow the supplied aligned payload.
pub type BorrowedMessage<'a, T> = TypedReader<NoAllocSliceSegments<'a>, T>;

/// Encodes schema `E` and decodes schema `D` using unpacked Cap'n Proto framing.
///
/// Encoding appends directly to the caller's buffer. Owned decoding allocates
/// segment storage; aligned reading borrows the payload without copying it.
/// Message construction, transport storage and errors have independent costs.
#[derive(Debug)]
pub struct CapnpCodec<E, D> {
    options: ReaderOptions,
    schema: PhantomData<fn(E) -> D>,
}

impl<E, D> CapnpCodec<E, D> {
    /// HTTP content type for messages encoded with this format.
    pub const CONTENT_TYPE: &'static str = "application/grpc+capnp";

    /// Uses Cap'n Proto's finite default traversal and nesting limits.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            options: ReaderOptions::new(),
            schema: PhantomData,
        }
    }

    /// Sets finite limits for both owned decoding and aligned reading.
    ///
    /// The word limit also bounds the total declared segment size before owned
    /// decoding allocates storage. Traversal counts repeated field accesses.
    ///
    /// # Errors
    ///
    /// Returns an error if either limit is zero or the nesting limit is negative.
    pub fn with_limits(traversal_limit_in_words: usize, nesting_limit: i32) -> capnp::Result<Self> {
        if traversal_limit_in_words == 0 || nesting_limit <= 0 {
            return Err(capnp::Error::failed(
                "reader limits must be positive".into(),
            ));
        }
        Ok(Self {
            options: ReaderOptions {
                traversal_limit_in_words: Some(traversal_limit_in_words),
                nesting_limit,
            },
            schema: PhantomData,
        })
    }
}

impl<E, D> Default for CapnpCodec<E, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, D: Owned> CapnpCodec<E, D> {
    /// Reads exactly one message by borrowing an 8-byte aligned payload.
    ///
    /// The returned reader cannot outlive `payload`. Segment parsing uses the
    /// backend's allocation-free flat-slice API. Nested fields remain lazy and
    /// field access can fail after this method succeeds. The typed root is
    /// checked here and consumes part of the reader's traversal budget.
    ///
    /// # Errors
    ///
    /// Returns an error for an unaligned payload, a malformed or truncated
    /// segment table, trailing bytes, an invalid root, or an exceeded read limit.
    pub fn read_aligned<'a>(&self, payload: &'a [u8]) -> capnp::Result<BorrowedMessage<'a, D>> {
        if payload.as_ptr().align_offset(8) != 0 {
            return Err(capnp::Error::failed(
                "payload must be 8-byte aligned".into(),
            ));
        }
        let mut remaining = payload;
        let reader =
            capnp::serialize::read_message_from_flat_slice_no_alloc(&mut remaining, self.options)?;
        require_consumed(remaining)?;
        let typed = reader.into_typed::<D>();
        typed.get()?;
        Ok(typed)
    }
}

impl<E: Owned, D: Owned> Codec for CapnpCodec<E, D> {
    type EncodeItem = TypedBuilder<E>;
    type DecodeItem = OwnedMessage<D>;
    type Error = capnp::Error;

    const CONTENT_TYPE: &'static str = Self::CONTENT_TYPE;

    fn encode(&mut self, item: &Self::EncodeItem, destination: &mut Vec<u8>) -> capnp::Result<()> {
        capnp::serialize::write_message(destination, item.borrow_inner())
    }

    fn decode(&mut self, payload: &[u8]) -> capnp::Result<Self::DecodeItem> {
        // Validate declared body lengths before allocating owned segment storage.
        // The framing-only preflight does not dereference unaligned message words.
        let mut preflight = payload;
        drop(capnp::serialize::read_message_from_flat_slice(
            &mut preflight,
            self.options,
        )?);
        require_consumed(preflight)?;
        let mut remaining = payload;
        let reader = capnp::serialize::read_message(&mut remaining, self.options)?;
        require_consumed(remaining)?;
        let typed = reader.into_typed::<D>();
        typed.get()?;
        Ok(typed)
    }
}

fn require_consumed(remaining: &[u8]) -> capnp::Result<()> {
    if remaining.is_empty() {
        Ok(())
    } else {
        Err(capnp::Error::failed(
            "trailing bytes after Cap'n Proto message".into(),
        ))
    }
}

#[cfg(test)]
mod unit {
    use capnp::{Word, data, text};

    use super::*;

    type DataCodec = CapnpCodec<data::Owned, data::Owned>;

    fn data_message(bytes: &[u8]) -> TypedBuilder<data::Owned> {
        let mut message = TypedBuilder::new_default();
        message.set_root(bytes).expect("data root");
        message
    }

    fn encoded_data(bytes: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();
        DataCodec::new()
            .encode(&data_message(bytes), &mut output)
            .expect("encode data");
        output
    }

    fn aligned(bytes: &[u8]) -> Vec<Word> {
        let mut words = Word::allocate_zeroed_vec(bytes.len().div_ceil(8));
        Word::words_to_bytes_mut(&mut words)[..bytes.len()].copy_from_slice(bytes);
        words
    }

    fn rejects_both(bytes: &[u8]) {
        let words = aligned(bytes);
        let input = &Word::words_to_bytes(&words)[..bytes.len()];
        assert!(DataCodec::new().read_aligned(input).is_err());
        assert!(DataCodec::new().decode(input).is_err());
    }

    #[test]
    fn content_type_identifies_capnp() {
        assert_eq!(<DataCodec as Codec>::CONTENT_TYPE, "application/grpc+capnp");
    }

    #[test]
    fn caller_buffer_encoding_appends_and_reuses_capacity() {
        let message = data_message(b"outbound payload");
        let expected = capnp::serialize::write_message_to_words(message.borrow_inner());
        let mut output = Vec::with_capacity(expected.len() + 32);
        output.extend_from_slice(b"prefix");
        let pointer = output.as_ptr();
        DataCodec::new()
            .encode(&message, &mut output)
            .expect("encode");
        assert_eq!(&output[..6], b"prefix");
        assert_eq!(&output[6..], expected);
        assert_eq!(output.as_ptr(), pointer);
    }

    #[test]
    fn owned_decode_survives_reuse_of_unaligned_input() {
        let encoded = encoded_data(b"owned payload");
        let mut words = Word::allocate_zeroed_vec((encoded.len() + 1).div_ceil(8));
        let storage = Word::words_to_bytes_mut(&mut words);
        storage[1..1 + encoded.len()].copy_from_slice(&encoded);
        let input = &mut storage[1..1 + encoded.len()];
        let decoded = DataCodec::new().decode(input).expect("owned decode");
        input.fill(0);
        assert_eq!(decoded.get().expect("data root"), b"owned payload");
    }

    #[test]
    fn aligned_data_root_aliases_original_payload() {
        let encoded = encoded_data(b"borrowed payload");
        let words = aligned(&encoded);
        let input = Word::words_to_bytes(&words);
        let reader = DataCodec::new()
            .read_aligned(input)
            .expect("borrowed decode");
        let root = reader.get().expect("data root");
        assert_eq!(root, b"borrowed payload");
        assert_eq!(root.as_ptr(), input[16..].as_ptr());
    }

    #[test]
    fn text_roots_round_trip_through_both_read_paths() {
        let mut codec = CapnpCodec::<text::Owned, text::Owned>::new();
        let mut message = TypedBuilder::new_default();
        message.set_root("typed text".into()).expect("text root");
        let mut output = Vec::new();
        codec.encode(&message, &mut output).expect("encode text");
        let words = aligned(&output);
        let borrowed = codec
            .read_aligned(Word::words_to_bytes(&words))
            .expect("read text");
        assert_eq!(
            borrowed.get().expect("root").to_str().expect("utf8"),
            "typed text"
        );
        let owned = codec.decode(&output).expect("owned text");
        assert_eq!(
            owned.get().expect("root").to_str().expect("utf8"),
            "typed text"
        );
    }

    #[test]
    fn distinct_request_and_response_schema_types() {
        let mut codec = CapnpCodec::<text::Owned, data::Owned>::default();
        let mut request = TypedBuilder::new_default();
        request.set_root("request".into()).expect("text root");
        codec
            .encode(&request, &mut Vec::new())
            .expect("encode request");
        let response = codec
            .decode(&encoded_data(b"response"))
            .expect("decode response");
        assert_eq!(response.get().expect("data root"), b"response");
    }

    #[test]
    fn multiple_segments_round_trip_and_alias_payload() {
        let allocator = capnp::message::HeapAllocator::new().first_segment_words(1);
        let mut message = TypedBuilder::<data::Owned>::new(capnp::message::Builder::new(allocator));
        message
            .set_root(&b"multiple segments"[..])
            .expect("data root");
        let mut output = Vec::new();
        DataCodec::new()
            .encode(&message, &mut output)
            .expect("encode");
        assert_ne!(&output[..4], &[0; 4]);
        let words = aligned(&output);
        let input = Word::words_to_bytes(&words);
        let borrowed = DataCodec::new()
            .read_aligned(input)
            .expect("borrowed decode");
        let root = borrowed.get().expect("root");
        assert_eq!(root, b"multiple segments");
        assert!(root.as_ptr() >= input.as_ptr());
        assert!(root.as_ptr_range().end <= input.as_ptr_range().end);
        assert_eq!(
            DataCodec::new()
                .decode(&output)
                .expect("owned decode")
                .get()
                .expect("root"),
            root
        );
    }

    #[test]
    fn scenario_validates_requested_root_type() {
        let encoded = encoded_data(b"no terminator");
        let words = aligned(&encoded);
        let mut codec = CapnpCodec::<data::Owned, text::Owned>::new();
        assert!(codec.decode(&encoded).is_err());
        assert!(codec.read_aligned(Word::words_to_bytes(&words)).is_err());
    }

    #[test]
    fn scenario_rejects_unaligned_borrow_without_copying() {
        let encoded = encoded_data(b"alignment");
        let mut words = Word::allocate_zeroed_vec((encoded.len() + 1).div_ceil(8));
        let storage = Word::words_to_bytes_mut(&mut words);
        storage[1..1 + encoded.len()].copy_from_slice(&encoded);
        assert!(
            DataCodec::new()
                .read_aligned(&storage[1..1 + encoded.len()])
                .is_err()
        );
    }

    #[test]
    fn scenario_rejects_every_truncated_prefix() {
        let encoded = encoded_data(b"truncation");
        for length in 0..encoded.len() {
            rejects_both(&encoded[..length]);
        }
    }

    #[test]
    fn scenario_rejects_trailing_bytes_and_concatenated_messages() {
        let encoded = encoded_data(b"first");
        for suffix in [vec![0], vec![0; 8], encoded_data(b"second")] {
            let mut payload = encoded.clone();
            payload.extend_from_slice(&suffix);
            rejects_both(&payload);
        }
    }

    #[test]
    fn scenario_rejects_malformed_segment_table_and_root() {
        let mut invalid_count = [0_u8; 8];
        invalid_count[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        rejects_both(&invalid_count);
        let mut invalid_root = encoded_data(b"root");
        invalid_root[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        rejects_both(&invalid_root);
        let mut missing_segment = [0_u8; 8];
        missing_segment[4..].copy_from_slice(&1_u32.to_le_bytes());
        rejects_both(&missing_segment);
    }

    #[test]
    fn scenario_rejects_oversized_declarations_before_reading_body() {
        let mut header = [0_u8; 8];
        header[4..].copy_from_slice(&u32::MAX.to_le_bytes());
        rejects_both(&header);
    }

    #[test]
    fn scenario_rejects_far_pointers_outside_declared_segments() {
        for pointer in [(1_u64 << 32) | 2, (1 << 3) | 2, 6, 2] {
            let mut payload = [0_u8; 16];
            payload[4..8].copy_from_slice(&1_u32.to_le_bytes());
            payload[8..].copy_from_slice(&pointer.to_le_bytes());
            rejects_both(&payload);
        }
    }

    #[test]
    fn scenario_segment_count_limit_matches_owned_and_borrowed_reading() {
        let count = capnp::serialize::SEGMENTS_COUNT_LIMIT - 1;
        let table_bytes = (count / 2 + 1) * 8;
        let mut payload = vec![0_u8; table_bytes + 8];
        payload[..4].copy_from_slice(&u32::try_from(count - 1).unwrap().to_le_bytes());
        payload[4..8].copy_from_slice(&1_u32.to_le_bytes());
        let words = aligned(&payload);
        let mut codec = DataCodec::new();
        assert!(codec.decode(&payload).unwrap().get().unwrap().is_empty());
        assert!(
            codec
                .read_aligned(Word::words_to_bytes(&words))
                .unwrap()
                .get()
                .unwrap()
                .is_empty()
        );

        payload[..4].copy_from_slice(&u32::try_from(count).unwrap().to_le_bytes());
        rejects_both(&payload);
    }

    #[test]
    fn scenario_repeated_root_access_consumes_each_readers_own_budget() {
        let payload = encoded_data(b"one word");
        let words = aligned(&payload);
        let input = Word::words_to_bytes(&words);
        // Each root access reads the pointer word and the data word, including
        // the validation performed while constructing each typed reader.
        let mut codec = DataCodec::with_limits(4, 8).unwrap();
        let owned = codec.decode(input).unwrap();
        let borrowed = codec.read_aligned(input).unwrap();

        assert_eq!(owned.get().unwrap(), b"one word");
        assert_eq!(borrowed.get().unwrap(), b"one word");
        assert_eq!(
            owned.get().unwrap_err().kind,
            capnp::ErrorKind::ReadLimitExceeded
        );
        assert_eq!(
            borrowed.get().unwrap_err().kind,
            capnp::ErrorKind::ReadLimitExceeded
        );

        assert_eq!(codec.decode(input).unwrap().get().unwrap(), b"one word");
        assert_eq!(
            codec.read_aligned(input).unwrap().get().unwrap(),
            b"one word"
        );
    }

    #[test]
    fn scenario_enforces_configured_word_limits() {
        let encoded = encoded_data(&[42; 64]);
        let words = aligned(&encoded);
        let mut limited = DataCodec::with_limits(2, 8).expect("limits");
        assert!(limited.decode(&encoded).is_err());
        assert!(limited.read_aligned(Word::words_to_bytes(&words)).is_err());
        let mut sufficient = DataCodec::with_limits(64, 8).expect("limits");
        assert_eq!(
            sufficient
                .decode(&encoded)
                .expect("decode")
                .get()
                .expect("root"),
            &[42; 64]
        );
        assert!(
            sufficient
                .read_aligned(Word::words_to_bytes(&words))
                .is_ok()
        );
    }

    #[test]
    fn scenario_nested_field_access_enforces_nesting_limit_lazily() {
        type Nested = capnp::list_list::Owned<capnp::primitive_list::Owned<u8>>;
        type NestedCodec = CapnpCodec<Nested, Nested>;

        let mut inner = TypedBuilder::<capnp::primitive_list::Owned<u8>>::new_default();
        inner.initn_root(1).set(0, 42);
        let mut message = TypedBuilder::<Nested>::new_default();
        message
            .initn_root(1)
            .set(0, inner.get_root_as_reader().expect("inner list"))
            .expect("nested list");
        let mut payload = Vec::new();
        NestedCodec::new()
            .encode(&message, &mut payload)
            .expect("encode");
        let words = aligned(&payload);
        let input = Word::words_to_bytes(&words);

        let mut limited = NestedCodec::with_limits(64, 1).expect("limits");
        let owned = limited.decode(input).expect("root fits nesting limit");
        let borrowed = limited
            .read_aligned(input)
            .expect("root fits nesting limit");
        for root in [
            owned.get().expect("owned root"),
            borrowed.get().expect("borrowed root"),
        ] {
            assert_eq!(root.len(), 1);
            assert_eq!(
                root.get(0)
                    .expect_err("inner list exceeds nesting limit")
                    .kind,
                capnp::ErrorKind::NestingLimitExceeded
            );
        }

        let mut sufficient = NestedCodec::with_limits(64, 2).expect("limits");
        let owned = sufficient.decode(input).expect("owned decode");
        let borrowed = sufficient.read_aligned(input).expect("borrowed decode");
        for root in [
            owned.get().expect("owned root"),
            borrowed.get().expect("borrowed root"),
        ] {
            let inner = root.get(0).expect("inner list fits nesting limit");
            assert_eq!(inner.len(), 1);
            assert_eq!(inner.get(0), 42);
        }
    }

    #[test]
    fn scenario_preflights_body_at_allocation_limit() {
        let words = ReaderOptions::new().traversal_limit_in_words.unwrap();
        let mut header = [0_u8; 8];
        header[4..].copy_from_slice(&(words as u32).to_le_bytes());
        let error = DataCodec::new().decode(&header).err().unwrap();
        assert_eq!(
            error.kind,
            capnp::ErrorKind::MessageEndsPrematurely(words, 0)
        );
        let mut unaligned = [0_u8; 9];
        unaligned[1..].copy_from_slice(&header);
        let error = DataCodec::new().decode(&unaligned[1..]).err().unwrap();
        assert_eq!(
            error.kind,
            capnp::ErrorKind::MessageEndsPrematurely(words, 0)
        );
    }

    #[test]
    fn scenario_rejects_nonpositive_limits() {
        assert!(DataCodec::with_limits(0, 64).is_err());
        assert!(DataCodec::with_limits(64, 0).is_err());
        assert!(DataCodec::with_limits(64, -1).is_err());
    }
}
