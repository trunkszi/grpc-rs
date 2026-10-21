//! Incremental gRPC framing with eight-byte-aligned message payloads.

use crate::status::Status;

/// Size of the compressed flag and big-endian message length on the wire.
pub const FRAME_HEADER_SIZE: usize = 5;
/// Default maximum encoded message size accepted by the decoder.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const LEADING_PADDING: usize = 3;
const PAYLOAD_OFFSET: usize = LEADING_PADDING + FRAME_HEADER_SIZE;

#[repr(C, align(8))]
#[derive(Clone, Debug)]
struct AlignedWord([u8; 8]);

/// An owned gRPC message whose payload starts at an eight-byte-aligned address.
///
/// Three private padding bytes precede the five-byte gRPC header. Only
/// [`Self::wire_bytes`] should be transmitted; the padding never goes on the wire.
/// Alignment applies to the encoded payload, including compressed payloads.
#[derive(Clone, Debug)]
pub struct AlignedMessageBuf {
    words: Vec<AlignedWord>,
    payload_len: usize,
}

impl AlignedMessageBuf {
    /// Copies a payload into aligned storage and constructs its wire prefix.
    ///
    /// # Errors
    /// Returns resource exhausted if the payload exceeds the wire length limit
    /// or storage cannot be allocated.
    pub fn new(payload: &[u8], compressed: bool) -> Result<Self, Status> {
        let mut message = Self::with_payload_len(payload.len(), compressed)?;
        message.payload_mut().copy_from_slice(payload);
        Ok(message)
    }

    fn with_payload_len(payload_len: usize, compressed: bool) -> Result<Self, Status> {
        let wire_len = u32::try_from(payload_len)
            .map_err(|_| Status::resource_exhausted("gRPC message length exceeds u32"))?;
        let storage_len = payload_len
            .checked_add(PAYLOAD_OFFSET)
            .ok_or_else(|| Status::resource_exhausted("gRPC aligned storage size overflow"))?;
        let word_count = storage_len.div_ceil(size_of::<AlignedWord>());
        let mut words = Vec::new();
        words
            .try_reserve_exact(word_count)
            .map_err(|_| Status::resource_exhausted("cannot allocate gRPC message storage"))?;
        words.resize(word_count, AlignedWord([0; 8]));
        let mut message = Self { words, payload_len };
        let bytes = message.storage_mut();
        bytes[LEADING_PADDING] = u8::from(compressed);
        bytes[LEADING_PADDING + 1..PAYLOAD_OFFSET].copy_from_slice(&wire_len.to_be_bytes());
        Ok(message)
    }

    fn storage(&self) -> &[u8] {
        // SAFETY: Every aligned word contains eight initialized bytes and has no
        // padding. The
        // byte slice stays within the allocation and borrows it immutably.
        unsafe {
            std::slice::from_raw_parts(
                self.words.as_ptr().cast::<u8>(),
                self.words.len() * size_of::<AlignedWord>(),
            )
        }
    }

    fn storage_mut(&mut self) -> &mut [u8] {
        // SAFETY: The exclusive borrow covers initialized byte arrays without
        // padding. Every byte value is valid, and the slice cannot outlive it.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.words.as_mut_ptr().cast::<u8>(),
                self.words.len() * size_of::<AlignedWord>(),
            )
        }
    }

    /// Returns the aligned message payload without its gRPC header.
    pub fn payload(&self) -> &[u8] {
        &self.storage()[PAYLOAD_OFFSET..PAYLOAD_OFFSET + self.payload_len]
    }

    /// Returns the aligned payload for in-place processing without changing its length.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        let end = PAYLOAD_OFFSET + self.payload_len;
        &mut self.storage_mut()[PAYLOAD_OFFSET..end]
    }

    /// Returns precisely the five-byte header followed by the payload.
    pub fn wire_bytes(&self) -> &[u8] {
        &self.storage()[LEADING_PADDING..PAYLOAD_OFFSET + self.payload_len]
    }

    /// Whether the gRPC compressed flag is set; decompression is a separate step.
    pub fn is_compressed(&self) -> bool {
        self.storage()[LEADING_PADDING] == 1
    }

    /// Returns the encoded payload length.
    pub fn len(&self) -> usize {
        self.payload_len
    }

    /// Whether the encoded payload is empty.
    pub fn is_empty(&self) -> bool {
        self.payload_len == 0
    }
}

/// Decodes arbitrary fragmented or coalesced HTTP/2 DATA payloads.
///
/// Length limits are checked before allocating a message body. A failed decoder
/// rejects further input until reset. An error from `push` discards any messages
/// completed in that call; callers must terminate the affected RPC stream.
#[derive(Debug)]
pub struct LengthPrefixedFrame {
    max_message_size: usize,
    header: [u8; FRAME_HEADER_SIZE],
    header_len: usize,
    message: Option<AlignedMessageBuf>,
    received: usize,
    failure: Option<Status>,
}

impl Default for LengthPrefixedFrame {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_MESSAGE_SIZE)
    }
}

impl LengthPrefixedFrame {
    /// Creates a decoder with a maximum encoded payload length in bytes.
    pub fn new(max_message_size: usize) -> Self {
        Self {
            max_message_size,
            header: [0; FRAME_HEADER_SIZE],
            header_len: 0,
            message: None,
            received: 0,
            failure: None,
        }
    }

    /// Consumes one DATA chunk and returns every complete message in wire order.
    ///
    /// Empty chunks are valid. Partial headers and bodies remain buffered until
    /// the next call. Compression negotiation and decompression belong to the caller.
    ///
    /// # Errors
    /// Returns internal for an invalid compressed flag, resource exhausted for
    /// oversized messages or allocation failure, or the previous decoder failure.
    pub fn push(&mut self, mut chunk: &[u8]) -> Result<Vec<AlignedMessageBuf>, Status> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let mut messages = Vec::new();
        while !chunk.is_empty() {
            if self.message.is_none() {
                let count = (FRAME_HEADER_SIZE - self.header_len).min(chunk.len());
                self.header[self.header_len..self.header_len + count]
                    .copy_from_slice(&chunk[..count]);
                self.header_len += count;
                chunk = &chunk[count..];
                if self.header_len < FRAME_HEADER_SIZE {
                    continue;
                }
                let result = self.start_message();
                if let Err(error) = result {
                    return Err(self.fail(error));
                }
            }
            if let Some(message) = &mut self.message {
                let count = (message.len() - self.received).min(chunk.len());
                message.payload_mut()[self.received..self.received + count]
                    .copy_from_slice(&chunk[..count]);
                self.received += count;
                chunk = &chunk[count..];
                if self.received == message.len() {
                    if messages.try_reserve(1).is_err() {
                        return Err(self.fail(Status::resource_exhausted(
                            "cannot allocate gRPC decoded message list",
                        )));
                    }
                    if let Some(complete) = self.message.take() {
                        messages.push(complete);
                    }
                    self.header_len = 0;
                    self.received = 0;
                }
            }
        }
        Ok(messages)
    }

    fn start_message(&mut self) -> Result<(), Status> {
        if self.header[0] > 1 {
            return Err(Status::internal("invalid gRPC compressed flag"));
        }
        let length = u32::from_be_bytes([
            self.header[1],
            self.header[2],
            self.header[3],
            self.header[4],
        ]);
        let length = usize::try_from(length)
            .map_err(|_| Status::resource_exhausted("gRPC message length exceeds platform size"))?;
        if length > self.max_message_size {
            return Err(Status::resource_exhausted(
                "gRPC message exceeds maximum decoding size",
            ));
        }
        self.message = Some(AlignedMessageBuf::with_payload_len(
            length,
            self.header[0] == 1,
        )?);
        Ok(())
    }

    fn fail(&mut self, error: Status) -> Status {
        self.message = None;
        self.failure = Some(error.clone());
        error
    }

    /// Validates the end of a DATA stream, including an empty stream.
    ///
    /// # Errors
    /// Returns internal for a truncated header or body, or the previous failure.
    pub fn finish(&mut self) -> Result<(), Status> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.header_len != 0 || self.message.is_some() {
            return Err(self.fail(Status::internal("truncated gRPC message frame")));
        }
        Ok(())
    }

    /// Discards buffered input and failure state before decoding a different stream.
    pub fn reset(&mut self) {
        self.header_len = 0;
        self.message = None;
        self.received = 0;
        self.failure = None;
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    use crate::status::Code;

    fn assert_aligned(message: &AlignedMessageBuf, payload: &[u8], compressed: bool) {
        assert_eq!(message.payload().as_ptr() as usize % 8, 0);
        assert_eq!(message.payload(), payload);
        assert_eq!(message.is_compressed(), compressed);
        assert_eq!(
            message.wire_bytes().len(),
            FRAME_HEADER_SIZE + payload.len()
        );
        assert_eq!(message.wire_bytes()[0], u8::from(compressed));
        assert_eq!(
            &message.wire_bytes()[1..FRAME_HEADER_SIZE],
            &(payload.len() as u32).to_be_bytes()
        );
    }

    #[test]
    fn scenario_alignment_survives_all_chunk_sizes_and_coalesced_messages() {
        let payloads: [&[u8]; 4] = [b"", b"a", b"12345678", b"arbitrary payload length"];
        let mut wire = Vec::new();
        for (index, payload) in payloads.iter().enumerate() {
            let message = AlignedMessageBuf::new(payload, index % 2 == 1).unwrap();
            assert_aligned(&message, payload, index % 2 == 1);
            assert_aligned(&message.clone(), payload, index % 2 == 1);
            wire.extend_from_slice(message.wire_bytes());
        }
        for chunk_size in 1..=wire.len() {
            let mut decoder = LengthPrefixedFrame::new(128);
            let mut decoded = Vec::new();
            for chunk in wire.chunks(chunk_size) {
                decoded.extend(decoder.push(chunk).unwrap());
                assert!(decoder.push(&[]).unwrap().is_empty());
            }
            decoder.finish().unwrap();
            assert_eq!(decoded.len(), payloads.len());
            for (index, (message, payload)) in decoded.iter().zip(payloads).enumerate() {
                assert_aligned(message, payload, index % 2 == 1);
            }
        }
    }

    #[test]
    fn scenario_every_single_split_preserves_alignment_and_wire_bytes() {
        let original = AlignedMessageBuf::new(b"capnp aligned payload", false).unwrap();
        for split in 0..=original.wire_bytes().len() {
            let mut decoder = LengthPrefixedFrame::default();
            let mut decoded = decoder.push(&original.wire_bytes()[..split]).unwrap();
            decoded.extend(decoder.push(&original.wire_bytes()[split..]).unwrap());
            decoder.finish().unwrap();
            assert_eq!(decoded.len(), 1);
            assert_aligned(&decoded[0], original.payload(), false);
            assert_eq!(decoded[0].wire_bytes(), original.wire_bytes());
        }
    }

    #[test]
    fn scenario_corrupt_flags_fail_closed_until_reset() {
        for flag in [2, 127, 255] {
            let mut decoder = LengthPrefixedFrame::default();
            let error = decoder.push(&[flag, 0, 0, 0, 0]).unwrap_err();
            assert_eq!(error.code(), Code::Internal);
            assert_eq!(decoder.push(&[]).unwrap_err().code(), Code::Internal);
            assert_eq!(decoder.finish().unwrap_err().code(), Code::Internal);
            decoder.reset();
            assert_eq!(decoder.push(&[0; 5]).unwrap().len(), 1);
            decoder.finish().unwrap();
        }
    }

    #[test]
    fn scenario_size_limit_rejects_header_without_receiving_body() {
        let mut decoder = LengthPrefixedFrame::new(3);
        assert!(decoder.push(&[0, 0, 0, 0]).unwrap().is_empty());
        assert_eq!(
            decoder.push(&[4]).unwrap_err().code(),
            Code::ResourceExhausted
        );
        decoder.reset();
        let message = AlignedMessageBuf::new(b"abc", false).unwrap();
        assert_eq!(decoder.push(message.wire_bytes()).unwrap().len(), 1);
        decoder.finish().unwrap();
        decoder.reset();
        assert_eq!(
            decoder.push(&[0, 255, 255, 255, 255]).unwrap_err().code(),
            Code::ResourceExhausted
        );
    }

    #[test]
    fn scenario_truncation_detects_every_partial_header_and_body() {
        let message = AlignedMessageBuf::new(b"body", false).unwrap();
        for end in 1..message.wire_bytes().len() {
            let mut decoder = LengthPrefixedFrame::default();
            assert!(
                decoder
                    .push(&message.wire_bytes()[..end])
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(decoder.finish().unwrap_err().code(), Code::Internal);
        }
        LengthPrefixedFrame::default().finish().unwrap();
    }

    #[test]
    fn scenario_zero_limit_accepts_empty_payload_only() {
        let mut decoder = LengthPrefixedFrame::new(0);
        let decoded = decoder.push(&[0; 10]).unwrap();
        assert_eq!(decoded.len(), 2);
        for message in decoded {
            assert_aligned(&message, b"", false);
        }
        decoder.finish().unwrap();
        assert_eq!(
            decoder.push(&[0, 0, 0, 0, 1]).unwrap_err().code(),
            Code::ResourceExhausted
        );
    }
}
