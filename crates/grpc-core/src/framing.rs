//! Incremental gRPC framing with eight-byte-aligned message payloads.

use uarch::prelude::*;

use crate::status::Status;

/// Size of the compressed flag and big-endian message length on the wire.
pub const FRAME_HEADER_SIZE: usize = 5;
/// Default maximum encoded message size accepted by the decoder.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
/// Five-byte gRPC envelope descriptor with a register-passing layout proof.
#[register_pass]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameEnvelopeHeader {
    compressed: u8,
    length_msb: u8,
    length_high: u8,
    length_low: u8,
    length_lsb: u8,
}
impl FrameEnvelopeHeader {
    /// Constructs a wire header without allocating message storage.
    #[hot_inline]
    pub fn new(payload_len: u32, compressed: bool) -> Self {
        let [length_msb, length_high, length_low, length_lsb] = payload_len.to_be_bytes();
        Self {
            compressed: predicate(compressed).select(1, 0),
            length_msb,
            length_high,
            length_low,
            length_lsb,
        }
    }
    /// Validates a complete wire prefix.
    /// # Errors
    /// Rejects compression flags other than zero or one.
    #[hot_inline]
    pub fn parse(wire: [u8; FRAME_HEADER_SIZE]) -> Result<Self, Status> {
        if !predicate(wire[0] <= 1).as_bool() {
            return Err(invalid_envelope_flag());
        }
        Ok(Self {
            compressed: wire[0],
            length_msb: wire[1],
            length_high: wire[2],
            length_low: wire[3],
            length_lsb: wire[4],
        })
    }
    /// Returns the exact five-byte network representation.
    #[hot_inline]
    pub const fn wire_bytes(self) -> [u8; FRAME_HEADER_SIZE] {
        [
            self.compressed,
            self.length_msb,
            self.length_high,
            self.length_low,
            self.length_lsb,
        ]
    }
    /// Encoded payload length, excluding the envelope.
    #[hot_inline]
    pub const fn payload_len(self) -> u32 {
        u32::from_be_bytes([
            self.length_msb,
            self.length_high,
            self.length_low,
            self.length_lsb,
        ])
    }
    /// Whether the negotiated compression algorithm applies to this payload.
    #[hot_inline]
    pub const fn is_compressed(self) -> bool {
        self.compressed == 1
    }
}
#[cold_sink]
fn invalid_envelope_flag() -> Status {
    Status::internal("invalid gRPC compressed flag")
}

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
        bytes[LEADING_PADDING..PAYLOAD_OFFSET]
            .copy_from_slice(&FrameEnvelopeHeader::new(wire_len, compressed).wire_bytes());
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
    #[inline]
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<AlignedMessageBuf>, Status> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if chunk.is_empty() {
            return Ok(Vec::new());
        }
        if self.message.is_none() && chunk.len() < FRAME_HEADER_SIZE - self.header_len {
            let end = self.header_len + chunk.len();
            self.header[self.header_len..end].copy_from_slice(chunk);
            self.header_len = end;
            return Ok(Vec::new());
        }
        self.push_chunk(chunk)
    }

    fn push_chunk(&mut self, mut chunk: &[u8]) -> Result<Vec<AlignedMessageBuf>, Status> {
        let mut messages = Vec::new();
        while !chunk.is_empty() {
            if self.message.is_none() && self.header_len == 0 && chunk.len() >= FRAME_HEADER_SIZE {
                let length = match self.header_length(chunk) {
                    Ok(length) => length,
                    Err(error) => return Err(self.fail(error)),
                };
                if length <= chunk.len() - FRAME_HEADER_SIZE {
                    let end = FRAME_HEADER_SIZE + length;
                    let message =
                        AlignedMessageBuf::new(&chunk[FRAME_HEADER_SIZE..end], chunk[0] == 1)
                            .map_err(|error| self.fail(error))?;
                    messages.push(message);
                    chunk = &chunk[end..];
                    continue;
                }
            }
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

    #[hot_inline]
    fn header_length(&self, header: &[u8]) -> Result<usize, Status> {
        let descriptor =
            FrameEnvelopeHeader::parse([header[0], header[1], header[2], header[3], header[4]])?;
        let length = descriptor.payload_len();
        let length = usize::try_from(length)
            .map_err(|_| Status::resource_exhausted("gRPC message length exceeds platform size"))?;
        if length > self.max_message_size {
            return Err(Status::resource_exhausted(
                "gRPC message exceeds maximum decoding size",
            ));
        }
        Ok(length)
    }

    fn start_message(&mut self) -> Result<(), Status> {
        let length = self.header_length(&self.header)?;
        self.message = Some(AlignedMessageBuf::with_payload_len(
            length,
            self.header[0] == 1,
        )?);
        Ok(())
    }

    #[cold_sink]
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

uarch::layout::assert_layout!(FrameEnvelopeHeader, size = 5, align = 1);
