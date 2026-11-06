//! Bounded, single-threaded ownership of HTTP/2 stream receive events.
//!
//! Budgets count metadata, message descriptors and wire bytes, excluding allocator overhead.
//! Normalized payloads and bounded reusable queues limit spare capacity. Rejections preserve
//! accepted data.
use std::{
    collections::{BTreeMap, VecDeque, btree_map::Entry},
    mem::size_of,
};

use uarch::prelude::*;

bitword! {
    struct SlotKey(u32) {
        [0..=5] BIT: u8,
        [6..=30] BLOCK: u32,
    }
}

/// Ordered raw HTTP/2 metadata, including protocol keys such as `grpc-status`.
pub type Headers = Vec<(String, Vec<u8>)>;

/// One complete gRPC envelope; decompression belongs to the codec boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamMessage {
    /// Whether the payload uses the negotiated message compression algorithm.
    pub compressed: bool,
    /// Encoded message bytes without the five-byte envelope.
    pub payload: Vec<u8>,
}

/// Independent receive limits applied to every stream.
#[register_pass]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamLimits {
    /// Maximum retained bytes, including metadata and message descriptors.
    pub max_buffered_bytes: usize,
    /// Maximum complete messages awaiting consumption, including empty messages.
    pub max_queued_messages: usize,
}
impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            max_buffered_bytes: 8 * 1024 * 1024,
            max_queued_messages: 1024,
        }
    }
}

/// A rejected stream operation. Rejection never consumes an input prefix.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SlotMapError {
    /// HTTP/2 stream identifiers must be positive signed 31-bit integers.
    #[error("invalid stream identifier {0}")]
    InvalidStreamId(i32),
    /// The identifier already has a slot.
    #[error("stream {0} is already open")]
    AlreadyOpen(i32),
    /// The identifier has no slot.
    #[error("unknown stream {0}")]
    UnknownStream(i32),
    /// The peer's GOAWAY prohibits this stream.
    #[error("GOAWAY: stream={stream_id}, last={last_stream_id}, code={error_code}")]
    GoAway {
        stream_id: i32,
        last_stream_id: i32,
        error_code: u32,
    },
    /// An error or incomplete-response reset prevents reads of buffered events.
    #[error("stream {stream_id} reset: code={error_code}")]
    Reset { stream_id: i32, error_code: u32 },
    /// The event violates the receive or half-close lifecycle.
    #[error("stream {stream_id}: cannot {operation}")]
    InvalidTransition {
        stream_id: i32,
        operation: &'static str,
    },
    /// The stream would exceed its retained byte budget.
    #[error("stream {stream_id}: byte limit {limit}")]
    MemoryLimit { stream_id: i32, limit: usize },
    /// The stream would exceed its queued message count.
    #[error("stream {stream_id}: message limit {limit}")]
    MessageLimit { stream_id: i32, limit: usize },
    /// A gRPC envelope compression flag was neither zero nor one.
    #[error("stream {stream_id}: invalid compression flag {flag}")]
    InvalidCompressionFlag { stream_id: i32, flag: u8 },
    /// Trailers or END_STREAM arrived in an incomplete envelope.
    #[error("stream {0}: truncated envelope")]
    TruncatedFrame(i32),
    /// Both halves must close, or a reset must arrive, before removal.
    #[error("stream {0} is not closed")]
    NotClosed(i32),
}
/// Result of admitting, consuming or transitioning a stream event.
pub type SlotResult<T> = Result<T, SlotMapError>;
fn memory_limit(stream_id: i32, limits: StreamLimits) -> SlotMapError {
    SlotMapError::MemoryLimit {
        stream_id,
        limit: limits.max_buffered_bytes,
    }
}
fn transition(stream_id: i32, operation: &'static str) -> SlotMapError {
    SlotMapError::InvalidTransition {
        stream_id,
        operation,
    }
}

bitword! {
    /// Packed receive lifecycle flags and the complete HTTP/2 reset code.
    #[derive(Debug)]
    pub struct StreamStateWord(u64) {
        [0] pub INITIAL_SEEN,
        [1] pub TRAILERS_SEEN,
        [2] pub REMOTE_END,
        [3] pub LOCAL_END,
        [4] pub HAS_RESET,
        [32..=63] pub RESET_CODE: u32,
    }
}

/// Receive queues and independent local/remote lifecycle state for one stream.
#[derive(Debug, Default)]
pub struct StreamSlot {
    state: StreamStateWord,
    initial_metadata: Option<Headers>,
    initial_bytes: usize,
    messages: VecDeque<StreamMessage>,
    trailers: Option<Headers>,
    trailer_bytes: usize,
    partial: Vec<u8>,
    buffered_bytes: usize,
}
impl StreamSlot {
    /// Retained bytes charged to this stream's budget.
    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }
    /// Complete messages currently awaiting consumption.
    #[must_use]
    pub fn queued_message_count(&self) -> usize {
        self.messages.len()
    }
    /// Whether the peer has closed its sending half.
    #[must_use]
    pub const fn remote_end_stream(&self) -> bool {
        self.state.matches(pattern!(StreamStateWord { REMOTE_END }))
    }
    /// Whether the local endpoint has closed its sending half.
    #[must_use]
    pub const fn local_end_stream(&self) -> bool {
        self.state.matches(pattern!(StreamStateWord { LOCAL_END }))
    }
    /// The HTTP/2 RST_STREAM error code, if received.
    #[must_use]
    pub const fn reset_error_code(&self) -> Option<u32> {
        if self.state.matches(pattern!(StreamStateWord { HAS_RESET })) {
            Some(self.state.get(StreamStateWord::RESET_CODE))
        } else {
            None
        }
    }
    fn check_reset(&self, stream_id: i32) -> SlotResult<()> {
        match self.reset_error_code() {
            Some(0)
                if self
                    .state
                    .matches(pattern!(StreamStateWord { TRAILERS_SEEN & REMOTE_END })) =>
            {
                Ok(())
            }
            Some(error_code) => Err(SlotMapError::Reset {
                stream_id,
                error_code,
            }),
            None => Ok(()),
        }
    }
    #[hot_inline]
    fn receive(&self, stream_id: i32, operation: &'static str) -> SlotResult<()> {
        if self
            .state
            .matches(pattern!(StreamStateWord { !HAS_RESET & !REMOTE_END & !TRAILERS_SEEN }))
        {
            return Ok(());
        }
        self.check_reset(stream_id)?;
        Err(transition(stream_id, operation))
    }
    fn budget(&self, id: i32, bytes: usize, limits: StreamLimits) -> SlotResult<usize> {
        self.buffered_bytes
            .checked_add(bytes)
            .filter(|total| *total <= limits.max_buffered_bytes)
            .ok_or(memory_limit(id, limits))
    }
}

/// The local HTTP/2 endpoint determines which stream identifiers it initiates.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EndpointRole {
    /// Clients initiate odd-numbered streams.
    #[default]
    Client,
    /// Servers initiate even-numbered streams.
    Server,
}
impl EndpointRole {
    const fn initiates(self, id: i32) -> bool {
        matches!(self, Self::Client) == (id % 2 == 1)
    }
}
/// A synchronous stream demultiplexer with explicit ownership transfer on reads.
#[derive(Debug, Default)]
pub struct StreamSlotMap {
    slots: BTreeMap<i32, StreamSlot>,
    occupied: BTreeMap<u32, Bitmap64>,
    limits: StreamLimits,
    role: EndpointRole,
    goaway: Option<(i32, u32)>,
}
impl StreamSlotMap {
    /// Creates a client map. Zero limits intentionally disable buffering.
    #[must_use]
    pub const fn new(limits: StreamLimits) -> Self {
        Self::with_role(limits, EndpointRole::Client)
    }
    /// Creates an empty map for the specified local HTTP/2 endpoint role.
    #[must_use]
    pub const fn with_role(limits: StreamLimits, role: EndpointRole) -> Self {
        Self {
            slots: BTreeMap::new(),
            occupied: BTreeMap::new(),
            limits,
            role,
            goaway: None,
        }
    }
    /// Opens a stream without allocating receive buffers.
    /// # Errors
    /// Rejects invalid/existing identifiers and new local streams after GOAWAY.
    pub fn open_stream(&mut self, stream_id: i32) -> SlotResult<()> {
        if stream_id <= 0 {
            return Err(SlotMapError::InvalidStreamId(stream_id));
        }
        if let Some((last_stream_id, error_code)) = self.goaway
            && self.role.initiates(stream_id)
        {
            return Err(SlotMapError::GoAway {
                stream_id,
                last_stream_id,
                error_code,
            });
        }
        match self.slots.entry(stream_id) {
            Entry::Vacant(slot) => {
                slot.insert(StreamSlot::default());
                let key = SlotKey::new(stream_id as u32);
                let block = self.occupied.entry(key.get(SlotKey::BLOCK)).or_default();
                *block = block.set(u32::from(key.get(SlotKey::BIT)));
                Ok(())
            }
            Entry::Occupied(_) => Err(SlotMapError::AlreadyOpen(stream_id)),
        }
    }
    /// Looks up a stream without consuming events.
    #[must_use]
    #[inline]
    pub fn get(&self, stream_id: i32) -> Option<&StreamSlot> {
        let slot = self.slots.get(&stream_id)?;
        prefetch_read::<L1, _>(slot);
        Some(slot)
    }
    /// Number of retained stream slots, including closed slots awaiting removal.
    #[must_use]
    pub fn active_stream_count(&self) -> usize {
        self.slots.len()
    }
    /// Most restrictive GOAWAY boundary and its associated HTTP/2 error code.
    #[must_use]
    pub const fn goaway(&self) -> Option<(i32, u32)> {
        self.goaway
    }
    /// Stores the initial header block exactly once, even after it is consumed.
    /// # Errors
    /// Rejects unknown/reset streams, duplicate or late headers, and budget excess.
    pub fn push_initial_metadata(&mut self, stream_id: i32, headers: Headers) -> SlotResult<()> {
        self.push_headers(stream_id, headers, false)
    }
    /// Accepts arbitrary DATA fragmentation and queues complete gRPC envelopes.
    /// # Errors
    /// Rejects missing headers, ended/reset streams, malformed flags or excess resources.
    /// Rejections preserve accepted frames and partial bytes.
    pub fn append_recv_bytes(&mut self, stream_id: i32, bytes: &[u8]) -> SlotResult<()> {
        let limits = self.limits;
        let slot = self.slot_mut(stream_id)?;
        slot.receive(stream_id, "receive DATA")?;
        if !slot
            .state
            .matches(pattern!(StreamStateWord { INITIAL_SEEN }))
        {
            return Err(transition(
                stream_id,
                "receive DATA before initial metadata",
            ));
        }
        let mut total = slot.budget(stream_id, bytes.len(), limits)?;
        let len = slot.partial.len() + bytes.len();
        let at = |index: usize| {
            if index < slot.partial.len() {
                slot.partial[index]
            } else {
                bytes[index - slot.partial.len()]
            }
        };
        let mut consumed = 0;
        let mut count = 0;
        while consumed < len {
            let flag = at(consumed);
            if flag > 1 {
                return Err(SlotMapError::InvalidCompressionFlag { stream_id, flag });
            }
            if len - consumed < 5 {
                break;
            }
            let payload_len =
                u32::from_be_bytes(std::array::from_fn(|i| at(consumed + 1 + i))) as usize;
            let frame_len = payload_len
                .checked_add(5)
                .ok_or(memory_limit(stream_id, limits))?;
            if frame_len
                .checked_add(size_of::<StreamMessage>())
                .is_none_or(|required| required > limits.max_buffered_bytes)
            {
                return Err(memory_limit(stream_id, limits));
            }
            if len - consumed < frame_len {
                break;
            }
            count += 1;
            if slot
                .messages
                .len()
                .checked_add(count)
                .is_none_or(|n| n > limits.max_queued_messages)
            {
                return Err(SlotMapError::MessageLimit {
                    stream_id,
                    limit: limits.max_queued_messages,
                });
            }
            total = total
                .checked_add(size_of::<StreamMessage>())
                .filter(|n| *n <= limits.max_buffered_bytes)
                .ok_or(memory_limit(stream_id, limits))?;
            consumed += frame_len;
        }
        if count == 0 {
            slot.partial.extend_from_slice(bytes);
            slot.buffered_bytes = total;
            return Ok(());
        }
        let mut combined = Vec::with_capacity(len);
        combined.extend_from_slice(&slot.partial);
        combined.extend_from_slice(bytes);
        let mut offset = 0;
        for _ in 0..count {
            let payload_len =
                u32::from_be_bytes(std::array::from_fn(|i| combined[offset + 1 + i])) as usize;
            let end = offset + 5 + payload_len;
            slot.messages.push_back(StreamMessage {
                compressed: combined[offset] == 1,
                payload: combined[offset + 5..end].to_vec(),
            });
            offset = end;
        }
        slot.partial = combined[consumed..].to_vec();
        slot.buffered_bytes = total;
        Ok(())
    }
    /// Queues a complete envelope supplied by an upstream frame decoder.
    /// # Errors
    /// Rejects missing headers, ended/reset streams, partial frames or excess resources.
    pub fn push_message(&mut self, stream_id: i32, message: StreamMessage) -> SlotResult<()> {
        let limits = self.limits;
        let slot = self.slot_mut(stream_id)?;
        slot.receive(stream_id, "receive a message")?;
        if !slot
            .state
            .matches(pattern!(StreamStateWord { INITIAL_SEEN }))
            || !slot.partial.is_empty()
        {
            return Err(transition(
                stream_id,
                "receive decoded message before headers or within a partial frame",
            ));
        }
        if slot.messages.len() >= limits.max_queued_messages {
            return Err(SlotMapError::MessageLimit {
                stream_id,
                limit: limits.max_queued_messages,
            });
        }
        let additional = message
            .payload
            .len()
            .checked_add(5 + size_of::<StreamMessage>())
            .ok_or(memory_limit(stream_id, limits))?;
        let total = slot.budget(stream_id, additional, limits)?;
        slot.messages.push_back(StreamMessage {
            compressed: message.compressed,
            payload: message.payload.into_boxed_slice().into_vec(),
        });
        slot.buffered_bytes = total;
        Ok(())
    }
    /// Stores trailers or a Trailers-Only response; mark END_STREAM separately.
    /// # Errors
    /// Rejects ended/reset streams, duplicate trailers, partial frames or excess resources.
    pub fn push_trailing_metadata(&mut self, stream_id: i32, headers: Headers) -> SlotResult<()> {
        self.push_headers(stream_id, headers, true)
    }
    fn push_headers(&mut self, stream_id: i32, headers: Headers, trailing: bool) -> SlotResult<()> {
        let limits = self.limits;
        let slot = self.slot_mut(stream_id)?;
        slot.receive(stream_id, "receive metadata")?;
        if !trailing
            && slot
                .state
                .matches(pattern!(StreamStateWord { INITIAL_SEEN }))
        {
            return Err(transition(stream_id, "receive duplicate initial metadata"));
        }
        if trailing && !slot.partial.is_empty() {
            return Err(SlotMapError::TruncatedFrame(stream_id));
        }
        let additional = header_bytes(&headers).ok_or(memory_limit(stream_id, limits))?;
        let total = slot.budget(stream_id, additional, limits)?;
        if trailing {
            slot.trailer_bytes = additional;
            slot.trailers = Some(normalize_headers(headers));
            slot.state = slot.state.patch(patch!(StreamStateWord {
                TRAILERS_SEEN: true
            }));
        } else {
            slot.initial_bytes = additional;
            slot.initial_metadata = Some(normalize_headers(headers));
            slot.state = slot
                .state
                .patch(patch!(StreamStateWord { INITIAL_SEEN: true }));
        }
        slot.buffered_bytes = total;
        Ok(())
    }
    /// Closes the peer's sending half; request streams need not carry trailers.
    /// # Errors
    /// Rejects unknown/reset streams, duplicate END_STREAM and truncated envelopes.
    pub fn mark_remote_end_stream(&mut self, stream_id: i32) -> SlotResult<()> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        if slot.state.matches(pattern!(StreamStateWord { REMOTE_END })) {
            return Err(transition(stream_id, "receive duplicate END_STREAM"));
        }
        if !slot.partial.is_empty() {
            return Err(SlotMapError::TruncatedFrame(stream_id));
        }
        slot.state = slot
            .state
            .patch(patch!(StreamStateWord { REMOTE_END: true }));
        Ok(())
    }
    /// Closes the local sending half independently of the receive queues.
    /// # Errors
    /// Rejects unknown/reset streams or duplicate local END_STREAM.
    pub fn mark_local_end_stream(&mut self, stream_id: i32) -> SlotResult<()> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        if slot.state.matches(pattern!(StreamStateWord { LOCAL_END })) {
            return Err(transition(stream_id, "send duplicate END_STREAM"));
        }
        slot.state = slot
            .state
            .patch(patch!(StreamStateWord { LOCAL_END: true }));
        Ok(())
    }
    /// Closes both halves, preserving completed responses for NO_ERROR resets.
    /// # Errors
    /// Rejects unknown streams and repeated resets, preserving the first code.
    pub fn record_rst_stream(&mut self, stream_id: i32, error_code: u32) -> SlotResult<()> {
        let slot = self.slot_mut(stream_id)?;
        if let Some(error_code) = slot.reset_error_code() {
            return Err(SlotMapError::Reset {
                stream_id,
                error_code,
            });
        }
        if error_code == 0
            && slot
                .state
                .matches(pattern!(StreamStateWord { TRAILERS_SEEN }))
            && slot.state.matches(pattern!(StreamStateWord { REMOTE_END }))
        {
            slot.state = slot.state.patch(patch!(StreamStateWord {
                HAS_RESET: true,
                RESET_CODE: 0,
                LOCAL_END: true
            }));
            return Ok(());
        }
        *slot = StreamSlot {
            state: StreamStateWord::default()
                .patch(patch!(StreamStateWord {
                    HAS_RESET: true,
                    REMOTE_END: true,
                    LOCAL_END: true
                }))
                .patch(StreamStateWord::RESET_CODE.patch(error_code)),
            ..StreamSlot::default()
        };
        Ok(())
    }
    /// Prunes excluded local streams; peer-initiated streams remain unaffected.
    /// Repeated GOAWAY frames can only reduce the accepted stream boundary.
    /// # Errors
    /// Rejects negative boundaries and increases to an established boundary.
    pub fn apply_goaway(&mut self, last_stream_id: i32, error_code: u32) -> SlotResult<Vec<i32>> {
        if last_stream_id < 0 {
            return Err(SlotMapError::InvalidStreamId(last_stream_id));
        }
        if self
            .goaway
            .is_some_and(|(previous, _)| last_stream_id > previous)
        {
            return Err(transition(last_stream_id, "increase GOAWAY boundary"));
        }
        let mut removed = Vec::new();
        let first_block = SlotKey::new(last_stream_id as u32).get(SlotKey::BLOCK);
        for (&block, &bits) in self.occupied.range(first_block..) {
            let local = match self.role {
                EndpointRole::Client => Bitmap64::new(0xaaaa_aaaa_aaaa_aaaa),
                EndpointRole::Server => Bitmap64::new(0x5555_5555_5555_5555),
            };
            for bit in bits.intersection(local).iter_ones() {
                let id = ((block << 6) | bit) as i32;
                if id > last_stream_id {
                    removed.push(id);
                }
            }
        }
        for &id in &removed {
            self.slots.remove(&id);
            self.clear_occupied(id);
        }
        self.goaway = Some((last_stream_id, error_code));
        Ok(removed)
    }
    /// Transfers initial metadata ownership, leaving lifecycle history intact.
    /// # Errors
    /// Rejects unknown streams or resets other than completed-response NO_ERROR.
    pub fn pop_initial_metadata(&mut self, stream_id: i32) -> SlotResult<Option<Headers>> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        let headers = slot.initial_metadata.take();
        slot.buffered_bytes -= std::mem::take(&mut slot.initial_bytes);
        Ok(headers)
    }
    /// Transfers the next message in stream order and releases its byte charge.
    /// # Errors
    /// Rejects unknown streams or resets other than completed-response NO_ERROR.
    pub fn pop_message(&mut self, stream_id: i32) -> SlotResult<Option<StreamMessage>> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        let message = slot.messages.pop_front();
        // Retain the bounded descriptor capacity for reuse until stream removal.
        if let Some(message) = &message {
            slot.buffered_bytes -= message.payload.len() + 5 + size_of::<StreamMessage>();
        }
        Ok(message)
    }
    /// Transfers trailers exactly once without changing the remote half state.
    /// # Errors
    /// Rejects unknown streams or resets other than completed-response NO_ERROR.
    pub fn take_trailers(&mut self, stream_id: i32) -> SlotResult<Option<Headers>> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        let headers = slot.trailers.take();
        slot.buffered_bytes -= std::mem::take(&mut slot.trailer_bytes);
        Ok(headers)
    }
    /// Removes a reset or fully closed stream, including any unread events.
    /// # Errors
    /// Rejects unknown streams and streams with an open sending half.
    pub fn remove_closed_stream(&mut self, stream_id: i32) -> SlotResult<StreamSlot> {
        let Entry::Occupied(entry) = self.slots.entry(stream_id) else {
            return Err(SlotMapError::UnknownStream(stream_id));
        };
        let slot = entry.get();
        if !slot.state.matches(pattern!(StreamStateWord { HAS_RESET }))
            && !slot
                .state
                .matches(pattern!(StreamStateWord { REMOTE_END & LOCAL_END }))
        {
            return Err(SlotMapError::NotClosed(stream_id));
        }
        let removed = entry.remove();
        self.clear_occupied(stream_id);
        Ok(removed)
    }
    fn clear_occupied(&mut self, id: i32) {
        let key = SlotKey::new(id as u32);
        let block = key.get(SlotKey::BLOCK);
        if let Entry::Occupied(mut entry) = self.occupied.entry(block) {
            let bits = entry.get().clear(u32::from(key.get(SlotKey::BIT)));
            if bits.is_empty() {
                entry.remove();
            } else {
                *entry.get_mut() = bits;
            }
        }
    }
    #[inline]
    fn slot_mut(&mut self, id: i32) -> SlotResult<&mut StreamSlot> {
        let slot = self
            .slots
            .get_mut(&id)
            .ok_or(SlotMapError::UnknownStream(id))?;
        prefetch_read::<L1, _>(slot);
        Ok(slot)
    }
}
fn header_bytes(headers: &Headers) -> Option<usize> {
    headers.iter().try_fold(0_usize, |total, (key, value)| {
        total
            .checked_add(size_of::<(String, Vec<u8>)>())?
            .checked_add(key.len())?
            .checked_add(value.len())
    })
}
fn normalize_headers(headers: Headers) -> Headers {
    headers
        .into_iter()
        .map(|(key, value)| (key.into_boxed_str().into(), value.into_boxed_slice().into()))
        .collect::<Box<[_]>>()
        .into_vec()
}

uarch::layout::assert_layout!(StreamLimits, size = 16, align = 8);
uarch::layout::assert_layout!(StreamStateWord, size = 8, align = 8);

uarch::layout::assert_layout!(SlotKey, size = 4, align = 4);
