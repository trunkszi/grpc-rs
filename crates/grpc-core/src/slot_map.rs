//! Bounded, single-threaded ownership of HTTP/2 stream receive events.
//!
//! The byte budget counts retained metadata entries, message descriptors and wire
//! bytes, including incomplete five-byte gRPC envelopes. Container bookkeeping
//! and allocator overhead are excluded. Spare capacity is bounded by admitted bytes and queue
//! counts; queues shrink at quarter occupancy. Owned buffers are normalized. Rejected inputs
//! preserve all previously accepted events and partial frames.
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    mem::size_of,
};

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SlotMapError {
    /// HTTP/2 stream identifiers must be positive signed 31-bit integers.
    InvalidStreamId(i32),
    /// The identifier already has a slot.
    AlreadyOpen(i32),
    /// The identifier has no slot.
    UnknownStream(i32),
    /// The peer's GOAWAY excludes this stream.
    GoAway {
        stream_id: i32,
        last_stream_id: i32,
        error_code: u32,
    },
    /// The peer reset the stream; buffered events must not mask the reset.
    Reset { stream_id: i32, error_code: u32 },
    /// The event violates the receive or half-close lifecycle.
    InvalidTransition {
        stream_id: i32,
        operation: &'static str,
    },
    /// The stream would exceed its retained byte budget.
    MemoryLimit { stream_id: i32, limit: usize },
    /// The stream would exceed its queued message count.
    MessageLimit { stream_id: i32, limit: usize },
    /// A gRPC envelope compression flag was neither zero nor one.
    InvalidCompressionFlag { stream_id: i32, flag: u8 },
    /// Trailers or END_STREAM arrived in an incomplete envelope.
    TruncatedFrame(i32),
    /// Both halves must close, or a reset must arrive, before removal.
    NotClosed(i32),
}
impl fmt::Display for SlotMapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStreamId(id) => write!(f, "invalid stream identifier {id}"),
            Self::AlreadyOpen(id) => write!(f, "stream {id} is already open"),
            Self::UnknownStream(id) => write!(f, "unknown stream {id}"),
            Self::GoAway {
                stream_id,
                last_stream_id,
                error_code,
            } => write!(
                f,
                "GOAWAY: stream={stream_id}, last={last_stream_id}, code={error_code}"
            ),
            Self::Reset {
                stream_id,
                error_code,
            } => write!(f, "stream {stream_id} reset: code={error_code}"),
            Self::InvalidTransition {
                stream_id,
                operation,
            } => write!(f, "stream {stream_id}: cannot {operation}"),
            Self::MemoryLimit { stream_id, limit } => {
                write!(f, "stream {stream_id}: byte limit {limit}")
            }
            Self::MessageLimit { stream_id, limit } => {
                write!(f, "stream {stream_id}: message limit {limit}")
            }
            Self::InvalidCompressionFlag { stream_id, flag } => {
                write!(f, "stream {stream_id}: invalid compression flag {flag}")
            }
            Self::TruncatedFrame(id) => write!(f, "stream {id}: truncated envelope"),
            Self::NotClosed(id) => write!(f, "stream {id} is not closed"),
        }
    }
}
impl std::error::Error for SlotMapError {}
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

/// Receive queues and independent local/remote lifecycle state for one stream.
#[derive(Debug, Default)]
pub struct StreamSlot {
    initial_metadata: Option<Headers>,
    initial_seen: bool,
    initial_bytes: usize,
    messages: VecDeque<StreamMessage>,
    trailers: Option<Headers>,
    trailers_seen: bool,
    trailer_bytes: usize,
    partial: Vec<u8>,
    buffered_bytes: usize,
    remote_end: bool,
    local_end: bool,
    reset: Option<u32>,
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
        self.remote_end
    }
    /// Whether the local endpoint has closed its sending half.
    #[must_use]
    pub const fn local_end_stream(&self) -> bool {
        self.local_end
    }
    /// The HTTP/2 RST_STREAM error code, if received.
    #[must_use]
    pub const fn reset_error_code(&self) -> Option<u32> {
        self.reset
    }
    fn check_reset(&self, stream_id: i32) -> Result<(), SlotMapError> {
        match self.reset {
            Some(error_code) => Err(SlotMapError::Reset {
                stream_id,
                error_code,
            }),
            None => Ok(()),
        }
    }
    fn receive(&self, stream_id: i32, operation: &'static str) -> Result<(), SlotMapError> {
        self.check_reset(stream_id)?;
        if self.remote_end || self.trailers_seen {
            return Err(transition(stream_id, operation));
        }
        Ok(())
    }
    fn budget(&self, id: i32, bytes: usize, limits: StreamLimits) -> Result<usize, SlotMapError> {
        self.buffered_bytes
            .checked_add(bytes)
            .filter(|total| *total <= limits.max_buffered_bytes)
            .ok_or(memory_limit(id, limits))
    }
}

/// A synchronous stream demultiplexer with explicit ownership transfer on reads.
#[derive(Debug)]
pub struct StreamSlotMap {
    slots: BTreeMap<i32, StreamSlot>,
    limits: StreamLimits,
    goaway: Option<(i32, u32)>,
}
impl Default for StreamSlotMap {
    fn default() -> Self {
        Self::new(StreamLimits::default())
    }
}
impl StreamSlotMap {
    /// Creates an empty map. Zero limits intentionally disable buffering.
    #[must_use]
    pub const fn new(limits: StreamLimits) -> Self {
        Self {
            slots: BTreeMap::new(),
            limits,
            goaway: None,
        }
    }
    /// Opens a stream without allocating receive buffers.
    /// # Errors
    /// Rejects invalid, existing, or GOAWAY-excluded identifiers.
    pub fn open_stream(&mut self, stream_id: i32) -> Result<(), SlotMapError> {
        if stream_id <= 0 {
            return Err(SlotMapError::InvalidStreamId(stream_id));
        }
        if let Some((last_stream_id, error_code)) = self.goaway
            && stream_id > last_stream_id
        {
            return Err(SlotMapError::GoAway {
                stream_id,
                last_stream_id,
                error_code,
            });
        }
        if self.slots.contains_key(&stream_id) {
            return Err(SlotMapError::AlreadyOpen(stream_id));
        }
        self.slots.insert(stream_id, StreamSlot::default());
        Ok(())
    }
    /// Looks up a stream without consuming events.
    #[must_use]
    pub fn get(&self, stream_id: i32) -> Option<&StreamSlot> {
        self.slots.get(&stream_id)
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
    pub fn push_initial_metadata(
        &mut self,
        stream_id: i32,
        headers: Headers,
    ) -> Result<(), SlotMapError> {
        self.push_headers(stream_id, headers, false)
    }
    /// Accepts arbitrary DATA fragmentation and queues complete gRPC envelopes.
    /// # Errors
    /// Rejects unknown/reset/ended streams, DATA before headers, malformed flags,
    /// oversized declared messages, or either resource limit. Every rejection
    /// leaves prior partial bytes and queued events unchanged.
    pub fn append_recv_bytes(&mut self, stream_id: i32, bytes: &[u8]) -> Result<(), SlotMapError> {
        let limits = self.limits;
        let slot = self.slot_mut(stream_id)?;
        slot.receive(stream_id, "receive DATA")?;
        if !slot.initial_seen {
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
            let payload_len = u32::from_be_bytes([
                at(consumed + 1),
                at(consumed + 2),
                at(consumed + 3),
                at(consumed + 4),
            ]) as usize;
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
            let payload_len = u32::from_be_bytes([
                combined[offset + 1],
                combined[offset + 2],
                combined[offset + 3],
                combined[offset + 4],
            ]) as usize;
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
    /// Rejects unknown/reset/ended streams, missing headers, partial wire frames,
    /// and message or byte limit excess.
    pub fn push_message(
        &mut self,
        stream_id: i32,
        message: StreamMessage,
    ) -> Result<(), SlotMapError> {
        let limits = self.limits;
        let slot = self.slot_mut(stream_id)?;
        slot.receive(stream_id, "receive a message")?;
        if !slot.initial_seen || !slot.partial.is_empty() {
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
    /// Stores trailers, including a Trailers-Only response without initial headers.
    /// The caller separately marks the END_STREAM carried by the header block.
    /// # Errors
    /// Rejects unknown/reset/ended streams, duplicate trailers, partial frames,
    /// and budget excess.
    pub fn push_trailing_metadata(
        &mut self,
        stream_id: i32,
        headers: Headers,
    ) -> Result<(), SlotMapError> {
        self.push_headers(stream_id, headers, true)
    }
    fn push_headers(
        &mut self,
        stream_id: i32,
        headers: Headers,
        trailing: bool,
    ) -> Result<(), SlotMapError> {
        let limits = self.limits;
        let slot = self.slot_mut(stream_id)?;
        slot.receive(stream_id, "receive metadata")?;
        if !trailing && slot.initial_seen {
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
            slot.trailers_seen = true;
        } else {
            slot.initial_bytes = additional;
            slot.initial_metadata = Some(normalize_headers(headers));
            slot.initial_seen = true;
        }
        slot.buffered_bytes = total;
        Ok(())
    }
    /// Closes the peer's sending half; request streams need not carry trailers.
    /// # Errors
    /// Rejects unknown/reset streams, duplicate END_STREAM and truncated envelopes.
    pub fn mark_remote_end_stream(&mut self, stream_id: i32) -> Result<(), SlotMapError> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        if slot.remote_end {
            return Err(transition(stream_id, "receive duplicate END_STREAM"));
        }
        if !slot.partial.is_empty() {
            return Err(SlotMapError::TruncatedFrame(stream_id));
        }
        slot.remote_end = true;
        Ok(())
    }
    /// Closes the local sending half independently of the receive queues.
    /// # Errors
    /// Rejects unknown/reset streams or duplicate local END_STREAM.
    pub fn mark_local_end_stream(&mut self, stream_id: i32) -> Result<(), SlotMapError> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        if slot.local_end {
            return Err(transition(stream_id, "send duplicate END_STREAM"));
        }
        slot.local_end = true;
        Ok(())
    }
    /// Records a reset, releases all buffered events and closes both halves.
    /// # Errors
    /// Rejects unknown streams and repeated resets, preserving the first code.
    pub fn record_rst_stream(
        &mut self,
        stream_id: i32,
        error_code: u32,
    ) -> Result<(), SlotMapError> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        *slot = StreamSlot {
            reset: Some(error_code),
            remote_end: true,
            local_end: true,
            ..StreamSlot::default()
        };
        Ok(())
    }
    /// Prunes excluded streams and returns their identifiers in ascending order.
    /// Repeated GOAWAY frames can only reduce the accepted stream boundary.
    /// # Errors
    /// Rejects negative boundaries and increases to an established boundary.
    pub fn apply_goaway(
        &mut self,
        last_stream_id: i32,
        error_code: u32,
    ) -> Result<Vec<i32>, SlotMapError> {
        if last_stream_id < 0 {
            return Err(SlotMapError::InvalidStreamId(last_stream_id));
        }
        if self
            .goaway
            .is_some_and(|(previous, _)| last_stream_id > previous)
        {
            return Err(transition(last_stream_id, "increase GOAWAY boundary"));
        }
        let removed = if last_stream_id == i32::MAX {
            BTreeMap::new()
        } else {
            self.slots.split_off(&(last_stream_id + 1))
        };
        self.goaway = Some((last_stream_id, error_code));
        Ok(removed.into_keys().collect())
    }
    /// Transfers initial metadata ownership, leaving lifecycle history intact.
    /// # Errors
    /// Rejects unknown or reset streams, including resets with empty queues.
    pub fn pop_initial_metadata(
        &mut self,
        stream_id: i32,
    ) -> Result<Option<Headers>, SlotMapError> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        let headers = slot.initial_metadata.take();
        slot.buffered_bytes -= std::mem::take(&mut slot.initial_bytes);
        Ok(headers)
    }
    /// Transfers the next message in stream order and releases its byte charge.
    /// # Errors
    /// Rejects unknown or reset streams.
    pub fn pop_message(&mut self, stream_id: i32) -> Result<Option<StreamMessage>, SlotMapError> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        let message = slot.messages.pop_front();
        if slot.messages.len() <= slot.messages.capacity() / 4 {
            slot.messages.shrink_to_fit();
        }
        if let Some(message) = &message {
            slot.buffered_bytes -= message.payload.len() + 5 + size_of::<StreamMessage>();
        }
        Ok(message)
    }
    /// Transfers trailers exactly once without changing the remote half state.
    /// # Errors
    /// Rejects unknown or reset streams.
    pub fn take_trailers(&mut self, stream_id: i32) -> Result<Option<Headers>, SlotMapError> {
        let slot = self.slot_mut(stream_id)?;
        slot.check_reset(stream_id)?;
        let headers = slot.trailers.take();
        slot.buffered_bytes -= std::mem::take(&mut slot.trailer_bytes);
        Ok(headers)
    }
    /// Removes a reset or fully closed stream, including any unread events.
    /// # Errors
    /// Rejects unknown streams and streams with an open sending half.
    pub fn remove_closed_stream(&mut self, stream_id: i32) -> Result<StreamSlot, SlotMapError> {
        let slot = self
            .slots
            .get(&stream_id)
            .ok_or(SlotMapError::UnknownStream(stream_id))?;
        if slot.reset.is_none() && !(slot.remote_end && slot.local_end) {
            return Err(SlotMapError::NotClosed(stream_id));
        }
        self.slots
            .remove(&stream_id)
            .ok_or(SlotMapError::UnknownStream(stream_id))
    }
    fn slot_mut(&mut self, id: i32) -> Result<&mut StreamSlot, SlotMapError> {
        self.slots
            .get_mut(&id)
            .ok_or(SlotMapError::UnknownStream(id))
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
        .map(|(key, value)| {
            (
                key.into_boxed_str().into_string(),
                value.into_boxed_slice().into_vec(),
            )
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
        .into_vec()
}
#[cfg(test)]
mod tests {
    use super::*;
    fn headers(name: &str, value: &[u8]) -> Headers {
        vec![(name.to_owned(), value.to_vec())]
    }
    fn message(payload: &[u8]) -> StreamMessage {
        StreamMessage {
            compressed: false,
            payload: payload.to_vec(),
        }
    }
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut wire = vec![0];
        let len = u32::try_from(payload.len()).expect("test length");
        wire.extend(len.to_be_bytes());
        wire.extend(payload);
        wire
    }
    fn bounded(bytes: usize, messages: usize) -> StreamSlotMap {
        StreamSlotMap::new(StreamLimits {
            max_buffered_bytes: bytes,
            max_queued_messages: messages,
        })
    }
    fn open(map: &mut StreamSlotMap, id: i32) -> Result<(), SlotMapError> {
        map.open_stream(id)?;
        map.push_initial_metadata(id, vec![])
    }
    #[test]
    fn interleaved_streams_preserve_all_events() -> Result<(), SlotMapError> {
        let mut map = StreamSlotMap::default();
        let initial = headers(":status", b"200");
        for id in [1, 3] {
            map.open_stream(id)?;
            map.push_initial_metadata(id, initial.clone())?;
        }
        map.push_message(3, message(b"three-a"))?;
        map.push_message(1, message(b"one"))?;
        map.push_message(3, message(b"three-b"))?;
        for id in [1, 3] {
            map.push_trailing_metadata(id, headers("grpc-status", b"0"))?;
            map.mark_remote_end_stream(id)?;
        }
        assert_eq!(map.pop_message(1)?, Some(message(b"one")));
        for payload in [b"three-a", b"three-b"] {
            assert_eq!(map.pop_message(3)?, Some(message(payload)));
        }
        for id in [1, 3] {
            assert_eq!(map.pop_initial_metadata(id)?, Some(initial.clone()));
            assert_eq!(map.take_trailers(id)?, Some(headers("grpc-status", b"0")));
            assert_eq!(map.pop_message(id)?, None);
        }
        Ok(())
    }
    #[test]
    fn trailers_only_and_duplicate_metadata() -> Result<(), SlotMapError> {
        let mut map = StreamSlotMap::default();
        map.open_stream(1)?;
        map.push_trailing_metadata(1, headers("grpc-status", b"12"))?;
        map.mark_remote_end_stream(1)?;
        assert_eq!(map.pop_initial_metadata(1)?, None);
        assert_eq!(map.pop_message(1)?, None);
        assert_eq!(map.take_trailers(1)?, Some(headers("grpc-status", b"12")));
        assert_eq!(map.take_trailers(1)?, None);
        open(&mut map, 3)?;
        map.pop_initial_metadata(3)?;
        assert!(map.push_initial_metadata(3, vec![]).is_err());
        map.push_trailing_metadata(3, vec![])?;
        map.take_trailers(3)?;
        assert!(map.push_trailing_metadata(3, vec![]).is_err());
        Ok(())
    }
    #[test]
    fn goaway_prunes_and_enforces_decreasing_boundary() -> Result<(), SlotMapError> {
        let mut map = StreamSlotMap::default();
        for id in [1, 3, 5, 7, i32::MAX] {
            open(&mut map, id)?;
        }
        map.push_message(1, message(b"retained"))?;
        assert!(map.apply_goaway(i32::MAX, 0)?.is_empty());
        assert_eq!(map.apply_goaway(3, 0)?, vec![5, 7, i32::MAX]);
        assert_eq!(map.active_stream_count(), 2);
        assert!(map.get(5).is_none());
        assert!(map.open_stream(5).is_err());
        assert_eq!(map.pop_message(1)?, Some(message(b"retained")));
        assert_eq!(map.apply_goaway(1, 0)?, vec![3]);
        assert!(map.apply_goaway(7, 0).is_err());
        assert!(map.apply_goaway(-1, 0).is_err());
        assert_eq!(map.goaway(), Some((1, 0)));
        assert_eq!(map.apply_goaway(0, 7)?, vec![1]);
        assert_eq!(map.active_stream_count(), 0);
        assert!(matches!(
            map.open_stream(1),
            Err(SlotMapError::GoAway { error_code: 7, .. })
        ));
        Ok(())
    }
    #[test]
    fn reset_and_half_close_lifecycle() -> Result<(), SlotMapError> {
        let mut map = StreamSlotMap::default();
        for id in [0, -1] {
            assert!(map.open_stream(id).is_err());
        }
        assert!(map.push_message(99, message(b"unknown")).is_err());
        open(&mut map, 1)?;
        assert!(map.open_stream(1).is_err());
        assert!(map.remove_closed_stream(1).is_err());
        map.mark_local_end_stream(1)?;
        map.push_message(1, message(b"half-open"))?;
        assert!(map.mark_local_end_stream(1).is_err());
        assert!(map.remove_closed_stream(1).is_err());
        map.mark_remote_end_stream(1)?;
        assert!(map.push_message(1, message(b"late")).is_err());
        map.remove_closed_stream(1)?;
        open(&mut map, 3)?;
        map.push_message(3, message(b"discarded"))?;
        map.append_recv_bytes(3, &[0, 0])?;
        map.record_rst_stream(3, 8)?;
        assert_eq!(map.get(3).expect("slot").buffered_bytes(), 0);
        assert!(map.pop_initial_metadata(3).is_err());
        assert!(map.take_trailers(3).is_err());
        assert!(map.record_rst_stream(3, 9).is_err());
        assert_eq!(map.get(3).expect("slot").reset_error_code(), Some(8));
        assert!(matches!(
            map.pop_message(3),
            Err(SlotMapError::Reset { error_code: 8, .. })
        ));
        assert!(map.push_message(3, message(b"late")).is_err());
        map.remove_closed_stream(3)?;
        assert_eq!(map.active_stream_count(), 0);
        Ok(())
    }
    #[test]
    fn fragmented_frames_compression_and_invalid_prefixes() -> Result<(), SlotMapError> {
        let mut map = StreamSlotMap::default();
        open(&mut map, 1)?;
        let mut wire = frame(b"abc");
        wire.extend(frame(b""));
        for byte in wire {
            map.append_recv_bytes(1, &[byte])?;
        }
        for payload in [b"abc".as_slice(), b"".as_slice()] {
            assert_eq!(map.pop_message(1)?, Some(message(payload)));
        }
        let mut compressed = frame(b"xyz");
        compressed[0] = 1;
        map.append_recv_bytes(1, &compressed)?;
        assert!(map.pop_message(1)?.expect("message").compressed);
        assert!(map.append_recv_bytes(1, &[2, 0, 0, 0, 0]).is_err());
        Ok(())
    }
    #[test]
    fn rejected_batches_preserve_partial_frames() -> Result<(), SlotMapError> {
        let mut map = bounded(128, 1);
        open(&mut map, 1)?;
        let wire = frame(b"ok");
        map.append_recv_bytes(1, &wire[..2])?;
        let mut queue_excess = wire[2..].to_vec();
        queue_excess.extend(frame(b""));
        let mut malformed = wire[2..].to_vec();
        malformed.push(2);
        for rejected in [queue_excess, malformed, vec![0; 128]] {
            assert!(map.append_recv_bytes(1, &rejected).is_err());
            let slot = map.get(1).expect("slot");
            assert_eq!(slot.buffered_bytes(), 2);
            assert_eq!(slot.queued_message_count(), 0);
        }
        map.append_recv_bytes(1, &wire[2..])?;
        assert_eq!(map.pop_message(1)?, Some(message(b"ok")));
        assert_eq!(map.get(1).expect("slot").buffered_bytes(), 0);
        Ok(())
    }
    #[test]
    fn truncated_frames_and_declared_lengths_are_bounded() -> Result<(), SlotMapError> {
        for wire in [&[0, 0][..], &[0, 0, 0, 0, 3, b'a'][..]] {
            let mut map = bounded(128, 4);
            open(&mut map, 1)?;
            map.append_recv_bytes(1, wire)?;
            assert!(map.mark_remote_end_stream(1).is_err());
        }
        let mut map = bounded(8, 4);
        open(&mut map, 1)?;
        let oversized = [0, 0xff, 0xff, 0xff, 0xff];
        assert!(map.append_recv_bytes(1, &oversized).is_err());
        map.append_recv_bytes(1, &[0, 0, 0])?;
        assert_eq!(map.get(1).expect("slot").buffered_bytes(), 3);
        assert!(map.push_message(1, message(b"123456")).is_err());
        Ok(())
    }
    #[test]
    fn byte_and_queue_budgets_release_on_consumption() -> Result<(), SlotMapError> {
        let mut map = bounded(128, 1);
        map.open_stream(1)?;
        map.push_initial_metadata(1, headers("key", b"value"))?;
        assert!(map.push_message(1, message(&[0; 128])).is_err());
        map.pop_initial_metadata(1)?;
        assert_eq!(map.get(1).expect("slot").buffered_bytes(), 0);
        map.push_message(1, message(&[0; 64]))?;
        assert!(map.push_trailing_metadata(1, headers("x", b"y")).is_err());
        map.pop_message(1)?;
        map.push_message(1, message(b""))?;
        assert!(map.push_message(1, message(b"")).is_err());
        assert_eq!(map.get(1).expect("slot").queued_message_count(), 1);
        assert!(map.append_recv_bytes(1, &frame(b"blocked")).is_err());
        assert_eq!(map.pop_message(1)?, Some(message(b"")));
        map.append_recv_bytes(1, &frame(b"next"))?;
        assert_eq!(map.pop_message(1)?, Some(message(b"next")));
        map.push_trailing_metadata(1, headers("x", b"y"))?;
        map.take_trailers(1)?;
        assert_eq!(map.get(1).expect("slot").buffered_bytes(), 0);
        Ok(())
    }
}
