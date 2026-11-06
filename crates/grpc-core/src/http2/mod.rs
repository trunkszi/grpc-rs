//! Sans-IO streaming HTTP/2 with independent connection and stream flow control.
//!
//! Frame and HPACK codecs are pure Rust. Application consumption returns receive
//! credit; body bytes are limited by both peer windows. Zero-length probes can
//! discover EOF even while flow control prevents transmission of body bytes.
mod flags;
mod headers;
use flags::{FrameFlags, HttpStreamWord, SettingsFlags};
use uarch::prelude::*;
mod prefix;
mod receive;
use prefix::{FramePrefix, StreamId, WindowDelta};
mod send;
mod types;

use std::collections::{BTreeMap, VecDeque};

pub use nexora_proto::http2::{
    HpackDecoder, Http2FrameType as FrameType, Http2Role, Http2Settings,
};
use nexora_proto::{
    BytesMut,
    http2::{CLIENT_PREFACE, HpackEncoder},
};
pub use types::{DataPoll, DataProvider, DataSource, HeaderField, ProtocolError, SessionEvent};
use types::{Result, error};

struct Stream {
    send_window: i64,
    recv_window: i64,
    unconsumed: usize,
    credit: u32,
    provider: Option<DataProvider>,
    state: HttpStreamWord,
}
impl Stream {
    fn new(local: Http2Settings, peer: Http2Settings) -> Self {
        Self {
            send_window: peer.initial_window_size.into(),
            recv_window: local.initial_window_size.into(),
            unconsumed: 0,
            credit: 0,
            provider: None,
            state: HttpStreamWord::default(),
        }
    }
}
enum Outgoing {
    Frame {
        bytes: Vec<u8>,
        id: i32,
        kind: FrameType,
        flags: u8,
    },
    Headers {
        id: i32,
        block: Vec<u8>,
        end: bool,
    },
}
struct HeaderBlock {
    id: i32,
    end: bool,
    bytes: Vec<u8>,
    reject_code: Option<u32>,
}
/// A single-threaded, incremental HTTP/2 connection with no socket ownership.
pub struct Http2Session {
    role: Http2Role,
    local: Http2Settings,
    peer: Http2Settings,
    streams: BTreeMap<i32, Stream>,
    // Keep exhaustion beyond the last 31-bit ID instead of wrapping the cursor.
    next_id: i64,
    last_peer: i32,
    last_processed: i32,
    last_sent: i32,
    input: BytesMut,
    preface: usize,
    received_settings: bool,
    pending_ack: bool,
    encoder: HpackEncoder,
    decoder: HpackDecoder,
    headers: Option<HeaderBlock>,
    outgoing: VecDeque<Outgoing>,
    events: VecDeque<SessionEvent>,
    output: Vec<u8>,
    send_window: i64,
    recv_window: i64,
    credit: u32,
    sent_goaway: Option<i32>,
    received_goaway: Option<i32>,
    failed: Option<ProtocolError>,
}
impl Http2Session {
    /// Creates a peer and queues its connection preface and local settings.
    /// # Errors
    /// Rejects invalid frame and flow-control window sizes.
    pub fn new(role: Http2Role, local: Http2Settings) -> Result<Self> {
        if local.initial_window_size > i32::MAX as u32
            || !(16_384..=16_777_215).contains(&local.max_frame_size)
        {
            return Err(error(1, "invalid local settings"));
        }
        let mut session = Self {
            role,
            local,
            peer: Http2Settings::default(),
            streams: BTreeMap::new(),
            next_id: 1,
            last_peer: 0,
            last_processed: 0,
            last_sent: 0,
            input: BytesMut::new(),
            preface: if role == Http2Role::Client {
                CLIENT_PREFACE.len()
            } else {
                0
            },
            received_settings: false,
            pending_ack: true,
            encoder: HpackEncoder::default(),
            decoder: HpackDecoder::new(
                local.header_table_size as usize,
                local.max_header_list_size as usize,
            ),
            headers: None,
            outgoing: VecDeque::new(),
            events: VecDeque::new(),
            output: Vec::new(),
            send_window: 65_535,
            recv_window: 65_535,
            credit: 0,
            sent_goaway: None,
            received_goaway: None,
            failed: None,
        };
        if role == Http2Role::Client {
            session.outgoing.push_back(Outgoing::Frame {
                bytes: CLIENT_PREFACE.to_vec(),
                id: 0,
                kind: FrameType::Unknown(255),
                flags: 0,
            });
        }
        let mut settings = Vec::new();
        for (id, value) in [
            (1u16, local.header_table_size),
            (3, local.max_concurrent_streams),
            (4, local.initial_window_size),
            (5, local.max_frame_size),
            (6, local.max_header_list_size),
        ] {
            settings.extend_from_slice(&id.to_be_bytes());
            settings.extend_from_slice(&value.to_be_bytes());
        }
        if role == Http2Role::Client {
            settings.extend_from_slice(&[0, 2, 0, 0, 0, 0]);
        }
        session.queue_frame(0, FrameType::Settings, 0, &settings);
        Ok(session)
    }
    pub const fn role(&self) -> Http2Role {
        self.role
    }
    pub const fn last_proc_stream_id(&self) -> i32 {
        self.last_processed
    }
    pub fn next_event(&mut self) -> Option<SessionEvent> {
        self.events.pop_front()
    }
    pub fn want_read(&self) -> bool {
        self.failed.is_none()
            && (self.received_goaway.is_none()
                || self
                    .streams
                    .values()
                    .any(|s| !s.state.matches(pattern!(HttpStreamWord { CLOSE_EMITTED }))))
    }
    pub fn want_write(&self) -> bool {
        self.failed.is_none()
            && (!self.outgoing.is_empty()
                || self.streams.values().any(|s| {
                    s.provider.is_some()
                        && s.state.matches(
                            pattern!(HttpStreamWord { !DEFERRED & !RESET & !LOCAL_CLOSED }),
                        )
                        && (!s.state.matches(pattern!(HttpStreamWord { WINDOW_BLOCKED }))
                            || (self.send_window > 0 && s.send_window > 0))
                }))
    }
    /// Releases a closed stream's protocol bookkeeping.
    /// # Errors
    /// Rejects removal before the closure event has been emitted.
    pub fn release_stream(&mut self, id: i32) -> Result<()> {
        let std::collections::btree_map::Entry::Occupied(entry) = self.streams.entry(id) else {
            return Err(error(1, "stream still open"));
        };
        if !entry
            .get()
            .state
            .matches(pattern!(HttpStreamWord { CLOSE_EMITTED }))
        {
            return Err(error(1, "stream still open"));
        }
        // Discarded DATA still consumed connection credit, including after a reset.
        // No stream-level credit is needed after protocol closure.
        let unconsumed = entry.remove().unconsumed;
        self.credit_connection(unconsumed)
    }
    fn check_live(&self) -> Result<()> {
        self.failed.clone().map_or(Ok(()), Err)
    }
    fn close_if_complete(&mut self, id: i32) {
        if let Some(stream) = self.streams.get_mut(&id)
            && stream
                .state
                .matches(pattern!(HttpStreamWord { LOCAL_CLOSED & REMOTE_CLOSED & !CLOSE_EMITTED }))
        {
            stream.state = stream.state.patch(patch!(HttpStreamWord {
                CLOSE_EMITTED: true
            }));
            stream.provider = None;
            self.events.push_back(SessionEvent::StreamClose {
                stream_id: id,
                error_code: 0,
            });
        }
    }
    fn stream_failure(&mut self, id: i32, resource: bool, message: impl Into<String>) {
        self.events.push_back(SessionEvent::StreamError {
            stream_id: id,
            resource,
            message: message.into(),
        });
    }
}

#[cfg(test)]
#[path = "../../tests/unit/http2_boundary.rs"]
mod boundary_tests;
