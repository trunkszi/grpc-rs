//! Sans-IO streaming HTTP/2 with independent connection and stream flow control.
//!
//! Frame and HPACK codecs are pure Rust. Application consumption returns receive
//! credit; body producers run only when both peer windows permit transmission.
mod headers;
mod receive;
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
    deferred: bool,
    initial: bool,
    local_closed: bool,
    remote_closed: bool,
    close_emitted: bool,
    reset: bool,
}
impl Stream {
    fn new(local: Http2Settings, peer: Http2Settings) -> Self {
        Self {
            send_window: peer.initial_window_size.into(),
            recv_window: local.initial_window_size.into(),
            unconsumed: 0,
            credit: 0,
            provider: None,
            deferred: false,
            initial: false,
            local_closed: false,
            remote_closed: false,
            close_emitted: false,
            reset: false,
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
    next_id: i32,
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
            && (self.received_goaway.is_none() || self.streams.values().any(|s| !s.close_emitted))
    }
    pub fn want_write(&self) -> bool {
        !self.outgoing.is_empty()
            || (self.send_window > 0
                && self
                    .streams
                    .values()
                    .any(|s| s.provider.is_some() && !s.deferred && !s.reset && s.send_window > 0))
    }
    /// Releases a closed stream's protocol bookkeeping.
    /// # Errors
    /// Rejects removal before the closure event has been emitted.
    pub fn release_stream(&mut self, id: i32) -> Result<()> {
        if !self.streams.get(&id).is_some_and(|s| s.close_emitted) {
            return Err(error(1, "stream still open"));
        }
        self.streams.remove(&id);
        Ok(())
    }
    fn check_live(&self) -> Result<()> {
        self.failed.clone().map_or(Ok(()), Err)
    }
    fn close_if_complete(&mut self, id: i32) {
        if let Some(stream) = self.streams.get_mut(&id)
            && stream.local_closed
            && stream.remote_closed
            && !stream.close_emitted
        {
            stream.close_emitted = true;
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
