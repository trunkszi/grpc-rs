//! Incremental body sources and wire events shared by HTTP/2 peers.
use super::FrameType;

/// Borrowed HTTP/2 header bytes.
#[derive(Clone, Copy)]
pub struct HeaderField<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}
impl<'a> HeaderField<'a> {
    pub const fn new(name: &'a [u8], value: &'a [u8]) -> Self {
        Self { name, value }
    }
}
/// One bounded read from an application-owned response or request body.
pub enum DataPoll<'a> {
    Copy(&'a [u8]),
    Deferred,
    Eof,
    EofWithTrailers,
}
/// A body producer resumes after the application makes more data available.
pub trait DataSource {
    fn read(&mut self, max_len: usize) -> DataPoll<'_>;
}
/// Owned incremental body source; callbacks execute on the session thread.
pub struct DataProvider(pub(super) Box<dyn DataSource>);
impl DataProvider {
    pub fn new(source: impl DataSource + 'static) -> Self {
        Self(Box::new(source))
    }
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::new(VectorSource { bytes, offset: 0 })
    }
}
struct VectorSource {
    bytes: Vec<u8>,
    offset: usize,
}
impl DataSource for VectorSource {
    fn read(&mut self, max_len: usize) -> DataPoll<'_> {
        if self.offset == self.bytes.len() {
            return DataPoll::Eof;
        }
        let start = self.offset;
        self.offset += max_len.min(self.bytes.len() - start);
        DataPoll::Copy(&self.bytes[start..self.offset])
    }
}
/// Owned events retain their payload until the application dispatches them.
#[derive(Debug)]
pub enum SessionEvent {
    BeginHeaders {
        stream_id: i32,
    },
    Header {
        stream_id: i32,
        name: Vec<u8>,
        value: Vec<u8>,
    },
    DataChunk {
        stream_id: i32,
        data: Vec<u8>,
    },
    FrameRecv {
        stream_id: i32,
        frame_type: FrameType,
        flags: u8,
    },
    FrameSend {
        stream_id: i32,
        frame_type: FrameType,
        flags: u8,
    },
    StreamClose {
        stream_id: i32,
        error_code: u32,
    },
    GoAway {
        last_stream_id: i32,
        error_code: u32,
    },
    StreamError {
        stream_id: i32,
        resource: bool,
        message: String,
    },
    Error {
        code: u32,
        message: Vec<u8>,
    },
}
/// A connection-level framing, compression or lifecycle failure.
#[derive(Clone, Debug, thiserror::Error)]
#[error("HTTP/2 error {code}: {message}")]
pub struct ProtocolError {
    pub code: u32,
    pub message: String,
}
pub(super) fn error(code: u32, message: impl Into<String>) -> ProtocolError {
    ProtocolError {
        code,
        message: message.into(),
    }
}
pub(super) type Result<T> = std::result::Result<T, ProtocolError>;
