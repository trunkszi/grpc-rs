//! Incremental framing, peer settings and receive-credit ownership.
use nexora_proto::{
    ProtoError,
    http2::{Http2Frame, decode_frame},
};

use super::*;

impl Http2Session {
    /// Feeds wire bytes, retaining incomplete prefaces, frames and header blocks.
    /// # Errors
    /// Connection errors are sticky; malformed streams produce stream-local events.
    pub fn mem_recv(&mut self, bytes: &[u8]) -> Result<usize> {
        self.check_live()?;
        self.input.extend_from_slice(bytes);
        if let Err(failure) = self.receive() {
            self.failed = Some(failure.clone());
            return Err(failure);
        }
        Ok(bytes.len())
    }
    fn receive(&mut self) -> Result<()> {
        while self.preface < CLIENT_PREFACE.len() && !self.input.is_empty() {
            if self.input.as_slice()[0] != CLIENT_PREFACE[self.preface] {
                return Err(error(1, "invalid client preface"));
            }
            self.preface += 1;
            self.input.advance(1);
        }
        if self.preface < CLIENT_PREFACE.len() {
            return Ok(());
        }
        loop {
            let raw = self.input.as_slice();
            if raw.len() < 9 {
                break;
            }
            let length =
                (usize::from(raw[0]) << 16) | (usize::from(raw[1]) << 8) | usize::from(raw[2]);
            let kind = raw[3];
            let flags = raw[4];
            let id = (u32::from_be_bytes(raw[5..9].try_into().expect("frame header")) & 0x7fff_ffff)
                as i32;
            if !self.received_settings && (kind != 4 || flags & 1 != 0) {
                return Err(error(1, "first frame must be non-ACK SETTINGS"));
            }
            if self
                .headers
                .as_ref()
                .is_some_and(|h| kind != 9 || id != h.id)
            {
                return Err(error(1, "interleaved header continuation"));
            }
            if length > self.local.max_frame_size as usize {
                return Err(error(6, "frame exceeds local maximum"));
            }
            if raw.len() < 9 + length {
                break;
            }
            let settings = if kind == 4 && flags & 1 == 0 {
                Some(raw[9..9 + length].to_vec())
            } else {
                None
            };
            let reset = if kind == 3 && length == 4 {
                Some(u32::from_be_bytes(
                    raw[9..13].try_into().expect("reset code"),
                ))
            } else {
                None
            };
            let goaway = if kind == 7 && length >= 8 {
                Some(u32::from_be_bytes(
                    raw[13..17].try_into().expect("shutdown code"),
                ))
            } else {
                None
            };
            let frame = match decode_frame(&mut self.input, self.local.max_frame_size) {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(ProtoError::Http2StreamError {
                    stream_id,
                    code,
                    reason,
                }) => {
                    self.input.advance(9 + length);
                    self.stream_failure(stream_id as i32, code.as_u32() == 11, reason);
                    continue;
                }
                Err(ProtoError::Http2ConnectionError { code, reason }) => {
                    return Err(error(code.as_u32(), reason));
                }
                Err(failure) => return Err(error(1, failure.to_string())),
            };
            match frame {
                Http2Frame::Settings { ack, .. } => {
                    if ack {
                        if !self.pending_ack {
                            return Err(error(1, "unexpected SETTINGS acknowledgement"));
                        }
                        self.pending_ack = false;
                    } else {
                        self.apply_settings(settings.as_deref().expect("settings payload"))?;
                        self.received_settings = true;
                        self.queue_frame(0, FrameType::Settings, 1, &[]);
                    }
                }
                Http2Frame::Headers {
                    stream_id,
                    end_stream,
                    end_headers,
                    header_block,
                } => {
                    self.begin_headers(
                        stream_id as i32,
                        end_stream,
                        header_block.as_slice(),
                        end_headers,
                    )?;
                }
                Http2Frame::Continuation {
                    stream_id,
                    end_headers,
                    header_block,
                } => {
                    let Some(block) = &mut self.headers else {
                        return Err(error(1, "unexpected continuation"));
                    };
                    if block.id != stream_id as i32 {
                        return Err(error(1, "wrong continuation stream"));
                    }
                    block.bytes.extend_from_slice(header_block.as_slice());
                    self.check_header_block_size()?;
                    if end_headers {
                        self.finish_headers()?;
                    }
                }
                Http2Frame::Data {
                    stream_id,
                    end_stream,
                    data,
                } => {
                    self.receive_data(stream_id as i32, end_stream, data.as_slice(), length)?;
                }
                Http2Frame::WindowUpdate {
                    stream_id,
                    increment,
                } => {
                    let window = if stream_id == 0 {
                        &mut self.send_window
                    } else if let Some(stream) = self.streams.get_mut(&(stream_id as i32)) {
                        if stream.close_emitted {
                            continue;
                        }
                        &mut stream.send_window
                    } else if self.idle(stream_id as i32) {
                        return Err(error(1, "window update on idle stream"));
                    } else {
                        continue;
                    };
                    if *window + i64::from(increment) > i64::from(i32::MAX) {
                        if stream_id == 0 {
                            return Err(error(3, "connection send window overflow"));
                        }
                        self.stream_failure(stream_id as i32, false, "stream send window overflow");
                        continue;
                    }
                    *window += i64::from(increment);
                }
                Http2Frame::RstStream { stream_id, .. } => {
                    let id = stream_id as i32;
                    if self.idle(id) {
                        return Err(error(1, "reset on idle stream"));
                    }
                    if let Some(stream) = self.streams.get_mut(&id)
                        && !stream.close_emitted
                    {
                        stream.close_emitted = true;
                        stream.reset = true;
                        stream.provider = None;
                        self.events.push_back(SessionEvent::StreamClose {
                            stream_id: id,
                            error_code: reset.expect("reset payload"),
                        });
                    }
                }
                Http2Frame::GoAway { last_stream_id, .. } => {
                    let last = self
                        .received_goaway
                        .map_or(last_stream_id as i32, |old| old.min(last_stream_id as i32));
                    self.received_goaway = Some(last);
                    self.events.push_back(SessionEvent::GoAway {
                        last_stream_id: last,
                        error_code: goaway.expect("shutdown payload"),
                    });
                    for (&id, stream) in &mut self.streams {
                        if self.role == Http2Role::Client && id > last && !stream.close_emitted {
                            stream.reset = true;
                            stream.close_emitted = true;
                            stream.provider = None;
                            self.events.push_back(SessionEvent::StreamClose {
                                stream_id: id,
                                error_code: 7,
                            });
                        }
                    }
                }
                Http2Frame::Ping {
                    ack: false,
                    opaque_data,
                } => self.queue_frame(0, FrameType::Ping, 1, &opaque_data),
                Http2Frame::Unknown { header, .. }
                    if header.frame_type == FrameType::PushPromise =>
                {
                    return Err(error(1, "server push is disabled"));
                }
                _ => {}
            }
        }
        // A fully drained connection can close without emitting queued control acknowledgements.
        if self.received_goaway.is_some() && self.streams.values().all(|s| s.close_emitted) {
            self.outgoing.retain(|item| {
                !matches!(
                    item,
                    Outgoing::Frame {
                        id: 0,
                        kind: FrameType::Settings,
                        flags: 1,
                        ..
                    }
                )
            });
        }
        Ok(())
    }
    fn idle(&self, id: i32) -> bool {
        if id <= 0 {
            return true;
        }
        if id & 1 == 0 {
            return true;
        }
        if self.role == Http2Role::Client {
            id >= self.next_id
        } else {
            id > self.last_peer
        }
    }
    fn apply_settings(&mut self, payload: &[u8]) -> Result<()> {
        for setting in payload.as_chunks::<6>().0 {
            let id = u16::from_be_bytes(setting[..2].try_into().expect("setting ID"));
            let value = u32::from_be_bytes(setting[2..].try_into().expect("setting value"));
            match id {
                1 => {
                    self.peer.header_table_size = value;
                    self.encoder.set_max_table_size(value.min(4096) as usize);
                }
                2 => {
                    if value > 1 || self.role == Http2Role::Client {
                        return Err(error(1, "invalid ENABLE_PUSH setting"));
                    }
                    self.peer.enable_push = value != 0;
                }
                3 => self.peer.max_concurrent_streams = value,
                4 => {
                    if value > i32::MAX as u32 {
                        return Err(error(3, "invalid initial window"));
                    }
                    let delta = i64::from(value) - i64::from(self.peer.initial_window_size);
                    if self
                        .streams
                        .values()
                        .any(|s| !s.close_emitted && s.send_window + delta > i64::from(i32::MAX))
                    {
                        return Err(error(3, "initial window update overflow"));
                    }
                    for stream in self.streams.values_mut().filter(|s| !s.close_emitted) {
                        stream.send_window += delta;
                    }
                    self.peer.initial_window_size = value;
                }
                5 => {
                    if !(16_384..=16_777_215).contains(&value) {
                        return Err(error(1, "invalid frame size setting"));
                    }
                    self.peer.max_frame_size = value;
                }
                6 => self.peer.max_header_list_size = value,
                _ => {}
            }
        }
        Ok(())
    }
    fn receive_data(&mut self, id: i32, end: bool, data: &[u8], wire_len: usize) -> Result<()> {
        self.recv_window -= wire_len as i64;
        if self.recv_window < 0 {
            return Err(error(3, "connection receive window exceeded"));
        }
        let Some(stream) = self.streams.get_mut(&id) else {
            if self.idle(id) {
                return Err(error(1, "DATA on idle stream"));
            }
            self.credit_connection(wire_len)?;
            return Ok(());
        };
        if stream.reset || stream.close_emitted {
            self.credit_connection(wire_len)?;
            return Ok(());
        }
        stream.recv_window -= wire_len as i64;
        if stream.recv_window < 0 || !stream.initial || stream.remote_closed {
            self.credit_connection(wire_len)?;
            self.stream_failure(id, false, "DATA violates stream state or flow window");
            return Ok(());
        }
        stream.unconsumed += data.len();
        if end {
            stream.remote_closed = true;
        }
        self.events.push_back(SessionEvent::DataChunk {
            stream_id: id,
            data: data.to_vec(),
        });
        self.events.push_back(SessionEvent::FrameRecv {
            stream_id: id,
            frame_type: FrameType::Data,
            flags: u8::from(end),
        });
        let padding = wire_len - data.len();
        self.credit_connection(padding)?;
        self.credit_stream(id, padding)?;
        self.close_if_complete(id);
        Ok(())
    }
    /// Returns application-consumed bytes to both receive windows in bounded batches.
    /// # Errors
    /// Rejects unknown streams and credit exceeding delivered DATA bytes.
    pub fn consume(&mut self, id: i32, amount: usize) -> Result<()> {
        let stream = self
            .streams
            .get_mut(&id)
            .ok_or_else(|| error(1, "consume on unknown stream"))?;
        if amount > stream.unconsumed {
            return Err(error(1, "receive credit exceeds delivered bytes"));
        }
        stream.unconsumed -= amount;
        self.credit_connection(amount)?;
        self.credit_stream(id, amount)
    }
    pub(super) fn credit_connection(&mut self, amount: usize) -> Result<()> {
        self.credit = self
            .credit
            .checked_add(u32::try_from(amount).map_err(|_| error(3, "receive credit overflow"))?)
            .ok_or_else(|| error(3, "receive credit overflow"))?;
        if self.credit >= 32_768 {
            let credit = std::mem::take(&mut self.credit);
            self.recv_window += i64::from(credit);
            self.queue_frame(0, FrameType::WindowUpdate, 0, &credit.to_be_bytes());
        }
        Ok(())
    }
    fn credit_stream(&mut self, id: i32, amount: usize) -> Result<()> {
        let stream = self.streams.get_mut(&id).expect("delivered stream");
        if stream.reset || stream.remote_closed {
            return Ok(());
        }
        stream.credit = stream
            .credit
            .checked_add(u32::try_from(amount).map_err(|_| error(3, "stream credit overflow"))?)
            .ok_or_else(|| error(3, "stream credit overflow"))?;
        if stream.credit >= (self.local.initial_window_size / 2).max(1) {
            let credit = std::mem::take(&mut stream.credit);
            stream.recv_window += i64::from(credit);
            self.queue_frame(id, FrameType::WindowUpdate, 0, &credit.to_be_bytes());
        }
        Ok(())
    }
}
