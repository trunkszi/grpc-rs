//! Outbound header ordering, incremental body scheduling and peer window accounting.
use super::*;

fn wire_type(kind: FrameType) -> u8 {
    match kind {
        FrameType::Data => 0,
        FrameType::Headers => 1,
        FrameType::Priority => 2,
        FrameType::RstStream => 3,
        FrameType::Settings => 4,
        FrameType::PushPromise => 5,
        FrameType::Ping => 6,
        FrameType::GoAway => 7,
        FrameType::WindowUpdate => 8,
        FrameType::Continuation => 9,
        FrameType::Unknown(value) => value,
    }
}
#[inline]
fn append_frame(out: &mut Vec<u8>, id: i32, kind: FrameType, flags: u8, payload: &[u8]) {
    let prefix = FramePrefix::new(
        payload.len() as u32,
        wire_type(kind),
        flags,
        StreamId(id as u32),
    );
    out.extend_from_slice(&prefix.wire());
    out.extend_from_slice(payload);
}
impl Http2Session {
    pub(super) fn queue_frame(&mut self, id: i32, kind: FrameType, flags: u8, payload: &[u8]) {
        let mut bytes = Vec::with_capacity(9 + payload.len());
        append_frame(&mut bytes, id, kind, flags, payload);
        self.outgoing.push_back(Outgoing::Frame {
            bytes,
            id,
            kind,
            flags,
        });
    }
    fn queue_headers(&mut self, id: i32, fields: &[HeaderField<'_>], end: bool) -> Result<()> {
        let mut size = 0usize;
        let fields = fields
            .iter()
            .map(|field| {
                size = size
                    .saturating_add(field.name.len())
                    .saturating_add(field.value.len())
                    .saturating_add(32);
                let name = std::str::from_utf8(field.name).map_err(|e| error(1, e.to_string()))?;
                let value =
                    std::str::from_utf8(field.value).map_err(|e| error(1, e.to_string()))?;
                Ok((name.to_owned(), value.to_owned()))
            })
            .collect::<Result<Vec<_>>>()?;
        if size > self.peer.max_header_list_size as usize {
            return Err(error(11, "peer header budget exceeded"));
        }
        let mut block = BytesMut::new();
        self.encoder.encode(&fields, &mut block);
        self.outgoing.push_back(Outgoing::Headers {
            id,
            block: block.as_slice().to_vec(),
            end,
        });
        Ok(())
    }
    /// Opens a client stream, retaining the body source until EOF or reset.
    /// # Errors
    /// Rejects draining connections, exhausted IDs and peer concurrency limits.
    pub fn submit_request(
        &mut self,
        fields: &[HeaderField<'_>],
        provider: Option<DataProvider>,
    ) -> Result<i32> {
        self.check_live()?;
        if self.role != Http2Role::Client
            || self.sent_goaway.is_some()
            || self.received_goaway.is_some()
        {
            return Err(error(7, "connection cannot open another request"));
        }
        if self
            .streams
            .values()
            .filter(|s| !s.state.matches(pattern!(HttpStreamWord { CLOSE_EMITTED })))
            .count()
            >= self.peer.max_concurrent_streams as usize
        {
            return Err(error(7, "peer concurrency limit reached"));
        }
        let id = i32::try_from(self.next_id).map_err(|_| error(7, "stream IDs exhausted"))?;
        let next = self.next_id + 2;
        self.queue_headers(id, fields, provider.is_none())?;
        let mut stream = Stream::new(self.local, self.peer);
        stream.provider = provider;
        self.streams.insert(id, stream);
        self.next_id = next;
        Ok(id)
    }
    /// Queues response headers, with an optional incremental body producer.
    /// # Errors
    /// Rejects unknown or closed streams and invalid outgoing headers.
    pub fn submit_response(
        &mut self,
        id: i32,
        fields: &[HeaderField<'_>],
        provider: Option<DataProvider>,
    ) -> Result<()> {
        self.check_stream_send(id)?;
        self.queue_headers(id, fields, provider.is_none())?;
        self.streams
            .get_mut(&id)
            .ok_or_else(|| error(5, "unknown stream"))?
            .provider = provider;
        Ok(())
    }
    /// Queues an informational or final header block with explicit END_STREAM.
    /// # Errors
    /// Rejects unknown or closed streams and invalid outgoing headers.
    pub fn submit_headers(&mut self, flags: u8, id: i32, fields: &[HeaderField<'_>]) -> Result<()> {
        self.check_stream_send(id)?;
        self.queue_headers(
            id,
            fields,
            FrameFlags::new(u32::from(flags)).matches(pattern!(FrameFlags { END_STREAM })),
        )
    }
    /// Queues the final trailer block after the body source has finished.
    /// # Errors
    /// Rejects closed streams or a body source that has not reached EOF.
    pub fn submit_trailer(&mut self, id: i32, fields: &[HeaderField<'_>]) -> Result<()> {
        self.check_stream_send(id)?;
        if self.streams[&id].provider.is_some() {
            return Err(error(1, "body source has not finished"));
        }
        self.queue_headers(id, fields, true)
    }
    fn check_stream_send(&self, id: i32) -> Result<()> {
        self.check_live()?;
        if self.streams.get(&id).is_none_or(|s| {
            s.state.matches(pattern!(HttpStreamWord { LOCAL_CLOSED }))
                || s.state.matches(pattern!(HttpStreamWord { RESET }))
        }) {
            return Err(error(5, "stream cannot send"));
        }
        Ok(())
    }
    /// Resumes a body source that returned Deferred.
    /// # Errors
    /// Rejects unknown or reset streams.
    pub fn resume_data(&mut self, id: i32) -> Result<()> {
        self.check_stream_send(id)?;
        let stream = self
            .streams
            .get_mut(&id)
            .ok_or_else(|| error(5, "unknown stream"))?;
        stream.state = stream
            .state
            .patch(patch!(HttpStreamWord { DEFERRED: false }));
        stream.state = stream.state.patch(patch!(HttpStreamWord {
            WINDOW_BLOCKED: false
        }));
        Ok(())
    }
    /// Queues a reset without discarding previously encoded HPACK context.
    /// # Errors
    /// Rejects an unknown stream.
    pub fn submit_rst_stream(&mut self, id: i32, code: u32) -> Result<()> {
        let stream = self
            .streams
            .get_mut(&id)
            .ok_or_else(|| error(1, "unknown reset stream"))?;
        if stream.state.matches(pattern!(HttpStreamWord { RESET }))
            || stream
                .state
                .matches(pattern!(HttpStreamWord { CLOSE_EMITTED }))
        {
            return Ok(());
        }
        stream.provider = None;
        stream.state = stream.state.patch(patch!(HttpStreamWord { RESET: true }));
        let unconsumed = std::mem::take(&mut stream.unconsumed);
        self.credit_connection(unconsumed)?;
        self.queue_frame(id, FrameType::RstStream, 0, &code.to_be_bytes());
        Ok(())
    }
    /// Starts graceful shutdown while allowing accepted streams to finish.
    /// # Errors
    /// Rejects invalid boundaries and oversized debug payloads.
    pub fn submit_goaway(&mut self, last: i32, code: u32, debug: &[u8]) -> Result<()> {
        if last < 0 || debug.len() + 8 > self.peer.max_frame_size as usize {
            return Err(error(6, "invalid GOAWAY"));
        }
        let last = self.sent_goaway.map_or(last, |old| old.min(last));
        let mut bytes = Vec::with_capacity(8 + debug.len());
        bytes.extend_from_slice(&(last as u32).to_be_bytes());
        bytes.extend_from_slice(&code.to_be_bytes());
        bytes.extend_from_slice(debug);
        self.queue_frame(0, FrameType::GoAway, 0, &bytes);
        self.sent_goaway = Some(last);
        Ok(())
    }
    /// Serializes one queued control/header block or one flow-controlled DATA frame.
    /// # Errors
    /// Returns a sticky connection failure or an invalid body-source result.
    #[inline]
    pub fn mem_send(&mut self) -> Result<&[u8]> {
        self.check_live()?;
        self.output.clear();
        if let Some(item) = self.outgoing.pop_front() {
            self.serialize(item);
        } else if let Err(failure) = self.send_body() {
            self.failed = Some(failure.clone());
            return Err(failure);
        }
        Ok(&self.output)
    }
    fn serialize(&mut self, item: Outgoing) {
        let (id, kind, flags) = match item {
            Outgoing::Frame {
                bytes,
                id,
                kind,
                flags,
            } => {
                self.output.extend_from_slice(&bytes);
                (id, kind, flags)
            }
            Outgoing::Headers { id, block, end } => {
                let max = self.peer.max_frame_size as usize;
                let total = block.len().max(1).div_ceil(max);
                if block.is_empty() {
                    append_frame(
                        &mut self.output,
                        id,
                        FrameType::Headers,
                        4 | u8::from(end),
                        &[],
                    );
                }
                for (index, chunk) in block.chunks(max).enumerate() {
                    let kind = if index == 0 {
                        FrameType::Headers
                    } else {
                        FrameType::Continuation
                    };
                    let flags = FrameFlags::default()
                        .patch(
                            BitPatch::EMPTY
                                .with_flag(FrameFlags::END_STREAM, index == 0 && end)
                                .with_flag(FrameFlags::END_HEADERS, index + 1 == total),
                        )
                        .raw() as u8;
                    append_frame(&mut self.output, id, kind, flags, chunk);
                }
                (id, FrameType::Headers, u8::from(end))
            }
        };
        self.events.push_back(SessionEvent::FrameSend {
            stream_id: id,
            frame_type: kind,
            flags,
        });
        if kind == FrameType::RstStream {
            if let Some(stream) = self.streams.get_mut(&id)
                && !stream
                    .state
                    .matches(pattern!(HttpStreamWord { CLOSE_EMITTED }))
            {
                stream.state = stream.state.patch(patch!(HttpStreamWord {
                    CLOSE_EMITTED: true
                }));
                let code = u32::from_be_bytes([
                    self.output[9],
                    self.output[10],
                    self.output[11],
                    self.output[12],
                ]);
                self.events.push_back(SessionEvent::StreamClose {
                    stream_id: id,
                    error_code: code,
                });
            }
        } else if FrameFlags::new(u32::from(flags)).matches(pattern!(FrameFlags { END_STREAM }))
            && matches!(kind, FrameType::Headers | FrameType::Data)
        {
            if let Some(stream) = self.streams.get_mut(&id) {
                stream.state = stream
                    .state
                    .patch(patch!(HttpStreamWord { LOCAL_CLOSED: true }));
            }
            self.close_if_complete(id);
        }
    }
    fn send_body(&mut self) -> Result<()> {
        let ids: Vec<_> = self
            .streams
            .range((
                std::ops::Bound::Excluded(self.last_sent),
                std::ops::Bound::Unbounded,
            ))
            .chain(self.streams.range(..=self.last_sent))
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let Some(stream) = self.streams.get_mut(&id) else {
                continue;
            };
            if !stream
                .state
                .matches(pattern!(HttpStreamWord { !DEFERRED & !RESET & !LOCAL_CLOSED }))
            {
                continue;
            }
            let Some(provider) = &mut stream.provider else {
                continue;
            };
            let max = self
                .send_window
                .min(stream.send_window)
                .max(0)
                .min(self.peer.max_frame_size.into()) as usize;
            let (data, eof, trailers) = match provider.0.read(max) {
                DataPoll::Copy(bytes) => {
                    if bytes.len() > max {
                        return Err(error(2, "body producer exceeded permitted length"));
                    }
                    if bytes.is_empty() {
                        stream.state = stream.state.patch(
                            BitPatch::EMPTY.with_flag(HttpStreamWord::WINDOW_BLOCKED, max == 0),
                        );
                        continue;
                    }
                    stream.state = stream.state.patch(patch!(HttpStreamWord {
                        WINDOW_BLOCKED: false
                    }));
                    (bytes.to_vec(), false, false)
                }
                DataPoll::Deferred => {
                    stream.state = stream
                        .state
                        .patch(patch!(HttpStreamWord { DEFERRED: true }));
                    continue;
                }
                DataPoll::Eof => (Vec::new(), true, false),
                DataPoll::EofWithTrailers => (Vec::new(), true, true),
            };
            if eof {
                stream.provider = None;
            }
            if trailers {
                continue;
            }
            stream.send_window -= data.len() as i64;
            self.send_window -= data.len() as i64;
            let mut bytes = Vec::with_capacity(9 + data.len());
            append_frame(&mut bytes, id, FrameType::Data, u8::from(eof), &data);
            self.last_sent = id;
            self.serialize(Outgoing::Frame {
                bytes,
                id,
                kind: FrameType::Data,
                flags: u8::from(eof),
            });
            break;
        }
        Ok(())
    }
}
