//! Inbound HTTP/2 events, metadata budgets and gRPC message envelopes.
use super::*;

#[rustfmt::skip]
impl<Role> GrpcSession<Role> {
    pub(super) fn events(&mut self) -> Result<(), Status> {
        while let Some(event) = self.protocol.next_event() {
            let id = match &event {
                SessionEvent::BeginHeaders { stream_id, .. } | SessionEvent::Header { stream_id, .. } | SessionEvent::DataChunk { stream_id, .. } | SessionEvent::FrameRecv { stream_id, .. } | SessionEvent::StreamError { stream_id, .. } => {
                    Some(*stream_id)
                }
                _ => None,
            };
            if id.is_some_and(|id| self.streams.get(&id).is_some_and(|s| s.failure.is_some())) {
                continue;
            }
            if let Err(error) = self.event(event) {
                if let Some(id) = id.filter(|id| *id > 0) {
                    self.protocol.submit_rst_stream(id, 2).map_err(internal)?;
                    if self.streams.contains_key(&id) {
                        self.fail_stream(id, error, 2)?;
                    }
                } else {
                    self.failed = Some(error.clone());
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    fn event(&mut self, event: SessionEvent) -> Result<(), Status> {
        match event {
            SessionEvent::FrameSend { stream_id, frame_type, flags } if flags & 1 != 0 && matches!(frame_type, FrameType::Headers | FrameType::Data) => {
                if let Some(stream) = self.streams.get_mut(&stream_id) { stream.end_stream_serialized = true; }
            }
            SessionEvent::BeginHeaders { stream_id: id, .. } => {
                if !self.streams.contains_key(&id) {
                    if self.client() { return Err(Status::internal("unsolicited server stream")); }
                    self.open(id, Rc::new(RefCell::new(Outbound::default())), CompressionAlgorithm::Identity)?;
                }
                let stream = self.stream_mut(id)?;
                stream.headers.clear();
                stream.header_bytes = 0;
            }
            SessionEvent::Header { stream_id: id, name, value, .. } => {
                let budget = self.config.per_stream_byte_budget;
                let max = self.config.max_header_list_size as usize;
                let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
                let stream = self.stream_mut(id)?;
                let size = name.len().checked_add(value.len()).and_then(|n| n.checked_add(32)).ok_or_else(resource)?;
                let total = stream.header_bytes.checked_add(size).ok_or_else(resource)?;
                if total > max || retained.checked_add(total).and_then(|n| n.checked_add(stream.reserved)).is_none_or(|n| n > budget) { return Err(resource()); }
                stream.header_bytes = total;
                stream.headers.push((String::from_utf8(name).map_err(internal)?, value));
            }
            SessionEvent::DataChunk { stream_id: id, data, .. } => {
                self.protocol.consume(id, data.len()).map_err(internal)?;
                self.data(id, &data)?;
            }
            SessionEvent::FrameRecv { stream_id: id, frame_type, flags } if id > 0 => {
                if frame_type == FrameType::Headers {
                    self.headers(id, flags & 1 != 0)?;
                }
                if flags & 1 != 0 && (frame_type == FrameType::Headers || frame_type == FrameType::Data) {
                    self.end(id)?;
                }
            }
            SessionEvent::StreamClose { stream_id: id, error_code } => {
                if !self.streams.contains_key(&id) { self.protocol.release_stream(id).map_err(internal)?; return Ok(()); }
                if let Some(stream) = self.streams.get_mut(&id) { stream.closed = true; }
                if self.streams.contains_key(&id) && !self.streams[&id].terminal && self.streams[&id].failure.is_none() {
                    if error_code != 0 {
                        self.fail_stream(id, Status::new(if error_code == 8 { Code::Cancelled } else { Code::Unavailable }, format!("HTTP/2 reset {}", error_code)), error_code)?;
                    } else if self.client() {
                        self.end(id)?;
                    } else if self.slots.get(id).is_some_and(|s| !s.remote_end_stream() || !s.local_end_stream()) {
                        self.fail_stream(id, Status::cancelled("peer closed unfinished request"), 0)?;
                    }
                }
                if self.client() && error_code == 0 && self.slots.get(id).is_some_and(|s| s.reset_error_code().is_none()) { self.slots.record_rst_stream(id, 0).map_err(slot_error)?; }
            }
            SessionEvent::GoAway { last_stream_id, error_code, .. } => {
                for id in self.slots.apply_goaway(last_stream_id, error_code).map_err(slot_error)? {
                    let stream = self.stream_mut(id)?;
                    stream.failure = Some(Status::unavailable("stream excluded by GOAWAY"));
                    stream.outgoing.borrow_mut().bytes.clear();
                    stream.pending_trailers = None;
                    stream.decoder.reset();
                    stream.reserved = 0;
                    self.incoming.retain(|r| r.stream_id != id);
                }
            }
            SessionEvent::StreamError { resource, message, .. } => {
                return Err(if resource { Status::resource_exhausted(message) } else { Status::internal(message) });
            }
            SessionEvent::Error { code, .. } => return Err(internal(code)),
            _ => {}
        }
        Ok(())
    }
    fn headers(&mut self, id: i32, end: bool) -> Result<(), Status> {
        let headers = std::mem::take(&mut self.stream_mut(id)?.headers);
        self.stream_mut(id)?.header_bytes = 0;
        let first = !self.stream_mut(id)?.initial;
        if first {
            if self.client() {
                let status = value(&headers, ":status")?.unwrap_or("").parse::<u16>().map_err(internal)?;
                if (100..200).contains(&status) && !end { return Ok(()); }
                self.stream_mut(id)?.http_status = status;
            } else if value(&headers, ":method")? != Some("POST") || value(&headers, "te")? != Some("trailers") {
                return Err(Status::internal("invalid gRPC request headers"));
            }
            if (!self.client() || self.stream_mut(id)?.http_status == 200 || value(&headers, "grpc-status")?.is_some())
                && !value(&headers, "content-type")?.is_some_and(|v| v.starts_with("application/grpc"))
            {
                return Err(Status::internal("invalid gRPC content-type"));
            }
            let encoding = CompressionAlgorithm::from_encoding(value(&headers, "grpc-encoding")?.unwrap_or("identity"))?;
            if !self.client() {
                self.incoming.push_back(IncomingRequest {
                    stream_id: id, content_type: value(&headers, "content-type")?.unwrap_or("").into(),
                    path: value(&headers, ":path")?.ok_or_else(|| Status::internal("missing :path"))?.into(),
                    authority: value(&headers, ":authority")?.unwrap_or("").into(),
                    timeout: value(&headers, "grpc-timeout")?.map(parse_grpc_timeout).transpose().map_err(internal)?,
                    request_encoding: encoding,
                    accept_encoding: value(&headers, "grpc-accept-encoding")?.unwrap_or("identity").into(),
                    initial_metadata: metadata(&headers)?,
                });
            }
            self.stream_mut(id)?.encoding = encoding;
            self.stream_mut(id)?.initial = true;
            if !(self.client() && end && value(&headers, "grpc-status")?.is_some()) {
                self.slots.push_initial_metadata(id, headers.clone()).map_err(slot_error)?;
            }
        }
        if self.client() && end {
            if let Some(code) = value(&headers, "grpc-status")? {
                let n = code.parse::<i32>().map_err(internal)?;
                if !(0..=16).contains(&n) { return Err(Status::internal("invalid grpc-status")); }
            } else {
                return self.missing_trailers(id);
            }
            self.stream_mut(id)?.decoder.finish()?;
            self.slots.push_trailing_metadata(id, headers).map_err(slot_error)?;
            self.stream_mut(id)?.terminal = true;
        } else if !first {
            return Err(Status::internal("unexpected nonterminal header block"));
        }
        Ok(())
    }
    fn missing_trailers(&mut self, id: i32) -> Result<(), Status> {
        let http = self.stream_mut(id)?.http_status;
        let status = if http == 200 { Status::unknown("missing grpc-status trailer") } else { Status::from_http_status(http) };
        self.slots
            .push_trailing_metadata(id, vec![("grpc-status".into(), status.code().as_i32().to_string().into_bytes()), ("grpc-message".into(), status.encoded_message().into_bytes())])
            .map_err(slot_error)?;
        self.stream_mut(id)?.terminal = true;
        Ok(())
    }
    fn end(&mut self, id: i32) -> Result<(), Status> {
        self.stream_mut(id)?.decoder.finish()?;
        if self.client() && !self.stream_mut(id)?.terminal {
            self.missing_trailers(id)?;
        }
        if !self.slots.get(id).is_some_and(|s| s.remote_end_stream()) {
            self.slots.mark_remote_end_stream(id).map_err(slot_error)?;
        }
        Ok(())
    }
    fn data(&mut self, id: i32, data: &[u8]) -> Result<(), Status> {
        let budget = self.config.per_stream_byte_budget;
        let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
        let client = self.client();
        let stream = self.stream_mut(id)?;
        if client && stream.http_status != 200 { return Ok(()); }
        if !stream.initial { return Err(Status::internal("DATA before initial headers")); }
        let mut completed = 0usize;
        for &byte in data {
            if stream.remaining > 0 {
                stream.remaining -= 1;
                if stream.remaining == 0 {
                    completed = completed.checked_add(stream.reserved).ok_or_else(resource)?;
                    stream.reserved = 0;
                }
            } else {
                stream.prefix.push(byte);
                if stream.prefix.len() == 5 {
                    let n = u32::from_be_bytes([stream.prefix[1], stream.prefix[2], stream.prefix[3], stream.prefix[4]]) as usize;
                    stream.reserved = n.checked_add(5 + std::mem::size_of::<StreamMessage>()).ok_or_else(resource)?;
                    stream.remaining = n;
                    stream.prefix.clear();
                    if n == 0 {
                        completed = completed.checked_add(stream.reserved).ok_or_else(resource)?;
                        stream.reserved = 0;
                    }
                }
            }
            if retained.checked_add(completed).and_then(|n| n.checked_add(stream.reserved)).and_then(|n| n.checked_add(stream.prefix.len())).is_none_or(|n| n > budget) { return Err(resource()); }
        }
        let frames = stream.decoder.push(data)?;
        let encoding = stream.encoding;
        for frame in frames {
            let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
            let remaining = self.stream_mut(id)?.reserved;
            let available = budget.saturating_sub(retained).saturating_sub(remaining).saturating_sub(5 + std::mem::size_of::<StreamMessage>());
            let config = CompressionConfig { algorithm: encoding, max_decoding_message_size: self.config.compression.max_decoding_message_size.min(self.config.max_inbound_message_size).min(available), ..self.config.compression };
            let payload = config.decompress(frame.payload(), frame.is_compressed())?;
            self.slots.push_message(id, StreamMessage { compressed: false, payload }).map_err(slot_error)?;
        }
        Ok(())
    }
}
