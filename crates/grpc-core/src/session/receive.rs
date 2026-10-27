//! Inbound HTTP/2 events, metadata budgets and gRPC message envelopes.
use super::*;

impl<Role> GrpcSession<Role> {
    pub(super) fn events(&mut self) -> Result<(), Status> {
        while let Some(event) = self.protocol.next_event() {
            let id = match &event {
                SessionEvent::BeginHeaders { stream_id, .. }
                | SessionEvent::Header { stream_id, .. }
                | SessionEvent::DataChunk { stream_id, .. }
                | SessionEvent::FrameRecv { stream_id, .. }
                | SessionEvent::StreamError { stream_id, .. } => Some(*stream_id),
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
                    self.failure = Some(SessionFailure {
                        phase: self.phase,
                        status: error.clone(),
                    });
                    self.phase = SessionPhase::Failed;
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    fn event(&mut self, event: SessionEvent) -> Result<(), Status> {
        match event {
            SessionEvent::FrameSend {
                stream_id,
                frame_type,
                flags,
            } if flags & 1 != 0 && matches!(frame_type, FrameType::Headers | FrameType::Data) => {
                if let Some(stream) = self.streams.get_mut(&stream_id) {
                    stream.state.serialize_end();
                }
            }
            SessionEvent::BeginHeaders { stream_id: id, .. } => {
                if !self.streams.contains_key(&id) {
                    if self.client() {
                        return Err(Status::internal("unsolicited server stream"));
                    }
                    self.open(
                        id,
                        Rc::new(RefCell::new(Outbound::default())),
                        CompressionAlgorithm::Identity,
                    )?;
                }
                let stream = self.stream_mut(id)?;
                stream.headers.clear();
                stream.header_bytes = 0;
            }
            SessionEvent::Header {
                stream_id: id,
                name,
                value,
                ..
            } => {
                let budget = self.config.per_stream_byte_budget;
                let max = self.config.max_header_list_size as usize;
                let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
                let stream = self.stream_mut(id)?;
                let size = name
                    .len()
                    .checked_add(value.len())
                    .and_then(|n| n.checked_add(32))
                    .ok_or_else(resource)?;
                let total = stream.header_bytes.checked_add(size).ok_or_else(resource)?;
                if total > max
                    || retained
                        .checked_add(total)
                        .and_then(|n| n.checked_add(stream.reserved))
                        .is_none_or(|n| n > budget)
                {
                    return Err(resource());
                }
                stream.header_bytes = total;
                stream
                    .headers
                    .push((String::from_utf8(name).map_err(internal)?, value));
            }
            SessionEvent::DataChunk {
                stream_id: id,
                data,
                ..
            } => {
                self.protocol.consume(id, data.len()).map_err(internal)?;
                self.data(id, &data)?;
            }
            SessionEvent::FrameRecv {
                stream_id: id,
                frame_type,
                flags,
            } if id > 0 => {
                if frame_type == FrameType::Headers {
                    self.headers(id, flags & 1 != 0)?;
                }
                if flags & 1 != 0
                    && (frame_type == FrameType::Headers || frame_type == FrameType::Data)
                {
                    self.end(id)?;
                }
            }
            SessionEvent::StreamClose {
                stream_id: id,
                error_code,
            } => {
                if !self.streams.contains_key(&id) {
                    self.protocol.release_stream(id).map_err(internal)?;
                    return Ok(());
                }
                if let Some(stream) = self.streams.get_mut(&id) {
                    stream.state.close();
                }
                if self.streams.contains_key(&id)
                    && !self.streams[&id].state.terminal()
                    && self.streams[&id].failure.is_none()
                {
                    if error_code != 0 {
                        self.fail_stream(
                            id,
                            Status::new(
                                if error_code == 8 {
                                    Code::Cancelled
                                } else {
                                    Code::Unavailable
                                },
                                format!("HTTP/2 reset {}", error_code),
                            ),
                            error_code,
                        )?;
                    } else if self.client() {
                        self.end(id)?;
                    } else if self
                        .slots
                        .get(id)
                        .is_some_and(|s| !s.remote_end_stream() || !s.local_end_stream())
                    {
                        self.fail_stream(
                            id,
                            Status::cancelled("peer closed unfinished request"),
                            0,
                        )?;
                    }
                }
                if self.client()
                    && error_code == 0
                    && self
                        .slots
                        .get(id)
                        .is_some_and(|s| s.reset_error_code().is_none())
                {
                    self.slots.record_rst_stream(id, 0).map_err(slot_error)?;
                }
            }
            SessionEvent::GoAway {
                last_stream_id,
                error_code,
                ..
            } => {
                self.phase = SessionPhase::Draining;
                for id in self
                    .slots
                    .apply_goaway(last_stream_id, error_code)
                    .map_err(slot_error)?
                {
                    let stream = self.stream_mut(id)?;
                    stream.failure = Some(Status::unavailable("stream excluded by GOAWAY"));
                    stream.outgoing.borrow_mut().bytes.clear();
                    stream.pending_trailers = None;
                    stream.decoder.reset();
                    stream.reserved = 0;
                    self.incoming.retain(|r| r.stream_id != id);
                }
            }
            SessionEvent::StreamError {
                resource, message, ..
            } => {
                return Err(if resource {
                    Status::resource_exhausted(message)
                } else {
                    Status::internal(message)
                });
            }
            SessionEvent::Error { code, .. } => return Err(internal(code)),
            _ => {}
        }
        Ok(())
    }
    fn headers(&mut self, id: i32, end: bool) -> Result<(), Status> {
        let headers = std::mem::take(&mut self.stream_mut(id)?.headers);
        self.stream_mut(id)?.header_bytes = 0;
        let first = !self.stream_mut(id)?.state.initial_received();
        if first {
            if self.client() {
                let status = value(&headers, ":status")?
                    .unwrap_or("")
                    .parse::<u16>()
                    .map_err(internal)?;
                if (100..200).contains(&status) && !end {
                    return Ok(());
                }
                self.stream_mut(id)?.http_status = status;
            } else if value(&headers, ":method")? != Some("POST")
                || value(&headers, "te")? != Some("trailers")
            {
                return Err(Status::internal("invalid gRPC request headers"));
            }
            if (!self.client()
                || self.stream_mut(id)?.http_status == 200
                || value(&headers, "grpc-status")?.is_some())
                && !value(&headers, "content-type")?
                    .is_some_and(|v| v.starts_with("application/grpc"))
            {
                return Err(Status::internal("invalid gRPC content-type"));
            }
            let encoding = CompressionAlgorithm::from_encoding(
                value(&headers, "grpc-encoding")?.unwrap_or("identity"),
            )?;
            if !self.client() {
                self.incoming.push_back(IncomingRequest {
                    stream_id: id,
                    content_type: value(&headers, "content-type")?.unwrap_or("").into(),
                    path: value(&headers, ":path")?
                        .ok_or_else(|| Status::internal("missing :path"))?
                        .into(),
                    authority: value(&headers, ":authority")?.unwrap_or("").into(),
                    timeout: value(&headers, "grpc-timeout")?
                        .map(parse_grpc_timeout)
                        .transpose()
                        .map_err(internal)?,
                    request_encoding: encoding,
                    accept_encoding: value(&headers, "grpc-accept-encoding")?
                        .unwrap_or("identity")
                        .into(),
                    initial_metadata: metadata(&headers)?,
                });
            }
            self.stream_mut(id)?.encoding = encoding;
            self.stream_mut(id)?.state.receive_headers();
            if !(self.client() && end && value(&headers, "grpc-status")?.is_some()) {
                self.slots
                    .push_initial_metadata(id, headers.clone())
                    .map_err(slot_error)?;
            }
        }
        if self.client() && end {
            if let Some(code) = value(&headers, "grpc-status")? {
                let n = code.parse::<i32>().map_err(internal)?;
                if !(0..=16).contains(&n) {
                    return Err(Status::internal("invalid grpc-status"));
                }
            } else {
                return self.missing_trailers(id);
            }
            self.stream_mut(id)?.decoder.finish()?;
            self.slots
                .push_trailing_metadata(id, headers)
                .map_err(slot_error)?;
            self.stream_mut(id)?.state.receive_trailers();
        } else if !first {
            return Err(Status::internal("unexpected nonterminal header block"));
        }
        Ok(())
    }
    fn missing_trailers(&mut self, id: i32) -> Result<(), Status> {
        let http = self.stream_mut(id)?.http_status;
        let status = if http == 200 {
            Status::unknown("missing grpc-status trailer")
        } else {
            Status::from_http_status(http)
        };
        self.slots
            .push_trailing_metadata(
                id,
                vec![
                    (
                        "grpc-status".into(),
                        status.code().as_i32().to_string().into_bytes(),
                    ),
                    ("grpc-message".into(), status.encoded_message().into_bytes()),
                ],
            )
            .map_err(slot_error)?;
        self.stream_mut(id)?.state.receive_trailers();
        Ok(())
    }
    fn end(&mut self, id: i32) -> Result<(), Status> {
        self.stream_mut(id)?.decoder.finish()?;
        if self.client() && !self.stream_mut(id)?.state.terminal() {
            self.missing_trailers(id)?;
        }
        if !self.slots.get(id).is_some_and(|s| s.remote_end_stream()) {
            self.slots.mark_remote_end_stream(id).map_err(slot_error)?;
            self.stream_mut(id)?.state.remote_end();
        }
        Ok(())
    }
    fn data(&mut self, id: i32, data: &[u8]) -> Result<(), Status> {
        let budget = self.config.per_stream_byte_budget;
        let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
        let client = self.client();
        let stream = self.stream_mut(id)?;
        if client && stream.http_status != 200 {
            return Ok(());
        }
        if !stream.state.initial_received() {
            return Err(Status::internal("DATA before initial headers"));
        }
        let mut completed = 0usize;
        let mut pending = data;
        while !pending.is_empty() {
            if stream.remaining > 0 {
                let count = stream.remaining.min(pending.len());
                stream.remaining -= count;
                pending = &pending[count..];
                if stream.remaining == 0 {
                    completed = completed
                        .checked_add(stream.reserved)
                        .ok_or_else(resource)?;
                    stream.reserved = 0;
                }
            } else {
                let count = (5 - stream.prefix_len).min(pending.len());
                stream.prefix[stream.prefix_len..stream.prefix_len + count]
                    .copy_from_slice(&pending[..count]);
                stream.prefix_len += count;
                pending = &pending[count..];
                if stream.prefix_len == 5 {
                    let n = u32::from_be_bytes([
                        stream.prefix[1],
                        stream.prefix[2],
                        stream.prefix[3],
                        stream.prefix[4],
                    ]) as usize;
                    stream.reserved = n
                        .checked_add(5 + std::mem::size_of::<StreamMessage>())
                        .ok_or_else(resource)?;
                    stream.remaining = n;
                    stream.prefix_len = 0;
                    if n == 0 {
                        completed = completed
                            .checked_add(stream.reserved)
                            .ok_or_else(resource)?;
                        stream.reserved = 0;
                    }
                }
            }
            if retained
                .checked_add(completed)
                .and_then(|n| n.checked_add(stream.reserved))
                .and_then(|n| n.checked_add(stream.prefix_len))
                .is_none_or(|n| n > budget)
            {
                return Err(resource());
            }
        }
        let frames = stream.decoder.push(data)?;
        let encoding = stream.encoding;
        for frame in frames {
            let retained = self.slots.get(id).map_or(0, |s| s.buffered_bytes());
            let remaining = self.stream_mut(id)?.reserved;
            let available = budget
                .saturating_sub(retained)
                .saturating_sub(remaining)
                .saturating_sub(5 + std::mem::size_of::<StreamMessage>());
            let config = CompressionConfig {
                algorithm: encoding,
                max_decoding_message_size: self
                    .config
                    .compression
                    .max_decoding_message_size
                    .min(self.config.max_inbound_message_size)
                    .min(available),
                ..self.config.compression
            };
            let payload = config.decompress(frame.payload(), frame.is_compressed())?;
            self.slots
                .push_message(
                    id,
                    StreamMessage {
                        compressed: false,
                        payload,
                    },
                )
                .map_err(slot_error)?;
        }
        Ok(())
    }
}

impl<Role> GrpcSession<Role> {
    /// Transfers custom initial metadata, preserving stream lifecycle history.
    /// # Errors
    /// Returns unknown/reset stream or metadata decoding errors.
    pub fn pop_initial_metadata(&mut self, id: i32) -> Result<Option<Metadata>, Status> {
        self.check_read(id)?;
        self.slots
            .pop_initial_metadata(id)
            .map_err(slot_error)?
            .map(|h| metadata(&h))
            .transpose()
    }
    /// Transfers one decompressed message into eight-byte-aligned storage.
    /// # Errors
    /// Returns unknown/reset stream or allocation errors.
    pub fn pop_message(&mut self, id: i32) -> Result<Option<AlignedMessageBuf>, Status> {
        self.check_read(id)?;
        self.slots
            .pop_message(id)
            .map_err(slot_error)?
            .map(|m| AlignedMessageBuf::new(&m.payload, false))
            .transpose()
    }
    /// Whether the peer's request or response half has ended.
    /// # Errors
    /// Returns unknown/reset stream errors.
    pub fn is_remote_end_stream(&self, id: i32) -> Result<bool, Status> {
        self.check_read(id)?;
        Ok(self.slots.get(id).is_some_and(|s| s.remote_end_stream()))
    }
    pub(super) fn check_read(&self, id: i32) -> Result<(), Status> {
        let s = self
            .streams
            .get(&id)
            .ok_or_else(|| Status::not_found("unknown stream"))?;
        s.failure.clone().map_or(Ok(()), Err)
    }
    /// Takes terminal status and custom trailing metadata exactly once.
    /// # Errors
    /// Returns unknown/reset stream or invalid metadata errors.
    pub fn take_trailers(&mut self, id: i32) -> Result<Option<(Status, Metadata)>, Status> {
        if let Some(error) = self.stream_mut(id)?.failure.clone() {
            if self.stream_mut(id)?.failure_taken {
                return Ok(None);
            }
            self.stream_mut(id)?.failure_taken = true;
            return Ok(Some((error, Metadata::new())));
        }
        self.slots
            .take_trailers(id)
            .map_err(slot_error)?
            .map(|h| {
                let code = value(&h, "grpc-status")?
                    .and_then(|v| v.parse::<i32>().ok())
                    .map_or(Code::Unknown, Code::from_i32);
                let message = decode_grpc_message(value(&h, "grpc-message")?.unwrap_or(""));
                Ok((Status::new(code, message), metadata(&h)?))
            })
            .transpose()
    }
    /// Releases a closed stream and all unread events.
    /// # Errors
    /// Rejects unknown or still-open streams.
    pub fn remove_closed_stream(&mut self, id: i32) -> Result<(), Status> {
        if !self.stream_mut(id)?.state.closed() {
            return Err(Status::failed_precondition("protocol stream is not closed"));
        }
        if self.slots.get(id).is_some() {
            self.slots.remove_closed_stream(id).map_err(slot_error)?;
        }
        self.protocol.release_stream(id).map_err(internal)?;
        self.streams.remove(&id);
        self.incoming.retain(|r| r.stream_id != id);
        Ok(())
    }
}
pub(super) fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}
pub(super) fn resource() -> Status {
    Status::resource_exhausted("per-stream byte budget exceeded")
}
pub(super) fn slot_error(error: SlotMapError) -> Status {
    match error {
        SlotMapError::MemoryLimit { .. } | SlotMapError::MessageLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        _ => internal(error),
    }
}
pub(super) fn fields(headers: &[(String, String)]) -> Vec<HeaderField<'_>> {
    headers
        .iter()
        .map(|(k, v)| HeaderField::new(k.as_bytes(), v.as_bytes()))
        .collect()
}
pub(super) fn metadata(headers: &Headers) -> Result<Metadata, Status> {
    let mut result = Metadata::new();
    for (key, value) in headers {
        if key.starts_with(':')
            || matches!(key.as_str(), "content-type" | "te" | "user-agent")
            || key.starts_with("grpc-")
        {
            continue;
        }
        result
            .append_header(key, std::str::from_utf8(value).map_err(internal)?)
            .map_err(internal)?;
    }
    Ok(result)
}
pub(super) fn validate_content_type(value: &str) -> Result<(), Status> {
    let media = value.split(';').next().unwrap_or("");
    let suffix = media.strip_prefix("application/grpc+");
    if !value.bytes().all(|b| (32..127).contains(&b))
        || !(media == "application/grpc"
            || suffix.is_some_and(|s| {
                !s.is_empty()
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
            }))
    {
        return Err(Status::invalid_argument("invalid gRPC content-type"));
    }
    Ok(())
}
pub(super) fn validate_metadata(metadata: &Metadata) -> Result<(), Status> {
    if metadata
        .iter()
        .any(|(key, _)| matches!(key, "content-type" | "te" | "user-agent"))
    {
        return Err(Status::invalid_argument(
            "application metadata contains protocol header",
        ));
    }
    Ok(())
}
pub(super) fn value<'a>(headers: &'a Headers, key: &str) -> Result<Option<&'a str>, Status> {
    let mut values = headers.iter().filter(|(k, _)| k == key);
    let result = values
        .next()
        .map(|(_, v)| std::str::from_utf8(v).map_err(internal))
        .transpose()?;
    if values.next().is_some() {
        return Err(Status::internal(format!("duplicate {key}")));
    }
    Ok(result)
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionPhase {
    Active,
    Draining,
    Failed,
}
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum InboundPhase {
    AwaitingInitialHeaders,
    StreamingBody,
    HalfClosedRemote,
    Terminal,
}
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum OutboundPhase {
    UnsentHeaders,
    StreamingBody,
    EndStreamQueued,
    Closed,
}

/// Orthogonal wire phases with dispatched and protocol-retired substates.
/// Closed output includes serialized END_STREAM; retirement also releases native state.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(super) struct StreamHsm {
    inbound: u8,
    outbound: u8,
}
const _: () = assert!(std::mem::size_of::<StreamHsm>() == 2);
impl StreamHsm {
    const SUBSTATE: u8 = 4;
    pub(super) fn inbound_phase(self) -> InboundPhase {
        match self.inbound & 3 {
            0 => InboundPhase::AwaitingInitialHeaders,
            1 => InboundPhase::StreamingBody,
            2 => InboundPhase::HalfClosedRemote,
            _ => InboundPhase::Terminal,
        }
    }
    pub(super) fn outbound_phase(self) -> OutboundPhase {
        match self.outbound & 3 {
            0 => OutboundPhase::UnsentHeaders,
            1 => OutboundPhase::StreamingBody,
            2 => OutboundPhase::EndStreamQueued,
            _ => OutboundPhase::Closed,
        }
    }
    fn inbound(&mut self, phase: InboundPhase) {
        self.inbound = (self.inbound & Self::SUBSTATE) | phase as u8;
    }
    pub(super) fn initial_received(self) -> bool {
        self.inbound_phase() != InboundPhase::AwaitingInitialHeaders
    }
    pub(super) fn terminal(self) -> bool {
        self.inbound_phase() == InboundPhase::Terminal
    }
    pub(super) fn headers_sent(self) -> bool {
        self.outbound_phase() != OutboundPhase::UnsentHeaders
    }
    pub(super) fn dispatched(self) -> bool {
        self.inbound & Self::SUBSTATE != 0
    }
    pub(super) fn closed(self) -> bool {
        self.outbound & Self::SUBSTATE != 0
    }
    pub(super) fn dispatch(&mut self) {
        self.inbound |= Self::SUBSTATE;
    }
    pub(super) fn receive_headers(&mut self) {
        assert_eq!(self.inbound_phase(), InboundPhase::AwaitingInitialHeaders);
        self.inbound(InboundPhase::StreamingBody);
    }
    pub(super) fn receive_trailers(&mut self) {
        self.inbound(InboundPhase::Terminal);
    }
    pub(super) fn remote_end(&mut self) {
        if !self.terminal() {
            self.inbound(InboundPhase::HalfClosedRemote);
        }
    }
    pub(super) fn send_headers(&mut self) {
        assert_eq!(self.outbound_phase(), OutboundPhase::UnsentHeaders);
        self.outbound = OutboundPhase::StreamingBody as u8;
    }
    pub(super) fn queue_end(&mut self) {
        assert!(matches!(
            self.outbound_phase(),
            OutboundPhase::UnsentHeaders | OutboundPhase::StreamingBody
        ));
        self.outbound = OutboundPhase::EndStreamQueued as u8;
    }
    pub(super) fn serialize_end(&mut self) {
        self.outbound = (self.outbound & Self::SUBSTATE) | OutboundPhase::Closed as u8;
    }
    pub(super) fn close(&mut self) {
        self.outbound = Self::SUBSTATE | OutboundPhase::Closed as u8;
    }
}

// Preserve GOAWAY admission history after entering the common failed superstate.
pub(super) struct SessionFailure {
    pub(super) phase: SessionPhase,
    pub(super) status: Status,
}
impl<Role> GrpcSession<Role> {
    pub(super) fn check_live(&self) -> Result<(), Status> {
        if self.phase == SessionPhase::Failed {
            Err(self
                .failure
                .as_ref()
                .expect("failed session retains its reason")
                .status
                .clone())
        } else {
            Ok(())
        }
    }
}
