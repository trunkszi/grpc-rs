//! HPACK block assembly and HTTP message header validation.
use nexora_proto::ProtoError;

use super::*;

#[rustfmt::skip]
fn validate_headers(fields: &[(String, String)], role: Http2Role, trailers: bool, id: u32) -> Result<()> {
    let invalid = || error(1, format!("invalid HTTP/2 message headers on stream {id}"));
    let mut regular = false;
    let mut pseudo = BTreeMap::new();
    for (name, value) in fields {
        if name.is_empty() || name.bytes().any(|b| b.is_ascii_uppercase()) || value.bytes().any(|b| matches!(b, 0 | b'\r' | b'\n')) || value.starts_with([' ', '\t']) || value.ends_with([' ', '\t']) {
            return Err(invalid());
        }
        if name.starts_with(':') {
            if trailers || regular || pseudo.insert(name.as_str(), value.as_str()).is_some() {
                return Err(invalid());
            }
            if !(if role == Http2Role::Server { matches!(name.as_str(), ":method" | ":scheme" | ":path" | ":authority") } else { name == ":status" }) {
                return Err(invalid());
            }
        } else {
            regular = true;
            if !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&b))
                || matches!(name.as_str(), "connection" | "proxy-connection" | "keep-alive" | "transfer-encoding" | "upgrade")
                || (name == "te" && value != "trailers")
                || (trailers && name == "content-length")
            {
                return Err(invalid());
            }
        }
    }
    if trailers {
        return Ok(());
    }
    if role == Http2Role::Server {
        let method = pseudo.get(":method").copied().ok_or_else(invalid)?;
        if method == "CONNECT" {
            if !pseudo.contains_key(":authority") || pseudo.contains_key(":scheme") || pseudo.contains_key(":path") {
                return Err(invalid());
            }
        } else if !matches!(pseudo.get(":scheme").copied(), Some("http" | "https")) || !pseudo.get(":path").is_some_and(|path| path.starts_with('/') || *path == "*") {
            return Err(invalid());
        }
        if !pseudo.get(":authority").is_some_and(|value| !value.is_empty()) && !fields.iter().any(|(name, value)| name == "host" && !value.is_empty()) {
            return Err(invalid());
        }
    } else if !pseudo.get(":status").is_some_and(|status| {
        status.len() == 3 && status.bytes().all(|b| b.is_ascii_digit()) && *status != "101" && status.parse::<u16>().is_ok_and(|status| (100..=599).contains(&status))
    }) {
        return Err(invalid());
    }
    Ok(())
}

impl Http2Session {
    pub(super) fn begin_headers(
        &mut self,
        id: i32,
        end: bool,
        bytes: &[u8],
        complete: bool,
    ) -> Result<()> {
        let mut reject_code = None;
        if !self.streams.contains_key(&id) {
            if id <= 0 || id & 1 == 0 {
                return Err(error(1, "invalid new peer stream"));
            }
            if self.role == Http2Role::Client {
                if id >= self.next_id {
                    return Err(error(1, "unsolicited peer stream"));
                }
                reject_code = Some(0);
            } else if id <= self.last_peer {
                reject_code = Some(0);
            } else {
                self.last_peer = id;
                if self.sent_goaway.is_some_and(|last| id > last)
                    || self.streams.values().filter(|s| !s.close_emitted).count()
                        >= self.local.max_concurrent_streams as usize
                {
                    reject_code = Some(7);
                } else {
                    self.streams.insert(id, Stream::new(self.local, self.peer));
                }
            }
        }
        self.headers = Some(HeaderBlock {
            id,
            end,
            bytes: bytes.to_vec(),
            reject_code,
        });
        self.check_header_block_size()?;
        if complete {
            self.finish_headers()?;
        }
        Ok(())
    }
    pub(super) fn check_header_block_size(&self) -> Result<()> {
        let limit = (self.local.max_header_list_size as usize)
            .saturating_mul(4)
            .saturating_add(4096);
        if self.headers.as_ref().is_some_and(|h| h.bytes.len() > limit) {
            return Err(error(11, "encoded header block exceeds budget"));
        }
        Ok(())
    }
    pub(super) fn finish_headers(&mut self) -> Result<()> {
        let block = self.headers.take().expect("pending header block");
        let decoded = self.decoder.decode(&block.bytes);
        if let Some(code) = block.reject_code {
            match decoded {
                Ok(_) | Err(ProtoError::HeaderListTooLarge { .. }) => {
                    if code != 0 {
                        self.queue_frame(block.id, FrameType::RstStream, 0, &code.to_be_bytes());
                    }
                }
                Err(failure) => return Err(error(9, failure.to_string())),
            }
            return Ok(());
        }
        let fields = match decoded {
            Ok(fields) => fields,
            Err(ProtoError::HeaderListTooLarge { .. }) => {
                self.events.push_back(SessionEvent::BeginHeaders {
                    stream_id: block.id,
                });
                self.stream_failure(block.id, true, "decoded header list exceeds budget");
                return Ok(());
            }
            Err(failure) => return Err(error(9, failure.to_string())),
        };
        let stream = self.streams.get_mut(&block.id).expect("header stream");
        if stream.reset || stream.close_emitted {
            return Ok(());
        }
        if stream.remote_closed {
            self.stream_failure(block.id, false, "headers after remote closure");
            return Ok(());
        }
        let trailers = stream.initial;
        if let Err(failure) = validate_headers(&fields, self.role, trailers, block.id as u32) {
            self.events.push_back(SessionEvent::BeginHeaders {
                stream_id: block.id,
            });
            self.stream_failure(block.id, false, failure.to_string());
            return Ok(());
        }
        let informational = self.role == Http2Role::Client
            && !trailers
            && fields
                .iter()
                .any(|(key, value)| key == ":status" && value.starts_with('1'));
        if (informational && block.end) || (trailers && !block.end) {
            self.stream_failure(block.id, false, "invalid header END_STREAM");
            return Ok(());
        }
        if !informational {
            stream.initial = true;
        }
        if block.end {
            stream.remote_closed = true;
        }
        self.last_processed = self.last_processed.max(block.id);
        self.events.push_back(SessionEvent::BeginHeaders {
            stream_id: block.id,
        });
        for (name, value) in fields {
            self.events.push_back(SessionEvent::Header {
                stream_id: block.id,
                name: name.into_bytes(),
                value: value.into_bytes(),
            });
        }
        self.events.push_back(SessionEvent::FrameRecv {
            stream_id: block.id,
            frame_type: FrameType::Headers,
            flags: u8::from(block.end),
        });
        self.close_if_complete(block.id);
        Ok(())
    }
}
