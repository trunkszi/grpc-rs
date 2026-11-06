//! Native HTTP/2 session integration with in-memory, fragmented byte delivery.

use std::time::Duration;

use grpc_core::{
    Code, CompressionAlgorithm as Encoding, Metadata, MetadataValue, Status,
    session::{GrpcClientSession, GrpcServerSession, SessionConfig},
};

fn exchange(client: &mut GrpcClientSession, server: &mut GrpcServerSession) -> Result<(), Status> {
    for _ in 0..128 {
        let up = client.mem_send()?.to_vec();
        for chunk in up.chunks(7) {
            assert_eq!(server.mem_recv(chunk)?, chunk.len());
        }
        let down = server.mem_send()?.to_vec();
        for chunk in down.chunks(11) {
            assert_eq!(client.mem_recv(chunk)?, chunk.len());
        }
        if up.is_empty() && down.is_empty() {
            return Ok(());
        }
    }
    Err(Status::internal("in-memory session did not quiesce"))
}
fn pair() -> Result<(GrpcClientSession, GrpcServerSession), Status> {
    Ok((
        GrpcClientSession::new(SessionConfig::default())?,
        GrpcServerSession::new(SessionConfig::default())?,
    ))
}
fn metadata() -> Metadata {
    let mut metadata = Metadata::new();
    metadata.append_ascii("trace", "first").unwrap();
    metadata.append_ascii("trace", "second").unwrap();
    metadata
        .append_binary("context-bin", &[0, 255, 128])
        .unwrap();
    metadata
}

#[test]
#[rustfmt::skip]
fn integration_lifecycle_unary_metadata_alignment_and_trailers() -> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    assert!(client.want_read() && client.want_write());
    let headers = metadata();
    let id = client.start_request("localhost", "/example.Echo/Call", &headers, Some(Duration::from_millis(123)), Encoding::Identity, false)?;
    client.send_message(id, b"request", Encoding::Identity)?;
    client.half_close(id)?;
    exchange(&mut client, &mut server)?;
    let incoming = server.pop_incoming_request().expect("request");
    assert_eq!(incoming.stream_id, id);
    assert_eq!(incoming.path, "/example.Echo/Call");
    assert_eq!(incoming.authority, "localhost");
    assert_eq!(incoming.timeout, Some(Duration::from_millis(123)));
    assert_eq!(incoming.initial_metadata, headers);
    assert_eq!(server.pop_message(id)?.expect("body").payload(), b"request");
    assert!(server.is_remote_end_stream(id)?);
    server.send_initial_metadata(id, &headers, Encoding::Identity)?;
    server.send_message(id, b"response", Encoding::Identity)?;
    server.finish_response(id, &Status::new(Code::Ok, "done % caf\u{e9}"), &headers)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(client.pop_initial_metadata(id)?, Some(headers.clone()));
    let body = client.pop_message(id)?.expect("response");
    assert_eq!(body.payload(), b"response");
    assert_eq!(body.payload().as_ptr() as usize % 8, 0);
    assert_eq!(client.pop_message(id)?.map(|m| m.len()), None);
    let (status, trailing) = client.take_trailers(id)?.expect("trailers");
    assert_eq!(status, Status::new(Code::Ok, "done % caf\u{e9}"));
    assert_eq!(trailing, headers);
    assert!(client.is_remote_end_stream(id)?);
    server.submit_goaway(0)?;
    assert!(server.is_draining());
    exchange(&mut client, &mut server)?;
    assert_eq!(client.goaway_received(), Some((id, 0)));
    assert!(client.is_draining() && !server.want_write());
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_streaming_compression_and_interleaved_streams() -> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    let encodings = [
        Encoding::Zstd,
        Encoding::Lz4,
        Encoding::Gzip,
        Encoding::Deflate,
        Encoding::Snappy,
        Encoding::Identity,
    ];
    let mut ids = Vec::new();
    for &encoding in &encodings {
        let id = client.start_request("localhost", "/example.Echo/Stream", &Metadata::new(), None, encoding, false)?;
        client.send_message(id, b"tiny", encoding)?;
        ids.push(id);
    }
    exchange(&mut client, &mut server)?;
    for (&id, &encoding) in ids.iter().zip(&encodings) {
        let incoming = server.pop_incoming_request().expect("request");
        assert_eq!(incoming.stream_id, id);
        assert_eq!(incoming.request_encoding, encoding);
        assert_eq!(server.pop_message(id)?.expect("tiny").payload(), b"tiny");
        assert!(!server.is_remote_end_stream(id)?);
        server.send_initial_metadata(id, &Metadata::new(), encoding)?;
        server.send_message(id, b"first", encoding)?;
    }
    exchange(&mut client, &mut server)?;
    let large = vec![b'x'; 100_000];
    for (&id, &encoding) in ids.iter().zip(&encodings) {
        assert_eq!(
            client.pop_message(id)?.expect("first response").payload(),
            b"first"
        );
        client.send_message(id, &large, encoding)?;
        client.send_message(id, b"", encoding)?;
        client.half_close(id)?;
    }
    exchange(&mut client, &mut server)?;
    for (&id, &encoding) in ids.iter().zip(&encodings) {
        assert_eq!(
            server.pop_message(id)?.expect("large request").payload(),
            large
        );
        assert!(server.pop_message(id)?.expect("empty request").is_empty());
        assert!(server.is_remote_end_stream(id)?);
        server.send_message(id, &large, encoding)?;
        server.finish_response(id, &Status::new(Code::Ok, ""), &Metadata::new())?;
    }
    exchange(&mut client, &mut server)?;
    for id in ids {
        let message = client.pop_message(id)?.expect("large response");
        assert_eq!(message.payload(), large);
        assert_eq!(message.payload().as_ptr() as usize % 8, 0);
        assert_eq!(
            client.take_trailers(id)?.expect("status").0.code(),
            Code::Ok
        );
    }
    Ok(())
}

#[test]
fn integration_scenario_trailers_only_and_remote_error() -> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    let id = client.start_request(
        "localhost",
        "/example.Echo/Denied",
        &Metadata::new(),
        None,
        Encoding::Identity,
        true,
    )?;
    exchange(&mut client, &mut server)?;
    assert_eq!(
        server.pop_incoming_request().expect("request").stream_id,
        id
    );
    server.finish_response(
        id,
        &Status::permission_denied("not allowed: %\ncaf\u{e9}"),
        &metadata(),
    )?;
    exchange(&mut client, &mut server)?;
    assert!(client.pop_message(id)?.is_none());
    let (status, trailers) = client.take_trailers(id)?.expect("trailers only");
    assert_eq!(
        status,
        Status::permission_denied("not allowed: %\ncaf\u{e9}")
    );
    assert_eq!(
        trailers.get_all("context-bin").next(),
        Some(&MetadataValue::Binary(vec![0, 255, 128]))
    );
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_stream_limits_and_outbound_lifecycle() -> Result<(), Status> {
    let mut client = GrpcClientSession::new(SessionConfig {
        max_active_streams: 1,
        ..SessionConfig::default()
    })?;
    let id = client.start_request("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, false)?;
    assert_eq!(
        client
            .start_request(
                "localhost",
                "/example.Echo/Call",
                &Metadata::new(),
                None,
                Encoding::Identity,
                false
            )
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    client.half_close(id)?;
    assert!(
        client
            .send_message(id, b"late", Encoding::Identity)
            .is_err()
    );
    assert!(client.half_close(id).is_err());
    assert!(
        client
            .send_message(99, b"unknown", Encoding::Identity)
            .is_err()
    );
    Ok(())
}

#[rustfmt::skip]
fn raw_pair(end_stream: bool) -> Result<(GrpcClientSession, grpc_core::http2::Http2Session, i32), Box<dyn std::error::Error>>
{
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    let mut server = grpc_core::http2::Http2Session::new(grpc_core::http2::Http2Role::Server, grpc_core::http2::Http2Settings::default())?;
    let id = client.start_request("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, end_stream)?;
    for _ in 0..16 {
        let data = client.mem_send()?.to_vec();
        if data.is_empty() {
            break;
        }
        assert_eq!(server.mem_recv(&data)?, data.len());
    }
    Ok((client, server, id))
}
fn deliver_raw(
    client: &mut GrpcClientSession,
    server: &mut grpc_core::http2::Http2Session,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..32 {
        let data = server.mem_send()?.to_vec();
        if data.is_empty() {
            return Ok(());
        }
        assert_eq!(client.mem_recv(&data)?, data.len());
    }
    panic!("native server did not quiesce");
}

#[test]
fn integration_scenario_http_fallback_without_grpc_status() -> Result<(), Box<dyn std::error::Error>>
{
    for (http, code) in [
        (400, Code::Internal),
        (401, Code::Unauthenticated),
        (403, Code::PermissionDenied),
        (404, Code::Unimplemented),
        (429, Code::Unavailable),
        (502, Code::Unavailable),
        (503, Code::Unavailable),
        (504, Code::Unavailable),
        (500, Code::Unknown),
        (200, Code::Unknown),
    ] {
        let (mut client, mut server, id) = raw_pair(true)?;
        let status = http.to_string();
        server.submit_response(
            id,
            &[
                grpc_core::http2::HeaderField::new(b":status", status.as_bytes()),
                grpc_core::http2::HeaderField::new(b"content-type", b"application/grpc"),
            ],
            None,
        )?;
        deliver_raw(&mut client, &mut server)?;
        assert_eq!(
            client.take_trailers(id)?.expect("fallback").0.code(),
            code,
            "HTTP {http}"
        );
    }
    Ok(())
}

#[test]
fn integration_scenario_trailers_only_canonical_status_overrides_http_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut client, mut server, id) = raw_pair(true)?;
    server.submit_response(
        id,
        &[
            grpc_core::http2::HeaderField::new(b":status", b"503"),
            grpc_core::http2::HeaderField::new(b"content-type", b"application/grpc+proto"),
            grpc_core::http2::HeaderField::new(b"grpc-status", b"7"),
            grpc_core::http2::HeaderField::new(b"grpc-message", b"denied%20%25%0A"),
        ],
        None,
    )?;
    deliver_raw(&mut client, &mut server)?;
    assert_eq!(
        client.take_trailers(id)?.expect("explicit status").0,
        Status::permission_denied("denied %\n")
    );
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_cancel_does_not_corrupt_other_streams() -> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    let a = client.start_request("localhost", "/example.Echo/Cancel", &Metadata::new(), None, Encoding::Identity, false)?;
    let b = client.start_request("localhost", "/example.Echo/Keep", &Metadata::new(), None, Encoding::Identity, true)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(server.pop_incoming_request().expect("first").stream_id, a);
    assert_eq!(server.pop_incoming_request().expect("second").stream_id, b);
    client.cancel_stream(a, 8)?;
    exchange(&mut client, &mut server)?;
    assert!(server.send_message(a, b"late", Encoding::Identity).is_err());
    server.finish_response(b, &Status::new(Code::Ok, ""), &Metadata::new())?;
    exchange(&mut client, &mut server)?;
    assert_eq!(
        client
            .take_trailers(b)?
            .expect("unaffected response")
            .0
            .code(),
        Code::Ok
    );
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_outbound_budget_rejection_preserves_accepted_body() -> Result<(), Status> {
    let config = SessionConfig {
        per_stream_byte_budget: 2048,
        max_header_list_size: 1024,
        ..SessionConfig::default()
    };
    let mut client = GrpcClientSession::new(config)?;
    let mut server = GrpcServerSession::new(SessionConfig::default())?;
    let id = client.start_request("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, false)?;
    client.send_message(id, b"accepted", Encoding::Identity)?;
    assert_eq!(
        client
            .send_message(id, &[0; 4096], Encoding::Identity)
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    client.half_close(id)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(
        server.pop_message(id)?.expect("accepted prefix").payload(),
        b"accepted"
    );
    assert!(server.pop_message(id)?.is_none());
    Ok(())
}

#[test]
fn integration_scenario_invalid_settings_fail_before_native_session_creation() {
    for config in [
        SessionConfig {
            max_frame_size: 1,
            ..SessionConfig::default()
        },
        SessionConfig {
            initial_window_size: u32::MAX,
            ..SessionConfig::default()
        },
        SessionConfig {
            max_active_streams: 0,
            ..SessionConfig::default()
        },
    ] {
        assert!(GrpcClientSession::new(config).is_err());
    }
}

#[test]
fn integration_scenario_application_metadata_cannot_override_protocol_headers() -> Result<(), Status>
{
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    for (key, value) in [
        ("content-type", "text/html"),
        ("te", "chunked"),
        ("user-agent", "injected"),
    ] {
        let mut headers = Metadata::new();
        headers.append_ascii(key, value).unwrap();
        assert!(
            client
                .start_request(
                    "localhost",
                    "/example.Echo/Call",
                    &headers,
                    None,
                    Encoding::Identity,
                    true
                )
                .is_err(),
            "accepted reserved protocol field {key}"
        );
    }
    Ok(())
}

#[test]
fn integration_scenario_missing_content_type_and_truncated_envelopes_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    for (content, body) in [
        (b"text/html".as_slice(), b"".as_slice()),
        (b"application/grpc", &[0, 0, 0, 0, 3, b'a'][..]),
        (b"application/grpc", &[2, 0, 0, 0, 0][..]),
    ] {
        let (mut client, mut server, id) = raw_pair(true)?;
        server.submit_response(
            id,
            &[
                grpc_core::http2::HeaderField::new(b":status", b"200"),
                grpc_core::http2::HeaderField::new(b"content-type", content),
            ],
            Some(grpc_core::http2::DataProvider::from_vec(body.to_vec())),
        )?;
        let mut observed_error = false;
        for _ in 0..32 {
            let wire = server.mem_send()?.to_vec();
            if wire.is_empty() {
                break;
            }
            if let Err(status) = client.mem_recv(&wire) {
                assert_ne!(status.code(), Code::Ok);
                observed_error = true;
                break;
            }
        }
        if !observed_error {
            assert_ne!(
                client.take_trailers(id)?.expect("failed RPC").0.code(),
                Code::Ok
            );
        }
    }
    Ok(())
}

#[test]
fn integration_scenario_received_goaway_keeps_accepted_stream_and_rejects_new_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut client, mut server, id) = raw_pair(true)?;
    server.submit_goaway(id, 0, b"drain")?;
    deliver_raw(&mut client, &mut server)?;
    assert!(
        client
            .start_request(
                "localhost",
                "/example.Echo/New",
                &Metadata::new(),
                None,
                Encoding::Identity,
                true
            )
            .is_err()
    );
    server.submit_response(
        id,
        &[
            grpc_core::http2::HeaderField::new(b":status", b"200"),
            grpc_core::http2::HeaderField::new(b"content-type", b"application/grpc"),
            grpc_core::http2::HeaderField::new(b"grpc-status", b"0"),
        ],
        None,
    )?;
    deliver_raw(&mut client, &mut server)?;
    assert_eq!(
        client.take_trailers(id)?.expect("accepted stream").0.code(),
        Code::Ok
    );
    Ok(())
}

struct ShortWriter {
    bytes: Vec<u8>,
    calls: usize,
}
impl std::io::Write for ShortWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.calls += 1;
        if self.calls % 3 == 0 {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        let len = bytes.len().min(5);
        self.bytes.extend_from_slice(&bytes[..len]);
        Ok(len)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl std::io::Read for ShortWriter {
    fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
        Err(std::io::ErrorKind::WouldBlock.into())
    }
}
impl grpc_core::transport::SyncTransport for ShortWriter {
    fn set_nonblocking(&mut self, _: bool) -> std::io::Result<()> {
        Ok(())
    }
    fn set_read_timeout(&mut self, _: Option<Duration>) -> std::io::Result<()> {
        Ok(())
    }
    fn set_write_timeout(&mut self, _: Option<Duration>) -> std::io::Result<()> {
        Ok(())
    }
    fn read_timeout(&self) -> std::io::Result<Option<Duration>> {
        Ok(None)
    }
    fn write_timeout(&self) -> std::io::Result<Option<Duration>> {
        Ok(None)
    }
    fn peer_endpoint(&self) -> std::io::Result<String> {
        Ok("short-write-test".into())
    }
}

#[test]
fn integration_scenario_transport_short_writes_preserve_bytes_across_receive()
-> Result<(), Box<dyn std::error::Error>> {
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    let mut server = grpc_core::http2::Http2Session::new(
        grpc_core::http2::Http2Role::Server,
        grpc_core::http2::Http2Settings::default(),
    )?;
    let id = client.start_request(
        "localhost",
        "/example.Echo/Call",
        &Metadata::new(),
        None,
        Encoding::Identity,
        false,
    )?;
    client.send_message(id, b"body", Encoding::Identity)?;
    client.half_close(id)?;
    let mut writer = ShortWriter {
        bytes: Vec::new(),
        calls: 0,
    };
    // The peer's SETTINGS can arrive while a serialized client frame is partially written.
    let settings = server.mem_send()?.to_vec();
    for attempt in 0..256 {
        if let Err(error) = client.flush_to_transport(&mut writer) {
            assert_eq!(error.code(), Code::Unavailable);
        }
        if attempt == 0 {
            assert_eq!(client.mem_recv(&settings)?, settings.len());
        }
    }
    assert!(
        writer
            .bytes
            .starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
    );
    assert_eq!(server.mem_recv(&writer.bytes)?, writer.bytes.len());
    let mut body = Vec::new();
    let mut ended = false;
    while let Some(event) = server.next_event() {
        match event {
            grpc_core::http2::SessionEvent::DataChunk {
                stream_id, data, ..
            } if stream_id == id => body.extend(data),
            grpc_core::http2::SessionEvent::FrameRecv {
                stream_id, flags, ..
            } if stream_id == id && flags & 1 != 0 => ended = true,
            grpc_core::http2::SessionEvent::Error { message, .. } => panic!(
                "native protocol error: {}",
                String::from_utf8_lossy(&message)
            ),
            _ => {}
        }
    }
    assert_eq!(
        body,
        grpc_core::AlignedMessageBuf::new(b"body", false)?.wire_bytes()
    );
    assert!(ended);
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_lifecycle_closed_slot_reclamation_reuses_capacity() -> Result<(), Status> {
    let config = SessionConfig {
        max_active_streams: 1,
        ..SessionConfig::default()
    };
    let mut client = GrpcClientSession::new(config.clone())?;
    let mut server = GrpcServerSession::new(config)?;
    for _ in 0..3 {
        let id = client.start_request("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, true)?;
        exchange(&mut client, &mut server)?;
        assert_eq!(
            server.pop_incoming_request().expect("request").stream_id,
            id
        );
        server.finish_response(id, &Status::new(Code::Ok, ""), &Metadata::new())?;
        exchange(&mut client, &mut server)?;
        assert_eq!(
            client.take_trailers(id)?.expect("completion").0.code(),
            Code::Ok
        );
        assert!(client.take_trailers(id)?.is_none());
        client.remove_closed_stream(id)?;
        server.remove_closed_stream(id)?;
    }
    Ok(())
}

#[test]
fn integration_scenario_decompression_limit_isolated_from_other_streams() -> Result<(), Status> {
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    let mut server = GrpcServerSession::new(SessionConfig {
        max_inbound_message_size: 64,
        ..SessionConfig::default()
    })?;
    let large = client.start_request(
        "localhost",
        "/example.Echo/Large",
        &Metadata::new(),
        None,
        Encoding::Zstd,
        false,
    )?;
    let small = client.start_request(
        "localhost",
        "/example.Echo/Small",
        &Metadata::new(),
        None,
        Encoding::Identity,
        true,
    )?;
    client.send_message(large, &[0; 4096], Encoding::Zstd)?;
    client.half_close(large)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(
        server
            .take_trailers(large)?
            .expect("limit failure")
            .0
            .code(),
        Code::ResourceExhausted
    );
    let requests: Vec<_> = std::iter::from_fn(|| server.pop_incoming_request()).collect();
    assert!(requests.iter().any(|r| r.stream_id == small));
    server.finish_response(small, &Status::new(Code::Ok, ""), &Metadata::new())?;
    exchange(&mut client, &mut server)?;
    assert_eq!(
        client
            .take_trailers(small)?
            .expect("unaffected stream")
            .0
            .code(),
        Code::Ok
    );
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_negotiated_encoding_mismatch_rejects_without_losing_prefix()
-> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    let id = client.start_request("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Zstd, false)?;
    client.send_message(id, b"accepted", Encoding::Zstd)?;
    assert_eq!(
        client
            .send_message(id, &[0; 2048], Encoding::Gzip)
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    client.half_close(id)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(
        server.pop_message(id)?.expect("accepted").payload(),
        b"accepted"
    );
    assert!(server.pop_message(id)?.is_none());
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_lifecycle_reclamation_waits_for_native_trailer_serialization() -> Result<(), Status>
{
    let (mut client, mut server) = pair()?;
    let id = client.start_request("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, true)?;
    exchange(&mut client, &mut server)?;
    server.send_message(id, b"retained", Encoding::Identity)?;
    server.finish_response(id, &Status::new(Code::Ok, ""), &Metadata::new())?;
    assert!(server.remove_closed_stream(id).is_err());
    exchange(&mut client, &mut server)?;
    assert_eq!(
        client.pop_message(id)?.expect("body").payload(),
        b"retained"
    );
    assert_eq!(
        client.take_trailers(id)?.expect("trailers").0.code(),
        Code::Ok
    );
    assert!(server.stream_outbound_complete(id));
    server.remove_closed_stream(id)?;
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_lifecycle_zero_error_reset_closes_unfinished_server_request() -> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    let id = client.start_request("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, false)?;
    exchange(&mut client, &mut server)?;
    client.cancel_stream(id, 0)?;
    exchange(&mut client, &mut server)?;
    assert!(server.pop_incoming_request().is_none());
    assert_ne!(
        server.take_trailers(id)?.expect("reset status").0.code(),
        Code::Ok
    );
    server.remove_closed_stream(id)?;
    let (mut client, mut peer, id) = raw_pair(false).map_err(|e| Status::internal(e.to_string()))?;
    peer.submit_rst_stream(id, 0).map_err(|e| Status::internal(e.to_string()))?;
    deliver_raw(&mut client, &mut peer).map_err(|e| Status::internal(e.to_string()))?;
    assert!(client.send_message(id, b"late", Encoding::Identity).is_err());
    assert_eq!(client.take_trailers(id)?.expect("reset outcome").0.code(), Code::Unknown);
    client.remove_closed_stream(id)?;
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_cancel_before_serialization_keeps_connection_usable() -> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    let a = client.start_request("localhost", "/example.Echo/Cancel", &Metadata::new(), None, Encoding::Identity, false)?;
    let b = client.start_request("localhost", "/example.Echo/Keep", &Metadata::new(), None, Encoding::Identity, true)?;
    client.cancel_stream(a, 8)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(server.pop_incoming_request().expect("surviving request").stream_id, b);
    server.finish_response(b, &Status::new(Code::Ok, ""), &Metadata::new())?;
    exchange(&mut client, &mut server)?;
    assert_eq!(client.take_trailers(b)?.expect("completion").0.code(), Code::Ok);
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_informational_headers_do_not_replace_final_headers() -> Result<(), Box<dyn std::error::Error>> {
    let (mut client, mut server, id) = raw_pair(true)?;
    server.submit_headers(0, id, &[grpc_core::http2::HeaderField::new(b":status", b"100")])?;
    deliver_raw(&mut client, &mut server)?;
    assert!(client.pop_initial_metadata(id)?.is_none());
    let body = grpc_core::AlignedMessageBuf::new(b"final body", false)?;
    server.submit_response(id, &[grpc_core::http2::HeaderField::new(b":status", b"200"), grpc_core::http2::HeaderField::new(b"content-type", b"application/grpc")], Some(grpc_core::http2::DataProvider::from_vec(body.wire_bytes().to_vec())))?;
    deliver_raw(&mut client, &mut server)?;
    assert_eq!(client.pop_message(id)?.expect("final body").payload(), b"final body");
    assert_eq!(client.take_trailers(id)?.expect("missing status fallback").0.code(), Code::Unknown);
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_message_count_limit_is_resource_exhausted() -> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    let id = client.start_request("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, false)?;
    for _ in 0..1025 { client.send_message(id, b"", Encoding::Identity)?; }
    client.half_close(id)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(server.take_trailers(id)?.expect("queue limit").0.code(), Code::ResourceExhausted);
    Ok(())
}

#[test]
#[rustfmt::skip]
fn integration_scenario_codec_content_types_are_advertised_and_received() -> Result<(), Status> {
    let (mut client, mut server) = pair()?;
    for content_type in ["application/grpc+proto", "application/grpc+fory", "application/grpc+capnp"] {
        let id = client.start_request_with_content_type("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, true, content_type)?;
        exchange(&mut client, &mut server)?;
        assert_eq!(server.pop_incoming_request().expect("typed request").content_type, content_type);
        for invalid in ["text/html", "application/grpcx", "application/grpc+", "application/grpc+proto\r\nx: y"] {
            assert_eq!(client.start_request_with_content_type("localhost", "/example.Echo/Call", &Metadata::new(), None, Encoding::Identity, true, invalid).unwrap_err().code(), Code::InvalidArgument);
            assert_eq!(server.send_initial_metadata_with_content_type(id, &Metadata::new(), Encoding::Identity, invalid).unwrap_err().code(), Code::InvalidArgument);
        }
        server.send_initial_metadata_with_content_type(id, &Metadata::new(), Encoding::Identity, content_type)?;
        server.send_message(id, b"codec body", Encoding::Identity)?;
        server.finish_response(id, &Status::new(Code::Ok, ""), &Metadata::new())?;
        exchange(&mut client, &mut server)?;
        assert_eq!(client.pop_message(id)?.expect("typed response").payload(), b"codec body");
        assert_eq!(client.take_trailers(id)?.expect("typed response").0.code(), Code::Ok);
    }
    let id = client.start_request("localhost", "/example.Echo/Default", &Metadata::new(), None, Encoding::Identity, true)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(server.pop_incoming_request().expect("default request").content_type, "application/grpc");
    server.send_initial_metadata(id, &Metadata::new(), Encoding::Identity)?;
    Ok(())
}
