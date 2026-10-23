//! Integration scenarios drive the streaming engine through raw HTTP/2 frames.
use grpc_core::http2::{
    DataProvider, HeaderField, Http2Role, Http2Session, Http2Settings, SessionEvent,
};
use nexora_proto::{
    BytesMut,
    http2::{CLIENT_PREFACE, HpackEncoder},
};

fn frame(kind: u8, flags: u8, id: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    bytes.extend_from_slice(&[kind, flags]);
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}
fn settings(entries: &[(u16, u32)]) -> Vec<u8> {
    let mut payload = Vec::new();
    for (id, value) in entries {
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&value.to_be_bytes());
    }
    frame(4, 0, 0, &payload)
}
fn request() -> [HeaderField<'static>; 4] {
    [
        HeaderField::new(b":method", b"POST"),
        HeaderField::new(b":scheme", b"http"),
        HeaderField::new(b":authority", b"localhost"),
        HeaderField::new(b":path", b"/test"),
    ]
}
fn drain(session: &mut Http2Session) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut wire = Vec::new();
    loop {
        let bytes = session.mem_send()?;
        if bytes.is_empty() {
            break;
        }
        wire.extend_from_slice(bytes);
    }
    Ok(wire)
}
fn raw_client(config: Http2Settings) -> Result<Http2Session, Box<dyn std::error::Error>> {
    let mut client = Http2Session::new(Http2Role::Client, config)?;
    drain(&mut client)?;
    client.mem_recv(&settings(&[]))?;
    drain(&mut client)?;
    Ok(client)
}
fn encoded(encoder: &mut HpackEncoder, values: &[(&str, &str)]) -> Vec<u8> {
    let mut out = BytesMut::new();
    encoder.encode(
        &values
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<_>>(),
        &mut out,
    );
    out.as_slice().to_vec()
}
#[test]
fn integration_rfc_huffman_request_survives_every_byte_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = Http2Session::new(Http2Role::Server, Http2Settings::default())?;
    // RFC 7541 C.4.1: GET http://www.example.com/ with Huffman authority.
    let block = [
        0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90,
        0xf4, 0xff,
    ];
    let bytes = [
        CLIENT_PREFACE.to_vec(),
        settings(&[]),
        frame(1, 5, 1, &block),
    ]
    .concat();
    for byte in bytes {
        assert_eq!(server.mem_recv(&[byte])?, 1);
    }
    let mut authority = None;
    let mut ended = false;
    while let Some(event) = server.next_event() {
        match event {
            SessionEvent::Header { name, value, .. } if name == b":authority" => {
                authority = Some(value)
            }
            SessionEvent::FrameRecv {
                stream_id: 1,
                flags,
                ..
            } if flags & 1 != 0 => ended = true,
            _ => {}
        }
    }
    assert_eq!(authority.as_deref(), Some(b"www.example.com".as_slice()));
    assert!(ended);
    // A server never advertises ENABLE_PUSH, which is a client-only setting.
    let wire = drain(&mut server)?;
    let size = (usize::from(wire[0]) << 16) | (usize::from(wire[1]) << 8) | usize::from(wire[2]);
    assert!(
        wire[9..9 + size]
            .as_chunks::<6>()
            .0
            .iter()
            .all(|s| s[..2] != [0, 2])
    );
    Ok(())
}
#[test]
fn integration_scenario_flow_windows_resume_large_incremental_body()
-> Result<(), Box<dyn std::error::Error>> {
    let mut client = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    let mut server = Http2Session::new(
        Http2Role::Server,
        Http2Settings {
            initial_window_size: 128,
            ..Http2Settings::default()
        },
    )?;
    server.mem_recv(&drain(&mut client)?)?;
    client.mem_recv(&drain(&mut server)?)?;
    server.mem_recv(&drain(&mut client)?)?;
    let payload: Vec<_> = (0..100_000).map(|n| (n % 251) as u8).collect();
    let id = client.submit_request(&request(), Some(DataProvider::from_vec(payload.clone())))?;
    let first = drain(&mut client)?;
    assert!(
        !client.want_write(),
        "peer window must stop body scheduling"
    );
    server.mem_recv(&first)?;
    let mut received = Vec::new();
    let mut ended = false;
    for _ in 0..2000 {
        while let Some(event) = server.next_event() {
            match event {
                SessionEvent::DataChunk { stream_id, data } => {
                    server.consume(stream_id, data.len())?;
                    received.extend_from_slice(&data);
                }
                SessionEvent::FrameRecv {
                    stream_id,
                    frame_type: grpc_core::http2::FrameType::Data,
                    flags,
                } if stream_id == id && flags & 1 != 0 => ended = true,
                _ => {}
            }
        }
        if ended {
            break;
        }
        client.mem_recv(&drain(&mut server)?)?;
        server.mem_recv(&drain(&mut client)?)?;
    }
    assert!(ended);
    assert_eq!(received, payload);
    Ok(())
}
#[test]
fn integration_scenario_header_limit_retains_sibling_hpack_context()
-> Result<(), Box<dyn std::error::Error>> {
    let mut client = raw_client(Http2Settings {
        max_header_list_size: 100,
        ..Http2Settings::default()
    })?;
    let rejected = client.submit_request(&request(), None)?;
    let sibling = client.submit_request(&request(), None)?;
    drain(&mut client)?;
    let mut encoder = HpackEncoder::default();
    let large = encoded(
        &mut encoder,
        &[
            (":status", "200"),
            ("x-history", "retained"),
            ("x-padding", &"a".repeat(100)),
        ],
    );
    client.mem_recv(&frame(1, 5, rejected as u32, &large))?;
    let mut limited = false;
    while let Some(event) = client.next_event() {
        if matches!(event, SessionEvent::StreamError { resource: true, stream_id, .. } if stream_id == rejected)
        {
            limited = true;
        }
    }
    assert!(limited);
    let next = encoded(
        &mut encoder,
        &[(":status", "200"), ("x-history", "retained")],
    );
    client.mem_recv(&frame(1, 5, sibling as u32, &next))?;
    let mut retained = false;
    while let Some(event) = client.next_event() {
        if let SessionEvent::Header {
            stream_id,
            name,
            value,
        } = event
        {
            retained |= stream_id == sibling && name == b"x-history" && value == b"retained";
        }
    }
    assert!(retained);
    Ok(())
}
#[test]
fn integration_scenario_continuation_excludes_interleaved_control_frames()
-> Result<(), Box<dyn std::error::Error>> {
    let mut client = raw_client(Http2Settings::default())?;
    let id = client.submit_request(&request(), None)?;
    drain(&mut client)?;
    client.mem_recv(&frame(1, 0, id as u32, &[0x88]))?;
    let failure = client
        .mem_recv(&frame(6, 0, 0, &[0; 8]))
        .expect_err("continuation cannot be interleaved");
    assert_eq!(failure.code, 1);
    assert_eq!(client.mem_recv(&[]).expect_err("failure is sticky").code, 1);
    Ok(())
}
#[test]
fn integration_scenario_settings_updates_preserve_intermediate_overflow_checks()
-> Result<(), Box<dyn std::error::Error>> {
    let mut client = raw_client(Http2Settings::default())?;
    let id = client.submit_request(&request(), Some(DataProvider::from_vec(vec![1])))?;
    client.mem_recv(&frame(
        8,
        0,
        id as u32,
        &(0x7fff_ffffu32 - 65_535).to_be_bytes(),
    ))?;
    let error = client
        .mem_recv(&settings(&[(4, 65_536), (4, 65_535)]))
        .expect_err("intermediate overflow is fatal");
    assert_eq!(error.code, 3);
    Ok(())
}
#[test]
fn integration_scenario_invalid_stream_frame_is_consumed_and_sibling_survives()
-> Result<(), Box<dyn std::error::Error>> {
    let mut client = raw_client(Http2Settings::default())?;
    let id = client.submit_request(&request(), None)?;
    let sibling = client.submit_request(&request(), None)?;
    drain(&mut client)?;
    let bytes = [
        frame(8, 0, id as u32, &[0; 4]),
        frame(1, 5, sibling as u32, &[0x88]),
    ]
    .concat();
    client.mem_recv(&bytes)?;
    let mut invalid = false;
    let mut valid = false;
    while let Some(event) = client.next_event() {
        invalid |= matches!(event, SessionEvent::StreamError { stream_id, .. } if stream_id == id);
        valid |= matches!(event, SessionEvent::Header { stream_id, .. } if stream_id == sibling);
    }
    assert!(invalid && valid);
    Ok(())
}
#[test]
fn integration_scenario_reset_and_goaway_preserve_unknown_wire_error_codes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut client = raw_client(Http2Settings::default())?;
    let id = client.submit_request(&request(), None)?;
    drain(&mut client)?;
    client.mem_recv(&frame(3, 0, id as u32, &0xaabb_ccddu32.to_be_bytes()))?;
    assert!(std::iter::from_fn(|| client.next_event()).any(|event| matches!(event, SessionEvent::StreamClose { stream_id, error_code: 0xaabb_ccdd } if stream_id == id)));
    let bytes = [0u32.to_be_bytes(), 0xaabb_ccddu32.to_be_bytes()].concat();
    client.mem_recv(&frame(7, 0, 0, &bytes))?;
    assert!(
        std::iter::from_fn(|| client.next_event()).any(|event| matches!(
            event,
            SessionEvent::GoAway {
                error_code: 0xaabb_ccdd,
                ..
            }
        ))
    );
    Ok(())
}
#[test]
fn integration_scenario_padding_returns_both_stream_and_connection_credit()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = Http2Session::new(
        Http2Role::Server,
        Http2Settings {
            initial_window_size: 8,
            ..Http2Settings::default()
        },
    )?;
    server.mem_recv(CLIENT_PREFACE)?;
    server.mem_recv(&settings(&[]))?;
    drain(&mut server)?;
    let mut encoder = HpackEncoder::default();
    let block = encoded(
        &mut encoder,
        &[
            (":method", "POST"),
            (":scheme", "http"),
            (":authority", "localhost"),
            (":path", "/test"),
        ],
    );
    server.mem_recv(&frame(1, 4, 1, &block))?;
    for _ in 0..5000 {
        server.mem_recv(&frame(0, 8, 1, &[2, b'a', b'b', b'c', b'd', b'e', 0, 0]))?;
        while let Some(event) = server.next_event() {
            if let SessionEvent::DataChunk { stream_id, data } = event {
                assert_eq!(data, b"abcde");
                server.consume(stream_id, data.len())?;
            }
        }
        drain(&mut server)?;
    }
    Ok(())
}

#[test]
fn integration_lifecycle_late_headers_keep_compression_context_after_reclamation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut client = raw_client(Http2Settings::default())?;
    let old = client.submit_request(&request(), None)?;
    let sibling = client.submit_request(&request(), None)?;
    drain(&mut client)?;
    client.mem_recv(&frame(3, 0, old as u32, &8u32.to_be_bytes()))?;
    client.release_stream(old)?;
    while client.next_event().is_some() {}
    let mut encoder = HpackEncoder::default();
    let late = encoded(&mut encoder, &[(":status", "200"), ("x-late", "retained")]);
    let next = encoded(&mut encoder, &[(":status", "200"), ("x-late", "retained")]);
    client.mem_recv(
        &[
            frame(1, 5, old as u32, &late),
            frame(1, 5, sibling as u32, &next),
        ]
        .concat(),
    )?;
    let mut retained = false;
    while let Some(event) = client.next_event() {
        if let SessionEvent::Header {
            stream_id,
            name,
            value,
        } = event
        {
            assert_eq!(stream_id, sibling);
            retained |= name == b"x-late" && value == b"retained";
        }
    }
    assert!(retained);
    assert!(
        drain(&mut client)?.is_empty(),
        "late headers cannot resurrect a closed stream"
    );
    Ok(())
}
