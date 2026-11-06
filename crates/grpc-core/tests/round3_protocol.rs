//! Adversarial wire boundaries for caller-driven HTTP/2 sessions.
use grpc_core::{
    Code, CompressionAlgorithm, GrpcClientSession, GrpcServerSession, Metadata, SessionConfig,
    http2::{DataProvider, HeaderField, Http2Role, Http2Session, Http2Settings},
};
use nexora_proto::{BytesMut, http2::HpackEncoder};

fn frame(kind: u8, flags: u8, id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_be_bytes()[1..].to_vec();
    out.extend_from_slice(&[kind, flags]);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}
fn encoded(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut block = BytesMut::new();
    HpackEncoder::default().encode(
        &fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<_>>(),
        &mut block,
    );
    block.as_slice().to_vec()
}
fn drain(session: &mut Http2Session) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    for _ in 0..100 {
        let bytes = session.mem_send()?;
        if bytes.is_empty() {
            return Ok(out);
        }
        out.extend_from_slice(bytes);
    }
    anyhow::bail!("session never quiesced")
}
fn request() -> [HeaderField<'static>; 4] {
    [
        HeaderField::new(b":method", b"POST"),
        HeaderField::new(b":scheme", b"http"),
        HeaderField::new(b":authority", b"localhost"),
        HeaderField::new(b":path", b"/echo"),
    ]
}

#[test]
fn zero_window_still_advertises_end_stream() -> anyhow::Result<()> {
    let mut client = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    drain(&mut client)?;
    client.mem_recv(&frame(4, 0, 0, &[0, 4, 0, 0, 0, 0]))?;
    drain(&mut client)?;
    client.submit_request(&request(), Some(DataProvider::from_vec(vec![])))?;
    assert_eq!(client.mem_send()?[3], 1);
    assert!(
        client.want_write(),
        "zero-byte END_STREAM needs no flow credit"
    );
    let end = client.mem_send()?.to_vec();
    assert_eq!((end.len(), end[3], end[4]), (9, 0, 1));
    assert!(!client.want_write());
    Ok(())
}

#[test]
fn malformed_content_type_rejects_requests() -> anyhow::Result<()> {
    for content_type in [
        "application/grpcgarbage",
        "application/grpc+",
        "application/grpc/invalid",
    ] {
        let mut server = GrpcServerSession::new(SessionConfig::default())?;
        let headers = encoded(&[
            (":method", "POST"),
            (":scheme", "http"),
            (":path", "/echo"),
            (":authority", "localhost"),
            ("te", "trailers"),
            ("content-type", content_type),
        ]);
        let mut bytes = nexora_proto::http2::CLIENT_PREFACE.to_vec();
        bytes.extend(frame(4, 0, 0, &[]));
        bytes.extend(frame(1, 5, 1, &headers));
        server.mem_recv(&bytes)?;
        assert!(
            server.pop_incoming_request().is_none(),
            "accepted request {content_type}"
        );
    }
    Ok(())
}

#[test]
fn malformed_content_type_rejects_responses() -> anyhow::Result<()> {
    for content_type in [
        "application/grpcgarbage",
        "application/grpc+",
        "application/grpc/invalid",
    ] {
        let mut client = GrpcClientSession::new(SessionConfig::default())?;
        let id = client.start_request(
            "localhost",
            "/echo",
            &Metadata::new(),
            None,
            CompressionAlgorithm::Identity,
            true,
        )?;
        let mut response = frame(4, 0, 0, &[]);
        response.extend(frame(
            1,
            5,
            id as u32,
            &encoded(&[
                (":status", "200"),
                ("content-type", content_type),
                ("grpc-status", "0"),
            ]),
        ));
        client.mem_recv(&response)?;
        assert_ne!(
            client
                .take_trailers(id)?
                .expect("terminal failure")
                .0
                .code(),
            Code::Ok,
            "accepted response {content_type}"
        );
    }
    Ok(())
}

#[test]
fn zero_window_body_waits_without_spinning() -> anyhow::Result<()> {
    let mut client = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    drain(&mut client)?;
    client.mem_recv(&frame(4, 0, 0, &[0, 4, 0, 0, 0, 0]))?;
    drain(&mut client)?;
    client.submit_request(&request(), Some(DataProvider::from_vec(vec![1])))?;
    client.mem_send()?;
    assert!(client.mem_send()?.is_empty());
    assert!(
        !client.want_write(),
        "nonempty bodies must wait for window credit"
    );
    client.mem_recv(&frame(8, 0, 1, &1u32.to_be_bytes()))?;
    assert!(client.want_write());
    assert_eq!(&client.mem_send()?[9..], &[1]);
    assert!(
        client.want_write(),
        "EOF must be probed after the last permitted byte"
    );
    assert_eq!(client.mem_send()?[4], 1);
    Ok(())
}

#[test]
fn negative_window_reopens_without_oversending() -> anyhow::Result<()> {
    let mut client = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    drain(&mut client)?;
    client.mem_recv(&frame(4, 0, 0, &[0, 4, 0, 0, 0, 4]))?;
    drain(&mut client)?;
    client.submit_request(&request(), Some(DataProvider::from_vec(vec![1; 6])))?;
    client.mem_send()?;
    assert_eq!(client.mem_send()?.len(), 13);
    client.mem_recv(&frame(4, 0, 0, &[0, 4, 0, 0, 0, 0]))?;
    assert_eq!(client.mem_send()?[3], 4);
    assert!(client.mem_send()?.is_empty());
    client.mem_recv(&frame(8, 0, 1, &3u32.to_be_bytes()))?;
    assert!(
        client.mem_send()?.is_empty(),
        "negative window cannot send bytes"
    );
    client.mem_recv(&frame(8, 0, 1, &2u32.to_be_bytes()))?;
    assert_eq!(client.mem_send()?.len(), 10);
    assert!(client.mem_send()?.is_empty());
    client.mem_recv(&frame(8, 0, 1, &1u32.to_be_bytes()))?;
    assert_eq!(client.mem_send()?.len(), 10);
    assert_eq!(client.mem_send()?[4], 1);
    Ok(())
}

#[test]
fn compression_rejects_every_truncated_prefix() -> anyhow::Result<()> {
    for algorithm in [
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Deflate,
        CompressionAlgorithm::Snappy,
    ] {
        for payload in [
            b"".as_slice(),
            b"bounded payload with repeated repeated repeated bytes".as_slice(),
        ] {
            let compressed = algorithm.compress(payload)?;
            assert_eq!(algorithm.decompress(&compressed, payload.len())?, payload);
            for cut in 0..compressed.len() {
                assert!(
                    algorithm
                        .decompress(&compressed[..cut], payload.len())
                        .is_err(),
                    "accepted {algorithm:?} prefix {cut}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn framing_empty_messages_survive_every_split() -> anyhow::Result<()> {
    use grpc_core::framing::{AlignedMessageBuf, LengthPrefixedFrame};
    let frame = AlignedMessageBuf::new(&[], false)?;
    for cut in 0..=5 {
        let mut decoder = LengthPrefixedFrame::new(0);
        let mut messages = decoder.push(&frame.wire_bytes()[..cut])?;
        messages.extend(decoder.push(&frame.wire_bytes()[cut..])?);
        decoder.finish()?;
        assert_eq!(messages.len(), 1);
        assert!(messages[0].payload().is_empty());
    }
    Ok(())
}

#[test]
fn idle_window_update_is_connection_error() -> anyhow::Result<()> {
    for increment in [0u32, 1] {
        let mut client = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
        drain(&mut client)?;
        client.mem_recv(&frame(4, 0, 0, &[]))?;
        let error = client
            .mem_recv(&frame(8, 0, 1, &increment.to_be_bytes()))
            .expect_err("WINDOW_UPDATE on idle stream must close connection");
        assert_eq!(error.code, 1);
        assert_eq!(
            client
                .mem_recv(&frame(6, 0, 0, &[0; 8]))
                .expect_err("failure must be sticky")
                .code,
            1
        );
    }
    Ok(())
}
