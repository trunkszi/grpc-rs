//! Coalesced response completion while the native connection enters CLOSING.

use grpc_core::{
    Code, CompressionAlgorithm, GrpcClientSession, GrpcServerSession, Metadata, SessionConfig,
    Status,
};

#[test]
fn coalesced_settings_response_and_goaway_preserve_completed_unary() -> anyhow::Result<()> {
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    let mut server = GrpcServerSession::new(SessionConfig::default())?;
    let id = client.start_request(
        "localhost",
        "/example.Echo/Call",
        &Metadata::new(),
        None,
        CompressionAlgorithm::Identity,
        false,
    )?;
    client.send_message(id, b"request", CompressionAlgorithm::Identity)?;
    client.half_close(id)?;
    loop {
        let request = client.mem_send()?.to_vec();
        if request.is_empty() {
            break;
        }
        assert_eq!(server.mem_recv(&request)?, request.len());
    }
    assert_eq!(
        server.pop_incoming_request().expect("request").stream_id,
        id
    );
    assert_eq!(
        server.pop_message(id)?.expect("request body").payload(),
        b"request"
    );
    let mut metadata = Metadata::new();
    metadata.append_ascii("trace", "coalesced")?;
    server.send_initial_metadata(id, &metadata, CompressionAlgorithm::Identity)?;
    server.send_message(id, b"response", CompressionAlgorithm::Identity)?;
    let status = Status::ok("completed before connection close");
    server.finish_response(id, &status, &metadata)?;
    server.submit_goaway(0)?;

    // No server bytes reach the client until SETTINGS and completion share one receive.
    let mut wire = Vec::new();
    loop {
        let chunk = server.mem_send()?;
        if chunk.is_empty() {
            break;
        }
        wire.extend_from_slice(chunk);
    }
    let mut frames = Vec::new();
    let mut offset = 0;
    while offset < wire.len() {
        let header = &wire[offset..offset + 9];
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        frames.push((header[3], header[4]));
        offset += 9 + length;
    }
    assert_eq!(offset, wire.len());
    assert_eq!(
        frames[0],
        (4, 0),
        "initial SETTINGS must queue a client ACK"
    );
    assert!(frames.contains(&(0, 0)), "response DATA must be present");
    let goaway = frames
        .iter()
        .position(|(kind, _)| *kind == 7)
        .expect("GOAWAY");
    let trailers = frames
        .iter()
        .position(|(kind, flags)| *kind == 1 && flags & 1 != 0)
        .expect("END_STREAM trailers");
    assert!(goaway < trailers, "GOAWAY must precede stream completion");
    assert!(wire.len() < 16 * 1024, "one native receive batch");
    assert_eq!(client.mem_recv(&wire)?, wire.len());
    assert_eq!(client.goaway_received(), Some((id, 0)));
    // C HTTP/2 discards the queued SETTINGS ACK with SESSION_CLOSING (-530).
    assert!(client.mem_send()?.is_empty());
    assert_eq!(client.pop_initial_metadata(id)?, Some(metadata.clone()));
    assert_eq!(
        client.pop_message(id)?.expect("response body").payload(),
        b"response"
    );
    assert!(client.pop_message(id)?.is_none());
    let (received_status, trailers) = client.take_trailers(id)?.expect("completion");
    assert_eq!(received_status.code(), Code::Ok);
    assert_eq!(received_status, status);
    assert_eq!(trailers, metadata);
    assert!(client.take_trailers(id)?.is_none());
    Ok(())
}
