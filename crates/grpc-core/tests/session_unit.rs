//! In-process session limits checked with controlled native peer frames.
use grpc_core::{
    Code, CompressionAlgorithm, CompressionConfig, GrpcClientSession, GrpcServerSession, Metadata,
    SessionConfig, Status,
};

fn exchange(client: &mut GrpcClientSession, server: &mut GrpcServerSession) -> Result<(), Status> {
    for _ in 0..128 {
        let request = client.mem_send()?.to_vec();
        server.mem_recv(&request)?;
        let response = server.mem_send()?.to_vec();
        client.mem_recv(&response)?;
        if request.is_empty() && response.is_empty() {
            return Ok(());
        }
    }
    Err(Status::internal(
        "controlled session exchange did not finish",
    ))
}
fn limited_config() -> SessionConfig {
    SessionConfig {
        compression: CompressionConfig {
            max_decoding_message_size: 128,
            ..Default::default()
        },
        ..Default::default()
    }
}
#[test]
fn unit_decoding_limit_applies_to_both_roles_and_all_encodings() -> Result<(), Status> {
    for algorithm in [
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Identity,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Deflate,
        CompressionAlgorithm::Snappy,
    ] {
        for response in [true, false] {
            for length in [128, 129, 5120] {
                let mut client = GrpcClientSession::new(if response {
                    limited_config()
                } else {
                    SessionConfig::default()
                })?;
                let mut server = GrpcServerSession::new(if response {
                    SessionConfig::default()
                } else {
                    limited_config()
                })?;
                let id = client.start_request(
                    "localhost",
                    "/limits.Check/Message",
                    &Metadata::new(),
                    None,
                    algorithm,
                    response,
                )?;
                let bytes = vec![7; length];
                if response {
                    exchange(&mut client, &mut server)?;
                    server.send_initial_metadata(id, &Metadata::new(), algorithm)?;
                    server.send_message(id, &bytes, algorithm)?;
                    server.finish_response(id, &Status::ok(""), &Metadata::new())?;
                } else {
                    client.send_message(id, &bytes, algorithm)?;
                    client.half_close(id)?;
                }
                exchange(&mut client, &mut server)?;
                let message = if response {
                    client.pop_message(id)
                } else {
                    server.pop_message(id)
                };
                if length <= 128 {
                    assert_eq!(message?.expect("message at exact limit").payload(), bytes);
                } else {
                    assert_eq!(
                        message.unwrap_err().code(),
                        Code::ResourceExhausted,
                        "{algorithm:?}, response={response}, length={length}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn unit_early_ok_peer_reset_and_local_upload_close_are_idempotent() -> Result<(), Status> {
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    let mut server = GrpcServerSession::new(SessionConfig::default())?;
    let id = client.start_request(
        "localhost",
        "/early.Check/Message",
        &Metadata::new(),
        None,
        CompressionAlgorithm::Identity,
        false,
    )?;
    exchange(&mut client, &mut server)?;
    server.send_message(id, b"early response", CompressionAlgorithm::Identity)?;
    server.finish_response(id, &Status::ok(""), &Metadata::new())?;
    exchange(&mut client, &mut server)?;
    server.cancel_stream(id, 0)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(
        client.pop_message(id)?.expect("response").payload(),
        b"early response"
    );
    assert_eq!(
        client.take_trailers(id)?.expect("trailers").0.code(),
        Code::Ok
    );
    // The peer already stopped the upload after delivering the complete response.
    client.cancel_stream(id, 0)?;
    client.remove_closed_stream(id)?;
    Ok(())
}

#[test]
fn unit_reaping_preserves_dispatched_terminal_failure() -> Result<(), Status> {
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    let mut server = GrpcServerSession::new(SessionConfig::default())?;
    let id = client.start_request(
        "localhost",
        "/limits.Check/Call",
        &Metadata::new(),
        None,
        CompressionAlgorithm::Identity,
        false,
    )?;
    exchange(&mut client, &mut server)?;
    assert_eq!(
        server
            .pop_incoming_request()
            .expect("request ownership")
            .stream_id,
        id
    );
    client.cancel_stream(id, 8)?;
    exchange(&mut client, &mut server)?;
    assert_eq!(server.reap_closed_undispatched_streams()?, 0);
    assert_eq!(server.pop_message(id).unwrap_err().code(), Code::Cancelled);
    server.remove_closed_stream(id)?;
    Ok(())
}
