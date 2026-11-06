//! Header assembly remains checked when a closed stream is released mid-block.
use grpc_core::http2::{HeaderField, Http2Role, Http2Session, Http2Settings};
use nexora_proto::{BytesMut, http2::HpackEncoder};

fn frame(kind: u8, flags: u8, id: i32, payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_be_bytes()[1..].to_vec();
    out.extend_from_slice(&[kind, flags]);
    out.extend_from_slice(&(id as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}
fn drain(session: &mut Http2Session) -> anyhow::Result<()> {
    for _ in 0..16 {
        if session.mem_send()?.is_empty() {
            return Ok(());
        }
    }
    anyhow::bail!("session never quiesced")
}

#[test]
fn released_stream_header_block_fails() -> anyhow::Result<()> {
    let mut client = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    drain(&mut client)?;
    client.mem_recv(&frame(4, 0, 0, &[]))?;
    let id = client.submit_request(
        &[
            HeaderField::new(b":method", b"POST"),
            HeaderField::new(b":scheme", b"http"),
            HeaderField::new(b":authority", b"localhost"),
            HeaderField::new(b":path", b"/echo"),
        ],
        None,
    )?;
    drain(&mut client)?;
    let mut response = BytesMut::new();
    HpackEncoder::default().encode(&[(":status".into(), "200".into())], &mut response);
    client.mem_recv(&frame(1, 5, id, response.as_slice()))?;
    client.mem_recv(&frame(1, 0, id, &[]))?;
    client.release_stream(id)?;
    let error = client
        .mem_recv(&frame(9, 4, id, &[]))
        .expect_err("released header-block owner must remain checked");
    assert_eq!(error.code, 1);
    assert_eq!(error.message, "header block names an unknown stream");
    assert_eq!(
        client.mem_recv(&[]).expect_err("sticky failure").message,
        error.message
    );
    Ok(())
}
