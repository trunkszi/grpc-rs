//! Closed streams return discarded DATA credit without reopening stream windows.
use grpc_core::http2::{HeaderField, Http2Role, Http2Session, Http2Settings};
use nexora_proto::{BytesMut, http2::HpackEncoder};

fn frame(kind: u8, flags: u8, id: i32, payload: &[u8]) -> Vec<u8> {
    let mut wire = (payload.len() as u32).to_be_bytes()[1..].to_vec();
    wire.extend_from_slice(&[kind, flags]);
    wire.extend_from_slice(&(id as u32).to_be_bytes());
    wire.extend_from_slice(payload);
    wire
}
fn drain(session: &mut Http2Session) -> anyhow::Result<Vec<u8>> {
    let mut wire = Vec::new();
    loop {
        let chunk = session.mem_send()?;
        if chunk.is_empty() {
            return Ok(wire);
        }
        wire.extend_from_slice(chunk);
    }
}
fn response(client: &mut Http2Session, end: bool) -> anyhow::Result<i32> {
    let id = client.submit_request(
        &[
            HeaderField::new(b":method", b"POST"),
            HeaderField::new(b":scheme", b"http"),
            HeaderField::new(b":authority", b"localhost"),
            HeaderField::new(b":path", b"/echo"),
        ],
        None,
    )?;
    drain(client)?;
    let mut block = BytesMut::new();
    HpackEncoder::default().encode(&[(":status".into(), "200".into())], &mut block);
    client.mem_recv(&frame(1, 4, id, block.as_slice()))?;
    for index in 0..2 {
        client.mem_recv(&frame(0, u8::from(end && index == 1), id, &[1; 16_384]))?;
    }
    if !end {
        client.mem_recv(&frame(3, 0, id, &8u32.to_be_bytes()))?;
    }
    Ok(id)
}
fn reclaimed_credit(end: bool, consume: usize) -> anyhow::Result<()> {
    let mut client = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    drain(&mut client)?;
    client.mem_recv(&frame(4, 0, 0, &[]))?;
    drain(&mut client)?;
    let id = response(&mut client, end)?;
    client.consume(id, consume)?;
    client.release_stream(id)?;
    let output = drain(&mut client)?;
    let mut offset = 0;
    let mut connection_credit = 0;
    while offset < output.len() {
        let size = (usize::from(output[offset]) << 16)
            | (usize::from(output[offset + 1]) << 8)
            | usize::from(output[offset + 2]);
        if output[offset + 3] == 8 {
            assert_eq!(&output[offset + 5..offset + 9], &[0; 4]);
            connection_credit += u32::from_be_bytes(output[offset + 9..offset + 13].try_into()?);
        }
        offset += 9 + size;
    }
    assert_eq!(
        connection_credit, 32_768,
        "discarded DATA must restore connection credit"
    );
    assert!(client.release_stream(id).is_err());
    assert!(
        drain(&mut client)?.is_empty(),
        "credit must not be returned twice"
    );
    Ok(())
}
#[test]
fn reset_retirement_returns_unread_credit() -> anyhow::Result<()> {
    reclaimed_credit(false, 0)
}
#[test]
fn completed_retirement_returns_unread_credit() -> anyhow::Result<()> {
    reclaimed_credit(true, 0)
}
#[test]
fn retirement_returns_only_unconsumed_credit() -> anyhow::Result<()> {
    reclaimed_credit(false, 123)
}

#[test]
fn body_scheduler_wraps_in_stream_order() -> anyhow::Result<()> {
    use grpc_core::http2::DataProvider;
    let mut client = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    drain(&mut client)?;
    client.mem_recv(&frame(4, 0, 0, &[]))?;
    drain(&mut client)?;
    for _ in 0..3 {
        client.submit_request(
            &[
                HeaderField::new(b":method", b"POST"),
                HeaderField::new(b":scheme", b"http"),
                HeaderField::new(b":authority", b"localhost"),
                HeaderField::new(b":path", b"/echo"),
            ],
            Some(DataProvider::from_vec(vec![1; 32])),
        )?;
    }
    let wire = drain(&mut client)?;
    let mut ids = Vec::new();
    let mut offset = 0;
    while offset < wire.len() {
        let size = (usize::from(wire[offset]) << 16)
            | (usize::from(wire[offset + 1]) << 8)
            | usize::from(wire[offset + 2]);
        if wire[offset + 3] == 0 {
            ids.push(i32::from_be_bytes(wire[offset + 5..offset + 9].try_into()?));
        }
        offset += 9 + size;
    }
    assert_eq!(ids, [1, 3, 5, 1, 3, 5]);
    Ok(())
}
