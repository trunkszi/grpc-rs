use super::*;

#[test]
fn final_stream_id_is_usable() -> Result<()> {
    let mut session = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    session.next_id = i32::MAX.into();
    let id = session.submit_request(
        &[
            HeaderField::new(b":method", b"POST"),
            HeaderField::new(b":scheme", b"http"),
            HeaderField::new(b":authority", b"localhost"),
            HeaderField::new(b":path", b"/last"),
        ],
        None,
    )?;
    assert_eq!(id, i32::MAX);
    assert!(session.submit_request(&[], None).is_err());
    while !session.mem_send()?.is_empty() {}
    session.mem_recv(&[0, 0, 0, 4, 0, 0, 0, 0, 0])?;
    session.mem_recv(&[0, 0, 4, 3, 0, 127, 255, 255, 255, 0, 0, 0, 8])?;
    let mut closed = false;
    while let Some(event) = session.next_event() {
        if let SessionEvent::StreamClose {
            stream_id,
            error_code,
        } = event
        {
            assert_eq!(stream_id, i32::MAX);
            assert_eq!(error_code, 8);
            closed = true;
        }
    }
    assert!(closed, "last stream must remain a known peer reset target");
    Ok(())
}
