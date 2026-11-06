//! Public contract regressions.

use grpc_core::slot_map::*;

fn headers(name: &str, value: &[u8]) -> Headers {
    vec![(name.to_owned(), value.to_vec())]
}
fn message(payload: &[u8]) -> StreamMessage {
    StreamMessage {
        compressed: false,
        payload: payload.to_vec(),
    }
}
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut wire = vec![0];
    let len = u32::try_from(payload.len()).expect("test length");
    wire.extend(len.to_be_bytes());
    wire.extend(payload);
    wire
}
fn bounded(bytes: usize, messages: usize) -> StreamSlotMap {
    StreamSlotMap::new(StreamLimits {
        max_buffered_bytes: bytes,
        max_queued_messages: messages,
    })
}
fn open(map: &mut StreamSlotMap, id: i32) -> SlotResult<()> {
    map.open_stream(id)?;
    map.push_initial_metadata(id, vec![])
}
#[test]
fn interleaved_streams_preserve_all_events() -> SlotResult<()> {
    let mut map = StreamSlotMap::default();
    let initial = headers(":status", b"200");
    for id in [1, 3] {
        map.open_stream(id)?;
        map.push_initial_metadata(id, initial.clone())?;
    }
    map.push_message(3, message(b"three-a"))?;
    map.push_message(1, message(b"one"))?;
    map.push_message(3, message(b"three-b"))?;
    assert_eq!(map.pop_message(1)?, Some(message(b"one")));
    for payload in [b"three-a", b"three-b"] {
        assert_eq!(map.pop_message(3)?, Some(message(payload)));
    }
    for id in [1, 3] {
        map.push_trailing_metadata(id, headers("grpc-status", b"0"))?;
        map.mark_remote_end_stream(id)?;
        assert_eq!(map.pop_initial_metadata(id)?, Some(initial.clone()));
        assert_eq!(map.take_trailers(id)?, Some(headers("grpc-status", b"0")));
    }
    Ok(())
}
#[test]
fn trailers_only_and_duplicate_metadata() -> SlotResult<()> {
    let mut map = StreamSlotMap::default();
    map.open_stream(1)?;
    map.push_trailing_metadata(1, headers("grpc-status", b"12"))?;
    map.mark_remote_end_stream(1)?;
    assert_eq!(map.pop_initial_metadata(1)?, None);
    assert_eq!(map.pop_message(1)?, None);
    assert_eq!(map.take_trailers(1)?, Some(headers("grpc-status", b"12")));
    open(&mut map, 3)?;
    map.pop_initial_metadata(3)?;
    assert!(map.push_initial_metadata(3, vec![]).is_err());
    map.push_trailing_metadata(3, vec![])?;
    map.take_trailers(3)?;
    assert!(map.push_trailing_metadata(3, vec![]).is_err());
    Ok(())
}
#[test]
fn goaway_prunes_and_enforces_decreasing_boundary() -> SlotResult<()> {
    let mut map = StreamSlotMap::default();
    for id in [1, 3, 5, 7, i32::MAX] {
        open(&mut map, id)?;
    }
    assert!(map.open_stream(1).is_err());
    map.push_message(1, message(b"retained"))?;
    assert!(map.apply_goaway(i32::MAX, 0)?.is_empty());
    assert!(matches!(
        map.open_stream(9),
        Err(SlotMapError::GoAway { .. })
    ));
    open(&mut map, 8)?;
    assert_eq!(map.apply_goaway(3, 0)?, vec![5, 7, i32::MAX]);
    assert!(map.open_stream(5).is_err());
    assert_eq!(map.pop_message(1)?, Some(message(b"retained")));
    assert_eq!(map.apply_goaway(1, 0)?, vec![3]);
    assert!(map.apply_goaway(7, 0).is_err());
    assert!(map.apply_goaway(-1, 0).is_err());
    assert_eq!(map.apply_goaway(0, 7)?, vec![1]);
    assert!(map.get(8).is_some());
    let mut server = StreamSlotMap::with_role(StreamLimits::default(), EndpointRole::Server);
    for id in [1, 2, 3, 4] {
        open(&mut server, id)?;
    }
    server.push_message(3, message(b"request"))?;
    assert_eq!(server.apply_goaway(2, 0)?, vec![4]);
    assert!(server.open_stream(6).is_err());
    assert_eq!(server.apply_goaway(0, 0)?, vec![2]);
    assert_eq!(server.pop_message(3)?, Some(message(b"request")));
    open(&mut server, 5)?;
    assert!(matches!(
        map.open_stream(1),
        Err(SlotMapError::GoAway { error_code: 7, .. })
    ));
    Ok(())
}
#[test]
fn completed_response_survives_no_error_reset() -> SlotResult<()> {
    for (code, complete) in [(0, true), (0, false), (8, true)] {
        let mut map = StreamSlotMap::default();
        open(&mut map, 1)?;
        map.push_message(1, message(b"early response"))?;
        if complete {
            map.push_trailing_metadata(1, headers("grpc-status", b"0"))?;
            map.mark_remote_end_stream(1)?;
        } else {
            map.append_recv_bytes(1, &[0, 0])?;
        }
        let bytes = map.get(1).expect("slot").buffered_bytes();
        map.record_rst_stream(1, code)?;
        assert!(map.record_rst_stream(1, code).is_err());
        let slot = map.get(1).expect("slot");
        assert!(slot.remote_end_stream() && slot.local_end_stream());
        if code == 0 && complete {
            assert_eq!(slot.buffered_bytes(), bytes);
            assert_eq!(map.pop_initial_metadata(1)?, Some(vec![]));
            assert_eq!(map.pop_message(1)?, Some(message(b"early response")));
            assert_eq!(map.take_trailers(1)?, Some(headers("grpc-status", b"0")));
            assert_eq!(map.get(1).expect("slot").buffered_bytes(), 0);
        } else {
            assert_eq!(slot.buffered_bytes(), 0);
            assert!(map.pop_initial_metadata(1).is_err());
            assert!(map.take_trailers(1).is_err());
            assert!(matches!(
                map.pop_message(1),
                Err(SlotMapError::Reset { .. })
            ));
            assert!(map.push_message(1, message(b"late")).is_err());
        }
        map.remove_closed_stream(1)?;
    }
    Ok(())
}
#[test]
fn fragmented_frames_reject_invalid_prefixes() -> SlotResult<()> {
    let mut map = StreamSlotMap::default();
    open(&mut map, 1)?;
    let mut wire = frame(b"abc");
    wire.extend(frame(b""));
    for byte in wire {
        map.append_recv_bytes(1, &[byte])?;
    }
    for payload in [b"abc".as_slice(), b"".as_slice()] {
        assert_eq!(map.pop_message(1)?, Some(message(payload)));
    }
    let mut compressed = frame(b"xyz");
    compressed[0] = 1;
    map.append_recv_bytes(1, &compressed)?;
    assert!(map.pop_message(1)?.expect("message").compressed);
    assert!(map.append_recv_bytes(1, &[2, 0, 0, 0, 0]).is_err());
    assert!(map.remove_closed_stream(1).is_err());
    map.mark_local_end_stream(1)?;
    map.push_message(1, message(b"half-open"))?;
    assert!(map.mark_local_end_stream(1).is_err());
    map.mark_remote_end_stream(1)?;
    assert!(map.push_message(1, message(b"late")).is_err());
    map.remove_closed_stream(1)?;
    Ok(())
}
#[test]
fn rejected_batches_preserve_partial_frames() -> SlotResult<()> {
    let mut map = bounded(128, 1);
    open(&mut map, 1)?;
    let wire = frame(b"ok");
    map.append_recv_bytes(1, &wire[..2])?;
    let mut queue_excess = wire[2..].to_vec();
    queue_excess.extend(frame(b""));
    let mut malformed = wire[2..].to_vec();
    malformed.push(2);
    for rejected in [queue_excess, malformed, vec![0; 128]] {
        assert!(map.append_recv_bytes(1, &rejected).is_err());
        let slot = map.get(1).expect("slot");
        assert_eq!(slot.buffered_bytes(), 2);
        assert_eq!(slot.queued_message_count(), 0);
    }
    map.append_recv_bytes(1, &wire[2..])?;
    assert_eq!(map.pop_message(1)?, Some(message(b"ok")));
    assert_eq!(map.get(1).expect("slot").buffered_bytes(), 0);
    Ok(())
}
#[test]
fn truncated_frames_and_lengths_are_bounded() -> SlotResult<()> {
    for wire in [&[0, 0][..], &[0, 0, 0, 0, 3, b'a'][..]] {
        let mut map = bounded(128, 4);
        open(&mut map, 1)?;
        map.append_recv_bytes(1, wire)?;
        assert!(map.mark_remote_end_stream(1).is_err());
    }
    let mut map = bounded(8, 4);
    open(&mut map, 1)?;
    let oversized = [0, 0xff, 0xff, 0xff, 0xff];
    assert!(map.append_recv_bytes(1, &oversized).is_err());
    map.append_recv_bytes(1, &[0, 0, 0])?;
    assert_eq!(map.get(1).expect("slot").buffered_bytes(), 3);
    assert!(map.push_message(1, message(b"123456")).is_err());
    Ok(())
}
#[test]
fn budgets_release_on_consumption() -> SlotResult<()> {
    let mut map = bounded(128, 1);
    map.open_stream(1)?;
    map.push_initial_metadata(1, headers("key", b"value"))?;
    assert!(map.push_message(1, message(&[0; 128])).is_err());
    map.pop_initial_metadata(1)?;
    assert_eq!(map.get(1).expect("slot").buffered_bytes(), 0);
    map.push_message(1, message(&[0; 64]))?;
    assert!(map.push_trailing_metadata(1, headers("x", b"y")).is_err());
    map.pop_message(1)?;
    map.push_message(1, message(b""))?;
    assert!(map.push_message(1, message(b"")).is_err());
    assert_eq!(map.get(1).expect("slot").queued_message_count(), 1);
    assert!(map.append_recv_bytes(1, &frame(b"blocked")).is_err());
    assert_eq!(map.pop_message(1)?, Some(message(b"")));
    map.append_recv_bytes(1, &frame(b"next"))?;
    assert_eq!(map.pop_message(1)?, Some(message(b"next")));
    map.push_trailing_metadata(1, headers("x", b"y"))?;
    map.take_trailers(1)?;
    assert_eq!(map.get(1).expect("slot").buffered_bytes(), 0);
    Ok(())
}

#[test]
fn failed_retirement_preserves_queued_message() -> SlotResult<()> {
    let mut map = StreamSlotMap::default();
    open(&mut map, 1)?;
    map.push_message(1, message(b"retained"))?;
    assert!(matches!(
        map.remove_closed_stream(1),
        Err(SlotMapError::NotClosed(1))
    ));
    assert_eq!(map.pop_message(1)?, Some(message(b"retained")));
    map.mark_local_end_stream(1)?;
    map.mark_remote_end_stream(1)?;
    map.remove_closed_stream(1)?;
    assert!(matches!(
        map.remove_closed_stream(1),
        Err(SlotMapError::UnknownStream(1))
    ));
    Ok(())
}

#[test]
fn state_word_preserves_full_reset_code() -> SlotResult<()> {
    use uarch::prelude::*;
    let state = StreamStateWord::default().patch(patch!(StreamStateWord {
        INITIAL_SEEN: true,
        LOCAL_END: true,
        REMOTE_END: true,
        HAS_RESET: true,
        RESET_CODE: 0xffff_ffff
    }));
    assert_eq!(size_of::<StreamStateWord>(), 8);
    assert_eq!(state.raw(), 0xffff_ffff_0000_001d);
    assert!(
        state.matches(
            pattern!(StreamStateWord { INITIAL_SEEN & LOCAL_END & REMOTE_END & HAS_RESET })
        )
    );
    assert_eq!(state.get(StreamStateWord::RESET_CODE), u32::MAX);
    for code in [0, 1, 0x8000_0000, u32::MAX] {
        let mut map = StreamSlotMap::default();
        map.open_stream(1)?;
        map.record_rst_stream(1, code)?;
        let slot = map.get(1).ok_or(SlotMapError::UnknownStream(1))?;
        assert_eq!(slot.reset_error_code(), Some(code));
        assert!(slot.remote_end_stream() && slot.local_end_stream());
        assert!(
            matches!(map.record_rst_stream(1, 7), Err(SlotMapError::Reset { error_code, .. }) if error_code == code)
        );
    }
    Ok(())
}

#[test]
fn sparse_goaway_crosses_bitmap_blocks() -> SlotResult<()> {
    let mut map = StreamSlotMap::default();
    for id in [1, 62, 63, 64, 65, 126, 127, 128, 129, i32::MAX] {
        map.open_stream(id)?;
    }
    map.record_rst_stream(65, 0)?;
    map.remove_closed_stream(65)?;
    assert_eq!(map.apply_goaway(63, 0)?, vec![127, 129, i32::MAX]);
    assert_eq!(map.active_stream_count(), 6);
    for id in [62, 64, 126, 128] {
        assert!(map.get(id).is_some());
    }
    assert_eq!(map.apply_goaway(0, 0)?, vec![1, 63]);
    assert!(map.apply_goaway(0, 0)?.is_empty());
    assert_eq!(map.active_stream_count(), 4);
    Ok(())
}
