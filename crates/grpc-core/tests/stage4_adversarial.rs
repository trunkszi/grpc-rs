//! Terminal-state and wire-boundary regressions through public protocol seams.
use grpc_core::http2::{Http2Role, Http2Session, Http2Settings};

#[test]
fn fatal_http2_error_clears_write_readiness() -> anyhow::Result<()> {
    let mut protocol = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    assert!(protocol.want_write());
    let error = protocol.mem_recv(&[0, 0, 0, 4, 1, 0, 0, 0, 0]).unwrap_err();
    assert_eq!(error.code, 1);
    assert!(!protocol.want_read());
    assert!(
        !protocol.want_write(),
        "terminal connection cannot serialize queued frames"
    );
    assert_eq!(protocol.mem_send().unwrap_err().message, error.message);
    Ok(())
}

#[test]
fn fatal_session_error_retires_pending_output() -> anyhow::Result<()> {
    use grpc_core::{
        GrpcClientSession, SessionConfig,
        transport::{MemoryPipe, SyncTransport},
    };
    let mut session = GrpcClientSession::new(SessionConfig::default())?;
    let (mut transport, _peer) = MemoryPipe::with_capacity(1)?;
    transport.set_nonblocking(true)?;
    session.flush_to_transport(&mut transport)?;
    assert!(session.want_write());
    let error = session.mem_recv(&[0, 0, 0, 4, 1, 0, 0, 0, 0]).unwrap_err();
    assert_eq!(
        session.mem_recv(&[]).unwrap_err(),
        error,
        "failure must remain sticky even for empty input"
    );
    assert!(!session.want_read());
    assert!(
        !session.want_write(),
        "failed sessions must not advertise undrainable output"
    );
    assert_eq!(session.mem_send().unwrap_err(), error);
    assert_eq!(
        session.flush_to_transport(&mut transport).unwrap_err(),
        error
    );
    Ok(())
}

#[test]
fn grpc_length_preserves_all_bits() -> anyhow::Result<()> {
    use grpc_core::framing::FrameEnvelopeHeader;
    for (length, bytes) in [
        (0, [0, 0, 0, 0]),
        (0x00ff_ffff, [0, 255, 255, 255]),
        (0x0100_0000, [1, 0, 0, 0]),
        (u32::MAX, [255, 255, 255, 255]),
    ] {
        for compressed in [false, true] {
            let wire = [u8::from(compressed), bytes[0], bytes[1], bytes[2], bytes[3]];
            assert_eq!(
                FrameEnvelopeHeader::new(length, compressed).wire_bytes(),
                wire
            );
            let header = FrameEnvelopeHeader::parse(wire)?;
            assert_eq!(header.payload_len(), length);
            assert_eq!(header.is_compressed(), compressed);
        }
    }
    assert!(FrameEnvelopeHeader::parse([2, 0, 0, 0, 0]).is_err());
    assert!(FrameEnvelopeHeader::parse([255, 0, 0, 0, 0]).is_err());
    Ok(())
}

#[test]
fn oversized_grpc_prefix_fails_before_allocation() -> anyhow::Result<()> {
    use grpc_core::{Code, LengthPrefixedFrame};
    let mut decoder = LengthPrefixedFrame::new(0x00ff_ffff);
    let error = decoder.push(&[0, 1, 0, 0, 0]).unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(decoder.push(&[0, 0, 0, 0, 0]).unwrap_err(), error);
    decoder.reset();
    assert_eq!(decoder.push(&[0, 0, 0, 0, 0])?.len(), 1);
    decoder.finish()?;
    Ok(())
}

#[test]
fn http2_frame_limit_validates_before_packing() -> anyhow::Result<()> {
    for max_frame_size in [0, 16_383, 0x0100_0000, u32::MAX] {
        assert!(
            Http2Session::new(
                Http2Role::Client,
                Http2Settings {
                    max_frame_size,
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
    let mut protocol = Http2Session::new(
        Http2Role::Client,
        Http2Settings {
            max_frame_size: 0x00ff_ffff,
            ..Default::default()
        },
    )?;
    let error = protocol
        .mem_recv(&[0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 5, 1, 0, 0, 0])
        .unwrap_err();
    assert_eq!(error.code, 1);
    assert!(!protocol.want_write());
    Ok(())
}

#[test]
fn full_bitmap_block_preserves_peer_streams() -> anyhow::Result<()> {
    use grpc_core::slot_map::{EndpointRole, StreamLimits, StreamSlotMap};
    let mut slots = StreamSlotMap::with_role(StreamLimits::default(), EndpointRole::Server);
    for invalid in [i32::MIN, -1, 0] {
        assert!(slots.open_stream(invalid).is_err());
    }
    assert_eq!(slots.active_stream_count(), 0);
    for id in 64..128 {
        slots.open_stream(id)?;
    }
    assert!(slots.open_stream(127).is_err());
    assert_eq!(slots.active_stream_count(), 64);
    for id in [64, 127] {
        slots.record_rst_stream(id, u32::MAX)?;
        assert_eq!(
            slots.get(id).and_then(|s| s.reset_error_code()),
            Some(u32::MAX)
        );
        slots.remove_closed_stream(id)?;
    }
    let removed = slots.apply_goaway(63, u32::MAX)?;
    assert_eq!(removed, (66..128).step_by(2).collect::<Vec<_>>());
    assert_eq!(slots.active_stream_count(), 31);
    for id in (65..127).step_by(2) {
        assert!(slots.get(id).is_some());
    }
    assert!(slots.apply_goaway(0, 0)?.is_empty());
    Ok(())
}

#[test]
fn invalid_body_error_retires_connection() -> anyhow::Result<()> {
    use grpc_core::http2::{DataPoll, DataProvider, DataSource, HeaderField};
    struct InvalidBody {
        bytes: Vec<u8>,
        read: bool,
    }
    impl DataSource for InvalidBody {
        fn read(&mut self, _max_len: usize) -> DataPoll<'_> {
            if std::mem::replace(&mut self.read, true) {
                DataPoll::Eof
            } else {
                DataPoll::Copy(&self.bytes)
            }
        }
    }
    let mut protocol = Http2Session::new(Http2Role::Client, Http2Settings::default())?;
    protocol.submit_request(
        &[
            HeaderField::new(b":method", b"POST"),
            HeaderField::new(b":scheme", b"http"),
            HeaderField::new(b":authority", b"localhost"),
            HeaderField::new(b":path", b"/boundary"),
        ],
        Some(DataProvider::new(InvalidBody {
            bytes: vec![0; 16_385],
            read: false,
        })),
    )?;
    let mut failure = None;
    for _ in 0..8 {
        if let Err(error) = protocol.mem_send() {
            failure = Some(error);
            break;
        }
    }
    let failure = failure.expect("body budget violation");
    assert_eq!(failure.code, 2);
    assert!(!protocol.want_write());
    assert!(!protocol.want_read());
    assert_eq!(protocol.mem_send().unwrap_err().message, failure.message);
    Ok(())
}
