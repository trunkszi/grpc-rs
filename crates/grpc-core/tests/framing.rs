//! Public contract regressions.
use grpc_core::{Code, framing::*};

fn assert_aligned(message: &AlignedMessageBuf, payload: &[u8], compressed: bool) {
    assert_eq!(message.payload().as_ptr() as usize % 8, 0);
    assert_eq!(message.payload(), payload);
    assert_eq!(message.is_compressed(), compressed);
    assert_eq!(
        message.wire_bytes().len(),
        FRAME_HEADER_SIZE + payload.len()
    );
    assert_eq!(message.wire_bytes()[0], u8::from(compressed));
    assert_eq!(
        &message.wire_bytes()[1..FRAME_HEADER_SIZE],
        &(payload.len() as u32).to_be_bytes()
    );
}

#[test]
fn alignment_survives_fragmented_coalesced_frames() {
    let payloads: [&[u8]; 4] = [b"", b"a", b"12345678", b"arbitrary payload length"];
    let mut wire = Vec::new();
    for (index, payload) in payloads.iter().enumerate() {
        let message = AlignedMessageBuf::new(payload, index % 2 == 1).unwrap();
        assert_aligned(&message, payload, index % 2 == 1);
        assert_aligned(&message.clone(), payload, index % 2 == 1);
        wire.extend_from_slice(message.wire_bytes());
    }
    for chunk_size in 1..=wire.len() {
        let mut decoder = LengthPrefixedFrame::new(128);
        let mut decoded = Vec::new();
        for chunk in wire.chunks(chunk_size) {
            decoded.extend(decoder.push(chunk).unwrap());
            assert!(decoder.push(&[]).unwrap().is_empty());
        }
        decoder.finish().unwrap();
        assert_eq!(decoded.len(), payloads.len());
        for (index, (message, payload)) in decoded.iter().zip(payloads).enumerate() {
            assert_aligned(message, payload, index % 2 == 1);
        }
    }
}

#[test]
fn every_split_preserves_alignment_and_wire() {
    let original = AlignedMessageBuf::new(b"capnp aligned payload", false).unwrap();
    for split in 0..=original.wire_bytes().len() {
        let mut decoder = LengthPrefixedFrame::default();
        let mut decoded = decoder.push(&original.wire_bytes()[..split]).unwrap();
        decoded.extend(decoder.push(&original.wire_bytes()[split..]).unwrap());
        decoder.finish().unwrap();
        assert_eq!(decoded.len(), 1);
        assert_aligned(&decoded[0], original.payload(), false);
        assert_eq!(decoded[0].wire_bytes(), original.wire_bytes());
    }
}

#[test]
fn corrupt_flags_fail_closed_until_reset() {
    for flag in [2, 127, 255] {
        let mut decoder = LengthPrefixedFrame::default();
        let error = decoder.push(&[flag, 0, 0, 0, 0]).unwrap_err();
        assert_eq!(error.code(), Code::Internal);
        assert_eq!(decoder.push(&[]).unwrap_err().code(), Code::Internal);
        assert_eq!(decoder.finish().unwrap_err().code(), Code::Internal);
        decoder.reset();
        assert_eq!(decoder.push(&[0; 5]).unwrap().len(), 1);
        decoder.finish().unwrap();
    }
}

#[test]
fn size_limit_rejects_header_before_body() {
    let mut decoder = LengthPrefixedFrame::new(3);
    assert!(decoder.push(&[0, 0, 0, 0]).unwrap().is_empty());
    assert_eq!(
        decoder.push(&[4]).unwrap_err().code(),
        Code::ResourceExhausted
    );
    decoder.reset();
    let message = AlignedMessageBuf::new(b"abc", false).unwrap();
    assert_eq!(decoder.push(message.wire_bytes()).unwrap().len(), 1);
    decoder.finish().unwrap();
    decoder.reset();
    assert_eq!(
        decoder.push(&[0, 255, 255, 255, 255]).unwrap_err().code(),
        Code::ResourceExhausted
    );
}

#[test]
fn truncation_detects_partial_header_and_body() {
    let message = AlignedMessageBuf::new(b"body", false).unwrap();
    for end in 1..message.wire_bytes().len() {
        let mut decoder = LengthPrefixedFrame::default();
        assert!(
            decoder
                .push(&message.wire_bytes()[..end])
                .unwrap()
                .is_empty()
        );
        assert_eq!(decoder.finish().unwrap_err().code(), Code::Internal);
    }
    LengthPrefixedFrame::default().finish().unwrap();
}

#[test]
fn zero_limit_accepts_empty_payload_only() {
    let mut decoder = LengthPrefixedFrame::new(0);
    let decoded = decoder.push(&[0; 10]).unwrap();
    assert_eq!(decoded.len(), 2);
    for message in decoded {
        assert_aligned(&message, b"", false);
    }
    decoder.finish().unwrap();
    assert_eq!(
        decoder.push(&[0, 0, 0, 0, 1]).unwrap_err().code(),
        Code::ResourceExhausted
    );
}

#[test]
fn reset_discards_every_partial_frame() {
    let abandoned = AlignedMessageBuf::new(b"abandoned", true).unwrap();
    let replacement = AlignedMessageBuf::new(b"replacement", false).unwrap();
    for end in 1..abandoned.wire_bytes().len() {
        let mut decoder = LengthPrefixedFrame::default();
        assert!(
            decoder
                .push(&abandoned.wire_bytes()[..end])
                .unwrap()
                .is_empty()
        );
        decoder.reset();
        let decoded = decoder.push(replacement.wire_bytes()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_aligned(&decoded[0], replacement.payload(), false);
        decoder.finish().unwrap();
    }
}

#[test]
fn failure_rejects_entire_chunk() {
    let valid = AlignedMessageBuf::new(b"valid", false).unwrap();
    let mut chunk = valid.wire_bytes().to_vec();
    chunk.extend_from_slice(&[2, 0, 0, 0, 0]);
    let mut decoder = LengthPrefixedFrame::default();
    let error = decoder.push(&chunk).unwrap_err();
    assert_eq!(error.code(), Code::Internal);
    assert_eq!(decoder.push(valid.wire_bytes()).unwrap_err(), error);
    assert_eq!(decoder.finish().unwrap_err(), error);
    decoder.reset();
    let decoded = decoder.push(valid.wire_bytes()).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_aligned(&decoded[0], valid.payload(), false);
    decoder.finish().unwrap();
}

#[test]
fn envelope_header_layout_and_wire() {
    assert_eq!(size_of::<FrameEnvelopeHeader>(), 5);
    assert_eq!(align_of::<FrameEnvelopeHeader>(), 1);
    for (length, compressed, wire) in [
        (0, false, [0, 0, 0, 0, 0]),
        (0x0102_0304, true, [1, 1, 2, 3, 4]),
        (u32::MAX, false, [0, 255, 255, 255, 255]),
    ] {
        let header = FrameEnvelopeHeader::new(length, compressed);
        assert_eq!(header.wire_bytes(), wire);
        let decoded = FrameEnvelopeHeader::parse(wire).unwrap();
        assert_eq!(decoded.payload_len(), length);
        assert_eq!(decoded.is_compressed(), compressed);
    }
    for flag in [2, 128, 255] {
        assert!(FrameEnvelopeHeader::parse([flag, 0, 0, 0, 0]).is_err());
    }
    assert_eq!(size_of::<grpc_core::slot_map::StreamLimits>(), 16);
}
