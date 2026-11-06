//! Integration tests for custom codecs and application message envelopes.

use grpc_core::{Codec, Metadata, MetadataError, MetadataValue, Request, Response};

struct IntegerCodec {
    encoded: usize,
    decoded: usize,
}

impl Codec for IntegerCodec {
    type EncodeItem = u32;
    type DecodeItem = u32;
    type Error = std::array::TryFromSliceError;

    const CONTENT_TYPE: &'static str = "application/grpc+integer";

    fn encode(&mut self, item: &u32, destination: &mut Vec<u8>) -> Result<(), Self::Error> {
        destination.extend_from_slice(&item.to_be_bytes());
        self.encoded += 1;
        Ok(())
    }

    fn decode(&mut self, payload: &[u8]) -> Result<u32, Self::Error> {
        let bytes = payload.try_into()?;
        self.decoded += 1;
        Ok(u32::from_be_bytes(bytes))
    }
}

#[test]
fn integration_custom_codec_reuses_state_and_rejects_invalid_payloads()
-> Result<(), Box<dyn std::error::Error>> {
    let mut codec = IntegerCodec {
        encoded: 0,
        decoded: 0,
    };
    let mut buffer = vec![0xff];
    codec.encode(&7, &mut buffer)?;
    codec.encode(&42, &mut buffer)?;
    assert_eq!(buffer, [0xff, 0, 0, 0, 7, 0, 0, 0, 42]);
    assert_eq!(codec.decode(&buffer[1..5])?, 7);
    assert_eq!(codec.decode(&buffer[5..])?, 42);
    assert!(codec.decode(&buffer[1..]).is_err());
    assert_eq!((codec.encoded, codec.decoded), (2, 2));
    Ok(())
}

#[test]
fn integration_metadata_survives_request_and_response_transforms()
-> Result<(), Box<dyn std::error::Error>> {
    let mut metadata = Metadata::new();
    metadata.append_ascii("trace", "first")?;
    metadata.append_ascii("trace", "second")?;
    metadata.append_binary("context-bin", &[0, 0xff, 0x80])?;
    let request = Request::from_parts(metadata.clone(), 7_u32).map(|number| number.to_be_bytes());
    let (request_metadata, bytes) = request.into_parts();
    assert_eq!(request_metadata, metadata);
    let response = Response::from_parts(request_metadata, bytes).map(u32::from_be_bytes);
    assert_eq!(response.into_parts(), (metadata.clone(), 7));
    assert_eq!(
        metadata.get_all("trace").collect::<Vec<_>>(),
        [
            &MetadataValue::Ascii("first".to_owned()),
            &MetadataValue::Ascii("second".to_owned()),
        ]
    );
    assert_eq!(
        metadata.get_all("context-bin").next(),
        Some(&MetadataValue::Binary(vec![0, 0xff, 0x80]))
    );
    Ok(())
}

#[test]
fn integration_scenario_invalid_metadata_leaves_envelope_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let mut request = Request::new(7);
    request.metadata_mut().append_ascii("trace", "valid")?;
    let original = request.metadata().clone();
    for (key, value, error) in [
        ("", "value", MetadataError::InvalidKey),
        ("Trace", "value", MetadataError::InvalidKey),
        ("grpc-status", "0", MetadataError::ReservedKey),
        ("trace-bin", "value", MetadataError::ValueKindMismatch),
        ("trace", "bad\r\nvalue", MetadataError::InvalidAsciiValue),
        ("trace", "\u{7f}", MetadataError::InvalidAsciiValue),
        ("trace", "\u{e9}", MetadataError::InvalidAsciiValue),
    ] {
        assert_eq!(request.metadata_mut().append_ascii(key, value), Err(error));
        assert_eq!(request.metadata(), &original);
    }
    assert_eq!(
        request.metadata_mut().append_binary("trace", &[0]),
        Err(MetadataError::ValueKindMismatch)
    );
    assert_eq!(request.metadata(), &original);
    Ok(())
}
