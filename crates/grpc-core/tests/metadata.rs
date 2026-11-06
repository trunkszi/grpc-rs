//! Public contract regressions.
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use grpc_core::metadata::*;

#[test]
fn binary_headers_roundtrip_and_join() -> Result<(), MetadataError> {
    let mut metadata = Metadata::new();
    metadata.append_binary("trace-bin", &[0, 255, 128])?;
    metadata.append_ascii("trace", "one,two")?;
    let wire = metadata.to_headers();
    assert_eq!(wire[0].1, "AP+A");
    let decoded = Metadata::from_headers(wire.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;
    assert_eq!(decoded, metadata);
    let joined = Metadata::from_headers([("data-bin", " YQ==,Yg, ")])?;
    assert_eq!(
        joined.get_all("data-bin").collect::<Vec<_>>(),
        vec![
            &MetadataValue::Binary(vec![b'a']),
            &MetadataValue::Binary(vec![b'b']),
            &MetadataValue::Binary(vec![])
        ]
    );
    let original = metadata.clone();
    assert_eq!(
        metadata.append_header("data-bin", "YQ,%%%"),
        Err(MetadataError::InvalidBinaryValue)
    );
    assert_eq!(metadata, original);
    assert_eq!(
        metadata.append_header("grpc-status", "0"),
        Err(MetadataError::ReservedKey)
    );
    assert_eq!(
        metadata.append_header("ascii", "bad\n"),
        Err(MetadataError::InvalidAsciiValue)
    );
    Ok(())
}

#[test]
fn incoming_tabs_preserve_strict_outgoing_ascii() -> Result<(), MetadataError> {
    let mut metadata = Metadata::from_headers([("note", "a\tb")])?;
    metadata.append_header("note", "\t c \t")?;
    assert_eq!(
        metadata.to_headers(),
        vec![
            ("note".to_owned(), "a\tb".to_owned()),
            ("note".to_owned(), "\t c \t".to_owned()),
        ]
    );
    let original = metadata.clone();
    assert_eq!(
        metadata.append_ascii("note", "a\tb"),
        Err(MetadataError::InvalidAsciiValue)
    );
    assert_eq!(metadata, original);
    Ok(())
}

#[test]
fn incoming_ascii_rejects_other_controls_atomically() -> Result<(), MetadataError> {
    let mut metadata = Metadata::from_headers([("note", "retained")])?;
    let original = metadata.clone();
    for byte in (0_u8..=0x1f).chain([0x7f]).filter(|byte| *byte != b'\t') {
        let value = format!("a{}b", char::from(byte));
        assert_eq!(
            metadata.append_header("note", &value),
            Err(MetadataError::InvalidAsciiValue),
            "control {byte:#x}"
        );
        assert_eq!(metadata, original);
        assert_eq!(
            Metadata::from_headers([("note", value.as_str())]),
            Err(MetadataError::InvalidAsciiValue)
        );
    }
    assert_eq!(
        metadata.append_header("note", "non-ASCII \u{e9}"),
        Err(MetadataError::InvalidAsciiValue)
    );
    assert_eq!(metadata, original);
    Ok(())
}

#[test]
fn timeout_units_and_rounding() -> Result<(), MetadataError> {
    for (wire, duration) in [
        ("1H", Duration::from_secs(3600)),
        ("2M", Duration::from_secs(120)),
        ("3S", Duration::from_secs(3)),
        ("4m", Duration::from_millis(4)),
        ("5u", Duration::from_micros(5)),
        ("6n", Duration::from_nanos(6)),
        ("0n", Duration::ZERO),
    ] {
        assert_eq!(parse_grpc_timeout(wire)?, duration);
        assert!(parse_grpc_timeout(&format_grpc_timeout(duration)?)? >= duration);
    }
    let duration = Duration::from_nanos(100_000_001);
    assert_eq!(format_grpc_timeout(duration)?, "100001u");
    assert!(parse_grpc_timeout(&format_grpc_timeout(duration)?)? >= duration);
    for invalid in [
        "",
        "1",
        "1s",
        "-1S",
        "1.5S",
        " 1S",
        "1S ",
        "123456789n",
        "\u{e9}S",
    ] {
        assert!(parse_grpc_timeout(invalid).is_err(), "{invalid}");
    }
    assert!(format_grpc_timeout(Duration::MAX).is_err());
    Ok(())
}
#[test]
fn binary_padding_invalid_suffix_are_atomic() -> Result<(), MetadataError> {
    use base64::engine::general_purpose::STANDARD;
    let payload: Vec<_> = (0..=255).collect();
    for size in 0..=payload.len() {
        for encoded in [
            STANDARD.encode(&payload[..size]),
            STANDARD_NO_PAD.encode(&payload[..size]),
        ] {
            let mut metadata = Metadata::new();
            metadata.append_header("all-bin", &encoded)?;
            assert_eq!(
                metadata.get_all("all-bin").next(),
                Some(&MetadataValue::Binary(payload[..size].to_vec()))
            );
            for invalid in ["A", "YQ===", "_w", "YR", "YQ\n", "\u{e9}"] {
                let original = metadata.clone();
                assert_eq!(
                    metadata.append_header("all-bin", &format!("{encoded},{invalid}")),
                    Err(MetadataError::InvalidBinaryValue),
                    "{invalid}"
                );
                assert_eq!(metadata, original);
            }
        }
    }
    Ok(())
}

#[test]
fn timeout_boundaries_never_shorten_deadline() -> Result<(), MetadataError> {
    for nanos in [
        1_u128,
        1_000,
        1_000_000,
        1_000_000_000,
        60_000_000_000,
        3_600_000_000_000,
    ] {
        let maximum = nanos * 99_999_999;
        for value in [maximum - 1, maximum, maximum + 1] {
            let duration = Duration::new(
                (value / 1_000_000_000) as u64,
                (value % 1_000_000_000) as u32,
            );
            if let Ok(header) = format_grpc_timeout(duration) {
                let decoded = parse_grpc_timeout(&header)?;
                assert!(decoded >= duration, "{header}");
                assert!(header.len() <= 9);
            } else {
                assert!(value > 99_999_999 * 3_600_000_000_000);
            }
        }
    }
    assert_eq!(parse_grpc_timeout("00000001H")?, Duration::from_secs(3600));
    assert_eq!(format_grpc_timeout(Duration::ZERO)?, "0n");
    Ok(())
}

#[test]
fn timeout_parallel_digits_match_scalar() -> Result<(), MetadataError> {
    for fill in *b"09" {
        for position in 0..8 {
            for byte in 0..=127 {
                let mut input = [fill; 9];
                input[8] = b'n';
                input[position] = byte;
                let text = std::str::from_utf8(&input).unwrap();
                if byte.is_ascii_digit() {
                    let count = input[..8]
                        .iter()
                        .fold(0, |n, b| n * 10 + u64::from(b - b'0'));
                    assert_eq!(parse_grpc_timeout(text)?, Duration::from_nanos(count));
                } else {
                    assert_eq!(parse_grpc_timeout(text), Err(MetadataError::InvalidTimeout));
                }
            }
        }
    }
    Ok(())
}

#[test]
fn timeout_format_matches_wide_reference() -> Result<(), MetadataError> {
    let mut seed = 12345_u64;
    for index in 0..10_000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let timeout = Duration::new(seed >> (index % 64), (seed % 1_000_000_000) as u32);
        let expected = [
            ('n', 1_u128),
            ('u', 1_000),
            ('m', 1_000_000),
            ('S', 1_000_000_000),
            ('M', 60_000_000_000),
            ('H', 3_600_000_000_000),
        ]
        .into_iter()
        .find_map(|(unit, scale)| {
            let count = timeout.as_nanos().div_ceil(scale);
            (count <= 99_999_999).then(|| format!("{count}{unit}"))
        });
        assert_eq!(format_grpc_timeout(timeout).ok(), expected);
    }
    Ok(())
}
