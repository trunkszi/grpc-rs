//! Validated application metadata with raw binary values.
//!
//! Binary values remain unencoded until the HTTP/2 boundary applies Base64.
//! Repeated entries retain insertion order. This owned collection allocates on
//! insertion; it makes no allocation guarantees for RPC hot paths.

use std::{fmt, time::Duration};

use base64::{
    Engine as _, alphabet,
    engine::{
        DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig, general_purpose::STANDARD_NO_PAD,
    },
};

/// A validated ASCII value or an unencoded binary value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataValue {
    /// Printable ASCII bytes, including spaces.
    Ascii(String),
    /// Raw bytes associated with a `-bin` key.
    Binary(Vec<u8>),
}

/// Invalid application metadata rejected before insertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataError {
    /// A key is empty or contains a character outside `[a-z0-9_.-]`.
    InvalidKey,
    /// A key uses the protocol-reserved `grpc-` prefix.
    ReservedKey,
    /// A binary key was paired with ASCII data, or the reverse.
    ValueKindMismatch,
    /// An ASCII value contains bytes outside `0x20..=0x7e`.
    InvalidAsciiValue,
    /// A binary header contains malformed Base64.
    InvalidBinaryValue,
    /// A timeout is malformed or cannot fit the eight-digit wire representation.
    InvalidTimeout,
}

impl fmt::Display for MetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidKey => "metadata keys must be nonempty lowercase ASCII tokens",
            Self::ReservedKey => "the grpc- metadata prefix is reserved for the protocol",
            Self::ValueKindMismatch => "binary metadata requires a -bin key and raw bytes",
            Self::InvalidAsciiValue => "ASCII metadata values must contain printable ASCII only",
            Self::InvalidBinaryValue => "binary metadata must contain valid standard Base64",
            Self::InvalidTimeout => "grpc-timeout requires up to eight digits and a supported unit",
        })
    }
}

impl std::error::Error for MetadataError {}

/// Application metadata preserving repeated keys and insertion order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Metadata {
    entries: Vec<(String, MetadataValue)>,
}

impl Metadata {
    /// Creates an empty metadata collection without allocating.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Appends an ASCII entry, preserving earlier entries with the same key.
    ///
    /// # Errors
    ///
    /// Rejects malformed or protocol-reserved keys, `-bin` keys, and values
    /// containing non-printable ASCII. Rejection leaves the collection unchanged.
    pub fn append_ascii(&mut self, key: &str, value: &str) -> Result<(), MetadataError> {
        validate_key(key)?;
        if key.ends_with("-bin") {
            return Err(MetadataError::ValueKindMismatch);
        }
        if !value.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
            return Err(MetadataError::InvalidAsciiValue);
        }
        self.entries
            .push((key.to_owned(), MetadataValue::Ascii(value.to_owned())));
        Ok(())
    }

    /// Appends raw binary data under a `-bin` key.
    ///
    /// # Errors
    ///
    /// Rejects malformed or protocol-reserved keys and keys without a `-bin`
    /// suffix. Rejection leaves the collection unchanged.
    pub fn append_binary(&mut self, key: &str, value: &[u8]) -> Result<(), MetadataError> {
        validate_key(key)?;
        if !key.ends_with("-bin") {
            return Err(MetadataError::ValueKindMismatch);
        }
        self.entries
            .push((key.to_owned(), MetadataValue::Binary(value.to_vec())));
        Ok(())
    }

    /// Encodes raw binary entries as unpadded Base64 for HTTP/2 headers or trailers.
    #[must_use]
    pub fn to_headers(&self) -> Vec<(String, String)> {
        self.entries
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    match value {
                        MetadataValue::Ascii(value) => value.clone(),
                        MetadataValue::Binary(value) => STANDARD_NO_PAD.encode(value),
                    },
                )
            })
            .collect()
    }

    /// Decodes one application header, splitting comma-joined binary values.
    ///
    /// # Errors
    /// Rejects reserved keys, invalid ASCII or Base64. Rejection is atomic even
    /// when a later member of a comma-joined binary value is malformed.
    pub fn append_header(&mut self, key: &str, value: &str) -> Result<(), MetadataError> {
        validate_key(key)?;
        if !key.ends_with("-bin") {
            return self.append_ascii(key, value);
        }
        let engine = GeneralPurpose::new(
            &alphabet::STANDARD,
            GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
        );
        let decoded: Vec<Vec<u8>> = value
            .split(',')
            .map(|part| {
                engine
                    .decode(part.trim_matches([' ', '\t']))
                    .map_err(|_| MetadataError::InvalidBinaryValue)
            })
            .collect::<Result<_, _>>()?;
        self.entries.extend(
            decoded
                .into_iter()
                .map(|v| (key.to_owned(), MetadataValue::Binary(v))),
        );
        Ok(())
    }

    /// Builds application metadata from HTTP/2 headers, preserving duplicate order.
    ///
    /// # Errors
    /// Rejects any invalid application entry; protocol headers must be separated
    /// by the transport before invoking this constructor.
    pub fn from_headers<'a>(
        headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, MetadataError> {
        let mut result = Self::new();
        for (key, value) in headers {
            result.append_header(key, value)?;
        }
        Ok(result)
    }

    /// Iterates over entries in insertion order, including repeated keys.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &MetadataValue)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value))
    }

    /// Iterates over all values matching an exact, case-sensitive key.
    pub fn get_all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a MetadataValue> {
        self.entries
            .iter()
            .filter_map(move |(entry_key, value)| (entry_key == key).then_some(value))
    }

    /// Returns the number of entries, counting repeated keys separately.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reports whether the collection contains no entries.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn validate_key(key: &str) -> Result<(), MetadataError> {
    if key.is_empty()
        || !key.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_.-".contains(&byte)
        })
    {
        return Err(MetadataError::InvalidKey);
    }
    if key.starts_with("grpc-") {
        return Err(MetadataError::ReservedKey);
    }
    Ok(())
}

/// Parses a gRPC timeout with one to eight digits and a case-sensitive time unit.
/// A zero timeout represents an already expired deadline.
///
/// # Errors
/// Rejects missing/unsupported units, whitespace, signs or more than eight digits.
pub fn parse_grpc_timeout(value: &str) -> Result<Duration, MetadataError> {
    let bytes = value.as_bytes();
    if !(2..=9).contains(&bytes.len()) || !bytes[..bytes.len() - 1].iter().all(u8::is_ascii_digit) {
        return Err(MetadataError::InvalidTimeout);
    }
    let mut count = 0_u64;
    for byte in &bytes[..bytes.len() - 1] {
        count = count * 10 + u64::from(*byte - b'0');
    }
    Ok(match bytes[bytes.len() - 1] {
        b'H' => Duration::from_secs(count * 3600),
        b'M' => Duration::from_secs(count * 60),
        b'S' => Duration::from_secs(count),
        b'm' => Duration::from_millis(count),
        b'u' => Duration::from_micros(count),
        b'n' => Duration::from_nanos(count),
        _ => return Err(MetadataError::InvalidTimeout),
    })
}

/// Formats the finest representable timeout, rounding up to avoid an early deadline.
///
/// # Errors
/// Rejects durations exceeding 99,999,999 hours rather than silently shortening them.
pub fn format_grpc_timeout(timeout: Duration) -> Result<String, MetadataError> {
    for (unit, nanos) in [
        ('n', 1_u128),
        ('u', 1_000),
        ('m', 1_000_000),
        ('S', 1_000_000_000),
        ('M', 60_000_000_000),
        ('H', 3_600_000_000_000),
    ] {
        let count = timeout.as_nanos().div_ceil(nanos);
        if count <= 99_999_999 {
            return Ok(format!("{count}{unit}"));
        }
    }
    Err(MetadataError::InvalidTimeout)
}

#[cfg(test)]
mod unit_scenario {
    use super::*;

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
            "éS",
        ] {
            assert!(parse_grpc_timeout(invalid).is_err(), "{invalid}");
        }
        assert!(format_grpc_timeout(Duration::MAX).is_err());
        Ok(())
    }
    #[test]
    fn binary_padding_and_invalid_suffix_are_atomic() -> Result<(), MetadataError> {
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
                for invalid in ["A", "YQ===", "_w", "YR", "YQ\n", "é"] {
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
}
