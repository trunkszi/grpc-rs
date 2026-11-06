//! Validated application metadata with raw binary values.
//!
//! Binary values remain unencoded until the HTTP/2 boundary applies Base64.
//! Repeated entries retain insertion order. This owned collection allocates on
//! insertion; it makes no allocation guarantees for RPC hot paths.

use std::time::Duration;

use base64::{
    Engine as _, alphabet,
    engine::{
        DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig, general_purpose::STANDARD_NO_PAD,
    },
};
use uarch::prelude::*;

/// A validated ASCII value or an unencoded binary value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataValue {
    /// Printable ASCII bytes, with horizontal tabs accepted from incoming headers.
    Ascii(String),
    /// Raw bytes associated with a `-bin` key.
    Binary(Vec<u8>),
}

/// Invalid application metadata rejected before insertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MetadataError {
    /// A key is empty or contains a character outside `[a-z0-9_.-]`.
    #[error("metadata keys must be nonempty lowercase ASCII tokens")]
    InvalidKey,
    /// A key uses the protocol-reserved `grpc-` prefix.
    #[error("the grpc- metadata prefix is reserved for the protocol")]
    ReservedKey,
    /// A binary key was paired with ASCII data, or the reverse.
    #[error("binary metadata requires a -bin key and raw bytes")]
    ValueKindMismatch,
    /// An ASCII value violates its incoming or outgoing character policy.
    #[error("metadata values contain invalid ASCII characters")]
    InvalidAsciiValue,
    /// A binary header contains malformed Base64.
    #[error("binary metadata must contain valid standard Base64")]
    InvalidBinaryValue,
    /// A timeout is malformed or cannot fit the eight-digit wire representation.
    #[error("grpc-timeout requires up to eight digits and a supported unit")]
    InvalidTimeout,
}

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
        if binary_key(key) {
            return Err(MetadataError::ValueKindMismatch);
        }
        if !value
            .bytes()
            .all(|byte| predicate((0x20..=0x7e).contains(&byte)).as_bool())
        {
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
        if !binary_key(key) {
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
    /// Incoming ASCII values accept HTTP horizontal tabs as well as printable ASCII.
    ///
    /// # Errors
    /// Rejects reserved keys, invalid ASCII or Base64. Rejection is atomic even
    /// when a later member of a comma-joined binary value is malformed.
    pub fn append_header(&mut self, key: &str, value: &str) -> Result<(), MetadataError> {
        validate_key(key)?;
        if !binary_key(key) {
            if !value
                .bytes()
                .all(|byte| byte == b'\t' || predicate((0x20..=0x7e).contains(&byte)).as_bool())
            {
                return Err(MetadataError::InvalidAsciiValue);
            }
            self.entries
                .push((key.to_owned(), MetadataValue::Ascii(value.to_owned())));
            return Ok(());
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

#[inline]
fn validate_key(key: &str) -> Result<(), MetadataError> {
    if key.is_empty()
        || !key.bytes().all(|byte| {
            ilp_any!(
                byte.is_ascii_lowercase(),
                byte.is_ascii_digit(),
                b"_.-".contains(&byte)
            )
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
#[inline]
pub fn parse_grpc_timeout(value: &str) -> Result<Duration, MetadataError> {
    let bytes = value.as_bytes();
    if !(2..=9).contains(&bytes.len()) {
        return Err(MetadataError::InvalidTimeout);
    }
    let count = if bytes.len() == 9 {
        let word = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        // The high bits detect a digit outside ASCII 0..9 in any byte lane.
        if (word.wrapping_sub(0x3030_3030_3030_3030) | word.wrapping_add(0x4646_4646_4646_4646))
            & 0x8080_8080_8080_8080
            != 0
        {
            return Err(MetadataError::InvalidTimeout);
        }
        let digits = word & 0x0f0f_0f0f_0f0f_0f0f;
        let pairs = (digits * 10 + (digits >> 8)) & 0x00ff_00ff_00ff_00ff;
        let quads = (pairs * 100 + (pairs >> 16)) & 0x0000_ffff_0000_ffff;
        (quads * 10_000 + (quads >> 32)) & 0xffff_ffff
    } else {
        let mut count = 0_u64;
        for byte in &bytes[..bytes.len() - 1] {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return Err(MetadataError::InvalidTimeout);
            }
            count = count * 10 + u64::from(digit);
        }
        count
    };
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
    let nanos = timeout.as_nanos();
    let (unit, count) = if nanos <= 99_999_999 {
        (b'n', nanos as u32)
    } else if nanos <= 99_999_999_000 {
        (b'u', (nanos as u64).div_ceil(1_000) as u32)
    } else if nanos <= 99_999_999_000_000 {
        (b'm', (nanos as u64).div_ceil(1_000_000) as u32)
    } else {
        let seconds = timeout
            .as_secs()
            .checked_add(u64::from(timeout.subsec_nanos() != 0))
            .ok_or(MetadataError::InvalidTimeout)?;
        if seconds <= 99_999_999 {
            (b'S', seconds as u32)
        } else if seconds <= 99_999_999 * 60 {
            (b'M', seconds.div_ceil(60) as u32)
        } else if seconds <= 99_999_999 * 3600 {
            (b'H', seconds.div_ceil(3600) as u32)
        } else {
            return Err(MetadataError::InvalidTimeout);
        }
    };
    let mut buffer = [0_u8; 9];
    buffer[8] = unit;
    let mut cursor = 8;
    let mut count = count;
    loop {
        cursor -= 1;
        buffer[cursor] = b'0' + (count % 10) as u8;
        count /= 10;
        if count == 0 {
            break;
        }
    }
    let mut result = String::with_capacity(9 - cursor);
    result.extend(buffer[cursor..].iter().copied().map(char::from));
    Ok(result)
}

#[hot_inline]
fn binary_key(key: &str) -> bool {
    key.as_bytes().last_chunk::<4>().is_some_and(|tail| {
        ilp_all!(
            tail[0] == b'-',
            tail[1] == b'b',
            tail[2] == b'i',
            tail[3] == b'n'
        )
    })
}
