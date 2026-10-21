//! Validated application metadata with raw binary values.
//!
//! Binary values remain unencoded until the HTTP/2 boundary applies Base64.
//! Repeated entries retain insertion order. This owned collection allocates on
//! insertion; it makes no allocation guarantees for RPC hot paths.

use std::fmt;

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
}

impl fmt::Display for MetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidKey => "metadata keys must be nonempty lowercase ASCII tokens",
            Self::ReservedKey => "the grpc- metadata prefix is reserved for the protocol",
            Self::ValueKindMismatch => "binary metadata requires a -bin key and raw bytes",
            Self::InvalidAsciiValue => "ASCII metadata values must contain printable ASCII only",
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
