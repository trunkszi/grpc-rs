//! Canonical gRPC status codes and their HTTP/2 trailer representation.

/// The seventeen canonical gRPC status codes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(i32)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    /// Returns the canonical code, treating unrecognized values as `Unknown`.
    #[must_use]
    pub const fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Ok,
            1 => Self::Cancelled,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self as i32
    }

    /// Maps an HTTP status when the response has no `grpc-status` trailer.
    ///
    /// Even HTTP 200 maps to `Unknown`: HTTP success alone cannot establish
    /// successful completion of a gRPC call.
    #[must_use]
    pub const fn from_http_status(status: u16) -> Self {
        match status {
            400 => Self::Internal,
            401 => Self::Unauthenticated,
            403 => Self::PermissionDenied,
            404 => Self::Unimplemented,
            429 | 502 | 503 | 504 => Self::Unavailable,
            _ => Self::Unknown,
        }
    }
}

/// A gRPC outcome containing a canonical code and a UTF-8 diagnostic message.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct Status {
    code: Code,
    message: String,
}

macro_rules! constructors {
    ($($name:ident => $code:ident),+ $(,)?) => {
        $(
            #[must_use]
            pub fn $name(message: impl Into<String>) -> Self {
                Self::new(Code::$code, message)
            }
        )+
    };
}

impl Status {
    #[must_use]
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    constructors! {
        ok => Ok,
        cancelled => Cancelled,
        unknown => Unknown,
        invalid_argument => InvalidArgument,
        deadline_exceeded => DeadlineExceeded,
        not_found => NotFound,
        already_exists => AlreadyExists,
        permission_denied => PermissionDenied,
        resource_exhausted => ResourceExhausted,
        failed_precondition => FailedPrecondition,
        aborted => Aborted,
        out_of_range => OutOfRange,
        unimplemented => Unimplemented,
        internal => Internal,
        unavailable => Unavailable,
        data_loss => DataLoss,
        unauthenticated => Unauthenticated,
    }

    #[must_use]
    pub const fn code(&self) -> Code {
        self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Constructs a fallback outcome only when `grpc-status` is absent.
    #[must_use]
    pub fn from_http_status(status: u16) -> Self {
        Self::new(
            Code::from_http_status(status),
            format!("HTTP status {status} without grpc-status"),
        )
    }

    /// Encodes the message for a `grpc-message` trailer.
    #[must_use]
    pub fn encoded_message(&self) -> String {
        encode_grpc_message(&self.message)
    }
}

/// Percent-encodes UTF-8 bytes, leaving only RFC 3986 unreserved bytes literal.
#[must_use]
pub fn encode_grpc_message(message: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(message.len());
    for byte in message.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0xf)]));
        }
    }
    encoded
}

/// Decodes a `grpc-message` without discarding malformed diagnostics.
///
/// Malformed percent escapes remain literal and invalid decoded UTF-8 becomes
/// the replacement character. A literal plus sign remains a plus sign.
#[must_use]
pub fn decode_grpc_message(message: &str) -> String {
    let input = message.as_bytes();
    let mut decoded = Vec::with_capacity(input.len());
    let mut offset = 0;
    while offset < input.len() {
        if input[offset] == b'%'
            && let Some(escape) = input.get(offset + 1..offset + 3)
            && let (Some(high), Some(low)) = (hex_value(escape[0]), hex_value(escape[1]))
        {
            decoded.push((high << 4) | low);
            offset += 3;
        } else {
            decoded.push(input[offset]);
            offset += 1;
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod unit {
    use super::{Code, Status, decode_grpc_message, encode_grpc_message};

    #[test]
    fn canonical_codes_cover_all_seventeen_wire_values() {
        for value in 0..=16 {
            assert_eq!(Code::from_i32(value).as_i32(), value);
        }
        assert_eq!(Code::from_i32(-1), Code::Unknown);
        assert_eq!(Code::from_i32(17), Code::Unknown);
    }

    #[test]
    fn fallback_requires_grpc_status_even_for_http_success() {
        for (http, expected) in [
            (400, Code::Internal),
            (401, Code::Unauthenticated),
            (403, Code::PermissionDenied),
            (404, Code::Unimplemented),
            (429, Code::Unavailable),
            (502, Code::Unavailable),
            (503, Code::Unavailable),
            (504, Code::Unavailable),
            (200, Code::Unknown),
            (500, Code::Unknown),
        ] {
            assert_eq!(Status::from_http_status(http).code(), expected);
        }
    }

    #[test]
    fn grpc_message_encodes_reserved_bytes_and_utf8() {
        let message = "unreserved-._~ /%+\r\n\0中文";
        let encoded = encode_grpc_message(message);
        assert_eq!(
            encoded,
            "unreserved-._~%20%2F%25%2B%0D%0A%00%E4%B8%AD%E6%96%87"
        );
        assert_eq!(decode_grpc_message(&encoded), message);
        assert_eq!(decode_grpc_message("%2f+%7e"), "/+~");
        assert_eq!(decode_grpc_message(""), "");
        assert_eq!(decode_grpc_message("plain text"), "plain text");
    }

    #[test]
    fn malformed_escapes_are_preserved_and_invalid_utf8_is_replaced() {
        for input in ["%", "%0", "%GG", "%0g"] {
            assert_eq!(decode_grpc_message(input), input);
        }
        assert_eq!(decode_grpc_message("%FF"), "\u{fffd}");
        assert_eq!(decode_grpc_message("bad%GG%20message"), "bad%GG message");
        assert_eq!(decode_grpc_message("%C0%AF"), "\u{fffd}\u{fffd}");
    }

    #[test]
    fn status_preserves_code_and_message_for_wire_encoding() {
        let status = Status::internal(String::from("connection lost"));
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), "connection lost");
        assert_eq!(status.encoded_message(), "connection%20lost");
        assert_eq!(status.to_string(), "Internal: connection lost");
    }
}
