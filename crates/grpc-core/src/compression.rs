//! Per-message compression and `grpc-accept-encoding` negotiation.
//!
//! Zstd uses its frame format, LZ4 uses a single standard frame (without legacy
//! or skippable frames), Gzip uses RFC 1952, Deflate uses the RFC 1950 zlib
//! wrapper, and Snappy uses its raw block format. Compression contexts are
//! independent for each gRPC message.

use std::{
    io::{Read, Write},
    str::FromStr,
};

use crate::status::Status;

/// Maximum decoded message size accepted by the default configuration.
pub const DEFAULT_MAX_DECODING_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
/// Messages smaller than this are sent without compression by default.
pub const DEFAULT_MIN_COMPRESS_SIZE_BYTES: usize = 256;

/// A supported value of the `grpc-encoding` header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CompressionAlgorithm {
    /// Zstandard frames, compressed at level 1.
    #[default]
    Zstd,
    /// LZ4 frames.
    Lz4,
    /// RFC 1952 gzip streams.
    Gzip,
    /// RFC 1950 zlib-wrapped deflate streams.
    Deflate,
    /// Snappy raw blocks.
    Snappy,
    /// Uncompressed bytes.
    Identity,
}

impl CompressionAlgorithm {
    /// The lowercase wire token used in gRPC encoding headers.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
            Self::Snappy => "snappy",
            Self::Identity => "identity",
        }
    }

    /// Parses an exact, case-sensitive gRPC encoding token.
    ///
    /// # Errors
    /// Returns `Unimplemented` for an unsupported encoding.
    pub fn from_encoding(encoding: &str) -> Result<Self, Status> {
        match encoding {
            "zstd" => Ok(Self::Zstd),
            "lz4" => Ok(Self::Lz4),
            "gzip" => Ok(Self::Gzip),
            "deflate" => Ok(Self::Deflate),
            "snappy" => Ok(Self::Snappy),
            "identity" => Ok(Self::Identity),
            _ => Err(Status::unimplemented("unsupported grpc-encoding")),
        }
    }

    /// Compresses a complete message without applying the adaptive threshold.
    ///
    /// # Errors
    /// Returns `Internal` if a compressor fails.
    pub fn compress(self, input: &[u8]) -> Result<Vec<u8>, Status> {
        let failed = |_| Status::internal("message compression failed");
        match self {
            Self::Identity => Ok(input.to_vec()),
            Self::Zstd => zstd::stream::encode_all(input, 1).map_err(failed),
            Self::Lz4 => {
                let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
                encoder.write_all(input).map_err(failed)?;
                encoder
                    .finish()
                    .map_err(|_| Status::internal("LZ4 compression failed"))
            }
            Self::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                encoder.write_all(input).map_err(failed)?;
                encoder.finish().map_err(failed)
            }
            Self::Deflate => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
                encoder.write_all(input).map_err(failed)?;
                encoder.finish().map_err(failed)
            }
            Self::Snappy => snap::raw::Encoder::new()
                .compress_vec(input)
                .map_err(|_| Status::internal("Snappy compression failed")),
        }
    }

    /// Decodes a message with a strict limit on the returned byte count.
    ///
    /// Streaming decoders read at most one byte beyond the limit and never
    /// reserve output memory from an untrusted advertised decompressed size.
    /// Zstd's decoding window is capped at 16 MiB independently of this limit;
    /// LZ4 frame blocks are bounded by the format's 4 MiB maximum.
    ///
    /// # Errors
    /// Returns `ResourceExhausted` if output exceeds `max_size`, or
    /// `Internal` if the compressed stream is malformed.
    pub fn decompress(self, input: &[u8], max_size: usize) -> Result<Vec<u8>, Status> {
        match self {
            Self::Identity => copy_bounded(input, max_size),
            Self::Zstd => {
                let mut decoder =
                    zstd::stream::read::Decoder::new(input).map_err(|_| malformed())?;
                decoder.window_log_max(24).map_err(|_| malformed())?;
                read_bounded(decoder, max_size)
            }
            Self::Lz4 => {
                validate_lz4_extent(input)?;
                read_bounded(
                    Lz4MessageReader(lz4_flex::frame::FrameDecoder::new(StrictFrameReader(input))),
                    max_size,
                )
            }
            Self::Gzip => read_bounded(flate2::read::MultiGzDecoder::new(input), max_size),
            Self::Deflate => decode_deflate(input, max_size),
            Self::Snappy => {
                let size = snap::raw::decompress_len(input).map_err(|_| malformed())?;
                if size > max_size {
                    return Err(too_large());
                }
                let mut output = vec![0; size];
                let written = snap::raw::Decoder::new()
                    .decompress(input, &mut output)
                    .map_err(|_| malformed())?;
                output.truncate(written);
                Ok(output)
            }
        }
    }
}

impl FromStr for CompressionAlgorithm {
    type Err = Status;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_encoding(value)
    }
}

/// Payload bytes and the value of the gRPC frame's compressed flag.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompressedMessage {
    pub compressed: bool,
    pub data: Vec<u8>,
}

/// Compression policy for outgoing and incoming messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub min_compress_size_bytes: usize,
    pub max_decoding_message_size: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::default(),
            min_compress_size_bytes: DEFAULT_MIN_COMPRESS_SIZE_BYTES,
            max_decoding_message_size: DEFAULT_MAX_DECODING_MESSAGE_SIZE,
        }
    }
}

impl CompressionConfig {
    /// Compresses only when the threshold is met and the result is smaller.
    ///
    /// # Errors
    /// Returns `Internal` if the selected compressor fails.
    pub fn compress(self, input: &[u8]) -> Result<CompressedMessage, Status> {
        if self.algorithm != CompressionAlgorithm::Identity
            && input.len() >= self.min_compress_size_bytes
        {
            let data = self.algorithm.compress(input)?;
            if data.len() < input.len() {
                return Ok(CompressedMessage {
                    compressed: true,
                    data,
                });
            }
        }
        Ok(CompressedMessage {
            compressed: false,
            data: input.to_vec(),
        })
    }

    /// Decodes according to a frame's compressed flag, enforcing the size limit
    /// for both compressed and uncompressed messages.
    ///
    /// # Errors
    /// Returns `Internal` if compressed bytes have no selected encoding
    /// or are malformed, and `ResourceExhausted` if the decoded limit is exceeded.
    pub fn decompress(self, input: &[u8], compressed: bool) -> Result<Vec<u8>, Status> {
        if !compressed {
            return copy_bounded(input, self.max_decoding_message_size);
        }
        if self.algorithm == CompressionAlgorithm::Identity {
            return Err(Status::internal(
                "compressed flag requires a non-identity encoding",
            ));
        }
        self.algorithm
            .decompress(input, self.max_decoding_message_size)
    }
}

/// Selects the first locally preferred encoding explicitly accepted by a peer.
/// Unknown tokens are ignored; identity is always available as the fallback.
/// An absent header can be represented by an empty string. Parameters and
/// wildcards are not part of the gRPC accept-encoding grammar.
pub fn negotiate_compression(
    preferred: &[CompressionAlgorithm],
    peer_accept_encoding: &str,
) -> CompressionAlgorithm {
    preferred
        .iter()
        .copied()
        .find(|algorithm| {
            *algorithm == CompressionAlgorithm::Identity
                || peer_accept_encoding
                    .split(',')
                    .any(|token| token.trim() == algorithm.as_str())
        })
        .unwrap_or(CompressionAlgorithm::Identity)
}

/// Formats a comma-separated header, removing duplicate algorithms.
pub fn accept_encoding_header(algorithms: &[CompressionAlgorithm]) -> String {
    let mut header = String::new();
    for (index, algorithm) in algorithms.iter().enumerate() {
        if algorithms[..index].contains(algorithm) {
            continue;
        }
        if !header.is_empty() {
            header.push(',');
        }
        header.push_str(algorithm.as_str());
    }
    header
}

// LZ4's reader treats EOF before an end marker as a clean end of input.
// An exhausted source must instead fail until the decoder consumes that marker.
struct StrictFrameReader<'a>(&'a [u8]);

// The upstream decoder also returns zero for a data block that produces no
// bytes. Only the validated frame boundary establishes message completion.
struct Lz4MessageReader<'a>(lz4_flex::frame::FrameDecoder<StrictFrameReader<'a>>);

impl Read for Lz4MessageReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let count = self.0.read(output)?;
            if count != 0 || output.is_empty() || self.0.get_ref().0.is_empty() {
                return Ok(count);
            }
        }
    }
}

// Check block extents independently of decoded byte counts. Header validity,
// block contents, declared content size and checksums remain upstream's job.
fn validate_lz4_extent(input: &[u8]) -> Result<(), Status> {
    if input.get(..4) != Some(&[0x04, 0x22, 0x4d, 0x18]) {
        return Err(malformed());
    }
    let flags = *input.get(4).ok_or_else(malformed)?;
    let header_size = 7 + usize::from(flags & 8 != 0) * 8 + usize::from(flags & 1 != 0) * 4;
    let mut remaining = input.get(header_size..).ok_or_else(malformed)?;
    loop {
        let header = remaining.get(..4).ok_or_else(malformed)?;
        let size = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        remaining = &remaining[4..];
        if size == 0 {
            let checksum_size = usize::from(flags & 4 != 0) * 4;
            return if remaining.len() == checksum_size {
                Ok(())
            } else {
                Err(malformed())
            };
        }
        let data_size = usize::try_from(size & 0x7fff_ffff).map_err(|_| malformed())?;
        let block_size = data_size
            .checked_add(usize::from(flags & 16 != 0) * 4)
            .ok_or_else(malformed)?;
        remaining = remaining.get(block_size..).ok_or_else(malformed)?;
    }
}

impl Read for StrictFrameReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.0.is_empty() && !output.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing LZ4 end marker",
            ));
        }
        self.0.read(output)
    }
}

fn decode_deflate(input: &[u8], max_size: usize) -> Result<Vec<u8>, Status> {
    let mut decoder = flate2::Decompress::new(true);
    let mut output = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        let before_in = decoder.total_in();
        let before_out = decoder.total_out();
        let remaining = max_size - output.len();
        let wanted = chunk.len().min(remaining.saturating_add(1));
        let state = decoder
            .decompress(
                &input[before_in as usize..],
                &mut chunk[..wanted],
                flate2::FlushDecompress::None,
            )
            .map_err(|_| malformed())?;
        let count = (decoder.total_out() - before_out) as usize;
        if count > remaining {
            return Err(too_large());
        }
        output.extend_from_slice(&chunk[..count]);
        if state == flate2::Status::StreamEnd {
            if decoder.total_in() != input.len() as u64 {
                return Err(malformed());
            }
            return Ok(output);
        }
        if before_in == decoder.total_in() && count == 0 {
            return Err(malformed());
        }
    }
}

fn copy_bounded(input: &[u8], max_size: usize) -> Result<Vec<u8>, Status> {
    if input.len() > max_size {
        return Err(too_large());
    }
    Ok(input.to_vec())
}

fn read_bounded(mut reader: impl Read, max_size: usize) -> Result<Vec<u8>, Status> {
    let mut output = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        let remaining = max_size - output.len();
        let wanted = chunk.len().min(remaining.saturating_add(1));
        let count = reader.read(&mut chunk[..wanted]).map_err(|error| {
            // libzstd exposes its window admission error through io::Error's
            // message rather than a typed source or a distinct ErrorKind.
            if error.to_string() == "Frame requires too much memory for decoding" {
                Status::resource_exhausted("Zstd decoding window exceeds 16 MiB")
            } else {
                malformed()
            }
        })?;
        if count == 0 {
            return Ok(output);
        }
        if count > remaining {
            return Err(too_large());
        }
        output.extend_from_slice(&chunk[..count]);
    }
}

fn too_large() -> Status {
    Status::resource_exhausted("decoded message exceeds configured size limit")
}

fn malformed() -> Status {
    Status::internal("malformed compressed message")
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::status::Code;

    const ALGORITHMS: [CompressionAlgorithm; 6] = [
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Deflate,
        CompressionAlgorithm::Snappy,
        CompressionAlgorithm::Identity,
    ];

    #[test]
    fn all_algorithms_round_trip_and_enforce_exact_limit() {
        let payload = vec![b'x'; 32768];
        for algorithm in ALGORITHMS {
            let encoded = algorithm.compress(&payload).unwrap();
            assert_eq!(
                algorithm.decompress(&encoded, payload.len()).unwrap(),
                payload
            );
            assert_eq!(
                algorithm
                    .decompress(&encoded, payload.len() - 1)
                    .unwrap_err()
                    .code(),
                Code::ResourceExhausted
            );
            let empty = algorithm.compress(&[]).unwrap();
            assert_eq!(algorithm.decompress(&empty, 0).unwrap(), b"");
        }
    }

    #[test]
    fn adaptive_threshold_and_negative_compression_keep_flag_clear() {
        for algorithm in ALGORITHMS {
            let config = CompressionConfig {
                algorithm,
                ..CompressionConfig::default()
            };
            let small = vec![b'x'; 255];
            let output = config.compress(&small).unwrap();
            assert!(!output.compressed);
            assert_eq!(output.data, small);
            let output = config.compress(&vec![b'x'; 256]).unwrap();
            assert_eq!(
                output.compressed,
                algorithm != CompressionAlgorithm::Identity
            );
            assert_eq!(
                config.decompress(&output.data, output.compressed).unwrap(),
                vec![b'x'; 256]
            );
            let config = CompressionConfig {
                min_compress_size_bytes: 0,
                ..config
            };
            let output = config.compress(b"a").unwrap();
            assert!(!output.compressed);
            assert_eq!(output.data, b"a");
        }
    }

    #[test]
    fn scenario_high_expansion_streams_stop_at_small_limit() {
        let payload = vec![0; 1024 * 1024];
        for algorithm in ALGORITHMS {
            let encoded = algorithm.compress(&payload).unwrap();
            assert_eq!(
                algorithm.decompress(&encoded, 1024).unwrap_err().code(),
                Code::ResourceExhausted
            );
        }
    }

    #[test]
    fn scenario_default_limit_rejects_large_output_and_zstd_window() {
        let config = CompressionConfig::default();
        let payload = vec![0; DEFAULT_MAX_DECODING_MESSAGE_SIZE + 1];
        let encoded = config.algorithm.compress(&payload).unwrap();
        assert_eq!(
            config.decompress(&encoded, true).unwrap_err().code(),
            Code::ResourceExhausted
        );
        // A standard frame with a 32 MiB window and an empty final block.
        let large_window = [0x28, 0xb5, 0x2f, 0xfd, 0, 0x78, 1, 0, 0];
        assert_eq!(
            config.decompress(&large_window, true).unwrap_err().code(),
            Code::ResourceExhausted
        );
    }

    #[test]
    fn scenario_corrupt_compressed_input_returns_status() {
        for algorithm in ALGORITHMS
            .into_iter()
            .filter(|a| *a != CompressionAlgorithm::Identity)
        {
            let config = CompressionConfig {
                algorithm,
                max_decoding_message_size: 1024,
                ..CompressionConfig::default()
            };
            let corrupt = b"not a compressed stream";
            assert_eq!(
                algorithm.decompress(corrupt, 1024).unwrap_err().code(),
                Code::Internal
            );
            assert_eq!(
                config.decompress(corrupt, true).unwrap_err().code(),
                Code::Internal
            );
        }
    }

    #[test]
    fn scenario_truncated_streams_are_rejected() {
        for algorithm in ALGORITHMS
            .into_iter()
            .filter(|a| *a != CompressionAlgorithm::Identity)
        {
            let encoded = algorithm.compress(&vec![b'x'; 1024]).unwrap();
            for length in 0..encoded.len() {
                assert_eq!(
                    algorithm
                        .decompress(&encoded[..length], 2048)
                        .unwrap_err()
                        .code(),
                    Code::Internal,
                    "{algorithm:?} truncated stream of {length}/{} bytes",
                    encoded.len()
                );
            }
        }
    }

    #[test]
    fn scenario_trailing_garbage_is_not_silently_discarded() {
        for algorithm in ALGORITHMS
            .into_iter()
            .filter(|a| *a != CompressionAlgorithm::Identity)
        {
            let encoded = algorithm.compress(b"message").unwrap();
            for suffix in [&b"\0"[..], &b"garbage"[..], &[255; 16][..]] {
                let mut trailing = encoded.clone();
                trailing.extend_from_slice(suffix);
                assert_eq!(
                    algorithm.decompress(&trailing, 1024).unwrap_err().code(),
                    Code::Internal,
                    "{algorithm:?} accepted trailing {suffix:?}"
                );
            }
        }
    }

    #[test]
    fn scenario_lz4_empty_blocks_require_end_marker_and_preserve_following_data() {
        let algorithm = CompressionAlgorithm::Lz4;
        let empty = algorithm.compress(b"").unwrap();
        let following = algorithm.compress(b"following block").unwrap();
        for block in [&[0, 0, 0, 0x80][..], &[1, 0, 0, 0, 0][..]] {
            let mut encoded = empty[..empty.len() - 4].to_vec();
            encoded.extend_from_slice(block);
            assert_eq!(
                algorithm.decompress(&encoded, 64).unwrap_err().code(),
                Code::Internal
            );
            encoded.extend_from_slice(&following[7..]);
            assert_eq!(
                algorithm.decompress(&encoded, 64).unwrap(),
                b"following block"
            );
            assert_eq!(
                algorithm.decompress(&encoded, 14).unwrap_err().code(),
                Code::ResourceExhausted
            );
        }
    }

    #[test]
    fn scenario_lz4_optional_fields_and_linked_blocks_round_trip() {
        let payload = vec![b'x'; 200_000];
        let info = lz4_flex::frame::FrameInfo::new()
            .block_size(lz4_flex::frame::BlockSize::Max64KB)
            .block_mode(lz4_flex::frame::BlockMode::Linked)
            .content_size(Some(payload.len() as u64))
            .block_checksums(true)
            .content_checksum(true);
        let mut encoder = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
        encoder.write_all(&payload).unwrap();
        let encoded = encoder.finish().unwrap();
        assert_eq!(
            CompressionAlgorithm::Lz4
                .decompress(&encoded, payload.len())
                .unwrap(),
            payload
        );
        for length in [encoded.len() - 1, encoded.len() - 4] {
            assert_eq!(
                CompressionAlgorithm::Lz4
                    .decompress(&encoded[..length], payload.len())
                    .unwrap_err()
                    .code(),
                Code::Internal
            );
        }
        let first_block_size = u32::from_le_bytes(encoded[15..19].try_into().unwrap());
        let block_checksum_offset = 19 + (first_block_size & 0x7fff_ffff) as usize;
        for offset in [block_checksum_offset, encoded.len() - 1] {
            let mut corrupt = encoded.clone();
            corrupt[offset] ^= 1;
            assert_eq!(
                CompressionAlgorithm::Lz4
                    .decompress(&corrupt, payload.len())
                    .unwrap_err()
                    .code(),
                Code::Internal
            );
        }
    }

    #[test]
    fn scenario_concatenated_streams_preserve_bytes_and_enforce_combined_limit() {
        for algorithm in [CompressionAlgorithm::Zstd, CompressionAlgorithm::Gzip] {
            let mut encoded = algorithm.compress(b"first").unwrap();
            encoded.extend(algorithm.compress(b"second").unwrap());
            assert_eq!(algorithm.decompress(&encoded, 11).unwrap(), b"firstsecond");
            assert_eq!(
                algorithm.decompress(&encoded, 10).unwrap_err().code(),
                Code::ResourceExhausted
            );
            encoded.pop();
            assert_eq!(
                algorithm.decompress(&encoded, 11).unwrap_err().code(),
                Code::Internal
            );
        }
        for algorithm in [
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Deflate,
            CompressionAlgorithm::Snappy,
        ] {
            let mut encoded = algorithm.compress(b"first").unwrap();
            encoded.extend(algorithm.compress(b"second").unwrap());
            assert_eq!(
                algorithm.decompress(&encoded, 11).unwrap_err().code(),
                Code::Internal
            );
        }
    }

    #[test]
    fn uncompressed_frames_obey_limits_and_identity_rejects_compressed_flag() {
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Identity,
            max_decoding_message_size: 3,
            ..CompressionConfig::default()
        };
        assert_eq!(
            config.decompress(b"four", false).unwrap_err().code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            config.decompress(b"one", true).unwrap_err().code(),
            Code::Internal
        );
    }

    #[test]
    fn negotiation_uses_exact_tokens_and_local_preference() {
        use CompressionAlgorithm::{Gzip, Identity, Lz4, Zstd};
        assert_eq!(negotiate_compression(&[Zstd, Gzip], "gzip, zstd"), Zstd);
        assert_eq!(negotiate_compression(&[Zstd, Gzip], "other, gzip"), Gzip);
        for value in ["", "ZSTD", "x-zstd", "*", "zstd;q=1"] {
            assert_eq!(negotiate_compression(&[Zstd, Gzip], value), Identity);
        }
        assert_eq!(
            accept_encoding_header(&[Zstd, Lz4, Zstd, Identity]),
            "zstd,lz4,identity"
        );
        for algorithm in ALGORITHMS {
            assert_eq!(
                algorithm.as_str().parse::<CompressionAlgorithm>().unwrap(),
                algorithm
            );
        }
        assert_eq!(
            CompressionAlgorithm::from_encoding("unknown")
                .unwrap_err()
                .code(),
            Code::Unimplemented
        );
    }
}
