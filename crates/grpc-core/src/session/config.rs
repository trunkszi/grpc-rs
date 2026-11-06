//! HTTP/2 settings and bounded gRPC session configuration.
use std::time::Duration;

use crate::compression::CompressionConfig;

/// Relative request deadline represented by the grpc-timeout header.
pub type Timeout = Duration;

/// HTTP/2 settings and application-owned stream limits.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub max_concurrent_streams: u32,
    pub initial_window_size: u32,
    pub max_frame_size: u32,
    pub max_header_list_size: u32,
    pub max_inbound_message_size: usize,
    pub per_stream_byte_budget: usize,
    pub max_active_streams: usize,
    pub compression: CompressionConfig,
    pub user_agent: String,
    pub content_type: String,
    pub scheme: String,
}
impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100,
            initial_window_size: 1024 * 1024,
            max_frame_size: 16_384,
            max_header_list_size: 64 * 1024,
            max_inbound_message_size: 16 * 1024 * 1024,
            per_stream_byte_budget: 16 * 1024 * 1024,
            max_active_streams: 1024,
            compression: CompressionConfig::default(),
            user_agent: "grpc-rs/0.1.0".into(),
            scheme: "http".into(),
            content_type: "application/grpc".into(),
        }
    }
}

uarch::layout::assert_layout!(SessionConfig, size = 136, align = 8);
