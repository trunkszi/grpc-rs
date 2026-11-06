//! Integration contract for public error presentation and source traversal.
use std::error::Error;

use grpc::{Status, StreamError};

#[test]
fn integration_stream_error_preserves_status_source_and_display() -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    let error = StreamError::from(Status::invalid_argument("quantity must be positive"));
    ensure!(error.to_string() == "InvalidArgument: quantity must be positive");
    let source = error.source().context("stream error lost Status source")?;
    let source = source
        .downcast_ref::<Status>()
        .context("wrong source type")?;
    ensure!(source.code() == grpc::core::Code::InvalidArgument);
    ensure!(source.source().is_none());
    ensure!(StreamError::WouldBlock.to_string() == "stream would block");
    ensure!(StreamError::WouldBlock.source().is_none());
    Ok(())
}
