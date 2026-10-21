//! Structured HFT RPCs compiled from Protobuf, Fory and Cap'n Proto IDLs.
use anyhow::{Context, Result};
#[path = "support/hft/mod.rs"]
mod hft;

fn main() -> Result<()> {
    for (codec, samples) in hft::run_all(1).context("IDL-generated HFT service")? {
        println!(
            "{codec}+Zstd TCP: unary order, depth=3, fills=15, bidi=2; four-shape round trip={} us",
            samples[0].as_micros()
        );
    }
    Ok(())
}
