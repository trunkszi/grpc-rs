//! Four-shape caller-thread RPC latency, excluding connection setup and warmup.
use anyhow::{Context, Result, ensure};
#[path = "../examples/support/order/mod.rs"]
mod order;

fn main() -> Result<()> {
    let iterations = std::env::var("GRPC_BENCH_ITERATIONS")
        .map_or(Ok(32), |value| value.parse::<usize>())
        .context("GRPC_BENCH_ITERATIONS must be an integer")?;
    ensure!(
        (1..=1_000_000).contains(&iterations),
        "benchmark requires 1-1,000,000 iterations"
    );
    for (codec, mut samples) in order::run_all(iterations)? {
        samples.sort_unstable();
        let percentile = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1];
        let total: f64 = samples.iter().map(std::time::Duration::as_secs_f64).sum();
        println!(
            "{codec}+Zstd typed unary/server/client/bidi batch: p50={} ns p99={} ns \
             throughput={:.0} batches/s samples={}",
            percentile(50).as_nanos(),
            percentile(99).as_nanos(),
            samples.len() as f64 / total,
            samples.len()
        );
    }
    Ok(())
}
