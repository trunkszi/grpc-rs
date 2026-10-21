//! E2E wire failures and IDL-generated structured messages across real TCP.
#[path = "hft_e2e_parity/wire_scenarios.rs"]
mod wire_scenarios;

#[cfg(all(feature = "protobuf", feature = "fory", feature = "capnp"))]
#[path = "../examples/support/hft/mod.rs"]
mod hft;

#[cfg(all(feature = "protobuf", feature = "fory", feature = "capnp"))]
#[test]
fn e2e_generated_hft_four_shapes_all_codecs() -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    let samples = hft::run_all(2).context("structured IDL parity")?;
    ensure!(samples.len() == 3, "all three codecs must execute");
    ensure!(samples.iter().all(|(_, samples)| samples.len() == 2));
    Ok(())
}
