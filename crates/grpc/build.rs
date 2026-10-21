//! Compile only the IDLs selected by the facade's codec features.
use std::{env, path::PathBuf};

use anyhow::Context;

fn main() -> anyhow::Result<()> {
    let root = PathBuf::from(env::var_os("OUT_DIR").context("OUT_DIR is not set")?);
    for (feature, extension, directory) in [
        ("PROTOBUF", "proto", "protobuf"),
        ("FORY", "fdl", "fory"),
        ("CAPNP", "capnp", "capnp"),
    ] {
        let schema = PathBuf::from(format!("../../proto/hft_order.{extension}"));
        println!("cargo:rerun-if-changed={}", schema.display());
        if env::var_os(format!("CARGO_FEATURE_{feature}")).is_some() {
            grpc_build::SchemaCompiler::new(root.join(directory)).compile(&schema)?;
        }
    }
    for tool in ["PROTOC", "FORYC", "PATH"] {
        println!("cargo:rerun-if-env-changed={tool}");
    }
    Ok(())
}
