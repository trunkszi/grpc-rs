# grpc-rs Agent Guide

grpc-rs is a Rust 2024 synchronous gRPC workspace built on nghttp2-rs. Connections,
codecs and stream handles run on the calling thread. Transport readiness drives
progress; no executor or background runtime is required.

## Workspace

```text
proto/hft_order.proto             Protocol Buffers HFT messages and service
proto/hft_order.fdl               Apache Fory HFT messages and service
proto/hft_order.capnp             Cap'n Proto HFT messages and service annotations
crates/grpc-core/                 Framing, compression, metadata, sessions, transports
crates/grpc-codec/prost/          Protocol Buffers codec
crates/grpc-codec/fory/           Apache Fory codec
crates/grpc-codec/capnp/          Cap'n Proto codec
crates/grpc-macros/              Synchronous service attribute macro
crates/grpc-build/               Schema compilation and service lowering
crates/grpc/                     Channel, server, streaming facade and build.rs
.github/scripts/                Isolation, toolchain and CI gates
```

Root Cargo.toml owns dependency versions, Git sources, lint settings and profiles.
Cargo.lock pins the tested Git revisions. Generated Rust belongs in OUT_DIR;
checked-in IDLs are the source of truth.

| Layer | Dependencies and responsibility |
|---|---|
| grpc-core | nghttp2-rs, synchronous transport and protocol state machines; no codec adapters |
| grpc-codec-* | grpc-core and the selected serialization backend |
| grpc-macros | Compile-time service API generation; emitted code uses grpc-core |
| grpc-build | External IDL compilers and optional Rust message generators |
| grpc | grpc-core, grpc-macros and feature-selected codec adapters; grpc-build at build time |

## Features

| Package / feature | Behavior |
|---|---|
| grpc / protobuf (default) | Prost codec and Protobuf IDL generation |
| grpc / fory | Fory codec and foryc-generated messages; independently usable without Prost |
| grpc / capnp | Cap'n Proto codec and capnpc Rust message generation |
| grpc-build / protobuf (default) | prost-build message generation with configured protoc |
| grpc-build / capnp (default) | capnpc Rust message generation with configured capnp |

crates/grpc/build.rs compiles the HFT IDL for each enabled codec. SchemaCompiler
also lowers synchronous service traits and descriptors into *_grpc.rs. Fory's
native generated RPC bindings are separate from this framework's synchronous
service bindings. Cap'n Proto method annotations express gRPC cardinality.

## Commands

Run from the workspace root. Install protoc, foryc and capnp for their selected
codec features. PROTOC and FORYC override compiler discovery. NGHTTP2_DIR may
select an existing native nghttp2 installation; otherwise the upstream build
handles its native dependency.

| Command | Purpose |
|---|---|
| cargo fmt --all -- --check | Formatting |
| cargo clippy --workspace --all-targets --all-features -- -D warnings | Strict lint gate |
| cargo test --workspace --all-targets --all-features | Unit, integration, E2E and benchmark targets |
| cargo check -p grpc --no-default-features --features fory | Fory-only build |
| cargo run -p grpc --all-features --example hft_order_service | Typed HFT RPCs for three codecs and four cardinalities |
| cargo bench -p grpc --all-features --bench hft_rpc_bench | Four-shape RPC latency batches |
| bash .github/scripts/ci-incremental.sh | Canonical isolated CI entry point |
| git diff --check | Patch whitespace |

## Hard constraints

- Keep each source file (.rs, .c, .h) at most 800 lines. Split by responsibility.
- Every dependency declaration uses an inline table and inherits shared versions
  from workspace.dependencies. Preserve root lint and profile settings.
- Use thiserror for library errors with stable Display text and source chains.
  Use anyhow for example, benchmark and integration harness context. Protocol
  failures remain typed grpc-core::Status values.
- Keep dependencies synchronous: async runtimes, futures and async RPC stacks
  are outside this framework's dependency graph.
- Use grpc-build for IDL message and service generation. Exercise generated
  messages through real codec and transport boundaries, not string stand-ins.
- Every unsafe block needs an accurate SAFETY comment; public unsafe functions
  need a Safety section. Fallible public functions document Errors; functions
  with reachable panics document Panics. Preserve ownership across callbacks.
- Keep connection/session handles single-threaded. Move transport ownership to
  another thread only at an explicit application boundary.
- Maintain bounded framing, decoding, stream and timeout budgets. Return named
  errors on malformed input and preserve completed messages during shutdown.
- Keep local planning artifacts ignored and untracked. Never stage ignored paths.
- Before committing, inspect the final diff and changed file set. Commit or push
  only when requested; preserve unrelated worktree edits.
