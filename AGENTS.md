# AGENTS.md

Operational contract for coding agents (OpenAI Codex, Claude Code, Gemini CLI) working in `grpc-rs`. Treat this file as the authoritative source for repository architecture, build commands, verification gates, and coding invariants.

## Project Overview

`grpc-rs` is a strictly synchronous, single-threaded Rust 2024 gRPC framework built on [`nexora-proto`](https://github.com/trunkszi/nexora-rs), supporting Protocol Buffers (`prost`), Apache Fory (`fory`), Cap'n Proto (`capnp`), and custom `Codec` implementations. Connections, HTTP/2 sessions, and streaming handles are driven directly on the caller's thread without a background executor. TCP and Unix I/O use a lazily initialized calling-thread `liburing::Proactor`; transports retain their `Send` contract across connection handoff.

## Workspace Layout

```text
grpc-rs/
|-- Cargo.toml                        # Workspace members, dependencies, lints, profiles
|-- Cargo.lock                        # Locked dependency graph and Git revisions
|-- LICENSE                           # Apache-2.0 License (upstream gRPC)
|-- README.md                         # User-facing architecture and API documentation
|-- rust-toolchain.toml               # Pinned nightly toolchain and components
|-- rustfmt.toml                      # Workspace formatting configuration
|-- proto/
|   |-- order.proto                   # Protocol Buffers order messages and service
|   |-- order.fdl                     # Apache Fory order messages and service
|   +-- order.capnp                   # Cap'n Proto order messages and streaming annotations
|-- crates/
|   |-- grpc-core/                    # L1: framing, compression, metadata, status, HTTP/2 sessions, transports
|   |-- grpc-codec/
|   |   |-- prost/                    # L2: `ProstCodec<E, D>` Protobuf payload serializer
|   |   |-- fory/                     # L2: `ForyCodec<E, D>` Apache Fory payload serializer
|   |   +-- capnp/                    # L2: `CapnpCodec<E, D>` Cap'n Proto serializer and aligned borrowed readers
|   |-- grpc-macros/                  # L2: `#[grpc_service]` proc-macro for synchronous traits, clients, servers
|   |-- grpc-build/                   # L2: build-time `.proto`, `.fdl`, and `.capnp` compiler driver
|   +-- grpc/                         # L3: public facade (`SyncChannel`, `SyncServer`, streaming handles)
|       |-- build.rs                  # Feature-selected IDL compilation into `OUT_DIR`
|       |-- examples/order_service.rs # Four-shape RPC example across Protobuf, Fory, and Cap'n Proto
|       |-- benches/rpc_bench.rs      # Four-shape caller-thread RPC batch latency benchmark
|       +-- tests/                    # Lifecycle, wire failure, and end-to-end parity tests
+-- .github/
    +-- workflows/ci.yml              # GitHub Actions workflow
```

## Crate Layering & Feature Selection

| Layer | Crate | Responsibilities & Dependency Boundaries |
| --- | --- | --- |
| L1 | `grpc-core` | Depends on `nexora-proto` and `liburing-rs`; owns `Codec`, `AlignedMessageBuf`, `LengthPrefixedFrame`, `Metadata`, `Status`, compression, `GrpcClientSession`, `GrpcServerSession`, and `SyncTransport` (`MemoryPipe`, `TcpTransport`, `UnixTransport`, `TlsTransport`). Does not depend on codec crates. |
| L2 | `grpc-codec-*` | Depend on `grpc-core` and their respective serialization backend (`prost`, `fory`, `capnp`). |
| L2 | `grpc-macros` | Expands `#[grpc_service]` traits, clients, servers, and method descriptors referencing `grpc_core`. |
| L2 | `grpc-build` | Invokes external `protoc`, `foryc`, and `capnp` compilers plus optional Rust message codegen (`prost-build`, `capnpc`) and renders `*_grpc.rs` service modules. |
| L3 | `grpc` | Facade crate re-exporting `grpc-core`, `grpc-macros`, and selected codec adapters (`protobuf` default, `fory`, `capnp`). |

`crates/grpc/build.rs` compiles only the checked-in `proto/order.{proto,fdl,capnp}` schemas enabled by active `grpc` features. Override external compiler paths via `PROTOC` or `FORYC` environment variables or `SchemaCompiler::{protoc_path, foryc_path, capnp_path}`.

## Build, Lint, and Test Commands

Run all commands from the workspace root:

| Command | Purpose |
| --- | --- |
| `cargo fmt --all -- --check` | Check formatting across all workspace crates |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Check default-feature targets without warnings |
| `cargo test --workspace --locked` | Test the default workspace feature graph |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` | Build strict default-feature API documentation |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | Strict lint gate across all targets and codec features |
| `cargo test --locked --workspace --all-targets --all-features` | Run unit, integration, E2E, and benchmark targets with all codecs |
| `cargo test --locked -p grpc --no-default-features --features fory` | Verify standalone Apache Fory build without `prost` |
| `cargo run -p grpc --all-features --example order_service` | Run the four-shape RPC example across Protobuf, Fory, and Cap'n Proto |
| `cargo bench -p grpc --all-features --bench rpc_bench` | Run the four-shape RPC batch latency benchmark |
| `git diff --check` | Check for whitespace or conflict marker errors |

Before completing code changes, run formatting, Clippy, tests, and strict rustdoc directly through Cargo. When modifying `crates/grpc/examples/`, `benches/`, or `proto/`, also run `cargo run -p grpc --all-features --example order_service`.

## Hard Invariants & Coding Standards

- **Platform & Toolchain**: Linux `x86_64`, Rust edition `2024`, pinned nightly toolchain in `rust-toolchain.toml`.
- **Synchronous caller ownership**: no language-level suspension, background executor, shared-state locks, or standard channel primitives. Preserve the forbidden identifier and resolved dependency policy across all checkout text.
- **File Size & Checkout Governance**:
  - Keep authored source, documentation, and configuration ASCII and at most 800 lines. Split modules by cohesive responsibility; independent test files belong under crate `tests/`, never `src/tests.rs`. The generated `Cargo.lock` is exempt.
  - Every tracked or non-ignored `.rs`, `.c`, `.cc`, `.cpp`, `.h`, `.hpp`, `.sh`, and `.py` file must be `<= 800` lines.
  - Symlinks are forbidden in governed checkout files.
- **Manifest Conventions**: Every dependency entry in every `Cargo.toml` must use inline table syntax `key = { ... }`. Declare shared dependencies in root `[workspace.dependencies]` and inherit via `{ workspace = true }`.
- **Error Handling**: Library error types derive `thiserror::Error`; wire and protocol failures return typed `grpc_core::Status`. Build scripts, examples, benchmarks, and fallible test entry points use `anyhow::Result`.
- **Safety & Documentation**:
  - Every `unsafe` block requires a preceding `// SAFETY:` comment; every `unsafe fn` requires a `# Safety` rustdoc section; fallible or panicking public functions require `# Errors` / `# Panics` sections.
  - All repository text (source comments, rustdoc, error messages, logs, and documentation) must be written in ASCII English; express non-ASCII test inputs with Rust Unicode escapes.

## Proactor Ownership & Socket I/O

- Source `liburing` from `https://github.com/trunkszi/liburing-rs.git`, branch `main`, through root workspace dependencies.
- Keep `TcpTransport`, `UnixTransport`, and `MemoryPipe` movable across threads after each completed operation. Each thread lazily owns its own `!Send` Proactor and stable 1 MiB boxed DMA allocation.
- Route socket connect, accept, send, receive, and notification waits through SQEs. Nonblocking data operations set `MSG_DONTWAIT`; blocking deadlines use `ops::Timeout`. Nonblocking accept uses `IORING_ACCEPT_DONTWAIT`, with a minimal cancellable kernel timeout on older kernels.
- Copy caller bytes through persistent DMA storage; cap each partial transfer at its fixed capacity. Normal data calls neither allocate a new data buffer nor duplicate descriptors. Cache the immutable upstream Timeout operand for repeated durations; changed deadlines rebuild that operand.
- Keep raw descriptors borrowed and DMA/address/timeout operands owned through every target and cancellation CQE. Reuse the ring only after complete retirement. Failed shutdown quarantines operands and attempts descriptor duplication only on that exceptional path.
- Preserve `get_ref` and `into_inner`; observe socket options modified through the standard streams. Address resolution and socket construction/configuration remain cold standard-library operations.
- `MemoryPipe` uses two exact-capacity atomic SPSC byte lanes and a Unix notification pair. Publish bytes/room and peer closure with release ordering, observe them with acquire ordering, and recheck state after draining notifications before registering a wait.
- Peer closure drains published bytes before EOF and rejects subsequent writes with BrokenPipe. Signal saturation coalesces wakeups; unexpected notification failures publish a terminal error and shut down the notification socket.
- The upstream Proactor uses the pure Rust io-uring backend without C liburing bindings or libclang. Runtime validation requires Linux with usable io_uring, loopback networking, and the native Zstd build prerequisites. Setup failure is unavailable validation.

## Protocol & Lifecycle Invariants

- TLS `handshake_until(Some(deadline))` uses the remaining absolute budget before each encrypted read, write and flush, and restores both original underlying timeouts on every normal return. `SyncTransport::{read_timeout, write_timeout}` are required methods; transport wrappers delegate these queries to the underlying stream. `handshake()` and `handshake_until(None)` preserve configured per-operation timeouts without querying them. Restoration failure disables further application I/O.
- Preserve single-threaded ownership of connections, HTTP/2 sessions, and stream handles (`!Send` where applicable); transfer transports across threads only at explicit connection boundaries.
- Enforce bounded framing, decompression (`max_decoding_message_size`, 16 MiB Zstd window cap), per-stream queues, and timeout parsing/formatting.
- Preserve completed inbound messages on stream close and drain outbound `DATA` and trailing `HEADERS` (`grpc-status`) before terminating the send side.
- Ensure `AlignedMessageBuf` maintains 8-byte payload alignment for zero-copy Cap'n Proto `read_aligned` without altering the 5-byte wire header produced by `wire_bytes()`.

## Change Hygiene

- Inspect `git status` and `git diff` before and after edits; keep `Cargo.lock` synchronized when manifest or Git dependencies change.
- Do not commit, push, reset, or rewrite history unless explicitly instructed by the user.
- Update `AGENTS.md` and `README.md` in the same change whenever crate names, schema paths, features, commands, or public API contracts change.
