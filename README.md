# grpc-rs

A strictly synchronous, single-threaded gRPC framework for HFT services and
Run-to-Completion applications, built on [nghttp2-rs](https://github.com/trunkszi/nghttp2-rs).
Message serialization is independent of the HTTP/2 engine: use Protocol Buffers,
Apache Fory, Cap'n Proto, or a custom `Codec` without an asynchronous runtime.

Execution follows the caller's thread. There is no Tokio, futures, async-std or
smol dependency, and no executor is required to encode, frame or decode a message.
Blocking I/O and explicit nonblocking progress are the transport design;
`!async` describes the execution model, not a Rust auto trait or a latency guarantee.

## Implementation and integration boundaries

The current checkout provides message envelopes, method descriptors, all three
codec adapters, aligned framing, metadata, status handling and bounded compression.
`SyncChannel`, `SyncServer`, `SansIoSession`, socket transports, service macros and
schema-build integration are being integrated in their dedicated modules.
The transport and RPC sections below describe their intended composition;
they do not advertise callable APIs in the current scaffolding modules.

The Protobuf interoperability target is **100% official gRPC wire compatibility**.
Passing local codec and framing tests establishes those boundaries; it does not
by itself establish end-to-end interoperability with every external gRPC stack.
Fory and Cap'n Proto use the same envelope with explicitly negotiated payload
formats; a Protobuf-only peer cannot decode those payloads.

## Workspace architecture

| Crate directory | Package | Responsibility |
| --- | --- | --- |
| `crates/grpc-core` | `grpc-core` | Codec contract, envelopes, framing, status, metadata, compression; session and transport integration |
| `crates/grpc-codec/prost` | `grpc-codec-prost` | Protobuf payload serialization with `ProstCodec<E, D>` |
| `crates/grpc-codec/fory` | `grpc-codec-fory` | Reusable Apache Fory context with `ForyCodec<E, D = E>` |
| `crates/grpc-codec/capnp` | `grpc-codec-capnp` | Typed Cap'n Proto messages and aligned borrowed readers |
| `crates/grpc-macros` | `grpc-macros` | Code-first synchronous service generation integration |
| `crates/grpc-build` | `grpc-build` | Schema-first compiler integration |
| `crates/grpc` | `grpc` | Public facade and optional codec selection |

```text
Application / generated service
            |
      SyncChannel / SyncServer          RPC integration layer
            |
      SansIoSession + StreamSlotMap     per-stream HTTP/2 event routing
            |
         nghttp2-rs                     synchronous HTTP/2 backend
            |
         SyncTransport                  memory / TCP / Unix / TLS

Codec <-> payload <-> compression <-> aligned gRPC message frame
```

`Codec::encode` appends one payload to a caller-owned `Vec<u8>`;
`Codec::decode` consumes a payload slice. Neither method owns HTTP/2 headers,
message compression, the five-byte gRPC prefix or transport policy.
`Request<T>` and `Response<T>` keep metadata separate from the message body.
`MethodDescriptor` records a canonical `/package.Service/Method` path and cardinality.

## Build and feature selection

The workspace uses Rust Edition 2024 and the nightly toolchain in
[rust-toolchain.toml](rust-toolchain.toml). Install the native build prerequisites
required by nghttp2-rs and Zstd as well as the Rust toolchain.
Workspace packages currently have `publish = false`; consume them from Git or a checkout.

```toml
[dependencies]
grpc = { git = "https://github.com/trunkszi/grpc-rs.git", branch = "main" }
```

| Facade selection | Enabled codecs |
| --- | --- |
| Default / `protobuf` | `ProstCodec` |
| `--no-default-features` | Core contracts without a facade codec |
| `--no-default-features --features fory` | Apache Fory without Protobuf |
| `--features capnp` | Cap'n Proto plus the default Protobuf codec |
| `--all-features` | All three codec adapters |

```sh
cargo test -p grpc
cargo test -p grpc --no-default-features --features fory
cargo test -p grpc --features capnp
cargo test -p grpc --all-features
cargo tree -p grpc --no-default-features --features fory --edges normal,build
```

Use `-p grpc` when checking codec isolation. Building the entire workspace also
selects the individual codec packages and therefore does not demonstrate that a
Fory-only application excludes Prost. Compression algorithms are core dependencies,
not separate facade features. The current facade has no `tls` feature flag.
`nghttp2` and `fory` are Git dependencies tracking their respective upstream `main`
branches; `Cargo.lock` records the revisions selected for this checkout.

## Protocol Buffers

`ProstCodec<E, D>` supports distinct outbound and inbound message types.
It appends the Protobuf payload directly to the destination, without a second
Protobuf length delimiter or a gRPC header. It emits `application/grpc` and accepts
both that media type and `application/grpc+proto`.

```rust
use grpc::{Codec, ProstCodec};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut codec = ProstCodec::<String, String>::new();
    let mut bytes = Vec::with_capacity(256);
    codec.encode(&"order accepted".to_owned(), &mut bytes)?;
    assert_eq!(codec.decode(&bytes)?, "order accepted");
    bytes.clear(); // Reuse the outbound allocation for the next message.
    Ok(())
}
```

Generated Prost messages implement the same backend `Message` contract.
Unknown well-formed Protobuf fields follow Protobuf's normal decoding rules.

## Apache Fory

Select `grpc` with `default-features = false, features = ["fory"]` for a standalone
Fory application. Its content type is `application/grpc+fory`.

```rust
use grpc::{Codec, ForyCodec};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut codec = ForyCodec::<String>::new();
    let mut bytes = Vec::new();
    for value in ["quote", "order", "execution"] {
        bytes.clear();
        codec.encode(&value.to_owned(), &mut bytes)?;
        assert_eq!(codec.decode(&bytes)?, value);
    }
    Ok(())
}
```

Each codec owns and reuses a `Fory` instance and its cached type metadata.
For registered structures, configure `grpc_codec_fory::backend::Fory`, register
all types, then transfer it with `ForyCodec::with_fory`. Add the adapter crate
as a direct dependency when using its backend exports. Registration IDs, schemas
and cross-language settings must match the peer; registration freezes on first use.
Keep backend validation and resource limits enabled for untrusted messages.
Discard a codec after a custom serializer panics; error recovery via `Result`
does not imply that backend contexts recover from unwinding.

The external `foryc` compiler accepts `.fdl` schemas and can generate Rust types:

```sh
foryc --lang rust --output generated schema.fdl
```

This command assumes `schema.fdl` exists and `foryc` is installed on `PATH`.
Generating schema types is distinct from generating this framework's service
stubs. `grpc-build` is the integration point for `.proto`/`prost-build`,
`.fdl`/`foryc` and `.capnp`/`capnpc`; its compiler API is not implemented yet.
`grpc-macros` reserves the code-first `#[grpc_service]` integration;
that attribute is not currently exported.

## Cap'n Proto and alignment

Enable `capnp` for `CapnpCodec<E, D>` and `application/grpc+capnp`.
Encoding accepts a typed builder. Ordinary `Codec::decode` returns an owned,
aligned reader and copies segment storage. For borrowing, use
`CapnpCodec::read_aligned(payload)`; its result cannot outlive the payload.

`AlignedMessageBuf` guarantees actual eight-byte payload alignment:

```text
Internal allocation: [3 private padding bytes][flag: u8][length: u32 BE][payload]
Offsets:              0..3                    3         4..8           8..
Wire bytes:                                   [five-byte header][payload]
```

Only `wire_bytes()` is transmitted. Padding never changes the official wire format.
`LengthPrefixedFrame` assembles fragmented or coalesced HTTP/2 DATA into aligned
owned buffers and checks declared sizes before allocating message bodies.
Borrowed Cap'n Proto segment reads avoid an additional realignment copy;
message construction, frame assembly and decompression have independent costs.
Decompressed `Vec<u8>` output must be moved into suitably aligned storage before
calling `read_aligned`. Nested pointer validation is lazy, and callers must handle
field-access errors and retain finite traversal and nesting limits.

## Wire interoperability and diagnostics

The official [gRPC over HTTP/2 protocol](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
defines the shared transport envelope:

- Requests use `:method: POST`, canonical service paths and `te: trailers`.
- Protobuf accepts `content-type: application/grpc` or `application/grpc+proto`;
  other codecs identify themselves as `+fory` or `+capnp`.
- Each message has a one-byte compressed flag and four-byte big-endian length.
  HTTP/2 DATA boundaries need not match gRPC message boundaries.
- Responses finish with `grpc-status`; Trailers-Only responses carry immediate
  outcomes without a response DATA message. HTTP 200 alone does not imply RPC success.
- `Status` covers all 17 canonical codes. `grpc-message` uses UTF-8 percent encoding
  following RFC 3986; decoding preserves malformed diagnostics rather than discarding them.
- Application `-bin` metadata is raw bytes internally and unpadded Base64 on the wire.
  Decoding accepts padded/unpadded values and splits comma-joined binary entries.
- `parse_grpc_timeout` and `format_grpc_timeout` support `H`, `M`, `S`, `m`, `u`, `n`.
  Formatting rounds upward within eight digits; unrepresentable durations return an error.

When `grpc-status` is absent, use the canonical HTTP fallback:

| HTTP `:status` | gRPC code |
| --- | --- |
| 400 | `Internal` |
| 401 | `Unauthenticated` |
| 403 | `PermissionDenied` |
| 404 | `Unimplemented` |
| 429, 502, 503, 504 | `Unavailable` |
| Other values, including 200 | `Unknown` |

The core implements status, metadata, timeout and framing utilities now.
Emitting mandatory headers, routing Trailers-Only outcomes and validating external
peers belong to the HTTP/2 session and RPC integration layer.

## Adaptive HFT compression

`CompressionConfig::default()` selects **Zstd level 1**, a **256-byte** compression
threshold and a **16 MiB** decoded-message limit. Messages smaller than 256 bytes
bypass the compressor. A candidate result that is no smaller than the original
also bypasses compression; both paths emit `Compressed-Flag = 0`.

| Algorithm | `grpc-encoding` | Representation |
| --- | --- | --- |
| `Zstd` | `zstd` | Zstandard frames |
| `Lz4` | `lz4` | One standard LZ4 frame; legacy/skippable frames rejected |
| `Gzip` | `gzip` | RFC 1952 gzip |
| `Deflate` | `deflate` | RFC 1950 zlib-wrapped deflate |
| `Snappy` | `snappy` | Raw Snappy blocks |
| `Identity` | `identity` | Uncompressed bytes |

`negotiate_compression` selects the first local preference advertised in
`grpc-accept-encoding`, falling back to identity. Put `Gzip` in the preference list
when a peer advertising only `gzip,identity` should receive compressed messages.
Unknown peer tokens are ignored; an unsupported selected encoding is rejected.
Codec compatibility does not imply compression compatibility: external peers must
also support the selected encoding, including custom `zstd` or `lz4` tokens.

Decoded limits apply to compressed and uncompressed messages. Oversized output
returns `Status::resource_exhausted`; malformed or truncated streams are rejected.
Zstd additionally caps its decoding window at 16 MiB. These are message and decoder
bounds, not a total connection-memory budget or an allocation-free guarantee.

## Synchronous transport and RPC design

The integration design connects `SyncTransport` to `SansIoSession` over nghttp2-rs:

| Transport | Intended use and behavior |
| --- | --- |
| `MemoryPipe` | In-process deterministic byte exchange without network I/O |
| `TcpTransport` | `socket2` configuration and `TCP_NODELAY` for TCP streams |
| `UnixTransport` | Unix-domain streams for local IPC |
| `TlsTransport` | Synchronous rustls with certificate validation and ALPN `h2` |

These transport types are integration targets; they are not exported by the
current transport scaffold. TLS configuration must supply the appropriate trust
roots and server identity; negotiating HTTP/1.1 is not a gRPC fallback.

`SyncChannel` owns client-side HTTP/2 progress; `SyncServer` dispatches services.
The four method cardinalities are `Unary`, `ServerStreaming`, `ClientStreaming`
and `BidiStreaming`. Blocking `call`/`recv` and explicit `step`/`try_recv` progress
are the intended operating modes, with no runtime task spawning.
`StreamSlotMap` must retain each stream's headers, messages and terminal status,
so driving an order-placement Unary cannot discard a concurrent market-data stream.
The design uses configurable 8 MiB stream/connection flow-control windows and
returns receive credit as queued messages are consumed.
These names describe the integration contract, not fabricated callable examples.

## Verification and maintenance

Run the repository gates from the workspace root:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bash .github/scripts/ci-incremental.sh
```

The CI script additionally validates source-size limits, inline-table dependency
syntax, the resolved dependency graph and codec feature combinations.
Every source file must stay within 800 lines; dependency and lint policy is
centralized in the workspace manifest. See [AGENTS.md](AGENTS.md) for maintenance rules.
Feature compilation, local protocol tests and remote interoperability tests are
separate evidence: validate service schemas, codec configuration, compression
support, TLS identities and streaming behavior against the actual peer before deployment.
