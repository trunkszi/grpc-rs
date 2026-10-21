# grpc-rs

A strictly synchronous, single-threaded gRPC framework for HFT services and
Run-to-Completion applications, built on [nghttp2-rs](https://github.com/trunkszi/nghttp2-rs).
Message serialization is independent of the HTTP/2 engine: use Protocol Buffers,
Apache Fory, Cap'n Proto, or a custom `Codec` without an asynchronous runtime.

Execution follows the caller's thread. There is no Tokio, futures, async-std or
smol dependency, and no executor is required to encode, frame or decode a message.
Blocking I/O and explicit nonblocking progress are the transport design;
`!async` describes the execution model, not a Rust auto trait or a latency guarantee.

## Runtime and interoperability boundaries

The checkout provides `SyncChannel`, `SyncServer`, caller-thread streaming handles,
HTTP/2 sessions, memory/TCP/Unix/TLS transports, code-first service macros and
schema-build integration. Codec adapters, aligned framing, metadata, statuses and
bounded compression are shared by client and server paths.

The Protobuf interoperability target is **100% official gRPC wire compatibility**.
Passing local codec and framing tests establishes those boundaries; it does not
by itself establish end-to-end interoperability with every external gRPC stack.
Fory and Cap'n Proto use the same envelope with codec-specific content types;
a Protobuf-only peer cannot decode those payloads.

## Workspace architecture

| Crate directory | Package | Responsibility |
| --- | --- | --- |
| `crates/grpc-core` | `grpc-core` | Codec contract, envelopes, framing, status, metadata, compression; HTTP/2 sessions and synchronous transports |
| `crates/grpc-codec/prost` | `grpc-codec-prost` | Protobuf payload serialization with `ProstCodec<E, D>` |
| `crates/grpc-codec/fory` | `grpc-codec-fory` | Reusable Apache Fory context with `ForyCodec<E, D = E>` |
| `crates/grpc-codec/capnp` | `grpc-codec-capnp` | Typed Cap'n Proto messages and aligned borrowed readers |
| `crates/grpc-macros` | `grpc-macros` | Code-first synchronous service traits, clients and handlers |
| `crates/grpc-build` | `grpc-build` | Schema compiler invocation and service-module generation |
| `crates/grpc` | `grpc` | Public facade and optional codec selection |

```text
Application / generated service
            |
      SyncChannel / SyncServer          RPC integration layer
            |
      GrpcClientSession / GrpcServerSession     per-stream HTTP/2 state and event routing
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
Custom serializers must not recursively invoke backend serialization or
deserialization while operating on the supplied serialization context.
Discard a codec after a custom serializer panics; error recovery via `Result`
does not imply that backend contexts recover from unwinding.

The external `foryc` compiler accepts `.fdl` schemas and can generate Rust types:

```sh
foryc --lang rust --output generated schema.fdl
```

This command assumes `schema.fdl` exists and `foryc` is installed on `PATH`.
Generating schema types is distinct from generating this framework's service
stubs. `grpc-build::SchemaCompiler` invokes `protoc`, `foryc` or `capnp` and emits
Rust service definitions. Configure `protoc_path`, `foryc_path`, `capnp_path` and
`include` for the installed tools and schema imports. Compiler-generated message
types and generated service modules are separate outputs; retain both as needed.

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

`GrpcClientSession` and `GrpcServerSession` emit the required HTTP/2 headers,
route messages and terminal outcomes by stream, and expose statuses and metadata
to `SyncChannel` and `SyncServer`. External interoperability still depends on the
peer's payload codec, compression support and service schema.

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

## Synchronous transports and RPCs

`SyncChannel<T>` and `SyncServer<T>` own HTTP/2 session progress over a
`grpc::core::transport::SyncTransport`. Construct and drive each endpoint on its
own caller thread; the framework creates no worker thread or executor.

| Transport | API and behavior |
| --- | --- |
| `MemoryPipe` | `pair()` creates bounded, connected in-process endpoints. |
| `TcpTransport` | `connect` / `from_std` enable `TCP_NODELAY`; `TcpTransportListener` accepts connections. |
| `UnixTransport` | Unix-domain streams and `UnixTransportListener` provide local IPC on Unix. |
| `TlsTransport` | `grpc::core::tls` wraps a synchronous transport with rustls and requires negotiated ALPN `h2`. |

`TlsClientConfig::new` takes trusted roots; `TlsServerConfig::new` takes a DER
certificate chain and private key. These constructors use TLS 1.3. Supply the
expected server name to `TlsTransport::client`; `TlsTransport::server` takes the
server identity configuration. Constructors perform no I/O: call `handshake`
or let the first nonempty read/write progress it. Configure socket deadlines for
blocking handshakes. HTTP/1.1 negotiation is an error, not a gRPC fallback.

`SyncChannel::new` uses `ChannelConfig::default()`. `with_config` controls authority,
scheme, RPC timeout, default compression and session limits. Use scheme `https`
with TLS. Outgoing channel compression defaults to `Identity`; select Zstd through
`ChannelConfig::with_compression` or per-call options. `CallOptions` supplies
per-call metadata, timeout and compression. Unary returns the decoded response,
initial metadata and trailing metadata. Stream handles preserve status errors and
provide blocking operations plus explicit `try_*` progress returning `WouldBlock`.

| RPC shape | Client entry point | Server registration |
| --- | --- | --- |
| Unary | `channel.unary` | `register_unary` |
| Server streaming | `channel.server_streaming`, then `recv_next` / `finish` | `register_server_streaming` with a `ResponseSink` |
| Client streaming | `channel.client_streaming`, then `send` / `finish` | `register_client_streaming` with a `RequestStream` |
| Bidirectional streaming | `channel.bidi_streaming`, then interleaved `send` / `recv_next` and `half_close` | `register_bidi_streaming` with request stream and response sink |

`serve_connection` dispatches a registered server connection on its caller thread.
`shutdown_with_goaway` starts graceful draining. The advertised initial stream receive
window defaults to 1 MiB; message and per-stream byte limits default to 16 MiB. These limits are
configurable through `SessionConfig`. Consume queued messages to return receive
credit; connection progress retains other streams' pending messages and statuses.

A complete unary exchange over a memory pipe uses a thread for each endpoint:

```rust
use std::thread;
use grpc::{CallOptions, ProstCodec, Response, SyncChannel, SyncServer};
use grpc::core::transport::MemoryPipe;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client_transport, server_transport) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<(), grpc::Status> {
        let mut server = SyncServer::new(server_transport)?;
        server.register_unary(
            "/hft.OrderService/PlaceOrder",
            ProstCodec::<String, String>::new(),
            |request| Ok(Response::new(format!("accepted: {}", request.get_ref()))),
        )?;
        server.serve_connection()
    });
    let result = (|| {
        let mut channel = SyncChannel::new(client_transport)?;
        channel.unary(
            "/hft.OrderService/PlaceOrder",
            &"buy 42".to_owned(),
            &mut ProstCodec::<String, String>::new(),
            CallOptions::default(),
        )
    })();
    let served = worker.join().map_err(|_| "server worker panicked")?;
    let (message, _initial, _trailers) = result?;
    assert_eq!(message, "accepted: buy 42");
    served?;
    Ok(())
}
```

## Generated service clients and handlers

`#[grpc::macros::grpc_service]` exports a handler trait, a client wrapper, a server
wrapper, a service-specific channel adapter trait and method descriptors:

```rust
#[grpc::macros::grpc_service(package = "hft")]
pub trait OrderService {
    fn place_order(request: String) -> String;
    #[server_streaming]
    fn subscribe_depth(request: String) -> String;
    #[client_streaming]
    fn upload_fills(request: String) -> String;
    #[bidi_streaming]
    fn order_stream(request: String) -> String;
}

fn main() {
    assert_eq!(ORDER_SERVICE_PLACE_ORDER.path, "/hft.OrderService/PlaceOrder");
}
```

The expansion uses `grpc_core` paths; applications using the attribute must also
include `grpc-core` as a direct dependency from the same checkout or Git revision.
Implement the expanded `OrderService` handler trait with `Request` / `Response`
envelopes. Generated streaming methods batch their request or response messages
in `Vec`; the direct channel/server streaming interfaces above support incremental
send/receive instead.

Construct `OrderServiceClient::new(adapter, codec)` and
`OrderServiceServer::new(handler, codec)`. The adapter implements the generated
`OrderServiceChannel<C>` trait and forwards each method descriptor, request metadata
and compression selection to `SyncChannel`. Register generated server methods with
the matching `SyncServer::register_*` routes. The macro does not automatically
register network routes or implement a service-specific channel adapter.

Both wrappers provide `.with_codec::<NewCodec>()` and
`.with_compression(CompressionAlgorithm::Zstd)`. Codec replacement constructs
`NewCodec::default()`; configure a registered Fory context before passing it to
`new` when its type registry is required. Client and server must agree on their
message schemas and selected codec. Protobuf, Fory and Cap'n Proto all pass through
the same framing, compression and HTTP/2 layers.

## Examples, E2E tests and benchmarks

Run the order-service example, the client/server parity test target, and the
standalone benchmark from the workspace root:

```sh
cargo run -p grpc --all-features --example hft_order_service
cargo test -p grpc --all-features --test hft_e2e_parity
cargo bench -p grpc --all-features --bench hft_rpc_bench
```

The example uses loopback TCP and generated service adapters. The E2E target covers
RPC results across method shapes, codecs, compression and transports. Local
client/server E2E tests exercise this implementation on both endpoints; they do
not establish interoperability with an independent gRPC implementation.

Benchmarks report codec/framing, compression and complete RPC exchange costs.
RPC samples reuse established connections and exclude connection setup/teardown.
`GRPC_BENCH_ITERATIONS` selects 1–1,000,000 samples per workload (default 32);
results include p50/p99 latency in nanoseconds and throughput in operations/second.
Interpret results with the workload, payload size, transport and build profile; the framework's synchronous design does not imply
an allocation-free path or a fixed latency bound.

## Verification and maintenance

Run the repository gates from the workspace root:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo test -p grpc --no-default-features --features fory
bash .github/scripts/ci-incremental.sh
```

The CI script additionally validates source-size limits, inline-table dependency
syntax, the resolved dependency graph and codec feature combinations.
Every source file must stay within 800 lines; dependency and lint policy is
centralized in the workspace manifest. See [AGENTS.md](AGENTS.md) for maintenance rules.
Feature compilation, local protocol tests and remote interoperability tests are
separate evidence: validate service schemas, codec configuration, compression
support, TLS identities and streaming behavior against the actual peer before deployment.
