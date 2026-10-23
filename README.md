# grpc-rs

A strictly synchronous, single-threaded gRPC framework for low-latency services and
Run-to-Completion applications, built on pure Rust HTTP/2 and HPACK components from [nexora-proto](https://github.com/trunkszi/nexora-rs).
Message serialization is independent of the HTTP/2 engine: use Protocol Buffers,
Apache Fory, Cap'n Proto, or a custom `Codec` without an asynchronous runtime.

Execution follows the caller's thread without a background executor. All TCP and
Unix socket operations use a calling-thread [liburing-rs](https://github.com/trunkszi/liburing-rs)
`Proactor` and Linux io_uring SQEs/CQEs, including nonblocking data operations.

## Why grpc-rs?

An RPC's latency includes more than the handler: messages pass through encoding,
HTTP/2 flow control, transport I/O, and final status delivery. In a low-latency
service or client, it helps to know which part of that path owns the next step.
A synchronous application should not need a second execution model just to
expose a method or consume a stream of replies.

[Tonic's default transport](https://docs.rs/tonic/latest/tonic/) builds on Hyper,
Tower, and Tokio. Its middleware ecosystem is useful for composing general
services, but integrating layers of service futures,
[boxed service adapters](https://docs.rs/tower/latest/tower/util/struct.BoxService.html),
and executor tasks adds boundaries to a caller-thread design. Queueing and
multithreaded work stealing can make tail latency harder to attribute to the
RPC itself. During debugging, following a request through nested middleware and
poll/resume paths can also obscure whether it is waiting for a message, stream
credit, or a handler result.

`grpc-rs` keeps that path synchronous. `SyncChannel` and `SyncServer` expose
ordinary calls and caller-thread streaming handles over pure Rust HTTP/2 and
HPACK engines. TCP and Unix transports use a calling-thread
`liburing::Proactor`; no background executor drives the connection on the
application's behalf. Service traits describe RPC methods directly, without
requiring an associated future or an asynchronous middleware stack.

The payload format is a separate decision. Protocol Buffers, Apache Fory, and
Cap'n Proto share the same framing, metadata, compression, and status machinery
through the `Codec` contract. A service can choose the representation its peers
understand without rebuilding its transport or adopting another scheduler.
Protobuf is the interoperability path for conventional gRPC peers; the other
codecs require peers that understand their payloads.

This layout makes a stalled or malformed RPC inspectable along its actual
call path: encode, frame, transfer, decode, and deliver a status. It also gives
AI-assisted service generation concrete method signatures and ownership rules
instead of nested future types and lifetimes across suspension points.
Engineers can review streaming cardinality, message limits, and terminal status
handling in the same synchronous model used by the application.

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
         nexora-proto                     pure Rust HTTP/2 frame and HPACK codecs
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
required by Zstd as well as the Rust toolchain. liburing-rs uses
the pure Rust io-uring backend and requires neither C liburing nor libclang.
Socket I/O requires Linux with usable io_uring. Ring setup errors are
returned to the caller without a blocking-syscall fallback.
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
`liburing`, `nexora-proto`, and `fory` are Git dependencies tracking their respective upstream `main`
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
The default `grpc-build` features generate Prost and Cap'n Proto Rust messages;
use `default-features = false` for a Fory-only build dependency. `PROTOC` and
`FORYC` select compiler executables, with explicit builder paths taking priority.

The checked-in `proto/order.{proto,fdl,capnp}` schemas define integer-lot,
integer-tick orders, acknowledgements, depth and fill messages. The facade's
`build.rs` compiles only schemas selected by codec features into `OUT_DIR`.
Cap'n Proto's `$clientStreaming` and `$serverStreaming` method annotations
select gRPC cardinality; its native capability RPC bindings are not used as the
gRPC transport.

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

The pure Rust streaming engine shares nexora-proto frame and HPACK codecs;
grpc-core owns incremental body delivery, connection/stream receive credit,
continuation ordering and graceful stream closure.

`GrpcClientSession` and `GrpcServerSession` emit the required HTTP/2 headers,
route messages and terminal outcomes by stream, and expose statuses and metadata
to `SyncChannel` and `SyncServer`. External interoperability still depends on the
peer's payload codec, compression support and service schema.

## Adaptive message compression

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
or let the first nonempty read/write progress it. Use
`handshake_until(Some(deadline))` to bound the complete negotiation by a
`std::time::Instant`, including partial reads and writes. The method restores
both original transport timeouts on success or error; `None` uses the existing
per-operation settings. Nonblocking callers retain progress across `WouldBlock`
and pass the same absolute deadline when retrying. Every `SyncTransport`
implementation must provide `read_timeout` and `write_timeout` queries;
transport wrappers delegate them to their underlying stream.
HTTP/1.1 negotiation is an error, not a gRPC fallback.

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
            "/order.OrderService/PlaceOrder",
            ProstCodec::<String, String>::new(),
            |request| Ok(Response::new(format!("accepted: {}", request.get_ref()))),
        )?;
        server.serve_connection()
    });
    let result = (|| {
        let mut channel = SyncChannel::new(client_transport)?;
        channel.unary(
            "/order.OrderService/PlaceOrder",
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

TCP and Unix reads/writes submit `net::prep_recv` / `net::prep_send` to a
lazily initialized thread-local `liburing::Proactor`. Read/write socket timeouts
arm `ops::Timeout`; completion cancels and drains the remaining operation
before returning. EOF, partial transfers, `Interrupted`, and `TimedOut` retain
the synchronous `Read`/`Write` contract. Nonblocking data operations submit SQEs
with `MSG_DONTWAIT` and return `WouldBlock` when no data or room is available.
Connect and accept also use the ring. Nonblocking accept uses `IORING_ACCEPT_DONTWAIT`, with a minimal cancellable
kernel timeout on older kernels.
Address resolution and socket construction/configuration remain cold setup calls.

`TcpTransport` and `UnixTransport` remain `Send`: accept on one thread and move
the transport into a server thread, or move it again after completed I/O. The
`!Send` ring belongs to the thread executing each operation. `get_ref` and
`into_inner` continue to expose standard streams. A fixed 1 MiB boxed DMA buffer
is reused per thread; larger reads/writes make partial progress without growing
it. Normal data calls perform no descriptor duplication or new data-buffer
allocation. Timeout operands are cached for repeated durations and rebuilt only
when the requested duration changes. Failed ring shutdown preserves all operands
and attempts descriptor retention on that exceptional path.

`MemoryPipe` uses two atomic SPSC byte rings with exactly the requested capacity,
including capacities of one or three bytes. A Unix socket pair signals data,
new room, and peer closure. Waiting and notification I/O run on the same thread's
Proactor with kernel-backed deadlines. Published bytes drain before EOF; writes
after peer loss return BrokenPipe. Full notification sockets already contain a
wakeup, so notification saturation cannot expand the byte budget.

## Generated service clients and handlers

`#[grpc::macros::grpc_service]` exports a handler trait, a client wrapper, a server
wrapper, a service-specific channel adapter trait and method descriptors:

```rust
#[grpc::macros::grpc_service(package = "order")]
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
    assert_eq!(ORDER_SERVICE_PLACE_ORDER.path, "/order.OrderService/PlaceOrder");
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
cargo run -p grpc --all-features --example order_service
cargo test -p grpc --all-features --test e2e_parity
cargo bench -p grpc --all-features --bench rpc_bench
```

The example uses loopback TCP, IDL-generated structured messages and generated
service adapters. All four RPC shapes run with each codec and Zstd. The E2E target covers
RPC results across method shapes, codecs, compression and transports. Local
client/server E2E tests exercise this implementation on both endpoints; they do
not establish interoperability with an independent gRPC implementation.

The benchmark measures batches of all four RPC shapes using the same generated
messages as the example. Samples reuse established TCP connections, exclude
connection setup/teardown and run after one warmup batch per codec.
`GRPC_BENCH_ITERATIONS` selects 1-1,000,000 batches per codec (default 32);
results include p50/p99 batch latency in nanoseconds and throughput in batches/second.
Interpret results with the workload, payload size, transport and build profile; the framework's synchronous design does not imply
an allocation-free path or a fixed latency bound.

## Verification and maintenance

Run the repository gates from the workspace root:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo test -p grpc --no-default-features --features fory --locked
```

GitHub-hosted CI executes the Cargo checks directly for the default and complete
codec graphs. Review source-size limits, inline-table dependency syntax, and the
resolved dependency graph when changing the workspace.
Every source file must stay within 800 lines; dependency and lint policy is
centralized in the workspace manifest. See [AGENTS.md](AGENTS.md) for maintenance rules.
Feature compilation, local protocol tests and remote interoperability tests are
separate evidence: validate service schemas, codec configuration, compression
support, TLS identities and streaming behavior against the actual peer before deployment.
