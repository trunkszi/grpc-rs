//! Caller-thread latency samples, including framing and RPC completion.
use std::{
    hint::black_box,
    io, thread,
    time::{Duration, Instant},
};

use grpc::{
    CallOptions, ChannelConfig, Codec, Response, Status, StreamError, SyncChannel, SyncServer,
    core::{
        compression::{CompressionAlgorithm, CompressionConfig},
        framing::{AlignedMessageBuf, LengthPrefixedFrame},
        transport::{MemoryPipe, SyncTransport, TcpTransport, TcpTransportListener},
    },
};

type BenchResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn report(label: &str, mut samples: Vec<u128>, elapsed: Duration) {
    samples.sort_unstable();
    let percentile = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1];
    println!(
        "{label}: p50={} ns p99={} ns throughput={:.0} ops/s samples={}",
        percentile(50),
        percentile(99),
        samples.len() as f64 / elapsed.as_secs_f64(),
        samples.len(),
    );
}

fn measure(
    label: &str,
    iterations: usize,
    mut operation: impl FnMut() -> BenchResult,
) -> BenchResult {
    // Warm the reusable codec/channel independently of measured samples.
    operation()?;
    let mut samples = Vec::with_capacity(iterations);
    let total = Instant::now();
    for _ in 0..iterations {
        let start = Instant::now();
        operation()?;
        samples.push(start.elapsed().as_nanos());
    }
    report(label, samples, total.elapsed());
    Ok(())
}

fn framed(payload: &[u8]) -> BenchResult<AlignedMessageBuf> {
    let outgoing = AlignedMessageBuf::new(payload, false)?;
    let mut parser = LengthPrefixedFrame::new(1024 * 1024);
    let mut messages = parser.push(outgoing.wire_bytes())?;
    assert_eq!(messages.len(), 1);
    let received = messages.remove(0);
    assert_eq!(received.payload().as_ptr() as usize % 8, 0);
    Ok(received)
}

fn codec_benchmarks(iterations: usize) -> BenchResult {
    #[cfg(any(feature = "protobuf", feature = "fory"))]
    let message = "place-order: venue=XNAS symbol=EXAMPLE side=BUY qty=100".to_owned();
    #[cfg(feature = "protobuf")]
    {
        let mut codec = grpc::ProstCodec::<String, String>::new();
        let mut encoded = Vec::with_capacity(256);
        measure("Prost codec+frame+aligned owned decode", iterations, || {
            encoded.clear();
            codec.encode(black_box(&message), &mut encoded)?;
            let incoming = framed(black_box(&encoded))?;
            assert_eq!(codec.decode(black_box(incoming.payload()))?, message);
            Ok(())
        })?;
    }
    #[cfg(feature = "fory")]
    {
        let mut codec = grpc::ForyCodec::<String>::new();
        let mut encoded = Vec::with_capacity(256);
        measure("Fory codec+frame+aligned owned decode", iterations, || {
            encoded.clear();
            codec.encode(black_box(&message), &mut encoded)?;
            let incoming = framed(black_box(&encoded))?;
            assert_eq!(codec.decode(black_box(incoming.payload()))?, message);
            Ok(())
        })?;
    }
    #[cfg(feature = "capnp")]
    {
        use grpc_codec_capnp::backend::{data, message::TypedBuilder};
        let payload = b"place-order: venue=XNAS symbol=EXAMPLE side=BUY qty=100";
        let mut message = TypedBuilder::<data::Owned>::new_default();
        message.set_root(payload.as_slice())?;
        let mut codec = grpc::CapnpCodec::<data::Owned, data::Owned>::new();
        let mut encoded = Vec::with_capacity(256);
        measure(
            "Capnp codec+frame+aligned borrowed decode",
            iterations,
            || {
                encoded.clear();
                codec.encode(black_box(&message), &mut encoded)?;
                let incoming = framed(black_box(&encoded))?;
                let decoded = codec.read_aligned(black_box(incoming.payload()))?;
                let bytes = decoded.get()?;
                assert_eq!(bytes, payload);
                // The root data bytes borrow the aligned transport allocation.
                let start = incoming.payload().as_ptr() as usize;
                let end = start + incoming.payload().len();
                assert!((start..end).contains(&(bytes.as_ptr() as usize)));
                black_box(bytes);
                Ok(())
            },
        )?;
    }
    Ok(())
}

fn compression_benchmarks(iterations: usize) -> BenchResult {
    for size in [255, 256, 1024] {
        let input = vec![b'A'; size];
        for algorithm in [
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Identity,
        ] {
            let config = CompressionConfig {
                algorithm,
                ..CompressionConfig::default()
            };
            let label = format!("compression {algorithm:?} {size}B encode+decode");
            measure(&label, iterations, || {
                let compressed = config.compress(black_box(&input))?;
                assert_eq!(
                    compressed.compressed,
                    size >= 256 && algorithm != CompressionAlgorithm::Identity
                );
                let decoded =
                    config.decompress(black_box(&compressed.data), compressed.compressed)?;
                assert_eq!(decoded, input);
                black_box(decoded);
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct OrderCodec;
impl Codec for OrderCodec {
    type EncodeItem = u64;
    type DecodeItem = u64;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = "application/grpc+bench-order";
    fn encode(&mut self, value: &u64, destination: &mut Vec<u8>) -> io::Result<()> {
        destination.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }
    fn decode(&mut self, payload: &[u8]) -> io::Result<u64> {
        let bytes = payload
            .try_into()
            .map_err(|_| io::Error::other("order requires eight bytes"))?;
        Ok(u64::from_be_bytes(bytes))
    }
}

fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking benchmark call remained pending"),
    }
}

fn rpc_benchmarks<T: SyncTransport + Send + 'static>(
    label: &str,
    client: T,
    server: T,
    iterations: usize,
) -> BenchResult {
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(server)?;
        server.register_unary("/bench.Order/Unary", OrderCodec, |request| {
            Ok(Response::new(*request.get_ref()))
        })?;
        server.register_bidi_streaming(
            "/bench.Order/Bidi",
            OrderCodec,
            |mut incoming, outgoing| {
                while let Some(order) = incoming.recv_next().map_err(stream_status)? {
                    outgoing.send(&order).map_err(stream_status)?;
                }
                Ok(())
            },
        )?;
        server.serve_connection()
    });
    let result = (|| -> BenchResult {
        let mut channel = SyncChannel::with_config(
            client,
            ChannelConfig {
                default_timeout: Some(Duration::from_secs(5)),
                ..ChannelConfig::default()
            },
        )?;
        measure(&format!("{label} unary completed RPC"), iterations, || {
            let response =
                channel.call_unary("/bench.Order/Unary", black_box(&42), &mut OrderCodec)?;
            assert_eq!(response, 42);
            black_box(response);
            Ok(())
        })?;
        measure(
            &format!("{label} bidi completed RPC (2 interleaved messages)"),
            iterations,
            || {
                let mut call = channel.bidi_streaming(
                    "/bench.Order/Bidi",
                    OrderCodec,
                    CallOptions::default(),
                )?;
                for order in [42, 43] {
                    call.send(black_box(&order))?;
                    assert_eq!(call.recv_next()?, Some(order));
                }
                assert!(call.finish()?.is_empty());
                Ok(())
            },
        )?;
        Ok(())
    })();
    // The channel is dropped before joining, delivering EOF even on an RPC error.
    let served = worker
        .join()
        .map_err(|_| io::Error::other("benchmark server panicked"))?;
    result?;
    served?;
    Ok(())
}

fn main() -> BenchResult {
    let iterations =
        std::env::var("GRPC_BENCH_ITERATIONS").map_or(Ok(32usize), |text| text.parse())?;
    if !(1..=1_000_000).contains(&iterations) {
        return Err("GRPC_BENCH_ITERATIONS must be between 1 and 1000000".into());
    }
    println!(
        "Elapsed wall-clock samples; codec/compression include validation; RPCs reuse \
         connections, exclude setup/teardown; no CPU pinning."
    );
    codec_benchmarks(iterations)?;
    compression_benchmarks(iterations)?;
    let (client, server) = MemoryPipe::pair();
    rpc_benchmarks("MemoryPipe", client, server, iterations)?;
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let client = TcpTransport::connect(listener.local_addr()?)?;
    let (server, _) = listener.accept()?;
    assert!(client.get_ref().nodelay()? && server.get_ref().nodelay()?);
    rpc_benchmarks("TCP_NODELAY", client, server, iterations)
}
