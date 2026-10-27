//! Core operation latency; owned frame creation includes its documented allocation.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
    time::Duration,
};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use grpc_core::{
    AlignedMessageBuf, CompressionAlgorithm, GrpcClientSession, LengthPrefixedFrame, Metadata,
    SessionConfig,
    metadata::{format_grpc_timeout, parse_grpc_timeout},
    slot_map::{Headers, StreamLimits, StreamMessage, StreamSlotMap},
};

struct CountingAllocator;
thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
fn allocation() {
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
    }
}
// SAFETY: Every operation forwards its original pointer and layout to System.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocation();
        // SAFETY: The caller supplies the allocator layout contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocation();
        // SAFETY: The caller supplies the allocator layout contract.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The original allocation and layout are passed through unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocation();
        // SAFETY: The caller supplies a valid live allocation and new size.
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn zero_alloc<T>(operation: impl FnOnce() -> T) -> T {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|enabled| enabled.set(true));
    let result = operation();
    COUNTING.with(|enabled| enabled.set(false));
    assert_eq!(ALLOCATIONS.with(Cell::get), 0, "hot operation allocated");
    result
}
fn request() -> (GrpcClientSession, i32) {
    let mut client = GrpcClientSession::new(SessionConfig::default()).expect("client setup");
    let id = client
        .start_request(
            "localhost",
            "/bench.Echo/Call",
            &Metadata::new(),
            None,
            CompressionAlgorithm::Identity,
            false,
        )
        .expect("request setup");
    (client, id)
}
fn slots() -> StreamSlotMap {
    let mut slots = StreamSlotMap::new(StreamLimits::default());
    slots.open_stream(1).expect("open");
    slots
        .push_initial_metadata(1, Headers::new())
        .expect("headers");
    slots
}
fn preflight() {
    assert_eq!(
        parse_grpc_timeout("99999999n").unwrap(),
        Duration::from_nanos(99_999_999)
    );
    assert_eq!(
        format_grpc_timeout(Duration::from_nanos(100_000_001)).unwrap(),
        "100001u"
    );
    let mut metadata = Metadata::new();
    metadata.append_ascii("trace", "value").unwrap();
    let mut decoder = LengthPrefixedFrame::new(128);
    let (mut client, id) = request();
    let mut slots = slots();
    let message = StreamMessage {
        compressed: false,
        payload: vec![0; 64],
    };
    slots.push_message(1, message).unwrap();
    let message = slots.pop_message(1).unwrap().unwrap();
    zero_alloc(|| {
        black_box(parse_grpc_timeout(black_box("99999999n")).unwrap());
        assert!(metadata.get_all("trace").next().is_some());
        assert!(decoder.push(&[0, 0, 0, 0]).unwrap().is_empty());
        slots.push_message(1, message).unwrap();
        black_box(slots.pop_message(1).unwrap().unwrap());
        client.half_close(id).unwrap();
        assert!(!client.is_draining());
    });
}
fn core(c: &mut Criterion) {
    preflight();
    let payload = [42; 128];
    let frame = AlignedMessageBuf::new(&payload, false).unwrap();
    c.bench_function("framing/encode_owned_128", |b| {
        b.iter(|| black_box(AlignedMessageBuf::new(black_box(&payload), false).unwrap()))
    });
    let mut decoder = LengthPrefixedFrame::new(128);
    c.bench_function("framing/decode_owned_128", |b| {
        b.iter(|| black_box(decoder.push(black_box(frame.wire_bytes())).unwrap()))
    });
    c.bench_function("framing/fragmented_header", |b| {
        b.iter(|| {
            decoder.reset();
            black_box(decoder.push(black_box(&[0, 0, 0, 0])).unwrap())
        })
    });
    c.bench_function("timeout/parse", |b| {
        b.iter(|| black_box(parse_grpc_timeout(black_box("99999999n")).unwrap()))
    });
    c.bench_function("timeout/format", |b| {
        b.iter(|| {
            black_box(format_grpc_timeout(black_box(Duration::from_nanos(100_000_001))).unwrap())
        })
    });
    let mut metadata = Metadata::new();
    for key in [
        "trace",
        "request-id",
        "route",
        "peer",
        "token",
        "region",
        "tenant",
        "deadline",
    ] {
        metadata.append_ascii(key, "value").unwrap();
    }
    c.bench_function("metadata/lookup_8", |b| {
        b.iter(|| black_box(metadata.get_all(black_box("tenant")).next()))
    });
    let mut slots = slots();
    slots
        .push_message(
            1,
            StreamMessage {
                compressed: false,
                payload: vec![0; 64],
            },
        )
        .unwrap();
    c.bench_function("slots/push_pop_reused", |b| {
        b.iter(|| {
            let message = slots.pop_message(1).unwrap().unwrap();
            slots.push_message(1, message).unwrap();
        })
    });
    c.bench_function("slots/insert", |b| {
        b.iter_batched_ref(
            || StreamSlotMap::new(StreamLimits::default()),
            |slots| {
                slots.open_stream(black_box(1)).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    for count in [1, 64, 1024] {
        let mut map = StreamSlotMap::new(StreamLimits::default());
        for i in 0..count {
            map.open_stream(2 * i + 1).unwrap();
        }
        c.bench_function(&format!("slots/lookup_{count}"), |b| {
            b.iter(|| black_box(map.get(black_box(2 * count - 1))))
        });
    }
    c.bench_function("session/half_close", |b| {
        b.iter_batched_ref(
            request,
            |(client, id)| {
                client.half_close(black_box(*id)).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    c.bench_function("session/draining", |b| {
        b.iter_batched_ref(
            request,
            |(client, _)| {
                client.submit_goaway(0).unwrap();
                assert!(client.is_draining());
            },
            BatchSize::SmallInput,
        )
    });
}
criterion_group! { name = benches; config = Criterion::default().sample_size(20); targets = core }
criterion_main!(benches);
