//! Observe allocations across real-ring transfers after caller-thread initialization.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    io::{Read, Write},
    os::unix::net::UnixStream,
    time::Duration,
};

use grpc_core::transport::{MemoryPipe, SyncTransport, UnixTransport};

thread_local! {
    static ALLOCATIONS: Cell<(bool, usize)> = const { Cell::new((false, 0)) };
}

struct CountingAllocator;

fn record_allocation() {
    ALLOCATIONS.with(|state| {
        let (enabled, count) = state.get();
        if enabled {
            state.set((true, count + 1));
        }
    });
}

// SAFETY: Every allocation and release forwards the identical pointer/layout
// contract to System. The thread-local scalar accounting never allocates.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplies a valid allocation layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplies a valid allocation layout.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller retains System's original pointer/layout ownership.
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplies a live System allocation and valid new size.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Measured;
impl Measured {
    fn start() -> Self {
        ALLOCATIONS.with(|state| state.set((true, 0)));
        Self
    }
    fn count(&self) -> usize {
        ALLOCATIONS.with(|state| state.get().1)
    }
}
impl Drop for Measured {
    fn drop(&mut self) {
        ALLOCATIONS.with(|state| state.set((false, 0)));
    }
}

#[test]
fn integration_scenario_dma_reuses_storage_for_large_and_nonblocking_transfers()
-> anyhow::Result<()> {
    let (socket, mut peer) = UnixStream::pair()?;
    let mut transport = UnixTransport::from_std(socket)?;
    transport.set_read_timeout(Some(Duration::from_secs(1)))?;
    transport.set_write_timeout(Some(Duration::from_secs(1)))?;
    transport.write_all(b"a")?;
    peer.read_exact(&mut [0])?;
    peer.write_all(b"a")?;
    transport.read_exact(&mut [0])?;
    let source = vec![0x5a; 2 * 1024 * 1024 + 17];
    let mut destination = vec![0; source.len()];
    let length = source.len();
    let worker = std::thread::spawn(move || -> std::io::Result<()> {
        let mut bytes = vec![0; length];
        for _ in 0..4 {
            peer.read_exact(&mut bytes)?;
            assert!(bytes.iter().all(|byte| *byte == 0x5a));
            peer.write_all(&bytes)?;
        }
        Ok(())
    });
    let measured = Measured::start();
    for nonblocking in [false, true] {
        transport.set_nonblocking(nonblocking)?;
        for _ in 0..2 {
            let mut sent = 0;
            while sent < length {
                match transport.write(&source[sent..]) {
                    Ok(count) => {
                        assert!(count > 0 && count <= 1024 * 1024);
                        sent += count;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::hint::spin_loop()
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            let mut received = 0;
            while received < length {
                match transport.read(&mut destination[received..]) {
                    Ok(count) => {
                        assert!(count > 0 && count <= 1024 * 1024);
                        received += count;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::hint::spin_loop()
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            assert_eq!(source, destination);
        }
    }
    let allocations = measured.count();
    drop(measured);
    worker.join().expect("socket peer completes")?;
    assert_eq!(
        allocations, 0,
        "fixed data buffers and unchanged timers must be reused"
    );
    Ok(())
}

#[test]
fn integration_scenario_memory_ring_reuses_exact_capacity_and_notification_storage()
-> anyhow::Result<()> {
    let (mut first, mut second) = MemoryPipe::with_capacity(3)?;
    first.write_all(b"abc")?;
    second.read_exact(&mut [0; 3])?;
    let measured = Measured::start();
    for _ in 0..64 {
        assert_eq!(first.write(b"abcdef")?, 3);
        let mut bytes = [0; 3];
        second.read_exact(&mut bytes)?;
        assert_eq!(&bytes, b"abc");
    }
    let allocations = measured.count();
    drop(measured);
    assert_eq!(allocations, 0);
    Ok(())
}

#[test]
fn integration_lifecycle_spsc_wraparound_survives_thread_handoff() -> anyhow::Result<()> {
    for capacity in [1, 3, 7] {
        let (mut producer, mut consumer) = MemoryPipe::with_capacity(capacity)?;
        producer.set_write_timeout(Some(Duration::from_secs(5)))?;
        consumer.set_read_timeout(Some(Duration::from_secs(5)))?;
        let worker = std::thread::spawn(move || -> std::io::Result<MemoryPipe> {
            for index in 0..2048 {
                producer.write_all(&[(index % 251) as u8])?;
            }
            Ok(producer)
        });
        for index in 0..2048 {
            let mut byte = [0];
            consumer.read_exact(&mut byte)?;
            assert_eq!(byte, [(index % 251) as u8]);
        }
        let mut producer = worker.join().expect("producer completes")?;
        producer.write_all(b"z")?;
        let mut byte = [0];
        consumer.read_exact(&mut byte)?;
        assert_eq!(&byte, b"z");
        drop(producer);
        assert_eq!(consumer.read(&mut byte)?, 0);
    }
    Ok(())
}
