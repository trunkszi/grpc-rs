//! Exact-capacity SPSC byte lanes with completion-driven peer notification.

use std::{
    cell::UnsafeCell,
    io::{self, Read, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use super::{SyncTransport, proactor};

#[repr(align(64))]
struct Cursor(AtomicUsize);

struct MemoryLane {
    bytes: Box<[UnsafeCell<u8>]>,
    head: Cursor,
    tail: Cursor,
    reader_alive: AtomicBool,
    writer_alive: AtomicBool,
    error: AtomicI32,
    #[cfg(test)]
    waiting: Arc<AtomicBool>,
}

// SAFETY: Each lane has exactly one producer and one consumer, accessed through
// separate endpoints requiring &mut self. Release publication of tail/head and
// acquire observation transfer disjoint byte ranges between those two owners.
// No byte references escape; neither side accesses the other's in-flight range.
unsafe impl Sync for MemoryLane {}

impl MemoryLane {
    fn new(capacity: usize) -> io::Result<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        bytes.resize_with(capacity, || UnsafeCell::new(0));
        Ok(Self {
            bytes: bytes.into_boxed_slice(),
            head: Cursor(AtomicUsize::new(0)),
            tail: Cursor(AtomicUsize::new(0)),
            reader_alive: AtomicBool::new(true),
            writer_alive: AtomicBool::new(true),
            error: AtomicI32::new(0),
            #[cfg(test)]
            waiting: Arc::new(AtomicBool::new(false)),
        })
    }

    fn distance(&self, head: usize, tail: usize) -> usize {
        if tail >= head {
            tail - head
        } else {
            self.bytes.len() * 2 - head + tail
        }
    }

    fn advance(&self, cursor: usize, count: usize) -> usize {
        let until_wrap = self.bytes.len() * 2 - cursor;
        if count >= until_wrap {
            count - until_wrap
        } else {
            cursor + count
        }
    }

    fn read(&self, destination: &mut [u8]) -> usize {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);
        let count = destination.len().min(self.distance(head, tail));
        if count == 0 {
            return 0;
        }
        let start = head % self.bytes.len();
        let first = count.min(self.bytes.len() - start);
        // SAFETY: Acquired tail publishes these initialized bytes exclusively to
        // this consumer. The producer cannot reuse them before the head release.
        // Both wrapped ranges lie in the allocation and destination is external.
        unsafe {
            std::ptr::copy_nonoverlapping(self.bytes[start].get(), destination.as_mut_ptr(), first);
            std::ptr::copy_nonoverlapping(
                self.bytes[0].get(),
                destination.as_mut_ptr().add(first),
                count - first,
            );
        }
        self.head
            .0
            .store(self.advance(head, count), Ordering::Release);
        count
    }

    fn write(&self, source: &[u8]) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        let count = source
            .len()
            .min(self.bytes.len() - self.distance(head, tail));
        if count == 0 {
            return 0;
        }
        let start = tail % self.bytes.len();
        let first = count.min(self.bytes.len() - start);
        // SAFETY: Acquired head transfers these free bytes exclusively to this
        // producer. No consumer can access them before the tail release; both
        // wrapped ranges are in the allocation and source is external.
        unsafe {
            std::ptr::copy_nonoverlapping(source.as_ptr(), self.bytes[start].get(), first);
            std::ptr::copy_nonoverlapping(
                source.as_ptr().add(first),
                self.bytes[0].get(),
                count - first,
            );
        }
        self.tail
            .0
            .store(self.advance(tail, count), Ordering::Release);
        count
    }

    fn check_error(&self) -> io::Result<()> {
        match self.error.load(Ordering::Acquire) {
            0 => Ok(()),
            errno => Err(io::Error::from_raw_os_error(errno)),
        }
    }
}

/// Bounded full-duplex SPSC byte stream with exact per-direction capacity.
/// Reads drain published bytes before reporting EOF after the peer is dropped.
/// Endpoints may move between threads after each completed operation.
pub struct MemoryPipe {
    incoming: Arc<MemoryLane>,
    outgoing: Arc<MemoryLane>,
    wake: UnixStream,
    nonblocking: bool,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl MemoryPipe {
    /// Creates a pair with 64 KiB of preallocated capacity in each direction.
    ///
    /// # Panics
    /// Panics if buffers or notification sockets cannot be created.
    pub fn pair() -> (Self, Self) {
        Self::with_capacity(64 * 1024).expect("cannot create default memory pipe")
    }

    /// Creates connected endpoints with an exact byte capacity in each direction.
    ///
    /// # Errors
    /// Returns invalid capacity, allocation, or notification socket errors.
    pub fn with_capacity(capacity: usize) -> io::Result<(Self, Self)> {
        if capacity == 0 || capacity > isize::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory capacity must be positive and fit an allocation",
            ));
        }
        let incoming = Arc::new(MemoryLane::new(capacity)?);
        let outgoing = Arc::new(MemoryLane::new(capacity)?);
        let (first, second) = UnixStream::pair()?;
        let endpoint = |incoming, outgoing, wake| Self {
            incoming,
            outgoing,
            wake,
            nonblocking: false,
            read_timeout: None,
            write_timeout: None,
        };
        Ok((
            endpoint(Arc::clone(&incoming), Arc::clone(&outgoing), first),
            endpoint(outgoing, incoming, second),
        ))
    }

    fn validate_timeout(timeout: Option<Duration>) -> io::Result<()> {
        if timeout.is_some_and(|duration| {
            duration.is_zero() || Instant::now().checked_add(duration).is_none()
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timeout must be nonzero and fit the monotonic clock",
            ));
        }
        Ok(())
    }

    fn notify(&self) {
        loop {
            match proactor::write(&self.wake, &[1], None, true) {
                Ok(1) => return,
                // A full socket already carries a wakeup for the peer.
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                    ) && !self.outgoing.reader_alive.load(Ordering::Acquire) =>
                {
                    return;
                }
                result => {
                    // Bytes already committed cannot be retracted or reported as
                    // unaccepted. Publish a terminal error for the next operation
                    // and wake a blocked peer through notification-channel EOF.
                    let errno = match result {
                        Err(error) => error.raw_os_error().unwrap_or(libc::EIO),
                        Ok(_) => libc::EIO,
                    };
                    self.incoming.error.store(errno, Ordering::Release);
                    self.outgoing.error.store(errno, Ordering::Release);
                    if let Err(error) = self.wake.shutdown(Shutdown::Both) {
                        self.incoming
                            .error
                            .store(error.raw_os_error().unwrap_or(libc::EIO), Ordering::Release);
                        self.outgoing
                            .error
                            .store(error.raw_os_error().unwrap_or(libc::EIO), Ordering::Release);
                    }
                    return;
                }
            }
        }
    }

    fn drain_notification(&self) -> io::Result<()> {
        match proactor::read(&self.wake, &mut [0; 256], None, true) {
            Ok(_) => Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(())
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                ) && !self.incoming.writer_alive.load(Ordering::Acquire) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn wait(&self, lane: &MemoryLane, deadline: Option<Instant>) -> io::Result<()> {
        let remaining = deadline
            .map(|deadline| {
                deadline
                    .checked_duration_since(Instant::now())
                    .ok_or_else(|| io::Error::from(io::ErrorKind::TimedOut))
            })
            .transpose()?;
        #[cfg(test)]
        lane.waiting.store(true, Ordering::Release);
        #[cfg(not(test))]
        lane.check_error()?;
        proactor::readable(&self.wake, remaining)
    }

    #[cfg(test)]
    pub(super) fn wait_flag(&self, writing: bool) -> Arc<AtomicBool> {
        Arc::clone(
            &if writing {
                &self.outgoing
            } else {
                &self.incoming
            }
            .waiting,
        )
    }
}

impl Read for MemoryPipe {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        if destination.is_empty() {
            return Ok(0);
        }
        let deadline = self
            .read_timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
        let mut drained = false;
        loop {
            let count = self.incoming.read(destination);
            if count > 0 {
                self.notify();
                return Ok(count);
            }
            if !self.incoming.writer_alive.load(Ordering::Acquire) {
                // Recheck after the release/acquire edge from peer Drop so the
                // final published bytes are drained even if the first tail was stale.
                let count = self.incoming.read(destination);
                if count > 0 {
                    self.notify();
                }
                return Ok(count);
            }
            self.incoming.check_error()?;
            if self.nonblocking {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            if drained {
                self.wait(&self.incoming, deadline)?;
            }
            self.drain_notification()?;
            // Always recheck shared state after draining, before registering a wait.
            drained = true;
        }
    }
}

impl Write for MemoryPipe {
    fn write(&mut self, source: &[u8]) -> io::Result<usize> {
        if source.is_empty() {
            return Ok(0);
        }
        let deadline = self
            .write_timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
        let mut drained = false;
        loop {
            if !self.outgoing.reader_alive.load(Ordering::Acquire) {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            self.outgoing.check_error()?;
            let count = self.outgoing.write(source);
            if count > 0 {
                self.notify();
                return Ok(count);
            }
            if self.nonblocking {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            if drained {
                self.wait(&self.outgoing, deadline)?;
            }
            self.drain_notification()?;
            drained = true;
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SyncTransport for MemoryPipe {
    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        self.nonblocking = nonblocking;
        Ok(())
    }
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        Self::validate_timeout(timeout)?;
        self.read_timeout = timeout;
        Ok(())
    }
    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        Self::validate_timeout(timeout)?;
        self.write_timeout = timeout;
        Ok(())
    }
    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.read_timeout)
    }
    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.write_timeout)
    }
    fn peer_endpoint(&self) -> io::Result<String> {
        Ok("memory://peer".into())
    }
}

impl Drop for MemoryPipe {
    fn drop(&mut self) {
        self.incoming.reader_alive.store(false, Ordering::Release);
        self.outgoing.writer_alive.store(false, Ordering::Release);
        // Closing the notification endpoint wakes peer waits even if notification
        // submission is unavailable during unwinding or thread-local destruction.
    }
}
