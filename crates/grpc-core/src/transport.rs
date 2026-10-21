//! Synchronous byte transports for blocking callers and externally driven event loops.
//!
//! Memory pipes are bounded and allocate their buffers at construction. Socket
//! transports use OS buffering; callers retain control over scheduling and threads.

use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

/// Synchronous HTTP/2 backend used by transport and session implementations.
pub use nghttp2 as http2;

/// A bidirectional byte stream with explicit blocking and timeout controls.
///
/// Nonblocking operations return [`io::ErrorKind::WouldBlock`] when no progress
/// is possible. A zero-length read result on a nonempty buffer indicates EOF.
/// Timeout settings apply to blocking operations; zero durations are invalid.
/// Callers must retry interrupted operations when using `read` or `write` directly.
pub trait SyncTransport: Read + Write {
    /// Select blocking or nonblocking I/O.
    ///
    /// # Errors
    /// Returns an error if the underlying transport cannot change its mode.
    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()>;

    /// Set the blocking read deadline relative to the start of each operation.
    ///
    /// # Errors
    /// Returns an error for a zero timeout or an OS configuration failure.
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()>;

    /// Set the blocking write deadline relative to the start of each operation.
    ///
    /// # Errors
    /// Returns an error for a zero timeout or an OS configuration failure.
    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()>;

    /// Return a diagnostic description of the remote endpoint.
    ///
    /// # Errors
    /// Returns an error if the underlying peer address cannot be queried.
    fn peer_endpoint(&self) -> io::Result<String>;
}

struct MemoryBuffer {
    bytes: VecDeque<u8>,
    capacity: usize,
    reader_alive: bool,
    writer_alive: bool,
}

struct MemoryLane {
    buffer: Mutex<MemoryBuffer>,
    changed: Condvar,
}

impl MemoryLane {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(MemoryBuffer {
                bytes: VecDeque::with_capacity(capacity),
                capacity,
                reader_alive: true,
                writer_alive: true,
            }),
            changed: Condvar::new(),
        }
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, MemoryBuffer>> {
        self.buffer
            .lock()
            .map_err(|_| io::Error::other("memory transport lock poisoned"))
    }

    fn wait<'a>(
        &self,
        guard: MutexGuard<'a, MemoryBuffer>,
        deadline: Option<Instant>,
    ) -> io::Result<MutexGuard<'a, MemoryBuffer>> {
        if let Some(deadline) = deadline {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::TimedOut, "memory transport timed out")
                })?;
            self.changed
                .wait_timeout(guard, remaining)
                .map(|(guard, _)| guard)
                .map_err(|_| io::Error::other("memory transport lock poisoned"))
        } else {
            self.changed
                .wait(guard)
                .map_err(|_| io::Error::other("memory transport lock poisoned"))
        }
    }
}

/// Bounded, full-duplex in-memory byte stream. Each direction has its own capacity.
/// Reads drain queued bytes before reporting EOF after the peer is dropped.
pub struct MemoryPipe {
    incoming: Arc<MemoryLane>,
    outgoing: Arc<MemoryLane>,
    nonblocking: bool,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl MemoryPipe {
    /// Creates a pair with 64 KiB of preallocated capacity in each direction.
    pub fn pair() -> (Self, Self) {
        Self::pair_with_capacity(64 * 1024)
    }

    /// Creates a pair with the given byte capacity in each direction.
    ///
    /// # Errors
    /// Returns `InvalidInput` if capacity is zero or exceeds the maximum allocation size.
    pub fn with_capacity(capacity: usize) -> io::Result<(Self, Self)> {
        if capacity == 0 || capacity > isize::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory transport capacity must be positive and fit an allocation",
            ));
        }
        Ok(Self::pair_with_capacity(capacity))
    }

    fn pair_with_capacity(capacity: usize) -> (Self, Self) {
        let incoming = Arc::new(MemoryLane::new(capacity));
        let outgoing = Arc::new(MemoryLane::new(capacity));
        let first = Self {
            incoming: Arc::clone(&incoming),
            outgoing: Arc::clone(&outgoing),
            nonblocking: false,
            read_timeout: None,
            write_timeout: None,
        };
        let second = Self {
            incoming: outgoing,
            outgoing: incoming,
            nonblocking: false,
            read_timeout: None,
            write_timeout: None,
        };
        (first, second)
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
}

impl Read for MemoryPipe {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        if destination.is_empty() {
            return Ok(0);
        }
        let deadline = self
            .read_timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
        let mut buffer = self.incoming.lock()?;
        loop {
            if !buffer.bytes.is_empty() {
                let count = destination.len().min(buffer.bytes.len());
                let (first, second) = buffer.bytes.as_slices();
                let first_count = count.min(first.len());
                destination[..first_count].copy_from_slice(&first[..first_count]);
                destination[first_count..count].copy_from_slice(&second[..count - first_count]);
                buffer.bytes.drain(..count);
                self.incoming.changed.notify_all();
                return Ok(count);
            }
            if !buffer.writer_alive {
                return Ok(0);
            }
            if self.nonblocking {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            buffer = self.incoming.wait(buffer, deadline)?;
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
        let mut buffer = self.outgoing.lock()?;
        loop {
            if !buffer.reader_alive {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            }
            let count = source.len().min(buffer.capacity - buffer.bytes.len());
            if count > 0 {
                buffer.bytes.extend(&source[..count]);
                self.outgoing.changed.notify_all();
                return Ok(count);
            }
            if self.nonblocking {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            buffer = self.outgoing.wait(buffer, deadline)?;
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

    fn peer_endpoint(&self) -> io::Result<String> {
        Ok("memory://peer".into())
    }
}

impl Drop for MemoryPipe {
    fn drop(&mut self) {
        // Recover poisoned guards so a failed peer still wakes blocked operations.
        let mut incoming = match self.incoming.buffer.lock() {
            Ok(buffer) => buffer,
            Err(error) => error.into_inner(),
        };
        incoming.reader_alive = false;
        self.incoming.changed.notify_all();
        drop(incoming);
        let mut outgoing = match self.outgoing.buffer.lock() {
            Ok(buffer) => buffer,
            Err(error) => error.into_inner(),
        };
        outgoing.writer_alive = false;
        self.outgoing.changed.notify_all();
    }
}

/// Socket settings applied before exposing a TCP transport.
#[derive(Debug, Clone, Copy)]
pub struct TcpTransportOptions {
    /// Idle interval before TCP keepalive probes; `None` disables keepalive.
    pub keepalive: Option<Duration>,
    /// Requested kernel send buffer size, or the operating system default.
    pub send_buffer_size: Option<usize>,
    /// Requested kernel receive buffer size, or the operating system default.
    pub recv_buffer_size: Option<usize>,
    /// Whether reads and writes return immediately when they would block.
    pub nonblocking: bool,
}

impl Default for TcpTransportOptions {
    fn default() -> Self {
        Self {
            keepalive: Some(Duration::from_secs(60)),
            send_buffer_size: None,
            recv_buffer_size: None,
            nonblocking: false,
        }
    }
}

/// A TCP byte stream with Nagle's algorithm disabled.
#[derive(Debug)]
pub struct TcpTransport(TcpStream);

impl TcpTransport {
    /// Connects to an endpoint with `TCP_NODELAY` enabled.
    ///
    /// # Errors
    /// Returns an error if connection or socket configuration fails.
    pub fn connect(address: impl ToSocketAddrs) -> io::Result<Self> {
        Self::connect_with_options(address, TcpTransportOptions::default())
    }

    /// Wraps a stream and enables `TCP_NODELAY`.
    ///
    /// # Errors
    /// Returns an error if applying the default socket settings fails.
    pub fn from_std(stream: TcpStream) -> io::Result<Self> {
        Self::from_std_with_options(stream, TcpTransportOptions::default())
    }

    /// Connects in blocking mode, then applies the requested socket settings.
    ///
    /// # Errors
    /// Returns an error if connecting or applying any option fails.
    pub fn connect_with_options(
        address: impl ToSocketAddrs,
        options: TcpTransportOptions,
    ) -> io::Result<Self> {
        Self::from_std_with_options(TcpStream::connect(address)?, options)
    }

    /// Applies socket settings, always enabling `TCP_NODELAY`.
    ///
    /// # Errors
    /// Returns an error if applying any option fails.
    pub fn from_std_with_options(
        stream: TcpStream,
        options: TcpTransportOptions,
    ) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        let socket = socket2::SockRef::from(&stream);
        socket.set_keepalive(options.keepalive.is_some())?;
        if let Some(interval) = options.keepalive {
            if interval.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "keepalive interval must be positive",
                ));
            }
            socket.set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(interval))?;
        }
        if let Some(size) = options.send_buffer_size {
            socket.set_send_buffer_size(size)?;
        }
        if let Some(size) = options.recv_buffer_size {
            socket.set_recv_buffer_size(size)?;
        }
        stream.set_nonblocking(options.nonblocking)?;
        Ok(Self(stream))
    }

    /// Returns the connected peer address.
    ///
    /// # Errors
    /// Returns an error if the socket address cannot be queried.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.peer_addr()
    }

    /// Returns the local socket address.
    ///
    /// # Errors
    /// Returns an error if the socket address cannot be queried.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
}

macro_rules! impl_stream_transport {
    ($transport:ident, $stream:ty) => {
        impl $transport {
            /// Borrows the underlying stream.
            pub fn get_ref(&self) -> &$stream {
                &self.0
            }

            /// Returns ownership of the underlying stream.
            pub fn into_inner(self) -> $stream {
                self.0
            }
        }

        impl io::Read for $transport {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                io::Read::read(&mut self.0, buffer)
            }
        }

        impl io::Write for $transport {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                io::Write::write(&mut self.0, buffer)
            }

            fn flush(&mut self) -> io::Result<()> {
                io::Write::flush(&mut self.0)
            }
        }

        impl SyncTransport for $transport {
            fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
                self.0.set_nonblocking(nonblocking)
            }

            fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
                self.0.set_read_timeout(timeout)
            }

            fn set_write_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
                self.0.set_write_timeout(timeout)
            }

            fn peer_endpoint(&self) -> io::Result<String> {
                self.endpoint()
            }
        }
    };
}

impl TcpTransport {
    fn endpoint(&self) -> io::Result<String> {
        Ok(self.0.peer_addr()?.to_string())
    }
}

impl_stream_transport!(TcpTransport, TcpStream);

/// A TCP listener whose accepted streams enable `TCP_NODELAY`.
#[derive(Debug)]
pub struct TcpTransportListener(TcpListener);

impl TcpTransportListener {
    /// Binds a TCP listener.
    ///
    /// # Errors
    /// Returns an error if address resolution or binding fails.
    pub fn bind(address: impl ToSocketAddrs) -> io::Result<Self> {
        let mut last_error =
            io::Error::new(io::ErrorKind::InvalidInput, "no socket addresses resolved");
        for address in address.to_socket_addrs()? {
            let result = (|| {
                let socket = socket2::Socket::new(
                    socket2::Domain::for_address(address),
                    socket2::Type::STREAM,
                    Some(socket2::Protocol::TCP),
                )?;
                socket.set_reuse_address(true)?;
                socket.bind(&address.into())?;
                socket.listen(128)?;
                Ok(Self(socket.into()))
            })();
            match result {
                Ok(listener) => return Ok(listener),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    /// Wraps an existing listener.
    pub fn from_std(listener: TcpListener) -> Self {
        Self(listener)
    }

    /// Accepts a stream and configures `TCP_NODELAY`.
    ///
    /// # Errors
    /// Returns an error if accepting or configuring the stream fails.
    pub fn accept(&self) -> io::Result<(TcpTransport, SocketAddr)> {
        let (stream, address) = self.0.accept()?;
        Ok((TcpTransport::from_std(stream)?, address))
    }

    /// Returns the bound address, including the assigned ephemeral port.
    ///
    /// # Errors
    /// Returns an error if the address cannot be queried.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }

    /// Configures whether accepting returns immediately.
    ///
    /// # Errors
    /// Returns an error if the operating system rejects the setting.
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.set_nonblocking(nonblocking)
    }
}

/// A Unix domain byte stream.
#[cfg(unix)]
#[derive(Debug)]
pub struct UnixTransport(std::os::unix::net::UnixStream);

#[cfg(unix)]
impl UnixTransport {
    /// Connects to a Unix domain socket path.
    ///
    /// # Errors
    /// Returns an error if the connection fails.
    pub fn connect(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        Self::from_std(std::os::unix::net::UnixStream::connect(path)?)
    }

    /// Wraps an existing Unix domain stream.
    ///
    /// # Errors
    /// This operation currently always succeeds.
    pub fn from_std(stream: std::os::unix::net::UnixStream) -> io::Result<Self> {
        Ok(Self(stream))
    }

    fn endpoint(&self) -> io::Result<String> {
        let address = self.0.peer_addr()?;
        Ok(match address.as_pathname() {
            Some(path) => format!("unix:{}", path.display()),
            None => format!("unix:{address:?}"),
        })
    }
}

#[cfg(unix)]
impl_stream_transport!(UnixTransport, std::os::unix::net::UnixStream);

/// A Unix domain listener. The caller owns cleanup of its socket path.
#[cfg(unix)]
#[derive(Debug)]
pub struct UnixTransportListener(std::os::unix::net::UnixListener);

#[cfg(unix)]
impl UnixTransportListener {
    /// Binds a Unix domain socket path.
    ///
    /// # Errors
    /// Returns an error if the path cannot be bound.
    pub fn bind(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        Ok(Self(std::os::unix::net::UnixListener::bind(path)?))
    }

    /// Wraps an existing Unix domain listener.
    pub fn from_std(listener: std::os::unix::net::UnixListener) -> Self {
        Self(listener)
    }

    /// Accepts a connected Unix domain stream.
    ///
    /// # Errors
    /// Returns an error if accepting fails.
    pub fn accept(&self) -> io::Result<(UnixTransport, std::os::unix::net::SocketAddr)> {
        let (stream, address) = self.0.accept()?;
        Ok((UnixTransport::from_std(stream)?, address))
    }

    /// Returns the bound Unix domain socket address.
    ///
    /// # Errors
    /// Returns an error if the address cannot be queried.
    pub fn local_addr(&self) -> io::Result<std::os::unix::net::SocketAddr> {
        self.0.local_addr()
    }

    /// Configures whether accepting returns immediately.
    ///
    /// # Errors
    /// Returns an error if the operating system rejects the setting.
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.0.set_nonblocking(nonblocking)
    }
}

#[cfg(test)]
mod tests {
    use io::ErrorKind::{BrokenPipe, TimedOut, WouldBlock};

    use super::*;

    fn error_kind<T>(result: io::Result<T>) -> Option<io::ErrorKind> {
        result.err().map(|error| error.kind())
    }

    #[test]
    fn memory_full_duplex_and_eof() -> io::Result<()> {
        let (mut first, mut second) = MemoryPipe::pair();
        first.write_all(b"forward")?;
        second.write_all(b"back")?;
        let mut forward = [0; 7];
        let mut back = [0; 4];
        second.read_exact(&mut forward)?;
        first.read_exact(&mut back)?;
        assert_eq!(&forward, b"forward");
        assert_eq!(&back, b"back");
        first.write_all(b"queued")?;
        drop(first);
        let mut drained = Vec::new();
        second.read_to_end(&mut drained)?;
        assert_eq!(drained, b"queued");
        assert_eq!(error_kind(second.write(b"x")), Some(BrokenPipe));
        assert_eq!(second.read(&mut [])?, 0);
        assert_eq!(second.write(&[])?, 0);
        Ok(())
    }

    #[test]
    fn memory_nonblocking_capacity_and_wraparound() -> io::Result<()> {
        let (mut first, mut second) = MemoryPipe::with_capacity(3)?;
        first.set_nonblocking(true)?;
        second.set_nonblocking(true)?;
        let mut bytes = [0; 3];
        assert_eq!(error_kind(first.read(&mut bytes)), Some(WouldBlock));
        assert_eq!(first.write(b"abcd")?, 3);
        assert_eq!(error_kind(first.write(b"e")), Some(WouldBlock));
        assert_eq!(second.read(&mut bytes[..2])?, 2);
        assert_eq!(&bytes[..2], b"ab");
        first.write_all(b"de")?;
        second.read_exact(&mut bytes)?;
        assert_eq!(&bytes, b"cde");
        drop(first);
        assert_eq!(second.read(&mut bytes)?, 0);
        assert!(MemoryPipe::with_capacity(0).is_err());
        assert!(MemoryPipe::with_capacity(usize::MAX).is_err());
        Ok(())
    }

    #[test]
    fn memory_timeouts_and_validation() -> io::Result<()> {
        let (mut first, _second) = MemoryPipe::with_capacity(1)?;
        assert!(first.set_read_timeout(Some(Duration::ZERO)).is_err());
        assert!(first.set_write_timeout(Some(Duration::ZERO)).is_err());
        assert!(first.set_read_timeout(Some(Duration::MAX)).is_err());
        first.set_read_timeout(Some(Duration::from_millis(10)))?;
        first.set_write_timeout(Some(Duration::from_millis(10)))?;
        let mut byte = [0];
        assert_eq!(error_kind(first.read(&mut byte)), Some(TimedOut));
        first.write_all(b"a")?;
        assert_eq!(error_kind(first.write(b"b")), Some(TimedOut));
        first.set_nonblocking(true)?;
        assert_eq!(error_kind(first.read(&mut byte)), Some(WouldBlock));
        first.set_read_timeout(None)?;
        first.set_write_timeout(None)?;
        Ok(())
    }

    #[test]
    fn memory_blocking_reader_wakes_on_data_and_drop() -> io::Result<()> {
        let (mut first, mut second) = MemoryPipe::pair();
        first.set_read_timeout(Some(Duration::from_secs(5)))?;
        let reader = std::thread::spawn(move || -> io::Result<Vec<u8>> {
            let mut bytes = Vec::new();
            first.read_to_end(&mut bytes)?;
            Ok(bytes)
        });
        second.write_all(b"wake")?;
        drop(second);
        assert_eq!(
            reader
                .join()
                .map_err(|_| io::Error::other("reader panicked"))??,
            b"wake"
        );
        Ok(())
    }

    #[test]
    fn memory_blocking_writer_wakes_on_read() -> io::Result<()> {
        let (mut first, mut second) = MemoryPipe::with_capacity(1)?;
        first.set_write_timeout(Some(Duration::from_secs(5)))?;
        second.set_read_timeout(Some(Duration::from_secs(5)))?;
        first.write_all(b"a")?;
        let writer = std::thread::spawn(move || -> io::Result<()> { first.write_all(b"bc") });
        let mut bytes = [0; 3];
        second.read_exact(&mut bytes)?;
        writer
            .join()
            .map_err(|_| io::Error::other("writer panicked"))??;
        assert_eq!(&bytes, b"abc");
        Ok(())
    }

    #[test]
    fn memory_blocking_writer_wakes_on_peer_drop() -> io::Result<()> {
        let (mut first, second) = MemoryPipe::with_capacity(1)?;
        first.set_write_timeout(Some(Duration::from_secs(5)))?;
        first.write_all(b"a")?;
        let writer = std::thread::spawn(move || first.write(b"b"));
        drop(second);
        let result = writer
            .join()
            .map_err(|_| io::Error::other("writer panicked"))?;
        assert_eq!(error_kind(result), Some(BrokenPipe));
        Ok(())
    }

    #[test]
    fn tcp_loopback_enables_nodelay_and_transfers_both_directions() -> io::Result<()> {
        let listener = TcpTransportListener::bind("127.0.0.1:0")?;
        let mut client = TcpTransport::connect(listener.local_addr()?)?;
        let (mut server, peer) = listener.accept()?;
        assert!(client.get_ref().nodelay()?);
        assert!(server.get_ref().nodelay()?);
        assert!(socket2::SockRef::from(client.get_ref()).keepalive()?);
        assert!(socket2::SockRef::from(server.get_ref()).keepalive()?);
        assert_eq!(peer, client.local_addr()?);
        assert_eq!(client.peer_endpoint()?, listener.local_addr()?.to_string());
        client.set_read_timeout(Some(Duration::from_secs(2)))?;
        server.set_read_timeout(Some(Duration::from_secs(2)))?;
        client.write_all(b"request")?;
        let mut request = [0; 7];
        server.read_exact(&mut request)?;
        assert_eq!(&request, b"request");
        server.write_all(b"reply")?;
        let mut reply = [0; 5];
        client.read_exact(&mut reply)?;
        assert_eq!(&reply, b"reply");
        client.set_nonblocking(true)?;
        assert_eq!(
            client.read(&mut reply).expect_err("empty socket").kind(),
            WouldBlock
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn unix_socket_transfers_both_directions() -> io::Result<()> {
        struct SocketDirectory(std::path::PathBuf);
        impl Drop for SocketDirectory {
            fn drop(&mut self) {
                drop(std::fs::remove_dir_all(&self.0));
            }
        }
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let directory = loop {
            let serial = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("grpc-transport-{}-{serial}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => break SocketDirectory(path),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        };
        let path = directory.0.join("socket");
        let listener = UnixTransportListener::bind(&path)?;
        assert_eq!(listener.local_addr()?.as_pathname(), Some(path.as_path()));
        let mut client = UnixTransport::connect(&path)?;
        let (mut server, _) = listener.accept()?;
        client.set_read_timeout(Some(Duration::from_secs(2)))?;
        server.set_read_timeout(Some(Duration::from_secs(2)))?;
        assert_eq!(client.peer_endpoint()?, format!("unix:{}", path.display()));
        client.write_all(b"ping")?;
        let mut buffer = [0; 4];
        server.read_exact(&mut buffer)?;
        assert_eq!(&buffer, b"ping");
        server.write_all(b"pong")?;
        client.read_exact(&mut buffer)?;
        assert_eq!(&buffer, b"pong");
        Ok(())
    }

    #[test]
    fn tcp_options_apply_buffers_keepalive_and_nonblocking() -> io::Result<()> {
        let listener = TcpTransportListener::bind("127.0.0.1:0")?;
        let options = TcpTransportOptions {
            keepalive: None,
            send_buffer_size: Some(8192),
            recv_buffer_size: Some(8192),
            nonblocking: true,
        };
        let mut client = TcpTransport::connect_with_options(listener.local_addr()?, options)?;
        let (_server, _) = listener.accept()?;
        let socket = socket2::SockRef::from(client.get_ref());
        assert!(!socket.keepalive()?);
        assert!(socket.send_buffer_size()? >= 8192);
        assert!(socket.recv_buffer_size()? >= 8192);
        assert!(client.get_ref().nodelay()?);
        assert_eq!(
            client.read(&mut [0]).expect_err("empty socket").kind(),
            WouldBlock
        );
        listener.set_nonblocking(true)?;
        assert_eq!(
            listener.accept().expect_err("empty accept queue").kind(),
            WouldBlock
        );
        Ok(())
    }
}
