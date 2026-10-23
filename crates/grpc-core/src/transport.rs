//! Synchronous byte transports for blocking callers and externally driven event loops.
//!
//! Memory pipes are bounded and allocate their buffers at construction. Socket
//! transports drive a calling-thread io_uring proactor in blocking mode.

#[cfg(unix)]
use std::os::unix::net::{SocketAddr as UnixSocketAddr, UnixListener, UnixStream};
use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    time::Duration,
};

#[path = "transport/memory.rs"]
mod memory;
#[path = "transport/proactor.rs"]
mod proactor;
pub use memory::MemoryPipe;

/// Synchronous HTTP/2 backend used by transport and session implementations.
pub use crate::http2;

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

    /// Returns the current blocking read timeout for temporary deadline overrides.
    ///
    /// # Errors
    /// Returns an error if the current transport setting cannot be queried.
    fn read_timeout(&self) -> io::Result<Option<Duration>>;

    /// Returns the current blocking write timeout for temporary deadline overrides.
    ///
    /// # Errors
    /// Returns an error if the current transport setting cannot be queried.
    fn write_timeout(&self) -> io::Result<Option<Duration>>;

    /// Return a diagnostic description of the remote endpoint.
    ///
    /// # Errors
    /// Returns an error if the underlying peer address cannot be queried.
    fn peer_endpoint(&self) -> io::Result<String>;
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
        let mut last = io::Error::new(io::ErrorKind::InvalidInput, "no socket addresses resolved");
        for address in address.to_socket_addrs()? {
            let result = (|| {
                let socket = socket2::Socket::new(
                    socket2::Domain::for_address(address),
                    socket2::Type::STREAM,
                    Some(socket2::Protocol::TCP),
                )?;
                proactor::connect(&socket, address.into())?;
                Self::from_std_with_options(socket.into(), options)
            })();
            match result {
                Ok(transport) => return Ok(transport),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// Applies socket settings, always enabling `TCP_NODELAY`.
    ///
    /// # Errors
    /// Returns an error if applying any option fails.
    pub fn from_std_with_options(
        stream: TcpStream,
        options: TcpTransportOptions,
    ) -> io::Result<Self> {
        for size in [options.send_buffer_size, options.recv_buffer_size]
            .into_iter()
            .flatten()
        {
            if size == 0 || size > i32::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "socket buffer size must fit a positive c_int",
                ));
            }
        }
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

    fn endpoint(&self) -> io::Result<String> {
        Ok(self.0.peer_addr()?.to_string())
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
                proactor::read(
                    &self.0,
                    buffer,
                    self.0.read_timeout()?,
                    proactor::is_nonblocking(&self.0)?,
                )
            }
        }

        impl io::Write for $transport {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                proactor::write(
                    &self.0,
                    buffer,
                    self.0.write_timeout()?,
                    proactor::is_nonblocking(&self.0)?,
                )
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
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

            fn read_timeout(&self) -> io::Result<Option<Duration>> {
                self.0.read_timeout()
            }

            fn write_timeout(&self) -> io::Result<Option<Duration>> {
                self.0.write_timeout()
            }

            fn peer_endpoint(&self) -> io::Result<String> {
                self.endpoint()
            }
        }
    };
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
}

/// A Unix domain byte stream.
#[cfg(unix)]
#[derive(Debug)]
pub struct UnixTransport(UnixStream);

#[cfg(unix)]
impl UnixTransport {
    /// Connects to a Unix domain socket path.
    ///
    /// # Errors
    /// Returns an error if the connection fails.
    pub fn connect(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let address = socket2::SockAddr::unix(path)?;
        let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
        proactor::connect(&socket, address)?;
        Self::from_std(socket.into())
    }

    /// Wraps an existing Unix domain stream.
    ///
    /// # Errors
    /// This operation currently always succeeds.
    pub fn from_std(stream: UnixStream) -> io::Result<Self> {
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
impl_stream_transport!(UnixTransport, UnixStream);

/// A Unix domain listener. The caller owns cleanup of its socket path.
#[cfg(unix)]
#[derive(Debug)]
pub struct UnixTransportListener(UnixListener);

#[cfg(unix)]
impl UnixTransportListener {
    /// Binds a Unix domain socket path.
    ///
    /// # Errors
    /// Returns an error if the path cannot be bound.
    pub fn bind(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        Ok(Self(UnixListener::bind(path)?))
    }
}

macro_rules! impl_listener {
    ($wrapper:ident, $listener:ty, $stream:ty, $transport:ident, $address:ty) => {
        impl $wrapper {
            /// Wraps an existing listener.
            pub fn from_std(listener: $listener) -> Self {
                Self(listener)
            }

            /// Accepts and configures a connected transport.
            ///
            /// # Errors
            /// Returns an error if accepting or configuring the stream fails.
            pub fn accept(&self) -> io::Result<($transport, $address)> {
                let fd = proactor::accept(&self.0)?;
                let stream = <$stream>::from(fd);
                let address = stream.peer_addr()?;
                Ok(($transport::from_std(stream)?, address))
            }

            /// Returns the bound socket address.
            ///
            /// # Errors
            /// Returns an error if the address cannot be queried.
            pub fn local_addr(&self) -> io::Result<$address> {
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
    };
}
impl_listener!(
    TcpTransportListener,
    TcpListener,
    TcpStream,
    TcpTransport,
    SocketAddr
);
#[cfg(unix)]
impl_listener!(
    UnixTransportListener,
    UnixListener,
    UnixStream,
    UnixTransport,
    UnixSocketAddr
);

#[cfg(test)]
#[path = "../tests/unit/transport.rs"]
mod tests;
