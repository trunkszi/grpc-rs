//! Synchronous rustls transports with authenticated HTTP/2 ALPN negotiation.
//!
//! Constructors perform no I/O. Call `handshake` explicitly or let the first
//! nonempty read/write complete it. Nonblocking callers retry after readiness;
//! `handshake_until` bounds negotiation by an absolute deadline and restores
//! the underlying transport timeouts before returning.

use std::{
    io::{self, Read, Write},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::transport::SyncTransport;

/// Trusted roots and HTTP/2 ALPN policy for synchronous TLS clients.
#[derive(Clone)]
pub struct TlsClientConfig {
    pub(crate) config: Arc<rustls::ClientConfig>,
}

impl TlsClientConfig {
    /// Builds a TLS 1.3 client with certificate and server-name verification.
    ///
    /// # Errors
    /// Returns an error if the TLS provider cannot support TLS 1.3.
    pub fn new(roots: rustls::RootCertStore) -> io::Result<Self> {
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        Ok(Self::from_rustls(config))
    }

    /// Uses a caller's certificate policy, requiring HTTP/2 and disabling 0-RTT.
    ///
    /// The caller remains responsible for the supplied configuration's verifier,
    /// protocol versions, and cryptographic provider.
    pub fn from_rustls(mut config: rustls::ClientConfig) -> Self {
        config.alpn_protocols = vec![b"h2".to_vec()];
        config.enable_early_data = false;
        Self {
            config: Arc::new(config),
        }
    }

    /// Returns the immutable rustls configuration.
    pub fn get_ref(&self) -> &rustls::ClientConfig {
        &self.config
    }
}

/// Server identity and HTTP/2 ALPN policy for synchronous TLS servers.
#[derive(Clone)]
pub struct TlsServerConfig {
    pub(crate) config: Arc<rustls::ServerConfig>,
}

impl TlsServerConfig {
    /// Builds a TLS 1.3 server from a DER certificate chain and private key.
    ///
    /// Clients authenticate the server; client certificates are not required.
    ///
    /// # Errors
    /// Returns an error for an unsupported provider, invalid identity, or a
    /// certificate and private key that do not match.
    pub fn new(
        cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
        private_key: rustls::pki_types::PrivateKeyDer<'static>,
    ) -> io::Result<Self> {
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        Ok(Self::from_rustls(config))
    }

    /// Uses a caller's certificate policy, requiring HTTP/2 and disabling 0-RTT.
    ///
    /// The caller remains responsible for the supplied configuration's client
    /// authentication, protocol versions, and cryptographic provider.
    pub fn from_rustls(mut config: rustls::ServerConfig) -> Self {
        config.alpn_protocols = vec![b"h2".to_vec()];
        config.max_early_data_size = 0;
        Self {
            config: Arc::new(config),
        }
    }

    /// Returns the immutable rustls configuration.
    pub fn get_ref(&self) -> &rustls::ServerConfig {
        &self.config
    }
}

/// A synchronous TLS stream that permits application I/O only after HTTP/2 ALPN.
///
/// Construction performs no I/O. Call `handshake` or perform application
/// I/O to drive negotiation. In nonblocking mode, retry `WouldBlock` operations
/// when the socket becomes ready. Writes may buffer accepted plaintext: call
/// `flush` to drain encrypted records before waiting for a peer response.
/// Dropping the transport closes its socket without blocking or sending an alert.
pub struct TlsTransport<C, T: SyncTransport> {
    stream: rustls::StreamOwned<C, T>,
    state: HandshakeState,
    write_closed: bool,
}

#[derive(Clone, Copy)]
enum HandshakeState {
    Negotiating,
    Ready,
    Failed(io::ErrorKind),
}

impl<T: SyncTransport> TlsTransport<rustls::ClientConnection, T> {
    /// Creates a client stream without performing network I/O.
    ///
    /// # Errors
    /// Returns an error if rustls rejects the connection configuration.
    pub fn client(
        sock: T,
        config: &TlsClientConfig,
        server_name: rustls::pki_types::ServerName<'static>,
    ) -> io::Result<Self> {
        let conn = rustls::ClientConnection::new(Arc::clone(&config.config), server_name)
            .map_err(io::Error::other)?;
        Ok(Self {
            stream: rustls::StreamOwned::new(conn, sock),
            state: HandshakeState::Negotiating,
            write_closed: false,
        })
    }
}

impl<T: SyncTransport> TlsTransport<rustls::ServerConnection, T> {
    /// Creates a server stream without performing network I/O.
    ///
    /// # Errors
    /// Returns an error if rustls rejects the connection configuration.
    pub fn server(sock: T, config: &TlsServerConfig) -> io::Result<Self> {
        let conn =
            rustls::ServerConnection::new(Arc::clone(&config.config)).map_err(io::Error::other)?;
        Ok(Self {
            stream: rustls::StreamOwned::new(conn, sock),
            state: HandshakeState::Negotiating,
            write_closed: false,
        })
    }
}

impl<C, T: SyncTransport> TlsTransport<C, T> {
    /// Borrows the underlying transport for inspection.
    pub fn get_ref(&self) -> &T {
        &self.stream.sock
    }
}

fn remaining_handshake_time(deadline: Option<Instant>) -> io::Result<Option<Duration>> {
    deadline
        .map(|deadline| {
            deadline
                .checked_duration_since(Instant::now())
                .filter(|left| !left.is_zero())
                .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "tls handshake timed out"))
        })
        .transpose()
}

fn prepare_handshake_io<T: SyncTransport>(
    transport: &mut T,
    deadline: Option<Instant>,
) -> io::Result<()> {
    if let Some(remaining) = remaining_handshake_time(deadline)? {
        transport.set_read_timeout(Some(remaining))?;
        transport.set_write_timeout(Some(remaining))?;
    }
    Ok(())
}

macro_rules! impl_tls_transport {
    ($connection:ty, $drain_final_output:expr) => {
        impl<T: SyncTransport> TlsTransport<$connection, T> {
            /// Completes negotiation and verifies that the peer selected HTTP/2.
            ///
            /// Retains progress when nonblocking I/O returns `WouldBlock`.
            /// Configure socket timeouts to bound blocking negotiation.
            ///
            /// # Errors
            /// Returns TLS, I/O, or ALPN errors. Fatal failures permanently disable
            /// application I/O; `WouldBlock`, `Interrupted`, and `TimedOut` can be retried.
            pub fn handshake(&mut self) -> io::Result<()> {
                self.handshake_until(None)
            }

            /// Negotiates HTTP/2 TLS under one absolute deadline across all I/O.
            ///
            /// Every encrypted read, write and flush uses the remaining budget.
            /// Both original transport timeouts are restored before returning,
            /// including after WouldBlock, timeout, TLS failure or setup failure.
            /// None preserves the transport's configured per-operation timeouts.
            ///
            /// # Errors
            /// Returns TimedOut with "tls handshake timed out" when the deadline
            /// expires. Also returns TLS, ALPN, transport and timeout-query errors.
            /// Custom transports must support timeout queries for Some(deadline).
            /// A timeout restoration failure permanently disables application I/O.
            pub fn handshake_until(&mut self, deadline: Option<Instant>) -> io::Result<()> {
                match self.state {
                    HandshakeState::Ready => return Ok(()),
                    HandshakeState::Failed(kind) => {
                        return Err(io::Error::new(kind, "TLS handshake previously failed"));
                    }
                    HandshakeState::Negotiating => {}
                }
                let result = if deadline.is_some() {
                    remaining_handshake_time(deadline)?;
                    let read_timeout = self.stream.sock.read_timeout()?;
                    let write_timeout = self.stream.sock.write_timeout()?;
                    let result = self.drive_handshake(deadline);
                    let restored_read = self.stream.sock.set_read_timeout(read_timeout);
                    let restored_write = self.stream.sock.set_write_timeout(write_timeout);
                    if restored_read.is_err() || restored_write.is_err() {
                        let kind = restored_read
                            .as_ref()
                            .err()
                            .or(restored_write.as_ref().err())
                            .map_or(io::ErrorKind::Other, io::Error::kind);
                        self.state = HandshakeState::Failed(kind);
                        return Err(io::Error::new(
                            kind,
                            format!(
                                "restore TLS transport timeouts: read={restored_read:?}, \
                                 write={restored_write:?}; handshake={result:?}"
                            ),
                        ));
                    }
                    result
                } else {
                    self.drive_handshake(None)
                };
                if let Err(error) = result {
                    let error = if error.kind() == io::ErrorKind::TimedOut && deadline.is_some() {
                        remaining_handshake_time(deadline).err().unwrap_or(error)
                    } else {
                        error
                    };
                    if !matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::Interrupted
                            | io::ErrorKind::TimedOut
                    ) {
                        self.state = HandshakeState::Failed(error.kind());
                    }
                    return Err(error);
                }
                self.state = HandshakeState::Ready;
                Ok(())
            }

            fn drive_handshake(&mut self, deadline: Option<Instant>) -> io::Result<()> {
                while self.stream.conn.is_handshaking() {
                    self.drain_handshake_output(deadline)?;
                    prepare_handshake_io(&mut self.stream.sock, deadline)?;
                    if self.stream.conn.read_tls(&mut self.stream.sock)? == 0 {
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    if let Err(error) = self.stream.conn.process_new_packets() {
                        // Preserve the protocol error if sending its alert fails.
                        drop(self.drain_handshake_output(deadline));
                        return Err(io::Error::new(io::ErrorKind::InvalidData, error));
                    }
                }
                if self.stream.conn.alpn_protocol() != Some(b"h2".as_slice()) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TLS peer did not negotiate HTTP/2 ALPN",
                    ));
                }
                // Client Finished must reach the peer. Server session tickets can
                // wait for application I/O without delaying handshake readiness.
                if $drain_final_output {
                    self.drain_handshake_output(deadline)?;
                }
                Ok(())
            }

            fn drain_handshake_output(&mut self, deadline: Option<Instant>) -> io::Result<()> {
                while self.stream.conn.wants_write() {
                    prepare_handshake_io(&mut self.stream.sock, deadline)?;
                    if self.stream.conn.write_tls(&mut self.stream.sock)? == 0 {
                        return Err(io::ErrorKind::WriteZero.into());
                    }
                }
                prepare_handshake_io(&mut self.stream.sock, deadline)?;
                self.stream.sock.flush()
            }

            /// Returns ALPN only after successful HTTP/2 negotiation.
            pub fn negotiated_alpn(&self) -> Option<&[u8]> {
                matches!(self.state, HandshakeState::Ready)
                    .then(|| self.stream.conn.alpn_protocol())
                    .flatten()
            }

            /// Returns the TLS version only after successful HTTP/2 negotiation.
            pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
                matches!(self.state, HandshakeState::Ready)
                    .then(|| self.stream.conn.protocol_version())
                    .flatten()
            }

            /// Sends a TLS close notification and flushes pending encrypted data.
            ///
            /// # Errors
            /// Returns negotiation or socket errors. Retry `WouldBlock` to drain
            /// pending records. Dropping the stream does not invoke this method.
            pub fn close_notify(&mut self) -> io::Result<()> {
                self.handshake()?;
                self.write_closed = true;
                self.stream.conn.send_close_notify();
                self.stream.flush()
            }
        }

        impl<T: SyncTransport> Read for TlsTransport<$connection, T> {
            fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
                if destination.is_empty() {
                    return Ok(0);
                }
                self.handshake()?;
                self.stream.read(destination)
            }
        }

        impl<T: SyncTransport> Write for TlsTransport<$connection, T> {
            fn write(&mut self, source: &[u8]) -> io::Result<usize> {
                if source.is_empty() {
                    return Ok(0);
                }
                self.handshake()?;
                if self.write_closed {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "TLS write direction is closed",
                    ));
                }
                // StreamOwned reports bytes already accepted even if its socket
                // write blocks, so retrying cannot duplicate application bytes.
                self.stream.write(source)
            }

            fn flush(&mut self) -> io::Result<()> {
                self.handshake()?;
                self.stream.flush()
            }
        }

        impl<T: SyncTransport> SyncTransport for TlsTransport<$connection, T> {
            fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
                self.stream.sock.set_nonblocking(nonblocking)
            }

            fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
                self.stream.sock.set_read_timeout(timeout)
            }

            fn set_write_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
                self.stream.sock.set_write_timeout(timeout)
            }

            fn read_timeout(&self) -> io::Result<Option<Duration>> {
                self.stream.sock.read_timeout()
            }

            fn write_timeout(&self) -> io::Result<Option<Duration>> {
                self.stream.sock.write_timeout()
            }

            fn peer_endpoint(&self) -> io::Result<String> {
                self.stream.sock.peer_endpoint()
            }
        }
    };
}

impl_tls_transport!(rustls::ClientConnection, true);
impl_tls_transport!(rustls::ServerConnection, false);

#[cfg(test)]
#[path = "../tests/unit/tls.rs"]
mod tests;
