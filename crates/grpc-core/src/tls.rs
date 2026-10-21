//! Synchronous rustls transports with authenticated HTTP/2 ALPN negotiation.
//!
//! Constructors perform no I/O. Call `handshake` explicitly or let the first
//! nonempty read/write complete it. Nonblocking callers retry after readiness;
//! blocking callers must configure socket timeouts to bound handshake waits.

use std::{
    io::{self, Read, Write},
    sync::Arc,
    time::Duration,
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
                match self.state {
                    HandshakeState::Ready => return Ok(()),
                    HandshakeState::Failed(kind) => {
                        return Err(io::Error::new(kind, "TLS handshake previously failed"));
                    }
                    HandshakeState::Negotiating => {}
                }
                if let Err(error) = self.drive_handshake() {
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

            fn drive_handshake(&mut self) -> io::Result<()> {
                while self.stream.conn.is_handshaking() {
                    self.drain_handshake_output()?;
                    if self.stream.conn.read_tls(&mut self.stream.sock)? == 0 {
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    if let Err(error) = self.stream.conn.process_new_packets() {
                        // Preserve the protocol error if sending its alert fails.
                        drop(self.drain_handshake_output());
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
                    self.drain_handshake_output()?;
                }
                Ok(())
            }

            fn drain_handshake_output(&mut self) -> io::Result<()> {
                while self.stream.conn.wants_write() {
                    if self.stream.conn.write_tls(&mut self.stream.sock)? == 0 {
                        return Err(io::ErrorKind::WriteZero.into());
                    }
                }
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

            fn peer_endpoint(&self) -> io::Result<String> {
                self.stream.sock.peer_endpoint()
            }
        }
    };
}

impl_tls_transport!(rustls::ClientConnection, true);
impl_tls_transport!(rustls::ServerConnection, false);

#[cfg(test)]
mod tests {
    use std::thread::{self, JoinHandle};

    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};

    use super::*;
    use crate::transport::MemoryPipe;

    fn configs() -> io::Result<(TlsClientConfig, TlsServerConfig)> {
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .map_err(io::Error::other)?;
        let certificate = CertificateDer::from(identity.cert.der().to_vec());
        let key = PrivatePkcs8KeyDer::from(identity.key_pair.serialize_der()).into();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate.clone()).map_err(io::Error::other)?;
        Ok((
            TlsClientConfig::new(roots)?,
            TlsServerConfig::new(vec![certificate], key)?,
        ))
    }

    fn pipes() -> io::Result<(MemoryPipe, MemoryPipe)> {
        let (mut first, mut second) = MemoryPipe::with_capacity(64 * 1024)?;
        for pipe in [&mut first, &mut second] {
            pipe.set_read_timeout(Some(Duration::from_secs(3)))?;
            pipe.set_write_timeout(Some(Duration::from_secs(3)))?;
        }
        Ok((first, second))
    }

    fn joined<T>(worker: JoinHandle<io::Result<T>>) -> io::Result<T> {
        worker
            .join()
            .map_err(|_| io::Error::other("TLS worker panicked"))?
    }

    fn localhost() -> io::Result<ServerName<'static>> {
        ServerName::try_from("localhost".to_owned()).map_err(io::Error::other)
    }

    #[test]
    fn tls13_h2_handshake_and_bidirectional_frames() -> io::Result<()> {
        let (client_config, server_config) = configs()?;
        assert_eq!(client_config.get_ref().alpn_protocols, [b"h2".to_vec()]);
        assert_eq!(server_config.get_ref().alpn_protocols, [b"h2".to_vec()]);
        let (client_pipe, server_pipe) = pipes()?;
        let worker = thread::spawn(move || -> io::Result<()> {
            let mut server =
                TlsTransport::<rustls::ServerConnection, _>::server(server_pipe, &server_config)?;
            server.handshake()?;
            assert_eq!(server.negotiated_alpn(), Some(b"h2".as_slice()));
            assert_eq!(
                server.protocol_version(),
                Some(rustls::ProtocolVersion::TLSv1_3)
            );
            let mut request = [0; 9];
            server.read_exact(&mut request)?;
            assert_eq!(request, *b"\0\0\0\0\x04ping");
            server.write_all(b"\0\0\0\0\x04pong")?;
            server.flush()?;
            Ok(())
        });
        let mut client = TlsTransport::<rustls::ClientConnection, _>::client(
            client_pipe,
            &client_config,
            localhost()?,
        )?;
        client.handshake()?;
        assert_eq!(client.negotiated_alpn(), Some(b"h2".as_slice()));
        assert_eq!(
            client.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3)
        );
        assert_eq!(client.peer_endpoint()?, client.get_ref().peer_endpoint()?);
        client.write_all(b"\0\0\0\0\x04ping")?;
        client.flush()?;
        let mut response = [0; 9];
        client.read_exact(&mut response)?;
        assert_eq!(response, *b"\0\0\0\0\x04pong");
        joined(worker)
    }

    #[test]
    fn nonblocking_handshake_resumes_after_would_block() -> io::Result<()> {
        for capacity in [32, 64 * 1024] {
            let (client_config, server_config) = configs()?;
            let (client_pipe, server_pipe) = MemoryPipe::with_capacity(capacity)?;
            let mut client = TlsTransport::<rustls::ClientConnection, _>::client(
                client_pipe,
                &client_config,
                localhost()?,
            )?;
            let mut server =
                TlsTransport::<rustls::ServerConnection, _>::server(server_pipe, &server_config)?;
            client.set_nonblocking(true)?;
            server.set_nonblocking(true)?;
            let (mut client_ready, mut server_ready, mut blocked) = (false, false, false);
            for _ in 0..10000 {
                for (ready, result) in [
                    (&mut client_ready, client.handshake()),
                    (&mut server_ready, server.handshake()),
                ] {
                    match result {
                        Ok(()) => *ready = true,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => blocked = true,
                        Err(error) => return Err(error),
                    }
                }
                if client_ready && server_ready {
                    break;
                }
            }
            assert!(blocked, "handshake must wait for the peer");
            assert!(client_ready && server_ready, "handshake did not complete");
            assert_eq!(client.negotiated_alpn(), Some(b"h2".as_slice()));
            assert_eq!(server.negotiated_alpn(), Some(b"h2".as_slice()));
            if capacity >= 64 * 1024 {
                client.write_all(b"a")?;
                client.flush()?;
                let mut byte = [0];
                server.read_exact(&mut byte)?;
                assert_eq!(&byte, b"a");
            }
        }
        Ok(())
    }

    fn rejected_client(
        client_config: &TlsClientConfig,
        server_config: rustls::ServerConfig,
        hostname: &'static str,
    ) -> io::Result<io::ErrorKind> {
        let (client_pipe, mut server_pipe) = pipes()?;
        let (release, released) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || -> io::Result<()> {
            let mut connection =
                rustls::ServerConnection::new(Arc::new(server_config)).map_err(io::Error::other)?;
            let result = (|| {
                while connection.is_handshaking() {
                    connection.complete_io(&mut server_pipe)?;
                }
                Ok(())
            })();
            while connection.wants_write() {
                match connection.write_tls(&mut server_pipe) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            released
                .recv_timeout(Duration::from_secs(3))
                .map_err(io::Error::other)?;
            result
        });
        let name = ServerName::try_from(hostname.to_owned()).map_err(io::Error::other)?;
        let mut client =
            TlsTransport::<rustls::ClientConnection, _>::client(client_pipe, client_config, name)?;
        let error = client
            .handshake()
            .err()
            .ok_or_else(|| io::Error::other("invalid TLS peer accepted"))?;
        assert_eq!(
            client.handshake().err().map(|e| e.kind()),
            Some(error.kind())
        );
        assert_eq!(
            client.read(&mut [0]).err().map(|e| e.kind()),
            Some(error.kind())
        );
        assert_eq!(
            client.write(b"x").err().map(|e| e.kind()),
            Some(error.kind())
        );
        let _release_result = release.send(());
        drop(client);
        // The raw peer may observe the client's certificate or ALPN alert.
        let _peer_result = joined(worker);
        Ok(error.kind())
    }

    #[test]
    fn rejects_missing_and_mismatched_alpn() -> io::Result<()> {
        let (client_config, server_config) = configs()?;
        for protocols in [Vec::new(), vec![b"http/1.1".to_vec()]] {
            let mut raw_server = server_config.get_ref().clone();
            raw_server.alpn_protocols = protocols;
            assert_eq!(
                rejected_client(&client_config, raw_server, "localhost")?,
                io::ErrorKind::InvalidData,
            );
        }
        Ok(())
    }

    #[test]
    fn rejects_untrusted_certificate_and_wrong_hostname() -> io::Result<()> {
        let (client_config, server_config) = configs()?;
        let (untrusted, _) = configs()?;
        for (config, name) in [(&untrusted, "localhost"), (&client_config, "wrong.example")] {
            assert_eq!(
                rejected_client(config, server_config.get_ref().clone(), name)?,
                io::ErrorKind::InvalidData,
            );
        }
        Ok(())
    }

    #[test]
    fn transport_settings_are_delegated() -> io::Result<()> {
        let (client_config, _) = configs()?;
        let (client_pipe, _peer) = pipes()?;
        let mut client = TlsTransport::<rustls::ClientConnection, _>::client(
            client_pipe,
            &client_config,
            localhost()?,
        )?;
        assert!(client.set_read_timeout(Some(Duration::ZERO)).is_err());
        assert!(client.set_write_timeout(Some(Duration::ZERO)).is_err());
        client.set_read_timeout(Some(Duration::from_millis(5)))?;
        client.set_write_timeout(Some(Duration::from_millis(5)))?;
        assert_eq!(
            client.handshake().err().map(|e| e.kind()),
            Some(io::ErrorKind::TimedOut)
        );
        client.set_nonblocking(true)?;
        assert_eq!(
            client.handshake().err().map(|e| e.kind()),
            Some(io::ErrorKind::WouldBlock)
        );
        assert_eq!(client.peer_endpoint()?, "memory://peer");
        Ok(())
    }
    #[test]
    fn server_rejects_client_without_alpn() -> io::Result<()> {
        let (client_config, server_config) = configs()?;
        let (mut client_pipe, server_pipe) = pipes()?;
        let worker = thread::spawn(move || -> io::Result<io::ErrorKind> {
            let mut server =
                TlsTransport::<rustls::ServerConnection, _>::server(server_pipe, &server_config)?;
            let kind = server
                .handshake()
                .err()
                .map(|error| error.kind())
                .ok_or_else(|| io::Error::other("server accepted missing ALPN"))?;
            assert_eq!(server.handshake().err().map(|e| e.kind()), Some(kind));
            assert_eq!(server.read(&mut [0]).err().map(|e| e.kind()), Some(kind));
            assert_eq!(server.write(b"x").err().map(|e| e.kind()), Some(kind));
            Ok(kind)
        });
        let mut raw_config = client_config.get_ref().clone();
        raw_config.alpn_protocols.clear();
        let mut client = rustls::ClientConnection::new(Arc::new(raw_config), localhost()?)
            .map_err(io::Error::other)?;
        while client.is_handshaking() {
            if client.complete_io(&mut client_pipe).is_err() {
                break;
            }
        }
        assert_eq!(joined(worker)?, io::ErrorKind::InvalidData);
        drop(client_pipe);
        Ok(())
    }

    #[test]
    fn clean_shutdown_reports_eof_and_truncation_is_rejected() -> io::Result<()> {
        for clean in [false, true] {
            let (client_config, server_config) = configs()?;
            let (client_pipe, server_pipe) = pipes()?;
            let worker = thread::spawn(move || -> io::Result<()> {
                let mut server = TlsTransport::<rustls::ServerConnection, _>::server(
                    server_pipe,
                    &server_config,
                )?;
                server.handshake()?;
                server.write_all(b"x")?;
                server.flush()?;
                if clean {
                    server.close_notify()?;
                    assert_eq!(
                        server.write(b"after close").err().map(|e| e.kind()),
                        Some(io::ErrorKind::BrokenPipe)
                    );
                }
                Ok(())
            });
            let mut client = TlsTransport::<rustls::ClientConnection, _>::client(
                client_pipe,
                &client_config,
                localhost()?,
            )?;
            client.handshake()?;
            let mut byte = [0];
            client.read_exact(&mut byte)?;
            assert_eq!(&byte, b"x");
            joined(worker)?;
            if clean {
                assert_eq!(client.read(&mut byte)?, 0);
            } else {
                assert_eq!(
                    client.read(&mut byte).err().map(|e| e.kind()),
                    Some(io::ErrorKind::UnexpectedEof)
                );
            }
        }
        Ok(())
    }
}
