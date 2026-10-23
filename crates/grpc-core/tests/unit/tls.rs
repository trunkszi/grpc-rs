use std::thread::{self, JoinHandle};

use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};

use super::*;
use crate::transport::MemoryPipe;

fn configs() -> io::Result<(TlsClientConfig, TlsServerConfig)> {
    let identity =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).map_err(io::Error::other)?;
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
    let (mut release, mut released) = MemoryPipe::pair();
    released.set_read_timeout(Some(Duration::from_secs(3)))?;
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
        released.read_exact(&mut [0])?;
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
    let _release_result = release.write_all(&[1]);
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
            let mut server =
                TlsTransport::<rustls::ServerConnection, _>::server(server_pipe, &server_config)?;
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

#[test]
fn nonblocking_partial_records_and_close_notify_resume() -> io::Result<()> {
    let (client_config, server_config) = configs()?;
    let (client_pipe, server_pipe) = MemoryPipe::with_capacity(32)?;
    let mut client = TlsTransport::<rustls::ClientConnection, _>::client(
        client_pipe,
        &client_config,
        localhost()?,
    )?;
    let mut server =
        TlsTransport::<rustls::ServerConnection, _>::server(server_pipe, &server_config)?;
    client.set_nonblocking(true)?;
    server.set_nonblocking(true)?;
    for _ in 0..10000 {
        for result in [client.handshake(), server.handshake()] {
            if let Err(error) = result
                && error.kind() != io::ErrorKind::WouldBlock
            {
                return Err(error);
            }
        }
        if client.negotiated_alpn().is_some() && server.negotiated_alpn().is_some() {
            break;
        }
    }
    assert!(client.negotiated_alpn().is_some() && server.negotiated_alpn().is_some());
    let (request, response) = ([b'a'; 4096], [b'b'; 4096]);
    let (mut written, mut received) = ([0; 2], [Vec::new(), Vec::new()]);
    let mut blocked = false;
    for _ in 0..10000 {
        for (index, transport, message) in [
            (0, &mut client as &mut dyn SyncTransport, &request),
            (1, &mut server as &mut dyn SyncTransport, &response),
        ] {
            if written[index] < message.len() {
                match transport.write(&message[written[index]..]) {
                    Ok(count) => written[index] += count,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => blocked = true,
                    Err(error) => return Err(error),
                }
            }
            if let Err(error) = transport.flush() {
                if error.kind() != io::ErrorKind::WouldBlock {
                    return Err(error);
                }
                blocked = true;
            }
            let mut buffer = [0; 97];
            match transport.read(&mut buffer) {
                Ok(count) => received[index].extend_from_slice(&buffer[..count]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        if received[0].len() == response.len() && received[1].len() == request.len() {
            break;
        }
    }
    assert!(
        blocked,
        "capacity must exercise partial encrypted record writes"
    );
    assert_eq!(received[0], response);
    assert_eq!(received[1], request);
    // Queue more than the wire capacity, then close with encrypted data pending.
    client.write_all(b"abcdefghijklmnopqrstuvwxyz0123456789")?;
    assert_eq!(
        client.close_notify().err().map(|e| e.kind()),
        Some(io::ErrorKind::WouldBlock)
    );
    let mut tail = Vec::new();
    let mut eof = false;
    for _ in 0..10000 {
        if let Err(error) = client.close_notify()
            && error.kind() != io::ErrorKind::WouldBlock
        {
            return Err(error);
        }
        let mut buffer = [0; 97];
        match server.read(&mut buffer) {
            Ok(0) => {
                eof = true;
                break;
            }
            Ok(count) => tail.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
    }
    assert!(eof, "close notification must eventually produce clean EOF");
    assert_eq!(tail, b"abcdefghijklmnopqrstuvwxyz0123456789");
    assert_eq!(
        client.write(b"x").err().map(|e| e.kind()),
        Some(io::ErrorKind::BrokenPipe)
    );
    server.write_all(b"ack")?;
    let mut reply = Vec::new();
    for _ in 0..1000 {
        if let Err(error) = server.flush()
            && error.kind() != io::ErrorKind::WouldBlock
        {
            return Err(error);
        }
        let mut buffer = [0; 3];
        match client.read(&mut buffer) {
            Ok(count) => reply.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        if reply.len() == 3 {
            break;
        }
    }
    assert_eq!(
        reply, b"ack",
        "write-close must preserve the read direction"
    );
    Ok(())
}

struct DeadlineProbe {
    input: io::Cursor<Vec<u8>>,
    read_limit: usize,
    write_limit: usize,
    read_delay: Duration,
    write_delay: Duration,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    reads: Vec<Duration>,
    writes: Vec<Duration>,
    reject_write_setting: bool,
    reject_read_restore: bool,
}
impl DeadlineProbe {
    fn new(input: Vec<u8>) -> Self {
        Self {
            input: io::Cursor::new(input),
            read_limit: usize::MAX,
            write_limit: usize::MAX,
            read_delay: Duration::ZERO,
            write_delay: Duration::ZERO,
            read_timeout: Some(Duration::from_secs(3)),
            write_timeout: Some(Duration::from_secs(7)),
            reads: Vec::new(),
            writes: Vec::new(),
            reject_write_setting: false,
            reject_read_restore: false,
        }
    }
    fn delay(duration: Duration) {
        let end = Instant::now() + duration;
        while Instant::now() < end {
            std::hint::spin_loop();
        }
    }
}
impl Read for DeadlineProbe {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if let Some(timeout) = self.read_timeout {
            self.reads.push(timeout);
        }
        Self::delay(self.read_delay);
        let length = bytes.len().min(self.read_limit);
        self.input.read(&mut bytes[..length])
    }
}
impl Write for DeadlineProbe {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(timeout) = self.write_timeout {
            self.writes.push(timeout);
        }
        Self::delay(self.write_delay);
        Ok(bytes.len().min(self.write_limit))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl SyncTransport for DeadlineProbe {
    fn set_nonblocking(&mut self, _: bool) -> io::Result<()> {
        Ok(())
    }
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        if self.reject_read_restore && timeout == Some(Duration::from_secs(3)) {
            self.reject_read_restore = false;
            return Err(io::Error::other("read timeout restoration failed"));
        }
        self.read_timeout = timeout;
        Ok(())
    }
    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.write_timeout = timeout;
        if std::mem::take(&mut self.reject_write_setting) {
            return Err(io::Error::other("write timeout setup failed"));
        }
        Ok(())
    }
    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.read_timeout)
    }
    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.write_timeout)
    }
    fn peer_endpoint(&self) -> io::Result<String> {
        Ok("test://tls-peer".into())
    }
}
fn assert_original_timeouts<T: SyncTransport>(transport: &T) -> io::Result<()> {
    assert_eq!(transport.read_timeout()?, Some(Duration::from_secs(3)));
    assert_eq!(transport.write_timeout()?, Some(Duration::from_secs(7)));
    Ok(())
}

#[test]
fn unit_scenario_expired_handshake_deadline_performs_no_io() -> io::Result<()> {
    let (config, _) = configs()?;
    let mut client = TlsTransport::client(DeadlineProbe::new(Vec::new()), &config, localhost()?)?;
    let error = client
        .handshake_until(Some(Instant::now()))
        .expect_err("expired deadline");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(error.to_string(), "tls handshake timed out");
    assert!(client.get_ref().reads.is_empty() && client.get_ref().writes.is_empty());
    assert_original_timeouts(client.get_ref())
}

#[test]
fn unit_scenario_trickled_tls_reads_share_one_deadline() -> io::Result<()> {
    let (config, _) = configs()?;
    // A partial record remains incomplete while the peer supplies one byte per read.
    let mut record = vec![22, 3, 3, 0x40, 0];
    record.resize(16_389, 0);
    let mut probe = DeadlineProbe::new(record);
    probe.read_limit = 1;
    probe.read_delay = Duration::from_millis(20);
    let mut client = TlsTransport::client(probe, &config, localhost()?)?;
    let error = client
        .handshake_until(Some(Instant::now() + Duration::from_millis(100)))
        .expect_err("shared read budget");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(error.to_string(), "tls handshake timed out");
    assert!(client.get_ref().reads.len() >= 2);
    assert!(
        client
            .get_ref()
            .reads
            .windows(2)
            .all(|pair| pair[1] < pair[0])
    );
    assert_original_timeouts(client.get_ref())
}

#[test]
fn unit_scenario_partial_tls_writes_share_one_deadline() -> io::Result<()> {
    let (client_config, server_config) = configs()?;
    let mut client = rustls::ClientConnection::new(Arc::clone(&client_config.config), localhost()?)
        .map_err(io::Error::other)?;
    let mut hello = Vec::new();
    client.write_tls(&mut hello)?;
    let mut probe = DeadlineProbe::new(hello);
    probe.write_limit = 1;
    probe.write_delay = Duration::from_millis(20);
    let mut server = TlsTransport::server(probe, &server_config)?;
    let error = server
        .handshake_until(Some(Instant::now() + Duration::from_millis(100)))
        .expect_err("shared write budget");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(error.to_string(), "tls handshake timed out");
    assert!(server.get_ref().writes.len() >= 2);
    assert!(
        server
            .get_ref()
            .writes
            .windows(2)
            .all(|pair| pair[1] < pair[0])
    );
    assert_original_timeouts(server.get_ref())
}

#[test]
fn unit_scenario_failed_tls_or_timeout_setup_restores_both_options() -> io::Result<()> {
    let (config, _) = configs()?;
    for reject_setting in [false, true] {
        let mut probe = DeadlineProbe::new(b"invalid TLS record".to_vec());
        probe.reject_write_setting = reject_setting;
        let mut client = TlsTransport::client(probe, &config, localhost()?)?;
        let error = client
            .handshake_until(Some(Instant::now() + Duration::from_secs(1)))
            .expect_err("negotiation fails");
        assert_eq!(
            error.kind(),
            if reject_setting {
                io::ErrorKind::Other
            } else {
                io::ErrorKind::InvalidData
            }
        );
        assert_original_timeouts(client.get_ref())?;
    }
    Ok(())
}

#[test]
fn integration_lifecycle_deadline_handshake_restores_preexisting_socket_options() -> io::Result<()>
{
    let (client_config, server_config) = configs()?;
    let (mut client_pipe, mut server_pipe) = pipes()?;
    client_pipe.set_read_timeout(None)?;
    client_pipe.set_write_timeout(Some(Duration::from_secs(7)))?;
    server_pipe.set_read_timeout(Some(Duration::from_secs(3)))?;
    server_pipe.set_write_timeout(Some(Duration::from_secs(7)))?;
    let worker = thread::spawn(move || -> io::Result<()> {
        let mut server = TlsTransport::server(server_pipe, &server_config)?;
        server.handshake_until(Some(Instant::now() + Duration::from_secs(1)))?;
        assert_original_timeouts(server.get_ref())?;
        server.write_all(b"ready")?;
        server.flush()?;
        Ok(())
    });
    let mut client = TlsTransport::client(client_pipe, &client_config, localhost()?)?;
    client.handshake_until(Some(Instant::now() + Duration::from_secs(1)))?;
    assert_eq!(client.read_timeout()?, None);
    assert_eq!(client.write_timeout()?, Some(Duration::from_secs(7)));
    let mut bytes = [0; 5];
    client.read_exact(&mut bytes)?;
    assert_eq!(&bytes, b"ready");
    joined(worker)
}

#[test]
fn integration_scenario_deadline_wait_restores_timeouts_after_kernel_timeout() -> io::Result<()> {
    let (config, _) = configs()?;
    let (mut pipe, _silent_peer) = pipes()?;
    pipe.set_write_timeout(Some(Duration::from_secs(7)))?;
    let mut client = TlsTransport::client(pipe, &config, localhost()?)?;
    let error = client
        .handshake_until(Some(Instant::now() + Duration::from_millis(10)))
        .expect_err("silent peer reaches deadline");
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(error.to_string(), "tls handshake timed out");
    assert_original_timeouts(client.get_ref())
}

#[test]
fn integration_lifecycle_deadline_handshake_resumes_after_would_block() -> io::Result<()> {
    let (client_config, server_config) = configs()?;
    let (mut first, mut second) = pipes()?;
    for pipe in [&mut first, &mut second] {
        pipe.set_nonblocking(true)?;
        pipe.set_write_timeout(Some(Duration::from_secs(7)))?;
    }
    let mut client = TlsTransport::client(first, &client_config, localhost()?)?;
    let mut server = TlsTransport::server(second, &server_config)?;
    let deadline = Some(Instant::now() + Duration::from_secs(1));
    let mut blocked = false;
    for _ in 0..1000 {
        for result in [
            client.handshake_until(deadline),
            server.handshake_until(deadline),
        ] {
            match result {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => blocked = true,
                Err(error) => return Err(error),
            }
        }
        assert_original_timeouts(client.get_ref())?;
        assert_original_timeouts(server.get_ref())?;
        if client.negotiated_alpn().is_some() && server.negotiated_alpn().is_some() {
            assert!(blocked);
            return Ok(());
        }
    }
    Err(io::Error::other(
        "nonblocking deadline handshake did not finish",
    ))
}

#[test]
fn unit_scenario_restoration_failure_attempts_both_options_and_fails_closed() -> io::Result<()> {
    let (config, _) = configs()?;
    let mut probe = DeadlineProbe::new(b"invalid TLS record".to_vec());
    probe.reject_read_restore = true;
    let mut client = TlsTransport::client(probe, &config, localhost()?)?;
    let error = client
        .handshake_until(Some(Instant::now() + Duration::from_secs(1)))
        .expect_err("restoration failed");
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(
        error
            .to_string()
            .contains("read timeout restoration failed")
    );
    assert_eq!(client.write_timeout()?, Some(Duration::from_secs(7)));
    let reads = client.get_ref().reads.len();
    assert_eq!(
        client
            .handshake()
            .expect_err("restoration failure disables I/O")
            .kind(),
        io::ErrorKind::Other
    );
    assert_eq!(client.get_ref().reads.len(), reads);
    Ok(())
}
