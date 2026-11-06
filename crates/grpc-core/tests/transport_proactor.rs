//! Real-kernel socket integration, cancellation, and thread-handoff regressions.

use std::{
    io::{self, Read, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
    thread,
    time::{Duration, Instant},
};

use grpc_core::transport::{SyncTransport, TcpTransport, TcpTransportListener, UnixTransport};

fn assert_send<T: Send + SyncTransport>() {}

#[test]
fn integration_lifecycle_tcp_accept_and_completed_io_move_between_threads() -> anyhow::Result<()> {
    assert_send::<TcpTransport>();
    assert_send::<UnixTransport>();
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let mut client = TcpTransport::connect(listener.local_addr()?)?;
    let (mut server, _) = listener.accept()?;
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    server.set_read_timeout(Some(Duration::from_secs(5)))?;
    client.write_all(b"a")?;
    server.read_exact(&mut [0])?;
    let worker = thread::spawn(move || -> io::Result<TcpTransport> {
        server.write_all(b"b")?;
        let mut byte = [0];
        server.read_exact(&mut byte)?;
        assert_eq!(&byte, b"c");
        Ok(server)
    });
    let mut byte = [0];
    client.read_exact(&mut byte)?;
    assert_eq!(&byte, b"b");
    client.write_all(b"c")?;
    let mut server = worker.join().expect("worker completes")?;
    server.write_all(b"d")?;
    client.read_exact(&mut byte)?;
    assert_eq!(&byte, b"d");
    server.get_ref().shutdown(Shutdown::Write)?;
    assert_eq!(client.read(&mut byte)?, 0);
    Ok(())
}

fn timeout_and_reuse<T: SyncTransport>(mut first: T, mut peer: T) -> anyhow::Result<()> {
    first.set_read_timeout(Some(Duration::from_millis(20)))?;
    let mut byte = [0x7f];
    for _ in 0..3 {
        let started = Instant::now();
        assert_eq!(
            first.read(&mut byte).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(byte, [0x7f]);
        peer.write_all(b"x")?;
        first.read_exact(&mut byte)?;
        assert_eq!(&byte, b"x");
        byte = [0x7f];
    }
    first.set_nonblocking(true)?;
    assert_eq!(
        first.read(&mut byte).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    first.set_nonblocking(false)?;
    first.set_read_timeout(None)?;
    peer.write_all(b"y")?;
    first.read_exact(&mut byte)?;
    assert_eq!(&byte, b"y");
    assert!(first.set_read_timeout(Some(Duration::ZERO)).is_err());
    assert!(first.set_write_timeout(Some(Duration::ZERO)).is_err());
    drop(peer);
    assert_eq!(first.read(&mut byte)?, 0);
    Ok(())
}

#[test]
fn integration_scenario_tcp_timeout_cancellation_and_reuse() -> anyhow::Result<()> {
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let client = TcpTransport::connect(listener.local_addr()?)?;
    let (server, _) = listener.accept()?;
    timeout_and_reuse(client, server)
}

#[test]
fn integration_scenario_unix_timeout_cancellation_and_reuse() -> anyhow::Result<()> {
    let (first, second) = UnixStream::pair()?;
    timeout_and_reuse(
        UnixTransport::from_std(first)?,
        UnixTransport::from_std(second)?,
    )
}

#[test]
fn integration_scenario_write_backpressure_times_out_without_poisoning_ring() -> anyhow::Result<()>
{
    let (first, mut second) = UnixStream::pair()?;
    socket2::SockRef::from(&first).set_send_buffer_size(4096)?;
    let mut transport = UnixTransport::from_std(first)?;
    transport.set_write_timeout(Some(Duration::from_millis(20)))?;
    // Fill the socket deterministically without consuming data at the peer.
    transport.set_nonblocking(true)?;
    let bytes = [7; 8192];
    loop {
        match transport.write(&bytes) {
            Ok(count) => assert!(count > 0),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => return Err(error.into()),
        }
    }
    transport.set_nonblocking(false)?;
    assert_eq!(
        transport.write(&bytes).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    second.set_nonblocking(true)?;
    let mut received = 0;
    loop {
        match second.read(&mut [0; 8192]) {
            Ok(count) => received += count,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => return Err(error.into()),
        }
    }
    assert!(received > 0);
    transport.write_all(b"ok")?;
    let mut reply = [0; 2];
    second.read_exact(&mut reply)?;
    assert_eq!(&reply, b"ok");
    second.shutdown(Shutdown::Read)?;
    assert_eq!(
        transport.write(b"closed").unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    Ok(())
}

#[test]
fn integration_lifecycle_unix_handoff_observes_borrowed_stream_settings() -> anyhow::Result<()> {
    let (first, mut peer) = UnixStream::pair()?;
    let transport = UnixTransport::from_std(first)?;
    transport.get_ref().set_nonblocking(true)?;
    let transport = thread::spawn(move || -> io::Result<UnixTransport> {
        let mut transport = transport;
        assert_eq!(
            transport.read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        transport.get_ref().set_nonblocking(false)?;
        transport
            .get_ref()
            .set_read_timeout(Some(Duration::from_millis(20)))?;
        assert_eq!(
            transport.read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        transport.write_all(b"ok")?;
        Ok(transport)
    })
    .join()
    .expect("worker completes")?;
    let mut reply = [0; 2];
    peer.read_exact(&mut reply)?;
    assert_eq!(&reply, b"ok");
    transport.into_inner().write_all(b"std")?;
    let mut reply = [0; 3];
    peer.read_exact(&mut reply)?;
    assert_eq!(&reply, b"std");
    Ok(())
}
