use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use io::ErrorKind::{BrokenPipe, InvalidInput, TimedOut, WouldBlock};

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
    assert!(MemoryPipe::with_capacity(isize::MAX as usize).is_err());
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
fn memory_blocked_operations_wake_on_peer_progress_and_drop() -> io::Result<()> {
    for case in 0..4 {
        let (mut first, mut second) = MemoryPipe::with_capacity(1)?;
        first.set_read_timeout(Some(Duration::from_secs(5)))?;
        first.set_write_timeout(Some(Duration::from_secs(5)))?;
        let writing = case >= 2;
        let entered = first.wait_flag(writing);
        let done = Arc::new(AtomicBool::new(false));
        let completed = Arc::clone(&done);
        if writing {
            first.write_all(b"a")?;
        }
        let worker = std::thread::spawn(move || {
            let mut byte = [0];
            let result = if writing {
                first.write(b"b")
            } else {
                first.read(&mut byte)
            };
            completed.store(true, Ordering::Release);
            (result, byte)
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !entered.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "worker did not register a wait");
            std::thread::yield_now();
        }
        match case {
            0 => second.write_all(b"b")?,
            1 | 3 => drop(second),
            _ => second.read_exact(&mut [0])?,
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while !done.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let timely = done.load(Ordering::Acquire);
        let (result, byte) = worker
            .join()
            .map_err(|_| io::Error::other("worker panicked"))?;
        assert!(timely, "peer progress must wake before the I/O deadline");
        match case {
            0 => {
                assert_eq!(result?, 1);
                assert_eq!(&byte, b"b");
            }
            1 => assert_eq!(result?, 0),
            2 => assert_eq!(result?, 1),
            _ => assert_eq!(error_kind(result), Some(BrokenPipe)),
        }
    }
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
    assert_eq!(error_kind(client.read(&mut reply)), Some(WouldBlock));
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
        let path =
            std::env::temp_dir().join(format!("grpc-transport-{}-{serial}", std::process::id()));
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
    assert_eq!(error_kind(client.read(&mut [0])), Some(WouldBlock));
    listener.set_nonblocking(true)?;
    assert_eq!(error_kind(listener.accept()), Some(WouldBlock));
    Ok(())
}
#[test]
fn tcp_rejects_unrepresentable_and_zero_buffer_sizes() -> io::Result<()> {
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    for size in [0, i32::MAX as usize + 1, usize::MAX] {
        for send in [false, true] {
            let options = TcpTransportOptions {
                send_buffer_size: send.then_some(size),
                recv_buffer_size: (!send).then_some(size),
                ..TcpTransportOptions::default()
            };
            let result = TcpTransport::connect_with_options(listener.local_addr()?, options);
            assert_eq!(error_kind(result), Some(InvalidInput));
        }
    }
    Ok(())
}
