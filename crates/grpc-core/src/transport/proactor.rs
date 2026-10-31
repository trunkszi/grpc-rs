//! Persistent calling-thread DMA storage and complete operation retirement.

use std::{
    cell::RefCell,
    io,
    os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd, RawFd},
    time::Duration,
};

use liburing::{
    Completion, OperationId, Proactor, ProactorConfig, SubmissionContext, UringError,
    ops::{Timeout, net},
};

thread_local! {
    static DRIVER: RefCell<Option<Driver>> = const { RefCell::new(None) };
}

const IO: OperationId = OperationId::from_raw((1 << 56) | 1);
const TIMER: OperationId = OperationId::from_raw((1 << 56) | 2);
const CANCEL: OperationId = OperationId::from_raw((1 << 56) | 3);
const DMA_CAPACITY: usize = 1024 * 1024;

fn io_error(error: UringError) -> io::Error {
    match error {
        UringError::RingInit(errno)
        | UringError::Submit(errno)
        | UringError::Wait(errno)
        | UringError::Syscall { errno, .. } => io::Error::from_raw_os_error(errno),
        UringError::SqThreadGone => io::Error::new(io::ErrorKind::BrokenPipe, error),
        UringError::OpcodeNotAdmitted { .. }
        | UringError::BlockingWaitUnavailable { .. }
        | UringError::CancellationUnavailable { .. } => {
            io::Error::new(io::ErrorKind::Unsupported, error)
        }
        _ => io::Error::other(error),
    }
}

pub(super) fn is_nonblocking(stream: &impl AsFd) -> io::Result<bool> {
    // SAFETY: The descriptor is borrowed open; F_GETFL has no third operand.
    let flags = unsafe { libc::fcntl(stream.as_fd().as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(flags & libc::O_NONBLOCK != 0)
    }
}

enum Request {
    Receive { len: usize, flags: i32 },
    Send { len: usize, flags: i32 },
    Connect,
    Readable,
    Accept { nonblocking: bool },
}

struct Driver {
    ring: Proactor,
    bytes: Option<Box<[u8]>>,
    address: Option<Box<socket2::SockAddr>>,
    timer: Option<(Duration, Timeout)>,
    active: Option<RawFd>,
}

impl Driver {
    fn new() -> io::Result<Self> {
        Ok(Self {
            ring: Proactor::new(ProactorConfig {
                entries: 8,
                staged_capacity: 8,
                max_cqe_batch: 8,
                ..ProactorConfig::default()
            })
            .map_err(io_error)?,
            bytes: Some(vec![0; DMA_CAPACITY].into_boxed_slice()),
            address: Some(Box::new(
                std::net::SocketAddr::from(([0, 0, 0, 0], 0)).into(),
            )),
            timer: None,
            active: None,
        })
    }

    fn execute(
        &mut self,
        fd: RawFd,
        request: Request,
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        // The immutable upstream Timeout operand is cached across calls with the
        // same setting. A changed deadline replaces it only after full retirement.
        if let Some(duration) = timeout
            && self
                .timer
                .as_ref()
                .is_none_or(|(cached, _)| *cached != duration)
        {
            self.timer = Some((duration, Timeout::new(duration).map_err(io_error)?));
        }
        self.active = Some(fd);
        let pointer = self
            .bytes
            .as_mut()
            .expect("driver owns DMA storage")
            .as_mut_ptr();
        let address = self.address.as_ref().expect("driver owns address storage");
        self.ring
            .prepare_sqe_with(IO, |sqe| {
                // SAFETY: Exclusive SQE; the fd is borrowed through completion and the
                // driver retains stable, exclusive DMA/address storage through the final
                // CQE. Failed shutdown quarantines storage before the borrow can end.
                unsafe {
                    match request {
                        Request::Receive { len, flags } => {
                            net::prep_recv(sqe, fd, pointer, len, flags)
                        }
                        Request::Send { len, flags } => {
                            net::prep_send(sqe, fd, pointer, len, flags)
                        }
                        Request::Connect => {
                            net::prep_connect(sqe, fd, address.as_ptr().cast(), address.len())
                        }
                        Request::Readable => net::prep_poll_add(sqe, fd, libc::POLLIN as u32),
                        Request::Accept { nonblocking } => {
                            net::prep_accept(
                                sqe,
                                fd,
                                std::ptr::null_mut(),
                                std::ptr::null_mut(),
                                libc::SOCK_CLOEXEC,
                            );
                            if nonblocking {
                                // Linux IORING_ACCEPT_DONTWAIT: complete with EAGAIN
                                // rather than internally waiting for a new connection.
                                (*sqe).ioprio |= 1 << 1;
                            }
                        }
                    }
                }
            })
            .map_err(io_error)?;
        if timeout.is_some() {
            let (_, timer) = self
                .timer
                .as_ref()
                .expect("timer configured before submission");
            self.ring
                .prepare_sqe_with(TIMER, |sqe| {
                    // SAFETY: Exclusive SQE and an immutable boxed timespec retained
                    // by this driver through the timer's own final completion.
                    unsafe { timer.prepare(sqe) };
                })
                .map_err(io_error)?;
        }
        let mut io_result = None;
        let mut timer_result = timeout.is_none().then_some(0);
        let mut cancelling = false;
        let mut cancel_done = true;
        while io_result.is_none() || timer_result.is_none() || !cancel_done {
            self.ring
                .wait_completions(
                    1,
                    None,
                    &mut |completion: Completion, _: &mut SubmissionContext<'_>| {
                        match completion.operation_id {
                            IO => io_result = Some(completion.result),
                            TIMER => timer_result = Some(completion.result),
                            CANCEL => {
                                if completion.result < 0
                                    && completion.result != -libc::ENOENT
                                    && completion.result != -libc::EALREADY
                                {
                                    return Err(UringError::syscalls(
                                        "socket cancellation",
                                        completion.result,
                                    ));
                                }
                                cancel_done = true;
                            }
                            _ => {
                                return Err(UringError::InvalidPayload(
                                    completion.operation_id.raw(),
                                ));
                            }
                        }
                        Ok(())
                    },
                )
                .map_err(io_error)?;
            let target = if io_result.is_some() && timer_result.is_none() {
                Some(TIMER)
            } else if io_result.is_none() && timer_result.is_some() {
                Some(IO)
            } else {
                None
            };
            if !cancelling
                && timeout.is_some()
                && let Some(target) = target
            {
                self.ring
                    .prepare_sqe_with(CANCEL, |sqe| {
                        // SAFETY: Exclusive SQE; cancellation references a key rather
                        // than external memory. Target operands stay owned until its CQE.
                        unsafe { net::prep_cancel(sqe, target, 0) };
                    })
                    .map_err(io_error)?;
                cancelling = true;
                cancel_done = false;
            }
        }
        self.active = None;
        let result = io_result.expect("I/O final completion was drained");
        if result == -libc::ECANCELED {
            let timer = timer_result.expect("timer final completion was drained");
            if timer == -libc::ETIME {
                return Err(io::ErrorKind::TimedOut.into());
            }
            if timer < 0 && timer != -libc::ECANCELED {
                return Err(io::Error::from_raw_os_error(-timer));
            }
        }
        if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Ok(result as usize)
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        if let Some(fd) = self.active.take()
            && self.ring.shutdown().is_err()
        {
            // A failed drain does not authorize reuse. Only this exceptional
            // path duplicates the socket; preserve all DMA operands indefinitely.
            // SAFETY: The caller still borrows the original open descriptor.
            let retained = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
            if retained >= 0 {
                // SAFETY: F_DUPFD_CLOEXEC returns a new, uniquely owned descriptor.
                std::mem::forget(unsafe { OwnedFd::from_raw_fd(retained) });
            }
            if let Some(bytes) = self.bytes.take() {
                std::mem::forget(bytes);
            }
            if let Some(address) = self.address.take() {
                std::mem::forget(address);
            }
            if let Some(timer) = self.timer.take() {
                std::mem::forget(timer);
            }
        }
    }
}

fn with_driver<T>(operation: impl FnOnce(&mut Driver) -> io::Result<T>) -> io::Result<T> {
    DRIVER.with(|cell| {
        let mut slot = cell
            .try_borrow_mut()
            .map_err(|_| io::Error::other("recursive proactor I/O"))?;
        if slot.is_none() {
            *slot = Some(Driver::new()?);
        }
        let mut driver = slot.take().expect("driver initialized above");
        let result = operation(&mut driver);
        if driver.active.is_none() {
            *slot = Some(driver);
        } else {
            // Retire or quarantine before releasing the caller's descriptor borrow.
            drop(driver);
        }
        result
    })
}

pub(super) fn read(
    stream: &impl AsFd,
    destination: &mut [u8],
    timeout: Option<Duration>,
    nonblocking: bool,
) -> io::Result<usize> {
    if destination.is_empty() {
        return Ok(0);
    }
    with_driver(|driver| {
        let count = driver.execute(
            stream.as_fd().as_raw_fd(),
            Request::Receive {
                len: destination.len().min(DMA_CAPACITY),
                flags: if nonblocking { libc::MSG_DONTWAIT } else { 0 },
            },
            if nonblocking { None } else { timeout },
        )?;
        destination[..count]
            .copy_from_slice(&driver.bytes.as_ref().expect("retired DMA buffer")[..count]);
        Ok(count)
    })
}

pub(super) fn write(
    stream: &impl AsFd,
    source: &[u8],
    timeout: Option<Duration>,
    nonblocking: bool,
) -> io::Result<usize> {
    if source.is_empty() {
        return Ok(0);
    }
    with_driver(|driver| {
        let len = source.len().min(DMA_CAPACITY);
        driver.bytes.as_mut().expect("retired DMA buffer")[..len].copy_from_slice(&source[..len]);
        driver.execute(
            stream.as_fd().as_raw_fd(),
            Request::Send {
                len,
                flags: libc::MSG_NOSIGNAL | if nonblocking { libc::MSG_DONTWAIT } else { 0 },
            },
            if nonblocking { None } else { timeout },
        )
    })
}

pub(super) fn connect(stream: &impl AsFd, address: socket2::SockAddr) -> io::Result<()> {
    with_driver(|driver| {
        **driver.address.as_mut().expect("retired address storage") = address;
        driver
            .execute(stream.as_fd().as_raw_fd(), Request::Connect, None)
            .map(|_| ())
    })
}

pub(super) fn readable(stream: &impl AsFd, timeout: Option<Duration>) -> io::Result<()> {
    with_driver(|driver| {
        driver
            .execute(stream.as_fd().as_raw_fd(), Request::Readable, timeout)
            .map(|_| ())
    })
}

pub(super) fn accept(listener: &impl AsFd) -> io::Result<OwnedFd> {
    let nonblocking = is_nonblocking(listener)?;
    with_driver(|driver| {
        let fd = match driver.execute(
            listener.as_fd().as_raw_fd(),
            Request::Accept { nonblocking },
            None,
        ) {
            Err(error)
                if nonblocking
                    && matches!(error.raw_os_error(), Some(libc::EINVAL | libc::EOPNOTSUPP)) =>
            {
                // Older kernels lack the immediate-accept flag. A minimal kernel
                // timeout bounds the same accept SQE, with full target retirement.
                bounded_accept(driver, listener.as_fd().as_raw_fd())?
            }
            result => result?,
        };
        // SAFETY: A successful accept CQE returns a new uniquely owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
    })
}

fn bounded_accept(driver: &mut Driver, listener: RawFd) -> io::Result<usize> {
    match driver.execute(
        listener,
        Request::Accept { nonblocking: false },
        Some(Duration::from_nanos(1)),
    ) {
        Err(error) if error.kind() == io::ErrorKind::TimedOut => {
            Err(io::ErrorKind::WouldBlock.into())
        }
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_errors_map_io_kinds() {
        assert_eq!(
            io_error(UringError::SqThreadGone).kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            io_error(UringError::OpcodeNotAdmitted {
                opcode: 0,
                mode: "iopoll",
            })
            .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            io_error(UringError::BlockingWaitUnavailable { mode: "sqpoll" }).kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            io_error(UringError::CancellationUnavailable { mode: "sqpoll" }).kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            io_error(UringError::RingInit(libc::EINVAL)).raw_os_error(),
            Some(libc::EINVAL)
        );
        assert_eq!(
            io_error(UringError::CompletionsDropped).kind(),
            io::ErrorKind::Other
        );
    }

    #[test]
    fn integration_scenario_legacy_immediate_accept_cancels_and_reuses_ring() -> io::Result<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let mut driver = Driver::new()?;
        let error = bounded_accept(&mut driver, listener.as_raw_fd()).expect_err("empty queue");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        let client = std::net::TcpStream::connect(listener.local_addr()?)?;
        let fd = bounded_accept(&mut driver, listener.as_raw_fd())?;
        // SAFETY: The successful accept completion returns one uniquely owned fd.
        let server = unsafe { std::net::TcpStream::from_raw_fd(fd as RawFd) };
        assert_eq!(server.peer_addr()?, client.local_addr()?);
        Ok(())
    }
}
