use grpc_core::{Metadata, Status};
use uarch::prelude::*;

use super::{Result, StreamDriver, StreamError, invalid};

bitword! {
    struct RpcStreamWord(u32) {
        [1] HALF_CLOSED_LOCAL,
        [2] HALF_CLOSED_REMOTE,
        [5] FINISHED,
        [6] CLIENT,
    }
}
pub(super) struct State<D: StreamDriver> {
    pub(super) driver: D,
    error: Option<StreamError>,
    flags: RpcStreamWord,
}
impl<D: StreamDriver> State<D> {
    pub(super) fn new(driver: D, client: bool, send_closed: bool) -> Self {
        Self {
            driver,
            error: None,
            flags: RpcStreamWord::default().patch(
                BitPatch::EMPTY
                    .with_flag(RpcStreamWord::CLIENT, client)
                    .with_flag(RpcStreamWord::HALF_CLOSED_LOCAL, send_closed),
            ),
        }
    }

    #[cold_sink]
    pub(super) fn fail(&mut self, error: StreamError) -> StreamError {
        self.error = Some(error.clone());
        if !self.flags.matches(pattern!(RpcStreamWord { FINISHED })) {
            self.driver.cancel();
        }
        self.flags = self.flags.patch(patch!(RpcStreamWord { FINISHED: true }));
        error
    }
    pub(super) fn check(&self) -> Result<()> {
        self.error.clone().map_or(Ok(()), Err)
    }
    pub(super) fn accept<T>(&mut self, result: Result<T>) -> Result<T> {
        match result {
            Err(StreamError::WouldBlock) => Err(StreamError::WouldBlock),
            Err(e) => Err(self.fail(e)),
            Ok(value) => Ok(value),
        }
    }
    #[inline]
    pub(super) fn poll(&mut self) -> Result<Option<D::Receive>> {
        self.check()?;
        if self
            .flags
            .matches(pattern!(RpcStreamWord { HALF_CLOSED_REMOTE }))
        {
            return Ok(None);
        }
        let result = self.driver.try_recv_next();
        let message = self.accept(result)?;
        if message.is_none() {
            if self.flags.matches(pattern!(RpcStreamWord { CLIENT }))
                && self.driver.trailing_metadata().is_none()
            {
                return Err(
                    self.fail(Status::unknown("response ended without validated trailers").into())
                );
            }
            self.flags = self.flags.patch(patch!(RpcStreamWord {
                HALF_CLOSED_REMOTE: true
            }));
            if self.flags.matches(pattern!(RpcStreamWord { CLIENT })) {
                if !self
                    .flags
                    .matches(pattern!(RpcStreamWord { HALF_CLOSED_LOCAL }))
                {
                    self.driver.cancel();
                }
                self.flags = self.flags.patch(patch!(RpcStreamWord {
                    FINISHED: true,
                    HALF_CLOSED_LOCAL: true
                }));
            }
        }
        Ok(message)
    }
    #[inline]
    pub(super) fn send(&mut self, value: &D::Send) -> Result<()> {
        self.check()?;
        if self
            .flags
            .matches(pattern!(RpcStreamWord { HALF_CLOSED_LOCAL }))
        {
            return Err(invalid("send direction is closed"));
        }
        let result = self.driver.send(value);
        self.accept(result)
    }
    #[inline]
    pub(super) fn close(&mut self) -> Result<()> {
        self.check()?;
        if self
            .flags
            .matches(pattern!(RpcStreamWord { HALF_CLOSED_LOCAL }))
        {
            return Ok(());
        }
        let result = self.driver.half_close();
        self.accept(result)?;
        self.flags = self.flags.patch(patch!(RpcStreamWord {
            HALF_CLOSED_LOCAL: true
        }));
        Ok(())
    }
    pub(super) fn finish(&mut self, status: &Status, trailers: &Metadata) -> Result<()> {
        self.check()?;
        if self.flags.matches(pattern!(RpcStreamWord { FINISHED })) {
            return Err(invalid("RPC is already finished"));
        }
        let result = self.driver.finish(status, trailers);
        self.accept(result)?;
        self.flags = self.flags.patch(patch!(RpcStreamWord {
            FINISHED: true,
            HALF_CLOSED_LOCAL: true,
            HALF_CLOSED_REMOTE: true
        }));
        Ok(())
    }
    pub(super) fn wait(&mut self) -> Result<()> {
        self.check()?;
        match self.driver.wait_ready() {
            Ok(()) => Ok(()),
            Err(StreamError::WouldBlock) => {
                Err(self.fail(Status::internal("driver wait made no progress").into()))
            }
            Err(e) => Err(self.fail(e)),
        }
    }
    pub(super) fn cancel(&mut self) {
        if !self.flags.matches(pattern!(RpcStreamWord { FINISHED })) {
            self.fail(Status::cancelled("call cancelled").into());
        }
    }
}
impl<D: StreamDriver> Drop for State<D> {
    fn drop(&mut self) {
        if !self.flags.matches(pattern!(RpcStreamWord { FINISHED })) {
            self.driver.cancel();
        }
    }
}
