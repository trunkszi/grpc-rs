use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use uarch::prelude::*;

use crate::http2::{DataPoll, DataSource};

bitword! {
    struct OutboundState(u32) {
        [0] pub EOF,
        [1] pub FINISHED,
        [2] pub TRAILERS,
    }
}
#[derive(Default)]
pub(super) struct Outbound {
    pub(super) bytes: VecDeque<u8>,
    state: OutboundState,
}
impl Outbound {
    #[hot_inline]
    pub(super) fn mark_eof(&mut self) {
        self.state = self.state.patch(patch!(OutboundState { EOF: true }));
    }
    pub(super) fn mark_trailers(&mut self) {
        self.state = self.state.patch(patch!(OutboundState {
            EOF: true,
            TRAILERS: true
        }));
    }
    pub(super) fn finished(&self) -> bool {
        self.state.matches(pattern!(OutboundState { FINISHED }))
    }
}
pub(super) struct Source {
    pub(super) queue: Rc<RefCell<Outbound>>,
    pub(super) chunk: Vec<u8>,
}
impl DataSource for Source {
    #[inline]
    fn read(&mut self, max_len: usize) -> DataPoll<'_> {
        let mut queue = self.queue.borrow_mut();
        self.chunk.clear();
        if !queue.bytes.is_empty() {
            let count = max_len.min(queue.bytes.len());
            self.chunk.extend(queue.bytes.drain(..count));
            return DataPoll::Copy(&self.chunk);
        }
        if queue.state.matches(pattern!(OutboundState { EOF })) {
            queue.state = queue.state.patch(patch!(OutboundState { FINISHED: true }));
            if queue.state.matches(pattern!(OutboundState { TRAILERS })) {
                DataPoll::EofWithTrailers
            } else {
                DataPoll::Eof
            }
        } else {
            DataPoll::Deferred
        }
    }
}
