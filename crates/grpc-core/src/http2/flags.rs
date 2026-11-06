use uarch::prelude::*;

bitword! {
    pub(super) struct FrameFlags(u32) {
        [0] pub(super) END_STREAM,
        [2] pub(super) END_HEADERS,
        [3] pub(super) PADDED,
        [5] pub(super) PRIORITY,
    }
}
// SETTINGS and PING interpret bit zero as ACK, not END_STREAM.
bitword! {
    pub(super) struct SettingsFlags(u32) { [0] pub(super) ACK, }
}

bitword! {
    pub(super) struct HttpStreamWord(u32) {
        [0] pub(super) DEFERRED,
        [1] pub(super) WINDOW_BLOCKED,
        [2] pub(super) INITIAL,
        [3] pub(super) LOCAL_CLOSED,
        [4] pub(super) REMOTE_CLOSED,
        [5] pub(super) CLOSE_EMITTED,
        [6] pub(super) RESET,
    }
}
