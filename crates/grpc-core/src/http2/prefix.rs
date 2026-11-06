//! Native register descriptors; the HTTP/2 wire prefix remains nine bytes.
use uarch::prelude::*;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(super) struct StreamId(pub(super) u32);
// SAFETY: Transparent initialized u32 storage has no padding.
unsafe impl uarch::layout::NoPadding for StreamId {}
// SAFETY: Transparent u32 storage contributes exactly one native scalar.
unsafe impl uarch::layout::RegisterLeaf for StreamId {
    const LEAVES: u8 = 1;
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(super) struct WindowDelta(pub(super) u32);
// SAFETY: Transparent initialized u32 storage has no padding.
unsafe impl uarch::layout::NoPadding for WindowDelta {}
// SAFETY: Transparent u32 storage contributes exactly one native scalar.
unsafe impl uarch::layout::RegisterLeaf for WindowDelta {
    const LEAVES: u8 = 1;
}

bitword! {
    struct PrefixWord(u64) {
        [0..=23] LENGTH: u32,
        [24..=31] KIND: u8,
        [32..=39] FLAGS: u8,
    }
}
#[register_pass]
#[derive(Clone, Copy)]
pub(super) struct FramePrefix {
    control: PrefixWord,
    stream: u64,
}
impl FramePrefix {
    #[hot_inline]
    pub(super) fn new(length: u32, kind: u8, flags: u8, id: StreamId) -> Self {
        Self {
            control: PrefixWord::default()
                .patch(PrefixWord::LENGTH.patch(length & 0x00ff_ffff))
                .patch(PrefixWord::KIND.patch(kind))
                .patch(PrefixWord::FLAGS.patch(flags)),
            stream: u64::from(id.0),
        }
    }
    #[hot_inline]
    pub(super) fn parse(raw: &[u8; 9]) -> Self {
        Self::new(
            u32::from_be_bytes([0, raw[0], raw[1], raw[2]]),
            raw[3],
            raw[4],
            StreamId(u32::from_be_bytes([raw[5], raw[6], raw[7], raw[8]]) & 0x7fff_ffff),
        )
    }
    #[hot_inline]
    pub(super) fn wire(self) -> [u8; 9] {
        let length = self.length().to_be_bytes();
        let id = (self.stream as u32).to_be_bytes();
        [
            length[1],
            length[2],
            length[3],
            self.kind(),
            self.flags(),
            id[0],
            id[1],
            id[2],
            id[3],
        ]
    }
    pub(super) fn length(self) -> u32 {
        self.control.get(PrefixWord::LENGTH)
    }
    pub(super) fn kind(self) -> u8 {
        self.control.get(PrefixWord::KIND)
    }
    pub(super) fn flags(self) -> u8 {
        self.control.get(PrefixWord::FLAGS)
    }
    pub(super) fn stream(self) -> i32 {
        self.stream as i32
    }
}
uarch::layout::assert_layout!(FramePrefix, size = 16, align = 8);
uarch::layout::assert_layout!(StreamId, size = 4, align = 4);
uarch::layout::assert_layout!(WindowDelta, size = 4, align = 4);
