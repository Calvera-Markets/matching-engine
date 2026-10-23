//! Segment header: written once, when the engine makes the segment active.
//! It names the segment's place in the log and links it to the segment
//! before it through that segment's last frame checksum.

use std::ptr;

use super::frame::{CHAIN_SEED, fnv};

pub const HEADER: usize = 64;
const MAGIC: u32 = 0x2147_4553; // "SEG!"
/// Header layout version.
pub const SEGMENT_VERSION: u8 = 1;

#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    magic: u32,
    checksum: u32,
    version: u8,
    _pad: [u8; 7],
    pub epoch: u64,
    pub index: u64,
    pub first_seq: u64,
    /// Checksum of the last frame before this segment (`CHAIN_SEED` for the first).
    pub prev: u32,
    _pad2: [u8; 20],
}

const _: () = assert!(size_of::<SegmentHeader>() == HEADER);

/// What the header slot of a segment file holds.
pub enum Slot {
    /// Never activated: a prepared spare.
    Blank,
    Invalid,
    /// Valid layout but a version this build does not read.
    Version(u8),
    Active(SegmentHeader),
}

impl SegmentHeader {
    pub fn new(epoch: u64, index: u64, first_seq: u64, prev: u32) -> Self {
        let mut h = Self {
            magic: MAGIC,
            checksum: 0,
            version: SEGMENT_VERSION,
            _pad: [0; 7],
            epoch,
            index,
            first_seq,
            prev,
            _pad2: [0; 20],
        };
        h.checksum = h.sum();
        h
    }

    fn sum(&self) -> u32 {
        let bytes = unsafe {
            std::slice::from_raw_parts((self as *const Self as *const u8).add(8), HEADER - 8)
        };
        fnv(CHAIN_SEED, bytes)
    }

    /// # Safety
    /// `at` must point at `HEADER` readable bytes.
    pub unsafe fn read(at: *const u8) -> Slot {
        let bytes = unsafe { std::slice::from_raw_parts(at, HEADER) };
        if bytes.iter().all(|&b| b == 0) {
            return Slot::Blank;
        }
        let h = unsafe { ptr::read_unaligned(at as *const Self) };
        if h.magic != MAGIC || h.sum() != h.checksum {
            return Slot::Invalid;
        }
        if h.version != SEGMENT_VERSION {
            return Slot::Version(h.version);
        }
        Slot::Active(h)
    }

    /// # Safety
    /// `at` must point at `HEADER` writable bytes.
    pub unsafe fn write(self, at: *mut u8) {
        unsafe { ptr::write_unaligned(at as *mut Self, self) };
    }
}
