//! One journalled command: a 64-byte frame whose checksum is chained to the
//! frame before it, so a missing, reordered, or foreign frame breaks the chain.

use std::ptr;

use calvera_books::{Price, Side};

use crate::types::{Command, CommandType};

pub const FRAME: usize = 64;
const MAGIC: u32 = 0x214C_4157; // "WAL!"
/// Frame layout version. Frames with any other version are refused on recovery.
pub const WAL_VERSION: u8 = 2;
/// Chain seed for the first frame of a log.
pub const CHAIN_SEED: u32 = 0x811c_9dc5;

#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct WalFrame {
    pub magic: u32,
    pub checksum: u32,
    pub seq: u64,
    // packed command (not the Rust layout)
    ty: u8,
    side: u8,
    pub version: u8,
    _pad: [u8; 5],
    price: u64,
    quantity: u64,
    client_fd: i32,
    user_ref: u32,
    cl_ord_id: [u8; 14],
    time_in_force: u8,
    display: u8,
}

const _: () = assert!(size_of::<WalFrame>() == FRAME);

/// What a frame read from the log turned out to be.
pub enum Read {
    /// Never written.
    Empty,
    /// Bad magic or a checksum that does not follow `prev`.
    Invalid,
    Valid(WalFrame),
}

impl WalFrame {
    pub fn pack(cmd: &Command, seq: u64, prev: u32) -> Self {
        let mut frame = Self {
            magic: MAGIC,
            checksum: 0,
            seq,
            ty: cmd.ty as u8,
            side: cmd.side as u8,
            version: WAL_VERSION,
            _pad: [0; 5],
            price: cmd.price.0,
            quantity: cmd.quantity,
            client_fd: cmd.client_fd,
            user_ref: cmd.user_ref,
            cl_ord_id: cmd.cl_ord_id,
            time_in_force: cmd.time_in_force,
            display: cmd.display,
        };
        frame.checksum = frame.chained(prev);
        frame
    }

    pub fn command(&self) -> Command {
        Command {
            ty: match self.ty {
                0 => CommandType::Add,
                1 => CommandType::Modify,
                2 => CommandType::Cancel,
                3 => CommandType::Reset,
                5 => CommandType::Entry,
                _ => CommandType::Poison,
            },
            side: if self.side == 0 { Side::Bid } else { Side::Ask },
            price: Price(self.price),
            quantity: self.quantity,
            client_fd: self.client_fd,
            user_ref: self.user_ref,
            cl_ord_id: self.cl_ord_id,
            time_in_force: self.time_in_force,
            display: self.display,
            capacity: 0,
        }
    }

    /// Checksum of this frame's body seeded with the previous frame's checksum.
    pub fn chained(&self, prev: u32) -> u32 {
        // Portable stand-in for `_mm_crc32_u64`. Enough to detect torn writes.
        let bytes = unsafe {
            std::slice::from_raw_parts((self as *const WalFrame as *const u8).add(8), FRAME - 8)
        };
        fnv(prev, bytes)
    }

    /// # Safety
    /// `at` must point at `FRAME` readable bytes.
    pub unsafe fn read(at: *const u8, prev: u32) -> Read {
        let bytes = unsafe { std::slice::from_raw_parts(at, FRAME) };
        if bytes.iter().all(|&b| b == 0) {
            return Read::Empty;
        }
        let frame = unsafe { ptr::read_unaligned(at as *const WalFrame) };
        if frame.magic != MAGIC || frame.chained(prev) != frame.checksum {
            return Read::Invalid;
        }
        Read::Valid(frame)
    }

    /// # Safety
    /// `at` must point at `FRAME` writable bytes.
    #[inline]
    pub unsafe fn write(self, at: *mut u8) {
        unsafe { ptr::write_unaligned(at as *mut WalFrame, self) };
    }
}

pub fn fnv(seed: u32, bytes: &[u8]) -> u32 {
    let mut h = seed;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}
