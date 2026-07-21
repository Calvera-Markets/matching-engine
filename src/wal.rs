//! 2 GB mmap WAL. Write `Command` before apply; recover by replay.

use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::ptr;

use crate::types::{Command, CommandType};
use calvera_books::{Price, Side};

const WAL_MAGIC: u32 = 0x214C_4157; // "WAL!"
pub const WAL_SIZE: usize = 2 * 1024 * 1024 * 1024;
const FRAME: usize = 64;

#[repr(C, align(64))]
struct WalFrame {
    magic: u32,
    checksum: u32,
    seq: u64,
    // packed command (not the Rust layout)
    ty: u8,
    side: u8,
    _pad: [u8; 6],
    price: u64,
    quantity: u64,
    client_fd: i32,
    user_ref: u32,
    cl_ord_id: [u8; 14],
    time_in_force: u8,
    display: u8,
}

const _: () = assert!(size_of::<WalFrame>() == FRAME);

pub struct Wal {
    ptr: *mut u8,
    len: usize,
    seq: u64,
    offset: usize,
}

unsafe impl Send for Wal {}

impl Wal {
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_sized(path, WAL_SIZE)
    }

    pub fn open_sized(path: &Path, size: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.set_len(size as u64)?;

        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                use_raw_fd(&file),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            ptr: ptr as *mut u8,
            len: size,
            seq: 0,
            offset: 0,
        })
    }

    pub fn bytes_written(&self) -> usize {
        self.offset
    }

    pub fn write(&mut self, cmd: &Command) {
        if self.offset + FRAME > self.len {
            return;
        }
        self.seq += 1;
        let frame = pack(cmd, self.seq);
        unsafe {
            ptr::write_unaligned(self.ptr.add(self.offset) as *mut WalFrame, frame);
        }
        self.offset += FRAME;
    }

    pub fn recover(&mut self, mut visit: impl FnMut(Command)) {
        let mut offset = 0;
        let mut seq = 0u64;
        while offset + FRAME <= self.len {
            let frame = unsafe { ptr::read_unaligned(self.ptr.add(offset) as *const WalFrame) };
            if frame.magic != WAL_MAGIC {
                break;
            }
            let got = checksum(&frame);
            if got != frame.checksum {
                break;
            }
            visit(unpack(&frame));
            seq = frame.seq;
            offset += FRAME;
        }
        self.seq = seq;
        self.offset = offset;
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

fn pack(cmd: &Command, seq: u64) -> WalFrame {
    let mut frame = WalFrame {
        magic: WAL_MAGIC,
        checksum: 0,
        seq,
        ty: cmd.ty as u8,
        side: cmd.side as u8,
        _pad: [0; 6],
        price: cmd.price.0,
        quantity: cmd.quantity,
        client_fd: cmd.client_fd,
        user_ref: cmd.user_ref,
        cl_ord_id: cmd.cl_ord_id,
        time_in_force: cmd.time_in_force,
        display: cmd.display,
    };
    frame.checksum = checksum(&frame);
    frame
}

fn unpack(frame: &WalFrame) -> Command {
    Command {
        ty: match frame.ty {
            0 => CommandType::Add,
            1 => CommandType::Modify,
            2 => CommandType::Cancel,
            3 => CommandType::Reset,
            _ => CommandType::Poison,
        },
        side: if frame.side == 0 { Side::Bid } else { Side::Ask },
        price: Price(frame.price),
        quantity: frame.quantity,
        client_fd: frame.client_fd,
        user_ref: frame.user_ref,
        cl_ord_id: frame.cl_ord_id,
        time_in_force: frame.time_in_force,
        display: frame.display,
        capacity: 0,
    }
}

fn checksum(frame: &WalFrame) -> u32 {
    // Portable stand-in for `_mm_crc32_u64`. Good enough to detect torn writes.
    let mut h = 0x811c_9dc5_u32;
    let bytes = unsafe {
        std::slice::from_raw_parts(
            (frame as *const WalFrame as *const u8).add(8),
            FRAME - 8,
        )
    };
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn use_raw_fd(file: &std::fs::File) -> libc::c_int {
    use std::os::fd::AsRawFd;
    file.as_raw_fd()
}
