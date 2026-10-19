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
        side: if frame.side == 0 {
            Side::Bid
        } else {
            Side::Ask
        },
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
        std::slice::from_raw_parts((frame as *const WalFrame as *const u8).add(8), FRAME - 8)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TempWal;
    use crate::types::{Command, CommandType};
    use calvera_books::{Price, Side};
    use std::path::Path;

    fn sample(ty: CommandType, side: Side, user: u32) -> Command {
        let mut cmd = Command::blank(ty);
        cmd.side = side;
        cmd.user_ref = user;
        cmd.price = Price(10 + user as u64);
        cmd.quantity = 5 + user as u64;
        cmd.client_fd = 3;
        cmd.time_in_force = 7;
        cmd.display = 8;
        cmd.cl_ord_id = [b'Q'; 14];
        cmd.capacity = 9;
        cmd
    }

    #[test]
    fn round_trips_every_command_and_stops_on_a_bad_checksum() {
        let wal = TempWal::new("roundtrip");
        let mut w = Wal::open_sized(&wal.0, 4096).unwrap();
        let mut none = 0;
        w.recover(|_| none += 1);
        assert_eq!(none, 0);

        let written = [
            sample(CommandType::Add, Side::Bid, 0),
            sample(CommandType::Modify, Side::Ask, 1),
            sample(CommandType::Cancel, Side::Bid, 2),
            sample(CommandType::Reset, Side::Ask, 3),
            sample(CommandType::Poison, Side::Bid, 4),
        ];
        for cmd in &written {
            w.write(cmd);
        }
        assert_eq!(w.bytes_written(), written.len() * FRAME);

        let mut got = Vec::new();
        w.recover(|cmd| got.push(cmd));
        assert_eq!(got.len(), written.len());
        assert_eq!(got[4].ty, CommandType::Poison);
        assert_eq!(got[1].side, Side::Ask);
        unsafe {
            *w.ptr.add(4 * FRAME + 16) = 0xAB;
        }
        got.clear();
        w.recover(|cmd| got.push(cmd));
        assert_eq!(got.len(), 4);
        assert_eq!(w.bytes_written(), 4 * FRAME);
        for (got, want) in got.iter().zip(written.iter()) {
            assert_eq!(got.ty, want.ty);
            assert_eq!(got.side, want.side);
            assert_eq!(got.user_ref, want.user_ref);
            assert_eq!(got.price, want.price);
            assert_eq!(got.quantity, want.quantity);
            assert_eq!(got.client_fd, want.client_fd);
            assert_eq!(got.time_in_force, want.time_in_force);
            assert_eq!(got.display, want.display);
            assert_eq!(got.cl_ord_id, want.cl_ord_id);
            assert_eq!(got.capacity, 0);
        }
    }

    #[test]
    fn write_stops_when_the_file_is_full() {
        let wal = TempWal::new("full");
        let mut w = Wal::open_sized(&wal.0, 4096).unwrap();
        let cmd = sample(CommandType::Add, Side::Bid, 1);
        for _ in 0..(4096 / FRAME) {
            w.write(&cmd);
        }
        assert_eq!(w.bytes_written(), 4096);
        w.write(&cmd);
        assert_eq!(w.bytes_written(), 4096);
    }

    #[test]
    fn a_zero_length_map_fails() {
        let wal = TempWal::new("zero");
        assert!(Wal::open_sized(&wal.0, 0).is_err());
    }

    #[test]
    fn missing_directory_fails() {
        let err = Wal::open_sized(Path::new("/no/such/me-wal-dir/book.wal"), 4096);
        assert!(err.is_err());
    }

    #[test]
    fn open_uses_the_default_size() {
        let wal = TempWal::new("default");
        let w = Wal::open(&wal.0).unwrap();
        assert_eq!(w.len, WAL_SIZE);
        assert_eq!(w.bytes_written(), 0);
    }
}
