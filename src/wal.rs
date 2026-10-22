//! 2 GB mmap WAL. Write `Command` before apply; recover by replay.

use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::ptr;

use crate::error::WalError;
use crate::types::{Command, CommandType};
use calvera_books::{Price, Side};

const WAL_MAGIC: u32 = 0x214C_4157; // "WAL!"
pub const WAL_SIZE: usize = 2 * 1024 * 1024 * 1024;
const FRAME: usize = 64;
/// Frame layout version. Frames with any other version are refused on recovery.
pub const WAL_VERSION: u8 = 1;

#[repr(C, align(64))]
struct WalFrame {
    magic: u32,
    checksum: u32,
    seq: u64,
    // packed command (not the Rust layout)
    ty: u8,
    side: u8,
    version: u8,
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

    /// Append one frame. `Full` leaves the log unchanged; the caller must
    /// not apply the command.
    pub fn write(&mut self, cmd: &Command) -> Result<(), WalError> {
        if self.offset + FRAME > self.len {
            return Err(WalError::Full);
        }
        self.seq += 1;
        let frame = pack(cmd, self.seq);
        unsafe {
            ptr::write_unaligned(self.ptr.add(self.offset) as *mut WalFrame, frame);
        }
        self.offset += FRAME;
        Ok(())
    }

    /// Replay every frame in order. Sequences must run 1, 2, 3, ... with no
    /// gap. The log ends at the first all-zero frame. An invalid frame is
    /// dropped only when nothing but zeros follows it (a torn last write);
    /// anywhere else it is an error. A frame with another layout version is
    /// refused, never misread.
    pub fn recover(&mut self, mut visit: impl FnMut(Command)) -> Result<(), WalError> {
        let mut offset = 0;
        let mut seq = 0u64;
        while offset + FRAME <= self.len {
            let bytes = unsafe { std::slice::from_raw_parts(self.ptr.add(offset), FRAME) };
            if is_zero(bytes) {
                break;
            }
            let frame = unsafe { ptr::read_unaligned(self.ptr.add(offset) as *const WalFrame) };
            if frame.magic != WAL_MAGIC || checksum(&frame) != frame.checksum {
                let rest = unsafe {
                    std::slice::from_raw_parts(
                        self.ptr.add(offset + FRAME),
                        self.len - offset - FRAME,
                    )
                };
                if is_zero(rest) {
                    unsafe { ptr::write_bytes(self.ptr.add(offset), 0, FRAME) };
                    break;
                }
                return Err(WalError::Corrupt { offset });
            }
            if frame.version != WAL_VERSION {
                return Err(WalError::Version {
                    seq: frame.seq,
                    found: frame.version,
                });
            }
            if frame.seq != seq + 1 {
                return Err(WalError::Gap {
                    expected: seq + 1,
                    found: frame.seq,
                });
            }
            visit(unpack(&frame));
            seq = frame.seq;
            offset += FRAME;
        }
        self.seq = seq;
        self.offset = offset;
        Ok(())
    }
}

fn is_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
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
            5 => CommandType::Entry,
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

    fn frame_at(w: &Wal, i: usize) -> WalFrame {
        unsafe { ptr::read_unaligned(w.ptr.add(i * FRAME) as *const WalFrame) }
    }

    fn put_frame(w: &mut Wal, i: usize, frame: WalFrame) {
        unsafe { ptr::write_unaligned(w.ptr.add(i * FRAME) as *mut WalFrame, frame) };
    }

    #[test]
    fn round_trips_every_command_and_drops_a_torn_tail() {
        let wal = TempWal::new("roundtrip");
        let mut w = Wal::open_sized(&wal.0, 4096).unwrap();
        let mut none = 0;
        w.recover(|_| none += 1).unwrap();
        assert_eq!(none, 0);

        let written = [
            sample(CommandType::Add, Side::Bid, 0),
            sample(CommandType::Modify, Side::Ask, 1),
            sample(CommandType::Cancel, Side::Bid, 2),
            sample(CommandType::Reset, Side::Ask, 3),
            sample(CommandType::Poison, Side::Bid, 4),
            sample(CommandType::Entry, Side::Bid, 5),
        ];
        for cmd in &written {
            w.write(cmd).unwrap();
        }
        assert_eq!(w.bytes_written(), written.len() * FRAME);

        let mut got = Vec::new();
        w.recover(|cmd| got.push(cmd)).unwrap();
        assert_eq!(got.len(), written.len());
        assert_eq!(got[4].ty, CommandType::Poison);
        assert_eq!(got[5].ty, CommandType::Entry);
        assert_eq!(got[1].side, Side::Ask);
        unsafe {
            *w.ptr.add(5 * FRAME + 16) = 0xAB;
        }
        got.clear();
        w.recover(|cmd| got.push(cmd)).unwrap();
        assert_eq!(got.len(), 5);
        assert_eq!(w.bytes_written(), 5 * FRAME);
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

        w.write(&written[0]).unwrap();
        let mut seqs = Vec::new();
        w.recover(|_| seqs.push(())).unwrap();
        assert_eq!(seqs.len(), 6);
        assert_eq!(frame_at(&w, 5).seq, 6);
    }

    #[test]
    fn a_bad_frame_before_the_tail_is_an_error() {
        let wal = TempWal::new("mid");
        let mut w = Wal::open_sized(&wal.0, 4096).unwrap();
        for user in 0..4 {
            w.write(&sample(CommandType::Add, Side::Bid, user)).unwrap();
        }
        unsafe {
            *w.ptr.add(FRAME + 16) = 0xAB;
        }
        let mut got = 0;
        let err = w.recover(|_| got += 1).unwrap_err();
        assert!(matches!(err, WalError::Corrupt { offset } if offset == FRAME));
        assert_eq!(got, 1);
    }

    #[test]
    fn a_sequence_gap_is_an_error() {
        let wal = TempWal::new("gap");
        let mut w = Wal::open_sized(&wal.0, 4096).unwrap();
        for user in 0..3 {
            w.write(&sample(CommandType::Add, Side::Bid, user)).unwrap();
        }
        let mut frame = frame_at(&w, 1);
        frame.seq = 7;
        frame.checksum = checksum(&frame);
        put_frame(&mut w, 1, frame);
        let err = w.recover(|_| {}).unwrap_err();
        assert!(matches!(
            err,
            WalError::Gap {
                expected: 2,
                found: 7
            }
        ));
    }

    #[test]
    fn a_frame_from_another_layout_version_is_refused() {
        let wal = TempWal::new("version");
        let mut w = Wal::open_sized(&wal.0, 4096).unwrap();
        w.write(&sample(CommandType::Add, Side::Bid, 1)).unwrap();
        w.write(&sample(CommandType::Add, Side::Ask, 2)).unwrap();
        let mut frame = frame_at(&w, 1);
        frame.version = WAL_VERSION.wrapping_add(1);
        frame.checksum = checksum(&frame);
        put_frame(&mut w, 1, frame);
        let mut got = 0;
        let err = w.recover(|_| got += 1).unwrap_err();
        assert!(matches!(err, WalError::Version { seq: 2, .. }));
        assert_eq!(got, 1);
    }

    #[test]
    fn a_write_to_a_full_log_errors_and_writes_nothing() {
        let wal = TempWal::new("full");
        let mut w = Wal::open_sized(&wal.0, 4096).unwrap();
        let cmd = sample(CommandType::Add, Side::Bid, 1);
        for _ in 0..(4096 / FRAME) {
            w.write(&cmd).unwrap();
        }
        assert_eq!(w.bytes_written(), 4096);
        assert!(matches!(w.write(&cmd), Err(WalError::Full)));
        assert_eq!(w.bytes_written(), 4096);
        let mut n = 0;
        w.recover(|_| n += 1).unwrap();
        assert_eq!(n, 4096 / FRAME);
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
