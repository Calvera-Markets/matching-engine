//! Frame writer over the current segment, with the switch to the next one.

use std::ffi::CString;
use std::io;
use std::mem;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;

use matching_engine::Spsc;

use crate::FRAME;
use crate::args::{Args, Prep};
use crate::segment::Segment;

pub struct Writer {
    cur: Segment,
    offset: usize,
    prev: u32,
    ready: Arc<Spsc<Segment>>,
    used: Arc<Spsc<Segment>>,
    hot: Option<Hot>,
    stalls: usize,
}

struct Hot {
    dir: PathBuf,
    next: u64,
    len: usize,
}

impl Writer {
    pub fn new(
        first: Segment,
        args: &Args,
        ready: Arc<Spsc<Segment>>,
        used: Arc<Spsc<Segment>>,
    ) -> Self {
        let hot = match args.prep {
            Prep::Hot => Some(Hot {
                dir: args.dir.clone(),
                next: 1,
                len: args.segment_bytes,
            }),
            _ => None,
        };
        Self {
            cur: first,
            offset: 0,
            prev: 0x811c_9dc5,
            ready,
            used,
            hot,
            stalls: 0,
        }
    }

    /// Write one frame; `true` when this write rolled to a new segment.
    #[inline]
    pub fn write(&mut self, seq: u64) -> io::Result<bool> {
        let mut switched = false;
        if self.offset + FRAME > self.cur.len {
            self.switch()?;
            switched = true;
        }
        let mut frame = [0u8; FRAME];
        frame[0..4].copy_from_slice(&0x214C_4157u32.to_le_bytes());
        frame[8..16].copy_from_slice(&seq.to_le_bytes());
        frame[16..24].copy_from_slice(&seq.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        let sum = checksum(self.prev, &frame[8..]);
        frame[4..8].copy_from_slice(&sum.to_le_bytes());
        unsafe {
            ptr::copy_nonoverlapping(frame.as_ptr(), self.cur.ptr.add(self.offset), FRAME);
        }
        self.prev = sum;
        self.offset += FRAME;
        Ok(switched)
    }

    fn switch(&mut self) -> io::Result<()> {
        let next = match &mut self.hot {
            Some(hot) => {
                let seg = Segment::create(&hot.dir, hot.next, hot.len, Prep::Sparse)?;
                hot.next += 1;
                seg
            }
            None => loop {
                if let Some(seg) = self.ready.pop() {
                    break seg;
                }
                self.stalls += 1;
                std::hint::spin_loop();
            },
        };
        let old = mem::replace(&mut self.cur, next);
        self.offset = 0;
        if self.hot.is_some() {
            old.retire();
        } else if let Err(old) = self.used.try_push(old) {
            old.retire();
        }
        Ok(())
    }

    /// Retire the current segment; returns how many spins waited for a spare.
    pub fn finish(self) -> usize {
        self.cur.retire();
        self.stalls
    }
}

fn checksum(seed: u32, bytes: &[u8]) -> u32 {
    let mut h = seed;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

pub fn fs_name(dir: &Path) -> io::Result<String> {
    let c = CString::new(dir.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let mut st: libc::statfs = unsafe { mem::zeroed() };
    if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(match st.f_type as i64 {
        0x5846_5342 => "xfs".into(),
        0xEF53 => "ext4".into(),
        0x0102_1994 => "tmpfs".into(),
        0x9123_683E => "btrfs".into(),
        0x794C_7630 => "overlayfs".into(),
        other => format!("0x{other:x}"),
    })
}
