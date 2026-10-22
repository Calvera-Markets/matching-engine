//! Memory-mapped segment files and the helper thread that prepares them.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use matching_engine::Spsc;

use crate::args::Prep;

pub struct Segment {
    pub ptr: *mut u8,
    pub len: usize,
    path: PathBuf,
    _file: File,
}

unsafe impl Send for Segment {}

impl Segment {
    pub fn create(dir: &Path, n: u64, len: usize, prep: Prep) -> io::Result<Self> {
        let path = dir.join(format!("bench-{n:012}.seg"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        if prep.preallocates() {
            let rc = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len as libc::off_t) };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }
        } else {
            file.set_len(len as u64)?;
        }
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let seg = Self {
            ptr: ptr as *mut u8,
            len,
            path,
            _file: file,
        };
        if prep.prefaults() {
            seg.prefault();
        }
        Ok(seg)
    }

    fn prefault(&self) {
        let page = page_size();
        let mut at = 0;
        while at < self.len {
            unsafe { ptr::write_volatile(self.ptr.add(at), 0) };
            at += page;
        }
    }

    /// Unmap and unlink. The benchmark keeps no history.
    pub fn retire(self) {
        unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len) };
        let _ = std::fs::remove_file(&self.path);
    }
}

pub fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

/// Helper loop: keeps `ready` full of prepared segments and retires used ones.
pub struct Helper {
    pub dir: PathBuf,
    pub len: usize,
    pub prep: Prep,
    pub first: u64,
    pub count: u64,
    pub ready: Arc<Spsc<Segment>>,
    pub used: Arc<Spsc<Segment>>,
    pub stop: Arc<AtomicBool>,
    pub prepared: Arc<AtomicUsize>,
}

impl Helper {
    pub fn run(self) -> io::Result<()> {
        let mut n = self.first;
        let mut pending: Option<Segment> = None;
        while !self.stop.load(Ordering::Relaxed) {
            while let Some(seg) = self.used.pop() {
                seg.retire();
            }
            if pending.is_none() && n < self.first + self.count {
                pending = Some(Segment::create(&self.dir, n, self.len, self.prep)?);
                n += 1;
            }
            match pending.take() {
                Some(seg) => match self.ready.try_push(seg) {
                    Ok(()) => {
                        self.prepared.fetch_add(1, Ordering::Release);
                    }
                    Err(back) => {
                        pending = Some(back);
                        std::thread::yield_now();
                    }
                },
                None => std::thread::yield_now(),
            }
        }
        if let Some(seg) = pending {
            seg.retire();
        }
        while let Some(seg) = self.ready.pop() {
            seg.retire();
        }
        while let Some(seg) = self.used.pop() {
            seg.retire();
        }
        Ok(())
    }
}
