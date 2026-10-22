//! One memory-mapped segment file.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr;

use super::frame::FRAME;
use super::header::{HEADER, SegmentHeader, Slot};

pub struct Segment {
    ptr: *mut u8,
    len: usize,
    path: PathBuf,
    _file: File,
}

// The mapping is owned by exactly one thread at a time.
unsafe impl Send for Segment {}

/// How a new segment file is readied before the engine writes to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Prep {
    /// Reserve blocks with `fallocate` instead of a sparse `ftruncate`.
    pub preallocate: bool,
    /// Write every page once so no page faults land on the writer.
    pub prefault: bool,
}

impl Segment {
    pub fn file_name(name: u64) -> String {
        format!("{name:020}.seg")
    }

    /// Parse a name produced by `file_name`.
    pub fn parse_name(file: &str) -> Option<u64> {
        file.strip_suffix(".seg")?.parse().ok()
    }

    /// Create a new, blank segment file of `len` bytes.
    pub fn create(dir: &Path, name: u64, len: usize, prep: Prep) -> io::Result<Self> {
        let path = dir.join(Self::file_name(name));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        if prep.preallocate {
            let rc = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len as libc::off_t) };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }
        } else {
            file.set_len(len as u64)?;
        }
        let seg = Self::map(file, path, len)?;
        if prep.prefault {
            seg.prefault();
        }
        Ok(seg)
    }

    /// Map an existing segment file at its current length.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let len = file.metadata()?.len() as usize;
        Self::map(file, path.to_path_buf(), len)
    }

    fn map(file: File, path: PathBuf, len: usize) -> io::Result<Self> {
        if len < HEADER + FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "segment {} is {len} bytes, below one header and one frame",
                    path.display()
                ),
            ));
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
        Ok(Self {
            ptr: ptr as *mut u8,
            len,
            path,
            _file: file,
        })
    }

    fn prefault(&self) {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let mut at = 0;
        while at < self.len {
            unsafe { ptr::write_volatile(self.ptr.add(at), 0) };
            at += page;
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Byte offset just past the last frame slot that fits.
    pub fn end(&self) -> usize {
        HEADER + (self.len - HEADER) / FRAME * FRAME
    }

    pub fn header(&self) -> Slot {
        unsafe { SegmentHeader::read(self.ptr) }
    }

    /// Stamp the header. A plain store into the mapping: no syscall.
    #[inline]
    pub fn activate(&mut self, header: SegmentHeader) {
        unsafe { header.write(self.ptr) };
    }

    #[inline]
    pub fn at(&self, offset: usize) -> *mut u8 {
        debug_assert!(offset + FRAME <= self.len);
        unsafe { self.ptr.add(offset) }
    }

    /// Whether every byte from `offset` to the end is zero.
    pub fn zero_from(&self, offset: usize) -> bool {
        let rest = unsafe { std::slice::from_raw_parts(self.ptr.add(offset), self.len - offset) };
        rest.iter().all(|&b| b == 0)
    }

    pub fn clear(&mut self, offset: usize, len: usize) {
        unsafe { ptr::write_bytes(self.ptr.add(offset), 0, len) };
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}
