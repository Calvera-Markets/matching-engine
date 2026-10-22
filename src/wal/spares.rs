//! Source of blank segments for the next roll.

use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;

use super::segment::{Prep, Segment};

/// Keeps `want` blank segments ready.
// TODO: prepare and retire on a helper thread so no file creation, fallocate,
// page fault, or munmap happens on the engine thread.
pub struct Spares {
    dir: PathBuf,
    len: usize,
    prep: Prep,
    want: usize,
    next_name: u64,
    ready: VecDeque<Segment>,
    failures: u64,
    held: bool,
}

impl Spares {
    pub fn new(dir: PathBuf, len: usize, prep: Prep, want: usize, next_name: u64) -> Self {
        Self {
            dir,
            len,
            prep,
            want,
            next_name,
            ready: VecDeque::with_capacity(want),
            failures: 0,
            held: false,
        }
    }

    /// Stop preparing new spares (tests drive the log to exhaustion).
    #[cfg(test)]
    pub fn hold(&mut self, held: bool) {
        self.held = held;
    }

    pub fn fill(&mut self) -> io::Result<()> {
        while !self.held && self.ready.len() < self.want {
            let seg = Segment::create(&self.dir, self.next_name, self.len, self.prep)?;
            self.next_name += 1;
            self.ready.push_back(seg);
        }
        Ok(())
    }

    /// Next blank segment, or `None` when none is ready and none can be made.
    pub fn take(&mut self) -> Option<Segment> {
        if self.ready.is_empty() && self.fill().is_err() {
            self.failures += 1;
        }
        let seg = self.ready.pop_front();
        if self.fill().is_err() {
            self.failures += 1;
        }
        seg
    }

    /// A finished segment. Unmapped on drop; the file stays as history.
    pub fn retire(&mut self, done: Segment) {
        drop(done);
    }

    pub fn ready(&self) -> usize {
        self.ready.len()
    }

    /// Times a spare could not be created.
    pub fn failures(&self) -> u64 {
        self.failures
    }
}
