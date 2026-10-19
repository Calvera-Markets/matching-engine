//! Helper thread that keeps blank segments ready and retires full ones, so
//! file creation, `fallocate`, page faults, `msync` and `munmap` stay off the
//! writing thread. The writer only pops a ready segment and pushes a full one.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::config::WalConfig;
use super::segment::{Prep, Segment};
use super::stats::{Counters, WalStats};
use crate::spsc::Spsc;

struct Shared {
    want: usize,
    ready: Spsc<Segment>,
    reserve: Spsc<Segment>,
    retired: Spsc<Segment>,
    stop: AtomicBool,
    held: AtomicBool,
    counters: Counters,
}

/// Writer-side handle. Dropping it stops and joins the helper.
pub struct Spares {
    shared: Arc<Shared>,
    helper: Option<JoinHandle<()>>,
}

/// Read-only view for monitoring from another thread.
#[derive(Clone)]
pub struct WalMonitor {
    shared: Arc<Shared>,
}

struct Helper {
    shared: Arc<Shared>,
    dir: PathBuf,
    len: usize,
    reserve_len: usize,
    prep: Prep,
    seal_sync: bool,
    poll: Duration,
    next_name: u64,
}

impl Spares {
    /// Prepare `cfg.spares` segments on the calling thread, then hand upkeep
    /// to the helper.
    pub fn start(cfg: &WalConfig, next_name: u64) -> io::Result<Self> {
        let shared = Arc::new(Shared {
            want: cfg.spares,
            ready: Spsc::new(cfg.spares.next_power_of_two()),
            reserve: Spsc::new(1),
            retired: Spsc::new(cfg.retire_queue.next_power_of_two()),
            stop: AtomicBool::new(false),
            held: AtomicBool::new(false),
            counters: Counters::default(),
        });
        let mut helper = Helper {
            shared: shared.clone(),
            dir: cfg.dir.clone(),
            len: cfg.segment_bytes,
            reserve_len: cfg.reserve_bytes,
            prep: cfg.prep(),
            seal_sync: cfg.seal_sync,
            poll: Duration::from_micros(cfg.preparer_poll_us),
            next_name,
        };
        helper.fill()?;
        let cpu = cfg.preparer_cpu;
        let handle = thread::Builder::new()
            .name("wal-preparer".into())
            .spawn(move || {
                if let Some(cpu) = cpu {
                    crate::pin_to_cpu(cpu);
                }
                helper.run();
            })?;
        Ok(Self {
            shared,
            helper: Some(handle),
        })
    }

    /// Next blank segment, or `None` when the helper has none ready.
    #[inline]
    pub fn take(&mut self) -> Option<Segment> {
        let seg = self.shared.ready.pop();
        match seg {
            Some(_) => Counters::bump(&self.shared.counters.rolls),
            None => Counters::bump(&self.shared.counters.no_spare),
        }
        seg
    }

    /// Whether a spare is ready, without taking it.
    #[inline]
    pub fn has_spare(&self) -> bool {
        !self.shared.ready.is_empty()
    }

    /// The emergency reserve segment, if the helper has one ready.
    pub fn take_reserve(&mut self) -> Option<Segment> {
        let seg = self.shared.reserve.pop();
        if seg.is_some() {
            Counters::bump(&self.shared.counters.reserves_used);
        }
        seg
    }

    pub fn set_mode(&self, mode: super::mode::Mode) {
        self.shared.counters.set_mode(mode);
    }

    /// Hand a full segment to the helper for flushing and unmapping.
    #[inline]
    pub fn retire(&mut self, done: Segment) {
        if let Err(done) = self.shared.retired.try_push(done) {
            Counters::bump(&self.shared.counters.inline_retires);
            drop(done);
        }
    }

    pub fn monitor(&self) -> WalMonitor {
        WalMonitor {
            shared: self.shared.clone(),
        }
    }

    #[cfg(test)]
    pub fn wanted(&self) -> usize {
        self.shared.want
    }

    /// Stop preparing new spares (tests drive the log to exhaustion).
    #[cfg(test)]
    pub fn hold(&mut self, held: bool) {
        self.shared.held.store(held, Ordering::Release);
    }
}

impl Drop for Spares {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(h) = self.helper.take() {
            let _ = h.join();
        }
    }
}

impl WalMonitor {
    pub fn stats(&self) -> WalStats {
        self.shared.counters.read(self.shared.ready.len())
    }
}

impl Helper {
    fn run(&mut self) {
        while !self.shared.stop.load(Ordering::Acquire) {
            let sealed = self.seal();
            if self.fill().is_err() {
                Counters::bump(&self.shared.counters.prepare_failures);
            }
            if !sealed {
                thread::sleep(self.poll);
            }
        }
        self.seal();
    }

    /// Top up the ready queue to `want`, then the reserve slot.
    fn fill(&mut self) -> io::Result<()> {
        while !self.shared.held.load(Ordering::Acquire)
            && self.shared.ready.len() < self.shared.want
        {
            let seg = Segment::create(&self.dir, self.next_name, self.len, self.prep)?;
            self.next_name += 1;
            if let Err(seg) = self.shared.ready.try_push(seg) {
                drop(seg);
                break;
            }
            Counters::bump(&self.shared.counters.prepared);
        }
        if !self.shared.held.load(Ordering::Acquire) && self.shared.reserve.is_empty() {
            let seg = Segment::create(&self.dir, self.next_name, self.reserve_len, self.prep)?;
            self.next_name += 1;
            if let Err(seg) = self.shared.reserve.try_push(seg) {
                drop(seg);
            }
        }
        Ok(())
    }

    /// Flush and unmap every retired segment; `true` if there was one.
    fn seal(&mut self) -> bool {
        let mut any = false;
        while let Some(seg) = self.shared.retired.pop() {
            any = true;
            if self.seal_sync && seg.sync().is_err() {
                Counters::bump(&self.shared.counters.seal_failures);
                continue;
            }
            Counters::bump(&self.shared.counters.sealed);
        }
        any
    }
}
