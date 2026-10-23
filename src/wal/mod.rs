//! Segmented mmap WAL. Every command is written before it is applied, and
//! recovery replays the log in order.
//!
//! The log is a chain of fixed-size segment files. A helper thread keeps
//! blank spares ready and retires full segments. When the active segment
//! fills, the writer swaps in a spare and stamps its header: a pointer
//! exchange and a 64-byte store, with no syscall.

mod config;
mod flags;
mod frame;
mod header;
mod mode;
mod preparer;
mod recover;
mod segment;
mod stats;

#[cfg(test)]
mod tests;

use std::mem;

pub use config::WalConfig;
pub use flags::{USAGE as FLAGS_USAGE, WalFlags};
pub use frame::{FRAME, WAL_VERSION};
pub use header::{HEADER, SEGMENT_VERSION};
pub use mode::Mode;
pub use preparer::WalMonitor;
pub use segment::{Prep, reserve_blocks};
pub use stats::WalStats;

use frame::{CHAIN_SEED, WalFrame};
use header::SegmentHeader;
use preparer::Spares;
use segment::Segment;

use crate::error::WalError;
use crate::types::Command;

pub struct Wal {
    epoch: u64,
    active: Segment,
    index: u64,
    offset: usize,
    end: usize,
    seq: u64,
    prev: u32,
    mode: Mode,
    spares: Spares,
}

impl Wal {
    /// Open the log in `cfg.dir`, replaying every journalled command into
    /// `visit` in order, and position the writer after the last one.
    pub fn open(cfg: &WalConfig, visit: impl FnMut(Command)) -> Result<Self, WalError> {
        cfg.validate()?;
        let tail = recover::recover(cfg, visit)?;
        let (active, index, offset, next_name) = match tail.active {
            Some((seg, h)) => (seg, h.index, tail.offset, tail.next_name),
            None => {
                let mut seg =
                    Segment::create(&cfg.dir, tail.next_name, cfg.segment_bytes, cfg.prep())?;
                seg.activate(SegmentHeader::new(cfg.epoch, 0, 1, CHAIN_SEED));
                (seg, 0, HEADER, tail.next_name + 1)
            }
        };
        let spares = Spares::start(cfg, next_name)?;
        let end = active.end();
        Ok(Self {
            epoch: cfg.epoch,
            active,
            index,
            offset,
            end,
            seq: tail.seq,
            prev: tail.prev,
            mode: Mode::Normal,
            spares,
        })
    }

    /// Append one frame. On `Err` nothing was written and the caller must
    /// not apply the command. `CancelOnly` and `Full` are the degraded modes:
    /// the log never halts and never skips a frame it reported as written.
    #[inline]
    pub fn write(&mut self, cmd: &Command) -> Result<(), WalError> {
        if self.mode != Mode::Normal {
            self.degraded(cmd)?;
        } else if self.offset + FRAME > self.end {
            self.roll(cmd)?;
        }
        let frame = WalFrame::pack(cmd, self.seq + 1, self.prev);
        unsafe { frame.write(self.active.at(self.offset)) };
        self.seq += 1;
        self.prev = frame.checksum;
        self.offset += FRAME;
        Ok(())
    }

    /// The active segment is full: swap in a spare, or fall back to the
    /// reserve in cancel-only mode, or refuse everything.
    #[cold]
    fn roll(&mut self, cmd: &Command) -> Result<(), WalError> {
        if let Some(next) = self.spares.take() {
            self.swap(next);
            return Ok(());
        }
        self.enter_reserve(cmd)
    }

    #[cold]
    fn enter_reserve(&mut self, cmd: &Command) -> Result<(), WalError> {
        match self.spares.take_reserve() {
            Some(reserve) => {
                self.swap(reserve);
                self.set_mode(Mode::CancelOnly);
                admit_cancel_only(cmd)
            }
            None => {
                self.set_mode(Mode::RejectAll);
                Err(WalError::Full)
            }
        }
    }

    /// Leave a degraded mode as soon as a spare is ready; until then apply
    /// that mode's rule.
    #[cold]
    fn degraded(&mut self, cmd: &Command) -> Result<(), WalError> {
        if self.spares.has_spare()
            && let Some(next) = self.spares.take()
        {
            self.swap(next);
            self.set_mode(Mode::Normal);
            return Ok(());
        }
        let room = self.offset + FRAME <= self.end;
        // `write` only calls this once `mode` has left Normal.
        if self.mode == Mode::CancelOnly && room {
            admit_cancel_only(cmd)
        } else {
            self.enter_reserve(cmd)
        }
    }

    fn swap(&mut self, mut next: Segment) {
        next.activate(SegmentHeader::new(
            self.epoch,
            self.index + 1,
            self.seq + 1,
            self.prev,
        ));
        let done = mem::replace(&mut self.active, next);
        self.index += 1;
        self.offset = HEADER;
        self.end = self.active.end();
        self.spares.retire(done);
    }

    fn set_mode(&mut self, mode: Mode) {
        if self.mode != mode {
            self.mode = mode;
            self.spares.set_mode(mode);
        }
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Sequence of the last journalled command (0 for an empty log).
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Header index of the active segment.
    pub fn segment(&self) -> u64 {
        self.index
    }

    /// Handle for reading journal counters from another thread.
    pub fn monitor(&self) -> WalMonitor {
        self.spares.monitor()
    }

    pub fn stats(&self) -> WalStats {
        self.spares.monitor().stats()
    }

    #[cfg(test)]
    pub(crate) fn spares_wanted(&self) -> usize {
        self.spares.wanted()
    }

    #[cfg(test)]
    pub(crate) fn hold_spares(&mut self, held: bool) {
        self.spares.hold(held);
    }
}

fn admit_cancel_only(cmd: &Command) -> Result<(), WalError> {
    if Mode::cancel_only_admits(cmd) {
        Ok(())
    } else {
        Err(WalError::CancelOnly)
    }
}
