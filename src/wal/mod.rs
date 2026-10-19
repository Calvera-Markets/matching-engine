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
pub use preparer::WalMonitor;
pub use segment::Prep;
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
            spares,
        })
    }

    /// Append one frame. On `Err` nothing was written and the caller must
    /// not apply the command.
    #[inline]
    pub fn write(&mut self, cmd: &Command) -> Result<(), WalError> {
        if self.offset + FRAME > self.end {
            self.roll()?;
        }
        let frame = WalFrame::pack(cmd, self.seq + 1, self.prev);
        unsafe { frame.write(self.active.at(self.offset)) };
        self.seq += 1;
        self.prev = frame.checksum;
        self.offset += FRAME;
        Ok(())
    }

    /// Swap the full segment for a spare. `Full` when no spare is ready.
    #[cold]
    fn roll(&mut self) -> Result<(), WalError> {
        let mut next = self.spares.take().ok_or(WalError::Full)?;
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
        Ok(())
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
    pub(crate) fn hold_spares(&mut self, held: bool) {
        self.spares.hold(held);
    }
}
