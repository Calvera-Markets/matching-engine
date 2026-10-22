//! Counters the journal publishes for monitoring. Read from any thread.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use super::mode::Mode;

#[derive(Default)]
pub struct Counters {
    pub(crate) prepared: AtomicU64,
    pub(crate) prepare_failures: AtomicU64,
    pub(crate) sealed: AtomicU64,
    pub(crate) seal_failures: AtomicU64,
    pub(crate) rolls: AtomicU64,
    pub(crate) no_spare: AtomicU64,
    pub(crate) inline_retires: AtomicU64,
    pub(crate) reserves_used: AtomicU64,
    pub(crate) mode: AtomicU8,
    pub(crate) mode_changes: AtomicU64,
}

/// Point-in-time view of the journal's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalStats {
    /// Prepared spares waiting for the next roll.
    pub spares_ready: usize,
    /// Spares the preparer created.
    pub prepared: u64,
    /// Spares the preparer failed to create.
    pub prepare_failures: u64,
    /// Finished segments flushed and unmapped.
    pub sealed: u64,
    /// Finished segments whose flush failed.
    pub seal_failures: u64,
    /// Rolls to a new segment.
    pub rolls: u64,
    /// Rolls refused because no spare was ready.
    pub no_spare: u64,
    /// Finished segments unmapped on the writer because the retire queue was full.
    pub inline_retires: u64,
    /// Emergency reserve segments switched to.
    pub reserves_used: u64,
    pub mode: Mode,
    /// Transitions between modes.
    pub mode_changes: u64,
}

impl Counters {
    pub(crate) fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_mode(&self, mode: Mode) {
        self.mode.store(mode as u8, Ordering::Relaxed);
        Self::bump(&self.mode_changes);
    }

    pub(crate) fn read(&self, spares_ready: usize) -> WalStats {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        WalStats {
            spares_ready,
            prepared: get(&self.prepared),
            prepare_failures: get(&self.prepare_failures),
            sealed: get(&self.sealed),
            seal_failures: get(&self.seal_failures),
            rolls: get(&self.rolls),
            no_spare: get(&self.no_spare),
            inline_retires: get(&self.inline_retires),
            reserves_used: get(&self.reserves_used),
            mode: Mode::from_u8(self.mode.load(Ordering::Relaxed)),
            mode_changes: get(&self.mode_changes),
        }
    }
}
