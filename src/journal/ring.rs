//! Sequenced ring between the journaller (single producer), an optional
//! replicator, and the engine. A slot is published only after its command is
//! in the WAL; the engine reads slot `i` only once it is published and, in
//! sync mode, once the replicator has the standby's ack for it. A slot is
//! reused only after both readers are past it.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

use super::Verdict;
use crate::types::Command;

#[derive(Clone, Copy)]
pub struct Slot {
    pub cmd: Command,
    pub verdict: Verdict,
    /// WAL sequence of an applied slot; 0 for a refused or unjournalled one.
    pub seq: u64,
}

#[repr(align(64))]
struct Cursor(AtomicU64);

pub struct GateRing {
    slots: Box<[UnsafeCell<MaybeUninit<Slot>>]>,
    mask: u64,
    replicated_gates_reuse: bool,
    published: Cursor,
    replicated: Cursor,
    consumed: Cursor,
    replica_failures: Cursor,
}

unsafe impl Send for GateRing {}
unsafe impl Sync for GateRing {}

/// Cursor positions and replica health, readable from any thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateStats {
    pub published: u64,
    pub replicated: u64,
    pub consumed: u64,
    pub replica_failures: u64,
}

impl GateRing {
    /// `capacity` is a power of two (checked by `GateConfig::validate`).
    /// With a replicator, slots are also held until it passes them.
    pub fn new(capacity: usize, with_replicator: bool) -> Self {
        let slots = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        Self {
            slots,
            mask: capacity as u64 - 1,
            replicated_gates_reuse: with_replicator,
            published: Cursor(AtomicU64::new(0)),
            replicated: Cursor(AtomicU64::new(0)),
            consumed: Cursor(AtomicU64::new(0)),
            replica_failures: Cursor(AtomicU64::new(0)),
        }
    }

    /// Producer: whether slot `published` is free.
    #[inline]
    pub fn has_room(&self, published: u64) -> bool {
        let mut low = self.consumed.0.load(Ordering::Acquire);
        if self.replicated_gates_reuse {
            low = low.min(self.replicated.0.load(Ordering::Acquire));
        }
        published - low <= self.mask
    }

    /// Producer: write slot `at` and publish it. Caller checked `has_room`.
    #[inline]
    pub fn publish(&self, at: u64, slot: Slot) {
        unsafe { (*self.slots[(at & self.mask) as usize].get()).write(slot) };
        self.published.0.store(at + 1, Ordering::Release);
    }

    #[inline]
    pub fn published(&self) -> u64 {
        self.published.0.load(Ordering::Acquire)
    }

    #[inline]
    pub fn replicated(&self) -> u64 {
        self.replicated.0.load(Ordering::Acquire)
    }

    /// Reader: copy of published slot `at`.
    #[inline]
    pub fn read(&self, at: u64) -> Slot {
        unsafe { (*self.slots[(at & self.mask) as usize].get()).assume_init_read() }
    }

    #[inline]
    pub fn set_replicated(&self, upto: u64) {
        self.replicated.0.store(upto, Ordering::Release);
    }

    #[inline]
    pub fn set_consumed(&self, upto: u64) {
        self.consumed.0.store(upto, Ordering::Release);
    }

    pub fn replica_failed(&self) {
        self.replica_failures.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> GateStats {
        GateStats {
            published: self.published(),
            replicated: self.replicated(),
            consumed: self.consumed.0.load(Ordering::Acquire),
            replica_failures: self.replica_failures.0.load(Ordering::Relaxed),
        }
    }
}
