//! Where the engine gets its commands, and the proof each one was journalled.
//!
//! [`Inline`] journals on the engine thread before the engine applies the
//! command; it is the default and compiles to a pop, a WAL store and a branch.
//! [`Gated`] reads from a ring that a journaller thread fills only after the
//! WAL write, and that a replicator thread can additionally gate on a standby
//! ack. Either way the engine never applies a command that is not in the log.

mod config;
mod gated;
mod inline;
mod journaller;
mod replicator;
mod ring;

#[cfg(test)]
mod tests;

pub use config::{GateConfig, ReplicaMode, WaitStrategy};
pub use gated::{Gated, GatedThreads, spawn_gated};
pub use inline::Inline;
pub use replicator::Transport;
pub use ring::GateStats;

use crate::error::WalError;
use crate::reject;
use crate::types::Command;

/// Whether the engine may apply a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Journalled (or journalling is off): apply it.
    Apply,
    /// Not journalled: reject with this `reject` code and do not apply.
    Refuse(u16),
}

impl Verdict {
    pub fn of(result: Result<(), WalError>) -> Self {
        match result {
            Ok(()) => Verdict::Apply,
            Err(WalError::Full) => Verdict::Refuse(reject::JOURNAL_FULL),
            Err(WalError::CancelOnly) => Verdict::Refuse(reject::JOURNAL_CANCEL_ONLY),
            Err(_) => Verdict::Refuse(reject::JOURNAL_FAULT),
        }
    }
}

/// The engine's command source.
pub trait Journal {
    /// The next command and its verdict, or `None` when nothing is ready.
    fn next(&mut self) -> Option<(Command, Verdict)>;
}
