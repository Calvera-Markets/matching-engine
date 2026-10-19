//! Journaller thread: WAL first, then publish to the engine.

use std::sync::Arc;

use super::Verdict;
use super::config::WaitStrategy;
use super::inline::Inline;
use super::ring::{GateRing, Slot};
use crate::spsc::Spsc;
use crate::types::{Command, CommandType};

pub struct Journaller {
    pub input: Arc<Spsc<Command>>,
    pub journal: Inline,
    pub ring: Arc<GateRing>,
    pub wait: WaitStrategy,
}

impl Journaller {
    /// Runs until it has forwarded a Poison command.
    pub fn run(mut self) {
        let mut at = self.ring.published();
        loop {
            let Some(cmd) = self.input.pop() else {
                self.wait.idle();
                continue;
            };
            while !self.ring.has_room(at) {
                self.wait.idle();
            }
            let verdict = self.journal.record(&cmd);
            let seq = match verdict {
                Verdict::Apply if cmd.ty != CommandType::Poison => self.journal.wal().seq(),
                _ => 0,
            };
            self.ring.publish(at, Slot { cmd, verdict, seq });
            at += 1;
            if cmd.ty == CommandType::Poison {
                return;
            }
        }
    }
}
