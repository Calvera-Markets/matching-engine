//! Replicator thread: stream journalled commands to the standby and advance
//! the replicated cursor as the standby acknowledges them.

use std::sync::Arc;
use std::time::Duration;

use super::Verdict;
use super::config::WaitStrategy;
use super::ring::GateRing;
use crate::error::ReplicaError;
use crate::types::{Command, CommandType};

/// Link to the standby. The runtime supplies it; the engine owns no sockets.
pub trait Transport: Send {
    /// Deliver one journalled command and return once the standby has it.
    fn replicate(&mut self, seq: u64, cmd: &Command) -> Result<(), ReplicaError>;
}

pub struct Replicator<T: Transport> {
    pub ring: Arc<GateRing>,
    pub transport: T,
    pub wait: WaitStrategy,
    pub retry: Duration,
}

impl<T: Transport> Replicator<T> {
    /// Runs until it has passed a Poison command. A failed send is retried
    /// in place: in sync mode the engine stalls behind it, never skips it.
    pub fn run(mut self) {
        let mut at = self.ring.replicated();
        loop {
            if at >= self.ring.published() {
                self.wait.idle();
                continue;
            }
            let slot = self.ring.read(at);
            if slot.verdict == Verdict::Apply && slot.cmd.ty != CommandType::Poison {
                while self.transport.replicate(slot.seq, &slot.cmd).is_err() {
                    self.ring.replica_failed();
                    std::thread::sleep(self.retry);
                }
            }
            at += 1;
            self.ring.set_replicated(at);
            if slot.cmd.ty == CommandType::Poison {
                return;
            }
        }
    }
}
