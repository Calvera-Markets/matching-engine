//! Gated-journal parameters, supplied by the control plane.

use crate::error::GateError;

/// How the engine relates to the standby.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaMode {
    /// No replicator thread.
    Off,
    /// Replicate in the background; the engine gates on the journal only.
    Async,
    /// The engine waits for the standby's ack as well as the journal.
    Sync,
}

/// What a thread does while it waits for a cursor to move.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    Spin,
    Yield,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateConfig {
    /// Slots between the journaller and the engine; a power of two.
    pub ring_capacity: usize,
    pub replica: ReplicaMode,
    pub wait: WaitStrategy,
    pub journaller_cpu: Option<usize>,
    pub replicator_cpu: Option<usize>,
    /// Pause between failed sends to the standby.
    pub replica_retry_us: u64,
}

impl GateConfig {
    pub fn validate(&self) -> Result<(), GateError> {
        if self.ring_capacity == 0 || !self.ring_capacity.is_power_of_two() {
            return Err(GateError::Config(format!(
                "ring capacity {} is not a power of two",
                self.ring_capacity
            )));
        }
        if self.replica != ReplicaMode::Off && self.replica_retry_us == 0 {
            return Err(GateError::Config("replica retry must be above zero".into()));
        }
        Ok(())
    }
}

impl WaitStrategy {
    #[inline]
    pub fn idle(self) {
        match self {
            WaitStrategy::Spin => crate::pause(),
            WaitStrategy::Yield => std::thread::yield_now(),
        }
    }
}
