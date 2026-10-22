//! Engine side of the gated journal, and the threads behind it.

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::config::{GateConfig, ReplicaMode};
use super::inline::Inline;
use super::journaller::Journaller;
use super::replicator::{Replicator, Transport};
use super::ring::{GateRing, GateStats};
use super::{Journal, Verdict};
use crate::error::GateError;
use crate::spsc::Spsc;
use crate::types::Command;
use crate::wal::Wal;

pub struct Gated {
    ring: Arc<GateRing>,
    next: u64,
    sync: bool,
}

/// Threads feeding a [`Gated`] journal. Both exit after forwarding Poison.
pub struct GatedThreads {
    pub journaller: JoinHandle<()>,
    pub replicator: Option<JoinHandle<()>>,
    ring: Arc<GateRing>,
}

impl GatedThreads {
    pub fn stats(&self) -> GateStats {
        self.ring.stats()
    }

    pub fn join(self) -> thread::Result<()> {
        self.journaller.join()?;
        if let Some(r) = self.replicator {
            r.join()?;
        }
        Ok(())
    }
}

/// Start the journaller (and, unless `ReplicaMode::Off`, the replicator) on
/// `wal`, reading commands from `input`. `wal` must already be recovered.
pub fn spawn_gated<T: Transport + 'static>(
    cfg: &GateConfig,
    input: Arc<Spsc<Command>>,
    wal: Wal,
    transport: Option<T>,
) -> Result<(Gated, GatedThreads), GateError> {
    cfg.validate()?;
    let with_replicator = cfg.replica != ReplicaMode::Off;
    if with_replicator != transport.is_some() {
        return Err(GateError::Config(
            "a transport is required exactly when replication is on".into(),
        ));
    }
    let ring = Arc::new(GateRing::new(cfg.ring_capacity, with_replicator));
    let journaller = Journaller {
        input: input.clone(),
        journal: Inline::new(input, wal),
        ring: ring.clone(),
        wait: cfg.wait,
    };
    let cpu = cfg.journaller_cpu;
    let journaller = thread::Builder::new()
        .name("journaller".into())
        .spawn(move || {
            if let Some(cpu) = cpu {
                crate::pin_to_cpu(cpu);
            }
            journaller.run();
        })
        .map_err(GateError::Spawn)?;
    let replicator = match transport {
        Some(transport) => {
            let r = Replicator {
                ring: ring.clone(),
                transport,
                wait: cfg.wait,
                retry: Duration::from_micros(cfg.replica_retry_us),
            };
            let cpu = cfg.replicator_cpu;
            Some(
                thread::Builder::new()
                    .name("replicator".into())
                    .spawn(move || {
                        if let Some(cpu) = cpu {
                            crate::pin_to_cpu(cpu);
                        }
                        r.run();
                    })
                    .map_err(GateError::Spawn)?,
            )
        }
        None => None,
    };
    let gated = Gated {
        ring: ring.clone(),
        next: 0,
        sync: cfg.replica == ReplicaMode::Sync,
    };
    Ok((
        gated,
        GatedThreads {
            journaller,
            replicator,
            ring,
        },
    ))
}

impl Gated {
    pub fn stats(&self) -> GateStats {
        self.ring.stats()
    }
}

impl Journal for Gated {
    #[inline]
    fn next(&mut self) -> Option<(Command, Verdict)> {
        if self.next >= self.ring.published() {
            return None;
        }
        if self.sync && self.next >= self.ring.replicated() {
            return None;
        }
        let slot = self.ring.read(self.next);
        self.next += 1;
        self.ring.set_consumed(self.next);
        Some((slot.cmd, slot.verdict))
    }
}
