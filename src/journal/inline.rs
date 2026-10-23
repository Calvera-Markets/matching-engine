//! Journal on the engine thread, then apply.

use std::sync::Arc;

use super::{Journal, Verdict};
use crate::spsc::Spsc;
use crate::types::{Command, CommandType};
use crate::wal::Wal;

pub struct Inline {
    cmds: Arc<Spsc<Command>>,
    wal: Wal,
    enabled: bool,
}

impl Inline {
    pub fn new(cmds: Arc<Spsc<Command>>, wal: Wal) -> Self {
        Self {
            cmds,
            wal,
            enabled: true,
        }
    }

    /// Journal one command. Poison is never journalled.
    #[inline(always)]
    pub fn record(&mut self, cmd: &Command) -> Verdict {
        if !self.enabled || cmd.ty == CommandType::Poison {
            return Verdict::Apply;
        }
        match self.wal.write(cmd) {
            Ok(()) => Verdict::Apply,
            Err(e) => Verdict::of(Err(e)),
        }
    }

    pub fn wal(&self) -> &Wal {
        &self.wal
    }

    pub fn wal_mut(&mut self) -> &mut Wal {
        &mut self.wal
    }

    /// Turn journalling off for throughput measurements. With it off nothing
    /// is durable and replay cannot rebuild state.
    pub fn set_enabled(&mut self, on: bool) {
        self.enabled = on;
    }
}

impl Journal for Inline {
    #[inline(always)]
    fn next(&mut self) -> Option<(Command, Verdict)> {
        let cmd = self.cmds.pop()?;
        let verdict = self.record(&cmd);
        Some((cmd, verdict))
    }
}
