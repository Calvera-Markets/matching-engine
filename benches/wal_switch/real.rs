//! The engine's own WAL (segments, preparer thread, pointer-swap roll) under
//! the same measurement as the synthetic writer.

use std::io;
use std::path::PathBuf;

use matching_engine::types::{Command, CommandType};
use matching_engine::{Wal, WalConfig, WalError};

use crate::args::{Args, Prep};

pub struct RealWal {
    wal: Wal,
    dir: PathBuf,
    segment: u64,
    stalls: usize,
}

impl RealWal {
    pub fn open(args: &Args, poll_us: u64) -> io::Result<Self> {
        if args.prep == Prep::Hot {
            return Err(io::Error::other(
                "--writer wal always prepares on the helper; use sparse|prealloc|prefault|both",
            ));
        }
        let dir = args.dir.join(format!("wal-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let cfg = WalConfig {
            dir: dir.clone(),
            epoch: 1,
            segment_bytes: args.segment_bytes,
            spares: args.spares,
            preallocate: args.prep.preallocates(),
            prefault: args.prep.prefaults(),
            retire_queue: args.segments,
            seal_sync: false,
            preparer_poll_us: poll_us,
            preparer_cpu: args.cpu_helper,
        };
        let wal = Wal::open(&cfg, |_| {}).map_err(io::Error::other)?;
        let segment = wal.segment();
        Ok(Self {
            wal,
            dir,
            segment,
            stalls: 0,
        })
    }

    #[inline]
    pub fn write(&mut self, seq: u64) -> io::Result<bool> {
        let mut cmd = Command::blank(CommandType::Add);
        cmd.user_ref = seq as u32;
        cmd.quantity = seq;
        loop {
            match self.wal.write(&cmd) {
                Ok(()) => break,
                Err(WalError::Full) => {
                    self.stalls += 1;
                    std::hint::spin_loop();
                }
                Err(e) => return Err(io::Error::other(e)),
            }
        }
        let switched = self.wal.segment() != self.segment;
        self.segment = self.wal.segment();
        Ok(switched)
    }

    pub fn finish(self) -> usize {
        let stalls = self.stalls;
        drop(self.wal);
        let _ = std::fs::remove_dir_all(&self.dir);
        stalls
    }
}
