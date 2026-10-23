//! Journal parameters. Every field is supplied by the control plane; there
//! are no built-in values.

use std::path::PathBuf;

use super::frame::FRAME;
use super::header::HEADER;
use super::segment::Prep;
use crate::error::WalError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalConfig {
    /// Directory holding this log's segment files, and nothing else.
    pub dir: PathBuf,
    /// Log lineage. A segment stamped with another epoch is refused.
    pub epoch: u64,
    /// Size of every segment file, header included.
    pub segment_bytes: usize,
    /// Prepared segments kept ready ahead of the active one.
    pub spares: usize,
    /// Size of the emergency segment used only when no spare is ready; it
    /// takes cancels only.
    pub reserve_bytes: usize,
    /// Reserve blocks up front. NOTE: with this off, a full disk surfaces as
    /// SIGBUS on a write into the sparse mapping, not as an error.
    pub preallocate: bool,
    /// Touch every page of a spare before the engine gets it.
    pub prefault: bool,
    /// Full segments queued for the preparer to flush and unmap.
    pub retire_queue: usize,
    /// `msync` a full segment before unmapping it.
    pub seal_sync: bool,
    /// Preparer sleep between passes when it has nothing to do.
    pub preparer_poll_us: u64,
    /// Core to pin the preparer to, or `None` to leave it to the scheduler.
    pub preparer_cpu: Option<usize>,
}

impl WalConfig {
    pub fn validate(&self) -> Result<(), WalError> {
        if self.segment_bytes < HEADER + FRAME {
            return Err(WalError::Config(format!(
                "segment_bytes {} is below one header and one frame ({})",
                self.segment_bytes,
                HEADER + FRAME
            )));
        }
        if self.reserve_bytes < HEADER + FRAME {
            return Err(WalError::Config(format!(
                "reserve_bytes {} is below one header and one frame ({})",
                self.reserve_bytes,
                HEADER + FRAME
            )));
        }
        if self.spares == 0 {
            return Err(WalError::Config("spares must be at least 1".into()));
        }
        if self.retire_queue == 0 {
            return Err(WalError::Config("retire_queue must be at least 1".into()));
        }
        if !self.dir.is_dir() {
            return Err(WalError::Config(format!(
                "wal dir {} is not a directory",
                self.dir.display()
            )));
        }
        Ok(())
    }

    pub fn prep(&self) -> Prep {
        Prep {
            preallocate: self.preallocate,
            prefault: self.prefault,
        }
    }
}
