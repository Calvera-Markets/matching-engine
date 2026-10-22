//! Crate errors.

use std::fmt;
use std::io;
use std::path::PathBuf;

/// Journal failures. A command whose write fails is never applied.
#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    /// Neither a spare nor room in the emergency reserve: nothing is journalled.
    Full,
    /// The log is on the emergency reserve, which takes cancels only.
    CancelOnly,
    /// A frame written with another layout version.
    Version {
        seq: u64,
        found: u8,
    },
    /// A frame's sequence does not follow the previous frame.
    Gap {
        expected: u64,
        found: u64,
    },
    /// An invalid or missing frame that is not the torn tail of the log.
    Corrupt {
        segment: u64,
        offset: usize,
    },
    /// Configuration from the control plane is missing or unusable.
    Config(String),
    /// A segment stamped with another log lineage.
    StaleEpoch {
        index: u64,
        found: u64,
        expected: u64,
    },
    /// Segment indexes do not run without a gap.
    MissingSegment {
        expected: u64,
        found: u64,
    },
    /// A segment header does not link to the last frame before it.
    BrokenChain {
        index: u64,
    },
    /// A segment header that is neither blank nor valid, with frames after it.
    BadHeader {
        path: PathBuf,
    },
    /// A segment header with a layout version this build does not read.
    SegmentVersion {
        found: u8,
    },
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "wal io: {e}"),
            WalError::Full => write!(f, "wal full: no spare and no reserve room"),
            WalError::CancelOnly => write!(f, "wal on the emergency reserve: cancels only"),
            WalError::Version { seq, found } => {
                write!(f, "wal frame {seq} has layout version {found}")
            }
            WalError::Gap { expected, found } => {
                write!(f, "wal sequence gap: expected {expected}, found {found}")
            }
            WalError::Corrupt { segment, offset } => write!(
                f,
                "wal segment {segment}: frame at byte {offset} is invalid and is not the tail"
            ),
            WalError::Config(msg) => write!(f, "wal config: {msg}"),
            WalError::StaleEpoch {
                index,
                found,
                expected,
            } => write!(
                f,
                "wal segment {index} has epoch {found}, expected {expected}"
            ),
            WalError::MissingSegment { expected, found } => {
                write!(f, "wal segment {expected} missing; found {found}")
            }
            WalError::BrokenChain { index } => {
                write!(
                    f,
                    "wal segment {index} does not link to the segment before it"
                )
            }
            WalError::BadHeader { path } => {
                write!(f, "wal segment {} has an invalid header", path.display())
            }
            WalError::SegmentVersion { found } => {
                write!(f, "wal segment header has layout version {found}")
            }
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}
