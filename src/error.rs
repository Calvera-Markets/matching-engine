//! Crate errors.

use std::fmt;
use std::io;

/// Journal failures. A command whose write fails is never applied.
#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    /// No room left for another frame.
    Full,
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
    /// An invalid frame with valid data after it, so not a torn tail.
    Corrupt {
        offset: usize,
    },
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "wal io: {e}"),
            WalError::Full => write!(f, "wal full"),
            WalError::Version { seq, found } => {
                write!(f, "wal frame {seq} has layout version {found}")
            }
            WalError::Gap { expected, found } => {
                write!(f, "wal sequence gap: expected {expected}, found {found}")
            }
            WalError::Corrupt { offset } => {
                write!(
                    f,
                    "wal frame at byte {offset} is corrupt and is not the tail"
                )
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
