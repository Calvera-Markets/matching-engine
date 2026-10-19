//! `--wal-*` command-line flags for binaries that host the engine.

use std::path::PathBuf;

use super::config::WalConfig;
use crate::error::WalError;

pub const USAGE: &str = "--wal-dir DIR --wal-epoch N --wal-segment-bytes N --wal-spares N \
--wal-preallocate on|off --wal-prefault on|off";

/// Collects `--wal-*` flags; `build` fails on any missing one.
pub struct WalFlags {
    dir: Option<PathBuf>,
    epoch: Option<u64>,
    segment_bytes: Option<usize>,
    spares: Option<usize>,
    preallocate: Option<bool>,
    prefault: Option<bool>,
}

impl WalFlags {
    pub fn new() -> Self {
        Self {
            dir: None,
            epoch: None,
            segment_bytes: None,
            spares: None,
            preallocate: None,
            prefault: None,
        }
    }

    /// Whether `flag` is a WAL flag this parser owns.
    pub fn owns(flag: &str) -> bool {
        matches!(
            flag,
            "--wal-dir"
                | "--wal-epoch"
                | "--wal-segment-bytes"
                | "--wal-spares"
                | "--wal-preallocate"
                | "--wal-prefault"
        )
    }

    pub fn set(&mut self, flag: &str, value: &str) -> Result<(), WalError> {
        match flag {
            "--wal-dir" => self.dir = Some(PathBuf::from(value)),
            "--wal-epoch" => self.epoch = Some(number(flag, value)?),
            "--wal-segment-bytes" => self.segment_bytes = Some(number(flag, value)?),
            "--wal-spares" => self.spares = Some(number(flag, value)?),
            "--wal-preallocate" => self.preallocate = Some(switch(flag, value)?),
            "--wal-prefault" => self.prefault = Some(switch(flag, value)?),
            _ => return Err(WalError::Config(format!("unknown wal flag {flag}"))),
        }
        Ok(())
    }

    pub fn build(self) -> Result<WalConfig, WalError> {
        Ok(WalConfig {
            dir: need("--wal-dir", self.dir)?,
            epoch: need("--wal-epoch", self.epoch)?,
            segment_bytes: need("--wal-segment-bytes", self.segment_bytes)?,
            spares: need("--wal-spares", self.spares)?,
            preallocate: need("--wal-preallocate", self.preallocate)?,
            prefault: need("--wal-prefault", self.prefault)?,
        })
    }
}

impl Default for WalFlags {
    fn default() -> Self {
        Self::new()
    }
}

fn need<T>(flag: &str, v: Option<T>) -> Result<T, WalError> {
    v.ok_or_else(|| WalError::Config(format!("missing {flag}")))
}

fn number<T: std::str::FromStr>(flag: &str, v: &str) -> Result<T, WalError>
where
    T::Err: std::fmt::Display,
{
    v.replace('_', "")
        .parse()
        .map_err(|e| WalError::Config(format!("{flag} {v}: {e}")))
}

fn switch(flag: &str, v: &str) -> Result<bool, WalError> {
    match v {
        "on" => Ok(true),
        "off" => Ok(false),
        _ => Err(WalError::Config(format!("{flag} {v}: expected on|off"))),
    }
}
