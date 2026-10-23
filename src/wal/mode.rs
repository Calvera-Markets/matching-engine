//! What the journal accepts when no prepared segment is left.

use crate::types::{Command, CommandType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Mode {
    /// Every command is journalled.
    Normal = 0,
    /// Writing to the emergency reserve segment: only cancels are journalled,
    /// so resting risk can still come off the book.
    CancelOnly = 1,
    /// The reserve is full too: nothing is journalled, so nothing is applied.
    RejectAll = 2,
}

impl Mode {
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            1 => Mode::CancelOnly,
            2 => Mode::RejectAll,
            _ => Mode::Normal,
        }
    }

    /// Commands the reserve takes.
    // TODO: withdrawals (defund entries) in cancel-only mode are an open
    // decision. Entries are opaque here, so every entry is refused for now.
    #[inline]
    pub fn cancel_only_admits(cmd: &Command) -> bool {
        matches!(cmd.ty, CommandType::Cancel)
    }
}
