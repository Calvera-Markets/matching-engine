//! Why a resting or in-flight order was cancelled.

/// Ordinary cancel (user, reset, modify stand-in).
pub const NONE: u16 = 0;
/// Maker could not cover its fill leg at match (check-at-match).
pub const MAKER_UNCOVERED: u16 = 0x0200;
/// Taker could not cover; matching stopped and the remainder was cancelled.
pub const TAKER_UNCOVERED: u16 = 0x0201;
