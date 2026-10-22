//! Pluggable balance check run inside the engine's single writer.
//!
//! The engine calls the check at fixed points of an order's life and hands it
//! every journalled [`Entry`]. Because the hooks also run during WAL replay,
//! the check's state is rebuilt exactly on restart.

mod null;

pub use null::NoCheck;

use calvera_books::{Price, Side};

use crate::entry::Entry;
use crate::snapshot::SnapshotSink;

/// Engine identity of an order: the session it came from and its reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderRef {
    pub client_fd: i32,
    pub user_ref: u32,
}

/// An order about to enter matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaceCtx {
    pub order: OrderRef,
    pub side: Side,
    pub price: Price,
    pub quantity: u64,
}

/// One executed fill, after the book applied it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillCtx {
    pub match_number: u64,
    pub taker: OrderRef,
    pub taker_side: Side,
    pub maker: OrderRef,
    pub price: Price,
    pub quantity: u64,
}

pub trait BalanceCheck {
    type Error: Copy;

    /// Static check before matching. `Err` rejects the order untouched.
    fn on_place(&mut self, order: &PlaceCtx) -> Result<(), Self::Error>;

    /// Both legs of a fill. Called once per fill, maker and taker together.
    fn on_fill(&mut self, fill: &FillCtx);

    /// The order is gone: fully filled, cancelled, or never rested.
    fn on_done(&mut self, order: OrderRef);

    /// A journalled entry addressed to the check.
    fn apply_entry(&mut self, entry: &Entry);

    /// The book was reset; every open order is gone.
    fn on_reset(&mut self);

    /// Check-specific reject code, reported as `reject::check(code)`.
    fn reject_code(err: Self::Error) -> u8;

    /// Write durable check state after the book section of a snapshot.
    /// Transient buffers are skipped.
    fn write_snapshot(&self, out: &mut SnapshotSink);
}

#[cfg(test)]
mod tests;
