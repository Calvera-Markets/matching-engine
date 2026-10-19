use std::convert::Infallible;

use super::{BalanceCheck, FillCtx, OrderRef, PlaceCtx};
use crate::entry::Entry;
use crate::hash::{SnapshotSink, StateHasher};

/// No balance check. Every hook is an empty inline body, so an engine built
/// with it compiles to the same code as an engine without hooks.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoCheck;

const _: () = assert!(size_of::<NoCheck>() == 0);

impl BalanceCheck for NoCheck {
    type Error = Infallible;

    #[inline(always)]
    fn on_place(&mut self, _order: &PlaceCtx) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn on_fill(&mut self, _fill: &FillCtx) {}

    #[inline(always)]
    fn on_done(&mut self, _order: OrderRef) {}

    /// Entries are journalled and replayed but have no effect without a check.
    #[inline(always)]
    fn apply_entry(&mut self, _entry: &Entry) {}

    #[inline(always)]
    fn on_reset(&mut self) {}

    fn reject_code(err: Self::Error) -> u8 {
        match err {}
    }

    #[inline(always)]
    fn hash_into(&self, _h: &mut StateHasher) {}

    #[inline(always)]
    fn write_snapshot(&self, _out: &mut SnapshotSink) {}
}
