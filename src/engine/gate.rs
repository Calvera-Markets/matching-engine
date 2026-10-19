//! Bridges the book's per-fill veto to the balance check.

use std::collections::HashMap;

use calvera_books::{Admit, MakerRef, MatchGate, OrderHandle, Side};

use super::Live;
use crate::balance::{BalanceCheck, MatchCtx, OrderRef, Verdict};

pub(super) struct EngineGate<'a, B: BalanceCheck> {
    pub check: &'a mut B,
    pub by_handle: &'a HashMap<OrderHandle, u64>,
    pub by_client: &'a HashMap<u64, Live>,
    pub taker: OrderRef,
    pub taker_side: Side,
    /// Makers cancelled for lack of cover during this sweep.
    pub uncovered: &'a mut Vec<OrderHandle>,
}

impl<B: BalanceCheck> MatchGate for EngineGate<'_, B> {
    #[inline(always)]
    fn admit(&mut self, maker: MakerRef, fill_qty: u64) -> Admit {
        if !B::GATES {
            return Admit::Fill;
        }
        // NOTE: an order the engine does not track (never happens for engine-placed orders) is admitted.
        let Some(live) = self
            .by_handle
            .get(&maker.handle)
            .and_then(|key| self.by_client.get(key))
        else {
            return Admit::Fill;
        };
        let verdict = self.check.admit(&MatchCtx {
            taker: self.taker,
            taker_side: self.taker_side,
            maker: OrderRef {
                client_fd: live.client_fd,
                user_ref: live.user_ref,
            },
            price: maker.price,
            quantity: fill_qty,
        });
        match verdict {
            Verdict::Fill => Admit::Fill,
            Verdict::CancelMaker => {
                self.uncovered.push(maker.handle);
                Admit::CancelMaker
            }
            Verdict::StopTaker => Admit::StopTaker,
        }
    }
}
