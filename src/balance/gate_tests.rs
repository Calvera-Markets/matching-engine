//! Check-at-match verdicts through the engine.

use std::collections::HashMap;
use std::sync::Arc;

use calvera_books::{Price, Side};

use super::{BalanceCheck, FillCtx, MatchCtx, OrderRef, PlaceCtx, Verdict};
use crate::TempWal;
use crate::cancel_reason;
use crate::engine::MatchingEngine;
use crate::entry::Entry;
use crate::spsc::Spsc;
use crate::types::{Command, CommandType, Event, EventType};

/// Each session may fill at most `budget[fd]` units in total across all
/// its orders; the budget is spent inside `admit`.
#[derive(Debug, Default)]
struct Budget {
    budget: HashMap<i32, u64>,
    done: Vec<OrderRef>,
    fills: Vec<FillCtx>,
}

impl BalanceCheck for Budget {
    type Error = u8;
    const GATES: bool = true;

    fn admit(&mut self, m: &MatchCtx) -> Verdict {
        let maker = self.budget.get(&m.maker.client_fd).copied();
        let taker = self.budget.get(&m.taker.client_fd).copied();
        match (maker, taker) {
            (Some(mk), _) if mk < m.quantity => Verdict::CancelMaker,
            (_, Some(tk)) if tk < m.quantity => Verdict::StopTaker,
            _ => {
                for fd in [m.maker.client_fd, m.taker.client_fd] {
                    if let Some(b) = self.budget.get_mut(&fd) {
                        *b -= m.quantity;
                    }
                }
                Verdict::Fill
            }
        }
    }

    fn on_place(&mut self, _order: &PlaceCtx) -> Result<(), u8> {
        Ok(())
    }

    fn on_fill(&mut self, fill: &FillCtx) {
        self.fills.push(*fill);
    }

    fn on_done(&mut self, order: OrderRef) {
        self.done.push(order);
    }

    /// `kind` 9: set `budget[words[0]] = words[1]`.
    fn apply_entry(&mut self, entry: &Entry) {
        if entry.kind == 9 {
            self.budget.insert(entry.words[0] as i32, entry.words[1]);
        }
    }

    fn on_reset(&mut self) {}

    fn reject_code(err: u8) -> u8 {
        err
    }

    fn hash_into(&self, h: &mut crate::hash::StateHasher) {
        let mut b: Vec<_> = self.budget.iter().collect();
        b.sort_unstable();
        for (fd, v) in b {
            h.write_i32(*fd);
            h.write_u64(*v);
        }
    }

    fn write_snapshot(&self, _out: &mut crate::hash::SnapshotSink) {}
}

fn open(wal: &TempWal) -> (MatchingEngine<Budget>, Arc<Spsc<Event>>) {
    let ouch = Arc::new(Spsc::new(256));
    let eng = MatchingEngine::with_check(
        Arc::new(Spsc::new(8)),
        ouch.clone(),
        Arc::new(Spsc::new(256)),
        &wal.0,
        64,
        64 * 1024,
        Budget::default(),
    )
    .unwrap();
    (eng, ouch)
}

fn budget(fd: i32, units: u64) -> Command {
    Entry {
        kind: 9,
        tag: 0,
        channel: 0,
        words: [fd as u64, units, 0, 0],
    }
    .to_command()
}

fn add(fd: i32, user: u32, side: Side, px: u64, qty: u64) -> Command {
    let mut c = Command::blank(CommandType::Add);
    c.client_fd = fd;
    c.user_ref = user;
    c.side = side;
    c.price = Price(px);
    c.quantity = qty;
    c
}

fn drain(q: &Spsc<Event>) -> Vec<Event> {
    std::iter::from_fn(|| q.pop()).collect()
}

fn of(ev: &[Event], ty: EventType) -> Vec<Event> {
    ev.iter().filter(|e| e.ty == ty).copied().collect()
}

#[test]
fn uncovered_maker_is_cancelled_whole_and_matching_continues() {
    let wal = TempWal::new("gate-maker");
    let (mut eng, ouch) = open(&wal);
    eng.step(&budget(2, 1));
    eng.step(&add(2, 1, Side::Ask, 100, 4));
    eng.step(&add(3, 1, Side::Ask, 100, 4));
    drain(&ouch);
    eng.step(&add(1, 1, Side::Bid, 100, 3));
    let ev = drain(&ouch);

    let trades = of(&ev, EventType::TradeExecuted);
    assert!(trades.iter().all(|t| t.trade.quantity == 3));
    assert!(trades.iter().all(|t| t.trade.maker_user_ref == 1));
    let cancels = of(&ev, EventType::OrderCancelled);
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0].client_fd, 2);
    assert_eq!(cancels[0].order.quantity, 4);
    assert_eq!(
        cancels[0].order.cancel_reason,
        cancel_reason::MAKER_UNCOVERED
    );
    assert!(eng.check().done.contains(&OrderRef {
        client_fd: 2,
        user_ref: 1
    }));
    assert_eq!(eng.check().fills.len(), 1);
    assert_eq!(eng.check().fills[0].maker.client_fd, 3);

    // The cancelled maker is gone from the engine too: a user cancel is a no-op.
    let mut cancel = Command::blank(CommandType::Cancel);
    cancel.client_fd = 2;
    cancel.user_ref = 1;
    eng.step(&cancel);
    assert!(drain(&ouch).is_empty());
}

#[test]
fn uncovered_taker_stops_and_its_remainder_is_cancelled() {
    let wal = TempWal::new("gate-taker");
    let (mut eng, ouch) = open(&wal);
    eng.step(&budget(1, 2));
    eng.step(&add(2, 1, Side::Ask, 100, 2));
    eng.step(&add(3, 1, Side::Ask, 101, 2));
    drain(&ouch);
    eng.step(&add(1, 7, Side::Bid, 101, 5));
    let ev = drain(&ouch);

    let fills = &eng.check().fills;
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity, 2);
    let cancels = of(&ev, EventType::OrderCancelled);
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0].client_fd, 1);
    assert_eq!(cancels[0].order.user_ref, 7);
    assert_eq!(cancels[0].order.quantity, 3);
    assert_eq!(cancels[0].order.order_id, 0);
    assert_eq!(
        cancels[0].order.cancel_reason,
        cancel_reason::TAKER_UNCOVERED
    );
    assert!(of(&ev, EventType::OrderAccepted).is_empty());

    // Nothing rested for the taker: an ask at its limit finds no bid.
    eng.step(&add(4, 1, Side::Ask, 100, 1));
    assert!(of(&drain(&ouch), EventType::TradeExecuted).is_empty());
    // The maker at 101 the taker stopped on is still there, behind the new ask.
    eng.step(&add(5, 1, Side::Bid, 101, 3));
    let ev = drain(&ouch);
    assert!(
        of(&ev, EventType::TradeExecuted)
            .iter()
            .any(|t| t.trade.quantity == 2 && t.trade.price == Price(101))
    );
}

#[test]
fn gated_replay_rebuilds_the_same_state_hash() {
    let wal = TempWal::new("gate-replay");
    let live = {
        let (mut eng, _) = open(&wal);
        eng.step(&budget(1, 3));
        eng.step(&budget(2, 1));
        eng.step(&add(2, 1, Side::Ask, 100, 4));
        eng.step(&add(3, 1, Side::Ask, 100, 4));
        eng.step(&add(1, 1, Side::Bid, 100, 5));
        eng.step(&add(4, 1, Side::Bid, 99, 2));
        eng.state_hash()
    };
    let (eng, _) = open(&wal);
    assert_eq!(eng.state_hash(), live);
}
