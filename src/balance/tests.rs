use std::sync::Arc;

use calvera_books::{Price, Side};

use super::{BalanceCheck, FillCtx, OrderRef, PlaceCtx};
use crate::TempWal;
use crate::engine::MatchingEngine;
use crate::entry::Entry;
use crate::reject;
use crate::spsc::Spsc;
use crate::types::{Command, CommandType, Event, EventType};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Place(PlaceCtx),
    Fill(FillCtx),
    Done(OrderRef),
    Entry(Entry),
    Reset,
}

/// Records every hook; refuses orders above `max_qty`.
#[derive(Debug, Default)]
struct Recorder {
    max_qty: u64,
    calls: Vec<Call>,
}

impl BalanceCheck for Recorder {
    type Error = u8;

    fn on_place(&mut self, order: &PlaceCtx) -> Result<(), u8> {
        self.calls.push(Call::Place(*order));
        if order.quantity > self.max_qty {
            return Err(7);
        }
        Ok(())
    }

    fn on_fill(&mut self, fill: &FillCtx) {
        self.calls.push(Call::Fill(*fill));
    }

    fn on_done(&mut self, order: OrderRef) {
        self.calls.push(Call::Done(order));
    }

    fn apply_entry(&mut self, entry: &Entry) {
        self.calls.push(Call::Entry(*entry));
    }

    fn on_reset(&mut self) {
        self.calls.push(Call::Reset);
    }

    fn reject_code(err: u8) -> u8 {
        err
    }

    fn write_snapshot(&self, out: &mut crate::snapshot::SnapshotSink) {
        out.write_u64(self.max_qty);
    }
}

type Rig = (MatchingEngine<Recorder>, Arc<Spsc<Event>>);

fn open(wal: &TempWal, slab: usize, max_qty: u64) -> Rig {
    let ouch = Arc::new(Spsc::new(256));
    let eng = MatchingEngine::with_check(
        Arc::new(Spsc::new(8)),
        ouch.clone(),
        Arc::new(Spsc::new(256)),
        &wal.0,
        slab,
        64 * 1024,
        Recorder {
            max_qty,
            calls: Vec::new(),
        },
    )
    .unwrap();
    (eng, ouch)
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

fn order(fd: i32, user: u32) -> OrderRef {
    OrderRef {
        client_fd: fd,
        user_ref: user,
    }
}

fn drain(q: &Spsc<Event>) -> Vec<Event> {
    let mut out = Vec::new();
    while let Some(e) = q.pop() {
        out.push(e);
    }
    out
}

#[test]
fn a_refused_order_never_reaches_the_book() {
    let wal = TempWal::new("check-refuse");
    let (mut eng, ouch) = open(&wal, 64, 5);
    eng.step(&add(1, 1, Side::Bid, 100, 6));
    let ev = drain(&ouch);
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].ty, EventType::OrderRejected);
    assert_eq!(ev[0].reject.reason, reject::check(7));
    assert_eq!(reject::check_code(ev[0].reject.reason), Some(7));
    eng.step(&add(2, 1, Side::Ask, 100, 1));
    assert!(
        drain(&ouch)
            .iter()
            .all(|e| e.ty != EventType::TradeExecuted)
    );
}

#[test]
fn hooks_follow_the_order_lifecycle() {
    let wal = TempWal::new("check-life");
    let (mut eng, _) = open(&wal, 64, 100);
    eng.step(&add(1, 10, Side::Ask, 100, 3));
    eng.step(&add(2, 20, Side::Bid, 101, 5));
    let mut cancel = Command::blank(CommandType::Cancel);
    cancel.client_fd = 2;
    cancel.user_ref = 20;
    eng.step(&cancel);

    let calls = &eng.check().calls;
    assert!(matches!(calls[0], Call::Place(p) if p.order == order(1, 10)));
    assert!(matches!(calls[1], Call::Place(p) if p.order == order(2, 20)));
    assert_eq!(
        calls[2],
        Call::Fill(FillCtx {
            match_number: 1,
            taker: order(2, 20),
            taker_side: Side::Bid,
            maker: order(1, 10),
            price: Price(100),
            quantity: 3,
        })
    );
    assert_eq!(calls[3], Call::Done(order(1, 10)));
    assert_eq!(calls[4], Call::Done(order(2, 20)));
    assert_eq!(calls.len(), 5);
}

#[test]
fn a_fully_filled_taker_is_done_without_resting() {
    let wal = TempWal::new("check-taker");
    let (mut eng, _) = open(&wal, 64, 100);
    eng.step(&add(1, 10, Side::Ask, 100, 5));
    eng.step(&add(2, 20, Side::Bid, 100, 5));
    let calls = &eng.check().calls;
    assert_eq!(calls[3], Call::Done(order(1, 10)));
    assert_eq!(calls[4], Call::Done(order(2, 20)));
}

#[test]
fn fills_before_a_full_slab_are_still_reported() {
    let wal = TempWal::new("check-slab");
    let (mut eng, ouch) = open(&wal, 2, 100);
    eng.step(&add(1, 1, Side::Ask, 100, 1));
    eng.step(&add(1, 2, Side::Bid, 90, 1));
    drain(&ouch);
    eng.step(&add(2, 3, Side::Bid, 100, 4));
    let ev = drain(&ouch);
    assert!(ev.iter().any(|e| e.ty == EventType::TradeExecuted));
    let rej = ev
        .iter()
        .find(|e| e.ty == EventType::OrderRejected)
        .unwrap();
    assert_eq!(rej.reject.reason, reject::SLAB_FULL);
    let calls = &eng.check().calls;
    assert!(calls.iter().any(|c| matches!(c, Call::Fill(_))));
    assert_eq!(calls.last(), Some(&Call::Done(order(2, 3))));
}

#[test]
fn entries_are_journalled_and_replayed_through_the_check() {
    let wal = TempWal::new("check-replay");
    let entry = Entry {
        kind: 3,
        tag: 1,
        channel: 9,
        words: [11, 22, 33, 44],
    };
    let live = {
        let (mut eng, _) = open(&wal, 64, 100);
        eng.step(&entry.to_command());
        eng.step(&add(1, 1, Side::Bid, 100, 2));
        eng.step(&Command::reset());
        eng.check().calls.clone()
    };
    assert_eq!(live[0], Call::Entry(entry));
    assert_eq!(live.last(), Some(&Call::Reset));
    let (eng, _) = open(&wal, 64, 100);
    assert_eq!(eng.check().calls, live);
}
