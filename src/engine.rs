//! Matching engine hot loop: pop Command → WAL → book → push Events.

mod gate;

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use calvera_books::{BookError, OrderBook, OrderHandle, Price, Side, VecConsumer};

use gate::EngineGate;

use crate::balance::{BalanceCheck, BalanceOutput, FillCtx, NoCheck, OrderRef, PlaceCtx};
use crate::cancel_reason;
use crate::entry::Entry;
use crate::error::WalError;
use crate::reject;
use crate::snapshot::{SNAPSHOT_VERSION, Snapshot, SnapshotSink};
use crate::spsc::Spsc;
use crate::types::{Command, CommandType, Event, EventOrder, EventReject, EventTrade};
use crate::wal::{Wal, WalConfig};

struct Live {
    handle: OrderHandle,
    price: Price,
    qty: u64,
    side: Side,
    cl_ord_id: [u8; 14],
    client_fd: i32,
    user_ref: u32,
}

pub struct MatchingEngine<B: BalanceCheck = NoCheck> {
    cmds: Arc<Spsc<Command>>,
    ouch: Arc<Spsc<Event>>,
    itch: Arc<Spsc<Event>>,
    book: OrderBook<VecConsumer>,
    wal: Wal,
    by_client: HashMap<u64, Live>,
    by_handle: HashMap<OrderHandle, u64>,
    next_match: u64,
    /// Monotonic sequence stamped on balance acks and changes.
    next_ack: u64,
    publish: bool,
    wal_enabled: bool,
    slab: usize,
    check: B,
    /// Makers the check cancelled during the current sweep.
    uncovered: Vec<OrderHandle>,
}

impl MatchingEngine<NoCheck> {
    pub fn new(
        cmds: Arc<Spsc<Command>>,
        ouch: Arc<Spsc<Event>>,
        itch: Arc<Spsc<Event>>,
        wal: &WalConfig,
        slab: usize,
    ) -> Result<Self, WalError> {
        MatchingEngine::with_check(cmds, ouch, itch, wal, slab, NoCheck)
    }
}

impl<B: BalanceCheck> MatchingEngine<B> {
    /// Open an engine whose orders and entries go through `check`. The WAL is
    /// replayed through the same hooks, so `check` is rebuilt before this returns.
    pub fn with_check(
        cmds: Arc<Spsc<Command>>,
        ouch: Arc<Spsc<Event>>,
        itch: Arc<Spsc<Event>>,
        wal: &WalConfig,
        slab: usize,
        check: B,
    ) -> Result<Self, WalError> {
        let mut recovered = Vec::new();
        let wal = Wal::open(wal, |cmd| recovered.push(cmd))?;
        let mut engine = Self {
            cmds,
            ouch,
            itch,
            book: OrderBook::new(slab),
            wal,
            by_client: HashMap::with_capacity(1 << 20),
            by_handle: HashMap::with_capacity(1 << 20),
            next_match: 1,
            next_ack: 1,
            publish: false,
            wal_enabled: true,
            slab,
            check,
            uncovered: Vec::new(),
        };
        for cmd in recovered {
            engine.apply(&cmd);
        }
        engine.publish = true;
        Ok(engine)
    }

    /// Hot loop. Run on a dedicated pinned thread.
    pub fn run(&mut self, running: &AtomicBool) {
        while running.load(Ordering::Relaxed) {
            let Some(cmd) = self.cmds.pop() else {
                crate::pause();
                continue;
            };
            if cmd.ty == CommandType::Poison {
                break;
            }
            self.step(&cmd);
        }
    }

    /// One engine beat: WAL, then apply. A command the WAL refuses is
    /// rejected and not applied, so live state never runs ahead of the log.
    /// Events sit on the rings until `drain_events` (or the egress / ITCH
    /// threads) pop them.
    pub fn step(&mut self, cmd: &Command) {
        if self.wal_enabled
            && cmd.ty != CommandType::Poison
            && let Err(err) = self.wal.write(cmd)
        {
            self.refuse(cmd, journal_reject(&err));
            return;
        }
        self.apply(cmd);
    }

    /// Answer a command that was not journalled. Entries go back through the
    /// check, which owns their acks; orders get a reject.
    fn refuse(&mut self, cmd: &Command, reason: u16) {
        match cmd.ty {
            CommandType::Entry => {
                self.check.refuse_entry(&Entry::from_command(cmd), reason);
                self.emit_balance_outputs();
            }
            _ => self.reject(cmd, reason),
        }
    }

    /// Turn journalling off for throughput measurements. With it off nothing
    /// is durable and replay cannot rebuild state.
    pub fn set_wal(&mut self, on: bool) {
        self.wal_enabled = on;
    }

    pub fn wal(&self) -> &Wal {
        &self.wal
    }

    #[cfg(test)]
    pub(crate) fn wal_mut(&mut self) -> &mut Wal {
        &mut self.wal
    }

    pub fn check(&self) -> &B {
        &self.check
    }

    /// Mutable access for draining outputs the check buffers between steps.
    /// State changes that must survive replay belong in journalled entries.
    pub fn check_mut(&mut self) -> &mut B {
        &mut self.check
    }

    /// Checkpoint of open orders, match counter, and the balance check.
    /// Layout: version, match counter, open orders sorted by key, check.
    pub fn snapshot(&self) -> Snapshot {
        let mut out = SnapshotSink::new();
        out.write_u64(SNAPSHOT_VERSION);
        out.write_u64(self.next_match);
        let mut keys: Vec<u64> = self.by_client.keys().copied().collect();
        keys.sort_unstable();
        out.write_u64(keys.len() as u64);
        for key in keys {
            write_live(&mut out, key, &self.by_client[&key]);
        }
        self.check.write_snapshot(&mut out);
        out.finish()
    }

    pub fn drain_events(&mut self) -> usize {
        let mut n = 0;
        while self.ouch.pop().is_some() {
            n += 1;
        }
        while self.itch.pop().is_some() {
            n += 1;
        }
        n
    }

    fn apply(&mut self, cmd: &Command) {
        match cmd.ty {
            CommandType::Add => self.add(cmd),
            CommandType::Cancel => self.cancel(cmd),
            CommandType::Modify => self.modify(cmd),
            CommandType::Reset => self.reset(),
            CommandType::Entry => {
                self.check.apply_entry(&Entry::from_command(cmd));
                self.emit_balance_outputs();
            }
            CommandType::Poison => {}
        }
    }

    fn add(&mut self, cmd: &Command) {
        let key = cmd.composite_key();
        if self.by_client.contains_key(&key) {
            self.reject(cmd, reject::DUPLICATE_KEY);
            return;
        }
        let order = OrderRef {
            client_fd: cmd.client_fd,
            user_ref: cmd.user_ref,
        };
        let placed = self.check.on_place(&PlaceCtx {
            order,
            side: cmd.side,
            price: cmd.price,
            quantity: cmd.quantity,
        });
        if let Err(err) = placed {
            self.reject(cmd, reject::check(B::reject_code(err)));
            return;
        }

        let mut gate = EngineGate {
            check: &mut self.check,
            by_handle: &self.by_handle,
            by_client: &self.by_client,
            taker: order,
            taker_side: cmd.side,
            uncovered: &mut self.uncovered,
        };
        let added = self
            .book
            .add_limit_order_gated(cmd.side, cmd.price, cmd.quantity, &mut gate);

        match added {
            Ok(outcome) => {
                self.publish_fills(cmd);
                self.cancel_uncovered_makers();
                if let Some(handle) = outcome.handle {
                    let live = Live {
                        handle,
                        price: cmd.price,
                        qty: outcome.remaining,
                        side: cmd.side,
                        cl_ord_id: cmd.cl_ord_id,
                        client_fd: cmd.client_fd,
                        user_ref: cmd.user_ref,
                    };
                    self.by_handle.insert(handle, key);
                    self.by_client.insert(key, live);
                    self.accept(cmd, handle, outcome.remaining, b'L');
                } else {
                    self.check.on_done(order);
                    if outcome.stopped {
                        self.emit_balance_outputs();
                        self.emit_taker_uncovered(cmd, outcome.remaining);
                    }
                }
            }
            Err(err) => {
                // Matching ran before the rest failed: report those fills too.
                self.publish_fills(cmd);
                self.cancel_uncovered_makers();
                self.check.on_done(order);
                let reason = match err {
                    BookError::SlabFull => reject::SLAB_FULL,
                    _ => reject::BOOK_FAULT,
                };
                self.reject(cmd, reason);
            }
        }
    }

    /// Makers the check vetoed are already off the book; drop them here.
    fn cancel_uncovered_makers(&mut self) {
        if self.uncovered.is_empty() {
            return;
        }
        let mut uncovered = mem::take(&mut self.uncovered);
        for handle in uncovered.drain(..) {
            let Some(key) = self.by_handle.remove(&handle) else {
                continue;
            };
            let Some(live) = self.by_client.remove(&key) else {
                continue;
            };
            self.check.on_done(OrderRef {
                client_fd: live.client_fd,
                user_ref: live.user_ref,
            });
            self.emit_balance_outputs();
            self.emit_cancelled_with(&live, cancel_reason::MAKER_UNCOVERED);
        }
        self.uncovered = uncovered;
    }

    fn cancel(&mut self, cmd: &Command) {
        let key = cmd.composite_key();
        let Some(live) = self.by_client.remove(&key) else {
            return;
        };
        self.by_handle.remove(&live.handle);
        let _ = self.book.cancel_limit_order(live.handle);
        self.check.on_done(OrderRef {
            client_fd: live.client_fd,
            user_ref: live.user_ref,
        });
        self.emit_balance_outputs();
        self.emit_cancelled(&live);
    }

    fn modify(&mut self, cmd: &Command) {
        // Same stand-in as tape_replay: cancel + add (lose priority).
        self.cancel(cmd);
        let mut add = *cmd;
        add.ty = CommandType::Add;
        self.add(&add);
    }

    fn reset(&mut self) {
        self.by_client.clear();
        self.by_handle.clear();
        self.book = OrderBook::new(self.slab);
        self.check.on_reset();
        self.next_match = 1;
        // NOTE: ack sequence is not reset; standby comparison uses the snapshot, not ack ids.
        if self.publish {
            self.ouch.push(Event::reset());
        }
    }

    fn publish_fills(&mut self, taker: &Command) -> u64 {
        // Move the fill buffer out, then put the empty buffer back so the next match keeps its capacity.
        let mut fills = mem::take(&mut self.book.consumer.fills);
        let mut filled = 0u64;
        for fill in &fills {
            filled += fill.quantity;
            if let Some(&key) = self.by_handle.get(&fill.resting_id)
                && let Some(maker) = self.by_client.get_mut(&key)
            {
                let px = maker.price;
                let uref = maker.user_ref;
                let hid = maker.handle.as_u64();
                let maker_fd = maker.client_fd;
                maker.qty = maker.qty.saturating_sub(fill.quantity);
                let dead = maker.qty == 0;
                if dead {
                    self.by_client.remove(&key);
                    self.by_handle.remove(&fill.resting_id);
                }
                let maker_ref = OrderRef {
                    client_fd: maker_fd,
                    user_ref: uref,
                };
                self.check.on_fill(&FillCtx {
                    match_number: self.next_match,
                    taker: OrderRef {
                        client_fd: taker.client_fd,
                        user_ref: taker.user_ref,
                    },
                    taker_side: taker.side,
                    maker: maker_ref,
                    price: px,
                    quantity: fill.quantity,
                });
                if dead {
                    self.check.on_done(maker_ref);
                }
                let match_no = self.next_match;
                self.next_match += 1;
                self.emit_fill(
                    taker.client_fd,
                    maker_fd,
                    EventTrade {
                        match_number: match_no,
                        maker_exchange_id: hid,
                        maker_user_ref: uref,
                        price: px,
                        quantity: fill.quantity,
                        taker_side: taker.side,
                    },
                );
                continue;
            }
            let match_no = self.next_match;
            self.next_match += 1;
            self.emit_fill(
                taker.client_fd,
                taker.client_fd,
                EventTrade {
                    match_number: match_no,
                    maker_exchange_id: fill.resting_id.as_u64(),
                    maker_user_ref: 0,
                    price: taker.price,
                    quantity: fill.quantity,
                    taker_side: taker.side,
                },
            );
        }
        fills.clear();
        self.book.consumer.fills = fills;
        self.emit_balance_outputs();
        filled
    }

    fn accept(&mut self, cmd: &Command, handle: OrderHandle, qty: u64, state: u8) {
        self.emit(Event::accepted(
            cmd.client_fd,
            EventOrder {
                order_id: handle.as_u64(),
                user_ref: cmd.user_ref,
                price: cmd.price,
                quantity: qty,
                side: cmd.side,
                order_state: state,
                cl_ord_id: cmd.cl_ord_id,
                cancel_reason: 0,
            },
        ));
    }

    fn reject(&mut self, cmd: &Command, reason: u16) {
        if self.publish {
            self.ouch.push(Event::rejected(
                cmd.client_fd,
                EventReject {
                    user_ref: cmd.user_ref,
                    reason,
                    cl_ord_id: cmd.cl_ord_id,
                },
            ));
        }
    }

    fn emit_cancelled(&mut self, live: &Live) {
        self.emit_cancelled_with(live, cancel_reason::NONE);
    }

    fn emit_cancelled_with(&mut self, live: &Live, reason: u16) {
        self.emit(Event::cancelled_with_reason(
            live.client_fd,
            EventOrder {
                order_id: live.handle.as_u64(),
                user_ref: live.user_ref,
                price: live.price,
                quantity: live.qty,
                side: live.side,
                order_state: b'D',
                cl_ord_id: live.cl_ord_id,
                cancel_reason: 0,
            },
            reason,
        ));
    }

    /// The taker never rested; `order_id` 0 marks it as such.
    fn emit_taker_uncovered(&mut self, cmd: &Command, remaining: u64) {
        self.emit(Event::cancelled_with_reason(
            cmd.client_fd,
            EventOrder {
                order_id: 0,
                user_ref: cmd.user_ref,
                price: cmd.price,
                quantity: remaining,
                side: cmd.side,
                order_state: b'D',
                cl_ord_id: cmd.cl_ord_id,
                cancel_reason: 0,
            },
            cancel_reason::TAKER_UNCOVERED,
        ));
    }

    fn emit(&mut self, evt: Event) {
        if !self.publish {
            return;
        }
        match evt.ty {
            crate::types::EventType::OrderRejected
            | crate::types::EventType::BookReset
            | crate::types::EventType::BalanceAck
            | crate::types::EventType::BalanceChanged => {
                self.ouch.push(evt);
            }
            _ => {
                self.ouch.push(evt);
                self.itch.push(evt);
            }
        }
    }

    /// Private acks go to both counterparties; the public tape gets one print.
    fn emit_balance_outputs(&mut self) {
        if !self.publish {
            // Replay drains without publishing, but the ack sequence still
            // advances so numbers consumers already saw are never reissued.
            self.next_ack += self.check.take_outputs().len() as u64;
            return;
        }
        for out in self.check.take_outputs() {
            match out {
                BalanceOutput::Ack(mut ack) => {
                    ack.engine_seq = self.next_ack;
                    self.next_ack += 1;
                    self.ouch.push(Event::balance_ack(ack));
                }
                BalanceOutput::Changed(mut ch) => {
                    ch.engine_seq = self.next_ack;
                    self.next_ack += 1;
                    self.ouch.push(Event::balance_changed(ch));
                }
            }
        }
    }

    fn emit_fill(&mut self, taker_fd: i32, maker_fd: i32, trade: EventTrade) {
        if !self.publish {
            return;
        }
        let tape = Event::trade(taker_fd, trade);
        self.ouch.push(tape);
        self.itch.push(tape);
        if maker_fd != taker_fd {
            self.ouch.push(Event::trade(maker_fd, trade));
        }
    }
}

fn journal_reject(err: &WalError) -> u16 {
    match err {
        WalError::Full => reject::JOURNAL_FULL,
        _ => reject::JOURNAL_FAULT,
    }
}

fn write_live(out: &mut SnapshotSink, key: u64, live: &Live) {
    out.write_u64(key);
    out.write_u64(live.handle.as_u64());
    out.write_u64(live.price.0);
    out.write_u64(live.qty);
    out.write_u8(live.side as u8);
    out.write_bytes(&live.cl_ord_id);
    out.write_i32(live.client_fd);
    out.write_u32(live.user_ref);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TempWal;
    use crate::spsc::Spsc;
    use crate::types::{Command, CommandType, Event, EventReject, EventType};
    use crate::wal::{FRAME, HEADER};
    use calvera_books::{Fill, OrderHandle, Price, Side, SlabIndex};
    use std::num::NonZeroU32;
    use std::sync::atomic::AtomicBool;
    use std::thread;
    use std::time::Duration;

    fn order(ty: CommandType, fd: i32, user: u32, side: Side, px: u64, qty: u64) -> Command {
        let mut cmd = Command::blank(ty);
        cmd.client_fd = fd;
        cmd.user_ref = user;
        cmd.side = side;
        cmd.price = Price(px);
        cmd.quantity = qty;
        cmd
    }

    fn pop_all(q: &Spsc<Event>) -> Vec<Event> {
        let mut out = Vec::new();
        while let Some(ev) = q.pop() {
            out.push(ev);
        }
        out
    }

    type Rig = (
        MatchingEngine,
        Arc<Spsc<Command>>,
        Arc<Spsc<Event>>,
        Arc<Spsc<Event>>,
    );

    fn open(wal: &TempWal, slab: usize) -> Rig {
        let cmds = Arc::new(Spsc::new(64));
        let ouch = Arc::new(Spsc::new(64));
        let itch = Arc::new(Spsc::new(64));
        let eng = MatchingEngine::new(
            cmds.clone(),
            ouch.clone(),
            itch.clone(),
            &wal.config(64 * 1024),
            slab,
        )
        .unwrap();
        (eng, cmds, ouch, itch)
    }

    #[test]
    fn same_log_same_snapshot() {
        let wal = TempWal::new("snap");
        let run = |wal: &TempWal| {
            let (mut eng, _, _, _) = open(wal, 64);
            eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 10));
            eng.step(&order(CommandType::Add, 2, 1, Side::Ask, 100, 4));
            eng.step(&order(CommandType::Add, 1, 2, Side::Bid, 99, 3));
            eng.snapshot()
        };
        let s1 = run(&wal);
        // Reopen on the same WAL: recovery rebuilds identical state.
        let (mut eng2, _, _, _) = open(&wal, 64);
        assert_eq!(eng2.snapshot(), s1);
        // An independent run of the same tape on a fresh WAL must match.
        let wal_b = TempWal::new("snap-b");
        assert_eq!(run(&wal_b), s1);
        // A different tape gives a different snapshot.
        eng2.step(&order(CommandType::Cancel, 1, 2, Side::Bid, 0, 0));
        assert_ne!(eng2.snapshot(), s1);
    }

    #[test]
    fn a_full_log_rejects_and_live_state_matches_replay() {
        let wal = TempWal::new("full");
        let open_small = |wal: &TempWal| {
            let ouch = Arc::new(Spsc::new(256));
            let mut eng = MatchingEngine::new(
                Arc::new(Spsc::new(2)),
                ouch.clone(),
                Arc::new(Spsc::new(256)),
                &wal.config(HEADER + 4 * FRAME),
                64,
            )
            .unwrap();
            eng.wal.hold_spares(true);
            (eng, ouch)
        };
        let live = {
            let (mut eng, ouch) = open_small(&wal);
            for user in 0..8 {
                eng.step(&order(
                    CommandType::Add,
                    1,
                    user,
                    Side::Bid,
                    100 - user as u64,
                    1,
                ));
            }
            let _ = pop_all(&ouch);
            eng.step(&order(CommandType::Add, 1, 9, Side::Bid, 50, 1));
            let ev = pop_all(&ouch);
            assert_eq!(ev.len(), 1);
            assert_eq!(ev[0].ty, EventType::OrderRejected);
            assert_eq!(ev[0].reject.reason, reject::JOURNAL_FULL);
            eng.step(&order(CommandType::Cancel, 1, 0, Side::Bid, 0, 0));
            assert_eq!(pop_all(&ouch)[0].ty, EventType::OrderRejected);
            eng.snapshot()
        };
        let (eng, _) = open_small(&wal);
        assert_eq!(eng.snapshot(), live);
    }

    #[test]
    fn rejects_a_duplicate_client_key() {
        let wal = TempWal::new("dup");
        let (mut eng, _, ouch, itch) = open(&wal, 64);
        eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 10));
        eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 101, 3));
        let private = pop_all(&ouch);
        assert_eq!(private[0].ty, EventType::OrderAccepted);
        assert_eq!(private[1].ty, EventType::OrderRejected);
        assert_eq!(private[1].reject.reason, 0);
        assert!(
            pop_all(&itch)
                .iter()
                .all(|e| e.ty != EventType::OrderRejected)
        );
    }

    #[test]
    fn partial_fill_leaves_the_maker_and_a_crossing_bid_rests() {
        let wal = TempWal::new("partial");
        let (mut eng, _, ouch, itch) = open(&wal, 64);
        eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 10));
        eng.step(&order(CommandType::Add, 2, 2, Side::Ask, 100, 4));
        eng.step(&order(CommandType::Cancel, 1, 1, Side::Bid, 0, 0));
        let private = pop_all(&ouch);
        let cancel = private
            .iter()
            .find(|e| e.ty == EventType::OrderCancelled)
            .unwrap();
        assert_eq!(cancel.order.quantity, 6);
        assert!(
            private
                .iter()
                .filter(|e| e.ty == EventType::OrderAccepted)
                .all(|e| e.order.user_ref == 1)
        );
        assert_eq!(
            pop_all(&itch)
                .iter()
                .filter(|e| e.ty == EventType::TradeExecuted)
                .count(),
            1
        );

        eng.step(&order(CommandType::Add, 3, 3, Side::Ask, 100, 5));
        eng.step(&order(CommandType::Add, 4, 4, Side::Bid, 100, 12));
        let private = pop_all(&ouch);
        let rest = private
            .iter()
            .find(|e| e.ty == EventType::OrderAccepted && e.order.user_ref == 4)
            .unwrap();
        assert_eq!(rest.order.quantity, 7);
        assert_eq!(rest.order.order_state, b'L');
    }

    #[test]
    fn cancel_modify_and_reset() {
        let wal = TempWal::new("lifecycle");
        let (mut eng, _, ouch, itch) = open(&wal, 64);
        eng.step(&order(CommandType::Cancel, 9, 9, Side::Bid, 0, 0));
        assert!(ouch.pop().is_none());

        eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 10));
        eng.step(&order(CommandType::Modify, 1, 1, Side::Ask, 110, 4));
        let private = pop_all(&ouch);
        assert_eq!(private[1].ty, EventType::OrderCancelled);
        assert_eq!(private[2].ty, EventType::OrderAccepted);
        assert_eq!(private[2].order.quantity, 4);
        assert_eq!(private[2].order.side, Side::Ask);

        eng.step(&Command::reset());
        let private = pop_all(&ouch);
        assert_eq!(private.last().unwrap().ty, EventType::BookReset);
        assert!(pop_all(&itch).iter().all(|e| e.ty != EventType::BookReset));

        eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 80, 1));
        assert_eq!(ouch.pop().unwrap().ty, EventType::OrderAccepted);
    }

    #[test]
    fn slab_full_rejects_with_reason_one() {
        let wal = TempWal::new("slab");
        let (mut eng, _, ouch, _) = open(&wal, 2);
        eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 1));
        eng.step(&order(CommandType::Add, 1, 2, Side::Bid, 101, 1));
        let private = pop_all(&ouch);
        assert_eq!(private[1].ty, EventType::OrderRejected);
        assert_eq!(private[1].reject.reason, 1);
        eng.step(&order(CommandType::Add, 1, 3, Side::Ask, 200, 1));
        assert_eq!(ouch.pop().unwrap().ty, EventType::OrderAccepted);
    }

    #[test]
    fn wal_can_be_disabled_and_poison_is_not_applied() {
        let wal = TempWal::new("wal-off");
        let (mut eng, _, ouch, _) = open(&wal, 64);
        eng.set_wal(false);
        eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 1));
        assert_eq!(eng.wal.seq(), 0);
        assert_eq!(ouch.pop().unwrap().ty, EventType::OrderAccepted);

        eng.set_wal(true);
        eng.step(&order(CommandType::Add, 1, 2, Side::Ask, 101, 1));
        assert_eq!(eng.wal.seq(), 1);
        let _ = pop_all(&ouch);

        let before = eng.wal.seq();
        eng.step(&Command::poison());
        assert_eq!(eng.wal.seq(), before);
        assert!(ouch.pop().is_none());
    }

    #[test]
    fn drain_empties_both_rings() {
        let wal = TempWal::new("drain");
        let (mut eng, _, ouch, itch) = open(&wal, 64);
        eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 10));
        eng.step(&order(CommandType::Add, 2, 2, Side::Ask, 100, 10));
        assert!(eng.drain_events() > 0);
        assert!(ouch.pop().is_none());
        assert!(itch.pop().is_none());
        assert_eq!(eng.drain_events(), 0);
    }

    #[test]
    fn run_spins_until_poison() {
        let wal = TempWal::new("spin");
        let (mut eng, cmds, _, _) = open(&wal, 64);
        let running = AtomicBool::new(true);
        thread::scope(|s| {
            s.spawn(|| eng.run(&running));
            thread::sleep(Duration::from_millis(20));
            cmds.push(Command::poison());
        });
    }

    #[test]
    fn recovery_replays_without_publishing() {
        let wal = TempWal::new("recover");
        {
            let (mut eng, _, ouch, _) = open(&wal, 64);
            eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 10));
            eng.step(&order(CommandType::Add, 2, 2, Side::Ask, 100, 4));
            eng.step(&order(CommandType::Add, 1, 1, Side::Bid, 100, 1));
            eng.step(&Command::reset());
            eng.step(&order(CommandType::Add, 3, 3, Side::Ask, 50, 2));
            assert!(ouch.pop().is_some());
        }
        let (mut eng, _, ouch, itch) = open(&wal, 64);
        assert!(ouch.pop().is_none());
        assert!(itch.pop().is_none());
        eng.step(&order(CommandType::Cancel, 3, 3, Side::Ask, 0, 0));
        let ev = ouch.pop().unwrap();
        assert_eq!(ev.ty, EventType::OrderCancelled);
        assert_eq!(ev.order.user_ref, 3);
        assert_eq!(ev.order.quantity, 2);
    }

    #[test]
    fn an_untracked_fill_prints_once_and_a_quiet_engine_prints_nothing() {
        let wal = TempWal::new("orphan");
        let (mut eng, _, ouch, itch) = open(&wal, 64);
        let handle = OrderHandle::new(Side::Bid, SlabIndex::new(1), NonZeroU32::new(1).unwrap());
        let taker = order(CommandType::Add, 9, 9, Side::Ask, 100, 4);

        eng.publish = false;
        eng.book.consumer.fills.push(Fill {
            resting_id: handle,
            quantity: 4,
        });
        assert_eq!(eng.publish_fills(&taker), 4);
        assert!(ouch.pop().is_none());
        eng.emit(Event::rejected(
            1,
            EventReject {
                user_ref: 1,
                reason: 3,
                cl_ord_id: [b' '; 14],
            },
        ));
        assert!(ouch.pop().is_none());

        eng.publish = true;
        eng.by_handle.insert(handle, 99);
        eng.book.consumer.fills.push(Fill {
            resting_id: handle,
            quantity: 4,
        });
        assert_eq!(eng.publish_fills(&taker), 4);
        let ev = ouch.pop().unwrap();
        assert_eq!(ev.ty, EventType::TradeExecuted);
        assert_eq!(ev.client_fd, 9);
        assert_eq!(ev.trade.maker_user_ref, 0);
        assert_eq!(ev.trade.quantity, 4);
        assert!(itch.pop().is_some());
        assert!(ouch.pop().is_none());

        eng.emit(Event::rejected(
            4,
            EventReject {
                user_ref: 8,
                reason: 3,
                cl_ord_id: [b'R'; 14],
            },
        ));
        let ev = ouch.pop().unwrap();
        assert_eq!(ev.ty, EventType::OrderRejected);
        assert_eq!(ev.reject.reason, 3);
        assert!(itch.pop().is_none());
        eng.emit(Event::reset());
        assert_eq!(ouch.pop().unwrap().ty, EventType::BookReset);
        assert!(itch.pop().is_none());
    }

    #[test]
    fn missing_wal_directory_fails() {
        let mut cfg = TempWal::new("missing").config(4096);
        cfg.dir = std::path::PathBuf::from("/no/such/me-engine-dir");
        let err = MatchingEngine::new(
            Arc::new(Spsc::new(2)),
            Arc::new(Spsc::new(2)),
            Arc::new(Spsc::new(2)),
            &cfg,
            64,
        );
        assert!(matches!(err, Err(WalError::Config(_))));
    }

    #[test]
    fn new_opens_an_empty_wal() {
        let wal = TempWal::new("new");
        let eng = MatchingEngine::new(
            Arc::new(Spsc::new(2)),
            Arc::new(Spsc::new(2)),
            Arc::new(Spsc::new(2)),
            &wal.config(4096),
            64,
        )
        .unwrap();
        assert_eq!(eng.wal().seq(), 0);
    }
}
