//! Matching engine hot loop: pop Command → WAL → book → push Events.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use calvera_books::{BookError, OrderBook, OrderHandle, Price, Side, VecConsumer};

use crate::spsc::Spsc;
use crate::types::{Command, CommandType, Event, EventOrder, EventReject, EventTrade};
use crate::wal::Wal;

struct Live {
    handle: OrderHandle,
    price: Price,
    qty: u64,
    side: Side,
    cl_ord_id: [u8; 14],
    client_fd: i32,
    user_ref: u32,
}

pub struct MatchingEngine {
    cmds: Arc<Spsc<Command>>,
    ouch: Arc<Spsc<Event>>,
    itch: Arc<Spsc<Event>>,
    book: OrderBook<VecConsumer>,
    wal: Wal,
    by_client: HashMap<u64, Live>,
    by_handle: HashMap<OrderHandle, u64>,
    next_match: u64,
    publish: bool,
    wal_enabled: bool,
    slab: usize,
}

impl MatchingEngine {
    pub fn new(
        cmds: Arc<Spsc<Command>>,
        ouch: Arc<Spsc<Event>>,
        itch: Arc<Spsc<Event>>,
        wal_path: &Path,
        slab: usize,
    ) -> std::io::Result<Self> {
        Self::with_wal_size(cmds, ouch, itch, wal_path, slab, crate::wal::WAL_SIZE)
    }

    pub fn with_wal_size(
        cmds: Arc<Spsc<Command>>,
        ouch: Arc<Spsc<Event>>,
        itch: Arc<Spsc<Event>>,
        wal_path: &Path,
        slab: usize,
        wal_size: usize,
    ) -> std::io::Result<Self> {
        let mut engine = Self {
            cmds,
            ouch,
            itch,
            book: OrderBook::new(slab),
            wal: Wal::open_sized(wal_path, wal_size)?,
            by_client: HashMap::with_capacity(1 << 20),
            by_handle: HashMap::with_capacity(1 << 20),
            next_match: 1,
            publish: false,
            wal_enabled: true,
            slab,
        };
        let mut recovered = Vec::new();
        engine.wal.recover(|cmd| recovered.push(cmd));
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

    /// One engine beat: optional WAL, then apply. Events sit on the rings
    /// until `drain_events` (or the egress / ITCH threads) pop them.
    pub fn step(&mut self, cmd: &Command) {
        if self.wal_enabled && cmd.ty != CommandType::Poison {
            self.wal.write(cmd);
        }
        self.apply(cmd);
    }

    pub fn set_wal(&mut self, on: bool) {
        self.wal_enabled = on;
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
            CommandType::Poison => {}
        }
    }

    fn add(&mut self, cmd: &Command) {
        let key = cmd.composite_key();
        if self.by_client.contains_key(&key) {
            self.reject(cmd, 0);
            return;
        }

        match self.book.add_limit_order(cmd.side, cmd.price, cmd.quantity) {
            Ok(resting) => {
                let filled = self.publish_fills(cmd);
                if let Some(handle) = resting {
                    let remaining = cmd.quantity.saturating_sub(filled);
                    let live = Live {
                        handle,
                        price: cmd.price,
                        qty: remaining,
                        side: cmd.side,
                        cl_ord_id: cmd.cl_ord_id,
                        client_fd: cmd.client_fd,
                        user_ref: cmd.user_ref,
                    };
                    self.by_handle.insert(handle, key);
                    self.by_client.insert(key, live);
                    self.accept(cmd, handle, remaining, b'L');
                }
            }
            Err(BookError::SlabFull) => self.reject(cmd, 1),
            Err(_) => self.reject(cmd, 0),
        }
    }

    fn cancel(&mut self, cmd: &Command) {
        let key = cmd.composite_key();
        let Some(live) = self.by_client.remove(&key) else {
            return;
        };
        self.by_handle.remove(&live.handle);
        let _ = self.book.cancel_limit_order(live.handle);
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
        self.next_match = 1;
        if self.publish {
            self.ouch.push(Event::reset());
        }
    }

    fn publish_fills(&mut self, taker: &Command) -> u64 {
        let fills: Vec<_> = self.book.consumer.fills.drain(..).collect();
        let mut filled = 0u64;
        for fill in &fills {
            filled += fill.quantity;
            if let Some(&key) = self.by_handle.get(&fill.resting_id) {
                if let Some(maker) = self.by_client.get_mut(&key) {
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
        self.emit(Event::cancelled(
            live.client_fd,
            EventOrder {
                order_id: live.handle.as_u64(),
                user_ref: live.user_ref,
                price: live.price,
                quantity: live.qty,
                side: live.side,
                order_state: b'D',
                cl_ord_id: live.cl_ord_id,
            },
        ));
    }

    fn emit(&mut self, evt: Event) {
        if !self.publish {
            return;
        }
        match evt.ty {
            crate::types::EventType::OrderRejected | crate::types::EventType::BookReset => {
                self.ouch.push(evt);
            }
            _ => {
                self.ouch.push(evt);
                self.itch.push(evt);
            }
        }
    }

    /// Private acks go to both counterparties; the public tape gets one print.
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
