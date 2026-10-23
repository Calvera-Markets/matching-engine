//! Command in, Event out. The book never sees these.

use calvera_books::{Price, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandType {
    Add = 0,
    Modify = 1,
    Cancel = 2,
    Reset = 3,
    Poison = 4,
    /// Journalled entry for the balance check; see `entry::Entry`.
    Entry = 5,
}

#[derive(Debug, Clone, Copy)]
pub struct Command {
    pub ty: CommandType,
    pub side: Side,
    pub price: Price,
    pub quantity: u64,
    pub client_fd: i32,
    pub user_ref: u32,
    pub cl_ord_id: [u8; 14],
    pub time_in_force: u8,
    pub display: u8,
    pub capacity: u8,
}

impl Command {
    pub fn blank(ty: CommandType) -> Self {
        Self {
            ty,
            side: Side::Bid,
            price: Price(0),
            quantity: 0,
            client_fd: 1,
            user_ref: 0,
            cl_ord_id: [b' '; 14],
            time_in_force: 0,
            display: 0,
            capacity: 0,
        }
    }

    pub fn poison() -> Self {
        Self::blank(CommandType::Poison)
    }

    pub fn reset() -> Self {
        Self::blank(CommandType::Reset)
    }

    pub fn composite_key(&self) -> u64 {
        composite_key(self.client_fd, self.user_ref)
    }
}

pub fn composite_key(fd: i32, user_ref: u32) -> u64 {
    ((fd as u32 as u64) << 32) | u64::from(user_ref)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventType {
    OrderAccepted,
    OrderCancelled,
    OrderModified,
    OrderRejected,
    TradeExecuted,
    BookReset,
    /// Terminal result of one journalled balance entry.
    BalanceAck,
    /// Slice after an applied fund/defund/lock/unlock/settle leg.
    BalanceChanged,
}

#[derive(Debug, Clone, Copy)]
pub struct EventOrder {
    pub order_id: u64,
    pub user_ref: u32,
    pub price: Price,
    pub quantity: u64,
    pub side: Side,
    pub order_state: u8,
    pub cl_ord_id: [u8; 14],
    /// Meaningful on `OrderCancelled`. See `cancel_reason`.
    pub cancel_reason: u16,
}

#[derive(Debug, Clone, Copy)]
pub struct EventTrade {
    pub match_number: u64,
    pub maker_exchange_id: u64,
    pub maker_user_ref: u32,
    pub price: Price,
    pub quantity: u64,
    pub taker_side: Side,
}

#[derive(Debug, Clone, Copy)]
pub struct EventReject {
    pub user_ref: u32,
    pub reason: u16,
    pub cl_ord_id: [u8; 14],
}

/// Terminal ack for one `CommandType::Entry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventBalanceAck {
    pub channel: u32,
    pub seq: u64,
    pub engine_seq: u64,
    /// 0 = Applied, 1 = Refused, 2 = Duplicate.
    pub kind: u8,
    pub reason: u16,
}

/// One slice after an applied balance change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventBalanceChanged {
    pub account: u64,
    pub asset: u8,
    pub balance: u64,
    pub locked: u64,
    pub engine_seq: u64,
}

/// 64-byte slot so producer/consumer do not false-share a ring entry.
#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct Event {
    pub ty: EventType,
    pub client_fd: i32,
    pub timestamp: u64,
    pub order: EventOrder,
    pub trade: EventTrade,
    pub reject: EventReject,
    pub balance_ack: EventBalanceAck,
    pub balance_changed: EventBalanceChanged,
}

impl Event {
    pub fn accepted(fd: i32, order: EventOrder) -> Self {
        Self {
            ty: EventType::OrderAccepted,
            client_fd: fd,
            timestamp: 0,
            order,
            trade: empty_trade(),
            reject: empty_reject(),
            balance_ack: empty_ack(),
            balance_changed: empty_changed(),
        }
    }

    pub fn cancelled(fd: i32, order: EventOrder) -> Self {
        Self {
            ty: EventType::OrderCancelled,
            client_fd: fd,
            timestamp: 0,
            order,
            trade: empty_trade(),
            reject: empty_reject(),
            balance_ack: empty_ack(),
            balance_changed: empty_changed(),
        }
    }

    pub fn cancelled_with_reason(fd: i32, mut order: EventOrder, reason: u16) -> Self {
        order.cancel_reason = reason;
        Self::cancelled(fd, order)
    }

    pub fn modified(fd: i32, order: EventOrder) -> Self {
        Self {
            ty: EventType::OrderModified,
            client_fd: fd,
            timestamp: 0,
            order,
            trade: empty_trade(),
            reject: empty_reject(),
            balance_ack: empty_ack(),
            balance_changed: empty_changed(),
        }
    }

    pub fn rejected(fd: i32, reject: EventReject) -> Self {
        Self {
            ty: EventType::OrderRejected,
            client_fd: fd,
            timestamp: 0,
            order: empty_order(),
            trade: empty_trade(),
            reject,
            balance_ack: empty_ack(),
            balance_changed: empty_changed(),
        }
    }

    pub fn trade(fd: i32, trade: EventTrade) -> Self {
        Self {
            ty: EventType::TradeExecuted,
            client_fd: fd,
            timestamp: 0,
            order: empty_order(),
            trade,
            reject: empty_reject(),
            balance_ack: empty_ack(),
            balance_changed: empty_changed(),
        }
    }

    pub fn balance_ack(ack: EventBalanceAck) -> Self {
        Self {
            ty: EventType::BalanceAck,
            client_fd: -1,
            timestamp: 0,
            order: empty_order(),
            trade: empty_trade(),
            reject: empty_reject(),
            balance_ack: ack,
            balance_changed: empty_changed(),
        }
    }

    pub fn balance_changed(change: EventBalanceChanged) -> Self {
        Self {
            ty: EventType::BalanceChanged,
            client_fd: -1,
            timestamp: 0,
            order: empty_order(),
            trade: empty_trade(),
            reject: empty_reject(),
            balance_ack: empty_ack(),
            balance_changed: change,
        }
    }

    pub fn reset() -> Self {
        Self {
            ty: EventType::BookReset,
            client_fd: -1,
            timestamp: 0,
            order: empty_order(),
            trade: empty_trade(),
            reject: empty_reject(),
            balance_ack: empty_ack(),
            balance_changed: empty_changed(),
        }
    }
}

fn empty_order() -> EventOrder {
    EventOrder {
        order_id: 0,
        user_ref: 0,
        price: Price(0),
        quantity: 0,
        side: Side::Bid,
        order_state: 0,
        cl_ord_id: [b' '; 14],
        cancel_reason: 0,
    }
}

fn empty_ack() -> EventBalanceAck {
    EventBalanceAck {
        channel: 0,
        seq: 0,
        engine_seq: 0,
        kind: 0,
        reason: 0,
    }
}

fn empty_changed() -> EventBalanceChanged {
    EventBalanceChanged {
        account: 0,
        asset: 0,
        balance: 0,
        locked: 0,
        engine_seq: 0,
    }
}

fn empty_trade() -> EventTrade {
    EventTrade {
        match_number: 0,
        maker_exchange_id: 0,
        maker_user_ref: 0,
        price: Price(0),
        quantity: 0,
        taker_side: Side::Bid,
    }
}

fn empty_reject() -> EventReject {
    EventReject {
        user_ref: 0,
        reason: 0,
        cl_ord_id: [b' '; 14],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use calvera_books::{Price, Side};

    #[test]
    fn command_builders_and_the_client_key() {
        let add = Command::blank(CommandType::Add);
        assert_eq!(add.ty, CommandType::Add);
        assert_eq!(add.side, Side::Bid);
        assert_eq!(Command::poison().ty, CommandType::Poison);
        assert_eq!(Command::reset().ty, CommandType::Reset);
        assert_eq!(add.composite_key(), composite_key(1, 0));
        assert_eq!(composite_key(7, 3), (7u64 << 32) | 3);
    }

    #[test]
    fn each_event_constructor_sets_its_type() {
        let order = EventOrder {
            order_id: 1,
            user_ref: 2,
            price: Price(3),
            quantity: 4,
            side: Side::Ask,
            order_state: b'L',
            cl_ord_id: [b'Z'; 14],
            cancel_reason: 0,
        };
        assert_eq!(Event::accepted(1, order).ty, EventType::OrderAccepted);
        assert_eq!(Event::cancelled(1, order).ty, EventType::OrderCancelled);
        assert_eq!(
            Event::cancelled_with_reason(1, order, 0x200)
                .order
                .cancel_reason,
            0x200
        );
        assert_eq!(Event::modified(1, order).ty, EventType::OrderModified);
        assert_eq!(
            Event::rejected(
                1,
                EventReject {
                    user_ref: 2,
                    reason: 5,
                    cl_ord_id: [b'Z'; 14],
                },
            )
            .ty,
            EventType::OrderRejected
        );
        assert_eq!(
            Event::trade(
                1,
                EventTrade {
                    match_number: 1,
                    maker_exchange_id: 2,
                    maker_user_ref: 3,
                    price: Price(4),
                    quantity: 5,
                    taker_side: Side::Bid,
                },
            )
            .ty,
            EventType::TradeExecuted
        );
        assert_eq!(
            Event::balance_ack(EventBalanceAck {
                channel: 1,
                seq: 2,
                engine_seq: 3,
                kind: 0,
                reason: 0,
            })
            .ty,
            EventType::BalanceAck
        );
        let changed = Event::balance_changed(EventBalanceChanged {
            account: 9,
            asset: 1,
            balance: 50,
            locked: 10,
            engine_seq: 4,
        });
        assert_eq!(changed.ty, EventType::BalanceChanged);
        assert_eq!(changed.balance_changed.balance, 50);
        assert_eq!(Event::reset().ty, EventType::BookReset);
    }
}
