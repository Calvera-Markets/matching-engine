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
    ((fd as u32 as u64) << 32) | user_ref as u64
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
        }
    }

    pub fn modified(fd: i32, order: EventOrder) -> Self {
        Self {
            ty: EventType::OrderModified,
            client_fd: fd,
            timestamp: 0,
            order,
            trade: empty_trade(),
            reject: empty_reject(),
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
