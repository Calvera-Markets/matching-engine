//! OUCH 5.0 subset — inbound parse, outbound serialize.

use calvera_books::{Price, Side};

use crate::types::{Command, CommandType, Event, EventType};

pub const ENTER: u8 = b'O';
pub const CANCEL: u8 = b'X';
pub const MODIFY: u8 = b'M';

pub const ENTER_LEN: usize = 47;
pub const CANCEL_LEN: usize = 11;
pub const MODIFY_LEN: usize = 12;
pub const MAX_MSG: usize = ENTER_LEN;

pub fn parse(buf: &[u8], fd: i32) -> Option<(Command, usize)> {
    if buf.is_empty() {
        return None;
    }
    match buf[0] {
        ENTER if buf.len() >= ENTER_LEN => Some((parse_enter(buf, fd), ENTER_LEN)),
        CANCEL if buf.len() >= CANCEL_LEN => Some((parse_cancel(buf, fd), CANCEL_LEN)),
        MODIFY if buf.len() >= MODIFY_LEN => Some((parse_modify(buf, fd), MODIFY_LEN)),
        _ => None,
    }
}

fn parse_enter(buf: &[u8], fd: i32) -> Command {
    let mut cl_ord_id = [b' '; 14];
    cl_ord_id.copy_from_slice(&buf[31..45]);
    Command {
        ty: CommandType::Add,
        client_fd: fd,
        user_ref: u32::from_be_bytes(buf[1..5].try_into().unwrap()),
        side: if buf[5] == b'B' { Side::Bid } else { Side::Ask },
        quantity: u32::from_be_bytes(buf[6..10].try_into().unwrap()) as u64,
        price: Price(u64::from_be_bytes(buf[18..26].try_into().unwrap())),
        time_in_force: buf[26],
        display: buf[27],
        capacity: buf[28],
        cl_ord_id,
    }
}

fn parse_cancel(buf: &[u8], fd: i32) -> Command {
    Command {
        ty: CommandType::Cancel,
        client_fd: fd,
        user_ref: u32::from_be_bytes(buf[1..5].try_into().unwrap()),
        quantity: u32::from_be_bytes(buf[5..9].try_into().unwrap()) as u64,
        side: Side::Bid,
        price: Price(0),
        cl_ord_id: [b' '; 14],
        time_in_force: 0,
        display: 0,
        capacity: 0,
    }
}

fn parse_modify(buf: &[u8], fd: i32) -> Command {
    Command {
        ty: CommandType::Modify,
        client_fd: fd,
        user_ref: u32::from_be_bytes(buf[1..5].try_into().unwrap()),
        side: if buf[5] == b'B' { Side::Bid } else { Side::Ask },
        quantity: u32::from_be_bytes(buf[6..10].try_into().unwrap()) as u64,
        price: Price(0),
        cl_ord_id: [b' '; 14],
        time_in_force: 0,
        display: 0,
        capacity: 0,
    }
}

/// Serialize an engine Event to an outbound OUCH message. Returns bytes written.
pub fn serialize(evt: &Event, out: &mut [u8]) -> usize {
    match evt.ty {
        EventType::OrderAccepted => write_accepted(evt, out),
        EventType::OrderCancelled => write_canceled(evt, out),
        EventType::OrderModified => write_modified(evt, out),
        EventType::OrderRejected => write_rejected(evt, out),
        EventType::TradeExecuted => write_executed(evt, out),
        EventType::BookReset => 0,
    }
}

fn write_accepted(evt: &Event, out: &mut [u8]) -> usize {
    // 'A' + ts(8) + userRef(4) + side(1) + qty(4) + symbol(8) + price(8)
    // + tif(1) + display(1) + orderRef(8) + cap(1) + iso(1) + cross(1)
    // + state(1) + clOrd(14) + app(2) = 64
    const N: usize = 64;
    if out.len() < N {
        return 0;
    }
    out[..N].fill(0);
    out[0] = b'A';
    out[9..13].copy_from_slice(&evt.order.user_ref.to_be_bytes());
    out[13] = match evt.order.side {
        Side::Bid => b'B',
        Side::Ask => b'S',
    };
    out[14..18].copy_from_slice(&(evt.order.quantity as u32).to_be_bytes());
    out[18..26].fill(b' ');
    out[26..34].copy_from_slice(&evt.order.price.0.to_be_bytes());
    out[37..45].copy_from_slice(&evt.order.order_id.to_be_bytes());
    out[48] = evt.order.order_state;
    out[49..63].copy_from_slice(&evt.order.cl_ord_id);
    N
}

fn write_canceled(evt: &Event, out: &mut [u8]) -> usize {
    const N: usize = 20;
    if out.len() < N {
        return 0;
    }
    out[..N].fill(0);
    out[0] = b'C';
    out[9..13].copy_from_slice(&evt.order.user_ref.to_be_bytes());
    out[13..17].copy_from_slice(&(evt.order.quantity as u32).to_be_bytes());
    out[17] = b'U';
    N
}

fn write_modified(evt: &Event, out: &mut [u8]) -> usize {
    const N: usize = 19;
    if out.len() < N {
        return 0;
    }
    out[..N].fill(0);
    out[0] = b'M';
    out[9..13].copy_from_slice(&evt.order.user_ref.to_be_bytes());
    out[13] = match evt.order.side {
        Side::Bid => b'B',
        Side::Ask => b'S',
    };
    out[14..18].copy_from_slice(&(evt.order.quantity as u32).to_be_bytes());
    N
}

fn write_rejected(evt: &Event, out: &mut [u8]) -> usize {
    const N: usize = 32;
    if out.len() < N {
        return 0;
    }
    out[..N].fill(0);
    out[0] = b'J';
    out[9..13].copy_from_slice(&evt.reject.user_ref.to_be_bytes());
    out[13..15].copy_from_slice(&evt.reject.reason.to_be_bytes());
    out[15..29].copy_from_slice(&evt.reject.cl_ord_id);
    N
}

fn write_executed(evt: &Event, out: &mut [u8]) -> usize {
    const N: usize = 38;
    if out.len() < N {
        return 0;
    }
    out[..N].fill(0);
    out[0] = b'E';
    out[9..13].copy_from_slice(&evt.trade.maker_user_ref.to_be_bytes());
    out[13..17].copy_from_slice(&(evt.trade.quantity as u32).to_be_bytes());
    out[17..25].copy_from_slice(&evt.trade.price.0.to_be_bytes());
    out[26..34].copy_from_slice(&evt.trade.match_number.to_be_bytes());
    N
}
