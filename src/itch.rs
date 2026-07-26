//! ITCH 5.0 + MoldUDP64 packing for the public tape.

use calvera_books::Side;

use crate::types::{Event, EventType};

const LOCATE: u16 = 0x0100;
const MTU: usize = 1400;

#[repr(C, packed)]
struct MoldHeader {
    session: [u8; 10],
    seq: u64,
    count: u16,
}

pub struct Packet {
    buf: [u8; MTU],
    off: usize,
    count: u16,
    seq: u64,
}

impl Packet {
    pub fn new() -> Self {
        let mut p = Self {
            buf: [0; MTU],
            off: size_of::<MoldHeader>(),
            count: 0,
            seq: 1,
        };
        p.buf[..10].copy_from_slice(b"OB_SESSION");
        p
    }

    pub fn empty(&self) -> bool {
        self.count == 0
    }

    pub fn push(&mut self, evt: &Event) -> bool {
        let mut tmp = [0u8; 64];
        let n = encode_itch(evt, &mut tmp);
        if n == 0 {
            return true;
        }
        let need = 2 + n;
        if self.off + need > MTU {
            return false;
        }
        self.buf[self.off..self.off + 2].copy_from_slice(&(n as u16).to_be_bytes());
        self.off += 2;
        self.buf[self.off..self.off + n].copy_from_slice(&tmp[..n]);
        self.off += n;
        self.count += 1;
        true
    }

    pub fn take(&mut self) -> Option<&[u8]> {
        if self.count == 0 {
            return None;
        }
        self.buf[10..18].copy_from_slice(&self.seq.to_be_bytes());
        self.buf[18..20].copy_from_slice(&self.count.to_be_bytes());
        let n = self.off;
        self.seq += self.count as u64;
        self.off = size_of::<MoldHeader>();
        self.count = 0;
        Some(&self.buf[..n])
    }
}

fn encode_itch(evt: &Event, out: &mut [u8]) -> usize {
    match evt.ty {
        EventType::OrderAccepted => add(evt, out),
        EventType::TradeExecuted => executed(evt, out),
        EventType::OrderCancelled => delete(evt, out),
        _ => 0,
    }
}

fn add(evt: &Event, out: &mut [u8]) -> usize {
    const N: usize = 36;
    if out.len() < N {
        return 0;
    }
    out[..N].fill(0);
    out[0] = b'A';
    out[1..3].copy_from_slice(&LOCATE.to_be_bytes());
    out[7..15].copy_from_slice(&evt.order.order_id.to_be_bytes());
    out[15] = match evt.order.side {
        Side::Bid => b'B',
        Side::Ask => b'S',
    };
    out[16..20].copy_from_slice(&(evt.order.quantity as u32).to_be_bytes());
    out[20..28].fill(b' ');
    out[28..32].copy_from_slice(&(evt.order.price.0 as u32).to_be_bytes());
    N
}

fn executed(evt: &Event, out: &mut [u8]) -> usize {
    const N: usize = 31;
    if out.len() < N {
        return 0;
    }
    out[..N].fill(0);
    out[0] = b'E';
    out[1..3].copy_from_slice(&LOCATE.to_be_bytes());
    out[7..15].copy_from_slice(&evt.trade.maker_exchange_id.to_be_bytes());
    out[15..19].copy_from_slice(&(evt.trade.quantity as u32).to_be_bytes());
    out[19..27].copy_from_slice(&evt.trade.match_number.to_be_bytes());
    N
}

fn delete(evt: &Event, out: &mut [u8]) -> usize {
    const N: usize = 19;
    if out.len() < N {
        return 0;
    }
    out[..N].fill(0);
    out[0] = b'D';
    out[1..3].copy_from_slice(&LOCATE.to_be_bytes());
    out[7..15].copy_from_slice(&evt.order.order_id.to_be_bytes());
    N
}
