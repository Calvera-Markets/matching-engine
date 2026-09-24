//! ITCH 5.0 + MoldUDP64 packing for the public tape.

use calvera_books::Side;

use crate::codec::MarketData;
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

impl MarketData for Packet {
    fn push(&mut self, evt: &Event) -> bool {
        Packet::push(self, evt)
    }

    fn take(&mut self) -> Option<&[u8]> {
        Packet::take(self)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::MarketData;
    use crate::types::{Event, EventOrder, EventReject, EventTrade, EventType};
    use calvera_books::{Price, Side};

    fn order(side: Side) -> EventOrder {
        EventOrder {
            order_id: 0x11,
            user_ref: 1,
            price: Price(50),
            quantity: 8,
            side,
            order_state: b'L',
            cl_ord_id: [b'I'; 14],
        }
    }

    fn trade() -> Event {
        Event::trade(
            1,
            EventTrade {
                match_number: 4,
                maker_exchange_id: 0x22,
                maker_user_ref: 2,
                price: Price(50),
                quantity: 3,
                taker_side: Side::Ask,
            },
        )
    }

    #[test]
    fn packs_add_execute_and_delete_and_skips_the_rest() {
        let mut pkt = Packet::new();
        assert!(pkt.empty());
        assert!(pkt.take().is_none());

        let bid = Event::accepted(1, order(Side::Bid));
        let ask = Event::accepted(1, order(Side::Ask));
        assert!(pkt.push(&bid));
        assert!(pkt.push(&ask));
        assert!(!pkt.empty());
        let bytes = pkt.take().unwrap().to_vec();
        assert_eq!(&bytes[..10], b"OB_SESSION");
        assert_eq!(u64::from_be_bytes(bytes[10..18].try_into().unwrap()), 1);
        assert_eq!(u16::from_be_bytes(bytes[18..20].try_into().unwrap()), 2);
        assert_eq!(bytes[22], b'A');
        assert_eq!(bytes[22 + 15], b'B');
        assert!(pkt.empty());

        assert!(pkt.push(&trade()));
        assert!(pkt.push(&Event::cancelled(1, order(Side::Bid))));
        let bytes = pkt.take().unwrap().to_vec();
        assert_eq!(u64::from_be_bytes(bytes[10..18].try_into().unwrap()), 3);
        assert_eq!(bytes[22], b'E');

        assert!(pkt.push(&Event::rejected(
            1,
            EventReject {
                user_ref: 1,
                reason: 0,
                cl_ord_id: [b' '; 14],
            },
        )));
        assert!(pkt.push(&Event::modified(1, order(Side::Bid))));
        assert!(pkt.push(&Event::reset()));
        assert!(pkt.empty());
        assert_eq!(Event::reset().ty, EventType::BookReset);

        assert!(MarketData::push(&mut pkt, &bid));
        assert!(MarketData::take(&mut pkt).is_some());
    }

    #[test]
    fn returns_false_when_the_packet_is_full() {
        let mut pkt = Packet::new();
        let ev = Event::accepted(1, order(Side::Bid));
        let mut n = 0;
        while pkt.push(&ev) {
            n += 1;
            assert!(n < 100, "packet never filled");
        }
        assert!(n > 1);
        assert!(pkt.take().is_some());
    }

    #[test]
    fn short_buffers_encode_nothing() {
        let ev = Event::accepted(1, order(Side::Bid));
        assert_eq!(add(&ev, &mut [0u8; 8]), 0);
        assert_eq!(executed(&trade(), &mut [0u8; 8]), 0);
        assert_eq!(delete(&ev, &mut [0u8; 8]), 0);

        let mut wide = [0u8; 64];
        assert_eq!(add(&Event::accepted(1, order(Side::Ask)), &mut wide), 36);
        assert_eq!(wide[15], b'S');
        assert_eq!(executed(&trade(), &mut wide), 31);
        assert_eq!(wide[0], b'E');
        assert_eq!(delete(&ev, &mut wide), 19);
        assert_eq!(wide[0], b'D');
    }
}
