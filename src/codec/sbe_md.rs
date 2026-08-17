//! SBE public tape. One message per datagram.

use calvera_books::Side;

use crate::codec::sbe::market_data::{AddEncoder, DeleteEncoder, ExecuteEncoder};
use crate::codec::sbe::market_data::Side as SbeSide;
use crate::codec::MarketData;
use crate::types::{Event, EventType};

const CAP: usize = 64;

/// SBE market-data packer. `push` holds one encoded message until `take`.
pub struct SbeMd {
    symbol: [u8; 8],
    buf: [u8; CAP],
    len: usize,
}

impl SbeMd {
    pub fn new(symbol: impl AsRef<[u8]>) -> Self {
        let mut s = [0u8; 8];
        let src = symbol.as_ref();
        let n = src.len().min(8);
        s[..n].copy_from_slice(&src[..n]);
        Self {
            symbol: s,
            buf: [0; CAP],
            len: 0,
        }
    }

    fn encode(evt: &Event, symbol: &[u8; 8], out: &mut [u8]) -> usize {
        match evt.ty {
            EventType::OrderAccepted => {
                let mut enc = AddEncoder::wrap(out, 0);
                enc.set_order_id(evt.order.order_id);
                enc.set_side(match evt.order.side {
                    Side::Bid => SbeSide::Buy,
                    Side::Ask => SbeSide::Sell,
                });
                enc.set_symbol(symbol);
                enc.set_price(evt.order.price.0 as i64);
                enc.set_quantity(evt.order.quantity);
                enc.encoded_length()
            }
            EventType::TradeExecuted => {
                let mut enc = ExecuteEncoder::wrap(out, 0);
                enc.set_order_id(evt.trade.maker_exchange_id);
                enc.set_match_number(evt.trade.match_number);
                enc.set_quantity(evt.trade.quantity);
                enc.encoded_length()
            }
            EventType::OrderCancelled => {
                let mut enc = DeleteEncoder::wrap(out, 0);
                enc.set_order_id(evt.order.order_id);
                enc.encoded_length()
            }
            _ => 0,
        }
    }
}

impl MarketData for SbeMd {
    fn push(&mut self, evt: &Event) -> bool {
        if self.len != 0 {
            return false;
        }
        let n = Self::encode(evt, &self.symbol, &mut self.buf);
        if n == 0 {
            return true;
        }
        self.len = n;
        true
    }

    fn take(&mut self) -> Option<&[u8]> {
        if self.len == 0 {
            return None;
        }
        let n = self.len;
        self.len = 0;
        Some(&self.buf[..n])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::sbe::market_data::{AddDecoder, ExecuteDecoder};
    use crate::types::EventOrder;
    use calvera_books::Price;
    use ironsbe_core::header::MessageHeader;

    #[test]
    fn add_event_round_trip() {
        let mut md = SbeMd::new(b"AAPL");
        let evt = Event::accepted(
            1,
            EventOrder {
                order_id: 42,
                user_ref: 1,
                price: Price(100),
                quantity: 10,
                side: Side::Bid,
                order_state: b'L',
                cl_ord_id: [b' '; 14],
            },
        );
        assert!(md.push(&evt));
        assert!(!md.push(&evt));
        let bytes = md.take().expect("datagram").to_vec();
        assert!(md.take().is_none());

        let hdr = MessageHeader::wrap(bytes.as_slice(), 0);
        let template_id = hdr.template_id;
        let version = hdr.version;
        assert_eq!(template_id, AddDecoder::TEMPLATE_ID);
        let dec = AddDecoder::wrap(&bytes, MessageHeader::ENCODED_LENGTH, version);
        assert_eq!(dec.order_id(), 42);
        assert_eq!(dec.side(), SbeSide::Buy);
        assert_eq!(dec.symbol_as_str(), "AAPL");
        assert_eq!(dec.price(), 100);
        assert_eq!(dec.quantity(), 10);
    }

    #[test]
    fn execute_fields() {
        use crate::types::EventTrade;
        let mut md = SbeMd::new(b"AAPL");
        let evt = Event::trade(
            1,
            EventTrade {
                match_number: 9,
                maker_exchange_id: 42,
                maker_user_ref: 1,
                price: Price(100),
                quantity: 3,
                taker_side: Side::Ask,
            },
        );
        assert!(md.push(&evt));
        let bytes = md.take().unwrap().to_vec();
        let dec = ExecuteDecoder::wrap(&bytes, MessageHeader::ENCODED_LENGTH, 0);
        assert_eq!(dec.order_id(), 42);
        assert_eq!(dec.match_number(), 9);
        assert_eq!(dec.quantity(), 3);
    }
}
