//! SBE order-entry codec. Identity is `userRef` (u32), same as OUCH.

use ironsbe_core::header::MessageHeader;

use calvera_books::{Price, Side};

use crate::codec::sbe::order_entry::Side as SbeSide;
use crate::codec::sbe::order_entry::{
    ExecType, ExecutionReportEncoder, NewOrderSingleDecoder, OrdStatus,
    OrderCancelReplaceRequestDecoder, OrderCancelRequestDecoder, RejectEncoder, SCHEMA_ID,
};
use crate::codec::{OrderEntry, ParseOutcome, SessionId};
use crate::types::{Command, CommandType, Event, EventType};

/// Stateless SBE order-entry adapter. `Clone` is a symbol copy.
#[derive(Clone, Copy)]
pub struct SbeOe {
    symbol: [u8; 8],
}

impl SbeOe {
    pub fn new(symbol: impl AsRef<[u8]>) -> Self {
        let mut s = [0u8; 8];
        let src = symbol.as_ref();
        let n = src.len().min(8);
        s[..n].copy_from_slice(&src[..n]);
        Self { symbol: s }
    }

    fn reject(user_ref: u32, reason: u16, out: &mut [u8]) -> usize {
        let mut enc = RejectEncoder::wrap(out, 0);
        enc.set_user_ref(user_ref);
        enc.set_reason(reason);
        enc.encoded_length()
    }

    fn map_add(&self, session: SessionId, buf: &[u8], version: u16) -> Result<Command, u32> {
        let d = NewOrderSingleDecoder::wrap(buf, MessageHeader::ENCODED_LENGTH, version);
        let user_ref = d.user_ref();
        if d.symbol() != self.symbol.as_slice() {
            return Err(user_ref);
        }
        let mut cmd = Command::blank(CommandType::Add);
        cmd.client_fd = session.0;
        cmd.user_ref = user_ref;
        cmd.side = match d.side() {
            SbeSide::Buy => Side::Bid,
            SbeSide::Sell => Side::Ask,
        };
        cmd.price = Price(d.price() as u64);
        cmd.quantity = d.quantity();
        Ok(cmd)
    }

    fn map_cancel(session: SessionId, buf: &[u8], version: u16) -> Command {
        let d = OrderCancelRequestDecoder::wrap(buf, MessageHeader::ENCODED_LENGTH, version);
        let mut cmd = Command::blank(CommandType::Cancel);
        cmd.client_fd = session.0;
        cmd.user_ref = d.user_ref();
        cmd.quantity = d.quantity();
        cmd
    }

    fn map_replace(session: SessionId, buf: &[u8], version: u16) -> Command {
        let d = OrderCancelReplaceRequestDecoder::wrap(buf, MessageHeader::ENCODED_LENGTH, version);
        let mut cmd = Command::blank(CommandType::Modify);
        cmd.client_fd = session.0;
        cmd.user_ref = d.user_ref();
        cmd.side = match d.side() {
            SbeSide::Buy => Side::Bid,
            SbeSide::Sell => Side::Ask,
        };
        cmd.price = Price(d.price() as u64);
        cmd.quantity = d.quantity();
        cmd
    }
}

impl OrderEntry for SbeOe {
    fn parse(&mut self, buf: &[u8], session: SessionId, reply: &mut [u8]) -> ParseOutcome {
        if buf.len() < MessageHeader::ENCODED_LENGTH {
            return ParseOutcome::NeedMore;
        }
        let hdr = MessageHeader::wrap(buf, 0);
        let block_length = hdr.block_length;
        let template_id = hdr.template_id;
        let schema_id = hdr.schema_id;
        let version = hdr.version;
        let need = MessageHeader::ENCODED_LENGTH + block_length as usize;
        if buf.len() < need {
            return ParseOutcome::NeedMore;
        }
        if schema_id != SCHEMA_ID {
            return ParseOutcome::Bad { consumed: 1 };
        }
        match template_id {
            NewOrderSingleDecoder::TEMPLATE_ID => match self.map_add(session, buf, version) {
                Ok(cmd) => ParseOutcome::Command {
                    cmd,
                    consumed: need,
                },
                Err(user_ref) => ParseOutcome::Reply {
                    bytes: Self::reject(user_ref, 1, reply),
                    consumed: need,
                },
            },
            OrderCancelRequestDecoder::TEMPLATE_ID => ParseOutcome::Command {
                cmd: Self::map_cancel(session, buf, version),
                consumed: need,
            },
            OrderCancelReplaceRequestDecoder::TEMPLATE_ID => ParseOutcome::Command {
                cmd: Self::map_replace(session, buf, version),
                consumed: need,
            },
            _ => ParseOutcome::Bad { consumed: need },
        }
    }

    fn encode_event(&mut self, evt: &Event, out: &mut [u8]) -> usize {
        if evt.ty == EventType::BookReset || evt.client_fd < 0 {
            return 0;
        }
        let (user_ref, side, px, qty, last_px, last_qty, oid, exec_id, et, os) = match evt.ty {
            EventType::OrderAccepted => (
                evt.order.user_ref,
                evt.order.side,
                evt.order.price.0 as i64,
                evt.order.quantity,
                0i64,
                0u64,
                evt.order.order_id,
                evt.order.order_id,
                ExecType::New,
                OrdStatus::New,
            ),
            EventType::OrderCancelled => (
                evt.order.user_ref,
                evt.order.side,
                evt.order.price.0 as i64,
                evt.order.quantity,
                0,
                0,
                evt.order.order_id,
                evt.order.order_id,
                ExecType::Canceled,
                OrdStatus::Canceled,
            ),
            EventType::OrderModified => (
                evt.order.user_ref,
                evt.order.side,
                evt.order.price.0 as i64,
                evt.order.quantity,
                0,
                0,
                evt.order.order_id,
                evt.order.order_id,
                ExecType::Replaced,
                OrdStatus::Replaced,
            ),
            EventType::OrderRejected => (
                evt.reject.user_ref,
                Side::Bid,
                0,
                0,
                0,
                0,
                0,
                0,
                ExecType::Rejected,
                OrdStatus::Rejected,
            ),
            EventType::TradeExecuted => (
                evt.trade.maker_user_ref,
                evt.trade.taker_side,
                evt.trade.price.0 as i64,
                evt.trade.quantity,
                evt.trade.price.0 as i64,
                evt.trade.quantity,
                evt.trade.maker_exchange_id,
                evt.trade.match_number,
                ExecType::Trade,
                OrdStatus::PartialFill,
            ),
            EventType::BookReset => return 0,
        };
        let mut enc = ExecutionReportEncoder::wrap(out, 0);
        enc.set_user_ref(user_ref);
        enc.set_side(match side {
            Side::Bid => SbeSide::Buy,
            Side::Ask => SbeSide::Sell,
        });
        enc.set_exec_type(et);
        enc.set_ord_status(os);
        enc.set_exec_id(exec_id);
        enc.set_order_id(oid);
        enc.set_price(px);
        enc.set_quantity(qty);
        enc.set_last_px(last_px);
        enc.set_last_qty(last_qty);
        enc.set_symbol(&self.symbol);
        enc.encoded_length()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::sbe::order_entry::{
        ExecutionReportDecoder, NewOrderSingleEncoder, OrderCancelRequestEncoder,
    };
    use crate::types::EventOrder;

    fn take_cmd(outcome: ParseOutcome) -> Option<Command> {
        match outcome {
            ParseOutcome::Command { cmd, .. } => Some(cmd),
            ParseOutcome::Reply { .. }
            | ParseOutcome::Disconnect { .. }
            | ParseOutcome::NeedMore
            | ParseOutcome::Bad { .. } => None,
        }
    }

    fn encode_add(symbol: &[u8], user_ref: u32) -> Vec<u8> {
        let mut buf = vec![0u8; 64];
        let mut enc = NewOrderSingleEncoder::wrap(&mut buf, 0);
        enc.set_user_ref(user_ref);
        enc.set_symbol(symbol);
        enc.set_side(SbeSide::Buy);
        enc.set_price(100);
        enc.set_quantity(10);
        let n = enc.encoded_length();
        buf.truncate(n);
        buf
    }

    #[test]
    fn parse_add_round_trip() {
        let mut oe = SbeOe::new("AAPL");
        let wire = encode_add(b"AAPL", 7);
        let mut reply = [0u8; 128];
        let cmd = take_cmd(oe.parse(&wire, SessionId(3), &mut reply)).unwrap();
        assert_eq!(cmd.ty, CommandType::Add);
        assert_eq!(cmd.client_fd, 3);
        assert_eq!(cmd.user_ref, 7);
        assert_eq!(cmd.quantity, 10);
        assert_eq!(cmd.price, Price(100));
        assert_eq!(cmd.side, Side::Bid);
        assert!(matches!(
            oe.parse(&wire[..4], SessionId(3), &mut reply),
            ParseOutcome::NeedMore
        ));
    }

    #[test]
    fn parse_cancel_uses_user_ref() {
        let mut oe = SbeOe::new("AAPL");
        let mut buf = vec![0u8; 32];
        let mut enc = OrderCancelRequestEncoder::wrap(&mut buf, 0);
        enc.set_user_ref(7);
        enc.set_quantity(3);
        let n = enc.encoded_length();
        let mut reply = [0u8; 128];
        let cmd = take_cmd(oe.parse(&buf[..n], SessionId(1), &mut reply)).unwrap();
        assert_eq!(cmd.ty, CommandType::Cancel);
        assert_eq!(cmd.user_ref, 7);
        assert_eq!(cmd.quantity, 3);
    }

    #[test]
    fn wrong_symbol_rejects() {
        let mut oe = SbeOe::new("AAPL");
        let wire = encode_add(b"MSFT", 7);
        let mut reply = [0u8; 128];
        assert!(take_cmd(oe.parse(&wire, SessionId(1), &mut reply)).is_none());
        assert!(matches!(
            oe.parse(&wire, SessionId(1), &mut reply),
            ParseOutcome::Reply { bytes, .. } if bytes > 0
        ));
    }

    #[test]
    fn encode_accepted_is_numeric() {
        let mut oe = SbeOe::new("AAPL");
        let evt = Event::accepted(
            1,
            EventOrder {
                order_id: 42,
                user_ref: 7,
                price: Price(100),
                quantity: 10,
                side: Side::Bid,
                order_state: b'L',
                cl_ord_id: [b' '; 14],
            },
        );
        let mut out = [0u8; 128];
        let n = oe.encode_event(&evt, &mut out);
        let hdr = MessageHeader::wrap(&out[..n], 0);
        let version = hdr.version;
        let d = ExecutionReportDecoder::wrap(&out[..n], MessageHeader::ENCODED_LENGTH, version);
        assert_eq!(d.user_ref(), 7);
        assert_eq!(d.exec_id(), 42);
        assert_eq!(d.order_id(), 42);
        assert_eq!(d.exec_type(), ExecType::New);
        assert_eq!(d.quantity(), 10);
    }

    fn encode_replace(side: SbeSide) -> Vec<u8> {
        use crate::codec::sbe::order_entry::OrderCancelReplaceRequestEncoder;
        let mut buf = vec![0u8; 64];
        let mut enc = OrderCancelReplaceRequestEncoder::wrap(&mut buf, 0);
        enc.set_user_ref(7);
        enc.set_side(side);
        enc.set_price(50);
        enc.set_quantity(4);
        let n = enc.encoded_length();
        buf.truncate(n);
        buf
    }

    fn report<'a>(out: &'a [u8]) -> ExecutionReportDecoder<'a> {
        let hdr = MessageHeader::wrap(out, 0);
        ExecutionReportDecoder::wrap(out, MessageHeader::ENCODED_LENGTH, hdr.version)
    }

    #[test]
    fn sell_replace_and_the_other_reports() {
        let mut oe = SbeOe::new("AAPLTOOLONG");
        let mut sell = encode_add(b"AAPLTOOL", 3);
        // encode_add writes "AAPL" only; rebuild a sell for the truncated symbol.
        let _ = sell;
        let mut buf = vec![0u8; 64];
        let mut enc = NewOrderSingleEncoder::wrap(&mut buf, 0);
        enc.set_user_ref(3);
        enc.set_symbol(b"AAPLTOOL");
        enc.set_side(SbeSide::Sell);
        enc.set_price(40);
        enc.set_quantity(2);
        let n = enc.encoded_length();
        sell = buf[..n].to_vec();

        let mut reply = [0u8; 128];
        let cmd = take_cmd(oe.parse(&sell, SessionId(2), &mut reply)).unwrap();
        assert_eq!(cmd.side, Side::Ask);
        assert!(matches!(
            oe.parse(
                &sell[..MessageHeader::ENCODED_LENGTH],
                SessionId(2),
                &mut reply
            ),
            ParseOutcome::NeedMore
        ));

        let mut bad_schema = sell.clone();
        bad_schema[4] = 0xff;
        bad_schema[5] = 0xff;
        assert!(matches!(
            oe.parse(&bad_schema, SessionId(2), &mut reply),
            ParseOutcome::Bad { consumed: 1 }
        ));
        let mut unknown = sell.clone();
        unknown[2] = 99;
        unknown[3] = 0;
        assert!(matches!(
            oe.parse(&unknown, SessionId(2), &mut reply),
            ParseOutcome::Bad { .. }
        ));

        for side in [SbeSide::Buy, SbeSide::Sell] {
            let wire = encode_replace(side);
            let cmd = take_cmd(oe.parse(&wire, SessionId(2), &mut reply)).unwrap();
            assert_eq!(cmd.ty, CommandType::Modify);
            assert_eq!(cmd.quantity, 4);
            assert_eq!(cmd.price, Price(50));
        }

        let mut out = [0u8; 128];
        let cancelled = Event::cancelled(
            1,
            EventOrder {
                order_id: 9,
                user_ref: 3,
                price: Price(40),
                quantity: 2,
                side: Side::Ask,
                order_state: b'4',
                cl_ord_id: [b' '; 14],
            },
        );
        let n = oe.encode_event(&cancelled, &mut out);
        assert_eq!(report(&out[..n]).exec_type(), ExecType::Canceled);
        assert_eq!(report(&out[..n]).side(), SbeSide::Sell);

        let modified = Event::modified(
            1,
            EventOrder {
                order_id: 9,
                user_ref: 3,
                price: Price(50),
                quantity: 4,
                side: Side::Bid,
                order_state: b'5',
                cl_ord_id: [b' '; 14],
            },
        );
        let n = oe.encode_event(&modified, &mut out);
        assert_eq!(report(&out[..n]).exec_type(), ExecType::Replaced);

        let rejected = Event::rejected(
            1,
            crate::types::EventReject {
                user_ref: 3,
                reason: 1,
                cl_ord_id: [b' '; 14],
            },
        );
        let n = oe.encode_event(&rejected, &mut out);
        assert_eq!(report(&out[..n]).exec_type(), ExecType::Rejected);

        let trade = Event::trade(
            1,
            crate::types::EventTrade {
                match_number: 11,
                maker_exchange_id: 9,
                maker_user_ref: 3,
                price: Price(40),
                quantity: 2,
                taker_side: Side::Ask,
            },
        );
        let n = oe.encode_event(&trade, &mut out);
        let decoded = report(&out[..n]);
        assert_eq!(decoded.exec_type(), ExecType::Trade);
        assert_eq!(decoded.last_qty(), 2);
        assert_eq!(decoded.side(), SbeSide::Sell);

        assert_eq!(oe.encode_event(&Event::reset(), &mut out), 0);
        let mut hidden = cancelled;
        hidden.client_fd = -1;
        assert_eq!(oe.encode_event(&hidden, &mut out), 0);
    }
}
