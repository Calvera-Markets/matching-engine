//! SBE order-entry codec.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ironsbe_core::header::MessageHeader;

use calvera_books::{Price, Side};

use crate::codec::sbe::order_entry::{
    ExecType, ExecutionReportEncoder, NewOrderSingleDecoder, OrdStatus,
    OrderCancelReplaceRequestDecoder, OrderCancelRequestDecoder, RejectEncoder, SCHEMA_ID,
};
use crate::codec::sbe::order_entry::Side as SbeSide;
use crate::codec::{OrderEntry, ParseOutcome, SessionId};
use crate::types::{Command, CommandType, Event, EventType};

/// SBE order-entry adapter. Clone to share ClOrdID state across ingress and egress.
#[derive(Clone)]
pub struct SbeOe {
    // TODO: consider if a ring buffer or disruptor should be used here
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    symbol: String,
    ids: HashMap<(SessionId, String), u32>,
    refs: HashMap<(SessionId, u32), String>,
    last_ref: HashMap<SessionId, u32>,
    next_ref: u32,
    next_exec: u64,
}

impl SbeOe {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                symbol: symbol.into(),
                ids: HashMap::new(),
                refs: HashMap::new(),
                last_ref: HashMap::new(),
                next_ref: 1,
                next_exec: 1,
            })),
        }
    }
}

impl Inner {

    fn pad_clord(s: &str) -> [u8; 14] {
        let mut id = [b' '; 14];
        let b = s.as_bytes();
        let n = b.len().min(14);
        id[..n].copy_from_slice(&b[..n]);
        id
    }

    fn assign_ref(&mut self, session: SessionId, cl: &str) -> u32 {
        if let Some(&id) = self.ids.get(&(session, cl.to_string())) {
            return id;
        }
        let id = self.next_ref;
        self.next_ref = self.next_ref.saturating_add(1);
        self.ids.insert((session, cl.to_string()), id);
        self.refs.insert((session, id), cl.to_string());
        id
    }

    fn lookup(&self, session: SessionId, cl: &str) -> Option<u32> {
        self.ids.get(&(session, cl.to_string())).copied()
    }

    fn clord_of(&self, sid: SessionId, uref: u32, fallback: &[u8; 14]) -> Vec<u8> {
        if let Some(s) = self.refs.get(&(sid, uref)) {
            return s.as_bytes().to_vec();
        }
        fallback
            .iter()
            .copied()
            .take_while(|&b| b != b' ')
            .collect()
    }

    fn reject(&self, cl: &[u8], reason: u16, out: &mut [u8]) -> usize {
        let mut enc = RejectEncoder::wrap(out, 0);
        enc.set_cl_ord_id(cl);
        enc.set_reason(reason);
        enc.encoded_length()
    }

    fn map_add(&mut self, session: SessionId, buf: &[u8], hdr: &MessageHeader) -> Option<Command> {
        let d = NewOrderSingleDecoder::wrap(buf, MessageHeader::ENCODED_LENGTH, hdr.version);
        if d.symbol_as_str() != self.symbol {
            return None;
        }
        let cl = d.cl_ord_id_as_str();
        let uref = self.assign_ref(session, cl);
        self.last_ref.insert(session, uref);
        let mut cmd = Command::blank(CommandType::Add);
        cmd.client_fd = session.0;
        cmd.user_ref = uref;
        cmd.cl_ord_id = Self::pad_clord(cl);
        cmd.side = match d.side() {
            SbeSide::Buy => Side::Bid,
            SbeSide::Sell => Side::Ask,
        };
        cmd.price = Price(d.price() as u64);
        cmd.quantity = d.quantity();
        Some(cmd)
    }

    fn map_cancel(
        &mut self,
        session: SessionId,
        buf: &[u8],
        hdr: &MessageHeader,
    ) -> Option<Command> {
        let d = OrderCancelRequestDecoder::wrap(buf, MessageHeader::ENCODED_LENGTH, hdr.version);
        if d.symbol_as_str() != self.symbol {
            return None;
        }
        let orig = d.orig_cl_ord_id_as_str();
        let orig = if orig.is_empty() {
            d.cl_ord_id_as_str()
        } else {
            orig
        };
        let uref = self.lookup(session, orig)?;
        self.last_ref.insert(session, uref);
        let mut cmd = Command::blank(CommandType::Cancel);
        cmd.client_fd = session.0;
        cmd.user_ref = uref;
        cmd.cl_ord_id = Self::pad_clord(orig);
        cmd.quantity = d.quantity();
        Some(cmd)
    }

    fn map_replace(
        &mut self,
        session: SessionId,
        buf: &[u8],
        hdr: &MessageHeader,
    ) -> Option<Command> {
        let d =
            OrderCancelReplaceRequestDecoder::wrap(buf, MessageHeader::ENCODED_LENGTH, hdr.version);
        if d.symbol_as_str() != self.symbol {
            return None;
        }
        let orig = d.orig_cl_ord_id_as_str();
        let orig = if orig.is_empty() {
            d.cl_ord_id_as_str()
        } else {
            orig
        };
        let uref = self.lookup(session, orig)?;
        let cl = d.cl_ord_id_as_str();
        if cl != orig {
            self.ids.insert((session, cl.to_string()), uref);
        }
        self.last_ref.insert(session, uref);
        let mut cmd = Command::blank(CommandType::Modify);
        cmd.client_fd = session.0;
        cmd.user_ref = uref;
        cmd.cl_ord_id = Self::pad_clord(orig);
        cmd.side = match d.side() {
            SbeSide::Buy => Side::Bid,
            SbeSide::Sell => Side::Ask,
        };
        cmd.price = Price(d.price() as u64);
        cmd.quantity = d.quantity();
        Some(cmd)
    }
}

impl Inner {
    fn parse(&mut self, buf: &[u8], session: SessionId, reply: &mut [u8]) -> ParseOutcome {
        if buf.len() < MessageHeader::ENCODED_LENGTH {
            return ParseOutcome::NeedMore;
        }
        let hdr = MessageHeader::wrap(buf, 0);
        let need = MessageHeader::ENCODED_LENGTH + hdr.block_length as usize;
        if buf.len() < need {
            return ParseOutcome::NeedMore;
        }
        if hdr.schema_id != SCHEMA_ID {
            return ParseOutcome::Bad { consumed: 1 };
        }
        match hdr.template_id {
            NewOrderSingleDecoder::TEMPLATE_ID => match self.map_add(session, buf, &hdr) {
                Some(cmd) => ParseOutcome::Command {
                    cmd,
                    consumed: need,
                },
                None => {
                    let n = self.reject(b"", 1, reply);
                    ParseOutcome::Reply {
                        bytes: n,
                        consumed: need,
                    }
                }
            },
            OrderCancelRequestDecoder::TEMPLATE_ID => match self.map_cancel(session, buf, &hdr) {
                Some(cmd) => ParseOutcome::Command {
                    cmd,
                    consumed: need,
                },
                None => {
                    let n = self.reject(b"", 1, reply);
                    ParseOutcome::Reply {
                        bytes: n,
                        consumed: need,
                    }
                }
            },
            OrderCancelReplaceRequestDecoder::TEMPLATE_ID => {
                match self.map_replace(session, buf, &hdr) {
                    Some(cmd) => ParseOutcome::Command {
                        cmd,
                        consumed: need,
                    },
                    None => {
                        let n = self.reject(b"", 1, reply);
                        ParseOutcome::Reply {
                            bytes: n,
                            consumed: need,
                        }
                    }
                }
            }
            _ => ParseOutcome::Bad { consumed: need },
        }
    }

    fn encode_event(&mut self, evt: &Event, out: &mut [u8]) -> usize {
        if evt.ty == EventType::BookReset || evt.client_fd < 0 {
            return 0;
        }
        let sid = SessionId(evt.client_fd);
        let exec = self.next_exec;
        self.next_exec += 1;
        let exec_s = exec.to_string();
        let (uref, fallback, side, px, qty, last_px, last_qty, oid, et, os) = match evt.ty {
            EventType::OrderAccepted => (
                evt.order.user_ref,
                evt.order.cl_ord_id,
                evt.order.side,
                evt.order.price.0 as i64,
                evt.order.quantity,
                0i64,
                0u64,
                evt.order.order_id,
                ExecType::New,
                OrdStatus::New,
            ),
            EventType::OrderCancelled => (
                evt.order.user_ref,
                evt.order.cl_ord_id,
                evt.order.side,
                evt.order.price.0 as i64,
                evt.order.quantity,
                0,
                0,
                evt.order.order_id,
                ExecType::Canceled,
                OrdStatus::Canceled,
            ),
            EventType::OrderModified => (
                evt.order.user_ref,
                evt.order.cl_ord_id,
                evt.order.side,
                evt.order.price.0 as i64,
                evt.order.quantity,
                0,
                0,
                evt.order.order_id,
                ExecType::Replaced,
                OrdStatus::Replaced,
            ),
            EventType::OrderRejected => (
                evt.reject.user_ref,
                evt.reject.cl_ord_id,
                Side::Bid,
                0,
                0,
                0,
                0,
                0,
                ExecType::Rejected,
                OrdStatus::Rejected,
            ),
            EventType::TradeExecuted => {
                let uref = if self.refs.contains_key(&(sid, evt.trade.maker_user_ref)) {
                    evt.trade.maker_user_ref
                } else {
                    self.last_ref.get(&sid).copied().unwrap_or(0)
                };
                let side = if self.refs.contains_key(&(sid, evt.trade.maker_user_ref)) {
                    match evt.trade.taker_side {
                        Side::Bid => Side::Ask,
                        Side::Ask => Side::Bid,
                    }
                } else {
                    evt.trade.taker_side
                };
                (
                    uref,
                    evt.order.cl_ord_id,
                    side,
                    evt.trade.price.0 as i64,
                    evt.trade.quantity,
                    evt.trade.price.0 as i64,
                    evt.trade.quantity,
                    evt.trade.maker_exchange_id,
                    ExecType::New,
                    OrdStatus::PartialFill,
                )
            }
            EventType::BookReset => return 0,
        };
        let cl = self.clord_of(sid, uref, &fallback);
        let mut enc = ExecutionReportEncoder::wrap(out, 0);
        enc.set_cl_ord_id(&cl);
        enc.set_exec_id(exec_s.as_bytes());
        enc.set_symbol(self.symbol.as_bytes());
        enc.set_side(match side {
            Side::Bid => SbeSide::Buy,
            Side::Ask => SbeSide::Sell,
        });
        enc.set_exec_type(et);
        enc.set_ord_status(os);
        enc.set_price(px);
        enc.set_quantity(qty);
        enc.set_last_px(last_px);
        enc.set_last_qty(last_qty);
        enc.set_order_id(oid);
        enc.encoded_length()
    }

    fn on_session_end(&mut self, session: SessionId) {
        self.ids.retain(|(s, _), _| *s != session);
        self.refs.retain(|(s, _), _| *s != session);
        self.last_ref.remove(&session);
    }
}

impl OrderEntry for SbeOe {
    const MAX_OUT: usize = 256;

    fn parse(&mut self, buf: &[u8], session: SessionId, reply: &mut [u8]) -> ParseOutcome {
        self.inner.lock().expect("sbe").parse(buf, session, reply)
    }

    fn encode_event(&mut self, evt: &Event, out: &mut [u8]) -> usize {
        self.inner.lock().expect("sbe").encode_event(evt, out)
    }

    fn on_session_end(&mut self, session: SessionId) {
        self.inner.lock().expect("sbe").on_session_end(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::sbe::order_entry::NewOrderSingleEncoder;

    fn encode_add(symbol: &[u8]) -> Vec<u8> {
        let mut buf = vec![0u8; 64];
        let mut enc = NewOrderSingleEncoder::wrap(&mut buf, 0);
        enc.set_cl_ord_id(b"ORD1");
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
        let wire = encode_add(b"AAPL");
        let mut reply = [0u8; 256];
        match oe.parse(&wire, SessionId(3), &mut reply) {
            ParseOutcome::Command { cmd, consumed } => {
                assert_eq!(consumed, wire.len());
                assert_eq!(cmd.ty, CommandType::Add);
                assert_eq!(cmd.client_fd, 3);
                assert_eq!(cmd.quantity, 10);
                assert_eq!(cmd.price, Price(100));
                assert_eq!(cmd.side, Side::Bid);
            }
            _ => panic!("expected Command"),
        }
        assert!(matches!(
            oe.parse(&wire[..4], SessionId(3), &mut reply),
            ParseOutcome::NeedMore
        ));
    }
}
