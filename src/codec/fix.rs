//! FIX 4.4 order-entry codec.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ironfix_core::error::DecodeError;
use ironfix_core::message::{MsgType, RawMessage};
use ironfix_tagvalue::{Decoder, Encoder};

use calvera_books::{Price, Side};

use crate::codec::{OrderEntry, ParseOutcome, SessionId};
use crate::types::{Command, CommandType, Event, EventType};

const BEGIN: &str = "FIX.4.4";
const TAG_SENDER: u32 = 49;
const TAG_TARGET: u32 = 56;
const TAG_SEQ: u32 = 34;
const TAG_HB: u32 = 108;
const TAG_TEST_REQ: u32 = 112;
const TAG_REF_SEQ: u32 = 45;
const TAG_TEXT: u32 = 58;
const TAG_ENCRYPT: u32 = 98;
const TAG_CLORD: u32 = 11;
const TAG_ORIG_CLORD: u32 = 41;
const TAG_SIDE: u32 = 54;
const TAG_QTY: u32 = 38;
const TAG_PRICE: u32 = 44;
const TAG_SYMBOL: u32 = 55;
const TAG_TIF: u32 = 59;

/// FIX 4.4 acceptor. Clone to share one session table across ingress and egress.
#[derive(Clone)]
pub struct Fix {
    // TODO: consider if a ring buffer or disruptor should be used here
    inner: Arc<Mutex<Inner>>,
}

/// `us` is this venue's CompID; `them` is the expected client CompID.
struct Inner {
    us: String,
    them: String,
    symbol: String,
    sessions: HashMap<SessionId, Sess>,
    ids: HashMap<(SessionId, String), u32>,
    refs: HashMap<(SessionId, u32), String>,
    last_ref: HashMap<SessionId, u32>,
    next_exec: u64,
}

struct Sess {
    next_in: u64,
    next_out: u64,
    next_ref: u32,
    logged_on: bool,
    last_out: Instant,
    hb: Duration,
}

impl Fix {
    pub fn new(us: impl Into<String>, them: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                us: us.into(),
                them: them.into(),
                symbol: symbol.into(),
                sessions: HashMap::new(),
                ids: HashMap::new(),
                refs: HashMap::new(),
                last_ref: HashMap::new(),
                next_exec: 1,
            })),
        }
    }

    #[cfg(test)]
    fn clord_for(&self, session: SessionId, user_ref: u32) -> Option<String> {
        self.inner
            .lock()
            .expect("fix")
            .refs
            .get(&(session, user_ref))
            .cloned()
    }
}

impl Inner {
    fn clord_of(&self, sid: SessionId, uref: u32, fallback: &[u8; 14]) -> String {
        if let Some(s) = self.refs.get(&(sid, uref)) {
            return s.clone();
        }
        String::from_utf8_lossy(fallback).trim().to_string()
    }

    fn trade_uref(&self, sid: SessionId, evt: &Event) -> u32 {
        if self.refs.contains_key(&(sid, evt.trade.maker_user_ref)) {
            evt.trade.maker_user_ref
        } else {
            self.last_ref.get(&sid).copied().unwrap_or(0)
        }
    }

    fn write_exec(&mut self, evt: &Event, out: &mut [u8]) -> usize {
        if evt.ty == EventType::BookReset || evt.client_fd < 0 {
            return 0;
        }
        let sid = SessionId(evt.client_fd);
        let seq = {
            let Some(sess) = self.sessions.get_mut(&sid) else {
                return 0;
            };
            if !sess.logged_on {
                return 0;
            }
            let seq = sess.next_out;
            sess.next_out += 1;
            sess.last_out = Instant::now();
            seq
        };
        let exec_id = self.next_exec;
        self.next_exec += 1;
        let exec_id_s = exec_id.to_string();

        match evt.ty {
            EventType::OrderAccepted => {
                let cl = self.clord_of(sid, evt.order.user_ref, &evt.order.cl_ord_id);
                let oid = evt.order.order_id.to_string();
                let qty = evt.order.quantity.to_string();
                let px = evt.order.price.0.to_string();
                let side = Self::side_fix(evt.order.side);
                self.frame(
                    seq,
                    "8",
                    |enc| {
                        enc.put_str(11, &cl);
                        enc.put_str(17, &exec_id_s);
                        enc.put_str(150, "0");
                        enc.put_str(39, "0");
                        enc.put_str(54, side);
                        enc.put_str(38, &qty);
                        enc.put_str(44, &px);
                        enc.put_str(37, &oid);
                    },
                    out,
                )
            }
            EventType::OrderCancelled => {
                let cl = self.clord_of(sid, evt.order.user_ref, &evt.order.cl_ord_id);
                let oid = evt.order.order_id.to_string();
                self.frame(
                    seq,
                    "8",
                    |enc| {
                        enc.put_str(11, &cl);
                        enc.put_str(17, &exec_id_s);
                        enc.put_str(150, "4");
                        enc.put_str(39, "4");
                        enc.put_str(37, &oid);
                        enc.put_str(54, Self::side_fix(evt.order.side));
                    },
                    out,
                )
            }
            EventType::OrderModified => {
                let cl = self.clord_of(sid, evt.order.user_ref, &evt.order.cl_ord_id);
                let oid = evt.order.order_id.to_string();
                let qty = evt.order.quantity.to_string();
                let px = evt.order.price.0.to_string();
                self.frame(
                    seq,
                    "8",
                    |enc| {
                        enc.put_str(11, &cl);
                        enc.put_str(17, &exec_id_s);
                        enc.put_str(150, "5");
                        enc.put_str(39, "5");
                        enc.put_str(54, Self::side_fix(evt.order.side));
                        enc.put_str(38, &qty);
                        enc.put_str(44, &px);
                        enc.put_str(37, &oid);
                    },
                    out,
                )
            }
            EventType::OrderRejected => {
                let cl = self.clord_of(sid, evt.reject.user_ref, &evt.reject.cl_ord_id);
                let reason = evt.reject.reason.to_string();
                self.frame(
                    seq,
                    "8",
                    |enc| {
                        enc.put_str(11, &cl);
                        enc.put_str(17, &exec_id_s);
                        enc.put_str(150, "8");
                        enc.put_str(39, "8");
                        enc.put_str(103, &reason);
                    },
                    out,
                )
            }
            EventType::TradeExecuted => {
                let uref = self.trade_uref(sid, evt);
                let cl = self.clord_of(sid, uref, &evt.order.cl_ord_id);
                let last_qty = evt.trade.quantity.to_string();
                let last_px = evt.trade.price.0.to_string();
                let oid = evt.trade.maker_exchange_id.to_string();
                let side = if self.refs.contains_key(&(sid, evt.trade.maker_user_ref)) {
                    Self::side_fix(match evt.trade.taker_side {
                        Side::Bid => Side::Ask,
                        Side::Ask => Side::Bid,
                    })
                } else {
                    Self::side_fix(evt.trade.taker_side)
                };
                self.frame(
                    seq,
                    "8",
                    |enc| {
                        enc.put_str(11, &cl);
                        enc.put_str(17, &exec_id_s);
                        enc.put_str(150, "F");
                        enc.put_str(39, "1");
                        enc.put_str(54, side);
                        enc.put_str(31, &last_px);
                        enc.put_str(32, &last_qty);
                        enc.put_str(37, &oid);
                    },
                    out,
                )
            }
            EventType::BookReset => 0,
        }
    }

    fn frame(
        &self,
        seq: u64,
        msg_type: &str,
        extra: impl FnOnce(&mut Encoder),
        out: &mut [u8],
    ) -> usize {
        let mut enc = Encoder::new(BEGIN);
        enc.put_str(35, msg_type);
        enc.put_str(TAG_SENDER, &self.us);
        enc.put_str(TAG_TARGET, &self.them);
        enc.put_int(TAG_SEQ, seq as i64);
        extra(&mut enc);
        match enc.finish() {
            Ok(frame) if frame.len() <= out.len() => {
                out[..frame.len()].copy_from_slice(frame);
                frame.len()
            }
            _ => 0,
        }
    }

    fn reject(&self, seq_out: u64, ref_seq: u64, text: &str, out: &mut [u8]) -> usize {
        self.frame(
            seq_out,
            "3",
            |enc| {
                enc.put_int(TAG_REF_SEQ, ref_seq as i64);
                enc.put_str(TAG_TEXT, text);
            },
            out,
        )
    }

    fn biz_reject(
        &mut self,
        session: SessionId,
        seq_out: u64,
        consumed: usize,
        text: &str,
        out: &mut [u8],
    ) -> ParseOutcome {
        let n = self.frame(seq_out, "j", |enc| enc.put_str(TAG_TEXT, text), out);
        if let Some(sess) = self.sessions.get_mut(&session) {
            sess.next_out = seq_out + 1;
        }
        ParseOutcome::Reply { bytes: n, consumed }
    }

    fn map_app(
        &mut self,
        ty: MsgType,
        session: SessionId,
        msg: &RawMessage<'_>,
        consumed: usize,
        seq_out: u64,
        reply: &mut [u8],
    ) -> ParseOutcome {
        let sym = msg.get_field_str(TAG_SYMBOL).unwrap_or("");
        if sym != self.symbol {
            return self.biz_reject(session, seq_out, consumed, "symbol", reply);
        }

        let cmd = match ty {
            MsgType::NewOrderSingle => self.cmd_add(session, msg).map_err(|_| "fields"),
            MsgType::OrderCancelRequest => self.cmd_cancel(session, msg),
            MsgType::OrderCancelReplaceRequest => self.cmd_modify(session, msg),
            _ => Err("fields"),
        };
        match cmd {
            Ok(cmd) => ParseOutcome::Command { cmd, consumed },
            Err(why) => self.biz_reject(session, seq_out, consumed, why, reply),
        }
    }

    fn cmd_add(&mut self, session: SessionId, msg: &RawMessage<'_>) -> Result<Command, ()> {
        let cl = msg.get_field_str(TAG_CLORD).ok_or(())?;
        let side = Self::parse_side(msg.get_field_str(TAG_SIDE).ok_or(())?).ok_or(())?;
        let quantity = Self::parse_qty(msg.get_field_str(TAG_QTY).ok_or(())?).ok_or(())?;
        let price = Self::parse_price(msg.get_field_str(TAG_PRICE).ok_or(())?).ok_or(())?;
        let user_ref = if let Some(&id) = self.ids.get(&(session, cl.to_string())) {
            id
        } else {
            let sess = self.sessions.get_mut(&session).ok_or(())?;
            let id = sess.next_ref;
            sess.next_ref = sess.next_ref.saturating_add(1);
            self.ids.insert((session, cl.to_string()), id);
            self.refs.insert((session, id), cl.to_string());
            id
        };
        let mut cmd = Command::blank(CommandType::Add);
        cmd.client_fd = session.0;
        cmd.side = side;
        cmd.quantity = quantity;
        cmd.price = Price(price);
        cmd.user_ref = user_ref;
        cmd.cl_ord_id = Self::pad_clord(cl);
        if let Some(tif) = msg.get_field_str(TAG_TIF) {
            cmd.time_in_force = tif.as_bytes().first().copied().unwrap_or(0);
        }
        self.last_ref.insert(session, user_ref);
        Ok(cmd)
    }

    fn cmd_cancel(
        &mut self,
        session: SessionId,
        msg: &RawMessage<'_>,
    ) -> Result<Command, &'static str> {
        let orig = msg
            .get_field_str(TAG_ORIG_CLORD)
            .or_else(|| msg.get_field_str(TAG_CLORD))
            .ok_or("fields")?;
        let user_ref = *self
            .ids
            .get(&(session, orig.to_string()))
            .ok_or("clordid")?;
        let mut cmd = Command::blank(CommandType::Cancel);
        cmd.client_fd = session.0;
        cmd.user_ref = user_ref;
        cmd.cl_ord_id = Self::pad_clord(orig);
        if let Some(q) = msg.get_field_str(TAG_QTY).and_then(Self::parse_qty) {
            cmd.quantity = q;
        }
        self.last_ref.insert(session, user_ref);
        Ok(cmd)
    }

    fn cmd_modify(
        &mut self,
        session: SessionId,
        msg: &RawMessage<'_>,
    ) -> Result<Command, &'static str> {
        let orig = msg
            .get_field_str(TAG_ORIG_CLORD)
            .or_else(|| msg.get_field_str(TAG_CLORD))
            .ok_or("fields")?;
        let user_ref = *self
            .ids
            .get(&(session, orig.to_string()))
            .ok_or("clordid")?;
        if let Some(cl) = msg.get_field_str(TAG_CLORD) {
            if cl != orig {
                self.ids.insert((session, cl.to_string()), user_ref);
            }
        }
        let mut cmd = Command::blank(CommandType::Modify);
        cmd.client_fd = session.0;
        cmd.user_ref = user_ref;
        cmd.cl_ord_id = Self::pad_clord(orig);
        if let Some(side) = msg.get_field_str(TAG_SIDE).and_then(Self::parse_side) {
            cmd.side = side;
        }
        if let Some(q) = msg.get_field_str(TAG_QTY).and_then(Self::parse_qty) {
            cmd.quantity = q;
        }
        if let Some(px) = msg.get_field_str(TAG_PRICE).and_then(Self::parse_price) {
            cmd.price = Price(px);
        }
        self.last_ref.insert(session, user_ref);
        Ok(cmd)
    }

    fn pad_clord(s: &str) -> [u8; 14] {
        let mut id = [b' '; 14];
        let b = s.as_bytes();
        let n = b.len().min(14);
        id[..n].copy_from_slice(&b[..n]);
        id
    }

    fn side_fix(side: Side) -> &'static str {
        match side {
            Side::Bid => "1",
            Side::Ask => "2",
        }
    }

    fn parse_side(s: &str) -> Option<Side> {
        match s {
            "1" => Some(Side::Bid),
            "2" => Some(Side::Ask),
            _ => None,
        }
    }

    fn parse_qty(s: &str) -> Option<u64> {
        s.parse().ok()
    }

    fn parse_price(s: &str) -> Option<u64> {
        if let Ok(v) = s.parse::<u64>() {
            return Some(v);
        }
        s.parse::<f64>().ok().map(|v| v as u64)
    }

    fn handle(
        &mut self,
        session: SessionId,
        msg: &RawMessage<'_>,
        consumed: usize,
        reply: &mut [u8],
    ) -> ParseOutcome {
        let begin = msg.begin_string().unwrap_or("");
        if begin != BEGIN {
            return ParseOutcome::Disconnect { bytes: 0, consumed };
        }

        let inbound_seq = match msg.get_field_as::<u64>(TAG_SEQ) {
            Ok(n) => n,
            Err(_) => {
                return ParseOutcome::Disconnect { bytes: 0, consumed };
            }
        };

        let sender = msg.get_field_str(TAG_SENDER).unwrap_or("");
        let target = msg.get_field_str(TAG_TARGET).unwrap_or("");
        if sender != self.them || target != self.us {
            let seq_out = self.sessions.get(&session).map(|s| s.next_out).unwrap_or(1);
            let n = self.reject(seq_out, inbound_seq, "compid", reply);
            return ParseOutcome::Disconnect { bytes: n, consumed };
        }

        let ty = msg.msg_type();
        let now = Instant::now();

        if *ty == MsgType::Logon {
            if inbound_seq != 1 {
                let n = self.reject(1, inbound_seq, "seq", reply);
                return ParseOutcome::Disconnect { bytes: n, consumed };
            }
            let hb_secs = msg.get_field_as::<u64>(TAG_HB).unwrap_or(30);
            let n = self.frame(
                1,
                "A",
                |enc| {
                    enc.put_int(TAG_ENCRYPT, 0);
                    enc.put_int(TAG_HB, hb_secs as i64);
                },
                reply,
            );
            self.sessions.insert(
                session,
                Sess {
                    next_in: 2,
                    next_out: 2,
                    next_ref: 1,
                    logged_on: true,
                    last_out: now,
                    hb: Duration::from_secs(hb_secs),
                },
            );
            return ParseOutcome::Reply { bytes: n, consumed };
        }

        let (next_in, next_out, logged_on) = match self.sessions.get(&session) {
            Some(s) => (s.next_in, s.next_out, s.logged_on),
            None => {
                return ParseOutcome::Disconnect { bytes: 0, consumed };
            }
        };

        if inbound_seq != next_in {
            let n = self.reject(next_out, inbound_seq, "seq", reply);
            self.sessions.remove(&session);
            return ParseOutcome::Disconnect { bytes: n, consumed };
        }

        if !logged_on {
            self.sessions.remove(&session);
            return ParseOutcome::Disconnect { bytes: 0, consumed };
        }

        if let Some(sess) = self.sessions.get_mut(&session) {
            sess.next_in += 1;
        }

        match *ty {
            MsgType::Heartbeat => ParseOutcome::Reply { bytes: 0, consumed },
            MsgType::TestRequest => {
                let req = msg.get_field_str(TAG_TEST_REQ).unwrap_or("").to_owned();
                let n = self.frame(
                    next_out,
                    "0",
                    |enc| {
                        if !req.is_empty() {
                            enc.put_str(TAG_TEST_REQ, &req);
                        }
                    },
                    reply,
                );
                if let Some(sess) = self.sessions.get_mut(&session) {
                    sess.next_out = next_out + 1;
                    sess.last_out = now;
                }
                ParseOutcome::Reply { bytes: n, consumed }
            }
            MsgType::Logout => {
                let n = self.frame(next_out, "5", |_| {}, reply);
                if let Some(sess) = self.sessions.get_mut(&session) {
                    sess.next_out = next_out + 1;
                    sess.logged_on = false;
                    sess.last_out = now;
                }
                ParseOutcome::Reply { bytes: n, consumed }
            }
            MsgType::NewOrderSingle
            | MsgType::OrderCancelRequest
            | MsgType::OrderCancelReplaceRequest => {
                self.map_app(ty.clone(), session, msg, consumed, next_out, reply)
            }
            _ => ParseOutcome::Reply { bytes: 0, consumed },
        }
    }

    fn parse(&mut self, buf: &[u8], session: SessionId, reply: &mut [u8]) -> ParseOutcome {
        if buf.is_empty() {
            return ParseOutcome::NeedMore;
        }
        let mut dec = Decoder::new(buf).with_checksum_validation(true);
        match dec.decode() {
            Ok(msg) => self.handle(session, &msg, msg.len().max(1), reply),
            Err(DecodeError::Incomplete) => ParseOutcome::NeedMore,
            Err(DecodeError::UnterminatedField { .. }) => ParseOutcome::NeedMore,
            Err(_) => ParseOutcome::Bad {
                consumed: dec.offset().clamp(1, buf.len()),
            },
        }
    }

    fn on_idle(&mut self, now: Instant, session: SessionId, reply: &mut [u8]) -> usize {
        let seq = {
            let Some(sess) = self.sessions.get_mut(&session) else {
                return 0;
            };
            if !sess.logged_on || sess.hb.is_zero() {
                return 0;
            }
            if now.saturating_duration_since(sess.last_out) < sess.hb {
                return 0;
            }
            let seq = sess.next_out;
            sess.next_out += 1;
            sess.last_out = now;
            seq
        };
        self.frame(seq, "0", |_| {}, reply)
    }

    fn on_session_end(&mut self, session: SessionId) {
        self.sessions.remove(&session);
        self.ids.retain(|(s, _), _| *s != session);
        self.refs.retain(|(s, _), _| *s != session);
        self.last_ref.remove(&session);
    }
}

impl OrderEntry for Fix {
    const MAX_OUT: usize = 1024;

    fn parse(&mut self, buf: &[u8], session: SessionId, reply: &mut [u8]) -> ParseOutcome {
        self.inner.lock().expect("fix").parse(buf, session, reply)
    }

    fn encode_event(&mut self, evt: &Event, out: &mut [u8]) -> usize {
        self.inner.lock().expect("fix").write_exec(evt, out)
    }

    fn on_idle(&mut self, now: Instant, session: SessionId, reply: &mut [u8]) -> usize {
        self.inner.lock().expect("fix").on_idle(now, session, reply)
    }

    fn on_session_end(&mut self, session: SessionId) {
        self.inner.lock().expect("fix").on_session_end(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Event, EventOrder, EventReject, EventTrade, EventType};
    use ironfix_tagvalue::Encoder;

    const SID: SessionId = SessionId(1);

    fn client_frame(seq: i64, msg_type: &str, extra: impl FnOnce(&mut Encoder)) -> Vec<u8> {
        let mut enc = Encoder::new(BEGIN);
        enc.put_str(35, msg_type);
        enc.put_str(TAG_SENDER, "CLIENT");
        enc.put_str(TAG_TARGET, "VENUE");
        enc.put_int(TAG_SEQ, seq);
        extra(&mut enc);
        enc.finish().expect("frame").to_vec()
    }

    fn heartbeat() -> Vec<u8> {
        client_frame(1, "0", |_| {})
    }

    fn logon(seq: i64, hb: i64) -> Vec<u8> {
        client_frame(seq, "A", |enc| {
            enc.put_int(TAG_ENCRYPT, 0);
            enc.put_int(TAG_HB, hb);
        })
    }

    fn parse_on(fix: &mut Fix, buf: &[u8], reply: &mut [u8]) -> ParseOutcome {
        fix.parse(buf, SID, reply)
    }

    fn command(outcome: ParseOutcome) -> Option<Command> {
        match outcome {
            ParseOutcome::Command { cmd, .. } => Some(cmd),
            ParseOutcome::Reply { .. }
            | ParseOutcome::Disconnect { .. }
            | ParseOutcome::NeedMore
            | ParseOutcome::Bad { .. } => None,
        }
    }

    fn reply_parts(outcome: ParseOutcome) -> Option<(usize, usize)> {
        match outcome {
            ParseOutcome::Reply { bytes, consumed } => Some((bytes, consumed)),
            ParseOutcome::Command { .. }
            | ParseOutcome::Disconnect { .. }
            | ParseOutcome::NeedMore
            | ParseOutcome::Bad { .. } => None,
        }
    }

    fn disconnect_bytes(outcome: ParseOutcome) -> Option<usize> {
        match outcome {
            ParseOutcome::Disconnect { bytes, .. } => Some(bytes),
            ParseOutcome::Command { .. }
            | ParseOutcome::Reply { .. }
            | ParseOutcome::NeedMore
            | ParseOutcome::Bad { .. } => None,
        }
    }

    fn decode_type(buf: &[u8]) -> MsgType {
        let mut dec = Decoder::new(buf).with_checksum_validation(true);
        dec.decode().expect("reply").msg_type().clone()
    }

    #[test]
    fn truncated_needs_more() {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let hb = heartbeat();
        assert!(matches!(
            parse_on(&mut fix, &hb[..hb.len() / 2], &mut [0u8; 8]),
            ParseOutcome::NeedMore
        ));
        assert!(matches!(
            parse_on(&mut fix, &[], &mut [0u8; 8]),
            ParseOutcome::NeedMore
        ));
    }

    #[test]
    fn bad_checksum_is_bad() {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let mut hb = heartbeat();
        let i = hb.iter().position(|&b| b == b'=').expect("tag 8");
        hb[i + 1] ^= 0x01;
        assert!(matches!(
            parse_on(&mut fix, &hb, &mut [0u8; 256]),
            ParseOutcome::Bad { consumed } if consumed >= 1
        ));
    }

    #[test]
    fn logon_replies_logon() {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let mut reply = [0u8; 512];
        let (bytes, consumed) = reply_parts(parse_on(&mut fix, &logon(1, 30), &mut reply)).unwrap();
        assert!(consumed > 0);
        assert_eq!(decode_type(&reply[..bytes]), MsgType::Logon);
    }

    #[test]
    fn heartbeat_after_logon_consumed() {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let mut reply = [0u8; 512];
        assert!(matches!(
            parse_on(&mut fix, &logon(1, 30), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        let (bytes, consumed) = reply_parts(parse_on(
            &mut fix,
            &client_frame(2, "0", |_| {}),
            &mut reply,
        ))
        .unwrap();
        assert_eq!(bytes, 0);
        assert!(consumed > 0);
    }

    #[test]
    fn logout_replies_logout() {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let mut reply = [0u8; 512];
        assert!(matches!(
            parse_on(&mut fix, &logon(1, 30), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        let (bytes, _) = reply_parts(parse_on(
            &mut fix,
            &client_frame(2, "5", |_| {}),
            &mut reply,
        ))
        .unwrap();
        assert_eq!(decode_type(&reply[..bytes]), MsgType::Logout);
    }

    #[test]
    fn unexpected_seq_disconnects() {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let mut reply = [0u8; 512];
        assert!(matches!(
            parse_on(&mut fix, &logon(1, 30), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        let bytes = disconnect_bytes(parse_on(
            &mut fix,
            &client_frame(9, "0", |_| {}),
            &mut reply,
        ))
        .unwrap();
        assert_eq!(decode_type(&reply[..bytes]), MsgType::Reject);
    }

    #[test]
    fn idle_heartbeat_after_interval() {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let mut reply = [0u8; 512];
        assert!(matches!(
            parse_on(&mut fix, &logon(1, 1), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        let later = Instant::now() + Duration::from_secs(2);
        let n = fix.on_idle(later, SID, &mut reply);
        assert!(n > 0);
        assert_eq!(decode_type(&reply[..n]), MsgType::Heartbeat);
        assert_eq!(fix.on_idle(later, SID, &mut reply), 0);
    }

    fn logged_in() -> (Fix, [u8; 512]) {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let mut reply = [0u8; 512];
        assert!(matches!(
            parse_on(&mut fix, &logon(1, 30), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        (fix, reply)
    }

    #[test]
    fn new_order_single_is_add() {
        let (mut fix, mut reply) = logged_in();
        let wire = client_frame(2, "D", |enc| {
            enc.put_str(TAG_CLORD, "ORD1");
            enc.put_str(TAG_SYMBOL, "AAPL");
            enc.put_str(TAG_SIDE, "1");
            enc.put_str(TAG_QTY, "10");
            enc.put_str(TAG_PRICE, "100");
        });
        let cmd = command(parse_on(&mut fix, &wire, &mut reply)).unwrap();
        assert_eq!(cmd.ty, CommandType::Add);
        assert_eq!(cmd.side, Side::Bid);
        assert_eq!(cmd.quantity, 10);
        assert_eq!(cmd.price, Price(100));
        assert_eq!(cmd.client_fd, 1);
    }

    #[test]
    fn wrong_symbol_is_not_a_command() {
        let (mut fix, mut reply) = logged_in();
        let wire = client_frame(2, "D", |enc| {
            enc.put_str(TAG_CLORD, "ORD1");
            enc.put_str(TAG_SYMBOL, "MSFT");
            enc.put_str(TAG_SIDE, "1");
            enc.put_str(TAG_QTY, "10");
            enc.put_str(TAG_PRICE, "100");
        });
        let (bytes, _) = reply_parts(parse_on(&mut fix, &wire, &mut reply)).unwrap();
        assert!(bytes > 0);
        assert_eq!(decode_type(&reply[..bytes]), MsgType::BusinessMessageReject);
    }

    fn add_ord1(seq: i64) -> Vec<u8> {
        client_frame(seq, "D", |enc| {
            enc.put_str(TAG_CLORD, "ORD1");
            enc.put_str(TAG_SYMBOL, "AAPL");
            enc.put_str(TAG_SIDE, "1");
            enc.put_str(TAG_QTY, "10");
            enc.put_str(TAG_PRICE, "100");
        })
    }

    #[test]
    fn cancel_and_replace_share_user_ref() {
        let (mut fix, mut reply) = logged_in();
        let uref = command(parse_on(&mut fix, &add_ord1(2), &mut reply))
            .unwrap()
            .user_ref;
        assert_eq!(fix.clord_for(SID, uref).as_deref(), Some("ORD1"));
        let replace = client_frame(3, "G", |enc| {
            enc.put_str(TAG_CLORD, "ORD2");
            enc.put_str(TAG_ORIG_CLORD, "ORD1");
            enc.put_str(TAG_SYMBOL, "AAPL");
            enc.put_str(TAG_SIDE, "2");
            enc.put_str(TAG_QTY, "5");
            enc.put_str(TAG_PRICE, "99");
        });
        let cmd = command(parse_on(&mut fix, &replace, &mut reply)).unwrap();
        assert_eq!(cmd.ty, CommandType::Modify);
        assert_eq!(cmd.user_ref, uref);
        assert_eq!(cmd.quantity, 5);
        let cancel = client_frame(4, "F", |enc| {
            enc.put_str(TAG_CLORD, "CXL1");
            enc.put_str(TAG_ORIG_CLORD, "ORD2");
            enc.put_str(TAG_SYMBOL, "AAPL");
        });
        let cmd = command(parse_on(&mut fix, &cancel, &mut reply)).unwrap();
        assert_eq!(cmd.ty, CommandType::Cancel);
        assert_eq!(cmd.user_ref, uref);
    }

    #[test]
    fn unknown_clordid_is_not_a_command() {
        let (mut fix, mut reply) = logged_in();
        let cancel = client_frame(2, "F", |enc| {
            enc.put_str(TAG_CLORD, "CXL1");
            enc.put_str(TAG_ORIG_CLORD, "NOPE");
            enc.put_str(TAG_SYMBOL, "AAPL");
        });
        let (bytes, _) = reply_parts(parse_on(&mut fix, &cancel, &mut reply)).unwrap();
        assert!(bytes > 0);
        assert_eq!(decode_type(&reply[..bytes]), MsgType::BusinessMessageReject);
    }

    #[test]
    fn encode_accept_and_trade_tags() {
        use crate::types::{Event, EventOrder, EventTrade};

        let (mut fix, mut reply) = logged_in();
        let uref = command(parse_on(&mut fix, &add_ord1(2), &mut reply))
            .unwrap()
            .user_ref;
        let acc = Event::accepted(
            SID.0,
            EventOrder {
                order_id: 42,
                user_ref: uref,
                price: Price(100),
                quantity: 10,
                side: Side::Bid,
                order_state: b'L',
                cl_ord_id: Inner::pad_clord("ORD1"),
            },
        );
        let n = fix.encode_event(&acc, &mut reply);
        assert!(n > 0);
        {
            let mut dec = Decoder::new(&reply[..n]).with_checksum_validation(true);
            let msg = dec.decode().expect("er");
            assert_eq!(msg.msg_type().clone(), MsgType::ExecutionReport);
            assert_eq!(msg.get_field_str(11), Some("ORD1"));
            assert!(msg.get_field_str(17).is_some());
            assert_eq!(msg.get_field_str(39), Some("0"));
            assert_eq!(msg.get_field_str(150), Some("0"));
        }

        let tr = Event::trade(
            SID.0,
            EventTrade {
                match_number: 1,
                maker_exchange_id: 42,
                maker_user_ref: uref,
                price: Price(100),
                quantity: 3,
                taker_side: Side::Ask,
            },
        );
        let n = fix.encode_event(&tr, &mut reply);
        let mut dec = Decoder::new(&reply[..n]).with_checksum_validation(true);
        let msg = dec.decode().expect("tr");
        assert_eq!(msg.get_field_str(11), Some("ORD1"));
        assert!(msg.get_field_str(17).is_some());
        assert_eq!(msg.get_field_str(31), Some("100"));
        assert_eq!(msg.get_field_str(32), Some("3"));
        assert_eq!(msg.get_field_str(39), Some("1"));
        assert_eq!(msg.get_field_str(150), Some("F"));
    }

    fn order_event(ty: EventType, side: Side, uref: u32) -> Event {
        let order = EventOrder {
            order_id: 7,
            user_ref: uref,
            price: Price(100),
            quantity: 4,
            side,
            order_state: b'0',
            cl_ord_id: Inner::pad_clord("FALLBACK"),
        };
        match ty {
            EventType::OrderAccepted => Event::accepted(SID.0, order),
            EventType::OrderCancelled => Event::cancelled(SID.0, order),
            EventType::OrderModified => Event::modified(SID.0, order),
            EventType::OrderRejected => Event::rejected(
                SID.0,
                EventReject {
                    user_ref: uref,
                    reason: 3,
                    cl_ord_id: Inner::pad_clord("NOMAP"),
                },
            ),
            EventType::TradeExecuted => Event::trade(
                SID.0,
                EventTrade {
                    match_number: 2,
                    maker_exchange_id: 7,
                    maker_user_ref: uref,
                    price: Price(100),
                    quantity: 1,
                    taker_side: side,
                },
            ),
            _ => Event::reset(),
        }
    }

    fn er_tag(fix: &mut Fix, evt: &Event, reply: &mut [u8], tag: u32) -> Option<String> {
        let n = fix.encode_event(evt, reply);
        assert!(n > 0, "execution report");
        let mut dec = Decoder::new(&reply[..n]).with_checksum_validation(true);
        dec.decode()
            .expect("er")
            .get_field_str(tag)
            .map(str::to_string)
    }

    #[test]
    fn execution_reports_and_session_edges() {
        let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
        let mut reply = [0u8; 512];

        assert!(matches!(
            parse_on(&mut fix, b"8=FIX.4.4", &mut reply),
            ParseOutcome::NeedMore
        ));
        assert!(matches!(
            parse_on(&mut fix, &client_frame(1, "0", |_| {}), &mut reply),
            ParseOutcome::Disconnect { bytes: 0, .. }
        ));
        let wrong_begin = {
            let mut enc = Encoder::new("FIX.4.2");
            enc.put_str(35, "0");
            enc.put_str(TAG_SENDER, "CLIENT");
            enc.put_str(TAG_TARGET, "VENUE");
            enc.put_int(TAG_SEQ, 1);
            enc.finish().expect("frame").to_vec()
        };
        assert!(matches!(
            parse_on(&mut fix, &wrong_begin, &mut reply),
            ParseOutcome::Disconnect { bytes: 0, .. }
        ));
        let no_seq = {
            let mut enc = Encoder::new(BEGIN);
            enc.put_str(35, "0");
            enc.put_str(TAG_SENDER, "CLIENT");
            enc.put_str(TAG_TARGET, "VENUE");
            enc.finish().expect("frame").to_vec()
        };
        assert!(matches!(
            parse_on(&mut fix, &no_seq, &mut reply),
            ParseOutcome::Disconnect { bytes: 0, .. }
        ));
        let bad_comp = {
            let mut enc = Encoder::new(BEGIN);
            enc.put_str(35, "0");
            enc.put_str(TAG_SENDER, "OTHER");
            enc.put_str(TAG_TARGET, "VENUE");
            enc.put_int(TAG_SEQ, 1);
            enc.finish().expect("frame").to_vec()
        };
        assert!(matches!(
            parse_on(&mut fix, &bad_comp, &mut reply),
            ParseOutcome::Disconnect { .. }
        ));
        assert!(matches!(
            parse_on(&mut fix, &logon(9, 30), &mut reply),
            ParseOutcome::Disconnect { .. }
        ));

        assert!(matches!(
            parse_on(&mut fix, &logon(1, 0), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        assert_eq!(fix.on_idle(Instant::now(), SID, &mut reply), 0);
        assert_eq!(fix.on_idle(Instant::now(), SessionId(9), &mut reply), 0);
        let _ = order_event(EventType::BookReset, Side::Bid, 0);
        assert_eq!(fix.encode_event(&Event::reset(), &mut reply), 0);
        assert_eq!(
            fix.encode_event(
                &order_event(EventType::OrderAccepted, Side::Bid, 1),
                &mut [0u8; 8]
            ),
            0
        );

        let (mut fix, mut reply) = logged_in();
        let uref = command(parse_on(&mut fix, &add_ord1(2), &mut reply))
            .unwrap()
            .user_ref;
        let again = parse_on(&mut fix, &add_ord1(3), &mut reply);
        assert!(matches!(again, ParseOutcome::Command { .. }));

        let with_tif = client_frame(4, "D", |enc| {
            enc.put_str(TAG_CLORD, "ORD9");
            enc.put_str(TAG_SYMBOL, "AAPL");
            enc.put_str(TAG_SIDE, "2");
            enc.put_str(TAG_QTY, "4");
            enc.put_str(TAG_PRICE, "100.5");
            enc.put_str(TAG_TIF, "0");
        });
        let cmd = command(parse_on(&mut fix, &with_tif, &mut reply)).unwrap();
        assert_eq!(cmd.side, Side::Ask);
        assert_eq!(cmd.price, Price(100));
        assert_eq!(cmd.time_in_force, b'0');
        let bad_side = client_frame(5, "D", |enc| {
            enc.put_str(TAG_CLORD, "BAD");
            enc.put_str(TAG_SYMBOL, "AAPL");
            enc.put_str(TAG_SIDE, "9");
            enc.put_str(TAG_QTY, "1");
            enc.put_str(TAG_PRICE, "1");
        });
        assert!(matches!(
            parse_on(&mut fix, &bad_side, &mut reply),
            ParseOutcome::Reply { .. }
        ));
        let bad_qty = client_frame(6, "D", |enc| {
            enc.put_str(TAG_CLORD, "BADQ");
            enc.put_str(TAG_SYMBOL, "AAPL");
            enc.put_str(TAG_SIDE, "1");
            enc.put_str(TAG_QTY, "nope");
            enc.put_str(TAG_PRICE, "1");
        });
        assert!(matches!(
            parse_on(&mut fix, &bad_qty, &mut reply),
            ParseOutcome::Reply { .. }
        ));

        let no_new_id = client_frame(7, "G", |enc| {
            enc.put_str(TAG_ORIG_CLORD, "ORD1");
            enc.put_str(TAG_SYMBOL, "AAPL");
        });
        assert!(command(parse_on(&mut fix, &no_new_id, &mut reply)).is_some());
        let incomplete = b"8=FIX.4.4\x019=5\x0135=0\x01";
        assert!(matches!(
            parse_on(&mut fix, incomplete, &mut reply),
            ParseOutcome::NeedMore
        ));
        let same_id = client_frame(8, "G", |enc| {
            enc.put_str(TAG_CLORD, "ORD1");
            enc.put_str(TAG_ORIG_CLORD, "ORD1");
            enc.put_str(TAG_SYMBOL, "AAPL");
            enc.put_str(TAG_QTY, "2");
        });
        assert!(matches!(
            parse_on(&mut fix, &same_id, &mut reply),
            ParseOutcome::Command { .. }
        ));
        let cancel_qty = client_frame(9, "F", |enc| {
            enc.put_str(TAG_ORIG_CLORD, "ORD1");
            enc.put_str(TAG_SYMBOL, "AAPL");
            enc.put_str(TAG_QTY, "2");
        });
        assert!(matches!(
            parse_on(&mut fix, &cancel_qty, &mut reply),
            ParseOutcome::Command { .. }
        ));

        let ask = Event::accepted(
            SID.0,
            crate::types::EventOrder {
                order_id: 8,
                user_ref: uref,
                price: Price(100),
                quantity: 4,
                side: Side::Ask,
                order_state: b'0',
                cl_ord_id: Inner::pad_clord("ORD9"),
            },
        );
        assert_eq!(er_tag(&mut fix, &ask, &mut reply, 54).as_deref(), Some("2"));
        assert_eq!(
            er_tag(
                &mut fix,
                &order_event(EventType::OrderCancelled, Side::Ask, uref),
                &mut reply,
                150
            )
            .as_deref(),
            Some("4")
        );
        assert_eq!(
            er_tag(
                &mut fix,
                &order_event(EventType::OrderModified, Side::Bid, uref),
                &mut reply,
                150
            )
            .as_deref(),
            Some("5")
        );
        assert_eq!(
            er_tag(
                &mut fix,
                &order_event(EventType::OrderRejected, Side::Bid, 99),
                &mut reply,
                150
            )
            .as_deref(),
            Some("8")
        );
        assert_eq!(
            er_tag(
                &mut fix,
                &order_event(EventType::TradeExecuted, Side::Bid, uref),
                &mut reply,
                54
            )
            .as_deref(),
            Some("2")
        );
        assert_eq!(
            er_tag(
                &mut fix,
                &order_event(EventType::TradeExecuted, Side::Ask, 4242),
                &mut reply,
                54
            )
            .as_deref(),
            Some("2")
        );
        assert_eq!(
            fix.encode_event(
                &order_event(EventType::OrderAccepted, Side::Bid, uref),
                &mut [0u8; 4]
            ),
            0
        );

        assert!(command(ParseOutcome::NeedMore).is_none());
        assert!(reply_parts(ParseOutcome::NeedMore).is_none());
        assert!(disconnect_bytes(ParseOutcome::NeedMore).is_none());
        let probe = client_frame(10, "1", |enc| enc.put_str(TAG_TEST_REQ, "PING"));
        let (bytes, _) = reply_parts(parse_on(&mut fix, &probe, &mut reply)).unwrap();
        assert_eq!(decode_type(&reply[..bytes]), MsgType::Heartbeat);
        let probe = client_frame(11, "1", |_| {});
        assert!(matches!(
            parse_on(&mut fix, &probe, &mut reply),
            ParseOutcome::Reply { .. }
        ));
        let other = client_frame(12, "2", |_| {});
        assert!(matches!(
            parse_on(&mut fix, &other, &mut reply),
            ParseOutcome::Reply { bytes: 0, .. }
        ));
        assert!(matches!(
            parse_on(&mut fix, &client_frame(13, "5", |_| {}), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        assert_eq!(
            fix.encode_event(
                &order_event(EventType::OrderAccepted, Side::Bid, uref),
                &mut reply
            ),
            0
        );
        assert_eq!(fix.on_idle(Instant::now(), SID, &mut reply), 0);
        assert!(matches!(
            parse_on(&mut fix, &client_frame(14, "0", |_| {}), &mut reply),
            ParseOutcome::Disconnect { bytes: 0, .. }
        ));
        fix.on_session_end(SID);
        assert_eq!(fix.clord_for(SID, uref), None);
        assert_eq!(fix.encode_event(&ask, &mut reply), 0);

        let mut reset = Event::reset();
        reset.client_fd = SID.0;
        assert_eq!(fix.encode_event(&reset, &mut reply), 0);
    }
}
