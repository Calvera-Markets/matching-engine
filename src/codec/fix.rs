//! FIX 4.4 order-entry codec.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ironfix_core::error::DecodeError;
use ironfix_core::message::{MsgType, RawMessage};
use ironfix_tagvalue::{Decoder, Encoder};

use crate::codec::{OrderEntry, ParseOutcome, SessionId};
use crate::types::Event;

const BEGIN: &str = "FIX.4.4";
const TAG_SENDER: u32 = 49;
const TAG_TARGET: u32 = 56;
const TAG_SEQ: u32 = 34;
const TAG_HB: u32 = 108;
const TAG_TEST_REQ: u32 = 112;
const TAG_REF_SEQ: u32 = 45;
const TAG_TEXT: u32 = 58;
const TAG_ENCRYPT: u32 = 98;

/// FIX 4.4 acceptor. `us` is this venue's CompID; `them` is the expected client CompID.
pub struct Fix {
    us: String,
    them: String,
    sessions: HashMap<SessionId, Sess>,
}

struct Sess {
    next_in: u64,
    next_out: u64,
    logged_on: bool,
    last_out: Instant,
    hb: Duration,
}

impl Fix {
    pub fn new(us: impl Into<String>, them: impl Into<String>) -> Self {
        Self {
            us: us.into(),
            them: them.into(),
            sessions: HashMap::new(),
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

    fn handle(
        &mut self,
        session: SessionId,
        msg: &RawMessage<'_>,
        consumed: usize,
        reply: &mut [u8],
    ) -> ParseOutcome {
        let begin = msg.begin_string().unwrap_or("");
        if begin != BEGIN {
            return ParseOutcome::Disconnect {
                bytes: 0,
                consumed,
            };
        }

        let inbound_seq = match msg.get_field_as::<u64>(TAG_SEQ) {
            Ok(n) => n,
            Err(_) => {
                return ParseOutcome::Disconnect {
                    bytes: 0,
                    consumed,
                };
            }
        };

        let sender = msg.get_field_str(TAG_SENDER).unwrap_or("");
        let target = msg.get_field_str(TAG_TARGET).unwrap_or("");
        if sender != self.them || target != self.us {
            let seq_out = self
                .sessions
                .get(&session)
                .map(|s| s.next_out)
                .unwrap_or(1);
            let n = self.reject(seq_out, inbound_seq, "compid", reply);
            return ParseOutcome::Disconnect {
                bytes: n,
                consumed,
            };
        }

        let ty = msg.msg_type();
        let now = Instant::now();

        if *ty == MsgType::Logon {
            if inbound_seq != 1 {
                let n = self.reject(1, inbound_seq, "seq", reply);
                return ParseOutcome::Disconnect {
                    bytes: n,
                    consumed,
                };
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
                    logged_on: true,
                    last_out: now,
                    hb: Duration::from_secs(hb_secs),
                },
            );
            return ParseOutcome::Reply {
                bytes: n,
                consumed,
            };
        }

        let (next_in, next_out, logged_on) = match self.sessions.get(&session) {
            Some(s) => (s.next_in, s.next_out, s.logged_on),
            None => {
                return ParseOutcome::Disconnect {
                    bytes: 0,
                    consumed,
                };
            }
        };

        if inbound_seq != next_in {
            let n = self.reject(next_out, inbound_seq, "seq", reply);
            self.sessions.remove(&session);
            return ParseOutcome::Disconnect {
                bytes: n,
                consumed,
            };
        }

        if !logged_on {
            self.sessions.remove(&session);
            return ParseOutcome::Disconnect {
                bytes: 0,
                consumed,
            };
        }

        if let Some(sess) = self.sessions.get_mut(&session) {
            sess.next_in += 1;
        }

        match *ty {
            MsgType::Heartbeat => ParseOutcome::Reply {
                bytes: 0,
                consumed,
            },
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
                ParseOutcome::Reply {
                    bytes: n,
                    consumed,
                }
            }
            MsgType::Logout => {
                let n = self.frame(next_out, "5", |_| {}, reply);
                if let Some(sess) = self.sessions.get_mut(&session) {
                    sess.next_out = next_out + 1;
                    sess.logged_on = false;
                    sess.last_out = now;
                }
                ParseOutcome::Reply {
                    bytes: n,
                    consumed,
                }
            }
            _ => ParseOutcome::Reply {
                bytes: 0,
                consumed,
            },
        }
    }
}

impl OrderEntry for Fix {
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

    fn encode_event(&mut self, _evt: &Event, _out: &mut [u8]) -> usize {
        0
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn decode_type(buf: &[u8]) -> MsgType {
        let mut dec = Decoder::new(buf).with_checksum_validation(true);
        dec.decode().expect("reply").msg_type().clone()
    }

    #[test]
    fn truncated_needs_more() {
        let mut fix = Fix::new("VENUE", "CLIENT");
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
        let mut fix = Fix::new("VENUE", "CLIENT");
        let mut hb = heartbeat();
        let i = hb.iter().position(|&b| b == b'=').expect("tag 8");
        hb[i + 1] ^= 0x01;
        match parse_on(&mut fix, &hb, &mut [0u8; 256]) {
            ParseOutcome::Bad { consumed } => assert!(consumed >= 1),
            ParseOutcome::NeedMore => panic!("NeedMore"),
            ParseOutcome::Reply { .. } => panic!("Reply"),
            ParseOutcome::Command { .. } => panic!("Command"),
            ParseOutcome::Disconnect { .. } => panic!("Disconnect"),
        }
    }

    #[test]
    fn logon_replies_logon() {
        let mut fix = Fix::new("VENUE", "CLIENT");
        let mut reply = [0u8; 512];
        match parse_on(&mut fix, &logon(1, 30), &mut reply) {
            ParseOutcome::Reply { bytes, consumed } => {
                assert!(consumed > 0);
                assert_eq!(decode_type(&reply[..bytes]), MsgType::Logon);
            }
            _ => panic!("expected Logon reply"),
        }
    }

    #[test]
    fn heartbeat_after_logon_consumed() {
        let mut fix = Fix::new("VENUE", "CLIENT");
        let mut reply = [0u8; 512];
        assert!(matches!(
            parse_on(&mut fix, &logon(1, 30), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        match parse_on(&mut fix, &client_frame(2, "0", |_| {}), &mut reply) {
            ParseOutcome::Reply { bytes, consumed } => {
                assert_eq!(bytes, 0);
                assert!(consumed > 0);
            }
            _ => panic!("expected silent Heartbeat"),
        }
    }

    #[test]
    fn logout_replies_logout() {
        let mut fix = Fix::new("VENUE", "CLIENT");
        let mut reply = [0u8; 512];
        assert!(matches!(
            parse_on(&mut fix, &logon(1, 30), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        match parse_on(&mut fix, &client_frame(2, "5", |_| {}), &mut reply) {
            ParseOutcome::Reply { bytes, .. } => {
                assert_eq!(decode_type(&reply[..bytes]), MsgType::Logout);
            }
            _ => panic!("expected Logout reply"),
        }
    }

    #[test]
    fn unexpected_seq_disconnects() {
        let mut fix = Fix::new("VENUE", "CLIENT");
        let mut reply = [0u8; 512];
        assert!(matches!(
            parse_on(&mut fix, &logon(1, 30), &mut reply),
            ParseOutcome::Reply { .. }
        ));
        match parse_on(&mut fix, &client_frame(9, "0", |_| {}), &mut reply) {
            ParseOutcome::Disconnect { bytes, .. } => {
                assert_eq!(decode_type(&reply[..bytes]), MsgType::Reject);
            }
            _ => panic!("expected Disconnect"),
        }
    }

    #[test]
    fn idle_heartbeat_after_interval() {
        let mut fix = Fix::new("VENUE", "CLIENT");
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
}
