//! FIX 4.4 order-entry codec.

use ironfix_core::error::DecodeError;
use ironfix_tagvalue::Decoder;

use crate::codec::{OrderEntry, ParseOutcome, SessionId};
use crate::types::Event;

/// FIX 4.4 order-entry codec
pub struct Fix;

impl OrderEntry for Fix {
    fn parse(&mut self, buf: &[u8], _session: SessionId, _reply: &mut [u8]) -> ParseOutcome {
        if buf.is_empty() {
            return ParseOutcome::NeedMore;
        }
        let mut dec = Decoder::new(buf).with_checksum_validation(true);
        match dec.decode() {
            Ok(msg) => ParseOutcome::Reply {
                bytes: 0,
                consumed: msg.len().max(1),
            },
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironfix_tagvalue::Encoder;

    fn heartbeat() -> Vec<u8> {
        let mut enc = Encoder::new("FIX.4.4");
        enc.put_str(35, "0");
        enc.put_str(49, "CLIENT");
        enc.put_str(56, "VENUE");
        enc.put_int(34, 1);
        enc.finish().expect("heartbeat").to_vec()
    }

    fn parse(buf: &[u8]) -> ParseOutcome {
        Fix.parse(buf, SessionId(1), &mut [0u8; 8])
    }

    #[test]
    fn truncated_needs_more() {
        let hb = heartbeat();
        assert!(matches!(parse(&hb[..hb.len() / 2]), ParseOutcome::NeedMore));
        assert!(matches!(parse(&[]), ParseOutcome::NeedMore));
    }

    #[test]
    fn bad_checksum_is_bad() {
        let mut hb = heartbeat();
        let i = hb.iter().position(|&b| b == b'=').expect("tag 8");
        hb[i + 1] ^= 0x01;
        match parse(&hb) {
            ParseOutcome::Bad { consumed } => assert!(consumed >= 1),
            ParseOutcome::NeedMore => panic!("expected Bad, got NeedMore"),
            ParseOutcome::Reply { .. } => panic!("expected Bad, got Reply"),
            ParseOutcome::Command { .. } => panic!("expected Bad, got Command"),
        }
    }

    #[test]
    fn complete_heartbeat_consumed() {
        let hb = heartbeat();
        match parse(&hb) {
            ParseOutcome::Reply { bytes, consumed } => {
                assert_eq!(bytes, 0);
                assert_eq!(consumed, hb.len());
            }
            ParseOutcome::NeedMore => panic!("expected Reply, got NeedMore"),
            ParseOutcome::Bad { .. } => panic!("expected Reply, got Bad"),
            ParseOutcome::Command { .. } => panic!("expected Reply, got Command"),
        }
    }
}
