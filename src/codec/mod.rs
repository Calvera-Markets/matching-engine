//! Wire codecs (maps bytes to `Command`/`Event`).

#[cfg(feature = "fix")]
pub mod fix;
#[cfg(feature = "itch")]
pub mod itch;
#[cfg(feature = "ouch")]
pub mod ouch;
#[cfg(feature = "sbe")]
pub mod sbe_oe;
#[cfg(feature = "sbe")]
#[allow(dead_code, unused_imports)]
pub mod sbe {
    pub mod order_entry {
        include!(concat!(env!("OUT_DIR"), "/order_entry.rs"));
    }
    pub mod market_data {
        include!(concat!(env!("OUT_DIR"), "/market_data.rs"));
    }
}

use std::time::Instant;

use crate::types::{Command, Event};

/// Connection identity. Today this is the accepted TCP `client_fd`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub i32);

/// Result of feeding inbound bytes to an order-entry codec.
pub enum ParseOutcome {
    /// Application message → engine. `consumed` bytes of the input are done.
    Command { cmd: Command, consumed: usize },
    /// Session traffic that must go back on the same TCP (Logon, Heartbeat, Reject).
    /// `bytes` were written into the `reply` buffer; `consumed` input bytes are done.
    Reply { bytes: usize, consumed: usize },
    /// Not enough data for a complete message.
    NeedMore,
    /// Malformed bytes. Skip `consumed` (often 1) and keep scanning.
    Bad { consumed: usize },
    /// Session is dead. Write `reply[..bytes]` if nonzero, then drop the connection.
    Disconnect { bytes: usize, consumed: usize },
}

/// Private order-entry protocol: inbound parse, outbound acks, optional session idle.
pub trait OrderEntry {
    /// Scratch for `parse` replies and `encode_event`. OUCH fits in 128; FIX does not.
    /// TODO: This is a default, perhaps to be moved out
    const MAX_OUT: usize = 128;

    fn parse(&mut self, buf: &[u8], session: SessionId, reply: &mut [u8]) -> ParseOutcome;

    /// Serialize an engine `Event` into `out`. Returns bytes written, or 0 to skip.
    fn encode_event(&mut self, evt: &Event, out: &mut [u8]) -> usize;

    /// Called when the read loop is idle for `session`. FIX heartbeats; OUCH/SBE no-op.
    fn on_idle(&mut self, _now: Instant, _session: SessionId, _reply: &mut [u8]) -> usize {
        0
    }

    fn on_session_end(&mut self, _session: SessionId) {}
}

/// Public market-data protocol: pack events, flush datagrams.
pub trait MarketData {
    /// Append `evt`. `false` means the packet is full: caller flushes and retries.
    fn push(&mut self, evt: &Event) -> bool;

    /// Take the current packet if it has anything. Buffer is borrowed from `self`.
    fn take(&mut self) -> Option<&[u8]>;
}
