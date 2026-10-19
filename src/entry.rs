//! Opaque journalled entry carried by a `CommandType::Entry` command.
//!
//! The engine never interprets an entry; it writes it to the WAL like any
//! command and passes it to the balance check, live and on replay. The fields
//! map one-to-one onto the journalled parts of a `Command`.

use calvera_books::{Price, Side};

use crate::types::{Command, CommandType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub kind: u8,
    pub tag: u8,
    pub channel: u32,
    pub words: [u64; 4],
}

impl Entry {
    pub fn to_command(self) -> Command {
        let mut cl_ord_id = [0u8; 14];
        cl_ord_id[..8].copy_from_slice(&self.words[2].to_le_bytes());
        cl_ord_id[8..12].copy_from_slice(&self.channel.to_le_bytes());
        cl_ord_id[12] = self.tag;
        let mut cmd = Command::blank(CommandType::Entry);
        cmd.side = Side::Bid;
        cmd.price = Price(self.words[0]);
        cmd.quantity = self.words[1];
        cmd.client_fd = (self.words[3] >> 32) as u32 as i32;
        cmd.user_ref = self.words[3] as u32;
        cmd.cl_ord_id = cl_ord_id;
        cmd.time_in_force = 0;
        cmd.display = self.kind;
        cmd
    }

    pub fn from_command(cmd: &Command) -> Self {
        let mut w2 = [0u8; 8];
        w2.copy_from_slice(&cmd.cl_ord_id[..8]);
        let mut channel = [0u8; 4];
        channel.copy_from_slice(&cmd.cl_ord_id[8..12]);
        let w3 = (u64::from(cmd.client_fd as u32) << 32) | u64::from(cmd.user_ref);
        Self {
            kind: cmd.display,
            tag: cmd.cl_ord_id[12],
            channel: u32::from_le_bytes(channel),
            words: [cmd.price.0, cmd.quantity, u64::from_le_bytes(w2), w3],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_bit() {
        let e = Entry {
            kind: 0xA5,
            tag: 0x5A,
            channel: 0xDEAD_BEEF,
            words: [u64::MAX, 1, 0x0123_4567_89AB_CDEF, 0xFFFF_FFFF_0000_0001],
        };
        let cmd = e.to_command();
        assert_eq!(cmd.ty, CommandType::Entry);
        assert_eq!(Entry::from_command(&cmd), e);
    }
}
