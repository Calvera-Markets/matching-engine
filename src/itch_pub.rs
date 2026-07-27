//! Public ITCH/MoldUDP multicast. Sole consumer of the ITCH event ring.

use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::itch::Packet;
use crate::spsc::Spsc;
use crate::types::Event;

pub struct ItchPub {
    events: Arc<Spsc<Event>>,
    sock: UdpSocket,
    dest: SocketAddrV4,
    packet: Packet,
}

impl ItchPub {
    pub fn new(
        events: Arc<Spsc<Event>>,
        group: Ipv4Addr,
        port: u16,
        iface: Ipv4Addr,
    ) -> std::io::Result<Self> {
        let sock = UdpSocket::bind((iface, 0))?;
        sock.set_nonblocking(true)?;
        sock.set_multicast_loop_v4(true)?;
        Ok(Self {
            events,
            sock,
            dest: SocketAddrV4::new(group, port),
            packet: Packet::new(),
        })
    }

    pub fn run(&mut self, running: &AtomicBool) {
        while running.load(Ordering::Relaxed) {
            match self.events.pop() {
                None => {
                    self.flush();
                    crate::pause();
                }
                Some(evt) => {
                    if !self.packet.push(&evt) {
                        self.flush();
                        let _ = self.packet.push(&evt);
                    }
                }
            }
        }
        self.flush();
    }

    fn flush(&mut self) {
        if let Some(bytes) = self.packet.take() {
            let _ = self.sock.send_to(bytes, self.dest);
        }
    }
}
