//! Public market-data multicast. Sole consumer of the public event ring.

use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::codec::MarketData;
use crate::spsc::Spsc;
use crate::types::Event;

pub struct MdPub<Md> {
    events: Arc<Spsc<Event>>,
    sock: UdpSocket,
    dest: SocketAddrV4,
    md: Md,
}

impl<Md: MarketData> MdPub<Md> {
    pub fn new(
        events: Arc<Spsc<Event>>,
        group: Ipv4Addr,
        port: u16,
        iface: Ipv4Addr,
        md: Md,
    ) -> std::io::Result<Self> {
        let sock = UdpSocket::bind((iface, 0))?;
        sock.set_nonblocking(true)?;
        sock.set_multicast_loop_v4(true)?;
        Ok(Self {
            events,
            sock,
            dest: SocketAddrV4::new(group, port),
            md,
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
                    if !self.md.push(&evt) {
                        self.flush();
                        let _ = self.md.push(&evt);
                    }
                }
            }
        }
        self.flush();
    }

    fn flush(&mut self) {
        if let Some(bytes) = self.md.take() {
            let _ = self.sock.send_to(bytes, self.dest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::itch::Packet;
    use crate::spsc::Spsc;
    use crate::types::{Event, EventOrder};
    use calvera_books::{Price, Side};
    use std::net::{Ipv4Addr, UdpSocket};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    fn accepted() -> Event {
        Event::accepted(
            1,
            EventOrder {
                order_id: 9,
                user_ref: 1,
                price: Price(40),
                quantity: 2,
                side: Side::Bid,
                order_state: b'L',
                cl_ord_id: [b'M'; 14],
            },
        )
    }

    #[test]
    fn publishes_a_full_packet_then_the_retry() {
        let rx = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        rx.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let port = rx.local_addr().unwrap().port();
        let events = Arc::new(Spsc::new(8));
        let mut md = MdPub::new(
            events.clone(),
            Ipv4Addr::LOCALHOST,
            port,
            Ipv4Addr::LOCALHOST,
            Packet::new(),
        )
        .unwrap();
        let ev = accepted();
        while md.md.push(&ev) {}
        events.push(ev);

        let running = AtomicBool::new(true);
        thread::scope(|s| {
            s.spawn(|| md.run(&running));
            let mut buf = [0u8; 1500];
            let n = rx.recv(&mut buf).unwrap();
            assert!(n > 20);
            assert_eq!(&buf[..10], b"OB_SESSION");
            thread::sleep(Duration::from_millis(20));
            running.store(false, Ordering::Relaxed);
        });
    }

    #[test]
    fn a_foreign_interface_fails_to_bind() {
        let err = MdPub::new(
            Arc::new(Spsc::new(2)),
            Ipv4Addr::LOCALHOST,
            9,
            Ipv4Addr::new(192, 0, 2, 1),
            Packet::new(),
        );
        assert!(err.is_err());
    }
}
