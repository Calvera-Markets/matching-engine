//! Private order-entry replies. Sole consumer of the private event ring.

use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::codec::OrderEntry;
use crate::spsc::Spsc;
use crate::types::Event;

const BATCH: usize = 64;

pub struct Egress<Oe> {
    events: Arc<Spsc<Event>>,
    streams: HashMap<i32, TcpStream>,
    oe: Oe,
    scratch: Vec<u8>,
}

impl<Oe: OrderEntry> Egress<Oe> {
    pub fn new(events: Arc<Spsc<Event>>, oe: Oe) -> Self {
        Self {
            events,
            streams: HashMap::new(),
            oe,
            scratch: vec![0; Oe::MAX_OUT],
        }
    }

    pub fn run(&mut self, running: &AtomicBool) {
        while running.load(Ordering::Relaxed) {
            let mut n = 0;
            while n < BATCH {
                let Some(evt) = self.events.pop() else {
                    break;
                };
                self.send(&evt);
                n += 1;
            }
            if n == 0 {
                crate::pause();
            }
        }
    }

    fn send(&mut self, evt: &Event) {
        if evt.client_fd < 0 {
            return;
        }
        let len = self.oe.encode_event(evt, &mut self.scratch);
        if len == 0 {
            return;
        }
        let fd = evt.client_fd;
        let stream = self.streams.entry(fd).or_insert_with(|| {
            // Ingress owns the accepted socket. Dup so egress can write
            // without taking that fd from the kernel accept table.
            let dup = unsafe { libc::dup(fd) };
            unsafe { TcpStream::from_raw_fd(dup) }
        });
        let _ = stream.write_all(&self.scratch[..len]);
    }
}

impl<Oe> Drop for Egress<Oe> {
    fn drop(&mut self) {
        for (_, stream) in self.streams.drain() {
            let fd = stream.into_raw_fd();
            unsafe {
                libc::close(fd);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ouch::Ouch;
    use crate::spsc::Spsc;
    use crate::types::{Event, EventOrder, EventType};
    use calvera_books::{Price, Side};
    use std::io::Read;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    fn accepted(fd: i32, side: Side) -> Event {
        Event::accepted(
            fd,
            EventOrder {
                order_id: 5,
                user_ref: 7,
                price: Price(10),
                quantity: 3,
                side,
                order_state: b'L',
                cl_ord_id: [b'E'; 14],
            },
        )
    }

    #[test]
    fn writes_acks_and_skips_silent_events() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        client.set_nodelay(true).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nodelay(true).unwrap();
        let fd = server.as_raw_fd();

        let events = Arc::new(Spsc::new(128));
        events.push(Event::reset());
        let mut quiet = Event::reset();
        quiet.client_fd = fd;
        quiet.ty = EventType::BookReset;
        events.push(quiet);
        events.push(accepted(fd, Side::Bid));
        events.push(accepted(fd, Side::Ask));
        for _ in 0..64 {
            events.push(accepted(fd, Side::Bid));
        }

        let running = AtomicBool::new(true);
        let mut eg = Egress::new(events.clone(), Ouch);
        thread::scope(|s| {
            s.spawn(|| eg.run(&running));
            let mut buf = [0u8; 64];
            let n = client.read(&mut buf).unwrap();
            assert_eq!(n, 64);
            assert_eq!(buf[0], b'A');
            assert_eq!(buf[13], b'B');
            thread::sleep(Duration::from_millis(20));
            running.store(false, Ordering::Relaxed);
        });
        drop(server);
    }
}
