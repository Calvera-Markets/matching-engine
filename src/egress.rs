//! Private order-entry replies. Sole consumer of the private event ring.

use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
