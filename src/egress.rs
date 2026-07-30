//! Private OUCH replies. Sole consumer of the OUCH event ring.

use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::ouch;
use crate::spsc::Spsc;
use crate::types::Event;

const BATCH: usize = 64;

pub struct Egress {
    events: Arc<Spsc<Event>>,
    streams: HashMap<i32, TcpStream>,
}

impl Egress {
    pub fn new(events: Arc<Spsc<Event>>) -> Self {
        Self {
            events,
            streams: HashMap::new(),
        }
    }

    pub fn run(&mut self, running: &AtomicBool) {
        let mut scratch = [0u8; 128];
        while running.load(Ordering::Relaxed) {
            let mut n = 0;
            while n < BATCH {
                let Some(evt) = self.events.pop() else {
                    break;
                };
                self.send(&evt, &mut scratch);
                n += 1;
            }
            if n == 0 {
                crate::pause();
            }
        }
    }

    fn send(&mut self, evt: &Event, scratch: &mut [u8]) {
        if evt.client_fd < 0 {
            return;
        }
        let len = ouch::serialize(evt, scratch);
        if len == 0 {
            return;
        }
        let fd = evt.client_fd;
        let stream = self.streams.entry(fd).or_insert_with(|| {
            // The ingress thread owns the accepted socket. We dup so egress
            // can write without stealing that fd from the kernel accept table.
            let dup = unsafe { libc::dup(fd) };
            unsafe { TcpStream::from_raw_fd(dup) }
        });
        let _ = stream.write_all(&scratch[..len]);
    }
}

impl Drop for Egress {
    fn drop(&mut self) {
        for (_, stream) in self.streams.drain() {
            let fd = stream.into_raw_fd();
            unsafe {
                libc::close(fd);
            }
        }
    }
}
