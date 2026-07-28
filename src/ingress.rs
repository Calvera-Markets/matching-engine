//! One thread: accept TCP, parse OUCH, push Commands. Sole producer on cmdQueue.

use std::collections::HashMap;
use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::ouch::{self, MAX_MSG};
use crate::spsc::Spsc;
use crate::types::Command;

const READ_BUF: usize = 4096;

struct Client {
    stream: TcpStream,
    buf: [u8; READ_BUF],
    len: usize,
}

pub struct Ingress {
    listener: TcpListener,
    clients: HashMap<i32, Client>,
    cmds: Arc<Spsc<Command>>,
}

impl Ingress {
    pub fn bind(port: u16, cmds: Arc<Spsc<Command>>) -> io::Result<Self> {
        let listener = TcpListener::bind(("0.0.0.0", port))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            clients: HashMap::new(),
            cmds,
        })
    }

    pub fn run(&mut self, running: &AtomicBool) {
        while running.load(Ordering::Relaxed) {
            self.accept_all();
            self.read_all();
        }
    }

    fn accept_all(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let _ = stream.set_nonblocking(true);
                    set_nodelay(&stream);
                    let fd = stream.as_raw_fd();
                    self.clients.insert(
                        fd,
                        Client {
                            stream,
                            buf: [0; READ_BUF],
                            len: 0,
                        },
                    );
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    fn read_all(&mut self) {
        let mut dead = Vec::new();
        let fds: Vec<i32> = self.clients.keys().copied().collect();
        for fd in fds {
            let Some(client) = self.clients.get_mut(&fd) else {
                continue;
            };
            match client.stream.read(&mut client.buf[client.len..]) {
                Ok(0) => dead.push(fd),
                Ok(n) => {
                    client.len += n;
                    let mut off = 0;
                    while off < client.len {
                        match ouch::parse(&client.buf[off..client.len], fd) {
                            Some((cmd, used)) => {
                                self.cmds.push(cmd);
                                off += used;
                            }
                            None => {
                                if client.len - off < MAX_MSG {
                                    break;
                                }
                                off += 1;
                            }
                        }
                    }
                    if off > 0 {
                        client.buf.copy_within(off..client.len, 0);
                        client.len -= off;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(_) => dead.push(fd),
            }
        }
        for fd in dead {
            self.clients.remove(&fd);
        }
    }
}

fn set_nodelay(stream: &TcpStream) {
    let fd = stream.as_raw_fd();
    let flag: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &flag as *const _ as *const libc::c_void,
            size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
