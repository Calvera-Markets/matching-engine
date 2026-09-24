//! One thread: accept TCP, parse order entry, push Commands. Sole producer on cmdQueue.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::codec::{OrderEntry, ParseOutcome, SessionId};
use crate::spsc::Spsc;
use crate::types::Command;

const READ_BUF: usize = 4096;

struct Client {
    stream: TcpStream,
    buf: [u8; READ_BUF],
    len: usize,
}

pub struct Ingress<Oe> {
    listener: TcpListener,
    clients: HashMap<i32, Client>,
    cmds: Arc<Spsc<Command>>,
    oe: Oe,
    reply: Vec<u8>,
}

impl<Oe: OrderEntry> Ingress<Oe> {
    pub fn bind(port: u16, cmds: Arc<Spsc<Command>>, oe: Oe) -> io::Result<Self> {
        let listener = TcpListener::bind(("0.0.0.0", port))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            clients: HashMap::new(),
            cmds,
            oe,
            reply: vec![0; Oe::MAX_OUT],
        })
    }

    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
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
        let now = Instant::now();
        for fd in fds {
            let client = self.clients.get_mut(&fd).expect("fd was just listed");
            match client.stream.read(&mut client.buf[client.len..]) {
                Ok(0) => dead.push(fd),
                Ok(n) => {
                    client.len += n;
                    let mut off = 0;
                    while off < client.len {
                        match self.oe.parse(
                            &client.buf[off..client.len],
                            SessionId(fd),
                            &mut self.reply,
                        ) {
                            ParseOutcome::Command { cmd, consumed } => {
                                self.cmds.push(cmd);
                                off += consumed.max(1);
                            }
                            ParseOutcome::Reply { bytes, consumed } => {
                                if bytes > 0 {
                                    let _ = client.stream.write_all(&self.reply[..bytes]);
                                }
                                off += consumed.max(1);
                            }
                            ParseOutcome::NeedMore => break,
                            ParseOutcome::Bad { consumed } => {
                                off += consumed.max(1);
                            }
                            ParseOutcome::Disconnect { bytes, consumed } => {
                                if bytes > 0 {
                                    let _ = client.stream.write_all(&self.reply[..bytes]);
                                }
                                off += consumed.max(1);
                                dead.push(fd);
                                break;
                            }
                        }
                    }
                    if off > 0 {
                        client.buf.copy_within(off..client.len, 0);
                        client.len -= off;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let n = self.oe.on_idle(now, SessionId(fd), &mut self.reply);
                    if n > 0 {
                        let _ = client.stream.write_all(&self.reply[..n]);
                    }
                }
                Err(_) => dead.push(fd),
            }
        }
        for fd in dead {
            self.oe.on_session_end(SessionId(fd));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ouch::{self, Ouch};
    use crate::codec::{OrderEntry, ParseOutcome, SessionId};
    use crate::spsc::Spsc;
    use crate::types::{Command, CommandType, Event};
    use std::fs::OpenOptions;
    use std::net::TcpStream;
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
    use std::time::Duration;

    struct Script {
        steps: Vec<ParseOutcome>,
        idle: usize,
        ended: Vec<i32>,
    }

    impl OrderEntry for Script {
        const MAX_OUT: usize = 32;

        fn parse(&mut self, _buf: &[u8], _session: SessionId, reply: &mut [u8]) -> ParseOutcome {
            if self.steps.is_empty() {
                return ParseOutcome::NeedMore;
            }
            let next = self.steps.remove(0);
            let bytes = match &next {
                ParseOutcome::Reply { bytes, .. } | ParseOutcome::Disconnect { bytes, .. } => {
                    *bytes
                }
                _ => 0,
            };
            if bytes > 0 {
                reply[..bytes].fill(b'Z');
            }
            next
        }

        fn encode_event(&mut self, _evt: &Event, _out: &mut [u8]) -> usize {
            0
        }

        fn on_idle(&mut self, _now: Instant, _session: SessionId, reply: &mut [u8]) -> usize {
            if self.idle > 0 {
                reply[..self.idle].fill(b'I');
            }
            self.idle
        }

        fn on_session_end(&mut self, session: SessionId) {
            self.ended.push(session.0);
        }
    }

    fn try_recv(client: &mut TcpStream) -> Vec<u8> {
        let mut buf = [0u8; 64];
        let n = client.read(&mut buf).unwrap_or(0);
        buf[..n].to_vec()
    }

    fn recv_expect(client: &mut TcpStream) -> Vec<u8> {
        let mut got = Vec::new();
        for _ in 0..20 {
            got = try_recv(client);
            if !got.is_empty() {
                break;
            }
            thread_sleep();
        }
        assert!(!got.is_empty());
        got
    }

    fn thread_sleep() {
        std::thread::sleep(Duration::from_millis(5));
    }

    fn connect_to(ing: &Ingress<impl OrderEntry>) -> TcpStream {
        let port = ing.local_addr().unwrap().port();
        TcpStream::connect(("127.0.0.1", port)).unwrap()
    }

    fn accept_ready(ing: &mut Ingress<impl OrderEntry>) {
        for _ in 0..50 {
            ing.accept_all();
            thread_sleep();
            if !ing.clients.is_empty() {
                return;
            }
        }
        assert!(!ing.clients.is_empty());
    }

    fn await_cmd(ing: &mut Ingress<impl OrderEntry>, cmds: &Spsc<Command>) -> Command {
        let mut found = None;
        for _ in 0..50 {
            ing.read_all();
            if found.is_none() {
                found = cmds.pop();
            }
            if found.is_some() {
                break;
            }
            thread_sleep();
        }
        found.unwrap()
    }

    fn await_gone(ing: &mut Ingress<impl OrderEntry>) {
        let mut gone = false;
        for _ in 0..50 {
            ing.read_all();
            gone = ing.clients.is_empty();
            if gone {
                break;
            }
            thread_sleep();
        }
        assert!(gone);
    }

    #[test]
    fn scripted_parse_covers_every_outcome() {
        let cmds = Arc::new(Spsc::new(16));
        let mut ing = Ingress::bind(
            0,
            cmds.clone(),
            Script {
                steps: Vec::new(),
                idle: 4,
                ended: Vec::new(),
            },
        )
        .unwrap();
        ing.accept_all();
        assert!(matches!(
            ing.oe.parse(b"", SessionId(1), &mut [0u8; 4]),
            ParseOutcome::NeedMore
        ));
        let mut client = connect_to(&ing);
        client.set_nonblocking(true).unwrap();
        client.set_nodelay(true).unwrap();
        accept_ready(&mut ing);
        ing.read_all();
        assert_eq!(recv_expect(&mut client), b"IIII");

        ing.oe.idle = 0;
        ing.read_all();
        assert!(try_recv(&mut client).is_empty());
        assert_eq!(ing.oe.encode_event(&Event::reset(), &mut [0u8; 4]), 0);

        ing.oe.steps = vec![
            ParseOutcome::Command {
                cmd: Command::blank(CommandType::Add),
                consumed: 1,
            },
            ParseOutcome::Reply {
                bytes: 2,
                consumed: 1,
            },
            ParseOutcome::Reply {
                bytes: 0,
                consumed: 1,
            },
            ParseOutcome::Bad { consumed: 0 },
            ParseOutcome::NeedMore,
        ];
        client.write_all(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        let cmd = await_cmd(&mut ing, &cmds);
        assert_eq!(cmd.ty, CommandType::Add);
        assert_eq!(recv_expect(&mut client), b"ZZ");

        ing.oe.steps = vec![ParseOutcome::Disconnect {
            bytes: 2,
            consumed: 1,
        }];
        client.write_all(&[9]).unwrap();
        await_gone(&mut ing);
        assert_eq!(ing.oe.ended.len(), 1);
        assert_eq!(recv_expect(&mut client), b"ZZ");

        let mut client = connect_to(&ing);
        client.set_nonblocking(true).unwrap();
        accept_ready(&mut ing);
        ing.oe.steps = vec![ParseOutcome::Disconnect {
            bytes: 0,
            consumed: 1,
        }];
        client.write_all(&[1]).unwrap();
        await_gone(&mut ing);
        assert_eq!(ing.oe.ended.len(), 2);
    }

    #[test]
    fn ouch_session_uses_the_default_idle_hook() {
        let cmds = Arc::new(Spsc::new(8));
        let mut ing = Ingress::bind(0, cmds.clone(), Ouch).unwrap();
        let mut client = connect_to(&ing);
        client.set_nodelay(true).unwrap();
        accept_ready(&mut ing);
        ing.read_all();

        let mut enter = [0u8; ouch::ENTER_LEN];
        enter[0] = ouch::ENTER;
        enter[5] = b'B';
        enter[31..45].copy_from_slice(b"CLORD-00000001");
        client.write_all(&enter).unwrap();
        let cmd = await_cmd(&mut ing, &cmds);
        assert_eq!(cmd.ty, CommandType::Add);
        assert_eq!(cmd.side, calvera_books::Side::Bid);
        assert_eq!(&cmd.cl_ord_id, b"CLORD-00000001");
        drop(client);
        await_gone(&mut ing);
    }

    #[test]
    fn accept_and_read_errors_drop_the_client() {
        let cmds = Arc::new(Spsc::new(4));
        let mut ing = Ingress::bind(0, cmds, Ouch).unwrap();
        let _client = connect_to(&ing);
        accept_ready(&mut ing);
        let fd = *ing.clients.keys().next().unwrap();
        let wal = crate::TempWal::new("ingress-fd");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&wal.0)
            .unwrap();
        unsafe {
            libc::dup2(file.as_raw_fd(), fd);
        }
        ing.read_all();
        assert!(ing.clients.is_empty());

        let null = std::fs::File::open("/dev/null").unwrap();
        unsafe {
            libc::dup2(null.as_raw_fd(), ing.listener.as_raw_fd());
        }
        ing.accept_all();
    }
}
