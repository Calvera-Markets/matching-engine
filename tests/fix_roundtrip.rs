use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use ironfix_core::message::MsgType;
use ironfix_tagvalue::{Decoder, Encoder};
use matching_engine::codec::fix::Fix;
use matching_engine::codec::{OrderEntry, ParseOutcome, SessionId};
use matching_engine::egress::Egress;
use matching_engine::engine::MatchingEngine;
use matching_engine::ingress::Ingress;
use matching_engine::spsc::Spsc;
use matching_engine::types::{CommandType, Event};

fn client_frame(seq: i64, msg_type: &str, extra: impl FnOnce(&mut Encoder)) -> Vec<u8> {
    let mut enc = Encoder::new("FIX.4.4");
    enc.put_str(35, msg_type);
    enc.put_str(49, "CLIENT");
    enc.put_str(56, "VENUE");
    enc.put_int(34, seq);
    extra(&mut enc);
    enc.finish().expect("frame").to_vec()
}

fn decode(buf: &[u8]) -> ironfix_tagvalue::RawMessage<'_> {
    let mut dec = Decoder::new(buf).with_checksum_validation(true);
    dec.decode().expect("decode")
}

#[test]
fn encoder_parse_command_then_event_report() {
    let mut fix = Fix::new("VENUE", "CLIENT", "AAPL");
    let mut reply = [0u8; 1024];
    let sid = SessionId(1);

    let logon = client_frame(1, "A", |enc| {
        enc.put_int(98, 0);
        enc.put_int(108, 30);
    });
    assert!(matches!(
        fix.parse(&logon, sid, &mut reply),
        ParseOutcome::Reply { .. }
    ));

    let nos = client_frame(2, "D", |enc| {
        enc.put_str(11, "ORD1");
        enc.put_str(55, "AAPL");
        enc.put_str(54, "1");
        enc.put_str(38, "10");
        enc.put_str(44, "100");
    });
    let cmd = match fix.parse(&nos, sid, &mut reply) {
        ParseOutcome::Command { cmd, .. } => cmd,
        _ => panic!("expected Command"),
    };
    assert_eq!(cmd.ty, CommandType::Add);
    assert_eq!(cmd.quantity, 10);

    let ev = Event::accepted(
        sid.0,
        matching_engine::types::EventOrder {
            order_id: 7,
            user_ref: cmd.user_ref,
            price: cmd.price,
            quantity: cmd.quantity,
            side: cmd.side,
            order_state: b'L',
            cl_ord_id: cmd.cl_ord_id,
        },
    );
    let n = fix.encode_event(&ev, &mut reply);
    assert!(n > 0);
    let msg = decode(&reply[..n]);
    assert_eq!(msg.msg_type().clone(), MsgType::ExecutionReport);
    assert_eq!(msg.get_field_str(11), Some("ORD1"));
    assert_eq!(msg.get_field_str(150), Some("0"));
    assert_eq!(msg.get_field_str(39), Some("0"));
}

fn read_fix(stream: &mut TcpStream, acc: &mut Vec<u8>) -> Vec<u8> {
    let mut tmp = [0u8; 512];
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(Instant::now() < deadline, "timeout reading FIX");
        match stream.read(&mut tmp) {
            Ok(0) => panic!("eof"),
            Ok(n) => acc.extend_from_slice(&tmp[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            Err(e) => panic!("read: {e}"),
        }
        let n = {
            let mut dec = Decoder::new(acc).with_checksum_validation(true);
            dec.decode().ok().map(|msg| msg.len())
        };
        if let Some(n) = n {
            return acc.drain(..n).collect();
        }
    }
}

#[test]
fn tcp_logon_new_order_execution_report() {
    let dir = std::env::temp_dir().join(format!("me-fix-rt-{}.wal", std::process::id()));
    let cmds = Arc::new(Spsc::new(64));
    let private = Arc::new(Spsc::new(64));
    let public = Arc::new(Spsc::new(64));
    let mut engine = MatchingEngine::with_wal_size(
        cmds.clone(),
        private.clone(),
        public.clone(),
        &dir,
        1024,
        64 * 1024,
    )
    .unwrap();

    let fix = Fix::new("VENUE", "CLIENT", "AAPL");
    let mut ingress = Ingress::bind(0, cmds.clone(), fix.clone()).unwrap();
    let mut addr = ingress.local_addr().unwrap();
    if addr.ip().is_unspecified() {
        addr.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    }
    let mut egress = Egress::new(private, fix);

    let running = Arc::new(AtomicBool::new(true));
    let r1 = running.clone();
    let t_in = thread::spawn(move || ingress.run(&r1));
    let r2 = running.clone();
    let t_eg = thread::spawn(move || egress.run(&r2));
    let r3 = running.clone();
    let t_en = thread::spawn(move || engine.run(&r3));

    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();

    stream
        .write_all(&client_frame(1, "A", |enc| {
            enc.put_int(98, 0);
            enc.put_int(108, 30);
        }))
        .unwrap();
    let mut acc = Vec::new();
    let logon_reply = read_fix(&mut stream, &mut acc);
    assert_eq!(decode(&logon_reply).msg_type().clone(), MsgType::Logon);

    stream
        .write_all(&client_frame(2, "D", |enc| {
            enc.put_str(11, "ORD1");
            enc.put_str(55, "AAPL");
            enc.put_str(54, "1");
            enc.put_str(38, "10");
            enc.put_str(44, "100");
        }))
        .unwrap();
    let er = read_fix(&mut stream, &mut acc);
    let msg = decode(&er);
    assert_eq!(msg.msg_type().clone(), MsgType::ExecutionReport);
    assert_eq!(msg.get_field_str(11), Some("ORD1"));
    assert_eq!(msg.get_field_str(150), Some("0"));

    running.store(false, Ordering::Relaxed);
    cmds.push(matching_engine::types::Command::poison());
    let _ = t_in.join();
    let _ = t_en.join();
    let _ = t_eg.join();
    let _ = std::fs::remove_file(dir);
}
