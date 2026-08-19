//! Same A/C/M tape as `calvera-books`'s `tape_replay`, driven through the
//! matching engine (WAL + client-id map + event publish) instead of the book
//! alone.
//!
//!   cargo run -p matching-engine --release --example tape_replay -- --synthetic 10000000
//!   cargo run -p matching-engine --release --example tape_replay -- --tape FILE --no-wal
//!   cargo run -p matching-engine --release --example tape_replay -- --synthetic 10000000 --no-wal --codec ouch
//!   cargo run -p matching-engine --release --example tape_replay --features fix -- --synthetic 10000000 --no-wal --codec fix
//!   cargo run -p matching-engine --release --example tape_replay --features sbe -- --synthetic 10000000 --no-wal --codec sbe

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use calvera_books::{Price, Side};
use matching_engine::codec::itch::Packet;
use matching_engine::codec::ouch::{self, Ouch};
use matching_engine::codec::{MarketData, OrderEntry};
#[cfg(any(feature = "fix", feature = "sbe"))]
use matching_engine::codec::{ParseOutcome, SessionId};
use matching_engine::engine::MatchingEngine;
use matching_engine::spsc::Spsc;
use matching_engine::types::{Command, CommandType, Event};

#[derive(Clone, Copy)]
enum Action {
    Add,
    Cancel,
    Modify,
}

struct TapeOp {
    action: Action,
    id: u64,
    side: Side,
    price: Price,
    qty: u64,
}

fn to_cmd(op: &TapeOp) -> Command {
    let mut c = Command::blank(match op.action {
        Action::Add => CommandType::Add,
        Action::Cancel => CommandType::Cancel,
        Action::Modify => CommandType::Modify,
    });
    c.client_fd = 1;
    c.user_ref = op.id as u32;
    c.side = op.side;
    c.price = op.price;
    c.quantity = op.qty;
    c
}

fn synthetic(n: usize) -> Vec<TapeOp> {
    const MID: u64 = 10_000;
    const SPREAD: u64 = 50;
    const TARGET: usize = 4_000;
    struct Live {
        id: u64,
        side: Side,
        price: Price,
        qty: u64,
    }
    let mut ops = Vec::with_capacity(n);
    let mut live = Vec::with_capacity(TARGET * 2);
    let mut next_id = 1u64;
    let mut rng = 0xC0FFEE_u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let seed = TARGET.min(n);
    for i in 0..seed {
        let side = if i % 2 == 0 { Side::Bid } else { Side::Ask };
        let off = (i as u64 / 2) % SPREAD + 1;
        let price = Price(if side == Side::Bid { MID - off } else { MID + off });
        live.push(Live { id: next_id, side, price, qty: 1 });
        ops.push(TapeOp { action: Action::Add, id: next_id, side, price, qty: 1 });
        next_id += 1;
    }
    while ops.len() < n {
        let roll = next() % 100;
        let occ = live.len();
        let force_add = occ < TARGET / 2;
        let force_cancel = occ > TARGET * 2 && !live.is_empty();
        if force_cancel || (!force_add && !live.is_empty() && roll < 45) {
            let i = (next() as usize) % live.len();
            let g = live.swap_remove(i);
            ops.push(TapeOp { action: Action::Cancel, id: g.id, side: g.side, price: g.price, qty: g.qty });
        } else if !force_add && !live.is_empty() && roll < 55 {
            let i = (next() as usize) % live.len();
            let qty = (next() % 4) + 1;
            let off = (next() % SPREAD) + 1;
            let side = live[i].side;
            let price = Price(if side == Side::Bid { MID - off } else { MID + off });
            live[i].price = price;
            live[i].qty = qty;
            ops.push(TapeOp { action: Action::Modify, id: live[i].id, side, price, qty });
        } else {
            let side = if next() % 2 == 0 { Side::Bid } else { Side::Ask };
            let off = (next() % SPREAD) + 1;
            let price = Price(if side == Side::Bid { MID - off } else { MID + off });
            let qty = (next() % 4) + 1;
            live.push(Live { id: next_id, side, price, qty });
            ops.push(TapeOp { action: Action::Add, id: next_id, side, price, qty });
            next_id += 1;
        }
    }
    ops
}

fn load_dbn(path: &Path) -> Vec<TapeOp> {
    use dbn::decode::{DbnDecoder, DecodeRecordRef};
    use dbn::MboMsg;
    let mut dec = DbnDecoder::from_zstd_file(path).unwrap_or_else(|e| {
        eprintln!("{}: {e}", path.display());
        std::process::exit(2);
    });
    let mut ops = Vec::with_capacity(20_000_000);
    while let Some(rec) = dec.decode_record_ref().unwrap() {
        let Some(m) = rec.get::<MboMsg>() else { continue };
        let action = match m.action as u8 {
            b'A' => Action::Add,
            b'C' => Action::Cancel,
            b'M' => Action::Modify,
            _ => continue,
        };
        let side = match m.side as u8 {
            b'B' => Side::Bid,
            b'A' => Side::Ask,
            _ => continue,
        };
        if m.price < 0 || m.price == dbn::UNDEF_PRICE {
            continue;
        }
        ops.push(TapeOp {
            action,
            id: m.order_id,
            side,
            price: Price(m.price as u64),
            qty: m.size as u64,
        });
    }
    ops
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Codec {
    None,
    Ouch,
    #[cfg(feature = "fix")]
    Fix,
    #[cfg(feature = "sbe")]
    Sbe,
}

fn parse_codec(s: &str) -> Codec {
    match s {
        "none" => Codec::None,
        "ouch" => Codec::Ouch,
        "fix" => {
            #[cfg(feature = "fix")]
            {
                return Codec::Fix;
            }
            #[cfg(not(feature = "fix"))]
            {
                eprintln!("rebuild with --features fix");
                std::process::exit(2);
            }
        }
        "sbe" => {
            #[cfg(feature = "sbe")]
            {
                return Codec::Sbe;
            }
            #[cfg(not(feature = "sbe"))]
            {
                eprintln!("rebuild with --features sbe");
                std::process::exit(2);
            }
        }
        other => {
            eprintln!("unknown --codec {other} (none|ouch|fix|sbe)");
            std::process::exit(2);
        }
    }
}

fn parse_args() -> (Option<PathBuf>, Option<usize>, usize, usize, bool, bool, Codec) {
    let mut tape = std::env::var_os("TAPE").map(PathBuf::from);
    let mut synthetic = None;
    let mut slab = 200_000usize;
    let mut warmup = 100_000usize;
    let mut wal = true;
    let mut latency = true;
    let mut codec = Codec::None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut take = || args.next().expect("flag needs a value");
        match a.as_str() {
            "--tape" => tape = Some(PathBuf::from(take())),
            "--synthetic" => synthetic = Some(take().replace('_', "").parse().unwrap()),
            "--slab" => slab = take().replace('_', "").parse().unwrap(),
            "--warmup" => warmup = take().parse().unwrap(),
            "--no-wal" => wal = false,
            "--no-latency" => latency = false,
            "--codec" => codec = parse_codec(&take()),
            "-h" | "--help" => {
                eprintln!(
                    "tape_replay --synthetic N | --tape FILE [--slab N] [--warmup N]\n\
                     [--no-wal] [--no-latency] [--codec none|ouch|fix|sbe]"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown {other}");
                std::process::exit(2);
            }
        }
    }
    (tape, synthetic, slab, warmup, wal, latency, codec)
}

fn main() {
    let (tape, synth_n, slab, warmup, wal_on, latency, codec) = parse_args();
    let ops = if let Some(n) = synth_n {
        synthetic(n)
    } else if let Some(p) = tape {
        load_dbn(&p)
    } else {
        eprintln!("--synthetic N or --tape FILE");
        std::process::exit(2);
    };
    let codec_name = match codec {
        Codec::None => "none",
        Codec::Ouch => "ouch+itch",
        #[cfg(feature = "fix")]
        Codec::Fix => "fix+itch",
        #[cfg(feature = "sbe")]
        Codec::Sbe => "sbe",
    };
    println!(
        "ops={}  slab={}  wal={}  warmup={}  codec={}",
        ops.len(),
        slab,
        wal_on,
        warmup,
        codec_name
    );

    let wal_path = std::env::temp_dir().join(format!("me-tape-{}.wal", std::process::id()));
    let wal_bytes = if wal_on {
        ((ops.len() + 2) * 64).next_multiple_of(4096).max(64 * 1024)
    } else {
        64 * 1024
    };
    let cmds = Arc::new(Spsc::new(16));
    let private = Arc::new(Spsc::new(1 << 16));
    let public = Arc::new(Spsc::new(1 << 16));
    let mut eng = MatchingEngine::with_wal_size(
        cmds,
        private.clone(),
        public.clone(),
        &wal_path,
        slab,
        wal_bytes,
    )
    .expect("engine");
    eng.set_wal(wal_on);

    match codec {
        Codec::None => run_none(&ops, warmup, latency, &mut eng),
        Codec::Ouch => run_ouch(&ops, warmup, latency, &mut eng, &private, &public),
        #[cfg(feature = "fix")]
        Codec::Fix => run_fix(&ops, warmup, latency, &mut eng, &private, &public),
        #[cfg(feature = "sbe")]
        Codec::Sbe => run_sbe(&ops, warmup, latency, &mut eng, &private, &public),
    }

    let _ = std::fs::remove_file(wal_path);
}

fn report(n: usize, dt: f64, samples: &[u32]) {
    let n = n as f64;
    println!(
        "throughput  {:.2} M ops/s   {:.2} ns/op   wall {:.3}s",
        n / dt / 1e6,
        dt * 1e9 / n,
        dt
    );
    if samples.is_empty() {
        return;
    }
    let mut s = samples.to_vec();
    s.sort_unstable();
    let p = |q: f64| s[((s.len() as f64 * q) as usize).min(s.len() - 1)];
    println!(
        "latency     p50 {} ns   p99 {} ns   p99.9 {} ns   max {} ns",
        p(0.50),
        p(0.99),
        p(0.999),
        s[s.len() - 1]
    );
}

fn warmup_reset(eng: &mut MatchingEngine, ops: &[TapeOp], warmup: usize) {
    let warm = warmup.min(ops.len());
    for op in &ops[..warm] {
        eng.step(&to_cmd(op));
        eng.drain_events();
    }
    eng.step(&Command::reset());
    eng.drain_events();
    println!("warmup {warm} + reset");
}

fn publish(
    private: &Spsc<Event>,
    public: &Spsc<Event>,
    oe: &mut impl OrderEntry,
    md: &mut impl MarketData,
    scratch: &mut [u8],
) {
    while let Some(ev) = private.pop() {
        let _ = oe.encode_event(&ev, scratch);
    }
    while let Some(ev) = public.pop() {
        if !md.push(&ev) {
            let _ = md.take();
            let _ = md.push(&ev);
        }
    }
    let _ = md.take();
}

fn run_none(ops: &[TapeOp], warmup: usize, latency: bool, eng: &mut MatchingEngine) {
    warmup_reset(eng, ops, warmup);
    let mut samples = if latency {
        Vec::with_capacity(ops.len())
    } else {
        Vec::new()
    };
    let t0 = Instant::now();
    for op in ops {
        let c = to_cmd(op);
        if latency {
            let a = Instant::now();
            eng.step(&c);
            let ns = a.elapsed().as_nanos() as u64;
            eng.drain_events();
            if ns > 0 {
                samples.push(ns.min(u32::MAX as u64) as u32);
            }
        } else {
            eng.step(&c);
            eng.drain_events();
        }
    }
    report(ops.len(), t0.elapsed().as_secs_f64().max(1e-12), &samples);
}

fn run_ouch(
    ops: &[TapeOp],
    warmup: usize,
    latency: bool,
    eng: &mut MatchingEngine,
    private: &Spsc<Event>,
    public: &Spsc<Event>,
) {
    warmup_reset(eng, ops, warmup);
    let mut oe = Ouch;
    let mut md = Packet::new();
    let mut scratch = vec![0u8; Ouch::MAX_OUT];
    let mut wire = [0u8; 64];
    let mut samples = if latency {
        Vec::with_capacity(ops.len())
    } else {
        Vec::new()
    };
    let t0 = Instant::now();
    for op in ops {
        if latency {
            let a = Instant::now();
            let n = encode_ouch(op, &mut wire);
            let cmd = match ouch::parse(&wire[..n], 1) {
                Some((c, _)) => c,
                None => panic!("ouch parse"),
            };
            eng.step(&cmd);
            publish(private, public, &mut oe, &mut md, &mut scratch);
            let ns = a.elapsed().as_nanos() as u64;
            if ns > 0 {
                samples.push(ns.min(u32::MAX as u64) as u32);
            }
        } else {
            let n = encode_ouch(op, &mut wire);
            let cmd = ouch::parse(&wire[..n], 1).expect("ouch").0;
            eng.step(&cmd);
            publish(private, public, &mut oe, &mut md, &mut scratch);
        }
    }
    report(ops.len(), t0.elapsed().as_secs_f64().max(1e-12), &samples);
}

fn encode_ouch(op: &TapeOp, out: &mut [u8]) -> usize {
    let uref = (op.id as u32).to_be_bytes();
    match op.action {
        Action::Add => {
            out[..ouch::ENTER_LEN].fill(0);
            out[0] = ouch::ENTER;
            out[1..5].copy_from_slice(&uref);
            out[5] = if op.side == Side::Bid { b'B' } else { b'S' };
            out[6..10].copy_from_slice(&(op.qty as u32).to_be_bytes());
            out[18..26].copy_from_slice(&op.price.0.to_be_bytes());
            out[31..45].fill(b' ');
            ouch::ENTER_LEN
        }
        Action::Cancel => {
            out[0] = ouch::CANCEL;
            out[1..5].copy_from_slice(&uref);
            out[5..9].copy_from_slice(&(op.qty as u32).to_be_bytes());
            ouch::CANCEL_LEN
        }
        Action::Modify => {
            out[0] = ouch::MODIFY;
            out[1..5].copy_from_slice(&uref);
            out[5] = if op.side == Side::Bid { b'B' } else { b'S' };
            out[6..10].copy_from_slice(&(op.qty as u32).to_be_bytes());
            ouch::MODIFY_LEN
        }
    }
}

#[cfg(feature = "fix")]
fn run_fix(
    ops: &[TapeOp],
    warmup: usize,
    latency: bool,
    eng: &mut MatchingEngine,
    private: &Spsc<Event>,
    public: &Spsc<Event>,
) {
    use ironfix_tagvalue::Encoder;
    use matching_engine::codec::fix::Fix;

    warmup_reset(eng, ops, warmup);
    let mut oe = Fix::new("VENUE", "CLIENT", "AAPL");
    let mut md = Packet::new();
    let mut scratch = vec![0u8; Fix::MAX_OUT];
    let mut reply = vec![0u8; Fix::MAX_OUT];
    let sid = SessionId(1);
    {
        let mut enc = Encoder::new("FIX.4.4");
        enc.put_str(35, "A");
        enc.put_str(49, "CLIENT");
        enc.put_str(56, "VENUE");
        enc.put_int(34, 1);
        enc.put_int(98, 0);
        enc.put_int(108, 0);
        let logon = enc.finish().expect("logon").to_vec();
        assert!(matches!(
            oe.parse(&logon, sid, &mut reply),
            ParseOutcome::Reply { .. }
        ));
    }
    let mut seq = 2i64;
    let mut samples = if latency {
        Vec::with_capacity(ops.len())
    } else {
        Vec::new()
    };
    let t0 = Instant::now();
    for op in ops {
        if latency {
            let a = Instant::now();
            let wire = encode_fix(op, seq);
            seq += 1;
            let cmd = match oe.parse(&wire, sid, &mut reply) {
                ParseOutcome::Command { cmd, .. } => cmd,
                _ => panic!("fix parse"),
            };
            eng.step(&cmd);
            publish(private, public, &mut oe, &mut md, &mut scratch);
            let ns = a.elapsed().as_nanos() as u64;
            if ns > 0 {
                samples.push(ns.min(u32::MAX as u64) as u32);
            }
        } else {
            let wire = encode_fix(op, seq);
            seq += 1;
            let cmd = match oe.parse(&wire, sid, &mut reply) {
                ParseOutcome::Command { cmd, .. } => cmd,
                _ => panic!("fix parse"),
            };
            eng.step(&cmd);
            publish(private, public, &mut oe, &mut md, &mut scratch);
        }
    }
    report(ops.len(), t0.elapsed().as_secs_f64().max(1e-12), &samples);
}

#[cfg(feature = "fix")]
fn encode_fix(op: &TapeOp, seq: i64) -> Vec<u8> {
    use ironfix_tagvalue::Encoder;
    let id = op.id.to_string();
    let mut enc = Encoder::new("FIX.4.4");
    match op.action {
        Action::Add => {
            enc.put_str(35, "D");
            enc.put_str(49, "CLIENT");
            enc.put_str(56, "VENUE");
            enc.put_int(34, seq);
            enc.put_str(11, &id);
            enc.put_str(55, "AAPL");
            enc.put_str(54, if op.side == Side::Bid { "1" } else { "2" });
            enc.put_str(38, &op.qty.to_string());
            enc.put_str(44, &op.price.0.to_string());
        }
        Action::Cancel => {
            enc.put_str(35, "F");
            enc.put_str(49, "CLIENT");
            enc.put_str(56, "VENUE");
            enc.put_int(34, seq);
            enc.put_str(11, &format!("C{id}"));
            enc.put_str(41, &id);
            enc.put_str(55, "AAPL");
        }
        Action::Modify => {
            enc.put_str(35, "G");
            enc.put_str(49, "CLIENT");
            enc.put_str(56, "VENUE");
            enc.put_int(34, seq);
            enc.put_str(11, &id);
            enc.put_str(41, &id);
            enc.put_str(55, "AAPL");
            enc.put_str(54, if op.side == Side::Bid { "1" } else { "2" });
            enc.put_str(38, &op.qty.to_string());
            enc.put_str(44, &op.price.0.to_string());
        }
    }
    enc.finish().expect("fix").to_vec()
}

#[cfg(feature = "sbe")]
fn run_sbe(
    ops: &[TapeOp],
    warmup: usize,
    latency: bool,
    eng: &mut MatchingEngine,
    private: &Spsc<Event>,
    public: &Spsc<Event>,
) {
    use matching_engine::codec::sbe_md::SbeMd;
    use matching_engine::codec::sbe_oe::SbeOe;

    warmup_reset(eng, ops, warmup);
    let mut oe = SbeOe::new("AAPL");
    let mut md = SbeMd::new(b"AAPL");
    let mut scratch = vec![0u8; SbeOe::MAX_OUT];
    let mut reply = vec![0u8; SbeOe::MAX_OUT];
    let mut wire = vec![0u8; 128];
    let sid = SessionId(1);
    let mut samples = if latency {
        Vec::with_capacity(ops.len())
    } else {
        Vec::new()
    };
    let t0 = Instant::now();
    for op in ops {
        if latency {
            let a = Instant::now();
            let n = encode_sbe(op, &mut wire);
            let cmd = match oe.parse(&wire[..n], sid, &mut reply) {
                ParseOutcome::Command { cmd, .. } => cmd,
                _ => panic!("sbe parse"),
            };
            eng.step(&cmd);
            publish(private, public, &mut oe, &mut md, &mut scratch);
            let ns = a.elapsed().as_nanos() as u64;
            if ns > 0 {
                samples.push(ns.min(u32::MAX as u64) as u32);
            }
        } else {
            let n = encode_sbe(op, &mut wire);
            let cmd = match oe.parse(&wire[..n], sid, &mut reply) {
                ParseOutcome::Command { cmd, .. } => cmd,
                _ => panic!("sbe parse"),
            };
            eng.step(&cmd);
            publish(private, public, &mut oe, &mut md, &mut scratch);
        }
    }
    report(ops.len(), t0.elapsed().as_secs_f64().max(1e-12), &samples);
}

#[cfg(feature = "sbe")]
fn encode_sbe(op: &TapeOp, out: &mut [u8]) -> usize {
    use matching_engine::codec::sbe::order_entry::{
        NewOrderSingleEncoder, OrderCancelReplaceRequestEncoder, OrderCancelRequestEncoder,
        Side as SbeSide,
    };
    let user_ref = op.id as u32;
    let side = if op.side == Side::Bid {
        SbeSide::Buy
    } else {
        SbeSide::Sell
    };
    match op.action {
        Action::Add => {
            let mut enc = NewOrderSingleEncoder::wrap(out, 0);
            enc.set_user_ref(user_ref);
            enc.set_symbol(b"AAPL");
            enc.set_side(side);
            enc.set_price(op.price.0 as i64);
            enc.set_quantity(op.qty);
            enc.encoded_length()
        }
        Action::Cancel => {
            let mut enc = OrderCancelRequestEncoder::wrap(out, 0);
            enc.set_user_ref(user_ref);
            enc.set_quantity(op.qty);
            enc.encoded_length()
        }
        Action::Modify => {
            let mut enc = OrderCancelReplaceRequestEncoder::wrap(out, 0);
            enc.set_user_ref(user_ref);
            enc.set_side(side);
            enc.set_price(op.price.0 as i64);
            enc.set_quantity(op.qty);
            enc.encoded_length()
        }
    }
}
