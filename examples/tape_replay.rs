//! Same A/C/M tape as `calvera-books`'s `tape_replay`, driven through the
//! matching engine (WAL + client-id map + event publish) instead of the book
//! alone.
//!
//!   cargo run -p matching-engine --release --example tape_replay -- --synthetic 10000000
//!   cargo run -p matching-engine --release --example tape_replay -- --tape FILE --no-wal

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use calvera_books::{Price, Side};
use matching_engine::engine::MatchingEngine;
use matching_engine::spsc::Spsc;
use matching_engine::types::{Command, CommandType};

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

fn parse_args() -> (Option<PathBuf>, Option<usize>, usize, usize, bool, bool) {
    let mut tape = std::env::var_os("TAPE").map(PathBuf::from);
    let mut synthetic = None;
    let mut slab = 200_000usize;
    let mut warmup = 100_000usize;
    let mut wal = true;
    let mut latency = true;
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
            "-h" | "--help" => {
                eprintln!(
                    "tape_replay --synthetic N | --tape FILE [--slab N] [--warmup N] [--no-wal] [--no-latency]"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown {other}");
                std::process::exit(2);
            }
        }
    }
    (tape, synthetic, slab, warmup, wal, latency)
}

fn main() {
    let (tape, synth_n, slab, warmup, wal_on, latency) = parse_args();
    let ops = if let Some(n) = synth_n {
        synthetic(n)
    } else if let Some(p) = tape {
        load_dbn(&p)
    } else {
        eprintln!("--synthetic N or --tape FILE");
        std::process::exit(2);
    };
    println!("ops={}  slab={}  wal={}  warmup={}", ops.len(), slab, wal_on, warmup);

    let wal_path = std::env::temp_dir().join(format!("me-tape-{}.wal", std::process::id()));
    let wal_bytes = if wal_on {
        ((ops.len() + 2) * 64).next_multiple_of(4096).max(64 * 1024)
    } else {
        64 * 1024
    };
    let cmds = Arc::new(Spsc::new(16));
    let ouch = Arc::new(Spsc::new(1 << 16));
    let itch = Arc::new(Spsc::new(1 << 16));
    let mut eng = MatchingEngine::with_wal_size(cmds, ouch, itch, &wal_path, slab, wal_bytes)
        .expect("engine");
    eng.set_wal(wal_on);

    let warm = warmup.min(ops.len());
    for op in &ops[..warm] {
        eng.step(&to_cmd(op));
        eng.drain_events();
    }
    eng.step(&Command::reset());
    eng.drain_events();
    println!("warmup {warm} + reset");

    let mut samples = if latency {
        Vec::with_capacity(ops.len())
    } else {
        Vec::new()
    };
    let t0 = Instant::now();
    if latency {
        for op in &ops {
            let c = to_cmd(op);
            let a = Instant::now();
            eng.step(&c);
            let ns = a.elapsed().as_nanos() as u64;
            eng.drain_events();
            if ns > 0 {
                samples.push(ns.min(u32::MAX as u64) as u32);
            }
        }
    } else {
        for op in &ops {
            eng.step(&to_cmd(op));
            eng.drain_events();
        }
    }
    let dt = t0.elapsed().as_secs_f64().max(1e-12);
    let n = ops.len() as f64;
    println!(
        "throughput  {:.2} M ops/s   {:.2} ns/op   wall {:.3}s",
        n / dt / 1e6,
        dt * 1e9 / n,
        dt
    );

    if !samples.is_empty() {
        samples.sort_unstable();
        let p = |q: f64| samples[((samples.len() as f64 * q) as usize).min(samples.len() - 1)];
        println!(
            "latency     p50 {} ns   p99 {} ns   p99.9 {} ns   max {} ns",
            p(0.50),
            p(0.99),
            p(0.999),
            samples[samples.len() - 1]
        );
    }

    let _ = std::fs::remove_file(wal_path);
}
