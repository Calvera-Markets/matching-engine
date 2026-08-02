//! Engine-level benches: same mixed add/cancel idea as calvera-books `mixed`,
//! but each op is `MatchingEngine::step` (WAL off, events drained).

use std::sync::Arc;
use std::time::Instant;

use calvera_books::{Price, Side};
use criterion::{Criterion, criterion_group, criterion_main};
use matching_engine::engine::MatchingEngine;
use matching_engine::spsc::Spsc;
use matching_engine::types::{Command, CommandType, Event};

fn cmd_add(id: u32, side: Side, price: u64, qty: u64) -> Command {
    let mut c = Command::blank(CommandType::Add);
    c.user_ref = id;
    c.side = side;
    c.price = Price(price);
    c.quantity = qty;
    c
}

fn cmd_cancel(id: u32) -> Command {
    let mut c = Command::blank(CommandType::Cancel);
    c.user_ref = id;
    c
}

fn make_engine(slab: usize) -> MatchingEngine {
    let wal = std::env::temp_dir().join(format!("me-bench-{}-{}.wal", std::process::id(), slab));
    let cmds = Arc::new(Spsc::<Command>::new(8));
    let ouch = Arc::new(Spsc::<Event>::new(1 << 14));
    let itch = Arc::new(Spsc::<Event>::new(1 << 14));
    let mut eng = MatchingEngine::with_wal_size(cmds, ouch, itch, &wal, slab, 64 * 1024)
        .expect("engine");
    eng.set_wal(false);
    let _ = std::fs::remove_file(wal);
    eng
}

fn bench_mixed(c: &mut Criterion) {
    let mut eng = make_engine(16_384);
    let mid = 10_000u64;
    let mut id = 1u32;
    for i in 1..=50u64 {
        for _ in 0..4 {
            eng.step(&cmd_add(id, Side::Bid, mid - i, 1));
            id += 1;
            eng.step(&cmd_add(id, Side::Ask, mid + i, 1));
            id += 1;
            eng.drain_events();
        }
    }
    let mut next = id;
    let mut live: Vec<u32> = (1..id).collect();
    let mut i = 0usize;

    c.bench_function("mixed", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                if i % 2 == 0 || live.is_empty() {
                    let side = if next % 2 == 0 { Side::Bid } else { Side::Ask };
                    let px = if side == Side::Bid { mid - 1 } else { mid + 1 };
                    eng.step(&cmd_add(next, side, px, 1));
                    live.push(next);
                    next += 1;
                } else {
                    let k = live.remove(0);
                    eng.step(&cmd_cancel(k));
                }
                eng.drain_events();
                i += 1;
            }
            start.elapsed()
        })
    });
}

fn bench_add_cancel(c: &mut Criterion) {
    let mut eng = make_engine(1024);
    let mut on = false;
    c.bench_function("add_cancel", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                if !on {
                    eng.step(&cmd_add(1, Side::Bid, 100, 1));
                    on = true;
                } else {
                    eng.step(&cmd_cancel(1));
                    on = false;
                }
                eng.drain_events();
            }
            start.elapsed()
        })
    });
}

criterion_group!(hot, bench_mixed, bench_add_cancel);
criterion_main!(hot);
