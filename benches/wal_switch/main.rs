//! Journal file-switch benchmark: latency of 64-byte frame writes into
//! memory-mapped segments, and of the write that rolls to the next segment.
//!
//! Matrix axes: the filesystem under `--dir`, how the next segment is prepared
//! (`--prep`), and whether the measuring thread writes inline or waits on a
//! journaller thread (`--mode`). See `scripts/wal_fs_bench.sh`.

mod args;
mod segment;
mod stats;
mod writer;

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use matching_engine::{Spsc, pin_to_cpu};

use args::{Args, Mode, Prep};
use segment::{Helper, Segment};
use stats::{Summary, count_over};
use writer::Writer;

pub const FRAME: usize = 64;

struct Run {
    lat: Vec<u64>,
    switches: Vec<usize>,
    stalls: usize,
}

fn main() {
    let args = match args::parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = bench(&args) {
        eprintln!("wal_switch: {e}");
        std::process::exit(1);
    }
}

fn bench(args: &Args) -> io::Result<()> {
    let per_segment = args.segment_bytes / FRAME;
    let total = per_segment * args.segments;
    let ready = Arc::new(Spsc::new(args.spares.next_power_of_two()));
    let used = Arc::new(Spsc::new(args.segments.next_power_of_two()));
    let stop = Arc::new(AtomicBool::new(false));
    let prepared = Arc::new(AtomicUsize::new(0));

    let first = Segment::create(&args.dir, 0, args.segment_bytes, prep_for_first(args.prep))?;
    let helper = if args.prep == Prep::Hot {
        None
    } else {
        let h = Helper {
            dir: args.dir.clone(),
            len: args.segment_bytes,
            prep: args.prep,
            first: 1,
            count: args.segments as u64,
            ready: ready.clone(),
            used: used.clone(),
            stop: stop.clone(),
            prepared: prepared.clone(),
        };
        let cpu = args.cpu_helper;
        Some(thread::spawn(move || {
            if let Some(cpu) = cpu {
                pin_to_cpu(cpu);
            }
            h.run()
        }))
    };
    let want = ready.cap().min(args.segments);
    while args.prep != Prep::Hot && prepared.load(Ordering::Acquire) < want {
        thread::yield_now();
    }

    let writer = Writer::new(first, args, ready.clone(), used.clone());
    if let Some(cpu) = args.cpu_writer {
        pin_to_cpu(cpu);
    }
    let run = match args.mode {
        Mode::Inline => inline(writer, args, total)?,
        Mode::Threaded => threaded(writer, args, total)?,
    };

    stop.store(true, Ordering::Relaxed);
    if let Some(h) = helper {
        h.join()
            .map_err(|_| io::Error::other("helper panicked"))??;
    }
    report(args, run)
}

/// The first segment is ready before timing starts in every mode.
fn prep_for_first(prep: Prep) -> Prep {
    match prep {
        Prep::Hot => Prep::Sparse,
        other => other,
    }
}

fn pace(start: Instant, i: usize, rate: u64) {
    if rate == 0 {
        return;
    }
    let due = start + std::time::Duration::from_nanos((i as u64 * 1_000_000_000) / rate);
    while Instant::now() < due {
        std::hint::spin_loop();
    }
}

fn inline(mut w: Writer, args: &Args, total: usize) -> io::Result<Run> {
    let mut lat = Vec::with_capacity(total);
    let mut switches = Vec::new();
    let start = Instant::now();
    for i in 0..total {
        pace(start, i, args.rate);
        let t0 = Instant::now();
        let switched = w.write(i as u64 + 1)?;
        lat.push(t0.elapsed().as_nanos() as u64);
        if switched {
            switches.push(i);
        }
    }
    let stalls = w.finish();
    Ok(Run {
        lat,
        switches,
        stalls,
    })
}

fn threaded(mut w: Writer, args: &Args, total: usize) -> io::Result<Run> {
    let inbox: Arc<Spsc<u64>> = Arc::new(Spsc::new(1 << 16));
    let cursor = Arc::new(AtomicU64::new(0));
    let journal = {
        let inbox = inbox.clone();
        let cursor = cursor.clone();
        let cpu = args.cpu_journal;
        thread::spawn(move || -> io::Result<(Vec<usize>, usize)> {
            if let Some(cpu) = cpu {
                pin_to_cpu(cpu);
            }
            let mut switches = Vec::new();
            let mut done = 0usize;
            while done < total {
                let Some(seq) = inbox.pop() else {
                    std::hint::spin_loop();
                    continue;
                };
                if w.write(seq)? {
                    switches.push(done);
                }
                cursor.store(seq, Ordering::Release);
                done += 1;
            }
            Ok((switches, w.finish()))
        })
    };
    let mut lat = Vec::with_capacity(total);
    let start = Instant::now();
    for i in 0..total {
        pace(start, i, args.rate);
        let seq = i as u64 + 1;
        let t0 = Instant::now();
        inbox.push(seq);
        while cursor.load(Ordering::Acquire) < seq {
            std::hint::spin_loop();
        }
        lat.push(t0.elapsed().as_nanos() as u64);
    }
    let (switches, stalls) = journal
        .join()
        .map_err(|_| io::Error::other("journaller panicked"))??;
    Ok(Run {
        lat,
        switches,
        stalls,
    })
}

fn report(args: &Args, run: Run) -> io::Result<()> {
    let fs = writer::fs_name(&args.dir)?;
    let mut switch_lat: Vec<u64> = run.switches.iter().map(|&i| run.lat[i]).collect();
    let mut window = Vec::new();
    for &i in &run.switches {
        let end = (i + args.window).min(run.lat.len());
        window.extend_from_slice(&run.lat[i..end]);
    }
    let over_10us = count_over(&run.lat, 10_000);
    let over_100us = count_over(&run.lat, 100_000);
    let mut all = run.lat;
    let all = Summary::of(&mut all).ok_or_else(|| io::Error::other("no samples"))?;
    let sw = Summary::of(&mut switch_lat);
    let win = Summary::of(&mut window);
    let (sw_n, sw_p50, sw_max) = match &sw {
        Some(s) => (s.n, s.p50, s.max),
        None => (0, 0, 0),
    };
    let (win_p9999, win_max) = match &win {
        Some(s) => (s.p9999, s.max),
        None => (0, 0),
    };

    let header = "label,fs,prep,mode,segment_bytes,segments,rate,writes,mean_ns,p50_ns,p99_ns,p9999_ns,max_ns,switches,switch_p50_ns,switch_max_ns,window,window_p9999_ns,window_max_ns,over_10us,over_100us,spare_stalls";
    let row = format!(
        "{},{},{},{},{},{},{},{},{:.1},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        args.label,
        fs,
        args.prep.name(),
        args.mode.name(),
        args.segment_bytes,
        args.segments,
        args.rate,
        all.n,
        all.mean,
        all.p50,
        all.p99,
        all.p9999,
        all.max,
        sw_n,
        sw_p50,
        sw_max,
        args.window,
        win_p9999,
        win_max,
        over_10us,
        over_100us,
        run.stalls
    );
    println!("{header}\n{row}");
    let fresh = !args.out.exists();
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.out)?;
    if fresh {
        writeln!(f, "{header}")?;
    }
    writeln!(f, "{row}")
}
