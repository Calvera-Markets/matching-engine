//! Four pinned threads, three SPSC rings, WAL-before-apply.

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use matching_engine::codec::ouch::Ouch;
use matching_engine::egress::Egress;
use matching_engine::engine::MatchingEngine;
use matching_engine::ingress::Ingress;
use matching_engine::itch_pub::ItchPub;
use matching_engine::types::{Command, Event};
use matching_engine::{Spsc, lock_memory, pin_to_cpu};

const CMD_Q: usize = 1 << 20;
const EVT_Q: usize = 1 << 20;

struct Config {
    port: u16,
    wal: PathBuf,
    itch_ip: Ipv4Addr,
    itch_port: u16,
    itch_iface: Ipv4Addr,
    cpu_ingress: usize,
    cpu_engine: usize,
    cpu_egress: usize,
    cpu_itch: usize,
    slab: usize,
}

fn parse_args() -> Config {
    let mut cfg = Config {
        port: 12345,
        wal: PathBuf::from("orderbook.wal"),
        itch_ip: Ipv4Addr::new(239, 0, 0, 1),
        itch_port: 5000,
        itch_iface: Ipv4Addr::LOCALHOST,
        cpu_ingress: 8,
        cpu_engine: 10,
        cpu_egress: 12,
        cpu_itch: 14,
        slab: 1 << 20,
    };

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let take = |args: &mut std::iter::Skip<std::env::Args>| {
            args.next().unwrap_or_else(|| {
                eprintln!("flag needs a value");
                std::process::exit(2);
            })
        };
        match a.as_str() {
            "-p" | "--port" => cfg.port = take(&mut args).parse().unwrap(),
            "-w" | "--wal" => cfg.wal = PathBuf::from(take(&mut args)),
            "--itch-ip" => cfg.itch_ip = take(&mut args).parse().unwrap(),
            "--itch-port" => cfg.itch_port = take(&mut args).parse().unwrap(),
            "--itch-iface" => cfg.itch_iface = take(&mut args).parse().unwrap(),
            "-i" | "--cpu-ingress" => cfg.cpu_ingress = take(&mut args).parse().unwrap(),
            "-e" | "--cpu-engine" => cfg.cpu_engine = take(&mut args).parse().unwrap(),
            "-o" | "--cpu-egress" => cfg.cpu_egress = take(&mut args).parse().unwrap(),
            "-m" | "--cpu-itch" => cfg.cpu_itch = take(&mut args).parse().unwrap(),
            "--slab" => cfg.slab = take(&mut args).parse().unwrap(),
            "-h" | "--help" => {
                eprintln!(
                    "matching-engine [--port 12345] [--wal orderbook.wal]\n\
                     [--itch-ip 239.0.0.1] [--itch-port 5000] [--itch-iface 127.0.0.1]\n\
                     [--cpu-ingress 8] [--cpu-engine 10] [--cpu-egress 12] [--cpu-itch 14]\n\
                     [--slab 1048576]"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg {other}");
                std::process::exit(2);
            }
        }
    }
    cfg
}

fn main() {
    let cfg = parse_args();
    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        let _ = ctrlc_or_signal(r);
    }

    let cmds: Arc<Spsc<Command>> = Arc::new(Spsc::new(CMD_Q));
    let ouch: Arc<Spsc<Event>> = Arc::new(Spsc::new(EVT_Q));
    let itch: Arc<Spsc<Event>> = Arc::new(Spsc::new(EVT_Q));

    let mut engine =
        MatchingEngine::new(cmds.clone(), ouch.clone(), itch.clone(), &cfg.wal, cfg.slab)
            .unwrap_or_else(|e| {
                eprintln!("wal: {e}");
                std::process::exit(1);
            });

    let mut ingress = Ingress::bind(cfg.port, cmds.clone(), Ouch).unwrap_or_else(|e| {
        eprintln!("bind :{}: {e}", cfg.port);
        std::process::exit(1);
    });
    let mut egress = Egress::new(ouch, Ouch);
    let mut itch_pub = ItchPub::new(itch, cfg.itch_ip, cfg.itch_port, cfg.itch_iface)
        .unwrap_or_else(|e| {
            eprintln!("itch socket: {e}");
            std::process::exit(1);
        });

    lock_memory();

    let run_in = running.clone();
    let t_in = thread::Builder::new()
        .name("ingress".into())
        .spawn(move || {
            pin_to_cpu(cfg.cpu_ingress);
            ingress.run(&run_in);
        })
        .unwrap();

    let run_eg = running.clone();
    let t_eg = thread::Builder::new()
        .name("egress".into())
        .spawn(move || {
            pin_to_cpu(cfg.cpu_egress);
            egress.run(&run_eg);
        })
        .unwrap();

    let run_it = running.clone();
    let t_it = thread::Builder::new()
        .name("itch".into())
        .spawn(move || {
            pin_to_cpu(cfg.cpu_itch);
            itch_pub.run(&run_it);
        })
        .unwrap();

    let run_en = running.clone();
    let cpu_engine = cfg.cpu_engine;
    let cmds_poison = cmds;
    let t_en = thread::Builder::new()
        .name("engine".into())
        .spawn(move || {
            pin_to_cpu(cpu_engine);
            engine.run(&run_en);
        })
        .unwrap();

    eprintln!(
        "matching-engine live  ouch=:{}  itch={}:{}",
        cfg.port, cfg.itch_ip, cfg.itch_port
    );

    while running.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(200));
    }

    let _ = t_in.join();
    cmds_poison.push(Command::poison());
    let _ = t_en.join();
    let _ = t_eg.join();
    let _ = t_it.join();
}

fn ctrlc_or_signal(running: Arc<AtomicBool>) {
    RUNNING.store(true, Ordering::Relaxed);
    unsafe {
        libc::signal(libc::SIGINT, handle_sig as *const () as usize);
        libc::signal(libc::SIGTERM, handle_sig as *const () as usize);
    }
    thread::spawn(move || {
        while RUNNING.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(50));
        }
        running.store(false, Ordering::Relaxed);
    });
}

static RUNNING: AtomicBool = AtomicBool::new(true);

extern "C" fn handle_sig(_: libc::c_int) {
    RUNNING.store(false, Ordering::Relaxed);
}
