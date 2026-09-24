//! Four pinned threads, three SPSC rings, WAL-before-apply.

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use matching_engine::codec::itch::Packet;
use matching_engine::codec::ouch::Ouch;
use matching_engine::egress::Egress;
use matching_engine::engine::MatchingEngine;
use matching_engine::ingress::Ingress;
use matching_engine::md_pub::MdPub;
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

fn main() {
    let code = launch(std::env::args().skip(1));
    if code != 0 {
        std::process::exit(code);
    }
}

fn launch(args: impl Iterator<Item = String>) -> i32 {
    let running = Arc::new(AtomicBool::new(true));
    ctrlc_or_signal(running.clone());
    serve(args, running).map_or_else(|code| code, |()| 0)
}

fn serve(args: impl Iterator<Item = String>, running: Arc<AtomicBool>) -> Result<(), i32> {
    let cfg = parse_args(args)?;

    let cmds: Arc<Spsc<Command>> = Arc::new(Spsc::new(CMD_Q));
    let ouch: Arc<Spsc<Event>> = Arc::new(Spsc::new(EVT_Q));
    let itch: Arc<Spsc<Event>> = Arc::new(Spsc::new(EVT_Q));

    let mut engine =
        MatchingEngine::new(cmds.clone(), ouch.clone(), itch.clone(), &cfg.wal, cfg.slab).map_err(
            |e| {
                eprintln!("wal: {e}");
                1
            },
        )?;

    let mut ingress = Ingress::bind(cfg.port, cmds.clone(), Ouch).map_err(|e| {
        eprintln!("bind :{}: {e}", cfg.port);
        1
    })?;
    let mut egress = Egress::new(ouch, Ouch);
    let mut itch_pub = MdPub::new(
        itch,
        cfg.itch_ip,
        cfg.itch_port,
        cfg.itch_iface,
        Packet::new(),
    )
    .map_err(|e| {
        eprintln!("itch socket: {e}");
        1
    })?;

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
    Ok(())
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Config, i32> {
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

    while let Some(a) = args.next() {
        let mut take = || -> Result<String, i32> {
            args.next().ok_or_else(|| {
                eprintln!("flag needs a value");
                2
            })
        };
        match a.as_str() {
            "-p" | "--port" => cfg.port = take()?.parse().unwrap(),
            "-w" | "--wal" => cfg.wal = PathBuf::from(take()?),
            "--itch-ip" => cfg.itch_ip = take()?.parse().unwrap(),
            "--itch-port" => cfg.itch_port = take()?.parse().unwrap(),
            "--itch-iface" => cfg.itch_iface = take()?.parse().unwrap(),
            "-i" | "--cpu-ingress" => cfg.cpu_ingress = take()?.parse().unwrap(),
            "-e" | "--cpu-engine" => cfg.cpu_engine = take()?.parse().unwrap(),
            "-o" | "--cpu-egress" => cfg.cpu_egress = take()?.parse().unwrap(),
            "-m" | "--cpu-itch" => cfg.cpu_itch = take()?.parse().unwrap(),
            "--slab" => cfg.slab = take()?.parse().unwrap(),
            "-h" | "--help" => {
                eprintln!(
                    "matching-engine [--port 12345] [--wal orderbook.wal]\n\
                     [--itch-ip 239.0.0.1] [--itch-port 5000] [--itch-iface 127.0.0.1]\n\
                     [--cpu-ingress 8] [--cpu-engine 10] [--cpu-egress 12] [--cpu-itch 14]\n\
                     [--slab 1048576]"
                );
                return Err(0);
            }
            other => {
                eprintln!("unknown arg {other}");
                return Err(2);
            }
        }
    }
    Ok(cfg)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::time::Instant;

    struct Rm(PathBuf);

    impl Drop for Rm {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_string()).collect()
    }

    fn idle_flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(true))
    }

    #[test]
    fn help_unknown_and_a_missing_value() {
        assert_eq!(serve(args(&["--help"]).into_iter(), idle_flag()), Err(0));
        assert_eq!(serve(args(&["-h"]).into_iter(), idle_flag()), Err(0));
        assert_eq!(serve(args(&["--nope"]).into_iter(), idle_flag()), Err(2));
        assert_eq!(serve(args(&["--slab"]).into_iter(), idle_flag()), Err(2));
        assert_eq!(serve(args(&["-p"]).into_iter(), idle_flag()), Err(2));
    }

    #[test]
    fn bad_wal_bind_and_itch_address_fail() {
        let missing = serve(
            args(&["--wal", "/no/such/me-bin-dir/book.wal"]).into_iter(),
            idle_flag(),
        );
        assert_eq!(missing, Err(1));

        let wal = Rm(std::env::temp_dir().join(format!("me-bin-bind-{}.wal", std::process::id())));
        let held = TcpListener::bind(("0.0.0.0", 0)).unwrap();
        let port = held.local_addr().unwrap().port().to_string();
        let bound = serve(
            args(&["--port", &port, "--wal", wal.0.to_str().unwrap()]).into_iter(),
            idle_flag(),
        );
        assert_eq!(bound, Err(1));
        drop(held);

        let wal = Rm(std::env::temp_dir().join(format!("me-bin-itch-{}.wal", std::process::id())));
        let itch = serve(
            args(&[
                "--port",
                "0",
                "--wal",
                wal.0.to_str().unwrap(),
                "--itch-iface",
                "192.0.2.1",
            ])
            .into_iter(),
            idle_flag(),
        );
        assert_eq!(itch, Err(1));
    }

    #[test]
    fn live_server_stops_when_the_flag_drops() {
        let probe = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = probe.local_addr().unwrap().port().to_string();
        let port_arg = port.clone();
        drop(probe);
        let wal = Rm(std::env::temp_dir().join(format!("me-bin-live-{}.wal", std::process::id())));
        let running = idle_flag();
        let flag = running.clone();
        let wal_arg = wal.0.to_str().unwrap().to_string();
        let handle = thread::spawn(move || {
            serve(
                args(&[
                    "--port",
                    &port_arg,
                    "--wal",
                    &wal_arg,
                    "--itch-ip",
                    "239.0.0.1",
                    "--itch-port",
                    "5000",
                    "--itch-iface",
                    "127.0.0.1",
                    "--cpu-ingress",
                    "0",
                    "--cpu-engine",
                    "0",
                    "--cpu-egress",
                    "0",
                    "--cpu-itch",
                    "0",
                    "--slab",
                    "1024",
                ])
                .into_iter(),
                running,
            )
        });

        let start = Instant::now();
        let mut connected = false;
        while start.elapsed() < Duration::from_secs(5) {
            if std::net::TcpStream::connect(("127.0.0.1", port.parse::<u16>().unwrap())).is_ok() {
                connected = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(connected);
        thread::sleep(Duration::from_millis(250));
        flag.store(false, Ordering::Relaxed);
        assert_eq!(handle.join().unwrap(), Ok(()));
    }

    #[test]
    fn sigint_handler_clears_the_flag() {
        assert_eq!(launch(args(&["--help"]).into_iter()), 0);
        assert_eq!(launch(args(&["--nope"]).into_iter()), 2);
        let running = idle_flag();
        ctrlc_or_signal(running.clone());
        thread::sleep(Duration::from_millis(40));
        handle_sig(libc::SIGINT);
        let start = Instant::now();
        while running.load(Ordering::Relaxed) && start.elapsed() < Duration::from_secs(1) {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!running.load(Ordering::Relaxed));
        unsafe {
            libc::signal(libc::SIGINT, libc::SIG_DFL);
            libc::signal(libc::SIGTERM, libc::SIG_DFL);
        }
    }
}
