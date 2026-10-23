//! Four pinned threads, three SPSC rings, WAL-before-apply.

use std::net::Ipv4Addr;
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
use matching_engine::wal::FLAGS_USAGE;
use matching_engine::{Spsc, WalConfig, WalFlags, lock_memory, pin_to_cpu};

const CMD_Q: usize = 1 << 20;
const EVT_Q: usize = 1 << 20;

struct Config {
    port: u16,
    wal: WalConfig,
    itch_ip: Ipv4Addr,
    itch_port: u16,
    itch_iface: Ipv4Addr,
    cpu_ingress: usize,
    cpu_engine: usize,
    cpu_egress: usize,
    cpu_itch: usize,
    slab: usize,
}

#[rustfmt::skip]
fn main() {
    let code = launch(std::env::args().skip(1));
    if code != 0 { std::process::exit(code) }
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
    let mut wal = WalFlags::new();
    let mut port = 12345;
    let mut itch_ip = Ipv4Addr::new(239, 0, 0, 1);
    let mut itch_port = 5000;
    let mut itch_iface = Ipv4Addr::LOCALHOST;
    let mut cpu_ingress = 8;
    let mut cpu_engine = 10;
    let mut cpu_egress = 12;
    let mut cpu_itch = 14;
    let mut slab = 1 << 20;

    while let Some(a) = args.next() {
        let mut take = || -> Result<String, i32> {
            args.next().ok_or_else(|| {
                eprintln!("flag needs a value");
                2
            })
        };
        match a.as_str() {
            "-p" | "--port" => port = take()?.parse().unwrap(),
            flag if WalFlags::owns(flag) => wal.set(flag, &take()?).map_err(|e| {
                eprintln!("{e}");
                2
            })?,
            "--itch-ip" => itch_ip = take()?.parse().unwrap(),
            "--itch-port" => itch_port = take()?.parse().unwrap(),
            "--itch-iface" => itch_iface = take()?.parse().unwrap(),
            "-i" | "--cpu-ingress" => cpu_ingress = take()?.parse().unwrap(),
            "-e" | "--cpu-engine" => cpu_engine = take()?.parse().unwrap(),
            "-o" | "--cpu-egress" => cpu_egress = take()?.parse().unwrap(),
            "-m" | "--cpu-itch" => cpu_itch = take()?.parse().unwrap(),
            "--slab" => slab = take()?.parse().unwrap(),
            "-h" | "--help" => {
                eprintln!(
                    "matching-engine {FLAGS_USAGE}\n\
                     [--port 12345] [--itch-ip 239.0.0.1] [--itch-port 5000] [--itch-iface 127.0.0.1]\n\
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
    let wal = wal.build().map_err(|e| {
        eprintln!("{e}");
        2
    })?;
    Ok(Config {
        port,
        wal,
        itch_ip,
        itch_port,
        itch_iface,
        cpu_ingress,
        cpu_engine,
        cpu_egress,
        cpu_itch,
        slab,
    })
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

    struct Rm(std::path::PathBuf);

    impl Rm {
        fn dir(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!("me-bin-{label}-{}", std::process::id()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn flags(&self) -> Vec<String> {
            wal_flags(self.0.to_str().unwrap())
        }
    }

    impl Drop for Rm {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn wal_flags(dir: &str) -> Vec<String> {
        args(&[
            "--wal-dir",
            dir,
            "--wal-epoch",
            "1",
            "--wal-segment-bytes",
            "65536",
            "--wal-spares",
            "1",
            "--wal-reserve-bytes",
            "4096",
            "--wal-preallocate",
            "off",
            "--wal-prefault",
            "off",
            "--wal-retire-queue",
            "4",
            "--wal-seal-sync",
            "off",
            "--wal-preparer-poll-us",
            "100",
            "--wal-preparer-cpu",
            "none",
        ])
    }

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_string()).collect()
    }

    fn with(mut base: Vec<String>, more: &[&str]) -> Vec<String> {
        base.extend(args(more));
        base
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
        assert_eq!(
            serve(args(&["--wal-dir", "/tmp"]).into_iter(), idle_flag()),
            Err(2)
        );
        assert_eq!(
            serve(args(&["--wal-prefault", "maybe"]).into_iter(), idle_flag()),
            Err(2)
        );
    }

    #[test]
    fn bad_wal_bind_and_itch_address_fail() {
        let missing = serve(wal_flags("/no/such/me-bin-dir").into_iter(), idle_flag());
        assert_eq!(missing, Err(1));

        let wal = Rm::dir("bind");
        let held = TcpListener::bind(("0.0.0.0", 0)).unwrap();
        let port = held.local_addr().unwrap().port().to_string();
        let bound = serve(
            with(wal.flags(), &["--port", &port]).into_iter(),
            idle_flag(),
        );
        assert_eq!(bound, Err(1));
        drop(held);

        let wal = Rm::dir("itch");
        let itch = serve(
            with(wal.flags(), &["--port", "0", "--itch-iface", "192.0.2.1"]).into_iter(),
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
        let wal = Rm::dir("live");
        let running = idle_flag();
        let flag = running.clone();
        let wal_args = wal.flags();
        let handle = thread::spawn(move || {
            serve(
                with(
                    wal_args,
                    &[
                        "--port",
                        &port_arg,
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
                    ],
                )
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
