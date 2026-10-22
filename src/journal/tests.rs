use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use calvera_books::{Price, Side};

use super::*;
use crate::error::{GateError, ReplicaError};
use crate::spsc::Spsc;
use crate::types::{Command, CommandType, Event};
use crate::wal::{FRAME, HEADER, Wal, WalConfig};
use crate::{MatchingEngine, NoCheck, TempWal, reject};

fn order(ty: CommandType, user: u32, side: Side, px: u64, qty: u64) -> Command {
    let mut cmd = Command::blank(ty);
    cmd.client_fd = 1;
    cmd.user_ref = user;
    cmd.side = side;
    cmd.price = Price(px);
    cmd.quantity = qty;
    cmd
}

fn tape() -> Vec<Command> {
    vec![
        order(CommandType::Add, 1, Side::Bid, 100, 10),
        order(CommandType::Add, 2, Side::Ask, 100, 4),
        order(CommandType::Add, 3, Side::Bid, 99, 3),
        order(CommandType::Cancel, 3, Side::Bid, 0, 0),
        order(CommandType::Add, 4, Side::Ask, 98, 8),
    ]
}

fn gate(replica: ReplicaMode) -> GateConfig {
    GateConfig {
        ring_capacity: 8,
        replica,
        wait: WaitStrategy::Yield,
        journaller_cpu: None,
        replicator_cpu: None,
        replica_retry_us: 100,
    }
}

fn wait_for(mut ok: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while !ok() {
        if start.elapsed() > Duration::from_secs(5) {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
    true
}

fn rings() -> (Arc<Spsc<Event>>, Arc<Spsc<Event>>) {
    (Arc::new(Spsc::new(256)), Arc::new(Spsc::new(256)))
}

/// A standby that records what it was sent and fails while `down`.
#[derive(Clone)]
struct Standby {
    down: Arc<AtomicBool>,
    got: Arc<Mutex<Vec<u64>>>,
}

impl Standby {
    fn new(down: bool) -> Self {
        Self {
            down: Arc::new(AtomicBool::new(down)),
            got: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn got(&self) -> Vec<u64> {
        self.got.lock().unwrap().clone()
    }
}

impl Transport for Standby {
    fn replicate(&mut self, seq: u64, _cmd: &Command) -> Result<(), ReplicaError> {
        if self.down.load(Ordering::Acquire) {
            return Err(ReplicaError::Unavailable("down".into()));
        }
        self.got.lock().unwrap().push(seq);
        Ok(())
    }
}

fn open_wal(cfg: &WalConfig) -> (Wal, Vec<Command>) {
    let mut recovered = Vec::new();
    let wal = Wal::open(cfg, |c| recovered.push(c)).unwrap();
    (wal, recovered)
}

fn drain(g: &mut Gated, n: usize) -> Vec<(Command, Verdict)> {
    let mut out = Vec::new();
    assert!(wait_for(|| {
        while let Some(x) = g.next() {
            out.push(x);
        }
        out.len() >= n
    }));
    out
}

#[test]
fn gated_engine_matches_inline_and_its_log_replays() {
    let inline_wal = TempWal::new("gate-inline");
    let (ouch, itch) = rings();
    let mut inline = MatchingEngine::new(
        Arc::new(Spsc::new(4)),
        ouch.clone(),
        itch,
        &inline_wal.config(64 * 1024),
        64,
    )
    .unwrap();
    for cmd in tape() {
        inline.step(&cmd);
    }
    let expected = inline.snapshot();
    let inline_events = ouch.len();

    let gated_wal = TempWal::new("gate-gated");
    let cfg = gated_wal.config(64 * 1024);
    let (wal, recovered) = open_wal(&cfg);
    let input = Arc::new(Spsc::new(64));
    let (journal, threads) =
        spawn_gated::<Standby>(&gate(ReplicaMode::Off), input.clone(), wal, None).unwrap();
    let (ouch, itch) = rings();
    let mut eng = MatchingEngine::with_journal(journal, recovered, ouch.clone(), itch, 64, NoCheck);
    for cmd in tape() {
        input.push(cmd);
    }
    input.push(Command::blank(CommandType::Poison));
    eng.run(&AtomicBool::new(true));
    threads.join().unwrap();
    assert_eq!(eng.snapshot(), expected);
    assert_eq!(ouch.len(), inline_events);
    assert_eq!(eng.journal().stats().consumed, tape().len() as u64 + 1);

    let (ouch, itch) = rings();
    let replayed = MatchingEngine::new(Arc::new(Spsc::new(4)), ouch, itch, &cfg, 64).unwrap();
    assert_eq!(replayed.snapshot(), expected);
}

#[test]
fn sync_engine_waits_for_the_standby_and_never_skips() {
    let tmp = TempWal::new("gate-sync");
    let (wal, _) = open_wal(&tmp.config(64 * 1024));
    let standby = Standby::new(true);
    let input = Arc::new(Spsc::new(64));
    let (mut g, threads) = spawn_gated(
        &gate(ReplicaMode::Sync),
        input.clone(),
        wal,
        Some(standby.clone()),
    )
    .unwrap();
    for cmd in tape().into_iter().take(3) {
        input.push(cmd);
    }
    assert!(wait_for(|| g.stats().published == 3));
    assert!(wait_for(|| g.stats().replica_failures > 0));
    thread::sleep(Duration::from_millis(5));
    assert!(g.next().is_none());
    assert_eq!(g.stats().replicated, 0);

    standby.down.store(false, Ordering::Release);
    let got = drain(&mut g, 3);
    assert!(got.iter().all(|(_, v)| *v == Verdict::Apply));
    assert_eq!(standby.got(), vec![1, 2, 3]);
    input.push(Command::blank(CommandType::Poison));
    assert_eq!(drain(&mut g, 1)[0].0.ty, CommandType::Poison);
    threads.join().unwrap();
}

#[test]
fn async_engine_runs_ahead_of_the_standby_but_not_the_journal() {
    let tmp = TempWal::new("gate-async");
    let (wal, _) = open_wal(&tmp.config(64 * 1024));
    let standby = Standby::new(true);
    let input = Arc::new(Spsc::new(64));
    let (mut g, threads) = spawn_gated(
        &gate(ReplicaMode::Async),
        input.clone(),
        wal,
        Some(standby.clone()),
    )
    .unwrap();
    for cmd in tape().into_iter().take(3) {
        input.push(cmd);
    }
    assert_eq!(drain(&mut g, 3).len(), 3);
    let stats = g.stats();
    assert_eq!(stats.published, 3);
    assert_eq!(stats.replicated, 0);
    assert!(g.next().is_none());

    standby.down.store(false, Ordering::Release);
    input.push(Command::blank(CommandType::Poison));
    assert_eq!(drain(&mut g, 1)[0].0.ty, CommandType::Poison);
    threads.join().unwrap();
    assert_eq!(standby.got(), vec![1, 2, 3]);
}

#[test]
fn a_lagging_standby_backs_up_the_ring_instead_of_dropping() {
    let tmp = TempWal::new("gate-lag");
    let (wal, _) = open_wal(&tmp.config(64 * 1024));
    let standby = Standby::new(true);
    let input = Arc::new(Spsc::new(64));
    let cfg = gate(ReplicaMode::Async);
    let (mut g, threads) = spawn_gated(&cfg, input.clone(), wal, Some(standby.clone())).unwrap();
    let n = cfg.ring_capacity as u32 + 4;
    for user in 0..n {
        input.push(order(
            CommandType::Add,
            user,
            Side::Bid,
            100 - user as u64,
            1,
        ));
    }
    assert_eq!(drain(&mut g, cfg.ring_capacity).len(), cfg.ring_capacity);
    thread::sleep(Duration::from_millis(5));
    assert!(g.next().is_none());
    assert_eq!(g.stats().published, cfg.ring_capacity as u64);

    standby.down.store(false, Ordering::Release);
    assert_eq!(drain(&mut g, 4).len(), 4);
    input.push(Command::blank(CommandType::Poison));
    drain(&mut g, 1);
    threads.join().unwrap();
    assert_eq!(standby.got(), (1..=n as u64).collect::<Vec<_>>());
}

#[test]
fn commands_the_log_refuses_reach_the_engine_as_refusals() {
    let tmp = TempWal::new("gate-refuse");
    let cfg = WalConfig {
        reserve_bytes: HEADER + 2 * FRAME,
        ..tmp.config(HEADER + 4 * FRAME)
    };
    let (mut wal, _) = open_wal(&cfg);
    let want = wal.spares_wanted();
    assert!(wait_for(|| wal.stats().spares_ready == want));
    wal.hold_spares(true);
    let input = Arc::new(Spsc::new(64));
    let (mut g, threads) =
        spawn_gated::<Standby>(&gate(ReplicaMode::Off), input.clone(), wal, None).unwrap();
    let n = 16;
    for user in 0..n {
        input.push(order(CommandType::Add, user, Side::Bid, 100, 1));
    }
    let got = drain(&mut g, n as usize);
    let applied = got.iter().take_while(|(_, v)| *v == Verdict::Apply).count();
    assert!(applied >= 4 && applied < n as usize);
    assert!(got[applied..].iter().all(|(_, v)| matches!(
        v,
        Verdict::Refuse(reject::JOURNAL_CANCEL_ONLY) | Verdict::Refuse(reject::JOURNAL_FULL)
    )));
    input.push(Command::blank(CommandType::Poison));
    drain(&mut g, 1);
    threads.join().unwrap();

    let (_, recovered) = open_wal(&cfg);
    assert_eq!(recovered.len(), applied);
}

#[test]
fn gate_config_is_validated() {
    let tmp = TempWal::new("gate-cfg");
    let wal_cfg = tmp.config(64 * 1024);
    let input = Arc::new(Spsc::new(4));
    let bad = GateConfig {
        ring_capacity: 6,
        ..gate(ReplicaMode::Off)
    };
    let (wal, _) = open_wal(&wal_cfg);
    assert!(matches!(
        spawn_gated::<Standby>(&bad, input.clone(), wal, None),
        Err(GateError::Config(_))
    ));
    let (wal, _) = open_wal(&wal_cfg);
    assert!(matches!(
        spawn_gated::<Standby>(&gate(ReplicaMode::Sync), input.clone(), wal, None),
        Err(GateError::Config(_))
    ));
    let (wal, _) = open_wal(&wal_cfg);
    assert!(matches!(
        spawn_gated(
            &gate(ReplicaMode::Off),
            input,
            wal,
            Some(Standby::new(false))
        ),
        Err(GateError::Config(_))
    ));
}
