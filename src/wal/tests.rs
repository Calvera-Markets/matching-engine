use std::fs;
use std::ptr;

use calvera_books::{Price, Side};

use super::frame::{CHAIN_SEED, WalFrame};
use super::header::SegmentHeader;
use super::*;
use crate::TempWal;
use crate::types::{Command, CommandType};

const PER: usize = 4;

fn small(wal: &TempWal) -> WalConfig {
    wal.config(HEADER + PER * FRAME)
}

fn sample(ty: CommandType, side: Side, user: u32) -> Command {
    let mut cmd = Command::blank(ty);
    cmd.side = side;
    cmd.user_ref = user;
    cmd.price = Price(10 + user as u64);
    cmd.quantity = 5 + user as u64;
    cmd.client_fd = 3;
    cmd.time_in_force = 7;
    cmd.display = 8;
    cmd.cl_ord_id = [b'Q'; 14];
    cmd.capacity = 9;
    cmd
}

fn add(user: u32) -> Command {
    sample(CommandType::Add, Side::Bid, user)
}

fn open(cfg: &WalConfig) -> Result<(Wal, Vec<Command>), WalError> {
    let mut got = Vec::new();
    let w = Wal::open(cfg, |c| got.push(c))?;
    Ok((w, got))
}

fn write_n(cfg: &WalConfig, n: u32) {
    let (mut w, _) = open(cfg).unwrap();
    for user in 0..n {
        w.write(&add(user)).unwrap();
    }
}

/// Activated segment files, ordered by header index.
fn segments(cfg: &WalConfig) -> Vec<(std::path::PathBuf, SegmentHeader)> {
    let mut out = Vec::new();
    for e in fs::read_dir(&cfg.dir).unwrap() {
        let path = e.unwrap().path();
        let bytes = fs::read(&path).unwrap();
        let h = unsafe { ptr::read_unaligned(bytes.as_ptr() as *const SegmentHeader) };
        if bytes[..HEADER].iter().any(|&b| b != 0) {
            out.push((path, h));
        }
    }
    out.sort_by_key(|(_, h)| h.index);
    out
}

fn poke(path: &std::path::Path, at: usize, f: impl FnOnce(&mut [u8])) {
    let mut bytes = fs::read(path).unwrap();
    f(&mut bytes[at..]);
    fs::write(path, bytes).unwrap();
}

fn reframe(path: &std::path::Path, slot: usize, prev: u32, f: impl FnOnce(&mut WalFrame)) {
    poke(path, HEADER + slot * FRAME, |b| {
        let mut frame = unsafe { ptr::read_unaligned(b.as_ptr() as *const WalFrame) };
        f(&mut frame);
        frame.checksum = frame.chained(prev);
        unsafe { ptr::write_unaligned(b.as_mut_ptr() as *mut WalFrame, frame) };
    });
}

fn checksum_at(path: &std::path::Path, slot: usize) -> u32 {
    let bytes = fs::read(path).unwrap();
    let frame =
        unsafe { ptr::read_unaligned(bytes[HEADER + slot * FRAME..].as_ptr() as *const WalFrame) };
    frame.checksum
}

#[test]
fn round_trips_every_command_field() {
    let wal = TempWal::new("seg-roundtrip");
    let cfg = small(&wal);
    let written = [
        sample(CommandType::Add, Side::Bid, 0),
        sample(CommandType::Modify, Side::Ask, 1),
        sample(CommandType::Cancel, Side::Bid, 2),
        sample(CommandType::Reset, Side::Ask, 3),
        sample(CommandType::Poison, Side::Bid, 4),
        sample(CommandType::Entry, Side::Bid, 5),
    ];
    {
        let (mut w, got) = open(&cfg).unwrap();
        assert!(got.is_empty());
        for cmd in &written {
            w.write(cmd).unwrap();
        }
        assert_eq!(w.seq(), 6);
    }
    let (_, got) = open(&cfg).unwrap();
    assert_eq!(got.len(), written.len());
    for (got, want) in got.iter().zip(written.iter()) {
        assert_eq!(got.ty, want.ty);
        assert_eq!(got.side, want.side);
        assert_eq!(got.user_ref, want.user_ref);
        assert_eq!(got.price, want.price);
        assert_eq!(got.quantity, want.quantity);
        assert_eq!(got.client_fd, want.client_fd);
        assert_eq!(got.time_in_force, want.time_in_force);
        assert_eq!(got.display, want.display);
        assert_eq!(got.cl_ord_id, want.cl_ord_id);
        assert_eq!(got.capacity, 0);
    }
}

#[test]
fn rolls_across_many_segments_and_resumes_where_it_stopped() {
    let wal = TempWal::new("seg-many");
    let cfg = small(&wal);
    write_n(&cfg, (PER * 5 + 2) as u32);
    let segs = segments(&cfg);
    assert_eq!(segs.len(), 6);
    for (i, (_, h)) in segs.iter().enumerate() {
        assert_eq!(h.index, i as u64);
        assert_eq!(h.first_seq, (i * PER) as u64 + 1);
        assert_eq!(h.epoch, cfg.epoch);
    }
    assert_eq!(segs[0].1.prev, CHAIN_SEED);
    assert_eq!(segs[1].1.prev, checksum_at(&segs[0].0, PER - 1));

    let (mut w, got) = open(&cfg).unwrap();
    assert_eq!(got.len(), PER * 5 + 2);
    assert!(got.iter().enumerate().all(|(i, c)| c.user_ref == i as u32));
    assert_eq!(w.segment(), 5);
    w.write(&add(99)).unwrap();
    w.write(&add(100)).unwrap();
    w.write(&add(101)).unwrap();
    assert_eq!(w.segment(), 6);
    drop(w);
    let (w, got) = open(&cfg).unwrap();
    assert_eq!(got.len(), PER * 5 + 5);
    assert_eq!(got.last().unwrap().user_ref, 101);
    assert_eq!(w.seq(), (PER * 5 + 5) as u64);
}

#[test]
fn blank_spares_are_removed_on_open() {
    let wal = TempWal::new("seg-blank");
    let cfg = small(&wal);
    write_n(&cfg, 1);
    let files = || fs::read_dir(&cfg.dir).unwrap().count();
    assert_eq!(files(), 2);
    let (w, _) = open(&cfg).unwrap();
    assert_eq!(files(), 2);
    assert_eq!(w.spares_ready(), 1);
}

#[test]
fn a_torn_last_frame_is_dropped() {
    let wal = TempWal::new("seg-torn");
    let cfg = small(&wal);
    write_n(&cfg, PER as u32 + 2);
    let segs = segments(&cfg);
    poke(&segs[1].0, HEADER + FRAME + 20, |b| b[0] ^= 0xFF);
    let (mut w, got) = open(&cfg).unwrap();
    assert_eq!(got.len(), PER + 1);
    w.write(&add(77)).unwrap();
    drop(w);
    let (_, got) = open(&cfg).unwrap();
    assert_eq!(got.len(), PER + 2);
    assert_eq!(got.last().unwrap().user_ref, 77);
}

#[test]
fn a_bad_frame_before_the_tail_is_an_error() {
    let wal = TempWal::new("seg-mid");
    let cfg = small(&wal);
    write_n(&cfg, 3);
    let segs = segments(&cfg);
    poke(&segs[0].0, HEADER + FRAME + 20, |b| b[0] ^= 0xFF);
    assert!(matches!(
        open(&cfg),
        Err(WalError::Corrupt { segment: 0, offset }) if offset == HEADER + FRAME
    ));
}

#[test]
fn a_bad_frame_in_an_earlier_segment_is_an_error_even_at_its_end() {
    let wal = TempWal::new("seg-earlier");
    let cfg = small(&wal);
    write_n(&cfg, PER as u32 + 1);
    let segs = segments(&cfg);
    poke(&segs[0].0, HEADER + (PER - 1) * FRAME + 20, |b| {
        b[0] ^= 0xFF
    });
    assert!(matches!(
        open(&cfg),
        Err(WalError::Corrupt { segment: 0, .. })
    ));
}

#[test]
fn a_zeroed_frame_with_data_after_it_is_an_error() {
    let wal = TempWal::new("seg-hole");
    let cfg = small(&wal);
    write_n(&cfg, 3);
    let segs = segments(&cfg);
    poke(&segs[0].0, HEADER + FRAME, |b| b[..FRAME].fill(0));
    assert!(matches!(
        open(&cfg),
        Err(WalError::Corrupt { segment: 0, .. })
    ));
}

#[test]
fn a_sequence_gap_is_an_error() {
    let wal = TempWal::new("seg-gap");
    let cfg = small(&wal);
    write_n(&cfg, 2);
    let segs = segments(&cfg);
    let prev = checksum_at(&segs[0].0, 0);
    reframe(&segs[0].0, 1, prev, |f| f.seq = 7);
    assert!(matches!(
        open(&cfg),
        Err(WalError::Gap {
            expected: 2,
            found: 7
        })
    ));
}

#[test]
fn a_frame_from_another_layout_version_is_refused() {
    let wal = TempWal::new("seg-version");
    let cfg = small(&wal);
    write_n(&cfg, 2);
    let segs = segments(&cfg);
    let prev = checksum_at(&segs[0].0, 0);
    reframe(&segs[0].0, 1, prev, |f| {
        f.version = WAL_VERSION.wrapping_add(1)
    });
    assert!(matches!(open(&cfg), Err(WalError::Version { seq: 2, .. })));
}

#[test]
fn a_missing_segment_is_an_error() {
    let wal = TempWal::new("seg-missing");
    let cfg = small(&wal);
    write_n(&cfg, (PER * 2 + 1) as u32);
    let segs = segments(&cfg);
    fs::remove_file(&segs[1].0).unwrap();
    assert!(matches!(
        open(&cfg),
        Err(WalError::MissingSegment {
            expected: 1,
            found: 2
        })
    ));
}

#[test]
fn a_broken_chain_is_an_error() {
    let wal = TempWal::new("seg-chain");
    let cfg = small(&wal);
    write_n(&cfg, PER as u32 + 1);
    let segs = segments(&cfg);
    let h = segs[1].1;
    let forged = SegmentHeader::new(h.epoch, h.index, h.first_seq, h.prev ^ 1);
    poke(&segs[1].0, 0, |b| unsafe {
        ptr::write_unaligned(b.as_mut_ptr() as *mut SegmentHeader, forged)
    });
    assert!(matches!(
        open(&cfg),
        Err(WalError::BrokenChain { index: 1 })
    ));
}

#[test]
fn a_segment_from_another_epoch_is_refused() {
    let wal = TempWal::new("seg-epoch");
    let cfg = small(&wal);
    write_n(&cfg, 1);
    let mut other = cfg.clone();
    other.epoch = cfg.epoch + 1;
    assert!(matches!(
        open(&other),
        Err(WalError::StaleEpoch {
            index: 0,
            found: 1,
            expected: 2
        })
    ));
}

#[test]
fn a_corrupt_header_with_frames_is_an_error() {
    let wal = TempWal::new("seg-header");
    let cfg = small(&wal);
    write_n(&cfg, 1);
    let segs = segments(&cfg);
    poke(&segs[0].0, 20, |b| b[0] ^= 0xFF);
    assert!(matches!(open(&cfg), Err(WalError::BadHeader { .. })));
}

#[test]
fn full_when_no_spare_is_ready_and_nothing_is_written() {
    let wal = TempWal::new("seg-full");
    let cfg = small(&wal);
    let (mut w, _) = open(&cfg).unwrap();
    w.hold_spares(true);
    for user in 0..(PER * 2) as u32 {
        w.write(&add(user)).unwrap();
    }
    assert!(matches!(w.write(&add(50)), Err(WalError::Full)));
    assert_eq!(w.seq(), (PER * 2) as u64);
    w.hold_spares(false);
    w.write(&add(51)).unwrap();
    drop(w);
    let (_, got) = open(&cfg).unwrap();
    assert_eq!(got.len(), PER * 2 + 1);
    assert_eq!(got.last().unwrap().user_ref, 51);
}

#[test]
fn bad_config_is_refused() {
    let wal = TempWal::new("seg-cfg");
    let mut cfg = wal.config(HEADER);
    assert!(matches!(open(&cfg), Err(WalError::Config(_))));
    cfg.segment_bytes = HEADER + FRAME;
    cfg.spares = 0;
    assert!(matches!(open(&cfg), Err(WalError::Config(_))));
    cfg.spares = 1;
    cfg.dir = cfg.dir.join("nope");
    assert!(matches!(open(&cfg), Err(WalError::Config(_))));
}

#[test]
fn preallocated_and_prefaulted_segments_work() {
    let wal = TempWal::new("seg-prep");
    let mut cfg = small(&wal);
    cfg.preallocate = true;
    cfg.prefault = true;
    write_n(&cfg, PER as u32 * 2);
    let (_, got) = open(&cfg).unwrap();
    assert_eq!(got.len(), PER * 2);
}

#[test]
fn flags_build_a_config_and_report_what_is_missing() {
    let mut flags = WalFlags::new();
    for (f, v) in [
        ("--wal-dir", "/tmp"),
        ("--wal-epoch", "3"),
        ("--wal-segment-bytes", "1_048_576"),
        ("--wal-spares", "2"),
        ("--wal-preallocate", "on"),
    ] {
        assert!(WalFlags::owns(f));
        flags.set(f, v).unwrap();
    }
    assert!(flags.set("--wal-prefault", "maybe").is_err());
    assert!(!WalFlags::owns("--port"));
    let mut partial = WalFlags::new();
    partial.set("--wal-dir", "/tmp").unwrap();
    assert!(matches!(partial.build(), Err(WalError::Config(m)) if m.contains("--wal-epoch")));
    flags.set("--wal-prefault", "off").unwrap();
    let cfg = flags.build().unwrap();
    assert_eq!(cfg.segment_bytes, 1 << 20);
    assert_eq!(cfg.epoch, 3);
    assert!(cfg.preallocate && !cfg.prefault);
}
