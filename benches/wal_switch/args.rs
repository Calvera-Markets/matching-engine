//! Command-line parameters for one benchmark run. Every value is required;
//! the runner script supplies the matrix.

use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prep {
    /// The writing thread creates and maps the next file at the switch.
    Hot,
    /// Helper creates a sparse file and maps it; pages fault on first write.
    Sparse,
    /// Helper reserves blocks with `fallocate`, then maps.
    Prealloc,
    /// Helper maps a sparse file and writes every page once.
    Prefault,
    /// Helper reserves blocks and writes every page once.
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The measuring thread writes each frame itself.
    Inline,
    /// A journaller thread writes; the measuring thread waits for its cursor.
    Threaded,
}

#[derive(Debug, Clone)]
pub struct Args {
    pub dir: PathBuf,
    pub segment_bytes: usize,
    pub segments: usize,
    pub spares: usize,
    pub rate: u64,
    pub window: usize,
    pub prep: Prep,
    pub mode: Mode,
    pub label: String,
    pub out: PathBuf,
    pub cpu_writer: Option<usize>,
    pub cpu_helper: Option<usize>,
    pub cpu_journal: Option<usize>,
}

impl Prep {
    pub fn name(self) -> &'static str {
        match self {
            Prep::Hot => "hot",
            Prep::Sparse => "sparse",
            Prep::Prealloc => "prealloc",
            Prep::Prefault => "prefault",
            Prep::Both => "both",
        }
    }

    pub fn preallocates(self) -> bool {
        matches!(self, Prep::Prealloc | Prep::Both)
    }

    pub fn prefaults(self) -> bool {
        matches!(self, Prep::Prefault | Prep::Both)
    }
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::Inline => "inline",
            Mode::Threaded => "threaded",
        }
    }
}

const USAGE: &str = "wal_switch --dir DIR --segment-bytes N --segments N --spares N \
--rate MSGS_PER_S(0=unpaced) --window FRAMES --prep hot|sparse|prealloc|prefault|both \
--mode inline|threaded --label TEXT --out CSV [--cpu-writer N] [--cpu-helper N] [--cpu-journal N]";

pub fn parse(mut it: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut dir = None;
    let mut segment_bytes = None;
    let mut segments = None;
    let mut spares = None;
    let mut rate = None;
    let mut window = None;
    let mut prep = None;
    let mut mode = None;
    let mut label = None;
    let mut out = None;
    let mut cpu_writer = None;
    let mut cpu_helper = None;
    let mut cpu_journal = None;
    while let Some(flag) = it.next() {
        if flag == "--bench" {
            continue;
        }
        let value = it
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        match flag.as_str() {
            "--dir" => dir = Some(PathBuf::from(value)),
            "--segment-bytes" => segment_bytes = Some(number(&flag, &value)?),
            "--segments" => segments = Some(number(&flag, &value)?),
            "--spares" => spares = Some(number(&flag, &value)?),
            "--rate" => rate = Some(number(&flag, &value)? as u64),
            "--window" => window = Some(number(&flag, &value)?),
            "--prep" => prep = Some(parse_prep(&value)?),
            "--mode" => mode = Some(parse_mode(&value)?),
            "--label" => label = Some(value),
            "--out" => out = Some(PathBuf::from(value)),
            "--cpu-writer" => cpu_writer = Some(number(&flag, &value)?),
            "--cpu-helper" => cpu_helper = Some(number(&flag, &value)?),
            "--cpu-journal" => cpu_journal = Some(number(&flag, &value)?),
            _ => return Err(format!("unknown flag {flag}\n{USAGE}")),
        }
    }
    let args = Args {
        dir: required("--dir", dir)?,
        segment_bytes: required("--segment-bytes", segment_bytes)?,
        segments: required("--segments", segments)?,
        spares: required("--spares", spares)?,
        rate: required("--rate", rate)?,
        window: required("--window", window)?,
        prep: required("--prep", prep)?,
        mode: required("--mode", mode)?,
        label: required("--label", label)?,
        out: required("--out", out)?,
        cpu_writer,
        cpu_helper,
        cpu_journal,
    };
    if args.segment_bytes == 0 || !args.segment_bytes.is_multiple_of(super::FRAME) {
        return Err(format!(
            "--segment-bytes must be a positive multiple of {}",
            super::FRAME
        ));
    }
    if args.segments < 2 || args.spares == 0 {
        return Err("--segments must be at least 2 and --spares at least 1".into());
    }
    Ok(args)
}

fn required<T>(flag: &str, v: Option<T>) -> Result<T, String> {
    v.ok_or_else(|| format!("missing {flag}\n{USAGE}"))
}

fn number(flag: &str, v: &str) -> Result<usize, String> {
    v.replace('_', "")
        .parse()
        .map_err(|e| format!("{flag} {v}: {e}"))
}

fn parse_prep(v: &str) -> Result<Prep, String> {
    match v {
        "hot" => Ok(Prep::Hot),
        "sparse" => Ok(Prep::Sparse),
        "prealloc" => Ok(Prep::Prealloc),
        "prefault" => Ok(Prep::Prefault),
        "both" => Ok(Prep::Both),
        _ => Err(format!(
            "--prep {v}: expected hot|sparse|prealloc|prefault|both"
        )),
    }
}

fn parse_mode(v: &str) -> Result<Mode, String> {
    match v {
        "inline" => Ok(Mode::Inline),
        "threaded" => Ok(Mode::Threaded),
        _ => Err(format!("--mode {v}: expected inline|threaded")),
    }
}
