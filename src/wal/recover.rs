//! Rebuild the log position from the segment files in the WAL directory.
//!
//! Segments are ordered by the index in their header. Indexes must run with
//! no gap, each header must start at the sequence after the previous
//! segment's last frame and carry that frame's checksum, and frames inside a
//! segment must chain. Only an invalid last frame of the last segment, with
//! nothing but zeros after it, is dropped as a torn write; every other
//! inconsistency is an error. Blank (never activated) files are removed.

use std::fs;

use super::config::WalConfig;
use super::frame::{CHAIN_SEED, FRAME, Read, WAL_VERSION, WalFrame};
use super::header::{HEADER, SegmentHeader, Slot};
use super::segment::Segment;
use crate::error::WalError;
use crate::types::Command;

/// Where the log ends.
pub struct Tail {
    /// The last activated segment, still mapped, or `None` for an empty log.
    pub active: Option<(Segment, SegmentHeader)>,
    /// Byte offset of the next frame in `active`.
    pub offset: usize,
    pub seq: u64,
    pub prev: u32,
    /// First unused file name number.
    pub next_name: u64,
}

pub fn recover(cfg: &WalConfig, mut visit: impl FnMut(Command)) -> Result<Tail, WalError> {
    let mut next_name = 0u64;
    let mut found = Vec::new();
    for entry in fs::read_dir(&cfg.dir)? {
        let entry = entry?;
        let file = entry.file_name();
        let Some(name) = file.to_str().and_then(Segment::parse_name) else {
            continue;
        };
        next_name = next_name.max(name + 1);
        let seg = Segment::open(&entry.path())?;
        match seg.header() {
            Slot::Active(h) => found.push((seg, h)),
            Slot::Blank => remove(seg)?,
            Slot::Invalid if seg.zero_from(HEADER) => remove(seg)?,
            Slot::Invalid => {
                return Err(WalError::BadHeader {
                    path: seg.path().to_path_buf(),
                });
            }
            Slot::Version(found) => return Err(WalError::SegmentVersion { found }),
        }
    }
    found.sort_by_key(|(_, h)| h.index);

    let mut seq = 0u64;
    let mut prev = CHAIN_SEED;
    let mut offset = HEADER;
    let count = found.len();
    let mut active = None;
    for (i, (mut seg, h)) in found.into_iter().enumerate() {
        check_header(cfg, &h, i as u64, seq, prev)?;
        let last = i + 1 == count;
        offset = replay(&mut seg, h.index, last, &mut seq, &mut prev, &mut visit)?;
        if last {
            active = Some((seg, h));
        }
    }
    Ok(Tail {
        active,
        offset,
        seq,
        prev,
        next_name,
    })
}

fn remove(seg: Segment) -> Result<(), WalError> {
    let path = seg.path().to_path_buf();
    drop(seg);
    fs::remove_file(path)?;
    Ok(())
}

fn check_header(
    cfg: &WalConfig,
    h: &SegmentHeader,
    expected: u64,
    seq: u64,
    prev: u32,
) -> Result<(), WalError> {
    if h.epoch != cfg.epoch {
        return Err(WalError::StaleEpoch {
            index: h.index,
            found: h.epoch,
            expected: cfg.epoch,
        });
    }
    // NOTE: the log starts at index 0 until retention anchors it at a snapshot.
    if h.index != expected {
        return Err(WalError::MissingSegment {
            expected,
            found: h.index,
        });
    }
    if h.first_seq != seq + 1 {
        return Err(WalError::Gap {
            expected: seq + 1,
            found: h.first_seq,
        });
    }
    if h.prev != prev {
        return Err(WalError::BrokenChain { index: h.index });
    }
    Ok(())
}

/// Replay one segment; returns the offset after its last frame.
fn replay(
    seg: &mut Segment,
    index: u64,
    last: bool,
    seq: &mut u64,
    prev: &mut u32,
    visit: &mut impl FnMut(Command),
) -> Result<usize, WalError> {
    let end = seg.end();
    let mut offset = HEADER;
    while offset + FRAME <= end {
        match unsafe { WalFrame::read(seg.at(offset), *prev) } {
            Read::Empty => {
                if !seg.zero_from(offset) {
                    return Err(WalError::Corrupt {
                        segment: index,
                        offset,
                    });
                }
                break;
            }
            Read::Invalid => {
                if last && seg.zero_from(offset + FRAME) {
                    seg.clear(offset, FRAME);
                    break;
                }
                return Err(WalError::Corrupt {
                    segment: index,
                    offset,
                });
            }
            Read::Valid(frame) => {
                if frame.version != WAL_VERSION {
                    return Err(WalError::Version {
                        seq: frame.seq,
                        found: frame.version,
                    });
                }
                if frame.seq != *seq + 1 {
                    return Err(WalError::Gap {
                        expected: *seq + 1,
                        found: frame.seq,
                    });
                }
                visit(frame.command());
                *seq = frame.seq;
                *prev = frame.checksum;
                offset += FRAME;
            }
        }
    }
    Ok(offset)
}
