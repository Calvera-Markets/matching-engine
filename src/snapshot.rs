//! Checkpoint of engine state: open orders, match counter, balance check.
//!
//! The engine only writes bytes. Hashing or comparing state belongs to a
//! downstream consumer that replays the log, never to the engine thread.

/// Layout version written first in every snapshot. Bump on any layout change.
pub const SNAPSHOT_VERSION: u64 = 1;

/// Bytes of a checkpoint. Layout is owned by the writer; readers must match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    bytes: Vec<u8>,
}

impl Snapshot {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Sink used while a component writes its durable state.
pub struct SnapshotSink {
    buf: Vec<u8>,
}

impl Default for SnapshotSink {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotSink {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn write_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_u64(bytes.len() as u64);
        self.buf.extend_from_slice(bytes);
    }

    pub fn finish(self) -> Snapshot {
        Snapshot::new(self.buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_round_trip_and_default_is_empty() {
        let mut sink = SnapshotSink::default();
        sink.write_u8(7);
        let snap = sink.finish();
        assert_eq!(snap.as_bytes(), &[7]);
        assert_eq!(snap.into_bytes(), vec![7]);
        assert!(SnapshotSink::default().finish().as_bytes().is_empty());
    }
}
