//! Deterministic hashing of engine state for standby comparison.
//!
//! The hasher is a fixed, non-cryptographic mixer. Two engines that have
//! applied the same log must produce the same digest. Changing the mixer
//! or the feed order is a WAL / protocol break.

/// Digests of engine state. Width is fixed so it can sit on a wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StateHash([u8; 32]);

impl StateHash {
    pub const ZERO: Self = Self([0; 32]);

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for StateHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Feeds typed fields into a digest. Callers must write in a fixed order.
pub struct StateHasher {
    state: u128,
    n: u64,
}

impl Default for StateHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl StateHasher {
    /// Seed is fixed so every process starts the same.
    pub fn new() -> Self {
        Self {
            // NOTE: fixed seed; changing it breaks hash compatibility.
            state: 0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835,
            n: 0,
        }
    }

    pub fn write_u8(&mut self, v: u8) {
        self.mix(u64::from(v));
    }

    pub fn write_u32(&mut self, v: u32) {
        self.mix(u64::from(v));
    }

    pub fn write_u64(&mut self, v: u64) {
        self.mix(v);
    }

    pub fn write_i32(&mut self, v: i32) {
        self.mix(v as u32 as u64);
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_u64(bytes.len() as u64);
        let mut i = 0;
        while i + 8 <= bytes.len() {
            let mut w = [0u8; 8];
            w.copy_from_slice(&bytes[i..i + 8]);
            self.mix(u64::from_le_bytes(w));
            i += 8;
        }
        if i < bytes.len() {
            let mut w = [0u8; 8];
            w[..bytes.len() - i].copy_from_slice(&bytes[i..]);
            self.mix(u64::from_le_bytes(w));
        }
    }

    /// Mark a section so two different layouts cannot collide by accident.
    pub fn tag(&mut self, tag: u64) {
        self.mix(tag ^ 0xA5A5_A5A5_A5A5_A5A5);
    }

    pub fn finish(self) -> StateHash {
        let mut out = [0u8; 32];
        let a = self.state.to_le_bytes();
        let b = (self.state.wrapping_mul(0xD6E8_FEB8_6659_FD93)).to_le_bytes();
        out[..16].copy_from_slice(&a);
        out[16..].copy_from_slice(&b);
        // Fold the write count into the last 8 bytes so empty and non-empty differ.
        let n = self.n.to_le_bytes();
        for i in 0..8 {
            out[24 + i] ^= n[i];
        }
        StateHash(out)
    }

    fn mix(&mut self, v: u64) {
        self.n = self.n.wrapping_add(1);
        let mut x = self.state ^ (u128::from(v).wrapping_mul(0x2127_59F5_1F8B_E6A5));
        x ^= x >> 47;
        x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 47;
        self.state = x;
    }
}

/// Something that can be fed into a [`StateHasher`].
pub trait HashInto {
    fn hash_into(&self, h: &mut StateHasher);
}

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
