//! Identity-style hasher for `blake3::Hash` keys.
//!
//! Default `HashMap`s mix every key through SipHash-1-3 — meaningful work
//! for arbitrary keys, but pure overhead for `blake3::Hash`, whose 32 bytes
//! are already cryptographically uniform. XOR-folding all incoming bytes
//! 8 at a time keeps the result uniform (blake3 output is) while skipping
//! SipHash entirely.
//!
//! XOR-fold (rather than "take the first 8 bytes") because std's derived
//! `Hash` for `[u8; 32]` routes through `write_length_prefix(32)` before
//! the payload — a "first 8 bytes win" hasher would capture the prefix,
//! not the hash. Folding makes the wire-format choice in std irrelevant:
//! constant prefix bytes XOR-cancel between any two keys of the same
//! length.
//!
//! Used by hot-path maps in `extentindex` (looked up per read extent).

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};

#[derive(Default)]
pub struct Blake3IdHasher(u64);

impl Hasher for Blake3IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            self.0 ^= u64::from_le_bytes(buf);
        }
    }
}

#[derive(Default, Clone, Copy)]
pub struct Blake3IdBuildHasher;

impl BuildHasher for Blake3IdBuildHasher {
    type Hasher = Blake3IdHasher;
    fn build_hasher(&self) -> Blake3IdHasher {
        Blake3IdHasher::default()
    }
}

/// `HashMap` with a `blake3::Hash` key and identity-style hashing.
pub type Blake3HashMap<V> = HashMap<blake3::Hash, V, Blake3IdBuildHasher>;

/// `HashSet` of `blake3::Hash` with identity-style hashing.
pub type Blake3HashSet = HashSet<blake3::Hash, Blake3IdBuildHasher>;

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_u64(h: &blake3::Hash) -> u64 {
        let mut hasher = Blake3IdHasher::default();
        std::hash::Hash::hash(h, &mut hasher);
        hasher.finish()
    }

    #[test]
    fn distinct_inputs_distinct_hashes() {
        let a = hash_u64(&blake3::hash(b"a"));
        let b = hash_u64(&blake3::hash(b"b"));
        assert_ne!(a, b);
    }

    #[test]
    fn map_roundtrips_inserted_keys() {
        let mut m: Blake3HashMap<u32> = Blake3HashMap::default();
        let h1 = blake3::hash(b"one");
        let h2 = blake3::hash(b"two");
        m.insert(h1, 1);
        m.insert(h2, 2);
        assert_eq!(m.get(&h1), Some(&1));
        assert_eq!(m.get(&h2), Some(&2));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn map_handles_many_keys() {
        // Stress: insert 1024 distinct blake3 hashes and confirm every
        // round-trips. Cheap defence against a regression that loses
        // distinguishability across the XOR fold.
        let mut m: Blake3HashMap<u32> = Blake3HashMap::default();
        for i in 0..1024u32 {
            m.insert(blake3::hash(&i.to_le_bytes()), i);
        }
        for i in 0..1024u32 {
            assert_eq!(m.get(&blake3::hash(&i.to_le_bytes())), Some(&i));
        }
    }
}
