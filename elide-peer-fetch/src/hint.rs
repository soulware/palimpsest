//! `PrefetchHint` — typed wrapper around the wire `.prefetch` response.
//!
//! Clients consume this as advice ("these entries are worth warming"),
//! never as authoritative cache state. The on-disk `.present` bitmap
//! is the source for v1 and the bytes are returned verbatim, but the
//! wrapper hides that detail from callers — a future encoding (RLE,
//! LBA-restricted projection, etc.) won't break call sites.
//!
//! See `docs/design-peer-segment-fetch.md` § "What's served" for the
//! wire/file decoupling rationale.

use bytes::Bytes;

/// Opaque advisory hint received from a peer's `.prefetch` route.
///
/// In v1 this wraps the bytes of the peer's local `cache/<ulid>.present`
/// (a packed bitset, one bit per index entry, LSB-first within each
/// byte). Callers iterate via [`Self::iter_populated_entries`] rather
/// than touching the underlying bytes; this lets the encoding evolve
/// without rippling into call sites.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefetchHint {
    bytes: Bytes,
}

impl PrefetchHint {
    /// Wrap raw wire bytes. The caller has already validated that
    /// the response was a 200 from the peer; v1 does not parse or
    /// validate the bitset structure itself (out-of-range entry
    /// indices simply iterate zero bits).
    pub fn from_wire_bytes(bytes: Bytes) -> Self {
        Self { bytes }
    }

    /// Number of bytes in the hint payload. Useful for logging /
    /// counters.
    pub fn payload_len(&self) -> usize {
        self.bytes.len()
    }

    /// Iterate the entry indices the hint marks as worth warming.
    ///
    /// `entry_count` is the number of index entries in the segment
    /// (read from the segment's `.idx` header). Indices ≥ `entry_count`
    /// are skipped — a peer with a `.present` file longer than the
    /// segment's actual entry count is treated as if the trailing
    /// bits are zero (the hint is advisory, so being conservative
    /// is correct).
    pub fn iter_populated_entries(&self, entry_count: u32) -> impl Iterator<Item = u32> + '_ {
        let bytes = self.bytes.clone();
        (0..entry_count).filter(move |&idx| {
            let byte_idx = (idx / 8) as usize;
            let bit = idx % 8;
            bytes.get(byte_idx).is_some_and(|b| b & (1 << bit) != 0)
        })
    }

    /// Count of populated entries within `entry_count`. Useful for
    /// logging and prefetch-budget decisions.
    pub fn populated_count(&self, entry_count: u32) -> u32 {
        self.iter_populated_entries(entry_count).count() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_hint_iterates_nothing() {
        let hint = PrefetchHint::from_wire_bytes(Bytes::new());
        let entries: Vec<_> = hint.iter_populated_entries(64).collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn iterates_set_bits_lsb_first() {
        // Byte 0 = 0b1010_0001 → entries 0, 5, 7 set.
        // Byte 1 = 0b0000_0010 → entry 9 set.
        let hint = PrefetchHint::from_wire_bytes(Bytes::from_static(&[0b1010_0001, 0b0000_0010]));
        let entries: Vec<_> = hint.iter_populated_entries(16).collect();
        assert_eq!(entries, vec![0, 5, 7, 9]);
    }

    #[test]
    fn entry_count_truncates_iteration() {
        let hint = PrefetchHint::from_wire_bytes(Bytes::from_static(&[0xff, 0xff]));
        // Only the first 5 bits count even though the bitset has 16 bits set.
        let entries: Vec<_> = hint.iter_populated_entries(5).collect();
        assert_eq!(entries, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn payload_length_reports_wire_size() {
        let hint = PrefetchHint::from_wire_bytes(Bytes::from_static(&[0u8; 32]));
        assert_eq!(hint.payload_len(), 32);
    }

    #[test]
    fn populated_count_matches_iteration() {
        let hint = PrefetchHint::from_wire_bytes(Bytes::from_static(&[0b1111_0000, 0b1010_1010]));
        // 4 in first byte (bits 4..8) + 4 in second byte = 8 total.
        assert_eq!(hint.populated_count(16), 8);
    }

    #[test]
    fn entry_index_past_end_of_payload_is_zero() {
        let hint = PrefetchHint::from_wire_bytes(Bytes::from_static(&[0xff]));
        // Entry 8 lives in byte index 1, which is past the payload —
        // treated as zero bit.
        let entries: Vec<_> = hint.iter_populated_entries(16).collect();
        assert_eq!(entries, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }
}
