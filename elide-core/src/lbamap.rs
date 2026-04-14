// LBA map: in-memory structure mapping logical block addresses to content hashes.
//
// The map is a BTreeMap keyed by `start_lba`. Each entry holds
// `(lba_length, extent_hash)`. It is the authoritative source for read-path
// lookups and is updated after every promoted write.
//
// Rebuild on startup:
//   1. Scan index/*.idx for uploaded segments and pending/ for not-yet-uploaded
//      segments, in ULID order (oldest first). Applying oldest-to-newest means
//      each insert naturally overwrites earlier entries for the same LBA range.
//   2. Volume::open() replays the in-progress WAL on top in a single pass
//      that also rebuilds pending_entries (see src/volume.rs).
//
// Contrast with lab47/lsvd: the reference uses a red-black tree (TreeMap) with
// a `compactPE` value encoding both logical and physical location. Palimpsest's
// map is purely logical (LBA → hash); physical location (hash → segment+offset)
// lives in the separate extent index. This means GC repacking never touches the
// LBA map.

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::PathBuf;

use log::warn;

use crate::segment;
use crate::signing;

/// A portion of a stored extent that overlaps a read request.
///
/// Returned by [`LbaMap::extents_in_range`]. Describes exactly which blocks
/// the caller needs to copy from the stored payload.
#[derive(Debug, PartialEq)]
pub struct ExtentRead {
    /// Content hash — key into the extent index to find the segment file and offset.
    pub hash: blake3::Hash,
    /// First LBA within the requested range covered by this extent.
    pub range_start: u64,
    /// One past the last LBA within the requested range covered by this extent.
    pub range_end: u64,
    /// Block offset within the stored payload for `range_start`.
    /// Byte offset into the payload = `payload_block_offset as u64 * 4096`.
    pub payload_block_offset: u32,
}

/// Value stored per LBA map entry.
#[derive(Clone, Copy)]
struct MapEntry {
    lba_length: u32,
    hash: blake3::Hash,
    /// Number of 4KB blocks from the start of the stored payload to the data
    /// for this entry's `start_lba`. Zero for freshly inserted entries;
    /// non-zero only for entries produced by splitting a larger entry —
    /// e.g. if `[0, 100) → H` is split by a write to `[30, 50)`, the
    /// resulting tail `[50, 100) → H` has `payload_block_offset = 50`.
    payload_block_offset: u32,
}

/// The live in-memory LBA map.
///
/// Maps `start_lba → MapEntry` for every committed extent. Unwritten LBA
/// ranges have no entry (implicitly zero, as the block device presents
/// unwritten blocks as zeroes).
///
/// Also tracks the set of *delta source hashes* — BLAKE3 hashes referenced
/// as `source_hash` by any live Delta entry in the segments this map was
/// built from. These sources are not directly reachable via the normal
/// `MapEntry.hash` path (a Delta entry's content hash is what's in the
/// map; the source hash is separate), so they need their own book-keeping
/// to keep GC's canonical-presence rule honest: a source DATA entry must
/// stay alive as long as any Delta depends on it for decompression. See
/// `lba_referenced_hashes()` for the fold.
///
/// The set is grow-only within a single map lifetime — when a Delta LBA
/// is later overwritten, its source hash is *not* removed from the set,
/// so GC may over-retain sources until the next rebuild. This is a
/// bounded leak (flushed on every rebuild_segments call) and is
/// acceptable because it degrades efficiency, not correctness. If
/// telemetry ever shows it matters, move to a refcount.
#[derive(Clone)]
pub struct LbaMap {
    inner: BTreeMap<u64, MapEntry>,
    delta_source_hashes: HashSet<blake3::Hash>,
}

impl LbaMap {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            delta_source_hashes: HashSet::new(),
        }
    }

    /// Insert an extent `[start_lba, start_lba + lba_length)` → `hash`,
    /// trimming or splitting any existing entries it overlaps.
    ///
    /// Called after every successful [`crate::segment::promote`] and during
    /// startup rebuild. New entries always have `payload_block_offset = 0`;
    /// non-zero offsets arise only in the split/tail entries created internally.
    pub fn insert(&mut self, start_lba: u64, lba_length: u32, hash: blake3::Hash) {
        let new_end = start_lba + lba_length as u64;

        // Step 1: Handle a predecessor entry that starts before `start_lba`
        // but whose tail overlaps the new range.
        if let Some((&pred_start, &pred)) = self.inner.range(..start_lba).next_back() {
            let pred_end = pred_start + pred.lba_length as u64;
            if pred_end > start_lba {
                self.inner.remove(&pred_start);
                // Prefix [pred_start, start_lba): same payload_block_offset.
                self.inner.insert(
                    pred_start,
                    MapEntry {
                        lba_length: (start_lba - pred_start) as u32,
                        hash: pred.hash,
                        payload_block_offset: pred.payload_block_offset,
                    },
                );
                // Suffix [new_end, pred_end): only present in the "hole punch"
                // case. payload_block_offset advances by (new_end - pred_start).
                if pred_end > new_end {
                    self.inner.insert(
                        new_end,
                        MapEntry {
                            lba_length: (pred_end - new_end) as u32,
                            hash: pred.hash,
                            payload_block_offset: pred.payload_block_offset
                                + (new_end - pred_start) as u32,
                        },
                    );
                }
            }
        }

        // Step 2: Remove all entries that start within [start_lba, new_end).
        // Collect keys first to avoid mutating the map while iterating it.
        // In typical sequential-write workloads this Vec holds 0 or 1 element.
        let overlapping: Vec<u64> = self
            .inner
            .range(start_lba..new_end)
            .map(|(&k, _)| k)
            .collect();
        for key in overlapping {
            // Key was found in range query above; remove cannot fail.
            let Some(e) = self.inner.remove(&key) else {
                continue;
            };
            let entry_end = key + e.lba_length as u64;
            if entry_end > new_end {
                // Entry extends past the new range; preserve its tail.
                // payload_block_offset advances by (new_end - key).
                self.inner.insert(
                    new_end,
                    MapEntry {
                        lba_length: (entry_end - new_end) as u32,
                        hash: e.hash,
                        payload_block_offset: e.payload_block_offset + (new_end - key) as u32,
                    },
                );
            }
        }

        self.inner.insert(
            start_lba,
            MapEntry {
                lba_length,
                hash,
                payload_block_offset: 0,
            },
        );
    }

    /// Iterate over all extents that overlap `[start_lba, end_lba)`, in ascending LBA order.
    ///
    /// Each entry describes the portion of the extent that falls within the requested range:
    /// - `hash` — identifies the stored payload via the extent index
    /// - `range_start`, `range_end` — the sub-range of LBAs within `[start_lba, end_lba)`
    ///   that this extent covers; `range_end - range_start` blocks are needed
    /// - `payload_block_offset` — block offset within the stored payload for `range_start`
    ///
    /// Unwritten gaps between extents are omitted; the caller is responsible for
    /// leaving those output bytes as zero.
    pub fn extents_in_range(&self, start_lba: u64, end_lba: u64) -> Vec<ExtentRead> {
        let mut result = Vec::new();

        // A predecessor entry (key < start_lba) may extend into the range.
        if let Some((&key, &e)) = self.inner.range(..start_lba).next_back() {
            let entry_end = key + e.lba_length as u64;
            if entry_end > start_lba {
                let range_end = entry_end.min(end_lba);
                result.push(ExtentRead {
                    hash: e.hash,
                    range_start: start_lba,
                    range_end,
                    payload_block_offset: e.payload_block_offset + (start_lba - key) as u32,
                });
            }
        }

        // All entries whose start_lba falls within [start_lba, end_lba).
        for (&key, &e) in self.inner.range(start_lba..end_lba) {
            let range_end = (key + e.lba_length as u64).min(end_lba);
            result.push(ExtentRead {
                hash: e.hash,
                range_start: key,
                range_end,
                payload_block_offset: e.payload_block_offset,
            });
        }

        result
    }

    /// Look up the extent containing `lba`.
    ///
    /// True iff the map has an extent keyed at exactly `start_lba` that
    /// covers `lba_length` blocks, has `payload_block_offset == 0`, and
    /// matches `hash`.
    ///
    /// Used by the no-op write skip in `Volume::write`: a match means the
    /// LBA map already records our exact content at the exact range, so
    /// the write can return immediately without touching the WAL, segment
    /// tree, or extent index. See `docs/design-noop-write-skip.md`.
    ///
    /// BLAKE3 length folding means a hash match would already imply a
    /// length match *for whole payloads*, but an LBA map entry may be a
    /// proper prefix of the payload (head of a split extent). The
    /// `lba_length` and `payload_block_offset == 0` checks reject those
    /// cases — skipping them would leave stale mappings in the tail of
    /// the incoming range.
    pub fn has_full_match(&self, start_lba: u64, lba_length: u32, hash: &blake3::Hash) -> bool {
        self.inner.get(&start_lba).is_some_and(|e| {
            e.lba_length == lba_length && e.payload_block_offset == 0 && &e.hash == hash
        })
    }

    /// Returns `(hash, block_offset)` where `block_offset` is the number of
    /// 4KB blocks from the start of the stored payload (identified by `hash`)
    /// to `lba`'s data. The byte offset into the segment body is
    /// `body_offset + block_offset as u64 * 4096`.
    ///
    /// Returns `None` if `lba` falls in an unwritten region.
    pub fn lookup(&self, lba: u64) -> Option<(blake3::Hash, u32)> {
        let (&start, &e) = self.inner.range(..=lba).next_back()?;
        if lba < start + e.lba_length as u64 {
            Some((e.hash, e.payload_block_offset + (lba - start) as u32))
        } else {
            None
        }
    }

    /// Number of extents in the map.
    #[allow(dead_code)] // used in tests; available for diagnostics
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return the set of all content hashes currently referenced by any LBA
    /// range, regardless of how the LBA got its hash (DATA write, DedupRef
    /// write, Delta write, or rebuilt from a segment of any entry kind),
    /// *plus* every source hash of every live Delta entry (so GC keeps
    /// the source DATA alive for decompression).
    ///
    /// **Load-bearing for the canonical-presence invariant.** GC and
    /// `redact_segment` use this to keep a DATA entry alive whenever any
    /// live LBA references its hash — including LBAs that reference the
    /// hash via a DedupRef (variant-b correctness; see
    /// `docs/architecture.md § Dedup`) and Delta entries whose stored
    /// content is decompressed against a separate `source_hash`. The
    /// naming is deliberately precise (`lba_`-prefixed) to discourage a
    /// future "optimisation" to a DATA-only filter: such a filter would
    /// drop DATA entries whose only live referrer is a sibling DedupRef
    /// or a Delta source, violating canonical-presence and corrupting
    /// reads.
    pub fn lba_referenced_hashes(&self) -> HashSet<blake3::Hash> {
        let mut out: HashSet<blake3::Hash> = self.inner.values().map(|e| e.hash).collect();
        out.extend(self.delta_source_hashes.iter().copied());
        out
    }

    /// Record a delta source hash as referenced. Called for every
    /// `DeltaOption::source_hash` on every live Delta entry fed into
    /// the map at insert or rebuild time. See the `LbaMap` doc comment
    /// for the grow-only semantics.
    pub fn register_delta_source(&mut self, source_hash: blake3::Hash) {
        self.delta_source_hashes.insert(source_hash);
    }

    /// Return all (start_lba, lba_length) ranges whose hash equals `target`.
    ///
    /// Used for diagnostics only (linear scan).
    pub fn lbas_for_hash(&self, target: &blake3::Hash) -> Vec<(u64, u32)> {
        self.inner
            .iter()
            .filter(|(_, e)| &e.hash == target)
            .map(|(&lba, e)| (lba, e.lba_length))
            .collect()
    }

    /// Return all `(start_lba, lba_length, payload_block_offset)` runs
    /// whose hash equals `target`.
    ///
    /// Extent reclamation uses this for two checks:
    /// - **Containment**: every run must fall inside a given target range
    ///   before we can safely rewrite the hash (otherwise a rewrite in
    ///   isolation would strand out-of-range references on the bloated
    ///   body).
    /// - **Bloat detection**: any run with `payload_block_offset != 0`
    ///   is evidence that a prior write split the original payload, and
    ///   dead bytes exist inside the stored body.
    ///
    /// Linear scan over the full map.
    pub fn runs_for_hash(&self, target: &blake3::Hash) -> Vec<(u64, u32, u32)> {
        self.inner
            .iter()
            .filter(|(_, e)| &e.hash == target)
            .map(|(&lba, e)| (lba, e.lba_length, e.payload_block_offset))
            .collect()
    }

    /// Return the content hash mapped to `lba`, if any entry covers it.
    ///
    /// Used by GC to check whether a dedup-ref entry is still live: the ref
    /// should only be carried into the GC output if the LBA still maps to
    /// the ref's hash.
    pub fn hash_at(&self, lba: u64) -> Option<blake3::Hash> {
        if let Some((&start, entry)) = self.inner.range(..=lba).next_back()
            && lba < start + entry.lba_length as u64
        {
            return Some(entry.hash);
        }
        None
    }
}

impl Default for LbaMap {
    fn default() -> Self {
        Self::new()
    }
}

// --- rebuild from disk ---

/// Rebuild the LBA map from all committed segments across a fork ancestry chain.
///
/// `layers` is ordered oldest-first (root ancestor first, live fork last).
/// Each element is `(fork_dir, branch_ulid)`:
/// - `fork_dir`: the fork directory containing `pending/`, `index/`, and `cache/`.
/// - `branch_ulid`: if `Some`, only segments whose ULID string is ≤ this value
///   are included. `None` means include all segments (used for the live fork).
///
/// Applying layers oldest-to-newest means later layers shadow earlier ones for
/// any overlapping LBA range, which is the correct layer-merge semantics.
///
/// The caller (`Volume::open`) is responsible for replaying the in-progress
/// WAL on top of the result.
pub fn rebuild_segments(layers: &[(PathBuf, Option<String>)]) -> io::Result<LbaMap> {
    let mut map = LbaMap::new();

    for (fork_dir, branch_ulid) in layers {
        // index/*.idx: committed segments (evicted to two-file format; permanent LBA index).
        let mut cache_paths = segment::collect_idx_files(&fork_dir.join("index"))?;
        cache_paths.sort_unstable_by(|a, b| a.file_stem().cmp(&b.file_stem()));
        if let Some(cutoff) = branch_ulid {
            cache_paths.retain(|p| {
                p.file_stem()
                    .and_then(|n| n.to_str())
                    .map(|n| n <= cutoff.as_str())
                    .unwrap_or(false)
            });
        }

        // gc/*.applied: GC output bodies awaiting coordinator upload to S3.
        // These are produced BEFORE the WAL flush that follows gc_checkpoint
        // (u_repack < u_flush), so they are lower priority than index/*.idx
        // entries — a WAL flush promoted to index/ after GC ran must win.
        let mut gc_paths = segment::collect_gc_applied_segment_files(fork_dir)?;
        gc_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

        // pending/: in-flight WAL flushes (highest priority — most recent writes).
        let mut pending_paths = segment::collect_segment_files(&fork_dir.join("pending"))?;
        pending_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

        if let Some(cutoff) = branch_ulid {
            gc_paths.retain(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n <= cutoff.as_str())
                    .unwrap_or(false)
            });
            pending_paths.retain(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n <= cutoff.as_str())
                    .unwrap_or(false)
            });
        }

        if cache_paths.is_empty() && gc_paths.is_empty() && pending_paths.is_empty() {
            continue;
        }

        // Load the verifying key only when this layer has segments to check.
        let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE)?;

        // Process in priority order (last write wins):
        //   1. gc/*.applied  — lowest: GC-compacted data, ULID < the following WAL flush
        //   2. index/*.idx   — middle: committed segments promoted from pending/
        //   3. pending/      — highest: in-flight WAL flushes, always most recent
        //
        // This ordering ensures that a pending-then-promoted segment (index/*.idx)
        // with a higher ULID than the GC output correctly shadows the GC entry
        // for any overlapping LBA.
        for path in gc_paths
            .iter()
            .chain(cache_paths.iter())
            .chain(pending_paths.iter())
        {
            let (_bss, entries) = match segment::read_and_verify_segment_index(path, &vk) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    warn!(
                        "segment vanished during rebuild (GC race): {}",
                        path.display()
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };
            for entry in entries {
                map.insert(entry.start_lba, entry.lba_length, entry.hash);
                if entry.kind == segment::EntryKind::Delta {
                    for opt in &entry.delta_options {
                        map.register_delta_source(opt.source_hash);
                    }
                }
            }
        }
    }

    Ok(map)
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signing;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("elide-lbamap-test-{}-{}", std::process::id(), n));
        p
    }

    fn h(b: u8) -> blake3::Hash {
        blake3::hash(&[b; 32])
    }

    /// Write `volume.pub` into `dir` and return the signer.
    fn write_test_pub(dir: &std::path::Path) -> std::sync::Arc<dyn segment::SegmentSigner> {
        let (signer, vk) = signing::generate_ephemeral_signer();
        let pub_hex = signing::encode_hex(&vk.to_bytes()) + "\n";
        segment::write_file_atomic(&dir.join(signing::VOLUME_PUB_FILE), pub_hex.as_bytes())
            .unwrap();
        signer
    }

    // --- insert / lookup unit tests ---

    #[test]
    fn empty_lookup_returns_none() {
        let map = LbaMap::new();
        assert!(map.lookup(0).is_none());
        assert!(map.lookup(100).is_none());
    }

    #[test]
    fn insert_and_lookup_exact() {
        let mut map = LbaMap::new();
        map.insert(10, 5, h(1));
        // First block of extent — offset 0.
        assert_eq!(map.lookup(10), Some((h(1), 0)));
        // Middle block — offset 2.
        assert_eq!(map.lookup(12), Some((h(1), 2)));
        // Last block — offset 4.
        assert_eq!(map.lookup(14), Some((h(1), 4)));
    }

    #[test]
    fn lookup_miss_outside_extent() {
        let mut map = LbaMap::new();
        map.insert(10, 5, h(1)); // covers [10, 15)
        assert!(map.lookup(9).is_none());
        assert!(map.lookup(15).is_none());
        assert!(map.lookup(100).is_none());
    }

    #[test]
    fn lookup_miss_in_gap() {
        let mut map = LbaMap::new();
        map.insert(0, 5, h(1)); // [0, 5)
        map.insert(10, 5, h(2)); // [10, 15)
        assert!(map.lookup(5).is_none());
        assert!(map.lookup(7).is_none());
        assert!(map.lookup(9).is_none());
    }

    #[test]
    fn insert_overwrites_exact_range() {
        let mut map = LbaMap::new();
        map.insert(0, 10, h(1));
        map.insert(0, 10, h(2));
        assert_eq!(map.len(), 1);
        assert_eq!(map.lookup(0), Some((h(2), 0)));
        assert_eq!(map.lookup(9), Some((h(2), 9)));
    }

    #[test]
    fn insert_trims_predecessor_tail() {
        // [0, 20) → A; then insert [10, 30) → B.
        // Expected: [0, 10) → A, [10, 30) → B.
        let mut map = LbaMap::new();
        map.insert(0, 20, h(1));
        map.insert(10, 20, h(2));
        assert_eq!(map.len(), 2);
        assert_eq!(map.lookup(5), Some((h(1), 5)));
        assert_eq!(map.lookup(9), Some((h(1), 9)));
        assert_eq!(map.lookup(10), Some((h(2), 0)));
        assert_eq!(map.lookup(29), Some((h(2), 19)));
    }

    #[test]
    fn insert_splits_predecessor() {
        // [0, 100) → A; then insert [30, 20) → B (range [30, 50)).
        // Expected: [0, 30) → A, [30, 50) → B, [50, 100) → A.
        let mut map = LbaMap::new();
        map.insert(0, 100, h(1));
        map.insert(30, 20, h(2));
        assert_eq!(map.len(), 3);
        assert_eq!(map.lookup(0), Some((h(1), 0)));
        assert_eq!(map.lookup(29), Some((h(1), 29)));
        assert_eq!(map.lookup(30), Some((h(2), 0)));
        assert_eq!(map.lookup(49), Some((h(2), 19)));
        assert_eq!(map.lookup(50), Some((h(1), 50)));
        assert_eq!(map.lookup(99), Some((h(1), 99)));
    }

    #[test]
    fn insert_removes_fully_covered_entries() {
        // Three adjacent entries; overwrite the middle two.
        let mut map = LbaMap::new();
        map.insert(0, 10, h(1)); // [0, 10)
        map.insert(10, 10, h(2)); // [10, 20)
        map.insert(20, 10, h(3)); // [20, 30)
        map.insert(8, 15, h(4)); // [8, 23) — covers parts of all three
        // Expected: [0, 8) → A, [8, 23) → D, [23, 30) → C.
        assert_eq!(map.len(), 3);
        assert_eq!(map.lookup(7), Some((h(1), 7)));
        assert_eq!(map.lookup(8), Some((h(4), 0)));
        assert_eq!(map.lookup(22), Some((h(4), 14)));
        assert_eq!(map.lookup(23), Some((h(3), 3)));
        assert_eq!(map.lookup(29), Some((h(3), 9)));
    }

    #[test]
    fn insert_preserves_tail_of_last_covered_entry() {
        // [50, 100) → A; insert [30, 40) → B (range [30, 70)).
        // [50, 100) starts within [30, 70) but extends past 70.
        // Expected: [30, 70) → B, [70, 100) → A.
        // (Nothing before 30 to worry about.)
        let mut map = LbaMap::new();
        map.insert(50, 50, h(1)); // [50, 100)
        map.insert(30, 40, h(2)); // [30, 70)
        assert_eq!(map.len(), 2);
        assert_eq!(map.lookup(30), Some((h(2), 0)));
        assert_eq!(map.lookup(69), Some((h(2), 39)));
        assert_eq!(map.lookup(70), Some((h(1), 20)));
        assert_eq!(map.lookup(99), Some((h(1), 49)));
    }

    // --- rebuild integration test ---

    #[test]
    fn rebuild_from_segments_in_order() {
        use crate::segment::SegmentEntry;

        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();
        let signer = write_test_pub(&base);

        // Segment 1 (ULID "01A..."): covers [0, 10) → hash_1.
        {
            let mut entries = vec![SegmentEntry::new_data(
                h(1),
                0,
                10,
                segment::SegmentFlags::empty(),
                vec![0u8; 40960],
            )];
            segment::write_segment(
                &pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
                &mut entries,
                signer.as_ref(),
            )
            .unwrap();
        }

        // Segment 2 (ULID "01B..."): overwrites [5, 10) → hash_2.
        {
            let mut entries = vec![SegmentEntry::new_data(
                h(2),
                5,
                5,
                segment::SegmentFlags::empty(),
                vec![0u8; 20480],
            )];
            segment::write_segment(
                &pending.join("01BBBBBBBBBBBBBBBBBBBBBBBB"),
                &mut entries,
                signer.as_ref(),
            )
            .unwrap();
        }

        let map = rebuild_segments(&[(base.clone(), None)]).unwrap();

        // [0, 5) should be from segment 1.
        assert_eq!(map.lookup(0), Some((h(1), 0)));
        assert_eq!(map.lookup(4), Some((h(1), 4)));
        // [5, 10) should be from segment 2 (newer wins).
        assert_eq!(map.lookup(5), Some((h(2), 0)));
        assert_eq!(map.lookup(9), Some((h(2), 4)));

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rebuild_empty_dirs_returns_empty_map() {
        let base = temp_dir();
        // No subdirs at all — fresh volume.
        std::fs::create_dir_all(&base).unwrap();
        let map = rebuild_segments(&[(base.clone(), None)]).unwrap();
        assert!(map.is_empty());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rebuild_merges_ancestor_chain() {
        use crate::segment::SegmentEntry;

        let ancestor = temp_dir();
        let live = temp_dir();
        std::fs::create_dir_all(ancestor.join("pending")).unwrap();
        std::fs::create_dir_all(live.join("pending")).unwrap();
        let ancestor_signer = write_test_pub(&ancestor);
        let live_signer = write_test_pub(&live);

        // Ancestor: LBA 0..10 → h(1)
        {
            let mut entries = vec![SegmentEntry::new_data(
                h(1),
                0,
                10,
                segment::SegmentFlags::empty(),
                vec![0u8; 40960],
            )];
            segment::write_segment(
                &ancestor.join("pending").join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
                &mut entries,
                ancestor_signer.as_ref(),
            )
            .unwrap();
        }
        // Live node: LBA 5..10 → h(2) (shadows ancestor)
        {
            let mut entries = vec![SegmentEntry::new_data(
                h(2),
                5,
                5,
                segment::SegmentFlags::empty(),
                vec![0u8; 20480],
            )];
            segment::write_segment(
                &live.join("pending").join("01BBBBBBBBBBBBBBBBBBBBBBBB"),
                &mut entries,
                live_signer.as_ref(),
            )
            .unwrap();
        }

        let map = rebuild_segments(&[(ancestor.clone(), None), (live.clone(), None)]).unwrap();

        // Ancestor range not overwritten.
        assert_eq!(map.lookup(0), Some((h(1), 0)));
        assert_eq!(map.lookup(4), Some((h(1), 4)));
        // Live node shadows ancestor.
        assert_eq!(map.lookup(5), Some((h(2), 0)));
        assert_eq!(map.lookup(9), Some((h(2), 4)));

        std::fs::remove_dir_all(ancestor).unwrap();
        std::fs::remove_dir_all(live).unwrap();
    }

    #[test]
    fn rebuild_registers_delta_source_hashes() {
        // A segment with a Delta entry must cause its source_hash(es)
        // to appear in lba_referenced_hashes, even though the LBA map
        // itself only stores the Delta's content hash. This is the
        // load-bearing fold that keeps GC from collecting the source
        // DATA body out from under a live Delta.
        use crate::segment::{DeltaOption, SegmentEntry};

        let base = temp_dir();
        std::fs::create_dir_all(base.join("pending")).unwrap();
        let signer = write_test_pub(&base);

        let content_hash = h(7);
        let source_a = h(11);
        let source_b = h(13);
        let unrelated = h(99);

        let options = vec![
            DeltaOption {
                source_hash: source_a,
                delta_offset: 0,
                delta_length: 16,
            },
            DeltaOption {
                source_hash: source_b,
                delta_offset: 16,
                delta_length: 16,
            },
        ];

        let mut entries = vec![SegmentEntry::new_delta(content_hash, 0, 1, options)];
        segment::write_segment(
            &base.join("pending").join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
            &mut entries,
            signer.as_ref(),
        )
        .unwrap();

        let map = rebuild_segments(&[(base.clone(), None)]).unwrap();
        let referenced = map.lba_referenced_hashes();

        // Content hash reachable via the LBA map.
        assert!(
            referenced.contains(&content_hash),
            "delta content hash missing from lba_referenced_hashes"
        );
        // Both source hashes folded in via register_delta_source.
        assert!(
            referenced.contains(&source_a),
            "delta source A missing from lba_referenced_hashes"
        );
        assert!(
            referenced.contains(&source_b),
            "delta source B missing from lba_referenced_hashes"
        );
        // Unrelated hash not in the set.
        assert!(!referenced.contains(&unrelated));

        std::fs::remove_dir_all(base).unwrap();
    }

    // --- extents_in_range tests ---

    #[test]
    fn extents_in_range_empty_map() {
        let map = LbaMap::new();
        assert!(map.extents_in_range(0, 10).is_empty());
    }

    #[test]
    fn extents_in_range_single_extent_fully_inside() {
        let mut map = LbaMap::new();
        map.insert(5, 3, h(1)); // [5, 8)
        let result = map.extents_in_range(0, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].hash, h(1));
        assert_eq!(result[0].range_start, 5);
        assert_eq!(result[0].range_end, 8);
        assert_eq!(result[0].payload_block_offset, 0);
    }

    #[test]
    fn extents_in_range_predecessor_extends_into_range() {
        let mut map = LbaMap::new();
        map.insert(0, 10, h(1)); // [0, 10)
        // Request [5, 15) — predecessor starts before range but extends in.
        let result = map.extents_in_range(5, 15);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].range_start, 5);
        assert_eq!(result[0].range_end, 10);
        assert_eq!(result[0].payload_block_offset, 5); // 5 blocks into the payload
    }

    #[test]
    fn extents_in_range_multiple_extents() {
        let mut map = LbaMap::new();
        map.insert(0, 4, h(1)); // [0, 4)
        map.insert(4, 4, h(2)); // [4, 8)
        map.insert(8, 4, h(3)); // [8, 12)
        let result = map.extents_in_range(2, 10);
        assert_eq!(result.len(), 3);
        // First: predecessor [0,4) clipped to [2,4)
        assert_eq!(result[0].range_start, 2);
        assert_eq!(result[0].range_end, 4);
        assert_eq!(result[0].payload_block_offset, 2);
        // Second: [4,8) fully inside
        assert_eq!(result[1].range_start, 4);
        assert_eq!(result[1].range_end, 8);
        assert_eq!(result[1].payload_block_offset, 0);
        // Third: [8,12) clipped to [8,10)
        assert_eq!(result[2].range_start, 8);
        assert_eq!(result[2].range_end, 10);
        assert_eq!(result[2].payload_block_offset, 0);
    }

    #[test]
    fn extents_in_range_gap_between_extents() {
        let mut map = LbaMap::new();
        map.insert(0, 2, h(1)); // [0, 2)
        map.insert(5, 2, h(2)); // [5, 7) — gap at [2, 5)
        let result = map.extents_in_range(0, 7);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].range_start, 0);
        assert_eq!(result[0].range_end, 2);
        assert_eq!(result[1].range_start, 5);
        assert_eq!(result[1].range_end, 7);
    }

    #[test]
    fn extents_in_range_extent_ends_exactly_at_range_start() {
        let mut map = LbaMap::new();
        map.insert(0, 5, h(1)); // [0, 5) — ends exactly at range start
        map.insert(5, 5, h(2)); // [5, 10)
        let result = map.extents_in_range(5, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].hash, h(2));
    }

    #[test]
    fn extents_in_range_split_extent_payload_offsets() {
        // Insert [0, 10) then split it with [3, 4). Tail [4, 10) gets payload_block_offset = 4.
        // extents_in_range over [5, 8) should return the tail clipped, with
        // payload_block_offset = 4 + (5 - 4) = 5.
        let mut map = LbaMap::new();
        map.insert(0, 10, h(1));
        map.insert(3, 1, h(2)); // splits [0,10) into [0,3), [3,4), [4,10) with offset=4
        let result = map.extents_in_range(5, 8);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].hash, h(1));
        assert_eq!(result[0].range_start, 5);
        assert_eq!(result[0].range_end, 8);
        assert_eq!(result[0].payload_block_offset, 5); // 4 (tail offset) + 1 (5-4)
    }
}
