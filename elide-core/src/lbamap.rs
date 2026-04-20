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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

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
/// Also tracks *delta source hashes* — BLAKE3 hashes referenced as
/// `source_hash` by any live Delta entry in the segments this map was
/// built from. These sources are not directly reachable via the normal
/// `MapEntry.hash` path (a Delta entry's content hash is what's in the
/// map; the source hash is separate), so they need their own book-keeping
/// to keep GC's canonical-presence rule honest: a source DATA entry must
/// stay alive as long as any Delta depends on it for decompression. See
/// `lba_referenced_hashes()` for the fold.
///
/// Source tracking is refcounted: `delta_sources_by_lba` records the
/// source list attached to each Delta LBA entry, and `delta_source_counts`
/// holds a per-hash refcount equal to the number of live LBA entries
/// whose source list contains that hash. When an LBA entry is trimmed,
/// split, or overwritten, the refcounts are updated in lockstep. A hash
/// with refcount zero is removed from the map, so `lba_referenced_hashes`
/// never reports stale sources.
#[derive(Clone)]
pub struct LbaMap {
    inner: BTreeMap<u64, MapEntry>,
    /// Source hashes attached to each live Delta LBA entry. A key here
    /// always corresponds to a key in `inner` whose origin was a Delta
    /// segment entry; splits of a Delta LBA range share the same `Arc`.
    delta_sources_by_lba: BTreeMap<u64, Arc<[blake3::Hash]>>,
    /// Refcounts for delta source hashes. Invariant: for every `h` in any
    /// `delta_sources_by_lba[k]`, `delta_source_counts[h]` exists and
    /// equals the number of keys in `delta_sources_by_lba` whose value
    /// contains `h`. Zero-count entries are removed eagerly.
    delta_source_counts: HashMap<blake3::Hash, u32>,
}

impl LbaMap {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            delta_sources_by_lba: BTreeMap::new(),
            delta_source_counts: HashMap::new(),
        }
    }

    fn incref(&mut self, h: blake3::Hash) {
        *self.delta_source_counts.entry(h).or_insert(0) += 1;
    }

    fn decref(&mut self, h: &blake3::Hash) {
        match self.delta_source_counts.get_mut(h) {
            Some(c) if *c == 1 => {
                self.delta_source_counts.remove(h);
            }
            Some(c) => *c -= 1,
            None => debug_assert!(false, "decref of untracked delta source"),
        }
    }

    /// Remove the entry at `key` from `inner` and decref any attached
    /// Delta sources. Returns the removed entry if one existed.
    fn remove_entry(&mut self, key: u64) -> Option<MapEntry> {
        let entry = self.inner.remove(&key)?;
        if let Some(srcs) = self.delta_sources_by_lba.remove(&key) {
            for h in srcs.iter() {
                self.decref(h);
            }
        }
        Some(entry)
    }

    /// Insert `(key, entry)` into `inner`, optionally attaching Delta
    /// sources. Increments refcounts for each source in the list.
    fn add_entry(&mut self, key: u64, entry: MapEntry, sources: Option<Arc<[blake3::Hash]>>) {
        self.inner.insert(key, entry);
        if let Some(srcs) = sources {
            for h in srcs.iter() {
                self.incref(*h);
            }
            self.delta_sources_by_lba.insert(key, srcs);
        }
    }

    /// Insert an extent `[start_lba, start_lba + lba_length)` → `hash`,
    /// trimming or splitting any existing entries it overlaps.
    ///
    /// Called after every successful [`crate::segment::promote`] and during
    /// startup rebuild. New entries always have `payload_block_offset = 0`;
    /// non-zero offsets arise only in the split/tail entries created internally.
    pub fn insert(&mut self, start_lba: u64, lba_length: u32, hash: blake3::Hash) {
        self.insert_inner(start_lba, lba_length, hash, None);
    }

    /// Insert a Delta extent. Same as [`insert`] but attaches `source_hashes`
    /// to the new LBA entry and refcounts each source. Splits propagate the
    /// source list (each surviving split contributes to the refcount).
    pub fn insert_delta(
        &mut self,
        start_lba: u64,
        lba_length: u32,
        hash: blake3::Hash,
        source_hashes: Arc<[blake3::Hash]>,
    ) {
        self.insert_inner(start_lba, lba_length, hash, Some(source_hashes));
    }

    fn insert_inner(
        &mut self,
        start_lba: u64,
        lba_length: u32,
        hash: blake3::Hash,
        sources: Option<Arc<[blake3::Hash]>>,
    ) {
        let new_end = start_lba + lba_length as u64;

        // Step 1: Handle a predecessor entry that starts before `start_lba`
        // but whose tail overlaps the new range.
        if let Some((&pred_start, &pred)) = self.inner.range(..start_lba).next_back() {
            let pred_end = pred_start + pred.lba_length as u64;
            if pred_end > start_lba {
                let pred_sources = self.delta_sources_by_lba.get(&pred_start).cloned();
                self.remove_entry(pred_start);
                // Prefix [pred_start, start_lba): same payload_block_offset.
                self.add_entry(
                    pred_start,
                    MapEntry {
                        lba_length: (start_lba - pred_start) as u32,
                        hash: pred.hash,
                        payload_block_offset: pred.payload_block_offset,
                    },
                    pred_sources.clone(),
                );
                // Suffix [new_end, pred_end): only present in the "hole punch"
                // case. payload_block_offset advances by (new_end - pred_start).
                if pred_end > new_end {
                    self.add_entry(
                        new_end,
                        MapEntry {
                            lba_length: (pred_end - new_end) as u32,
                            hash: pred.hash,
                            payload_block_offset: pred.payload_block_offset
                                + (new_end - pred_start) as u32,
                        },
                        pred_sources,
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
            let e_sources = self.delta_sources_by_lba.get(&key).cloned();
            // Key was found in range query above; remove cannot fail.
            let Some(e) = self.remove_entry(key) else {
                continue;
            };
            let entry_end = key + e.lba_length as u64;
            if entry_end > new_end {
                // Entry extends past the new range; preserve its tail.
                // payload_block_offset advances by (new_end - key).
                self.add_entry(
                    new_end,
                    MapEntry {
                        lba_length: (entry_end - new_end) as u32,
                        hash: e.hash,
                        payload_block_offset: e.payload_block_offset + (new_end - key) as u32,
                    },
                    e_sources,
                );
            }
        }

        self.add_entry(
            start_lba,
            MapEntry {
                lba_length,
                hash,
                payload_block_offset: 0,
            },
            sources,
        );
    }

    /// Attach (or replace) the Delta source list for the LBA entry starting
    /// at `start_lba`, but only if the entry's content hash matches
    /// `expected_hash`. Returns `true` if updated, `false` if the LBA has no
    /// entry or its hash no longer matches (i.e. a concurrent overwrite
    /// raced the caller).
    ///
    /// Used by post-flush Data→Delta conversions (`delta_repack`) where the
    /// segment file is rewritten with Delta entries but the LBA map's
    /// content hash is unchanged.
    pub fn set_delta_sources_if_matches(
        &mut self,
        start_lba: u64,
        expected_hash: blake3::Hash,
        source_hashes: Arc<[blake3::Hash]>,
    ) -> bool {
        let Some(entry) = self.inner.get(&start_lba) else {
            return false;
        };
        if entry.hash != expected_hash {
            return false;
        }
        if let Some(old) = self.delta_sources_by_lba.remove(&start_lba) {
            for h in old.iter() {
                self.decref(h);
            }
        }
        for h in source_hashes.iter() {
            self.incref(*h);
        }
        self.delta_sources_by_lba.insert(start_lba, source_hashes);
        true
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
        out.extend(self.delta_source_counts.keys().copied());
        out
    }

    /// Iterate every entry in the map as
    /// `(start_lba, lba_length, hash, payload_block_offset)`, sorted by
    /// `start_lba`. Used by the extent-reclamation candidate scanner to
    /// fold LBA map state into per-hash run lists in a single O(n) pass.
    pub fn iter_entries(&self) -> impl Iterator<Item = (u64, u32, blake3::Hash, u32)> + '_ {
        self.inner
            .iter()
            .map(|(&lba, e)| (lba, e.lba_length, e.hash, e.payload_block_offset))
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

    /// Refcount of `target` as a live Delta source hash. Returns 0 when the
    /// hash is not referenced by any live Delta entry. Used for diagnostics
    /// alongside [`lbas_for_hash`] to explain why a hash shows up in
    /// [`lba_referenced_hashes`].
    pub fn delta_source_refcount(&self, target: &blake3::Hash) -> u32 {
        self.delta_source_counts.get(target).copied().unwrap_or(0)
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
        // `discover_fork_segments` handles the race-safe listing order
        // (pending → gc → index) and returns segments in the correct
        // rebuild *processing* order: the committed tier (gc ∪ index) by
        // ULID ascending, then pending by ULID ascending. Last-write-wins
        // on overlapping LBAs gives the intended semantics: a bare
        // `gc/<U>` output (minted above all its inputs' ULIDs) shadows
        // any older non-input index segment at the same LBA, and pending
        // writes — always minted after any concurrent GC output — win over
        // both. See the helper's doc comment.
        let segments = segment::discover_fork_segments(fork_dir, branch_ulid.as_deref())?;

        if segments.is_empty() {
            continue;
        }

        // Load the verifying key only when this layer has segments to check.
        let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE)?;

        for sref in &segments {
            let (_bss, entries, _inputs) =
                match segment::read_and_verify_segment_index(&sref.path, &vk) {
                    Ok(v) => v,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        warn!(
                            "segment vanished during rebuild (GC race): {}",
                            sref.path.display()
                        );
                        continue;
                    }
                    Err(e) => return Err(e),
                };
            for entry in entries {
                // CanonicalData / CanonicalInline entries carry a body for
                // dedup resolution via the extent index but make no LBA claim
                // on rebuild. Skip them so their zeroed start_lba never
                // mutates the map.
                if entry.kind.is_canonical_only() {
                    continue;
                }
                if entry.kind == segment::EntryKind::Delta {
                    let sources: Arc<[blake3::Hash]> =
                        entry.delta_options.iter().map(|o| o.source_hash).collect();
                    map.insert_delta(entry.start_lba, entry.lba_length, entry.hash, sources);
                } else {
                    map.insert(entry.start_lba, entry.lba_length, entry.hash);
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
        // Both source hashes folded in via insert_delta's refcount.
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

    // --- delta source refcount tests ---

    #[test]
    fn delta_source_removed_when_lba_overwritten() {
        let mut map = LbaMap::new();
        let src = h(11);
        map.insert_delta(0, 4, h(1), Arc::from([src]));
        assert!(map.lba_referenced_hashes().contains(&src));

        // Overwrite the entire Delta range with a plain Data write.
        map.insert(0, 4, h(2));
        let referenced = map.lba_referenced_hashes();
        assert!(
            !referenced.contains(&src),
            "delta source should be removed after the Delta LBA is overwritten"
        );
    }

    #[test]
    fn delta_source_survives_split_and_drops_when_last_half_overwritten() {
        let mut map = LbaMap::new();
        let src = h(11);
        // Delta at [0, 10) with source src.
        map.insert_delta(0, 10, h(1), Arc::from([src]));
        // Hole-punch in the middle: splits into [0, 3) and [5, 10), both still Delta.
        map.insert(3, 2, h(2));
        assert!(
            map.lba_referenced_hashes().contains(&src),
            "source must stay live while any split of the Delta remains"
        );
        // Overwrite the first half.
        map.insert(0, 3, h(3));
        assert!(
            map.lba_referenced_hashes().contains(&src),
            "source must stay live while the other split remains"
        );
        // Overwrite the second half.
        map.insert(5, 5, h(4));
        assert!(
            !map.lba_referenced_hashes().contains(&src),
            "source must drop once all splits are gone"
        );
    }

    #[test]
    fn delta_source_refcount_tracks_multiple_deltas_per_source() {
        let mut map = LbaMap::new();
        let src = h(11);
        // Two independent Delta LBAs share the same source.
        map.insert_delta(0, 1, h(1), Arc::from([src]));
        map.insert_delta(100, 1, h(2), Arc::from([src]));

        assert!(map.lba_referenced_hashes().contains(&src));
        // Overwrite the first Delta — source should remain live (refcount 1).
        map.insert(0, 1, h(3));
        assert!(
            map.lba_referenced_hashes().contains(&src),
            "source alive while another Delta still references it"
        );
        // Overwrite the second — refcount hits zero.
        map.insert(100, 1, h(4));
        assert!(!map.lba_referenced_hashes().contains(&src));
    }

    #[test]
    fn set_delta_sources_if_matches_updates_and_replaces_refcounts() {
        let mut map = LbaMap::new();
        let content = h(1);
        let old_src = h(11);
        let new_src = h(13);

        // Start with a plain Data entry — no delta sources.
        map.insert(0, 1, content);
        assert!(!map.lba_referenced_hashes().contains(&old_src));

        // Attach an initial source.
        assert!(map.set_delta_sources_if_matches(0, content, Arc::from([old_src])));
        assert!(map.lba_referenced_hashes().contains(&old_src));

        // Replace with a new source list — old must go away, new must appear.
        assert!(map.set_delta_sources_if_matches(0, content, Arc::from([new_src])));
        let referenced = map.lba_referenced_hashes();
        assert!(!referenced.contains(&old_src));
        assert!(referenced.contains(&new_src));
    }

    #[test]
    fn set_delta_sources_if_matches_rejects_hash_mismatch() {
        let mut map = LbaMap::new();
        let content = h(1);
        let other = h(2);
        let src = h(11);

        map.insert(0, 1, content);
        // Concurrent overwrite changed the hash.
        map.insert(0, 1, other);

        assert!(
            !map.set_delta_sources_if_matches(0, content, Arc::from([src])),
            "must reject when LBA hash no longer matches"
        );
        assert!(!map.lba_referenced_hashes().contains(&src));
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
