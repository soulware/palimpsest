// LBA map: in-memory structure mapping logical block addresses to content hashes.
//
// The map is a BTreeMap keyed by `start_lba`. Each entry holds
// `(lba_length, extent_hash)`. It is the authoritative source for read-path
// lookups and is updated after every promoted write.
//
// Rebuild on startup:
//   1. Scan pending/*.idx and segments/*.idx in ULID order (oldest first).
//      Applying oldest-to-newest means each insert naturally overwrites
//      earlier entries for the same LBA range — no special ordering logic needed.
//   2. Replay wal/* on top: WAL files are always the most recent writes.
//
// Contrast with lab47/lsvd: the reference uses a red-black tree (TreeMap) with
// a `compactPE` value encoding both logical and physical location. Palimpsest's
// map is purely logical (LBA → hash); physical location (hash → segment+offset)
// lives in the separate extent index. This means GC repacking never touches the
// LBA map.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use crate::segment;
use crate::writelog;

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
pub struct LbaMap {
    inner: BTreeMap<u64, MapEntry>,
}

impl LbaMap {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
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

    /// Look up the extent containing `lba`.
    ///
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
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for LbaMap {
    fn default() -> Self {
        Self::new()
    }
}

// --- rebuild from disk ---

/// Rebuild the LBA map from all committed segments and in-progress WAL files.
///
/// Scans `<base>/pending/*.idx` and `<base>/segments/*.idx` in ULID order
/// (oldest first), then replays `<base>/wal/*` on top. Directories that do not
/// exist are silently skipped — a fresh volume has none of these yet.
pub fn rebuild(base_dir: &Path) -> io::Result<LbaMap> {
    let mut map = LbaMap::new();

    // Committed segments: pending/ (local, not yet uploaded) and segments/ (S3-backed).
    let mut idx_paths = collect_idx_files(&base_dir.join("pending"))?;
    idx_paths.extend(collect_idx_files(&base_dir.join("segments"))?);
    // Sort by filename so inserts go in ULID (chronological) order.
    idx_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

    for path in &idx_paths {
        for entry in segment::read_idx(path)? {
            map.insert(entry.start_lba, entry.lba_length, entry.hash);
        }
    }

    // WAL files are always more recent than any segment; replay them last.
    let mut wal_paths = collect_dir_files(&base_dir.join("wal"))?;
    wal_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

    for path in &wal_paths {
        let (records, _) = writelog::scan(path)?;
        for record in records {
            match record {
                writelog::LogRecord::Data {
                    hash,
                    start_lba,
                    lba_length,
                    ..
                } => map.insert(start_lba, lba_length, hash),
                writelog::LogRecord::Ref {
                    hash,
                    start_lba,
                    lba_length,
                } => map.insert(start_lba, lba_length, hash),
            }
        }
    }

    Ok(map)
}

/// Return all `.idx` files in `dir`. `.idx.tmp` files are excluded because
/// `Path::extension()` returns `"tmp"` for them, not `"idx"`.
/// Returns an empty Vec if the directory does not exist.
fn collect_idx_files(dir: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    match fs::read_dir(dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
        Ok(entries) => {
            let mut paths = Vec::new();
            for entry in entries {
                let path = entry?.path();
                if path.extension().is_some_and(|e| e == "idx") {
                    paths.push(path);
                }
            }
            Ok(paths)
        }
    }
}

/// Return all regular files in `dir`. Returns an empty Vec if the directory
/// does not exist. Uses `DirEntry::file_type()` to avoid an extra stat per
/// entry on Linux (the file type is returned by `getdents64`).
fn collect_dir_files(dir: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    match fs::read_dir(dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
        Ok(entries) => {
            let mut paths = Vec::new();
            for entry in entries {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    paths.push(entry.path());
                }
            }
            Ok(paths)
        }
    }
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "palimpsest-lbamap-test-{}-{}",
            std::process::id(),
            n
        ));
        p
    }

    fn h(b: u8) -> blake3::Hash {
        blake3::hash(&[b; 32])
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
        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();

        // Segment 1 (ULID "01A..."): covers [0, 10) → hash_1.
        {
            use crate::segment::{IdxEntry, write_idx};
            let entries = vec![IdxEntry::from_wal_data(h(1), 0, 10, 0, 0, vec![0u8; 40960])];
            write_idx(&pending.join("01AAAAAAAAAAAAAAAAAAAAAAAAA.idx"), &entries).unwrap();
        }

        // Segment 2 (ULID "01B..."): overwrites [5, 10) → hash_2.
        {
            use crate::segment::{IdxEntry, write_idx};
            let entries = vec![IdxEntry::from_wal_data(h(2), 5, 5, 0, 0, vec![0u8; 20480])];
            write_idx(&pending.join("01BBBBBBBBBBBBBBBBBBBBBBBBB.idx"), &entries).unwrap();
        }

        let map = rebuild(&base).unwrap();

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
        let map = rebuild(&base).unwrap();
        assert!(map.is_empty());
        std::fs::remove_dir_all(base).unwrap();
    }
}
