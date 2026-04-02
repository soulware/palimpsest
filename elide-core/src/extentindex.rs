// Extent index: maps blake3::Hash → segment location.
//
// The extent index completes the read path:
//   lba → hash      (LBA map, src/lbamap.rs)
//   hash → location (this module)
//
// A location names the segment file and the absolute byte offset within it
// where the payload starts. At read time the file is located by checking each
// storage directory in order (wal/ → pending/ → segments/).
//
// Body offsets are always absolute file offsets:
//   - For in-progress entries (WAL not yet promoted): the absolute offset of
//     the data payload within the WAL file, as returned by WriteLog::append_data.
//   - For promoted entries (pending/ or segments/): body_section_start +
//     entry.stored_offset, where body_section_start comes from the segment header.
//
// Rebuild on startup:
//   extentindex::rebuild(base_dir) scans pending/ and segments/ for committed
//   segment files and reads their index sections. Volume::open() then inserts
//   WAL Data records on top via recover_wal().

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use log::warn;

use crate::segment;
use crate::signing;

/// Physical location of an extent within a segment file.
#[derive(Clone)]
pub struct ExtentLocation {
    /// ULID of the segment (filename in wal/, pending/, or segments/).
    pub segment_id: String,
    /// Byte offset of the start of the payload.
    ///
    /// For full segments (`pending/`, `segments/`, `wal/`): absolute file offset
    /// (`body_section_start + stored_offset`).
    /// For fetched entries (from `fetched/*.idx`): body-relative offset
    /// (`stored_offset`), matching byte 0 of the `.body` file.
    pub body_offset: u64,
    /// Byte length of the stored payload (compressed size if `compressed`).
    pub body_length: u32,
    /// True if the payload is lz4-compressed.
    pub compressed: bool,
    /// Position of this entry in the segment's raw index (0-based).
    /// `Some` for entries rebuilt from `fetched/*.idx`; `None` for full segments.
    /// Used to check and update the `.present` bitset for per-extent fetching.
    pub entry_idx: Option<u32>,
    /// Absolute offset of the body section within the segment file in the store.
    /// Equals `HEADER_LEN + index_length + inline_length` for this segment.
    /// `Some` for entries rebuilt from `fetched/*.idx`; `None` for full segments.
    /// Combined with `body_offset` to compute the store range-GET start:
    /// `body_section_start + body_offset`.
    pub body_section_start: Option<u64>,
}

/// In-memory index mapping content hash to segment location.
#[derive(Clone)]
pub struct ExtentIndex {
    inner: HashMap<blake3::Hash, ExtentLocation>,
}

impl ExtentIndex {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Insert or overwrite the location for `hash`.
    pub fn insert(&mut self, hash: blake3::Hash, location: ExtentLocation) {
        self.inner.insert(hash, location);
    }

    /// Look up the segment location for `hash`.
    pub fn lookup(&self, hash: &blake3::Hash) -> Option<&ExtentLocation> {
        self.inner.get(hash)
    }

    /// Remove the entry for `hash`, if present.
    pub fn remove(&mut self, hash: &blake3::Hash) {
        self.inner.remove(hash);
    }

    /// Number of entries in the index.
    #[allow(dead_code)] // used in tests; available for diagnostics
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for ExtentIndex {
    fn default() -> Self {
        Self::new()
    }
}

// --- rebuild from disk ---

/// Rebuild the extent index from all committed segments across an ancestor chain.
///
/// `node_chain` is ordered oldest-first (root ancestor first, live node last).
/// Each node's `pending/` and `segments/` are scanned in ULID order. Later
/// entries for the same hash overwrite earlier ones (same segment ULID is
/// globally unique, so this only matters across layers for moved extents after
/// GC repacking).
///
/// Inline entries and dedup-ref entries are skipped:
/// - Inline entries: read path not yet implemented (INLINE_THRESHOLD = 0).
/// - Dedup-ref entries: no body in this segment; the hash is already indexed
///   from the ancestor segment that holds the actual data.
///
/// Rebuild the extent index from all committed segments across a fork ancestry chain.
///
/// `layers` is ordered oldest-first (root ancestor first, live fork last).
/// Each element is `(fork_dir, branch_ulid)`:
/// - `fork_dir`: the fork directory containing `pending/` and `segments/`.
/// - `branch_ulid`: if `Some`, only segments whose ULID string is ≤ this value
///   are included. `None` means include all segments (used for the live fork).
///
/// Inline entries and dedup-ref entries are skipped:
/// - Inline entries: read path not yet implemented (INLINE_THRESHOLD = 0).
/// - Dedup-ref entries: no body in this segment; the hash is already indexed
///   from the ancestor segment that holds the actual data.
///
/// The caller (Volume::open) inserts in-progress WAL entries on top.
pub fn rebuild(layers: &[(PathBuf, Option<String>)]) -> io::Result<ExtentIndex> {
    let mut index = ExtentIndex::new();

    for (fork_dir, branch_ulid) in layers {
        // Process fetched/*.idx first (body-relative offsets). pending/ and
        // segments/ are processed after, so their absolute-offset entries win
        // when the same segment is present in both places.
        let mut fetched_paths = segment::collect_fetched_idx_files(&fork_dir.join("fetched"))?;
        fetched_paths.sort_unstable_by(|a, b| a.file_stem().cmp(&b.file_stem()));
        if let Some(cutoff) = branch_ulid {
            fetched_paths.retain(|p| {
                p.file_stem()
                    .and_then(|n| n.to_str())
                    .map(|n| n <= cutoff.as_str())
                    .unwrap_or(false)
            });
        }
        // Process pending/ and segments/ (absolute offsets). These overwrite
        // any fetched/ entries for the same hashes.
        let mut paths = segment::collect_segment_files(&fork_dir.join("pending"))?;
        paths.extend(segment::collect_segment_files(&fork_dir.join("segments"))?);
        segment::sort_for_rebuild(fork_dir, &mut paths);

        if let Some(cutoff) = branch_ulid {
            paths.retain(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n <= cutoff.as_str())
                    .unwrap_or(false)
            });
        }

        if fetched_paths.is_empty() && paths.is_empty() {
            continue;
        }

        // Load the verifying key only when this layer has segments to check.
        let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE)?;

        for path in &fetched_paths {
            let segment_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad fetched idx filename"))?;
            let segment_id = ulid::Ulid::from_string(segment_id)
                .map_err(|e| io::Error::other(e.to_string()))?
                .to_string();
            let (body_section_start, entries) =
                match segment::read_and_verify_segment_index(path, &vk) {
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
            for (raw_idx, entry) in entries.iter().enumerate() {
                if entry.is_dedup_ref || entry.is_inline {
                    continue;
                }
                // body_offset is body-relative: the .body file starts at byte 0
                // of the body section, so no adjustment needed.
                // entry_idx and body_section_start enable per-extent range-GETs.
                index.insert(
                    entry.hash,
                    ExtentLocation {
                        segment_id: segment_id.clone(),
                        body_offset: entry.stored_offset,
                        body_length: entry.stored_length,
                        compressed: entry.compressed,
                        entry_idx: Some(raw_idx as u32),
                        body_section_start: Some(body_section_start),
                    },
                );
            }
        }

        for path in &paths {
            let segment_id = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad segment filename"))?;
            // Validate as ULID and canonicalize.
            let segment_id = ulid::Ulid::from_string(segment_id)
                .map_err(|e| io::Error::other(e.to_string()))?
                .to_string();

            let (body_section_start, entries) =
                match segment::read_and_verify_segment_index(path, &vk) {
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
                if entry.is_dedup_ref || entry.is_inline {
                    continue;
                }
                index.insert(
                    entry.hash,
                    ExtentLocation {
                        segment_id: segment_id.clone(),
                        body_offset: body_section_start + entry.stored_offset,
                        body_length: entry.stored_length,
                        compressed: entry.compressed,
                        entry_idx: None,
                        body_section_start: None,
                    },
                );
            }
        }
    }

    Ok(index)
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::SegmentEntry;
    use crate::signing;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "elide-extentindex-test-{}-{}",
            std::process::id(),
            n
        ));
        p
    }

    /// Write `volume.pub` into `dir` using an ephemeral keypair.
    /// Returns the signer so the caller can sign segments with it.
    fn write_test_pub(dir: &std::path::Path) -> std::sync::Arc<dyn crate::segment::SegmentSigner> {
        let (signer, vk) = signing::generate_ephemeral_signer();
        crate::segment::write_file_atomic(&dir.join(signing::VOLUME_PUB_FILE), &vk.to_bytes())
            .unwrap();
        signer
    }

    fn h(b: u8) -> blake3::Hash {
        blake3::hash(&[b; 32])
    }

    #[test]
    fn empty_lookup_returns_none() {
        let index = ExtentIndex::new();
        assert!(index.lookup(&h(1)).is_none());
    }

    #[test]
    fn insert_and_lookup() {
        let mut index = ExtentIndex::new();
        let hash = h(1);
        index.insert(
            hash,
            ExtentLocation {
                segment_id: "01JQEXAMPLEULID0000000000A".to_string(),
                body_offset: 1024,
                body_length: 4096,
                compressed: false,
                entry_idx: None,
                body_section_start: None,
            },
        );
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.segment_id, "01JQEXAMPLEULID0000000000A");
        assert_eq!(loc.body_offset, 1024);
        assert_eq!(loc.body_length, 4096);
        assert!(!loc.compressed);
    }

    #[test]
    fn rebuild_from_pending() {
        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();
        let signer = write_test_pub(&base);

        let data = vec![0xabu8; 4096];
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(hash, 0, 1, 0, data)];
        let bss = segment::write_segment(
            &pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
            &mut entries,
            signer.as_ref(),
        )
        .unwrap();

        let index = rebuild(&[(base.clone(), None)]).unwrap();
        assert_eq!(index.len(), 1);
        let loc = index.lookup(&hash).unwrap();
        // body_offset should be absolute (body_section_start + 0).
        assert_eq!(loc.body_offset, bss + entries[0].stored_offset);
        assert_eq!(loc.body_length, 4096);

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rebuild_skips_dedup_ref() {
        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();
        let signer = write_test_pub(&base);

        let ref_hash = h(0xAA);
        let data_hash = blake3::hash(b"real data");
        let mut entries = vec![
            SegmentEntry::new_dedup_ref(ref_hash, 0, 1),
            SegmentEntry::new_data(
                data_hash,
                1,
                1,
                0,
                b"real data".repeat(512)[..4096].to_vec(),
            ),
        ];
        segment::write_segment(
            &pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
            &mut entries,
            signer.as_ref(),
        )
        .unwrap();

        let index = rebuild(&[(base.clone(), None)]).unwrap();
        // Only the DATA entry should be indexed; the dedup-ref is skipped.
        assert_eq!(index.len(), 1);
        assert!(index.lookup(&ref_hash).is_none());
        assert!(index.lookup(&data_hash).is_some());

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn newer_segment_overwrites_older_for_same_hash() {
        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();
        let signer = write_test_pub(&base);

        let data = vec![0u8; 4096];
        let hash = blake3::hash(&data);

        // Older segment.
        {
            let mut entries = vec![SegmentEntry::new_data(hash, 0, 1, 0, data.clone())];
            segment::write_segment(
                &pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
                &mut entries,
                signer.as_ref(),
            )
            .unwrap();
        }
        // Newer segment: same hash, different position.
        let bss2;
        let stored_offset2;
        {
            let data2 = vec![0u8; 8192]; // put something before it
            let hash2 = blake3::hash(&data2);
            let mut entries = vec![
                SegmentEntry::new_data(hash2, 10, 2, 0, data2),
                SegmentEntry::new_data(hash, 0, 1, 0, data),
            ];
            bss2 = segment::write_segment(
                &pending.join("01BBBBBBBBBBBBBBBBBBBBBBBB"),
                &mut entries,
                signer.as_ref(),
            )
            .unwrap();
            stored_offset2 = entries[1].stored_offset;
        }

        let index = rebuild(&[(base.clone(), None)]).unwrap();
        // Newer segment's offset wins.
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.body_offset, bss2 + stored_offset2);

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rebuild_empty_dirs_returns_empty() {
        let base = temp_dir();
        std::fs::create_dir_all(&base).unwrap();
        let index = rebuild(&[(base.clone(), None)]).unwrap();
        assert!(index.is_empty());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rebuild_indexes_ancestor_segments() {
        let ancestor = temp_dir();
        let live = temp_dir();
        std::fs::create_dir_all(ancestor.join("segments")).unwrap();
        std::fs::create_dir_all(live.join("pending")).unwrap();
        let signer = write_test_pub(&ancestor);

        let data = vec![0xabu8; 4096];
        let hash = blake3::hash(&data);

        // Hash lives in ancestor segments/.
        let bss;
        let stored_offset;
        {
            let mut entries = vec![SegmentEntry::new_data(hash, 0, 1, 0, data)];
            bss = segment::write_segment(
                &ancestor.join("segments").join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
                &mut entries,
                signer.as_ref(),
            )
            .unwrap();
            stored_offset = entries[0].stored_offset;
        }

        let index = rebuild(&[(ancestor.clone(), None), (live.clone(), None)]).unwrap();
        assert_eq!(index.len(), 1);
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.body_offset, bss + stored_offset);

        std::fs::remove_dir_all(ancestor).unwrap();
        std::fs::remove_dir_all(live).unwrap();
    }
}
