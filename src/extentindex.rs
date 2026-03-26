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
use std::path::Path;

use crate::segment;

/// Physical location of an extent within a segment file.
#[derive(Clone)]
pub struct ExtentLocation {
    /// ULID of the segment (filename in wal/, pending/, or segments/).
    pub segment_id: String,
    /// Absolute byte offset of the start of the payload in the file.
    pub body_offset: u64,
    /// Byte length of the payload (compressed size if the entry is compressed).
    #[allow(dead_code)] // used for bounds checking / compressed reads
    pub body_length: u32,
}

/// In-memory index mapping content hash to segment location.
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

/// Rebuild the extent index from all committed segments.
///
/// Scans `<base>/pending/` and `<base>/segments/` in ULID order (oldest
/// first). Reads the index section of each segment file; body_offset is
/// stored as an absolute file offset (`body_section_start + stored_offset`).
///
/// Inline entries and dedup-ref entries are skipped:
/// - Inline entries: read path not yet implemented (INLINE_THRESHOLD = 0).
/// - Dedup-ref entries: no body in this segment; the hash is already indexed
///   from the ancestor segment that holds the actual data.
///
/// The caller (Volume::open) inserts in-progress WAL entries on top.
pub fn rebuild(base_dir: &Path) -> io::Result<ExtentIndex> {
    let mut index = ExtentIndex::new();

    let mut paths = segment::collect_segment_files(&base_dir.join("pending"))?;
    paths.extend(segment::collect_segment_files(&base_dir.join("segments"))?);
    paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

    for path in &paths {
        let segment_id = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| io::Error::other("bad segment filename"))?;
        // Validate as ULID and canonicalize.
        let segment_id = ulid::Ulid::from_string(segment_id)
            .map_err(|e| io::Error::other(e.to_string()))?
            .to_string();

        let (body_section_start, entries) = segment::read_segment_index(path)?;

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
                },
            );
        }
    }

    Ok(index)
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::SegmentEntry;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "palimpsest-extentindex-test-{}-{}",
            std::process::id(),
            n
        ));
        p
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
            },
        );
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.segment_id, "01JQEXAMPLEULID0000000000A");
        assert_eq!(loc.body_offset, 1024);
        assert_eq!(loc.body_length, 4096);
    }

    #[test]
    fn rebuild_from_pending() {
        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();

        let data = vec![0xabu8; 4096];
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(hash, 0, 1, 0, data)];
        let bss = segment::write_segment(&pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"), &mut entries)
            .unwrap();

        let index = rebuild(&base).unwrap();
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
        segment::write_segment(&pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"), &mut entries).unwrap();

        let index = rebuild(&base).unwrap();
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

        let data = vec![0u8; 4096];
        let hash = blake3::hash(&data);

        // Older segment.
        {
            let mut entries = vec![SegmentEntry::new_data(hash, 0, 1, 0, data.clone())];
            segment::write_segment(&pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"), &mut entries)
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
            bss2 =
                segment::write_segment(&pending.join("01BBBBBBBBBBBBBBBBBBBBBBBB"), &mut entries)
                    .unwrap();
            stored_offset2 = entries[1].stored_offset;
        }

        let index = rebuild(&base).unwrap();
        // Newer segment's offset wins.
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.body_offset, bss2 + stored_offset2);

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rebuild_empty_dirs_returns_empty() {
        let base = temp_dir();
        std::fs::create_dir_all(&base).unwrap();
        let index = rebuild(&base).unwrap();
        assert!(index.is_empty());
        std::fs::remove_dir_all(base).unwrap();
    }
}
