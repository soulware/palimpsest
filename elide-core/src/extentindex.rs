// Extent index: maps blake3::Hash → segment location.
//
// The extent index completes the read path:
//   lba → hash      (LBA map, src/lbamap.rs)
//   hash → location (this module)
//
// A location names the segment file and the body-relative byte offset where
// the payload starts. At read time the file is located by checking each
// storage directory in order (wal/ → pending/ → cache/).
//
// Body offsets are always body-relative (= stored_offset from the segment index):
//   - For WAL entries (not yet promoted): body_section_start == 0 and
//     body_offset is the absolute WAL file offset (WAL has no header prefix).
//   - For promoted entries (pending/): body_offset == stored_offset;
//     body_section_start is the absolute file offset of the body section.
//     The actual file seek position is body_section_start + body_offset.
//   - For cached entries (.body files): body_section_start == 0 and
//     body_offset is the body-relative offset (file starts at body section byte 0).
//
// Rebuild on startup:
//   extentindex::rebuild scans pending/ for not-yet-uploaded segment files and
//   index/*.idx for uploaded segments. Volume::open() then inserts WAL Data
//   records on top via recover_wal().

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use log::warn;
use ulid::Ulid;

use crate::segment::{self, EntryKind};
use crate::signing;

/// How the extent's body is stored locally.
///
/// `Local` entries live in a complete file (WAL or pending segment) — the body
/// is always present and no demand-fetch is needed.
///
/// `Cached` entries live in a sparse `cache/<id>.body` file — individual
/// extents may or may not be present and can be demand-fetched via the
/// `.present` bitset and per-extent range-GETs.
#[derive(Clone, Copy, Debug)]
pub enum BodySource {
    /// Body is in a complete local file (WAL, pending, or gc segment).
    /// No present-bit check needed; demand-fetch is never triggered.
    Local,
    /// Body is in a sparse cache file. The `u32` is the 0-based entry index
    /// in the segment's index section, used to check/set the `.present` bitset
    /// and to compute the S3 byte range for per-extent fetching.
    Cached(u32),
}

/// Physical location of an extent within a segment file.
#[derive(Clone)]
pub struct ExtentLocation {
    /// ULID of the segment (filename in wal/, pending/, gc/, or index/).
    pub segment_id: Ulid,
    /// Body-relative byte offset of the start of the payload (= `stored_offset`
    /// from the segment index). For WAL entries, equals the absolute WAL file
    /// offset (WAL has no header, so body_section_start == 0 and the two coincide).
    pub body_offset: u64,
    /// Byte length of the stored payload (compressed size if `compressed`).
    pub body_length: u32,
    /// True if the payload is lz4-compressed.
    pub compressed: bool,
    /// How this extent's body is stored locally.
    pub body_source: BodySource,
    /// Absolute offset of the body section within the full segment file.
    /// 0 for WAL entries and `.body` cache files (both start at byte 0 of
    /// the body data). Non-zero for entries in `pending/` or `gc/*.applied` files.
    /// The actual seek position for a read is `body_section_start + body_offset`.
    /// Also used to compute the store range-GET start for per-extent fetching.
    pub body_section_start: u64,
    /// For inline extents: the raw payload bytes held in memory.
    /// Reads return this directly with zero file I/O.  `None` for non-inline
    /// entries (the normal case).
    pub inline_data: Option<Box<[u8]>>,
}

// Inherent impl on SegmentBodyLayout that needs ExtentLocation. Lives here
// (rather than in segment.rs) so `segment` stays free of a back-reference to
// `extentindex`.
impl segment::SegmentBodyLayout {
    /// Absolute file offset to seek to when reading the body bytes of
    /// the extent at `loc` from a file of this layout.
    ///
    /// For a `FullSegment` file this is `body_section_start + body_offset`;
    /// for a `BodyOnly` (`cache/<id>.body`) file the body section sits at
    /// byte 0, so it collapses to `body_offset` — even when the stored
    /// `loc.body_section_start` reflects the full segment's prefix.
    #[inline]
    pub fn body_seek(self, loc: &ExtentLocation) -> u64 {
        self.body_section_file_offset(loc.body_section_start) + loc.body_offset
    }
}

/// Where a Delta entry's delta blob lives locally.
///
/// Delta blobs are tiny — typically sub-KB post-zstd-dict — but they
/// live in different files depending on how the segment is staged:
///
/// - `Full`: the segment is a complete file (e.g. `pending/<id>`),
///   so the delta body section is inline at
///   `body_section_start + body_length`. The reader opens the
///   segment file and seeks there.
/// - `Cached`: the segment has been evicted to the cache three-file
///   shape, so the delta body section lives in a separate
///   `cache/<id>.delta` file that starts at delta-region byte 0.
///   On a pull host, `.delta` may not exist yet — the reader is
///   responsible for demand-fetching it.
#[derive(Clone, Copy, Debug)]
pub enum DeltaBodySource {
    /// Delta body lives inline in a full segment file.
    /// Seek base in that file = `body_section_start + body_length`.
    Full {
        body_section_start: u64,
        body_length: u64,
    },
    /// Delta body lives in a separate `cache/<id>.delta` file.
    /// Seek base = 0; reader demand-fetches if the file is absent.
    Cached,
}

/// Location of a thin Delta entry. Separate from `ExtentLocation`
/// because Delta entries are materialised lazily at read time by
/// fetching a delta blob from their owning segment's delta body
/// section and decompressing against a source extent's bytes.
#[derive(Clone, Debug)]
pub struct DeltaLocation {
    /// ULID of the segment that holds both the Delta index entry and
    /// its delta blob in the segment's delta body section.
    pub segment_id: Ulid,
    /// Where to find the delta blob bytes locally.
    pub body_source: DeltaBodySource,
    /// Delta options exactly as stored on disk. The reader scans them
    /// in order: already-cached sources preferred, then earliest ULID.
    pub options: Vec<segment::DeltaOption>,
}

/// In-memory index mapping content hash to segment location.
#[derive(Clone)]
pub struct ExtentIndex {
    inner: HashMap<blake3::Hash, ExtentLocation>,
    /// Thin Delta entries, keyed by the Delta's content hash (the
    /// hash of the bytes *after* decompression). Separate from
    /// `inner` so the hot-path DATA lookup stays untouched.
    deltas: HashMap<blake3::Hash, DeltaLocation>,
}

impl ExtentIndex {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
            deltas: HashMap::new(),
        }
    }

    /// Insert or overwrite the location for `hash`.
    pub fn insert(&mut self, hash: blake3::Hash, location: ExtentLocation) {
        self.inner.insert(hash, location);
    }

    /// Insert `location` only if `hash` is not already present.
    /// Returns `true` if the entry was inserted, `false` if it already existed.
    pub fn insert_if_absent(&mut self, hash: blake3::Hash, location: ExtentLocation) -> bool {
        use std::collections::hash_map::Entry;
        match self.inner.entry(hash) {
            Entry::Vacant(v) => {
                v.insert(location);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Look up the segment location for `hash`.
    pub fn lookup(&self, hash: &blake3::Hash) -> Option<&ExtentLocation> {
        self.inner.get(hash)
    }

    /// Register a Delta entry. Inserted only if the hash is not
    /// already present as either a DATA entry or another Delta — the
    /// lowest-ULID-wins canonicality rule takes precedence for DATA
    /// entries, and Delta entries are skipped when a direct DATA
    /// already exists.
    pub fn insert_delta_if_absent(&mut self, hash: blake3::Hash, location: DeltaLocation) -> bool {
        if self.inner.contains_key(&hash) {
            return false;
        }
        use std::collections::hash_map::Entry;
        match self.deltas.entry(hash) {
            Entry::Vacant(v) => {
                v.insert(location);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Look up a Delta entry by its content hash. Returns `None` if
    /// the hash is not registered as a Delta (either unknown, or
    /// present as a direct DATA entry instead).
    pub fn lookup_delta(&self, hash: &blake3::Hash) -> Option<&DeltaLocation> {
        self.deltas.get(hash)
    }

    /// Remove the entry for `hash`, if present.
    pub fn remove(&mut self, hash: &blake3::Hash) {
        self.inner.remove(hash);
        self.deltas.remove(hash);
    }

    /// Number of entries in the index.
    #[allow(dead_code)] // used in tests; available for diagnostics
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty() && self.deltas.is_empty()
    }

    /// Iterate `(hash, location)` pairs. Ordering is unspecified.
    pub fn iter(&self) -> impl Iterator<Item = (&blake3::Hash, &ExtentLocation)> {
        self.inner.iter()
    }
}

impl Default for ExtentIndex {
    fn default() -> Self {
        Self::new()
    }
}

// --- rebuild from disk ---

/// Rebuild the extent index from all committed segments across a fork ancestry chain.
///
/// `forks` is ordered oldest-first (root ancestor first, live fork last).
/// Each element is `(fork_dir, branch_ulid)`:
/// - `fork_dir`: the fork directory containing `pending/`, `index/`, and `cache/`.
/// - `branch_ulid`: if `Some`, only segments whose ULID string is ≤ this value
///   are included. `None` means include all segments (used for the live fork).
///
/// Dedup-ref and zero entries are skipped:
/// - Dedup-ref entries: no body in this segment; the hash is already indexed
///   from the ancestor segment that holds the actual data.
///
/// Inline entries are indexed with their data held in memory (`inline_data`).
/// The inline section bytes are read from the segment or `.idx` file and
/// sliced per entry, so reads can return inline data with zero file I/O.
///
/// The caller (Volume::open) inserts in-progress WAL entries on top.
/// Canonical location semantics: when the same hash appears in multiple
/// segments (e.g. a DATA entry and a later DedupRef from dedup),
/// the **lowest ULID wins** — segments are processed in ascending order
/// with first-write-wins insert (`insert_if_absent`), so the earliest
/// segment becomes canonical.  This is correct because a DedupRef always
/// refers to a segment with a lower ULID than itself, so the original
/// DATA entry (lowest ULID) is the natural canonical location.
///
/// `promote_segment`'s `should_update` check and `compact_candidates_inner`'s
/// Repack deduplication both agree on this direction.
pub fn rebuild(forks: &[(PathBuf, Option<String>)]) -> io::Result<ExtentIndex> {
    let mut index = ExtentIndex::new();

    for (fork_dir, branch_ulid) in forks {
        // Process pending/ and gc/*.applied first (absolute offsets, full
        // segment files still on disk).  index/*.idx is processed second
        // (body-relative offsets for cache/.body files).  Both loops use
        // insert_if_absent (first-write-wins = lowest ULID canonical).
        //
        // When the same segment appears in both pending/ and index/ (crash
        // recovery: coordinator wrote index/ but didn't delete pending/),
        // the pending/ entry goes in first and the index/ entry is skipped.
        // The read path checks pending/ before cache/, so the stored
        // absolute offsets match the file that will actually be opened.
        let mut paths = segment::collect_segment_files(&fork_dir.join("pending"))?;
        // Include GC handoff bodies in .applied state (volume-signed, in gc/
        // awaiting coordinator upload to S3).
        let bare_gc_paths = segment::collect_gc_applied_segment_files(fork_dir)?;
        paths.extend(bare_gc_paths.iter().cloned());
        segment::sort_for_rebuild(fork_dir, &mut paths);

        if let Some(cutoff) = branch_ulid {
            paths.retain(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n <= cutoff.as_str())
                    .unwrap_or(false)
            });
        }

        // Each bare GC body declares the input segments it superseded via the
        // segment header's `inputs` field. Their `index/<input>.idx` files are
        // stale: any entry the bare body deliberately omitted (a Removed entry)
        // would be re-introduced into the rebuilt extent index by
        // insert_if_absent, leaving a dangling reference once the coordinator
        // finishes apply_done_handoffs and deletes the input segments.
        // Skip those .idx files entirely. Found by HandoffProtocol.tla.
        let mut consumed_inputs: std::collections::HashSet<Ulid> = std::collections::HashSet::new();
        for bare_path in &bare_gc_paths {
            if let Ok((_, _, inputs)) = segment::read_segment_index(bare_path) {
                for input in inputs {
                    consumed_inputs.insert(input);
                }
            }
        }

        let mut cache_paths = segment::collect_idx_files(&fork_dir.join("index"))?;
        cache_paths.sort_unstable_by(|a, b| a.file_stem().cmp(&b.file_stem()));
        cache_paths.retain(|p| {
            let Some(stem) = p.file_stem().and_then(|s| s.to_str()) else {
                return true;
            };
            match Ulid::from_string(stem) {
                Ok(ulid) => !consumed_inputs.contains(&ulid),
                Err(_) => true,
            }
        });
        if let Some(cutoff) = branch_ulid {
            cache_paths.retain(|p| {
                p.file_stem()
                    .and_then(|n| n.to_str())
                    .map(|n| n <= cutoff.as_str())
                    .unwrap_or(false)
            });
        }

        if cache_paths.is_empty() && paths.is_empty() {
            continue;
        }

        // Load the verifying key only when this fork has segments to check.
        let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE)?;

        for path in &paths {
            let segment_id = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad segment filename"))?;
            // Validate as ULID.
            let segment_id =
                Ulid::from_string(segment_id).map_err(|e| io::Error::other(e.to_string()))?;

            let (body_section_start, entries, _inputs) =
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

            // Read inline section lazily: only when at least one Inline entry exists.
            let has_inline = entries.iter().any(|e| e.kind == EntryKind::Inline);
            let inline_bytes = if has_inline {
                segment::read_inline_section(path)?
            } else {
                Vec::new()
            };

            // Capture the segment's body_length so Delta entries can
            // record their delta body offset relative to the body
            // section. Only needed when at least one Delta entry is
            // present.
            let delta_body_length = if entries.iter().any(|e| e.kind == EntryKind::Delta) {
                Some(segment::read_segment_layout(path)?.body_length)
            } else {
                None
            };

            for entry in entries {
                match entry.kind {
                    EntryKind::Data | EntryKind::Inline => {}
                    EntryKind::DedupRef | EntryKind::Zero => continue,
                    EntryKind::Delta => {
                        if let Some(body_length) = delta_body_length {
                            index.insert_delta_if_absent(
                                entry.hash,
                                DeltaLocation {
                                    segment_id,
                                    body_source: DeltaBodySource::Full {
                                        body_section_start,
                                        body_length,
                                    },
                                    options: entry.delta_options.clone(),
                                },
                            );
                        }
                        continue;
                    }
                }
                let idata = if entry.kind == EntryKind::Inline {
                    let start = entry.stored_offset as usize;
                    let end = start + entry.stored_length as usize;
                    if end <= inline_bytes.len() {
                        Some(inline_bytes[start..end].into())
                    } else {
                        // Truncated inline section — skip this entry.
                        continue;
                    }
                } else {
                    None
                };
                index.insert_if_absent(
                    entry.hash,
                    ExtentLocation {
                        segment_id,
                        body_offset: entry.stored_offset,
                        body_length: entry.stored_length,
                        compressed: entry.compressed,
                        body_source: BodySource::Local,
                        body_section_start,
                        inline_data: idata,
                    },
                );
            }
        }

        for path in &cache_paths {
            let segment_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad cache idx filename"))?;
            let segment_id =
                Ulid::from_string(segment_id).map_err(|e| io::Error::other(e.to_string()))?;

            // .idx file size == body_section_start (the file is exactly the
            // [0, body_section_start) prefix of the full segment).
            let body_section_start = segment::idx_body_section_start(path)?;

            let layout = match segment::read_segment_layout(path) {
                Ok(l) => l,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    warn!(
                        "segment vanished during rebuild (GC race): {}",
                        path.display()
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };
            let body_length = layout.body_length;
            let delta_length = layout.delta_length;

            let entries = match segment::read_and_verify_segment_index(path, &vk) {
                Ok((_, entries, _)) => entries,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    warn!(
                        "segment vanished during rebuild (GC race): {}",
                        path.display()
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };
            // Read inline section from .idx (which includes header + index + inline).
            let has_inline = entries.iter().any(|e| e.kind == EntryKind::Inline);
            let inline_bytes = if has_inline {
                segment::read_inline_section(path)?
            } else {
                Vec::new()
            };

            // For cached segments, Delta entries register unconditionally:
            // the delta body lives in a separate `cache/<id>.delta` file
            // which may or may not be present locally. The reader
            // (`try_read_delta_extent`) stats `.delta` before reading and
            // demand-fetches via the attached `SegmentFetcher` when it's
            // missing. Rebuild has no file-size check for delta presence.
            let _ = (body_length, delta_length); // retained for future diagnostics

            for (raw_idx, entry) in entries.iter().enumerate() {
                match entry.kind {
                    EntryKind::Data | EntryKind::Inline => {}
                    EntryKind::DedupRef | EntryKind::Zero => continue,
                    EntryKind::Delta => {
                        index.insert_delta_if_absent(
                            entry.hash,
                            DeltaLocation {
                                segment_id,
                                body_source: DeltaBodySource::Cached,
                                options: entry.delta_options.clone(),
                            },
                        );
                        continue;
                    }
                }
                let idata = if entry.kind == EntryKind::Inline {
                    let start = entry.stored_offset as usize;
                    let end = start + entry.stored_length as usize;
                    if end <= inline_bytes.len() {
                        Some(inline_bytes[start..end].into())
                    } else {
                        continue;
                    }
                } else {
                    None
                };
                // body_offset is body-relative: the .body file starts at byte 0
                // of the body section, so no adjustment needed.
                // body_source and body_section_start enable per-extent range-GETs.
                index.insert_if_absent(
                    entry.hash,
                    ExtentLocation {
                        segment_id,
                        body_offset: entry.stored_offset,
                        body_length: entry.stored_length,
                        compressed: entry.compressed,
                        body_source: BodySource::Cached(raw_idx as u32),
                        body_section_start,
                        inline_data: idata,
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
        let pub_hex = signing::encode_hex(&vk.to_bytes()) + "\n";
        crate::segment::write_file_atomic(&dir.join(signing::VOLUME_PUB_FILE), pub_hex.as_bytes())
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
                segment_id: Ulid::from_string("01JQTEST000000000000000001").unwrap(),
                body_offset: 1024,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 0,
                inline_data: None,
            },
        );
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(
            loc.segment_id,
            Ulid::from_string("01JQTEST000000000000000001").unwrap()
        );
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
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            segment::SegmentFlags::empty(),
            data,
        )];
        let bss = segment::write_segment(
            &pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
            &mut entries,
            signer.as_ref(),
        )
        .unwrap();

        let index = rebuild(&[(base.clone(), None)]).unwrap();
        assert_eq!(index.len(), 1);
        let loc = index.lookup(&hash).unwrap();
        // body_offset is body-relative (= stored_offset); body_section_start carries bss.
        assert_eq!(loc.body_offset, entries[0].stored_offset);
        assert_eq!(loc.body_section_start, bss);
        assert_eq!(loc.body_length, 4096);

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rebuild_indexes_data_skips_dedup_ref() {
        // DedupRef entries carry no body bytes and must NOT be indexed — the
        // extent index for that hash points to the canonical Data segment.
        // Only Data entries are indexed.
        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();
        let signer = write_test_pub(&base);

        let ref_hash = blake3::hash(b"dedup ref body");
        let ref_entry = SegmentEntry::new_dedup_ref(ref_hash, 0, 1);

        let data_body = b"real data".repeat(512)[..4096].to_vec();
        let data_hash = blake3::hash(&data_body);
        let mut entries = vec![
            ref_entry,
            SegmentEntry::new_data(data_hash, 2, 1, segment::SegmentFlags::empty(), data_body),
        ];
        segment::write_segment(
            &pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
            &mut entries,
            signer.as_ref(),
        )
        .unwrap();

        let index = rebuild(&[(base.clone(), None)]).unwrap();
        // Only Data indexed; DedupRef skipped.
        assert_eq!(index.len(), 1);
        assert!(index.lookup(&ref_hash).is_none());
        assert!(index.lookup(&data_hash).is_some());

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn oldest_segment_wins_for_same_hash() {
        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();
        let signer = write_test_pub(&base);

        let data = vec![0u8; 4096];
        let hash = blake3::hash(&data);

        // Older segment.
        let bss1;
        let stored_offset1;
        {
            let mut entries = vec![SegmentEntry::new_data(
                hash,
                0,
                1,
                segment::SegmentFlags::empty(),
                data.clone(),
            )];
            bss1 = segment::write_segment(
                &pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
                &mut entries,
                signer.as_ref(),
            )
            .unwrap();
            stored_offset1 = entries[0].stored_offset;
        }
        // Newer segment: same hash, different position.
        {
            let data2 = vec![0u8; 8192]; // put something before it
            let hash2 = blake3::hash(&data2);
            let mut entries = vec![
                SegmentEntry::new_data(hash2, 10, 2, segment::SegmentFlags::empty(), data2),
                SegmentEntry::new_data(hash, 0, 1, segment::SegmentFlags::empty(), data),
            ];
            segment::write_segment(
                &pending.join("01BBBBBBBBBBBBBBBBBBBBBBBB"),
                &mut entries,
                signer.as_ref(),
            )
            .unwrap();
        }

        let index = rebuild(&[(base.clone(), None)]).unwrap();
        // Oldest segment (lowest ULID) wins; body_offset is body-relative.
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.body_offset, stored_offset1);
        assert_eq!(loc.body_section_start, bss1);

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
        std::fs::create_dir_all(ancestor.join("pending")).unwrap();
        std::fs::create_dir_all(live.join("pending")).unwrap();
        let signer = write_test_pub(&ancestor);

        let data = vec![0xabu8; 4096];
        let hash = blake3::hash(&data);

        // Hash lives in ancestor pending/ (simulating an uploaded segment still cached locally).
        let bss;
        let stored_offset;
        {
            let mut entries = vec![SegmentEntry::new_data(
                hash,
                0,
                1,
                segment::SegmentFlags::empty(),
                data,
            )];
            bss = segment::write_segment(
                &ancestor.join("pending").join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
                &mut entries,
                signer.as_ref(),
            )
            .unwrap();
            stored_offset = entries[0].stored_offset;
        }

        let index = rebuild(&[(ancestor.clone(), None), (live.clone(), None)]).unwrap();
        assert_eq!(index.len(), 1);
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.body_offset, stored_offset);
        assert_eq!(loc.body_section_start, bss);

        std::fs::remove_dir_all(ancestor).unwrap();
        std::fs::remove_dir_all(live).unwrap();
    }

    #[test]
    fn rebuild_indexes_inline_entries_from_pending() {
        // Inline entries should be indexed with their data in inline_data.
        let base = temp_dir();
        let pending = base.join("pending");
        std::fs::create_dir_all(&pending).unwrap();
        let signer = write_test_pub(&base);

        let data = vec![0xABu8; 100]; // well below INLINE_THRESHOLD
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            segment::SegmentFlags::empty(),
            data.clone(),
        )];
        assert_eq!(entries[0].kind, segment::EntryKind::Inline);

        segment::write_segment(
            &pending.join("01AAAAAAAAAAAAAAAAAAAAAAAA"),
            &mut entries,
            signer.as_ref(),
        )
        .unwrap();

        let index = rebuild(&[(base.clone(), None)]).unwrap();
        assert_eq!(index.len(), 1);

        let loc = index.lookup(&hash).unwrap();
        assert!(
            loc.inline_data.is_some(),
            "inline entry must have inline_data populated"
        );
        assert_eq!(loc.inline_data.as_deref().unwrap(), &data);

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rebuild_indexes_inline_entries_from_idx() {
        // .idx files include the inline section; rebuild must load inline_data.
        let base = temp_dir();
        let pending = base.join("pending");
        let index_dir = base.join("index");
        std::fs::create_dir_all(&pending).unwrap();
        std::fs::create_dir_all(&index_dir).unwrap();
        let signer = write_test_pub(&base);

        let data = vec![0xCDu8; 200];
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            segment::SegmentFlags::empty(),
            data.clone(),
        )];

        let seg_name = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let seg_path = pending.join(seg_name);
        segment::write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();

        // Extract .idx and remove the full segment so rebuild uses the .idx.
        let idx_path = index_dir.join(format!("{seg_name}.idx"));
        segment::extract_idx(&seg_path, &idx_path).unwrap();
        std::fs::remove_file(&seg_path).unwrap();

        let index = rebuild(&[(base.clone(), None)]).unwrap();
        assert_eq!(index.len(), 1);

        let loc = index.lookup(&hash).unwrap();
        assert!(
            loc.inline_data.is_some(),
            "inline entry rebuilt from .idx must have inline_data"
        );
        assert_eq!(loc.inline_data.as_deref().unwrap(), &data);
        // Should be Cached source since it came from an .idx file.
        assert!(matches!(loc.body_source, BodySource::Cached(_)));

        std::fs::remove_dir_all(base).unwrap();
    }
}
