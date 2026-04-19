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

    /// Compare-and-replace: overwrite the entry for `hash` only if the current
    /// location matches `(expected_segment_id, expected_body_offset)`. Returns
    /// `true` if the replacement happened, `false` if the precondition failed
    /// (entry missing, or superseded by a concurrent writer / GC handoff).
    ///
    /// Used by the WAL promote apply phase to avoid clobbering an entry that
    /// a later writer has already pointed at its own segment. The precondition
    /// token is the `(wal_ulid, wal_offset)` snapshot taken before the promote
    /// started, so any mutation after the snapshot causes the CAS to fail.
    pub fn replace_if_matches(
        &mut self,
        hash: blake3::Hash,
        expected_segment_id: Ulid,
        expected_body_offset: u64,
        new_location: ExtentLocation,
    ) -> bool {
        match self.inner.get(&hash) {
            Some(loc)
                if loc.segment_id == expected_segment_id
                    && loc.body_offset == expected_body_offset =>
            {
                self.inner.insert(hash, new_location);
                true
            }
            _ => false,
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

    /// Compare-and-remove: drop the entry for `hash` only if the current
    /// location matches `(expected_segment_id, expected_body_offset)`.
    /// Returns `true` if the removal happened, `false` if the precondition
    /// failed (entry missing, or now points elsewhere).
    ///
    /// Sibling of [`Self::replace_if_matches`] for the case where an offload
    /// apply phase has identified an entry as dead and wants to drop it
    /// without clobbering a concurrent writer that has since re-pointed the
    /// hash at a newer segment.
    pub fn remove_if_matches(
        &mut self,
        hash: &blake3::Hash,
        expected_segment_id: Ulid,
        expected_body_offset: u64,
    ) -> bool {
        match self.inner.get(hash) {
            Some(loc)
                if loc.segment_id == expected_segment_id
                    && loc.body_offset == expected_body_offset =>
            {
                self.inner.remove(hash);
                true
            }
            _ => false,
        }
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
        // Discover all segments in race-safe listing order (pending → gc →
        // index) and rebuild-processing order (gc → index → pending). Both
        // `lbamap::rebuild_segments` and this function share the helper.
        //
        // Insert semantics are per-tier: `insert_if_absent` is used
        // throughout (first-write-wins = lowest ULID canonical). Because
        // we iterate tiers in gc → index → pending order and each tier in
        // ULID order, the first insert for any hash comes from the
        // lowest-ULID segment holding it — matching the documented rule.
        let segments = segment::discover_fork_segments(fork_dir, branch_ulid.as_deref())?;

        // Each bare GC body declares the input segments it superseded via
        // its `inputs` field. Those inputs' `index/<input>.idx` files are
        // stale: any entry the bare body deliberately omitted (a Removed
        // entry) would be re-introduced here by `insert_if_absent`, leaving
        // a dangling reference once the coordinator finishes
        // `apply_done_handoffs` and deletes the input segments.
        // Skip those index entries entirely. Found by HandoffProtocol.tla.
        let mut consumed_inputs: std::collections::HashSet<Ulid> = std::collections::HashSet::new();
        for sref in &segments {
            if sref.tier == segment::SegmentTier::GcApplied
                && let Ok((_, _, inputs)) = segment::read_segment_index(&sref.path)
            {
                for input in inputs {
                    consumed_inputs.insert(input);
                }
            }
        }

        // Filter out the stale Index entries consumed by bare GC outputs.
        let segments: Vec<segment::SegmentRef> = segments
            .into_iter()
            .filter(|s| {
                !(s.tier == segment::SegmentTier::Index && consumed_inputs.contains(&s.ulid))
            })
            .collect();

        if segments.is_empty() {
            continue;
        }

        // Load the verifying key only when this fork has segments to check.
        let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE)?;

        for sref in &segments {
            let segment_id = sref.ulid;
            let path = &sref.path;

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

            // Read inline section lazily: only when at least one inline-kind
            // entry exists (Inline or CanonicalInline).
            let has_inline = entries.iter().any(|e| e.kind.is_inline());
            let inline_bytes = if has_inline {
                segment::read_inline_section(path)?
            } else {
                Vec::new()
            };

            // Per-tier body wiring.
            //
            // Pending/GcApplied: the full segment file is still on disk at
            // its pending/ or gc/ path; body offsets are absolute within
            // that file, and Delta body resolution uses the header's
            // `body_length` + `body_section_start` to locate the delta
            // body section inside the same file.
            //
            // Index: the segment lives in the two-file cache format
            // (`cache/<ulid>.body` + `<ulid>.idx`). Body offsets are
            // body-section-relative (the `.body` file starts at byte 0 of
            // the body section); Delta body resolution uses
            // `DeltaBodySource::Cached`, which checks for a separate
            // `cache/<ulid>.delta` file and demand-fetches if missing.
            let (body_src_builder, delta_body_source): (
                Box<dyn Fn(u32) -> BodySource>,
                DeltaBodySource,
            ) = match sref.tier {
                segment::SegmentTier::Pending | segment::SegmentTier::GcApplied => {
                    // Capture body_length for DeltaBodySource::Full; only
                    // needed if Delta entries are present.
                    let body_length = if entries.iter().any(|e| e.kind == EntryKind::Delta) {
                        segment::read_segment_layout(path)?.body_length
                    } else {
                        0
                    };
                    (
                        Box::new(|_idx: u32| BodySource::Local),
                        DeltaBodySource::Full {
                            body_section_start,
                            body_length,
                        },
                    )
                }
                segment::SegmentTier::Index => (
                    Box::new(|idx: u32| BodySource::Cached(idx)),
                    DeltaBodySource::Cached,
                ),
            };

            for (raw_idx, entry) in entries.iter().enumerate() {
                match entry.kind {
                    EntryKind::Data
                    | EntryKind::Inline
                    | EntryKind::CanonicalData
                    | EntryKind::CanonicalInline => {}
                    EntryKind::DedupRef | EntryKind::Zero => continue,
                    EntryKind::Delta => {
                        index.insert_delta_if_absent(
                            entry.hash,
                            DeltaLocation {
                                segment_id,
                                body_source: delta_body_source,
                                options: entry.delta_options.clone(),
                            },
                        );
                        continue;
                    }
                }
                let idata = if entry.kind.is_inline() {
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
                        body_source: body_src_builder(raw_idx as u32),
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
    fn replace_if_matches_rewrites_on_exact_token() {
        // Classic use: the WAL promote apply phase snapshots (wal_ulid,
        // wal_offset) before the segment is written, then conditionally
        // rewrites the extent index to segment-relative coordinates. If
        // nothing superseded the entry, the CAS succeeds.
        let mut index = ExtentIndex::new();
        let hash = h(1);
        let wal_ulid = Ulid::from_string("01JQTEST000000000000000100").unwrap();
        let wal_offset = 4096u64;
        index.insert(
            hash,
            ExtentLocation {
                segment_id: wal_ulid,
                body_offset: wal_offset,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 0,
                inline_data: None,
            },
        );

        let seg_ulid = Ulid::from_string("01JQTEST000000000000000200").unwrap();
        let replaced = index.replace_if_matches(
            hash,
            wal_ulid,
            wal_offset,
            ExtentLocation {
                segment_id: seg_ulid,
                body_offset: 200,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 512,
                inline_data: None,
            },
        );
        assert!(replaced);
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.segment_id, seg_ulid);
        assert_eq!(loc.body_offset, 200);
        assert_eq!(loc.body_section_start, 512);
    }

    #[test]
    fn replace_if_matches_leaves_superseded_entry_alone() {
        // The hash was written to the WAL, then a later writer (or a GC
        // handoff) pointed the extent index at a different segment before
        // the worker completed. The apply phase must not clobber that.
        let mut index = ExtentIndex::new();
        let hash = h(2);
        let wal_ulid = Ulid::from_string("01JQTEST000000000000000100").unwrap();
        let wal_offset = 4096u64;

        // Superseding entry already in place.
        let superseder = Ulid::from_string("01JQTEST000000000000000300").unwrap();
        index.insert(
            hash,
            ExtentLocation {
                segment_id: superseder,
                body_offset: 9999,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 64,
                inline_data: None,
            },
        );

        let seg_ulid = Ulid::from_string("01JQTEST000000000000000200").unwrap();
        let replaced = index.replace_if_matches(
            hash,
            wal_ulid,
            wal_offset,
            ExtentLocation {
                segment_id: seg_ulid,
                body_offset: 200,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 512,
                inline_data: None,
            },
        );
        assert!(!replaced);
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.segment_id, superseder);
        assert_eq!(loc.body_offset, 9999);
    }

    #[test]
    fn replace_if_matches_rejects_offset_mismatch() {
        // Same ULID but a different body_offset — e.g. the entry was
        // reinserted by a later code path at a different position in the
        // same segment. CAS must fail.
        let mut index = ExtentIndex::new();
        let hash = h(3);
        let wal_ulid = Ulid::from_string("01JQTEST000000000000000100").unwrap();
        index.insert(
            hash,
            ExtentLocation {
                segment_id: wal_ulid,
                body_offset: 8192,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 0,
                inline_data: None,
            },
        );

        let replaced = index.replace_if_matches(
            hash,
            wal_ulid,
            4096,
            ExtentLocation {
                segment_id: wal_ulid,
                body_offset: 0,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 64,
                inline_data: None,
            },
        );
        assert!(!replaced);
        assert_eq!(index.lookup(&hash).unwrap().body_offset, 8192);
    }

    #[test]
    fn replace_if_matches_rejects_missing_entry() {
        let mut index = ExtentIndex::new();
        let hash = h(4);
        let wal_ulid = Ulid::from_string("01JQTEST000000000000000100").unwrap();
        let replaced = index.replace_if_matches(
            hash,
            wal_ulid,
            0,
            ExtentLocation {
                segment_id: wal_ulid,
                body_offset: 0,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 0,
                inline_data: None,
            },
        );
        assert!(!replaced);
        assert!(index.lookup(&hash).is_none());
    }

    #[test]
    fn remove_if_matches_drops_on_exact_token() {
        // Sweep apply phase identified the entry as dead at prep time,
        // captured (segment_id, body_offset) as the precondition, and
        // nothing has touched it since — the CAS-style remove succeeds.
        let mut index = ExtentIndex::new();
        let hash = h(5);
        let seg_ulid = Ulid::from_string("01JQTEST000000000000000400").unwrap();
        let body_offset = 4096u64;
        index.insert(
            hash,
            ExtentLocation {
                segment_id: seg_ulid,
                body_offset,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 0,
                inline_data: None,
            },
        );

        let removed = index.remove_if_matches(&hash, seg_ulid, body_offset);
        assert!(removed);
        assert!(index.lookup(&hash).is_none());
    }

    #[test]
    fn remove_if_matches_leaves_superseded_entry_alone() {
        // Between sweep prep and apply, a concurrent writer re-pointed
        // the hash at a newer segment. The remove precondition no longer
        // holds, so the entry must survive untouched.
        let mut index = ExtentIndex::new();
        let hash = h(6);
        let dead_ulid = Ulid::from_string("01JQTEST000000000000000400").unwrap();
        let dead_offset = 4096u64;
        let live_ulid = Ulid::from_string("01JQTEST000000000000000500").unwrap();
        index.insert(
            hash,
            ExtentLocation {
                segment_id: live_ulid,
                body_offset: 8192,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 64,
                inline_data: None,
            },
        );

        let removed = index.remove_if_matches(&hash, dead_ulid, dead_offset);
        assert!(!removed);
        let loc = index.lookup(&hash).unwrap();
        assert_eq!(loc.segment_id, live_ulid);
        assert_eq!(loc.body_offset, 8192);
    }

    #[test]
    fn remove_if_matches_rejects_missing_entry() {
        let mut index = ExtentIndex::new();
        let hash = h(7);
        let seg_ulid = Ulid::from_string("01JQTEST000000000000000400").unwrap();
        let removed = index.remove_if_matches(&hash, seg_ulid, 0);
        assert!(!removed);
        assert!(index.lookup(&hash).is_none());
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
