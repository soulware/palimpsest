// Volume: top-level I/O interface — owns the LBA map, WAL, and directory layout.
//
// Directory layout:
//   <base>/wal/       — active write-ahead log (at most one file at a time)
//   <base>/pending/   — promoted segments awaiting S3 upload
//   <base>/index/     — coordinator-written LBA index files (*.idx); permanent; never evicted
//   <base>/cache/     — coordinator-written body cache (*.body, *.present); evictable
//   <base>/gc/        — GC handoff files (coordinator-written `.staged`, volume-
//                       applied bare `<ulid>`; see docs/design-gc-self-describing-handoff.md)
//
// Write path:
//   1. Volume::write(lba, data) — hashes data, appends to WAL, updates LBA map
//      and extent index (WAL offset as temporary location)
//   2. When the WAL reaches FLUSH_THRESHOLD, it is promoted to a clean segment
//      in pending/ and the extent index is updated to segment offsets
//
// Read path:
//   1. lbamap.lookup(lba) → (hash, block_offset)
//   2. extent_index.lookup(hash) → ExtentLocation (segment_id, body_offset, body_length)
//   3. find_segment_file (wal/ → pending/ → bare gc/<id> → cache/<id>.body) → open file, seek, read
//
// Recovery:
//   Volume::open() calls lbamap::rebuild_segments() (segments only), then
//   scans the WAL once: that single pass truncates any partial-tail record,
//   replays entries into the LBA map, extent index, and pending_entries.
//   Any .tmp files in pending/ are removed (incomplete promotions).

use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use segment::BoxFetcher;

use ulid::Ulid;

use crate::{
    delta_compute,
    extentindex::{self, BodySource},
    gc_plan, lbamap,
    segment::{self, EntryKind},
    segment_cache,
    ulid_mint::UlidMint,
    writelog,
};

mod jobs;
mod readonly;
mod reclaim;
mod repack;

use jobs::GcCheckpointUlids;
pub use jobs::{
    GcCheckpointPrep, GcPlanApplyJob, GcPlanApplyResult, PromoteJob, PromoteResult,
    PromoteSegmentJob, PromoteSegmentPrep, PromoteSegmentResult, SignSnapshotManifestJob,
    SignSnapshotManifestResult, WorkerJob, WorkerResult,
};
pub use readonly::{
    ReadonlyVolume, fork_volume, fork_volume_at, fork_volume_at_with_manifest_key, latest_snapshot,
    resolve_ancestor_dir, verify_ancestor_manifests, walk_ancestors, walk_extent_ancestors,
};
use readonly::{create_fresh_wal, open_read_state, recover_wal, replay_wal_records};
pub use reclaim::{
    ReclaimCandidate, ReclaimJob, ReclaimOutcome, ReclaimResult, ReclaimThresholds, ReclaimedEntry,
    scan_reclaim_candidates,
};
pub use repack::{
    CompactionStats, DeltaRepackJob, DeltaRepackResult, DeltaRepackStats, DeltaRepackedSegment,
    RepackJob, RepackResult, RepackedDeadEntry, RepackedLiveEntry, RepackedSegment, SweepJob,
    SweepResult, SweptDeadEntry, SweptLiveEntry,
};

/// Compute the Shannon entropy of `data` in bits per byte.
///
/// Used to gate compression: data with entropy above 7.0 bits/byte is
/// already close to random and unlikely to compress meaningfully.
fn shannon_entropy(data: &[u8]) -> f64 {
    let mut counts = [0u32; 256];
    for &b in data {
        counts[b as usize] += 1;
    }
    let len = data.len() as f64;
    counts
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / len;
            -p * p.log2()
        })
        .sum()
}

/// Entropy threshold above which compression is skipped (bits/byte).
///
/// Taken from the lab47/lsvd reference implementation. Data at or above this
/// level is already near-random and compression would at best be a no-op.
const ENTROPY_THRESHOLD: f64 = 7.0;

/// Minimum compression ratio required to store compressed data (1.5×).
///
/// If the compressed payload is not at least 1/3 smaller than the original,
/// the compression overhead is not worth it and the raw data is stored instead.
const MIN_COMPRESSION_RATIO_NUM: usize = 3;
const MIN_COMPRESSION_RATIO_DEN: usize = 2;

/// Attempt lz4 compression on `data`.
///
/// Returns `Some(compressed_bytes)` if the entropy is low enough and the
/// compression ratio meets the minimum threshold; `None` to store raw.
pub(crate) fn maybe_compress(data: &[u8]) -> Option<Vec<u8>> {
    if shannon_entropy(data) > ENTROPY_THRESHOLD {
        return None;
    }
    let compressed = lz4_flex::compress_prepend_size(data);
    // Only keep if we achieve at least MIN_COMPRESSION_RATIO (1.5×).
    if compressed.len() * MIN_COMPRESSION_RATIO_NUM / MIN_COMPRESSION_RATIO_DEN >= data.len() {
        return None;
    }
    Some(compressed)
}

/// WAL size (bytes) at which the log is promoted to a pending segment.
/// This is a soft cap: a single write larger than this threshold will still
/// succeed, producing a segment larger than intended. The block layer
/// (NBD/ublk) enforces its own per-request maximum before reaching here.
const FLUSH_THRESHOLD: u64 = 32 * 1024 * 1024;

/// Entry-count cap at which the WAL is promoted, regardless of byte size.
/// Bounds the per-segment index region for workloads that produce many
/// thin entries (DedupRef, Zero, Inline) without advancing the byte cap.
/// Matches a 4 KiB-block 32 MiB segment exactly (32 MiB / 4 KiB = 8192).
const FLUSH_ENTRY_THRESHOLD: usize = 8192;

/// Maximum byte length of a single write. The segment format stores
/// `body_length` as a `u32`, so payloads must fit in 4 GiB. We cap at
/// `u32::MAX` rounded down to a 4 KiB boundary.
const MAX_WRITE_SIZE: usize = (u32::MAX as usize / 4096) * 4096;

/// Sentinel hash used in the LBA map and segment entries to represent an
/// explicitly-zeroed LBA range. All-zero bytes cannot be a valid BLAKE3 output
/// for any non-trivial input; finding a preimage would require breaking 256-bit
/// hash preimage resistance.
pub const ZERO_HASH: blake3::Hash = blake3::Hash::from_bytes([0u8; 32]);

/// Default capacity for the segment file handle LRU cache.
const FILE_CACHE_CAPACITY: usize = 16;

/// Default capacity for the parsed segment-index LRU cache. Each cached
/// entry holds `Vec<SegmentEntry>` for one segment (a few tens of KiB
/// for a 32 MiB segment); 64 entries comfortably covers the working set
/// for sweep/repack/delta_repack/promote passes without unbounded
/// memory growth on large volumes.
const SEGMENT_INDEX_CACHE_CAPACITY: usize = 64;

/// The on-disk layout of a cached segment file, which determines how body
/// offsets are interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentLayout {
    /// A full segment file (wal/, pending/, gc/). Body data starts at
    /// `body_section_start` — callers must add it to body-relative offsets.
    Full,
    /// A `.body` cache file (cache/<id>.body). Contains only body bytes
    /// starting at offset 0 — body-relative offsets are file offsets directly.
    BodyOnly,
}

/// Outcome of applying one `.staged` GC handoff via the derive-at-apply path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedApply {
    /// The staged segment was applied; extent index updated, body re-signed
    /// and renamed to bare, `.staged` file removed.
    Applied,
    /// The apply was cancelled (e.g. stale-liveness check) and the staged
    /// file was removed. Extent index is unchanged.
    Cancelled,
}

impl SegmentLayout {
    /// Determine the layout from a file path: `.body` extension → `BodyOnly`,
    /// everything else → `Full`.
    fn from_path(path: &Path) -> Self {
        if path.extension().is_some_and(|e| e == "body") {
            Self::BodyOnly
        } else {
            Self::Full
        }
    }
}

/// Approximate-LRU cache of open segment file handles using the CLOCK algorithm.
///
/// Fixed-size ring buffer keyed by segment ULID. Each slot has a `referenced`
/// bit that is set on access. On eviction the clock hand sweeps the ring,
/// clearing referenced bits until it finds an unreferenced slot to evict.
///
/// The hot-path operation (`get`) is a linear scan + flag set — no data
/// movement, no allocation, no pointer chasing.  At 16 slots the scan fits
/// comfortably in L1 cache.
pub(crate) struct FileCache {
    slots: Vec<Option<FileCacheSlot>>,
    hand: usize,
}

struct FileCacheSlot {
    segment_id: Ulid,
    layout: SegmentLayout,
    file: fs::File,
    referenced: bool,
}

impl Default for FileCache {
    fn default() -> Self {
        Self::new(FILE_CACHE_CAPACITY)
    }
}

impl FileCache {
    fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);
        Self { slots, hand: 0 }
    }

    /// Look up a cached file handle by segment id.
    /// On hit, sets the referenced bit and returns the layout and file handle.
    fn get(&mut self, segment_id: Ulid) -> Option<(SegmentLayout, &mut fs::File)> {
        let slot = self
            .slots
            .iter_mut()
            .flatten()
            .find(|s| s.segment_id == segment_id)?;
        slot.referenced = true;
        Some((slot.layout, &mut slot.file))
    }

    /// Insert a file handle. If the segment is already cached, replaces it
    /// in-place. Otherwise, uses the CLOCK algorithm to find a slot to evict.
    fn insert(&mut self, segment_id: Ulid, layout: SegmentLayout, file: fs::File) {
        // Replace in-place if already present.
        for slot in self.slots.iter_mut() {
            if slot.as_ref().is_some_and(|s| s.segment_id == segment_id) {
                *slot = Some(FileCacheSlot {
                    segment_id,
                    layout,
                    file,
                    referenced: true,
                });
                return;
            }
        }

        // Fill an empty slot if one exists.
        for slot in self.slots.iter_mut() {
            if slot.is_none() {
                *slot = Some(FileCacheSlot {
                    segment_id,
                    layout,
                    file,
                    referenced: true,
                });
                return;
            }
        }

        // CLOCK sweep: advance the hand, clearing referenced bits, until we
        // find an unreferenced slot to evict.
        let len = self.slots.len();
        loop {
            let slot = self.slots[self.hand].as_mut().expect("all slots occupied");
            if slot.referenced {
                slot.referenced = false;
                self.hand = (self.hand + 1) % len;
            } else {
                self.slots[self.hand] = Some(FileCacheSlot {
                    segment_id,
                    layout,
                    file,
                    referenced: true,
                });
                self.hand = (self.hand + 1) % len;
                return;
            }
        }
    }

    /// Evict all entries for a given segment.
    pub(crate) fn evict(&mut self, segment_id: Ulid) {
        for slot in self.slots.iter_mut() {
            if slot.as_ref().is_some_and(|s| s.segment_id == segment_id) {
                *slot = None;
            }
        }
    }

    /// Clear all entries.
    pub(crate) fn clear(&mut self) {
        for slot in self.slots.iter_mut() {
            *slot = None;
        }
    }
}

/// A fork ancestry layer used when rebuilding the LBA map and extent index.
///
/// `branch_ulid` is the latest segment ULID from this fork that belongs to the
/// derived fork's view — segments with a strictly greater ULID were written after
/// the branch point and must not be included. `None` for the live (current) fork,
/// where all segments are always included.
#[derive(Clone)]
pub struct AncestorLayer {
    pub dir: PathBuf,
    pub branch_ulid: Option<String>,
}

/// On-disk WAL state: file handle, ULID, and path. Present iff a WAL file
/// exists under `wal/<ulid>`. Absent between promotes / on idle volumes.
struct OpenWal {
    wal: writelog::WriteLog,
    ulid: Ulid,
    path: PathBuf,
}

/// A writable block-device volume backed by a content-addressable store.
///
/// Owns the in-memory LBA map, the active WAL, and the directory layout.
/// In the Named Forks model, `base_dir` is the fork directory (e.g.
/// `volumes/myvm/default/`), not the volume root.
pub struct Volume {
    base_dir: PathBuf,
    /// Ancestor fork layers, oldest-first. Does not include the current fork.
    ancestor_layers: Vec<AncestorLayer>,
    /// Exclusive lock on `base_dir/volume.lock`. Held for the lifetime of the Volume.
    /// The `Flock` releases the lock automatically when dropped.
    #[allow(dead_code)]
    lock_file: nix::fcntl::Flock<fs::File>,
    lbamap: Arc<lbamap::LbaMap>,
    extent_index: Arc<extentindex::ExtentIndex>,
    /// Lazy WAL state. `None` means no WAL file exists on disk — the next
    /// write opens a fresh one at `mint.next()`. Keeps idle volumes from
    /// churning the WAL on every GC tick.
    wal: Option<OpenWal>,
    /// DATA and REF extents written since the last promotion; used to write
    /// the clean segment file on the next promote().
    pending_entries: Vec<segment::SegmentEntry>,
    /// True if at least one segment has been committed since the last snapshot
    /// (or since open, if no snapshot has been taken this session). Used by
    /// `snapshot()` to decide whether a new marker is needed or the latest
    /// existing snapshot can be reused.
    has_new_segments: bool,
    /// ULID of the most recently committed segment across pending/ and index/,
    /// or `None` if no segments exist. Used by `snapshot()` to name the snapshot
    /// marker with the same ULID as the segment it covers.
    last_segment_ulid: Option<Ulid>,
    /// LRU cache of open segment file handles for the read path.
    ///
    /// Retains recently-opened segment files across `read` calls so that
    /// reads hitting the same segments avoid repeated `open` syscalls.
    /// `RefCell` keeps `read` logically non-mutating (`&self`) while allowing
    /// the cache to be updated internally.
    file_cache: RefCell<FileCache>,
    /// Signer for segment promotion. Every segment written by this volume
    /// (at WAL promotion and compaction) is signed with the fork's private key.
    /// See `segment::SegmentSigner`.
    signer: Arc<dyn segment::SegmentSigner>,
    /// Verifying key derived from `volume.key` at open time. Used to verify
    /// segment signatures when reading during compaction and GC.
    verifying_key: ed25519_dalek::VerifyingKey,
    /// Optional fetcher for demand-fetch on segment cache miss. When set,
    /// `find_segment_file` fetches missing segments from remote storage and
    /// caches them in `cache/`. See `segment::SegmentFetcher`.
    fetcher: Option<BoxFetcher>,
    /// Monotonic ULID generator. Seeded from the highest known ULID at open
    /// (WAL filename or max segment). Used for all WAL and compaction outputs
    /// to guarantee strict ordering regardless of host clock behaviour.
    mint: UlidMint,
    /// Stats for the no-op write skip path (LBA-map hash compare).
    /// See `docs/design-noop-write-skip.md`.
    noop_stats: NoopSkipStats,
    /// Shared LRU of parsed+verified segment indices. Keyed by
    /// `(path, file_len)`. Cloned into worker jobs so the actor thread
    /// and the worker thread hit the same cache. See
    /// `segment_cache::SegmentIndexCache`.
    segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// Counters for the no-op write skip path. Reset to zero on `Volume::open`.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSkipStats {
    /// Number of `write()` calls short-circuited because the LBA map
    /// already records the incoming content's hash at the target range.
    pub skipped_writes: u64,
    /// Total bytes of incoming data the skip avoided writing to the WAL.
    pub skipped_bytes: u64,
}

impl Volume {
    /// Open (or create) a fork at `base_dir`.
    ///
    /// `base_dir` must be the fork directory (e.g. `volumes/myvm/default/`), not the
    /// volume root. Creates `wal/` and `pending/` if they do not exist.
    /// Rebuilds the LBA map from all committed segments across the ancestry chain
    /// (following `volume.parent` files), then recovers or creates the WAL.
    ///
    /// Loads the signing key from `volume.key` in `base_dir`. Fails hard if the key
    /// is absent — every writable volume must have a signing key. Fork from a snapshot
    /// to create a new writable volume with a fresh keypair.
    pub fn open(base_dir: &Path, by_id_dir: &Path) -> io::Result<Self> {
        let (signer, verifying_key) =
            crate::signing::load_keypair(base_dir, crate::signing::VOLUME_KEY_FILE).map_err(
                |e| {
                    io::Error::other(format!(
                        "{e}; fork from a snapshot to create a writable volume"
                    ))
                },
            )?;
        Self::open_impl(base_dir, signer, verifying_key, by_id_dir)
    }

    fn open_impl(
        base_dir: &Path,
        signer: Arc<dyn segment::SegmentSigner>,
        verifying_key: ed25519_dalek::VerifyingKey,
        by_id_dir: &Path,
    ) -> io::Result<Self> {
        let wal_dir = base_dir.join("wal");
        let pending_dir = base_dir.join("pending");

        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&pending_dir)?;

        // Acquire exclusive lock. Fails immediately if another process has this
        // fork open. The lock is released when Volume is dropped.
        let lock_file = acquire_lock(base_dir)?;

        // Remove any .tmp files in pending/ — incomplete promotions from a crash.
        for entry in fs::read_dir(&pending_dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                fs::remove_file(&path)?;
            }
        }

        // Walk the origin chain and rebuild maps from all committed segments.
        let (ancestor_layers, mut lbamap, mut extent_index) = open_read_state(base_dir, by_id_dir)?;

        // Find the in-progress WAL file (there should be at most one).
        let mut wal_files: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(&wal_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                wal_files.push(entry.path());
            }
        }
        wal_files.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

        // Edge case: if pending/<ulid> already exists alongside wal/<ulid>,
        // the promotion completed (rename succeeded) but the WAL delete was
        // interrupted. The segment is authoritative — delete the stale WAL file.
        wal_files.retain(|path| {
            let Some(ulid) = path.file_name().and_then(|s| s.to_str()) else {
                return true; // non-UTF-8 name: leave it alone
            };
            if pending_dir.join(ulid).exists() {
                let _ = fs::remove_file(path);
                false
            } else {
                true
            }
        });

        // Scan pending/ and index/ to find the latest committed segment ULID
        // and determine whether any segments postdate the latest snapshot.
        // Cross-session ULID comparison is reliable: those files came from
        // earlier runs at distinct timestamps.
        //
        // Done before WAL recovery so we can compute the mint floor below.
        let latest_snap = latest_snapshot(base_dir)?;
        let mut last_segment_ulid: Option<Ulid> = None;
        // Collect pending/ segment ULIDs (full files, not yet uploaded).
        for p in segment::collect_segment_files(&base_dir.join("pending"))? {
            if let Some(ulid) = p
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| Ulid::from_string(s).ok())
                && last_segment_ulid < Some(ulid)
            {
                last_segment_ulid = Some(ulid);
            }
        }
        // Collect index/*.idx ULIDs (uploaded segments; file stem is the ULID).
        for p in segment::collect_idx_files(&base_dir.join("index"))? {
            if let Some(ulid) = p
                .file_stem()
                .and_then(|n| n.to_str())
                .and_then(|s| Ulid::from_string(s).ok())
                && last_segment_ulid < Some(ulid)
            {
                last_segment_ulid = Some(ulid);
            }
        }
        // A volume-applied GC output (bare `gc/<ulid>`) has ULID =
        // max(inputs).increment(), which may be the highest known ULID —
        // include it so the mint floor is correct.
        for p in segment::collect_gc_applied_segment_files(base_dir)? {
            if let Some(ulid) = p
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| Ulid::from_string(s).ok())
                && last_segment_ulid < Some(ulid)
            {
                last_segment_ulid = Some(ulid);
            }
        }

        // Compute the mint floor: max of the highest segment ULID and the
        // WAL filename ULID (if one exists). This guarantees the first fresh
        // WAL ULID is above all existing local data even when the system clock
        // has drifted backwards.
        let segment_floor = last_segment_ulid.unwrap_or(Ulid::from_parts(0, 0));
        let wal_floor = wal_files
            .last()
            .and_then(|p| p.file_name().and_then(|n| n.to_str()))
            .and_then(|s| Ulid::from_string(s).ok())
            .unwrap_or(Ulid::from_parts(0, 0));
        let mut mint = UlidMint::new(segment_floor.max(wal_floor));

        // Promote every non-latest WAL to a fresh segment so the volume
        // returns to its "one active WAL" invariant before we open the
        // actor. This path fires when a crash or the off-actor worker
        // (Landing 3) leaves multiple WAL files behind; in normal single-
        // WAL operation the loop body never executes.
        //
        // The freshly-minted segment ULID is strictly > any wal_floor or
        // segment_floor (mint monotonicity), so it never collides with an
        // existing file. Entries use the same CAS apply path as the online
        // `flush_wal_to_pending_as` flow — safe even when an orphan pending
        // segment from the pre-crash worker has already repopulated the
        // same hashes.
        let wal_files_to_promote: Vec<PathBuf> = if wal_files.len() > 1 {
            let split = wal_files.len() - 1;
            let rest = wal_files.split_off(split);
            std::mem::replace(&mut wal_files, rest)
        } else {
            Vec::new()
        };
        for wal_path in wal_files_to_promote {
            let (old_wal_ulid, _valid_size, mut entries) =
                replay_wal_records(&wal_path, &mut lbamap, &mut extent_index)?;
            if entries.is_empty() {
                fs::remove_file(&wal_path)?;
                continue;
            }
            // Snapshot pre-promote WAL offsets for the CAS apply, matching
            // `flush_wal_to_pending_as`.
            let pre_promote_offsets: Vec<Option<u64>> = entries
                .iter()
                .map(|e| match e.kind {
                    EntryKind::Data
                    | EntryKind::Inline
                    | EntryKind::CanonicalData
                    | EntryKind::CanonicalInline => {
                        extent_index.lookup(&e.hash).map(|loc| loc.body_offset)
                    }
                    EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => None,
                })
                .collect();
            let segment_ulid = mint.next();
            let body_section_start = segment::write_and_commit(
                &pending_dir,
                segment_ulid,
                &mut entries,
                signer.as_ref(),
            )?;
            for (entry, old_wal_offset) in entries.iter().zip(pre_promote_offsets.iter().copied()) {
                match entry.kind {
                    EntryKind::Data
                    | EntryKind::Inline
                    | EntryKind::CanonicalData
                    | EntryKind::CanonicalInline => {}
                    EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
                }
                let Some(old_wal_offset) = old_wal_offset else {
                    continue;
                };
                let idata = if entry.kind.is_inline() {
                    entry.data.clone().map(Vec::into_boxed_slice)
                } else {
                    None
                };
                extent_index.replace_if_matches(
                    entry.hash,
                    old_wal_ulid,
                    old_wal_offset,
                    extentindex::ExtentLocation {
                        segment_id: segment_ulid,
                        body_offset: entry.stored_offset,
                        body_length: entry.stored_length,
                        compressed: entry.compressed,
                        body_source: BodySource::Local,
                        body_section_start,
                        inline_data: idata,
                    },
                );
            }
            // Bump last_segment_ulid so the first-snapshot pinning invariant
            // (see `Volume::snapshot`) covers this recovery-promoted segment.
            if last_segment_ulid < Some(segment_ulid) {
                last_segment_ulid = Some(segment_ulid);
            }
            fs::remove_file(&wal_path)?;
        }

        // recover_wal does the single WAL scan: truncates any partial tail,
        // replays records into the LBA map, and rebuilds pending_entries.
        // When no WAL file is present on disk, leave `wal` as None; the next
        // write lazily opens a fresh WAL. This avoids creating an empty WAL
        // for read-only volumes and idle sessions that never write.
        let (wal, pending_entries) = if let Some(path) = wal_files.into_iter().last() {
            let (wal, ulid, path, pending_entries) =
                recover_wal(path, &mut lbamap, &mut extent_index)?;
            (Some(OpenWal { wal, ulid, path }), pending_entries)
        } else {
            (None, Vec::new())
        };

        let has_new_segments = !pending_entries.is_empty()
            || matches!((&latest_snap, &last_segment_ulid), (Some(snap), Some(last)) if last > snap);

        Ok(Self {
            base_dir: base_dir.to_owned(),
            ancestor_layers,
            lock_file,
            lbamap: Arc::new(lbamap),
            extent_index: Arc::new(extent_index),
            wal,
            pending_entries,
            has_new_segments,
            last_segment_ulid,
            file_cache: RefCell::new(FileCache::default()),
            signer,
            verifying_key,
            fetcher: None,
            mint,
            noop_stats: NoopSkipStats::default(),
            segment_cache: Arc::new(segment_cache::SegmentIndexCache::new(
                SEGMENT_INDEX_CACHE_CAPACITY,
            )),
        })
    }

    /// Write `data` starting at logical block address `lba`.
    ///
    /// `data.len()` must be a non-zero multiple of 4096 and must not exceed
    /// `MAX_WRITE_SIZE` (4 GiB − 4 KiB). The segment format stores `body_length`
    /// as a `u32` byte count, so larger payloads cannot be represented.
    ///
    /// The data is appended to the WAL and the LBA map is updated in memory.
    /// Promotion to a pending segment is triggered after the write if the WAL
    /// reaches `FLUSH_THRESHOLD` (32 MiB). Because the check is post-write, a
    /// single large write may produce a segment larger than the threshold; the
    /// block layer (NBD/ublk) is expected to enforce its own per-request cap.
    pub fn write(&mut self, lba: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() || !data.len().is_multiple_of(4096) {
            return Err(io::Error::other(
                "data length must be a non-zero multiple of 4096",
            ));
        }
        if data.len() > MAX_WRITE_SIZE {
            return Err(io::Error::other(
                "data length exceeds maximum write size (4 GiB − 4 KiB)",
            ));
        }
        let hash = blake3::hash(data);
        self.write_with_hash(lba, data, hash).map(|_| ())
    }

    /// Like `write`, but with a caller-supplied hash. Returns `Ok(true)` if
    /// the write was committed to the WAL, `Ok(false)` if the no-op skip
    /// short-circuited it.
    ///
    /// Used by callers that have already hashed `data` (notably extent
    /// reclamation, which hashes off-actor in phase 2 and would otherwise
    /// pay a redundant blake3 pass on the actor thread). The caller-supplied
    /// `hash` MUST be `blake3::hash(data)`.
    ///
    /// See `docs/design-noop-write-skip.md`.
    pub fn write_with_hash(
        &mut self,
        lba: u64,
        data: &[u8],
        hash: blake3::Hash,
    ) -> io::Result<bool> {
        if data.is_empty() || !data.len().is_multiple_of(4096) {
            return Err(io::Error::other(
                "data length must be a non-zero multiple of 4096",
            ));
        }
        if data.len() > MAX_WRITE_SIZE {
            return Err(io::Error::other(
                "data length exceeds maximum write size (4 GiB − 4 KiB)",
            ));
        }
        let lba_length = (data.len() / 4096) as u32;

        // No-op skip — pure LBA map lookup, zero body I/O. BLAKE3
        // collision resistance means hash equality implies byte equality,
        // so this is safe regardless of where the body lives (Local,
        // Cached present, Cached absent, or S3-only). See
        // `docs/design-noop-write-skip.md`.
        if self.lbamap.has_full_match(lba, lba_length, &hash) {
            self.noop_stats.skipped_writes += 1;
            self.noop_stats.skipped_bytes += data.len() as u64;
            return Ok(false);
        }

        self.write_commit(lba, lba_length, data, hash)?;
        Ok(true)
    }

    /// Shared tail of the write path after the no-op skip check has
    /// decided the bytes must hit the WAL.
    fn write_commit(
        &mut self,
        lba: u64,
        lba_length: u32,
        data: &[u8],
        hash: blake3::Hash,
    ) -> io::Result<()> {
        let compressed_data = maybe_compress(data);
        let compressed = compressed_data.is_some();
        let owned_data: Vec<u8> = compressed_data.unwrap_or_else(|| data.to_vec());
        let wal_flags = if compressed {
            writelog::WalFlags::COMPRESSED
        } else {
            writelog::WalFlags::empty()
        };

        // Write-path dedup: if this extent already exists in this volume's
        // segment tree (own segments + ancestors), write a thin REF record
        // instead of a DATA record. No body bytes in the WAL — reads resolve
        // through the extent index to the canonical segment's body.
        if self.extent_index.lookup(&hash).is_some() {
            self.ensure_wal_open()?
                .wal
                .append_ref(lba, lba_length, &hash)?;
            Arc::make_mut(&mut self.lbamap).insert(lba, lba_length, hash);
            // Do NOT update extent_index — the canonical entry already points
            // to the segment with the body bytes. DedupRef entries carry no
            // body bytes and no body reservation.
            self.pending_entries
                .push(segment::SegmentEntry::new_dedup_ref(hash, lba, lba_length));
            return Ok(());
        }

        let seg_flags = if compressed {
            segment::SegmentFlags::COMPRESSED
        } else {
            segment::SegmentFlags::empty()
        };

        let (body_offset, wal_ulid) = {
            let open = self.ensure_wal_open()?;
            let offset = open
                .wal
                .append_data(lba, lba_length, &hash, wal_flags, &owned_data)?;
            (offset, open.ulid)
        };
        Arc::make_mut(&mut self.lbamap).insert(lba, lba_length, hash);
        // Temporary extent index entry: points into the WAL at the raw payload offset.
        // Updated to segment file offsets after promotion.
        Arc::make_mut(&mut self.extent_index).insert(
            hash,
            extentindex::ExtentLocation {
                segment_id: wal_ulid,
                body_offset,
                body_length: owned_data.len() as u32,
                compressed,
                body_source: BodySource::Local,
                body_section_start: 0,
                inline_data: None,
            },
        );
        self.pending_entries.push(segment::SegmentEntry::new_data(
            hash, lba, lba_length, seg_flags, owned_data,
        ));

        Ok(())
    }

    /// Open the WAL if it is currently absent. Mints a fresh ULID from
    /// `self.mint` — always monotonically above any prior segment or
    /// checkpoint ULID, preserving the "new WAL above GC output" invariant
    /// without needing a reserved `u_wal` in `GcCheckpointUlids`.
    fn ensure_wal_open(&mut self) -> io::Result<&mut OpenWal> {
        if self.wal.is_none() {
            let ulid = self.mint.next();
            let (wal, ulid, path, _) = create_fresh_wal(&self.base_dir.join("wal"), ulid)?;
            self.wal = Some(OpenWal { wal, ulid, path });
        }
        // ensure_wal_open just populated self.wal if it was None.
        Ok(self.wal.as_mut().expect("wal open"))
    }

    /// Zero `lba_count` blocks starting at `lba`.
    ///
    /// Appends a single ZERO WAL record covering the entire range — no hashing,
    /// no data payload, no chunking. The LBA map entry uses `ZERO_HASH` as a
    /// sentinel, which the read path recognises and short-circuits to return
    /// zeros without any extent index lookup.
    ///
    /// Zero extents explicitly override ancestor data: a ZERO_HASH entry in the
    /// LBA map masks any data at those LBAs in ancestor segments, unlike an
    /// unwritten LBA range which falls through to the ancestor.
    pub fn write_zeroes(&mut self, start_lba: u64, lba_count: u32) -> io::Result<()> {
        self.ensure_wal_open()?
            .wal
            .append_zero(start_lba, lba_count)?;
        Arc::make_mut(&mut self.lbamap).insert(start_lba, lba_count, ZERO_HASH);
        self.pending_entries
            .push(segment::SegmentEntry::new_zero(start_lba, lba_count));
        Ok(())
    }

    /// Trim (discard) `lba_count` blocks starting at `lba`.
    ///
    /// Implemented via `write_zeroes` — a single zero-extent WAL record with no
    /// data payload. The whole-volume TRIM issued by `mkfs.ext4` becomes one
    /// ~40-byte record regardless of volume size.
    pub fn trim(&mut self, start_lba: u64, lba_count: u32) -> io::Result<()> {
        self.write_zeroes(start_lba, lba_count)
    }

    /// Read `lba_count` blocks (4096 bytes each) starting at `lba`.
    ///
    /// Blocks that have never been written are returned as zeros (the
    /// block-device convention for unwritten regions). Written blocks are
    /// fetched extent-by-extent: one file open and one read (or decompress)
    /// per extent, regardless of how many blocks within the extent are needed.
    pub fn read(&self, lba: u64, lba_count: u32) -> io::Result<Vec<u8>> {
        read_extents(
            lba,
            lba_count,
            &self.lbamap,
            &self.extent_index,
            &self.file_cache,
            |id, bss, idx| self.find_segment_file(id, bss, idx),
            |id| {
                open_delta_body_in_dirs(
                    id,
                    &self.base_dir,
                    &self.ancestor_layers,
                    self.fetcher.as_ref(),
                )
            },
        )
    }

    /// Flush buffered WAL writes and fsync to disk. No-op when no WAL is open.
    pub fn fsync(&mut self) -> io::Result<()> {
        match self.wal.as_mut() {
            Some(open) => open.wal.fsync(),
            None => Ok(()),
        }
    }

    /// No-op skip counters. See `docs/design-noop-write-skip.md`.
    pub fn noop_stats(&self) -> NoopSkipStats {
        self.noop_stats
    }

    /// Compact sparse segments in `pending/`.
    ///
    /// For each segment where the ratio of live stored bytes to total stored
    /// bytes is below `min_live_ratio`, the live extents are copied into a new
    /// denser segment in `pending/` and the old segment is deleted. Segments
    /// where all extents are dead are deleted directly without writing a new one.
    ///
    /// The WAL is not touched. The extent index is updated in place.
    ///
    /// `min_live_ratio` is in [0.0, 1.0]: 0.7 compacts any segment where more
    /// than 30% of stored bytes are dead.
    ///
    /// Synchronous wrapper around [`Self::prepare_repack`] +
    /// [`crate::actor::execute_repack`] + [`Self::apply_repack_result`].
    /// The actor offloads the middle phase to the worker thread; callers
    /// that hold a `&mut Volume` directly (tests, tools) can keep using
    /// this wrapper.
    pub fn repack(&mut self, min_live_ratio: f64) -> io::Result<CompactionStats> {
        let Some(job) = self.prepare_repack(min_live_ratio)? else {
            return Ok(CompactionStats::default());
        };
        let result = crate::actor::execute_repack(job)?;
        self.apply_repack_result(result)
    }

    /// Prep phase of `repack` — runs on the actor thread.
    ///
    /// Snapshots `lbamap` (used by the worker to recompute liveness),
    /// resolves the snapshot floor, and packages the directory + signer
    /// state into a [`RepackJob`]. Returns `None` when `pending/` is
    /// missing or has no segments.
    ///
    /// All segment reads, eligibility decisions, and rewrites run in
    /// [`crate::actor::execute_repack`]. The apply phase
    /// [`Self::apply_repack_result`] does the conditional extent-index
    /// updates and file-cache eviction.
    pub fn prepare_repack(&self, min_live_ratio: f64) -> io::Result<Option<RepackJob>> {
        let pending_dir = self.base_dir.join("pending");
        let segs = match segment::collect_segment_files(&pending_dir) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if segs.is_empty() {
            return Ok(None);
        }
        let floor = latest_snapshot(&self.base_dir)?;
        Ok(Some(RepackJob {
            lbamap: Arc::clone(&self.lbamap),
            floor,
            pending_dir,
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
            min_live_ratio,
        }))
    }

    /// Apply phase of `repack` — runs on the actor thread after the
    /// worker returns.
    ///
    /// For each rewritten segment: re-points live Data/Inline entries via
    /// `replace_if_matches((hash, seg_id, source_body_offset) → new_loc)`,
    /// where the new location reuses the same `seg_id` but with the
    /// post-write `body_offset`. A concurrent writer that superseded the
    /// hash between prep and apply fails the CAS and is preserved
    /// untouched — the repacked body simply becomes unreferenced until
    /// the next pass picks it up.
    ///
    /// Dead entries are dropped with `remove_if_matches` for the same
    /// reason. File-cache eviction runs for every touched segment so a
    /// cached fd on the old inode cannot serve stale offsets after the
    /// rename.
    pub fn apply_repack_result(&mut self, result: RepackResult) -> io::Result<CompactionStats> {
        let RepackResult {
            mut stats,
            segments,
        } = result;

        for seg in &segments {
            for dead in &seg.dead {
                if Arc::make_mut(&mut self.extent_index).remove_if_matches(
                    &dead.hash,
                    seg.seg_id,
                    dead.source_body_offset,
                ) {
                    stats.extents_removed += 1;
                }
            }

            self.evict_cached_segment(seg.seg_id);

            if !seg.all_dead_deleted {
                for live in &seg.live {
                    let entry = &live.entry;
                    let inline_data = match entry.kind {
                        EntryKind::Data | EntryKind::CanonicalData => None,
                        EntryKind::Inline | EntryKind::CanonicalInline => {
                            entry.data.clone().map(Vec::into_boxed_slice)
                        }
                        EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
                    };
                    Arc::make_mut(&mut self.extent_index).replace_if_matches(
                        entry.hash,
                        seg.seg_id,
                        live.source_body_offset,
                        extentindex::ExtentLocation {
                            segment_id: seg.seg_id,
                            body_offset: entry.stored_offset,
                            body_length: entry.stored_length,
                            compressed: entry.compressed,
                            body_source: BodySource::Local,
                            body_section_start: seg.new_body_section_start,
                            inline_data,
                        },
                    );
                }
            }
        }

        Ok(stats)
    }

    /// Phase 5 Tier 1: rewrite post-snapshot pending segments with
    /// zstd-dictionary deltas against same-LBA extents from the prior
    /// sealed snapshot.
    ///
    /// For every segment in `pending/` whose ULID is greater than the
    /// latest sealed snapshot, walks single-block `Data` entries and
    /// looks up the LBA in a snapshot-pinned `BlockReader` on that
    /// snapshot. If the prior snapshot holds a different extent at
    /// that LBA and the source body is locally available, the entry
    /// is converted to a thin `Delta` with the prior extent as its
    /// dictionary source.
    ///
    /// Runs on post-snapshot segments only — never touches segments
    /// that are part of a sealed snapshot. No-op when there is no
    /// sealed snapshot (nothing to source deltas from) or when no
    /// entries match.
    ///
    /// Synchronous wrapper around [`Self::prepare_delta_repack`] +
    /// [`crate::actor::execute_delta_repack`] +
    /// [`Self::apply_delta_repack_result`]. The actor uses the three
    /// phases directly so that the heavy middle phase runs off the
    /// request channel; this wrapper exists for tests and any inline
    /// callers.
    pub fn delta_repack_post_snapshot(&mut self) -> io::Result<DeltaRepackStats> {
        let Some(job) = self.prepare_delta_repack()? else {
            return Ok(DeltaRepackStats::default());
        };
        let result = crate::actor::execute_delta_repack(job)?;
        self.apply_delta_repack_result(result)
    }

    /// Prep phase of `delta_repack_post_snapshot` — runs on the actor
    /// thread.
    ///
    /// Resolves the latest sealed snapshot and packages the signer
    /// state + segment-cache handle + base/pending directories into a
    /// [`DeltaRepackJob`]. Returns `None` when there is no sealed
    /// snapshot (nothing to source deltas from); the worker dispatch is
    /// then skipped.
    ///
    /// The snapshot-pinned `BlockReader` is constructed by the worker,
    /// not here — its provenance walk and extent-index rebuild can run
    /// off the actor.
    pub fn prepare_delta_repack(&self) -> io::Result<Option<DeltaRepackJob>> {
        let Some(snap_ulid) = latest_snapshot(&self.base_dir)? else {
            return Ok(None);
        };
        Ok(Some(DeltaRepackJob {
            base_dir: self.base_dir.clone(),
            pending_dir: self.base_dir.join("pending"),
            snap_ulid,
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
        }))
    }

    /// Apply phase of `delta_repack_post_snapshot` — runs on the actor
    /// thread after the worker returns.
    ///
    /// For each rewritten segment: evicts the fd cache, then walks
    /// entries paired with their pre-rewrite `(kind, stored_offset)`.
    /// CAS against the source location so a concurrent writer that
    /// re-pointed a hash at a newer segment wins — we leave their
    /// entry untouched, and the repacked body simply becomes
    /// unreferenced until the next pass picks it up.
    ///
    /// - Data kept as Data, Inline kept as Inline:
    ///   `replace_if_matches(hash, seg_id, pre_offset, new_loc)`.
    /// - Data converted to Delta:
    ///   `remove_if_matches(hash, seg_id, pre_offset)` followed by
    ///   `insert_delta_if_absent(...)`.
    /// - Pre-existing Delta: re-register via `insert_delta_if_absent`
    ///   — no-op if the delta slot is still valid, which matches the
    ///   pre-offload behaviour.
    pub fn apply_delta_repack_result(
        &mut self,
        result: DeltaRepackResult,
    ) -> io::Result<DeltaRepackStats> {
        let DeltaRepackResult {
            mut stats,
            segments,
        } = result;

        for seg in segments {
            let DeltaRepackedSegment { seg_id, rewrite } = seg;
            let crate::delta_compute::RewrittenSegment {
                entries,
                new_body_section_start: new_bss,
                delta_region_body_length,
                stats: seg_stats,
            } = rewrite;

            // Evict the file-cache fd — the segment file was swapped
            // atomically by the worker's rename. A surviving cached fd
            // on the old inode would serve reads with new offsets after
            // the index update below lands.
            self.evict_cached_segment(seg_id);

            let entries_len = entries.len();
            let ei = Arc::make_mut(&mut self.extent_index);
            for item in &entries {
                let post = &item.post;
                match (item.pre_kind, post.kind) {
                    (EntryKind::Data, EntryKind::Data) => {
                        ei.replace_if_matches(
                            post.hash,
                            seg_id,
                            item.pre_stored_offset,
                            extentindex::ExtentLocation {
                                segment_id: seg_id,
                                body_offset: post.stored_offset,
                                body_length: post.stored_length,
                                compressed: post.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: None,
                            },
                        );
                    }
                    (EntryKind::Inline, EntryKind::Inline) => {
                        ei.replace_if_matches(
                            post.hash,
                            seg_id,
                            item.pre_stored_offset,
                            extentindex::ExtentLocation {
                                segment_id: seg_id,
                                body_offset: post.stored_offset,
                                body_length: post.stored_length,
                                compressed: post.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: post.data.clone().map(Vec::into_boxed_slice),
                            },
                        );
                    }
                    (EntryKind::Data, EntryKind::Delta) => {
                        ei.remove_if_matches(&post.hash, seg_id, item.pre_stored_offset);
                        ei.insert_delta_if_absent(
                            post.hash,
                            extentindex::DeltaLocation {
                                segment_id: seg_id,
                                body_source: extentindex::DeltaBodySource::Full {
                                    body_section_start: new_bss,
                                    body_length: delta_region_body_length,
                                },
                                options: post.delta_options.clone(),
                            },
                        );
                        let sources: Arc<[blake3::Hash]> =
                            post.delta_options.iter().map(|o| o.source_hash).collect();
                        Arc::make_mut(&mut self.lbamap).set_delta_sources_if_matches(
                            post.start_lba,
                            post.hash,
                            sources,
                        );
                    }
                    (EntryKind::Delta, EntryKind::Delta) => {
                        // Pre-existing delta: re-register so a reader
                        // hitting this hash sees the updated
                        // body_section_start. `_if_absent` is a no-op
                        // when the slot still exists — matches the
                        // pre-offload behaviour.
                        ei.insert_delta_if_absent(
                            post.hash,
                            extentindex::DeltaLocation {
                                segment_id: seg_id,
                                body_source: extentindex::DeltaBodySource::Full {
                                    body_section_start: new_bss,
                                    body_length: delta_region_body_length,
                                },
                                options: post.delta_options.clone(),
                            },
                        );
                        let sources: Arc<[blake3::Hash]> =
                            post.delta_options.iter().map(|o| o.source_hash).collect();
                        Arc::make_mut(&mut self.lbamap).set_delta_sources_if_matches(
                            post.start_lba,
                            post.hash,
                            sources,
                        );
                    }
                    (EntryKind::DedupRef, _) | (EntryKind::Zero, _) => {}
                    // Kind transitions other than Data→Data, Data→Delta,
                    // Inline→Inline, Delta→Delta aren't produced by
                    // `rewrite_post_snapshot_with_prior`; ignore rather
                    // than assert so a future rewriter extension can't
                    // crash the actor.
                    _ => {}
                }
            }

            log::info!(
                "delta_repack: seg {seg_id} converted {}/{} entries, {}→{} bytes",
                seg_stats.entries_converted,
                entries_len,
                seg_stats.original_body_bytes,
                seg_stats.delta_body_bytes,
            );

            stats.segments_rewritten += 1;
            stats.entries_converted += seg_stats.entries_converted;
            stats.original_body_bytes += seg_stats.original_body_bytes;
            stats.delta_body_bytes += seg_stats.delta_body_bytes;
        }

        Ok(stats)
    }

    /// Compact `pending/` segments opportunistically, before upload.
    ///
    /// Scans every segment in `pending/`. A segment is a candidate if:
    /// - it has at least one dead extent (an LBA since overwritten), or
    /// - its file size is below [`COMPACT_SMALL_THRESHOLD`] (8 MiB).
    ///
    /// All candidates are merged: their live extents are collected, written into
    /// one or more new `pending/<ulid>` segments (split at [`FLUSH_THRESHOLD`]),
    /// the extent index is updated, and the originals are deleted.
    ///
    /// Segments at or below the latest snapshot ULID are frozen and skipped.
    /// Returns immediately (no-op) if there are no candidates.
    ///
    /// Synchronous wrapper around the offloadable prep / execute / apply
    /// trio. The actor uses [`Self::prepare_sweep`] +
    /// [`crate::actor::execute_sweep`] + [`Self::apply_sweep_result`]
    /// directly so that the worker thread runs the heavy middle phase off
    /// the request channel; this wrapper exists for tests and any
    /// remaining inline callers.
    pub fn sweep_pending(&mut self) -> io::Result<CompactionStats> {
        let Some(job) = self.prepare_sweep()? else {
            return Ok(CompactionStats::default());
        };
        let result = crate::actor::execute_sweep(job)?;
        self.apply_sweep_result(result)
    }

    /// Prep phase of `sweep_pending` — runs on the actor thread.
    ///
    /// Snapshots `lbamap` (used by the worker for `DedupRef` liveness),
    /// resolves the snapshot floor, and packages the directory + signer
    /// state into a [`SweepJob`]. Returns `None` when `pending/` is
    /// missing or has no segments — there is nothing for the worker to
    /// do, so the dispatch is skipped.
    ///
    /// All actual segment reads, partitioning, and the rewrite happen in
    /// [`crate::actor::execute_sweep`]. The apply phase
    /// [`Self::apply_sweep_result`] does the conditional extent-index
    /// updates and candidate deletion.
    pub fn prepare_sweep(&self) -> io::Result<Option<SweepJob>> {
        let pending_dir = self.base_dir.join("pending");
        let segs = match segment::collect_segment_files(&pending_dir) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if segs.is_empty() {
            return Ok(None);
        }
        let floor = latest_snapshot(&self.base_dir)?;
        Ok(Some(SweepJob {
            lbamap: Arc::clone(&self.lbamap),
            floor,
            pending_dir,
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
        }))
    }

    /// Apply phase of `sweep_pending` — runs on the actor thread after
    /// the worker returns.
    ///
    /// Drops dead Data/Inline entries from the extent index using
    /// [`extentindex::ExtentIndex::remove_if_matches`] so a concurrent
    /// writer that re-pointed the hash at a newer segment is left
    /// untouched. Re-points carried-live entries with
    /// [`extentindex::ExtentIndex::replace_if_matches`] for the same
    /// reason — a CAS miss simply means the new write wins (apply does
    /// not insert, leaving the body unreferenced until the next sweep,
    /// which is the conservative direction).
    ///
    /// File-cache eviction and candidate deletion run last. The
    /// candidate whose ULID equals `new_ulid` was already replaced
    /// atomically by the worker's `tmp` → final rename; apply skips its
    /// `remove_file` while still evicting the cached fd (the old inode
    /// was unlinked and a surviving handle would seek into stale
    /// offsets after the publish).
    pub fn apply_sweep_result(&mut self, result: SweepResult) -> io::Result<CompactionStats> {
        let SweepResult {
            mut stats,
            new_ulid,
            new_body_section_start,
            merged_live,
            dead_entries,
            candidate_paths,
        } = result;

        for dead in &dead_entries {
            if Arc::make_mut(&mut self.extent_index).remove_if_matches(
                &dead.hash,
                dead.source_segment_id,
                dead.source_body_offset,
            ) {
                stats.extents_removed += 1;
            }
        }

        if let Some(new_ulid) = new_ulid {
            for live in &merged_live {
                let entry = &live.entry;
                let inline_data = match entry.kind {
                    EntryKind::Data | EntryKind::CanonicalData => None,
                    EntryKind::Inline | EntryKind::CanonicalInline => {
                        entry.data.clone().map(Vec::into_boxed_slice)
                    }
                    EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
                };
                Arc::make_mut(&mut self.extent_index).replace_if_matches(
                    entry.hash,
                    live.source_segment_id,
                    live.source_body_offset,
                    extentindex::ExtentLocation {
                        segment_id: new_ulid,
                        body_offset: entry.stored_offset,
                        body_length: entry.stored_length,
                        compressed: entry.compressed,
                        body_source: BodySource::Local,
                        body_section_start: new_body_section_start,
                        inline_data,
                    },
                );
            }
        }

        for seg_path in &candidate_paths {
            let seg_ulid_opt = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| Ulid::from_string(s).ok());
            if let Some(ulid) = seg_ulid_opt {
                self.file_cache.borrow_mut().evict(ulid);
            }
            if seg_ulid_opt == new_ulid {
                // Already replaced atomically by the worker's rename;
                // re-deleting would unlink the new content.
                continue;
            }
            match fs::remove_file(seg_path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }

        Ok(stats)
    }

    /// Inline, test-only variant of the GC checkpoint.
    ///
    /// Mints the two ULIDs (`u_gc < u_flush`) and flushes the current WAL
    /// to `pending/<u_flush>` synchronously on the caller's thread.
    /// Returns `u_gc`.  The post-flush WAL is left unopened — the next
    /// write lazily opens a fresh one (see `Volume::wal`).
    ///
    /// **Production uses [`Volume::prepare_gc_checkpoint`] instead**, which
    /// splits mint+rotate (actor thread) from the old-WAL fsync (worker
    /// thread) so writes aren't blocked.  This method keeps both on one
    /// thread for tests that want a synchronous checkpoint without spinning
    /// up the actor machinery.  See [`GcCheckpointUlids`] for why both
    /// ULIDs are minted before any I/O.
    pub fn gc_checkpoint_for_test(&mut self) -> io::Result<Ulid> {
        let GcCheckpointUlids { u_gc, u_flush } = self.mint_gc_checkpoint_ulids();
        // Flush the current WAL to pending/ under u_flush. If the WAL is
        // empty (or absent), the file is deleted/skipped and u_flush is unused.
        self.flush_wal_to_pending_as(u_flush)?;
        Ok(u_gc)
    }

    /// Mint the two ULIDs for a GC checkpoint, in ordering-invariant order.
    ///
    /// See [`GcCheckpointUlids`] for the ordering invariant and why both
    /// are minted before any I/O.
    fn mint_gc_checkpoint_ulids(&mut self) -> GcCheckpointUlids {
        GcCheckpointUlids {
            u_gc: self.mint.next(),
            u_flush: self.mint.next(),
        }
    }

    /// Apply staged GC handoff files written by the coordinator.
    ///
    /// Under the self-describing handoff protocol, the coordinator writes the
    /// compacted segment to `gc/<new-ulid>.staged` (signed with an ephemeral
    /// key; the `inputs` list is embedded in the segment header). This method
    /// walks `gc/` for `.staged` entries, reads each segment's `inputs`, diffs
    /// those inputs' `.idx` files against the new segment's entries to build
    /// the extent-index updates, re-signs the body with the volume key, and
    /// renames `<ulid>.tmp → <ulid>` (the bare name — an atomic commit point
    /// meaning "volume-applied, awaiting coordinator upload"), then removes
    /// `<ulid>.staged`. The coordinator subsequently uploads the segment to
    /// S3 and sends a `promote <new-ulid>` IPC; `promote_segment` writes
    /// `index/<new-ulid>.idx` and `cache/<new-ulid>.{body,present}` and
    /// deletes `index/<old>.idx` for each consumed input. Input
    /// `cache/<input>.{body,present}` files are deleted by the coordinator
    /// during `apply_done_handoffs`, not here.
    ///
    /// This two-phase approach preserves the invariant: **`index/<ulid>.idx`
    /// present ↔ segment confirmed in S3**. The idx is never written before the
    /// coordinator confirms the upload, so a segment in `gc/` or `pending/` with
    /// no idx is never mistaken for an S3-confirmed segment.
    ///
    /// Returns the number of handoff files processed. Returns `Ok(0)` if
    /// the `gc/` directory does not exist yet.
    pub fn apply_gc_handoffs(&mut self) -> io::Result<usize> {
        let gc_dir = self.base_dir.join("gc");
        self.apply_all_staged_handoffs(&gc_dir)
    }

    /// Scan `gc/` for plan handoff files that need processing.
    ///
    /// Sweeps stale volume-owned `<ulid>.tmp` scratch files, applies bare-wins
    /// shortcuts for `.plan` + bare co-presence, and returns a list of
    /// `(plan_path, new_ulid)` pairs for the caller to dispatch to the worker.
    /// Also returns a count of handoffs that were already applied (bare wins).
    ///
    /// Coordinator-owned `<ulid>.plan.tmp` scratch is left alone — the coord
    /// may be actively writing it; the coord sweeps its own stale scratch at
    /// the start of each GC pass.
    pub fn scan_plan_handoffs(&self) -> io::Result<(Vec<(PathBuf, Ulid)>, usize)> {
        let gc_dir = self.base_dir.join("gc");
        if !gc_dir.try_exists()? {
            return Ok((Vec::new(), 0));
        }

        // Pass 1: sweep stale volume-owned `<ulid>.tmp` scratch files.
        for entry in fs::read_dir(&gc_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(stem) = name.strip_suffix(".tmp") else {
                continue;
            };
            if Ulid::from_string(stem).is_ok() {
                let _ = fs::remove_file(entry.path());
            }
        }

        // Pass 2: collect `.plan` files.
        let mut plans: Vec<(String, Ulid)> = fs::read_dir(&gc_dir)?
            .filter_map(|e| {
                let e = e.ok()?;
                let name = e.file_name().into_string().ok()?;
                let stem = name.strip_suffix(".plan")?;
                let ulid = Ulid::from_string(stem).ok()?;
                Some((name, ulid))
            })
            .collect();
        plans.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut to_process = Vec::new();
        let mut already_applied = 0usize;
        for (plan_name, new_ulid) in plans {
            let plan_path = gc_dir.join(&plan_name);
            let bare_path = gc_dir.join(new_ulid.to_string());

            // Crash recovery: `.plan` + bare → bare wins, drop `.plan`.
            if bare_path.try_exists()? {
                let _ = fs::remove_file(&plan_path);
                already_applied += 1;
                continue;
            }

            to_process.push((plan_path, new_ulid));
        }

        Ok((to_process, already_applied))
    }

    /// Walk `gc/` for `.plan` entries and apply each via the
    /// self-describing derive-at-apply path.
    ///
    /// Also handles crash-recovery filename states:
    /// - `<ulid>.tmp` — volume-owned apply scratch from a crashed write.
    ///   Remove on sight. Coordinator-owned `<ulid>.staged.tmp` scratch
    ///   is left alone (the coord may be actively writing it; the coord
    ///   cleans its own stale scratch at the start of each GC pass).
    /// - `<ulid>.staged` alone — apply normally.
    /// - `<ulid>.staged` + bare `<ulid>` — bare wins (previous apply
    ///   committed before cleanup); remove the `.staged`.
    /// - bare `<ulid>` alone — already applied, no action.
    fn apply_all_staged_handoffs(&mut self, gc_dir: &Path) -> io::Result<usize> {
        if !gc_dir.try_exists()? {
            return Ok(0);
        }

        // Pass 1: sweep stale volume-owned `<ulid>.tmp` scratch files
        // (incomplete apply writes). The suffix must be exactly `.tmp`
        // on a valid Ulid stem — this deliberately excludes the
        // coordinator's `<ulid>.staged.tmp` compaction scratch, which
        // the coord may still be writing in a concurrent tick. Deleting
        // it here would race `tokio::fs::rename` to ENOENT and fail the
        // compaction handoff.
        for entry in fs::read_dir(gc_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(stem) = name.strip_suffix(".tmp") else {
                continue;
            };
            if Ulid::from_string(stem).is_ok() {
                let _ = fs::remove_file(entry.path());
            }
        }

        // Pass 2: collect `.plan` files emitted by the coordinator. See
        // docs/design-gc-plan-handoff.md for the protocol.
        let mut plans: Vec<(String, Ulid)> = fs::read_dir(gc_dir)?
            .filter_map(|e| {
                let e = e.ok()?;
                let name = e.file_name().into_string().ok()?;
                let stem = name.strip_suffix(".plan")?;
                let ulid = Ulid::from_string(stem).ok()?;
                Some((name, ulid))
            })
            .collect();
        plans.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut count = 0usize;
        for (name, new_ulid) in plans {
            let plan_path = gc_dir.join(&name);
            let bare_path = gc_dir.join(new_ulid.to_string());

            // Crash recovery: plan + bare → bare wins, drop plan.
            if bare_path.try_exists()? {
                let _ = fs::remove_file(&plan_path);
                count += 1;
                continue;
            }

            match self.apply_plan_handoff(gc_dir, &plan_path, new_ulid)? {
                StagedApply::Applied => count += 1,
                StagedApply::Cancelled => {
                    // Cancel removes the handoff input file inside.
                }
            }
        }

        Ok(count)
    }

    /// Prep phase of GC plan application.
    ///
    /// Reads and validates `<ulid>.plan`, builds a [`GcPlanApplyJob`] the
    /// worker can materialise off-actor. Returns `Ok(None)` when the plan is
    /// rejected up front (parse failure, ULID mismatch, empty inputs) — the
    /// plan file is removed inside, and the caller treats it as a cancelled
    /// handoff.
    pub fn prepare_plan_apply(
        &self,
        plan_path: PathBuf,
        new_ulid: Ulid,
    ) -> io::Result<Option<GcPlanApplyJob>> {
        let plan = match gc_plan::GcPlan::read(&plan_path) {
            Ok(p) => p,
            Err(e) => {
                log::warn!(
                    "plan {new_ulid}: parse failed ({e}); removing {}",
                    plan_path.display()
                );
                let _ = fs::remove_file(&plan_path);
                return Ok(None);
            }
        };
        if plan.new_ulid != new_ulid {
            log::warn!(
                "plan ulid mismatch: filename={new_ulid} plan={}; removing",
                plan.new_ulid
            );
            let _ = fs::remove_file(&plan_path);
            return Ok(None);
        }
        if plan.inputs().is_empty() {
            log::warn!("plan {new_ulid} has no inputs; removing");
            let _ = fs::remove_file(&plan_path);
            return Ok(None);
        }

        let gc_dir = self.base_dir.join("gc");
        let index_dir = self.base_dir.join("index");
        Ok(Some(GcPlanApplyJob {
            plan_path,
            new_ulid,
            gc_dir,
            index_dir,
            base_dir: self.base_dir.clone(),
            ancestor_layers: self.ancestor_layers.clone(),
            fetcher: self.fetcher.clone(),
            extent_index: Arc::clone(&self.extent_index),
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            plan,
        }))
    }

    /// Apply phase for a [`GcPlanApplyResult`] returned by the worker.
    ///
    /// Re-derives the to-remove and stale-cancel sets against the **current**
    /// extent index and lbamap (which may have diverged while the worker was
    /// running), updates the extent index for carried entries, then commits
    /// via `rename(<tmp>, <bare>)` as the atomic commit point. Cancelled
    /// materialisations skip the commit — the plan was already removed by
    /// the worker and any stale `.tmp` will be swept on the next apply tick.
    pub fn apply_plan_apply_result(
        &mut self,
        result: GcPlanApplyResult,
    ) -> io::Result<StagedApply> {
        // Cancelled in the worker: plan file already removed; any stale
        // `.tmp` is cleaned up on the next apply pass by the sweep at the
        // top of `apply_all_staged_handoffs`. Nothing more to do here.
        if matches!(result.outcome, StagedApply::Cancelled) {
            return Ok(StagedApply::Cancelled);
        }

        let GcPlanApplyResult {
            new_ulid,
            plan_path,
            gc_dir,
            tmp_path,
            new_bss,
            entries,
            inputs,
            input_old_entries,
            handoff_inline,
            outcome: _,
        } = result;
        let tmp_path = match tmp_path {
            Some(p) => p,
            None => return Ok(StagedApply::Cancelled),
        };

        let live = self.lbamap.lba_referenced_hashes();
        let carried_hashes: std::collections::HashSet<blake3::Hash> = entries
            .iter()
            .filter(|e| e.kind != EntryKind::DedupRef)
            .map(|e| e.hash)
            .collect();

        let mut to_remove: Vec<(blake3::Hash, Ulid)> = Vec::new();
        let mut stale_cancel: Vec<(blake3::Hash, Ulid)> = Vec::new();
        for (hash, _kind, input_ulid) in &input_old_entries {
            let still_at_input = self
                .extent_index
                .lookup(hash)
                .is_some_and(|loc| loc.segment_id == *input_ulid);
            if !still_at_input {
                continue;
            }
            if carried_hashes.contains(hash) {
                continue;
            }
            if live.contains(hash) {
                stale_cancel.push((*hash, *input_ulid));
            }
            to_remove.push((*hash, *input_ulid));
        }

        if !stale_cancel.is_empty() {
            log::warn!(
                "plan {new_ulid}: stale-liveness cancellation — {} hash(es) live in \
                 volume but absent from materialised output; removing plan [{}]",
                stale_cancel.len(),
                describe_stale_cancel(&stale_cancel, &self.lbamap),
            );
            diagnose_stale_cancel_legacy(
                &self.base_dir,
                &self.ancestor_layers,
                &self.lbamap,
                &stale_cancel,
                &self.base_dir.join("index"),
            );
            let _ = fs::remove_file(&tmp_path);
            let _ = fs::remove_file(&plan_path);
            // Refresh `self.lbamap` from disk before returning. Without
            // this, a legitimate cancel here pins the in-memory view to
            // the pre-supersede state: if another GC output has already
            // committed `disk[L] = H_new` while our snapshot still has
            // `self.lbamap[L] = H_old`, the next pass re-reads `H_old`
            // as live and fires the same cancel again — forever. Reads
            // through `self.lbamap` would also keep returning `H_old`'s
            // body even after disk moved on. Refreshing after the cancel
            // lets the next pass see the current truth (one wasted pass,
            // then convergence).
            self.rebuild_lbamap_from_disk()?;
            return Ok(StagedApply::Cancelled);
        }

        for (i, e) in entries.iter().enumerate() {
            if e.kind == EntryKind::DedupRef {
                continue;
            }
            let current = self.extent_index.lookup(&e.hash);
            let should_update = match current {
                None => true,
                Some(loc) => inputs.contains(&loc.segment_id),
            };
            if !should_update {
                continue;
            }
            let idata = if matches!(e.kind, EntryKind::Inline | EntryKind::CanonicalInline) {
                let start = e.stored_offset as usize;
                let end = start + e.stored_length as usize;
                if end <= handoff_inline.len() {
                    Some(handoff_inline[start..end].into())
                } else {
                    continue;
                }
            } else {
                None
            };
            Arc::make_mut(&mut self.extent_index).insert(
                e.hash,
                extentindex::ExtentLocation {
                    segment_id: new_ulid,
                    body_offset: e.stored_offset,
                    body_length: e.stored_length,
                    compressed: e.compressed,
                    body_source: BodySource::Cached(i as u32),
                    body_section_start: new_bss,
                    inline_data: idata,
                },
            );
        }

        for (hash, old_ulid) in &to_remove {
            if self
                .extent_index
                .lookup(hash)
                .is_some_and(|loc| loc.segment_id == *old_ulid)
            {
                Arc::make_mut(&mut self.extent_index).remove(hash);
            }
        }

        let pending_dir = self.base_dir.join("pending");
        for input in &inputs {
            let _ = fs::remove_file(pending_dir.join(input.to_string()));
        }

        let bare_path = gc_dir.join(new_ulid.to_string());
        fs::rename(&tmp_path, &bare_path)?;
        let _ = fs::remove_file(&plan_path);

        // Rebuild self.lbamap from disk so the in-memory view reflects the
        // post-commit state, including the sub-run hashes the output segment
        // just introduced. Without this step a partial-death sub-run leaves
        // the old composite hash stale in the in-memory lbamap, which then
        // trips the stale-liveness check on the next GC pass (the input
        // entry's "still live" status is evaluated against a hash no longer
        // backed by any on-disk writer), cancelling apply and stalling GC.
        //
        // Crash ordering: on a crash between rename and rebuild, restart
        // re-runs `Volume::open`'s rebuild which produces the same result.
        self.rebuild_lbamap_from_disk()?;

        Ok(StagedApply::Applied)
    }

    /// Rebuild `self.lbamap` from disk (segments across fork + ancestors)
    /// and replay the current WAL tail on top, mirroring the initial
    /// construction in `Volume::open`. Used by the GC apply path on both
    /// the stale-cancel and commit branches to keep the in-memory view
    /// from drifting relative to on-disk state.
    fn rebuild_lbamap_from_disk(&mut self) -> io::Result<()> {
        let mut chain: Vec<(PathBuf, Option<String>)> = self
            .ancestor_layers
            .iter()
            .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
            .collect();
        chain.push((self.base_dir.clone(), None));
        let mut fresh = lbamap::rebuild_segments(&chain)?;
        let wal_dir = self.base_dir.join("wal");
        if let Ok(entries) = fs::read_dir(&wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    continue;
                }
                let Ok((records, _)) = writelog::scan_readonly(&path) else {
                    continue;
                };
                for record in records {
                    match record {
                        writelog::LogRecord::Data {
                            hash,
                            start_lba,
                            lba_length,
                            ..
                        }
                        | writelog::LogRecord::Ref {
                            hash,
                            start_lba,
                            lba_length,
                        } => {
                            fresh.insert(start_lba, lba_length, hash);
                        }
                        writelog::LogRecord::Zero {
                            start_lba,
                            lba_length,
                        } => {
                            fresh.insert(start_lba, lba_length, ZERO_HASH);
                        }
                    }
                }
            }
        }
        self.lbamap = Arc::new(fresh);
        Ok(())
    }

    /// Synchronous single-shot variant of the plan apply path — runs prep,
    /// execute, and apply inline on the current thread. Used by tests and
    /// any caller that doesn't have an actor behind a worker thread.
    fn apply_plan_handoff(
        &mut self,
        _gc_dir: &Path,
        plan_path: &Path,
        new_ulid: Ulid,
    ) -> io::Result<StagedApply> {
        let job = match self.prepare_plan_apply(plan_path.to_path_buf(), new_ulid)? {
            Some(j) => j,
            None => return Ok(StagedApply::Cancelled),
        };
        let result = crate::actor::execute_gc_plan_apply(job)?;
        self.apply_plan_apply_result(result)
    }

    /// Promote a segment to the local cache after confirmed S3 upload.
    ///
    /// Called in response to the coordinator's `promote <ulid>` IPC, which is
    /// sent only after a confirmed S3 upload.
    ///
    /// Writes `index/<ulid>.idx` first (restoring the invariant that idx presence
    /// ↔ segment confirmed in S3), then `cache/<ulid>.body` and
    /// `cache/<ulid>.present`.
    ///
    /// **Drain path** (`pending/<ulid>` exists): also deletes `pending/<ulid>`.
    /// The coordinator never deletes `pending/` directly.
    ///
    /// **GC path** (bare `gc/<ulid>` exists): also deletes `index/<old>.idx` for
    /// each segment consumed by the GC handoff (read from the bare `gc/<ulid>`
    /// segment header's `inputs` field). This happens after writing the new idx
    /// so there is never a window where no idx covers the affected LBAs.  The
    /// `gc/<ulid>` body file is also deleted here — it has already been copied
    /// into `cache/<ulid>.body`, and deleting it inside the actor (rather than
    /// from the coordinator) keeps every mutation of `gc/` serialised with the
    /// idle-tick `apply_gc_handoffs` path.
    ///
    /// Idempotent: if `cache/<ulid>.body` already exists and no source
    /// remains in `pending/` or `gc/` the function returns `Ok(())` without
    /// re-writing.
    pub fn promote_segment(&mut self, ulid: Ulid) -> io::Result<()> {
        let job = match self.prepare_promote_segment(ulid)? {
            PromoteSegmentPrep::AlreadyPromoted => return Ok(()),
            PromoteSegmentPrep::Job(job) => *job,
        };
        let result = crate::actor::execute_promote_segment(job)?;
        self.apply_promote_segment_result(result)
    }

    /// Prep phase of `promote_segment`. Pure function of the on-disk
    /// layout — runs on the actor thread in microseconds.
    ///
    /// Selects the source segment (`pending/<ulid>` > `gc/<ulid>` >
    /// body-exists early-return) and builds a [`PromoteSegmentJob`] for
    /// the worker. The source-preference ordering is load-bearing: if a
    /// previous promote committed its idx/body but crashed before the
    /// apply phase, `pending/<ulid>` (or `gc/<ulid>`) will still exist
    /// and the retry must take the full path, not the idempotent
    /// early-return. See `promote_segment_recovers_mid_apply_crash`
    /// regression test.
    ///
    /// Ensures `index/` and `cache/` exist so the worker never touches
    /// the directory structure.
    pub fn prepare_promote_segment(&self, ulid: Ulid) -> io::Result<PromoteSegmentPrep> {
        let ulid_str = ulid.to_string();
        let cache_dir = self.base_dir.join("cache");
        let body_path = cache_dir.join(format!("{ulid_str}.body"));
        let present_path = cache_dir.join(format!("{ulid_str}.present"));
        let pending_path = self.base_dir.join("pending").join(&ulid_str);
        let gc_path = self.base_dir.join("gc").join(&ulid_str);
        let index_dir = self.base_dir.join("index");
        let idx_path = index_dir.join(format!("{ulid_str}.idx"));

        let (src_path, is_drain) = if pending_path.try_exists()? {
            (pending_path, true)
        } else if gc_path.try_exists()? {
            (gc_path, false)
        } else if body_path.try_exists()? {
            return Ok(PromoteSegmentPrep::AlreadyPromoted);
        } else {
            return Err(io::Error::other(format!(
                "promote {ulid_str}: segment not found in pending/ or gc/"
            )));
        };

        fs::create_dir_all(&index_dir)?;
        fs::create_dir_all(&cache_dir)?;

        Ok(PromoteSegmentPrep::Job(Box::new(PromoteSegmentJob {
            ulid,
            src_path,
            is_drain,
            body_path,
            present_path,
            idx_path,
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
        })))
    }

    /// Apply phase of `promote_segment`. Consumes the worker's result.
    ///
    /// Drain path: transitions extent-index entries from
    /// `BodySource::Local` (pointing at `pending/<ulid>`) to
    /// `BodySource::Cached(n)` (pointing at the new `cache/<ulid>.body`).
    /// The CAS check (`segment_id == ulid`) makes the rewrite a no-op for
    /// any entry a concurrent write has already superseded. Then evicts
    /// the segment's cached fd, deletes the delta sidecar if present,
    /// and deletes `pending/<ulid>`.
    ///
    /// GC tombstone path: deletes `index/<old>.idx` for every consumed
    /// input. No extent-index updates (tombstones carry no entries).
    ///
    /// GC carried path: same as tombstone plus the extent-index state
    /// stays untouched — the `apply_gc_handoffs` step already rewrote
    /// the extent index to `BodySource::Cached` against the fresh ULID.
    pub fn apply_promote_segment_result(&mut self, result: PromoteSegmentResult) -> io::Result<()> {
        let PromoteSegmentResult {
            ulid,
            is_drain,
            body_section_start,
            entries,
            inputs,
            inline,
            tombstone,
        } = result;
        let index_dir = self.base_dir.join("index");

        if tombstone {
            for old_ulid in &inputs {
                let _ = fs::remove_file(index_dir.join(format!("{old_ulid}.idx")));
            }
            return Ok(());
        }

        if is_drain {
            // Evict before the CAS so readers arriving post-publish
            // open the new cache body, not a stale handle to the
            // soon-to-be-deleted pending file.
            self.evict_cached_segment(ulid);

            for (i, entry) in entries.iter().enumerate() {
                if !entry.kind.has_body_bytes() {
                    continue;
                }
                if self
                    .extent_index
                    .lookup(&entry.hash)
                    .is_some_and(|loc| loc.segment_id == ulid)
                {
                    let idata = if entry.kind.is_inline() {
                        let start = entry.stored_offset as usize;
                        let end = start + entry.stored_length as usize;
                        if end <= inline.len() {
                            Some(inline[start..end].into())
                        } else {
                            continue;
                        }
                    } else {
                        None
                    };
                    Arc::make_mut(&mut self.extent_index).insert(
                        entry.hash,
                        extentindex::ExtentLocation {
                            segment_id: ulid,
                            body_offset: entry.stored_offset,
                            body_length: entry.stored_length,
                            compressed: entry.compressed,
                            body_source: BodySource::Cached(i as u32),
                            body_section_start,
                            inline_data: idata,
                        },
                    );
                }
            }

            // Delta entries: the delta blob has moved from inline in
            // the now-deleted pending file to the standalone
            // `cache/<ulid>.delta` sidecar, so flip
            // `DeltaBodySource::Full → Cached`. CAS against
            // `segment_id == ulid` so a concurrent delta-repack or
            // reclaim that re-pointed the hash at a newer segment
            // wins.
            for entry in entries.iter() {
                if entry.kind != EntryKind::Delta {
                    continue;
                }
                Arc::make_mut(&mut self.extent_index)
                    .flip_delta_body_source_to_cached_if_matches(&entry.hash, ulid);
            }

            let ulid_str = ulid.to_string();
            let delta_path = self
                .base_dir
                .join("pending")
                .join(format!("{ulid_str}.delta"));
            let _ = fs::remove_file(&delta_path);
            let pending_path = self.base_dir.join("pending").join(&ulid_str);
            fs::remove_file(&pending_path)?;
        } else {
            // GC carried path: delete each consumed input's idx.
            for old_ulid in &inputs {
                let _ = fs::remove_file(index_dir.join(format!("{old_ulid}.idx")));
            }
        }
        Ok(())
    }

    /// Finalize a completed GC handoff by deleting the bare `gc/<ulid>` file.
    ///
    /// Called by the coordinator after the new segment has been uploaded to
    /// S3, `promote_segment` has moved it into the local cache, and the old
    /// segments have been deleted from S3. This is the last step in the
    /// handoff lifecycle and must happen AFTER the S3 delete so that a crash
    /// between the two cannot leak old-segment objects in S3 — the bare file's
    /// presence keeps `apply_done_handoffs` eligible to retry the delete, and
    /// only removing the bare file removes that eligibility.
    ///
    /// Routing through the actor (rather than letting the coordinator unlink
    /// `gc/<ulid>` directly) keeps every mutation of `gc/` serialised with the
    /// idle-tick `apply_gc_handoffs` path, so there is no race between the
    /// coordinator removing a file and the actor reading it.
    pub fn finalize_gc_handoff(&mut self, ulid: Ulid) -> io::Result<()> {
        let gc_dir = self.base_dir.join("gc");
        let bare = gc_dir.join(ulid.to_string());
        match fs::remove_file(&bare) {
            Ok(()) => {}
            // Idempotent: already removed by a previous finalize or a
            // promote that ran before we flipped the protocol.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // Best-effort cleanup of any stray `.plan` sibling left over from
        // a crash between the bare rename and `.plan` removal.
        let _ = fs::remove_file(gc_dir.join(format!("{ulid}.plan")));
        Ok(())
    }

    /// Drop hash-dead DATA entries from `pending/<ulid>`, so that deleted
    /// data never leaves the local host via S3 upload. Called by the
    /// coordinator just before the segment is read for upload.
    ///
    /// A DATA entry is **hash-dead** when:
    /// - Its LBA no longer maps to this hash (LBA-dead), and
    /// - No other live LBA in the volume references this hash.
    ///
    /// Such an entry has no readers (by construction) and its bytes are
    /// safe to drop. LBA-dead-but-hash-alive entries are kept — their
    /// bytes still back a DedupRef or Delta source somewhere. DedupRef,
    /// Zero, Inline, Canonical*, and Delta entries are kept untouched.
    ///
    /// Implementation: the pending segment is rewritten via tmp+rename
    /// with the dropped entries removed from the index and their body
    /// bytes excluded. Removing the entries (rather than only
    /// hole-punching their bodies) is required for correctness: on
    /// crash+rebuild, `extent_index::rebuild` walks segments in ULID
    /// ascending order and takes the first insert per hash; a stale
    /// hash-dead entry surviving in a lower-ULID .idx would shadow the
    /// same hash's live body in a later segment.
    ///
    /// Idempotent: a second call is a no-op because the first call
    /// already dropped every hash-dead entry.
    ///
    /// Fast path: if no hash-dead DATA entries exist, the function
    /// returns without rewriting.
    pub fn redact_segment(&mut self, ulid: Ulid) -> io::Result<()> {
        let ulid_str = ulid.to_string();
        let pending_dir = self.base_dir.join("pending");
        let seg_path = pending_dir.join(&ulid_str);

        let (body_section_start, entries, inputs) =
            segment::read_and_verify_segment_index(&seg_path, &self.verifying_key)?;

        let live_hashes = self.lbamap.lba_referenced_hashes();
        let is_hash_dead = |entry: &segment::SegmentEntry| -> bool {
            if !entry.kind.is_data() || entry.stored_length == 0 {
                return false;
            }
            let lba_live = self.lbamap.hash_at(entry.start_lba) == Some(entry.hash);
            !lba_live && !live_hashes.contains(&entry.hash)
        };

        if !entries.iter().any(&is_hash_dead) {
            return Ok(());
        }

        // Partition: dropped hashes (for extent-index cleanup) vs
        // surviving entries (rewritten into the new segment).
        let (mut kept_entries, dropped_entries): (Vec<_>, Vec<_>) =
            entries.into_iter().partition(|e| !is_hash_dead(e));
        let dropped_count = dropped_entries.len();
        let dropped_bytes: u64 = dropped_entries.iter().map(|e| e.stored_length as u64).sum();
        let dropped_hashes: Vec<blake3::Hash> =
            dropped_entries.into_iter().map(|e| e.hash).collect();

        // Load live body bytes + inline section + delta body verbatim.
        let inline_bytes = segment::read_inline_section(&seg_path)?;
        segment::read_extent_bodies(
            &seg_path,
            body_section_start,
            &mut kept_entries,
            &inline_bytes,
        )?;
        let delta_body = segment::read_delta_body_section(&seg_path)?;

        // Rewrite via tmp+rename. `write_segment_full` reassigns
        // `stored_offset` for surviving entries based on the compacted
        // body layout, so new offsets may shift downward.
        let tmp_path = pending_dir.join(format!("{ulid_str}.tmp"));
        // `create_new` inside write_segment_full requires no stale .tmp.
        let _ = fs::remove_file(&tmp_path);
        segment::write_segment_full(
            &tmp_path,
            &mut kept_entries,
            &delta_body,
            &inputs,
            self.signer.as_ref(),
        )?;
        fs::rename(&tmp_path, &seg_path)?;
        segment::fsync_dir(&seg_path)?;

        // If a previous drain crashed mid-promote, this segment may
        // have sibling `index/<u>.idx` + `cache/<u>.{body,present,delta}`
        // files left over from the pre-redact layout. Those now point
        // at stale body offsets — `promote_segment`'s idempotence guard
        // would keep them. Remove them so the following promote rewrites
        // them from the freshly-redacted pending body.
        crate::actor::invalidate_promote_siblings(
            &self.base_dir.join("index"),
            &self.base_dir.join("cache"),
            ulid,
        )?;

        // Body offsets and the file's inode contents both changed — any
        // cached fd for this segment must be dropped.
        self.evict_cached_segment(ulid);

        // Clear extent-index entries for dropped hashes if they still
        // point at this segment. A later GC/repack may have already
        // moved the canonical location elsewhere; leave those alone.
        //
        // Without this, the dedup write shortcut (`write_commit`) would
        // see a surviving extent-index entry for the dropped hash and
        // emit a thin DedupRef whose canonical body is gone.
        if !dropped_hashes.is_empty() {
            let index = Arc::make_mut(&mut self.extent_index);
            for hash in &dropped_hashes {
                if index.lookup(hash).is_some_and(|loc| loc.segment_id == ulid) {
                    index.remove(hash);
                }
            }
        }

        log::info!(
            "redact {ulid_str}: dropped {dropped_count} hash-dead DATA entries ({dropped_bytes} bytes)"
        );

        Ok(())
    }

    /// Flush the current WAL to a segment in this node's `pending/`, update
    /// the extent index, and clear `pending_entries`. The WAL file is deleted.
    ///
    /// If `pending_entries` is empty (nothing written since last flush), the
    /// WAL file is deleted directly without writing a segment.
    ///
    /// Evict `segment_id` from the file handle cache.
    ///
    /// The read path (`read_extents`) maintains an LRU cache of open segment
    /// fds keyed by segment ULID, with a `SegmentLayout` that controls how
    /// body offsets are computed (`BodyOnly` files start at offset 0; `Full`
    /// segment files add `body_section_start`).
    ///
    /// Callers must evict whenever a segment's on-disk representation changes
    /// in a way that invalidates the cached fd or layout:
    ///
    /// - **`flush_wal_to_pending`** — WAL file deleted, replaced by a
    ///   pending segment with a different byte layout.
    /// - **`promote_segment`** (drain path) — `pending/<ulid>` deleted,
    ///   replaced by `cache/<ulid>.body` (body-section-relative offsets),
    ///   so `body_section_start` changes from the full-segment value to 0.
    /// - **`apply_gc_handoffs`** (repack) — old segment deleted and
    ///   replaced by a denser segment with reassigned body offsets.
    ///
    /// Without eviction the cached fd silently serves stale data or — worse —
    /// applies `body_section_start` from the new extent index entry against
    /// the old file layout, seeking past the body section.
    fn evict_cached_segment(&self, segment_id: Ulid) {
        self.file_cache.borrow_mut().evict(segment_id);
    }

    /// Flush the current WAL to a fresh `pending/<segment_ulid>` and leave
    /// the volume in a no-WAL state. The next write lazily opens a new WAL.
    fn flush_wal_to_pending(&mut self) -> io::Result<()> {
        // Mint a fresh segment ULID distinct from the old WAL's ULID so
        // `wal/<old_wal_ulid>` and `pending/<segment_ulid>` never collide
        // on the same path. With a shared ULID, a stale cold-cache reader
        // that loaded the pre-promote snapshot could look up the old WAL
        // ULID, fall through to `pending/<same_ulid>`, and read WAL-relative
        // offsets as if they were segment-relative — silent wrong bytes.
        // A distinct segment ULID turns that cold-cache race into NotFound.
        //
        // Wastes one mint when the WAL is empty or absent (the early return
        // below skips the segment write). The mint is cheap and monotonic;
        // the extra advance is harmless.
        let segment_ulid = self.mint.next();
        self.flush_wal_to_pending_as(segment_ulid)
    }

    /// Like `flush_wal_to_pending`, but uses the caller-provided `segment_ulid`
    /// rather than minting a fresh one.
    ///
    /// Used by `gc_checkpoint` to give the flushed WAL segment a ULID that has
    /// been pre-minted above the GC output ULIDs, so that the pending segment
    /// sorts correctly above GC outputs on crash-recovery rebuild.
    ///
    /// The WAL file itself retains its original name (the WAL ULID) — only the
    /// output segment in `pending/` receives `segment_ulid`.
    ///
    /// Leaves `self.wal = None` on success — the next write lazily opens a
    /// fresh WAL. No-op when no WAL is currently open.
    fn flush_wal_to_pending_as(&mut self, segment_ulid: Ulid) -> io::Result<()> {
        let Some(mut open) = self.wal.take() else {
            return Ok(());
        };
        open.wal.fsync()?;
        if self.pending_entries.is_empty() {
            fs::remove_file(&open.path)?;
            return Ok(());
        }
        self.has_new_segments = true;
        self.last_segment_ulid = Some(segment_ulid);
        // Snapshot the WAL-relative body offsets for every Data/Inline entry
        // before `segment::write_and_commit` rewrites `stored_offset` to
        // segment-relative. These become the CAS precondition tokens in the
        // apply loop below: we only rewrite an extent index entry if it still
        // points at (wal_ulid, original_wal_offset). Any later writer or GC
        // handoff that has already superseded the entry leaves the CAS failing
        // and its placement intact.
        //
        // Today the promote runs on the actor thread, so no concurrent writer
        // can interpose between snapshot and apply — the CAS always succeeds.
        // The machinery is wired in now so the upcoming off-actor apply phase
        // inherits the correct precondition check.
        let old_wal_ulid = open.ulid;
        let old_wal_path = open.path;
        let pre_promote_offsets: Vec<Option<u64>> = self
            .pending_entries
            .iter()
            .map(|e| match e.kind {
                EntryKind::Data
                | EntryKind::Inline
                | EntryKind::CanonicalData
                | EntryKind::CanonicalInline => {
                    self.extent_index.lookup(&e.hash).map(|loc| loc.body_offset)
                }
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => None,
            })
            .collect();
        let body_section_start = segment::write_and_commit(
            &self.base_dir.join("pending"),
            segment_ulid,
            &mut self.pending_entries,
            self.signer.as_ref(),
        )?;
        // Update the extent index: replace temporary WAL offsets with
        // body-relative offsets into the committed segment file.
        // Thin DedupRef entries have no body in this segment — the extent
        // index already points to the canonical segment. Zero extents are
        // not indexed.
        for (entry, old_wal_offset) in self
            .pending_entries
            .iter()
            .zip(pre_promote_offsets.iter().copied())
        {
            match entry.kind {
                EntryKind::Data
                | EntryKind::Inline
                | EntryKind::CanonicalData
                | EntryKind::CanonicalInline => {}
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
            }
            let Some(old_wal_offset) = old_wal_offset else {
                // No prior extent index entry for this hash. write_commit
                // always inserts a Data/Inline hash before pushing the
                // SegmentEntry, so this is only possible if something
                // removed the entry out-of-band between the write and the
                // flush — treat it like a failed CAS and leave it alone.
                continue;
            };
            let idata = if entry.kind.is_inline() {
                entry.data.clone().map(Vec::into_boxed_slice)
            } else {
                None
            };
            Arc::make_mut(&mut self.extent_index).replace_if_matches(
                entry.hash,
                old_wal_ulid,
                old_wal_offset,
                extentindex::ExtentLocation {
                    segment_id: segment_ulid,
                    body_offset: entry.stored_offset,
                    body_length: entry.stored_length,
                    compressed: entry.compressed,
                    body_source: BodySource::Local,
                    body_section_start,
                    inline_data: idata,
                },
            );
        }
        {
            let (mut data, mut refs, mut zero, mut inline, mut delta, mut canonical) =
                (0usize, 0usize, 0usize, 0usize, 0usize, 0usize);
            for e in &self.pending_entries {
                match e.kind {
                    EntryKind::Data => data += 1,
                    EntryKind::DedupRef => refs += 1,
                    EntryKind::Zero => zero += 1,
                    EntryKind::Inline => inline += 1,
                    EntryKind::Delta => delta += 1,
                    EntryKind::CanonicalData | EntryKind::CanonicalInline => canonical += 1,
                }
            }
            let _ = canonical; // unused in this flush-path log (user writes never produce canonicals); present to keep the match exhaustive.
            log::info!(
                "flush {segment_ulid} (from WAL {old_wal_ulid}): {data} data, {inline} inline, \
                 {refs} dedup-ref, {zero} zero, {delta} delta ({} entries total)",
                self.pending_entries.len()
            );
        }
        self.pending_entries.clear();
        // index/<ulid>.idx is written later by the promote_segment IPC handler,
        // after the coordinator confirms S3 upload. Until then pending/<ulid>
        // is the authoritative body source for both reads and crash recovery.
        //
        // Delete the old WAL file. `segment::write_and_commit` leaves the WAL
        // alone so the off-actor worker (Landing 3) can defer this delete
        // until after the actor's publish_snapshot; on the current actor-
        // inline path we just delete immediately. With a fresh segment ULID
        // (not reusing `old_wal_ulid`), a stale cold-cache reader that still
        // holds the pre-promote snapshot either finds `wal/<old_wal_ulid>`
        // at its expected path (before this unlink) or gets NotFound (after)
        // — never a silent read of wrong bytes through `pending/<same_ulid>`.
        fs::remove_file(&old_wal_path)?;
        // Evict any cached fd for the deleted WAL so subsequent lookups of
        // `old_wal_ulid` re-open rather than reuse a handle to the deleted
        // inode. The cache is keyed by the path that was open, so we pass
        // the WAL's original ULID — not `segment_ulid`.
        self.evict_cached_segment(old_wal_ulid);
        Ok(())
    }

    /// Promote the current WAL to a pending segment. The next write lazily
    /// opens a fresh WAL via `ensure_wal_open`.
    fn promote(&mut self) -> io::Result<()> {
        self.flush_wal_to_pending()
    }

    /// In-process checkpoint of the fork at the current point in the
    /// segment sequence. **Not** the production path — the coordinator-
    /// driven snapshot flow (see `docs/coordinator-driven-snapshot-plan.md`)
    /// orchestrates flush → S3 drain → signed manifest → upload.
    ///
    /// This in-process variant exists for tests and offline tooling that
    /// need a self-contained snapshot without a running coordinator. It
    /// flushes the WAL to `pending/`, promotes every pending segment so it
    /// appears under `index/`, signs the `.manifest` file over the
    /// resulting full index, then writes the `snapshots/<ulid>` marker.
    ///
    /// Note that promotion writes `cache/<ulid>.body` + `index/<ulid>.idx`
    /// without uploading to S3; in production only the coordinator is
    /// allowed to promote, and only after confirming upload.
    ///
    /// If no new data has been committed since the latest existing snapshot
    /// (nothing in `pending/` or `index/` sorts after it), the existing
    /// snapshot ULID is returned without writing a new marker.
    ///
    /// Returns the snapshot ULID.
    pub fn snapshot(&mut self) -> io::Result<Ulid> {
        // Flush WAL to pending/ first so the snapshot marker sorts after it.
        self.flush_wal_to_pending()?;

        // If no new segments have been committed since the last snapshot, reuse
        // the existing snapshot ULID rather than writing a new marker. The WAL
        // stays closed — the next write lazily opens a fresh one.
        if !self.has_new_segments
            && let Some(latest_str) = latest_snapshot(&self.base_dir)?
        {
            return Ok(latest_str);
        }

        // Write a new snapshot marker, reusing the last segment's ULID so the
        // branch point is self-describing. Falls back to a fresh ULID only when
        // no segments exist (e.g. first snapshot on an empty fork).
        let snap_ulid = self.last_segment_ulid.unwrap_or_else(|| self.mint.next());

        // First-snapshot pinning invariant (see docs/architecture.md § Dedup).
        // Every DedupRef written in this volume resolves through the extent
        // index to a canonical `Data` entry; the entry's segment_id is the
        // DedupRef's target. At snapshot time, every own-volume target must
        // have ULID <= snap_ulid so that advancing the floor pins every live
        // DedupRef atomically. Violation would mean a future write raced the
        // snapshot and leaked an unpinned reference — a correctness bug.
        // Ancestor targets are pinned by their own volume's floor and are
        // excluded from this check.
        #[cfg(debug_assertions)]
        {
            let mut own_segments: std::collections::HashSet<Ulid> =
                std::collections::HashSet::new();
            if let Some(open) = self.wal.as_ref() {
                own_segments.insert(open.ulid);
            }
            for entry in fs::read_dir(self.base_dir.join("pending"))?.flatten() {
                if let Some(s) = entry.file_name().to_str()
                    && !s.contains('.')
                    && let Ok(u) = Ulid::from_string(s)
                {
                    own_segments.insert(u);
                }
            }
            if let Ok(idx_files) = segment::collect_idx_files(&self.base_dir.join("index")) {
                for p in idx_files {
                    if let Some(u) = p
                        .file_stem()
                        .and_then(|n| n.to_str())
                        .and_then(|s| Ulid::from_string(s).ok())
                    {
                        own_segments.insert(u);
                    }
                }
            }
            for (_hash, loc) in self.extent_index.iter() {
                if own_segments.contains(&loc.segment_id) {
                    debug_assert!(
                        loc.segment_id <= snap_ulid,
                        "first-snapshot pinning invariant violated: extent index \
                         references own segment {} which is > snap_ulid {}",
                        loc.segment_id,
                        snap_ulid,
                    );
                }
            }
        }

        // Promote every pending segment so the signed `.manifest` file
        // can enumerate a complete `index/` rather than a partial view.
        // In production this is driven by the coordinator after confirming
        // S3 upload; the in-process variant skips the upload step.
        let pending_dir = self.base_dir.join("pending");
        let mut pending_ulids: Vec<Ulid> = Vec::new();
        if let Ok(entries) = fs::read_dir(&pending_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(s) = name.to_str() else { continue };
                if s.contains('.') {
                    continue;
                }
                if let Ok(u) = Ulid::from_string(s) {
                    pending_ulids.push(u);
                }
            }
        }
        pending_ulids.sort();
        for u in pending_ulids {
            self.promote_segment(u)?;
        }

        let snapshots_dir = self.base_dir.join("snapshots");
        fs::create_dir_all(&snapshots_dir)?;

        // Collect every segment ULID now under `index/` for the signed
        // manifest — this is the full set of segments belonging to this
        // volume up to `snap_ulid`, including promoted-this-call and
        // anything already there from prior activity.
        let index_dir = self.base_dir.join("index");
        let mut index_ulids: Vec<Ulid> = Vec::new();
        if let Ok(entries) = fs::read_dir(&index_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(s) = name.to_str() else { continue };
                let Some(stem) = s.strip_suffix(".idx") else {
                    continue;
                };
                if let Ok(u) = Ulid::from_string(stem) {
                    index_ulids.push(u);
                }
            }
        }
        crate::signing::write_snapshot_manifest(
            &self.base_dir,
            self.signer.as_ref(),
            &snap_ulid,
            &index_ulids,
        )?;

        // Marker is written last — a partial sequence leaves no snapshot
        // visible under `snapshots/`.
        fs::write(snapshots_dir.join(snap_ulid.to_string()), "")?;
        self.has_new_segments = false;

        // The WAL was closed by `flush_wal_to_pending` above. The next write
        // lazily opens a fresh one.
        Ok(snap_ulid)
    }

    /// Sign and write a snapshot manifest under `snapshots/<snap_ulid>.manifest`,
    /// then write the `snapshots/<snap_ulid>` marker.
    ///
    /// Called by the coordinator after a synchronous drain has moved every
    /// in-flight segment out of `pending/` and into `index/`. The volume
    /// enumerates its own `index/` at the moment of the call: the result is a
    /// full list of every segment ULID that belongs to this volume as of the
    /// snapshot, *not* a delta over the previous snapshot. See
    /// `docs/coordinator-driven-snapshot-plan.md` for the rationale.
    ///
    /// The manifest is signed with the volume's private key so ancestor
    /// verification at open time can trust it via the embedded
    /// `parent_pubkey` in the child's `volume.provenance`.
    ///
    /// The caller selects `snap_ulid` — typically the max ULID in `index/`
    /// at the moment the lock is acquired, or a fresh ULID if `index/` is
    /// empty. The volume does not validate the choice.
    /// Synchronous wrapper around the offloadable prep / execute / apply
    /// trio. The actor uses [`Self::prepare_sign_snapshot_manifest`],
    /// [`crate::actor::execute_sign_snapshot_manifest`], and
    /// [`Self::apply_sign_snapshot_manifest_result`] directly so the
    /// worker thread runs the heavy middle — `index/` enumeration,
    /// Ed25519 sign, manifest fsync, marker write — off the request
    /// channel. This wrapper exists for tests and any inline callers.
    pub fn sign_snapshot_manifest(&mut self, snap_ulid: Ulid) -> io::Result<()> {
        let job = self.prepare_sign_snapshot_manifest(snap_ulid);
        let result = crate::actor::execute_sign_snapshot_manifest(job)?;
        self.apply_sign_snapshot_manifest_result(result);
        Ok(())
    }

    /// Prep phase of `sign_snapshot_manifest` — runs on the actor
    /// thread. Cheap: clones the signer `Arc` and captures the base dir
    /// and target ULID.
    pub fn prepare_sign_snapshot_manifest(&self, snap_ulid: Ulid) -> SignSnapshotManifestJob {
        SignSnapshotManifestJob {
            snap_ulid,
            base_dir: self.base_dir.clone(),
            signer: Arc::clone(&self.signer),
        }
    }

    /// Apply phase of `sign_snapshot_manifest` — runs on the actor
    /// thread after the worker has written the manifest and marker.
    /// Clears `has_new_segments` so subsequent snapshot attempts with
    /// no new data reuse the marker instead of re-signing.
    pub fn apply_sign_snapshot_manifest_result(&mut self, _result: SignSnapshotManifestResult) {
        self.has_new_segments = false;
    }

    /// Locate the segment body file for `segment_id` within this fork's
    /// ancestry chain.
    ///
    /// Search order:
    ///   1. Current fork: `wal/`, `pending/`, bare `gc/<id>`, `cache/<id>.body`
    ///   2. Ancestor forks (newest first): `pending/`, bare `gc/<id>`, `cache/<id>.body`
    ///   3. Demand-fetch via fetcher (writes three-file format to `cache/`)
    ///
    /// For full segment files (`wal/`, `pending/`, bare `gc/<id>`), body reads
    /// use absolute file offsets (`ExtentLocation.body_offset`). For cached
    /// body files (`cache/<id>.body`), the file IS the body section, so reads
    /// use body-relative offsets — consistent with how `extentindex::rebuild`
    /// stores offsets for cached entries.
    fn find_segment_file(
        &self,
        segment_id: Ulid,
        body_section_start: u64,
        body_source: BodySource,
    ) -> io::Result<PathBuf> {
        find_segment_in_dirs(
            segment_id,
            &self.base_dir,
            &self.ancestor_layers,
            self.fetcher.as_ref(),
            body_section_start,
            body_source,
        )
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn ancestor_count(&self) -> usize {
        self.ancestor_layers.len()
    }

    pub fn lbamap_len(&self) -> usize {
        self.lbamap.len()
    }

    /// Attach a `SegmentFetcher` for demand-fetch on segment cache miss.
    ///
    /// Once set, `find_segment_file` will call the fetcher after all local
    /// directories are checked, caching the result in `cache/`.
    pub fn set_fetcher(&mut self, fetcher: BoxFetcher) {
        self.fetcher = Some(fetcher);
    }

    /// Return all fork directories in the ancestry chain, oldest-first,
    /// with the current fork last.
    ///
    /// Used by callers building a `SegmentFetcher` that needs to know which
    /// forks to search on a cache miss.
    pub fn fork_dirs(&self) -> Vec<PathBuf> {
        self.ancestor_layers
            .iter()
            .map(|l| l.dir.clone())
            .chain(std::iter::once(self.base_dir.clone()))
            .collect()
    }

    /// Return the current LBA map and extent index as shared references.
    ///
    /// Called by `VolumeActor` after every mutation to publish a new `ReadSnapshot`.
    /// The cost is two `Arc::clone` calls — O(1) unless a snapshot reader is still
    /// holding the previous version, in which case `Arc::make_mut` in the next
    /// mutation triggers a copy-on-write clone.
    pub fn snapshot_maps(&self) -> (Arc<lbamap::LbaMap>, Arc<extentindex::ExtentIndex>) {
        (Arc::clone(&self.lbamap), Arc::clone(&self.extent_index))
    }

    /// Synchronous alias-merge wrapper: prepare → execute → apply.
    ///
    /// Used by tests and inline callers that hold a `&mut Volume`
    /// directly. Production callers go through the actor channel via
    /// [`crate::actor::VolumeClient::reclaim_alias_merge`], where the
    /// heavy middle phase runs on the worker thread.
    pub fn reclaim_alias_merge(
        &mut self,
        start_lba: u64,
        lba_length: u32,
    ) -> io::Result<ReclaimOutcome> {
        let job = self.prepare_reclaim(start_lba, lba_length)?;
        let result = crate::actor::execute_reclaim(job)?;
        self.apply_reclaim_result(result)
    }

    /// Prep phase of reclaim — runs on the actor thread.
    ///
    /// Snapshots `lbamap` (precondition token + read source for the
    /// worker's bloat-gate walk), snapshots `extent_index` (so the
    /// worker can resolve hashes to segment bodies without an actor
    /// round-trip), captures the clipped range entries, and mints the
    /// output segment ULID so the worker can write
    /// `pending/<segment_ulid>` directly.
    ///
    /// Search dirs are the fork directory followed by ancestor layers
    /// in the same order `BlockReader` uses — the worker's read helper
    /// walks them to find segment body files.
    ///
    /// ## Ordering invariant: `u_flush < u_reclaim`
    ///
    /// Flushes any open WAL to `pending/<u_flush>` before minting the
    /// reclaim output ULID `u_reclaim`. Mirrors the `u_gc < u_flush`
    /// invariant documented on [`GcCheckpointUlids`], but with reversed
    /// polarity: reclaim outputs must sort *above* the flushed-WAL
    /// segment, because reclaim's new LBA mappings supersede the
    /// pre-reclaim WAL entries they consumed.
    ///
    /// Without this flush, a WAL entry for an LBA that reclaim rewrites
    /// survives to `pending/<u_flush>` where `u_flush > u_reclaim`. On
    /// crash rebuild the flushed segment applies after the reclaim
    /// output and shadows it — re-pointing the LBA at a hash whose body
    /// `redact_segment` may have already hole-punched (hash-dead under
    /// the post-reclaim lbamap), producing zero reads.
    pub fn prepare_reclaim(&mut self, start_lba: u64, lba_length: u32) -> io::Result<ReclaimJob> {
        self.flush_wal()?;

        let end_lba = start_lba + lba_length as u64;
        let entries = self.lbamap.extents_in_range(start_lba, end_lba);

        let mut search_dirs: Vec<PathBuf> = vec![self.base_dir.clone()];
        for layer in &self.ancestor_layers {
            if !search_dirs.contains(&layer.dir) {
                search_dirs.push(layer.dir.clone());
            }
        }

        let segment_ulid = self.mint.next();
        let snapshot_floor_ulid = latest_snapshot(&self.base_dir)?;

        Ok(ReclaimJob {
            target_start_lba: start_lba,
            target_lba_length: lba_length,
            entries,
            lbamap_snapshot: Arc::clone(&self.lbamap),
            extent_index_snapshot: Arc::clone(&self.extent_index),
            search_dirs,
            pending_dir: self.base_dir.join("pending"),
            segment_ulid,
            signer: Arc::clone(&self.signer),
            snapshot_floor_ulid,
        })
    }

    /// Apply phase of reclaim — runs on the actor thread after the
    /// worker returns.
    ///
    /// Precondition: `Arc::ptr_eq(result.lbamap_snapshot, self.lbamap)`.
    /// Any mutation between prepare and apply would have called
    /// `Arc::make_mut` on the volume's `lbamap` Arc (externally visible
    /// via at least the published snapshot), which reallocates — so the
    /// pointers differ and this returns cleanly with `discarded: true`,
    /// deleting the worker's pending segment as an orphan. GC would
    /// eventually classify it as all-dead, but cleaning up eagerly
    /// avoids the extra GC round-trip.
    ///
    /// On success, splices the worker's entries into the live lbamap +
    /// extent index. Runs under the actor lock; no CAS needed because
    /// the pointer-equality guard above already proved no concurrent
    /// mutation happened.
    pub fn apply_reclaim_result(&mut self, result: ReclaimResult) -> io::Result<ReclaimOutcome> {
        if !Arc::ptr_eq(&result.lbamap_snapshot, &self.lbamap) {
            // Orphan cleanup: delete the worker's pending/<segment_ulid>.
            if result.segment_written {
                let path = result.pending_dir.join(result.segment_ulid.to_string());
                let _ = std::fs::remove_file(&path);
            }
            return Ok(ReclaimOutcome {
                discarded: true,
                ..Default::default()
            });
        }

        if !result.segment_written {
            return Ok(ReclaimOutcome::default());
        }

        self.has_new_segments = true;
        self.last_segment_ulid = Some(result.segment_ulid);

        let lbamap = Arc::make_mut(&mut self.lbamap);
        let extent_index = Arc::make_mut(&mut self.extent_index);
        for re in &result.entries {
            let entry = &re.entry;
            match entry.kind {
                EntryKind::Data => {
                    lbamap.insert(entry.start_lba, entry.lba_length, entry.hash);
                    extent_index.insert(
                        entry.hash,
                        extentindex::ExtentLocation {
                            segment_id: result.segment_ulid,
                            body_offset: entry.stored_offset,
                            body_length: entry.stored_length,
                            compressed: entry.compressed,
                            body_source: BodySource::Local,
                            body_section_start: result.body_section_start,
                            inline_data: None,
                        },
                    );
                }
                EntryKind::Inline => {
                    lbamap.insert(entry.start_lba, entry.lba_length, entry.hash);
                    extent_index.insert(
                        entry.hash,
                        extentindex::ExtentLocation {
                            segment_id: result.segment_ulid,
                            body_offset: entry.stored_offset,
                            body_length: entry.stored_length,
                            compressed: entry.compressed,
                            body_source: BodySource::Local,
                            body_section_start: result.body_section_start,
                            inline_data: entry.data.clone().map(Vec::into_boxed_slice),
                        },
                    );
                }
                EntryKind::DedupRef => {
                    // Canonical body already indexed — nothing to insert
                    // in extent_index; lbamap just claims the LBA run.
                    lbamap.insert(entry.start_lba, entry.lba_length, entry.hash);
                }
                EntryKind::Delta => {
                    // Thin Delta: claim the LBA run with its source
                    // list attached (so GC's `lba_referenced_hashes`
                    // fold keeps the source alive), and register the
                    // delta in the extent_index. `body_length` is the
                    // tail of the body section; the delta region
                    // begins at `body_section_start + body_length`.
                    let source_hashes: Arc<[blake3::Hash]> =
                        entry.delta_options.iter().map(|o| o.source_hash).collect();
                    lbamap.insert_delta(
                        entry.start_lba,
                        entry.lba_length,
                        entry.hash,
                        source_hashes,
                    );
                    extent_index.insert_delta_if_absent(
                        entry.hash,
                        extentindex::DeltaLocation {
                            segment_id: result.segment_ulid,
                            body_source: extentindex::DeltaBodySource::Full {
                                body_section_start: result.body_section_start,
                                body_length: result.body_length,
                            },
                            options: entry.delta_options.clone(),
                        },
                    );
                }
                EntryKind::CanonicalData | EntryKind::CanonicalInline | EntryKind::Zero => {
                    unreachable!(
                        "reclaim output produces only Data/Inline/DedupRef/Delta, got {:?}",
                        entry.kind
                    );
                }
            }
        }

        let mut outcome = ReclaimOutcome::default();
        for re in &result.entries {
            outcome.runs_rewritten += 1;
            outcome.bytes_rewritten += re.uncompressed_bytes;
        }
        Ok(outcome)
    }

    /// Ancestor layers for this fork, oldest-first.
    pub fn ancestor_layers(&self) -> &[AncestorLayer] {
        &self.ancestor_layers
    }

    /// The attached demand-fetch fetcher, if any.
    pub fn fetcher(&self) -> Option<&BoxFetcher> {
        self.fetcher.as_ref()
    }

    /// Flush the current WAL to a pending segment if it contains any entries.
    /// No-op if the WAL is empty. Called by the idle-flush path in the NBD server.
    pub fn flush_wal(&mut self) -> io::Result<()> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }
        self.promote()
    }

    /// True if the WAL should be promoted to a pending segment.
    ///
    /// Trips on either the byte cap ([`FLUSH_THRESHOLD`]) or the entry-count
    /// cap ([`FLUSH_ENTRY_THRESHOLD`]) — the latter bounds the index region
    /// for workloads (heavy dedup, lots of inline / zero writes) that produce
    /// many thin entries without advancing the byte cap.
    ///
    /// The actor calls this after every write reply and promotes if true.
    /// The check is separated from `write()` so that writes are always fast
    /// (WAL append only) and the promotion cost is never borne by the write caller.
    pub fn needs_promote(&self) -> bool {
        self.wal.as_ref().is_some_and(|o| {
            o.wal.size() >= FLUSH_THRESHOLD || self.pending_entries.len() >= FLUSH_ENTRY_THRESHOLD
        })
    }

    pub fn promote_for_test(&mut self) -> io::Result<()> {
        self.promote()
    }

    // ------------------------------------------------------------------
    // Off-actor promote: prep + apply
    // ------------------------------------------------------------------

    /// Fsync the WAL without promoting. No-op when no WAL is open.
    ///
    /// Used by the actor's `Flush` handler to satisfy the NBD durability
    /// contract without blocking on segment serialization.
    pub fn wal_fsync(&mut self) -> io::Result<()> {
        match self.wal.as_mut() {
            Some(open) => open.wal.fsync(),
            None => Ok(()),
        }
    }

    /// Prep phase of the off-actor promote.  Runs on the actor thread.
    ///
    /// Takes the current WAL, snapshots CAS precondition tokens, takes
    /// ownership of `pending_entries`, and mints a fresh segment ULID.
    /// Returns `None` if the WAL is empty or absent (nothing to promote).
    ///
    /// After this call the volume's `wal` is `None`. The next write will
    /// lazily open a fresh WAL via `ensure_wal_open`; writes resume
    /// immediately. The returned [`PromoteJob`] is sent to the worker
    /// thread for the heavy segment-write work.
    pub fn prepare_promote(&mut self) -> io::Result<Option<PromoteJob>> {
        if self.pending_entries.is_empty() {
            return Ok(None);
        }

        // The old WAL's fsync is deferred to the worker thread (see
        // `execute_promote`), so that the actor returns to the select
        // loop without blocking on disk I/O.  `VolumeActor::Flush`
        // parks on the promote generation counter until the worker
        // has completed any in-flight promote, preserving NBD FLUSH's
        // durability contract while letting the actor keep processing
        // writes in the meantime.

        // pending_entries non-empty implies wal is Some (write path only
        // ever appends entries after opening the WAL).
        let open = self.wal.take().ok_or_else(|| {
            io::Error::other("internal: pending_entries non-empty but wal absent")
        })?;
        let old_wal_ulid = open.ulid;
        let old_wal_path = open.path;

        // Snapshot CAS tokens before write_and_commit rewrites stored_offset.
        let pre_promote_offsets: Vec<Option<u64>> = self
            .pending_entries
            .iter()
            .map(|e| match e.kind {
                EntryKind::Data
                | EntryKind::Inline
                | EntryKind::CanonicalData
                | EntryKind::CanonicalInline => {
                    self.extent_index.lookup(&e.hash).map(|loc| loc.body_offset)
                }
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => None,
            })
            .collect();

        let entries = std::mem::take(&mut self.pending_entries);
        let segment_ulid = self.mint.next();
        let pending_dir = self.base_dir.join("pending");

        Ok(Some(PromoteJob {
            segment_ulid,
            old_wal_ulid,
            old_wal_path,
            entries,
            pre_promote_offsets,
            signer: Arc::clone(&self.signer),
            pending_dir,
        }))
    }

    /// Apply phase of the off-actor promote.  Runs on the actor thread
    /// after the worker has written the segment.
    ///
    /// Updates the extent index (CAS), deletes the old WAL, and evicts
    /// the cached file descriptor.  The caller must call `publish_snapshot`
    /// after this to make the changes visible to readers.
    pub fn apply_promote(&mut self, result: &PromoteResult) {
        self.has_new_segments = true;
        self.last_segment_ulid = Some(result.segment_ulid);

        // CAS loop: rewrite extent index entries from WAL-relative to
        // segment-relative offsets, but only if the entry hasn't been
        // superseded by a concurrent write or GC handoff.
        for (entry, old_wal_offset) in result
            .entries
            .iter()
            .zip(result.pre_promote_offsets.iter().copied())
        {
            match entry.kind {
                EntryKind::Data
                | EntryKind::Inline
                | EntryKind::CanonicalData
                | EntryKind::CanonicalInline => {}
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
            }
            let Some(old_wal_offset) = old_wal_offset else {
                continue;
            };
            let idata = if entry.kind.is_inline() {
                entry.data.clone().map(Vec::into_boxed_slice)
            } else {
                None
            };
            Arc::make_mut(&mut self.extent_index).replace_if_matches(
                entry.hash,
                result.old_wal_ulid,
                old_wal_offset,
                extentindex::ExtentLocation {
                    segment_id: result.segment_ulid,
                    body_offset: entry.stored_offset,
                    body_length: entry.stored_length,
                    compressed: entry.compressed,
                    body_source: BodySource::Local,
                    body_section_start: result.body_section_start,
                    inline_data: idata,
                },
            );
        }

        // Log entry counts.
        {
            let (mut data, mut refs, mut zero, mut inline, mut delta, mut canonical) =
                (0usize, 0usize, 0usize, 0usize, 0usize, 0usize);
            for e in &result.entries {
                match e.kind {
                    EntryKind::Data => data += 1,
                    EntryKind::DedupRef => refs += 1,
                    EntryKind::Zero => zero += 1,
                    EntryKind::Inline => inline += 1,
                    EntryKind::Delta => delta += 1,
                    EntryKind::CanonicalData | EntryKind::CanonicalInline => canonical += 1,
                }
            }
            let _ = canonical;
            log::info!(
                "flush {} (from WAL {}): {data} data, {inline} inline, {refs} dedup-ref, \
                 {zero} zero, {delta} delta ({} entries total)",
                result.segment_ulid,
                result.old_wal_ulid,
                result.entries.len()
            );
        }

        // Delete old WAL — only after the extent index is updated.
        if let Err(e) = fs::remove_file(&result.old_wal_path) {
            log::warn!(
                "failed to delete old WAL {}: {e}",
                result.old_wal_path.display()
            );
        }
        self.evict_cached_segment(result.old_wal_ulid);
    }

    // ------------------------------------------------------------------
    // Off-actor GC checkpoint: prep + complete
    // ------------------------------------------------------------------

    /// Prep phase of the off-actor GC checkpoint.
    ///
    /// Mints two ULIDs (`u_gc < u_flush`), snapshots CAS
    /// tokens, takes entries, and builds a [`PromoteJob`] using `u_flush`
    /// as the segment ULID. No fresh WAL is opened here — the next write
    /// lazily opens one at `mint.next()`, which is guaranteed >
    /// `u_flush > u_gc` by monotonicity. This avoids churning a new
    /// empty WAL file on every idle GC tick.
    ///
    /// The old WAL's `fsync()` is deferred to the worker thread (see
    /// `execute_promote`), identical to the write-path promote offload.
    /// The parked GC reply only resolves after the worker returns, so
    /// the caller of `GcCheckpoint` still observes a durable old WAL
    /// before acting on `u_gc`.
    ///
    /// Returns `job: None` when the WAL was empty or absent (no segment
    /// to promote). The checkpoint completes immediately in that case.
    ///
    /// Always mints ULIDs. An earlier Idle short-circuit (cfcb132) was
    /// reverted because the coordinator's per-tick `promote_wal` IPC
    /// empties `pending_entries` before this call, which would make the
    /// Idle check fire on every tick under active writes and silently
    /// disable GC. We still run GC on every tick; we only stop creating
    /// a new WAL file when there is nothing to promote.
    pub fn prepare_gc_checkpoint(&mut self) -> io::Result<GcCheckpointPrep> {
        let GcCheckpointUlids { u_gc, u_flush } = self.mint_gc_checkpoint_ulids();

        if self.pending_entries.is_empty() {
            // Empty or absent WAL — delete any lingering file, leave wal None.
            if let Some(open) = self.wal.take() {
                fs::remove_file(&open.path)?;
            }
            return Ok(GcCheckpointPrep {
                u_gc,
                u_flush,
                job: None,
            });
        }

        // pending_entries non-empty implies wal is Some (write path only
        // ever appends entries after opening the WAL).
        let open = self.wal.take().ok_or_else(|| {
            io::Error::other("internal: pending_entries non-empty but wal absent")
        })?;
        let old_wal_ulid = open.ulid;
        let old_wal_path = open.path;

        let pre_promote_offsets: Vec<Option<u64>> = self
            .pending_entries
            .iter()
            .map(|e| match e.kind {
                EntryKind::Data
                | EntryKind::Inline
                | EntryKind::CanonicalData
                | EntryKind::CanonicalInline => {
                    self.extent_index.lookup(&e.hash).map(|loc| loc.body_offset)
                }
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => None,
            })
            .collect();

        let entries = std::mem::take(&mut self.pending_entries);
        let pending_dir = self.base_dir.join("pending");

        Ok(GcCheckpointPrep {
            u_gc,
            u_flush,
            job: Some(PromoteJob {
                segment_ulid: u_flush,
                old_wal_ulid,
                old_wal_path,
                entries,
                pre_promote_offsets,
                signer: Arc::clone(&self.signer),
                pending_dir,
            }),
        })
    }
}

// --- helpers ---

/// Read `lba_count` 4KB blocks starting at `lba` from the given LBA map and extent index.
///
/// Unwritten blocks are returned as zeros. Written blocks are fetched extent-by-extent
/// using `find_segment` to locate each segment file, with recently-opened file handles
/// cached in `file_cache` (LRU) to amortize `open` syscalls across reads.
pub(crate) fn read_extents(
    lba: u64,
    lba_count: u32,
    lbamap: &lbamap::LbaMap,
    extent_index: &extentindex::ExtentIndex,
    file_cache: &RefCell<FileCache>,
    find_segment: impl Fn(Ulid, u64, BodySource) -> io::Result<PathBuf>,
    open_delta_body: impl Fn(Ulid) -> io::Result<fs::File>,
) -> io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    let mut out = vec![0u8; lba_count as usize * 4096];
    for er in lbamap.extents_in_range(lba, lba + lba_count as u64) {
        // Zero extents: output buffer is already zeroed; nothing to fetch.
        if er.hash == ZERO_HASH {
            continue;
        }

        // Extract owned copies so the borrow of extent_index ends before
        // we mutate file_cache.
        let direct = extent_index.lookup(&er.hash).map(|loc| {
            (
                loc.segment_id,
                loc.body_offset,
                loc.body_length,
                loc.compressed,
                loc.body_section_start,
                loc.body_source,
                loc.inline_data.clone(),
            )
        });
        let (
            segment_id,
            body_offset,
            body_length,
            compressed,
            body_section_start,
            body_source,
            inline_data,
        ) = match direct {
            Some(loc) => loc,
            None => {
                // No direct DATA/Inline entry. Try a Delta entry.
                if try_read_delta_extent(
                    &er,
                    lba,
                    extent_index,
                    file_cache,
                    &find_segment,
                    &open_delta_body,
                    &mut out,
                )? {
                    continue;
                }
                continue; // truly unknown — treat as unwritten
            }
        };

        // Inline extents: data is held in memory, no file I/O needed.
        if let Some(ref idata) = inline_data {
            let block_count = (er.range_end - er.range_start) as usize;
            let out_start = (er.range_start - lba) as usize * 4096;
            let out_slice = &mut out[out_start..out_start + block_count * 4096];

            let raw = if compressed {
                lz4_flex::decompress_size_prepended(idata).map_err(io::Error::other)?
            } else {
                idata.to_vec()
            };
            let src_start = er.payload_block_offset as usize * 4096;
            let src_end = src_start + block_count * 4096;
            let src_slice = raw
                .get(src_start..src_end)
                .ok_or_else(|| io::Error::other("corrupt segment: inline payload too short"))?;
            out_slice.copy_from_slice(src_slice);
            continue;
        }

        // For cached entries, always call find_segment to check the .present
        // bitset — the .body file may exist but the specific entry may not
        // yet be fetched.
        let mut cache = file_cache.borrow_mut();
        if matches!(body_source, BodySource::Cached(_)) || cache.get(segment_id).is_none() {
            let path = find_segment(segment_id, body_section_start, body_source)?;
            let layout = SegmentLayout::from_path(&path);
            cache.insert(segment_id, layout, fs::File::open(&path)?);
        }
        let (layout, f) = cache
            .get(segment_id)
            .expect("entry was just inserted or found");

        // body_offset is always body-relative (= stored_offset from the segment index).
        // For full segment files we must add body_section_start to get the file offset.
        let file_body_offset = match layout {
            SegmentLayout::BodyOnly => body_offset,
            SegmentLayout::Full => body_section_start + body_offset,
        };

        let block_count = (er.range_end - er.range_start) as usize;
        let out_start = (er.range_start - lba) as usize * 4096;
        let out_slice = &mut out[out_start..out_start + block_count * 4096];

        if compressed {
            f.seek(SeekFrom::Start(file_body_offset))?;
            let mut compressed_buf = vec![0u8; body_length as usize];
            f.read_exact(&mut compressed_buf)?;
            let decompressed =
                lz4_flex::decompress_size_prepended(&compressed_buf).map_err(|e| {
                    log::error!(
                        "lz4 decompression failed: lba={} segment={} layout={:?} \
                     bss={} body_offset={} body_length={} body_source={:?} \
                     file_body_offset={} first_bytes={:?} err={}",
                        lba,
                        segment_id,
                        layout,
                        body_section_start,
                        body_offset,
                        body_length,
                        body_source,
                        file_body_offset,
                        &compressed_buf[..compressed_buf.len().min(16)],
                        e,
                    );
                    io::Error::other(e)
                })?;
            let src_start = er.payload_block_offset as usize * 4096;
            let src_end = src_start + block_count * 4096;
            let src_slice = decompressed.get(src_start..src_end).ok_or_else(|| {
                io::Error::other("corrupt segment: decompressed payload too short")
            })?;
            out_slice.copy_from_slice(src_slice);
        } else {
            f.seek(SeekFrom::Start(
                file_body_offset + er.payload_block_offset as u64 * 4096,
            ))?;
            if let Err(e) = f.read_exact(out_slice) {
                let file_size = f.metadata().map(|m| m.len()).unwrap_or(0);
                log::error!(
                    "read_extents failed: lba={} segment={} layout={:?} \
                     bss={} body_offset={} body_length={} payload_block_offset={} \
                     file_body_offset={} read_len={} file_size={} err={}",
                    lba,
                    segment_id,
                    layout,
                    body_section_start,
                    body_offset,
                    body_length,
                    er.payload_block_offset,
                    file_body_offset,
                    out_slice.len(),
                    file_size,
                    e,
                );
                return Err(e);
            }
        }
    }
    Ok(out)
}

/// Try to materialise a Delta extent for the range covered by `er`,
/// writing decoded bytes into `out` at the appropriate offset.
///
/// Returns `Ok(true)` if a Delta entry was found and decompressed
/// successfully, `Ok(false)` if no Delta entry is registered for
/// `er.hash` (caller falls through to "unwritten" handling), or
/// `Err` for any I/O or decompression failure.
///
/// Source selection uses the earliest-source preference: scan the
/// delta options in order, pick the first one whose `source_hash`
/// resolves via `extent_index.lookup` to a DATA/Inline location. No
/// caching of decompressed output — each read decompresses fresh.
/// Phase C accepts the decompression cost in exchange for
/// implementation simplicity; a follow-up can add a content-hash-
/// addressed materialisation cache if telemetry shows it matters.
#[allow(clippy::too_many_arguments)]
fn try_read_delta_extent(
    er: &lbamap::ExtentRead,
    lba: u64,
    extent_index: &extentindex::ExtentIndex,
    file_cache: &RefCell<FileCache>,
    find_segment: &dyn Fn(Ulid, u64, BodySource) -> io::Result<PathBuf>,
    open_delta_body: &dyn Fn(Ulid) -> io::Result<fs::File>,
    out: &mut [u8],
) -> io::Result<bool> {
    use std::io::{Read, Seek, SeekFrom};

    let Some(delta_loc) = extent_index.lookup_delta(&er.hash) else {
        return Ok(false);
    };
    let delta_segment_id = delta_loc.segment_id;
    let delta_body_source = delta_loc.body_source;
    let options = delta_loc.options.clone();

    // Pick the first option whose source hash resolves to a DATA/Inline
    // location. This is the earliest-source preference in its simplest
    // form; a more sophisticated version (prefer already-cached sources,
    // then earliest ULID among uncached) is a follow-up once the
    // demand-fetch path integrates.
    let mut picked: Option<(segment::DeltaOption, extentindex::ExtentLocation)> = None;
    for opt in &options {
        if let Some(source_loc) = extent_index.lookup(&opt.source_hash) {
            picked = Some((opt.clone(), source_loc.clone()));
            break;
        }
    }
    let Some((opt, source_loc)) = picked else {
        return Err(io::Error::other(format!(
            "delta extent {}: no source option resolved in extent index",
            er.hash.to_hex()
        )));
    };

    // --- Read the source body (full extent, lz4-decompressed if needed). ---
    let source_bytes: Vec<u8> = if let Some(ref idata) = source_loc.inline_data {
        if source_loc.compressed {
            lz4_flex::decompress_size_prepended(idata).map_err(io::Error::other)?
        } else {
            idata.to_vec()
        }
    } else {
        let mut cache = file_cache.borrow_mut();
        if matches!(source_loc.body_source, BodySource::Cached(_))
            || cache.get(source_loc.segment_id).is_none()
        {
            let path = find_segment(
                source_loc.segment_id,
                source_loc.body_section_start,
                source_loc.body_source,
            )?;
            let layout = SegmentLayout::from_path(&path);
            cache.insert(source_loc.segment_id, layout, fs::File::open(&path)?);
        }
        let (layout, f) = cache
            .get(source_loc.segment_id)
            .expect("source just inserted or found");
        let file_body_offset = match layout {
            SegmentLayout::BodyOnly => source_loc.body_offset,
            SegmentLayout::Full => source_loc.body_section_start + source_loc.body_offset,
        };
        f.seek(SeekFrom::Start(file_body_offset))?;
        let mut buf = vec![0u8; source_loc.body_length as usize];
        f.read_exact(&mut buf)?;
        if source_loc.compressed {
            lz4_flex::decompress_size_prepended(&buf).map_err(io::Error::other)?
        } else {
            buf
        }
    };

    // --- Read the delta blob from the Delta segment's delta body section. ---
    //
    // Two shapes: a full segment in `pending/` (delta body inline at
    // `body_section_start + body_length`) or a separate
    // `cache/<id>.delta` file (delta body starts at byte 0). The
    // extent_index records which via `DeltaBodySource`. For the
    // cached case we call `open_delta_body`, which returns an open
    // file handle — demand-fetching from the volume's attached
    // `SegmentFetcher` on miss.
    let delta_blob: Vec<u8> = match delta_body_source {
        extentindex::DeltaBodySource::Full {
            body_section_start: delta_bss,
            body_length: delta_body_length,
        } => {
            let mut cache = file_cache.borrow_mut();
            if cache.get(delta_segment_id).is_none() {
                let path = find_segment(delta_segment_id, delta_bss, BodySource::Local)?;
                let layout = SegmentLayout::from_path(&path);
                cache.insert(delta_segment_id, layout, fs::File::open(&path)?);
            }
            let (_layout, f) = cache
                .get(delta_segment_id)
                .expect("delta segment just inserted or found");
            f.seek(SeekFrom::Start(
                delta_bss + delta_body_length + opt.delta_offset,
            ))?;
            let mut buf = vec![0u8; opt.delta_length as usize];
            f.read_exact(&mut buf)?;
            buf
        }
        extentindex::DeltaBodySource::Cached => {
            // Opens cache/<id>.delta (demand-fetching via the attached
            // `SegmentFetcher` if the file is absent on a pull host).
            // Not routed through `file_cache` because .delta is a
            // distinct file from the segment body, and delta reads
            // are rare enough that caching the FD would complicate
            // eviction for little benefit.
            let mut f = open_delta_body(delta_segment_id)?;
            f.seek(SeekFrom::Start(opt.delta_offset))?;
            let mut buf = vec![0u8; opt.delta_length as usize];
            f.read_exact(&mut buf)?;
            buf
        }
    };

    // Reconstruct the full fragment bytes. We slice out the requested
    // portion below; the decompressor returns every byte the delta was
    // computed over, regardless of which LBA sub-range we want.
    let decompressed = delta_compute::apply_delta(&source_bytes, &delta_blob)?;

    // Copy the requested portion into the output buffer.
    let block_count = (er.range_end - er.range_start) as usize;
    let out_start = (er.range_start - lba) as usize * 4096;
    let out_slice = &mut out[out_start..out_start + block_count * 4096];
    let src_start = er.payload_block_offset as usize * 4096;
    let src_end = src_start + block_count * 4096;
    let src_slice = decompressed
        .get(src_start..src_end)
        .ok_or_else(|| io::Error::other("delta decompressed payload too short"))?;
    out_slice.copy_from_slice(src_slice);
    Ok(true)
}

/// Open `cache/<id>.delta` for reading, demand-fetching it on miss.
///
/// Only called from `try_read_delta_extent` when the extent_index
/// recorded the Delta entry as `DeltaBodySource::Cached` — i.e. the
/// segment has already been promoted to the three-file cache shape,
/// so the delta body, if local, lives in its own `.delta` file
/// rather than inline in a full segment.
///
/// On a pull host where `.delta` is absent the attached fetcher
/// downloads it atomically (tmp+rename) before we open. Returns
/// `NotFound` when the file is missing locally and no fetcher is
/// attached to fetch it.
pub(crate) fn open_delta_body_in_dirs(
    segment_id: Ulid,
    base_dir: &Path,
    ancestor_layers: &[AncestorLayer],
    fetcher: Option<&BoxFetcher>,
) -> io::Result<fs::File> {
    let sid = segment_id.to_string();

    let cache_delta = base_dir.join("cache").join(format!("{sid}.delta"));
    if cache_delta.exists() {
        return fs::File::open(&cache_delta);
    }
    for layer in ancestor_layers.iter().rev() {
        let ancestor_delta = layer.dir.join("cache").join(format!("{sid}.delta"));
        if ancestor_delta.exists() {
            return fs::File::open(&ancestor_delta);
        }
    }
    if let Some(fetcher) = fetcher {
        let index_dir = base_dir.join("index");
        let body_dir = base_dir.join("cache");
        fetcher.fetch_delta_body(segment_id, &index_dir, &body_dir)?;
        return fs::File::open(&cache_delta);
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("delta body not found: {sid}"),
    ))
}

/// Gate a `cache/<id>.body` hit on the `.present` bit for `Cached` entries.
/// Returns true for any non-cache layout (wal/pending/gc) and for cache hits
/// on `Local` entries. For `Cached` cache hits, checks the corresponding
/// `cache/<id>.present` bit alongside the `.body` file in `dir`.
fn cache_hit_allowed(
    layout: segment::SegmentBodyLayout,
    dir: &Path,
    sid: &str,
    body_source: BodySource,
) -> bool {
    if layout != segment::SegmentBodyLayout::BodyOnly {
        return true;
    }
    match body_source {
        BodySource::Local => true,
        BodySource::Cached(idx) => {
            let present_path = dir.join("cache").join(format!("{sid}.present"));
            segment::check_present_bit(&present_path, idx).unwrap_or(false)
        }
    }
}

/// Search for a segment file across the fork directory tree.
///
/// Search order:
///   1. Current fork: `wal/`, `pending/`, bare `gc/<id>`, `cache/<id>.body`
///   2. Ancestor forks (newest-first): `pending/`, bare `gc/<id>`, `cache/<id>.body`
///   3. Demand-fetch via fetcher (writes three-file format to `cache/`)
///
/// For `Cached` entries, a `cache/<id>.body` hit is only accepted if the
/// corresponding bit in `cache/<id>.present` is set — otherwise the entry
/// is not yet locally available and we fall through to the fetcher.
///
/// `.idx` files live in `index/` (coordinator-written, permanent).
/// `.body` and `.present` files live in `cache/` (volume-managed read cache).
///
/// Extracted from `Volume::find_segment_file` so that `VolumeReader` can serve
/// reads directly from a `ReadSnapshot` without going through the actor channel.
pub(crate) fn find_segment_in_dirs(
    segment_id: Ulid,
    base_dir: &Path,
    ancestor_layers: &[AncestorLayer],
    fetcher: Option<&BoxFetcher>,
    body_section_start: u64,
    body_source: BodySource,
) -> io::Result<PathBuf> {
    let sid = segment_id.to_string();
    // Self dir: full canonical precedence (wal → pending → bare gc/<id> → cache).
    // The bare-`gc/<id>` branch matters here because the extent index flips to
    // the new segment_id the moment the volume renames `<id>.tmp → <id>` (the
    // commit point of apply), before the coordinator has promoted the body to
    // `cache/`.
    if let Some((path, layout)) = segment::locate_segment_body(base_dir, segment_id)
        && cache_hit_allowed(layout, base_dir, &sid, body_source)
    {
        return Ok(path);
    }
    // Ancestor layers: segments here are always fork-parent state. They cannot
    // be mid-GC-handoff from this child's perspective, and they have no live
    // wal/, but pending/ and cache/<id>.body can both appear — the same helper
    // yields the right path; we just re-gate cache hits on the layer's own
    // `.present` file.
    for layer in ancestor_layers.iter().rev() {
        if let Some((path, layout)) = segment::locate_segment_body(&layer.dir, segment_id)
            && cache_hit_allowed(layout, &layer.dir, &sid, body_source)
        {
            return Ok(path);
        }
    }
    if let (Some(fetcher), BodySource::Cached(idx)) = (fetcher, body_source) {
        // The segment's `.idx` file lives in the index directory of whichever
        // volume wrote it — self for locally-written segments, an ancestor
        // for fork-parent segments. Search self first, then the ancestor
        // chain (in the same order rebuild_segments merges), and use that
        // volume's dirs so the fetched body lands in the owner's `cache/`
        // (where subsequent reads will find it via the ancestor scan above).
        let idx_filename = format!("{sid}.idx");
        let owner_dir = std::iter::once(base_dir)
            .chain(ancestor_layers.iter().map(|l| l.dir.as_path()))
            .find(|dir| dir.join("index").join(&idx_filename).exists())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "segment index not found in self or ancestors: {sid}.idx \
                         (ancestor chain may not be prefetched yet)"
                    ),
                )
            })?;
        let index_dir = owner_dir.join("index");
        let body_dir = owner_dir.join("cache");
        fetcher.fetch_extent(
            segment_id,
            &index_dir,
            &body_dir,
            &segment::ExtentFetch {
                body_section_start,
                body_offset: 0,
                body_length: 0,
                entry_idx: idx,
            },
        )?;
        return Ok(body_dir.join(format!("{sid}.body")));
    }
    Err(io::Error::other(format!("segment not found: {sid}")))
}

/// Rebuild the lbamap from disk and compare against the live in-memory
/// lbamap at each cancelled LBA. Logs only; never mutates volume state.
fn diagnose_stale_cancel_legacy(
    base_dir: &Path,
    ancestor_layers: &[AncestorLayer],
    in_memory: &lbamap::LbaMap,
    stale: &[(blake3::Hash, Ulid)],
    index_dir: &Path,
) {
    let mut chain: Vec<(PathBuf, Option<String>)> = ancestor_layers
        .iter()
        .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
        .collect();
    chain.push((base_dir.to_path_buf(), None));

    let rebuilt = match lbamap::rebuild_segments(&chain) {
        Ok(m) => m,
        Err(e) => {
            log::warn!("stale-liveness diagnostic rebuild failed: {e}");
            return;
        }
    };

    for (hash, input_ulid) in stale.iter().take(8) {
        let lbas = lbas_for_hash_in_segment(index_dir, input_ulid, hash);
        for (lba, len) in &lbas {
            let mem = in_memory.hash_at(*lba);
            let disk = rebuilt.hash_at(*lba);
            log::warn!(
                "stale-cancel diag: lba={lba}+{len} hash={} input={input_ulid} \
                 in_memory={:?} disk_rebuild={:?} {}",
                hash.to_hex(),
                mem.map(|h| h.to_hex().to_string()),
                disk.map(|h| h.to_hex().to_string()),
                if mem == disk { "AGREE" } else { "DIVERGE" }
            );
        }
    }
}

/// Read `index/<input_ulid>.idx` and return every entry matching `hash`.
fn lbas_for_hash_in_segment(
    index_dir: &Path,
    input_ulid: &Ulid,
    hash: &blake3::Hash,
) -> Vec<(u64, u32)> {
    let idx_path = index_dir.join(format!("{input_ulid}.idx"));
    let Ok((_, entries, _)) = segment::read_segment_index(&idx_path) else {
        return Vec::new();
    };
    entries
        .into_iter()
        .filter(|e| &e.hash == hash)
        .map(|e| (e.start_lba, e.lba_length))
        .collect()
}

/// Render a diagnostic summary of the stale-liveness hashes so the log
/// pinpoints which hash diverged and how it stays live in this volume.
/// Caps at the first 3 entries; trailing `...+N` indicates more.
fn describe_stale_cancel(stale: &[(blake3::Hash, Ulid)], lbamap: &lbamap::LbaMap) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for (i, (hash, input_ulid)) in stale.iter().take(3).enumerate() {
        if i > 0 {
            out.push_str("; ");
        }
        let lbas = lbamap.lbas_for_hash(hash);
        let delta_refcount = lbamap.delta_source_refcount(hash);
        let _ = write!(
            out,
            "hash={} input={input_ulid} lbas={lbas:?} delta_src_refcount={delta_refcount}",
            hash.to_hex(),
        );
    }
    if stale.len() > 3 {
        let _ = write!(out, "; ...+{} more", stale.len() - 3);
    }
    out
}

/// Acquire an exclusive non-blocking flock on `<dir>/volume.lock`.
///
/// Creates the lock file if it does not exist. Returns the open `File` — the
/// lock is held for as long as this handle is open and released when dropped.
/// Returns an error immediately if the lock is already held by another process.
fn acquire_lock(dir: &Path) -> io::Result<nix::fcntl::Flock<fs::File>> {
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.join("volume.lock"))?;
    nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock)
        .map_err(|(_, e)| io::Error::from(e))
}

#[cfg(test)]
mod tests;
