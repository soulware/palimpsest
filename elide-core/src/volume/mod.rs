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
    extentindex::{self, BodySource},
    gc_plan, lbamap,
    segment::{self, EntryKind},
    segment_cache,
    ulid_mint::UlidMint,
    writelog,
};

mod compress;
mod fork;
mod jobs;
mod read;
mod readonly;
mod reclaim;
mod repack;
mod wal;

pub(crate) use compress::maybe_compress;
#[cfg(test)]
pub(in crate::volume) use compress::{
    ENTROPY_THRESHOLD, MIN_COMPRESSION_RATIO_DEN, MIN_COMPRESSION_RATIO_NUM, shannon_entropy,
};
pub use fork::{fork_volume, fork_volume_at, fork_volume_at_with_manifest_key};
use jobs::GcCheckpointUlids;
pub use jobs::{
    GcCheckpointPrep, GcPlanApplyJob, GcPlanApplyResult, PromoteJob, PromoteResult,
    PromoteSegmentJob, PromoteSegmentPrep, PromoteSegmentResult, SignSnapshotManifestJob,
    SignSnapshotManifestResult, WorkerJob, WorkerResult,
};
#[cfg(test)]
pub(in crate::volume) use read::SegmentLayout;
pub(crate) use read::{FileCache, find_segment_in_dirs, open_delta_body_in_dirs, read_extents};
use readonly::open_read_state;
pub use readonly::{
    ReadonlyVolume, latest_snapshot, resolve_ancestor_dir, verify_ancestor_manifests,
    walk_ancestors, walk_extent_ancestors,
};
pub use reclaim::{
    ReclaimCandidate, ReclaimJob, ReclaimOutcome, ReclaimResult, ReclaimThresholds, ReclaimedEntry,
    scan_reclaim_candidates,
};
pub use repack::{
    CompactionStats, DeltaRepackJob, DeltaRepackResult, DeltaRepackStats, DeltaRepackedSegment,
    RepackJob, RepackResult, RepackedDeadEntry, RepackedLiveEntry, RepackedSegment, SweepJob,
    SweepResult, SweptDeadEntry, SweptLiveEntry,
};
use wal::{create_fresh_wal, recover_wal, replay_wal_records};

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

/// Default capacity for the parsed segment-index LRU cache. Each cached
/// entry holds `Vec<SegmentEntry>` for one segment (a few tens of KiB
/// for a 32 MiB segment); 64 entries comfortably covers the working set
/// for sweep/repack/delta_repack/promote passes without unbounded
/// memory growth on large volumes.
const SEGMENT_INDEX_CACHE_CAPACITY: usize = 64;

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
    pub(in crate::volume) base_dir: PathBuf,
    /// Ancestor fork layers, oldest-first. Does not include the current fork.
    pub(in crate::volume) ancestor_layers: Vec<AncestorLayer>,
    /// Exclusive lock on `base_dir/volume.lock`. Held for the lifetime of the Volume.
    /// The `Flock` releases the lock automatically when dropped.
    #[allow(dead_code)]
    lock_file: nix::fcntl::Flock<fs::File>,
    pub(in crate::volume) lbamap: Arc<lbamap::LbaMap>,
    pub(in crate::volume) extent_index: Arc<extentindex::ExtentIndex>,
    /// Lazy WAL state. `None` means no WAL file exists on disk — the next
    /// write opens a fresh one at `mint.next()`. Keeps idle volumes from
    /// churning the WAL on every GC tick.
    pub(in crate::volume) wal: Option<OpenWal>,
    /// DATA and REF extents written since the last promotion; used to write
    /// the clean segment file on the next promote().
    pub(in crate::volume) pending_entries: Vec<segment::SegmentEntry>,
    /// True if at least one segment has been committed since the last snapshot
    /// (or since open, if no snapshot has been taken this session). Used by
    /// `snapshot()` to decide whether a new marker is needed or the latest
    /// existing snapshot can be reused.
    pub(in crate::volume) has_new_segments: bool,
    /// ULID of the most recently committed segment across pending/ and index/,
    /// or `None` if no segments exist. Used by `snapshot()` to name the snapshot
    /// marker with the same ULID as the segment it covers.
    pub(in crate::volume) last_segment_ulid: Option<Ulid>,
    /// LRU cache of open segment file handles for the read path.
    ///
    /// Retains recently-opened segment files across `read` calls so that
    /// reads hitting the same segments avoid repeated `open` syscalls.
    /// `RefCell` keeps `read` logically non-mutating (`&self`) while allowing
    /// the cache to be updated internally.
    pub(in crate::volume) file_cache: RefCell<FileCache>,
    /// Signer for segment promotion. Every segment written by this volume
    /// (at WAL promotion and compaction) is signed with the fork's private key.
    /// See `segment::SegmentSigner`.
    pub(in crate::volume) signer: Arc<dyn segment::SegmentSigner>,
    /// Verifying key derived from `volume.key` at open time. Used to verify
    /// segment signatures when reading during compaction and GC.
    pub(in crate::volume) verifying_key: ed25519_dalek::VerifyingKey,
    /// Optional fetcher for demand-fetch on segment cache miss. When set,
    /// `find_segment_file` fetches missing segments from remote storage and
    /// caches them in `cache/`. See `segment::SegmentFetcher`.
    pub(in crate::volume) fetcher: Option<BoxFetcher>,
    /// Monotonic ULID generator. Seeded from the highest known ULID at open
    /// (WAL filename or max segment). Used for all WAL and compaction outputs
    /// to guarantee strict ordering regardless of host clock behaviour.
    pub(in crate::volume) mint: UlidMint,
    /// Stats for the no-op write skip path (LBA-map hash compare).
    /// See `docs/design-noop-write-skip.md`.
    pub(in crate::volume) noop_stats: NoopSkipStats,
    /// Shared LRU of parsed+verified segment indices. Keyed by
    /// `(path, file_len)`. Cloned into worker jobs so the actor thread
    /// and the worker thread hit the same cache. See
    /// `segment_cache::SegmentIndexCache`.
    pub(in crate::volume) segment_cache: Arc<segment_cache::SegmentIndexCache>,
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

            // Install the in-memory mirror of the all-bits-set
            // `cache/<ulid>.present` that `promote_to_cache` just
            // wrote. The hot read path uses this for the per-entry
            // presence check on `BodyOnly` cache hits — without it
            // the rebuild-on-startup path would be the only source,
            // which is too late for live drains.
            Arc::make_mut(&mut self.extent_index).set_segment_presence(
                ulid,
                Arc::new(extentindex::SegmentPresence::from_data_kinds(&entries)),
            );

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

            // GC carried entries already reference `BodySource::Cached(idx)`
            // against `ulid` (planted by `apply_gc_handoffs`); now that
            // `promote_to_cache` has produced `cache/<ulid>.body` +
            // `cache/<ulid>.present`, install the in-memory presence
            // mirror so reads against the new cache shape succeed
            // without consulting `.present` on disk.
            Arc::make_mut(&mut self.extent_index).set_segment_presence(
                ulid,
                Arc::new(extentindex::SegmentPresence::from_data_kinds(&entries)),
            );
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
    pub(in crate::volume) fn evict_cached_segment(&self, segment_id: Ulid) {
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
        // The manifest's existence under `snapshots/` is the
        // snapshot's existence; `write_snapshot_manifest` writes
        // atomically, so a partial sequence leaves no snapshot visible.
        crate::signing::write_snapshot_manifest(
            &self.base_dir,
            self.signer.as_ref(),
            &snap_ulid,
            &index_ulids,
            None,
        )?;
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
            &self.extent_index,
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
pub(in crate::volume) mod test_util;
#[cfg(test)]
mod tests;
