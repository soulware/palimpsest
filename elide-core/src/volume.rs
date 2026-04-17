// Volume: top-level I/O interface — owns the LBA map, WAL, and directory layout.
//
// Directory layout:
//   <base>/wal/       — active write-ahead log (at most one file at a time)
//   <base>/pending/   — promoted segments awaiting S3 upload
//   <base>/index/     — coordinator-written LBA index files (*.idx); permanent; never evicted
//   <base>/cache/     — coordinator-written body cache (*.body, *.present); evictable
//   <base>/gc/        — coordinator GC handoff files (*.pending → *.applied → *.done)
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
//   3. find_segment_file (wal/ → pending/ → gc/*.applied → cache/<id>.body) → open file, seek, read
//
// Recovery:
//   Volume::open() calls lbamap::rebuild_segments() (segments only), then
//   scans the WAL once: that single pass truncates any partial-tail record,
//   replays entries into the LBA map, extent index, and pending_entries.
//   Any .tmp files in pending/ are removed (incomplete promotions).

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use segment::BoxFetcher;

use ulid::Ulid;

use crate::{
    extentindex::{self, BodySource},
    lbamap,
    segment::{self, EntryKind},
    ulid_mint::UlidMint,
    writelog,
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
fn maybe_compress(data: &[u8]) -> Option<Vec<u8>> {
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

/// Results from a single compaction run.
#[derive(Debug, Default)]
pub struct CompactionStats {
    /// Number of input segments consumed (deleted after compaction).
    pub segments_compacted: usize,
    /// Number of output segments written.
    pub new_segments: usize,
    /// Stored bytes reclaimed from deleted segment bodies.
    pub bytes_freed: u64,
    /// Number of dead extent entries removed from the extent index.
    pub extents_removed: usize,
}

/// Stats from a single `delta_repack_post_snapshot` pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct DeltaRepackStats {
    /// Number of post-snapshot segments inspected.
    pub segments_scanned: usize,
    /// Number of segments actually rewritten (had at least one conversion).
    pub segments_rewritten: usize,
    /// Total Data→Delta conversions across all rewritten segments.
    pub entries_converted: usize,
    /// Sum of original `stored_length` for converted entries.
    pub original_body_bytes: u64,
    /// Sum of delta blob sizes written.
    pub delta_body_bytes: u64,
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
    wal: writelog::WriteLog,
    wal_ulid: Ulid,
    wal_path: PathBuf,
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
}

// ---------------------------------------------------------------------------
// Worker offload types
// ---------------------------------------------------------------------------

/// Data needed by the worker thread to write a pending segment.
///
/// Produced by [`Volume::prepare_promote`] on the actor thread, consumed by
/// the worker thread which calls [`segment::write_and_commit`].  All fields
/// are `Send` so the struct can cross a thread boundary.
pub struct PromoteJob {
    pub segment_ulid: Ulid,
    pub old_wal_ulid: Ulid,
    pub old_wal_path: PathBuf,
    pub entries: Vec<segment::SegmentEntry>,
    /// CAS precondition tokens: the `body_offset` each Data/Inline entry
    /// had in the extent index at prep time.  `None` for DedupRef/Zero/Delta.
    pub pre_promote_offsets: Vec<Option<u64>>,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub pending_dir: PathBuf,
}

/// Result returned by the worker thread after writing the segment.
///
/// Consumed by [`Volume::apply_promote`] on the actor thread.
pub struct PromoteResult {
    pub segment_ulid: Ulid,
    pub old_wal_ulid: Ulid,
    pub old_wal_path: PathBuf,
    pub body_section_start: u64,
    pub entries: Vec<segment::SegmentEntry>,
    pub pre_promote_offsets: Vec<Option<u64>>,
}

/// Result of the GC checkpoint prep phase.
///
/// Carries the pre-minted ULIDs and an optional promote job.  The actor
/// dispatches the job to the worker and stashes the reply.  When `job`
/// is `None` the WAL was empty and the checkpoint completes immediately.
pub struct GcCheckpointPrep {
    pub u_repack: Ulid,
    pub u_sweep: Ulid,
    /// Segment ULID used for the promoted WAL.  Used to identify the
    /// GC promote's `PromoteComplete` among other in-flight promotes.
    pub u_flush: Ulid,
    pub job: Option<PromoteJob>,
}

/// Data needed by the worker thread to re-sign a GC handoff segment.
///
/// Produced by the actor's scan phase (directory listing of `gc/*.staged`).
/// The worker reads the staged segment, reads each input's `.idx` file,
/// re-signs the segment with the volume key, and returns a
/// [`GcHandoffResult`] for the actor to apply.
pub struct GcHandoffJob {
    pub staged_path: PathBuf,
    pub new_ulid: Ulid,
    pub gc_dir: PathBuf,
    pub index_dir: PathBuf,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
}

/// Result returned by the worker thread after re-signing a GC handoff.
///
/// The actor derives the action set (to_remove, stale_cancel, carried
/// updates) against the **current** extent index and lbamap — not the
/// state at dispatch time — to handle concurrent writes correctly.
pub struct GcHandoffResult {
    pub new_ulid: Ulid,
    pub staged_path: PathBuf,
    pub gc_dir: PathBuf,
    pub new_bss: u64,
    pub entries: Vec<segment::SegmentEntry>,
    pub inputs: Vec<Ulid>,
    /// Body-owning entries from each input's `.idx` file.
    /// `(hash, kind, input_ulid)` — used by the apply phase to build the
    /// to_remove and stale_cancel sets against the current extent index.
    pub input_old_entries: Vec<(blake3::Hash, segment::EntryKind, Ulid)>,
    /// Inline bytes from the staged segment, needed for building
    /// `inline_data` in extent locations during the apply phase.
    pub handoff_inline: Vec<u8>,
}

/// Data needed by the worker thread to promote a confirmed-in-S3 segment
/// from `pending/<ulid>` (drain path) or `gc/<ulid>` (GC path) into
/// `cache/<ulid>.{body,present}` + `index/<ulid>.idx`.
///
/// Produced by [`Volume::prepare_promote_segment`] on the actor thread.
/// The worker reads and verifies the segment index once, then writes idx
/// and cache body (both operations idempotent on retry), and returns the
/// parsed state the actor's apply phase needs for extent-index updates.
pub struct PromoteSegmentJob {
    pub ulid: Ulid,
    /// Full path of the source segment — `pending/<ulid>` if `is_drain`,
    /// otherwise `gc/<ulid>`.
    pub src_path: PathBuf,
    /// True when the source is in `pending/`, false when in `gc/`.
    /// Selects the apply-phase branch (Local→Cached CAS + pending delete
    /// vs input-idx cleanup).
    pub is_drain: bool,
    pub body_path: PathBuf,
    pub present_path: PathBuf,
    pub idx_path: PathBuf,
    pub verifying_key: ed25519_dalek::VerifyingKey,
}

/// Result returned by the worker after a `PromoteSegmentJob`.
///
/// Consumed by [`Volume::apply_promote_segment_result`] on the actor
/// thread. Reuses the parsed segment index so the apply phase never
/// re-reads the segment file.
pub struct PromoteSegmentResult {
    pub ulid: Ulid,
    pub is_drain: bool,
    pub body_section_start: u64,
    pub entries: Vec<segment::SegmentEntry>,
    /// Consumed input ULIDs for the GC path. Empty on drain.
    pub inputs: Vec<Ulid>,
    /// Inline section bytes. Populated only when the drain path has
    /// Inline entries; empty otherwise.
    pub inline: Vec<u8>,
    /// True when the worker took the GC tombstone shortcut (zero-entry
    /// output with a non-empty inputs list). Apply phase deletes the
    /// input idx files and stops — no idx/body was written.
    pub tombstone: bool,
}

/// Prep-phase outcome for `promote_segment`.
///
/// `AlreadyPromoted` short-circuits the apply phase: both `pending/<ulid>`
/// and `gc/<ulid>` are absent but `cache/<ulid>.body` exists, meaning an
/// earlier call already completed. `Job` carries the work for the worker
/// thread to execute — boxed because `PromoteSegmentJob` is large compared
/// to the unit `AlreadyPromoted` variant.
pub enum PromoteSegmentPrep {
    Job(Box<PromoteSegmentJob>),
    AlreadyPromoted,
}

/// Job dispatched from the actor to the worker thread.
pub enum WorkerJob {
    Promote(PromoteJob),
    GcHandoff(GcHandoffJob),
    PromoteSegment(PromoteSegmentJob),
}

/// Result returned by the worker thread to the actor.
///
/// Each variant wraps its own `io::Result` so the actor can distinguish
/// which job type failed.  `PromoteSegment` carries the target ULID
/// out-of-band so the actor can match a failed job to its parked reply
/// (the `Err` path otherwise has no ULID to match on).
pub enum WorkerResult {
    Promote(io::Result<PromoteResult>),
    GcHandoff(io::Result<GcHandoffResult>),
    PromoteSegment {
        ulid: Ulid,
        result: io::Result<PromoteSegmentResult>,
    },
}

/// Snapshot captured at reclaim phase 1. Carries the target range, a
/// clone of the current `Arc<LbaMap>` (used as the precondition token in
/// phase 3 and as the read source for bloat detection in phase 2), and
/// the clipped entries covering the target range at capture time.
///
/// `lbamap_snapshot` is kept private: the pointer identity is the entire
/// precondition check, and exposing it would invite accidental aliasing
/// that weakens the guarantee.
pub struct ReclaimPlan {
    target_start_lba: u64,
    target_lba_length: u32,
    entries: Vec<lbamap::ExtentRead>,
    lbamap_snapshot: Arc<lbamap::LbaMap>,
}

impl std::fmt::Debug for ReclaimPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReclaimPlan")
            .field("target_start_lba", &self.target_start_lba)
            .field("target_lba_length", &self.target_lba_length)
            .field("entries", &self.entries.len())
            .finish()
    }
}

/// A single rewrite proposal: a fresh compact entry to commit at phase 3.
#[derive(Debug, Clone)]
pub struct ReclaimProposed {
    pub start_lba: u64,
    pub data: Vec<u8>,
    pub hash: blake3::Hash,
}

/// Outcome of a complete alias-merge reclaim pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimOutcome {
    /// True if the phase-3 precondition failed (the LBA map was mutated
    /// between snapshot and commit) and nothing was committed.
    pub discarded: bool,
    /// Number of rewrite proposals committed (excluding ones the noop-skip
    /// hash check absorbed because the LBA map already records the rewrite).
    pub runs_rewritten: u32,
    /// Total bytes committed to fresh compact entries.
    pub bytes_rewritten: u64,
}

impl ReclaimPlan {
    /// The target range captured at phase 1, as `(start_lba, lba_length)`.
    pub fn target(&self) -> (u64, u32) {
        (self.target_start_lba, self.target_lba_length)
    }

    /// Clipped view of the LBA map over the target range at phase 1.
    pub fn entries(&self) -> &[lbamap::ExtentRead] {
        &self.entries
    }

    /// Phase 2 of extent reclamation: compute rewrite proposals for this
    /// plan. The `read` callback produces bytes for a given LBA range —
    /// supply `VolumeHandle::read` or `Volume::read` depending on whether
    /// phase 2 is running off-actor (the intended shape, see
    /// `docs/design-extent-reclamation.md § Optimistic commit structure`)
    /// or on the actor thread (acceptable in tests).
    ///
    /// Every rewrite decision is made entirely against the plan's captured
    /// map snapshot; the current volume state is not inspected. That is
    /// what makes this safe to run off-actor without holding any lock —
    /// any interleaved mutation is caught by phase 3's precondition check.
    ///
    /// Two predicates gate each candidate extent:
    /// 1. **Containment** — every LBA map run for this hash (not just the
    ///    in-range ones) must fall inside the target range. Rewriting a
    ///    hash whose body is partially referenced from outside the target
    ///    would leave those outside references on the now-bloated body
    ///    and *introduce* waste rather than eliminate it.
    /// 2. **Bloat** — at least one run for the hash has
    ///    `payload_block_offset != 0`, which is a strong signal that a
    ///    prior write split the original payload and dead bytes exist
    ///    inside the stored body.
    ///
    /// `ZERO_HASH` is always skipped: zero extents carry no body, so
    /// "rewriting" them would invent a body for bytes that never had one.
    pub fn compute_rewrites<F>(&self, mut read: F) -> io::Result<Vec<ReclaimProposed>>
    where
        F: FnMut(u64, u32) -> io::Result<Vec<u8>>,
    {
        let target_start = self.target_start_lba;
        let target_end = target_start + self.target_lba_length as u64;

        // Cache containment/bloat decisions per hash so repeated runs of
        // the same hash inside the target share one full-map walk.
        let mut decision: HashMap<blake3::Hash, bool> = HashMap::new();

        let mut proposed = Vec::new();
        for er in &self.entries {
            if er.hash == ZERO_HASH {
                continue;
            }
            let should_rewrite = *decision.entry(er.hash).or_insert_with(|| {
                let runs = self.lbamap_snapshot.runs_for_hash(&er.hash);
                let contained = runs.iter().all(|(lba, length, _offset)| {
                    *lba >= target_start && *lba + *length as u64 <= target_end
                });
                if !contained {
                    return false;
                }
                runs.iter().any(|(_, _, offset)| *offset != 0)
            });
            if !should_rewrite {
                continue;
            }
            let length_blocks = (er.range_end - er.range_start) as u32;
            let bytes = read(er.range_start, length_blocks)?;
            let expected_len = length_blocks as usize * 4096;
            if bytes.len() != expected_len {
                return Err(io::Error::other(format!(
                    "reclaim read returned {} bytes, expected {expected_len}",
                    bytes.len()
                )));
            }
            let hash = blake3::hash(&bytes);
            proposed.push(ReclaimProposed {
                start_lba: er.range_start,
                data: bytes,
                hash,
            });
        }
        Ok(proposed)
    }
}

/// Per-hash thresholds controlling which hashes the reclamation scanner
/// proposes as worth rewriting. All defaults are placeholders pending
/// empirical tuning on real aged volumes — see the open questions in
/// `docs/design-extent-reclamation.md § Measurement before mechanism`.
#[derive(Debug, Clone, Copy)]
pub struct ReclaimThresholds {
    /// Minimum number of 4K blocks detectably dead inside a hash's stored
    /// payload before the hash is a candidate. Small waste isn't worth the
    /// rewrite cost.
    pub min_dead_blocks: u32,
    /// Minimum `dead / total` ratio. `payload_block_offset` aliasing
    /// already serves reads without decompress-to-discard below this
    /// ratio, so rewriting is pure write amplification.
    pub min_dead_ratio: f64,
    /// Minimum stored body size. Rewriting a tiny entry amortises badly
    /// over the WAL-append + extent_index-update overhead.
    pub min_stored_bytes: u64,
}

impl Default for ReclaimThresholds {
    fn default() -> Self {
        Self {
            min_dead_blocks: 8,
            min_dead_ratio: 0.3,
            min_stored_bytes: 64 * 1024,
        }
    }
}

/// A single reclamation candidate identified by the scanner. The caller
/// passes `(start_lba, lba_length)` to
/// [`crate::actor::VolumeHandle::reclaim_alias_merge`].
///
/// The range is chosen to tightly cover every LBA map run for this
/// hash. The primitive's containment check therefore always succeeds
/// for this hash — but the range may also sweep in other, unrelated
/// hashes that happen to sit between this hash's runs; those are left
/// alone by the primitive's own per-hash containment check.
#[derive(Debug, Clone, Copy)]
pub struct ReclaimCandidate {
    pub start_lba: u64,
    pub lba_length: u32,
    /// Detectable dead block count for this hash's stored payload.
    pub dead_blocks: u32,
    /// Sum of live block lengths across all runs that reference this hash.
    pub live_blocks: u32,
    /// Stored body length in bytes (compressed if the payload was compressed).
    pub stored_bytes: u64,
    /// `true` if the stored payload is compressed and the dead count is
    /// a lower bound rather than exact (we can't know trailing-dead bytes
    /// inside a compressed payload without decompressing).
    pub dead_count_is_lower_bound: bool,
}

/// Walk the LBA map, fold per-hash run lists, and emit reclamation
/// candidates that clear all three thresholds in `ReclaimThresholds`.
///
/// The scanner is read-only and takes `&LbaMap` / `&ExtentIndex` so it
/// can run on a [`crate::actor::VolumeHandle`] snapshot without any
/// actor round-trip. Returned candidates are sorted by `dead_blocks`
/// descending (the most wasteful rewrites first).
///
/// **Dead-block detection:** for each hash H we compute
/// `live_blocks = sum(run.length)` and
/// `max_payload_end = max(run.offset + run.length)` across all runs.
/// For uncompressed payloads the exact logical length is
/// `body_length / 4096` and `dead_blocks = logical_length - live_blocks`.
/// For compressed payloads the exact logical length is unknown without
/// decompressing, so we use `max_payload_end - live_blocks` — a lower
/// bound that never produces false positives but may miss dead bytes
/// past the last observed run. Zero-extents, hashes absent from the
/// extent index, and delta-source hashes are skipped.
pub fn scan_reclaim_candidates(
    lbamap: &lbamap::LbaMap,
    extent_index: &extentindex::ExtentIndex,
    thresholds: ReclaimThresholds,
) -> Vec<ReclaimCandidate> {
    // Per-hash aggregate: (min_lba, max_lba_end, sum_live_blocks, max_offset_end)
    #[derive(Clone, Copy)]
    struct HashAgg {
        min_lba: u64,
        max_lba_end: u64,
        live_blocks: u64,
        max_offset_end: u64,
    }

    let mut per_hash: HashMap<blake3::Hash, HashAgg> = HashMap::new();
    for (lba, length, hash, offset) in lbamap.iter_entries() {
        if hash == ZERO_HASH {
            continue;
        }
        let lba_end = lba + length as u64;
        let offset_end = offset as u64 + length as u64;
        per_hash
            .entry(hash)
            .and_modify(|agg| {
                if lba < agg.min_lba {
                    agg.min_lba = lba;
                }
                if lba_end > agg.max_lba_end {
                    agg.max_lba_end = lba_end;
                }
                agg.live_blocks += length as u64;
                if offset_end > agg.max_offset_end {
                    agg.max_offset_end = offset_end;
                }
            })
            .or_insert(HashAgg {
                min_lba: lba,
                max_lba_end: lba_end,
                live_blocks: length as u64,
                max_offset_end: offset_end,
            });
    }

    let mut candidates = Vec::new();
    for (hash, agg) in &per_hash {
        let Some(loc) = extent_index.lookup(hash) else {
            continue;
        };
        // Inline entries are small by construction and do not benefit
        // from compaction — their bytes already live in the .idx, not
        // the body section.
        if loc.inline_data.is_some() {
            continue;
        }

        // Determine the payload's logical block count. For uncompressed
        // payloads it's exact; for compressed we use the highest observed
        // payload offset as a lower bound.
        let (logical_blocks, is_lower_bound) = if loc.compressed {
            (agg.max_offset_end, true)
        } else {
            (loc.body_length as u64 / 4096, false)
        };
        if logical_blocks < agg.live_blocks {
            // Can happen for compressed payloads when max_offset_end
            // underestimates — treat as "no detectable bloat".
            continue;
        }
        let dead_blocks = logical_blocks - agg.live_blocks;
        if dead_blocks < u64::from(thresholds.min_dead_blocks) {
            continue;
        }
        if (loc.body_length as u64) < thresholds.min_stored_bytes {
            continue;
        }
        let dead_ratio = dead_blocks as f64 / logical_blocks as f64;
        if dead_ratio < thresholds.min_dead_ratio {
            continue;
        }
        let lba_length = agg.max_lba_end - agg.min_lba;
        if lba_length > u32::MAX as u64 {
            // Pathological: wouldn't fit in a single reclaim call. Skip.
            continue;
        }
        candidates.push(ReclaimCandidate {
            start_lba: agg.min_lba,
            lba_length: lba_length as u32,
            dead_blocks: dead_blocks.min(u32::MAX as u64) as u32,
            live_blocks: agg.live_blocks.min(u32::MAX as u64) as u32,
            stored_bytes: loc.body_length as u64,
            dead_count_is_lower_bound: is_lower_bound,
        });
    }

    candidates.sort_unstable_by(|a, b| b.dead_blocks.cmp(&a.dead_blocks));
    candidates
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
        // A GC output in .applied state has a ULID = max(inputs).increment(),
        // which may be the highest known ULID — include it so the mint floor is correct.
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
                    EntryKind::Data | EntryKind::Inline => {
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
                    EntryKind::Data | EntryKind::Inline => {}
                    EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
                }
                let Some(old_wal_offset) = old_wal_offset else {
                    continue;
                };
                let idata = if entry.kind == EntryKind::Inline {
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
        let (wal, wal_ulid, wal_path, pending_entries) =
            if let Some(path) = wal_files.into_iter().last() {
                recover_wal(path, &mut lbamap, &mut extent_index)?
            } else {
                create_fresh_wal(&wal_dir, mint.next())?
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
            wal_ulid,
            wal_path,
            pending_entries,
            has_new_segments,
            last_segment_ulid,
            file_cache: RefCell::new(FileCache::default()),
            signer,
            verifying_key,
            fetcher: None,
            mint,
            noop_stats: NoopSkipStats::default(),
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
            self.wal.append_ref(lba, lba_length, &hash)?;
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

        let body_offset = self
            .wal
            .append_data(lba, lba_length, &hash, wal_flags, &owned_data)?;
        Arc::make_mut(&mut self.lbamap).insert(lba, lba_length, hash);
        // Temporary extent index entry: points into the WAL at the raw payload offset.
        // Updated to segment file offsets after promotion.
        Arc::make_mut(&mut self.extent_index).insert(
            hash,
            extentindex::ExtentLocation {
                segment_id: self.wal_ulid,
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
        self.wal.append_zero(start_lba, lba_count)?;
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

    /// Flush buffered WAL writes and fsync to disk.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.wal.fsync()
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
    pub fn repack(&mut self, min_live_ratio: f64) -> io::Result<CompactionStats> {
        use std::collections::HashSet;

        let live: HashSet<blake3::Hash> = self.lbamap.lba_referenced_hashes();
        let mut stats = CompactionStats::default();

        // Segments at or below the latest snapshot ULID are frozen: they may be
        // referenced by child forks that branched from a snapshot in this fork.
        // Only post-snapshot segments are eligible for compaction.
        let floor: Option<Ulid> = latest_snapshot(&self.base_dir)?;

        let all_segs = segment::collect_segment_files(&self.base_dir.join("pending"))?;

        for seg_path in all_segs {
            let seg_id = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad segment filename"))?;
            let seg_id = Ulid::from_string(seg_id).map_err(|e| io::Error::other(e.to_string()))?;

            // Skip segments frozen by the latest snapshot.
            if floor.is_some_and(|f| seg_id <= f) {
                continue;
            }

            let (body_section_start, mut entries, _inputs) =
                match segment::read_and_verify_segment_index(&seg_path, &self.verifying_key) {
                    Ok(v) => v,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };

            // DATA and Inline entries have real stored bytes.
            // DedupRef body regions are zero-filled; Zero has stored_length=0.
            let total_bytes: u64 = entries
                .iter()
                .filter(|e| matches!(e.kind, EntryKind::Data | EntryKind::Inline))
                .map(|e| e.stored_length as u64)
                .sum();

            if total_bytes == 0 {
                continue;
            }

            let live_bytes: u64 = entries
                .iter()
                .filter(|e| {
                    matches!(e.kind, EntryKind::Data | EntryKind::Inline) && live.contains(&e.hash)
                })
                .map(|e| e.stored_length as u64)
                .sum();

            if live_bytes as f64 / total_bytes as f64 >= min_live_ratio {
                continue;
            }

            let (mut live_entries, dead_entries): (Vec<_>, Vec<_>) =
                entries.drain(..).partition(|e| match e.kind {
                    EntryKind::Zero => self.lbamap.hash_at(e.start_lba) == Some(ZERO_HASH),
                    EntryKind::DedupRef | EntryKind::Delta => {
                        self.lbamap.hash_at(e.start_lba) == Some(e.hash)
                    }
                    EntryKind::Data | EntryKind::Inline => live.contains(&e.hash),
                });

            // Remove dead entries from the extent index (only those pointing at
            // this segment — entries pointing elsewhere belong to another copy).
            // Thin DedupRef, Zero, and Delta entries are not in the extent index.
            let mut removed = 0usize;
            for entry in &dead_entries {
                if matches!(
                    entry.kind,
                    EntryKind::Zero | EntryKind::DedupRef | EntryKind::Delta
                ) {
                    continue;
                }
                if self
                    .extent_index
                    .lookup(&entry.hash)
                    .map(|loc| loc.segment_id == seg_id)
                    .unwrap_or(false)
                {
                    Arc::make_mut(&mut self.extent_index).remove(&entry.hash);
                    removed += 1;
                }
            }

            // Evict the old segment from the file handle cache before
            // replacing or deleting it.
            self.evict_cached_segment(seg_id);

            if !live_entries.is_empty() {
                // Read body bytes for live entries (Data) and inline data (Inline).
                // DedupRef regions are zero-filled placeholders in pending/ and are
                // re-emitted as zeros by write_segment.
                let inline_bytes = segment::read_inline_section(&seg_path)?;
                segment::read_extent_bodies(
                    &seg_path,
                    body_section_start,
                    &mut live_entries,
                    [EntryKind::Data, EntryKind::Inline],
                    &inline_bytes,
                )?;

                // Reuse the source segment's own ULID for the output.  This
                // guarantees the output ULID < the current WAL ULID (all segments
                // predate the current WAL), so a subsequent WAL flush always
                // produces a higher ULID and wins on rebuild.  Using mint.next()
                // here would generate a ULID past the WAL ULID and break that
                // ordering — the same bug fixed in sweep_pending.
                let new_ulid = seg_id;
                let new_ulid_str = new_ulid.to_string();
                let pending_dir = self.base_dir.join("pending");
                let tmp_path = pending_dir.join(format!("{new_ulid_str}.tmp"));
                let final_path = pending_dir.join(&new_ulid_str);
                // write_segment reassigns stored_offset in live_entries to new positions.
                let new_bss =
                    segment::write_segment(&tmp_path, &mut live_entries, self.signer.as_ref())?;
                // Atomically replaces the original segment file.
                fs::rename(&tmp_path, &final_path)?;
                segment::fsync_dir(&final_path)?;
                stats.new_segments += 1;

                for entry in &live_entries {
                    match entry.kind {
                        EntryKind::Data => {
                            Arc::make_mut(&mut self.extent_index).insert(
                                entry.hash,
                                extentindex::ExtentLocation {
                                    segment_id: new_ulid,
                                    body_offset: entry.stored_offset,
                                    body_length: entry.stored_length,
                                    compressed: entry.compressed,
                                    body_source: BodySource::Local,
                                    body_section_start: new_bss,
                                    inline_data: None,
                                },
                            );
                        }
                        EntryKind::Inline => {
                            Arc::make_mut(&mut self.extent_index).insert(
                                entry.hash,
                                extentindex::ExtentLocation {
                                    segment_id: new_ulid,
                                    body_offset: entry.stored_offset,
                                    body_length: entry.stored_length,
                                    compressed: entry.compressed,
                                    body_source: BodySource::Local,
                                    body_section_start: new_bss,
                                    inline_data: entry.data.clone().map(Vec::into_boxed_slice),
                                },
                            );
                        }
                        EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => {}
                    }
                }
            } else {
                // All entries are dead — delete the segment file. Without
                // this, a subsequent drain would try to process DedupRef
                // entries whose canonical hashes we just removed from the
                // extent index.
                fs::remove_file(&seg_path)?;
                segment::fsync_dir(&seg_path)?;
            }

            stats.segments_compacted += 1;
            stats.bytes_freed += total_bytes - live_bytes;
            stats.extents_removed += removed;
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
    pub fn delta_repack_post_snapshot(&mut self) -> io::Result<DeltaRepackStats> {
        use crate::block_reader::BlockReader;
        use crate::delta_compute;

        let mut stats = DeltaRepackStats::default();

        // No prior snapshot → no source for deltas. Bail cleanly.
        let Some(latest_snap) = latest_snapshot(&self.base_dir)? else {
            return Ok(stats);
        };

        // Snapshot-pinned reader on the prior sealed snapshot. We pass
        // a `None` fetcher: delta repack is best-effort, and if a
        // source body is evicted locally we skip it rather than pull
        // bytes off S3 just to seed a dictionary.
        let prior = BlockReader::open_snapshot(&self.base_dir, &latest_snap, Box::new(|_| None))?;

        let all_segs = segment::collect_segment_files(&self.base_dir.join("pending"))?;
        let vk = self.verifying_key;

        for seg_path in all_segs {
            let seg_id = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad segment filename"))?;
            let seg_id = Ulid::from_string(seg_id).map_err(|e| io::Error::other(e.to_string()))?;

            // Skip segments at or below the latest snapshot — they are
            // snapshot-frozen and must not be rewritten.
            if seg_id <= latest_snap {
                continue;
            }

            stats.segments_scanned += 1;

            // Evict cached file handle before rewriting in place.
            self.evict_cached_segment(seg_id);

            let rewritten = match delta_compute::rewrite_post_snapshot_with_prior(
                &seg_path,
                &prior,
                self.signer.as_ref(),
                &vk,
            ) {
                Ok(r) => r,
                Err(e) => {
                    log::warn!(
                        "delta_repack: seg {seg_id} rewrite failed: {e} — leaving segment unchanged"
                    );
                    continue;
                }
            };
            let Some((entries, new_bss, seg_stats)) = rewritten else {
                continue;
            };

            // Refresh the in-memory extent index for every entry in the
            // rewritten segment. Converted Delta entries need their
            // previous Data location dropped, then re-registered under
            // the deltas map. Non-converted Data/Inline entries need
            // their `stored_offset` refreshed — `write_segment_with_delta_body`
            // reassigned offsets when the body section shrank.
            //
            // The delta body section starts exactly at the end of the
            // body section, which is `body_section_start + body_length`
            // where body_length is the sum of remaining Data entries'
            // stored_length. Compute once for all Delta entries.
            let delta_region_body_length: u64 = entries
                .iter()
                .filter(|e| matches!(e.kind, EntryKind::Data))
                .map(|e| e.stored_length as u64)
                .sum();
            let ei = Arc::make_mut(&mut self.extent_index);
            for entry in entries.iter() {
                match entry.kind {
                    EntryKind::Data => {
                        ei.insert(
                            entry.hash,
                            extentindex::ExtentLocation {
                                segment_id: seg_id,
                                body_offset: entry.stored_offset,
                                body_length: entry.stored_length,
                                compressed: entry.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: None,
                            },
                        );
                    }
                    EntryKind::Inline => {
                        ei.insert(
                            entry.hash,
                            extentindex::ExtentLocation {
                                segment_id: seg_id,
                                body_offset: entry.stored_offset,
                                body_length: entry.stored_length,
                                compressed: entry.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: entry.data.clone().map(Vec::into_boxed_slice),
                            },
                        );
                    }
                    EntryKind::Delta => {
                        // Drop any stale Data location under this hash,
                        // then register the Delta entry so the reader
                        // finds it via `lookup_delta`.
                        ei.remove(&entry.hash);
                        ei.insert_delta_if_absent(
                            entry.hash,
                            extentindex::DeltaLocation {
                                segment_id: seg_id,
                                body_source: extentindex::DeltaBodySource::Full {
                                    body_section_start: new_bss,
                                    body_length: delta_region_body_length,
                                },
                                options: entry.delta_options.clone(),
                            },
                        );
                    }
                    EntryKind::DedupRef | EntryKind::Zero => {}
                }
            }

            log::info!(
                "delta_repack: seg {seg_id} converted {}/{} entries, {}→{} bytes",
                seg_stats.entries_converted,
                entries.len(),
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

    /// Minimum segment file size below which a `pending/` segment is always a
    /// merge candidate regardless of its live ratio.
    const COMPACT_SMALL_THRESHOLD: u64 = 8 * 1024 * 1024;

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
    pub fn sweep_pending(&mut self) -> io::Result<CompactionStats> {
        use std::collections::HashSet;

        let live: HashSet<blake3::Hash> = self.lbamap.lba_referenced_hashes();
        let mut stats = CompactionStats::default();

        let floor: Option<Ulid> = latest_snapshot(&self.base_dir)?;

        let pending_dir = self.base_dir.join("pending");
        let mut seg_paths = segment::collect_segment_files(&pending_dir)?;
        // Sort by filename (ULID) ascending so entries appear oldest-first in
        // the merged output.  rebuild_segments applies entries in sequence and
        // the last entry wins for each LBA, so this guarantees the most-recent
        // write takes precedence even when two candidates both cover the same LBA
        // with the same data hash (hash-based liveness keeps both alive but
        // ordering ensures the correct one survives crash+rebuild).
        seg_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

        let mut candidate_paths: Vec<std::path::PathBuf> = Vec::new();
        let mut merged_live: Vec<segment::SegmentEntry> = Vec::new();
        let mut any_dead = false;

        for seg_path in &seg_paths {
            let seg_filename = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad segment filename"))?;
            let seg_ulid =
                Ulid::from_string(seg_filename).map_err(|e| io::Error::other(e.to_string()))?;

            if floor.is_some_and(|f| seg_ulid <= f) {
                continue;
            }

            let file_size = fs::metadata(seg_path)?.len();
            let (body_section_start, mut entries, _inputs) =
                segment::read_and_verify_segment_index(seg_path, &self.verifying_key)?;

            let has_dead = entries.iter().any(|e| !live.contains(&e.hash));
            let is_small = file_size < Self::COMPACT_SMALL_THRESHOLD;

            if !has_dead && !is_small {
                continue;
            }

            if has_dead {
                any_dead = true;
            }

            let (live_entries, dead_entries): (Vec<_>, Vec<_>) =
                entries.drain(..).partition(|e| match e.kind {
                    EntryKind::DedupRef => {
                        // A dedup ref is only live if the LBA still maps to
                        // this hash. If the LBA was overwritten with different
                        // data, carrying the stale ref would reintroduce the
                        // old mapping after crash + rebuild.
                        self.lbamap.hash_at(e.start_lba) == Some(e.hash)
                    }
                    _ => live.contains(&e.hash),
                });

            let dead_bytes: u64 = dead_entries.iter().map(|e| e.stored_length as u64).sum();

            for entry in &dead_entries {
                if entry.kind == EntryKind::Zero || entry.kind == EntryKind::DedupRef {
                    continue;
                }
                if self
                    .extent_index
                    .lookup(&entry.hash)
                    .map(|loc| loc.segment_id == seg_ulid)
                    .unwrap_or(false)
                {
                    Arc::make_mut(&mut self.extent_index).remove(&entry.hash);
                    stats.extents_removed += 1;
                }
            }

            let mut live_entries = live_entries;
            let inline_bytes = segment::read_inline_section(seg_path)?;
            segment::read_extent_bodies(
                seg_path,
                body_section_start,
                &mut live_entries,
                [EntryKind::Data, EntryKind::Inline],
                &inline_bytes,
            )?;
            merged_live.extend(live_entries);

            candidate_paths.push(seg_path.clone());
            stats.segments_compacted += 1;
            stats.bytes_freed += dead_bytes;
        }

        if candidate_paths.is_empty() {
            return Ok(stats);
        }

        // A single small segment with no dead extents gains nothing from
        // rewriting: the output would be the same size and content. Only
        // merge when dead space is reclaimed or two or more small segments
        // can be combined into one.
        if candidate_paths.len() == 1 && !any_dead {
            return Ok(CompactionStats::default());
        }

        // Use max(candidate ULIDs) as the output ULID. This guarantees the
        // output sorts below the current WAL ULID (all pending segments were
        // created before the WAL was opened, so their ULIDs are strictly less).
        // Preserving this invariant ensures that a WAL flush always produces a
        // segment with a higher ULID than any compact output, so rebuild always
        // applies data in write order. Using mint.next() here would generate a
        // ULID past the WAL ULID and break that ordering.
        //
        // The merged output is written as a single segment (no FLUSH_THRESHOLD
        // split). The split served only to bound segment size, but FLUSH_THRESHOLD
        // is a soft cap. Avoiding a split means we need only one output ULID,
        // which is safe to derive from the inputs.
        let new_ulid = candidate_paths
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| Ulid::from_string(s).ok())
            })
            .max()
            .ok_or_else(|| io::Error::other("sweep_pending: no valid candidate ULIDs"))?;
        let new_ulid_str = new_ulid.to_string();

        // Write the merged output, atomically replacing the max-ULID candidate.
        if !merged_live.is_empty() {
            let tmp_path = pending_dir.join(format!("{new_ulid_str}.tmp"));
            let final_path = pending_dir.join(&new_ulid_str);
            let new_bss =
                segment::write_segment(&tmp_path, &mut merged_live, self.signer.as_ref())?;
            fs::rename(&tmp_path, &final_path)?;
            segment::fsync_dir(&final_path)?;
            stats.new_segments += 1;

            for entry in &merged_live {
                match entry.kind {
                    EntryKind::Data => {
                        Arc::make_mut(&mut self.extent_index).insert(
                            entry.hash,
                            extentindex::ExtentLocation {
                                segment_id: new_ulid,
                                body_offset: entry.stored_offset,
                                body_length: entry.stored_length,
                                compressed: entry.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: None,
                            },
                        );
                    }
                    EntryKind::Inline => {
                        Arc::make_mut(&mut self.extent_index).insert(
                            entry.hash,
                            extentindex::ExtentLocation {
                                segment_id: new_ulid,
                                body_offset: entry.stored_offset,
                                body_length: entry.stored_length,
                                compressed: entry.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: entry.data.clone().map(Vec::into_boxed_slice),
                            },
                        );
                    }
                    EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => {}
                }
            }
        }

        // Evict and delete input candidates. The max-ULID candidate was already
        // replaced atomically by the output rename above; skip re-deleting it,
        // but still evict its cached fd — the rename unlinked the old inode
        // and a surviving cached handle would continue to serve bytes from
        // the old layout, seeking with the new body_section_start into the
        // old index section.
        for seg_path in &candidate_paths {
            let seg_ulid_opt = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| Ulid::from_string(s).ok());
            if let Some(ulid) = seg_ulid_opt {
                self.file_cache.borrow_mut().evict(ulid);
            }
            if seg_ulid_opt == Some(new_ulid) && !merged_live.is_empty() {
                continue; // already replaced atomically above
            }
            fs::remove_file(seg_path)?;
        }

        Ok(stats)
    }

    /// Establish a consistent checkpoint for coordinator GC.
    ///
    /// Mints three ULIDs from the volume's monotonic clock — `u_repack`,
    /// `u_sweep`, and `u_wal` — in that order, then flushes the current WAL to
    /// `pending/` under the name `u_wal` (not the WAL's existing ULID), and
    /// opens a fresh WAL with ULID > `u_wal`.  Returns `(u_repack, u_sweep)` to
    /// the coordinator.
    ///
    /// **Why four ULIDs, minted first.**
    ///
    /// The monotonic mint is a logical clock.  Pulling all four identifiers
    /// from it *before* any I/O encodes the relative ordering of operations in
    /// advance: `u_repack < u_sweep < u_flush < u_wal`.  The I/O steps then
    /// execute in the pre-determined logical order without requiring any
    /// coordination after the fact.
    ///
    /// Without pre-minting `u_flush`, the WAL segment flushed by this call would
    /// carry the WAL's *existing* ULID (assigned when the WAL was opened,
    /// before the GC ULIDs were minted).  That ULID is lower than `u_sweep`, so
    /// after the segment is drained to `index/`, crash-recovery rebuild would
    /// apply the GC output *after* the WAL segment and return stale data.
    ///
    /// When the WAL is empty, the WAL file is deleted and `u_flush` is not used
    /// (no segment is produced), so the empty-WAL case is also safe: the fresh
    /// WAL opened after minting carries a ULID > `u_sweep`.
    ///
    /// All ULIDs come from the volume's own monotonic mint, never from an
    /// external clock — coordinator clock skew cannot corrupt ULID ordering.
    pub fn gc_checkpoint(&mut self) -> io::Result<(Ulid, Ulid)> {
        // Mint all four ULIDs before any I/O.  The ordering constraint —
        // u_repack < u_sweep < u_flush < u_wal — is established here, before
        // any flush or WAL rotation.  UlidMint guarantees strict monotonicity
        // even within the same millisecond (increments random bits).
        let u_repack = self.mint.next();
        let u_sweep = self.mint.next();
        let u_flush = self.mint.next();
        let u_wal = self.mint.next();
        // Flush the current WAL to pending/ under u_flush.  If the WAL is
        // empty, the file is deleted and u_flush is unused (no segment produced).
        self.flush_wal_to_pending_as(u_flush)?;
        // Open a new WAL with u_wal > u_flush.
        let (wal, wal_ulid, wal_path, pending_entries) =
            create_fresh_wal(&self.base_dir.join("wal"), u_wal)?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        self.pending_entries = pending_entries;
        Ok((u_repack, u_sweep))
    }

    /// Apply pending GC handoff files written by the coordinator.
    ///
    /// The coordinator writes the compacted segment to `gc/<new-ulid>` (staged,
    /// signed with an ephemeral key) and then writes `gc/<new-ulid>.pending`.
    /// This method re-signs `gc/<new-ulid>` in-place with the volume's own key,
    /// updates the in-memory extent index, and renames the handoff file to
    /// `gc/<new-ulid>.applied`.  The coordinator then uploads the segment to S3
    /// and sends a `promote <new-ulid>` IPC.  The `promote_segment` handler writes
    /// `index/<new-ulid>.idx` and `cache/<new-ulid>.{body,present}`, and deletes
    /// `index/<old>.idx` for each consumed segment.
    ///
    /// This two-phase approach preserves the invariant: **`index/<ulid>.idx`
    /// present ↔ segment confirmed in S3**.  The idx is never written before the
    /// coordinator confirms the upload, so a segment in `gc/` or `pending/` with
    /// no idx is never mistaken for an S3-confirmed segment.
    ///
    /// **`.pending` handoffs** (normal path):
    /// Walks `gc/` for `.staged` entries and applies each via the
    /// derive-at-apply path: read `inputs` from the segment header, diff
    /// each input's `.idx` against the new segment's entries to build the
    /// extent-index updates, re-sign the body with the volume key, rename
    /// `<ulid>.tmp → <ulid>` (atomic commit), then remove `<ulid>.staged`
    /// and evict input cache files.
    ///
    /// Returns the number of handoff files processed. Returns `Ok(0)` if
    /// the `gc/` directory does not exist yet.
    pub fn apply_gc_handoffs(&mut self) -> io::Result<usize> {
        let gc_dir = self.base_dir.join("gc");
        self.apply_all_staged_handoffs(&gc_dir)
    }

    /// Build a [`GcHandoffJob`] for dispatch to the worker thread.
    pub fn build_gc_handoff_job(&self, staged_path: PathBuf, new_ulid: Ulid) -> GcHandoffJob {
        GcHandoffJob {
            staged_path,
            new_ulid,
            gc_dir: self.base_dir.join("gc"),
            index_dir: self.base_dir.join("index"),
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
        }
    }

    /// Scan `gc/` for staged handoff files that need processing.
    ///
    /// Sweeps stale `.tmp` files, applies bare-wins shortcuts, and returns
    /// a list of `(staged_path, new_ulid)` pairs to dispatch to the worker.
    /// Also returns a count of handoffs that were already applied (bare wins).
    ///
    /// This is the prep phase of the GC handoff offload — cheap directory
    /// listing that runs on the actor thread.
    pub fn scan_staged_handoffs(&self) -> io::Result<(Vec<(PathBuf, Ulid)>, usize)> {
        let gc_dir = self.base_dir.join("gc");
        if !gc_dir.try_exists()? {
            return Ok((Vec::new(), 0));
        }

        // Pass 1: sweep stale `.tmp` files (incomplete writes).
        for entry in fs::read_dir(&gc_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(".tmp") {
                let _ = fs::remove_file(entry.path());
            }
        }

        // Pass 2: collect `.staged` files.
        let mut staged: Vec<(String, Ulid)> = fs::read_dir(&gc_dir)?
            .filter_map(|e| {
                let e = e.ok()?;
                let name = e.file_name().into_string().ok()?;
                let stem = name.strip_suffix(".staged")?;
                let ulid = Ulid::from_string(stem).ok()?;
                Some((name, ulid))
            })
            .collect();
        staged.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut to_process = Vec::new();
        let mut already_applied = 0usize;
        for (staged_name, new_ulid) in staged {
            let staged_path = gc_dir.join(&staged_name);
            let bare_path = gc_dir.join(new_ulid.to_string());

            // Crash recovery: `.staged` + bare → bare wins, drop `.staged`.
            if bare_path.try_exists()? {
                let _ = fs::remove_file(&staged_path);
                already_applied += 1;
                continue;
            }

            to_process.push((staged_path, new_ulid));
        }

        Ok((to_process, already_applied))
    }

    /// Apply a GC handoff result that was processed by the worker thread.
    ///
    /// Re-derives the action set (to_remove, stale_cancel, carried updates)
    /// against the **current** extent index and lbamap to handle concurrent
    /// writes that may have arrived while the worker was running.
    ///
    /// Returns `Applied` if the handoff was committed, `Cancelled` if the
    /// stale-liveness check failed.
    pub fn apply_gc_handoff_result(&mut self, result: &GcHandoffResult) -> io::Result<StagedApply> {
        use std::collections::HashSet;

        // Build carried_hashes: body-owning entries in the GC output.
        let carried_hashes: HashSet<blake3::Hash> = result
            .entries
            .iter()
            .filter(|e| e.kind != segment::EntryKind::DedupRef)
            .map(|e| e.hash)
            .collect();

        // Walk input old entries, check extent index for each.
        let live = self.lbamap.lba_referenced_hashes();
        let mut to_remove: Vec<(blake3::Hash, Ulid)> = Vec::new();
        let mut stale_cancel: Vec<blake3::Hash> = Vec::new();

        for &(hash, kind, input_ulid) in &result.input_old_entries {
            if !matches!(kind, segment::EntryKind::Data | segment::EntryKind::Inline) {
                continue;
            }
            let still_at_input = self
                .extent_index
                .lookup(&hash)
                .is_some_and(|loc| loc.segment_id == input_ulid);
            if !still_at_input {
                continue;
            }
            if carried_hashes.contains(&hash) {
                continue; // will be updated below
            }
            // Not carried: will be removed. If LBA-live, cancel.
            if live.contains(&hash) {
                stale_cancel.push(hash);
            }
            to_remove.push((hash, input_ulid));
        }

        if !stale_cancel.is_empty() {
            log::warn!(
                "gc staged {}: stale-liveness cancellation — {} hash(es) live in \
                 volume but absent from coordinator output; removing staged file \
                 and tmp",
                result.staged_path.display(),
                stale_cancel.len(),
            );
            let _ = fs::remove_file(&result.staged_path);
            let tmp_path = result.gc_dir.join(format!("{}.tmp", result.new_ulid));
            let _ = fs::remove_file(&tmp_path);
            return Ok(StagedApply::Cancelled);
        }

        // Update extent index for carried entries.
        for (i, e) in result.entries.iter().enumerate() {
            if e.kind == segment::EntryKind::DedupRef {
                continue;
            }
            let still_at_input = self
                .extent_index
                .lookup(&e.hash)
                .is_some_and(|loc| result.inputs.contains(&loc.segment_id));
            if !still_at_input {
                continue;
            }
            let idata = if e.kind == segment::EntryKind::Inline {
                let start = e.stored_offset as usize;
                let end = start + e.stored_length as usize;
                if end <= result.handoff_inline.len() {
                    Some(result.handoff_inline[start..end].into())
                } else {
                    continue;
                }
            } else {
                None
            };
            Arc::make_mut(&mut self.extent_index).insert(
                e.hash,
                extentindex::ExtentLocation {
                    segment_id: result.new_ulid,
                    body_offset: e.stored_offset,
                    body_length: e.stored_length,
                    compressed: e.compressed,
                    body_source: BodySource::Cached(i as u32),
                    body_section_start: result.new_bss,
                    inline_data: idata,
                },
            );
        }

        // Remove extent index entries for hashes no longer carried.
        for (hash, old_ulid) in &to_remove {
            if self
                .extent_index
                .lookup(hash)
                .is_some_and(|loc| loc.segment_id == *old_ulid)
            {
                Arc::make_mut(&mut self.extent_index).remove(hash);
            }
        }

        // Clean up pending/ files for consumed inputs.
        let pending_dir = self.base_dir.join("pending");
        for input in &result.inputs {
            let _ = fs::remove_file(pending_dir.join(input.to_string()));
        }

        // Commit: rename tmp → bare.
        let tmp_path = result.gc_dir.join(format!("{}.tmp", result.new_ulid));
        let bare_path = result.gc_dir.join(result.new_ulid.to_string());
        fs::rename(&tmp_path, &bare_path)?;
        let _ = fs::remove_file(&result.staged_path);

        // Evict cache for consumed inputs.
        let cache_dir = self.base_dir.join("cache");
        for input in &result.inputs {
            let s = input.to_string();
            let _ = fs::remove_file(cache_dir.join(format!("{s}.body")));
            let _ = fs::remove_file(cache_dir.join(format!("{s}.present")));
        }

        Ok(StagedApply::Applied)
    }

    /// Walk `gc/` for `.staged` entries and apply each via the
    /// self-describing derive-at-apply path.
    ///
    /// Also handles crash-recovery filename states:
    /// - `<ulid>.tmp` / `<ulid>.staged.tmp` — stale from a crashed write.
    ///   Remove on sight.
    /// - `<ulid>.staged` alone — apply normally.
    /// - `<ulid>.staged` + bare `<ulid>` — bare wins (previous apply
    ///   committed before cleanup); remove the `.staged`.
    /// - bare `<ulid>` alone — already applied, no action.
    fn apply_all_staged_handoffs(&mut self, gc_dir: &Path) -> io::Result<usize> {
        if !gc_dir.try_exists()? {
            return Ok(0);
        }

        // Pass 1: sweep stale `.tmp` files (incomplete writes).
        for entry in fs::read_dir(gc_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(".tmp") {
                let _ = fs::remove_file(entry.path());
            }
        }

        // Pass 2: collect `.staged` files.
        let mut staged: Vec<(String, Ulid)> = fs::read_dir(gc_dir)?
            .filter_map(|e| {
                let e = e.ok()?;
                let name = e.file_name().into_string().ok()?;
                let stem = name.strip_suffix(".staged")?;
                let ulid = Ulid::from_string(stem).ok()?;
                Some((name, ulid))
            })
            .collect();
        staged.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut count = 0usize;
        for (staged_name, new_ulid) in staged {
            let staged_path = gc_dir.join(&staged_name);
            let bare_path = gc_dir.join(new_ulid.to_string());

            // Crash recovery: `.staged` + bare → bare wins, drop `.staged`.
            if bare_path.try_exists()? {
                let _ = fs::remove_file(&staged_path);
                count += 1;
                continue;
            }

            match self.apply_staged_handoff(gc_dir, &staged_path, new_ulid)? {
                StagedApply::Applied => count += 1,
                StagedApply::Cancelled => {
                    // Stale-liveness cancel already removed `.staged` inside.
                }
            }
        }

        Ok(count)
    }

    /// Apply one `.staged` GC output by deriving the action set from the
    /// segment's own inputs field + each input's `.idx` file.
    ///
    /// Commit protocol: writes a re-signed copy to `<ulid>.tmp`, renames to
    /// bare `<ulid>` (the atomic commit point), then removes `<ulid>.staged`.
    /// A crash between rename and remove is handled by the caller (bare wins).
    fn apply_staged_handoff(
        &mut self,
        gc_dir: &Path,
        staged_path: &Path,
        new_ulid: Ulid,
    ) -> io::Result<StagedApply> {
        // Read staged segment (entries + inputs). Unverified: the coordinator
        // signs with an ephemeral key we don't verify against, same as the
        // legacy path; the volume re-signs with its own key before the
        // commit rename.
        let (bss, mut entries, inputs) = segment::read_segment_index(staged_path)?;

        // Empty inputs list means this isn't a GC output — reject.
        if inputs.is_empty() {
            log::warn!(
                "gc staged file {} has no inputs; removing",
                staged_path.display()
            );
            let _ = fs::remove_file(staged_path);
            return Ok(StagedApply::Cancelled);
        }

        // Hashes the new segment carries (DATA/Inline — body-owning kinds).
        let carried_hashes: HashSet<blake3::Hash> = entries
            .iter()
            .filter(|e| e.kind != EntryKind::DedupRef)
            .map(|e| e.hash)
            .collect();

        // Walk each input's idx and partition body-owning hashes into
        // (move, remove, stale) buckets against the current extent index.
        //
        // An entry contributes only if the extent index currently points at
        // the input segment for that hash — otherwise a newer write has
        // already superseded it and we must not touch it.
        let index_dir = self.base_dir.join("index");
        let live = self.lbamap.lba_referenced_hashes();
        let mut to_remove: Vec<(blake3::Hash, Ulid)> = Vec::new();
        let mut stale_cancel: Vec<blake3::Hash> = Vec::new();
        for input_ulid in &inputs {
            let idx_path = index_dir.join(format!("{input_ulid}.idx"));
            let parsed = match segment::read_segment_index(&idx_path) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let (_, old_entries, _) = parsed;
            for e in &old_entries {
                // Body-owning kinds only. DedupRef/Zero/Delta carry no
                // responsibility for this segment's body cleanup.
                if !matches!(e.kind, EntryKind::Data | EntryKind::Inline) {
                    continue;
                }
                // Only touch entries still pointed at by the extent index.
                let still_at_input = self
                    .extent_index
                    .lookup(&e.hash)
                    .is_some_and(|loc| loc.segment_id == *input_ulid);
                if !still_at_input {
                    continue;
                }
                if carried_hashes.contains(&e.hash) {
                    // will be updated in the second pass below
                    continue;
                }
                // Not carried: will be removed. If LBA-live, cancel.
                if live.contains(&e.hash) {
                    stale_cancel.push(e.hash);
                }
                to_remove.push((e.hash, *input_ulid));
            }
        }

        if !stale_cancel.is_empty() {
            log::warn!(
                "gc staged {}: stale-liveness cancellation — {} hash(es) live in \
                 volume but absent from coordinator output; removing staged file",
                staged_path.display(),
                stale_cancel.len(),
            );
            let _ = fs::remove_file(staged_path);
            return Ok(StagedApply::Cancelled);
        }

        // Re-sign: read body + inline + delta bytes into memory, write a fresh
        // segment with the volume's key, rename tmp → bare as the commit point.
        //
        // TODO(step 4 follow-up): pipe delta_body through — current path
        // omits the delta body section, which is fine for GC outputs that
        // don't carry delta entries (the common case) but will need to be
        // preserved when delta-rewrite interacts with GC compaction.
        let handoff_inline = segment::read_inline_section(staged_path)?;
        segment::read_extent_bodies(
            staged_path,
            bss,
            &mut entries,
            [EntryKind::Data, EntryKind::DedupRef, EntryKind::Inline],
            &handoff_inline,
        )?;
        let tmp_path = gc_dir.join(format!("{new_ulid}.tmp"));
        segment::write_gc_segment(&tmp_path, &mut entries, &inputs, self.signer.as_ref())?;

        // Update extent index for carried hashes (pass through entries again,
        // now that we have the freshly-written body_section_start).
        let (new_bss, _, _) =
            segment::read_and_verify_segment_index(&tmp_path, &self.verifying_key)?;
        for (i, e) in entries.iter().enumerate() {
            if e.kind == EntryKind::DedupRef {
                continue;
            }
            // Only update if still pointed at one of the inputs.
            let still_at_input = self
                .extent_index
                .lookup(&e.hash)
                .is_some_and(|loc| inputs.contains(&loc.segment_id));
            if !still_at_input {
                continue;
            }
            let idata = if e.kind == EntryKind::Inline {
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

        // Remove extent index entries for inputs' hashes that are no longer
        // carried — the old segment files will be deleted by the coordinator
        // once this bare-named handoff is uploaded.
        for (hash, old_ulid) in &to_remove {
            if self
                .extent_index
                .lookup(hash)
                .is_some_and(|loc| loc.segment_id == *old_ulid)
            {
                Arc::make_mut(&mut self.extent_index).remove(hash);
            }
        }

        // Clean up any stale pending/ files for consumed inputs.
        let pending_dir = self.base_dir.join("pending");
        for input in &inputs {
            let _ = fs::remove_file(pending_dir.join(input.to_string()));
        }

        // Commit: rename tmp → bare. From this point on, the handoff is
        // applied; the `.staged` file is cleanup.
        let bare_path = gc_dir.join(new_ulid.to_string());
        fs::rename(&tmp_path, &bare_path)?;
        let _ = fs::remove_file(staged_path);

        // Evict input cache bodies. By this point the extent index no
        // longer references any old hash that the new segment doesn't
        // carry, so no concurrent reader can still resolve via the old
        // cache.
        let cache_dir = self.base_dir.join("cache");
        for input in &inputs {
            let s = input.to_string();
            let _ = fs::remove_file(cache_dir.join(format!("{s}.body")));
            let _ = fs::remove_file(cache_dir.join(format!("{s}.present")));
        }

        Ok(StagedApply::Applied)
    }

    // --- self-describing GC handoff support (step 4a) ---

    /// Evict old cache files for completed GC handoffs.
    ///
    /// Called by the actor AFTER publishing the new snapshot (which redirects
    /// all new reads to the GC output segment). Walks `gc/` for bare
    /// volume-applied handoffs, reads each one's `inputs` field from the
    /// segment header, and deletes `cache/<input>.{body,present}` for each
    /// consumed input ULID.
    ///
    /// Safe to call multiple times — file deletions are best-effort and
    /// silently skip already-absent files.
    pub fn evict_applied_gc_cache(&self) {
        let gc_dir = self.base_dir.join("gc");
        let cache_dir = self.base_dir.join("cache");
        let Ok(entries) = fs::read_dir(&gc_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            // Bare gc/<ulid>: a volume-applied handoff awaiting upload.
            if name_str.contains('.') {
                continue;
            }
            if Ulid::from_string(name_str).is_err() {
                continue;
            }
            let path = entry.path();
            let Ok((_, _, inputs)) =
                segment::read_and_verify_segment_index(&path, &self.verifying_key)
            else {
                continue;
            };
            for old_ulid in &inputs {
                let s = old_ulid.to_string();
                let _ = fs::remove_file(cache_dir.join(format!("{s}.body")));
                let _ = fs::remove_file(cache_dir.join(format!("{s}.present")));
            }
        }
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
    /// **GC path** (`gc/<ulid>` exists): also deletes `index/<old>.idx` for each
    /// segment consumed by the GC handoff (read from `gc/<ulid>.applied`).  This
    /// happens after writing the new idx so there is never a window where no idx
    /// covers the affected LBAs.  The `gc/<ulid>` body file is also deleted here
    /// — it has already been copied into `cache/<ulid>.body`, and deleting it
    /// inside the actor (rather than from the coordinator) keeps every mutation
    /// of `gc/` serialised with the idle-tick `apply_gc_handoffs` path.
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
                if !matches!(entry.kind, EntryKind::Data | EntryKind::Inline) {
                    continue;
                }
                if self
                    .extent_index
                    .lookup(&entry.hash)
                    .is_some_and(|loc| loc.segment_id == ulid)
                {
                    let idata = if entry.kind == EntryKind::Inline {
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

    /// Finalize a completed GC handoff by renaming `gc/<ulid>.applied`
    /// to `gc/<ulid>.done`.
    ///
    /// Called by the coordinator after the new segment has been uploaded to
    /// S3, `promote_segment` has moved it into the local cache, and the old
    /// segments have been deleted from S3. The `.done` rename is the last
    /// step in the handoff lifecycle and must happen AFTER the S3 delete so
    /// that a crash between the two cannot leak old-segment objects in S3 —
    /// the `.applied` state keeps `apply_done_handoffs` eligible to retry the
    /// delete, and only `.done` removes that eligibility.
    ///
    /// Routing through the actor (rather than letting the coordinator unlink
    /// `gc/<ulid>` directly) keeps every mutation of `gc/` serialised with the
    /// idle-tick `apply_gc_handoffs` path, so there is no race between the
    /// coordinator removing a file and the actor reading it.
    ///
    /// Under the self-describing GC handoff protocol this deletes the bare
    /// `gc/<ulid>` file — the file whose presence previously signalled
    /// "volume applied, coordinator upload pending." Once the coordinator has
    /// uploaded the segment to S3, invoked `promote_segment` (which populates
    /// `cache/<ulid>.body` + `index/<ulid>.idx`), and deleted the old S3
    /// objects, this call reclaims the last on-disk copy.
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
        // Best-effort cleanup of any stray `.staged` sibling left over from
        // a crash between the bare rename and `.staged` removal.
        let _ = fs::remove_file(gc_dir.join(format!("{ulid}.staged")));
        Ok(())
    }

    /// Hole-punch hash-dead DATA entries in `pending/<ulid>` in place, so
    /// that deleted data never leaves the local host via S3 upload. Called
    /// by the coordinator just before the segment is read for upload.
    ///
    /// A DATA entry is **hash-dead** when:
    /// - Its LBA no longer maps to this hash (LBA-dead), and
    /// - No other live LBA in the volume references this hash.
    ///
    /// Such an entry has no readers (by construction) and its bytes are
    /// safe to free. LBA-dead-but-hash-alive entries keep their bytes —
    /// they're still referenced via dedup from a different LBA, and GC may
    /// later repack them.
    ///
    /// DedupRef, Zero, and Inline entries are skipped — they carry no
    /// bytes in the body section.
    ///
    /// The operation is **in place** on `pending/<ulid>`: no sidecar file,
    /// no copy, no rename. Only the physical storage of dead DATA regions
    /// is freed; the file size, `body_length`, index section, and
    /// signature are all unchanged. `fallocate(FALLOC_FL_PUNCH_HOLE)` on
    /// Linux; zero-write on other platforms.
    ///
    /// Idempotent: a second call is a no-op because the first call already
    /// freed all hash-dead regions.
    ///
    /// Fast path: if no hash-dead DATA entries exist, the function opens
    /// nothing and returns immediately.
    pub fn redact_segment(&mut self, ulid: Ulid) -> io::Result<()> {
        let ulid_str = ulid.to_string();
        let pending_dir = self.base_dir.join("pending");
        let seg_path = pending_dir.join(&ulid_str);

        let (body_section_start, entries, _inputs) =
            segment::read_and_verify_segment_index(&seg_path, &self.verifying_key)?;

        // Cheap pre-scan: is there any DATA entry whose LBA no longer maps
        // to its hash? If not, there's nothing for redact to do.
        let has_lba_dead_data = entries.iter().any(|e| {
            e.kind == EntryKind::Data
                && e.stored_length > 0
                && self.lbamap.hash_at(e.start_lba) != Some(e.hash)
        });
        if !has_lba_dead_data {
            return Ok(());
        }

        // Any LBA-dead DATA entry whose hash is still referenced elsewhere
        // must keep its bytes. Only hash-dead entries get punched.
        let live_hashes = self.lbamap.lba_referenced_hashes();

        let mut out = fs::OpenOptions::new().write(true).open(&seg_path)?;
        let mut punched = 0usize;
        let mut punched_bytes: u64 = 0;
        let mut punched_hashes: Vec<blake3::Hash> = Vec::new();
        for entry in &entries {
            if entry.kind != EntryKind::Data || entry.stored_length == 0 {
                continue;
            }
            let lba_live = self.lbamap.hash_at(entry.start_lba) == Some(entry.hash);
            if lba_live || live_hashes.contains(&entry.hash) {
                continue;
            }
            segment::punch_hole(
                &mut out,
                body_section_start + entry.stored_offset,
                entry.stored_length as u64,
            )?;
            punched += 1;
            punched_bytes += entry.stored_length as u64;
            punched_hashes.push(entry.hash);
        }
        out.sync_data()?;
        drop(out);

        // Invalidate extent-index entries for every hash whose body we just
        // destroyed, but only if the index still points at this segment. A
        // later GC/repack may have moved the canonical location elsewhere,
        // in which case another segment holds the real body.
        //
        // Without this, the dedup write shortcut (`write_commit`) would see
        // a surviving extent-index entry for the punched hash and emit a
        // thin DedupRef whose canonical body is now zeros.
        if !punched_hashes.is_empty() {
            let index = Arc::make_mut(&mut self.extent_index);
            for hash in &punched_hashes {
                if index.lookup(hash).is_some_and(|loc| loc.segment_id == ulid) {
                    index.remove(hash);
                }
            }
        }

        log::info!(
            "redact {ulid_str}: punched {punched} hash-dead DATA regions ({punched_bytes} bytes)"
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

    /// Does NOT open a new WAL — the caller is responsible for that.
    fn flush_wal_to_pending(&mut self) -> io::Result<()> {
        // Mint a fresh segment ULID distinct from `self.wal_ulid` so
        // `wal/<old_wal_ulid>` and `pending/<segment_ulid>` never collide
        // on the same path. With a shared ULID, a stale cold-cache reader
        // that loaded the pre-promote snapshot could look up the old WAL
        // ULID, fall through to `pending/<same_ulid>`, and read WAL-relative
        // offsets as if they were segment-relative — silent wrong bytes.
        // A distinct segment ULID turns that cold-cache race into NotFound.
        //
        // Wastes one mint when the WAL is empty (the early return below
        // skips the segment write). The mint is cheap and monotonic; the
        // extra advance is harmless.
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
    fn flush_wal_to_pending_as(&mut self, segment_ulid: Ulid) -> io::Result<()> {
        self.wal.fsync()?;
        if self.pending_entries.is_empty() {
            fs::remove_file(&self.wal_path)?;
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
        let old_wal_ulid = self.wal_ulid;
        let pre_promote_offsets: Vec<Option<u64>> = self
            .pending_entries
            .iter()
            .map(|e| match e.kind {
                EntryKind::Data | EntryKind::Inline => {
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
                EntryKind::Data | EntryKind::Inline => {}
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
            let idata = if entry.kind == EntryKind::Inline {
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
            let (mut data, mut refs, mut zero, mut inline, mut delta) =
                (0usize, 0usize, 0usize, 0usize, 0usize);
            for e in &self.pending_entries {
                match e.kind {
                    EntryKind::Data => data += 1,
                    EntryKind::DedupRef => refs += 1,
                    EntryKind::Zero => zero += 1,
                    EntryKind::Inline => inline += 1,
                    EntryKind::Delta => delta += 1,
                }
            }
            log::info!(
                "flush {segment_ulid}: {data} data, {inline} inline, {refs} dedup-ref, \
                 {zero} zero, {delta} delta ({} entries total)",
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
        fs::remove_file(&self.wal_path)?;
        // Evict any cached fd for the deleted WAL so subsequent lookups of
        // `old_wal_ulid` re-open rather than reuse a handle to the deleted
        // inode. The cache is keyed by the path that was open, so we pass
        // the WAL's original ULID — not `segment_ulid`.
        self.evict_cached_segment(old_wal_ulid);
        Ok(())
    }

    /// Promote the current WAL to a pending segment, then open a fresh WAL.
    fn promote(&mut self) -> io::Result<()> {
        self.flush_wal_to_pending()?;
        // Create the fresh WAL. If this fails the segment is safe in pending/
        // and will be found on the next startup rebuild.
        let (wal, wal_ulid, wal_path, _) =
            create_fresh_wal(&self.base_dir.join("wal"), self.mint.next())?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        Ok(())
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
        // the existing snapshot ULID rather than writing a new marker.
        if !self.has_new_segments
            && let Some(latest_str) = latest_snapshot(&self.base_dir)?
        {
            let (wal, wal_ulid, wal_path, pending_entries) =
                create_fresh_wal(&self.base_dir.join("wal"), self.mint.next())?;
            self.wal = wal;
            self.wal_ulid = wal_ulid;
            self.wal_path = wal_path;
            self.pending_entries = pending_entries;
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
            own_segments.insert(self.wal_ulid);
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

        // Open a fresh WAL to continue writing.
        let (wal, wal_ulid, wal_path, pending_entries) =
            create_fresh_wal(&self.base_dir.join("wal"), self.mint.next())?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        self.pending_entries = pending_entries;

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
    pub fn sign_snapshot_manifest(&mut self, snap_ulid: Ulid) -> io::Result<()> {
        let index_dir = self.base_dir.join("index");
        let mut seg_ulids: Vec<Ulid> = Vec::new();
        match fs::read_dir(&index_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let Some(s) = name.to_str() else { continue };
                    let Some(stem) = s.strip_suffix(".idx") else {
                        continue;
                    };
                    if let Ok(u) = Ulid::from_string(stem) {
                        seg_ulids.push(u);
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        let snapshots_dir = self.base_dir.join("snapshots");
        fs::create_dir_all(&snapshots_dir)?;

        crate::signing::write_snapshot_manifest(
            &self.base_dir,
            self.signer.as_ref(),
            &snap_ulid,
            &seg_ulids,
        )?;

        // Write the marker last — partial sequences leave no snapshot visible.
        fs::write(snapshots_dir.join(snap_ulid.to_string()), "")?;
        self.has_new_segments = false;
        Ok(())
    }

    /// Locate the segment body file for `segment_id` within this fork's
    /// ancestry chain.
    ///
    /// Search order:
    ///   1. Current fork: `wal/`, `pending/`, `gc/*.applied`, `cache/<id>.body`
    ///   2. Ancestor forks (newest first): `pending/`, `gc/*.applied`, `cache/<id>.body`
    ///   3. Demand-fetch via fetcher (writes three-file format to `cache/`)
    ///
    /// For full segment files (`wal/`, `pending/`, `gc/*.applied`), body reads use
    /// absolute file offsets (`ExtentLocation.body_offset`). For cached body
    /// files (`cache/<id>.body`), the file IS the body section, so reads use
    /// body-relative offsets — consistent with how `extentindex::rebuild` stores
    /// offsets for cached entries.
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

    /// Phase 1 of extent reclamation: capture an immutable snapshot of the
    /// LBA map state over the target range. Cheap: one `Arc::clone` and
    /// one O(log n) range query.
    ///
    /// The returned plan carries a clone of the current `Arc<LbaMap>` which
    /// serves as the precondition token at phase 3 (`Arc::ptr_eq` detects
    /// any mutation between capture and commit) and as the read source for
    /// phase 2's bloat detection (no need to re-walk the live map).
    ///
    /// See `docs/design-extent-reclamation.md § Optimistic commit structure`.
    pub fn reclaim_snapshot(&self, start_lba: u64, lba_length: u32) -> ReclaimPlan {
        let end_lba = start_lba + lba_length as u64;
        let entries = self.lbamap.extents_in_range(start_lba, end_lba);
        ReclaimPlan {
            target_start_lba: start_lba,
            target_lba_length: lba_length,
            entries,
            lbamap_snapshot: Arc::clone(&self.lbamap),
        }
    }

    /// Phase 3 of extent reclamation: verify the LBA map has not changed
    /// since the plan was captured, then apply each proposed rewrite through
    /// the internal-origin write path.
    ///
    /// The precondition check is `Arc::ptr_eq(plan.lbamap_snapshot, self.lbamap)`.
    /// Any mutation between phase 1 and this call would have called
    /// `Arc::make_mut` on the volume's `lbamap` Arc (externally-visible via
    /// at least the published snapshot), which reallocates — so the pointers
    /// differ and this returns cleanly with `discarded: true`.
    ///
    /// A discard leaves all Volume state completely unchanged; the caller
    /// can retry on the next quiet window. The wasted work is whatever
    /// phase 2 did (reads, hashing) — never any WAL or map state.
    pub fn reclaim_commit(
        &mut self,
        plan: ReclaimPlan,
        proposed: Vec<ReclaimProposed>,
    ) -> io::Result<ReclaimOutcome> {
        if !Arc::ptr_eq(&plan.lbamap_snapshot, &self.lbamap) {
            return Ok(ReclaimOutcome {
                discarded: true,
                ..Default::default()
            });
        }
        let mut outcome = ReclaimOutcome::default();
        for p in proposed {
            let bytes = p.data.len() as u64;
            if self.write_with_hash(p.start_lba, &p.data, p.hash)? {
                outcome.runs_rewritten += 1;
                outcome.bytes_rewritten += bytes;
            }
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

    /// True if the WAL has reached the 32 MiB soft cap and should be promoted.
    ///
    /// The actor calls this after every write reply and promotes if true.
    /// The check is separated from `write()` so that writes are always fast
    /// (WAL append only) and the promotion cost is never borne by the write caller.
    pub fn needs_promote(&self) -> bool {
        self.wal.size() >= FLUSH_THRESHOLD
    }

    pub fn promote_for_test(&mut self) -> io::Result<()> {
        self.promote()
    }

    // ------------------------------------------------------------------
    // Off-actor promote: prep + apply
    // ------------------------------------------------------------------

    /// Fsync the WAL without promoting.
    ///
    /// Used by the actor's `Flush` handler to satisfy the NBD durability
    /// contract without blocking on segment serialization.
    pub fn wal_fsync(&mut self) -> io::Result<()> {
        self.wal.fsync()
    }

    /// Prep phase of the off-actor promote.  Runs on the actor thread.
    ///
    /// Fsyncs the WAL, snapshots CAS precondition tokens, takes ownership
    /// of `pending_entries`, mints a fresh segment ULID, and opens a new
    /// WAL.  Returns `None` if the WAL is empty (nothing to promote).
    ///
    /// After this call the volume is ready to accept new writes on the
    /// fresh WAL.  The returned [`PromoteJob`] is sent to the worker
    /// thread for the heavy segment-write work.
    pub fn prepare_promote(&mut self) -> io::Result<Option<PromoteJob>> {
        if self.pending_entries.is_empty() {
            return Ok(None);
        }
        self.wal.fsync()?;

        let old_wal_ulid = self.wal_ulid;
        let old_wal_path = self.wal_path.clone();

        // Snapshot CAS tokens before write_and_commit rewrites stored_offset.
        let pre_promote_offsets: Vec<Option<u64>> = self
            .pending_entries
            .iter()
            .map(|e| match e.kind {
                EntryKind::Data | EntryKind::Inline => {
                    self.extent_index.lookup(&e.hash).map(|loc| loc.body_offset)
                }
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => None,
            })
            .collect();

        let entries = std::mem::take(&mut self.pending_entries);
        let segment_ulid = self.mint.next();
        let pending_dir = self.base_dir.join("pending");

        // Open fresh WAL — new writes start flowing immediately.
        let (wal, wal_ulid, wal_path, _) =
            create_fresh_wal(&self.base_dir.join("wal"), self.mint.next())?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;

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
                EntryKind::Data | EntryKind::Inline => {}
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
            }
            let Some(old_wal_offset) = old_wal_offset else {
                continue;
            };
            let idata = if entry.kind == EntryKind::Inline {
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
            let (mut data, mut refs, mut zero, mut inline, mut delta) =
                (0usize, 0usize, 0usize, 0usize, 0usize);
            for e in &result.entries {
                match e.kind {
                    EntryKind::Data => data += 1,
                    EntryKind::DedupRef => refs += 1,
                    EntryKind::Zero => zero += 1,
                    EntryKind::Inline => inline += 1,
                    EntryKind::Delta => delta += 1,
                }
            }
            log::info!(
                "flush {}: {data} data, {inline} inline, {refs} dedup-ref, \
                 {zero} zero, {delta} delta ({} entries total)",
                result.segment_ulid,
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
    /// Mints four ULIDs (`u_repack < u_sweep < u_flush < u_wal`), fsyncs
    /// the WAL, snapshots CAS tokens, takes entries, builds a
    /// [`PromoteJob`] using `u_flush` as the segment ULID, and opens
    /// a fresh WAL at `u_wal`.  Writes resume immediately on the fresh
    /// WAL — no deferral needed.
    ///
    /// Returns `None` inside the `job` field if the WAL is empty (no
    /// segment to promote).  The checkpoint completes immediately in
    /// that case.
    pub fn prepare_gc_checkpoint(&mut self) -> io::Result<GcCheckpointPrep> {
        let u_repack = self.mint.next();
        let u_sweep = self.mint.next();
        let u_flush = self.mint.next();
        let u_wal = self.mint.next();

        self.wal.fsync()?;

        if self.pending_entries.is_empty() {
            // Empty WAL — delete the WAL file, open fresh WAL at u_wal.
            fs::remove_file(&self.wal_path)?;
            let (wal, wal_ulid, wal_path, _) = create_fresh_wal(&self.base_dir.join("wal"), u_wal)?;
            self.wal = wal;
            self.wal_ulid = wal_ulid;
            self.wal_path = wal_path;
            return Ok(GcCheckpointPrep {
                u_repack,
                u_sweep,
                u_flush,
                job: None,
            });
        }

        let old_wal_ulid = self.wal_ulid;
        let old_wal_path = self.wal_path.clone();

        let pre_promote_offsets: Vec<Option<u64>> = self
            .pending_entries
            .iter()
            .map(|e| match e.kind {
                EntryKind::Data | EntryKind::Inline => {
                    self.extent_index.lookup(&e.hash).map(|loc| loc.body_offset)
                }
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => None,
            })
            .collect();

        let entries = std::mem::take(&mut self.pending_entries);
        let pending_dir = self.base_dir.join("pending");

        // Open fresh WAL at u_wal — writes resume immediately.
        let (wal, wal_ulid, wal_path, _) = create_fresh_wal(&self.base_dir.join("wal"), u_wal)?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;

        Ok(GcCheckpointPrep {
            u_repack,
            u_sweep,
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

    // --- Decompress the delta blob using the source as the zstd dictionary. ---
    // The decompressed length equals the Delta entry's logical size
    // (`lba_length * 4096`). We don't have lba_length on ExtentRead
    // directly, but `er.range_end - er.range_start` gives the number
    // of LBAs in the portion we need, and the delta produces bytes
    // for the full fragment regardless of which portion we want.
    // Use a generous upper bound and slice the result.
    let mut decoder = zstd::bulk::Decompressor::with_dictionary(&source_bytes)
        .map_err(|e| io::Error::other(format!("zstd dict decoder: {e}")))?;
    // Uncompressed size bound: the Delta entry describes one fragment
    // of a file. We don't carry the exact uncompressed size here, so
    // pass a large enough capacity (16 MiB — the segment-size cap).
    let decompressed = decoder
        .decompress(&delta_blob, 16 * 1024 * 1024)
        .map_err(|e| io::Error::other(format!("zstd decompress: {e}")))?;

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
///   1. Current fork: `wal/`, `pending/`, `gc/*.applied`, `cache/<id>.body`
///   2. Ancestor forks (newest-first): `pending/`, `gc/*.applied`, `cache/<id>.body`
///   3. Demand-fetch via fetcher (writes three-file format to `cache/`)
///
/// For `Cached` entries, a `cache/<id>.body` hit is only accepted if the
/// corresponding bit in `cache/<id>.present` is set — otherwise the entry
/// is not yet locally available and we fall through to the fetcher.
///
/// `.idx` files live in `index/` (coordinator-written, permanent).
/// `.body` and `.present` files live in `cache/` (volume-managed read cache).
///
/// Extracted from `Volume::find_segment_file` so that `VolumeHandle` can serve
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
    // Self dir: full canonical precedence (wal → pending → gc/.applied → cache).
    // The `.applied` GC branch matters here because the extent index flips to the
    // new segment_id the moment the volume writes `.applied`, before the
    // coordinator has promoted the body to `pending/`.
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

/// A read-only view of a fork. Used for readonly NBD serving (no WAL, no write lock).
/// Reads work identically to `Volume`; writes and fsyncs are not supported.
pub struct ReadonlyVolume {
    base_dir: PathBuf,
    ancestor_layers: Vec<AncestorLayer>,
    lbamap: lbamap::LbaMap,
    extent_index: extentindex::ExtentIndex,
    file_cache: RefCell<FileCache>,
    fetcher: Option<BoxFetcher>,
}

impl ReadonlyVolume {
    /// Open a volume directory for read-only access.
    ///
    /// Does not create `wal/`, does not acquire an exclusive lock, and does not
    /// replay the WAL. WAL records from an active writer on the same volume will
    /// not be visible. Intended for the `--readonly` NBD serve path.
    pub fn open(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Self> {
        let (ancestor_layers, lbamap, extent_index) = open_read_state(fork_dir, by_id_dir)?;
        Ok(Self {
            base_dir: fork_dir.to_owned(),
            ancestor_layers,
            lbamap,
            extent_index,
            file_cache: RefCell::new(FileCache::default()),
            fetcher: None,
        })
    }

    /// Read `lba_count` 4KB blocks starting at `start_lba`.
    /// Unwritten blocks are returned as zeros.
    pub fn read(&self, start_lba: u64, lba_count: u32) -> io::Result<Vec<u8>> {
        read_extents(
            start_lba,
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

    /// Attach a `SegmentFetcher` for demand-fetch on segment cache miss.
    pub fn set_fetcher(&mut self, fetcher: BoxFetcher) {
        self.fetcher = Some(fetcher);
    }

    /// Return all fork directories in the ancestry chain, oldest-first,
    /// with the current fork last.
    pub fn fork_dirs(&self) -> Vec<PathBuf> {
        self.ancestor_layers
            .iter()
            .map(|l| l.dir.clone())
            .chain(std::iter::once(self.base_dir.clone()))
            .collect()
    }
}

/// Walk the fork ancestry chain and return ancestor layers, oldest-first.
/// Public so that `ls.rs` and other read-only tools can build the rebuild chain.
///
/// Walk the ancestry chain and rebuild the LBA map and extent index.
///
/// This is the common open-time setup shared by `Volume::open` and
/// `ReadonlyVolume::open`.  Returns the ancestor layers (oldest-first), the
/// rebuilt LBA map, and the rebuilt extent index.
fn open_read_state(
    fork_dir: &Path,
    by_id_dir: &Path,
) -> io::Result<(Vec<AncestorLayer>, lbamap::LbaMap, extentindex::ExtentIndex)> {
    // Fail-fast verification: every ancestor in the fork chain must have a
    // signed `.manifest` file whose listed `.idx` files are all present
    // locally. The trust chain is rooted in this volume's own pubkey and
    // walked via the `parent_pubkey` embedded in each child's provenance.
    verify_ancestor_manifests(fork_dir, by_id_dir)?;
    let ancestor_layers = walk_ancestors(fork_dir, by_id_dir)?;
    let lba_chain: Vec<(PathBuf, Option<String>)> = ancestor_layers
        .iter()
        .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
        .chain(std::iter::once((fork_dir.to_owned(), None)))
        .collect();
    let lbamap = lbamap::rebuild_segments(&lba_chain)?;

    // The extent index is seeded from both the fork ancestry (volume.parent)
    // and the extent-index ancestry (volume.extent_index). The extent-only
    // ancestors contribute hashes for dedup/delta without affecting the LBA
    // map, so reads never fall through to them.
    let extent_only = walk_extent_ancestors(fork_dir, by_id_dir)?;
    let mut hash_chain = lba_chain.clone();
    for layer in extent_only {
        if !hash_chain.iter().any(|(dir, _)| dir == &layer.dir) {
            hash_chain.push((layer.dir, layer.branch_ulid));
        }
    }
    let extent_index = extentindex::rebuild(&hash_chain)?;
    Ok((ancestor_layers, lbamap, extent_index))
}

/// Parse a `<source-ulid>/<snapshot-ulid>` lineage entry, validating
/// both components as ULIDs to prevent path traversal. Returns the source ULID
/// slice (borrowed from `entry`) and the owned snapshot ULID string.
fn parse_lineage_entry<'a>(
    entry: &'a str,
    field: &str,
    fork_dir: &Path,
) -> io::Result<(&'a str, String)> {
    let (source_ulid_str, snapshot_ulid_str) = entry.split_once('/').ok_or_else(|| {
        io::Error::other(format!(
            "malformed {field} entry in {}: {entry}",
            fork_dir.display()
        ))
    })?;
    if snapshot_ulid_str.contains('/') {
        return Err(io::Error::other(format!(
            "malformed {field} entry in {}: {entry} has more than one '/' separator",
            fork_dir.display()
        )));
    }
    let snapshot_ulid = Ulid::from_string(snapshot_ulid_str)
        .map_err(|e| io::Error::other(format!("bad snapshot ULID in {field}: {e}")))?
        .to_string();
    Ulid::from_string(source_ulid_str).map_err(|_| {
        io::Error::other(format!(
            "malformed {field} entry in {}: source '{source_ulid_str}' is not a valid ULID",
            fork_dir.display(),
        ))
    })?;
    Ok((source_ulid_str, snapshot_ulid))
}

/// A volume with no `volume.provenance` is treated as root (empty chain).
/// All other provenance read errors propagate — in particular, a missing
/// or malformed file on a volume that had lineage is a loud failure.
fn load_lineage_or_empty(fork_dir: &Path) -> io::Result<crate::signing::ProvenanceLineage> {
    let provenance_path = fork_dir.join(crate::signing::VOLUME_PROVENANCE_FILE);
    if !provenance_path.exists() {
        return Ok(crate::signing::ProvenanceLineage::default());
    }
    crate::signing::read_lineage_verifying_signature(
        fork_dir,
        crate::signing::VOLUME_PUB_FILE,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )
}

/// Each layer holds the ancestor fork directory and the branch-point ULID.
/// Segments with ULID > `branch_ulid` in that ancestor fork were written
/// after the branch and are excluded when rebuilding the LBA map.
///
/// A volume with no `volume.provenance` or with an empty `parent` field is
/// the root of its fork chain; returns an empty vec. The `parent` field is
/// in the form `<parent-ulid>/snapshots/<branch-ulid>`, validated as ULIDs
/// at parse time.
/// Resolve an ancestor volume directory by ULID.
///
/// An ancestor may live in the writable `by_id/<ulid>/` tree or in the
/// readonly pulled tree `readonly/<ulid>/`. Prefer `by_id/` when both exist:
/// a locally writable copy supersedes a pulled readonly skeleton.
///
/// Falls back to `by_id_dir.join(ulid)` when neither candidate is present so
/// that callers (and tests) get a deterministic path they can report in errors.
pub fn resolve_ancestor_dir(by_id_dir: &Path, ulid: &str) -> PathBuf {
    let by_id_candidate = by_id_dir.join(ulid);
    if by_id_candidate.exists() {
        return by_id_candidate;
    }
    if let Some(parent) = by_id_dir.parent() {
        let readonly_candidate = parent.join("readonly").join(ulid);
        if readonly_candidate.exists() {
            return readonly_candidate;
        }
    }
    by_id_candidate
}

/// Verify every ancestor of `fork_dir` by walking the fork chain from the
/// current volume, using the `parent_pubkey` embedded in each child's
/// signed provenance as the trust anchor for the next link.
///
/// For each ancestor in the chain:
/// 1. Verify the ancestor's `volume.provenance` under the pubkey the child
///    signed over (NOT the `volume.pub` on disk at the ancestor path).
/// 2. Read the ancestor's `snapshots/<snap_ulid>.manifest` file, also
///    verified under the same pubkey.
/// 3. Assert every segment ULID listed in the manifest is present as
///    `index/<ulid>.idx` in the ancestor directory.
///
/// Fails fast on any missing file, failed signature, or missing `.idx`.
/// Does not perform any demand-fetch — the caller is expected to prefetch
/// ancestor data before opening a fork.
///
/// The trust root is the current volume's own `volume.pub`, which the
/// caller has already validated as the identity of the volume they asked
/// to open.
pub fn verify_ancestor_manifests(fork_dir: &Path, by_id_dir: &Path) -> io::Result<()> {
    // Fast-path: if this volume has no parent, nothing to verify.
    let provenance_path = fork_dir.join(crate::signing::VOLUME_PROVENANCE_FILE);
    if !provenance_path.exists() {
        return Ok(());
    }
    let own_pubkey = crate::signing::load_verifying_key(fork_dir, crate::signing::VOLUME_PUB_FILE)?;
    let own_lineage = crate::signing::read_lineage_with_key(
        fork_dir,
        &own_pubkey,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )?;
    let Some(mut current_parent) = own_lineage.parent else {
        return Ok(());
    };

    loop {
        let parent_dir = resolve_ancestor_dir(by_id_dir, &current_parent.volume_ulid);
        if !parent_dir.exists() {
            return Err(io::Error::other(format!(
                "ancestor {} not found locally (run `elide volume remote pull` first)",
                current_parent.volume_ulid
            )));
        }
        let parent_verifying = crate::signing::VerifyingKey::from_bytes(&current_parent.pubkey)
            .map_err(|e| {
                io::Error::other(format!(
                    "invalid parent pubkey in provenance for {}: {e}",
                    current_parent.volume_ulid
                ))
            })?;

        let snap_ulid = Ulid::from_string(&current_parent.snapshot_ulid).map_err(|e| {
            io::Error::other(format!("invalid snapshot ULID in provenance parent: {e}"))
        })?;
        let segments =
            crate::signing::read_snapshot_manifest(&parent_dir, &parent_verifying, &snap_ulid)?;

        let index_dir = parent_dir.join("index");
        for seg in &segments {
            let idx_path = index_dir.join(format!("{seg}.idx"));
            if !idx_path.exists() {
                return Err(io::Error::other(format!(
                    "ancestor {} snapshot {}: missing index/{}.idx",
                    current_parent.volume_ulid, snap_ulid, seg
                )));
            }
        }

        // Advance to this ancestor's own parent (if any), verifying its
        // provenance under the pubkey we already trust (from the previous
        // child's embedded parent_pubkey).
        let parent_lineage = crate::signing::read_lineage_with_key(
            &parent_dir,
            &parent_verifying,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )?;
        let Some(next) = parent_lineage.parent else {
            return Ok(());
        };
        current_parent = next;
    }
}

pub fn walk_ancestors(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Vec<AncestorLayer>> {
    let lineage = load_lineage_or_empty(fork_dir)?;
    let Some(parent) = lineage.parent else {
        return Ok(Vec::new());
    };
    let parent_fork_dir = resolve_ancestor_dir(by_id_dir, &parent.volume_ulid);

    // Recurse into the parent's fork chain first (builds oldest-first order).
    let mut ancestors = walk_ancestors(&parent_fork_dir, by_id_dir)?;
    ancestors.push(AncestorLayer {
        dir: parent_fork_dir,
        branch_ulid: Some(parent.snapshot_ulid),
    });
    Ok(ancestors)
}

/// Read the flat extent-index source list from `volume.provenance`.
///
/// The `extent_index` field is a flat list of
/// `<source-ulid>/<snapshot-ulid>` entries, each naming a snapshot
/// whose extents populate this volume's `ExtentIndex` (for dedup and delta
/// compression source lookups) but are **never** merged into the LBA map.
/// The child is born with an empty LBA map; hashes from these sources are
/// only consulted when the child writes an extent whose content hash matches
/// a source extent, in which case the child emits a `DedupRef` pointing at
/// the source segment. See `docs/architecture.md` for the "not in read path"
/// invariant.
///
/// The list is flat, not a chain: when a new volume is imported with
/// `--extents-from X`, the coordinator reads `X`'s own extent_index list,
/// appends `X`, dedupes, and writes the result into the new provenance.
/// There is no recursion at attach time — the list is already fully
/// expanded. Multiple sources passed at import time each contribute their
/// (already-flat) lists, concatenated and deduped by directory path.
pub fn walk_extent_ancestors(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Vec<AncestorLayer>> {
    let lineage = load_lineage_or_empty(fork_dir)?;
    let mut layers: Vec<AncestorLayer> = Vec::new();
    for entry in &lineage.extent_index {
        let (source_ulid_str, snapshot_ulid) =
            parse_lineage_entry(entry, "extent_index", fork_dir)?;
        let source_dir = resolve_ancestor_dir(by_id_dir, source_ulid_str);
        if layers.iter().any(|l| l.dir == source_dir) {
            continue;
        }
        layers.push(AncestorLayer {
            dir: source_dir,
            branch_ulid: Some(snapshot_ulid),
        });
    }
    Ok(layers)
}

/// Return the latest snapshot ULID string for a fork, or `None` if no
/// snapshots exist. Snapshots live as plain files under `fork_dir/snapshots/`.
pub fn latest_snapshot(fork_dir: &Path) -> io::Result<Option<Ulid>> {
    let snapshots_dir = fork_dir.join("snapshots");
    let iter = match fs::read_dir(&snapshots_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let latest = iter
        .filter_map(|e| e.ok())
        .filter_map(|e| Ulid::from_string(e.file_name().to_str()?).ok())
        .max();
    Ok(latest)
}

/// Create a new volume directory, branched from the latest snapshot of the source volume.
///
/// The source volume must have at least one snapshot (written by `snapshot()`).
/// `new_fork_dir` is created with `wal/` and `pending/`, a fresh keypair is
/// generated, and a signed `volume.provenance` is written recording the
/// fork's `parent` field in the form `<source-ulid>/snapshots/<branch-ulid>`.
/// The source ULID is derived from `source_fork_dir`'s directory name.
///
/// Returns `Ok(())` on success; `new_fork_dir` must not already exist.
pub fn fork_volume(new_fork_dir: &Path, source_fork_dir: &Path) -> io::Result<()> {
    let branch_ulid = latest_snapshot(source_fork_dir)?.ok_or_else(|| {
        io::Error::other(format!(
            "source volume '{}' has no snapshots; run snapshot-volume first",
            source_fork_dir.display()
        ))
    })?;
    fork_volume_at(new_fork_dir, source_fork_dir, branch_ulid)
}

/// Like `fork_volume` but pins the fork to an explicit snapshot ULID.
///
/// Used by `volume fork --from <vol_ulid>/<snap_ulid>` when the caller
/// wants the branch point to be something other than the source volume's
/// latest snapshot — typically because the source is a pulled readonly
/// ancestor and the caller has a specific snapshot ULID in mind.
///
/// The snapshot is **not** required to exist as a local file: a pulled
/// readonly ancestor may not have its snapshot markers prefetched yet at
/// the time of forking. The snapshot ULID is still recorded in the child's
/// signed provenance and will be resolved at open time once prefetch has
/// populated the ancestor's `snapshots/` directory.
pub fn fork_volume_at(
    new_fork_dir: &Path,
    source_fork_dir: &Path,
    branch_ulid: Ulid,
) -> io::Result<()> {
    if new_fork_dir.exists() {
        return Err(io::Error::other(format!(
            "fork directory '{}' already exists",
            new_fork_dir.display()
        )));
    }

    // Canonicalize so that symlink paths (e.g. by_name/<name>) resolve to
    // their real by_id/<ulid> directory before we extract the ULID component.
    let source_real = fs::canonicalize(source_fork_dir)?;
    let source_ulid = source_real
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("source fork dir has no name"))?;
    // Validate the source directory name really is a ULID before we embed
    // it in the child's provenance as an ancestor reference.
    Ulid::from_string(source_ulid).map_err(|e| {
        io::Error::other(format!(
            "source fork dir name is not a ULID ({}): {e}",
            source_real.display()
        ))
    })?;

    fs::create_dir_all(new_fork_dir.join("wal"))?;
    fs::create_dir_all(new_fork_dir.join("pending"))?;

    // Generate a fresh keypair for the new fork. Every writable volume must have
    // a signing key; the fork gets its own identity independent of its parent.
    // The signing key's in-memory form is reused immediately to write provenance
    // so we never have to re-read it from disk.
    let key = crate::signing::generate_keypair(
        new_fork_dir,
        crate::signing::VOLUME_KEY_FILE,
        crate::signing::VOLUME_PUB_FILE,
    )?;

    // Write signed provenance carrying the fork's parent reference. Extent
    // index is empty for forks — fork ancestry is a read-path relationship
    // tracked in `parent`, not a hash-pool relationship.
    //
    // Embed the parent's current public key under the child's signature so
    // the fork's open-time ancestor walk has a trust anchor for the parent's
    // own signed artefacts — see `ParentRef` in signing.rs.
    let parent_pubkey =
        crate::signing::load_verifying_key(&source_real, crate::signing::VOLUME_PUB_FILE)?;
    let lineage = crate::signing::ProvenanceLineage {
        parent: Some(crate::signing::ParentRef {
            volume_ulid: source_ulid.to_owned(),
            snapshot_ulid: branch_ulid.to_string(),
            pubkey: parent_pubkey.to_bytes(),
        }),
        extent_index: Vec::new(),
    };
    crate::signing::write_provenance(
        new_fork_dir,
        &key,
        crate::signing::VOLUME_PROVENANCE_FILE,
        &lineage,
    )?;

    Ok(())
}

// --- WAL helpers ---

/// Scan a WAL file and replay its records into `lbamap` + `extent_index`,
/// returning the WAL ULID, the valid (non-partial) tail size, and the
/// reconstructed pending_entries list.
///
/// Shared between:
/// - [`recover_wal`], which also reopens the file for continued appending
///   (latest WAL case).
/// - [`Volume::open_impl`]'s recovery-time promote loop, which promotes
///   each non-latest WAL to a fresh segment and deletes the WAL file
///   rather than reopening it.
///
/// `writelog::scan` truncates any partial-tail record before returning.
fn replay_wal_records(
    path: &Path,
    lbamap: &mut lbamap::LbaMap,
    extent_index: &mut extentindex::ExtentIndex,
) -> io::Result<(Ulid, u64, Vec<segment::SegmentEntry>)> {
    let ulid_str = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("bad WAL filename"))?;
    let ulid = Ulid::from_string(ulid_str).map_err(|e| io::Error::other(e.to_string()))?;

    let (records, valid_size) = writelog::scan(path)?;

    let mut pending_entries = Vec::new();
    for record in records {
        match record {
            writelog::LogRecord::Data {
                hash,
                start_lba,
                lba_length,
                flags,
                body_offset,
                data,
            } => {
                let body_length = data.len() as u32;
                let compressed = flags.contains(writelog::WalFlags::COMPRESSED);
                // Translate WalFlags → SegmentFlags: the two namespaces use different
                // bit values (WalFlags::COMPRESSED = 0x01, SegmentFlags::COMPRESSED = 0x04).
                let seg_flags = if compressed {
                    segment::SegmentFlags::COMPRESSED
                } else {
                    segment::SegmentFlags::empty()
                };
                lbamap.insert(start_lba, lba_length, hash);
                // Temporary WAL offset — updated to segment offset on promotion.
                extent_index.insert(
                    hash,
                    extentindex::ExtentLocation {
                        segment_id: ulid,
                        body_offset,
                        body_length,
                        compressed,
                        body_source: BodySource::Local,
                        body_section_start: 0,
                        inline_data: None,
                    },
                );
                pending_entries.push(segment::SegmentEntry::new_data(
                    hash, start_lba, lba_length, seg_flags, data,
                ));
            }
            writelog::LogRecord::Ref {
                hash,
                start_lba,
                lba_length,
            } => {
                lbamap.insert(start_lba, lba_length, hash);
                // REF: no body bytes, no body reservation, no extent_index
                // update. The canonical entry is populated from whichever
                // segment holds the DATA for this hash.
                pending_entries.push(segment::SegmentEntry::new_dedup_ref(
                    hash, start_lba, lba_length,
                ));
            }
            writelog::LogRecord::Zero {
                start_lba,
                lba_length,
            } => {
                lbamap.insert(start_lba, lba_length, ZERO_HASH);
                pending_entries.push(segment::SegmentEntry::new_zero(start_lba, lba_length));
            }
        }
    }

    Ok((ulid, valid_size, pending_entries))
}

/// Scan an existing WAL, replay its records into `lbamap`, rebuild
/// `pending_entries`, and reopen the WAL for continued appending.
///
/// This is the single WAL scan on startup — it both updates the LBA map
/// (WAL is more recent than any segment) and recovers the pending_entries
/// list needed for the next promotion.
fn recover_wal(
    path: PathBuf,
    lbamap: &mut lbamap::LbaMap,
    extent_index: &mut extentindex::ExtentIndex,
) -> io::Result<(
    writelog::WriteLog,
    Ulid,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let (ulid, valid_size, pending_entries) = replay_wal_records(&path, lbamap, extent_index)?;
    let wal = writelog::WriteLog::reopen(&path, valid_size)?;
    Ok((wal, ulid, path, pending_entries))
}

/// Create a new WAL file using the provided `ulid`.
///
/// The caller is responsible for generating a ULID that sorts after all
/// existing segments and WAL files (typically via `Volume::mint`).
fn create_fresh_wal(
    wal_dir: &Path,
    ulid: Ulid,
) -> io::Result<(
    writelog::WriteLog,
    Ulid,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let path = wal_dir.join(ulid.to_string());
    let wal = writelog::WriteLog::create(&path)?;
    Ok((wal, ulid, path, Vec::new()))
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("elide-volume-test-{}-{}", std::process::id(), n));
        p
    }

    /// Simulate coordinator drain: upload all pending segments to S3 (no-op in
    /// tests) then call `promote_segment` on each.  `promote_segment` writes
    /// `index/<ulid>.idx`, copies the body to `cache/`, and deletes `pending/<ulid>`.
    fn simulate_upload(vol: &mut Volume) {
        let pending_dir = vol.base_dir.join("pending");
        for entry in std::fs::read_dir(&pending_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if name.ends_with(".tmp") {
                continue;
            }
            let ulid = ulid::Ulid::from_string(&name).unwrap();
            vol.promote_segment(ulid).unwrap();
        }
    }

    /// Generate a keypair and write `volume.key` + `volume.pub` into `dir`.
    ///
    /// Must be called before `Volume::open` in any test that creates a volume.
    fn write_test_keypair(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        let key = crate::signing::generate_keypair(
            dir,
            crate::signing::VOLUME_KEY_FILE,
            crate::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        // Match production `volume up` behaviour: a fresh writable volume
        // also gets a default (root) `volume.provenance`. Skipping this
        // makes `Volume::open` fail in the ancestor walk when another
        // volume forks from this one, because the child's provenance
        // refers back to a volume whose own provenance is missing.
        crate::signing::write_provenance(
            dir,
            &key,
            crate::signing::VOLUME_PROVENANCE_FILE,
            &crate::signing::ProvenanceLineage::default(),
        )
        .unwrap();
    }

    /// Write a signed `volume.provenance` with the given lineage fields into
    /// `dir`. Routes through `write_raw_provenance_for_test` so that
    /// syntactically bad `parent_entry` strings can be persisted for
    /// parse-error coverage — the file signature is still valid over the raw
    /// bytes, so the parse error fires before signature verification.
    ///
    /// When `parent_entry` is `Some`, an all-zero dummy `parent_pubkey` is
    /// embedded. Tests that walk the chain only care about structural fields.
    fn write_test_provenance(dir: &Path, parent_entry: Option<&str>, extent_entries: &[&str]) {
        let (raw_parent, raw_parent_pubkey) = match parent_entry {
            Some(p) => (p.to_owned(), crate::signing::encode_hex(&[0u8; 32])),
            None => (String::new(), String::new()),
        };
        let extent_owned: Vec<String> = extent_entries.iter().map(|s| (*s).to_owned()).collect();
        crate::signing::write_raw_provenance_for_test(
            dir,
            &raw_parent,
            &raw_parent_pubkey,
            &extent_owned,
        )
        .unwrap();
    }

    /// Create a temp dir and pre-populate it with a test keypair.
    ///
    /// Use in place of `temp_dir()` whenever the dir will be passed directly
    /// to `Volume::open`.
    fn keyed_temp_dir() -> PathBuf {
        let dir = temp_dir();
        write_test_keypair(&dir);
        dir
    }

    #[test]
    fn open_creates_directories() {
        let base = keyed_temp_dir();
        let _ = Volume::open(&base, &base).unwrap();
        assert!(base.join("wal").is_dir());
        assert!(base.join("pending").is_dir());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_is_idempotent() {
        let base = keyed_temp_dir();
        let _ = Volume::open(&base, &base).unwrap();
        // Second open on the same dir should succeed (dirs already exist).
        let _ = Volume::open(&base, &base).unwrap();
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_single_block() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![0x42u8; 4096]).unwrap();
        vol.fsync().unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_multi_block_extent() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        // Write 8 LBAs (32 KiB) as a single call.
        vol.write(10, &vec![0xabu8; 8 * 4096]).unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn noop_skip_same_lba_same_content() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let data = vec![0x42u8; 4096];

        vol.write(0, &data).unwrap();
        let before = vol.noop_stats();
        assert_eq!(before.skipped_writes, 0);
        assert_eq!(before.skipped_bytes, 0);

        // Same LBA, same content — short-circuited by the LBA-map hash check.
        vol.write(0, &data).unwrap();
        let after = vol.noop_stats();
        assert_eq!(after.skipped_writes, 1);
        assert_eq!(after.skipped_bytes, 4096);

        // Data still reads back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), data);
        // LBA map still has exactly one entry.
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn noop_skip_different_content_falls_through() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let a = vec![0x42u8; 4096];
        let b = vec![0x99u8; 4096];

        vol.write(0, &a).unwrap();
        vol.write(0, &b).unwrap();
        let stats = vol.noop_stats();
        assert_eq!(stats.skipped_writes, 0);
        // Latest write wins.
        assert_eq!(vol.read(0, 1).unwrap(), b);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn noop_skip_after_promotion() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let data = vec![0xaau8; 4 * 4096];

        vol.write(10, &data).unwrap();
        vol.flush_wal().unwrap(); // promote WAL → pending/
        // Body now lives in a pending segment file (BodySource::Local).
        vol.write(10, &data).unwrap();

        let stats = vol.noop_stats();
        assert_eq!(stats.skipped_writes, 1);
        assert_eq!(stats.skipped_bytes, 4 * 4096);
        assert_eq!(vol.read(10, 4).unwrap(), data);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn noop_skip_multi_block_same_content() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let data: Vec<u8> = (0..8 * 4096).map(|i| (i as u8).wrapping_mul(17)).collect();

        vol.write(32, &data).unwrap();
        vol.write(32, &data).unwrap();

        let stats = vol.noop_stats();
        assert_eq!(stats.skipped_writes, 1);
        assert_eq!(stats.skipped_bytes, 8 * 4096);
        assert_eq!(vol.read(32, 8).unwrap(), data);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn noop_skip_does_not_fire_on_fragmented_match() {
        // The hash check keys on a single LBA-map entry that exactly
        // covers the incoming range. When the existing content is split
        // into two entries whose concatenation matches, no single map
        // entry hashes the whole range — the skip cannot fire and the
        // write commits normally. (Earlier designs added a body
        // byte-compare tier to catch this; see
        // `docs/design-noop-write-skip.md § Why no byte-compare tier`.)
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let a = vec![0xa1u8; 4096];
        let b = vec![0xb2u8; 4096];

        vol.write(0, &a).unwrap();
        vol.write(1, &b).unwrap();

        let mut combined = Vec::with_capacity(8192);
        combined.extend_from_slice(&a);
        combined.extend_from_slice(&b);
        vol.write(0, &combined).unwrap();

        let stats = vol.noop_stats();
        assert_eq!(stats.skipped_writes, 0);
        // The fresh 8 KiB write replaces the two split entries with one.
        assert_eq!(vol.lbamap_len(), 1);
        // Read still returns the expected concatenation.
        assert_eq!(vol.read(0, 2).unwrap(), combined);
        fs::remove_dir_all(base).unwrap();
    }

    // ---------- extent reclamation (alias-merge) ----------

    /// Produce a 4096-byte block whose bytes depend on `seed` and `block_idx`,
    /// giving incompressible, distinct content per block so that splitting an
    /// originally-contiguous payload exposes the fragmentation clearly.
    fn reclaim_block(seed: u8, block_idx: usize) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        let key = [seed; 32];
        let mut hasher = blake3::Hasher::new_keyed(&key);
        hasher.update(&(block_idx as u64).to_le_bytes());
        let mut xof = hasher.finalize_xof();
        xof.fill(&mut buf);
        buf
    }

    fn reclaim_payload(seed: u8, n_blocks: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n_blocks * 4096);
        for i in 0..n_blocks {
            out.extend_from_slice(&reclaim_block(seed, i));
        }
        out
    }

    /// Write a single 8-block entry, overwrite the middle 2 blocks with a
    /// smaller (1-block) entry so the original is split prefix/tail, then
    /// run alias-merge over the whole range. The split-tail entry has
    /// `payload_block_offset != 0`, so the primitive should detect bloat
    /// and rewrite both the prefix and the tail as fresh compact entries.
    #[test]
    fn reclaim_alias_merge_rewrites_split_entry() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Big 8-block write at LBA 100.
        let big = reclaim_payload(0xA1, 8);
        vol.write(100, &big).unwrap();
        // Overwrite LBA 103 (1 block, middle) with unrelated content. The map
        // now has [100,103)@0 (hash A) + [103,104)@0 (hash B) + [104,108)@4 (hash A).
        let hole = [0x77u8; 4096];
        vol.write(103, &hole).unwrap();

        // Oracle expected bytes at [100, 108).
        let mut expected = vec![0u8; 8 * 4096];
        expected[..3 * 4096].copy_from_slice(&big[..3 * 4096]);
        expected[3 * 4096..4 * 4096].copy_from_slice(&hole);
        expected[4 * 4096..].copy_from_slice(&big[4 * 4096..]);
        assert_eq!(vol.read(100, 8).unwrap(), expected);

        // Before: 3 entries.
        assert_eq!(vol.lbamap_len(), 3);

        // Phase 1.
        let plan = vol.reclaim_snapshot(100, 8);
        // Phase 2 — use the on-actor read path directly; MVP proptest path too.
        let proposed = plan
            .compute_rewrites(|lba, len| vol.read(lba, len))
            .unwrap();
        // Two rewrites: prefix run [100,103) and tail run [104,108). The
        // middle [103,104) is a clean single-block entry with offset=0 —
        // its hash has no `offset != 0` run anywhere, so it's left alone.
        assert_eq!(proposed.len(), 2);
        let starts: Vec<u64> = proposed.iter().map(|p| p.start_lba).collect();
        assert_eq!(starts, vec![100, 104]);

        // Phase 3.
        let outcome = vol.reclaim_commit(plan, proposed).unwrap();
        assert!(!outcome.discarded);
        assert_eq!(outcome.runs_rewritten, 2);
        assert_eq!(outcome.bytes_rewritten, (3 + 4) * 4096);

        // Readback still matches.
        assert_eq!(vol.read(100, 8).unwrap(), expected);

        // Second pass is an idempotent no-op: hashes are now stable, the
        // LBA-map skip catches every rewrite the planner would propose.
        let plan2 = vol.reclaim_snapshot(100, 8);
        let proposed2 = plan2
            .compute_rewrites(|lba, len| vol.read(lba, len))
            .unwrap();
        // All three entries now have payload_block_offset == 0 and cover their
        // whole (contained) body — no bloat signal, no proposals.
        assert!(proposed2.is_empty());

        fs::remove_dir_all(base).unwrap();
    }

    /// Query range that slices mid-way through a hash's span: the prefix of
    /// the big write is outside the query. Containment check must refuse to
    /// rewrite — doing so would strand the outside references on the bloated
    /// body and *introduce* fragmentation.
    #[test]
    fn reclaim_alias_merge_skips_non_contained_hash() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Map state: [100, 150) → H/offset=0 (single big entry).
        let big = reclaim_payload(0x3C, 50);
        vol.write(100, &big).unwrap();

        // Query only the tail half — H's first 25 blocks live outside.
        let plan = vol.reclaim_snapshot(125, 25);
        let proposed = plan
            .compute_rewrites(|lba, len| vol.read(lba, len))
            .unwrap();
        // Containment fails: H has a run [100, 150) which starts at 100, outside
        // the query [125, 150). No rewrite.
        assert!(proposed.is_empty());

        fs::remove_dir_all(base).unwrap();
    }

    /// When the LBA map is mutated between phase 1 and phase 3, reclaim_commit
    /// must discard cleanly with no state change.
    #[test]
    fn reclaim_alias_merge_discards_on_concurrent_mutation() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let big = reclaim_payload(0x5E, 8);
        vol.write(200, &big).unwrap();
        let hole = [0x11u8; 4096];
        vol.write(203, &hole).unwrap();

        let plan = vol.reclaim_snapshot(200, 8);
        let proposed = plan
            .compute_rewrites(|lba, len| vol.read(lba, len))
            .unwrap();
        assert!(!proposed.is_empty());

        // Simulate concurrent mutation: force the Volume's lbamap Arc to
        // reallocate by doing any mutation (here: a write to an unrelated LBA
        // that still clones the Arc via Arc::make_mut because phase 1 is
        // holding a reference).
        vol.write(500, &reclaim_payload(0x77, 1)).unwrap();

        // Phase 3 must detect the mutation and discard.
        let outcome = vol.reclaim_commit(plan, proposed).unwrap();
        assert!(outcome.discarded);
        assert_eq!(outcome.runs_rewritten, 0);
        assert_eq!(outcome.bytes_rewritten, 0);

        fs::remove_dir_all(base).unwrap();
    }

    /// Zero extents carry no body and must never be rewritten by alias-merge.
    #[test]
    fn reclaim_alias_merge_skips_zero_extents() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write_zeroes(300, 10).unwrap();
        // Split the zero extent with an unrelated data write so the tail
        // ends up with payload_block_offset != 0. Our rule would normally
        // treat that as "bloat" for any non-zero hash, but ZERO_HASH is
        // always skipped.
        vol.write(304, &[0xABu8; 4096]).unwrap();

        let plan = vol.reclaim_snapshot(300, 10);
        let proposed = plan
            .compute_rewrites(|lba, len| vol.read(lba, len))
            .unwrap();
        assert!(proposed.is_empty());

        fs::remove_dir_all(base).unwrap();
    }

    // ---------- reclaim candidate scanner ----------

    fn scanner_thresholds_permissive() -> crate::volume::ReclaimThresholds {
        // Loose thresholds so tests can use small payloads while still
        // exercising the scanner's detection logic.
        crate::volume::ReclaimThresholds {
            min_dead_blocks: 1,
            min_dead_ratio: 0.0,
            min_stored_bytes: 0,
        }
    }

    /// A clean volume with a single compact write produces no candidates.
    #[test]
    fn scan_reclaim_candidates_no_bloat() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(100, &reclaim_payload(0x11, 8)).unwrap();

        let (lbamap, extent_index) = vol.snapshot_maps();
        let candidates = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            scanner_thresholds_permissive(),
        );
        assert!(
            candidates.is_empty(),
            "fresh compact entry should not be a candidate, got {candidates:?}"
        );

        fs::remove_dir_all(base).unwrap();
    }

    /// Write an 8-block entry, overwrite the middle 2 blocks with a
    /// 2-block entry. The original's payload now has dead bytes (blocks
    /// 3..4 are LBA-overwritten). The scanner should flag exactly one
    /// candidate covering the full extent of the original hash's
    /// surviving runs.
    #[test]
    fn scan_reclaim_candidates_flags_split_entry() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Big incompressible 8-block write so payload lives in the body.
        vol.write(200, &reclaim_payload(0x22, 8)).unwrap();
        // Overwrite the middle 2 blocks with unrelated content.
        let hole: Vec<u8> = (0..2).flat_map(|_| [0x77u8; 4096]).collect();
        vol.write(203, &hole).unwrap();

        let (lbamap, extent_index) = vol.snapshot_maps();
        let candidates = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            scanner_thresholds_permissive(),
        );
        assert_eq!(
            candidates.len(),
            1,
            "expected exactly one candidate for the bloated hash, got {candidates:?}"
        );
        let c = candidates[0];
        // Tight LBA bound: the hash's first live run starts at 200, its
        // last live run ends at 208.
        assert_eq!(c.start_lba, 200);
        assert_eq!(c.lba_length, 8);
        // Live: 3 prefix + 3 tail = 6. Dead: 2 (the hole in the middle).
        assert_eq!(c.live_blocks, 6);
        assert_eq!(c.dead_blocks, 2);

        fs::remove_dir_all(base).unwrap();
    }

    /// Thresholds are respected: bumping `min_dead_blocks` above the
    /// actual dead count drops the candidate.
    #[test]
    fn scan_reclaim_candidates_respects_min_dead_blocks() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(300, &reclaim_payload(0x33, 8)).unwrap();
        vol.write(303, &[0x99u8; 4096]).unwrap(); // 1 block hole

        let (lbamap, extent_index) = vol.snapshot_maps();

        // With min_dead_blocks=1 the 1-block hole qualifies.
        let loose = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            crate::volume::ReclaimThresholds {
                min_dead_blocks: 1,
                min_dead_ratio: 0.0,
                min_stored_bytes: 0,
            },
        );
        assert_eq!(loose.len(), 1);

        // With min_dead_blocks=2 it does not.
        let strict = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            crate::volume::ReclaimThresholds {
                min_dead_blocks: 2,
                min_dead_ratio: 0.0,
                min_stored_bytes: 0,
            },
        );
        assert!(strict.is_empty());

        fs::remove_dir_all(base).unwrap();
    }

    /// Candidates produced by the scanner must always be valid inputs
    /// to `reclaim_alias_merge` — round-trip through the primitive
    /// rewrites the hash and a rescan produces no further candidates
    /// for it.
    #[test]
    fn scan_reclaim_candidates_round_trip_through_primitive() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(400, &reclaim_payload(0x44, 8)).unwrap();
        vol.write(404, &[0x11u8; 4096]).unwrap();

        // Oracle: bytes at [400, 408) after the second write.
        let expected = {
            let mut buf = vec![0u8; 8 * 4096];
            let orig = reclaim_payload(0x44, 8);
            buf[..4 * 4096].copy_from_slice(&orig[..4 * 4096]);
            buf[4 * 4096..5 * 4096].fill(0x11);
            buf[5 * 4096..].copy_from_slice(&orig[5 * 4096..]);
            buf
        };
        assert_eq!(vol.read(400, 8).unwrap(), expected);

        let (lbamap, extent_index) = vol.snapshot_maps();
        let candidates = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            scanner_thresholds_permissive(),
        );
        assert_eq!(candidates.len(), 1);
        let c = candidates[0];

        // Drop the snapshot clones before mutating Volume state, so
        // Arc::make_mut in the primitive doesn't reallocate.
        drop(lbamap);
        drop(extent_index);

        let plan = vol.reclaim_snapshot(c.start_lba, c.lba_length);
        let proposed = plan
            .compute_rewrites(|lba, len| vol.read(lba, len))
            .unwrap();
        assert!(!proposed.is_empty());
        let outcome = vol.reclaim_commit(plan, proposed).unwrap();
        assert!(!outcome.discarded);
        assert!(outcome.runs_rewritten > 0);

        // Content preserved.
        assert_eq!(vol.read(400, 8).unwrap(), expected);

        // Rescan: the old hash is gone from the LBA map, the new
        // compact ones have no bloat — zero candidates.
        let (lbamap2, extent_index2) = vol.snapshot_maps();
        let candidates2 = crate::volume::scan_reclaim_candidates(
            &lbamap2,
            &extent_index2,
            scanner_thresholds_permissive(),
        );
        assert!(
            candidates2.is_empty(),
            "rescan after reclaim should find nothing, got {candidates2:?}"
        );

        fs::remove_dir_all(base).unwrap();
    }

    /// Idempotent-convergence property: a reclaim pass over an
    /// already-optimal range produces no proposals at all, and any that
    /// did slip through would be absorbed by the noop-skip hash check
    /// when committed via `write_with_hash`.
    #[test]
    fn reclaim_alias_merge_optimal_range_is_noop() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(400, &reclaim_payload(0x7A, 4)).unwrap();

        let plan = vol.reclaim_snapshot(400, 4);
        let proposed = plan
            .compute_rewrites(|lba, len| vol.read(lba, len))
            .unwrap();
        assert!(proposed.is_empty());
        let outcome = vol.reclaim_commit(plan, proposed).unwrap();
        assert_eq!(outcome.runs_rewritten, 0);
        assert!(!outcome.discarded);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_rejects_empty() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let err = vol.write(0, &[]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_rejects_misaligned() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let err = vol.write(0, &[0u8; 1000]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_sets_needs_promote_after_threshold() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write 33 × 1 MiB of incompressible data to exceed FLUSH_THRESHOLD (32 MiB).
        // Each block uses a unique byte value so entropy is high and compression is skipped.
        let mut block = vec![0u8; 1024 * 1024];
        for i in 0u64..33 {
            // Fill with a pattern that defeats compression: vary every byte.
            let fill = (i & 0xFF) as u8;
            for (j, b) in block.iter_mut().enumerate() {
                *b = fill ^ (j as u8).wrapping_mul(0x6D).wrapping_add(0x4F);
            }
            vol.write(i * 256, &block).unwrap();
        }

        // writes no longer auto-promote; needs_promote() should be true.
        assert!(
            vol.needs_promote(),
            "expected needs_promote() after 33 MiB of writes"
        );

        // Explicit flush_wal() should promote to pending/.
        vol.flush_wal().unwrap();

        // At least one segment should have been promoted to pending/.
        let has_pending = fs::read_dir(base.join("pending"))
            .unwrap()
            .any(|e| e.is_ok());
        assert!(
            has_pending,
            "expected at least one promoted segment in pending/"
        );

        // A fresh WAL should have been created.
        let wal_count = fs::read_dir(base.join("wal"))
            .unwrap()
            .filter(|e| e.is_ok())
            .count();
        assert_eq!(
            wal_count, 1,
            "expected exactly one WAL file after promotion"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn recovery_rebuilds_lbamap() {
        let base = keyed_temp_dir();

        // Write two blocks, fsync, then drop (simulates clean shutdown before promotion).
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &vec![1u8; 4096]).unwrap();
            vol.write(1, &vec![2u8; 4096]).unwrap();
            vol.fsync().unwrap();
        }

        // Reopen — lbamap should contain both blocks.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_unwritten_returns_zeros() {
        let base = keyed_temp_dir();
        let vol = Volume::open(&base, &base).unwrap();
        let data = vol.read(0, 4).unwrap();
        assert_eq!(data.len(), 4 * 4096);
        assert!(data.iter().all(|&b| b == 0));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_zeroes_reads_back_as_zeros() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write real data, then zero it out.
        vol.write(0, &vec![0xabu8; 4096]).unwrap();
        vol.write_zeroes(0, 4).unwrap();

        let result = vol.read(0, 4).unwrap();
        assert_eq!(result.len(), 4 * 4096);
        assert!(result.iter().all(|&b| b == 0));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_zeroes_no_data_in_segment() {
        // After write_zeroes + promote, the segment has a zero entry with no body bytes.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write_zeroes(0, 16).unwrap();
        vol.flush_wal().unwrap();

        let seg_path = segment::collect_segment_files(&base.join("pending"))
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one pending segment");

        let (_, entries, _) = segment::read_segment_index(&seg_path).unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.kind, segment::EntryKind::Zero);
        assert_eq!(e.stored_length, 0);
        assert_eq!(e.start_lba, 0);
        assert_eq!(e.lba_length, 16);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_after_zeroes_overrides() {
        // Data written after write_zeroes should be readable.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write_zeroes(0, 4).unwrap();
        let payload = vec![0x77u8; 4096];
        vol.write(0, &payload).unwrap();

        let result = vol.read(0, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_zeroes_survives_wal_recovery() {
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write_zeroes(5, 8).unwrap();
            vol.fsync().unwrap();
            // Drop without promoting — WAL remains.
        }

        // Reopen: WAL is replayed; zeroed range should read as zeros.
        let vol = Volume::open(&base, &base).unwrap();
        let result = vol.read(5, 8).unwrap();
        assert!(result.iter().all(|&b| b == 0));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_zeroes_masks_ancestor_data() {
        // An explicit zero in the child masks ancestor data at those LBAs.
        let by_id = temp_dir();
        let ancestor_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&ancestor_dir);

        // Write data in ancestor, promote, snapshot.
        {
            let mut vol = Volume::open(&ancestor_dir, &by_id).unwrap();
            vol.write(0, &vec![0xbbu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            vol.snapshot().unwrap();
        }

        // Fork and zero the LBA in the child.
        fork_volume(&child_dir, &ancestor_dir).unwrap();
        let mut child_vol = Volume::open(&child_dir, &by_id).unwrap();
        child_vol.write_zeroes(0, 1).unwrap();

        let result = child_vol.read(0, 1).unwrap();
        assert!(
            result.iter().all(|&b| b == 0),
            "zero extent should mask ancestor data"
        );

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn read_written_data_same_session() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload = vec![0x42u8; 4096];
        vol.write(5, &payload).unwrap();

        // Written block reads back correctly.
        let result = vol.read(5, 1).unwrap();
        assert_eq!(result, payload);

        // Adjacent unwritten blocks are zero.
        let before = vol.read(4, 1).unwrap();
        assert!(before.iter().all(|&b| b == 0));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_multi_block_extent() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write 4 blocks with distinct fill bytes so we can verify each block.
        let mut payload = Vec::with_capacity(4 * 4096);
        for fill in [0xAAu8, 0xBB, 0xCC, 0xDD] {
            payload.extend_from_slice(&[fill; 4096]);
        }
        vol.write(10, &payload).unwrap();

        let result = vol.read(10, 4).unwrap();
        assert_eq!(result, payload);

        // Reading a sub-range within the extent.
        let mid = vol.read(11, 2).unwrap();
        assert_eq!(mid, payload[4096..3 * 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_after_promote() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload = vec![0x55u8; 4096];
        vol.write(0, &payload).unwrap();
        vol.promote_for_test().unwrap();

        // After promotion, data lives in pending/<ulid>; reads must still work.
        let result = vol.read(0, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_after_reopen() {
        let base = keyed_temp_dir();

        let payload = vec![0x77u8; 4096];
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(3, &payload).unwrap();
            vol.fsync().unwrap();
        }

        // Reopen: WAL recovery must restore both the LBA map and extent index.
        let vol = Volume::open(&base, &base).unwrap();
        let result = vol.read(3, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    /// Regression: compressed WAL entries must be promoted with the correct
    /// SegmentFlags::COMPRESSED so reads after recovery+promote work.
    ///
    /// WalFlags::COMPRESSED=0x01; SegmentFlags::COMPRESSED=0x04.
    /// recover_wal must translate between them before calling new_data().
    #[test]
    fn compressed_entry_survives_recover_and_promote() {
        let base = keyed_temp_dir();

        // Write compressible data (zeros compress very well).
        let payload = vec![0u8; 4096];
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &payload).unwrap();
            vol.fsync().unwrap();
            // Drop without promoting — WAL contains the compressed entry.
        }

        // Reopen (recover_wal runs) then promote (writes segment).
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Reopen again and read — must not fail with "failed to fill whole buffer".
        let vol = Volume::open(&base, &base).unwrap();
        let result = vol.read(0, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn ulid_is_unique_and_sortable() {
        let u1 = Ulid::new().to_string();
        let u2 = Ulid::new().to_string();
        assert_eq!(u1.len(), 26);
        assert_ne!(u1, u2);
        // ULIDs generated in sequence should sort correctly (same millisecond
        // is not guaranteed, but two different values prove uniqueness).
    }

    #[test]
    fn recovery_after_promotion() {
        // Write enough to trigger a promotion, drop, reopen — the LBA map must
        // be rebuilt from both pending/ segments and the remaining WAL.
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            let block = vec![0u8; 1024 * 1024]; // 1 MiB = 256 LBAs
            for i in 0u64..33 {
                vol.write(i * 256, &block).unwrap();
            }
            vol.fsync().unwrap();
        }

        // All 33 extents should survive across the promotion boundary.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 33);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn promotion_after_wal_recovery() {
        // Write to the WAL, drop (simulating a crash), reopen (WAL recovery),
        // promote, then reopen again — verifies that pending_entries is correctly
        // rebuilt from the recovered WAL so the segment contains the pre-crash writes.
        let base = keyed_temp_dir();

        // Phase 1: write two blocks, fsync, drop.
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &vec![1u8; 4096]).unwrap();
            vol.write(1, &vec![2u8; 4096]).unwrap();
            vol.fsync().unwrap();
        }

        // Phase 2: recover and immediately promote.
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            assert_eq!(vol.lbamap_len(), 2); // confirm recovery
            vol.promote_for_test().unwrap();
        }

        // Phase 3: reopen — both blocks must now come from the pending/ segment.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);

        // Confirm the promoted segment landed correctly: one file in pending/.
        let pending_count = fs::read_dir(base.join("pending"))
            .unwrap()
            .filter(|e| e.is_ok())
            .count();
        assert_eq!(pending_count, 1, "expected one segment file in pending/");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn wal_deleted_when_pending_segment_exists() {
        // Simulate a crash between the segment rename and the WAL delete:
        // both wal/<ulid> and pending/<ulid> exist. On reopen, the WAL must
        // be silently discarded and data read from the committed segment.
        let base = keyed_temp_dir();

        // Phase 1: write two blocks and promote so a segment lands in pending/.
        let ulid;
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &vec![0xaau8; 4096]).unwrap();
            vol.write(1, &vec![0xbbu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            // Grab the segment ULID (there is exactly one file in pending/).
            let entry = fs::read_dir(base.join("pending"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let filename = entry.file_name();
            ulid = filename.to_string_lossy().into_owned();
        }

        // Simulate the crash: copy the segment back as a WAL file so both exist.
        fs::copy(
            base.join("pending").join(&ulid),
            base.join("wal").join(&ulid),
        )
        .unwrap();

        // Reopen — should delete the stale WAL and load cleanly from the segment.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);
        assert!(
            vol.read(0, 1).unwrap().iter().all(|&b| b == 0xaa),
            "LBA 0 should be 0xaa"
        );
        assert!(
            vol.read(1, 1).unwrap().iter().all(|&b| b == 0xbb),
            "LBA 1 should be 0xbb"
        );
        // The stale WAL file should be gone.
        assert!(
            !base.join("wal").join(&ulid).exists(),
            "stale WAL was not removed"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn recovery_replays_all_wals_promoting_non_latest() {
        // Multiple WAL files on disk — e.g. left by a crash between
        // `segment::write_and_commit` and the old-WAL unlink, or
        // produced by the upcoming off-actor worker — must be
        // collapsed back to a single active WAL before `Volume::open`
        // returns. Every non-latest WAL is promoted to a fresh pending
        // segment; the highest-ULID WAL stays active.
        let base = keyed_temp_dir();

        // Bootstrap to create the standard directory layout + keypair.
        // The bootstrap open leaves an empty WAL that we then strip so
        // we can build our own two-WAL state from scratch.
        {
            let _vol = Volume::open(&base, &base).unwrap();
        }
        let wal_dir = base.join("wal");
        for entry in fs::read_dir(&wal_dir).unwrap() {
            fs::remove_file(entry.unwrap().path()).unwrap();
        }

        // Two ULIDs with a strict ordering. Fixed strings keep the
        // test deterministic independently of the system clock.
        let low_ulid = Ulid::from_string("01AAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let high_ulid = Ulid::from_string("01BBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
        assert!(low_ulid < high_ulid);

        // Low WAL: one DATA record covering LBA 0.
        let payload_low = vec![0x11u8; 4096];
        let hash_low = blake3::hash(&payload_low);
        {
            let mut wl = writelog::WriteLog::create(&wal_dir.join(low_ulid.to_string())).unwrap();
            wl.append_data(0, 1, &hash_low, writelog::WalFlags::empty(), &payload_low)
                .unwrap();
            wl.fsync().unwrap();
        }

        // High WAL: one DATA record covering LBA 1.
        let payload_high = vec![0x22u8; 4096];
        let hash_high = blake3::hash(&payload_high);
        {
            let mut wl = writelog::WriteLog::create(&wal_dir.join(high_ulid.to_string())).unwrap();
            wl.append_data(1, 1, &hash_high, writelog::WalFlags::empty(), &payload_high)
                .unwrap();
            wl.fsync().unwrap();
        }

        // Reopen — recovery must promote `low_ulid` to a fresh segment
        // and keep `high_ulid` as the active WAL.
        let vol = Volume::open(&base, &base).unwrap();

        // Exactly one WAL remains: the high one.
        let wal_files: Vec<_> = fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name().into_string().unwrap()))
            .collect();
        assert_eq!(
            wal_files.len(),
            1,
            "expected one active WAL after recovery, got {wal_files:?}"
        );
        assert_eq!(wal_files[0], high_ulid.to_string());

        // Exactly one segment in pending/ — the recovery-promoted low
        // WAL, at a freshly-minted ULID strictly above the wal floor.
        let pending_files: Vec<_> = fs::read_dir(base.join("pending"))
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name().into_string().unwrap()))
            .filter(|n| !n.ends_with(".tmp"))
            .collect();
        assert_eq!(
            pending_files.len(),
            1,
            "expected one recovery-promoted segment in pending/, got {pending_files:?}"
        );
        let seg_ulid = Ulid::from_string(&pending_files[0]).unwrap();
        assert!(
            seg_ulid > high_ulid,
            "recovery-promoted segment ULID {seg_ulid} must sort above wal floor {high_ulid}"
        );

        // Both LBAs read back correctly. LBA 0 comes from the promoted
        // segment; LBA 1 from the active WAL's pending_entries.
        assert_eq!(vol.read(0, 1).unwrap(), payload_low);
        assert_eq!(vol.read(1, 1).unwrap(), payload_high);
        assert_eq!(vol.lbamap_len(), 2);

        fs::remove_dir_all(base).unwrap();
    }

    // --- durability guarantee tests ---
    //
    // These tests make the crash-recovery guarantees from docs/formats.md explicit
    // and executable. They simulate the intermediate filesystem states that can
    // arise from a machine crash at each step of the promotion commit sequence,
    // and verify that Volume::open() recovers correctly in each case.
    //
    // What these tests cannot cover: whether sync_data() / fsync_dir() actually
    // flush to physical media. That requires hardware fault injection (dm-flakey,
    // CrashMonkey, etc.) and is out of scope for a unit test suite.

    #[test]
    fn recovery_reads_data_after_promotion_and_reopen() {
        // Guarantee: after flush_wal() completes (WAL promoted to pending/),
        // a subsequent Volume::open() reads the correct data from the segment.
        // This covers the common path: crash after a guest fsync, before the
        // coordinator uploads the segment to S3.
        let base = keyed_temp_dir();

        let payload_a = vec![0xAAu8; 4096];
        let payload_b = vec![0xBBu8; 4096];
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &payload_a).unwrap();
            vol.write(1, &payload_b).unwrap();
            // promote_for_test flushes the WAL to pending/ and opens a fresh WAL.
            vol.promote_for_test().unwrap();
            // Drop without explicit shutdown — simulates a process crash after promotion.
        }

        // On reopen, data must come from the pending/ segment.
        // The fresh empty WAL (opened after promotion) contributes nothing.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), payload_a);
        assert_eq!(vol.read(1, 1).unwrap(), payload_b);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn recovery_removes_tmp_orphans() {
        // Guarantee: a .tmp file left in pending/ by a crashed segment write
        // (crash between write_segment and rename — the rename never committed)
        // is removed by Volume::open() and does not affect recovery.
        // The WAL is intact as a fallback and is replayed normally.
        let base = keyed_temp_dir();

        let payload = vec![0xCCu8; 4096];
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &payload).unwrap();
            vol.fsync().unwrap();
            // Drop with WAL intact — simulates crash before/during promotion.
        }

        // Simulate a crash mid-promotion: a .tmp file exists in pending/ but
        // no completed segment (the rename never happened).
        let orphan = base.join("pending").join("01AAAAAAAAAAAAAAAAAAAAAAAAA.tmp");
        fs::write(&orphan, b"incomplete segment bytes").unwrap();

        // Recovery must succeed, data must be correct, and the orphan removed.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        assert_eq!(vol.read(0, 1).unwrap(), payload);
        assert!(!orphan.exists(), ".tmp orphan should be cleaned up on open");

        fs::remove_dir_all(base).unwrap();
    }

    // --- compaction tests ---

    #[test]
    fn repack_noop_when_all_live() {
        // Write two blocks, promote, compact — nothing should be compacted
        // since all data is still referenced.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.write(1, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.repack(0.7).unwrap();
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.bytes_freed, 0);
        assert_eq!(stats.extents_removed, 0);

        // Data still readable.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x11u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_reclaims_overwritten_extent() {
        // Write block A, promote, overwrite block A with B, promote.
        // First segment now has a dead extent; compaction should reclaim it.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let original = vec![0x11u8; 4096];
        let replacement = vec![0x22u8; 4096];

        vol.write(0, &original).unwrap();
        vol.promote_for_test().unwrap();

        vol.write(0, &replacement).unwrap();
        vol.promote_for_test().unwrap();

        // Two segments: first is 100% dead, second is live.
        let stats = vol.repack(0.7).unwrap();
        assert_eq!(
            stats.segments_compacted, 1,
            "first segment should be compacted"
        );
        assert!(stats.bytes_freed > 0);
        assert_eq!(stats.extents_removed, 1);

        // Data still reads back correctly after compaction.
        assert_eq!(vol.read(0, 1).unwrap(), replacement);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_reads_back_correctly_after_reopen() {
        // Verify that the compacted segment is a valid segment that survives
        // a volume reopen (LBA map rebuild + extent index rebuild).
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &vec![0xAAu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            vol.write(0, &vec![0xBBu8; 4096]).unwrap(); // overwrite
            vol.promote_for_test().unwrap();
            vol.repack(0.7).unwrap();
        }

        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), vec![0xBBu8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_partial_segment() {
        // Segment has two extents; one is overwritten (dead), one is live.
        // Compaction should rewrite the segment keeping only the live extent.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap(); // will be overwritten
        vol.write(1, &vec![0x22u8; 4096]).unwrap(); // stays live
        vol.promote_for_test().unwrap();

        vol.write(0, &vec![0x33u8; 4096]).unwrap(); // overwrites LBA 0
        vol.promote_for_test().unwrap();

        // First segment is 50% dead — above default threshold of 30% dead (0.7 live).
        let stats = vol.repack(0.7).unwrap();
        assert_eq!(stats.segments_compacted, 1);
        assert!(stats.bytes_freed > 0);

        // Both LBAs read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x33u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_respects_min_live_ratio() {
        // With a strict ratio (1.0), any dead byte triggers compaction.
        // With a lenient ratio (0.0), nothing is ever compacted.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap(); // LBA 0 now dead in seg 1
        vol.promote_for_test().unwrap();

        // Lenient threshold: first segment is 100% dead but ratio=0.0 → nothing compacted.
        let stats = vol.repack(0.0).unwrap();
        assert_eq!(stats.segments_compacted, 0);

        // Strict threshold: compact anything with any dead bytes.
        let stats = vol.repack(1.0).unwrap();
        assert_eq!(stats.segments_compacted, 1);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_does_not_touch_pre_snapshot_segments() {
        // Write and overwrite a block, then snapshot. The dead segment is
        // pre-snapshot and must not be compacted — it is frozen by the floor.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Snapshot freezes both segments (floor = latest segment ULID).
        vol.snapshot().unwrap();

        // Even with a strict threshold the pre-snapshot segments must be skipped.
        let stats = vol.repack(1.0).unwrap();
        assert_eq!(
            stats.segments_compacted, 0,
            "pre-snapshot segments must not be compacted"
        );

        // Data still readable.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_only_touches_post_snapshot_segments() {
        // Pre-snapshot dead segment: frozen. Post-snapshot dead segment: compactable.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Pre-snapshot: write and overwrite LBA 0.
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        vol.snapshot().unwrap();

        // Post-snapshot: write and overwrite LBA 1.
        vol.write(1, &vec![0x33u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &vec![0x44u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // One pre-snapshot dead segment (frozen) + one post-snapshot dead segment (eligible).
        let stats = vol.repack(1.0).unwrap();
        assert_eq!(
            stats.segments_compacted, 1,
            "exactly the post-snapshot dead segment should be compacted"
        );

        // Both LBAs read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x44u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_does_not_touch_uploaded_segments() {
        // Simulate an uploaded segment (promoted to cache/ by the coordinator).
        // repack() must not touch it even if its extents are dead.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Simulate coordinator upload + promote IPC: pending → index/ + cache/.
        simulate_upload(&mut vol);

        // Overwrite LBA 0 — the uploaded segment's extent is now dead.
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Strict threshold: repack anything with dead bytes.
        let stats = vol.repack(1.0).unwrap();
        assert_eq!(
            stats.segments_compacted, 0,
            "repack must not touch uploaded (cache/) segments"
        );

        // Data still reads correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    // --- sweep_pending tests ---

    #[test]
    fn sweep_pending_noop_when_all_live() {
        // Single pending segment with no dead extents: sweep_pending must not
        // rewrite it. Rewriting a single all-live small segment is a no-op that
        // only wastes IO — merging only makes sense when >=2 segments combine or
        // dead space is reclaimed.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.write(1, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.new_segments, 0);
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x11u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_removes_dead_extents() {
        // Write LBA 0, promote, overwrite LBA 0, promote.
        // sweep_pending should remove the dead extent from the first segment.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        assert!(stats.segments_compacted >= 1);
        assert!(stats.bytes_freed > 0);
        assert_eq!(stats.extents_removed, 1);

        // Current value of LBA 0 must be the replacement.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_only_scans_pending_not_uploaded() {
        // Upload a segment (simulate coordinator promoting pending → cache/).
        // sweep_pending must not touch uploaded segments.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Simulate coordinator upload + promote IPC: pending → index/ + cache/.
        simulate_upload(&mut vol);

        // Now overwrite LBA 0 and promote — creates a new pending segment.
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        // The old dead extent is in cache/ — sweep_pending doesn't touch it.
        assert_eq!(stats.extents_removed, 0);
        // The new pending segment is small and all-live: single segment, no
        // dead extents, so sweep_pending correctly leaves it alone.
        assert_eq!(stats.segments_compacted, 0);

        // Data still reads correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_respects_snapshot_floor() {
        // Segments at or below the snapshot ULID must not be touched.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write and promote before snapshot.
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        vol.snapshot().unwrap();

        // The two pre-snapshot segments are now frozen.
        let stats = vol.sweep_pending().unwrap();
        assert_eq!(
            stats.segments_compacted, 0,
            "pre-snapshot segments must not be touched"
        );

        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_multi_block_inplace_overwrite_same_wal() {
        // Regression: two multi-block DATA writes at the same LBA range in the
        // same WAL flush. Both land as DATA entries (different hashes) in one
        // pending segment. sweep_pending then partitions entries into live/dead,
        // rewrites the segment, and updates the extent index — the surviving
        // live entry must read back correctly from the rewritten segment.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // High-entropy so neither payload is inlined and both stay in the
        // body section. Eight 4 KiB blocks each.
        let payload_a: Vec<u8> = (0..8 * 4096usize).map(|i| (i * 7 + 13) as u8).collect();
        let payload_b: Vec<u8> = (0..8 * 4096usize).map(|i| (i * 11 + 3) as u8).collect();
        assert_ne!(payload_a, payload_b);

        vol.write(24, &payload_a).unwrap();
        vol.write(24, &payload_b).unwrap();
        vol.flush_wal().unwrap();
        assert_eq!(
            vol.read(24, 8).unwrap(),
            payload_b,
            "pre-sweep read must return the second write"
        );

        vol.sweep_pending().unwrap();
        assert_eq!(
            vol.read(24, 8).unwrap(),
            payload_b,
            "post-sweep read must still return the second write"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_merges_multiple_small_segments() {
        // Three separate promotes → three small pending segments.
        // sweep_pending should merge them into one.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0xaau8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &vec![0xbbu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(2, &vec![0xccu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        assert_eq!(stats.segments_compacted, 3);
        assert_eq!(stats.new_segments, 1);

        // All three LBAs must still read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0xaau8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0xbbu8; 4096]);
        assert_eq!(vol.read(2, 1).unwrap(), vec![0xccu8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    // --- compression helper unit tests ---

    /// Build a 4096-byte block where every byte is distinct (entropy = 8 bits/byte).
    /// The LCG multiplier 109 (0x6D) is odd so it is coprime to 256, giving a
    /// bijection on [0, 255] — each value appears exactly 16 times in 4096 bytes.
    fn high_entropy_block(seed: u8) -> Vec<u8> {
        (0..4096u16)
            .map(|i| (i as u8).wrapping_mul(0x6D).wrapping_add(seed))
            .collect()
    }

    #[test]
    fn shannon_entropy_all_same_byte() {
        assert_eq!(shannon_entropy(&vec![0x42u8; 4096]), 0.0);
    }

    #[test]
    fn shannon_entropy_uniform_is_8_bits() {
        // 256 distinct values each appearing 16 times → exactly 8 bits/byte.
        let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let e = shannon_entropy(&data);
        assert!((e - 8.0).abs() < 0.01, "expected ~8.0, got {e}");
    }

    #[test]
    fn maybe_compress_compresses_low_entropy() {
        // All-zeros: entropy = 0, compresses to almost nothing.
        let data = vec![0u8; 4096];
        let compressed = maybe_compress(&data).expect("expected compression to succeed");
        // Must achieve at least 1.5× ratio.
        assert!(
            compressed.len() * MIN_COMPRESSION_RATIO_NUM / MIN_COMPRESSION_RATIO_DEN < data.len()
        );
    }

    #[test]
    fn maybe_compress_skips_high_entropy() {
        let data = high_entropy_block(0);
        assert!(shannon_entropy(&data) > ENTROPY_THRESHOLD);
        assert!(maybe_compress(&data).is_none());
    }

    // --- volume read/write tests for compressed and uncompressed paths ---

    #[test]
    fn read_incompressible_data() {
        // High-entropy data must not be compressed, and must read back correctly.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload = high_entropy_block(0x5A);
        assert!(
            shannon_entropy(&payload) > ENTROPY_THRESHOLD,
            "test data must be incompressible"
        );

        vol.write(0, &payload).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn compressed_and_uncompressed_extents_coexist() {
        // Write one compressible and one incompressible extent; both must read back correctly.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let compressible = vec![0xCCu8; 4096];
        let incompressible = high_entropy_block(0xA3);

        vol.write(0, &compressible).unwrap();
        vol.write(1, &incompressible).unwrap();

        assert_eq!(vol.read(0, 1).unwrap(), compressible);
        assert_eq!(vol.read(1, 1).unwrap(), incompressible);

        fs::remove_dir_all(base).unwrap();
    }

    // --- write-path dedup tests ---

    #[test]
    fn dedup_write_same_data_same_lba() {
        // Writing identical data to the same LBA twice: second write is a dedup hit.
        // The LBA map must have exactly one entry, reads must return the correct data.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0x42u8; 4096];
        vol.write(0, &data).unwrap();
        vol.write(0, &data).unwrap();

        assert_eq!(vol.lbamap_len(), 1);
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn dedup_write_same_data_different_lba() {
        // Identical data written to two different LBAs: second write is a dedup hit.
        // Both LBA entries exist in the map; reads return the correct data from both.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0x77u8; 4096];
        vol.write(0, &data).unwrap();
        vol.write(5, &data).unwrap();

        assert_eq!(vol.lbamap_len(), 2);
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(5, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn dedup_ref_survives_promote_and_reopen() {
        // Write data, promote so it lands in pending/, then write the same data
        // to a new LBA (dedup REF in WAL). Reopen and verify both LBAs read back.
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            let data = vec![0xABu8; 4096];
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            // Second write: same data, different LBA → dedup hit, REF record in WAL.
            vol.write(1, &data).unwrap();
            vol.fsync().unwrap();
        }

        let vol = Volume::open(&base, &base).unwrap();
        let data = vec![0xABu8; 4096];
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn dedup_ref_in_segment_survives_reopen() {
        // Write data, promote, write same data (REF in WAL), promote again so
        // the REF lands in a segment. Reopen and verify reads still work.
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            let data = vec![0xCDu8; 4096];
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            vol.write(1, &data).unwrap(); // REF
            vol.promote_for_test().unwrap(); // REF lands in segment
        }

        let vol = Volume::open(&base, &base).unwrap();
        let data = vec![0xCDu8; 4096];
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    // --- dedup-ref redact / promote regression tests ---

    /// Helper: collect all pending segment ULIDs (excluding sidecars and tmps).
    fn pending_ulids(base: &Path) -> Vec<ulid::Ulid> {
        let pending_dir = base.join("pending");
        let mut ulids: Vec<ulid::Ulid> = Vec::new();
        for entry in fs::read_dir(&pending_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if name.contains('.') {
                continue;
            }
            ulids.push(ulid::Ulid::from_string(&name).unwrap());
        }
        ulids.sort();
        ulids
    }

    #[test]
    fn redact_segment_punches_hash_dead_data_in_place() {
        // An entry whose LBA has been overwritten and whose hash is no longer
        // referenced anywhere must have its body region hole-punched in place
        // on pending/<ulid> so deleted data never leaves the host. No sidecar
        // is produced; the original file is modified directly.
        // High-entropy data avoids compression below the inline threshold,
        // guaranteeing the entry lands in the body section (not inline).
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let secret: Vec<u8> = (0..8192).map(|i| (i * 17 + 31) as u8).collect();
        vol.write(0, &secret).unwrap();
        vol.promote_for_test().unwrap();

        let ulids = pending_ulids(&base);
        assert_eq!(ulids.len(), 1);
        let seg_ulid = ulids[0];

        // Overwrite LBA 0-1 with different content. Hash of `secret` is no
        // longer referenced anywhere → fully dead. Do not promote so the
        // overwrite stays in the WAL and the pending segment still holds the
        // now-dead entry.
        let replacement: Vec<u8> = (0..8192).map(|i| (i * 23 + 41) as u8).collect();
        vol.write(0, &replacement).unwrap();

        vol.redact_segment(seg_ulid).unwrap();

        // No sidecar — the original pending file is modified in place.
        let seg_path = base.join("pending").join(seg_ulid.to_string());
        assert!(seg_path.exists(), "pending/<ulid> must still exist");
        assert!(
            !base
                .join("pending")
                .join(format!("{}.materialized", seg_ulid))
                .exists(),
            "no .materialized sidecar should be produced"
        );

        use std::io::{Read, Seek, SeekFrom};
        let (bss, entries, _) =
            segment::read_and_verify_segment_index(&seg_path, &vol.verifying_key).unwrap();
        let dead_entry = entries
            .iter()
            .find(|e| e.kind == EntryKind::Data && e.start_lba == 0)
            .expect("should have a Data entry at LBA 0");
        assert!(dead_entry.stored_length > 0);

        // The dead region must read as zeros. On Linux it is a true sparse
        // hole; on macOS it is a zero-write fallback — both read as zeros.
        let mut f = fs::File::open(&seg_path).unwrap();
        let mut body = vec![0xFFu8; dead_entry.stored_length as usize];
        f.seek(SeekFrom::Start(bss + dead_entry.stored_offset))
            .unwrap();
        f.read_exact(&mut body).unwrap();
        assert!(
            body.iter().all(|b| *b == 0),
            "dead entry body must be punched to zeros in place"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn promote_segment_after_redact_produces_correct_idx_and_present() {
        // After redact + promote, the .idx contains DedupRef entries and the
        // .present bitset marks only Data entries as present.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0xDDu8; 4096];
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let after_first = pending_ulids(&base);
        let s1_ulid = after_first[0];

        vol.write(1, &data).unwrap(); // dedup hit
        vol.promote_for_test().unwrap();

        let after_second = pending_ulids(&base);
        let s2_ulid = *after_second.iter().find(|u| **u != s1_ulid).unwrap();

        // Redact and promote S2 (simulating the coordinator drain path).
        vol.redact_segment(s2_ulid).unwrap();
        vol.promote_segment(s2_ulid).unwrap();

        // The .idx should exist and contain DedupRef entries.
        let idx_path = base.join("index").join(format!("{}.idx", s2_ulid));
        assert!(
            idx_path.exists(),
            "index/<ulid>.idx must exist after promote"
        );

        let (_, idx_entries, _) =
            segment::read_and_verify_segment_index(&idx_path, &vol.verifying_key).unwrap();
        assert!(
            idx_entries.iter().any(|e| e.kind == EntryKind::DedupRef),
            "idx should contain DedupRef entries"
        );

        // The .present bitset should mark DedupRef entries as not-present.
        let present_path = base.join("cache").join(format!("{}.present", s2_ulid));
        assert!(present_path.exists(), ".present must exist after promote");
        for (i, entry) in idx_entries.iter().enumerate() {
            let present = segment::check_present_bit(&present_path, i as u32).unwrap_or(false);
            if entry.kind == EntryKind::Data {
                assert!(present, "Data entry {i} should be marked present");
            } else if entry.kind == EntryKind::DedupRef {
                assert!(!present, "DedupRef entry {i} should NOT be marked present");
            }
        }

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn reads_work_after_redact_and_promote() {
        // After redact + promote, reads must still work correctly.
        // DedupRef reads go through the extent index to the canonical segment.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0xBBu8; 4096];
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let after_first = pending_ulids(&base);
        let s1_ulid = after_first[0];

        vol.write(1, &data).unwrap(); // dedup hit → DedupRef
        vol.promote_for_test().unwrap();

        let after_second = pending_ulids(&base);
        let s2_ulid = *after_second.iter().find(|u| **u != s1_ulid).unwrap();

        vol.redact_segment(s2_ulid).unwrap();
        vol.promote_segment(s2_ulid).unwrap();

        assert_eq!(vol.read(0, 1).unwrap(), data, "LBA 0 after redact+promote");
        assert_eq!(vol.read(1, 1).unwrap(), data, "LBA 1 after redact+promote");

        // Also verify after reopen (extent index rebuilt from .idx files).
        drop(vol);
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data, "LBA 0 after reopen");
        assert_eq!(vol.read(1, 1).unwrap(), data, "LBA 1 after reopen");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn redact_segment_idempotent() {
        // A second redact call is a no-op because the first call already
        // punched all hash-dead DATA regions.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let secret: Vec<u8> = (0..8192).map(|i| (i * 17 + 31) as u8).collect();
        vol.write(0, &secret).unwrap();
        vol.promote_for_test().unwrap();

        let ulids = pending_ulids(&base);
        let seg_ulid = ulids[0];

        let replacement: Vec<u8> = (0..8192).map(|i| (i * 23 + 41) as u8).collect();
        vol.write(0, &replacement).unwrap();

        // First redact punches the dead region; second is a no-op.
        vol.redact_segment(seg_ulid).unwrap();
        vol.redact_segment(seg_ulid).unwrap();

        // Segment file still present, no sidecar produced.
        assert!(base.join("pending").join(seg_ulid.to_string()).exists());
        assert!(
            !base
                .join("pending")
                .join(format!("{}.materialized", seg_ulid))
                .exists()
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn redact_segment_no_op_when_all_live() {
        // A segment with no hash-dead DATA entries is untouched by redact:
        // the file is unchanged, no sidecar is produced.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0x77u8; 4096];
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let ulids = pending_ulids(&base);
        let ulid = ulids[0];
        let seg_path = base.join("pending").join(ulid.to_string());
        let before = fs::read(&seg_path).unwrap();

        vol.redact_segment(ulid).unwrap();

        let after = fs::read(&seg_path).unwrap();
        assert_eq!(
            before, after,
            "redact with no dead DATA must not modify file"
        );
        assert!(
            !base
                .join("pending")
                .join(format!("{}.materialized", ulid))
                .exists(),
            "no sidecar should be produced"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn redact_preserves_body_for_lba_dead_but_hash_alive_entry() {
        // Regression test: if a Data entry's LBA is overwritten but the same
        // hash is alive at another LBA, redact must NOT punch the body.
        // GC's collect_stats keeps such entries via extent+hash liveness, so
        // punching the body would cause GC to copy zeros into its output.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Use high-entropy data that won't compress below INLINE_THRESHOLD.
        let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
        // LBA 0-1 → DATA(hash=H). Also dedup-indexed.
        vol.write(0, &data).unwrap();
        // LBA 2-3 → dedup hit → DedupRef(hash=H). Hash H is now alive at LBAs 0 and 2.
        vol.write(2, &data).unwrap();
        vol.promote_for_test().unwrap();

        let ulids = pending_ulids(&base);
        assert_eq!(ulids.len(), 1);
        let seg_ulid = ulids[0];

        // Overwrite LBA 0-1 with different data. The DATA entry at LBA 0 is
        // now LBA-dead, but hash H is still alive at LBA 2.
        let other: Vec<u8> = (0..8192).map(|i| (i * 11 + 3) as u8).collect();
        vol.write(0, &other).unwrap();

        vol.redact_segment(seg_ulid).unwrap();

        // Verify the DATA entry at LBA 0 still has real body bytes (not zeros)
        // in the in-place pending file.
        use std::io::{Read as _, Seek as _, SeekFrom};
        let seg_path = base.join("pending").join(seg_ulid.to_string());
        let (bss, entries, _) =
            segment::read_and_verify_segment_index(&seg_path, &vol.verifying_key).unwrap();
        let data_entry = entries
            .iter()
            .find(|e| e.kind == EntryKind::Data && e.start_lba == 0)
            .expect("should have a Data entry at LBA 0");
        assert!(data_entry.stored_length > 0);

        let mut f = fs::File::open(&seg_path).unwrap();
        let mut body = vec![0u8; data_entry.stored_length as usize];
        f.seek(SeekFrom::Start(bss + data_entry.stored_offset))
            .unwrap();
        f.read_exact(&mut body).unwrap();
        assert!(
            body.iter().any(|&b| b != 0),
            "redact must NOT punch body of LBA-dead but hash-alive Data entry"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn redact_punches_body_when_hash_fully_dead() {
        // When both the LBA and the hash are dead (no LBA references the hash),
        // redact must punch the body to prevent uploading deleted data.
        // Uses high-entropy data that won't compress below INLINE_THRESHOLD.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let ulids = pending_ulids(&base);
        assert_eq!(ulids.len(), 1);
        let seg_ulid = ulids[0];

        // Overwrite LBA 0-1 with different data. Hash H is no longer alive
        // at any LBA.
        let other: Vec<u8> = (0..8192).map(|i| (i * 11 + 3) as u8).collect();
        vol.write(0, &other).unwrap();

        vol.redact_segment(seg_ulid).unwrap();

        use std::io::{Read as _, Seek as _, SeekFrom};
        let seg_path = base.join("pending").join(seg_ulid.to_string());
        let (bss, entries, _) =
            segment::read_and_verify_segment_index(&seg_path, &vol.verifying_key).unwrap();
        let data_entry = entries
            .iter()
            .find(|e| e.kind == EntryKind::Data && e.start_lba == 0)
            .expect("should have a Data entry at LBA 0");
        assert!(data_entry.stored_length > 0);

        let mut f = fs::File::open(&seg_path).unwrap();
        let mut body = vec![0u8; data_entry.stored_length as usize];
        f.seek(SeekFrom::Start(bss + data_entry.stored_offset))
            .unwrap();
        f.read_exact(&mut body).unwrap();
        assert!(
            body.iter().all(|&b| b == 0),
            "redact must punch body of fully-dead entry (both LBA and hash dead)"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn redact_invalidates_extent_index_for_punched_hash() {
        // Regression: after redact punches a hash-dead DATA body, a later write
        // whose content hashes to the same value must not use the dedup
        // shortcut — the canonical body bytes are gone. Before the fix, the
        // surviving extent-index entry caused `write_commit` to emit a thin
        // DedupRef pointing at zero-punched bytes, so subsequent reads of the
        // new LBA returned zeros.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // High-entropy payloads so they stay in the body section (no inline).
        let payload_a: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
        let payload_b: Vec<u8> = (0..8192).map(|i| (i * 11 + 3) as u8).collect();

        // Seed LBA 28 with payload_A, flush so it lives in pending/.
        vol.write(28, &payload_a).unwrap();
        vol.promote_for_test().unwrap();

        let ulids = pending_ulids(&base);
        assert_eq!(ulids.len(), 1);
        let seg_ulid = ulids[0];

        // Overwrite LBA 28 with payload_B. Hash of payload_A is now LBA-dead
        // and no other LBA references it — hash-fully-dead.
        vol.write(28, &payload_b).unwrap();

        // Drain: redact (punches payload_A body bytes in place) then promote
        // to index/ + cache/. This mirrors the coordinator upload flow.
        vol.redact_segment(seg_ulid).unwrap();
        vol.promote_segment(seg_ulid).unwrap();

        // A fresh write with content matching payload_A. Without the fix, the
        // surviving extent-index entry for H_A makes `write_commit` emit a
        // DedupRef pointing at the (now zero) location in cache/<seg>.body.
        vol.write(100, &payload_a).unwrap();

        assert_eq!(
            vol.read(100, 2).unwrap(),
            payload_a,
            "new write of redacted content must read back correctly"
        );
        // Existing reads unaffected.
        assert_eq!(vol.read(28, 2).unwrap(), payload_b, "LBA 28 (overwrite)");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn wal_recovery_with_thin_ref() {
        // Write data to LBA 0, promote to pending, then write same data to
        // LBA 1 (dedup hit → thin ref in WAL). Do NOT flush — leave the thin
        // ref in the WAL. Drop (crash), reopen, verify both LBAs read back.
        let base = keyed_temp_dir();
        let data = vec![0x99u8; 4096];

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            // Second write: same data, different LBA → dedup hit → REF in WAL.
            vol.write(1, &data).unwrap();
            vol.fsync().unwrap();
            // Drop without promote — thin ref stays in WAL only.
        }

        // Reopen triggers WAL recovery.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(
            vol.read(0, 1).unwrap(),
            data,
            "LBA 0 must survive crash with thin ref in WAL"
        );
        assert_eq!(
            vol.read(1, 1).unwrap(),
            data,
            "LBA 1 (thin ref) must survive crash with thin ref in WAL"
        );

        fs::remove_dir_all(base).unwrap();
    }

    /// Proptest regression: DedupWrite → Flush → DedupWrite (overwrite) →
    /// Repack → DrainWithRedact.
    ///
    /// Repack finds all entries in the first segment dead (overwritten by the
    /// second DedupWrite) and removes the hash from the extent index. Before
    /// the fix, repack left the segment file behind; the subsequent drain
    /// then tried to process it, hit a DedupRef whose canonical hash was
    /// gone, and panicked. The fix: repack deletes the segment file when all
    /// entries are dead.
    #[test]
    fn repack_deletes_fully_dead_segment_before_drain() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Pre-snapshot segments (frozen by snapshot, skipped by repack).
        let data_a = vec![17u8; 4096];
        vol.write(2, &data_a).unwrap();
        vol.flush_wal().unwrap();
        for ulid in pending_ulids(&base) {
            vol.redact_segment(ulid).unwrap();
            vol.promote_segment(ulid).unwrap();
        }

        let data_b = vec![34u8; 4096];
        vol.write(3, &data_b).unwrap();
        vol.flush_wal().unwrap();
        for ulid in pending_ulids(&base) {
            vol.redact_segment(ulid).unwrap();
            vol.promote_segment(ulid).unwrap();
        }

        vol.snapshot().unwrap();

        // DedupWrite seed=0: LBA 0 (Data) + LBA 6 (DedupRef), same hash.
        let dedup_data_0 = vec![0u8; 4096];
        vol.write(0, &dedup_data_0).unwrap();
        vol.write(6, &dedup_data_0).unwrap();
        vol.flush_wal().unwrap();

        // DedupWrite seed=1: overwrite both LBAs with new data.
        let dedup_data_1 = vec![1u8; 4096];
        vol.write(0, &dedup_data_1).unwrap();
        vol.write(6, &dedup_data_1).unwrap();

        // Repack: the post-snapshot segment (seed=0) is now fully dead.
        // min_live_ratio=0.01 so the segment (0% live) is eligible.
        vol.repack(0.01).unwrap();

        // The fully-dead segment must have been deleted.
        let ulids = pending_ulids(&base);
        assert!(
            ulids.is_empty(),
            "repack should delete fully-dead segment, but found: {ulids:?}"
        );

        // DrainWithRedact: flush the WAL (seed=1), redact, promote.
        vol.flush_wal().unwrap();
        for ulid in pending_ulids(&base) {
            vol.redact_segment(ulid).unwrap();
            vol.promote_segment(ulid).unwrap();
        }

        // Verify reads.
        assert_eq!(vol.read(0, 1).unwrap(), dedup_data_1, "LBA 0");
        assert_eq!(vol.read(6, 1).unwrap(), dedup_data_1, "LBA 6");
        assert_eq!(vol.read(2, 1).unwrap(), data_a, "LBA 2 (pre-snapshot)");
        assert_eq!(vol.read(3, 1).unwrap(), data_b, "LBA 3 (pre-snapshot)");

        fs::remove_dir_all(base).unwrap();
    }

    /// Known failure: proptest minimal reproducer for dedup canonical overwrite
    /// data loss. When PopulateFetched overwrites the extent index entry for a
    /// hash that a DedupRef depends on, then DrainLocal removes pending/, then
    /// GC runs, the thin ref's canonical body is lost. After crash, LBA 4
    /// reads zeros instead of the expected data.
    ///
    /// Un-ignore when the fix lands.
    #[test]
    #[ignore]
    fn proptest_minimal_dedup_overwrite_data_loss() {
        let base = keyed_temp_dir();
        let fork_dir = base.clone();
        let mut vol = Volume::open(&base, &base).unwrap();

        // DedupWrite: write [1u8; 4096] to LBA 0 and LBA 4 (dedup hit on LBA 4).
        let data = [1u8; 4096];
        vol.write(0, &data).unwrap();
        vol.write(4, &data).unwrap();

        // Flush — promotes WAL to pending/.
        vol.flush_wal().unwrap();

        // PopulateFetched: write different data to cache for LBA 0,
        // overwriting the extent index entry for the original hash.
        let (pop_ulid, _) = vol.gc_checkpoint().unwrap();
        {
            // Use the common helper pattern from tests/common/mod.rs.
            let index_dir = fork_dir.join("index");
            let cache_dir = fork_dir.join("cache");
            let _ = fs::create_dir_all(&index_dir);
            let _ = fs::create_dir_all(&cache_dir);

            let seed = 128u8;
            let pop_data = vec![seed; 4096];
            let pop_hash = blake3::hash(&pop_data);
            let mut entries = vec![segment::SegmentEntry::new_data(
                pop_hash,
                0,
                1,
                segment::SegmentFlags::empty(),
                pop_data,
            )];

            let signer =
                crate::signing::load_signer(&fork_dir, crate::signing::VOLUME_KEY_FILE).unwrap();
            let tmp = cache_dir.join(format!("{pop_ulid}.tmp"));
            let bss = segment::write_segment(&tmp, &mut entries, signer.as_ref()).unwrap();
            let bytes = fs::read(&tmp).unwrap();
            fs::remove_file(&tmp).unwrap();

            let s = pop_ulid.to_string();
            fs::write(index_dir.join(format!("{s}.idx")), &bytes[..bss as usize]).unwrap();
            fs::write(cache_dir.join(format!("{s}.body")), &bytes[bss as usize..]).unwrap();
            segment::set_present_bit(&cache_dir.join(format!("{s}.present")), 0, 1).unwrap();
        }

        // DrainLocal: promote all pending segments to index/ + cache/.
        {
            let pending = fork_dir.join("pending");
            let index_dir = fork_dir.join("index");
            let cache_dir = fork_dir.join("cache");
            let _ = fs::create_dir_all(&index_dir);
            let _ = fs::create_dir_all(&cache_dir);
            if let Ok(entries) = fs::read_dir(&pending) {
                for entry in entries.flatten() {
                    let name = entry.file_name().into_string().unwrap();
                    if name.contains('.') {
                        continue;
                    }
                    let file_data = fs::read(entry.path()).unwrap();
                    if file_data.len() < 96 {
                        continue;
                    }
                    let entry_count = u32::from_le_bytes([
                        file_data[8],
                        file_data[9],
                        file_data[10],
                        file_data[11],
                    ]);
                    let index_length = u32::from_le_bytes([
                        file_data[12],
                        file_data[13],
                        file_data[14],
                        file_data[15],
                    ]);
                    let inline_length = u32::from_le_bytes([
                        file_data[16],
                        file_data[17],
                        file_data[18],
                        file_data[19],
                    ]);
                    let bss = 96 + index_length as usize + inline_length as usize;
                    if file_data.len() < bss {
                        continue;
                    }
                    let _ = fs::write(index_dir.join(format!("{name}.idx")), &file_data[..bss]);
                    let _ = fs::write(cache_dir.join(format!("{name}.body")), &file_data[bss..]);
                    let bitset_len = (entry_count as usize).div_ceil(8);
                    let _ = fs::write(
                        cache_dir.join(format!("{name}.present")),
                        vec![0xFFu8; bitset_len],
                    );
                    let _ = fs::remove_file(entry.path());
                }
            }
        }

        // CoordGcLocal: run GC.
        {
            let (gc_ulid, _) = vol.gc_checkpoint().unwrap();
            vol.flush_wal().unwrap();
            // Need at least 2 segments for GC; use all available.
            let idx_files = segment::collect_idx_files(&fork_dir.join("index")).unwrap();
            if idx_files.len() >= 2 {
                let to_delete = {
                    use crate::{extentindex, lbamap};
                    let rebuild_chain = vec![(fork_dir.clone(), None)];
                    let lba_map = lbamap::rebuild_segments(&rebuild_chain).unwrap();
                    let _live_hashes = lba_map.lba_referenced_hashes();
                    let extent_index = extentindex::rebuild(&rebuild_chain).unwrap();

                    let vk = crate::signing::load_verifying_key(
                        &fork_dir,
                        crate::signing::VOLUME_PUB_FILE,
                    )
                    .unwrap();
                    let (ephemeral_signer, _) = crate::signing::generate_ephemeral_signer();

                    let gc_dir = fork_dir.join("gc");
                    let _ = fs::create_dir_all(&gc_dir);

                    // Build candidates from all .idx files
                    let mut candidates: Vec<(Ulid, PathBuf)> = idx_files
                        .iter()
                        .filter_map(|p| {
                            let stem = p.file_stem()?.to_str()?;
                            let ulid = Ulid::from_string(stem).ok()?;
                            Some((ulid, p.clone()))
                        })
                        .collect();
                    candidates.sort_by_key(|(u, _)| *u);

                    // Read and compact
                    let mut all_entries: Vec<segment::SegmentEntry> = Vec::new();
                    for (ulid, path) in &candidates {
                        let Ok((_bss, mut seg_entries, _)) =
                            segment::read_and_verify_segment_index(path, &vk)
                        else {
                            continue;
                        };
                        let body_path = fork_dir.join("cache").join(format!("{}.body", ulid));
                        if segment::read_extent_bodies(
                            &body_path,
                            0,
                            &mut seg_entries,
                            segment::EntryKind::LOCAL_BODY,
                            &[],
                        )
                        .is_err()
                        {
                            continue;
                        }
                        for e in seg_entries {
                            if e.kind == EntryKind::DedupRef {
                                continue;
                            }
                            let lba_live = lba_map.hash_at(e.start_lba) == Some(e.hash);
                            let extent_live = extent_index
                                .lookup(&e.hash)
                                .is_some_and(|loc| loc.segment_id == *ulid);
                            if lba_live || extent_live {
                                all_entries.push(e);
                            }
                        }
                    }

                    if !all_entries.is_empty() {
                        let mut inputs: Vec<Ulid> = candidates.iter().map(|(u, _)| *u).collect();
                        inputs.sort();
                        let tmp = gc_dir.join(format!("{gc_ulid}.staged.tmp"));
                        let staged = gc_dir.join(format!("{gc_ulid}.staged"));
                        segment::write_gc_segment(
                            &tmp,
                            &mut all_entries,
                            &inputs,
                            ephemeral_signer.as_ref(),
                        )
                        .unwrap();
                        fs::rename(&tmp, &staged).unwrap();
                    }

                    candidates
                        .iter()
                        .map(|(_, p)| p.clone())
                        .collect::<Vec<_>>()
                };
                let applied = vol.apply_gc_handoffs().unwrap_or(0);
                if applied > 0 {
                    for path in &to_delete {
                        let _ = fs::remove_file(path);
                    }
                }
            }
        }

        // Crash: drop and reopen.
        drop(vol);
        let vol = Volume::open(&base, &base).unwrap();

        // Assert LBA 4 reads [1u8; 4096] — the dedup ref target.
        // This is the assertion that currently fails due to the known bug.
        assert_eq!(
            vol.read(4, 1).unwrap(),
            vec![1u8; 4096],
            "LBA 4 (dedup ref) must read back original data after GC + crash"
        );

        fs::remove_dir_all(base).unwrap();
    }

    // --- walk_ancestors tests ---

    #[test]
    fn walk_ancestors_root_returns_empty() {
        let by_id = temp_dir();
        let vol_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        // No origin file → root volume; ancestors are empty.
        assert!(walk_ancestors(&vol_dir, &by_id).unwrap().is_empty());
    }

    #[test]
    fn walk_ancestors_rejects_invalid_parent_entries() {
        let by_id = temp_dir();
        let fork_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        let bad_parents = [
            // not a ULID parent (old "base/" prefix)
            "base/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // path traversal attempt
            "../01AAAAAAAAAAAAAAAAAAAAAAAA/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // parent component is not a valid ULID
            "not-a-ulid/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // missing '/' separator entirely
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // branch ULID missing
            "01AAAAAAAAAAAAAAAAAAAAAAAA/",
        ];
        for bad in bad_parents {
            write_test_provenance(&fork_dir, Some(bad), &[]);
            assert!(
                walk_ancestors(&fork_dir, &by_id).is_err(),
                "expected error for parent entry: {bad}"
            );
        }
    }

    #[test]
    fn walk_ancestors_one_level() {
        let by_id = temp_dir();
        let parent_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(parent_ulid);
        let dev_dir = by_id.join(child_ulid);

        // dev's provenance names default at a fixed branch ULID.
        write_test_provenance(
            &dev_dir,
            Some(&format!("{parent_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV")),
            &[],
        );

        let ancestors = walk_ancestors(&dev_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 1);
        assert_eq!(ancestors[0].dir, default_dir);
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn walk_ancestors_crosses_into_readonly_tree() {
        // Simulate the fork-from-remote layout: a writable child in
        // `by_id/<child>/` whose parent only exists as a pulled readonly
        // skeleton in `readonly/<parent>/`. `walk_ancestors` must resolve
        // across both trees.
        let data_dir = temp_dir();
        std::fs::create_dir_all(&data_dir).unwrap();
        let by_id = data_dir.join("by_id");
        let readonly = data_dir.join("readonly");

        let parent_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let parent_dir = readonly.join(parent_ulid);
        let child_dir = by_id.join(child_ulid);

        write_test_provenance(
            &child_dir,
            Some(&format!("{parent_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV")),
            &[],
        );
        // Create the readonly parent dir so `resolve_ancestor_dir` picks it.
        std::fs::create_dir_all(&parent_dir).unwrap();

        let ancestors = walk_ancestors(&child_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 1);
        assert_eq!(
            ancestors[0].dir, parent_dir,
            "ancestor should resolve into the readonly/ tree"
        );
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn resolve_ancestor_dir_prefers_by_id_over_readonly() {
        let data_dir = temp_dir();
        let by_id = data_dir.join("by_id");
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        std::fs::create_dir_all(by_id.join(ulid)).unwrap();
        std::fs::create_dir_all(data_dir.join("readonly").join(ulid)).unwrap();
        assert_eq!(resolve_ancestor_dir(&by_id, ulid), by_id.join(ulid));
    }

    #[test]
    fn walk_ancestors_two_levels() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mid_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let leaf_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let default_dir = by_id.join(root_ulid);
        let mid_dir = by_id.join(mid_ulid);
        let leaf_dir = by_id.join(leaf_ulid);

        write_test_provenance(
            &mid_dir,
            Some(&format!("{root_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV")),
            &[],
        );
        write_test_provenance(
            &leaf_dir,
            Some(&format!("{mid_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV")),
            &[],
        );

        let ancestors = walk_ancestors(&leaf_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 2);
        assert_eq!(ancestors[0].dir, default_dir);
        assert_eq!(ancestors[1].dir, mid_dir);
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(
            ancestors[1].branch_ulid.as_deref(),
            Some("01BX5ZZKJKTSV4RRFFQ69G5FAV")
        );
    }

    // --- walk_extent_ancestors tests ---

    #[test]
    fn walk_extent_ancestors_missing_file_is_empty() {
        let by_id = temp_dir();
        let vol_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        fs::create_dir_all(&vol_dir).unwrap();
        assert!(walk_extent_ancestors(&vol_dir, &by_id).unwrap().is_empty());
    }

    #[test]
    fn walk_extent_ancestors_rejects_invalid_entries() {
        let by_id = temp_dir();
        let vol_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        let bad_entries = [
            "not-a-ulid/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "../01AAAAAAAAAAAAAAAAAAAAAAAA/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "01AAAAAAAAAAAAAAAAAAAAAAAA/",
            "01AAAAAAAAAAAAAAAAAAAAAAAA",
        ];
        for bad in bad_entries {
            write_test_provenance(&vol_dir, None, &[bad]);
            assert!(
                walk_extent_ancestors(&vol_dir, &by_id).is_err(),
                "expected error for extent_index entry: {bad}"
            );
        }
    }

    #[test]
    fn walk_extent_ancestors_one_level() {
        let by_id = temp_dir();
        let parent_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let parent_dir = by_id.join(parent_ulid);
        let child_dir = by_id.join(child_ulid);
        let entry = format!("{parent_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
        write_test_provenance(&child_dir, None, &[&entry]);

        let ancestors = walk_extent_ancestors(&child_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 1);
        assert_eq!(ancestors[0].dir, parent_dir);
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn walk_extent_ancestors_multi_entry() {
        // Flat union of several sources in a single signed provenance.
        let by_id = temp_dir();
        let a_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let b_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let a_dir = by_id.join(a_ulid);
        let b_dir = by_id.join(b_ulid);
        let c_dir = by_id.join(c_ulid);
        let a_entry = format!("{a_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let b_entry = format!("{b_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
        write_test_provenance(&c_dir, None, &[&a_entry, &b_entry]);

        let layers = walk_extent_ancestors(&c_dir, &by_id).unwrap();
        assert_eq!(layers.len(), 2, "two sources expected");
        assert_eq!(layers[0].dir, a_dir);
        assert_eq!(layers[1].dir, b_dir);
        assert_eq!(
            layers[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(
            layers[1].branch_ulid.as_deref(),
            Some("01BX5ZZKJKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn walk_extent_ancestors_dedupes_duplicate_entries() {
        let by_id = temp_dir();
        let a_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let a_dir = by_id.join(a_ulid);
        let c_dir = by_id.join(c_ulid);
        // Same source listed twice — later entry is silently dropped.
        let a1 = format!("{a_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let a2 = format!("{a_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
        write_test_provenance(&c_dir, None, &[&a1, &a2]);

        let layers = walk_extent_ancestors(&c_dir, &by_id).unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].dir, a_dir);
    }

    #[test]
    fn walk_extent_ancestors_combined_with_walk_ancestors() {
        // A single signed provenance carrying both fork parent (P) and
        // extent-index source (X). walk_ancestors returns [P],
        // walk_extent_ancestors returns [X]: the two chains are distinct.
        let by_id = temp_dir();
        let p_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let x_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let p_dir = by_id.join(p_ulid);
        let x_dir = by_id.join(x_ulid);
        let c_dir = by_id.join(c_ulid);
        let parent_entry = format!("{p_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let extent_entry = format!("{x_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
        write_test_provenance(&c_dir, Some(&parent_entry), &[&extent_entry]);

        let fork_chain = walk_ancestors(&c_dir, &by_id).unwrap();
        let extent_chain = walk_extent_ancestors(&c_dir, &by_id).unwrap();
        assert_eq!(fork_chain.len(), 1);
        assert_eq!(fork_chain[0].dir, p_dir);
        assert_eq!(extent_chain.len(), 1);
        assert_eq!(extent_chain[0].dir, x_dir);
    }

    // --- ancestor-aware open / read integration test ---

    /// Write data into a root volume, snapshot it, create a child volume via
    /// fork_volume, and verify the child can read the ancestor's data.
    #[test]
    fn open_reads_ancestor_segments() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        // Write data into the root volume and promote to a segment.
        let data = vec![0xABu8; 4096];
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            vol.snapshot().unwrap();
        }

        // Create a child volume branched from default.
        fork_volume(&child_dir, &default_dir).unwrap();

        // Child should see the ancestor's data through layer merge.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(by_id).unwrap();
    }

    /// Ancestor data is shadowed by a write in the live child volume.
    #[test]
    fn child_write_shadows_ancestor() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);
        let ancestor_data = vec![0xAAu8; 4096];
        let child_data = vec![0xBBu8; 4096];

        // Write into the root volume, promote, snapshot.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &ancestor_data).unwrap();
            vol.promote_for_test().unwrap();
            vol.snapshot().unwrap();
        }

        // Create child volume, write different data at the same LBA, promote.
        fork_volume(&child_dir, &default_dir).unwrap();
        {
            let mut vol = Volume::open(&child_dir, &by_id).unwrap();
            vol.write(0, &child_data).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Re-open child and verify child data wins.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), child_data);

        fs::remove_dir_all(by_id).unwrap();
    }

    // --- lock tests ---

    #[test]
    fn double_open_same_fork_fails() {
        let fork_dir = keyed_temp_dir();
        let _vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        // Second open on the same live fork must fail (lock already held).
        assert!(Volume::open(&fork_dir, &fork_dir).is_err());
        fs::remove_dir_all(fork_dir).unwrap();
    }

    // --- snapshot() tests ---

    #[test]
    fn snapshot_writes_marker_and_stays_live() {
        let fork_dir = keyed_temp_dir();
        let data = vec![0xAAu8; 4096];

        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &data).unwrap();
        let snap_ulid = vol.snapshot().unwrap();

        // Fork still has wal/ (still live).
        assert!(fork_dir.join("wal").is_dir());
        // Snapshot marker file exists.
        assert!(
            fork_dir
                .join("snapshots")
                .join(snap_ulid.to_string())
                .exists()
        );

        // Writes after snapshot still go to the same fork.
        let new_data = vec![0xBBu8; 4096];
        vol.write(1, &new_data).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), new_data);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_ulid_matches_last_segment_ulid() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();
        let snap_ulid = vol.snapshot().unwrap().to_string();

        // Snapshot promotes segments from pending/ to index/ + cache/, so
        // the segment shows up as `index/<ulid>.idx` after the call.
        let idx_files: Vec<_> = fs::read_dir(fork_dir.join("index"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(idx_files.len(), 1);
        let idx_name = idx_files[0].file_name().into_string().unwrap();
        assert_eq!(idx_name, format!("{snap_ulid}.idx"));

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_empty_wal_no_segment_written() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        // No writes — WAL is empty.
        vol.snapshot().unwrap();

        // pending/ should be empty (no segment written for empty WAL).
        let pending: Vec<_> = fs::read_dir(fork_dir.join("pending")).unwrap().collect();
        assert!(pending.is_empty());

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_idempotent_when_no_new_data() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();

        let ulid1 = vol.snapshot().unwrap();
        // No new writes — second snapshot must return the same ULID.
        let ulid2 = vol.snapshot().unwrap();
        assert_eq!(ulid1, ulid2);

        // Still only one snapshot marker on disk (filter out the
        // `<ulid>.manifest` file that sits next to each marker).
        let marker_count = fs::read_dir(fork_dir.join("snapshots"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|s| !s.contains('.')))
            .count();
        assert_eq!(marker_count, 1);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_not_idempotent_after_new_write() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();

        let ulid1 = vol.snapshot().unwrap();
        vol.write(1, &vec![0xBBu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let ulid2 = vol.snapshot().unwrap();
        assert_ne!(ulid1, ulid2);

        let marker_count = fs::read_dir(fork_dir.join("snapshots"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|s| !s.contains('.')))
            .count();
        assert_eq!(marker_count, 2);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_idempotent_after_auto_promoted_data_already_snapshotted() {
        // Data promoted via FLUSH_THRESHOLD (pending_entries empty at snapshot
        // time) but that segment was already covered by a prior snapshot.
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();
        vol.promote_for_test().unwrap(); // lands in pending/ with wal_ulid_1
        let ulid1 = vol.snapshot().unwrap(); // snapshot covers pending/wal_ulid_1
        // pending_entries is now empty; pending/ has one file but it's <= ulid1.
        let ulid2 = vol.snapshot().unwrap();
        assert_eq!(ulid1, ulid2);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_lock_held_after_snapshot() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.snapshot().unwrap();

        // Fork is still locked (still live); second open must fail.
        assert!(Volume::open(&fork_dir, &fork_dir).is_err());
        drop(vol); // now released

        // After drop, a fresh open succeeds.
        assert!(Volume::open(&fork_dir, &fork_dir).is_ok());

        fs::remove_dir_all(fork_dir).unwrap();
    }

    // --- fork_volume tests ---

    #[test]
    fn fork_volume_creates_fork_with_signed_provenance() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(root_ulid);
        let fork_dir = by_id.join(child_ulid);
        write_test_keypair(&default_dir);

        // snapshot default to give it a branch point.
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();
        let snap_ulid = vol.snapshot().unwrap().to_string();
        drop(vol);

        // Create the fork.
        fork_volume(&fork_dir, &default_dir).unwrap();
        assert!(fork_dir.join("wal").is_dir());
        assert!(fork_dir.join("pending").is_dir());
        assert!(
            !fork_dir.join("volume.parent").exists(),
            "standalone volume.parent file must not exist; parent lives in provenance"
        );

        // Parent lineage must be present in the signed provenance file.
        let lineage = crate::signing::read_lineage_verifying_signature(
            &fork_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let parent = lineage.parent.expect("fork must record parent");
        assert_eq!(parent.volume_ulid, root_ulid);
        assert_eq!(parent.snapshot_ulid, snap_ulid);
        assert!(lineage.extent_index.is_empty());

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_errors_if_source_has_no_snapshots() {
        let by_id = temp_dir();
        let root_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        // Create root_dir so canonicalize() succeeds, but leave it without
        // a snapshots/ directory so latest_snapshot returns "no snapshots".
        fs::create_dir_all(&root_dir).unwrap();
        let err = fork_volume(&child_dir, &root_dir).unwrap_err();
        assert!(err.to_string().contains("no snapshots"), "{err}");
    }

    #[test]
    fn fork_volume_at_pins_explicit_snapshot_without_requiring_local_marker() {
        // Simulate forking from a readonly ancestor: the source dir exists
        // but has no snapshots/ directory (prefetch hasn't landed yet). The
        // explicit pin must still succeed and be recorded in provenance.
        //
        // The source still needs its `volume.pub` locally so the fork can
        // embed the parent pubkey in its signed provenance — volume.pub is
        // pulled by the prefetch pathway before any fork operation.
        let by_id = temp_dir();
        let source_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let source_dir = by_id.join(source_ulid);
        let child_dir = by_id.join(child_ulid);
        fs::create_dir_all(&source_dir).unwrap();
        write_test_keypair(&source_dir);

        let branch_ulid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        fork_volume_at(&child_dir, &source_dir, branch_ulid).unwrap();

        let lineage = crate::signing::read_lineage_verifying_signature(
            &child_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let parent = lineage.parent.expect("fork must record parent");
        assert_eq!(parent.volume_ulid, source_ulid);
        assert_eq!(parent.snapshot_ulid, branch_ulid.to_string());

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_at_rejects_non_ulid_source_dir() {
        let tmp = temp_dir();
        let source_dir = tmp.join("not-a-ulid");
        let child_dir = tmp.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        fs::create_dir_all(&source_dir).unwrap();
        let branch_ulid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let err = fork_volume_at(&child_dir, &source_dir, branch_ulid).unwrap_err();
        assert!(err.to_string().contains("ULID"), "{err}");
        fs::remove_dir_all(tmp).ok();
    }

    #[test]
    fn fork_volume_uses_latest_snapshot_when_multiple_exist() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(root_ulid);
        let fork_dir = by_id.join(child_ulid);
        write_test_keypair(&default_dir);

        let data_snap1 = vec![0x11u8; 4096];
        let data_snap2 = vec![0x22u8; 4096];

        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        // First snapshot — should NOT be the branch point.
        vol.write(0, &data_snap1).unwrap();
        let snap1 = vol.snapshot().unwrap().to_string();
        // Second snapshot — should be the branch point.
        vol.write(1, &data_snap2).unwrap();
        let snap2 = vol.snapshot().unwrap().to_string();
        drop(vol);

        // snap2 must sort after snap1 (ULIDs are monotonically increasing).
        assert!(snap2 > snap1);

        fork_volume(&fork_dir, &default_dir).unwrap();
        let lineage = crate::signing::read_lineage_verifying_signature(
            &fork_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let parent = lineage.parent.expect("fork must record parent");
        assert_eq!(parent.volume_ulid, root_ulid);
        assert_eq!(
            parent.snapshot_ulid, snap2,
            "provenance parent should point to the latest snapshot"
        );

        // Fork branched from snap2 sees both pre-snap1 and pre-snap2 writes.
        let vol = Volume::open(&fork_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_snap1);
        assert_eq!(vol.read(1, 1).unwrap(), data_snap2);

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_from_child_fork_creates_three_level_chain() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mid_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let leaf_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let default_dir = by_id.join(root_ulid);
        let mid_dir = by_id.join(mid_ulid);
        let leaf_dir = by_id.join(leaf_ulid);
        write_test_keypair(&default_dir);

        let data_root = vec![0xAAu8; 4096];
        let data_mid = vec![0xBBu8; 4096];
        let data_leaf = vec![0xCCu8; 4096];

        // Root volume: write + snapshot.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &data_root).unwrap();
            vol.snapshot().unwrap();
        }

        // Mid volume: branch from default, write + snapshot.
        fork_volume(&mid_dir, &default_dir).unwrap();
        {
            let mut vol = Volume::open(&mid_dir, &by_id).unwrap();
            vol.write(1, &data_mid).unwrap();
            vol.snapshot().unwrap();
        }

        // Leaf volume: branch from mid.
        fork_volume(&leaf_dir, &mid_dir).unwrap();

        // origin chain: leaf → mid → default (read from signed provenance).
        let leaf_lineage = crate::signing::read_lineage_verifying_signature(
            &leaf_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let leaf_parent = leaf_lineage.parent.expect("leaf must record parent");
        assert_eq!(leaf_parent.volume_ulid, mid_ulid);
        let mid_lineage = crate::signing::read_lineage_verifying_signature(
            &mid_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let mid_parent = mid_lineage.parent.expect("mid must record parent");
        assert_eq!(mid_parent.volume_ulid, root_ulid);

        // Leaf sees data from all three levels.
        let vol = Volume::open(&leaf_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_root);
        assert_eq!(vol.read(1, 1).unwrap(), data_mid);
        assert_eq!(vol.read(2, 1).unwrap(), vec![0u8; 4096]); // unwritten

        // Write to leaf does not affect mid or default.
        drop(vol);
        {
            let mut vol = Volume::open(&leaf_dir, &by_id).unwrap();
            vol.write(2, &data_leaf).unwrap();
        }
        let vol = Volume::open(&leaf_dir, &by_id).unwrap();
        assert_eq!(vol.read(2, 1).unwrap(), data_leaf);
        assert_eq!(vol.ancestor_count(), 2);

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_errors_if_fork_exists() {
        let by_id = temp_dir();
        let root_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&root_dir);
        let mut vol = Volume::open(&root_dir, &by_id).unwrap();
        vol.snapshot().unwrap();
        drop(vol);

        fork_volume(&child_dir, &root_dir).unwrap();
        let err = fork_volume(&child_dir, &root_dir).unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");

        fs::remove_dir_all(by_id).unwrap();
    }

    // --- multi-snapshot read tests ---

    #[test]
    fn two_snapshots_data_readable_after_reopen() {
        let fork_dir = keyed_temp_dir();
        let data_a = vec![0xAAu8; 4096];
        let data_b = vec![0xBBu8; 4096];

        {
            let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
            vol.write(0, &data_a).unwrap();
            vol.snapshot().unwrap();
            vol.write(1, &data_b).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Re-open the same fork: both writes visible.
        let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_a);
        assert_eq!(vol.read(1, 1).unwrap(), data_b);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn fork_data_visible_across_ancestry() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);
        let data_a = vec![0xAAu8; 4096];
        let data_b = vec![0xBBu8; 4096];

        // Write to default, snapshot, create fork, write to fork.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &data_a).unwrap();
            vol.promote_for_test().unwrap();
            vol.snapshot().unwrap();
        }

        fork_volume(&child_dir, &default_dir).unwrap();
        {
            let mut vol = Volume::open(&child_dir, &by_id).unwrap();
            vol.write(1, &data_b).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Re-open child: sees both ancestor and own data.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_a);
        assert_eq!(vol.read(1, 1).unwrap(), data_b);
        assert_eq!(vol.ancestor_count(), 1);

        fs::remove_dir_all(by_id).unwrap();
    }

    // --- ULID cutoff tests ---

    /// Segments written to an ancestor volume *after* the branch point must not
    /// be visible to a child volume. This is the core correctness property of
    /// the per-ancestor ULID cutoff stored in `origin`.
    #[test]
    fn ulid_cutoff_hides_post_branch_ancestor_writes() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        let pre_branch = vec![0xAAu8; 4096];
        let post_branch = vec![0xBBu8; 4096];

        // Write pre-branch data to ancestor, snapshot, then branch.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &pre_branch).unwrap();
            vol.snapshot().unwrap();
        }
        fork_volume(&child_dir, &default_dir).unwrap();

        // Write post-branch data to the ancestor volume at LBA 1 (a new LBA).
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(1, &post_branch).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Child must see pre-branch data at LBA 0 and zeros at LBA 1.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(
            vol.read(0, 1).unwrap(),
            pre_branch,
            "pre-branch data must be visible"
        );
        assert_eq!(
            vol.read(1, 1).unwrap(),
            vec![0u8; 4096],
            "post-branch ancestor write must be invisible"
        );

        fs::remove_dir_all(by_id).unwrap();
    }

    /// A post-branch write to an ancestor that *overwrites* a pre-branch LBA
    /// must also be invisible — the child must still see the original value.
    #[test]
    fn ulid_cutoff_overwrite_stays_invisible() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        let original = vec![0xAAu8; 4096];
        let overwrite = vec![0xBBu8; 4096];

        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &original).unwrap();
            vol.snapshot().unwrap();
        }
        fork_volume(&child_dir, &default_dir).unwrap();

        // Ancestor overwrites LBA 0 after the branch.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &overwrite).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Child must still see the original pre-branch value.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), original);

        fs::remove_dir_all(by_id).unwrap();
    }

    // --- ReadonlyVolume tests ---

    #[test]
    fn readonly_volume_unwritten_returns_zeros() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        // Create the directory structure without a WAL (simulating a readonly base).
        fs::create_dir_all(fork_dir.join("pending")).unwrap();

        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), vec![0u8; 4096]);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    #[test]
    fn readonly_volume_reads_committed_segment() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        write_test_keypair(&fork_dir);

        let data = vec![0xCCu8; 4096];

        // Write data into the fork via Volume, then drop the lock.
        {
            let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
        }
        // Remove wal/ so ReadonlyVolume::open doesn't see a live WAL.
        // (ReadonlyVolume intentionally skips WAL replay; this also tests the
        //  no-WAL path.)
        fs::remove_dir_all(fork_dir.join("wal")).unwrap();

        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), data);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    #[test]
    fn readonly_volume_reads_ancestor_data() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        let ancestor_data = vec![0xDDu8; 4096];

        // Write data into default, snapshot, fork.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &ancestor_data).unwrap();
            vol.snapshot().unwrap();
        }
        fork_volume(&child_dir, &default_dir).unwrap();
        // ReadonlyVolume doesn't take a write lock, so this always works.

        let rv = ReadonlyVolume::open(&child_dir, &by_id).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), ancestor_data);

        fs::remove_dir_all(by_id).unwrap();
    }

    /// Regression test for the fork-from-remote demand-fetch bug: when a
    /// forked child needs to demand-fetch a segment that belongs to an
    /// ancestor, `find_segment_in_dirs` must route the fetcher at the
    /// ancestor's `index/` and `cache/` directories — not the child's.
    /// The child's `index/` does not hold ancestor `.idx` files in the
    /// `readonly/` layout, so using the child's dirs fails with ENOENT on
    /// the very first read. See docs/fork-from-remote-plan.md phase 1c.
    #[test]
    fn find_segment_in_dirs_routes_fetcher_at_ancestor_index_dir() {
        use crate::extentindex::BodySource;
        use std::sync::Mutex;

        struct OwnerAssertingFetcher {
            captured: Mutex<Option<(PathBuf, PathBuf)>>,
        }

        impl crate::segment::SegmentFetcher for OwnerAssertingFetcher {
            fn fetch_extent(
                &self,
                segment_id: Ulid,
                index_dir: &Path,
                body_dir: &Path,
                _extent: &crate::segment::ExtentFetch,
            ) -> io::Result<()> {
                *self.captured.lock().unwrap() = Some((index_dir.to_owned(), body_dir.to_owned()));
                // Simulate a successful fetch: write the body file where
                // the caller expects to find it on return.
                std::fs::create_dir_all(body_dir)?;
                std::fs::write(body_dir.join(format!("{segment_id}.body")), b"fake body")?;
                Ok(())
            }

            fn fetch_delta_body(
                &self,
                _segment_id: Ulid,
                _index_dir: &Path,
                _body_dir: &Path,
            ) -> io::Result<()> {
                Err(io::Error::other("unused"))
            }
        }

        let tmp = temp_dir();
        let child_dir = tmp.join("child");
        let ancestor_dir = tmp.join("ancestor");
        std::fs::create_dir_all(child_dir.join("index")).unwrap();
        std::fs::create_dir_all(ancestor_dir.join("index")).unwrap();

        // Only the ancestor holds the segment's `.idx`, matching the
        // fork-from-remote layout where each volume's signed index lives
        // in its own `index/` directory. Content is irrelevant — the
        // routing code only checks existence.
        let seg_ulid = Ulid::new();
        let idx_name = format!("{seg_ulid}.idx");
        std::fs::write(ancestor_dir.join("index").join(&idx_name), b"stub").unwrap();

        let layers = vec![AncestorLayer {
            dir: ancestor_dir.clone(),
            branch_ulid: None,
        }];
        let concrete = Arc::new(OwnerAssertingFetcher {
            captured: Mutex::new(None),
        });
        let fetcher: BoxFetcher = concrete.clone();

        let returned = find_segment_in_dirs(
            seg_ulid,
            &child_dir,
            &layers,
            Some(&fetcher),
            0,
            BodySource::Cached(0),
        )
        .expect("fetcher should have been routed at the ancestor's dirs");

        // The body must land under the ancestor, not the child.
        assert_eq!(
            returned,
            ancestor_dir.join("cache").join(format!("{seg_ulid}.body")),
        );
        assert!(
            !child_dir
                .join("cache")
                .join(format!("{seg_ulid}.body"))
                .exists(),
            "body must not be written under the child's cache dir"
        );

        // And the fetcher itself must have been called with the ancestor's
        // dirs — this is what the pre-fix code got wrong.
        let (idx_dir, body_dir) = concrete
            .captured
            .lock()
            .unwrap()
            .clone()
            .expect("fetcher must be called");
        assert_eq!(idx_dir, ancestor_dir.join("index"));
        assert_eq!(body_dir, ancestor_dir.join("cache"));

        fs::remove_dir_all(tmp).unwrap();
    }

    /// Complement to the previous test: when the child itself owns the
    /// segment (its own `index/` holds the `.idx`), the fetcher must be
    /// routed at the child's own dirs even if an ancestor is present.
    #[test]
    fn find_segment_in_dirs_prefers_self_over_ancestor_when_self_owns_idx() {
        use crate::extentindex::BodySource;
        use std::sync::Mutex;

        struct CaptureFetcher {
            captured: Mutex<Option<PathBuf>>,
        }
        impl crate::segment::SegmentFetcher for CaptureFetcher {
            fn fetch_extent(
                &self,
                segment_id: Ulid,
                index_dir: &Path,
                body_dir: &Path,
                _extent: &crate::segment::ExtentFetch,
            ) -> io::Result<()> {
                *self.captured.lock().unwrap() = Some(index_dir.to_owned());
                std::fs::create_dir_all(body_dir)?;
                std::fs::write(body_dir.join(format!("{segment_id}.body")), b"")?;
                Ok(())
            }
            fn fetch_delta_body(&self, _: Ulid, _: &Path, _: &Path) -> io::Result<()> {
                Err(io::Error::other("unused"))
            }
        }

        let tmp = temp_dir();
        let child_dir = tmp.join("child");
        let ancestor_dir = tmp.join("ancestor");
        std::fs::create_dir_all(child_dir.join("index")).unwrap();
        std::fs::create_dir_all(ancestor_dir.join("index")).unwrap();

        let seg_ulid = Ulid::new();
        let idx_name = format!("{seg_ulid}.idx");
        // Both self and ancestor have the `.idx`; self must win.
        std::fs::write(child_dir.join("index").join(&idx_name), b"stub").unwrap();
        std::fs::write(ancestor_dir.join("index").join(&idx_name), b"stub").unwrap();

        let layers = vec![AncestorLayer {
            dir: ancestor_dir.clone(),
            branch_ulid: None,
        }];
        let concrete = Arc::new(CaptureFetcher {
            captured: Mutex::new(None),
        });
        let fetcher: BoxFetcher = concrete.clone();

        let returned = find_segment_in_dirs(
            seg_ulid,
            &child_dir,
            &layers,
            Some(&fetcher),
            0,
            BodySource::Cached(0),
        )
        .unwrap();

        assert_eq!(
            returned,
            child_dir.join("cache").join(format!("{seg_ulid}.body")),
        );
        assert_eq!(
            concrete.captured.lock().unwrap().clone().unwrap(),
            child_dir.join("index")
        );

        fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn readonly_volume_does_not_see_wal_records() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        write_test_keypair(&fork_dir);

        let committed = vec![0xEEu8; 4096];
        let in_wal = vec![0xFFu8; 4096];

        // Write and promote LBA 0, then write LBA 1 to the WAL only.
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &committed).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &in_wal).unwrap();
        // Do NOT promote — LBA 1 is only in the WAL.
        // Drop the writable volume so the lock is released.
        drop(vol);

        // ReadonlyVolume skips WAL replay: LBA 1 must appear as zeros.
        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), committed);
        assert_eq!(rv.read(1, 1).unwrap(), vec![0u8; 4096]);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    // --- apply_gc_handoffs tests ---
    //
    // These tests simulate the coordinator GC workflow:
    //   write → flush → drain (pending→cache + index) → coordinator compacts
    //   into new segment + writes gc/*.pending → volume applies handoff.

    #[test]
    fn gc_handoff_applies_and_renames() {
        // End-to-end: stage a GC output, apply it (volume re-signs and
        // commits to bare), promote it (coordinator writes new idx and
        // deletes old idx via the inputs field), evict caches.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let pending_dir = base.join("pending");
        let old_ulid = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();
        simulate_upload(&mut vol);

        let new_ulid = simulate_coord_gc_staged(&mut vol, &base, &old_ulid);

        // Apply the handoff: volume re-signs `gc/<new>.staged` to `gc/<new>`.
        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 1);

        let gc_dir = base.join("gc");
        assert!(
            !gc_dir.join(format!("{new_ulid}.staged")).exists(),
            "staged file must be removed after commit"
        );
        assert!(
            gc_dir.join(&new_ulid).exists(),
            "bare gc/<new> must exist after commit"
        );

        // After apply_gc_handoffs the old idx is still present — promote_segment
        // is the step that deletes it (after the coordinator confirms upload).
        let cache_dir = base.join("cache");
        let index_dir = base.join("index");
        assert!(
            index_dir.join(format!("{old_ulid}.idx")).exists(),
            "old idx must persist until promote_segment runs"
        );
        assert!(
            !index_dir.join(format!("{new_ulid}.idx")).exists(),
            "new idx must not exist before promote_segment (not yet S3-confirmed)"
        );

        // Promote: coordinator confirms upload and asks the volume to write
        // index/<new>.idx + cache/<new>.body. promote_segment derives the
        // list of input ulids from the new segment's header and deletes
        // their idx files.
        let new_ulid_parsed = Ulid::from_string(&new_ulid).unwrap();
        vol.promote_segment(new_ulid_parsed).unwrap();

        assert!(
            index_dir.join(format!("{new_ulid}.idx")).exists(),
            "promote_segment must write index/<new>.idx"
        );
        assert!(
            !index_dir.join(format!("{old_ulid}.idx")).exists(),
            "promote_segment must delete index/<old>.idx for each input"
        );

        // Reads still work via cache/<new>.body.
        assert_eq!(vol.read(0, 2).unwrap(), data);

        // Coordinator finalize: deletes the bare gc/<new> file.
        vol.finalize_gc_handoff(new_ulid_parsed).unwrap();
        assert!(
            !gc_dir.join(&new_ulid).exists(),
            "finalize_gc_handoff must delete bare gc/<new>"
        );
        // Reads still work — cache/<new>.body covers it.
        assert_eq!(vol.read(0, 2).unwrap(), data);

        // Note: under the new protocol cache/<old>.* is dropped by
        // promote_segment's input cleanup path, not by a separate evict step.
        let _ = cache_dir;

        fs::remove_dir_all(base).unwrap();
    }

    /// Simulate a step 4 coordinator GC pass: read the old segment, build
    /// the compacted entries, write a `.staged` GC segment with the input
    /// ULID list embedded in the header (no sidecar manifest).
    fn simulate_coord_gc_staged(vol: &mut Volume, fork_dir: &Path, old_ulid: &str) -> String {
        use crate::{segment, signing};

        let idx_path = fork_dir.join("index").join(format!("{old_ulid}.idx"));
        let body_path = fork_dir.join("cache").join(format!("{old_ulid}.body"));
        let (_old_bss, mut entries, _) =
            segment::read_and_verify_segment_index(&idx_path, &vol.verifying_key).unwrap();
        let inline_bytes = segment::read_inline_section(&idx_path).unwrap();
        segment::read_extent_bodies(
            &body_path,
            0,
            &mut entries,
            [segment::EntryKind::Data, segment::EntryKind::Inline],
            &inline_bytes,
        )
        .unwrap();

        let (new_ulid, _) = vol.gc_checkpoint().unwrap();
        let new_ulid_str = new_ulid.to_string();

        let gc_dir = fork_dir.join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let (ephemeral_signer, _) = signing::generate_ephemeral_signer();
        let old_ulid_parsed = Ulid::from_string(old_ulid).unwrap();
        let inputs = vec![old_ulid_parsed];
        let tmp_path = gc_dir.join(format!("{new_ulid_str}.staged.tmp"));
        segment::write_gc_segment(&tmp_path, &mut entries, &inputs, ephemeral_signer.as_ref())
            .unwrap();
        fs::rename(&tmp_path, gc_dir.join(format!("{new_ulid_str}.staged"))).unwrap();

        new_ulid_str
    }

    #[test]
    fn gc_staged_handoff_applies_and_commits_bare() {
        // Step 4a: derive-at-apply path. Coordinator writes gc/<ulid>.staged
        // with inputs in the segment header; volume walks `.staged`, re-signs,
        // commits by renaming tmp → bare, removes `.staged`.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let pending_dir = base.join("pending");
        let old_ulid = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();
        simulate_upload(&mut vol);

        let new_ulid = simulate_coord_gc_staged(&mut vol, &base, &old_ulid);

        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 1);

        let gc_dir = base.join("gc");
        assert!(
            !gc_dir.join(format!("{new_ulid}.staged")).exists(),
            "`.staged` must be removed after commit"
        );
        assert!(
            gc_dir.join(&new_ulid).exists(),
            "bare <ulid> must exist after commit"
        );

        // Reads go through the extent index → new segment.
        assert_eq!(vol.read(0, 2).unwrap(), data);

        // Re-running is a no-op: bare exists, nothing to apply.
        let again = vol.apply_gc_handoffs().unwrap();
        assert_eq!(again, 0);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn gc_staged_crash_recovery_bare_wins() {
        // Crash state: rename tmp→bare succeeded, but `.staged` removal
        // failed. On next apply: detect the bare file, drop `.staged`,
        // count the handoff as recovered.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data: Vec<u8> = (0..8192).map(|i| (i * 5 + 17) as u8).collect();
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let pending_dir = base.join("pending");
        let old_ulid = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();
        simulate_upload(&mut vol);

        // Stage + commit once to produce a bare file.
        let new_ulid = simulate_coord_gc_staged(&mut vol, &base, &old_ulid);
        vol.apply_gc_handoffs().unwrap();

        // Inject the crash state: re-create a `.staged` next to the bare file.
        let gc_dir = base.join("gc");
        let bare_path = gc_dir.join(&new_ulid);
        let staged_path = gc_dir.join(format!("{new_ulid}.staged"));
        fs::copy(&bare_path, &staged_path).unwrap();

        // Apply: bare wins, `.staged` is removed, count=1 (crash-recovered).
        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 1);
        assert!(bare_path.exists());
        assert!(!staged_path.exists());

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn gc_staged_sweeps_stale_tmp_files() {
        // Stray `<ulid>.tmp` and `<ulid>.staged.tmp` files from crashed writes
        // are removed at the start of the apply pass.
        let base = keyed_temp_dir();
        let vol = Volume::open(&base, &base).unwrap();
        let gc_dir = base.join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let ulid = Ulid::new();
        let tmp1 = gc_dir.join(format!("{ulid}.tmp"));
        let tmp2 = gc_dir.join(format!("{ulid}.staged.tmp"));
        fs::write(&tmp1, b"garbage").unwrap();
        fs::write(&tmp2, b"garbage").unwrap();

        let mut vol = vol;
        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 0);
        assert!(!tmp1.exists(), ".tmp must be swept");
        assert!(!tmp2.exists(), ".staged.tmp must be swept");

        fs::remove_dir_all(base).unwrap();
    }

    /// Build a `.staged` GC output that compacts two input segments. Carries
    /// only the entries from `seg_b_ulid` (the live ones); entries from
    /// `seg_a_ulid` are intentionally omitted, so they become "removed"
    /// hashes from the apply path's perspective. Inputs list = [a, b] sorted.
    ///
    /// Used by the regression test below to set up a handoff with both
    /// Carried and Removed entries so we can crash-and-restart between
    /// `VolumeFinishApply` and `CoordPromote`.
    fn simulate_coord_gc_staged_two_inputs(
        vol: &mut Volume,
        fork_dir: &Path,
        seg_a_ulid: &str,
        seg_b_ulid: &str,
    ) -> String {
        use crate::{segment, signing};

        // Read entries from seg_b only — these become Carried.
        let idx_b = fork_dir.join("index").join(format!("{seg_b_ulid}.idx"));
        let body_b = fork_dir.join("cache").join(format!("{seg_b_ulid}.body"));
        let (_old_bss, mut entries, _) =
            segment::read_and_verify_segment_index(&idx_b, &vol.verifying_key).unwrap();
        let inline_bytes = segment::read_inline_section(&idx_b).unwrap();
        segment::read_extent_bodies(
            &body_b,
            0,
            &mut entries,
            [segment::EntryKind::Data, segment::EntryKind::Inline],
            &inline_bytes,
        )
        .unwrap();

        let (new_ulid, _) = vol.gc_checkpoint().unwrap();
        let new_ulid_str = new_ulid.to_string();

        let gc_dir = fork_dir.join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let (ephemeral_signer, _) = signing::generate_ephemeral_signer();
        let mut inputs = vec![
            Ulid::from_string(seg_a_ulid).unwrap(),
            Ulid::from_string(seg_b_ulid).unwrap(),
        ];
        inputs.sort();
        let tmp_path = gc_dir.join(format!("{new_ulid_str}.staged.tmp"));
        segment::write_gc_segment(&tmp_path, &mut entries, &inputs, ephemeral_signer.as_ref())
            .unwrap();
        fs::rename(&tmp_path, gc_dir.join(format!("{new_ulid_str}.staged"))).unwrap();

        new_ulid_str
    }

    #[test]
    fn gc_staged_crash_in_bare_phase_drops_removed_extents() {
        // Regression for a bug found by the TLA+ model (HandoffProtocol.tla):
        //
        // Sequence:
        //   1. Write D0 to lba 0, drain → seg_a with hash h0.
        //   2. Overwrite lba 0 with D1, drain → seg_b with hash h1.
        //      h0 is now LBA-dead; extent_index still has h0 → seg_a.
        //   3. Stage a GC output that carries h1 only. h0 is "removed".
        //   4. apply_gc_handoffs commits bare gc/<new>; in-memory
        //      extent_index now has h1 → new_ulid and h0 removed entirely.
        //   5. Crash + reopen. Rebuild reconstructs the extent_index from
        //      on-disk state — bare gc/<new> + index/<seg_a>.idx + index/<seg_b>.idx.
        //
        // Bug: rebuild uses insert_if_absent in pass order [bare, idx]. The
        // bare body inserts h1 → new_ulid (winning the later seg_b.idx).
        // But h0 is NOT in the bare body, so when the rebuild processes
        // index/<seg_a>.idx, it inserts h0 → seg_a — re-introducing the
        // entry the apply path explicitly removed.
        //
        // Fix: extentindex::rebuild reads the inputs field of every bare
        // gc/<ulid> file and skips the .idx files for those input segments.
        //
        // This test asserts the fixed behaviour: after restart, the
        // in-memory extent_index has no entry for h0.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let d0: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
        let h0 = blake3::hash(&d0);
        vol.write(0, &d0).unwrap();
        vol.promote_for_test().unwrap();
        let pending_dir = base.join("pending");
        let seg_a_ulid = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();
        simulate_upload(&mut vol);

        let d1: Vec<u8> = (0..8192).map(|i| (i * 11 + 17) as u8).collect();
        let h1 = blake3::hash(&d1);
        vol.write(0, &d1).unwrap();
        vol.promote_for_test().unwrap();
        let seg_b_ulid = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();
        simulate_upload(&mut vol);

        // Sanity: both hashes are in the extent_index, h0 → seg_a, h1 → seg_b.
        assert!(
            vol.extent_index.lookup(&h0).is_some(),
            "h0 should be in extent_index pre-GC"
        );
        assert!(
            vol.extent_index.lookup(&h1).is_some(),
            "h1 should be in extent_index pre-GC"
        );

        // Stage a GC output that carries h1 and "removes" h0 (by omitting it).
        let _new_ulid =
            simulate_coord_gc_staged_two_inputs(&mut vol, &base, &seg_a_ulid, &seg_b_ulid);

        // Apply: the in-memory extent_index now has h1 → new and h0 removed.
        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 1);
        assert!(
            vol.extent_index.lookup(&h0).is_none(),
            "h0 should be removed from extent_index after apply"
        );
        assert!(
            vol.extent_index.lookup(&h1).is_some(),
            "h1 should still be in extent_index after apply"
        );

        // Crash + reopen. Rebuild from disk.
        drop(vol);
        let vol = Volume::open(&base, &base).unwrap();

        // h1 must still be in the extent_index (carried by the bare GC body).
        assert!(
            vol.extent_index.lookup(&h1).is_some(),
            "h1 should be in extent_index after restart"
        );

        // h0 must NOT be in the extent_index. Before the fix, the rebuild
        // would re-introduce it via index/<seg_a>.idx because insert_if_absent
        // doesn't know that seg_a was consumed by the bare GC body.
        assert!(
            vol.extent_index.lookup(&h0).is_none(),
            "h0 must be gone after restart — was a Removed entry in the GC handoff. \
             A stale entry here means index/<seg_a>.idx was processed without consulting \
             the bare gc body's `inputs` field. See HandoffProtocol.tla counterexample."
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn gc_handoff_idempotent_after_crash() {
        // Simulate a crash between coordinator writing `.staged` and the
        // volume processing it. After reopen, the extent index is rebuilt
        // from index/*.idx (old segment still has its .idx), so reads are
        // correct before the handoff is applied. apply_gc_handoffs then
        // commits the bare `gc/<new>` and updates the extent index.
        let base = keyed_temp_dir();

        let old_ulid;
        let new_ulid;
        let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();

            let pending_dir = base.join("pending");
            old_ulid = fs::read_dir(&pending_dir)
                .unwrap()
                .flatten()
                .next()
                .unwrap()
                .file_name()
                .into_string()
                .unwrap();
            simulate_upload(&mut vol);

            new_ulid = simulate_coord_gc_staged(&mut vol, &base, &old_ulid);

            // "Crash" — drop the volume before apply_gc_handoffs runs.
        }

        let mut vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.read(0, 2).unwrap(), data);

        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 1);

        let gc_dir = base.join("gc");
        assert!(!gc_dir.join(format!("{new_ulid}.staged")).exists());
        assert!(gc_dir.join(&new_ulid).exists());

        let index_dir = base.join("index");
        assert!(
            index_dir.join(format!("{old_ulid}.idx")).exists(),
            "old idx must persist until promote_segment runs"
        );
        assert!(
            !index_dir.join(format!("{new_ulid}.idx")).exists(),
            "new idx must not exist before promote_segment"
        );

        let new_ulid_parsed = Ulid::from_string(&new_ulid).unwrap();
        vol.promote_segment(new_ulid_parsed).unwrap();

        assert!(
            index_dir.join(format!("{new_ulid}.idx")).exists(),
            "promote_segment must write index/<new>.idx"
        );
        assert!(
            !index_dir.join(format!("{old_ulid}.idx")).exists(),
            "promote_segment must delete index/<old>.idx for each input"
        );

        // Reads still correct: extent index points to new_ulid, body in cache/.
        assert_eq!(vol.read(0, 2).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    // --- FileCache (CLOCK) tests ---

    fn dummy_file() -> fs::File {
        fs::File::open("/dev/null").unwrap()
    }

    fn ulid(n: u128) -> Ulid {
        Ulid::from(n)
    }

    #[test]
    fn file_cache_hit_and_miss() {
        let mut cache = FileCache::new(4);
        assert!(cache.get(ulid(1)).is_none());

        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        assert!(cache.get(ulid(1)).is_some());
        assert!(cache.get(ulid(2)).is_none());
    }

    #[test]
    fn file_cache_returns_correct_layout() {
        let mut cache = FileCache::new(4);
        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(2), SegmentLayout::BodyOnly, dummy_file());

        let (layout, _) = cache.get(ulid(1)).unwrap();
        assert_eq!(layout, SegmentLayout::Full);

        let (layout, _) = cache.get(ulid(2)).unwrap();
        assert_eq!(layout, SegmentLayout::BodyOnly);
    }

    #[test]
    fn file_cache_replace_in_place() {
        let mut cache = FileCache::new(4);
        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(1), SegmentLayout::BodyOnly, dummy_file());

        let (layout, _) = cache.get(ulid(1)).unwrap();
        assert_eq!(layout, SegmentLayout::BodyOnly);
    }

    #[test]
    fn file_cache_fills_empty_slots_before_evicting() {
        let mut cache = FileCache::new(3);
        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(2), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(3), SegmentLayout::Full, dummy_file());

        // All three should be present — no eviction yet.
        assert!(cache.get(ulid(1)).is_some());
        assert!(cache.get(ulid(2)).is_some());
        assert!(cache.get(ulid(3)).is_some());
    }

    #[test]
    fn file_cache_clock_evicts_unreferenced() {
        let mut cache = FileCache::new(3);
        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(2), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(3), SegmentLayout::Full, dummy_file());

        // Touch 2 and 3 so their referenced bits are set.
        cache.get(ulid(2));
        cache.get(ulid(3));

        // Insert a 4th — should evict ulid(1) (unreferenced after insert,
        // since insert sets referenced but the CLOCK sweep clears it).
        // Actually: all three were inserted with referenced=true. Then we
        // called get() on 2 and 3 (re-setting their bits). The hand starts
        // at 0. On sweep: slot 0 (ulid 1) has referenced=true from insert,
        // so it gets cleared and hand advances. Slot 1 (ulid 2) has
        // referenced=true from get, cleared, hand advances. Slot 2 (ulid 3)
        // has referenced=true from get, cleared, hand advances. Back to
        // slot 0 (ulid 1) — now unreferenced — evicted.
        cache.insert(ulid(4), SegmentLayout::Full, dummy_file());

        assert!(
            cache.get(ulid(1)).is_none(),
            "ulid(1) should have been evicted"
        );
        assert!(cache.get(ulid(4)).is_some());
    }

    #[test]
    fn file_cache_recently_accessed_survives_eviction() {
        // With 3 slots, insert three entries. Access ulid(2) to refresh its
        // referenced bit, then insert a 4th. The CLOCK sweep clears all
        // referenced bits on the first pass, then evicts the entry at the
        // hand position (slot 0 = ulid(1)) on the second pass.
        // Crucially, get() on ulid(2) refreshes its bit *after* insert set it,
        // so when the sweep clears it on the first pass, ulid(2) gets cleared
        // like everyone else — but if we access it *between* two inserts, the
        // second sweep finds it referenced again.
        let mut cache = FileCache::new(3);
        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(2), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(3), SegmentLayout::Full, dummy_file());

        // First overflow: inserts ulid(4). The sweep clears all three
        // referenced bits (first pass), then evicts slot 0 (ulid(1)) on
        // the second pass. Hand ends at slot 1.
        cache.insert(ulid(4), SegmentLayout::Full, dummy_file());
        assert!(cache.get(ulid(1)).is_none(), "ulid(1) evicted");

        // Now touch ulid(2) — refreshes its referenced bit.
        cache.get(ulid(2));

        // Second overflow: inserts ulid(5). Hand is at slot 1.
        // Slot 1 (ulid(2)) ref=true → cleared, hand→2.
        // Slot 2 (ulid(3)) ref=false (cleared by first sweep, never re-accessed) → evicted.
        cache.insert(ulid(5), SegmentLayout::Full, dummy_file());
        assert!(cache.get(ulid(3)).is_none(), "ulid(3) evicted");
        assert!(
            cache.get(ulid(2)).is_some(),
            "ulid(2) survived — was accessed"
        );
        assert!(cache.get(ulid(4)).is_some());
        assert!(cache.get(ulid(5)).is_some());
    }

    #[test]
    fn file_cache_evict_by_id() {
        let mut cache = FileCache::new(4);
        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(2), SegmentLayout::Full, dummy_file());

        cache.evict(ulid(1));
        assert!(cache.get(ulid(1)).is_none());
        assert!(cache.get(ulid(2)).is_some());
    }

    #[test]
    fn file_cache_clear() {
        let mut cache = FileCache::new(4);
        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(2), SegmentLayout::Full, dummy_file());

        cache.clear();
        assert!(cache.get(ulid(1)).is_none());
        assert!(cache.get(ulid(2)).is_none());
    }

    #[test]
    fn file_cache_evict_frees_slot_for_reuse() {
        let mut cache = FileCache::new(2);
        cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
        cache.insert(ulid(2), SegmentLayout::Full, dummy_file());

        cache.evict(ulid(1));

        // The freed slot should be reused without evicting ulid(2).
        cache.insert(ulid(3), SegmentLayout::Full, dummy_file());
        assert!(cache.get(ulid(2)).is_some());
        assert!(cache.get(ulid(3)).is_some());
    }

    // --- inline extent tests ---

    #[test]
    fn inline_write_and_read_roundtrip() {
        // Small writes that compress below INLINE_THRESHOLD should be
        // readable immediately (from WAL) and after promotion (from inline_data).
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // All-same-byte 4KB data compresses to a few bytes → inline.
        let data = vec![0xAAu8; 4096];
        vol.write(0, &data).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data, "read before promotion");

        vol.promote_for_test().unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data, "read after promotion");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn inline_survives_reopen() {
        // After close+reopen, inline data is rebuilt from the segment's
        // inline section and reads still work.
        let base = keyed_temp_dir();
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            let data = vec![0xBBu8; 4096];
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
        }
        // Reopen: extent index is rebuilt from pending/ segments.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), vec![0xBBu8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn inline_coexists_with_body_entries() {
        // A segment with both inline and body entries: both are readable.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Small write → inline (compresses below threshold).
        let small = vec![0xCCu8; 4096];
        vol.write(0, &small).unwrap();

        // Large high-entropy write → body (doesn't compress below threshold).
        let large: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
        vol.write(1, &large).unwrap();

        vol.promote_for_test().unwrap();

        assert_eq!(vol.read(0, 1).unwrap(), small);
        assert_eq!(vol.read(1, 2).unwrap(), large);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn inline_dedup_as_canonical_source() {
        // An inline extent can serve as the canonical source for dedup.
        // Write the same small data at two different LBAs: first is DATA/Inline,
        // second should dedup (REF).
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0xDDu8; 4096]; // compresses → inline
        vol.write(0, &data).unwrap();
        vol.write(1, &data).unwrap(); // dedup hit → REF

        vol.promote_for_test().unwrap();

        // Both LBAs should read correctly — the REF resolves via the
        // inline canonical entry.
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn inline_repack_preserves_data() {
        // GC repack of a segment containing inline entries must preserve
        // inline data through the rewrite.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let d0 = vec![0xEEu8; 4096]; // inline
        let d1 = vec![0xFFu8; 4096]; // inline
        vol.write(0, &d0).unwrap();
        vol.write(1, &d1).unwrap();
        vol.promote_for_test().unwrap();

        // Overwrite LBA 0 to make d0 dead, creating GC opportunity.
        let d2 = vec![0x11u8; 4096];
        vol.write(0, &d2).unwrap();
        vol.promote_for_test().unwrap();

        // Repack: the segment with d0+d1 should be compacted; d1 survives.
        // Threshold 1.0 → compact any segment with dead extents.
        let stats = vol.repack(1.0).unwrap();
        assert!(stats.segments_compacted > 0);

        // Reads still correct after repack.
        assert_eq!(vol.read(0, 1).unwrap(), d2);
        assert_eq!(vol.read(1, 1).unwrap(), d1);

        fs::remove_dir_all(base).unwrap();
    }

    /// Simulates a crash window in `promote_segment`: the segment's cache body
    /// and idx have been committed on disk, but the extent-index CAS + pending
    /// delete have not run (in the offloaded design these live in a separate
    /// actor-side apply phase). The next `promote_segment` call for the same
    /// ULID must complete the half-done work — delete `pending/<ulid>` and
    /// transition extent-index entries to `BodySource::Cached` — not silently
    /// early-return.
    ///
    /// Today (synchronous in-actor `promote_segment`) the window is narrow but
    /// still observable because `extract_idx` and `promote_to_cache` commit
    /// their files via atomic rename before the pending delete runs. Under the
    /// planned worker offload the window widens, so this test is also a
    /// regression guard for the offload landing.
    #[test]
    fn promote_segment_recovers_mid_apply_crash() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = [42u8; 4096];
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let pending_dir = base.join("pending");
        let ulid_str = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .find_map(|e| {
                let name = e.file_name().into_string().ok()?;
                (!name.contains('.')).then_some(name)
            })
            .unwrap();
        let ulid = Ulid::from_string(&ulid_str).unwrap();
        let pending_path = pending_dir.join(&ulid_str);

        // Perform only the "worker" half of promote_segment — extract_idx +
        // promote_to_cache. Skip the extent-index CAS + pending delete.
        let cache_dir = base.join("cache");
        fs::create_dir_all(&cache_dir).unwrap();
        let body_path = cache_dir.join(format!("{ulid_str}.body"));
        let present_path = cache_dir.join(format!("{ulid_str}.present"));
        let index_dir = base.join("index");
        fs::create_dir_all(&index_dir).unwrap();
        let idx_path = index_dir.join(format!("{ulid_str}.idx"));
        segment::extract_idx(&pending_path, &idx_path).unwrap();
        segment::promote_to_cache(&pending_path, &body_path, &present_path).unwrap();

        assert!(pending_path.exists(), "precondition: pending survives");
        assert!(body_path.exists(), "precondition: cache body committed");
        assert!(idx_path.exists(), "precondition: idx committed");

        // Simulate the process crash: drop and reopen.
        drop(vol);
        let mut vol = Volume::open(&base, &base).unwrap();

        // Coordinator retries promote_segment on its next tick.
        vol.promote_segment(ulid).unwrap();

        // Invariant 1: pending/<ulid> is gone after the retry.
        assert!(
            !pending_path.exists(),
            "pending/<ulid> survived retry — half-done promote not recovered",
        );

        // Invariant 2: the extent-index entry for the written hash now points
        // at Cached, not Local.
        let hash = blake3::hash(&data);
        let loc = vol
            .extent_index
            .lookup(&hash)
            .expect("hash still present in extent index");
        assert!(
            matches!(loc.body_source, BodySource::Cached(_)),
            "extent-index entry still BodySource::Local after retry: {:?}",
            loc.body_source
        );

        // Invariant 3: data still reads back correctly.
        let actual = vol.read(0, 1).unwrap();
        assert_eq!(actual.as_slice(), data.as_slice(), "data readback wrong");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn all_inline_segment_readable() {
        // A segment where every entry is inline (body_length = 0).
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write several small extents — all compress to inline.
        for lba in 0..4u64 {
            let data = vec![lba as u8; 4096];
            vol.write(lba, &data).unwrap();
        }
        vol.promote_for_test().unwrap();

        // Verify all reads.
        for lba in 0..4u64 {
            let expected = vec![lba as u8; 4096];
            assert_eq!(vol.read(lba, 1).unwrap(), expected, "LBA {lba} mismatch");
        }

        // Verify the segment has body_length = 0.
        let pending_dir = base.join("pending");
        let seg_path = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .path();
        let (bss, _, _) =
            segment::read_and_verify_segment_index(&seg_path, &vol.verifying_key).unwrap();
        let file_len = fs::metadata(&seg_path).unwrap().len();
        assert_eq!(file_len, bss, "all-inline segment should have no body");

        fs::remove_dir_all(base).unwrap();
    }
}
