//! Worker offload types — the data exchanged between the actor and
//! worker threads. Each job is `Send` so the struct can cross a thread
//! boundary; each result is consumed back on the actor.
//!
//! `impl Volume` for the prep/apply phases lives in `volume/mod.rs`
//! because it touches private fields on `Volume`.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use ulid::Ulid;

use crate::{extentindex, gc_plan, segment, segment_cache};

use super::{
    AncestorLayer, BoxFetcher, DeltaRepackJob, DeltaRepackResult, ReclaimJob, ReclaimResult,
    RepackJob, RepackResult, StagedApply, SweepJob, SweepResult,
};

/// Data needed by the worker thread to write a pending segment.
///
/// Produced by [`super::Volume::prepare_promote`] on the actor thread, consumed by
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
/// Consumed by [`super::Volume::apply_promote`] on the actor thread.
pub struct PromoteResult {
    pub segment_ulid: Ulid,
    pub old_wal_ulid: Ulid,
    pub old_wal_path: PathBuf,
    pub body_section_start: u64,
    pub entries: Vec<segment::SegmentEntry>,
    pub pre_promote_offsets: Vec<Option<u64>>,
}

/// The two ULIDs needed for a GC checkpoint, minted atomically in order.
///
/// Ordering invariant: `u_gc < u_flush`.  Both come from the volume's own
/// monotonic mint (never from an external clock), and `UlidMint` guarantees
/// strict monotonicity even within the same millisecond.  Minting both up
/// front — before any I/O — is what makes the ordering self-documenting and
/// crash-safe: the WAL segment flushed at `u_flush` is guaranteed > `u_gc`,
/// so rebuild applies the GC output before the flushed WAL segment.
///
/// No `u_wal` is pre-minted for the *next* WAL. Any future `mint.next()`
/// that opens a WAL is monotonically > `u_flush > u_gc` by construction, so
/// the "new WAL above GC output" invariant holds without reservation.
/// Deferring WAL open to first-write is what avoids per-tick WAL churn on
/// idle volumes.
pub(super) struct GcCheckpointUlids {
    pub(super) u_gc: Ulid,
    pub(super) u_flush: Ulid,
}

/// Result of the GC checkpoint prep phase.
///
/// Carries the pre-minted ULIDs and an optional promote job.  The actor
/// dispatches the job to the worker and stashes the reply.  When `job`
/// is `None` the WAL was empty and the checkpoint completes immediately.
pub struct GcCheckpointPrep {
    pub u_gc: Ulid,
    /// Segment ULID used for the promoted WAL.  Used to identify the
    /// GC promote's `PromoteComplete` among other in-flight promotes.
    pub u_flush: Ulid,
    pub job: Option<PromoteJob>,
}

/// Data needed by the worker thread to materialise a coordinator-emitted
/// GC plan (`gc/<ulid>.plan`).
///
/// Produced by [`super::Volume::prepare_plan_apply`] on the actor thread. The worker
/// builds a `BodyResolver` from the owned fields, resolves bodies, assembles
/// the output segment, signs it with the volume's key, and writes it to
/// `gc/<ulid>.tmp`. It also collects the body-owning entries from each
/// input's `.idx` so the actor's apply phase can derive extent-index updates
/// against the **current** extent index (which may have diverged while the
/// worker was running).
pub struct GcPlanApplyJob {
    pub plan_path: PathBuf,
    pub new_ulid: Ulid,
    pub gc_dir: PathBuf,
    pub index_dir: PathBuf,
    pub base_dir: PathBuf,
    /// Cloned ancestor layers — the worker walks them via a local
    /// `BodyResolver` impl for ancestor-aware body lookup.
    pub ancestor_layers: Vec<AncestorLayer>,
    /// Demand-fetcher, if one is attached to the volume.
    pub fetcher: Option<BoxFetcher>,
    /// Snapshot of the merged extent index at dispatch time. The worker uses
    /// it to resolve DedupRef / Delta-base bodies. The actor's apply phase
    /// uses a fresh snapshot of `self.extent_index` to compute updates so
    /// concurrent writes don't get clobbered.
    pub extent_index: Arc<extentindex::ExtentIndex>,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    /// Pre-parsed plan. The actor validates parse + ULID match before
    /// dispatch so the worker never sees a malformed plan.
    pub plan: gc_plan::GcPlan,
}

/// Result returned by the worker after materialising a plan. The actor's
/// apply phase uses it to derive extent-index updates, commit the rename,
/// and clean up the plan file.
pub struct GcPlanApplyResult {
    pub new_ulid: Ulid,
    pub plan_path: PathBuf,
    pub gc_dir: PathBuf,
    /// `gc/<ulid>.tmp` — already written + signed. Apply phase renames it
    /// to bare `gc/<ulid>`. `None` for cancelled materialisations.
    pub tmp_path: Option<PathBuf>,
    pub new_bss: u64,
    pub entries: Vec<segment::SegmentEntry>,
    pub inputs: Vec<Ulid>,
    /// Body-owning entries from each input's `.idx` at dispatch time.
    /// `(hash, kind, input_ulid)` — used by the apply phase to build the
    /// to-remove + stale-cancel sets.
    pub input_old_entries: Vec<(blake3::Hash, segment::EntryKind, Ulid)>,
    /// Inline bytes of the freshly written output segment, used to populate
    /// `inline_data` on extent locations during apply.
    pub handoff_inline: Vec<u8>,
    /// `Applied` when materialisation succeeded; `Cancelled` when the worker
    /// decided to bail (missing input, unresolvable hash). Apply only does
    /// cleanup for cancelled results.
    pub outcome: StagedApply,
}

/// Data needed by the worker thread to promote a confirmed-in-S3 segment
/// from `pending/<ulid>` (drain path) or `gc/<ulid>` (GC path) into
/// `cache/<ulid>.{body,present}` + `index/<ulid>.idx`.
///
/// Produced by [`super::Volume::prepare_promote_segment`] on the actor thread.
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
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// Result returned by the worker after a `PromoteSegmentJob`.
///
/// Consumed by [`super::Volume::apply_promote_segment_result`] on the actor
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

/// Inputs for signing and writing a `snapshots/<snap_ulid>.manifest`
/// file plus its `snapshots/<snap_ulid>` marker.
///
/// Produced by [`super::Volume::prepare_sign_snapshot_manifest`] on the actor
/// thread. The worker enumerates `index/` itself — keeping the
/// `read_dir` off the actor is the whole point of the offload.
///
/// `extent_index` and `lbamap` are snapshot `Arc`s used to filter fully-dead
/// segments out of the manifest at sign time. A segment is omitted if every
/// entry in its `.idx` is dead under the liveness predicate (no body still
/// canonical in `extent_index`, no LBA still mapped to a thin entry's hash).
/// Reclamation of those segment files remains GC's responsibility — the
/// filter is a manifest-only optimisation.
pub struct SignSnapshotManifestJob {
    pub snap_ulid: Ulid,
    pub base_dir: PathBuf,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub extent_index: Arc<extentindex::ExtentIndex>,
    pub lbamap: Arc<crate::lbamap::LbaMap>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// Result of a [`SignSnapshotManifestJob`]. Consumed by
/// [`super::Volume::apply_sign_snapshot_manifest_result`] on the actor thread
/// to flip the `has_new_segments` flag.
pub struct SignSnapshotManifestResult {
    pub snap_ulid: Ulid,
}

/// Job dispatched from the actor to the worker thread.
pub enum WorkerJob {
    Promote(PromoteJob),
    GcPlan(GcPlanApplyJob),
    PromoteSegment(PromoteSegmentJob),
    Sweep(SweepJob),
    Repack(RepackJob),
    DeltaRepack(DeltaRepackJob),
    SignSnapshotManifest(SignSnapshotManifestJob),
    Reclaim(ReclaimJob),
}

/// Result returned by the worker thread to the actor.
///
/// Each variant wraps its own `io::Result` so the actor can distinguish
/// which job type failed.  `PromoteSegment` carries the target ULID
/// out-of-band so the actor can match a failed job to its parked reply
/// (the `Err` path otherwise has no ULID to match on).
pub enum WorkerResult {
    Promote(io::Result<PromoteResult>),
    GcPlan(io::Result<GcPlanApplyResult>),
    PromoteSegment {
        ulid: Ulid,
        result: io::Result<PromoteSegmentResult>,
    },
    Sweep(io::Result<SweepResult>),
    Repack(io::Result<RepackResult>),
    DeltaRepack(io::Result<DeltaRepackResult>),
    SignSnapshotManifest(io::Result<SignSnapshotManifestResult>),
    Reclaim(io::Result<ReclaimResult>),
}
