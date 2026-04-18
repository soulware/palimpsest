// VolumeActor + VolumeHandle: the intended integration pattern for ublk and NBD.
//
// VolumeActor owns a Volume exclusively and processes requests from a
// crossbeam-channel in a dedicated thread.  VolumeHandle is the shareable
// client handle — Clone + Send — held by NBD/ublk queue threads.
//
// Reads bypass the channel entirely: the calling thread loads the current
// ReadSnapshot via ArcSwap and resolves the read locally.  Writes, flushes,
// and compaction go through the channel and block until the actor replies.
//
// The actor publishes a new ReadSnapshot after every write so that reads
// immediately reflect all accepted writes, including those not yet flushed
// to a pending/ segment — matching the read-your-writes guarantee of a
// physical block device.
//
// See docs/architecture.md — "Concurrency model" for rationale and design.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender, bounded, tick};
use log::warn;

use ulid::Ulid;

use crate::extentindex::ExtentIndex;
use crate::lbamap::LbaMap;
use crate::segment::{self, BoxFetcher};
use crate::volume::{
    AncestorLayer, CompactionStats, DeltaRepackJob, DeltaRepackResult, DeltaRepackStats,
    DeltaRepackedSegment, FileCache, GcCheckpointPrep, GcHandoffJob, GcHandoffResult,
    NoopSkipStats, PromoteJob, PromoteResult, PromoteSegmentJob, PromoteSegmentPrep,
    PromoteSegmentResult, ReclaimCandidate, ReclaimOutcome, ReclaimPlan, ReclaimProposed,
    ReclaimThresholds, RepackJob, RepackResult, RepackedDeadEntry, RepackedLiveEntry,
    RepackedSegment, SignSnapshotManifestJob, SignSnapshotManifestResult, SweepJob, SweepResult,
    SweptDeadEntry, SweptLiveEntry, Volume, WorkerJob, WorkerResult, find_segment_in_dirs,
    open_delta_body_in_dirs, read_extents, scan_reclaim_candidates,
};

// ---------------------------------------------------------------------------
// Static configuration
// ---------------------------------------------------------------------------

/// Static configuration for a volume session.
///
/// Holds the fork directory paths and optional fetcher — data that is fixed
/// for the lifetime of the session.  Wrapped in `Arc` and shared across all
/// `VolumeHandle` clones without copying.
pub struct VolumeConfig {
    pub base_dir: PathBuf,
    pub ancestor_layers: Vec<AncestorLayer>,
    pub fetcher: Option<BoxFetcher>,
}

// ---------------------------------------------------------------------------
// Read snapshot
// ---------------------------------------------------------------------------

/// Immutable snapshot of the LBA map and extent index.
///
/// Published by `VolumeActor` after every `write()` and after every WAL
/// promotion.  Readers load the current snapshot via `ArcSwap::load()` —
/// no channel round-trip, no lock.
///
/// Both map fields are `Arc`-wrapped so that publication is O(1): the actor
/// calls `Arc::clone` on its live maps.  If a reader is still holding the
/// previous version when the next write occurs, `Arc::make_mut` in `Volume`
/// performs a copy-on-write clone; in practice reads complete in microseconds
/// so the refcount is almost always 1.
///
/// `flush_gen` is incremented by the actor on every WAL promotion.  Handles
/// compare it against their cached value; a change means the extent index now
/// contains post-promote (segment-format) body offsets and any cached WAL file
/// descriptor must be evicted.  Because `flush_gen` is stored inside the
/// snapshot, a handle always sees a consistent pair: if it observes a new
/// generation it also observes the updated extent index in the same atomic
/// load — there is no window between the two.
pub struct ReadSnapshot {
    pub lbamap: Arc<LbaMap>,
    pub extent_index: Arc<ExtentIndex>,
    pub flush_gen: u64,
}

// ---------------------------------------------------------------------------
// Channel message type
// ---------------------------------------------------------------------------

pub(crate) enum VolumeRequest {
    Write {
        lba: u64,
        data: Vec<u8>,
        reply: Sender<io::Result<()>>,
    },
    Flush {
        reply: Sender<io::Result<()>>,
    },
    Trim {
        start_lba: u64,
        lba_count: u32,
        reply: Sender<io::Result<()>>,
    },
    SweepPending {
        reply: Sender<io::Result<CompactionStats>>,
    },
    ApplyGcHandoffs {
        reply: Sender<io::Result<usize>>,
    },
    Repack {
        min_live_ratio: f64,
        reply: Sender<io::Result<CompactionStats>>,
    },
    DeltaRepackPostSnapshot {
        reply: Sender<io::Result<DeltaRepackStats>>,
    },
    /// Promote the current WAL to a `pending/` segment via the worker
    /// thread.  Reply is sent once `pending/<ulid>` is on disk.
    /// No-op (immediate reply) if the WAL is empty.
    PromoteWal {
        reply: Sender<io::Result<()>>,
    },
    GcCheckpoint {
        reply: Sender<io::Result<(Ulid, Ulid)>>,
    },
    RedactSegment {
        ulid: Ulid,
        reply: Sender<io::Result<()>>,
    },
    Promote {
        ulid: Ulid,
        reply: Sender<io::Result<()>>,
    },
    FinalizeGcHandoff {
        ulid: Ulid,
        reply: Sender<io::Result<()>>,
    },
    SignSnapshotManifest {
        snap_ulid: Ulid,
        reply: Sender<io::Result<()>>,
    },
    NoopStats {
        reply: Sender<NoopSkipStats>,
    },
    /// Phase 1 of extent reclamation: capture an LBA map snapshot over a
    /// target range. Returns a `ReclaimPlan` the caller carries through
    /// the off-actor heavy work. See `docs/design-extent-reclamation.md`.
    ReclaimSnapshot {
        start_lba: u64,
        lba_length: u32,
        reply: Sender<io::Result<ReclaimPlan>>,
    },
    /// Phase 3 of extent reclamation: verify the plan's snapshot is still
    /// valid and commit each proposed rewrite via internal-origin writes.
    /// The precondition check is pointer-equality on the captured
    /// `Arc<LbaMap>`, so a single short critical section suffices.
    ReclaimCommit {
        plan: ReclaimPlan,
        proposed: Vec<ReclaimProposed>,
        reply: Sender<io::Result<ReclaimOutcome>>,
    },
    Shutdown,
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

/// Owns a `Volume` exclusively and drives the request channel.
///
/// Spawn a thread and call `actor.run()`.  The thread exits when the last
/// `VolumeHandle` is dropped (channel closes) or when a `Shutdown` message
/// is received.
pub struct VolumeActor {
    volume: Volume,
    snapshot: Arc<ArcSwap<ReadSnapshot>>,
    rx: Receiver<VolumeRequest>,
    /// Local promotion counter.  Incremented on every WAL promotion and
    /// embedded into the next `ReadSnapshot` store so that handles see a
    /// consistent (generation, extent_index) pair from a single atomic load.
    flush_gen: u64,
    /// Sender for dispatching jobs to the worker thread.
    /// `Option` so shutdown can `take()` it, dropping the sender to signal
    /// the worker to exit.
    worker_tx: Option<Sender<WorkerJob>>,
    /// Receiver for results from the worker thread.
    /// Third arm in the `select!` loop.
    worker_rx: Receiver<WorkerResult>,
    /// Join handle for the worker thread, joined on shutdown.
    worker_handle: Option<JoinHandle<()>>,
    /// Number of promote jobs dispatched but not yet applied.
    promotes_in_flight: usize,
    /// Monotonic counter, incremented on every `WorkerJob::Promote`
    /// dispatch (post-write threshold, `PromoteWal`, `GcCheckpoint`).
    /// Used together with `completed_gen` to park NBD `Flush` replies
    /// until every promote dispatched *before* the flush has had its
    /// old-WAL fsync completed by the worker.
    promote_gen: u64,
    /// Monotonic counter, incremented on every `WorkerResult::Promote`
    /// (success *or* error) received from the worker.  For errors the
    /// actor performs a fallback fsync itself before bumping the
    /// counter, so `completed_gen >= needed_gen` always implies every
    /// promote dispatched at or before `needed_gen` has had its old
    /// WAL made durable.
    completed_gen: u64,
    /// FIFO queue of old WAL paths for promotes currently dispatched
    /// but not yet completed.  Matches the worker's strict dispatch
    /// order (single thread, bounded FIFO channel).  Popped on every
    /// worker result; the error path re-fsyncs the popped path on the
    /// actor thread as a fallback before bumping `completed_gen`.
    inflight_old_wals: VecDeque<PathBuf>,
    /// NBD `Flush` replies parked until `completed_gen >= needed_gen`.
    /// Each entry records the `promote_gen` snapshot at the time the
    /// flush arrived; as worker results come in the actor resolves any
    /// entries whose precondition now holds.
    parked_flushes: Vec<ParkedFlush>,
    /// Parked GC checkpoint: the reply sender and GC ULIDs, waiting for
    /// the GC promote (`u_flush`) to complete on the worker.  `None`
    /// when no GC checkpoint is in progress.
    parked_gc: Option<ParkedGcCheckpoint>,
    /// Parked `PromoteWal` replies waiting for their specific promote to
    /// complete.  Multiple can be parked if several `PromoteWal` requests
    /// arrive while the worker is busy.
    parked_promote_wal: Vec<ParkedPromoteWal>,
    /// Parked `Promote` (promote_segment) replies waiting for their
    /// specific segment promote to complete on the worker.
    parked_promote_segments: Vec<ParkedPromoteSegment>,
    /// Number of `promote_segment` jobs dispatched but not yet applied.
    promote_segments_in_flight: usize,
    /// In-progress GC handoff batch.  At most one batch at a time.
    /// `None` when no handoff processing is active.
    parked_handoffs: Option<ParkedGcHandoffs>,
    /// Whether a GC handoff job is currently on the worker thread.
    handoff_in_flight: bool,
    /// Reply channel for an in-flight `SweepPending` request, parked
    /// while the worker thread executes the sweep.  `None` when no
    /// sweep is in progress; concurrent `SweepPending` requests are
    /// rejected with an error.
    parked_sweep: Option<Sender<io::Result<CompactionStats>>>,
    /// Reply channel for an in-flight `Repack` request, parked while
    /// the worker thread executes the repack.  `None` when no repack
    /// is in progress; concurrent `Repack` requests are rejected with
    /// an error.
    parked_repack: Option<Sender<io::Result<CompactionStats>>>,
    /// Reply channel for an in-flight `DeltaRepackPostSnapshot`
    /// request, parked while the worker thread executes the delta
    /// rewrite.  `None` when no delta_repack is in progress;
    /// concurrent requests are rejected with an error.
    parked_delta_repack: Option<Sender<io::Result<DeltaRepackStats>>>,
    /// Reply channel for an in-flight `SignSnapshotManifest` request,
    /// parked while the worker thread enumerates `index/`, signs, and
    /// writes the manifest + marker.  `None` when none is in flight;
    /// concurrent requests are rejected (the coordinator's per-volume
    /// snapshot lock already prevents them in production).
    parked_sign_snapshot_manifest: Option<Sender<io::Result<()>>>,
}

/// State stashed while a `PromoteWal` promote is in flight.
struct ParkedPromoteWal {
    segment_ulid: Ulid,
    reply: Sender<io::Result<()>>,
}

/// State stashed while an NBD `Flush` waits for an in-flight promote's
/// old-WAL fsync to complete on the worker.  Released when
/// `VolumeActor::completed_gen >= needed_gen`.
struct ParkedFlush {
    needed_gen: u64,
    reply: Sender<io::Result<()>>,
}

/// State stashed while a `promote_segment` job is on the worker thread.
struct ParkedPromoteSegment {
    ulid: Ulid,
    reply: Sender<io::Result<()>>,
}

/// State stashed while a GC checkpoint's promote is in flight.
struct ParkedGcCheckpoint {
    u_repack: Ulid,
    u_sweep: Ulid,
    u_flush: Ulid,
    reply: Sender<io::Result<(Ulid, Ulid)>>,
}

/// State for an in-progress batch of GC handoff applications.
///
/// The actor dispatches one handoff at a time to the worker thread.
/// On each completion it applies the result, then dispatches the next.
/// When the list is exhausted, the reply (if any) is sent.
struct ParkedGcHandoffs {
    remaining: Vec<(PathBuf, Ulid)>,
    reply: Option<Sender<io::Result<usize>>>,
    applied_count: usize,
}

/// Idle period after which the actor promotes a non-empty WAL to a pending
/// segment even without an explicit flush request.  10 seconds is a
/// conservative value chosen for observability during development; it can be
/// tightened without any correctness implications.
const IDLE_FLUSH_INTERVAL: Duration = Duration::from_secs(10);

impl VolumeActor {
    /// Republish the snapshot with updated maps and increment `flush_gen`.
    ///
    /// Called after any operation that changes the extent index or LBA map:
    /// WAL promotions (explicit flush, threshold, idle tick) and compaction
    /// operations (sweep_pending, repack).  Handles compare `flush_gen` against
    /// their cached value and evict their file-handle cache on a mismatch,
    /// ensuring they never serve stale segment offsets after a compaction
    /// deletes old segment files.
    fn publish_snapshot(&mut self) {
        self.flush_gen += 1;
        let (lbamap, extent_index) = self.volume.snapshot_maps();
        self.snapshot.store(Arc::new(ReadSnapshot {
            lbamap,
            extent_index,
            flush_gen: self.flush_gen,
        }));
    }

    /// NBD `Flush` arrives.  The current WAL has already been fsynced
    /// by the caller; here we decide whether the reply can go out
    /// immediately or must wait for an in-flight promote's old-WAL
    /// fsync on the worker.
    fn park_or_resolve_flush(&mut self, reply: Sender<io::Result<()>>) {
        if self.completed_gen >= self.promote_gen {
            let _ = reply.send(Ok(()));
        } else {
            self.parked_flushes.push(ParkedFlush {
                needed_gen: self.promote_gen,
                reply,
            });
        }
    }

    /// Called after the actor has finished applying a successful
    /// `Promote(Ok(..))` result — extent index CAS'd, old WAL deleted,
    /// snapshot republished.  Pops the FIFO head of
    /// `inflight_old_wals` (matching the worker's dispatch order),
    /// bumps `completed_gen`, and resolves any parked flushes whose
    /// precondition now holds.  The worker already fsynced the old
    /// WAL, so no extra I/O is needed here.  Resolving *after*
    /// `apply_promote` ensures callers of `Flush` observe the
    /// housekeeping state (old WAL deleted, new snapshot published)
    /// and not just the durability barrier.
    fn on_promote_success(&mut self) {
        self.inflight_old_wals.pop_front();
        self.completed_gen += 1;
        self.resolve_parked_flushes(Ok(()));
    }

    /// Called after each worker-result `Promote(Err(..))`.  The
    /// worker may or may not have fsynced the old WAL before failing,
    /// so we perform a best-effort fallback fsync on the actor thread
    /// to guarantee that `completed_gen` advancing implies durability
    /// of every write that was in the old WAL at dispatch time.
    fn on_promote_failure(&mut self) {
        let outcome = if let Some(path) = self.inflight_old_wals.pop_front() {
            match std::fs::File::open(&path).and_then(|f| f.sync_data()) {
                Ok(()) => Ok(()),
                Err(e) => {
                    warn!("fallback fsync of {} failed: {e}", path.display());
                    Err(io::Error::other(format!(
                        "promote failed and fallback WAL fsync failed: {e}"
                    )))
                }
            }
        } else {
            // Shouldn't happen — every dispatch pushes a path — but
            // don't panic in library code.  Treat as "nothing to
            // fsync" which is vacuously durable.
            Ok(())
        };
        self.completed_gen += 1;
        self.resolve_parked_flushes(outcome);
    }

    /// Drain `parked_flushes` of any entry whose `needed_gen` is now
    /// satisfied by `completed_gen`, delivering `outcome` to each.
    /// Entries whose `needed_gen` is still in the future stay parked.
    fn resolve_parked_flushes(&mut self, outcome: io::Result<()>) {
        let done = self.completed_gen;
        let mut i = 0;
        while i < self.parked_flushes.len() {
            if self.parked_flushes[i].needed_gen <= done {
                let parked = self.parked_flushes.swap_remove(i);
                let reply_outcome = match &outcome {
                    Ok(()) => Ok(()),
                    Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
                };
                let _ = parked.reply.send(reply_outcome);
            } else {
                i += 1;
            }
        }
    }

    /// Forward the result of a completed `promote_segment` job to the
    /// matching parked reply, if any.  Matched by ULID — callers receive
    /// the apply-phase outcome, not the worker outcome (those only differ
    /// when apply itself fails, which is rare: both success paths imply
    /// the segment is fully promoted and the extent index is up to date).
    fn reply_parked_promote_segment(&mut self, ulid: Ulid, result: io::Result<()>) {
        if let Some(idx) = self
            .parked_promote_segments
            .iter()
            .position(|p| p.ulid == ulid)
        {
            let parked = self.parked_promote_segments.swap_remove(idx);
            let _ = parked.reply.send(result);
        }
    }

    /// Dispatch a promote job to the worker thread.
    ///
    /// Calls [`Volume::prepare_promote`] to snapshot the WAL state and open
    /// a fresh WAL, then sends the job to the worker.  No-op if the WAL
    /// is empty.  Logs and returns on error.
    fn dispatch_promote(&mut self) {
        let job = match self.volume.prepare_promote() {
            Ok(Some(job)) => job,
            Ok(None) => return,
            Err(e) => {
                warn!("promote prep failed: {e}");
                return;
            }
        };
        if let Some(tx) = &self.worker_tx {
            let old_wal_path = job.old_wal_path.clone();
            if let Err(e) = tx.send(WorkerJob::Promote(job)) {
                warn!("worker channel closed: {e}");
                return;
            }
            self.promotes_in_flight += 1;
            self.promote_gen += 1;
            self.inflight_old_wals.push_back(old_wal_path);
        }
    }

    /// Run the GC checkpoint prep and dispatch the promote to the worker.
    ///
    /// Mints ULIDs, opens the fresh WAL immediately (writes resume),
    /// and dispatches the GC promote.  If the WAL is empty, completes
    /// immediately.  The reply is parked until `PromoteComplete` for
    /// `u_flush` arrives so that `pending/<u_flush>` is on disk before
    /// the coordinator runs `gc_fork`.
    fn start_gc_checkpoint(&mut self, reply: Sender<io::Result<(Ulid, Ulid)>>) {
        let prep = match self.volume.prepare_gc_checkpoint() {
            Ok(prep) => prep,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        let GcCheckpointPrep {
            u_repack,
            u_sweep,
            u_flush,
            job,
        } = prep;

        if let Some(job) = job {
            // Dispatch to worker, park the reply.
            self.parked_gc = Some(ParkedGcCheckpoint {
                u_repack,
                u_sweep,
                u_flush,
                reply,
            });
            if let Some(tx) = &self.worker_tx {
                let old_wal_path = job.old_wal_path.clone();
                if let Err(e) = tx.send(WorkerJob::Promote(job)) {
                    warn!("worker channel closed during gc_checkpoint: {e}");
                    if let Some(parked) = self.parked_gc.take() {
                        let _ = parked.reply.send(Err(io::Error::other(
                            "worker channel closed during gc_checkpoint",
                        )));
                    }
                    return;
                }
                self.promotes_in_flight += 1;
                self.promote_gen += 1;
                self.inflight_old_wals.push_back(old_wal_path);
            }
        } else {
            // WAL was empty — fresh WAL already opened by prepare_gc_checkpoint.
            self.publish_snapshot();
            let _ = reply.send(Ok((u_repack, u_sweep)));
        }
    }

    /// Scan for staged GC handoffs and begin dispatching them to the worker.
    ///
    /// If `reply` is `Some`, the reply is sent when all handoffs have been
    /// applied (or immediately if there are none).  If `None` (idle tick),
    /// results are applied silently.
    fn start_gc_handoffs(&mut self, reply: Option<Sender<io::Result<usize>>>) {
        let (to_process, already_applied) = match self.volume.scan_staged_handoffs() {
            Ok(v) => v,
            Err(e) => {
                if let Some(reply) = reply {
                    let _ = reply.send(Err(e));
                } else {
                    warn!("gc handoff scan failed: {e}");
                }
                return;
            }
        };

        if to_process.is_empty() {
            if let Some(reply) = reply {
                let _ = reply.send(Ok(already_applied));
            }
            return;
        }

        let mut parked = ParkedGcHandoffs {
            remaining: to_process,
            reply,
            applied_count: already_applied,
        };

        self.dispatch_next_handoff(&mut parked);
        self.parked_handoffs = Some(parked);
    }

    /// Run the sweep prep on the actor and dispatch the heavy middle to
    /// the worker.  Reply is parked until [`crate::volume::SweepResult`]
    /// arrives and is applied.
    ///
    /// Reply is sent immediately with default stats when the prep
    /// returns `None` (no `pending/` segments to consider) or when the
    /// dispatch fails because the worker channel has closed.
    fn start_sweep(&mut self, reply: Sender<io::Result<CompactionStats>>) {
        let job = match self.volume.prepare_sweep() {
            Ok(Some(j)) => j,
            Ok(None) => {
                let _ = reply.send(Ok(CompactionStats::default()));
                return;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        if let Some(tx) = &self.worker_tx {
            if let Err(e) = tx.send(WorkerJob::Sweep(job)) {
                warn!("worker channel closed during sweep: {e}");
                let _ = reply.send(Err(io::Error::other("worker channel closed during sweep")));
                return;
            }
            self.parked_sweep = Some(reply);
        } else {
            let _ = reply.send(Err(io::Error::other("worker not running")));
        }
    }

    /// Run the repack prep on the actor and dispatch the heavy middle
    /// to the worker.  Reply is parked until
    /// [`crate::volume::RepackResult`] arrives and is applied.
    fn start_repack(&mut self, min_live_ratio: f64, reply: Sender<io::Result<CompactionStats>>) {
        let job = match self.volume.prepare_repack(min_live_ratio) {
            Ok(Some(j)) => j,
            Ok(None) => {
                let _ = reply.send(Ok(CompactionStats::default()));
                return;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        if let Some(tx) = &self.worker_tx {
            if let Err(e) = tx.send(WorkerJob::Repack(job)) {
                warn!("worker channel closed during repack: {e}");
                let _ = reply.send(Err(io::Error::other("worker channel closed during repack")));
                return;
            }
            self.parked_repack = Some(reply);
        } else {
            let _ = reply.send(Err(io::Error::other("worker not running")));
        }
    }

    /// Run the delta_repack prep on the actor and dispatch the heavy
    /// middle to the worker.  Reply is parked until
    /// [`crate::volume::DeltaRepackResult`] arrives and is applied.
    ///
    /// Reply is sent immediately with default stats when the prep
    /// returns `None` (no sealed snapshot) or when the dispatch fails
    /// because the worker channel has closed.
    fn start_delta_repack(&mut self, reply: Sender<io::Result<DeltaRepackStats>>) {
        let job = match self.volume.prepare_delta_repack() {
            Ok(Some(j)) => j,
            Ok(None) => {
                let _ = reply.send(Ok(DeltaRepackStats::default()));
                return;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        if let Some(tx) = &self.worker_tx {
            if let Err(e) = tx.send(WorkerJob::DeltaRepack(job)) {
                warn!("worker channel closed during delta_repack: {e}");
                let _ = reply.send(Err(io::Error::other(
                    "worker channel closed during delta_repack",
                )));
                return;
            }
            self.parked_delta_repack = Some(reply);
        } else {
            let _ = reply.send(Err(io::Error::other("worker not running")));
        }
    }

    /// Run the snapshot-manifest prep on the actor and dispatch the
    /// heavy middle (`index/` enumeration + signing + manifest/marker
    /// writes) to the worker.  Reply is parked until
    /// [`crate::volume::SignSnapshotManifestResult`] arrives and the
    /// `has_new_segments` flag is flipped on the actor.
    fn start_sign_snapshot_manifest(&mut self, snap_ulid: Ulid, reply: Sender<io::Result<()>>) {
        let job = self.volume.prepare_sign_snapshot_manifest(snap_ulid);
        if let Some(tx) = &self.worker_tx {
            if let Err(e) = tx.send(WorkerJob::SignSnapshotManifest(job)) {
                warn!("worker channel closed during sign_snapshot_manifest: {e}");
                let _ = reply.send(Err(io::Error::other(
                    "worker channel closed during sign_snapshot_manifest",
                )));
                return;
            }
            self.parked_sign_snapshot_manifest = Some(reply);
        } else {
            let _ = reply.send(Err(io::Error::other("worker not running")));
        }
    }

    /// Pop the next staged handoff from the parked batch and dispatch it.
    fn dispatch_next_handoff(&mut self, parked: &mut ParkedGcHandoffs) {
        if let Some((staged_path, new_ulid)) = parked.remaining.pop() {
            let job = self.volume.build_gc_handoff_job(staged_path, new_ulid);
            if let Some(tx) = &self.worker_tx {
                if let Err(e) = tx.send(WorkerJob::GcHandoff(job)) {
                    warn!("worker channel closed during gc handoff: {e}");
                    return;
                }
                self.handoff_in_flight = true;
            }
        }
    }

    /// Drain in-flight jobs and join the worker thread.
    ///
    /// Called on shutdown (explicit or handle-drop).  Drops the job sender
    /// to signal the worker to exit, then drains all pending results,
    /// applying successful ones so that the extent index is up to date
    /// before the volume is closed.
    fn shutdown_worker(&mut self) {
        // Drop the sender — worker's recv() will return Disconnected.
        self.worker_tx.take();

        // Drain remaining results (promotes and any in-flight handoff).
        while self.promotes_in_flight > 0
            || self.handoff_in_flight
            || self.promote_segments_in_flight > 0
            || self.parked_sweep.is_some()
            || self.parked_repack.is_some()
            || self.parked_delta_repack.is_some()
            || self.parked_sign_snapshot_manifest.is_some()
        {
            match self.worker_rx.recv() {
                Ok(WorkerResult::Promote(Ok(result))) => {
                    self.promotes_in_flight -= 1;
                    self.volume.apply_promote(&result);
                    self.publish_snapshot();
                    self.on_promote_success();
                }
                Ok(WorkerResult::Promote(Err(e))) => {
                    self.promotes_in_flight -= 1;
                    warn!("worker promote failed during shutdown: {e}");
                    self.on_promote_failure();
                }
                Ok(WorkerResult::GcHandoff(Ok(result))) => {
                    self.handoff_in_flight = false;
                    if let Ok(crate::volume::StagedApply::Applied) =
                        self.volume.apply_gc_handoff_result(&result)
                    {
                        self.publish_snapshot();
                    }
                }
                Ok(WorkerResult::GcHandoff(Err(e))) => {
                    self.handoff_in_flight = false;
                    warn!("worker gc handoff failed during shutdown: {e}");
                }
                Ok(WorkerResult::PromoteSegment { ulid, result }) => {
                    self.promote_segments_in_flight -= 1;
                    match result {
                        Ok(r) => {
                            let apply_result = self.volume.apply_promote_segment_result(r);
                            if apply_result.is_ok() {
                                self.publish_snapshot();
                            }
                            self.reply_parked_promote_segment(ulid, apply_result);
                        }
                        Err(e) => {
                            warn!("worker promote_segment for {ulid} failed during shutdown: {e}");
                            self.reply_parked_promote_segment(ulid, Err(e));
                        }
                    }
                }
                Ok(WorkerResult::Sweep(result)) => {
                    let reply = self.parked_sweep.take();
                    let outcome = match result {
                        Ok(r) => {
                            let apply_result = self.volume.apply_sweep_result(r);
                            if matches!(&apply_result, Ok(s) if s.segments_compacted > 0) {
                                self.publish_snapshot();
                            }
                            apply_result
                        }
                        Err(e) => {
                            warn!("worker sweep failed during shutdown: {e}");
                            Err(e)
                        }
                    };
                    if let Some(reply) = reply {
                        let _ = reply.send(outcome);
                    }
                }
                Ok(WorkerResult::Repack(result)) => {
                    let reply = self.parked_repack.take();
                    let outcome = match result {
                        Ok(r) => {
                            let apply_result = self.volume.apply_repack_result(r);
                            if matches!(&apply_result, Ok(s) if s.segments_compacted > 0) {
                                self.publish_snapshot();
                            }
                            apply_result
                        }
                        Err(e) => {
                            warn!("worker repack failed during shutdown: {e}");
                            Err(e)
                        }
                    };
                    if let Some(reply) = reply {
                        let _ = reply.send(outcome);
                    }
                }
                Ok(WorkerResult::DeltaRepack(result)) => {
                    let reply = self.parked_delta_repack.take();
                    let outcome = match result {
                        Ok(r) => {
                            let apply_result = self.volume.apply_delta_repack_result(r);
                            if matches!(&apply_result, Ok(s) if s.entries_converted > 0) {
                                self.publish_snapshot();
                            }
                            apply_result
                        }
                        Err(e) => {
                            warn!("worker delta_repack failed during shutdown: {e}");
                            Err(e)
                        }
                    };
                    if let Some(reply) = reply {
                        let _ = reply.send(outcome);
                    }
                }
                Ok(WorkerResult::SignSnapshotManifest(result)) => {
                    let reply = self.parked_sign_snapshot_manifest.take();
                    let outcome = match result {
                        Ok(r) => {
                            self.volume.apply_sign_snapshot_manifest_result(r);
                            Ok(())
                        }
                        Err(e) => {
                            warn!("worker sign_snapshot_manifest failed during shutdown: {e}");
                            Err(e)
                        }
                    };
                    if let Some(reply) = reply {
                        let _ = reply.send(outcome);
                    }
                }
                Err(_) => {
                    // Channel closed — worker exited unexpectedly.
                    break;
                }
            }
        }
        // Drop any remaining parked handoff state (remaining items
        // will be re-applied on next startup).
        self.parked_handoffs.take();

        // Join the worker thread.
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }

    pub fn run(mut self) {
        let idle_tick = tick(IDLE_FLUSH_INTERVAL);
        loop {
            crossbeam_channel::select! {
                recv(self.rx) -> msg => {
                    let req = match msg {
                        Ok(r) => r,
                        Err(_) => {
                            // All handles dropped — drain and exit.
                            self.shutdown_worker();
                            return;
                        }
                    };
                    match req {
                        VolumeRequest::Write { lba, data, reply } => {
                            let result = self.volume.write(lba, &data);
                            if result.is_ok() {
                                let (lbamap, extent_index) = self.volume.snapshot_maps();
                                self.snapshot.store(Arc::new(ReadSnapshot {
                                    lbamap,
                                    extent_index,
                                    flush_gen: self.flush_gen,
                                }));
                            }
                            let _ = reply.send(result);
                            if self.volume.needs_promote() {
                                self.dispatch_promote();
                            }
                        }
                        VolumeRequest::Flush { reply } => {
                            // Flush = WAL fsync + wait for any in-flight
                            // promote's old-WAL fsync to complete on the
                            // worker.  The actor stays on the select loop
                            // during the wait — new writes continue to flow
                            // onto the fresh WAL, matching how a real block
                            // device keeps accepting commands while a FLUSH
                            // is in flight at the controller.
                            match self.volume.wal_fsync() {
                                Ok(()) => self.park_or_resolve_flush(reply),
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                }
                            }
                        }
                        VolumeRequest::PromoteWal { reply } => {
                            // Promote the WAL to a pending/ segment via the
                            // worker.  Reply once the segment is on disk.
                            match self.volume.prepare_promote() {
                                Ok(Some(job)) => {
                                    let ulid = job.segment_ulid;
                                    let old_wal_path = job.old_wal_path.clone();
                                    if let Some(tx) = &self.worker_tx {
                                        if let Err(e) = tx.send(WorkerJob::Promote(job)) {
                                            let _ = reply.send(Err(io::Error::other(
                                                format!("worker channel closed: {e}"),
                                            )));
                                        } else {
                                            self.promotes_in_flight += 1;
                                            self.promote_gen += 1;
                                            self.inflight_old_wals.push_back(old_wal_path);
                                            self.parked_promote_wal.push(
                                                ParkedPromoteWal { segment_ulid: ulid, reply },
                                            );
                                        }
                                    }
                                }
                                Ok(None) => {
                                    // WAL empty — nothing to promote.
                                    let _ = reply.send(Ok(()));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                }
                            }
                        }
                        VolumeRequest::Trim {
                            start_lba,
                            lba_count,
                            reply,
                        } => {
                            let result = self.volume.write_zeroes(start_lba, lba_count);
                            if result.is_ok() {
                                let (lbamap, extent_index) = self.volume.snapshot_maps();
                                self.snapshot.store(Arc::new(ReadSnapshot {
                                    lbamap,
                                    extent_index,
                                    flush_gen: self.flush_gen,
                                }));
                            }
                            let _ = reply.send(result);
                            if self.volume.needs_promote() {
                                self.dispatch_promote();
                            }
                        }
                        VolumeRequest::SweepPending { reply } => {
                            if self.parked_sweep.is_some() {
                                let _ = reply
                                    .send(Err(io::Error::other("concurrent sweep_pending not allowed")));
                            } else {
                                self.start_sweep(reply);
                            }
                        }
                        VolumeRequest::Repack { min_live_ratio, reply } => {
                            if self.parked_repack.is_some() {
                                let _ = reply
                                    .send(Err(io::Error::other("concurrent repack not allowed")));
                            } else {
                                self.start_repack(min_live_ratio, reply);
                            }
                        }
                        VolumeRequest::DeltaRepackPostSnapshot { reply } => {
                            if self.parked_delta_repack.is_some() {
                                let _ = reply.send(Err(io::Error::other(
                                    "concurrent delta_repack not allowed",
                                )));
                            } else {
                                self.start_delta_repack(reply);
                            }
                        }
                        VolumeRequest::ApplyGcHandoffs { reply } => {
                            if self.parked_handoffs.is_some() {
                                let _ = reply.send(Err(io::Error::other(
                                    "concurrent apply_gc_handoffs not allowed",
                                )));
                            } else {
                                self.start_gc_handoffs(Some(reply));
                            }
                        }
                        VolumeRequest::GcCheckpoint { reply } => {
                            if self.parked_gc.is_some() {
                                // Concurrent GC checkpoint is an error.
                                let _ = reply.send(Err(io::Error::other(
                                    "concurrent gc_checkpoint not allowed",
                                )));
                            } else {
                                self.start_gc_checkpoint(reply);
                            }
                        }
                        VolumeRequest::RedactSegment { ulid, reply } => {
                            let _ = reply.send(self.volume.redact_segment(ulid));
                        }
                        VolumeRequest::Promote { ulid, reply } => {
                            // Prep on the actor: cheap directory stat +
                            // job build. Dispatch to worker, park reply.
                            match self.volume.prepare_promote_segment(ulid) {
                                Ok(PromoteSegmentPrep::AlreadyPromoted) => {
                                    let _ = reply.send(Ok(()));
                                }
                                Ok(PromoteSegmentPrep::Job(job)) => {
                                    if let Some(tx) = &self.worker_tx {
                                        match tx.send(WorkerJob::PromoteSegment(*job)) {
                                            Ok(()) => {
                                                self.promote_segments_in_flight += 1;
                                                self.parked_promote_segments.push(
                                                    ParkedPromoteSegment { ulid, reply },
                                                );
                                            }
                                            Err(e) => {
                                                let _ = reply.send(Err(io::Error::other(
                                                    format!("worker channel closed: {e}"),
                                                )));
                                            }
                                        }
                                    } else {
                                        let _ = reply.send(Err(io::Error::other(
                                            "worker channel closed",
                                        )));
                                    }
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                }
                            }
                        }
                        VolumeRequest::FinalizeGcHandoff { ulid, reply } => {
                            let _ = reply.send(self.volume.finalize_gc_handoff(ulid));
                        }
                        VolumeRequest::SignSnapshotManifest { snap_ulid, reply } => {
                            if self.parked_sign_snapshot_manifest.is_some() {
                                let _ = reply.send(Err(io::Error::other(
                                    "concurrent sign_snapshot_manifest not allowed",
                                )));
                            } else {
                                self.start_sign_snapshot_manifest(snap_ulid, reply);
                            }
                        }
                        VolumeRequest::NoopStats { reply } => {
                            let _ = reply.send(self.volume.noop_stats());
                        }
                        VolumeRequest::ReclaimSnapshot {
                            start_lba,
                            lba_length,
                            reply,
                        } => {
                            let plan = self.volume.reclaim_snapshot(start_lba, lba_length);
                            let _ = reply.send(Ok(plan));
                        }
                        VolumeRequest::ReclaimCommit {
                            plan,
                            proposed,
                            reply,
                        } => {
                            let result = self.volume.reclaim_commit(plan, proposed);
                            if matches!(&result, Ok(o) if !o.discarded && o.runs_rewritten > 0) {
                                self.publish_snapshot();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::Shutdown => {
                            self.shutdown_worker();
                            return;
                        }
                    }
                }
                // Worker thread results (promote completions, GC handoffs).
                recv(self.worker_rx) -> msg => {
                    match msg {
                        Ok(WorkerResult::Promote(Ok(result))) => {
                            self.promotes_in_flight -= 1;
                            let ulid = result.segment_ulid;
                            self.volume.apply_promote(&result);
                            self.publish_snapshot();
                            // Resolve parked NBD flushes only after apply + publish
                            // so the caller observes the old WAL deleted and the
                            // new snapshot visible — not just the durability barrier.
                            self.on_promote_success();

                            // Complete any parked operations waiting for this ULID.
                            // GC checkpoint.
                            let is_gc = self
                                .parked_gc
                                .as_ref()
                                .is_some_and(|p| ulid == p.u_flush);
                            if is_gc {
                                let parked = self.parked_gc.take().unwrap();
                                let _ =
                                    parked.reply.send(Ok((parked.u_repack, parked.u_sweep)));
                            }
                            // PromoteWal callers.
                            let mut i = 0;
                            while i < self.parked_promote_wal.len() {
                                if self.parked_promote_wal[i].segment_ulid == ulid {
                                    let parked = self.parked_promote_wal.swap_remove(i);
                                    let _ = parked.reply.send(Ok(()));
                                } else {
                                    i += 1;
                                }
                            }
                        }
                        Ok(WorkerResult::Promote(Err(e))) => {
                            self.promotes_in_flight -= 1;
                            warn!("worker promote failed: {e}");
                            self.on_promote_failure();
                        }
                        Ok(WorkerResult::GcHandoff(Ok(result))) => {
                            self.handoff_in_flight = false;
                            match self.volume.apply_gc_handoff_result(&result) {
                                Ok(crate::volume::StagedApply::Applied) => {
                                    self.publish_snapshot();
                                    if let Some(ref mut parked) = self.parked_handoffs {
                                        parked.applied_count += 1;
                                    }
                                }
                                Ok(crate::volume::StagedApply::Cancelled) => {
                                    // Stale-liveness cancel — logged inside
                                    // apply_gc_handoff_result, continue to next.
                                }
                                Err(e) => {
                                    warn!("gc handoff apply failed: {e}");
                                    if let Some(parked) = self.parked_handoffs.take()
                                        && let Some(reply) = parked.reply
                                    {
                                        let _ = reply.send(Err(e));
                                    }
                                }
                            }
                            // Dispatch next handoff or complete the batch.
                            if let Some(mut parked) = self.parked_handoffs.take() {
                                if parked.remaining.is_empty() {
                                    if let Some(reply) = parked.reply {
                                        let _ = reply.send(Ok(parked.applied_count));
                                    }
                                } else {
                                    self.dispatch_next_handoff(&mut parked);
                                    self.parked_handoffs = Some(parked);
                                }
                            }
                        }
                        Ok(WorkerResult::GcHandoff(Err(e))) => {
                            self.handoff_in_flight = false;
                            warn!("worker gc handoff failed: {e}");
                            if let Some(parked) = self.parked_handoffs.take()
                                && let Some(reply) = parked.reply
                            {
                                let _ = reply.send(Err(e));
                            }
                        }
                        Ok(WorkerResult::PromoteSegment { ulid, result }) => {
                            self.promote_segments_in_flight -= 1;
                            match result {
                                Ok(r) => {
                                    let apply_result =
                                        self.volume.apply_promote_segment_result(r);
                                    if apply_result.is_ok() {
                                        self.publish_snapshot();
                                    }
                                    self.reply_parked_promote_segment(ulid, apply_result);
                                }
                                Err(e) => {
                                    warn!(
                                        "worker promote_segment for {ulid} failed: {e}"
                                    );
                                    self.reply_parked_promote_segment(ulid, Err(e));
                                }
                            }
                        }
                        Ok(WorkerResult::Sweep(result)) => {
                            let reply = self.parked_sweep.take();
                            let outcome = match result {
                                Ok(r) => {
                                    let apply_result = self.volume.apply_sweep_result(r);
                                    if matches!(&apply_result, Ok(s) if s.segments_compacted > 0) {
                                        self.publish_snapshot();
                                    }
                                    apply_result
                                }
                                Err(e) => {
                                    warn!("worker sweep failed: {e}");
                                    Err(e)
                                }
                            };
                            if let Some(reply) = reply {
                                let _ = reply.send(outcome);
                            }
                        }
                        Ok(WorkerResult::Repack(result)) => {
                            let reply = self.parked_repack.take();
                            let outcome = match result {
                                Ok(r) => {
                                    let apply_result = self.volume.apply_repack_result(r);
                                    if matches!(&apply_result, Ok(s) if s.segments_compacted > 0) {
                                        self.publish_snapshot();
                                    }
                                    apply_result
                                }
                                Err(e) => {
                                    warn!("worker repack failed: {e}");
                                    Err(e)
                                }
                            };
                            if let Some(reply) = reply {
                                let _ = reply.send(outcome);
                            }
                        }
                        Ok(WorkerResult::DeltaRepack(result)) => {
                            let reply = self.parked_delta_repack.take();
                            let outcome = match result {
                                Ok(r) => {
                                    let apply_result = self.volume.apply_delta_repack_result(r);
                                    if matches!(&apply_result, Ok(s) if s.entries_converted > 0) {
                                        self.publish_snapshot();
                                    }
                                    apply_result
                                }
                                Err(e) => {
                                    warn!("worker delta_repack failed: {e}");
                                    Err(e)
                                }
                            };
                            if let Some(reply) = reply {
                                let _ = reply.send(outcome);
                            }
                        }
                        Ok(WorkerResult::SignSnapshotManifest(result)) => {
                            let reply = self.parked_sign_snapshot_manifest.take();
                            let outcome = match result {
                                Ok(r) => {
                                    self.volume.apply_sign_snapshot_manifest_result(r);
                                    Ok(())
                                }
                                Err(e) => {
                                    warn!("worker sign_snapshot_manifest failed: {e}");
                                    Err(e)
                                }
                            };
                            if let Some(reply) = reply {
                                let _ = reply.send(outcome);
                            }
                        }
                        Err(_) => {
                            warn!("worker result channel closed unexpectedly");
                        }
                    }
                }
                recv(idle_tick) -> _ => {
                    // Dispatch a promote if the WAL has unflushed data.
                    // prepare_promote handles the empty-WAL case internally.
                    self.dispatch_promote();
                    // Scan for GC handoff files and dispatch if not already
                    // processing a batch.
                    if self.parked_handoffs.is_none() {
                        self.start_gc_handoffs(None);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// Shareable client handle for a volume session.
///
/// `Clone + Send`.  Each clone gets its own empty file-handle cache so that
/// concurrent readers (e.g. separate ublk queue threads) never share file
/// descriptors.  The channel sender and snapshot store are shared across all
/// clones via `Arc`.
pub struct VolumeHandle {
    tx: Sender<VolumeRequest>,
    snapshot: Arc<ArcSwap<ReadSnapshot>>,
    config: Arc<VolumeConfig>,
    /// Per-handle LRU cache of open segment file handles.  Never contended:
    /// each ublk queue thread holds its own clone.  `RefCell` is sufficient;
    /// `Mutex` is not needed.
    file_cache: RefCell<FileCache>,
    /// Generation of the last snapshot whose extent index offsets were used to
    /// populate `file_cache`.  Compared against `ReadSnapshot::flush_gen` on
    /// every read; if they differ the cache is evicted before proceeding.
    /// Reading both the generation and the extent index from the same snapshot
    /// load means the two are always in sync — no separate atomic needed.
    last_flush_gen: Cell<u64>,
}

// VolumeHandle is Send: all fields are Send and file_cache is only accessed
// from the owning thread (each clone is intended for one thread).
// It is not Sync: RefCell is not Sync, and handles are not meant to be shared
// across threads — clone instead.
unsafe impl Send for VolumeHandle {}

impl Clone for VolumeHandle {
    fn clone(&self) -> Self {
        let current_gen = self.snapshot.load().flush_gen;
        Self {
            tx: self.tx.clone(),
            snapshot: Arc::clone(&self.snapshot),
            config: Arc::clone(&self.config),
            file_cache: RefCell::new(FileCache::default()), // fresh cache per clone/thread
            last_flush_gen: Cell::new(current_gen),
        }
    }
}

impl VolumeHandle {
    /// Write `data` at `lba` via the actor.  Blocks until the actor replies.
    pub fn write(&self, lba: u64, data: Vec<u8>) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::Write {
                lba,
                data,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Zero `lba_count` blocks starting at `lba`.  Blocks until the actor replies.
    ///
    /// Writes a single zero-extent WAL record — no hashing, no data payload.
    /// See [`Volume::write_zeroes`] for details.
    pub fn write_zeroes(&self, start_lba: u64, lba_count: u32) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::Trim {
                start_lba,
                lba_count,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Trim (discard) `lba_count` blocks starting at `lba`.  Blocks until the actor replies.
    pub fn trim(&self, start_lba: u64, lba_count: u32) -> io::Result<()> {
        self.write_zeroes(start_lba, lba_count)
    }

    /// Fetch the current no-op write skip counters from the actor.
    /// Blocks until the actor replies. See [`NoopSkipStats`].
    pub fn noop_stats(&self) -> io::Result<NoopSkipStats> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::NoopStats { reply: reply_tx })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))
    }

    /// Fsync the WAL.  Durability barrier — data survives a crash after
    /// this returns.  Does not promote the WAL to a segment.
    pub fn flush(&self) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::Flush { reply: reply_tx })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Promote the WAL to a `pending/` segment.  Blocks until the segment
    /// is on disk.  No-op if the WAL is empty.
    pub fn promote_wal(&self) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::PromoteWal { reply: reply_tx })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Sweep pending segments.  Blocks until the actor replies.
    pub fn sweep_pending(&self) -> io::Result<CompactionStats> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::SweepPending { reply: reply_tx })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Repack sparse pending segments below `min_live_ratio`.  Blocks until
    /// the actor replies.
    pub fn repack(&self, min_live_ratio: f64) -> io::Result<CompactionStats> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::Repack {
                min_live_ratio,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Rewrite post-snapshot pending segments with zstd-dictionary deltas
    /// against same-LBA extents from the latest sealed snapshot.  Blocks
    /// until the actor replies.
    pub fn delta_repack_post_snapshot(&self) -> io::Result<DeltaRepackStats> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::DeltaRepackPostSnapshot { reply: reply_tx })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Read `lba_count` blocks starting at `lba`.
    ///
    /// Resolved entirely on the calling thread using the current `ReadSnapshot`
    /// — no channel round-trip.  Reflects all writes that have returned `Ok`,
    /// including those not yet flushed to disk (read-your-writes guarantee).
    pub fn read(&self, lba: u64, lba_count: u32) -> io::Result<Vec<u8>> {
        // Load the snapshot first.  flush_gen is embedded in the snapshot so
        // the generation and the extent index offsets are always consistent —
        // a single ArcSwap::load() gives both atomically with no window.
        let snap = self.snapshot.load();
        if snap.flush_gen != self.last_flush_gen.get() {
            self.file_cache.borrow_mut().clear();
            self.last_flush_gen.set(snap.flush_gen);
        }
        read_extents(
            lba,
            lba_count,
            &snap.lbamap,
            &snap.extent_index,
            &self.file_cache,
            |id, bss, idx| {
                find_segment_in_dirs(
                    id,
                    &self.config.base_dir,
                    &self.config.ancestor_layers,
                    self.config.fetcher.as_ref(),
                    bss,
                    idx,
                )
            },
            |id| {
                open_delta_body_in_dirs(
                    id,
                    &self.config.base_dir,
                    &self.config.ancestor_layers,
                    self.config.fetcher.as_ref(),
                )
            },
        )
    }

    /// Apply any pending GC handoff files via the actor.  Blocks until the
    /// actor replies.  The actor republishes the snapshot if any handoffs were
    /// applied so that reads immediately reflect the updated extent index.
    pub fn apply_gc_handoffs(&self) -> io::Result<usize> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::ApplyGcHandoffs { reply: reply_tx })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Establish a GC checkpoint: flush the WAL and return two fresh ULIDs for
    /// GC output segments (repack and sweep).  The two ULIDs are strictly
    /// ordered and sort before the fresh WAL's ULID.  Blocks until the
    /// actor replies.
    pub fn gc_checkpoint(&self) -> io::Result<(Ulid, Ulid)> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::GcCheckpoint { reply: reply_tx })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Redact a pending segment: hole-punch hash-dead DATA entries in place
    /// so deleted data never leaves the host via S3 upload.
    ///
    /// Called by the coordinator before reading `pending/<ulid>` for S3
    /// upload. Idempotent; no-op when the segment has no hash-dead entries.
    pub fn redact_segment(&self, ulid: Ulid) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::RedactSegment {
                ulid,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Promote a segment to the local cache after confirmed S3 upload.
    ///
    /// Sends a `promote <ulid>` request to the actor and blocks until it replies.
    pub fn promote_segment(&self, ulid: Ulid) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::Promote {
                ulid,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Finalize a GC handoff by renaming `gc/<ulid>.applied` → `.done`
    /// via the actor.  Routing the rename through the actor keeps every
    /// mutation of `gc/` serialised with the idle-tick apply path, so the
    /// coordinator never races the volume on `gc/` filenames.  Blocks until
    /// the actor replies.
    pub fn finalize_gc_handoff(&self, ulid: Ulid) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::FinalizeGcHandoff {
                ulid,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Sign and write a `snapshots/<snap_ulid>.manifest` file plus the
    /// marker file. Called by the coordinator after a synchronous drain has
    /// moved every in-flight segment from `pending/` to `index/`.
    ///
    /// The volume enumerates its own `index/` at handler time — no prior
    /// snapshot is read. The result is a full list of segment ULIDs
    /// belonging to this volume as of the snapshot.
    pub fn sign_snapshot_manifest(&self, snap_ulid: Ulid) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::SignSnapshotManifest {
                snap_ulid,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Signal the actor to shut down and drain remaining requests.
    pub fn shutdown(&self) {
        let _ = self.tx.send(VolumeRequest::Shutdown);
    }

    /// Scan the current LBA map + extent index for hashes worth rewriting.
    ///
    /// Read-only. Runs entirely against the current `ReadSnapshot` with
    /// no actor round-trip and no file I/O. Returned candidates are
    /// sorted by dead-block count descending — feed them to
    /// [`VolumeHandle::reclaim_alias_merge`] in order for
    /// "most-wasteful-first" reclamation.
    ///
    /// See [`scan_reclaim_candidates`] for the detection logic.
    pub fn reclaim_candidates(&self, thresholds: ReclaimThresholds) -> Vec<ReclaimCandidate> {
        let snap = self.snapshot.load();
        scan_reclaim_candidates(&snap.lbamap, &snap.extent_index, thresholds)
    }

    /// Alias-merge extent reclamation over `[lba, lba + lba_length)`.
    ///
    /// This is the volume-side primitive that rewrites aliased runs of a
    /// single hash inside the target range as fresh compact entries,
    /// leaving the old bloated body orphaned for coordinator GC to
    /// eventually drop. Preserves content boundaries — never merges
    /// across different hashes. Safe on any volume.
    ///
    /// Three phases bracket the heavy work with two short actor messages:
    /// 1. `ReclaimSnapshot` — actor thread, microseconds. Captures an
    ///    `Arc<LbaMap>` clone + the clipped in-range entries.
    /// 2. Phase 2 — **this thread**, millisecond-scale. Walks the plan
    ///    off-actor, reads live bytes via `VolumeHandle::read`, hashes
    ///    each contiguous same-hash run, emits rewrite proposals.
    /// 3. `ReclaimCommit` — actor thread, microseconds. Pointer-equality
    ///    precondition check; on success commits each proposal through
    ///    `Volume::write_with_hash` (the noop-skip hash check makes
    ///    re-running reclamation over an already-merged range idempotent).
    ///    On mismatch returns a clean discard.
    ///
    /// Heavy work between phases never holds any lock and does not block
    /// writes queued behind it on the actor channel. A concurrent mutation
    /// causes a discard — not a retry here — and the caller is free to
    /// try again later.
    ///
    /// See `docs/design-extent-reclamation.md`.
    pub fn reclaim_alias_merge(&self, lba: u64, lba_length: u32) -> io::Result<ReclaimOutcome> {
        let plan = self.reclaim_snapshot(lba, lba_length)?;
        let proposed = plan.compute_rewrites(|start, len| self.read(start, len))?;
        if proposed.is_empty() {
            return Ok(ReclaimOutcome::default());
        }
        self.reclaim_commit(plan, proposed)
    }

    fn reclaim_snapshot(&self, lba: u64, lba_length: u32) -> io::Result<ReclaimPlan> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::ReclaimSnapshot {
                start_lba: lba,
                lba_length,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    fn reclaim_commit(
        &self,
        plan: ReclaimPlan,
        proposed: Vec<ReclaimProposed>,
    ) -> io::Result<ReclaimOutcome> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::ReclaimCommit {
                plan,
                proposed,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }
}

// ---------------------------------------------------------------------------
// Worker thread
// ---------------------------------------------------------------------------

/// Long-lived worker thread that processes off-actor jobs (WAL promotes,
/// GC handoff re-signs, etc.).
///
/// Receives jobs via `job_rx`, executes each, and sends the result back on
/// `result_tx`.  Exits when `job_rx` disconnects (actor dropped the sender)
/// or `result_tx` disconnects (actor gone).
fn worker_thread(job_rx: Receiver<WorkerJob>, result_tx: Sender<WorkerResult>) {
    while let Ok(job) = job_rx.recv() {
        let msg = match job {
            WorkerJob::Promote(job) => WorkerResult::Promote(execute_promote(job)),
            WorkerJob::GcHandoff(job) => WorkerResult::GcHandoff(execute_gc_handoff(job)),
            WorkerJob::PromoteSegment(job) => {
                let ulid = job.ulid;
                let result = execute_promote_segment(job);
                WorkerResult::PromoteSegment { ulid, result }
            }
            WorkerJob::Sweep(job) => WorkerResult::Sweep(execute_sweep(job)),
            WorkerJob::Repack(job) => WorkerResult::Repack(execute_repack(job)),
            WorkerJob::DeltaRepack(job) => WorkerResult::DeltaRepack(execute_delta_repack(job)),
            WorkerJob::SignSnapshotManifest(job) => {
                WorkerResult::SignSnapshotManifest(execute_sign_snapshot_manifest(job))
            }
        };
        if result_tx.send(msg).is_err() {
            break;
        }
    }
}

/// Execute a WAL promote job: fsync the old WAL, then write the
/// segment to `pending/`.
///
/// The old-WAL fsync is the durability barrier that `prepare_promote`
/// used to run on the actor thread.  Moving it here off-loads the
/// 10–50 ms fsync cost from the write path: the actor keeps taking
/// new writes onto the fresh WAL while the worker makes the old one
/// durable in parallel — matching the way a real block device keeps
/// accepting commands while a FLUSH is in flight.  `VolumeActor::Flush`
/// parks on a promote-generation counter so NBD FLUSH still replies
/// only after every prior write is durable.
fn execute_promote(mut job: PromoteJob) -> io::Result<PromoteResult> {
    std::fs::File::open(&job.old_wal_path)?.sync_data()?;

    let body_section_start = segment::write_and_commit(
        &job.pending_dir,
        job.segment_ulid,
        &mut job.entries,
        job.signer.as_ref(),
    )?;
    Ok(PromoteResult {
        segment_ulid: job.segment_ulid,
        old_wal_ulid: job.old_wal_ulid,
        old_wal_path: job.old_wal_path,
        body_section_start,
        entries: job.entries,
        pre_promote_offsets: job.pre_promote_offsets,
    })
}

/// Execute a `promote_segment` job: read + verify the source segment
/// index once, write `index/<ulid>.idx` + `cache/<ulid>.{body,present}`
/// (both idempotent), and return the parsed state the actor's apply
/// phase needs for extent-index updates.
///
/// Also reachable from the inline (on-actor) `Volume::promote_segment`
/// path so that the two execution sites share one parse/verify pass.
pub(crate) fn execute_promote_segment(job: PromoteSegmentJob) -> io::Result<PromoteSegmentResult> {
    let parsed = job
        .segment_cache
        .read_and_verify(&job.src_path, &job.verifying_key)?;
    let bss = parsed.body_section_start;

    // Tombstone shortcut: GC output with zero entries + non-empty inputs
    // exists only to acknowledge that the input segments are safe to
    // delete. No idx or body is written; the apply phase handles the
    // input-idx cleanup.
    if !job.is_drain && parsed.entries.is_empty() && !parsed.inputs.is_empty() {
        return Ok(PromoteSegmentResult {
            ulid: job.ulid,
            is_drain: job.is_drain,
            body_section_start: bss,
            entries: parsed.entries.clone(),
            inputs: parsed.inputs.clone(),
            inline: Vec::new(),
            tombstone: true,
        });
    }

    // Both writes are idempotent: extract_idx early-returns when idx_path
    // exists; promote_to_cache early-returns when body_path exists. This
    // covers the mid-apply crash retry window described in
    // docs/promote-segment-offload-plan.md — the source survives, prep
    // picks it up, the worker re-parses (cheap) and the file writes
    // short-circuit.
    segment::extract_idx(&job.src_path, &job.idx_path)?;
    segment::promote_to_cache(&job.src_path, &job.body_path, &job.present_path)?;

    // Inline section is only needed by the drain-path apply to build
    // `inline_data` for `BodySource::Cached` entries whose kind is
    // `Inline`. The GC apply phase never touches the extent index so
    // the read would be wasted there.
    let inline = if job.is_drain
        && parsed
            .entries
            .iter()
            .any(|e| e.kind == segment::EntryKind::Inline)
    {
        segment::read_inline_section(&job.src_path)?
    } else {
        Vec::new()
    };

    Ok(PromoteSegmentResult {
        ulid: job.ulid,
        is_drain: job.is_drain,
        body_section_start: bss,
        entries: parsed.entries.clone(),
        inputs: parsed.inputs.clone(),
        inline,
        tombstone: false,
    })
}

/// Execute a GC handoff job: read the staged segment, read input `.idx`
/// files, re-sign with the volume key, and write to `gc/<ulid>.tmp`.
fn execute_gc_handoff(job: GcHandoffJob) -> io::Result<GcHandoffResult> {
    // 1. Read staged segment.
    let (bss, mut entries, inputs) = segment::read_segment_index(&job.staged_path)?;
    if inputs.is_empty() {
        return Err(io::Error::other(
            "gc staged file has no inputs; not a GC output",
        ));
    }

    // 2. Collect body-owning entries from each input's .idx file.
    let mut input_old_entries = Vec::new();
    for input_ulid in &inputs {
        let idx_path = job.index_dir.join(format!("{input_ulid}.idx"));
        let parsed = match segment::read_segment_index(&idx_path) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let (_, old_entries, _) = parsed;
        for e in &old_entries {
            if matches!(
                e.kind,
                segment::EntryKind::Data | segment::EntryKind::Inline
            ) {
                input_old_entries.push((e.hash, e.kind, *input_ulid));
            }
        }
    }

    // 3. Read inline + body data from the staged segment.
    let handoff_inline = segment::read_inline_section(&job.staged_path)?;
    segment::read_extent_bodies(
        &job.staged_path,
        bss,
        &mut entries,
        [
            segment::EntryKind::Data,
            segment::EntryKind::DedupRef,
            segment::EntryKind::Inline,
        ],
        &handoff_inline,
    )?;

    // 4. Re-sign and write gc/<ulid>.tmp.
    let tmp_path = job.gc_dir.join(format!("{}.tmp", job.new_ulid));
    let new_bss = segment::write_gc_segment(&tmp_path, &mut entries, &inputs, job.signer.as_ref())?;

    Ok(GcHandoffResult {
        new_ulid: job.new_ulid,
        staged_path: job.staged_path,
        gc_dir: job.gc_dir,
        new_bss,
        entries,
        inputs,
        input_old_entries,
        handoff_inline,
    })
}

/// Target output segment size for sweep, in live bytes. Matches the WAL
/// `FLUSH_THRESHOLD` so swept outputs sit at the same scale as
/// freshly-flushed segments.
const SWEEP_TARGET_LIVE: u64 = 32 * 1024 * 1024;

/// Live-bytes threshold at or below which a pending segment is treated
/// as a "small" sweep candidate. Picked at half the target so two smalls
/// always combine to fit and the merged output exits the small set
/// permanently — no infinite re-pack loop.
const SWEEP_SMALL_THRESHOLD: u64 = SWEEP_TARGET_LIVE / 2;

/// Per-segment scratch state for sweep candidate selection.
///
/// `live_part` and `dead_part` are computed in the cheap scan phase
/// using the segment-index cache; the inline section and body bytes
/// are only fetched in the second phase, after selection has chosen
/// which candidates actually contribute to the merged output.
struct SweepCandidate {
    seg_path: PathBuf,
    seg_ulid: Ulid,
    body_section_start: u64,
    live_part: Vec<segment::SegmentEntry>,
    dead_part: Vec<segment::SegmentEntry>,
    /// Live `Data + Inline` body bytes — the budget unit.  `DedupRef`
    /// and `Zero` are thin entries with no body cost.
    live_bytes: u64,
    dead_bytes: u64,
}

/// Execute a sweep job: scan every `pending/` segment for liveness,
/// pack a bucket of candidates up to [`SWEEP_TARGET_LIVE`] live bytes,
/// and merge their live extents into a single new segment named with
/// the max input ULID.
///
/// Pure with respect to in-memory `Volume` state — only touches the
/// filesystem and the snapshot maps in `job`. The CAS preconditions
/// returned in [`SweepResult::merged_live`] / [`SweepResult::dead_entries`]
/// are the source `(segment_id, body_offset)` pairs, captured before
/// `write_segment` reassigns offsets, so the apply phase can safely
/// rewrite the extent index under concurrent writes.
///
/// **Selection is purely about packing, not dead-data removal.** A
/// segment with mostly-dead but large live bytes is *not* a sweep
/// candidate — that's `Volume::repack`'s job, gated on density.
/// Sweep clears any dead entries within its selected inputs as a
/// side-effect of the rewrite, but does not select on that signal.
pub(crate) fn execute_sweep(job: SweepJob) -> io::Result<SweepResult> {
    let seg_paths = segment::collect_segment_files(&job.pending_dir)?;
    let live: std::collections::HashSet<blake3::Hash> = job.lbamap.lba_referenced_hashes();

    // Phase 1 — scan: parse + verify every non-floor segment, compute
    // (live_part, dead_part, live_bytes). The segment cache makes the
    // parse cheap on the second visit (apply phase doesn't re-parse).
    let mut all: Vec<SweepCandidate> = Vec::new();
    for seg_path in &seg_paths {
        let seg_filename = seg_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| io::Error::other("bad segment filename"))?;
        let seg_ulid =
            Ulid::from_string(seg_filename).map_err(|e| io::Error::other(e.to_string()))?;
        if job.floor.is_some_and(|f| seg_ulid <= f) {
            continue;
        }

        let parsed = job
            .segment_cache
            .read_and_verify(seg_path, &job.verifying_key)?;
        let body_section_start = parsed.body_section_start;
        let mut entries = parsed.entries.clone();
        let (live_part, dead_part): (Vec<_>, Vec<_>) =
            entries.drain(..).partition(|e| match e.kind {
                segment::EntryKind::DedupRef => {
                    // A dedup ref is only live if the LBA still maps to
                    // this hash. If the LBA was overwritten with different
                    // data, carrying the stale ref would reintroduce the
                    // old mapping after crash + rebuild.
                    job.lbamap.hash_at(e.start_lba) == Some(e.hash)
                }
                _ => live.contains(&e.hash),
            });
        let live_bytes: u64 = live_part
            .iter()
            .filter(|e| {
                matches!(
                    e.kind,
                    segment::EntryKind::Data | segment::EntryKind::Inline
                )
            })
            .map(|e| e.stored_length as u64)
            .sum();
        let dead_bytes: u64 = dead_part.iter().map(|e| e.stored_length as u64).sum();
        all.push(SweepCandidate {
            seg_path: seg_path.clone(),
            seg_ulid,
            body_section_start,
            live_part,
            dead_part,
            live_bytes,
            dead_bytes,
        });
    }

    // Phase 2 — select: bin-pack toward SWEEP_TARGET_LIVE.
    //
    // Tier 1: smalls (live_bytes ≤ SMALL_THRESHOLD), sorted ascending so
    // we fit the maximum count of inputs into the budget — every
    // included input reduces the segment count of the next pass.
    //
    // Tier 2: at most one filler (live_bytes > SMALL_THRESHOLD) chosen
    // best-fit against the remaining headroom. The filler trades
    // copy-cost (rewriting a large segment) for one-fewer total
    // segments, which is only worth it when there's already a small
    // bucket to attach to.
    let (smalls, fillers): (Vec<_>, Vec<_>) = all
        .into_iter()
        .partition(|c| c.live_bytes <= SWEEP_SMALL_THRESHOLD);

    let mut smalls = smalls;
    smalls.sort_by_key(|c| c.live_bytes);

    let mut bucket: Vec<SweepCandidate> = Vec::new();
    let mut budget = SWEEP_TARGET_LIVE;
    for c in smalls {
        if c.live_bytes <= budget {
            budget -= c.live_bytes;
            bucket.push(c);
        }
    }
    if !bucket.is_empty()
        && budget > 0
        && let Some(pos) = fillers
            .iter()
            .enumerate()
            .filter(|(_, c)| c.live_bytes <= budget)
            .max_by_key(|(_, c)| c.live_bytes)
            .map(|(i, _)| i)
    {
        let mut fillers = fillers;
        let f = fillers.remove(pos);
        bucket.push(f);
    }

    // Single-input sweep is a no-op rewrite — skip. Repack handles
    // single-segment dead-data cleanup.
    if bucket.len() < 2 {
        return Ok(SweepResult {
            stats: CompactionStats::default(),
            new_ulid: None,
            new_body_section_start: 0,
            merged_live: Vec::new(),
            dead_entries: Vec::new(),
            candidate_paths: Vec::new(),
        });
    }

    // Sort selected candidates by ULID ascending so entries land in the
    // merged output in write order — rebuild applies entries in sequence
    // and the last entry wins for each LBA.
    bucket.sort_by_key(|c| c.seg_ulid);

    // Phase 3 — fetch and merge.
    let mut stats = CompactionStats::default();
    let mut merged_live: Vec<SweptLiveEntry> = Vec::new();
    let mut dead_entries: Vec<SweptDeadEntry> = Vec::new();
    let mut candidate_paths: Vec<PathBuf> = Vec::new();

    for c in &mut bucket {
        for entry in &c.dead_part {
            // Zero/DedupRef are thin entries with no extent-index slot.
            // Delta entries live in the deltas table, not `inner`, so
            // `remove_if_matches` would always miss — skipping keeps the
            // dead set focused on entries the apply phase can actually
            // remove.
            if matches!(
                entry.kind,
                segment::EntryKind::Zero | segment::EntryKind::DedupRef | segment::EntryKind::Delta
            ) {
                continue;
            }
            dead_entries.push(SweptDeadEntry {
                hash: entry.hash,
                source_segment_id: c.seg_ulid,
                source_body_offset: entry.stored_offset,
            });
        }
        let inline_bytes = segment::read_inline_section(&c.seg_path)?;
        segment::read_extent_bodies(
            &c.seg_path,
            c.body_section_start,
            &mut c.live_part,
            [segment::EntryKind::Data, segment::EntryKind::Inline],
            &inline_bytes,
        )?;
        // Verify each body matches its declared hash before it's carried
        // into the merged output. See apply_staged_handoff for background.
        for entry in &c.live_part {
            if let Some(body) = entry.data.as_deref() {
                segment::verify_body_hash(entry, body).map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!("sweep: input segment {}: {e}", c.seg_ulid),
                    )
                })?;
            }
        }
        for entry in std::mem::take(&mut c.live_part) {
            let source_body_offset = entry.stored_offset;
            merged_live.push(SweptLiveEntry {
                entry,
                source_segment_id: c.seg_ulid,
                source_body_offset,
            });
        }
        candidate_paths.push(c.seg_path.clone());
        stats.segments_compacted += 1;
        stats.bytes_freed += c.dead_bytes;
    }

    // Use max(candidate ULIDs) as the output ULID. This guarantees the
    // output sorts below the current WAL ULID (all pending segments were
    // created before the WAL was opened), so a subsequent WAL flush
    // always produces a higher ULID and wins on rebuild. Using
    // mint.next() here would generate a ULID past the WAL ULID and
    // break that ordering.
    let new_ulid = bucket
        .iter()
        .map(|c| c.seg_ulid)
        .max()
        .ok_or_else(|| io::Error::other("sweep: no valid candidate ULIDs"))?;

    let mut new_body_section_start = 0u64;
    let new_ulid_out = if merged_live.is_empty() {
        // Every input was fully dead — no segment to write, but the
        // dead-removal + candidate deletion still need to run.
        None
    } else {
        // write_segment reassigns each entry's stored_offset to its
        // new position. Operate on a borrowed buffer of SegmentEntry,
        // then read the new offsets back into our SweptLiveEntry list.
        let mut entries: Vec<segment::SegmentEntry> =
            merged_live.iter().map(|e| e.entry.clone()).collect();
        let new_ulid_str = new_ulid.to_string();
        let tmp_path = job.pending_dir.join(format!("{new_ulid_str}.tmp"));
        let final_path = job.pending_dir.join(&new_ulid_str);
        new_body_section_start =
            segment::write_segment(&tmp_path, &mut entries, job.signer.as_ref())?;
        std::fs::rename(&tmp_path, &final_path)?;
        segment::fsync_dir(&final_path)?;
        stats.new_segments += 1;
        // Replace the cloned entry on each SweptLiveEntry with the
        // post-write copy so the apply phase sees the new offsets.
        for (sw, written) in merged_live.iter_mut().zip(entries.into_iter()) {
            sw.entry = written;
        }
        Some(new_ulid)
    };

    Ok(SweepResult {
        stats,
        new_ulid: new_ulid_out,
        new_body_section_start,
        merged_live,
        dead_entries,
        candidate_paths,
    })
}

/// Execute a repack job: iterate every non-floor segment in `pending/`,
/// compute liveness against the captured `lbamap`, and rewrite (in place,
/// reusing the input ULID) any segment whose live ratio is below
/// `min_live_ratio`. Segments with no live entries are deleted.
///
/// Produces a per-segment `RepackedSegment` with CAS preconditions for
/// every Data/Inline entry touched — the apply phase on the actor uses
/// those to update the extent index without clobbering concurrent writes.
pub(crate) fn execute_repack(job: RepackJob) -> io::Result<RepackResult> {
    let seg_paths = segment::collect_segment_files(&job.pending_dir)?;
    let live: std::collections::HashSet<blake3::Hash> = job.lbamap.lba_referenced_hashes();

    let mut stats = CompactionStats::default();
    let mut segments: Vec<RepackedSegment> = Vec::new();

    for seg_path in &seg_paths {
        let seg_filename = seg_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| io::Error::other("bad segment filename"))?;
        let seg_id =
            Ulid::from_string(seg_filename).map_err(|e| io::Error::other(e.to_string()))?;
        if job.floor.is_some_and(|f| seg_id <= f) {
            continue;
        }

        let parsed = match job
            .segment_cache
            .read_and_verify(seg_path, &job.verifying_key)
        {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let body_section_start = parsed.body_section_start;
        let mut entries = parsed.entries.clone();

        let total_bytes: u64 = entries
            .iter()
            .filter(|e| {
                matches!(
                    e.kind,
                    segment::EntryKind::Data
                        | segment::EntryKind::Inline
                        | segment::EntryKind::CanonicalData
                        | segment::EntryKind::CanonicalInline
                )
            })
            .map(|e| e.stored_length as u64)
            .sum();

        if total_bytes == 0 {
            continue;
        }

        let live_bytes: u64 = entries
            .iter()
            .filter(|e| {
                matches!(
                    e.kind,
                    segment::EntryKind::Data
                        | segment::EntryKind::Inline
                        | segment::EntryKind::CanonicalData
                        | segment::EntryKind::CanonicalInline
                ) && live.contains(&e.hash)
            })
            .map(|e| e.stored_length as u64)
            .sum();

        if live_bytes as f64 / total_bytes as f64 >= job.min_live_ratio {
            continue;
        }

        let (mut live_entries, dead_entries): (Vec<_>, Vec<_>) =
            entries.drain(..).partition(|e| match e.kind {
                segment::EntryKind::Zero => {
                    job.lbamap.hash_at(e.start_lba) == Some(crate::volume::ZERO_HASH)
                }
                segment::EntryKind::DedupRef | segment::EntryKind::Delta => {
                    job.lbamap.hash_at(e.start_lba) == Some(e.hash)
                }
                segment::EntryKind::Data | segment::EntryKind::Inline => live.contains(&e.hash),
                segment::EntryKind::CanonicalData | segment::EntryKind::CanonicalInline => {
                    // Canonical-only entries carry body for dedup resolution;
                    // kept whenever their hash is still referenced anywhere.
                    live.contains(&e.hash)
                }
            });

        let mut dead: Vec<RepackedDeadEntry> = Vec::new();
        for entry in &dead_entries {
            // Zero, DedupRef, and Delta are thin entries with no
            // extent-index slot; the apply phase's `remove_if_matches`
            // would always miss. Skipping keeps the dead set focused
            // on entries apply can actually act on.
            if matches!(
                entry.kind,
                segment::EntryKind::Zero | segment::EntryKind::DedupRef | segment::EntryKind::Delta
            ) {
                continue;
            }
            dead.push(RepackedDeadEntry {
                hash: entry.hash,
                source_body_offset: entry.stored_offset,
            });
        }

        let mut repacked_live: Vec<RepackedLiveEntry> = Vec::new();
        let mut new_body_section_start = 0u64;
        let mut all_dead_deleted = false;

        if !live_entries.is_empty() {
            let inline_bytes = segment::read_inline_section(seg_path)?;
            segment::read_extent_bodies(
                seg_path,
                body_section_start,
                &mut live_entries,
                [
                    segment::EntryKind::Data,
                    segment::EntryKind::Inline,
                    segment::EntryKind::CanonicalData,
                    segment::EntryKind::CanonicalInline,
                ],
                &inline_bytes,
            )?;

            // Verify bodies hash to their declared hash. Without this, a
            // poisoned segment (zero-filled body) would be silently
            // rewritten under the same extent index, surviving eviction of
            // the only good copy.
            for entry in &live_entries {
                if let Some(body) = entry.data.as_deref() {
                    segment::verify_body_hash(entry, body).map_err(|e| {
                        io::Error::new(e.kind(), format!("repack: input segment {seg_id}: {e}"))
                    })?;
                }
            }

            // Capture pre-write offsets — `write_segment` reassigns
            // `stored_offset` to the new body positions.
            let source_offsets: Vec<u64> = live_entries.iter().map(|e| e.stored_offset).collect();

            let new_ulid_str = seg_id.to_string();
            let tmp_path = job.pending_dir.join(format!("{new_ulid_str}.tmp"));
            let final_path = job.pending_dir.join(&new_ulid_str);
            new_body_section_start =
                segment::write_segment(&tmp_path, &mut live_entries, job.signer.as_ref())?;
            std::fs::rename(&tmp_path, &final_path)?;
            segment::fsync_dir(&final_path)?;
            stats.new_segments += 1;

            for (entry, source_body_offset) in
                live_entries.into_iter().zip(source_offsets.into_iter())
            {
                repacked_live.push(RepackedLiveEntry {
                    entry,
                    source_body_offset,
                });
            }
        } else {
            // Every entry is dead — delete the segment file outright.
            // Leaving it on disk would keep DedupRef bodies visible to a
            // later drain after their canonical hashes have been
            // dropped from the extent index by the apply phase.
            std::fs::remove_file(seg_path)?;
            segment::fsync_dir(seg_path)?;
            all_dead_deleted = true;
        }

        stats.segments_compacted += 1;
        let bytes_freed = total_bytes - live_bytes;
        stats.bytes_freed += bytes_freed;

        segments.push(RepackedSegment {
            seg_id,
            new_body_section_start,
            live: repacked_live,
            dead,
            all_dead_deleted,
            bytes_freed,
        });
    }

    Ok(RepackResult { stats, segments })
}

/// Worker-thread execution of a [`DeltaRepackJob`]. Constructs the
/// snapshot-pinned `BlockReader`, iterates every post-snapshot segment
/// in `pending/`, and invokes
/// [`crate::delta_compute::rewrite_post_snapshot_with_prior`] against
/// the prior-snapshot reader.
///
/// Per-segment errors are logged and the segment is skipped so one bad
/// segment can't derail the whole pass — mirrors the pre-offload
/// behaviour. The worker never updates the extent index; that's the
/// apply phase's job.
pub(crate) fn execute_delta_repack(job: DeltaRepackJob) -> io::Result<DeltaRepackResult> {
    use crate::block_reader::BlockReader;
    use crate::delta_compute;

    let DeltaRepackJob {
        base_dir,
        pending_dir,
        snap_ulid,
        signer,
        verifying_key,
        segment_cache,
    } = job;

    // Snapshot-pinned reader on the prior sealed snapshot. We pass a
    // `None` fetcher: delta repack is best-effort, and if a source body
    // is evicted locally we skip it rather than pull bytes off S3 just
    // to seed a dictionary.
    let prior = BlockReader::open_snapshot(&base_dir, &snap_ulid, Box::new(|_| None))?;

    let seg_paths = match segment::collect_segment_files(&pending_dir) {
        Ok(v) => v,
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };

    let mut stats = DeltaRepackStats::default();
    let mut segments: Vec<DeltaRepackedSegment> = Vec::new();

    for seg_path in seg_paths {
        let seg_filename = seg_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| io::Error::other("bad segment filename"))?;
        let seg_id =
            Ulid::from_string(seg_filename).map_err(|e| io::Error::other(e.to_string()))?;

        // Skip segments at or below the latest snapshot — they are
        // snapshot-frozen and must not be rewritten.
        if seg_id <= snap_ulid {
            continue;
        }

        stats.segments_scanned += 1;

        let rewritten = match delta_compute::rewrite_post_snapshot_with_prior(
            &seg_path,
            &prior,
            signer.as_ref(),
            &verifying_key,
            &segment_cache,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!("delta_repack: seg {seg_id} rewrite failed: {e} — leaving segment unchanged");
                continue;
            }
        };
        let Some(rewrite) = rewritten else {
            continue;
        };

        segments.push(DeltaRepackedSegment { seg_id, rewrite });
    }

    Ok(DeltaRepackResult { stats, segments })
}

/// Execute a snapshot-manifest sign job: enumerate `index/`, Ed25519
/// sign the manifest, atomic-write it, write the marker last.
///
/// `snapshots/` is created on demand. A `NotFound` on `index/` is
/// treated as an empty list — matches the inline behaviour.
pub(crate) fn execute_sign_snapshot_manifest(
    job: SignSnapshotManifestJob,
) -> io::Result<SignSnapshotManifestResult> {
    let SignSnapshotManifestJob {
        snap_ulid,
        base_dir,
        signer,
    } = job;

    let index_dir = base_dir.join("index");
    let mut seg_ulids: Vec<Ulid> = Vec::new();
    match std::fs::read_dir(&index_dir) {
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

    let snapshots_dir = base_dir.join("snapshots");
    std::fs::create_dir_all(&snapshots_dir)?;

    crate::signing::write_snapshot_manifest(&base_dir, signer.as_ref(), &snap_ulid, &seg_ulids)?;

    // Marker last — partial sequences leave no snapshot visible.
    std::fs::write(snapshots_dir.join(snap_ulid.to_string()), "")?;

    Ok(SignSnapshotManifestResult { snap_ulid })
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

/// Create a `VolumeActor` / `VolumeHandle` pair from an opened `Volume`.
///
/// The caller must spawn a thread and call `actor.run()` on it.  The
/// `VolumeHandle` can be cloned freely; each clone is intended for one thread.
///
/// Also spawns a worker thread for off-actor I/O (WAL promotion, etc.).
/// The worker exits when the actor shuts down and drops its job sender.
pub fn spawn(volume: Volume) -> (VolumeActor, VolumeHandle) {
    let (lbamap, extent_index) = volume.snapshot_maps();
    let initial = Arc::new(ReadSnapshot {
        lbamap,
        extent_index,
        flush_gen: 0,
    });
    let snapshot = Arc::new(ArcSwap::new(initial));

    let config = Arc::new(VolumeConfig {
        base_dir: volume.base_dir().to_owned(),
        ancestor_layers: volume.ancestor_layers().to_vec(),
        fetcher: volume.fetcher().cloned(),
    });

    // Channel depth of 64: enough to absorb bursts without blocking callers
    // while still providing backpressure if the actor falls behind.
    let (tx, rx) = bounded(64);

    // Worker channels: job channel bounded at 4, result channel matched.
    let (worker_job_tx, worker_job_rx) = bounded::<WorkerJob>(4);
    let (worker_result_tx, worker_result_rx) = bounded::<WorkerResult>(4);
    let worker_handle = std::thread::Builder::new()
        .name("volume-worker".into())
        .spawn(move || worker_thread(worker_job_rx, worker_result_tx))
        .expect("failed to spawn worker thread");

    let actor = VolumeActor {
        volume,
        snapshot: Arc::clone(&snapshot),
        rx,
        flush_gen: 0,
        worker_tx: Some(worker_job_tx),
        worker_rx: worker_result_rx,
        worker_handle: Some(worker_handle),
        promotes_in_flight: 0,
        promote_gen: 0,
        completed_gen: 0,
        inflight_old_wals: VecDeque::new(),
        parked_flushes: Vec::new(),
        parked_gc: None,
        parked_promote_wal: Vec::new(),
        parked_promote_segments: Vec::new(),
        promote_segments_in_flight: 0,
        parked_handoffs: None,
        handoff_in_flight: false,
        parked_sweep: None,
        parked_repack: None,
        parked_delta_repack: None,
        parked_sign_snapshot_manifest: None,
    };

    let handle = VolumeHandle {
        tx,
        snapshot,
        config,
        file_cache: RefCell::new(FileCache::default()),
        last_flush_gen: Cell::new(0),
    };

    (actor, handle)
}
