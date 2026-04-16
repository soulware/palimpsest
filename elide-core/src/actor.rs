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
    AncestorLayer, CompactionStats, DeltaRepackStats, FileCache, NoopSkipStats, PromoteJob,
    PromoteResult, ReclaimCandidate, ReclaimOutcome, ReclaimPlan, ReclaimProposed,
    ReclaimThresholds, Volume, find_segment_in_dirs, open_delta_body_in_dirs, read_extents,
    scan_reclaim_candidates,
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
    Snapshot {
        reply: Sender<io::Result<String>>,
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
    /// Sender for dispatching promote jobs to the flusher thread.
    /// `Option` so shutdown can `take()` it, dropping the sender to signal
    /// the flusher to exit.
    flusher_tx: Option<Sender<PromoteJob>>,
    /// Receiver for promote results from the flusher thread.
    /// Third arm in the `select!` loop.
    flusher_rx: Receiver<io::Result<PromoteResult>>,
    /// Join handle for the flusher thread, joined on shutdown.
    flusher_handle: Option<JoinHandle<()>>,
    /// Number of promote jobs dispatched but not yet applied.
    /// Used by the future GC drain gate (step 8).
    promotes_in_flight: usize,
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

    /// Dispatch a promote job to the flusher thread.
    ///
    /// Calls [`Volume::prepare_promote`] to snapshot the WAL state and open
    /// a fresh WAL, then sends the job to the flusher.  No-op if the WAL
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
        if let Some(tx) = &self.flusher_tx {
            if let Err(e) = tx.send(job) {
                warn!("flusher channel closed: {e}");
                return;
            }
            self.promotes_in_flight += 1;
        }
    }

    /// Drain in-flight promotes and join the flusher thread.
    ///
    /// Called on shutdown (explicit or handle-drop).  Drops the job sender
    /// to signal the flusher to exit, then drains all pending results,
    /// applying successful promotes so that the extent index is up to date
    /// before the volume is closed.
    fn shutdown_flusher(&mut self) {
        // Drop the sender — flusher's recv() will return Disconnected.
        self.flusher_tx.take();

        // Drain remaining results.
        while self.promotes_in_flight > 0 {
            match self.flusher_rx.recv() {
                Ok(Ok(result)) => {
                    self.promotes_in_flight -= 1;
                    self.volume.apply_promote(&result);
                    self.publish_snapshot();
                }
                Ok(Err(e)) => {
                    self.promotes_in_flight -= 1;
                    warn!("flusher promote failed during shutdown: {e}");
                }
                Err(_) => {
                    // Channel closed — flusher exited unexpectedly.
                    break;
                }
            }
        }

        // Join the flusher thread.
        if let Some(handle) = self.flusher_handle.take() {
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
                            self.shutdown_flusher();
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
                            // Flush = WAL fsync only.  Durability barrier for
                            // data already appended to the WAL.  Promotion is
                            // triggered asynchronously by threshold / idle tick.
                            let result = self.volume.wal_fsync();
                            let _ = reply.send(result);
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
                            let result = self.volume.sweep_pending();
                            if matches!(&result, Ok(s) if s.segments_compacted > 0) {
                                self.publish_snapshot();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::Repack { min_live_ratio, reply } => {
                            let result = self.volume.repack(min_live_ratio);
                            if matches!(&result, Ok(s) if s.segments_compacted > 0) {
                                self.publish_snapshot();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::DeltaRepackPostSnapshot { reply } => {
                            let result = self.volume.delta_repack_post_snapshot();
                            if matches!(&result, Ok(s) if s.entries_converted > 0) {
                                self.publish_snapshot();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::ApplyGcHandoffs { reply } => {
                            let result = self.volume.apply_gc_handoffs();
                            if matches!(&result, Ok(n) if *n > 0) {
                                let (lbamap, extent_index) = self.volume.snapshot_maps();
                                self.snapshot.store(Arc::new(ReadSnapshot {
                                    lbamap,
                                    extent_index,
                                    flush_gen: self.flush_gen,
                                }));
                                self.volume.evict_applied_gc_cache();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::GcCheckpoint { reply } => {
                            let result = self.volume.gc_checkpoint();
                            if result.is_ok() {
                                self.publish_snapshot();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::RedactSegment { ulid, reply } => {
                            let _ = reply.send(self.volume.redact_segment(ulid));
                        }
                        VolumeRequest::Promote { ulid, reply } => {
                            let result = self.volume.promote_segment(ulid);
                            if result.is_ok() {
                                self.publish_snapshot();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::FinalizeGcHandoff { ulid, reply } => {
                            let _ = reply.send(self.volume.finalize_gc_handoff(ulid));
                        }
                        VolumeRequest::Snapshot { reply } => {
                            let result = self.volume.snapshot().map(|u| u.to_string());
                            if result.is_ok() {
                                self.publish_snapshot();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::SignSnapshotManifest { snap_ulid, reply } => {
                            let _ = reply.send(self.volume.sign_snapshot_manifest(snap_ulid));
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
                            self.shutdown_flusher();
                            return;
                        }
                    }
                }
                recv(self.flusher_rx) -> msg => {
                    match msg {
                        Ok(Ok(result)) => {
                            self.promotes_in_flight -= 1;
                            self.volume.apply_promote(&result);
                            self.publish_snapshot();
                        }
                        Ok(Err(e)) => {
                            self.promotes_in_flight -= 1;
                            warn!("flusher promote failed: {e}");
                        }
                        Err(_) => {
                            warn!("flusher result channel closed unexpectedly");
                        }
                    }
                }
                recv(idle_tick) -> _ => {
                    // Dispatch a promote if the WAL has unflushed data.
                    // prepare_promote handles the empty-WAL case internally.
                    self.dispatch_promote();
                    // Apply any GC handoff files written by the coordinator.
                    match self.volume.apply_gc_handoffs() {
                        Ok(0) => {}
                        Ok(_) => {
                            let (lbamap, extent_index) = self.volume.snapshot_maps();
                            self.snapshot.store(Arc::new(ReadSnapshot {
                                lbamap,
                                extent_index,
                                flush_gen: self.flush_gen,
                            }));
                        }
                        Err(e) => warn!("gc handoff apply failed: {e}"),
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

    /// Flush the WAL to a pending segment.  Blocks until the actor replies.
    pub fn flush(&self) -> io::Result<()> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::Flush { reply: reply_tx })
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
    /// GC output segments (repack and sweep).  The two ULIDs are generated 2ms
    /// apart on the volume side so they are strictly ordered and have distinct
    /// timestamps.  Blocks until the actor replies.
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

    /// Write a snapshot marker for the current volume state.
    /// Flushes the WAL first.  Returns the snapshot ULID string.
    pub fn snapshot(&self) -> io::Result<String> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::Snapshot { reply: reply_tx })
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
    ///    `Volume::write_internal` (tier-1 noop skip active, tier-2
    ///    bypassed). On mismatch returns a clean discard.
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
// Flusher thread
// ---------------------------------------------------------------------------

/// Long-lived thread that processes [`PromoteJob`]s from the actor.
///
/// Receives jobs via `job_rx`, calls [`segment::write_and_commit`] for each,
/// and sends the result back on `result_tx`.  Exits when `job_rx` disconnects
/// (actor dropped the sender) or `result_tx` disconnects (actor gone).
fn flusher_thread(job_rx: Receiver<PromoteJob>, result_tx: Sender<io::Result<PromoteResult>>) {
    while let Ok(mut job) = job_rx.recv() {
        let outcome = segment::write_and_commit(
            &job.pending_dir,
            job.segment_ulid,
            &mut job.entries,
            job.signer.as_ref(),
        );
        let msg = match outcome {
            Ok(body_section_start) => Ok(PromoteResult {
                segment_ulid: job.segment_ulid,
                old_wal_ulid: job.old_wal_ulid,
                old_wal_path: job.old_wal_path,
                body_section_start,
                entries: job.entries,
                pre_promote_offsets: job.pre_promote_offsets,
            }),
            Err(e) => Err(e),
        };
        if result_tx.send(msg).is_err() {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

/// Create a `VolumeActor` / `VolumeHandle` pair from an opened `Volume`.
///
/// The caller must spawn a thread and call `actor.run()` on it.  The
/// `VolumeHandle` can be cloned freely; each clone is intended for one thread.
///
/// Also spawns a flusher thread for off-actor WAL promotion.  The flusher
/// exits when the actor shuts down and drops its job sender.
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

    // Flusher channels: job channel bounded at 4 (~128 MiB of WAL data),
    // result channel bounded at 4 (matching job capacity).
    let (flusher_job_tx, flusher_job_rx) = bounded::<PromoteJob>(4);
    let (flusher_result_tx, flusher_result_rx) = bounded::<io::Result<PromoteResult>>(4);
    let flusher_handle = std::thread::Builder::new()
        .name("volume-flusher".into())
        .spawn(move || flusher_thread(flusher_job_rx, flusher_result_tx))
        .expect("failed to spawn flusher thread");

    let actor = VolumeActor {
        volume,
        snapshot: Arc::clone(&snapshot),
        rx,
        flush_gen: 0,
        flusher_tx: Some(flusher_job_tx),
        flusher_rx: flusher_result_rx,
        flusher_handle: Some(flusher_handle),
        promotes_in_flight: 0,
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
