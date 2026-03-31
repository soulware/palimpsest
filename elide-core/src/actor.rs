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
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender, bounded, tick};
use log::warn;

use crate::extentindex::ExtentIndex;
use crate::lbamap::LbaMap;
use crate::segment::BoxFetcher;
use crate::volume::{AncestorLayer, CompactionStats, Volume, find_segment_in_dirs, read_extents};

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
    GcCheckpoint {
        reply: Sender<io::Result<(String, String)>>,
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
    fn after_promote(&mut self) {
        self.flush_gen += 1;
        let (lbamap, extent_index) = self.volume.snapshot_maps();
        self.snapshot.store(Arc::new(ReadSnapshot {
            lbamap,
            extent_index,
            flush_gen: self.flush_gen,
        }));
    }

    pub fn run(mut self) {
        let idle_tick = tick(IDLE_FLUSH_INTERVAL);
        loop {
            crossbeam_channel::select! {
                recv(self.rx) -> msg => {
                    let req = match msg {
                        Ok(r) => r,
                        Err(_) => return, // all handles dropped
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
                            // Reply before promoting: the write caller is unblocked
                            // immediately after the WAL append.  The promote cost
                            // (fsync + segment write) is paid before the next
                            // queued message is processed, not by this caller.
                            let _ = reply.send(result);
                            if self.volume.needs_promote() {
                                if let Err(e) = self.volume.flush_wal() {
                                    warn!("threshold-triggered promote failed: {e}");
                                } else {
                                    self.after_promote();
                                }
                            }
                        }
                        VolumeRequest::Flush { reply } => {
                            let result = self.volume.flush_wal();
                            if result.is_ok() {
                                self.after_promote();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::SweepPending { reply } => {
                            let result = self.volume.sweep_pending();
                            if matches!(&result, Ok(s) if s.segments_compacted > 0) {
                                self.after_promote();
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::Repack { min_live_ratio, reply } => {
                            let result = self.volume.repack(min_live_ratio);
                            if matches!(&result, Ok(s) if s.segments_compacted > 0) {
                                self.after_promote();
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
                            }
                            let _ = reply.send(result);
                        }
                        VolumeRequest::GcCheckpoint { reply } => {
                            let result = self.volume.gc_checkpoint();
                            if result.is_ok() {
                                self.after_promote();
                            }
                            let pair = result.map(|_| {
                                let u1 = ulid::Ulid::new().to_string();
                                std::thread::sleep(std::time::Duration::from_millis(2));
                                let u2 = ulid::Ulid::new().to_string();
                                (u1, u2)
                            });
                            let _ = reply.send(pair);
                        }
                        VolumeRequest::Shutdown => return,
                    }
                }
                recv(idle_tick) -> _ => {
                    // Promote any unflushed WAL data that has been sitting idle.
                    // No-op if the WAL is empty.  Errors are logged and not fatal:
                    // the data is safe in the WAL; the next write or explicit
                    // flush will retry.
                    if let Err(e) = self.volume.flush_wal() {
                        warn!("idle flush failed: {e}");
                    } else {
                        self.after_promote();
                    }
                    // Apply any GC handoff files written by the coordinator.
                    // No flush_gen bump: GC is a segment-to-segment move; body
                    // offsets remain absolute and the fd cache's segment-id
                    // mismatch detection handles eviction naturally.
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
    /// Per-handle file-handle cache.  Never contended: each ublk queue thread
    /// holds its own clone.  `RefCell` is sufficient; `Mutex` is not needed.
    file_cache: RefCell<Option<(String, fs::File)>>,
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
            file_cache: RefCell::new(None), // fresh cache per clone/thread
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
            *self.file_cache.borrow_mut() = None;
            self.last_flush_gen.set(snap.flush_gen);
        }
        read_extents(
            lba,
            lba_count,
            &snap.lbamap,
            &snap.extent_index,
            &self.file_cache,
            |id| {
                find_segment_in_dirs(
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
    pub fn gc_checkpoint(&self) -> io::Result<(String, String)> {
        let (reply_tx, reply_rx) = bounded(1);
        self.tx
            .send(VolumeRequest::GcCheckpoint { reply: reply_tx })
            .map_err(|_| io::Error::other("volume actor channel closed"))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::other("volume actor reply channel closed"))?
    }

    /// Signal the actor to shut down and drain remaining requests.
    pub fn shutdown(&self) {
        let _ = self.tx.send(VolumeRequest::Shutdown);
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
/// `volume_size` is the block-device size in bytes, forwarded to the handle
/// for NBD/ublk export info.
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

    let actor = VolumeActor {
        volume,
        snapshot: Arc::clone(&snapshot),
        rx,
        flush_gen: 0,
    };

    let handle = VolumeHandle {
        tx,
        snapshot,
        config,
        file_cache: RefCell::new(None),
        last_flush_gen: Cell::new(0),
    };

    (actor, handle)
}
