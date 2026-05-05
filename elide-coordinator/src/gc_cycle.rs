// Per-volume drain + GC tick orchestrator.
//
// Mechanical extraction of the tick body that used to live inline in
// `run_volume_tasks` (see `tasks.rs`). One `run_tick()` call performs the
// pre-flight checks, volume-side IPC compactions, S3 drain, and the
// rate-limited GC pass — same call order, same logs, same behaviour.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use object_store::ObjectStore;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{error, info};

use crate::config::GcConfig;
use crate::volume_state::IMPORTING_FILE;
use crate::{SnapshotLockRegistry, control, gc, snapshot_lock_for, upload};

/// Outcome of a single tick. `Stop` is returned when the fork directory has
/// disappeared and the per-volume task should exit.
pub enum TickOutcome {
    Continue,
    Stop,
}

/// Drives one drain + GC cycle per `run_tick()` call. Constructed once per
/// volume task; cross-tick state (`last_gc`, `gc_was_active`) lives on
/// `&mut self`.
pub struct GcCycleOrchestrator {
    fork_dir: PathBuf,
    by_id_dir: PathBuf,
    volume_id: String,
    store: Arc<dyn ObjectStore>,
    part_size_bytes: usize,
    gc_config: GcConfig,
    snap_lock: Arc<AsyncMutex<()>>,
    last_gc: Instant,
    gc_was_active: bool,
}

impl GcCycleOrchestrator {
    pub fn new(
        fork_dir: PathBuf,
        volume_id: String,
        store: Arc<dyn ObjectStore>,
        part_size_bytes: usize,
        gc_config: GcConfig,
        snapshot_locks: &SnapshotLockRegistry,
    ) -> Self {
        let by_id_dir = fork_dir
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| fork_dir.clone());
        let snap_lock = snapshot_lock_for(snapshot_locks, &fork_dir);
        // Run GC on the first tick to clear any backlog from a previous run.
        let last_gc = Instant::now()
            .checked_sub(gc_config.interval)
            .unwrap_or_else(Instant::now);
        Self {
            fork_dir,
            by_id_dir,
            volume_id,
            store,
            part_size_bytes,
            gc_config,
            snap_lock,
            last_gc,
            gc_was_active: true,
        }
    }

    pub fn fork_dir(&self) -> &Path {
        &self.fork_dir
    }

    pub async fn run_tick(&mut self) -> TickOutcome {
        if !self.fork_dir.exists() {
            info!(
                "[coordinator] fork removed, stopping: {}",
                self.fork_dir.display()
            );
            return TickOutcome::Stop;
        }

        // Skip drain/GC while an import is in its write phase (volume.importing
        // present but no control.sock yet). When both are present the import
        // is in its serve phase and is ready to handle promote IPC — fall
        // through to the normal drain path.
        if self.fork_dir.join(IMPORTING_FILE).exists()
            && !self.fork_dir.join("control.sock").exists()
        {
            return TickOutcome::Continue;
        }

        // Skip drain/GC while a snapshot is in flight for this volume. The
        // snapshot handler holds this lock for its full sequence (flush →
        // drain → sign manifest → upload); racing the tick loop against it
        // would reorder pending/ uploads against the manifest's index view.
        //
        // Cloning the Arc gives the guard an owner that is not borrowed
        // from `self`, so subsequent `&mut self` calls (e.g. `run_gc_pass`)
        // don't conflict with the live guard.
        let snap_lock = self.snap_lock.clone();
        let _snap_guard = match snap_lock.try_lock() {
            Ok(g) => g,
            Err(_) => {
                tracing::debug!(
                    "[coordinator] skipping tick for {}: snapshot in flight",
                    self.fork_dir.display()
                );
                return TickOutcome::Continue;
            }
        };

        self.run_volume_compactions().await;
        let drain_ok = self.run_drain().await;

        if self.last_gc.elapsed() >= self.gc_config.interval {
            // Finalize outstanding bare `gc/<ulid>` files first, independent
            // of `gc_checkpoint` and `drain_ok`. A bare file is a handoff the
            // volume already committed (`.staged` → bare) but which the
            // coordinator has not yet uploaded + promoted. If the coordinator
            // crashes between those steps on a quiescent volume, the next
            // `gc_checkpoint` returns `Idle` (WAL empty + no `.staged`), and
            // gating cleanup behind the checkpoint would strand the bare file
            // indefinitely — `has_pending_results` would then also block
            // every future `gc_fork` pass. Always run this.
            self.run_handoff_cleanup().await;

            if !drain_ok {
                return TickOutcome::Continue;
            }

            self.run_gc_pass().await;
            self.last_gc = Instant::now();
        }

        TickOutcome::Continue
    }

    /// Volume-side compactions (best-effort; skipped silently if the control
    /// socket is absent so that drain still runs for forks without a live
    /// volume process). Skipped for readonly volumes: flush/sweep/repack are
    /// WAL and compaction operations that only make sense for writable
    /// volumes. During an import serve phase, control.sock is bound by the
    /// import process which only handles promote IPC.
    async fn run_volume_compactions(&self) {
        if !self.fork_dir.join("control.sock").exists()
            || self.fork_dir.join("volume.readonly").exists()
        {
            return;
        }

        let volume_id = &self.volume_id;
        control::promote_wal(&self.fork_dir).await;

        if let Some(s) = control::sweep_pending(&self.fork_dir).await
            && s.segments_compacted > 0
        {
            info!(
                "[drain {volume_id}] sweep: {} segment(s), ~{} bytes freed",
                s.segments_compacted, s.bytes_freed
            );
        }

        if let Some(s) = control::repack(&self.fork_dir, self.gc_config.density_threshold).await
            && s.segments_compacted > 0
        {
            info!(
                "[drain {volume_id}] repack: {} segment(s), ~{} bytes freed",
                s.segments_compacted, s.bytes_freed
            );
        }

        // Alias-merge extent reclamation: rewrites LBA sub-ranges of bloated
        // hashes (partial-overwrite survivors) into fresh compact entries.
        // One candidate per tick caps per-tick latency; the scanner sorts
        // most-wasteful-first, so sustained bloat converges across ticks.
        // Default scanner thresholds gate tiny / weakly-bloated hashes out.
        if let Some(s) = control::reclaim(&self.fork_dir, Some(1)).await
            && s.runs_rewritten > 0
        {
            info!(
                "[drain {volume_id}] reclaim: scanned={} runs={} bytes={} discarded={}",
                s.candidates_scanned, s.runs_rewritten, s.bytes_rewritten, s.discarded,
            );
        }

        // Phase 5 Tier 1: rewrite post-snapshot pending segments with
        // zstd-dictionary deltas against same-LBA extents from the latest
        // sealed snapshot. Runs before drain so converted segments reach S3
        // as thin Delta entries rather than full bodies.
        if let Some(s) = control::delta_repack_post_snapshot(&self.fork_dir).await
            && s.entries_converted > 0
        {
            info!(
                "[drain {volume_id}] delta_repack: {}/{} segment(s) rewritten, \
                 {} entries converted, {} → {} bytes",
                s.segments_rewritten,
                s.segments_scanned,
                s.entries_converted,
                s.original_body_bytes,
                s.delta_body_bytes,
            );
        }
    }

    /// Drain pending segments to S3. Returns whether GC may proceed: a drain
    /// failure forces this tick's GC to be skipped, since pending segments
    /// that failed to promote still have no `cache/<ulid>.body` and would
    /// not appear in the GC candidate set, while their LBAs would be
    /// invisible to `collect_stats`.
    async fn run_drain(&self) -> bool {
        if !self.fork_dir.join("pending").exists() {
            return true;
        }
        let volume_id = &self.volume_id;
        match upload::drain_pending(&self.fork_dir, volume_id, &self.store, self.part_size_bytes)
            .await
        {
            Ok(r) if r.failed > 0 => {
                error!(
                    "[drain {volume_id}] {} segment(s) failed to upload; \
                     skipping GC this tick to preserve ULID ordering invariant",
                    r.failed
                );
                false
            }
            Ok(r) => {
                if r.uploaded > 0 {
                    info!("[drain {volume_id}] {} uploaded", r.uploaded);
                }
                true
            }
            Err(e) => {
                error!(
                    "[drain {volume_id}] drain error: {e:#}; \
                     skipping GC this tick to preserve ULID ordering invariant"
                );
                false
            }
        }
    }

    async fn run_handoff_cleanup(&self) {
        let volume_id = &self.volume_id;
        match gc::apply_done_handoffs(&self.fork_dir, volume_id, &self.store, self.part_size_bytes)
            .await
        {
            Ok(0) => {}
            Ok(n) => info!("[gc {volume_id}] completed {n} GC handoff(s)"),
            Err(e) => error!("[gc {volume_id}] handoff cleanup error: {e:#}"),
        }
    }

    async fn run_gc_pass(&mut self) {
        let volume_id = &self.volume_id;
        let Some(u_gc) = control::gc_checkpoint(&self.fork_dir).await else {
            return;
        };

        let handoffs_applied = control::apply_gc_handoffs(&self.fork_dir).await;
        if handoffs_applied > 0 {
            info!("[gc {volume_id}] volume applied {handoffs_applied} GC handoff(s)");
        }

        let gc_result = {
            let fork_dir = self.fork_dir.clone();
            let by_id_dir = self.by_id_dir.clone();
            let gc_config = self.gc_config.clone();
            tokio::task::spawn_blocking(move || {
                gc::gc_fork(&fork_dir, &by_id_dir, &gc_config, u_gc)
            })
            .await
            .unwrap_or_else(|e| Err(anyhow::anyhow!("gc task panicked: {e}")))
        };
        match gc_result {
            Ok(gc::GcStats {
                strategy: gc::GcStrategy::Compact,
                candidates,
                bytes_freed,
                dead_cleaned,
                ..
            }) => {
                self.gc_was_active = true;
                info!(
                    "[gc {volume_id}] compact: {candidates} input(s) ({dead_cleaned} dead), \
                     ~{bytes_freed} bytes freed"
                );
            }
            Ok(gc::GcStats {
                strategy: gc::GcStrategy::None(reason),
                total_segments,
                ..
            }) => {
                // Only the NoCandidates reason reflects a real idle-pass
                // result. NoIndex and PendingHandoffs are transient bail-outs
                // that do not advance the active→idle state — another tick
                // will re-evaluate once the bail condition clears. The
                // "volume applied" / "completed N handoff(s)" logs already
                // cover PendingHandoffs visibility.
                if matches!(reason, gc::NoneReason::NoCandidates) && self.gc_was_active {
                    info!(
                        "[gc {volume_id}] idle — {total_segments} segment(s), \
                         nothing eligible (threshold {:.2})",
                        self.gc_config.density_threshold
                    );
                    self.gc_was_active = false;
                }
            }
            Err(e) => error!("[gc {volume_id}] error: {e:#}"),
        }
    }
}
