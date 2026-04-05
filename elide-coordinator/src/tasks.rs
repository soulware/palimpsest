// Single-volume coordinator tasks: drain pending segments to S3, run GC,
// and prefetch indexes on startup.
//
// Called by both the multi-volume coordinator daemon (once per discovered
// volume) and standalone single-volume mode (`elide volume up`).
//
// Per-tick sequencing:
//   1. Upload pending/ segments to S3 (drain_pending)
//   2. If gc_interval has elapsed: apply done handoffs, run GC pass
//
// Drain and GC are sequential within each tick so that GC sees all segments
// uploaded this tick, and concurrent GC rewrites can never race an upload.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::ObjectStore;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

use crate::config::GcConfig;
use crate::control;
use crate::gc;
use crate::prefetch;
use crate::upload;

/// Name of the import lock file inside a volume directory.
/// Present while an import job is actively writing to the volume; drain and GC
/// are skipped during this window.
const IMPORT_LOCK_FILE: &str = "import.lock";

/// Run the drain + GC loop for a single volume directory.
///
/// Exits when the volume directory is removed. Intended to be spawned as a
/// tokio task.
pub async fn run_volume_tasks(
    fork_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    drain_interval: Duration,
    gc_config: GcConfig,
) {
    let volume_id = match upload::derive_names(&fork_dir) {
        Ok(id) => id,
        Err(e) => {
            warn!(
                "[coordinator] cannot derive volume id for {}: {e}",
                fork_dir.display()
            );
            return;
        }
    };
    let volume_name = read_volume_name(&fork_dir)
        .map(|n| format!(" ({n})"))
        .unwrap_or_default();

    let gc_interval = Duration::from_secs(gc_config.interval_secs);

    // Run GC on the first tick to clear any backlog from a previous run.
    let mut last_gc = Instant::now()
        .checked_sub(gc_interval)
        .unwrap_or_else(Instant::now);

    // Prefetch segment indexes on startup if the fork has no local segments.
    // This covers the common case of a volume pulled from the store with only
    // the directory skeleton — without .idx files Volume::open cannot rebuild
    // the LBA map and the volume is unreadable.
    let has_local_segments = fork_dir.join("segments").exists()
        && fork_dir
            .join("segments")
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
    if !has_local_segments {
        let did_fetch = match prefetch::prefetch_indexes(&fork_dir, &store).await {
            Ok(r) if r.fetched > 0 => {
                info!(
                    "[prefetch {volume_id}{volume_name}] fetched {} index section(s)",
                    r.fetched
                );
                true
            }
            Ok(_) => false,
            Err(e) => {
                warn!("[prefetch {volume_id}{volume_name}] error: {e:#}");
                false
            }
        };

        if did_fetch {
            let prewarm_dir = fork_dir.clone();
            let prewarm_store = store.clone();
            let by_id_dir = fork_dir.parent().unwrap_or(&fork_dir).to_owned();
            match tokio::task::spawn_blocking(move || {
                elide_fetch::prewarm_volume_start(
                    &prewarm_dir,
                    &by_id_dir,
                    prewarm_store,
                    elide_fetch::DEFAULT_FETCH_BATCH_BYTES,
                )
            })
            .await
            {
                Ok(Ok(())) => info!("[prewarm {volume_id}{volume_name}] volume start pre-warmed"),
                Ok(Err(e)) => warn!("[prewarm {volume_id}{volume_name}] {e}"),
                Err(e) => warn!("[prewarm {volume_id}{volume_name}] task error: {e}"),
            }
        }
    }

    let mut tick = tokio::time::interval(drain_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;

        if !fork_dir.exists() {
            info!(
                "[coordinator] fork removed, stopping: {}",
                fork_dir.display()
            );
            break;
        }

        // Skip drain/GC while an import is writing to this fork.
        if fork_dir.join(IMPORT_LOCK_FILE).exists() {
            continue;
        }

        // Steps 1-3: compact pending segments via volume IPC (best-effort;
        // skipped silently if the control socket is absent so that upload
        // still runs for forks without a live volume process).
        if fork_dir.join("control.sock").exists() {
            control::flush(&fork_dir).await;

            if let Some(s) = control::sweep_pending(&fork_dir).await
                && s.segments_compacted > 0
            {
                info!(
                    "[drain {volume_id}] sweep: {} segment(s), ~{} bytes freed",
                    s.segments_compacted, s.bytes_freed
                );
            }

            if let Some(s) = control::repack(&fork_dir, gc_config.density_threshold).await
                && s.segments_compacted > 0
            {
                info!(
                    "[drain {volume_id}] repack: {} segment(s), ~{} bytes freed",
                    s.segments_compacted, s.bytes_freed
                );
            }
        }

        // Step 4: drain pending segments to S3.
        //
        // ORDERING INVARIANT: step 4 must fully succeed before step 5 (GC)
        // runs.  gc_checkpoint mints GC output ULIDs from the volume's
        // monotonic clock; any segment already in pending/ at that moment has
        // a ULID below the GC output.  If such a segment is later drained to
        // segments/, crash-recovery rebuild gives the GC output incorrect
        // priority for the shared LBAs, returning stale data.  Skipping GC
        // when drain has failures enforces the invariant and defers the GC
        // pass to the next tick.
        let mut drain_ok = true;
        if fork_dir.join("pending").exists() {
            match upload::drain_pending(&fork_dir, &volume_id, &store).await {
                Ok(r) if r.failed > 0 => {
                    error!(
                        "[drain {volume_id}] {} segment(s) failed to upload; \
                         skipping GC this tick to preserve ULID ordering invariant",
                        r.failed
                    );
                    drain_ok = false;
                }
                Ok(r) if r.uploaded > 0 => {
                    info!("[drain {volume_id}] {} uploaded", r.uploaded);
                }
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "[drain {volume_id}] drain error: {e:#}; \
                         skipping GC this tick to preserve ULID ordering invariant"
                    );
                    drain_ok = false;
                }
            }
        }

        // Step 5: GC pass (rate-limited to gc_interval).
        if last_gc.elapsed() >= gc_interval {
            if !drain_ok {
                continue;
            }

            let Some((repack_ulid, sweep_ulid)) = control::gc_checkpoint(&fork_dir).await else {
                last_gc = Instant::now();
                continue;
            };

            let handoffs_applied = control::apply_gc_handoffs(&fork_dir).await;
            if handoffs_applied > 0 {
                info!(
                    "[gc {volume_id}] re-applied {handoffs_applied} GC handoff(s) \
                     after restart"
                );
            }

            match gc::apply_done_handoffs(&fork_dir, &volume_id, &store).await {
                Ok(0) => {}
                Ok(n) => info!("[gc {volume_id}] completed {n} GC handoff(s)"),
                Err(e) => error!("[gc {volume_id}] handoff cleanup error: {e:#}"),
            }

            let pruned = gc::cleanup_done_handoffs(&fork_dir, gc::DONE_FILE_TTL);
            if pruned > 0 {
                info!("[gc {volume_id}] pruned {pruned} expired .done file(s)");
            }

            match gc::gc_fork(
                &fork_dir,
                &volume_id,
                &store,
                &gc_config,
                repack_ulid,
                sweep_ulid,
            )
            .await
            {
                Ok(gc::GcStats {
                    strategy: gc::GcStrategy::Repack,
                    bytes_freed,
                    ..
                }) => {
                    info!(
                        "[gc {volume_id}] density: compacted 1 segment, ~{bytes_freed} bytes freed"
                    );
                }
                Ok(gc::GcStats {
                    strategy: gc::GcStrategy::Sweep,
                    candidates,
                    bytes_freed,
                }) => {
                    info!(
                        "[gc {volume_id}] sweep: packed {candidates} small segment(s), ~{bytes_freed} bytes freed"
                    );
                }
                Ok(gc::GcStats {
                    strategy: gc::GcStrategy::Both,
                    candidates,
                    bytes_freed,
                }) => {
                    info!(
                        "[gc {volume_id}] repack+sweep: {candidates} segment(s) compacted, ~{bytes_freed} bytes freed"
                    );
                }
                Ok(_) => {}
                Err(e) => error!("[gc {volume_id}] error: {e:#}"),
            }

            last_gc = Instant::now();
        }
    }
}

/// Read the volume name from `<fork_dir>/volume.name`, if present and non-empty.
pub fn read_volume_name(fork_dir: &Path) -> Option<String> {
    let s = std::fs::read_to_string(fork_dir.join("volume.name")).ok()?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}
