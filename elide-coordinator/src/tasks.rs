// Single-volume coordinator tasks: drain pending segments to S3, run GC,
// and prefetch indexes on startup.
//
// Spawned once per volume by the coordinator daemon.
//
// Per-tick sequencing:
//   1. Upload pending/ segments to S3 (drain_pending)
//   2. If gc_interval has elapsed: apply done handoffs, run GC pass
//
// Drain and GC are sequential within each tick so that GC sees all segments
// uploaded this tick, and concurrent GC rewrites can never race an upload.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::ObjectStore;
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

use crate::config::GcConfig;
use crate::control;
use crate::gc;
use crate::prefetch;
use crate::upload;
use crate::{SnapshotLockRegistry, snapshot_lock_for};

/// Name of the import lock file inside a volume directory.
/// Present while an import job is actively writing to the volume; drain and GC
/// are skipped during this window.
const IMPORT_LOCK_FILE: &str = "import.lock";

/// Request type for the per-fork evict channel.
/// The sender receives the eviction result (count of bodies deleted).
pub type EvictReply = oneshot::Sender<io::Result<usize>>;

/// Evict all S3-confirmed segment bodies from `cache/` for the given fork.
///
/// Called from within the fork task (between drain/GC ticks) so it never
/// races with the GC pass's collect_stats → compact_segments window.
fn evict_bodies(fork_dir: &Path) -> io::Result<usize> {
    let cache_dir = fork_dir.join("cache");
    let index_dir = fork_dir.join("index");
    let mut count = 0;
    let entries = match std::fs::read_dir(&cache_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let Some(ulid_str) = name_str.strip_suffix(".body") else {
            continue;
        };
        if !index_dir.join(format!("{ulid_str}.idx")).exists() {
            continue; // not yet S3-confirmed; skip
        }
        let _ = std::fs::remove_file(entry.path());
        let _ = std::fs::remove_file(cache_dir.join(format!("{ulid_str}.present")));
        count += 1;
    }
    Ok(count)
}

/// Evict a single S3-confirmed segment body from `cache/`.
fn evict_one_body(fork_dir: &Path, ulid_str: &str) -> io::Result<usize> {
    let idx_path = fork_dir.join("index").join(format!("{ulid_str}.idx"));
    if !idx_path.try_exists()? {
        return Err(io::Error::other(format!(
            "segment {ulid_str} has no index/{ulid_str}.idx — not S3-confirmed"
        )));
    }
    let body_path = fork_dir.join("cache").join(format!("{ulid_str}.body"));
    if !body_path.try_exists()? {
        return Err(io::Error::other(format!(
            "segment {ulid_str} not found in cache/"
        )));
    }
    std::fs::remove_file(&body_path)?;
    let _ = std::fs::remove_file(fork_dir.join("cache").join(format!("{ulid_str}.present")));
    Ok(1)
}

/// Run the drain + GC loop for a single volume directory.
///
/// `evict_rx` receives eviction requests from the coordinator inbound handler.
/// Evictions are processed between ticks so they never race with the GC pass.
///
/// Exits when the volume directory is removed. Intended to be spawned as a
/// tokio task.
#[allow(clippy::too_many_arguments)]
pub async fn run_volume_tasks(
    fork_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    drain_interval: Duration,
    gc_config: GcConfig,
    part_size_bytes: usize,
    mut evict_rx: mpsc::Receiver<(Option<String>, EvictReply)>,
    snapshot_locks: SnapshotLockRegistry,
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
    // Track whether the previous GC tick did any work, so we can log once
    // when GC transitions from active → idle (converged).
    let mut gc_was_active = true;

    // Prefetch segment indexes on startup if the fork has no local index files.
    // This covers the common case of a volume pulled from the store with only
    // the directory skeleton — without .idx files Volume::open cannot rebuild
    // the LBA map and the volume is unreadable.
    let has_local_segments = fork_dir.join("index").exists()
        && fork_dir
            .join("index")
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
            let prewarm_store: std::sync::Arc<dyn elide_fetch::RangeFetcher> = std::sync::Arc::new(
                crate::range_fetcher::ObjectStoreRangeFetcher::new(store.clone()),
            );
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
        tokio::select! {
            // Eviction requests are processed between ticks — never mid-GC.
            Some((ulid_str, reply_tx)) = evict_rx.recv() => {
                let result = match ulid_str {
                    Some(ref s) => evict_one_body(&fork_dir, s),
                    None => evict_bodies(&fork_dir),
                };
                let _ = reply_tx.send(result);
                continue;
            }
            _ = tick.tick() => {}
        }

        if !fork_dir.exists() {
            info!(
                "[coordinator] fork removed, stopping: {}",
                fork_dir.display()
            );
            break;
        }

        // Skip drain/GC while an import is in its write phase (import.lock
        // present but no control.sock yet).  When both are present the import
        // is in its serve phase and is ready to handle promote IPC — fall
        // through to the normal drain path.
        if fork_dir.join(IMPORT_LOCK_FILE).exists() && !fork_dir.join("control.sock").exists() {
            continue;
        }

        // Skip drain/GC while a snapshot is in flight for this volume. The
        // snapshot handler holds this lock for its full sequence (flush →
        // drain → sign manifest → upload); racing the tick loop against it
        // would reorder pending/ uploads against the manifest's index view.
        let snap_lock = snapshot_lock_for(&snapshot_locks, &fork_dir);
        let _tick_guard = match snap_lock.try_lock() {
            Ok(g) => g,
            Err(_) => {
                tracing::debug!(
                    "[coordinator] skipping tick for {}: snapshot in flight",
                    fork_dir.display()
                );
                continue;
            }
        };

        // Steps 1-3: compact pending segments via volume IPC (best-effort;
        // skipped silently if the control socket is absent so that upload
        // still runs for forks without a live volume process).
        // Skipped for readonly volumes: flush/sweep/repack are WAL and
        // compaction operations that only make sense for writable volumes.
        // During an import serve phase, control.sock is bound by the import
        // process which only handles promote IPC.
        if fork_dir.join("control.sock").exists() && !fork_dir.join("volume.readonly").exists() {
            control::promote_wal(&fork_dir).await;

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

            // Alias-merge extent reclamation: rewrites LBA sub-ranges of
            // bloated hashes (partial-overwrite survivors) into fresh
            // compact entries. One candidate per tick caps per-tick
            // latency; the scanner sorts most-wasteful-first, so
            // sustained bloat converges across ticks. Default scanner
            // thresholds gate tiny / weakly-bloated hashes out.
            if let Some(s) = control::reclaim(&fork_dir, Some(1)).await
                && s.runs_rewritten > 0
            {
                info!(
                    "[drain {volume_id}] reclaim: scanned={} runs={} bytes={} discarded={}",
                    s.candidates_scanned, s.runs_rewritten, s.bytes_rewritten, s.discarded,
                );
            }

            // Phase 5 Tier 1: rewrite post-snapshot pending segments with
            // zstd-dictionary deltas against same-LBA extents from the latest
            // sealed snapshot. Runs before drain so converted segments reach
            // S3 as thin Delta entries rather than full bodies.
            if let Some(s) = control::delta_repack_post_snapshot(&fork_dir).await
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

        // Step 4: drain pending segments to S3.
        //
        // Skip GC this tick if drain had upload or promote failures.  Pending
        // segments that failed to promote still have no cache/<ulid>.body, so
        // collect_stats would skip them and GC would not compact them — but
        // their LBAs would not appear in the GC candidate set either.  Deferring
        // GC until drain succeeds avoids repeated no-ops and ensures GC always
        // operates on a fully-promoted, consistent segment set.
        let mut drain_ok = true;
        if fork_dir.join("pending").exists() {
            match upload::drain_pending(&fork_dir, &volume_id, &store, part_size_bytes).await {
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
            // Finalize outstanding bare `gc/<ulid>` files first, independent
            // of `gc_checkpoint` and `drain_ok`. A bare file is a handoff the
            // volume already committed (`.staged` → bare) but which the
            // coordinator has not yet uploaded + promoted. If the coordinator
            // crashes between those steps on a quiescent volume, the next
            // `gc_checkpoint` returns `Idle` (WAL empty + no `.staged`), and
            // gating cleanup behind the checkpoint would strand the bare file
            // indefinitely — `has_pending_results` would then also block
            // every future `gc_fork` pass. Always run this.
            match gc::apply_done_handoffs(&fork_dir, &volume_id, &store, part_size_bytes).await {
                Ok(0) => {}
                Ok(n) => info!("[gc {volume_id}] completed {n} GC handoff(s)"),
                Err(e) => error!("[gc {volume_id}] handoff cleanup error: {e:#}"),
            }

            if !drain_ok {
                continue;
            }

            let Some(u_gc) = control::gc_checkpoint(&fork_dir).await else {
                last_gc = Instant::now();
                continue;
            };

            let handoffs_applied = control::apply_gc_handoffs(&fork_dir).await;
            if handoffs_applied > 0 {
                info!("[gc {volume_id}] volume applied {handoffs_applied} GC handoff(s)");
            }

            let gc_result = {
                let fork_dir = fork_dir.clone();
                let by_id_dir = fork_dir.parent().unwrap_or(&fork_dir).to_path_buf();
                let gc_config = gc_config.clone();
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
                    gc_was_active = true;
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
                    if matches!(reason, gc::NoneReason::NoCandidates) && gc_was_active {
                        info!(
                            "[gc {volume_id}] idle — {total_segments} segment(s), \
                             nothing eligible (threshold {:.2})",
                            gc_config.density_threshold
                        );
                        gc_was_active = false;
                    }
                }
                Err(e) => error!("[gc {volume_id}] error: {e:#}"),
            }

            last_gc = Instant::now();
        }
    }
}

/// Read the volume name from `volume.toml`, if present and non-empty.
pub fn read_volume_name(fork_dir: &Path) -> Option<String> {
    let cfg = elide_core::config::VolumeConfig::read(fork_dir).ok()?;
    let name = cfg.name?;
    if name.trim().is_empty() {
        None
    } else {
        Some(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_vol(tmp: &TempDir) -> PathBuf {
        let vol = tmp.path().to_path_buf();
        fs::create_dir_all(vol.join("cache")).unwrap();
        fs::create_dir_all(vol.join("index")).unwrap();
        vol
    }

    fn write_stub_body(vol: &Path, ulid: &str) {
        fs::write(vol.join("cache").join(format!("{ulid}.body")), b"body").unwrap();
        fs::write(vol.join("cache").join(format!("{ulid}.present")), b"\xff").unwrap();
    }

    #[test]
    fn evict_bodies_removes_body_and_present_leaves_idx() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        write_stub_body(&vol, ulid);
        fs::write(vol.join("index").join(format!("{ulid}.idx")), b"idx").unwrap();

        assert_eq!(evict_bodies(&vol).unwrap(), 1);
        assert!(!vol.join("cache").join(format!("{ulid}.body")).exists());
        assert!(!vol.join("cache").join(format!("{ulid}.present")).exists());
        assert!(vol.join("index").join(format!("{ulid}.idx")).exists());
    }

    #[test]
    fn evict_bodies_skips_body_without_idx() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let confirmed = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        let unconfirmed = "01AAAAAAAAAAAAAAAAAAAAAAA2";
        write_stub_body(&vol, confirmed);
        write_stub_body(&vol, unconfirmed);
        fs::write(vol.join("index").join(format!("{confirmed}.idx")), b"idx").unwrap();

        assert_eq!(evict_bodies(&vol).unwrap(), 1);
        assert!(!vol.join("cache").join(format!("{confirmed}.body")).exists());
        assert!(
            vol.join("cache")
                .join(format!("{unconfirmed}.body"))
                .exists()
        );
    }

    #[test]
    fn evict_bodies_missing_cache_dir_returns_zero() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(evict_bodies(tmp.path()).unwrap(), 0);
    }

    #[test]
    fn evict_one_body_succeeds_with_idx() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        write_stub_body(&vol, ulid);
        fs::write(vol.join("index").join(format!("{ulid}.idx")), b"idx").unwrap();

        assert_eq!(evict_one_body(&vol, ulid).unwrap(), 1);
        assert!(!vol.join("cache").join(format!("{ulid}.body")).exists());
        assert!(!vol.join("cache").join(format!("{ulid}.present")).exists());
        assert!(vol.join("index").join(format!("{ulid}.idx")).exists());
    }

    #[test]
    fn evict_one_body_refuses_without_idx() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        write_stub_body(&vol, ulid);
        let err = evict_one_body(&vol, ulid).unwrap_err();
        assert!(err.to_string().contains("not S3-confirmed"));
        assert!(vol.join("cache").join(format!("{ulid}.body")).exists());
    }

    #[test]
    fn evict_one_body_refuses_missing_body() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        fs::write(vol.join("index").join(format!("{ulid}.idx")), b"idx").unwrap();
        let err = evict_one_body(&vol, ulid).unwrap_err();
        assert!(err.to_string().contains("not found in cache/"));
    }
}
