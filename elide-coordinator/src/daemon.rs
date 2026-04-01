// Coordinator daemon: watches the configured data directory, discovers volumes,
// drains pending segments to S3, runs GC, and supervises volume processes.
//
// Architecture:
//   - A root scanner runs every `scan_interval_secs`, walking configured root
//     directories to find volume directories. Each newly-discovered volume gets:
//       - a fork_loop task: drain + GC, sequential within each tick
//       - a supervisor task (if `serve.toml` exists): spawns and restarts
//         `elide serve-volume`
//   - The first scan fires immediately at startup to clear any backlog and
//     adopt any volume processes already running.
//   - Shutdown on Ctrl+C: all tasks are aborted. Volume processes are detached
//     (setsid) and continue running after the coordinator exits.
//
// Per-tick sequencing (fork_loop):
//   1. Upload pending/ segments to S3 (drain_pending)
//   2. If gc_interval has elapsed: apply done handoffs, run GC pass (gc_fork)
//
// Drain and GC are sequential within each tick so that GC sees all segments
// uploaded this tick, and concurrent GC rewrites can never race an upload.
//
// Directory layout expected:
//   <data_dir>/by_id/<ulid>/    — one directory per volume (ULID name = S3 prefix)
//   <data_dir>/by_name/<name>   — symlinks into by_id/ for human navigation

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use object_store::ObjectStore;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, warn};

use crate::config::CoordinatorConfig;
use crate::control;
use crate::gc;
use crate::import;
use crate::inbound;
use crate::supervisor;
use crate::upload;

pub async fn run(config: CoordinatorConfig, store: Arc<dyn ObjectStore>) -> Result<()> {
    let drain_interval = Duration::from_secs(config.drain.interval_secs);
    let scan_interval = Duration::from_secs(config.drain.scan_interval_secs);
    let elide_bin = config.elide_bin.clone();
    let elide_import_bin = Arc::new(config.elide_import_bin.clone());
    let gc_config = config.gc.clone();
    let socket_path = config.resolved_socket_path();
    let data_dir = Arc::new(config.data_dir.clone());

    info!(
        "[coordinator] data_dir: {}; drain every {}s, scan every {}s; elide bin: {}",
        data_dir.display(),
        config.drain.interval_secs,
        config.drain.scan_interval_secs,
        elide_bin.display(),
    );

    std::fs::create_dir_all(data_dir.as_ref())
        .with_context(|| format!("creating data_dir: {}", data_dir.display()))?;
    std::fs::create_dir_all(data_dir.join("by_id"))
        .with_context(|| format!("creating by_id dir under {}", data_dir.display()))?;
    std::fs::create_dir_all(data_dir.join("by_name"))
        .with_context(|| format!("creating by_name dir under {}", data_dir.display()))?;

    // Reconcile by_name/ against by_id/: remove symlinks whose target no longer
    // exists, add missing symlinks for volumes that have a volume.name file.
    reconcile_by_name(&data_dir);

    // Clean up any stale import locks left by a previous coordinator run.
    import::cleanup_stale_locks(&config.data_dir);

    // Shared notify: inbound socket triggers this to request an immediate rescan.
    let rescan_notify = Arc::new(Notify::new());

    // Import job registry: tracks running and recently-completed import jobs.
    let import_registry = import::new_registry();

    // Spawn the inbound socket server.
    {
        let data_dir = data_dir.clone();
        let notify = rescan_notify.clone();
        let registry = import_registry.clone();
        let bin = elide_import_bin.clone();
        tokio::spawn(async move {
            inbound::serve(&socket_path, data_dir, notify, registry, bin).await;
        });
    }

    let mut known: HashSet<PathBuf> = HashSet::new();
    let mut tasks: JoinSet<()> = JoinSet::new();

    let mut scan_tick = tokio::time::interval(scan_interval);
    scan_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Consume the first tick so the loop body runs immediately at startup.
    scan_tick.tick().await;

    loop {
        let volumes = discover_volumes(&data_dir);
        for vol_dir in volumes {
            // Skip volumes that are being actively imported — they will be picked
            // up on the next scan pass once the import.lock is removed.
            if vol_dir.join(import::LOCK_FILE).exists() {
                continue;
            }
            if known.insert(vol_dir.clone()) {
                info!("[coordinator] discovered volume: {}", vol_dir.display());

                tasks.spawn(fork_loop(
                    vol_dir.clone(),
                    store.clone(),
                    drain_interval,
                    gc_config.clone(),
                ));

                tasks.spawn(supervisor::supervise(vol_dir, elide_bin.clone()));
            }
        }

        // Reap any tasks that have exited (e.g. fork directory removed).
        while tasks.try_join_next().is_some() {}

        tokio::select! {
            _ = scan_tick.tick() => {}
            _ = rescan_notify.notified() => {
                info!("[coordinator] rescan triggered via socket");
            }
            _ = tokio::signal::ctrl_c() => {
                info!("[coordinator] shutting down (foreground mode)");
                // Abort supervisor and drain tasks first so they cannot
                // interfere with or restart processes we are about to stop.
                tasks.abort_all();

                // SIGTERM every volume and import process across all known volumes.
                let all_pids: Vec<u32> = known
                    .iter()
                    .flat_map(|vol_dir| import::terminate_fork_processes(vol_dir))
                    .collect();

                if all_pids.is_empty() {
                    info!("[coordinator] no processes to stop");
                } else {
                    info!(
                        "[coordinator] waiting for {} process(es) to exit...",
                        all_pids.len()
                    );
                    wait_for_pids(&all_pids, Duration::from_secs(10)).await;
                    info!("[coordinator] done");
                }

                break;
            }
        }
    }

    Ok(())
}

/// Poll until all pids have exited or the timeout elapses.
///
/// Logs a warning if any processes are still alive after the timeout.
async fn wait_for_pids(pids: &[u32], timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let still_alive: Vec<u32> = pids
            .iter()
            .copied()
            .filter(|&pid| {
                if let Ok(raw) = i32::try_from(pid) {
                    nix::sys::signal::kill(nix::unistd::Pid::from_raw(raw), None).is_ok()
                } else {
                    false
                }
            })
            .collect();

        if still_alive.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(
                "[coordinator] shutdown timed out; {} process(es) still running: {:?}",
                still_alive.len(),
                still_alive
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Sequential drain + GC loop for a single fork.
///
/// Each tick:
///   1. Upload any pending segments to the object store.
///   2. If the GC interval has elapsed, run a GC pass on uploaded segments.
///
/// Drain and GC are sequential within each tick so that GC sees a stable
/// `segments/` directory that already contains all segments uploaded this
/// tick, and uploads are never racing with a concurrent GC rewrite.
///
/// Exits if the fork directory is removed.
async fn fork_loop(
    fork_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    drain_interval: Duration,
    gc_config: crate::config::GcConfig,
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

    let gc_interval = Duration::from_secs(gc_config.interval_secs);

    // Run GC on the first tick to clear any backlog from a previous run.
    let mut last_gc = Instant::now()
        .checked_sub(gc_interval)
        .unwrap_or_else(Instant::now);

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
        if fork_dir.join(import::LOCK_FILE).exists() {
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
        if fork_dir.join("pending").exists() {
            match upload::drain_pending(&fork_dir, &volume_id, &store).await {
                Ok(r) if r.uploaded > 0 || r.failed > 0 => {
                    info!(
                        "[drain {volume_id}] {} uploaded, {} failed",
                        r.uploaded, r.failed
                    );
                }
                Ok(_) => {}
                Err(e) => warn!("[drain {volume_id}] error: {e:#}"),
            }
        }

        // Step 5: GC pass (rate-limited to gc_interval).
        if last_gc.elapsed() >= gc_interval {
            // Get two GC output ULIDs from the volume (flushes WAL as a side
            // effect).  If the volume is unreachable, skip GC for this tick.
            let Some((repack_ulid, sweep_ulid)) = control::gc_checkpoint(&fork_dir).await else {
                last_gc = Instant::now();
                continue;
            };

            // Process any handoffs the volume has acknowledged (.applied) before
            // running a new pass, so old segments are removed from the index first.
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
                &repack_ulid,
                &sweep_ulid,
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

/// Scan `<data_dir>/by_id/` and return all writable volume directories.
///
/// Skips:
///   - entries whose name is not a valid ULID
///   - volumes with a `volume.readonly` marker
///   - volumes with no `pending/` or `segments/` subdirectory (not yet initialised)
fn discover_volumes(data_dir: &Path) -> Vec<PathBuf> {
    let by_id_dir = data_dir.join("by_id");
    let mut volumes = Vec::new();
    let entries = match std::fs::read_dir(&by_id_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return volumes,
        Err(e) => {
            warn!(
                "[coordinator] cannot read by_id dir {}: {e}",
                by_id_dir.display()
            );
            return volumes;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if ulid::Ulid::from_string(name).is_err() {
            continue;
        }
        if path.join("volume.readonly").exists() {
            continue;
        }
        if !path.join("pending").exists() && !path.join("segments").exists() {
            continue;
        }
        volumes.push(path);
    }
    volumes
}

/// Reconcile `<data_dir>/by_name/` against `<data_dir>/by_id/`.
///
/// - Removes stale symlinks whose target ULID no longer exists in `by_id/`.
/// - Adds missing symlinks for volumes that have a `volume.name` file but no
///   corresponding `by_name/` entry.
fn reconcile_by_name(data_dir: &Path) {
    let by_id_dir = data_dir.join("by_id");
    let by_name_dir = data_dir.join("by_name");

    // Remove stale symlinks.
    if let Ok(entries) = std::fs::read_dir(&by_name_dir) {
        for entry in entries.flatten() {
            let link = entry.path();
            // Check that the symlink target still exists.
            if !link.exists() {
                let _ = std::fs::remove_file(&link);
                info!(
                    "[coordinator] removed stale by_name symlink: {}",
                    link.display()
                );
            }
        }
    }

    // Add missing symlinks.
    let Ok(entries) = std::fs::read_dir(&by_id_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let vol_dir = entry.path();
        if !vol_dir.is_dir() {
            continue;
        }
        let Some(ulid_str) = vol_dir.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if ulid::Ulid::from_string(ulid_str).is_err() {
            continue;
        }
        let Ok(name) = std::fs::read_to_string(vol_dir.join("volume.name")) else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let link = by_name_dir.join(name);
        if !link.exists() {
            let target = format!("../by_id/{ulid_str}");
            if let Err(e) = std::os::unix::fs::symlink(&target, &link) {
                warn!("[coordinator] failed to create by_name/{name} -> {target}: {e}");
            } else {
                info!("[coordinator] created by_name/{name} -> {target}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn mk(root: &Path, rel: &str) {
        std::fs::create_dir_all(root.join(rel)).unwrap();
    }

    #[test]
    fn discover_volumes_scans_by_id() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // Valid writable volumes in by_id/.
        let ulid1 = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let ulid2 = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        let ulid3 = "01JQCCCCCCCCCCCCCCCCCCCCCC";
        mk(root, &format!("by_id/{ulid1}/segments"));
        mk(root, &format!("by_id/{ulid2}/pending"));

        // Readonly volume — must be skipped.
        mk(root, &format!("by_id/{ulid3}/segments"));
        std::fs::write(root.join(format!("by_id/{ulid3}/volume.readonly")), "").unwrap();

        // Non-ULID entry — must be skipped.
        mk(root, "by_id/not-a-ulid/segments");

        // ULID dir with no pending/ or segments/ — not yet initialised, skip.
        let ulid4 = "01JQDDDDDDDDDDDDDDDDDDDDDDD";
        mk(root, &format!("by_id/{ulid4}"));

        let volumes = discover_volumes(root);
        let mut names: Vec<String> = volumes
            .iter()
            .map(|p| p.strip_prefix(root).unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();

        assert_eq!(
            names,
            vec![format!("by_id/{ulid1}"), format!("by_id/{ulid2}"),]
        );
    }
}
