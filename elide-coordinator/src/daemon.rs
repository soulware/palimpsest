// Coordinator daemon: watches the configured data directory, discovers volumes,
// drains pending segments to S3, runs GC, and supervises volume processes.
//
// Architecture:
//   - A root scanner runs every `scan_interval_secs`, walking configured root
//     directories to find volume directories. Each newly-discovered volume gets:
//       - a run_volume_tasks task: drain + GC, sequential within each tick
//       - a supervisor task (if `serve.toml` exists): spawns and restarts
//         `elide serve-volume`
//   - The first scan fires immediately at startup to clear any backlog and
//     adopt any volume processes already running.
//   - Shutdown on Ctrl+C: all tasks are aborted. Volume processes are detached
//     (setsid) and continue running after the coordinator exits.
//
// Directory layout expected:
//   <data_dir>/by_id/<ulid>/    — one directory per volume (ULID name = S3 prefix)
//   <data_dir>/by_name/<name>   — symlinks into by_id/ for human navigation

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::config::CoordinatorConfig;
use crate::import;
use crate::inbound;
use crate::supervisor;

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

    // Maps each known volume path to its directory inode.  The inode detects
    // delete-and-recreate: if the same path reappears with a different inode
    // (e.g. `volume delete` followed by `volume remote pull` of the same ULID),
    // we treat it as a new discovery and spawn fresh tasks.
    let mut known: HashMap<PathBuf, u64> = HashMap::new();
    let mut tasks: JoinSet<()> = JoinSet::new();

    let mut scan_tick = tokio::time::interval(scan_interval);
    scan_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Consume the first tick so the loop body runs immediately at startup.
    scan_tick.tick().await;

    loop {
        // Prune volumes that have been deleted since we last saw them so that
        // if the same ULID directory is recreated (e.g. by `volume remote pull`
        // after a `volume delete`), it will be discovered and processed again.
        // Prune volumes whose path no longer exists OR whose directory inode
        // changed (deleted and recreated at the same path with the same ULID).
        known.retain(|p, ino| p.metadata().map(|m| m.ino() == *ino).unwrap_or(false));

        let volumes = discover_volumes(&data_dir);
        for vol_dir in volumes {
            // Skip volumes that are being actively imported — they will be picked
            // up on the next scan pass once the import.lock is removed.
            if vol_dir.join(import::LOCK_FILE).exists() {
                continue;
            }
            let vol_ino = vol_dir.metadata().map(|m| m.ino()).unwrap_or(0);
            if known
                .insert(vol_dir.clone(), vol_ino)
                .is_none_or(|old| old != vol_ino)
            {
                let label = volume_label(&vol_dir);
                info!("[coordinator] discovered volume: {label}");

                tasks.spawn(elide_coordinator::tasks::run_volume_tasks(
                    vol_dir.clone(),
                    store.clone(),
                    drain_interval,
                    gc_config.clone(),
                ));

                // Readonly volumes (imported bases) have no live process —
                // skip supervision so we don't crash-loop on serve-volume.
                if !vol_dir.join("volume.readonly").exists() {
                    tasks.spawn(supervisor::supervise(vol_dir, elide_bin.clone()));
                }
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
                    .keys()
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

                // Clean up pid files now that processes have exited. The
                // supervisor tasks were aborted above and cannot do this
                // themselves.
                for vol_dir in known.keys() {
                    let _ = std::fs::remove_file(vol_dir.join("volume.pid"));
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

/// Scan `<data_dir>/by_id/` and return volume directories that need coordinator
/// attention (drain, GC, supervision, or prefetch).
///
/// Skips:
///   - entries whose name is not a valid ULID
///   - volumes with no `pending/` or `segments/` subdirectory (not yet initialised)
///   - readonly volumes that are fully indexed: either `segments/` is non-empty
///     (drain completed) or `index/` is non-empty (prefetch completed)
///
/// Readonly volumes with `pending/` are included for drain. Readonly volumes
/// with empty `segments/` and no `index/` are included for prefetch (pulled
/// from the store but not yet indexed locally).
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
        let has_pending = path.join("pending").exists();
        let has_segments = path.join("segments").exists();
        if !has_pending && !has_segments {
            continue;
        }
        // Skip readonly volumes that are already fully indexed locally —
        // either segments/ is non-empty (drain completed) or index/ is
        // non-empty (prefetch completed). Include volumes with pending/ (need
        // drain) or with both empty (pulled but not yet prefetched).
        if path.join("volume.readonly").exists() && !has_pending {
            let dir_non_empty = |sub: &str| {
                path.join(sub)
                    .read_dir()
                    .map(|mut d| d.next().is_some())
                    .unwrap_or(false)
            };
            if dir_non_empty("segments") || dir_non_empty("index") {
                continue;
            }
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

    // Remove stale symlinks: entries that are symlinks but whose target no
    // longer exists.  Non-symlink entries (real directories, files) are left
    // alone and warned about — they indicate unexpected manual changes.
    if let Ok(entries) = std::fs::read_dir(&by_name_dir) {
        for entry in entries.flatten() {
            let link = entry.path();
            if !link.is_symlink() {
                warn!(
                    "[coordinator] by_name/{} is not a symlink; skipping",
                    entry.file_name().to_string_lossy()
                );
                continue;
            }
            // Broken symlink: target no longer exists.
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
        let Some(name) = elide_coordinator::tasks::read_volume_name(&vol_dir) else {
            continue;
        };
        let name = name.as_str();
        let link = by_name_dir.join(name);
        if link.is_symlink() || link.exists() {
            // Already present (symlink or unexpected non-symlink); leave it.
            continue;
        }
        let target = format!("../by_id/{ulid_str}");
        if let Err(e) = std::os::unix::fs::symlink(&target, &link) {
            warn!("[coordinator] failed to create by_name/{name} -> {target}: {e}");
        } else {
            info!("[coordinator] created by_name/{name} -> {target}");
        }
    }
}

/// Human-readable label for log messages: "name (ulid)" if the volume has a
/// name, otherwise just the path.
fn volume_label(fork_dir: &Path) -> String {
    let path_str = fork_dir.display().to_string();
    match elide_coordinator::tasks::read_volume_name(fork_dir) {
        Some(name) => {
            if let Some(ulid) = fork_dir.file_name().and_then(|n| n.to_str()) {
                format!("{name} ({ulid})")
            } else {
                format!("{name} ({path_str})")
            }
        }
        None => path_str,
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
        mk(root, &format!("by_id/{ulid1}/segments"));
        mk(root, &format!("by_id/{ulid2}/pending"));

        // Readonly volume with non-empty segments/ — fully drained, must be skipped.
        let ulid3 = "01JQCCCCCCCCCCCCCCCCCCCCCC";
        mk(root, &format!("by_id/{ulid3}/segments"));
        std::fs::write(
            root.join(format!("by_id/{ulid3}/segments/01JQAAAAAAAAAAAAAAAAAAAAA1")),
            "",
        )
        .unwrap();
        std::fs::write(root.join(format!("by_id/{ulid3}/volume.readonly")), "").unwrap();

        // Readonly volume with non-empty index/ — prefetch done, must be skipped.
        let ulid6 = "01JQFFFFFFFFFFFFFFFFFFFFFF";
        mk(root, &format!("by_id/{ulid6}/segments"));
        mk(root, &format!("by_id/{ulid6}/index"));
        std::fs::write(
            root.join(format!(
                "by_id/{ulid6}/index/01JQAAAAAAAAAAAAAAAAAAAAA1.idx"
            )),
            "",
        )
        .unwrap();
        std::fs::write(root.join(format!("by_id/{ulid6}/volume.readonly")), "").unwrap();

        // Readonly volume with empty segments/ and no cache/ — pulled, needs prefetch.
        let ulid5 = "01JQEEEEEEEEEEEEEEEEEEEEEE";
        mk(root, &format!("by_id/{ulid5}/segments"));
        std::fs::write(root.join(format!("by_id/{ulid5}/volume.readonly")), "").unwrap();

        // Readonly volume with pending/ — newly imported, must be included for drain.
        let ulid7 = "01JQGGGGGGGGGGGGGGGGGGGGGG";
        mk(root, &format!("by_id/{ulid7}/pending"));
        std::fs::write(root.join(format!("by_id/{ulid7}/volume.readonly")), "").unwrap();

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
            vec![
                format!("by_id/{ulid1}"),
                format!("by_id/{ulid2}"),
                format!("by_id/{ulid5}"),
                format!("by_id/{ulid7}"),
            ]
        );
    }
}
