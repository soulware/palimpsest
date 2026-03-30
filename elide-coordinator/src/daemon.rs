// Coordinator daemon: watches configured volume roots, discovers forks,
// drains pending segments to S3, and supervises volume processes.
//
// Architecture:
//   - A root scanner runs every `scan_interval_secs`, walking configured root
//     directories to find fork directories. Each newly-discovered fork gets:
//       - a drain task: polls `pending/` every `interval_secs` and uploads
//       - a supervisor task (if `serve.toml` exists): spawns and restarts
//         `elide serve-volume`
//   - The first scan fires immediately at startup to clear any backlog and
//     adopt any volume processes already running.
//   - Shutdown on Ctrl+C: all tasks are aborted. Volume processes are detached
//     (setsid) and continue running after the coordinator exits.
//
// Fork directory layout expected:
//   <root>/<volume>/base/            — base fork
//   <root>/<volume>/forks/<name>/    — named fork

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use object_store::ObjectStore;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::config::CoordinatorConfig;
use crate::gc;
use crate::serve_config;
use crate::supervisor;
use crate::upload;

pub async fn run(config: CoordinatorConfig, store: Arc<dyn ObjectStore>) -> Result<()> {
    let drain_interval = Duration::from_secs(config.drain.interval_secs);
    let scan_interval = Duration::from_secs(config.drain.scan_interval_secs);
    let elide_bin = config.elide_bin.clone();
    let gc_config = config.gc.clone();

    info!(
        "[coordinator] watching {} root(s); drain every {}s, scan every {}s; elide bin: {}",
        config.roots.len(),
        config.drain.interval_secs,
        config.drain.scan_interval_secs,
        elide_bin.display(),
    );
    for root in &config.roots {
        info!("[coordinator] root: {}", root.display());
    }

    let mut known: HashSet<PathBuf> = HashSet::new();
    let mut tasks: JoinSet<()> = JoinSet::new();

    let mut scan_tick = tokio::time::interval(scan_interval);
    scan_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Consume the first tick so the loop body runs immediately at startup.
    scan_tick.tick().await;

    loop {
        let forks = discover_forks(&config.roots);
        for fork_dir in forks {
            if known.insert(fork_dir.clone()) {
                info!("[coordinator] discovered fork: {}", fork_dir.display());

                // Always drain pending segments.
                tasks.spawn(drain_loop(fork_dir.clone(), store.clone(), drain_interval));

                // Always run GC on uploaded segments.
                let (vol_id, frk_name) = match upload::derive_names(&fork_dir) {
                    Ok(names) => names,
                    Err(e) => {
                        warn!(
                            "[coordinator] cannot derive names for gc {}: {e}",
                            fork_dir.display()
                        );
                        continue;
                    }
                };
                // In production the coordinator will call gc_checkpoint on
                // the volume process via IPC.  For now, Ulid::new() is a
                // safe placeholder: GC inputs are always old segments whose
                // timestamps are seconds to minutes behind the current clock,
                // so a fresh ULID is guaranteed to sort between them and any
                // concurrent write.
                let checkpoint: Arc<dyn Fn() -> String + Send + Sync> =
                    Arc::new(|| ulid::Ulid::new().to_string());
                tasks.spawn(gc::gc_loop(
                    fork_dir.clone(),
                    vol_id,
                    frk_name,
                    store.clone(),
                    gc_config.clone(),
                    checkpoint,
                ));

                // Supervise if serve.toml is present.
                match serve_config::load(&fork_dir) {
                    Ok(Some(sc)) => {
                        info!(
                            "[coordinator] supervising fork: {} (bind {})",
                            fork_dir.display(),
                            sc.bind,
                        );
                        tasks.spawn(supervisor::supervise(fork_dir, sc, elide_bin.clone()));
                    }
                    Ok(None) => {}
                    Err(e) => warn!(
                        "[coordinator] failed to read serve.toml for {}: {e}",
                        fork_dir.display()
                    ),
                }
            }
        }

        // Reap any tasks that have exited (e.g. fork directory removed).
        while tasks.try_join_next().is_some() {}

        tokio::select! {
            _ = scan_tick.tick() => {}
            _ = tokio::signal::ctrl_c() => {
                info!("[coordinator] shutting down");
                tasks.abort_all();
                break;
            }
        }
    }

    Ok(())
}

/// Drain pending segments for a single fork, polling every `interval`.
/// Exits if the fork directory is removed.
async fn drain_loop(fork_dir: PathBuf, store: Arc<dyn ObjectStore>, interval: Duration) {
    let (volume_id, fork_name) = match upload::derive_names(&fork_dir) {
        Ok(names) => names,
        Err(e) => {
            warn!(
                "[coordinator] cannot derive names for {}: {e}",
                fork_dir.display()
            );
            return;
        }
    };

    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;

        if !fork_dir.exists() {
            info!(
                "[coordinator] fork removed, stopping drain: {}",
                fork_dir.display()
            );
            break;
        }

        let pending = fork_dir.join("pending");
        if !pending.exists() {
            continue;
        }

        match upload::drain_pending(&fork_dir, &volume_id, &fork_name, &store).await {
            Ok(r) if r.uploaded > 0 || r.failed > 0 => {
                info!(
                    "[drain {}/{}] {} uploaded, {} failed",
                    volume_id, fork_name, r.uploaded, r.failed
                );
            }
            Ok(_) => {}
            Err(e) => {
                warn!("[drain {}/{}] error: {e:#}", volume_id, fork_name);
            }
        }
    }
}

/// Scan configured root directories and return all fork directories found.
///
/// A fork directory is any directory that contains a `pending/` or `segments/`
/// subdirectory. Forks live at:
///   `<root>/<volume>/base/`
///   `<root>/<volume>/forks/<name>/`
fn discover_forks(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut forks = Vec::new();
    for root in roots {
        let entries = match std::fs::read_dir(root) {
            Ok(e) => e,
            Err(e) => {
                warn!("[coordinator] cannot read root {}: {e}", root.display());
                continue;
            }
        };
        for vol_entry in entries.flatten() {
            let vol_path = vol_entry.path();
            if !vol_path.is_dir() {
                continue;
            }

            // base/ fork lives directly under the volume root.
            let base = vol_path.join("base");
            if is_fork_dir(&base) {
                forks.push(base);
            }

            // Named forks live under forks/<name>/.
            let forks_dir = vol_path.join("forks");
            if let Ok(fork_entries) = std::fs::read_dir(&forks_dir) {
                for fork_entry in fork_entries.flatten() {
                    let fork_path = fork_entry.path();
                    if fork_path.is_dir() && is_fork_dir(&fork_path) {
                        forks.push(fork_path);
                    }
                }
            }
        }
    }
    forks
}

/// A directory is a fork if it contains `pending/` or `segments/`.
fn is_fork_dir(path: &Path) -> bool {
    path.is_dir() && (path.join("pending").exists() || path.join("segments").exists())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn mk(root: &Path, rel: &str) {
        std::fs::create_dir_all(root.join(rel)).unwrap();
    }

    #[test]
    fn discover_base_and_named_forks() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // Volume "myvol" with base/ and two named forks.
        mk(root, "myvol/base/pending");
        mk(root, "myvol/forks/vm1/segments");
        mk(root, "myvol/forks/vm2/pending");

        // Volume "other" with only a bare base/ dir (no pending or segments — not a fork).
        mk(root, "other/base");

        // Volume "empty" — just a volume root, no forks.
        mk(root, "empty");

        let forks = discover_forks(&[root.to_path_buf()]);
        let mut names: Vec<String> = forks
            .iter()
            .map(|p| p.strip_prefix(root).unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();

        assert_eq!(
            names,
            vec!["myvol/base", "myvol/forks/vm1", "myvol/forks/vm2"]
        );
    }

    #[test]
    fn is_fork_dir_requires_pending_or_segments() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        let fork = root.join("fork");
        std::fs::create_dir_all(&fork).unwrap();
        assert!(!is_fork_dir(&fork));

        std::fs::create_dir_all(fork.join("pending")).unwrap();
        assert!(is_fork_dir(&fork));
    }
}
