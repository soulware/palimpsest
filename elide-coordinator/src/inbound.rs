// Coordinator inbound socket.
//
// Listens on coordinator.sock for commands from the elide CLI.
// Protocol: one request line per connection, one response line, then close.
// Exception: `import attach` streams multiple response lines until done.
//
// Unauthenticated operations (any caller):
//   rescan                    — trigger an immediate fork discovery pass
//   status <volume>           — report running state of a named volume
//   import <name> <ref>       — spawn an OCI import
//   import status <name>      — poll import state by volume name (running / done / failed)
//   import attach <name>      — stream import output by volume name until completion
//   delete <volume>           — stop all processes and remove the volume directory
//   evict <volume> [<ulid>]   — evict all (or one) S3-confirmed segment body from cache/
//
// Volume-process operations (macaroon required — not yet implemented):
//   register <volume> <fork>   — mint a per-fork macaroon (PID-bound)
//   credentials <macaroon>     — exchange macaroon for short-lived S3 creds

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::import::{self, ImportRegistry, ImportState};
use elide_coordinator::{EvictRegistry, SnapshotLockRegistry};

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    socket_path: &Path,
    data_dir: Arc<PathBuf>,
    rescan: Arc<Notify>,
    registry: ImportRegistry,
    elide_import_bin: Arc<PathBuf>,
    evict_registry: EvictRegistry,
    snapshot_locks: SnapshotLockRegistry,
    store: Arc<dyn ObjectStore>,
) {
    let _ = std::fs::remove_file(socket_path);

    let listener = match UnixListener::bind(socket_path) {
        Ok(l) => l,
        Err(e) => {
            warn!("[inbound] failed to bind {}: {e}", socket_path.display());
            return;
        }
    };

    info!("[inbound] listening on {}", socket_path.display());

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let data_dir = data_dir.clone();
                let rescan = rescan.clone();
                let registry = registry.clone();
                let bin = elide_import_bin.clone();
                let evict_reg = evict_registry.clone();
                let snap_locks = snapshot_locks.clone();
                let store = store.clone();
                tokio::spawn(handle(
                    stream, data_dir, rescan, registry, bin, evict_reg, snap_locks, store,
                ));
            }
            Err(e) => warn!("[inbound] accept error: {e}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle(
    stream: tokio::net::UnixStream,
    data_dir: Arc<PathBuf>,
    rescan: Arc<Notify>,
    registry: ImportRegistry,
    elide_import_bin: Arc<PathBuf>,
    evict_registry: EvictRegistry,
    snapshot_locks: SnapshotLockRegistry,
    store: Arc<dyn ObjectStore>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let line = match lines.next_line().await {
        Ok(Some(line)) => line,
        Ok(None) => return,
        Err(e) => {
            warn!("[inbound] read error: {e}");
            return;
        }
    };
    let line = line.trim().to_owned();

    // `import attach` is the one streaming operation — it keeps the connection
    // open and writes lines until the import completes.
    if let Some(name) = line.strip_prefix("import attach ") {
        stream_import_by_name(name.trim(), &data_dir, &mut writer, &registry).await;
        return;
    }

    let response = dispatch(
        &line,
        &data_dir,
        rescan,
        &registry,
        &elide_import_bin,
        &evict_registry,
        &snapshot_locks,
        &store,
    )
    .await;
    let _ = writer.write_all(format!("{response}\n").as_bytes()).await;
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    line: &str,
    data_dir: &Path,
    rescan: Arc<Notify>,
    registry: &ImportRegistry,
    elide_import_bin: &Path,
    evict_registry: &EvictRegistry,
    snapshot_locks: &SnapshotLockRegistry,
    store: &Arc<dyn ObjectStore>,
) -> String {
    if line.is_empty() {
        return "err empty request".to_string();
    }

    let (op, args) = match line.split_once(' ') {
        Some((op, args)) => (op, args.trim()),
        None => (line, ""),
    };

    match op {
        "rescan" => {
            rescan.notify_one();
            "ok".to_string()
        }

        "status" => {
            if args.is_empty() {
                return "err usage: status <volume>".to_string();
            }
            volume_status(args, data_dir)
        }

        "import" => {
            let (sub, sub_args) = match args.split_once(' ') {
                Some((sub, rest)) => (sub, rest.trim()),
                None => (args, ""),
            };
            match sub {
                "status" => {
                    if sub_args.is_empty() {
                        return "err usage: import status <name>".to_string();
                    }
                    import_status_by_name(sub_args, data_dir, registry).await
                }
                _ => {
                    // `import <name> <oci-ref> [extents:<name>[,<name>…]]`:
                    //   sub = volume name
                    //   sub_args = oci-ref [space extents:<name,name…>]
                    if sub_args.is_empty() {
                        return "err usage: import <name> <oci-ref> [extents:<name>[,<name>…]]"
                            .to_string();
                    }
                    let (oci_ref, extents_from) = match sub_args.split_once(' ') {
                        Some((oci, rest)) => match rest.trim().strip_prefix("extents:") {
                            Some(list) if !list.is_empty() => {
                                let names: Vec<String> =
                                    list.split(',').map(|s| s.to_owned()).collect();
                                (oci, names)
                            }
                            _ => {
                                return "err malformed import args; expected extents:<name>[,…]"
                                    .to_string();
                            }
                        },
                        None => (sub_args, Vec::new()),
                    };
                    start_import(
                        sub,
                        oci_ref,
                        &extents_from,
                        data_dir,
                        elide_import_bin,
                        registry,
                        &rescan,
                    )
                    .await
                }
            }
        }

        "delete" => {
            if args.is_empty() {
                return "err usage: delete <volume>".to_string();
            }
            delete_volume(args, data_dir)
        }

        "evict" => {
            if args.is_empty() {
                return "err usage: evict <volume> [<ulid>]".to_string();
            }
            let (vol_name, ulid_str) = match args.split_once(' ') {
                Some((name, ulid)) => (name, Some(ulid.trim().to_owned())),
                None => (args, None),
            };
            evict_volume(vol_name, ulid_str, data_dir, evict_registry).await
        }

        "snapshot" => {
            if args.is_empty() {
                return "err usage: snapshot <volume>".to_string();
            }
            snapshot_volume(args, data_dir, snapshot_locks, store).await
        }

        _ => {
            warn!("[inbound] unexpected op: {op:?}");
            format!("err unknown op: {op}")
        }
    }
}

// ── Import operations ─────────────────────────────────────────────────────────

async fn start_import(
    vol_name: &str,
    oci_ref: &str,
    extents_from: &[String],
    data_dir: &Path,
    elide_import_bin: &Path,
    registry: &ImportRegistry,
    rescan: &Arc<Notify>,
) -> String {
    match import::spawn_import(
        vol_name,
        oci_ref,
        extents_from,
        data_dir,
        elide_import_bin,
        registry,
        rescan.clone(),
    )
    .await
    {
        Ok(ulid) => format!("ok {ulid}"),
        Err(e) => format!("err {e}"),
    }
}

/// Resolve a volume name to its import ULID via `import.lock`, if present.
fn import_ulid_for_volume(name: &str, data_dir: &Path) -> Option<String> {
    let lock_path = data_dir.join("by_name").join(name).join(import::LOCK_FILE);
    std::fs::read_to_string(lock_path)
        .ok()
        .map(|s| s.trim().to_owned())
}

async fn import_status_by_name(name: &str, data_dir: &Path, registry: &ImportRegistry) -> String {
    let vol_dir = data_dir.join("by_name").join(name);
    if !vol_dir.exists() {
        return format!("err volume not found: {name}");
    }
    let Some(ulid) = import_ulid_for_volume(name, data_dir) else {
        // No active import lock — if volume is readonly the import completed.
        if vol_dir.join("volume.readonly").exists() {
            return "ok done".to_string();
        }
        return format!("err no active import for: {name}");
    };
    let job = registry.lock().await.get(&ulid).cloned();
    match job {
        // import.lock exists but not in registry (coordinator restarted mid-import)
        None => "ok running".to_string(),
        Some(job) => match job.state().await {
            ImportState::Running => "ok running".to_string(),
            ImportState::Done => "ok done".to_string(),
            ImportState::Failed(msg) => format!("err failed: {msg}"),
        },
    }
}

/// Stream buffered and live import output to `writer`, closing with a terminal
/// `ok done` or `err failed: <msg>` line when the import completes.
async fn stream_import_by_name(
    name: &str,
    data_dir: &Path,
    writer: &mut OwnedWriteHalf,
    registry: &ImportRegistry,
) {
    let vol_dir = data_dir.join("by_name").join(name);
    if !vol_dir.exists() {
        let _ = writer
            .write_all(format!("err volume not found: {name}\n").as_bytes())
            .await;
        return;
    }
    let Some(ulid) = import_ulid_for_volume(name, data_dir) else {
        // No active import — if volume is readonly the import already completed.
        if vol_dir.join("volume.readonly").exists() {
            let _ = writer.write_all(b"ok done\n").await;
        } else {
            let _ = writer
                .write_all(format!("err no active import for: {name}\n").as_bytes())
                .await;
        }
        return;
    };

    let job = registry.lock().await.get(&ulid).cloned();
    let Some(job) = job else {
        // import.lock exists but not in registry (coordinator restarted mid-import).
        // We can't stream output we never buffered; just report running.
        let _ = writer
            .write_all(b"err import output unavailable (coordinator restarted)\n")
            .await;
        return;
    };

    let mut offset = 0;
    loop {
        let lines = job.read_from(offset).await;
        for line in &lines {
            if writer
                .write_all(format!("{line}\n").as_bytes())
                .await
                .is_err()
            {
                return; // client disconnected
            }
        }
        offset += lines.len();

        match job.state().await {
            ImportState::Done => {
                let _ = writer.write_all(b"ok done\n").await;
                return;
            }
            ImportState::Failed(msg) => {
                let _ = writer
                    .write_all(format!("err failed: {msg}\n").as_bytes())
                    .await;
                return;
            }
            ImportState::Running => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

// ── Volume evict ─────────────────────────────────────────────────────────────

/// Route an eviction request to the fork's task loop.
///
/// The fork task processes the request between drain/GC ticks, ensuring it
/// never races with the GC pass's collect_stats → compact_segments window.
async fn evict_volume(
    vol_name: &str,
    ulid_str: Option<String>,
    data_dir: &Path,
    evict_registry: &EvictRegistry,
) -> String {
    // Resolve name → fork directory path using the same construction as
    // daemon.rs (data_dir.join("by_id/<ulid>")), so the key matches the
    // EvictRegistry entry.  canonicalize() returns an absolute path which
    // would not match when data_dir is relative.
    let link = data_dir.join("by_name").join(vol_name);
    let target = match std::fs::read_link(&link) {
        Ok(t) => t,
        Err(_) => return format!("err volume not found: {vol_name}"),
    };
    // The symlink target is ../by_id/<ulid>; extract just the ULID component.
    let Some(ulid_component) = target.file_name() else {
        return format!("err malformed volume symlink: {vol_name}");
    };
    let fork_dir = data_dir.join("by_id").join(ulid_component);

    // Look up the fork's evict sender.
    let sender = evict_registry.lock().await.get(&fork_dir).cloned();
    let Some(sender) = sender else {
        return format!("err volume not managed by coordinator: {vol_name}");
    };

    // Send the request and wait for the result.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    if sender.send((ulid_str, reply_tx)).await.is_err() {
        return "err fork task no longer running".to_string();
    }
    match reply_rx.await {
        Ok(Ok(n)) => format!("ok {n}"),
        Ok(Err(e)) => format!("err {e}"),
        Err(_) => "err fork task dropped reply".to_string(),
    }
}

// ── Volume snapshot ──────────────────────────────────────────────────────────

/// Coordinator-orchestrated snapshot handler.
///
/// Sequence for the named volume:
///   1. Acquire per-volume snapshot lock (blocks the tick loop for this volume).
///   2. `flush` IPC to the volume → WAL into `pending/`.
///   3. Inline drain of `pending/` via `upload::drain_pending` (uploads to S3
///      and triggers `promote` IPCs that move bodies into `cache/` and
///      materialise `index/<ulid>.idx`).
///   4. Pick `snap_ulid` as the max ULID in `index/` after drain, or a fresh
///      ULID if `index/` is empty.
///   5. `snapshot_manifest <snap_ulid>` IPC → volume writes the signed
///      `snapshots/<snap_ulid>.manifest` file and marker.
///   6. Upload the just-written snapshot files to S3 via
///      `upload::upload_snapshots_and_filemaps`.
///
/// Lock is released when this function returns (via the `Drop` guard on
/// the returned `MutexGuard`).
async fn snapshot_volume(
    vol_name: &str,
    data_dir: &Path,
    snapshot_locks: &SnapshotLockRegistry,
    store: &Arc<dyn ObjectStore>,
) -> String {
    let link = data_dir.join("by_name").join(vol_name);
    let fork_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(_) => return format!("err volume not found: {vol_name}"),
    };
    if !fork_dir.join("control.sock").exists() {
        return format!("err volume '{vol_name}' is not running — start it first");
    }
    if fork_dir.join("volume.readonly").exists() {
        return format!("err volume '{vol_name}' is readonly");
    }

    let volume_id = match elide_coordinator::upload::derive_names(&fork_dir) {
        Ok(id) => id,
        Err(e) => return format!("err deriving volume id: {e}"),
    };

    let lock = elide_coordinator::snapshot_lock_for(snapshot_locks, &fork_dir).await;
    let _guard = lock.lock_owned().await;

    // 1. Flush WAL into pending/.
    if !elide_coordinator::control::flush(&fork_dir).await {
        return "err flush failed or volume unreachable".to_string();
    }

    // 2. Inline drain: upload every pending segment, promote each, then
    //    upload any snapshot files already sitting under snapshots/.
    //    We run this before sign_snapshot_manifest so that index/ is populated
    //    with every segment up to the flush point.
    match elide_coordinator::upload::drain_pending(&fork_dir, &volume_id, store).await {
        Ok(r) if r.failed > 0 => {
            return format!("err drain reported {} failed segment(s)", r.failed);
        }
        Ok(_) => {}
        Err(e) => return format!("err drain: {e:#}"),
    }

    // 3. Pick snap_ulid: the max ULID in index/, or a fresh ULID if empty.
    let snap_ulid = match pick_snapshot_ulid(&fork_dir) {
        Ok(u) => u,
        Err(e) => return format!("err picking snap_ulid: {e}"),
    };

    // 4. Tell the volume to sign and write the manifest + marker.
    if !elide_coordinator::control::sign_snapshot_manifest(&fork_dir, snap_ulid).await {
        return format!("err sign_snapshot_manifest {snap_ulid} failed");
    }

    // 5. Upload the new snapshot marker and manifest.
    if let Err(e) =
        elide_coordinator::upload::upload_snapshots_and_filemaps(&fork_dir, &volume_id, store).await
    {
        return format!("err uploading snapshot files: {e:#}");
    }

    info!("[snapshot {volume_id}] committed {snap_ulid}");
    format!("ok {snap_ulid}")
}

/// Pick a snapshot ULID: the max ULID in `fork_dir/index/`, or a fresh one
/// if `index/` is empty.
fn pick_snapshot_ulid(fork_dir: &Path) -> std::io::Result<ulid::Ulid> {
    let index_dir = fork_dir.join("index");
    let mut latest: Option<ulid::Ulid> = None;
    match std::fs::read_dir(&index_dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(s) = name.to_str() else { continue };
                let Some(stem) = s.strip_suffix(".idx") else {
                    continue;
                };
                if let Ok(u) = ulid::Ulid::from_string(stem)
                    && latest.is_none_or(|cur| u > cur)
                {
                    latest = Some(u);
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    // `Ulid::new()` mints a fresh ULID — not the default (zero) Ulid that
    // `unwrap_or_default` would produce. Both clippy lints point at the same
    // spot, so suppress the unwrap_or_default one locally.
    #[allow(clippy::unwrap_or_default)]
    Ok(latest.unwrap_or_else(ulid::Ulid::new))
}

// ── Volume status ─────────────────────────────────────────────────────────────

fn volume_status(volume_name: &str, data_dir: &Path) -> String {
    // Resolve name via by_name/ symlink → by_id/<ulid>/.
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!("err volume not found: {volume_name}");
    }
    // The OS follows the symlink transparently for all path ops below.
    let vol_dir = link;

    // Check for an active import.
    if vol_dir.join(import::LOCK_FILE).exists() {
        let ulid = std::fs::read_to_string(vol_dir.join(import::LOCK_FILE))
            .unwrap_or_default()
            .trim()
            .to_owned();
        return format!("ok importing {ulid}");
    }

    // Check if a volume process is running.
    if let Ok(text) = std::fs::read_to_string(vol_dir.join("volume.pid"))
        && let Ok(pid) = text.trim().parse::<u32>()
        && pid_is_alive(pid)
    {
        return "ok running".to_string();
    }

    "ok stopped".to_string()
}

// ── Volume delete ─────────────────────────────────────────────────────────────

fn delete_volume(volume_name: &str, data_dir: &Path) -> String {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!("err volume not found: {volume_name}");
    }

    // Resolve the symlink to get the actual by_id/<ulid>/ directory.
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    import::kill_all_for_volume(&vol_dir);

    // Remove the by_name symlink first, then the volume directory.
    let _ = std::fs::remove_file(&link);

    match std::fs::remove_dir_all(&vol_dir) {
        Ok(()) => {
            info!("[inbound] deleted volume {volume_name}");
            "ok".to_string()
        }
        Err(e) => format!("err delete failed: {e}"),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn pid_is_alive(pid: u32) -> bool {
    let Ok(raw) = i32::try_from(pid) else {
        return false;
    };
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(raw), None).is_ok()
}
