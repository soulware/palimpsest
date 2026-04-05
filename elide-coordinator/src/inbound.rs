// Coordinator inbound socket.
//
// Listens on coordinator.sock for commands from the elide CLI.
// Protocol: one request line per connection, one response line, then close.
// Exception: `import attach` streams multiple response lines until done.
//
// Unauthenticated operations (any caller):
//   rescan                — trigger an immediate fork discovery pass
//   status <volume>       — report running state of a named volume
//   import <name> <ref>   — spawn an OCI import
//   import status <name>  — poll import state by volume name (running / done / failed)
//   import attach <name>  — stream import output by volume name until completion
//   delete <volume>       — stop all processes and remove the volume directory
//
// Volume-process operations (macaroon required — not yet implemented):
//   register <volume> <fork>   — mint a per-fork macaroon (PID-bound)
//   credentials <macaroon>     — exchange macaroon for short-lived S3 creds

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::import::{self, ImportRegistry, ImportState};

pub async fn serve(
    socket_path: &Path,
    data_dir: Arc<PathBuf>,
    rescan: Arc<Notify>,
    registry: ImportRegistry,
    elide_import_bin: Arc<PathBuf>,
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
                tokio::spawn(handle(stream, data_dir, rescan, registry, bin));
            }
            Err(e) => warn!("[inbound] accept error: {e}"),
        }
    }
}

async fn handle(
    stream: tokio::net::UnixStream,
    data_dir: Arc<PathBuf>,
    rescan: Arc<Notify>,
    registry: ImportRegistry,
    elide_import_bin: Arc<PathBuf>,
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

    let response = dispatch(&line, &data_dir, &rescan, &registry, &elide_import_bin).await;
    let _ = writer.write_all(format!("{response}\n").as_bytes()).await;
}

async fn dispatch(
    line: &str,
    data_dir: &Path,
    rescan: &Notify,
    registry: &ImportRegistry,
    elide_import_bin: &Path,
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
                    // `import <name> <oci-ref>`: sub = name, sub_args = oci-ref
                    if sub_args.is_empty() {
                        return "err usage: import <name> <oci-ref>".to_string();
                    }
                    start_import(sub, sub_args, data_dir, elide_import_bin, registry).await
                }
            }
        }

        "delete" => {
            if args.is_empty() {
                return "err usage: delete <volume>".to_string();
            }
            delete_volume(args, data_dir)
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
    data_dir: &Path,
    elide_import_bin: &Path,
    registry: &ImportRegistry,
) -> String {
    match import::spawn_import(vol_name, oci_ref, data_dir, elide_import_bin, registry).await {
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
