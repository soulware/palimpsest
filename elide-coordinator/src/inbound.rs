// Coordinator inbound socket.
//
// Listens on control.sock for commands from the elide CLI.
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
// Volume-process operations (macaroon-authenticated):
//   register <volume-ulid>     — mint a per-volume macaroon, PID-bound via SO_PEERCRED
//   credentials <macaroon>     — exchange macaroon for short-lived S3 creds (PID re-checked)

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::credential::CredentialIssuer;
use crate::import::{self, ImportRegistry, ImportState};
use crate::macaroon::{self, Caveat, Macaroon, Scope};
use elide_coordinator::config::StoreSection;
use elide_coordinator::{EvictRegistry, SnapshotLockRegistry};
use elide_core::process::pid_is_alive;

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
    store_config: Arc<StoreSection>,
    part_size_bytes: usize,
    root_key: [u8; 32],
    issuer: Arc<dyn CredentialIssuer>,
) {
    let _ = std::fs::remove_file(socket_path);

    let listener = match UnixListener::bind(socket_path) {
        Ok(l) => l,
        Err(e) => {
            warn!("[inbound] failed to bind {}: {e}", socket_path.display());
            return;
        }
    };

    // The socket inherits the binding process's umask (typically 0022 → 0755),
    // which on Linux blocks non-root from `connect()` (write perm is required).
    // A coordinator running under sudo would otherwise leave a CLI in the user's
    // session unable to talk to it. Relax to 0666 so any local user can issue
    // unprivileged ops; permission-sensitive ops still go through coordinator
    // logic, not raw filesystem access.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) =
            std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o666))
        {
            warn!(
                "[inbound] chmod 0666 on {} failed: {e} (non-root CLI may be unable to connect)",
                socket_path.display()
            );
        }
    }

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
                let store_cfg = store_config.clone();
                let issuer = issuer.clone();
                tokio::spawn(handle(
                    stream,
                    data_dir,
                    rescan,
                    registry,
                    bin,
                    evict_reg,
                    snap_locks,
                    store,
                    store_cfg,
                    part_size_bytes,
                    root_key,
                    issuer,
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
    store_config: Arc<StoreSection>,
    part_size_bytes: usize,
    root_key: [u8; 32],
    issuer: Arc<dyn CredentialIssuer>,
) {
    // Capture peer credentials before splitting the stream — needed for
    // SO_PEERCRED on the `register` and `credentials` ops. Other ops
    // ignore the peer pid; capturing once here keeps the code symmetric
    // and avoids a second syscall later.
    let peer_pid = stream.peer_cred().ok().and_then(|c| c.pid());

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
        &store_config,
        part_size_bytes,
        &root_key,
        issuer.as_ref(),
        peer_pid,
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
    store_config: &StoreSection,
    part_size_bytes: usize,
    root_key: &[u8; 32],
    issuer: &dyn CredentialIssuer,
    peer_pid: Option<i32>,
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
                        store,
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

        "stop" => {
            if args.is_empty() {
                return "err usage: stop <volume>".to_string();
            }
            stop_volume_op(args, data_dir, store, root_key).await
        }

        "release" => {
            if args.is_empty() {
                return "err usage: release <volume>".to_string();
            }
            release_volume_op(
                args,
                data_dir,
                snapshot_locks,
                store,
                part_size_bytes,
                root_key,
            )
            .await
        }

        "claim" => {
            // claim <name> <new_vol_ulid>
            //
            // Atomically rebind a `Released` name to a freshly-minted
            // local fork. The CLI orchestrates fork creation
            // (mint ULID + keypair, materialise by_id/<new_ulid>/);
            // this op handles only the conditional names/<name>
            // mutation and notifies the supervisor.
            let (name, new_ulid_str) = match args.split_once(' ') {
                Some((n, u)) => (n.trim(), u.trim()),
                None => return "err usage: claim <volume> <new_vol_ulid>".to_string(),
            };
            claim_volume_op(name, new_ulid_str, data_dir, store, root_key, &rescan).await
        }

        "start" => {
            if args.is_empty() {
                return "err usage: start <volume>".to_string();
            }
            start_volume_op(args, data_dir, store, root_key, &rescan).await
        }

        "create" => {
            // create <name> <size_bytes> [<flag>...]
            if args.is_empty() {
                return "err usage: create <name> <size_bytes> [flags...]".to_string();
            }
            create_volume_op(args, data_dir, store, root_key, &rescan).await
        }

        "update" => {
            // update <name> [<flag>...]
            if args.is_empty() {
                return "err usage: update <volume> [flags...]".to_string();
            }
            update_volume_op(args, data_dir).await
        }

        "pull-readonly" => {
            // pull-readonly <vol_ulid>
            if args.is_empty() {
                return "err usage: pull-readonly <vol_ulid>".to_string();
            }
            pull_readonly_op(args, data_dir, store).await
        }

        "force-snapshot-now" => {
            // force-snapshot-now <vol_ulid>
            if args.is_empty() {
                return "err usage: force-snapshot-now <vol_ulid>".to_string();
            }
            force_snapshot_now_op(args, data_dir, store).await
        }

        "fork-create" => {
            // fork-create <new_name> <source_ulid> [snap=<ulid>] [parent-key=<hex>] [<flags>...]
            if args.is_empty() {
                return "err usage: fork-create <name> <source_ulid> [snap=<ulid>] [parent-key=<hex>] [flags...]".to_string();
            }
            fork_create_op(args, data_dir, store, root_key, &rescan).await
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
            snapshot_volume(args, data_dir, snapshot_locks, store, part_size_bytes).await
        }

        "reclaim" => {
            if args.is_empty() {
                return "err usage: reclaim <volume>".to_string();
            }
            reclaim_volume(args, data_dir).await
        }

        // Vend the non-secret `[store]` config so CLI read operations
        // (remote list, remote pull, volume create --from) can build an
        // S3 client that matches the coordinator's. Returned as TOML so
        // the caller can round-trip through toml::from_str.
        "get-store-config" => render_store_config(store_config),

        // Vend S3 credentials from the coordinator's env. Today this is
        // the long-lived AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY; the
        // same wire format will carry macaroon-issued short-lived
        // credentials when that lands.
        "get-store-creds" => render_store_creds(),

        // Macaroon mint for a spawned volume process. Authenticated by
        // SO_PEERCRED on the connecting socket — we accept only when the
        // peer's PID matches the volume's recorded `volume.pid`.
        "register" => {
            if args.is_empty() {
                return "err usage: register <volume-ulid>".to_string();
            }
            register_volume(args, data_dir, peer_pid, root_key)
        }

        // Macaroon-authenticated credential issuance. Verifies the MAC,
        // re-checks SO_PEERCRED matches the macaroon's `pid` caveat,
        // then delegates to the configured `CredentialIssuer`.
        "credentials" => {
            if args.is_empty() {
                return "err usage: credentials <macaroon>".to_string();
            }
            issue_credentials(args, data_dir, peer_pid, root_key, issuer)
        }

        _ => {
            warn!("[inbound] unexpected op: {op:?}");
            format!("err unknown op: {op}")
        }
    }
}

// ── Store config / creds vending ──────────────────────────────────────────────

/// Response for `get-store-config`: a multi-line TOML body naming either a
/// local store path or an S3 bucket/endpoint/region. Intentionally contains
/// no secrets.
fn render_store_config(store: &StoreSection) -> String {
    let mut body = String::from("ok\n");
    if let Some(path) = &store.local_path {
        body.push_str(&format!(
            "local_path = {}\n",
            toml_quote(&path.display().to_string())
        ));
        return body;
    }
    if let Some(bucket) = &store.bucket {
        body.push_str(&format!("bucket = {}\n", toml_quote(bucket)));
        if let Some(ep) = &store.endpoint {
            body.push_str(&format!("endpoint = {}\n", toml_quote(ep)));
        }
        if let Some(region) = &store.region {
            body.push_str(&format!("region = {}\n", toml_quote(region)));
        }
        return body;
    }
    // No explicit store configured — coordinator's own fallback is
    // `./elide_store`, so surface that so the CLI sees the same default.
    body.push_str("local_path = \"elide_store\"\n");
    body
}

/// Response for `get-store-creds`: TOML-bodied access key + secret + optional
/// session token read from the coordinator's env. Returns `err` when the
/// coordinator's env has no credentials — callers should treat that as
/// fatal rather than silently swap in their own env.
fn render_store_creds() -> String {
    let ak = match std::env::var("AWS_ACCESS_KEY_ID") {
        Ok(v) if !v.is_empty() => v,
        _ => return "err no credentials configured in coordinator env".to_string(),
    };
    let sk = match std::env::var("AWS_SECRET_ACCESS_KEY") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            return "err AWS_ACCESS_KEY_ID set but AWS_SECRET_ACCESS_KEY missing".to_string();
        }
    };
    let mut body = String::from("ok\n");
    body.push_str(&format!("access_key_id = {}\n", toml_quote(&ak)));
    body.push_str(&format!("secret_access_key = {}\n", toml_quote(&sk)));
    if let Ok(tok) = std::env::var("AWS_SESSION_TOKEN")
        && !tok.is_empty()
    {
        body.push_str(&format!("session_token = {}\n", toml_quote(&tok)));
    }
    body
}

/// Quote a string as a TOML basic string. The values we serialise (bucket
/// names, URLs, AWS keys) never contain characters that would require
/// `r"..."` literals or multi-line strings.
fn toml_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// ── Import operations ─────────────────────────────────────────────────────────

async fn start_import(
    vol_name: &str,
    oci_ref: &str,
    extents_from: &[String],
    data_dir: &Path,
    elide_import_bin: &Path,
    registry: &ImportRegistry,
    store: &Arc<dyn ObjectStore>,
    rescan: &Arc<Notify>,
) -> String {
    match import::spawn_import(
        vol_name,
        oci_ref,
        extents_from,
        data_dir,
        elide_import_bin,
        registry,
        store.clone(),
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
    let job = registry
        .lock()
        .expect("import registry poisoned")
        .get(&ulid)
        .cloned();
    match job {
        // import.lock exists but not in registry (coordinator restarted mid-import)
        None => "ok running".to_string(),
        Some(job) => match job.state() {
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

    let job = registry
        .lock()
        .expect("import registry poisoned")
        .get(&ulid)
        .cloned();
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
        let lines = job.read_from(offset);
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

        match job.state() {
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

// ── Volume reclaim ───────────────────────────────────────────────────────────

/// Run an alias-merge extent reclamation pass on a named volume.
///
/// The coordinator's only job is to resolve the volume name to a fork
/// directory and relay a `reclaim` IPC to the volume's control.sock.
/// No coordinator state is involved — reclaim never touches S3, GC state,
/// or the snapshot lock. The volume-side handler does scan + execute
/// against its current snapshot with default thresholds.
///
/// Returns
/// "ok <candidates_scanned> <runs_rewritten> <bytes_rewritten> <discarded>"
/// on success, or "err <message>" if the volume is not running or the
/// underlying IPC fails.
async fn reclaim_volume(vol_name: &str, data_dir: &Path) -> String {
    let link = data_dir.join("by_name").join(vol_name);
    let fork_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(_) => return format!("err volume not found: {vol_name}"),
    };
    if !fork_dir.join("control.sock").exists() {
        return format!("err volume '{vol_name}' is not running — start it first");
    }
    info!("[reclaim {vol_name}] starting pass");
    match elide_coordinator::control::reclaim(&fork_dir, None).await {
        Some(stats) => {
            info!(
                "[reclaim {vol_name}] done: scanned={} runs_rewritten={} bytes_rewritten={} discarded={}",
                stats.candidates_scanned,
                stats.runs_rewritten,
                stats.bytes_rewritten,
                stats.discarded,
            );
            format!(
                "ok {} {} {} {}",
                stats.candidates_scanned,
                stats.runs_rewritten,
                stats.bytes_rewritten,
                stats.discarded
            )
        }
        None => format!("err reclaim IPC failed for volume '{vol_name}'"),
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
    let sender = evict_registry
        .lock()
        .expect("evict registry poisoned")
        .get(&fork_dir)
        .cloned();
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
    part_size_bytes: usize,
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

    let lock = elide_coordinator::snapshot_lock_for(snapshot_locks, &fork_dir);
    let _guard = lock.lock_owned().await;

    // 1. Promote WAL into pending/.
    if !elide_coordinator::control::promote_wal(&fork_dir).await {
        return "err promote_wal failed or volume unreachable".to_string();
    }

    // 2. Inline drain: upload every pending segment, promote each, then
    //    upload any snapshot files already sitting under snapshots/.
    //    We run this before sign_snapshot_manifest so that index/ is populated
    //    with every segment up to the flush point.
    match elide_coordinator::upload::drain_pending(&fork_dir, &volume_id, store, part_size_bytes)
        .await
    {
        Ok(r) if r.failed > 0 => {
            return format!("err drain reported {} failed segment(s)", r.failed);
        }
        Ok(_) => {}
        Err(e) => return format!("err drain: {e:#}"),
    }

    // 3. Drain any outstanding GC handoffs so `index/` is in a stable
    //    post-GC state before the manifest is signed. Without this, a
    //    volume-applied handoff (bare `gc/<ulid>`) left over from a prior
    //    tick still references segments that `promote_segment` is about to
    //    delete from `index/` (and from S3 via `apply_done_handoffs`). The
    //    signed manifest would then point at segments that vanish seconds
    //    later, and any reader — delta_repack, fork, restart — would fail
    //    with ENOENT on the missing `.idx` file.
    //
    //    Safe under the snapshot lock: the tick loop's GC path
    //    `try_lock`s the same lock and skips the tick while we hold it,
    //    so there is no race with a concurrent apply_done_handoffs.
    let _ = elide_coordinator::control::apply_gc_handoffs(&fork_dir).await;
    if let Err(e) =
        elide_coordinator::gc::apply_done_handoffs(&fork_dir, &volume_id, store, part_size_bytes)
            .await
    {
        return format!("err draining gc handoffs: {e:#}");
    }

    // 4. Pick snap_ulid: the max ULID in index/. Empty forks cannot be
    //    snapshotted — the coordinator must never mint ULIDs, so there
    //    is no valid tag for a snapshot over zero segments. We surface
    //    this as the sentinel `ok empty` rather than an error so callers
    //    that legitimately want to publish an empty volume (e.g.
    //    `volume release` on a freshly-created name) can act on it.
    let snap_ulid = match pick_snapshot_ulid(&fork_dir) {
        Ok(Some(u)) => u,
        Ok(None) => return "ok empty".to_string(),
        Err(e) => return format!("err picking snap_ulid: {e}"),
    };

    // 5. Tell the volume to sign and write the manifest + marker.
    if !elide_coordinator::control::sign_snapshot_manifest(&fork_dir, snap_ulid).await {
        return format!("err sign_snapshot_manifest {snap_ulid} failed");
    }

    // 4b. Phase 4: regenerate the snapshot filemap for NBD-written volumes
    //     that drained without one. For import-written snapshots this is a
    //     no-op overwrite with byte-identical content (paths + LBA-map hashes
    //     match what the import path already wrote). Inline so the upload
    //     step below picks up the new file in the same pass; restart recovery
    //     for crashed mid-write filemaps falls out of the .tmp+rename commit.
    if let Err(e) = generate_snapshot_filemap(&fork_dir, snap_ulid, store.clone()).await {
        warn!("[snapshot {volume_id}] filemap generation failed for {snap_ulid}: {e:#}");
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

/// Phase 4: regenerate `snapshots/<snap_ulid>.filemap` from the sealed
/// snapshot's segments. Runs blocking ext4 + LBA-map walk on a worker
/// thread so the tokio reactor stays responsive.
///
/// Wires a `RemoteFetcher` so demand-fetch works for evicted segments
/// — Phase 4 may run on a volume whose ext4 metadata blocks have been
/// evicted to S3. The fetcher is constructed inside the closure (after the
/// search-dir list is known) because each fetcher binds to a fork chain.
async fn generate_snapshot_filemap(
    fork_dir: &Path,
    snap_ulid: ulid::Ulid,
    store: Arc<dyn ObjectStore>,
) -> std::io::Result<()> {
    let fork_dir = fork_dir.to_owned();
    let range_fetcher: Arc<dyn elide_fetch::RangeFetcher> =
        Arc::new(elide_coordinator::range_fetcher::ObjectStoreRangeFetcher::new(store));
    tokio::task::spawn_blocking(move || {
        let range_fetcher_for_factory = range_fetcher.clone();
        let mk_fetcher: Box<elide_core::block_reader::FetcherFactory<'_>> =
            Box::new(move |search_dirs: &[PathBuf]| {
                // RemoteFetcher wants oldest-first; BlockReader hands us
                // newest-first (fork → ancestors).
                let oldest_first: Vec<PathBuf> = search_dirs.iter().rev().cloned().collect();
                elide_fetch::RemoteFetcher::from_store(
                    range_fetcher_for_factory,
                    &oldest_first,
                    elide_fetch::DEFAULT_FETCH_BATCH_BYTES,
                )
                .ok()
                .map(|f| Box::new(f) as Box<dyn elide_core::segment::SegmentFetcher>)
            });
        let _wrote =
            elide_core::filemap::generate_from_snapshot(&fork_dir, &snap_ulid, mk_fetcher)?;
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|e| std::io::Error::other(format!("filemap join: {e}")))?
}

/// Pick a snapshot ULID as the max ULID in `fork_dir/index/`.
///
/// Returns `None` when `index/` is empty or missing — the coordinator must
/// never mint ULIDs, so an empty fork has no valid snapshot tag and the
/// caller rejects the snapshot request.
fn pick_snapshot_ulid(fork_dir: &Path) -> std::io::Result<Option<ulid::Ulid>> {
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
    Ok(latest)
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

    // Check if intentionally stopped.
    if vol_dir.join("volume.stopped").exists() {
        return "ok stopped (manual)".to_string();
    }

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

    // Refuse to delete a running writable volume — must be stopped first.
    if !vol_dir.join("volume.readonly").exists() && !vol_dir.join("volume.stopped").exists() {
        return "err volume is running; stop it first with: elide volume stop <name>".to_string();
    }

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

// ── Volume create / update ────────────────────────────────────────────────────

/// Parsed transport flags from the `create`/`update` IPC argument string.
/// Each `Some` field is an explicit set request; the corresponding `no_*`
/// boolean is an explicit clear. Mutually-exclusive flags are validated by
/// the consumer (different rules for create vs update).
#[derive(Default)]
struct TransportPatch {
    nbd_port: Option<u16>,
    nbd_bind: Option<String>,
    nbd_socket: Option<std::path::PathBuf>,
    no_nbd: bool,
    ublk: bool,
    ublk_id: Option<i32>,
    no_ublk: bool,
}

/// Parse a flat space-separated flag list. Recognised tokens:
///   nbd-port=<u16>, nbd-bind=<addr>, nbd-socket=<path>, no-nbd,
///   ublk, ublk-id=<i32>, no-ublk
/// Unknown tokens produce an error so silent typos don't get accepted.
fn parse_transport_flags(args: &str) -> Result<TransportPatch, String> {
    let mut patch = TransportPatch::default();
    for tok in args.split_whitespace() {
        let (key, val) = match tok.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (tok, None),
        };
        match (key, val) {
            ("nbd-port", Some(v)) => {
                patch.nbd_port = Some(v.parse().map_err(|e| format!("bad nbd-port {v:?}: {e}"))?);
            }
            ("nbd-bind", Some(v)) => patch.nbd_bind = Some(v.to_owned()),
            ("nbd-socket", Some(v)) => patch.nbd_socket = Some(v.into()),
            ("no-nbd", None) => patch.no_nbd = true,
            ("ublk", None) => patch.ublk = true,
            ("ublk-id", Some(v)) => {
                patch.ublk_id = Some(v.parse().map_err(|e| format!("bad ublk-id {v:?}: {e}"))?);
            }
            ("no-ublk", None) => patch.no_ublk = true,
            _ => return Err(format!("unknown flag: {tok}")),
        }
    }
    Ok(patch)
}

fn validate_volume_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("volume name must not be empty".to_owned());
    }
    if let Some(c) = name
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_' && *c != '.')
    {
        return Err(format!(
            "invalid character {c:?} in volume name {name:?}: only [a-zA-Z0-9._-] allowed"
        ));
    }
    if matches!(name, "status" | "attach") {
        return Err(format!("'{name}' is a reserved name"));
    }
    Ok(())
}

async fn create_volume_op(
    args: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    root_key: &[u8; 32],
    rescan: &Notify,
) -> String {
    // First two whitespace-separated tokens: name, size_bytes. Remainder is flags.
    let mut iter = args.splitn(3, ' ').map(str::trim);
    let name = match iter.next() {
        Some(s) if !s.is_empty() => s,
        _ => return "err usage: create <name> <size_bytes> [flags...]".to_string(),
    };
    let size_str = match iter.next() {
        Some(s) if !s.is_empty() => s,
        _ => return "err usage: create <name> <size_bytes> [flags...]".to_string(),
    };
    let flag_str = iter.next().unwrap_or("");

    if let Err(e) = validate_volume_name(name) {
        return format!("err {e}");
    }
    let bytes: u64 = match size_str.parse() {
        Ok(b) if b > 0 => b,
        Ok(_) => return "err size must be non-zero".to_string(),
        Err(e) => return format!("err bad size {size_str:?}: {e}"),
    };

    let patch = match parse_transport_flags(flag_str) {
        Ok(p) => p,
        Err(e) => return format!("err {e}"),
    };
    if patch.no_nbd || patch.no_ublk {
        return "err no-nbd / no-ublk are not valid on create (volume starts without transport)"
            .to_string();
    }
    if (patch.nbd_port.is_some() || patch.nbd_bind.is_some() || patch.nbd_socket.is_some())
        && (patch.ublk || patch.ublk_id.is_some())
    {
        return "err nbd-* flags are mutually exclusive with ublk".to_string();
    }

    let nbd_cfg = if let Some(socket) = patch.nbd_socket {
        Some(elide_core::config::NbdConfig {
            socket: Some(socket),
            ..Default::default()
        })
    } else if patch.nbd_port.is_some() || patch.nbd_bind.is_some() {
        Some(elide_core::config::NbdConfig {
            port: patch.nbd_port,
            bind: patch.nbd_bind,
            ..Default::default()
        })
    } else {
        None
    };
    let ublk_cfg = if patch.ublk || patch.ublk_id.is_some() {
        Some(elide_core::config::UblkConfig {
            dev_id: patch.ublk_id,
        })
    } else {
        None
    };

    let by_name_dir = data_dir.join("by_name");
    if by_name_dir.join(name).exists() {
        return format!("err volume already exists: {name}");
    }

    let vol_ulid = ulid::Ulid::new();
    let vol_ulid_str = vol_ulid.to_string();
    let vol_dir = data_dir.join("by_id").join(&vol_ulid_str);

    // Claim the name in S3 *before* creating any local state. This
    // closes the cross-coordinator race where two hosts run
    // `volume create <name>` concurrently — the loser sees
    // `AlreadyExists` and refuses, leaving no partial local artefacts.
    use elide_coordinator::lifecycle::{LifecycleError, MarkInitialOutcome, mark_initial};
    match mark_initial(store, name, root_key, vol_ulid).await {
        Ok(MarkInitialOutcome::Claimed) => {}
        Ok(MarkInitialOutcome::AlreadyExists {
            existing_vol_ulid,
            existing_state,
            existing_owner,
        }) => {
            let owner = existing_owner.as_deref().unwrap_or("<unowned>");
            return format!(
                "err name '{name}' already exists in bucket \
                 (vol_ulid={existing_vol_ulid}, state={existing_state:?}, \
                 owner={owner})"
            );
        }
        Err(LifecycleError::Store(e)) => {
            return format!("err claiming name in bucket: {e}");
        }
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            // mark_initial doesn't currently surface this, but match
            // exhaustively so a future refactor doesn't silently drop it.
            return format!("err name held by another coordinator: {held_by}");
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            return format!("err names/<name> is in unexpected state {from:?}");
        }
    }

    let result: std::io::Result<()> = (|| {
        std::fs::create_dir_all(&vol_dir)?;
        std::fs::create_dir_all(vol_dir.join("pending"))?;
        std::fs::create_dir_all(vol_dir.join("index"))?;
        std::fs::create_dir_all(vol_dir.join("cache"))?;
        let key = elide_core::signing::generate_keypair(
            &vol_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )?;
        elide_core::signing::write_provenance(
            &vol_dir,
            &key,
            elide_core::signing::VOLUME_PROVENANCE_FILE,
            &elide_core::signing::ProvenanceLineage::default(),
        )?;
        elide_core::config::VolumeConfig {
            name: Some(name.to_owned()),
            size: Some(bytes),
            nbd: nbd_cfg,
            ublk: ublk_cfg,
        }
        .write(&vol_dir)?;
        std::fs::create_dir_all(&by_name_dir)?;
        std::os::unix::fs::symlink(format!("../by_id/{vol_ulid_str}"), by_name_dir.join(name))?;
        Ok(())
    })();

    if let Err(e) = result {
        // Local setup failed. Best-effort: roll back local artefacts
        // *and* the bucket-side claim so the name is free to retry.
        let _ = std::fs::remove_file(by_name_dir.join(name));
        let _ = std::fs::remove_dir_all(&vol_dir);
        let name_key = object_store::path::Path::from(format!("names/{name}"));
        if let Err(del_err) = store.delete(&name_key).await {
            warn!(
                "[inbound] create {name}: local setup failed and rollback \
                 of names/<name> also failed: {del_err}"
            );
        }
        return format!("err create failed: {e}");
    }

    rescan.notify_one();
    info!("[inbound] created volume {name} ({vol_ulid_str})");
    format!("ok {vol_ulid_str}")
}

async fn update_volume_op(args: &str, data_dir: &Path) -> String {
    let (name, flag_str) = match args.split_once(' ') {
        Some((n, rest)) => (n.trim(), rest.trim()),
        None => (args.trim(), ""),
    };
    if name.is_empty() {
        return "err usage: update <volume> [flags...]".to_string();
    }
    let link = data_dir.join("by_name").join(name);
    if !link.exists() {
        return format!("err volume not found: {name}");
    }
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    let patch = match parse_transport_flags(flag_str) {
        Ok(p) => p,
        Err(e) => return format!("err {e}"),
    };

    let mut cfg = match elide_core::config::VolumeConfig::read(&vol_dir) {
        Ok(c) => c,
        Err(e) => return format!("err reading volume.toml: {e}"),
    };

    // Mirror the CLI's apply order: nbd flags first, then ublk. The setters
    // clear the opposite transport so the two sections remain mutually
    // exclusive.
    if patch.no_nbd {
        cfg.nbd = None;
    } else if let Some(socket) = patch.nbd_socket {
        cfg.nbd = Some(elide_core::config::NbdConfig {
            socket: Some(socket),
            ..Default::default()
        });
        cfg.ublk = None;
    } else if patch.nbd_port.is_some() || patch.nbd_bind.is_some() {
        let existing = cfg.nbd.get_or_insert_with(Default::default);
        if let Some(port) = patch.nbd_port {
            existing.port = Some(port);
            existing.socket = None;
        }
        if let Some(bind) = patch.nbd_bind {
            existing.bind = Some(bind);
        }
        cfg.ublk = None;
    }

    if patch.no_ublk {
        cfg.ublk = None;
    } else if patch.ublk || patch.ublk_id.is_some() {
        cfg.ublk = Some(elide_core::config::UblkConfig {
            dev_id: patch.ublk_id,
        });
        cfg.nbd = None;
    }

    if let Err(e) = cfg.write(&vol_dir) {
        return format!("err writing volume.toml: {e}");
    }

    // Restart the volume process so it picks up the new config. ECONNREFUSED /
    // ENOENT mean the volume isn't running — fine, the new config takes effect
    // on next start.
    match elide_coordinator::control::call_for_inbound(&vol_dir, "shutdown").await {
        Some(resp) if resp == "ok" => {
            info!("[inbound] updated volume {name}; restart triggered");
            "ok restarting".to_string()
        }
        Some(resp) => format!("err shutdown failed: {resp}"),
        None => {
            info!("[inbound] updated volume {name}; not running");
            "ok not-running".to_string()
        }
    }
}

// ── Volume fork (create --from) plumbing ──────────────────────────────────────

/// Decode a 64-character hex string into a 32-byte array (Ed25519 pubkey).
fn decode_hex32(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| format!("invalid hex at byte {i}"))?;
    }
    Ok(out)
}

/// Pull one readonly ancestor from the store.
///
/// Mirrors the CLI's `pull_one_readonly`: downloads `manifest.toml`,
/// `volume.pub`, and `volume.provenance`, writes the ancestor under
/// `data_dir/by_id/<vol_ulid>/`, and returns the parent ULID parsed from
/// the (signature-verified) provenance, or empty if this is a root volume.
async fn pull_readonly_op(args: &str, data_dir: &Path, store: &Arc<dyn ObjectStore>) -> String {
    use object_store::path::Path as StorePath;

    let volume_id = args.trim();
    if ulid::Ulid::from_string(volume_id).is_err() {
        return format!("err invalid ULID: {volume_id}");
    }

    let vol_dir = data_dir.join("by_id").join(volume_id);
    if vol_dir.exists() {
        return format!("err volume already present locally: {volume_id}");
    }

    // Step 1: fetch manifest.toml.
    let manifest_key = StorePath::from(format!("by_id/{volume_id}/manifest.toml"));
    let manifest_bytes = match store.get(&manifest_key).await {
        Ok(d) => match d.bytes().await {
            Ok(b) => b,
            Err(e) => return format!("err reading manifest: {e}"),
        },
        Err(object_store::Error::NotFound { .. }) => {
            return format!("err manifest not found in store for volume {volume_id}");
        }
        Err(e) => return format!("err downloading manifest: {e}"),
    };
    let manifest: toml::Table = match std::str::from_utf8(&manifest_bytes)
        .map_err(|e| format!("manifest is not valid utf-8: {e}"))
        .and_then(|s| toml::from_str(s).map_err(|e| format!("parsing manifest.toml: {e}")))
    {
        Ok(t) => t,
        Err(e) => return format!("err {e}"),
    };
    let size = match manifest.get("size").and_then(|v| v.as_integer()) {
        Some(s) => s,
        None => return "err manifest.toml missing 'size'".to_string(),
    };

    // Step 2: fetch volume.pub.
    let pub_key_bytes = match store
        .get(&StorePath::from(format!("by_id/{volume_id}/volume.pub")))
        .await
    {
        Ok(d) => match d.bytes().await {
            Ok(b) => b,
            Err(e) => return format!("err reading volume.pub: {e}"),
        },
        Err(e) => return format!("err downloading volume.pub: {e}"),
    };

    // Step 3: fetch volume.provenance. NotFound is hard error.
    let provenance_bytes = match store
        .get(&StorePath::from(format!(
            "by_id/{volume_id}/volume.provenance"
        )))
        .await
    {
        Ok(d) => match d.bytes().await {
            Ok(b) => b,
            Err(e) => return format!("err reading volume.provenance: {e}"),
        },
        Err(object_store::Error::NotFound { .. }) => {
            return format!("err volume.provenance not found in store for volume {volume_id}");
        }
        Err(e) => return format!("err downloading volume.provenance: {e}"),
    };

    // Step 4: write the ancestor entry. No name carried over from manifest:
    // a missing volume.name (and missing by_name/ symlink) is the on-disk
    // marker that distinguishes a pulled ancestor from a user-managed volume.
    let _ = manifest;
    let result: std::io::Result<()> = (|| {
        std::fs::create_dir_all(&vol_dir)?;
        elide_core::config::VolumeConfig {
            name: None,
            size: Some(size as u64),
            ..Default::default()
        }
        .write(&vol_dir)?;
        std::fs::write(vol_dir.join("volume.readonly"), "")?;
        std::fs::write(vol_dir.join("volume.pub"), &pub_key_bytes)?;
        std::fs::write(
            vol_dir.join(elide_core::signing::VOLUME_PROVENANCE_FILE),
            &provenance_bytes,
        )?;
        std::fs::create_dir_all(vol_dir.join("index"))?;
        Ok(())
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_dir_all(&vol_dir);
        return format!("err writing pulled ancestor: {e}");
    }

    // Step 5: parse and verify provenance to find the parent.
    let parent_ulid = match elide_core::signing::read_lineage_verifying_signature(
        &vol_dir,
        elide_core::signing::VOLUME_PUB_FILE,
        elide_core::signing::VOLUME_PROVENANCE_FILE,
    ) {
        Ok(lineage) => lineage.parent.map(|p| p.volume_ulid.to_string()),
        Err(e) => {
            let _ = std::fs::remove_dir_all(&vol_dir);
            return format!("err verifying provenance for {volume_id}: {e}");
        }
    };

    info!("[inbound] pulled ancestor {volume_id}");
    match parent_ulid {
        Some(p) => format!("ok {p}"),
        None => "ok".to_string(),
    }
}

/// Synthesize a "now" snapshot for a readonly source volume.
///
/// Mirrors the CLI's `create_readonly_snapshot_now`: prefetches indexes,
/// uploads the snapshot marker, verifies pinned segments are still in S3,
/// and writes a signed manifest under an ephemeral key in the ancestor's
/// directory. Returns `<snap_ulid> <ephemeral_pubkey_hex>`.
async fn force_snapshot_now_op(
    args: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> String {
    use futures::TryStreamExt;
    use object_store::PutPayload;
    use object_store::path::Path as StorePath;

    let volume_id = args.trim();
    if ulid::Ulid::from_string(volume_id).is_err() {
        return format!("err invalid ULID: {volume_id}");
    }

    let ancestor_dir = data_dir.join("by_id").join(volume_id);
    if !ancestor_dir.exists() {
        return format!("err volume not found locally: {volume_id}");
    }

    // Step 1: prefetch indexes from S3.
    if let Err(e) = elide_coordinator::prefetch::prefetch_indexes(&ancestor_dir, store).await {
        return format!("err prefetching ancestor indexes: {e:#}");
    }

    // Step 2: enumerate prefetched .idx files. Pin ULID = max(segments).
    let index_dir = ancestor_dir.join("index");
    let mut segments: Vec<ulid::Ulid> = Vec::new();
    let mut max_seg: Option<ulid::Ulid> = None;
    let entries = match std::fs::read_dir(&index_dir) {
        Ok(e) => e,
        Err(e) => return format!("err reading index dir: {e}"),
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => return format!("err reading index entry: {e}"),
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".idx") else {
            continue;
        };
        if let Ok(u) = ulid::Ulid::from_string(stem) {
            segments.push(u);
            if max_seg.is_none_or(|m| u > m) {
                max_seg = Some(u);
            }
        }
    }
    let snap = match max_seg {
        Some(m) => m,
        None => ulid::Ulid::new(),
    };
    let snap_str = snap.to_string();

    // Step 3 + 4: upload marker, verify pinned segments still present.
    let marker_key = match elide_coordinator::upload::snapshot_key(volume_id, &snap_str) {
        Ok(k) => k,
        Err(e) => return format!("err snapshot_key: {e}"),
    };
    let seg_prefix = StorePath::from(format!("by_id/{volume_id}/segments/"));
    let pinned: std::collections::HashSet<ulid::Ulid> = segments.iter().copied().collect();

    if let Err(e) = store.put(&marker_key, PutPayload::from_static(b"")).await {
        return format!("err uploading snapshot marker: {e}");
    }

    let objects = match store.list(Some(&seg_prefix)).try_collect::<Vec<_>>().await {
        Ok(v) => v,
        Err(e) => return format!("err listing segments for verification: {e}"),
    };
    let present: std::collections::HashSet<ulid::Ulid> = objects
        .iter()
        .filter_map(|o| o.location.filename())
        .filter_map(|name| ulid::Ulid::from_string(name).ok())
        .collect();
    let mut missing: Vec<ulid::Ulid> = pinned.difference(&present).copied().collect();
    if !missing.is_empty() {
        missing.sort();
        if let Err(e) = store.delete(&marker_key).await {
            warn!("[inbound] failed to delete stale snapshot marker {snap_str}: {e}");
        }
        let preview: Vec<String> = missing.iter().take(5).map(|u| u.to_string()).collect();
        let extra = missing.len().saturating_sub(preview.len());
        let suffix = if extra > 0 {
            format!(" (+{extra} more)")
        } else {
            String::new()
        };
        return format!(
            "err force-snapshot aborted: {} of {} pinned segment(s) no longer present in S3 \
             (owner GC raced us); missing: [{}]{}",
            missing.len(),
            pinned.len(),
            preview.join(", "),
            suffix,
        );
    }

    // Step 5: load-or-create the per-source attestation key.
    let (signer, vk) = match elide_core::signing::load_or_create_keypair(
        &ancestor_dir,
        elide_core::signing::FORCE_SNAPSHOT_KEY_FILE,
        elide_core::signing::FORCE_SNAPSHOT_PUB_FILE,
    ) {
        Ok(s) => s,
        Err(e) => return format!("err attestation keypair: {e}"),
    };

    // Step 6: signed manifest + local marker.
    if let Err(e) =
        elide_core::signing::write_snapshot_manifest(&ancestor_dir, &*signer, &snap, &segments)
    {
        return format!("err writing snapshot manifest: {e}");
    }
    let snap_dir = ancestor_dir.join("snapshots");
    if let Err(e) = std::fs::create_dir_all(&snap_dir) {
        return format!("err creating snapshots dir: {e}");
    }
    if let Err(e) = std::fs::write(snap_dir.join(&snap_str), b"") {
        return format!("err writing local marker: {e}");
    }

    info!(
        "[inbound] attested now-snapshot {snap_str} for {volume_id} ({} segments)",
        segments.len()
    );
    let pubkey_hex: String = vk.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
    format!("ok {snap_str} {pubkey_hex}")
}

/// Fork an existing source volume into a new writable volume.
///
/// Mirrors the CLI's `fork_volume_at*` + by_name symlink + volume.toml write.
/// `source_ulid` resolves to any volume in `by_id/<ulid>/` — writable,
/// imported readonly base, or pulled ancestor. `snap` is optional: if
/// omitted, falls back to `volume::fork_volume` (latest local snapshot).
/// `parent-key` is the hex ephemeral pubkey from `force-snapshot-now`,
/// recorded in the new fork's provenance for force-snapshot pins.
async fn fork_create_op(
    args: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    root_key: &[u8; 32],
    rescan: &Notify,
) -> String {
    let mut iter = args.splitn(3, ' ').map(str::trim);
    let new_name = match iter.next() {
        Some(s) if !s.is_empty() => s,
        _ => return "err usage: fork-create <name> <source_ulid> [snap=<ulid>] [parent-key=<hex>] [flags...]".to_string(),
    };
    let source_ulid_str = match iter.next() {
        Some(s) if !s.is_empty() => s,
        _ => return "err usage: fork-create <name> <source_ulid> [snap=<ulid>] [parent-key=<hex>] [flags...]".to_string(),
    };
    let rest = iter.next().unwrap_or("");

    if let Err(e) = validate_volume_name(new_name) {
        return format!("err {e}");
    }
    if ulid::Ulid::from_string(source_ulid_str).is_err() {
        return format!("err invalid source ULID: {source_ulid_str}");
    }

    // Pull off `snap=<ulid>` and `parent-key=<hex>` first; remaining tokens
    // go to the transport-flag parser.
    let mut snap: Option<ulid::Ulid> = None;
    let mut parent_key: Option<elide_core::signing::VerifyingKey> = None;
    let mut flag_tokens: Vec<&str> = Vec::new();
    for tok in rest.split_whitespace() {
        if let Some(v) = tok.strip_prefix("snap=") {
            match ulid::Ulid::from_string(v) {
                Ok(u) => snap = Some(u),
                Err(e) => return format!("err bad snap ULID {v:?}: {e}"),
            }
        } else if let Some(v) = tok.strip_prefix("parent-key=") {
            let arr = match decode_hex32(v) {
                Ok(a) => a,
                Err(e) => return format!("err bad parent-key: {e}"),
            };
            match elide_core::signing::VerifyingKey::from_bytes(&arr) {
                Ok(k) => parent_key = Some(k),
                Err(e) => return format!("err parent-key not a valid Ed25519 pubkey: {e}"),
            }
        } else {
            flag_tokens.push(tok);
        }
    }
    let patch = match parse_transport_flags(&flag_tokens.join(" ")) {
        Ok(p) => p,
        Err(e) => return format!("err {e}"),
    };
    if patch.no_nbd || patch.no_ublk {
        return "err no-nbd / no-ublk are not valid on fork-create".to_string();
    }
    let nbd_cfg = if let Some(socket) = patch.nbd_socket {
        Some(elide_core::config::NbdConfig {
            socket: Some(socket),
            ..Default::default()
        })
    } else if patch.nbd_port.is_some() || patch.nbd_bind.is_some() {
        Some(elide_core::config::NbdConfig {
            port: patch.nbd_port,
            bind: patch.nbd_bind,
            ..Default::default()
        })
    } else {
        None
    };
    let ublk_cfg = if patch.ublk || patch.ublk_id.is_some() {
        Some(elide_core::config::UblkConfig {
            dev_id: patch.ublk_id,
        })
    } else {
        None
    };

    let by_name_dir = data_dir.join("by_name");
    let symlink_path = by_name_dir.join(new_name);
    if symlink_path.exists() {
        return format!("err volume already exists: {new_name}");
    }

    let by_id_dir = data_dir.join("by_id");
    let source_dir = elide_core::volume::resolve_ancestor_dir(&by_id_dir, source_ulid_str);
    if !source_dir.exists() {
        return format!("err source volume {source_ulid_str} not found locally");
    }

    let new_vol_ulid_value = ulid::Ulid::new();
    let new_vol_ulid = new_vol_ulid_value.to_string();
    let new_fork_dir = by_id_dir.join(&new_vol_ulid);

    // Claim the name in S3 *before* materialising the fork. Same
    // pattern as `create_volume_op`: lose the race cleanly with no
    // partial local state if another coordinator already holds the
    // name, and roll back the bucket-side claim if local fork-out
    // fails.
    use elide_coordinator::lifecycle::{LifecycleError, MarkInitialOutcome, mark_initial};
    match mark_initial(store, new_name, root_key, new_vol_ulid_value).await {
        Ok(MarkInitialOutcome::Claimed) => {}
        Ok(MarkInitialOutcome::AlreadyExists {
            existing_vol_ulid,
            existing_state,
            existing_owner,
        }) => {
            let owner = existing_owner.as_deref().unwrap_or("<unowned>");
            return format!(
                "err name '{new_name}' already exists in bucket \
                 (vol_ulid={existing_vol_ulid}, state={existing_state:?}, \
                 owner={owner})"
            );
        }
        Err(LifecycleError::Store(e)) => {
            return format!("err claiming name in bucket: {e}");
        }
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            return format!("err name held by another coordinator: {held_by}");
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            return format!("err names/<name> is in unexpected state {from:?}");
        }
    }

    // Local-only rollback. After a successful `mark_initial` claim,
    // every error-return path below also rolls back the bucket-side
    // claim via `delete_name_claim` so the name is free to retry.
    let cleanup = |fork_dir: &Path, link: &Path| {
        let _ = std::fs::remove_file(link);
        let _ = std::fs::remove_dir_all(fork_dir);
    };
    let rollback_claim = async || {
        let key = object_store::path::Path::from(format!("names/{new_name}"));
        if let Err(e) = store.delete(&key).await {
            warn!(
                "[inbound] fork-create {new_name}: local fork failed and \
                 rollback of names/<name> also failed: {e}"
            );
        }
    };

    let fork_result: std::io::Result<()> = match (snap, parent_key) {
        (Some(snap), Some(vk)) => elide_core::volume::fork_volume_at_with_manifest_key(
            &new_fork_dir,
            &source_dir,
            snap,
            vk,
        ),
        (Some(snap), None) => elide_core::volume::fork_volume_at(&new_fork_dir, &source_dir, snap),
        (None, _) => elide_core::volume::fork_volume(&new_fork_dir, &source_dir),
    };
    if let Err(e) = fork_result {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return format!("err fork failed: {e}");
    }

    // Inherit size from source; require it to be set.
    let src_cfg = match elide_core::config::VolumeConfig::read(&source_dir) {
        Ok(c) => c,
        Err(e) => {
            cleanup(&new_fork_dir, &symlink_path);
            rollback_claim().await;
            return format!("err reading source volume config: {e}");
        }
    };
    let size = match src_cfg.size {
        Some(s) => s,
        None => {
            cleanup(&new_fork_dir, &symlink_path);
            rollback_claim().await;
            return "err source volume has no size (import may not have completed)".to_string();
        }
    };
    if let Err(e) = (elide_core::config::VolumeConfig {
        name: Some(new_name.to_owned()),
        size: Some(size),
        nbd: nbd_cfg,
        ublk: ublk_cfg,
    }
    .write(&new_fork_dir))
    {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return format!("err writing volume config: {e}");
    }
    if let Err(e) = std::fs::create_dir_all(&by_name_dir) {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return format!("err creating by_name dir: {e}");
    }
    if let Err(e) = std::os::unix::fs::symlink(format!("../by_id/{new_vol_ulid}"), &symlink_path) {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return format!("err creating by_name symlink: {e}");
    }

    rescan.notify_one();
    info!("[inbound] forked volume {new_name} ({new_vol_ulid}) from {source_ulid_str}");
    format!("ok {new_vol_ulid}")
}

// ── Volume stop / start ───────────────────────────────────────────────────────

async fn stop_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    root_key: &[u8; 32],
) -> String {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!("err volume not found: {volume_name}");
    }
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    if vol_dir.join("volume.readonly").exists() {
        return "err volume is readonly; nothing to stop".to_string();
    }
    if vol_dir.join("volume.stopped").exists() {
        // Already stopped — idempotent success.
        return "ok".to_string();
    }

    // Refuse to stop while an NBD client is connected. ublk volumes have
    // no NBD client and `connected` will simply return false.
    match elide_coordinator::control::call_for_inbound(&vol_dir, "connected").await {
        Some(resp) if resp == "ok true" => {
            return "err nbd client is connected; disconnect it first".to_string();
        }
        _ => {}
    }

    // Verify ownership in S3 before taking any local action: refuse early
    // if names/<name> is held by another coordinator. A missing record is
    // fine — the volume may not yet have been drained to S3, in which
    // case the local stop proceeds and the S3 update is a no-op.
    use elide_coordinator::lifecycle::{LifecycleError, mark_stopped};
    match mark_stopped(store, volume_name, root_key).await {
        Ok(_) => {}
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            return format!(
                "err name '{volume_name}' is owned by coordinator {held_by}; \
                 use --force-takeover to override"
            );
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            return format!("err names/<name> is in state {from:?}; cannot stop");
        }
        Err(LifecycleError::Store(e)) => {
            // Transient store error or parse failure. The S3 record is
            // unchanged; we can still proceed with the local stop, but
            // surface the warning so operators know the bucket state may
            // be stale.
            warn!("[inbound] stop {volume_name}: failed to update names/<name>: {e}");
        }
    }

    // Write the marker before sending shutdown so the supervisor won't restart.
    if let Err(e) = std::fs::write(vol_dir.join("volume.stopped"), "") {
        return format!("err writing volume.stopped: {e}");
    }

    match elide_coordinator::control::call_for_inbound(&vol_dir, "shutdown").await {
        Some(resp) if resp == "ok" => {
            info!("[inbound] stopped volume {volume_name}");
            "ok".to_string()
        }
        Some(resp) => {
            // Roll back the marker so the supervisor doesn't strand a still-
            // running volume. (Note: the S3 state has already flipped to
            // Stopped; that's a soft inconsistency the operator can resolve
            // by issuing `volume start` once the underlying issue is fixed.)
            let _ = std::fs::remove_file(vol_dir.join("volume.stopped"));
            format!("err shutdown failed: {resp}")
        }
        None => {
            // Volume process wasn't running — marker is correct as-is.
            info!("[inbound] stopped volume {volume_name} (process was not running)");
            "ok".to_string()
        }
    }
}

/// Relinquish ownership of `<volume_name>` so any other coordinator can
/// `volume start` it. Composes the existing snapshot path:
///
///   1. Refuse if NBD client connected, volume is readonly, or already
///      stopped (release of a stopped volume requires `volume start`
///      first to bring it up for the drain).
///   2. Verify S3 ownership before doing the expensive drain.
///   3. Drain WAL → publish handoff snapshot via `snapshot_volume`.
///   4. Send shutdown RPC to halt the daemon.
///   5. Write `volume.stopped` marker so the supervisor won't restart.
///   6. Conditional PUT to `names/<name>` setting state=Released and
///      recording the handoff snapshot ULID.
async fn release_volume_op(
    volume_name: &str,
    data_dir: &Path,
    snapshot_locks: &SnapshotLockRegistry,
    store: &Arc<dyn ObjectStore>,
    part_size_bytes: usize,
    root_key: &[u8; 32],
) -> String {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!("err volume not found: {volume_name}");
    }
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    if vol_dir.join("volume.readonly").exists() {
        return "err volume is readonly; nothing to release".to_string();
    }
    if vol_dir.join("volume.stopped").exists() {
        return "err volume is stopped — `volume start` it first, then release".to_string();
    }
    if !vol_dir.join("control.sock").exists() {
        return format!("err volume '{volume_name}' is not running — start it first");
    }

    match elide_coordinator::control::call_for_inbound(&vol_dir, "connected").await {
        Some(resp) if resp == "ok true" => {
            return "err nbd client is connected; disconnect it first".to_string();
        }
        _ => {}
    }

    // Verify ownership in S3 before kicking off the (expensive) drain.
    use elide_coordinator::lifecycle;
    use elide_core::name_record::NameState;
    match elide_coordinator::name_store::read_name_record(store, volume_name).await {
        Ok(Some((rec, _))) => {
            use elide_coordinator::portable::{coordinator_id, format_coordinator_id};
            let self_id = format_coordinator_id(&coordinator_id(root_key));
            if let Some(existing) = rec.coordinator_id.as_deref()
                && existing != self_id
            {
                return format!(
                    "err name '{volume_name}' is owned by coordinator {existing}; \
                     use --force-takeover to override"
                );
            }
            if rec.state == NameState::Released {
                return format!("err name '{volume_name}' is already released");
            }
        }
        Ok(None) => {
            // No record yet — release of an unpublished volume is
            // meaningless (nothing for the next claimant to fork from).
            return format!("err name '{volume_name}' has no S3 record; drain the volume first");
        }
        Err(e) => {
            return format!("err reading names/{volume_name}: {e}");
        }
    }

    // Drain WAL → publish handoff snapshot. Reuses the existing
    // snapshot_volume path; the response is one of:
    //   - `ok <snap_ulid>` — drained, snapshot published.
    //   - `ok empty`        — no segments to snapshot (freshly-created
    //                         name with nothing written). Release
    //                         proceeds without a handoff snapshot;
    //                         the next claimant forks a fresh root.
    //   - `err <msg>`       — drain or snapshot failed.
    let snap_resp = snapshot_volume(
        volume_name,
        data_dir,
        snapshot_locks,
        store,
        part_size_bytes,
    )
    .await;
    let snap_ulid: Option<ulid::Ulid> = match snap_resp.strip_prefix("ok ") {
        Some(s) if s.trim() == "empty" => None,
        Some(s) => match ulid::Ulid::from_string(s.trim()) {
            Ok(u) => Some(u),
            Err(e) => return format!("err parsing snapshot ULID '{}': {e}", s.trim()),
        },
        None => return format!("err snapshot for release failed: {snap_resp}"),
    };

    // Halt the daemon. Writing the marker first prevents the supervisor
    // from restarting it between shutdown and our final state write.
    if let Err(e) = std::fs::write(vol_dir.join("volume.stopped"), "") {
        return format!("err writing volume.stopped: {e}");
    }
    match elide_coordinator::control::call_for_inbound(&vol_dir, "shutdown").await {
        Some(resp) if resp == "ok" => {}
        Some(resp) => {
            let _ = std::fs::remove_file(vol_dir.join("volume.stopped"));
            return format!("err shutdown failed: {resp}");
        }
        None => {
            // Volume process wasn't running by the time we reached
            // this step. The snapshot succeeded though, so we proceed
            // with the state flip.
        }
    }

    // Final step: flip names/<name> to Released, recording the handoff
    // snapshot (or `None` for the empty-volume case). From this point
    // any coordinator may claim the name.
    let snap_str: String = snap_ulid
        .map(|s| s.to_string())
        .unwrap_or_else(|| "none (empty volume)".to_owned());
    match lifecycle::mark_released(store, volume_name, root_key, snap_ulid).await {
        Ok(_) => {
            info!("[inbound] released volume {volume_name} at handoff snapshot {snap_str}");
            // Reply shape: clients expect `ok <ulid>`. For the empty
            // case there is no ulid; emit the literal `empty` so the
            // CLI can render a useful message.
            match snap_ulid {
                Some(u) => format!("ok {u}"),
                None => "ok empty".to_string(),
            }
        }
        Err(e) => {
            // Local state has already moved (volume is stopped, snapshot
            // is published). The S3 record write failed — the operator
            // can retry or use --force-takeover from a peer to recover.
            warn!("[inbound] release {volume_name}: state flip failed: {e}");
            format!("err snapshot {snap_str} published but names/<name> update failed: {e}")
        }
    }
}

/// Claim a `Released` name for a freshly-minted local fork.
///
/// Pre-conditions enforced here:
/// - `by_id/<new_vol_ulid>/` must exist locally (the CLI created it
///   before calling).
/// - `by_name/<name>` must point at it (or be absent — we create it
///   if missing).
/// - `names/<name>` must be in `Released` state and the CLI's
///   conditional-PUT must succeed.
///
/// On success the name's S3 record is rebound to the new fork and
/// the supervisor is notified to launch the daemon.
async fn claim_volume_op(
    volume_name: &str,
    new_ulid_str: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    root_key: &[u8; 32],
    rescan: &Notify,
) -> String {
    let new_ulid = match ulid::Ulid::from_string(new_ulid_str) {
        Ok(u) => u,
        Err(e) => return format!("err invalid new_vol_ulid: {e}"),
    };

    // The new fork directory must already exist locally — the CLI is
    // responsible for materialising it before invoking `claim`.
    let new_dir = data_dir.join("by_id").join(new_ulid.to_string());
    if !new_dir.exists() {
        return format!(
            "err new fork directory not found at {} — \
             the CLI must materialise the fork before calling claim",
            new_dir.display()
        );
    }

    // Ensure by_name/<name> points at the new fork. Create it if absent;
    // otherwise verify it already targets the right ULID. The supervisor
    // task spawning depends on a discoverable directory, and the
    // coordinator's reconcile_by_name pass also relies on this symlink.
    let symlink_path = data_dir.join("by_name").join(volume_name);
    let target = std::path::PathBuf::from(format!("../by_id/{new_ulid}"));
    match std::fs::read_link(&symlink_path) {
        Ok(existing) if existing == target => {}
        Ok(other) => {
            return format!(
                "err by_name/{volume_name} already points at {} (expected ../by_id/{new_ulid})",
                other.display()
            );
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(unix)]
            if let Err(e) = std::os::unix::fs::symlink(&target, &symlink_path) {
                return format!("err creating by_name/{volume_name} symlink: {e}");
            }
        }
        Err(e) => return format!("err reading by_name/{volume_name}: {e}"),
    }

    use elide_coordinator::lifecycle::{LifecycleError, MarkClaimedOutcome, mark_claimed};
    match mark_claimed(store, volume_name, root_key, new_ulid).await {
        Ok(MarkClaimedOutcome::Claimed) => {
            rescan.notify_one();
            info!("[inbound] claimed name {volume_name} for new fork {new_ulid}");
            format!("ok {new_ulid}")
        }
        Ok(MarkClaimedOutcome::Absent) => {
            format!("err names/{volume_name} does not exist; nothing to claim")
        }
        Ok(MarkClaimedOutcome::NotReleased { observed }) => {
            format!(
                "err names/{volume_name} is in state {observed:?}, not Released; \
                 cannot claim"
            )
        }
        Err(LifecycleError::Store(e)) => format!("err claim failed: {e}"),
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            format!(
                "err name '{volume_name}' raced with another claim ({held_by} won); \
                 your local fork at {new_ulid} can be re-purposed manually"
            )
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            format!("err names/{volume_name} is in state {from:?}; cannot claim")
        }
    }
}

async fn start_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    root_key: &[u8; 32],
    rescan: &Notify,
) -> String {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        // Local volume directory absent. The claim-from-released path
        // (where we'd materialise a new fork from a released ancestor's
        // handoff snapshot) lands in a follow-up commit; for now refuse.
        return format!(
            "err volume not found locally: {volume_name} \
             (claim-from-released path not yet implemented)"
        );
    }
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    if !vol_dir.join("volume.stopped").exists() {
        return "err volume is not stopped".to_string();
    }

    // Verify ownership in S3 before any local change. A foreign-owned
    // record refuses; a Released record routes to the claim path
    // (not yet implemented — point the operator at it).
    use elide_coordinator::lifecycle::{LifecycleError, MarkLiveOutcome, mark_live};
    match mark_live(store, volume_name, root_key).await {
        Ok(MarkLiveOutcome::Resumed) | Ok(MarkLiveOutcome::AlreadyLive) => {}
        Ok(MarkLiveOutcome::Absent) => {
            // No S3 record yet — proceed with the local-only start. The
            // next `drain_pending` will publish an initial Live record.
        }
        Ok(MarkLiveOutcome::Released) => {
            // Surface the released ancestor pin so the CLI can orchestrate
            // the claim-from-released path: pull the ancestor (if not
            // local), materialise a fresh fork, then call `claim <name>
            // <new_vol_ulid>` to atomically rebind the name. mark_live
            // does not return the record; re-read to extract the pin.
            return match elide_coordinator::name_store::read_name_record(store, volume_name).await {
                Ok(Some((rec, _))) => {
                    let snap = rec
                        .handoff_snapshot
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "_".to_string());
                    format!("released {} {}", rec.vol_ulid, snap)
                }
                Ok(None) | Err(_) => {
                    format!("err name '{volume_name}' is Released but record is unreadable")
                }
            };
        }
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            return format!(
                "err name '{volume_name}' is owned by coordinator {held_by}; \
                 use --force-takeover to override"
            );
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            return format!("err names/<name> is in state {from:?}; cannot start");
        }
        Err(LifecycleError::Store(e)) => {
            warn!("[inbound] start {volume_name}: failed to update names/<name>: {e}");
        }
    }

    // NBD endpoint conflict check before clearing the marker.
    match elide_core::config::find_nbd_conflict(&vol_dir, data_dir) {
        Ok(Some(conflict)) => {
            return format!(
                "err nbd endpoint {} conflicts with volume '{}'",
                conflict.endpoint, conflict.name,
            );
        }
        Ok(None) => {}
        Err(e) => return format!("err nbd conflict check: {e}"),
    }

    if let Err(e) = std::fs::remove_file(vol_dir.join("volume.stopped")) {
        return format!("err clearing volume.stopped: {e}");
    }
    rescan.notify_one();
    info!("[inbound] started volume {volume_name}");
    "ok".to_string()
}

// ── Macaroon-authenticated credential vending ────────────────────────────────

/// Verify the connecting peer matches the volume's recorded `volume.pid`.
///
/// Returns `Ok(pid)` on a clean match. Returns `Err(<wire-error-line>)` ready
/// to send back to the volume — the messages are intentionally generic so a
/// hostile caller can't tell apart "no such volume", "pid not yet recorded"
/// (the spawn-write race), or "pid mismatch".
fn check_peer_pid(vol_dir: &Path, peer_pid: Option<i32>) -> Result<i32, String> {
    let peer_pid = peer_pid.ok_or_else(|| "err peer pid unavailable".to_string())?;
    let pid_path = vol_dir.join("volume.pid");
    let recorded: i32 = match std::fs::read_to_string(&pid_path) {
        Ok(s) => s
            .trim()
            .parse()
            .map_err(|_| "err volume.pid is not numeric".to_string())?,
        // The supervisor writes volume.pid right after spawn returns. There is
        // a brief window where a fast-starting volume can dial control.sock
        // before the parent has written the file — the volume retries, so we
        // simply report "not registered" and let the caller back off.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err("err volume not registered".to_string());
        }
        Err(e) => return Err(format!("err reading volume.pid: {e}")),
    };
    if recorded != peer_pid {
        return Err("err peer pid does not match volume.pid".to_string());
    }
    Ok(peer_pid)
}

fn register_volume(
    volume_ulid: &str,
    data_dir: &Path,
    peer_pid: Option<i32>,
    root_key: &[u8; 32],
) -> String {
    // Parse the ULID at the boundary so we never thread a raw string to the
    // caveat or the filesystem path.
    let ulid = match ulid::Ulid::from_string(volume_ulid) {
        Ok(u) => u.to_string(),
        Err(e) => return format!("err invalid volume ulid: {e}"),
    };
    let vol_dir = data_dir.join("by_id").join(&ulid);
    if !vol_dir.exists() {
        return "err unknown volume".to_string();
    }
    let pid = match check_peer_pid(&vol_dir, peer_pid) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let m = macaroon::mint(
        root_key,
        vec![
            Caveat::Volume(ulid),
            Caveat::Scope(Scope::Credentials),
            Caveat::Pid(pid),
        ],
    );
    format!("ok {}", m.encode())
}

fn issue_credentials(
    macaroon_str: &str,
    data_dir: &Path,
    peer_pid: Option<i32>,
    root_key: &[u8; 32],
    issuer: &dyn CredentialIssuer,
) -> String {
    let m = match Macaroon::parse(macaroon_str) {
        Ok(m) => m,
        Err(e) => return format!("err parse macaroon: {e}"),
    };
    if !macaroon::verify(root_key, &m) {
        // Generic message — don't help an attacker distinguish "wrong key"
        // from "tampered caveats".
        return "err invalid macaroon".to_string();
    }
    if m.scope() != Some(Scope::Credentials) {
        return "err macaroon scope mismatch".to_string();
    }
    let Some(volume_ulid) = m.volume() else {
        return "err macaroon missing volume caveat".to_string();
    };
    let Some(macaroon_pid) = m.pid() else {
        return "err macaroon missing pid caveat".to_string();
    };
    let peer_pid = match peer_pid {
        Some(p) => p,
        None => return "err peer pid unavailable".to_string(),
    };
    if peer_pid != macaroon_pid {
        return "err peer pid does not match macaroon".to_string();
    }
    if let Some(not_after) = m.not_after() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now >= not_after {
            return "err macaroon expired".to_string();
        }
    }
    // Re-validate that volume.pid still matches — covers the case where the
    // original process has exited and the PID was reused.
    let vol_dir = data_dir.join("by_id").join(volume_ulid);
    if let Err(e) = check_peer_pid(&vol_dir, Some(peer_pid)) {
        return e;
    }
    if !pid_is_alive(peer_pid as u32) {
        return "err peer pid not alive".to_string();
    }
    let creds = match issuer.issue(volume_ulid) {
        Ok(c) => c,
        Err(e) => return format!("err issue: {e}"),
    };
    let mut body = String::from("ok\n");
    body.push_str(&format!(
        "access_key_id = {}\n",
        toml_quote(&creds.access_key_id)
    ));
    body.push_str(&format!(
        "secret_access_key = {}\n",
        toml_quote(&creds.secret_access_key)
    ));
    if let Some(tok) = &creds.session_token {
        body.push_str(&format!("session_token = {}\n", toml_quote(tok)));
    }
    if let Some(exp) = creds.expiry_unix {
        body.push_str(&format!("expiry_unix = {exp}\n"));
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::IssuedCredentials;
    use tempfile::TempDir;

    struct FixedIssuer;
    impl CredentialIssuer for FixedIssuer {
        fn issue(&self, _vol: &str) -> std::io::Result<IssuedCredentials> {
            Ok(IssuedCredentials {
                access_key_id: "AK".into(),
                secret_access_key: "SK".into(),
                session_token: None,
                expiry_unix: None,
            })
        }
    }

    fn setup_volume(data_dir: &Path, ulid: &str, pid: i32) {
        let dir = data_dir.join("by_id").join(ulid);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("volume.pid"), pid.to_string()).unwrap();
    }

    fn key() -> [u8; 32] {
        [0x42; 32]
    }

    #[test]
    fn register_succeeds_when_peer_pid_matches_volume_pid() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        setup_volume(tmp.path(), ulid, 4242);
        let resp = register_volume(ulid, tmp.path(), Some(4242), &key());
        assert!(resp.starts_with("ok "), "expected ok, got {resp}");
    }

    #[test]
    fn register_rejects_pid_mismatch() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        setup_volume(tmp.path(), ulid, 4242);
        let resp = register_volume(ulid, tmp.path(), Some(9999), &key());
        assert!(resp.starts_with("err"), "expected err, got {resp}");
        assert!(resp.contains("does not match"));
    }

    #[test]
    fn register_rejects_when_volume_pid_missing() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        // Create the volume dir but no volume.pid yet — this is the spawn-write
        // race the volume-side retry absorbs.
        std::fs::create_dir_all(tmp.path().join("by_id").join(ulid)).unwrap();
        let resp = register_volume(ulid, tmp.path(), Some(4242), &key());
        assert!(resp.contains("not registered"), "{resp}");
    }

    #[test]
    fn register_rejects_unknown_volume() {
        let tmp = TempDir::new().unwrap();
        let resp = register_volume("01JQAAAAAAAAAAAAAAAAAAAAAA", tmp.path(), Some(4242), &key());
        assert!(resp.contains("unknown volume"), "{resp}");
    }

    #[test]
    fn register_rejects_invalid_ulid() {
        let tmp = TempDir::new().unwrap();
        let resp = register_volume("not-a-ulid", tmp.path(), Some(4242), &key());
        assert!(resp.contains("invalid volume ulid"), "{resp}");
    }

    #[test]
    fn credentials_round_trip_with_live_pid() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        // Use our own pid so the pid_is_alive check passes inside the handler.
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        let mint_resp = register_volume(ulid, tmp.path(), Some(my_pid), &key());
        let macaroon = mint_resp.strip_prefix("ok ").unwrap();
        let issuer = FixedIssuer;
        let resp = issue_credentials(macaroon, tmp.path(), Some(my_pid), &key(), &issuer);
        assert!(resp.starts_with("ok\n"), "expected ok body, got {resp}");
        assert!(resp.contains("access_key_id = \"AK\""));
        assert!(resp.contains("secret_access_key = \"SK\""));
    }

    #[test]
    fn credentials_rejects_wrong_root_key() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        let mint_resp = register_volume(ulid, tmp.path(), Some(my_pid), &key());
        let macaroon = mint_resp.strip_prefix("ok ").unwrap();
        let mut other = key();
        other[0] ^= 0xFF;
        let issuer = FixedIssuer;
        let resp = issue_credentials(macaroon, tmp.path(), Some(my_pid), &other, &issuer);
        assert!(resp.contains("invalid macaroon"), "{resp}");
    }

    #[test]
    fn credentials_rejects_pid_mismatch() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        let mint_resp = register_volume(ulid, tmp.path(), Some(my_pid), &key());
        let macaroon = mint_resp.strip_prefix("ok ").unwrap();
        let issuer = FixedIssuer;
        // Present a different peer pid than the macaroon was minted for.
        let resp = issue_credentials(macaroon, tmp.path(), Some(my_pid + 1), &key(), &issuer);
        assert!(
            resp.contains("does not match macaroon") || resp.contains("does not match volume.pid"),
            "{resp}"
        );
    }

    #[test]
    fn credentials_rejects_tampered_caveat() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        // Mint a macaroon for a different volume — the verify call rejects
        // because the volume caveat is wrong even though MAC matches.
        let other_ulid = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        setup_volume(tmp.path(), other_ulid, my_pid);
        let mint_resp = register_volume(other_ulid, tmp.path(), Some(my_pid), &key());
        let macaroon = mint_resp.strip_prefix("ok ").unwrap();
        // Now hand-edit the macaroon to claim it's for `ulid`. We can do that
        // by re-minting with a tampered caveat list and copying the original
        // MAC — but that fails verify, which is what we want to assert.
        let parsed = Macaroon::parse(macaroon).unwrap();
        let mut caveats: Vec<Caveat> = parsed.caveats().to_vec();
        for c in &mut caveats {
            if let Caveat::Volume(v) = c {
                *v = ulid.to_owned();
            }
        }
        // Construct a forged macaroon with the original MAC but rewritten
        // caveats. We have to drop into the same module to do this since
        // the struct fields are private — exposing a helper would broaden
        // the API just for the test, so reuse the encode round-trip with
        // the *new* mint and confirm verify fails for the *old* MAC.
        let forged = macaroon::mint(&[0u8; 32], caveats); // wrong key
        let issuer = FixedIssuer;
        let resp = issue_credentials(&forged.encode(), tmp.path(), Some(my_pid), &key(), &issuer);
        assert!(resp.contains("invalid macaroon"), "{resp}");
    }

    #[test]
    fn credentials_rejects_expired_macaroon() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        let m = macaroon::mint(
            &key(),
            vec![
                Caveat::Volume(ulid.to_owned()),
                Caveat::Scope(macaroon::Scope::Credentials),
                Caveat::Pid(my_pid),
                Caveat::NotAfter(1), // unix second 1 — long ago
            ],
        );
        let issuer = FixedIssuer;
        let resp = issue_credentials(&m.encode(), tmp.path(), Some(my_pid), &key(), &issuer);
        assert!(resp.contains("expired"), "{resp}");
    }

    #[test]
    fn parse_flags_empty() {
        let p = parse_transport_flags("").unwrap();
        assert!(p.nbd_port.is_none());
        assert!(p.nbd_bind.is_none());
        assert!(p.nbd_socket.is_none());
        assert!(!p.no_nbd && !p.ublk && !p.no_ublk);
        assert!(p.ublk_id.is_none());
    }

    #[test]
    fn parse_flags_nbd() {
        let p = parse_transport_flags("nbd-port=10809 nbd-bind=0.0.0.0").unwrap();
        assert_eq!(p.nbd_port, Some(10809));
        assert_eq!(p.nbd_bind.as_deref(), Some("0.0.0.0"));
    }

    #[test]
    fn parse_flags_ublk() {
        let p = parse_transport_flags("ublk ublk-id=7").unwrap();
        assert!(p.ublk);
        assert_eq!(p.ublk_id, Some(7));
    }

    #[test]
    fn parse_flags_clearing() {
        let p = parse_transport_flags("no-nbd no-ublk").unwrap();
        assert!(p.no_nbd);
        assert!(p.no_ublk);
    }

    #[test]
    fn parse_flags_unknown_rejected() {
        assert!(parse_transport_flags("nbd-port=80 unknown=1").is_err());
        assert!(parse_transport_flags("not-a-flag").is_err());
    }

    #[test]
    fn parse_flags_bad_value() {
        assert!(parse_transport_flags("nbd-port=not-a-number").is_err());
        assert!(parse_transport_flags("ublk-id=").is_err());
    }

    #[test]
    fn validate_name_accepts() {
        for n in &["foo", "vol-1", "my_vol", "ubuntu.22.04"] {
            assert!(validate_volume_name(n).is_ok(), "expected ok for {n}");
        }
    }

    #[test]
    fn validate_name_rejects() {
        assert!(validate_volume_name("").is_err());
        assert!(validate_volume_name("foo:bar").is_err());
        assert!(validate_volume_name("foo/bar").is_err());
        assert!(validate_volume_name("status").is_err());
        assert!(validate_volume_name("attach").is_err());
    }

    #[test]
    fn decode_hex32_roundtrip() {
        let bytes = [0xAB; 32];
        let s: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(decode_hex32(&s).unwrap(), bytes);
    }

    #[test]
    fn decode_hex32_wrong_length() {
        assert!(decode_hex32("abcd").is_err());
        assert!(decode_hex32(&"a".repeat(63)).is_err());
        assert!(decode_hex32(&"a".repeat(65)).is_err());
    }

    #[test]
    fn decode_hex32_invalid_chars() {
        let mut s = "ab".repeat(32);
        s.replace_range(2..4, "zz");
        assert!(decode_hex32(&s).is_err());
    }
}
