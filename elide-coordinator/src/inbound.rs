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
use elide_coordinator::config::StoreSection;
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
    store_config: Arc<StoreSection>,
    part_size_bytes: usize,
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
                let store_cfg = store_config.clone();
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
        &store_config,
        part_size_bytes,
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
    match elide_coordinator::control::reclaim(&fork_dir).await {
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
    //    is no valid tag for a snapshot over zero segments.
    let snap_ulid = match pick_snapshot_ulid(&fork_dir) {
        Ok(Some(u)) => u,
        Ok(None) => return format!("err volume '{vol_name}' has no segments to snapshot"),
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

// ── Helpers ───────────────────────────────────────────────────────────────────

fn pid_is_alive(pid: u32) -> bool {
    let Ok(raw) = i32::try_from(pid) else {
        return false;
    };
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(raw), None).is_ok()
}
