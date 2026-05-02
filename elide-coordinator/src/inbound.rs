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
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::claim::{ClaimJob, ClaimJobState, ClaimRegistry};
use crate::credential::CredentialIssuer;
use crate::fork::{ForkJob, ForkJobState, ForkRegistry};
use crate::import::{self, ImportRegistry, ImportState};
use crate::macaroon::{self, Caveat, Macaroon, Scope};
use elide_coordinator::config::StoreSection;
use elide_coordinator::eligibility::Eligibility;
use elide_coordinator::ipc::{
    self, ClaimAttachEvent, ClaimReply, ClaimStartReply, CreateReply, Envelope, EvictReply,
    ForceSnapshotNowReply, ForkAttachEvent, ForkCreateReply, ForkSource, ForkStartReply,
    GenerateFilemapReply, ImportAttachEvent, ImportStartReply, ImportStatusReply, IpcError,
    LatestSnapshotReply, PullReadonlyReply, RegisterReply, ReleaseReply, Request,
    ResolveHandoffKeyReply, ResolveNameReply, SnapshotReply, StatusRemoteReply, StatusReply,
    StoreConfigReply, StoreCredsReply, UpdateReply, VolumeEventsReply,
};
use elide_coordinator::volume_state::{IMPORT_LOCK_FILE, PID_FILE, STOPPED_FILE};
use elide_coordinator::{
    EvictRegistry, PrefetchTracker, SnapshotLockRegistry, register_prefetch_or_get,
    subscribe_prefetch,
};
use elide_core::process::pid_is_alive;

/// Shared coordinator state threaded through every inbound op.
///
/// All fields are cheap to clone (Arc-wrapped or Copy), so per-connection
/// fan-out in `serve` is a flat clone rather than a long argument list.
#[derive(Clone)]
pub struct IpcContext {
    pub data_dir: Arc<PathBuf>,
    pub rescan: Arc<Notify>,
    pub registry: ImportRegistry,
    pub fork_registry: ForkRegistry,
    pub claim_registry: ClaimRegistry,
    pub elide_import_bin: Arc<PathBuf>,
    pub evict_registry: EvictRegistry,
    pub snapshot_locks: SnapshotLockRegistry,
    pub prefetch_tracker: PrefetchTracker,
    pub stores: Arc<dyn elide_coordinator::stores::ScopedStores>,
    pub store_config: Arc<StoreSection>,
    pub part_size_bytes: usize,
    /// Crockford-Base32 ULID-shaped coordinator id, derived once at
    /// startup from `coordinator.pub`. Cheap to clone (26 bytes).
    pub coord_id: String,
    /// 32-byte MAC root for `macaroon::mint` / `macaroon::verify`,
    /// derived in-memory from `coordinator.key` at startup.
    pub macaroon_root: [u8; 32],
    /// The coordinator's identity bundle, used as a `SegmentSigner`
    /// when minting synthesised handoff snapshots during
    /// `volume release --force`. Arc-shared so per-connection clones
    /// stay cheap.
    pub identity: Arc<elide_coordinator::identity::CoordinatorIdentity>,
    pub issuer: Arc<dyn CredentialIssuer>,
}

pub async fn serve(socket_path: &Path, ctx: IpcContext) {
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
                tokio::spawn(handle(stream, ctx.clone()));
            }
            Err(e) => warn!("[inbound] accept error: {e}"),
        }
    }
}

async fn handle(stream: tokio::net::UnixStream, ctx: IpcContext) {
    // Capture peer credentials before splitting the stream — needed for
    // SO_PEERCRED on the `register` and `credentials` verbs. Other
    // verbs ignore the peer pid; capturing once here keeps the code
    // symmetric and avoids a second syscall later.
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

    dispatch_json(&line, &ctx, peer_pid, &mut writer).await;
}

/// Typed JSON dispatch. Each match arm runs the verb-specific
/// handler, wraps the result in an [`Envelope`], and writes one or
/// more reply messages back. Most verbs reply with a single
/// envelope; the streaming variant (`ImportAttach`) writes a
/// sequence terminated by either an `Ok(Done)` or `Err`. Unknown
/// verbs fail at the `serde_json::from_str` step — `serde` rejects
/// unrecognised variants by default for internally-tagged enums.
async fn dispatch_json(
    line: &str,
    ctx: &IpcContext,
    peer_pid: Option<i32>,
    writer: &mut OwnedWriteHalf,
) {
    let request: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            let env: Envelope<()> =
                Envelope::err(IpcError::bad_request(format!("parse request: {e}")));
            let _ = ipc::write_message(writer, &env).await;
            return;
        }
    };

    match request {
        Request::Rescan => {
            ctx.rescan.notify_one();
            let env: Envelope<()> = Envelope::ok(());
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Status { volume } => {
            let env: Envelope<StatusReply> = volume_status_typed(&volume, &ctx.data_dir).into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::StatusRemote { volume } => {
            // Reads names/<volume>: coordinator-wide.
            let store = ctx.stores.coordinator_wide();
            let result = volume_status_remote_typed(&volume, &store, &ctx.coord_id).await;
            let env: Envelope<StatusRemoteReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Stop { volume } => {
            // Conditional PUT on names/<volume>: coordinator-wide.
            let store = ctx.stores.coordinator_wide();
            let result = stop_volume_op(
                &volume,
                &ctx.data_dir,
                &store,
                &ctx.coord_id,
                ctx.identity.hostname(),
            )
            .await;
            let env: Envelope<()> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Release { volume, force } => {
            // Mixed: per-volume snapshot publish + names/<volume> flip.
            // Coordinator-wide today; future Tigris work splits this.
            let store = ctx.stores.coordinator_wide();
            let result = if force {
                force_release_volume_op(&volume, &ctx.data_dir, &store, &ctx.identity).await
            } else {
                release_volume_op(
                    &volume,
                    &ctx.data_dir,
                    &ctx.snapshot_locks,
                    &store,
                    ctx.part_size_bytes,
                    &ctx.identity,
                    &ctx.rescan,
                )
                .await
            };
            let env: Envelope<ReleaseReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Start { volume } => {
            // Conditional PUT on names/<volume>: coordinator-wide.
            let store = ctx.stores.coordinator_wide();
            let result = start_volume_op(
                &volume,
                &ctx.data_dir,
                &store,
                &ctx.coord_id,
                ctx.identity.hostname(),
                &ctx.rescan,
            )
            .await;
            let env: Envelope<()> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Create {
            volume,
            size_bytes,
            flags,
        } => {
            // Mixed: names/<volume> claim + per-volume artefacts.
            let store = ctx.stores.coordinator_wide();
            let result = create_volume_op(
                &volume,
                size_bytes,
                &flags,
                &ctx.data_dir,
                &store,
                &ctx.identity,
                &ctx.rescan,
            )
            .await;
            let env: Envelope<CreateReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Update { volume, flags } => {
            let result = update_volume_op(&volume, &flags, &ctx.data_dir).await;
            let env: Envelope<UpdateReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::ImportStart {
            volume,
            oci_ref,
            extents_from,
        } => {
            // Mixed: names/<volume> mark_initial; the spawned import
            // subprocess writes locally only.
            let store = ctx.stores.coordinator_wide();
            let result = start_import(
                &volume,
                &oci_ref,
                &extents_from,
                &ctx.data_dir,
                &ctx.elide_import_bin,
                &ctx.registry,
                &store,
                &ctx.rescan,
                &ctx.identity,
            )
            .await;
            let env: Envelope<ImportStartReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::ImportStatus { volume } => {
            let result = import_status_by_name(&volume, &ctx.data_dir, &ctx.registry).await;
            let env: Envelope<ImportStatusReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::ImportAttach { volume } => {
            stream_import_by_name(&volume, &ctx.data_dir, writer, &ctx.registry).await;
        }
        Request::Snapshot { volume } => {
            // Per-volume artefact upload + events/<volume>/ emit.
            // Coordinator-wide today; future Tigris work splits the
            // upload from the event emit.
            let store = ctx.stores.coordinator_wide();
            let result = snapshot_volume(
                &volume,
                &ctx.data_dir,
                &ctx.snapshot_locks,
                &store,
                ctx.part_size_bytes,
            )
            .await;
            let env: Envelope<SnapshotReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Evict {
            volume,
            segment_ulid,
        } => {
            let result =
                evict_volume(&volume, segment_ulid, &ctx.data_dir, &ctx.evict_registry).await;
            let env: Envelope<EvictReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::GenerateFilemap { volume, snap_ulid } => {
            // Demand-fetches segment bodies from the ancestor chain
            // for filemap generation: cross-volume reads, so
            // coordinator-wide.
            let store = ctx.stores.coordinator_wide();
            let result = generate_filemap_op(&volume, snap_ulid, &ctx.data_dir, &store).await;
            let env: Envelope<GenerateFilemapReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::AwaitPrefetch { vol_ulid } => {
            let result = await_prefetch_op(vol_ulid, &ctx.prefetch_tracker).await;
            let env: Envelope<()> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Remove { volume, force } => {
            let result = remove_volume(&volume, force, &ctx.data_dir);
            let env: Envelope<()> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::VolumeEvents { volume } => {
            // Reads events/<volume>/: coordinator-wide.
            let store = ctx.stores.coordinator_wide();
            let result = volume_events_typed(&volume, &store).await;
            let env: Envelope<VolumeEventsReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::GetStoreConfig => {
            let reply = render_store_config(&ctx.store_config);
            let env: Envelope<StoreConfigReply> = Envelope::ok(reply);
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Register { volume_ulid } => {
            let result = register_volume(volume_ulid, &ctx.data_dir, peer_pid, &ctx.macaroon_root);
            let env: Envelope<RegisterReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::Credentials { macaroon } => {
            let result = issue_credentials(
                &macaroon,
                &ctx.data_dir,
                peer_pid,
                &ctx.macaroon_root,
                ctx.issuer.as_ref(),
            );
            let env: Envelope<StoreCredsReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }

        // ── Iteration 5: consolidated fork / claim flows ─────────────
        Request::ForkStart {
            new_name,
            from,
            force_snapshot,
            flags,
        } => {
            let result = start_fork(new_name, from, force_snapshot, flags, ctx.clone());
            let env: Envelope<ForkStartReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::ForkAttach { new_name } => {
            stream_fork_by_name(&new_name, writer, &ctx.fork_registry).await;
        }
        Request::ClaimStart { volume } => {
            let result = start_claim(volume, ctx.clone()).await;
            let env: Envelope<ClaimStartReply> = result.into();
            let _ = ipc::write_message(writer, &env).await;
        }
        Request::ClaimAttach { volume } => {
            stream_claim_by_name(&volume, writer, &ctx.claim_registry).await;
        }
    }
}

// ── Store config / creds vending ──────────────────────────────────────────────

/// Resolve the store config the CLI should see. If neither a local
/// path nor a bucket is configured, falls back to `./elide_store` to
/// match the coordinator's own default — the CLI builds an
/// object_store from this and must agree with the coordinator on
/// where the bytes live.
fn render_store_config(store: &StoreSection) -> StoreConfigReply {
    if let Some(path) = &store.local_path {
        return StoreConfigReply {
            local_path: Some(path.display().to_string()),
            ..Default::default()
        };
    }
    if let Some(bucket) = &store.bucket {
        return StoreConfigReply {
            bucket: Some(bucket.clone()),
            endpoint: store.endpoint.clone(),
            region: store.region.clone(),
            ..Default::default()
        };
    }
    StoreConfigReply {
        local_path: Some("elide_store".to_owned()),
        ..Default::default()
    }
}

/// Resolve `names/<name>` in the bucket and return the bound `vol_ulid`.
/// Errors NotFound when the name has no S3 record.
async fn resolve_name_op(
    name: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<ResolveNameReply, IpcError> {
    validate_volume_name(name).map_err(IpcError::bad_request)?;
    match elide_coordinator::name_store::read_name_record(store, name).await {
        Ok(Some((rec, _))) => Ok(ResolveNameReply {
            vol_ulid: rec.vol_ulid,
        }),
        Ok(None) => Err(IpcError::not_found(format!(
            "volume '{name}' not found in store"
        ))),
        Err(e) => Err(IpcError::store(format!("reading names/{name}: {e}"))),
    }
}

/// LIST `by_id/<vol_ulid>/snapshots/` and return the highest snapshot
/// ULID. Filemap and manifest siblings (filenames containing `.`) are
/// filtered out so only the bare snapshot markers contribute.
async fn latest_snapshot_op(
    vol_ulid: ulid::Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<LatestSnapshotReply, IpcError> {
    use futures::TryStreamExt;
    use object_store::path::Path as StorePath;

    let prefix = StorePath::from(format!("by_id/{vol_ulid}/snapshots/"));
    let objects: Vec<object_store::ObjectMeta> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(|e| IpcError::store(format!("listing by_id/{vol_ulid}/snapshots/: {e}")))?;

    let mut latest: Option<ulid::Ulid> = None;
    for obj in objects {
        let Some(filename) = obj.location.filename() else {
            continue;
        };
        if filename.contains('.') {
            continue;
        }
        if let Ok(u) = ulid::Ulid::from_string(filename)
            && latest.is_none_or(|cur| u > cur)
        {
            latest = Some(u);
        }
    }
    Ok(LatestSnapshotReply {
        snapshot_ulid: latest,
    })
}

// ── Import operations ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn start_import(
    volume: &str,
    oci_ref: &str,
    extents_from: &[String],
    data_dir: &Path,
    elide_import_bin: &Path,
    registry: &ImportRegistry,
    store: &Arc<dyn ObjectStore>,
    rescan: &Arc<Notify>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
) -> Result<ImportStartReply, IpcError> {
    let req = import::ImportRequest {
        vol_name: volume,
        oci_ref,
        extents_from,
    };
    let ulid_str = import::spawn_import(
        req,
        data_dir,
        elide_import_bin,
        registry,
        store.clone(),
        rescan.clone(),
        identity.clone(),
    )
    .await
    .map_err(|e| IpcError::internal(format!("{e}")))?;
    let import_ulid = ulid::Ulid::from_string(&ulid_str)
        .map_err(|e| IpcError::internal(format!("import ulid {ulid_str:?}: {e}")))?;
    Ok(ImportStartReply { import_ulid })
}

/// Resolve a volume name to its import ULID via `import.lock`, if present.
fn import_ulid_for_volume(name: &str, data_dir: &Path) -> Option<String> {
    let lock_path = data_dir.join("by_name").join(name).join(IMPORT_LOCK_FILE);
    std::fs::read_to_string(lock_path)
        .ok()
        .map(|s| s.trim().to_owned())
}

async fn import_status_by_name(
    name: &str,
    data_dir: &Path,
    registry: &ImportRegistry,
) -> Result<ImportStatusReply, IpcError> {
    let vol_dir = data_dir.join("by_name").join(name);
    if !vol_dir.exists() {
        return Err(IpcError::not_found(format!("volume not found: {name}")));
    }
    let Some(ulid) = import_ulid_for_volume(name, data_dir) else {
        // No active import lock — if volume is readonly the import completed.
        if vol_dir.join("volume.readonly").exists() {
            return Ok(ImportStatusReply::Done);
        }
        return Err(IpcError::not_found(format!("no active import for: {name}")));
    };
    let job = registry
        .lock()
        .expect("import registry poisoned")
        .get(&ulid)
        .cloned();
    match job {
        // import.lock exists but not in registry (coordinator restarted mid-import)
        None => Ok(ImportStatusReply::Running),
        Some(job) => match job.state() {
            ImportState::Running => Ok(ImportStatusReply::Running),
            ImportState::Done => Ok(ImportStatusReply::Done),
            ImportState::Failed(msg) => Err(IpcError::internal(format!("import failed: {msg}"))),
        },
    }
}

/// Stream buffered and live import output to `writer` as a sequence of
/// [`Envelope<ImportAttachEvent>`] messages, terminating with either
/// `ImportAttachEvent::Done` (success) or `Envelope::Err` (failure).
async fn stream_import_by_name(
    name: &str,
    data_dir: &Path,
    writer: &mut OwnedWriteHalf,
    registry: &ImportRegistry,
) {
    async fn write_err(writer: &mut OwnedWriteHalf, error: IpcError) {
        let env: Envelope<ImportAttachEvent> = Envelope::err(error);
        let _ = ipc::write_message(writer, &env).await;
    }

    let vol_dir = data_dir.join("by_name").join(name);
    if !vol_dir.exists() {
        write_err(
            writer,
            IpcError::not_found(format!("volume not found: {name}")),
        )
        .await;
        return;
    }
    let Some(ulid) = import_ulid_for_volume(name, data_dir) else {
        // No active import — if volume is readonly the import already completed.
        if vol_dir.join("volume.readonly").exists() {
            let env: Envelope<ImportAttachEvent> = Envelope::ok(ImportAttachEvent::Done);
            let _ = ipc::write_message(writer, &env).await;
        } else {
            write_err(
                writer,
                IpcError::not_found(format!("no active import for: {name}")),
            )
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
        // We can't stream output we never buffered.
        write_err(
            writer,
            IpcError::internal("import output unavailable (coordinator restarted)"),
        )
        .await;
        return;
    };

    let mut offset = 0;
    loop {
        let lines = job.read_from(offset);
        for line in &lines {
            let env: Envelope<ImportAttachEvent> = Envelope::ok(ImportAttachEvent::Line {
                content: line.clone(),
            });
            if ipc::write_message(writer, &env).await.is_err() {
                return; // client disconnected
            }
        }
        offset += lines.len();

        match job.state() {
            ImportState::Done => {
                let env: Envelope<ImportAttachEvent> = Envelope::ok(ImportAttachEvent::Done);
                let _ = ipc::write_message(writer, &env).await;
                return;
            }
            ImportState::Failed(msg) => {
                write_err(writer, IpcError::internal(format!("import failed: {msg}"))).await;
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
    segment_ulid: Option<ulid::Ulid>,
    data_dir: &Path,
    evict_registry: &EvictRegistry,
) -> Result<EvictReply, IpcError> {
    // Resolve name → fork directory path using the same construction as
    // daemon.rs (data_dir.join("by_id/<ulid>")), so the key matches the
    // EvictRegistry entry.  canonicalize() returns an absolute path which
    // would not match when data_dir is relative.
    let link = data_dir.join("by_name").join(vol_name);
    let target = std::fs::read_link(&link)
        .map_err(|_| IpcError::not_found(format!("volume not found: {vol_name}")))?;
    // The symlink target is ../by_id/<ulid>; extract just the ULID component.
    let ulid_component = target
        .file_name()
        .ok_or_else(|| IpcError::internal(format!("malformed volume symlink: {vol_name}")))?;
    let fork_dir = data_dir.join("by_id").join(ulid_component);

    // Look up the fork's evict sender.
    let sender = evict_registry
        .lock()
        .expect("evict registry poisoned")
        .get(&fork_dir)
        .cloned();
    let sender = sender.ok_or_else(|| {
        IpcError::conflict(format!("volume not managed by coordinator: {vol_name}"))
    })?;

    // Send the request and wait for the result. The fork task accepts an
    // owned ULID string for legacy reasons; encode the typed `Ulid` back.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let ulid_str = segment_ulid.map(|u| u.to_string());
    if sender.send((ulid_str, reply_tx)).await.is_err() {
        return Err(IpcError::internal("fork task no longer running"));
    }
    match reply_rx.await {
        Ok(Ok(n)) => Ok(EvictReply { evicted: n }),
        Ok(Err(e)) => Err(IpcError::internal(format!("{e}"))),
        Err(_) => Err(IpcError::internal("fork task dropped reply")),
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
) -> Result<SnapshotReply, IpcError> {
    let link = data_dir.join("by_name").join(vol_name);
    let fork_dir = std::fs::canonicalize(&link)
        .map_err(|_| IpcError::not_found(format!("volume not found: {vol_name}")))?;
    if !fork_dir.join("control.sock").exists() {
        return Err(IpcError::conflict(format!(
            "volume '{vol_name}' is not running — start it first"
        )));
    }
    if fork_dir.join("volume.readonly").exists() {
        return Err(IpcError::conflict(format!(
            "volume '{vol_name}' is readonly"
        )));
    }

    let volume_id = elide_coordinator::upload::derive_names(&fork_dir)
        .map_err(|e| IpcError::internal(format!("deriving volume id: {e}")))?;

    let lock = elide_coordinator::snapshot_lock_for(snapshot_locks, &fork_dir);
    let _guard = lock.lock_owned().await;

    // 1. Promote WAL into pending/.
    if !elide_coordinator::control::promote_wal(&fork_dir).await {
        return Err(IpcError::internal(
            "promote_wal failed or volume unreachable",
        ));
    }

    // 2. Inline drain: upload every pending segment, promote each, then
    //    upload any snapshot files already sitting under snapshots/.
    //    We run this before sign_snapshot_manifest so that index/ is populated
    //    with every segment up to the flush point.
    match elide_coordinator::upload::drain_pending(&fork_dir, &volume_id, store, part_size_bytes)
        .await
    {
        Ok(r) if r.failed > 0 => {
            return Err(IpcError::store(format!(
                "drain reported {} failed segment(s)",
                r.failed
            )));
        }
        Ok(_) => {}
        Err(e) => return Err(IpcError::store(format!("drain: {e:#}"))),
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
    elide_coordinator::gc::apply_done_handoffs(&fork_dir, &volume_id, store, part_size_bytes)
        .await
        .map_err(|e| IpcError::store(format!("draining gc handoffs: {e:#}")))?;

    // 4. Pick snap_ulid: the max ULID in index/, or a freshly-minted
    //    one when the volume has no segments. The volume's
    //    `sign_snapshot_manifest` accepts either (see
    //    `elide_core::volume::Volume::sign_snapshot_manifest` doc) —
    //    an empty snapshot is just a signed manifest with zero
    //    entries, valid for the claim path to fork from.
    let snap_ulid = pick_snapshot_ulid(&fork_dir)
        .map_err(|e| IpcError::internal(format!("picking snap_ulid: {e}")))?;

    // 5. Tell the volume to sign and write the manifest + marker.
    if !elide_coordinator::control::sign_snapshot_manifest(&fork_dir, snap_ulid).await {
        return Err(IpcError::internal(format!(
            "sign_snapshot_manifest {snap_ulid} failed"
        )));
    }

    // Phase 4 (snapshot-time filemap generation) used to run here. It
    // dominated `volume release` wall time on freshly-pulled volumes —
    // ext4 layout scan with per-fragment hash lookup that demand-fetched
    // each missing block range across the ancestor chain (~100s on a
    // cold local fork). The filemap is strictly additive and has only
    // one consumer: `elide volume import --extents-from`. The import
    // path now generates the source filemap on demand if missing, so
    // the release-time work is wasted everywhere else.
    //
    // Imports continue to write the importing volume's filemap inline
    // at import time (`elide-core/src/import.rs`); that path is fast
    // because the importer already has ext4 layout in hand.

    // 5. Upload the new snapshot marker and manifest.
    elide_coordinator::upload::upload_snapshots_and_filemaps(&fork_dir, &volume_id, store)
        .await
        .map_err(|e| IpcError::store(format!("uploading snapshot files: {e:#}")))?;

    info!("[snapshot {volume_id}] committed {snap_ulid}");
    Ok(SnapshotReply { snap_ulid })
}

/// Pick a snapshot ULID as the max ULID in `fork_dir/index/`.
///
/// Returns `None` when `index/` is empty or missing — the coordinator must
/// never mint ULIDs, so an empty fork has no valid snapshot tag and the
/// caller rejects the snapshot request.
/// Pick the ULID to tag a snapshot with: the max segment ULID in
/// `index/` if any are present, else a fresh `Ulid::new()`. Mirrors
/// `force_snapshot_now_op` at `inbound.rs` (the ancestor-pin path),
/// so empty volumes get a valid snapshot tag through both paths.
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
    // `Ulid::default()` is the nil ULID, not a fresh one — we explicitly
    // want a freshly minted ULID when the index is empty.
    #[allow(clippy::unwrap_or_default)]
    Ok(latest.unwrap_or_else(ulid::Ulid::new))
}

// ── Snapshot filemap generation ───────────────────────────────────────────────

/// Handle the `generate-filemap <vol> [<snap_ulid>]` IPC verb.
///
/// Looks up the volume by name, defaults `snap_ulid` to the latest local
/// snapshot when omitted, and runs `filemap::generate_from_snapshot` against
/// it with a coordinator-vended `RemoteFetcher` so demand-fetch works for
/// evicted segments.
///
/// The filemap is the only piece of snapshot metadata that is expensive to
/// produce (ext4 layout walk + per-fragment hash lookup). Used to be inline
/// in `snapshot_volume` but the cost was wasted on the user-visible release
/// path because the only consumer is `elide volume import --extents-from`.
/// Operators wanting to seed a delta source now invoke this verb explicitly.
async fn generate_filemap_op(
    vol_name: &str,
    snap_arg: Option<ulid::Ulid>,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<GenerateFilemapReply, IpcError> {
    let link = data_dir.join("by_name").join(vol_name);
    let fork_dir = std::fs::canonicalize(&link)
        .map_err(|_| IpcError::not_found(format!("volume not found: {vol_name}")))?;

    let snap_ulid = match snap_arg {
        Some(u) => u,
        None => match elide_core::volume::latest_snapshot(&fork_dir) {
            Ok(Some(u)) => u,
            Ok(None) => {
                return Err(IpcError::not_found(format!(
                    "volume '{vol_name}' has no local snapshot; \
                     publish one first with `volume snapshot`"
                )));
            }
            Err(e) => return Err(IpcError::internal(format!("reading snapshots dir: {e}"))),
        },
    };

    // Pre-check that the snapshot the caller asked for is actually
    // sealed on disk. `BlockReader::open_snapshot` would surface a
    // generic NotFound otherwise; a verb-level check makes the error
    // reflect the user's intent ("you named a snapshot that doesn't
    // exist locally" vs. "something deep in the reader failed").
    let manifest_path = fork_dir
        .join("snapshots")
        .join(format!("{snap_ulid}.manifest"));
    if !manifest_path.exists() {
        return Err(IpcError::not_found(format!(
            "snapshot {snap_ulid} not found locally for volume '{vol_name}' \
             (no snapshots/{snap_ulid}.manifest); pull or snapshot first"
        )));
    }

    let started = std::time::Instant::now();
    generate_snapshot_filemap(&fork_dir, snap_ulid, store.clone())
        .await
        .map_err(|e| {
            IpcError::internal(format!("filemap generation failed for {snap_ulid}: {e:#}"))
        })?;
    info!(
        "[generate-filemap {vol_name}] snapshot {snap_ulid} written in {:.2?}",
        started.elapsed()
    );
    Ok(GenerateFilemapReply { snap_ulid })
}

/// Open a sealed snapshot, walk its ext4 layout, and write
/// `snapshots/<snap_ulid>.filemap`. Runs on a worker thread so the tokio
/// reactor stays responsive.
///
/// Wires a `RemoteFetcher` so demand-fetch works for evicted segments —
/// freshly-pulled forks have no local segment bodies, so the ext4 inode
/// table reads must reach S3. The fetcher is constructed inside the
/// closure (after the search-dir list is known) because each fetcher
/// binds to a fork chain.
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

// ── Volume status ─────────────────────────────────────────────────────────────

/// Typed implementation of the `status` verb. Returned to the JSON
/// dispatcher as-is; legacy callers exist as a separate adapter.
fn volume_status_typed(volume_name: &str, data_dir: &Path) -> Result<StatusReply, IpcError> {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return Err(IpcError::not_found(format!(
            "volume not found: {volume_name}"
        )));
    }
    // The OS follows the symlink transparently for all path ops below.
    let lifecycle = elide_coordinator::volume_state::VolumeLifecycle::from_dir(&link);
    Ok(StatusReply { lifecycle })
}

/// Typed implementation of the `status-remote` verb. Reads
/// `names/<volume>` from the bucket and classifies this coordinator's
/// eligibility against it.
async fn volume_status_remote_typed(
    volume_name: &str,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
) -> Result<StatusRemoteReply, IpcError> {
    let record = match elide_coordinator::name_store::read_name_record(store, volume_name).await {
        Ok(Some((rec, _))) => rec,
        Ok(None) => {
            return Err(IpcError::not_found(format!(
                "name '{volume_name}' has no S3 record"
            )));
        }
        Err(e) => {
            return Err(IpcError::store(format!("reading names/{volume_name}: {e}")));
        }
    };

    let eligibility = Eligibility::from_record(&record, coord_id);

    Ok(StatusRemoteReply {
        state: record.state,
        vol_ulid: record.vol_ulid,
        coordinator_id: record.coordinator_id,
        hostname: record.hostname,
        claimed_at: record.claimed_at,
        parent: record.parent,
        handoff_snapshot: record.handoff_snapshot,
        eligibility,
    })
}

/// Typed implementation of the `volume-events` verb. Lists every
/// event under `events/<volume>/`, parsed and signature-
/// verified. A missing prefix returns an empty list — every name
/// has a journal even if no events have been emitted yet, so
/// "empty log" is not the same as "name doesn't exist".
async fn volume_events_typed(
    volume_name: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<VolumeEventsReply, IpcError> {
    let entries = elide_coordinator::volume_event_store::list_and_verify_events(store, volume_name)
        .await
        .map_err(|e| IpcError::store(format!("listing events for {volume_name}: {e}")))?;
    Ok(VolumeEventsReply { events: entries })
}

// ── Volume remove ─────────────────────────────────────────────────────────────

/// Remove the local instance of a volume.
///
/// Removes the on-disk fork (`by_id/<ulid>/`) and its `by_name/<name>`
/// symlink. Does not delete bucket-side records or segments — this is a
/// local-instance verb, not a `purge`.
///
/// Preconditions (without `force`):
///  - `volume.stopped` must be present (the local daemon is halted)
///  - `pending/` and `wal/` must be empty (all writes are durable in S3)
///
/// `force = true` skips the second check, accepting that any local-only
/// pending segments or unflushed WAL records will be discarded.
fn remove_volume(volume_name: &str, force: bool, data_dir: &Path) -> Result<(), IpcError> {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return Err(IpcError::not_found(format!(
            "volume not found: {volume_name}"
        )));
    }

    let vol_dir = std::fs::canonicalize(&link)
        .map_err(|e| IpcError::internal(format!("resolving volume dir: {e}")))?;

    if !vol_dir.join(STOPPED_FILE).exists() {
        return Err(IpcError::conflict(
            "volume is running; stop it first with: elide volume stop <name>",
        ));
    }

    if !force && let Some(reason) = unflushed_state_reason(&vol_dir) {
        return Err(IpcError::conflict(format!(
            "{reason}; take a snapshot first with: elide volume snapshot <name> \
             — or pass --force to discard the unflushed local state"
        )));
    }

    import::kill_all_for_volume(&vol_dir);

    let _ = std::fs::remove_file(&link);

    std::fs::remove_dir_all(&vol_dir)
        .map_err(|e| IpcError::internal(format!("remove failed: {e}")))?;
    info!("[inbound] removed volume {volume_name}");
    Ok(())
}

/// Returns a human-readable reason if the fork has on-disk state that
/// has not been flushed and uploaded to S3, or `None` if the fork is
/// fully durable. Used to gate `remove` and decide whether `--force` is
/// required.
///
/// "Fully durable" means: no segments awaiting upload (`pending/` empty)
/// and no unflushed WAL records (`wal/` empty). Both directories are
/// populated by writes and emptied by the drain pipeline.
fn unflushed_state_reason(vol_dir: &Path) -> Option<String> {
    let dir_has_entries = |sub: &str| {
        std::fs::read_dir(vol_dir.join(sub))
            .map(|mut d| d.next().is_some())
            .unwrap_or(false)
    };
    if dir_has_entries("pending") {
        return Some("local segments are pending upload to S3".to_string());
    }
    if dir_has_entries("wal") {
        return Some("local WAL has unflushed writes".to_string());
    }
    None
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
    name: &str,
    size_bytes: u64,
    flags: &[String],
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
    rescan: &Notify,
) -> Result<CreateReply, IpcError> {
    let coord_id = identity.coordinator_id_str();

    validate_volume_name(name).map_err(IpcError::bad_request)?;
    if size_bytes == 0 {
        return Err(IpcError::bad_request("size must be non-zero"));
    }

    let patch = parse_transport_flags(&flags.join(" ")).map_err(IpcError::bad_request)?;
    if patch.no_nbd || patch.no_ublk {
        return Err(IpcError::bad_request(
            "no-nbd / no-ublk are not valid on create (volume starts without transport)",
        ));
    }
    if (patch.nbd_port.is_some() || patch.nbd_bind.is_some() || patch.nbd_socket.is_some())
        && (patch.ublk || patch.ublk_id.is_some())
    {
        return Err(IpcError::bad_request(
            "nbd-* flags are mutually exclusive with ublk",
        ));
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
        return Err(IpcError::conflict(format!("volume already exists: {name}")));
    }

    let vol_ulid = ulid::Ulid::new();
    let vol_ulid_str = vol_ulid.to_string();
    let vol_dir = data_dir.join("by_id").join(&vol_ulid_str);

    // Phase 1: create the local volume directory, signing key, and
    // signed empty-lineage `volume.provenance`. Both immutable trust
    // artefacts must exist on disk before Phase 2 uploads them, and
    // both must land in S3 *before* `names/<name>` so the invariant
    // "every named vol_ulid has volume.pub *and* volume.provenance in
    // the bucket" holds across crashes. A SIGINT before the upload
    // leaves only local-disk state, which `cleanup_local` rolls back.
    let cleanup_local = || {
        let _ = std::fs::remove_file(by_name_dir.join(name));
        let _ = std::fs::remove_dir_all(&vol_dir);
    };
    if let Err(e) = (|| {
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
        Ok::<_, std::io::Error>(())
    })() {
        cleanup_local();
        return Err(IpcError::internal(format!("create failed: {e}")));
    }

    // Phase 2: publish volume.pub *and* volume.provenance to S3
    // *before* claiming the name. Both files are immutable. A SIGINT
    // here at worst leaves orphan
    // `by_id/<vol_ulid>/{volume.pub, volume.provenance}` (no
    // names/<name> references them, so they're harmless and
    // GC-reclaimable).
    if let Err(e) =
        elide_coordinator::upload::upload_volume_pub_initial(&vol_dir, &vol_ulid_str, store).await
    {
        cleanup_local();
        return Err(IpcError::store(format!("uploading volume.pub: {e:#}")));
    }
    if let Err(e) =
        elide_coordinator::upload::upload_volume_provenance_initial(&vol_dir, &vol_ulid_str, store)
            .await
    {
        cleanup_local();
        return Err(IpcError::store(format!(
            "uploading volume.provenance: {e:#}"
        )));
    }

    // Phase 3: claim the name in S3. After this point the names/<name>
    // record exists in the bucket and references a vol_ulid whose
    // volume.pub is already uploaded — recovery via `release --force`
    // is always possible from here on.
    use elide_coordinator::lifecycle::{LifecycleError, MarkInitialOutcome, mark_initial};
    match mark_initial(store, name, coord_id, identity.hostname(), vol_ulid).await {
        Ok(MarkInitialOutcome::Claimed) => {
            elide_coordinator::volume_event_store::emit_best_effort(
                store,
                identity.as_ref(),
                name,
                elide_core::volume_event::EventKind::Created,
                vol_ulid,
            )
            .await;
        }
        Ok(MarkInitialOutcome::AlreadyExists {
            existing_vol_ulid,
            existing_state,
            existing_owner,
        }) => {
            cleanup_local();
            let owner = existing_owner.as_deref().unwrap_or("<unowned>");
            return Err(IpcError::conflict(format!(
                "name '{name}' already exists in bucket \
                 (vol_ulid={existing_vol_ulid}, state={existing_state:?}, \
                 owner={owner})"
            )));
        }
        Err(LifecycleError::Store(e)) => {
            cleanup_local();
            return Err(IpcError::store(format!("claiming name in bucket: {e}")));
        }
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            // mark_initial doesn't currently surface this, but match
            // exhaustively so a future refactor doesn't silently drop it.
            cleanup_local();
            return Err(IpcError::conflict(format!(
                "name held by another coordinator: {held_by}"
            )));
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            cleanup_local();
            return Err(IpcError::conflict(format!(
                "names/<name> is in unexpected state {from:?}"
            )));
        }
    }

    // Phase 4: finish local state (config + by_name symlink).
    // Provenance was already written in Phase 1 so it could be
    // uploaded in Phase 2 ahead of the names/<name> claim.
    let result: std::io::Result<()> = (|| {
        elide_core::config::VolumeConfig {
            name: Some(name.to_owned()),
            size: Some(size_bytes),
            nbd: nbd_cfg,
            ublk: ublk_cfg,
        }
        .write(&vol_dir)?;
        std::fs::create_dir_all(&by_name_dir)?;
        std::os::unix::fs::symlink(format!("../by_id/{vol_ulid_str}"), by_name_dir.join(name))?;
        Ok(())
    })();

    if let Err(e) = result {
        // Phase-4 failure: roll back local artefacts *and* the bucket-side
        // claim so the name is free to retry. The orphan volume.pub stays
        // in S3 — it's not pointed to by anything and will be reclaimed
        // by future GC.
        cleanup_local();
        let name_key = object_store::path::Path::from(format!("names/{name}"));
        if let Err(del_err) = store.delete(&name_key).await {
            warn!(
                "[inbound] create {name}: local setup failed and rollback \
                 of names/<name> also failed: {del_err}"
            );
        }
        return Err(IpcError::internal(format!("create failed: {e}")));
    }

    rescan.notify_one();
    info!("[inbound] created volume {name} ({vol_ulid_str})");
    Ok(CreateReply { vol_ulid })
}

async fn update_volume_op(
    name: &str,
    flags: &[String],
    data_dir: &Path,
) -> Result<UpdateReply, IpcError> {
    if name.is_empty() {
        return Err(IpcError::bad_request("usage: update <volume> [flags...]"));
    }
    let link = data_dir.join("by_name").join(name);
    if !link.exists() {
        return Err(IpcError::not_found(format!("volume not found: {name}")));
    }
    let vol_dir = std::fs::canonicalize(&link)
        .map_err(|e| IpcError::internal(format!("resolving volume dir: {e}")))?;

    let patch = parse_transport_flags(&flags.join(" ")).map_err(IpcError::bad_request)?;

    let mut cfg = elide_core::config::VolumeConfig::read(&vol_dir)
        .map_err(|e| IpcError::internal(format!("reading volume.toml: {e}")))?;

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

    cfg.write(&vol_dir)
        .map_err(|e| IpcError::internal(format!("writing volume.toml: {e}")))?;

    // Restart the volume process so it picks up the new config. ECONNREFUSED /
    // ENOENT mean the volume isn't running — fine, the new config takes effect
    // on next start.
    use elide_coordinator::control::ShutdownOutcome;
    match elide_coordinator::control::shutdown(&vol_dir).await {
        ShutdownOutcome::Acknowledged => {
            info!("[inbound] updated volume {name}; restart triggered");
            Ok(UpdateReply { restarted: true })
        }
        ShutdownOutcome::Failed(msg) => Err(IpcError::internal(format!("shutdown failed: {msg}"))),
        ShutdownOutcome::NotRunning => {
            info!("[inbound] updated volume {name}; not running");
            Ok(UpdateReply { restarted: false })
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
async fn pull_readonly_op(
    vol_ulid: ulid::Ulid,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<PullReadonlyReply, IpcError> {
    use object_store::path::Path as StorePath;

    let volume_id = vol_ulid.to_string();
    let vol_dir = data_dir.join("by_id").join(&volume_id);
    if vol_dir.exists() {
        return Err(IpcError::conflict(format!(
            "volume already present locally: {volume_id}"
        )));
    }

    let pull_started = std::time::Instant::now();

    // Step 1: fetch manifest.toml.
    let step1_started = std::time::Instant::now();
    let manifest_key = StorePath::from(format!("by_id/{volume_id}/manifest.toml"));
    let manifest_bytes = match store.get(&manifest_key).await {
        Ok(d) => d
            .bytes()
            .await
            .map_err(|e| IpcError::store(format!("reading manifest: {e}")))?,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(IpcError::not_found(format!(
                "manifest not found in store for volume {volume_id}"
            )));
        }
        Err(e) => return Err(IpcError::store(format!("downloading manifest: {e}"))),
    };
    let manifest: toml::Table = std::str::from_utf8(&manifest_bytes)
        .map_err(|e| IpcError::internal(format!("manifest is not valid utf-8: {e}")))
        .and_then(|s| {
            toml::from_str(s).map_err(|e| IpcError::internal(format!("parsing manifest.toml: {e}")))
        })?;
    let size = manifest
        .get("size")
        .and_then(|v| v.as_integer())
        .ok_or_else(|| IpcError::internal("manifest.toml missing 'size'"))?;
    let manifest_elapsed = step1_started.elapsed();

    // Step 2: fetch volume.pub.
    let step2_started = std::time::Instant::now();
    let pub_key_bytes = store
        .get(&StorePath::from(format!("by_id/{volume_id}/volume.pub")))
        .await
        .map_err(|e| IpcError::store(format!("downloading volume.pub: {e}")))?
        .bytes()
        .await
        .map_err(|e| IpcError::store(format!("reading volume.pub: {e}")))?;
    let pub_elapsed = step2_started.elapsed();

    // Step 3: fetch volume.provenance. NotFound is hard error.
    let step3_started = std::time::Instant::now();
    let provenance_bytes = match store
        .get(&StorePath::from(format!(
            "by_id/{volume_id}/volume.provenance"
        )))
        .await
    {
        Ok(d) => d
            .bytes()
            .await
            .map_err(|e| IpcError::store(format!("reading volume.provenance: {e}")))?,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(IpcError::not_found(format!(
                "volume.provenance not found in store for volume {volume_id}"
            )));
        }
        Err(e) => {
            return Err(IpcError::store(format!(
                "downloading volume.provenance: {e}"
            )));
        }
    };
    let provenance_elapsed = step3_started.elapsed();

    // Step 4: write the ancestor entry. No name carried over from manifest:
    // a missing volume.name (and missing by_name/ symlink) is the on-disk
    // marker that distinguishes a pulled ancestor from a user-managed volume.
    let _ = manifest;
    let step4_started = std::time::Instant::now();
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
        return Err(IpcError::internal(format!("writing pulled ancestor: {e}")));
    }
    let write_elapsed = step4_started.elapsed();

    // Step 5: parse and verify provenance to find the parent.
    let step5_started = std::time::Instant::now();
    let parent = match elide_core::signing::read_lineage_verifying_signature(
        &vol_dir,
        elide_core::signing::VOLUME_PUB_FILE,
        elide_core::signing::VOLUME_PROVENANCE_FILE,
    ) {
        Ok(lineage) => lineage
            .parent
            .map(|p| {
                ulid::Ulid::from_string(&p.volume_ulid).map_err(|e| {
                    IpcError::internal(format!("malformed parent ULID {:?}: {e}", p.volume_ulid))
                })
            })
            .transpose()?,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&vol_dir);
            return Err(IpcError::internal(format!(
                "verifying provenance for {volume_id}: {e}"
            )));
        }
    };
    let verify_elapsed = step5_started.elapsed();

    info!(
        "[inbound] pulled ancestor {volume_id} in {:.2?} (manifest {:.2?}, pub {:.2?}, provenance {:.2?}, write {:.2?}, verify {:.2?})",
        pull_started.elapsed(),
        manifest_elapsed,
        pub_elapsed,
        provenance_elapsed,
        write_elapsed,
        verify_elapsed,
    );
    Ok(PullReadonlyReply { parent })
}

/// Synthesize a "now" snapshot for a readonly source volume.
///
/// Mirrors the CLI's `create_readonly_snapshot_now`: prefetches indexes,
/// uploads the snapshot marker, verifies pinned segments are still in S3,
/// and writes a signed manifest under an ephemeral key in the ancestor's
/// directory. Returns `<snap_ulid> <ephemeral_pubkey_hex>`.
/// Implementation of the `resolve-handoff-key` IPC verb. See the
/// dispatch comment for the wire format.
async fn resolve_handoff_key_op(
    vol_ulid: ulid::Ulid,
    snap_ulid: ulid::Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<ResolveHandoffKeyReply, IpcError> {
    use elide_coordinator::recovery::{HandoffVerifier, resolve_handoff_verifier};

    match resolve_handoff_verifier(store, vol_ulid, snap_ulid).await {
        Ok(HandoffVerifier::Normal) => Ok(ResolveHandoffKeyReply::Normal),
        Ok(HandoffVerifier::Synthesised {
            manifest_pubkey, ..
        }) => {
            let hex: String = manifest_pubkey
                .as_ref()
                .to_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            Ok(ResolveHandoffKeyReply::Recovery {
                manifest_pubkey_hex: hex,
            })
        }
        Err(e) => Err(IpcError::internal(format!("{e}"))),
    }
}

/// Wait for the per-fork prefetch result and echo it.
///
/// The IPC has no internal timeout — the volume-side caller bounds it. If
/// the per-fork task disappears (channel closed without a final value, e.g.
/// the volume directory was removed) we report that as an error rather than
/// silently returning ok, so the caller can act on it.
async fn await_prefetch_op(
    vol_ulid: ulid::Ulid,
    tracker: &PrefetchTracker,
) -> Result<(), IpcError> {
    let mut rx = match subscribe_prefetch(tracker, &vol_ulid) {
        Some(rx) => rx,
        // Untracked: either already prefetched on a previous coordinator run,
        // or this coordinator hasn't discovered the fork yet. Both cases are
        // safe to treat as ready — Volume::open's own retry helper still
        // absorbs the rare "fork not yet on disk" sub-race.
        None => return Ok(()),
    };

    loop {
        // `borrow()` returns the most recently published value; if a value
        // was already sent before we subscribed it is visible immediately.
        if let Some(result) = rx.borrow().clone() {
            return match result {
                Ok(()) => Ok(()),
                Err(msg) => Err(IpcError::internal(format!("prefetch failed: {msg}"))),
            };
        }
        if rx.changed().await.is_err() {
            // Sender was dropped before publishing — the per-fork task exited
            // abnormally (volume removed mid-prefetch, panic, etc.).
            return Err(IpcError::internal(
                "prefetch task exited without publishing a result",
            ));
        }
    }
}

async fn force_snapshot_now_op(
    vol_ulid: ulid::Ulid,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<ForceSnapshotNowReply, IpcError> {
    use futures::TryStreamExt;
    use object_store::PutPayload;
    use object_store::path::Path as StorePath;

    let volume_id = vol_ulid.to_string();
    let ancestor_dir = data_dir.join("by_id").join(&volume_id);
    if !ancestor_dir.exists() {
        return Err(IpcError::not_found(format!(
            "volume not found locally: {volume_id}"
        )));
    }

    // Step 1: prefetch indexes from S3.
    // No peer-fetch tier on the IPC pull-readonly path: this code runs
    // for ad-hoc readonly hydration (e.g. CLI inspect of a peer's
    // snapshot), not for the per-volume claim flow that owns the
    // peer-fetch context. S3 is the canonical fallback for everything
    // else; passing `None` here matches the v1 plan's "existing call
    // sites pass None initially" guidance.
    elide_coordinator::prefetch::prefetch_indexes(&ancestor_dir, store, None)
        .await
        .map_err(|e| IpcError::store(format!("prefetching ancestor indexes: {e:#}")))?;

    // Step 2: enumerate prefetched .idx files. Pin ULID = max(segments).
    let index_dir = ancestor_dir.join("index");
    let mut segments: Vec<ulid::Ulid> = Vec::new();
    let mut max_seg: Option<ulid::Ulid> = None;
    let entries = std::fs::read_dir(&index_dir)
        .map_err(|e| IpcError::internal(format!("reading index dir: {e}")))?;
    for entry in entries {
        let entry = entry.map_err(|e| IpcError::internal(format!("reading index entry: {e}")))?;
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
    // `Ulid::default()` is the nil ULID, not a fresh one — explicitly mint.
    #[allow(clippy::unwrap_or_default)]
    let snap = max_seg.unwrap_or_else(ulid::Ulid::new);
    let snap_str = snap.to_string();

    // Step 3 + 4: upload marker, verify pinned segments still present.
    let marker_key = elide_coordinator::upload::snapshot_key(&volume_id, &snap_str)
        .map_err(|e| IpcError::internal(format!("snapshot_key: {e}")))?;
    let seg_prefix = StorePath::from(format!("by_id/{volume_id}/segments/"));
    let pinned: std::collections::HashSet<ulid::Ulid> = segments.iter().copied().collect();

    store
        .put(&marker_key, PutPayload::from_static(b""))
        .await
        .map_err(|e| IpcError::store(format!("uploading snapshot marker: {e}")))?;

    let objects = store
        .list(Some(&seg_prefix))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| IpcError::store(format!("listing segments for verification: {e}")))?;
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
        return Err(IpcError::conflict(format!(
            "force-snapshot aborted: {} of {} pinned segment(s) no longer present in S3 \
             (owner GC raced us); missing: [{}]{}",
            missing.len(),
            pinned.len(),
            preview.join(", "),
            suffix,
        )));
    }

    // Step 5: load-or-create the per-source attestation key.
    let (signer, vk) = elide_core::signing::load_or_create_keypair(
        &ancestor_dir,
        elide_core::signing::FORCE_SNAPSHOT_KEY_FILE,
        elide_core::signing::FORCE_SNAPSHOT_PUB_FILE,
    )
    .map_err(|e| IpcError::internal(format!("attestation keypair: {e}")))?;

    // Step 6: signed manifest + local marker.
    elide_core::signing::write_snapshot_manifest(&ancestor_dir, &*signer, &snap, &segments, None)
        .map_err(|e| IpcError::internal(format!("writing snapshot manifest: {e}")))?;
    let snap_dir = ancestor_dir.join("snapshots");
    std::fs::create_dir_all(&snap_dir)
        .map_err(|e| IpcError::internal(format!("creating snapshots dir: {e}")))?;
    std::fs::write(snap_dir.join(&snap_str), b"")
        .map_err(|e| IpcError::internal(format!("writing local marker: {e}")))?;

    info!(
        "[inbound] attested now-snapshot {snap_str} for {volume_id} ({} segments)",
        segments.len()
    );
    let pubkey_hex: String = vk.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
    Ok(ForceSnapshotNowReply {
        snap_ulid: snap,
        attestation_pubkey_hex: pubkey_hex,
    })
}

/// Fork an existing source volume into a new writable volume.
///
/// Mirrors the CLI's `fork_volume_at*` + by_name symlink + volume.toml write.
/// `source_ulid` resolves to any volume in `by_id/<ulid>/` — writable,
/// imported readonly base, or pulled ancestor. `snap` is optional: if
/// omitted, falls back to `volume::fork_volume` (latest local snapshot).
/// `parent-key` is the hex ephemeral pubkey from `force-snapshot-now`,
/// recorded in the new fork's provenance for force-snapshot pins.
#[allow(clippy::too_many_arguments)]
/// Classify a pre-existing `by_name/<name>` symlink seen during a
/// `for_claim` fork-create.
///
/// Returns:
///
///   - `Some(ulid)` when the symlink target's `by_id/<ulid>/`
///     directory holds a verified `volume.provenance` whose parent
///     matches `expected_parent` and `expected_snap` — i.e. it's
///     exactly what a fresh `fork_create` would mint, the residue
///     of an aborted prior attempt. The caller reuses it (resume
///     mode): no new fork minted, no orphan accumulation.
///
///   - `None` otherwise: symlink absent, target dir missing,
///     provenance unverifiable, or lineage doesn't match. The
///     caller's `for_claim` branch removes the stale link and
///     mints a fresh fork.
///
/// Read-only — never mutates filesystem.
fn classify_resumable_orphan(
    symlink: &std::path::Path,
    by_id_dir: &std::path::Path,
    expected_parent: ulid::Ulid,
    expected_snap: Option<ulid::Ulid>,
) -> Option<ulid::Ulid> {
    let target = std::fs::read_link(symlink).ok()?;
    let prev_ulid_str = target.file_name().and_then(|n| n.to_str())?;
    let prev_ulid = ulid::Ulid::from_string(prev_ulid_str).ok()?;
    let prev_dir = by_id_dir.join(prev_ulid_str);
    if !prev_dir.is_dir() {
        return None;
    }
    let lineage = elide_core::signing::read_lineage_verifying_signature(
        &prev_dir,
        elide_core::signing::VOLUME_PUB_FILE,
        elide_core::signing::VOLUME_PROVENANCE_FILE,
    )
    .ok()?;
    let parent = lineage.parent?;
    let expected_snap = expected_snap?;
    if parent.volume_ulid == expected_parent.to_string()
        && parent.snapshot_ulid == expected_snap.to_string()
    {
        Some(prev_ulid)
    } else {
        None
    }
}

// IPC handler: arguments mirror the dispatcher's per-call context.
// Packing them into a struct would create a one-shot type with no
// other callers.
#[allow(clippy::too_many_arguments)]
async fn fork_create_op(
    new_name: &str,
    source_vol_ulid: ulid::Ulid,
    snap: Option<ulid::Ulid>,
    parent_key_hex: Option<&str>,
    for_claim: bool,
    flags: &[String],
    // Orchestrator-supplied source name. Used as a fallback when the
    // source's on-disk `volume.toml` has no `name` field — typically
    // because the source was pulled from S3 (`pull.rs` writes
    // `name: None` for pulled ancestors). Without this hint, forks
    // from a pulled source emit a `Created` journal event instead of
    // the more useful `ForkedFrom { source_name, ... }`.
    source_name_hint: Option<&str>,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
    rescan: &Notify,
    prefetch_tracker: &PrefetchTracker,
) -> Result<ForkCreateReply, IpcError> {
    let coord_id = identity.coordinator_id_str();
    validate_volume_name(new_name).map_err(IpcError::bad_request)?;
    let source_ulid_str = source_vol_ulid.to_string();

    let parent_key = match parent_key_hex {
        Some(hex) => {
            let arr = decode_hex32(hex)
                .map_err(|e| IpcError::bad_request(format!("bad parent-key: {e}")))?;
            Some(
                elide_core::signing::VerifyingKey::from_bytes(&arr).map_err(|e| {
                    IpcError::bad_request(format!("parent-key not a valid Ed25519 pubkey: {e}"))
                })?,
            )
        }
        None => None,
    };

    let patch = parse_transport_flags(&flags.join(" ")).map_err(IpcError::bad_request)?;
    if patch.no_nbd || patch.no_ublk {
        return Err(IpcError::bad_request(
            "no-nbd / no-ublk are not valid on fork-create",
        ));
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
    let by_id_dir = data_dir.join("by_id");
    let source_dir = elide_core::volume::resolve_ancestor_dir(&by_id_dir, &source_ulid_str);
    if !source_dir.exists() {
        return Err(IpcError::not_found(format!(
            "source volume {source_ulid_str} not found locally"
        )));
    }

    // For_claim mode: a pre-existing `by_name/<name>` symlink may be
    // the residue of an aborted prior attempt. If its signed
    // provenance matches the parent + snap we'd be forking from,
    // resume against it — no new fork minted, no orphan accumulation.
    // Otherwise, remove the stale link and fall through to the
    // fresh-fork path. (For `!for_claim`, an existing symlink is
    // always a hard conflict — that's `volume create --from`'s
    // contract.)
    let resume_existing: Option<ulid::Ulid> = if for_claim {
        classify_resumable_orphan(&symlink_path, &by_id_dir, source_vol_ulid, snap)
    } else {
        None
    };
    if let Some(prev) = resume_existing {
        info!("[inbound] fork-create {new_name}: resuming aborted prior attempt at {prev}");
    } else if symlink_path.exists() {
        if for_claim {
            std::fs::remove_file(&symlink_path).map_err(|e| {
                IpcError::internal(format!("removing stale by_name/{new_name}: {e}"))
            })?;
            info!("[inbound] removed stale by_name/{new_name}");
        } else {
            return Err(IpcError::conflict(format!(
                "volume already exists: {new_name}"
            )));
        }
    }

    // `Ulid::default()` is the nil ULID; we need a freshly minted one
    // when resume isn't in play, so suppress clippy's unwrap_or_default
    // suggestion explicitly.
    #[allow(clippy::unwrap_or_default)]
    let new_vol_ulid_value = resume_existing.unwrap_or_else(ulid::Ulid::new);
    let new_vol_ulid = new_vol_ulid_value.to_string();
    let new_fork_dir = by_id_dir.join(&new_vol_ulid);

    // Local rollback helpers. `rollback_claim` is a no-op until
    // `mark_initial` succeeds (or in the `for-claim` path, where the
    // bucket record was created by an earlier release and is owned by
    // the CLI's subsequent `claim` IPC).
    let cleanup = |fork_dir: &Path, link: &Path| {
        let _ = std::fs::remove_file(link);
        let _ = std::fs::remove_dir_all(fork_dir);
    };
    let mut name_claimed_in_bucket = false;
    let rollback_claim = async |claimed: bool| {
        if !claimed {
            return;
        }
        let key = object_store::path::Path::from(format!("names/{new_name}"));
        if let Err(e) = store.delete(&key).await {
            warn!(
                "[inbound] fork-create {new_name}: local fork failed and \
                 rollback of names/<name> also failed: {e}"
            );
        }
    };

    // Phases 1 + 2 + 4 are skipped when resuming an aborted prior
    // attempt: the orphan fork's signed provenance was already
    // verified by `classify_resumable_orphan`, the volume.pub +
    // volume.provenance uploads were completed before the orphan
    // attempt's symlink was created, and `volume.toml` + the
    // by_name symlink are exactly what they would be on a fresh
    // mint. The remaining work — bucket-side `mark_claimed`,
    // `volume.stopped` marker, journal event — runs unconditionally
    // for the `for_claim` path further down.
    let (resolved_snap, src_name) = if resume_existing.is_none() {
        // Phase 1: materialise the fork locally. This generates the new
        // fork's keypair on disk so we can upload `volume.pub` before
        // touching `names/<name>`.
        let fork_result: std::io::Result<()> = match (snap, parent_key) {
            (Some(snap), Some(vk)) => elide_core::volume::fork_volume_at_with_manifest_key(
                &new_fork_dir,
                &source_dir,
                snap,
                vk,
            ),
            (Some(snap), None) => {
                elide_core::volume::fork_volume_at(&new_fork_dir, &source_dir, snap)
            }
            (None, _) => elide_core::volume::fork_volume(&new_fork_dir, &source_dir),
        };
        if let Err(e) = fork_result {
            cleanup(&new_fork_dir, &symlink_path);
            return Err(IpcError::internal(format!("fork failed: {e}")));
        }

        // Phase 2: publish volume.pub *and* volume.provenance to S3
        // *before* claiming the name. Both files are immutable from
        // fork creation onward and self-signed by the new fork's
        // keypair. A SIGINT here at worst leaves orphan
        // `by_id/<new_vol_ulid>/{volume.pub, volume.provenance}` (no
        // names/<name> references them, so they're harmless and
        // reclaimable by future GC). Without this ordering, a crash
        // between mark_initial and the daemon's first metadata-drain
        // leaves `names/<name>` pointing at a vol_ulid whose
        // immutable trust artefacts are missing — which breaks both
        // the normal claim path and the peer-fetch auth pipeline
        // (lineage walk 404s on volume.provenance).
        if let Err(e) = elide_coordinator::upload::upload_volume_pub_initial(
            &new_fork_dir,
            &new_vol_ulid,
            store,
        )
        .await
        {
            cleanup(&new_fork_dir, &symlink_path);
            return Err(IpcError::store(format!("uploading volume.pub: {e:#}")));
        }
        if let Err(e) = elide_coordinator::upload::upload_volume_provenance_initial(
            &new_fork_dir,
            &new_vol_ulid,
            store,
        )
        .await
        {
            cleanup(&new_fork_dir, &symlink_path);
            return Err(IpcError::store(format!(
                "uploading volume.provenance: {e:#}"
            )));
        }

        // Read source volume config: `src_cfg.name` feeds the
        // `ForkedFrom` journal entry below, and `src_cfg.size` is
        // inherited into the new fork's `volume.toml` further down.
        // Done before the bucket claim so a malformed source fails
        // cleanly without leaving a half-claimed name.
        let src_cfg = match elide_core::config::VolumeConfig::read(&source_dir) {
            Ok(c) => c,
            Err(e) => {
                cleanup(&new_fork_dir, &symlink_path);
                return Err(IpcError::internal(format!(
                    "reading source volume config: {e}"
                )));
            }
        };
        let size = match src_cfg.size {
            Some(s) => s,
            None => {
                cleanup(&new_fork_dir, &symlink_path);
                return Err(IpcError::conflict(
                    "source volume has no size (import may not have completed)",
                ));
            }
        };

        // Snap actually used for the fork. `snap.is_some()` matches
        // the explicit-pin call sites; otherwise `fork_volume` above
        // resolved `latest_snapshot(&source_dir)` internally and we
        // recompute it here so the journal records the same value.
        // Resolution failure here only suppresses the `ForkedFrom`
        // event; the lifecycle proceeds with `Created` as a fallback.
        let resolved_snap = snap.or_else(|| {
            elide_core::volume::latest_snapshot(&source_dir)
                .ok()
                .flatten()
        });

        // Phase 4 prep: stash the local-config write alongside the
        // symlink creation below. Doing it here keeps the cleanup
        // semantics symmetric with the upload failures above.
        if let Err(e) = (elide_core::config::VolumeConfig {
            name: Some(new_name.to_owned()),
            size: Some(size),
            nbd: nbd_cfg,
            ublk: ublk_cfg,
        }
        .write(&new_fork_dir))
        {
            cleanup(&new_fork_dir, &symlink_path);
            return Err(IpcError::internal(format!("writing volume config: {e}")));
        }

        let src_name = src_cfg.name.or_else(|| source_name_hint.map(str::to_owned));
        (resolved_snap, src_name)
    } else {
        // Resume mode: orphan fork is fully materialised on disk,
        // volume.pub + volume.provenance are already on S3, and
        // volume.toml + by_name symlink already exist. We just need
        // the `resolved_snap` value so the journal event below can
        // emit a meaningful record. We don't read `volume.toml`
        // because the fresh-fork path read source's, not the new
        // fork's; mirroring "use snap if pinned, else latest" is
        // enough for the event.
        let resolved_snap = snap.or_else(|| {
            elide_core::volume::latest_snapshot(&source_dir)
                .ok()
                .flatten()
        });
        let src_name = elide_core::config::VolumeConfig::read(&source_dir)
            .ok()
            .and_then(|c| c.name)
            .or_else(|| source_name_hint.map(str::to_owned));
        (resolved_snap, src_name)
    };

    // Phase 3: for brand-new names, claim in S3. For the
    // claim-from-released path (`for-claim` flag), the bucket record
    // already exists as `Released` and the CLI's subsequent `claim`
    // IPC handles the rebind via `mark_claimed`; the volume.pub we
    // just uploaded is what makes that rebind safe.
    if !for_claim {
        use elide_coordinator::lifecycle::{LifecycleError, MarkInitialOutcome, mark_initial};
        match mark_initial(
            store,
            new_name,
            coord_id,
            identity.hostname(),
            new_vol_ulid_value,
        )
        .await
        {
            Ok(MarkInitialOutcome::Claimed) => {
                name_claimed_in_bucket = true;
                // `volume create --from` mints a fork, so the
                // opening journal entry on the new name is
                // `ForkedFrom` (not `Created`). Falls back to
                // `Created` only when fork context cannot be
                // reconstructed — typically a ULID-only ancestor
                // with no `name` in its `volume.toml`. Both the
                // source name and snap have to be present; a
                // partial `ForkedFrom` would publish a less useful
                // record than just stating "this name appeared".
                let kind = match (resolved_snap, src_name.clone()) {
                    (Some(source_snap_ulid), Some(source_name)) => {
                        elide_core::volume_event::EventKind::ForkedFrom {
                            source_name,
                            source_vol_ulid,
                            source_snap_ulid,
                        }
                    }
                    _ => {
                        warn!(
                            "[inbound] fork-create {new_name}: source \
                             {source_ulid_str} missing name or snap; emitting \
                             Created in lieu of ForkedFrom"
                        );
                        elide_core::volume_event::EventKind::Created
                    }
                };
                elide_coordinator::volume_event_store::emit_best_effort(
                    store,
                    identity.as_ref(),
                    new_name,
                    kind,
                    new_vol_ulid_value,
                )
                .await;
            }
            Ok(MarkInitialOutcome::AlreadyExists {
                existing_vol_ulid,
                existing_state,
                existing_owner,
            }) => {
                cleanup(&new_fork_dir, &symlink_path);
                let owner = existing_owner.as_deref().unwrap_or("<unowned>");
                return Err(IpcError::conflict(format!(
                    "name '{new_name}' already exists in bucket \
                     (vol_ulid={existing_vol_ulid}, state={existing_state:?}, \
                     owner={owner})"
                )));
            }
            Err(LifecycleError::Store(e)) => {
                cleanup(&new_fork_dir, &symlink_path);
                return Err(IpcError::store(format!("claiming name in bucket: {e}")));
            }
            Err(LifecycleError::OwnershipConflict { held_by }) => {
                cleanup(&new_fork_dir, &symlink_path);
                return Err(IpcError::conflict(format!(
                    "name held by another coordinator: {held_by}"
                )));
            }
            Err(LifecycleError::InvalidTransition { from, .. }) => {
                cleanup(&new_fork_dir, &symlink_path);
                return Err(IpcError::conflict(format!(
                    "names/<name> is in unexpected state {from:?}"
                )));
            }
        }
    }

    // Phase 4: by_name/<name> symlink (volume.toml was written
    // earlier as part of the fresh-fork phases, before mark_initial,
    // so a Phase-3 failure didn't leave a half-written config).
    // Resume mode skips this — the orphan attempt's symlink already
    // points at the same ULID.
    if resume_existing.is_none() {
        if let Err(e) = std::fs::create_dir_all(&by_name_dir) {
            cleanup(&new_fork_dir, &symlink_path);
            rollback_claim(name_claimed_in_bucket).await;
            return Err(IpcError::internal(format!("creating by_name dir: {e}")));
        }
        if let Err(e) =
            std::os::unix::fs::symlink(format!("../by_id/{new_vol_ulid}"), &symlink_path)
        {
            cleanup(&new_fork_dir, &symlink_path);
            rollback_claim(name_claimed_in_bucket).await;
            return Err(IpcError::internal(format!("creating by_name symlink: {e}")));
        }
    }

    // For-claim post-work: write `volume.stopped` and atomically
    // rebind `names/<name>` to the new (or resumed) fork via
    // `mark_claimed`. Subsumes the previous `RebindName` IPC: the
    // claim flow is now one round-trip from the CLI, and the
    // CLI no longer touches root-owned filesystem state.
    if for_claim {
        // Idempotent on resume — file may already exist from an
        // earlier successful rebind that the CLI never observed.
        if let Err(e) = std::fs::write(new_fork_dir.join(STOPPED_FILE), "") {
            return Err(IpcError::internal(format!(
                "writing volume.stopped on new fork: {e}"
            )));
        }

        use elide_coordinator::lifecycle::{LifecycleError, MarkClaimedOutcome, mark_claimed};
        use elide_core::name_record::NameState;
        match mark_claimed(
            store,
            new_name,
            coord_id,
            identity.hostname(),
            new_vol_ulid_value,
            NameState::Stopped,
        )
        .await
        {
            Ok(MarkClaimedOutcome::Claimed) => {
                info!("[inbound] rebound name {new_name} to new fork {new_vol_ulid} (stopped)");
                elide_coordinator::volume_event_store::emit_best_effort(
                    store,
                    identity.as_ref(),
                    new_name,
                    elide_core::volume_event::EventKind::Claimed,
                    new_vol_ulid_value,
                )
                .await;
            }
            Ok(MarkClaimedOutcome::Absent) => {
                return Err(IpcError::not_found(format!(
                    "names/{new_name} does not exist; nothing to claim"
                )));
            }
            Ok(MarkClaimedOutcome::NotReleased { observed }) => {
                return Err(IpcError::conflict(format!(
                    "names/{new_name} is in state {observed:?}, not Released; cannot claim"
                )));
            }
            Err(LifecycleError::Store(e)) => {
                return Err(IpcError::store(format!("claim failed: {e}")));
            }
            Err(LifecycleError::OwnershipConflict { held_by }) => {
                return Err(IpcError::precondition_failed(format!(
                    "name '{new_name}' raced with another claim ({held_by} won); \
                     your local fork at {new_vol_ulid} can be re-purposed manually"
                )));
            }
            Err(LifecycleError::InvalidTransition { from, .. }) => {
                return Err(IpcError::conflict(format!(
                    "names/{new_name} is in state {from:?}; cannot claim"
                )));
            }
        }
    }

    // Pre-register the prefetch tracker entry before notifying the daemon's
    // discovery loop. This closes the race where the CLI's
    // `await-prefetch <new_vol_ulid>` (called immediately after this IPC
    // returns) could land before the daemon has discovered the new fork
    // and registered the entry — in which case `await-prefetch` would
    // hit the "untracked → ok" path and falsely report prefetch complete.
    // `register_prefetch_or_get` is idempotent: when discovery later runs,
    // it gets back the same `Arc<Sender>` and passes it to
    // `run_volume_tasks`. Drop the local Arc immediately; the tracker
    // holds the entry until the per-fork task's Drop guard removes it.
    let _ = register_prefetch_or_get(prefetch_tracker, new_vol_ulid_value);

    rescan.notify_one();
    info!("[inbound] forked volume {new_name} ({new_vol_ulid}) from {source_ulid_str}");
    Ok(ForkCreateReply {
        new_vol_ulid: new_vol_ulid_value,
    })
}

// ── Volume stop / start ───────────────────────────────────────────────────────

/// True if `<data_dir>/by_name/<name>` exists and the resolved fork
/// has no `volume.stopped` marker — i.e. this host is actively
/// serving (or supposed to be serving) `<name>`.
///
/// Used by recovery verbs (`release --force`, `claim`) to refuse when
/// the operator has typo'd a verb at their own running volume: those
/// verbs are designed for unreachable peers and would otherwise leave
/// on-disk state diverging from the bucket record.
fn local_daemon_running(data_dir: &Path, volume_name: &str) -> bool {
    let link = data_dir.join("by_name").join(volume_name);
    match std::fs::canonicalize(&link) {
        Ok(vol_dir) => !vol_dir.join(STOPPED_FILE).exists(),
        Err(_) => false,
    }
}

async fn stop_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    hostname: Option<&str>,
) -> Result<(), IpcError> {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return Err(IpcError::not_found(format!(
            "volume not found: {volume_name}"
        )));
    }
    let vol_dir = std::fs::canonicalize(&link)
        .map_err(|e| IpcError::internal(format!("resolving volume dir: {e}")))?;

    if vol_dir.join(STOPPED_FILE).exists() {
        // Already stopped — idempotent success.
        return Ok(());
    }

    let readonly = vol_dir.join("volume.readonly").exists();

    // Refuse to stop while an NBD client is connected. ublk volumes have
    // no NBD client and `is_connected` returns Disconnected.
    if elide_coordinator::control::is_connected(&vol_dir).await
        == elide_coordinator::control::ConnectedStatus::Connected
    {
        return Err(IpcError::conflict(
            "nbd client is connected; disconnect it first",
        ));
    }

    // `stop` is a local-lifecycle verb: its job is to halt the daemon
    // on this host. The bucket update is best-effort and only applies
    // to writable volumes — readonly volumes have a `Readonly` bucket
    // record that is its own terminal state, so there is no Live → Stopped
    // transition to make.
    //
    // For writable volumes, the bucket update only succeeds for the
    // canonical case (record owned by us, Live → Stopped); every other
    // case (no record, already stopped, foreign-owned, Released,
    // transient store error) becomes a warning and we proceed with
    // the local halt. The daemon may legitimately still be running
    // while the bucket says Released — e.g. after a partial release
    // that flipped the bucket but failed to halt the process — and
    // `stop` must be able to recover from that. Halting our local
    // daemon never affects other hosts.
    if !readonly {
        use elide_coordinator::lifecycle::{LifecycleError, mark_stopped};
        match mark_stopped(store, volume_name, coord_id, hostname).await {
            Ok(_) => {}
            Err(LifecycleError::OwnershipConflict { held_by }) => {
                warn!(
                    "[inbound] stop {volume_name}: names/<name> is owned by coordinator \
                     {held_by}; halting locally, bucket record left untouched"
                );
            }
            Err(LifecycleError::InvalidTransition { from, .. }) => {
                warn!(
                    "[inbound] stop {volume_name}: names/<name> is in state {from:?}; \
                     halting locally, bucket record left untouched"
                );
            }
            Err(LifecycleError::Store(e)) => {
                warn!("[inbound] stop {volume_name}: failed to update names/<name>: {e}");
            }
        }
    }

    // Write the marker before sending shutdown so the supervisor won't restart.
    std::fs::write(vol_dir.join(STOPPED_FILE), "")
        .map_err(|e| IpcError::internal(format!("writing volume.stopped: {e}")))?;

    use elide_coordinator::control::ShutdownOutcome;
    match elide_coordinator::control::shutdown(&vol_dir).await {
        ShutdownOutcome::Acknowledged => {
            info!("[inbound] stopped volume {volume_name}");
            Ok(())
        }
        ShutdownOutcome::Failed(msg) => {
            // Roll back the marker so the supervisor doesn't strand a still-
            // running volume. (Note: the S3 state has already flipped to
            // Stopped; that's a soft inconsistency the operator can resolve
            // by issuing `volume start` once the underlying issue is fixed.)
            let _ = std::fs::remove_file(vol_dir.join(STOPPED_FILE));
            Err(IpcError::internal(format!("shutdown failed: {msg}")))
        }
        ShutdownOutcome::NotRunning => {
            // Volume process wasn't running — marker is correct as-is.
            info!("[inbound] stopped volume {volume_name} (process was not running)");
            Ok(())
        }
    }
}

/// RAII guard that restores the original local state on drop.
///
/// Used by `release_volume_op` when it transparently restarts a
/// stopped volume to perform the drain: the marker is cleaned up
/// regardless of which exit path the function takes (success, error,
/// panic), and on failure paths the `volume.stopped` marker is
/// re-written so the volume returns to its pre-release state instead
/// of being left running.
struct DrainingMarkerGuard {
    vol_dir: std::path::PathBuf,
    /// Set to `true` once the release has reached a point where the
    /// caller has explicitly written `volume.stopped` (i.e. the
    /// release path's own halt step). Defuses the rollback so the
    /// guard only removes the draining marker, leaving the volume
    /// stopped as the release flow intends.
    success: bool,
}

impl DrainingMarkerGuard {
    fn new(vol_dir: std::path::PathBuf) -> Self {
        Self {
            vol_dir,
            success: false,
        }
    }

    /// Mark the release as having reached its own halt step — at this
    /// point `volume.stopped` is back in place via the normal path
    /// and we should *not* re-write it on drop.
    fn defuse(&mut self) {
        self.success = true;
    }
}

impl Drop for DrainingMarkerGuard {
    fn drop(&mut self) {
        let draining = self.vol_dir.join("volume.draining");
        if let Err(e) = std::fs::remove_file(&draining)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                "[inbound] failed to remove draining marker {}: {e}",
                draining.display()
            );
        }
        if !self.success {
            // Release failed before the explicit halt step. Re-write
            // `volume.stopped` so the volume returns to its
            // pre-release state — without this the supervisor would
            // keep the (now transport-suppressed-then-restored)
            // process running indefinitely.
            let stopped = self.vol_dir.join(STOPPED_FILE);
            if let Err(e) = std::fs::write(&stopped, "")
                && e.kind() != std::io::ErrorKind::AlreadyExists
            {
                warn!(
                    "[inbound] failed to restore volume.stopped after \
                     aborted release at {}: {e}",
                    stopped.display()
                );
            }
            // The volume process is still running with the actor
            // initialised; ask it to halt cleanly so the supervisor
            // sees a clean exit and respects the marker we just
            // wrote. Best-effort: a failed shutdown leaves the
            // supervisor to notice the stopped marker on the next
            // poll and not respawn after the eventual exit.
            let vol_dir = self.vol_dir.clone();
            tokio::spawn(async move {
                let _ = elide_coordinator::control::shutdown(&vol_dir).await;
            });
        }
    }
}

/// Poll until `control.sock` actually accepts a connection. Returns
/// `true` as soon as a connect+accept succeeds, or `false` if
/// `timeout` elapses first. Used by `release_volume_op` when
/// transparently restarting a stopped volume so the snapshot path can
/// talk to it.
///
/// File existence alone is not enough: the previous volume process
/// can leave a stale socket file behind, and the new volume's
/// `UnixListener::bind` does `remove_file` + `bind` — there is a
/// window where the file exists but no listener is attached, so
/// `connect()` returns `ECONNREFUSED`. We probe the connection itself
/// and treat that as the readiness signal.
async fn wait_for_control_sock(vol_dir: &Path, timeout: std::time::Duration) -> bool {
    let socket = vol_dir.join("control.sock");
    let deadline = std::time::Instant::now() + timeout;
    let probe_step = std::time::Duration::from_millis(50);
    while std::time::Instant::now() < deadline {
        if socket.exists() {
            match tokio::net::UnixStream::connect(&socket).await {
                Ok(_) => return true,
                Err(_) => {
                    // Stale file (waiting for the new listener), or
                    // listener is up but transient refusal — retry.
                }
            }
        }
        tokio::time::sleep(probe_step).await;
    }
    false
}

/// `volume release --force`.
///
/// Override path for an unreachable previous owner: synthesise a
/// fresh handoff snapshot from S3-visible segments under the dead
/// fork's prefix, sign it with this coordinator's identity key, and
/// unconditionally rewrite `names/<name>` to `Released`.
///
/// Does **not** require a local symlink, does **not** drain any WAL
/// (the dead owner's WAL is unreachable), does **not** halt or touch
/// any local volume daemon. The data-loss boundary is "writes the
/// dead owner accepted but never promoted to S3" — same as the
/// crash-recovery contract elsewhere.
async fn force_release_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
) -> Result<ReleaseReply, IpcError> {
    use elide_coordinator::lifecycle::{self, ForceReleaseOutcome};
    use elide_coordinator::recovery;

    // Refuse when the "dead peer" is actually this host's running
    // daemon. force-release is for unreachable peers; against a local
    // running fork it would leave on-disk state diverging from the
    // bucket record. The operator wants `volume stop` first.
    if local_daemon_running(data_dir, volume_name) {
        return Err(IpcError::conflict(format!(
            "volume '{volume_name}' is running on this host; \
             stop it first with: elide volume stop {volume_name}"
        )));
    }

    // Read the current record to learn which dead fork to recover from.
    let dead_vol_ulid =
        match elide_coordinator::name_store::read_name_record(store, volume_name).await {
            Ok(Some((rec, _))) => {
                use elide_core::name_record::NameState;
                match rec.state {
                    NameState::Live | NameState::Stopped => rec.vol_ulid,
                    other => {
                        return Err(IpcError::conflict(format!(
                            "names/{volume_name} is in state {other:?}; \
                             force-release only overrides Live or Stopped records"
                        )));
                    }
                }
            }
            Ok(None) => {
                return Err(IpcError::not_found(format!(
                    "name '{volume_name}' has no S3 record"
                )));
            }
            Err(e) => {
                return Err(IpcError::store(format!("reading names/{volume_name}: {e}")));
            }
        };

    // Recovery pipeline: fetch dead fork's pubkey, list+verify
    // segments, mint+sign+publish synthesised handoff snapshot.
    //
    // If `volume.pub` is absent the dead fork crashed during the
    // create-time window before the coordinator published it. No
    // segment could have been signed-and-verified under a missing key,
    // so the dead fork is provably empty: publish an empty synthesised
    // handoff and flip to Released.
    let dead_pub = recovery::fetch_volume_pub_optional(store, dead_vol_ulid)
        .await
        .map_err(|e| {
            IpcError::store(format!(
                "fetching volume.pub for released fork {dead_vol_ulid}: {e:#}"
            ))
        })?;
    let segment_ulids: Vec<ulid::Ulid> = match dead_pub {
        Some(dead_pub) => {
            let recovered = recovery::list_and_verify_segments(store, dead_vol_ulid, &dead_pub)
                .await
                .map_err(|e| {
                    IpcError::store(format!(
                        "listing/verifying segments for released fork {dead_vol_ulid}: {e:#}"
                    ))
                })?;
            let ulids: Vec<ulid::Ulid> =
                recovered.segments.iter().map(|s| s.segment_ulid).collect();
            info!(
                "[inbound] force-release {volume_name}: recovered {} segments \
                 ({} dropped) from released fork {dead_vol_ulid}",
                ulids.len(),
                recovered.dropped,
            );
            ulids
        }
        None => {
            info!(
                "[inbound] force-release {volume_name}: released fork \
                 {dead_vol_ulid} has no volume.pub in bucket — treating as \
                 empty (create-time crash before pub upload)"
            );
            Vec::new()
        }
    };

    let published = recovery::mint_and_publish_synthesised_snapshot(
        store,
        dead_vol_ulid,
        &segment_ulids,
        identity.as_ref(),
        identity.coordinator_id_str(),
    )
    .await
    .map_err(|e| IpcError::store(format!("publishing synthesised snapshot: {e}")))?;

    // Unconditional flip of names/<name>.
    let outcome = lifecycle::mark_released_force(store, volume_name, published.snap_ulid).await;
    match outcome {
        Ok(ForceReleaseOutcome::Overwritten {
            dead_vol_ulid: d,
            displaced_coordinator_id,
        }) => {
            info!(
                "[inbound] force-released volume {volume_name} (released fork {d}) at \
                 synthesised handoff snapshot {}",
                published.snap_ulid,
            );

            // Best-effort journal entry recording the override.
            elide_coordinator::volume_event_store::emit_best_effort(
                store,
                identity.as_ref(),
                volume_name,
                elide_core::volume_event::EventKind::ForceReleased {
                    handoff_snapshot: published.snap_ulid,
                    displaced_coordinator_id: displaced_coordinator_id
                        .unwrap_or_else(|| "<unknown>".to_string()),
                },
                d,
            )
            .await;

            Ok(ReleaseReply {
                handoff_snapshot: published.snap_ulid,
            })
        }
        Ok(ForceReleaseOutcome::Absent) => {
            // Race: record disappeared between our read and our write.
            Err(IpcError::precondition_failed(format!(
                "names/{volume_name} vanished between read and force-write"
            )))
        }
        Ok(ForceReleaseOutcome::InvalidState { observed }) => {
            // Race: state changed under us. The synthesised snapshot
            // is still published (harmless); operator can retry.
            Err(IpcError::precondition_failed(format!(
                "names/{volume_name} changed underneath us; now in state {observed:?}"
            )))
        }
        Err(e) => Err(IpcError::store(format!(
            "force-release flip failed (synthesised snapshot {} already published): {e}",
            published.snap_ulid
        ))),
    }
}

/// Relinquish ownership of `<volume_name>` so any other coordinator can
/// `volume start` it. Composes the existing snapshot path:
///
/// 1. Refuse if the volume is readonly (no exclusive owner to release)
///    or an NBD client is connected (must disconnect cleanly first).
/// 2. If the volume is `stopped`, transparently bring it back up
///    (clear the marker, notify the supervisor, wait for
///    `control.sock`) — the drain step needs a running daemon.
/// 3. Verify S3 ownership before doing the expensive drain.
/// 4. Drain WAL → publish handoff snapshot via `snapshot_volume`.
/// 5. Send shutdown RPC to halt the daemon.
/// 6. Write `volume.stopped` marker so the supervisor won't restart.
/// 7. Conditional PUT to `names/<name>` setting state=Released and
///    recording the handoff snapshot ULID.
///
/// Two execution paths:
///
/// 1. **Fast path** (clean stopped volume, nothing to drain): reuse
///    the previously-published snapshot as the handoff, skip the
///    daemon restart entirely. Costs one S3 GET (ownership) + one
///    conditional PUT (flip).
///
/// 2. **Slow path** (WAL non-empty / pending uploads / GC handoffs /
///    new segments since last snapshot): bring the daemon up in
///    drain mode, run the existing snapshot pipeline, halt, flip.
#[allow(clippy::too_many_arguments)]
async fn release_volume_op(
    volume_name: &str,
    data_dir: &Path,
    snapshot_locks: &SnapshotLockRegistry,
    store: &Arc<dyn ObjectStore>,
    part_size_bytes: usize,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
    rescan: &Notify,
) -> Result<ReleaseReply, IpcError> {
    let coord_id = identity.coordinator_id_str();
    let started = std::time::Instant::now();
    info!("[release {volume_name}] start");

    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return Err(IpcError::not_found(format!(
            "volume not found: {volume_name}"
        )));
    }
    let vol_dir = std::fs::canonicalize(&link)
        .map_err(|e| IpcError::internal(format!("resolving volume dir: {e}")))?;

    if vol_dir.join("volume.readonly").exists() {
        return Err(IpcError::conflict("volume is readonly; nothing to release"));
    }

    // `release` is composed of two distinct phases: drain+publish (needs
    // a running daemon) and bucket-flip (no daemon needed). To keep the
    // operator-visible state coherent we require the daemon to already
    // be `stopped` — otherwise a release on a running volume would have
    // to halt it inline, and any failure between halt and bucket-flip
    // would leave the volume in a "Released-but-running" mismatch the
    // operator can't easily recover from.
    if !vol_dir.join(STOPPED_FILE).exists() {
        return Err(IpcError::conflict(format!(
            "volume '{volume_name}' is running; \
             stop it first with: elide volume stop {volume_name}"
        )));
    }

    // Verify ownership in S3 before doing any local state mutation.
    // Pulled ahead of the daemon restart so a "wrong owner" or
    // "already released" reply doesn't perturb the local volume.
    use elide_core::name_record::NameState;
    let read_started = std::time::Instant::now();
    match elide_coordinator::name_store::read_name_record(store, volume_name).await {
        Ok(Some((rec, _))) => {
            info!(
                "[release {volume_name}] read names/<name>: state={:?} owner={:?} ({:.2?})",
                rec.state,
                rec.coordinator_id,
                read_started.elapsed()
            );
            if let Some(existing) = rec.coordinator_id.as_deref()
                && existing != coord_id
            {
                return Err(IpcError::conflict(format!(
                    "name '{volume_name}' is owned by coordinator {existing}; \
                     run `volume release --force` to override"
                )));
            }
            if rec.state == NameState::Released {
                return Err(IpcError::conflict(format!(
                    "name '{volume_name}' is already released"
                )));
            }
        }
        Ok(None) => {
            return Err(IpcError::not_found(format!(
                "name '{volume_name}' has no S3 record; drain the volume first"
            )));
        }
        Err(e) => {
            return Err(IpcError::store(format!("reading names/{volume_name}: {e}")));
        }
    }

    // Fast path: nothing has changed since the last published snapshot,
    // so reuse it as the handoff. The next claimant forks from it
    // identically to a freshly-minted one.
    match release_fast_path_handoff(&vol_dir) {
        Ok(Some(snap_ulid)) => {
            info!(
                "[release {volume_name}] fast path: reusing snapshot {snap_ulid} \
                 (clean stopped volume, no daemon restart needed)"
            );
            let result = perform_release_flip(volume_name, store, identity, snap_ulid).await;
            info!(
                "[release {volume_name}] complete in {:.2?}",
                started.elapsed()
            );
            return result;
        }
        Ok(None) => {
            info!(
                "[release {volume_name}] slow path: WAL/pending/gc has work or \
                 segments post-date last snapshot"
            );
        }
        Err(e) => {
            warn!(
                "[release {volume_name}] fast-path inspection failed ({e}); \
                 falling back to slow path"
            );
        }
    }

    // ── Slow path: bring daemon up in drain mode ────────────────────
    //
    // The drain step needs an IPC-capable daemon, so we transparently
    // bring the stopped volume back up in IPC-only mode here. The
    // `volume.draining` marker tells both the supervisor and the
    // volume binary to suppress transport setup; from a client's
    // perspective the volume is never visibly running during release.
    //
    // The `_draining_guard` removes the marker when this function
    // returns, regardless of which exit path is taken (success, early
    // error, panic).
    std::fs::write(vol_dir.join("volume.draining"), "")
        .map_err(|e| IpcError::internal(format!("writing volume.draining: {e}")))?;
    if let Err(e) = std::fs::remove_file(vol_dir.join(STOPPED_FILE)) {
        let _ = std::fs::remove_file(vol_dir.join("volume.draining"));
        return Err(IpcError::internal(format!(
            "clearing volume.stopped for release: {e}"
        )));
    }
    rescan.notify_one();
    let mut _draining_guard = Some(DrainingMarkerGuard::new(vol_dir.clone()));

    let bringup_started = std::time::Instant::now();
    info!("[release {volume_name}] bringing daemon up for drain");
    if !wait_for_control_sock(&vol_dir, std::time::Duration::from_secs(30)).await {
        return Err(IpcError::internal(format!(
            "timed out waiting for volume '{volume_name}' to come up for release"
        )));
    }
    info!(
        "[release {volume_name}] daemon ready in {:.2?}",
        bringup_started.elapsed()
    );

    if !vol_dir.join("control.sock").exists() {
        return Err(IpcError::internal(format!(
            "volume '{volume_name}' is not running — start it first"
        )));
    }

    if elide_coordinator::control::is_connected(&vol_dir).await
        == elide_coordinator::control::ConnectedStatus::Connected
    {
        return Err(IpcError::conflict(
            "nbd client is connected; disconnect it first",
        ));
    }

    // Drain WAL → publish handoff snapshot. Empty volumes get a
    // freshly-minted snapshot ULID; the claim path forks from a
    // zero-entry snapshot as a fresh empty root.
    let snap_started = std::time::Instant::now();
    info!("[release {volume_name}] draining WAL and publishing handoff snapshot");
    let snap_ulid = snapshot_volume(
        volume_name,
        data_dir,
        snapshot_locks,
        store,
        part_size_bytes,
    )
    .await
    .map_err(|e| IpcError::internal(format!("snapshot for release failed: {e}")))?
    .snap_ulid;
    info!(
        "[release {volume_name}] handoff snapshot {snap_ulid} published in {:.2?}",
        snap_started.elapsed()
    );

    // Halt the daemon. Writing the marker first prevents the supervisor
    // from restarting it between shutdown and our final state write.
    // From this point on the release is past the "could fail and need
    // to leave the volume in a usable Stopped state" window — defuse
    // the draining-marker guard so it doesn't fight the explicit
    // halt step we're about to do.
    if let Some(g) = _draining_guard.as_mut() {
        g.defuse();
    }
    std::fs::write(vol_dir.join(STOPPED_FILE), "")
        .map_err(|e| IpcError::internal(format!("writing volume.stopped: {e}")))?;
    info!("[release {volume_name}] halting daemon");
    {
        use elide_coordinator::control::ShutdownOutcome;
        match elide_coordinator::control::shutdown(&vol_dir).await {
            ShutdownOutcome::Acknowledged => {}
            ShutdownOutcome::Failed(msg) => {
                let _ = std::fs::remove_file(vol_dir.join(STOPPED_FILE));
                return Err(IpcError::internal(format!("shutdown failed: {msg}")));
            }
            ShutdownOutcome::NotRunning => {
                // Volume process wasn't running by the time we reached
                // this step. The snapshot succeeded though, so we proceed
                // with the state flip.
            }
        }
    }

    let result = perform_release_flip(volume_name, store, identity, snap_ulid).await;
    info!(
        "[release {volume_name}] complete in {:.2?}",
        started.elapsed()
    );
    result
}

/// Final S3 conditional PUT flipping `names/<name>` to Released.
async fn perform_release_flip(
    volume_name: &str,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
    snap_ulid: ulid::Ulid,
) -> Result<ReleaseReply, IpcError> {
    use elide_coordinator::lifecycle::{self, MarkReleasedOutcome};
    let flip_started = std::time::Instant::now();
    info!(
        "[release {volume_name}] flipping names/<name> -> Released \
         with handoff snapshot {snap_ulid}"
    );
    match lifecycle::mark_released(store, volume_name, identity.coordinator_id_str(), snap_ulid)
        .await
    {
        Ok(MarkReleasedOutcome::Updated { vol_ulid }) => {
            info!(
                "[release {volume_name}] released at handoff snapshot {snap_ulid} \
                 (flip {:.2?})",
                flip_started.elapsed()
            );
            elide_coordinator::volume_event_store::emit_best_effort(
                store,
                identity.as_ref(),
                volume_name,
                elide_core::volume_event::EventKind::Released {
                    handoff_snapshot: snap_ulid,
                },
                vol_ulid,
            )
            .await;
            Ok(ReleaseReply {
                handoff_snapshot: snap_ulid,
            })
        }
        Ok(_) => {
            info!(
                "[release {volume_name}] release flip was idempotent or absent \
                 (no event emitted)"
            );
            Ok(ReleaseReply {
                handoff_snapshot: snap_ulid,
            })
        }
        Err(e) => {
            warn!("[release {volume_name}] state flip failed: {e}");
            Err(IpcError::store(format!(
                "snapshot {snap_ulid} published but names/<name> update failed: {e}"
            )))
        }
    }
}

/// Decide whether `release` can short-circuit using the volume's most
/// recently published snapshot as the handoff point.
///
/// Returns `Ok(Some(ulid))` when **all** of the following hold:
///   - `wal/`, `pending/`, `gc/` are empty or absent (no in-flight work)
///   - the latest segment in `index/` does not post-date the latest
///     local snapshot marker (the snapshot covers everything)
///   - that snapshot's S3 upload sentinel is present (manifest +
///     marker + filemap are confirmed on S3 — without this, a future
///     claimant could fail to fetch the manifest)
///
/// `Ok(None)` means slow path required; an `Err` is propagated to the
/// caller as a fast-path inspection failure (also slow-path fallback).
fn release_fast_path_handoff(vol_dir: &Path) -> std::io::Result<Option<ulid::Ulid>> {
    if !dir_is_empty_or_absent(&vol_dir.join("wal"))? {
        return Ok(None);
    }
    if !dir_is_empty_or_absent(&vol_dir.join("pending"))? {
        return Ok(None);
    }
    if !dir_is_empty_or_absent(&vol_dir.join("gc"))? {
        return Ok(None);
    }

    let Some(snap_ulid) = latest_snapshot_marker(&vol_dir.join("snapshots"))? else {
        return Ok(None);
    };

    // The snapshot triple (marker + filemap + .manifest) is uploaded
    // atomically; the sentinel is written only after all three succeed.
    // Its presence is the canonical "this snapshot is on S3" check.
    let sentinel = vol_dir
        .join("uploaded")
        .join("snapshots")
        .join(snap_ulid.to_string());
    if !sentinel.exists() {
        return Ok(None);
    }

    if let Some(seg) = latest_segment_ulid(&vol_dir.join("index"))?
        && seg > snap_ulid
    {
        return Ok(None);
    }

    Ok(Some(snap_ulid))
}

fn dir_is_empty_or_absent(p: &Path) -> std::io::Result<bool> {
    match std::fs::read_dir(p) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e),
    }
}

/// Return the highest ULID among `snapshots/<ulid>` markers (skipping
/// `<ulid>.manifest` / `<ulid>.filemap` siblings).
fn latest_snapshot_marker(snap_dir: &Path) -> std::io::Result<Option<ulid::Ulid>> {
    let entries = match std::fs::read_dir(snap_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut latest: Option<ulid::Ulid> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(s) = name.to_str() else { continue };
        // Skip filemap/manifest siblings: `<ulid>.filemap`,
        // `<ulid>.manifest`. Plain marker is the bare ULID.
        if s.contains('.') {
            continue;
        }
        if let Ok(u) = ulid::Ulid::from_string(s)
            && latest.is_none_or(|cur| u > cur)
        {
            latest = Some(u);
        }
    }
    Ok(latest)
}

/// Return the highest ULID among `index/<ulid>.idx` files.
fn latest_segment_ulid(index_dir: &Path) -> std::io::Result<Option<ulid::Ulid>> {
    let entries = match std::fs::read_dir(index_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut latest: Option<ulid::Ulid> = None;
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
    Ok(latest)
}

/// `volume claim <name>` IPC handler.
///
/// Inspects `names/<name>` and either:
///   - reclaims in place (own released fork still on disk) → `ok reclaimed`
///   - directs the CLI to orchestrate a foreign claim → `released <vol_ulid> <snap>`
///   - refuses if the record is `Live`/`Stopped` and owned by another
///     coordinator. The operator must run `release --force` first to
///     declare the previous owner dead and flip the record to
///     `Released`. Splitting the verbs keeps the claim step
///     CAS-protected (via `mark_claimed`) so concurrent claimants
///     are arbitrated by the conditional PUT, not by the unconditional
///     overwrite that `release --force` performs.
///
/// The result always leaves the volume `Stopped` (no daemon launched).
/// The CLI calls `start` afterwards if `volume start --claim` was the
/// composed flow.
async fn claim_volume_bucket_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
) -> Result<ClaimReply, IpcError> {
    let coord_id = identity.coordinator_id_str();
    use elide_core::name_record::NameState;

    // Claim always lands the volume in `Stopped`. A running local
    // daemon contradicts that — refuse and point the operator at
    // `volume stop` first.
    if local_daemon_running(data_dir, volume_name) {
        return Err(IpcError::conflict(format!(
            "volume '{volume_name}' is running on this host; \
             stop it first with: elide volume stop {volume_name}"
        )));
    }

    let record_opt = elide_coordinator::name_store::read_name_record(store, volume_name)
        .await
        .map_err(|e| IpcError::store(format!("reading names/{volume_name}: {e}")))?;

    let (record, _version) = record_opt.ok_or_else(|| {
        IpcError::not_found(format!(
            "name '{volume_name}' has no S3 record; nothing to claim"
        ))
    })?;

    match record.state {
        NameState::Released => {
            // Determine whether we hold a matching local fork.
            let link = data_dir.join("by_name").join(volume_name);
            let local_vol_ulid = match std::fs::canonicalize(&link) {
                Ok(p) => p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| ulid::Ulid::from_string(s).ok()),
                Err(_) => None,
            };

            if local_vol_ulid == Some(record.vol_ulid) {
                use elide_coordinator::lifecycle::{
                    LifecycleError, MarkReclaimedLocalOutcome, mark_reclaimed_local,
                };
                match mark_reclaimed_local(
                    store,
                    volume_name,
                    coord_id,
                    identity.hostname(),
                    record.vol_ulid,
                    NameState::Stopped,
                )
                .await
                {
                    Ok(MarkReclaimedLocalOutcome::Reclaimed) => {
                        info!(
                            "[inbound] reclaimed {volume_name} in place (vol_ulid {})",
                            record.vol_ulid
                        );
                        elide_coordinator::volume_event_store::emit_best_effort(
                            store,
                            identity.as_ref(),
                            volume_name,
                            elide_core::volume_event::EventKind::Claimed,
                            record.vol_ulid,
                        )
                        .await;
                        Ok(ClaimReply::Reclaimed)
                    }
                    Ok(MarkReclaimedLocalOutcome::Absent) => Err(IpcError::precondition_failed(
                        format!("names/{volume_name} vanished between read and reclaim"),
                    )),
                    Ok(MarkReclaimedLocalOutcome::NotReleased { observed_state, .. }) => {
                        Err(IpcError::precondition_failed(format!(
                            "names/{volume_name} changed underneath us; now in state \
                             {observed_state:?}"
                        )))
                    }
                    Ok(MarkReclaimedLocalOutcome::ForkMismatch {
                        released_vol_ulid, ..
                    }) => {
                        // Race: someone rebound between our read and write.
                        // Surface as MustClaimFresh routing — same shape as
                        // the foreign-content path below.
                        Ok(ClaimReply::MustClaimFresh {
                            released_vol_ulid,
                            handoff_snapshot: record.handoff_snapshot,
                        })
                    }
                    Err(LifecycleError::Store(e)) => {
                        Err(IpcError::store(format!("reclaim failed: {e}")))
                    }
                    Err(LifecycleError::OwnershipConflict { .. })
                    | Err(LifecycleError::InvalidTransition { .. }) => Err(IpcError::conflict(
                        format!("in-place reclaim of {volume_name} refused"),
                    )),
                }
            } else {
                // Foreign content — CLI must orchestrate the claim.
                Ok(ClaimReply::MustClaimFresh {
                    released_vol_ulid: record.vol_ulid,
                    handoff_snapshot: record.handoff_snapshot,
                })
            }
        }
        NameState::Live | NameState::Stopped => match record.coordinator_id.as_deref() {
            Some(owner) if owner == coord_id => {
                // Already ours — nothing to claim.
                Err(IpcError::conflict(format!(
                    "name '{volume_name}' is already held by this coordinator"
                )))
            }
            Some(owner) => Err(IpcError::conflict(format!(
                "name '{volume_name}' is held by coordinator {owner}"
            ))),
            None => Err(IpcError::internal(format!(
                "name '{volume_name}' has no coordinator_id (malformed record)"
            ))),
        },
        NameState::Readonly => Err(IpcError::conflict(format!(
            "name '{volume_name}' is readonly (immutable handle); \
             pull it with `volume pull` to serve locally"
        ))),
    }
}

async fn start_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    hostname: Option<&str>,
    rescan: &Notify,
) -> Result<(), IpcError> {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return Err(IpcError::not_found(format!(
            "volume '{volume_name}' not found locally; \
             to claim it from the bucket, run: elide volume claim {volume_name}"
        )));
    }
    let vol_dir = std::fs::canonicalize(&link)
        .map_err(|e| IpcError::internal(format!("resolving volume dir: {e}")))?;

    if !vol_dir.join(STOPPED_FILE).exists() {
        return Err(IpcError::conflict("volume is not stopped"));
    }

    use elide_coordinator::lifecycle::{LifecycleError, MarkLiveOutcome, mark_live};
    match mark_live(store, volume_name, coord_id, hostname).await {
        Ok(MarkLiveOutcome::Resumed) | Ok(MarkLiveOutcome::AlreadyLive) => {}
        Ok(MarkLiveOutcome::Absent) => {
            // No S3 record yet — proceed local-only.
        }
        Ok(MarkLiveOutcome::Released) => {
            return Err(IpcError::conflict(format!(
                "name '{volume_name}' is Released; \
                 reclaim with: elide volume claim {volume_name}"
            )));
        }
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            return Err(IpcError::conflict(format!(
                "name '{volume_name}' is owned by coordinator {held_by}; \
                 run `volume release --force` to override"
            )));
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            return Err(IpcError::conflict(format!(
                "names/<name> is in state {from:?}; cannot start"
            )));
        }
        Err(LifecycleError::Store(e)) => {
            warn!("[inbound] start {volume_name}: failed to update names/<name>: {e}");
        }
    }

    match elide_core::config::find_nbd_conflict(&vol_dir, data_dir) {
        Ok(Some(conflict)) => {
            return Err(IpcError::conflict(format!(
                "nbd endpoint {} conflicts with volume '{}'",
                conflict.endpoint, conflict.name,
            )));
        }
        Ok(None) => {}
        Err(e) => return Err(IpcError::internal(format!("nbd conflict check: {e}"))),
    }

    std::fs::remove_file(vol_dir.join(STOPPED_FILE))
        .map_err(|e| IpcError::internal(format!("clearing volume.stopped: {e}")))?;
    rescan.notify_one();
    info!("[inbound] started volume {volume_name}");
    Ok(())
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
    let pid_path = vol_dir.join(PID_FILE);
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
    volume_ulid: ulid::Ulid,
    data_dir: &Path,
    peer_pid: Option<i32>,
    macaroon_root: &[u8; 32],
) -> Result<RegisterReply, IpcError> {
    let ulid_str = volume_ulid.to_string();
    let vol_dir = data_dir.join("by_id").join(&ulid_str);
    if !vol_dir.exists() {
        return Err(IpcError::not_found("unknown volume"));
    }
    let pid = check_peer_pid(&vol_dir, peer_pid).map_err(IpcError::forbidden)?;
    let m = macaroon::mint(
        macaroon_root,
        vec![
            Caveat::Volume(ulid_str),
            Caveat::Scope(Scope::Credentials),
            Caveat::Pid(pid),
        ],
    );
    Ok(RegisterReply {
        macaroon: m.encode(),
    })
}

fn issue_credentials(
    macaroon_str: &str,
    data_dir: &Path,
    peer_pid: Option<i32>,
    macaroon_root: &[u8; 32],
    issuer: &dyn CredentialIssuer,
) -> Result<StoreCredsReply, IpcError> {
    let m = Macaroon::parse(macaroon_str)
        .map_err(|e| IpcError::bad_request(format!("parse macaroon: {e}")))?;
    if !macaroon::verify(macaroon_root, &m) {
        // Generic message — don't help an attacker distinguish "wrong key"
        // from "tampered caveats".
        return Err(IpcError::forbidden("invalid macaroon"));
    }
    if m.scope() != Some(Scope::Credentials) {
        return Err(IpcError::forbidden("macaroon scope mismatch"));
    }
    let volume_ulid = m
        .volume()
        .ok_or_else(|| IpcError::forbidden("macaroon missing volume caveat"))?;
    let macaroon_pid = m
        .pid()
        .ok_or_else(|| IpcError::forbidden("macaroon missing pid caveat"))?;
    let peer_pid = peer_pid.ok_or_else(|| IpcError::forbidden("peer pid unavailable"))?;
    if peer_pid != macaroon_pid {
        return Err(IpcError::forbidden("peer pid does not match macaroon"));
    }
    if let Some(not_after) = m.not_after() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now >= not_after {
            return Err(IpcError::forbidden("macaroon expired"));
        }
    }
    // Re-validate that volume.pid still matches — covers the case where the
    // original process has exited and the PID was reused.
    let vol_dir = data_dir.join("by_id").join(volume_ulid);
    check_peer_pid(&vol_dir, Some(peer_pid)).map_err(IpcError::forbidden)?;
    if !pid_is_alive(peer_pid as u32) {
        return Err(IpcError::forbidden("peer pid not alive"));
    }
    let creds = issuer
        .issue(volume_ulid)
        .map_err(|e| IpcError::internal(format!("issue: {e}")))?;
    Ok(StoreCredsReply {
        access_key_id: creds.access_key_id,
        secret_access_key: creds.secret_access_key,
        session_token: creds.session_token,
        expiry_unix: creds.expiry_unix,
    })
}

// ── Fork orchestrator (consolidated `fork-start` / `fork-attach`) ────────────

/// Register a fork job and spawn the orchestrator task.
///
/// Returns immediately once the job is in the registry; the actual
/// chain pull / snapshot / fork-create / prefetch flow runs in the
/// background. Errors here are synchronous validation failures
/// (duplicate name in flight, bad inputs); orchestrator errors are
/// surfaced via `attach_fork` instead.
fn start_fork(
    new_name: String,
    from: ForkSource,
    force_snapshot: bool,
    flags: Vec<String>,
    ctx: IpcContext,
) -> Result<ForkStartReply, IpcError> {
    {
        let mut reg = ctx.fork_registry.lock().expect("fork registry poisoned");
        if let Some(job) = reg.get(&new_name)
            && matches!(job.state(), ForkJobState::Running)
        {
            return Err(IpcError::conflict(format!(
                "fork '{new_name}' is already in progress"
            )));
        }
        reg.insert(new_name.clone(), ForkJob::new());
    }

    let job = {
        let reg = ctx.fork_registry.lock().expect("fork registry poisoned");
        reg.get(&new_name).cloned().expect("just inserted")
    };

    tokio::spawn(async move {
        let outcome = run_fork_job(job.clone(), new_name, from, force_snapshot, flags, ctx).await;
        match outcome {
            Ok(()) => {
                job.append(ForkAttachEvent::Done);
                job.finish(ForkJobState::Done);
            }
            Err(e) => job.finish(ForkJobState::Failed(e)),
        }
    });

    Ok(ForkStartReply::default())
}

/// Drive one fork-job to completion. Pushes per-stage events into
/// `job` so an attached subscriber sees progress in real time.
async fn run_fork_job(
    job: Arc<ForkJob>,
    new_name: String,
    from: ForkSource,
    force_snapshot: bool,
    flags: Vec<String>,
    ctx: IpcContext,
) -> Result<(), IpcError> {
    use elide_core::volume;

    let by_id_dir = ctx.data_dir.join("by_id");

    // Step 1: resolve `from` to (source_vol_ulid, optional source_name,
    // optional pre-pinned snap). Pulls the chain when the source isn't
    // already local.
    let (source_vol_ulid, source_name, snap_hint): (
        ulid::Ulid,
        Option<String>,
        Option<ulid::Ulid>,
    ) = match &from {
        ForkSource::Pinned {
            vol_ulid,
            snap_ulid,
        } => (*vol_ulid, None, Some(*snap_ulid)),
        ForkSource::BareUlid { vol_ulid } => (*vol_ulid, None, None),
        ForkSource::Name { name } => {
            let local = ctx.data_dir.join("by_name").join(name);
            if local.exists() {
                let canon = std::fs::canonicalize(&local)
                    .map_err(|e| IpcError::internal(format!("canonicalize by_name/{name}: {e}")))?;
                let ulid_str = canon
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| IpcError::internal("by_name link has non-utf8 target"))?;
                let vol_ulid = ulid::Ulid::from_string(ulid_str).map_err(|e| {
                    IpcError::internal(format!(
                        "by_name/{name} target {ulid_str:?} not a ULID: {e}"
                    ))
                })?;
                (vol_ulid, Some(name.clone()), None)
            } else {
                job.append(ForkAttachEvent::ResolvingName { name: name.clone() });
                let store = ctx.stores.coordinator_wide();
                let reply = resolve_name_op(name, &store).await?;
                (reply.vol_ulid, Some(name.clone()), None)
            }
        }
    };

    // Step 2: walk the ancestor chain, pulling each missing entry.
    let mut next: Option<ulid::Ulid> = Some(source_vol_ulid);
    while let Some(vol_ulid) = next.take() {
        let dir = volume::resolve_ancestor_dir(&by_id_dir, &vol_ulid.to_string());
        if dir.exists() {
            break;
        }
        job.append(ForkAttachEvent::PullingAncestor { vol_ulid });
        let store = ctx.stores.for_volume(&vol_ulid);
        let reply = pull_readonly_op(vol_ulid, &ctx.data_dir, &store).await?;
        next = reply.parent;
    }

    let source_ulid_str = source_vol_ulid.to_string();
    let source_dir = volume::resolve_ancestor_dir(&by_id_dir, &source_ulid_str);
    if !source_dir.exists() {
        return Err(IpcError::not_found(format!(
            "source volume {source_ulid_str} not found in remote store"
        )));
    }
    if source_dir.join(IMPORT_LOCK_FILE).exists() {
        return Err(IpcError::conflict(format!(
            "source '{source_ulid_str}' is still importing; wait for import to complete"
        )));
    }

    // Step 3: decide which snapshot the fork pins to.
    let mut parent_key_hex: Option<String> = None;
    let snap_ulid: Option<ulid::Ulid> = if let Some(snap) = snap_hint {
        // Pinned source already names the snapshot.
        Some(snap)
    } else if source_dir.join("volume.readonly").exists() {
        if force_snapshot {
            let store = ctx.stores.coordinator_wide();
            let reply = force_snapshot_now_op(source_vol_ulid, &ctx.data_dir, &store).await?;
            parent_key_hex = Some(reply.attestation_pubkey_hex.clone());
            job.append(ForkAttachEvent::AttestedSnapshot {
                snap_ulid: reply.snap_ulid,
                pubkey_hex: reply.attestation_pubkey_hex,
            });
            Some(reply.snap_ulid)
        } else if let Some(snap) = volume::latest_snapshot(&source_dir)
            .map_err(|e| IpcError::internal(format!("reading local snapshots: {e}")))?
        {
            Some(snap)
        } else {
            let store = ctx.stores.for_volume(&source_vol_ulid);
            match latest_snapshot_op(source_vol_ulid, &store)
                .await?
                .snapshot_ulid
            {
                Some(snap) => Some(snap),
                None => {
                    return Err(IpcError::not_found(format!(
                        "source volume {source_ulid_str} has no snapshots; pass \
                         force_snapshot=true to upload a new 'now' marker"
                    )));
                }
            }
        }
    } else {
        // Writable source: take an implicit snapshot. Need the
        // source's name to drive `snapshot_volume`; for `BareUlid` we
        // read it out of `volume.toml`.
        let name = if let Some(n) = source_name.clone() {
            n
        } else {
            elide_core::config::VolumeConfig::read(&source_dir)
                .map_err(|e| IpcError::internal(format!("read volume.toml: {e}")))?
                .name
                .ok_or_else(|| IpcError::internal("source volume has no name in volume.toml"))?
        };
        let store = ctx.stores.coordinator_wide();
        let reply = snapshot_volume(
            &name,
            &ctx.data_dir,
            &ctx.snapshot_locks,
            &store,
            ctx.part_size_bytes,
        )
        .await?;
        job.append(ForkAttachEvent::SnapshotTaken {
            snap_ulid: reply.snap_ulid,
        });
        Some(reply.snap_ulid)
    };

    // Step 4: mint the fork.
    job.append(ForkAttachEvent::ForkingFrom {
        source_vol_ulid,
        snap_ulid,
    });
    let store = ctx.stores.coordinator_wide();
    let reply = fork_create_op(
        &new_name,
        source_vol_ulid,
        snap_ulid,
        parent_key_hex.as_deref(),
        false,
        &flags,
        // For a `Name` source the orchestrator already knows the
        // user-facing name; pass it as a hint so a pulled source
        // (whose `volume.toml` lacks `name`) still produces a
        // `ForkedFrom` journal event instead of falling back to
        // `Created`. ULID-only sources (`BareUlid` / `Pinned`) have
        // no orchestrator-known name and rely on `src_cfg.name`.
        source_name.as_deref(),
        &ctx.data_dir,
        &store,
        &ctx.identity,
        &ctx.rescan,
        &ctx.prefetch_tracker,
    )
    .await?;
    job.append(ForkAttachEvent::ForkCreated {
        new_vol_ulid: reply.new_vol_ulid,
    });

    // Step 5: surface the coordinator's background prefetch. The fork
    // is already durable above; prefetch failure here is non-fatal —
    // the volume opens regardless and `volume start` re-awaits.
    job.append(ForkAttachEvent::PrefetchStarted);
    let _ = await_prefetch_op(reply.new_vol_ulid, &ctx.prefetch_tracker).await;
    job.append(ForkAttachEvent::PrefetchDone);

    Ok(())
}

/// Stream buffered and live fork events to `writer` as a sequence of
/// [`Envelope<ForkAttachEvent>`] messages, terminating with either
/// `ForkAttachEvent::Done` (success) or `Envelope::Err` (failure).
async fn stream_fork_by_name(new_name: &str, writer: &mut OwnedWriteHalf, registry: &ForkRegistry) {
    async fn write_err(writer: &mut OwnedWriteHalf, error: IpcError) {
        let env: Envelope<ForkAttachEvent> = Envelope::err(error);
        let _ = ipc::write_message(writer, &env).await;
    }

    let job = {
        let reg = registry.lock().expect("fork registry poisoned");
        reg.get(new_name).cloned()
    };
    let Some(job) = job else {
        write_err(
            writer,
            IpcError::not_found(format!("no active fork for: {new_name}")),
        )
        .await;
        return;
    };

    let mut offset = 0;
    loop {
        let events = job.read_from(offset);
        for event in &events {
            let env: Envelope<ForkAttachEvent> = Envelope::ok(event.clone());
            if ipc::write_message(writer, &env).await.is_err() {
                return;
            }
        }
        offset += events.len();

        match job.state() {
            ForkJobState::Done => return,
            ForkJobState::Failed(err) => {
                write_err(writer, err).await;
                return;
            }
            ForkJobState::Running => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

// ── Claim orchestrator (consolidated `claim-start` / `claim-attach`) ────────

/// Run the bucket-side claim synchronously and either return
/// `Reclaimed` (no further work) or register a job and spawn the
/// foreign-claim orchestrator. Returns immediately in both branches —
/// `Claiming` callers subscribe via `claim-attach` to stream progress.
async fn start_claim(volume: String, ctx: IpcContext) -> Result<ClaimStartReply, IpcError> {
    let store = ctx.stores.coordinator_wide();
    let bucket_started = std::time::Instant::now();
    let bucket = claim_volume_bucket_op(&volume, &ctx.data_dir, &store, &ctx.identity).await?;
    info!(
        "[claim {volume}] bucket-side claim resolved in {:.2?}",
        bucket_started.elapsed()
    );
    match bucket {
        ClaimReply::Reclaimed => Ok(ClaimStartReply::Reclaimed),
        ClaimReply::MustClaimFresh {
            released_vol_ulid,
            handoff_snapshot,
        } => {
            let snap = handoff_snapshot.ok_or_else(|| {
                IpcError::not_found(format!(
                    "name '{volume}' is Released but has no handoff snapshot — \
                     manual recovery required (see docs/operations.md)"
                ))
            })?;

            {
                let mut reg = ctx.claim_registry.lock().expect("claim registry poisoned");
                if let Some(job) = reg.get(&volume)
                    && matches!(job.state(), ClaimJobState::Running)
                {
                    return Err(IpcError::conflict(format!(
                        "claim for '{volume}' is already in progress"
                    )));
                }
                reg.insert(volume.clone(), ClaimJob::new());
            }
            let job = ctx
                .claim_registry
                .lock()
                .expect("claim registry poisoned")
                .get(&volume)
                .cloned()
                .expect("just inserted");

            tokio::spawn(async move {
                let outcome =
                    run_claim_job(job.clone(), volume, released_vol_ulid, snap, ctx).await;
                match outcome {
                    Ok(()) => {
                        job.append(ClaimAttachEvent::Done);
                        job.finish(ClaimJobState::Done);
                    }
                    Err(e) => job.finish(ClaimJobState::Failed(e)),
                }
            });

            Ok(ClaimStartReply::Claiming { released_vol_ulid })
        }
    }
}

/// Drive one claim-job to completion. Pulls the released volume's
/// chain if needed, resolves the handoff key, mints the fresh fork
/// (with `for_claim = true` so it skips `mark_initial` and lets
/// `mark_claimed` do the bucket rebind), and surfaces the
/// coordinator's background prefetch.
async fn run_claim_job(
    job: Arc<ClaimJob>,
    volume: String,
    released_vol_ulid: ulid::Ulid,
    handoff_snap: ulid::Ulid,
    ctx: IpcContext,
) -> Result<(), IpcError> {
    use elide_core::volume;

    let by_id_dir = ctx.data_dir.join("by_id");

    // Step 1: pull the released chain locally if absent. Walks
    // ancestor-by-ancestor exactly as the fork orchestrator does.
    let chain_started = std::time::Instant::now();
    let mut chain_pulled = 0usize;
    let mut next: Option<ulid::Ulid> = Some(released_vol_ulid);
    while let Some(vol_ulid) = next.take() {
        let dir = volume::resolve_ancestor_dir(&by_id_dir, &vol_ulid.to_string());
        if dir.exists() {
            break;
        }
        job.append(ClaimAttachEvent::PullingAncestor { vol_ulid });
        let store = ctx.stores.for_volume(&vol_ulid);
        let reply = pull_readonly_op(vol_ulid, &ctx.data_dir, &store).await?;
        chain_pulled += 1;
        next = reply.parent;
    }
    info!(
        "[claim {volume}] ancestor chain pulled: {chain_pulled} in {:.2?}",
        chain_started.elapsed()
    );

    let source_dir = volume::resolve_ancestor_dir(&by_id_dir, &released_vol_ulid.to_string());
    if !source_dir.exists() {
        return Err(IpcError::not_found(format!(
            "source volume {released_vol_ulid} not found in remote store"
        )));
    }

    // Step 2: resolve the handoff key. Recovery snapshots are signed
    // by an attestation key the fork's provenance must record so its
    // own signature verifies later.
    let handoff_started = std::time::Instant::now();
    let store = ctx.stores.for_volume(&released_vol_ulid);
    let key = resolve_handoff_key_op(released_vol_ulid, handoff_snap, &store).await?;
    info!(
        "[claim {volume}] handoff key resolved in {:.2?}",
        handoff_started.elapsed()
    );
    let parent_key_hex = match &key {
        ResolveHandoffKeyReply::Normal => None,
        ResolveHandoffKeyReply::Recovery {
            manifest_pubkey_hex,
        } => Some(manifest_pubkey_hex.clone()),
    };
    job.append(ClaimAttachEvent::HandoffKeyResolved { key });

    // Step 3: mint (or resume) the local fork. `for_claim = true`
    // tells `fork_create_op` to skip `mark_initial` (the released
    // record already exists) and to detect a resumable orphan at
    // `by_name/<name>`.
    let fork_create_started = std::time::Instant::now();
    let store_wide = ctx.stores.coordinator_wide();
    // Source's pre-release name == new fork name. Claim rebinds the
    // same `names/<volume>` record; the released volume held that
    // name before, so the journal's `ForkedFrom { source_name, ... }`
    // is the volume name itself. The released source's
    // `volume.toml` was written by the prior owner and is rarely
    // local to this host, so the hint is what makes this work.
    let reply = fork_create_op(
        &volume,
        released_vol_ulid,
        Some(handoff_snap),
        parent_key_hex.as_deref(),
        true,
        &[],
        Some(&volume),
        &ctx.data_dir,
        &store_wide,
        &ctx.identity,
        &ctx.rescan,
        &ctx.prefetch_tracker,
    )
    .await?;
    job.append(ClaimAttachEvent::ForkCreated {
        new_vol_ulid: reply.new_vol_ulid,
    });
    info!(
        "[claim {volume}] fork created in {:.2?}",
        fork_create_started.elapsed()
    );

    // Step 4: surface prefetch warm-up. Same non-fatal semantics as
    // the fork flow: the bucket-side claim and local fork are durable
    // by this point.
    let prefetch_wait_started = std::time::Instant::now();
    job.append(ClaimAttachEvent::PrefetchStarted);
    let _ = await_prefetch_op(reply.new_vol_ulid, &ctx.prefetch_tracker).await;
    job.append(ClaimAttachEvent::PrefetchDone);
    info!(
        "[claim {volume}] prefetch+prewarm awaited in {:.2?}",
        prefetch_wait_started.elapsed()
    );

    Ok(())
}

/// Stream buffered + live claim events to `writer`, terminating with
/// `Done` (success) or `Envelope::Err` (failure).
async fn stream_claim_by_name(volume: &str, writer: &mut OwnedWriteHalf, registry: &ClaimRegistry) {
    async fn write_err(writer: &mut OwnedWriteHalf, error: IpcError) {
        let env: Envelope<ClaimAttachEvent> = Envelope::err(error);
        let _ = ipc::write_message(writer, &env).await;
    }

    let job = {
        let reg = registry.lock().expect("claim registry poisoned");
        reg.get(volume).cloned()
    };
    let Some(job) = job else {
        write_err(
            writer,
            IpcError::not_found(format!("no active claim for: {volume}")),
        )
        .await;
        return;
    };

    let mut offset = 0;
    loop {
        let events = job.read_from(offset);
        for event in &events {
            let env: Envelope<ClaimAttachEvent> = Envelope::ok(event.clone());
            if ipc::write_message(writer, &env).await.is_err() {
                return;
            }
        }
        offset += events.len();

        match job.state() {
            ClaimJobState::Done => return,
            ClaimJobState::Failed(err) => {
                write_err(writer, err).await;
                return;
            }
            ClaimJobState::Running => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::IssuedCredentials;
    use elide_coordinator::ipc::IpcErrorKind;
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
        std::fs::write(dir.join(PID_FILE), pid.to_string()).unwrap();
    }

    fn key() -> [u8; 32] {
        [0x42; 32]
    }

    fn ulid_from(s: &str) -> ulid::Ulid {
        ulid::Ulid::from_string(s).unwrap()
    }

    #[test]
    fn register_succeeds_when_peer_pid_matches_volume_pid() {
        let tmp = TempDir::new().unwrap();
        let ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        setup_volume(tmp.path(), ulid_str, 4242);
        let reply = register_volume(ulid_from(ulid_str), tmp.path(), Some(4242), &key())
            .expect("matching pid → ok");
        assert!(!reply.macaroon.is_empty());
    }

    #[test]
    fn register_rejects_pid_mismatch() {
        let tmp = TempDir::new().unwrap();
        let ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        setup_volume(tmp.path(), ulid_str, 4242);
        let err = register_volume(ulid_from(ulid_str), tmp.path(), Some(9999), &key())
            .expect_err("mismatched pid should error");
        assert_eq!(err.kind, IpcErrorKind::Forbidden);
        assert!(err.message.contains("does not match"), "{err}");
    }

    #[test]
    fn register_rejects_when_volume_pid_missing() {
        let tmp = TempDir::new().unwrap();
        let ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        // Create the volume dir but no volume.pid yet — this is the spawn-write
        // race the volume-side retry absorbs.
        std::fs::create_dir_all(tmp.path().join("by_id").join(ulid_str)).unwrap();
        let err = register_volume(ulid_from(ulid_str), tmp.path(), Some(4242), &key())
            .expect_err("missing pid file should error");
        assert!(err.message.contains("not registered"), "{err}");
    }

    #[test]
    fn register_rejects_unknown_volume() {
        let tmp = TempDir::new().unwrap();
        let err = register_volume(
            ulid_from("01JQAAAAAAAAAAAAAAAAAAAAAA"),
            tmp.path(),
            Some(4242),
            &key(),
        )
        .expect_err("unknown volume should error");
        assert_eq!(err.kind, IpcErrorKind::NotFound);
        assert!(err.message.contains("unknown volume"), "{err}");
    }

    #[test]
    fn credentials_round_trip_with_live_pid() {
        let tmp = TempDir::new().unwrap();
        let ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        // Use our own pid so the pid_is_alive check passes inside the handler.
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid_str, my_pid);
        let mint_reply = register_volume(ulid_from(ulid_str), tmp.path(), Some(my_pid), &key())
            .expect("register should succeed");
        let issuer = FixedIssuer;
        let creds = issue_credentials(
            &mint_reply.macaroon,
            tmp.path(),
            Some(my_pid),
            &key(),
            &issuer,
        )
        .expect("credentials should succeed");
        assert_eq!(creds.access_key_id, "AK");
        assert_eq!(creds.secret_access_key, "SK");
    }

    #[test]
    fn credentials_rejects_wrong_root_key() {
        let tmp = TempDir::new().unwrap();
        let ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid_str, my_pid);
        let mint_reply = register_volume(ulid_from(ulid_str), tmp.path(), Some(my_pid), &key())
            .expect("register should succeed");
        let mut other = key();
        other[0] ^= 0xFF;
        let issuer = FixedIssuer;
        let err = issue_credentials(
            &mint_reply.macaroon,
            tmp.path(),
            Some(my_pid),
            &other,
            &issuer,
        )
        .expect_err("wrong root key should fail");
        assert_eq!(err.kind, IpcErrorKind::Forbidden);
        assert!(err.message.contains("invalid macaroon"), "{err}");
    }

    #[test]
    fn credentials_rejects_pid_mismatch() {
        let tmp = TempDir::new().unwrap();
        let ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid_str, my_pid);
        let mint_reply = register_volume(ulid_from(ulid_str), tmp.path(), Some(my_pid), &key())
            .expect("register should succeed");
        let issuer = FixedIssuer;
        // Present a different peer pid than the macaroon was minted for.
        let err = issue_credentials(
            &mint_reply.macaroon,
            tmp.path(),
            Some(my_pid + 1),
            &key(),
            &issuer,
        )
        .expect_err("pid mismatch should fail");
        assert_eq!(err.kind, IpcErrorKind::Forbidden);
        assert!(
            err.message.contains("does not match macaroon")
                || err.message.contains("does not match volume.pid"),
            "{err}"
        );
    }

    #[test]
    fn credentials_rejects_tampered_caveat() {
        let tmp = TempDir::new().unwrap();
        let ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid_str, my_pid);
        // Mint a macaroon for a different volume — the verify call rejects
        // because the volume caveat is wrong even though MAC matches.
        let other_ulid_str = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        setup_volume(tmp.path(), other_ulid_str, my_pid);
        let mint_reply =
            register_volume(ulid_from(other_ulid_str), tmp.path(), Some(my_pid), &key())
                .expect("register should succeed");
        let parsed = Macaroon::parse(&mint_reply.macaroon).unwrap();
        let mut caveats: Vec<Caveat> = parsed.caveats().to_vec();
        for c in &mut caveats {
            if let Caveat::Volume(v) = c {
                *v = ulid_str.to_owned();
            }
        }
        // Construct a forged macaroon with rewritten caveats under a wrong
        // key — verify must fail.
        let forged = macaroon::mint(&[0u8; 32], caveats);
        let issuer = FixedIssuer;
        let err = issue_credentials(&forged.encode(), tmp.path(), Some(my_pid), &key(), &issuer)
            .expect_err("tampered caveat should fail");
        assert_eq!(err.kind, IpcErrorKind::Forbidden);
        assert!(err.message.contains("invalid macaroon"), "{err}");
    }

    #[test]
    fn credentials_rejects_expired_macaroon() {
        let tmp = TempDir::new().unwrap();
        let ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid_str, my_pid);
        let m = macaroon::mint(
            &key(),
            vec![
                Caveat::Volume(ulid_str.to_owned()),
                Caveat::Scope(macaroon::Scope::Credentials),
                Caveat::Pid(my_pid),
                Caveat::NotAfter(1), // unix second 1 — long ago
            ],
        );
        let issuer = FixedIssuer;
        let err = issue_credentials(&m.encode(), tmp.path(), Some(my_pid), &key(), &issuer)
            .expect_err("expired macaroon should fail");
        assert_eq!(err.kind, IpcErrorKind::Forbidden);
        assert!(err.message.contains("expired"), "{err}");
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

    // ── status-remote ─────────────────────────────────────────────────────

    fn mem_store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    fn sample_ulid() -> ulid::Ulid {
        ulid::Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    #[tokio::test]
    async fn status_remote_absent_name_returns_err() {
        let store = mem_store();
        let err = volume_status_remote_typed("ghost", &store, "coord-self")
            .await
            .expect_err("absent name should return an error");
        assert_eq!(err.kind, elide_coordinator::ipc::IpcErrorKind::NotFound);
        assert!(err.message.contains("no S3 record"), "{}", err.message);
    }

    #[tokio::test]
    async fn status_remote_owned_live_record() {
        let store = mem_store();
        let mut rec = elide_core::name_record::NameRecord::live_minimal(sample_ulid());
        rec.coordinator_id = Some("coord-self".into());
        rec.hostname = Some("host-A".into());
        rec.claimed_at = Some("2026-04-28T00:00:00Z".into());
        elide_coordinator::name_store::create_name_record(&store, "vol", &rec)
            .await
            .unwrap();

        let reply = volume_status_remote_typed("vol", &store, "coord-self")
            .await
            .unwrap();
        assert_eq!(reply.state, elide_core::name_record::NameState::Live);
        assert_eq!(reply.vol_ulid, sample_ulid());
        assert_eq!(reply.coordinator_id.as_deref(), Some("coord-self"));
        assert_eq!(reply.hostname.as_deref(), Some("host-A"));
        assert_eq!(reply.claimed_at.as_deref(), Some("2026-04-28T00:00:00Z"));
        assert_eq!(reply.eligibility, Eligibility::Owned);
    }

    #[tokio::test]
    async fn status_remote_foreign_live_record() {
        let store = mem_store();
        let mut rec = elide_core::name_record::NameRecord::live_minimal(sample_ulid());
        rec.coordinator_id = Some("coord-other".into());
        elide_coordinator::name_store::create_name_record(&store, "vol", &rec)
            .await
            .unwrap();

        let reply = volume_status_remote_typed("vol", &store, "coord-self")
            .await
            .unwrap();
        assert_eq!(reply.state, elide_core::name_record::NameState::Live);
        assert_eq!(reply.eligibility, Eligibility::Foreign);
    }

    #[tokio::test]
    async fn status_remote_released_is_claimable() {
        let store = mem_store();
        let mut rec = elide_core::name_record::NameRecord::live_minimal(sample_ulid());
        rec.state = elide_core::name_record::NameState::Released;
        rec.handoff_snapshot = Some(sample_ulid());
        elide_coordinator::name_store::create_name_record(&store, "vol", &rec)
            .await
            .unwrap();

        let reply = volume_status_remote_typed("vol", &store, "coord-self")
            .await
            .unwrap();
        assert_eq!(reply.state, elide_core::name_record::NameState::Released);
        assert_eq!(reply.eligibility, Eligibility::ReleasedClaimable);
        assert_eq!(reply.handoff_snapshot, Some(sample_ulid()));
    }

    #[tokio::test]
    async fn status_remote_readonly() {
        let store = mem_store();
        let mut rec = elide_core::name_record::NameRecord::live_minimal(sample_ulid());
        rec.state = elide_core::name_record::NameState::Readonly;
        elide_coordinator::name_store::create_name_record(&store, "img", &rec)
            .await
            .unwrap();

        let reply = volume_status_remote_typed("img", &store, "coord-self")
            .await
            .unwrap();
        assert_eq!(reply.state, elide_core::name_record::NameState::Readonly);
        assert_eq!(reply.eligibility, Eligibility::Readonly);
    }

    // ── force_release_volume_op ───────────────────────────────────────────
    //
    // Verify the inbound op composes recovery + lifecycle + name_store
    // correctly. The lower-level helpers each have unit coverage already;
    // these tests exercise the IPC verb's end-to-end path: read current
    // record → fetch dead pubkey → list+verify segments → publish
    // synthesised snapshot → unconditionally rewrite names/<name>.

    use elide_coordinator::identity::CoordinatorIdentity;
    use elide_coordinator::name_store as ns;
    use elide_core::name_record::{NameRecord, NameState};
    use elide_core::segment::{SegmentEntry, SegmentFlags, SegmentSigner, write_segment};
    use elide_core::signing::generate_ephemeral_signer;
    use object_store::PutPayload;
    use object_store::path::Path as StorePath;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Upload a `volume.pub` for the dead fork at the canonical path.
    async fn upload_dead_pub(
        store: &Arc<dyn ObjectStore>,
        vol_ulid: ulid::Ulid,
        vk: &ed25519_dalek::VerifyingKey,
    ) {
        let key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
        let body = format!("{}\n", hex(&vk.to_bytes()));
        store
            .put(&key, PutPayload::from(body.into_bytes()))
            .await
            .unwrap();
    }

    /// Build a single-entry signed segment via the canonical writer and
    /// upload it under `by_id/<vol_ulid>/segments/<seg_ulid>`.
    async fn upload_signed_segment(
        store: &Arc<dyn ObjectStore>,
        vol_ulid: ulid::Ulid,
        seg_ulid: ulid::Ulid,
        signer: &dyn SegmentSigner,
        body: &[u8],
    ) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg");
        let hash = blake3::hash(body);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            body.to_vec(),
        )];
        write_segment(&path, &mut entries, signer).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let key = StorePath::from(format!("by_id/{vol_ulid}/segments/{seg_ulid}"));
        store.put(&key, PutPayload::from(bytes)).await.unwrap();
    }

    /// Fixture: a name in `Live` state pointing at a dead fork that has
    /// `volume.pub` and one signed segment in S3, plus a fresh
    /// `CoordinatorIdentity` for the recovering coordinator.
    async fn force_release_fixture(
        name: &str,
    ) -> (
        Arc<dyn ObjectStore>,
        Arc<CoordinatorIdentity>,
        ulid::Ulid,
        ulid::Ulid,
        TempDir,
    ) {
        let store: Arc<dyn ObjectStore> = mem_store();
        let dead_vol = ulid::Ulid::new();
        let seg_ulid = ulid::Ulid::new();

        let (signer, vk) = generate_ephemeral_signer();
        upload_dead_pub(&store, dead_vol, &vk).await;
        upload_signed_segment(&store, dead_vol, seg_ulid, signer.as_ref(), b"data").await;

        // names/<name> = Live, owned by some "previous" coordinator id.
        let mut rec = NameRecord::live_minimal(dead_vol);
        rec.coordinator_id = Some("dead-owner".into());
        ns::create_name_record(&store, name, &rec).await.unwrap();

        // Recovering coordinator's identity, rooted in a tempdir.
        let coord_dir = TempDir::new().unwrap();
        let identity = Arc::new(CoordinatorIdentity::load_or_generate(coord_dir.path()).unwrap());

        (store, identity, dead_vol, seg_ulid, coord_dir)
    }

    #[tokio::test]
    async fn force_release_op_overwrites_live_to_released() {
        let (store, identity, dead_vol, _seg, _td) = force_release_fixture("vol").await;

        let reply =
            force_release_volume_op("vol", TempDir::new().unwrap().path(), &store, &identity)
                .await
                .expect("force-release should succeed");
        let snap_ulid = reply.handoff_snapshot;

        // names/<vol> is now Released, references the dead fork.
        let (rec, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(rec.state, NameState::Released);
        assert_eq!(rec.vol_ulid, dead_vol);
        assert_eq!(rec.handoff_snapshot, Some(snap_ulid));

        // Synthesised manifest landed under the dead fork's snapshots/ prefix.
        let snap_prefix = StorePath::from(format!("by_id/{dead_vol}/snapshots/"));
        use futures::TryStreamExt;
        let listed: Vec<_> = store.list(Some(&snap_prefix)).try_collect().await.unwrap();
        assert_eq!(listed.len(), 1, "exactly one synthesised snapshot");
        assert!(
            listed[0]
                .location
                .as_ref()
                .ends_with(&format!("{snap_ulid}.manifest")),
            "manifest path is {}",
            listed[0].location.as_ref()
        );
    }

    #[tokio::test]
    async fn force_release_op_refuses_already_released_record() {
        let (store, identity, _dead, _seg, _td) = force_release_fixture("vol").await;

        // Pre-flip names/<vol> to Released so the op refuses.
        let (mut rec, v) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        rec.state = NameState::Released;
        rec.coordinator_id = None;
        ns::update_name_record(&store, "vol", &rec, v)
            .await
            .unwrap();

        let err = force_release_volume_op("vol", TempDir::new().unwrap().path(), &store, &identity)
            .await
            .expect_err("already-released record must refuse");
        assert_eq!(err.kind, elide_coordinator::ipc::IpcErrorKind::Conflict);
        assert!(
            err.message
                .contains("force-release only overrides Live or Stopped"),
            "{}",
            err.message
        );
    }

    #[tokio::test]
    async fn force_release_op_refuses_absent_name() {
        let store = mem_store();
        let coord_dir = TempDir::new().unwrap();
        let identity = Arc::new(CoordinatorIdentity::load_or_generate(coord_dir.path()).unwrap());

        let err =
            force_release_volume_op("ghost", TempDir::new().unwrap().path(), &store, &identity)
                .await
                .expect_err("ghost name must error");
        assert_eq!(err.kind, elide_coordinator::ipc::IpcErrorKind::NotFound);
        assert!(err.message.contains("no S3 record"), "{}", err.message);
    }

    #[tokio::test]
    async fn force_release_op_recovers_when_dead_pub_missing() {
        // Reproduces the create-time crash window: `names/<name>` was
        // published to S3 but the coordinator died before
        // `volume.pub` made it to the bucket. With no `volume.pub`
        // there is no key to verify any segment under, so the dead
        // fork is provably empty — force-release publishes an empty
        // synthesised handoff and flips to Released.
        let store: Arc<dyn ObjectStore> = mem_store();
        let dead_vol = ulid::Ulid::new();
        let mut rec = NameRecord::live_minimal(dead_vol);
        rec.coordinator_id = Some("dead-owner".into());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        let coord_dir = TempDir::new().unwrap();
        let identity = Arc::new(CoordinatorIdentity::load_or_generate(coord_dir.path()).unwrap());

        let reply =
            force_release_volume_op("vol", TempDir::new().unwrap().path(), &store, &identity)
                .await
                .expect("force-release on missing-pub fork should succeed");
        let snap_ulid = reply.handoff_snapshot;

        let (rec, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(rec.state, NameState::Released);
        assert_eq!(rec.vol_ulid, dead_vol);
        assert_eq!(rec.handoff_snapshot, Some(snap_ulid));

        // The synthesised manifest covers no segments.
        let snap_prefix = StorePath::from(format!("by_id/{dead_vol}/snapshots/"));
        use futures::TryStreamExt;
        let listed: Vec<_> = store.list(Some(&snap_prefix)).try_collect().await.unwrap();
        assert_eq!(listed.len(), 1);
        let manifest = store
            .get(&listed[0].location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let recovery = elide_core::signing::peek_snapshot_manifest_recovery(&manifest)
            .unwrap()
            .expect("synthesised handoff must carry recovery metadata");
        assert_eq!(
            recovery.recovering_coordinator_id,
            identity.coordinator_id_str()
        );
    }

    #[tokio::test]
    async fn force_release_op_drops_tampered_segment_but_succeeds() {
        // A tampered segment must be dropped (signature failure) without
        // failing the verb. The published snapshot covers only the
        // verified segments; the operator can still recover the name.
        let store: Arc<dyn ObjectStore> = mem_store();
        let dead_vol = ulid::Ulid::new();
        let (signer, vk) = generate_ephemeral_signer();
        upload_dead_pub(&store, dead_vol, &vk).await;

        // One good segment, one tampered.
        let good_id = ulid::Ulid::new();
        upload_signed_segment(&store, dead_vol, good_id, signer.as_ref(), b"good").await;
        let bad_id = ulid::Ulid::new();
        // Build a valid segment then flip a byte inside the index section.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg");
        let hash = blake3::hash(b"bad");
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            b"bad".to_vec(),
        )];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        // Header is 100 bytes; first index entry starts at offset 100.
        bytes[104] ^= 0xff;
        let bad_key = StorePath::from(format!("by_id/{dead_vol}/segments/{bad_id}"));
        store.put(&bad_key, PutPayload::from(bytes)).await.unwrap();

        let mut rec = NameRecord::live_minimal(dead_vol);
        rec.coordinator_id = Some("dead-owner".into());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        let coord_dir = TempDir::new().unwrap();
        let identity = Arc::new(CoordinatorIdentity::load_or_generate(coord_dir.path()).unwrap());

        let reply =
            force_release_volume_op("vol", TempDir::new().unwrap().path(), &store, &identity)
                .await
                .expect("force-release with one tampered segment must still succeed");
        let snap_str = reply.handoff_snapshot.to_string();

        // Verify the synthesised manifest contains exactly one segment ULID
        // — the good one. Read the manifest body and look for both ids.
        use futures::TryStreamExt;
        let snap_prefix = StorePath::from(format!("by_id/{dead_vol}/snapshots/"));
        let listed: Vec<_> = store.list(Some(&snap_prefix)).try_collect().await.unwrap();
        let entry = listed
            .into_iter()
            .find(|m| {
                m.location
                    .as_ref()
                    .ends_with(&format!("{snap_str}.manifest"))
            })
            .expect("synthesised manifest exists");
        let manifest_body = store
            .get(&entry.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let body_str = std::str::from_utf8(&manifest_body).unwrap();
        assert!(
            body_str.contains(&good_id.to_string()),
            "good segment present"
        );
        assert!(
            !body_str.contains(&bad_id.to_string()),
            "tampered segment must not appear in manifest"
        );
    }

    // ── release / stop preconditions ──────────────────────────────────────
    //
    // Two bugs surfaced by manual testing:
    //
    //   1. `release` accepted a running volume and tried to halt it
    //      inline. A failure between the inline halt and the bucket
    //      flip would strand the volume in a "Released-but-running"
    //      state. Fix: refuse if `volume.stopped` is absent.
    //
    //   2. `stop` refused when the bucket said Released/
    //      Readonly. But `stop` is a *local* lifecycle verb — its job
    //      is to halt the daemon. The bucket update is best-effort.
    //      A daemon left running while the bucket says Released
    //      (e.g. because of bug 1) was unstoppable. Fix: warn-and-skip
    //      the bucket update on InvalidTransition, halt locally
    //      regardless.

    /// Build a `by_name/<vol>` symlink pointing at a fresh
    /// `by_id/<ulid>/` directory without a `volume.stopped` marker —
    /// i.e. the on-disk shape of a (notionally) running volume.
    fn make_running_volume(data_dir: &Path) -> ulid::Ulid {
        let vol_ulid = ulid::Ulid::new();
        let vol_dir = data_dir.join("by_id").join(vol_ulid.to_string());
        std::fs::create_dir_all(&vol_dir).unwrap();
        std::fs::create_dir_all(data_dir.join("by_name")).unwrap();
        let link = data_dir.join("by_name").join("vol");
        let target = std::path::PathBuf::from(format!("../by_id/{vol_ulid}"));
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link).unwrap();
        vol_ulid
    }

    #[tokio::test]
    async fn release_op_refuses_when_volume_is_running() {
        let store = mem_store();
        let data_dir = TempDir::new().unwrap();
        let snapshot_locks = SnapshotLockRegistry::default();
        let rescan = Notify::new();

        // Running volume: by_name symlink + by_id dir, NO volume.stopped.
        let vol_ulid = make_running_volume(data_dir.path());

        let identity = std::sync::Arc::new(
            elide_coordinator::identity::CoordinatorIdentity::load_or_generate(data_dir.path())
                .unwrap(),
        );

        // names/<vol> = Live owned by us — would have been the path
        // through the rest of release_volume_op before this fix.
        let mut rec = NameRecord::live_minimal(vol_ulid);
        rec.coordinator_id = Some(identity.coordinator_id_str().to_owned());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        let err = release_volume_op(
            "vol",
            data_dir.path(),
            &snapshot_locks,
            &store,
            8 * 1024 * 1024,
            &identity,
            &rescan,
        )
        .await
        .expect_err("running volume must refuse release");

        assert_eq!(err.kind, elide_coordinator::ipc::IpcErrorKind::Conflict);
        assert!(
            err.message.contains("running") && err.message.contains("volume stop"),
            "expected operator to be pointed at `volume stop`, got: {}",
            err.message
        );

        // The bucket record must be untouched — still Live.
        let (still, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(still.state, NameState::Live);
    }

    #[tokio::test]
    async fn stop_op_halts_locally_when_bucket_says_released() {
        // Bug-2 reproducer: bucket is Released (e.g. from a partial
        // earlier release), daemon is still running on this host. Stop
        // must succeed, halting the daemon and leaving the bucket
        // record unchanged (we don't own a Released record).
        let store = mem_store();
        let data_dir = TempDir::new().unwrap();

        let vol_ulid = make_running_volume(data_dir.path());

        let mut rec = NameRecord::live_minimal(vol_ulid);
        rec.state = NameState::Released;
        rec.coordinator_id = None;
        rec.handoff_snapshot = Some(ulid::Ulid::new());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        stop_volume_op("vol", data_dir.path(), &store, "coord-self", None)
            .await
            .expect("stop must halt locally regardless of bucket state");

        // Local marker now present.
        let vol_dir = data_dir.path().join("by_id").join(vol_ulid.to_string());
        assert!(
            vol_dir.join(STOPPED_FILE).exists(),
            "volume.stopped marker should be written"
        );

        // Bucket record untouched: still Released, no coordinator_id.
        let (still, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(still.state, NameState::Released);
        assert!(still.coordinator_id.is_none());
    }

    #[tokio::test]
    async fn stop_op_halts_locally_when_bucket_says_foreign_live() {
        // Even split-brain bucket state must not block a local halt.
        // If a daemon is running on our host while names/<name> is
        // owned by another coordinator, halting our local process is
        // the right cleanup — it doesn't affect their host. We leave
        // the bucket record untouched.
        let store = mem_store();
        let data_dir = TempDir::new().unwrap();

        let vol_ulid = make_running_volume(data_dir.path());

        let mut rec = NameRecord::live_minimal(vol_ulid);
        rec.coordinator_id = Some("coord-other".into());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        stop_volume_op("vol", data_dir.path(), &store, "coord-self", None)
            .await
            .expect("stop must halt locally despite foreign bucket state");

        let vol_dir = data_dir.path().join("by_id").join(vol_ulid.to_string());
        assert!(vol_dir.join(STOPPED_FILE).exists());

        // Bucket record untouched — still owned by the other coordinator.
        let (still, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(still.state, NameState::Live);
        assert_eq!(still.coordinator_id.as_deref(), Some("coord-other"));
    }

    // ── release fast-path predicate ────────────────────────────────────
    //
    // `release_fast_path_handoff` decides whether a `volume release` can
    // skip the daemon restart and reuse the previously-published
    // snapshot. Each branch below exercises one ineligibility reason
    // plus one happy path. Per CLAUDE.md "monotonic ULIDs in tests" we
    // mint via `UlidMint` whenever ordering matters.

    use elide_core::ulid_mint::UlidMint;

    /// Set up the on-disk skeleton a clean stopped volume would have
    /// after at least one snapshot has been published and uploaded.
    fn fast_path_clean_volume(snap_ulid: ulid::Ulid) -> TempDir {
        let tmp = TempDir::new().unwrap();
        for sub in ["wal", "pending", "gc", "index", "snapshots"] {
            std::fs::create_dir_all(tmp.path().join(sub)).unwrap();
        }
        // Snapshot marker (bare-ULID file).
        std::fs::write(tmp.path().join("snapshots").join(snap_ulid.to_string()), "").unwrap();
        // Upload sentinel: volume/<id>/uploaded/snapshots/<ulid>.
        std::fs::create_dir_all(tmp.path().join("uploaded").join("snapshots")).unwrap();
        std::fs::write(
            tmp.path()
                .join("uploaded")
                .join("snapshots")
                .join(snap_ulid.to_string()),
            "",
        )
        .unwrap();
        tmp
    }

    #[test]
    fn fast_path_eligible_when_clean_with_uploaded_snapshot() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        let got = release_fast_path_handoff(tmp.path()).unwrap();
        assert_eq!(got, Some(snap));
    }

    #[test]
    fn fast_path_ineligible_when_wal_non_empty() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        std::fs::write(
            tmp.path().join("wal").join("01JANYSEGULID00000000000000"),
            "x",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_pending_non_empty() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        std::fs::write(tmp.path().join("pending").join("seg"), "x").unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_gc_non_empty() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        std::fs::write(
            tmp.path().join("gc").join("01JANYGCULID0000000000000000"),
            "x",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_no_snapshot_published() {
        let tmp = TempDir::new().unwrap();
        for sub in ["wal", "pending", "gc", "index", "snapshots"] {
            std::fs::create_dir_all(tmp.path().join(sub)).unwrap();
        }
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_snapshot_not_yet_uploaded() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        // Remove the sentinel: snapshot is signed locally but not on S3.
        std::fs::remove_file(
            tmp.path()
                .join("uploaded")
                .join("snapshots")
                .join(snap.to_string()),
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_segment_post_dates_snapshot() {
        let mut mint = UlidMint::new(ulid::Ulid::nil());
        let snap = mint.next();
        let later_segment = mint.next();
        assert!(later_segment > snap, "UlidMint must mint monotonically");
        let tmp = fast_path_clean_volume(snap);
        // A new segment landed in `index/` after the last snapshot —
        // slow path must run so the new snapshot covers it.
        std::fs::write(
            tmp.path()
                .join("index")
                .join(format!("{later_segment}.idx")),
            "",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_eligible_when_segment_predates_snapshot() {
        let mut mint = UlidMint::new(ulid::Ulid::nil());
        let earlier_segment = mint.next();
        let snap = mint.next();
        assert!(snap > earlier_segment);
        let tmp = fast_path_clean_volume(snap);
        // Older segment is already covered by the snapshot — fine.
        std::fs::write(
            tmp.path()
                .join("index")
                .join(format!("{earlier_segment}.idx")),
            "",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), Some(snap));
    }

    #[test]
    fn fast_path_picks_latest_snapshot_when_multiple_present() {
        let mut mint = UlidMint::new(ulid::Ulid::nil());
        let older_snap = mint.next();
        let newer_snap = mint.next();
        let tmp = fast_path_clean_volume(newer_snap);
        // Older marker + sentinel (the volume kept history).
        std::fs::write(
            tmp.path().join("snapshots").join(older_snap.to_string()),
            "",
        )
        .unwrap();
        std::fs::write(
            tmp.path()
                .join("uploaded")
                .join("snapshots")
                .join(older_snap.to_string()),
            "",
        )
        .unwrap();
        assert_eq!(
            release_fast_path_handoff(tmp.path()).unwrap(),
            Some(newer_snap)
        );
    }

    #[test]
    fn fast_path_ignores_filemap_and_manifest_siblings() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        // The snapshots dir has marker plus .filemap and .manifest
        // siblings; only the bare-ULID marker should be considered.
        std::fs::write(
            tmp.path().join("snapshots").join(format!("{snap}.filemap")),
            "fm",
        )
        .unwrap();
        std::fs::write(
            tmp.path()
                .join("snapshots")
                .join(format!("{snap}.manifest")),
            "mf",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), Some(snap));
    }

    #[test]
    fn fast_path_treats_missing_subdirs_as_empty() {
        let tmp = TempDir::new().unwrap();
        // No wal/, pending/, gc/, index/ directories yet — a brand-new
        // volume that just happens to have no snapshot. Should fall
        // through cleanly to the "no snapshot" branch.
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    // ----- await-prefetch -----

    /// Untracked volume → ok. Used for volumes already-prefetched on a previous
    /// coordinator run (no entry in the tracker yet) or running without
    /// coordinator-managed prefetch at all.
    #[tokio::test]
    async fn await_prefetch_returns_ok_for_untracked_volume() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        await_prefetch_op(vol, &tracker)
            .await
            .expect("untracked → ok");
    }

    /// Tracker already has a Done(Ok) entry → returns immediately, no blocking.
    #[tokio::test]
    async fn await_prefetch_returns_ok_when_already_done_success() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let tx = elide_coordinator::register_prefetch_or_get(&tracker, vol);
        tx.send_replace(Some(Ok(())));
        // Don't await with a timeout: if this blocks, the test blocks — that's
        // a clearer failure than a timeout.
        await_prefetch_op(vol, &tracker)
            .await
            .expect("already-done → ok");
    }

    /// Tracker has Done(Err) → surfaces the message, doesn't return ok.
    #[tokio::test]
    async fn await_prefetch_returns_err_when_already_done_failed() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let tx = elide_coordinator::register_prefetch_or_get(&tracker, vol);
        tx.send_replace(Some(Err("S3 timeout".into())));
        let err = await_prefetch_op(vol, &tracker)
            .await
            .expect_err("done(err) should surface");
        assert!(err.message.contains("S3 timeout"), "{err}");
    }

    /// In-progress entry: caller blocks; producer publishes Ok mid-wait;
    /// caller unblocks with ok.
    #[tokio::test]
    async fn await_prefetch_blocks_until_publisher_sends_ok() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let tx = elide_coordinator::register_prefetch_or_get(&tracker, vol);

        let tracker_clone = tracker.clone();
        let waiter = tokio::spawn(async move { await_prefetch_op(vol, &tracker_clone).await });

        // Give the waiter a moment to subscribe before the publisher fires.
        // Without this, the publisher might send before the waiter is even
        // running; the test would still pass via initial-borrow, so this
        // sleep specifically exercises the rx.changed() path.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send_replace(Some(Ok(())));

        waiter.await.unwrap().expect("publisher → ok");
    }

    /// In-progress entry, sender dropped (per-fork task panicked / volume
    /// removed mid-prefetch) → IPC returns a clear error rather than hanging
    /// or silently saying ok.
    #[tokio::test]
    async fn await_prefetch_returns_err_when_sender_dropped_without_value() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let tx = elide_coordinator::register_prefetch_or_get(&tracker, vol);

        let tracker_clone = tracker.clone();
        let waiter = tokio::spawn(async move { await_prefetch_op(vol, &tracker_clone).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // Simulate the per-fork task exiting: drop the task's local
        // Arc<Sender> AND remove the tracker entry (the Drop guard in
        // run_volume_tasks does this on real exit). With both gone, the
        // underlying watch channel has no more senders and the waiter
        // unblocks with Err.
        drop(tx);
        elide_coordinator::unregister_prefetch(&tracker, &vol);

        let err = waiter
            .await
            .unwrap()
            .expect_err("dropped sender should error");
        assert!(
            err.message.contains("prefetch task exited"),
            "expected 'prefetch task exited...', got: {err}"
        );
    }

    // ── generate-filemap argument validation ─────────────────────────────

    #[tokio::test]
    async fn generate_filemap_unknown_volume_returns_err() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("by_name")).unwrap();
        let store = mem_store();
        let err = generate_filemap_op("ghost", None, tmp.path(), &store)
            .await
            .expect_err("ghost volume should error");
        assert_eq!(err.kind, IpcErrorKind::NotFound);
        assert!(err.message.contains("volume not found"), "{err}");
    }

    #[tokio::test]
    async fn generate_filemap_no_snapshot_returns_err() {
        let tmp = TempDir::new().unwrap();
        let vol_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let vol_dir = tmp.path().join("by_id").join(vol_ulid);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let by_name = tmp.path().join("by_name");
        std::fs::create_dir_all(&by_name).unwrap();
        std::os::unix::fs::symlink(&vol_dir, by_name.join("vol")).unwrap();
        let store = mem_store();
        let err = generate_filemap_op("vol", None, tmp.path(), &store)
            .await
            .expect_err("no snapshot should error");
        assert_eq!(err.kind, IpcErrorKind::NotFound);
        assert!(err.message.contains("no local snapshot"), "{err}");
    }

    #[tokio::test]
    async fn generate_filemap_explicit_unknown_snapshot_returns_err() {
        let tmp = TempDir::new().unwrap();
        let vol_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let vol_dir = tmp.path().join("by_id").join(vol_ulid);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let by_name = tmp.path().join("by_name");
        std::fs::create_dir_all(&by_name).unwrap();
        std::os::unix::fs::symlink(&vol_dir, by_name.join("vol")).unwrap();
        let store = mem_store();
        // Valid ULID, but no matching snapshots/<ulid>.manifest on disk.
        let bogus = ulid::Ulid::from_string("01J0000000000000000000000V").unwrap();
        let err = generate_filemap_op("vol", Some(bogus), tmp.path(), &store)
            .await
            .expect_err("missing manifest should error");
        assert_eq!(err.kind, IpcErrorKind::NotFound);
        assert!(
            err.message.contains("not found locally") && err.message.contains(&bogus.to_string()),
            "{err}"
        );
    }

    /// Tests for [`classify_resumable_orphan`] — exercises the
    /// `for_claim` resume-detection logic now living coordinator-side
    /// (was previously a CLI-side classifier + provenance check).
    mod classify_resumable_orphan_tests {
        use super::super::classify_resumable_orphan;
        use std::os::unix::fs::symlink;
        use tempfile::TempDir;

        fn setup() -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
            let tmp = TempDir::new().unwrap();
            let by_id = tmp.path().join("by_id");
            let by_name = tmp.path().join("by_name");
            std::fs::create_dir_all(&by_id).unwrap();
            std::fs::create_dir_all(&by_name).unwrap();
            (tmp, by_id, by_name)
        }

        /// Write a fork directory whose signed `volume.provenance`
        /// records `(parent_ulid, snap_ulid)` as the parent ref.
        /// Mirrors the on-disk shape `fork_volume_at` produces.
        fn write_orphan_fork(
            dir: &std::path::Path,
            parent_ulid: ulid::Ulid,
            snap_ulid: ulid::Ulid,
        ) {
            std::fs::create_dir_all(dir).unwrap();
            let key = elide_core::signing::generate_keypair(
                dir,
                elide_core::signing::VOLUME_KEY_FILE,
                elide_core::signing::VOLUME_PUB_FILE,
            )
            .unwrap();
            let lineage = elide_core::signing::ProvenanceLineage {
                parent: Some(elide_core::signing::ParentRef {
                    volume_ulid: parent_ulid.to_string(),
                    snapshot_ulid: snap_ulid.to_string(),
                    pubkey: [0u8; 32],
                    manifest_pubkey: None,
                }),
                extent_index: Vec::new(),
            };
            elide_core::signing::write_provenance(
                dir,
                &key,
                elide_core::signing::VOLUME_PROVENANCE_FILE,
                &lineage,
            )
            .unwrap();
        }

        fn ulids() -> (ulid::Ulid, ulid::Ulid, ulid::Ulid, ulid::Ulid) {
            // Distinct, deterministic — order doesn't matter, only equality.
            let mut mint = elide_core::ulid_mint::UlidMint::new(ulid::Ulid::nil());
            (mint.next(), mint.next(), mint.next(), mint.next())
        }

        #[test]
        fn returns_none_when_symlink_absent() {
            let (_t, by_id, by_name) = setup();
            let (released, snap, _, _) = ulids();
            assert_eq!(
                classify_resumable_orphan(&by_name.join("vol"), &by_id, released, Some(snap)),
                None
            );
        }

        #[test]
        fn returns_some_when_provenance_records_expected_parent_and_snap() {
            let (_t, by_id, by_name) = setup();
            let (released, snap, orphan, _) = ulids();
            write_orphan_fork(&by_id.join(orphan.to_string()), released, snap);
            symlink(format!("../by_id/{orphan}"), by_name.join("vol")).unwrap();
            assert_eq!(
                classify_resumable_orphan(&by_name.join("vol"), &by_id, released, Some(snap)),
                Some(orphan)
            );
        }

        #[test]
        fn returns_none_when_parent_ulid_differs() {
            let (_t, by_id, by_name) = setup();
            let (released, snap, orphan, other) = ulids();
            write_orphan_fork(&by_id.join(orphan.to_string()), other, snap);
            symlink(format!("../by_id/{orphan}"), by_name.join("vol")).unwrap();
            assert_eq!(
                classify_resumable_orphan(&by_name.join("vol"), &by_id, released, Some(snap)),
                None
            );
        }

        #[test]
        fn returns_none_when_snap_differs() {
            let (_t, by_id, by_name) = setup();
            let (released, snap, orphan, other_snap) = ulids();
            write_orphan_fork(&by_id.join(orphan.to_string()), released, other_snap);
            symlink(format!("../by_id/{orphan}"), by_name.join("vol")).unwrap();
            assert_eq!(
                classify_resumable_orphan(&by_name.join("vol"), &by_id, released, Some(snap)),
                None
            );
        }

        #[test]
        fn returns_none_when_target_dir_missing() {
            let (_t, by_id, by_name) = setup();
            let (released, snap, orphan, _) = ulids();
            // Symlink points at by_id/<orphan>/ but no fork was written.
            symlink(format!("../by_id/{orphan}"), by_name.join("vol")).unwrap();
            assert_eq!(
                classify_resumable_orphan(&by_name.join("vol"), &by_id, released, Some(snap)),
                None
            );
        }

        #[test]
        fn returns_none_when_provenance_corrupted() {
            let (_t, by_id, by_name) = setup();
            let (released, snap, orphan, _) = ulids();
            let dir = by_id.join(orphan.to_string());
            write_orphan_fork(&dir, released, snap);
            // Truncate provenance to invalidate the signature.
            std::fs::write(dir.join(elide_core::signing::VOLUME_PROVENANCE_FILE), "").unwrap();
            symlink(format!("../by_id/{orphan}"), by_name.join("vol")).unwrap();
            assert_eq!(
                classify_resumable_orphan(&by_name.join("vol"), &by_id, released, Some(snap)),
                None
            );
        }

        #[test]
        fn returns_none_when_no_snap_supplied() {
            // No snap ⇒ can't match any provenance, regardless of state.
            let (_t, by_id, by_name) = setup();
            let (released, snap, orphan, _) = ulids();
            write_orphan_fork(&by_id.join(orphan.to_string()), released, snap);
            symlink(format!("../by_id/{orphan}"), by_name.join("vol")).unwrap();
            assert_eq!(
                classify_resumable_orphan(&by_name.join("vol"), &by_id, released, None),
                None
            );
        }
    }
}
