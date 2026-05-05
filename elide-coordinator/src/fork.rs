//! Fork flow: registry, orchestrator, and the start-fork entry point.
//!
//! Mirrors the structure of [`crate::claim`]:
//!
//! - [`ForkJob`] / [`ForkRegistry`] — buffered events + state for one
//!   in-flight fork. Polled by `fork-attach` IPC subscribers.
//! - [`start_fork`] — synchronous entry point invoked by the
//!   `Request::ForkStart` dispatch arm. Registers the job, spawns the
//!   orchestrator, and returns immediately.
//! - [`ForkOrchestrator`] — the four-stage pipeline (resolve-source →
//!   pull-chain → resolve-snapshot → mint-fork → surface-prefetch),
//!   each stage `&mut self` so per-job state lives on the struct rather
//!   than threading through helper-function arguments.
//!
//! Job state is in-memory: unlike imports there is no long-lived child
//! process to outlive the coordinator, so a coordinator restart simply
//! means the caller gets "no active fork" and re-runs `volume create
//! --from`. [`fork_create_op`] already handles cleaning up the kind of
//! partial `by_name/<name>` symlinks a mid-flight crash can leave behind.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use object_store::ObjectStore;
use tokio::sync::Notify;
use tracing::{info, warn};
use ulid::Ulid;

use crate::inbound::{
    CoordinatorCore, DrainingMarkerGuard, await_prefetch_op, decode_hex32, parse_transport_flags,
    pull_readonly_op, snapshot_volume, validate_volume_name, wait_for_control_sock,
};
use elide_coordinator::ipc::{
    ForceSnapshotNowReply, ForkAttachEvent, ForkCreateReply, ForkSource, ForkStartReply, IpcError,
    LatestSnapshotReply, ResolveNameReply,
};
use elide_coordinator::register_prefetch_or_get;
use elide_coordinator::volume_state::{IMPORTING_FILE, STOPPED_FILE};

// ── Per-domain context ───────────────────────────────────────────────────────

/// Coordinator state needed by the fork flow: the universal hot core
/// plus the fork-domain registries and config. Constructed via
/// [`crate::inbound::IpcContext::for_fork`].
#[derive(Clone)]
pub(crate) struct ForkContext {
    pub core: CoordinatorCore,
    pub fork_registry: ForkRegistry,
    pub prefetch_tracker: elide_coordinator::PrefetchTracker,
    pub snapshot_locks: elide_coordinator::SnapshotLockRegistry,
}

// ── Job + registry ────────────────────────────────────────────────────────────

/// Terminal state of a fork job. `Failed` carries the error that the
/// orchestrator surfaced; `attach_fork` translates it back into an
/// `Envelope::Err` for the wire.
#[derive(Clone, Debug)]
pub enum ForkJobState {
    Running,
    Done,
    Failed(IpcError),
}

/// In-memory record for one in-flight fork. The orchestrator pushes
/// `ForkAttachEvent` values into `events` as the flow progresses;
/// `attach_fork` polls and replays them to the subscriber.
pub struct ForkJob {
    /// Buffered progress events. The orchestrator only ever appends;
    /// `attach_fork` reads from a per-subscriber offset.
    events: Mutex<Vec<ForkAttachEvent>>,
    /// Current job state. The orchestrator flips it to `Done` /
    /// `Failed` exactly once at the end of the flow.
    state: RwLock<ForkJobState>,
}

impl ForkJob {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            state: RwLock::new(ForkJobState::Running),
        })
    }

    /// Append one event to the job's buffer. Cheap (single mutex lock,
    /// no I/O); safe to call from the orchestrator task.
    pub fn append(&self, event: ForkAttachEvent) {
        self.events
            .lock()
            .expect("fork job events poisoned")
            .push(event);
    }

    /// Mark the job terminal. Called once by the orchestrator.
    pub fn finish(&self, state: ForkJobState) {
        *self.state.write().expect("fork job state poisoned") = state;
    }

    /// Snapshot the events appended at or after `offset`. Used by
    /// `attach_fork` for its polling loop.
    pub fn read_from(&self, offset: usize) -> Vec<ForkAttachEvent> {
        self.events.lock().expect("fork job events poisoned")[offset..].to_vec()
    }

    pub fn state(&self) -> ForkJobState {
        self.state.read().expect("fork job state poisoned").clone()
    }
}

/// Registry of in-flight fork jobs keyed by the new fork's name. The
/// name uniquely identifies a fork in flight: [`fork_create_op`] rejects
/// a second concurrent attempt for the same `by_name/<name>` symlink, so
/// two `fork-start` calls for the same name cannot both be live.
pub type ForkRegistry = Arc<Mutex<HashMap<String, Arc<ForkJob>>>>;

pub fn new_registry() -> ForkRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

// ── Entry point ──────────────────────────────────────────────────────────────

/// Register a fork job and spawn the orchestrator task.
///
/// Returns immediately once the job is in the registry; the actual
/// chain-pull / snapshot / fork-create / prefetch flow runs in the
/// background. Errors here are synchronous validation failures
/// (duplicate name in flight, bad inputs); orchestrator errors are
/// surfaced via `attach_fork` instead.
pub(crate) fn start_fork(
    new_name: String,
    from: ForkSource,
    force_snapshot: bool,
    flags: Vec<String>,
    ctx: ForkContext,
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
        let orch = ForkOrchestrator::new(job.clone(), new_name, from, force_snapshot, flags, ctx);
        match orch.run().await {
            Ok(()) => {
                job.append(ForkAttachEvent::Done);
                job.finish(ForkJobState::Done);
            }
            Err(e) => job.finish(ForkJobState::Failed(e)),
        }
    });

    Ok(ForkStartReply::default())
}

// ── Orchestrator ─────────────────────────────────────────────────────────────

/// Source resolved in stage 1. `name` is `Some` only for `Name` sources;
/// `snap_hint` is `Some` only for `Pinned` sources.
struct ResolvedSource {
    vol_ulid: Ulid,
    name: Option<String>,
    snap_hint: Option<Ulid>,
}

/// Drive one fork job to completion.
///
/// The flow is a five-stage pipeline; each stage consumes earlier outputs
/// from `self` and writes its own. See [`Self::run`] for the linear
/// sequence.
pub(crate) struct ForkOrchestrator {
    job: Arc<ForkJob>,
    new_name: String,
    from: ForkSource,
    force_snapshot: bool,
    flags: Vec<String>,
    ctx: ForkContext,
    by_id_dir: PathBuf,

    // Stage outputs.
    source: Option<ResolvedSource>,
    /// Snapshot the new fork pins to. Set during `resolve_snapshot`. The
    /// `Option` mirrors what [`fork_create_op`] accepts; in practice every
    /// success path sets `Some(_)`.
    snap_ulid: Option<Ulid>,
    /// Hex ephemeral pubkey from `force-snapshot-now`, recorded in the
    /// new fork's provenance for force-snapshot pins. Only set when the
    /// readonly + force_snapshot branch fires.
    parent_key_hex: Option<String>,
    /// Set by `mint_fork` so `surface_prefetch` knows which volume to
    /// await.
    new_vol_ulid: Option<Ulid>,
}

impl ForkOrchestrator {
    pub(crate) fn new(
        job: Arc<ForkJob>,
        new_name: String,
        from: ForkSource,
        force_snapshot: bool,
        flags: Vec<String>,
        ctx: ForkContext,
    ) -> Self {
        let by_id_dir = ctx.core.data_dir.join("by_id");
        Self {
            job,
            new_name,
            from,
            force_snapshot,
            flags,
            ctx,
            by_id_dir,
            source: None,
            snap_ulid: None,
            parent_key_hex: None,
            new_vol_ulid: None,
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), IpcError> {
        self.resolve_source().await?;
        self.pull_chain().await?;
        self.resolve_snapshot().await?;
        self.mint_fork().await?;
        self.surface_prefetch().await;
        Ok(())
    }

    /// Stage 1. Resolve `from` to `(source_vol_ulid, source_name,
    /// snap_hint)`. For `Name` sources, look up the local symlink first
    /// and fall back to `resolve_name_op` (LIST `names/<name>` in the
    /// bucket).
    async fn resolve_source(&mut self) -> Result<(), IpcError> {
        let resolved = match &self.from {
            ForkSource::Pinned {
                vol_ulid,
                snap_ulid,
            } => ResolvedSource {
                vol_ulid: *vol_ulid,
                name: None,
                snap_hint: Some(*snap_ulid),
            },
            ForkSource::BareUlid { vol_ulid } => ResolvedSource {
                vol_ulid: *vol_ulid,
                name: None,
                snap_hint: None,
            },
            ForkSource::Name { name } => {
                let local = self.ctx.core.data_dir.join("by_name").join(name);
                if local.exists() {
                    let canon = std::fs::canonicalize(&local).map_err(|e| {
                        IpcError::internal(format!("canonicalize by_name/{name}: {e}"))
                    })?;
                    let ulid_str = canon
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or_else(|| IpcError::internal("by_name link has non-utf8 target"))?;
                    let vol_ulid = Ulid::from_string(ulid_str).map_err(|e| {
                        IpcError::internal(format!(
                            "by_name/{name} target {ulid_str:?} not a ULID: {e}"
                        ))
                    })?;
                    ResolvedSource {
                        vol_ulid,
                        name: Some(name.clone()),
                        snap_hint: None,
                    }
                } else {
                    self.job
                        .append(ForkAttachEvent::ResolvingName { name: name.clone() });
                    let store = self.ctx.core.stores.coordinator_wide();
                    let reply = resolve_name_op(name, &store).await?;
                    ResolvedSource {
                        vol_ulid: reply.vol_ulid,
                        name: Some(name.clone()),
                        snap_hint: None,
                    }
                }
            }
        };
        self.source = Some(resolved);
        Ok(())
    }

    /// Stage 2. Walk the ancestor chain, pulling each missing entry.
    /// S3-only — fork is the `volume create --from` path with no released
    /// ancestor to claim, so peer-fetch can't authenticate (no
    /// `names/<volume>` rebind to anchor against).
    async fn pull_chain(&mut self) -> Result<(), IpcError> {
        use elide_core::volume::resolve_ancestor_dir;

        let source = self
            .source
            .as_ref()
            .expect("resolve_source must run before pull_chain");
        let source_vol_ulid = source.vol_ulid;

        let mut next: Option<Ulid> = Some(source_vol_ulid);
        while let Some(vol_ulid) = next.take() {
            let dir = resolve_ancestor_dir(&self.by_id_dir, &vol_ulid.to_string());
            if dir.exists() {
                break;
            }
            self.job
                .append(ForkAttachEvent::PullingAncestor { vol_ulid });
            let store = self.ctx.core.stores.for_volume(&vol_ulid);
            let reply = pull_readonly_op(vol_ulid, &self.ctx.core.data_dir, &store, None).await?;
            next = reply.parent;
        }

        let source_ulid_str = source_vol_ulid.to_string();
        let source_dir = resolve_ancestor_dir(&self.by_id_dir, &source_ulid_str);
        if !source_dir.exists() {
            return Err(IpcError::not_found(format!(
                "source volume {source_ulid_str} not found in remote store"
            )));
        }
        if source_dir.join(IMPORTING_FILE).exists() {
            return Err(IpcError::conflict(format!(
                "source '{source_ulid_str}' is still importing; wait for import to complete"
            )));
        }
        Ok(())
    }

    /// Stage 3. Decide which snapshot the fork pins to.
    ///
    /// Resolution order:
    ///   - `Pinned` source: use the explicit `snap_hint`.
    ///   - Readonly source + `force_snapshot`: synthesise an attested
    ///     "now" snapshot via [`force_snapshot_now_op`] and record the
    ///     attestation pubkey as `parent_key_hex`.
    ///   - Readonly source: use the latest local snapshot, falling back
    ///     to [`latest_snapshot_op`] (LIST in S3).
    ///   - Writable source: take an implicit snapshot. If the source
    ///     daemon is stopped, transparently bring it up in
    ///     transport-suppressed mode for the drain, then halt and
    ///     restore `volume.stopped`.
    async fn resolve_snapshot(&mut self) -> Result<(), IpcError> {
        use elide_core::volume::resolve_ancestor_dir;

        let source = self
            .source
            .as_ref()
            .expect("resolve_source must run before resolve_snapshot");
        let source_vol_ulid = source.vol_ulid;
        let source_ulid_str = source_vol_ulid.to_string();
        let source_dir = resolve_ancestor_dir(&self.by_id_dir, &source_ulid_str);

        if let Some(snap) = source.snap_hint {
            // Pinned source already names the snapshot.
            self.snap_ulid = Some(snap);
            return Ok(());
        }

        if source_dir.join("volume.readonly").exists() {
            let snap_ulid = if self.force_snapshot {
                let store = self.ctx.core.stores.coordinator_wide();
                let reply =
                    force_snapshot_now_op(source_vol_ulid, &self.ctx.core.data_dir, &store).await?;
                self.parent_key_hex = Some(reply.attestation_pubkey_hex.clone());
                self.job.append(ForkAttachEvent::AttestedSnapshot {
                    snap_ulid: reply.snap_ulid,
                    pubkey_hex: reply.attestation_pubkey_hex,
                });
                reply.snap_ulid
            } else if let Some(snap) = elide_core::volume::latest_snapshot(&source_dir)
                .map_err(|e| IpcError::internal(format!("reading local snapshots: {e}")))?
            {
                snap
            } else {
                let store = self.ctx.core.stores.for_volume(&source_vol_ulid);
                match latest_snapshot_op(source_vol_ulid, &store)
                    .await?
                    .snapshot_ulid
                {
                    Some(snap) => snap,
                    None => {
                        return Err(IpcError::not_found(format!(
                            "source volume {source_ulid_str} has no snapshots; pass \
                             force_snapshot=true to upload a new 'now' marker"
                        )));
                    }
                }
            };
            self.snap_ulid = Some(snap_ulid);
            return Ok(());
        }

        // Writable source: take an implicit snapshot. Need the source's
        // name to drive `snapshot_volume`; for `BareUlid` we read it out
        // of `volume.toml`.
        let name = if let Some(n) = source.name.clone() {
            n
        } else {
            elide_core::config::VolumeConfig::read(&source_dir)
                .map_err(|e| IpcError::internal(format!("read volume.toml: {e}")))?
                .name
                .ok_or_else(|| IpcError::internal("source volume has no name in volume.toml"))?
        };

        // If the source is stopped, transparently bring its daemon up in
        // transport-suppressed mode so we can drive the implicit snapshot.
        // The `DrainingMarkerGuard` re-writes `volume.stopped` and shuts
        // the daemon down on any failure path; the success path defuses
        // the guard and halts explicitly so the volume returns to its
        // pre-fork state.
        let was_stopped = source_dir.join(STOPPED_FILE).exists();
        let mut draining_guard: Option<DrainingMarkerGuard> = None;
        if was_stopped {
            std::fs::write(source_dir.join("volume.draining"), "")
                .map_err(|e| IpcError::internal(format!("writing volume.draining: {e}")))?;
            if let Err(e) = std::fs::remove_file(source_dir.join(STOPPED_FILE)) {
                let _ = std::fs::remove_file(source_dir.join("volume.draining"));
                return Err(IpcError::internal(format!(
                    "clearing volume.stopped for fork drain: {e}"
                )));
            }
            self.ctx.core.rescan.notify_one();
            draining_guard = Some(DrainingMarkerGuard::new(source_dir.clone()));
            if !wait_for_control_sock(&source_dir, std::time::Duration::from_secs(30)).await {
                return Err(IpcError::internal(format!(
                    "timed out waiting for source volume '{name}' to come up for fork drain"
                )));
            }
        }

        let reply = snapshot_volume(&name, &self.ctx.core, &self.ctx.snapshot_locks).await?;
        self.job.append(ForkAttachEvent::SnapshotTaken {
            snap_ulid: reply.snap_ulid,
        });

        // Snapshot succeeded: restore the source to `Stopped` if we
        // brought it up. Defuse the guard so it doesn't fight the
        // explicit halt; write the marker before shutdown so the
        // supervisor won't respawn between exit and our final state.
        if was_stopped {
            if let Some(g) = draining_guard.as_mut() {
                g.defuse();
            }
            std::fs::write(source_dir.join(STOPPED_FILE), "").map_err(|e| {
                IpcError::internal(format!("restoring volume.stopped after fork drain: {e}"))
            })?;
            use elide_coordinator::control::ShutdownOutcome;
            match elide_coordinator::control::shutdown(&source_dir).await {
                ShutdownOutcome::Acknowledged | ShutdownOutcome::NotRunning => {}
                ShutdownOutcome::Failed(msg) => {
                    warn!(
                        "[fork {name}] post-drain shutdown of source daemon failed: {msg}; \
                         supervisor will respect volume.stopped marker"
                    );
                }
            }
        }
        self.snap_ulid = Some(reply.snap_ulid);
        Ok(())
    }

    /// Stage 4. Mint the fork.
    ///
    /// For a `Name` source the orchestrator already knows the user-facing
    /// name; pass it as `source_name_hint` so a pulled source (whose
    /// `volume.toml` lacks `name`) still produces a `ForkedFrom` journal
    /// event instead of falling back to `Created`. ULID-only sources
    /// (`BareUlid` / `Pinned`) have no orchestrator-known name and rely
    /// on `src_cfg.name`.
    async fn mint_fork(&mut self) -> Result<(), IpcError> {
        let source = self
            .source
            .as_ref()
            .expect("resolve_source must run before mint_fork");
        let source_vol_ulid = source.vol_ulid;
        let source_name = source.name.clone();
        let snap_ulid = self.snap_ulid;

        self.job.append(ForkAttachEvent::ForkingFrom {
            source_vol_ulid,
            snap_ulid,
        });
        let store = self.ctx.core.stores.coordinator_wide();
        let reply = fork_create_op(
            &self.new_name,
            source_vol_ulid,
            snap_ulid,
            self.parent_key_hex.as_deref(),
            &self.flags,
            source_name.as_deref(),
            &store,
            &self.ctx,
        )
        .await?;
        self.new_vol_ulid = Some(reply.new_vol_ulid);
        self.job.append(ForkAttachEvent::ForkCreated {
            new_vol_ulid: reply.new_vol_ulid,
        });
        Ok(())
    }

    /// Stage 5. Surface the coordinator's background prefetch. The fork
    /// is already durable; prefetch failure here is non-fatal — the
    /// volume opens regardless and `volume start` re-awaits.
    async fn surface_prefetch(&self) {
        let new_vol_ulid = self
            .new_vol_ulid
            .expect("mint_fork must run before surface_prefetch");
        self.job.append(ForkAttachEvent::PrefetchStarted);
        let _ = await_prefetch_op(new_vol_ulid, &self.ctx.prefetch_tracker).await;
        self.job.append(ForkAttachEvent::PrefetchDone);
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

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
    vol_ulid: Ulid,
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

    let mut latest: Option<Ulid> = None;
    for obj in objects {
        let Some(filename) = obj.location.filename() else {
            continue;
        };
        if filename.contains('.') {
            continue;
        }
        if let Ok(u) = Ulid::from_string(filename)
            && latest.is_none_or(|cur| u > cur)
        {
            latest = Some(u);
        }
    }
    Ok(LatestSnapshotReply {
        snapshot_ulid: latest,
    })
}

/// Synthesize a "now" snapshot for a readonly source volume.
///
/// Mirrors the CLI's `create_readonly_snapshot_now`: prefetches indexes,
/// uploads the snapshot marker, verifies pinned segments are still in
/// S3, and writes a signed manifest under an ephemeral key in the
/// ancestor's directory. Returns `<snap_ulid> <ephemeral_pubkey_hex>`.
async fn force_snapshot_now_op(
    vol_ulid: Ulid,
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

    // Step 1: pull every .idx visible in S3 for this volume into local
    // index/. We deliberately use the LIST-based path here rather than
    // `prefetch_indexes` — the whole point of `force_snapshot_now` is to
    // capture *pre-snapshot* bucket state (segments published since the
    // last manifest, or where no manifest exists yet), so anchoring on a
    // manifest would defeat the operation. The retention/ filter is
    // preserved so we don't round-trip GC-superseded inputs.
    //
    // No peer-fetch tier on the IPC pull-readonly path: this code runs
    // for ad-hoc readonly hydration (e.g. CLI inspect of a peer's
    // snapshot), not for the per-volume claim flow that owns the
    // peer-fetch context.
    let vk = elide_core::signing::load_verifying_key(
        &ancestor_dir,
        elide_core::signing::VOLUME_PUB_FILE,
    )
    .map_err(|e| IpcError::internal(format!("loading volume.pub: {e}")))?;
    elide_coordinator::prefetch::pull_indexes_via_list(store, &ancestor_dir, &volume_id, &vk, None)
        .await
        .map_err(|e| IpcError::store(format!("pulling indexes via list: {e:#}")))?;

    // Step 2: enumerate prefetched .idx files. Pin ULID = max(segments).
    let index_dir = ancestor_dir.join("index");
    let mut segments: Vec<Ulid> = Vec::new();
    let mut max_seg: Option<Ulid> = None;
    let entries = std::fs::read_dir(&index_dir)
        .map_err(|e| IpcError::internal(format!("reading index dir: {e}")))?;
    for entry in entries {
        let entry = entry.map_err(|e| IpcError::internal(format!("reading index entry: {e}")))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".idx") else {
            continue;
        };
        if let Ok(u) = Ulid::from_string(stem) {
            segments.push(u);
            if max_seg.is_none_or(|m| u > m) {
                max_seg = Some(u);
            }
        }
    }
    // `Ulid::default()` is the nil ULID, not a fresh one — explicitly mint.
    #[allow(clippy::unwrap_or_default)]
    let snap = max_seg.unwrap_or_else(Ulid::new);
    let snap_str = snap.to_string();

    // Step 3: verify pinned segments still present in S3 before
    // committing to a manifest. We list rather than HEAD-each because a
    // single LIST is cheaper than N HEADs once there's more than a
    // handful of pins.
    let seg_prefix = StorePath::from(format!("by_id/{volume_id}/segments/"));
    let pinned: std::collections::HashSet<Ulid> = segments.iter().copied().collect();
    let objects = store
        .list(Some(&seg_prefix))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| IpcError::store(format!("listing segments for verification: {e}")))?;
    let present: std::collections::HashSet<Ulid> = objects
        .iter()
        .filter_map(|o| o.location.filename())
        .filter_map(|name| Ulid::from_string(name).ok())
        .collect();
    let mut missing: Vec<Ulid> = pinned.difference(&present).copied().collect();
    if !missing.is_empty() {
        missing.sort();
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

    // Step 4: load-or-create the per-source attestation key.
    let (signer, vk) = elide_core::signing::load_or_create_keypair(
        &ancestor_dir,
        elide_core::signing::FORCE_SNAPSHOT_KEY_FILE,
        elide_core::signing::FORCE_SNAPSHOT_PUB_FILE,
    )
    .map_err(|e| IpcError::internal(format!("attestation keypair: {e}")))?;

    // Step 5: sign + write manifest locally.
    elide_core::signing::write_snapshot_manifest(&ancestor_dir, &*signer, &snap, &segments, None)
        .map_err(|e| IpcError::internal(format!("writing snapshot manifest: {e}")))?;

    // Step 6: upload the manifest to S3. The manifest's S3 presence is
    // the snapshot's S3 visibility — no separate marker.
    let manifest_path = ancestor_dir
        .join("snapshots")
        .join(format!("{snap_str}.manifest"));
    let manifest_bytes = std::fs::read(&manifest_path)
        .map_err(|e| IpcError::internal(format!("reading just-written manifest: {e}")))?;
    let manifest_key = elide_coordinator::upload::snapshot_manifest_key(&volume_id, snap);
    store
        .put(&manifest_key, PutPayload::from(manifest_bytes))
        .await
        .map_err(|e| IpcError::store(format!("uploading snapshot manifest: {e}")))?;

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
/// Mirrors the CLI's `fork_volume_at*` + by_name symlink + volume.toml
/// write. `source_vol_ulid` resolves to any volume in `by_id/<ulid>/` —
/// writable, imported readonly base, or pulled ancestor. `snap` is
/// optional: if omitted, falls back to `volume::fork_volume` (latest
/// local snapshot). `parent_key_hex` is the hex ephemeral pubkey from
/// `force-snapshot-now`, recorded in the new fork's provenance for
/// force-snapshot pins. `source_name_hint` is the orchestrator-supplied
/// source name, used as a fallback when the source's on-disk
/// `volume.toml` has no `name` field — typically because the source was
/// pulled from S3 (`pull.rs` writes `name: None` for pulled ancestors).
/// Without this hint, forks from a pulled source emit a `Created`
/// journal event instead of the more useful
/// `ForkedFrom { source_name, ... }`.
#[allow(clippy::too_many_arguments)]
async fn fork_create_op(
    new_name: &str,
    source_vol_ulid: Ulid,
    snap: Option<Ulid>,
    parent_key_hex: Option<&str>,
    flags: &[String],
    source_name_hint: Option<&str>,
    store: &Arc<dyn ObjectStore>,
    ctx: &ForkContext,
) -> Result<ForkCreateReply, IpcError> {
    let identity = &ctx.core.identity;
    let data_dir: &Path = &ctx.core.data_dir;
    let rescan: &Notify = &ctx.core.rescan;
    let prefetch_tracker = &ctx.prefetch_tracker;
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
    if symlink_path.exists() {
        return Err(IpcError::conflict(format!(
            "volume already exists: {new_name}"
        )));
    }

    let new_vol_ulid_value = Ulid::new();
    let new_vol_ulid = new_vol_ulid_value.to_string();
    let new_fork_dir = by_id_dir.join(&new_vol_ulid);

    // Local rollback helpers. `cleanup` undoes the on-disk fork dir +
    // symlink; `rollback_claim` deletes `names/<name>` from the bucket
    // and is only called after `mark_initial` has succeeded, so it
    // always actually rolls back.
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
        (Some(snap), None) => elide_core::volume::fork_volume_at(&new_fork_dir, &source_dir, snap),
        (None, _) => elide_core::volume::fork_volume(&new_fork_dir, &source_dir),
    };
    if let Err(e) = fork_result {
        cleanup(&new_fork_dir, &symlink_path);
        return Err(IpcError::internal(format!("fork failed: {e}")));
    }

    // Phase 2: publish volume.pub *and* volume.provenance to S3 *before*
    // claiming the name. Both files are immutable from fork creation
    // onward and self-signed by the new fork's keypair. A SIGINT here
    // at worst leaves orphan
    // `by_id/<new_vol_ulid>/{volume.pub, volume.provenance}` (no
    // names/<name> references them, so they're harmless and reclaimable
    // by future GC). Without this ordering, a crash between
    // mark_initial and the daemon's first metadata-drain leaves
    // `names/<name>` pointing at a vol_ulid whose immutable trust
    // artefacts are missing — which breaks both the normal claim path
    // and the peer-fetch auth pipeline (lineage walk 404s on
    // volume.provenance).
    if let Err(e) =
        elide_coordinator::upload::upload_volume_pub_initial(&new_fork_dir, &new_vol_ulid, store)
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

    // Read source volume config for `src_cfg.name` (feeds the
    // `ForkedFrom` journal entry). Done before the bucket claim so a
    // malformed source fails cleanly without leaving a half-claimed
    // name. Size for the new fork's local `volume.toml` comes from the
    // source's cached `volume.toml.size` — the source is always a live
    // volume on this host, so its config is authoritative.
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

    // Snap actually used for the fork. `snap.is_some()` matches the
    // explicit-pin call sites; otherwise `fork_volume` above resolved
    // `latest_snapshot(&source_dir)` internally and we recompute it
    // here so the journal records the same value. Resolution failure
    // here only suppresses the `ForkedFrom` event; the lifecycle
    // proceeds with `Created` as a fallback.
    let resolved_snap = snap.or_else(|| {
        elide_core::volume::latest_snapshot(&source_dir)
            .ok()
            .flatten()
    });

    // Phase 4 prep: write the local volume.toml alongside the symlink
    // creation below. Doing it here keeps the cleanup semantics
    // symmetric with the upload failures above.
    if let Err(e) = (elide_core::config::VolumeConfig {
        name: Some(new_name.to_owned()),
        size: Some(size),
        nbd: nbd_cfg,
        ublk: ublk_cfg,
        lazy: None,
    }
    .write(&new_fork_dir))
    {
        cleanup(&new_fork_dir, &symlink_path);
        return Err(IpcError::internal(format!("writing volume config: {e}")));
    }

    let src_name = src_cfg.name.or_else(|| source_name_hint.map(str::to_owned));

    // Phase 3: claim `names/<name>` in S3.
    use elide_coordinator::lifecycle::{LifecycleError, MarkInitialOutcome, mark_initial};
    match mark_initial(
        store,
        new_name,
        coord_id,
        identity.hostname(),
        new_vol_ulid_value,
        size,
    )
    .await
    {
        Ok(MarkInitialOutcome::Claimed) => {
            // `volume create --from` mints a fork, so the opening
            // journal entry on the new name is `ForkedFrom` (not
            // `Created`). Falls back to `Created` only when fork context
            // cannot be reconstructed — typically a ULID-only ancestor
            // with no `name` in its `volume.toml`. Both the source name
            // and snap have to be present; a partial `ForkedFrom` would
            // publish a less useful record than just stating "this name
            // appeared".
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

    // Phase 4: by_name/<name> symlink. volume.toml was written above
    // before mark_initial, so a Phase-3 failure didn't leave a
    // half-written config.
    if let Err(e) = std::fs::create_dir_all(&by_name_dir) {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return Err(IpcError::internal(format!("creating by_name dir: {e}")));
    }
    if let Err(e) = std::os::unix::fs::symlink(format!("../by_id/{new_vol_ulid}"), &symlink_path) {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return Err(IpcError::internal(format!("creating by_name symlink: {e}")));
    }

    // Pre-register the prefetch tracker entry before notifying the
    // daemon's discovery loop. This closes the race where the CLI's
    // `await-prefetch <new_vol_ulid>` (called immediately after this IPC
    // returns) could land before the daemon has discovered the new fork
    // and registered the entry — in which case `await-prefetch` would
    // hit the "untracked → ok" path and falsely report prefetch
    // complete. `register_prefetch_or_get` is idempotent: when discovery
    // later runs, it gets back the same `Arc<Sender>` and passes it to
    // `run_volume_tasks`. Drop the local Arc immediately; the tracker
    // holds the entry until the per-fork task's Drop guard removes it.
    let _ = register_prefetch_or_get(prefetch_tracker, new_vol_ulid_value);

    rescan.notify_one();
    info!("[inbound] forked volume {new_name} ({new_vol_ulid}) from {source_ulid_str}");
    Ok(ForkCreateReply {
        new_vol_ulid: new_vol_ulid_value,
    })
}
