//! Volume-fetch flow: registry, orchestrator, and the start-fetch entry point.
//!
//! Mirrors the structure of [`crate::fork`] and [`crate::import`]: a
//! synchronous front-half (`start_fetch`) registers a job and spawns
//! the orchestrator task, then returns. The orchestrator does
//! name-resolution → ancestor-chain pull → manifest fetch + verify →
//! per-segment idx pull → spawn `elide fetch-volume` worker → wait
//! for exit → write `volume.fetched` marker.
//!
//! The worker is a separate process (`elide fetch-volume`, see
//! `src/fetch_volume.rs`) so the body-warming pass runs in its own
//! address space rather than inside the coordinator. Coordinator-side
//! work is metadata-only (skeleton + manifest + idx); the worker
//! handles the bulk-S3 leg.
//!
//! No peer-fetch tier: a fetched volume is foreign by definition,
//! with no `names/<volume>` rebind on this host to anchor peer auth
//! against. Mirrors `Fork::pull_chain`'s reasoning.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, RwLock};

use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::{info, warn};
use ulid::Ulid;

use crate::inbound::{CoordinatorCore, pull_readonly_op, validate_volume_name};
use elide_coordinator::ipc::{FetchAttachEvent, FetchStartReply, IpcError};
use elide_coordinator::volume_state::{FETCHED_FILE, FetchedRecord};

// ── Per-domain context ───────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct FetchContext {
    pub core: CoordinatorCore,
    pub registry: FetchRegistry,
}

// ── Job + registry ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum FetchJobState {
    Running,
    Done,
    Failed(IpcError),
}

pub struct FetchJob {
    events: Mutex<Vec<FetchAttachEvent>>,
    state: RwLock<FetchJobState>,
}

impl FetchJob {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            state: RwLock::new(FetchJobState::Running),
        })
    }

    pub fn append(&self, event: FetchAttachEvent) {
        self.events
            .lock()
            .expect("fetch job events poisoned")
            .push(event);
    }

    pub fn line(&self, content: impl Into<String>) {
        self.append(FetchAttachEvent::Line {
            content: content.into(),
        });
    }

    pub fn finish(&self, state: FetchJobState) {
        *self.state.write().expect("fetch job state poisoned") = state;
    }

    pub fn read_from(&self, offset: usize) -> Vec<FetchAttachEvent> {
        self.events.lock().expect("fetch job events poisoned")[offset..].to_vec()
    }

    pub fn state(&self) -> FetchJobState {
        self.state.read().expect("fetch job state poisoned").clone()
    }
}

pub type FetchRegistry = Arc<Mutex<HashMap<String, Arc<FetchJob>>>>;

pub fn new_registry() -> FetchRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

// ── Entry point ──────────────────────────────────────────────────────────────

/// Register a fetch job, run the synchronous front-half (resolve +
/// latest-snapshot lookup so we can fail fast with NotFound), and
/// spawn the orchestrator task.
///
/// Returns [`FetchStartReply`] with the resolved `vol_ulid`,
/// `basis_snapshot` and freshly-minted `fetch_ulid`. Subsequent
/// progress is observable via [`elide_coordinator::ipc::Request::FetchAttach`]
/// and [`elide_coordinator::ipc::Request::FetchStatus`].
pub(crate) async fn start_fetch(
    volume_name: String,
    ctx: FetchContext,
) -> Result<FetchStartReply, IpcError> {
    validate_volume_name(&volume_name).map_err(IpcError::bad_request)?;

    {
        let reg = ctx.registry.lock().expect("fetch registry poisoned");
        if let Some(job) = reg.get(&volume_name)
            && matches!(job.state(), FetchJobState::Running)
        {
            return Err(IpcError::conflict(format!(
                "fetch '{volume_name}' is already in progress"
            )));
        }
    }

    // Synchronous front-half: name resolution + latest-snapshot lookup
    // so we can fail fast (NotFound on either) without registering a
    // doomed-to-fail job. The actual body-warm work runs detached.
    let store = ctx.core.stores.coordinator_wide();
    let coord_id = ctx.core.identity.coordinator_id_str();
    let (vol_ulid, size_bytes, owned_by_us) = resolve_name(&volume_name, &store, coord_id).await?;

    // `fetch` is a *foreign* read: it pulls another coordinator's
    // volume to local readonly state so we can browse it or fork
    // from it. On a volume we already own there's nothing to fetch —
    // the indexes are local, bodies arrive on demand at first read,
    // and the post-fetch shape (`<S>.fetched` + `volume.readonly`)
    // would actively contradict the writable owned state. Refuse
    // with a hint pointing at the right verb.
    if owned_by_us {
        use elide_coordinator::volume_state::VolumeLifecycle;
        let by_name = ctx.core.data_dir.join("by_name").join(&volume_name);
        let (local_vol_dir, _shape) = VolumeLifecycle::resolve(&by_name)
            .map_err(|e| IpcError::internal(format!("resolving local fork: {e}")))?;
        let hint = if local_vol_dir.is_some() {
            format!(
                "volume '{volume_name}' is already owned by this coordinator; \
                 use `volume start {volume_name}` to bring it up, or \
                 `volume create --from {volume_name} <new-name>` to fork from it"
            )
        } else {
            format!(
                "volume '{volume_name}' is owned by this coordinator but has no \
                 local fork; use `volume start {volume_name}` to hydrate it from S3"
            )
        };
        return Err(IpcError::conflict(hint));
    }

    let basis_snapshot = match latest_snapshot_in_store(vol_ulid, &store).await? {
        Some(u) => u,
        None => {
            return Err(IpcError::not_found(format!(
                "volume '{volume_name}' (vol_ulid {vol_ulid}) has no snapshot in the store; \
                 the owner must publish a snapshot before it can be fetched"
            )));
        }
    };

    // Register the job. Using volume name as the key matches the
    // import / fork pattern: at most one fetch per name in flight.
    let job = FetchJob::new();
    {
        let mut reg = ctx.registry.lock().expect("fetch registry poisoned");
        reg.insert(volume_name.clone(), Arc::clone(&job));
    }

    let fetch_ulid = Ulid::new();

    let job_clone = Arc::clone(&job);
    let volume_name_clone = volume_name.clone();
    tokio::spawn(async move {
        let result = run_orchestrator(
            volume_name_clone.clone(),
            vol_ulid,
            basis_snapshot,
            size_bytes,
            ctx,
            job_clone.clone(),
        )
        .await;
        match result {
            Ok(()) => {
                job_clone.append(FetchAttachEvent::Done);
                job_clone.finish(FetchJobState::Done);
                info!("[fetch {volume_name_clone}] done");
            }
            Err(e) => {
                warn!("[fetch {volume_name_clone}] failed: {e:#?}");
                job_clone.finish(FetchJobState::Failed(e));
            }
        }
    });

    Ok(FetchStartReply {
        fetch_ulid,
        basis_snapshot,
        vol_ulid,
    })
}

// ── Orchestrator ─────────────────────────────────────────────────────────────

async fn run_orchestrator(
    volume_name: String,
    vol_ulid: Ulid,
    basis_snapshot: Ulid,
    size_bytes: u64,
    ctx: FetchContext,
    job: Arc<FetchJob>,
) -> Result<(), IpcError> {
    let data_dir: PathBuf = (*ctx.core.data_dir).clone();
    let by_id_dir = data_dir.join("by_id");
    let fork_dir = by_id_dir.join(vol_ulid.to_string());
    let store = ctx.core.stores.for_volume(&vol_ulid);

    // Stage 1. Pull ancestor skeleton chain (volume.pub +
    // volume.provenance per ancestor). Stops at the first ancestor
    // that's already on disk.
    pull_ancestor_chain(vol_ulid, &data_dir, &by_id_dir, &store, &job).await?;

    // Stage 2. Fetch the basis manifest if not already present, then
    // verify it under the volume's `volume.pub`. Manifest signature
    // failure aborts before any idx is fetched — we never write
    // bytes under an unverified basis.
    fetch_and_verify_manifest(vol_ulid, basis_snapshot, &fork_dir, &store, &job).await?;

    // Stage 3. Pull every `.idx` referenced by the manifest. The
    // helper is manifest-driven (only the segments the basis attests
    // to) and S3-only (no peer tier). Per-segment signature
    // verification happens on each fetch.
    let verifying_key =
        elide_core::signing::load_verifying_key(&fork_dir, elide_core::signing::VOLUME_PUB_FILE)
            .map_err(|e| IpcError::internal(format!("loading volume.pub: {e}")))?;

    let n_idx = elide_coordinator::prefetch::pull_indexes_for_snapshot(
        &store,
        &fork_dir,
        &vol_ulid.to_string(),
        basis_snapshot,
        &verifying_key,
    )
    .await
    .map_err(|e| IpcError::store(format!("pulling indexes for {basis_snapshot}: {e:#}")))?;
    job.line(format!(
        "pulled {n_idx} idx file(s) for snapshot {basis_snapshot}"
    ));

    // Stage 4. Write the local `volume.toml`. Size lives on the
    // bucket-side `names/<name>` claim record (the owner is the
    // single source of truth — see `design-volume-size-ownership`),
    // not in the per-fork dir; ancestors carry no size. We snapshot
    // the size at fetch time so consumers like `volume create --from`
    // (which reads `volume.toml.size` from the source dir to
    // initialise the new fork's size) work without a bucket
    // round-trip. Refresh-fetch overwrites this if the owner has
    // since resized.
    let cfg = elide_core::config::VolumeConfig {
        name: Some(volume_name.clone()),
        size: Some(size_bytes),
        ..Default::default()
    };
    cfg.write(&fork_dir)
        .map_err(|e| IpcError::internal(format!("writing volume.toml: {e}")))?;

    // Stage 5. Plant the by_name symlink so the fetched volume is
    // discoverable mid-warm via `volume list`. Skipped if a symlink
    // already exists pointing at this vol_ulid; an existing symlink
    // pointing elsewhere is an error (caller should `volume remove`
    // first).
    plant_by_name_symlink(&volume_name, vol_ulid, &data_dir, &job)?;

    // Stage 6. Spawn `elide fetch-volume <fork_dir>` and wait. The
    // worker opens the readonly volume, runs full_warm to
    // completion, and exits.
    job.line("warming bodies (full-warm)");
    spawn_fetch_worker(&fork_dir, &volume_name, Arc::clone(&job)).await?;

    // Stage 7. On clean exit, write the `volume.fetched` marker. Only
    // here, after every prior step succeeded, does the volume
    // formally enter the `Fetched` lifecycle state. Reaching this
    // point implies the bucket record is foreign-owned — owned-by-us
    // fetches are refused in the synchronous front-half.
    write_fetched_marker(&fork_dir, basis_snapshot, &ctx)?;
    job.line(format!(
        "fetched: marker written ({} = {basis_snapshot})",
        FETCHED_FILE
    ));

    Ok(())
}

// ── Stage helpers ────────────────────────────────────────────────────────────

async fn resolve_name(
    volume_name: &str,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
) -> Result<(Ulid, u64, bool), IpcError> {
    match elide_coordinator::name_store::read_name_record(store, volume_name).await {
        Ok(Some((rec, _))) => {
            let owned_by_us = rec.coordinator_id.as_deref() == Some(coord_id);
            Ok((rec.vol_ulid, rec.size, owned_by_us))
        }
        Ok(None) => Err(IpcError::not_found(format!(
            "volume '{volume_name}' not found in store"
        ))),
        Err(e) => Err(IpcError::store(format!("reading names/{volume_name}: {e}"))),
    }
}

/// LIST `by_id/<vol_ulid>/snapshots/` and return the highest snapshot
/// ULID, ignoring filemap and other dotted siblings. Returns `None`
/// when the volume has no snapshot in the store.
async fn latest_snapshot_in_store(
    vol_ulid: Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<Option<Ulid>, IpcError> {
    use futures::TryStreamExt;
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
        let stem = match filename.strip_suffix(".manifest") {
            Some(s) => s,
            None => continue,
        };
        if let Ok(u) = Ulid::from_string(stem)
            && latest.is_none_or(|cur| u > cur)
        {
            latest = Some(u);
        }
    }
    Ok(latest)
}

async fn pull_ancestor_chain(
    leaf_ulid: Ulid,
    data_dir: &Path,
    by_id_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    job: &FetchJob,
) -> Result<(), IpcError> {
    use elide_core::volume::resolve_ancestor_dir;

    let mut next: Option<Ulid> = Some(leaf_ulid);
    while let Some(vol_ulid) = next.take() {
        let dir = resolve_ancestor_dir(by_id_dir, &vol_ulid.to_string());
        if dir.exists() {
            break;
        }
        job.line(format!("pulling ancestor {vol_ulid}"));
        let reply = pull_readonly_op(vol_ulid, data_dir, store, None).await?;
        next = reply.parent;
    }
    Ok(())
}

async fn fetch_and_verify_manifest(
    vol_ulid: Ulid,
    snap_ulid: Ulid,
    fork_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    job: &FetchJob,
) -> Result<(), IpcError> {
    let snap_dir = fork_dir.join("snapshots");
    let filename = elide_core::signing::snapshot_manifest_filename(&snap_ulid);
    let local_path = snap_dir.join(&filename);

    if !local_path.exists() {
        job.line(format!("fetching basis manifest {snap_ulid}"));
        let key =
            elide_coordinator::upload::snapshot_manifest_key(&vol_ulid.to_string(), snap_ulid);
        let bytes = store
            .get(&key)
            .await
            .map_err(|e| IpcError::store(format!("fetching {filename} from store: {e}")))?
            .bytes()
            .await
            .map_err(|e| IpcError::store(format!("reading {filename}: {e}")))?;

        std::fs::create_dir_all(&snap_dir)
            .map_err(|e| IpcError::internal(format!("creating snapshots/: {e}")))?;
        let tmp = snap_dir.join(format!("{filename}.tmp"));
        std::fs::write(&tmp, &bytes)
            .map_err(|e| IpcError::internal(format!("writing {filename}.tmp: {e}")))?;
        std::fs::rename(&tmp, &local_path)
            .map_err(|e| IpcError::internal(format!("renaming {filename}.tmp: {e}")))?;
    }

    // Verify under the volume's own `volume.pub`. (Synthesised
    // handoff-recovery manifests would need a different verifying
    // key; v1 fetch supports only owner-signed basis snapshots.)
    let verifying_key =
        elide_core::signing::load_verifying_key(fork_dir, elide_core::signing::VOLUME_PUB_FILE)
            .map_err(|e| IpcError::internal(format!("loading volume.pub: {e}")))?;

    elide_core::signing::read_snapshot_manifest(fork_dir, &verifying_key, &snap_ulid)
        .map_err(|e| IpcError::internal(format!("verifying basis manifest {snap_ulid}: {e}")))?;
    Ok(())
}

fn plant_by_name_symlink(
    volume_name: &str,
    vol_ulid: Ulid,
    data_dir: &Path,
    job: &FetchJob,
) -> Result<(), IpcError> {
    let by_name_dir = data_dir.join("by_name");
    std::fs::create_dir_all(&by_name_dir)
        .map_err(|e| IpcError::internal(format!("creating by_name/: {e}")))?;
    let link = by_name_dir.join(volume_name);
    if link.is_symlink() || link.exists() {
        // Existing entry — verify it points at our vol_ulid. If yes,
        // idempotent. If no, refuse rather than clobber a real volume.
        let canon = std::fs::canonicalize(&link).map_err(|e| {
            IpcError::internal(format!(
                "canonicalizing existing by_name/{volume_name}: {e}"
            ))
        })?;
        let canon_ulid = canon.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if canon_ulid != vol_ulid.to_string() {
            return Err(IpcError::conflict(format!(
                "by_name/{volume_name} already exists pointing at {canon_ulid}, not {vol_ulid}; \
                 run `volume remove {volume_name}` first"
            )));
        }
        return Ok(());
    }
    std::os::unix::fs::symlink(format!("../by_id/{vol_ulid}"), &link)
        .map_err(|e| IpcError::internal(format!("creating by_name/{volume_name}: {e}")))?;
    job.line(format!("by_name/{volume_name} → by_id/{vol_ulid}"));
    Ok(())
}

async fn spawn_fetch_worker(
    fork_dir: &Path,
    volume_name: &str,
    job: Arc<FetchJob>,
) -> Result<(), IpcError> {
    let elide_bin = elide_coordinator::bins::elide_bin();
    let mut cmd = tokio::process::Command::new(elide_bin);
    cmd.arg("fetch-volume")
        .arg(fork_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Hand the worker the IPC socket path so it can call back for
    // store config (`GetStoreConfig`) and credentials
    // (`RegisterFetchWorker` → `Credentials`). The coordinator daemon
    // installs the path during `daemon::run`; the supervisor's
    // volume-daemon spawn-path sets this via `child_env`, but our
    // orchestrator doesn't go through the supervisor and the
    // coordinator process's own env is unset.
    if let Some(sock) = elide_coordinator::config::coordinator_socket_path() {
        cmd.env("ELIDE_COORDINATOR_SOCKET", sock);
    }

    // Fetch worker inherits the coordinator's session/process group:
    // a fetch is an admin task warming a cache no one is reading yet,
    // and is expected to die with the coordinator (Ctrl-C in
    // foreground; cgroup kill under systemd `KillMode=control-group`).
    // Coordinator-driven shutdown still SIGTERMs explicitly via
    // `terminate_fork_processes` for `KillMode=process` deployments.
    let mut child = cmd
        .spawn()
        .map_err(|e| IpcError::internal(format!("spawning elide fetch-volume worker: {e}")))?;

    // Plant `fetch.pid` immediately after spawn with the worker's
    // PID — the macaroon handshake (`Request::RegisterFetchWorker`)
    // checks SO_PEERCRED against this file. The guard cleans up on
    // every exit path including panic, so a leaked pidfile can
    // never authorise a process the coordinator didn't spawn. The
    // worker-side retry loop absorbs the brief window where the
    // worker dials before this write lands.
    let pid = child.id().ok_or_else(|| {
        IpcError::internal("fetch worker spawned but pid is unavailable (already exited?)")
    })?;
    let pid_path = fork_dir.join(elide_coordinator::volume_state::FETCH_PID_FILE);
    let _pid_guard = crate::pidfile::PidFileGuard::write(pid_path, pid)
        .map_err(|e| IpcError::internal(format!("writing fetch.pid: {e}")))?;

    // Pipe stderr lines into the job event log so attached subscribers
    // see worker progress. The Arc<FetchJob> outlives the task: it's
    // also held by the registry and the orchestrator's outer task.
    if let Some(stderr) = child.stderr.take() {
        let job_for_log = Arc::clone(&job);
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, BufReader};
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                job_for_log.line(format!("[worker] {line}"));
            }
        });
    }

    let status = child
        .wait()
        .await
        .map_err(|e| IpcError::internal(format!("waiting on fetch worker: {e}")))?;
    if !status.success() {
        return Err(IpcError::internal(format!(
            "fetch worker for '{volume_name}' exited with {status}"
        )));
    }
    Ok(())
}

fn write_fetched_marker(
    fork_dir: &Path,
    basis_snapshot: Ulid,
    ctx: &FetchContext,
) -> Result<(), IpcError> {
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let owner_coordinator_id = ctx.core.identity.coordinator_id_str().to_owned();
    let record = FetchedRecord {
        basis_snapshot: basis_snapshot.to_string(),
        // For owner-signed basis snapshots, leave empty — the verifier
        // is the volume's `volume.pub`, not a coordinator pubkey.
        // Recovery-style synthesised manifests would populate this
        // from the manifest's `recovering_coordinator_id`; v1 fetch
        // supports only owner-signed basis, so this stays blank.
        owner_coordinator_id: String::new(),
        fetched_at: now,
    };
    record
        .write(fork_dir)
        .map_err(|e| IpcError::internal(format!("writing volume.fetched: {e}")))?;
    let _ = owner_coordinator_id;
    Ok(())
}
