//! Hydrate a volume whose name we still own remotely but whose local
//! fork has been removed. Called from [`crate::inbound::start_volume_op`]
//! when `by_name/<name>` is absent and the bucket `names/<name>` record
//! is `Live`/`Stopped` and owned by this coordinator.
//!
//! The shape mirrors [`crate::fetch`] stages 1–5 (skeleton → manifest →
//! idx → `volume.toml` → `by_name` symlink) plus the daemon-expected
//! `wal/` and `pending/` and a `volume.stopped` marker, so the existing
//! local-resume path in `start_volume_op` can run unchanged after this
//! returns. Body warm is *not* run — bodies remain demand-fetched, the
//! whole point of starting fast.

use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::{info, warn};
use ulid::Ulid;

use crate::inbound::{CoordinatorCore, pull_readonly_op};
use elide_coordinator::ipc::IpcError;
use elide_coordinator::volume_state::STOPPED_FILE;

/// Fully metadata-hydrate a remote-owned volume so the existing
/// [`crate::inbound::start_volume_op`] flow can resume it as `Stopped`.
///
/// On return: `by_name/<volume_name>` is a symlink to the freshly-
/// hydrated `by_id/<vol_ulid>/`, which has `volume.{pub,provenance}`,
/// `snapshots/<basis>.manifest`, `index/*.idx`, `volume.toml`, `wal/`,
/// `pending/`, and `volume.stopped`. The `data_dir/remote/<volume_name>`
/// breadcrumb is cleared.
pub(crate) async fn hydrate_remote_owned(
    volume_name: &str,
    vol_ulid: Ulid,
    size_bytes: u64,
    store: &Arc<dyn ObjectStore>,
    core: &CoordinatorCore,
) -> Result<(), IpcError> {
    let started = std::time::Instant::now();
    let by_id_dir = core.data_dir.join("by_id");
    let fork_dir = by_id_dir.join(vol_ulid.to_string());

    pull_skeleton_chain(vol_ulid, &core.data_dir, &by_id_dir, store).await?;

    let (basis_snapshot, basis_kind) = match latest_snapshot_in_store(vol_ulid, store).await? {
        Some(pair) => pair,
        None => {
            return Err(IpcError::not_found(format!(
                "volume '{volume_name}' has no published snapshot in the store; \
                 cannot hydrate without a basis manifest"
            )));
        }
    };

    fetch_and_verify_manifest(vol_ulid, basis_snapshot, basis_kind, &fork_dir, store).await?;

    let verifying_key =
        elide_core::signing::load_verifying_key(&fork_dir, elide_core::signing::VOLUME_PUB_FILE)
            .map_err(|e| IpcError::internal(format!("loading volume.pub: {e}")))?;
    elide_coordinator::prefetch::pull_indexes_for_snapshot(
        store,
        &fork_dir,
        &vol_ulid.to_string(),
        basis_snapshot,
        &verifying_key,
    )
    .await
    .map_err(|e| IpcError::store(format!("pulling indexes for {basis_snapshot}: {e:#}")))?;

    elide_core::config::VolumeConfig {
        name: Some(volume_name.to_owned()),
        size: Some(size_bytes),
        ..Default::default()
    }
    .write(&fork_dir)
    .map_err(|e| IpcError::internal(format!("writing volume.toml: {e}")))?;

    std::fs::create_dir_all(fork_dir.join("wal"))
        .map_err(|e| IpcError::internal(format!("creating wal/: {e}")))?;
    std::fs::create_dir_all(fork_dir.join("pending"))
        .map_err(|e| IpcError::internal(format!("creating pending/: {e}")))?;
    std::fs::write(fork_dir.join(STOPPED_FILE), "")
        .map_err(|e| IpcError::internal(format!("writing volume.stopped: {e}")))?;

    plant_by_name_symlink(volume_name, vol_ulid, &core.data_dir)?;

    if let Err(e) = elide_coordinator::remote_breadcrumb::remove(&core.data_dir, volume_name) {
        warn!("[inbound] start {volume_name}: clearing remote breadcrumb: {e}");
    }

    info!(
        "[inbound] start {volume_name}: hydrated remote-owned volume \
         (vol {vol_ulid}, basis {basis_snapshot}, {:.2?})",
        started.elapsed()
    );
    Ok(())
}

/// Walk the parent chain rooted at `leaf_ulid`, calling [`pull_readonly_op`]
/// on each ancestor not already on disk. Stops at the first ancestor
/// already pulled.
async fn pull_skeleton_chain(
    leaf_ulid: Ulid,
    data_dir: &Path,
    by_id_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), IpcError> {
    let mut next: Option<Ulid> = Some(leaf_ulid);
    while let Some(u) = next.take() {
        if by_id_dir.join(u.to_string()).exists() {
            break;
        }
        let reply = pull_readonly_op(u, data_dir, store, None).await?;
        next = reply.parent;
    }
    Ok(())
}

async fn latest_snapshot_in_store(
    vol_ulid: Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<Option<(Ulid, elide_core::signing::SnapshotKind)>, IpcError> {
    use futures::TryStreamExt;
    let prefix = StorePath::from(format!("by_id/{vol_ulid}/snapshots/"));
    let objects: Vec<object_store::ObjectMeta> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(|e| IpcError::store(format!("listing by_id/{vol_ulid}/snapshots/: {e}")))?;

    let mut latest: Option<(Ulid, elide_core::signing::SnapshotKind)> = None;
    for obj in objects {
        let Some(filename) = obj.location.filename() else {
            continue;
        };
        let Some((u, kind)) = elide_core::signing::parse_snapshot_filename(filename) else {
            continue;
        };
        if latest.is_none_or(|(cur, _)| u > cur) {
            latest = Some((u, kind));
        }
    }
    Ok(latest)
}

async fn fetch_and_verify_manifest(
    vol_ulid: Ulid,
    snap_ulid: Ulid,
    kind: elide_core::signing::SnapshotKind,
    fork_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), IpcError> {
    let snap_dir = fork_dir.join("snapshots");
    let filename = match kind {
        elide_core::signing::SnapshotKind::User => {
            elide_core::signing::snapshot_manifest_filename(&snap_ulid)
        }
        elide_core::signing::SnapshotKind::Auto => {
            elide_core::signing::auto_snapshot_manifest_filename(&snap_ulid)
        }
    };
    let local_path = snap_dir.join(&filename);

    if !local_path.exists() {
        let key = match kind {
            elide_core::signing::SnapshotKind::User => {
                elide_coordinator::upload::snapshot_manifest_key(&vol_ulid.to_string(), snap_ulid)
            }
            elide_core::signing::SnapshotKind::Auto => {
                elide_coordinator::upload::auto_snapshot_manifest_key(
                    &vol_ulid.to_string(),
                    snap_ulid,
                )
            }
        };
        let bytes = store
            .get(&key)
            .await
            .map_err(|e| IpcError::store(format!("fetching {filename}: {e}")))?
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

    let verifying_key =
        elide_core::signing::load_verifying_key(fork_dir, elide_core::signing::VOLUME_PUB_FILE)
            .map_err(|e| IpcError::internal(format!("loading volume.pub: {e}")))?;
    // Verify against the bytes we just have on disk regardless of
    // filename — `read_snapshot_manifest` defaults to `<ulid>.manifest`,
    // so we read+verify from bytes for the auto variant.
    let bytes = std::fs::read(&local_path)
        .map_err(|e| IpcError::internal(format!("reading {filename}: {e}")))?;
    elide_core::signing::read_snapshot_manifest_from_bytes(&bytes, &verifying_key, &snap_ulid)
        .map_err(|e| IpcError::internal(format!("verifying basis manifest {snap_ulid}: {e}")))?;
    Ok(())
}

fn plant_by_name_symlink(
    volume_name: &str,
    vol_ulid: Ulid,
    data_dir: &Path,
) -> Result<(), IpcError> {
    let by_name_dir = data_dir.join("by_name");
    std::fs::create_dir_all(&by_name_dir)
        .map_err(|e| IpcError::internal(format!("creating by_name/: {e}")))?;
    let link = by_name_dir.join(volume_name);
    if link.is_symlink() || link.exists() {
        let canon = std::fs::canonicalize(&link).map_err(|e| {
            IpcError::internal(format!(
                "canonicalizing existing by_name/{volume_name}: {e}"
            ))
        })?;
        let canon_ulid = canon.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if canon_ulid != vol_ulid.to_string() {
            return Err(IpcError::conflict(format!(
                "by_name/{volume_name} already exists pointing at {canon_ulid}, not {vol_ulid}"
            )));
        }
        return Ok(());
    }
    std::os::unix::fs::symlink(format!("../by_id/{vol_ulid}"), &link)
        .map_err(|e| IpcError::internal(format!("creating by_name/{volume_name}: {e}")))?;
    Ok(())
}
