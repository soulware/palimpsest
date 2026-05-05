// Single-volume coordinator tasks: drain pending segments to S3, run GC,
// and prefetch indexes on startup.
//
// Spawned once per volume by the coordinator daemon.
//
// Per-tick sequencing:
//   1. Upload pending/ segments to S3 (drain_pending)
//   2. If gc_interval has elapsed: apply done handoffs, run GC pass
//
// Drain and GC are sequential within each tick so that GC sees all segments
// uploaded this tick, and concurrent GC rewrites can never race an upload.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::PrefetchState;

use crate::config::GcConfig;
use crate::gc_cycle::{GcCycleOrchestrator, TickOutcome};
use crate::prefetch;
use crate::upload;
use crate::{PrefetchTracker, SnapshotLockRegistry, unregister_prefetch};

/// Request type for the per-fork evict channel.
/// The sender receives the eviction result (count of bodies deleted).
pub type EvictReply = oneshot::Sender<io::Result<usize>>;

/// Evict all S3-confirmed segment bodies from `cache/` for the given fork.
///
/// Called from within the fork task (between drain/GC ticks) so it never
/// races with the GC pass's collect_stats → compact_segments window.
fn evict_bodies(fork_dir: &Path) -> io::Result<usize> {
    let cache_dir = fork_dir.join("cache");
    let index_dir = fork_dir.join("index");
    let mut count = 0;
    let entries = match std::fs::read_dir(&cache_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let Some(ulid_str) = name_str.strip_suffix(".body") else {
            continue;
        };
        if !index_dir.join(format!("{ulid_str}.idx")).exists() {
            continue; // not yet S3-confirmed; skip
        }
        let _ = std::fs::remove_file(entry.path());
        let _ = std::fs::remove_file(cache_dir.join(format!("{ulid_str}.present")));
        count += 1;
    }
    Ok(count)
}

/// Evict a single S3-confirmed segment body from `cache/`.
fn evict_one_body(fork_dir: &Path, ulid_str: &str) -> io::Result<usize> {
    let idx_path = fork_dir.join("index").join(format!("{ulid_str}.idx"));
    if !idx_path.try_exists()? {
        return Err(io::Error::other(format!(
            "segment {ulid_str} has no index/{ulid_str}.idx — not S3-confirmed"
        )));
    }
    let body_path = fork_dir.join("cache").join(format!("{ulid_str}.body"));
    if !body_path.try_exists()? {
        return Err(io::Error::other(format!(
            "segment {ulid_str} not found in cache/"
        )));
    }
    std::fs::remove_file(&body_path)?;
    let _ = std::fs::remove_file(fork_dir.join("cache").join(format!("{ulid_str}.present")));
    Ok(1)
}

/// Run the drain + GC loop for a single volume directory.
///
/// `evict_rx` receives eviction requests from the coordinator inbound handler.
/// Evictions are processed between ticks so they never race with the GC pass.
///
/// Exits when the volume directory is removed. Intended to be spawned as a
/// tokio task.
/// Peer-fetch handle passed into [`run_volume_tasks`] when the
/// coordinator has peer-fetch configured. `client` mints tokens via
/// the coordinator's identity and pools HTTP/2 connections; the
/// per-volume task uses it to construct a [`prefetch::PeerFetchContext`]
/// after running the discovery hook.
#[derive(Clone)]
pub struct PeerFetchHandle {
    pub client: elide_peer_fetch::PeerFetchClient,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_volume_tasks(
    fork_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    drain_interval: Duration,
    gc_config: GcConfig,
    mut evict_rx: mpsc::Receiver<(Option<String>, EvictReply)>,
    snapshot_locks: SnapshotLockRegistry,
    prefetch_done: Arc<watch::Sender<PrefetchState>>,
    prefetch_tracker: PrefetchTracker,
    peer_fetch: Option<PeerFetchHandle>,
) {
    // Drop guard: when this task exits (clean break, await-point cancellation
    // on JoinSet abort, or panic) the tracker entry is removed. Combined with
    // `prefetch_done` (the task's own `Arc<Sender>`) being dropped at function
    // exit, this leaves the underlying watch channel with no senders, so any
    // pending `await-prefetch` subscriber sees `changed() -> Err` rather than
    // hanging forever. Declared *before* the volume_id parse so the cleanup
    // also runs on the early-return error path below.
    struct PrefetchEntryGuard {
        tracker: PrefetchTracker,
        vol_ulid: Option<ulid::Ulid>,
    }
    impl Drop for PrefetchEntryGuard {
        fn drop(&mut self) {
            if let Some(u) = self.vol_ulid {
                unregister_prefetch(&self.tracker, &u);
            }
        }
    }
    let parsed_ulid = fork_dir
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|s| ulid::Ulid::from_string(s).ok());
    let _prefetch_guard = PrefetchEntryGuard {
        tracker: prefetch_tracker,
        vol_ulid: parsed_ulid,
    };

    let volume_id = match upload::derive_names(&fork_dir) {
        Ok(id) => id,
        Err(e) => {
            warn!(
                "[coordinator] cannot derive volume id for {}: {e}",
                fork_dir.display()
            );
            // Publish the error so any waiting `await-prefetch` IPC unblocks
            // with a concrete reason rather than timing out. `send_replace`
            // never fails when there are no live subscribers — the value
            // stays in the channel for late subscribers.
            prefetch_done.send_replace(Some(Err(format!("derive volume id: {e}"))));
            return;
        }
    };
    let volume_name = read_volume_name(&fork_dir)
        .map(|n| format!(" ({n})"))
        .unwrap_or_default();

    // Prefetch segment indexes on startup if the fork has no local index files.
    // This covers the common case of a volume pulled from the store with only
    // the directory skeleton — without .idx files Volume::open cannot rebuild
    // the LBA map and the volume is unreadable.
    let has_local_segments = fork_dir.join("index").exists()
        && fork_dir
            .join("index")
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
    // Pulled ancestors are populated by the descendant writable fork's
    // `prefetch_indexes` chain walk; running their own per-volume prefetch
    // repeats every LIST/range-GET on the chain.
    if !has_local_segments && !is_pulled_ancestor(&fork_dir) {
        // Publish prefetch progress to anyone waiting on `await-prefetch`.
        // The send target is the watch channel in `PrefetchTracker`; clones
        // already subscribed will see this transition from `None` → `Some`.
        // Peer-fetch context: populated only if the volume just claimed
        // has a clean Released predecessor with a published peer-endpoint.
        // Discovery is best-effort — every failure path here collapses
        // to `None` and prefetch falls through to S3 cleanly.
        let peer_ctx = match (peer_fetch.as_ref(), read_volume_name(&fork_dir)) {
            (Some(handle), Some(volume_name)) => {
                crate::peer_discovery::discover_peer_for_claim(&store, &volume_name)
                    .await
                    .map(|discovered| prefetch::PeerFetchContext {
                        client: handle.client.clone(),
                        endpoint: discovered.endpoint,
                        volume_name,
                    })
            }
            _ => None,
        };
        let prefetch_result =
            prefetch::prefetch_indexes(&fork_dir, &store, peer_ctx.as_ref()).await;
        match &prefetch_result {
            Ok(r) if r.fetched > 0 || r.snapshots_fetched > 0 || r.hints_fetched > 0 => {
                if r.fetched > 0 {
                    info!(
                        "[prefetch {volume_id}{volume_name}] fetched {} index section(s) ({} from peer, {} from store, {} superseded by GC)",
                        r.fetched,
                        r.fetched_from_peer,
                        r.fetched - r.fetched_from_peer,
                        r.superseded,
                    );
                }
                if r.snapshots_fetched > 0 {
                    info!(
                        "[prefetch {volume_id}{volume_name}] fetched {} snapshot artifact(s) ({} from peer, {} from store)",
                        r.snapshots_fetched,
                        r.snapshots_from_peer,
                        r.snapshots_fetched - r.snapshots_from_peer,
                    );
                }
                if r.hints_fetched > 0 {
                    info!(
                        "[prefetch {volume_id}{volume_name}] persisted {} prefetch hint(s) for body warming",
                        r.hints_fetched,
                    );
                }
            }
            Ok(_) => {}
            Err(e) => {
                warn!("[prefetch {volume_id}{volume_name}] error: {e:#}");
            }
        }
        // Publish the result so any volume binary blocked on `await-prefetch`
        // unblocks before we move on to drain. Errors propagate to the caller
        // as the IPC response. `send_replace` stores the value even when
        // there are no live subscribers, so a late `await-prefetch` call
        // still sees it.
        prefetch_done.send_replace(Some(
            prefetch_result.map(|_| ()).map_err(|e| format!("{e:#}")),
        ));
    } else {
        // No prefetch needed: either the fork already has its full local
        // index, or it's a pulled ancestor whose .idx files are populated by
        // the descendant fork's chain walk. Publish ready so any waiter
        // (await-prefetch IPC, supervisor) unblocks immediately.
        prefetch_done.send_replace(Some(Ok(())));
    }

    let mut orch = GcCycleOrchestrator::new(
        fork_dir.clone(),
        volume_id,
        store,
        gc_config,
        &snapshot_locks,
    );

    let mut tick = tokio::time::interval(drain_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Eviction requests are processed between ticks — never mid-GC.
            Some((ulid_str, reply_tx)) = evict_rx.recv() => {
                let result = match ulid_str {
                    Some(ref s) => evict_one_body(orch.fork_dir(), s),
                    None => evict_bodies(orch.fork_dir()),
                };
                let _ = reply_tx.send(result);
            }
            _ = tick.tick() => {
                if matches!(orch.run_tick().await, TickOutcome::Stop) {
                    break;
                }
            }
        }
    }
}

/// Read the volume name from `volume.toml`, if present and non-empty.
pub fn read_volume_name(fork_dir: &Path) -> Option<String> {
    let cfg = elide_core::config::VolumeConfig::read(fork_dir).ok()?;
    let name = cfg.name?;
    if name.trim().is_empty() {
        None
    } else {
        Some(name)
    }
}

/// True when `fork_dir` is a pulled ancestor: a `by_id/<ulid>/` skeleton with
/// no `volume.name`, materialized by a descendant fork's prefetch chain walk.
/// Pulled ancestors have their `.idx` populated by that descendant's walk;
/// running their own per-volume prefetch repeats every LIST/range-GET.
/// Imported readonly bases have a name and so are NOT pulled ancestors.
pub fn is_pulled_ancestor(fork_dir: &Path) -> bool {
    read_volume_name(fork_dir).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_vol(tmp: &TempDir) -> PathBuf {
        let vol = tmp.path().to_path_buf();
        fs::create_dir_all(vol.join("cache")).unwrap();
        fs::create_dir_all(vol.join("index")).unwrap();
        vol
    }

    fn write_stub_body(vol: &Path, ulid: &str) {
        fs::write(vol.join("cache").join(format!("{ulid}.body")), b"body").unwrap();
        fs::write(vol.join("cache").join(format!("{ulid}.present")), b"\xff").unwrap();
    }

    #[test]
    fn evict_bodies_removes_body_and_present_leaves_idx() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        write_stub_body(&vol, ulid);
        fs::write(vol.join("index").join(format!("{ulid}.idx")), b"idx").unwrap();

        assert_eq!(evict_bodies(&vol).unwrap(), 1);
        assert!(!vol.join("cache").join(format!("{ulid}.body")).exists());
        assert!(!vol.join("cache").join(format!("{ulid}.present")).exists());
        assert!(vol.join("index").join(format!("{ulid}.idx")).exists());
    }

    #[test]
    fn evict_bodies_skips_body_without_idx() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let confirmed = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        let unconfirmed = "01AAAAAAAAAAAAAAAAAAAAAAA2";
        write_stub_body(&vol, confirmed);
        write_stub_body(&vol, unconfirmed);
        fs::write(vol.join("index").join(format!("{confirmed}.idx")), b"idx").unwrap();

        assert_eq!(evict_bodies(&vol).unwrap(), 1);
        assert!(!vol.join("cache").join(format!("{confirmed}.body")).exists());
        assert!(
            vol.join("cache")
                .join(format!("{unconfirmed}.body"))
                .exists()
        );
    }

    #[test]
    fn evict_bodies_missing_cache_dir_returns_zero() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(evict_bodies(tmp.path()).unwrap(), 0);
    }

    #[test]
    fn evict_one_body_succeeds_with_idx() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        write_stub_body(&vol, ulid);
        fs::write(vol.join("index").join(format!("{ulid}.idx")), b"idx").unwrap();

        assert_eq!(evict_one_body(&vol, ulid).unwrap(), 1);
        assert!(!vol.join("cache").join(format!("{ulid}.body")).exists());
        assert!(!vol.join("cache").join(format!("{ulid}.present")).exists());
        assert!(vol.join("index").join(format!("{ulid}.idx")).exists());
    }

    #[test]
    fn evict_one_body_refuses_without_idx() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        write_stub_body(&vol, ulid);
        let err = evict_one_body(&vol, ulid).unwrap_err();
        assert!(err.to_string().contains("not S3-confirmed"));
        assert!(vol.join("cache").join(format!("{ulid}.body")).exists());
    }

    #[test]
    fn evict_one_body_refuses_missing_body() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAA1";
        fs::write(vol.join("index").join(format!("{ulid}.idx")), b"idx").unwrap();
        let err = evict_one_body(&vol, ulid).unwrap_err();
        assert!(err.to_string().contains("not found in cache/"));
    }

    /// Pulled ancestor: skeleton dir with no `volume.name` in volume.toml.
    /// `is_pulled_ancestor` must return true so per-volume prefetch is
    /// skipped — the descendant fork's chain walk covers it.
    #[test]
    fn is_pulled_ancestor_true_when_no_name() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        elide_core::config::VolumeConfig {
            name: None,
            size: Some(4096),
            ..Default::default()
        }
        .write(&vol)
        .unwrap();
        fs::write(vol.join("volume.readonly"), "").unwrap();
        assert!(is_pulled_ancestor(&vol));
    }

    /// Writable fork: has `volume.name`. Per-volume prefetch must run for it
    /// since it owns the chain walk that populates ancestors.
    #[test]
    fn is_pulled_ancestor_false_for_named_writable() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        elide_core::config::VolumeConfig {
            name: Some("my-fork".to_owned()),
            size: Some(4096),
            ..Default::default()
        }
        .write(&vol)
        .unwrap();
        assert!(!is_pulled_ancestor(&vol));
    }

    /// Imported readonly base: has `volume.name` AND `volume.readonly`.
    /// These are roots, not pulled ancestors — they need their own prefetch.
    #[test]
    fn is_pulled_ancestor_false_for_imported_readonly_base() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        elide_core::config::VolumeConfig {
            name: Some("ubuntu-22.04".to_owned()),
            size: Some(4096),
            ..Default::default()
        }
        .write(&vol)
        .unwrap();
        fs::write(vol.join("volume.readonly"), "").unwrap();
        assert!(!is_pulled_ancestor(&vol));
    }

    /// Empty-string name is treated as no name — guards against config drift.
    #[test]
    fn is_pulled_ancestor_true_for_empty_name() {
        let tmp = TempDir::new().unwrap();
        let vol = setup_vol(&tmp);
        elide_core::config::VolumeConfig {
            name: Some(String::new()),
            size: Some(4096),
            ..Default::default()
        }
        .write(&vol)
        .unwrap();
        assert!(is_pulled_ancestor(&vol));
    }
}
