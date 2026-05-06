// Coordinator startup reconciliation for ublk devices.
//
// Under the shutdown-park policy (docs/design-ublk-shutdown-park.md), volume
// daemons no longer del_dev on shutdown — they leave the kernel device QUIESCED
// for the next serve to recover via UBLK_F_USER_RECOVERY_REISSUE. This means
// stale kernel devices and stale per-volume bindings (the `[ublk] dev_id`
// field in `volume.toml`) can accumulate when a volume directory is removed
// out-of-band, when the host reboots, or when an operator manually deletes
// a device.
//
// `reconcile` runs once at coordinator startup and brings the two sources of
// truth back into agreement:
//
//   1. Enumerate `/sys/class/ublk-char/ublkc<N>` to get the set of live ids.
//   2. Walk `<data_dir>/by_id/*/volume.toml` for `[ublk] dev_id` values.
//   3. Live id with no matching binding → orphan: kill_dev + del_dev via
//      libublk on a `tokio::task::spawn_blocking` thread (bounded by a
//      per-device timeout).
//   4. Binding pointing at a non-existent live id → stale: clear the
//      `dev_id` field (preserving the `[ublk]` section). The next serve
//      sees no bound id + no sysfs entry and does a fresh ADD.
//
// The sweep is idempotent and cheap. On non-Linux hosts the sysfs path simply
// does not exist and the sweep is a no-op.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use tracing::{info, warn};

const SYSFS_UBLK_CHAR: &str = "/sys/class/ublk-char";

/// Per-device timeout for libublk's `kill_dev` + `del_dev`. The kernel's
/// `del_dev` blocks until every reference on the ublk_device is released; a
/// stale orphan should not have any holders, but bound the call so a
/// misbehaving kernel state does not block coordinator startup indefinitely.
/// On timeout the spawn_blocking task is detached — the syscall finishes (or
/// not) on its own; the coordinator does not wait further.
#[cfg(target_os = "linux")]
const PER_DEV_DELETE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Run the reconciliation sweep. Logs but never fails — the coordinator must
/// keep starting even if a kernel device cannot be deleted (operator can
/// retry via `elide ublk delete --all`).
pub async fn reconcile(data_dir: &Path) {
    let live = match scan_sysfs() {
        Ok(set) => set,
        Err(e) => {
            // ENOENT is the normal case on non-Linux or hosts without the
            // ublk_drv module: nothing to reconcile.
            if e.kind() == std::io::ErrorKind::NotFound {
                return;
            }
            warn!(
                "[ublk-sweep] could not enumerate {SYSFS_UBLK_CHAR}: {e}; skipping reconciliation"
            );
            return;
        }
    };

    let bindings = scan_bindings(data_dir);
    let (orphans, stale_bindings) = diff(&live, &bindings);

    if orphans.is_empty() && stale_bindings.is_empty() {
        return;
    }

    for id in orphans {
        info!("[ublk-sweep] deleting orphan kernel device ublk{id} (no matching volume binding)");
        if let Err(e) = delete_device(id).await {
            warn!("[ublk-sweep] del_dev ublk{id}: {e}");
        }
    }

    for (vol_dir, id) in stale_bindings {
        info!(
            "[ublk-sweep] clearing stale binding {} → ublk{id} (kernel device gone)",
            vol_dir.display()
        );
        if let Err(e) = elide_core::config::VolumeConfig::clear_bound_ublk_id(&vol_dir) {
            warn!("[ublk-sweep] clear dev_id in {}: {e}", vol_dir.display());
        }
    }
}

/// Scan `/sys/class/ublk-char` for `ublkc<N>` entries. Returns `NotFound` when
/// the directory does not exist — that is the signal "no ublk on this host."
fn scan_sysfs() -> std::io::Result<HashSet<i32>> {
    let mut ids = HashSet::new();
    for entry in std::fs::read_dir(SYSFS_UBLK_CHAR)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix("ublkc") else {
            continue;
        };
        if let Ok(id) = rest.parse::<i32>() {
            ids.insert(id);
        }
    }
    Ok(ids)
}

/// Walk `<data_dir>/by_id/*/volume.toml`, returning a map from volume
/// directory to the bound dev_id. Volumes without a `[ublk]` section or
/// without a `dev_id` are skipped (no binding to reconcile). Unreadable
/// configs are skipped with a warning.
fn scan_bindings(data_dir: &Path) -> HashMap<PathBuf, i32> {
    let mut out = HashMap::new();
    let by_id = data_dir.join("by_id");
    let entries = match std::fs::read_dir(&by_id) {
        Ok(e) => e,
        Err(e) => {
            // Empty data dir on first start: nothing to reconcile.
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("[ublk-sweep] read {}: {e}", by_id.display());
            }
            return out;
        }
    };
    for vol_entry in entries.flatten() {
        let vol_dir = vol_entry.path();
        let cfg = match elide_core::config::VolumeConfig::read(&vol_dir) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "[ublk-sweep] read {}: {e}",
                    vol_dir.join("volume.toml").display()
                );
                continue;
            }
        };
        if let Some(id) = cfg.ublk.and_then(|u| u.dev_id) {
            out.insert(vol_dir, id);
        }
    }
    out
}

/// Compute the reconciliation diff: `(orphans, stale_bindings)` where
/// orphans are live kernel ids with no binding and stale_bindings are
/// binding paths whose id has no live kernel entry. Pure function for
/// unit-testing the sweep's decisions independent of sysfs / subprocess.
fn diff(live: &HashSet<i32>, bindings: &HashMap<PathBuf, i32>) -> (Vec<i32>, Vec<(PathBuf, i32)>) {
    let bound_ids: HashSet<i32> = bindings.values().copied().collect();
    let mut orphans: Vec<i32> = live
        .iter()
        .copied()
        .filter(|id| !bound_ids.contains(id))
        .collect();
    orphans.sort();
    let mut stale: Vec<(PathBuf, i32)> = bindings
        .iter()
        .filter(|(_, id)| !live.contains(id))
        .map(|(p, id)| (p.clone(), *id))
        .collect();
    stale.sort_by(|a, b| a.0.cmp(&b.0));
    (orphans, stale)
}

/// Tear down a previously-bound kernel ublk device for this volume.
///
/// `bound_id` is the dev_id captured from `volume.toml` *before* the
/// caller wrote the new config (which may have removed the `[ublk]`
/// section entirely). On a `update --no-ublk` flow the new cfg has
/// already had its dev_id dropped along with the section, so there's
/// nothing left to clear here — only the kernel device to remove.
///
/// Caller must have already shut the volume daemon down so the kernel's
/// queue io_uring fds are released. `kill_dev` itself is safe-from-
/// anywhere, but `del_dev` blocks until refcounts drop.
pub(crate) async fn teardown_bound_device(vol_dir: &Path, bound_id: i32) {
    info!(
        "[ublk-teardown] removing kernel device ublk{bound_id} bound to {}",
        vol_dir.display()
    );
    if let Err(e) = delete_device(bound_id).await {
        warn!("[ublk-teardown] del_dev ublk{bound_id}: {e}");
    }
}

/// Open a fresh `new_simple` control device and issue `kill_dev` + `del_dev`
/// against it, on a `spawn_blocking` thread (libublk's control ioctls are
/// synchronous). Bounded by `PER_DEV_DELETE_TIMEOUT`. On timeout the blocking
/// task is detached: cancelling a kernel `wait_event` from outside is not
/// possible, so the underlying syscall finishes on its own when the kernel's
/// last reference drops, and the leak is bounded by process lifetime.
#[cfg(target_os = "linux")]
async fn delete_device(id: i32) -> std::io::Result<()> {
    let task = tokio::task::spawn_blocking(move || {
        let ctrl = libublk::ctrl::UblkCtrl::new_simple(id)
            .map_err(|e| std::io::Error::other(format!("open ctrl for dev {id}: {e}")))?;
        // kill_dev is the documented safe-from-anywhere stop; del_dev then
        // removes the kernel entry and libublk's json file. del_dev on a
        // for-add ctrl deadlocks (see libublk Drop), but we built a
        // new_simple ctrl above so this is safe.
        let _ = ctrl.kill_dev();
        ctrl.del_dev()
            .map_err(|e| std::io::Error::other(format!("del_dev {id}: {e}")))?;
        Ok::<(), std::io::Error>(())
    });
    match tokio::time::timeout(PER_DEV_DELETE_TIMEOUT, task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => Err(e),
        Ok(Err(e)) => Err(std::io::Error::other(format!("blocking task: {e}"))),
        Err(_) => Err(std::io::Error::other(format!(
            "timed out after {PER_DEV_DELETE_TIMEOUT:?}"
        ))),
    }
}

/// Non-Linux stub. The sweep never reaches here because `scan_sysfs` returns
/// `NotFound` and the function exits early, but the symbol must exist for the
/// caller to compile cross-platform.
#[cfg(not(target_os = "linux"))]
async fn delete_device(_id: i32) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "ublk del_dev not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(vol_dir: &str, id: i32) -> (PathBuf, i32) {
        (PathBuf::from(vol_dir), id)
    }

    /// Write a `volume.toml` for a test volume directory with the given
    /// optional bound ublk dev id.
    fn write_cfg(vol_dir: &Path, dev_id: Option<i32>) {
        let cfg = elide_core::config::VolumeConfig {
            ublk: Some(elide_core::config::UblkConfig { dev_id }),
            ..Default::default()
        };
        cfg.write(vol_dir).unwrap();
    }

    #[test]
    fn diff_no_state_means_no_work() {
        let (orphans, stale) = diff(&HashSet::new(), &HashMap::new());
        assert!(orphans.is_empty());
        assert!(stale.is_empty());
    }

    #[test]
    fn diff_matched_binding_is_kept() {
        let live = HashSet::from([0]);
        let bindings = HashMap::from([binding("/d/by_id/A", 0)]);
        let (orphans, stale) = diff(&live, &bindings);
        assert!(orphans.is_empty(), "matched device must not be orphaned");
        assert!(stale.is_empty(), "matched binding must not be stale");
    }

    #[test]
    fn diff_orphan_kernel_device_is_flagged() {
        // Kernel still has ublk0 but no volume claims it (volume dir was
        // removed out-of-band). Sweep should delete it.
        let live = HashSet::from([0, 1]);
        let bindings = HashMap::from([binding("/d/by_id/A", 1)]);
        let (orphans, stale) = diff(&live, &bindings);
        assert_eq!(orphans, vec![0]);
        assert!(stale.is_empty());
    }

    #[test]
    fn diff_stale_binding_is_flagged() {
        // Volume bound to ublk0, but the kernel forgot the device (host
        // reboot). The binding should be cleared so the next serve does
        // a fresh ADD.
        let live = HashSet::new();
        let bindings = HashMap::from([binding("/d/by_id/A", 0)]);
        let (orphans, stale) = diff(&live, &bindings);
        assert!(orphans.is_empty());
        assert_eq!(stale, vec![binding("/d/by_id/A", 0)]);
    }

    #[test]
    fn diff_handles_orphan_and_stale_concurrently() {
        // Mixed: one matched, one orphan kernel device, one stale binding.
        let live = HashSet::from([0, 7]);
        let bindings = HashMap::from([binding("/d/by_id/A", 0), binding("/d/by_id/B", 9)]);
        let (orphans, stale) = diff(&live, &bindings);
        assert_eq!(orphans, vec![7]);
        assert_eq!(stale, vec![binding("/d/by_id/B", 9)]);
    }

    #[test]
    fn scan_bindings_reads_cfg_skips_volumes_without_dev_id() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let by_id = tmp.path().join("by_id");
        std::fs::create_dir_all(&by_id).unwrap();

        // Valid binding: cfg.ublk = Some, dev_id = Some(3).
        let a = by_id.join("A");
        std::fs::create_dir_all(&a).unwrap();
        write_cfg(&a, Some(3));

        // Volume with [ublk] but no bound id (auto-allocate, never
        // ADDed). Must be skipped — there's no binding to reconcile.
        let b = by_id.join("B");
        std::fs::create_dir_all(&b).unwrap();
        write_cfg(&b, None);

        // Volume with no volume.toml at all (e.g. ancestor skeleton).
        // Must be skipped silently.
        let c = by_id.join("C");
        std::fs::create_dir_all(&c).unwrap();

        let out = scan_bindings(tmp.path());
        assert_eq!(out.len(), 1);
        assert_eq!(out.get(&a).copied(), Some(3));
    }

    #[test]
    fn scan_bindings_returns_empty_when_data_dir_absent() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // No by_id/ subdirectory.
        let out = scan_bindings(tmp.path());
        assert!(out.is_empty());
    }
}
