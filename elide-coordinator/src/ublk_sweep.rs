// Coordinator startup reconciliation for ublk devices.
//
// Under the shutdown-park policy (docs/design-ublk-shutdown-park.md), volume
// daemons no longer del_dev on shutdown — they leave the kernel device QUIESCED
// for the next serve to recover via UBLK_F_USER_RECOVERY_REISSUE. This means
// stale kernel devices and stale `ublk.id` bindings can accumulate when a volume
// directory is removed out-of-band, when the host reboots, or when an operator
// manually deletes a device.
//
// `reconcile` runs once at coordinator startup and brings the two sources of
// truth back into agreement:
//
//   1. Enumerate `/sys/class/ublk-char/ublkc<N>` to get the set of live ids.
//   2. Walk `<data_dir>/by_id/*/ublk.id` to get the set of bound ids.
//   3. Live id with no matching binding → orphan: shell out to
//      `elide ublk delete <id>` (bounded subprocess with a per-id timeout).
//   4. Binding pointing at a non-existent live id → stale: remove the file.
//      The next serve sees no binding + no sysfs entry and does a fresh ADD.
//
// The sweep is idempotent and cheap. On non-Linux hosts the sysfs path simply
// does not exist and the sweep is a no-op.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Command;
use tracing::{info, warn};

const SYSFS_UBLK_CHAR: &str = "/sys/class/ublk-char";
/// Per-device timeout for `elide ublk delete`. The CLI's `del_dev` blocks in
/// the kernel until every reference on the ublk_device is released; a stale
/// orphan should not have any holders, but bound the call so a misbehaving
/// kernel state does not block coordinator startup indefinitely.
const PER_DEV_DELETE_TIMEOUT: Duration = Duration::from_secs(10);

/// Run the reconciliation sweep. Logs but never fails — the coordinator must
/// keep starting even if a kernel device cannot be deleted (operator can
/// retry via `elide ublk delete --all`).
pub async fn reconcile(data_dir: &Path, elide_bin: &Path) {
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
        if let Err(e) = delete_device(elide_bin, id).await {
            warn!("[ublk-sweep] elide ublk delete {id}: {e}");
        }
    }

    for (path, id) in stale_bindings {
        info!(
            "[ublk-sweep] clearing stale binding {} → ublk{id} (kernel device gone)",
            path.display()
        );
        if let Err(e) = std::fs::remove_file(&path) {
            warn!("[ublk-sweep] remove {}: {e}", path.display());
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

/// Walk `<data_dir>/by_id/*/ublk.id`, returning a map from binding-file path
/// to the id it contains. Unreadable or unparseable files are skipped with a
/// warning — they will be ignored until the operator cleans them up.
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
        let path = vol_entry.path().join("ublk.id");
        let raw = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                warn!("[ublk-sweep] read {}: {e}", path.display());
                continue;
            }
        };
        match raw.trim().parse::<i32>() {
            Ok(id) => {
                out.insert(path, id);
            }
            Err(e) => {
                warn!("[ublk-sweep] parse {}: {e}", path.display());
            }
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

/// Spawn `elide ublk delete <id>` and bound it with a per-device timeout. The
/// CLI internally does `kill_dev` + `del_dev`, both of which can block in the
/// kernel; the timeout ensures one stuck device does not block startup.
async fn delete_device(elide_bin: &Path, id: i32) -> std::io::Result<()> {
    let mut child = Command::new(elide_bin)
        .arg("ublk")
        .arg("delete")
        .arg(id.to_string())
        .kill_on_drop(true)
        .spawn()?;
    match tokio::time::timeout(PER_DEV_DELETE_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(std::io::Error::other(format!("exit {status}"))),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            let _ = child.start_kill();
            Err(std::io::Error::other(format!(
                "timed out after {PER_DEV_DELETE_TIMEOUT:?}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(path: &str, id: i32) -> (PathBuf, i32) {
        (PathBuf::from(path), id)
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
        let bindings = HashMap::from([binding("/d/by_id/A/ublk.id", 0)]);
        let (orphans, stale) = diff(&live, &bindings);
        assert!(orphans.is_empty(), "matched device must not be orphaned");
        assert!(stale.is_empty(), "matched binding must not be stale");
    }

    #[test]
    fn diff_orphan_kernel_device_is_flagged() {
        // Kernel still has ublk0 but no volume claims it (volume dir was
        // removed out-of-band). Sweep should delete it.
        let live = HashSet::from([0, 1]);
        let bindings = HashMap::from([binding("/d/by_id/A/ublk.id", 1)]);
        let (orphans, stale) = diff(&live, &bindings);
        assert_eq!(orphans, vec![0]);
        assert!(stale.is_empty());
    }

    #[test]
    fn diff_stale_binding_is_flagged() {
        // Volume bound to ublk0, but the kernel forgot the device (host
        // reboot). The binding file should be cleared so the next serve
        // does a fresh ADD.
        let live = HashSet::new();
        let bindings = HashMap::from([binding("/d/by_id/A/ublk.id", 0)]);
        let (orphans, stale) = diff(&live, &bindings);
        assert!(orphans.is_empty());
        assert_eq!(stale, vec![binding("/d/by_id/A/ublk.id", 0)]);
    }

    #[test]
    fn diff_handles_orphan_and_stale_concurrently() {
        // Mixed: one matched, one orphan kernel device, one stale binding.
        let live = HashSet::from([0, 7]);
        let bindings = HashMap::from([
            binding("/d/by_id/A/ublk.id", 0),
            binding("/d/by_id/B/ublk.id", 9),
        ]);
        let (orphans, stale) = diff(&live, &bindings);
        assert_eq!(orphans, vec![7]);
        assert_eq!(stale, vec![binding("/d/by_id/B/ublk.id", 9)]);
    }

    #[test]
    fn scan_bindings_reads_valid_ignores_garbage_and_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let by_id = tmp.path().join("by_id");
        std::fs::create_dir_all(&by_id).unwrap();

        // Valid binding.
        let a = by_id.join("A");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::write(a.join("ublk.id"), "3\n").unwrap();

        // Garbage binding — must be skipped, not crash.
        let b = by_id.join("B");
        std::fs::create_dir_all(&b).unwrap();
        std::fs::write(b.join("ublk.id"), "not-a-number").unwrap();

        // Volume directory with no ublk.id at all — silently absent.
        let c = by_id.join("C");
        std::fs::create_dir_all(&c).unwrap();

        let out = scan_bindings(tmp.path());
        assert_eq!(out.len(), 1);
        assert_eq!(out.get(&a.join("ublk.id")).copied(), Some(3));
    }

    #[test]
    fn scan_bindings_returns_empty_when_data_dir_absent() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // No by_id/ subdirectory.
        let out = scan_bindings(tmp.path());
        assert!(out.is_empty());
    }
}
