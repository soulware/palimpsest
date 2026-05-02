//! Local-only volume lifecycle and mode classification.
//!
//! `VolumeMode` and `VolumeLifecycle` capture the on-disk state the
//! coordinator can determine by inspecting a volume directory. Both
//! the CLI (`elide volume list`) and the coordinator's
//! `volume_status` IPC verb derive their answers through this
//! module, so the operator vocabulary lives in exactly one place.
//!
//! This is distinct from `NameState` (in `elide-core::name_record`),
//! which describes the *bucket-level* lifecycle of a named volume. A
//! single volume may simultaneously be `NameState::Live` (S3 thinks
//! we own it) and `VolumeLifecycle::Stopped` (the daemon is down on
//! this host); the two views are intentionally orthogonal.

use std::path::Path;

use elide_core::process::pid_is_alive;
use serde::{Deserialize, Serialize};

/// Per-volume daemon pidfile. Written by the volume process on
/// startup; presence + liveness drives the `Running` classification.
/// Canonical home for the filename — other modules consume it from
/// here rather than redefining the literal.
pub const PID_FILE: &str = "volume.pid";

/// Manual-stop marker. Presence pins the volume to `StoppedManual`
/// regardless of any other state — the coordinator's supervisor
/// treats this as "do not relaunch".
pub const STOPPED_FILE: &str = "volume.stopped";

/// Released marker. Written by the release IPC handlers after a
/// successful `names/<name>` flip to `Released`, cleared by the
/// in-place reclaim path. Body is the handoff snapshot ULID.
///
/// **Display only.** Authoritative claim state lives in
/// `names/<name>` in the bucket; this marker exists so `elide
/// volume list` can render `released` without an S3 round-trip.
/// The supervisor, reconcile path, and claim logic must not gate on
/// it.
pub const RELEASED_FILE: &str = "volume.released";

/// Per-volume import lock. Written by `elide-import`'s supervision
/// protocol while a subprocess is running. The body is the import
/// ULID.
pub const IMPORT_LOCK_FILE: &str = "import.lock";

/// Read/write mode for a volume. Readonly is set on imported OCI
/// volumes; everything else is read/write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeMode {
    /// Read-only (typically an imported OCI image).
    Ro,
    /// Read/write.
    Rw,
}

impl VolumeMode {
    /// Lowercase 2-char label for table display.
    pub fn label(self) -> &'static str {
        match self {
            Self::Ro => "ro",
            Self::Rw => "rw",
        }
    }
}

impl std::fmt::Display for VolumeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Local lifecycle of a volume, derived from on-disk markers.
///
/// Order of precedence in [`VolumeLifecycle::from_dir`]:
///   1. `volume.released` exists → `Released { handoff_snapshot }`
///   2. `volume.stopped` exists → `StoppedManual`
///   3. `import.lock` exists → `Importing { import_ulid }`
///   4. `volume.pid` names a live process → `Running { pid }`
///   5. otherwise → `Stopped`
///
/// `Released` is a CLI-display variant. The bucket's `names/<name>`
/// record is authoritative for claim state; the local marker only
/// drives table rendering so `volume list` can label a released
/// volume without an S3 round-trip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum VolumeLifecycle {
    /// Daemon is running with the embedded pid.
    Running { pid: u32 },
    /// Import subprocess is active. The ULID is read from the lock file.
    Importing { import_ulid: String },
    /// `volume.released` marker is present; the bucket record is in
    /// `Released` state and a fresh claim is needed before this host
    /// can serve the volume again. The handoff snapshot ULID is read
    /// from the marker body (best-effort; empty when absent or
    /// unparsable).
    Released { handoff_snapshot: String },
    /// `volume.stopped` marker is present; supervisor will not relaunch.
    StoppedManual,
    /// Daemon is not running and no manual-stop marker is present.
    Stopped,
}

impl VolumeLifecycle {
    /// Derive lifecycle from the on-disk markers in `vol_dir`.
    ///
    /// Reads up to four small files; fast enough to call per-volume
    /// in the CLI's list path. Errors reading any file collapse to
    /// the next-precedence variant rather than surfacing.
    pub fn from_dir(vol_dir: &Path) -> Self {
        let released = vol_dir.join(RELEASED_FILE);
        if released.exists() {
            let handoff_snapshot = std::fs::read_to_string(&released)
                .unwrap_or_default()
                .trim()
                .to_owned();
            return Self::Released { handoff_snapshot };
        }
        if vol_dir.join(STOPPED_FILE).exists() {
            return Self::StoppedManual;
        }
        let lock = vol_dir.join(IMPORT_LOCK_FILE);
        if lock.exists() {
            let import_ulid = std::fs::read_to_string(&lock)
                .unwrap_or_default()
                .trim()
                .to_owned();
            return Self::Importing { import_ulid };
        }
        if let Ok(text) = std::fs::read_to_string(vol_dir.join(PID_FILE))
            && let Ok(pid) = text.trim().parse::<u32>()
            && pid_is_alive(pid)
        {
            return Self::Running { pid };
        }
        Self::Stopped
    }

    /// Operator-facing label for table display: `"running"`,
    /// `"importing"`, `"released"`, `"stopped (manual)"`,
    /// `"stopped"`. Drops the pid/ulid payload — see
    /// [`Self::wire_body`] for the IPC format.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Running { .. } => "running",
            Self::Importing { .. } => "importing",
            Self::Released { .. } => "released",
            Self::StoppedManual => "stopped (manual)",
            Self::Stopped => "stopped",
        }
    }

    /// Body string for the `volume_status` IPC reply (without the
    /// leading `"ok "`). Identical to [`Self::label`] except
    /// `Importing` and `Released` append their associated ULIDs so
    /// clients can correlate with bucket state.
    pub fn wire_body(&self) -> String {
        match self {
            Self::Importing { import_ulid } => format!("importing {import_ulid}"),
            Self::Released { handoff_snapshot } if !handoff_snapshot.is_empty() => {
                format!("released {handoff_snapshot}")
            }
            other => other.label().to_owned(),
        }
    }

    /// Pid of the live volume daemon, when running.
    pub fn pid(&self) -> Option<u32> {
        match self {
            Self::Running { pid } => Some(*pid),
            _ => None,
        }
    }
}

/// Write `volume.released` with the handoff snapshot ULID as the body.
///
/// Called from the release IPC handlers after the bucket-side
/// `names/<name>` flip succeeds. Display-only — see [`RELEASED_FILE`].
pub fn write_released_marker(vol_dir: &Path, handoff: ulid::Ulid) -> std::io::Result<()> {
    std::fs::write(vol_dir.join(RELEASED_FILE), handoff.to_string())
}

/// Remove `volume.released` if present; missing-file is not an error.
///
/// Called from the in-place reclaim path after the bucket flip back
/// to a non-`Released` state succeeds.
pub fn clear_released_marker(vol_dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(vol_dir.join(RELEASED_FILE)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn empty_dir_classifies_as_stopped() {
        let d = TempDir::new().unwrap();
        assert_eq!(
            VolumeLifecycle::from_dir(d.path()),
            VolumeLifecycle::Stopped
        );
    }

    #[test]
    fn released_marker_classifies_as_released() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(RELEASED_FILE), "01J0000000000000000000000V").unwrap();
        match VolumeLifecycle::from_dir(d.path()) {
            VolumeLifecycle::Released { handoff_snapshot } => {
                assert_eq!(handoff_snapshot, "01J0000000000000000000000V");
            }
            other => panic!("expected Released, got {other:?}"),
        }
    }

    #[test]
    fn released_marker_takes_precedence_over_stopped_pid_and_import_lock() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(RELEASED_FILE), "01J9").unwrap();
        std::fs::write(d.path().join(STOPPED_FILE), "").unwrap();
        std::fs::write(d.path().join(IMPORT_LOCK_FILE), "01J7").unwrap();
        std::fs::write(d.path().join(PID_FILE), std::process::id().to_string()).unwrap();
        match VolumeLifecycle::from_dir(d.path()) {
            VolumeLifecycle::Released { handoff_snapshot } => {
                assert_eq!(handoff_snapshot, "01J9");
            }
            other => panic!("expected Released, got {other:?}"),
        }
    }

    #[test]
    fn released_marker_with_empty_body_classifies_as_released_with_empty_snapshot() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(RELEASED_FILE), "").unwrap();
        match VolumeLifecycle::from_dir(d.path()) {
            VolumeLifecycle::Released { handoff_snapshot } => {
                assert_eq!(handoff_snapshot, "");
            }
            other => panic!("expected Released, got {other:?}"),
        }
    }

    #[test]
    fn write_and_clear_released_marker_round_trip() {
        let d = TempDir::new().unwrap();
        let snap = ulid::Ulid::new();
        write_released_marker(d.path(), snap).unwrap();
        assert_eq!(
            VolumeLifecycle::from_dir(d.path()),
            VolumeLifecycle::Released {
                handoff_snapshot: snap.to_string()
            }
        );
        clear_released_marker(d.path()).unwrap();
        assert_eq!(
            VolumeLifecycle::from_dir(d.path()),
            VolumeLifecycle::Stopped
        );
        // Idempotent when already cleared.
        clear_released_marker(d.path()).unwrap();
    }

    #[test]
    fn stopped_marker_takes_precedence_over_pid_and_lock() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(STOPPED_FILE), "").unwrap();
        std::fs::write(
            d.path().join(IMPORT_LOCK_FILE),
            "01J0000000000000000000000V",
        )
        .unwrap();
        std::fs::write(d.path().join(PID_FILE), std::process::id().to_string()).unwrap();
        assert_eq!(
            VolumeLifecycle::from_dir(d.path()),
            VolumeLifecycle::StoppedManual
        );
    }

    #[test]
    fn import_lock_takes_precedence_over_pid() {
        let d = TempDir::new().unwrap();
        std::fs::write(
            d.path().join(IMPORT_LOCK_FILE),
            "01J0000000000000000000000V\n",
        )
        .unwrap();
        std::fs::write(d.path().join(PID_FILE), std::process::id().to_string()).unwrap();
        match VolumeLifecycle::from_dir(d.path()) {
            VolumeLifecycle::Importing { import_ulid } => {
                assert_eq!(import_ulid, "01J0000000000000000000000V");
            }
            other => panic!("expected Importing, got {other:?}"),
        }
    }

    #[test]
    fn import_lock_with_empty_body_classifies_as_importing_with_empty_ulid() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(IMPORT_LOCK_FILE), "").unwrap();
        match VolumeLifecycle::from_dir(d.path()) {
            VolumeLifecycle::Importing { import_ulid } => {
                assert_eq!(import_ulid, "");
            }
            other => panic!("expected Importing, got {other:?}"),
        }
    }

    #[test]
    fn live_pidfile_classifies_as_running() {
        let d = TempDir::new().unwrap();
        let me = std::process::id();
        std::fs::write(d.path().join(PID_FILE), me.to_string()).unwrap();
        assert_eq!(
            VolumeLifecycle::from_dir(d.path()),
            VolumeLifecycle::Running { pid: me }
        );
    }

    #[test]
    fn dead_pidfile_classifies_as_stopped() {
        let d = TempDir::new().unwrap();
        // u32::MAX is far above any plausible system pid_max, so the
        // kernel returns ESRCH for `kill(pid, 0)` (matches the
        // `pid_is_alive` test in elide-core).
        std::fs::write(d.path().join(PID_FILE), u32::MAX.to_string()).unwrap();
        assert_eq!(
            VolumeLifecycle::from_dir(d.path()),
            VolumeLifecycle::Stopped
        );
    }

    #[test]
    fn malformed_pidfile_classifies_as_stopped() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(PID_FILE), "not a number").unwrap();
        assert_eq!(
            VolumeLifecycle::from_dir(d.path()),
            VolumeLifecycle::Stopped
        );
    }

    #[test]
    fn label_drops_payload() {
        assert_eq!(VolumeLifecycle::Running { pid: 42 }.label(), "running");
        assert_eq!(
            VolumeLifecycle::Importing {
                import_ulid: "01...".to_owned()
            }
            .label(),
            "importing"
        );
        assert_eq!(VolumeLifecycle::StoppedManual.label(), "stopped (manual)");
        assert_eq!(VolumeLifecycle::Stopped.label(), "stopped");
        assert_eq!(
            VolumeLifecycle::Released {
                handoff_snapshot: "01J9".to_owned()
            }
            .label(),
            "released"
        );
    }

    #[test]
    fn wire_body_includes_ulid_for_importing_only() {
        assert_eq!(VolumeLifecycle::Running { pid: 42 }.wire_body(), "running");
        assert_eq!(
            VolumeLifecycle::Importing {
                import_ulid: "01J0".to_owned()
            }
            .wire_body(),
            "importing 01J0"
        );
        assert_eq!(
            VolumeLifecycle::StoppedManual.wire_body(),
            "stopped (manual)"
        );
        assert_eq!(VolumeLifecycle::Stopped.wire_body(), "stopped");
        assert_eq!(
            VolumeLifecycle::Released {
                handoff_snapshot: "01J9".to_owned()
            }
            .wire_body(),
            "released 01J9"
        );
        assert_eq!(
            VolumeLifecycle::Released {
                handoff_snapshot: String::new()
            }
            .wire_body(),
            "released"
        );
    }

    #[test]
    fn pid_only_set_for_running() {
        assert_eq!(VolumeLifecycle::Running { pid: 42 }.pid(), Some(42));
        assert_eq!(
            VolumeLifecycle::Importing {
                import_ulid: String::new()
            }
            .pid(),
            None
        );
        assert_eq!(VolumeLifecycle::StoppedManual.pid(), None);
        assert_eq!(VolumeLifecycle::Stopped.pid(), None);
        assert_eq!(
            VolumeLifecycle::Released {
                handoff_snapshot: "01J9".to_owned()
            }
            .pid(),
            None
        );
    }

    #[test]
    fn volume_mode_label() {
        assert_eq!(VolumeMode::Ro.label(), "ro");
        assert_eq!(VolumeMode::Rw.label(), "rw");
        assert_eq!(format!("{}", VolumeMode::Ro), "ro");
    }
}
