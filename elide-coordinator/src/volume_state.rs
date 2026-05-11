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
/// Acts as a park marker: the supervisor refuses to spawn a daemon
/// while this is present (see [`crate::park`]). `lifecycle::reconcile_marker`
/// keeps the file in sync with the bucket's `names/<name>` record so
/// drift self-heals at the next scan. Authoritative claim state still
/// lives in S3.
pub const RELEASED_FILE: &str = "volume.released";

/// Importing marker. Written by `elide-import`'s supervision protocol
/// while a subprocess is running. Body is the import ULID. Naming is
/// aligned with the other `volume.<state>` lifecycle markers.
pub const IMPORTING_FILE: &str = "volume.importing";

/// Per-fetch-worker pidfile. Written by the fetch orchestrator
/// before it spawns `elide fetch-volume`, removed when the worker
/// exits. Used by `Request::RegisterFetchWorker` to PID-bind the
/// macaroon — same model as `volume.pid` for the volume daemon, but
/// a distinct file so the lifecycle classifier doesn't see the
/// fetch worker's PID and mis-classify the volume as `Running`.
pub const FETCH_PID_FILE: &str = "fetch.pid";

/// Fetched marker. Written by the coordinator after a `volume fetch`
/// run completes successfully. Indicates this host holds a local copy
/// of a foreign volume that has *not* been claimed locally — the
/// owner's bucket-side state machine is untouched. Body is a small
/// TOML record (`basis_snapshot`, `owner_coordinator_id`,
/// `fetched_at`); see [`FetchedRecord`].
///
/// Lifetime: from worker exit on a successful fetch until either
///   - `volume remove` (manual cleanup), or
///   - `volume claim` of the foreign name (the local copy transitions
///     to a readonly ancestor of the freshly-minted claim fork; the
///     marker is cleared by the claim path).
pub const FETCHED_FILE: &str = "volume.fetched";

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
///   1. `volume.fetched` exists → `Fetched { basis_snapshot }`
///   2. `volume.released` exists → `Released { handoff_snapshot }`
///   3. `volume.stopped` exists → `StoppedManual`
///   4. `volume.importing` exists → `Importing { import_ulid }`
///   5. `volume.pid` names a live process → `Running { pid }`
///   6. otherwise → `Stopped`
///
/// `Fetched` is a purely-local state, orthogonal to the bucket-side
/// `names/<name>` record (which still reflects the owner's view —
/// `Live`, `Stopped`, or `Released` from their perspective). It says
/// only "this host has a warm cache of a foreign volume."
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
    /// `volume.fetched` marker is present; this host holds a foreign
    /// volume's bytes against the named basis snapshot but has not
    /// claimed the name. The basis snapshot ULID is read from the
    /// marker body.
    Fetched { basis_snapshot: String },
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
        if let Some(record) = FetchedRecord::read(vol_dir) {
            return Self::Fetched {
                basis_snapshot: record.basis_snapshot,
            };
        }
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
        let lock = vol_dir.join(IMPORTING_FILE);
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
            Self::Fetched { .. } => "fetched",
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
            Self::Fetched { basis_snapshot } if !basis_snapshot.is_empty() => {
                format!("fetched {basis_snapshot}")
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

/// Contents of a `volume.fetched` marker. Written by the coordinator
/// after a successful `volume fetch` worker exit; cleared by either
/// `volume remove` or the claim path's transition into ancestor-of-
/// fork shape.
///
/// Format on disk is TOML so the file is inspectable with `cat`:
///
/// ```toml
/// basis_snapshot = "01J7…"
/// owner_coordinator_id = "01J7…"
/// fetched_at = "2026-05-09T14:23:51Z"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchedRecord {
    /// Owner-signed snapshot ULID this fetch was warmed against.
    pub basis_snapshot: String,
    /// Coordinator id that signed `basis_snapshot`'s manifest at the
    /// time of fetch — pulled from the manifest's recovery metadata if
    /// the basis is a synthesised handoff snapshot, otherwise empty.
    /// Advisory; the volume's own `volume.pub` is the verifying key
    /// for ordinary basis snapshots.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner_coordinator_id: String,
    /// Wall-clock time the fetch worker exited cleanly (RFC3339).
    pub fetched_at: String,
}

impl FetchedRecord {
    /// Read and parse `<vol_dir>/volume.fetched`. Returns `None` if the
    /// marker is absent or unparseable; treat unparseable as absent so
    /// a hand-edited file doesn't pin the volume to a broken state.
    pub fn read(vol_dir: &Path) -> Option<Self> {
        let content = std::fs::read_to_string(vol_dir.join(FETCHED_FILE)).ok()?;
        toml::from_str::<Self>(&content).ok()
    }

    /// Write `<vol_dir>/volume.fetched` atomically. The marker is small
    /// (sub-1 KiB) so we don't bother with a tmp+rename dance —
    /// presence is what matters for the lifecycle classifier; the body
    /// is advisory.
    pub fn write(&self, vol_dir: &Path) -> std::io::Result<()> {
        let s = toml::to_string(self)
            .map_err(|e| std::io::Error::other(format!("serialise volume.fetched: {e}")))?;
        std::fs::write(vol_dir.join(FETCHED_FILE), s)
    }
}

/// Remove `volume.fetched` if present; missing-file is not an error.
/// Called by the claim path after the foreign by_id directory becomes
/// a readonly ancestor of a freshly-minted owned fork, and by
/// `volume remove`.
pub fn clear_fetched_marker(vol_dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(vol_dir.join(FETCHED_FILE)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
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

/// Outcome categories for [`reconcile_owned_local_to_stopped`].
#[derive(Debug, PartialEq, Eq)]
pub enum ReconcileOutcome {
    /// Local fork was already in the canonical Stopped+writable shape;
    /// nothing was changed. Returned when `volume.key` was present,
    /// no transient markers (`volume.fetched`/`volume.readonly`) were
    /// stamped, and `volume.stopped` already existed.
    AlreadyStopped,
    /// At least one of `volume.key`, the transient markers, or
    /// `volume.stopped` had to be written/removed to reach the
    /// canonical shape.
    Reconciled,
}

/// Errors returned by [`reconcile_owned_local_to_stopped`].
#[derive(Debug)]
pub enum ReconcileError {
    /// `volume.key` is missing locally and no key shadow exists at
    /// `data_dir/keys/<vol_ulid>.key`. This fork can't be made
    /// writable in place — the caller should refuse and direct the
    /// operator to fork via `volume create --from`.
    NoKeyShadow,
    /// Filesystem I/O failed mid-reconcile (e.g. permission denied
    /// writing `volume.key` or `volume.stopped`). Local state may be
    /// partially reconciled; idempotent retry is the recovery path.
    Io(std::io::Error),
    /// The volume daemon appears to be currently running (`control.sock`
    /// is present). Reconciliation refuses rather than racing the
    /// daemon — operator should `volume stop` first.
    DaemonRunning,
}

impl std::fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoKeyShadow => write!(
                f,
                "no key shadow available; foreign fetched fork cannot be made \
                 writable in place"
            ),
            Self::Io(e) => write!(f, "i/o error during reconcile: {e}"),
            Self::DaemonRunning => {
                write!(f, "volume daemon is running; stop it before reconciling")
            }
        }
    }
}

impl std::error::Error for ReconcileError {}

impl From<std::io::Error> for ReconcileError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Bring an owned-by-us local fork into the canonical Stopped+
/// writable shape. Idempotent — calling on an already-reconciled
/// fork returns [`ReconcileOutcome::AlreadyStopped`].
///
/// The shape we converge on:
///
///   - `volume.key` present (restored from `data_dir/keys/<vol_ulid>.key`
///     when absent).
///   - `volume.readonly` absent.
///   - `volume.fetched` absent.
///   - `volume.stopped` present.
///   - `control.sock` absent (i.e. daemon not running).
///
/// Used by:
///   - `volume claim` against a `Live`/`Stopped` record owned by us
///     (idempotent "I already own this; just make sure local is
///     consistent").
///   - `volume fetch` against a record owned by us (final stage:
///     drop the foreign-fetched markers in favour of the writable
///     Stopped shape).
///   - `volume claim` against a `Released` record where the local
///     fork is a fetched copy of our own lineage (the case fixed in
///     the original key-shadow rollout).
pub fn reconcile_owned_local_to_stopped(
    fork_dir: &Path,
    data_dir: &Path,
    vol_ulid: ulid::Ulid,
) -> Result<ReconcileOutcome, ReconcileError> {
    if fork_dir.join("control.sock").exists() {
        return Err(ReconcileError::DaemonRunning);
    }

    let mut changed = false;

    // (1) Ensure volume.key is present.
    let key_path = fork_dir.join(elide_core::signing::VOLUME_KEY_FILE);
    if !key_path.exists() {
        let shadow = crate::key_shadow::read(data_dir, vol_ulid)?;
        let Some(key_bytes) = shadow else {
            return Err(ReconcileError::NoKeyShadow);
        };
        elide_core::segment::write_file_atomic(&key_path, &key_bytes)?;
        changed = true;
    }

    // (2) Strip transient markers.
    for marker in ["volume.readonly", FETCHED_FILE] {
        match std::fs::remove_file(fork_dir.join(marker)) {
            Ok(()) => changed = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(ReconcileError::Io(e)),
        }
    }

    // (3) Ensure volume.stopped is present.
    let stopped = fork_dir.join(STOPPED_FILE);
    if !stopped.exists() {
        std::fs::write(&stopped, "")?;
        changed = true;
    }

    Ok(if changed {
        ReconcileOutcome::Reconciled
    } else {
        ReconcileOutcome::AlreadyStopped
    })
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
        std::fs::write(d.path().join(IMPORTING_FILE), "01J7").unwrap();
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
    fn fetched_marker_classifies_as_fetched() {
        let d = TempDir::new().unwrap();
        let rec = FetchedRecord {
            basis_snapshot: "01J0000000000000000000000V".to_owned(),
            owner_coordinator_id: "01J7".to_owned(),
            fetched_at: "2026-05-09T14:23:51Z".to_owned(),
        };
        rec.write(d.path()).unwrap();
        match VolumeLifecycle::from_dir(d.path()) {
            VolumeLifecycle::Fetched { basis_snapshot } => {
                assert_eq!(basis_snapshot, "01J0000000000000000000000V");
            }
            other => panic!("expected Fetched, got {other:?}"),
        }
    }

    #[test]
    fn fetched_marker_takes_precedence_over_other_markers() {
        let d = TempDir::new().unwrap();
        FetchedRecord {
            basis_snapshot: "01J9".to_owned(),
            owner_coordinator_id: String::new(),
            fetched_at: "2026-05-09T14:23:51Z".to_owned(),
        }
        .write(d.path())
        .unwrap();
        std::fs::write(d.path().join(STOPPED_FILE), "").unwrap();
        std::fs::write(d.path().join(IMPORTING_FILE), "01J7").unwrap();
        std::fs::write(d.path().join(PID_FILE), std::process::id().to_string()).unwrap();
        match VolumeLifecycle::from_dir(d.path()) {
            VolumeLifecycle::Fetched { basis_snapshot } => {
                assert_eq!(basis_snapshot, "01J9");
            }
            other => panic!("expected Fetched, got {other:?}"),
        }
    }

    #[test]
    fn fetched_record_round_trip_through_disk() {
        let d = TempDir::new().unwrap();
        let original = FetchedRecord {
            basis_snapshot: "01J0000000000000000000000V".to_owned(),
            owner_coordinator_id: "01J7000000000000000000000W".to_owned(),
            fetched_at: "2026-05-09T14:23:51Z".to_owned(),
        };
        original.write(d.path()).unwrap();
        let parsed = FetchedRecord::read(d.path()).unwrap();
        assert_eq!(parsed, original);
        clear_fetched_marker(d.path()).unwrap();
        assert!(FetchedRecord::read(d.path()).is_none());
        // Idempotent when already cleared.
        clear_fetched_marker(d.path()).unwrap();
    }

    #[test]
    fn fetched_marker_garbage_body_treated_as_absent() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(FETCHED_FILE), "not toml at all =").unwrap();
        // Unparseable -> classifier falls through to next-precedence rule.
        assert_eq!(
            VolumeLifecycle::from_dir(d.path()),
            VolumeLifecycle::Stopped
        );
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
        std::fs::write(d.path().join(IMPORTING_FILE), "01J0000000000000000000000V").unwrap();
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
            d.path().join(IMPORTING_FILE),
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
        std::fs::write(d.path().join(IMPORTING_FILE), "").unwrap();
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

    // ── reconcile_owned_local_to_stopped ─────────────────────────────

    /// Set up a `(data_dir, vol_dir)` pair plus an optional key shadow
    /// under `data_dir/keys/<vol_ulid>.key`. Returns the temp guard
    /// alongside the paths so the caller can let it drop on exit.
    fn reconcile_scaffolding(
        with_shadow: bool,
    ) -> (TempDir, std::path::PathBuf, std::path::PathBuf, ulid::Ulid) {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let vol_ulid = ulid::Ulid::new();
        let vol_dir = data_dir.join("by_id").join(vol_ulid.to_string());
        std::fs::create_dir_all(&vol_dir).unwrap();
        if with_shadow {
            crate::key_shadow::write(&data_dir, vol_ulid, &[0u8; 32]).unwrap();
        }
        (tmp, data_dir, vol_dir, vol_ulid)
    }

    #[test]
    fn reconcile_no_op_when_already_stopped_writable() {
        let (_tmp, data_dir, vol_dir, vol_ulid) = reconcile_scaffolding(false);
        // Already-good shape: key present, stopped marker present, no
        // transient markers.
        std::fs::write(
            vol_dir.join(elide_core::signing::VOLUME_KEY_FILE),
            [0u8; 32],
        )
        .unwrap();
        std::fs::write(vol_dir.join(STOPPED_FILE), "").unwrap();
        let out = reconcile_owned_local_to_stopped(&vol_dir, &data_dir, vol_ulid).unwrap();
        assert_eq!(out, ReconcileOutcome::AlreadyStopped);
    }

    #[test]
    fn reconcile_restores_key_from_shadow_and_strips_markers() {
        let (_tmp, data_dir, vol_dir, vol_ulid) = reconcile_scaffolding(true);
        // Fetched shape: readonly + fetched markers, no key, no stopped.
        std::fs::write(vol_dir.join("volume.readonly"), "").unwrap();
        std::fs::write(vol_dir.join(FETCHED_FILE), "{}").unwrap();
        let out = reconcile_owned_local_to_stopped(&vol_dir, &data_dir, vol_ulid).unwrap();
        assert_eq!(out, ReconcileOutcome::Reconciled);
        assert!(
            vol_dir.join(elide_core::signing::VOLUME_KEY_FILE).exists(),
            "key must be restored from shadow"
        );
        assert!(
            !vol_dir.join("volume.readonly").exists(),
            "readonly marker must be stripped"
        );
        assert!(
            !vol_dir.join(FETCHED_FILE).exists(),
            "fetched marker must be stripped"
        );
        assert!(
            vol_dir.join(STOPPED_FILE).exists(),
            "stopped marker must be written"
        );
    }

    #[test]
    fn reconcile_refuses_when_no_key_and_no_shadow() {
        let (_tmp, data_dir, vol_dir, vol_ulid) = reconcile_scaffolding(false);
        std::fs::write(vol_dir.join("volume.readonly"), "").unwrap();
        std::fs::write(vol_dir.join(FETCHED_FILE), "{}").unwrap();
        let err = reconcile_owned_local_to_stopped(&vol_dir, &data_dir, vol_ulid)
            .expect_err("foreign fetched fork must refuse");
        assert!(matches!(err, ReconcileError::NoKeyShadow));
        // No destructive side-effects on refusal.
        assert!(vol_dir.join("volume.readonly").exists());
        assert!(vol_dir.join(FETCHED_FILE).exists());
    }

    #[test]
    fn reconcile_writes_stopped_marker_when_only_missing_part() {
        // Volume.key is present (operator manually placed it, or this
        // is an in-flight reconcile); only the stopped marker is missing.
        let (_tmp, data_dir, vol_dir, vol_ulid) = reconcile_scaffolding(false);
        std::fs::write(
            vol_dir.join(elide_core::signing::VOLUME_KEY_FILE),
            [0u8; 32],
        )
        .unwrap();
        let out = reconcile_owned_local_to_stopped(&vol_dir, &data_dir, vol_ulid).unwrap();
        assert_eq!(out, ReconcileOutcome::Reconciled);
        assert!(vol_dir.join(STOPPED_FILE).exists());
    }

    #[test]
    fn reconcile_refuses_when_daemon_running() {
        let (_tmp, data_dir, vol_dir, vol_ulid) = reconcile_scaffolding(false);
        std::fs::write(
            vol_dir.join(elide_core::signing::VOLUME_KEY_FILE),
            [0u8; 32],
        )
        .unwrap();
        // Simulate a running daemon by planting control.sock as a file
        // (real socket creation would require nix unix-socket support;
        // the helper only checks existence).
        std::fs::write(vol_dir.join("control.sock"), "").unwrap();
        let err = reconcile_owned_local_to_stopped(&vol_dir, &data_dir, vol_ulid)
            .expect_err("running daemon must refuse");
        assert!(matches!(err, ReconcileError::DaemonRunning));
    }
}
