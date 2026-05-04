//! Park markers — on-disk files that gate `supervisor::supervise`
//! against spawning a volume daemon for this directory.
//!
//! Two reasons today:
//!   - `volume.stopped`  → operator did `volume stop`
//!   - `volume.released` → released to S3; this host can no longer serve
//!
//! The supervisor parks whenever any of these is present.
//!
//! `volume.readonly` is handled one layer up: `daemon.rs` skips
//! spawning a supervisor for readonly volumes entirely.
//! `volume.importing` is handled in `discover_volumes`: volumes in the
//! import-write phase are filtered before any per-volume tasks spawn.

use std::path::Path;

use crate::volume_state::{RELEASED_FILE, STOPPED_FILE};

/// Reason a volume directory is parked. Released takes precedence
/// over Stopped to match `VolumeLifecycle::from_dir`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParkReason {
    Released { handoff_snapshot: String },
    Stopped,
}

impl ParkReason {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Released { .. } => "released",
            Self::Stopped => "stopped",
        }
    }
}

/// Returns the highest-precedence park reason for `vol_dir`, or
/// `None` if the volume should be supervised.
pub fn is_parked(vol_dir: &Path) -> Option<ParkReason> {
    let released = vol_dir.join(RELEASED_FILE);
    if released.exists() {
        let handoff_snapshot = std::fs::read_to_string(&released)
            .unwrap_or_default()
            .trim()
            .to_owned();
        return Some(ParkReason::Released { handoff_snapshot });
    }
    if vol_dir.join(STOPPED_FILE).exists() {
        return Some(ParkReason::Stopped);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn empty_dir_not_parked() {
        let d = TempDir::new().unwrap();
        assert_eq!(is_parked(d.path()), None);
    }

    #[test]
    fn stopped_marker_parks() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(STOPPED_FILE), "").unwrap();
        assert_eq!(is_parked(d.path()), Some(ParkReason::Stopped));
    }

    #[test]
    fn released_marker_parks() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(RELEASED_FILE), "01J9").unwrap();
        assert_eq!(
            is_parked(d.path()),
            Some(ParkReason::Released {
                handoff_snapshot: "01J9".to_owned()
            })
        );
    }

    #[test]
    fn released_takes_precedence_over_stopped() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(STOPPED_FILE), "").unwrap();
        std::fs::write(d.path().join(RELEASED_FILE), "01J9").unwrap();
        match is_parked(d.path()) {
            Some(ParkReason::Released { handoff_snapshot }) => {
                assert_eq!(handoff_snapshot, "01J9");
            }
            other => panic!("expected Released, got {other:?}"),
        }
    }

    #[test]
    fn released_marker_with_empty_body() {
        let d = TempDir::new().unwrap();
        std::fs::write(d.path().join(RELEASED_FILE), "").unwrap();
        assert_eq!(
            is_parked(d.path()),
            Some(ParkReason::Released {
                handoff_snapshot: String::new()
            })
        );
    }

    #[test]
    fn label_drops_payload() {
        assert_eq!(
            ParkReason::Released {
                handoff_snapshot: "01J9".to_owned()
            }
            .label(),
            "released"
        );
        assert_eq!(ParkReason::Stopped.label(), "stopped");
    }
}
