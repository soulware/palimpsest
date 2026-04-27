//! High-level lifecycle transitions for portable named volumes.
//!
//! Each function in this module mediates one state change of
//! `names/<name>` and translates the underlying conditional-PUT
//! mechanics into operator-level errors.
//!
//! See `docs/design-portable-live-volume.md`. Phase 2 of the rollout
//! lands `mark_stopped` here; subsequent phases add `mark_released`
//! (release verb) and `claim_started_from_released` (the start verb's
//! claim path).

use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;
use tracing::warn;
use ulid::Ulid;

use elide_core::name_record::{NameState, current_hostname};

use crate::name_store::{self, NameStoreError};
use crate::portable;

/// Errors from lifecycle transitions.
#[derive(Debug)]
pub enum LifecycleError {
    /// The store-level operation failed (transient I/O, parse error, etc.).
    Store(NameStoreError),
    /// `names/<name>` is held by another coordinator. The caller may
    /// retry with `--force-takeover` (Phase 3).
    OwnershipConflict { held_by: String },
    /// `names/<name>` is in a state that does not permit this transition
    /// (e.g. trying to mark `stopped` something already `released`).
    InvalidTransition { from: NameState, verb: &'static str },
}

impl std::fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(e) => write!(f, "{e}"),
            Self::OwnershipConflict { held_by } => write!(
                f,
                "name is held by another coordinator ({held_by}); use --force-takeover to override"
            ),
            Self::InvalidTransition { from, verb } => {
                write!(f, "cannot {verb} a name in state {from:?}")
            }
        }
    }
}

impl std::error::Error for LifecycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(e) => Some(e),
            _ => None,
        }
    }
}

impl From<NameStoreError> for LifecycleError {
    fn from(e: NameStoreError) -> Self {
        Self::Store(e)
    }
}

/// Outcome of a `mark_stopped` call.
#[derive(Debug)]
pub enum MarkStoppedOutcome {
    /// `names/<name>` was updated from `Live` to `Stopped`.
    Updated,
    /// `names/<name>` did not exist in the bucket — nothing to mark.
    /// The local stop has already happened; the caller may treat this
    /// as a soft success.
    Absent,
    /// `names/<name>` was already `Stopped` and owned by us.
    AlreadyStopped,
}

/// Transition `names/<name>` from `Live` to `Stopped`, retaining
/// ownership.
///
/// Read-modify-write under conditional PUT. If the record is owned by
/// another coordinator, returns `OwnershipConflict`. If the record is
/// absent, returns `Absent` so callers can decide whether to error or
/// proceed (a freshly-created volume that was never drained may not
/// yet have a record in the bucket).
///
/// Populates `coordinator_id`, `acquired_at`, and `hostname` if they
/// are not already set — this is the first lifecycle verb that has
/// the context to record them, so it backfills the Phase 1 gap.
pub async fn mark_stopped(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    root_key: &[u8; 32],
) -> Result<MarkStoppedOutcome, LifecycleError> {
    let coord_id = portable::format_coordinator_id(&portable::coordinator_id(root_key));

    let Some((mut record, version)) = name_store::read_name_record(store, name).await? else {
        return Ok(MarkStoppedOutcome::Absent);
    };

    // Ownership check: the record must be owned by us, or unowned (Phase 1
    // records have no coordinator_id and any coordinator may claim).
    if let Some(existing) = record.coordinator_id.as_deref()
        && existing != coord_id
    {
        return Err(LifecycleError::OwnershipConflict {
            held_by: existing.to_owned(),
        });
    }

    match record.state {
        NameState::Live => {}
        NameState::Stopped => return Ok(MarkStoppedOutcome::AlreadyStopped),
        NameState::Released => {
            return Err(LifecycleError::InvalidTransition {
                from: NameState::Released,
                verb: "stop",
            });
        }
    }

    record.state = NameState::Stopped;
    if record.coordinator_id.is_none() {
        record.coordinator_id = Some(coord_id);
    }
    if record.acquired_at.is_none() {
        record.acquired_at = Some(chrono::Utc::now().to_rfc3339());
    }
    if record.hostname.is_none() {
        record.hostname = current_hostname();
    }

    name_store::update_name_record(store, name, &record, version).await?;
    Ok(MarkStoppedOutcome::Updated)
}

/// Outcome of a `mark_released` call.
#[derive(Debug)]
pub enum MarkReleasedOutcome {
    /// `names/<name>` was updated to `Released`, recording
    /// `handoff_snapshot` so the next claimant can fork from it.
    Updated,
    /// `names/<name>` did not exist in the bucket. The release verb
    /// requires a published record (something to hand off); callers
    /// should treat this as an error.
    Absent,
    /// `names/<name>` was already `Released` and matched our caller.
    /// Idempotent success.
    AlreadyReleased,
}

/// Transition `names/<name>` from `Live` or `Stopped` to `Released`,
/// recording the handoff snapshot so the next claimant can fork from
/// it. Ownership (`coordinator_id`) is preserved on the record as
/// historical metadata; the next `volume start` will overwrite it.
///
/// This is the cross-coordinator-handoff verb. Callers must already
/// have:
///   1. drained the volume's WAL,
///   2. published a snapshot covering everything drained,
///   3. recorded the resulting `snap_ulid` here as `handoff_snapshot`.
///
/// The conditional PUT ensures no other coordinator has mutated the
/// record between our read and our write.
pub async fn mark_released(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    root_key: &[u8; 32],
    handoff_snapshot: Ulid,
) -> Result<MarkReleasedOutcome, LifecycleError> {
    let coord_id = portable::format_coordinator_id(&portable::coordinator_id(root_key));

    let Some((mut record, version)) = name_store::read_name_record(store, name).await? else {
        return Ok(MarkReleasedOutcome::Absent);
    };

    if let Some(existing) = record.coordinator_id.as_deref()
        && existing != coord_id
    {
        return Err(LifecycleError::OwnershipConflict {
            held_by: existing.to_owned(),
        });
    }

    match record.state {
        NameState::Live | NameState::Stopped => {}
        NameState::Released => return Ok(MarkReleasedOutcome::AlreadyReleased),
    }

    record.state = NameState::Released;
    record.handoff_snapshot = Some(handoff_snapshot);
    if record.coordinator_id.is_none() {
        record.coordinator_id = Some(coord_id);
    }
    if record.acquired_at.is_none() {
        record.acquired_at = Some(chrono::Utc::now().to_rfc3339());
    }
    if record.hostname.is_none() {
        record.hostname = current_hostname();
    }

    name_store::update_name_record(store, name, &record, version).await?;
    Ok(MarkReleasedOutcome::Updated)
}

/// Outcome of a `mark_live` call (the local-resume path of `volume start`).
#[derive(Debug)]
pub enum MarkLiveOutcome {
    /// `names/<name>` was updated from `Stopped` to `Live`.
    Resumed,
    /// `names/<name>` did not exist; the local start may proceed
    /// without an S3 update (volume not yet drained).
    Absent,
    /// `names/<name>` was already `Live` and owned by us.
    AlreadyLive,
    /// `names/<name>` is `Released`; the caller needs the claim-from-
    /// released path, not local resume.
    Released,
}

/// Transition `names/<name>` from `Stopped` to `Live` (local resume).
/// Refuses on ownership conflict or if the record is already `Released`
/// (callers must take the claim-from-released path instead).
pub async fn mark_live(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    root_key: &[u8; 32],
) -> Result<MarkLiveOutcome, LifecycleError> {
    let coord_id = portable::format_coordinator_id(&portable::coordinator_id(root_key));

    let Some((mut record, version)) = name_store::read_name_record(store, name).await? else {
        return Ok(MarkLiveOutcome::Absent);
    };

    if let Some(existing) = record.coordinator_id.as_deref()
        && existing != coord_id
    {
        return Err(LifecycleError::OwnershipConflict {
            held_by: existing.to_owned(),
        });
    }

    match record.state {
        NameState::Live => return Ok(MarkLiveOutcome::AlreadyLive),
        NameState::Stopped => {}
        NameState::Released => return Ok(MarkLiveOutcome::Released),
    }

    record.state = NameState::Live;
    if record.coordinator_id.is_none() {
        record.coordinator_id = Some(coord_id);
    }
    if record.acquired_at.is_none() {
        record.acquired_at = Some(chrono::Utc::now().to_rfc3339());
    }
    if record.hostname.is_none() {
        record.hostname = current_hostname();
    }

    name_store::update_name_record(store, name, &record, version).await?;
    Ok(MarkLiveOutcome::Resumed)
}

/// Reconcile the local `volume.stopped` marker against
/// `names/<name>.state`. S3 is authoritative — the local marker is a
/// host-side cache.
///
/// Behaviour, scoped to records this coordinator owns:
/// - `state == Stopped` and marker absent → write the marker so the
///   supervisor does not relaunch the daemon.
/// - `state == Live` and marker present → remove the marker so the
///   supervisor can launch the daemon.
/// - All other cases (foreign owner, `Released`, no record at all)
///   are left to the lifecycle verbs to handle. Reconciliation never
///   acts on records owned by another coordinator.
///
/// Best-effort: errors are logged and the function returns `Ok(())`.
/// Reconciliation drift is a soft inconsistency — the next operator
/// `volume start` / `volume stop` resolves it cleanly.
pub async fn reconcile_marker(
    store: &Arc<dyn ObjectStore>,
    vol_dir: &Path,
    volume_name: &str,
    root_key: &[u8; 32],
) {
    let coord_id = portable::format_coordinator_id(&portable::coordinator_id(root_key));

    let record = match name_store::read_name_record(store, volume_name).await {
        Ok(Some((r, _))) => r,
        Ok(None) => return,
        Err(e) => {
            warn!("[reconcile {volume_name}] reading names/<name>: {e}");
            return;
        }
    };

    // Reconciliation only acts on records this coordinator owns.
    let owned_by_us = record
        .coordinator_id
        .as_deref()
        .is_some_and(|id| id == coord_id);
    if !owned_by_us {
        return;
    }

    let marker = vol_dir.join("volume.stopped");
    let marker_present = marker.exists();

    match (record.state, marker_present) {
        (NameState::Stopped, false) => {
            if let Err(e) = std::fs::write(&marker, "") {
                warn!("[reconcile {volume_name}] writing volume.stopped: {e}");
            } else {
                tracing::info!(
                    "[reconcile {volume_name}] S3 says Stopped; wrote volume.stopped marker"
                );
            }
        }
        (NameState::Live, true) => {
            if let Err(e) = std::fs::remove_file(&marker) {
                warn!("[reconcile {volume_name}] removing volume.stopped: {e}");
            } else {
                tracing::info!(
                    "[reconcile {volume_name}] S3 says Live; removed volume.stopped marker"
                );
            }
        }
        // (Stopped, true) and (Live, false) are aligned — no action.
        // (Released, _) is left to `volume start --claim` semantics.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::name_store::create_name_record;
    use elide_core::name_record::NameRecord;
    use object_store::memory::InMemory;
    use ulid::Ulid;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn sample_ulid() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    fn key_a() -> [u8; 32] {
        [0xABu8; 32]
    }

    fn key_b() -> [u8; 32] {
        [0xCDu8; 32]
    }

    #[tokio::test]
    async fn mark_stopped_returns_absent_when_record_missing() {
        let s = store();
        let r = mark_stopped(&s, "missing", &key_a()).await.unwrap();
        assert!(matches!(r, MarkStoppedOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_stopped_flips_live_to_stopped() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        let outcome = mark_stopped(&s, "vol", &key_a()).await.unwrap();
        assert!(matches!(outcome, MarkStoppedOutcome::Updated));

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Stopped);
        assert!(got.coordinator_id.is_some(), "coordinator_id backfilled");
        assert!(got.acquired_at.is_some(), "acquired_at backfilled");
    }

    #[tokio::test]
    async fn mark_stopped_is_idempotent_for_same_owner() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        mark_stopped(&s, "vol", &key_a()).await.unwrap();
        let outcome = mark_stopped(&s, "vol", &key_a()).await.unwrap();
        assert!(matches!(outcome, MarkStoppedOutcome::AlreadyStopped));
    }

    #[tokio::test]
    async fn mark_stopped_refuses_other_owners_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        // First coordinator A claims via stop.
        mark_stopped(&s, "vol", &key_a()).await.unwrap();

        // Coordinator B tries to flip an A-owned record.
        let err = mark_stopped(&s, "vol", &key_b())
            .await
            .expect_err("B must be refused on A-owned record");
        assert!(matches!(err, LifecycleError::OwnershipConflict { .. }));
    }

    #[tokio::test]
    async fn mark_stopped_refuses_released_record() {
        let s = store();
        let mut rec = NameRecord::live_minimal(sample_ulid());
        rec.state = NameState::Released;
        create_name_record(&s, "vol", &rec).await.unwrap();

        let err = mark_stopped(&s, "vol", &key_a())
            .await
            .expect_err("released → stopped is not a valid transition");
        assert!(matches!(
            err,
            LifecycleError::InvalidTransition {
                from: NameState::Released,
                verb: "stop"
            }
        ));
    }

    fn snap() -> Ulid {
        Ulid::from_string("01J1111111111111111111111V").unwrap()
    }

    #[tokio::test]
    async fn mark_released_returns_absent_when_record_missing() {
        let s = store();
        let r = mark_released(&s, "missing", &key_a(), snap())
            .await
            .unwrap();
        assert!(matches!(r, MarkReleasedOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_released_flips_live_to_released_with_handoff() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        let outcome = mark_released(&s, "vol", &key_a(), snap()).await.unwrap();
        assert!(matches!(outcome, MarkReleasedOutcome::Updated));

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Released);
        assert_eq!(got.handoff_snapshot, Some(snap()));
        assert!(got.coordinator_id.is_some(), "coordinator_id backfilled");
    }

    #[tokio::test]
    async fn mark_released_flips_stopped_to_released() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_stopped(&s, "vol", &key_a()).await.unwrap();

        let outcome = mark_released(&s, "vol", &key_a(), snap()).await.unwrap();
        assert!(matches!(outcome, MarkReleasedOutcome::Updated));

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Released);
        assert_eq!(got.handoff_snapshot, Some(snap()));
    }

    #[tokio::test]
    async fn mark_released_is_idempotent_for_same_owner() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        mark_released(&s, "vol", &key_a(), snap()).await.unwrap();
        let outcome = mark_released(&s, "vol", &key_a(), snap()).await.unwrap();
        assert!(matches!(outcome, MarkReleasedOutcome::AlreadyReleased));
    }

    #[tokio::test]
    async fn mark_released_refuses_other_owners_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        // A claims via stop.
        mark_stopped(&s, "vol", &key_a()).await.unwrap();

        // B tries to release A's record.
        let err = mark_released(&s, "vol", &key_b(), snap())
            .await
            .expect_err("B must be refused on A-owned record");
        assert!(matches!(err, LifecycleError::OwnershipConflict { .. }));
    }

    #[tokio::test]
    async fn mark_live_returns_absent_when_record_missing() {
        let s = store();
        let r = mark_live(&s, "missing", &key_a()).await.unwrap();
        assert!(matches!(r, MarkLiveOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_live_resumes_stopped_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_stopped(&s, "vol", &key_a()).await.unwrap();

        let outcome = mark_live(&s, "vol", &key_a()).await.unwrap();
        assert!(matches!(outcome, MarkLiveOutcome::Resumed));

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Live);
    }

    #[tokio::test]
    async fn mark_live_is_idempotent_when_already_live() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        // Phase 1 records start as Live with coordinator_id=None;
        // mark_stopped→mark_live cycles through both states with our id set.
        mark_stopped(&s, "vol", &key_a()).await.unwrap();
        mark_live(&s, "vol", &key_a()).await.unwrap();

        let outcome = mark_live(&s, "vol", &key_a()).await.unwrap();
        assert!(matches!(outcome, MarkLiveOutcome::AlreadyLive));
    }

    #[tokio::test]
    async fn mark_live_signals_released_for_claim_path() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_released(&s, "vol", &key_a(), snap()).await.unwrap();

        // Even from the original owner, mark_live on a Released record
        // is not local-resume; the claim-from-released path is required.
        let outcome = mark_live(&s, "vol", &key_a()).await.unwrap();
        assert!(matches!(outcome, MarkLiveOutcome::Released));
    }

    #[tokio::test]
    async fn mark_live_refuses_other_owners_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_stopped(&s, "vol", &key_a()).await.unwrap();

        let err = mark_live(&s, "vol", &key_b())
            .await
            .expect_err("B must be refused on A-owned record");
        assert!(matches!(err, LifecycleError::OwnershipConflict { .. }));
    }

    #[tokio::test]
    async fn reconcile_marker_writes_marker_when_s3_says_stopped() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_stopped(&s, "vol", &key_a()).await.unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let vol_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&vol_dir).unwrap();

        reconcile_marker(&s, &vol_dir, "vol", &key_a()).await;
        assert!(vol_dir.join("volume.stopped").exists());
    }

    #[tokio::test]
    async fn reconcile_marker_removes_marker_when_s3_says_live() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        // Cycle through stop → start so coordinator_id is set on the
        // record (Phase 1 records start as Live with no coordinator_id,
        // and reconcile only acts on records *we* own).
        mark_stopped(&s, "vol", &key_a()).await.unwrap();
        mark_live(&s, "vol", &key_a()).await.unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let vol_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&vol_dir).unwrap();
        std::fs::write(vol_dir.join("volume.stopped"), "").unwrap();

        reconcile_marker(&s, &vol_dir, "vol", &key_a()).await;
        assert!(!vol_dir.join("volume.stopped").exists());
    }

    #[tokio::test]
    async fn reconcile_marker_ignores_foreign_owned_records() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        // A claims via stop.
        mark_stopped(&s, "vol", &key_a()).await.unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let vol_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&vol_dir).unwrap();

        // B reconciles; A's record must not affect B's local state.
        reconcile_marker(&s, &vol_dir, "vol", &key_b()).await;
        assert!(!vol_dir.join("volume.stopped").exists());
    }

    #[tokio::test]
    async fn reconcile_marker_no_op_when_record_absent() {
        let s = store();
        let tmp = tempfile::TempDir::new().unwrap();
        let vol_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&vol_dir).unwrap();

        reconcile_marker(&s, &vol_dir, "vol", &key_a()).await;
        assert!(!vol_dir.join("volume.stopped").exists());
    }
}
