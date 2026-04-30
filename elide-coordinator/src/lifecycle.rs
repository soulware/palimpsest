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

use elide_core::name_record::{
    Lifecycle, NameRecord, NameState, TransitionCheck, current_hostname,
};

use crate::name_store::{self, NameStoreError};

/// Errors from lifecycle transitions.
#[derive(Debug)]
pub enum LifecycleError {
    /// The store-level operation failed (transient I/O, parse error, etc.).
    Store(NameStoreError),
    /// `names/<name>` is held by another coordinator. The caller may
    /// retry after `volume release --force` (Phase 3).
    OwnershipConflict { held_by: String },
    /// `names/<name>` is in a state that does not permit this transition
    /// (e.g. trying to mark `stopped` something already `released`).
    InvalidTransition { from: NameState, verb: Lifecycle },
}

/// Refuse if `record` is owned by a different coordinator. A `None`
/// `coordinator_id` is treated as unowned (Phase 1 record without owner
/// plumbing) and any coordinator may proceed.
fn check_owner(record: &NameRecord, coord_id: &str) -> Result<(), LifecycleError> {
    if let Some(existing) = record.coordinator_id.as_deref()
        && existing != coord_id
    {
        return Err(LifecycleError::OwnershipConflict {
            held_by: existing.to_owned(),
        });
    }
    Ok(())
}

/// Backfill owner-identity fields if absent. Phase 1 records arrive
/// without `coordinator_id`/`claimed_at`/`hostname`; the first lifecycle
/// verb that has the context populates them in place.
fn ensure_owner_fields(record: &mut NameRecord, coord_id: &str) {
    if record.coordinator_id.is_none() {
        record.coordinator_id = Some(coord_id.to_owned());
    }
    if record.claimed_at.is_none() {
        record.claimed_at = Some(chrono::Utc::now().to_rfc3339());
    }
    if record.hostname.is_none() {
        record.hostname = current_hostname();
    }
}

impl std::fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(e) => write!(f, "{e}"),
            Self::OwnershipConflict { held_by } => write!(
                f,
                "name is held by another coordinator ({held_by}); run `volume release --force` to override"
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
/// Populates `coordinator_id`, `claimed_at`, and `hostname` if they
/// are not already set — this is the first lifecycle verb that has
/// the context to record them, so it backfills the Phase 1 gap.
pub async fn mark_stopped(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    coord_id: &str,
) -> Result<MarkStoppedOutcome, LifecycleError> {
    let Some((mut record, version)) = name_store::read_name_record(store, name).await? else {
        return Ok(MarkStoppedOutcome::Absent);
    };

    check_owner(&record, coord_id)?;

    match record.state.check_transition(Lifecycle::Stop) {
        TransitionCheck::Proceed => {}
        TransitionCheck::Idempotent => return Ok(MarkStoppedOutcome::AlreadyStopped),
        TransitionCheck::Reroute | TransitionCheck::Refuse => {
            return Err(LifecycleError::InvalidTransition {
                from: record.state,
                verb: Lifecycle::Stop,
            });
        }
    }

    record.state = NameState::Stopped;
    ensure_owner_fields(&mut record, coord_id);

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
/// it. The owner-identity fields (`coordinator_id`, `claimed_at`,
/// `hostname`) are **cleared** so the record matches its semantics:
/// `Released` means "no current owner", and the populated fields
/// would otherwise describe the previous owner — confusing for
/// operators inspecting the bucket. The next `mark_claimed` populates
/// them fresh for the new owner.
///
/// This is the cross-coordinator-handoff verb. Callers must already
/// have:
///   1. drained the volume's WAL,
///   2. published a snapshot covering everything drained (an empty
///      volume publishes a snapshot with zero entries, which is fine —
///      the claim path forks from it as a fresh empty root),
///   3. recorded the resulting `snap_ulid` here as `handoff_snapshot`.
///
/// The conditional PUT ensures no other coordinator has mutated the
/// record between our read and our write.
pub async fn mark_released(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    coord_id: &str,
    handoff_snapshot: Ulid,
) -> Result<MarkReleasedOutcome, LifecycleError> {
    let Some((mut record, version)) = name_store::read_name_record(store, name).await? else {
        return Ok(MarkReleasedOutcome::Absent);
    };

    check_owner(&record, coord_id)?;

    match record.state.check_transition(Lifecycle::Release) {
        TransitionCheck::Proceed => {}
        TransitionCheck::Idempotent => return Ok(MarkReleasedOutcome::AlreadyReleased),
        TransitionCheck::Reroute | TransitionCheck::Refuse => {
            return Err(LifecycleError::InvalidTransition {
                from: record.state,
                verb: Lifecycle::Release,
            });
        }
    }

    record.state = NameState::Released;
    record.handoff_snapshot = Some(handoff_snapshot);
    record.coordinator_id = None;
    record.claimed_at = None;
    record.hostname = None;

    name_store::update_name_record(store, name, &record, version).await?;
    Ok(MarkReleasedOutcome::Updated)
}

/// Outcome of `mark_released_force`.
#[derive(Debug)]
pub enum ForceReleaseOutcome {
    /// `names/<name>` was unconditionally rewritten to the new state.
    /// Carries the dead fork's `vol_ulid` so callers can surface it
    /// (e.g. for operator output).
    Overwritten { dead_vol_ulid: Ulid },
    /// `names/<name>` did not exist in the bucket. Force-release of a
    /// non-existent name is meaningless — there is no dead fork to
    /// recover from.
    Absent,
    /// `names/<name>` exists but is in a state where force-release
    /// must refuse: `Released` or `Readonly`. Force is
    /// the override path for an unreachable owner; it must not flip
    /// states whose semantics make no sense for the verb.
    InvalidState { observed: NameState },
}

/// **Unconditionally** rewrite `names/<name>` to `Released`,
/// recording `handoff_snapshot` for the next claimant. Used by
/// `volume release --force` when the previous owner is unreachable
/// (machine gone, `coordinator.key` deleted, partition with no
/// expected recovery).
///
/// No ownership check, no conditional PUT — overriding foreign
/// ownership is the whole point of the verb. Callers must already
/// have published the synthesised handoff snapshot at
/// `by_id/<dead_vol_ulid>/snapshots/<handoff_snapshot>.manifest` via
/// `recovery::mint_and_publish_synthesised_snapshot`. The dead
/// fork's `vol_ulid` is preserved on the record so claimants know
/// which prefix to fork from.
///
/// Refuses to operate on `Released` (already released — no-op) or
/// `Readonly` (no exclusive owner to override).
pub async fn mark_released_force(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    handoff_snapshot: Ulid,
) -> Result<ForceReleaseOutcome, LifecycleError> {
    let Some((current, _version)) = name_store::read_name_record(store, name).await? else {
        return Ok(ForceReleaseOutcome::Absent);
    };

    match current.state.check_transition(Lifecycle::ForceRelease) {
        TransitionCheck::Proceed => {}
        TransitionCheck::Idempotent | TransitionCheck::Reroute | TransitionCheck::Refuse => {
            return Ok(ForceReleaseOutcome::InvalidState {
                observed: current.state,
            });
        }
    }

    let new = elide_core::name_record::NameRecord {
        version: elide_core::name_record::NameRecord::CURRENT_VERSION,
        // Same dead fork — the synthesised snapshot lives under this
        // prefix, and the next claimant forks from it.
        vol_ulid: current.vol_ulid,
        // Released → no current owner.
        coordinator_id: None,
        state: NameState::Released,
        // Preserve provenance so the chain stays walkable.
        parent: current.parent,
        claimed_at: None,
        hostname: None,
        handoff_snapshot: Some(handoff_snapshot),
    };

    name_store::overwrite_name_record(store, name, &new).await?;
    Ok(ForceReleaseOutcome::Overwritten {
        dead_vol_ulid: current.vol_ulid,
    })
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
    coord_id: &str,
) -> Result<MarkLiveOutcome, LifecycleError> {
    let Some((mut record, version)) = name_store::read_name_record(store, name).await? else {
        return Ok(MarkLiveOutcome::Absent);
    };

    check_owner(&record, coord_id)?;

    match record.state.check_transition(Lifecycle::Start) {
        TransitionCheck::Proceed => {}
        TransitionCheck::Idempotent => return Ok(MarkLiveOutcome::AlreadyLive),
        TransitionCheck::Reroute => return Ok(MarkLiveOutcome::Released),
        TransitionCheck::Refuse => {
            return Err(LifecycleError::InvalidTransition {
                from: record.state,
                verb: Lifecycle::Start,
            });
        }
    }

    record.state = NameState::Live;
    ensure_owner_fields(&mut record, coord_id);

    name_store::update_name_record(store, name, &record, version).await?;
    Ok(MarkLiveOutcome::Resumed)
}

/// Outcome of a `mark_reclaimed_local` call.
#[derive(Debug)]
pub enum MarkReclaimedLocalOutcome {
    /// `names/<name>` was updated from `Released` to `Live`, keeping
    /// the existing `vol_ulid`. The caller (this coordinator) had the
    /// matching local fork on disk so no new fork is needed.
    Reclaimed,
    /// `names/<name>` did not exist in the bucket. The caller should
    /// fall back to the local-only start path.
    Absent,
    /// `names/<name>` is not `Released` — the caller asked for the
    /// in-place reclaim path but the record is in some other state.
    /// Includes the observed state and vol_ulid so the caller can
    /// route to the right verb.
    NotReleased {
        observed_state: NameState,
        observed_vol_ulid: Ulid,
    },
    /// `names/<name>` is `Released` but its `vol_ulid` does not match
    /// the local fork the caller offered. The caller must take the
    /// cross-coordinator claim path (mint a new fork, `mark_claimed`),
    /// because the released ancestor is foreign content.
    ForkMismatch {
        local_vol_ulid: Ulid,
        released_vol_ulid: Ulid,
    },
}

/// In-place reclaim of a `Released` name when the local fork is the
/// same one the record points at — i.e. the previous owner was *us*
/// and the local fork is still on disk. Transitions `Released` to the
/// requested `target_state` (`Live` for `start --claim`, `Stopped` for
/// `claim`), keeping the existing `vol_ulid`, repopulating
/// `coordinator_id`/`claimed_at`/`hostname`, and clearing
/// `handoff_snapshot`.
///
/// Cross-coordinator claim (different host, no local fork) still
/// goes through `mark_claimed` which mints a new `vol_ulid`.
pub async fn mark_reclaimed_local(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    coord_id: &str,
    local_vol_ulid: Ulid,
    target_state: NameState,
) -> Result<MarkReclaimedLocalOutcome, LifecycleError> {
    debug_assert!(matches!(target_state, NameState::Live | NameState::Stopped));

    let Some((mut record, version)) = name_store::read_name_record(store, name).await? else {
        return Ok(MarkReclaimedLocalOutcome::Absent);
    };

    match record.state.check_transition(Lifecycle::Claim) {
        TransitionCheck::Proceed => {}
        TransitionCheck::Idempotent | TransitionCheck::Reroute | TransitionCheck::Refuse => {
            return Ok(MarkReclaimedLocalOutcome::NotReleased {
                observed_state: record.state,
                observed_vol_ulid: record.vol_ulid,
            });
        }
    }

    if record.vol_ulid != local_vol_ulid {
        return Ok(MarkReclaimedLocalOutcome::ForkMismatch {
            local_vol_ulid,
            released_vol_ulid: record.vol_ulid,
        });
    }

    record.state = target_state;
    record.coordinator_id = Some(coord_id.to_owned());
    record.claimed_at = Some(chrono::Utc::now().to_rfc3339());
    record.hostname = current_hostname();
    record.handoff_snapshot = None;

    name_store::update_name_record(store, name, &record, version).await?;
    Ok(MarkReclaimedLocalOutcome::Reclaimed)
}

/// Outcome of a `mark_initial` call (the create-time claim of a fresh
/// name).
#[derive(Debug)]
pub enum MarkInitialOutcome {
    /// `names/<name>` was newly created and now points at `vol_ulid`,
    /// owned by this coordinator, in state `Live`.
    Claimed,
    /// `names/<name>` already exists. The caller should refuse the
    /// create — the name is taken (possibly by us from a previous
    /// session, possibly by another coordinator). Includes the existing
    /// owner string (if any) so the caller can produce a clean error.
    AlreadyExists {
        existing_vol_ulid: Ulid,
        existing_state: NameState,
        existing_owner: Option<String>,
    },
}

/// Atomically claim a fresh name in S3 at `volume create` time.
///
/// Issues `If-None-Match: *` so two coordinators racing to create the
/// same name resolve cleanly: one wins, the other gets `AlreadyExists`.
/// On success the record is fully populated — `coordinator_id`,
/// `claimed_at`, and `hostname` are all set up front, so the volume
/// has a real owner from the moment its directory exists locally
/// (no Phase-1 "ownership-blank-until-first-lifecycle-verb" gap).
pub async fn mark_initial(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    coord_id: &str,
    vol_ulid: Ulid,
) -> Result<MarkInitialOutcome, LifecycleError> {
    // If a record already exists, surface its details so the caller can
    // produce a useful error. We do this read first so the common
    // success path (no record) issues exactly one PUT, but a name
    // collision returns rich information without an extra round trip.
    if let Some((existing, _)) = name_store::read_name_record(store, name).await? {
        return Ok(MarkInitialOutcome::AlreadyExists {
            existing_vol_ulid: existing.vol_ulid,
            existing_state: existing.state,
            existing_owner: existing.coordinator_id,
        });
    }

    let record = elide_core::name_record::NameRecord {
        version: elide_core::name_record::NameRecord::CURRENT_VERSION,
        vol_ulid,
        coordinator_id: Some(coord_id.to_owned()),
        state: NameState::Live,
        parent: None,
        claimed_at: Some(chrono::Utc::now().to_rfc3339()),
        hostname: current_hostname(),
        handoff_snapshot: None,
    };

    match name_store::create_name_record(store, name, &record).await {
        Ok(_) => Ok(MarkInitialOutcome::Claimed),
        // Lost a race against another coordinator that created the
        // same name between our read and our conditional create.
        // Re-read so the caller sees the winning record.
        Err(NameStoreError::PreconditionFailed) => {
            match name_store::read_name_record(store, name).await? {
                Some((existing, _)) => Ok(MarkInitialOutcome::AlreadyExists {
                    existing_vol_ulid: existing.vol_ulid,
                    existing_state: existing.state,
                    existing_owner: existing.coordinator_id,
                }),
                // Vanished between the failed create and the re-read
                // (TTL? GC?). Surface as a transient store error so
                // the caller can retry.
                None => Err(LifecycleError::Store(NameStoreError::PreconditionFailed)),
            }
        }
        Err(e) => Err(LifecycleError::Store(e)),
    }
}

/// Atomically claim a fresh name in S3 for a **readonly** import.
///
/// Same conditional-create mechanics as `mark_initial` (one writer
/// wins on `If-None-Match: *`), but the resulting record carries
/// `state = Readonly` and *no* coordinator identity:
/// `coordinator_id`, `claimed_at`, and `hostname` are all `None`,
/// reflecting that imports have no exclusive owner. Multiple
/// coordinators may pull and serve the same name concurrently
/// (see design doc § "Readonly names").
///
/// Takes no `root_key` — there is no owner to identify. The
/// conditional create still gives uniqueness: two coordinators
/// racing to import the same name resolve cleanly (one wins,
/// the other gets `AlreadyExists`).
pub async fn mark_initial_readonly(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    vol_ulid: Ulid,
) -> Result<MarkInitialOutcome, LifecycleError> {
    if let Some((existing, _)) = name_store::read_name_record(store, name).await? {
        return Ok(MarkInitialOutcome::AlreadyExists {
            existing_vol_ulid: existing.vol_ulid,
            existing_state: existing.state,
            existing_owner: existing.coordinator_id,
        });
    }

    let record = elide_core::name_record::NameRecord {
        version: elide_core::name_record::NameRecord::CURRENT_VERSION,
        vol_ulid,
        coordinator_id: None,
        state: NameState::Readonly,
        parent: None,
        claimed_at: None,
        hostname: None,
        handoff_snapshot: None,
    };

    match name_store::create_name_record(store, name, &record).await {
        Ok(_) => Ok(MarkInitialOutcome::Claimed),
        Err(NameStoreError::PreconditionFailed) => {
            match name_store::read_name_record(store, name).await? {
                Some((existing, _)) => Ok(MarkInitialOutcome::AlreadyExists {
                    existing_vol_ulid: existing.vol_ulid,
                    existing_state: existing.state,
                    existing_owner: existing.coordinator_id,
                }),
                None => Err(LifecycleError::Store(NameStoreError::PreconditionFailed)),
            }
        }
        Err(e) => Err(LifecycleError::Store(e)),
    }
}

/// Outcome of a `mark_claimed` call (the claim-from-released path of
/// `volume claim`).
#[derive(Debug)]
pub enum MarkClaimedOutcome {
    /// `names/<name>` was updated from `Released` to the requested
    /// state (`Live` or `Stopped`), with `vol_ulid` rewritten to
    /// `new_vol_ulid` and the previous fork's
    /// `<vol_ulid>/<handoff_snapshot>` recorded as `parent`.
    Claimed,
    /// `names/<name>` did not exist in the bucket; nothing to claim.
    /// Callers handling `volume claim` should treat this as the
    /// "no record yet" path.
    Absent,
    /// `names/<name>` is not in a claimable state. Includes the
    /// observed state so callers can produce a clear error.
    NotReleased { observed: NameState },
}

/// Atomically claim a `Released` name for this coordinator and rebind
/// it to a fresh fork. Conditional PUT under the ETag observed at
/// read time, so two coordinators racing to claim the same released
/// name resolve cleanly: one wins, the other gets `PreconditionFailed`
/// (returned as `LifecycleError::Store(NameStoreError::PreconditionFailed)`).
///
/// `target_state` selects the post-claim state: `Live` for
/// `volume start --claim` (composed claim+start), `Stopped` for
/// `volume claim` (claim only, leave daemon halted). Any other state
/// is a programmer error.
///
/// Caller responsibilities (orchestrated externally — typically the
/// CLI for `volume claim`):
///   1. Mint `new_vol_ulid` locally.
///   2. Materialise a fork at `by_id/<new_vol_ulid>/` whose provenance
///      points at the released ancestor's
///      `<previous_vol_ulid>/<handoff_snapshot>`.
///   3. Call `mark_claimed` to atomically rebind the name.
///   4. (Optional) Spawn the volume daemon.
pub async fn mark_claimed(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    coord_id: &str,
    new_vol_ulid: Ulid,
    target_state: NameState,
) -> Result<MarkClaimedOutcome, LifecycleError> {
    debug_assert!(matches!(target_state, NameState::Live | NameState::Stopped));

    let Some((existing, version)) = name_store::read_name_record(store, name).await? else {
        return Ok(MarkClaimedOutcome::Absent);
    };

    match existing.state.check_transition(Lifecycle::Claim) {
        TransitionCheck::Proceed => {}
        TransitionCheck::Idempotent | TransitionCheck::Reroute | TransitionCheck::Refuse => {
            return Ok(MarkClaimedOutcome::NotReleased {
                observed: existing.state,
            });
        }
    }

    let parent_pin = existing
        .handoff_snapshot
        .map(|snap| format!("{}/{}", existing.vol_ulid, snap));

    let new_record = elide_core::name_record::NameRecord {
        version: elide_core::name_record::NameRecord::CURRENT_VERSION,
        vol_ulid: new_vol_ulid,
        coordinator_id: Some(coord_id.to_owned()),
        state: target_state,
        parent: parent_pin,
        claimed_at: Some(chrono::Utc::now().to_rfc3339()),
        hostname: current_hostname(),
        handoff_snapshot: None,
    };

    name_store::update_name_record(store, name, &new_record, version).await?;
    Ok(MarkClaimedOutcome::Claimed)
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
    coord_id: &str,
) {
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
    use crate::portable;
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

    fn id_a() -> String {
        portable::format_coordinator_id(&portable::coordinator_id(&key_a()))
    }

    fn id_b() -> String {
        portable::format_coordinator_id(&portable::coordinator_id(&key_b()))
    }

    #[tokio::test]
    async fn mark_stopped_returns_absent_when_record_missing() {
        let s = store();
        let r = mark_stopped(&s, "missing", &id_a()).await.unwrap();
        assert!(matches!(r, MarkStoppedOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_stopped_flips_live_to_stopped() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        let outcome = mark_stopped(&s, "vol", &id_a()).await.unwrap();
        assert!(matches!(outcome, MarkStoppedOutcome::Updated));

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Stopped);
        assert!(got.coordinator_id.is_some(), "coordinator_id backfilled");
        assert!(got.claimed_at.is_some(), "claimed_at backfilled");
    }

    #[tokio::test]
    async fn mark_stopped_is_idempotent_for_same_owner() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        mark_stopped(&s, "vol", &id_a()).await.unwrap();
        let outcome = mark_stopped(&s, "vol", &id_a()).await.unwrap();
        assert!(matches!(outcome, MarkStoppedOutcome::AlreadyStopped));
    }

    #[tokio::test]
    async fn mark_stopped_refuses_other_owners_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        // First coordinator A claims via stop.
        mark_stopped(&s, "vol", &id_a()).await.unwrap();

        // Coordinator B tries to flip an A-owned record.
        let err = mark_stopped(&s, "vol", &id_b())
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

        let err = mark_stopped(&s, "vol", &id_a())
            .await
            .expect_err("released → stopped is not a valid transition");
        assert!(matches!(
            err,
            LifecycleError::InvalidTransition {
                from: NameState::Released,
                verb: Lifecycle::Stop,
            }
        ));
    }

    fn snap() -> Ulid {
        Ulid::from_string("01J1111111111111111111111V").unwrap()
    }

    #[tokio::test]
    async fn mark_released_returns_absent_when_record_missing() {
        let s = store();
        let r = mark_released(&s, "missing", &id_a(), snap()).await.unwrap();
        assert!(matches!(r, MarkReleasedOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_released_flips_live_to_released_with_handoff() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        let outcome = mark_released(&s, "vol", &id_a(), snap()).await.unwrap();
        assert!(matches!(outcome, MarkReleasedOutcome::Updated));

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Released);
        assert_eq!(got.handoff_snapshot, Some(snap()));
        // Owner-identity fields are cleared on release so the record's
        // populated fields agree with its semantics: `Released` ==
        // "no current owner".
        assert!(got.coordinator_id.is_none(), "coordinator_id cleared");
        assert!(got.claimed_at.is_none(), "claimed_at cleared");
        assert!(got.hostname.is_none(), "hostname cleared");
    }

    #[tokio::test]
    async fn mark_released_clears_owner_identity_from_populated_record() {
        // Record has full ownership populated (mark_initial-style),
        // verify mark_released wipes all three identity fields.
        let s = store();
        mark_initial(&s, "vol", &id_a(), sample_ulid())
            .await
            .unwrap();

        let (before, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert!(before.coordinator_id.is_some());
        assert!(before.claimed_at.is_some());

        mark_released(&s, "vol", &id_a(), snap()).await.unwrap();

        let (after, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert!(after.coordinator_id.is_none());
        assert!(after.claimed_at.is_none());
        assert!(after.hostname.is_none());
        // vol_ulid and handoff_snapshot are still required for the
        // claim-from-released path.
        assert_eq!(after.vol_ulid, sample_ulid());
        assert_eq!(after.handoff_snapshot, Some(snap()));
    }

    #[tokio::test]
    async fn mark_released_flips_stopped_to_released() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_stopped(&s, "vol", &id_a()).await.unwrap();

        let outcome = mark_released(&s, "vol", &id_a(), snap()).await.unwrap();
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

        mark_released(&s, "vol", &id_a(), snap()).await.unwrap();
        let outcome = mark_released(&s, "vol", &id_a(), snap()).await.unwrap();
        assert!(matches!(outcome, MarkReleasedOutcome::AlreadyReleased));
    }

    #[tokio::test]
    async fn mark_released_refuses_other_owners_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();

        // A claims via stop.
        mark_stopped(&s, "vol", &id_a()).await.unwrap();

        // B tries to release A's record.
        let err = mark_released(&s, "vol", &id_b(), snap())
            .await
            .expect_err("B must be refused on A-owned record");
        assert!(matches!(err, LifecycleError::OwnershipConflict { .. }));
    }

    #[tokio::test]
    async fn mark_live_returns_absent_when_record_missing() {
        let s = store();
        let r = mark_live(&s, "missing", &id_a()).await.unwrap();
        assert!(matches!(r, MarkLiveOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_live_resumes_stopped_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_stopped(&s, "vol", &id_a()).await.unwrap();

        let outcome = mark_live(&s, "vol", &id_a()).await.unwrap();
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
        mark_stopped(&s, "vol", &id_a()).await.unwrap();
        mark_live(&s, "vol", &id_a()).await.unwrap();

        let outcome = mark_live(&s, "vol", &id_a()).await.unwrap();
        assert!(matches!(outcome, MarkLiveOutcome::AlreadyLive));
    }

    #[tokio::test]
    async fn mark_live_signals_released_for_claim_path() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_released(&s, "vol", &id_a(), snap()).await.unwrap();

        // Even from the original owner, mark_live on a Released record
        // is not local-resume; the claim-from-released path is required.
        let outcome = mark_live(&s, "vol", &id_a()).await.unwrap();
        assert!(matches!(outcome, MarkLiveOutcome::Released));
    }

    #[tokio::test]
    async fn mark_live_refuses_other_owners_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_stopped(&s, "vol", &id_a()).await.unwrap();

        let err = mark_live(&s, "vol", &id_b())
            .await
            .expect_err("B must be refused on A-owned record");
        assert!(matches!(err, LifecycleError::OwnershipConflict { .. }));
    }

    #[tokio::test]
    async fn reconcile_marker_writes_marker_when_s3_says_stopped() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_stopped(&s, "vol", &id_a()).await.unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let vol_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&vol_dir).unwrap();

        reconcile_marker(&s, &vol_dir, "vol", &id_a()).await;
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
        mark_stopped(&s, "vol", &id_a()).await.unwrap();
        mark_live(&s, "vol", &id_a()).await.unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let vol_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&vol_dir).unwrap();
        std::fs::write(vol_dir.join("volume.stopped"), "").unwrap();

        reconcile_marker(&s, &vol_dir, "vol", &id_a()).await;
        assert!(!vol_dir.join("volume.stopped").exists());
    }

    #[tokio::test]
    async fn reconcile_marker_ignores_foreign_owned_records() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        // A claims via stop.
        mark_stopped(&s, "vol", &id_a()).await.unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let vol_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&vol_dir).unwrap();

        // B reconciles; A's record must not affect B's local state.
        reconcile_marker(&s, &vol_dir, "vol", &id_b()).await;
        assert!(!vol_dir.join("volume.stopped").exists());
    }

    fn other_ulid() -> Ulid {
        Ulid::from_string("01J2222222222222222222222V").unwrap()
    }

    #[tokio::test]
    async fn mark_claimed_returns_absent_when_record_missing() {
        let s = store();
        let outcome = mark_claimed(&s, "missing", &id_b(), other_ulid(), NameState::Live)
            .await
            .unwrap();
        assert!(matches!(outcome, MarkClaimedOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_claimed_refuses_non_released_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        // A owns + Live.
        mark_stopped(&s, "vol", &id_a()).await.unwrap();
        mark_live(&s, "vol", &id_a()).await.unwrap();

        let outcome = mark_claimed(&s, "vol", &id_b(), other_ulid(), NameState::Live)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            MarkClaimedOutcome::NotReleased {
                observed: NameState::Live
            }
        ));
    }

    #[tokio::test]
    async fn mark_claimed_rebinds_released_record() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_released(&s, "vol", &id_a(), snap()).await.unwrap();

        // B claims A's released name with a freshly-minted ULID.
        let outcome = mark_claimed(&s, "vol", &id_b(), other_ulid(), NameState::Live)
            .await
            .unwrap();
        assert!(matches!(outcome, MarkClaimedOutcome::Claimed));

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Live);
        assert_eq!(got.vol_ulid, other_ulid());
        // parent records the released ancestor + handoff snapshot.
        assert_eq!(
            got.parent.as_deref(),
            Some(format!("{}/{}", sample_ulid(), snap()).as_str()),
        );
        // handoff_snapshot is reset for the new ownership episode.
        assert!(got.handoff_snapshot.is_none());
        // coordinator_id flips to the claimant (B).
        let b_id = id_b();
        assert_eq!(got.coordinator_id.as_deref(), Some(b_id.as_str()));
    }

    #[tokio::test]
    async fn mark_claimed_resolves_concurrent_races_to_one_winner() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        create_name_record(&s, "vol", &rec).await.unwrap();
        mark_released(&s, "vol", &id_a(), snap()).await.unwrap();

        let id_b = id_b();
        let id_c = portable::format_coordinator_id(&portable::coordinator_id(&[0xEFu8; 32]));
        let ulid_b = other_ulid();
        let ulid_c = Ulid::from_string("01J3333333333333333333333V").unwrap();
        let (b_outcome, c_outcome) = tokio::join!(
            mark_claimed(&s, "vol", &id_b, ulid_b, NameState::Live),
            mark_claimed(&s, "vol", &id_c, ulid_c, NameState::Live),
        );

        // Exactly one wins via the conditional PUT; the other observes
        // either NotReleased (loser read after winner committed) or a
        // PreconditionFailed (loser read before, wrote after).
        let outcomes = [b_outcome, c_outcome];
        let claimed = outcomes
            .iter()
            .filter(|r| matches!(r, Ok(MarkClaimedOutcome::Claimed)))
            .count();
        assert_eq!(
            claimed, 1,
            "exactly one claim should win, got: {outcomes:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_marker_no_op_when_record_absent() {
        let s = store();
        let tmp = tempfile::TempDir::new().unwrap();
        let vol_dir = tmp.path().join("vol");
        std::fs::create_dir_all(&vol_dir).unwrap();

        reconcile_marker(&s, &vol_dir, "vol", &id_a()).await;
        assert!(!vol_dir.join("volume.stopped").exists());
    }

    // ── mark_initial ────────────────────────────────────────────────────

    #[tokio::test]
    async fn mark_initial_claims_fresh_name_with_full_ownership() {
        let s = store();
        let outcome = mark_initial(&s, "fresh", &id_a(), sample_ulid())
            .await
            .unwrap();
        assert!(matches!(outcome, MarkInitialOutcome::Claimed));

        let (got, _) = name_store::read_name_record(&s, "fresh")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.vol_ulid, sample_ulid());
        assert_eq!(got.state, NameState::Live);
        assert!(
            got.coordinator_id.is_some(),
            "coordinator_id populated at create time, not deferred"
        );
        assert!(got.claimed_at.is_some(), "claimed_at populated");
    }

    #[tokio::test]
    async fn mark_initial_refuses_when_record_already_exists() {
        let s = store();
        // Pre-existing record under a different owner (key_b).
        mark_initial(&s, "vol", &id_b(), sample_ulid())
            .await
            .unwrap();

        let outcome = mark_initial(&s, "vol", &id_a(), snap()).await.unwrap();
        match outcome {
            MarkInitialOutcome::AlreadyExists {
                existing_vol_ulid,
                existing_state,
                existing_owner,
            } => {
                assert_eq!(existing_vol_ulid, sample_ulid());
                assert_eq!(existing_state, NameState::Live);
                assert!(existing_owner.is_some(), "owner surfaced for diagnostics");
            }
            _ => panic!("expected AlreadyExists, got {outcome:?}"),
        }

        // The existing record must not have been overwritten.
        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.vol_ulid, sample_ulid(), "original ULID intact");
    }

    #[tokio::test]
    async fn mark_initial_distinguishes_creators() {
        // Two different coordinators creating two different names both
        // succeed independently.
        let s = store();
        let u1 = sample_ulid();
        let u2 = snap();

        let r1 = mark_initial(&s, "alpha", &id_a(), u1).await.unwrap();
        let r2 = mark_initial(&s, "beta", &id_b(), u2).await.unwrap();
        assert!(matches!(r1, MarkInitialOutcome::Claimed));
        assert!(matches!(r2, MarkInitialOutcome::Claimed));

        let (a, _) = name_store::read_name_record(&s, "alpha")
            .await
            .unwrap()
            .unwrap();
        let (b, _) = name_store::read_name_record(&s, "beta")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            a.coordinator_id, b.coordinator_id,
            "different root keys produce different coordinator ids"
        );
    }

    // ── mark_reclaimed_local ────────────────────────────────────────────

    #[tokio::test]
    async fn mark_reclaimed_local_flips_released_to_live_keeping_vol_ulid() {
        let s = store();
        // Set up: claim via mark_initial, release.
        mark_initial(&s, "vol", &id_a(), sample_ulid())
            .await
            .unwrap();
        mark_released(&s, "vol", &id_a(), snap()).await.unwrap();

        // Reclaim in-place.
        let outcome = mark_reclaimed_local(&s, "vol", &id_a(), sample_ulid(), NameState::Live)
            .await
            .unwrap();
        assert!(matches!(outcome, MarkReclaimedLocalOutcome::Reclaimed));

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Live);
        assert_eq!(got.vol_ulid, sample_ulid(), "vol_ulid preserved");
        assert!(got.coordinator_id.is_some());
        assert!(got.claimed_at.is_some());
        // handoff_snapshot was set on release; cleared on reclaim.
        assert!(got.handoff_snapshot.is_none());
    }

    #[tokio::test]
    async fn mark_reclaimed_local_returns_absent_when_record_missing() {
        let s = store();
        let outcome = mark_reclaimed_local(&s, "missing", &id_a(), sample_ulid(), NameState::Live)
            .await
            .unwrap();
        assert!(matches!(outcome, MarkReclaimedLocalOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_reclaimed_local_reports_not_released_for_live_record() {
        let s = store();
        mark_initial(&s, "vol", &id_a(), sample_ulid())
            .await
            .unwrap();

        let outcome = mark_reclaimed_local(&s, "vol", &id_a(), sample_ulid(), NameState::Live)
            .await
            .unwrap();
        match outcome {
            MarkReclaimedLocalOutcome::NotReleased {
                observed_state,
                observed_vol_ulid,
            } => {
                assert_eq!(observed_state, NameState::Live);
                assert_eq!(observed_vol_ulid, sample_ulid());
            }
            _ => panic!("expected NotReleased, got {outcome:?}"),
        }
    }

    #[tokio::test]
    async fn mark_reclaimed_local_reports_fork_mismatch_for_foreign_release() {
        // A different vol_ulid than the local fork → caller must use
        // the cross-coordinator claim path, not the in-place reclaim.
        let s = store();
        mark_initial(&s, "vol", &id_a(), sample_ulid())
            .await
            .unwrap();
        mark_released(&s, "vol", &id_a(), snap()).await.unwrap();

        let other_local_ulid = snap(); // different from sample_ulid()
        let outcome = mark_reclaimed_local(&s, "vol", &id_b(), other_local_ulid, NameState::Live)
            .await
            .unwrap();
        match outcome {
            MarkReclaimedLocalOutcome::ForkMismatch {
                local_vol_ulid,
                released_vol_ulid,
            } => {
                assert_eq!(local_vol_ulid, other_local_ulid);
                assert_eq!(released_vol_ulid, sample_ulid());
            }
            _ => panic!("expected ForkMismatch, got {outcome:?}"),
        }

        // The bucket record was not touched.
        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Released);
    }

    // ── mark_initial_readonly ───────────────────────────────────────────

    #[tokio::test]
    async fn mark_initial_readonly_claims_with_no_owner_identity() {
        let s = store();
        let outcome = mark_initial_readonly(&s, "ubuntu24", sample_ulid())
            .await
            .unwrap();
        assert!(matches!(outcome, MarkInitialOutcome::Claimed));

        let (got, _) = name_store::read_name_record(&s, "ubuntu24")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.vol_ulid, sample_ulid());
        assert_eq!(got.state, NameState::Readonly);
        assert!(got.coordinator_id.is_none(), "no exclusive owner");
        assert!(got.claimed_at.is_none(), "no claim time recorded");
        assert!(got.hostname.is_none(), "no hostname recorded");
    }

    #[tokio::test]
    async fn mark_initial_readonly_refuses_when_name_already_exists() {
        let s = store();
        mark_initial_readonly(&s, "ubuntu24", sample_ulid())
            .await
            .unwrap();

        let outcome = mark_initial_readonly(&s, "ubuntu24", snap()).await.unwrap();
        match outcome {
            MarkInitialOutcome::AlreadyExists {
                existing_vol_ulid,
                existing_state,
                ..
            } => {
                assert_eq!(existing_vol_ulid, sample_ulid());
                assert_eq!(existing_state, NameState::Readonly);
            }
            _ => panic!("expected AlreadyExists, got {outcome:?}"),
        }
    }

    #[tokio::test]
    async fn mark_initial_readonly_collides_with_writable_record() {
        // First a writable claim, then an attempt to import the same
        // name → must be refused.
        let s = store();
        mark_initial(&s, "name", &id_a(), sample_ulid())
            .await
            .unwrap();

        let outcome = mark_initial_readonly(&s, "name", snap()).await.unwrap();
        assert!(matches!(
            outcome,
            MarkInitialOutcome::AlreadyExists {
                existing_state: NameState::Live,
                ..
            }
        ));
    }

    // ── Lifecycle verbs refuse Readonly records ─────────────────────────

    #[tokio::test]
    async fn mark_stopped_refuses_readonly_record() {
        let s = store();
        mark_initial_readonly(&s, "vol", sample_ulid())
            .await
            .unwrap();

        let err = mark_stopped(&s, "vol", &id_a())
            .await
            .expect_err("readonly record cannot be stopped");
        assert!(matches!(
            err,
            LifecycleError::InvalidTransition {
                from: NameState::Readonly,
                verb: Lifecycle::Stop,
            }
        ));
    }

    #[tokio::test]
    async fn mark_released_refuses_readonly_record() {
        let s = store();
        mark_initial_readonly(&s, "vol", sample_ulid())
            .await
            .unwrap();

        let err = mark_released(&s, "vol", &id_a(), snap())
            .await
            .expect_err("readonly record cannot be released");
        assert!(matches!(
            err,
            LifecycleError::InvalidTransition {
                from: NameState::Readonly,
                verb: Lifecycle::Release,
            }
        ));
    }

    #[tokio::test]
    async fn mark_live_refuses_readonly_record() {
        let s = store();
        mark_initial_readonly(&s, "vol", sample_ulid())
            .await
            .unwrap();

        let err = mark_live(&s, "vol", &id_a())
            .await
            .expect_err("readonly record cannot transition to live");
        assert!(matches!(
            err,
            LifecycleError::InvalidTransition {
                from: NameState::Readonly,
                verb: Lifecycle::Start,
            }
        ));
    }

    #[tokio::test]
    async fn mark_claimed_reports_readonly_as_not_released() {
        // mark_claimed observes Readonly via its existing NotReleased
        // path — readonly records are not part of the claim-from-released
        // flow.
        let s = store();
        mark_initial_readonly(&s, "vol", sample_ulid())
            .await
            .unwrap();

        let outcome = mark_claimed(&s, "vol", &id_a(), snap(), NameState::Live)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            MarkClaimedOutcome::NotReleased {
                observed: NameState::Readonly
            }
        ));
    }

    // ── mark_released_force ───────────────────────────────────────────

    #[tokio::test]
    async fn mark_released_force_returns_absent_when_record_missing() {
        let s = store();
        let outcome = mark_released_force(&s, "missing", snap()).await.unwrap();
        assert!(matches!(outcome, ForceReleaseOutcome::Absent));
    }

    #[tokio::test]
    async fn mark_released_force_overrides_foreign_owned_live_record() {
        let s = store();
        // A owns Live.
        mark_initial(&s, "vol", &id_a(), sample_ulid())
            .await
            .unwrap();

        // B force-releases without any ownership check.
        let outcome = mark_released_force(&s, "vol", snap()).await.unwrap();
        match outcome {
            ForceReleaseOutcome::Overwritten { dead_vol_ulid } => {
                assert_eq!(dead_vol_ulid, sample_ulid());
            }
            other => panic!("expected Overwritten, got {other:?}"),
        }

        let (got, _) = name_store::read_name_record(&s, "vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.state, NameState::Released);
        assert_eq!(got.vol_ulid, sample_ulid(), "dead vol_ulid preserved");
        assert_eq!(got.handoff_snapshot, Some(snap()));
        // Released → no current owner.
        assert!(got.coordinator_id.is_none());
        assert!(got.claimed_at.is_none());
        assert!(got.hostname.is_none());
    }

    #[tokio::test]
    async fn mark_released_force_overrides_foreign_owned_stopped_record() {
        let s = store();
        mark_initial(&s, "vol", &id_a(), sample_ulid())
            .await
            .unwrap();
        mark_stopped(&s, "vol", &id_a()).await.unwrap();

        let outcome = mark_released_force(&s, "vol", snap()).await.unwrap();
        assert!(matches!(outcome, ForceReleaseOutcome::Overwritten { .. }));
    }

    #[tokio::test]
    async fn mark_released_force_refuses_already_released_record() {
        let s = store();
        mark_initial(&s, "vol", &id_a(), sample_ulid())
            .await
            .unwrap();
        mark_released(&s, "vol", &id_a(), snap()).await.unwrap();

        let outcome = mark_released_force(&s, "vol", snap()).await.unwrap();
        match outcome {
            ForceReleaseOutcome::InvalidState { observed } => {
                assert_eq!(observed, NameState::Released);
            }
            other => panic!("expected InvalidState, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mark_released_force_refuses_readonly_record() {
        let s = store();
        mark_initial_readonly(&s, "vol", sample_ulid())
            .await
            .unwrap();

        let outcome = mark_released_force(&s, "vol", snap()).await.unwrap();
        match outcome {
            ForceReleaseOutcome::InvalidState { observed } => {
                assert_eq!(observed, NameState::Readonly);
            }
            other => panic!("expected InvalidState, got {other:?}"),
        }
    }
}
