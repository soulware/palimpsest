//! Domain-typed handle over the `names/<name>` claim records.
//!
//! Second slice of the domain-typed store layer
//! (`docs/design-domain-store.md`). Wraps the existing
//! [`crate::name_store`] primitive (S3 CAS) and the
//! [`crate::lifecycle`] state-machine verbs (`mark_*`) behind a typed
//! trait pair: [`NameClaimsReader`] (read-only, coord-base) and
//! [`NameClaims`] (full read+write, coord-writer for mutations).
//!
//! The split mirrors [`EventJournalReader`] vs [`EventJournal`] and
//! [`crate::stores::ReadStore`] vs `ObjectStore`: credential scope
//! becomes type scope. A pure-read site (`Request::ResolveName`,
//! `bucket_position::fetch_position`) takes
//! `&dyn NameClaimsReader` and **cannot** invoke a `mark_*` at the
//! type level. The mutation methods are bundled into [`NameClaims`]
//! because no current caller needs to do its own CAS — every state
//! transition is one of the typed `mark_*` verbs whose
//! read-modify-write runs wholly on `coord-writer`.
//!
//! [`EventJournalReader`]: crate::event_journal::EventJournalReader
//! [`EventJournal`]: crate::event_journal::EventJournal

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use object_store::ObjectStore;
use ulid::Ulid;

use elide_core::name_record::{NameRecord, NameState};

use crate::lifecycle::{
    ForceReleaseOutcome, LifecycleError, MarkClaimedOutcome, MarkInitialOutcome, MarkLiveOutcome,
    MarkReclaimedLocalOutcome, MarkReleasedOutcome, MarkStoppedOutcome,
};
use crate::name_store::{self, NameStoreError};

/// Read-only view over the `names/<name>` records. Coord-base scope.
/// A holder cannot invoke any of the `mark_*` mutating verbs — they
/// do not exist on this trait.
///
/// Acquired via [`crate::stores::ScopedStores::name_claims_ro`].
#[async_trait]
pub trait NameClaimsReader: Send + Sync {
    /// Read `names/<name>`. `Ok(None)` if the key is absent.
    async fn read(&self, name: &str) -> Result<Option<NameRecord>, NameStoreError>;
}

/// Full read+write handle over `names/<name>`. Extends
/// [`NameClaimsReader`] with the state-machine verbs. Each `mark_*`
/// runs its full read-modify-write on the `coord-writer` credential
/// (one credential per mutation — the `docs/design-mint.md` rule),
/// inherited reads stay on `coord-base`.
///
/// Acquired via [`crate::stores::ScopedStores::name_claims`].
///
/// The trait deliberately exposes no untyped `update` /
/// `overwrite` verb. Every state change goes through one of the
/// typed `mark_*` methods, which fold the lifecycle invariants
/// (owner check, valid-transition check, idempotency) into the
/// store call.
#[async_trait]
pub trait NameClaims: NameClaimsReader {
    /// Create-time claim of a fresh name. `If-None-Match: *` —
    /// concurrent callers resolve cleanly: one wins, others get
    /// [`MarkInitialOutcome::AlreadyExists`].
    async fn mark_initial(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
        vol_ulid: Ulid,
        size: u64,
    ) -> Result<MarkInitialOutcome, LifecycleError>;

    /// Create-time claim of a fresh **readonly** name (no exclusive
    /// owner). Same conditional-create mechanics as
    /// [`Self::mark_initial`]; record carries `state = Readonly`.
    async fn mark_initial_readonly(
        &self,
        name: &str,
        vol_ulid: Ulid,
        size: u64,
    ) -> Result<MarkInitialOutcome, LifecycleError>;

    /// Transition `Live` → `Stopped`, retaining ownership.
    async fn mark_stopped(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
    ) -> Result<MarkStoppedOutcome, LifecycleError>;

    /// Transition `Live`/`Stopped` → `Released`, recording the
    /// handoff snapshot for the next claimant.
    async fn mark_released(
        &self,
        name: &str,
        coord_id: &str,
        handoff_snapshot: Ulid,
    ) -> Result<MarkReleasedOutcome, LifecycleError>;

    /// **Unconditional** rewrite to `Released` for `volume release
    /// --force`. No ownership check, no `If-Match` — overriding
    /// foreign ownership is the verb's whole point.
    async fn mark_released_force(
        &self,
        name: &str,
        handoff_snapshot: Ulid,
    ) -> Result<ForceReleaseOutcome, LifecycleError>;

    /// Transition `Stopped` → `Live` (local resume of a volume this
    /// coordinator already owns).
    async fn mark_live(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
    ) -> Result<MarkLiveOutcome, LifecycleError>;

    /// Atomically claim a `Released` name and rebind it to
    /// `new_vol_ulid`. Conditional PUT under the ETag observed at
    /// read time so two coordinators racing to claim resolve cleanly.
    async fn mark_claimed(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
        new_vol_ulid: Ulid,
        target_state: NameState,
    ) -> Result<MarkClaimedOutcome, LifecycleError>;

    /// In-place reclaim of a `Released` name when the local fork is
    /// the same `vol_ulid` the record points at — the previous owner
    /// was us. Cheaper than [`Self::mark_claimed`] because no new
    /// fork is minted.
    async fn mark_reclaimed_local(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
        local_vol_ulid: Ulid,
        target_state: NameState,
    ) -> Result<MarkReclaimedLocalOutcome, LifecycleError>;

    /// Best-effort reconcile of the local park markers
    /// (`volume.stopped`, `volume.released`) against the authoritative
    /// `names/<name>.state`.
    async fn reconcile_marker(&self, vol_dir: &Path, volume_name: &str, coord_id: &str);
}

/// Read-only `NameClaimsReader` over a `coord-base`-scoped store.
pub struct ReadOnlyNameClaims {
    reader: Arc<dyn ObjectStore>,
}

impl ReadOnlyNameClaims {
    pub fn new(reader: Arc<dyn ObjectStore>) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl NameClaimsReader for ReadOnlyNameClaims {
    async fn read(&self, name: &str) -> Result<Option<NameRecord>, NameStoreError> {
        Ok(name_store::read_name_record(&self.reader, name)
            .await?
            .map(|(rec, _ver)| rec))
    }
}

/// Full `NameClaims` impl. `writer` (`coord-writer`) carries every
/// `mark_*` call's full read-modify-write; `reader` (`coord-base`)
/// carries pure reads.
pub struct BucketNameClaims {
    writer: Arc<dyn ObjectStore>,
    reader: Arc<dyn ObjectStore>,
}

impl BucketNameClaims {
    pub fn new(writer: Arc<dyn ObjectStore>, reader: Arc<dyn ObjectStore>) -> Self {
        Self { writer, reader }
    }
}

#[async_trait]
impl NameClaimsReader for BucketNameClaims {
    async fn read(&self, name: &str) -> Result<Option<NameRecord>, NameStoreError> {
        Ok(name_store::read_name_record(&self.reader, name)
            .await?
            .map(|(rec, _ver)| rec))
    }
}

#[async_trait]
impl NameClaims for BucketNameClaims {
    async fn mark_initial(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
        vol_ulid: Ulid,
        size: u64,
    ) -> Result<MarkInitialOutcome, LifecycleError> {
        crate::lifecycle::mark_initial(&self.writer, name, coord_id, hostname, vol_ulid, size).await
    }

    async fn mark_initial_readonly(
        &self,
        name: &str,
        vol_ulid: Ulid,
        size: u64,
    ) -> Result<MarkInitialOutcome, LifecycleError> {
        crate::lifecycle::mark_initial_readonly(&self.writer, name, vol_ulid, size).await
    }

    async fn mark_stopped(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
    ) -> Result<MarkStoppedOutcome, LifecycleError> {
        crate::lifecycle::mark_stopped(&self.writer, name, coord_id, hostname).await
    }

    async fn mark_released(
        &self,
        name: &str,
        coord_id: &str,
        handoff_snapshot: Ulid,
    ) -> Result<MarkReleasedOutcome, LifecycleError> {
        crate::lifecycle::mark_released(&self.writer, name, coord_id, handoff_snapshot).await
    }

    async fn mark_released_force(
        &self,
        name: &str,
        handoff_snapshot: Ulid,
    ) -> Result<ForceReleaseOutcome, LifecycleError> {
        crate::lifecycle::mark_released_force(&self.writer, name, handoff_snapshot).await
    }

    async fn mark_live(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
    ) -> Result<MarkLiveOutcome, LifecycleError> {
        crate::lifecycle::mark_live(&self.writer, name, coord_id, hostname).await
    }

    async fn mark_claimed(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
        new_vol_ulid: Ulid,
        target_state: NameState,
    ) -> Result<MarkClaimedOutcome, LifecycleError> {
        crate::lifecycle::mark_claimed(
            &self.writer,
            name,
            coord_id,
            hostname,
            new_vol_ulid,
            target_state,
        )
        .await
    }

    async fn mark_reclaimed_local(
        &self,
        name: &str,
        coord_id: &str,
        hostname: Option<&str>,
        local_vol_ulid: Ulid,
        target_state: NameState,
    ) -> Result<MarkReclaimedLocalOutcome, LifecycleError> {
        crate::lifecycle::mark_reclaimed_local(
            &self.writer,
            name,
            coord_id,
            hostname,
            local_vol_ulid,
            target_state,
        )
        .await
    }

    async fn reconcile_marker(&self, vol_dir: &Path, volume_name: &str, coord_id: &str) {
        crate::lifecycle::reconcile_marker(&self.writer, vol_dir, volume_name, coord_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn claims() -> (Arc<dyn ObjectStore>, BucketNameClaims) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c = BucketNameClaims::new(Arc::clone(&store), Arc::clone(&store));
        (store, c)
    }

    fn sample_ulid() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    const SAMPLE_SIZE: u64 = 4 * 1024 * 1024 * 1024;

    #[tokio::test]
    async fn read_returns_none_for_absent() {
        let (_s, c) = claims();
        assert!(c.read("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mark_initial_then_read_round_trips() {
        let (_s, c) = claims();
        let outcome = c
            .mark_initial("vol", "coord-a", Some("host-a"), sample_ulid(), SAMPLE_SIZE)
            .await
            .unwrap();
        assert!(matches!(outcome, MarkInitialOutcome::Claimed));

        let rec = c.read("vol").await.unwrap().expect("present");
        assert_eq!(rec.vol_ulid, sample_ulid());
        assert_eq!(rec.state, NameState::Live);
        assert_eq!(rec.coordinator_id.as_deref(), Some("coord-a"));
    }

    #[tokio::test]
    async fn mark_initial_second_call_reports_already_exists() {
        let (_s, c) = claims();
        c.mark_initial("vol", "coord-a", None, sample_ulid(), SAMPLE_SIZE)
            .await
            .unwrap();
        let outcome = c
            .mark_initial("vol", "coord-b", None, Ulid::new(), SAMPLE_SIZE)
            .await
            .unwrap();
        assert!(matches!(outcome, MarkInitialOutcome::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn mark_stopped_after_initial_succeeds() {
        let (_s, c) = claims();
        c.mark_initial("vol", "coord-a", None, sample_ulid(), SAMPLE_SIZE)
            .await
            .unwrap();
        let outcome = c.mark_stopped("vol", "coord-a", None).await.unwrap();
        assert!(matches!(outcome, MarkStoppedOutcome::Updated));

        let rec = c.read("vol").await.unwrap().unwrap();
        assert_eq!(rec.state, NameState::Stopped);
    }

    #[tokio::test]
    async fn mark_released_then_reclaim_local_round_trips() {
        let (_s, c) = claims();
        c.mark_initial("vol", "coord-a", None, sample_ulid(), SAMPLE_SIZE)
            .await
            .unwrap();
        let handoff = Ulid::new();
        let released = c.mark_released("vol", "coord-a", handoff).await.unwrap();
        assert!(matches!(released, MarkReleasedOutcome::Updated { .. }));

        let outcome = c
            .mark_reclaimed_local("vol", "coord-a", None, sample_ulid(), NameState::Stopped)
            .await
            .unwrap();
        assert!(matches!(outcome, MarkReclaimedLocalOutcome::Reclaimed));
    }

    #[tokio::test]
    async fn mark_released_force_overrides_foreign_owner() {
        let (_s, c) = claims();
        c.mark_initial("vol", "coord-foreign", None, sample_ulid(), SAMPLE_SIZE)
            .await
            .unwrap();
        let handoff = Ulid::new();
        let outcome = c.mark_released_force("vol", handoff).await.unwrap();
        assert!(matches!(outcome, ForceReleaseOutcome::Overwritten { .. }));
        let rec = c.read("vol").await.unwrap().unwrap();
        assert_eq!(rec.state, NameState::Released);
        assert_eq!(rec.handoff_snapshot, Some(handoff));
    }
}
