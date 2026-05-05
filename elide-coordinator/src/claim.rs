//! Claim flow: registry, orchestrator, and the bucket-side entry point.
//!
//! The claim flow runs in two halves:
//!
//! 1. **Bucket-side claim** ([`claim_volume_bucket_op`]). Synchronous; returns
//!    [`ClaimReply::Reclaimed`] when this host already holds a matching local
//!    fork (in-place reclaim, nothing more to do), or
//!    [`ClaimReply::MustClaimFresh`] when foreign content needs to be pulled
//!    and a fresh fork minted.
//!
//! 2. **Orchestrator** ([`ClaimOrchestrator`]). Spawned in a background tokio
//!    task by [`start_claim`] for the `MustClaimFresh` branch; streams
//!    progress events into a [`ClaimJob`] which `claim-attach` subscribers
//!    consume.
//!
//! The orchestrator owns the per-job state (new-fork skeleton, peer-fetch
//! context, pulled ancestor guard, effective ancestor) so each stage method
//! reads/writes via `&mut self` instead of threading the state through
//! function arguments. Stage outputs that downstream stages consume live as
//! `Option<...>` fields and are unwrapped with explicit `expect()` messages
//! that document which earlier stage was supposed to populate them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use ed25519_dalek::SigningKey;
use object_store::ObjectStore;
use tracing::{info, warn};
use ulid::Ulid;

use crate::inbound::{
    CoordinatorCore, await_prefetch_op, decode_hex32, local_daemon_running, pull_readonly_op,
};
use elide_coordinator::ipc::{
    ClaimAttachEvent, ClaimReply, ClaimStartReply, IpcError, ResolveHandoffKeyReply,
};
use elide_coordinator::prefetch::PeerFetchContext;
use elide_coordinator::register_prefetch_or_get;
use elide_coordinator::volume_state::STOPPED_FILE;

// ── Per-domain context ───────────────────────────────────────────────────────

/// Coordinator state needed by the claim flow: the universal hot core
/// plus the claim-domain registries. Constructed via
/// [`crate::inbound::IpcContext::for_claim`].
#[derive(Clone)]
pub(crate) struct ClaimContext {
    pub core: CoordinatorCore,
    pub claim_registry: ClaimRegistry,
    pub prefetch_tracker: elide_coordinator::PrefetchTracker,
    pub peer_fetch: Option<elide_coordinator::tasks::PeerFetchHandle>,
}

// ── Job + registry ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum ClaimJobState {
    Running,
    Done,
    Failed(IpcError),
}

pub struct ClaimJob {
    events: Mutex<Vec<ClaimAttachEvent>>,
    state: RwLock<ClaimJobState>,
}

impl ClaimJob {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            state: RwLock::new(ClaimJobState::Running),
        })
    }

    pub fn append(&self, event: ClaimAttachEvent) {
        self.events
            .lock()
            .expect("claim job events poisoned")
            .push(event);
    }

    pub fn finish(&self, state: ClaimJobState) {
        *self.state.write().expect("claim job state poisoned") = state;
    }

    pub fn read_from(&self, offset: usize) -> Vec<ClaimAttachEvent> {
        self.events.lock().expect("claim job events poisoned")[offset..].to_vec()
    }

    pub fn state(&self) -> ClaimJobState {
        self.state.read().expect("claim job state poisoned").clone()
    }
}

/// Registry of in-flight claim jobs keyed by volume name. The bucket-side
/// `claim-start` op already serialises concurrent claims for the same name
/// (the conditional PUT inside `mark_claimed` will lose), so two claim jobs
/// for the same name cannot both be in their post-claim phase.
pub type ClaimRegistry = Arc<Mutex<HashMap<String, Arc<ClaimJob>>>>;

pub fn new_registry() -> ClaimRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

// ── Pulled-ancestor cleanup guard ─────────────────────────────────────────────

/// Tracks ancestor skeletons pulled during one claim attempt and removes them
/// from disk on drop unless [`Self::commit`] was called.
///
/// Why: [`pull_readonly_op`] verifies provenance against the just-downloaded
/// `volume.pub` — a self-consistent check that does not catch a peer (or
/// store) supplying matched-but-forged `volume.pub` + `volume.provenance`
/// bytes. The forgery only fails later, when the released volume's signed
/// handoff manifest from S3 is checked against that pubkey. Without this
/// guard a failed claim leaves the bogus skeleton in `data_dir/by_id/<id>/`;
/// the next retry sees the directory exists and reuses the lie. The guard
/// ensures every ancestor pulled in a failing attempt is torn down before
/// the error propagates.
///
/// Removal is cheap blocking I/O (`std::fs::remove_dir_all`), safe to run
/// from `Drop`. Failures are logged but not propagated — at worst a leftover
/// dir survives, the same outcome as today's behaviour.
pub(crate) struct PulledAncestorsGuard {
    by_id_dir: PathBuf,
    pulled: Vec<Ulid>,
    committed: bool,
}

impl PulledAncestorsGuard {
    pub(crate) fn new(by_id_dir: PathBuf) -> Self {
        Self {
            by_id_dir,
            pulled: Vec::new(),
            committed: false,
        }
    }

    pub(crate) fn record(&mut self, vol_ulid: Ulid) {
        self.pulled.push(vol_ulid);
    }

    /// Mark the pulled set as kept. Call after every downstream verification
    /// step that could reject a peer-served forgery has passed — typically
    /// right before [`ClaimOrchestrator::finalize`].
    pub(crate) fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for PulledAncestorsGuard {
    fn drop(&mut self) {
        if self.committed || self.pulled.is_empty() {
            return;
        }
        for vol_ulid in &self.pulled {
            let dir = self.by_id_dir.join(vol_ulid.to_string());
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                warn!(
                    "[claim cleanup] failed to remove pulled ancestor {}: {e}",
                    dir.display()
                );
            } else {
                info!("[claim cleanup] removed unverified ancestor {vol_ulid}");
            }
        }
    }
}

// ── Entry point ──────────────────────────────────────────────────────────────

/// Run the bucket-side claim synchronously and either return `Reclaimed` (no
/// further work) or register a job and spawn the foreign-claim orchestrator.
/// Returns immediately in both branches — `Claiming` callers subscribe via
/// `claim-attach` to stream progress.
pub(crate) async fn start_claim(
    volume: String,
    ctx: ClaimContext,
) -> Result<ClaimStartReply, IpcError> {
    let store = ctx.core.stores.coordinator_wide();
    let bucket_started = std::time::Instant::now();
    let bucket =
        claim_volume_bucket_op(&volume, &ctx.core.data_dir, &store, &ctx.core.identity).await?;
    info!(
        "[claim {volume}] bucket-side claim resolved in {:.2?}",
        bucket_started.elapsed()
    );
    match bucket {
        ClaimReply::Reclaimed => Ok(ClaimStartReply::Reclaimed),
        ClaimReply::MustClaimFresh {
            released_vol_ulid,
            handoff_snapshot,
        } => {
            let snap = handoff_snapshot.ok_or_else(|| {
                IpcError::not_found(format!(
                    "name '{volume}' is Released but has no handoff snapshot — \
                     manual recovery required (see docs/operations.md)"
                ))
            })?;

            {
                let mut reg = ctx.claim_registry.lock().expect("claim registry poisoned");
                if let Some(job) = reg.get(&volume)
                    && matches!(job.state(), ClaimJobState::Running)
                {
                    return Err(IpcError::conflict(format!(
                        "claim for '{volume}' is already in progress"
                    )));
                }
                reg.insert(volume.clone(), ClaimJob::new());
            }
            let job = ctx
                .claim_registry
                .lock()
                .expect("claim registry poisoned")
                .get(&volume)
                .cloned()
                .expect("just inserted");

            tokio::spawn(async move {
                let orch =
                    ClaimOrchestrator::new(job.clone(), volume, released_vol_ulid, snap, ctx);
                match orch.run().await {
                    Ok(()) => {
                        job.append(ClaimAttachEvent::Done);
                        job.finish(ClaimJobState::Done);
                    }
                    Err(e) => job.finish(ClaimJobState::Failed(e)),
                }
            });

            Ok(ClaimStartReply::Claiming { released_vol_ulid })
        }
    }
}

// ── Bucket-side claim ─────────────────────────────────────────────────────────

async fn claim_volume_bucket_op(
    volume_name: &str,
    data_dir: &std::path::Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
) -> Result<ClaimReply, IpcError> {
    use elide_coordinator::volume_state::clear_released_marker;
    use elide_core::name_record::NameState;

    let coord_id = identity.coordinator_id_str();

    // Claim always lands the volume in `Stopped`. A running local daemon
    // contradicts that — refuse and point the operator at `volume stop` first.
    if local_daemon_running(data_dir, volume_name) {
        return Err(IpcError::conflict(format!(
            "volume '{volume_name}' is running on this host; \
             stop it first with: elide volume stop {volume_name}"
        )));
    }

    let record_opt = elide_coordinator::name_store::read_name_record(store, volume_name)
        .await
        .map_err(|e| IpcError::store(format!("reading names/{volume_name}: {e}")))?;

    let (record, _version) = record_opt.ok_or_else(|| {
        IpcError::not_found(format!(
            "name '{volume_name}' has no S3 record; nothing to claim"
        ))
    })?;

    match record.state {
        NameState::Released => {
            // Determine whether we hold a matching local fork.
            let link = data_dir.join("by_name").join(volume_name);
            let local_vol_ulid = match std::fs::canonicalize(&link) {
                Ok(p) => p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| Ulid::from_string(s).ok()),
                Err(_) => None,
            };

            if local_vol_ulid == Some(record.vol_ulid) {
                use elide_coordinator::lifecycle::{
                    LifecycleError, MarkReclaimedLocalOutcome, mark_reclaimed_local,
                };
                match mark_reclaimed_local(
                    store,
                    volume_name,
                    coord_id,
                    identity.hostname(),
                    record.vol_ulid,
                    NameState::Stopped,
                )
                .await
                {
                    Ok(MarkReclaimedLocalOutcome::Reclaimed) => {
                        info!(
                            "[inbound] reclaimed {volume_name} in place (vol_ulid {})",
                            record.vol_ulid
                        );
                        // Best-effort: drop the display-only marker now that
                        // the bucket record is no longer Released.
                        if let Ok(vol_dir) = std::fs::canonicalize(&link)
                            && let Err(e) = clear_released_marker(&vol_dir)
                        {
                            warn!(
                                "[inbound] reclaim {volume_name}: clearing \
                                 volume.released marker: {e}"
                            );
                        }
                        elide_coordinator::volume_event_store::emit_best_effort(
                            store,
                            identity.as_ref(),
                            volume_name,
                            elide_core::volume_event::EventKind::Claimed,
                            record.vol_ulid,
                        )
                        .await;
                        Ok(ClaimReply::Reclaimed)
                    }
                    Ok(MarkReclaimedLocalOutcome::Absent) => Err(IpcError::precondition_failed(
                        format!("names/{volume_name} vanished between read and reclaim"),
                    )),
                    Ok(MarkReclaimedLocalOutcome::NotReleased { observed_state, .. }) => {
                        Err(IpcError::precondition_failed(format!(
                            "names/{volume_name} changed underneath us; now in state \
                             {observed_state:?}"
                        )))
                    }
                    Ok(MarkReclaimedLocalOutcome::ForkMismatch {
                        released_vol_ulid, ..
                    }) => {
                        // Race: someone rebound between our read and write.
                        // Surface as MustClaimFresh routing — same shape as the
                        // foreign-content path below.
                        Ok(ClaimReply::MustClaimFresh {
                            released_vol_ulid,
                            handoff_snapshot: record.handoff_snapshot,
                        })
                    }
                    Err(LifecycleError::Store(e)) => {
                        Err(IpcError::store(format!("reclaim failed: {e}")))
                    }
                    Err(LifecycleError::OwnershipConflict { .. })
                    | Err(LifecycleError::InvalidTransition { .. }) => Err(IpcError::conflict(
                        format!("in-place reclaim of {volume_name} refused"),
                    )),
                }
            } else {
                // Foreign content — CLI must orchestrate the claim.
                Ok(ClaimReply::MustClaimFresh {
                    released_vol_ulid: record.vol_ulid,
                    handoff_snapshot: record.handoff_snapshot,
                })
            }
        }
        NameState::Live | NameState::Stopped => match record.coordinator_id.as_deref() {
            Some(owner) if owner == coord_id => {
                // Already ours — nothing to claim.
                Err(IpcError::conflict(format!(
                    "name '{volume_name}' is already held by this coordinator"
                )))
            }
            Some(owner) => Err(IpcError::conflict(format!(
                "name '{volume_name}' is held by coordinator {owner}"
            ))),
            None => Err(IpcError::internal(format!(
                "name '{volume_name}' has no coordinator_id (malformed record)"
            ))),
        },
        NameState::Readonly => Err(IpcError::conflict(format!(
            "name '{volume_name}' is readonly (immutable handle); \
             pull it with `volume pull` to serve locally"
        ))),
    }
}

// ── Orchestrator ─────────────────────────────────────────────────────────────

/// Skeleton minted in stage 1 (`early_rebind`) and consumed in stage 6
/// (`finalize`). The fork dir on disk holds `volume.{key,pub}` only — no
/// `wal/`, no `pending/`, no `index/` — until `finalize` writes them.
struct NewForkSkeleton {
    vol_ulid: Ulid,
    dir: PathBuf,
    signing_key: SigningKey,
}

/// The ancestor chosen as the new fork's parent after stage 4b
/// (`skip_empty_intermediates`). `parent_key_hex` is `Some` only when the
/// chosen ancestor's snapshot was a recovery snapshot — its
/// `manifest_pubkey` override flows into the new fork's provenance.
struct EffectiveAncestor {
    vol: Ulid,
    snap: Ulid,
    parent_key_hex: Option<String>,
}

/// Drive one claim job to completion.
///
/// The flow is a six-stage pipeline; each stage consumes earlier outputs from
/// `self` and writes its own. See [`Self::run`] for the linear sequence.
pub(crate) struct ClaimOrchestrator {
    job: Arc<ClaimJob>,
    volume: String,
    released_vol_ulid: Ulid,
    handoff_snap: Ulid,
    ctx: ClaimContext,
    by_id_dir: PathBuf,
    pulled_guard: PulledAncestorsGuard,

    // Stage outputs.
    new_fork: Option<NewForkSkeleton>,
    peer_ctx: Option<PeerFetchContext>,
    parent_key_hex: Option<String>,
    effective: Option<EffectiveAncestor>,
}

impl ClaimOrchestrator {
    pub(crate) fn new(
        job: Arc<ClaimJob>,
        volume: String,
        released_vol_ulid: Ulid,
        handoff_snap: Ulid,
        ctx: ClaimContext,
    ) -> Self {
        let by_id_dir = ctx.core.data_dir.join("by_id");
        let pulled_guard = PulledAncestorsGuard::new(by_id_dir.clone());
        Self {
            job,
            volume,
            released_vol_ulid,
            handoff_snap,
            ctx,
            by_id_dir,
            pulled_guard,
            new_fork: None,
            peer_ctx: None,
            parent_key_hex: None,
            effective: None,
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), IpcError> {
        self.early_rebind().await?;
        self.discover_peer().await;
        self.pull_chain().await?;
        self.resolve_handoff_key().await?;
        self.skip_empty_intermediates().await?;
        // All signature checks against S3-rooted artifacts have passed.
        // Commit so the pulled skeletons survive the rest of this job.
        self.pulled_guard.commit();
        self.finalize().await?;
        self.surface_prefetch().await;
        Ok(())
    }

    /// Stage 1. Mint a fresh fork ULID + keypair, upload `volume.pub` to S3,
    /// and `mark_claimed` to rebind `names/<volume>` to this coordinator.
    /// After this returns the bucket says we own the name, peer-fetch auth
    /// accepts our coord_id for the chain walk that follows, and the local
    /// fork dir holds `volume.{key,pub}` only — crucially **no `wal/`, no
    /// `pending/`, no `index/`**, so the daemon's discovery loop won't pick
    /// the partial fork up and try to open it before [`Self::finalize`]
    /// writes the provenance.
    ///
    /// Crash semantics. If the coordinator dies between this returning and
    /// `finalize`'s `volume.provenance` upload, the bucket points at a fork
    /// that has a pubkey but no provenance. The fork is unmountable but
    /// recoverable: `volume release --force` treats a missing provenance as
    /// a crashed-during-create empty fork (its
    /// `recovery::list_and_verify_segments` finds zero segments and publishes
    /// an empty synthesised handoff manifest). After force-release, a fresh
    /// `claim` mints a new vol_ulid and proceeds.
    async fn early_rebind(&mut self) -> Result<(), IpcError> {
        use elide_coordinator::lifecycle::{LifecycleError, MarkClaimedOutcome, mark_claimed};
        use elide_core::name_record::NameState;
        use elide_core::signing::{VOLUME_KEY_FILE, VOLUME_PUB_FILE, generate_keypair};

        let early_started = std::time::Instant::now();

        let new_vol_ulid = Ulid::new();
        let new_vol_ulid_str = new_vol_ulid.to_string();
        let new_fork_dir = self.ctx.core.data_dir.join("by_id").join(&new_vol_ulid_str);

        // Bare dir + keypair only. No wal/, no pending/, no index/ — daemon
        // discovery requires one of those to consider a dir a volume, so this
        // skeleton stays invisible to the supervisor until `finalize` adds them.
        std::fs::create_dir_all(&new_fork_dir)
            .map_err(|e| IpcError::internal(format!("creating fork dir: {e}")))?;
        let signing_key = generate_keypair(&new_fork_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE)
            .map_err(|e| IpcError::internal(format!("generating keypair: {e}")))?;

        // Upload volume.pub to S3 so peer-fetch ancestry walks (which read
        // `by_id/<id>/volume.pub` to verify our parent provenance) and future
        // claimants doing release --force on a stuck fork have the file they
        // expect.
        let store = self.ctx.core.stores.for_volume(&new_vol_ulid);
        elide_coordinator::upload::upload_volume_pub_initial(
            &new_fork_dir,
            &new_vol_ulid_str,
            &store,
        )
        .await
        .map_err(|e| IpcError::store(format!("uploading volume.pub: {e:#}")))?;

        // Bucket rebind. Peer-fetch auth accepts our coord_id from this point
        // onward.
        let store_wide = self.ctx.core.stores.coordinator_wide();
        match mark_claimed(
            &store_wide,
            &self.volume,
            self.ctx.core.identity.coordinator_id_str(),
            self.ctx.core.identity.hostname(),
            new_vol_ulid,
            NameState::Stopped,
        )
        .await
        {
            Ok(MarkClaimedOutcome::Claimed) => {
                let vol = &self.volume;
                info!(
                    "[claim {vol}] early-rebind: bucket → {new_vol_ulid_str} \
                     (provenance pending)"
                );
                elide_coordinator::volume_event_store::emit_best_effort(
                    &store_wide,
                    self.ctx.core.identity.as_ref(),
                    &self.volume,
                    elide_core::volume_event::EventKind::Claimed,
                    new_vol_ulid,
                )
                .await;
                self.new_fork = Some(NewForkSkeleton {
                    vol_ulid: new_vol_ulid,
                    dir: new_fork_dir,
                    signing_key,
                });
                info!(
                    "[claim {}] early-rebind completed in {:.2?}",
                    self.volume,
                    early_started.elapsed()
                );
                Ok(())
            }
            Ok(MarkClaimedOutcome::Absent) => Err(IpcError::not_found(format!(
                "names/{} disappeared between bucket-side claim and rebind",
                self.volume
            ))),
            Ok(MarkClaimedOutcome::NotReleased { observed }) => Err(IpcError::conflict(format!(
                "names/{} changed underneath us; now in state {observed:?}",
                self.volume
            ))),
            Err(LifecycleError::Store(e)) => Err(IpcError::store(format!("rebind failed: {e}"))),
            Err(LifecycleError::OwnershipConflict { held_by }) => {
                Err(IpcError::precondition_failed(format!(
                    "name '{}' raced with another claim ({held_by} won)",
                    self.volume
                )))
            }
            Err(LifecycleError::InvalidTransition { from, .. }) => Err(IpcError::conflict(
                format!("names/{} is in unexpected state {from:?}", self.volume),
            )),
        }
    }

    /// Stage 2. Discover the previous claimer's peer-fetch endpoint.
    ///
    /// Best-effort — `peer_ctx` is set only when `[peer_fetch].port` is
    /// configured, the event log yields a clean Released, and the previous
    /// claimer published a peer endpoint. Peer auth now accepts our coord_id
    /// (we `mark_claimed` in stage 1), so peer requests will succeed.
    async fn discover_peer(&mut self) {
        let Some(handle) = self.ctx.peer_fetch.as_ref() else {
            return;
        };
        let store_wide = self.ctx.core.stores.coordinator_wide();
        if let Some(discovered) =
            elide_coordinator::peer_discovery::discover_peer_for_claim(&store_wide, &self.volume)
                .await
        {
            self.peer_ctx = Some(PeerFetchContext {
                client: handle.client.clone(),
                endpoint: discovered.endpoint,
                volume_name: self.volume.clone(),
            });
        }
    }

    /// Stage 3. Pull the released chain locally if absent. Peer-first when a
    /// context is available — auth now accepts our coord_id.
    async fn pull_chain(&mut self) -> Result<(), IpcError> {
        use elide_core::volume::resolve_ancestor_dir;

        let chain_started = std::time::Instant::now();
        let mut chain_pulled = 0usize;
        let mut next: Option<Ulid> = Some(self.released_vol_ulid);
        while let Some(vol_ulid) = next.take() {
            let dir = resolve_ancestor_dir(&self.by_id_dir, &vol_ulid.to_string());
            if dir.exists() {
                break;
            }
            self.job
                .append(ClaimAttachEvent::PullingAncestor { vol_ulid });
            let store = self.ctx.core.stores.for_volume(&vol_ulid);
            self.pulled_guard.record(vol_ulid);
            let reply = pull_readonly_op(
                vol_ulid,
                &self.ctx.core.data_dir,
                &store,
                self.peer_ctx.as_ref(),
            )
            .await?;
            chain_pulled += 1;
            next = reply.parent;
        }
        info!(
            "[claim {}] ancestor chain pulled: {chain_pulled} in {:.2?}",
            self.volume,
            chain_started.elapsed()
        );

        let source_dir = resolve_ancestor_dir(&self.by_id_dir, &self.released_vol_ulid.to_string());
        if !source_dir.exists() {
            return Err(IpcError::not_found(format!(
                "source volume {} not found in remote store",
                self.released_vol_ulid
            )));
        }
        Ok(())
    }

    /// Stage 4. Resolve the handoff key.
    ///
    /// Recovery snapshots are signed by an attestation key the fork's
    /// provenance must record so its own signature verifies later. **This is
    /// the first step that verifies a peer-served pubkey against an S3-rooted
    /// artifact**: the released volume's signed handoff manifest comes from
    /// the bucket and is checked against the just-pulled `volume.pub`. A
    /// tampering peer is detected here; on `?` propagation `pulled_guard`
    /// tears down the bogus skeletons before the error returns. (The partial
    /// fork minted in stage 1 is left for `volume release --force` to clean
    /// up; see [`Self::early_rebind`] for the recovery story.)
    async fn resolve_handoff_key(&mut self) -> Result<(), IpcError> {
        let handoff_started = std::time::Instant::now();
        let store = self.ctx.core.stores.for_volume(&self.released_vol_ulid);
        let key =
            resolve_handoff_key_via_recovery(self.released_vol_ulid, self.handoff_snap, &store)
                .await?;
        info!(
            "[claim {}] handoff key resolved in {:.2?}",
            self.volume,
            handoff_started.elapsed()
        );
        self.parent_key_hex = match &key {
            ResolveHandoffKeyReply::Normal => None,
            ResolveHandoffKeyReply::Recovery {
                manifest_pubkey_hex,
            } => Some(manifest_pubkey_hex.clone()),
        };
        self.job
            .append(ClaimAttachEvent::HandoffKeyResolved { key });
        Ok(())
    }

    /// Stage 4b. Skip empty intermediate forks.
    ///
    /// A fork that produced no writes between claim and release leaves a
    /// handoff snapshot whose segment list is identical to its parent's —
    /// every segment was inherited, none minted under this fork. Forking from
    /// such a no-op intermediate just bloats the chain by one link per cycle.
    /// Detect it and rewrite `effective` to point at the deepest non-empty
    /// ancestor.
    async fn skip_empty_intermediates(&mut self) -> Result<(), IpcError> {
        let (vol, snap, parent_key_hex) = skip_empty_intermediates_impl(
            &self.job,
            &self.volume,
            self.released_vol_ulid,
            self.handoff_snap,
            self.parent_key_hex.take(),
            &self.ctx.core.data_dir,
            &self.ctx.core.stores,
            self.peer_ctx.as_ref(),
            &mut self.pulled_guard,
        )
        .await?;
        self.effective = Some(EffectiveAncestor {
            vol,
            snap,
            parent_key_hex,
        });
        Ok(())
    }

    /// Stage 5. Now that ancestor verification has passed and `effective` is
    /// known, sign and upload `volume.provenance`, write the local config +
    /// `wal/` + `pending/`, link `by_name/<volume>`, drop the
    /// `volume.stopped` marker, and emit the journal event. Once this returns
    /// the fork is fully materialised and the daemon's next discovery tick
    /// will find and supervise it.
    async fn finalize(&mut self) -> Result<(), IpcError> {
        use elide_core::signing::{
            ParentRef, ProvenanceLineage, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE,
            load_verifying_key, write_provenance,
        };
        use elide_core::volume::resolve_ancestor_dir;

        let fork_create_started = std::time::Instant::now();
        let new_fork = self
            .new_fork
            .as_ref()
            .expect("early_rebind must run before finalize");
        let effective = self
            .effective
            .as_ref()
            .expect("skip_empty_intermediates must run before finalize");
        let new_vol_ulid_str = new_fork.vol_ulid.to_string();

        // Ancestor's identity pubkey for the embedded `ParentRef.pubkey` trust
        // anchor. Loaded from the just-pulled (and verified) ancestor skeleton.
        let parent_dir = resolve_ancestor_dir(&self.by_id_dir, &effective.vol.to_string());
        let parent_pubkey = load_verifying_key(&parent_dir, VOLUME_PUB_FILE)
            .map_err(|e| IpcError::internal(format!("loading parent volume.pub: {e}")))?;

        let manifest_pubkey = match effective.parent_key_hex.as_deref() {
            Some(hex) => Some(
                decode_hex32(hex)
                    .map_err(|e| IpcError::internal(format!("bad parent-key: {e}")))?,
            ),
            None => None,
        };

        let lineage = ProvenanceLineage {
            parent: Some(ParentRef {
                volume_ulid: effective.vol.to_string(),
                snapshot_ulid: effective.snap.to_string(),
                pubkey: parent_pubkey.to_bytes(),
                manifest_pubkey,
            }),
            extent_index: Vec::new(),
            oci_source: None,
        };
        write_provenance(
            &new_fork.dir,
            &new_fork.signing_key,
            VOLUME_PROVENANCE_FILE,
            &lineage,
        )
        .map_err(|e| IpcError::internal(format!("writing provenance: {e}")))?;

        let store = self.ctx.core.stores.for_volume(&new_fork.vol_ulid);
        elide_coordinator::upload::upload_volume_provenance_initial(
            &new_fork.dir,
            &new_vol_ulid_str,
            &store,
        )
        .await
        .map_err(|e| IpcError::store(format!("uploading volume.provenance: {e:#}")))?;

        // wal/ and pending/ now — daemon discovery becomes interested only
        // after these exist, by which point the provenance is on S3 and the
        // volume is fully openable.
        std::fs::create_dir_all(new_fork.dir.join("wal"))
            .map_err(|e| IpcError::internal(format!("creating wal/: {e}")))?;
        std::fs::create_dir_all(new_fork.dir.join("pending"))
            .map_err(|e| IpcError::internal(format!("creating pending/: {e}")))?;

        // Local volume.toml: size from the released NameRecord (claim is a
        // continuation of the same logical volume identity, not a resize).
        let store_wide = self.ctx.core.stores.coordinator_wide();
        let size = match elide_coordinator::name_store::read_name_record(&store_wide, &self.volume)
            .await
        {
            Ok(Some((rec, _))) => rec.size,
            Ok(None) => {
                return Err(IpcError::not_found(format!(
                    "names/{} disappeared during finalize",
                    self.volume
                )));
            }
            Err(e) => {
                return Err(IpcError::store(format!(
                    "reading names/{}: {e}",
                    self.volume
                )));
            }
        };
        elide_core::config::VolumeConfig {
            name: Some(self.volume.clone()),
            size: Some(size),
            nbd: None,
            ublk: None,
            lazy: None,
        }
        .write(&new_fork.dir)
        .map_err(|e| IpcError::internal(format!("writing volume.toml: {e}")))?;

        // by_name symlink + volume.stopped marker.
        let by_name_dir = self.ctx.core.data_dir.join("by_name");
        let symlink_path = by_name_dir.join(&self.volume);
        std::fs::create_dir_all(&by_name_dir)
            .map_err(|e| IpcError::internal(format!("creating by_name dir: {e}")))?;
        if symlink_path.exists() || symlink_path.is_symlink() {
            std::fs::remove_file(&symlink_path)
                .map_err(|e| IpcError::internal(format!("removing stale by_name link: {e}")))?;
        }
        std::os::unix::fs::symlink(format!("../by_id/{new_vol_ulid_str}"), &symlink_path)
            .map_err(|e| IpcError::internal(format!("creating by_name symlink: {e}")))?;
        std::fs::write(new_fork.dir.join(STOPPED_FILE), "")
            .map_err(|e| IpcError::internal(format!("writing volume.stopped: {e}")))?;

        register_prefetch_or_get(&self.ctx.prefetch_tracker, new_fork.vol_ulid);
        self.ctx.core.rescan.notify_one();
        self.job.append(ClaimAttachEvent::ForkCreated {
            new_vol_ulid: new_fork.vol_ulid,
        });
        info!(
            "[claim {}] finalized fork {new_vol_ulid_str} (parent {}/{})",
            self.volume, effective.vol, effective.snap
        );
        info!(
            "[claim {}] fork finalized in {:.2?}",
            self.volume,
            fork_create_started.elapsed()
        );
        Ok(())
    }

    /// Stage 6. Surface prefetch warm-up. Non-fatal: the bucket-side claim
    /// and local fork are durable by this point.
    async fn surface_prefetch(&self) {
        let new_fork = self
            .new_fork
            .as_ref()
            .expect("early_rebind must run before surface_prefetch");
        let prefetch_wait_started = std::time::Instant::now();
        self.job.append(ClaimAttachEvent::PrefetchStarted);
        let _ = await_prefetch_op(new_fork.vol_ulid, &self.ctx.prefetch_tracker).await;
        self.job.append(ClaimAttachEvent::PrefetchDone);
        info!(
            "[claim {}] prefetch awaited in {:.2?}",
            self.volume,
            prefetch_wait_started.elapsed()
        );
    }
}

// ── Private helpers ──────────────────────────────────────────────────────────

/// Implementation of [`ResolveHandoffKey`]: maps a (vol, snap) onto either
/// `Normal` or `Recovery { manifest_pubkey_hex }`, used by the orchestrator
/// before forking from the released chain.
async fn resolve_handoff_key_via_recovery(
    vol_ulid: Ulid,
    snap_ulid: Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<ResolveHandoffKeyReply, IpcError> {
    use elide_coordinator::recovery::{HandoffVerifier, resolve_handoff_verifier};

    match resolve_handoff_verifier(store, vol_ulid, snap_ulid).await {
        Ok(HandoffVerifier::Normal) => Ok(ResolveHandoffKeyReply::Normal),
        Ok(HandoffVerifier::Synthesised {
            manifest_pubkey, ..
        }) => {
            let hex: String = manifest_pubkey
                .as_ref()
                .to_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            Ok(ResolveHandoffKeyReply::Recovery {
                manifest_pubkey_hex: hex,
            })
        }
        Err(e) => Err(IpcError::internal(format!("{e}"))),
    }
}

/// Walk back through empty intermediate forks and return the deepest non-empty
/// ancestor as the source the new fork should pin to.
///
/// Each iteration:
///   1. Read the current effective fork's signed `volume.provenance` to find
///      its `parent` ref.
///   2. Fetch and verify its handoff manifest from S3.
///   3. If `max(segment_ulids) < parent.snapshot_ulid`, the fork produced no
///      writes of its own (every segment is inherited) — advance to its
///      parent and loop.
///   4. Otherwise, stop.
///
/// On loop advance, also pulls the parent's directory locally if not already
/// present — chain-pull in stage 3 stops at the first existing dir, but the
/// skip walk may need to reach further back.
///
/// Returns `(source_vol_ulid, snapshot_ulid, parent_key_hex)`. `parent_key_hex`
/// is the manifest_pubkey override carried in the chosen ancestor reference
/// (Some only for ancestors whose snapshot was a recovery snapshot).
///
/// Kept as a free fn (rather than folded into the `&mut self` method above)
/// so the existing test suite can drive it directly without needing to
/// construct a full [`IpcContext`] / [`ClaimOrchestrator`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn skip_empty_intermediates_impl(
    job: &Arc<ClaimJob>,
    volume: &str,
    released_vol_ulid: Ulid,
    handoff_snap: Ulid,
    initial_parent_key_hex: Option<String>,
    data_dir: &std::path::Path,
    stores: &Arc<dyn elide_coordinator::stores::ScopedStores>,
    peer: Option<&PeerFetchContext>,
    guard: &mut PulledAncestorsGuard,
) -> Result<(Ulid, Ulid, Option<String>), IpcError> {
    use elide_core::signing::{
        VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE, encode_hex, load_verifying_key,
        read_lineage_verifying_signature,
    };
    use elide_core::volume;

    let by_id_dir = data_dir.join("by_id");

    let mut effective_vol = released_vol_ulid;
    let mut effective_snap = handoff_snap;
    let mut effective_parent_key_hex = initial_parent_key_hex;

    loop {
        let dir = volume::resolve_ancestor_dir(&by_id_dir, &effective_vol.to_string());
        let lineage =
            read_lineage_verifying_signature(&dir, VOLUME_PUB_FILE, VOLUME_PROVENANCE_FILE)
                .map_err(|e| {
                    IpcError::internal(format!("reading provenance for {effective_vol}: {e}"))
                })?;
        let Some(parent) = lineage.parent else {
            // Root volume — nothing to skip to.
            break;
        };
        let parent_vol_ulid = Ulid::from_string(&parent.volume_ulid).map_err(|e| {
            IpcError::internal(format!(
                "malformed parent volume_ulid in {effective_vol}: {e}"
            ))
        })?;
        let parent_snap_ulid = Ulid::from_string(&parent.snapshot_ulid).map_err(|e| {
            IpcError::internal(format!(
                "malformed parent snapshot_ulid in {effective_vol}: {e}"
            ))
        })?;

        // Verify and read this fork's handoff manifest. The fallback pubkey
        // for a non-recovery manifest is the fork's own `volume.pub` on disk;
        // recovery manifests are auto-resolved by the helper.
        let fallback_pubkey = load_verifying_key(&dir, VOLUME_PUB_FILE).map_err(|e| {
            IpcError::internal(format!("loading volume.pub for {effective_vol}: {e}"))
        })?;

        let store = stores.for_volume(&effective_vol);
        let (manifest, _verifier) = elide_coordinator::recovery::fetch_verified_handoff_manifest(
            &store,
            effective_vol,
            effective_snap,
            &fallback_pubkey,
            peer,
        )
        .await
        .map_err(|e| {
            IpcError::internal(format!(
                "fetching handoff manifest {effective_vol}/{effective_snap}: {e}"
            ))
        })?;

        let is_empty = manifest
            .segment_ulids
            .last()
            .is_none_or(|m| *m < parent_snap_ulid);
        if !is_empty {
            break;
        }

        // Advance to parent. Pull it locally if not already there — stage 3
        // may not have reached this far back. Register the pull with the
        // guard so a downstream verification failure cleans it up.
        let parent_dir = volume::resolve_ancestor_dir(&by_id_dir, &parent.volume_ulid);
        if !parent_dir.exists() {
            job.append(ClaimAttachEvent::PullingAncestor {
                vol_ulid: parent_vol_ulid,
            });
            let parent_store = stores.for_volume(&parent_vol_ulid);
            guard.record(parent_vol_ulid);
            let _ = pull_readonly_op(parent_vol_ulid, data_dir, &parent_store, peer).await?;
        }

        info!(
            "[claim {volume}] skipping empty intermediate {effective_vol}; \
             using {parent_vol_ulid}/{parent_snap_ulid}"
        );

        effective_vol = parent_vol_ulid;
        effective_snap = parent_snap_ulid;
        effective_parent_key_hex = parent.manifest_pubkey.map(|k| encode_hex(&k));
    }

    Ok((effective_vol, effective_snap, effective_parent_key_hex))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use elide_coordinator::stores::PassthroughStores;
    use elide_core::signing::{
        ParentRef, ProvenanceLineage, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE,
        build_snapshot_manifest_bytes, encode_hex, load_verifying_key, setup_readonly_identity,
    };
    use elide_core::ulid_mint::UlidMint;
    use object_store::{ObjectStore, PutPayload, memory::InMemory};
    use tempfile::TempDir;

    struct Fork {
        vol: Ulid,
        snap: Ulid,
        signer: Arc<dyn elide_core::segment::SegmentSigner>,
        verifying_key: ed25519_dalek::VerifyingKey,
    }

    fn build_fork(
        data_dir: &std::path::Path,
        vol: Ulid,
        snap: Ulid,
        parent: Option<&Fork>,
    ) -> Fork {
        let dir = data_dir.join("by_id").join(vol.to_string());
        std::fs::create_dir_all(&dir).unwrap();

        let lineage = match parent {
            None => ProvenanceLineage::default(),
            Some(p) => ProvenanceLineage {
                parent: Some(ParentRef {
                    volume_ulid: p.vol.to_string(),
                    snapshot_ulid: p.snap.to_string(),
                    pubkey: p.verifying_key.to_bytes(),
                    manifest_pubkey: None,
                }),
                extent_index: vec![],
                oci_source: None,
            },
        };

        let signer =
            setup_readonly_identity(&dir, VOLUME_PUB_FILE, VOLUME_PROVENANCE_FILE, &lineage)
                .unwrap();
        let verifying_key = load_verifying_key(&dir, VOLUME_PUB_FILE).unwrap();
        Fork {
            vol,
            snap,
            signer,
            verifying_key,
        }
    }

    async fn upload_handoff_manifest(
        store: &Arc<dyn ObjectStore>,
        fork: &Fork,
        segment_ulids: &[Ulid],
    ) {
        let bytes = build_snapshot_manifest_bytes(fork.signer.as_ref(), segment_ulids, None);
        let key =
            elide_coordinator::upload::snapshot_manifest_key(&fork.vol.to_string(), fork.snap);
        store.put(&key, PutPayload::from(bytes)).await.unwrap();
    }

    fn passthrough(
        store: Arc<dyn ObjectStore>,
    ) -> Arc<dyn elide_coordinator::stores::ScopedStores> {
        Arc::new(PassthroughStores::new(store))
    }

    #[tokio::test]
    async fn empty_fork_is_skipped() {
        // R(writes) → F1(empty, released). Claim should fork from R, not F1.
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut mint = UlidMint::new(Ulid::nil());
        let seg_a = mint.next();
        let seg_b = mint.next();
        let r_snap = mint.next(); // > seg_a, seg_b
        let f1_snap = mint.next(); // > r_snap

        let r = build_fork(data_dir, mint.next(), r_snap, None);
        let f1 = build_fork(data_dir, mint.next(), f1_snap, Some(&r));

        // R's handoff manifest = [seg_a, seg_b].
        upload_handoff_manifest(&store, &r, &[seg_a, seg_b]).await;
        // F1 wrote nothing → manifest inherits R's segments verbatim.
        upload_handoff_manifest(&store, &f1, &[seg_a, seg_b]).await;

        let job = ClaimJob::new();
        let stores = passthrough(store);
        let (vol, snap, key_hex) = skip_empty_intermediates_impl(
            &job,
            "vol",
            f1.vol,
            f1.snap,
            None,
            data_dir,
            &stores,
            None,
            &mut PulledAncestorsGuard::new(data_dir.join("by_id")),
        )
        .await
        .unwrap();
        assert_eq!(vol, r.vol);
        assert_eq!(snap, r.snap);
        assert!(key_hex.is_none());
    }

    #[tokio::test]
    async fn chained_empties_collapse_to_deepest_non_empty() {
        // R(writes) → F1(empty) → F2(empty, released). Claim → R.
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut mint = UlidMint::new(Ulid::nil());
        let seg_a = mint.next();
        let r_snap = mint.next();
        let f1_snap = mint.next();
        let f2_snap = mint.next();

        let r = build_fork(data_dir, mint.next(), r_snap, None);
        let f1 = build_fork(data_dir, mint.next(), f1_snap, Some(&r));
        let f2 = build_fork(data_dir, mint.next(), f2_snap, Some(&f1));

        upload_handoff_manifest(&store, &r, &[seg_a]).await;
        upload_handoff_manifest(&store, &f1, &[seg_a]).await;
        upload_handoff_manifest(&store, &f2, &[seg_a]).await;

        let job = ClaimJob::new();
        let stores = passthrough(store);
        let (vol, snap, _) = skip_empty_intermediates_impl(
            &job,
            "vol",
            f2.vol,
            f2.snap,
            None,
            data_dir,
            &stores,
            None,
            &mut PulledAncestorsGuard::new(data_dir.join("by_id")),
        )
        .await
        .unwrap();
        assert_eq!(vol, r.vol);
        assert_eq!(snap, r.snap);
    }

    #[tokio::test]
    async fn non_empty_fork_is_not_skipped() {
        // R(writes) → F1(writes, released). Claim should fork from F1.
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut mint = UlidMint::new(Ulid::nil());
        let seg_a = mint.next();
        let r_snap = mint.next();
        let seg_b = mint.next(); // > r_snap → owned by F1
        let f1_snap = mint.next();

        let r = build_fork(data_dir, mint.next(), r_snap, None);
        let f1 = build_fork(data_dir, mint.next(), f1_snap, Some(&r));

        upload_handoff_manifest(&store, &r, &[seg_a]).await;
        upload_handoff_manifest(&store, &f1, &[seg_a, seg_b]).await;

        let job = ClaimJob::new();
        let stores = passthrough(store);
        let (vol, snap, _) = skip_empty_intermediates_impl(
            &job,
            "vol",
            f1.vol,
            f1.snap,
            None,
            data_dir,
            &stores,
            None,
            &mut PulledAncestorsGuard::new(data_dir.join("by_id")),
        )
        .await
        .unwrap();
        assert_eq!(vol, f1.vol);
        assert_eq!(snap, f1.snap);
    }

    #[tokio::test]
    async fn root_fork_with_no_parent_is_not_skipped() {
        // Released name points at a root volume (no parent). Even if its
        // handoff manifest is empty, there's nothing to skip to.
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut mint = UlidMint::new(Ulid::nil());
        let r_snap = mint.next();
        let r = build_fork(data_dir, mint.next(), r_snap, None);
        // Empty manifest — but no parent to redirect to.
        upload_handoff_manifest(&store, &r, &[]).await;

        let job = ClaimJob::new();
        let stores = passthrough(store);
        let (vol, snap, _) = skip_empty_intermediates_impl(
            &job,
            "vol",
            r.vol,
            r.snap,
            None,
            data_dir,
            &stores,
            None,
            &mut PulledAncestorsGuard::new(data_dir.join("by_id")),
        )
        .await
        .unwrap();
        assert_eq!(vol, r.vol);
        assert_eq!(snap, r.snap);
    }

    #[tokio::test]
    async fn manifest_pubkey_override_flows_through_when_skipping() {
        // F1's parent ref carries a manifest_pubkey override (recovery
        // snapshot at the grandparent). When we skip F1 (empty), the override
        // must be propagated as the new fork's parent_key_hex.
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut mint = UlidMint::new(Ulid::nil());
        let seg_a = mint.next();
        let r_snap = mint.next();
        let f1_snap = mint.next();

        let r = build_fork(data_dir, mint.next(), r_snap, None);
        let f1_vol = mint.next();

        // Construct F1 manually so we can inject a manifest_pubkey override.
        let f1_dir = data_dir.join("by_id").join(f1_vol.to_string());
        std::fs::create_dir_all(&f1_dir).unwrap();
        let override_pubkey_bytes = [0xCDu8; 32];
        let lineage = ProvenanceLineage {
            parent: Some(ParentRef {
                volume_ulid: r.vol.to_string(),
                snapshot_ulid: r.snap.to_string(),
                pubkey: r.verifying_key.to_bytes(),
                manifest_pubkey: Some(override_pubkey_bytes),
            }),
            extent_index: vec![],
            oci_source: None,
        };
        let f1_signer =
            setup_readonly_identity(&f1_dir, VOLUME_PUB_FILE, VOLUME_PROVENANCE_FILE, &lineage)
                .unwrap();
        let f1_vk = load_verifying_key(&f1_dir, VOLUME_PUB_FILE).unwrap();
        let f1 = Fork {
            vol: f1_vol,
            snap: f1_snap,
            signer: f1_signer,
            verifying_key: f1_vk,
        };

        upload_handoff_manifest(&store, &r, &[seg_a]).await;
        upload_handoff_manifest(&store, &f1, &[seg_a]).await;

        let job = ClaimJob::new();
        let stores = passthrough(store);
        let (vol, snap, key_hex) = skip_empty_intermediates_impl(
            &job,
            "vol",
            f1.vol,
            f1.snap,
            None,
            data_dir,
            &stores,
            None,
            &mut PulledAncestorsGuard::new(data_dir.join("by_id")),
        )
        .await
        .unwrap();
        assert_eq!(vol, r.vol);
        assert_eq!(snap, r.snap);
        assert_eq!(
            key_hex.as_deref(),
            Some(encode_hex(&override_pubkey_bytes).as_str())
        );
    }
}
