//\! Lifecycle verbs: start, stop, release, force-release, hydrate, and
//\! the helpers that support them.
//\!
//\! Every verb here either operates within `Role::Owner` (start, stop)
//\! or drives a role transition (release / force-release →
//\! `Observer { Released }`; hydrate_or_route resolves bucket-state
//\! against the current coordinator before routing `start` to the
//\! right path).
//\!
//\! Extracted from the original `inbound.rs` "Volume stop / start"
//\! section. The dispatcher in `super` (`dispatch_json`) calls these
//\! verb-level fns directly; helpers like `release_fast_path_handoff`
//\! and `promote_auto_snapshot` are also reachable cross-module via
//\! `pub(crate)` for `fork.rs`.

use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;
use tracing::{info, warn};

use elide_coordinator::SnapshotLockRegistry;
use elide_coordinator::ipc::{IpcError, ReleaseReply};
use elide_coordinator::volume_state::{STOPPED_FILE, write_released_marker};

use super::{
    CoordinatorCore, IpcContext, emit_release_aftermath, ensure_release_eligible,
    snapshot_take_new, snapshot_volume_kind,
};

/// True if `<data_dir>/by_name/<name>` resolves to a fork with a live
/// volume daemon — i.e. this host is actively serving `<name>`.
///
/// Used by recovery verbs (`release --force`, `claim`) to refuse when
/// the operator has typo'd a verb at their own running volume: those
/// verbs are designed for unreachable peers and would otherwise leave
/// on-disk state diverging from the bucket record. A `Released` or
/// `StoppedManual` fork is parked (no daemon, supervisor refuses to
/// relaunch) so it is not "running" for these checks.
pub(crate) fn local_daemon_running(data_dir: &Path, volume_name: &str) -> bool {
    use elide_coordinator::volume_state::VolumeLifecycle;
    let link = data_dir.join("by_name").join(volume_name);
    match VolumeLifecycle::resolve(&link) {
        Ok((_, shape)) => shape.is_running(),
        Err(_) => false,
    }
}

pub(crate) async fn stop_volume_op(
    volume_name: &str,
    force: bool,
    core: &CoordinatorCore,
    snapshot_locks: &SnapshotLockRegistry,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    hostname: Option<&str>,
) -> Result<(), IpcError> {
    use elide_coordinator::volume_state::VolumeLifecycle;
    let data_dir: &Path = &core.data_dir;
    let link = data_dir.join("by_name").join(volume_name);
    let (vol_dir, shape) = VolumeLifecycle::resolve(&link)
        .map_err(|e| IpcError::internal(format!("resolving local fork: {e}")))?;
    let vol_dir =
        vol_dir.ok_or_else(|| IpcError::not_found(format!("volume not found: {volume_name}")))?;

    // Idempotent shapes: already stopped, or terminal-released
    // locally. Importing has its own active subprocess to drain —
    // refuse with a hint rather than proceeding into the snapshot
    // path (which would error mid-flow). All other shapes proceed
    // with drain+halt; the `readonly` flag controls whether the
    // bucket-flip + drain steps run.
    match &shape {
        VolumeLifecycle::StoppedManual | VolumeLifecycle::Released { .. } => {
            return Ok(());
        }
        VolumeLifecycle::Importing { import_ulid } => {
            return Err(IpcError::conflict(format!(
                "volume '{volume_name}' is currently importing (job {import_ulid}); \
                 wait for the import to finish or cancel it first"
            )));
        }
        _ => {}
    }

    let readonly = shape.is_readonly_local();

    // Refuse to stop while a block-device client is connected. The ublk
    // transport always reports `Disconnected` today, so this is a future
    // hook rather than an active gate.
    if elide_coordinator::control::is_connected(&vol_dir).await
        == elide_coordinator::control::ConnectedStatus::Connected
    {
        return Err(IpcError::conflict(
            "client is connected; disconnect it first",
        ));
    }

    // Clean stop on a writable volume: drain pending and publish an
    // auto-snapshot before any state change. The auto-snapshot is what
    // gives a future `start` (this host, or another host via `claim`)
    // a basis to hydrate from. Skipped under `--force`.
    if !readonly && !force {
        match snapshot_volume_kind(
            volume_name,
            core,
            snapshot_locks,
            elide_core::signing::SnapshotKind::Auto,
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                return Err(IpcError::internal(format!(
                    "stop {volume_name}: drain/auto-snapshot failed: {e:#}; \
                     retry, or use `stop --force` to halt without a checkpoint"
                )));
            }
        }
    }

    // `stop` is a local-lifecycle verb: its job is to halt the daemon
    // on this host. The bucket update is best-effort and only applies
    // to writable volumes — readonly volumes have a `Readonly` bucket
    // record that is its own terminal state, so there is no Live → Stopped
    // transition to make.
    //
    // For writable volumes, the bucket update only succeeds for the
    // canonical case (record owned by us, Live → Stopped); every other
    // case (no record, already stopped, foreign-owned, Released,
    // transient store error) becomes a warning and we proceed with
    // the local halt. The daemon may legitimately still be running
    // while the bucket says Released — e.g. after a partial release
    // that flipped the bucket but failed to halt the process — and
    // `stop` must be able to recover from that. Halting our local
    // daemon never affects other hosts.
    if !readonly {
        use elide_coordinator::lifecycle::{LifecycleError, mark_stopped};
        match mark_stopped(store, volume_name, coord_id, hostname).await {
            Ok(_) => {}
            Err(LifecycleError::OwnershipConflict { held_by }) => {
                warn!(
                    "[inbound] stop {volume_name}: names/<name> is owned by coordinator \
                     {held_by}; halting locally, bucket record left untouched"
                );
            }
            Err(LifecycleError::InvalidTransition { from, .. }) => {
                warn!(
                    "[inbound] stop {volume_name}: names/<name> is in state {from:?}; \
                     halting locally, bucket record left untouched"
                );
            }
            Err(LifecycleError::Store(e)) => {
                warn!("[inbound] stop {volume_name}: failed to update names/<name>: {e}");
            }
        }
    }

    // Write the marker before sending shutdown so the supervisor won't restart.
    std::fs::write(vol_dir.join(STOPPED_FILE), "")
        .map_err(|e| IpcError::internal(format!("writing volume.stopped: {e}")))?;

    use elide_coordinator::control::ShutdownOutcome;
    match elide_coordinator::control::shutdown(&vol_dir).await {
        ShutdownOutcome::Acknowledged => {
            info!("[inbound] stopped volume {volume_name}");
            Ok(())
        }
        ShutdownOutcome::Failed(msg) => {
            // Roll back the marker so the supervisor doesn't strand a still-
            // running volume. (Note: the S3 state has already flipped to
            // Stopped; that's a soft inconsistency the operator can resolve
            // by issuing `volume start` once the underlying issue is fixed.)
            let _ = std::fs::remove_file(vol_dir.join(STOPPED_FILE));
            Err(IpcError::internal(format!("shutdown failed: {msg}")))
        }
        ShutdownOutcome::NotRunning => {
            // Volume process wasn't running — marker is correct as-is.
            info!("[inbound] stopped volume {volume_name} (process was not running)");
            Ok(())
        }
    }
}

/// `volume release --force`.
///
/// Override path for an unreachable previous owner: synthesise a
/// fresh handoff snapshot from S3-visible segments under the dead
/// fork's prefix, sign it with this coordinator's identity key, and
/// unconditionally rewrite `names/<name>` to `Released`.
///
/// Does **not** require a local symlink, does **not** drain any WAL
/// (the dead owner's WAL is unreachable), does **not** halt or touch
/// any local volume daemon. The data-loss boundary is "writes the
/// dead owner accepted but never promoted to S3" — same as the
/// crash-recovery contract elsewhere.
pub(crate) async fn force_release_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
) -> Result<ReleaseReply, IpcError> {
    use elide_coordinator::lifecycle;
    use elide_coordinator::recovery;

    // Refuse when the "dead peer" is actually this host's running
    // daemon. force-release is for unreachable peers; against a local
    // running fork it would leave on-disk state diverging from the
    // bucket record. The operator wants `volume stop` first.
    if local_daemon_running(data_dir, volume_name) {
        return Err(IpcError::conflict(format!(
            "volume '{volume_name}' is running on this host; \
             stop it first with: elide volume stop {volume_name}"
        )));
    }

    // Read the current record to learn which dead fork to recover from.
    let dead_vol_ulid = {
        use elide_coordinator::bucket_position::{OwnershipPosition, fetch_position};
        let (position, _) = fetch_position(store, volume_name, identity.coordinator_id_str())
            .await
            .map_err(|e| IpcError::store(format!("reading names/{volume_name}: {e}")))?;
        match position {
            OwnershipPosition::OwnedByUs { vol_ulid, .. }
            | OwnershipPosition::OwnedByOther { vol_ulid, .. } => vol_ulid,
            OwnershipPosition::Absent => {
                return Err(IpcError::not_found(format!(
                    "name '{volume_name}' has no S3 record"
                )));
            }
            OwnershipPosition::Released { .. } | OwnershipPosition::Readonly { .. } => {
                return Err(IpcError::conflict(format!(
                    "names/{volume_name} is not in a Live or Stopped state; \
                     force-release only overrides Live or Stopped records"
                )));
            }
        }
    };

    // Recovery pipeline: fetch dead fork's pubkey, then either
    // promote an existing auto-snapshot (fast path) or synthesise a
    // fresh handoff manifest from S3-visible segments (slow path).
    //
    // If `volume.pub` is absent the dead fork crashed during the
    // create-time window before the coordinator published it. No
    // segment could have been signed-and-verified under a missing key,
    // so the dead fork is provably empty: publish an empty synthesised
    // handoff and flip to Released.
    let dead_pub = recovery::fetch_volume_pub_optional(store, dead_vol_ulid)
        .await
        .map_err(|e| {
            IpcError::store(format!(
                "fetching volume.pub for released fork {dead_vol_ulid}: {e:#}"
            ))
        })?;

    // Fast path: if the dead owner went through a clean `stop` before
    // becoming unreachable, an `<S>.auto.manifest` is in S3 covering
    // the durable state at that point. Promote it server-side
    // instead of re-deriving the segment list. The promoted manifest
    // retains the dead owner's signature; claimants verify it under
    // the dead fork's `volume.pub`, same as any user snapshot.
    if let Some(dead_pub_ref) = dead_pub.as_ref() {
        match recovery::try_promote_auto_snapshot_for_force_release(
            store,
            dead_vol_ulid,
            dead_pub_ref,
        )
        .await
        {
            Ok(Some(promoted)) => {
                info!(
                    "[inbound] force-release {volume_name}: fast path — promoted \
                     dead owner's auto-snapshot {} (signed by dead volume.pub, \
                     no recovery metadata)",
                    promoted.snap_ulid
                );
                let outcome =
                    lifecycle::mark_released_force(store, volume_name, promoted.snap_ulid).await;
                return finalize_force_release(
                    volume_name,
                    data_dir,
                    store,
                    identity,
                    promoted.snap_ulid,
                    outcome,
                )
                .await;
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "[inbound] force-release {volume_name}: auto-snapshot \
                     promotion failed ({e:#}); falling back to synthesis"
                );
            }
        }
    }

    let segment_ulids: Vec<ulid::Ulid> = match dead_pub {
        Some(dead_pub) => {
            let recovered = recovery::list_and_verify_segments(store, dead_vol_ulid, &dead_pub)
                .await
                .map_err(|e| {
                    IpcError::store(format!(
                        "listing/verifying segments for released fork {dead_vol_ulid}: {e:#}"
                    ))
                })?;
            let ulids: Vec<ulid::Ulid> =
                recovered.segments.iter().map(|s| s.segment_ulid).collect();
            info!(
                "[inbound] force-release {volume_name}: recovered {} segments \
                 ({} dropped) from released fork {dead_vol_ulid}",
                ulids.len(),
                recovered.dropped,
            );
            ulids
        }
        None => {
            info!(
                "[inbound] force-release {volume_name}: released fork \
                 {dead_vol_ulid} has no volume.pub in bucket — treating as \
                 empty (create-time crash before pub upload)"
            );
            Vec::new()
        }
    };

    let published = recovery::mint_and_publish_synthesised_snapshot(
        store,
        dead_vol_ulid,
        &segment_ulids,
        identity.as_ref(),
        identity.coordinator_id_str(),
    )
    .await
    .map_err(|e| IpcError::store(format!("publishing synthesised snapshot: {e}")))?;

    // Unconditional flip of names/<name>.
    let outcome = lifecycle::mark_released_force(store, volume_name, published.snap_ulid).await;
    finalize_force_release(
        volume_name,
        data_dir,
        store,
        identity,
        published.snap_ulid,
        outcome,
    )
    .await
}

/// Handle the outcome of `mark_released_force` plus the best-effort
/// after-effects (local display marker, journal entry, breadcrumb
/// cleanup). Factored out so both the auto-promotion fast path and
/// the segment-list synthesis slow path in `force_release_volume_op`
/// converge on identical operator-visible behaviour once the bucket
/// flip outcome is known.
async fn finalize_force_release(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
    handoff_snapshot: ulid::Ulid,
    outcome: Result<
        elide_coordinator::lifecycle::ForceReleaseOutcome,
        elide_coordinator::lifecycle::LifecycleError,
    >,
) -> Result<ReleaseReply, IpcError> {
    use elide_coordinator::lifecycle::ForceReleaseOutcome;
    match outcome {
        Ok(ForceReleaseOutcome::Overwritten {
            dead_vol_ulid: d,
            displaced_coordinator_id,
        }) => {
            info!(
                "[inbound] force-released volume {volume_name} (released fork {d}) at \
                 handoff snapshot {handoff_snapshot}",
            );
            // force-release is also used to displace a *foreign*
            // coordinator's record without any local fork — in that
            // case the by_name symlink doesn't resolve and we
            // silently skip the marker write.
            let local_vol_dir =
                std::fs::canonicalize(data_dir.join("by_name").join(volume_name)).ok();
            emit_release_aftermath(
                data_dir,
                store,
                identity,
                volume_name,
                local_vol_dir.as_deref(),
                handoff_snapshot,
                d,
                elide_core::volume_event::EventKind::ForceReleased {
                    handoff_snapshot,
                    displaced_coordinator_id: displaced_coordinator_id
                        .unwrap_or_else(|| "<unknown>".to_string()),
                },
                true,
                "force-release",
            )
            .await;
            Ok(ReleaseReply { handoff_snapshot })
        }
        Ok(ForceReleaseOutcome::Absent) => {
            // Race: record disappeared between our read and our write.
            Err(IpcError::precondition_failed(format!(
                "names/{volume_name} vanished between read and force-write"
            )))
        }
        Ok(ForceReleaseOutcome::InvalidState { observed }) => {
            // Race: state changed under us. The handoff snapshot is
            // still published (harmless); operator can retry.
            Err(IpcError::precondition_failed(format!(
                "names/{volume_name} changed underneath us; now in state {observed:?}"
            )))
        }
        Err(e) => Err(IpcError::store(format!(
            "force-release flip failed (handoff snapshot {handoff_snapshot} already published): {e}"
        ))),
    }
}

/// Relinquish ownership of `<volume_name>` so any other coordinator can
/// `volume start` it. Composes the existing snapshot path:
///
/// 1. Refuse if the volume is readonly (no exclusive owner to release)
///    or a block-device client is connected (must disconnect cleanly first).
/// 2. If the volume is `stopped`, transparently bring it back up
///    (clear the marker, notify the supervisor, wait for
///    `control.sock`) — the drain step needs a running daemon.
/// 3. Verify S3 ownership before doing the expensive drain.
/// 4. Drain WAL → publish handoff snapshot via `snapshot_volume`.
/// 5. Send shutdown RPC to halt the daemon.
/// 6. Write `volume.stopped` marker so the supervisor won't restart.
/// 7. Conditional PUT to `names/<name>` setting state=Released and
///    recording the handoff snapshot ULID.
///
/// Two execution paths:
///
/// 1. **Fast path** (clean stopped volume, nothing to drain): reuse
///    the previously-published snapshot as the handoff, skip the
///    daemon restart entirely. Costs one S3 GET (ownership) + one
///    conditional PUT (flip).
///
/// 2. **Slow path** (WAL non-empty / pending uploads / GC handoffs /
///    new segments since last snapshot): bring the daemon up in
///    drain mode, run the existing snapshot pipeline, halt, flip.
pub(crate) async fn release_volume_op(
    volume_name: &str,
    store: &Arc<dyn ObjectStore>,
    ctx: &IpcContext,
) -> Result<ReleaseReply, IpcError> {
    let identity = &ctx.identity;
    let data_dir: &Path = &ctx.data_dir;
    let coord_id = identity.coordinator_id_str();
    let started = std::time::Instant::now();
    info!("[release {volume_name}] start");

    use elide_coordinator::volume_state::VolumeLifecycle;
    let link = data_dir.join("by_name").join(volume_name);
    let (vol_dir, shape) = VolumeLifecycle::resolve(&link)
        .map_err(|e| IpcError::internal(format!("resolving local fork: {e}")))?;
    let vol_dir = match (vol_dir, &shape) {
        (Some(d), _) => d,
        (None, _) => {
            // No local fork — but a `remote/<name>` breadcrumb plus a
            // bucket record that names us as owner is enough to release:
            // the auto-snapshot from the preceding clean `stop` (which
            // ran before `remove`) already covers the durable state, so
            // there's no drain to do. Just flip the bucket to Released
            // using that snapshot as the handoff, and clear the
            // breadcrumb.
            //
            // This makes `stop → remove → release` work without forcing
            // the operator to detour through `start → stop` first just
            // to hand off a name they've already mentally given up.
            return release_breadcrumb_only(
                volume_name,
                data_dir,
                store,
                identity,
                coord_id,
                started,
            )
            .await;
        }
    };

    // `release` is composed of two distinct phases: drain+publish (needs
    // a running daemon) and bucket-flip (no daemon needed). To keep the
    // operator-visible state coherent we require the daemon to already
    // be `stopped` — otherwise a release on a running volume would have
    // to halt it inline, and any failure between halt and bucket-flip
    // would leave the volume in a "Released-but-running" mismatch the
    // operator can't easily recover from.
    match &shape {
        VolumeLifecycle::StoppedManual => {}
        VolumeLifecycle::ReadonlyImported | VolumeLifecycle::Fetched { .. } => {
            return Err(IpcError::conflict("volume is readonly; nothing to release"));
        }
        VolumeLifecycle::Released { .. } => {
            return Err(IpcError::conflict(format!(
                "name '{volume_name}' is already released"
            )));
        }
        _ => {
            return Err(IpcError::conflict(format!(
                "volume '{volume_name}' is running; \
                 stop it first with: elide volume stop {volume_name}"
            )));
        }
    }

    // Verify ownership in S3 before doing any local state mutation.
    // Pulled ahead of the daemon restart so a "wrong owner" or
    // "already released" reply doesn't perturb the local volume.
    use elide_coordinator::bucket_position::fetch_position;
    let read_started = std::time::Instant::now();
    let (position, _) = fetch_position(store, volume_name, coord_id)
        .await
        .map_err(|e| IpcError::store(format!("reading names/{volume_name}: {e}")))?;
    info!(
        "[release {volume_name}] read names/<name>: position={position:?} ({:.2?})",
        read_started.elapsed()
    );
    ensure_release_eligible(
        &position,
        volume_name,
        format!("name '{volume_name}' has no S3 record; drain the volume first"),
    )?;

    // Reuse the latest published snapshot as the handoff. The next
    // claimant forks from it identically to a freshly-minted one. If
    // the covering snapshot is an `Auto` (written by the preceding
    // `stop`), promote it to a stable user manifest first — `Released`
    // names must point at stable bases since claim/fork lineages will
    // be built on top.
    //
    // Refuses if the latest snapshot does not cover all durable state
    // (WAL/pending/gc has work, or segments post-date the snapshot).
    // The operator's recovery is `start` → `stop` (clean drain) →
    // `release` — `release` itself is a pure bucket-flip, no daemon
    // interaction.
    let volume_id_for_promote = elide_coordinator::upload::derive_names(&vol_dir).map_err(|e| {
        IpcError::internal(format!("[release {volume_name}] deriving volume id: {e}"))
    })?;
    let cover = match release_fast_path_handoff(&vol_dir) {
        Ok(Some(cover)) => cover,
        Ok(None) => {
            return Err(IpcError::conflict(format!(
                "volume '{volume_name}' has durable state past the last snapshot \
                 (WAL/pending uploads not yet drained); the previous stop did not \
                 complete a clean drain. Recover with: \
                 `elide volume start {volume_name}` then \
                 `elide volume stop {volume_name}`, then re-run release"
            )));
        }
        Err(e) => {
            return Err(IpcError::internal(format!(
                "release fast-path inspection failed: {e}"
            )));
        }
    };
    if cover.kind == elide_core::signing::SnapshotKind::Auto {
        info!(
            "[release {volume_name}] promoting auto-snapshot {} → stable manifest",
            cover.snap_ulid
        );
        if let Err(e) =
            promote_auto_snapshot(&vol_dir, &volume_id_for_promote, cover.snap_ulid, store).await
        {
            return Err(IpcError::internal(format!(
                "promoting auto-snapshot {} for release: {e}",
                cover.snap_ulid
            )));
        }
    }
    info!(
        "[release {volume_name}] reusing snapshot {} (clean stopped volume)",
        cover.snap_ulid
    );
    let result = perform_release_flip(
        volume_name,
        data_dir,
        &vol_dir,
        store,
        identity,
        cover.snap_ulid,
    )
    .await;
    info!(
        "[release {volume_name}] complete in {:.2?}",
        started.elapsed()
    );
    result
}

/// Final S3 conditional PUT flipping `names/<name>` to Released.
///
/// On success also writes `volume.released` into `vol_dir` as a
/// best-effort display marker — the bucket record is authoritative,
/// the local marker only drives `volume list` rendering.
/// Breadcrumb-only release: there's no local fork, but a
/// `data_dir/remote/<name>` breadcrumb plus a bucket record that
/// names us as owner is enough to release. The preceding clean
/// `stop` published an auto-snapshot that covers the durable state;
/// use it as the handoff, flip `names/<name>` to `Released`, and
/// clear the breadcrumb.
///
/// Refuses if:
///   - The breadcrumb is absent (no record of ever owning this).
///   - The bucket record is missing or owned by another coordinator
///     (the cross-host case — operator must `release --force` from
///     a host that's actually the dead owner).
///   - The bucket record is already `Released` (idempotent failure
///     to give better operator feedback).
///   - No snapshot exists under `by_id/<vol_ulid>/snapshots/` (the
///     auto-snapshot was somehow lost; cross-host recovery via
///     `release --force` is the only remaining path, and even that
///     would synthesise).
async fn release_breadcrumb_only(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
    coord_id: &str,
    started: std::time::Instant,
) -> Result<ReleaseReply, IpcError> {
    use elide_coordinator::lifecycle::{self, MarkReleasedOutcome};

    // Breadcrumb is the local fingerprint of "we still own this
    // name remotely". Without one, this volume isn't a candidate for
    // breadcrumb-only release — surface the same not-found error
    // operators expect.
    let breadcrumb = elide_coordinator::remote_breadcrumb::read(data_dir, volume_name)
        .map_err(|e| IpcError::internal(format!("reading breadcrumb: {e}")))?;
    let Some(_breadcrumb) = breadcrumb else {
        return Err(IpcError::not_found(format!(
            "volume not found: {volume_name}"
        )));
    };

    use elide_coordinator::bucket_position::fetch_position;
    let (position, fetched) = fetch_position(store, volume_name, coord_id)
        .await
        .map_err(|e| IpcError::store(format!("reading names/{volume_name}: {e}")))?;
    ensure_release_eligible(
        &position,
        volume_name,
        format!(
            "name '{volume_name}' has no S3 record despite local breadcrumb; \
             stale breadcrumb — remove `{}/remote/{volume_name}` to dismiss",
            data_dir.display()
        ),
    )?;
    let rec = fetched
        .expect("ensure_release_eligible(OwnedByUs) implies fetched is Some")
        .0;

    // Find the latest published snapshot for this vol_ulid to use as
    // the handoff. The bucket should have an auto-snapshot from the
    // preceding `stop` even though the local fork is gone. If there
    // is no snapshot at all (the volume was minted via `claim`, never
    // started or stopped, then removed), synthesise an empty handoff
    // signed by the volume's own key from the local shadow.
    let snap_ulid = match latest_release_handoff_snapshot(rec.vol_ulid, store).await? {
        Some((snap_ulid, elide_core::signing::SnapshotKind::User)) => snap_ulid,
        Some((snap_ulid, elide_core::signing::SnapshotKind::Auto)) => {
            // Promote auto → user in S3 before flipping. Claimants
            // resolve the handoff key as `<vol>/snapshots/<.../>
            // <snap>.manifest` (not `.auto.manifest`), so an
            // unpromoted auto-snapshot would surface as a NotFound on
            // claim.
            promote_auto_in_store(&rec.vol_ulid.to_string(), snap_ulid, store).await?;
            snap_ulid
        }
        None => synthesise_empty_owner_handoff(volume_name, data_dir, rec.vol_ulid, store).await?,
    };
    info!(
        "[release {volume_name}] breadcrumb-only: handoff snapshot {snap_ulid} \
         (no local fork, no drain needed)"
    );

    let outcome = lifecycle::mark_released(store, volume_name, coord_id, snap_ulid).await;
    match outcome {
        Ok(MarkReleasedOutcome::Updated { vol_ulid }) => {
            info!(
                "[release {volume_name}] released at handoff snapshot {snap_ulid} \
                 (breadcrumb-only, total {:.2?})",
                started.elapsed()
            );
            emit_release_aftermath(
                data_dir,
                store,
                identity,
                volume_name,
                None,
                snap_ulid,
                vol_ulid,
                elide_core::volume_event::EventKind::Released {
                    handoff_snapshot: snap_ulid,
                },
                true,
                "release",
            )
            .await;
            Ok(ReleaseReply {
                handoff_snapshot: snap_ulid,
            })
        }
        Ok(other) => Err(IpcError::store(format!(
            "release flip for {volume_name}: unexpected outcome {other:?}"
        ))),
        Err(e) => Err(IpcError::store(format!("release flip: {e}"))),
    }
}

/// List `by_id/<vol_ulid>/snapshots/` in the bucket and return the
/// highest ULID with its kind (User from `<u>.manifest`, Auto from
/// `<u>.auto.manifest`). Used by breadcrumb-only release to pick
/// the handoff snapshot and decide whether to promote it before the
/// bucket flip. On ties (both kinds at the same ULID — a transient
/// state from an interrupted prior promotion) User wins, matching
/// the precedence in `latest_snapshot_marker`.
async fn latest_release_handoff_snapshot(
    vol_ulid: ulid::Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<Option<(ulid::Ulid, elide_core::signing::SnapshotKind)>, IpcError> {
    use futures::TryStreamExt;
    use object_store::path::Path as StorePath;
    let prefix = StorePath::from(format!("by_id/{vol_ulid}/snapshots/"));
    let objects: Vec<object_store::ObjectMeta> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(|e| IpcError::store(format!("listing snapshots for {vol_ulid}: {e}")))?;
    let mut latest: Option<(ulid::Ulid, elide_core::signing::SnapshotKind)> = None;
    for obj in objects {
        let Some(filename) = obj.location.filename() else {
            continue;
        };
        let Some((u, kind)) = elide_core::signing::parse_snapshot_filename(filename) else {
            continue;
        };
        latest = match latest {
            None => Some((u, kind)),
            Some(cur) if snapshot_take_new((u, kind), cur) => Some((u, kind)),
            cur => cur,
        };
    }
    Ok(latest)
}

/// Server-side promote an auto-snapshot to a stable user manifest in
/// S3, without touching any local state. Used by breadcrumb-only
/// release when the latest published basis is `<S>.auto.manifest` —
/// claimants resolve the handoff via the stable filename, so the
/// auto must be re-addressed before the bucket flip. Equivalent to
/// the S3-side half of `promote_auto_snapshot` (used by the local
/// fast path); shared helper would be nice but the local variant
/// also needs to rename the sentinel + manifest on disk, which
/// doesn't apply here.
/// Sign and publish an empty handoff manifest for an owned volume
/// that never accumulated any segments. Reached only from the
/// breadcrumb-only release path when `latest_release_handoff_snapshot`
/// returns `None` — i.e. the operator did
/// `volume claim` → `volume remove` → `volume release` without ever
/// starting the volume, so the daemon never wrote anything and no
/// auto-snapshot was published at stop time.
///
/// Signs with the volume's own key (loaded from the local
/// `data_dir/keys/<vol_ulid>.key` shadow). The result is a normal
/// `<S>.manifest` with zero segment ULIDs and no recovery metadata —
/// identical shape to what `volume stop` would have published if the
/// volume had run with no writes.
///
/// Refuses if no key shadow exists. That would mean we own the
/// bucket record but the host has no record of ever minting the
/// volume — an inconsistency the operator should resolve via
/// `release --force` from somewhere with credentials.
///
/// The `snap_ulid` is freshly minted coordinator-side. This is one
/// of the few legitimate coordinator-side ULID mints — there is no
/// volume actor to consult (the local fork is gone), and there's no
/// other handoff to compare against.
async fn synthesise_empty_owner_handoff(
    volume_name: &str,
    data_dir: &Path,
    vol_ulid: ulid::Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<ulid::Ulid, IpcError> {
    let shadow = elide_coordinator::key_shadow::read(data_dir, vol_ulid)
        .map_err(|e| IpcError::internal(format!("reading key shadow: {e}")))?;
    let key_bytes = shadow.ok_or_else(|| {
        IpcError::not_found(format!(
            "name '{volume_name}' has no snapshot in the store and no key shadow \
             locally — recover via `release --force` from another host"
        ))
    })?;
    let (signer, _vk) = elide_core::signing::signer_from_bytes(&key_bytes)
        .map_err(|e| IpcError::internal(format!("loading shadow signer: {e}")))?;

    let snap_ulid = ulid::Ulid::new();
    let manifest_bytes =
        elide_core::signing::build_snapshot_manifest_bytes(signer.as_ref(), &[], None);
    let key = elide_coordinator::upload::snapshot_manifest_key(&vol_ulid.to_string(), snap_ulid);
    store
        .put(&key, manifest_bytes.into())
        .await
        .map_err(|e| IpcError::store(format!("publishing empty handoff manifest {key}: {e}")))?;
    info!(
        "[release {volume_name}] breadcrumb-only: synthesised empty handoff \
         {snap_ulid} (signed by local key shadow)"
    );
    Ok(snap_ulid)
}

/// S3-side half of an auto→user promotion: server-side COPY of
/// `<S>.auto.manifest` to `<S>.manifest`, then DELETE of the auto
/// key. Best-effort on the DELETE — a leftover redundant
/// `.auto.manifest` is benign (the reader path prefers User on a
/// tie). Shared between the breadcrumb-only release path (S3 only)
/// and the local fast-path release (which wraps with local file +
/// sentinel renames).
async fn promote_auto_in_store(
    vol_ulid: &str,
    snap_ulid: ulid::Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), IpcError> {
    let auto_key = elide_coordinator::upload::auto_snapshot_manifest_key(vol_ulid, snap_ulid);
    let user_key = elide_coordinator::upload::snapshot_manifest_key(vol_ulid, snap_ulid);
    store.copy(&auto_key, &user_key).await.map_err(|e| {
        IpcError::store(format!("copying {auto_key} → {user_key} on promotion: {e}"))
    })?;
    if let Err(e) = store.delete(&auto_key).await {
        warn!("[promote-auto {snap_ulid}] deleting {auto_key}: {e}");
    }
    Ok(())
}

async fn perform_release_flip(
    volume_name: &str,
    data_dir: &Path,
    vol_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
    snap_ulid: ulid::Ulid,
) -> Result<ReleaseReply, IpcError> {
    use elide_coordinator::lifecycle::{self, MarkReleasedOutcome};
    let flip_started = std::time::Instant::now();
    info!(
        "[release {volume_name}] flipping names/<name> -> Released \
         with handoff snapshot {snap_ulid}"
    );
    match lifecycle::mark_released(store, volume_name, identity.coordinator_id_str(), snap_ulid)
        .await
    {
        Ok(MarkReleasedOutcome::Updated { vol_ulid }) => {
            info!(
                "[release {volume_name}] released at handoff snapshot {snap_ulid} \
                 (flip {:.2?})",
                flip_started.elapsed()
            );
            // No breadcrumb to clear here: this path runs when a
            // local fork existed, which means no breadcrumb was
            // written (`remove` is the only producer).
            emit_release_aftermath(
                data_dir,
                store,
                identity,
                volume_name,
                Some(vol_dir),
                snap_ulid,
                vol_ulid,
                elide_core::volume_event::EventKind::Released {
                    handoff_snapshot: snap_ulid,
                },
                false,
                "release",
            )
            .await;
            Ok(ReleaseReply {
                handoff_snapshot: snap_ulid,
            })
        }
        Ok(_) => {
            info!(
                "[release {volume_name}] release flip was idempotent or absent \
                 (no event emitted)"
            );
            // Idempotent path: the bucket record is already Released. Best-
            // effort backfill the local display marker so `volume list`
            // shows the right state on hosts that released earlier.
            if let Err(e) = write_released_marker(vol_dir, snap_ulid) {
                warn!(
                    "[release {volume_name}] writing volume.released marker: {e} \
                     (display-only; bucket state authoritative)"
                );
            }
            Ok(ReleaseReply {
                handoff_snapshot: snap_ulid,
            })
        }
        Err(e) => {
            warn!("[release {volume_name}] state flip failed: {e}");
            Err(IpcError::store(format!(
                "snapshot {snap_ulid} published but names/<name> update failed: {e}"
            )))
        }
    }
}

/// Decide whether `release` can short-circuit using the volume's most
/// recently published snapshot as the handoff point.
///
/// Returns `Ok(Some(ulid))` when **all** of the following hold:
///   - `wal/`, `pending/`, `gc/` are empty or absent (no in-flight work)
///   - the latest segment in `index/` does not post-date the latest
///     local snapshot marker (the snapshot covers everything)
///   - that snapshot's S3 upload sentinel is present (manifest +
///     marker + filemap are confirmed on S3 — without this, a future
///     claimant could fail to fetch the manifest)
///
/// `Ok(None)` means slow path required; an `Err` is propagated to the
/// caller as a fast-path inspection failure (also slow-path fallback).
/// Result of a successful release fast-path inspection: the snapshot
/// ULID to use as the handoff basis, plus the kind on disk. An `Auto`
/// kind triggers the rename/copy promotion to `<ulid>.manifest`
/// before the bucket flip — auto-snapshots are not stable enough to
/// serve as a Released-name basis without that step.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FastPathCover {
    pub(crate) snap_ulid: ulid::Ulid,
    pub(crate) kind: elide_core::signing::SnapshotKind,
}

/// Promote `<ulid>.auto.manifest` to `<ulid>.manifest` both locally
/// and in S3, plus rename the upload sentinel so the next drain
/// doesn't try to re-upload anything. The signed bytes are byte-
/// identical between kinds — filename is just addressing — so the
/// S3 step is a server-side COPY + DELETE: zero data transit through
/// the coordinator, no re-sign.
///
/// Ordering: S3 first (COPY new key, DELETE old key), then locally.
/// A crash between the COPY and the DELETE leaves both
/// `<ulid>.manifest` and `<ulid>.auto.manifest` in S3; the reader
/// path (and `latest_snapshot_marker`) prefers `User` on a tie, so
/// the transient state is benign. A crash between S3-OK and
/// local-rename leaves stale `<ulid>.auto.manifest` on disk — since
/// the volume is about to be released and removed locally, this is
/// irrelevant.
pub(crate) async fn promote_auto_snapshot(
    vol_dir: &Path,
    volume_id: &str,
    snap_ulid: ulid::Ulid,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), IpcError> {
    // 1+2. Server-side COPY + DELETE in S3 via the shared helper.
    promote_auto_in_store(volume_id, snap_ulid, store).await?;

    // 3. Rename the local files. Best-effort: if the local copy was
    //    already cleaned by NotifyVolumeReady at start, the source
    //    won't exist and the rename returns NotFound — that's fine,
    //    S3 is the source of truth and is already promoted.
    let snap_dir = vol_dir.join("snapshots");
    let from = snap_dir.join(elide_core::signing::auto_snapshot_manifest_filename(
        &snap_ulid,
    ));
    let to = snap_dir.join(elide_core::signing::snapshot_manifest_filename(&snap_ulid));
    match std::fs::rename(&from, &to) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            warn!(
                "[promote-auto {snap_ulid}] renaming local manifest {} → {}: {e}",
                from.display(),
                to.display()
            );
        }
    }

    // 4. Rename the upload sentinel so the drain loop doesn't try
    //    to re-upload anything for either kind.
    let uploaded_dir = vol_dir.join("uploaded").join("snapshots");
    let sentinel_from = uploaded_dir.join(format!("{snap_ulid}.auto"));
    let sentinel_to = uploaded_dir.join(snap_ulid.to_string());
    match std::fs::rename(&sentinel_from, &sentinel_to) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            warn!(
                "[promote-auto {snap_ulid}] renaming sentinel {} → {}: {e}",
                sentinel_from.display(),
                sentinel_to.display()
            );
        }
    }

    Ok(())
}

pub(crate) fn release_fast_path_handoff(vol_dir: &Path) -> std::io::Result<Option<FastPathCover>> {
    if !dir_is_empty_or_absent(&vol_dir.join("wal"))? {
        return Ok(None);
    }
    if !dir_is_empty_or_absent(&vol_dir.join("pending"))? {
        return Ok(None);
    }
    if !dir_is_empty_or_absent(&vol_dir.join("gc"))? {
        return Ok(None);
    }

    let Some((snap_ulid, kind)) = latest_snapshot_marker(&vol_dir.join("snapshots"))? else {
        return Ok(None);
    };

    // The snapshot pair (marker + .manifest / .auto.manifest) is
    // uploaded atomically; the sentinel is written only after both
    // succeed. Its presence is the canonical "this snapshot is on
    // S3" check. User and Auto have distinct sentinel labels under
    // `uploaded/snapshots/` — see `upload_snapshot_metadata`.
    let sentinel_name = match kind {
        elide_core::signing::SnapshotKind::User => snap_ulid.to_string(),
        elide_core::signing::SnapshotKind::Auto => format!("{snap_ulid}.auto"),
    };
    let sentinel = vol_dir
        .join("uploaded")
        .join("snapshots")
        .join(&sentinel_name);
    if !sentinel.exists() {
        return Ok(None);
    }

    if let Some(seg) = latest_segment_ulid(&vol_dir.join("index"))?
        && seg > snap_ulid
    {
        return Ok(None);
    }

    Ok(Some(FastPathCover { snap_ulid, kind }))
}

fn dir_is_empty_or_absent(p: &Path) -> std::io::Result<bool> {
    match std::fs::read_dir(p) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e),
    }
}

/// Return the highest snapshot ULID found under `snap_dir`, paired
/// with the kind (User from `<ulid>.manifest`, Auto from
/// `<ulid>.auto.manifest`). Filemap and other siblings are skipped.
/// On ties (same ULID present in both forms) the User kind wins —
/// release's auto-promotion path is the only producer of that
/// transient state, and the just-written stable manifest is the
/// authoritative artefact.
fn latest_snapshot_marker(
    snap_dir: &Path,
) -> std::io::Result<Option<(ulid::Ulid, elide_core::signing::SnapshotKind)>> {
    let entries = match std::fs::read_dir(snap_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut latest: Option<(ulid::Ulid, elide_core::signing::SnapshotKind)> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(s) = name.to_str() else { continue };
        let Some((u, k)) = elide_core::signing::parse_snapshot_filename(s) else {
            continue;
        };
        latest = match latest {
            None => Some((u, k)),
            Some(cur) if snapshot_take_new((u, k), cur) => Some((u, k)),
            cur => cur,
        };
    }
    Ok(latest)
}

/// Return the highest ULID among `index/<ulid>.idx` files.
fn latest_segment_ulid(index_dir: &Path) -> std::io::Result<Option<ulid::Ulid>> {
    let entries = match std::fs::read_dir(index_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut latest: Option<ulid::Ulid> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(s) = name.to_str() else { continue };
        let Some(stem) = s.strip_suffix(".idx") else {
            continue;
        };
        if let Ok(u) = ulid::Ulid::from_string(stem)
            && latest.is_none_or(|cur| u > cur)
        {
            latest = Some(u);
        }
    }
    Ok(latest)
}

/// `volume claim <name>` IPC handler.
///
/// Inspects `names/<name>` and either:
///   - reclaims in place (own released fork still on disk) → `ok reclaimed`
///   - directs the CLI to orchestrate a foreign claim → `released <vol_ulid> <snap>`
///   - refuses if the record is `Live`/`Stopped` and owned by another
///     coordinator. The operator must run `release --force` first to
///     declare the previous owner dead and flip the record to
///     `Released`. Splitting the verbs keeps the claim step
///     CAS-protected (via `mark_claimed`) so concurrent claimants
///     are arbitrated by the conditional PUT, not by the unconditional
///     overwrite that `release --force` performs.
///
/// The result always leaves the volume `Stopped` (no daemon launched).
/// The CLI calls `start` afterwards if `volume start --claim` was the
/// composed flow.
/// Resolve `volume start <name>` against the bucket when no local fork
/// exists. Hydrates a remote-owned volume into local state on success;
/// surfaces the appropriate error otherwise (foreign-owned → conflict,
/// released → claim hint, unknown → not_found).
pub(crate) async fn hydrate_or_route(
    volume_name: &str,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    core: &CoordinatorCore,
) -> Result<(), IpcError> {
    use elide_coordinator::bucket_position::{OwnershipPosition, fetch_position};

    let (position, record) = fetch_position(store, volume_name, coord_id)
        .await
        .map_err(|e| IpcError::store(format!("reading names/{volume_name}: {e}")))?;

    match position {
        OwnershipPosition::Absent => Err(IpcError::not_found(format!(
            "volume '{volume_name}' not found locally or in the bucket"
        ))),
        OwnershipPosition::OwnedByUs { vol_ulid, .. } => {
            // Need `size` for the hydrate; pulled from the record we
            // already read.
            let size = record
                .expect("fetch_position returned OwnedByUs => record is Some")
                .0
                .size;
            crate::start_remote::hydrate_remote_owned(volume_name, vol_ulid, size, store, core)
                .await
        }
        OwnershipPosition::OwnedByOther {
            coord_id: held_by, ..
        } => Err(IpcError::conflict(format!(
            "name '{volume_name}' is held by coordinator {held_by}; \
                 run `volume release --force` to override"
        ))),
        OwnershipPosition::Released { .. } => Err(IpcError::conflict(format!(
            "name '{volume_name}' is Released; \
             reclaim with: elide volume claim {volume_name}"
        ))),
        OwnershipPosition::Readonly { .. } => Err(IpcError::conflict(format!(
            "name '{volume_name}' is readonly; cannot start"
        ))),
    }
}

pub(crate) async fn start_volume_op(
    volume_name: &str,
    core: &CoordinatorCore,
) -> Result<(), IpcError> {
    use elide_coordinator::volume_state::VolumeLifecycle;
    let data_dir: &Path = &core.data_dir;
    let store = core.stores.coordinator_wide();
    let coord_id = core.identity.coordinator_id_str();
    let hostname = core.identity.hostname();
    let link = data_dir.join("by_name").join(volume_name);

    // Resolve local fork state up front. `Absent` → route via the
    // hydrate-or-claim-hint pipeline, then re-resolve to inspect the
    // freshly-planted fork.
    let (vol_dir, shape) = {
        let (maybe_dir, shape) = VolumeLifecycle::resolve(&link)
            .map_err(|e| IpcError::internal(format!("resolving local fork: {e}")))?;
        match maybe_dir {
            Some(dir) => (dir, shape),
            None => {
                hydrate_or_route(volume_name, &store, coord_id, core).await?;
                let (dir, shape) = VolumeLifecycle::resolve(&link).map_err(|e| {
                    IpcError::internal(format!("resolving local fork post-hydrate: {e}"))
                })?;
                let dir = dir.ok_or_else(|| {
                    IpcError::internal(format!(
                        "hydrate {volume_name}: by_name symlink still absent after hydrate"
                    ))
                })?;
                (dir, shape)
            }
        }
    };

    // Start only proceeds against a writable fork explicitly parked
    // by `volume.stopped`. Every other shape gets a verb-specific
    // error so the operator hint is actionable.
    match &shape {
        VolumeLifecycle::StoppedManual => {}
        VolumeLifecycle::Running { pid } => {
            return Err(IpcError::conflict(format!(
                "volume '{volume_name}' is already running (pid {pid})"
            )));
        }
        VolumeLifecycle::Importing { import_ulid } => {
            return Err(IpcError::conflict(format!(
                "volume '{volume_name}' is currently importing (job {import_ulid})"
            )));
        }
        VolumeLifecycle::Stopped => {
            return Err(IpcError::conflict("volume is not stopped"));
        }
        VolumeLifecycle::Released { .. } => {
            return Err(IpcError::conflict(format!(
                "volume '{volume_name}' is released; \
                 reclaim with: elide volume claim {volume_name}"
            )));
        }
        VolumeLifecycle::ReadonlyImported => {
            return Err(IpcError::conflict(format!(
                "volume '{volume_name}' is readonly (imported base); nothing to start"
            )));
        }
        VolumeLifecycle::Fetched { .. } => {
            return Err(IpcError::conflict(format!(
                "volume '{volume_name}' is a fetched readonly copy; \
                 use `volume claim {volume_name}` to take ownership first"
            )));
        }
        VolumeLifecycle::Absent => {
            return Err(IpcError::internal(format!(
                "hydrate {volume_name}: classified Absent after successful hydrate"
            )));
        }
    }

    // Self-heal the signing-key shadow at `data_dir/keys/<vol_ulid>.key`.
    // For volumes created after key-shadow landed, the shadow already
    // exists from create / fork / claim time; for pre-existing volumes
    // this is the migration path. Cheap (32-byte file write) and
    // best-effort — failures log but don't block start.
    if let Some(vol_ulid) = vol_dir
        .file_name()
        .and_then(|s| s.to_str())
        .and_then(|s| ulid::Ulid::from_string(s).ok())
        && let Err(e) = self_heal_key_shadow(data_dir, &vol_dir, vol_ulid)
    {
        warn!("[inbound] start {volume_name}: self-heal key shadow: {e}");
    }

    use elide_coordinator::lifecycle::{LifecycleError, MarkLiveOutcome, mark_live};
    match mark_live(&store, volume_name, coord_id, hostname).await {
        Ok(MarkLiveOutcome::Resumed) | Ok(MarkLiveOutcome::AlreadyLive) => {}
        Ok(MarkLiveOutcome::Absent) => {
            // No S3 record yet — proceed local-only.
        }
        Ok(MarkLiveOutcome::Released) => {
            return Err(IpcError::conflict(format!(
                "name '{volume_name}' is Released; \
                 reclaim with: elide volume claim {volume_name}"
            )));
        }
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            return Err(IpcError::conflict(format!(
                "name '{volume_name}' is owned by coordinator {held_by}; \
                 run `volume release --force` to override"
            )));
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            return Err(IpcError::conflict(format!(
                "names/<name> is in state {from:?}; cannot start"
            )));
        }
        Err(LifecycleError::Store(e)) => {
            warn!("[inbound] start {volume_name}: failed to update names/<name>: {e}");
        }
    }

    std::fs::remove_file(vol_dir.join(STOPPED_FILE))
        .map_err(|e| IpcError::internal(format!("clearing volume.stopped: {e}")))?;
    crate::rescan::trigger();

    // Note: auto-snapshot cleanup is no longer done here. The volume
    // binary signals readiness via `NotifyVolumeReady` once
    // `Volume::open` succeeds, and that handler cleans up the
    // auto-snapshot. This avoids a window where `start` returned OK
    // but the daemon failed to open, leaving the user with the
    // bucket-side basis already deleted and no way to recover.

    info!("[inbound] started volume {volume_name}");
    Ok(())
}

/// Delete any auto-snapshot manifests for this volume from both the
/// local `snapshots/` directory and the bucket. Idempotent;
/// missing-file errors are not surfaced.
/// Read `vol_dir/volume.key` if present and write it to
/// `data_dir/keys/<vol_ulid>.key`. No-op (Ok) on volumes that have no
/// `volume.key` — readonly forks legitimately don't have one.
fn self_heal_key_shadow(
    data_dir: &Path,
    vol_dir: &Path,
    vol_ulid: ulid::Ulid,
) -> std::io::Result<()> {
    let key_path = vol_dir.join(elide_core::signing::VOLUME_KEY_FILE);
    let bytes = match std::fs::read(&key_path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    elide_coordinator::key_shadow::write(data_dir, vol_ulid, &bytes)
}

/// Handle the volume binary's `NotifyVolumeReady` signal: the volume
/// has successfully opened (key loaded, WAL replayed, extent index
/// reconstructed) and the local fork is provably sufficient to serve.
/// The auto-snapshot from the preceding `stop` is no longer needed as
/// a recovery basis and is cleaned up here.
///
/// Best-effort: a cleanup failure leaves the auto-snapshot in S3 (and
/// pinning the GC floor) until the next `stop` overwrites it. The
/// volume continues to serve regardless.
pub(crate) async fn notify_volume_ready_op(
    vol_ulid: ulid::Ulid,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), IpcError> {
    let vol_ulid_str = vol_ulid.to_string();
    let fork_dir = data_dir.join("by_id").join(&vol_ulid_str);
    if !fork_dir.exists() {
        return Err(IpcError::not_found(format!(
            "fork dir for {vol_ulid_str} not present locally"
        )));
    }
    if let Err(e) = cleanup_auto_snapshots(&fork_dir, &vol_ulid_str, store).await {
        warn!(
            "[inbound] notify-ready {vol_ulid_str}: cleanup_auto_snapshots failed: {e:#}; \
             auto-snapshot will be overwritten by the next stop"
        );
    }
    Ok(())
}

async fn cleanup_auto_snapshots(
    fork_dir: &Path,
    vol_ulid_str: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), IpcError> {
    use futures::TryStreamExt;

    // 1. Local cleanup.
    let snap_dir = fork_dir.join("snapshots");
    if let Ok(entries) = std::fs::read_dir(&snap_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some((_, kind)) = elide_core::signing::parse_snapshot_filename(name) else {
                continue;
            };
            if kind == elide_core::signing::SnapshotKind::Auto {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    // 2. Bucket cleanup. List the snapshots prefix and delete any
    //    object whose filename parses as an auto-snapshot.
    let prefix = object_store::path::Path::from(format!("by_id/{vol_ulid_str}/snapshots/"));
    let objects: Vec<object_store::ObjectMeta> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(|e| IpcError::store(format!("listing snapshots for cleanup: {e}")))?;

    for obj in objects {
        let Some(filename) = obj.location.filename() else {
            continue;
        };
        let Some((_, kind)) = elide_core::signing::parse_snapshot_filename(filename) else {
            continue;
        };
        if kind == elide_core::signing::SnapshotKind::Auto
            && let Err(e) = store.delete(&obj.location).await
        {
            warn!(
                "[inbound] failed to delete auto-snapshot {}: {e}",
                obj.location
            );
        }
    }
    Ok(())
}
