// Prefetch segment index files from S3.
//
// Before a forked volume can be opened on a host that has no local segments for
// its ancestors, the index sections (.idx files) must be downloaded so that
// Volume::open can rebuild the LBA map and extent index. Body bytes are not
// fetched; individual extent reads trigger demand-fetch on the first access.
//
// This is the "warm start" step for a fresh host mounting a forked volume —
// always invoked post-claim, where the source coordinator's release has
// already published a signed snapshot manifest covering current state.
//
// For each fork in the rebuild chain (writable head + each ancestor):
//   1. Fetch the snapshot artifacts (marker + manifest) at the
//      relevant snapshot ULID — peer-first, S3 fallback, no LIST on
//      the happy path. For ancestors the ULID is the branch point
//      from the child's signed provenance; for the writable head it
//      is the latest local snapshot (populated by step 1's marker
//      LIST when needed).
//   2. Verify the manifest under the appropriate pubkey (parent
//      pubkey from `volume.provenance` for ancestors; the volume's
//      own `volume.pub` for the writable head). The manifest
//      enumerates every segment belonging to that fork at the
//      snapshot — authoritative, exhaustive, and reaper-stable thanks
//      to GC's snapshot-floor rule.
//   3. For each manifest segment ULID not already in `index/`:
//        a. Range-GET [0..96] to parse the header (determines body_section_start)
//        b. Range-GET [0..body_section_start] — header + index + inline
//        c. Write atomically to <fork_dir>/index/<ulid>.idx
//
// `prefetch_indexes` therefore performs no `LIST segments/` and no
// `LIST retention/`. Operator-initiated capture of pre-snapshot bucket
// state (e.g. `force_snapshot_now`) goes through `pull_indexes_via_list`
// instead, which retains the LIST + retention-marker filter.
//
// After this runs, Volume::open will find the .idx files, rebuild_segments and
// extentindex::rebuild will both scan index/*.idx, and the volume opens
// correctly. Individual reads then demand-fetch the body bytes on first access.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use elide_peer_fetch::{PeerEndpoint, PeerFetchClient};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::{info, trace, warn};
use ulid::Ulid;

use elide_core::signing::{self, VerifyingKey};
use elide_core::volume::{resolve_ancestor_dir, walk_extent_ancestors};

use crate::pull::pull_volume_skeleton;
use crate::retention::{parse_marker_body, parse_marker_key};
use crate::upload::derive_names;

/// Per-volume peer-fetch context populated by the claim-discovery
/// hook ([`crate::peer_discovery::discover_peer_for_claim`]) and
/// consumed once by the next prefetch run.
///
/// `volume_name` is the requesting volume's name (used in
/// [`elide_peer_fetch::PeerFetchToken`]); `client` mints tokens on
/// demand and pools HTTP/2 connections; `endpoint` is the previous
/// claimer's advertised dial address.
///
/// Holding the context across multiple prefetch ticks is intentional:
/// the discovered peer's relevance lifetime is short (the previous
/// claimer is presumed to have been releasing its working set), and
/// v1 only consults it on the *first* tick after claim. Subsequent
/// ticks within the same volume task pass `None` so they fall through
/// to S3 directly.
#[derive(Clone)]
pub struct PeerFetchContext {
    pub client: PeerFetchClient,
    pub endpoint: PeerEndpoint,
    pub volume_name: String,
}

#[derive(Default)]
pub struct PrefetchResult {
    pub fetched: usize,
    /// Of `fetched`, how many came from a peer (the rest came from S3).
    /// Useful for the per-prefetch-run success-signal counters the v1
    /// plan calls out.
    pub fetched_from_peer: usize,
    pub skipped: usize,
    /// Of `skipped`, how many were dropped because a `retention/` marker
    /// records them as inputs to a GC output that is also present in the
    /// `segments/` listing — i.e. their replacement is observed and the
    /// fetch would be redundant.
    pub superseded: usize,
    pub failed: usize,
    /// Snapshot manifests downloaded during this run. Counted
    /// separately from `.idx` so the per-run log line keeps the
    /// index/snapshot signals distinct.
    pub snapshots_fetched: usize,
    /// Of `snapshots_fetched`, how many came from a peer.
    pub snapshots_from_peer: usize,
    /// Per-segment `.prefetch` hints persisted to
    /// `cache/<ulid>.prefetch-hint` for the volume daemon's body
    /// prefetch task to consume at `volume up`. Only populated when a
    /// peer context is present; segments with empty hints (peer has no
    /// body cache for that segment) are not counted.
    pub hints_fetched: usize,
}

// Segment file header constants are re-exported from elide-core/src/segment.rs
// so prefetch's manual header parsing stays in sync with the canonical format.
use elide_core::segment::{HEADER_LEN as SEGMENT_HEADER_LEN, MAGIC as SEGMENT_MAGIC};
const HEADER_LEN: usize = SEGMENT_HEADER_LEN as usize;

/// Concurrency cap for parallel object fetches inside one prefetch
/// pass — both `.idx` segment indexes and snapshot artifacts. Sized
/// for HTTP/2 multiplexing on a single store connection: low enough
/// not to trip rate limits on commodity object stores, high enough
/// that one slow GET doesn't stall a chain walk.
const PREFETCH_CONCURRENCY: usize = 8;

/// Prefetch the index section (`.idx`) for all segments in `fork_dir` and its
/// ancestors that are not present locally.
///
/// Processes the current fork first (no branch cutoff), then each ancestor fork
/// in order (with their respective branch-point cutoffs). For each fork, lists
/// S3 objects under `by_id/<volume_id>/` and downloads the header+index portion
/// of any segment not already in `index/`.
pub async fn prefetch_indexes(
    fork_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    peer: Option<&PeerFetchContext>,
) -> Result<PrefetchResult> {
    // `fork_dir` is `<data_dir>/by_id/<ulid>/`. All volumes — writable,
    // imported readonly bases, and pulled ancestors — live in `by_id/`.
    let data_dir = fork_dir
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(fork_dir);
    let by_id_dir = data_dir.join("by_id");

    // Two-pass shape:
    //   Pass 1 (this scope): walk lineage chain + extent ancestors to
    //   collect a flat list of `prefetch_fork` invocations to run.
    //   Sequential by necessity — each parent's lineage must be read
    //   to know the next parent, and pulling a missing skeleton is
    //   prerequisite for that read. Mostly local-fs-fast.
    //
    //   Pass 2 (below): run all `prefetch_fork` calls concurrently
    //   under `buffer_unordered(PREFETCH_CONCURRENCY)`. The work in
    //   each call (LIST + parallel GETs) is independent across forks,
    //   so the across-fork wall time collapses from N × per-fork to
    //   max(per-fork) (modulo concurrency cap).
    let mut tasks: Vec<PrefetchTask> = Vec::new();

    // Current fork: all its segments are valid (no branch-point cutoff).
    // We always have the current fork's own volume.pub locally — it was
    // written when the fork was created (or pulled as a skeleton).
    let current_volume_id = derive_names(fork_dir)
        .with_context(|| format!("resolving volume id for {}", fork_dir.display()))?;
    let own_vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE)
        .with_context(|| format!("loading volume.pub from {}", fork_dir.display()))?;
    tasks.push(PrefetchTask {
        dir: fork_dir.to_path_buf(),
        volume_id: current_volume_id,
        branch_ulid: None,
        vk: own_vk,
        // Head fork has no branch_ulid → no manifest is read for it
        // here. If `latest_snapshot(fork_dir)` later resolves to one
        // it was authored by *this* fork (own_vk).
        manifest_vk: own_vk,
    });

    // Walk the fork parent chain using embedded pubkeys committed in
    // each child's signed provenance — *not* the parent's on-disk
    // `volume.pub`. This matches the read-path trust model (see
    // `block_reader.rs`): the child's provenance fixes its parent's
    // key at fork time, so the parent directory doesn't need a
    // locally-trusted `volume.pub`. Pull missing skeletons here so
    // we can read each parent's lineage to discover the next.
    let mut trusted_dirs: Vec<PathBuf> = Vec::new();
    let own_lineage =
        signing::read_lineage_with_key(fork_dir, &own_vk, signing::VOLUME_PROVENANCE_FILE)
            .with_context(|| format!("reading provenance for {}", fork_dir.display()))?;
    let mut cursor = own_lineage.parent;
    while let Some(parent) = cursor {
        let parent_dir = resolve_ancestor_dir(by_id_dir.as_path(), &parent.volume_ulid);
        let parent_dir = if parent_dir.exists() {
            parent_dir
        } else {
            info!(
                "[prefetch] pulling ancestor skeleton: {}",
                parent.volume_ulid
            );
            pull_volume_skeleton(store, data_dir, &parent.volume_ulid, peer)
                .await
                .with_context(|| format!("pulling ancestor {}", parent.volume_ulid))?
        };
        let parent_vk = VerifyingKey::from_bytes(&parent.pubkey).map_err(|e| {
            anyhow::anyhow!(
                "invalid parent pubkey in provenance for {}: {e}",
                parent.volume_ulid
            )
        })?;
        // Synthesised handoff snapshots (force-released parents) are
        // signed under the recovering coordinator's attestation key,
        // not the parent's volume.pub. The child's signed provenance
        // records that override in `ParentRef::manifest_pubkey`;
        // honour it here so the manifest verify under
        // `read_snapshot_manifest` uses the right key.
        let parent_manifest_vk = match parent.manifest_pubkey {
            Some(bytes) => VerifyingKey::from_bytes(&bytes).map_err(|e| {
                anyhow::anyhow!(
                    "invalid parent manifest_pubkey in provenance for {}: {e}",
                    parent.volume_ulid
                )
            })?,
            None => parent_vk,
        };
        tasks.push(PrefetchTask {
            dir: parent_dir.clone(),
            volume_id: parent.volume_ulid.clone(),
            branch_ulid: Some(parent.snapshot_ulid.clone()),
            vk: parent_vk,
            manifest_vk: parent_manifest_vk,
        });
        trusted_dirs.push(parent_dir.clone());
        // Continue walking under the pubkey the child committed to.
        let parent_lineage = signing::read_lineage_with_key(
            &parent_dir,
            &parent_vk,
            signing::VOLUME_PROVENANCE_FILE,
        )
        .with_context(|| format!("reading provenance for ancestor {}", parent.volume_ulid))?;
        cursor = parent_lineage.parent;
    }

    // Extent-index ancestors have no embedded pubkey — the child just
    // names them by `<volume_ulid>/<snapshot_ulid>` with no trust
    // anchor. For these we pull the skeleton if missing and fall back
    // to loading `volume.pub` from disk (the just-pulled one, which is
    // what the store itself published). Dedup against the fork chain
    // we already walked.
    let extent_ancestors = walk_extent_ancestors(fork_dir, by_id_dir.as_path())
        .context("walking extent-index ancestor chain")?;
    for ancestor in extent_ancestors {
        if trusted_dirs.contains(&ancestor.dir) {
            continue;
        }
        let ancestor_dir = if ancestor.dir.exists() {
            ancestor.dir.clone()
        } else {
            let ulid_str = ancestor
                .dir
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "extent ancestor dir has no ULID component: {}",
                        ancestor.dir.display()
                    )
                })?;
            info!("[prefetch] pulling extent-source skeleton: {ulid_str}");
            pull_volume_skeleton(store, data_dir, ulid_str, peer)
                .await
                .with_context(|| format!("pulling extent-source {ulid_str}"))?
        };
        let volume_id = derive_names(&ancestor_dir)
            .with_context(|| format!("resolving volume id for {}", ancestor_dir.display()))?;
        let ancestor_vk = signing::load_verifying_key(&ancestor_dir, signing::VOLUME_PUB_FILE)
            .with_context(|| format!("loading volume.pub from {}", ancestor_dir.display()))?;
        // Extent-index ancestors don't reach the synthesised-handoff
        // path (their branch ULID is recorded in extent index, not in
        // the requesting child's provenance, so no
        // `manifest_pubkey` override applies). Manifest verify and
        // segment verify both use the ancestor's own volume.pub.
        tasks.push(PrefetchTask {
            dir: ancestor_dir,
            volume_id,
            branch_ulid: ancestor.branch_ulid,
            vk: ancestor_vk,
            manifest_vk: ancestor_vk,
        });
    }

    // Pass 2: dispatch all `prefetch_fork` calls concurrently. Each
    // task gets its own `PrefetchResult`; merge after.
    let peer_owned: Option<PeerFetchContext> = peer.cloned();
    let outcomes: Vec<PrefetchResult> = futures::stream::iter(tasks.into_iter().map(|task| {
        let store = store.clone();
        let peer = peer_owned.clone();
        async move {
            let mut local = PrefetchResult::default();
            prefetch_fork(
                &store,
                &task.dir,
                &task.volume_id,
                task.branch_ulid.as_deref(),
                &task.vk,
                &task.manifest_vk,
                peer.as_ref(),
                &mut local,
            )
            .await?;
            anyhow::Ok(local)
        }
    }))
    .buffer_unordered(PREFETCH_CONCURRENCY)
    .try_collect()
    .await?;

    let mut result = PrefetchResult::default();
    for r in outcomes {
        result.fetched += r.fetched;
        result.fetched_from_peer += r.fetched_from_peer;
        result.skipped += r.skipped;
        result.superseded += r.superseded;
        result.failed += r.failed;
        result.snapshots_fetched += r.snapshots_fetched;
        result.snapshots_from_peer += r.snapshots_from_peer;
        result.hints_fetched += r.hints_fetched;
    }
    Ok(result)
}

/// One unit of prefetch work collected during the lineage walk and
/// dispatched concurrently in Pass 2.
///
/// `vk` verifies per-segment signatures on `.idx` bytes — those are
/// always signed by the volume's own `volume.key` regardless of who
/// authored the snapshot manifest.
///
/// `manifest_vk` verifies the snapshot manifest at `branch_ulid`. For
/// normal handoffs this equals `vk`. For force-released volumes the
/// manifest is *synthesised* by the recovering coordinator and signed
/// under its attestation key — the requesting child records that
/// override key in its own provenance via `ParentRef::manifest_pubkey`,
/// and we propagate it here so the manifest verify uses the right key.
struct PrefetchTask {
    dir: PathBuf,
    volume_id: String,
    branch_ulid: Option<String>,
    vk: VerifyingKey,
    manifest_vk: VerifyingKey,
}

#[allow(clippy::too_many_arguments)]
async fn prefetch_fork(
    store: &Arc<dyn ObjectStore>,
    fork_dir: &Path,
    volume_id: &str,
    branch_ulid: Option<&str>,
    verifying_key: &VerifyingKey,
    manifest_verifying_key: &VerifyingKey,
    peer: Option<&PeerFetchContext>,
    result: &mut PrefetchResult,
) -> Result<()> {
    // Snapshot artifacts (markers + manifests) come first. For ancestors
    // we already know the branch ULID and only need the manifest at that
    // point; for the writable head we need to discover the latest
    // snapshot in S3 (LIST snapshots/) so we can identify *its*
    // manifest. Either way the manifest must land on disk before we can
    // compute the .idx fetch set.
    prefetch_snapshots(store, fork_dir, volume_id, branch_ulid, peer, result).await?;

    // Resolve the manifest anchor for this fork:
    //   - Ancestor fork: the branch point recorded in the requesting
    //     child's signed provenance.
    //   - Writable head: the latest local snapshot, populated by the
    //     prefetch_snapshots call above. None means this is a fresh
    //     fork (or one that has never published a snapshot) — there's
    //     nothing for warm-start to fetch. Operator-initiated capture
    //     of pre-snapshot bucket state belongs in
    //     [`pull_indexes_via_list`], not here.
    let anchor: Option<String> = match branch_ulid {
        Some(b) => Some(b.to_owned()),
        None => elide_core::volume::latest_snapshot(fork_dir)
            .with_context(|| format!("reading latest local snapshot for {}", fork_dir.display()))?
            .map(|u| u.to_string()),
    };

    let to_fetch: Vec<(StorePath, String, Ulid)> = match anchor.as_deref() {
        Some(snap) => {
            manifest_driven_fetch_set(fork_dir, volume_id, snap, manifest_verifying_key, result)?
        }
        None => Vec::new(),
    };

    let vol_ulid = Ulid::from_string(volume_id).ok();
    fetch_idx_set(
        store,
        fork_dir,
        verifying_key,
        peer,
        vol_ulid,
        to_fetch,
        result,
    )
    .await;
    Ok(())
}

/// Pull every `.idx` currently visible under
/// `by_id/<volume_id>/segments/` into `<fork_dir>/index/`, filtering
/// out GC-superseded inputs via retention/ markers. Used by operator
/// IPCs that need to capture raw bucket state — notably
/// `force_snapshot_now`, which synthesises a handoff manifest from
/// whatever is currently in S3 *including* segments past the latest
/// snapshot. The warm-start claim path uses [`prefetch_indexes`]
/// instead, which anchors on a signed manifest and never lists
/// segments/ or retention/.
///
/// `verifying_key` is the volume's `volume.pub`; per-segment
/// signatures are checked at fetch time, same as the manifest path.
pub async fn pull_indexes_via_list(
    store: &Arc<dyn ObjectStore>,
    fork_dir: &Path,
    volume_id: &str,
    verifying_key: &VerifyingKey,
    peer: Option<&PeerFetchContext>,
) -> Result<PrefetchResult> {
    let mut result = PrefetchResult::default();

    // Phase 1: load retention/ markers BEFORE listing segments/. The
    // ordering is load-bearing for correctness under concurrent GC.
    // GC publishes in this order: PUT segments/<output>, then PUT
    // retention/<output> marker listing inputs. Reaper deletes inputs
    // first, then the marker. With strong-read S3, listing retention
    // first guarantees: any marker we observe was written before our
    // subsequent segments/ LIST, so the marker's GC output (PUT
    // before the marker) is also visible to us. We can then safely
    // skip an input segment iff its replacement output is observed in
    // the segments listing.
    //
    // Reversing the order would be unsafe: a GC committing between
    // segments LIST and retention LIST could mark an input as
    // superseded by an output we never enumerated, leading us to skip
    // the input *and* miss its replacement.
    let supersessions = list_supersessions(store, volume_id).await;

    // Phase 2: list the segments/ sub-prefix.
    let seg_prefix = StorePath::from(format!("by_id/{volume_id}/segments/"));
    let objects: Vec<_> = store
        .list(Some(&seg_prefix))
        .try_collect()
        .await
        .with_context(|| format!("listing by_id/{volume_id}/segments/"))?;

    // Set of segment ULIDs observed in *this* segments LIST. Used
    // below to validate supersessions: a marker tells us "X is dead
    // because Y replaces it"; we only skip X when Y is also in our
    // listing. This belt-and-braces check catches the (impossible per
    // GC ordering, but cheap to assert) case of a marker without its
    // output and surfaces it as a wasted fetch instead of a silent
    // gap.
    let observed_outputs: HashSet<Ulid> = objects
        .iter()
        .filter_map(|obj| obj.location.filename())
        .filter_map(|f| Ulid::from_string(f).ok())
        .collect();

    let to_fetch: Vec<(StorePath, String, Ulid)> = objects
        .into_iter()
        .filter_map(|obj| {
            let ulid_str = obj.location.filename()?.to_owned();
            let ulid = Ulid::from_string(&ulid_str).ok()?;
            let local_idx = fork_dir.join("index").join(format!("{ulid_str}.idx"));
            if local_idx.exists() {
                result.skipped += 1;
                return None;
            }
            if let Some(output) = supersessions.get(&ulid)
                && observed_outputs.contains(output)
            {
                trace!(
                    "[prefetch] skipping {ulid_str}: superseded by GC output {output} \
                     (retention marker present, output observed in segments/)"
                );
                result.skipped += 1;
                result.superseded += 1;
                return None;
            }
            Some((obj.location, ulid_str, ulid))
        })
        .collect();

    let vol_ulid = Ulid::from_string(volume_id).ok();
    fetch_idx_set(
        store,
        fork_dir,
        verifying_key,
        peer,
        vol_ulid,
        to_fetch,
        &mut result,
    )
    .await;
    Ok(result)
}

/// Drive the parallel `.idx` fetch for a pre-computed set of
/// segments. Each entry is `(s3_key, ulid_str, ulid)`. Tries the peer
/// first when a context is available, falls through to S3 on any
/// failure (including signature mismatch — see
/// [`peer_fetch_idx`]). Aggregates outcomes into `result`.
///
/// When a peer context is present *and* the peer served the `.idx`
/// successfully, the same task also fetches the `.prefetch` hint and
/// persists it as `cache/<ulid>.prefetch-hint`. The volume daemon
/// consumes those files on `volume up` to drive background body
/// warming. Hint fetch is best-effort: any failure (404, network,
/// empty payload) is non-fatal and just means no warming advice for
/// that segment.
///
/// Hints are paired with peer-served `.idx` deliberately: a peer that
/// 404'd `.idx` has no body cache for the segment either, so a
/// follow-up hint GET would also 404. Skipping the round-trip in that
/// case keeps the fan-out tight without losing any signal.
async fn fetch_idx_set(
    store: &Arc<dyn ObjectStore>,
    fork_dir: &Path,
    verifying_key: &VerifyingKey,
    peer: Option<&PeerFetchContext>,
    vol_ulid: Option<Ulid>,
    to_fetch: Vec<(StorePath, String, Ulid)>,
    result: &mut PrefetchResult,
) {
    enum IdxOutcome {
        FromPeer,
        FromStore,
        Failed,
    }

    struct SegOutcome {
        idx: IdxOutcome,
        hint_persisted: bool,
    }

    let outcomes: Vec<SegOutcome> =
        futures::stream::iter(to_fetch.into_iter().map(|(location, ulid_str, ulid)| {
            let store = store.clone();
            let fork_dir = fork_dir.to_path_buf();
            let verifying_key = *verifying_key;
            async move {
                let mut hint_persisted = false;
                if let (Some(peer_ctx), Some(vol_ulid)) = (peer, vol_ulid) {
                    match peer_fetch_idx(
                        peer_ctx,
                        vol_ulid,
                        ulid,
                        &fork_dir,
                        &ulid_str,
                        &verifying_key,
                    )
                    .await
                    {
                        Ok(()) => {
                            info!("[prefetch] fetched index from peer: {ulid_str}");
                            // Pair the hint fetch with peer-served idx.
                            // Best-effort: a hint failure does not
                            // demote the idx outcome.
                            match peer_fetch_hint_persist(peer_ctx, vol_ulid, ulid, &fork_dir).await
                            {
                                Ok(persisted) => {
                                    hint_persisted = persisted;
                                    if persisted {
                                        trace!("[prefetch] persisted prefetch-hint: {ulid_str}");
                                    }
                                }
                                Err(e) => {
                                    trace!(
                                        "[prefetch] peer hint miss for {ulid_str}: {e:#}; \
                                         body prefetch will demand-fetch"
                                    );
                                }
                            }
                            return SegOutcome {
                                idx: IdxOutcome::FromPeer,
                                hint_persisted,
                            };
                        }
                        Err(e) => {
                            trace!(
                                "[prefetch] peer miss for {ulid_str}: {e:#}; falling through to S3"
                            );
                        }
                    }
                }

                let idx = match fetch_idx(&store, &location, &fork_dir, &ulid_str, &verifying_key)
                    .await
                {
                    Ok(()) => {
                        info!("[prefetch] fetched index: {ulid_str}");
                        IdxOutcome::FromStore
                    }
                    Err(e) => {
                        warn!("[prefetch] failed to fetch index {ulid_str}: {e:#}");
                        IdxOutcome::Failed
                    }
                };
                SegOutcome {
                    idx,
                    hint_persisted,
                }
            }
        }))
        .buffer_unordered(PREFETCH_CONCURRENCY)
        .collect()
        .await;

    for outcome in outcomes {
        match outcome.idx {
            IdxOutcome::FromPeer => {
                result.fetched += 1;
                result.fetched_from_peer += 1;
            }
            IdxOutcome::FromStore => result.fetched += 1,
            IdxOutcome::Failed => result.failed += 1,
        }
        if outcome.hint_persisted {
            result.hints_fetched += 1;
        }
    }
}

/// Build a map `input_ulid -> output_ulid` from all retention markers
/// under `by_id/<volume_id>/retention/`. Each marker name is the GC
/// output ULID; the body lists the input ULIDs that output replaces.
///
/// Best-effort: any LIST or GET error returns whatever has been
/// collected so far (often empty), and prefetch falls back to the
/// pre-retention behaviour of fetching every input. We never fail
/// prefetch on a retention LIST error — the worst case is wasted
/// peer/S3 round-trips, not data loss.
///
/// A marker that races deletion (404 on body GET) is dropped
/// silently: the absence of supersession info just means we may
/// re-fetch the input segment from S3, which by definition the
/// reaper has either already removed (we then 404 on the segment
/// fetch) or is about to. Either is correct.
async fn list_supersessions(store: &Arc<dyn ObjectStore>, volume_id: &str) -> HashMap<Ulid, Ulid> {
    let mut map = HashMap::new();
    let prefix = StorePath::from(format!("by_id/{volume_id}/retention/"));
    let listing: Vec<_> = match store.list(Some(&prefix)).try_collect().await {
        Ok(v) => v,
        Err(e) => {
            warn!("[prefetch] listing retention/ for {volume_id}: {e:#}");
            return map;
        }
    };
    for object in listing {
        let key = object.location.as_ref();
        let output_ulid = match parse_marker_key(key) {
            Ok((_vol, out)) => out,
            Err(_) => continue,
        };
        let payload = match store.get(&object.location).await {
            Ok(p) => p,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => {
                warn!("[prefetch] fetching marker {key}: {e:#}");
                continue;
            }
        };
        let bytes = match payload.bytes().await {
            Ok(b) => b,
            Err(e) => {
                warn!("[prefetch] reading marker body {key}: {e:#}");
                continue;
            }
        };
        let text = match std::str::from_utf8(&bytes) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let inputs = match parse_marker_body(text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        for input in inputs {
            map.insert(input, output_ulid);
        }
    }
    map
}

/// Build the `.idx` fetch set for an ancestor fork from its signed
/// snapshot manifest at `branch_ulid`.
///
/// `prefetch_snapshots` is expected to have already landed
/// `<fork_dir>/snapshots/<branch_ulid>.manifest` on disk; this helper
/// reads + verifies it under `manifest_verifying_key` and drops any
/// segments already present in `index/`. Each remaining ULID is
/// paired with its computed S3 key via [`crate::upload::segment_key`]
/// — no bucket LIST is involved.
///
/// `manifest_verifying_key` may differ from the ancestor's own
/// `volume.pub`: synthesised handoff manifests (force-released
/// parents) are signed under the recovering coordinator's
/// attestation key, recorded in the requesting child's provenance
/// via `ParentRef::manifest_pubkey`.
fn manifest_driven_fetch_set(
    fork_dir: &Path,
    volume_id: &str,
    branch_ulid: &str,
    manifest_verifying_key: &VerifyingKey,
    result: &mut PrefetchResult,
) -> Result<Vec<(StorePath, String, Ulid)>> {
    let snap_ulid = Ulid::from_string(branch_ulid)
        .map_err(|e| anyhow::anyhow!("invalid branch ULID '{branch_ulid}': {e}"))?;
    let manifest =
        elide_core::signing::read_snapshot_manifest(fork_dir, manifest_verifying_key, &snap_ulid)
            .with_context(|| {
            format!(
                "reading snapshot manifest {branch_ulid} for {} (verifying under manifest pubkey)",
                fork_dir.display()
            )
        })?;

    let mut out: Vec<(StorePath, String, Ulid)> = Vec::with_capacity(manifest.segment_ulids.len());
    for ulid in manifest.segment_ulids {
        let ulid_str = ulid.to_string();
        let local_idx = fork_dir.join("index").join(format!("{ulid_str}.idx"));
        if local_idx.exists() {
            result.skipped += 1;
            continue;
        }
        let key = crate::upload::segment_key(volume_id, ulid);
        out.push((key, ulid_str, ulid));
    }
    Ok(out)
}

/// Try to fetch a segment's `.prefetch` hint from the peer and persist
/// it to `<fork_dir>/cache/<ulid>.prefetch-hint` for the volume
/// daemon's body prefetch task to consume at `volume up`.
///
/// Returns `Ok(true)` if a non-empty hint was persisted, `Ok(false)`
/// if the peer returned 404 / network error / empty payload (any of
/// which means "no warming advice for this segment"). All failure
/// modes are non-fatal: hints are advisory and prefetch falls through
/// to demand-fetch when absent.
///
/// Atomic write via `tmp + rename` so a daemon crash mid-write
/// can't leave a half-written hint that would confuse the volume
/// daemon's parser.
async fn peer_fetch_hint_persist(
    peer: &PeerFetchContext,
    vol_ulid: Ulid,
    seg_ulid: Ulid,
    fork_dir: &Path,
) -> Result<bool> {
    let hint = match peer
        .client
        .fetch_prefetch_hint(&peer.endpoint, &peer.volume_name, vol_ulid, seg_ulid)
        .await
    {
        Some(h) => h,
        None => return Ok(false),
    };
    if hint.payload_len() == 0 {
        return Ok(false);
    }

    let cache_dir = fork_dir.join("cache");
    std::fs::create_dir_all(&cache_dir).context("creating cache dir for hint")?;
    let final_path = cache_dir.join(format!("{seg_ulid}.prefetch-hint"));
    let tmp_path = cache_dir.join(format!("{seg_ulid}.prefetch-hint.tmp"));
    std::fs::write(&tmp_path, hint.wire_bytes()).context("writing prefetch-hint tmp")?;
    std::fs::rename(&tmp_path, &final_path).context("renaming prefetch-hint tmp")?;
    Ok(true)
}

/// Try to fetch a segment's `.idx` from the peer, verify its signature,
/// and write it to `<fork_dir>/index/<ulid>.idx`.
///
/// Returns `Ok(())` on success. Any non-200 from the peer (404, auth
/// failure, network error, timeout) bubbles up as `Err`, signalling
/// "fall through to S3" — the caller never propagates this error
/// further.
async fn peer_fetch_idx(
    peer: &PeerFetchContext,
    vol_ulid: Ulid,
    seg_ulid: Ulid,
    fork_dir: &Path,
    ulid_str: &str,
    verifying_key: &VerifyingKey,
) -> Result<()> {
    let bytes = peer
        .client
        .fetch_idx(&peer.endpoint, &peer.volume_name, vol_ulid, seg_ulid)
        .await
        .ok_or_else(|| anyhow::anyhow!("peer returned no bytes"))?;

    // Verify before writing — a tampering peer is caught here and
    // the caller falls through to S3 with no on-disk side effect.
    elide_core::segment::verify_segment_bytes(&bytes, ulid_str, verifying_key)
        .with_context(|| format!("verifying peer-served signature for {ulid_str}"))?;

    let index_dir = fork_dir.join("index");
    std::fs::create_dir_all(&index_dir).context("creating index dir")?;
    let tmp_path = index_dir.join(format!("{ulid_str}.idx.tmp"));
    let final_path = index_dir.join(format!("{ulid_str}.idx"));
    std::fs::write(&tmp_path, &bytes).context("writing peer idx.tmp")?;
    std::fs::rename(&tmp_path, &final_path).context("renaming peer idx.tmp")?;
    Ok(())
}

/// Download the index portion of one segment, verify its signature, and write
/// it as `<fork_dir>/index/<ulid>.idx`.
async fn fetch_idx(
    store: &Arc<dyn ObjectStore>,
    key: &StorePath,
    fork_dir: &Path,
    ulid_str: &str,
    verifying_key: &VerifyingKey,
) -> Result<()> {
    // Fetch the header first to determine how large the index section is.
    let header = store
        .get_range(key, 0..HEADER_LEN)
        .await
        .with_context(|| format!("fetching header for {ulid_str}"))?;

    if header.len() < HEADER_LEN {
        anyhow::bail!("header too short ({} bytes)", header.len());
    }
    if &header[0..8] != SEGMENT_MAGIC {
        anyhow::bail!("bad segment magic");
    }

    let index_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    let inline_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    let body_section_start = HEADER_LEN + index_length as usize + inline_length as usize;

    // Fetch [0, body_section_start) — header + index + inline.
    let idx_bytes = store
        .get_range(key, 0..body_section_start)
        .await
        .with_context(|| format!("fetching index section for {ulid_str}"))?;

    // Verify signature before writing anything to disk.
    elide_core::segment::verify_segment_bytes(&idx_bytes, ulid_str, verifying_key)
        .with_context(|| format!("verifying signature for {ulid_str}"))?;

    // Write atomically: tmp → rename.
    let index_dir = fork_dir.join("index");
    std::fs::create_dir_all(&index_dir).context("creating index dir")?;

    let tmp_path = index_dir.join(format!("{ulid_str}.idx.tmp"));
    let final_path = index_dir.join(format!("{ulid_str}.idx"));

    std::fs::write(&tmp_path, &idx_bytes).context("writing idx.tmp")?;
    std::fs::rename(&tmp_path, &final_path).context("renaming idx.tmp")?;

    Ok(())
}

/// Download snapshot manifests from S3 (and/or the configured peer)
/// into `<fork_dir>/snapshots/`. Post-#215 the manifest IS the
/// snapshot record; bare-ULID markers are not fetched.
///
/// Two paths:
///
/// 1. **Branch-point fast path** — `branch_ulid = Some(b)` and a peer
///    context is available. The manifest filename is known up-front
///    (`<b>.manifest`); we GET it from the peer first and on miss fall
///    through to a single S3 GET keyed via
///    [`crate::upload::snapshot_manifest_key`]. No LIST on either tier.
///
/// 2. **Listed path** — `branch_ulid = None` (current writable fork)
///    or no peer context. Lists `by_id/<volume_id>/snapshots/`,
///    filters to `*.manifest` (skipping legacy bare markers + filemaps)
///    and the branch (if set) and skip-if-local, and fetches the
///    remainder via S3 GETs bounded by `buffer_unordered`.
///
/// Both paths write atomically (tmp → rename).
async fn prefetch_snapshots(
    store: &Arc<dyn ObjectStore>,
    fork_dir: &Path,
    volume_id: &str,
    branch_ulid: Option<&str>,
    peer: Option<&PeerFetchContext>,
    result: &mut PrefetchResult,
) -> Result<()> {
    let snap_dir = fork_dir.join("snapshots");

    // Fast path: known branch + peer available. The three artifact
    // names are deterministic, so we can fetch by-name without ever
    // listing. S3 LIST stays off the critical path entirely when the
    // peer answers; on a peer miss we fall back to a single S3 GET
    // per missed artifact (also no LIST).
    if let (Some(branch), Some(peer_ctx)) = (branch_ulid, peer)
        && let (Ok(vol_ulid), Ok(snap_ulid)) =
            (Ulid::from_string(volume_id), Ulid::from_string(branch))
    {
        return prefetch_branch_snapshot_artifacts(
            store, &snap_dir, vol_ulid, snap_ulid, peer_ctx, result,
        )
        .await;
    }

    // Listed path: no branch, or no peer. Either way LIST is the only
    // way to discover what to fetch.
    let prefix = StorePath::from(format!("by_id/{volume_id}/snapshots/"));
    let objects: Vec<_> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .with_context(|| format!("listing by_id/{volume_id}/snapshots/"))?;

    // Filter + skip-if-local synchronously, then fetch the remaining
    // artifacts concurrently. Bounded with `buffer_unordered` so a
    // very wide snapshot dir doesn't blast the store with hundreds
    // of in-flight GETs.
    // Snapshots are recorded as `<ulid>.manifest`; everything else
    // under `snapshots/` (pre-#212 `.filemap` siblings, pre-#215
    // bare-ULID markers) is skipped.
    let to_fetch: Vec<(object_store::path::Path, String)> = objects
        .into_iter()
        .filter_map(|obj| {
            let filename = obj.location.filename()?.to_owned();
            let stem = filename.strip_suffix(".manifest")?;
            if let Some(branch) = branch_ulid
                && stem != branch
            {
                return None;
            }
            if snap_dir.join(&filename).exists() {
                return None;
            }
            Some((obj.location, filename))
        })
        .collect();

    if to_fetch.is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(&snap_dir).context("creating snapshots dir")?;

    let snap_dir_owned = snap_dir.clone();
    let n = to_fetch.len();
    futures::stream::iter(to_fetch.into_iter().map(|(location, filename)| {
        let store = store.clone();
        let snap_dir = snap_dir_owned.clone();
        async move {
            let data = store
                .get(&location)
                .await
                .with_context(|| format!("downloading {location}"))?
                .bytes()
                .await
                .with_context(|| format!("reading {location}"))?;

            write_snapshot_artifact_atomic(&snap_dir, &filename, &data)?;
            info!("[prefetch] fetched snapshot artifact: {filename}");
            anyhow::Ok(())
        }
    }))
    .buffer_unordered(PREFETCH_CONCURRENCY)
    .try_collect::<Vec<()>>()
    .await?;

    result.snapshots_fetched += n;

    Ok(())
}

/// Write `<snap_dir>/<filename>` atomically (tmp → rename).
fn write_snapshot_artifact_atomic(snap_dir: &Path, filename: &str, data: &[u8]) -> Result<()> {
    let tmp_path = snap_dir.join(format!("{filename}.tmp"));
    let local_path = snap_dir.join(filename);
    std::fs::write(&tmp_path, data).with_context(|| format!("writing {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &local_path)
        .with_context(|| format!("renaming to {}", local_path.display()))?;
    Ok(())
}

/// Branch-point fast path. Fetch the snapshot manifest peer-first,
/// S3 by-name on miss, no LIST on either tier.
async fn prefetch_branch_snapshot_artifacts(
    store: &Arc<dyn ObjectStore>,
    snap_dir: &Path,
    vol_ulid: Ulid,
    snap_ulid: Ulid,
    peer: &PeerFetchContext,
    result: &mut PrefetchResult,
) -> Result<()> {
    let volume_id = vol_ulid.to_string();
    let snap_ulid_str = snap_ulid.to_string();
    let manifest_name = format!("{snap_ulid_str}.manifest");

    if snap_dir.join(&manifest_name).exists() {
        return Ok(());
    }
    std::fs::create_dir_all(snap_dir).context("creating snapshots dir")?;

    if let Some(bytes) = peer
        .client
        .fetch_snapshot_manifest(&peer.endpoint, &peer.volume_name, vol_ulid, snap_ulid)
        .await
    {
        write_snapshot_artifact_atomic(snap_dir, &manifest_name, &bytes)?;
        info!("[prefetch] fetched snapshot artifact from peer: {manifest_name}");
        result.snapshots_fetched += 1;
        result.snapshots_from_peer += 1;
        return Ok(());
    }

    trace!("[prefetch] peer miss for snapshot artifact {manifest_name}; falling through to S3");
    let key = crate::upload::snapshot_manifest_key(&volume_id, snap_ulid);
    let data = store
        .get(&key)
        .await
        .with_context(|| format!("downloading {key}"))?
        .bytes()
        .await
        .with_context(|| format!("reading {key}"))?;
    write_snapshot_artifact_atomic(snap_dir, &manifest_name, &data)?;
    info!("[prefetch] fetched snapshot artifact: {manifest_name}");
    result.snapshots_fetched += 1;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as StorePath;
    use std::sync::Arc;
    use tempfile::TempDir;

    use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};
    use elide_core::signing::{
        ProvenanceLineage, VOLUME_KEY_FILE, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE,
        build_snapshot_manifest_bytes, generate_keypair, load_signer, write_provenance,
    };

    /// Build a parent+child fork pair using the flat by_id/<ulid> layout.
    /// Upload the parent segment to the store at the correct by_id/ prefix.
    /// Verify prefetch_indexes downloads the .idx file for the parent.
    #[tokio::test]
    async fn prefetch_indexes_downloads_ancestor_idx() {
        // Flat layout under a by_id/ dir (mirrors real data_dir/by_id/).
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");

        let parent_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        let parent_dir = by_id.join(parent_ulid);
        let child_dir = by_id.join(child_ulid);

        std::fs::create_dir_all(parent_dir.join("snapshots")).unwrap();
        std::fs::create_dir_all(child_dir.join("pending")).unwrap();

        // Generate a keypair for each of parent and child, writing their
        // volume.key + volume.pub to disk. The child's signing key is used
        // below to sign provenance with a real `parent` lineage entry.
        let parent_key = generate_keypair(&parent_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        let child_key = generate_keypair(&child_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();

        // Parent's own provenance (no lineage) — prefetch will
        // read+verify it when walking from the child.
        write_provenance(
            &parent_dir,
            &parent_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        // Build the parent segment, signed by the parent's key.
        let data = vec![0xABu8; 4096];
        let hash = blake3::hash(&data);
        let seg_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let parent_signer = load_signer(&parent_dir, VOLUME_KEY_FILE).unwrap();
        let staging = tmp.path().join(seg_ulid);
        write_segment(&staging, &mut entries, parent_signer.as_ref()).unwrap();

        // Create a snapshot marker in parent (branch point for child).
        let snap_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        std::fs::write(parent_dir.join("snapshots").join(snap_ulid), "").unwrap();

        // Child's signed provenance carries the parent reference in its
        // `parent` field. Prefetch reads this via `walk_ancestors`.
        write_provenance(
            &child_dir,
            &child_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage {
                parent: Some(elide_core::signing::ParentRef {
                    volume_ulid: parent_ulid.to_owned(),
                    snapshot_ulid: snap_ulid.to_owned(),
                    pubkey: parent_key.verifying_key().to_bytes(),
                    manifest_pubkey: None,
                }),
                extent_index: Vec::new(),
                oci_source: None,
            },
        )
        .unwrap();

        // Set up a local object store and upload the parent segment at the
        // canonical S3 key (date subdir derived from the segment ULID).
        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        let seg_bytes = std::fs::read(&staging).unwrap();
        let key = crate::upload::segment_key(parent_ulid, seg_ulid.parse().unwrap());
        store.put(&key, seg_bytes.into()).await.unwrap();

        // Upload the parent's signed snapshot manifest at the branch
        // point. Prefetch now reads this manifest as the authoritative
        // .idx fetch set for the ancestor.
        let manifest_bytes = build_snapshot_manifest_bytes(
            parent_signer.as_ref(),
            &[Ulid::from_string(seg_ulid).unwrap()],
            None,
        );
        let manifest_key =
            crate::upload::snapshot_manifest_key(parent_ulid, snap_ulid.parse().unwrap());
        store
            .put(&manifest_key, manifest_bytes.into())
            .await
            .unwrap();

        // Run prefetch_indexes on the child fork.
        let result = prefetch_indexes(&child_dir, &store, None).await.unwrap();
        assert_eq!(result.fetched, 1, "should fetch one .idx");
        assert_eq!(result.failed, 0);

        // Verify the .idx file was written into parent's index/ dir.
        let idx_path = parent_dir.join("index").join(format!("{seg_ulid}.idx"));
        assert!(idx_path.exists(), ".idx file should exist");

        // Verify it is parseable as a segment index.
        let (_, entries, _) = elide_core::segment::read_segment_index(&idx_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].hash, hash);

        // Running again should skip (already present).
        let result2 = prefetch_indexes(&child_dir, &store, None).await.unwrap();
        assert_eq!(result2.fetched, 0);
        assert_eq!(result2.skipped, 1);
    }

    /// Pulled ancestor: the child is writable and its parent is a pulled
    /// ancestor (no `volume.name`, no `by_name/` symlink). Both live in
    /// `by_id/`. Prefetch must write the parent's `.idx` into the parent's
    /// own `index/` dir, verifying it against the parent's own `volume.pub`.
    #[tokio::test]
    async fn prefetch_indexes_writes_pulled_ancestor_idx() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path();
        let by_id = data_dir.join("by_id");

        let parent_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        let parent_dir = by_id.join(parent_ulid);
        let child_dir = by_id.join(child_ulid);

        std::fs::create_dir_all(parent_dir.join("snapshots")).unwrap();
        std::fs::create_dir_all(child_dir.join("pending")).unwrap();

        let parent_key = generate_keypair(&parent_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        let child_key = generate_keypair(&child_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();

        write_provenance(
            &parent_dir,
            &parent_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        let data = vec![0x5Au8; 4096];
        let hash = blake3::hash(&data);
        let seg_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let parent_signer = load_signer(&parent_dir, VOLUME_KEY_FILE).unwrap();
        let staging = tmp.path().join(seg_ulid);
        write_segment(&staging, &mut entries, parent_signer.as_ref()).unwrap();

        let snap_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        std::fs::write(parent_dir.join("snapshots").join(snap_ulid), "").unwrap();

        write_provenance(
            &child_dir,
            &child_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage {
                parent: Some(elide_core::signing::ParentRef {
                    volume_ulid: parent_ulid.to_owned(),
                    snapshot_ulid: snap_ulid.to_owned(),
                    pubkey: parent_key.verifying_key().to_bytes(),
                    manifest_pubkey: None,
                }),
                extent_index: Vec::new(),
                oci_source: None,
            },
        )
        .unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        let seg_bytes = std::fs::read(&staging).unwrap();
        let key = crate::upload::segment_key(parent_ulid, seg_ulid.parse().unwrap());
        store.put(&key, seg_bytes.into()).await.unwrap();

        // Parent's signed manifest at the branch point — required by
        // the manifest-driven ancestor fetch path.
        let manifest_bytes = build_snapshot_manifest_bytes(
            parent_signer.as_ref(),
            &[Ulid::from_string(seg_ulid).unwrap()],
            None,
        );
        let manifest_key =
            crate::upload::snapshot_manifest_key(parent_ulid, snap_ulid.parse().unwrap());
        store
            .put(&manifest_key, manifest_bytes.into())
            .await
            .unwrap();

        let result = prefetch_indexes(&child_dir, &store, None).await.unwrap();
        assert_eq!(result.fetched, 1);
        assert_eq!(result.failed, 0);

        let idx_path = parent_dir.join("index").join(format!("{seg_ulid}.idx"));
        assert!(idx_path.exists(), ".idx must land in by_id/<parent>/index/");
        // And definitely not in the child's index dir.
        assert!(
            !child_dir
                .join("index")
                .join(format!("{seg_ulid}.idx"))
                .exists(),
            "child must not inherit parent's .idx"
        );
    }

    /// Synthesised handoff manifest: a force-released parent's
    /// snapshot is signed under the recovering coordinator's
    /// attestation key, **not** the parent's `volume.pub`. The
    /// requesting child's provenance records the override via
    /// `ParentRef::manifest_pubkey`. Prefetch must verify the parent's
    /// manifest under that override pubkey, while still verifying the
    /// segment `.idx` bytes under the parent's own volume key (those
    /// were signed when the parent was alive).
    ///
    /// Regression: prior to this fix `prefetch` used a single
    /// `verifying_key` for both manifest verify and segment verify,
    /// so any claim from a force-release immediately failed
    /// startup with `manifest signature invalid` and the volume
    /// supervisor entered a respawn loop. Reproduced live by
    /// `volume release --force` followed by `volume claim` + `start`.
    #[tokio::test]
    async fn prefetch_indexes_verifies_synthesised_handoff_manifest_under_override_key() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path();
        let by_id = data_dir.join("by_id");

        let parent_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        let parent_dir = by_id.join(parent_ulid);
        let child_dir = by_id.join(child_ulid);

        std::fs::create_dir_all(parent_dir.join("snapshots")).unwrap();
        std::fs::create_dir_all(child_dir.join("pending")).unwrap();

        // Parent's own volume key signs its segments. Child has its
        // own key. The "recovering coordinator" key is what signed
        // the synthesised handoff manifest after the force-release.
        let parent_key = generate_keypair(&parent_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        let child_key = generate_keypair(&child_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        let recovering_key = ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng);

        write_provenance(
            &parent_dir,
            &parent_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        let data = vec![0xC1u8; 4096];
        let hash = blake3::hash(&data);
        let seg_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        // Segment is signed by the parent's own key (it was minted
        // when the parent was still alive).
        let parent_signer = load_signer(&parent_dir, VOLUME_KEY_FILE).unwrap();
        let staging = tmp.path().join(seg_ulid);
        write_segment(&staging, &mut entries, parent_signer.as_ref()).unwrap();

        let snap_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        std::fs::write(parent_dir.join("snapshots").join(snap_ulid), "").unwrap();

        // Child's signed provenance overrides the manifest pubkey to
        // the recovering coordinator's attestation key. The parent's
        // own pubkey still authoritatively verifies its segments.
        write_provenance(
            &child_dir,
            &child_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage {
                parent: Some(elide_core::signing::ParentRef {
                    volume_ulid: parent_ulid.to_owned(),
                    snapshot_ulid: snap_ulid.to_owned(),
                    pubkey: parent_key.verifying_key().to_bytes(),
                    manifest_pubkey: Some(recovering_key.verifying_key().to_bytes()),
                }),
                extent_index: Vec::new(),
                oci_source: None,
            },
        )
        .unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        let seg_bytes = std::fs::read(&staging).unwrap();
        let seg_key = crate::upload::segment_key(parent_ulid, seg_ulid.parse().unwrap());
        store.put(&seg_key, seg_bytes.into()).await.unwrap();

        // Synthesised manifest signed under the *recovering coord*
        // key, not the parent's. Without the override the verify
        // step fails with `manifest signature invalid`.
        struct AttestationSigner(ed25519_dalek::SigningKey);
        impl elide_core::segment::SegmentSigner for AttestationSigner {
            fn sign(&self, msg: &[u8]) -> [u8; 64] {
                use ed25519_dalek::Signer;
                self.0.sign(msg).to_bytes()
            }
        }
        let attestation_signer = AttestationSigner(recovering_key.clone());
        let manifest_bytes = build_snapshot_manifest_bytes(
            &attestation_signer,
            &[Ulid::from_string(seg_ulid).unwrap()],
            None,
        );
        let manifest_key =
            crate::upload::snapshot_manifest_key(parent_ulid, snap_ulid.parse().unwrap());
        store
            .put(&manifest_key, manifest_bytes.into())
            .await
            .unwrap();

        // Run prefetch on the child. Should succeed: manifest verifies
        // under the override key, the .idx itself verifies under the
        // parent's volume.pub.
        let result = prefetch_indexes(&child_dir, &store, None).await.unwrap();
        assert_eq!(
            result.fetched, 1,
            "synthesised handoff manifest must verify under override key"
        );
        assert_eq!(result.failed, 0);

        let idx_path = parent_dir.join("index").join(format!("{seg_ulid}.idx"));
        assert!(idx_path.exists(), ".idx fetched and written");
    }

    /// Root volume with a published manifest: warm-start `prefetch_indexes`
    /// uses the latest local manifest as the segment fetch set for the
    /// writable head — no LIST against `segments/`, no retention filter.
    #[tokio::test]
    async fn prefetch_indexes_fetches_own_segments_for_root() {
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let root_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let root_dir = by_id.join(root_ulid);
        std::fs::create_dir_all(&root_dir).unwrap();

        let key = generate_keypair(&root_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        write_provenance(
            &root_dir,
            &key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        let data = vec![0xCDu8; 4096];
        let hash = blake3::hash(&data);
        let seg_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let signer = load_signer(&root_dir, VOLUME_KEY_FILE).unwrap();
        let staging = tmp.path().join(seg_ulid);
        write_segment(&staging, &mut entries, signer.as_ref()).unwrap();

        // Upload the segment under its canonical S3 key.
        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        let seg_bytes = std::fs::read(&staging).unwrap();
        let key = crate::upload::segment_key(root_ulid, seg_ulid.parse().unwrap());
        store.put(&key, seg_bytes.into()).await.unwrap();

        // Publish a snapshot manifest at a snap ULID > seg ULID, signed
        // by the volume's own key. This is the manifest the warm-start
        // prefetch path will anchor on for the writable head.
        let snap_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let manifest_bytes = build_snapshot_manifest_bytes(
            signer.as_ref(),
            &[Ulid::from_string(seg_ulid).unwrap()],
            None,
        );
        let manifest_key =
            crate::upload::snapshot_manifest_key(root_ulid, snap_ulid.parse().unwrap());
        store
            .put(&manifest_key, manifest_bytes.into())
            .await
            .unwrap();

        let result = prefetch_indexes(&root_dir, &store, None).await.unwrap();
        assert_eq!(result.fetched, 1, "should fetch own .idx");
        assert_eq!(result.failed, 0);

        let idx_path = root_dir.join("index").join(format!("{seg_ulid}.idx"));
        assert!(idx_path.exists(), ".idx file should exist in root's index/");

        let (_, entries, _) = elide_core::segment::read_segment_index(&idx_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].hash, hash);
    }

    /// Fresh writable head with no published snapshot: warm-start
    /// `prefetch_indexes` is a no-op (manifest-anchored, nothing to
    /// anchor on). Operator-initiated capture of pre-snapshot bucket
    /// state belongs in [`pull_indexes_via_list`].
    #[tokio::test]
    async fn prefetch_indexes_is_noop_for_root_without_snapshot() {
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let root_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let root_dir = by_id.join(root_ulid);
        std::fs::create_dir_all(&root_dir).unwrap();

        let key = generate_keypair(&root_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        write_provenance(
            &root_dir,
            &key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        // Upload a segment to S3 but publish no snapshot. Warm-start
        // must not fetch it: the manifest is the trust anchor and
        // there is no manifest yet.
        let data = vec![0xCDu8; 4096];
        let seg_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mut entries = vec![SegmentEntry::new_data(
            blake3::hash(&data),
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let signer = load_signer(&root_dir, VOLUME_KEY_FILE).unwrap();
        let staging = tmp.path().join(seg_ulid);
        write_segment(&staging, &mut entries, signer.as_ref()).unwrap();
        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        let seg_bytes = std::fs::read(&staging).unwrap();
        let key = crate::upload::segment_key(root_ulid, seg_ulid.parse().unwrap());
        store.put(&key, seg_bytes.into()).await.unwrap();

        let result = prefetch_indexes(&root_dir, &store, None).await.unwrap();
        assert_eq!(result.fetched, 0, "no snapshot → no fetch");
        assert!(
            !root_dir
                .join("index")
                .join(format!("{seg_ulid}.idx"))
                .exists(),
            ".idx must not appear without a manifest anchor",
        );
    }

    /// Retention awareness on the LIST-driven path: an input segment
    /// listed in a `retention/` marker must be skipped during
    /// `pull_indexes_via_list` when the marker's GC output is also
    /// visible in the same `segments/` listing. The warm-start
    /// `prefetch_indexes` path no longer LISTs segments/, so the
    /// retention filter only applies to operator IPCs that capture
    /// raw bucket state.
    #[tokio::test]
    async fn pull_via_list_skips_segments_superseded_by_retention_marker() {
        use crate::retention::{marker_key, render_marker};

        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let root_ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let root_ulid = Ulid::from_string(root_ulid_str).unwrap();
        let root_dir = by_id.join(root_ulid_str);
        std::fs::create_dir_all(&root_dir).unwrap();

        let _key = generate_keypair(&root_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        let vk = elide_core::signing::load_verifying_key(&root_dir, VOLUME_PUB_FILE).unwrap();

        let old_ulid_str = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let new_ulid_str = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let old_ulid = Ulid::from_string(old_ulid_str).unwrap();
        let new_ulid = Ulid::from_string(new_ulid_str).unwrap();
        let signer = load_signer(&root_dir, VOLUME_KEY_FILE).unwrap();

        let write_seg = |ulid_str: &str, fill: u8| {
            let data = vec![fill; 4096];
            let mut entries = vec![SegmentEntry::new_data(
                blake3::hash(&data),
                0,
                1,
                SegmentFlags::empty(),
                data,
            )];
            let staging = tmp.path().join(ulid_str);
            write_segment(&staging, &mut entries, signer.as_ref()).unwrap();
            std::fs::read(&staging).unwrap()
        };
        let old_bytes = write_seg(old_ulid_str, 0x11);
        let new_bytes = write_seg(new_ulid_str, 0x22);

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        store
            .put(
                &crate::upload::segment_key(root_ulid_str, old_ulid_str.parse().unwrap()),
                old_bytes.into(),
            )
            .await
            .unwrap();
        store
            .put(
                &crate::upload::segment_key(root_ulid_str, new_ulid_str.parse().unwrap()),
                new_bytes.into(),
            )
            .await
            .unwrap();

        // Retention marker: `new` supersedes `old`.
        store
            .put(
                &marker_key(root_ulid, new_ulid),
                bytes::Bytes::from(render_marker(&[old_ulid])).into(),
            )
            .await
            .unwrap();

        let result = pull_indexes_via_list(&store, &root_dir, root_ulid_str, &vk, None)
            .await
            .unwrap();
        assert_eq!(result.fetched, 1, "only the GC output should be fetched");
        assert_eq!(result.superseded, 1, "old must be skipped via marker");
        assert_eq!(result.failed, 0);

        assert!(
            root_dir
                .join("index")
                .join(format!("{new_ulid_str}.idx"))
                .exists(),
            "GC output .idx must be fetched"
        );
        assert!(
            !root_dir
                .join("index")
                .join(format!("{old_ulid_str}.idx"))
                .exists(),
            "superseded input .idx must NOT be fetched"
        );
    }

    /// Defensive case on the LIST-driven path: a marker exists but
    /// its GC output is not observed in the segments LIST. The
    /// pull must NOT skip the input — we'd otherwise drop coverage
    /// of the input's LBAs entirely. This races against an ordering
    /// inversion that shouldn't happen given GC's PUT order, but
    /// it's cheap to guard against and surfaces as a wasted (correct)
    /// fetch.
    #[tokio::test]
    async fn pull_via_list_does_not_skip_input_when_output_missing() {
        use crate::retention::{marker_key, render_marker};

        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let root_ulid_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let root_ulid = Ulid::from_string(root_ulid_str).unwrap();
        let root_dir = by_id.join(root_ulid_str);
        std::fs::create_dir_all(&root_dir).unwrap();

        let _key = generate_keypair(&root_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        let vk = elide_core::signing::load_verifying_key(&root_dir, VOLUME_PUB_FILE).unwrap();

        let old_ulid_str = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let phantom_new_str = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let old_ulid = Ulid::from_string(old_ulid_str).unwrap();
        let phantom_new = Ulid::from_string(phantom_new_str).unwrap();
        let signer = load_signer(&root_dir, VOLUME_KEY_FILE).unwrap();

        let data = vec![0x33; 4096];
        let mut entries = vec![SegmentEntry::new_data(
            blake3::hash(&data),
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let staging = tmp.path().join(old_ulid_str);
        write_segment(&staging, &mut entries, signer.as_ref()).unwrap();
        let bytes_old = std::fs::read(&staging).unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        store
            .put(
                &crate::upload::segment_key(root_ulid_str, old_ulid_str.parse().unwrap()),
                bytes_old.into(),
            )
            .await
            .unwrap();

        // Marker references a phantom GC output that is NOT in segments/.
        store
            .put(
                &marker_key(root_ulid, phantom_new),
                bytes::Bytes::from(render_marker(&[old_ulid])).into(),
            )
            .await
            .unwrap();

        let result = pull_indexes_via_list(&store, &root_dir, root_ulid_str, &vk, None)
            .await
            .unwrap();
        assert_eq!(result.fetched, 1);
        assert_eq!(result.superseded, 0);
        assert_eq!(result.failed, 0);
        assert!(
            root_dir
                .join("index")
                .join(format!("{old_ulid_str}.idx"))
                .exists(),
            "input must be fetched when replacement is not observed"
        );
    }

    #[tokio::test]
    async fn prefetch_downloads_snapshot_marker_but_skips_filemap() {
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let vol_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let vol_dir = by_id.join(vol_ulid);
        std::fs::create_dir_all(&vol_dir).unwrap();

        // Generate the volume's keypair + provenance on disk so prefetch can
        // verify signatures and walk (empty) lineage.
        let key = generate_keypair(&vol_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        write_provenance(
            &vol_dir,
            &key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        // Manifest is the snapshot record (post-#215). The bucket
        // also has a stale `.filemap` (pre-#212) and a stale bare-ULID
        // marker (pre-#215); both must be skipped. The manifest is a
        // real signed (empty-segment) one because the manifest-driven
        // .idx path will read+verify it under the volume's pubkey.
        let snap_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let signer = load_signer(&vol_dir, VOLUME_KEY_FILE).unwrap();
        let manifest_bytes = build_snapshot_manifest_bytes(signer.as_ref(), &[], None);
        store
            .put(
                &crate::upload::snapshot_manifest_key(vol_ulid, snap_ulid.parse().unwrap()),
                manifest_bytes.into(),
            )
            .await
            .unwrap();
        let bare_key = StorePath::from(format!("by_id/{vol_ulid}/snapshots/19700101/{snap_ulid}"));
        store
            .put(&bare_key, bytes::Bytes::new().into())
            .await
            .unwrap();
        let fm_key = StorePath::from(format!(
            "by_id/{vol_ulid}/snapshots/19700101/{snap_ulid}.filemap"
        ));
        store
            .put(
                &fm_key,
                bytes::Bytes::from_static(b"# elide-filemap v1\n").into(),
            )
            .await
            .unwrap();

        let result = prefetch_indexes(&vol_dir, &store, None).await.unwrap();
        assert_eq!(result.failed, 0);

        let snap_dir = vol_dir.join("snapshots");
        assert!(snap_dir.join(format!("{snap_ulid}.manifest")).exists());
        assert!(!snap_dir.join(snap_ulid).exists());
        assert!(!snap_dir.join(format!("{snap_ulid}.filemap")).exists());

        // Idempotent: re-running doesn't re-download.
        prefetch_indexes(&vol_dir, &store, None).await.unwrap();
    }

    /// Ancestor-walk path: when an ancestor has multiple snapshots in
    /// its bucket prefix, only the snapshot at the branch point (and
    /// its sibling `.manifest`) should be downloaded into
    /// the local fork — older snapshots in the ancestor are not on the
    /// read path of this fork. Regression for the "every claim pulls
    /// every ancestor snapshot" pattern observed in production logs.
    #[tokio::test]
    async fn ancestor_walk_only_fetches_branch_point_snapshot() {
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");

        let parent_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        let parent_dir = by_id.join(parent_ulid);
        let child_dir = by_id.join(child_ulid);
        std::fs::create_dir_all(&parent_dir).unwrap();
        std::fs::create_dir_all(child_dir.join("pending")).unwrap();

        let parent_key = generate_keypair(&parent_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        let child_key = generate_keypair(&child_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        write_provenance(
            &parent_dir,
            &parent_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        // Three snapshots in the parent's bucket prefix. Only `branch`
        // is the branch point recorded in the child's provenance; the
        // other two should NOT be downloaded.
        let branch = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let earlier = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let later = "01CCCCCCCCCCCCCCCCCCCCCCCC";

        write_provenance(
            &child_dir,
            &child_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage {
                parent: Some(elide_core::signing::ParentRef {
                    volume_ulid: parent_ulid.to_owned(),
                    snapshot_ulid: branch.to_owned(),
                    pubkey: parent_key.verifying_key().to_bytes(),
                    manifest_pubkey: None,
                }),
                extent_index: Vec::new(),
                oci_source: None,
            },
        )
        .unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        // Upload all three snapshots' artifacts into the parent's
        // bucket prefix at canonical keys. The branch-point manifest
        // is a real signed (empty-segment) manifest because the
        // manifest-driven .idx path will read+verify it; the other two
        // are junk to verify the branch filter doesn't pull them.
        let parent_signer = load_signer(&parent_dir, VOLUME_KEY_FILE).unwrap();
        let branch_manifest_bytes =
            build_snapshot_manifest_bytes(parent_signer.as_ref(), &[], None);
        for snap in [earlier, branch, later] {
            let manifest_key =
                crate::upload::snapshot_manifest_key(parent_ulid, snap.parse().unwrap());
            let manifest_payload = if snap == branch {
                branch_manifest_bytes.clone().into()
            } else {
                bytes::Bytes::from_static(b"x").into()
            };
            store.put(&manifest_key, manifest_payload).await.unwrap();
        }

        prefetch_indexes(&child_dir, &store, None).await.unwrap();

        let parent_snap_dir = parent_dir.join("snapshots");
        assert!(
            parent_snap_dir.join(format!("{branch}.manifest")).exists(),
            "branch manifest missing",
        );
        // Bare-ULID and `.filemap` siblings are not pulled even at the
        // branch point. Non-branch ancestor snapshots stay remote.
        assert!(!parent_snap_dir.join(branch).exists());
        assert!(!parent_snap_dir.join(format!("{branch}.filemap")).exists());
        for snap in [earlier, later] {
            for suffix in ["", ".manifest", ".filemap"] {
                let p = parent_snap_dir.join(format!("{snap}{suffix}"));
                assert!(!p.exists(), "{snap}{suffix} should not exist locally");
            }
        }
    }

    /// `peer_fetch_idx` against an unreachable peer returns an error so
    /// `prefetch_fork` falls through to S3. Covers the negative path
    /// without requiring a live peer fixture (which the
    /// `elide-peer-fetch::client` tests already exercise).
    #[tokio::test]
    async fn peer_fetch_idx_unreachable_returns_error() {
        use elide_peer_fetch::PeerEndpoint;
        use std::sync::Arc as StdArc;

        #[derive(Debug)]
        struct StubSigner;
        impl elide_peer_fetch::TokenSigner for StubSigner {
            fn coordinator_id(&self) -> &str {
                "stub-coord"
            }
            fn sign(&self, _msg: &[u8]) -> [u8; 64] {
                [0u8; 64]
            }
        }

        let signer: StdArc<dyn elide_peer_fetch::TokenSigner> = StdArc::new(StubSigner);
        let client = elide_peer_fetch::PeerFetchClient::builder(signer)
            .request_timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();

        let endpoint = PeerEndpoint::new("127.0.0.1".to_owned(), 1); // ECONNREFUSED

        let peer_ctx = PeerFetchContext {
            client,
            endpoint,
            volume_name: "any".to_owned(),
        };

        let tmp = TempDir::new().unwrap();
        let fork_dir = tmp.path().join("by_id").join("01ZZ00000000000000000000ZZ");
        std::fs::create_dir_all(&fork_dir).unwrap();

        let vol_ulid = Ulid::new();
        let seg_ulid = Ulid::new();
        let dummy_vk = ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng).verifying_key();

        let result = peer_fetch_idx(
            &peer_ctx,
            vol_ulid,
            seg_ulid,
            &fork_dir,
            &seg_ulid.to_string(),
            &dummy_vk,
        )
        .await;

        assert!(
            result.is_err(),
            "peer_fetch_idx should propagate error so the caller falls through to S3"
        );
    }

    /// Branch-point fast path: with a peer context but an unreachable
    /// peer, `prefetch_branch_snapshot_artifacts` must fall through to
    /// the keyed S3 GETs (no LIST) and still land all three artifacts
    /// locally. Asserts the artifacts arrive *and* that the
    /// `snapshots_fetched` accounting reflects S3 (not peer).
    #[tokio::test]
    async fn branch_snapshot_fast_path_falls_back_to_s3_when_peer_unreachable() {
        use elide_peer_fetch::PeerEndpoint;
        use std::sync::Arc as StdArc;

        #[derive(Debug)]
        struct StubSigner;
        impl elide_peer_fetch::TokenSigner for StubSigner {
            fn coordinator_id(&self) -> &str {
                "stub-coord"
            }
            fn sign(&self, _msg: &[u8]) -> [u8; 64] {
                [0u8; 64]
            }
        }

        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");

        let parent_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        let parent_dir = by_id.join(parent_ulid);
        let child_dir = by_id.join(child_ulid);
        std::fs::create_dir_all(&parent_dir).unwrap();
        std::fs::create_dir_all(child_dir.join("pending")).unwrap();

        let parent_key = generate_keypair(&parent_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        let child_key = generate_keypair(&child_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        write_provenance(
            &parent_dir,
            &parent_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        let branch = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        write_provenance(
            &child_dir,
            &child_key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage {
                parent: Some(elide_core::signing::ParentRef {
                    volume_ulid: parent_ulid.to_owned(),
                    snapshot_ulid: branch.to_owned(),
                    pubkey: parent_key.verifying_key().to_bytes(),
                    manifest_pubkey: None,
                }),
                extent_index: Vec::new(),
                oci_source: None,
            },
        )
        .unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        // Upload a real signed (empty-segment) manifest at its
        // canonical key — the manifest-driven .idx path will read +
        // verify it. Per PR #215 the manifest is the sole snapshot
        // record; the test also uploads a stale bare-ULID marker
        // below to confirm prefetch ignores pre-#215 bucket residue.
        let parent_signer = load_signer(&parent_dir, VOLUME_KEY_FILE).unwrap();
        let manifest_bytes = build_snapshot_manifest_bytes(parent_signer.as_ref(), &[], None);
        let manifest_key =
            crate::upload::snapshot_manifest_key(parent_ulid, branch.parse().unwrap());
        store
            .put(&manifest_key, manifest_bytes.into())
            .await
            .unwrap();
        let bare_marker =
            StorePath::from(format!("by_id/{parent_ulid}/snapshots/19700101/{branch}"));
        store
            .put(&bare_marker, bytes::Bytes::new().into())
            .await
            .unwrap();

        // Build a peer context pointing at port 1 (ECONNREFUSED).
        let signer: StdArc<dyn elide_peer_fetch::TokenSigner> = StdArc::new(StubSigner);
        let client = elide_peer_fetch::PeerFetchClient::builder(signer)
            .request_timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();
        let peer = PeerFetchContext {
            client,
            endpoint: PeerEndpoint::new("127.0.0.1".to_owned(), 1),
            volume_name: "myvol".to_owned(),
        };

        let result = prefetch_indexes(&child_dir, &store, Some(&peer))
            .await
            .unwrap();

        // Manifest lands via S3 fallback; bare marker is ignored.
        let snap_dir = parent_dir.join("snapshots");
        assert!(
            snap_dir.join(format!("{branch}.manifest")).exists(),
            "branch manifest should land via S3 fallback",
        );
        assert!(!snap_dir.join(branch).exists());

        // 1 fetched (manifest) from S3; peer was unreachable.
        assert_eq!(result.snapshots_fetched, 1);
        assert_eq!(result.snapshots_from_peer, 0);
    }
}
