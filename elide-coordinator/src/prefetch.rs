// Prefetch segment index files from S3.
//
// Before a forked volume can be opened on a host that has no local segments for
// its ancestors, the index sections (.idx files) must be downloaded so that
// Volume::open can rebuild the LBA map and extent index. Body bytes are not
// fetched; individual extent reads trigger demand-fetch on the first access.
//
// This is the "warm start" step for a fresh host mounting a forked volume.
//
// For each ancestor fork in the rebuild chain:
//   1. List S3 objects under by_id/<volume_id>/
//   2. For each ULID that passes the branch-point cutoff and is not already
//      present in segments/ or cache/:
//        a. Range-GET [0..96] to parse the header (determines body_section_start)
//        b. Range-GET [0..body_section_start] — header + index + inline
//        c. Write atomically to <ancestor_fork_dir>/index/<ulid>.idx
//
// After this runs, Volume::open will find the .idx files, rebuild_segments and
// extentindex::rebuild will both scan index/*.idx, and the volume opens
// correctly. Individual reads then demand-fetch the body bytes on first access.

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
    pub failed: usize,
    /// Snapshot artifacts (markers, manifests, filemaps) downloaded
    /// during this run. Counted separately from `.idx` so the per-run
    /// log line keeps the index/snapshot signals distinct.
    pub snapshots_fetched: usize,
    /// Of `snapshots_fetched`, how many came from a peer.
    pub snapshots_from_peer: usize,
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
        tasks.push(PrefetchTask {
            dir: parent_dir.clone(),
            volume_id: parent.volume_ulid.clone(),
            branch_ulid: Some(parent.snapshot_ulid.clone()),
            vk: parent_vk,
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
        tasks.push(PrefetchTask {
            dir: ancestor_dir,
            volume_id,
            branch_ulid: ancestor.branch_ulid,
            vk: ancestor_vk,
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
        result.failed += r.failed;
        result.snapshots_fetched += r.snapshots_fetched;
        result.snapshots_from_peer += r.snapshots_from_peer;
    }
    Ok(result)
}

/// One unit of prefetch work collected during the lineage walk and
/// dispatched concurrently in Pass 2.
struct PrefetchTask {
    dir: PathBuf,
    volume_id: String,
    branch_ulid: Option<String>,
    vk: VerifyingKey,
}

async fn prefetch_fork(
    store: &Arc<dyn ObjectStore>,
    fork_dir: &Path,
    volume_id: &str,
    branch_ulid: Option<&str>,
    verifying_key: &VerifyingKey,
    peer: Option<&PeerFetchContext>,
    result: &mut PrefetchResult,
) -> Result<()> {
    // Prefetch segment indexes from segments/ sub-prefix.
    let seg_prefix = StorePath::from(format!("by_id/{volume_id}/segments/"));
    let objects: Vec<_> = store
        .list(Some(&seg_prefix))
        .try_collect()
        .await
        .with_context(|| format!("listing by_id/{volume_id}/segments/"))?;

    // Sync filter: drop bad-ULID / past-cutoff / already-local entries
    // up front. Counts already-local as skipped (matches the previous
    // sequential behaviour); other rejections are silent (malformed
    // listings shouldn't pollute the operator log).
    let vol_ulid = Ulid::from_string(volume_id).ok();
    let to_fetch: Vec<(StorePath, String, Ulid)> = objects
        .into_iter()
        .filter_map(|obj| {
            let ulid_str = obj.location.filename()?.to_owned();
            let ulid = Ulid::from_string(&ulid_str).ok()?;
            if branch_ulid.is_some_and(|cutoff| ulid_str.as_str() > cutoff) {
                return None;
            }
            let local_idx = fork_dir.join("index").join(format!("{ulid_str}.idx"));
            if local_idx.exists() {
                result.skipped += 1;
                return None;
            }
            Some((obj.location, ulid_str, ulid))
        })
        .collect();

    // Per-segment outcome from one parallel fetch. Aggregated into the
    // shared `PrefetchResult` after the bounded parallel pass.
    enum IdxOutcome {
        FromPeer,
        FromStore,
        Failed,
    }

    let outcomes: Vec<IdxOutcome> =
        futures::stream::iter(to_fetch.into_iter().map(|(location, ulid_str, ulid)| {
            let store = store.clone();
            let fork_dir = fork_dir.to_path_buf();
            let verifying_key = *verifying_key;
            async move {
                // Peer-fetch tier (best-effort): try the peer first if
                // a context is available and we resolved both ULIDs.
                // On any failure path fall through to S3.
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
                            return IdxOutcome::FromPeer;
                        }
                        Err(e) => {
                            trace!(
                                "[prefetch] peer miss for {ulid_str}: {e:#}; falling through to S3"
                            );
                        }
                    }
                }

                match fetch_idx(&store, &location, &fork_dir, &ulid_str, &verifying_key).await {
                    Ok(()) => {
                        info!("[prefetch] fetched index: {ulid_str}");
                        IdxOutcome::FromStore
                    }
                    Err(e) => {
                        warn!("[prefetch] failed to fetch index {ulid_str}: {e:#}");
                        IdxOutcome::Failed
                    }
                }
            }
        }))
        .buffer_unordered(PREFETCH_CONCURRENCY)
        .collect()
        .await;

    for outcome in outcomes {
        match outcome {
            IdxOutcome::FromPeer => {
                result.fetched += 1;
                result.fetched_from_peer += 1;
            }
            IdxOutcome::FromStore => result.fetched += 1,
            IdxOutcome::Failed => result.failed += 1,
        }
    }

    // Prefetch snapshot markers and filemaps from snapshots/ sub-prefix.
    // For ancestor walks (`branch_ulid = Some(...)`), restrict to just the
    // branch-point snapshot's artifacts — older snapshots in the ancestor
    // are not on the read path of this fork, and snapshots newer than the
    // branch point belong to a parallel timeline. For the current
    // writable fork (`branch_ulid = None`) we still fetch every snapshot
    // present in the bucket so the read path can replay against any of
    // them.
    prefetch_snapshots(store, fork_dir, volume_id, branch_ulid, peer, result).await?;

    Ok(())
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

/// Download snapshot markers, manifests, and filemaps from S3 (and/or
/// the configured peer) into `<fork_dir>/snapshots/`.
///
/// Two paths:
///
/// 1. **Branch-point fast path** — `branch_ulid = Some(b)` and a peer
///    context is available. The three artifact names are known
///    up-front (`<b>`, `<b>.manifest`, `<b>.filemap`); we issue all
///    three peer GETs in parallel via [`futures::future::join_all`]
///    and skip the S3 LIST entirely on the happy path. For any
///    artifact the peer doesn't have, we fall through to a per-name
///    S3 GET keyed via [`crate::upload::snapshot_key`] /
///    [`crate::upload::snapshot_manifest_key`] /
///    [`crate::upload::filemap_key`] — still no LIST.
///
/// 2. **Listed path** — `branch_ulid = None` (current writable fork)
///    or no peer context. Lists `by_id/<volume_id>/snapshots/`,
///    filters by branch (if set) and skip-if-local, and fetches the
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
    let to_fetch: Vec<(object_store::path::Path, String)> = objects
        .into_iter()
        .filter_map(|obj| {
            let filename = obj.location.filename()?.to_owned();
            if let Some(branch) = branch_ulid {
                let matches = filename == branch
                    || filename == format!("{branch}.manifest")
                    || filename == format!("{branch}.filemap");
                if !matches {
                    return None;
                }
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

/// Branch-point fast path. Fetch the three known artifact names in
/// parallel — peer first, S3 by-name on miss, no LIST on either tier.
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
    #[derive(Clone, Copy)]
    enum Kind {
        Marker,
        Manifest,
        Filemap,
    }

    let candidates: [(Kind, String); 3] = [
        (Kind::Marker, snap_ulid_str.to_string()),
        (Kind::Manifest, format!("{snap_ulid_str}.manifest")),
        (Kind::Filemap, format!("{snap_ulid_str}.filemap")),
    ];

    // Skip-if-local up front — same semantics as the listed path.
    let to_fetch: Vec<(Kind, String)> = candidates
        .into_iter()
        .filter(|(_, name)| !snap_dir.join(name).exists())
        .collect();

    if to_fetch.is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(snap_dir).context("creating snapshots dir")?;

    // Phase 1: try the peer for each artifact in parallel. Successful
    // fetches are written immediately; misses go to the S3 fallback
    // list. Each peer call already collapses every error mode to
    // `None`, so this future set never short-circuits on failure.
    let peer_futs = to_fetch.iter().map(|(kind, _)| {
        let client = peer.client.clone();
        let endpoint = peer.endpoint.clone();
        let volume_name = peer.volume_name.clone();
        let kind = *kind;
        async move {
            match kind {
                Kind::Marker => {
                    client
                        .fetch_snapshot_marker(&endpoint, &volume_name, vol_ulid, snap_ulid)
                        .await
                }
                Kind::Manifest => {
                    client
                        .fetch_snapshot_manifest(&endpoint, &volume_name, vol_ulid, snap_ulid)
                        .await
                }
                Kind::Filemap => {
                    client
                        .fetch_snapshot_filemap(&endpoint, &volume_name, vol_ulid, snap_ulid)
                        .await
                }
            }
        }
    });
    let peer_outcomes = futures::future::join_all(peer_futs).await;

    let mut s3_fallback: Vec<(Kind, String)> = Vec::new();
    for ((kind, name), peer_bytes) in to_fetch.into_iter().zip(peer_outcomes) {
        match peer_bytes {
            Some(bytes) => {
                write_snapshot_artifact_atomic(snap_dir, &name, &bytes)?;
                info!("[prefetch] fetched snapshot artifact from peer: {name}");
                result.snapshots_fetched += 1;
                result.snapshots_from_peer += 1;
            }
            None => {
                trace!("[prefetch] peer miss for snapshot artifact {name}; falling through to S3");
                s3_fallback.push((kind, name));
            }
        }
    }

    // Phase 2: S3 fallback for any peer misses, also in parallel.
    // Each artifact's S3 key is computable from `(volume_id, snap_ulid_str)`
    // via the upload helpers; no LIST needed.
    if s3_fallback.is_empty() {
        return Ok(());
    }
    let n = s3_fallback.len();
    let snap_dir_owned = snap_dir.to_path_buf();
    futures::stream::iter(s3_fallback.into_iter().map(|(kind, name)| {
        let store = store.clone();
        let snap_dir = snap_dir_owned.clone();
        let volume_id = volume_id.to_owned();
        let snap_ulid_str = snap_ulid_str.to_owned();
        async move {
            let key = match kind {
                Kind::Marker => crate::upload::snapshot_key(&volume_id, &snap_ulid_str),
                Kind::Manifest => crate::upload::snapshot_manifest_key(&volume_id, &snap_ulid_str),
                Kind::Filemap => crate::upload::filemap_key(&volume_id, &snap_ulid_str),
            }?;
            let data = store
                .get(&key)
                .await
                .with_context(|| format!("downloading {key}"))?
                .bytes()
                .await
                .with_context(|| format!("reading {key}"))?;
            write_snapshot_artifact_atomic(&snap_dir, &name, &data)?;
            info!("[prefetch] fetched snapshot artifact: {name}");
            anyhow::Ok(())
        }
    }))
    .buffer_unordered(PREFETCH_CONCURRENCY)
    .try_collect::<Vec<()>>()
    .await?;

    result.snapshots_fetched += n;

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
        generate_keypair, load_signer, write_provenance,
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
        // by_id/ key. Use a fixed date since seg_ulid has ts=0.
        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        let seg_bytes = std::fs::read(&staging).unwrap();
        let key = StorePath::from(format!("by_id/{parent_ulid}/segments/19700101/{seg_ulid}"));
        store.put(&key, seg_bytes.into()).await.unwrap();

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
        let key = StorePath::from(format!("by_id/{parent_ulid}/segments/19700101/{seg_ulid}"));
        store.put(&key, seg_bytes.into()).await.unwrap();

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

    /// Root volume (no volume.parent): prefetch_indexes must fetch its own segments.
    #[tokio::test]
    async fn prefetch_indexes_fetches_own_segments_for_root() {
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let root_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let root_dir = by_id.join(root_ulid);
        std::fs::create_dir_all(&root_dir).unwrap();

        // Generate the root's keypair + provenance on disk so prefetch can
        // verify signatures and walk (empty) lineage.
        let key = generate_keypair(&root_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE).unwrap();
        write_provenance(
            &root_dir,
            &key,
            VOLUME_PROVENANCE_FILE,
            &ProvenanceLineage::default(),
        )
        .unwrap();

        // Write one segment to a staging file, upload it, then discard locally.
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

        // Upload the segment to the store.
        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        let seg_bytes = std::fs::read(&staging).unwrap();
        let key = StorePath::from(format!("by_id/{root_ulid}/segments/19700101/{seg_ulid}"));
        store.put(&key, seg_bytes.into()).await.unwrap();

        // prefetch_indexes should now fetch the root's own segment.
        let result = prefetch_indexes(&root_dir, &store, None).await.unwrap();
        assert_eq!(result.fetched, 1, "should fetch own .idx");
        assert_eq!(result.failed, 0);

        let idx_path = root_dir.join("index").join(format!("{seg_ulid}.idx"));
        assert!(idx_path.exists(), ".idx file should exist in root's index/");

        let (_, entries, _) = elide_core::segment::read_segment_index(&idx_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].hash, hash);
    }

    #[tokio::test]
    async fn prefetch_downloads_snapshot_and_filemap() {
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

        // Upload a snapshot marker and filemap to the store.
        let snap_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let snap_key = StorePath::from(format!("by_id/{vol_ulid}/snapshots/19700101/{snap_ulid}"));
        store
            .put(&snap_key, bytes::Bytes::new().into())
            .await
            .unwrap();

        let filemap_content = b"# elide-filemap v1\n/etc/hosts\tabcd1234\t128\n";
        let fm_key = StorePath::from(format!(
            "by_id/{vol_ulid}/snapshots/19700101/{snap_ulid}.filemap"
        ));
        store
            .put(&fm_key, bytes::Bytes::from_static(filemap_content).into())
            .await
            .unwrap();

        // Run prefetch — should download both snapshot marker and filemap.
        let result = prefetch_indexes(&vol_dir, &store, None).await.unwrap();
        assert_eq!(result.failed, 0);

        let snap_dir = vol_dir.join("snapshots");
        assert!(
            snap_dir.join(snap_ulid).exists(),
            "snapshot marker should exist locally"
        );
        let local_filemap = snap_dir.join(format!("{snap_ulid}.filemap"));
        assert!(local_filemap.exists(), "filemap should exist locally");
        assert_eq!(std::fs::read(&local_filemap).unwrap(), filemap_content);

        // Running again should skip (already present).
        prefetch_indexes(&vol_dir, &store, None).await.unwrap();
        // No error, no re-download.
    }

    /// Ancestor-walk path: when an ancestor has multiple snapshots in
    /// its bucket prefix, only the snapshot at the branch point (and
    /// its sibling `.manifest` / `.filemap`) should be downloaded into
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
        // bucket prefix. Each gets bare + `.manifest` + `.filemap`.
        for snap in [earlier, branch, later] {
            for suffix in ["", ".manifest", ".filemap"] {
                let key = StorePath::from(format!(
                    "by_id/{parent_ulid}/snapshots/19700101/{snap}{suffix}"
                ));
                store
                    .put(&key, bytes::Bytes::from_static(b"x").into())
                    .await
                    .unwrap();
            }
        }

        prefetch_indexes(&child_dir, &store, None).await.unwrap();

        // Only branch-point artifacts should appear locally in the
        // parent's snapshots/ dir.
        let parent_snap_dir = parent_dir.join("snapshots");
        for suffix in ["", ".manifest", ".filemap"] {
            let p = parent_snap_dir.join(format!("{branch}{suffix}"));
            assert!(
                p.exists(),
                "branch artifact {branch}{suffix} should be local"
            );
        }
        for snap in [earlier, later] {
            for suffix in ["", ".manifest", ".filemap"] {
                let p = parent_snap_dir.join(format!("{snap}{suffix}"));
                assert!(
                    !p.exists(),
                    "non-branch ancestor snapshot {snap}{suffix} \
                     should NOT have been pulled, but exists at {}",
                    p.display(),
                );
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

        // Upload the three branch-point artifacts to S3 under the
        // canonical date-partitioned keys. Use the upload helpers so
        // the test stays in sync with the production keying.
        let marker_key = crate::upload::snapshot_key(parent_ulid, branch).unwrap();
        let manifest_key = crate::upload::snapshot_manifest_key(parent_ulid, branch).unwrap();
        let filemap_key = crate::upload::filemap_key(parent_ulid, branch).unwrap();
        store
            .put(&marker_key, bytes::Bytes::new().into())
            .await
            .unwrap();
        store
            .put(
                &manifest_key,
                bytes::Bytes::from_static(b"manifest-bytes").into(),
            )
            .await
            .unwrap();
        store
            .put(
                &filemap_key,
                bytes::Bytes::from_static(b"# elide-filemap v2\n").into(),
            )
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

        // All three artifacts present locally under the parent.
        let snap_dir = parent_dir.join("snapshots");
        for suffix in ["", ".manifest", ".filemap"] {
            assert!(
                snap_dir.join(format!("{branch}{suffix}")).exists(),
                "branch artifact {branch}{suffix} should land via S3 fallback"
            );
        }

        // All snapshots came from S3 (peer was unreachable).
        assert_eq!(result.snapshots_fetched, 3);
        assert_eq!(result.snapshots_from_peer, 0);
    }
}
