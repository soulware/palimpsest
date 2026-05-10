//! `elide fetch-volume` worker. Spawned by the coordinator to run a
//! synchronous `full_warm` pass against a foreign volume's by_id
//! directory, blocking until every live cold extent has been pulled
//! from S3.
//!
//! The coordinator is responsible for setting up the directory before
//! spawn: pulling the ancestor skeleton chain, fetching and verifying
//! the basis snapshot manifest, and pulling every `.idx` referenced
//! by the manifest. By the time this binary runs, `index/` is
//! populated and `Volume::open` (readonly) succeeds. The worker only
//! does the body-fetch leg.
//!
//! Auth: same `LazyCredsFetcher` flow the volume daemon uses, but
//! over `Request::RegisterFetchWorker` (PID-bound to `fetch.pid`
//! rather than `volume.pid`) and macaroons stamped with
//! `Scope::FetchWorker`. The orchestrator plants `fetch.pid` with
//! the worker's PID before this binary starts; the worker dials
//! back and the coordinator verifies SO_PEERCRED matches the file.

use std::path::Path;
use std::sync::Arc;

use elide_core::volume::ReadonlyVolume;
use elide_fetch::FetchConfig;

use crate::{CredsReissue, VolumeFetchInputs, coordinator_client};

/// Run the fetch worker against a fully-set-up by_id directory.
///
/// Returns when `full_warm` has joined every worker thread. Exits the
/// process via the caller; this function returns `io::Result<()>`.
pub fn run(fork_dir: &Path) -> std::io::Result<()> {
    let by_id_dir = fork_dir
        .parent()
        .ok_or_else(|| std::io::Error::other("fork_dir has no parent (expected by_id/<ulid>/)"))?;

    let inputs = resolve_worker_fetch_config(fork_dir)?;
    let build = crate::build_volume_fetcher(fork_dir, inputs)?;
    let Some(build) = build else {
        return Err(std::io::Error::other(
            "fetch worker requires a fetch config (S3 or local_path); none resolved",
        ));
    };
    let fetcher: Arc<dyn elide_core::segment::SegmentFetcher> = Arc::clone(&build.fetcher) as _;

    let mut volume = ReadonlyVolume::open(fork_dir, by_id_dir)?;
    volume.set_fetcher(Arc::clone(&fetcher));
    let (lba_map, extent_index) = volume.snapshot_maps();
    let fork_dirs = volume.fork_dirs();

    elide_fetch::full_warm::run_blocking(fork_dirs, lba_map, extent_index, fetcher);

    Ok(())
}

/// Worker-specific config resolution. Mirrors the volume daemon's
/// path closely (`GetStoreConfig` → register → `LazyCredsFetcher`)
/// but talks to `Request::RegisterFetchWorker` rather than
/// `Request::Register`. The macaroon comes back stamped with
/// `Scope::FetchWorker`; `Request::Credentials` accepts both scopes
/// so the lazy-creds wrapper's idle re-issue path works uniformly.
fn resolve_worker_fetch_config(fork_dir: &Path) -> std::io::Result<VolumeFetchInputs> {
    let sock = std::env::var("ELIDE_COORDINATOR_SOCKET").map_err(|_| {
        std::io::Error::other(
            "ELIDE_COORDINATOR_SOCKET not set; fetch worker must be coordinator-spawned",
        )
    })?;
    let coord = coordinator_client::Client::new(&sock);
    let store_config = coord
        .get_store_config()
        .map_err(|e| std::io::Error::other(format!("get_store_config: {e}")))?;

    if let Some(local_path) = store_config.local_path {
        return Ok(VolumeFetchInputs {
            fetch_config: Some(FetchConfig {
                bucket: None,
                endpoint: None,
                region: None,
                local_path: Some(local_path),
                fetch_batch_bytes: None,
            }),
            creds: None,
            reissue: None,
            peer_endpoint: None,
        });
    }

    let bucket = store_config.bucket.ok_or_else(|| {
        std::io::Error::other("coordinator returned empty store config (no bucket, no local_path)")
    })?;

    let volume_ulid = elide_fetch::derive_volume_id(fork_dir)?;
    let registered = coord.register_fetch_worker_with_retry(&volume_ulid)?;
    let reissue = CredsReissue {
        coordinator_socket: coord.socket_path().to_path_buf(),
        macaroon: registered.macaroon,
    };
    Ok(VolumeFetchInputs {
        fetch_config: Some(FetchConfig {
            bucket: Some(bucket),
            endpoint: store_config.endpoint,
            region: store_config.region,
            local_path: None,
            fetch_batch_bytes: None,
        }),
        creds: None,
        reissue: Some(reissue),
        peer_endpoint: None,
    })
}
