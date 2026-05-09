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
//! Config / credential resolution is intentionally lighter than the
//! volume daemon's: we call `GetStoreConfig` (unauth) for the bucket
//! / endpoint / region (or local_path), and read `AWS_*` from the
//! env for credentials. Both are set by the coordinator at spawn
//! time. We do **not** go through the volume-daemon macaroon
//! handshake — `register_volume` is PID-bound to a `volume.pid` file
//! the fetch worker doesn't own, and the worker is short-lived
//! anyway.

use std::path::Path;
use std::sync::Arc;

use elide_core::volume::ReadonlyVolume;
use elide_fetch::{FetchConfig, S3Credentials};

use crate::{VolumeFetchInputs, coordinator_client};

/// Run the fetch worker against a fully-set-up by_id directory.
///
/// Returns when `full_warm` has joined every worker thread. Exits the
/// process via the caller; this function returns `io::Result<()>`.
pub fn run(fork_dir: &Path) -> std::io::Result<()> {
    let by_id_dir = fork_dir
        .parent()
        .ok_or_else(|| std::io::Error::other("fork_dir has no parent (expected by_id/<ulid>/)"))?;

    let inputs = resolve_worker_fetch_config()?;
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

/// Worker-specific config resolution. The volume daemon's
/// `resolve_volume_fetch_config` goes through the macaroon handshake,
/// which requires a `volume.pid` file the fetch worker doesn't own.
/// Instead we rely on the unauthenticated `GetStoreConfig` IPC for
/// the store metadata and on `AWS_*` env vars for credentials (set
/// by the coordinator at spawn time, sourced from its
/// `CredentialIssuer`).
fn resolve_worker_fetch_config() -> std::io::Result<VolumeFetchInputs> {
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

    let creds = s3_creds_from_env()?;
    Ok(VolumeFetchInputs {
        fetch_config: Some(FetchConfig {
            bucket: Some(bucket),
            endpoint: store_config.endpoint,
            region: store_config.region,
            local_path: None,
            fetch_batch_bytes: None,
        }),
        creds: Some(creds),
        reissue: None,
        peer_endpoint: None,
    })
}

fn s3_creds_from_env() -> std::io::Result<S3Credentials> {
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| {
        std::io::Error::other(
            "fetch worker: AWS_ACCESS_KEY_ID not set in env (coordinator should have passed it)",
        )
    })?;
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| std::io::Error::other("fetch worker: AWS_SECRET_ACCESS_KEY not set in env"))?;
    let session_token = std::env::var("AWS_SESSION_TOKEN")
        .ok()
        .filter(|s| !s.is_empty());
    Ok(S3Credentials {
        access_key_id,
        secret_access_key,
        session_token,
    })
}
