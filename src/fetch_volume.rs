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

use std::path::Path;
use std::sync::Arc;

use elide_core::volume::ReadonlyVolume;

use crate::VolumeFetchInputs;

/// Run the fetch worker against a fully-set-up by_id directory.
///
/// Returns when `full_warm` has joined every worker thread. Exits the
/// process via the caller; this function returns `io::Result<()>`.
pub fn run(fork_dir: &Path, inputs: VolumeFetchInputs) -> std::io::Result<()> {
    let by_id_dir = fork_dir
        .parent()
        .ok_or_else(|| std::io::Error::other("fork_dir has no parent (expected by_id/<ulid>/)"))?;

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
