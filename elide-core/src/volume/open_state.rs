//! Shared on-disk → in-memory rebuild used by both `Volume::open` and
//! `ReadonlyVolume::open`.
//!
//! Lives outside `readonly.rs` so the timing log it emits doesn't
//! surface under the readonly module path on writable opens.

use std::path::{Path, PathBuf};

use crate::extentindex;
use crate::lbamap;

use super::AncestorLayer;
use super::ancestry::{verify_ancestor_manifests, walk_ancestors, walk_extent_ancestors};

/// Walk the ancestry chain and rebuild the LBA map and extent index.
///
/// This is the common open-time setup shared by `Volume::open` and
/// `ReadonlyVolume::open`. Returns the ancestor layers (oldest-first, fork
/// parents first then extent-index sources deduped by dir), the rebuilt
/// LBA map, and the rebuilt extent index.
///
/// **Ancestor layer semantics have two jobs** and used to conflate them:
///
/// 1. *LBA-map contribution* — which volumes' segments claim LBAs that
///    should be visible in this volume's read view. This is strictly the
///    fork parent chain (`volume.parent`); extent-index sources never
///    contribute LBA claims.
/// 2. *Body lookup search path* — when an extent resolves via the extent
///    index to a canonical segment, where to find that segment's body on
///    disk (and where to route demand-fetches). **This must include
///    extent-index sources**, because a fork's parent may hold DedupRef
///    entries whose canonical bodies live in an extent-index source.
///    Earlier versions of this function only returned fork parents, which
///    caused silent zero-fill on fork reads through DedupRef — see
///    `docs/architecture.md`.
///
/// The rebuilt `LbaMap` is computed from `lba_chain` (fork-only, correct).
/// The returned `ancestor_layers` is the broader set (fork + extent), used
/// downstream by `find_segment_in_dirs`, `open_delta_body_in_dirs`,
/// `prepare_reclaim`, and `RemoteFetcher`'s search list.
pub(super) fn open_read_state(
    fork_dir: &Path,
    by_id_dir: &Path,
) -> std::io::Result<(Vec<AncestorLayer>, lbamap::LbaMap, extentindex::ExtentIndex)> {
    let total_started = std::time::Instant::now();

    // Fail-fast verification: every ancestor in the fork chain must have a
    // signed `.manifest` file whose listed `.idx` files are all present
    // locally. The trust chain is rooted in this volume's own pubkey and
    // walked via the `parent_pubkey` embedded in each child's provenance.
    let verify_started = std::time::Instant::now();
    verify_ancestor_manifests(fork_dir, by_id_dir)?;
    let verify_elapsed = verify_started.elapsed();

    let fork_walk_started = std::time::Instant::now();
    let fork_layers = walk_ancestors(fork_dir, by_id_dir)?;
    let fork_walk_elapsed = fork_walk_started.elapsed();
    let fork_layers_n = fork_layers.len();

    let lba_chain: Vec<(PathBuf, Option<String>)> = fork_layers
        .iter()
        .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
        .chain(std::iter::once((fork_dir.to_owned(), None)))
        .collect();
    let lbamap_started = std::time::Instant::now();
    let lbamap = lbamap::rebuild_segments(&lba_chain)?;
    let lbamap_elapsed = lbamap_started.elapsed();

    // Extent-index sources: recursed across the fork chain by
    // `walk_extent_ancestors`. They contribute canonical hashes to the
    // extent index and must also be searchable for body lookups.
    let extent_walk_started = std::time::Instant::now();
    let extent_sources = walk_extent_ancestors(fork_dir, by_id_dir)?;
    let extent_walk_elapsed = extent_walk_started.elapsed();

    // Build the hash chain for extent-index rebuild: fork chain + extent
    // sources (deduped by dir). `extent_index.lookup` returns canonical
    // locations populated from both.
    let mut hash_chain = lba_chain;
    for layer in &extent_sources {
        if !hash_chain.iter().any(|(dir, _)| dir == &layer.dir) {
            hash_chain.push((layer.dir.clone(), layer.branch_ulid.clone()));
        }
    }
    let hash_chain_len = hash_chain.len();
    let extent_rebuild_started = std::time::Instant::now();
    let extent_index = extentindex::rebuild(&hash_chain)?;
    let extent_rebuild_elapsed = extent_rebuild_started.elapsed();

    // The returned `ancestor_layers` unifies fork parents and extent
    // sources. Callers use this as the body-lookup search path; the
    // LBA-map-only subset was already consumed above.
    let mut ancestor_layers = fork_layers;
    for layer in extent_sources {
        if !ancestor_layers.iter().any(|l| l.dir == layer.dir) {
            ancestor_layers.push(layer);
        }
    }

    log::info!(
        "[open_read_state] {} in {:.2?} (verify_manifests {:.2?}, walk_ancestors {:.2?}, lbamap::rebuild_segments {:.2?}, walk_extent_ancestors {:.2?}, extentindex::rebuild {:.2?}, fork_layers={}, hash_chain={})",
        fork_dir.display(),
        total_started.elapsed(),
        verify_elapsed,
        fork_walk_elapsed,
        lbamap_elapsed,
        extent_walk_elapsed,
        extent_rebuild_elapsed,
        fork_layers_n,
        hash_chain_len,
    );
    Ok((ancestor_layers, lbamap, extent_index))
}
