// Prefetch segment index files from S3.
#![allow(dead_code)]
//
// Before a forked volume can be opened on a host that has no local segments for
// its ancestors, the index sections (.idx files) must be downloaded so that
// Volume::open can rebuild the LBA map and extent index. Body bytes are not
// fetched; individual extent reads trigger demand-fetch on the first access.
//
// This is the "warm start" step for a fresh host mounting a forked volume.
//
// For each ancestor fork in the rebuild chain:
//   1. List S3 objects under <volume_id>/<fork_name>/
//   2. For each ULID that passes the branch-point cutoff and is not already
//      present in segments/ or fetched/:
//        a. Range-GET [0..96] to parse the header (determines body_section_start)
//        b. Range-GET [0..body_section_start] — header + index + inline
//        c. Write atomically to <ancestor_fork_dir>/fetched/<ulid>.idx
//
// After this runs, Volume::open will find the .idx files, rebuild_segments and
// extentindex::rebuild will both scan fetched/*.idx, and the volume opens
// correctly. Individual reads then demand-fetch the body bytes on first access.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use ulid::Ulid;

use elide_core::volume::walk_ancestors;

use crate::upload::derive_names;

pub struct PrefetchResult {
    pub fetched: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Segment file header constants (matches elide-core/src/segment.rs).
const HEADER_LEN: usize = 96;
const SEGMENT_MAGIC: &[u8; 8] = b"ELIDSEG\x02";

/// Prefetch the index section (`.idx`) for all ancestor segments not present locally.
///
/// Walks the full ancestry chain of `fork_dir`. For each ancestor layer,
/// lists S3 objects and downloads the header+index portion of any segment not
/// already present in `segments/` or `fetched/`.
pub async fn prefetch_indexes(
    fork_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<PrefetchResult> {
    let ancestors = walk_ancestors(fork_dir).context("walking ancestor chain")?;

    let mut fetched = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;

    for layer in &ancestors {
        let (volume_id, fork_name) = derive_names(&layer.dir)
            .with_context(|| format!("resolving names for {}", layer.dir.display()))?;

        println!("scanning {volume_id}/{fork_name}/");

        let prefix = StorePath::from(format!("{volume_id}/{fork_name}/"));
        let objects: Vec<_> = store
            .list(Some(&prefix))
            .try_collect()
            .await
            .with_context(|| format!("listing {volume_id}/{fork_name}"))?;

        for obj in objects {
            let key = &obj.location;

            // The last path component is the ULID.
            let Some(ulid_str) = key.filename() else {
                continue;
            };

            // Validate as ULID (skips sub-prefix entries like fork.pub).
            if Ulid::from_string(ulid_str).is_err() {
                continue;
            }

            // Apply branch-point cutoff: segments written after the snapshot
            // that this fork branched from are not part of this fork's ancestry.
            if layer
                .branch_ulid
                .as_deref()
                .is_some_and(|cutoff| ulid_str > cutoff)
            {
                continue;
            }

            // Skip if already available locally (full segment or index).
            let local_seg = layer.dir.join("segments").join(ulid_str);
            let local_idx = layer.dir.join("fetched").join(format!("{ulid_str}.idx"));
            if local_seg.exists() || local_idx.exists() {
                skipped += 1;
                continue;
            }

            match fetch_idx(store, key, &layer.dir, ulid_str).await {
                Ok(()) => {
                    println!("  fetched {ulid_str}");
                    fetched += 1;
                }
                Err(e) => {
                    eprintln!("  {ulid_str}: {e:#}");
                    failed += 1;
                }
            }
        }
    }

    Ok(PrefetchResult {
        fetched,
        skipped,
        failed,
    })
}

/// Download the index portion of one segment and write it as
/// `<fork_dir>/fetched/<ulid>.idx`.
async fn fetch_idx(
    store: &Arc<dyn ObjectStore>,
    key: &StorePath,
    fork_dir: &Path,
    ulid_str: &str,
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

    // Write atomically: tmp → rename.
    let fetched_dir = fork_dir.join("fetched");
    tokio::fs::create_dir_all(&fetched_dir)
        .await
        .context("creating fetched dir")?;

    let tmp_path = fetched_dir.join(format!("{ulid_str}.idx.tmp"));
    let final_path = fetched_dir.join(format!("{ulid_str}.idx"));

    tokio::fs::write(&tmp_path, &idx_bytes)
        .await
        .context("writing idx.tmp")?;
    tokio::fs::rename(&tmp_path, &final_path)
        .await
        .context("renaming idx.tmp")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as StorePath;
    use std::sync::Arc;
    use tempfile::TempDir;

    use elide_core::segment::{SegmentEntry, write_segment};

    /// Build a test volume with one fork, one segment, snapshot, child fork.
    /// Upload the parent segment to a local store. Verify prefetch_indexes
    /// downloads the .idx file for the parent and not the child.
    #[tokio::test]
    async fn prefetch_indexes_downloads_ancestor_idx() {
        // Volume layout:
        //   vol/
        //     forks/
        //       parent/    ← ancestor fork, has one uploaded segment
        //       child/     ← live fork, no segments
        let vol_tmp = TempDir::new().unwrap();
        let vol_dir = vol_tmp.path().join("myvol");
        let parent_dir = vol_dir.join("forks").join("parent");
        let child_dir = vol_dir.join("forks").join("child");

        std::fs::create_dir_all(parent_dir.join("segments")).unwrap();
        std::fs::create_dir_all(parent_dir.join("snapshots")).unwrap();
        std::fs::create_dir_all(child_dir.join("pending")).unwrap();
        std::fs::create_dir_all(child_dir.join("segments")).unwrap();

        // Write one segment into parent's segments/.
        let data = vec![0xABu8; 4096];
        let hash = blake3::hash(&data);
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mut entries = vec![SegmentEntry::new_data(hash, 0, 1, 0, data)];
        write_segment(&parent_dir.join("segments").join(ulid), &mut entries, None).unwrap();

        // Create a snapshot marker in parent (branch point for child).
        let snap_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        std::fs::write(parent_dir.join("snapshots").join(snap_ulid), "").unwrap();

        // child's origin: forks/parent/snapshots/<snap_ulid>
        std::fs::write(
            child_dir.join("origin"),
            format!("forks/parent/snapshots/{snap_ulid}"),
        )
        .unwrap();

        // Set up a local object store and upload the parent segment.
        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());
        let seg_bytes = std::fs::read(parent_dir.join("segments").join(ulid)).unwrap();
        let key = StorePath::from(format!("myvol/parent/20260101/{ulid}"));
        store.put(&key, seg_bytes.into()).await.unwrap();

        // Delete the local segment to simulate eviction.
        std::fs::remove_file(parent_dir.join("segments").join(ulid)).unwrap();

        // Run prefetch_indexes on the child fork.
        let result = prefetch_indexes(&child_dir, &store).await.unwrap();
        assert_eq!(result.fetched, 1, "should fetch one .idx");
        assert_eq!(result.failed, 0);

        // Verify the .idx file was written into parent's fetched/ dir.
        let idx_path = parent_dir.join("fetched").join(format!("{ulid}.idx"));
        assert!(idx_path.exists(), ".idx file should exist");

        // Verify it is parseable as a segment index.
        let (_, entries) = elide_core::segment::read_segment_index(&idx_path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].hash, hash);

        // Running again should skip (already present).
        let result2 = prefetch_indexes(&child_dir, &store).await.unwrap();
        assert_eq!(result2.fetched, 0);
        assert_eq!(result2.skipped, 1);
    }
}
