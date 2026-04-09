// Regression tests for eviction correctness.
//
// `evict` (the `elide volume evict` command) deletes segment body files from
// `cache/` to reclaim local disk space.  The coordinator writes
// `index/<ulid>.idx` after confirmed S3 upload, so evict can safely delete the
// body without any additional work.  After a crash+reopen, `Volume::open`
// rebuilds the LBA map from `pending/` and `index/*.idx`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use elide_core::segment::{self, SegmentFetcher};
use elide_core::volume::Volume;

mod common;

/// After correctly evicting local segment bodies (with coordinator-written
/// `index/*.idx` already present), the LBA map survives a crash+reopen.
///
/// `drain_with_materialise` simulates the coordinator drain: materialise thin
/// refs then promote to `index/<ulid>.idx` + `cache/<ulid>.body`.  Evict then
/// deletes the body.  After a restart, `Volume::open` rebuilds the LBA map
/// from `index/*.idx` and subsequent reads fall through to the `SegmentFetcher`.
#[test]
fn evict_then_crash_data_survives() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0xAB; 4096]).unwrap();
    vol.write(1, &[0xCD; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

    // Simulate evict: delete cache/ body files (index/.idx already present from drain).
    let cache_dir = fork_dir.join("cache");
    for entry in fs::read_dir(&cache_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|e| e == "body") {
            fs::remove_file(entry.path()).unwrap();
        }
    }

    // Crash + reopen (triggers full LBA map rebuild from disk).
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA map is rebuilt from index/*.idx; the data is known to exist but
    // the body is not locally present.  Without a SegmentFetcher configured,
    // the read fails with "segment not found" rather than returning zeros —
    // which is the correct behaviour (fail loudly vs silently corrupt).
    assert_eq!(
        vol.lbamap_len(),
        2,
        "lba map must have 2 entries after evict+crash"
    );
}

/// Without `index/*.idx` files, evicting segment bodies causes silent data
/// loss after a crash: the LBA map has no entries and reads return zeros.
#[test]
fn evict_without_idx_loses_lba_map() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0xAB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

    // Broken evict: delete index/*.idx (coordinator never committed to S3),
    // then delete the body — no trace remains.
    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    for entry in fs::read_dir(&index_dir).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    for entry in fs::read_dir(&cache_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|e| e == "body") {
            fs::remove_file(entry.path()).unwrap();
        }
    }

    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA map is empty — the segment's LBAs are gone without a trace.
    assert_eq!(
        vol.lbamap_len(),
        0,
        "evict without index/*.idx must produce empty lba map"
    );
}

/// Inline extents survive `.body` deletion without a fetcher.
///
/// When all data compresses below `INLINE_THRESHOLD`, every extent is stored in
/// the segment's inline section (carried in `.idx`).  After promote, deleting
/// the `.body` file must not affect reads — the data is served from the
/// in-memory `inline_data` on the `ExtentLocation`.
#[test]
fn inline_extents_survive_body_deletion() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // All-same-byte blocks compress to a few bytes → inline.
    let block_a = vec![0xAAu8; 4096];
    let block_b = vec![0xBBu8; 4096];
    vol.write(0, &block_a).unwrap();
    vol.write(1, &block_b).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

    // Delete every .body file in cache/ — simulates eviction.
    let cache_dir = fork_dir.join("cache");
    for entry in fs::read_dir(&cache_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|e| e == "body") {
            fs::remove_file(entry.path()).unwrap();
        }
    }

    // Reads must still succeed — data comes from inline_data, not the body file.
    // No SegmentFetcher is configured, so any body-path read would fail.
    let got_a = vol.read(0, 1).unwrap();
    let got_b = vol.read(1, 1).unwrap();
    assert_eq!(
        got_a.as_slice(),
        &block_a,
        "LBA 0 must survive body deletion (inline)"
    );
    assert_eq!(
        got_b.as_slice(),
        &block_b,
        "LBA 1 must survive body deletion (inline)"
    );
}

/// Inline extents survive `.body` deletion across crash+reopen.
///
/// Same as `inline_extents_survive_body_deletion` but also verifies the
/// property holds after a full reopen (rebuild from `index/*.idx`).
#[test]
fn inline_extents_survive_body_deletion_after_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);

    {
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        let block = vec![0xCCu8; 4096];
        vol.write(0, &block).unwrap();
        vol.flush_wal().unwrap();
        common::drain_with_materialise(&mut vol);
    }

    // Delete .body files while volume is closed.
    let cache_dir = fork_dir.join("cache");
    for entry in fs::read_dir(&cache_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|e| e == "body") {
            fs::remove_file(entry.path()).unwrap();
        }
    }

    // Reopen — rebuild from index/*.idx (which carries inline section).
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    let got = vol.read(0, 1).unwrap();
    assert_eq!(
        got.as_slice(),
        &vec![0xCCu8; 4096],
        "inline data must survive reopen without body"
    );
}

/// A SegmentFetcher that reads from a local directory (simulates S3).
///
/// For each fetch, reads the full segment from `store_dir/<segment_id>`,
/// extracts the body section, writes it to `body_dir/<segment_id>.body`,
/// and sets the present bit.
struct LocalStoreFetcher {
    store_dir: PathBuf,
}

impl SegmentFetcher for LocalStoreFetcher {
    fn fetch_extent(
        &self,
        segment_id: ulid::Ulid,
        _index_dir: &Path,
        body_dir: &Path,
        extent: &segment::ExtentFetch,
    ) -> io::Result<()> {
        let sid = segment_id.to_string();
        let store_path = self.store_dir.join(&sid);
        let data = fs::read(&store_path)?;
        let bss = extent.body_section_start as usize;
        let body_path = body_dir.join(format!("{sid}.body"));
        if !body_path.exists() {
            fs::write(&body_path, &data[bss..])?;
        }
        segment::set_present_bit(
            &body_dir.join(format!("{sid}.present")),
            extent.entry_idx,
            1, // at least 1 entry
        )?;
        Ok(())
    }
}

/// After promote (drain path), the in-memory extent index must transition
/// entries from `BodySource::Local` to `BodySource::Cached`.  Without this,
/// evicting the cache body bypasses demand-fetch and reads fail with
/// "segment not found".
///
/// Regression test for: promote_segment did not update the extent index's
/// BodySource, so after evict the demand-fetch guard `BodySource::Cached(idx)`
/// never matched and the fetcher was never called.
#[test]
fn evict_live_volume_reads_via_demand_fetch() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);

    // Set up a local "store" directory and a fetcher.
    let store_dir = fork_dir.join("test_store");
    fs::create_dir_all(&store_dir).unwrap();

    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    let block_a = vec![0xABu8; 4096];
    let block_b = vec![0xCDu8; 4096];
    vol.write(0, &block_a).unwrap();
    vol.write(1, &block_b).unwrap();
    vol.flush_wal().unwrap();

    // Copy pending segments to the store before promote deletes them.
    let pending_dir = fork_dir.join("pending");
    for entry in fs::read_dir(&pending_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name.to_string_lossy().contains('.') {
            continue;
        }
        fs::copy(entry.path(), store_dir.join(&name)).unwrap();
    }

    // Drain: materialise + promote (moves pending → cache + index).
    common::drain_with_materialise(&mut vol);

    // Verify extent index entries are now BodySource::Cached, not Local.
    let hash_a = blake3::hash(&block_a);
    let hash_b = blake3::hash(&block_b);
    let (_, extent_index) = vol.snapshot_maps();
    let loc_a = extent_index
        .lookup(&hash_a)
        .expect("hash_a must be in extent index");
    let loc_b = extent_index
        .lookup(&hash_b)
        .expect("hash_b must be in extent index");
    assert!(
        matches!(
            loc_a.body_source,
            elide_core::extentindex::BodySource::Cached(_)
        ),
        "after promote, body_source must be Cached, got {:?}",
        loc_a.body_source
    );
    assert!(
        matches!(
            loc_b.body_source,
            elide_core::extentindex::BodySource::Cached(_)
        ),
        "after promote, body_source must be Cached, got {:?}",
        loc_b.body_source
    );

    // Attach fetcher, then evict cache bodies.
    vol.set_fetcher(Arc::new(LocalStoreFetcher {
        store_dir: store_dir.clone(),
    }));

    let cache_dir = fork_dir.join("cache");
    for entry in fs::read_dir(&cache_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let ext = path.extension().and_then(|e| e.to_str());
        if ext == Some("body") || ext == Some("present") {
            fs::remove_file(&path).unwrap();
        }
    }

    // Reads must succeed via demand-fetch — not fail with "segment not found".
    let got_a = vol.read(0, 1).unwrap();
    let got_b = vol.read(1, 1).unwrap();
    assert_eq!(
        got_a.as_slice(),
        &block_a,
        "LBA 0 must be readable after evict"
    );
    assert_eq!(
        got_b.as_slice(),
        &block_b,
        "LBA 1 must be readable after evict"
    );
}
