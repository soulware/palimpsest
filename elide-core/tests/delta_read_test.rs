// Integration test for the thin Delta entry read path (Phase C).
//
// Hand-crafts two segments:
//   1. A source segment with one DATA entry holding the "parent" bytes.
//   2. A Delta segment with one Delta entry whose source_hash points at
//      the parent DATA, plus a zstd-dict-compressed delta blob in the
//      segment's delta body section.
//
// Opens the volume, reads the LBA covered by the Delta entry, and
// verifies that the returned bytes equal the "child" bytes that went
// through the zstd-dict compression step.
//
// No producer exists yet — this test builds segments via low-level
// primitives. It verifies the format, the extent-index registration,
// and the volume read path decompression all work end-to-end.

use std::fs;
use std::sync::Arc;

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use elide_core::block_reader::BlockReader;
use elide_core::config::VolumeConfig;
use elide_core::segment::{
    DeltaOption, ExtentFetch, SegmentEntry, SegmentFetcher, SegmentFlags, SegmentSigner,
    extract_idx, promote_to_cache, write_segment, write_segment_with_delta_body,
};
use elide_core::signing;
use elide_core::ulid_mint::UlidMint;
use elide_core::volume::ReadonlyVolume;
use tempfile::TempDir;
use ulid::Ulid;

mod common;

/// Create a new volume directory with a keypair and an empty volume.toml,
/// returning the dir path and the signer. The volume is not yet opened.
fn setup_volume_dir(tmp: &TempDir) -> (std::path::PathBuf, Arc<dyn SegmentSigner>) {
    let vol_dir = tmp.path().join("vol");
    fs::create_dir_all(&vol_dir).unwrap();
    signing::generate_keypair(&vol_dir, signing::VOLUME_KEY_FILE, signing::VOLUME_PUB_FILE)
        .unwrap();
    fs::create_dir_all(vol_dir.join("pending")).unwrap();
    fs::create_dir_all(vol_dir.join("snapshots")).unwrap();
    let signer = signing::load_signer(&vol_dir, signing::VOLUME_KEY_FILE).unwrap();
    VolumeConfig {
        name: Some("test".to_owned()),
        size: Some(1024 * 1024),
        nbd: None,
    }
    .write(&vol_dir)
    .unwrap();
    (vol_dir, signer)
}

#[test]
fn delta_entry_end_to_end_decompression() {
    let tmp = TempDir::new().unwrap();
    let (vol_dir, signer) = setup_volume_dir(&tmp);

    // Parent file content — a whole 4 KiB block, lz4-compressible.
    let parent_bytes = vec![0x55u8; 4096];
    let parent_hash = blake3::hash(&parent_bytes);

    // Child file content — different from parent but structurally
    // similar so the zstd-dict delta is small.
    let mut child_bytes = vec![0x55u8; 4096];
    for (i, byte) in child_bytes.iter_mut().enumerate().take(256) {
        *byte = i as u8;
    }
    let child_hash = blake3::hash(&child_bytes);

    // Compute the delta blob using zstd with parent as dictionary.
    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes).unwrap();
    let delta_blob = compressor.compress(&child_bytes).unwrap();
    assert!(!delta_blob.is_empty());

    // --- Segment 1: holds the parent DATA entry at LBA 0. ---
    let mut mint = UlidMint::new(Ulid::nil());
    let parent_seg_ulid = mint.next();
    let parent_seg_path = vol_dir.join(format!("pending/{parent_seg_ulid}"));
    let mut parent_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        1,
        SegmentFlags::empty(),
        parent_bytes.clone(),
    )];
    write_segment(&parent_seg_path, &mut parent_entries, signer.as_ref()).unwrap();

    // --- Segment 2: holds the Delta entry at LBA 10. ---
    // Its ULID must be greater than the parent segment's so the LBA map
    // rebuild applies it after the parent (the parent contributes nothing
    // to LBA 10 anyway, but monotonic ULIDs are required for rebuild
    // ordering to be safe).
    let delta_seg_ulid = mint.next();
    let delta_seg_path = vol_dir.join(format!("pending/{delta_seg_ulid}"));
    let delta_option = DeltaOption {
        source_hash: parent_hash,
        delta_offset: 0,
        delta_length: delta_blob.len() as u32,
        delta_hash: blake3::hash(&delta_blob),
    };
    let mut delta_entries = vec![SegmentEntry::new_delta(
        child_hash,
        10,
        1,
        vec![delta_option],
    )];
    write_segment_with_delta_body(
        &delta_seg_path,
        &mut delta_entries,
        &delta_blob,
        signer.as_ref(),
    )
    .unwrap();

    // Write a snapshot marker so the volume has a floor.
    fs::write(vol_dir.join(format!("snapshots/{delta_seg_ulid}")), "").unwrap();

    // --- Open the volume and read the Delta LBA. ---
    let vol = ReadonlyVolume::open(&vol_dir, &vol_dir).unwrap();
    let bytes = vol.read(10, 1).unwrap();
    assert_eq!(
        bytes.len(),
        4096,
        "read should return one 4 KiB block for the Delta LBA"
    );
    assert_eq!(
        bytes, child_bytes,
        "delta-decompressed bytes must equal the original child content"
    );

    // Also: reading the parent LBA should return the parent bytes
    // (sanity check that the normal DATA path still works).
    let parent_read = vol.read(0, 1).unwrap();
    assert_eq!(parent_read, parent_bytes);
}

/// Drained-cache regression for Phase B2/C: after both segments
/// are promoted into the `index/` + `cache/` three-file shape and
/// `pending/` is cleared, the Delta LBA must still round-trip via
/// the cache/<id>.body file's appended delta body section.
#[test]
fn delta_entry_roundtrip_from_drained_cache() {
    let tmp = TempDir::new().unwrap();
    let (vol_dir, signer) = setup_volume_dir(&tmp);
    fs::create_dir_all(vol_dir.join("index")).unwrap();

    let parent_bytes = vec![0x55u8; 4096];
    let parent_hash = blake3::hash(&parent_bytes);

    let mut child_bytes = vec![0x55u8; 4096];
    for (i, byte) in child_bytes.iter_mut().enumerate().take(256) {
        *byte = i as u8;
    }
    let child_hash = blake3::hash(&child_bytes);

    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes).unwrap();
    let delta_blob = compressor.compress(&child_bytes).unwrap();

    // ── Parent segment: one DATA entry with the parent bytes.
    let mut mint = UlidMint::new(Ulid::nil());
    let parent_seg_ulid = mint.next();
    let parent_seg_path = vol_dir.join(format!("pending/{parent_seg_ulid}"));
    let mut parent_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        1,
        SegmentFlags::empty(),
        parent_bytes.clone(),
    )];
    write_segment(&parent_seg_path, &mut parent_entries, signer.as_ref()).unwrap();

    // ── Delta segment: one Delta entry pointing at the parent hash.
    let delta_seg_ulid = mint.next();
    let delta_seg_path = vol_dir.join(format!("pending/{delta_seg_ulid}"));
    let delta_option = DeltaOption {
        source_hash: parent_hash,
        delta_offset: 0,
        delta_length: delta_blob.len() as u32,
        delta_hash: blake3::hash(&delta_blob),
    };
    let mut delta_entries = vec![SegmentEntry::new_delta(
        child_hash,
        10,
        1,
        vec![delta_option],
    )];
    write_segment_with_delta_body(
        &delta_seg_path,
        &mut delta_entries,
        &delta_blob,
        signer.as_ref(),
    )
    .unwrap();

    // ── Drain both segments into the `index/` + `cache/` shape,
    // mirroring what the coordinator does post-upload.
    for ulid in [parent_seg_ulid, delta_seg_ulid] {
        let pending = vol_dir.join(format!("pending/{ulid}"));
        let idx = vol_dir.join(format!("index/{ulid}.idx"));
        let body = vol_dir.join(format!("cache/{ulid}.body"));
        let present = vol_dir.join(format!("cache/{ulid}.present"));
        fs::create_dir_all(vol_dir.join("cache")).unwrap();
        extract_idx(&pending, &idx).unwrap();
        promote_to_cache(&pending, &body, &present).unwrap();
        fs::remove_file(&pending).unwrap();
    }

    // Post-promote the delta segment has a separate `.delta` file
    // sized exactly `delta_length`, sitting alongside an empty `.body`
    // (body_length == 0 for this fixture — the parent segment holds
    // the source extent). Reader opens `.delta` directly for cached
    // Delta entries.
    let delta_file = vol_dir.join(format!("cache/{delta_seg_ulid}.delta"));
    let sz = fs::metadata(&delta_file).unwrap().len();
    assert_eq!(
        sz,
        delta_blob.len() as u64,
        "cache/<id>.delta must hold exactly the delta body section"
    );

    // Snapshot marker on the post-drain volume.
    fs::write(vol_dir.join(format!("snapshots/{delta_seg_ulid}")), "").unwrap();

    // ── Read the Delta LBA. Extent-index rebuild must register the
    // Delta entry from the cached path, and the reader must resolve
    // through the `.body` file's delta body region.
    let vol = ReadonlyVolume::open(&vol_dir, &vol_dir).unwrap();
    let bytes = vol.read(10, 1).unwrap();
    assert_eq!(bytes, child_bytes, "post-drain delta read must round-trip");

    // Parent LBA still round-trips too.
    let parent_read = vol.read(0, 1).unwrap();
    assert_eq!(parent_read, parent_bytes);
}

/// Pull-host demand-fetch regression: a post-promote volume where
/// `cache/<id>.delta` is absent (simulating a host that received the
/// segment via S3 rather than running the import itself).  The
/// volume's attached `SegmentFetcher` is consulted on the first Delta
/// LBA read, materialises `.delta` in place, and the read returns the
/// expected child bytes.  A second read for the same LBA must resolve
/// locally without re-invoking the fetcher.
#[test]
fn delta_entry_demand_fetch_from_pull_host() {
    let tmp = TempDir::new().unwrap();
    let (vol_dir, signer) = setup_volume_dir(&tmp);
    fs::create_dir_all(vol_dir.join("index")).unwrap();

    let parent_bytes = vec![0x55u8; 4096];
    let parent_hash = blake3::hash(&parent_bytes);

    let mut child_bytes = vec![0x55u8; 4096];
    for (i, byte) in child_bytes.iter_mut().enumerate().take(256) {
        *byte = i as u8;
    }
    let child_hash = blake3::hash(&child_bytes);

    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes).unwrap();
    let delta_blob = compressor.compress(&child_bytes).unwrap();

    let mut mint = UlidMint::new(Ulid::nil());
    let parent_seg_ulid = mint.next();
    let parent_seg_path = vol_dir.join(format!("pending/{parent_seg_ulid}"));
    let mut parent_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        1,
        SegmentFlags::empty(),
        parent_bytes.clone(),
    )];
    write_segment(&parent_seg_path, &mut parent_entries, signer.as_ref()).unwrap();

    let delta_seg_ulid = mint.next();
    let delta_seg_path = vol_dir.join(format!("pending/{delta_seg_ulid}"));
    let delta_option = DeltaOption {
        source_hash: parent_hash,
        delta_offset: 0,
        delta_length: delta_blob.len() as u32,
        delta_hash: blake3::hash(&delta_blob),
    };
    let mut delta_entries = vec![SegmentEntry::new_delta(
        child_hash,
        10,
        1,
        vec![delta_option],
    )];
    write_segment_with_delta_body(
        &delta_seg_path,
        &mut delta_entries,
        &delta_blob,
        signer.as_ref(),
    )
    .unwrap();

    // Promote both segments as the import host would, then delete
    // cache/<delta>.delta so the volume looks like a pull host that
    // hasn't yet materialised the delta body region.
    for ulid in [parent_seg_ulid, delta_seg_ulid] {
        let pending = vol_dir.join(format!("pending/{ulid}"));
        let idx = vol_dir.join(format!("index/{ulid}.idx"));
        let body = vol_dir.join(format!("cache/{ulid}.body"));
        let present = vol_dir.join(format!("cache/{ulid}.present"));
        fs::create_dir_all(vol_dir.join("cache")).unwrap();
        extract_idx(&pending, &idx).unwrap();
        promote_to_cache(&pending, &body, &present).unwrap();
        fs::remove_file(&pending).unwrap();
    }
    let delta_file = vol_dir.join(format!("cache/{delta_seg_ulid}.delta"));
    assert!(delta_file.exists(), "promote should have created .delta");

    // Stash the delta bytes the test fetcher will hand back, then
    // delete the file to simulate a pull host with no local copy.
    let staged_delta_bytes = fs::read(&delta_file).unwrap();
    fs::remove_file(&delta_file).unwrap();

    fs::write(vol_dir.join(format!("snapshots/{delta_seg_ulid}")), "").unwrap();

    // --- Fake fetcher: only fetch_delta_body is exercised. Writes the
    // staged bytes atomically (tmp+rename) into body_dir/<id>.delta
    // and counts invocations so the test can verify caching. ---
    struct StagedDeltaFetcher {
        segment_id: Ulid,
        bytes: Vec<u8>,
        calls: AtomicUsize,
    }
    impl SegmentFetcher for StagedDeltaFetcher {
        fn fetch_extent(
            &self,
            _segment_id: Ulid,
            _index_dir: &Path,
            _body_dir: &Path,
            _extent: &ExtentFetch,
        ) -> io::Result<()> {
            Err(io::Error::other("unused in this test"))
        }
        fn fetch_delta_body(
            &self,
            segment_id: Ulid,
            _index_dir: &Path,
            body_dir: &Path,
        ) -> io::Result<()> {
            assert_eq!(segment_id, self.segment_id);
            self.calls.fetch_add(1, Ordering::SeqCst);
            let out = body_dir.join(format!("{segment_id}.delta"));
            let tmp = body_dir.join(format!("{segment_id}.delta.tmp"));
            fs::write(&tmp, &self.bytes)?;
            fs::rename(&tmp, &out)?;
            Ok(())
        }
    }
    let fetcher = Arc::new(StagedDeltaFetcher {
        segment_id: delta_seg_ulid,
        bytes: staged_delta_bytes,
        calls: AtomicUsize::new(0),
    });
    let calls = Arc::clone(&fetcher);

    let mut vol = ReadonlyVolume::open(&vol_dir, &vol_dir).unwrap();
    vol.set_fetcher(fetcher);

    // First read triggers the fetcher and rehydrates .delta.
    let bytes = vol.read(10, 1).unwrap();
    assert_eq!(bytes, child_bytes, "pull-host delta read must round-trip");
    assert_eq!(
        calls.calls.load(Ordering::SeqCst),
        1,
        "first read should invoke fetch_delta_body exactly once"
    );
    assert!(delta_file.exists(), ".delta must exist post-fetch");

    // Second read hits the cached path — fetcher must not be invoked again.
    let bytes2 = vol.read(10, 1).unwrap();
    assert_eq!(bytes2, child_bytes);
    assert_eq!(
        calls.calls.load(Ordering::SeqCst),
        1,
        "second read must not re-invoke the fetcher"
    );
}

/// Regression for the `BlockReader::read_block` delta dispatch bug
/// (April 2026). `BlockReader` is the read path used by `volume ls`,
/// `volume inspect`, and Phase 4 snapshot filemap generation — it is
/// *separate* from `ReadonlyVolume::read` (covered by the tests above).
///
/// The original bug: `read_block` looked the lbamap winner up only in
/// `extent_index.inner` (data/inline), missed delta-only hashes, and
/// silently returned 4 KiB of zeros. The concrete symptom was a
/// post-snapshot delta at ext4 group 0's inode-table block (LBA 145):
/// the root inode disappeared, the ext4 scan silently walked nothing,
/// and Phase 4 wrote a header-only filemap.
///
/// This test builds that exact shape — parent DATA at LBA 0, Delta
/// entry at LBA 10 — and asserts via `BlockReader::open_live` that
/// the delta LBA decompresses back to the child bytes, the data LBA
/// still round-trips, and an unmapped LBA still reads as zeros.
#[test]
fn block_reader_read_block_dispatches_to_delta() {
    let tmp = TempDir::new().unwrap();
    let (vol_dir, signer) = setup_volume_dir(&tmp);

    let parent_bytes = vec![0x55u8; 4096];
    let parent_hash = blake3::hash(&parent_bytes);

    let mut child_bytes = vec![0x55u8; 4096];
    for (i, byte) in child_bytes.iter_mut().enumerate().take(256) {
        *byte = i as u8;
    }
    let child_hash = blake3::hash(&child_bytes);

    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes).unwrap();
    let delta_blob = compressor.compress(&child_bytes).unwrap();

    // Parent DATA segment at LBA 0.
    let mut mint = UlidMint::new(Ulid::nil());
    let parent_seg_ulid = mint.next();
    let parent_seg_path = vol_dir.join(format!("pending/{parent_seg_ulid}"));
    let mut parent_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        1,
        SegmentFlags::empty(),
        parent_bytes.clone(),
    )];
    write_segment(&parent_seg_path, &mut parent_entries, signer.as_ref()).unwrap();

    // Delta segment with a Delta entry at LBA 10, source = parent_hash.
    let delta_seg_ulid = mint.next();
    let delta_seg_path = vol_dir.join(format!("pending/{delta_seg_ulid}"));
    let delta_option = DeltaOption {
        source_hash: parent_hash,
        delta_offset: 0,
        delta_length: delta_blob.len() as u32,
        delta_hash: blake3::hash(&delta_blob),
    };
    let mut delta_entries = vec![SegmentEntry::new_delta(
        child_hash,
        10,
        1,
        vec![delta_option],
    )];
    write_segment_with_delta_body(
        &delta_seg_path,
        &mut delta_entries,
        &delta_blob,
        signer.as_ref(),
    )
    .unwrap();

    fs::write(vol_dir.join(format!("snapshots/{delta_seg_ulid}")), "").unwrap();

    let reader = BlockReader::open_live(&vol_dir, Box::new(|_| None)).unwrap();

    // Delta dispatch: the lbamap winner for LBA 10 is `child_hash`, which
    // only lives in `extent_index.deltas`. Pre-fix this silently returned
    // zeros.
    let delta_block = reader.read_block(10).unwrap();
    assert_eq!(
        &delta_block[..],
        &child_bytes[..],
        "read_block must decompress the delta entry, not return zeros"
    );

    // Data dispatch: ensure the data path still works.
    let data_block = reader.read_block(0).unwrap();
    assert_eq!(
        &data_block[..],
        &parent_bytes[..],
        "read_block must still resolve DATA entries"
    );

    // Unmapped LBA: still reads as zeros.
    let zero_block = reader.read_block(999).unwrap();
    assert!(
        zero_block.iter().all(|&b| b == 0),
        "unmapped LBA must read as zeros"
    );
}
