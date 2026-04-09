// Deterministic tests for the delta compression pipeline.
//
// Tests exercise the full flow: write data → snapshot → overwrite →
// compute_deltas → rewrite_with_deltas → verify output.

use std::fs;

use elide_coordinator::delta;
use elide_core::segment::{self, EntryKind};
use elide_core::signing;
use elide_core::volume::Volume;

/// Set up a volume with signing keys in a temporary directory.
fn setup_volume(dir: &std::path::Path) -> Volume {
    signing::generate_keypair(dir, signing::VOLUME_KEY_FILE, signing::VOLUME_PUB_FILE).unwrap();
    Volume::open(dir, dir).unwrap()
}

/// Promote all pending segments to index/ + cache/ (simulates drain without S3).
fn drain_pending(fork_dir: &std::path::Path) {
    let pending_dir = fork_dir.join("pending");
    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    fs::create_dir_all(&index_dir).unwrap();
    fs::create_dir_all(&cache_dir).unwrap();
    let Ok(entries) = fs::read_dir(&pending_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let Some(ulid_str) = name.to_str() else {
            continue;
        };
        if ulid_str.ends_with(".tmp") || ulid_str.contains('.') {
            continue;
        }
        segment::extract_idx(&path, &index_dir.join(format!("{ulid_str}.idx"))).unwrap();
        segment::promote_to_cache(
            &path,
            &cache_dir.join(format!("{ulid_str}.body")),
            &cache_dir.join(format!("{ulid_str}.present")),
        )
        .unwrap();
        fs::remove_file(&path).unwrap();
    }
}

#[test]
fn delta_compression_single_block_mutation() {
    // Write a block, snapshot, overwrite with a small mutation,
    // verify delta compression produces a smaller delta blob.
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    let mut vol = setup_volume(fork_dir);

    // Write a 4 KiB block of patterned data at LBA 10.
    let mut original = vec![0xABu8; 4096];
    // Add some structure so zstd has something to work with.
    for (i, byte) in original.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    vol.write(10, &original).unwrap();
    vol.flush_wal().unwrap();

    // Drain to index/ so the snapshot includes this data.
    drain_pending(fork_dir);

    // Reopen volume to pick up the drained segments.
    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();

    // Snapshot — captures LBA 10 → original hash.
    let snap = vol.snapshot().unwrap();
    assert!(
        fork_dir.join("snapshots").join(snap.to_string()).exists(),
        "snapshot marker must exist"
    );

    // Overwrite LBA 10 with a small mutation (change first 16 bytes).
    let mut mutated = original.clone();
    mutated[..16].fill(0xFF);
    vol.write(10, &mutated).unwrap();
    vol.flush_wal().unwrap();

    // The pending segment should have 1 DATA entry.
    let pending_dir = fork_dir.join("pending");
    let pending_files: Vec<_> = fs::read_dir(&pending_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| !s.contains('.'))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        pending_files.len(),
        1,
        "expected exactly one pending segment"
    );
    let segment_path = pending_files[0].path();

    // Build parent state and compute deltas.
    let (parent_lbamap, source_index) = delta::build_parent_state(fork_dir)
        .unwrap()
        .expect("parent state should exist after snapshot");
    assert!(
        parent_lbamap.len() > 0,
        "parent LBA map should not be empty"
    );
    assert!(
        source_index.len() > 0,
        "source extent index should not be empty"
    );

    let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE).unwrap();
    let delta_result =
        delta::compute_deltas(&segment_path, &parent_lbamap, &source_index, fork_dir, &vk)
            .unwrap()
            .expect("delta should be produced for mutated block");

    // Delta should be much smaller than the original 4096 bytes.
    assert!(
        delta_result.delta_body.len() < 4096,
        "delta body {} should be smaller than original 4096",
        delta_result.delta_body.len()
    );
    assert_eq!(delta_result.deltas.len(), 1, "expected one delta entry");

    let (entry_idx, ref opts) = delta_result.deltas[0];
    assert_eq!(entry_idx, 0);
    assert_eq!(opts.len(), 1);
    assert_eq!(opts[0].delta_offset, 0);
    assert_eq!(opts[0].delta_length, delta_result.delta_body.len() as u32);

    // Rewrite the segment with deltas.
    let delta_path = pending_dir.join("test.delta");
    let signer = signing::load_signer(fork_dir, signing::VOLUME_KEY_FILE).unwrap();
    segment::rewrite_with_deltas(
        &segment_path,
        &delta_path,
        &delta_result.deltas,
        &delta_result.delta_body,
        signer.as_ref(),
    )
    .unwrap();

    // Verify the rewritten segment.
    let (bss, entries) = segment::read_and_verify_segment_index(&delta_path, &vk).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Data);
    assert_eq!(entries[0].start_lba, 10);
    assert_eq!(entries[0].lba_length, 1);
    assert_eq!(entries[0].delta_options.len(), 1);
    assert_eq!(entries[0].delta_options[0].source_hash, opts[0].source_hash);
    assert_eq!(
        entries[0].delta_options[0].delta_length,
        delta_result.delta_body.len() as u32
    );

    // Body data should still be readable and match the mutated data.
    let mut read_back = entries;
    segment::read_extent_bodies(&delta_path, bss, &mut read_back, [EntryKind::Data], &[]).unwrap();
    let body = read_back[0]
        .data
        .as_ref()
        .expect("body should be populated");
    // Body is uncompressed (patterned data compresses poorly with lz4).
    assert_eq!(body.as_slice(), &mutated[..]);
}

#[test]
fn delta_skipped_for_new_lba() {
    // Write to a new LBA that has no prior data in the parent snapshot.
    // Delta should not be produced.
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    let mut vol = setup_volume(fork_dir);

    // Write to LBA 10, snapshot.
    let data = vec![0x42u8; 4096];
    vol.write(10, &data).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);
    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    vol.snapshot().unwrap();

    // Write to LBA 20 — new LBA, no parent data.
    vol.write(20, &data).unwrap();
    vol.flush_wal().unwrap();

    let pending_files: Vec<_> = fs::read_dir(fork_dir.join("pending"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| !s.contains('.'))
                .unwrap_or(false)
        })
        .collect();
    let segment_path = pending_files[0].path();

    let (parent_lbamap, source_index) = delta::build_parent_state(fork_dir)
        .unwrap()
        .expect("parent state should exist");

    let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE).unwrap();
    let result =
        delta::compute_deltas(&segment_path, &parent_lbamap, &source_index, fork_dir, &vk).unwrap();
    assert!(result.is_none(), "no delta expected for new LBA");
}

#[test]
fn delta_skipped_for_identical_data() {
    // Write the same data to the same LBA — dedup should handle this,
    // so no delta is produced.
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    let mut vol = setup_volume(fork_dir);

    let data = vec![0x42u8; 4096];
    vol.write(10, &data).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);
    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    vol.snapshot().unwrap();

    // Write identical data to same LBA — volume dedup produces a DedupRef,
    // not a DATA entry.  compute_deltas only considers DATA entries.
    vol.write(10, &data).unwrap();
    vol.flush_wal().unwrap();

    let pending_files: Vec<_> = fs::read_dir(fork_dir.join("pending"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| !s.contains('.'))
                .unwrap_or(false)
        })
        .collect();
    let segment_path = pending_files[0].path();

    let (parent_lbamap, source_index) = delta::build_parent_state(fork_dir)
        .unwrap()
        .expect("parent state should exist");

    let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE).unwrap();
    let result =
        delta::compute_deltas(&segment_path, &parent_lbamap, &source_index, fork_dir, &vk).unwrap();
    assert!(result.is_none(), "no delta for identical data (dedup)");
}

#[test]
fn delta_produced_for_dissimilar_data_is_still_valid() {
    // Even when old and new data are dissimilar, if zstd produces a delta
    // smaller than stored, the delta is valid and the body is preserved.
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    let mut vol = setup_volume(fork_dir);

    // Write patterned data to LBA 10.
    let original: Vec<u8> = (0..4096).map(|i| (i * 37 % 256) as u8).collect();
    vol.write(10, &original).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);
    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    vol.snapshot().unwrap();

    // Write different patterned data.
    let different: Vec<u8> = (0..4096).map(|i| (i * 131 % 256) as u8).collect();
    assert_ne!(original, different);
    vol.write(10, &different).unwrap();
    vol.flush_wal().unwrap();

    let pending_files: Vec<_> = fs::read_dir(fork_dir.join("pending"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| !s.contains('.'))
                .unwrap_or(false)
        })
        .collect();
    let segment_path = pending_files[0].path();

    let (parent_lbamap, source_index) = delta::build_parent_state(fork_dir)
        .unwrap()
        .expect("parent state should exist");

    let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE).unwrap();
    let result =
        delta::compute_deltas(&segment_path, &parent_lbamap, &source_index, fork_dir, &vk).unwrap();

    // zstd may or may not find the delta beneficial. Either outcome is correct:
    // - None: delta was larger than stored → skipped (correct)
    // - Some: delta was smaller → produced (verify it's valid)
    if let Some(ref delta_result) = result {
        assert!(!delta_result.deltas.is_empty());
        assert!(!delta_result.delta_body.is_empty());

        // Verify the delta can be written and read back.
        let delta_path = fork_dir.join("pending").join("test.delta");
        let signer = signing::load_signer(fork_dir, signing::VOLUME_KEY_FILE).unwrap();
        segment::rewrite_with_deltas(
            &segment_path,
            &delta_path,
            &delta_result.deltas,
            &delta_result.delta_body,
            signer.as_ref(),
        )
        .unwrap();

        let (bss, entries) = segment::read_and_verify_segment_index(&delta_path, &vk).unwrap();
        assert!(!entries[0].delta_options.is_empty());

        // Body data must match the new (different) data.
        let mut read_back = entries;
        segment::read_extent_bodies(&delta_path, bss, &mut read_back, [EntryKind::Data], &[])
            .unwrap();
        let body = read_back[0]
            .data
            .as_ref()
            .expect("body should be populated");
        assert_eq!(body.as_slice(), &different[..]);
    }
}

#[test]
fn delta_not_attempted_without_snapshot() {
    // No snapshot → build_parent_state returns None.
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    let mut vol = setup_volume(fork_dir);
    vol.write(10, &[0xABu8; 4096]).unwrap();
    vol.flush_wal().unwrap();
    drop(vol);

    let result = delta::build_parent_state(fork_dir).unwrap();
    assert!(result.is_none(), "no parent state without a snapshot");
}

#[test]
fn try_rewrite_with_deltas_end_to_end() {
    // Full pipeline: try_rewrite_with_deltas on a pending segment.
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    let mut vol = setup_volume(fork_dir);

    // Write patterned data, drain, snapshot.
    let mut original = vec![0u8; 4096];
    for (i, byte) in original.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    vol.write(10, &original).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);
    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    vol.snapshot().unwrap();

    // Mutate and flush.
    let mut mutated = original.clone();
    mutated[..32].fill(0xFF);
    vol.write(10, &mutated).unwrap();
    vol.flush_wal().unwrap();

    let pending_files: Vec<_> = fs::read_dir(fork_dir.join("pending"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| !s.contains('.'))
                .unwrap_or(false)
        })
        .collect();
    let segment_path = pending_files[0].path();
    let delta_path = fork_dir.join("pending").join("test.delta");

    let signer = signing::load_signer(fork_dir, signing::VOLUME_KEY_FILE).unwrap();
    let result =
        delta::try_rewrite_with_deltas(fork_dir, &segment_path, &delta_path, signer.as_ref())
            .unwrap();

    assert!(result.is_some(), "delta rewrite should succeed");
    let delta_file = result.unwrap();
    assert!(delta_file.exists(), "delta file should exist");

    // Verify the delta file is a valid segment with delta options.
    let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE).unwrap();
    let (_, entries) = segment::read_and_verify_segment_index(&delta_file, &vk).unwrap();
    assert_eq!(entries.len(), 1);
    assert!(
        !entries[0].delta_options.is_empty(),
        "should have delta options"
    );

    // Delta file should be larger than original (body + delta body) but the
    // delta_length in header should be small.
    let raw = fs::read(&delta_file).unwrap();
    let delta_length = u32::from_le_bytes([raw[28], raw[29], raw[30], raw[31]]);
    assert!(delta_length > 0, "delta_length should be non-zero");
    assert!(
        delta_length < 4096,
        "delta body {} should be smaller than 4096",
        delta_length
    );
}

#[test]
fn delta_multi_block_fragmented_parent_skipped() {
    // Write two separate single-block entries at adjacent LBAs, snapshot,
    // then overwrite both LBAs with a single 2-block write.
    // Delta should be skipped because the parent has two different hashes.
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    let mut vol = setup_volume(fork_dir);

    // Two separate writes at LBA 10 and LBA 11.
    let block_a: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
    let block_b: Vec<u8> = (0..4096).map(|i| ((i + 128) % 256) as u8).collect();
    vol.write(10, &block_a).unwrap();
    vol.write(11, &block_b).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);
    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    vol.snapshot().unwrap();

    // Overwrite both LBAs with a single 2-block write (8192 bytes).
    let mut two_blocks = vec![0xFFu8; 8192];
    for (i, byte) in two_blocks.iter_mut().enumerate() {
        *byte = (i % 256) as u8 ^ 0xAA;
    }
    vol.write(10, &two_blocks).unwrap();
    vol.flush_wal().unwrap();

    let pending_files: Vec<_> = fs::read_dir(fork_dir.join("pending"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| !s.contains('.'))
                .unwrap_or(false)
        })
        .collect();
    let segment_path = pending_files[0].path();

    let (parent_lbamap, source_index) = delta::build_parent_state(fork_dir)
        .unwrap()
        .expect("parent state should exist");

    let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE).unwrap();
    let result =
        delta::compute_deltas(&segment_path, &parent_lbamap, &source_index, fork_dir, &vk).unwrap();
    assert!(
        result.is_none(),
        "delta should be skipped for multi-block extent with fragmented parent"
    );
}
