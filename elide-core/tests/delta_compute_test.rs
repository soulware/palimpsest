// Integration test for file-aware delta computation (Phase B2).
//
// Sets up a two-volume layout under a temp `by_id/` directory:
//   * source volume with one DATA entry for "/shared.bin" and a matching
//     filemap v2 entry.
//   * child volume whose provenance lists the source in its
//     `extent_index`, plus one DATA entry for the same path with
//     different (but delta-compressible) bytes.
//
// Runs `rewrite_pending_with_deltas` on the child and asserts that the
// matching DATA entry was converted to a thin Delta entry with a
// `source_hash` pointing at the parent's content hash.

use std::fs;
use std::path::{Path, PathBuf};

use elide_core::delta_compute;
use elide_core::filemap::{self, FilemapRow};
use elide_core::segment::{
    self, EntryKind, SegmentEntry, SegmentFlags, SegmentSigner, write_segment,
};
use elide_core::signing::{
    ProvenanceLineage, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE, setup_readonly_identity,
};
use tempfile::TempDir;
use ulid::Ulid;

fn make_readonly_volume(
    by_id_dir: &Path,
    lineage: &ProvenanceLineage,
) -> (Ulid, PathBuf, std::sync::Arc<dyn SegmentSigner>) {
    let vol_ulid = Ulid::new();
    let vol_dir = by_id_dir.join(vol_ulid.to_string());
    fs::create_dir_all(vol_dir.join("pending")).unwrap();
    fs::create_dir_all(vol_dir.join("snapshots")).unwrap();
    fs::write(vol_dir.join("volume.readonly"), "").unwrap();
    elide_core::config::VolumeConfig {
        name: Some(vol_ulid.to_string()),
        size: Some(1024 * 1024),
        nbd: None,
    }
    .write(&vol_dir)
    .unwrap();
    let signer =
        setup_readonly_identity(&vol_dir, VOLUME_PUB_FILE, VOLUME_PROVENANCE_FILE, lineage)
            .unwrap();
    (vol_ulid, vol_dir, signer)
}

fn write_single_entry_segment(
    vol_dir: &Path,
    signer: &dyn SegmentSigner,
    hash: blake3::Hash,
    start_lba: u64,
    lba_length: u32,
    body: Vec<u8>,
) -> Ulid {
    let seg_ulid = Ulid::new();
    let seg_path = vol_dir.join("pending").join(seg_ulid.to_string());
    let mut entries = vec![SegmentEntry::new_data(
        hash,
        start_lba,
        lba_length,
        SegmentFlags::empty(),
        body,
    )];
    write_segment(&seg_path, &mut entries, signer).unwrap();
    seg_ulid
}

fn write_snapshot_and_filemap(
    vol_dir: &Path,
    snap_ulid: Ulid,
    path: &str,
    hash: blake3::Hash,
    byte_count: u64,
) {
    let snap_str = snap_ulid.to_string();
    fs::write(vol_dir.join("snapshots").join(&snap_str), "").unwrap();
    let rows = vec![FilemapRow {
        path: path.to_owned(),
        file_offset: 0,
        hash,
        byte_count,
    }];
    filemap::write(&vol_dir.join("snapshots"), &snap_str, &rows).unwrap();
}

#[test]
fn rewrite_pending_with_deltas_converts_matching_entry() {
    let tmp = TempDir::new().unwrap();
    let by_id_dir = tmp.path().join("by_id");
    fs::create_dir_all(&by_id_dir).unwrap();

    // ── Source volume: one 4 KiB DATA entry at LBA 0 for "/shared.bin".
    let parent_bytes = vec![0x55u8; 4096];
    let parent_hash = blake3::hash(&parent_bytes);
    let (source_ulid, source_dir, source_signer) =
        make_readonly_volume(&by_id_dir, &ProvenanceLineage::default());
    let source_seg_ulid = write_single_entry_segment(
        &source_dir,
        source_signer.as_ref(),
        parent_hash,
        0,
        1,
        parent_bytes.clone(),
    );
    write_snapshot_and_filemap(
        &source_dir,
        source_seg_ulid,
        "/shared.bin",
        parent_hash,
        parent_bytes.len() as u64,
    );

    // ── Child volume: same path, different bytes (but delta-compressible).
    // Provenance names the source as an extent-index lineage entry.
    let mut child_bytes = vec![0x55u8; 4096];
    for (i, byte) in child_bytes.iter_mut().enumerate().take(128) {
        *byte = i as u8;
    }
    let child_hash = blake3::hash(&child_bytes);

    let child_lineage = ProvenanceLineage {
        parent: None,
        extent_index: vec![format!("{source_ulid}/snapshots/{source_seg_ulid}")],
    };
    let (_child_ulid, child_dir, child_signer) = make_readonly_volume(&by_id_dir, &child_lineage);
    let child_seg_ulid = write_single_entry_segment(
        &child_dir,
        child_signer.as_ref(),
        child_hash,
        0,
        1,
        child_bytes.clone(),
    );
    write_snapshot_and_filemap(
        &child_dir,
        child_seg_ulid,
        "/shared.bin",
        child_hash,
        child_bytes.len() as u64,
    );

    // ── Run the delta stage on the child.
    let stats =
        delta_compute::rewrite_pending_with_deltas(&child_dir, &by_id_dir, child_signer.as_ref())
            .expect("rewrite_pending_with_deltas");

    assert_eq!(stats.entries_converted, 1, "one entry should be converted");
    assert_eq!(stats.segments_rewritten, 1);
    assert!(stats.delta_body_bytes > 0);
    assert!(stats.delta_body_bytes < stats.original_body_bytes);

    // ── Re-read the rewritten pending segment and assert the entry is
    // now a thin Delta with source_hash = parent_hash.
    let rewritten = child_dir.join("pending").join(child_seg_ulid.to_string());
    let (_, entries) = segment::read_segment_index(&rewritten).unwrap();
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.kind, EntryKind::Delta);
    assert_eq!(e.hash, child_hash);
    assert_eq!(e.start_lba, 0);
    assert_eq!(e.lba_length, 1);
    assert_eq!(e.stored_offset, 0);
    assert_eq!(e.stored_length, 0);
    assert_eq!(e.delta_options.len(), 1);
    assert_eq!(e.delta_options[0].source_hash, parent_hash);
}

#[test]
fn rewrite_pending_with_deltas_skips_unchanged_hashes() {
    // Unchanged fragment (same hash on both sides) should not be
    // converted — dedup handles it via DedupRef elsewhere, and the
    // delta stage must not produce a self-delta.
    let tmp = TempDir::new().unwrap();
    let by_id_dir = tmp.path().join("by_id");
    fs::create_dir_all(&by_id_dir).unwrap();

    let bytes = vec![0xA5u8; 4096];
    let hash = blake3::hash(&bytes);

    let (source_ulid, source_dir, source_signer) =
        make_readonly_volume(&by_id_dir, &ProvenanceLineage::default());
    let source_seg_ulid = write_single_entry_segment(
        &source_dir,
        source_signer.as_ref(),
        hash,
        0,
        1,
        bytes.clone(),
    );
    write_snapshot_and_filemap(
        &source_dir,
        source_seg_ulid,
        "/same.bin",
        hash,
        bytes.len() as u64,
    );

    let child_lineage = ProvenanceLineage {
        parent: None,
        extent_index: vec![format!("{source_ulid}/snapshots/{source_seg_ulid}")],
    };
    let (_child_ulid, child_dir, child_signer) = make_readonly_volume(&by_id_dir, &child_lineage);
    let child_seg_ulid =
        write_single_entry_segment(&child_dir, child_signer.as_ref(), hash, 0, 1, bytes.clone());
    write_snapshot_and_filemap(
        &child_dir,
        child_seg_ulid,
        "/same.bin",
        hash,
        bytes.len() as u64,
    );

    let stats =
        delta_compute::rewrite_pending_with_deltas(&child_dir, &by_id_dir, child_signer.as_ref())
            .unwrap();
    assert_eq!(stats.entries_converted, 0);

    let rewritten = child_dir.join("pending").join(child_seg_ulid.to_string());
    let (_, entries) = segment::read_segment_index(&rewritten).unwrap();
    assert_eq!(
        entries[0].kind,
        EntryKind::Data,
        "unchanged fragment stays DATA"
    );
}
