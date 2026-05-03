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
    self, EntryKind, SegmentEntry, SegmentFlags, SegmentSigner, extract_idx, promote_to_cache,
    write_segment,
};
use elide_core::signing::{
    ProvenanceLineage, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE, setup_readonly_identity,
};
use lz4_flex::compress_prepend_size;
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
        ..Default::default()
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
    signer: &dyn elide_core::segment::SegmentSigner,
    snap_ulid: Ulid,
    path: &str,
    hash: blake3::Hash,
    byte_count: u64,
) {
    let snap_str = snap_ulid.to_string();
    elide_core::signing::write_snapshot_manifest(vol_dir, signer, &snap_ulid, &[], None).unwrap();
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
        source_signer.as_ref(),
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
        extent_index: vec![format!("{source_ulid}/{source_seg_ulid}")],
        oci_source: None,
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
        child_signer.as_ref(),
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
    let (_, entries, _) = segment::read_segment_index(&rewritten).unwrap();
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

/// Promote a pending source segment into the drained layout:
/// index/<ulid>.idx + cache/<ulid>.{body,present}, deleting pending/<ulid>.
/// Mirrors what the coordinator does after a successful S3 upload.
fn drain_source_segment(source_dir: &Path, seg_ulid: Ulid) {
    let pending = source_dir.join("pending").join(seg_ulid.to_string());
    let index_dir = source_dir.join("index");
    let cache_dir = source_dir.join("cache");
    fs::create_dir_all(&index_dir).unwrap();
    fs::create_dir_all(&cache_dir).unwrap();
    let idx = index_dir.join(format!("{seg_ulid}.idx"));
    let body = cache_dir.join(format!("{seg_ulid}.body"));
    let present = cache_dir.join(format!("{seg_ulid}.present"));
    extract_idx(&pending, &idx).unwrap();
    promote_to_cache(&pending, &body, &present).unwrap();
    fs::remove_file(&pending).unwrap();
}

#[test]
fn rewrite_pending_with_deltas_reads_drained_source_body() {
    // Regression: a drained source volume serves extents from
    // cache/<id>.body with body_offset used as the absolute file seek.
    // ExtentLocation.body_section_start reflects the full segment's
    // header+index+inline prefix and must NOT be added when reading a
    // `.body` file. Prior to the fix this produced a corrupted read
    // that lz4_decompress surfaced as "offset to copy is not contained
    // in the decompressed buffer".
    let tmp = TempDir::new().unwrap();
    let by_id_dir = tmp.path().join("by_id");
    fs::create_dir_all(&by_id_dir).unwrap();

    // A real file fragment: 8 KiB of semi-structured bytes so lz4
    // compression produces a body entry (not inline) and the resulting
    // seek arithmetic is exercised.
    let mut parent_bytes = Vec::with_capacity(8192);
    for i in 0..8192 {
        parent_bytes.push((i % 251) as u8);
    }
    let parent_hash = blake3::hash(&parent_bytes);
    let parent_stored = compress_prepend_size(&parent_bytes);
    assert!(parent_stored.len() >= 256, "must land in body section");

    let (source_ulid, source_dir, source_signer) =
        make_readonly_volume(&by_id_dir, &ProvenanceLineage::default());
    let source_seg_ulid = Ulid::new();
    let source_seg_path = source_dir.join("pending").join(source_seg_ulid.to_string());
    let mut source_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        2,
        SegmentFlags::COMPRESSED,
        parent_stored,
    )];
    assert_eq!(source_entries[0].kind, EntryKind::Data);
    write_segment(
        &source_seg_path,
        &mut source_entries,
        source_signer.as_ref(),
    )
    .unwrap();
    write_snapshot_and_filemap(
        &source_dir,
        source_signer.as_ref(),
        source_seg_ulid,
        "/blob",
        parent_hash,
        parent_bytes.len() as u64,
    );
    // Simulate the coordinator drain: move pending → index/.idx + cache/.body.
    drain_source_segment(&source_dir, source_seg_ulid);

    // Child: same path, small twist in the bytes.
    let mut child_bytes = parent_bytes.clone();
    for byte in child_bytes.iter_mut().take(512) {
        *byte ^= 0x55;
    }
    let child_hash = blake3::hash(&child_bytes);

    let child_lineage = ProvenanceLineage {
        parent: None,
        extent_index: vec![format!("{source_ulid}/{source_seg_ulid}")],
        oci_source: None,
    };
    let (_child_ulid, child_dir, child_signer) = make_readonly_volume(&by_id_dir, &child_lineage);
    let child_seg_ulid = write_single_entry_segment(
        &child_dir,
        child_signer.as_ref(),
        child_hash,
        0,
        2,
        child_bytes.clone(),
    );
    write_snapshot_and_filemap(
        &child_dir,
        child_signer.as_ref(),
        child_seg_ulid,
        "/blob",
        child_hash,
        child_bytes.len() as u64,
    );

    let stats =
        delta_compute::rewrite_pending_with_deltas(&child_dir, &by_id_dir, child_signer.as_ref())
            .expect("rewrite_pending_with_deltas must read drained source body correctly");
    assert_eq!(stats.entries_converted, 1);

    let rewritten = child_dir.join("pending").join(child_seg_ulid.to_string());
    let (_, entries, _) = segment::read_segment_index(&rewritten).unwrap();
    assert_eq!(entries[0].kind, EntryKind::Delta);
    assert_eq!(entries[0].delta_options[0].source_hash, parent_hash);
}

#[test]
fn rewrite_pending_with_deltas_reads_gc_applied_source_body() {
    // Regression: if an ancestor volume's source segment is sitting in
    // the bare `gc/<id>` handoff window (volume-signed GC output,
    // awaiting coordinator upload/promote), `read_source_extent` must
    // still find it. Prior to the fix the lookup only checked
    // `cache/<id>.body` and `pending/<id>`, so the delta conversion was
    // silently skipped under concurrent GC on the source.
    let tmp = TempDir::new().unwrap();
    let by_id_dir = tmp.path().join("by_id");
    fs::create_dir_all(&by_id_dir).unwrap();

    // Use a body-section-sized payload so the seek arithmetic (which
    // depends on body_section_start for a FullSegment layout) is
    // actually exercised — an inline source would short-circuit before
    // `read_source_extent`'s seek path.
    let mut parent_bytes = Vec::with_capacity(8192);
    for i in 0..8192 {
        parent_bytes.push((i % 251) as u8);
    }
    let parent_hash = blake3::hash(&parent_bytes);
    let parent_stored = compress_prepend_size(&parent_bytes);
    assert!(parent_stored.len() >= 256, "must land in body section");

    let (source_ulid, source_dir, source_signer) =
        make_readonly_volume(&by_id_dir, &ProvenanceLineage::default());
    let source_seg_ulid = Ulid::new();
    let source_seg_path = source_dir.join("pending").join(source_seg_ulid.to_string());
    let mut source_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        2,
        SegmentFlags::COMPRESSED,
        parent_stored,
    )];
    assert_eq!(source_entries[0].kind, EntryKind::Data);
    write_segment(
        &source_seg_path,
        &mut source_entries,
        source_signer.as_ref(),
    )
    .unwrap();
    write_snapshot_and_filemap(
        &source_dir,
        source_signer.as_ref(),
        source_seg_ulid,
        "/blob",
        parent_hash,
        parent_bytes.len() as u64,
    );

    // Simulate mid-GC-handoff: segment body lives in bare `gc/<id>`,
    // not in `pending/`. This is the state the volume produces when
    // `apply_gc_handoffs` renames `<id>.staged` → `<id>` — the body is
    // committed but the coordinator has not yet uploaded and promoted
    // it into `cache/`.
    let gc_dir = source_dir.join("gc");
    fs::create_dir_all(&gc_dir).unwrap();
    let gc_body = gc_dir.join(source_seg_ulid.to_string());
    fs::rename(&source_seg_path, &gc_body).unwrap();

    // Child with a twisted copy of the source bytes — delta-compressible.
    let mut child_bytes = parent_bytes.clone();
    for byte in child_bytes.iter_mut().take(512) {
        *byte ^= 0x55;
    }
    let child_hash = blake3::hash(&child_bytes);

    let child_lineage = ProvenanceLineage {
        parent: None,
        extent_index: vec![format!("{source_ulid}/{source_seg_ulid}")],
        oci_source: None,
    };
    let (_child_ulid, child_dir, child_signer) = make_readonly_volume(&by_id_dir, &child_lineage);
    let child_seg_ulid = write_single_entry_segment(
        &child_dir,
        child_signer.as_ref(),
        child_hash,
        0,
        2,
        child_bytes.clone(),
    );
    write_snapshot_and_filemap(
        &child_dir,
        child_signer.as_ref(),
        child_seg_ulid,
        "/blob",
        child_hash,
        child_bytes.len() as u64,
    );

    let stats =
        delta_compute::rewrite_pending_with_deltas(&child_dir, &by_id_dir, child_signer.as_ref())
            .expect("rewrite_pending_with_deltas must read bare gc/<id> source body");
    assert_eq!(
        stats.entries_converted, 1,
        "source segment in bare gc/<id> should still resolve"
    );

    let rewritten = child_dir.join("pending").join(child_seg_ulid.to_string());
    let (_, entries, _) = segment::read_segment_index(&rewritten).unwrap();
    assert_eq!(entries[0].kind, EntryKind::Delta);
    assert_eq!(entries[0].delta_options[0].source_hash, parent_hash);
}

#[test]
fn rewrite_pending_with_deltas_handles_inline_source() {
    // Regression: a highly-compressible 4 KiB source block lands as an
    // Inline entry (compressed bytes < 256). The delta stage must read
    // the source bytes from `loc.inline_data`, not from `cache/.body`
    // where an Inline entry has no presence. Prior to the fix this
    // failed with an lz4 decompression error on the first jammy->jammy
    // import.
    let tmp = TempDir::new().unwrap();
    let by_id_dir = tmp.path().join("by_id");
    fs::create_dir_all(&by_id_dir).unwrap();

    // 4 KiB of a single byte compresses to ~20 bytes → Inline.
    let parent_bytes = vec![0x00u8; 4096];
    let parent_hash = blake3::hash(&parent_bytes);
    let parent_stored = compress_prepend_size(&parent_bytes);
    assert!(parent_stored.len() < 256);

    let (source_ulid, source_dir, source_signer) =
        make_readonly_volume(&by_id_dir, &ProvenanceLineage::default());
    // Build the Inline source entry directly so we control the stored
    // form exactly. `new_data` promotes to Inline when data.len() < 256.
    let source_seg_ulid = Ulid::new();
    let source_seg_path = source_dir.join("pending").join(source_seg_ulid.to_string());
    let mut source_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        1,
        SegmentFlags::COMPRESSED,
        parent_stored,
    )];
    assert_eq!(source_entries[0].kind, EntryKind::Inline);
    write_segment(
        &source_seg_path,
        &mut source_entries,
        source_signer.as_ref(),
    )
    .unwrap();
    write_snapshot_and_filemap(
        &source_dir,
        source_signer.as_ref(),
        source_seg_ulid,
        "/config",
        parent_hash,
        parent_bytes.len() as u64,
    );

    // Child has the "same" file but a slightly different bitpattern,
    // still compressible enough to benefit from a zstd delta.
    let mut child_bytes = vec![0x00u8; 4096];
    for (i, byte) in child_bytes.iter_mut().enumerate().take(64) {
        *byte = i as u8;
    }
    let child_hash = blake3::hash(&child_bytes);

    let child_lineage = ProvenanceLineage {
        parent: None,
        extent_index: vec![format!("{source_ulid}/{source_seg_ulid}")],
        oci_source: None,
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
        child_signer.as_ref(),
        child_seg_ulid,
        "/config",
        child_hash,
        child_bytes.len() as u64,
    );

    let stats =
        delta_compute::rewrite_pending_with_deltas(&child_dir, &by_id_dir, child_signer.as_ref())
            .expect("rewrite_pending_with_deltas must not fail on inline source");
    assert_eq!(stats.entries_converted, 1);

    let rewritten = child_dir.join("pending").join(child_seg_ulid.to_string());
    let (_, entries, _) = segment::read_segment_index(&rewritten).unwrap();
    assert_eq!(entries[0].kind, EntryKind::Delta);
    assert_eq!(entries[0].delta_options[0].source_hash, parent_hash);
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
        source_signer.as_ref(),
        source_seg_ulid,
        "/same.bin",
        hash,
        bytes.len() as u64,
    );

    let child_lineage = ProvenanceLineage {
        parent: None,
        extent_index: vec![format!("{source_ulid}/{source_seg_ulid}")],
        oci_source: None,
    };
    let (_child_ulid, child_dir, child_signer) = make_readonly_volume(&by_id_dir, &child_lineage);
    let child_seg_ulid =
        write_single_entry_segment(&child_dir, child_signer.as_ref(), hash, 0, 1, bytes.clone());
    write_snapshot_and_filemap(
        &child_dir,
        child_signer.as_ref(),
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
    let (_, entries, _) = segment::read_segment_index(&rewritten).unwrap();
    assert_eq!(
        entries[0].kind,
        EntryKind::Data,
        "unchanged fragment stays DATA"
    );
}
