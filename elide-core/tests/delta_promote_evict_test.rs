// Regression test for a promote_segment bug: `DeltaBodySource::Full →
// Cached` was not flipped for Delta entries in the drain path.
//
// Before the fix, a Delta entry in a pending segment kept
// `DeltaBodySource::Full { body_section_start, body_length }` after
// `apply_promote_segment_result` moved the segment to the three-file
// cache layout. Reads then tried to seek into `cache/<ulid>.body`
// (BodyOnly layout) at offsets that assume a full segment file —
// producing silent wrong-bytes before `evict`, and a `segment not
// found` error after `evict` removed the `.body` file. The delta
// blob's actual home is `cache/<ulid>.delta`, which is only consulted
// when `DeltaBodySource::Cached` is set.
//
// The fix flips the delta body source during
// `apply_promote_segment_result`'s drain branch, with a CAS against
// `segment_id` so concurrent delta-repack / reclaim that re-pointed
// the hash at a newer segment wins.

use std::fs;
use std::sync::Arc;

use elide_core::extentindex::DeltaBodySource;
use elide_core::segment::{
    DeltaOption, SegmentEntry, SegmentFlags, SegmentSigner, write_segment,
    write_segment_with_delta_body,
};
use elide_core::signing;
use elide_core::ulid_mint::UlidMint;
use elide_core::volume::Volume;
use tempfile::TempDir;
use ulid::Ulid;

mod common;

/// 8 × 4 KiB of structured bytes. Distinct seeds give distinct hashes
/// but the patterns are similar enough that zstd-dict compression
/// produces a meaningfully smaller delta.
fn structured_payload(seed: u8, n_blocks: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n_blocks * 4096);
    for block in 0..n_blocks {
        for i in 0..4096 {
            out.push(seed.wrapping_add(block as u8).wrapping_add((i >> 5) as u8));
        }
    }
    out
}

/// Build a volume with two pending segments: a parent DATA segment at
/// LBA [0, 8) (the delta source dictionary) and a child Delta segment
/// at LBA [10, 18) whose single delta option references the parent's
/// hash. Returns `(vol_dir, parent_ulid, delta_ulid, parent_bytes,
/// child_bytes)`.
fn setup_delta_volume() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    Ulid,
    Ulid,
    Vec<u8>,
    Vec<u8>,
) {
    let tmp = TempDir::new().unwrap();
    let vol_dir = tmp.path().join("vol");
    fs::create_dir_all(&vol_dir).unwrap();
    common::write_test_keypair(&vol_dir);
    fs::create_dir_all(vol_dir.join("pending")).unwrap();
    fs::create_dir_all(vol_dir.join("snapshots")).unwrap();
    let signer = signing::load_signer(&vol_dir, signing::VOLUME_KEY_FILE).unwrap();

    let parent_bytes = structured_payload(0xA0, 8);
    let parent_hash = blake3::hash(&parent_bytes);

    let child_bytes = structured_payload(0xA1, 8);
    let child_hash = blake3::hash(&child_bytes);

    let delta_blob = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes)
        .unwrap()
        .compress(&child_bytes)
        .unwrap();
    assert!(
        delta_blob.len() < child_bytes.len(),
        "test fixture: delta must be smaller than raw child bytes"
    );

    let mut mint = UlidMint::new(Ulid::nil());
    let parent_ulid = mint.next();
    let parent_path = vol_dir.join(format!("pending/{parent_ulid}"));
    let mut parent_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        8,
        SegmentFlags::empty(),
        parent_bytes.clone(),
    )];
    write_segment(&parent_path, &mut parent_entries, signer.as_ref()).unwrap();

    let delta_ulid = mint.next();
    let delta_path = vol_dir.join(format!("pending/{delta_ulid}"));
    let delta_option = DeltaOption {
        source_hash: parent_hash,
        delta_offset: 0,
        delta_length: delta_blob.len() as u32,
        delta_hash: blake3::hash(&delta_blob),
    };
    let mut delta_entries = vec![SegmentEntry::new_delta(
        child_hash,
        10,
        8,
        vec![delta_option],
    )];
    write_segment_with_delta_body(
        &delta_path,
        &mut delta_entries,
        &delta_blob,
        signer.as_ref(),
    )
    .unwrap();

    (
        tmp,
        vol_dir,
        parent_ulid,
        delta_ulid,
        parent_bytes,
        child_bytes,
    )
}

/// `promote_segment` on a pending segment that carries a Delta entry
/// must flip its `DeltaBodySource` from `Full` to `Cached`, so reads
/// post-promote resolve the delta blob from `cache/<ulid>.delta`
/// rather than seeking into `cache/<ulid>.body` at offsets that
/// assume a full segment file.
#[test]
fn promote_segment_flips_delta_body_source_full_to_cached() {
    let (_tmp, vol_dir, parent_ulid, delta_ulid, _parent_bytes, child_bytes) = setup_delta_volume();

    let mut vol = Volume::open(&vol_dir, &vol_dir).unwrap();

    // Rebuild from pending: delta's body_source starts as Full.
    let (_lbamap, ei) = vol.snapshot_maps();
    let pre = ei
        .lookup_delta(&blake3::hash(&child_bytes))
        .expect("delta entry must be indexed after rebuild");
    assert!(
        matches!(pre.body_source, DeltaBodySource::Full { .. }),
        "pre-promote: pending delta is Full, got {:?}",
        pre.body_source
    );

    // Drain path: promote parent then delta into the three-file cache layout.
    vol.promote_segment(parent_ulid).unwrap();
    vol.promote_segment(delta_ulid).unwrap();

    // Post-promote: delta body source is Cached (bug fix).
    let (_lbamap, ei) = vol.snapshot_maps();
    let post = ei
        .lookup_delta(&blake3::hash(&child_bytes))
        .expect("delta entry must still be indexed after promote");
    assert!(
        matches!(post.body_source, DeltaBodySource::Cached),
        "post-promote: delta body_source must be Cached, got {:?}",
        post.body_source
    );
    assert_eq!(
        post.segment_id, delta_ulid,
        "delta segment_id must still match"
    );
}

/// End-to-end symptom: a delta read at LBA 10 after `volume evict`
/// (which removes `cache/<delta>.body` but leaves `cache/<delta>.delta`
/// and the parent body). Before the fix the read failed with
/// `segment not found: <delta_ulid>`; after the fix it resolves the
/// delta blob from the `.delta` sidecar and decompresses against the
/// parent body.
#[test]
fn delta_read_after_promote_and_body_evict() {
    let (_tmp, vol_dir, parent_ulid, delta_ulid, _parent_bytes, child_bytes) = setup_delta_volume();

    let mut vol = Volume::open(&vol_dir, &vol_dir).unwrap();

    // Sanity: pre-promote read from the pending delta works.
    assert_eq!(
        vol.read(10, 8).unwrap(),
        child_bytes,
        "pre-promote delta read must yield the full child fragment"
    );

    vol.promote_segment(parent_ulid).unwrap();
    vol.promote_segment(delta_ulid).unwrap();

    // Post-promote read still works (delta sidecar now in cache/).
    assert_eq!(
        vol.read(10, 8).unwrap(),
        child_bytes,
        "post-promote delta read must yield the full child fragment"
    );

    // Evict only the delta segment's `.body` file — the delta blob
    // lives in the `.delta` sidecar and the parent body is still
    // needed as the zstd dictionary.
    let body_path = vol_dir.join("cache").join(format!("{delta_ulid}.body"));
    assert!(body_path.exists(), "delta body must exist before evict");
    fs::remove_file(&body_path).unwrap();

    // Post-evict read must succeed: the delta blob is read from
    // `cache/<delta_ulid>.delta` and decompressed against the parent
    // body in `cache/<parent_ulid>.body`. Before the fix this failed
    // with `segment not found: <delta_ulid>` because
    // `DeltaBodySource::Full` routed through `find_segment_file`
    // looking for a full segment layout.
    assert_eq!(
        vol.read(10, 8).unwrap(),
        child_bytes,
        "post-evict delta read must still yield the full child fragment"
    );
}

/// Same property for reclaim-produced Delta entries: reclaim writes
/// its output into `pending/<new_ulid>` with `DeltaBodySource::Full`,
/// so the same `promote_segment` flip must apply when the coordinator
/// drains the reclaim output.
#[test]
fn reclaim_delta_output_flips_body_source_on_promote() {
    let tmp = TempDir::new().unwrap();
    let vol_dir = tmp.path().join("vol");
    fs::create_dir_all(&vol_dir).unwrap();
    common::write_test_keypair(&vol_dir);
    fs::create_dir_all(vol_dir.join("pending")).unwrap();
    fs::create_dir_all(vol_dir.join("snapshots")).unwrap();
    let signer: Arc<dyn SegmentSigner> =
        signing::load_signer(&vol_dir, signing::VOLUME_KEY_FILE).unwrap();

    let parent_bytes = structured_payload(0xA0, 8);
    let parent_hash = blake3::hash(&parent_bytes);
    let child_bytes = structured_payload(0xA1, 8);
    let child_hash = blake3::hash(&child_bytes);

    let delta_blob = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes)
        .unwrap()
        .compress(&child_bytes)
        .unwrap();

    let mut mint = UlidMint::new(Ulid::nil());
    let parent_ulid = mint.next();
    let parent_path = vol_dir.join(format!("pending/{parent_ulid}"));
    let mut parent_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        8,
        SegmentFlags::empty(),
        parent_bytes.clone(),
    )];
    write_segment(&parent_path, &mut parent_entries, signer.as_ref()).unwrap();

    let delta_ulid = mint.next();
    let delta_path = vol_dir.join(format!("pending/{delta_ulid}"));
    let delta_option = DeltaOption {
        source_hash: parent_hash,
        delta_offset: 0,
        delta_length: delta_blob.len() as u32,
        delta_hash: blake3::hash(&delta_blob),
    };
    let mut delta_entries = vec![SegmentEntry::new_delta(
        child_hash,
        10,
        8,
        vec![delta_option],
    )];
    write_segment_with_delta_body(
        &delta_path,
        &mut delta_entries,
        &delta_blob,
        signer.as_ref(),
    )
    .unwrap();

    fs::write(vol_dir.join(format!("snapshots/{delta_ulid}")), "").unwrap();

    let mut vol = Volume::open(&vol_dir, &vol_dir).unwrap();

    // Split the Delta's single 8-block LBA run by overwriting the
    // middle — this trips the reclaim bloat gate so the subsequent
    // `reclaim_alias_merge` emits two thin Delta outputs.
    let overwrite = vec![0xCDu8; 2 * 4096];
    vol.write(12, &overwrite).unwrap();

    // Reclaim: produces a new pending segment whose Delta entries
    // live with `DeltaBodySource::Full` (per the reclaim apply path).
    let outcome = vol.reclaim_alias_merge(10, 8).unwrap();
    assert!(!outcome.discarded, "reclaim must not be discarded");

    // Identify the new pending segment (neither parent nor original
    // delta). Promote it and verify the output's Delta entries flip
    // to Cached.
    // `prepare_reclaim` flushes the WAL before minting the reclaim output
    // ULID, so pending/ may also contain a flushed-WAL segment in addition
    // to the fixture parent/delta and the reclaim output. The reclaim
    // output is always the highest-ULID segment.
    let reclaim_ulid = {
        let mut hits = fs::read_dir(vol_dir.join("pending"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let n = e.file_name().into_string().ok()?;
                if n.contains('.') {
                    return None;
                }
                Ulid::from_string(&n).ok()
            })
            .filter(|u| *u != parent_ulid && *u != delta_ulid)
            .collect::<Vec<_>>();
        hits.sort();
        hits.pop()
            .expect("reclaim must have produced a pending segment")
    };

    // Collect live hashes whose delta entry points at the reclaim
    // output — these are the Deltas we need to watch across promote.
    let delta_output_hashes: Vec<blake3::Hash> = {
        let (lbamap, ei) = vol.snapshot_maps();
        lbamap
            .lba_referenced_hashes()
            .into_iter()
            .filter(|h| {
                ei.lookup_delta(h)
                    .is_some_and(|loc| loc.segment_id == reclaim_ulid)
            })
            .collect()
    };
    assert!(
        !delta_output_hashes.is_empty(),
        "reclaim must have produced at least one Delta output on the new segment"
    );
    // Verify pre-promote state: DeltaBodySource::Full.
    {
        let (_lbamap, ei) = vol.snapshot_maps();
        for hash in &delta_output_hashes {
            let loc = ei.lookup_delta(hash).unwrap();
            assert!(
                matches!(loc.body_source, DeltaBodySource::Full { .. }),
                "pre-promote reclaim output delta must be Full, got {:?}",
                loc.body_source
            );
        }
    }

    // Promote the reclaim output (parent is still in pending too —
    // promote it so its body lives at cache/<parent>.body for the
    // post-evict read).
    vol.promote_segment(parent_ulid).unwrap();
    vol.promote_segment(reclaim_ulid).unwrap();

    let (_lbamap, ei) = vol.snapshot_maps();
    for hash in &delta_output_hashes {
        let loc = ei
            .lookup_delta(hash)
            .expect("reclaim output delta must still be indexed after promote");
        assert!(
            matches!(loc.body_source, DeltaBodySource::Cached),
            "reclaim output delta must be DeltaBodySource::Cached after promote, got {:?}",
            loc.body_source
        );
        assert_eq!(loc.segment_id, reclaim_ulid);
    }
}
