// Regression test for Delta-bodied reclaim.
//
// Construction:
//   - Parent DATA segment at LBA [0, 8) carrying 8 × 4 KiB of
//     structurally compressible bytes (source dictionary).
//   - Delta segment at LBA [10, 18) — one Delta entry spanning all 8
//     blocks, with `source_hash = parent_hash` and a pre-computed
//     zstd-dict blob in the segment's delta body section. The child
//     bytes are different from parent bytes so the delta is non-trivial.
//
// After `Volume::open` rebuilds lbamap + extent_index, the LBA range
// [10, 18) is a single Delta run. We then:
//   1. Overwrite the middle two LBAs [12, 14) with fresh content —
//      this splits the Delta entry in the lbamap into a head
//      [10, 12) with `payload_block_offset = 0` and a tail
//      [14, 18) with `payload_block_offset = 4`. The Delta entry's
//      stored fragment is bloated: 8 blocks of body resolve 6 live
//      blocks across two LBA runs.
//   2. Call `reclaim_alias_merge(10, 8)`.
//   3. Assert the output is two thin Delta entries (head + tail),
//      each carrying one `DeltaOption` against the original source.
//   4. Assert the bytes read from [10, 18) still match the expected
//      content (parent-derived on the Delta-covered runs, overwrite
//      content in the middle).

use std::fs;
use std::sync::Arc;

use elide_core::extentindex::DeltaBodySource;
use elide_core::segment::{
    DeltaOption, EntryKind, SegmentEntry, SegmentFlags, SegmentSigner, write_segment,
    write_segment_with_delta_body,
};
use elide_core::signing;
use elide_core::ulid_mint::UlidMint;
use elide_core::volume::{ReclaimThresholds, Volume, scan_reclaim_candidates};
use tempfile::TempDir;
use ulid::Ulid;

mod common;

fn setup_volume_dir(tmp: &TempDir) -> (std::path::PathBuf, Arc<dyn SegmentSigner>) {
    let vol_dir = tmp.path().join("vol");
    fs::create_dir_all(&vol_dir).unwrap();
    common::write_test_keypair(&vol_dir);
    fs::create_dir_all(vol_dir.join("pending")).unwrap();
    fs::create_dir_all(vol_dir.join("snapshots")).unwrap();
    let signer = signing::load_signer(&vol_dir, signing::VOLUME_KEY_FILE).unwrap();
    (vol_dir, signer)
}

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

#[test]
fn reclaim_rewrites_bloated_delta_as_thin_delta() {
    let tmp = TempDir::new().unwrap();
    let (vol_dir, signer) = setup_volume_dir(&tmp);

    // Parent content (source dictionary) at LBA [0, 8).
    let parent_bytes = structured_payload(0xA0, 8);
    let parent_hash = blake3::hash(&parent_bytes);

    // Child content at LBA [10, 18) — similar but not identical.
    let child_bytes = structured_payload(0xA1, 8);
    let child_hash = blake3::hash(&child_bytes);

    // zstd-dict delta blob over the full 8-block child fragment.
    let delta_blob = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes)
        .unwrap()
        .compress(&child_bytes)
        .unwrap();
    assert!(
        delta_blob.len() < child_bytes.len(),
        "test fixture: delta must be smaller than raw child bytes to be meaningful"
    );

    // --- Parent DATA segment. ---
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

    // --- Delta segment, ULID strictly greater so rebuild applies it after. ---
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

    // Snapshot marker so the rebuild sees a floor (not strictly
    // required for reclaim, but matches production shape).
    fs::write(vol_dir.join(format!("snapshots/{delta_ulid}")), "").unwrap();

    // --- Open a mutable Volume and split the Delta with a middle overwrite. ---
    let mut vol = Volume::open(&vol_dir, &vol_dir).unwrap();

    // Sanity: the Delta reads back correctly before any bloat.
    assert_eq!(
        vol.read(10, 8).unwrap(),
        child_bytes,
        "pre-reclaim Delta read must return the full child fragment"
    );

    // Overwrite LBAs [12, 14) with fresh content. This splits the Delta
    // LBA entry into head [10, 12) and tail [14, 18); the tail run
    // carries `payload_block_offset = 4`, tripping the reclaim bloat gate.
    let overwrite = vec![0xCDu8; 2 * 4096];
    vol.write(12, &overwrite).unwrap();

    // Pre-reclaim oracle: what [10, 18) should read.
    let mut expected = Vec::with_capacity(8 * 4096);
    expected.extend_from_slice(&child_bytes[..2 * 4096]); // head [10, 12)
    expected.extend_from_slice(&overwrite); //              middle [12, 14)
    expected.extend_from_slice(&child_bytes[4 * 4096..]); // tail [14, 18)
    assert_eq!(vol.read(10, 8).unwrap(), expected, "pre-reclaim oracle");

    // --- Run the reclaim primitive over the full 8-LBA target range. ---
    let outcome = vol.reclaim_alias_merge(10, 8).unwrap();
    assert!(
        !outcome.discarded,
        "no concurrent mutation, must not discard"
    );
    assert_eq!(
        outcome.runs_rewritten, 2,
        "head and tail surviving Delta runs must each be rewritten"
    );

    // Content preserved across reclaim.
    assert_eq!(
        vol.read(10, 8).unwrap(),
        expected,
        "post-reclaim read must still match the oracle"
    );

    // --- Inspect the rewritten segment to confirm the output shape. ---
    let (_lbamap, extent_index) = vol.snapshot_maps();

    // Collect the two new fragment hashes from the head + tail LBA runs.
    // The middle run carries the overwrite hash (not reclaimed because
    // its sole run has `payload_block_offset = 0`).
    let head_block = &child_bytes[..4096];
    let tail_block = &child_bytes[7 * 4096..];
    let head_hash = {
        let mut bytes = Vec::with_capacity(2 * 4096);
        bytes.extend_from_slice(head_block);
        bytes.extend_from_slice(&child_bytes[4096..2 * 4096]);
        blake3::hash(&bytes)
    };
    let tail_hash = {
        let mut bytes = Vec::with_capacity(4 * 4096);
        bytes.extend_from_slice(&child_bytes[4 * 4096..5 * 4096]);
        bytes.extend_from_slice(&child_bytes[5 * 4096..6 * 4096]);
        bytes.extend_from_slice(&child_bytes[6 * 4096..7 * 4096]);
        bytes.extend_from_slice(tail_block);
        blake3::hash(&bytes)
    };

    for (hash, label) in [(head_hash, "head"), (tail_hash, "tail")] {
        let loc = extent_index
            .lookup_delta(&hash)
            .unwrap_or_else(|| panic!("{label} hash must be registered as a Delta"));
        assert_eq!(
            loc.options.len(),
            1,
            "{label} output Delta must carry one option"
        );
        assert_eq!(
            loc.options[0].source_hash, parent_hash,
            "{label} option must point at the original parent source"
        );
        assert!(
            matches!(loc.body_source, DeltaBodySource::Full { .. }),
            "{label} delta body lives inline in a pending segment"
        );
        // The output Delta lives in a new pending segment.
        assert_ne!(
            loc.segment_id, parent_ulid,
            "{label} must not map back to the parent"
        );
        assert_ne!(
            loc.segment_id, delta_ulid,
            "{label} must not map back to the original Delta segment"
        );
    }

    // Old child_hash must no longer be live in lbamap — both runs got
    // replaced. Reading through the extent index should also not find
    // it as a Delta with any live LBA.
    let (lbamap_final, _) = vol.snapshot_maps();
    assert!(
        !lbamap_final.lba_referenced_hashes().contains(&child_hash),
        "the old bloated Delta hash must no longer be referenced by any live LBA"
    );
    // ...but the parent source hash still is, via the reclaim outputs'
    // DeltaOption source pins.
    assert!(
        lbamap_final.lba_referenced_hashes().contains(&parent_hash),
        "the parent source hash must remain pinned by the new Delta entries"
    );

    // Finally: the reclaim output segment's sole entries in the index
    // section are Data (none), DedupRef (none) + Delta (both) — no
    // body_section bytes. Read the segment back and verify.
    // `prepare_reclaim` flushes the WAL before minting the reclaim output
    // ULID, so pending/ may also contain a flushed-WAL segment in addition
    // to the fixture parent/delta and the reclaim output. The reclaim
    // output is always the highest-ULID segment.
    let outcome_seg_ulid = {
        let mut hits: Vec<Ulid> = fs::read_dir(vol_dir.join("pending"))
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
            .collect();
        hits.sort();
        hits.pop()
            .expect("reclaim must have produced a pending segment")
    };
    let out_path = vol_dir.join(format!("pending/{outcome_seg_ulid}"));
    let vk = signing::load_verifying_key(&vol_dir, signing::VOLUME_PUB_FILE).unwrap();
    let (_bss, entries, _inputs) =
        elide_core::segment::read_and_verify_segment_index(&out_path, &vk).unwrap();
    assert_eq!(entries.len(), 2, "output segment carries exactly 2 entries");
    for e in &entries {
        assert_eq!(e.kind, EntryKind::Delta, "output entry must be Delta");
        assert_eq!(e.stored_offset, 0);
        assert_eq!(e.stored_length, 0);
        assert_eq!(e.delta_options.len(), 1);
        assert_eq!(e.delta_options[0].source_hash, parent_hash);
    }
}

/// When the bloated hash H is a DATA entry that's *already* serving as
/// a delta source for some other live entry, reclaim emits thin Deltas
/// pointing back at H instead of fresh Data entries. Rationale:
/// `lba_referenced_hashes` already pins H, so GC cannot drop H's body —
/// emitting a Delta against H is a strict win (100s of bytes of delta
/// blob versus a few KB of fresh body), same retention cost either way.
/// See `docs/design-extent-reclamation.md § Open questions` for the
/// cost/benefit breakdown.
///
/// Construction:
///   - Parent DATA H at LBA [0, 8) (8 × 4 KiB).
///   - Delta entry at LBA 50 with `source_hash = H` so
///     `lbamap.delta_source_refcount(H) > 0` after rebuild.
///   - Middle-overwrite [2, 4) to split H and trip the reclaim bloat
///     gate (tail run ends up with `payload_block_offset = 4`).
///   - Reclaim [0, 8).
///
/// Assertions:
///   - Head and tail outputs are thin Delta entries pointing at H.
///   - Reads preserve the original content.
///   - H remains referenced by `lba_referenced_hashes` post-reclaim
///     (pinned by both our outputs and the unrelated LBA-50 delta).
#[test]
fn reclaim_rewrites_bloated_data_as_delta_when_source_pinned() {
    let tmp = TempDir::new().unwrap();
    let (vol_dir, signer) = setup_volume_dir(&tmp);

    // Parent DATA H at LBA [0, 8). Use structured (not random) bytes so
    // the sub-range slices are substrings of H — zstd-dict compression
    // against the full H resolves them as near-free dictionary refs.
    let parent_bytes = structured_payload(0xC0, 8);
    let parent_hash = blake3::hash(&parent_bytes);

    // Unrelated Delta content at LBA 50 with source = parent_hash. The
    // exact bytes don't matter for this test — we only need the Delta
    // entry's rebuild to bump `delta_source_refcount(parent_hash)`.
    let child50_bytes = structured_payload(0xC1, 1);
    let child50_hash = blake3::hash(&child50_bytes);
    let delta50_blob = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes)
        .unwrap()
        .compress(&child50_bytes)
        .unwrap();

    // --- Parent segment (carries H). ---
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

    // --- Unrelated Delta segment at LBA 50 (source = parent_hash). ---
    let delta50_ulid = mint.next();
    let delta50_path = vol_dir.join(format!("pending/{delta50_ulid}"));
    let mut delta50_entries = vec![SegmentEntry::new_delta(
        child50_hash,
        50,
        1,
        vec![DeltaOption {
            source_hash: parent_hash,
            delta_offset: 0,
            delta_length: delta50_blob.len() as u32,
            delta_hash: blake3::hash(&delta50_blob),
        }],
    )];
    write_segment_with_delta_body(
        &delta50_path,
        &mut delta50_entries,
        &delta50_blob,
        signer.as_ref(),
    )
    .unwrap();
    fs::write(vol_dir.join(format!("snapshots/{delta50_ulid}")), "").unwrap();

    // --- Open, split H via middle overwrite, reclaim. ---
    let mut vol = Volume::open(&vol_dir, &vol_dir).unwrap();

    // Sanity: pre-split the full range reads parent bytes and the
    // LBA-50 Delta reads its child bytes.
    assert_eq!(vol.read(0, 8).unwrap(), parent_bytes);
    assert_eq!(vol.read(50, 1).unwrap(), child50_bytes);

    let overwrite = vec![0x33u8; 2 * 4096];
    vol.write(2, &overwrite).unwrap();

    // Pre-reclaim oracle: head from H, middle from overwrite, tail
    // from H.
    let mut expected = Vec::with_capacity(8 * 4096);
    expected.extend_from_slice(&parent_bytes[..2 * 4096]);
    expected.extend_from_slice(&overwrite);
    expected.extend_from_slice(&parent_bytes[4 * 4096..]);
    assert_eq!(vol.read(0, 8).unwrap(), expected);

    // Sanity check on the precondition the new branch depends on.
    let (lbamap_pre, _) = vol.snapshot_maps();
    assert_eq!(
        lbamap_pre.delta_source_refcount(&parent_hash),
        1,
        "the LBA-50 Delta must contribute one delta-source ref to parent_hash"
    );
    drop(lbamap_pre);

    // Run reclaim.
    let outcome = vol.reclaim_alias_merge(0, 8).unwrap();
    assert!(!outcome.discarded);
    assert_eq!(
        outcome.runs_rewritten, 2,
        "head + tail surviving H runs each rewritten"
    );
    assert_eq!(
        vol.read(0, 8).unwrap(),
        expected,
        "post-reclaim reads match the oracle"
    );

    // --- Shape assertions: output entries are Delta, not Data. ---
    let (lbamap_post, extent_index) = vol.snapshot_maps();

    // Reconstruct head + tail sub-range hashes the output must carry.
    let head_hash = blake3::hash(&parent_bytes[..2 * 4096]);
    let tail_hash = blake3::hash(&parent_bytes[4 * 4096..]);

    for (hash, label) in [(head_hash, "head"), (tail_hash, "tail")] {
        let loc = extent_index
            .lookup_delta(&hash)
            .unwrap_or_else(|| panic!("{label} hash must be registered as a Delta"));
        assert_eq!(loc.options.len(), 1, "{label} carries one option");
        assert_eq!(
            loc.options[0].source_hash, parent_hash,
            "{label} option must point at H"
        );
        assert!(matches!(loc.body_source, DeltaBodySource::Full { .. }));
        // The content is not registered as a DATA location — reclaim
        // must not have emitted both a Delta and a Data entry.
        assert!(
            extent_index.lookup(&hash).is_none(),
            "{label} hash must not also appear as a DATA entry"
        );
    }

    // H itself must still be live: referenced both by the LBA-50 Delta
    // and by our two new outputs.
    let referenced = lbamap_post.lba_referenced_hashes();
    assert!(
        referenced.contains(&parent_hash),
        "H must remain referenced post-reclaim"
    );
    // And H's DATA location is still indexed.
    assert!(
        extent_index.lookup(&parent_hash).is_some(),
        "H's DATA location must still be resolvable"
    );
    // Delta-source refcount rose from 1 (LBA-50 only) to 3 (+ head + tail).
    assert_eq!(
        lbamap_post.delta_source_refcount(&parent_hash),
        3,
        "H gains two delta-source refs from reclaim outputs"
    );
}

/// When H lives in a snapshot-pinned segment (segment_id <= snapshot
/// floor), the body is permanent for as long as the snapshot lives —
/// GC cannot drop it. Reclaim must therefore emit thin Deltas against
/// H rather than fresh Data entries, even when no other entry is
/// currently using H as a delta source. A fresh Data output adds a
/// new body on top of the permanently-retained H body; a Delta output
/// adds ~300 bytes of dict reference for the same retention cost.
///
/// Construction:
///   - DATA segment for H at LBA [0, 8). ULID `parent_ulid`.
///   - `snapshots/<parent_ulid>` marker — H's segment is now at/below
///     the snapshot floor, so `segment_id <= snapshot_floor_ulid`.
///   - No Delta entry references H; `delta_source_refcount(H) == 0`.
///   - Middle-overwrite [2, 4) to split H and trip the bloat gate.
///
/// Assertions:
///   - Both head and tail output entries are thin Deltas pointing at H.
///   - `delta_source_refcount(H) == 2` post-reclaim (only the new
///     outputs contribute), confirming the pre-snapshot guard — not
///     the refcount guard — is what fired.
#[test]
fn reclaim_emits_delta_when_h_is_snapshot_pinned() {
    let tmp = TempDir::new().unwrap();
    let (vol_dir, signer) = setup_volume_dir(&tmp);

    // Parent DATA H at LBA [0, 8). Structured bytes so a sub-range slice
    // is a literal substring of H — zstd-dict compression resolves to a
    // near-zero dict reference.
    let parent_bytes = structured_payload(0xE0, 8);
    let parent_hash = blake3::hash(&parent_bytes);

    let parent_ulid = Ulid::new();
    let parent_path = vol_dir.join(format!("pending/{parent_ulid}"));
    let mut parent_entries = vec![SegmentEntry::new_data(
        parent_hash,
        0,
        8,
        SegmentFlags::empty(),
        parent_bytes.clone(),
    )];
    write_segment(&parent_path, &mut parent_entries, signer.as_ref()).unwrap();

    // Snapshot marker AT parent_ulid — H's segment is now snapshot-pinned.
    fs::write(vol_dir.join(format!("snapshots/{parent_ulid}")), "").unwrap();

    // No Delta entry referencing H. `delta_source_refcount(H)` stays 0.

    let mut vol = Volume::open(&vol_dir, &vol_dir).unwrap();

    // Precondition: the pre-snapshot guard is the only signal firing.
    let (lbamap_pre, _) = vol.snapshot_maps();
    assert_eq!(
        lbamap_pre.delta_source_refcount(&parent_hash),
        0,
        "fixture must have no pre-existing delta sources"
    );
    drop(lbamap_pre);

    // Split H with a middle overwrite.
    let overwrite = vec![0x77u8; 2 * 4096];
    vol.write(2, &overwrite).unwrap();

    // Reclaim the full range.
    let outcome = vol.reclaim_alias_merge(0, 8).unwrap();
    assert!(!outcome.discarded);
    assert_eq!(outcome.runs_rewritten, 2);

    // Oracle: head from H, middle overwrite, tail from H.
    let mut expected = Vec::with_capacity(8 * 4096);
    expected.extend_from_slice(&parent_bytes[..2 * 4096]);
    expected.extend_from_slice(&overwrite);
    expected.extend_from_slice(&parent_bytes[4 * 4096..]);
    assert_eq!(vol.read(0, 8).unwrap(), expected);

    let (lbamap_post, extent_index) = vol.snapshot_maps();

    let head_hash = blake3::hash(&parent_bytes[..2 * 4096]);
    let tail_hash = blake3::hash(&parent_bytes[4 * 4096..]);

    // Both outputs are thin Deltas against H — the pre-snapshot guard
    // fired even though no existing Delta was sourcing from H.
    for (hash, label) in [(head_hash, "head"), (tail_hash, "tail")] {
        let loc = extent_index
            .lookup_delta(&hash)
            .unwrap_or_else(|| panic!("{label} must be emitted as a Delta"));
        assert_eq!(loc.options.len(), 1);
        assert_eq!(loc.options[0].source_hash, parent_hash);
        assert!(
            extent_index.lookup(&hash).is_none(),
            "{label} must not also appear as a DATA entry"
        );
    }

    // Only our two outputs contribute to H's delta-source refcount —
    // confirming that the pre-snapshot guard, not the refcount guard,
    // is what triggered the Delta output shape.
    assert_eq!(
        lbamap_post.delta_source_refcount(&parent_hash),
        2,
        "only the new reclaim outputs should pin H via delta-source refs"
    );
}

/// `scan_reclaim_candidates` historically only consulted the Data/Inline
/// extent-index table, so a bloated Delta fragment was invisible to the
/// scanner. The primitive could still reclaim it when invoked directly,
/// but the CLI's scanner-driven pass silently missed it. After the
/// scanner extension, a middle-overwritten Delta fragment surfaces as
/// a candidate and round-trips through the primitive cleanly.
#[test]
fn scanner_surfaces_bloated_delta_hash() {
    let tmp = TempDir::new().unwrap();
    let (vol_dir, signer) = setup_volume_dir(&tmp);

    // Parent DATA source at LBA [0, 8).
    let parent_bytes = structured_payload(0xD0, 8);
    let parent_hash = blake3::hash(&parent_bytes);

    // Child Delta at LBA [10, 18), source = parent_hash, 8-block
    // fragment (so there's something to bloat by splitting).
    let child_bytes = structured_payload(0xD1, 8);
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
    let mut delta_entries = vec![SegmentEntry::new_delta(
        child_hash,
        10,
        8,
        vec![DeltaOption {
            source_hash: parent_hash,
            delta_offset: 0,
            delta_length: delta_blob.len() as u32,
            delta_hash: blake3::hash(&delta_blob),
        }],
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

    // Split the Delta with a middle overwrite to create bloat.
    let overwrite = vec![0xEEu8; 2 * 4096];
    vol.write(12, &overwrite).unwrap();

    // Scanner must surface the bloated child_hash as a candidate. Use
    // permissive thresholds so the scanner doesn't gate on byte size.
    let (lbamap, extent_index) = vol.snapshot_maps();
    let candidates = scan_reclaim_candidates(
        &lbamap,
        &extent_index,
        ReclaimThresholds {
            min_dead_blocks: 1,
            min_dead_ratio: 0.0,
            min_stored_bytes: 0,
        },
    );
    drop(lbamap);
    drop(extent_index);

    // Exactly one candidate: the bloated Delta fragment. The parent
    // DATA at [0, 8) has no overwrites, so it's not flagged.
    assert_eq!(
        candidates.len(),
        1,
        "scanner must surface the split Delta fragment, got {candidates:?}"
    );
    let c = candidates[0];
    assert_eq!(c.start_lba, 10);
    assert_eq!(c.lba_length, 8);
    assert!(
        c.dead_count_is_lower_bound,
        "delta-backed candidates carry a lower-bound dead count"
    );

    // Round-trip the candidate through the primitive. The primitive
    // handles Delta-backed bloat by emitting thin Deltas pointing at
    // the same source (covered in detail by earlier tests).
    let outcome = vol.reclaim_alias_merge(c.start_lba, c.lba_length).unwrap();
    assert!(!outcome.discarded);
    assert!(
        outcome.runs_rewritten > 0,
        "scanner-proposed candidate must round-trip through the primitive"
    );

    // Rescan: no residual candidates.
    let (lbamap, extent_index) = vol.snapshot_maps();
    let candidates_after = scan_reclaim_candidates(
        &lbamap,
        &extent_index,
        ReclaimThresholds {
            min_dead_blocks: 1,
            min_dead_ratio: 0.0,
            min_stored_bytes: 0,
        },
    );
    assert!(
        candidates_after.is_empty(),
        "post-reclaim rescan must find nothing, got {candidates_after:?}"
    );
}
