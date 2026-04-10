// Integration tests: GC correctness with interleaved live writes.
//
// Two scenarios that the random proptest rarely hits because CoordGcLocal
// silently no-ops when index/ is empty:
//
// 1. Live write overwrites a GC candidate LBA *before* the GC pass runs.
//    `simulate_coord_gc_local` rebuilds the LBA map from all current segments,
//    so it filters out stale entries — the GC output will not contain the old
//    value for any LBA that was overwritten after gc_checkpoint().
//
// 2. Live write overwrites a GC candidate LBA *after* the GC pass runs (but
//    before the handoff is applied).  The GC output now contains the old value.
//    `sort_for_rebuild` gives GC-output segments lower priority than regular
//    segments, so the live write's pending segment wins on conflict during
//    rebuild.

use std::path::PathBuf;

use elide_core::volume::Volume;

mod common;

/// Overwrite a GC candidate LBA before the GC pass: the GC output must not
/// contain the stale pre-overwrite value.
#[test]
fn gc_filters_stale_entries_when_lba_overwritten_before_gc() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Seed batch 1 — LBAs 0-3 = 0xAA — drain to index/ + cache/.
    for lba in 0u64..4 {
        vol.write(lba, &[0xAA; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    // Seed batch 2 — LBAs 4-7 = 0xBB — drain to index/ + cache/.
    for lba in 4u64..8 {
        vol.write(lba, &[0xBB; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    // Take GC checkpoint (flushes WAL, advances mint).
    let (gc_ulid, _) = vol.gc_checkpoint().unwrap();

    // Live overwrite of LBAs 0-3 *before* GC runs.
    for lba in 0u64..4 {
        vol.write(lba, &[0xCC; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    // Keep in pending/ — do not drain.

    // GC pass: LBA map includes the 0xCC overwrite, so 0xAA entries are stale
    // and must be filtered out of the GC output.
    let (_, _, to_delete) = common::simulate_coord_gc_local(&fork_dir, gc_ulid, 2)
        .expect("GC should compact the two seeded segments");
    vol.apply_gc_handoffs().unwrap();
    for path in &to_delete {
        let _ = std::fs::remove_file(path);
    }

    // Live overwrite must win.
    let expected: &[(u64, u8)] = &[
        (0, 0xCC),
        (1, 0xCC),
        (2, 0xCC),
        (3, 0xCC),
        (4, 0xBB),
        (5, 0xBB),
        (6, 0xBB),
        (7, 0xBB),
    ];
    for &(lba, byte) in expected {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(actual, vec![byte; 4096], "lba {lba} wrong before crash");
    }

    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    for &(lba, byte) in expected {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual,
            vec![byte; 4096],
            "lba {lba} wrong after crash+rebuild"
        );
    }
    drop(vol);
}

/// Overwrite a GC candidate LBA *after* the GC pass runs: sort_for_rebuild
/// must give the GC output lower priority than the live pending segment.
#[test]
fn gc_output_loses_to_live_write_applied_after_gc() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Seed batch 1 — LBAs 0-3 = 0xAA.
    for lba in 0u64..4 {
        vol.write(lba, &[0xAA; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    // Seed batch 2 — LBAs 4-7 = 0xBB.
    for lba in 4u64..8 {
        vol.write(lba, &[0xBB; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    // GC checkpoint then GC pass — no overwrites yet so GC output contains
    // 0xAA for LBAs 0-3 and 0xBB for LBAs 4-7.
    let (gc_ulid, _) = vol.gc_checkpoint().unwrap();
    let (_, _, to_delete) = common::simulate_coord_gc_local(&fork_dir, gc_ulid, 2)
        .expect("GC should compact the two seeded segments");

    // Live overwrite of LBAs 0-3 *after* GC ran but before handoff is applied.
    for lba in 0u64..4 {
        vol.write(lba, &[0xCC; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    // Keep in pending/ — do not drain.

    // Apply handoff and delete old segments.  The GC output (containing stale
    // 0xAA for LBAs 0-3) and the live pending segment (containing 0xCC) both
    // exist; sort_for_rebuild must prefer the pending segment.
    vol.apply_gc_handoffs().unwrap();
    for path in &to_delete {
        let _ = std::fs::remove_file(path);
    }

    let expected: &[(u64, u8)] = &[
        (0, 0xCC),
        (1, 0xCC),
        (2, 0xCC),
        (3, 0xCC),
        (4, 0xBB),
        (5, 0xBB),
        (6, 0xBB),
        (7, 0xBB),
    ];
    for &(lba, byte) in expected {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(actual, vec![byte; 4096], "lba {lba} wrong before crash");
    }

    // Crash + rebuild: rebuild must still prefer the pending segment over the
    // GC output for LBAs 0-3.
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    for &(lba, byte) in expected {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual,
            vec![byte; 4096],
            "lba {lba} wrong after crash+rebuild"
        );
    }
    drop(vol);
}

/// Regression: write-path dedup creates two segments with the same hash (one
/// DATA, one DedupRef) for different LBAs.  The extent_index tracks a
/// single canonical location per hash, so the non-canonical segment's DATA
/// entry looks extent-dead — but its LBA mapping is still live.  GC must keep
/// such entries via lba_live, otherwise the LBA mapping is lost on crash.
///
/// Sequence (proptest regression seed 4):
///   Write(0, 40), Flush, Drain  — S1: LBA 0→H40
///   Write(1, 41), Flush, Drain  — S2: LBA 1→H41
///   CoordGcLocal(2)             — G1 carries H40+H41, deletes S1+S2
///   Write(0, 101), Flush        — S3: LBA 0→H101 (Data)
///   Write(1, 101)               — becomes DedupRef (same hash H101)
///   CoordGcLocal(2), Drain      — S4: LBA 1→H101 (DedupRef)
///                                  extent_index: H101→S4 (overwrites S3)
///                                  GC compacts S3+S4: S3's DATA entry is
///                                  extent-dead → dropped without lba_live fix
///   CoordGcLocal(2), Crash      — G2 only carries LBA 1→H101; LBA 0 reverts
///                                  to H40 from G1 → data loss
#[test]
fn gc_preserves_data_entry_when_lba_live_but_not_extent_canonical() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // S1: LBA 0 = seed 40
    vol.write(0, &[40u8; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    // S2: LBA 1 = seed 41
    vol.write(1, &[41u8; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    // GC pass 1: compact S1+S2 into G1.
    let (gc_ulid, _) = vol.gc_checkpoint().unwrap();
    let (_, _, to_delete) =
        common::simulate_coord_gc_local(&fork_dir, gc_ulid, 2).expect("GC should compact S1+S2");
    let applied = vol.apply_gc_handoffs().unwrap();
    assert!(applied > 0);
    for path in &to_delete {
        let _ = std::fs::remove_file(path);
    }

    // Overwrite LBA 0 with seed 101 (new hash H101, DATA entry).
    vol.write(0, &[101u8; 4096]).unwrap();
    vol.flush_wal().unwrap();
    // S3 now in pending/ with LBA 0→H101 (Data).

    // Overwrite LBA 1 with seed 101 — same data → write-path dedup creates
    // a thin DedupRef (H101 already in extent_index from S3).
    vol.write(1, &[101u8; 4096]).unwrap();
    // Not flushed yet — still in WAL / pending_entries.

    // GC pass 2: gc_checkpoint flushes WAL (creates S4 with DedupRef for
    // LBA 1→H101), then GC finds nothing in index/ (no candidates).
    let (gc_ulid2, _) = vol.gc_checkpoint().unwrap();
    let _ = common::simulate_coord_gc_local(&fork_dir, gc_ulid2, 2);
    let _ = vol.apply_gc_handoffs();

    // Drain: materialise S3 (no thin refs) and S4 (DedupRef → DedupRef).
    // After promote, index/ has S3.idx (Data) and S4.idx (DedupRef).
    // extent_index: H101 → S4 (S4 processed last, overwrites S3).
    common::drain_with_redact(&mut vol);

    // GC pass 3: compact S3+S4.
    // Bug: S3's DATA entry is not extent-canonical (extent says H101→S4), so
    // without the lba_live fix it's dropped.  G2 only carries LBA 1→H101.
    let (gc_ulid3, _) = vol.gc_checkpoint().unwrap();
    let to_delete3 =
        if let Some((_, _, paths)) = common::simulate_coord_gc_local(&fork_dir, gc_ulid3, 2) {
            let applied3 = vol.apply_gc_handoffs().unwrap();
            assert!(applied3 > 0, "GC pass 3 must apply");
            paths
        } else {
            panic!("GC pass 3 should find S3+S4 in index/");
        };
    for path in &to_delete3 {
        let _ = std::fs::remove_file(path);
    }

    // Verify before crash.
    assert_eq!(
        vol.read(0, 1).unwrap(),
        vec![101u8; 4096],
        "LBA 0 before crash"
    );
    assert_eq!(
        vol.read(1, 1).unwrap(),
        vec![101u8; 4096],
        "LBA 1 before crash"
    );

    // Crash + rebuild.
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    assert_eq!(
        vol.read(0, 1).unwrap(),
        vec![101u8; 4096],
        "LBA 0 wrong after crash+rebuild"
    );
    assert_eq!(
        vol.read(1, 1).unwrap(),
        vec![101u8; 4096],
        "LBA 1 wrong after crash+rebuild"
    );
}

/// Variant (b) regression: DATA + sibling DedupRef in the **same segment**,
/// with the DATA's LBA overwritten so the DATA entry is LBA-dead but its
/// hash is still referenced by the sibling DedupRef's LBA.
///
/// The canonical-presence invariant requires this DATA entry to survive
/// both redact and GC, because it is the only copy of the hash's bytes
/// anywhere — dropping it would leave the DedupRef unresolvable.
///
/// This is load-bearing on `lba_map::lba_referenced_hashes()` being sourced
/// from the LBA map and *not* from a DATA-only filter. If a future change
/// narrows the filter, this test fails loudly — see the worked examples in
/// `docs/architecture.md § Dedup`.
#[test]
fn gc_preserves_canonical_when_only_sibling_dedup_ref_keeps_hash_alive() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // High-entropy payload so it does not compress below INLINE_THRESHOLD
    // and lands in the body section as a genuine DATA entry.
    let payload: Vec<u8> = (0..4096).map(|i| (i * 7 + 13) as u8).collect();

    // Variant (b): single segment containing Data(LBA 0, H) and
    // DedupRef(LBA 1, H). In-session dedup produces the DedupRef in the
    // same WAL as the Data entry, so both land in the same segment on
    // flush.
    vol.write(0, &payload).unwrap();
    vol.write(1, &payload).unwrap();
    vol.flush_wal().unwrap();

    // Overwrite LBA 0 with different data. The DATA entry at LBA 0 is now
    // LBA-dead (LBA 0 maps to a new hash), but hash H is still referenced
    // by LBA 1's DedupRef. The DATA must NOT be dropped — it is the only
    // copy of H's bytes anywhere.
    let overwrite: Vec<u8> = (0..4096).map(|i| (i * 11 + 3) as u8).collect();
    vol.write(0, &overwrite).unwrap();
    vol.flush_wal().unwrap();

    // Drain: redact must preserve the LBA-dead-but-hash-alive DATA entry,
    // and promote publishes the segment to index/ + cache/.
    common::drain_with_redact(&mut vol);

    // LBA 1 reads must return the original payload (resolved via extent
    // index to the same-segment DATA at LBA 0).
    assert_eq!(
        vol.read(1, 1).unwrap(),
        payload,
        "LBA 1 must read original payload after drain"
    );
    assert_eq!(
        vol.read(0, 1).unwrap(),
        overwrite,
        "LBA 0 must read overwrite after drain"
    );

    // GC pass: compact the drained segment and any others. The canonical
    // DATA for H must be carried through — dropping it would break LBA 1.
    let (gc_ulid, _) = vol.gc_checkpoint().unwrap();
    if let Some((_, _, to_delete)) = common::simulate_coord_gc_local(&fork_dir, gc_ulid, 1) {
        vol.apply_gc_handoffs().unwrap();
        for path in &to_delete {
            let _ = std::fs::remove_file(path);
        }
    }

    // Verify both reads before crash.
    assert_eq!(
        vol.read(1, 1).unwrap(),
        payload,
        "LBA 1 must read original payload after GC"
    );
    assert_eq!(
        vol.read(0, 1).unwrap(),
        overwrite,
        "LBA 0 must read overwrite after GC"
    );

    // Crash + rebuild: extent index rebuilt from disk must still carry the
    // canonical DATA for H, so LBA 1 resolves.
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    assert_eq!(
        vol.read(1, 1).unwrap(),
        payload,
        "LBA 1 must read original payload after crash+rebuild"
    );
    assert_eq!(
        vol.read(0, 1).unwrap(),
        overwrite,
        "LBA 0 must read overwrite after crash+rebuild"
    );
}
