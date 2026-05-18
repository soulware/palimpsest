//\! Materialized deterministic reproducers for data-loss / corruption
//\! bugs originally surfaced by the randomized `volume_proptest` and
//\! `actor_proptest` suites and then minimized to a fixed op sequence.
//\!
//\! They live in their own test target, separate from the randomized
//\! `proptest\!` suites, so the CI proptest lane can run them with
//\! `volume-invariants` enabled. On a handful of short, deterministic
//\! sequences the per-op disk-rebuild invariant is cheap, and it turns
//\! structural drift into a clean panic at the introducing call site
//\! instead of an obscure oracle mismatch many ops later — exactly the
//\! localization a regression guard wants. The randomized suites stay
//\! invariants-off for speed (the per-op rebuild dominates their
//\! runtime). Membership is structural — a reproducer is covered by the
//\! invariant lane because it lives in this file, not because it matches
//\! a name filter. See `docs/testing.md`.

mod common;

use common::{incompressible_block, pick_snap_ulid};
use ulid::Ulid;

/// Deterministic regression for proptest seed
/// `a978281ba28abb699f33ac4b1c491da6efedd24a45b65db82d893504458c85fc`,
/// shrunk by `reclaim_crash_recovery` to the 7-op sequence below.
///
/// After the fourth `WriteLargeMulti` overlaps a previously-written
/// region, `reclaim_alias_merge` followed by `repack` and a crash left
/// a previously-readable LBA returning all-zeros instead of the bytes
/// the oracle wrote. Now fixed; kept as a regression guard so any
/// future change to the reclaim → repack → crash path that re-breaks
/// this shape fails outside the proptest harness.
#[test]
fn reclaim_crash_recovery_seed_a978281b_regression() {
    use elide_core::volume::Volume;

    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();
    common::write_test_keypair(fork_dir);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    let mut oracle: std::collections::HashMap<u64, [u8; 4096]> = std::collections::HashMap::new();

    // Each WriteLargeMulti writes 8 contiguous incompressible 4 KiB
    // blocks at LBA 24+lba.
    let write_large_multi = |vol: &mut Volume,
                             oracle: &mut std::collections::HashMap<u64, [u8; 4096]>,
                             lba: u8,
                             seed: u8| {
        let mut payload = Vec::with_capacity(8 * 4096);
        for i in 0..8 {
            payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i as u8)));
        }
        let start = 24 + lba as u64;
        if vol.write(start, &payload).is_ok() {
            for i in 0..8 {
                let mut block = [0u8; 4096];
                block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                oracle.insert(start + i as u64, block);
            }
        }
    };

    let assert_oracle =
        |vol: &mut Volume, oracle: &std::collections::HashMap<u64, [u8; 4096]>, step: &str| {
            for (&lba, expected) in oracle {
                let actual = vol.read(lba, 1).unwrap();
                assert_eq!(
                    actual.as_slice(),
                    expected.as_slice(),
                    "lba {lba} wrong after {step}"
                );
            }
        };

    write_large_multi(&mut vol, &mut oracle, 0, 113);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=0,seed=113)");

    write_large_multi(&mut vol, &mut oracle, 2, 62);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=2,seed=62)");

    write_large_multi(&mut vol, &mut oracle, 5, 113);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=5,seed=113)");

    write_large_multi(&mut vol, &mut oracle, 2, 75);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=2,seed=75)");

    let outcome = vol.reclaim_alias_merge(3, 6).unwrap();
    assert!(!outcome.discarded, "single-threaded driver: never discards");
    assert_oracle(&mut vol, &oracle, "ReclaimRange(start_lba=3,lba_count=6)");

    let _ = vol.repack();
    assert_oracle(&mut vol, &oracle, "Repack");

    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    assert_oracle(&mut vol, &oracle, "Crash");
}

/// Deterministic regression for proptest seed
/// `b0f166f0ea2c438cf612a27e217282a545141bd0faed6fe815b42177333fa71b`,
/// shrunk by `reclaim_crash_recovery` to the 7-op sequence below.
///
/// Two `WriteLargeMulti` writes at overlapping LBA ranges (LBAs 24..32
/// then 26..34, both with `seed = 102`), bracketed by
/// `reclaim_alias_merge(24, 1)` calls and a final `WriteLargeMulti` at
/// LBA 24..32 with `seed = 0`. After `sweep_pending` + crash, LBA 32
/// read back something other than the bytes the third
/// `WriteLargeMulti` wrote — its content was not preserved across the
/// crash boundary the way the oracle expects. Now fixed; kept as a
/// regression guard.
#[test]
fn reclaim_crash_recovery_seed_b0f166f0_regression() {
    use elide_core::volume::Volume;

    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();
    common::write_test_keypair(fork_dir);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    let mut oracle: std::collections::HashMap<u64, [u8; 4096]> = std::collections::HashMap::new();

    // Mirrors `ReclaimOp::WriteLargeMulti` in the proptest body.
    let write_large_multi = |vol: &mut Volume,
                             oracle: &mut std::collections::HashMap<u64, [u8; 4096]>,
                             lba: u8,
                             seed: u8| {
        let mut payload = Vec::with_capacity(8 * 4096);
        for i in 0..8 {
            payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i as u8)));
        }
        let start = 24 + lba as u64;
        if vol.write(start, &payload).is_ok() {
            for i in 0..8 {
                let mut block = [0u8; 4096];
                block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                oracle.insert(start + i as u64, block);
            }
        }
    };

    let assert_oracle =
        |vol: &mut Volume, oracle: &std::collections::HashMap<u64, [u8; 4096]>, step: &str| {
            for (&lba, expected) in oracle {
                let actual = vol.read(lba, 1).unwrap();
                assert_eq!(
                    actual.as_slice(),
                    expected.as_slice(),
                    "lba {lba} wrong after {step}"
                );
            }
        };

    write_large_multi(&mut vol, &mut oracle, 0, 102);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=0,seed=102)");

    write_large_multi(&mut vol, &mut oracle, 2, 102);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=2,seed=102)");

    let outcome = vol.reclaim_alias_merge(24, 1).unwrap();
    assert!(!outcome.discarded, "single-threaded driver: never discards");
    assert_oracle(
        &mut vol,
        &oracle,
        "ReclaimRange(start_lba=24,lba_count=1)#1",
    );

    write_large_multi(&mut vol, &mut oracle, 0, 0);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=0,seed=0)");

    let outcome = vol.reclaim_alias_merge(24, 1).unwrap();
    assert!(!outcome.discarded, "single-threaded driver: never discards");
    assert_oracle(
        &mut vol,
        &oracle,
        "ReclaimRange(start_lba=24,lba_count=1)#2",
    );

    let _ = vol.repack();
    assert_oracle(&mut vol, &oracle, "SweepPending");

    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    assert_oracle(&mut vol, &oracle, "Crash");
}

/// Deterministic reproduction for proptest seed
/// `3f9275b50fbcff2f3fa00c6b1b904cd9655ec341a5ebea5b2d350e38c43e07b4`,
/// shrunk by `reclaim_crash_recovery` to the 6-op sequence below.
///
/// Three `WriteLargeMulti` writes overlap on a multi-block payload that
/// shares the same composite `seed=88` content across two distinct
/// start LBAs (24 and 25), so the third write lands as a `DedupRef`
/// against the second write's body. After `reclaim_alias_merge` and
/// `drain_with_repack`, LBAs 25..32 read back blocks shifted by one
/// position relative to the oracle.
///
/// ## Root cause
///
/// `segment_classify::classify_entry` matches lbamap runs against the
/// input entry **by hash only**. When two same-hash entries anchor
/// the same body at *different* `start_lba`s (one direct DATA at
/// `start=24`, one DedupRef at `start=25`), partially-overlapping
/// LBAs are bytewise different — body block `i` of the first and
/// body block `i` of the second are distinct bytes — even though
/// the composite payload hash is identical.
///
/// The classifier currently counts every same-hash block as
/// "matching", so an entry that has been partially superseded by a
/// `DedupRef` at a different anchor classifies as `FullyLive` and is
/// passed through verbatim. When redact (or sweep / repack / GC)
/// rewrites under a fresh, *higher* ULID, the carried entry now
/// sorts above the DedupRef on rebuild, and `lbamap::insert_inner`
/// overwrites the DedupRef's anchor for the overlap range. Reads at
/// those LBAs resolve through the wrong anchor.
///
/// Fix: classifier must additionally check that, for each `ExtentRead`
/// in the lbamap, the anchor agrees with the input entry — i.e.
/// `r.range_start - entry.start_lba == r.payload_block_offset`. Runs
/// that match by hash but disagree on anchor are treated as
/// non-matching for the purpose of `matching_blocks`, so the entry
/// classifies as `PartialDeath` and is correctly split into surviving
/// sub-runs only.
///
/// **Currently failing — fix in subsequent commit on this branch.**
#[test]
fn reclaim_crash_recovery_seed_3f9275b5_regression() {
    use elide_core::volume::Volume;

    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();
    common::write_test_keypair(fork_dir);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    let mut oracle: std::collections::HashMap<u64, [u8; 4096]> = std::collections::HashMap::new();

    let write_large_multi = |vol: &mut Volume,
                             oracle: &mut std::collections::HashMap<u64, [u8; 4096]>,
                             lba: u8,
                             seed: u8| {
        let mut payload = Vec::with_capacity(8 * 4096);
        for i in 0..8 {
            payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i as u8)));
        }
        let start = 24 + lba as u64;
        if vol.write(start, &payload).is_ok() {
            for i in 0..8 {
                let mut block = [0u8; 4096];
                block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                oracle.insert(start + i as u64, block);
            }
        }
    };

    let assert_oracle =
        |vol: &mut Volume, oracle: &std::collections::HashMap<u64, [u8; 4096]>, step: &str| {
            for (&lba, expected) in oracle {
                let actual = vol.read(lba, 1).unwrap();
                assert_eq!(
                    actual.as_slice(),
                    expected.as_slice(),
                    "lba {lba} wrong after {step}"
                );
            }
        };

    write_large_multi(&mut vol, &mut oracle, 2, 0);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=2,seed=0)");

    write_large_multi(&mut vol, &mut oracle, 0, 88);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=0,seed=88)");

    vol.flush_wal().unwrap();
    assert_oracle(&mut vol, &oracle, "Flush");

    write_large_multi(&mut vol, &mut oracle, 1, 88);
    assert_oracle(&mut vol, &oracle, "WriteLargeMulti(lba=1,seed=88)");

    let outcome = vol.reclaim_alias_merge(28, 6).unwrap();
    assert!(!outcome.discarded, "single-threaded driver: never discards");
    assert_oracle(&mut vol, &oracle, "ReclaimRange(start_lba=28,lba_count=6)");

    common::drain_with_repack(&mut vol);
    assert_oracle(&mut vol, &oracle, "DrainWithRedact");
}

/// Deterministic regression for the failure surfaced by
/// `crash_recovery_oracle` after `SimOp::WriteMulti` was added in this
/// branch. Shrunk to the 7-op sequence below.
///
/// Two overlapping multi-LBA writes set up a content-dedup case across
/// distinct LBAs:
///   - First WriteMulti at LBA 47..49: writes `incompressible_block(0)`
///     and `incompressible_block(1)`.
///   - Second WriteMulti at LBA 46..48: writes `incompressible_block(1)`
///     and `incompressible_block(2)`, overwriting LBA 47.
/// LBA 46 (live, written second) and LBA 48 (live, written first) now
/// share content `incompressible_block(1)` — same hash, different LBAs,
/// in the same WAL window.
///
/// `GcCheckpoint` flushes the WAL into a pending segment.
/// `HalfPromotePending` performs the worker-phase file I/O for that
/// pending segment without the actor-phase apply, leaving the
/// extent-index in the mid-promote state. `PopulateFetched` writes a
/// directly-staged cache entry, then `GcApply` consumes the stashed
/// checkpoint. After `Crash`, LBA 48 reads back all-zeros instead of
/// `incompressible_block(1)`.
///
/// Root cause was in the test helper `simulate_coord_gc_local`'s
/// compactor: a point query (`lba_map.hash_at(start_lba)`) misclassified
/// any multi-LBA entry whose head LBA had been overwritten as fully
/// LBA-dead and demoted it to `Canonical`. Production
/// `elide-coordinator::gc::collect_stats` already used a full-range scan
/// via `extents_in_range`; the helper was rewritten to mirror that and
/// emit `PlanOutput::Run` for surviving sub-runs. Kept as a regression
/// guard.
#[test]
fn crash_recovery_writemulti_dedup_regression() {
    use elide_core::volume::Volume;

    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();
    common::write_test_keypair(fork_dir);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    let mut oracle: std::collections::HashMap<u64, [u8; 4096]> = std::collections::HashMap::new();

    // Mirrors `SimOp::WriteMulti` in the proptest body.
    let write_multi = |vol: &mut Volume,
                       oracle: &mut std::collections::HashMap<u64, [u8; 4096]>,
                       start_lba: u8,
                       lba_count: u8,
                       seed: u8| {
        let mut payload = Vec::with_capacity(lba_count as usize * 4096);
        for i in 0..lba_count {
            payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i)));
        }
        let start = 40 + start_lba as u64;
        if vol.write(start, &payload).is_ok() {
            for i in 0..lba_count as usize {
                let mut block = [0u8; 4096];
                block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                oracle.insert(start + i as u64, block);
            }
        }
    };

    write_multi(&mut vol, &mut oracle, 7, 2, 0);
    write_multi(&mut vol, &mut oracle, 6, 2, 1);

    // Drain before gc_checkpoint to match production sequencing — without
    // it the un-drained pending segments would violate
    // pending-above-committed once GC apply commits u_gc.
    common::drain_with_repack(&mut vol);

    let gc_ulid = vol.gc_checkpoint_for_test().unwrap();
    let _ = common::half_promote_first_pending(fork_dir);

    // Mirrors `SimOp::PopulateFetched { lba: 0, seed: 0 }`: writes a
    // direct-fetch cache entry at LBA 16 with effective_seed 0x80.
    // PopulateFetched also drains first; do the same here.
    common::drain_with_repack(&mut vol);
    let effective_seed: u8 = 0x80;
    let cache_ulid = vol.gc_checkpoint_for_test().unwrap();
    common::populate_cache(fork_dir, cache_ulid, 16, effective_seed);
    oracle.insert(16, [effective_seed; 4096]);

    // populate_cache writes directly to disk; drop+reopen so both
    // self.lbamap and self.extent_index pick up the populated entry —
    // without this, the next mutating op trips
    // `assert_lbamap_hashes_resolvable` (lbamap rebuilds from disk via
    // some apply paths, but extent_index doesn't, so lbamap[16] points
    // at a hash extent_index doesn't know about).
    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();

    // GcApply n=2.
    let to_delete =
        if let Some((_, _, paths)) = common::simulate_coord_gc_local(fork_dir, gc_ulid, 2) {
            paths
        } else {
            vec![]
        };
    let applied = vol.apply_gc_handoffs().unwrap_or(0);
    if applied > 0 {
        for path in &to_delete {
            let _ = std::fs::remove_file(path);
        }
    }

    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    common::assert_promote_recovery(&mut vol, fork_dir);
    for (&lba, expected) in &oracle {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "lba {lba} wrong after Crash"
        );
    }
}

/// Minimal isolated reproducer (no CoordGcLocal): two pending segments
/// — one from a prior flush, one from current WriteMulti payloads —
/// where the first WriteMulti's middle LBAs are still live but its
/// head LBA was overwritten by the second WriteMulti. Sweep merges
/// both pending segments; repack on the merged output loses bytes.
#[test]
fn writemulti_overlap_sweep_then_repack_regression() {
    use elide_core::volume::Volume;

    let tmp = tempfile::TempDir::new().unwrap();
    let fork_dir = tmp.path().join(Ulid::new().to_string());
    std::fs::create_dir_all(&fork_dir).unwrap();
    let fork_dir = fork_dir.as_path();
    let store_dir = tmp.path().join("_store");
    common::write_test_keypair(fork_dir);
    let mut vol = common::open_with_captured_body_fetcher(fork_dir, &store_dir);
    let mut oracle: std::collections::HashMap<u64, [u8; 4096]> = std::collections::HashMap::new();

    // Seed pending with one segment so sweep has a 2-input bucket
    // (sweep no-ops with <2 inputs).
    let _ = vol.write(0, &[0u8; 4096]);
    oracle.insert(0, [0u8; 4096]);
    vol.flush_wal().unwrap();

    let write_multi = |vol: &mut Volume,
                       oracle: &mut std::collections::HashMap<u64, [u8; 4096]>,
                       start_lba: u8,
                       lba_count: u8,
                       seed: u8| {
        let mut payload = Vec::with_capacity(lba_count as usize * 4096);
        for i in 0..lba_count {
            payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i)));
        }
        let start = 40 + start_lba as u64;
        if vol.write(start, &payload).is_ok() {
            for i in 0..lba_count as usize {
                let mut block = [0u8; 4096];
                block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                oracle.insert(start + i as u64, block);
            }
        }
    };

    write_multi(&mut vol, &mut oracle, 3, 4, 0);
    write_multi(&mut vol, &mut oracle, 2, 2, 0);

    let _ = vol.repack();
    let _ = vol.repack();

    drop(vol);
    let mut vol = common::open_with_captured_body_fetcher(fork_dir, &store_dir);
    common::assert_promote_recovery(&mut vol, fork_dir);
    for (&lba, expected) in &oracle {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "lba {lba} wrong after Crash"
        );
    }
}

/// Deterministic reproducer for `gc_interleaved_oracle` shrunk failure
/// (PR #291 CI run 25563279365). LBA 45 reads back zeros after Crash.
#[test]
fn gc_interleaved_writemulti_overlap_regression() {
    use elide_core::volume::Volume;

    let tmp = tempfile::TempDir::new().unwrap();
    let fork_dir = tmp.path().join(Ulid::new().to_string());
    std::fs::create_dir_all(&fork_dir).unwrap();
    let fork_dir = fork_dir.as_path();
    let store_dir = tmp.path().join("_store");
    common::write_test_keypair(fork_dir);
    let mut vol = common::open_with_captured_body_fetcher(fork_dir, &store_dir);
    let mut oracle: std::collections::HashMap<u64, [u8; 4096]> = std::collections::HashMap::new();

    // DedupWrite { lba_a: 0, lba_b: 4, seed: 0 }
    let data = [0u8; 4096];
    let _ = vol.write(0, &data);
    let _ = vol.write(4, &data);
    oracle.insert(0, data);
    oracle.insert(4, data);

    // CoordGcLocal { n: 2 }
    common::drain_with_repack(&mut vol);
    let gc_ulid = vol.gc_checkpoint_for_test().unwrap();
    let to_delete =
        if let Some((_, _, paths)) = common::simulate_coord_gc_local(fork_dir, gc_ulid, 2) {
            paths
        } else {
            vec![]
        };
    let applied = vol.apply_gc_handoffs().unwrap_or(0);
    if applied > 0 {
        for path in &to_delete {
            let _ = std::fs::remove_file(path);
        }
    }

    let write_multi = |vol: &mut Volume,
                       oracle: &mut std::collections::HashMap<u64, [u8; 4096]>,
                       start_lba: u8,
                       lba_count: u8,
                       seed: u8| {
        let mut payload = Vec::with_capacity(lba_count as usize * 4096);
        for i in 0..lba_count {
            payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i)));
        }
        let start = 40 + start_lba as u64;
        if vol.write(start, &payload).is_ok() {
            for i in 0..lba_count as usize {
                let mut block = [0u8; 4096];
                block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                oracle.insert(start + i as u64, block);
            }
        }
    };

    write_multi(&mut vol, &mut oracle, 3, 4, 0);
    write_multi(&mut vol, &mut oracle, 2, 2, 0);

    let _ = vol.repack();
    let _ = vol.repack();

    drop(vol);
    let mut vol = common::open_with_captured_body_fetcher(fork_dir, &store_dir);
    common::assert_promote_recovery(&mut vol, fork_dir);
    for (&lba, expected) in &oracle {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "lba {lba} wrong after Crash"
        );
    }
}

/// Deterministic reproducer for the `crash_recovery_oracle` failure
/// surfaced by CI run 26021377703 (seed
/// `cc b1c5223dc87a1a3d86f111dbae10b27598459aaf54ed53c506c84e6fa7d87237`).
///
/// Minimal failing op sequence:
///   WriteMulti{0,4,241}, CoordGcLocal{2}, SignSnapshot, Write{0,0},
///   DrainWithRedact, WriteMulti{1,4,241}, GcCheckpoint,
///   CoordGcLocal{2}, Crash
///
/// The corruption is observable on the *live* volume immediately after
/// the final CoordGcLocal{2}, before any crash: GC-local apply over a
/// snapshot-in-place segment that was partially overwritten by the
/// second WriteMulti loses the overwrite. Mirrors the
/// `crash_recovery_oracle` proptest arms exactly so the repro tracks
/// the harness.
#[test]
fn crash_recovery_seed_b1c5223d_regression() {
    use elide_core::volume::Volume;

    let tmp = tempfile::TempDir::new().unwrap();
    let fork_dir = tmp.path().join(Ulid::new().to_string());
    std::fs::create_dir_all(&fork_dir).unwrap();
    let fork_dir = fork_dir.as_path();
    let store_dir = tmp.path().join("_store");
    common::write_test_keypair(fork_dir);
    let mut vol = common::open_with_captured_body_fetcher(fork_dir, &store_dir);
    let mut oracle: std::collections::HashMap<u64, [u8; 4096]> = std::collections::HashMap::new();

    // SimOp::WriteMulti
    let write_multi = |vol: &mut Volume,
                       oracle: &mut std::collections::HashMap<u64, [u8; 4096]>,
                       start_lba: u8,
                       lba_count: u8,
                       seed: u8| {
        let mut payload = Vec::with_capacity(lba_count as usize * 4096);
        for i in 0..lba_count {
            payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i)));
        }
        let start = 40 + start_lba as u64;
        if vol.write(start, &payload).is_ok() {
            for i in 0..lba_count as usize {
                let mut block = [0u8; 4096];
                block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                oracle.insert(start + i as u64, block);
            }
        }
    };

    // SimOp::CoordGcLocal
    let coord_gc_local = |vol: &mut Volume, n: usize| {
        common::drain_with_repack(vol);
        let gc_ulid = vol.gc_checkpoint_for_test().unwrap();
        let to_delete =
            if let Some((_, _, paths)) = common::simulate_coord_gc_local(fork_dir, gc_ulid, n) {
                paths
            } else {
                vec![]
            };
        let applied = vol.apply_gc_handoffs().unwrap_or(0);
        if applied > 0 {
            for path in &to_delete {
                let _ = std::fs::remove_file(path);
            }
        }
    };

    write_multi(&mut vol, &mut oracle, 0, 4, 241);
    coord_gc_local(&mut vol, 2);

    // SimOp::SignSnapshot
    let snap_ulid = pick_snap_ulid(fork_dir);
    let _ = vol.sign_snapshot_manifest(snap_ulid);

    // SimOp::Write { lba: 0, seed: 0 }
    let data = [0u8; 4096];
    let _ = vol.write(0, &data);
    oracle.insert(0, data);

    // SimOp::DrainWithRedact
    common::drain_with_repack(&mut vol);

    write_multi(&mut vol, &mut oracle, 1, 4, 241);

    // SimOp::GcCheckpoint
    common::drain_with_repack(&mut vol);
    let _ = vol.gc_checkpoint_for_test().unwrap();

    // SimOp::CoordGcLocal { n: 2 } — invalidates the stashed checkpoint
    coord_gc_local(&mut vol, 2);

    // SimOp::Crash
    drop(vol);
    let mut vol = common::open_with_captured_body_fetcher(fork_dir, &store_dir);
    common::assert_promote_recovery(&mut vol, fork_dir);
    for (&lba, expected) in &oracle {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "lba {lba} wrong after crash+rebuild"
        );
    }
}
