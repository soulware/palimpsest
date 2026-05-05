// Property-based tests for volume ULID ordering and crash-recovery correctness.
//
// Invariant tested by `ulid_monotonicity`:
//   Every segment created by a volume operation (flush, sweep_pending) has a
//   ULID strictly greater than all segment ULIDs that existed before the
//   operation. For simulated coordinator GC the invariant is narrower: the
//   output ULID exceeds the maximum of the consumed input ULIDs.
//
// Invariant tested by `crash_recovery_oracle`:
//   After any sequence of volume operations + coordinator GC + crash, every
//   LBA reads back exactly the value last written to it. This directly tests
//   that no combination of operations can produce a stale read after rebuild.
//
// Together these two properties guarantee that rebuild always applies segments
// in write order and that crash recovery is always correct.

use std::fs;
use std::path::Path;

use elide_core::volume::Volume;
use proptest::prelude::*;
use ulid::Ulid;

mod common;

// --- simulation helpers ---

/// Collect every ULID-named file across wal/, pending/, and index/*.idx.
fn all_segment_ulids(fork_dir: &Path) -> std::collections::BTreeSet<Ulid> {
    let mut result = std::collections::BTreeSet::new();
    for subdir in ["wal", "pending"] {
        let dir = fork_dir.join(subdir);
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str()
                    && let Ok(u) = Ulid::from_string(name)
                {
                    result.insert(u);
                }
            }
        }
    }
    // Include index/*.idx stems so ULID monotonicity assertions cover demand-fetched segments.
    if let Ok(entries) = fs::read_dir(fork_dir.join("index")) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str()
                && let Some(stem) = name.strip_suffix(".idx")
                && let Ok(u) = Ulid::from_string(stem)
            {
                result.insert(u);
            }
        }
    }
    result
}

/// Pick the largest ULID present in `index/*.idx`, or mint a fresh
/// one if `index/` is empty. Mirrors the snapshot-ULID selection in
/// `actor_proptest.rs` so volume-level signing tests cover both the
/// "snapshot over real data" and "degenerate empty manifest" shapes.
fn pick_snap_ulid(fork_dir: &Path) -> Ulid {
    fs::read_dir(fork_dir.join("index"))
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let s = name.to_str()?;
            let stem = s.strip_suffix(".idx")?;
            Ulid::from_string(stem).ok()
        })
        .max()
        .unwrap_or_else(Ulid::new)
}

/// Generate 4096 bytes of incompressible data from a seed.
///
/// Uses blake3 in counter mode to fill the block with pseudo-random bytes.
/// The result does not compress below `INLINE_THRESHOLD` (4096), so it is
/// stored in the segment body section rather than inline.
fn incompressible_block(seed: u8) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let key = [seed; 32];
    let mut hasher = blake3::Hasher::new_keyed(&key);
    for (i, chunk) in buf.chunks_mut(32).enumerate() {
        hasher.update(&(i as u64).to_le_bytes());
        let hash = hasher.finalize();
        chunk.copy_from_slice(&hash.as_bytes()[..chunk.len()]);
        hasher.reset();
    }
    buf
}

// --- strategy ---

#[derive(Debug, Clone)]
enum SimOp {
    /// Write data to an LBA directly on the volume (no actor channel).
    Write { lba: u8, seed: u8 },
    /// Flush the WAL to a pending/ segment, directly on the volume.
    Flush,
    /// Volume-level sweep: packs small segments from pending/ into a single
    /// new segment. Calls vol.sweep_pending() directly, bypassing the actor
    /// channel. Exercises ULID monotonicity and crash-recovery invariants.
    SweepPending,
    /// Volume-level density pass: rewrites sparse segments from pending/.
    /// Analogous to the coordinator's repack pass but runs in-process on the
    /// volume, bypassing the actor channel.
    Repack,
    /// Exercises the full redact → promote path:
    /// redact_segment (in-place hash-dead DATA hole-punching) then
    /// promote_segment (extracts .idx + cache body, updates extent index,
    /// publishes snapshot).
    DrainWithRedact,
    /// Simulate one coordinator GC sweep pass directly on the filesystem,
    /// using `n` segments as input. Exercises ULID monotonicity and
    /// crash-recovery invariants for the coordinator GC path.
    CoordGcLocal { n: usize },
    /// Phase 1 of split GC: flush WAL, mint GC ULIDs, open fresh WAL.
    /// Stashes ULIDs — any Write/Flush ops before the matching GcApply
    /// model writes that arrive during the coordinator's GC window.
    GcCheckpoint,
    /// Phase 2 of split GC: build liveness snapshot (including WAL replay),
    /// compact, apply handoffs. Requires a prior GcCheckpoint; no-ops if
    /// no checkpoint is pending.
    GcApply { n: usize },
    /// Simulate a crash: drop the Volume, reopen it (triggering WAL recovery),
    /// and assert all oracle LBAs read back their last-written values.
    Crash,
    /// Read an LBA that has never been written; must return all-zero bytes.
    ReadUnwritten,
    /// Take a snapshot; segments at or below the returned ULID are frozen.
    Snapshot,
    /// Write a single block directly to cache/ in the three-file format
    /// (bypassing the WAL), simulating a demand-fetch result from S3.
    ///
    /// LBAs are offset by 16 (range 16–23) to stay disjoint from Write (0–7)
    /// and ReadUnwritten (64).  The oracle is updated immediately because after
    /// any subsequent Crash the rebuilt volume will serve this data from cache/.
    PopulateFetched { lba: u8, seed: u8 },
    /// Write [seed; 4096] to lba_a, then [seed; 4096] to lba_b.
    /// Because both writes carry identical data, lba_b's WAL entry is a
    /// DEDUP_REF pointing at lba_a's segment.  Guarantees the dedup and
    /// dead-REF paths are exercised on every run that includes this op.
    DedupWrite { lba_a: u8, lba_b: u8, seed: u8 },
    /// Write a single block of incompressible (hash-derived) data.
    ///
    /// Unlike `Write` — whose `[seed; 4096]` pattern compresses to a few bytes
    /// and always lands in the inline section — `WriteLarge` produces data that
    /// does not compress below `INLINE_THRESHOLD`, so it is stored in the body
    /// section.  This exercises the body read path through GC, crash, and reopen.
    ///
    /// LBAs are in range 24..32, disjoint from Write (0..8), WriteZeroes (8..16),
    /// PopulateFetched (16..24), and ReadUnwritten (64).
    WriteLarge { lba: u8, seed: u8 },
    /// Zero a single LBA.
    ///
    /// LBAs are in range 8..16 to stay disjoint from Write (0..8),
    /// PopulateFetched (16..23), and ReadUnwritten (64). The oracle is
    /// updated immediately with zeros; a subsequent Crash verifies the
    /// zero survives WAL recovery and rebuild.
    WriteZeroes { lba: u8 },
    /// Write `[seed; 4096]` to `lba`, then immediately write the same
    /// bytes to the same LBA again. The second write must short-circuit
    /// via the no-op skip path (LBA-map hash compare — lbamap already
    /// holds the hash after the first write), so `noop_stats().skipped_writes`
    /// must strictly increase across the second call.
    ///
    /// Randomly interleaved with SweepPending / Repack / DrainWithRedact /
    /// CoordGcLocal, this also covers the load-bearing case where the skip
    /// fires against post-transform extent state — a later SameContentWrite
    /// on the same LBA will find lbamap still consistent after drain/GC.
    ///
    /// LBAs 32..40 are disjoint from Write (0..8), WriteZeroes (8..16),
    /// PopulateFetched (16..24), WriteLarge (24..32), and ReadUnwritten (64).
    SameContentWrite { lba: u8, seed: u8 },
    /// Run alias-merge extent reclamation over a range. Exercises the
    /// full three-phase primitive (phase 1 snapshot, phase 2 compute
    /// outside the lock, phase 3 pointer-precondition commit) directly
    /// on the volume. Single-threaded, so phase 3 never discards — the
    /// discard path is covered by deterministic unit tests in
    /// `volume.rs`. What this covers in properties is the hard
    /// invariant that after a reclaim pass every LBA still reads back
    /// its oracle value, both immediately and across subsequent
    /// GC/crash combinations.
    ReclaimRange { start_lba: u8, lba_count: u8 },
    /// Simulate a crash between `promote_segment`'s worker-phase file I/O
    /// (extract_idx + promote_to_cache) and its actor-phase apply
    /// (extent-index CAS + pending delete). Runs only the file writes for
    /// one pending segment, leaving the volume in the mid-apply state a
    /// process crash would produce under the planned promote_segment
    /// offload. Exercised against a subsequent `Crash` + rebuild so the
    /// crash-recovery invariants fire on this specific shape.
    HalfPromotePending,
    /// Phase 5 Tier 1 dictionary-delta repack of post-snapshot pending
    /// segments. No-op when there is no sealed snapshot or no Data
    /// entries match same-LBA extents in the prior snapshot. Segment
    /// files are rewritten in place under their original ULID via
    /// atomic rename, so no new ULID files appear. The actor-layer
    /// proptest covers this through `ActorOp::DeltaRepack`; modelling
    /// it here exercises the same invariants directly against the
    /// volume layer (ULID set unchanged, oracle preserved across
    /// subsequent crash + rebuild).
    DeltaRepack,
    /// Seal a snapshot manifest over the current `index/` set: pick
    /// the max ULID in `index/` (or a fresh one if empty) and call
    /// `vol.sign_snapshot_manifest`. Writes `snapshots/<ulid>.manifest`
    /// and the `snapshots/<ulid>` marker — neither file lands in the
    /// ULID-tracking directories (`wal/`, `pending/`, `index/`), so
    /// the segment-ULID set is unaffected. Signing must not touch
    /// data paths; oracle preservation is verified by the next Crash.
    SignSnapshot,
    /// Write `lba_count` (2..=4) contiguous incompressible blocks
    /// starting at `40 + start_lba`. Unlike `Write` (single LBA,
    /// inline-compressible) and `WriteLarge` (single LBA, body), this
    /// exercises the multi-block write/read path: a single payload
    /// produces multiple body-section entries, and reads with
    /// `lba_count > 1` traverse the partial-range
    /// `payload_block_offset` arithmetic. Frequent overlaps with
    /// later WriteMulti / Write ops on the same range create the
    /// mixed live/dead partial-extent shapes that
    /// `sweep_pending`/`repack` need to handle.
    ///
    /// LBA range 40..52 — disjoint from Write (0..8), WriteZeroes
    /// (8..16), PopulateFetched (16..24), WriteLarge (24..32),
    /// SameContentWrite (32..40), and ReadUnwritten (64).
    WriteMulti {
        start_lba: u8,
        lba_count: u8,
        seed: u8,
    },
}

fn arb_sim_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        // Write and DedupWrite use seeds 0..=127 (bit 7 clear).
        // PopulateFetched effective seeds are always 128..=255 (bit 7 set).
        // This partitions the hash space so that populate_cache data can never
        // collide with data written through the volume's write path, mirroring
        // the production invariant that the fetcher only caches data that was
        // previously written through the volume (and thus already indexed).
        //
        // NOTE: Write always produces **inline** entries — [seed; 4096] (all
        // same byte) compresses to ~20 bytes, well below INLINE_THRESHOLD
        // (4096).  The body-entry lifecycle is exercised by WriteLarge below.
        (0u8..8, 0u8..128u8).prop_map(|(lba, seed)| SimOp::Write { lba, seed }),
        Just(SimOp::Flush),
        Just(SimOp::SweepPending),
        Just(SimOp::Repack),
        Just(SimOp::DrainWithRedact),
        (2usize..=5).prop_map(|n| SimOp::CoordGcLocal { n }),
        Just(SimOp::GcCheckpoint),
        (2usize..=5).prop_map(|n| SimOp::GcApply { n }),
        Just(SimOp::Crash),
        Just(SimOp::ReadUnwritten),
        Just(SimOp::Snapshot),
        (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::PopulateFetched { lba, seed }),
        (0u8..4, 4u8..8, 0u8..128u8).prop_map(|(lba_a, lba_b, seed)| SimOp::DedupWrite {
            lba_a,
            lba_b,
            seed
        }),
        (8u8..16).prop_map(|lba| SimOp::WriteZeroes { lba }),
        (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::WriteLarge { lba, seed }),
        (0u8..8, 0u8..128u8).prop_map(|(lba, seed)| SimOp::SameContentWrite { lba, seed }),
        (0u8..8, 1u8..8u8).prop_map(|(start_lba, lba_count)| SimOp::ReclaimRange {
            start_lba,
            lba_count,
        }),
        Just(SimOp::HalfPromotePending),
        Just(SimOp::DeltaRepack),
        Just(SimOp::SignSnapshot),
        (0u8..8, 2u8..=4, any::<u8>()).prop_map(|(start_lba, lba_count, seed)| {
            SimOp::WriteMulti {
                start_lba,
                lba_count,
                seed,
            }
        }),
    ]
}

/// Minimal op set for the reclaim-focused proptest. Deliberately does
/// **not** include `PopulateFetched`: it drives on-disk cache files that are
/// only visible via rebuild (not through the in-memory map), and expanding
/// into it from a reclaim test leaks orthogonal crash-recovery invariants.
///
/// The goal of this enum is one hard invariant: **reclaim never corrupts
/// observable content**. Every op here leaves the volume in a state where
/// `vol.read(lba, 1)` is authoritative against the oracle.
#[derive(Debug, Clone)]
enum ReclaimOp {
    /// Single-block write — seeds 0..=127, bit 7 clear.
    Write {
        lba: u8,
        seed: u8,
    },
    /// 8-block write of incompressible body-section data at
    /// `24 + lba` (disjoint from the 1-block Write range 0..8).
    /// This is the shape that actually exposes bloat: large bodies
    /// get split by subsequent overwrites.
    WriteLargeMulti {
        lba: u8,
        seed: u8,
    },
    /// Write `[seed; 4096]` to `lba_a`, then same bytes to `lba_b`
    /// (triggers DedupRef on the second).
    DedupWrite {
        lba_a: u8,
        lba_b: u8,
        seed: u8,
    },
    Flush,
    DrainWithRedact,
    SweepPending,
    Repack,
    /// Alias-merge reclaim over a sub-range of the Write/WriteLargeMulti area.
    ReclaimRange {
        start_lba: u8,
        lba_count: u8,
    },
    Crash,
}

fn arb_reclaim_op() -> impl Strategy<Value = ReclaimOp> {
    prop_oneof![
        3 => (0u8..8, 0u8..128u8).prop_map(|(lba, seed)| ReclaimOp::Write { lba, seed }),
        3 => (0u8..6, 0u8..128u8).prop_map(|(lba, seed)| ReclaimOp::WriteLargeMulti { lba, seed }),
        2 => (0u8..4, 4u8..8, 0u8..128u8).prop_map(|(lba_a, lba_b, seed)| ReclaimOp::DedupWrite {
            lba_a,
            lba_b,
            seed,
        }),
        1 => Just(ReclaimOp::Flush),
        1 => Just(ReclaimOp::DrainWithRedact),
        1 => Just(ReclaimOp::SweepPending),
        1 => Just(ReclaimOp::Repack),
        // Weight reclaim higher so sequences hit it multiple times against
        // progressively more fragmented state.
        4 => (24u8..32, 1u8..8u8)
            .prop_map(|(start_lba, lba_count)| ReclaimOp::ReclaimRange { start_lba, lba_count }),
        4 => (0u8..8, 1u8..8u8)
            .prop_map(|(start_lba, lba_count)| ReclaimOp::ReclaimRange { start_lba, lba_count }),
        1 => Just(ReclaimOp::Crash),
    ]
}

fn arb_reclaim_ops() -> impl Strategy<Value = Vec<ReclaimOp>> {
    prop::collection::vec(arb_reclaim_op(), 1..30)
}

fn arb_sim_ops() -> impl Strategy<Value = Vec<SimOp>> {
    prop::collection::vec(arb_sim_op(), 1..40)
}

/// Two segments drained to `index/` — CoordGcLocal has material to compact.
fn two_segment_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x0A },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Write { lba: 1, seed: 0x0B },
        SimOp::Flush,
        SimOp::DrainWithRedact,
    ]
}

/// Two segments drained then a Snapshot taken — snapshot floor is non-null
/// from the start, so every subsequent SweepPending/Repack exercises the floor
/// guard rather than hitting the `floor.is_none()` fast path.
fn snapshot_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 2, seed: 0x11 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Write { lba: 3, seed: 0x22 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Snapshot,
    ]
}

/// Two flushes left in `pending/` without draining — exercises sweep_pending
/// and repack when data has not yet crossed the drain boundary.
fn pending_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 4, seed: 0x33 },
        SimOp::Flush,
        SimOp::Write { lba: 5, seed: 0x44 },
        SimOp::Flush,
    ]
}

/// Dedup write drained via redact → promote, then overwritten so GC has
/// dead entries. Exercises the full redact + GC path with thin DedupRefs.
fn dedup_redact_gc_prefix() -> Vec<SimOp> {
    vec![
        // Write canonical data and flush.
        SimOp::Write { lba: 0, seed: 0xD0 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        // Write same data to different LBA — creates thin DedupRef.
        SimOp::DedupWrite {
            lba_a: 1,
            lba_b: 2,
            seed: 0xD0,
        },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        // Overwrite LBA 0 to make the first segment's data dead for GC.
        SimOp::Write { lba: 0, seed: 0xD1 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
    ]
}

/// Three segments drained to `index/` — allows CoordGcLocal { n: 3 } to
/// fire so the n=3..=5 range is exercised rather than always no-opping.
fn multi_segment_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x55 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Write { lba: 1, seed: 0x66 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Write { lba: 2, seed: 0x77 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
    ]
}

/// One low-density segment + two small high-density segments — sets up state
/// where the merged GC pass packs a sparse large with small fully-live
/// segments in one bucket.
///
/// S1 (LBA 0 = 0xAA) is overwritten by S2 (LBA 0 = 0xBB), making S1
/// low-density.  S3 (LBA 1) and S4 (LBA 2) are small and fully live.
/// CoordGcLocal can fire on all four: S1 is the sparse-large filler,
/// S2+S3+S4 are smalls.
fn repack_and_sweep_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x14 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Write { lba: 0, seed: 0x15 }, // overwrites LBA 0 — S1 becomes stale
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Write { lba: 1, seed: 0x16 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Write { lba: 2, seed: 0x17 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
    ]
}

/// One full GC pass already applied — tests the "second round of GC" path and
/// rebuild from a volume that already has GC history.
fn post_gc_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x28 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::Write { lba: 1, seed: 0x29 },
        SimOp::Flush,
        SimOp::DrainWithRedact,
        SimOp::CoordGcLocal { n: 2 },
    ]
}

fn with_prefix(prefix: Vec<SimOp>, ops: Vec<SimOp>) -> Vec<SimOp> {
    let mut v = prefix;
    v.extend(ops);
    v
}

fn arb_gc_interleaved_ops() -> impl Strategy<Value = Vec<SimOp>> {
    prop_oneof![
        // No prefix: cold-start and pathological early sequences.
        arb_sim_ops(),
        // Two drained segments: CoordGcLocal has material.
        arb_sim_ops().prop_map(|ops| with_prefix(two_segment_prefix(), ops)),
        // Snapshot in place: floor guard fires from the first SweepPending/Repack.
        arb_sim_ops().prop_map(|ops| with_prefix(snapshot_prefix(), ops)),
        // Pending-only: data in pending/ not yet drained to index/ + cache/.
        arb_sim_ops().prop_map(|ops| with_prefix(pending_prefix(), ops)),
        // Three drained segments: CoordGcLocal { n: 3..=5 } can fire.
        arb_sim_ops().prop_map(|ops| with_prefix(multi_segment_prefix(), ops)),
        // Post-GC: one GC pass already applied, tests second-round GC path.
        arb_sim_ops().prop_map(|ops| with_prefix(post_gc_prefix(), ops)),
        // One low-density + two small dense segments: the merged pass can
        // pack all four into a single bucket (sparse-large filler + smalls).
        arb_sim_ops().prop_map(|ops| with_prefix(repack_and_sweep_prefix(), ops)),
        // Dedup + redact + GC: exercises the redact → drain → GC pipeline with thin DedupRefs.
        arb_sim_ops().prop_map(|ops| with_prefix(dedup_redact_gc_prefix(), ops)),
    ]
}

// --- property tests ---

proptest! {
    #[test]
    fn ulid_monotonicity(ops in arb_sim_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();
        common::write_test_keypair(fork_dir);
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
        // Tracks the latest snapshot ULID; segments at or below this are frozen.
        let mut snapshot_floor: Option<Ulid> = None;
        // Pending GcCheckpoint ULIDs awaiting GcApply.
        let mut pending_gc: Option<Ulid> = None;

        for op in &ops {
            let ulids_before = all_segment_ulids(fork_dir);
            let max_before = ulids_before
                .iter()
                .copied()
                .max()
                .unwrap_or(Ulid::from_parts(0, 0));

            match op {
                SimOp::Write { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba as u64, &data);
                }
                SimOp::Flush => {
                    let _ = vol.flush_wal();
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "flush produced ULID {u} ≤ existing max {max_before}"
                        );
                    }
                }
                SimOp::SweepPending => {
                    let frozen_before: std::collections::BTreeSet<Ulid> =
                        if let Some(floor) = snapshot_floor {
                            ulids_before.iter().copied().filter(|u| *u <= floor).collect()
                        } else {
                            Default::default()
                        };
                    let _ = vol.sweep_pending();
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "sweep_pending produced ULID {u} ≤ existing max {max_before}"
                        );
                    }
                    for u in &frozen_before {
                        prop_assert!(
                            after.contains(u),
                            "sweep_pending deleted frozen segment {u} (floor {:?})",
                            snapshot_floor
                        );
                    }
                }
                SimOp::Repack => {
                    let frozen_before: std::collections::BTreeSet<Ulid> =
                        if let Some(floor) = snapshot_floor {
                            ulids_before.iter().copied().filter(|u| *u <= floor).collect()
                        } else {
                            Default::default()
                        };
                    // Use a high threshold so any segment with dead bytes is a
                    // candidate, maximising the chance of actually repacking.
                    let _ = vol.repack(0.9);
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "compact produced ULID {u} ≤ existing max {max_before}"
                        );
                    }
                    for u in &frozen_before {
                        prop_assert!(
                            after.contains(u),
                            "repack deleted frozen segment {u} (floor {:?})",
                            snapshot_floor
                        );
                    }
                }
                SimOp::DrainWithRedact => {
                    common::drain_with_redact(&mut vol);
                }
                SimOp::CoordGcLocal { n } => {
                    // A complete GC pass invalidates any previously-stashed
                    // `GcCheckpoint` ULIDs. Production enforces one GC pass
                    // per tick; the stash-then-apply split is for writes
                    // during a single GC window, not for another full GC
                    // running in between. Without this clear, the stashed
                    // low ULID would be used later for an output whose
                    // content is post-`CoordGcLocal`, breaking the
                    // rebuild-ordering invariant in
                    // docs/design-gc-ulid-ordering.md.
                    pending_gc = None;
                    let gc_ulid = vol.gc_checkpoint_for_test().unwrap();
                    // Core invariant: the volume mint must have advanced past the
                    // GC output ULID, so the next WAL flush produces a segment that
                    // sorts above it.  This is the property the pre-fix bug violated
                    // — the actor used Ulid::new() (system clock) instead of the
                    // volume's mint, so WAL segments could sort below GC outputs.
                    //
                    // Diff against post-checkpoint state (not ulids_before) so that
                    // W_new — the WAL opened inside gc_checkpoint with ULID between
                    // W_old and u_gc — is excluded.  Only the WAL opened by *this*
                    // flush_wal (W_new2, which must be > u_gc) appears in the diff.
                    let ulids_after_checkpoint = all_segment_ulids(fork_dir);
                    vol.flush_wal().unwrap();
                    let after_flush = all_segment_ulids(fork_dir);
                    for u in after_flush.difference(&ulids_after_checkpoint) {
                        prop_assert!(
                            *u > gc_ulid,
                            "WAL segment after gc_checkpoint sorts below GC output: {u} ≤ {gc_ulid}"
                        );
                    }
                    let to_delete = if let Some((consumed, produced, paths)) =
                        common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                    {
                        let max_consumed = consumed.iter().copied().max().unwrap();
                        prop_assert!(
                            produced > max_consumed,
                            "coord_gc produced ULID {produced} ≤ consumed max {max_consumed}"
                        );
                        paths
                    } else {
                        vec![]
                    };
                    // Apply any pending gc handoffs (from this pass or a
                    // pre-crash pass) through the volume's handoff path.
                    // Only delete consumed segment files when the handoff was
                    // actually applied (returned > 0).  If Bug B cancelled the
                    // GC, the consumed files must survive for the next tick.
                    let applied = vol.apply_gc_handoffs().unwrap_or(0);
                    if applied > 0 {
                        for path in &to_delete {
                            let _ = std::fs::remove_file(path);
                        }
                    }
                }
                SimOp::GcCheckpoint => {
                    let u_gc = vol.gc_checkpoint_for_test().unwrap();
                    pending_gc = Some(u_gc);
                }
                SimOp::GcApply { n } => {
                    // Apply the stashed checkpoint. Between GcCheckpoint and
                    // here, proptest may have injected Write/DedupWrite/Flush
                    // ops — this is the interleaving we want to exercise.
                    //
                    // No ULID ordering assertion: the checkpoint ULIDs were
                    // minted earlier, so segments created between checkpoint
                    // and apply may have higher ULIDs. The stale-liveness
                    // check inside apply_gc_handoffs is the real guard.
                    if let Some(gc_ulid) = pending_gc.take() {
                        let to_delete = if let Some((_, _, paths)) =
                            common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                        {
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
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
                    pending_gc = None;
                    // No assertion here: the next Flush or SweepPending
                    // will verify that the mint was correctly reseeded.
                }
                SimOp::ReadUnwritten => {
                    // LBA 64 is outside the Write range (0..8) so it is
                    // always unwritten; the volume must return all zeros.
                    let actual = vol.read(64, 1).unwrap();
                    prop_assert_eq!(
                        actual.as_slice(),
                        [0u8; 4096].as_slice(),
                        "unwritten lba 64 returned non-zero data"
                    );
                }
                SimOp::Snapshot => {
                    if let Ok(u) = vol.snapshot() {
                        snapshot_floor = Some(u);
                    }
                }
                SimOp::PopulateFetched { lba, seed } => {
                    // gc_checkpoint flushes the WAL (may create a pending segment) then
                    // mints two fresh ULIDs; we use the first for the cache file.  All
                    // new ULIDs must be > max_before.
                    //
                    // effective_seed always has bit 7 set (128..=255) so it never
                    // collides with Write/DedupWrite seeds (0..=127, bit 7 clear).
                    // Bits 6-4 encode lba (0..7), bits 3-0 vary within the lba slot.
                    let effective_seed = 0x80u8 | ((*lba & 0x07) << 4) | (*seed & 0x0F);
                    let ulid = vol.gc_checkpoint_for_test().unwrap();
                    common::populate_cache(fork_dir, ulid, 16 + *lba as u64, effective_seed);
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "PopulateFetched produced out-of-order ULID: {u} <= {max_before}"
                        );
                    }
                }
                SimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                }
                SimOp::WriteZeroes { lba } => {
                    // write_zeroes appends to the WAL in-place; no new ULID file
                    // is created, so no ULID ordering assertion is needed here.
                    let _ = vol.write_zeroes(*lba as u64, 1);
                }
                SimOp::WriteLarge { lba, seed } => {
                    let data = incompressible_block(*seed);
                    let _ = vol.write(24 + *lba as u64, &data);
                }
                SimOp::SameContentWrite { lba, seed } => {
                    let data = [*seed; 4096];
                    let actual_lba = 32 + *lba as u64;
                    let _ = vol.write(actual_lba, &data);
                    let mid = vol.noop_stats().skipped_writes;
                    let _ = vol.write(actual_lba, &data);
                    let after = vol.noop_stats().skipped_writes;
                    prop_assert!(
                        after > mid,
                        "second same-content write at lba {actual_lba} did not skip \
                         (skipped_writes mid={mid} after={after})"
                    );
                }
                SimOp::ReclaimRange { start_lba, lba_count } => {
                    // Reclaim writes directly to pending/ under a fresh
                    // mint ULID (bypassing the WAL), so it must preserve
                    // ULID monotonicity like any other segment-producing op.
                    let _ = vol.reclaim_alias_merge(*start_lba as u64, *lba_count as u32);
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "reclaim produced ULID {u} ≤ existing max {max_before}"
                        );
                    }
                }
                SimOp::HalfPromotePending => {
                    // Produces no new ULIDs — idx + cache body keep the
                    // original pending ULID. Nothing to assert for monotonicity.
                    let _ = common::half_promote_first_pending(fork_dir);
                }
                SimOp::DeltaRepack => {
                    // Rewrites post-snapshot pending segments in place
                    // under their original ULID via atomic rename. The
                    // ULID set must be unchanged: no new ULIDs appear
                    // and no existing ULID is dropped by this op.
                    let _ = vol.delta_repack_post_snapshot();
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            false,
                            "delta_repack produced unexpected new ULID {u}"
                        );
                    }
                    for u in ulids_before.difference(&after) {
                        prop_assert!(
                            false,
                            "delta_repack dropped existing ULID {u}"
                        );
                    }
                }
                SimOp::SignSnapshot => {
                    // Manifest + marker land under `snapshots/`, which
                    // `all_segment_ulids` does not scan. The segment
                    // ULID set must therefore be identical before/after.
                    let snap_ulid = pick_snap_ulid(fork_dir);
                    let _ = vol.sign_snapshot_manifest(snap_ulid);
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            false,
                            "sign_snapshot_manifest produced unexpected new ULID {u}"
                        );
                    }
                    for u in ulids_before.difference(&after) {
                        prop_assert!(
                            false,
                            "sign_snapshot_manifest dropped existing ULID {u}"
                        );
                    }
                }
                SimOp::WriteMulti {
                    start_lba,
                    lba_count,
                    seed,
                } => {
                    // Append-only WAL write — like Write/WriteLarge,
                    // produces no new ULID files until Flush.
                    let mut payload = Vec::with_capacity(*lba_count as usize * 4096);
                    for i in 0..*lba_count {
                        payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i)));
                    }
                    let _ = vol.write(40 + *start_lba as u64, &payload);
                }
            }
        }
    }

    /// Oracle-based crash recovery test.
    ///
    /// Maintains a ground-truth map of the most recent data written to each
    /// LBA. After every Crash + reopen, asserts that every LBA reads back
    /// exactly the value last written to it. This directly tests that no
    /// sequence of volume operations + GC + crash can produce a stale read.
    #[test]
    fn crash_recovery_oracle(ops in arb_sim_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();
        common::write_test_keypair(fork_dir);
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
        let mut oracle: std::collections::HashMap<u64, [u8; 4096]> =
            std::collections::HashMap::new();
        let mut pending_gc: Option<Ulid> = None;

        for op in &ops {
            match op {
                SimOp::Write { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba as u64, &data);
                    oracle.insert(*lba as u64, data);
                }
                SimOp::Flush => {
                    let _ = vol.flush_wal();
                }
                SimOp::SweepPending => {
                    let _ = vol.sweep_pending();
                }
                SimOp::Repack => {
                    let _ = vol.repack(0.9);
                }
                SimOp::DrainWithRedact => {
                    common::drain_with_redact(&mut vol);
                }
                SimOp::CoordGcLocal { n } => {
                    // See ulid_monotonicity's CoordGcLocal — a full GC
                    // pass invalidates any stashed `GcCheckpoint`.
                    pending_gc = None;
                    let gc_ulid = vol.gc_checkpoint_for_test().unwrap();
                    let to_delete = if let Some((_, _, paths)) =
                        common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                    {
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
                }
                SimOp::GcCheckpoint => {
                    let u_gc = vol.gc_checkpoint_for_test().unwrap();
                    pending_gc = Some(u_gc);
                }
                SimOp::GcApply { n } => {
                    if let Some(gc_ulid) = pending_gc.take() {
                        let to_delete = if let Some((_, _, paths)) =
                            common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                        {
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
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
                    pending_gc = None;
                    // For any pending/<ulid> whose cache/<ulid>.body survived
                    // (a HalfPromotePending before this crash), retrying
                    // promote_segment must finish the apply: delete pending,
                    // transition extent-index entries to Cached. The retry
                    // runs inline with prop_assert via the helper.
                    common::assert_promote_recovery(&mut vol, fork_dir);
                    for (&lba, expected) in &oracle {
                        let actual = vol.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            expected.as_slice(),
                            "lba {} wrong after crash+rebuild",
                            lba
                        );
                    }
                }
                SimOp::ReadUnwritten => {
                    // LBA 64 is outside the Write range (0..8) so it is
                    // always unwritten; the volume must return all zeros.
                    let actual = vol.read(64, 1).unwrap();
                    prop_assert_eq!(
                        actual.as_slice(),
                        [0u8; 4096].as_slice(),
                        "unwritten lba 64 returned non-zero data"
                    );
                }
                SimOp::Snapshot => {
                    // Snapshot does not change readable data; no oracle update.
                    let _ = vol.snapshot();
                }
                SimOp::HalfPromotePending => {
                    // Simulates the mid-apply crash state; the invariant check
                    // that verifies recovery runs inside the next `Crash`
                    // handler (before the oracle read-back loop).
                    let _ = common::half_promote_first_pending(fork_dir);
                }
                SimOp::PopulateFetched { lba, seed } => {
                    // Simulate a demand-fetch from S3: write one segment directly to
                    // index/ + cache/, bypassing the WAL.
                    //
                    // In production, index/*.idx is written by the coordinator once
                    // after S3 upload and is never duplicated.  A re-fetch of an
                    // evicted segment only rewrites the cache/ body, not index/.
                    // Creating two index entries for the same LBA is therefore an
                    // invalid production state — skip if this LBA is already populated.
                    let actual_lba = 16 + *lba as u64;
                    if oracle.contains_key(&actual_lba) {
                        continue;
                    }
                    // effective_seed always has bit 7 set (128..=255) so it never
                    // collides with Write/DedupWrite seeds (0..=127, bit 7 clear).
                    // Bits 6-4 encode lba (0..7), bits 3-0 vary within the lba slot.
                    let effective_seed = 0x80u8 | ((*lba & 0x07) << 4) | (*seed & 0x0F);
                    let ulid = vol.gc_checkpoint_for_test().unwrap();
                    common::populate_cache(fork_dir, ulid, actual_lba, effective_seed);
                    oracle.insert(actual_lba, [effective_seed; 4096]);
                }
                SimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                    oracle.insert(*lba_a as u64, data);
                    oracle.insert(*lba_b as u64, data);
                }
                SimOp::WriteZeroes { lba } => {
                    let _ = vol.write_zeroes(*lba as u64, 1);
                    // Oracle records zeros; a subsequent Crash asserts they survive rebuild.
                    oracle.insert(*lba as u64, [0u8; 4096]);
                }
                SimOp::WriteLarge { lba, seed } => {
                    let data = incompressible_block(*seed);
                    let actual_lba = 24 + *lba as u64;
                    let _ = vol.write(actual_lba, &data);
                    let mut block = [0u8; 4096];
                    block.copy_from_slice(&data);
                    oracle.insert(actual_lba, block);
                }
                SimOp::SameContentWrite { lba, seed } => {
                    let data = [*seed; 4096];
                    let actual_lba = 32 + *lba as u64;
                    let _ = vol.write(actual_lba, &data);
                    let mid = vol.noop_stats().skipped_writes;
                    let _ = vol.write(actual_lba, &data);
                    let after = vol.noop_stats().skipped_writes;
                    prop_assert!(
                        after > mid,
                        "second same-content write at lba {actual_lba} did not skip \
                         (skipped_writes mid={mid} after={after})"
                    );
                    oracle.insert(actual_lba, data);
                }
                SimOp::ReclaimRange { start_lba, lba_count } => {
                    // Reclaim preserves observable content by construction.
                    // Per-op oracle check is intentionally skipped here because
                    // PopulateFetched writes on-disk files that are only visible
                    // via the rebuilt lbamap after Crash. The next Crash handler
                    // will verify reclaim's contribution alongside everything else.
                    let _ = vol.reclaim_alias_merge(*start_lba as u64, *lba_count as u32);
                }
                SimOp::DeltaRepack => {
                    // Rewrites Data entries in post-snapshot pending
                    // segments to thin Deltas. Observable content is
                    // unchanged; oracle survival is verified by the
                    // next Crash handler.
                    let _ = vol.delta_repack_post_snapshot();
                }
                SimOp::SignSnapshot => {
                    // Signing must not touch the data path; oracle
                    // survival is verified by the next Crash handler.
                    let snap_ulid = pick_snap_ulid(fork_dir);
                    let _ = vol.sign_snapshot_manifest(snap_ulid);
                }
                SimOp::WriteMulti {
                    start_lba,
                    lba_count,
                    seed,
                } => {
                    // Multi-block write: produces lba_count contiguous
                    // body-section extents in one segment. Verify the
                    // multi-block read path immediately (lba_count > 1
                    // exercises payload_block_offset arithmetic), then
                    // record per-LBA in the oracle so the next Crash
                    // confirms each block survives WAL recovery.
                    let mut payload = Vec::with_capacity(*lba_count as usize * 4096);
                    for i in 0..*lba_count {
                        payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i)));
                    }
                    let start = 40 + *start_lba as u64;
                    if vol.write(start, &payload).is_ok() {
                        let actual = vol.read(start, *lba_count as u32).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            payload.as_slice(),
                            "multi-block read mismatch at lba {} count {}",
                            start,
                            lba_count
                        );
                        for i in 0..*lba_count as usize {
                            let mut block = [0u8; 4096];
                            block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                            oracle.insert(start + i as u64, block);
                        }
                    }
                }
            }
        }
    }

    /// GC-interleaved crash-recovery test.
    ///
    /// Identical oracle logic to `crash_recovery_oracle` but always starts with
    /// two segments already drained to `segments/`, ensuring `CoordGcLocal` has
    /// material to compact on most sequences rather than silently no-opping.
    #[test]
    fn gc_interleaved_oracle(ops in arb_gc_interleaved_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();
        common::write_test_keypair(fork_dir);
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
        let mut oracle: std::collections::HashMap<u64, [u8; 4096]> =
            std::collections::HashMap::new();
        let mut pending_gc: Option<Ulid> = None;

        for op in &ops {
            match op {
                SimOp::Write { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba as u64, &data);
                    oracle.insert(*lba as u64, data);
                }
                SimOp::Flush => {
                    let _ = vol.flush_wal();
                }
                SimOp::SweepPending => {
                    let _ = vol.sweep_pending();
                }
                SimOp::Repack => {
                    let _ = vol.repack(0.9);
                }
                SimOp::DrainWithRedact => {
                    common::drain_with_redact(&mut vol);
                }
                SimOp::CoordGcLocal { n } => {
                    // See ulid_monotonicity's CoordGcLocal — a full GC
                    // pass invalidates any stashed `GcCheckpoint`.
                    pending_gc = None;
                    let gc_ulid = vol.gc_checkpoint_for_test().unwrap();
                    let to_delete = if let Some((_, _, paths)) =
                        common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                    {
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
                }
                SimOp::GcCheckpoint => {
                    let u_gc = vol.gc_checkpoint_for_test().unwrap();
                    pending_gc = Some(u_gc);
                }
                SimOp::GcApply { n } => {
                    if let Some(gc_ulid) = pending_gc.take() {
                        let to_delete = if let Some((_, _, paths)) =
                            common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                        {
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
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
                    pending_gc = None;
                    // See crash_recovery_oracle for rationale.
                    common::assert_promote_recovery(&mut vol, fork_dir);
                    for (&lba, expected) in &oracle {
                        let actual = vol.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            expected.as_slice(),
                            "lba {} wrong after crash+rebuild",
                            lba
                        );
                    }
                }
                SimOp::ReadUnwritten => {
                    let actual = vol.read(64, 1).unwrap();
                    prop_assert_eq!(
                        actual.as_slice(),
                        [0u8; 4096].as_slice(),
                        "unwritten lba 64 returned non-zero data"
                    );
                }
                SimOp::Snapshot => {
                    let _ = vol.snapshot();
                }
                SimOp::HalfPromotePending => {
                    let _ = common::half_promote_first_pending(fork_dir);
                }
                SimOp::PopulateFetched { lba, seed } => {
                    // Skip if already populated — see crash_recovery_oracle for rationale.
                    let actual_lba = 16 + *lba as u64;
                    if oracle.contains_key(&actual_lba) {
                        continue;
                    }
                    // effective_seed always has bit 7 set (128..=255) so it never
                    // collides with Write/DedupWrite seeds (0..=127, bit 7 clear).
                    let effective_seed = 0x80u8 | ((*lba & 0x07) << 4) | (*seed & 0x0F);
                    let ulid = vol.gc_checkpoint_for_test().unwrap();
                    common::populate_cache(fork_dir, ulid, actual_lba, effective_seed);
                    oracle.insert(actual_lba, [effective_seed; 4096]);
                }
                SimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                    oracle.insert(*lba_a as u64, data);
                    oracle.insert(*lba_b as u64, data);
                }
                SimOp::WriteZeroes { lba } => {
                    let _ = vol.write_zeroes(*lba as u64, 1);
                    oracle.insert(*lba as u64, [0u8; 4096]);
                }
                SimOp::WriteLarge { lba, seed } => {
                    let data = incompressible_block(*seed);
                    let actual_lba = 24 + *lba as u64;
                    let _ = vol.write(actual_lba, &data);
                    let mut block = [0u8; 4096];
                    block.copy_from_slice(&data);
                    oracle.insert(actual_lba, block);
                }
                SimOp::SameContentWrite { lba, seed } => {
                    let data = [*seed; 4096];
                    let actual_lba = 32 + *lba as u64;
                    let _ = vol.write(actual_lba, &data);
                    let mid = vol.noop_stats().skipped_writes;
                    let _ = vol.write(actual_lba, &data);
                    let after = vol.noop_stats().skipped_writes;
                    prop_assert!(
                        after > mid,
                        "second same-content write at lba {actual_lba} did not skip \
                         (skipped_writes mid={mid} after={after})"
                    );
                    oracle.insert(actual_lba, data);
                }
                SimOp::ReclaimRange { start_lba, lba_count } => {
                    // Same reason as `crash_recovery_oracle` for not checking
                    // the oracle per-op: PopulateFetched leaves in-memory
                    // lbamap stale until Crash rebuilds it.
                    let _ = vol.reclaim_alias_merge(*start_lba as u64, *lba_count as u32);
                }
                SimOp::DeltaRepack => {
                    let _ = vol.delta_repack_post_snapshot();
                }
                SimOp::SignSnapshot => {
                    let snap_ulid = pick_snap_ulid(fork_dir);
                    let _ = vol.sign_snapshot_manifest(snap_ulid);
                }
                SimOp::WriteMulti {
                    start_lba,
                    lba_count,
                    seed,
                } => {
                    let mut payload = Vec::with_capacity(*lba_count as usize * 4096);
                    for i in 0..*lba_count {
                        payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i)));
                    }
                    let start = 40 + *start_lba as u64;
                    if vol.write(start, &payload).is_ok() {
                        for i in 0..*lba_count as usize {
                            let mut block = [0u8; 4096];
                            block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                            oracle.insert(start + i as u64, block);
                        }
                    }
                }
            }
        }
    }

    /// Dedicated reclaim correctness property.
    ///
    /// Drives a simplified op set (see `ReclaimOp`) that is deliberately
    /// narrower than `SimOp`: the existing `crash_recovery_oracle` already
    /// covers the broad set of state transitions. This proptest is about
    /// the single extra invariant reclamation brings:
    ///
    ///   **After alias-merge rewrites LBA map entries through internal-origin
    ///   writes, every LBA still reads back the bytes last written to it —
    ///   both immediately and across subsequent crash + rebuild.**
    ///
    /// Unlike `crash_recovery_oracle`, this asserts the oracle *after every
    /// single op*, not just at `Crash`. That is only safe because `ReclaimOp`
    /// excludes the direct-on-disk `PopulateFetched` path: every op here
    /// mutates state through the in-memory lbamap, so `vol.read` is always
    /// authoritative against the oracle.
    #[test]
    fn reclaim_crash_recovery(ops in arb_reclaim_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();
        common::write_test_keypair(fork_dir);
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
        let mut oracle: std::collections::HashMap<u64, [u8; 4096]> =
            std::collections::HashMap::new();

        for op in &ops {
            match op {
                ReclaimOp::Write { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba as u64, &data);
                    oracle.insert(*lba as u64, data);
                }
                ReclaimOp::WriteLargeMulti { lba, seed } => {
                    let mut payload = Vec::with_capacity(8 * 4096);
                    for i in 0..8 {
                        payload.extend_from_slice(&incompressible_block(
                            seed.wrapping_add(i as u8),
                        ));
                    }
                    let start = 24 + *lba as u64;
                    if vol.write(start, &payload).is_ok() {
                        for i in 0..8 {
                            let mut block = [0u8; 4096];
                            block.copy_from_slice(&payload[i * 4096..(i + 1) * 4096]);
                            oracle.insert(start + i as u64, block);
                        }
                    }
                }
                ReclaimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                    oracle.insert(*lba_a as u64, data);
                    oracle.insert(*lba_b as u64, data);
                }
                ReclaimOp::Flush => {
                    let _ = vol.flush_wal();
                }
                ReclaimOp::DrainWithRedact => {
                    common::drain_with_redact(&mut vol);
                }
                ReclaimOp::SweepPending => {
                    let _ = vol.sweep_pending();
                }
                ReclaimOp::Repack => {
                    let _ = vol.repack(0.9);
                }
                ReclaimOp::ReclaimRange { start_lba, lba_count } => {
                    let outcome = vol
                        .reclaim_alias_merge(*start_lba as u64, *lba_count as u32)
                        .unwrap();
                    // Single-threaded driver: apply never discards.
                    prop_assert!(!outcome.discarded);
                }
                ReclaimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
                }
            }

            // Hard invariant: every op (including reclaim) must preserve
            // every oracle LBA exactly.
            for (&lba, expected) in &oracle {
                let actual = vol.read(lba, 1).unwrap();
                prop_assert_eq!(
                    actual.as_slice(),
                    expected.as_slice(),
                    "lba {} wrong after {:?}",
                    lba,
                    op
                );
            }
        }
    }
}

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

    let _ = vol.repack(0.9);
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

    let _ = vol.sweep_pending();
    assert_oracle(&mut vol, &oracle, "SweepPending");

    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    assert_oracle(&mut vol, &oracle, "Crash");
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

    let gc_ulid = vol.gc_checkpoint_for_test().unwrap();
    let _ = common::half_promote_first_pending(fork_dir);

    // Mirrors `SimOp::PopulateFetched { lba: 0, seed: 0 }`: writes a
    // direct-fetch cache entry at LBA 16 with effective_seed 0x80.
    let effective_seed: u8 = 0x80;
    let cache_ulid = vol.gc_checkpoint_for_test().unwrap();
    common::populate_cache(fork_dir, cache_ulid, 16, effective_seed);
    oracle.insert(16, [effective_seed; 4096]);

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
