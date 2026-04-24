// Deterministic repro for a pre-existing interaction between `redact`'s
// hole-punching and `extent_index::rebuild`'s lowest-ULID-wins rule.
// Surfaced by `reclaim_crash_recovery` in volume_proptest once the
// flush-first reclaim fix (PR #115) closed the WAL-shadow bug and
// pushed the proptest deeper into the state space.
//
// Seed bytes from the failing CI run:
//   cc 00beae3f... (not persisted — see commit 2e0b8f8 for context).
//
// Minimal failing sequence:
//
//   WriteLargeMulti { lba: 3, seed: 102 }   // LBA 27..=34 = h_full_102
//   WriteLargeMulti { lba: 3, seed: 0 }     // LBA 27..=34 = h_full_0  (overwrites lbamap)
//   ReclaimRange { start_lba: 24, lba_count: 1 }  // no-op range, but flushes WAL
//   DrainWithRedact                          // redact hole-punches h_full_102 (lba-dead)
//                                            //   then promote writes idx + cache
//   WriteLargeMulti { lba: 0, seed: 102 }   // LBA 24..=31 = h_full_102 (new DATA entry)
//   ReclaimRange { start_lba: 24, lba_count: 1 }  // rewrites LBA 24 head as 1-block
//   Crash
//
// Failure mode (read LBA 25 returns zeros):
//   - After crash, `extent_index::rebuild` walks segments in ULID
//     ascending order with `insert_if_absent` — lowest-ULID canonical.
//   - The earlier (redacted) segment's .idx still lists the
//     `h_full_102` entry even though `redact` hole-punched its body.
//     That entry wins the lowest-ULID insert.
//   - The later segment holding h_full_102's *live* body is shadowed.
//   - lbamap at LBA 25 points at `h_full_102` with
//     `payload_block_offset = 1`; the resolve walks to the redacted
//     segment's hole-punched body and reads zeros.
//
// Fix direction (follow-up): redact should also delete or flag the
// entry metadata in the .idx / segment header, not just hole-punch
// the body. Otherwise lowest-ULID-wins silently resurrects a dead
// canonical location for any hash that later re-appears via a fresh
// DATA write.
//
// Marked `#[ignore]`; reproduce with
// `cargo test -p elide-core --test redact_then_dedup_rebuild_repro -- --ignored`.

use elide_core::volume::Volume;
use tempfile::TempDir;

mod common;

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

fn write_large_multi_payload(seed: u8) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 * 4096);
    for i in 0..8u8 {
        payload.extend_from_slice(&incompressible_block(seed.wrapping_add(i)));
    }
    payload
}

#[test]
#[ignore = "pre-existing bug: redact leaves .idx entry metadata intact; rebuild's lowest-ULID-wins resurrects hole-punched body (see file header)"]
fn redact_hole_punched_hash_not_resurrected_by_rebuild() {
    let tmp = TempDir::new().unwrap();
    let fork_dir = tmp.path();
    common::write_test_keypair(fork_dir);

    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();

    // Steps 1–2: two overlapping WriteLargeMulti at LBA 27..=34. The
    // second overwrites the lbamap, so the first's full-payload hash
    // (h_full_102) becomes LBA-dead.
    let payload_102 = write_large_multi_payload(102);
    let payload_0 = write_large_multi_payload(0);
    vol.write(27, &payload_102).unwrap();
    vol.write(27, &payload_0).unwrap();

    // Step 3: empty-range reclaim. With the flush-first fix, this
    // triggers a WAL flush to pending/<u_flush1> even though LBA 24 is
    // unoccupied.
    vol.reclaim_alias_merge(24, 1).unwrap();

    // Step 4: drain. `redact` sees h_full_102 is LBA-dead and not
    // referenced by any live hash → hole-punches its body in
    // pending/<u_flush1> and removes its in-memory extent-index entry.
    // `promote` then writes index/<u_flush1>.idx + cache/<u_flush1>.body.
    // The .idx still carries h_full_102 entry metadata.
    common::drain_with_redact(&mut vol);

    // Step 5: write LBA 24..=31 with the same payload as step 1. The
    // hash is h_full_102 again. The extent-index entry was removed by
    // redact's cleanup, so this is a fresh DATA write (not a DedupRef).
    vol.write(24, &payload_102).unwrap();

    // Step 6: reclaim LBA 24 head out of the new 8-block fragment.
    // This flushes the WAL to pending/<u_flush2> (which contains the
    // live h_full_102 body) and writes a 1-block reclaim output.
    vol.reclaim_alias_merge(24, 1).unwrap();

    // In-memory reads are fine at this point — extent_index has been
    // mutated by the flush inside step 6 to point h_full_102 at
    // U_flush2. The bug only manifests after rebuild.
    assert_eq!(
        vol.read(25, 1).unwrap(),
        incompressible_block(103),
        "LBA 25 correct pre-crash (in-memory extent_index points at live body)"
    );

    // Step 7: crash + rebuild. `extent_index::rebuild` walks in ULID
    // ascending order with `insert_if_absent`. U_flush1 comes first
    // (and is in index/ tier, which is processed ahead of pending/),
    // so its hole-punched h_full_102 entry wins — shadowing the live
    // U_flush2 entry.
    drop(vol);
    let vol = Volume::open(fork_dir, fork_dir).unwrap();

    assert_eq!(
        vol.read(25, 1).unwrap(),
        incompressible_block(103),
        "LBA 25 must still read block 103 after crash+rebuild"
    );
    for lba_off in 0..8 {
        let lba = 24 + lba_off;
        let expected = incompressible_block(102u8.wrapping_add(lba_off as u8));
        assert_eq!(
            vol.read(lba, 1).unwrap(),
            expected,
            "lba {lba} must read block {} after crash+rebuild",
            102 + lba_off
        );
    }
}
