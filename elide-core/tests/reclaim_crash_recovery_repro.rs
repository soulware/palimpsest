// Deterministic repro for a proptest-surfaced reclaim + crash-recovery bug.
//
// Seed 00beae3f5000d4abb913711617ba1d775a3f4e08d7f570e16c2c019cb5686cf1
// (see volume_proptest.proptest-regressions) minimised to:
//
//   WriteLargeMulti { lba: 1, seed: 0 }   // 8 blocks at LBA 25..=32
//   WriteLargeMulti { lba: 2, seed: 1 }   // 8 blocks at LBA 26..=33 (overwrites 26..=32)
//   ReclaimRange { start_lba: 24, lba_count: 2 }  // reclaim over LBA 24..26
//   Flush
//   DrainWithRedact
//   Crash
//
// After Crash+reopen, some LBA reads back all zeros instead of the
// oracle's expected incompressible bytes.
//
// Materialised here so host-tests catches regressions without needing the
// proptest feature gate.

use elide_core::volume::Volume;
use tempfile::TempDir;

mod common;

/// 4 KiB of pseudo-random bytes keyed on `seed`. Lifted verbatim from
/// `volume_proptest::incompressible_block` — keep in sync if that ever
/// changes.
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

fn write_large_multi(vol: &mut Volume, lba_offset: u8, seed: u8) -> Vec<Vec<u8>> {
    let mut payload = Vec::with_capacity(8 * 4096);
    let mut blocks = Vec::with_capacity(8);
    for i in 0..8u8 {
        let block = incompressible_block(seed.wrapping_add(i));
        payload.extend_from_slice(&block);
        blocks.push(block);
    }
    let start = 24 + lba_offset as u64;
    vol.write(start, &payload).unwrap();
    blocks
}

#[test]
fn reclaim_then_drain_redact_then_crash_preserves_bloat_fragment() {
    let tmp = TempDir::new().unwrap();
    let fork_dir = tmp.path();
    common::write_test_keypair(fork_dir);

    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();

    // WriteLargeMulti { lba: 1, seed: 0 } — LBAs 25..=32
    let first = write_large_multi(&mut vol, 1, 0);

    // WriteLargeMulti { lba: 2, seed: 1 } — LBAs 26..=33, overwriting 26..=32
    let second = write_large_multi(&mut vol, 2, 1);

    // Oracle: LBA 25 keeps first-write's block 0; LBAs 26..=33 hold
    // second-write's blocks 0..=7.
    let mut oracle: std::collections::HashMap<u64, Vec<u8>> = std::collections::HashMap::new();
    oracle.insert(25, first[0].clone());
    for (i, block) in second.iter().enumerate() {
        oracle.insert(26 + i as u64, block.clone());
    }

    // ReclaimRange { start_lba: 24, lba_count: 2 } — LBA [24, 26) covers
    // only LBA 25, which is a bloated head run (1 live LBA from an 8-block
    // fragment).
    let outcome = vol.reclaim_alias_merge(24, 2).unwrap();
    assert!(!outcome.discarded, "reclaim must not discard");

    // Flush
    vol.flush_wal().unwrap();

    // DrainWithRedact
    common::drain_with_redact(&mut vol);

    // Crash: drop + reopen.
    drop(vol);
    let vol = Volume::open(fork_dir, fork_dir).unwrap();

    // Oracle check: every recorded LBA must read back exactly.
    for (&lba, expected) in &oracle {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual, *expected,
            "lba {lba} read back wrong bytes after reclaim+drain+crash"
        );
    }
}
