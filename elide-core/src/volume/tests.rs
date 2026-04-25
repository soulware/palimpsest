use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut p = std::env::temp_dir();
    p.push(format!("elide-volume-test-{}-{}", std::process::id(), n));
    p
}

/// Simulate coordinator drain: upload all pending segments to S3 (no-op in
/// tests) then call `promote_segment` on each.  `promote_segment` writes
/// `index/<ulid>.idx`, copies the body to `cache/`, and deletes `pending/<ulid>`.
fn simulate_upload(vol: &mut Volume) {
    let pending_dir = vol.base_dir.join("pending");
    for entry in std::fs::read_dir(&pending_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.ends_with(".tmp") {
            continue;
        }
        let ulid = ulid::Ulid::from_string(&name).unwrap();
        vol.promote_segment(ulid).unwrap();
    }
}

/// Generate a keypair and write `volume.key` + `volume.pub` into `dir`.
///
/// Must be called before `Volume::open` in any test that creates a volume.
fn write_test_keypair(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let key = crate::signing::generate_keypair(
        dir,
        crate::signing::VOLUME_KEY_FILE,
        crate::signing::VOLUME_PUB_FILE,
    )
    .unwrap();
    // Match production volume-setup behaviour: a fresh writable volume
    // also gets a default (root) `volume.provenance`. Skipping this
    // makes `Volume::open` fail in the ancestor walk when another
    // volume forks from this one, because the child's provenance
    // refers back to a volume whose own provenance is missing.
    crate::signing::write_provenance(
        dir,
        &key,
        crate::signing::VOLUME_PROVENANCE_FILE,
        &crate::signing::ProvenanceLineage::default(),
    )
    .unwrap();
}

/// Write a signed `volume.provenance` with the given lineage fields into
/// `dir`. Routes through `write_raw_provenance_for_test` so that
/// syntactically bad `parent_entry` strings can be persisted for
/// parse-error coverage — the file signature is still valid over the raw
/// bytes, so the parse error fires before signature verification.
///
/// When `parent_entry` is `Some`, an all-zero dummy `parent_pubkey` is
/// embedded. Tests that walk the chain only care about structural fields.
fn write_test_provenance(dir: &Path, parent_entry: Option<&str>, extent_entries: &[&str]) {
    let (raw_parent, raw_parent_pubkey) = match parent_entry {
        Some(p) => (p.to_owned(), crate::signing::encode_hex(&[0u8; 32])),
        None => (String::new(), String::new()),
    };
    let extent_owned: Vec<String> = extent_entries.iter().map(|s| (*s).to_owned()).collect();
    crate::signing::write_raw_provenance_for_test(
        dir,
        &raw_parent,
        &raw_parent_pubkey,
        &extent_owned,
    )
    .unwrap();
}

/// Create a temp dir and pre-populate it with a test keypair.
///
/// Use in place of `temp_dir()` whenever the dir will be passed directly
/// to `Volume::open`.
fn keyed_temp_dir() -> PathBuf {
    let dir = temp_dir();
    write_test_keypair(&dir);
    dir
}

#[test]
fn open_creates_directories() {
    let base = keyed_temp_dir();
    let _ = Volume::open(&base, &base).unwrap();
    assert!(base.join("wal").is_dir());
    assert!(base.join("pending").is_dir());
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn open_is_idempotent() {
    let base = keyed_temp_dir();
    let _ = Volume::open(&base, &base).unwrap();
    // Second open on the same dir should succeed (dirs already exist).
    let _ = Volume::open(&base, &base).unwrap();
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_single_block() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    vol.write(0, &vec![0x42u8; 4096]).unwrap();
    vol.fsync().unwrap();
    assert_eq!(vol.lbamap_len(), 1);
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_multi_block_extent() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    // Write 8 LBAs (32 KiB) as a single call.
    vol.write(10, &vec![0xabu8; 8 * 4096]).unwrap();
    assert_eq!(vol.lbamap_len(), 1);
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn noop_skip_same_lba_same_content() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    let data = vec![0x42u8; 4096];

    vol.write(0, &data).unwrap();
    let before = vol.noop_stats();
    assert_eq!(before.skipped_writes, 0);
    assert_eq!(before.skipped_bytes, 0);

    // Same LBA, same content — short-circuited by the LBA-map hash check.
    vol.write(0, &data).unwrap();
    let after = vol.noop_stats();
    assert_eq!(after.skipped_writes, 1);
    assert_eq!(after.skipped_bytes, 4096);

    // Data still reads back correctly.
    assert_eq!(vol.read(0, 1).unwrap(), data);
    // LBA map still has exactly one entry.
    assert_eq!(vol.lbamap_len(), 1);
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn noop_skip_different_content_falls_through() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    let a = vec![0x42u8; 4096];
    let b = vec![0x99u8; 4096];

    vol.write(0, &a).unwrap();
    vol.write(0, &b).unwrap();
    let stats = vol.noop_stats();
    assert_eq!(stats.skipped_writes, 0);
    // Latest write wins.
    assert_eq!(vol.read(0, 1).unwrap(), b);
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn noop_skip_after_promotion() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    let data = vec![0xaau8; 4 * 4096];

    vol.write(10, &data).unwrap();
    vol.flush_wal().unwrap(); // promote WAL → pending/
    // Body now lives in a pending segment file (BodySource::Local).
    vol.write(10, &data).unwrap();

    let stats = vol.noop_stats();
    assert_eq!(stats.skipped_writes, 1);
    assert_eq!(stats.skipped_bytes, 4 * 4096);
    assert_eq!(vol.read(10, 4).unwrap(), data);
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn noop_skip_multi_block_same_content() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    let data: Vec<u8> = (0..8 * 4096).map(|i| (i as u8).wrapping_mul(17)).collect();

    vol.write(32, &data).unwrap();
    vol.write(32, &data).unwrap();

    let stats = vol.noop_stats();
    assert_eq!(stats.skipped_writes, 1);
    assert_eq!(stats.skipped_bytes, 8 * 4096);
    assert_eq!(vol.read(32, 8).unwrap(), data);
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn noop_skip_does_not_fire_on_fragmented_match() {
    // The hash check keys on a single LBA-map entry that exactly
    // covers the incoming range. When the existing content is split
    // into two entries whose concatenation matches, no single map
    // entry hashes the whole range — the skip cannot fire and the
    // write commits normally. (Earlier designs added a body
    // byte-compare tier to catch this; see
    // `docs/design-noop-write-skip.md § Why no byte-compare tier`.)
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    let a = vec![0xa1u8; 4096];
    let b = vec![0xb2u8; 4096];

    vol.write(0, &a).unwrap();
    vol.write(1, &b).unwrap();

    let mut combined = Vec::with_capacity(8192);
    combined.extend_from_slice(&a);
    combined.extend_from_slice(&b);
    vol.write(0, &combined).unwrap();

    let stats = vol.noop_stats();
    assert_eq!(stats.skipped_writes, 0);
    // The fresh 8 KiB write replaces the two split entries with one.
    assert_eq!(vol.lbamap_len(), 1);
    // Read still returns the expected concatenation.
    assert_eq!(vol.read(0, 2).unwrap(), combined);
    fs::remove_dir_all(base).unwrap();
}

// ---------- extent reclamation (alias-merge) ----------

/// Produce a 4096-byte block whose bytes depend on `seed` and `block_idx`,
/// giving incompressible, distinct content per block so that splitting an
/// originally-contiguous payload exposes the fragmentation clearly.
fn reclaim_block(seed: u8, block_idx: usize) -> [u8; 4096] {
    let mut buf = [0u8; 4096];
    let key = [seed; 32];
    let mut hasher = blake3::Hasher::new_keyed(&key);
    hasher.update(&(block_idx as u64).to_le_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut buf);
    buf
}

fn reclaim_payload(seed: u8, n_blocks: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n_blocks * 4096);
    for i in 0..n_blocks {
        out.extend_from_slice(&reclaim_block(seed, i));
    }
    out
}

/// Write a single 8-block entry, overwrite the middle 2 blocks with a
/// smaller (1-block) entry so the original is split prefix/tail, then
/// run alias-merge over the whole range. The split-tail entry has
/// `payload_block_offset != 0`, so the primitive should detect bloat
/// and rewrite both the prefix and the tail as fresh compact entries.
#[test]
fn reclaim_alias_merge_rewrites_split_entry() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Big 8-block write at LBA 100.
    let big = reclaim_payload(0xA1, 8);
    vol.write(100, &big).unwrap();
    // Overwrite LBA 103 (1 block, middle) with unrelated content. The map
    // now has [100,103)@0 (hash A) + [103,104)@0 (hash B) + [104,108)@4 (hash A).
    let hole = [0x77u8; 4096];
    vol.write(103, &hole).unwrap();

    // Oracle expected bytes at [100, 108).
    let mut expected = vec![0u8; 8 * 4096];
    expected[..3 * 4096].copy_from_slice(&big[..3 * 4096]);
    expected[3 * 4096..4 * 4096].copy_from_slice(&hole);
    expected[4 * 4096..].copy_from_slice(&big[4 * 4096..]);
    assert_eq!(vol.read(100, 8).unwrap(), expected);

    // Before: 3 entries.
    assert_eq!(vol.lbamap_len(), 3);

    // Sync prep+execute+apply via the in-process wrapper.
    let outcome = vol.reclaim_alias_merge(100, 8).unwrap();
    // Two rewrites: prefix run [100,103) and tail run [104,108). The
    // middle [103,104) is a clean single-block entry with offset=0 —
    // its hash has no `offset != 0` run anywhere, so it's left alone.
    assert!(!outcome.discarded);
    assert_eq!(outcome.runs_rewritten, 2);
    assert_eq!(outcome.bytes_rewritten, (3 + 4) * 4096);

    // Readback still matches.
    assert_eq!(vol.read(100, 8).unwrap(), expected);

    // Second pass is an idempotent no-op: hashes are now stable, every
    // rewrite the worker would propose hits the lbamap noop-skip.
    let outcome2 = vol.reclaim_alias_merge(100, 8).unwrap();
    assert!(!outcome2.discarded);
    assert_eq!(outcome2.runs_rewritten, 0);

    fs::remove_dir_all(base).unwrap();
}

/// Query range that slices mid-way through a hash's span: the prefix of
/// the big write is outside the query. Containment check must refuse to
/// rewrite — doing so would strand the outside references on the bloated
/// body and *introduce* fragmentation.
#[test]
fn reclaim_alias_merge_skips_non_contained_hash() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Map state: [100, 150) → H/offset=0 (single big entry).
    let big = reclaim_payload(0x3C, 50);
    vol.write(100, &big).unwrap();

    // Query only the tail half — H's first 25 blocks live outside.
    // Containment fails: H has a run [100, 150) which starts at 100,
    // outside the query [125, 150). Nothing to rewrite.
    let outcome = vol.reclaim_alias_merge(125, 25).unwrap();
    assert!(!outcome.discarded);
    assert_eq!(outcome.runs_rewritten, 0);

    fs::remove_dir_all(base).unwrap();
}

/// When the LBA map is mutated between prepare and apply, the apply
/// phase must discard cleanly — orphan-cleaning the worker's output
/// segment — with no state change to the live lbamap.
#[test]
fn reclaim_alias_merge_discards_on_concurrent_mutation() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let big = reclaim_payload(0x5E, 8);
    vol.write(200, &big).unwrap();
    let hole = [0x11u8; 4096];
    vol.write(203, &hole).unwrap();

    let job = vol.prepare_reclaim(200, 8).unwrap();
    let result = crate::actor::execute_reclaim(job).unwrap();
    // The worker must have produced at least one rewrite.
    assert!(result.segment_written);
    let segment_path = result.pending_dir.join(result.segment_ulid.to_string());
    assert!(segment_path.exists(), "worker segment should be on disk");

    // Simulate concurrent mutation: any write bumps the lbamap Arc and
    // breaks the pointer-equality precondition.
    vol.write(500, &reclaim_payload(0x77, 1)).unwrap();

    // Apply must detect the mutation, discard, and delete the orphan.
    let outcome = vol.apply_reclaim_result(result).unwrap();
    assert!(outcome.discarded);
    assert_eq!(outcome.runs_rewritten, 0);
    assert_eq!(outcome.bytes_rewritten, 0);
    assert!(
        !segment_path.exists(),
        "apply must remove the orphan segment on discard"
    );

    fs::remove_dir_all(base).unwrap();
}

/// Zero extents carry no body and must never be rewritten by alias-merge.
#[test]
fn reclaim_alias_merge_skips_zero_extents() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write_zeroes(300, 10).unwrap();
    // Split the zero extent with an unrelated data write so the tail
    // ends up with payload_block_offset != 0. Our rule would normally
    // treat that as "bloat" for any non-zero hash, but ZERO_HASH is
    // always skipped.
    vol.write(304, &[0xABu8; 4096]).unwrap();

    let outcome = vol.reclaim_alias_merge(300, 10).unwrap();
    assert!(!outcome.discarded);
    assert_eq!(outcome.runs_rewritten, 0);

    fs::remove_dir_all(base).unwrap();
}

// ---------- reclaim candidate scanner ----------

fn scanner_thresholds_permissive() -> crate::volume::ReclaimThresholds {
    // Loose thresholds so tests can use small payloads while still
    // exercising the scanner's detection logic.
    crate::volume::ReclaimThresholds {
        min_dead_blocks: 1,
        min_dead_ratio: 0.0,
        min_stored_bytes: 0,
    }
}

/// A clean volume with a single compact write produces no candidates.
#[test]
fn scan_reclaim_candidates_no_bloat() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    vol.write(100, &reclaim_payload(0x11, 8)).unwrap();

    let (lbamap, extent_index) = vol.snapshot_maps();
    let candidates = crate::volume::scan_reclaim_candidates(
        &lbamap,
        &extent_index,
        scanner_thresholds_permissive(),
    );
    assert!(
        candidates.is_empty(),
        "fresh compact entry should not be a candidate, got {candidates:?}"
    );

    fs::remove_dir_all(base).unwrap();
}

/// Write an 8-block entry, overwrite the middle 2 blocks with a
/// 2-block entry. The original's payload now has dead bytes (blocks
/// 3..4 are LBA-overwritten). The scanner should flag exactly one
/// candidate covering the full extent of the original hash's
/// surviving runs.
#[test]
fn scan_reclaim_candidates_flags_split_entry() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Big incompressible 8-block write so payload lives in the body.
    vol.write(200, &reclaim_payload(0x22, 8)).unwrap();
    // Overwrite the middle 2 blocks with unrelated content.
    let hole: Vec<u8> = (0..2).flat_map(|_| [0x77u8; 4096]).collect();
    vol.write(203, &hole).unwrap();

    let (lbamap, extent_index) = vol.snapshot_maps();
    let candidates = crate::volume::scan_reclaim_candidates(
        &lbamap,
        &extent_index,
        scanner_thresholds_permissive(),
    );
    assert_eq!(
        candidates.len(),
        1,
        "expected exactly one candidate for the bloated hash, got {candidates:?}"
    );
    let c = candidates[0];
    // Tight LBA bound: the hash's first live run starts at 200, its
    // last live run ends at 208.
    assert_eq!(c.start_lba, 200);
    assert_eq!(c.lba_length, 8);
    // Live: 3 prefix + 3 tail = 6. Dead: 2 (the hole in the middle).
    assert_eq!(c.live_blocks, 6);
    assert_eq!(c.dead_blocks, 2);

    fs::remove_dir_all(base).unwrap();
}

/// Thresholds are respected: bumping `min_dead_blocks` above the
/// actual dead count drops the candidate.
#[test]
fn scan_reclaim_candidates_respects_min_dead_blocks() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(300, &reclaim_payload(0x33, 8)).unwrap();
    vol.write(303, &[0x99u8; 4096]).unwrap(); // 1 block hole

    let (lbamap, extent_index) = vol.snapshot_maps();

    // With min_dead_blocks=1 the 1-block hole qualifies.
    let loose = crate::volume::scan_reclaim_candidates(
        &lbamap,
        &extent_index,
        crate::volume::ReclaimThresholds {
            min_dead_blocks: 1,
            min_dead_ratio: 0.0,
            min_stored_bytes: 0,
        },
    );
    assert_eq!(loose.len(), 1);

    // With min_dead_blocks=2 it does not.
    let strict = crate::volume::scan_reclaim_candidates(
        &lbamap,
        &extent_index,
        crate::volume::ReclaimThresholds {
            min_dead_blocks: 2,
            min_dead_ratio: 0.0,
            min_stored_bytes: 0,
        },
    );
    assert!(strict.is_empty());

    fs::remove_dir_all(base).unwrap();
}

/// Candidates produced by the scanner must always be valid inputs
/// to `reclaim_alias_merge` — round-trip through the primitive
/// rewrites the hash and a rescan produces no further candidates
/// for it.
#[test]
fn scan_reclaim_candidates_round_trip_through_primitive() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(400, &reclaim_payload(0x44, 8)).unwrap();
    vol.write(404, &[0x11u8; 4096]).unwrap();

    // Oracle: bytes at [400, 408) after the second write.
    let expected = {
        let mut buf = vec![0u8; 8 * 4096];
        let orig = reclaim_payload(0x44, 8);
        buf[..4 * 4096].copy_from_slice(&orig[..4 * 4096]);
        buf[4 * 4096..5 * 4096].fill(0x11);
        buf[5 * 4096..].copy_from_slice(&orig[5 * 4096..]);
        buf
    };
    assert_eq!(vol.read(400, 8).unwrap(), expected);

    let (lbamap, extent_index) = vol.snapshot_maps();
    let candidates = crate::volume::scan_reclaim_candidates(
        &lbamap,
        &extent_index,
        scanner_thresholds_permissive(),
    );
    assert_eq!(candidates.len(), 1);
    let c = candidates[0];

    // Drop the snapshot clones before mutating Volume state, so
    // Arc::make_mut in the primitive doesn't reallocate.
    drop(lbamap);
    drop(extent_index);

    let outcome = vol.reclaim_alias_merge(c.start_lba, c.lba_length).unwrap();
    assert!(!outcome.discarded);
    assert!(outcome.runs_rewritten > 0);

    // Content preserved.
    assert_eq!(vol.read(400, 8).unwrap(), expected);

    // Rescan: the old hash is gone from the LBA map, the new
    // compact ones have no bloat — zero candidates.
    let (lbamap2, extent_index2) = vol.snapshot_maps();
    let candidates2 = crate::volume::scan_reclaim_candidates(
        &lbamap2,
        &extent_index2,
        scanner_thresholds_permissive(),
    );
    assert!(
        candidates2.is_empty(),
        "rescan after reclaim should find nothing, got {candidates2:?}"
    );

    fs::remove_dir_all(base).unwrap();
}

/// Idempotent-convergence property: a reclaim pass over an
/// already-optimal range produces no rewrites at all.
#[test]
fn reclaim_alias_merge_optimal_range_is_noop() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    vol.write(400, &reclaim_payload(0x7A, 4)).unwrap();

    let outcome = vol.reclaim_alias_merge(400, 4).unwrap();
    assert_eq!(outcome.runs_rewritten, 0);
    assert!(!outcome.discarded);

    fs::remove_dir_all(base).unwrap();
}

/// A pure tail overwrite leaves the surviving run with
/// `payload_block_offset == 0` even though the stored body has a
/// dead tail. Historically the primitive silently rejected this
/// shape (its gate required at least one run with `offset != 0`)
/// while the scanner flagged it — causing "0 runs from N
/// candidates" on `elide volume reclaim`. Regression for the
/// wider gate in `execute_reclaim` that matches the scanner's
/// `live_blocks < logical_blocks` criterion.
#[test]
fn reclaim_alias_merge_rewrites_tail_overwrite() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // 8-block incompressible write at LBA 600.
    let big = reclaim_payload(0xB1, 8);
    vol.write(600, &big).unwrap();

    // Overwrite LBAs [606, 608) — the last 2 blocks of the extent.
    // Surviving run is [600, 606) with `payload_block_offset = 0`.
    let tail = [0x44u8; 2 * 4096];
    vol.write(606, &tail).unwrap();

    // Precondition: exactly one surviving run of the original hash
    // and its offset is zero — the shape the old gate missed.
    let (lbamap_pre, _) = vol.snapshot_maps();
    let original_hash = blake3::hash(&big);
    let runs_pre = lbamap_pre.runs_for_hash(&original_hash);
    assert_eq!(runs_pre.len(), 1);
    assert_eq!(
        runs_pre[0].2, 0,
        "surviving tail-overwrite run has offset 0"
    );
    drop(lbamap_pre);

    // Oracle.
    let mut expected = Vec::with_capacity(8 * 4096);
    expected.extend_from_slice(&big[..6 * 4096]);
    expected.extend_from_slice(&tail);
    assert_eq!(vol.read(600, 8).unwrap(), expected);

    let outcome = vol.reclaim_alias_merge(600, 8).unwrap();
    assert!(!outcome.discarded);
    assert_eq!(
        outcome.runs_rewritten, 1,
        "tail-overwrite must rewrite the surviving head run"
    );
    assert_eq!(vol.read(600, 8).unwrap(), expected);

    // Rescan: no residual candidates.
    let (lbamap_post, extent_index_post) = vol.snapshot_maps();
    let candidates = crate::volume::scan_reclaim_candidates(
        &lbamap_post,
        &extent_index_post,
        scanner_thresholds_permissive(),
    );
    assert!(
        candidates.is_empty(),
        "post-reclaim rescan must be empty, got {candidates:?}"
    );

    fs::remove_dir_all(base).unwrap();
}

/// Coordinator tick semantics: when the IPC handler is called with
/// `cap = 1` every tick (as `tasks.rs::run_volume_tasks` does), a
/// backlog of bloated hashes drains to zero over successive calls.
/// Simulates that loop against the Volume API directly.
#[test]
fn reclaim_cap_one_per_call_converges_to_zero_candidates() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Three independent bloated hashes at disjoint LBA ranges.
    // Each gets a middle overwrite to split it and trip the
    // scanner's dead-block criterion.
    for (seed, base_lba) in [(0xA1u8, 100u64), (0xB2, 200), (0xC3, 300)] {
        vol.write(base_lba, &reclaim_payload(seed, 8)).unwrap();
        vol.write(base_lba + 3, &[0xFFu8; 4096]).unwrap();
    }

    let thresholds = scanner_thresholds_permissive();

    // Scanner starts with three candidates.
    let (lbamap, ei) = vol.snapshot_maps();
    let initial = crate::volume::scan_reclaim_candidates(&lbamap, &ei, thresholds);
    assert_eq!(
        initial.len(),
        3,
        "expected 3 initial candidates, got {initial:?}"
    );
    drop(lbamap);
    drop(ei);

    // Simulate the tick loop: each "tick" scans and processes at
    // most one candidate. Bound iterations so a regression can't
    // infinite-loop the test.
    let mut tick_count = 0usize;
    for _ in 0..10 {
        let (lbamap, ei) = vol.snapshot_maps();
        let mut candidates = crate::volume::scan_reclaim_candidates(&lbamap, &ei, thresholds);
        drop(lbamap);
        drop(ei);
        if candidates.is_empty() {
            break;
        }
        let c = candidates.remove(0);
        let outcome = vol.reclaim_alias_merge(c.start_lba, c.lba_length).unwrap();
        assert!(!outcome.discarded);
        assert!(outcome.runs_rewritten > 0);
        tick_count += 1;
    }

    // Exactly three productive ticks for three initial candidates.
    assert_eq!(tick_count, 3);

    // Rescan: converged.
    let (lbamap, ei) = vol.snapshot_maps();
    let remaining = crate::volume::scan_reclaim_candidates(&lbamap, &ei, thresholds);
    assert!(
        remaining.is_empty(),
        "converged scan must be empty, got {remaining:?}"
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_rejects_empty() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    let err = vol.write(0, &[]).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_rejects_misaligned() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    let err = vol.write(0, &[0u8; 1000]).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_sets_needs_promote_after_threshold() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Write 33 × 1 MiB of incompressible data to exceed FLUSH_THRESHOLD (32 MiB).
    // Each block uses a unique byte value so entropy is high and compression is skipped.
    let mut block = vec![0u8; 1024 * 1024];
    for i in 0u64..33 {
        // Fill with a pattern that defeats compression: vary every byte.
        let fill = (i & 0xFF) as u8;
        for (j, b) in block.iter_mut().enumerate() {
            *b = fill ^ (j as u8).wrapping_mul(0x6D).wrapping_add(0x4F);
        }
        vol.write(i * 256, &block).unwrap();
    }

    // writes no longer auto-promote; needs_promote() should be true.
    assert!(
        vol.needs_promote(),
        "expected needs_promote() after 33 MiB of writes"
    );

    // Explicit flush_wal() should promote to pending/.
    vol.flush_wal().unwrap();

    // At least one segment should have been promoted to pending/.
    let has_pending = fs::read_dir(base.join("pending"))
        .unwrap()
        .any(|e| e.is_ok());
    assert!(
        has_pending,
        "expected at least one promoted segment in pending/"
    );

    // After promotion the WAL is left closed — the next write lazily
    // opens a fresh one. wal/ should therefore be empty until we write
    // again.
    let wal_count = fs::read_dir(base.join("wal"))
        .unwrap()
        .filter(|e| e.is_ok())
        .count();
    assert_eq!(
        wal_count, 0,
        "expected no WAL file after promotion (lazy open)"
    );
    vol.write(0, &vec![0xAB; 4096]).unwrap();
    let wal_count = fs::read_dir(base.join("wal"))
        .unwrap()
        .filter(|e| e.is_ok())
        .count();
    assert_eq!(
        wal_count, 1,
        "expected exactly one WAL file after first post-promote write"
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn entry_count_threshold_triggers_needs_promote() {
    // FLUSH_ENTRY_THRESHOLD must trip even when the WAL byte size is far
    // below FLUSH_THRESHOLD. Use Zero writes — each appends a single
    // entry of zero body bytes — so we cap on entry count, not byte size.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Write FLUSH_ENTRY_THRESHOLD - 1 zero entries, each one block
    // wide at a unique LBA. After this the WAL is one entry below
    // the cap; needs_promote() must still return false.
    for i in 0..(FLUSH_ENTRY_THRESHOLD as u64 - 1) {
        vol.write_zeroes(i, 1).unwrap();
    }
    assert!(
        !vol.needs_promote(),
        "needs_promote() should be false at {} entries (cap is {})",
        FLUSH_ENTRY_THRESHOLD - 1,
        FLUSH_ENTRY_THRESHOLD,
    );

    // One more entry pushes the WAL to exactly FLUSH_ENTRY_THRESHOLD;
    // needs_promote() must now return true even though WAL bytes are
    // a tiny fraction of FLUSH_THRESHOLD.
    vol.write_zeroes(FLUSH_ENTRY_THRESHOLD as u64 - 1, 1)
        .unwrap();
    assert!(
        vol.needs_promote(),
        "needs_promote() should be true at {} entries",
        FLUSH_ENTRY_THRESHOLD,
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn recovery_rebuilds_lbamap() {
    let base = keyed_temp_dir();

    // Write two blocks, fsync, then drop (simulates clean shutdown before promotion).
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![1u8; 4096]).unwrap();
        vol.write(1, &vec![2u8; 4096]).unwrap();
        vol.fsync().unwrap();
    }

    // Reopen — lbamap should contain both blocks.
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.lbamap_len(), 2);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn read_unwritten_returns_zeros() {
    let base = keyed_temp_dir();
    let vol = Volume::open(&base, &base).unwrap();
    let data = vol.read(0, 4).unwrap();
    assert_eq!(data.len(), 4 * 4096);
    assert!(data.iter().all(|&b| b == 0));
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_zeroes_reads_back_as_zeros() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Write real data, then zero it out.
    vol.write(0, &vec![0xabu8; 4096]).unwrap();
    vol.write_zeroes(0, 4).unwrap();

    let result = vol.read(0, 4).unwrap();
    assert_eq!(result.len(), 4 * 4096);
    assert!(result.iter().all(|&b| b == 0));

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_zeroes_no_data_in_segment() {
    // After write_zeroes + promote, the segment has a zero entry with no body bytes.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write_zeroes(0, 16).unwrap();
    vol.flush_wal().unwrap();

    let seg_path = segment::collect_segment_files(&base.join("pending"))
        .unwrap()
        .into_iter()
        .next()
        .expect("expected one pending segment");

    let (_, entries, _) = segment::read_segment_index(&seg_path).unwrap();
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.kind, segment::EntryKind::Zero);
    assert_eq!(e.stored_length, 0);
    assert_eq!(e.start_lba, 0);
    assert_eq!(e.lba_length, 16);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_after_zeroes_overrides() {
    // Data written after write_zeroes should be readable.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write_zeroes(0, 4).unwrap();
    let payload = vec![0x77u8; 4096];
    vol.write(0, &payload).unwrap();

    let result = vol.read(0, 1).unwrap();
    assert_eq!(result, payload);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_zeroes_survives_wal_recovery() {
    let base = keyed_temp_dir();

    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write_zeroes(5, 8).unwrap();
        vol.fsync().unwrap();
        // Drop without promoting — WAL remains.
    }

    // Reopen: WAL is replayed; zeroed range should read as zeros.
    let vol = Volume::open(&base, &base).unwrap();
    let result = vol.read(5, 8).unwrap();
    assert!(result.iter().all(|&b| b == 0));

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn write_zeroes_masks_ancestor_data() {
    // An explicit zero in the child masks ancestor data at those LBAs.
    let by_id = temp_dir();
    let ancestor_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    write_test_keypair(&ancestor_dir);

    // Write data in ancestor, promote, snapshot.
    {
        let mut vol = Volume::open(&ancestor_dir, &by_id).unwrap();
        vol.write(0, &vec![0xbbu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.snapshot().unwrap();
    }

    // Fork and zero the LBA in the child.
    fork_volume(&child_dir, &ancestor_dir).unwrap();
    let mut child_vol = Volume::open(&child_dir, &by_id).unwrap();
    child_vol.write_zeroes(0, 1).unwrap();

    let result = child_vol.read(0, 1).unwrap();
    assert!(
        result.iter().all(|&b| b == 0),
        "zero extent should mask ancestor data"
    );

    fs::remove_dir_all(by_id).unwrap();
}

#[test]
fn read_written_data_same_session() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let payload = vec![0x42u8; 4096];
    vol.write(5, &payload).unwrap();

    // Written block reads back correctly.
    let result = vol.read(5, 1).unwrap();
    assert_eq!(result, payload);

    // Adjacent unwritten blocks are zero.
    let before = vol.read(4, 1).unwrap();
    assert!(before.iter().all(|&b| b == 0));

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn read_multi_block_extent() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Write 4 blocks with distinct fill bytes so we can verify each block.
    let mut payload = Vec::with_capacity(4 * 4096);
    for fill in [0xAAu8, 0xBB, 0xCC, 0xDD] {
        payload.extend_from_slice(&[fill; 4096]);
    }
    vol.write(10, &payload).unwrap();

    let result = vol.read(10, 4).unwrap();
    assert_eq!(result, payload);

    // Reading a sub-range within the extent.
    let mid = vol.read(11, 2).unwrap();
    assert_eq!(mid, payload[4096..3 * 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn read_after_promote() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let payload = vec![0x55u8; 4096];
    vol.write(0, &payload).unwrap();
    vol.promote_for_test().unwrap();

    // After promotion, data lives in pending/<ulid>; reads must still work.
    let result = vol.read(0, 1).unwrap();
    assert_eq!(result, payload);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn read_after_reopen() {
    let base = keyed_temp_dir();

    let payload = vec![0x77u8; 4096];
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(3, &payload).unwrap();
        vol.fsync().unwrap();
    }

    // Reopen: WAL recovery must restore both the LBA map and extent index.
    let vol = Volume::open(&base, &base).unwrap();
    let result = vol.read(3, 1).unwrap();
    assert_eq!(result, payload);

    fs::remove_dir_all(base).unwrap();
}

/// Regression: compressed WAL entries must be promoted with the correct
/// SegmentFlags::COMPRESSED so reads after recovery+promote work.
///
/// WalFlags::COMPRESSED=0x01; SegmentFlags::COMPRESSED=0x04.
/// recover_wal must translate between them before calling new_data().
#[test]
fn compressed_entry_survives_recover_and_promote() {
    let base = keyed_temp_dir();

    // Write compressible data (zeros compress very well).
    let payload = vec![0u8; 4096];
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &payload).unwrap();
        vol.fsync().unwrap();
        // Drop without promoting — WAL contains the compressed entry.
    }

    // Reopen (recover_wal runs) then promote (writes segment).
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.promote_for_test().unwrap();
    }

    // Reopen again and read — must not fail with "failed to fill whole buffer".
    let vol = Volume::open(&base, &base).unwrap();
    let result = vol.read(0, 1).unwrap();
    assert_eq!(result, payload);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn ulid_is_unique_and_sortable() {
    let u1 = Ulid::new().to_string();
    let u2 = Ulid::new().to_string();
    assert_eq!(u1.len(), 26);
    assert_ne!(u1, u2);
    // ULIDs generated in sequence should sort correctly (same millisecond
    // is not guaranteed, but two different values prove uniqueness).
}

#[test]
fn recovery_after_promotion() {
    // Write enough to trigger a promotion, drop, reopen — the LBA map must
    // be rebuilt from both pending/ segments and the remaining WAL.
    let base = keyed_temp_dir();

    {
        let mut vol = Volume::open(&base, &base).unwrap();
        let block = vec![0u8; 1024 * 1024]; // 1 MiB = 256 LBAs
        for i in 0u64..33 {
            vol.write(i * 256, &block).unwrap();
        }
        vol.fsync().unwrap();
    }

    // All 33 extents should survive across the promotion boundary.
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.lbamap_len(), 33);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn promotion_after_wal_recovery() {
    // Write to the WAL, drop (simulating a crash), reopen (WAL recovery),
    // promote, then reopen again — verifies that pending_entries is correctly
    // rebuilt from the recovered WAL so the segment contains the pre-crash writes.
    let base = keyed_temp_dir();

    // Phase 1: write two blocks, fsync, drop.
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![1u8; 4096]).unwrap();
        vol.write(1, &vec![2u8; 4096]).unwrap();
        vol.fsync().unwrap();
    }

    // Phase 2: recover and immediately promote.
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 2); // confirm recovery
        vol.promote_for_test().unwrap();
    }

    // Phase 3: reopen — both blocks must now come from the pending/ segment.
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.lbamap_len(), 2);

    // Confirm the promoted segment landed correctly: one file in pending/.
    let pending_count = fs::read_dir(base.join("pending"))
        .unwrap()
        .filter(|e| e.is_ok())
        .count();
    assert_eq!(pending_count, 1, "expected one segment file in pending/");

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn wal_deleted_when_pending_segment_exists() {
    // Simulate a crash between the segment rename and the WAL delete:
    // both wal/<ulid> and pending/<ulid> exist. On reopen, the WAL must
    // be silently discarded and data read from the committed segment.
    let base = keyed_temp_dir();

    // Phase 1: write two blocks and promote so a segment lands in pending/.
    let ulid;
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![0xaau8; 4096]).unwrap();
        vol.write(1, &vec![0xbbu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        // Grab the segment ULID (there is exactly one file in pending/).
        let entry = fs::read_dir(base.join("pending"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let filename = entry.file_name();
        ulid = filename.to_string_lossy().into_owned();
    }

    // Simulate the crash: copy the segment back as a WAL file so both exist.
    fs::copy(
        base.join("pending").join(&ulid),
        base.join("wal").join(&ulid),
    )
    .unwrap();

    // Reopen — should delete the stale WAL and load cleanly from the segment.
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.lbamap_len(), 2);
    assert!(
        vol.read(0, 1).unwrap().iter().all(|&b| b == 0xaa),
        "LBA 0 should be 0xaa"
    );
    assert!(
        vol.read(1, 1).unwrap().iter().all(|&b| b == 0xbb),
        "LBA 1 should be 0xbb"
    );
    // The stale WAL file should be gone.
    assert!(
        !base.join("wal").join(&ulid).exists(),
        "stale WAL was not removed"
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn recovery_replays_all_wals_promoting_non_latest() {
    // Multiple WAL files on disk — e.g. left by a crash between
    // `segment::write_and_commit` and the old-WAL unlink, or
    // produced by the upcoming off-actor worker — must be
    // collapsed back to a single active WAL before `Volume::open`
    // returns. Every non-latest WAL is promoted to a fresh pending
    // segment; the highest-ULID WAL stays active.
    let base = keyed_temp_dir();

    // Bootstrap to create the standard directory layout + keypair.
    // The bootstrap open leaves an empty WAL that we then strip so
    // we can build our own two-WAL state from scratch.
    {
        let _vol = Volume::open(&base, &base).unwrap();
    }
    let wal_dir = base.join("wal");
    for entry in fs::read_dir(&wal_dir).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }

    // Two ULIDs with a strict ordering. Fixed strings keep the
    // test deterministic independently of the system clock.
    let low_ulid = Ulid::from_string("01AAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
    let high_ulid = Ulid::from_string("01BBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
    assert!(low_ulid < high_ulid);

    // Low WAL: one DATA record covering LBA 0.
    let payload_low = vec![0x11u8; 4096];
    let hash_low = blake3::hash(&payload_low);
    {
        let mut wl = writelog::WriteLog::create(&wal_dir.join(low_ulid.to_string())).unwrap();
        wl.append_data(0, 1, &hash_low, writelog::WalFlags::empty(), &payload_low)
            .unwrap();
        wl.fsync().unwrap();
    }

    // High WAL: one DATA record covering LBA 1.
    let payload_high = vec![0x22u8; 4096];
    let hash_high = blake3::hash(&payload_high);
    {
        let mut wl = writelog::WriteLog::create(&wal_dir.join(high_ulid.to_string())).unwrap();
        wl.append_data(1, 1, &hash_high, writelog::WalFlags::empty(), &payload_high)
            .unwrap();
        wl.fsync().unwrap();
    }

    // Reopen — recovery must promote `low_ulid` to a fresh segment
    // and keep `high_ulid` as the active WAL.
    let vol = Volume::open(&base, &base).unwrap();

    // Exactly one WAL remains: the high one.
    let wal_files: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name().into_string().unwrap()))
        .collect();
    assert_eq!(
        wal_files.len(),
        1,
        "expected one active WAL after recovery, got {wal_files:?}"
    );
    assert_eq!(wal_files[0], high_ulid.to_string());

    // Exactly one segment in pending/ — the recovery-promoted low
    // WAL, at a freshly-minted ULID strictly above the wal floor.
    let pending_files: Vec<_> = fs::read_dir(base.join("pending"))
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name().into_string().unwrap()))
        .filter(|n| !n.ends_with(".tmp"))
        .collect();
    assert_eq!(
        pending_files.len(),
        1,
        "expected one recovery-promoted segment in pending/, got {pending_files:?}"
    );
    let seg_ulid = Ulid::from_string(&pending_files[0]).unwrap();
    assert!(
        seg_ulid > high_ulid,
        "recovery-promoted segment ULID {seg_ulid} must sort above wal floor {high_ulid}"
    );

    // Both LBAs read back correctly. LBA 0 comes from the promoted
    // segment; LBA 1 from the active WAL's pending_entries.
    assert_eq!(vol.read(0, 1).unwrap(), payload_low);
    assert_eq!(vol.read(1, 1).unwrap(), payload_high);
    assert_eq!(vol.lbamap_len(), 2);

    fs::remove_dir_all(base).unwrap();
}

// --- durability guarantee tests ---
//
// These tests make the crash-recovery guarantees from docs/formats.md explicit
// and executable. They simulate the intermediate filesystem states that can
// arise from a machine crash at each step of the promotion commit sequence,
// and verify that Volume::open() recovers correctly in each case.
//
// What these tests cannot cover: whether sync_data() / fsync_dir() actually
// flush to physical media. That requires hardware fault injection (dm-flakey,
// CrashMonkey, etc.) and is out of scope for a unit test suite.

#[test]
fn recovery_reads_data_after_promotion_and_reopen() {
    // Guarantee: after flush_wal() completes (WAL promoted to pending/),
    // a subsequent Volume::open() reads the correct data from the segment.
    // This covers the common path: crash after a guest fsync, before the
    // coordinator uploads the segment to S3.
    let base = keyed_temp_dir();

    let payload_a = vec![0xAAu8; 4096];
    let payload_b = vec![0xBBu8; 4096];
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &payload_a).unwrap();
        vol.write(1, &payload_b).unwrap();
        // promote_for_test flushes the WAL to pending/ and opens a fresh WAL.
        vol.promote_for_test().unwrap();
        // Drop without explicit shutdown — simulates a process crash after promotion.
    }

    // On reopen, data must come from the pending/ segment.
    // The fresh empty WAL (opened after promotion) contributes nothing.
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), payload_a);
    assert_eq!(vol.read(1, 1).unwrap(), payload_b);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn recovery_removes_tmp_orphans() {
    // Guarantee: a .tmp file left in pending/ by a crashed segment write
    // (crash between write_segment and rename — the rename never committed)
    // is removed by Volume::open() and does not affect recovery.
    // The WAL is intact as a fallback and is replayed normally.
    let base = keyed_temp_dir();

    let payload = vec![0xCCu8; 4096];
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &payload).unwrap();
        vol.fsync().unwrap();
        // Drop with WAL intact — simulates crash before/during promotion.
    }

    // Simulate a crash mid-promotion: a .tmp file exists in pending/ but
    // no completed segment (the rename never happened).
    let orphan = base.join("pending").join("01AAAAAAAAAAAAAAAAAAAAAAAAA.tmp");
    fs::write(&orphan, b"incomplete segment bytes").unwrap();

    // Recovery must succeed, data must be correct, and the orphan removed.
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.lbamap_len(), 1);
    assert_eq!(vol.read(0, 1).unwrap(), payload);
    assert!(!orphan.exists(), ".tmp orphan should be cleaned up on open");

    fs::remove_dir_all(base).unwrap();
}

// --- compaction tests ---

#[test]
fn repack_noop_when_all_live() {
    // Write two blocks, promote, compact — nothing should be compacted
    // since all data is still referenced.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.write(1, &vec![0x22u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    let stats = vol.repack(0.7).unwrap();
    assert_eq!(stats.segments_compacted, 0);
    assert_eq!(stats.bytes_freed, 0);
    assert_eq!(stats.extents_removed, 0);

    // Data still readable.
    assert_eq!(vol.read(0, 1).unwrap(), vec![0x11u8; 4096]);
    assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn repack_reclaims_overwritten_extent() {
    // Write block A, promote, overwrite block A with B, promote.
    // First segment now has a dead extent; compaction should reclaim it.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let original = vec![0x11u8; 4096];
    let replacement = vec![0x22u8; 4096];

    vol.write(0, &original).unwrap();
    vol.promote_for_test().unwrap();

    vol.write(0, &replacement).unwrap();
    vol.promote_for_test().unwrap();

    // Two segments: first is 100% dead, second is live.
    let stats = vol.repack(0.7).unwrap();
    assert_eq!(
        stats.segments_compacted, 1,
        "first segment should be compacted"
    );
    assert!(stats.bytes_freed > 0);
    assert_eq!(stats.extents_removed, 1);

    // Data still reads back correctly after compaction.
    assert_eq!(vol.read(0, 1).unwrap(), replacement);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn repack_reads_back_correctly_after_reopen() {
    // Verify that the compacted segment is a valid segment that survives
    // a volume reopen (LBA map rebuild + extent index rebuild).
    let base = keyed_temp_dir();

    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0xBBu8; 4096]).unwrap(); // overwrite
        vol.promote_for_test().unwrap();
        vol.repack(0.7).unwrap();
    }

    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), vec![0xBBu8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn repack_partial_segment() {
    // Segment has two extents; one is overwritten (dead), one is live.
    // Compaction should rewrite the segment keeping only the live extent.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(0, &vec![0x11u8; 4096]).unwrap(); // will be overwritten
    vol.write(1, &vec![0x22u8; 4096]).unwrap(); // stays live
    vol.promote_for_test().unwrap();

    vol.write(0, &vec![0x33u8; 4096]).unwrap(); // overwrites LBA 0
    vol.promote_for_test().unwrap();

    // First segment is 50% dead — above default threshold of 30% dead (0.7 live).
    let stats = vol.repack(0.7).unwrap();
    assert_eq!(stats.segments_compacted, 1);
    assert!(stats.bytes_freed > 0);

    // Both LBAs read back correctly.
    assert_eq!(vol.read(0, 1).unwrap(), vec![0x33u8; 4096]);
    assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn repack_respects_min_live_ratio() {
    // With a strict ratio (1.0), any dead byte triggers compaction.
    // With a lenient ratio (0.0), nothing is ever compacted.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(0, &vec![0x22u8; 4096]).unwrap(); // LBA 0 now dead in seg 1
    vol.promote_for_test().unwrap();

    // Lenient threshold: first segment is 100% dead but ratio=0.0 → nothing compacted.
    let stats = vol.repack(0.0).unwrap();
    assert_eq!(stats.segments_compacted, 0);

    // Strict threshold: compact anything with any dead bytes.
    let stats = vol.repack(1.0).unwrap();
    assert_eq!(stats.segments_compacted, 1);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn repack_does_not_touch_pre_snapshot_segments() {
    // Write and overwrite a block, then snapshot. The dead segment is
    // pre-snapshot and must not be compacted — it is frozen by the floor.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(0, &vec![0x22u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    // Snapshot freezes both segments (floor = latest segment ULID).
    vol.snapshot().unwrap();

    // Even with a strict threshold the pre-snapshot segments must be skipped.
    let stats = vol.repack(1.0).unwrap();
    assert_eq!(
        stats.segments_compacted, 0,
        "pre-snapshot segments must not be compacted"
    );

    // Data still readable.
    assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn repack_only_touches_post_snapshot_segments() {
    // Pre-snapshot dead segment: frozen. Post-snapshot dead segment: compactable.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Pre-snapshot: write and overwrite LBA 0.
    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(0, &vec![0x22u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    vol.snapshot().unwrap();

    // Post-snapshot: write and overwrite LBA 1.
    vol.write(1, &vec![0x33u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(1, &vec![0x44u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    // One pre-snapshot dead segment (frozen) + one post-snapshot dead segment (eligible).
    let stats = vol.repack(1.0).unwrap();
    assert_eq!(
        stats.segments_compacted, 1,
        "exactly the post-snapshot dead segment should be compacted"
    );

    // Both LBAs read back correctly.
    assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);
    assert_eq!(vol.read(1, 1).unwrap(), vec![0x44u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn repack_does_not_touch_uploaded_segments() {
    // Simulate an uploaded segment (promoted to cache/ by the coordinator).
    // repack() must not touch it even if its extents are dead.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    // Simulate coordinator upload + promote IPC: pending → index/ + cache/.
    simulate_upload(&mut vol);

    // Overwrite LBA 0 — the uploaded segment's extent is now dead.
    vol.write(0, &vec![0x22u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    // Strict threshold: repack anything with dead bytes.
    let stats = vol.repack(1.0).unwrap();
    assert_eq!(
        stats.segments_compacted, 0,
        "repack must not touch uploaded (cache/) segments"
    );

    // Data still reads correctly.
    assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

// --- sweep_pending tests ---

#[test]
fn sweep_pending_noop_when_all_live() {
    // Single pending segment with no dead extents: sweep_pending must not
    // rewrite it. Rewriting a single all-live small segment is a no-op that
    // only wastes IO — merging only makes sense when >=2 segments combine or
    // dead space is reclaimed.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();
    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.write(1, &vec![0x22u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    let stats = vol.sweep_pending().unwrap();
    assert_eq!(stats.segments_compacted, 0);
    assert_eq!(stats.new_segments, 0);
    assert_eq!(vol.read(0, 1).unwrap(), vec![0x11u8; 4096]);
    assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn sweep_pending_removes_dead_extents() {
    // Write LBA 0, promote, overwrite LBA 0, promote.
    // sweep_pending should remove the dead extent from the first segment.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(0, &vec![0x22u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    let stats = vol.sweep_pending().unwrap();
    assert!(stats.segments_compacted >= 1);
    assert!(stats.bytes_freed > 0);
    assert_eq!(stats.extents_removed, 1);

    // Current value of LBA 0 must be the replacement.
    assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn sweep_pending_only_scans_pending_not_uploaded() {
    // Upload a segment (simulate coordinator promoting pending → cache/).
    // sweep_pending must not touch uploaded segments.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    // Simulate coordinator upload + promote IPC: pending → index/ + cache/.
    simulate_upload(&mut vol);

    // Now overwrite LBA 0 and promote — creates a new pending segment.
    vol.write(0, &vec![0x22u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    let stats = vol.sweep_pending().unwrap();
    // The old dead extent is in cache/ — sweep_pending doesn't touch it.
    assert_eq!(stats.extents_removed, 0);
    // The new pending segment is small and all-live: single segment, no
    // dead extents, so sweep_pending correctly leaves it alone.
    assert_eq!(stats.segments_compacted, 0);

    // Data still reads correctly.
    assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn sweep_pending_respects_snapshot_floor() {
    // Segments at or below the snapshot ULID must not be touched.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Write and promote before snapshot.
    vol.write(0, &vec![0x11u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(0, &vec![0x22u8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    vol.snapshot().unwrap();

    // The two pre-snapshot segments are now frozen.
    let stats = vol.sweep_pending().unwrap();
    assert_eq!(
        stats.segments_compacted, 0,
        "pre-snapshot segments must not be touched"
    );

    assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn sweep_pending_multi_block_inplace_overwrite_same_wal() {
    // Regression: two multi-block DATA writes at the same LBA range in the
    // same WAL flush. Both land as DATA entries (different hashes) in one
    // pending segment. sweep_pending then partitions entries into live/dead,
    // rewrites the segment, and updates the extent index — the surviving
    // live entry must read back correctly from the rewritten segment.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // High-entropy so neither payload is inlined and both stay in the
    // body section. Eight 4 KiB blocks each.
    let payload_a: Vec<u8> = (0..8 * 4096usize).map(|i| (i * 7 + 13) as u8).collect();
    let payload_b: Vec<u8> = (0..8 * 4096usize).map(|i| (i * 11 + 3) as u8).collect();
    assert_ne!(payload_a, payload_b);

    vol.write(24, &payload_a).unwrap();
    vol.write(24, &payload_b).unwrap();
    vol.flush_wal().unwrap();
    assert_eq!(
        vol.read(24, 8).unwrap(),
        payload_b,
        "pre-sweep read must return the second write"
    );

    vol.sweep_pending().unwrap();
    assert_eq!(
        vol.read(24, 8).unwrap(),
        payload_b,
        "post-sweep read must still return the second write"
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn sweep_pending_merges_multiple_small_segments() {
    // Three separate promotes → three small pending segments.
    // sweep_pending should merge them into one.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    vol.write(0, &vec![0xaau8; 4096]).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(1, &vec![0xbbu8; 4096]).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(2, &vec![0xccu8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    let stats = vol.sweep_pending().unwrap();
    assert_eq!(stats.segments_compacted, 3);
    assert_eq!(stats.new_segments, 1);

    // All three LBAs must still read back correctly.
    assert_eq!(vol.read(0, 1).unwrap(), vec![0xaau8; 4096]);
    assert_eq!(vol.read(1, 1).unwrap(), vec![0xbbu8; 4096]);
    assert_eq!(vol.read(2, 1).unwrap(), vec![0xccu8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

/// Build a 4 KiB block whose first byte is `seed` and the rest are
/// pseudo-random — high entropy so compression stays a no-op.
fn unique_block(seed: u32) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    let s = seed as u64;
    for (i, b) in buf.iter_mut().enumerate() {
        // Distinct per-(seed,i) using a cheap hash. Coprime multipliers
        // keep the byte distribution uniform.
        *b = ((s.wrapping_mul(0x9E37_79B9).wrapping_add(i as u64)) ^ (i as u64 * 31)) as u8;
    }
    buf
}

/// Promote `block_count` distinct 4 KiB blocks into one pending segment.
fn promote_segment_with_blocks(vol: &mut Volume, base_lba: u64, block_count: u64, tag: u32) {
    for i in 0..block_count {
        // Mix `tag` into the seed so different segments don't dedup.
        let block = unique_block(tag.wrapping_mul(0x10001).wrapping_add(i as u32));
        vol.write(base_lba + i, &block).unwrap();
    }
    vol.promote_for_test().unwrap();
}

#[test]
fn sweep_pending_packs_small_with_filler() {
    // One small (~4 KiB live) + one large filler (~17 MiB live).
    // Tier 1 picks up the small; tier 2 sees ~32 MiB - 4 KiB headroom
    // and pulls in the 17 MiB filler. Output is one ~17 MiB segment.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Small segment: 1 block.
    promote_segment_with_blocks(&mut vol, 0, 1, 1);
    // Filler: 17 MiB live (4352 blocks of 4 KiB).
    // Above the 16 MiB SWEEP_SMALL_THRESHOLD so it's filler material,
    // not a small. Must fit in the 32 MiB budget after the small.
    promote_segment_with_blocks(&mut vol, 1, 4352, 2);

    let stats = vol.sweep_pending().unwrap();
    assert_eq!(
        stats.segments_compacted, 2,
        "tier 2 must pull the filler in alongside the small"
    );
    assert_eq!(stats.new_segments, 1);

    // Both ranges must still read back correctly.
    assert_eq!(vol.read(0, 1).unwrap(), unique_block(0x10001));
    assert_eq!(vol.read(1, 1).unwrap(), unique_block(0x10001 * 2));
    assert_eq!(
        vol.read(4352, 1).unwrap(),
        unique_block(0x10001u32.wrapping_mul(2).wrapping_add(4351))
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn sweep_pending_respects_entry_cap() {
    // Three pending segments, each carrying 4096 DedupRef entries
    // (live_bytes = 0 — DedupRef has no body cost) plus one tiny
    // DATA segment, total 12_289 entries. Without an entry cap,
    // tier-1 packing would admit all three (byte budget never bites
    // on 0-live_bytes inputs) and produce a 12_289-entry output —
    // far past the WAL's flush cap. With SWEEP_ENTRY_CAP = 8192,
    // tier 1 admits exactly two of the dedup segments (8192 entries)
    // and stops; the third dedup segment and the lone DATA are left
    // for a later pass.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Anchor segment: a single DATA entry establishing the dedup hash.
    let payload = unique_block(0xCAFE);
    vol.write(0, &payload).unwrap();
    vol.promote_for_test().unwrap();

    // Two dedup-only pending segments, 4096 DedupRef entries each.
    for i in 1..=4096u64 {
        vol.write(i, &payload).unwrap();
    }
    vol.promote_for_test().unwrap();
    for i in 100_000..(100_000u64 + 4096) {
        vol.write(i, &payload).unwrap();
    }
    vol.promote_for_test().unwrap();

    // Plus another dedup-only segment so the cap actually has to
    // refuse one of them.
    for i in 200_000..(200_000u64 + 4096) {
        vol.write(i, &payload).unwrap();
    }
    vol.promote_for_test().unwrap();

    let stats = vol.sweep_pending().unwrap();
    assert_eq!(
        stats.segments_compacted, 2,
        "sweep must stop at SWEEP_ENTRY_CAP — exactly two of the \
             three dedup segments fit (8192 entries), the third is left \
             for a later pass"
    );
    assert_eq!(stats.new_segments, 1);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn sweep_pending_skips_lone_filler() {
    // A single filler (~17 MiB live, no small to pair with) must
    // not be rewritten — sweep is for packing, not for moving large
    // segments around. Repack is what handles single-segment cleanup.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    promote_segment_with_blocks(&mut vol, 0, 4352, 1);

    let stats = vol.sweep_pending().unwrap();
    assert_eq!(stats.segments_compacted, 0);
    assert_eq!(stats.new_segments, 0);

    fs::remove_dir_all(base).unwrap();
}

// --- compression helper unit tests ---

/// Build a 4096-byte block where every byte is distinct (entropy = 8 bits/byte).
/// The LCG multiplier 109 (0x6D) is odd so it is coprime to 256, giving a
/// bijection on [0, 255] — each value appears exactly 16 times in 4096 bytes.
fn high_entropy_block(seed: u8) -> Vec<u8> {
    (0..4096u16)
        .map(|i| (i as u8).wrapping_mul(0x6D).wrapping_add(seed))
        .collect()
}

#[test]
fn shannon_entropy_all_same_byte() {
    assert_eq!(shannon_entropy(&vec![0x42u8; 4096]), 0.0);
}

#[test]
fn shannon_entropy_uniform_is_8_bits() {
    // 256 distinct values each appearing 16 times → exactly 8 bits/byte.
    let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
    let e = shannon_entropy(&data);
    assert!((e - 8.0).abs() < 0.01, "expected ~8.0, got {e}");
}

#[test]
fn maybe_compress_compresses_low_entropy() {
    // All-zeros: entropy = 0, compresses to almost nothing.
    let data = vec![0u8; 4096];
    let compressed = maybe_compress(&data).expect("expected compression to succeed");
    // Must achieve at least 1.5× ratio.
    assert!(compressed.len() * MIN_COMPRESSION_RATIO_NUM / MIN_COMPRESSION_RATIO_DEN < data.len());
}

#[test]
fn maybe_compress_skips_high_entropy() {
    let data = high_entropy_block(0);
    assert!(shannon_entropy(&data) > ENTROPY_THRESHOLD);
    assert!(maybe_compress(&data).is_none());
}

// --- volume read/write tests for compressed and uncompressed paths ---

#[test]
fn read_incompressible_data() {
    // High-entropy data must not be compressed, and must read back correctly.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let payload = high_entropy_block(0x5A);
    assert!(
        shannon_entropy(&payload) > ENTROPY_THRESHOLD,
        "test data must be incompressible"
    );

    vol.write(0, &payload).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), payload);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn compressed_and_uncompressed_extents_coexist() {
    // Write one compressible and one incompressible extent; both must read back correctly.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let compressible = vec![0xCCu8; 4096];
    let incompressible = high_entropy_block(0xA3);

    vol.write(0, &compressible).unwrap();
    vol.write(1, &incompressible).unwrap();

    assert_eq!(vol.read(0, 1).unwrap(), compressible);
    assert_eq!(vol.read(1, 1).unwrap(), incompressible);

    fs::remove_dir_all(base).unwrap();
}

// --- write-path dedup tests ---

#[test]
fn dedup_write_same_data_same_lba() {
    // Writing identical data to the same LBA twice: second write is a dedup hit.
    // The LBA map must have exactly one entry, reads must return the correct data.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data = vec![0x42u8; 4096];
    vol.write(0, &data).unwrap();
    vol.write(0, &data).unwrap();

    assert_eq!(vol.lbamap_len(), 1);
    assert_eq!(vol.read(0, 1).unwrap(), data);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn dedup_write_same_data_different_lba() {
    // Identical data written to two different LBAs: second write is a dedup hit.
    // Both LBA entries exist in the map; reads return the correct data from both.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data = vec![0x77u8; 4096];
    vol.write(0, &data).unwrap();
    vol.write(5, &data).unwrap();

    assert_eq!(vol.lbamap_len(), 2);
    assert_eq!(vol.read(0, 1).unwrap(), data);
    assert_eq!(vol.read(5, 1).unwrap(), data);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn dedup_ref_survives_promote_and_reopen() {
    // Write data, promote so it lands in pending/, then write the same data
    // to a new LBA (dedup REF in WAL). Reopen and verify both LBAs read back.
    let base = keyed_temp_dir();

    {
        let mut vol = Volume::open(&base, &base).unwrap();
        let data = vec![0xABu8; 4096];
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();
        // Second write: same data, different LBA → dedup hit, REF record in WAL.
        vol.write(1, &data).unwrap();
        vol.fsync().unwrap();
    }

    let vol = Volume::open(&base, &base).unwrap();
    let data = vec![0xABu8; 4096];
    assert_eq!(vol.read(0, 1).unwrap(), data);
    assert_eq!(vol.read(1, 1).unwrap(), data);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn dedup_ref_in_segment_survives_reopen() {
    // Write data, promote, write same data (REF in WAL), promote again so
    // the REF lands in a segment. Reopen and verify reads still work.
    let base = keyed_temp_dir();

    {
        let mut vol = Volume::open(&base, &base).unwrap();
        let data = vec![0xCDu8; 4096];
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &data).unwrap(); // REF
        vol.promote_for_test().unwrap(); // REF lands in segment
    }

    let vol = Volume::open(&base, &base).unwrap();
    let data = vec![0xCDu8; 4096];
    assert_eq!(vol.read(0, 1).unwrap(), data);
    assert_eq!(vol.read(1, 1).unwrap(), data);

    fs::remove_dir_all(base).unwrap();
}

// --- dedup-ref redact / promote regression tests ---

/// Helper: collect all pending segment ULIDs (excluding sidecars and tmps).
fn pending_ulids(base: &Path) -> Vec<ulid::Ulid> {
    let pending_dir = base.join("pending");
    let mut ulids: Vec<ulid::Ulid> = Vec::new();
    for entry in fs::read_dir(&pending_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.contains('.') {
            continue;
        }
        ulids.push(ulid::Ulid::from_string(&name).unwrap());
    }
    ulids.sort();
    ulids
}

#[test]
fn redact_segment_drops_hash_dead_data_entry() {
    // An entry whose LBA has been overwritten and whose hash is no longer
    // referenced anywhere must be dropped from `pending/<ulid>`'s index
    // entirely so deleted data never leaves the host. The surviving
    // segment contains only live entries; the dropped entry's body is
    // not in the file at all.
    // High-entropy data avoids compression below the inline threshold,
    // guaranteeing the entry lands in the body section (not inline).
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let secret: Vec<u8> = (0..8192).map(|i| (i * 17 + 31) as u8).collect();
    vol.write(0, &secret).unwrap();
    vol.promote_for_test().unwrap();

    let ulids = pending_ulids(&base);
    assert_eq!(ulids.len(), 1);
    let seg_ulid = ulids[0];

    // Overwrite LBA 0-1 with different content. Hash of `secret` is no
    // longer referenced anywhere → fully dead. Do not promote so the
    // overwrite stays in the WAL and the pending segment still holds the
    // now-dead entry.
    let replacement: Vec<u8> = (0..8192).map(|i| (i * 23 + 41) as u8).collect();
    vol.write(0, &replacement).unwrap();

    let secret_hash = blake3::hash(&secret);

    vol.redact_segment(seg_ulid).unwrap();

    // No tmp leftovers — rewrite is tmp+rename.
    let seg_path = base.join("pending").join(seg_ulid.to_string());
    assert!(seg_path.exists(), "pending/<ulid> must still exist");
    assert!(
        !base
            .join("pending")
            .join(format!("{}.tmp", seg_ulid))
            .exists(),
        "no .tmp should survive redact"
    );

    let (_, entries, _) =
        segment::read_and_verify_segment_index(&seg_path, &vol.verifying_key).unwrap();
    assert!(
        entries.iter().all(|e| e.hash != secret_hash),
        "redact must drop the hash-dead entry from the index"
    );

    // The dropped secret's high-entropy bytes must not be findable
    // anywhere in the rewritten segment.
    let bytes = fs::read(&seg_path).unwrap();
    let needle: &[u8] = &secret[..64];
    assert!(
        bytes.windows(needle.len()).all(|w| w != needle),
        "dropped entry body bytes must not remain in the segment file"
    );

    let _ = replacement; // replacement is never flushed; used only to update lbamap

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn promote_segment_after_redact_produces_correct_idx_and_present() {
    // After redact + promote, the .idx contains DedupRef entries and the
    // .present bitset marks only Data entries as present.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data = vec![0xDDu8; 4096];
    vol.write(0, &data).unwrap();
    vol.promote_for_test().unwrap();

    let after_first = pending_ulids(&base);
    let s1_ulid = after_first[0];

    vol.write(1, &data).unwrap(); // dedup hit
    vol.promote_for_test().unwrap();

    let after_second = pending_ulids(&base);
    let s2_ulid = *after_second.iter().find(|u| **u != s1_ulid).unwrap();

    // Redact and promote S2 (simulating the coordinator drain path).
    vol.redact_segment(s2_ulid).unwrap();
    vol.promote_segment(s2_ulid).unwrap();

    // The .idx should exist and contain DedupRef entries.
    let idx_path = base.join("index").join(format!("{}.idx", s2_ulid));
    assert!(
        idx_path.exists(),
        "index/<ulid>.idx must exist after promote"
    );

    let (_, idx_entries, _) =
        segment::read_and_verify_segment_index(&idx_path, &vol.verifying_key).unwrap();
    assert!(
        idx_entries.iter().any(|e| e.kind == EntryKind::DedupRef),
        "idx should contain DedupRef entries"
    );

    // The .present bitset should mark DedupRef entries as not-present.
    let present_path = base.join("cache").join(format!("{}.present", s2_ulid));
    assert!(present_path.exists(), ".present must exist after promote");
    for (i, entry) in idx_entries.iter().enumerate() {
        let present = segment::check_present_bit(&present_path, i as u32).unwrap_or(false);
        if entry.kind.is_data() {
            assert!(present, "Data-shaped entry {i} should be marked present");
        } else if entry.kind == EntryKind::DedupRef {
            assert!(!present, "DedupRef entry {i} should NOT be marked present");
        }
    }

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn reads_work_after_redact_and_promote() {
    // After redact + promote, reads must still work correctly.
    // DedupRef reads go through the extent index to the canonical segment.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data = vec![0xBBu8; 4096];
    vol.write(0, &data).unwrap();
    vol.promote_for_test().unwrap();

    let after_first = pending_ulids(&base);
    let s1_ulid = after_first[0];

    vol.write(1, &data).unwrap(); // dedup hit → DedupRef
    vol.promote_for_test().unwrap();

    let after_second = pending_ulids(&base);
    let s2_ulid = *after_second.iter().find(|u| **u != s1_ulid).unwrap();

    vol.redact_segment(s2_ulid).unwrap();
    vol.promote_segment(s2_ulid).unwrap();

    assert_eq!(vol.read(0, 1).unwrap(), data, "LBA 0 after redact+promote");
    assert_eq!(vol.read(1, 1).unwrap(), data, "LBA 1 after redact+promote");

    // Also verify after reopen (extent index rebuilt from .idx files).
    drop(vol);
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data, "LBA 0 after reopen");
    assert_eq!(vol.read(1, 1).unwrap(), data, "LBA 1 after reopen");

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn redact_segment_idempotent() {
    // A second redact call is a no-op because the first call already
    // punched all hash-dead DATA regions.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let secret: Vec<u8> = (0..8192).map(|i| (i * 17 + 31) as u8).collect();
    vol.write(0, &secret).unwrap();
    vol.promote_for_test().unwrap();

    let ulids = pending_ulids(&base);
    let seg_ulid = ulids[0];

    let replacement: Vec<u8> = (0..8192).map(|i| (i * 23 + 41) as u8).collect();
    vol.write(0, &replacement).unwrap();

    // First redact punches the dead region; second is a no-op.
    vol.redact_segment(seg_ulid).unwrap();
    vol.redact_segment(seg_ulid).unwrap();

    // Segment file still present, no sidecar produced.
    assert!(base.join("pending").join(seg_ulid.to_string()).exists());
    assert!(
        !base
            .join("pending")
            .join(format!("{}.materialized", seg_ulid))
            .exists()
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn redact_segment_no_op_when_all_live() {
    // A segment with no hash-dead DATA entries is untouched by redact:
    // the file is unchanged, no sidecar is produced.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data = vec![0x77u8; 4096];
    vol.write(0, &data).unwrap();
    vol.promote_for_test().unwrap();

    let ulids = pending_ulids(&base);
    let ulid = ulids[0];
    let seg_path = base.join("pending").join(ulid.to_string());
    let before = fs::read(&seg_path).unwrap();

    vol.redact_segment(ulid).unwrap();

    let after = fs::read(&seg_path).unwrap();
    assert_eq!(
        before, after,
        "redact with no dead DATA must not modify file"
    );
    assert!(
        !base
            .join("pending")
            .join(format!("{}.materialized", ulid))
            .exists(),
        "no sidecar should be produced"
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn redact_preserves_body_for_lba_dead_but_hash_alive_entry() {
    // Regression test: if a Data entry's LBA is overwritten but the same
    // hash is alive at another LBA, redact must NOT punch the body.
    // GC's collect_stats keeps such entries via extent+hash liveness, so
    // punching the body would cause GC to copy zeros into its output.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Use high-entropy data that won't compress below INLINE_THRESHOLD.
    let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
    // LBA 0-1 → DATA(hash=H). Also dedup-indexed.
    vol.write(0, &data).unwrap();
    // LBA 2-3 → dedup hit → DedupRef(hash=H). Hash H is now alive at LBAs 0 and 2.
    vol.write(2, &data).unwrap();
    vol.promote_for_test().unwrap();

    let ulids = pending_ulids(&base);
    assert_eq!(ulids.len(), 1);
    let seg_ulid = ulids[0];

    // Overwrite LBA 0-1 with different data. The DATA entry at LBA 0 is
    // now LBA-dead, but hash H is still alive at LBA 2.
    let other: Vec<u8> = (0..8192).map(|i| (i * 11 + 3) as u8).collect();
    vol.write(0, &other).unwrap();

    vol.redact_segment(seg_ulid).unwrap();

    // Verify the DATA entry at LBA 0 still has real body bytes (not zeros)
    // in the in-place pending file.
    use std::io::{Read as _, Seek as _, SeekFrom};
    let seg_path = base.join("pending").join(seg_ulid.to_string());
    let (bss, entries, _) =
        segment::read_and_verify_segment_index(&seg_path, &vol.verifying_key).unwrap();
    let data_entry = entries
        .iter()
        .find(|e| e.kind.is_data() && e.start_lba == 0)
        .expect("should have a Data entry at LBA 0");
    assert!(data_entry.stored_length > 0);

    let mut f = fs::File::open(&seg_path).unwrap();
    let mut body = vec![0u8; data_entry.stored_length as usize];
    f.seek(SeekFrom::Start(bss + data_entry.stored_offset))
        .unwrap();
    f.read_exact(&mut body).unwrap();
    assert!(
        body.iter().any(|&b| b != 0),
        "redact must NOT punch body of LBA-dead but hash-alive Data entry"
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn redact_drops_entry_when_hash_fully_dead() {
    // When both the LBA and the hash are dead (no LBA references the hash),
    // redact must drop the entry from the index. The dropped hash's body
    // bytes do not appear anywhere in the resulting pending file.
    // Uses high-entropy data that won't compress below INLINE_THRESHOLD.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
    vol.write(0, &data).unwrap();
    vol.promote_for_test().unwrap();

    let ulids = pending_ulids(&base);
    assert_eq!(ulids.len(), 1);
    let seg_ulid = ulids[0];

    // Overwrite LBA 0-1 with different data. Hash H is no longer alive
    // at any LBA.
    let other: Vec<u8> = (0..8192).map(|i| (i * 11 + 3) as u8).collect();
    vol.write(0, &other).unwrap();

    let dead_hash = blake3::hash(&data);

    vol.redact_segment(seg_ulid).unwrap();

    let seg_path = base.join("pending").join(seg_ulid.to_string());
    let (_, entries, _) =
        segment::read_and_verify_segment_index(&seg_path, &vol.verifying_key).unwrap();
    assert!(
        entries.iter().all(|e| e.hash != dead_hash),
        "redact must drop the fully-dead entry from the index"
    );

    // The original body bytes must not be findable in the segment file.
    let bytes = fs::read(&seg_path).unwrap();
    let needle: &[u8] = &data[..64];
    assert!(
        bytes.windows(needle.len()).all(|w| w != needle),
        "dead entry body bytes must not remain in the segment file"
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn redact_invalidates_extent_index_for_punched_hash() {
    // Regression: after redact punches a hash-dead DATA body, a later write
    // whose content hashes to the same value must not use the dedup
    // shortcut — the canonical body bytes are gone. Before the fix, the
    // surviving extent-index entry caused `write_commit` to emit a thin
    // DedupRef pointing at zero-punched bytes, so subsequent reads of the
    // new LBA returned zeros.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // High-entropy payloads so they stay in the body section (no inline).
    let payload_a: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
    let payload_b: Vec<u8> = (0..8192).map(|i| (i * 11 + 3) as u8).collect();

    // Seed LBA 28 with payload_A, flush so it lives in pending/.
    vol.write(28, &payload_a).unwrap();
    vol.promote_for_test().unwrap();

    let ulids = pending_ulids(&base);
    assert_eq!(ulids.len(), 1);
    let seg_ulid = ulids[0];

    // Overwrite LBA 28 with payload_B. Hash of payload_A is now LBA-dead
    // and no other LBA references it — hash-fully-dead.
    vol.write(28, &payload_b).unwrap();

    // Drain: redact (punches payload_A body bytes in place) then promote
    // to index/ + cache/. This mirrors the coordinator upload flow.
    vol.redact_segment(seg_ulid).unwrap();
    vol.promote_segment(seg_ulid).unwrap();

    // A fresh write with content matching payload_A. Without the fix, the
    // surviving extent-index entry for H_A makes `write_commit` emit a
    // DedupRef pointing at the (now zero) location in cache/<seg>.body.
    vol.write(100, &payload_a).unwrap();

    assert_eq!(
        vol.read(100, 2).unwrap(),
        payload_a,
        "new write of redacted content must read back correctly"
    );
    // Existing reads unaffected.
    assert_eq!(vol.read(28, 2).unwrap(), payload_b, "LBA 28 (overwrite)");

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn wal_recovery_with_thin_ref() {
    // Write data to LBA 0, promote to pending, then write same data to
    // LBA 1 (dedup hit → thin ref in WAL). Do NOT flush — leave the thin
    // ref in the WAL. Drop (crash), reopen, verify both LBAs read back.
    let base = keyed_temp_dir();
    let data = vec![0x99u8; 4096];

    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();
        // Second write: same data, different LBA → dedup hit → REF in WAL.
        vol.write(1, &data).unwrap();
        vol.fsync().unwrap();
        // Drop without promote — thin ref stays in WAL only.
    }

    // Reopen triggers WAL recovery.
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(
        vol.read(0, 1).unwrap(),
        data,
        "LBA 0 must survive crash with thin ref in WAL"
    );
    assert_eq!(
        vol.read(1, 1).unwrap(),
        data,
        "LBA 1 (thin ref) must survive crash with thin ref in WAL"
    );

    fs::remove_dir_all(base).unwrap();
}

/// Proptest regression: DedupWrite → Flush → DedupWrite (overwrite) →
/// Repack → DrainWithRedact.
///
/// Repack finds all entries in the first segment dead (overwritten by the
/// second DedupWrite) and removes the hash from the extent index. Before
/// the fix, repack left the segment file behind; the subsequent drain
/// then tried to process it, hit a DedupRef whose canonical hash was
/// gone, and panicked. The fix: repack deletes the segment file when all
/// entries are dead.
#[test]
fn repack_deletes_fully_dead_segment_before_drain() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Pre-snapshot segments (frozen by snapshot, skipped by repack).
    let data_a = vec![17u8; 4096];
    vol.write(2, &data_a).unwrap();
    vol.flush_wal().unwrap();
    for ulid in pending_ulids(&base) {
        vol.redact_segment(ulid).unwrap();
        vol.promote_segment(ulid).unwrap();
    }

    let data_b = vec![34u8; 4096];
    vol.write(3, &data_b).unwrap();
    vol.flush_wal().unwrap();
    for ulid in pending_ulids(&base) {
        vol.redact_segment(ulid).unwrap();
        vol.promote_segment(ulid).unwrap();
    }

    vol.snapshot().unwrap();

    // DedupWrite seed=0: LBA 0 (Data) + LBA 6 (DedupRef), same hash.
    let dedup_data_0 = vec![0u8; 4096];
    vol.write(0, &dedup_data_0).unwrap();
    vol.write(6, &dedup_data_0).unwrap();
    vol.flush_wal().unwrap();

    // DedupWrite seed=1: overwrite both LBAs with new data.
    let dedup_data_1 = vec![1u8; 4096];
    vol.write(0, &dedup_data_1).unwrap();
    vol.write(6, &dedup_data_1).unwrap();

    // Repack: the post-snapshot segment (seed=0) is now fully dead.
    // min_live_ratio=0.01 so the segment (0% live) is eligible.
    vol.repack(0.01).unwrap();

    // The fully-dead segment must have been deleted.
    let ulids = pending_ulids(&base);
    assert!(
        ulids.is_empty(),
        "repack should delete fully-dead segment, but found: {ulids:?}"
    );

    // DrainWithRedact: flush the WAL (seed=1), redact, promote.
    vol.flush_wal().unwrap();
    for ulid in pending_ulids(&base) {
        vol.redact_segment(ulid).unwrap();
        vol.promote_segment(ulid).unwrap();
    }

    // Verify reads.
    assert_eq!(vol.read(0, 1).unwrap(), dedup_data_1, "LBA 0");
    assert_eq!(vol.read(6, 1).unwrap(), dedup_data_1, "LBA 6");
    assert_eq!(vol.read(2, 1).unwrap(), data_a, "LBA 2 (pre-snapshot)");
    assert_eq!(vol.read(3, 1).unwrap(), data_b, "LBA 3 (pre-snapshot)");

    fs::remove_dir_all(base).unwrap();
}

/// Known failure: proptest minimal reproducer for dedup canonical overwrite
/// data loss. When PopulateFetched overwrites the extent index entry for a
/// hash that a DedupRef depends on, then DrainLocal removes pending/, then
/// GC runs, the thin ref's canonical body is lost. After crash, LBA 4
/// reads zeros instead of the expected data.
///
/// Un-ignore when the fix lands.
#[test]
#[ignore]
fn proptest_minimal_dedup_overwrite_data_loss() {
    let base = keyed_temp_dir();
    let fork_dir = base.clone();
    let mut vol = Volume::open(&base, &base).unwrap();

    // DedupWrite: write [1u8; 4096] to LBA 0 and LBA 4 (dedup hit on LBA 4).
    let data = [1u8; 4096];
    vol.write(0, &data).unwrap();
    vol.write(4, &data).unwrap();

    // Flush — promotes WAL to pending/.
    vol.flush_wal().unwrap();

    // PopulateFetched: write different data to cache for LBA 0,
    // overwriting the extent index entry for the original hash.
    let pop_ulid = vol.gc_checkpoint_for_test().unwrap();
    {
        // Use the common helper pattern from tests/common/mod.rs.
        let index_dir = fork_dir.join("index");
        let cache_dir = fork_dir.join("cache");
        let _ = fs::create_dir_all(&index_dir);
        let _ = fs::create_dir_all(&cache_dir);

        let seed = 128u8;
        let pop_data = vec![seed; 4096];
        let pop_hash = blake3::hash(&pop_data);
        let mut entries = vec![segment::SegmentEntry::new_data(
            pop_hash,
            0,
            1,
            segment::SegmentFlags::empty(),
            pop_data,
        )];

        let signer =
            crate::signing::load_signer(&fork_dir, crate::signing::VOLUME_KEY_FILE).unwrap();
        let tmp = cache_dir.join(format!("{pop_ulid}.tmp"));
        let bss = segment::write_segment(&tmp, &mut entries, signer.as_ref()).unwrap();
        let bytes = fs::read(&tmp).unwrap();
        fs::remove_file(&tmp).unwrap();

        let s = pop_ulid.to_string();
        fs::write(index_dir.join(format!("{s}.idx")), &bytes[..bss as usize]).unwrap();
        fs::write(cache_dir.join(format!("{s}.body")), &bytes[bss as usize..]).unwrap();
        segment::set_present_bit(&cache_dir.join(format!("{s}.present")), 0, 1).unwrap();
    }

    // DrainLocal: promote all pending segments to index/ + cache/.
    {
        let pending = fork_dir.join("pending");
        let index_dir = fork_dir.join("index");
        let cache_dir = fork_dir.join("cache");
        let _ = fs::create_dir_all(&index_dir);
        let _ = fs::create_dir_all(&cache_dir);
        if let Ok(entries) = fs::read_dir(&pending) {
            for entry in entries.flatten() {
                let name = entry.file_name().into_string().unwrap();
                if name.contains('.') {
                    continue;
                }
                let file_data = fs::read(entry.path()).unwrap();
                if file_data.len() < 96 {
                    continue;
                }
                let entry_count =
                    u32::from_le_bytes([file_data[8], file_data[9], file_data[10], file_data[11]]);
                let index_length = u32::from_le_bytes([
                    file_data[12],
                    file_data[13],
                    file_data[14],
                    file_data[15],
                ]);
                let inline_length = u32::from_le_bytes([
                    file_data[16],
                    file_data[17],
                    file_data[18],
                    file_data[19],
                ]);
                let bss = 96 + index_length as usize + inline_length as usize;
                if file_data.len() < bss {
                    continue;
                }
                let _ = fs::write(index_dir.join(format!("{name}.idx")), &file_data[..bss]);
                let _ = fs::write(cache_dir.join(format!("{name}.body")), &file_data[bss..]);
                let bitset_len = (entry_count as usize).div_ceil(8);
                let _ = fs::write(
                    cache_dir.join(format!("{name}.present")),
                    vec![0xFFu8; bitset_len],
                );
                let _ = fs::remove_file(entry.path());
            }
        }
    }

    // CoordGcLocal: run GC.
    {
        let gc_ulid = vol.gc_checkpoint_for_test().unwrap();
        vol.flush_wal().unwrap();
        // Need at least 2 segments for GC; use all available.
        let idx_files = segment::collect_idx_files(&fork_dir.join("index")).unwrap();
        if idx_files.len() >= 2 {
            let to_delete = {
                use crate::{extentindex, lbamap};
                let rebuild_chain = vec![(fork_dir.clone(), None)];
                let lba_map = lbamap::rebuild_segments(&rebuild_chain).unwrap();
                let _live_hashes = lba_map.lba_referenced_hashes();
                let extent_index = extentindex::rebuild(&rebuild_chain).unwrap();

                let vk =
                    crate::signing::load_verifying_key(&fork_dir, crate::signing::VOLUME_PUB_FILE)
                        .unwrap();
                let (ephemeral_signer, _) = crate::signing::generate_ephemeral_signer();

                let gc_dir = fork_dir.join("gc");
                let _ = fs::create_dir_all(&gc_dir);

                // Build candidates from all .idx files
                let mut candidates: Vec<(Ulid, PathBuf)> = idx_files
                    .iter()
                    .filter_map(|p| {
                        let stem = p.file_stem()?.to_str()?;
                        let ulid = Ulid::from_string(stem).ok()?;
                        Some((ulid, p.clone()))
                    })
                    .collect();
                candidates.sort_by_key(|(u, _)| *u);

                // Classify each candidate's entries and build a plan:
                // emit one `Keep` per entry that's still LBA-live or
                // extent-canonical. Mirrors the coordinator's `collect_stats`
                // → `PlanOutput::Keep` path for the fully-alive case.
                use crate::gc_plan::{GcPlan, PlanOutput};

                let mut outputs: Vec<PlanOutput> = Vec::new();
                let mut kept_any = false;
                for (ulid, path) in &candidates {
                    let Ok((_bss, seg_entries, _)) =
                        segment::read_and_verify_segment_index(path, &vk)
                    else {
                        continue;
                    };
                    for (entry_idx, e) in seg_entries.iter().enumerate() {
                        if e.kind == EntryKind::DedupRef {
                            continue;
                        }
                        let lba_live = lba_map.hash_at(e.start_lba) == Some(e.hash);
                        let extent_live = extent_index
                            .lookup(&e.hash)
                            .is_some_and(|loc| loc.segment_id == *ulid);
                        if lba_live || extent_live {
                            outputs.push(PlanOutput::Keep {
                                input: *ulid,
                                entry_idx: entry_idx as u32,
                            });
                            kept_any = true;
                        }
                    }
                }

                if kept_any {
                    let plan = GcPlan {
                        new_ulid: gc_ulid,
                        outputs,
                    };
                    let plan_path = gc_dir.join(format!("{gc_ulid}.plan"));
                    plan.write_atomic(&plan_path).unwrap();
                }
                let _ = ephemeral_signer;

                candidates
                    .iter()
                    .map(|(_, p)| p.clone())
                    .collect::<Vec<_>>()
            };
            let applied = vol.apply_gc_handoffs().unwrap_or(0);
            if applied > 0 {
                for path in &to_delete {
                    let _ = fs::remove_file(path);
                }
            }
        }
    }

    // Crash: drop and reopen.
    drop(vol);
    let vol = Volume::open(&base, &base).unwrap();

    // Assert LBA 4 reads [1u8; 4096] — the dedup ref target.
    // This is the assertion that currently fails due to the known bug.
    assert_eq!(
        vol.read(4, 1).unwrap(),
        vec![1u8; 4096],
        "LBA 4 (dedup ref) must read back original data after GC + crash"
    );

    fs::remove_dir_all(base).unwrap();
}

// --- walk_ancestors tests ---

#[test]
fn walk_ancestors_root_returns_empty() {
    let by_id = temp_dir();
    let vol_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    // No origin file → root volume; ancestors are empty.
    assert!(walk_ancestors(&vol_dir, &by_id).unwrap().is_empty());
}

#[test]
fn walk_ancestors_rejects_invalid_parent_entries() {
    let by_id = temp_dir();
    let fork_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    let bad_parents = [
        // not a ULID parent (old "base/" prefix)
        "base/01ARZ3NDEKTSV4RRFFQ69G5FAV",
        // path traversal attempt
        "../01AAAAAAAAAAAAAAAAAAAAAAAA/01ARZ3NDEKTSV4RRFFQ69G5FAV",
        // parent component is not a valid ULID
        "not-a-ulid/01ARZ3NDEKTSV4RRFFQ69G5FAV",
        // missing '/' separator entirely
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        // branch ULID missing
        "01AAAAAAAAAAAAAAAAAAAAAAAA/",
    ];
    for bad in bad_parents {
        write_test_provenance(&fork_dir, Some(bad), &[]);
        assert!(
            walk_ancestors(&fork_dir, &by_id).is_err(),
            "expected error for parent entry: {bad}"
        );
    }
}

#[test]
fn walk_ancestors_one_level() {
    let by_id = temp_dir();
    let parent_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let default_dir = by_id.join(parent_ulid);
    let dev_dir = by_id.join(child_ulid);

    // dev's provenance names default at a fixed branch ULID.
    write_test_provenance(
        &dev_dir,
        Some(&format!("{parent_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV")),
        &[],
    );

    let ancestors = walk_ancestors(&dev_dir, &by_id).unwrap();
    assert_eq!(ancestors.len(), 1);
    assert_eq!(ancestors[0].dir, default_dir);
    assert_eq!(
        ancestors[0].branch_ulid.as_deref(),
        Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
    );
}

#[test]
fn walk_ancestors_two_levels() {
    let by_id = temp_dir();
    let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let mid_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let leaf_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
    let default_dir = by_id.join(root_ulid);
    let mid_dir = by_id.join(mid_ulid);
    let leaf_dir = by_id.join(leaf_ulid);

    write_test_provenance(
        &mid_dir,
        Some(&format!("{root_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV")),
        &[],
    );
    write_test_provenance(
        &leaf_dir,
        Some(&format!("{mid_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV")),
        &[],
    );

    let ancestors = walk_ancestors(&leaf_dir, &by_id).unwrap();
    assert_eq!(ancestors.len(), 2);
    assert_eq!(ancestors[0].dir, default_dir);
    assert_eq!(ancestors[1].dir, mid_dir);
    assert_eq!(
        ancestors[0].branch_ulid.as_deref(),
        Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
    );
    assert_eq!(
        ancestors[1].branch_ulid.as_deref(),
        Some("01BX5ZZKJKTSV4RRFFQ69G5FAV")
    );
}

// --- walk_extent_ancestors tests ---

#[test]
fn walk_extent_ancestors_missing_file_is_empty() {
    let by_id = temp_dir();
    let vol_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    fs::create_dir_all(&vol_dir).unwrap();
    assert!(walk_extent_ancestors(&vol_dir, &by_id).unwrap().is_empty());
}

#[test]
fn walk_extent_ancestors_rejects_invalid_entries() {
    let by_id = temp_dir();
    let vol_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    let bad_entries = [
        "not-a-ulid/01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "../01AAAAAAAAAAAAAAAAAAAAAAAA/01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "01AAAAAAAAAAAAAAAAAAAAAAAA/",
        "01AAAAAAAAAAAAAAAAAAAAAAAA",
    ];
    for bad in bad_entries {
        write_test_provenance(&vol_dir, None, &[bad]);
        assert!(
            walk_extent_ancestors(&vol_dir, &by_id).is_err(),
            "expected error for extent_index entry: {bad}"
        );
    }
}

#[test]
fn walk_extent_ancestors_one_level() {
    let by_id = temp_dir();
    let parent_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let parent_dir = by_id.join(parent_ulid);
    let child_dir = by_id.join(child_ulid);
    let entry = format!("{parent_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
    write_test_provenance(&child_dir, None, &[&entry]);

    let ancestors = walk_extent_ancestors(&child_dir, &by_id).unwrap();
    assert_eq!(ancestors.len(), 1);
    assert_eq!(ancestors[0].dir, parent_dir);
    assert_eq!(
        ancestors[0].branch_ulid.as_deref(),
        Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
    );
}

#[test]
fn walk_extent_ancestors_multi_entry() {
    // Flat union of several sources in a single signed provenance.
    let by_id = temp_dir();
    let a_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let b_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
    let a_dir = by_id.join(a_ulid);
    let b_dir = by_id.join(b_ulid);
    let c_dir = by_id.join(c_ulid);
    let a_entry = format!("{a_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
    let b_entry = format!("{b_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
    write_test_provenance(&c_dir, None, &[&a_entry, &b_entry]);

    let layers = walk_extent_ancestors(&c_dir, &by_id).unwrap();
    assert_eq!(layers.len(), 2, "two sources expected");
    assert_eq!(layers[0].dir, a_dir);
    assert_eq!(layers[1].dir, b_dir);
    assert_eq!(
        layers[0].branch_ulid.as_deref(),
        Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
    );
    assert_eq!(
        layers[1].branch_ulid.as_deref(),
        Some("01BX5ZZKJKTSV4RRFFQ69G5FAV")
    );
}

#[test]
fn walk_extent_ancestors_dedupes_duplicate_entries() {
    let by_id = temp_dir();
    let a_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
    let a_dir = by_id.join(a_ulid);
    let c_dir = by_id.join(c_ulid);
    // Same source listed twice — later entry is silently dropped.
    let a1 = format!("{a_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
    let a2 = format!("{a_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
    write_test_provenance(&c_dir, None, &[&a1, &a2]);

    let layers = walk_extent_ancestors(&c_dir, &by_id).unwrap();
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0].dir, a_dir);
}

#[test]
fn walk_extent_ancestors_combined_with_walk_ancestors() {
    // A single signed provenance carrying both fork parent (P) and
    // extent-index source (X). walk_ancestors returns [P],
    // walk_extent_ancestors returns [X]: the two chains are distinct.
    let by_id = temp_dir();
    let p_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let x_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
    let p_dir = by_id.join(p_ulid);
    let x_dir = by_id.join(x_ulid);
    let c_dir = by_id.join(c_ulid);
    let parent_entry = format!("{p_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
    let extent_entry = format!("{x_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
    write_test_provenance(&c_dir, Some(&parent_entry), &[&extent_entry]);

    let fork_chain = walk_ancestors(&c_dir, &by_id).unwrap();
    let extent_chain = walk_extent_ancestors(&c_dir, &by_id).unwrap();
    assert_eq!(fork_chain.len(), 1);
    assert_eq!(fork_chain[0].dir, p_dir);
    assert_eq!(extent_chain.len(), 1);
    assert_eq!(extent_chain[0].dir, x_dir);
}

// --- ancestor-aware open / read integration test ---

/// Write data into a root volume, snapshot it, create a child volume via
/// fork_volume, and verify the child can read the ancestor's data.
#[test]
fn open_reads_ancestor_segments() {
    let by_id = temp_dir();
    let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    write_test_keypair(&default_dir);

    // Write data into the root volume and promote to a segment.
    let data = vec![0xABu8; 4096];
    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();
        vol.snapshot().unwrap();
    }

    // Create a child volume branched from default.
    fork_volume(&child_dir, &default_dir).unwrap();

    // Child should see the ancestor's data through layer merge.
    let vol = Volume::open(&child_dir, &by_id).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data);

    fs::remove_dir_all(by_id).unwrap();
}

/// Ancestor data is shadowed by a write in the live child volume.
#[test]
fn child_write_shadows_ancestor() {
    let by_id = temp_dir();
    let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    write_test_keypair(&default_dir);
    let ancestor_data = vec![0xAAu8; 4096];
    let child_data = vec![0xBBu8; 4096];

    // Write into the root volume, promote, snapshot.
    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &ancestor_data).unwrap();
        vol.promote_for_test().unwrap();
        vol.snapshot().unwrap();
    }

    // Create child volume, write different data at the same LBA, promote.
    fork_volume(&child_dir, &default_dir).unwrap();
    {
        let mut vol = Volume::open(&child_dir, &by_id).unwrap();
        vol.write(0, &child_data).unwrap();
        vol.promote_for_test().unwrap();
    }

    // Re-open child and verify child data wins.
    let vol = Volume::open(&child_dir, &by_id).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), child_data);

    fs::remove_dir_all(by_id).unwrap();
}

// --- lock tests ---

#[test]
fn double_open_same_fork_fails() {
    let fork_dir = keyed_temp_dir();
    let _vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    // Second open on the same live fork must fail (lock already held).
    assert!(Volume::open(&fork_dir, &fork_dir).is_err());
    fs::remove_dir_all(fork_dir).unwrap();
}

// --- snapshot() tests ---

#[test]
fn snapshot_writes_marker_and_stays_live() {
    let fork_dir = keyed_temp_dir();
    let data = vec![0xAAu8; 4096];

    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    vol.write(0, &data).unwrap();
    let snap_ulid = vol.snapshot().unwrap();

    // Fork still has wal/ (still live).
    assert!(fork_dir.join("wal").is_dir());
    // Snapshot marker file exists.
    assert!(
        fork_dir
            .join("snapshots")
            .join(snap_ulid.to_string())
            .exists()
    );

    // Writes after snapshot still go to the same fork.
    let new_data = vec![0xBBu8; 4096];
    vol.write(1, &new_data).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data);
    assert_eq!(vol.read(1, 1).unwrap(), new_data);

    fs::remove_dir_all(fork_dir).unwrap();
}

#[test]
fn snapshot_ulid_matches_last_segment_ulid() {
    let fork_dir = keyed_temp_dir();
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    vol.write(0, &vec![0xAAu8; 4096]).unwrap();
    let snap_ulid = vol.snapshot().unwrap().to_string();

    // Snapshot promotes segments from pending/ to index/ + cache/, so
    // the segment shows up as `index/<ulid>.idx` after the call.
    let idx_files: Vec<_> = fs::read_dir(fork_dir.join("index"))
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(idx_files.len(), 1);
    let idx_name = idx_files[0].file_name().into_string().unwrap();
    assert_eq!(idx_name, format!("{snap_ulid}.idx"));

    fs::remove_dir_all(fork_dir).unwrap();
}

#[test]
fn snapshot_empty_wal_no_segment_written() {
    let fork_dir = keyed_temp_dir();
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    // No writes — WAL is empty.
    vol.snapshot().unwrap();

    // pending/ should be empty (no segment written for empty WAL).
    let pending: Vec<_> = fs::read_dir(fork_dir.join("pending")).unwrap().collect();
    assert!(pending.is_empty());

    fs::remove_dir_all(fork_dir).unwrap();
}

#[test]
fn snapshot_idempotent_when_no_new_data() {
    let fork_dir = keyed_temp_dir();
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    vol.write(0, &vec![0xAAu8; 4096]).unwrap();

    let ulid1 = vol.snapshot().unwrap();
    // No new writes — second snapshot must return the same ULID.
    let ulid2 = vol.snapshot().unwrap();
    assert_eq!(ulid1, ulid2);

    // Still only one snapshot marker on disk (filter out the
    // `<ulid>.manifest` file that sits next to each marker).
    let marker_count = fs::read_dir(fork_dir.join("snapshots"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_str().is_some_and(|s| !s.contains('.')))
        .count();
    assert_eq!(marker_count, 1);

    fs::remove_dir_all(fork_dir).unwrap();
}

#[test]
fn snapshot_not_idempotent_after_new_write() {
    let fork_dir = keyed_temp_dir();
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    vol.write(0, &vec![0xAAu8; 4096]).unwrap();

    let ulid1 = vol.snapshot().unwrap();
    vol.write(1, &vec![0xBBu8; 4096]).unwrap();
    vol.promote_for_test().unwrap();

    let ulid2 = vol.snapshot().unwrap();
    assert_ne!(ulid1, ulid2);

    let marker_count = fs::read_dir(fork_dir.join("snapshots"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_str().is_some_and(|s| !s.contains('.')))
        .count();
    assert_eq!(marker_count, 2);

    fs::remove_dir_all(fork_dir).unwrap();
}

#[test]
fn snapshot_idempotent_after_auto_promoted_data_already_snapshotted() {
    // Data promoted via FLUSH_THRESHOLD (pending_entries empty at snapshot
    // time) but that segment was already covered by a prior snapshot.
    let fork_dir = keyed_temp_dir();
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    vol.write(0, &vec![0xAAu8; 4096]).unwrap();
    vol.promote_for_test().unwrap(); // lands in pending/ with wal_ulid_1
    let ulid1 = vol.snapshot().unwrap(); // snapshot covers pending/wal_ulid_1
    // pending_entries is now empty; pending/ has one file but it's <= ulid1.
    let ulid2 = vol.snapshot().unwrap();
    assert_eq!(ulid1, ulid2);

    fs::remove_dir_all(fork_dir).unwrap();
}

#[test]
fn snapshot_lock_held_after_snapshot() {
    let fork_dir = keyed_temp_dir();
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    vol.snapshot().unwrap();

    // Fork is still locked (still live); second open must fail.
    assert!(Volume::open(&fork_dir, &fork_dir).is_err());
    drop(vol); // now released

    // After drop, a fresh open succeeds.
    assert!(Volume::open(&fork_dir, &fork_dir).is_ok());

    fs::remove_dir_all(fork_dir).unwrap();
}

// --- fork_volume tests ---

#[test]
fn fork_volume_creates_fork_with_signed_provenance() {
    let by_id = temp_dir();
    let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let default_dir = by_id.join(root_ulid);
    let fork_dir = by_id.join(child_ulid);
    write_test_keypair(&default_dir);

    // snapshot default to give it a branch point.
    let mut vol = Volume::open(&default_dir, &by_id).unwrap();
    vol.write(0, &vec![0xAAu8; 4096]).unwrap();
    let snap_ulid = vol.snapshot().unwrap().to_string();
    drop(vol);

    // Create the fork.
    fork_volume(&fork_dir, &default_dir).unwrap();
    assert!(fork_dir.join("wal").is_dir());
    assert!(fork_dir.join("pending").is_dir());
    assert!(
        !fork_dir.join("volume.parent").exists(),
        "standalone volume.parent file must not exist; parent lives in provenance"
    );

    // Parent lineage must be present in the signed provenance file.
    let lineage = crate::signing::read_lineage_verifying_signature(
        &fork_dir,
        crate::signing::VOLUME_PUB_FILE,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )
    .unwrap();
    let parent = lineage.parent.expect("fork must record parent");
    assert_eq!(parent.volume_ulid, root_ulid);
    assert_eq!(parent.snapshot_ulid, snap_ulid);
    assert!(lineage.extent_index.is_empty());

    fs::remove_dir_all(by_id).unwrap();
}

#[test]
fn fork_volume_errors_if_source_has_no_snapshots() {
    let by_id = temp_dir();
    let root_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    // Create root_dir so canonicalize() succeeds, but leave it without
    // a snapshots/ directory so latest_snapshot returns "no snapshots".
    fs::create_dir_all(&root_dir).unwrap();
    let err = fork_volume(&child_dir, &root_dir).unwrap_err();
    assert!(err.to_string().contains("no snapshots"), "{err}");
}

#[test]
fn fork_volume_at_pins_explicit_snapshot_without_requiring_local_marker() {
    // Simulate forking from a readonly ancestor: the source dir exists
    // but has no snapshots/ directory (prefetch hasn't landed yet). The
    // explicit pin must still succeed and be recorded in provenance.
    //
    // The source still needs its `volume.pub` locally so the fork can
    // embed the parent pubkey in its signed provenance — volume.pub is
    // pulled by the prefetch pathway before any fork operation.
    let by_id = temp_dir();
    let source_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let source_dir = by_id.join(source_ulid);
    let child_dir = by_id.join(child_ulid);
    fs::create_dir_all(&source_dir).unwrap();
    write_test_keypair(&source_dir);

    let branch_ulid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    fork_volume_at(&child_dir, &source_dir, branch_ulid).unwrap();

    let lineage = crate::signing::read_lineage_verifying_signature(
        &child_dir,
        crate::signing::VOLUME_PUB_FILE,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )
    .unwrap();
    let parent = lineage.parent.expect("fork must record parent");
    assert_eq!(parent.volume_ulid, source_ulid);
    assert_eq!(parent.snapshot_ulid, branch_ulid.to_string());

    fs::remove_dir_all(by_id).unwrap();
}

#[test]
fn fork_volume_at_rejects_non_ulid_source_dir() {
    let tmp = temp_dir();
    let source_dir = tmp.join("not-a-ulid");
    let child_dir = tmp.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    fs::create_dir_all(&source_dir).unwrap();
    let branch_ulid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    let err = fork_volume_at(&child_dir, &source_dir, branch_ulid).unwrap_err();
    assert!(err.to_string().contains("ULID"), "{err}");
    fs::remove_dir_all(tmp).ok();
}

#[test]
fn fork_volume_uses_latest_snapshot_when_multiple_exist() {
    let by_id = temp_dir();
    let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let default_dir = by_id.join(root_ulid);
    let fork_dir = by_id.join(child_ulid);
    write_test_keypair(&default_dir);

    let data_snap1 = vec![0x11u8; 4096];
    let data_snap2 = vec![0x22u8; 4096];

    let mut vol = Volume::open(&default_dir, &by_id).unwrap();
    // First snapshot — should NOT be the branch point.
    vol.write(0, &data_snap1).unwrap();
    let snap1 = vol.snapshot().unwrap().to_string();
    // Second snapshot — should be the branch point.
    vol.write(1, &data_snap2).unwrap();
    let snap2 = vol.snapshot().unwrap().to_string();
    drop(vol);

    // snap2 must sort after snap1 (ULIDs are monotonically increasing).
    assert!(snap2 > snap1);

    fork_volume(&fork_dir, &default_dir).unwrap();
    let lineage = crate::signing::read_lineage_verifying_signature(
        &fork_dir,
        crate::signing::VOLUME_PUB_FILE,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )
    .unwrap();
    let parent = lineage.parent.expect("fork must record parent");
    assert_eq!(parent.volume_ulid, root_ulid);
    assert_eq!(
        parent.snapshot_ulid, snap2,
        "provenance parent should point to the latest snapshot"
    );

    // Fork branched from snap2 sees both pre-snap1 and pre-snap2 writes.
    let vol = Volume::open(&fork_dir, &by_id).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data_snap1);
    assert_eq!(vol.read(1, 1).unwrap(), data_snap2);

    fs::remove_dir_all(by_id).unwrap();
}

#[test]
fn fork_volume_from_child_fork_creates_three_level_chain() {
    let by_id = temp_dir();
    let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let mid_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let leaf_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
    let default_dir = by_id.join(root_ulid);
    let mid_dir = by_id.join(mid_ulid);
    let leaf_dir = by_id.join(leaf_ulid);
    write_test_keypair(&default_dir);

    let data_root = vec![0xAAu8; 4096];
    let data_mid = vec![0xBBu8; 4096];
    let data_leaf = vec![0xCCu8; 4096];

    // Root volume: write + snapshot.
    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &data_root).unwrap();
        vol.snapshot().unwrap();
    }

    // Mid volume: branch from default, write + snapshot.
    fork_volume(&mid_dir, &default_dir).unwrap();
    {
        let mut vol = Volume::open(&mid_dir, &by_id).unwrap();
        vol.write(1, &data_mid).unwrap();
        vol.snapshot().unwrap();
    }

    // Leaf volume: branch from mid.
    fork_volume(&leaf_dir, &mid_dir).unwrap();

    // origin chain: leaf → mid → default (read from signed provenance).
    let leaf_lineage = crate::signing::read_lineage_verifying_signature(
        &leaf_dir,
        crate::signing::VOLUME_PUB_FILE,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )
    .unwrap();
    let leaf_parent = leaf_lineage.parent.expect("leaf must record parent");
    assert_eq!(leaf_parent.volume_ulid, mid_ulid);
    let mid_lineage = crate::signing::read_lineage_verifying_signature(
        &mid_dir,
        crate::signing::VOLUME_PUB_FILE,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )
    .unwrap();
    let mid_parent = mid_lineage.parent.expect("mid must record parent");
    assert_eq!(mid_parent.volume_ulid, root_ulid);

    // Leaf sees data from all three levels.
    let vol = Volume::open(&leaf_dir, &by_id).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data_root);
    assert_eq!(vol.read(1, 1).unwrap(), data_mid);
    assert_eq!(vol.read(2, 1).unwrap(), vec![0u8; 4096]); // unwritten

    // Write to leaf does not affect mid or default.
    drop(vol);
    {
        let mut vol = Volume::open(&leaf_dir, &by_id).unwrap();
        vol.write(2, &data_leaf).unwrap();
    }
    let vol = Volume::open(&leaf_dir, &by_id).unwrap();
    assert_eq!(vol.read(2, 1).unwrap(), data_leaf);
    assert_eq!(vol.ancestor_count(), 2);

    fs::remove_dir_all(by_id).unwrap();
}

#[test]
fn fork_volume_errors_if_fork_exists() {
    let by_id = temp_dir();
    let root_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    write_test_keypair(&root_dir);
    let mut vol = Volume::open(&root_dir, &by_id).unwrap();
    vol.snapshot().unwrap();
    drop(vol);

    fork_volume(&child_dir, &root_dir).unwrap();
    let err = fork_volume(&child_dir, &root_dir).unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");

    fs::remove_dir_all(by_id).unwrap();
}

// --- multi-snapshot read tests ---

#[test]
fn two_snapshots_data_readable_after_reopen() {
    let fork_dir = keyed_temp_dir();
    let data_a = vec![0xAAu8; 4096];
    let data_b = vec![0xBBu8; 4096];

    {
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &data_a).unwrap();
        vol.snapshot().unwrap();
        vol.write(1, &data_b).unwrap();
        vol.promote_for_test().unwrap();
    }

    // Re-open the same fork: both writes visible.
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data_a);
    assert_eq!(vol.read(1, 1).unwrap(), data_b);

    fs::remove_dir_all(fork_dir).unwrap();
}

#[test]
fn fork_data_visible_across_ancestry() {
    let by_id = temp_dir();
    let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    write_test_keypair(&default_dir);
    let data_a = vec![0xAAu8; 4096];
    let data_b = vec![0xBBu8; 4096];

    // Write to default, snapshot, create fork, write to fork.
    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &data_a).unwrap();
        vol.promote_for_test().unwrap();
        vol.snapshot().unwrap();
    }

    fork_volume(&child_dir, &default_dir).unwrap();
    {
        let mut vol = Volume::open(&child_dir, &by_id).unwrap();
        vol.write(1, &data_b).unwrap();
        vol.promote_for_test().unwrap();
    }

    // Re-open child: sees both ancestor and own data.
    let vol = Volume::open(&child_dir, &by_id).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data_a);
    assert_eq!(vol.read(1, 1).unwrap(), data_b);
    assert_eq!(vol.ancestor_count(), 1);

    fs::remove_dir_all(by_id).unwrap();
}

// --- ULID cutoff tests ---

/// Segments written to an ancestor volume *after* the branch point must not
/// be visible to a child volume. This is the core correctness property of
/// the per-ancestor ULID cutoff stored in `origin`.
#[test]
fn ulid_cutoff_hides_post_branch_ancestor_writes() {
    let by_id = temp_dir();
    let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    write_test_keypair(&default_dir);

    let pre_branch = vec![0xAAu8; 4096];
    let post_branch = vec![0xBBu8; 4096];

    // Write pre-branch data to ancestor, snapshot, then branch.
    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &pre_branch).unwrap();
        vol.snapshot().unwrap();
    }
    fork_volume(&child_dir, &default_dir).unwrap();

    // Write post-branch data to the ancestor volume at LBA 1 (a new LBA).
    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(1, &post_branch).unwrap();
        vol.promote_for_test().unwrap();
    }

    // Child must see pre-branch data at LBA 0 and zeros at LBA 1.
    let vol = Volume::open(&child_dir, &by_id).unwrap();
    assert_eq!(
        vol.read(0, 1).unwrap(),
        pre_branch,
        "pre-branch data must be visible"
    );
    assert_eq!(
        vol.read(1, 1).unwrap(),
        vec![0u8; 4096],
        "post-branch ancestor write must be invisible"
    );

    fs::remove_dir_all(by_id).unwrap();
}

/// A post-branch write to an ancestor that *overwrites* a pre-branch LBA
/// must also be invisible — the child must still see the original value.
#[test]
fn ulid_cutoff_overwrite_stays_invisible() {
    let by_id = temp_dir();
    let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    write_test_keypair(&default_dir);

    let original = vec![0xAAu8; 4096];
    let overwrite = vec![0xBBu8; 4096];

    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &original).unwrap();
        vol.snapshot().unwrap();
    }
    fork_volume(&child_dir, &default_dir).unwrap();

    // Ancestor overwrites LBA 0 after the branch.
    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &overwrite).unwrap();
        vol.promote_for_test().unwrap();
    }

    // Child must still see the original pre-branch value.
    let vol = Volume::open(&child_dir, &by_id).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), original);

    fs::remove_dir_all(by_id).unwrap();
}

// --- ReadonlyVolume tests ---

#[test]
fn readonly_volume_unwritten_returns_zeros() {
    let vol_dir = temp_dir();
    let fork_dir = vol_dir.join("base");
    // Create the directory structure without a WAL (simulating a readonly base).
    fs::create_dir_all(fork_dir.join("pending")).unwrap();

    let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
    assert_eq!(rv.read(0, 1).unwrap(), vec![0u8; 4096]);

    fs::remove_dir_all(vol_dir).unwrap();
}

#[test]
fn readonly_volume_reads_committed_segment() {
    let vol_dir = temp_dir();
    let fork_dir = vol_dir.join("base");
    write_test_keypair(&fork_dir);

    let data = vec![0xCCu8; 4096];

    // Write data into the fork via Volume, then drop the lock.
    {
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();
    }
    // Remove wal/ so ReadonlyVolume::open doesn't see a live WAL.
    // (ReadonlyVolume intentionally skips WAL replay; this also tests the
    //  no-WAL path.)
    fs::remove_dir_all(fork_dir.join("wal")).unwrap();

    let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
    assert_eq!(rv.read(0, 1).unwrap(), data);

    fs::remove_dir_all(vol_dir).unwrap();
}

#[test]
fn readonly_volume_reads_ancestor_data() {
    let by_id = temp_dir();
    let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
    let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
    write_test_keypair(&default_dir);

    let ancestor_data = vec![0xDDu8; 4096];

    // Write data into default, snapshot, fork.
    {
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &ancestor_data).unwrap();
        vol.snapshot().unwrap();
    }
    fork_volume(&child_dir, &default_dir).unwrap();
    // ReadonlyVolume doesn't take a write lock, so this always works.

    let rv = ReadonlyVolume::open(&child_dir, &by_id).unwrap();
    assert_eq!(rv.read(0, 1).unwrap(), ancestor_data);

    fs::remove_dir_all(by_id).unwrap();
}

/// Regression test for the fork-from-remote demand-fetch bug: when a
/// forked child needs to demand-fetch a segment that belongs to an
/// ancestor, `find_segment_in_dirs` must route the fetcher at the
/// ancestor's `index/` and `cache/` directories — not the child's.
/// The child's `index/` does not hold ancestor `.idx` files; using the
/// child's dirs fails with ENOENT on the very first read.
#[test]
fn find_segment_in_dirs_routes_fetcher_at_ancestor_index_dir() {
    use crate::extentindex::BodySource;
    use std::sync::Mutex;

    struct OwnerAssertingFetcher {
        captured: Mutex<Option<(PathBuf, PathBuf)>>,
    }

    impl crate::segment::SegmentFetcher for OwnerAssertingFetcher {
        fn fetch_extent(
            &self,
            segment_id: Ulid,
            index_dir: &Path,
            body_dir: &Path,
            _extent: &crate::segment::ExtentFetch,
        ) -> io::Result<()> {
            *self.captured.lock().unwrap() = Some((index_dir.to_owned(), body_dir.to_owned()));
            // Simulate a successful fetch: write the body file where
            // the caller expects to find it on return.
            std::fs::create_dir_all(body_dir)?;
            std::fs::write(body_dir.join(format!("{segment_id}.body")), b"fake body")?;
            Ok(())
        }

        fn fetch_delta_body(
            &self,
            _segment_id: Ulid,
            _index_dir: &Path,
            _body_dir: &Path,
        ) -> io::Result<()> {
            Err(io::Error::other("unused"))
        }
    }

    let tmp = temp_dir();
    let child_dir = tmp.join("child");
    let ancestor_dir = tmp.join("ancestor");
    std::fs::create_dir_all(child_dir.join("index")).unwrap();
    std::fs::create_dir_all(ancestor_dir.join("index")).unwrap();

    // Only the ancestor holds the segment's `.idx`, matching the
    // fork-from-remote layout where each volume's signed index lives
    // in its own `index/` directory. Content is irrelevant — the
    // routing code only checks existence.
    let seg_ulid = Ulid::new();
    let idx_name = format!("{seg_ulid}.idx");
    std::fs::write(ancestor_dir.join("index").join(&idx_name), b"stub").unwrap();

    let layers = vec![AncestorLayer {
        dir: ancestor_dir.clone(),
        branch_ulid: None,
    }];
    let concrete = Arc::new(OwnerAssertingFetcher {
        captured: Mutex::new(None),
    });
    let fetcher: BoxFetcher = concrete.clone();

    let returned = find_segment_in_dirs(
        seg_ulid,
        &child_dir,
        &layers,
        Some(&fetcher),
        0,
        BodySource::Cached(0),
    )
    .expect("fetcher should have been routed at the ancestor's dirs");

    // The body must land under the ancestor, not the child.
    assert_eq!(
        returned,
        ancestor_dir.join("cache").join(format!("{seg_ulid}.body")),
    );
    assert!(
        !child_dir
            .join("cache")
            .join(format!("{seg_ulid}.body"))
            .exists(),
        "body must not be written under the child's cache dir"
    );

    // And the fetcher itself must have been called with the ancestor's
    // dirs — this is what the pre-fix code got wrong.
    let (idx_dir, body_dir) = concrete
        .captured
        .lock()
        .unwrap()
        .clone()
        .expect("fetcher must be called");
    assert_eq!(idx_dir, ancestor_dir.join("index"));
    assert_eq!(body_dir, ancestor_dir.join("cache"));

    fs::remove_dir_all(tmp).unwrap();
}

/// Complement to the previous test: when the child itself owns the
/// segment (its own `index/` holds the `.idx`), the fetcher must be
/// routed at the child's own dirs even if an ancestor is present.
#[test]
fn find_segment_in_dirs_prefers_self_over_ancestor_when_self_owns_idx() {
    use crate::extentindex::BodySource;
    use std::sync::Mutex;

    struct CaptureFetcher {
        captured: Mutex<Option<PathBuf>>,
    }
    impl crate::segment::SegmentFetcher for CaptureFetcher {
        fn fetch_extent(
            &self,
            segment_id: Ulid,
            index_dir: &Path,
            body_dir: &Path,
            _extent: &crate::segment::ExtentFetch,
        ) -> io::Result<()> {
            *self.captured.lock().unwrap() = Some(index_dir.to_owned());
            std::fs::create_dir_all(body_dir)?;
            std::fs::write(body_dir.join(format!("{segment_id}.body")), b"")?;
            Ok(())
        }
        fn fetch_delta_body(&self, _: Ulid, _: &Path, _: &Path) -> io::Result<()> {
            Err(io::Error::other("unused"))
        }
    }

    let tmp = temp_dir();
    let child_dir = tmp.join("child");
    let ancestor_dir = tmp.join("ancestor");
    std::fs::create_dir_all(child_dir.join("index")).unwrap();
    std::fs::create_dir_all(ancestor_dir.join("index")).unwrap();

    let seg_ulid = Ulid::new();
    let idx_name = format!("{seg_ulid}.idx");
    // Both self and ancestor have the `.idx`; self must win.
    std::fs::write(child_dir.join("index").join(&idx_name), b"stub").unwrap();
    std::fs::write(ancestor_dir.join("index").join(&idx_name), b"stub").unwrap();

    let layers = vec![AncestorLayer {
        dir: ancestor_dir.clone(),
        branch_ulid: None,
    }];
    let concrete = Arc::new(CaptureFetcher {
        captured: Mutex::new(None),
    });
    let fetcher: BoxFetcher = concrete.clone();

    let returned = find_segment_in_dirs(
        seg_ulid,
        &child_dir,
        &layers,
        Some(&fetcher),
        0,
        BodySource::Cached(0),
    )
    .unwrap();

    assert_eq!(
        returned,
        child_dir.join("cache").join(format!("{seg_ulid}.body")),
    );
    assert_eq!(
        concrete.captured.lock().unwrap().clone().unwrap(),
        child_dir.join("index")
    );

    fs::remove_dir_all(tmp).unwrap();
}

#[test]
fn readonly_volume_does_not_see_wal_records() {
    let vol_dir = temp_dir();
    let fork_dir = vol_dir.join("base");
    write_test_keypair(&fork_dir);

    let committed = vec![0xEEu8; 4096];
    let in_wal = vec![0xFFu8; 4096];

    // Write and promote LBA 0, then write LBA 1 to the WAL only.
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    vol.write(0, &committed).unwrap();
    vol.promote_for_test().unwrap();
    vol.write(1, &in_wal).unwrap();
    // Do NOT promote — LBA 1 is only in the WAL.
    // Drop the writable volume so the lock is released.
    drop(vol);

    // ReadonlyVolume skips WAL replay: LBA 1 must appear as zeros.
    let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
    assert_eq!(rv.read(0, 1).unwrap(), committed);
    assert_eq!(rv.read(1, 1).unwrap(), vec![0u8; 4096]);

    fs::remove_dir_all(vol_dir).unwrap();
}

// --- apply_gc_handoffs tests ---
//
// These tests simulate the coordinator GC workflow:
//   write → flush → drain (pending→cache + index) → coordinator compacts
//   into new segment + writes gc/*.pending → volume applies handoff.

#[test]
fn gc_handoff_applies_and_renames() {
    // End-to-end: stage a GC output, apply it (volume re-signs and
    // commits to bare), promote it (coordinator writes new idx and
    // deletes old idx via the inputs field), evict caches.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
    vol.write(0, &data).unwrap();
    vol.promote_for_test().unwrap();

    let pending_dir = base.join("pending");
    let old_ulid = fs::read_dir(&pending_dir)
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();
    simulate_upload(&mut vol);

    let new_ulid = simulate_coord_gc_staged(&mut vol, &base, &old_ulid);

    // Apply the handoff: volume re-signs `gc/<new>.staged` to `gc/<new>`.
    let count = vol.apply_gc_handoffs().unwrap();
    assert_eq!(count, 1);

    let gc_dir = base.join("gc");
    assert!(
        !gc_dir.join(format!("{new_ulid}.plan")).exists(),
        "plan file must be removed after commit"
    );
    assert!(
        gc_dir.join(&new_ulid).exists(),
        "bare gc/<new> must exist after commit"
    );

    // After apply_gc_handoffs the old idx is still present — promote_segment
    // is the step that deletes it (after the coordinator confirms upload).
    let cache_dir = base.join("cache");
    let index_dir = base.join("index");
    assert!(
        index_dir.join(format!("{old_ulid}.idx")).exists(),
        "old idx must persist until promote_segment runs"
    );
    assert!(
        !index_dir.join(format!("{new_ulid}.idx")).exists(),
        "new idx must not exist before promote_segment (not yet S3-confirmed)"
    );

    // Promote: coordinator confirms upload and asks the volume to write
    // index/<new>.idx + cache/<new>.body. promote_segment derives the
    // list of input ulids from the new segment's header and deletes
    // their idx files.
    let new_ulid_parsed = Ulid::from_string(&new_ulid).unwrap();
    vol.promote_segment(new_ulid_parsed).unwrap();

    assert!(
        index_dir.join(format!("{new_ulid}.idx")).exists(),
        "promote_segment must write index/<new>.idx"
    );
    assert!(
        !index_dir.join(format!("{old_ulid}.idx")).exists(),
        "promote_segment must delete index/<old>.idx for each input"
    );

    // Reads still work via cache/<new>.body.
    assert_eq!(vol.read(0, 2).unwrap(), data);

    // Coordinator finalize: deletes the bare gc/<new> file.
    vol.finalize_gc_handoff(new_ulid_parsed).unwrap();
    assert!(
        !gc_dir.join(&new_ulid).exists(),
        "finalize_gc_handoff must delete bare gc/<new>"
    );
    // Reads still work — cache/<new>.body covers it.
    assert_eq!(vol.read(0, 2).unwrap(), data);

    // Note: under the new protocol cache/<old>.* is dropped by
    // promote_segment's input cleanup path, not by a separate evict step.
    let _ = cache_dir;

    fs::remove_dir_all(base).unwrap();
}

/// Simulate a coordinator GC pass: read the old segment's entries and
/// write a `gc/<new>.plan` file holding one `keep` per entry.
///
/// Matches what the real coordinator emits for fully-alive inputs under
/// the plan handoff protocol (see `docs/design-gc-plan-handoff.md`).
fn simulate_coord_gc_staged(vol: &mut Volume, fork_dir: &Path, old_ulid: &str) -> String {
    use crate::gc_plan::{GcPlan, PlanOutput};
    use crate::segment;

    let idx_path = fork_dir.join("index").join(format!("{old_ulid}.idx"));
    let (_bss, entries, _) =
        segment::read_and_verify_segment_index(&idx_path, &vol.verifying_key).unwrap();

    let new_ulid = vol.gc_checkpoint_for_test().unwrap();
    let new_ulid_str = new_ulid.to_string();

    let gc_dir = fork_dir.join("gc");
    fs::create_dir_all(&gc_dir).unwrap();

    let old_ulid_parsed = Ulid::from_string(old_ulid).unwrap();
    let outputs: Vec<PlanOutput> = (0..entries.len() as u32)
        .map(|entry_idx| PlanOutput::Keep {
            input: old_ulid_parsed,
            entry_idx,
        })
        .collect();
    let plan = GcPlan { new_ulid, outputs };
    let plan_path = gc_dir.join(format!("{new_ulid_str}.plan"));
    plan.write_atomic(&plan_path).unwrap();

    new_ulid_str
}

#[test]
fn gc_staged_handoff_applies_and_commits_bare() {
    // Step 4a: derive-at-apply path. Coordinator writes gc/<ulid>.staged
    // with inputs in the segment header; volume walks `.staged`, re-signs,
    // commits by renaming tmp → bare, removes `.staged`.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
    vol.write(0, &data).unwrap();
    vol.promote_for_test().unwrap();

    let pending_dir = base.join("pending");
    let old_ulid = fs::read_dir(&pending_dir)
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();
    simulate_upload(&mut vol);

    let new_ulid = simulate_coord_gc_staged(&mut vol, &base, &old_ulid);

    let count = vol.apply_gc_handoffs().unwrap();
    assert_eq!(count, 1);

    let gc_dir = base.join("gc");
    assert!(
        !gc_dir.join(format!("{new_ulid}.plan")).exists(),
        "`.plan` must be removed after commit"
    );
    assert!(
        gc_dir.join(&new_ulid).exists(),
        "bare <ulid> must exist after commit"
    );

    // Reads go through the extent index → new segment.
    assert_eq!(vol.read(0, 2).unwrap(), data);

    // Re-running is a no-op: bare exists, nothing to apply.
    let again = vol.apply_gc_handoffs().unwrap();
    assert_eq!(again, 0);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn gc_staged_crash_recovery_bare_wins() {
    // Crash state: rename tmp→bare succeeded, but `.plan` removal
    // failed. On next apply: detect the bare file, drop `.plan`,
    // count the handoff as recovered.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data: Vec<u8> = (0..8192).map(|i| (i * 5 + 17) as u8).collect();
    vol.write(0, &data).unwrap();
    vol.promote_for_test().unwrap();

    let pending_dir = base.join("pending");
    let old_ulid = fs::read_dir(&pending_dir)
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();
    simulate_upload(&mut vol);

    // Stage + commit once to produce a bare file.
    let new_ulid = simulate_coord_gc_staged(&mut vol, &base, &old_ulid);
    vol.apply_gc_handoffs().unwrap();

    // Inject the crash state: re-create a `.plan` next to the bare file.
    let gc_dir = base.join("gc");
    let bare_path = gc_dir.join(&new_ulid);
    let plan_path = gc_dir.join(format!("{new_ulid}.plan"));
    fs::copy(&bare_path, &plan_path).unwrap();

    // Apply: bare wins, `.plan` is removed, count=1 (crash-recovered).
    let count = vol.apply_gc_handoffs().unwrap();
    assert_eq!(count, 1);
    assert!(bare_path.exists());
    assert!(!plan_path.exists());

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn gc_staged_sweeps_stale_tmp_files() {
    // Stray volume-owned `<ulid>.tmp` files from crashed apply writes
    // are swept at the start of the apply pass. Coordinator-owned
    // `<ulid>.plan.tmp` scratch is deliberately preserved — the
    // coord may still be writing to it, and deleting it here would
    // race its plan emission rename to ENOENT.
    let base = keyed_temp_dir();
    let vol = Volume::open(&base, &base).unwrap();
    let gc_dir = base.join("gc");
    fs::create_dir_all(&gc_dir).unwrap();

    let ulid = Ulid::new();
    let volume_tmp = gc_dir.join(format!("{ulid}.tmp"));
    let coord_tmp = gc_dir.join(format!("{ulid}.plan.tmp"));
    fs::write(&volume_tmp, b"garbage").unwrap();
    fs::write(&coord_tmp, b"coord in-flight").unwrap();

    let mut vol = vol;
    let count = vol.apply_gc_handoffs().unwrap();
    assert_eq!(count, 0);
    assert!(!volume_tmp.exists(), "<ulid>.tmp must be swept");
    assert!(
        coord_tmp.exists(),
        "<ulid>.plan.tmp must be preserved (coord may still be writing)"
    );

    fs::remove_dir_all(base).unwrap();
}

/// Build a `.plan` GC handoff that compacts two input segments,
/// emitting Keep outputs only for the entries from `seg_b_ulid` (the
/// live ones); entries from `seg_a_ulid` are intentionally omitted, so
/// they become "removed" hashes from the apply path's perspective.
/// Inputs list = [a, b] sorted.
fn simulate_coord_gc_staged_two_inputs(
    vol: &mut Volume,
    fork_dir: &Path,
    seg_a_ulid: &str,
    seg_b_ulid: &str,
) -> String {
    use crate::gc_plan::{GcPlan, PlanOutput};
    use crate::segment;

    let idx_b = fork_dir.join("index").join(format!("{seg_b_ulid}.idx"));
    let (_bss, entries_b, _) =
        segment::read_and_verify_segment_index(&idx_b, &vol.verifying_key).unwrap();

    let new_ulid = vol.gc_checkpoint_for_test().unwrap();
    let new_ulid_str = new_ulid.to_string();

    let gc_dir = fork_dir.join("gc");
    fs::create_dir_all(&gc_dir).unwrap();

    let seg_a_parsed = Ulid::from_string(seg_a_ulid).unwrap();
    let seg_b_parsed = Ulid::from_string(seg_b_ulid).unwrap();
    // seg_a is consumed but contributes no output (its entries become
    // "removed" during apply) — signal this with a Drop record.
    let mut outputs: Vec<PlanOutput> = vec![PlanOutput::Drop {
        input: seg_a_parsed,
    }];
    outputs.extend(
        (0..entries_b.len() as u32).map(|entry_idx| PlanOutput::Keep {
            input: seg_b_parsed,
            entry_idx,
        }),
    );
    let plan = GcPlan { new_ulid, outputs };
    plan.write_atomic(&gc_dir.join(format!("{new_ulid_str}.plan")))
        .unwrap();

    new_ulid_str
}

#[test]
fn gc_staged_crash_in_bare_phase_drops_removed_extents() {
    // Regression for a bug found by the TLA+ model (HandoffProtocol.tla):
    //
    // Sequence:
    //   1. Write D0 to lba 0, drain → seg_a with hash h0.
    //   2. Overwrite lba 0 with D1, drain → seg_b with hash h1.
    //      h0 is now LBA-dead; extent_index still has h0 → seg_a.
    //   3. Stage a GC output that carries h1 only. h0 is "removed".
    //   4. apply_gc_handoffs commits bare gc/<new>; in-memory
    //      extent_index now has h1 → new_ulid and h0 removed entirely.
    //   5. Crash + reopen. Rebuild reconstructs the extent_index from
    //      on-disk state — bare gc/<new> + index/<seg_a>.idx + index/<seg_b>.idx.
    //
    // Bug: rebuild uses insert_if_absent in pass order [bare, idx]. The
    // bare body inserts h1 → new_ulid (winning the later seg_b.idx).
    // But h0 is NOT in the bare body, so when the rebuild processes
    // index/<seg_a>.idx, it inserts h0 → seg_a — re-introducing the
    // entry the apply path explicitly removed.
    //
    // Fix: extentindex::rebuild reads the inputs field of every bare
    // gc/<ulid> file and skips the .idx files for those input segments.
    //
    // This test asserts the fixed behaviour: after restart, the
    // in-memory extent_index has no entry for h0.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let d0: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
    let h0 = blake3::hash(&d0);
    vol.write(0, &d0).unwrap();
    vol.promote_for_test().unwrap();
    let pending_dir = base.join("pending");
    let seg_a_ulid = fs::read_dir(&pending_dir)
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();
    simulate_upload(&mut vol);

    let d1: Vec<u8> = (0..8192).map(|i| (i * 11 + 17) as u8).collect();
    let h1 = blake3::hash(&d1);
    vol.write(0, &d1).unwrap();
    vol.promote_for_test().unwrap();
    let seg_b_ulid = fs::read_dir(&pending_dir)
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();
    simulate_upload(&mut vol);

    // Sanity: both hashes are in the extent_index, h0 → seg_a, h1 → seg_b.
    assert!(
        vol.extent_index.lookup(&h0).is_some(),
        "h0 should be in extent_index pre-GC"
    );
    assert!(
        vol.extent_index.lookup(&h1).is_some(),
        "h1 should be in extent_index pre-GC"
    );

    // Stage a GC output that carries h1 and "removes" h0 (by omitting it).
    let _new_ulid = simulate_coord_gc_staged_two_inputs(&mut vol, &base, &seg_a_ulid, &seg_b_ulid);

    // Apply: the in-memory extent_index now has h1 → new and h0 removed.
    let count = vol.apply_gc_handoffs().unwrap();
    assert_eq!(count, 1);
    assert!(
        vol.extent_index.lookup(&h0).is_none(),
        "h0 should be removed from extent_index after apply"
    );
    assert!(
        vol.extent_index.lookup(&h1).is_some(),
        "h1 should still be in extent_index after apply"
    );

    // Crash + reopen. Rebuild from disk.
    drop(vol);
    let vol = Volume::open(&base, &base).unwrap();

    // h1 must still be in the extent_index (carried by the bare GC body).
    assert!(
        vol.extent_index.lookup(&h1).is_some(),
        "h1 should be in extent_index after restart"
    );

    // h0 must NOT be in the extent_index. Before the fix, the rebuild
    // would re-introduce it via index/<seg_a>.idx because insert_if_absent
    // doesn't know that seg_a was consumed by the bare GC body.
    assert!(
        vol.extent_index.lookup(&h0).is_none(),
        "h0 must be gone after restart — was a Removed entry in the GC handoff. \
             A stale entry here means index/<seg_a>.idx was processed without consulting \
             the bare gc body's `inputs` field. See HandoffProtocol.tla counterexample."
    );

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn gc_handoff_idempotent_after_crash() {
    // Simulate a crash between coordinator writing `.staged` and the
    // volume processing it. After reopen, the extent index is rebuilt
    // from index/*.idx (old segment still has its .idx), so reads are
    // correct before the handoff is applied. apply_gc_handoffs then
    // commits the bare `gc/<new>` and updates the extent index.
    let base = keyed_temp_dir();

    let old_ulid;
    let new_ulid;
    let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();

    {
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        let pending_dir = base.join("pending");
        old_ulid = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();
        simulate_upload(&mut vol);

        new_ulid = simulate_coord_gc_staged(&mut vol, &base, &old_ulid);

        // "Crash" — drop the volume before apply_gc_handoffs runs.
    }

    let mut vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.read(0, 2).unwrap(), data);

    let count = vol.apply_gc_handoffs().unwrap();
    assert_eq!(count, 1);

    let gc_dir = base.join("gc");
    assert!(!gc_dir.join(format!("{new_ulid}.plan")).exists());
    assert!(gc_dir.join(&new_ulid).exists());

    let index_dir = base.join("index");
    assert!(
        index_dir.join(format!("{old_ulid}.idx")).exists(),
        "old idx must persist until promote_segment runs"
    );
    assert!(
        !index_dir.join(format!("{new_ulid}.idx")).exists(),
        "new idx must not exist before promote_segment"
    );

    let new_ulid_parsed = Ulid::from_string(&new_ulid).unwrap();
    vol.promote_segment(new_ulid_parsed).unwrap();

    assert!(
        index_dir.join(format!("{new_ulid}.idx")).exists(),
        "promote_segment must write index/<new>.idx"
    );
    assert!(
        !index_dir.join(format!("{old_ulid}.idx")).exists(),
        "promote_segment must delete index/<old>.idx for each input"
    );

    // Reads still correct: extent index points to new_ulid, body in cache/.
    assert_eq!(vol.read(0, 2).unwrap(), data);

    fs::remove_dir_all(base).unwrap();
}

// --- FileCache (CLOCK) tests ---

fn dummy_file() -> fs::File {
    fs::File::open("/dev/null").unwrap()
}

fn ulid(n: u128) -> Ulid {
    Ulid::from(n)
}

#[test]
fn file_cache_hit_and_miss() {
    let mut cache = FileCache::new(4);
    assert!(cache.get(ulid(1)).is_none());

    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    assert!(cache.get(ulid(1)).is_some());
    assert!(cache.get(ulid(2)).is_none());
}

#[test]
fn file_cache_returns_correct_layout() {
    let mut cache = FileCache::new(4);
    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(2), SegmentLayout::BodyOnly, dummy_file());

    let (layout, _) = cache.get(ulid(1)).unwrap();
    assert_eq!(layout, SegmentLayout::Full);

    let (layout, _) = cache.get(ulid(2)).unwrap();
    assert_eq!(layout, SegmentLayout::BodyOnly);
}

#[test]
fn file_cache_replace_in_place() {
    let mut cache = FileCache::new(4);
    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(1), SegmentLayout::BodyOnly, dummy_file());

    let (layout, _) = cache.get(ulid(1)).unwrap();
    assert_eq!(layout, SegmentLayout::BodyOnly);
}

#[test]
fn file_cache_fills_empty_slots_before_evicting() {
    let mut cache = FileCache::new(3);
    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(2), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(3), SegmentLayout::Full, dummy_file());

    // All three should be present — no eviction yet.
    assert!(cache.get(ulid(1)).is_some());
    assert!(cache.get(ulid(2)).is_some());
    assert!(cache.get(ulid(3)).is_some());
}

#[test]
fn file_cache_clock_evicts_unreferenced() {
    let mut cache = FileCache::new(3);
    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(2), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(3), SegmentLayout::Full, dummy_file());

    // Touch 2 and 3 so their referenced bits are set.
    cache.get(ulid(2));
    cache.get(ulid(3));

    // Insert a 4th — should evict ulid(1) (unreferenced after insert,
    // since insert sets referenced but the CLOCK sweep clears it).
    // Actually: all three were inserted with referenced=true. Then we
    // called get() on 2 and 3 (re-setting their bits). The hand starts
    // at 0. On sweep: slot 0 (ulid 1) has referenced=true from insert,
    // so it gets cleared and hand advances. Slot 1 (ulid 2) has
    // referenced=true from get, cleared, hand advances. Slot 2 (ulid 3)
    // has referenced=true from get, cleared, hand advances. Back to
    // slot 0 (ulid 1) — now unreferenced — evicted.
    cache.insert(ulid(4), SegmentLayout::Full, dummy_file());

    assert!(
        cache.get(ulid(1)).is_none(),
        "ulid(1) should have been evicted"
    );
    assert!(cache.get(ulid(4)).is_some());
}

#[test]
fn file_cache_recently_accessed_survives_eviction() {
    // With 3 slots, insert three entries. Access ulid(2) to refresh its
    // referenced bit, then insert a 4th. The CLOCK sweep clears all
    // referenced bits on the first pass, then evicts the entry at the
    // hand position (slot 0 = ulid(1)) on the second pass.
    // Crucially, get() on ulid(2) refreshes its bit *after* insert set it,
    // so when the sweep clears it on the first pass, ulid(2) gets cleared
    // like everyone else — but if we access it *between* two inserts, the
    // second sweep finds it referenced again.
    let mut cache = FileCache::new(3);
    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(2), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(3), SegmentLayout::Full, dummy_file());

    // First overflow: inserts ulid(4). The sweep clears all three
    // referenced bits (first pass), then evicts slot 0 (ulid(1)) on
    // the second pass. Hand ends at slot 1.
    cache.insert(ulid(4), SegmentLayout::Full, dummy_file());
    assert!(cache.get(ulid(1)).is_none(), "ulid(1) evicted");

    // Now touch ulid(2) — refreshes its referenced bit.
    cache.get(ulid(2));

    // Second overflow: inserts ulid(5). Hand is at slot 1.
    // Slot 1 (ulid(2)) ref=true → cleared, hand→2.
    // Slot 2 (ulid(3)) ref=false (cleared by first sweep, never re-accessed) → evicted.
    cache.insert(ulid(5), SegmentLayout::Full, dummy_file());
    assert!(cache.get(ulid(3)).is_none(), "ulid(3) evicted");
    assert!(
        cache.get(ulid(2)).is_some(),
        "ulid(2) survived — was accessed"
    );
    assert!(cache.get(ulid(4)).is_some());
    assert!(cache.get(ulid(5)).is_some());
}

#[test]
fn file_cache_evict_by_id() {
    let mut cache = FileCache::new(4);
    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(2), SegmentLayout::Full, dummy_file());

    cache.evict(ulid(1));
    assert!(cache.get(ulid(1)).is_none());
    assert!(cache.get(ulid(2)).is_some());
}

#[test]
fn file_cache_clear() {
    let mut cache = FileCache::new(4);
    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(2), SegmentLayout::Full, dummy_file());

    cache.clear();
    assert!(cache.get(ulid(1)).is_none());
    assert!(cache.get(ulid(2)).is_none());
}

#[test]
fn file_cache_evict_frees_slot_for_reuse() {
    let mut cache = FileCache::new(2);
    cache.insert(ulid(1), SegmentLayout::Full, dummy_file());
    cache.insert(ulid(2), SegmentLayout::Full, dummy_file());

    cache.evict(ulid(1));

    // The freed slot should be reused without evicting ulid(2).
    cache.insert(ulid(3), SegmentLayout::Full, dummy_file());
    assert!(cache.get(ulid(2)).is_some());
    assert!(cache.get(ulid(3)).is_some());
}

// --- inline extent tests ---

#[test]
fn inline_write_and_read_roundtrip() {
    // Small writes that compress below INLINE_THRESHOLD should be
    // readable immediately (from WAL) and after promotion (from inline_data).
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // All-same-byte 4KB data compresses to a few bytes → inline.
    let data = vec![0xAAu8; 4096];
    vol.write(0, &data).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data, "read before promotion");

    vol.promote_for_test().unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), data, "read after promotion");

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn inline_survives_reopen() {
    // After close+reopen, inline data is rebuilt from the segment's
    // inline section and reads still work.
    let base = keyed_temp_dir();
    {
        let mut vol = Volume::open(&base, &base).unwrap();
        let data = vec![0xBBu8; 4096];
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();
    }
    // Reopen: extent index is rebuilt from pending/ segments.
    let vol = Volume::open(&base, &base).unwrap();
    assert_eq!(vol.read(0, 1).unwrap(), vec![0xBBu8; 4096]);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn inline_coexists_with_body_entries() {
    // A segment with both inline and body entries: both are readable.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Small write → inline (compresses below threshold).
    let small = vec![0xCCu8; 4096];
    vol.write(0, &small).unwrap();

    // Large high-entropy write → body (doesn't compress below threshold).
    let large: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
    vol.write(1, &large).unwrap();

    vol.promote_for_test().unwrap();

    assert_eq!(vol.read(0, 1).unwrap(), small);
    assert_eq!(vol.read(1, 2).unwrap(), large);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn inline_dedup_as_canonical_source() {
    // An inline extent can serve as the canonical source for dedup.
    // Write the same small data at two different LBAs: first is DATA/Inline,
    // second should dedup (REF).
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data = vec![0xDDu8; 4096]; // compresses → inline
    vol.write(0, &data).unwrap();
    vol.write(1, &data).unwrap(); // dedup hit → REF

    vol.promote_for_test().unwrap();

    // Both LBAs should read correctly — the REF resolves via the
    // inline canonical entry.
    assert_eq!(vol.read(0, 1).unwrap(), data);
    assert_eq!(vol.read(1, 1).unwrap(), data);

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn inline_repack_preserves_data() {
    // GC repack of a segment containing inline entries must preserve
    // inline data through the rewrite.
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let d0 = vec![0xEEu8; 4096]; // inline
    let d1 = vec![0xFFu8; 4096]; // inline
    vol.write(0, &d0).unwrap();
    vol.write(1, &d1).unwrap();
    vol.promote_for_test().unwrap();

    // Overwrite LBA 0 to make d0 dead, creating GC opportunity.
    let d2 = vec![0x11u8; 4096];
    vol.write(0, &d2).unwrap();
    vol.promote_for_test().unwrap();

    // Repack: the segment with d0+d1 should be compacted; d1 survives.
    // Threshold 1.0 → compact any segment with dead extents.
    let stats = vol.repack(1.0).unwrap();
    assert!(stats.segments_compacted > 0);

    // Reads still correct after repack.
    assert_eq!(vol.read(0, 1).unwrap(), d2);
    assert_eq!(vol.read(1, 1).unwrap(), d1);

    fs::remove_dir_all(base).unwrap();
}

/// Simulates a crash window in `promote_segment`: the segment's cache body
/// and idx have been committed on disk, but the extent-index CAS + pending
/// delete have not run (in the offloaded design these live in a separate
/// actor-side apply phase). The next `promote_segment` call for the same
/// ULID must complete the half-done work — delete `pending/<ulid>` and
/// transition extent-index entries to `BodySource::Cached` — not silently
/// early-return.
///
/// Today (synchronous in-actor `promote_segment`) the window is narrow but
/// still observable because `extract_idx` and `promote_to_cache` commit
/// their files via atomic rename before the pending delete runs. Under the
/// planned worker offload the window widens, so this test is also a
/// regression guard for the offload landing.
#[test]
fn promote_segment_recovers_mid_apply_crash() {
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    let data = [42u8; 4096];
    vol.write(0, &data).unwrap();
    vol.promote_for_test().unwrap();

    let pending_dir = base.join("pending");
    let ulid_str = fs::read_dir(&pending_dir)
        .unwrap()
        .flatten()
        .find_map(|e| {
            let name = e.file_name().into_string().ok()?;
            (!name.contains('.')).then_some(name)
        })
        .unwrap();
    let ulid = Ulid::from_string(&ulid_str).unwrap();
    let pending_path = pending_dir.join(&ulid_str);

    // Perform only the "worker" half of promote_segment — extract_idx +
    // promote_to_cache. Skip the extent-index CAS + pending delete.
    let cache_dir = base.join("cache");
    fs::create_dir_all(&cache_dir).unwrap();
    let body_path = cache_dir.join(format!("{ulid_str}.body"));
    let present_path = cache_dir.join(format!("{ulid_str}.present"));
    let index_dir = base.join("index");
    fs::create_dir_all(&index_dir).unwrap();
    let idx_path = index_dir.join(format!("{ulid_str}.idx"));
    segment::extract_idx(&pending_path, &idx_path).unwrap();
    segment::promote_to_cache(&pending_path, &body_path, &present_path).unwrap();

    assert!(pending_path.exists(), "precondition: pending survives");
    assert!(body_path.exists(), "precondition: cache body committed");
    assert!(idx_path.exists(), "precondition: idx committed");

    // Simulate the process crash: drop and reopen.
    drop(vol);
    let mut vol = Volume::open(&base, &base).unwrap();

    // Coordinator retries promote_segment on its next tick.
    vol.promote_segment(ulid).unwrap();

    // Invariant 1: pending/<ulid> is gone after the retry.
    assert!(
        !pending_path.exists(),
        "pending/<ulid> survived retry — half-done promote not recovered",
    );

    // Invariant 2: the extent-index entry for the written hash now points
    // at Cached, not Local.
    let hash = blake3::hash(&data);
    let loc = vol
        .extent_index
        .lookup(&hash)
        .expect("hash still present in extent index");
    assert!(
        matches!(loc.body_source, BodySource::Cached(_)),
        "extent-index entry still BodySource::Local after retry: {:?}",
        loc.body_source
    );

    // Invariant 3: data still reads back correctly.
    let actual = vol.read(0, 1).unwrap();
    assert_eq!(actual.as_slice(), data.as_slice(), "data readback wrong");

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn all_inline_segment_readable() {
    // A segment where every entry is inline (body_length = 0).
    let base = keyed_temp_dir();
    let mut vol = Volume::open(&base, &base).unwrap();

    // Write several small extents — all compress to inline.
    for lba in 0..4u64 {
        let data = vec![lba as u8; 4096];
        vol.write(lba, &data).unwrap();
    }
    vol.promote_for_test().unwrap();

    // Verify all reads.
    for lba in 0..4u64 {
        let expected = vec![lba as u8; 4096];
        assert_eq!(vol.read(lba, 1).unwrap(), expected, "LBA {lba} mismatch");
    }

    // Verify the segment has body_length = 0.
    let pending_dir = base.join("pending");
    let seg_path = fs::read_dir(&pending_dir)
        .unwrap()
        .flatten()
        .next()
        .unwrap()
        .path();
    let (bss, _, _) =
        segment::read_and_verify_segment_index(&seg_path, &vol.verifying_key).unwrap();
    let file_len = fs::metadata(&seg_path).unwrap().len();
    assert_eq!(file_len, bss, "all-inline segment should have no body");

    fs::remove_dir_all(base).unwrap();
}
