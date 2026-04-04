// Deterministic regression tests for the coordinator GC path.
//
// Each test here corresponds to a specific bug found (often by the proptest
// suite in gc_proptest.rs) and exercises the minimal sequence that reproduces
// it.  Tests are named after the bug they cover so the history is traceable.
//
// See docs/testing.md for the convention: proptest suites live in *_proptest.rs;
// deterministic reproductions live in *_test.rs.

use std::fs;
use std::sync::Arc;

use elide_coordinator::config::GcConfig;
use elide_coordinator::gc::{apply_done_handoffs, gc_fork};
use elide_core::volume::Volume;
use object_store::ObjectStore;
use object_store::memory::InMemory;

fn make_gc_config() -> GcConfig {
    // density_threshold=0.0 ensures any dead segment is compacted.
    // small_segment_bytes=MAX ensures all segments qualify for sweep.
    GcConfig {
        density_threshold: 0.0,
        small_segment_bytes: u64::MAX,
        interval_secs: 0,
    }
}

fn drain_pending(fork_dir: &std::path::Path) {
    let pending_dir = fork_dir.join("pending");
    let segments_dir = fork_dir.join("segments");
    if let Ok(entries) = fs::read_dir(&pending_dir) {
        for entry in entries.flatten() {
            let _ = fs::rename(entry.path(), segments_dir.join(entry.file_name()));
        }
    }
}

/// Regression test for Bug B: a DEDUP_REF written between gc_checkpoint and
/// apply_gc_handoffs makes a hash live again in the volume's LBA map, but
/// gc_fork's disk-based liveness view did not see the write.  Without the fix,
/// apply_gc_handoffs would remove H from the extent index AND
/// apply_done_handoffs would delete the segment H lives in, causing reads of
/// the DEDUP_REF LBA to return zeros or a "segment not found" error.
///
/// The fix: in apply_gc_handoffs, scan for stale liveness (hash in
/// old_ulid_by_hash, not carried, but live in lbamap) BEFORE any extent index
/// mutations.  If found, cancel the GC output (delete .pending and body) so
/// gc_fork can re-run with correct liveness data on the next tick.
#[test]
fn gc_handoff_bug_b_dedup_ref_after_checkpoint() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    elide_core::signing::generate_keypair(
        fork_dir,
        elide_core::signing::VOLUME_KEY_FILE,
        elide_core::signing::VOLUME_PUB_FILE,
    )
    .unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    fs::create_dir_all(fork_dir.join("segments")).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let gc_config = make_gc_config();

    // Step 1: Write D0 to lba=0.  D0's hash (H0) goes into the extent index.
    let d0 = [42u8; 4096];
    vol.write(0, &d0).unwrap();
    vol.flush_wal().unwrap();

    // Step 2: Overwrite lba=0 with different data.  H0 is now LBA-dead.
    let d1 = [99u8; 4096];
    vol.write(0, &d1).unwrap();
    vol.flush_wal().unwrap();

    // Move both pending segments to segments/ so gc_fork can operate on them.
    drain_pending(fork_dir);

    // Step 3: gc_checkpoint — flush WAL, mint GC output ULIDs.
    // H0 is LBA-dead at this point: lba=0 now points to H1.  gc_fork will
    // therefore not carry H0 and will emit a Remove(H0, S1) handoff line.
    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();

    // Step 4: gc_fork — GC compaction; H0 appears dead from disk state.
    rt.block_on(gc_fork(
        fork_dir,
        "test-vol",
        &store,
        &gc_config,
        &repack_ulid,
        &sweep_ulid,
    ))
    .unwrap();

    // Step 5: BUG B INJECTION — write D0 again to lba=5.
    // H0 is still in the extent index (apply_gc_handoffs hasn't run yet), so
    // the volume writes a DEDUP_REF in the WAL.  The in-memory LBA map now has
    // lba=5 → H0 (live), but gc_fork's disk-based view missed this write.
    // The WAL is intentionally NOT flushed — the DEDUP_REF is in-memory only,
    // exactly replicating the production window between checkpoint and apply.
    vol.write(5, &d0).unwrap();

    // Step 6: apply_gc_handoffs — the fix detects that H0 is live in the LBA
    // map but not carried, cancels the GC output (deletes .pending and body),
    // and returns without writing .applied.  The old segments survive.
    vol.apply_gc_handoffs().unwrap();

    // Step 7: apply_done_handoffs — no .applied file exists (cancelled), so
    // nothing is deleted.
    rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store))
        .unwrap();

    // Assert: lba=5 reads back D0 correctly.
    // Without the fix: apply_gc_handoffs removed H0 from the extent index (or
    // apply_done_handoffs deleted its segment), causing zeros or "not found".
    let got5 = vol.read(5, 1).expect("read lba=5 after Bug B GC cancel");
    assert_eq!(
        got5.as_slice(),
        &d0,
        "lba=5 should return D0 after GC cancel"
    );

    // lba=0 should return D1 — unaffected by the cancellation.
    let got0 = vol.read(0, 1).expect("read lba=0 after Bug B GC cancel");
    assert_eq!(got0.as_slice(), &d1, "lba=0 should return D1 unchanged");

    // Step 8: Second GC sweep — gc_checkpoint flushes the WAL, making the
    // DEDUP_REF for H0 visible on disk.  gc_fork correctly sees H0 as live,
    // carries it into the output, and the handoff completes without cancelling.
    let (repack_ulid2, sweep_ulid2) = vol.gc_checkpoint().unwrap();
    drain_pending(fork_dir);

    rt.block_on(gc_fork(
        fork_dir,
        "test-vol",
        &store,
        &gc_config,
        &repack_ulid2,
        &sweep_ulid2,
    ))
    .unwrap();
    vol.apply_gc_handoffs().unwrap();
    rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store))
        .unwrap();

    // After the corrected sweep, both LBAs must still read correctly.
    let got5 = vol.read(5, 1).expect("read lba=5 after second GC sweep");
    assert_eq!(
        got5.as_slice(),
        &d0,
        "lba=5 should return D0 after second sweep"
    );

    let got0 = vol.read(0, 1).expect("read lba=0 after second GC sweep");
    assert_eq!(
        got0.as_slice(),
        &d1,
        "lba=0 should return D1 after second sweep"
    );
}
