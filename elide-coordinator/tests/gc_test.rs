// Deterministic regression tests for the coordinator GC path.
//
// Each test here corresponds to a specific bug found (often by the proptest
// suite in gc_proptest.rs) and exercises the minimal sequence that reproduces
// it.  Tests are named after the bug they cover so the history is traceable.
//
// See docs/testing.md for the convention: proptest suites live in *_proptest.rs;
// deterministic reproductions live in *_test.rs.
//
// Bug inventory:
//
//   Bug A — DEDUP_REF-only consumed segments never deleted after GC.
//            Fixed by emitting a Dead handoff line for every consumed segment
//            not covered by a Repack or Remove line.
//            Covered by: gc_proptest::gc_segment_cleanup (proptest)
//
//   Bug B — DEDUP_REF written after gc_checkpoint makes a hash live again;
//            apply_gc_handoffs incorrectly removes it from the extent index
//            and apply_done_handoffs deletes the segment it lives in.
//            Fixed by stale-liveness detection before any extent index mutations.
//            Covered by: gc_handoff_bug_b_dedup_ref_after_checkpoint (below)
//
//   Bug C — gc_checkpoint mints GC output ULIDs before opening a new WAL.
//            When the WAL is empty at checkpoint time flush_wal is a no-op,
//            so the active WAL ULID stays below the minted GC output ULIDs.
//            After the GC output lands in segments/, a subsequent WAL flush
//            creates a segment with a lower ULID; on crash-recovery rebuild
//            the GC output sorts after and wins, returning stale data.
//            Fixed by always opening a new WAL after minting in gc_checkpoint.
//            Covered by: gc_checkpoint_ulid_ordering_crash_recovery (below)
//
//   Bug D — gc_checkpoint flushes a non-empty WAL to pending/ under the WAL's
//            existing ULID before minting the GC output ULIDs.  The WAL's ULID
//            was assigned when the WAL was opened (before the mint), so the
//            resulting pending segment has ULID < GC output ULIDs.  When that
//            segment is drained to segments/ and crash-recovery rebuild runs,
//            the GC output (higher ULID) wins for the affected LBAs, returning
//            stale data.
//            Fixed by minting all three ULIDs (repack, sweep, pending-segment)
//            before flushing, so the flushed WAL segment gets a ULID > sweep.
//            Covered by: gc_checkpoint_nonempty_wal_ulid_ordering_crash_recovery (below)
//
//   Bug E — GC restart-safety gap: apply_gc_handoffs only processed .pending
//            files, not .applied files.  After a coordinator/volume restart the
//            volume rebuilds its extent index from .idx files (pointing to old
//            segments).  If apply_done_handoffs then deletes the old segment
//            before apply_gc_handoffs has re-applied the .applied handoff, the
//            extent index becomes stale and all reads for the affected hashes
//            fail with "segment not found".
//            Fixed by:
//              1. apply_gc_handoffs now also processes .applied files (re-applies
//                 extent index updates idempotently, skips re-signing/rename).
//              2. The coordinator daemon calls apply_gc_handoffs (IPC) immediately
//                 before apply_done_handoffs on every GC tick, guaranteeing the
//                 volume's extent index is consistent before old segments are deleted.
//            Covered by: gc_restart_safety_applied_handoff (below)
//
//   Note: a structurally similar scenario exists — segments already in pending/
//         *before* gc_checkpoint is called also have ULIDs below the GC output
//         and would cause the same crash-recovery ordering problem if drained
//         after GC.  This is not a code bug: the coordinator tick always drains
//         pending/ to segments/ (Upload, step 4) before running GC (step 5), so
//         by the time gc_fork compacts segments/, every previously-pending
//         segment is already an input.  The proptest enforces this invariant by
//         draining pending/ inside GcSweep (before gc_checkpoint) rather than
//         as a separate op.

use std::fs;
use std::sync::Arc;

use elide_coordinator::config::GcConfig;
use elide_coordinator::gc::{apply_done_handoffs, gc_fork};
use elide_core::volume::Volume;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};

/// A store that always fails `put` operations.  Used to simulate S3 upload
/// failures in tests so we can verify that `drain_pending` correctly reports
/// failures and the coordinator tick skips GC when drain does not complete.
struct FailStore;

impl std::fmt::Display for FailStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailStore")
    }
}

impl std::fmt::Debug for FailStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailStore {
    async fn put_opts(
        &self,
        _location: &object_store::path::Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::Generic {
            store: "FailStore",
            source: "simulated upload failure".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &object_store::path::Path,
        _opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unimplemented!("FailStore::put_multipart_opts")
    }

    async fn get_opts(
        &self,
        _location: &object_store::path::Path,
        _options: GetOptions,
    ) -> object_store::Result<GetResult> {
        unimplemented!("FailStore::get_opts")
    }

    async fn delete(&self, _location: &object_store::path::Path) -> object_store::Result<()> {
        unimplemented!("FailStore::delete")
    }

    fn list(
        &self,
        _prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        unimplemented!("FailStore::list")
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<ListResult> {
        unimplemented!("FailStore::list_with_delimiter")
    }

    async fn copy(
        &self,
        _from: &object_store::path::Path,
        _to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        unimplemented!("FailStore::copy")
    }

    async fn copy_if_not_exists(
        &self,
        _from: &object_store::path::Path,
        _to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        unimplemented!("FailStore::copy_if_not_exists")
    }
}

fn make_gc_config() -> GcConfig {
    // density_threshold=0.0 ensures any dead segment is compacted.
    // small_segment_bytes=MAX ensures all segments qualify for sweep.
    GcConfig {
        density_threshold: 0.0,
        small_segment_bytes: u64::MAX,
        interval_secs: 0,
    }
}

/// Simulate coordinator drain: promote all files from pending/ to index/ + cache/,
/// mirroring what upload::drain_pending does in production after a successful S3 upload.
fn drain_pending(fork_dir: &std::path::Path) {
    const HEADER_LEN: usize = 96;
    let pending_dir = fork_dir.join("pending");
    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    fs::create_dir_all(&index_dir).unwrap();
    fs::create_dir_all(&cache_dir).unwrap();
    let Ok(entries) = fs::read_dir(&pending_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let Some(ulid_str) = name.to_str() else {
            continue;
        };
        if ulid_str.ends_with(".tmp") {
            continue;
        }
        let data = fs::read(&path).unwrap();
        assert!(data.len() >= HEADER_LEN, "segment too short: {ulid_str}");
        let entry_count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let index_length = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        let inline_length = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
        let bss = HEADER_LEN + index_length as usize + inline_length as usize;
        fs::write(index_dir.join(format!("{ulid_str}.idx")), &data[..bss]).unwrap();
        fs::write(cache_dir.join(format!("{ulid_str}.body")), &data[bss..]).unwrap();
        let bitset_len = (entry_count as usize).div_ceil(8);
        fs::write(
            cache_dir.join(format!("{ulid_str}.present")),
            vec![0xFFu8; bitset_len],
        )
        .unwrap();
        fs::remove_file(&path).unwrap();
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

    // Promote both pending segments to index/ + cache/ so gc_fork can operate on them.
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
        repack_ulid,
        sweep_ulid,
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
        repack_ulid2,
        sweep_ulid2,
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

/// Regression test for Bug C: gc_checkpoint mints GC output ULIDs before
/// opening a new WAL, so when the WAL is empty at checkpoint time the active
/// WAL ULID stays below the minted values.
///
/// Sequence that exposes the bug:
///
///   1. Write D0 to lba=0, flush → segment W1 in pending/.
///   2. Overwrite lba=0 with D1, flush → segment W2 in pending/. H0 is now dead.
///   3. Drain pending/ → segments/.  WAL W3 is empty.
///   4. GcSweep: gc_checkpoint with empty WAL → flush_wal is a no-op, WAL stays
///      W3.  Minted ULIDs u_repack, u_sweep are both > W3.  gc_fork compacts W1
///      and W2 into gc/u_sweep (H1 live, H0 dead).  apply_gc_handoffs updates
///      extent index; apply_done_handoffs moves gc/u_sweep → segments/u_sweep
///      and deletes W1, W2.
///      segments/ = { u_sweep }.  WAL ULID = W3 < u_sweep.  Bug C created.
///   5. Write D2 to lba=0, flush → segment W3 in pending/.  Drain → segments/.
///      segments/ = { u_sweep, W3 }.  u_sweep > W3: the GC output sorts AFTER
///      the segment containing the newer write.
///   6. Crash (drop Volume, reopen from disk).
///      Rebuild applies segments in ULID order: W3 first, u_sweep second.
///      u_sweep wins for lba=0, returning D1 (stale) instead of D2 (correct).
///
/// Without the fix, the post-crash read returns D1.
/// With the fix (gc_checkpoint always opens a new WAL after minting), the new
/// WAL has ULID > u_sweep, so the W3 segment with D2 always sorts above the GC
/// output and wins on rebuild.
///
/// Note: Bug B's stale-liveness detection does NOT protect against this.
/// Bug B prevents in-memory corruption; crash recovery bypasses in-memory state
/// entirely and rebuilds from disk, where the ULID ordering is wrong.
#[test]
fn gc_checkpoint_ulid_ordering_crash_recovery() {
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let gc_config = make_gc_config();

    // Step 1-2: write D0 then D1 to lba=0 so H0 is dead and H1 is live.
    // Two separate flushes so gc_fork has two distinct input segments to compact.
    let d0 = [11u8; 4096];
    let d1 = [22u8; 4096];
    let d2 = [33u8; 4096];

    vol.write(0, &d0).unwrap();
    vol.flush_wal().unwrap();
    vol.write(0, &d1).unwrap();
    vol.flush_wal().unwrap();

    // Step 3: drain pending/ → segments/.  The WAL is now empty.
    drain_pending(fork_dir);

    // Step 4: GcSweep with an empty WAL.
    //
    // BUG C is created here: gc_checkpoint calls flush_wal(), which is a no-op
    // because pending_entries is empty.  The active WAL ULID is therefore NOT
    // advanced before minting the GC output ULIDs, so:
    //   WAL ULID < u_repack < u_sweep
    // After apply_done_handoffs moves the GC output into segments/, any segment
    // produced by flushing the current WAL will have a ULID below u_sweep.
    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();

    rt.block_on(gc_fork(
        fork_dir,
        "test-vol",
        &store,
        &gc_config,
        repack_ulid,
        sweep_ulid,
    ))
    .unwrap();
    vol.apply_gc_handoffs().unwrap();
    rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store))
        .unwrap();

    // Step 5: write D2 to lba=0 and flush.  This goes to the current WAL whose
    // ULID is below u_sweep.  After draining, segments/ contains both the GC
    // output (u_sweep, carrying D1) and the new segment (W3 < u_sweep, carrying D2).
    vol.write(0, &d2).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);

    // Step 6: crash — drop the Volume and reopen from disk.
    //
    // This is the critical step.  Rebuild processes segments in ULID order.
    // With Bug C: GC output u_sweep > W3, so u_sweep is applied last and wins,
    // returning D1 (stale) for lba=0.
    // With the fix: the WAL opened after gc_checkpoint has ULID > u_sweep, so
    // W3 sorts above the GC output and D2 (correct) is returned.
    drop(vol);
    let vol = Volume::open(fork_dir, fork_dir).unwrap();

    let got = vol.read(0, 1).expect("read lba=0 after crash");
    assert_eq!(
        got.as_slice(),
        &d2,
        "lba=0 should return D2 (latest write) after crash, not D1 (stale GC output)"
    );
}

/// Regression test for Bug D: gc_checkpoint flushes a non-empty WAL to
/// pending/ under the WAL's existing ULID, which was assigned before the GC
/// output ULIDs were minted.  The resulting pending segment has ULID < GC
/// output ULIDs.  After the segment is drained to segments/ and the volume
/// crashes, rebuild processes segments in ULID order: the GC output (higher
/// ULID) applies last and wins for the affected LBAs, returning stale data.
///
/// Sequence:
///
///   1. Write D0 to lba=0, flush → W1 in pending/.
///   2. Write D1 to lba=0, flush → W2 in pending/.  H0 is now dead.
///   3. Drain pending/ → segments/ = {W1, W2}.
///   4. Write D2 to lba=0.  WAL is non-empty — do NOT flush.
///   5. gc_checkpoint: BUG D — flushes WAL under its existing ULID (assigned
///      before minting), so the pending segment has old_wal_ulid < u_sweep.
///      gc_fork compacts W1+W2 → output u_sweep (H1 live, H0 dead).
///      apply_gc_handoffs + apply_done_handoffs.
///   6. Drain pending/ → segments/ = {u_sweep, old_wal_ulid}.
///      old_wal_ulid < u_sweep: GC output sorts last and wins for lba=0.
///   7. Crash — drop Volume and reopen from disk.
///
/// Without the fix: rebuild applies old_wal_ulid first (lba=0→D2), then
/// u_sweep (lba=0→D1), so u_sweep wins → D1 (stale).
/// With the fix (pre-mint u_wal): the WAL is flushed under u_wal > u_sweep,
/// so it sorts after the GC output and wins → D2 (correct).
#[test]
fn gc_checkpoint_nonempty_wal_ulid_ordering_crash_recovery() {
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let gc_config = make_gc_config();

    let d0 = [11u8; 4096];
    let d1 = [22u8; 4096];
    let d2 = [33u8; 4096];

    // Steps 1-2: write D0 then D1 to lba=0 so H0 is dead and H1 is live.
    vol.write(0, &d0).unwrap();
    vol.flush_wal().unwrap();
    vol.write(0, &d1).unwrap();
    vol.flush_wal().unwrap();

    // Step 3: drain pending/ → segments/.
    drain_pending(fork_dir);

    // Step 4: write D2 to lba=0.  WAL is now non-empty.
    // Intentionally NOT flushed — gc_checkpoint must flush it.
    vol.write(0, &d2).unwrap();

    // Step 5: gc_checkpoint with a non-empty WAL.
    //
    // BUG D is created here without the fix: gc_checkpoint calls
    // flush_wal_to_pending(), which writes the WAL segment under its existing
    // ULID (assigned when the WAL was opened, before the GC output ULIDs are
    // minted).  After minting u_repack < u_sweep, the flushed segment has
    // old_wal_ulid < u_sweep.  Any subsequent drain puts both in segments/
    // with inverted ordering; on crash-recovery rebuild u_sweep wins → D1.
    //
    // With the fix: gc_checkpoint pre-mints u_repack < u_sweep < u_wal, then
    // flushes the WAL under u_wal.  After drain and crash, u_wal > u_sweep so
    // the WAL segment wins → D2.
    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();

    rt.block_on(gc_fork(
        fork_dir,
        "test-vol",
        &store,
        &gc_config,
        repack_ulid,
        sweep_ulid,
    ))
    .unwrap();
    vol.apply_gc_handoffs().unwrap();
    rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store))
        .unwrap();

    // Step 6: drain pending/ — the WAL segment flushed by gc_checkpoint lands
    // in segments/ alongside the GC output.
    drain_pending(fork_dir);

    // Step 7: crash — drop Volume and reopen from disk.
    drop(vol);
    let vol = Volume::open(fork_dir, fork_dir).unwrap();

    let got = vol.read(0, 1).expect("read lba=0 after crash");
    assert_eq!(
        got.as_slice(),
        &d2,
        "lba=0 should return D2 (latest write) after crash, not D1 (stale GC output)"
    );
}

/// Coverage for the drain-failure → skip-GC invariant enforced in
/// `daemon::fork_loop`.
///
/// When `drain_pending` fails (e.g. S3 is unreachable), the daemon sets
/// `drain_ok = false` and skips the GC step for that tick.  This test
/// verifies two things:
///
///   1. `drain_pending` correctly reports upload failures via
///      `DrainResult.failed > 0` when the store rejects `put` operations.
///
///   2. After a failed drain, pending segments are still present in
///      `pending/` — they have not been lost.  The next tick can retry
///      drain and then run GC with the segments safely in `segments/`.
///
/// Recovery path: after drain succeeds on the second attempt, GC runs
/// correctly and all data is readable.
#[test]
fn drain_failure_skips_gc_and_data_survives() {
    use elide_coordinator::upload;

    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();

    elide_core::signing::generate_keypair(
        fork_dir,
        elide_core::signing::VOLUME_KEY_FILE,
        elide_core::signing::VOLUME_PUB_FILE,
    )
    .unwrap();
    // volume.toml (with size) is required by upload_manifest inside drain_pending.
    elide_core::config::VolumeConfig {
        name: Some("test-vol".into()),
        size: Some(1073741824),
        ..Default::default()
    }
    .write(fork_dir)
    .unwrap();

    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let gc_config = make_gc_config();

    let d0 = [11u8; 4096];
    let d1 = [22u8; 4096];

    // Write two segments to segments/ so GC has something to compact.
    vol.write(0, &d0).unwrap();
    vol.flush_wal().unwrap();
    vol.write(0, &d1).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);

    // Write more data — this ends up in pending/ after flushing.
    vol.write(1, &d0).unwrap();
    vol.flush_wal().unwrap();

    // --- Tick N: drain fails ---
    //
    // The FailStore rejects all put operations. drain_pending returns
    // Ok(DrainResult { failed: 1 }), so the daemon sets drain_ok = false
    // and skips GC for this tick.
    let fail_store: Arc<dyn ObjectStore> = Arc::new(FailStore);
    let drain_result = rt
        .block_on(upload::drain_pending(fork_dir, "test-vol", &fail_store))
        .expect("drain_pending itself should not error");
    assert!(
        drain_result.failed > 0,
        "expected drain to report upload failure (failed={})",
        drain_result.failed
    );
    // drain_ok would be false in the daemon — GC is skipped.
    let drain_ok = drain_result.failed == 0;
    assert!(
        !drain_ok,
        "drain_ok must be false when uploads fail so GC is skipped"
    );

    // Pending segment must still be present (not lost during failed drain).
    let pending_count = fs::read_dir(fork_dir.join("pending"))
        .map(|d| d.flatten().count())
        .unwrap_or(0);
    assert!(
        pending_count > 0,
        "pending segment should survive a failed drain (count={})",
        pending_count
    );

    // --- Tick N+1: drain succeeds, GC runs ---
    //
    // On the next tick, drain succeeds with a working store.  GC then runs
    // with pending/ empty and produces correct output.
    let good_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let drain_result2 = rt
        .block_on(upload::drain_pending(fork_dir, "test-vol", &good_store))
        .expect("drain should succeed with good store");
    assert_eq!(
        drain_result2.failed, 0,
        "drain should report no failures with good store"
    );

    // GC runs after successful drain: pending/ is now empty, all prior
    // segments are in segments/.
    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();
    rt.block_on(gc_fork(
        fork_dir,
        "test-vol",
        &good_store,
        &gc_config,
        repack_ulid,
        sweep_ulid,
    ))
    .unwrap();
    vol.apply_gc_handoffs().unwrap();
    rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &good_store))
        .unwrap();

    // All data must be readable after recovery.
    let got0 = vol.read(0, 1).expect("read lba=0 after recovery");
    assert_eq!(got0.as_slice(), &d1, "lba=0 should return D1 after GC");
    let got1 = vol.read(1, 1).expect("read lba=1 after recovery");
    assert_eq!(got1.as_slice(), &d0, "lba=1 should return D0 after GC");
}

/// Regression test for Bug E: GC restart-safety gap.
///
/// `apply_gc_handoffs` previously only processed `.pending` files.  After a
/// coordinator/volume restart the volume rebuilds its extent index from on-disk
/// `.idx` files, which still point to the old segment (the old `.idx` is present
/// until `apply_done_handoffs` deletes it).  When `apply_done_handoffs` then
/// deletes the old segment, reads fail with "segment not found" because the
/// extent index was never updated to point to the new GC output.
///
/// Sequence that exposes the bug:
///
///   1. Write D0 to lba=0 and D1 to lba=1, flush, drain → old segment S1.
///   2. Overwrite lba=0 with D2, flush, drain → old segment S2.
///   3. gc_checkpoint + gc_fork → gc/<new>.pending (S1 and S2 compacted).
///   4. vol.apply_gc_handoffs() → gc/<new>.applied; extent index updated to
///      point to new segment in this Volume instance.
///   5. [RESTART] — drop Volume and reopen.  Extent index rebuilt from
///      index/*.idx (old segments S1, S2 still have .idx files).
///      apply_gc_handoffs() returns 0 (only saw .pending, not .applied).
///   6. apply_done_handoffs() — deletes S1, S2 (bodies and .idx files),
///      moves gc/<new> → segments/<new>.
///   7. Without fix: reads fail — extent index → S1 → deleted → "not found".
///      With fix: apply_gc_handoffs re-applies the .applied handoff → extent
///      index → new segment → reads succeed.
#[test]
fn gc_restart_safety_applied_handoff() {
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let gc_config = make_gc_config();

    let d0 = [11u8; 4096];
    let d1 = [22u8; 4096];
    let d2 = [33u8; 4096];

    // Step 1: write D0 to lba=0 and D1 to lba=1, flush, drain.
    vol.write(0, &d0).unwrap();
    vol.write(1, &d1).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);

    // Step 2: overwrite lba=0 with D2, flush, drain.  D0's hash is now dead.
    vol.write(0, &d2).unwrap();
    vol.flush_wal().unwrap();
    drain_pending(fork_dir);

    // Step 3: GC pass — gc_checkpoint mints ULIDs, gc_fork compacts the two
    // input segments into one GC output in gc/<new>.pending.
    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();
    rt.block_on(gc_fork(
        fork_dir,
        "test-vol",
        &store,
        &gc_config,
        repack_ulid,
        sweep_ulid,
    ))
    .unwrap();

    // Step 4: apply_gc_handoffs — re-signs gc/<new>, updates extent index in
    // THIS Volume instance to point to the new segment, renames .pending →
    // .applied.  The old segments still exist on disk at this point.
    let applied = vol.apply_gc_handoffs().unwrap();
    assert_eq!(applied, 1, "one handoff should be applied");

    // Step 5: simulate coordinator/volume restart — drop the volume and reopen.
    // Volume::open rebuilds the extent index from index/*.idx files; the old
    // segments still have their .idx files so the extent index points to the
    // OLD segment ULIDs.  The .applied handoff file exists but apply_gc_handoffs
    // previously returned 0 (only processed .pending files).
    drop(vol);
    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();

    // Step 6 (the fix): call apply_gc_handoffs on the reopened volume.  With
    // the fix it detects the .applied file and re-applies the extent index
    // update (idempotently, without re-signing or renaming).
    let re_applied = vol.apply_gc_handoffs().unwrap();
    assert_eq!(
        re_applied, 1,
        "apply_gc_handoffs must re-apply the .applied handoff after restart \
         (Bug E: previously returned 0 here)"
    );

    // Step 7: apply_done_handoffs — deletes old segment bodies and .idx files,
    // moves gc/<new> → segments/<new>.  Safe because the extent index now
    // points to the new segment.
    rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store))
        .unwrap();

    // All LBAs must read their last-written values.
    let got0 = vol
        .read(0, 1)
        .expect("read lba=0 after restart + GC cleanup");
    assert_eq!(
        got0.as_slice(),
        &d2,
        "lba=0 should return D2 after restart (Bug E: previously returned segment not found)"
    );
    let got1 = vol
        .read(1, 1)
        .expect("read lba=1 after restart + GC cleanup");
    assert_eq!(got1.as_slice(), &d1, "lba=1 should return D1 after restart");
}
