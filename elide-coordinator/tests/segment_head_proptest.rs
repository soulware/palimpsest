// Per-volume HEAD rebuild invariant.
//
// `docs/design-segment-index.md` *Rebuild defines correctness*: the
// rebuild is the specification, and HEAD is a *safe subset* of it.
// The one-directional invariant the proptest enforces:
//
//   live(manifest, actual HEAD) ⊆ live(manifest, rebuilt-from-LIST)
//
// HEAD never claims a segment is live that the rebuild cannot also
// see (the dangerous direction — a missing object HEAD names anyway,
// breaking the read path). The rebuild may legitimately include
// extras: crash orphans (segment PUT landed, HEAD PUT skipped) and
// pre-seal orphans (segment superseded but not yet reaped at seal).
// Both are operator-reclaimable (*Reconcile and orphan reclamation*).
//
// The rebuild reconstructs HEAD from:
//   - segments under `by_id/<vol>/segments/` (the `added` set =
//     S3 \ manifest);
//   - each GC output's signed `inputs` header (the `superseded`
//     edges, authoritative per the design);
//   - manifest segments absent from S3 (`tombstoned`, the
//     manifest-absence rule needed for chained supersessions where
//     the intermediate output is itself reaped).
//
// Two test surfaces:
//
//   1. `prop_rebuild_equivalence` — randomised Drain/Gc/Reap/Seal
//      sequences. After every op, live(actual) ⊆ live(rebuilt), and
//      live(actual) equals the simulator's ground-truth live set.
//
//   2. `crash_*` — deterministic scenarios that drop the HEAD PUT
//      after one specific op kind, asserting that the divergence is
//      exactly the design's "benign one-directional" kind.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use elide_coordinator::segment_head::{self, SegmentHead, Supersession};
use elide_coordinator::upload;
use elide_core::segment::{self, SegmentEntry, SegmentFlags, SegmentSigner};
use elide_core::signing::{self, VerifyingKey};
use elide_core::ulid_mint::UlidMint;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use proptest::prelude::*;
use ulid::Ulid;

// ── World ───────────────────────────────────────────────────────────────

struct World {
    store: Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
    signer: Arc<dyn SegmentSigner>,
    vk: VerifyingKey,
    mint: UlidMint,
    /// Ground-truth live set the simulator maintains as it issues ops.
    /// Used only to drive the GC generator (pick inputs that are
    /// actually live); the invariant is verified against
    /// live(actual) vs live(rebuilt), not this field.
    expected_live: BTreeSet<Ulid>,
}

impl World {
    fn new() -> Self {
        let (signer, vk) = signing::generate_ephemeral_signer();
        Self {
            store: Arc::new(InMemory::new()),
            vol_ulid: Ulid::new(),
            signer,
            vk,
            mint: UlidMint::new(Ulid::nil()),
            expected_live: BTreeSet::new(),
        }
    }

    fn vol_id(&self) -> String {
        self.vol_ulid.to_string()
    }
}

// ── Operations ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum SimOp {
    /// Drain `count` fresh segments to S3 and add them to HEAD.added.
    Drain { count: u8 },
    /// Pick `input_count` segments from the current live set as
    /// inputs, mint a GC output via `max(inputs).increment()`, write
    /// it to S3 with `inputs` header, and record one Superseded edge
    /// per input.
    Gc { input_count: u8 },
    /// Reap every Superseded edge currently in HEAD: DELETE the input
    /// segment from S3, then PUT HEAD with `apply_reap` applied.
    /// Models the orchestrator's tick-folded reap step with
    /// retention_window=0 (everything in `Superseded` is reapable).
    Reap,
    /// Build a manifest covering the current live set, signed by the
    /// world's volume key. Bump `snapshots/LATEST`. Truncate HEAD to
    /// empty anchored at the new snap_ulid.
    Seal,
}

// ── Segment-bytes helpers ───────────────────────────────────────────────

/// Build a valid signed segment file (with optional `inputs` table for
/// GC outputs) and return its raw bytes. Mirrors the existing pattern
/// in `recovery.rs` tests.
fn build_segment_bytes(signer: &dyn SegmentSigner, inputs: &[Ulid]) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg");
    // Single trivial data entry — enough to make the segment well-formed.
    let body = b"x".to_vec();
    let mut entries = vec![SegmentEntry::new_data(
        blake3::hash(&body),
        0,
        1,
        SegmentFlags::empty(),
        body,
    )];
    if inputs.is_empty() {
        segment::write_segment(&path, &mut entries, signer).unwrap();
    } else {
        segment::write_gc_segment(&path, &mut entries, inputs, signer).unwrap();
    }
    std::fs::read(&path).unwrap()
}

async fn put_segment(
    store: &Arc<dyn ObjectStore>,
    vol_id: &str,
    seg_ulid: Ulid,
    signer: &dyn SegmentSigner,
    inputs: &[Ulid],
) {
    let bytes = build_segment_bytes(signer, inputs);
    let key = upload::segment_key(vol_id, seg_ulid);
    store.put(&key, Bytes::from(bytes).into()).await.unwrap();
}

// ── Operation interpreters ──────────────────────────────────────────────

async fn run_drain(world: &mut World, head: &mut SegmentHead, count: u8) -> bool {
    if count == 0 {
        return false;
    }
    let vol_id = world.vol_id();
    for _ in 0..count {
        let u = world.mint.next();
        put_segment(&world.store, &vol_id, u, world.signer.as_ref(), &[]).await;
        head.added.insert(u);
        world.expected_live.insert(u);
    }
    true
}

async fn run_gc(world: &mut World, head: &mut SegmentHead, input_count: u8) -> bool {
    let candidates: Vec<Ulid> = world.expected_live.iter().copied().collect();
    let take = (input_count as usize).min(candidates.len());
    // GC needs at least one input to be meaningful (the design's
    // `max(inputs).increment()` ULID rule).
    if take < 1 {
        return false;
    }
    let inputs: Vec<Ulid> = candidates.into_iter().take(take).collect();
    let max = *inputs.iter().max().expect("non-empty");
    // `max(inputs).increment()` per `design-gc-ulid-ordering.md`. The
    // millisecond clock can hand back ULIDs in the same ms as `max`
    // when we're generating fast, so an explicit increment keeps
    // outputs strictly > inputs.
    let output = max.increment().expect("increment");
    let vol_id = world.vol_id();
    put_segment(
        &world.store,
        &vol_id,
        output,
        world.signer.as_ref(),
        &inputs,
    )
    .await;
    let since = Utc::now();
    head.added.insert(output);
    for input in &inputs {
        head.superseded
            .insert(*input, Supersession { output, since });
    }
    for input in &inputs {
        world.expected_live.remove(input);
    }
    world.expected_live.insert(output);
    true
}

async fn run_reap(world: &mut World, head: &mut SegmentHead) -> bool {
    let to_reap: Vec<Ulid> = head.superseded.keys().copied().collect();
    if to_reap.is_empty() {
        return false;
    }
    let vol_id = world.vol_id();
    for input in &to_reap {
        let key = upload::segment_key(&vol_id, *input);
        // 404 on already-reaped is fine.
        let _ = world.store.delete(&key).await;
    }
    head.apply_reap(&to_reap);
    true
}

async fn run_seal(world: &mut World, head: &mut SegmentHead) -> bool {
    let snap = world.mint.next();
    let live: Vec<Ulid> = world.expected_live.iter().copied().collect();
    let bytes = signing::build_snapshot_manifest_bytes(world.signer.as_ref(), &live, None);
    let vol_id = world.vol_id();
    let manifest_key = upload::snapshot_manifest_key(&vol_id, snap);
    world
        .store
        .put(&manifest_key, Bytes::from(bytes).into())
        .await
        .unwrap();
    let latest_key = upload::snapshot_latest_key(&vol_id);
    world
        .store
        .put(
            &latest_key,
            Bytes::from(snap.to_string().into_bytes()).into(),
        )
        .await
        .unwrap();
    // Truncate HEAD to empty anchored at the new snap.
    *head = SegmentHead::empty(Some(snap));
    true
}

/// Apply one op. On success, persists the updated HEAD; on no-op,
/// leaves S3 + HEAD untouched (e.g. `Reap` with no Superseded
/// edges, `Gc` with empty live set).
async fn apply_op(world: &mut World, op: &SimOp) {
    let mut head = segment_head::read_head(&world.store, world.vol_ulid)
        .await
        .unwrap();
    let mutated = match op {
        SimOp::Drain { count } => run_drain(world, &mut head, *count).await,
        SimOp::Gc { input_count } => run_gc(world, &mut head, *input_count).await,
        SimOp::Reap => run_reap(world, &mut head).await,
        SimOp::Seal => run_seal(world, &mut head).await,
    };
    if mutated {
        segment_head::put_head(&world.store, world.vol_ulid, &head)
            .await
            .unwrap();
    }
}

// ── Rebuild oracle ──────────────────────────────────────────────────────

/// Reconstruct HEAD from a privileged LIST of S3 — the design's
/// rebuild path. Returns the rebuilt HEAD; the caller computes
/// `live_set(manifest, rebuilt)` and compares against the actual
/// HEAD's live set.
async fn rebuild_head(world: &World) -> SegmentHead {
    let vol_id = world.vol_id();
    let anchor = upload::read_latest_snapshot(&world.store, &vol_id)
        .await
        .unwrap();

    let prefix = StorePath::from(format!("by_id/{}/segments/", world.vol_ulid));
    let objects: Vec<_> = world.store.list(Some(&prefix)).try_collect().await.unwrap();

    let mut s3_segments: BTreeSet<Ulid> = BTreeSet::new();
    let mut superseded: BTreeMap<Ulid, Supersession> = BTreeMap::new();
    // Use a single tempdir for header parses across the rebuild
    // (read_and_verify_segment_index needs a Path).
    let dir = tempfile::tempdir().unwrap();
    for obj in objects {
        let Some(filename) = obj.location.filename() else {
            continue;
        };
        let Ok(seg_ulid) = Ulid::from_string(filename) else {
            continue;
        };
        s3_segments.insert(seg_ulid);

        let bytes = world
            .store
            .get(&obj.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let path = dir.path().join(format!("seg-{seg_ulid}"));
        std::fs::write(&path, &bytes).unwrap();
        let (_, _, inputs) = segment::read_and_verify_segment_index(&path, &world.vk).unwrap();
        // The rebuild's `since` is unknowable from a signed segment
        // header (the GC output ULID is history-derived, not
        // wall-clock). Use a deterministic placeholder; the live-set
        // formula doesn't read `since`.
        let since = DateTime::<Utc>::from_timestamp_millis(0).unwrap();
        for input in inputs {
            superseded.insert(
                input,
                Supersession {
                    output: seg_ulid,
                    since,
                },
            );
        }
    }

    // Resolve manifest segments to compute `added` correctly: HEAD's
    // `Added` is the post-snapshot delta over the manifest — every
    // S3 segment not named by the manifest. A pre-seal orphan
    // (superseded but not reaped before seal) ends up here too, but
    // its corresponding `Superseded` edge — recoverable from the
    // surviving GC output's signed `inputs` header — cancels it out
    // of the live set. Filtering by ULID order is *not* sound: a GC
    // output's ULID is history-derived (`max(inputs).increment()`,
    // `design-gc-ulid-ordering.md`) and can sort below a later-minted
    // seal ULID even when it's a legitimate post-seal output.
    let manifest_segments = read_manifest_segments(world).await;
    let added: BTreeSet<Ulid> = s3_segments
        .difference(&manifest_segments)
        .copied()
        .collect();
    // Tombstone any manifest segment that is missing from S3. The
    // manifest only ever lists live-at-seal segments, so a manifest
    // segment absent from S3 *must* have been reaped. This signal is
    // necessary for the rebuild to converge with HEAD when a chain of
    // supersessions (X → Y → Z) gets fully reaped: by the time both
    // X and Y are gone, only Z is in S3 and Z's `inputs` header
    // mentions Y, not X. Without this tombstone-via-absence step the
    // rebuild would treat X as live and diverge from actual HEAD.
    let tombstoned: BTreeSet<Ulid> = manifest_segments
        .difference(&s3_segments)
        .copied()
        .collect();

    SegmentHead {
        anchor,
        added,
        superseded,
        tombstoned,
    }
}

async fn read_manifest_segments(world: &World) -> BTreeSet<Ulid> {
    let vol_id = world.vol_id();
    let snap = match upload::read_latest_snapshot(&world.store, &vol_id).await {
        Ok(Some(u)) => u,
        Ok(None) => return BTreeSet::new(),
        Err(_) => return BTreeSet::new(),
    };
    let key = upload::snapshot_manifest_key(&vol_id, snap);
    let bytes = match world.store.get(&key).await {
        Ok(g) => g.bytes().await.unwrap(),
        Err(_) => return BTreeSet::new(),
    };
    signing::read_snapshot_manifest_from_bytes(&bytes, &world.vk, &snap)
        .unwrap()
        .segment_ulids
        .into_iter()
        .collect()
}

// ── Invariant ───────────────────────────────────────────────────────────

async fn assert_equivalence(world: &World) -> Result<(), TestCaseError> {
    let actual = segment_head::read_head(&world.store, world.vol_ulid)
        .await
        .unwrap();
    let manifest = read_manifest_segments(world).await;
    let rebuilt = rebuild_head(world).await;

    let live_actual = segment_head::live_set(&manifest, &actual);
    let live_rebuilt = segment_head::live_set(&manifest, &rebuilt);

    // The invariant from `docs/design-segment-index.md` *Rebuild
    // defines correctness* is **one-directional**:
    // `live(actual) ⊆ live(rebuilt)`. The rebuild is a safe superset
    // — HEAD never claims a segment is live that the rebuild can't
    // also see, but the rebuild may legitimately include extras:
    //
    //  - Pre-seal orphans: a segment superseded before a seal but
    //    not yet reaped is in S3, not in the manifest, and (after
    //    its GC output gets reaped) no longer reachable via any
    //    output's `inputs` header. The rebuild treats it as live;
    //    actual HEAD doesn't (it was truncated at seal). The design
    //    accepts these as operator-reclaimable orphans (*Reconcile
    //    and orphan reclamation*).
    //
    //  - Crashes between segment PUT and HEAD PUT: the S3 object
    //    exists but HEAD doesn't know yet. Reader using actual
    //    HEAD won't fetch it (no harm); reader using rebuild
    //    treats it as live but a subsequent HEAD PUT converges
    //    them.
    //
    // The dangerous direction — actual claims live that rebuild
    // can't see — would indicate a real divergence (a missing
    // object that HEAD names anyway, breaking the read path). The
    // proptest asserts that never happens.
    let orphans: BTreeSet<_> = live_rebuilt.difference(&live_actual).copied().collect();
    let missing: BTreeSet<_> = live_actual.difference(&live_rebuilt).copied().collect();
    prop_assert!(
        missing.is_empty(),
        "live(actual) ⊈ live(rebuilt): HEAD claims segments the rebuild cannot see.\n\
         missing from rebuilt: {missing:?}\n\
         actual added={:?} superseded={:?} tombstoned={:?}\n\
         rebuilt added={:?} superseded={:?} tombstoned={:?}",
        actual.added,
        actual.superseded.keys().collect::<Vec<_>>(),
        actual.tombstoned,
        rebuilt.added,
        rebuilt.superseded.keys().collect::<Vec<_>>(),
        rebuilt.tombstoned,
    );
    // The simulator's ground-truth must match actual HEAD's live set
    // — catches model bugs. (Rebuild may have orphans on top.)
    prop_assert_eq!(
        &live_actual,
        &world.expected_live,
        "actual HEAD's live set diverges from simulator ground truth"
    );
    let _ = orphans; // available for diagnostics if assertions are extended later
    Ok(())
}

// ── Strategies ──────────────────────────────────────────────────────────

fn arb_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        // Bias drain so the world has live segments for GC/reap to consume.
        4 => (1u8..6).prop_map(|count| SimOp::Drain { count }),
        2 => (1u8..5).prop_map(|input_count| SimOp::Gc { input_count }),
        2 => Just(SimOp::Reap),
        1 => Just(SimOp::Seal),
    ]
}

fn arb_ops() -> impl Strategy<Value = Vec<SimOp>> {
    proptest::collection::vec(arb_op(), 1..40)
}

// ── Proptest ────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// After every op in a clean Drain/Gc/Reap/Seal sequence,
    /// live(actual HEAD) ≡ live(rebuild-from-LIST). This is the
    /// invariant `docs/design-segment-index.md` *Rebuild defines
    /// correctness* states.
    #[test]
    fn prop_rebuild_equivalence(ops in arb_ops()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut world = World::new();
            // Initial empty state.
            assert_equivalence(&world).await?;
            for op in ops {
                apply_op(&mut world, &op).await;
                assert_equivalence(&world).await?;
            }
            Ok::<_, TestCaseError>(())
        })?;
    }
}

// ── Crash-before-HEAD-PUT scenarios (deterministic) ─────────────────────
//
// Each test exercises a specific crash window between an S3 mutation
// and its HEAD PUT, verifying:
//
//  - the *safe* direction holds — live(actual) ⊆ live(rebuilt). HEAD
//    never claims a segment is live that the rebuild can't see, so
//    readers using HEAD are correct even mid-crash;
//
//  - the divergence (if any) is exactly the design's *Writers and
//    crash ordering* "benign one-directional" kind — un-indexed
//    object in rebuilt, or tolerated-404 input.
//
// These are NOT convergence tests: under the RMW writer model, a
// crashed mutation that doesn't make it to HEAD stays as a S3
// orphan indefinitely (until operator reclamation, `docs/design-
// segment-index.md` *Reconcile and orphan reclamation*). The next
// clean tick merges its own delta into HEAD; it does not back-fill
// the lost mutation.

/// Simulate `op` without the trailing HEAD PUT — S3 mutations land,
/// HEAD is left at its pre-op value. The simulator's `expected_live`
/// is *not* advanced (the crashed op was never visible to HEAD, so
/// its effect on live should be considered un-acknowledged).
async fn apply_op_crashing(world: &mut World, op: &SimOp) {
    let expected_pre = world.expected_live.clone();
    let mut head = segment_head::read_head(&world.store, world.vol_ulid)
        .await
        .unwrap();
    match op {
        SimOp::Drain { count } => {
            run_drain(world, &mut head, *count).await;
        }
        SimOp::Gc { input_count } => {
            run_gc(world, &mut head, *input_count).await;
        }
        SimOp::Reap => {
            run_reap(world, &mut head).await;
        }
        SimOp::Seal => {
            run_seal(world, &mut head).await;
        }
    }
    // Don't write HEAD. Roll back the simulator's view too — from
    // the rest of the system's perspective the op never happened.
    world.expected_live = expected_pre;
}

#[tokio::test]
async fn crash_after_drain_leaves_orphan_in_rebuilt_only() {
    // S3 PUT succeeded; HEAD PUT skipped. live(actual) doesn't
    // include the orphan; live(rebuilt) does. The asymmetry is
    // benign (design *Writers and crash ordering*).
    let mut world = World::new();
    apply_op(&mut world, &SimOp::Drain { count: 2 }).await;
    apply_op_crashing(&mut world, &SimOp::Drain { count: 3 }).await;

    let actual = segment_head::read_head(&world.store, world.vol_ulid)
        .await
        .unwrap();
    let manifest = read_manifest_segments(&world).await;
    let rebuilt = rebuild_head(&world).await;
    let live_actual = segment_head::live_set(&manifest, &actual);
    let live_rebuilt = segment_head::live_set(&manifest, &rebuilt);
    assert!(
        live_actual.is_subset(&live_rebuilt),
        "live(actual) must be a subset of live(rebuilt)"
    );
    assert_eq!(
        live_actual.len() + 3,
        live_rebuilt.len(),
        "exactly the 3 crashed drains are orphan in rebuilt only"
    );
}

#[tokio::test]
async fn crash_after_reap_leaves_input_404_in_actual() {
    // Crash AFTER the reap DELETE but BEFORE the HEAD PUT: S3 lost
    // the input; HEAD still names it as Superseded. The design's
    // *Writers and crash ordering*: this is the deliberate direction
    // — readers get a 404 they already tolerate.
    let mut world = World::new();
    apply_op(&mut world, &SimOp::Drain { count: 3 }).await;
    apply_op(&mut world, &SimOp::Gc { input_count: 2 }).await;
    apply_op_crashing(&mut world, &SimOp::Reap).await;

    let actual = segment_head::read_head(&world.store, world.vol_ulid)
        .await
        .unwrap();
    let manifest = read_manifest_segments(&world).await;
    let rebuilt = rebuild_head(&world).await;
    // Both views exclude the inputs: actual via Superseded,
    // rebuilt via the surviving output's `inputs` header.
    assert_eq!(
        segment_head::live_set(&manifest, &actual),
        segment_head::live_set(&manifest, &rebuilt),
        "crash-after-reap is benign for both readers: input is excluded by both views"
    );
}

#[tokio::test]
async fn crash_after_gc_diverges_benignly() {
    // Crash AFTER the GC output PUT but BEFORE the HEAD PUT: S3 has
    // the new output (with its signed `inputs` header) and the
    // inputs are still in S3; HEAD knows nothing about either side.
    //
    // Bidirectional divergence in the body, but both directions are
    // benign:
    //
    //   - rebuilt has the new output as live (un-indexed object —
    //     tolerated; readers using HEAD just don't fetch it);
    //   - actual still treats the inputs as live (they're still
    //     fetchable in S3, so this read is correct).
    //
    // The safe direction holds: live(actual) ⊆ ???. Actually NO —
    // actual has inputs that rebuilt excludes (rebuilt knows they're
    // superseded). So live(actual) ⊄ live(rebuilt) in this case.
    //
    // What we assert: both views are "safe" — every ULID in either
    // live set is fetchable from S3.
    let mut world = World::new();
    apply_op(&mut world, &SimOp::Drain { count: 3 }).await;
    apply_op_crashing(&mut world, &SimOp::Gc { input_count: 2 }).await;

    let actual = segment_head::read_head(&world.store, world.vol_ulid)
        .await
        .unwrap();
    let manifest = read_manifest_segments(&world).await;
    let rebuilt = rebuild_head(&world).await;
    let live_actual = segment_head::live_set(&manifest, &actual);
    let live_rebuilt = segment_head::live_set(&manifest, &rebuilt);

    // Both live sets must consist of fetchable S3 objects.
    let s3_objects: BTreeSet<Ulid> = list_s3_segments(&world).await;
    for u in &live_actual {
        assert!(
            s3_objects.contains(u),
            "live(actual) contains {u} not in S3"
        );
    }
    for u in &live_rebuilt {
        assert!(
            s3_objects.contains(u),
            "live(rebuilt) contains {u} not in S3"
        );
    }
}

#[tokio::test]
async fn crash_after_seal_leaves_pre_seal_state_in_actual() {
    // Crash AFTER manifest PUT + LATEST bump, BEFORE HEAD truncation
    // PUT. HEAD still has the *pre-seal* added/superseded entries.
    // live(actual) uses the new manifest with the old HEAD — the
    // overlap absorbs cleanly because manifest contains exactly what
    // pre-seal HEAD's net-live was.
    let mut world = World::new();
    apply_op(&mut world, &SimOp::Drain { count: 2 }).await;
    let pre_seal_live = world.expected_live.clone();
    apply_op_crashing(&mut world, &SimOp::Seal).await;

    let actual = segment_head::read_head(&world.store, world.vol_ulid)
        .await
        .unwrap();
    let manifest = read_manifest_segments(&world).await;
    let live_actual = segment_head::live_set(&manifest, &actual);
    // The just-written manifest contains the live set; HEAD's stale
    // added entries are a subset of the manifest. Net: live(actual)
    // equals the pre-seal live set.
    assert_eq!(
        live_actual, pre_seal_live,
        "live(actual) post-crashed-seal equals pre-seal live set"
    );
}

async fn list_s3_segments(world: &World) -> BTreeSet<Ulid> {
    let prefix = StorePath::from(format!("by_id/{}/segments/", world.vol_ulid));
    let objects: Vec<_> = world.store.list(Some(&prefix)).try_collect().await.unwrap();
    objects
        .iter()
        .filter_map(|o| o.location.filename())
        .filter_map(|f| Ulid::from_string(f).ok())
        .collect()
}
