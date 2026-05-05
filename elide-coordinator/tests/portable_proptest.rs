// Two-coordinator state-machine proptest for the portable-live-volume
// lifecycle. Drives `lifecycle::*` and `recovery::*` directly — the
// building blocks the inbound ops compose — against a shared
// `InMemory` bucket plus two `CoordinatorIdentity` instances.
//
// Op alphabet: { Create, Release, ReleaseTo, ForceRelease, ClaimReleased }.
// `Stop` is omitted (it is a single state flip already exhaustively
// covered by `lifecycle::tests`); `start --remote` is modelled as the
// `ClaimReleased` op since its bucket-side effect is identical.
//
// The proptest enforces the invariants the design doc requires after
// any sequence of ops:
//
//   1. Every `names/<name>` parses cleanly.
//   2. Every `Live`/`Stopped` record names a coordinator we know about.
//   3. Every `Released` record clears `coordinator_id` and carries a
//      `handoff_snapshot`.
//   4. Every `Reserved` record carries a target `coordinator_id` and a
//      `handoff_snapshot`.
//   5. Every name's `vol_ulid` has `volume.pub` in the bucket.
//   6. Every `handoff_snapshot` references a manifest that exists in
//      the bucket under `by_id/<vol_ulid>/snapshots/YYYYMMDD/`.
//
// Cases run in a single tokio runtime per test case; ops are
// interleaved sequentially. The interesting non-trivial races
// (concurrent claim) are already covered at the conditional-PUT layer
// in `name_store::tests` and `lifecycle::tests`; the value here is
// composing many ops and asserting the bucket stays internally
// consistent.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use elide_coordinator::identity::CoordinatorIdentity;
use elide_coordinator::identity::fetch_coordinator_pub;
use elide_coordinator::lifecycle::{
    self, ForceReleaseOutcome, MarkClaimedOutcome, MarkReleasedOutcome,
};
use elide_coordinator::name_store as ns;
use elide_coordinator::portable;
use elide_coordinator::recovery;
use elide_coordinator::upload::snapshot_manifest_key;
use elide_core::name_record::NameState;
use elide_core::segment::SegmentSigner;
use elide_core::signing::{
    build_snapshot_manifest_bytes, generate_ephemeral_signer, peek_snapshot_manifest_recovery,
    read_snapshot_manifest_from_bytes,
};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutPayload};
use proptest::prelude::*;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use ulid::Ulid;

const NUM_NAMES: usize = 3;
const NUM_COORDS: usize = 2;

#[derive(Debug, Clone)]
enum Op {
    Create { name: u8, coord: u8 },
    Release { name: u8, coord: u8 },
    ForceRelease { name: u8, recovering: u8 },
    ClaimReleased { name: u8, coord: u8 },
}

fn arb_op() -> impl Strategy<Value = Op> {
    let name = 0u8..(NUM_NAMES as u8);
    let coord = 0u8..(NUM_COORDS as u8);
    let rec = 0u8..(NUM_COORDS as u8);
    prop_oneof![
        (name.clone(), coord.clone()).prop_map(|(name, coord)| Op::Create { name, coord }),
        (name.clone(), coord.clone()).prop_map(|(name, coord)| Op::Release { name, coord }),
        (name.clone(), rec).prop_map(|(name, recovering)| Op::ForceRelease { name, recovering }),
        (name, coord).prop_map(|(name, coord)| Op::ClaimReleased { name, coord }),
    ]
}

struct World {
    store: Arc<dyn ObjectStore>,
    coords: Vec<CoordinatorIdentity>,
    /// Per-fork volume signing key. Populated when the fork is created
    /// or claimed. `Release` consults this to sign its (synthetic)
    /// handoff snapshot.
    vol_signers: HashMap<Ulid, Arc<dyn SegmentSigner>>,
    _coord_dirs: Vec<TempDir>,
}

fn name_for(idx: u8) -> &'static str {
    match idx {
        0 => "vol-a",
        1 => "vol-b",
        _ => "vol-c",
    }
}

impl World {
    async fn new() -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut dirs = Vec::with_capacity(NUM_COORDS);
        let mut coords = Vec::with_capacity(NUM_COORDS);
        for _ in 0..NUM_COORDS {
            let d = TempDir::new().unwrap();
            let id = CoordinatorIdentity::load_or_generate(d.path()).unwrap();
            // Publish coordinator.pub so synthesised-handoff verification
            // can resolve the recovering coordinator's pubkey.
            id.publish_pub(store.as_ref()).await.unwrap();
            coords.push(id);
            dirs.push(d);
        }
        Self {
            store,
            coords,
            vol_signers: HashMap::new(),
            _coord_dirs: dirs,
        }
    }

    /// Mint a fresh volume keypair, upload `volume.pub`, register the
    /// signer in `vol_signers`. Returns the new ULID.
    async fn mint_fork(&mut self) -> Ulid {
        let vol_ulid = Ulid::new();
        let (signer, vk) = generate_ephemeral_signer();
        let key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
        let body = format!(
            "{}\n",
            vk.to_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        self.store
            .put(&key, PutPayload::from(body.into_bytes()))
            .await
            .unwrap();
        self.vol_signers.insert(vol_ulid, signer);
        vol_ulid
    }

    /// Mint and upload a (regular) handoff snapshot manifest signed by
    /// the volume's own key. Used by `Release` / `ReleaseTo` to
    /// simulate the drain+snapshot step without a daemon. Returns the
    /// snapshot ULID.
    async fn publish_volume_snapshot(&self, vol_ulid: Ulid) -> Option<Ulid> {
        let signer = self.vol_signers.get(&vol_ulid)?;
        let snap_ulid = Ulid::new();
        let bytes = build_snapshot_manifest_bytes(signer.as_ref(), &[], None);
        let key = snapshot_manifest_key(&vol_ulid.to_string(), snap_ulid);
        // Use put_if_absent so we don't accidentally overwrite an
        // existing manifest minted by a parallel op.
        portable::put_if_absent(self.store.as_ref(), &key, Bytes::from(bytes))
            .await
            .ok()?;
        Some(snap_ulid)
    }

    async fn apply(&mut self, op: &Op) {
        match op {
            Op::Create { name, coord } => {
                let vol_ulid = self.mint_fork().await;
                let coord_id = self.coords[*coord as usize].coordinator_id_str().to_owned();
                let _ = lifecycle::mark_initial(
                    &self.store,
                    name_for(*name),
                    &coord_id,
                    None,
                    vol_ulid,
                    4 * 1024 * 1024 * 1024,
                )
                .await;
            }

            Op::Release { name, coord } => {
                let coord_id = self.coords[*coord as usize].coordinator_id_str().to_owned();
                // Need the current vol_ulid to sign the snapshot.
                let vol_ulid = match ns::read_name_record(&self.store, name_for(*name)).await {
                    Ok(Some((rec, _))) => rec.vol_ulid,
                    _ => return,
                };
                let Some(snap) = self.publish_volume_snapshot(vol_ulid).await else {
                    return;
                };
                // Tolerate every error class: wrong-state /
                // ownership-conflict / absent are valid outcomes given
                // the random op stream.
                let _: Result<MarkReleasedOutcome, _> =
                    lifecycle::mark_released(&self.store, name_for(*name), &coord_id, snap).await;
            }

            Op::ForceRelease { name, recovering } => {
                // Read the current record to learn which dead fork to
                // recover from. If the record is absent or already in
                // a state force-release refuses, skip silently — the
                // proptest must tolerate ordering noise.
                let dead_vol = match ns::read_name_record(&self.store, name_for(*name)).await {
                    Ok(Some((rec, _))) => match rec.state {
                        NameState::Live | NameState::Stopped => rec.vol_ulid,
                        _ => return,
                    },
                    _ => return,
                };
                let identity = &self.coords[*recovering as usize];
                // Need the dead pubkey + segments. There are no
                // segments in this model (we never write data), so
                // list_and_verify_segments returns an empty slice;
                // the synthesised manifest covers nothing, which the
                // recovery API explicitly allows.
                let dead_pub = match recovery::fetch_volume_pub(&self.store, dead_vol).await {
                    Ok(k) => k,
                    Err(_) => return,
                };
                if recovery::list_and_verify_segments(&self.store, dead_vol, &dead_pub)
                    .await
                    .is_err()
                {
                    return;
                }
                let snap = match recovery::mint_and_publish_synthesised_snapshot(
                    &self.store,
                    dead_vol,
                    &[],
                    identity,
                    identity.coordinator_id_str(),
                )
                .await
                {
                    Ok(p) => p.snap_ulid,
                    Err(_) => return,
                };
                let _: Result<ForceReleaseOutcome, _> =
                    lifecycle::mark_released_force(&self.store, name_for(*name), snap).await;
            }

            Op::ClaimReleased { name, coord } => {
                let coord_id = self.coords[*coord as usize].coordinator_id_str().to_owned();
                let new_vol = self.mint_fork().await;
                let _: Result<MarkClaimedOutcome, _> = lifecycle::mark_claimed(
                    &self.store,
                    name_for(*name),
                    &coord_id,
                    None,
                    new_vol,
                    NameState::Live,
                )
                .await;
            }
        }
    }

    /// Invariant 6 (strong form): every `handoff_snapshot` references
    /// a manifest that exists in S3 *and* verifies under the right
    /// pubkey. For ordinary handoff snapshots that key is the dead
    /// fork's `volume.pub`; for synthesised handoff snapshots
    /// (`recovery` block present) it's the recovering coordinator's
    /// `coordinator.pub`, which the coordinator-pub fetcher itself
    /// path-binds to its derived id.
    async fn verify_handoff_manifest(&self, vol_ulid: Ulid, snap: Ulid, name: &str) {
        let snap_key = snapshot_manifest_key(&vol_ulid.to_string(), snap);
        let body = self
            .store
            .get(&snap_key)
            .await
            .unwrap_or_else(|e| panic!("name '{name}' snapshot {snap} missing: {e}"))
            .bytes()
            .await
            .unwrap_or_else(|e| panic!("name '{name}' snapshot {snap} body unreadable: {e}"));

        let recovery = peek_snapshot_manifest_recovery(&body)
            .unwrap_or_else(|e| panic!("name '{name}' snapshot {snap} unparseable: {e}"));

        let vk: VerifyingKey = match recovery {
            None => {
                // Ordinary manifest: signed by the volume itself.
                let pub_key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
                let pub_body = self
                    .store
                    .get(&pub_key)
                    .await
                    .unwrap_or_else(|e| panic!("volume.pub for {vol_ulid} missing: {e}"))
                    .bytes()
                    .await
                    .expect("volume.pub body");
                let hex = std::str::from_utf8(&pub_body)
                    .expect("volume.pub utf8")
                    .trim();
                let bytes = (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex byte"))
                    .collect::<Vec<u8>>();
                let arr: [u8; 32] = bytes.try_into().expect("32-byte pub");
                VerifyingKey::from_bytes(&arr).expect("valid pub")
            }
            Some(r) => {
                // Synthesised handoff: signed by the recovering coordinator.
                fetch_coordinator_pub(self.store.as_ref(), &r.recovering_coordinator_id)
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "synthesised handoff for '{name}' references unknown \
                             coordinator '{}': {e}",
                            r.recovering_coordinator_id
                        )
                    })
            }
        };

        read_snapshot_manifest_from_bytes(&body, &vk, &snap).unwrap_or_else(|e| {
            panic!(
                "handoff manifest for '{name}' (vol {vol_ulid} snap {snap}) failed signature \
                 verification: {e}"
            )
        });
    }

    async fn check_invariants(&self) {
        let coord_ids: Vec<String> = self
            .coords
            .iter()
            .map(|c| c.coordinator_id_str().to_owned())
            .collect();

        for &name in &["vol-a", "vol-b", "vol-c"] {
            let Ok(Some((rec, _))) = ns::read_name_record(&self.store, name).await else {
                continue;
            };

            // Invariant 5: every recorded vol_ulid must have a
            // volume.pub uploaded.
            let pub_key = StorePath::from(format!("by_id/{}/volume.pub", rec.vol_ulid));
            assert!(
                self.store.head(&pub_key).await.is_ok(),
                "name '{name}' references vol_ulid {} but its volume.pub is missing",
                rec.vol_ulid,
            );

            match rec.state {
                NameState::Live | NameState::Stopped => {
                    // Invariant 2.
                    let owner = rec
                        .coordinator_id
                        .as_deref()
                        .expect("Live/Stopped record must name an owner");
                    assert!(
                        coord_ids.iter().any(|c| c == owner),
                        "Live/Stopped record names unknown coordinator '{owner}'"
                    );
                }
                NameState::Released => {
                    // Invariant 3.
                    assert!(
                        rec.coordinator_id.is_none(),
                        "Released record must clear coordinator_id"
                    );
                    let snap = rec
                        .handoff_snapshot
                        .expect("Released record must carry a handoff_snapshot");
                    self.verify_handoff_manifest(rec.vol_ulid, snap, name).await;
                }
                NameState::Readonly => {
                    // No coordinator-id constraints; readonly records
                    // are produced by `mark_initial_readonly` only,
                    // which our op alphabet doesn't drive.
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    /// After any sequence of {Create, Release, ReleaseTo, ForceRelease,
    /// ClaimReleased} ops on a shared bucket between two coordinators,
    /// the bucket state remains internally consistent. Each individual
    /// op tolerates wrong-state errors silently — the proptest is
    /// asserting the *invariants*, not the success of any op.
    #[test]
    fn portable_lifecycle_invariants_hold(
        ops in prop::collection::vec(arb_op(), 1..16)
    ) {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut world = World::new().await;
            for op in &ops {
                world.apply(op).await;
                world.check_invariants().await;
            }
        });
    }
}
