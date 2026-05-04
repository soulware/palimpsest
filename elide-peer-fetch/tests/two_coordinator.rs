//! Two-coordinator integration tests for the peer-fetch `.body` route.
//!
//! These exercise the cross-host handoff scenario at the HTTP layer:
//! a "previous claimer" (peer A) runs a peer-fetch server backed by
//! the volume's segment files on disk, and the "new claimer" (running
//! on a different host) drives `BodyFetchClient` / `PeerRangeFetcher`
//! against it. Both coordinators share the same in-memory
//! `ObjectStore`, modelling the bucket trust boundary.
//!
//! Scope is the wire surface only. The volume daemon, IPC, and
//! supervisor are not exercised here — the intent is to validate that
//! the route's auth pipeline + 200/206/404 + Content-Range + decorator
//! splice behaviour stay coherent across an honest-to-goodness HTTP
//! round-trip with independent server and client processes (separate
//! tokio tasks).

use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use bytes::Bytes;
use ed25519_dalek::{Signer, SigningKey};
use elide_core::segment::{
    SegmentEntry, SegmentFlags, SegmentSigner, promote_to_cache, write_segment,
};
use elide_core::signing::{ProvenanceLineage, write_provenance};
use elide_fetch::RangeFetcher;
use elide_peer_fetch::auth::AuthState;
use elide_peer_fetch::server::{ServerContext, router};
use elide_peer_fetch::{BodyFetchClient, BodyTokenSigner, PeerEndpoint, PeerRangeFetcher};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use rand_core::OsRng;
use tempfile::TempDir;
use tokio::net::TcpListener;
use ulid::Ulid;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Test volume — owns a signing key and a published `volume.pub` /
/// `volume.provenance` pair in the shared object store.
struct TestVolume {
    ulid: Ulid,
    key: SigningKey,
}

async fn mk_volume(store: &Arc<dyn ObjectStore>) -> TestVolume {
    let ulid = Ulid::new();
    let key = SigningKey::generate(&mut OsRng);

    let tmp = TempDir::new().unwrap();
    std::fs::write(tmp.path().join("volume.pub"), pub_hex(&key)).unwrap();
    write_provenance(
        tmp.path(),
        &key,
        "volume.provenance",
        &ProvenanceLineage::default(),
    )
    .unwrap();
    let pub_bytes = std::fs::read(tmp.path().join("volume.pub")).unwrap();
    let prov_bytes = std::fs::read(tmp.path().join("volume.provenance")).unwrap();
    store
        .put(
            &StorePath::from(format!("by_id/{ulid}/volume.pub")),
            Bytes::from(pub_bytes).into(),
        )
        .await
        .unwrap();
    store
        .put(
            &StorePath::from(format!("by_id/{ulid}/volume.provenance")),
            Bytes::from(prov_bytes).into(),
        )
        .await
        .unwrap();

    TestVolume { ulid, key }
}

/// Lay out one segment under a peer's `data_dir`. Each `entries_spec`
/// element becomes a Data entry; the per-entry `present` flag controls
/// whether that entry's bit is set in `<seg>.present` on disk.
///
/// Returns `(seg_ulid, body_section_start)` so callers can compute the
/// absolute byte offsets the decorator translates to body-relative
/// before hitting the peer.
fn mk_segment(
    data_dir: &std::path::Path,
    vol: &TestVolume,
    entries_spec: Vec<(Vec<u8>, bool)>,
) -> (Ulid, u64) {
    let seg_ulid = Ulid::new();
    let mut entries: Vec<SegmentEntry> = entries_spec
        .iter()
        .enumerate()
        .map(|(i, (payload, _))| {
            let hash = blake3::hash(payload);
            SegmentEntry::new_data(hash, i as u64, 1, SegmentFlags::empty(), payload.clone())
        })
        .collect();
    let staging = data_dir.join(format!("staging-{seg_ulid}"));
    let signer = TestSegSigner(vol.key.clone());
    write_segment(&staging, &mut entries, &signer).unwrap();

    let by_id = data_dir.join("by_id").join(vol.ulid.to_string());
    let cache = by_id.join("cache");
    let index = by_id.join("index");
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::create_dir_all(&index).unwrap();
    let body_path = cache.join(format!("{seg_ulid}.body"));
    let present_path = cache.join(format!("{seg_ulid}.present"));
    promote_to_cache(&staging, &body_path, &present_path).unwrap();

    // promote_to_cache marks every Data entry as present; rewrite
    // the bitmap to honour the per-entry `present` flags.
    let bitset_len = entries_spec.len().div_ceil(8);
    let mut bitmap = vec![0u8; bitset_len];
    for (idx, (_, present)) in entries_spec.iter().enumerate() {
        if *present {
            bitmap[idx / 8] |= 1 << (idx % 8);
        }
    }
    std::fs::write(&present_path, &bitmap).unwrap();

    let seg_bytes = std::fs::read(&staging).unwrap();
    let index_length =
        u32::from_le_bytes([seg_bytes[12], seg_bytes[13], seg_bytes[14], seg_bytes[15]]);
    let inline_length =
        u32::from_le_bytes([seg_bytes[16], seg_bytes[17], seg_bytes[18], seg_bytes[19]]);
    let body_section_start = 100u64 + index_length as u64 + inline_length as u64;
    let idx_path = index.join(format!("{seg_ulid}.idx"));
    std::fs::write(&idx_path, &seg_bytes[..body_section_start as usize]).unwrap();

    (seg_ulid, body_section_start)
}

/// Spawn a peer-fetch server on an ephemeral TCP port. Returns the
/// public endpoint plus the `JoinHandle` (held by the test to keep the
/// task alive for its lifetime).
struct TestPeer {
    endpoint: PeerEndpoint,
    _server: tokio::task::JoinHandle<()>,
    _data_dir: TempDir,
}

async fn spawn_peer(store: Arc<dyn ObjectStore>, data_dir: TempDir) -> TestPeer {
    let auth = AuthState::new(store);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let ctx = ServerContext::new(auth, data_dir.path().to_owned());
    let app = router(ctx);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let endpoint = PeerEndpoint::new(local_addr.ip().to_string(), local_addr.port());
    TestPeer {
        endpoint,
        _server: server,
        _data_dir: data_dir,
    }
}

/// `SegmentSigner` over a raw `SigningKey` — `elide_core::Ed25519Signer`
/// is private.
struct TestSegSigner(SigningKey);
impl SegmentSigner for TestSegSigner {
    fn sign(&self, msg: &[u8]) -> [u8; 64] {
        self.0.sign(msg).to_bytes()
    }
}

/// `BodyTokenSigner` impl over a raw `SigningKey`. Allows constructing
/// a body token whose `vol_ulid` is independent of the URL vol_id —
/// needed for the lineage-rejection negative case.
#[derive(Debug)]
struct TestBodySigner {
    vol_ulid: Ulid,
    key: SigningKey,
}
impl BodyTokenSigner for TestBodySigner {
    fn vol_ulid(&self) -> Ulid {
        self.vol_ulid
    }
    fn sign(&self, msg: &[u8]) -> [u8; 64] {
        self.key.sign(msg).to_bytes()
    }
}

/// Hex-encoded volume.pub line as the peer-fetch routes expect.
fn pub_hex(key: &SigningKey) -> String {
    let bytes = key.verifying_key().to_bytes();
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s.push('\n');
    s
}

/// Format a segment-store key the way `elide-fetch` does. The peer
/// decorator parses this to extract `(vol_id, seg_ulid)`.
fn segment_key(vol: Ulid, seg: Ulid) -> String {
    format!("by_id/{vol}/segments/19700101/{seg}")
}

/// Sync `RangeFetcher` mock recording every call.
struct RecordingInner {
    calls: StdMutex<Vec<(String, u64, u64)>>,
    fallback: Vec<u8>,
}
impl RecordingInner {
    fn new(fallback: Vec<u8>) -> Self {
        Self {
            calls: StdMutex::new(Vec::new()),
            fallback,
        }
    }
    fn calls(&self) -> Vec<(String, u64, u64)> {
        self.calls.lock().unwrap().clone()
    }
}
impl RangeFetcher for RecordingInner {
    fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> std::io::Result<Vec<u8>> {
        self.calls
            .lock()
            .unwrap()
            .push((key.to_owned(), start, end_exclusive));
        let len = (end_exclusive - start) as usize;
        Ok(self.fallback.iter().cycle().take(len).copied().collect())
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

/// Cross-host happy path: previous claimer (peer A) runs a server
/// holding the segment body. The new claimer's `BodyFetchClient` —
/// representing volume V running on a different host, signing with
/// V's `volume.key` — fetches the full body byte range and gets a
/// 200 with the expected bytes.
#[tokio::test]
async fn cross_host_full_body_hit() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let v = mk_volume(&store).await;

    let payload = vec![0xA5u8; 4096];
    let data_dir = TempDir::new().unwrap();
    let (seg_ulid, _body_section_start) =
        mk_segment(data_dir.path(), &v, vec![(payload.clone(), true)]);
    let peer_a = spawn_peer(store.clone(), data_dir).await;

    let signer = Arc::new(TestBodySigner {
        vol_ulid: v.ulid,
        key: v.key.clone(),
    });
    let client = BodyFetchClient::new(signer).unwrap();

    let bytes = client
        .fetch_body_range(&peer_a.endpoint, v.ulid, seg_ulid, 0, payload.len() as u64)
        .await
        .expect("peer A serves the full body");
    assert_eq!(&bytes[..], payload.as_slice());
}

/// Cross-host partial-coverage splice. Peer A's `.present` shows only
/// the first of two Data entries cached. The new claimer's
/// `PeerRangeFetcher` issues one body request, gets a 206 with the
/// peer-covered prefix, and splices the remainder via the inner
/// fetcher. Counters attribute the prefix to peer and the remainder
/// to store, summing to the requested length.
#[tokio::test]
async fn cross_host_partial_coverage_splice() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let v = mk_volume(&store).await;

    let first = vec![0xAAu8; 4096];
    let second = vec![0xBBu8; 4096];
    let data_dir = TempDir::new().unwrap();
    let data_dir_path = data_dir.path().to_owned();
    let (seg_ulid, body_section_start) = mk_segment(
        data_dir.path(),
        &v,
        vec![(first.clone(), true), (second.clone(), false)],
    );
    let peer_a = spawn_peer(store.clone(), data_dir).await;

    let signer = Arc::new(TestBodySigner {
        vol_ulid: v.ulid,
        key: v.key.clone(),
    });
    let client = BodyFetchClient::new(signer).unwrap();
    // 0xEE distinguishes the inner-store remainder bytes from the
    // peer-served prefix in the spliced result.
    let inner = Arc::new(RecordingInner::new(vec![0xEEu8]));
    let fetcher = PeerRangeFetcher::new(
        inner.clone(),
        client,
        peer_a.endpoint.clone(),
        data_dir_path,
        tokio::runtime::Handle::current(),
    );
    let counters = fetcher.counters();

    let key = segment_key(v.ulid, seg_ulid);
    let absolute_start = body_section_start;
    let absolute_end = body_section_start + 8192;
    let bytes =
        tokio::task::spawn_blocking(move || fetcher.get_range(&key, absolute_start, absolute_end))
            .await
            .unwrap()
            .expect("partial peer + inner remainder");

    assert_eq!(bytes.len(), 8192);
    assert_eq!(&bytes[..4096], first.as_slice());
    assert_eq!(&bytes[4096..], vec![0xEEu8; 4096].as_slice());

    let calls = inner.calls();
    assert_eq!(calls.len(), 1, "inner called once for the remainder");
    assert_eq!(calls[0].1, absolute_start + 4096);
    assert_eq!(calls[0].2, absolute_end);

    let snap = counters.snapshot();
    assert_eq!(snap.body_bytes_from_peer, 4096);
    assert_eq!(snap.body_bytes_from_store, 4096);
}

/// Peer A has the volume registered but no segment files cached for
/// the requested ULID. The route returns 404; the client returns
/// `None` so the decorator falls through to the inner store.
#[tokio::test]
async fn peer_404_when_segment_missing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let v = mk_volume(&store).await;

    // No mk_segment call: data_dir is empty for this volume.
    let data_dir = TempDir::new().unwrap();
    let peer_a = spawn_peer(store.clone(), data_dir).await;

    let signer = Arc::new(TestBodySigner {
        vol_ulid: v.ulid,
        key: v.key.clone(),
    });
    let client = BodyFetchClient::new(signer).unwrap();

    let made_up_seg = Ulid::new();
    let result = client
        .fetch_body_range(&peer_a.endpoint, v.ulid, made_up_seg, 0, 4096)
        .await;
    assert!(
        result.is_none(),
        "absent segment must surface as None to the caller"
    );
}

/// Two-coordinator + two-volume negative: peer A holds segment data
/// only for volume V. A *different* volume W is also published in the
/// shared store but has no lineage relationship to V. A request that
/// signs with W's key for a URL referencing V's segment must be
/// rejected by the lineage check and surface as `None`.
///
/// Models the privilege separation guarantee: a coordinator running
/// volume W cannot pull bytes belonging to an unrelated volume V
/// even though both share the same bucket.
#[tokio::test]
async fn unrelated_volume_token_rejected() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let v = mk_volume(&store).await;
    let w = mk_volume(&store).await;

    let payload = vec![0xCDu8; 4096];
    let data_dir = TempDir::new().unwrap();
    let (seg_ulid, _) = mk_segment(data_dir.path(), &v, vec![(payload, true)]);
    let peer_a = spawn_peer(store.clone(), data_dir).await;

    // Sign with W's key, request V's segment. Token decode + signature
    // verify under volume.pub for W succeeds, but the lineage walk
    // (W's ancestry doesn't contain V) rejects with OutsideLineage.
    let bad_signer = Arc::new(TestBodySigner {
        vol_ulid: w.ulid,
        key: w.key.clone(),
    });
    let client = BodyFetchClient::new(bad_signer).unwrap();

    let result = client
        .fetch_body_range(&peer_a.endpoint, v.ulid, seg_ulid, 0, 4096)
        .await;
    assert!(
        result.is_none(),
        "lineage rejection must surface as None (route returns 4xx, client falls through)"
    );
}
