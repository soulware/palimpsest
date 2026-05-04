//! `PeerRangeFetcher` — `elide_fetch::RangeFetcher` decorator that
//! consults a peer-fetch server for body bytes before falling through
//! to the inner (S3 or local-store) fetcher.
//!
//! The wrapper is opt-in: the volume daemon constructs it when a
//! `PeerEndpoint` is available (handed across the volume-start IPC),
//! and otherwise plugs the bare inner fetcher into `RemoteFetcher`
//! exactly like today. Peer 404 / 401 / 403 / network error / partial
//! coverage all collapse to "ask the inner store" — same opportunistic
//! shape as the index-class peer-fetch routes.
//!
//! ### Range translation
//!
//! `RangeFetcher::get_range` is keyed by the S3 object key
//! (`by_id/<vol>/segments/<date>/<seg>`) and absolute byte offsets
//! within the segment file. The peer-fetch `.body` route serves only
//! the body section, with body-relative offsets. The decorator
//! translates by reading `body_section_start` from the local
//! `index/<seg>.idx` (which the volume always has — `.idx` is the
//! authoritative segment header in this filesystem layout). The
//! result is cached per segment ULID; body layout is invariant once
//! a segment is written.
//!
//! Ranges that aren't entirely inside the body section (header /
//! index / inline reads) fall through to the inner fetcher
//! unconditionally — the peer doesn't serve those.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use elide_fetch::RangeFetcher;
use ulid::Ulid;

use crate::client::BodyFetchClient;
use crate::endpoint::PeerEndpoint;

/// Decorator that tries a peer-fetch server for body byte ranges
/// before falling through to `inner`.
///
/// Construct with [`PeerRangeFetcher::new`]. Cheap to clone
/// (internal state is `Arc`-wrapped).
pub struct PeerRangeFetcher {
    inner: Arc<dyn RangeFetcher>,
    client: BodyFetchClient,
    endpoint: PeerEndpoint,
    /// Root for resolving local `.idx` files: same layout the volume
    /// process already manages, i.e. `<data_dir>/by_id/<vol>/index/<seg>.idx`.
    data_dir: PathBuf,
    body_section_start_cache: Arc<Mutex<HashMap<Ulid, u64>>>,
    runtime: tokio::runtime::Handle,
}

impl PeerRangeFetcher {
    /// Build a decorator with an explicit tokio runtime handle.
    ///
    /// `runtime` must outlive every `get_range` call: the decorator
    /// uses it to drive the async peer client from the sync call site.
    /// Callers in long-running daemons typically leak a multi-thread
    /// runtime at process startup and pass its `Handle` here.
    pub fn new(
        inner: Arc<dyn RangeFetcher>,
        client: BodyFetchClient,
        endpoint: PeerEndpoint,
        data_dir: PathBuf,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            inner,
            client,
            endpoint,
            data_dir,
            body_section_start_cache: Arc::new(Mutex::new(HashMap::new())),
            runtime,
        }
    }

    /// Resolve the body-section start offset for `seg_ulid`, reading
    /// the local `.idx` on first call and memoising the result.
    fn body_section_start(&self, vol_id: Ulid, seg_ulid: Ulid) -> io::Result<u64> {
        if let Some(&start) = self
            .body_section_start_cache
            .lock()
            .expect("body_section_start cache mutex")
            .get(&seg_ulid)
        {
            return Ok(start);
        }

        let idx_path = self
            .data_dir
            .join("by_id")
            .join(vol_id.to_string())
            .join("index")
            .join(format!("{seg_ulid}.idx"));
        let layout = elide_core::segment::read_segment_layout(&idx_path)?;
        let start = layout.body_section_start;
        self.body_section_start_cache
            .lock()
            .expect("body_section_start cache mutex")
            .insert(seg_ulid, start);
        Ok(start)
    }
}

impl RangeFetcher for PeerRangeFetcher {
    fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> io::Result<Vec<u8>> {
        // The peer only serves body ranges. Everything else falls
        // through unconditionally.
        let parsed = parse_segment_key(key);
        if let Some((vol_id, seg_ulid)) = parsed {
            // Resolve body_section_start. A failure here (idx missing,
            // bad magic) is non-fatal for peer-fetch purposes — fall
            // through to the inner fetcher.
            if let Ok(body_section_start) = self.body_section_start(vol_id, seg_ulid)
                && start >= body_section_start
            {
                let body_start = start - body_section_start;
                let body_len = end_exclusive - start;
                let client = self.client.clone();
                let endpoint = self.endpoint.clone();
                let bytes = self.runtime.block_on(async move {
                    client
                        .fetch_body_range(&endpoint, vol_id, seg_ulid, body_start, body_len)
                        .await
                });
                if let Some(bytes) = bytes
                    && bytes.len() as u64 == body_len
                {
                    tracing::trace!(
                        target = "peer-fetch::range",
                        %vol_id,
                        %seg_ulid,
                        body_start,
                        body_len,
                        "peer body range served"
                    );
                    return Ok(bytes.to_vec());
                }
            }
        }

        self.inner.get_range(key, start, end_exclusive)
    }
}

/// Parse `by_id/<vol_id>/segments/<date>/<seg_ulid>` into
/// `(vol_id, seg_ulid)`. Returns `None` for any other key shape
/// (the caller falls through to the inner fetcher unchanged).
fn parse_segment_key(key: &str) -> Option<(Ulid, Ulid)> {
    let mut parts = key.split('/');
    if parts.next()? != "by_id" {
        return None;
    }
    let vol_str = parts.next()?;
    if parts.next()? != "segments" {
        return None;
    }
    let _date = parts.next()?;
    let seg_str = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let vol_id = Ulid::from_string(vol_str).ok()?;
    let seg_ulid = Ulid::from_string(seg_str).ok()?;
    Some((vol_id, seg_ulid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthState;
    use crate::client::BodyTokenSigner;
    use crate::server::{ServerContext, router};
    use bytes::Bytes;
    use ed25519_dalek::{Signer, SigningKey};
    use elide_core::segment::{
        EntryKind as SegEntryKind, SegmentEntry, SegmentFlags, SegmentSigner, promote_to_cache,
        set_present_bit, write_segment,
    };
    use elide_core::signing::{ProvenanceLineage, write_provenance};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use rand_core::OsRng;
    use std::sync::Mutex as StdMutex;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    /// Minimal SegmentSigner wrapping a `SigningKey` (the elide-core
    /// `Ed25519Signer` constructor is private).
    struct TestSegSigner(SigningKey);
    impl SegmentSigner for TestSegSigner {
        fn sign(&self, msg: &[u8]) -> [u8; 64] {
            self.0.sign(msg).to_bytes()
        }
    }

    /// Minimal BodyTokenSigner backed by a SigningKey.
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

    /// Sync `RangeFetcher` mock that records calls and returns
    /// canned bytes. Used to assert peer-first behaviour: a peer hit
    /// must NOT call into the inner fetcher.
    struct RecordingInner {
        calls: StdMutex<Vec<(String, u64, u64)>>,
        fallback_bytes: Vec<u8>,
    }
    impl RecordingInner {
        fn new(fallback_bytes: Vec<u8>) -> Self {
            Self {
                calls: StdMutex::new(Vec::new()),
                fallback_bytes,
            }
        }
        fn calls(&self) -> Vec<(String, u64, u64)> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl RangeFetcher for RecordingInner {
        fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> io::Result<Vec<u8>> {
            self.calls
                .lock()
                .unwrap()
                .push((key.to_owned(), start, end_exclusive));
            let len = (end_exclusive - start) as usize;
            Ok(self
                .fallback_bytes
                .iter()
                .cycle()
                .take(len)
                .copied()
                .collect())
        }
    }

    fn pub_hex(key: &SigningKey) -> String {
        let bytes = key.verifying_key().to_bytes();
        let mut s = String::with_capacity(64);
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s.push('\n');
        s
    }

    /// Live fixture: in-memory ObjectStore with the volume's
    /// volume.pub published, a real bound TCP listener serving the
    /// peer-fetch axum router, and a `data_dir` populated with one
    /// segment's `.idx` + `.body` + `.present` files.
    struct LiveFixture {
        peer: PeerEndpoint,
        vol_key: SigningKey,
        vol_ulid: Ulid,
        seg_ulid: Ulid,
        body_payload: Vec<u8>,
        body_section_start: u64,
        data_dir: TempDir,
        _server: tokio::task::JoinHandle<()>,
    }

    async fn live_fixture() -> LiveFixture {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let auth = AuthState::new(store.clone());
        let data_dir = TempDir::new().unwrap();
        let vol_key = SigningKey::generate(&mut OsRng);
        let vol_ulid = Ulid::new();
        let seg_ulid = Ulid::new();

        // Publish volume.pub + provenance.
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("volume.pub"), pub_hex(&vol_key)).unwrap();
        write_provenance(
            tmp.path(),
            &vol_key,
            "volume.provenance",
            &ProvenanceLineage::default(),
        )
        .unwrap();
        let pub_bytes = std::fs::read(tmp.path().join("volume.pub")).unwrap();
        let prov_bytes = std::fs::read(tmp.path().join("volume.provenance")).unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{vol_ulid}/volume.pub")),
                Bytes::from(pub_bytes).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{vol_ulid}/volume.provenance")),
                Bytes::from(prov_bytes).into(),
            )
            .await
            .unwrap();

        // Build the segment locally. One Data entry, > INLINE_THRESHOLD
        // so it lands in the body section.
        let body_payload = vec![0xABu8; 4096];
        let hash = blake3::hash(&body_payload);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            body_payload.clone(),
        )];
        let staging = data_dir.path().join("staging");
        let signer = TestSegSigner(vol_key.clone());
        write_segment(&staging, &mut entries, &signer).unwrap();

        // Lay out cache + index files under data_dir/by_id/<vol>/.
        let by_id = data_dir.path().join("by_id").join(vol_ulid.to_string());
        let cache = by_id.join("cache");
        let index = by_id.join("index");
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::create_dir_all(&index).unwrap();
        let body_path = cache.join(format!("{seg_ulid}.body"));
        let present_path = cache.join(format!("{seg_ulid}.present"));
        promote_to_cache(&staging, &body_path, &present_path).unwrap();
        for (idx, entry) in entries.iter().enumerate() {
            if entry.kind == SegEntryKind::Data {
                set_present_bit(&present_path, idx as u32, entries.len() as u32).unwrap();
            }
        }
        // Slice the staged segment file into <seg>.idx.
        let seg_bytes = std::fs::read(&staging).unwrap();
        let index_length =
            u32::from_le_bytes([seg_bytes[12], seg_bytes[13], seg_bytes[14], seg_bytes[15]]);
        let inline_length =
            u32::from_le_bytes([seg_bytes[16], seg_bytes[17], seg_bytes[18], seg_bytes[19]]);
        let body_section_start = 100u64 + index_length as u64 + inline_length as u64;
        let idx_path = index.join(format!("{seg_ulid}.idx"));
        std::fs::write(&idx_path, &seg_bytes[..body_section_start as usize]).unwrap();

        // Spin up the peer-fetch server on an ephemeral port.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let ctx = ServerContext::new(auth, data_dir.path().to_owned());
        let app = router(ctx);
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let peer = PeerEndpoint::new(local_addr.ip().to_string(), local_addr.port());

        LiveFixture {
            peer,
            vol_key,
            vol_ulid,
            seg_ulid,
            body_payload,
            body_section_start,
            data_dir,
            _server: server,
        }
    }

    fn segment_key(vol_ulid: Ulid, seg_ulid: Ulid) -> String {
        // `body_section_start` of the .body route is invariant per
        // segment, so the date subdir doesn't matter for the peer
        // route — only the inner fetcher cares about it. Use any
        // valid YYYYMMDD here.
        format!("by_id/{vol_ulid}/segments/19700101/{seg_ulid}")
    }

    #[tokio::test]
    async fn peer_hit_returns_body_bytes_and_skips_inner() {
        let f = live_fixture().await;
        let signer = Arc::new(TestBodySigner {
            vol_ulid: f.vol_ulid,
            key: f.vol_key.clone(),
        });
        let client = BodyFetchClient::new(signer).unwrap();
        let inner = Arc::new(RecordingInner::new(vec![0xFFu8]));
        let fetcher = PeerRangeFetcher::new(
            inner.clone(),
            client,
            f.peer.clone(),
            f.data_dir.path().to_owned(),
            tokio::runtime::Handle::current(),
        );

        // Ask for the full body range expressed in absolute segment
        // offsets — what fetch_batch passes today.
        let key = segment_key(f.vol_ulid, f.seg_ulid);
        let absolute_start = f.body_section_start;
        let absolute_end = f.body_section_start + f.body_payload.len() as u64;
        let bytes = tokio::task::spawn_blocking(move || {
            fetcher.get_range(&key, absolute_start, absolute_end)
        })
        .await
        .unwrap()
        .expect("peer-served body bytes");

        assert_eq!(bytes, f.body_payload);
        assert!(
            inner.calls().is_empty(),
            "inner fetcher must not be called on peer hit; got {:?}",
            inner.calls()
        );
    }

    #[tokio::test]
    async fn peer_miss_falls_through_to_inner() {
        let f = live_fixture().await;
        // Drop the body file so the peer 404s on coverage check.
        let body_path = f
            .data_dir
            .path()
            .join("by_id")
            .join(f.vol_ulid.to_string())
            .join("cache")
            .join(format!("{}.body", f.seg_ulid));
        std::fs::remove_file(&body_path).unwrap();

        let signer = Arc::new(TestBodySigner {
            vol_ulid: f.vol_ulid,
            key: f.vol_key.clone(),
        });
        let client = BodyFetchClient::new(signer).unwrap();
        let inner = Arc::new(RecordingInner::new(vec![0xCDu8]));
        let fetcher = PeerRangeFetcher::new(
            inner.clone(),
            client,
            f.peer.clone(),
            f.data_dir.path().to_owned(),
            tokio::runtime::Handle::current(),
        );

        let key = segment_key(f.vol_ulid, f.seg_ulid);
        let absolute_start = f.body_section_start;
        let absolute_end = f.body_section_start + 16;
        let bytes = tokio::task::spawn_blocking(move || {
            fetcher.get_range(&key, absolute_start, absolute_end)
        })
        .await
        .unwrap()
        .expect("inner fallback");

        assert_eq!(bytes, vec![0xCDu8; 16], "should be inner's canned bytes");
        let calls = inner.calls();
        assert_eq!(calls.len(), 1, "inner must be called exactly once");
        assert_eq!(calls[0].1, absolute_start);
        assert_eq!(calls[0].2, absolute_end);
    }

    /// Ranges that aren't entirely inside the body section (e.g.
    /// header / index / inline reads) bypass the peer entirely and
    /// go to the inner fetcher.
    #[tokio::test]
    async fn header_range_skips_peer() {
        let f = live_fixture().await;
        let signer = Arc::new(TestBodySigner {
            vol_ulid: f.vol_ulid,
            key: f.vol_key.clone(),
        });
        let client = BodyFetchClient::new(signer).unwrap();
        let inner = Arc::new(RecordingInner::new(vec![0xAA]));
        let fetcher = PeerRangeFetcher::new(
            inner.clone(),
            client,
            f.peer.clone(),
            f.data_dir.path().to_owned(),
            tokio::runtime::Handle::current(),
        );

        let key = segment_key(f.vol_ulid, f.seg_ulid);
        // Range entirely inside the header (offset < body_section_start).
        let bytes = tokio::task::spawn_blocking(move || fetcher.get_range(&key, 0, 96))
            .await
            .unwrap()
            .expect("inner serves header");

        assert_eq!(bytes.len(), 96);
        assert_eq!(inner.calls().len(), 1, "header reads must skip peer");
    }

    /// A non-segment key (e.g. a snapshot manifest object) routes
    /// through the inner unchanged — peer-fetch is body-only.
    #[tokio::test]
    async fn non_segment_key_passes_through() {
        let f = live_fixture().await;
        let signer = Arc::new(TestBodySigner {
            vol_ulid: f.vol_ulid,
            key: f.vol_key.clone(),
        });
        let client = BodyFetchClient::new(signer).unwrap();
        let inner = Arc::new(RecordingInner::new(vec![0x55]));
        let fetcher = PeerRangeFetcher::new(
            inner.clone(),
            client,
            f.peer.clone(),
            f.data_dir.path().to_owned(),
            tokio::runtime::Handle::current(),
        );

        let key = "by_id/01ABCDEFGHJKMNPQRSTVWXYZ00/snapshots/01XYZ.manifest";
        let bytes = tokio::task::spawn_blocking(move || fetcher.get_range(key, 0, 4))
            .await
            .unwrap();
        let _ = bytes.expect("inner serves non-segment keys");
        assert_eq!(inner.calls().len(), 1);
    }

    #[test]
    fn parse_segment_key_happy_path() {
        let vol = Ulid::new();
        let seg = Ulid::new();
        let key = format!("by_id/{vol}/segments/19700101/{seg}");
        assert_eq!(parse_segment_key(&key), Some((vol, seg)));
    }

    #[test]
    fn parse_segment_key_rejects_extra_components() {
        let vol = Ulid::new();
        let seg = Ulid::new();
        let key = format!("by_id/{vol}/segments/19700101/{seg}/extra");
        assert!(parse_segment_key(&key).is_none());
    }

    #[test]
    fn parse_segment_key_rejects_other_subdirs() {
        let vol = Ulid::new();
        let seg = Ulid::new();
        let key = format!("by_id/{vol}/snapshots/{seg}.manifest");
        assert!(parse_segment_key(&key).is_none());
    }

    #[test]
    fn parse_segment_key_rejects_bad_ulids() {
        assert!(parse_segment_key("by_id/not-a-ulid/segments/19700101/01ABC").is_none());
        let vol = Ulid::new();
        assert!(parse_segment_key(&format!("by_id/{vol}/segments/19700101/not-a-ulid")).is_none());
    }
}
