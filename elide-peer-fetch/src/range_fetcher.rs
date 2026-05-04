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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use elide_fetch::RangeFetcher;
use ulid::Ulid;

use crate::client::BodyFetchClient;
use crate::endpoint::PeerEndpoint;

/// Snapshot of body-byte counters split by source. Returned by
/// [`PeerRangeFetcher::counters`].
///
/// `peer` counts only bytes the peer actually returned (full hit and
/// the prefix in a partial-coverage 206). `store` counts only bytes
/// the decorator pulled from the inner fetcher *as part of a body
/// range request* — full miss (peer 404 / network error / token
/// failure) and the remainder of a partial-coverage splice. Inner
/// calls for non-body ranges (header / index / inline reads, or any
/// non-segment key) are not counted here — those bypass the peer
/// path entirely and there's no peer-vs-store decision to attribute.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerFetchCounters {
    pub body_bytes_from_peer: u64,
    pub body_bytes_from_store: u64,
}

/// Atomic counters shared across `PeerRangeFetcher` clones. Cheap to
/// clone — internally an `Arc`.
#[derive(Debug, Clone, Default)]
pub struct PeerFetchCountersHandle {
    inner: Arc<PeerFetchCountersInner>,
}

#[derive(Debug, Default)]
struct PeerFetchCountersInner {
    body_bytes_from_peer: AtomicU64,
    body_bytes_from_store: AtomicU64,
}

impl PeerFetchCountersHandle {
    fn add_peer(&self, n: u64) {
        self.inner
            .body_bytes_from_peer
            .fetch_add(n, Ordering::Relaxed);
    }

    fn add_store(&self, n: u64) {
        self.inner
            .body_bytes_from_store
            .fetch_add(n, Ordering::Relaxed);
    }

    /// Take an atomic snapshot. Counters keep advancing afterward.
    pub fn snapshot(&self) -> PeerFetchCounters {
        PeerFetchCounters {
            body_bytes_from_peer: self.inner.body_bytes_from_peer.load(Ordering::Relaxed),
            body_bytes_from_store: self.inner.body_bytes_from_store.load(Ordering::Relaxed),
        }
    }
}

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
    counters: PeerFetchCountersHandle,
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
            counters: PeerFetchCountersHandle::default(),
        }
    }

    /// Handle for reading the per-decorator body-byte counters. Cheap
    /// to clone; readers see live values without coordinating with
    /// the decorator.
    pub fn counters(&self) -> PeerFetchCountersHandle {
        self.counters.clone()
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
                if let Some(bytes) = bytes {
                    let got = bytes.len() as u64;
                    if got == body_len {
                        self.counters.add_peer(got);
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
                    if got > 0 && got < body_len {
                        // Partial coverage. The peer covered
                        // `[start, start + got)`; ask the inner
                        // fetcher for the remainder and splice. A
                        // failure on the remainder collapses the
                        // whole request to inner — we don't strand
                        // the caller with a partial result and we
                        // don't double-count peer bytes when the
                        // inner fetch fails.
                        let remainder_start = start + got;
                        let remainder =
                            self.inner.get_range(key, remainder_start, end_exclusive)?;
                        self.counters.add_peer(got);
                        self.counters.add_store(remainder.len() as u64);
                        let mut combined = Vec::with_capacity(body_len as usize);
                        combined.extend_from_slice(&bytes);
                        combined.extend_from_slice(&remainder);
                        tracing::trace!(
                            target = "peer-fetch::range",
                            %vol_id,
                            %seg_ulid,
                            body_start,
                            peer_len = got,
                            remainder_len = body_len - got,
                            "peer body range partial; spliced with inner remainder"
                        );
                        return Ok(combined);
                    }
                    // got == 0: nothing useful from peer; fall through.
                }
                // Body-range fall-through (peer miss / partial covers
                // nothing / no token / network error). Attribute the
                // whole inner result to "store" so totals add up to
                // exactly the body bytes the caller requested.
                let bytes = self.inner.get_range(key, start, end_exclusive)?;
                self.counters.add_store(bytes.len() as u64);
                return Ok(bytes);
            }
        }

        // Non-body-range request (header / index / inline / non-segment
        // key). Not attributable to the peer-vs-store split; bypass
        // counters.
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
        SegmentEntry, SegmentFlags, SegmentSigner, promote_to_cache, write_segment,
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
        let body_payload = vec![0xABu8; 4096];
        let f = live_fixture_with(vec![(body_payload.clone(), true)]).await;
        LiveFixture {
            peer: f.peer,
            vol_key: f.vol_key,
            vol_ulid: f.vol_ulid,
            seg_ulid: f.seg_ulid,
            body_payload,
            body_section_start: f.body_section_start,
            data_dir: f.data_dir,
            _server: f._server,
        }
    }

    /// Build a live fixture from a custom list of `(payload, present)`
    /// Data entries. Each payload becomes one Data entry; the
    /// `present` flag controls whether that entry's `.present` bit is
    /// set on disk. Used by the partial-coverage test below to set up
    /// "first entry present, second entry missing" scenarios.
    async fn live_fixture_with(entries_spec: Vec<(Vec<u8>, bool)>) -> LiveFixture {
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

        // Build a multi-Data-entry segment per the spec. Each payload
        // is > INLINE_THRESHOLD so it lands in the body section.
        let mut entries: Vec<SegmentEntry> = entries_spec
            .iter()
            .enumerate()
            .map(|(i, (payload, _))| {
                let hash = blake3::hash(payload);
                SegmentEntry::new_data(hash, i as u64, 1, SegmentFlags::empty(), payload.clone())
            })
            .collect();
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
        // promote_to_cache marks every Data entry as present. Rewrite
        // the bitmap from scratch to honour the per-entry `present`
        // flags in `entries_spec` so partial-coverage cases can be
        // exercised.
        let bitset_len = entries_spec.len().div_ceil(8);
        let mut bitmap = vec![0u8; bitset_len];
        for (idx, (_, present)) in entries_spec.iter().enumerate() {
            if *present {
                bitmap[idx / 8] |= 1 << (idx % 8);
            }
        }
        std::fs::write(&present_path, &bitmap).unwrap();
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
            body_payload: entries_spec
                .first()
                .map(|(p, _)| p.clone())
                .unwrap_or_default(),
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
        let counters = fetcher.counters();

        // Ask for the full body range expressed in absolute segment
        // offsets — what fetch_batch passes today.
        let key = segment_key(f.vol_ulid, f.seg_ulid);
        let absolute_start = f.body_section_start;
        let absolute_end = f.body_section_start + f.body_payload.len() as u64;
        let body_len = f.body_payload.len() as u64;
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

        let snap = counters.snapshot();
        assert_eq!(snap.body_bytes_from_peer, body_len);
        assert_eq!(snap.body_bytes_from_store, 0);
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
        let counters = fetcher.counters();

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

        // Whole body range came from the inner store; counters
        // attribute it to `body_bytes_from_store`.
        let snap = counters.snapshot();
        assert_eq!(snap.body_bytes_from_peer, 0);
        assert_eq!(snap.body_bytes_from_store, 16);
    }

    /// Two Data entries; first is present on the peer, second is
    /// missing. Asking for the full body range gets a 206 partial
    /// covering the first entry; the decorator splices that prefix
    /// with one inner call for the remainder.
    #[tokio::test]
    async fn peer_partial_coverage_splices_with_inner() {
        let first = vec![0xAAu8; 4096];
        let second = vec![0xBBu8; 4096];
        let f = live_fixture_with(vec![(first.clone(), true), (second.clone(), false)]).await;

        let signer = Arc::new(TestBodySigner {
            vol_ulid: f.vol_ulid,
            key: f.vol_key.clone(),
        });
        let client = BodyFetchClient::new(signer).unwrap();
        // Inner returns 0xEE so we can distinguish its bytes from the
        // peer's bytes in the spliced result.
        let inner = Arc::new(RecordingInner::new(vec![0xEEu8]));
        let fetcher = PeerRangeFetcher::new(
            inner.clone(),
            client,
            f.peer.clone(),
            f.data_dir.path().to_owned(),
            tokio::runtime::Handle::current(),
        );
        let counters = fetcher.counters();

        let key = segment_key(f.vol_ulid, f.seg_ulid);
        let absolute_start = f.body_section_start;
        let absolute_end = f.body_section_start + 8192; // both entries
        let bytes = tokio::task::spawn_blocking(move || {
            fetcher.get_range(&key, absolute_start, absolute_end)
        })
        .await
        .unwrap()
        .expect("partial peer + inner remainder");

        assert_eq!(bytes.len(), 8192, "spliced result spans full request");
        assert_eq!(&bytes[..4096], first.as_slice(), "first 4 KiB from peer");
        assert_eq!(
            &bytes[4096..],
            vec![0xEEu8; 4096].as_slice(),
            "trailing 4 KiB from inner"
        );

        let calls = inner.calls();
        assert_eq!(calls.len(), 1, "inner called once for the remainder");
        assert_eq!(
            calls[0].1,
            absolute_start + 4096,
            "remainder starts after the peer-covered prefix"
        );
        assert_eq!(calls[0].2, absolute_end);

        // Splice path attributes the prefix to peer and remainder to
        // store; the two should sum to the body request length.
        let snap = counters.snapshot();
        assert_eq!(snap.body_bytes_from_peer, 4096);
        assert_eq!(snap.body_bytes_from_store, 4096);
    }

    /// Header / non-segment-key reads bypass the peer-vs-store split
    /// and must not register on the body-byte counters.
    #[tokio::test]
    async fn counters_skip_non_body_ranges() {
        let f = live_fixture().await;
        let signer = Arc::new(TestBodySigner {
            vol_ulid: f.vol_ulid,
            key: f.vol_key.clone(),
        });
        let client = BodyFetchClient::new(signer).unwrap();
        let inner = Arc::new(RecordingInner::new(vec![0xAAu8]));
        let fetcher = PeerRangeFetcher::new(
            inner.clone(),
            client,
            f.peer.clone(),
            f.data_dir.path().to_owned(),
            tokio::runtime::Handle::current(),
        );
        let counters = fetcher.counters();

        // Header range (entirely below body_section_start).
        let key = segment_key(f.vol_ulid, f.seg_ulid);
        tokio::task::spawn_blocking({
            let key = key.clone();
            let f = fetcher;
            move || f.get_range(&key, 0, 32)
        })
        .await
        .unwrap()
        .unwrap();

        let snap = counters.snapshot();
        assert_eq!(snap.body_bytes_from_peer, 0);
        assert_eq!(
            snap.body_bytes_from_store, 0,
            "header reads bypass body counters"
        );
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
