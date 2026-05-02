//! HTTP client for fetching segment and snapshot data from peer coordinators.
//!
//! Five operations:
//! - [`PeerFetchClient::fetch_idx`] — full-file fetch of `.idx`.
//! - [`PeerFetchClient::fetch_prefetch_hint`] — full-file fetch of the
//!   advisory `.prefetch` payload (server synthesises from local
//!   `cache/<ulid>.present`; client receives a typed [`PrefetchHint`]).
//! - [`PeerFetchClient::fetch_snapshot_marker`] — full-file fetch of
//!   the bare snapshot marker (empty file under `snapshots/`).
//! - [`PeerFetchClient::fetch_snapshot_manifest`] — full-file fetch of
//!   `snapshots/<snap>.manifest` (signed handoff manifest).
//! - [`PeerFetchClient::fetch_snapshot_filemap`] — full-file fetch of
//!   `snapshots/<snap>.filemap` (per-file fragment table).
//!
//! Failure model: the client treats every non-200 response (404 / 401 /
//! 403 / network error / timeout) as `Ok(None)`. The caller — the
//! prefetch loop — falls through to S3 on miss for the artifact
//! flavours (`.idx`, `.manifest`, `.filemap`, marker) and simply drops
//! the warming hint for `.prefetch` misses. There is intentionally
//! no error type leakage from the peer; the peer is opportunistic, and
//! every failure mode collapses to "ask S3 instead."
//!
//! Connection pooling and per-token reuse:
//! - One [`reqwest::Client`] under the hood, keep-alive enabled,
//!   pool reuse across requests to the same peer endpoint within a
//!   prefetch session.
//! - Tokens are minted lazily and cached per `volume_name` for
//!   `DEFAULT_FRESHNESS_WINDOW_SECS / 2` (= 30 s) before refresh,
//!   so the peer's resolved-Authorized cache stays warm and the
//!   coordinator is comfortably inside the freshness window when
//!   the peer verifies.
//!
//! The signing surface is abstracted via [`TokenSigner`] so this
//! crate doesn't need to depend on `elide-coordinator`. The
//! coordinator's `CoordinatorIdentity` implements `TokenSigner`
//! externally (in the coordinator crate).

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::StatusCode;
use tokio::sync::RwLock;
use tracing::trace;
use ulid::Ulid;

use crate::endpoint::PeerEndpoint;
use crate::hint::PrefetchHint;
use crate::token::{DEFAULT_FRESHNESS_WINDOW_SECS, PeerFetchToken};

/// Trait implemented by anything that can sign peer-fetch tokens
/// with the coordinator's Ed25519 key.
///
/// Decouples this crate from `elide-coordinator::CoordinatorIdentity`
/// (which depends on `elide-peer-fetch` for endpoint publishing —
/// the trait keeps that DAG one-directional).
pub trait TokenSigner: Debug + Send + Sync {
    /// The coordinator id (Crockford-Base32 form) the signer's
    /// pubkey is published under at `coordinators/<id>/coordinator.pub`.
    fn coordinator_id(&self) -> &str;

    /// Ed25519 sign the token's canonical signing payload.
    fn sign(&self, msg: &[u8]) -> [u8; 64];
}

/// HTTP client for the peer-fetch protocol.
///
/// Holds a connection pool, a token signer, and a per-volume token
/// cache. Cheap to clone — internal state is `Arc`-wrapped.
#[derive(Clone)]
pub struct PeerFetchClient {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    signer: Arc<dyn TokenSigner>,
    request_timeout: Duration,
    token_refresh_after: Duration,
    tokens: RwLock<HashMap<String, CachedToken>>,
}

#[derive(Clone)]
struct CachedToken {
    bearer: String,
    refresh_at: Instant,
}

impl Debug for PeerFetchClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerFetchClient")
            .field("signer", &self.inner.signer)
            .field("request_timeout", &self.inner.request_timeout)
            .field("token_refresh_after", &self.inner.token_refresh_after)
            .finish()
    }
}

impl PeerFetchClient {
    /// Build a client with the default request timeout (5 s) and the
    /// default token refresh interval (half the freshness window).
    pub fn new(signer: Arc<dyn TokenSigner>) -> Result<Self, BuildError> {
        Self::builder(signer).build()
    }

    /// Configure a client. See [`PeerFetchClientBuilder`] for tunables.
    pub fn builder(signer: Arc<dyn TokenSigner>) -> PeerFetchClientBuilder {
        PeerFetchClientBuilder {
            signer,
            request_timeout: Duration::from_secs(5),
            token_refresh_after: Duration::from_secs(DEFAULT_FRESHNESS_WINDOW_SECS / 2),
        }
    }

    /// Fetch `<ulid>.idx` from the peer. Returns:
    /// - `Some(bytes)` on 200.
    /// - `None` on any other status, network error, or timeout.
    ///
    /// The caller verifies the segment signature against the
    /// volume's `volume.pub` (same as on the S3 path) before writing
    /// `index/<ulid>.idx`. This client does not attempt verification.
    pub async fn fetch_idx(
        &self,
        peer: &PeerEndpoint,
        volume_name: &str,
        vol_id: Ulid,
        ulid: Ulid,
    ) -> Option<Bytes> {
        let url = format!("{}/v1/{}/{}.idx", peer.url(), vol_id, ulid);
        self.get_bytes(volume_name, &url).await
    }

    /// Fetch `<ulid>.prefetch` from the peer. Returns:
    /// - `Some(hint)` on 200.
    /// - `None` on any other status, network error, or timeout —
    ///   the prefetch loop drops the warming hint for this segment
    ///   and demand-fetches body bytes as guest reads land.
    pub async fn fetch_prefetch_hint(
        &self,
        peer: &PeerEndpoint,
        volume_name: &str,
        vol_id: Ulid,
        ulid: Ulid,
    ) -> Option<PrefetchHint> {
        let url = format!("{}/v1/{}/{}.prefetch", peer.url(), vol_id, ulid);
        self.get_bytes(volume_name, &url)
            .await
            .map(PrefetchHint::from_wire_bytes)
    }

    /// Fetch the bare snapshot marker (`<snap>` with no on-disk suffix;
    /// `<snap>.snapshot` on the wire). Returns the response body — an
    /// empty `Bytes` on success, since the marker file is itself empty.
    /// Caller writes a 0-byte marker locally on `Some(_)`.
    pub async fn fetch_snapshot_marker(
        &self,
        peer: &PeerEndpoint,
        volume_name: &str,
        vol_id: Ulid,
        snap_ulid: Ulid,
    ) -> Option<Bytes> {
        let url = format!("{}/v1/{}/{}.snapshot", peer.url(), vol_id, snap_ulid);
        self.get_bytes(volume_name, &url).await
    }

    /// Fetch `snapshots/<snap>.manifest` (the signed handoff manifest).
    /// The caller is responsible for verifying the signature against
    /// the appropriate volume key before acting on the contents — this
    /// client returns raw bytes, matching the existing S3 path semantics.
    pub async fn fetch_snapshot_manifest(
        &self,
        peer: &PeerEndpoint,
        volume_name: &str,
        vol_id: Ulid,
        snap_ulid: Ulid,
    ) -> Option<Bytes> {
        let url = format!("{}/v1/{}/{}.manifest", peer.url(), vol_id, snap_ulid);
        self.get_bytes(volume_name, &url).await
    }

    /// Fetch `snapshots/<snap>.filemap` (per-file fragment table).
    /// The filemap itself is unsigned; row-level blake3 hashes are
    /// content-verified later when actually reading bytes, so a
    /// tampered filemap can mislead dedup/lookup but cannot let
    /// corrupted bytes through.
    pub async fn fetch_snapshot_filemap(
        &self,
        peer: &PeerEndpoint,
        volume_name: &str,
        vol_id: Ulid,
        snap_ulid: Ulid,
    ) -> Option<Bytes> {
        let url = format!("{}/v1/{}/{}.filemap", peer.url(), vol_id, snap_ulid);
        self.get_bytes(volume_name, &url).await
    }

    async fn get_bytes(&self, volume_name: &str, url: &str) -> Option<Bytes> {
        let bearer = self.token_for(volume_name).await;
        let response = match self
            .inner
            .http
            .get(url)
            .bearer_auth(&bearer)
            .timeout(self.inner.request_timeout)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                trace!(target = "peer-fetch::client", url, error = %e, "request failed");
                return None;
            }
        };

        let status = response.status();
        if status != StatusCode::OK {
            trace!(
                target = "peer-fetch::client",
                url,
                status = status.as_u16(),
                "non-200 response"
            );
            return None;
        }

        match response.bytes().await {
            Ok(b) => Some(b),
            Err(e) => {
                trace!(target = "peer-fetch::client", url, error = %e, "read body failed");
                None
            }
        }
    }

    /// Get a fresh-enough bearer token for `volume_name`, minting a
    /// new one if the cached value has crossed the refresh threshold.
    async fn token_for(&self, volume_name: &str) -> String {
        if let Some(cached) = self.inner.tokens.read().await.get(volume_name).cloned()
            && Instant::now() < cached.refresh_at
        {
            return cached.bearer;
        }
        self.mint_token(volume_name).await
    }

    async fn mint_token(&self, volume_name: &str) -> String {
        let coordinator_id = self.inner.signer.coordinator_id().to_owned();
        let issued_at = PeerFetchToken::now_unix_seconds();
        let payload = PeerFetchToken::signing_payload(volume_name, &coordinator_id, issued_at);
        let signature = self.inner.signer.sign(&payload);
        let token = PeerFetchToken {
            volume_name: volume_name.to_owned(),
            coordinator_id,
            issued_at,
            signature,
        };
        let bearer = token.encode();
        let refresh_at = Instant::now() + self.inner.token_refresh_after;
        self.inner.tokens.write().await.insert(
            volume_name.to_owned(),
            CachedToken {
                bearer: bearer.clone(),
                refresh_at,
            },
        );
        bearer
    }
}

/// Configuration for a [`PeerFetchClient`].
pub struct PeerFetchClientBuilder {
    signer: Arc<dyn TokenSigner>,
    request_timeout: Duration,
    token_refresh_after: Duration,
}

impl PeerFetchClientBuilder {
    /// Per-request timeout. Slow peers fall through to S3 — there's no
    /// retry. Default 5 s.
    pub fn request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }

    /// How long to reuse a minted token before refreshing it.
    /// Should be comfortably less than `DEFAULT_FRESHNESS_WINDOW_SECS`
    /// so requests carrying the cached token are still fresh when they
    /// arrive at the peer. Default is half the freshness window.
    pub fn token_refresh_after(mut self, d: Duration) -> Self {
        self.token_refresh_after = d;
        self
    }

    pub fn build(self) -> Result<PeerFetchClient, BuildError> {
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Some(Duration::from_secs(60)))
            .build()
            .map_err(|e| BuildError(format!("build reqwest client: {e}")))?;
        Ok(PeerFetchClient {
            inner: Arc::new(Inner {
                http,
                signer: self.signer,
                request_timeout: self.request_timeout,
                token_refresh_after: self.token_refresh_after,
                tokens: RwLock::new(HashMap::new()),
            }),
        })
    }
}

/// Error from [`PeerFetchClient::new`] / [`PeerFetchClientBuilder::build`].
/// Only fires if the underlying [`reqwest::Client`] fails to construct
/// — typically a TLS-init issue, which v1 doesn't use.
#[derive(Debug)]
pub struct BuildError(String);

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BuildError {}

// The client tests bind a real TCP listener on 127.0.0.1 and exercise
// the in-process axum router over the loopback. The Claude-Code
// sandbox blocks `bind()`, so these tests fail with `EPERM` when run
// sandboxed; running with the sandbox disabled (or in CI without the
// sandbox) is the supported path. Same convention as the existing
// `nbd::` / `ublk::` namespaces for kernel-dependent tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthState;
    use crate::server::{ServerContext, router};
    use bytes::Bytes;
    use ed25519_dalek::{Signer, SigningKey};
    use elide_core::name_record::{NameRecord, NameState};
    use elide_core::signing::{ProvenanceLineage, write_provenance};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use rand_core::OsRng;
    use std::sync::Mutex;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    /// Test-only signer wrapping a `SigningKey`. Counts how many times
    /// it has signed, so tests can assert token reuse.
    #[derive(Debug)]
    struct TestSigner {
        key: SigningKey,
        coord_id: String,
        sign_count: Mutex<usize>,
    }

    impl TestSigner {
        fn new(coord_id: &str, key: SigningKey) -> Self {
            Self {
                key,
                coord_id: coord_id.to_owned(),
                sign_count: Mutex::new(0),
            }
        }

        fn sign_count(&self) -> usize {
            *self.sign_count.lock().unwrap()
        }
    }

    impl TokenSigner for TestSigner {
        fn coordinator_id(&self) -> &str {
            &self.coord_id
        }

        fn sign(&self, msg: &[u8]) -> [u8; 64] {
            *self.sign_count.lock().unwrap() += 1;
            self.key.sign(msg).to_bytes()
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

    /// Fully-wired test environment: an in-memory ObjectStore, a
    /// tempdir data_dir, a coordinator, a volume + name record, and
    /// a real bound TCP listener serving the peer-fetch routes.
    struct LiveFixture {
        peer: PeerEndpoint,
        coord_key: Arc<TestSigner>,
        coord_id: String,
        vol_ulid: Ulid,
        vol_name: String,
        data_dir: TempDir,
        _server_handle: tokio::task::JoinHandle<()>,
    }

    async fn start_server() -> LiveFixture {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let auth = AuthState::new(store.clone());
        let data_dir = TempDir::new().unwrap();

        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a".to_owned();
        let vol_name = "myvol".to_owned();
        let vol_ulid = Ulid::new();

        // Publish coordinator.pub
        store
            .put(
                &StorePath::from(format!("coordinators/{coord_id}/coordinator.pub")),
                Bytes::from(pub_hex(&coord_key).into_bytes()).into(),
            )
            .await
            .unwrap();

        // Publish volume.pub + provenance
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

        // Publish names/<vol_name>
        let mut record = NameRecord::live_minimal(vol_ulid);
        record.coordinator_id = Some(coord_id.clone());
        record.state = NameState::Live;
        store
            .put(
                &StorePath::from(format!("names/{vol_name}")),
                Bytes::from(record.to_toml().unwrap().into_bytes()).into(),
            )
            .await
            .unwrap();

        // Bind a real TCP listener on an ephemeral port and spawn the
        // axum service.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let ctx = ServerContext::new(auth, data_dir.path().to_owned());
        let app = router(ctx);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let peer = PeerEndpoint::new(local_addr.ip().to_string(), local_addr.port());
        let signer = Arc::new(TestSigner::new(&coord_id, coord_key.clone()));

        LiveFixture {
            peer,
            coord_key: signer,
            coord_id,
            vol_ulid,
            vol_name,
            data_dir,
            _server_handle: handle,
        }
    }

    fn write_local_file(
        data_dir: &std::path::Path,
        vol_ulid: Ulid,
        subdir: &str,
        filename: &str,
        body: &[u8],
    ) {
        let dir = data_dir
            .join("by_id")
            .join(vol_ulid.to_string())
            .join(subdir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(filename), body).unwrap();
    }

    #[tokio::test]
    async fn fetch_idx_returns_local_bytes() {
        let f = start_server().await;
        let segment_ulid = Ulid::new();
        let body = b"the quick brown fox jumps over the lazy dog\n";
        write_local_file(
            f.data_dir.path(),
            f.vol_ulid,
            "index",
            &format!("{segment_ulid}.idx"),
            body,
        );

        let client = PeerFetchClient::new(f.coord_key.clone()).unwrap();
        let result = client
            .fetch_idx(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
            .await;

        assert_eq!(result.as_deref(), Some(body.as_ref()));
    }

    #[tokio::test]
    async fn fetch_prefetch_returns_typed_hint() {
        let f = start_server().await;
        let segment_ulid = Ulid::new();
        // Bits 1, 3, 5 set within the first byte.
        let body = vec![0b0010_1010];
        write_local_file(
            f.data_dir.path(),
            f.vol_ulid,
            "cache",
            &format!("{segment_ulid}.present"),
            &body,
        );

        let client = PeerFetchClient::new(f.coord_key.clone()).unwrap();
        let hint = client
            .fetch_prefetch_hint(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
            .await
            .expect("hint present");

        assert_eq!(hint.payload_len(), 1);
        let entries: Vec<_> = hint.iter_populated_entries(8).collect();
        assert_eq!(entries, vec![1, 3, 5]);
    }

    #[tokio::test]
    async fn missing_local_file_returns_none() {
        let f = start_server().await;
        let segment_ulid = Ulid::new(); // not written to disk

        let client = PeerFetchClient::new(f.coord_key.clone()).unwrap();
        let result = client
            .fetch_idx(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
            .await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn auth_failure_returns_none() {
        let f = start_server().await;
        let segment_ulid = Ulid::new();
        let body = b"x";
        write_local_file(
            f.data_dir.path(),
            f.vol_ulid,
            "index",
            &format!("{segment_ulid}.idx"),
            body,
        );

        // Sign with a *different* key; the peer's coordinator.pub
        // verification will reject.
        let imposter = SigningKey::generate(&mut OsRng);
        let bad_signer = Arc::new(TestSigner::new(&f.coord_id, imposter));
        let client = PeerFetchClient::new(bad_signer).unwrap();

        let result = client
            .fetch_idx(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn token_is_reused_across_requests_to_same_volume() {
        let f = start_server().await;
        let signer = f.coord_key.clone();
        let client = PeerFetchClient::new(signer.clone()).unwrap();

        // Make a few requests for different ulids on the same volume —
        // all should reuse the same minted token.
        for _ in 0..5 {
            let segment_ulid = Ulid::new();
            let _ = client
                .fetch_idx(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
                .await;
        }

        assert_eq!(
            signer.sign_count(),
            1,
            "expected exactly one token mint shared across all requests"
        );
    }

    #[tokio::test]
    async fn token_is_reused_across_idx_and_prefetch_routes() {
        let f = start_server().await;
        let signer = f.coord_key.clone();
        let client = PeerFetchClient::new(signer.clone()).unwrap();
        let segment_ulid = Ulid::new();

        client
            .fetch_idx(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
            .await;
        client
            .fetch_prefetch_hint(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
            .await;

        // Same volume_name across two route flavours → one token.
        assert_eq!(signer.sign_count(), 1);
    }

    #[tokio::test]
    async fn token_refresh_after_zero_mints_fresh_on_every_call() {
        let f = start_server().await;
        let signer = f.coord_key.clone();
        let client = PeerFetchClient::builder(signer.clone())
            .token_refresh_after(Duration::from_secs(0))
            .build()
            .unwrap();
        let segment_ulid = Ulid::new();

        for _ in 0..3 {
            let _ = client
                .fetch_idx(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
                .await;
        }

        assert_eq!(signer.sign_count(), 3);
    }

    #[tokio::test]
    async fn distinct_volumes_get_distinct_tokens() {
        // Two volumes ⇒ two distinct cache entries ⇒ two mints, even
        // if all requests go to the same peer.
        let f = start_server().await;
        let signer = f.coord_key.clone();
        let client = PeerFetchClient::new(signer.clone()).unwrap();
        let segment_ulid = Ulid::new();

        let _ = client
            .fetch_idx(&f.peer, &f.vol_name, f.vol_ulid, segment_ulid)
            .await;
        let _ = client
            .fetch_idx(&f.peer, "some-other-volume", f.vol_ulid, segment_ulid)
            .await;

        assert_eq!(signer.sign_count(), 2);
    }

    #[tokio::test]
    async fn unreachable_peer_returns_none_does_not_panic() {
        let unreachable = PeerEndpoint::new("127.0.0.1".to_owned(), 1); // port 1 → ECONNREFUSED
        let coord_key = SigningKey::generate(&mut OsRng);
        let signer = Arc::new(TestSigner::new("coord-a", coord_key));
        let client = PeerFetchClient::builder(signer)
            .request_timeout(Duration::from_millis(200))
            .build()
            .unwrap();

        let result = client
            .fetch_idx(&unreachable, "vol", Ulid::new(), Ulid::new())
            .await;
        assert!(result.is_none());
    }
}
