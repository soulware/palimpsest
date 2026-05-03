//! Five-step verify pipeline for incoming peer-fetch requests.
//!
//! See `docs/design-peer-segment-fetch.md` § "Peer verification (v1)"
//! for the full design. Each [`AuthError`] variant corresponds to one
//! failed check and maps to a specific HTTP status code:
//!
//! | Step | Check                           | On failure |
//! |------|---------------------------------|-----------|
//! | 1    | Token decode + freshness        | 401        |
//! | 2    | Ed25519 signature               | 401        |
//! | 2.5  | Rate limiter ([`RateLimiter`])  | 429        |
//! | 3    | Volume claimed by this coord    | 401        |
//! | 4    | URL `vol_id` is in ancestry     | 403        |
//! | 5    | Local file exists (route-level) | 404        |
//!
//! Step 4 only runs under [`RouteAuthMode::LineageGated`]. Skeleton
//! routes (`volume.pub`, `volume.provenance`) request
//! [`RouteAuthMode::SkeletonsOnly`] and skip step 4 — see that
//! enum's documentation for the security analysis. Step 5 is the
//! route handler's responsibility — it's a stat that falls out as
//! 404 if the file isn't present locally.
//!
//! ### Caching profile
//!
//! Three layers of cache, each tightly scoped:
//!
//! - **`coordinator.pub`** — immutable per `coordinator_id`. Cached
//!   forever after first fetch.
//!
//! - **Ancestry walk** — immutable per `vol_ulid` (provenance is
//!   write-once at fork time). Cached forever after first walk.
//!
//! - **`names/<volume_name>`** — ETag-conditional. The cache holds
//!   `(NameRecord, ETag)`; every request fires a conditional GET with
//!   `If-None-Match`. A 304 response confirms the cached value is
//!   still fresh; a 200 returns the new value (and updates the cache).
//!   This keeps the auth fence coincident with the S3 CAS — the
//!   moment a new claim lands, the next request sees it via the 200
//!   response.
//!
//! On top of that, [`AuthState`] memoises the *resolved* [`Authorized`]
//! result keyed on the bearer token + URL `vol_id` for the lifetime
//! of the token's freshness window. Within the cache window,
//! repeated requests skip steps 3 and 4 entirely — no S3 lookups for
//! `names/<name>` (not even the conditional one) and no ancestry
//! cache touch. Cache lifetime is capped at the token's residual
//! freshness so an entry can never authorise past the moment the
//! token would itself become stale; a refreshed token (any
//! coordinator re-mints in steady state every freshness-window
//! interval) is a fresh cache miss and re-runs the full pipeline.
//!
//! This composition gives most of the benefit of per-connection
//! memoisation (which the design doc anticipated as the natural future
//! optimisation) without binding to the HTTP/2 connection abstraction:
//! a typical prefetch session reuses one token throughout, which is
//! exactly when the cache hits.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::VerifyingKey;
use elide_core::name_record::{NameRecord, NameState};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use object_store::{Error as ObjectStoreError, GetOptions};
use tokio::sync::RwLock;
use ulid::Ulid;

use crate::ancestry::walk_ancestry;
use crate::token::{
    DEFAULT_FRESHNESS_WINDOW_SECS, PeerFetchToken, TokenDecodeError, TokenVerifyError,
};

/// Outcome of a failed authorisation check. Each variant maps to a
/// specific HTTP status code via [`AuthError::status_code`].
#[derive(Debug)]
pub enum AuthError {
    /// `Authorization` header missing or not a `Bearer <token>`.
    MissingBearer,
    /// Token wire form did not decode.
    BadToken(TokenDecodeError),
    /// Token clock skew exceeds the freshness window, or signature
    /// failed to verify against the coordinator's published pubkey.
    BadCredentials(TokenVerifyError),
    /// Volume name in the token does not currently resolve to the
    /// requesting coordinator (state isn't `Live` / `Stopped`, or
    /// `coordinator_id` doesn't match).
    NotCurrentClaimer,
    /// URL's `vol_id` is not in the requesting volume's signed
    /// fork-parent ancestry.
    OutsideLineage,
    /// Rate limiter rejected the request. Carries a static reason
    /// string from the limiter for log/operator surface.
    RateLimited(&'static str),
    /// Wrapper for any underlying S3 / parsing error encountered while
    /// resolving the auth pipeline.
    Backend(io::Error),
}

impl AuthError {
    /// HTTP status code this error maps to in the response.
    pub fn status_code(&self) -> u16 {
        match self {
            Self::MissingBearer => 401,
            Self::BadToken(_) => 401,
            Self::BadCredentials(_) => 401,
            Self::NotCurrentClaimer => 401,
            Self::OutsideLineage => 403,
            Self::RateLimited(_) => 429,
            Self::Backend(_) => 502,
        }
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingBearer => f.write_str("missing or malformed Authorization header"),
            Self::BadToken(e) => write!(f, "token decode failed: {e}"),
            Self::BadCredentials(e) => write!(f, "credentials rejected: {e}"),
            Self::NotCurrentClaimer => f.write_str("token holder is not the current claimer"),
            Self::OutsideLineage => {
                f.write_str("requested vol_id is outside claimed volume's lineage")
            }
            Self::RateLimited(reason) => write!(f, "rate limited: {reason}"),
            Self::Backend(e) => write!(f, "auth backend error: {e}"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Successful auth result. Carries the resolved fields the route
/// handler may want to log or use; held in request extensions if used
/// as middleware.
#[derive(Debug, Clone)]
pub struct Authorized {
    pub volume_name: String,
    pub coordinator_id: String,
    pub vol_id: Ulid,
}

/// Shared auth state: object store handle plus the immutable caches
/// (`coordinator.pub`, ancestry).
///
/// Cheap to clone — internally `Arc`-wrapped.
#[derive(Clone)]
pub struct AuthState {
    inner: Arc<AuthStateInner>,
}

struct AuthStateInner {
    store: Arc<dyn ObjectStore>,
    pub_keys: RwLock<HashMap<String, VerifyingKey>>,
    ancestry_cache: RwLock<HashMap<Ulid, HashSet<Ulid>>>,
    /// `(NameRecord, ETag)` per volume name. Revalidated via
    /// `If-None-Match` on every request that reaches step 3.
    name_records: RwLock<HashMap<String, (NameRecord, Option<String>)>>,
    /// Resolved `Authorized` outcomes keyed on `(bearer_token, vol_id)`.
    /// Entries expire at the token's residual freshness window so a
    /// cached result never outlives the token's own freshness.
    verified_tokens: RwLock<HashMap<VerifiedKey, VerifiedEntry>>,
    freshness_window_secs: u64,
    rate_limiter: Arc<dyn RateLimiter>,
}

/// Hook for per-token rate limiting on peer-fetch routes. Currently
/// a placeholder — the no-op default impl in [`NoRateLimit`] always
/// permits — so the surface lands now and an actual limiter can plug
/// in later without touching call sites. The skeleton routes are the
/// most likely to need rate limiting in practice (any authenticated
/// coordinator can hit them post-relaxation, see [`RouteAuthMode`]),
/// so the hook is consulted before the lineage walk, after token
/// signature verification.
///
/// Implementations should be cheap to clone; `AuthState` stores them
/// in an `Arc<dyn>`.
pub trait RateLimiter: Send + Sync {
    /// Return `Ok(())` to permit the request, `Err(reason)` to reject
    /// with a 429-ish error wrapped in [`AuthError::RateLimited`].
    /// `volume_name` is the token-claim (the requester's own volume),
    /// `mode` allows policy to differ between skeleton and payload
    /// surfaces. The `vol_id` of the URL is intentionally not passed
    /// — limiters that need it can be added later if a use case
    /// emerges.
    fn check(
        &self,
        coordinator_id: &str,
        volume_name: &str,
        mode: RouteAuthMode,
    ) -> Result<(), &'static str>;
}

/// No-op rate limiter; always permits. Used as the default until a
/// real limiter is plumbed in.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRateLimit;

impl RateLimiter for NoRateLimit {
    fn check(
        &self,
        _coordinator_id: &str,
        _volume_name: &str,
        _mode: RouteAuthMode,
    ) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Auth mode declared by the route registration. Skeleton routes
/// (`volume.pub`, `volume.provenance`) skip the lineage walk so a
/// claim-time chain pull can authenticate before the requesting
/// fork's own provenance is published. Payload routes (`.idx`,
/// `.manifest`) keep the full pipeline.
///
/// The check that the lineage gate enforces is *intent scoping* —
/// "this requester actually has business with the URL's vol_id" —
/// not confidentiality. Within a bucket every authenticated
/// coordinator already has S3 read access to every key. Skeleton
/// metadata (a 32-byte verifying key + a signed lineage record) is
/// already broadly readable from S3; relaxing the peer-side gate
/// for those routes mirrors that reality and lets peer-fetch warm
/// chain walks during the early-rebind phase of `volume claim`,
/// when the requester's own `volume.provenance` hasn't been signed
/// yet (parent isn't known until handoff verification).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouteAuthMode {
    /// Steps 1-3 only: token integrity + freshness + signature +
    /// `names/<volume>` ownership. No lineage walk.
    SkeletonsOnly,
    /// Full pipeline (steps 1-4): adds the ancestry walk.
    LineageGated,
}

/// Cache key for the resolved-Authorized cache. Bound to bearer +
/// vol_id + mode: the same token validating against the same vol_id
/// resolves *differently* across modes (skeleton mode skips the
/// lineage walk), so we mustn't serve a cached `SkeletonsOnly` hit
/// to a `LineageGated` request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VerifiedKey {
    bearer: String,
    vol_id: Ulid,
    mode: RouteAuthMode,
}

#[derive(Debug, Clone)]
struct VerifiedEntry {
    authorized: Authorized,
    expires_at: Instant,
}

impl AuthState {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::with_freshness_window(store, DEFAULT_FRESHNESS_WINDOW_SECS)
    }

    pub fn with_freshness_window(store: Arc<dyn ObjectStore>, freshness_window_secs: u64) -> Self {
        Self::with_freshness_window_and_limiter(store, freshness_window_secs, Arc::new(NoRateLimit))
    }

    /// Construct an `AuthState` with a custom rate limiter. The
    /// no-arg [`AuthState::new`] uses [`NoRateLimit`]; this hook
    /// exists so the daemon can plug in a real limiter later
    /// without touching call sites.
    pub fn with_freshness_window_and_limiter(
        store: Arc<dyn ObjectStore>,
        freshness_window_secs: u64,
        rate_limiter: Arc<dyn RateLimiter>,
    ) -> Self {
        Self {
            inner: Arc::new(AuthStateInner {
                store,
                pub_keys: RwLock::new(HashMap::new()),
                ancestry_cache: RwLock::new(HashMap::new()),
                name_records: RwLock::new(HashMap::new()),
                verified_tokens: RwLock::new(HashMap::new()),
                freshness_window_secs,
                rate_limiter,
            }),
        }
    }

    /// Run the auth pipeline for a request. Mode selects whether
    /// step 4 (lineage walk) runs:
    ///
    /// - [`RouteAuthMode::LineageGated`]: full pipeline, the URL's
    ///   `vol_id` must be in the token-volume's signed fork ancestry.
    /// - [`RouteAuthMode::SkeletonsOnly`]: skip step 4. Used for
    ///   `volume.pub` and `volume.provenance` so that a claim-time
    ///   chain walk authenticates before the requester's own
    ///   provenance is published. See [`RouteAuthMode`] for the
    ///   security analysis.
    ///
    /// Always-runs step 5 (local stat) is the route handler's
    /// concern; this method returns the resolved [`Authorized`].
    ///
    /// Cache: keyed on `(bearer, vol_id, mode)`. Expiry tracks the
    /// token's residual freshness so a cached entry can never
    /// authorise past the moment the token itself becomes stale. The
    /// mode is part of the key so a `SkeletonsOnly` hit cannot serve
    /// a `LineageGated` request.
    pub async fn verify(
        &self,
        bearer_value: &str,
        url_vol_id: Ulid,
        mode: RouteAuthMode,
    ) -> Result<Authorized, AuthError> {
        // Step 1: decode + freshness. Always runs — these are cheap
        // and the freshness check is what bounds the cache lifetime.
        let token = PeerFetchToken::decode(bearer_value).map_err(AuthError::BadToken)?;
        let now = PeerFetchToken::now_unix_seconds();
        token
            .check_freshness(now, self.inner.freshness_window_secs)
            .map_err(AuthError::BadCredentials)?;

        // Step 2: signature. Always runs — the cache only memoises
        // the *outcome* of the S3-side checks (steps 3 + 4); the
        // signature still proves the request bytes were authored by
        // the holder of `coordinator.key`.
        let vk = self.coordinator_pub(&token.coordinator_id).await?;
        token
            .verify_signature(&vk)
            .map_err(AuthError::BadCredentials)?;

        // Rate-limit hook (no-op default; see `RateLimiter`). Runs
        // before the cache so that even repeat requests within the
        // freshness window remain subject to the policy.
        self.inner
            .rate_limiter
            .check(&token.coordinator_id, &token.volume_name, mode)
            .map_err(AuthError::RateLimited)?;

        let cache_key = VerifiedKey {
            bearer: bearer_value.to_owned(),
            vol_id: url_vol_id,
            mode,
        };
        if let Some(entry) = self.lookup_cached(&cache_key).await {
            return Ok(entry.authorized);
        }

        // Step 3: ownership — the volume name's current claim record
        // must point at this coordinator. Revalidated via ETag on
        // every cache miss.
        let name_record = self.fetch_name_record(&token.volume_name).await?;
        match name_record.state {
            NameState::Live | NameState::Stopped => {}
            _ => return Err(AuthError::NotCurrentClaimer),
        }
        match name_record.coordinator_id.as_deref() {
            Some(id) if id == token.coordinator_id => {}
            _ => return Err(AuthError::NotCurrentClaimer),
        }

        // Step 4: lineage — `LineageGated` only. Skeletons skip this.
        if matches!(mode, RouteAuthMode::LineageGated) {
            let ancestry = self.ancestry(name_record.vol_ulid).await?;
            if !ancestry.contains(&url_vol_id) {
                return Err(AuthError::OutsideLineage);
            }
        }

        let authorized = Authorized {
            volume_name: token.volume_name.clone(),
            coordinator_id: token.coordinator_id.clone(),
            vol_id: url_vol_id,
        };

        self.cache_authorized(cache_key, authorized.clone(), &token, now)
            .await;

        Ok(authorized)
    }

    /// Look up a cached resolution; returns `None` if absent or
    /// expired (in which case the caller falls through to a fresh
    /// pipeline run).
    async fn lookup_cached(&self, key: &VerifiedKey) -> Option<VerifiedEntry> {
        let entry = self.inner.verified_tokens.read().await.get(key).cloned()?;
        if Instant::now() < entry.expires_at {
            Some(entry)
        } else {
            None
        }
    }

    async fn cache_authorized(
        &self,
        key: VerifiedKey,
        authorized: Authorized,
        token: &PeerFetchToken,
        now_unix: u64,
    ) {
        let drift = now_unix.abs_diff(token.issued_at);
        let remaining = self.inner.freshness_window_secs.saturating_sub(drift);
        if remaining == 0 {
            // Token will be stale by next request anyway; don't bother
            // caching (the entry would be invalidated immediately).
            return;
        }
        let expires_at = Instant::now() + Duration::from_secs(remaining);
        let entry = VerifiedEntry {
            authorized,
            expires_at,
        };
        let mut guard = self.inner.verified_tokens.write().await;
        // Opportunistic eviction: drop expired entries while we hold
        // the write lock, so the map can't grow unboundedly across
        // a long-lived process.
        let now = Instant::now();
        guard.retain(|_, v| v.expires_at > now);
        guard.insert(key, entry);
    }

    async fn coordinator_pub(&self, coordinator_id: &str) -> Result<VerifyingKey, AuthError> {
        if let Some(vk) = self.inner.pub_keys.read().await.get(coordinator_id) {
            return Ok(*vk);
        }

        let key = StorePath::from(format!("coordinators/{coordinator_id}/coordinator.pub"));
        let body = self
            .inner
            .store
            .get(&key)
            .await
            .map_err(|e| {
                AuthError::Backend(io::Error::other(format!("fetch coordinator.pub: {e}")))
            })?
            .bytes()
            .await
            .map_err(|e| {
                AuthError::Backend(io::Error::other(format!("read coordinator.pub: {e}")))
            })?;
        let text = std::str::from_utf8(&body).map_err(|e| {
            AuthError::Backend(io::Error::other(format!("coordinator.pub not utf-8: {e}")))
        })?;
        let vk = parse_pub_hex(text.trim()).map_err(|e| {
            AuthError::Backend(io::Error::other(format!("coordinator.pub parse: {e}")))
        })?;

        self.inner
            .pub_keys
            .write()
            .await
            .insert(coordinator_id.to_owned(), vk);
        Ok(vk)
    }

    async fn fetch_name_record(&self, volume_name: &str) -> Result<NameRecord, AuthError> {
        // ETag-conditional revalidation: if we have a cached value
        // for this volume, send `If-None-Match: <etag>`. A 304
        // confirms the cached value; a 200 ships the new value.
        let cached = self
            .inner
            .name_records
            .read()
            .await
            .get(volume_name)
            .cloned();
        let cached_etag = cached.as_ref().and_then(|(_, e)| e.clone());

        let key = StorePath::from(format!("names/{volume_name}"));
        let opts = GetOptions {
            if_none_match: cached_etag,
            ..Default::default()
        };

        let get_result = match self.inner.store.get_opts(&key, opts).await {
            Ok(r) => r,
            Err(ObjectStoreError::NotModified { .. }) => {
                // Cached value still authoritative — return it.
                let (record, _) = cached.expect("304 implies a cached value existed");
                return Ok(record);
            }
            Err(ObjectStoreError::NotFound { .. }) => return Err(AuthError::NotCurrentClaimer),
            Err(other) => {
                return Err(AuthError::Backend(io::Error::other(format!(
                    "fetch names/{volume_name}: {other}"
                ))));
            }
        };

        let new_etag = get_result.meta.e_tag.clone();
        let body = get_result.bytes().await.map_err(|e| {
            AuthError::Backend(io::Error::other(format!(
                "read names/{volume_name} body: {e}"
            )))
        })?;
        let text = std::str::from_utf8(&body).map_err(|e| {
            AuthError::Backend(io::Error::other(format!(
                "names/{volume_name} not utf-8: {e}"
            )))
        })?;
        let record = NameRecord::from_toml(text).map_err(|e| {
            AuthError::Backend(io::Error::other(format!("parse names/{volume_name}: {e}")))
        })?;

        self.inner
            .name_records
            .write()
            .await
            .insert(volume_name.to_owned(), (record.clone(), new_etag));

        Ok(record)
    }

    async fn ancestry(&self, vol_ulid: Ulid) -> Result<HashSet<Ulid>, AuthError> {
        if let Some(set) = self.inner.ancestry_cache.read().await.get(&vol_ulid) {
            return Ok(set.clone());
        }
        let set = walk_ancestry(self.inner.store.as_ref(), vol_ulid)
            .await
            .map_err(AuthError::Backend)?;
        self.inner
            .ancestry_cache
            .write()
            .await
            .insert(vol_ulid, set.clone());
        Ok(set)
    }
}

/// Parse `Authorization: Bearer <token>` into the bare token bytes.
/// Returns `Err(AuthError::MissingBearer)` if the header is absent or
/// malformed.
pub fn parse_bearer(header_value: Option<&str>) -> Result<&str, AuthError> {
    let value = header_value.ok_or(AuthError::MissingBearer)?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or(AuthError::MissingBearer)?;
    if token.is_empty() {
        return Err(AuthError::MissingBearer);
    }
    Ok(token)
}

fn parse_pub_hex(s: &str) -> Result<VerifyingKey, String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let hi = hex_nibble(s.as_bytes()[i * 2])?;
        let lo = hex_nibble(s.as_bytes()[i * 2 + 1])?;
        *byte = (hi << 4) | lo;
    }
    VerifyingKey::from_bytes(&bytes).map_err(|e| format!("invalid ed25519 pubkey: {e}"))
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("non-hex byte: 0x{b:02x}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ed25519_dalek::{Signer, SigningKey};
    use elide_core::name_record::{NameRecord, NameState};
    use elide_core::signing::{ParentRef, ProvenanceLineage, write_provenance};
    use object_store::memory::InMemory;
    use rand_core::OsRng;
    use tempfile::TempDir;

    fn pub_hex(key: &SigningKey) -> String {
        let bytes = key.verifying_key().to_bytes();
        let mut s = String::with_capacity(64);
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s.push('\n');
        s
    }

    /// Construct a token signed with `key` by computing the signature
    /// over the canonical signing payload directly. Mirrors what
    /// `CoordinatorIdentity::sign` would do in production.
    fn sign_token(
        volume_name: &str,
        coordinator_id: &str,
        issued_at: u64,
        key: &SigningKey,
    ) -> PeerFetchToken {
        let payload = PeerFetchToken::signing_payload(volume_name, coordinator_id, issued_at);
        let sig = key.sign(&payload).to_bytes();
        PeerFetchToken {
            volume_name: volume_name.to_owned(),
            coordinator_id: coordinator_id.to_owned(),
            issued_at,
            signature: sig,
        }
    }

    /// Publish a volume's `volume.pub` + `volume.provenance` on the store.
    async fn publish_volume(
        store: &dyn ObjectStore,
        vol_ulid: Ulid,
        key: &SigningKey,
        parent: Option<ParentRef>,
    ) {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("volume.pub"), pub_hex(key)).unwrap();
        let lineage = ProvenanceLineage {
            parent,
            extent_index: Vec::new(),
            oci_source: None,
        };
        write_provenance(tmp.path(), key, "volume.provenance", &lineage).unwrap();
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
    }

    /// Publish a coordinator's `coordinator.pub`.
    async fn publish_coordinator(store: &dyn ObjectStore, coord_id: &str, key: &SigningKey) {
        store
            .put(
                &StorePath::from(format!("coordinators/{coord_id}/coordinator.pub")),
                Bytes::from(pub_hex(key).into_bytes()).into(),
            )
            .await
            .unwrap();
    }

    /// Publish a `names/<name>` record claimed by `coord_id`.
    async fn publish_live_name(
        store: &dyn ObjectStore,
        name: &str,
        vol_ulid: Ulid,
        coord_id: &str,
    ) {
        let mut record = NameRecord::live_minimal(vol_ulid, 4 * 1024 * 1024 * 1024);
        record.coordinator_id = Some(coord_id.to_owned());
        record.state = NameState::Live;
        let toml = record.to_toml().unwrap();
        store
            .put(
                &StorePath::from(format!("names/{name}")),
                Bytes::from(toml.into_bytes()).into(),
            )
            .await
            .unwrap();
    }

    fn make_state() -> (Arc<dyn ObjectStore>, AuthState) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let auth = AuthState::new(store.clone());
        (store, auth)
    }

    #[tokio::test]
    async fn happy_path_root_volume() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_name = "myvol";
        let vol_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        publish_live_name(store.as_ref(), vol_name, vol_ulid, coord_id).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();
        let result = auth
            .verify(&bearer, vol_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();
        assert_eq!(result.coordinator_id, coord_id);
        assert_eq!(result.volume_name, vol_name);
        assert_eq!(result.vol_id, vol_ulid);
    }

    #[tokio::test]
    async fn happy_path_ancestor_vol_id() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let root_key = SigningKey::generate(&mut OsRng);
        let leaf_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_name = "myvol";

        let root_ulid = Ulid::new();
        let leaf_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), root_ulid, &root_key, None).await;
        publish_volume(
            store.as_ref(),
            leaf_ulid,
            &leaf_key,
            Some(ParentRef {
                volume_ulid: root_ulid.to_string(),
                snapshot_ulid: Ulid::new().to_string(),
                pubkey: root_key.verifying_key().to_bytes(),
                manifest_pubkey: None,
            }),
        )
        .await;
        publish_live_name(store.as_ref(), vol_name, leaf_ulid, coord_id).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        // Asking for a segment of the root (in leaf's ancestry) should pass.
        let result = auth
            .verify(&bearer, root_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();
        assert_eq!(result.vol_id, root_ulid);
    }

    #[tokio::test]
    async fn rejects_missing_bearer() {
        let (_, auth) = make_state();
        let err = auth
            .verify(
                "garbage-not-base64",
                Ulid::new(),
                RouteAuthMode::LineageGated,
            )
            .await
            .expect_err("decode");
        assert_eq!(err.status_code(), 401);
        assert!(matches!(err, AuthError::BadToken(_)));
    }

    #[tokio::test]
    async fn rejects_stale_token() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;

        // Token issued in the distant past — well outside the 60s window.
        let stale_at = PeerFetchToken::now_unix_seconds() - 3600;
        let token = sign_token("myvol", coord_id, stale_at, &coord_key);
        let bearer = token.encode();

        let err = auth
            .verify(&bearer, Ulid::new(), RouteAuthMode::LineageGated)
            .await
            .expect_err("stale");
        assert!(matches!(
            err,
            AuthError::BadCredentials(TokenVerifyError::Stale { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_wrong_signing_key() {
        let (store, auth) = make_state();
        let real_coord_key = SigningKey::generate(&mut OsRng);
        let imposter_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";

        // Published pubkey is real_coord_key, but the token is signed by imposter.
        publish_coordinator(store.as_ref(), coord_id, &real_coord_key).await;
        let token = sign_token(
            "myvol",
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &imposter_key,
        );
        let bearer = token.encode();

        let err = auth
            .verify(&bearer, Ulid::new(), RouteAuthMode::LineageGated)
            .await
            .expect_err("bad sig");
        assert!(matches!(
            err,
            AuthError::BadCredentials(TokenVerifyError::BadSignature(_))
        ));
    }

    #[tokio::test]
    async fn rejects_when_not_current_claimer() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let other_coord = "coord-b";
        let vol_key = SigningKey::generate(&mut OsRng);
        let vol_ulid = Ulid::new();
        let vol_name = "myvol";

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        // Names record claims coord-b owns the volume; token is from coord-a.
        publish_live_name(store.as_ref(), vol_name, vol_ulid, other_coord).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        let err = auth
            .verify(&bearer, vol_ulid, RouteAuthMode::LineageGated)
            .await
            .expect_err("wrong claimer");
        assert!(matches!(err, AuthError::NotCurrentClaimer));
        assert_eq!(err.status_code(), 401);
    }

    #[tokio::test]
    async fn rejects_when_volume_released() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_key = SigningKey::generate(&mut OsRng);
        let vol_ulid = Ulid::new();
        let vol_name = "myvol";

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;

        // Released — even though coord_id matches the *most recent* owner,
        // there's no current claim, so peer-fetch must not authorise.
        let mut record = NameRecord::live_minimal(vol_ulid, 4 * 1024 * 1024 * 1024);
        record.coordinator_id = Some(coord_id.to_owned());
        record.state = NameState::Released;
        store
            .put(
                &StorePath::from(format!("names/{vol_name}")),
                Bytes::from(record.to_toml().unwrap().into_bytes()).into(),
            )
            .await
            .unwrap();

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        let err = auth
            .verify(&bearer, vol_ulid, RouteAuthMode::LineageGated)
            .await
            .expect_err("released");
        assert!(matches!(err, AuthError::NotCurrentClaimer));
    }

    #[tokio::test]
    async fn rejects_outside_lineage() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_name = "myvol";
        let vol_ulid = Ulid::new();
        let unrelated_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        publish_live_name(store.as_ref(), vol_name, vol_ulid, coord_id).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        let err = auth
            .verify(&bearer, unrelated_ulid, RouteAuthMode::LineageGated)
            .await
            .expect_err("not in lineage");
        assert!(matches!(err, AuthError::OutsideLineage));
        assert_eq!(err.status_code(), 403);
    }

    #[tokio::test]
    async fn rejects_when_name_record_missing() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        // No names/<name> published.

        let token = sign_token(
            "missing-vol",
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        let err = auth
            .verify(&bearer, Ulid::new(), RouteAuthMode::LineageGated)
            .await
            .expect_err("no name");
        assert!(matches!(err, AuthError::NotCurrentClaimer));
    }

    #[tokio::test]
    async fn parse_bearer_extracts_token() {
        assert_eq!(parse_bearer(Some("Bearer abc123")).unwrap(), "abc123");
    }

    #[tokio::test]
    async fn parse_bearer_rejects_missing() {
        assert!(matches!(parse_bearer(None), Err(AuthError::MissingBearer)));
        assert!(matches!(
            parse_bearer(Some("Basic xyz")),
            Err(AuthError::MissingBearer)
        ));
        assert!(matches!(
            parse_bearer(Some("Bearer ")),
            Err(AuthError::MissingBearer)
        ));
    }

    /// Per-token cache hit: the second verify with the same bearer
    /// returns the cached `Authorized` even if the underlying
    /// `names/<name>` has flipped to disauthorise. This proves the
    /// resolved-Authorized cache is consulted; the trade-off is the
    /// fence-gap is bounded by the token's freshness window (a refreshed
    /// token would be a cache miss and pick up the change).
    #[tokio::test]
    async fn cache_returns_authorized_even_after_names_flips() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let other_coord = "coord-b";
        let vol_name = "myvol";
        let vol_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        publish_live_name(store.as_ref(), vol_name, vol_ulid, coord_id).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        // First verify: cache miss → full pipeline → success, cached.
        auth.verify(&bearer, vol_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();

        // Flip the names record to disauthorise this coordinator.
        publish_live_name(store.as_ref(), vol_name, vol_ulid, other_coord).await;

        // Second verify with same bearer + vol_id: cache hit, returns OK
        // even though S3-side state would now reject. This is the
        // expected behaviour — the cache lifetime is the token's
        // freshness window, after which a fresh pipeline run picks up
        // the change.
        let result = auth
            .verify(&bearer, vol_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();
        assert_eq!(result.coordinator_id, coord_id);
    }

    /// Cache is keyed on `(bearer, vol_id)` — a different `vol_id`
    /// for the same token bypasses the cache and re-runs the lineage
    /// step, so each ancestor is independently authorised.
    #[tokio::test]
    async fn cache_keys_on_vol_id_independently() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let root_key = SigningKey::generate(&mut OsRng);
        let leaf_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_name = "myvol";

        let root_ulid = Ulid::new();
        let leaf_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), root_ulid, &root_key, None).await;
        publish_volume(
            store.as_ref(),
            leaf_ulid,
            &leaf_key,
            Some(ParentRef {
                volume_ulid: root_ulid.to_string(),
                snapshot_ulid: Ulid::new().to_string(),
                pubkey: root_key.verifying_key().to_bytes(),
                manifest_pubkey: None,
            }),
        )
        .await;
        publish_live_name(store.as_ref(), vol_name, leaf_ulid, coord_id).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        // Both ancestor + leaf authorise.
        auth.verify(&bearer, leaf_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();
        auth.verify(&bearer, root_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();

        // Repeats hit cache.
        auth.verify(&bearer, leaf_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();
        auth.verify(&bearer, root_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();

        // An unrelated vol_id is a cache miss → pipeline runs →
        // ancestry rejection.
        let unrelated = Ulid::new();
        let err = auth
            .verify(&bearer, unrelated, RouteAuthMode::LineageGated)
            .await
            .expect_err("not in lineage");
        assert!(matches!(err, AuthError::OutsideLineage));
    }

    /// ETag-conditional names lookup: a verify with no prior cache
    /// (e.g. a different bearer that misses the resolved-Authorized
    /// cache) still hits the names record cache via `If-None-Match`.
    /// Updating the underlying names record changes the ETag, so the
    /// next request observes the new value.
    #[tokio::test]
    async fn etag_conditional_picks_up_names_changes_on_cache_miss() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let other_coord = "coord-b";
        let vol_name = "myvol";
        let vol_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_coordinator(store.as_ref(), other_coord, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        publish_live_name(store.as_ref(), vol_name, vol_ulid, coord_id).await;

        // First verify warms the names ETag cache.
        let now = PeerFetchToken::now_unix_seconds();
        let token1 = sign_token(vol_name, coord_id, now, &coord_key);
        auth.verify(&token1.encode(), vol_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();

        // Flip names to other_coord. ETag changes → next conditional
        // GET returns 200 with the new value.
        publish_live_name(store.as_ref(), vol_name, vol_ulid, other_coord).await;

        // Mint a fresh token so the resolved-Authorized cache misses
        // (different `issued_at` ⇒ different bearer bytes).
        let token2 = sign_token(vol_name, coord_id, now + 1, &coord_key);
        let err = auth
            .verify(&token2.encode(), vol_ulid, RouteAuthMode::LineageGated)
            .await
            .expect_err("names flipped");
        assert!(matches!(err, AuthError::NotCurrentClaimer));
    }

    /// Tokens issued at exactly the freshness boundary skip caching
    /// (the entry would expire immediately and never serve a hit).
    #[tokio::test]
    async fn boundary_token_does_not_pollute_cache() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_name = "myvol";
        let vol_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        publish_live_name(store.as_ref(), vol_name, vol_ulid, coord_id).await;

        // Token at exactly the boundary: drift == freshness_window.
        // `check_freshness` accepts `<=`, so this is the last instant
        // the token is fresh.
        let now = PeerFetchToken::now_unix_seconds();
        let token = sign_token(
            vol_name,
            coord_id,
            now - DEFAULT_FRESHNESS_WINDOW_SECS,
            &coord_key,
        );
        let bearer = token.encode();

        let result = auth
            .verify(&bearer, vol_ulid, RouteAuthMode::LineageGated)
            .await
            .unwrap();
        assert_eq!(result.coordinator_id, coord_id);

        // The verified-tokens map should not contain this entry —
        // residual freshness was zero.
        let cache_size = auth.inner.verified_tokens.read().await.len();
        assert_eq!(
            cache_size, 0,
            "boundary token should not enter the resolved cache"
        );
    }

    /// `SkeletonsOnly` mode skips the lineage walk: a request for a
    /// `vol_id` *not* in the requesting volume's signed ancestry
    /// passes auth, where `LineageGated` would reject it as
    /// `OutsideLineage`. Models the early-rebind claim path —
    /// requesting volume's provenance hasn't been signed yet, so
    /// even our own ancestry is unwalkable.
    #[tokio::test]
    async fn skeletons_only_skips_lineage_walk() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_name = "myvol";
        let vol_ulid = Ulid::new();
        let unrelated_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        publish_live_name(store.as_ref(), vol_name, vol_ulid, coord_id).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        // LineageGated rejects: unrelated_ulid is not in vol_ulid's
        // (singleton) ancestry.
        let err = auth
            .verify(&bearer, unrelated_ulid, RouteAuthMode::LineageGated)
            .await
            .expect_err("lineage gate must reject");
        assert!(matches!(err, AuthError::OutsideLineage));

        // SkeletonsOnly accepts the same request — no ancestry walk.
        let result = auth
            .verify(&bearer, unrelated_ulid, RouteAuthMode::SkeletonsOnly)
            .await
            .unwrap();
        assert_eq!(result.coordinator_id, coord_id);
        assert_eq!(result.vol_id, unrelated_ulid);
    }

    /// Cache is keyed on mode: a `SkeletonsOnly` hit for a
    /// non-ancestor `vol_id` must not satisfy a subsequent
    /// `LineageGated` request for the same `(bearer, vol_id)`.
    #[tokio::test]
    async fn cache_does_not_cross_modes() {
        let (store, auth) = make_state();
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_name = "myvol";
        let vol_ulid = Ulid::new();
        let unrelated_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        publish_live_name(store.as_ref(), vol_name, vol_ulid, coord_id).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        // SkeletonsOnly succeeds and caches.
        auth.verify(&bearer, unrelated_ulid, RouteAuthMode::SkeletonsOnly)
            .await
            .unwrap();

        // LineageGated for the same bearer + vol_id must NOT be served
        // from the SkeletonsOnly cache entry — it must run the lineage
        // walk and reject.
        let err = auth
            .verify(&bearer, unrelated_ulid, RouteAuthMode::LineageGated)
            .await
            .expect_err("must not serve cached SkeletonsOnly hit to LineageGated");
        assert!(matches!(err, AuthError::OutsideLineage));
    }

    /// The rate-limiter hook runs after signature verification. A
    /// limiter that always rejects produces `AuthError::RateLimited`.
    #[tokio::test]
    async fn rate_limiter_can_reject_authenticated_requests() {
        struct AlwaysReject;
        impl RateLimiter for AlwaysReject {
            fn check(&self, _: &str, _: &str, _: RouteAuthMode) -> Result<(), &'static str> {
                Err("test-policy")
            }
        }

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let auth = AuthState::with_freshness_window_and_limiter(
            store.clone(),
            DEFAULT_FRESHNESS_WINDOW_SECS,
            Arc::new(AlwaysReject),
        );
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a";
        let vol_name = "myvol";
        let vol_ulid = Ulid::new();

        publish_coordinator(store.as_ref(), coord_id, &coord_key).await;
        publish_volume(store.as_ref(), vol_ulid, &vol_key, None).await;
        publish_live_name(store.as_ref(), vol_name, vol_ulid, coord_id).await;

        let token = sign_token(
            vol_name,
            coord_id,
            PeerFetchToken::now_unix_seconds(),
            &coord_key,
        );
        let bearer = token.encode();

        let err = auth
            .verify(&bearer, vol_ulid, RouteAuthMode::LineageGated)
            .await
            .expect_err("limiter rejected");
        assert!(matches!(err, AuthError::RateLimited("test-policy")));
    }
}
