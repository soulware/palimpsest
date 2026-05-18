//! Mint-backed [`ScopedStores`] (`docs/design-mint.md` § *Coordinator
//! store architecture*).
//!
//! Each coordinator role (`coord-base`, `coord-writer`, and one
//! `coord-data` per volume) is a [`RoleStore`] facade over a Tigris
//! keypair that mint vends via `assume-role`. The facade implements
//! [`ObjectStore`] and acquires its keypair lazily on first use,
//! caching the built `AmazonS3` client and re-assuming once the cached
//! credential passes its refresh point (half of the remaining TTL —
//! the *TTL principle*: refresh well inside the revocation window). A
//! brief refresh stall is absorbed by the WAL for writes and is off
//! the hot path for reads.
//!
//! `ScopedStores`'s methods are sync, so the facade is returned
//! immediately and the `assume-role` round-trip happens inside the
//! facade's own async ops.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use tokio::sync::Mutex;
use ulid::Ulid;

use elide_coordinator::config::{MintConfig, StoreSection};
use elide_coordinator::identity::CoordinatorIdentity;
use elide_coordinator::stores::{ReadOnlyAdapter, ReadStore, ScopedStores};

use crate::mint_client::MintEndpoint;

pub const ROLE_COORD_BASE: &str = "coord-base";
pub const ROLE_COORD_WRITER: &str = "coord-writer";
pub const ROLE_COORD_DATA: &str = "coord-data";
const CAVEAT_VOLUME: &str = "elide:Volume";

/// Documented coord-* TTLs (`docs/design-mint.md` § *Elide as
/// customer*): coordinator-wide control plane 1h, per-volume data 24h.
const COORD_CONTROL_TTL_SECS: u64 = 60 * 60;
const COORD_DATA_TTL_SECS: u64 = 24 * 60 * 60;

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

struct Cached {
    store: Arc<dyn ObjectStore>,
    /// Unix seconds at which `ensure` re-assumes — half the original
    /// TTL window before the credential's hard expiry.
    refresh_at: u64,
}

/// One mint credential role as an [`ObjectStore`]. Holds the cached
/// `AmazonS3` built from the last vended keypair and re-assumes when
/// it passes `refresh_at`.
pub struct RoleStore {
    endpoint: MintEndpoint,
    store_cfg: StoreSection,
    role: &'static str,
    ttl_secs: u64,
    /// `coord-data` is per-volume; the `elide:Volume` narrowing caveat
    /// + audit value. `None` for the coordinator-wide roles.
    vol_ulid: Option<Ulid>,
    cached: Mutex<Option<Cached>>,
}

impl fmt::Debug for RoleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RoleStore({}", self.role)?;
        if let Some(v) = &self.vol_ulid {
            write!(f, " vol={v}")?;
        }
        write!(f, ")")
    }
}

impl fmt::Display for RoleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mint:{}", self.role)
    }
}

impl RoleStore {
    fn new(
        endpoint: MintEndpoint,
        store_cfg: StoreSection,
        role: &'static str,
        ttl_secs: u64,
        vol_ulid: Option<Ulid>,
    ) -> Self {
        Self {
            endpoint,
            store_cfg,
            role,
            ttl_secs,
            vol_ulid,
            cached: Mutex::new(None),
        }
    }

    /// Return a live `AmazonS3` for this role, re-assuming via mint if
    /// there is no cached keypair or the cached one has passed its
    /// refresh point.
    async fn ensure(&self) -> OsResult<Arc<dyn ObjectStore>> {
        let mut guard = self.cached.lock().await;
        if let Some(c) = guard.as_ref()
            && now_unix() < c.refresh_at
        {
            return Ok(Arc::clone(&c.store));
        }

        let issued = self
            .assume()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "mint",
                source: Box::new(e),
            })?;
        let store = self
            .store_cfg
            .build_with_creds(&issued.access_key_id, &issued.secret_access_key)
            .map_err(|e| object_store::Error::Generic {
                store: "mint",
                source: e.into(),
            })?;

        let now = now_unix();
        let expiry = issued.expiry_unix.unwrap_or(now + self.ttl_secs);
        // Refresh at the midpoint of the remaining window so a stalled
        // refresh still leaves a valid credential in hand.
        let refresh_at = now + expiry.saturating_sub(now) / 2;
        *guard = Some(Cached {
            store: Arc::clone(&store),
            refresh_at,
        });
        Ok(store)
    }

    async fn assume(&self) -> std::io::Result<crate::credential::IssuedCredentials> {
        let vol = self.vol_ulid.map(|v| v.to_string());
        let narrowing: Vec<(&str, &str)> = match &vol {
            Some(v) => vec![(CAVEAT_VOLUME, v.as_str())],
            None => Vec::new(),
        };
        self.endpoint
            .assume_role(self.role, self.ttl_secs, &narrowing, &[])
            .await
    }
}

#[async_trait]
impl ObjectStore for RoleStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.ensure().await?.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.ensure()
            .await?
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.ensure().await?.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> OsResult<Bytes> {
        self.ensure().await?.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.ensure().await?.as_ref().head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.ensure().await?.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        // `list` is sync-returning-a-stream, so it cannot await
        // `ensure`. A spawned task owns the (re-assumed) store and its
        // borrowed list stream together — no self-referential struct —
        // and forwards items over a channel; the returned `'static`
        // stream is just the receiver.
        let prefix = prefix.cloned();
        let (tx, rx) = tokio::sync::mpsc::channel::<OsResult<ObjectMeta>>(64);
        let endpoint = self.endpoint.clone();
        let store_cfg = self.store_cfg.clone();
        let role = self.role;
        let ttl = self.ttl_secs;
        let vol = self.vol_ulid;
        tokio::spawn(async move {
            let facade = RoleStore::new(endpoint, store_cfg, role, ttl, vol);
            let store = match facade.ensure().await {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            use futures::StreamExt;
            let mut s = store.list(prefix.as_ref());
            while let Some(item) = s.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.ensure().await?.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.ensure().await?.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.ensure().await?.copy_if_not_exists(from, to).await
    }
}

/// [`ScopedStores`] backed by the external mint service. Selected when
/// `[mint]` is configured; otherwise the coordinator uses
/// `PassthroughStores`.
pub struct MintScopedStores {
    base: Arc<RoleStore>,
    writer: Arc<RoleStore>,
    endpoint: MintEndpoint,
    store_cfg: StoreSection,
    data: Mutex<HashMap<Ulid, Arc<RoleStore>>>,
}

impl MintScopedStores {
    pub fn new(
        cfg: &MintConfig,
        store_cfg: StoreSection,
        data_dir: std::path::PathBuf,
        identity: Arc<CoordinatorIdentity>,
    ) -> Self {
        let endpoint = MintEndpoint::new(cfg, data_dir, identity);
        let base = Arc::new(RoleStore::new(
            endpoint.clone(),
            store_cfg.clone(),
            ROLE_COORD_BASE,
            COORD_CONTROL_TTL_SECS,
            None,
        ));
        let writer = Arc::new(RoleStore::new(
            endpoint.clone(),
            store_cfg.clone(),
            ROLE_COORD_WRITER,
            COORD_CONTROL_TTL_SECS,
            None,
        ));
        Self {
            base,
            writer,
            endpoint,
            store_cfg,
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl ScopedStores for MintScopedStores {
    fn base_ro(&self) -> Arc<dyn ReadStore> {
        Arc::new(ReadOnlyAdapter::new(
            Arc::clone(&self.base) as Arc<dyn ObjectStore>
        ))
    }

    fn writer(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.writer) as Arc<dyn ObjectStore>
    }

    fn peer_verifier_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.base) as Arc<dyn ObjectStore>
    }

    fn data_for_volume(&self, vol_ulid: &Ulid) -> Arc<dyn ObjectStore> {
        // Reuse a volume's facade so its keypair cache is shared
        // across ops. `try_lock` keeps this sync method non-blocking;
        // a momentary contention just builds a fresh facade (its first
        // op assumes lazily either way — no correctness impact).
        if let Ok(map) = self.data.try_lock()
            && let Some(rs) = map.get(vol_ulid)
        {
            return Arc::clone(rs) as Arc<dyn ObjectStore>;
        }
        let rs = Arc::new(RoleStore::new(
            self.endpoint.clone(),
            self.store_cfg.clone(),
            ROLE_COORD_DATA,
            COORD_DATA_TTL_SECS,
            Some(*vol_ulid),
        ));
        if let Ok(mut map) = self.data.try_lock() {
            map.insert(*vol_ulid, Arc::clone(&rs));
        }
        rs as Arc<dyn ObjectStore>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_ttls_match_doc() {
        assert_eq!(COORD_CONTROL_TTL_SECS, 3600);
        assert_eq!(COORD_DATA_TTL_SECS, 86400);
    }

    #[test]
    fn refresh_at_is_window_midpoint() {
        // expiry 1000s out → refresh at +500s.
        let now: u64 = 10_000;
        let expiry: u64 = now + 1000;
        let refresh_at = now + expiry.saturating_sub(now) / 2;
        assert_eq!(refresh_at, now + 500);
    }
}
