//! Scoped store provider for coordinator-side S3 access.
//!
//! Every coordinator S3 op routes through a [`ScopedStores`] handle.
//! The trait carves the bucket into the three mint credential roles
//! the coordinator wields (`docs/design-mint.md` § *Coordinator store
//! architecture*). A call site picks the role matching the purpose of
//! its code path. A mutation path uses [`ScopedStores::writer`] for
//! its whole `names/`+`events/`+own-`coordinators/` interaction; the
//! `coord-writer` policy holds `GetObject` on those prefixes, so the
//! reads inside a name-claim CAS run on the same credential as the
//! conditional write.
//!
//! * [`ScopedStores::base_ro`] — `coord-base`. The read-only
//!   control-plane baseline, and the credential the LAN/internet-
//!   exposed peer-fetch verifier holds. Returns a narrow [`ReadStore`]
//!   so a holder can read and `head` only.
//!
//! * [`ScopedStores::writer`] — `coord-writer`. Coordinator-wide
//!   write: `names/`, `events/` (get + append), own
//!   `coordinators/<sub>/`, and `ListBucket`.
//!
//! * [`ScopedStores::data_for_volume`] — `coord-data`. Per-volume
//!   read+write under `by_id/<vol_ulid>/`.
//!
//! `volume-ro` is vended to the volume process
//! (`crate::mint_client`); the coordinator holds the three roles
//! above.
//!
//! [`PassthroughStores`] is the impl for the local-store / no-`[mint]`
//! case: one underlying store for every role. The mint-backed impl
//! ([`crate::mint_stores`]) is selected when `[mint]` is configured.

use std::sync::Arc;

use async_trait::async_trait;
use object_store::path::Path;
use object_store::{GetResult, ObjectMeta, ObjectStore};
use ulid::Ulid;

use crate::event_journal::{
    BucketEventJournal, EventJournal, EventJournalReader, ReadOnlyEventJournal,
};
use crate::name_claims::{BucketNameClaims, NameClaims, NameClaimsReader, ReadOnlyNameClaims};

/// Read-only S3 surface — `coord-base`. Exposes `get` and `head`. A
/// holder can read individual objects; the containment boundary the
/// exposed peer-fetch verifier relies on is carried by this type.
#[async_trait]
pub trait ReadStore: Send + Sync {
    async fn get(&self, location: &Path) -> object_store::Result<GetResult>;
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta>;
}

/// Adapts any [`ObjectStore`] down to the read-only [`ReadStore`]
/// surface. The passthrough / local-store impl returns this over its
/// single inner store; the mint-backed impl returns it over the
/// `coord-base`-keyed store.
pub struct ReadOnlyAdapter {
    inner: Arc<dyn ObjectStore>,
}

impl ReadOnlyAdapter {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl ReadStore for ReadOnlyAdapter {
    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.as_ref().get(location).await
    }
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.as_ref().head(location).await
    }
}

/// A full [`ObjectStore`] handle also satisfies [`ReadStore`]. This is
/// what lets a pure-read helper take `&dyn ReadStore` while both a
/// read-only path (passing [`ScopedStores::base_ro`]) and a mutation
/// path (passing its already-held [`ScopedStores::writer`]) call it
/// unchanged — the credential is decided by what the call site
/// acquired for its purpose, not by the helper. A read-only path
/// still cannot write, because it only ever holds the narrow
/// `base_ro()` handle.
#[async_trait]
impl ReadStore for Arc<dyn ObjectStore> {
    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        (**self).get(location).await
    }
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        (**self).head(location).await
    }
}

/// Picks the right credential-scoped handle for a given coordinator
/// S3 op. With [`PassthroughStores`] every handle wraps the same inner
/// store; with the mint-backed impl they are distinct mint-vended
/// keypairs.
pub trait ScopedStores: Send + Sync {
    /// `coord-base`: read-only `names/* coordinators/* events/*`.
    /// Read-only paths and the exposed verifier.
    fn base_ro(&self) -> Arc<dyn ReadStore>;

    /// `coord-writer`: coordinator-wide write authority. Mutation
    /// paths use this end-to-end (the reads in a CAS included).
    fn writer(&self) -> Arc<dyn ObjectStore>;

    /// `coord-data`: read+write under `by_id/<vol_ulid>/`. Uploads,
    /// GC, snapshot publish, readonly ancestor pulls.
    fn data_for_volume(&self, vol_ulid: &Ulid) -> Arc<dyn ObjectStore>;

    /// The `coord-base` store as a plain [`ObjectStore`], for the one
    /// cross-crate consumer that needs it: the peer-fetch verifier
    /// (`elide_peer_fetch::auth::AuthState`), which reads only and
    /// lives in a lower crate that cannot depend on [`ReadStore`].
    /// In-coordinator code uses [`Self::base_ro`]; the read-only
    /// guarantee for this handle rests on `coord-base`'s IAM policy.
    fn peer_verifier_store(&self) -> Arc<dyn ObjectStore>;

    /// Full read+write handle for the per-name event log
    /// (`events/<name>/…`). Backed by both `coord-writer` (for
    /// emit's CAS, which runs wholly on one credential) and
    /// `coord-base` (for the inherited reads, which need
    /// cross-coordinator pubkey lookups in `list_and_verify`).
    /// First slice of the domain-typed store layer
    /// (`docs/design-domain-store.md`); the trait deliberately has
    /// no `delete`, so a caller holding only an [`EventJournal`]
    /// cannot violate the append-only invariant.
    fn event_journal(&self) -> Arc<dyn EventJournal> {
        Arc::new(BucketEventJournal::new(
            self.writer(),
            self.peer_verifier_store(),
        ))
    }

    /// Read-only handle for the per-name event log — `coord-base`
    /// scope. A holder cannot call `emit`, so pure-read call sites
    /// (`volume events` IPC, peer-discovery) carry no over-privilege
    /// at the type level either. Mirrors the
    /// [`ReadStore`] vs `ObjectStore` split.
    fn event_journal_ro(&self) -> Arc<dyn EventJournalReader> {
        Arc::new(ReadOnlyEventJournal::new(self.peer_verifier_store()))
    }

    /// Full read+write handle for the `names/<name>` claim records.
    /// Backed by both `coord-writer` (for the `mark_*` CAS verbs,
    /// which run wholly on one credential per mutation) and
    /// `coord-base` (for the inherited reads). The trait exposes no
    /// untyped `update` / `overwrite` — every state change is a typed
    /// `mark_*` verb.
    fn name_claims(&self) -> Arc<dyn NameClaims> {
        Arc::new(BucketNameClaims::new(
            self.writer(),
            self.peer_verifier_store(),
        ))
    }

    /// Read-only handle for the `names/<name>` claim records —
    /// `coord-base` scope. A holder cannot invoke any `mark_*` verb
    /// at the type level. Used by `Request::ResolveName`,
    /// `bucket_position::fetch_position`, and the few pure-display
    /// readers.
    fn name_claims_ro(&self) -> Arc<dyn NameClaimsReader> {
        Arc::new(ReadOnlyNameClaims::new(self.peer_verifier_store()))
    }
}

/// Returns the same underlying `Arc<dyn ObjectStore>` for every role
/// (wrapped in [`ReadOnlyAdapter`] for `base_ro`). The minimum-viable
/// impl — equivalent to the pre-mint behaviour where every op used one
/// full-bucket key. Used for the local-store / no-`[mint]` case.
pub struct PassthroughStores {
    inner: Arc<dyn ObjectStore>,
}

impl PassthroughStores {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { inner: store }
    }
}

impl ScopedStores for PassthroughStores {
    fn base_ro(&self) -> Arc<dyn ReadStore> {
        Arc::new(ReadOnlyAdapter::new(Arc::clone(&self.inner)))
    }

    fn writer(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    fn data_for_volume(&self, _vol_ulid: &Ulid) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    fn peer_verifier_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn passthrough_shares_one_store_and_readstore_can_read() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stores = PassthroughStores::new(Arc::clone(&inner));

        let w = stores.writer();
        let d = stores.data_for_volume(&Ulid::new());
        assert!(Arc::ptr_eq(&w, &inner));
        assert!(Arc::ptr_eq(&d, &inner));

        // The narrow ReadStore reads through to the same bytes.
        let key = Path::from("names/demo");
        w.put(&key, b"v".to_vec().into()).await.expect("put");
        let got = stores.base_ro().get(&key).await.expect("get");
        assert_eq!(got.bytes().await.expect("bytes").as_ref(), b"v");
    }
}
