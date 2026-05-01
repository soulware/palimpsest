//! Scoped store provider for coordinator-side S3 access.
//!
//! All coordinator writes route through a [`ScopedStores`] handle. The
//! trait carves the bucket into two scopes so each call site explicitly
//! tags whether its op is per-volume or coordinator-wide:
//!
//! * [`ScopedStores::for_volume`] — returns the store handle to use for
//!   ops scoped to a single volume's keyspace under
//!   `by_id/<vol_ulid>/`. Examples: segment uploads, GC rewrites and
//!   deletes, snapshot manifest/marker/filemap publish, readonly
//!   ancestor pulls.
//!
//! * [`ScopedStores::coordinator_wide`] — returns the store handle for
//!   keys not under any single `by_id/<vol_ulid>/` (the `names/`
//!   namespace, `events/`, `coordinator/<id>.pub`) and for ops that
//!   touch both per-volume and coordinator-wide keys in one workflow
//!   (e.g. the release flow, which uploads a snapshot manifest *and*
//!   flips `names/<name>` in the same op).
//!
//! Today the only impl is [`PassthroughStores`], which returns the
//! coordinator's single configured store from both methods — i.e. no
//! actual scoping. The trait exists so future per-volume credential
//! issuers (Tigris IAM, AWS STS) drop in without touching every call
//! site.

use std::sync::Arc;

use object_store::ObjectStore;
use ulid::Ulid;

/// Picks the right `object_store` handle for a given coordinator S3 op.
///
/// Each method documents the exact key prefixes the returned handle is
/// expected to be authorised for. With the passthrough impl all returned
/// handles are the same; with a future scoped impl they may differ.
pub trait ScopedStores: Send + Sync {
    /// Handle authorised for the per-volume keyspace under
    /// `by_id/<vol_ulid>/`. Used by uploads, GC, snapshot publish,
    /// and readonly pulls.
    fn for_volume(&self, vol_ulid: &Ulid) -> Arc<dyn ObjectStore>;

    /// Handle authorised for keys not scoped to a single volume:
    /// `names/`, `events/`, `coordinator/`. Also returned for ops
    /// that span both per-volume and coordinator-wide keys.
    fn coordinator_wide(&self) -> Arc<dyn ObjectStore>;
}

/// Returns the same `Arc<dyn ObjectStore>` from both scope methods.
/// This is the minimum-viable impl — equivalent to today's behaviour
/// where every coordinator op uses one full-bucket key. Logs nothing
/// at construction; callers may want to surface their own startup
/// note about running unscoped.
pub struct PassthroughStores {
    inner: Arc<dyn ObjectStore>,
}

impl PassthroughStores {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { inner: store }
    }
}

impl ScopedStores for PassthroughStores {
    fn for_volume(&self, _vol_ulid: &Ulid) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    fn coordinator_wide(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// Passthrough returns the same `Arc<dyn ObjectStore>` instance
    /// from both methods. Two distinct ULIDs share the handle: V1 has
    /// no per-volume scoping, that's the whole point.
    #[test]
    fn passthrough_returns_same_handle_for_every_scope() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stores = PassthroughStores::new(Arc::clone(&inner));

        let a = Ulid::new();
        let b = Ulid::new();
        let s_a = stores.for_volume(&a);
        let s_b = stores.for_volume(&b);
        let s_wide = stores.coordinator_wide();

        assert!(Arc::ptr_eq(&s_a, &inner));
        assert!(Arc::ptr_eq(&s_b, &inner));
        assert!(Arc::ptr_eq(&s_wide, &inner));
    }
}
