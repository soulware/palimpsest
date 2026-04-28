//! Foundations for portable live volumes.
//!
//! See `docs/design-portable-live-volume.md` and
//! `docs/portable-live-volume-plan.md`. This module hosts Phase 0 work:
//!
//! - `coordinator_id()` derivation from `coordinator.root_key`.
//! - Conditional-PUT helpers (`put_if_absent`, `put_with_match`).
//! - Bucket capability probe (`BucketCapabilities`).
//!
//! Later phases will plumb these into the `volume stop`/`volume start`
//! flows and the `volume list` redesign. For now the module is
//! standalone — no other code calls into it yet.

use std::fmt;

use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutMode, PutResult, UpdateVersion};

/// Derive the public coordinator identity from `coordinator.root_key`.
///
/// Domain-separated via `blake3::derive_key`; the raw key never leaves
/// the coordinator. The same `root_key` always derives the same id;
/// rotating or deleting the key produces a new identity (which is the
/// documented escape hatch in `design-portable-live-volume.md`).
pub fn coordinator_id(root_key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("elide coordinator-id v1", root_key)
}

/// Format a coordinator id as a stable Crockford-Base32 ULID-shaped
/// string, suitable for inclusion in `names/<name>` records and audit
/// metadata. Uses the lower 16 bytes of the derived key.
pub fn format_coordinator_id(id: &[u8; 32]) -> String {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&id[..16]);
    ulid::Ulid::from_bytes(bytes).to_string()
}

/// Errors from the conditional-PUT helpers.
#[derive(Debug)]
pub enum ConditionalPutError {
    /// The store-side precondition (existence or version match) was not met.
    PreconditionFailed,
    /// Any other error reported by the underlying store.
    Other(object_store::Error),
}

impl fmt::Display for ConditionalPutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PreconditionFailed => write!(f, "conditional put precondition failed"),
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConditionalPutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Other(e) => Some(e),
            _ => None,
        }
    }
}

impl From<object_store::Error> for ConditionalPutError {
    fn from(e: object_store::Error) -> Self {
        match e {
            object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. } => Self::PreconditionFailed,
            other => Self::Other(other),
        }
    }
}

/// Atomically write an object only if it does not already exist
/// (`If-None-Match: *`). Returns `PreconditionFailed` if a value is
/// already present at the key.
pub async fn put_if_absent(
    store: &dyn ObjectStore,
    key: &StorePath,
    body: Bytes,
) -> Result<PutResult, ConditionalPutError> {
    store
        .put_opts(key, body.into(), PutMode::Create.into())
        .await
        .map_err(Into::into)
}

/// Atomically replace an existing object only if its current version
/// matches `expected` (`If-Match: <etag>`). Returns `PreconditionFailed`
/// if the version differs or the object is absent.
pub async fn put_with_match(
    store: &dyn ObjectStore,
    key: &StorePath,
    body: Bytes,
    expected: UpdateVersion,
) -> Result<PutResult, ConditionalPutError> {
    store
        .put_opts(key, body.into(), PutMode::Update(expected).into())
        .await
        .map_err(Into::into)
}

/// Capabilities of a configured object store.
///
/// Populated once per coordinator startup by `probe_capabilities`. When
/// `conditional_put` is false, portable-volume operations that require
/// atomic ownership transfer must refuse with a clear error pointing
/// the user at `volume create --from <vol_ulid>/<snap_ulid>` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketCapabilities {
    pub conditional_put: bool,
}

/// Probe a bucket for conditional-PUT support. Idempotent and safe to
/// re-run on every coordinator start.
///
/// The lifecycle verbs need both:
///   * `If-None-Match: *` (`PutMode::Create`) — used to claim a name
///     without overwriting an existing record;
///   * `If-Match: <etag>` (`PutMode::Update`) — used to flip
///     `state` on `names/<name>` while detecting concurrent writers.
///
/// `AmazonS3` supports the first unconditionally but only the second
/// when `with_conditional_put(S3ConditionalPut::ETagMatch)` was set on
/// the builder. So the probe exercises *both*: write with `Create`,
/// re-write with `Update(etag)`. Either step returning
/// `NotImplemented` means the backend can't safely host the lifecycle
/// state machine. The probe object is best-effort cleaned up.
pub async fn probe_capabilities(
    store: &dyn ObjectStore,
    probe_key: &StorePath,
) -> Result<BucketCapabilities, object_store::Error> {
    let _ = store.delete(probe_key).await;

    let body = Bytes::from_static(b"elide-cap-probe");

    let create_result = match store
        .put_opts(probe_key, body.clone().into(), PutMode::Create.into())
        .await
    {
        Ok(r) => r,
        Err(object_store::Error::NotImplemented) => {
            return Ok(BucketCapabilities {
                conditional_put: false,
            });
        }
        Err(e) => return Err(e),
    };

    // Re-create must reject — confirms `If-None-Match: *` is honoured.
    let create_rejects_duplicate = matches!(
        store
            .put_opts(probe_key, body.clone().into(), PutMode::Create.into())
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    );

    // `If-Match: <etag>` must succeed — this is the path that returns
    // `NotImplemented` on AmazonS3 unless `S3ConditionalPut` is set.
    let update_supported = match store
        .put_opts(
            probe_key,
            body.into(),
            PutMode::Update(UpdateVersion::from(create_result)).into(),
        )
        .await
    {
        Ok(_) => true,
        Err(object_store::Error::NotImplemented) => false,
        Err(e) => {
            let _ = store.delete(probe_key).await;
            return Err(e);
        }
    };

    let _ = store.delete(probe_key).await;

    Ok(BucketCapabilities {
        conditional_put: create_rejects_duplicate && update_supported,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn coordinator_id_is_deterministic() {
        let key = [0xABu8; 32];
        assert_eq!(coordinator_id(&key), coordinator_id(&key));
    }

    #[test]
    fn coordinator_id_differs_per_key() {
        assert_ne!(coordinator_id(&[0xABu8; 32]), coordinator_id(&[0xCDu8; 32]));
    }

    #[test]
    fn coordinator_id_uses_domain_separation() {
        // A keyed_hash with a different context must not collide with
        // our derive_key — confirms we're using derive_key, not
        // keyed_hash with the empty context.
        let key = [0u8; 32];
        let id = coordinator_id(&key);
        let plain_keyed = blake3::keyed_hash(&key, b"").as_bytes().to_owned();
        assert_ne!(id, plain_keyed.as_slice());
    }

    #[test]
    fn format_coordinator_id_is_stable_ulid() {
        let id = coordinator_id(&[0u8; 32]);
        let s = format_coordinator_id(&id);
        assert_eq!(s.len(), 26, "ULID Base32 representation is 26 chars");
        ulid::Ulid::from_string(&s).expect("formatted id round-trips through Ulid::from_string");
    }

    #[tokio::test]
    async fn put_if_absent_creates_then_rejects_duplicate() {
        let store = InMemory::new();
        let key = StorePath::from("probe");

        put_if_absent(&store, &key, Bytes::from_static(b"v1"))
            .await
            .expect("first put_if_absent should succeed");

        let err = put_if_absent(&store, &key, Bytes::from_static(b"v2"))
            .await
            .expect_err("second put_if_absent should fail");
        assert!(matches!(err, ConditionalPutError::PreconditionFailed));
    }

    #[tokio::test]
    async fn put_with_match_succeeds_on_current_etag() {
        let store = InMemory::new();
        let key = StorePath::from("probe");

        let first = put_if_absent(&store, &key, Bytes::from_static(b"v1"))
            .await
            .expect("seed write");

        put_with_match(
            &store,
            &key,
            Bytes::from_static(b"v2"),
            UpdateVersion::from(first),
        )
        .await
        .expect("matching etag should succeed");
    }

    #[tokio::test]
    async fn put_with_match_fails_on_stale_etag() {
        let store = InMemory::new();
        let key = StorePath::from("probe");

        let first = put_if_absent(&store, &key, Bytes::from_static(b"v1"))
            .await
            .expect("seed write");

        store
            .put_opts(
                &key,
                Bytes::from_static(b"v2").into(),
                PutMode::Overwrite.into(),
            )
            .await
            .expect("overwrite to bump version");

        let err = put_with_match(
            &store,
            &key,
            Bytes::from_static(b"v3"),
            UpdateVersion::from(first),
        )
        .await
        .expect_err("stale etag should fail");
        assert!(matches!(err, ConditionalPutError::PreconditionFailed));
    }

    #[tokio::test]
    async fn put_with_match_fails_when_object_missing() {
        // PutMode::Update against a missing object normalises to
        // Precondition (per object_store docs). Confirm our wrapper
        // surfaces that as PreconditionFailed.
        let store = InMemory::new();
        let key = StorePath::from("probe");

        let err = put_with_match(
            &store,
            &key,
            Bytes::from_static(b"v1"),
            UpdateVersion {
                e_tag: Some("nonexistent".to_string()),
                version: None,
            },
        )
        .await
        .expect_err("update of missing object should fail");
        assert!(matches!(err, ConditionalPutError::PreconditionFailed));
    }

    #[tokio::test]
    async fn probe_reports_conditional_put_supported() {
        let store = InMemory::new();
        let key = StorePath::from("_capabilities/conditional-put-probe");

        let caps = probe_capabilities(&store, &key).await.expect("probe");
        assert!(caps.conditional_put);

        let err = store.head(&key).await.expect_err("probe key cleaned up");
        assert!(matches!(err, object_store::Error::NotFound { .. }));
    }

    #[tokio::test]
    async fn probe_is_idempotent() {
        let store = InMemory::new();
        let key = StorePath::from("_capabilities/conditional-put-probe");

        let a = probe_capabilities(&store, &key).await.expect("first probe");
        let b = probe_capabilities(&store, &key)
            .await
            .expect("second probe");
        assert_eq!(a, b);
    }
}
