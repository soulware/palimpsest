//! Conditional-PUT wrapper around `object_store::local::LocalFileSystem`.
//!
//! `LocalFileSystem` returns `NotImplemented` for `PutMode::Update` (and so
//! does the upstream impl as of object_store 0.13). Portable lifecycle
//! verbs require atomic If-Match read-modify-write on `names/<name>`, so
//! the OSS local-FS coordinator path needs that semantics filled in.
//!
//! Approach: serialize all conditional updates through an in-process mutex,
//! perform an explicit `head() → compare e_tag → put_opts(Overwrite)`
//! sequence under the lock, and translate mismatch into
//! `Error::Precondition`. This is sound for the OSS deployment shape
//! (per-host coordinator owns its local store; no cross-process writers
//! against the same prefix). All other operations delegate to the inner
//! store unchanged.
//!
//! `PutMode::Create` already works on `LocalFileSystem` natively (via
//! `link(2)` with `EEXIST`), so it bypasses the wrapper entirely.
//!
//! See `coordinator/src/portable.rs::probe_capabilities` for how
//! coordinator startup verifies the wrapped backend honours both modes.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, UpdateVersion,
};
use tokio::sync::Mutex;

/// Wraps a `LocalFileSystem` to fill in `PutMode::Update` semantics.
///
/// Single-host correctness assumption: only one coordinator process drives
/// updates against this prefix at a time. The mutex makes concurrent
/// updates from within this process serialize cleanly; cross-process races
/// are out of scope (and out of design, per per-host coordinator model).
#[derive(Debug)]
pub struct ConditionalLocalStore {
    inner: LocalFileSystem,
    update_lock: Mutex<()>,
}

impl ConditionalLocalStore {
    pub fn new(inner: LocalFileSystem) -> Self {
        Self {
            inner,
            update_lock: Mutex::new(()),
        }
    }

    async fn conditional_update(
        &self,
        location: &StorePath,
        payload: PutPayload,
        expected: UpdateVersion,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let _guard = self.update_lock.lock().await;

        let current = self.inner.head(location).await.map_err(|e| match e {
            object_store::Error::NotFound { path, source } => object_store::Error::Precondition {
                path,
                source: format!(
                    "conditional update on missing object (expected etag {:?}): {source}",
                    expected.e_tag
                )
                .into(),
            },
            other => other,
        })?;

        let etag_matches = match (&expected.e_tag, &current.e_tag) {
            (Some(want), Some(got)) => want == got,
            _ => false,
        };
        if !etag_matches {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: format!(
                    "etag mismatch (expected {:?}, found {:?})",
                    expected.e_tag, current.e_tag
                )
                .into(),
            });
        }

        let mut overwrite_opts = opts;
        overwrite_opts.mode = PutMode::Overwrite;
        self.inner.put_opts(location, payload, overwrite_opts).await
    }
}

impl std::fmt::Display for ConditionalLocalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

#[async_trait]
impl ObjectStore for ConditionalLocalStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if let PutMode::Update(expected) = opts.mode.clone() {
            return self
                .conditional_update(location, payload, expected, opts)
                .await;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &StorePath, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &StorePath, range: Range<usize>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[Range<usize>],
    ) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &StorePath) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &StorePath) -> Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &StorePath, to: &StorePath) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &StorePath, to: &StorePath) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename(&self, from: &StorePath, to: &StorePath) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &StorePath, to: &StorePath) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portable::probe_capabilities;

    fn build() -> (tempfile::TempDir, ConditionalLocalStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let inner = LocalFileSystem::new_with_prefix(dir.path()).expect("local fs");
        let store = ConditionalLocalStore::new(inner);
        (dir, store)
    }

    #[tokio::test]
    async fn capability_probe_passes_for_wrapped_local_fs() {
        let (_dir, store) = build();
        let probe_key = StorePath::from("__caps_probe__");
        let caps = probe_capabilities(&store, &probe_key).await.expect("probe");
        assert!(
            caps.conditional_put,
            "wrapped local FS must report conditional_put=true"
        );
    }

    #[tokio::test]
    async fn update_with_matching_etag_succeeds() {
        let (_dir, store) = build();
        let key = StorePath::from("rec");
        let v0 = store
            .put_opts(
                &key,
                Bytes::from_static(b"v0").into(),
                PutMode::Create.into(),
            )
            .await
            .expect("create");

        let v1 = store
            .put_opts(
                &key,
                Bytes::from_static(b"v1").into(),
                PutMode::Update(UpdateVersion::from(v0)).into(),
            )
            .await
            .expect("update with current etag");
        assert!(v1.e_tag.is_some());

        let body = store
            .get(&key)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("read");
        assert_eq!(body.as_ref(), b"v1");
    }

    #[tokio::test]
    async fn update_with_stale_etag_returns_precondition() {
        let (_dir, store) = build();
        let key = StorePath::from("rec");
        let v0 = store
            .put_opts(
                &key,
                Bytes::from_static(b"v0").into(),
                PutMode::Create.into(),
            )
            .await
            .expect("create");
        let stale = UpdateVersion::from(v0);

        // Bump the stored object so v0's etag is no longer current.
        store
            .put_opts(
                &key,
                Bytes::from_static(b"v1").into(),
                PutMode::Overwrite.into(),
            )
            .await
            .expect("overwrite");

        let err = store
            .put_opts(
                &key,
                Bytes::from_static(b"v2").into(),
                PutMode::Update(stale).into(),
            )
            .await
            .expect_err("stale update must be rejected");
        assert!(matches!(err, object_store::Error::Precondition { .. }));
    }

    #[tokio::test]
    async fn update_on_missing_object_returns_precondition() {
        let (_dir, store) = build();
        let key = StorePath::from("rec");
        let phantom = UpdateVersion {
            e_tag: Some("nonexistent".into()),
            version: None,
        };
        let err = store
            .put_opts(
                &key,
                Bytes::from_static(b"v0").into(),
                PutMode::Update(phantom).into(),
            )
            .await
            .expect_err("update on missing object must be rejected");
        assert!(matches!(err, object_store::Error::Precondition { .. }));
    }

    #[tokio::test]
    async fn create_still_uses_native_path() {
        let (_dir, store) = build();
        let key = StorePath::from("rec");
        store
            .put_opts(
                &key,
                Bytes::from_static(b"v0").into(),
                PutMode::Create.into(),
            )
            .await
            .expect("first create");
        let err = store
            .put_opts(
                &key,
                Bytes::from_static(b"v1").into(),
                PutMode::Create.into(),
            )
            .await
            .expect_err("second create must conflict");
        assert!(matches!(err, object_store::Error::AlreadyExists { .. }));
    }
}
