// Pull a readonly ancestor for a volume from S3 into the local data dir.
//
// A pulled ancestor is the minimal on-disk presence needed for the
// coordinator's prefetch task (and downstream code like `Volume::open`) to
// resolve a child's lineage:
//
//   by_id/<ulid>/
//     volume.readonly          — marker (ancestor, not user-managed)
//     volume.pub               — Ed25519 verifying key
//     volume.provenance        — signed lineage (parent + extent_index)
//     index/                   — empty, populated by `prefetch_indexes`
//
// Ancestors carry no `volume.toml` and no size: per
// `docs/design-volume-size-ownership.md` size lives only on
// `names/<name>` for the live volume, and ancestors are read-only segment
// containers reached through a child's LBA map.
//
// The coordinator uses this to auto-heal ancestor chains when a
// newly-discovered volume references a parent that isn't locally present
// (the "self-healing prefetch" path).
//
// Each of the two GETs (`volume.pub`, `volume.provenance`) is tried at
// the peer first when a [`PeerFetchContext`] is available, then falls
// through to S3 on any peer miss. See `docs/design-peer-segment-fetch.md`
// § "What's served" for the wire shape.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::{info, trace};
use ulid::Ulid;

use crate::prefetch::PeerFetchContext;

/// Pull a readonly ancestor for `volume_id` from the object store into
/// `<data_dir>/by_id/<volume_id>/`. If the directory already exists,
/// returns its path without re-pulling (idempotent — safe to call on every
/// prefetch tick).
///
/// Fetches:
///   - `by_id/<volume_id>/volume.pub`
///   - `by_id/<volume_id>/volume.provenance`
///
/// When `peer` is `Some(_)`, each of the two GETs is tried at the peer
/// first and falls through to S3 on miss. Both files are also peer-fetch
/// routes (`/v1/<vol_id>/volume.pub`, `/v1/<vol_id>/volume.provenance`).
///
/// Writes them into `<data_dir>/by_id/<volume_id>/` along with a
/// `volume.readonly` marker and an empty `index/` directory. No
/// `volume.toml` is written: ancestors carry no size, and the absent
/// `by_name/` symlink marks the entry as a pulled ancestor rather than a
/// user-managed volume.
///
/// Signature verification of the downloaded `volume.provenance` is *not*
/// performed here — the caller is responsible for verifying under the
/// pubkey it trusts (typically embedded in the child's `ParentRef`).
/// Doing verification here would require choosing a key, which the pull
/// API has no context to do safely.
pub async fn pull_volume_skeleton(
    store: &Arc<dyn ObjectStore>,
    data_dir: &Path,
    volume_id: &str,
    peer: Option<&PeerFetchContext>,
) -> Result<PathBuf> {
    let vol_dir = data_dir.join("by_id").join(volume_id);
    if vol_dir.exists() {
        return Ok(vol_dir);
    }

    let vol_ulid = Ulid::from_string(volume_id)
        .with_context(|| format!("parsing volume_id {volume_id} as ULID"))?;

    // Two independent GETs — fire concurrently so per-ancestor pull
    // latency is bounded by the slowest, not the sum. Each side tries
    // the peer first and falls through to S3 on any non-200 response.
    let pub_key = StorePath::from(format!("by_id/{volume_id}/volume.pub"));
    let provenance_key = StorePath::from(format!(
        "by_id/{volume_id}/{}",
        elide_core::signing::VOLUME_PROVENANCE_FILE
    ));
    let (pub_bytes, provenance_bytes) = tokio::try_join!(
        fetch_skeleton_file(
            store,
            &pub_key,
            SkeletonFile::VolumePub,
            volume_id,
            vol_ulid,
            peer,
        ),
        fetch_skeleton_file(
            store,
            &provenance_key,
            SkeletonFile::VolumeProvenance,
            volume_id,
            vol_ulid,
            peer,
        ),
    )?;

    std::fs::create_dir_all(&vol_dir).with_context(|| format!("creating {}", vol_dir.display()))?;
    std::fs::write(vol_dir.join("volume.readonly"), "")
        .with_context(|| format!("writing volume.readonly for {volume_id}"))?;
    std::fs::write(vol_dir.join("volume.pub"), &pub_bytes)
        .with_context(|| format!("writing volume.pub for {volume_id}"))?;
    std::fs::write(
        vol_dir.join(elide_core::signing::VOLUME_PROVENANCE_FILE),
        &provenance_bytes,
    )
    .with_context(|| format!("writing volume.provenance for {volume_id}"))?;
    std::fs::create_dir_all(vol_dir.join("index"))
        .with_context(|| format!("creating index/ for {volume_id}"))?;

    Ok(vol_dir)
}

#[derive(Clone, Copy)]
enum SkeletonFile {
    VolumePub,
    VolumeProvenance,
}

impl SkeletonFile {
    fn label(self) -> &'static str {
        match self {
            Self::VolumePub => "volume.pub",
            Self::VolumeProvenance => elide_core::signing::VOLUME_PROVENANCE_FILE,
        }
    }
}

/// Fetch one skeleton file, peer-first when a context is available,
/// falling through to S3 on any non-200 response.
async fn fetch_skeleton_file(
    store: &Arc<dyn ObjectStore>,
    key: &StorePath,
    kind: SkeletonFile,
    volume_id: &str,
    vol_ulid: Ulid,
    peer: Option<&PeerFetchContext>,
) -> Result<Bytes> {
    if let Some(peer_ctx) = peer
        && let Some(bytes) = peer_fetch_skeleton(peer_ctx, kind, vol_ulid).await
    {
        info!(
            "[pull] fetched {} from peer for {}",
            kind.label(),
            volume_id
        );
        return Ok(bytes);
    }
    fetch_bytes(store, key, kind.label(), volume_id).await
}

async fn peer_fetch_skeleton(
    peer: &PeerFetchContext,
    kind: SkeletonFile,
    vol_ulid: Ulid,
) -> Option<Bytes> {
    let bytes = match kind {
        SkeletonFile::VolumePub => {
            peer.client
                .fetch_volume_pub(&peer.endpoint, &peer.volume_name, vol_ulid)
                .await
        }
        SkeletonFile::VolumeProvenance => {
            peer.client
                .fetch_volume_provenance(&peer.endpoint, &peer.volume_name, vol_ulid)
                .await
        }
    };
    if bytes.is_none() {
        trace!(
            "[pull] peer miss for {} of {vol_ulid}; falling through to S3",
            kind.label()
        );
    }
    bytes
}

async fn fetch_bytes(
    store: &Arc<dyn ObjectStore>,
    key: &StorePath,
    what: &str,
    volume_id: &str,
) -> Result<bytes::Bytes> {
    let resp = store
        .get(key)
        .await
        .with_context(|| format!("downloading {what} for {volume_id}"))?;
    resp.bytes()
        .await
        .with_context(|| format!("reading {what} for {volume_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use elide_peer_fetch::PeerEndpoint;
    use object_store::memory::InMemory;
    use std::sync::Arc as StdArc;
    use tempfile::TempDir;

    /// Stub signer used in the unreachable-peer fallback test.
    /// Doesn't need to verify against anything — the peer will refuse
    /// to connect before any token is checked.
    #[derive(Debug)]
    struct StubSigner;
    impl elide_peer_fetch::TokenSigner for StubSigner {
        fn coordinator_id(&self) -> &str {
            "stub-coord"
        }
        fn sign(&self, _msg: &[u8]) -> [u8; 64] {
            [0u8; 64]
        }
    }

    /// No peer context: pull_volume_skeleton goes straight to S3 and
    /// produces the expected on-disk layout. Regression for the path
    /// every caller exercised before peer-fetch was added.
    #[tokio::test]
    async fn skeleton_pull_falls_back_to_s3_with_no_peer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let vol_id = vol_ulid.to_string();

        let pub_bytes = b"pub-bytes\n";
        let prov_bytes = b"prov-bytes\n";
        store
            .put(
                &StorePath::from(format!("by_id/{vol_id}/volume.pub")),
                bytes::Bytes::from_static(pub_bytes).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{vol_id}/volume.provenance")),
                bytes::Bytes::from_static(prov_bytes).into(),
            )
            .await
            .unwrap();

        let tmp = TempDir::new().unwrap();
        let vol_dir = pull_volume_skeleton(&store, tmp.path(), &vol_id, None)
            .await
            .unwrap();

        assert!(vol_dir.join("volume.readonly").exists());
        assert_eq!(
            std::fs::read(vol_dir.join("volume.pub")).unwrap(),
            pub_bytes
        );
        assert_eq!(
            std::fs::read(vol_dir.join("volume.provenance")).unwrap(),
            prov_bytes
        );
        assert!(vol_dir.join("index").is_dir());
    }

    /// Peer context pointing at an unreachable port (ECONNREFUSED). The
    /// skeleton pull must collapse cleanly to S3 and still write the
    /// canonical on-disk layout. Mirrors the
    /// `peer_fetch_idx_unreachable_returns_error` shape from prefetch.rs:
    /// stubs out the signer because the peer never accepts the
    /// connection, so signature verification is never reached.
    #[tokio::test]
    async fn skeleton_pull_with_unreachable_peer_falls_back_to_s3() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let vol_id = vol_ulid.to_string();
        let pub_bytes = b"x";
        let prov_bytes = b"y";
        store
            .put(
                &StorePath::from(format!("by_id/{vol_id}/volume.pub")),
                bytes::Bytes::from_static(pub_bytes).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{vol_id}/volume.provenance")),
                bytes::Bytes::from_static(prov_bytes).into(),
            )
            .await
            .unwrap();

        let signer: StdArc<dyn elide_peer_fetch::TokenSigner> = StdArc::new(StubSigner);
        let client = elide_peer_fetch::PeerFetchClient::builder(signer)
            .request_timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap();
        let peer = PeerFetchContext {
            client,
            endpoint: PeerEndpoint::new("127.0.0.1".to_owned(), 1),
            volume_name: "any".to_owned(),
        };

        let tmp = TempDir::new().unwrap();
        let vol_dir = pull_volume_skeleton(&store, tmp.path(), &vol_id, Some(&peer))
            .await
            .unwrap();

        assert_eq!(
            std::fs::read(vol_dir.join("volume.pub")).unwrap(),
            pub_bytes
        );
        assert_eq!(
            std::fs::read(vol_dir.join("volume.provenance")).unwrap(),
            prov_bytes
        );
    }
}
