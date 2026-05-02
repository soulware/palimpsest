//! ObjectStore-backed ancestry walker for the peer-fetch auth pipeline.
//!
//! Given a starting `vol_ulid`, walks the signed `volume.provenance`
//! parent chain in S3 and returns the set of fork ULIDs that make up
//! the volume's ancestry (including the starting volume itself).
//!
//! Trust shape mirrors the local-filesystem walker in
//! `elide-core::signing` and the read-path walker in
//! `elide-coordinator::prefetch`:
//!
//! - The starting volume's `volume.provenance` is verified against
//!   `volume.pub` sitting in the same `by_id/<vol_ulid>/` prefix on S3.
//! - Each ancestor step is verified against the `parent_pubkey`
//!   embedded in the *child's* signed provenance — never against the
//!   `volume.pub` sitting in the ancestor's prefix. This is the same
//!   trust anchoring used at volume open time.
//!
//! The walk follows the fork-parent chain only. `extent_index`
//! ancestors do not contribute to peer-fetch ancestry: they're a
//! dedup-source relationship, not a "this volume can read those
//! segments" relationship. Peer-fetch authorisation matches what the
//! S3 IAM layer will eventually enforce — the volume's own prefix and
//! its fork-parent prefixes — so the walk is fork-only.

use std::collections::HashSet;
use std::io;

use ed25519_dalek::VerifyingKey;
use elide_core::signing::{VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE, verify_lineage_with_key};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use ulid::Ulid;

/// Walk the fork-parent ancestry of `starting_vol_ulid` against
/// `store`. Returns the set of fork ULIDs in the ancestry, including
/// `starting_vol_ulid` itself.
///
/// Each step is signature-verified — failures bubble up as
/// `io::Error`, so callers can map them to 401/403 responses.
pub async fn walk_ancestry(
    store: &dyn ObjectStore,
    starting_vol_ulid: Ulid,
) -> io::Result<HashSet<Ulid>> {
    let mut set = HashSet::new();
    set.insert(starting_vol_ulid);

    // Trust anchor for the starting volume: its own `volume.pub` on S3.
    let mut current_ulid = starting_vol_ulid;
    let mut current_vk = load_volume_pub(store, &current_ulid).await?;

    loop {
        let lineage = load_provenance(store, &current_ulid, &current_vk).await?;
        let Some(parent) = lineage.parent else {
            break;
        };

        let parent_ulid = Ulid::from_string(&parent.volume_ulid).map_err(|e| {
            io::Error::other(format!(
                "provenance for {current_ulid}: parent ulid not parseable: {e}"
            ))
        })?;

        // The child's provenance commits the parent's pubkey; that's the
        // trust anchor for the parent's own provenance, not whatever
        // `volume.pub` happens to sit in the parent's prefix.
        let parent_vk = VerifyingKey::from_bytes(&parent.pubkey).map_err(|e| {
            io::Error::other(format!(
                "provenance for {current_ulid}: parent pubkey invalid: {e}"
            ))
        })?;

        if !set.insert(parent_ulid) {
            // Cycle in the parent chain — provenance is supposed to be a
            // DAG rooted at a fresh volume. Treat as data corruption.
            return Err(io::Error::other(format!(
                "ancestry cycle detected at {parent_ulid}"
            )));
        }

        current_ulid = parent_ulid;
        current_vk = parent_vk;
    }

    Ok(set)
}

async fn load_volume_pub(store: &dyn ObjectStore, vol_ulid: &Ulid) -> io::Result<VerifyingKey> {
    let key = StorePath::from(format!("by_id/{vol_ulid}/{VOLUME_PUB_FILE}"));
    let body = store
        .get(&key)
        .await
        .map_err(|e| io::Error::other(format!("fetch {VOLUME_PUB_FILE} for {vol_ulid}: {e}")))?
        .bytes()
        .await
        .map_err(|e| {
            io::Error::other(format!("read {VOLUME_PUB_FILE} body for {vol_ulid}: {e}"))
        })?;
    let text = std::str::from_utf8(&body).map_err(|e| {
        io::Error::other(format!("{VOLUME_PUB_FILE} for {vol_ulid} not utf-8: {e}"))
    })?;
    parse_pub_hex(text.trim())
        .map_err(|e| io::Error::other(format!("{VOLUME_PUB_FILE} for {vol_ulid} invalid: {e}")))
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

async fn load_provenance(
    store: &dyn ObjectStore,
    vol_ulid: &Ulid,
    verifying_key: &VerifyingKey,
) -> io::Result<elide_core::signing::ProvenanceLineage> {
    let key = StorePath::from(format!("by_id/{vol_ulid}/{VOLUME_PROVENANCE_FILE}"));
    let body = store
        .get(&key)
        .await
        .map_err(|e| {
            io::Error::other(format!(
                "fetch {VOLUME_PROVENANCE_FILE} for {vol_ulid}: {e}"
            ))
        })?
        .bytes()
        .await
        .map_err(|e| {
            io::Error::other(format!(
                "read {VOLUME_PROVENANCE_FILE} body for {vol_ulid}: {e}"
            ))
        })?;
    let text = std::str::from_utf8(&body).map_err(|e| {
        io::Error::other(format!(
            "{VOLUME_PROVENANCE_FILE} for {vol_ulid} not utf-8: {e}"
        ))
    })?;
    verify_lineage_with_key(text, verifying_key, VOLUME_PROVENANCE_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ed25519_dalek::SigningKey;
    use elide_core::signing::{ParentRef, ProvenanceLineage, write_provenance};
    use object_store::memory::InMemory;
    use rand_core::OsRng;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
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

    /// Mint a `volume.provenance` for a synthetic volume and publish it
    /// (plus `volume.pub`) under `by_id/<ulid>/` on the given store.
    async fn publish_volume(
        store: &dyn ObjectStore,
        ulid: Ulid,
        key: &SigningKey,
        parent: Option<ParentRef>,
    ) {
        // Use elide-core's signer by writing to a tempdir then reading
        // back — keeps the on-disk format identical without re-implementing
        // the writer.
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join(VOLUME_PUB_FILE), pub_hex(key)).unwrap();
        let lineage = ProvenanceLineage {
            parent,
            extent_index: Vec::new(),
            oci_source: None,
        };
        write_provenance(tmp.path(), key, VOLUME_PROVENANCE_FILE, &lineage).unwrap();

        let pub_bytes = std::fs::read(tmp.path().join(VOLUME_PUB_FILE)).unwrap();
        let prov_bytes = std::fs::read(tmp.path().join(VOLUME_PROVENANCE_FILE)).unwrap();

        store
            .put(
                &StorePath::from(format!("by_id/{ulid}/{VOLUME_PUB_FILE}")),
                Bytes::from(pub_bytes).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{ulid}/{VOLUME_PROVENANCE_FILE}")),
                Bytes::from(prov_bytes).into(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn root_volume_returns_singleton() {
        let store = store();
        let key = SigningKey::generate(&mut OsRng);
        let ulid = Ulid::new();
        publish_volume(store.as_ref(), ulid, &key, None).await;

        let set = walk_ancestry(store.as_ref(), ulid).await.unwrap();
        assert_eq!(set.len(), 1);
        assert!(set.contains(&ulid));
    }

    #[tokio::test]
    async fn fork_chain_walks_to_root() {
        let store = store();
        let root_key = SigningKey::generate(&mut OsRng);
        let mid_key = SigningKey::generate(&mut OsRng);
        let leaf_key = SigningKey::generate(&mut OsRng);

        let root_ulid = Ulid::new();
        let mid_ulid = Ulid::new();
        let leaf_ulid = Ulid::new();

        publish_volume(store.as_ref(), root_ulid, &root_key, None).await;
        publish_volume(
            store.as_ref(),
            mid_ulid,
            &mid_key,
            Some(ParentRef {
                volume_ulid: root_ulid.to_string(),
                snapshot_ulid: Ulid::new().to_string(),
                pubkey: root_key.verifying_key().to_bytes(),
                manifest_pubkey: None,
            }),
        )
        .await;
        publish_volume(
            store.as_ref(),
            leaf_ulid,
            &leaf_key,
            Some(ParentRef {
                volume_ulid: mid_ulid.to_string(),
                snapshot_ulid: Ulid::new().to_string(),
                pubkey: mid_key.verifying_key().to_bytes(),
                manifest_pubkey: None,
            }),
        )
        .await;

        let set = walk_ancestry(store.as_ref(), leaf_ulid).await.unwrap();
        assert_eq!(set.len(), 3);
        assert!(set.contains(&leaf_ulid));
        assert!(set.contains(&mid_ulid));
        assert!(set.contains(&root_ulid));
    }

    #[tokio::test]
    async fn missing_provenance_is_error() {
        let store = store();
        let ulid = Ulid::new();
        let err = walk_ancestry(store.as_ref(), ulid)
            .await
            .expect_err("absent");
        let msg = err.to_string();
        assert!(
            msg.contains("volume.pub") || msg.contains("not found"),
            "msg={msg}"
        );
    }

    #[tokio::test]
    async fn tampered_pubkey_in_provenance_fails_at_parent_step() {
        // Child's provenance commits a pubkey that doesn't match the parent's
        // actual signing key — the parent step's verification fails.
        let store = store();
        let root_key = SigningKey::generate(&mut OsRng);
        let leaf_key = SigningKey::generate(&mut OsRng);
        let imposter = SigningKey::generate(&mut OsRng);

        let root_ulid = Ulid::new();
        let leaf_ulid = Ulid::new();

        publish_volume(store.as_ref(), root_ulid, &root_key, None).await;
        // Leaf claims `imposter` as the parent's pubkey, but the published
        // provenance for `root_ulid` is signed by `root_key`. The walk
        // verifies parent provenance against the embedded pubkey, so this
        // mismatch is caught at the parent step.
        publish_volume(
            store.as_ref(),
            leaf_ulid,
            &leaf_key,
            Some(ParentRef {
                volume_ulid: root_ulid.to_string(),
                snapshot_ulid: Ulid::new().to_string(),
                pubkey: imposter.verifying_key().to_bytes(),
                manifest_pubkey: None,
            }),
        )
        .await;

        let err = walk_ancestry(store.as_ref(), leaf_ulid)
            .await
            .expect_err("imposter pubkey");
        assert!(err.to_string().contains("signature invalid"), "got {err}");
    }
}
