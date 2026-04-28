//! Force-release recovery: list S3 segments under a dead fork, verify
//! each one's signature, and return the recoverable set.
//!
//! Used by `volume release --force` (Phase 3 of the portable-live-volume
//! rollout) to synthesise a fresh handoff snapshot from segments
//! observable in S3 when the previous owner is unreachable. The
//! recovering coordinator:
//!
//!   1. Fetches the dead fork's `volume.pub` from S3
//!      ([`fetch_volume_pub`]).
//!   2. Lists every object under `by_id/<dead_vol>/segments/`,
//!      range-fetches each segment's `header + index` section, and
//!      verifies its Ed25519 signature against the dead fork's
//!      `volume.pub` ([`list_and_verify_segments`]).
//!   3. Mints a synthesised handoff snapshot naming the verified
//!      segment set, signed by the recovering coordinator's
//!      `coordinator.key` (next task — not in this module).
//!
//! Segments that fail verification are dropped with a per-segment
//! `tracing::warn!`. A summary count is returned to the caller.
//!
//! The data-loss boundary is "writes the dead owner accepted but never
//! promoted to S3" — identical to the crash-recovery contract
//! elsewhere in the system.

use std::sync::Arc;

use anyhow::{Context, Result};
use ed25519_dalek::VerifyingKey;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::warn;
use ulid::Ulid;

use elide_core::segment::{HEADER_LEN, MAGIC};

/// One segment from the dead fork's S3 prefix that passed signature
/// verification.
#[derive(Debug, Clone)]
pub struct VerifiedSegment {
    /// ULID of the segment (the last path component of the S3 key).
    pub segment_ulid: Ulid,
    /// S3 ETag, if the backend reports one. Forwarded for diagnostics
    /// only; the synthesised snapshot manifest is keyed by ULID, not
    /// ETag.
    pub etag: Option<String>,
    /// Object size in bytes as reported by S3 list.
    pub size: u64,
}

/// Outcome of [`list_and_verify_segments`].
#[derive(Debug, Default)]
pub struct RecoveredSegments {
    /// Verified segments, sorted by ULID (= chronological order).
    pub segments: Vec<VerifiedSegment>,
    /// Count of segment-shaped objects that were dropped because they
    /// failed signature verification, had bad magic, or were
    /// truncated. Distinct from non-segment objects (e.g. stray
    /// `.tmp` files), which are silently skipped.
    pub dropped: usize,
}

/// Fetch and parse `by_id/<vol_ulid>/volume.pub` from the bucket.
///
/// The file is the same `lowercase-hex(pub_bytes) + "\n"` shape used
/// for the on-disk `volume.pub`.
pub async fn fetch_volume_pub(
    store: &Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
) -> Result<VerifyingKey> {
    let key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
    let result = store
        .get(&key)
        .await
        .with_context(|| format!("fetching volume.pub for {vol_ulid}"))?;
    let bytes = result
        .bytes()
        .await
        .with_context(|| format!("reading volume.pub bytes for {vol_ulid}"))?;
    let hex = std::str::from_utf8(&bytes)
        .map_err(|e| anyhow::anyhow!("volume.pub for {vol_ulid} not valid utf-8: {e}"))?
        .trim();
    parse_hex_pubkey(hex).with_context(|| format!("parsing volume.pub for {vol_ulid}"))
}

fn parse_hex_pubkey(hex: &str) -> Result<VerifyingKey> {
    if hex.len() != 64 {
        anyhow::bail!(
            "expected 64 hex chars for Ed25519 pubkey, got {}",
            hex.len()
        );
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| anyhow::anyhow!("invalid hex at position {}", i * 2))?;
    }
    VerifyingKey::from_bytes(&bytes).map_err(|e| anyhow::anyhow!("invalid Ed25519 pubkey: {e}"))
}

/// List every segment under `by_id/<vol_ulid>/segments/`, verify each
/// one's signature against `verifying_key`, and return the set of
/// verified segments sorted by ULID.
///
/// Verification is the same as the regular fetch path
/// (`prefetch::fetch_idx`): two range-GETs per segment (header, then
/// `[0, body_section_start)`) and a call to
/// [`elide_core::segment::verify_segment_bytes`]. The body bytes are
/// not fetched.
///
/// Objects whose key doesn't parse as a ULID (e.g. stray `.tmp` files
/// from interrupted uploads, the `segments/` directory marker on some
/// backends) are silently skipped — they aren't segment uploads. A
/// segment-shaped object whose verification fails is dropped with a
/// `warn!` and counted in `RecoveredSegments::dropped`.
pub async fn list_and_verify_segments(
    store: &Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
    verifying_key: &VerifyingKey,
) -> Result<RecoveredSegments> {
    let prefix = StorePath::from(format!("by_id/{vol_ulid}/segments/"));
    let objects: Vec<_> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .with_context(|| format!("listing by_id/{vol_ulid}/segments/"))?;

    let mut out = RecoveredSegments::default();

    for obj in objects {
        let Some(filename) = obj.location.filename() else {
            continue;
        };
        let Ok(segment_ulid) = Ulid::from_string(filename) else {
            // Not a segment upload — could be a stray .tmp, a directory
            // placeholder, or some other artefact. Don't count as a
            // recovery failure.
            continue;
        };

        match verify_one_segment(store, &obj.location, filename, verifying_key).await {
            Ok(()) => {
                out.segments.push(VerifiedSegment {
                    segment_ulid,
                    etag: obj.e_tag.clone(),
                    size: obj.size as u64,
                });
            }
            Err(e) => {
                warn!(
                    "[recovery] dropping segment {filename} from {vol_ulid}: verification failed ({e:#})"
                );
                out.dropped += 1;
            }
        }
    }

    out.segments.sort_by_key(|v| v.segment_ulid);
    Ok(out)
}

/// Range-fetch header + index section for one segment and verify the
/// signature. Body bytes are not fetched. Mirrors the verification
/// step of `prefetch::fetch_idx`.
async fn verify_one_segment(
    store: &Arc<dyn ObjectStore>,
    key: &StorePath,
    segment_id: &str,
    verifying_key: &VerifyingKey,
) -> Result<()> {
    // Header first, to learn how big the index section is.
    let header = store
        .get_range(key, 0..HEADER_LEN as usize)
        .await
        .with_context(|| format!("fetching header for {segment_id}"))?;

    if header.len() < HEADER_LEN as usize {
        anyhow::bail!("header too short ({} bytes)", header.len());
    }
    if &header[..8] != MAGIC {
        anyhow::bail!("bad segment magic");
    }
    let index_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    let inline_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    let body_section_start = HEADER_LEN as usize + index_length as usize + inline_length as usize;

    // Header + index + inline. Inline isn't part of the signing input
    // but is bundled in the same prefix — fetching it here matches the
    // existing prefetch path and keeps the range count stable across
    // verification flavours.
    let idx_bytes = store
        .get_range(key, 0..body_section_start)
        .await
        .with_context(|| format!("fetching index section for {segment_id}"))?;

    elide_core::segment::verify_segment_bytes(&idx_bytes, segment_id, verifying_key)
        .with_context(|| format!("verifying signature for {segment_id}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use elide_core::segment::{SegmentEntry, SegmentFlags, SegmentSigner};
    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use rand_core::OsRng;

    fn encode_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Test signer wrapping an Ed25519 SigningKey.
    struct TestSigner {
        key: SigningKey,
    }
    impl SegmentSigner for TestSigner {
        fn sign(&self, msg: &[u8]) -> [u8; 64] {
            use ed25519_dalek::Signer;
            self.key.sign(msg).to_bytes()
        }
    }

    fn make_signer() -> (Arc<TestSigner>, VerifyingKey) {
        let key = SigningKey::generate(&mut OsRng);
        let vk = key.verifying_key();
        (Arc::new(TestSigner { key }), vk)
    }

    fn seg_entry(start_lba: u64, len: u32, body: &[u8]) -> SegmentEntry {
        let hash = blake3::hash(body);
        SegmentEntry::new_data(hash, start_lba, len, SegmentFlags::empty(), body.to_vec())
    }

    /// Build an in-memory segment file (header + index + inline + body)
    /// the same way `segment::write_segment` would, then return its
    /// raw bytes for upload via `put`.
    fn build_segment_bytes(signer: &dyn SegmentSigner, entries: Vec<SegmentEntry>) -> Vec<u8> {
        // `write_segment` uses `create_new(true)`, so the path must
        // not exist yet — write to a fresh path inside a tempdir.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg");
        let mut entries_vec = entries;
        elide_core::segment::write_segment(&path, &mut entries_vec, signer).unwrap();
        std::fs::read(&path).unwrap()
    }

    fn upload_volume_pub(
        store: &Arc<dyn ObjectStore>,
        vol_ulid: Ulid,
        vk: &VerifyingKey,
    ) -> impl std::future::Future<Output = ()> + use<> {
        let key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
        let payload = format!("{}\n", encode_hex(&vk.to_bytes()));
        let store = store.clone();
        async move {
            store
                .put(&key, PutPayload::from(payload.into_bytes()))
                .await
                .unwrap();
        }
    }

    fn segment_key(vol_ulid: Ulid, seg_ulid: Ulid) -> StorePath {
        // Mirror upload::segment_key without the date sharding (the
        // recovery code lists the whole segments/ prefix anyway).
        StorePath::from(format!("by_id/{vol_ulid}/segments/{seg_ulid}"))
    }

    #[tokio::test]
    async fn happy_path_returns_sorted_verified_segments() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let (signer, vk) = make_signer();
        upload_volume_pub(&store, vol_ulid, &vk).await;

        // Three segments minted in increasing-ULID order, uploaded in
        // a different order. We expect the result sorted by ULID.
        let mut mint = elide_core::ulid_mint::UlidMint::new(Ulid::nil());
        let s1 = mint.next();
        let s2 = mint.next();
        let s3 = mint.next();
        for (sid, payload) in [(s2, b"two".as_slice()), (s1, b"one"), (s3, b"three")] {
            let bytes = build_segment_bytes(signer.as_ref(), vec![seg_entry(0, 1, payload)]);
            store
                .put(&segment_key(vol_ulid, sid), PutPayload::from(bytes))
                .await
                .unwrap();
        }

        let got = list_and_verify_segments(&store, vol_ulid, &vk)
            .await
            .unwrap();
        assert_eq!(got.dropped, 0);
        let ulids: Vec<_> = got.segments.iter().map(|v| v.segment_ulid).collect();
        assert_eq!(ulids, vec![s1, s2, s3]);
        for v in &got.segments {
            assert!(v.size > 0, "segment size populated");
        }
    }

    #[tokio::test]
    async fn empty_prefix_returns_empty_no_error() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_, vk) = make_signer();
        let got = list_and_verify_segments(&store, Ulid::new(), &vk)
            .await
            .unwrap();
        assert!(got.segments.is_empty());
        assert_eq!(got.dropped, 0);
    }

    #[tokio::test]
    async fn tampered_segment_is_dropped_others_kept() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let (signer, vk) = make_signer();
        upload_volume_pub(&store, vol_ulid, &vk).await;

        let mut mint = elide_core::ulid_mint::UlidMint::new(Ulid::nil());
        let good_id = mint.next();
        let bad_id = mint.next();
        let good = build_segment_bytes(signer.as_ref(), vec![seg_entry(0, 1, b"good")]);
        let mut bad = build_segment_bytes(signer.as_ref(), vec![seg_entry(0, 1, b"bad")]);
        // Flip a byte inside the index section so the signature check
        // fails. Header is 100 bytes; the first index entry sits at
        // offset 100 onwards.
        let tamper_offset = HEADER_LEN as usize + 4;
        bad[tamper_offset] ^= 0xff;

        store
            .put(&segment_key(vol_ulid, good_id), PutPayload::from(good))
            .await
            .unwrap();
        store
            .put(&segment_key(vol_ulid, bad_id), PutPayload::from(bad))
            .await
            .unwrap();

        let got = list_and_verify_segments(&store, vol_ulid, &vk)
            .await
            .unwrap();
        assert_eq!(got.dropped, 1, "tampered segment must be dropped");
        let kept: Vec<_> = got.segments.iter().map(|v| v.segment_ulid).collect();
        assert_eq!(kept, vec![good_id]);
    }

    #[tokio::test]
    async fn bad_magic_segment_is_dropped() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let (_, vk) = make_signer();
        upload_volume_pub(&store, vol_ulid, &vk).await;

        // Object whose key parses as a ULID but whose contents are not
        // a segment file.
        let bogus = vec![0u8; 200];
        let bogus_id = Ulid::new();
        store
            .put(&segment_key(vol_ulid, bogus_id), PutPayload::from(bogus))
            .await
            .unwrap();

        let got = list_and_verify_segments(&store, vol_ulid, &vk)
            .await
            .unwrap();
        assert_eq!(got.dropped, 1);
        assert!(got.segments.is_empty());
    }

    #[tokio::test]
    async fn non_ulid_keys_are_silently_skipped() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let (signer, vk) = make_signer();
        upload_volume_pub(&store, vol_ulid, &vk).await;

        // A real segment.
        let mut mint = elide_core::ulid_mint::UlidMint::new(Ulid::nil());
        let seg_id = mint.next();
        let good = build_segment_bytes(signer.as_ref(), vec![seg_entry(0, 1, b"x")]);
        store
            .put(&segment_key(vol_ulid, seg_id), PutPayload::from(good))
            .await
            .unwrap();

        // A leftover .tmp upload (not a ULID key).
        let tmp_key = StorePath::from(format!("by_id/{vol_ulid}/segments/{}.tmp", mint.next()));
        store
            .put(&tmp_key, PutPayload::from(b"partial-upload".as_slice()))
            .await
            .unwrap();

        let got = list_and_verify_segments(&store, vol_ulid, &vk)
            .await
            .unwrap();
        // The .tmp object was silently skipped, not counted as dropped.
        assert_eq!(got.dropped, 0);
        assert_eq!(got.segments.len(), 1);
        assert_eq!(got.segments[0].segment_ulid, seg_id);
    }

    #[tokio::test]
    async fn segment_signed_by_wrong_key_is_dropped() {
        // The dead fork's volume.pub does not match the key that
        // actually signed a segment — hostile injection scenario, or
        // a stale segment from a previous incarnation of the prefix.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let (advertised_signer, advertised_vk) = make_signer();
        let (other_signer, _) = make_signer();
        upload_volume_pub(&store, vol_ulid, &advertised_vk).await;

        let mut mint = elide_core::ulid_mint::UlidMint::new(Ulid::nil());
        let good_id = mint.next();
        let foreign_id = mint.next();
        let good = build_segment_bytes(advertised_signer.as_ref(), vec![seg_entry(0, 1, b"legit")]);
        let foreign = build_segment_bytes(other_signer.as_ref(), vec![seg_entry(0, 1, b"foreign")]);

        store
            .put(&segment_key(vol_ulid, good_id), PutPayload::from(good))
            .await
            .unwrap();
        store
            .put(
                &segment_key(vol_ulid, foreign_id),
                PutPayload::from(foreign),
            )
            .await
            .unwrap();

        let got = list_and_verify_segments(&store, vol_ulid, &advertised_vk)
            .await
            .unwrap();
        assert_eq!(got.dropped, 1, "foreign-signed segment must be dropped");
        let kept: Vec<_> = got.segments.iter().map(|v| v.segment_ulid).collect();
        assert_eq!(kept, vec![good_id]);
    }

    #[tokio::test]
    async fn fetch_volume_pub_round_trips_via_bucket() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let (_, vk) = make_signer();
        upload_volume_pub(&store, vol_ulid, &vk).await;

        let got = fetch_volume_pub(&store, vol_ulid).await.unwrap();
        assert_eq!(got.to_bytes(), vk.to_bytes());
    }

    #[tokio::test]
    async fn fetch_volume_pub_rejects_malformed_hex() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
        store
            .put(&key, PutPayload::from(b"not hex\n".as_slice()))
            .await
            .unwrap();
        assert!(fetch_volume_pub(&store, vol_ulid).await.is_err());
    }
}
