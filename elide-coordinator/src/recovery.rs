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
//!      `coordinator.key`, and publishes it via conditional create
//!      ([`mint_and_publish_synthesised_snapshot`]).
//!
//! Segments that fail verification are dropped with a per-segment
//! `tracing::warn!`. A summary count is returned to the caller.
//!
//! The data-loss boundary is "writes the dead owner accepted but never
//! promoted to S3" — identical to the crash-recovery contract
//! elsewhere in the system.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::warn;
use ulid::Ulid;

use elide_core::segment::{HEADER_LEN, MAGIC, SegmentSigner};
use elide_core::signing::{
    SnapshotManifest, SnapshotManifestRecovery, build_snapshot_manifest_bytes,
    peek_snapshot_manifest_recovery, read_snapshot_manifest_from_bytes,
};

use crate::portable::{self, ConditionalPutError};
use crate::upload::snapshot_manifest_key;

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

/// Like [`fetch_volume_pub`] but returns `Ok(None)` when the object is
/// absent rather than an error.
///
/// Used by `volume release --force` to recover the corruption window
/// where `names/<name>` was published before `volume.pub`. With no
/// `volume.pub` in the bucket, no segments could ever have been
/// signed-and-verified under that key, so the dead fork is provably
/// empty: force-release publishes an empty synthesised handoff signed
/// by the recovering coordinator's identity.
pub async fn fetch_volume_pub_optional(
    store: &Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
) -> Result<Option<VerifyingKey>> {
    let key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
    match store.get(&key).await {
        Ok(result) => {
            let bytes = result
                .bytes()
                .await
                .with_context(|| format!("reading volume.pub bytes for {vol_ulid}"))?;
            let hex = std::str::from_utf8(&bytes)
                .map_err(|e| anyhow::anyhow!("volume.pub for {vol_ulid} not valid utf-8: {e}"))?
                .trim();
            parse_hex_pubkey(hex)
                .with_context(|| format!("parsing volume.pub for {vol_ulid}"))
                .map(Some)
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow::Error::new(e).context(format!("fetching volume.pub for {vol_ulid}"))),
    }
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

/// Outcome of [`mint_and_publish_synthesised_snapshot`].
#[derive(Debug)]
pub struct PublishedSynthesisedSnapshot {
    /// Freshly-minted ULID of the synthesised snapshot.
    pub snap_ulid: Ulid,
    /// Full S3 key the manifest was published to. Useful for
    /// subsequent reads or for surfacing in operator output.
    pub key: StorePath,
}

/// Errors from [`mint_and_publish_synthesised_snapshot`].
#[derive(Debug)]
pub enum PublishSnapshotError {
    /// Bucket-side conditional PUT refused — a manifest already exists
    /// at the freshly-minted snapshot ULID. Vanishingly improbable
    /// (it would require ULID collision against an unrelated prior
    /// write), but handled cleanly so a retry can mint a different
    /// ULID.
    AlreadyExists { key: StorePath },
    /// Underlying object-store or signing failure.
    Other(anyhow::Error),
}

impl std::fmt::Display for PublishSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists { key } => {
                write!(f, "synthesised snapshot already exists at {key}")
            }
            Self::Other(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for PublishSnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AlreadyExists { .. } => None,
            Self::Other(e) => Some(e.as_ref()),
        }
    }
}

impl From<anyhow::Error> for PublishSnapshotError {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}

/// Mint a fresh ULID, build a synthesised handoff snapshot manifest
/// naming `segment_ulids` and signed by `coord_signer`, and publish
/// it to `by_id/<dead_vol_ulid>/snapshots/<YYYYMMDD>/<snap>.manifest`
/// via conditional create.
///
/// `coord_id` populates the manifest's `recovering_coordinator_id`
/// field so verifiers can resolve it back to a pubkey via
/// `coordinators/<coord_id>/coordinator.pub`. `recovered_at` is set
/// to the current wall-clock time in RFC3339.
///
/// The signing input is domain-separated (see
/// `elide_core::signing` § "Recovery manifests"), so the resulting
/// signature lives in a different space from regular volume-signed
/// manifests — a verifier that loads this manifest with the wrong
/// key (or that swaps the recovery metadata) will fail verification
/// rather than silently accept it.
pub async fn mint_and_publish_synthesised_snapshot(
    store: &Arc<dyn ObjectStore>,
    dead_vol_ulid: Ulid,
    segment_ulids: &[Ulid],
    coord_signer: &dyn SegmentSigner,
    coord_id: &str,
) -> Result<PublishedSynthesisedSnapshot, PublishSnapshotError> {
    let snap_ulid = Ulid::new();
    let recovery = SnapshotManifestRecovery {
        recovering_coordinator_id: coord_id.to_owned(),
        recovered_at: chrono::Utc::now().to_rfc3339(),
    };

    let bytes = build_snapshot_manifest_bytes(coord_signer, segment_ulids, Some(&recovery));

    // Reuse the existing key shape so this manifest sits next to any
    // historical snapshots under the dead fork's prefix.
    let dead_vol_str = dead_vol_ulid.to_string();
    let key = snapshot_manifest_key(&dead_vol_str, snap_ulid);

    match portable::put_if_absent(store.as_ref(), &key, Bytes::from(bytes)).await {
        Ok(_) => Ok(PublishedSynthesisedSnapshot { snap_ulid, key }),
        Err(ConditionalPutError::PreconditionFailed) => {
            Err(PublishSnapshotError::AlreadyExists { key })
        }
        Err(ConditionalPutError::Other(e)) => Err(PublishSnapshotError::Other(
            anyhow::Error::new(e).context("publishing synthesised snapshot manifest"),
        )),
    }
}

/// Outcome of [`resolve_handoff_verifier`]: which Ed25519 key a
/// claimant should use to verify the snapshot manifest at
/// `by_id/<vol_ulid>/snapshots/.../<snap_ulid>.manifest`.
///
/// For ordinary handoff snapshots the claimant uses the dead fork's
/// own `volume.pub` (the regular block-reader path). For
/// **synthesised** handoff snapshots minted by `volume release
/// --force`, verification must use the recovering coordinator's
/// `coordinator.pub`, fetched from
/// `coordinators/<recovering_coordinator_id>/coordinator.pub` and
/// path-bound to its derived id.
#[derive(Debug)]
pub enum HandoffVerifier {
    /// Manifest is signed by the source volume's own key. Claimants
    /// take the standard block-reader path: load `volume.pub` and
    /// verify there.
    Normal,
    /// Manifest is a synthesised handoff snapshot signed by a
    /// recovering coordinator. The contained key is the
    /// already-verified pub the manifest was signed by, ready to
    /// embed in the new fork's `provenance.parent_manifest_pubkey`.
    /// The pub is boxed because `VerifyingKey` is ~160 bytes
    /// (compressed + decompressed point cache), and the `Normal`
    /// variant is zero-sized — boxing keeps the overall enum size
    /// small.
    Synthesised {
        recovering_coordinator_id: String,
        recovered_at: String,
        manifest_pubkey: Box<VerifyingKey>,
    },
}

/// Errors from [`resolve_handoff_verifier`].
#[derive(Debug)]
pub enum ResolveHandoffError {
    /// The snapshot manifest could not be read from the bucket.
    ManifestRead(anyhow::Error),
    /// The manifest parsed but its recovery metadata is malformed
    /// (e.g. partial fields, invalid version).
    ManifestParse(std::io::Error),
    /// The synthesised manifest names a recovering coordinator whose
    /// `coordinator.pub` is missing, malformed, or doesn't bind back
    /// to the named id.
    PubkeyResolution(std::io::Error),
    /// The manifest's signature does not verify under the resolved
    /// pubkey. Could indicate a tampered or wrong-fork manifest.
    SignatureInvalid(std::io::Error),
}

impl std::fmt::Display for ResolveHandoffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ManifestRead(e) => write!(f, "reading snapshot manifest: {e:#}"),
            Self::ManifestParse(e) => write!(f, "parsing snapshot manifest: {e}"),
            Self::PubkeyResolution(e) => write!(f, "resolving recovering coordinator pubkey: {e}"),
            Self::SignatureInvalid(e) => write!(f, "verifying snapshot manifest: {e}"),
        }
    }
}

impl std::error::Error for ResolveHandoffError {}

/// Decide which Ed25519 key a claimant should use to verify the
/// snapshot manifest at `by_id/<vol_ulid>/snapshots/.../<snap_ulid>.manifest`.
///
/// Pipeline:
///   1. Fetch the manifest bytes from S3.
///   2. [`peek_snapshot_manifest_recovery`] to detect whether this
///      is a synthesised handoff snapshot.
///   3. If synthesised: fetch the recovering coordinator's pub via
///      [`crate::identity::fetch_coordinator_pub`] (which path-binds
///      the pub to its derived id), then run
///      [`read_snapshot_manifest_from_bytes`] to verify the
///      signature under that pub. Returns
///      [`HandoffVerifier::Synthesised`] on success.
///   4. Otherwise: returns [`HandoffVerifier::Normal`]. The caller
///      uses the source volume's own `volume.pub` (the regular
///      block-reader path).
///
/// Failures at any step refuse cleanly with a typed
/// [`ResolveHandoffError`] so the caller can surface a clear error
/// to the operator.
pub async fn resolve_handoff_verifier(
    store: &Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
    snap_ulid: Ulid,
) -> Result<HandoffVerifier, ResolveHandoffError> {
    let key = snapshot_manifest_key(&vol_ulid.to_string(), snap_ulid);
    let bytes = store
        .get(&key)
        .await
        .map_err(|e| {
            ResolveHandoffError::ManifestRead(
                anyhow::Error::new(e).context(format!("fetching snapshot manifest at {key}")),
            )
        })?
        .bytes()
        .await
        .map_err(|e| {
            ResolveHandoffError::ManifestRead(
                anyhow::Error::new(e)
                    .context(format!("reading snapshot manifest bytes from {key}")),
            )
        })?;

    let recovery =
        peek_snapshot_manifest_recovery(&bytes).map_err(ResolveHandoffError::ManifestParse)?;
    let Some(recovery) = recovery else {
        return Ok(HandoffVerifier::Normal);
    };

    let manifest_pubkey =
        crate::identity::fetch_coordinator_pub(store.as_ref(), &recovery.recovering_coordinator_id)
            .await
            .map_err(ResolveHandoffError::PubkeyResolution)?;

    // Verify the manifest signature under the resolved pubkey. This
    // re-validates the signing input including the recovery metadata
    // — a tampered manifest will fail here.
    read_snapshot_manifest_from_bytes(&bytes, &manifest_pubkey, &snap_ulid)
        .map_err(ResolveHandoffError::SignatureInvalid)?;

    Ok(HandoffVerifier::Synthesised {
        recovering_coordinator_id: recovery.recovering_coordinator_id,
        recovered_at: recovery.recovered_at,
        manifest_pubkey: Box::new(manifest_pubkey),
    })
}

/// Fetch a handoff snapshot manifest, verify it under the right key,
/// and return the parsed manifest alongside the [`HandoffVerifier`]
/// that identified the key.
///
/// `fallback_pubkey` is the volume's own identity key, used when the
/// manifest is a normal (non-recovery) handoff. For recovery
/// manifests the recovering coordinator's pub is fetched and
/// verified under the same path-binding rules as
/// [`resolve_handoff_verifier`].
///
/// When `peer` is `Some`, the manifest bytes are tried over peer-
/// fetch first and only fall through to S3 on a peer miss / network
/// error. The signature is verified the same way regardless of which
/// tier served the bytes; a tampering peer is detected here and
/// surfaces as `ResolveHandoffError::SignatureInvalid` rather than a
/// silent acceptance.
///
/// Used by the claim path to inspect the released fork's segment
/// list and decide whether to skip an empty intermediate fork.
pub async fn fetch_verified_handoff_manifest(
    store: &Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
    snap_ulid: Ulid,
    fallback_pubkey: &VerifyingKey,
    peer: Option<&crate::prefetch::PeerFetchContext>,
) -> Result<(SnapshotManifest, HandoffVerifier), ResolveHandoffError> {
    let bytes = if let Some(peer_ctx) = peer {
        if let Some(b) = peer_ctx
            .client
            .fetch_snapshot_manifest(
                &peer_ctx.endpoint,
                &peer_ctx.volume_name,
                vol_ulid,
                snap_ulid,
            )
            .await
        {
            tracing::trace!(
                "[handoff-fetch] peer-served manifest {vol_ulid}/{snap_ulid} ({} bytes)",
                b.len()
            );
            b
        } else {
            tracing::trace!(
                "[handoff-fetch] peer miss for manifest {vol_ulid}/{snap_ulid}; falling through to S3"
            );
            fetch_manifest_from_store(store, vol_ulid, snap_ulid).await?
        }
    } else {
        fetch_manifest_from_store(store, vol_ulid, snap_ulid).await?
    };

    let recovery =
        peek_snapshot_manifest_recovery(&bytes).map_err(ResolveHandoffError::ManifestParse)?;

    match recovery {
        None => {
            let manifest = read_snapshot_manifest_from_bytes(&bytes, fallback_pubkey, &snap_ulid)
                .map_err(ResolveHandoffError::SignatureInvalid)?;
            Ok((manifest, HandoffVerifier::Normal))
        }
        Some(recovery) => {
            let manifest_pubkey = crate::identity::fetch_coordinator_pub(
                store.as_ref(),
                &recovery.recovering_coordinator_id,
            )
            .await
            .map_err(ResolveHandoffError::PubkeyResolution)?;
            let manifest = read_snapshot_manifest_from_bytes(&bytes, &manifest_pubkey, &snap_ulid)
                .map_err(ResolveHandoffError::SignatureInvalid)?;
            Ok((
                manifest,
                HandoffVerifier::Synthesised {
                    recovering_coordinator_id: recovery.recovering_coordinator_id,
                    recovered_at: recovery.recovered_at,
                    manifest_pubkey: Box::new(manifest_pubkey),
                },
            ))
        }
    }
}

async fn fetch_manifest_from_store(
    store: &Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
    snap_ulid: Ulid,
) -> Result<Bytes, ResolveHandoffError> {
    let key = snapshot_manifest_key(&vol_ulid.to_string(), snap_ulid);
    store
        .get(&key)
        .await
        .map_err(|e| {
            ResolveHandoffError::ManifestRead(
                anyhow::Error::new(e).context(format!("fetching snapshot manifest at {key}")),
            )
        })?
        .bytes()
        .await
        .map_err(|e| {
            ResolveHandoffError::ManifestRead(
                anyhow::Error::new(e)
                    .context(format!("reading snapshot manifest bytes from {key}")),
            )
        })
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

        // Three segments in known-ascending ULID order, uploaded out
        // of order. `increment()` is deterministic and strictly
        // larger, so s1 < s2 < s3 by construction — no separate sort
        // step needed to compute the expected output.
        let s1 = Ulid::new();
        let s2 = s1.increment().expect("increment");
        let s3 = s2.increment().expect("increment");
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

        let good_id = Ulid::new();
        let bad_id = Ulid::new();
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
        let seg_id = Ulid::new();
        let good = build_segment_bytes(signer.as_ref(), vec![seg_entry(0, 1, b"x")]);
        store
            .put(&segment_key(vol_ulid, seg_id), PutPayload::from(good))
            .await
            .unwrap();

        // A leftover .tmp upload (not a ULID key).
        let tmp_key = StorePath::from(format!("by_id/{vol_ulid}/segments/{}.tmp", Ulid::new()));
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

        let good_id = Ulid::new();
        let foreign_id = Ulid::new();
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
    async fn fetch_volume_pub_optional_returns_some_when_present() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let (_, vk) = make_signer();
        upload_volume_pub(&store, vol_ulid, &vk).await;

        let got = fetch_volume_pub_optional(&store, vol_ulid).await.unwrap();
        let got = got.expect("present");
        assert_eq!(got.to_bytes(), vk.to_bytes());
    }

    #[tokio::test]
    async fn fetch_volume_pub_optional_returns_none_when_absent() {
        // Mirrors the create-time crash window: names/<name> exists in
        // S3 but `by_id/<vol>/volume.pub` was never uploaded.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let got = fetch_volume_pub_optional(&store, vol_ulid).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn fetch_volume_pub_optional_propagates_malformed_hex() {
        // A present-but-corrupt object is *not* an absent object — the
        // caller wants to know about corruption, not silently treat it
        // as the empty-fork recovery case.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let vol_ulid = Ulid::new();
        let key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
        store
            .put(&key, PutPayload::from(b"not hex\n".as_slice()))
            .await
            .unwrap();
        assert!(fetch_volume_pub_optional(&store, vol_ulid).await.is_err());
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

    // ── mint_and_publish_synthesised_snapshot ─────────────────────────

    #[tokio::test]
    async fn publish_synthesised_snapshot_writes_verifiable_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dead_vol = Ulid::new();
        let (coord_signer, coord_vk) = make_signer();
        let coord_id = "01ABCDEFGHJKMNPQRSTVWXYZ23";

        let s1 = Ulid::new();
        let s2 = s1.increment().expect("increment");
        let s3 = s2.increment().expect("increment");
        let segments = vec![s2, s1, s3];

        let published = mint_and_publish_synthesised_snapshot(
            &store,
            dead_vol,
            &segments,
            coord_signer.as_ref(),
            coord_id,
        )
        .await
        .expect("publish should succeed on a fresh prefix");

        // Object lives at the expected key.
        let raw = store
            .get(&published.key)
            .await
            .expect("object present at returned key")
            .bytes()
            .await
            .unwrap();

        // Manifest verifies under the coordinator's pubkey, has the
        // recovery metadata populated, and lists ULIDs in sorted order.
        // We exercise this via a tempdir + the existing reader.
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();
        let local_path =
            tmp.path()
                .join("snapshots")
                .join(elide_core::signing::snapshot_manifest_filename(
                    &published.snap_ulid,
                ));
        std::fs::write(&local_path, &raw).unwrap();

        let manifest = elide_core::signing::read_snapshot_manifest(
            tmp.path(),
            &coord_vk,
            &published.snap_ulid,
        )
        .expect("manifest verifies under coordinator pubkey");
        assert_eq!(manifest.segment_ulids, vec![s1, s2, s3]);
        let recovery = manifest.recovery.expect("recovery metadata present");
        assert_eq!(recovery.recovering_coordinator_id, coord_id);
        assert!(!recovery.recovered_at.is_empty(), "recovered_at populated");
    }

    #[tokio::test]
    async fn publish_synthesised_snapshot_does_not_verify_under_volume_pub() {
        // Cross-class verification must fail: a synthesised manifest
        // signed by coord_signer must NOT validate against an
        // unrelated volume pubkey, even if a misconfigured caller
        // tries.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dead_vol = Ulid::new();
        let (coord_signer, _) = make_signer();
        let (_, unrelated_vk) = make_signer();

        let segments = vec![Ulid::new()];

        let published = mint_and_publish_synthesised_snapshot(
            &store,
            dead_vol,
            &segments,
            coord_signer.as_ref(),
            "01ABCDEFGHJKMNPQRSTVWXYZ23",
        )
        .await
        .unwrap();

        let raw = store
            .get(&published.key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();
        let local_path =
            tmp.path()
                .join("snapshots")
                .join(elide_core::signing::snapshot_manifest_filename(
                    &published.snap_ulid,
                ));
        std::fs::write(&local_path, &raw).unwrap();

        let err = elide_core::signing::read_snapshot_manifest(
            tmp.path(),
            &unrelated_vk,
            &published.snap_ulid,
        )
        .expect_err("must not verify under unrelated key");
        assert!(
            err.to_string().contains("signature invalid"),
            "expected signature failure, got: {err}"
        );
    }

    #[tokio::test]
    async fn publish_synthesised_snapshot_refuses_when_object_exists() {
        // Two writers that happen to mint the same ULID (or a retry
        // after a partial state) must see the second call refuse via
        // the conditional-create precondition. We can't easily force
        // a ULID collision, so we hand-write a placeholder at the
        // expected key first.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dead_vol = Ulid::new();
        let (coord_signer, _) = make_signer();
        let segments = vec![Ulid::new()];

        // First, do a real publish so we have a key to collide with.
        let first = mint_and_publish_synthesised_snapshot(
            &store,
            dead_vol,
            &segments,
            coord_signer.as_ref(),
            "01ABCDEFGHJKMNPQRSTVWXYZ23",
        )
        .await
        .unwrap();

        // Pre-occupy a chosen key with bogus contents.
        let collision_key = first.key.clone();
        // Re-run a publish that we force into the same key by writing
        // first, then attempting put_if_absent again at the same key
        // via an unsafe cheat: use the public helper directly.
        let payload = Bytes::from_static(b"already here");
        // Already present — ensure put_if_absent fails.
        let err = portable::put_if_absent(store.as_ref(), &collision_key, payload)
            .await
            .expect_err("expected precondition-failed");
        assert!(matches!(err, ConditionalPutError::PreconditionFailed));
    }

    // ── resolve_handoff_verifier ───────────────────────────────────────

    /// Stand up a coordinator-published `coordinators/<id>/coordinator.pub`
    /// using a fresh CoordinatorIdentity, returning the identity so the
    /// caller can sign manifests with it.
    async fn coordinator_with_published_pub(
        store: &Arc<dyn ObjectStore>,
    ) -> Arc<crate::identity::CoordinatorIdentity> {
        let dir = tempfile::tempdir().unwrap();
        let identity =
            Arc::new(crate::identity::CoordinatorIdentity::load_or_generate(dir.path()).unwrap());
        identity.publish_pub(store.as_ref()).await.unwrap();
        identity
    }

    #[tokio::test]
    async fn resolve_returns_normal_for_non_recovery_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dead_vol = Ulid::new();
        // Sign the manifest with the dead fork's volume key (the
        // normal handoff path).
        let (signer, _vk) = make_signer();
        let snap_ulid = Ulid::new();
        let bytes = elide_core::signing::build_snapshot_manifest_bytes(
            signer.as_ref(),
            &[Ulid::new()],
            None,
        );
        let key = snapshot_manifest_key(&dead_vol.to_string(), snap_ulid);
        store.put(&key, PutPayload::from(bytes)).await.unwrap();

        let outcome = resolve_handoff_verifier(&store, dead_vol, snap_ulid)
            .await
            .unwrap();
        assert!(matches!(outcome, HandoffVerifier::Normal));
    }

    #[tokio::test]
    async fn resolve_verifies_synthesised_manifest_under_coordinator_pub() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dead_vol = Ulid::new();
        let identity = coordinator_with_published_pub(&store).await;

        // Mint and publish a synthesised manifest signed by the
        // coordinator's identity key.
        let published = mint_and_publish_synthesised_snapshot(
            &store,
            dead_vol,
            &[Ulid::new(), Ulid::new()],
            identity.as_ref(),
            identity.coordinator_id_str(),
        )
        .await
        .unwrap();

        let outcome = resolve_handoff_verifier(&store, dead_vol, published.snap_ulid)
            .await
            .unwrap();
        match outcome {
            HandoffVerifier::Synthesised {
                recovering_coordinator_id,
                manifest_pubkey,
                ..
            } => {
                assert_eq!(recovering_coordinator_id, identity.coordinator_id_str());
                assert_eq!(
                    manifest_pubkey.to_bytes(),
                    identity.verifying_key().to_bytes(),
                );
            }
            HandoffVerifier::Normal => {
                panic!("expected Synthesised verifier for a recovery manifest")
            }
        }
    }

    #[tokio::test]
    async fn resolve_refuses_when_recovering_coordinator_pub_is_missing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dead_vol = Ulid::new();
        // Mint a manifest signed by an identity whose pub is NOT
        // published to the bucket.
        let dir = tempfile::tempdir().unwrap();
        let unpublished =
            crate::identity::CoordinatorIdentity::load_or_generate(dir.path()).unwrap();

        let published = mint_and_publish_synthesised_snapshot(
            &store,
            dead_vol,
            &[Ulid::new()],
            &unpublished,
            unpublished.coordinator_id_str(),
        )
        .await
        .unwrap();

        let err = resolve_handoff_verifier(&store, dead_vol, published.snap_ulid)
            .await
            .expect_err("missing coordinator.pub must refuse");
        assert!(
            matches!(err, ResolveHandoffError::PubkeyResolution(_)),
            "expected PubkeyResolution, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn resolve_refuses_when_manifest_object_is_missing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let err = resolve_handoff_verifier(&store, Ulid::new(), Ulid::new())
            .await
            .expect_err("missing manifest must refuse");
        assert!(matches!(err, ResolveHandoffError::ManifestRead(_)));
    }
}
