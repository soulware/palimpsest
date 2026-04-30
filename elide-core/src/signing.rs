// Ed25519 keypair management and signed provenance.
//
// `volume.provenance` is the signed statement of a volume's lineage:
// which other volumes it is related to via fork parent and extent-index
// sources. Both relationships are carried in the same file, under the
// same signature, so tampering with lineage is detectable with the
// volume's own public key.
//
// Key file naming convention (all volumes, flat layout):
//   volume.key / volume.pub / volume.provenance  (under <by_id>/<ulid>/)
//
// File contents:
//   *.key         — Ed25519 private key (32 raw bytes, never uploaded)
//   *.pub         — Ed25519 public key (64 lowercase hex chars + newline, uploaded to S3)
//   *.provenance  — signed lineage (parent + extent_index), uploaded to S3
//
// provenance file format:
//   parent: <volume-ulid>/<snapshot-ulid>              (empty string if none)
//   parent_pubkey: <64 lowercase hex chars>            (empty string if no parent)
//   extent_index:
//     <volume-ulid>/<snapshot-ulid>
//     <volume-ulid>/<snapshot-ulid>
//     ...
//   sig: <hex-encoded 64-byte Ed25519 signature>
//
// The `parent:` and `parent_pubkey:` lines are always present, even when
// empty, so "no parent" and "empty parent" are the same thing — both in
// signing input and parser. The `extent_index:` header is always present;
// the list may be empty.
//
// The `parent_pubkey` field is the Ed25519 verifying key of the parent
// volume at fork time, embedded under the child's signature. It is the
// trust anchor for verifying the parent's own signed artefacts
// (`volume.provenance`, `snapshots/<ulid>.manifest`) at open time without
// having to trust whatever `volume.pub` happens to sit in the parent's
// directory. Keys never rotate — if one needs to change, fork — so the
// embedded value is authoritative for the life of the child.
//
// Signing input (NUL-separated, fixed field order):
//   parent_or_empty ‖ NUL ‖ parent_pubkey_hex_or_empty ‖ NUL ‖
//   extent_entry_1 ‖ NUL ‖ extent_entry_2 ‖ NUL ‖ … ‖ extent_entry_N
//
// An empty extent_index contributes zero trailing entries (the signing
// input ends after the parent_pubkey field's terminating NUL).
//
// Signing input for segments: passed in from segment::SegmentSigner::sign(); the caller
//   (segment.rs) pre-hashes with BLAKE3 before calling sign(), so the key signs
//   the 32-byte hash.

use std::io;
use std::path::Path;
use std::sync::Arc;

pub use ed25519_dalek::VerifyingKey;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use rand_core::OsRng;

use crate::segment::SegmentSigner;

// File name constants.
pub const VOLUME_KEY_FILE: &str = "volume.key";
pub const VOLUME_PUB_FILE: &str = "volume.pub";
pub const VOLUME_PROVENANCE_FILE: &str = "volume.provenance";

// Force-snapshot attestation key (local only, never uploaded). Lives in
// the source ancestor's `by_id/<src>/` directory and signs locally-
// generated `.manifest` files for forker-attested "now" pins of a readonly
// source (`fork --force-snapshot`).
// Persistent across invocations on the same host so multiple forks against
// the same source share one attestation key and produce a single coherent
// `<snap>.manifest`.
pub const FORCE_SNAPSHOT_KEY_FILE: &str = "force-snapshot.key";
pub const FORCE_SNAPSHOT_PUB_FILE: &str = "force-snapshot.pub";

/// Suffix appended to a snapshot ULID to form the signed segments manifest
/// filename, e.g. `snapshots/01ABC....manifest`.
pub const SNAPSHOT_MANIFEST_SUFFIX: &str = ".manifest";

// --- Ed25519Signer ---

pub struct Ed25519Signer {
    key: SigningKey,
}

impl SegmentSigner for Ed25519Signer {
    fn sign(&self, msg: &[u8]) -> [u8; 64] {
        self.key.sign(msg).to_bytes()
    }
}

// --- keypair generation ---

/// Generate a new Ed25519 keypair and write the key files to `dir`.
///
/// `key_file` and `pub_file` are the filenames within `dir` (e.g. `"fork.key"`
/// and `"fork.pub"`, or `"base.key"` and `"base.pub"`).
///
/// Returns the signing key so the caller can immediately write an origin file
/// without re-reading from disk.
pub fn generate_keypair(dir: &Path, key_file: &str, pub_file: &str) -> io::Result<SigningKey> {
    let key = SigningKey::generate(&mut OsRng);
    crate::segment::write_file_atomic(&dir.join(key_file), &key.to_bytes())?;
    let pub_hex = encode_hex(&key.verifying_key().to_bytes()) + "\n";
    crate::segment::write_file_atomic(&dir.join(pub_file), pub_hex.as_bytes())?;
    Ok(key)
}

/// Load an Ed25519 signing key from `dir/<key_file>` and return a `SegmentSigner`.
pub fn load_signer(dir: &Path, key_file: &str) -> io::Result<Arc<dyn SegmentSigner>> {
    let (signer, _) = load_keypair(dir, key_file)?;
    Ok(signer)
}

/// Load an Ed25519 signing key and derive its verifying key.
///
/// Returns `(signer, verifying_key)`. The verifying key is derived directly
/// from the signing key — no separate `volume.pub` read is needed.
pub fn load_keypair(
    dir: &Path,
    key_file: &str,
) -> io::Result<(Arc<dyn SegmentSigner>, VerifyingKey)> {
    let bytes = std::fs::read(dir.join(key_file))
        .map_err(|e| io::Error::other(format!("{key_file} not readable: {e}")))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| io::Error::other(format!("{key_file} wrong length (expected 32 bytes)")))?;
    let key = SigningKey::from_bytes(&arr);
    let verifying_key = key.verifying_key();
    Ok((Arc::new(Ed25519Signer { key }), verifying_key))
}

/// Load an Ed25519 verifying key from `dir/<pub_file>`.
///
/// The file must contain exactly 64 lowercase hex chars followed by a newline.
pub fn load_verifying_key(dir: &Path, pub_file: &str) -> io::Result<VerifyingKey> {
    let hex = std::fs::read_to_string(dir.join(pub_file))
        .map_err(|e| io::Error::other(format!("{pub_file} not readable: {e}")))?;
    let bytes = decode_hex(hex.trim())
        .map_err(|_| io::Error::other(format!("{pub_file} is not valid hex")))?;
    let arr: [u8; 32] = bytes.try_into().map_err(|_| {
        io::Error::other(format!("{pub_file} wrong length (expected 64 hex chars)"))
    })?;
    VerifyingKey::from_bytes(&arr).map_err(|e| io::Error::other(format!("{pub_file} invalid: {e}")))
}

/// Generate an ephemeral Ed25519 keypair in memory.
///
/// Returns `(signer, verifying_key)`. Nothing is written to disk.
pub fn generate_ephemeral_signer() -> (Arc<dyn SegmentSigner>, VerifyingKey) {
    let key = SigningKey::generate(&mut OsRng);
    let verifying_key = key.verifying_key();
    (Arc::new(Ed25519Signer { key }), verifying_key)
}

/// Load the keypair at `dir/<key_file>`, or generate it (plus `<pub_file>`)
/// if the key file does not yet exist. Returns `(signer, verifying_key)`.
///
/// Used for the `force-snapshot.key` / `.pub` pair on a readonly source:
/// the first forker mints the keypair, subsequent forkers on the same host
/// reuse it.
pub fn load_or_create_keypair(
    dir: &Path,
    key_file: &str,
    pub_file: &str,
) -> io::Result<(Arc<dyn SegmentSigner>, VerifyingKey)> {
    if dir.join(key_file).exists() {
        return load_keypair(dir, key_file);
    }
    let key = generate_keypair(dir, key_file, pub_file)?;
    let verifying_key = key.verifying_key();
    Ok((Arc::new(Ed25519Signer { key }), verifying_key))
}

/// A reference to a specific snapshot of a parent volume, plus the
/// parent's verifying key captured at fork time.
///
/// This is the trust anchor for walking the fork ancestry chain: the
/// child signs over the parent's pubkey, and verification of the parent's
/// own `volume.provenance` and `snapshots/<ulid>.manifest` uses this
/// embedded key rather than whatever `volume.pub` happens to sit in the
/// parent's directory. Keys never rotate — if a volume's key needs to
/// change, the operation is "fork the volume" — so the embedded value is
/// authoritative for the lifetime of the child and all its descendants.
///
/// `manifest_pubkey`, when set, overrides `pubkey` for verifying the pinned
/// `snapshots/<snap_ulid>.manifest` only. Used for forker-attested "now"
/// pins (`volume create --from --force-snapshot`), where the forker doesn't hold
/// the parent's private key and instead signs the manifest with a
/// stable per-source key (`by_id/<src>/force-snapshot.key`) — shared
/// across multiple forks on the same host so concurrent or sequential
/// fork invocations produce one coherent manifest. The parent's own
/// `volume.provenance` and `.idx` signatures are still verified under
/// `pubkey` (the real owner's key). When `None`, the same `pubkey` is
/// used for both, matching the original single-key design.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParentRef {
    /// ULID of the parent volume.
    pub volume_ulid: String,
    /// ULID of the specific snapshot on the parent that this fork branches from.
    pub snapshot_ulid: String,
    /// Parent volume's Ed25519 verifying key at fork time (32 raw bytes).
    pub pubkey: [u8; 32],
    /// Optional override key for verifying the pinned snapshot's
    /// `.manifest`. See struct docs for use.
    pub manifest_pubkey: Option<[u8; 32]>,
}

impl ParentRef {
    /// On-disk `<volume-ulid>/<snapshot-ulid>` form written to
    /// `volume.provenance` and fed into the signing input.
    pub fn to_display(&self) -> String {
        format!("{}/{}", self.volume_ulid, self.snapshot_ulid)
    }
}

/// Lineage fields embedded in `volume.provenance` under the signature.
///
/// `parent` is the fork ancestor (writable CoW relationship — merged into
/// the child's LBA map at open time). `extent_index` is a flat list of
/// hash-source snapshots whose extents seed the child's extent index for
/// dedup and delta compression, but are never merged into the LBA map.
/// Both fields are optional: fresh writable volumes carry neither,
/// forks carry only `parent`, imports-with-`--extents-from` carry only
/// `extent_index`, and in principle a future flow could carry both.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct ProvenanceLineage {
    /// Parent snapshot reference with embedded parent pubkey, or `None`
    /// for root volumes.
    pub parent: Option<ParentRef>,
    /// Flat list of `<volume-ulid>/<snapshot-ulid>` entries naming
    /// hash-source snapshots.
    pub extent_index: Vec<String>,
}

/// Set up a readonly volume's identity and return a signer for segment writing.
///
/// Generates an ephemeral Ed25519 keypair, writes `volume.pub` and
/// `volume.provenance` (with the given `lineage`), and returns the signer.
/// The private key is never written to disk — readonly volumes (e.g. OCI
/// imports) have no use for it after the one-time write. Segment
/// verification at rebuild time uses the persisted `volume.pub`.
pub fn setup_readonly_identity(
    dir: &Path,
    pub_file: &str,
    provenance_file: &str,
    lineage: &ProvenanceLineage,
) -> io::Result<Arc<dyn SegmentSigner>> {
    let key = SigningKey::generate(&mut OsRng);
    let pub_hex = encode_hex(&key.verifying_key().to_bytes()) + "\n";
    crate::segment::write_file_atomic(&dir.join(pub_file), pub_hex.as_bytes())?;
    write_provenance(dir, &key, provenance_file, lineage)?;
    Ok(Arc::new(Ed25519Signer { key }))
}

// --- provenance files ---

/// Write a signed provenance file recording the volume's lineage.
pub fn write_provenance(
    dir: &Path,
    key: &SigningKey,
    provenance_file: &str,
    lineage: &ProvenanceLineage,
) -> io::Result<()> {
    let sig = sign_provenance(key, lineage);
    let content = serialize_provenance(lineage, &sig);
    crate::segment::write_file_atomic(&dir.join(provenance_file), content.as_bytes())
}

/// Read lineage from a volume's provenance, verifying the Ed25519
/// signature against `pub_file` sitting in the same directory.
///
/// Used by the current volume's open path and by ancestor walks that
/// don't have a caller-supplied trust anchor yet.
pub fn read_lineage_verifying_signature(
    dir: &Path,
    pub_file: &str,
    provenance_file: &str,
) -> io::Result<ProvenanceLineage> {
    let verifying_key = load_verifying_key(dir, pub_file)?;
    read_lineage_with_key(dir, &verifying_key, provenance_file)
}

/// Read lineage from an ancestor volume's provenance, verifying the
/// signature with a **caller-supplied** verifying key rather than the
/// `volume.pub` sitting in the ancestor's directory. Used by the
/// `Volume::open` ancestor walk, where the trust anchor for each step is
/// the `parent_pubkey` embedded in the child's signed provenance — not
/// whatever `volume.pub` happens to be on disk at the ancestor path.
pub fn read_lineage_with_key(
    dir: &Path,
    verifying_key: &VerifyingKey,
    provenance_file: &str,
) -> io::Result<ProvenanceLineage> {
    let content = std::fs::read_to_string(dir.join(provenance_file)).map_err(|e| {
        io::Error::other(format!(
            "{provenance_file} in {} not readable: {e}",
            dir.display()
        ))
    })?;
    let (lineage, sig_bytes) = parse_provenance(&content, provenance_file)?;
    let sig_arr: [u8; 64] = sig_bytes.try_into().map_err(|_| {
        io::Error::other(format!(
            "{provenance_file} sig wrong length (expected 64 bytes)"
        ))
    })?;
    let signature = Signature::from_bytes(&sig_arr);
    let msg = provenance_signing_input(&lineage);
    verifying_key.verify(&msg, &signature).map_err(|_| {
        io::Error::other(format!(
            "{provenance_file} in {} signature invalid",
            dir.display()
        ))
    })?;
    Ok(lineage)
}

// --- internal helpers ---

fn sign_provenance(key: &SigningKey, lineage: &ProvenanceLineage) -> [u8; 64] {
    key.sign(&provenance_signing_input(lineage)).to_bytes()
}

/// Signing input (NUL-separated, fixed field order):
///   parent_or_empty || NUL || parent_pubkey_hex_or_empty || NUL ||
///   entry_1 || NUL || entry_2 || NUL || … || entry_N
///   [ || NUL || manifest_pubkey_hex ]  (only when Some)
///
/// Empty `extent_index` contributes zero trailing entries. The optional
/// `manifest_pubkey` suffix is only included when a parent is present and
/// its `manifest_pubkey` field is `Some` — when absent, the signing input
/// is byte-identical to the original single-key format, so existing
/// provenance signatures continue to verify under the same input.
fn provenance_signing_input(lineage: &ProvenanceLineage) -> Vec<u8> {
    let parent_display = lineage.parent.as_ref().map(ParentRef::to_display);
    let parent_str = parent_display.as_deref().unwrap_or("");
    let parent_pubkey_hex = lineage
        .parent
        .as_ref()
        .map(|p| encode_hex(&p.pubkey))
        .unwrap_or_default();
    let manifest_pubkey_hex = lineage
        .parent
        .as_ref()
        .and_then(|p| p.manifest_pubkey.as_ref())
        .map(|k| encode_hex(k));
    let mut total = parent_str.len() + 1 + parent_pubkey_hex.len() + lineage.extent_index.len();
    for entry in &lineage.extent_index {
        total += entry.len();
    }
    if let Some(ref hex) = manifest_pubkey_hex {
        total += 1 + hex.len();
    }
    let mut msg = Vec::with_capacity(total);
    msg.extend_from_slice(parent_str.as_bytes());
    msg.push(0u8);
    msg.extend_from_slice(parent_pubkey_hex.as_bytes());
    for entry in &lineage.extent_index {
        msg.push(0u8);
        msg.extend_from_slice(entry.as_bytes());
    }
    if let Some(hex) = manifest_pubkey_hex {
        msg.push(0u8);
        msg.extend_from_slice(hex.as_bytes());
    }
    msg
}

fn serialize_provenance(lineage: &ProvenanceLineage, sig: &[u8; 64]) -> String {
    let parent_display = lineage.parent.as_ref().map(ParentRef::to_display);
    let parent_str = parent_display.as_deref().unwrap_or("");
    let parent_pubkey_hex = lineage
        .parent
        .as_ref()
        .map(|p| encode_hex(&p.pubkey))
        .unwrap_or_default();
    let manifest_pubkey_hex = lineage
        .parent
        .as_ref()
        .and_then(|p| p.manifest_pubkey.as_ref())
        .map(|k| encode_hex(k));
    let mut content = String::new();
    content.push_str("parent: ");
    content.push_str(parent_str);
    content.push('\n');
    content.push_str("parent_pubkey: ");
    content.push_str(&parent_pubkey_hex);
    content.push('\n');
    // Only emit when set: absence on disk means "same key as pubkey", which
    // keeps existing provenance files byte-identical under the new format.
    if let Some(hex) = manifest_pubkey_hex {
        content.push_str("parent_manifest_pubkey: ");
        content.push_str(&hex);
        content.push('\n');
    }
    content.push_str("extent_index:\n");
    for entry in &lineage.extent_index {
        content.push_str("  ");
        content.push_str(entry);
        content.push('\n');
    }
    content.push_str("sig: ");
    content.push_str(&encode_hex(sig));
    content.push('\n');
    content
}

/// Parse the on-disk file into its typed fields.
///
/// Field order is not required, but every required field must be present.
/// `extent_index:` is a header followed by zero or more indented lines
/// (two-space prefix). A blank line or a `key:` line ends the list.
fn parse_provenance(
    content: &str,
    provenance_file: &str,
) -> io::Result<(ProvenanceLineage, Vec<u8>)> {
    let mut parent_str: Option<Option<String>> = None;
    let mut parent_pubkey_str: Option<Option<String>> = None;
    let mut manifest_pubkey_str: Option<String> = None;
    let mut extent_index: Option<Vec<String>> = None;
    let mut sig: Option<Vec<u8>> = None;

    let mut lines = content.lines().peekable();
    while let Some(line) = lines.next() {
        if let Some(v) = line.strip_prefix("parent_manifest_pubkey: ") {
            if !v.is_empty() {
                manifest_pubkey_str = Some(v.to_owned());
            }
        } else if let Some(v) = line.strip_prefix("parent_pubkey: ") {
            parent_pubkey_str = Some(if v.is_empty() {
                None
            } else {
                Some(v.to_owned())
            });
        } else if let Some(stripped) = line.strip_prefix("parent_pubkey:") {
            // "parent_pubkey:" (no trailing space, no value) is equivalent to empty.
            if stripped.is_empty() {
                parent_pubkey_str = Some(None);
            }
        } else if let Some(v) = line.strip_prefix("parent: ") {
            parent_str = Some(if v.is_empty() {
                None
            } else {
                Some(v.to_owned())
            });
        } else if let Some(stripped) = line.strip_prefix("parent:") {
            // "parent:" (no trailing space, no value) is equivalent to empty.
            if stripped.is_empty() {
                parent_str = Some(None);
            }
        } else if line == "extent_index:" {
            let mut entries: Vec<String> = Vec::new();
            while let Some(peek) = lines.peek() {
                if let Some(entry) = peek.strip_prefix("  ") {
                    entries.push(entry.to_owned());
                    lines.next();
                } else {
                    break;
                }
            }
            extent_index = Some(entries);
        } else if let Some(v) = line.strip_prefix("sig: ") {
            sig = Some(decode_hex(v)?);
        }
    }

    let parent_str = parent_str
        .ok_or_else(|| io::Error::other(format!("{provenance_file} missing parent line")))?;
    let parent_pubkey_str = parent_pubkey_str
        .ok_or_else(|| io::Error::other(format!("{provenance_file} missing parent_pubkey line")))?;
    let extent_index = extent_index.ok_or_else(|| {
        io::Error::other(format!("{provenance_file} missing extent_index section"))
    })?;
    let sig = sig.ok_or_else(|| io::Error::other(format!("{provenance_file} missing sig line")))?;

    let parent = match (parent_str, parent_pubkey_str) {
        (None, None) => {
            if manifest_pubkey_str.is_some() {
                return Err(io::Error::other(format!(
                    "{provenance_file} has parent_manifest_pubkey but no parent"
                )));
            }
            None
        }
        (Some(s), Some(hex)) => {
            let (volume_ulid, snapshot_ulid) = s.split_once('/').ok_or_else(|| {
                io::Error::other(format!(
                    "{provenance_file} parent {s:?} missing '/' separator"
                ))
            })?;
            let volume_ulid = ulid::Ulid::from_string(volume_ulid)
                .map_err(|e| {
                    io::Error::other(format!("{provenance_file} parent volume ulid invalid: {e}"))
                })?
                .to_string();
            let snapshot_ulid = ulid::Ulid::from_string(snapshot_ulid)
                .map_err(|e| {
                    io::Error::other(format!(
                        "{provenance_file} parent snapshot ulid invalid: {e}"
                    ))
                })?
                .to_string();
            let pubkey_bytes = decode_hex(&hex)?;
            let pubkey: [u8; 32] = pubkey_bytes.try_into().map_err(|_| {
                io::Error::other(format!(
                    "{provenance_file} parent_pubkey wrong length (expected 64 hex chars)"
                ))
            })?;
            let manifest_pubkey = match manifest_pubkey_str {
                None => None,
                Some(hex) => {
                    let bytes = decode_hex(&hex)?;
                    let arr: [u8; 32] = bytes.try_into().map_err(|_| {
                        io::Error::other(format!(
                            "{provenance_file} parent_manifest_pubkey wrong length (expected 64 hex chars)"
                        ))
                    })?;
                    Some(arr)
                }
            };
            Some(ParentRef {
                volume_ulid,
                snapshot_ulid,
                pubkey,
                manifest_pubkey,
            })
        }
        (Some(_), None) => {
            return Err(io::Error::other(format!(
                "{provenance_file} has parent but missing parent_pubkey"
            )));
        }
        (None, Some(_)) => {
            return Err(io::Error::other(format!(
                "{provenance_file} has parent_pubkey but missing parent"
            )));
        }
    };

    Ok((
        ProvenanceLineage {
            parent,
            extent_index,
        },
        sig,
    ))
}

// --- snapshot manifest (`snapshots/<ulid>.manifest`) ---
//
// A snapshot manifest is the authoritative list of every segment ULID that
// belongs to a given snapshot of a volume — i.e. every `.idx` that the
// caller needs present under `index/` to fully reconstruct the LBA map for
// that snapshot. It is a *full* manifest (not a delta over the previous
// snapshot in the same volume), so open-time ancestor verification can
// walk the fork chain by reading exactly one `.manifest` per ancestor
// volume rather than chaining through every intermediate snapshot.
//
// File format (under `snapshots/<snap_ulid>.manifest`):
//
//   segments:
//     <segment-ulid>
//     <segment-ulid>
//     ...
//   [synthesised_from_recovery: true]
//   [recovering_coordinator_id: <coord_id>]
//   [recovered_at: <rfc3339>]
//   sig: <hex-encoded 64-byte Ed25519 signature>
//
// ULIDs are sorted lexicographically (= chronologically for ULIDs).
//
// The three bracketed fields are present only on **synthesised handoff
// snapshots** minted by `volume release --force` (Phase 3 of the
// portable-live-volume rollout): the recovering coordinator lists the
// dead fork's S3 segments, verifies each one against `volume.pub`, and
// signs the resulting list with its own `coordinator.key` rather than
// the dead fork's volume key. The recovery metadata identifies which
// coordinator did the recovery so verifiers can fetch the right pubkey
// from `coordinators/<recovering_coordinator_id>/coordinator.pub`.
//
// Signing input: NUL-separated concatenation of sorted ULIDs for
// non-recovery manifests; an empty manifest signs the empty byte
// string. **Recovery manifests** sign a domain-separated message:
// `"elide-snapshot-recovery-v1\0" + recovering_coordinator_id + "\0" +
// recovered_at + "\0\0" + <sorted-ulids>`. The unique prefix prevents
// cross-class signature reuse: a non-recovery sig won't validate a
// manifest edited to claim recovery, and a recovery sig won't
// validate a manifest with the recovery fields stripped.

/// Build the filename for `snap_ulid`'s segments manifest
/// (`<snap_ulid>.manifest`) inside a volume's `snapshots/` directory.
pub fn snapshot_manifest_filename(snap_ulid: &ulid::Ulid) -> String {
    format!("{snap_ulid}{SNAPSHOT_MANIFEST_SUFFIX}")
}

/// Domain-separation prefix for the recovery-manifest signing input.
/// Bumping the suffix (`v1`, `v2`, …) invalidates all previously signed
/// recovery manifests; chosen explicitly rather than implicitly so the
/// signing input is self-describing.
const RECOVERY_SIGNING_DOMAIN: &str = "elide-snapshot-recovery-v1";

/// Recovery metadata attached to a synthesised handoff snapshot.
///
/// Populated only on snapshots minted by `volume release --force`: the
/// recovering coordinator lists the dead fork's S3 segments, verifies
/// each against the dead fork's `volume.pub`, then signs the resulting
/// manifest with its own `coordinator.key` and writes these fields so
/// verifiers know which coordinator pubkey to fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotManifestRecovery {
    /// Coordinator id of the host that performed the recovery, in the
    /// same Crockford-Base32 ULID-shape used elsewhere. Verifiers
    /// resolve this to a pubkey via
    /// `coordinators/<recovering_coordinator_id>/coordinator.pub`.
    pub recovering_coordinator_id: String,
    /// Wall-clock time (RFC3339) the recovery was performed. Advisory
    /// metadata; not used in verification logic but signed-over so it
    /// cannot be edited post-hoc.
    pub recovered_at: String,
}

/// Result of parsing and verifying a snapshot manifest.
#[derive(Debug, Clone)]
pub struct SnapshotManifest {
    /// Segment ULIDs in strictly ascending order.
    pub segment_ulids: Vec<ulid::Ulid>,
    /// Recovery metadata, if this is a synthesised handoff snapshot.
    /// `None` for ordinary user/handoff snapshots.
    pub recovery: Option<SnapshotManifestRecovery>,
}

/// Write a signed snapshot manifest for `snap_ulid`.
///
/// `segment_ulids` is the unsorted list of segment ULIDs belonging to the
/// snapshot — typically every `.idx` present in the volume's `index/`
/// directory. The list is sorted and deduplicated before signing and
/// serialisation.
///
/// `recovery` is `Some(_)` only on synthesised handoff snapshots minted
/// by `volume release --force`; in that case the signer must be the
/// recovering coordinator's `coordinator.key` (not the dead fork's
/// volume signing key). The signing input is domain-separated when
/// recovery metadata is present so signatures of the two manifest
/// classes cannot be cross-validated.
///
/// Writes `vol_dir/snapshots/<snap_ulid>.manifest` atomically.
pub fn write_snapshot_manifest(
    vol_dir: &Path,
    signer: &dyn SegmentSigner,
    snap_ulid: &ulid::Ulid,
    segment_ulids: &[ulid::Ulid],
    recovery: Option<&SnapshotManifestRecovery>,
) -> io::Result<()> {
    let content = build_snapshot_manifest_bytes(signer, segment_ulids, recovery);
    let path = vol_dir
        .join("snapshots")
        .join(snapshot_manifest_filename(snap_ulid));
    crate::segment::write_file_atomic(&path, &content)
}

/// Build the signed bytes of a snapshot manifest without writing
/// anything to disk. Used by callers that need to publish the
/// manifest somewhere other than a local volume directory — notably
/// `volume release --force`, which mints a synthesised handoff
/// snapshot signed by the recovering coordinator's `coordinator.key`
/// and `PUT`s it directly to S3 under the dead fork's prefix.
///
/// The output is identical, byte-for-byte, to what
/// `write_snapshot_manifest` would write for the same inputs. The
/// caller is responsible for sorting/deduping is not their concern;
/// segment ULIDs are sorted and deduplicated internally before
/// signing.
pub fn build_snapshot_manifest_bytes(
    signer: &dyn SegmentSigner,
    segment_ulids: &[ulid::Ulid],
    recovery: Option<&SnapshotManifestRecovery>,
) -> Vec<u8> {
    let mut sorted: Vec<String> = segment_ulids.iter().map(|u| u.to_string()).collect();
    sorted.sort();
    sorted.dedup();

    let msg = manifest_signing_input(&sorted, recovery);
    let sig = signer.sign(&msg);
    serialize_snapshot_manifest(&sorted, recovery, &sig).into_bytes()
}

/// Read and verify a snapshot manifest from disk, returning its
/// sorted segment ULIDs and (for synthesised handoff snapshots) the
/// recovery metadata.
///
/// `verifying_key` is the Ed25519 verifying key that signed the
/// manifest:
///   - for ordinary snapshots, the volume's signing pubkey (for
///     ancestor verification this comes from the child's
///     `volume.provenance`, not the ancestor directory's `volume.pub`);
///   - for synthesised handoff snapshots, the recovering coordinator's
///     `coordinator.pub`. Callers identify this case via
///     [`peek_snapshot_manifest_recovery`] before fetching the right
///     pubkey, or by consulting the surrounding context — typically
///     the `names/<name>` record points the caller at the right
///     pubkey.
///
/// Fails if the file is missing, unparseable, the signature does not
/// match, or the ULIDs are not in strictly ascending order.
pub fn read_snapshot_manifest(
    vol_dir: &Path,
    verifying_key: &VerifyingKey,
    snap_ulid: &ulid::Ulid,
) -> io::Result<SnapshotManifest> {
    let filename = snapshot_manifest_filename(snap_ulid);
    let path = vol_dir.join("snapshots").join(&filename);
    let content = std::fs::read_to_string(&path).map_err(|e| {
        io::Error::other(format!(
            "{filename} in {} not readable: {e}",
            vol_dir.display()
        ))
    })?;
    read_snapshot_manifest_from_bytes(content.as_bytes(), verifying_key, snap_ulid)
}

/// Read and verify a snapshot manifest from raw bytes, returning its
/// sorted segment ULIDs and (for synthesised handoff snapshots) the
/// recovery metadata.
///
/// Same semantics as [`read_snapshot_manifest`] but takes a byte
/// slice directly. Used by callers that fetch a manifest from S3
/// rather than a local volume directory — notably the
/// claimant-side verification step of `volume claim`
/// against a synthesised handoff snapshot.
///
/// `snap_ulid` is used only for diagnostic strings; signature
/// verification is over the canonical content bytes.
pub fn read_snapshot_manifest_from_bytes(
    content: &[u8],
    verifying_key: &VerifyingKey,
    snap_ulid: &ulid::Ulid,
) -> io::Result<SnapshotManifest> {
    let filename = snapshot_manifest_filename(snap_ulid);
    let content_str = std::str::from_utf8(content)
        .map_err(|e| io::Error::other(format!("{filename} not valid utf-8: {e}")))?;

    let parsed = parse_snapshot_manifest(content_str, &filename)?;
    let sig_arr: [u8; 64] = parsed.sig.try_into().map_err(|_| {
        io::Error::other(format!("{filename} sig wrong length (expected 64 bytes)"))
    })?;
    let signature = Signature::from_bytes(&sig_arr);

    let msg = manifest_signing_input(&parsed.entries, parsed.recovery.as_ref());
    verifying_key
        .verify(&msg, &signature)
        .map_err(|_| io::Error::other(format!("{filename} signature invalid")))?;

    // Parse each entry as a typed ULID, enforcing strictly ascending order
    // (sort + dedup is done at write time, so any deviation is tamper or
    // corruption).
    let mut out: Vec<ulid::Ulid> = Vec::with_capacity(parsed.entries.len());
    for entry in &parsed.entries {
        let ulid = ulid::Ulid::from_string(entry).map_err(|e| {
            io::Error::other(format!(
                "{filename} contains invalid segment ULID {entry:?}: {e}"
            ))
        })?;
        if let Some(last) = out.last()
            && &ulid <= last
        {
            return Err(io::Error::other(format!(
                "{filename} segment ULIDs not in strictly ascending order at {entry}"
            )));
        }
        out.push(ulid);
    }
    Ok(SnapshotManifest {
        segment_ulids: out,
        recovery: parsed.recovery,
    })
}

/// Inspect a snapshot manifest's recovery metadata **without
/// verifying the signature**. Used by claimants on
/// `volume claim` to decide which pubkey to verify the
/// manifest under: regular volume identity key vs. recovering
/// coordinator's `coordinator.pub`.
///
/// Callers MUST verify under the chosen pubkey before trusting any
/// content (segments or recovery metadata) — peek alone is not safe
/// to act on, and the signing input is domain-separated so a
/// non-recovery sig cannot validate a recovery manifest and vice
/// versa.
///
/// Returns `Ok(None)` for ordinary (non-recovery) manifests,
/// `Ok(Some(recovery))` for synthesised handoff snapshots.
pub fn peek_snapshot_manifest_recovery(
    content: &[u8],
) -> io::Result<Option<SnapshotManifestRecovery>> {
    let content_str = std::str::from_utf8(content)
        .map_err(|e| io::Error::other(format!("manifest not valid utf-8: {e}")))?;
    let parsed = parse_snapshot_manifest(content_str, "<manifest>")?;
    Ok(parsed.recovery)
}

/// Signing input for a snapshot manifest.
///
/// Non-recovery manifests sign the NUL-separated concatenation of
/// sorted ULIDs (an empty manifest signs the empty byte string).
///
/// Recovery manifests prepend a domain-separated header so their
/// signatures live in a different signing-input space:
///   `"elide-snapshot-recovery-v1\0" + coord_id + "\0" + recovered_at +
///   "\0\0" + <NUL-joined ulids>`
///
/// The double-NUL separator between header and ULID list disambiguates
/// the boundary unambiguously even when the segment list is empty.
fn manifest_signing_input(
    sorted_ulids: &[String],
    recovery: Option<&SnapshotManifestRecovery>,
) -> Vec<u8> {
    let mut msg = Vec::new();
    if let Some(r) = recovery {
        msg.extend_from_slice(RECOVERY_SIGNING_DOMAIN.as_bytes());
        msg.push(0u8);
        msg.extend_from_slice(r.recovering_coordinator_id.as_bytes());
        msg.push(0u8);
        msg.extend_from_slice(r.recovered_at.as_bytes());
        // Double-NUL separator between header and ULID list.
        msg.push(0u8);
        msg.push(0u8);
    }
    for (i, u) in sorted_ulids.iter().enumerate() {
        if i > 0 {
            msg.push(0u8);
        }
        msg.extend_from_slice(u.as_bytes());
    }
    msg
}

fn serialize_snapshot_manifest(
    sorted_ulids: &[String],
    recovery: Option<&SnapshotManifestRecovery>,
    sig: &[u8; 64],
) -> String {
    let mut content = String::new();
    content.push_str("segments:\n");
    for u in sorted_ulids {
        content.push_str("  ");
        content.push_str(u);
        content.push('\n');
    }
    if let Some(r) = recovery {
        content.push_str("synthesised_from_recovery: true\n");
        content.push_str("recovering_coordinator_id: ");
        content.push_str(&r.recovering_coordinator_id);
        content.push('\n');
        content.push_str("recovered_at: ");
        content.push_str(&r.recovered_at);
        content.push('\n');
    }
    content.push_str("sig: ");
    content.push_str(&encode_hex(sig));
    content.push('\n');
    content
}

struct ParsedManifest {
    entries: Vec<String>,
    recovery: Option<SnapshotManifestRecovery>,
    sig: Vec<u8>,
}

fn parse_snapshot_manifest(content: &str, filename: &str) -> io::Result<ParsedManifest> {
    let mut entries: Option<Vec<String>> = None;
    let mut sig: Option<Vec<u8>> = None;
    let mut synthesised: bool = false;
    let mut recovering_coordinator_id: Option<String> = None;
    let mut recovered_at: Option<String> = None;

    let mut lines = content.lines().peekable();
    while let Some(line) = lines.next() {
        if line == "segments:" {
            let mut list: Vec<String> = Vec::new();
            while let Some(peek) = lines.peek() {
                if let Some(entry) = peek.strip_prefix("  ") {
                    list.push(entry.to_owned());
                    lines.next();
                } else {
                    break;
                }
            }
            entries = Some(list);
        } else if let Some(v) = line.strip_prefix("sig: ") {
            sig = Some(decode_hex(v)?);
        } else if let Some(v) = line.strip_prefix("synthesised_from_recovery: ") {
            synthesised = match v {
                "true" => true,
                "false" => false,
                other => {
                    return Err(io::Error::other(format!(
                        "{filename} synthesised_from_recovery has invalid value {other:?}"
                    )));
                }
            };
        } else if let Some(v) = line.strip_prefix("recovering_coordinator_id: ") {
            recovering_coordinator_id = Some(v.to_owned());
        } else if let Some(v) = line.strip_prefix("recovered_at: ") {
            recovered_at = Some(v.to_owned());
        }
    }

    let entries =
        entries.ok_or_else(|| io::Error::other(format!("{filename} missing segments section")))?;
    let sig = sig.ok_or_else(|| io::Error::other(format!("{filename} missing sig line")))?;

    let recovery = match (synthesised, recovering_coordinator_id, recovered_at) {
        (false, None, None) => None,
        (true, Some(coord_id), Some(at)) => Some(SnapshotManifestRecovery {
            recovering_coordinator_id: coord_id,
            recovered_at: at,
        }),
        // Any partial combination is malformed: either all three
        // recovery fields are present (and synthesised is true) or
        // none are.
        (synthesised, coord_id, at) => {
            return Err(io::Error::other(format!(
                "{filename} has inconsistent recovery metadata \
                 (synthesised_from_recovery={synthesised}, \
                 recovering_coordinator_id={}, recovered_at={})",
                coord_id.as_deref().unwrap_or("<absent>"),
                at.as_deref().unwrap_or("<absent>"),
            )));
        }
    };

    Ok(ParsedManifest {
        entries,
        recovery,
        sig,
    })
}

/// Test-only helper: write a signed `volume.provenance` with raw, unvalidated
/// parent and parent_pubkey strings. Signs over the content as written so
/// signature verification passes; parse errors fire downstream at
/// `ParentRef` construction. Used to exercise parser error paths with
/// syntactically bad content.
#[cfg(test)]
pub(crate) fn write_raw_provenance_for_test(
    dir: &Path,
    raw_parent: &str,
    raw_parent_pubkey_hex: &str,
    extent_index: &[String],
) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;

    let key = SigningKey::generate(&mut OsRng);
    let pub_hex = encode_hex(&key.verifying_key().to_bytes()) + "\n";
    crate::segment::write_file_atomic(&dir.join(VOLUME_PUB_FILE), pub_hex.as_bytes())?;

    let mut msg = Vec::new();
    msg.extend_from_slice(raw_parent.as_bytes());
    msg.push(0);
    msg.extend_from_slice(raw_parent_pubkey_hex.as_bytes());
    for entry in extent_index {
        msg.push(0);
        msg.extend_from_slice(entry.as_bytes());
    }
    let sig = key.sign(&msg).to_bytes();

    let mut content = String::new();
    content.push_str("parent: ");
    content.push_str(raw_parent);
    content.push('\n');
    content.push_str("parent_pubkey: ");
    content.push_str(raw_parent_pubkey_hex);
    content.push('\n');
    content.push_str("extent_index:\n");
    for entry in extent_index {
        content.push_str("  ");
        content.push_str(entry);
        content.push('\n');
    }
    content.push_str("sig: ");
    content.push_str(&encode_hex(&sig));
    content.push('\n');

    crate::segment::write_file_atomic(&dir.join(VOLUME_PROVENANCE_FILE), content.as_bytes())
}

pub fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn decode_hex(s: &str) -> io::Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(io::Error::other("hex string has odd length"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| io::Error::other(format!("invalid hex at position {i}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use ulid::Ulid;

    fn make_ulid(s: &str) -> Ulid {
        Ulid::from_string(s).unwrap()
    }

    fn signer_from(key: SigningKey) -> Ed25519Signer {
        Ed25519Signer { key }
    }

    #[test]
    fn snapshot_manifest_roundtrip() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        // Intentionally unsorted to exercise the sort-at-write step.
        let segs = vec![
            make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV"),
            make_ulid("01AAAAAAAAAAAAAAAAAAAAAAAA"),
            make_ulid("01BBBBBBBBBBBBBBBBBBBBBBBB"),
        ];

        write_snapshot_manifest(tmp.path(), &key, &snap, &segs, None).unwrap();
        let got = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap();

        let mut expected = segs.clone();
        expected.sort();
        assert_eq!(got.segment_ulids, expected);
        assert!(got.recovery.is_none());
    }

    #[test]
    fn snapshot_manifest_empty_list() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");

        write_snapshot_manifest(tmp.path(), &key, &snap, &[], None).unwrap();
        let got = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap();
        assert!(got.segment_ulids.is_empty());
        assert!(got.recovery.is_none());
    }

    #[test]
    fn snapshot_manifest_dedupes() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let dup = make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV");
        let segs = vec![dup, dup];

        write_snapshot_manifest(tmp.path(), &key, &snap, &segs, None).unwrap();
        let got = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap();
        assert_eq!(got.segment_ulids, vec![dup]);
    }

    #[test]
    fn snapshot_manifest_rejects_wrong_key() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let signing_key = signer_from(SigningKey::generate(&mut OsRng));
        let wrong_key = SigningKey::generate(&mut OsRng).verifying_key();
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        write_snapshot_manifest(
            tmp.path(),
            &signing_key,
            &snap,
            &[make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV")],
            None,
        )
        .unwrap();

        let err = read_snapshot_manifest(tmp.path(), &wrong_key, &snap).unwrap_err();
        assert!(err.to_string().contains("signature invalid"), "{err}");
    }

    #[test]
    fn snapshot_manifest_rejects_missing_file() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();
        let key = SigningKey::generate(&mut OsRng).verifying_key();
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert!(read_snapshot_manifest(tmp.path(), &key, &snap).is_err());
    }

    // ── Recovery (synthesised handoff snapshot) tests ────────────────

    fn sample_recovery() -> SnapshotManifestRecovery {
        SnapshotManifestRecovery {
            recovering_coordinator_id: "01ABCDEFGHJKMNPQRSTVWXYZ23".to_owned(),
            recovered_at: "2026-04-28T12:34:56Z".to_owned(),
        }
    }

    #[test]
    fn snapshot_manifest_recovery_roundtrip() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let segs = vec![
            make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV"),
            make_ulid("01AAAAAAAAAAAAAAAAAAAAAAAA"),
        ];
        let rec = sample_recovery();

        write_snapshot_manifest(tmp.path(), &key, &snap, &segs, Some(&rec)).unwrap();
        let got = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap();

        let mut expected = segs.clone();
        expected.sort();
        assert_eq!(got.segment_ulids, expected);
        assert_eq!(got.recovery, Some(rec));
    }

    #[test]
    fn snapshot_manifest_recovery_empty_segments() {
        // A force-release against a dead fork that never published a
        // single segment must still produce a verifiable recovery
        // manifest (signed empty handoff).
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let rec = sample_recovery();

        write_snapshot_manifest(tmp.path(), &key, &snap, &[], Some(&rec)).unwrap();
        let got = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap();
        assert!(got.segment_ulids.is_empty());
        assert_eq!(got.recovery, Some(rec));
    }

    #[test]
    fn snapshot_manifest_recovery_signing_input_is_domain_separated() {
        // A non-recovery signature must NOT validate a manifest whose
        // bytes have been edited to claim recovery — the signing input
        // is domain-separated so the two classes can't be cross-validated.
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let segs = vec![make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV")];

        // Write a non-recovery manifest, then mutate the file bytes to
        // append the three recovery fields without re-signing.
        write_snapshot_manifest(tmp.path(), &key, &snap, &segs, None).unwrap();
        let path = tmp
            .path()
            .join("snapshots")
            .join(snapshot_manifest_filename(&snap));
        let original = std::fs::read_to_string(&path).unwrap();
        // Surgically inject recovery fields just before `sig:`.
        let recovery_block = "synthesised_from_recovery: true\n\
                              recovering_coordinator_id: 01ABCDEFGHJKMNPQRSTVWXYZ23\n\
                              recovered_at: 2026-04-28T12:34:56Z\n";
        let tampered = original.replacen("sig: ", &format!("{recovery_block}sig: "), 1);
        std::fs::write(&path, tampered).unwrap();

        let err = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap_err();
        assert!(
            err.to_string().contains("signature invalid"),
            "expected signature failure on cross-class mutation, got: {err}"
        );
    }

    #[test]
    fn snapshot_manifest_recovery_rejects_stripped_recovery_fields() {
        // The reverse: a recovery manifest whose recovery fields are
        // stripped must also fail verification — the signature was
        // computed over the recovery signing input.
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let segs = vec![make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV")];
        let rec = sample_recovery();

        write_snapshot_manifest(tmp.path(), &key, &snap, &segs, Some(&rec)).unwrap();
        let path = tmp
            .path()
            .join("snapshots")
            .join(snapshot_manifest_filename(&snap));
        let original = std::fs::read_to_string(&path).unwrap();
        // Strip all three recovery lines.
        let stripped: String = original
            .lines()
            .filter(|l| {
                !l.starts_with("synthesised_from_recovery:")
                    && !l.starts_with("recovering_coordinator_id:")
                    && !l.starts_with("recovered_at:")
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(&path, stripped).unwrap();

        let err = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap_err();
        assert!(
            err.to_string().contains("signature invalid"),
            "expected signature failure on stripped recovery fields, got: {err}"
        );
    }

    #[test]
    fn snapshot_manifest_recovery_rejects_partial_recovery_metadata() {
        // A manifest with only some of the three recovery fields set
        // is structurally invalid; the parser must refuse before the
        // signature check (which would also fail).
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let path = tmp
            .path()
            .join("snapshots")
            .join(snapshot_manifest_filename(&snap));
        std::fs::write(
            &path,
            "segments:\n  01BX5ZZKJKTSV4RRFFQ69G5FAV\n\
             synthesised_from_recovery: true\n\
             sig: 00\n",
        )
        .unwrap();

        let key = SigningKey::generate(&mut OsRng).verifying_key();
        let err = read_snapshot_manifest(tmp.path(), &key, &snap).unwrap_err();
        assert!(
            err.to_string().contains("inconsistent recovery metadata"),
            "expected inconsistent-recovery error, got: {err}"
        );
    }

    // ── peek_snapshot_manifest_recovery / read_from_bytes ──────────────

    #[test]
    fn peek_returns_none_for_non_recovery_manifest() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        write_snapshot_manifest(
            tmp.path(),
            &key,
            &snap,
            &[make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV")],
            None,
        )
        .unwrap();

        let bytes = std::fs::read(
            tmp.path()
                .join("snapshots")
                .join(snapshot_manifest_filename(&snap)),
        )
        .unwrap();
        assert!(peek_snapshot_manifest_recovery(&bytes).unwrap().is_none());
    }

    #[test]
    fn peek_returns_recovery_metadata_for_synthesised_manifest() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let rec = SnapshotManifestRecovery {
            recovering_coordinator_id: "01ABCDEFGHJKMNPQRSTVWXYZ23".to_owned(),
            recovered_at: "2026-04-28T12:34:56Z".to_owned(),
        };
        write_snapshot_manifest(tmp.path(), &key, &snap, &[], Some(&rec)).unwrap();

        let bytes = std::fs::read(
            tmp.path()
                .join("snapshots")
                .join(snapshot_manifest_filename(&snap)),
        )
        .unwrap();
        let got = peek_snapshot_manifest_recovery(&bytes).unwrap().unwrap();
        assert_eq!(got, rec);
    }

    #[test]
    fn read_from_bytes_round_trips_normal_manifest() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let segs = vec![
            make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV"),
            make_ulid("01AAAAAAAAAAAAAAAAAAAAAAAA"),
        ];
        write_snapshot_manifest(tmp.path(), &key, &snap, &segs, None).unwrap();

        let bytes = std::fs::read(
            tmp.path()
                .join("snapshots")
                .join(snapshot_manifest_filename(&snap)),
        )
        .unwrap();
        let got = read_snapshot_manifest_from_bytes(&bytes, &verifying, &snap).unwrap();
        let mut expected = segs;
        expected.sort();
        assert_eq!(got.segment_ulids, expected);
        assert!(got.recovery.is_none());
    }

    #[test]
    fn read_from_bytes_verifies_synthesised_manifest_under_correct_key() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let rec = SnapshotManifestRecovery {
            recovering_coordinator_id: "01ABCDEFGHJKMNPQRSTVWXYZ23".to_owned(),
            recovered_at: "2026-04-28T12:34:56Z".to_owned(),
        };
        write_snapshot_manifest(
            tmp.path(),
            &key,
            &snap,
            &[make_ulid("01BX5ZZKJKTSV4RRFFQ69G5FAV")],
            Some(&rec),
        )
        .unwrap();

        let bytes = std::fs::read(
            tmp.path()
                .join("snapshots")
                .join(snapshot_manifest_filename(&snap)),
        )
        .unwrap();
        let got = read_snapshot_manifest_from_bytes(&bytes, &verifying, &snap).unwrap();
        assert_eq!(got.recovery, Some(rec));
    }

    #[test]
    fn read_from_bytes_rejects_synthesised_manifest_under_wrong_key() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let rec = SnapshotManifestRecovery {
            recovering_coordinator_id: "01ABCDEFGHJKMNPQRSTVWXYZ23".to_owned(),
            recovered_at: "2026-04-28T12:34:56Z".to_owned(),
        };
        write_snapshot_manifest(tmp.path(), &key, &snap, &[], Some(&rec)).unwrap();

        let bytes = std::fs::read(
            tmp.path()
                .join("snapshots")
                .join(snapshot_manifest_filename(&snap)),
        )
        .unwrap();
        let unrelated = SigningKey::generate(&mut OsRng).verifying_key();
        let err = read_snapshot_manifest_from_bytes(&bytes, &unrelated, &snap).unwrap_err();
        assert!(
            err.to_string().contains("signature invalid"),
            "expected sig failure, got: {err}"
        );
    }
}
