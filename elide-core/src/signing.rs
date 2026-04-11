// Ed25519 keypair management and signed provenance.
//
// `volume.provenance` is the single signed statement of what a volume is:
// where it lives, and what other volumes it is related to. All lineage
// relationships (fork parent, extent-index sources) are carried in the
// same file, under the same signature, so tampering with lineage is
// detectable with the volume's own public key.
//
// Key file naming convention (all volumes, flat layout):
//   volume.key / volume.pub / volume.provenance  (under <by_id>/<ulid>/)
//
// File contents:
//   *.key         — Ed25519 private key (32 raw bytes, never uploaded)
//   *.pub         — Ed25519 public key (64 lowercase hex chars + newline, uploaded to S3)
//   *.provenance  — plaintext host + path + lineage + signature (local only)
//
// provenance file format:
//   hostname: <value>
//   path: <canonical absolute path>
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
//   hostname ‖ NUL ‖ path ‖ NUL ‖ parent_or_empty ‖ NUL ‖
//   parent_pubkey_hex_or_empty ‖ NUL ‖
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParentRef {
    /// ULID of the parent volume.
    pub volume_ulid: String,
    /// ULID of the specific snapshot on the parent that this fork branches from.
    pub snapshot_ulid: String,
    /// Parent volume's Ed25519 verifying key at fork time (32 raw bytes).
    pub pubkey: [u8; 32],
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

/// The full verified contents of a `volume.provenance` file.
#[derive(Clone, Debug)]
pub struct Provenance {
    pub hostname: String,
    pub path: String,
    pub lineage: ProvenanceLineage,
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

/// Write a signed provenance file recording hostname, canonical path, and lineage.
///
/// `dir` must already exist — `canonicalize` requires the path to be present.
/// `provenance_file` is the filename within `dir`.
pub fn write_provenance(
    dir: &Path,
    key: &SigningKey,
    provenance_file: &str,
    lineage: &ProvenanceLineage,
) -> io::Result<()> {
    let hostname = get_hostname()?;
    let canonical = std::fs::canonicalize(dir)?;
    let path_str = canonical
        .to_str()
        .ok_or_else(|| io::Error::other("dir path is not valid UTF-8"))?
        .to_owned();

    let sig = sign_provenance(key, &hostname, &path_str, lineage);
    let content = serialize_provenance(&hostname, &path_str, lineage, &sig);
    crate::segment::write_file_atomic(&dir.join(provenance_file), content.as_bytes())
}

/// Verify a provenance file and return the recorded lineage.
///
/// Checks: file readable, parseable, recorded hostname matches the current
/// host, recorded path matches the current canonical path, signature valid
/// under `pub_file`. Returns the full parsed `Provenance` on success.
pub fn verify_provenance(
    dir: &Path,
    pub_file: &str,
    provenance_file: &str,
) -> io::Result<Provenance> {
    let content = std::fs::read_to_string(dir.join(provenance_file)).map_err(|e| {
        io::Error::other(format!(
            "{provenance_file} not readable (was this volume created with an older version?): {e}"
        ))
    })?;

    let (hostname, path, lineage, sig_bytes) = parse_provenance(&content, provenance_file)?;

    let current_hostname = get_hostname()?;
    let canonical = std::fs::canonicalize(dir)?;
    let current_path = canonical
        .to_str()
        .ok_or_else(|| io::Error::other("dir path is not valid UTF-8"))?;

    if hostname != current_hostname {
        return Err(io::Error::other(format!(
            "{provenance_file} hostname mismatch: recorded {hostname:?}, \
             current host is {current_hostname:?}"
        )));
    }
    if path != current_path {
        return Err(io::Error::other(format!(
            "{provenance_file} path mismatch: recorded {path:?}, \
             current canonical path is {current_path:?}"
        )));
    }

    let verifying_key = load_verifying_key(dir, pub_file)?;
    let sig_arr: [u8; 64] = sig_bytes.try_into().map_err(|_| {
        io::Error::other(format!(
            "{provenance_file} sig wrong length (expected 64 bytes)"
        ))
    })?;
    let signature = Signature::from_bytes(&sig_arr);

    let msg = provenance_signing_input(&hostname, &path, &lineage);
    verifying_key.verify(&msg, &signature).map_err(|_| {
        io::Error::other(format!(
            "{provenance_file} signature invalid — {provenance_file} or {pub_file} may have been tampered with"
        ))
    })?;

    Ok(Provenance {
        hostname,
        path,
        lineage,
    })
}

/// Verify `volume.provenance` and return the lineage without re-reading on
/// the caller side. Small convenience for read-only open paths that only
/// care about parent / extent_index after verification.
pub fn load_verified_lineage(
    dir: &Path,
    pub_file: &str,
    provenance_file: &str,
) -> io::Result<ProvenanceLineage> {
    Ok(verify_provenance(dir, pub_file, provenance_file)?.lineage)
}

/// Read lineage from an ancestor volume's provenance, verifying the
/// signature with a **caller-supplied** verifying key rather than the
/// `volume.pub` sitting in the ancestor's directory. Used by the
/// `Volume::open` ancestor walk, where the trust anchor for each step is
/// the `parent_pubkey` embedded in the child's signed provenance — not
/// whatever `volume.pub` happens to be on disk at the ancestor path.
///
/// Host/path checks are skipped (ancestors may have moved).
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
    let (hostname, path, lineage, sig_bytes) = parse_provenance(&content, provenance_file)?;
    let sig_arr: [u8; 64] = sig_bytes.try_into().map_err(|_| {
        io::Error::other(format!(
            "{provenance_file} sig wrong length (expected 64 bytes)"
        ))
    })?;
    let signature = Signature::from_bytes(&sig_arr);
    let msg = provenance_signing_input(&hostname, &path, &lineage);
    verifying_key.verify(&msg, &signature).map_err(|_| {
        io::Error::other(format!(
            "{provenance_file} in {} signature invalid under supplied key",
            dir.display()
        ))
    })?;
    Ok(lineage)
}

/// Read lineage from an ancestor volume's provenance, verifying the Ed25519
/// signature against its `volume.pub` but **skipping** the host and path
/// match checks. Used by `walk_ancestors` / `walk_extent_ancestors` on
/// ancestor volumes that may have been created on a different host or at a
/// different path. Still enforces integrity: the signature chain protects
/// lineage from tampering even across host moves.
pub fn read_lineage_verifying_signature(
    dir: &Path,
    pub_file: &str,
    provenance_file: &str,
) -> io::Result<ProvenanceLineage> {
    let content = std::fs::read_to_string(dir.join(provenance_file)).map_err(|e| {
        io::Error::other(format!(
            "{provenance_file} in {} not readable: {e}",
            dir.display()
        ))
    })?;
    let (hostname, path, lineage, sig_bytes) = parse_provenance(&content, provenance_file)?;
    let verifying_key = load_verifying_key(dir, pub_file)?;
    let sig_arr: [u8; 64] = sig_bytes.try_into().map_err(|_| {
        io::Error::other(format!(
            "{provenance_file} sig wrong length (expected 64 bytes)"
        ))
    })?;
    let signature = Signature::from_bytes(&sig_arr);
    let msg = provenance_signing_input(&hostname, &path, &lineage);
    verifying_key.verify(&msg, &signature).map_err(|_| {
        io::Error::other(format!(
            "{provenance_file} in {} signature invalid",
            dir.display()
        ))
    })?;
    Ok(lineage)
}

// --- internal helpers ---

fn sign_provenance(
    key: &SigningKey,
    hostname: &str,
    path: &str,
    lineage: &ProvenanceLineage,
) -> [u8; 64] {
    key.sign(&provenance_signing_input(hostname, path, lineage))
        .to_bytes()
}

/// Signing input (NUL-separated, fixed field order):
///   hostname || NUL || path || NUL || parent_or_empty || NUL ||
///   parent_pubkey_hex_or_empty || NUL ||
///   entry_1 || NUL || entry_2 || NUL || … || entry_N
///
/// Empty `extent_index` contributes zero trailing entries (the input ends
/// after the parent_pubkey field's terminating NUL).
fn provenance_signing_input(hostname: &str, path: &str, lineage: &ProvenanceLineage) -> Vec<u8> {
    let parent_display = lineage.parent.as_ref().map(ParentRef::to_display);
    let parent_str = parent_display.as_deref().unwrap_or("");
    let parent_pubkey_hex = lineage
        .parent
        .as_ref()
        .map(|p| encode_hex(&p.pubkey))
        .unwrap_or_default();
    let mut total = hostname.len()
        + 1
        + path.len()
        + 1
        + parent_str.len()
        + 1
        + parent_pubkey_hex.len()
        + lineage.extent_index.len();
    for entry in &lineage.extent_index {
        total += entry.len();
    }
    let mut msg = Vec::with_capacity(total);
    msg.extend_from_slice(hostname.as_bytes());
    msg.push(0u8);
    msg.extend_from_slice(path.as_bytes());
    msg.push(0u8);
    msg.extend_from_slice(parent_str.as_bytes());
    msg.push(0u8);
    msg.extend_from_slice(parent_pubkey_hex.as_bytes());
    for entry in &lineage.extent_index {
        msg.push(0u8);
        msg.extend_from_slice(entry.as_bytes());
    }
    msg
}

fn serialize_provenance(
    hostname: &str,
    path: &str,
    lineage: &ProvenanceLineage,
    sig: &[u8; 64],
) -> String {
    let parent_display = lineage.parent.as_ref().map(ParentRef::to_display);
    let parent_str = parent_display.as_deref().unwrap_or("");
    let parent_pubkey_hex = lineage
        .parent
        .as_ref()
        .map(|p| encode_hex(&p.pubkey))
        .unwrap_or_default();
    let mut content = String::new();
    content.push_str("hostname: ");
    content.push_str(hostname);
    content.push('\n');
    content.push_str("path: ");
    content.push_str(path);
    content.push('\n');
    content.push_str("parent: ");
    content.push_str(parent_str);
    content.push('\n');
    content.push_str("parent_pubkey: ");
    content.push_str(&parent_pubkey_hex);
    content.push('\n');
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
) -> io::Result<(String, String, ProvenanceLineage, Vec<u8>)> {
    let mut hostname: Option<String> = None;
    let mut path: Option<String> = None;
    let mut parent_str: Option<Option<String>> = None;
    let mut parent_pubkey_str: Option<Option<String>> = None;
    let mut extent_index: Option<Vec<String>> = None;
    let mut sig: Option<Vec<u8>> = None;

    let mut lines = content.lines().peekable();
    while let Some(line) = lines.next() {
        if let Some(v) = line.strip_prefix("hostname: ") {
            hostname = Some(v.to_owned());
        } else if let Some(v) = line.strip_prefix("path: ") {
            path = Some(v.to_owned());
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

    let hostname = hostname
        .ok_or_else(|| io::Error::other(format!("{provenance_file} missing hostname line")))?;
    let path =
        path.ok_or_else(|| io::Error::other(format!("{provenance_file} missing path line")))?;
    let parent_str = parent_str
        .ok_or_else(|| io::Error::other(format!("{provenance_file} missing parent line")))?;
    let parent_pubkey_str = parent_pubkey_str
        .ok_or_else(|| io::Error::other(format!("{provenance_file} missing parent_pubkey line")))?;
    let extent_index = extent_index.ok_or_else(|| {
        io::Error::other(format!("{provenance_file} missing extent_index section"))
    })?;
    let sig = sig.ok_or_else(|| io::Error::other(format!("{provenance_file} missing sig line")))?;

    let parent = match (parent_str, parent_pubkey_str) {
        (None, None) => None,
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
            Some(ParentRef {
                volume_ulid,
                snapshot_ulid,
                pubkey,
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
        hostname,
        path,
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
//   sig: <hex-encoded 64-byte Ed25519 signature>
//
// ULIDs are sorted lexicographically (= chronologically for ULIDs). The
// signature covers the NUL-separated concatenation of ULIDs in sorted
// order; an empty manifest signs the empty byte string.

/// Build the filename for `snap_ulid`'s segments manifest
/// (`<snap_ulid>.manifest`) inside a volume's `snapshots/` directory.
pub fn snapshot_manifest_filename(snap_ulid: &ulid::Ulid) -> String {
    format!("{snap_ulid}{SNAPSHOT_MANIFEST_SUFFIX}")
}

/// Write a signed snapshot manifest for `snap_ulid`.
///
/// `segment_ulids` is the unsorted list of segment ULIDs belonging to the
/// snapshot — typically every `.idx` present in the volume's `index/`
/// directory. The list is sorted and deduplicated before signing and
/// serialisation.
///
/// Writes `vol_dir/snapshots/<snap_ulid>.manifest` atomically.
pub fn write_snapshot_manifest(
    vol_dir: &Path,
    signer: &dyn SegmentSigner,
    snap_ulid: &ulid::Ulid,
    segment_ulids: &[ulid::Ulid],
) -> io::Result<()> {
    let mut sorted: Vec<String> = segment_ulids.iter().map(|u| u.to_string()).collect();
    sorted.sort();
    sorted.dedup();

    let msg = manifest_signing_input(&sorted);
    let sig = signer.sign(&msg);
    let content = serialize_snapshot_manifest(&sorted, &sig);

    let path = vol_dir
        .join("snapshots")
        .join(snapshot_manifest_filename(snap_ulid));
    crate::segment::write_file_atomic(&path, content.as_bytes())
}

/// Read and verify a snapshot manifest, returning its sorted segment ULIDs.
///
/// `verifying_key` is the Ed25519 verifying key of the volume that signed
/// the manifest — for ancestor verification this comes from the child's
/// `volume.provenance` (the embedded `parent_pubkey`), not from the
/// ancestor directory's own `volume.pub`.
///
/// Fails if the file is missing, unparseable, the signature does not match,
/// or the ULIDs are not in strictly ascending order.
pub fn read_snapshot_manifest(
    vol_dir: &Path,
    verifying_key: &VerifyingKey,
    snap_ulid: &ulid::Ulid,
) -> io::Result<Vec<ulid::Ulid>> {
    let filename = snapshot_manifest_filename(snap_ulid);
    let path = vol_dir.join("snapshots").join(&filename);
    let content = std::fs::read_to_string(&path).map_err(|e| {
        io::Error::other(format!(
            "{filename} in {} not readable: {e}",
            vol_dir.display()
        ))
    })?;

    let (entries_str, sig_bytes) = parse_snapshot_manifest(&content, &filename)?;
    let sig_arr: [u8; 64] = sig_bytes.try_into().map_err(|_| {
        io::Error::other(format!("{filename} sig wrong length (expected 64 bytes)"))
    })?;
    let signature = Signature::from_bytes(&sig_arr);

    let msg = manifest_signing_input(&entries_str);
    verifying_key
        .verify(&msg, &signature)
        .map_err(|_| io::Error::other(format!("{filename} signature invalid")))?;

    // Parse each entry as a typed ULID, enforcing strictly ascending order
    // (sort + dedup is done at write time, so any deviation is tamper or
    // corruption).
    let mut out: Vec<ulid::Ulid> = Vec::with_capacity(entries_str.len());
    for entry in &entries_str {
        let parsed = ulid::Ulid::from_string(entry).map_err(|e| {
            io::Error::other(format!(
                "{filename} contains invalid segment ULID {entry:?}: {e}"
            ))
        })?;
        if let Some(last) = out.last()
            && &parsed <= last
        {
            return Err(io::Error::other(format!(
                "{filename} segment ULIDs not in strictly ascending order at {entry}"
            )));
        }
        out.push(parsed);
    }
    Ok(out)
}

/// Signing input for a snapshot manifest: sorted ULIDs concatenated with
/// NUL separators. Empty manifest signs the empty byte string.
fn manifest_signing_input(sorted_ulids: &[String]) -> Vec<u8> {
    let mut total = sorted_ulids.len().saturating_sub(1);
    for u in sorted_ulids {
        total += u.len();
    }
    let mut msg = Vec::with_capacity(total);
    for (i, u) in sorted_ulids.iter().enumerate() {
        if i > 0 {
            msg.push(0u8);
        }
        msg.extend_from_slice(u.as_bytes());
    }
    msg
}

fn serialize_snapshot_manifest(sorted_ulids: &[String], sig: &[u8; 64]) -> String {
    let mut content = String::new();
    content.push_str("segments:\n");
    for u in sorted_ulids {
        content.push_str("  ");
        content.push_str(u);
        content.push('\n');
    }
    content.push_str("sig: ");
    content.push_str(&encode_hex(sig));
    content.push('\n');
    content
}

fn parse_snapshot_manifest(content: &str, filename: &str) -> io::Result<(Vec<String>, Vec<u8>)> {
    let mut entries: Option<Vec<String>> = None;
    let mut sig: Option<Vec<u8>> = None;

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
        }
    }

    let entries =
        entries.ok_or_else(|| io::Error::other(format!("{filename} missing segments section")))?;
    let sig = sig.ok_or_else(|| io::Error::other(format!("{filename} missing sig line")))?;
    Ok((entries, sig))
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
    let hostname = get_hostname()?;
    let canonical = std::fs::canonicalize(dir)?;
    let path_str = canonical
        .to_str()
        .ok_or_else(|| io::Error::other("dir path is not valid UTF-8"))?
        .to_owned();

    let key = SigningKey::generate(&mut OsRng);
    let pub_hex = encode_hex(&key.verifying_key().to_bytes()) + "\n";
    crate::segment::write_file_atomic(&dir.join(VOLUME_PUB_FILE), pub_hex.as_bytes())?;

    let mut msg = Vec::new();
    msg.extend_from_slice(hostname.as_bytes());
    msg.push(0);
    msg.extend_from_slice(path_str.as_bytes());
    msg.push(0);
    msg.extend_from_slice(raw_parent.as_bytes());
    msg.push(0);
    msg.extend_from_slice(raw_parent_pubkey_hex.as_bytes());
    for entry in extent_index {
        msg.push(0);
        msg.extend_from_slice(entry.as_bytes());
    }
    let sig = key.sign(&msg).to_bytes();

    let mut content = String::new();
    content.push_str("hostname: ");
    content.push_str(&hostname);
    content.push('\n');
    content.push_str("path: ");
    content.push_str(&path_str);
    content.push('\n');
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

fn get_hostname() -> io::Result<String> {
    nix::unistd::gethostname()
        .map_err(io::Error::from)?
        .into_string()
        .map_err(|_| io::Error::other("hostname is not valid UTF-8"))
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_hex(s: &str) -> io::Result<Vec<u8>> {
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

        write_snapshot_manifest(tmp.path(), &key, &snap, &segs).unwrap();
        let got = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap();

        let mut expected = segs.clone();
        expected.sort();
        assert_eq!(got, expected);
    }

    #[test]
    fn snapshot_manifest_empty_list() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();

        let raw_key = SigningKey::generate(&mut OsRng);
        let verifying = raw_key.verifying_key();
        let key = signer_from(raw_key);
        let snap = make_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV");

        write_snapshot_manifest(tmp.path(), &key, &snap, &[]).unwrap();
        let got = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap();
        assert!(got.is_empty());
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

        write_snapshot_manifest(tmp.path(), &key, &snap, &segs).unwrap();
        let got = read_snapshot_manifest(tmp.path(), &verifying, &snap).unwrap();
        assert_eq!(got, vec![dup]);
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
}
