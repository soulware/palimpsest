// Fork signing: Ed25519 keypair management and fork.origin sanity check.
//
// Key files (stored in the fork directory, never uploaded):
//   fork.key    — Ed25519 private key (32 bytes)
//   fork.pub    — Ed25519 public key (32 bytes); also uploaded to S3 at fork creation
//   fork.origin — plaintext hostname + canonical path + signature; local only, never uploaded
//
// fork.origin format:
//   hostname: <value>
//   path: <canonical absolute path>
//   sig: <hex-encoded 64-byte Ed25519 signature>
//
// Signing input for fork.origin: hostname_bytes || b"\0" || path_bytes (signed directly,
// not pre-hashed, since Ed25519 handles arbitrary-length messages internally).
//
// Signing input for segments: passed in from segment::SegmentSigner::sign(); the caller
// (segment.rs) pre-hashes with BLAKE3 before calling sign(), so the key just signs
// the 32-byte hash.

use std::io;
use std::path::Path;
use std::sync::Arc;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;

use elide_core::segment::SegmentSigner;

const KEY_FILE: &str = "fork.key";
const PUB_FILE: &str = "fork.pub";
const ORIGIN_FILE: &str = "fork.origin";

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

/// Generate a new Ed25519 keypair for a fork and write `fork.key` + `fork.pub`.
///
/// Called once at fork creation time. Returns the signing key so the caller can
/// immediately write `fork.origin` without re-reading from disk.
pub fn generate_keypair(fork_dir: &Path) -> io::Result<SigningKey> {
    let key = SigningKey::generate(&mut OsRng);
    std::fs::write(fork_dir.join(KEY_FILE), key.to_bytes())?;
    std::fs::write(fork_dir.join(PUB_FILE), key.verifying_key().to_bytes())?;
    Ok(key)
}

/// Load the fork's signing key and return a `SegmentSigner` for use with `Volume::open_with_signer`.
pub fn load_signer(fork_dir: &Path) -> io::Result<Arc<dyn SegmentSigner>> {
    let bytes = std::fs::read(fork_dir.join(KEY_FILE))
        .map_err(|e| io::Error::other(format!("fork.key not readable: {e}")))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| io::Error::other("fork.key wrong length (expected 32 bytes)"))?;
    Ok(Arc::new(Ed25519Signer {
        key: SigningKey::from_bytes(&arr),
    }))
}

// --- fork.origin ---

/// Write `fork.origin` recording the canonical path and hostname at fork creation time.
///
/// `fork_dir` must already exist — `canonicalize` requires the path to be present.
pub fn write_fork_origin(fork_dir: &Path, key: &SigningKey) -> io::Result<()> {
    let hostname = get_hostname()?;
    let canonical = std::fs::canonicalize(fork_dir)?;
    let path_str = canonical
        .to_str()
        .ok_or_else(|| io::Error::other("fork dir path is not valid UTF-8"))?
        .to_owned();

    let sig = sign_origin(key, &hostname, &path_str);
    let content = format!(
        "hostname: {hostname}\npath: {path_str}\nsig: {}\n",
        encode_hex(&sig)
    );
    std::fs::write(fork_dir.join(ORIGIN_FILE), content)
}

/// Verify `fork.origin` against the current hostname and canonical path.
///
/// Returns `Ok(())` if the file is present, correctly signed, and matches the
/// current host and path. Returns `Err` with a human-readable message otherwise.
///
/// Both sides canonicalize the path so symlinks and `..` components do not
/// cause false mismatches or allow bypasses.
pub fn verify_fork_origin(fork_dir: &Path) -> io::Result<()> {
    let content = std::fs::read_to_string(fork_dir.join(ORIGIN_FILE)).map_err(|e| {
        io::Error::other(format!(
            "fork.origin not readable (was this fork created with an older version?): {e}"
        ))
    })?;

    let (recorded_hostname, recorded_path, sig_bytes) = parse_origin(&content)?;

    let current_hostname = get_hostname()?;
    let canonical = std::fs::canonicalize(fork_dir)?;
    let current_path = canonical
        .to_str()
        .ok_or_else(|| io::Error::other("fork dir path is not valid UTF-8"))?;

    if recorded_hostname != current_hostname {
        return Err(io::Error::other(format!(
            "fork.origin hostname mismatch: recorded {recorded_hostname:?}, \
             current host is {current_hostname:?} — \
             use --force-origin if this fork has been intentionally moved"
        )));
    }
    if recorded_path != current_path {
        return Err(io::Error::other(format!(
            "fork.origin path mismatch: recorded {recorded_path:?}, \
             current canonical path is {current_path:?} — \
             use --force-origin if this fork has been intentionally moved"
        )));
    }

    // Verify the signature to confirm the file wasn't tampered with.
    let pub_bytes = std::fs::read(fork_dir.join(PUB_FILE))
        .map_err(|e| io::Error::other(format!("fork.pub not readable: {e}")))?;
    let pub_arr: [u8; 32] = pub_bytes
        .try_into()
        .map_err(|_| io::Error::other("fork.pub wrong length (expected 32 bytes)"))?;
    let verifying_key = VerifyingKey::from_bytes(&pub_arr)
        .map_err(|e| io::Error::other(format!("fork.pub invalid: {e}")))?;

    let sig_arr: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| io::Error::other("fork.origin sig wrong length (expected 64 bytes)"))?;
    let signature = Signature::from_bytes(&sig_arr);

    let msg = origin_message(&recorded_hostname, &recorded_path);
    verifying_key.verify(&msg, &signature).map_err(|_| {
        io::Error::other(
            "fork.origin signature invalid — \
             fork.origin or fork.pub may have been tampered with",
        )
    })
}

// --- internal helpers ---

fn sign_origin(key: &SigningKey, hostname: &str, path: &str) -> [u8; 64] {
    key.sign(&origin_message(hostname, path)).to_bytes()
}

/// Signing input: hostname_bytes || NUL || path_bytes.
fn origin_message(hostname: &str, path: &str) -> Vec<u8> {
    let mut msg = Vec::with_capacity(hostname.len() + 1 + path.len());
    msg.extend_from_slice(hostname.as_bytes());
    msg.push(0u8);
    msg.extend_from_slice(path.as_bytes());
    msg
}

fn parse_origin(content: &str) -> io::Result<(String, String, Vec<u8>)> {
    let mut hostname = None;
    let mut path = None;
    let mut sig = None;

    for line in content.lines() {
        if let Some(v) = line.strip_prefix("hostname: ") {
            hostname = Some(v.to_owned());
        } else if let Some(v) = line.strip_prefix("path: ") {
            path = Some(v.to_owned());
        } else if let Some(v) = line.strip_prefix("sig: ") {
            sig = Some(decode_hex(v)?);
        }
    }

    Ok((
        hostname.ok_or_else(|| io::Error::other("fork.origin missing hostname line"))?,
        path.ok_or_else(|| io::Error::other("fork.origin missing path line"))?,
        sig.ok_or_else(|| io::Error::other("fork.origin missing sig line"))?,
    ))
}

fn get_hostname() -> io::Result<String> {
    let mut buf = [0u8; 256];
    // SAFETY: buf is valid memory of the size passed to gethostname.
    let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8(buf[..len].to_vec())
        .map_err(|e| io::Error::other(format!("hostname is not valid UTF-8: {e}")))
}

fn encode_hex(bytes: &[u8]) -> String {
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
