// elide-signing: Ed25519 keypair management and origin sanity checks.
//
// Used by both the `elide` binary (fork keys) and `elide-import` (base keys).
//
// Key file naming conventions:
//   Forks:  fork.key / fork.pub / fork.origin  (under <vol_dir>/forks/<name>/)
//   Base:   base.key / base.pub / base.origin  (under <vol_dir>/)
//
// File contents:
//   *.key    — Ed25519 private key (32 bytes, never uploaded)
//   *.pub    — Ed25519 public key (32 bytes, uploaded to S3)
//   *.origin — plaintext hostname + canonical path + signature (local only)
//
// origin file format:
//   hostname: <value>
//   path: <canonical absolute path>
//   sig: <hex-encoded 64-byte Ed25519 signature>
//
// Signing input for origin:  hostname_bytes || b"\0" || path_bytes
// Signing input for segments: passed in from segment::SegmentSigner::sign(); the caller
//   (segment.rs) pre-hashes with BLAKE3 before calling sign(), so the key signs
//   the 32-byte hash.

use std::io;
use std::path::Path;
use std::sync::Arc;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;

use elide_core::segment::SegmentSigner;

// File name constants.
pub const FORK_KEY_FILE: &str = "fork.key";
pub const FORK_PUB_FILE: &str = "fork.pub";
pub const FORK_ORIGIN_FILE: &str = "fork.origin";
pub const BASE_KEY_FILE: &str = "base.key";
pub const BASE_PUB_FILE: &str = "base.pub";
pub const BASE_ORIGIN_FILE: &str = "base.origin";

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
    std::fs::write(dir.join(key_file), key.to_bytes())?;
    std::fs::write(dir.join(pub_file), key.verifying_key().to_bytes())?;
    Ok(key)
}

/// Load an Ed25519 signing key from `dir/<key_file>` and return a `SegmentSigner`.
pub fn load_signer(dir: &Path, key_file: &str) -> io::Result<Arc<dyn SegmentSigner>> {
    let bytes = std::fs::read(dir.join(key_file))
        .map_err(|e| io::Error::other(format!("{key_file} not readable: {e}")))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| io::Error::other(format!("{key_file} wrong length (expected 32 bytes)")))?;
    Ok(Arc::new(Ed25519Signer {
        key: SigningKey::from_bytes(&arr),
    }))
}

// --- origin files ---

/// Write an origin file recording the canonical path and hostname.
///
/// `dir` must already exist — `canonicalize` requires the path to be present.
/// `origin_file` is the filename within `dir` (e.g. `"fork.origin"` or `"base.origin"`).
pub fn write_origin(dir: &Path, key: &SigningKey, origin_file: &str) -> io::Result<()> {
    let hostname = get_hostname()?;
    let canonical = std::fs::canonicalize(dir)?;
    let path_str = canonical
        .to_str()
        .ok_or_else(|| io::Error::other("dir path is not valid UTF-8"))?
        .to_owned();

    let sig = sign_origin(key, &hostname, &path_str);
    let content = format!(
        "hostname: {hostname}\npath: {path_str}\nsig: {}\n",
        encode_hex(&sig)
    );
    std::fs::write(dir.join(origin_file), content)
}

/// Verify an origin file against the current hostname and canonical path.
///
/// Returns `Ok(())` if the file is present, correctly signed, and matches the
/// current host and path. Returns `Err` with a human-readable message otherwise.
///
/// `pub_file` and `origin_file` are filenames within `dir`.
pub fn verify_origin(dir: &Path, pub_file: &str, origin_file: &str) -> io::Result<()> {
    let content = std::fs::read_to_string(dir.join(origin_file)).map_err(|e| {
        io::Error::other(format!(
            "{origin_file} not readable (was this volume created with an older version?): {e}"
        ))
    })?;

    let (recorded_hostname, recorded_path, sig_bytes) = parse_origin(&content, origin_file)?;

    let current_hostname = get_hostname()?;
    let canonical = std::fs::canonicalize(dir)?;
    let current_path = canonical
        .to_str()
        .ok_or_else(|| io::Error::other("dir path is not valid UTF-8"))?;

    if recorded_hostname != current_hostname {
        return Err(io::Error::other(format!(
            "{origin_file} hostname mismatch: recorded {recorded_hostname:?}, \
             current host is {current_hostname:?}"
        )));
    }
    if recorded_path != current_path {
        return Err(io::Error::other(format!(
            "{origin_file} path mismatch: recorded {recorded_path:?}, \
             current canonical path is {current_path:?}"
        )));
    }

    let pub_bytes = std::fs::read(dir.join(pub_file))
        .map_err(|e| io::Error::other(format!("{pub_file} not readable: {e}")))?;
    let pub_arr: [u8; 32] = pub_bytes
        .try_into()
        .map_err(|_| io::Error::other(format!("{pub_file} wrong length (expected 32 bytes)")))?;
    let verifying_key = VerifyingKey::from_bytes(&pub_arr)
        .map_err(|e| io::Error::other(format!("{pub_file} invalid: {e}")))?;

    let sig_arr: [u8; 64] = sig_bytes.try_into().map_err(|_| {
        io::Error::other(format!(
            "{origin_file} sig wrong length (expected 64 bytes)"
        ))
    })?;
    let signature = Signature::from_bytes(&sig_arr);

    let msg = origin_message(&recorded_hostname, &recorded_path);
    verifying_key.verify(&msg, &signature).map_err(|_| {
        io::Error::other(format!(
            "{origin_file} signature invalid — {origin_file} or {pub_file} may have been tampered with"
        ))
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

fn parse_origin(content: &str, origin_file: &str) -> io::Result<(String, String, Vec<u8>)> {
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
        hostname.ok_or_else(|| io::Error::other(format!("{origin_file} missing hostname line")))?,
        path.ok_or_else(|| io::Error::other(format!("{origin_file} missing path line")))?,
        sig.ok_or_else(|| io::Error::other(format!("{origin_file} missing sig line")))?,
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
