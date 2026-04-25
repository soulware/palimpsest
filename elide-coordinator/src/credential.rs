// Coordinator root-key persistence and pluggable credential issuance.
//
// The root key is generated on first start and stored at
// `<data_dir>/coordinator.root_key` (mode 0600). It is the only material
// needed to mint and verify macaroons; verification is stateless.
//
// `CredentialIssuer` is the abstraction used by the inbound `credentials`
// handler to turn an authenticated request into an S3 access triple. The
// minimum-viable backend is `SharedKeyPassthrough`, which returns the
// coordinator's own configured key — a "downgrade mode" with no per-volume
// IAM scoping, equivalent to today's `get-store-creds` behaviour but
// reached through the macaroon handshake. Per-volume backends (AWS STS,
// Tigris IAM) are planned and slot in behind this same trait.

use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use rand_core::{OsRng, RngCore};
use tracing::{info, warn};

const ROOT_KEY_FILE: &str = "coordinator.root_key";
const ROOT_KEY_LEN: usize = 32;

/// Load the coordinator root key from `<data_dir>/coordinator.root_key`,
/// generating a fresh one (32 random bytes, mode 0600) on first start.
pub fn load_or_generate_root_key(data_dir: &Path) -> io::Result<[u8; 32]> {
    let path = data_dir.join(ROOT_KEY_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => {
            if bytes.len() != ROOT_KEY_LEN {
                return Err(io::Error::other(format!(
                    "{} has wrong length (expected {ROOT_KEY_LEN}, got {})",
                    path.display(),
                    bytes.len()
                )));
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            Ok(key)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut key = [0u8; 32];
            OsRng.fill_bytes(&mut key);
            // Atomic create+rename so a crashed write can't leave a partial
            // key. `create_new` plus `rename` over the final name is enough:
            // a half-written tmp will be ignored on the next start.
            let tmp = path.with_extension("tmp");
            // Best-effort: clear any leftover tmp from a previous crash so
            // create_new doesn't fail spuriously.
            let _ = std::fs::remove_file(&tmp);
            {
                let mut f = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o600)
                    .open(&tmp)?;
                f.write_all(&key)?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp, &path)?;
            info!("[coordinator] generated root key at {}", path.display());
            Ok(key)
        }
        Err(e) => Err(e),
    }
}

/// Credentials issued to a volume in response to an authenticated
/// `credentials` request.
pub struct IssuedCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    /// Unix-seconds expiry, when the issuer supports time-bounded creds.
    /// `None` means the credentials are long-lived (e.g. the shared-key
    /// passthrough issuer); the caller should not synthesize an expiry
    /// in that case.
    pub expiry_unix: Option<u64>,
}

/// Backend abstraction for the inbound `credentials` op. Implementations
/// see only the volume ULID and the coordinator's configured store; the
/// macaroon handshake (volume binding, PID check, MAC verify) runs
/// upstream and is identical for every backend.
pub trait CredentialIssuer: Send + Sync {
    fn issue(&self, volume_id: &str) -> io::Result<IssuedCredentials>;
}

/// Returns the coordinator's own configured access key for every volume.
/// No per-volume scoping; logs a downgrade warning at startup. This is
/// the "S3-compatible without STS or per-key IAM" row of the design doc
/// and is the minimum viable issuer — per-volume backends slot in behind
/// the same trait without changing the IPC handshake.
pub struct SharedKeyPassthrough;

impl SharedKeyPassthrough {
    pub fn new_with_warning() -> Self {
        warn!(
            "[coordinator] credential issuer: shared-key passthrough \
             (downgrade mode — same key vended to every volume; no per-volume IAM scoping)"
        );
        Self
    }
}

impl CredentialIssuer for SharedKeyPassthrough {
    fn issue(&self, _volume_id: &str) -> io::Result<IssuedCredentials> {
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| io::Error::other("AWS_ACCESS_KEY_ID not set in coordinator env"))?;
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| io::Error::other("AWS_SECRET_ACCESS_KEY not set in coordinator env"))?;
        let session_token = std::env::var("AWS_SESSION_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        Ok(IssuedCredentials {
            access_key_id,
            secret_access_key,
            session_token,
            expiry_unix: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    #[test]
    fn load_or_generate_creates_key_with_0600_perms() {
        let tmp = TempDir::new().unwrap();
        let key = load_or_generate_root_key(tmp.path()).unwrap();
        // Key is non-zero (extremely unlikely to have generated all zeros).
        assert!(key.iter().any(|b| *b != 0));

        let path = tmp.path().join(ROOT_KEY_FILE);
        let meta = std::fs::metadata(&path).unwrap();
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "root key file should be mode 0600, got {mode:o}"
        );
    }

    #[test]
    fn load_or_generate_returns_same_key_on_second_call() {
        let tmp = TempDir::new().unwrap();
        let key1 = load_or_generate_root_key(tmp.path()).unwrap();
        let key2 = load_or_generate_root_key(tmp.path()).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn load_or_generate_rejects_wrong_length_file() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join(ROOT_KEY_FILE), b"too short").unwrap();
        assert!(load_or_generate_root_key(tmp.path()).is_err());
    }
}
