//! `VolumeBodySigner` — file-backed [`BodyTokenSigner`] that loads
//! `volume.key` from a fork directory.
//!
//! The volume daemon constructs one of these at startup when peer
//! body-fetch is enabled and the running fork has a local
//! `volume.key`. The signing key never leaves the volume process.

use std::io;
use std::path::Path;

use ed25519_dalek::{Signer, SigningKey};
use ulid::Ulid;

use crate::client::BodyTokenSigner;

/// `volume.key` is a 32-byte Ed25519 secret key, raw on-disk
/// (matching `elide_core::signing::load_keypair`).
const VOLUME_KEY_LEN: usize = 32;

/// File-backed body-fetch signer. Holds the running volume's ULID
/// (so the verifying peer can resolve `by_id/<vol_ulid>/volume.pub`)
/// and its Ed25519 signing key.
#[derive(Debug)]
pub struct VolumeBodySigner {
    vol_ulid: Ulid,
    key: SigningKey,
}

impl VolumeBodySigner {
    /// Load the signing key from `dir/<key_file>`. Returns an error
    /// if the file is missing, unreadable, or not exactly 32 bytes.
    pub fn load(dir: &Path, key_file: &str, vol_ulid: Ulid) -> io::Result<Self> {
        let bytes = std::fs::read(dir.join(key_file))
            .map_err(|e| io::Error::other(format!("{key_file} not readable: {e}")))?;
        let arr: [u8; VOLUME_KEY_LEN] = bytes.try_into().map_err(|_| {
            io::Error::other(format!("{key_file} wrong length (expected 32 bytes)"))
        })?;
        Ok(Self {
            vol_ulid,
            key: SigningKey::from_bytes(&arr),
        })
    }
}

impl BodyTokenSigner for VolumeBodySigner {
    fn vol_ulid(&self) -> Ulid {
        self.vol_ulid
    }

    fn sign(&self, msg: &[u8]) -> [u8; 64] {
        self.key.sign(msg).to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Verifier;
    use rand_core::OsRng;
    use tempfile::TempDir;

    #[test]
    fn load_succeeds_for_valid_key_file() {
        let tmp = TempDir::new().unwrap();
        let key = SigningKey::generate(&mut OsRng);
        std::fs::write(tmp.path().join("volume.key"), key.to_bytes()).unwrap();

        let signer = VolumeBodySigner::load(tmp.path(), "volume.key", Ulid::new())
            .expect("load should succeed");
        let msg = b"hello";
        let sig = signer.sign(msg);
        let verifying = key.verifying_key();
        verifying
            .verify(msg, &ed25519_dalek::Signature::from_bytes(&sig))
            .expect("signature verifies");
    }

    #[test]
    fn load_rejects_missing_file() {
        let tmp = TempDir::new().unwrap();
        let err = VolumeBodySigner::load(tmp.path(), "volume.key", Ulid::new())
            .expect_err("missing file");
        assert!(err.to_string().contains("volume.key"));
    }

    #[test]
    fn load_rejects_wrong_length() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("volume.key"), b"too short").unwrap();
        let err = VolumeBodySigner::load(tmp.path(), "volume.key", Ulid::new())
            .expect_err("wrong length");
        assert!(err.to_string().contains("32 bytes"));
    }

    #[test]
    fn vol_ulid_round_trips() {
        let tmp = TempDir::new().unwrap();
        let key = SigningKey::generate(&mut OsRng);
        std::fs::write(tmp.path().join("volume.key"), key.to_bytes()).unwrap();
        let id = Ulid::new();
        let signer = VolumeBodySigner::load(tmp.path(), "volume.key", id).unwrap();
        assert_eq!(signer.vol_ulid(), id);
    }
}
