//! Local backup of a removed volume's Ed25519 signing key, so that a
//! later `volume start` on this host can resurrect the volume as
//! *writable* rather than degrading to a read-only hydration.
//!
//! Written by `volume remove` just before the fork directory is torn
//! down, alongside the `data_dir/remote/<name>` breadcrumb. Read by
//! [`crate::start_remote::hydrate_remote_owned`] when it sees a
//! retained name. Deleted by `volume start` once the volume is back
//! Live (the local fork now owns the key again), or by `volume
//! release` when ownership is given up.
//!
//! On-disk: `data_dir/keys/<vol_ulid>.key` containing 32 raw bytes
//! (the same encoding `volume.key` uses inside a fork directory). The
//! filename keys on the vol ULID rather than the volume name because:
//!
//!   1. A name can be released and re-claimed against a different
//!      lineage — the key shadow must follow the lineage, not the
//!      name.
//!   2. Hydrate already has the `vol_ulid` in hand (it just read it
//!      from `names/<name>` or the breadcrumb); no extra lookup is
//!      needed.
//!
//! Security: `data_dir` is already the trust boundary for all
//! per-volume secrets — `volume.key` lives inside `data_dir/by_id/<vol_ulid>/`
//! with default perms. The shadow inherits the same expectation.

use std::io;
use std::path::{Path, PathBuf};

use ulid::Ulid;

const KEYS_DIR: &str = "keys";

fn keys_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(KEYS_DIR)
}

fn key_path(data_dir: &Path, vol_ulid: Ulid) -> PathBuf {
    keys_dir(data_dir).join(format!("{vol_ulid}.key"))
}

/// Copy the volume's signing key into the shadow. `key_bytes` must be
/// the raw 32-byte Ed25519 private key (the same bytes
/// `volume.key` contains). Overwrites any existing shadow for the same
/// ULID. Creates the parent directory if needed.
pub fn write(data_dir: &Path, vol_ulid: Ulid, key_bytes: &[u8]) -> io::Result<()> {
    std::fs::create_dir_all(keys_dir(data_dir))?;
    elide_core::segment::write_file_atomic(&key_path(data_dir, vol_ulid), key_bytes)
}

/// Read the shadowed key, returning `None` if no shadow exists for
/// this ULID. Bytes are returned as-is — the caller is responsible for
/// length-validation (a corrupt or partial shadow surfaces as a
/// signing failure at write time, not here).
pub fn read(data_dir: &Path, vol_ulid: Ulid) -> io::Result<Option<Vec<u8>>> {
    match std::fs::read(key_path(data_dir, vol_ulid)) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Idempotent removal of the shadow. Missing file is not an error.
pub fn remove(data_dir: &Path, vol_ulid: Ulid) -> io::Result<()> {
    match std::fs::remove_file(key_path(data_dir, vol_ulid)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_ulid() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    fn sample_key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    #[test]
    fn write_then_read_round_trips() {
        let tmp = TempDir::new().unwrap();
        let key = sample_key();
        write(tmp.path(), sample_ulid(), &key).unwrap();
        let got = read(tmp.path(), sample_ulid()).unwrap().unwrap();
        assert_eq!(got, key);
    }

    #[test]
    fn read_returns_none_when_missing() {
        let tmp = TempDir::new().unwrap();
        assert!(read(tmp.path(), sample_ulid()).unwrap().is_none());
    }

    #[test]
    fn remove_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        remove(tmp.path(), sample_ulid()).unwrap();
        write(tmp.path(), sample_ulid(), &sample_key()).unwrap();
        remove(tmp.path(), sample_ulid()).unwrap();
        assert!(read(tmp.path(), sample_ulid()).unwrap().is_none());
    }

    #[test]
    fn write_overwrites_prior_shadow() {
        let tmp = TempDir::new().unwrap();
        write(tmp.path(), sample_ulid(), &[1u8; 32]).unwrap();
        write(tmp.path(), sample_ulid(), &[2u8; 32]).unwrap();
        assert_eq!(read(tmp.path(), sample_ulid()).unwrap().unwrap(), [2u8; 32]);
    }
}
