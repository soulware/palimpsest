//! Breadcrumbs for volumes whose name we still own remotely but whose
//! local fork has been removed. One TOML file per name under
//! `data_dir/remote/<name>`.
//!
//! Written by `volume remove` when the bucket-side `names/<name>` record
//! exists and is owned by this coordinator. Surfaced by `volume list` as
//! a `remote` row, so the user can see at a glance which of their
//! volumes are no longer hydrated locally. Removed when the volume
//! regains a local presence (claim, start --remote) or when ownership
//! is given up (`release --force`).
//!
//! Best-effort: not authoritative. The bucket name record is always the
//! source of truth; a stale or missing breadcrumb only affects what
//! `volume list` shows. Subsequent `start`/`claim` operations re-read
//! the bucket and surface the correct error if the breadcrumb lies.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

const REMOTE_DIR: &str = "remote";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteBreadcrumb {
    pub volume_id: Ulid,
    pub removed_at: String,
}

fn breadcrumb_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(REMOTE_DIR)
}

fn breadcrumb_path(data_dir: &Path, name: &str) -> PathBuf {
    breadcrumb_dir(data_dir).join(name)
}

/// Write `data_dir/remote/<name>` with the given volume_id. Stamps
/// `removed_at` with the current UTC time. Creates the parent directory
/// if needed. Overwrites any prior breadcrumb for the same name.
pub fn write(data_dir: &Path, name: &str, volume_id: Ulid) -> io::Result<()> {
    let dir = breadcrumb_dir(data_dir);
    std::fs::create_dir_all(&dir)?;
    let record = RemoteBreadcrumb {
        volume_id,
        removed_at: chrono::Utc::now().to_rfc3339(),
    };
    let body = toml::to_string(&record)
        .map_err(|e| io::Error::other(format!("serialise remote breadcrumb: {e}")))?;
    std::fs::write(breadcrumb_path(data_dir, name), body)
}

/// Idempotent removal of `data_dir/remote/<name>`. Missing file is not
/// an error.
pub fn remove(data_dir: &Path, name: &str) -> io::Result<()> {
    match std::fs::remove_file(breadcrumb_path(data_dir, name)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Read `data_dir/remote/<name>`, returning `None` if absent.
pub fn read(data_dir: &Path, name: &str) -> io::Result<Option<RemoteBreadcrumb>> {
    let path = breadcrumb_path(data_dir, name);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let s = std::str::from_utf8(&bytes)
        .map_err(|e| io::Error::other(format!("breadcrumb {name} not utf8: {e}")))?;
    let record: RemoteBreadcrumb =
        toml::from_str(s).map_err(|e| io::Error::other(format!("parse breadcrumb {name}: {e}")))?;
    Ok(Some(record))
}

/// Enumerate all `data_dir/remote/*` entries. Skips files that fail to
/// parse (a hand-edited or partially-written breadcrumb shouldn't break
/// `volume list`). Returns `(name, record)` pairs.
pub fn list(data_dir: &Path) -> io::Result<Vec<(String, RemoteBreadcrumb)>> {
    let dir = breadcrumb_dir(data_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Ok(Some(record)) = read(data_dir, &name) {
            out.push((name, record));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_ulid() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    #[test]
    fn write_then_read_round_trips() {
        let tmp = TempDir::new().unwrap();
        write(tmp.path(), "vol", sample_ulid()).unwrap();
        let got = read(tmp.path(), "vol").unwrap().unwrap();
        assert_eq!(got.volume_id, sample_ulid());
        assert!(!got.removed_at.is_empty());
    }

    #[test]
    fn read_returns_none_when_missing() {
        let tmp = TempDir::new().unwrap();
        assert!(read(tmp.path(), "ghost").unwrap().is_none());
    }

    #[test]
    fn remove_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        remove(tmp.path(), "ghost").unwrap();
        write(tmp.path(), "vol", sample_ulid()).unwrap();
        remove(tmp.path(), "vol").unwrap();
        assert!(read(tmp.path(), "vol").unwrap().is_none());
    }

    #[test]
    fn list_returns_entries_sorted() {
        let tmp = TempDir::new().unwrap();
        write(tmp.path(), "beta", sample_ulid()).unwrap();
        write(tmp.path(), "alpha", sample_ulid()).unwrap();
        let entries = list(tmp.path()).unwrap();
        let names: Vec<_> = entries.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn list_returns_empty_when_dir_missing() {
        let tmp = TempDir::new().unwrap();
        let entries = list(tmp.path()).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn list_skips_unparseable_entries() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join(REMOTE_DIR)).unwrap();
        std::fs::write(tmp.path().join(REMOTE_DIR).join("garbage"), b"not toml").unwrap();
        write(tmp.path(), "good", sample_ulid()).unwrap();
        let entries = list(tmp.path()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "good");
    }
}
