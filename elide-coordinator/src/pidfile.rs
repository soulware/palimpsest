//! RAII pidfile guard for coordinator-spawned subprocesses.
//!
//! `PidFileGuard::write` records a child PID at a path; `Drop` removes
//! the file on every exit path including panic. Used by `import` and
//! `fetch` so a stale pidfile never outlives the process it names.
//!
//! The guard is intentionally minimal: it owns the file's lifecycle,
//! nothing else. Role-specific behaviour (which file, what IPC verb
//! cross-checks the recorded PID, what scope the resulting macaroon
//! gets) lives in the call sites.

use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub struct PidFileGuard {
    path: PathBuf,
}

impl PidFileGuard {
    pub fn write(path: PathBuf, pid: u32) -> io::Result<Self> {
        std::fs::write(&path, pid.to_string())?;
        Ok(Self { path })
    }
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_creates_file_with_pid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("worker.pid");
        let _guard = PidFileGuard::write(path.clone(), 4242).unwrap();
        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body, "4242");
    }

    #[test]
    fn drop_removes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("worker.pid");
        {
            let _guard = PidFileGuard::write(path.clone(), 1).unwrap();
            assert!(path.exists());
        }
        assert!(!path.exists());
    }

    #[test]
    fn drop_tolerates_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("worker.pid");
        let guard = PidFileGuard::write(path.clone(), 1).unwrap();
        std::fs::remove_file(&path).unwrap();
        drop(guard);
    }

    #[test]
    fn write_propagates_io_error() {
        let path = PathBuf::from("/definitely/does/not/exist/worker.pid");
        let err = PidFileGuard::write(path, 1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
