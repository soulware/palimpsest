// Import process supervision and job registry.
//
// The coordinator spawns `elide-import` as a short-lived child process for OCI
// volume imports. Two marker files are written to the fork directory:
//
//   import.lock — ULID of the running import (one line); present while running or interrupted
//   import.pid  — PID of the running import process (one line)
//
// The coordinator creates import.lock before spawning and removes both files
// when the process exits (success or failure). On coordinator startup,
// cleanup_stale_locks() kills any surviving import processes and removes stale
// lock files so forks are in a clean, resumable state.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncBufReadExt;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use ulid::Ulid;

pub const LOCK_FILE: &str = "import.lock";

/// Validate a volume name: non-empty, only `[a-zA-Z0-9._-]`.
fn validate_volume_name(name: &str) -> std::io::Result<()> {
    if name.is_empty() {
        return Err(std::io::Error::other("volume name must not be empty"));
    }
    if let Some(c) = name
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_' && *c != '.')
    {
        return Err(std::io::Error::other(format!(
            "invalid character {c:?} in volume name {name:?}: only [a-zA-Z0-9._-] allowed"
        )));
    }
    Ok(())
}
const PID_FILE: &str = "import.pid";

#[derive(Clone, Debug)]
pub enum ImportState {
    Running,
    Done,
    Failed(String),
}

pub struct ImportJob {
    /// Fork directory being written by the import process.
    #[allow(dead_code)]
    pub fork_dir: PathBuf,
    /// PID of the running import process; useful for diagnostics.
    #[allow(dead_code)]
    pub pid: u32,
    lines: Mutex<Vec<String>>,
    pub state: RwLock<ImportState>,
}

impl ImportJob {
    fn new(fork_dir: PathBuf, pid: u32) -> Arc<Self> {
        Arc::new(Self {
            fork_dir,
            pid,
            lines: Mutex::new(Vec::new()),
            state: RwLock::new(ImportState::Running),
        })
    }

    async fn append(&self, line: String) {
        self.lines.lock().await.push(line);
    }

    async fn finish(&self, state: ImportState) {
        *self.state.write().await = state;
    }

    /// Return output lines starting from `offset`.
    pub async fn read_from(&self, offset: usize) -> Vec<String> {
        self.lines.lock().await[offset..].to_vec()
    }

    pub async fn state(&self) -> ImportState {
        self.state.read().await.clone()
    }
}

pub type ImportRegistry = Arc<Mutex<HashMap<String, Arc<ImportJob>>>>;

pub fn new_registry() -> ImportRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Spawn an import process for `vol_name` using OCI image `oci_ref`.
///
/// Generates a ULID for the new volume, creates `<data_dir>/by_id/<ulid>/`,
/// writes `volume.name`, `volume.readonly`, and `import.lock`, then spawns
/// `elide-import`. On success, creates the `<data_dir>/by_name/<vol_name>`
/// symlink. Returns the import job ULID.
pub async fn spawn_import(
    vol_name: &str,
    oci_ref: &str,
    data_dir: &Path,
    elide_import_bin: &Path,
    registry: &ImportRegistry,
) -> std::io::Result<String> {
    validate_volume_name(vol_name)?;

    let by_name_dir = data_dir.join("by_name");
    let symlink_path = by_name_dir.join(vol_name);

    // Reject if a volume with this name already exists.
    // Use is_symlink() || exists() so that a broken symlink (target deleted)
    // is still treated as "name in use" rather than silently overwritten.
    if symlink_path.is_symlink() || symlink_path.exists() {
        return Err(std::io::Error::other(format!(
            "volume already exists: {vol_name}"
        )));
    }

    // Generate a stable ULID for this volume (= S3 prefix).
    let vol_ulid = Ulid::new().to_string();
    let vol_dir = data_dir.join("by_id").join(&vol_ulid);

    std::fs::create_dir_all(&by_name_dir)?;
    std::fs::create_dir_all(&vol_dir)?;
    // Write volume.readonly immediately so a crashed import is never supervised
    // as a writable volume.
    std::fs::write(vol_dir.join("volume.readonly"), "")?;
    std::fs::write(vol_dir.join("volume.name"), vol_name)?;

    // Write the import lock.
    let import_ulid = Ulid::new().to_string();
    std::fs::write(vol_dir.join(LOCK_FILE), &import_ulid)?;

    // Create the by_name symlink immediately so `import status/attach` can
    // resolve the volume before the import completes. Removed on failure.
    std::os::unix::fs::symlink(format!("../by_id/{vol_ulid}"), &symlink_path)?;

    let mut cmd = tokio::process::Command::new(elide_import_bin);
    cmd.arg(&vol_dir)
        .arg("--image")
        .arg(oci_ref)
        .stderr(Stdio::piped())
        .stdout(Stdio::null());

    // Place the child in a new session so it is not affected by the
    // coordinator's lifetime. pre_exec is unsafe because the callback runs
    // between fork() and exec() where only async-signal-safe functions may be
    // called. setsid() is async-signal-safe.
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(std::io::Error::from)
        });
    }

    let mut child = cmd.spawn().map_err(|e| {
        std::io::Error::other(format!(
            "failed to spawn {}: {e}",
            elide_import_bin.display()
        ))
    })?;

    let pid = child.id().unwrap_or(0);
    std::fs::write(vol_dir.join(PID_FILE), pid.to_string())?;

    let job = ImportJob::new(vol_dir.clone(), pid);
    registry
        .lock()
        .await
        .insert(import_ulid.clone(), job.clone());

    let import_ulid_clone = import_ulid.clone();
    tokio::spawn(async move {
        // Stream stderr into the job's output buffer.
        if let Some(stderr) = child.stderr.take() {
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                job.append(line).await;
            }
        }

        // Wait for the process to exit.
        let final_state = match child.wait().await {
            Ok(s) if s.success() => {
                info!("[import {import_ulid_clone}] done");
                ImportState::Done
            }
            Ok(s) => {
                let msg = format!("exited with {s}");
                warn!("[import {import_ulid_clone}] failed: {msg}");
                // Remove the by_name symlink so the name is not reserved by a
                // failed import (the vol_dir is left for post-mortem inspection).
                let _ = std::fs::remove_file(&symlink_path);
                ImportState::Failed(msg)
            }
            Err(e) => {
                let msg = format!("wait error: {e}");
                warn!("[import {import_ulid_clone}] failed: {msg}");
                let _ = std::fs::remove_file(&symlink_path);
                ImportState::Failed(msg)
            }
        };

        job.finish(final_state).await;
        let _ = std::fs::remove_file(vol_dir.join(LOCK_FILE));
        let _ = std::fs::remove_file(vol_dir.join(PID_FILE));
    });

    info!("[import {import_ulid}] started pid {pid} for {vol_name} from {oci_ref}");
    Ok(import_ulid)
}

/// On coordinator startup, remove stale `import.lock` files.
///
/// A lock is stale if no live process matches `import.pid`. If a process is
/// found alive, it is sent SIGTERM so the volume is in a clean state for retry.
pub fn cleanup_stale_locks(data_dir: &Path) {
    let by_id_dir = data_dir.join("by_id");
    let Ok(entries) = std::fs::read_dir(&by_id_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            cleanup_stale_lock_in(&path);
        }
    }
}

fn cleanup_stale_lock_in(dir: &Path) {
    let lock_path = dir.join(LOCK_FILE);
    if !lock_path.exists() {
        return;
    }

    let ulid = std::fs::read_to_string(&lock_path)
        .unwrap_or_default()
        .trim()
        .to_owned();

    // Check if the recorded import process is still alive.
    let pid = std::fs::read_to_string(dir.join(PID_FILE))
        .ok()
        .and_then(|t| t.trim().parse::<u32>().ok());

    if let Some(pid) = pid
        && is_alive(pid)
    {
        sigterm(pid);
        warn!(
            "[import] killed stale import process pid={pid} in {} (ulid={ulid})",
            dir.display()
        );
    }

    warn!(
        "[import] removing stale import.lock in {} (ulid={ulid})",
        dir.display()
    );
    let _ = std::fs::remove_file(&lock_path);
    let _ = std::fs::remove_file(dir.join(PID_FILE));
}

/// Send SIGTERM to the import process in `fork_dir`, if one is recorded.
/// Returns true if a signal was sent.
pub fn kill_import(fork_dir: &Path) -> bool {
    let Ok(text) = std::fs::read_to_string(fork_dir.join(PID_FILE)) else {
        return false;
    };
    let Ok(pid) = text.trim().parse::<u32>() else {
        return false;
    };
    if !is_alive(pid) {
        return false;
    }
    sigterm(pid);
    true
}

/// Send SIGTERM to the volume and import processes in `fork_dir`.
///
/// Returns the PIDs that were signalled so the caller can wait for them
/// to exit. Used for clean coordinator shutdown in foreground mode.
pub fn terminate_fork_processes(fork_dir: &Path) -> Vec<u32> {
    let mut pids = Vec::new();
    let label = fork_dir.display();

    if let Ok(text) = std::fs::read_to_string(fork_dir.join("volume.pid"))
        && let Ok(pid) = text.trim().parse::<u32>()
        && is_alive(pid)
    {
        sigterm(pid);
        info!("[coordinator] SIGTERM volume pid={pid} in {label}");
        pids.push(pid);
    }

    if let Ok(text) = std::fs::read_to_string(fork_dir.join(PID_FILE))
        && let Ok(pid) = text.trim().parse::<u32>()
        && is_alive(pid)
    {
        sigterm(pid);
        info!("[coordinator] SIGTERM import pid={pid} in {label}");
        pids.push(pid);
    }

    pids
}

/// Send SIGTERM to the volume and import processes in `vol_dir`, then wait
/// briefly for them to exit. Used by the `delete` operation.
pub fn kill_all_for_volume(vol_dir: &Path) {
    if let Ok(text) = std::fs::read_to_string(vol_dir.join("volume.pid"))
        && let Ok(pid) = text.trim().parse::<u32>()
        && is_alive(pid)
    {
        sigterm(pid);
        info!(
            "[import] sent SIGTERM to volume process pid={pid} in {}",
            vol_dir.display()
        );
    }
    if kill_import(vol_dir) {
        info!(
            "[import] sent SIGTERM to import process in {}",
            vol_dir.display()
        );
    }

    // Brief pause to allow processes to exit before we remove the directory.
    std::thread::sleep(Duration::from_millis(500));
}

fn is_alive(pid: u32) -> bool {
    let Ok(raw) = i32::try_from(pid) else {
        return false;
    };
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(raw), None).is_ok()
}

fn sigterm(pid: u32) {
    if let Ok(raw) = i32::try_from(pid) {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(raw),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
}
