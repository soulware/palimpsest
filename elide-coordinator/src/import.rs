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
/// Creates the fork directory (`<vol_dir>/base`), writes `import.lock` and
/// `import.pid`, spawns `elide-import`, and registers the job. Returns the
/// import ULID.
pub async fn spawn_import(
    vol_name: &str,
    oci_ref: &str,
    roots: &[PathBuf],
    elide_import_bin: &Path,
    registry: &ImportRegistry,
) -> std::io::Result<String> {
    let root = roots
        .first()
        .ok_or_else(|| std::io::Error::other("no roots configured"))?;
    let vol_dir = root.join(vol_name);
    let fork_dir = vol_dir.join("base");

    // Reject if a volume process is already running in this fork.
    if let Ok(text) = std::fs::read_to_string(fork_dir.join("volume.pid"))
        && let Ok(pid) = text.trim().parse::<u32>()
        && is_alive(pid)
    {
        return Err(std::io::Error::other(format!(
            "volume process already running for {vol_name}/base (pid {pid})"
        )));
    }

    // Reject if another import is already registered in memory.
    if fork_dir.join(LOCK_FILE).exists() {
        let ulid_text = std::fs::read_to_string(fork_dir.join(LOCK_FILE)).unwrap_or_default();
        let existing = ulid_text.trim().to_owned();
        if registry.lock().await.contains_key(&existing) {
            return Err(std::io::Error::other(format!(
                "import already in progress for {vol_name}: {existing}"
            )));
        }
        // Stale lock — clean up and proceed.
        warn!("[import] removing stale lock for {vol_name}: {existing}");
        let _ = std::fs::remove_file(fork_dir.join(LOCK_FILE));
        let _ = std::fs::remove_file(fork_dir.join(PID_FILE));
    }

    std::fs::create_dir_all(&fork_dir)?;

    let ulid = Ulid::new().to_string();
    std::fs::write(fork_dir.join(LOCK_FILE), &ulid)?;

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
    std::fs::write(fork_dir.join(PID_FILE), pid.to_string())?;

    let job = ImportJob::new(fork_dir.clone(), pid);
    registry.lock().await.insert(ulid.clone(), job.clone());

    let ulid_clone = ulid.clone();
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
                info!("[import {ulid_clone}] done");
                ImportState::Done
            }
            Ok(s) => {
                let msg = format!("exited with {s}");
                warn!("[import {ulid_clone}] failed: {msg}");
                ImportState::Failed(msg)
            }
            Err(e) => {
                let msg = format!("wait error: {e}");
                warn!("[import {ulid_clone}] failed: {msg}");
                ImportState::Failed(msg)
            }
        };

        job.finish(final_state).await;
        let _ = std::fs::remove_file(fork_dir.join(LOCK_FILE));
        let _ = std::fs::remove_file(fork_dir.join(PID_FILE));
    });

    info!("[import {ulid}] started pid {pid} for {vol_name} from {oci_ref}");
    Ok(ulid)
}

/// On coordinator startup, remove stale `import.lock` files.
///
/// A lock is stale if no live process matches `import.pid`. If a process is
/// found alive, it is sent SIGTERM so the fork is in a clean state for retry.
pub fn cleanup_stale_locks(roots: &[PathBuf]) {
    for root in roots {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        for vol_entry in entries.flatten() {
            let vol_path = vol_entry.path();
            if !vol_path.is_dir() {
                continue;
            }
            cleanup_stale_lock_in(&vol_path.join("base"));
            if let Ok(fork_entries) = std::fs::read_dir(vol_path.join("forks")) {
                for fork_entry in fork_entries.flatten() {
                    cleanup_stale_lock_in(&fork_entry.path());
                }
            }
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

/// Send SIGTERM to all volume and import processes under `vol_dir`, then wait
/// briefly for them to exit. Used by the `delete` operation.
pub fn kill_all_for_volume(vol_dir: &Path) {
    // Kill volume processes under forks/ and base/.
    let fork_dirs: Vec<PathBuf> = {
        let mut dirs = vec![vol_dir.join("base")];
        if let Ok(entries) = std::fs::read_dir(vol_dir.join("forks")) {
            dirs.extend(entries.flatten().map(|e| e.path()));
        }
        dirs
    };

    for fork_dir in &fork_dirs {
        // Kill volume process.
        if let Ok(text) = std::fs::read_to_string(fork_dir.join("volume.pid"))
            && let Ok(pid) = text.trim().parse::<u32>()
            && is_alive(pid)
        {
            sigterm(pid);
            info!(
                "[import] sent SIGTERM to volume process pid={pid} in {}",
                fork_dir.display()
            );
        }
        // Kill import process.
        if kill_import(fork_dir) {
            info!(
                "[import] sent SIGTERM to import process in {}",
                fork_dir.display()
            );
        }
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
