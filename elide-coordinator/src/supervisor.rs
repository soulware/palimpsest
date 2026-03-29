// Volume process supervision.
//
// For each fork with a serve.toml, the supervisor spawns `elide serve-volume`
// and restarts it if it exits. The spawned process is placed in a new session
// (setsid) so it is not affected by the coordinator's lifetime — the volume
// keeps serving if the coordinator is restarted or upgraded.
//
// State files written to the fork directory:
//   volume.pid  — PID of the running volume process; absent when not running
//
// Re-adoption on coordinator restart:
//   If volume.pid exists and the process is alive, the supervisor polls until
//   it exits, then restarts it. If the pid file is stale (process gone), the
//   file is removed and a fresh process is spawned.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Command;

use crate::serve_config::ServeConfig;

const PID_FILE: &str = "volume.pid";
const RESTART_DELAY: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Supervise a single fork: spawn `elide serve-volume`, restart on exit.
/// Runs indefinitely; cancel the task to stop supervision.
pub async fn supervise(fork_dir: PathBuf, serve_config: ServeConfig, elide_bin: PathBuf) {
    let label = fork_dir.display().to_string();

    loop {
        // Check for a running process left by a previous coordinator session.
        if let Some(pid) = read_pid(&fork_dir) {
            if is_alive(pid) {
                eprintln!("[supervisor {label}] re-adopted running process {pid}");
                poll_until_dead(pid).await;
                eprintln!("[supervisor {label}] process {pid} exited");
                remove_pid(&fork_dir);
                tokio::time::sleep(RESTART_DELAY).await;
                continue;
            }
            // Stale pid file.
            eprintln!("[supervisor {label}] removing stale pid file (pid {pid} not running)");
            remove_pid(&fork_dir);
        }

        match spawn_volume(&fork_dir, &serve_config, &elide_bin) {
            Ok(mut child) => {
                let pid = child.id().unwrap_or(0);
                eprintln!("[supervisor {label}] started pid {pid}");
                write_pid(&fork_dir, pid);
                match child.wait().await {
                    Ok(status) => eprintln!("[supervisor {label}] pid {pid} exited: {status}"),
                    Err(e) => eprintln!("[supervisor {label}] wait error: {e}"),
                }
                remove_pid(&fork_dir);
            }
            Err(e) => {
                eprintln!("[supervisor {label}] failed to spawn: {e:#}");
            }
        }

        tokio::time::sleep(RESTART_DELAY).await;
    }
}

fn spawn_volume(
    fork_dir: &Path,
    serve_config: &ServeConfig,
    elide_bin: &Path,
) -> std::io::Result<tokio::process::Child> {
    let mut cmd = Command::new(elide_bin);
    cmd.arg("serve-volume")
        .arg(fork_dir)
        .arg("--bind")
        .arg(&serve_config.bind);

    if let Some(secs) = serve_config.auto_flush_secs {
        cmd.arg("--auto-flush").arg(secs.to_string());
    }

    if serve_config.readonly.unwrap_or(false) {
        cmd.arg("--readonly");
    }

    // Place the child in a new session so it is not signalled when the
    // coordinator's process group receives SIGHUP or is terminated.
    // pre_exec is unsafe because the callback runs between fork() and exec()
    // where only async-signal-safe functions may be called. setsid() is
    // async-signal-safe.
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| nix::unistd::setsid().map(|_| ()).map_err(io::Error::from));
    }

    cmd.spawn()
}

/// Poll every POLL_INTERVAL until the process is no longer alive.
async fn poll_until_dead(pid: u32) {
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        if !is_alive(pid) {
            break;
        }
    }
}

/// Returns true if the process exists and we have permission to signal it.
fn is_alive(pid: u32) -> bool {
    // Pid::from_raw takes i32; reject u32 values that would overflow.
    let Ok(raw) = i32::try_from(pid) else {
        return false;
    };
    // kill(pid, None) sends signal 0: checks existence without delivering a signal.
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(raw), None).is_ok()
}

fn read_pid(fork_dir: &Path) -> Option<u32> {
    let text = std::fs::read_to_string(fork_dir.join(PID_FILE)).ok()?;
    text.trim().parse().ok()
}

fn write_pid(fork_dir: &Path, pid: u32) {
    if let Err(e) = std::fs::write(fork_dir.join(PID_FILE), pid.to_string()) {
        eprintln!("[supervisor] failed to write pid file: {e}");
    }
}

fn remove_pid(fork_dir: &Path) {
    let _ = std::fs::remove_file(fork_dir.join(PID_FILE));
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn pid_file_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let pid = std::process::id();
        write_pid(tmp.path(), pid);
        assert_eq!(read_pid(tmp.path()), Some(pid));
        remove_pid(tmp.path());
        assert_eq!(read_pid(tmp.path()), None);
    }

    #[test]
    fn is_alive_current_process() {
        assert!(is_alive(std::process::id()));
    }

    #[test]
    fn is_alive_returns_false_for_nonexistent_pid() {
        // PID 0 is the kernel idle process and not a real user process;
        // sending it signal 0 typically fails with EPERM (not ESRCH) since
        // we don't own it. Use u32::MAX as a pid that almost certainly
        // does not exist on any system.
        assert!(!is_alive(u32::MAX));
    }

    #[test]
    fn stale_pid_not_alive() {
        let tmp = TempDir::new().unwrap();
        write_pid(tmp.path(), u32::MAX);
        let pid = read_pid(tmp.path()).unwrap();
        assert!(!is_alive(pid));
    }
}
