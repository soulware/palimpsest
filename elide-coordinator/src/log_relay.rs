//! Coordinator-side log relay server.
//!
//! Listens on `<data_dir>/log.sock` and forwards every byte received
//! to the coordinator's own `stderr`. Volume processes' tracing layer
//! connects to this socket as a second sink (alongside `elide.log`)
//! so live log output reaches whichever coordinator is currently
//! attached to the operator's terminal — even after a coordinator
//! rolling-restart that re-adopts pre-existing volumes whose stdio
//! was inherited from a now-dead coord. Volumes reconnect on EPIPE,
//! so the next coord rebinding the socket transparently picks up the
//! stream without volume restarts.
//!
//! Suppression: if coord's stderr is already pointing at the same
//! `elide.log` file (typical for `coord start` daemon mode where the
//! CLI redirects stdio to the file), the relay would just funnel
//! volume output back into the file that volumes already write to
//! directly, doubling every line. In that case we skip starting the
//! server entirely; volumes' connect attempts get `ECONNREFUSED` and
//! their relay sink is silently inert, leaving the file as the only
//! log path — which is the right behaviour for a daemon with no
//! attached terminal.

use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tracing::{error, info, warn};

/// Socket filename under `data_dir`. Stale dentry from a previous
/// coordinator is `unlink`'d before bind.
const SOCKET_FILE: &str = "log.sock";

/// Start the log-relay server. No-op (returns `Ok(())`) when coord's
/// stderr already points at the same file as `elide.log`.
pub fn start(data_dir: &Path) -> std::io::Result<()> {
    let log_path = data_dir.join("elide.log");
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    if !stderr_is_distinct_from(&log_file)? {
        info!("[log-relay] stderr already routes to elide.log; relay disabled");
        return Ok(());
    }
    drop(log_file);

    let socket_path = data_dir.join(SOCKET_FILE);
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)?;
    info!("[log-relay] listening on {}", socket_path.display());

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    tokio::spawn(async move {
                        if let Err(e) = forward_to_stderr(stream).await {
                            warn!("[log-relay] connection ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    error!("[log-relay] accept failed: {e}; relay shutting down");
                    break;
                }
            }
        }
    });
    Ok(())
}

/// Drain everything the connected volume writes and copy it to
/// `stderr`. Bypasses tracing entirely — the bytes are already
/// formatted log lines from the sender's tracing-subscriber, so
/// re-rendering would just nest formatting.
async fn forward_to_stderr(mut stream: tokio::net::UnixStream) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        // Lock stderr per-write to avoid interleaving with other
        // forwarders or coord's own tracing output. `write_all` errors
        // are dropped: if coord's stderr is gone (rare; coord is the
        // process owning it), there's nothing useful to do.
        let stderr = std::io::stderr();
        let _ = stderr.lock().write_all(&buf[..n]);
    }
}

fn stderr_is_distinct_from(file: &std::fs::File) -> std::io::Result<bool> {
    let file_stat = unsafe {
        let mut s: nix::libc::stat = std::mem::zeroed();
        if nix::libc::fstat(file.as_raw_fd(), &mut s) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        s
    };
    let stderr_stat = unsafe {
        let mut s: nix::libc::stat = std::mem::zeroed();
        if nix::libc::fstat(nix::libc::STDERR_FILENO, &mut s) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        s
    };
    Ok(file_stat.st_dev != stderr_stat.st_dev || file_stat.st_ino != stderr_stat.st_ino)
}
