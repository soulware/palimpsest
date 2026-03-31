// Coordinator-side client for the volume control socket.
//
// The volume process listens on <fork_dir>/control.sock.  Each operation is a
// single-connection round trip: send one line, receive one line, close.
//
// Protocol:
//   Request:  "<op>\n"
//   Success:  "ok <value...>\n"
//   Error:    "err <message>\n"
//
// Supported operations:
//   gc_checkpoint  →  "ok <repack_ulid> <sweep_ulid>"
//     Flushes the volume WAL and returns two ULIDs for GC output segments.
//     The two ULIDs are generated 2ms apart on the volume side to ensure
//     strict ordering and distinct timestamps.

use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::warn;

/// Call gc_checkpoint on the volume process for `fork_dir`.
///
/// Returns `Some((repack_ulid, sweep_ulid))` on success.
/// Returns `None` and logs a warning if the socket is absent (volume not
/// running) or if the call fails for any reason.
pub async fn gc_checkpoint(fork_dir: &Path) -> Option<(String, String)> {
    let socket = fork_dir.join("control.sock");
    if !socket.exists() {
        warn!(
            "[control] volume not reachable for {}: control.sock absent — skipping GC",
            fork_dir.display()
        );
        return None;
    }
    match call(&socket, "gc_checkpoint").await {
        Ok(response) => {
            let mut parts = response.splitn(2, ' ');
            let u1 = parts.next()?.to_owned();
            let u2 = parts.next()?.to_owned();
            if u1.is_empty() || u2.is_empty() {
                warn!(
                    "[control] gc_checkpoint for {} returned malformed response",
                    fork_dir.display()
                );
                return None;
            }
            Some((u1, u2))
        }
        Err(e) => {
            warn!(
                "[control] gc_checkpoint for {} failed: {e}",
                fork_dir.display()
            );
            None
        }
    }
}

/// Send `op` to the socket and return the value from an `ok <value>` response.
async fn call(socket: &Path, op: &str) -> std::io::Result<String> {
    let mut stream = UnixStream::connect(socket).await?;
    stream.write_all(format!("{op}\n").as_bytes()).await?;
    stream.flush().await?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let line = line.trim_end_matches('\n');
    if let Some(value) = line.strip_prefix("ok ") {
        Ok(value.to_owned())
    } else if let Some(msg) = line.strip_prefix("err ") {
        Err(std::io::Error::other(msg.to_owned()))
    } else {
        Err(std::io::Error::other(format!(
            "unexpected response: {line:?}"
        )))
    }
}
