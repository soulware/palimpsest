// Coordinator-side client for the volume control socket.
//
// The volume process listens on <fork_dir>/control.sock.  Each operation is a
// single-connection round trip: send one line, receive one line, close.
//
// Protocol:
//   Request:  "<op> [args...]\n"
//   Success:  "ok [values...]\n"
//   Error:    "err <message>\n"
//
// Supported operations:
//   flush                      →  "ok"
//   sweep_pending              →  "ok <segs> <bytes> <extents>"
//   repack <min_live_ratio>    →  "ok <segs> <bytes> <extents>"
//   gc_checkpoint              →  "ok <repack_ulid> <sweep_ulid>"
//
// If the socket is absent (volume not running), all functions return None and
// log a warning.  Callers decide whether to abort or proceed without the op.

use std::path::Path;

use elide_core::volume::CompactionStats;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::warn;

/// Flush the volume WAL.  Returns `true` on success.
/// Returns `false` and logs a warning if the socket is absent or the call fails.
pub async fn flush(fork_dir: &Path) -> bool {
    match call(fork_dir, "flush").await {
        Some(resp) if resp.trim() == "ok" => true,
        Some(resp) => {
            warn!(
                "[control] flush for {} returned unexpected response: {resp:?}",
                fork_dir.display()
            );
            false
        }
        None => false,
    }
}

/// Sweep small pending segments.  Returns compaction stats on success.
/// Returns `None` and logs a warning if the socket is absent or the call fails.
pub async fn sweep_pending(fork_dir: &Path) -> Option<CompactionStats> {
    parse_compaction_stats(fork_dir, call(fork_dir, "sweep_pending").await)
}

/// Repack sparse pending segments below `min_live_ratio`.
/// Returns compaction stats on success.
/// Returns `None` and logs a warning if the socket is absent or the call fails.
pub async fn repack(fork_dir: &Path, min_live_ratio: f64) -> Option<CompactionStats> {
    let req = format!("repack {min_live_ratio}");
    parse_compaction_stats(fork_dir, call(fork_dir, &req).await)
}

/// Call gc_checkpoint on the volume process for `fork_dir`.
///
/// Returns `Some((repack_ulid, sweep_ulid))` on success.
/// Returns `None` and logs a warning if the socket is absent (volume not
/// running) or if the call fails for any reason.
pub async fn gc_checkpoint(fork_dir: &Path) -> Option<(String, String)> {
    let response = call(fork_dir, "gc_checkpoint").await?;
    let rest = response.strip_prefix("ok ")?;
    let mut parts = rest.splitn(2, ' ');
    let u1 = parts.next()?.trim().to_owned();
    let u2 = parts.next()?.trim().to_owned();
    if u1.is_empty() || u2.is_empty() {
        warn!(
            "[control] gc_checkpoint for {} returned malformed response",
            fork_dir.display()
        );
        return None;
    }
    Some((u1, u2))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Parse an "ok <segs> <bytes> <extents>" response into `CompactionStats`.
fn parse_compaction_stats(fork_dir: &Path, response: Option<String>) -> Option<CompactionStats> {
    let response = response?;
    let rest = match response.strip_prefix("ok ") {
        Some(r) => r.trim(),
        None => {
            warn!(
                "[control] unexpected compaction response for {}: {response:?}",
                fork_dir.display()
            );
            return None;
        }
    };
    let mut parts = rest.splitn(4, ' ');
    let segs: usize = parts.next()?.parse().ok()?;
    let new_segs: usize = parts.next()?.parse().ok()?;
    let bytes: u64 = parts.next()?.parse().ok()?;
    let extents: usize = parts.next()?.trim().parse().ok()?;
    Some(CompactionStats {
        segments_compacted: segs,
        new_segments: new_segs,
        bytes_freed: bytes,
        extents_removed: extents,
    })
}

/// Send `op` to the control socket.  Returns the full response line (including
/// the "ok"/"err" prefix) on success, or `None` if the socket is absent or
/// any I/O error occurs.
async fn call(fork_dir: &Path, op: &str) -> Option<String> {
    let socket = fork_dir.join("control.sock");
    if !socket.exists() {
        return None;
    }
    let result: std::io::Result<String> = async {
        let mut stream = UnixStream::connect(&socket).await?;
        stream.write_all(format!("{op}\n").as_bytes()).await?;
        stream.flush().await?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let line = line.trim_end_matches('\n').to_owned();
        if let Some(msg) = line.strip_prefix("err ") {
            Err(std::io::Error::other(msg.to_owned()))
        } else {
            Ok(line)
        }
    }
    .await;
    match result {
        Ok(line) => Some(line),
        Err(e) => {
            warn!("[control] {op} for {} failed: {e}", fork_dir.display());
            None
        }
    }
}
