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
//   gc_checkpoint              →  "ok <gc_ulid>"
//   apply_gc_handoffs          →  "ok <n>"
//   promote <ulid>             →  "ok"
//
// If the socket is absent (volume not running), all functions return None and
// log a warning.  Callers decide whether to abort or proceed without the op.

use std::path::Path;

use elide_core::volume::{CompactionStats, DeltaRepackStats};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::warn;
use ulid::Ulid;

/// Fsync the volume WAL.  Durability barrier only — does not promote.
/// Returns `true` on success.
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

/// Promote the volume WAL to a `pending/` segment.  Blocks until the
/// segment is on disk.  Returns `true` on success.
/// Returns `false` and logs a warning if the socket is absent or the call fails.
pub async fn promote_wal(fork_dir: &Path) -> bool {
    match call(fork_dir, "promote_wal").await {
        Some(resp) if resp.trim() == "ok" => true,
        Some(resp) => {
            warn!(
                "[control] promote_wal for {} returned unexpected response: {resp:?}",
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

/// Rewrite post-snapshot pending segments with zstd-dictionary deltas
/// against same-LBA extents from the latest sealed snapshot. Phase 5 Tier 1.
/// Returns delta-repack stats on success.
/// Returns `None` and logs a warning if the socket is absent or the call fails.
pub async fn delta_repack_post_snapshot(fork_dir: &Path) -> Option<DeltaRepackStats> {
    let response = call(fork_dir, "delta_repack").await?;
    let rest = match response.strip_prefix("ok ") {
        Some(r) => r.trim(),
        None => {
            warn!(
                "[control] unexpected delta_repack response for {}: {response:?}",
                fork_dir.display()
            );
            return None;
        }
    };
    let mut parts = rest.splitn(5, ' ');
    let segments_scanned: usize = parts.next()?.parse().ok()?;
    let segments_rewritten: usize = parts.next()?.parse().ok()?;
    let entries_converted: usize = parts.next()?.parse().ok()?;
    let original_body_bytes: u64 = parts.next()?.parse().ok()?;
    let delta_body_bytes: u64 = parts.next()?.trim().parse().ok()?;
    Some(DeltaRepackStats {
        segments_scanned,
        segments_rewritten,
        entries_converted,
        original_body_bytes,
        delta_body_bytes,
    })
}

/// Call gc_checkpoint on the volume process for `fork_dir`.
///
/// Returns `Some(gc_ulid)` on success.
/// Returns `None` and logs a warning if the socket is absent (volume not
/// running) or if the call fails for any reason.
pub async fn gc_checkpoint(fork_dir: &Path) -> Option<Ulid> {
    let response = call(fork_dir, "gc_checkpoint").await?;
    let rest = response.strip_prefix("ok ")?;
    match Ulid::from_string(rest.trim()) {
        Ok(u) => Some(u),
        Err(_) => {
            warn!(
                "[control] gc_checkpoint for {} returned malformed response",
                fork_dir.display()
            );
            None
        }
    }
}

/// Apply staged GC handoffs on the volume process for `fork_dir`.
///
/// Called by the coordinator before `apply_done_handoffs` to ensure the
/// volume's in-memory extent index reflects all committed GC decisions. This
/// is critical for restart safety: after a restart the volume rebuilds its
/// extent index from on-disk `.idx` files (which still point to old segments),
/// so any `.staged` handoffs from a previous session must be re-applied before
/// old segments are deleted.
///
/// Returns the number of handoffs processed.  Returns 0 if the socket is
/// absent or the call fails (non-fatal: the next idle tick will retry).
pub async fn apply_gc_handoffs(fork_dir: &Path) -> usize {
    match call(fork_dir, "apply_gc_handoffs").await {
        Some(resp) => resp
            .strip_prefix("ok ")
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0),
        None => 0,
    }
}

/// Redact a pending segment: hole-punch hash-dead DATA entries in place so
/// deleted data never leaves the host via S3 upload. Called before
/// `upload_segment`.
///
/// Returns `true` on success. Returns `false` if the socket is absent or fails.
pub async fn redact_segment(fork_dir: &Path, ulid: ulid::Ulid) -> bool {
    let req = format!("redact {ulid}");
    match call(fork_dir, &req).await {
        Some(resp) if resp.trim() == "ok" => true,
        Some(resp) => {
            warn!(
                "[control] redact {ulid} for {} returned unexpected response: {resp:?}",
                fork_dir.display()
            );
            false
        }
        None => false,
    }
}

/// Sign and write a snapshot manifest (`snapshots/<snap_ulid>.manifest`)
/// plus the `snapshots/<snap_ulid>` marker via the volume's control socket.
///
/// Called by the coordinator snapshot handler after a synchronous drain has
/// moved every in-flight segment from `pending/` into `index/`. The volume
/// enumerates its own `index/` at handler time and writes a full, signed
/// manifest listing every segment ULID that belongs to the snapshot.
///
/// Returns `true` on success. Returns `false` with a warning if the socket
/// is absent or the call fails.
pub async fn sign_snapshot_manifest(fork_dir: &Path, snap_ulid: Ulid) -> bool {
    let req = format!("snapshot_manifest {snap_ulid}");
    match call(fork_dir, &req).await {
        Some(resp) if resp.trim() == "ok" => true,
        Some(resp) => {
            warn!(
                "[control] snapshot_manifest {snap_ulid} for {} returned unexpected response: {resp:?}",
                fork_dir.display()
            );
            false
        }
        None => false,
    }
}

/// Promote a segment to the volume's local cache after confirmed S3 upload.
///
/// Sends `promote <ulid>` to the volume's control socket.  The volume copies
/// the segment body to `cache/<ulid>.body`, writes `cache/<ulid>.present`, and
/// (on the drain path) deletes `pending/<ulid>`.
///
/// Returns `true` on success.  Returns `false` if the socket is absent (volume
/// not running) or the call fails.  The coordinator retries on the next tick.
pub async fn promote_segment(fork_dir: &Path, ulid: ulid::Ulid) -> bool {
    let req = format!("promote {ulid}");
    match call(fork_dir, &req).await {
        Some(resp) if resp.trim() == "ok" => true,
        Some(resp) => {
            warn!(
                "[control] promote {ulid} for {} returned unexpected response: {resp:?}",
                fork_dir.display()
            );
            false
        }
        None => false,
    }
}

/// Stats from a single `reclaim` IPC call to the volume's control socket.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReclaimIpcStats {
    /// Number of candidates the scanner identified (upper bound on reclaim attempts).
    pub candidates_scanned: usize,
    /// Total contiguous runs rewritten across all attempted candidates.
    pub runs_rewritten: u64,
    /// Total bytes committed to fresh compact entries.
    pub bytes_rewritten: u64,
    /// Number of candidates whose phase-3 commit discarded (unrelated
    /// concurrent mutation). Discarded candidates are not retried here —
    /// the next tick / call will re-observe them if still bloated.
    pub discarded: usize,
}

/// Run an alias-merge extent reclamation pass on the volume.
///
/// The volume-side handler scans for bloated-hash candidates using
/// default thresholds (most-wasteful first) and rewrites them via the
/// three-phase primitive. Heavy work (fetch / hash / compress) runs off
/// the actor thread; NBD writes are not blocked for the pass duration.
///
/// `cap` bounds how many candidates the handler will process in this
/// call:
/// * `None` — unlimited (CLI / ad-hoc full-sweep).
/// * `Some(n)` — process at most `n`; any remaining candidates are
///   picked up on the next call. Tick-loop callers pass a small cap
///   (typically `1`) to bound per-tick latency.
///
/// Returns `Some(stats)` on success. Returns `None` if the socket is
/// absent (volume not running) or the call fails — callers decide
/// whether to surface that as an error or a "nothing to reclaim".
pub async fn reclaim(fork_dir: &Path, cap: Option<u32>) -> Option<ReclaimIpcStats> {
    let req = match cap {
        Some(n) => format!("reclaim {n}"),
        None => "reclaim".to_string(),
    };
    let response = call(fork_dir, &req).await?;
    let rest = match response.strip_prefix("ok ") {
        Some(r) => r.trim(),
        None => {
            warn!(
                "[control] unexpected reclaim response for {}: {response:?}",
                fork_dir.display()
            );
            return None;
        }
    };
    let mut parts = rest.splitn(4, ' ');
    let candidates_scanned: usize = parts.next()?.parse().ok()?;
    let runs_rewritten: u64 = parts.next()?.parse().ok()?;
    let bytes_rewritten: u64 = parts.next()?.parse().ok()?;
    let discarded: usize = parts.next()?.trim().parse().ok()?;
    Some(ReclaimIpcStats {
        candidates_scanned,
        runs_rewritten,
        bytes_rewritten,
        discarded,
    })
}

/// Finalize a completed GC handoff by asking the volume to delete the
/// bare `gc/<ulid>` file (the volume-applied, coordinator-upload-pending
/// marker under the self-describing handoff protocol).
///
/// Called by `apply_done_handoffs` after the new segment has been promoted
/// and the old S3 objects have been deleted. Routing the delete through the
/// volume actor keeps every `gc/` mutation serialised with the idle-tick
/// `apply_gc_handoffs` path, so there is no race between the coordinator
/// removing a file and the actor trying to read it.
///
/// Returns `true` on success.  Returns `false` if the socket is absent or
/// the call fails; the coordinator retries on the next tick.
pub async fn finalize_gc_handoff(fork_dir: &Path, ulid: ulid::Ulid) -> bool {
    let req = format!("finalize_gc_handoff {ulid}");
    match call(fork_dir, &req).await {
        Some(resp) if resp.trim() == "ok" => true,
        Some(resp) => {
            warn!(
                "[control] finalize_gc_handoff {ulid} for {} returned unexpected response: {resp:?}",
                fork_dir.display()
            );
            false
        }
        None => false,
    }
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
            // ECONNREFUSED / ENOENT are expected while the volume process is
            // starting up or has not yet been spawned; log at debug only.
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) {
                tracing::debug!("[control] {op} for {} not ready: {e}", fork_dir.display());
            } else {
                warn!("[control] {op} for {} failed: {e}", fork_dir.display());
            }
            None
        }
    }
}
