// Coordinator-side client for the volume control socket.
//
// The volume process listens on `<fork_dir>/control.sock`. Each operation
// is a single-connection round trip: send one typed JSON request line,
// receive one typed JSON reply line, close.
//
// Wire types live in `elide_core::ipc` (envelope + error) and
// `elide_core::volume_ipc` (request + reply payloads).
//
// If the socket is absent (volume not running) most public functions
// return `None` / `false` and log a warning. Callers decide whether to
// abort or proceed without the op.

use std::path::Path;

use elide_core::ipc::{Envelope, IpcError};
use elide_core::volume::{CompactionStats, DeltaRepackStats};
use elide_core::volume_ipc::{
    ApplyGcHandoffsReply, CompactionReply, ConnectedReply, DeltaRepackReply, GcCheckpointReply,
    ReclaimReply, VolumeRequest,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::warn;
use ulid::Ulid;

/// Fsync the volume WAL.  Durability barrier only — does not promote.
/// Returns `true` on success.
/// Returns `false` and logs a warning if the socket is absent or the call fails.
pub async fn flush(fork_dir: &Path) -> bool {
    call_unit(fork_dir, &VolumeRequest::Flush).await
}

/// Promote the volume WAL to a `pending/` segment.  Blocks until the
/// segment is on disk.  Returns `true` on success.
/// Returns `false` and logs a warning if the socket is absent or the call fails.
pub async fn promote_wal(fork_dir: &Path) -> bool {
    call_unit(fork_dir, &VolumeRequest::PromoteWal).await
}

/// Sweep small pending segments.  Returns compaction stats on success.
/// Returns `None` and logs a warning if the socket is absent or the call fails.
pub async fn sweep_pending(fork_dir: &Path) -> Option<CompactionStats> {
    let reply: CompactionReply = call_typed(fork_dir, &VolumeRequest::SweepPending).await?;
    Some(reply.stats)
}

/// Repack sparse pending segments below `min_live_ratio`.
/// Returns compaction stats on success.
/// Returns `None` and logs a warning if the socket is absent or the call fails.
pub async fn repack(fork_dir: &Path, min_live_ratio: f64) -> Option<CompactionStats> {
    let reply: CompactionReply =
        call_typed(fork_dir, &VolumeRequest::Repack { min_live_ratio }).await?;
    Some(reply.stats)
}

/// Rewrite post-snapshot pending segments with zstd-dictionary deltas
/// against same-LBA extents from the latest sealed snapshot.
/// Returns delta-repack stats on success.
/// Returns `None` and logs a warning if the socket is absent or the call fails.
pub async fn delta_repack_post_snapshot(fork_dir: &Path) -> Option<DeltaRepackStats> {
    let reply: DeltaRepackReply = call_typed(fork_dir, &VolumeRequest::DeltaRepack).await?;
    Some(reply.stats)
}

/// Call gc_checkpoint on the volume process for `fork_dir`.
///
/// Returns `Some(gc_ulid)` on success.
/// Returns `None` and logs a warning if the socket is absent (volume not
/// running) or if the call fails for any reason.
pub async fn gc_checkpoint(fork_dir: &Path) -> Option<Ulid> {
    let reply: GcCheckpointReply = call_typed(fork_dir, &VolumeRequest::GcCheckpoint).await?;
    Some(reply.gc_ulid)
}

/// Apply staged GC handoffs on the volume process for `fork_dir`.
///
/// Returns the number of handoffs processed.  Returns 0 if the socket is
/// absent or the call fails (non-fatal: the next idle tick will retry).
pub async fn apply_gc_handoffs(fork_dir: &Path) -> usize {
    let reply: Option<ApplyGcHandoffsReply> =
        call_typed(fork_dir, &VolumeRequest::ApplyGcHandoffs).await;
    reply.map(|r| r.processed as usize).unwrap_or(0)
}

/// Redact a pending segment: hole-punch hash-dead DATA entries in place
/// before S3 upload. Returns `true` on success.
pub async fn redact_segment(fork_dir: &Path, segment_ulid: Ulid) -> bool {
    call_unit(fork_dir, &VolumeRequest::Redact { segment_ulid }).await
}

/// Sign and write a snapshot manifest plus the snapshot marker.
/// Returns `true` on success.
pub async fn sign_snapshot_manifest(fork_dir: &Path, snap_ulid: Ulid) -> bool {
    call_unit(fork_dir, &VolumeRequest::SnapshotManifest { snap_ulid }).await
}

/// Promote a segment to the volume's local cache after confirmed S3 upload.
/// Returns `true` on success.
pub async fn promote_segment(fork_dir: &Path, segment_ulid: Ulid) -> bool {
    call_unit(fork_dir, &VolumeRequest::Promote { segment_ulid }).await
}

/// Stats from a single `reclaim` IPC call. Mirrors
/// [`elide_core::volume_ipc::ReclaimReply`] with the historical
/// coordinator-internal `usize` fields preserved for callers.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReclaimIpcStats {
    /// Number of candidates the scanner identified (upper bound on reclaim attempts).
    pub candidates_scanned: usize,
    /// Total contiguous runs rewritten across all attempted candidates.
    pub runs_rewritten: u64,
    /// Total bytes committed to fresh compact entries.
    pub bytes_rewritten: u64,
    /// Number of candidates whose phase-3 commit discarded.
    pub discarded: usize,
}

impl From<ReclaimReply> for ReclaimIpcStats {
    fn from(r: ReclaimReply) -> Self {
        Self {
            candidates_scanned: r.candidates_scanned as usize,
            runs_rewritten: r.runs_rewritten,
            bytes_rewritten: r.bytes_rewritten,
            discarded: r.discarded as usize,
        }
    }
}

/// Run an alias-merge extent reclamation pass on the volume.
///
/// `cap` bounds how many candidates the handler will process this call.
pub async fn reclaim(fork_dir: &Path, cap: Option<u32>) -> Option<ReclaimIpcStats> {
    let reply: ReclaimReply = call_typed(fork_dir, &VolumeRequest::Reclaim { cap }).await?;
    Some(reply.into())
}

/// Finalize a completed GC handoff by asking the volume to delete the
/// bare `gc/<gc_ulid>` file. Returns `true` on success.
pub async fn finalize_gc_handoff(fork_dir: &Path, gc_ulid: Ulid) -> bool {
    call_unit(fork_dir, &VolumeRequest::FinalizeGcHandoff { gc_ulid }).await
}

/// Result of an `is_connected` query against the volume control socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectedStatus {
    /// Socket reachable; an NBD client is currently connected.
    Connected,
    /// Socket reachable; no NBD client is connected.
    Disconnected,
    /// Socket unreachable (volume not running, or transient I/O failure).
    Unavailable,
}

/// Query whether an NBD client is currently connected to the volume.
pub async fn is_connected(fork_dir: &Path) -> ConnectedStatus {
    match call_typed::<ConnectedReply>(fork_dir, &VolumeRequest::Connected).await {
        Some(r) if r.connected => ConnectedStatus::Connected,
        Some(_) => ConnectedStatus::Disconnected,
        None => ConnectedStatus::Unavailable,
    }
}

/// Outcome of a `shutdown` call against the volume control socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownOutcome {
    /// Volume accepted the shutdown and (the process now exiting).
    Acknowledged,
    /// Socket was unreachable — process likely already gone.
    NotRunning,
    /// The volume reported an error; the message comes from the volume.
    Failed(String),
}

/// Ask the volume process to flush its WAL and exit cleanly.
///
/// On success the volume's reply lands first, then the process calls
/// `exit(0)`. On the wire we treat both "Ok envelope" and "EOF before
/// envelope" as `Acknowledged`, since the volume may close the socket
/// before flushing the JSON line in some kernel-buffer interleavings.
pub async fn shutdown(fork_dir: &Path) -> ShutdownOutcome {
    let socket = fork_dir.join("control.sock");
    if !socket.exists() {
        return ShutdownOutcome::NotRunning;
    }
    let mut stream = match UnixStream::connect(&socket).await {
        Ok(s) => s,
        Err(e) => {
            return classify_socket_err(&socket, "shutdown", e);
        }
    };
    let req = serde_json::to_vec(&VolumeRequest::Shutdown).unwrap_or_default();
    let mut buf = req;
    buf.push(b'\n');
    if let Err(e) = stream.write_all(&buf).await {
        return ShutdownOutcome::Failed(format!("write: {e}"));
    }
    if let Err(e) = stream.flush().await {
        return ShutdownOutcome::Failed(format!("flush: {e}"));
    }
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    match reader.read_line(&mut line).await {
        Ok(0) => ShutdownOutcome::Acknowledged,
        Ok(_) => match parse_envelope::<()>(&line) {
            Ok(_) => ShutdownOutcome::Acknowledged,
            Err(e) => ShutdownOutcome::Failed(e.message),
        },
        Err(e) => ShutdownOutcome::Failed(format!("read: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Send a request expected to produce a unit (`()`) reply.
async fn call_unit(fork_dir: &Path, request: &VolumeRequest) -> bool {
    matches!(call_typed::<()>(fork_dir, request).await, Some(()))
}

/// Send a request and parse the reply as `T`. Returns `None` if the
/// socket is absent, the call fails at transport level, or the volume
/// returned an error envelope.
async fn call_typed<T>(fork_dir: &Path, request: &VolumeRequest) -> Option<T>
where
    T: for<'de> Deserialize<'de>,
{
    let socket = fork_dir.join("control.sock");
    if !socket.exists() {
        return None;
    }
    let result: std::io::Result<T> = async {
        let mut stream = UnixStream::connect(&socket).await?;
        write_request(&mut stream, request).await?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        match parse_envelope::<T>(&line) {
            Ok(v) => Ok(v),
            Err(e) => Err(std::io::Error::other(e.to_string())),
        }
    }
    .await;
    match result {
        Ok(v) => Some(v),
        Err(e) => {
            let verb = verb_label(request);
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) {
                tracing::debug!("[control] {verb} for {} not ready: {e}", fork_dir.display());
            } else {
                warn!("[control] {verb} for {} failed: {e}", fork_dir.display());
            }
            None
        }
    }
}

/// Render the verb tag for a [`VolumeRequest`] for tracing.
fn verb_label(request: &VolumeRequest) -> &'static str {
    match request {
        VolumeRequest::Flush => "flush",
        VolumeRequest::PromoteWal => "promote-wal",
        VolumeRequest::SweepPending => "sweep-pending",
        VolumeRequest::Repack { .. } => "repack",
        VolumeRequest::DeltaRepack => "delta-repack",
        VolumeRequest::GcCheckpoint => "gc-checkpoint",
        VolumeRequest::ApplyGcHandoffs => "apply-gc-handoffs",
        VolumeRequest::Redact { .. } => "redact",
        VolumeRequest::SnapshotManifest { .. } => "snapshot-manifest",
        VolumeRequest::Promote { .. } => "promote",
        VolumeRequest::FinalizeGcHandoff { .. } => "finalize-gc-handoff",
        VolumeRequest::Reclaim { .. } => "reclaim",
        VolumeRequest::Connected => "connected",
        VolumeRequest::Shutdown => "shutdown",
    }
}

async fn write_request<W, T>(writer: &mut W, request: &T) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let mut buf = serde_json::to_vec(request)?;
    buf.push(b'\n');
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

fn parse_envelope<T>(line: &str) -> Result<T, IpcError>
where
    T: for<'de> Deserialize<'de>,
{
    let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
    let envelope: Envelope<T> = serde_json::from_str(trimmed)
        .map_err(|e| IpcError::internal(format!("malformed reply: {e}")))?;
    envelope.into_result()
}

fn classify_socket_err(socket: &Path, verb: &str, e: std::io::Error) -> ShutdownOutcome {
    if matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
    ) {
        tracing::debug!("[control] {verb} for {} not ready: {e}", socket.display());
        ShutdownOutcome::NotRunning
    } else {
        warn!("[control] {verb} for {} failed: {e}", socket.display());
        ShutdownOutcome::Failed(e.to_string())
    }
}
