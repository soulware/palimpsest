//! Typed IPC protocol for the volume's control socket
//! (`<fork_dir>/control.sock`).
//!
//! Each connection is a single request → single (or, in future,
//! streamed) reply round trip. Wire format is NDJSON, framed by the
//! shared [`crate::ipc::Envelope`] for replies.
//!
//! The volume server lives in the root `elide` binary (sync std::io
//! handler thread); the coordinator's client lives in
//! `elide-coordinator/src/control.rs` (async tokio). Both serialise
//! through this module so the wire shape stays consistent.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::volume::{CompactionStats, DeltaRepackStats};

/// Typed volume control IPC request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "verb", rename_all = "kebab-case")]
pub enum VolumeRequest {
    /// Fsync the WAL. Durability barrier only — does not promote.
    Flush,
    /// Promote the WAL to a `pending/` segment, blocking until it's on disk.
    PromoteWal,
    /// Compact small pending segments using default thresholds.
    SweepPending,
    /// Compact sparse pending segments below the given live-extent ratio.
    Repack { min_live_ratio: f64 },
    /// Rewrite post-snapshot pending segments as zstd-dictionary deltas.
    DeltaRepack,
    /// Flush the WAL and return a fresh GC output ULID.
    GcCheckpoint,
    /// Apply staged GC handoffs into the in-memory extent index.
    ApplyGcHandoffs,
    /// Hole-punch hash-dead DATA entries in `pending/<segment_ulid>`
    /// before S3 upload.
    Redact { segment_ulid: Ulid },
    /// Sign and write `snapshots/<snap_ulid>.manifest` plus the marker.
    SnapshotManifest { snap_ulid: Ulid },
    /// Promote a confirmed-uploaded segment into `cache/`.
    Promote { segment_ulid: Ulid },
    /// Delete the bare `gc/<gc_ulid>` marker after a finalised handoff.
    FinalizeGcHandoff { gc_ulid: Ulid },
    /// Run an alias-merge extent reclamation pass; with `cap = Some(n)`,
    /// process at most `n` candidates this call.
    Reclaim {
        #[serde(skip_serializing_if = "Option::is_none", default)]
        cap: Option<u32>,
    },
    /// Report whether an NBD client is currently connected.
    Connected,
    /// Flush the WAL and exit cleanly. The supervisor decides whether to
    /// restart based on `volume.stopped` markers.
    Shutdown,
}

/// Reply for [`VolumeRequest::SweepPending`] / [`VolumeRequest::Repack`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CompactionReply {
    pub stats: CompactionStats,
}

/// Reply for [`VolumeRequest::DeltaRepack`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DeltaRepackReply {
    pub stats: DeltaRepackStats,
}

/// Reply for [`VolumeRequest::GcCheckpoint`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GcCheckpointReply {
    pub gc_ulid: Ulid,
}

/// Reply for [`VolumeRequest::ApplyGcHandoffs`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ApplyGcHandoffsReply {
    pub processed: u32,
}

/// Reply for [`VolumeRequest::Reclaim`]. Mirrors the historical
/// `ReclaimIpcStats` shape that the coordinator already exposes.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ReclaimReply {
    /// Number of candidates the scanner identified (upper bound on
    /// reclaim attempts this call).
    pub candidates_scanned: u32,
    /// Total contiguous runs rewritten across all attempted candidates.
    pub runs_rewritten: u64,
    /// Total bytes committed to fresh compact entries.
    pub bytes_rewritten: u64,
    /// Number of candidates whose phase-3 commit discarded (unrelated
    /// concurrent mutation). Discarded candidates are not retried here —
    /// the next tick / call will re-observe them if still bloated.
    pub discarded: u32,
}

/// Reply for [`VolumeRequest::Connected`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ConnectedReply {
    pub connected: bool,
}
