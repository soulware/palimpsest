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
    /// Rewrite every pending segment with any hash-dead body bytes;
    /// bin-pack small segments into denser outputs at the same time.
    Repack,
    /// Rewrite post-snapshot pending segments as zstd-dictionary deltas.
    DeltaRepack,
    /// Flush the WAL and return `max_buckets` pre-minted GC output
    /// ULIDs. The coordinator emits up to one plan per ULID this tick.
    GcCheckpoint {
        #[serde(default = "default_max_buckets")]
        max_buckets: u32,
    },
    /// Apply staged GC handoffs into the in-memory extent index.
    ApplyGcHandoffs,
    /// Sign and write `snapshots/<snap_ulid>.manifest` plus the marker.
    SnapshotManifest {
        snap_ulid: Ulid,
        /// When `true`, write to `snapshots/<snap_ulid>.auto.manifest`
        /// instead — the ephemeral checkpoint variant used by
        /// `volume stop`. The signed payload is identical; only the
        /// filename differs. Defaults to `false` so existing callers
        /// keep the user-snapshot semantics.
        #[serde(default)]
        auto: bool,
    },
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
    /// Report whether a block-device client is currently connected.
    Connected,
    /// Flush the WAL and exit cleanly. The supervisor decides whether to
    /// restart based on `volume.stopped` markers.
    Shutdown,
}

/// Reply for [`VolumeRequest::Repack`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CompactionReply {
    pub stats: CompactionStats,
}

/// Reply for [`VolumeRequest::DeltaRepack`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DeltaRepackReply {
    pub stats: DeltaRepackStats,
}

/// Reply for [`VolumeRequest::GcCheckpoint`]. Carries one ULID per
/// pre-minted output bucket; length matches the request's `max_buckets`.
/// The coordinator uses up to that many for emitted plans this tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcCheckpointReply {
    pub bucket_ulids: Vec<Ulid>,
}

fn default_max_buckets() -> u32 {
    1
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
