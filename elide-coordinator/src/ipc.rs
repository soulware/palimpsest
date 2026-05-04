//! JSON-over-Unix-socket IPC framing for the coordinator's `control.sock`.
//!
//! # Wire format
//!
//! Each message is a single JSON object on its own line (NDJSON). The
//! connection lifecycle is:
//!
//!   1. Client sends one request line.
//!   2. Server sends one or more reply lines.
//!   3. One side closes the socket.
//!
//! Unary verbs send exactly one reply line. Streaming verbs (currently
//! only `import attach`) send a sequence of reply lines until the
//! operation completes, then close.
//!
//! # Envelope
//!
//! Every reply is wrapped in [`Envelope`] so the outcome (`ok` vs `err`)
//! is the same shape across all verbs:
//!
//! ```json
//! {"outcome":"ok","data":<verb-specific>}
//! {"outcome":"err","error":{"kind":"not-found","message":"..."}}
//! ```
//!
//! The verb-specific reply type is opaque at the envelope layer; the
//! caller knows what verb it sent and deserialises `data` accordingly.
//!
//! # Errors
//!
//! [`IpcError`] uses a small set of coarse [`IpcErrorKind`] tags plus a
//! free-text `message`. The kind is what callers branch on; the message
//! is what gets shown to the operator. New failure modes pick the
//! closest existing kind rather than adding a new variant.

use std::io;

use elide_core::name_record::NameState;
use elide_core::volume_event::VolumeEvent;
use elide_peer_fetch::PeerEndpoint;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use ulid::Ulid;

use crate::eligibility::Eligibility;
use crate::volume_state::VolumeLifecycle;

// Envelope + error types are shared with the volume control protocol;
// re-exported so existing imports of `elide_coordinator::ipc::Envelope`
// (etc.) continue to resolve.
pub use elide_core::ipc::{Envelope, IpcError, IpcErrorKind};

/// Write a single JSON message followed by a newline. Used for both
/// request and reply lines.
pub async fn write_message<W, T>(writer: &mut W, message: &T) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let mut buf = serde_json::to_vec(message)?;
    buf.push(b'\n');
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a single JSON message terminated by `\n`. Returns `Ok(None)` on
/// clean EOF (peer closed without sending another line).
pub async fn read_message<R, T>(reader: &mut BufReader<R>) -> io::Result<Option<T>>
where
    R: tokio::io::AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
    let value = serde_json::from_str(trimmed).map_err(io::Error::other)?;
    Ok(Some(value))
}

// ── Verb request envelope ─────────────────────────────────────────────
//
// Migrated verbs are listed here. Unmigrated verbs continue to use the
// line-based dispatcher in `inbound::dispatch_legacy`. As verbs migrate
// they move from there into this enum.
//
// Wire shape (NDJSON, one object per line):
//   {"verb":"rescan"}
//   {"verb":"status","volume":"foo"}
//   {"verb":"status-remote","volume":"foo"}

/// Typed coordinator IPC request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "verb", rename_all = "kebab-case")]
pub enum Request {
    // ── Iteration 1: status family ───────────────────────────────────
    /// Trigger an immediate fork-discovery pass. No payload.
    Rescan,
    /// Report a volume's local lifecycle (running / stopped / importing).
    Status { volume: String },
    /// Fetch the bucket-side `names/<volume>` record plus this
    /// coordinator's eligibility to act on it.
    StatusRemote { volume: String },

    // ── Iteration 2: lifecycle ───────────────────────────────────────
    /// Halt the local volume daemon. Empty success reply.
    Stop { volume: String },
    /// Drain WAL → publish handoff snapshot → flip `names/<name>`
    /// to Released. With `force = true`, override an unreachable
    /// foreign owner (synthesises a handoff from S3-visible segments).
    Release {
        volume: String,
        #[serde(default)]
        force: bool,
    },
    /// Local resume of a `Stopped` volume. Empty success reply.
    Start { volume: String },

    // ── Iteration 3: creation + readonly ─────────────────────────────
    /// Mint a fresh writable volume. `flags` is the same transport-flag
    /// list the CLI accepts on the command line (`nbd-port=N`,
    /// `nbd-bind=…`, `ublk`, `ublk-id=N`, `nbd-socket=…`).
    Create {
        volume: String,
        size_bytes: u64,
        #[serde(default)]
        flags: Vec<String>,
    },
    /// Patch transport configuration on an existing volume and restart
    /// the daemon if it's running.
    Update {
        volume: String,
        #[serde(default)]
        flags: Vec<String>,
    },
    /// Spawn an OCI → volume import in the background. Returns the
    /// import ULID immediately; the actual import runs detached and is
    /// observed via `ImportAttach` / `ImportStatus`.
    ImportStart {
        volume: String,
        oci_ref: String,
        #[serde(default)]
        extents_from: Vec<String>,
    },
    /// Poll the state of an in-flight import by volume name.
    ImportStatus { volume: String },
    /// Stream an in-flight import's output. Server replies with a
    /// sequence of [`Envelope<ImportAttachEvent>`] messages: one per
    /// output line, then a terminal `Done`. Failure terminates with an
    /// `Envelope::Err`.
    ImportAttach { volume: String },

    // ── Iteration 4: maintenance ─────────────────────────────────────
    /// Coordinator-orchestrated snapshot of a running volume: flush
    /// WAL → drain pending → sign manifest → upload. Returns the
    /// snapshot ULID.
    Snapshot { volume: String },
    /// Evict S3-confirmed segment bodies from a volume's `cache/`. With
    /// `segment_ulid = None`, evicts every confirmed body; with `Some`,
    /// evicts only that one segment.
    Evict {
        volume: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        segment_ulid: Option<Ulid>,
    },
    /// Generate `snapshots/<snap_ulid>.filemap` for a sealed snapshot.
    /// Defaults to the latest local snapshot when `snap_ulid` is omitted.
    GenerateFilemap {
        volume: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        snap_ulid: Option<Ulid>,
    },
    /// Block until the per-fork prefetch task publishes a terminal
    /// result. Untracked volumes (not under coordinator management or
    /// already prefetched) return success immediately.
    AwaitPrefetch { vol_ulid: Ulid },
    /// Remove the local instance of a volume: `by_name/<volume>`
    /// symlink + `by_id/<vol_ulid>/` directory. Bucket records are
    /// untouched. With `force = false`, refuses if `pending/` or
    /// `wal/` is non-empty.
    Remove {
        volume: String,
        #[serde(default)]
        force: bool,
    },
    // ── Read-only history ────────────────────────────────────────────
    /// List the per-name event log at `events/<volume>/`,
    /// verifying each entry's signature against the emitting
    /// coordinator's published pubkey.
    VolumeEvents { volume: String },

    // ── Creds + cleanup (final iteration) ────────────────────────────
    /// Vend the non-secret `[store]` config (bucket / endpoint /
    /// region or local_path). Used by spawned volume subprocesses
    /// (over the macaroon handshake) to build an object_store that
    /// matches the coordinator's view of the world.
    GetStoreConfig,
    /// Mint a per-volume macaroon for a spawned volume process.
    /// PID-bound via SO_PEERCRED on the connecting socket — the
    /// coordinator refuses if the peer's PID doesn't match the
    /// volume's recorded `volume.pid`.
    Register { volume_ulid: Ulid },
    /// Macaroon-authenticated short-lived credential issuance.
    /// Verifies the MAC, re-checks SO_PEERCRED matches the macaroon's
    /// `pid` caveat, then delegates to the configured
    /// `CredentialIssuer`.
    Credentials { macaroon: String },

    // ── Consolidated fork / claim flows ──────────────────────────────
    //
    // These verbs collapse the multi-call CLI orchestration of
    // `volume create --from` and `volume claim` into a coordinator-side
    // job, mirroring the `ImportStart` / `ImportAttach` shape:
    //
    //   * `*Start` registers the job and (for fork) returns
    //     immediately, or (for claim) returns synchronously when the
    //     in-place reclaim path applies.
    //   * `*Attach` streams progress events until terminal.
    /// Spawn a fork-from-source flow as a background job. The
    /// coordinator does the full orchestration internally: name
    /// resolution, ancestor-chain pull, snapshot decision (latest,
    /// implicit, or `force_snapshot`-attested), and the fork mint.
    /// Returns immediately on registration; progress is streamed via
    /// [`Request::ForkAttach`]. The new fork's ULID arrives in the
    /// stream as [`ForkAttachEvent::ForkCreated`].
    ForkStart {
        new_name: String,
        from: ForkSource,
        #[serde(default)]
        force_snapshot: bool,
        #[serde(default)]
        flags: Vec<String>,
    },
    /// Stream progress for an in-flight fork started via
    /// [`Request::ForkStart`]. Addressed by the new fork's name (the
    /// `new_name` from `ForkStart`) so the caller doesn't have to
    /// remember a ULID across reconnects, matching the `ImportAttach`
    /// shape. Server replies with a sequence of
    /// [`Envelope<ForkAttachEvent>`] messages, terminating with
    /// `ForkAttachEvent::Done` (success) or `Envelope::Err` (failure).
    ForkAttach { new_name: String },
    /// Spawn a name-claim flow as a background job. Subsumes the
    /// existing `Claim` verb's `MustClaimFresh` branch: the coordinator
    /// runs the bucket-side claim, pulls the ancestor chain, resolves
    /// the handoff key, mints the fresh fork, and triggers prefetch.
    /// In-place reclaims return `Reclaimed` immediately with no job
    /// spawned; foreign-content claims return
    /// `Claiming { released_vol_ulid }` and the caller observes via
    /// [`Request::ClaimAttach`].
    ClaimStart { volume: String },
    /// Stream progress for an in-flight claim started via
    /// [`Request::ClaimStart`]. Addressed by the bucket-side name so
    /// the caller doesn't have to remember the freshly-minted fork
    /// ULID across reconnects.
    ClaimAttach { volume: String },
}

/// Source spec for [`Request::ForkStart`]. Mirrors the CLI's
/// `FromSpec`: a name to resolve, a writable volume by ULID (which
/// implies an implicit snapshot before forking), or an explicit
/// `(vol_ulid, snap_ulid)` pin.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ForkSource {
    /// Source addressed by name. Coordinator tries `by_name/<name>`
    /// locally first, then `names/<name>` in the store.
    Name { name: String },
    /// Writable source addressed by ULID. Coordinator takes an
    /// implicit snapshot of the running daemon before forking.
    BareUlid { vol_ulid: Ulid },
    /// Pinned source: a specific `(vol_ulid, snap_ulid)` pair.
    /// Coordinator pulls the chain if missing and verifies the
    /// snapshot exists in the store.
    Pinned { vol_ulid: Ulid, snap_ulid: Ulid },
}

/// Reply for [`Request::Status`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatusReply {
    pub lifecycle: VolumeLifecycle,
}

/// Reply for [`Request::StatusRemote`]. Mirrors the fields of
/// `NameRecord` plus the derived `eligibility` classification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatusRemoteReply {
    pub state: NameState,
    pub vol_ulid: Ulid,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub coordinator_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub claimed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub handoff_snapshot: Option<Ulid>,
    pub eligibility: Eligibility,
}

/// Reply for [`Request::Release`] (with or without `force`). The
/// `handoff_snapshot` field carries the snapshot ULID that the next
/// claimant will fork from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseReply {
    pub handoff_snapshot: Ulid,
}

/// Internal return type for `claim_volume_bucket_op`, the synchronous
/// bucket-side step inside [`Request::ClaimStart`]. The two arms map
/// directly onto [`ClaimStartReply`]'s `Reclaimed` / `Claiming`
/// branches: `Reclaimed` short-circuits with no job spawned;
/// `MustClaimFresh` triggers the foreign-content orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ClaimReply {
    Reclaimed,
    MustClaimFresh {
        released_vol_ulid: Ulid,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        handoff_snapshot: Option<Ulid>,
    },
}

/// Reply for [`Request::Create`]. The new volume's ULID is what the
/// caller stamps into local state for any follow-up IPCs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateReply {
    pub vol_ulid: Ulid,
}

/// Internal return type for `fork_create_op`, the fork-mint step
/// inside the fork and claim orchestrators. The new fork's ULID is
/// surfaced to subscribers as
/// [`ForkAttachEvent::ForkCreated`] / [`ClaimAttachEvent::ForkCreated`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForkCreateReply {
    pub new_vol_ulid: Ulid,
}

/// Reply for [`Request::Update`]. `restarted = true` means the daemon
/// was running and the coordinator triggered a restart so it picks up
/// the new transport config; `false` means the volume was not running
/// and the new config takes effect on next start.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateReply {
    pub restarted: bool,
}

/// Internal return type for `pull_readonly_op`, the per-ancestor
/// chain-walk step inside the fork and claim orchestrators. `parent`
/// is `None` for a root ancestor; otherwise it carries the verified
/// parent ULID parsed from the just-fetched provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PullReadonlyReply {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent: Option<Ulid>,
}

/// Internal return type for `resolve_name_op`, called from the fork
/// orchestrator's name-source resolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolveNameReply {
    pub vol_ulid: Ulid,
}

/// Internal return type for `latest_snapshot_op`, used by the fork
/// orchestrator to pick a snapshot when the source is readonly and
/// no local snapshot is recorded. `snapshot_ulid` is `None` when the
/// volume has no snapshots in the store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatestSnapshotReply {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_ulid: Option<Ulid>,
}

/// Reply for [`Request::ImportStart`]. The import runs detached; the
/// CLI uses [`Request::ImportAttach`] to stream output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImportStartReply {
    pub import_ulid: Ulid,
}

/// Reply for [`Request::ImportStatus`]. Failures (no active import,
/// import failed) come back as `Envelope::Err`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum ImportStatusReply {
    /// Import is in progress.
    Running,
    /// Import completed successfully. The volume is readonly.
    Done,
}

/// Streaming event for [`Request::ImportAttach`]. The server writes
/// one [`Envelope<ImportAttachEvent>`] per message until the import
/// terminates: `Line` events carry one buffered/live output line;
/// `Done` is the terminal success marker. Failure terminates with an
/// `Envelope::Err` instead of a `Done`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ImportAttachEvent {
    /// One line of import output.
    Line { content: String },
    /// Import completed successfully — last message in the stream.
    Done,
}

/// Reply for [`Request::Snapshot`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotReply {
    pub snap_ulid: Ulid,
}

/// Internal return type for `force_snapshot_now_op`, used by the
/// fork orchestrator's `--force-snapshot` branch. The attestation
/// pubkey is hex-encoded so it can be propagated into the new fork's
/// signed provenance without re-encoding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForceSnapshotNowReply {
    pub snap_ulid: Ulid,
    pub attestation_pubkey_hex: String,
}

/// Reply for [`Request::Evict`]. `evicted` counts segment bodies
/// reclaimed from `cache/`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvictReply {
    pub evicted: usize,
}

/// Reply for [`Request::GenerateFilemap`]. Echoes the snapshot ULID
/// the filemap was written for — useful when the caller passed
/// `snap_ulid = None` and wants to learn the resolved value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenerateFilemapReply {
    pub snap_ulid: Ulid,
}

/// Reply for [`Request::GetStoreConfig`]. Either `local_path` is
/// set, or `bucket` is set (with optional `endpoint` / `region`).
/// All fields are optional on the wire so an "everything-defaulted"
/// store still serialises cleanly.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoreConfigReply {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub local_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub region: Option<String>,
}

/// Reply for [`Request::Credentials`]. Vended only over the
/// macaroon-authenticated path used by spawned volume subprocesses.
/// `expiry_unix` is set only on issued (short-lived) creds — the
/// long-lived passthrough issuer leaves it `None`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoreCredsReply {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub expiry_unix: Option<u64>,
}

/// Reply for [`Request::Register`]. The macaroon is base64-encoded;
/// the caller passes it back as-is in [`Request::Credentials`].
///
/// `peer_endpoint` is populated when the coordinator has resolved a
/// previous claimer's peer-fetch endpoint via discovery
/// (`coordinators/<id>/peer-endpoint.toml`). The volume process uses
/// it to construct a `PeerRangeFetcher` that consults the peer for
/// body byte ranges before falling through to S3. `None` means
/// peer-fetch is unconfigured, the predecessor is unreachable, or the
/// claim is not a clean handoff — the volume runs S3-only in that
/// case.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterReply {
    pub macaroon: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub peer_endpoint: Option<PeerEndpoint>,
}

/// Internal return type for `resolve_handoff_key_op`, also embedded
/// in [`ClaimAttachEvent::HandoffKeyResolved`] so subscribers can
/// see which key the handoff snapshot is signed under. `Normal`
/// means the manifest is signed by the source volume's own key (the
/// source volume's `volume.pub` is the verifying key); `Recovery`
/// means the manifest is a synthesised handoff snapshot signed by a
/// recovering coordinator under an attestation key — the carried hex
/// is the already-verified Ed25519 pubkey.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ResolveHandoffKeyReply {
    Normal,
    Recovery { manifest_pubkey_hex: String },
}

/// Outcome of verifying a [`VolumeEvent::signature`] against the
/// emitting coordinator's published `coordinator.pub`.
///
/// The CLI renders these as one-character markers in the operator
/// log: `Valid` is silent, the others get a sigil so a tampered or
/// orphaned event is visible at a glance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum SignatureStatus {
    /// Signature verified against the emitting coordinator's
    /// published pubkey.
    Valid,
    /// Pubkey was fetched, but `verify` rejected the signature.
    /// Either the event was tampered with after signing or the
    /// emitter signed under a different key than the one published
    /// at `coordinators/<id>/coordinator.pub`.
    Invalid { reason: String },
    /// Could not fetch the emitting coordinator's pubkey from the
    /// bucket. Typical causes: emitter never published its pubkey,
    /// network/I-O failure, or `coordinator_id` mismatch with the
    /// stored bytes.
    KeyUnavailable { reason: String },
    /// Event has no `signature` field. Should not happen for events
    /// written by this codebase — surfaced rather than hidden so a
    /// hand-edited or pre-signing-era record is obvious.
    Missing,
}

/// One entry in [`VolumeEventsReply`] — the parsed event paired with
/// its signature verification outcome.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeEventEntry {
    pub event: VolumeEvent,
    pub signature_status: SignatureStatus,
}

/// Reply for [`Request::VolumeEvents`]. Entries are sorted by
/// `event_ulid` ascending — the canonical history order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeEventsReply {
    pub events: Vec<VolumeEventEntry>,
}

/// Reply for [`Request::ForkStart`]. Empty success envelope: the
/// background job has been registered against the requested
/// `new_name` and the caller should now subscribe via
/// [`Request::ForkAttach`] to observe progress. The eventual fork
/// ULID is delivered as a [`ForkAttachEvent::ForkCreated`] event.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForkStartReply {}

/// Streaming event for [`Request::ForkAttach`]. The server writes
/// one [`Envelope<ForkAttachEvent>`] per message until the fork
/// flow terminates with `Done` (success) or `Envelope::Err` (failure).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ForkAttachEvent {
    /// Resolving a `ForkSource::Name` to a vol ULID (local then
    /// bucket-side `names/<name>`).
    ResolvingName { name: String },
    /// Pulling one ancestor's manifest / volume.pub / provenance.
    /// Emitted once per ancestor as the chain walks back.
    PullingAncestor { vol_ulid: Ulid },
    /// Took an implicit snapshot of a writable source before forking.
    SnapshotTaken { snap_ulid: Ulid },
    /// Synthesised an attested "now" snapshot under
    /// `--force-snapshot`. `pubkey_hex` is the ephemeral attestation
    /// key the fork's provenance is signed under.
    AttestedSnapshot { snap_ulid: Ulid, pubkey_hex: String },
    /// Source resolved; about to mint the fork.
    ForkingFrom {
        source_vol_ulid: Ulid,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        snap_ulid: Option<Ulid>,
    },
    /// Fork minted — `new_vol_ulid` is the fresh writable volume ULID.
    /// Matches the `fork_ulid` returned by `ForkStart`.
    ForkCreated { new_vol_ulid: Ulid },
    /// Background prefetch of the ancestor `.idx` chain has started.
    PrefetchStarted,
    /// Background prefetch finished (or volume is untracked).
    PrefetchDone,
    /// Terminal success — last message in the stream.
    Done,
}

/// Reply for [`Request::ClaimStart`]. `Reclaimed` is the
/// no-op-required happy path; `Claiming` signals the foreign-content
/// branch — the caller subscribes via [`Request::ClaimAttach`] to
/// observe the chain pull / fork mint / prefetch warm-up.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ClaimStartReply {
    /// In-place reclaim. The bucket flipped from `Released` to
    /// `Stopped` with the same `vol_ulid`; nothing else to do.
    Reclaimed,
    /// Foreign-content claim spawned as a background job. Echoes the
    /// released record's `vol_ulid` for diagnostic logging; the new
    /// fork's ULID is delivered later as a
    /// [`ClaimAttachEvent::ForkCreated`] event in the attach stream.
    Claiming { released_vol_ulid: Ulid },
}

/// Streaming event for [`Request::ClaimAttach`]. Same envelope shape
/// as [`ForkAttachEvent`]; named separately so the wire format stays
/// self-documenting and so claim-only events (handoff-key resolution)
/// don't pollute the fork enum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ClaimAttachEvent {
    /// Pulling one ancestor in the released volume's chain.
    PullingAncestor { vol_ulid: Ulid },
    /// Resolved which key the handoff snapshot is signed under.
    HandoffKeyResolved { key: ResolveHandoffKeyReply },
    /// Fresh fork minted from the released volume's handoff snapshot.
    ForkCreated { new_vol_ulid: Ulid },
    /// Background prefetch of the ancestor index has started.
    PrefetchStarted,
    /// Background prefetch finished (or budget expired — coordinator
    /// continues in background; the caller may proceed regardless).
    PrefetchDone,
    /// Terminal success.
    Done,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_envelope_wire_shape() {
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        struct Reply {
            value: u32,
        }
        let env = Envelope::ok(Reply { value: 42 });
        let s = serde_json::to_string(&env).unwrap();
        assert_eq!(s, r#"{"outcome":"ok","data":{"value":42}}"#);
        let parsed: Envelope<Reply> = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed.into_result().unwrap(), Reply { value: 42 });
    }

    #[test]
    fn err_envelope_wire_shape() {
        let env: Envelope<()> = Envelope::err(IpcError::not_found("vol foo"));
        let s = serde_json::to_string(&env).unwrap();
        assert_eq!(
            s,
            r#"{"outcome":"err","error":{"kind":"not-found","message":"vol foo"}}"#
        );
        let parsed: Envelope<()> = serde_json::from_str(&s).unwrap();
        let err = parsed.into_result().expect_err("should be Err");
        assert_eq!(err.kind, IpcErrorKind::NotFound);
        assert_eq!(err.message, "vol foo");
    }

    #[test]
    fn error_kind_kebab_case() {
        assert_eq!(
            format!("{}", IpcErrorKind::PreconditionFailed),
            "precondition-failed"
        );
        assert_eq!(format!("{}", IpcErrorKind::BadRequest), "bad-request");
        assert_eq!(format!("{}", IpcErrorKind::NotFound), "not-found");
    }

    #[test]
    fn into_result_round_trips() {
        let env: Envelope<u32> = Ok(7).into();
        assert_eq!(env.into_result().unwrap(), 7);
        let env: Envelope<u32> = Err(IpcError::internal("oops")).into();
        let e = env.into_result().expect_err("should be err");
        assert_eq!(e.kind, IpcErrorKind::Internal);
    }

    #[tokio::test]
    async fn round_trip_through_pipe() {
        use tokio::io::duplex;
        let (a, b) = duplex(4096);
        let (b_read, _b_write) = tokio::io::split(b);
        let (_a_read, mut a_write) = tokio::io::split(a);

        let env = Envelope::ok(42u32);
        write_message(&mut a_write, &env).await.unwrap();
        drop(a_write);

        let mut reader = BufReader::new(b_read);
        let parsed: Envelope<u32> = read_message(&mut reader).await.unwrap().unwrap();
        assert_eq!(parsed.into_result().unwrap(), 42);
    }

    #[test]
    fn fork_source_wire_shape() {
        let pinned = ForkSource::Pinned {
            vol_ulid: Ulid::nil(),
            snap_ulid: Ulid::nil(),
        };
        let s = serde_json::to_string(&pinned).unwrap();
        assert_eq!(
            s,
            r#"{"kind":"pinned","vol_ulid":"00000000000000000000000000","snap_ulid":"00000000000000000000000000"}"#
        );

        let by_name = ForkSource::Name {
            name: "demo".to_owned(),
        };
        let s = serde_json::to_string(&by_name).unwrap();
        assert_eq!(s, r#"{"kind":"name","name":"demo"}"#);
    }

    #[test]
    fn fork_attach_event_round_trip() {
        let events = [
            ForkAttachEvent::ResolvingName {
                name: "demo".to_owned(),
            },
            ForkAttachEvent::PullingAncestor {
                vol_ulid: Ulid::nil(),
            },
            ForkAttachEvent::SnapshotTaken {
                snap_ulid: Ulid::nil(),
            },
            ForkAttachEvent::AttestedSnapshot {
                snap_ulid: Ulid::nil(),
                pubkey_hex: "deadbeef".to_owned(),
            },
            ForkAttachEvent::ForkingFrom {
                source_vol_ulid: Ulid::nil(),
                snap_ulid: Some(Ulid::nil()),
            },
            ForkAttachEvent::ForkCreated {
                new_vol_ulid: Ulid::nil(),
            },
            ForkAttachEvent::PrefetchStarted,
            ForkAttachEvent::PrefetchDone,
            ForkAttachEvent::Done,
        ];
        for event in &events {
            let s = serde_json::to_string(event).unwrap();
            let parsed: ForkAttachEvent = serde_json::from_str(&s).unwrap();
            assert_eq!(&parsed, event);
        }
    }

    #[test]
    fn claim_start_reply_wire_shape() {
        let reclaimed = ClaimStartReply::Reclaimed;
        let s = serde_json::to_string(&reclaimed).unwrap();
        assert_eq!(s, r#"{"kind":"reclaimed"}"#);

        let claiming = ClaimStartReply::Claiming {
            released_vol_ulid: Ulid::nil(),
        };
        let s = serde_json::to_string(&claiming).unwrap();
        assert_eq!(
            s,
            r#"{"kind":"claiming","released_vol_ulid":"00000000000000000000000000"}"#
        );
    }

    #[test]
    fn claim_attach_event_round_trip() {
        let events = [
            ClaimAttachEvent::PullingAncestor {
                vol_ulid: Ulid::nil(),
            },
            ClaimAttachEvent::HandoffKeyResolved {
                key: ResolveHandoffKeyReply::Normal,
            },
            ClaimAttachEvent::HandoffKeyResolved {
                key: ResolveHandoffKeyReply::Recovery {
                    manifest_pubkey_hex: "deadbeef".to_owned(),
                },
            },
            ClaimAttachEvent::ForkCreated {
                new_vol_ulid: Ulid::nil(),
            },
            ClaimAttachEvent::PrefetchStarted,
            ClaimAttachEvent::PrefetchDone,
            ClaimAttachEvent::Done,
        ];
        for event in &events {
            let s = serde_json::to_string(event).unwrap();
            let parsed: ClaimAttachEvent = serde_json::from_str(&s).unwrap();
            assert_eq!(&parsed, event);
        }
    }

    #[test]
    fn fork_start_request_wire_shape() {
        let req = Request::ForkStart {
            new_name: "child".to_owned(),
            from: ForkSource::Name {
                name: "parent".to_owned(),
            },
            force_snapshot: true,
            flags: vec!["nbd-port=10809".to_owned()],
        };
        let s = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed, req);
    }

    #[tokio::test]
    async fn read_message_returns_none_on_eof() {
        use tokio::io::duplex;
        let (a, b) = duplex(64);
        drop(a); // immediate EOF
        let (b_read, _) = tokio::io::split(b);
        let mut reader = BufReader::new(b_read);
        let parsed: Option<Envelope<u32>> = read_message(&mut reader).await.unwrap();
        assert!(parsed.is_none());
    }
}
