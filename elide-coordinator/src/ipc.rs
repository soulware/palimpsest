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

use elide_core::name_event::NameEvent;
use elide_core::name_record::NameState;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use ulid::Ulid;

use crate::eligibility::Eligibility;
use crate::volume_state::VolumeLifecycle;

/// Outer envelope wrapping every reply from the coordinator.
///
/// The `outcome` discriminator is what makes two replies distinguishable
/// at the protocol layer — every verb shares this shape, so a generic
/// client can always route a reply to "ok handler" vs "err handler"
/// without knowing the verb.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "lowercase")]
pub enum Envelope<T> {
    /// Operation succeeded. `data` is the verb-specific reply payload.
    Ok { data: T },
    /// Operation failed. `error` carries the kind + operator-readable message.
    Err { error: IpcError },
}

impl<T> Envelope<T> {
    /// Convert to a `Result` for ergonomic `?`-propagation on the client.
    pub fn into_result(self) -> Result<T, IpcError> {
        match self {
            Self::Ok { data } => Ok(data),
            Self::Err { error } => Err(error),
        }
    }

    /// Build an `Envelope::Ok` from a value.
    pub fn ok(value: T) -> Self {
        Self::Ok { data: value }
    }

    /// Build an `Envelope::Err` from an error.
    pub fn err(error: IpcError) -> Self {
        Self::Err { error }
    }
}

impl<T> From<Result<T, IpcError>> for Envelope<T> {
    fn from(r: Result<T, IpcError>) -> Self {
        match r {
            Ok(v) => Self::ok(v),
            Err(e) => Self::err(e),
        }
    }
}

/// Coarse error classification for IPC replies.
///
/// Callers branch on the `kind` to decide how to react (retry, escalate,
/// surface to the operator). The `message` is free-text intended for
/// human display — never parse it programmatically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcError {
    pub kind: IpcErrorKind,
    pub message: String,
}

impl IpcError {
    pub fn new(kind: IpcErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(IpcErrorKind::NotFound, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(IpcErrorKind::Conflict, message)
    }

    pub fn precondition_failed(message: impl Into<String>) -> Self {
        Self::new(IpcErrorKind::PreconditionFailed, message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(IpcErrorKind::BadRequest, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(IpcErrorKind::Forbidden, message)
    }

    pub fn store(message: impl Into<String>) -> Self {
        Self::new(IpcErrorKind::Store, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(IpcErrorKind::Internal, message)
    }
}

impl std::fmt::Display for IpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for IpcError {}

/// Coarse kinds — pick the closest match rather than adding a new variant.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum IpcErrorKind {
    /// The named resource (volume, name, segment) doesn't exist.
    NotFound,
    /// The operation conflicts with current state — held by another
    /// coordinator, illegal lifecycle transition, NBD client connected,
    /// volume not stopped, etc.
    Conflict,
    /// A conditional-write CAS lost to a concurrent writer.
    PreconditionFailed,
    /// The request was malformed: missing args, wrong types, unparseable
    /// ULID/name, etc.
    BadRequest,
    /// Caller is not authorised: peer-uid mismatch, macaroon failed,
    /// readonly-volume mutation refused.
    Forbidden,
    /// Transient store failure (object_store I/O, network, etc.).
    /// Callers can usually retry.
    Store,
    /// Catch-all for anything else. Includes panics, invariant
    /// violations, and "not implemented yet" stubs during migration.
    Internal,
}

impl std::fmt::Display for IpcErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::NotFound => "not-found",
            Self::Conflict => "conflict",
            Self::PreconditionFailed => "precondition-failed",
            Self::BadRequest => "bad-request",
            Self::Forbidden => "forbidden",
            Self::Store => "store",
            Self::Internal => "internal",
        };
        f.write_str(s)
    }
}

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
    /// Bucket-side claim flow against a `Released` record. Either
    /// reclaims in place or signals the CLI to mint a fresh fork.
    Claim { volume: String },
    /// Local resume of a `Stopped` volume. Empty success reply.
    Start { volume: String },
    /// Atomically rebind a `Released` name to a freshly-minted local
    /// fork. Internal helper called by the CLI's claim orchestration.
    RebindName { volume: String, new_vol_ulid: Ulid },

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
    /// Pull one readonly ancestor (manifest + volume.pub + provenance)
    /// from the store. Returns the parent ULID parsed from the verified
    /// provenance, or `None` if the ancestor is a root volume.
    PullReadonly { vol_ulid: Ulid },
    /// Mint a fresh writable fork from a local source. `for_claim`
    /// switches off the bucket-side `mark_initial` step — the
    /// claim-from-released flow handles the rebind via a subsequent
    /// `claim` call.
    ForkCreate {
        new_name: String,
        source_vol_ulid: Ulid,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        snap: Option<Ulid>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        parent_key_hex: Option<String>,
        #[serde(default)]
        for_claim: bool,
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
    /// Synthesise a "now" snapshot for a readonly source volume by
    /// pinning currently-prefetched segments under an ephemeral
    /// attestation key. Used by force-claim to mint a handoff against
    /// an unreachable owner.
    ForceSnapshotNow { vol_ulid: Ulid },
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
    /// Resolve which Ed25519 key the snapshot manifest at
    /// `by_id/<vol_ulid>/snapshots/<snap_ulid>.manifest` is signed by.
    /// The reply carries either `Normal` (use the source volume's
    /// `volume.pub`) or `Recovery { hex }` (the recovering
    /// coordinator's already-verified pubkey, ready to embed in
    /// `fork-create` as `parent-key=<hex>`).
    ResolveHandoffKey { vol_ulid: Ulid, snap_ulid: Ulid },

    // ── Read-only history ────────────────────────────────────────────
    /// List the per-name event log at `events/<volume>/`,
    /// verifying each entry's signature against the emitting
    /// coordinator's published pubkey.
    NameEvents { volume: String },

    // ── Creds + cleanup (final iteration) ────────────────────────────
    /// Vend the non-secret `[store]` config (bucket / endpoint /
    /// region or local_path). Used by CLI read paths to build an S3
    /// client that matches the coordinator's view of the world.
    GetStoreConfig,
    /// Vend long-lived S3 credentials from the coordinator's env.
    GetStoreCreds,
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

/// Reply for [`Request::RebindName`]. Echoes the new fork's ULID for
/// confirmation; the coordinator atomically rebound `names/<volume>`
/// to it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebindNameReply {
    pub new_vol_ulid: Ulid,
}

/// Reply for [`Request::Claim`]. The bucket-side claim flow is
/// non-deterministic in shape: an in-place reclaim succeeds with no
/// further action, but a foreign-content claim returns the routing
/// info the CLI needs to materialise a fresh fork and call
/// `rebind-name`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ClaimReply {
    /// In-place reclaim. The local fork's ULID matched the released
    /// record's; the bucket flipped from `Released` to `Stopped` with
    /// the same `vol_ulid`. No CLI orchestration is needed.
    Reclaimed,
    /// The released record's `vol_ulid` is foreign content. The CLI
    /// must pull the source, mint a fresh local fork, and call
    /// `rebind-name` with the new ULID. `handoff_snapshot` is the
    /// snapshot the new fork will inherit from; absent when the
    /// released record has no recorded handoff yet.
    MustClaimFresh {
        released_vol_ulid: Ulid,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        handoff_snapshot: Option<Ulid>,
    },
}

/// Reply for [`Request::Create`] and [`Request::ForkCreate`]. The new
/// volume's ULID is what the caller stamps into local state for any
/// follow-up IPCs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateReply {
    pub vol_ulid: Ulid,
}

/// Reply for [`Request::ForkCreate`].
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

/// Reply for [`Request::PullReadonly`]. `parent` is `None` for a root
/// ancestor; otherwise it carries the verified parent ULID parsed from
/// the just-fetched provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PullReadonlyReply {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parent: Option<Ulid>,
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

/// Reply for [`Request::ForceSnapshotNow`]. The attestation pubkey is
/// hex-encoded so callers can pass it through to `fork-create` as a
/// `parent-key=<hex>` token without re-encoding.
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

/// Reply for [`Request::GetStoreCreds`] and [`Request::Credentials`].
/// `expiry_unix` is set only on issued (short-lived) creds — the
/// long-lived env-creds path leaves it `None`.
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterReply {
    pub macaroon: String,
}

/// Reply for [`Request::ResolveHandoffKey`]. `Normal` means the
/// manifest is signed by the source volume's own key (the source
/// volume's `volume.pub` is the verifying key); `Recovery` means the
/// manifest is a synthesised handoff snapshot signed by a recovering
/// coordinator under an attestation key — the carried hex is the
/// already-verified Ed25519 pubkey.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ResolveHandoffKeyReply {
    Normal,
    Recovery { manifest_pubkey_hex: String },
}

/// Outcome of verifying a [`NameEvent::signature`] against the
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

/// One entry in [`NameEventsReply`] — the parsed event paired with
/// its signature verification outcome.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NameEventEntry {
    pub event: NameEvent,
    pub signature_status: SignatureStatus,
}

/// Reply for [`Request::NameEvents`]. Entries are sorted by
/// `event_ulid` ascending — the canonical history order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NameEventsReply {
    pub events: Vec<NameEventEntry>,
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
