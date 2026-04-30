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
    /// Trigger an immediate fork-discovery pass. No payload.
    Rescan,
    /// Report a volume's local lifecycle (running / stopped / importing).
    Status { volume: String },
    /// Fetch the bucket-side `names/<volume>` record plus this
    /// coordinator's eligibility to act on it.
    StatusRemote { volume: String },
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
