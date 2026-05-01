//! Shared IPC envelope + error types used by both the coordinator's
//! inbound socket (`<data_dir>/control.sock`) and the volume's control
//! socket (`<fork_dir>/control.sock`).
//!
//! # Wire format
//!
//! Each message is a single JSON object on its own line (NDJSON). The
//! outer [`Envelope`] discriminator (`outcome: "ok"` vs `outcome:
//! "err"`) makes every reply uniformly routable without knowing the
//! verb. Errors carry a coarse [`IpcErrorKind`] tag plus a free-text
//! message intended for human display.
//!
//! Transport-specific helpers (sync vs tokio I/O) live alongside their
//! callers; this module is purely the type definitions.

use serde::{Deserialize, Serialize};

/// Outer envelope wrapping every reply on every typed IPC socket.
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
