// Pluggable credential issuance for the inbound `credentials` op.
//
// `CredentialIssuer` is the abstraction used by the inbound
// `credentials` handler to turn an authenticated request into an S3
// access triple. The minimum-viable backend is `SharedKeyPassthrough`,
// which returns the coordinator's own configured key — a "downgrade
// mode" with no per-volume IAM scoping, equivalent to today's
// `get-store-creds` behaviour but reached through the macaroon
// handshake. Per-volume backends (AWS STS, Tigris IAM) are planned
// and slot in behind this same trait.
//
// Coordinator identity (signing key + macaroon MAC root) lives in
// `crate::identity` — see `docs/design-portable-live-volume.md`
// § "Coordinator identity".

use std::io;
use std::sync::OnceLock;

use tracing::warn;

/// Credentials issued to a volume in response to an authenticated
/// `credentials` request.
pub struct IssuedCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    /// Unix-seconds expiry, when the issuer supports time-bounded creds.
    /// `None` means the credentials are long-lived (e.g. the shared-key
    /// passthrough issuer); the caller should not synthesize an expiry
    /// in that case.
    pub expiry_unix: Option<u64>,
}

/// Backend abstraction for the inbound `credentials` op. Implementations
/// see only the volume ULID and the coordinator's configured store; the
/// macaroon handshake (volume binding, PID check, MAC verify) runs
/// upstream and is identical for every backend.
pub trait CredentialIssuer: Send + Sync {
    fn issue(&self, volume_id: &str) -> io::Result<IssuedCredentials>;
}

/// Returns the coordinator's own configured access key for every volume.
/// No per-volume scoping; logs a downgrade warning at startup. This is
/// the "S3-compatible without STS or per-key IAM" row of the design doc
/// and is the minimum viable issuer — per-volume backends slot in behind
/// the same trait without changing the IPC handshake.
pub struct SharedKeyPassthrough;

impl SharedKeyPassthrough {
    pub fn new_with_warning() -> Self {
        warn!(
            "[coordinator] credential issuer: shared-key passthrough \
             (downgrade mode — same key vended to every volume; no per-volume IAM scoping)"
        );
        Self
    }
}

impl CredentialIssuer for SharedKeyPassthrough {
    fn issue(&self, _volume_id: &str) -> io::Result<IssuedCredentials> {
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| io::Error::other("AWS_ACCESS_KEY_ID not set in coordinator env"))?;
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| io::Error::other("AWS_SECRET_ACCESS_KEY not set in coordinator env"))?;
        let session_token = std::env::var("AWS_SESSION_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        Ok(IssuedCredentials {
            access_key_id,
            secret_access_key,
            session_token,
            expiry_unix: None,
        })
    }
}

/// Process-global credential issuer. Set once by `daemon::run` from the
/// configured backend (`SharedKeyPassthrough` today; per-volume IAM
/// backends slot in here later) and read by the IPC handler that
/// services `Request::Credentials` (`issue_credentials` in
/// `inbound::dispatch_json`). Stored as `&'static dyn CredentialIssuer`
/// via `Box::leak` so the IPC dispatch path doesn't have to clone an
/// `Arc` per connection or thread the value through `serve`/`handle`.
///
/// The leaf `issue_credentials` still takes a `&dyn CredentialIssuer`
/// argument so its unit tests can drive it with a stub issuer
/// (`FixedIssuer`) without touching the global.
static CREDENTIAL_ISSUER: OnceLock<&'static dyn CredentialIssuer> = OnceLock::new();

/// Install the daemon-wide credential issuer. Called once by
/// `daemon::run` before the IPC socket is bound; later calls are
/// silently ignored.
pub fn set_credential_issuer<I: CredentialIssuer + 'static>(issuer: I) {
    let leaked: &'static dyn CredentialIssuer = Box::leak(Box::new(issuer));
    let _ = CREDENTIAL_ISSUER.set(leaked);
}

/// Read the daemon-wide credential issuer.
///
/// Panics if `set_credential_issuer` has not been called. The only
/// caller is `inbound::dispatch_json`'s `Request::Credentials` arm,
/// reachable only via the IPC server bound after `daemon::run`
/// installs the value — so the unset case is an
/// impossible-to-violate invariant in production. Unit tests for
/// `issue_credentials` pass their stub issuer directly and never hit
/// this getter.
pub fn credential_issuer() -> &'static dyn CredentialIssuer {
    *CREDENTIAL_ISSUER
        .get()
        .expect("credential_issuer not set before IPC dispatch")
}
