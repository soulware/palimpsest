//! Coordinator-side mint enrollment
//! (`docs/coordinator-mint-enrollment-plan.md`).
//!
//! One blocking operator command: `POST /v1/enroll` (A), wait while the
//! operator approves out of band (B), then exchange the ticket once per
//! role (C), writing `<data_dir>/credentials/<role>`. The credential
//! ticket lives in memory for the command's duration and never touches
//! disk — `credentials/<role>` is the only durable enrollment artifact.
//!
//! Because the command holds the bootstrap macaroon for its whole
//! duration it self-heals the ticket-expiry race: if the short-lived
//! ticket expires before approval lands it transparently re-enrolls
//! (the operator must then re-approve, since mint GC's the pending
//! record at the ticket `exp`).
//!
//! The macaroon / PoP / transport primitives are reused from
//! `crate::mint_client` (reimplemented there against the spec, no mint
//! dependency — the same deliberate duplication).

use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use tracing::{info, warn};

use elide_coordinator::config::MintConfig;
use elide_coordinator::identity::CoordinatorIdentity;

use crate::mint_client::{
    COORD_ENROLL_ROLES, WireMacaroon, json_str_field, now_unix, pop_digest, post,
};

const CAVEAT_SUB: &str = "sub";
const CAVEAT_CNF: &str = "cnf";

/// How often to re-attempt the exchange while awaiting operator
/// approval. Foreground operator command — a short, predictable cadence
/// the operator can watch, not a cache-driven one.
const POLL_INTERVAL: Duration = Duration::from_secs(3);

/// `ed25519:<base64 pub>` — the `cnf` value mint seals into the ticket
/// and verifies the PoP against (`mint/src/pop.rs::cnf_value`).
fn cnf_value(identity: &CoordinatorIdentity) -> String {
    format!(
        "ed25519:{}",
        BASE64.encode(identity.verifying_key().to_bytes())
    )
}

/// Stable short fingerprint of a `cnf` value: BLAKE3 of the raw string,
/// first 8 bytes hex. Byte-identical to what `mint enroll list` prints
/// (`mint/src/state.rs::fingerprint`), so the operator can compare the
/// two out of band before approving.
fn fingerprint(cnf: &str) -> String {
    blake3::hash(cnf.as_bytes()).as_bytes()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn credentials_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("credentials")
}

fn credential_path(data_dir: &Path, role: &str) -> PathBuf {
    credentials_dir(data_dir).join(role)
}

/// Resolve the bootstrap argument: `-` reads stdin, an inline macaroon
/// is used verbatim, anything else is a file path. Validated by a
/// decode at the boundary so a bad source fails here, not at the PoP.
fn resolve_bootstrap(src: &str) -> io::Result<String> {
    let raw = if src == "-" {
        let mut s = String::new();
        io::stdin().read_to_string(&mut s)?;
        s
    } else if WireMacaroon::decode(src).is_ok() {
        src.to_owned()
    } else {
        std::fs::read_to_string(src).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "bootstrap macaroon: {src:?} is neither an inline macaroon nor a readable file: {e}"
                ),
            )
        })?
    };
    let trimmed = raw.trim().to_owned();
    WireMacaroon::decode(&trimmed)
        .map_err(|e| io::Error::other(format!("bootstrap macaroon failed to decode: {e}")))?;
    Ok(trimmed)
}

/// A — `POST /v1/enroll`. Attenuate the bootstrap with `sub`/`cnf`, PoP
/// over `{ts}`, return the credential-ticket macaroon string.
async fn enroll_request(
    cfg: &MintConfig,
    identity: &CoordinatorIdentity,
    bootstrap: &str,
) -> io::Result<String> {
    let mut mac = WireMacaroon::decode(bootstrap)?;
    mac.attenuate(CAVEAT_SUB, identity.coordinator_id_str());
    mac.attenuate(CAVEAT_CNF, &cnf_value(identity));

    let body = format!(r#"{{"ts":{}}}"#, now_unix()?);
    let sig = BASE64.encode(identity.sign(&pop_digest(mac.tail(), body.as_bytes())));
    let auth = format!("Macaroon {}", mac.encode());

    let (status, text) = post(
        &cfg.url,
        cfg.connect_timeout,
        cfg.request_timeout,
        "/v1/enroll",
        &auth,
        &sig,
        body,
    )
    .await?;
    if status != 200 {
        let snippet: String = text.chars().take(200).collect();
        return Err(io::Error::other(format!(
            "mint /v1/enroll returned {status}: {snippet}"
        )));
    }
    json_str_field(&text, "credential.ticket")
}

enum ExchangeOutcome {
    Granted(String),
    AwaitingApproval,
    TicketExpired,
}

/// C (one role) — `POST /v1/enroll-exchange`, body `{ts, role}`, PoP
/// over it. `200` → the credential; `403` → not yet approved; `401` →
/// ticket expired (the single command re-enrolls); anything else fails.
async fn exchange_request(
    cfg: &MintConfig,
    identity: &CoordinatorIdentity,
    ticket: &str,
    role: &str,
) -> io::Result<ExchangeOutcome> {
    let mac = WireMacaroon::decode(ticket)?;
    let body = format!(
        r#"{{"ts":{},"role":{}}}"#,
        now_unix()?,
        serde_json::Value::from(role)
    );
    let sig = BASE64.encode(identity.sign(&pop_digest(mac.tail(), body.as_bytes())));
    let auth = format!("Macaroon {}", mac.encode());

    let (status, text) = post(
        &cfg.url,
        cfg.connect_timeout,
        cfg.request_timeout,
        "/v1/enroll-exchange",
        &auth,
        &sig,
        body,
    )
    .await?;
    match status {
        200 => Ok(ExchangeOutcome::Granted(json_str_field(
            &text,
            "credential",
        )?)),
        403 => Ok(ExchangeOutcome::AwaitingApproval),
        401 => Ok(ExchangeOutcome::TicketExpired),
        s => {
            let snippet: String = text.chars().take(200).collect();
            Err(io::Error::other(format!(
                "mint /v1/enroll-exchange ({role}) returned {s}: {snippet}"
            )))
        }
    }
}

/// Validate the credential decodes, then write it `0600` to
/// `credentials/<role>` via a temp file + rename.
fn write_credential(data_dir: &Path, role: &str, credential: &str) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    WireMacaroon::decode(credential).map_err(|e| {
        io::Error::other(format!(
            "mint returned an undecodable {role} credential: {e}"
        ))
    })?;
    let dir = credentials_dir(data_dir);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(role);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, credential.as_bytes())?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&tmp, &path)
}

fn credential_present(data_dir: &Path, role: &str) -> bool {
    std::fs::read_to_string(credential_path(data_dir, role))
        .ok()
        .is_some_and(|s| WireMacaroon::decode(s.trim()).is_ok())
}

/// `[mint]` startup gate. Every enrolled role's credential must exist
/// and decode; otherwise the daemon refuses to start half-credentialed.
pub fn assert_enrolled(data_dir: &Path) -> io::Result<()> {
    let missing: Vec<&str> = COORD_ENROLL_ROLES
        .iter()
        .copied()
        .filter(|role| !credential_present(data_dir, role))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "[mint] is configured but credential(s) for [{}] are missing or unreadable under {}; \
         run `elide coord enroll` to provision them",
        missing.join(", "),
        credentials_dir(data_dir).display()
    )))
}

/// The single blocking operator command: A → wait for approval → C
/// fan-out. Idempotent — only roles whose credential is absent (or all,
/// under `force`) are (re-)exchanged; an already-complete enrollment is
/// a no-op.
pub async fn run(
    cfg: &MintConfig,
    identity: &CoordinatorIdentity,
    data_dir: &Path,
    bootstrap_src: &str,
    wait: Duration,
    force: bool,
) -> io::Result<()> {
    let mut remaining: Vec<&str> = COORD_ENROLL_ROLES
        .iter()
        .copied()
        .filter(|role| force || !credential_present(data_dir, role))
        .collect();
    if remaining.is_empty() {
        info!(
            "[enroll] all {} role credential(s) already present under {}; nothing to do",
            COORD_ENROLL_ROLES.len(),
            credentials_dir(data_dir).display()
        );
        return Ok(());
    }

    let bootstrap = resolve_bootstrap(bootstrap_src)?;
    let sub = identity.coordinator_id_str();
    let cnf = cnf_value(identity);

    let mut ticket = enroll_request(cfg, identity, &bootstrap).await?;
    info!(
        "[enroll] enrolled sub={sub} cnf-fingerprint={} — now run `mint enroll approve {sub}` \
         on the mint host (match that fingerprint out of band first)",
        fingerprint(&cnf)
    );
    info!(
        "[enroll] waiting for approval, exchanging {} role(s): [{}]",
        remaining.len(),
        remaining.join(", ")
    );

    let deadline = Instant::now() + wait;
    loop {
        // One approval covers every role; the ticket is multi-use until
        // its `exp`, so on AwaitingApproval there is no point trying the
        // other roles this pass.
        let mut awaiting = false;
        // Always process from the front: Granted removes the head;
        // AwaitingApproval / TicketExpired break the pass.
        let idx = 0;
        while idx < remaining.len() {
            let role = remaining[idx];
            match exchange_request(cfg, identity, &ticket, role).await? {
                ExchangeOutcome::Granted(credential) => {
                    write_credential(data_dir, role, &credential)?;
                    info!("[enroll] {role}: credential written");
                    remaining.remove(idx);
                }
                ExchangeOutcome::AwaitingApproval => {
                    awaiting = true;
                    break;
                }
                ExchangeOutcome::TicketExpired => {
                    warn!(
                        "[enroll] credential ticket expired before approval; re-enrolling — \
                         the operator must re-run `mint enroll approve {sub}`"
                    );
                    ticket = enroll_request(cfg, identity, &bootstrap).await?;
                    awaiting = true;
                    break;
                }
            }
        }

        if remaining.is_empty() {
            info!(
                "[enroll] complete: {} role credential(s) under {}",
                COORD_ENROLL_ROLES.len(),
                credentials_dir(data_dir).display()
            );
            return Ok(());
        }
        if !awaiting {
            continue;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for operator approval; [{}] still unenrolled. \
                 Approval persists — re-run `elide coord enroll` to resume",
                remaining.join(", ")
            )));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_matches_mint_algorithm() {
        // BLAKE3 of the raw cnf string, first 8 bytes hex.
        let cnf = "ed25519:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let expect: String = blake3::hash(cnf.as_bytes()).as_bytes()[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(fingerprint(cnf), expect);
        assert_eq!(fingerprint(cnf).len(), 16);
    }

    #[test]
    fn resolve_bootstrap_distinguishes_inline_file_and_garbage() {
        // A real wire macaroon, built the way mint mints one.
        let nonce = [5u8; 16];
        let root = [2u8; 32];
        const DOMAIN: &[u8] = b"mint-macaroon-v1";
        let mut seed = Vec::new();
        seed.extend_from_slice(DOMAIN);
        seed.extend_from_slice(&nonce);
        let mut key = *blake3::keyed_hash(&root, &seed).as_bytes();
        let mut ser = Vec::new();
        ser.extend_from_slice(&(3u32).to_be_bytes());
        ser.extend_from_slice(b"aud");
        ser.extend_from_slice(&(4u32).to_be_bytes());
        ser.extend_from_slice(b"mint");
        key = *blake3::keyed_hash(&key, &ser).as_bytes();
        let mut buf = Vec::new();
        buf.extend_from_slice(b"mcrn1");
        buf.extend_from_slice(&nonce);
        buf.extend_from_slice(&key);
        buf.extend_from_slice(&(1u16).to_be_bytes());
        buf.extend_from_slice(&ser);
        let inline = BASE64.encode(buf);

        assert_eq!(resolve_bootstrap(&inline).expect("inline"), inline);

        let dir = tempfile::tempdir().expect("tempdir");
        let f = dir.path().join("bootstrap.mac");
        std::fs::write(&f, format!("  {inline}\n")).expect("write");
        assert_eq!(
            resolve_bootstrap(f.to_str().expect("utf8")).expect("file"),
            inline
        );

        assert!(resolve_bootstrap("not-a-macaroon-and-not-a-path").is_err());
    }

    #[test]
    fn assert_enrolled_reports_missing_roles() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = assert_enrolled(dir.path()).expect_err("none present");
        let msg = err.to_string();
        for role in COORD_ENROLL_ROLES {
            assert!(msg.contains(role), "missing list should name {role}: {msg}");
        }
    }
}
