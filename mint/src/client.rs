//! Reference client — the coordinator's half of the flow
//! (`docs/design-mint.md` § *Reference client & demo*). Lives in `mint/`
//! with no `elide-*` deps; it is also the conformance surface the
//! integration tests exercise.
//!
//! Identity is a `.key`/`.pub` pair, mirroring the elide coordinator
//! convention (`elide-coordinator/src/identity.rs`): lowercase hex of
//! the 32-byte Ed25519 material with a trailing newline; the private
//! `client.key` is mode 0600. Both live under a client directory
//! defaulting to `./mint_client` (analogous to the server's
//! `./mint_data`), overridable with `--client-dir`. The credential
//! ticket and credential received from the server are persisted there
//! too (file names are `--out`/`--in` overridable), so the client is
//! self-contained.

use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use ed25519_dalek::SigningKey;
use rand_core::{OsRng, RngCore};

use crate::caveat::{Caveat, name, op};
use crate::macaroon::Macaroon;
use crate::pop;
use crate::state::fingerprint;

const KEY_FILE: &str = "client.key";
const PUB_FILE: &str = "client.pub";
/// Default `enroll --out` / `exchange --in`: the credential ticket —
/// the short-lived, redeem-once token you trade in at the exchange.
pub const CREDENTIAL_TICKET_FILE: &str = "credential.ticket";
/// Per-role credentials live one file per role under this directory:
/// `credentials/<role>`. Kept distinct from the flat `credential.ticket`
/// so the `credential.` name is never overloaded (`docs/design-mint.md`
/// § *Coordinator bootstrap*).
pub const CREDENTIALS_DIR: &str = "credentials";

/// The default on-disk path (under the client dir) for the credential
/// of `role` — `credentials/<role>`. The `exchange --out` /
/// `assume-role --in` default.
pub fn credential_path(role: &str) -> String {
    format!("{CREDENTIALS_DIR}/{role}")
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{KEY_FILE} already exists in {0} (use --force to overwrite)")]
    KeyExists(String),
    #[error("malformed {0}")]
    BadFile(&'static str),
    #[error("{path} not found — {hint}")]
    Missing { path: String, hint: &'static str },
    #[error("--request must be a JSON object ({0})")]
    BadRequest(&'static str),
    #[error("--caveat must be NAME=VALUE (got {0:?})")]
    BadCaveat(String),
    #[error(
        "exchange refused (401) — the credential ticket most likely expired \
         (it is short-lived). Re-run `mint client enroll …` for a fresh \
         one; your approval persists, so just `mint client exchange` again"
    )]
    TicketRejected,
    #[error("server returned {status}: {body}")]
    Server { status: u16, body: String },
    #[error("server response missing the {0} field")]
    MissingField(&'static str),
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_hex_32(s: &str) -> Result<[u8; 32], ClientError> {
    let s = s.trim();
    if s.len() != 64 {
        return Err(ClientError::BadFile(KEY_FILE));
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| ClientError::BadFile(KEY_FILE))?;
    }
    Ok(out)
}

fn write_0600(path: &Path, bytes: &[u8]) -> io::Result<()> {
    fs::write(path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

/// Read a client-state file, turning a missing one into an actionable
/// error (which path, and the prerequisite command) rather than an
/// opaque `Io(NotFound)`. Other io errors stay distinct.
fn read_text(path: &Path, hint: &'static str) -> Result<String, ClientError> {
    match fs::read_to_string(path) {
        Ok(s) => Ok(s),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(ClientError::Missing {
            path: path.display().to_string(),
            hint,
        }),
        Err(e) => Err(ClientError::Io(e)),
    }
}

/// Generate a fresh Ed25519 identity into `dir`. Refuses to clobber an
/// existing `client.key` unless `force` (a key *is* an identity —
/// overwriting it silently is a footgun, same stance as elide).
/// Returns the `cnf` value and its fingerprint for display.
pub fn keygen(dir: &Path, force: bool) -> Result<(String, String), ClientError> {
    fs::create_dir_all(dir)?;
    let key_path = dir.join(KEY_FILE);
    if key_path.exists() && !force {
        return Err(ClientError::KeyExists(dir.display().to_string()));
    }
    let mut seed = [0u8; 32];
    OsRng.fill_bytes(&mut seed);
    let vk = SigningKey::from_bytes(&seed).verifying_key();
    write_0600(&key_path, format!("{}\n", encode_hex(&seed)).as_bytes())?;
    fs::write(
        dir.join(PUB_FILE),
        format!("{}\n", encode_hex(&vk.to_bytes())),
    )?;
    let cnf = pop::cnf_value(&seed);
    Ok((cnf.clone(), fingerprint(&cnf)))
}

fn load_seed(dir: &Path) -> Result<[u8; 32], ClientError> {
    let raw = read_text(
        &dir.join(KEY_FILE),
        "run `mint client keygen` (or --client-dir to point at an existing identity)",
    )?;
    decode_hex_32(&raw)
}

/// `(cnf, fingerprint)` for the identity in `dir` — what the operator
/// compares out of band before `mint enroll approve`.
pub fn identity(dir: &Path) -> Result<(String, String), ClientError> {
    let cnf = pop::cnf_value(&load_seed(dir)?);
    let fp = fingerprint(&cnf);
    Ok((cnf, fp))
}

fn now_unix() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

/// Resolve a `--bootstrap` argument, in precedence order:
/// `-` → stdin; an inline macaroon (the value itself decodes — the
/// strict `mcrn1` magic + structure check makes this an unambiguous
/// discriminator, no real file path collides); otherwise a file path.
fn read_macaroon_arg(src: &str) -> Result<Macaroon, ClientError> {
    if src == "-" {
        let mut s = String::new();
        io::stdin().read_to_string(&mut s)?;
        return Macaroon::decode(s.trim()).map_err(|_| ClientError::BadFile("bootstrap macaroon"));
    }
    if let Ok(m) = Macaroon::decode(src.trim()) {
        return Ok(m); // inline macaroon text
    }
    let text = fs::read_to_string(src).map_err(|_| {
        ClientError::BadFile("bootstrap (not an inline macaroon nor a readable file)")
    })?;
    Macaroon::decode(text.trim()).map_err(|_| ClientError::BadFile("bootstrap macaroon"))
}

/// POST `body` with the macaroon + PoP, return `(status, text)`.
async fn post(
    url: &str,
    mac: &Macaroon,
    seed: &[u8; 32],
    body: String,
) -> Result<(u16, String), ClientError> {
    let sig = pop::client_signature(seed, mac.tail(), body.as_bytes());
    let resp = reqwest::Client::new()
        .post(url)
        .header("authorization", format!("Macaroon {}", mac.encode()))
        .header("x-mint-coord-pop", sig)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await?;
    let status = resp.status().as_u16();
    Ok((status, resp.text().await?))
}

fn json_field(body: &str, key: &'static str) -> Result<String, ClientError> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get(key).and_then(|s| s.as_str()).map(str::to_string))
        .ok_or(ClientError::MissingField(key))
}

fn save_macaroon(dir: &Path, file: &str, b64: &str) -> Result<(), ClientError> {
    // Parse-don't-validate: only persist something that decodes.
    Macaroon::decode(b64).map_err(|_| ClientError::BadFile("server macaroon"))?;
    let path = dir.join(file);
    // `file` may be nested (e.g. `credentials/<role>`); create the
    // parent so per-role credentials land under their directory.
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, b64)?;
    Ok(())
}

/// Shorten a long opaque value for display, keeping both ends so it
/// stays recognisable. Standard caveat values here are ASCII (ULID,
/// `ed25519:<b64>`, unix int), so byte slicing is char-safe.
fn abbrev(s: &str) -> String {
    if s.len() <= 28 {
        s.to_string()
    } else {
        format!("{}…{}", &s[..14], &s[s.len() - 8..])
    }
}

/// One-line plain-English gloss for a standard caveat, so the demo
/// narration explains *why* each line is there. mint is
/// caveat-vocabulary-agnostic; an unrecognised name glosses to nothing
/// and is still shown verbatim.
fn caveat_gloss(cav: &Caveat) -> &'static str {
    match cav.name.as_str() {
        name::OP => match cav.value.as_str() {
            op::ENROLL => "participation gate — enroll only",
            op::ENROLL_EXCHANGE => "redeem-once — may only be exchanged for a credential",
            op::ASSUME_ROLE => "the working credential — mints role keypairs",
            _ => "mint operation this token is partitioned to",
        },
        name::AUD => "the mint instance this token is for",
        name::SUB => "the enrollment identity (operator-approved)",
        name::CNF => "bound to this client's key — proof-of-possession",
        name::EXP => "expiry, unix seconds",
        name::ROLE => "restricts the assumable role",
        name::BOOTSTRAP => "current bootstrap nonce",
        _ => "",
    }
}

/// Narrate a macaroon's caveat chain to stderr — what this token *is*,
/// in the demo. Renders `exp` as a readable UTC instant alongside the
/// raw seconds.
fn describe(label: &str, m: &Macaroon) {
    eprintln!("  {label} — {} caveat(s):", m.caveats().len());
    for c in m.caveats() {
        let mut shown = abbrev(&c.value);
        let exp_instant = (c.name == name::EXP)
            .then(|| c.value.parse::<i64>().ok())
            .flatten()
            .and_then(|s| chrono::DateTime::from_timestamp(s, 0));
        if let Some(dt) = exp_instant {
            shown = format!("{shown} ({})", dt.format("%Y-%m-%dT%H:%M:%SZ"));
        }
        let gloss = caveat_gloss(c);
        if gloss.is_empty() {
            eprintln!("    {:<10} {shown}", c.name);
        } else {
            eprintln!("    {:<10} {shown}  — {gloss}", c.name);
        }
    }
}

/// `mint client enroll`: attenuate the bootstrap macaroon with this
/// identity's `sub`/`cnf`, prove possession, receive + persist the
/// credential ticket.
pub async fn enroll(
    dir: &Path,
    base_url: &str,
    bootstrap_src: &str,
    sub: &str,
    out: &str,
) -> Result<(), ClientError> {
    let seed = load_seed(dir)?;
    let cnf = pop::cnf_value(&seed);
    let presented = read_macaroon_arg(bootstrap_src)?
        .attenuate(Caveat::scalar(name::SUB, sub))
        .attenuate(Caveat::scalar(name::CNF, cnf.clone()));
    eprintln!("enroll: attenuating the operator's bootstrap macaroon with your identity");
    eprintln!("  sub = {sub}  (the principal you are claiming)");
    eprintln!(
        "  cnf = {}  (your client key — binds the token to you)",
        abbrev(&cnf)
    );
    eprintln!("  → POST {base_url}/v1/enroll  (signed with your client key)");
    let body = format!(r#"{{"ts":{}}}"#, now_unix());
    let (status, text) = post(&format!("{base_url}/v1/enroll"), &presented, &seed, body).await?;
    if status != 200 {
        return Err(ClientError::Server { status, body: text });
    }
    let ticket = json_field(&text, "credential.ticket")?;
    if let Ok(m) = Macaroon::decode(&ticket) {
        eprintln!("  ← 200 — mint minted a credential ticket from its root:");
        describe("credential ticket", &m);
    }
    save_macaroon(dir, out, &ticket)?;
    eprintln!(
        "  saved to {}  (now: operator runs `mint enroll approve {sub}`)",
        dir.join(out).display()
    );
    Ok(())
}

/// `mint client exchange`: present the credential ticket. `Ok(true)` =
/// credential written; `Ok(false)` = still awaiting operator approval
/// (HTTP 403, the one non-failure non-200) — the caller decides the
/// exit code / retry.
pub async fn exchange(
    dir: &Path,
    base_url: &str,
    in_file: &str,
    role: &str,
    out: &str,
) -> Result<bool, ClientError> {
    let seed = load_seed(dir)?;
    let in_path = dir.join(in_file);
    let ticket = Macaroon::decode(read_text(&in_path, "run `mint client enroll …` first")?.trim())
        .map_err(|_| ClientError::BadFile("credential ticket"))?;
    eprintln!(
        "exchange: presenting your credential ticket ({}) for role `{role}`",
        in_path.display()
    );
    describe("credential ticket (what you hold)", &ticket);
    eprintln!(
        "  → POST {base_url}/v1/enroll-exchange  role={role}  (signed with your client key — proof-of-possession)"
    );
    let body = format!(
        r#"{{"ts":{},"role":{}}}"#,
        now_unix(),
        serde_json::Value::from(role)
    );
    let (status, text) = post(
        &format!("{base_url}/v1/enroll-exchange"),
        &ticket,
        &seed,
        body,
    )
    .await?;
    match status {
        200 => {
            let credential = json_field(&text, "credential")?;
            if let Ok(m) = Macaroon::decode(&credential) {
                eprintln!(
                    "  ← 200 — mint re-minted a credential from its root (a fresh chain, not an attenuation of the ticket):"
                );
                describe("credential (what you received)", &m);
            }
            save_macaroon(dir, out, &credential)?;
            eprintln!("  saved to {}", dir.join(out).display());
            Ok(true)
        }
        403 => {
            eprintln!("  ← 403 — the operator has not approved this enrollment yet");
            Ok(false)
        }
        // The server's 401 is deliberately opaque, but at exchange the
        // overwhelmingly likely cause is an expired ticket (it is
        // short-lived by design). Point at the idempotent remedy rather
        // than echoing a bare unauthorized.
        401 => Err(ClientError::TicketRejected),
        _ => Err(ClientError::Server { status, body: text }),
    }
}

/// Parse `NAME=VALUE` narrowing-caveat args. mint is
/// caveat-vocabulary-agnostic, so the client is too: no name is
/// special-cased (`elide:Volume`, `exp`, anything).
fn parse_caveats(args: &[String]) -> Result<Vec<(String, String)>, ClientError> {
    args.iter()
        .map(|a| match a.split_once('=') {
            Some((n, v)) if !n.is_empty() => Ok((n.to_string(), v.to_string())),
            _ => Err(ClientError::BadCaveat(a.clone())),
        })
        .collect()
}

/// Resolve `--request` (inline JSON, `@file`, or `-` for stdin) and
/// merge it under the client-owned `ts`/`role`/`ttl_seconds` (those are
/// the conventional fields the client sets and signs; a value supplied
/// for them in `--request` is overwritten, not trusted). Pure +
/// ts-injected for testability. mint is body-field-agnostic — every
/// other `request.*` field is opaque pass-through.
fn build_request_body(
    request_src: Option<&str>,
    role: &str,
    ttl_seconds: u64,
    ts: u64,
) -> Result<String, ClientError> {
    let mut obj = match request_src {
        None => serde_json::Map::new(),
        Some(src) => {
            let raw = if src == "-" {
                let mut s = String::new();
                io::stdin().read_to_string(&mut s)?;
                s
            } else if let Some(path) = src.strip_prefix('@') {
                fs::read_to_string(path)?
            } else {
                src.to_string()
            };
            match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(serde_json::Value::Object(m)) => m,
                Ok(_) => return Err(ClientError::BadRequest("not an object")),
                Err(_) => return Err(ClientError::BadRequest("invalid JSON")),
            }
        }
    };
    obj.insert("ts".into(), ts.into());
    obj.insert("role".into(), role.into());
    obj.insert("ttl_seconds".into(), ttl_seconds.into());
    Ok(serde_json::Value::Object(obj).to_string())
}

/// `mint client assume-role`: attenuate the held credential (the
/// bounding `exp` from `ttl`, plus any caller-supplied narrowing
/// caveats), exercise it. Returns the raw keypair JSON to print.
pub async fn assume_role(
    dir: &Path,
    base_url: &str,
    role: &str,
    request_src: Option<&str>,
    caveats: &[String],
    ttl_seconds: u64,
    in_file: &str,
) -> Result<String, ClientError> {
    let caveats = parse_caveats(caveats)?;
    let seed = load_seed(dir)?;
    let in_path = dir.join(in_file);
    let mut mac = Macaroon::decode(
        read_text(
            &in_path,
            "run `mint client exchange` after the operator approves",
        )?
        .trim(),
    )
    .map_err(|_| ClientError::BadFile("credential"))?;
    eprintln!(
        "assume-role: attenuating your credential ({}) for role `{role}`",
        in_path.display()
    );
    describe("credential (what you hold)", &mac);
    // The credential does not expire; the role gate requires `exp`.
    // Bound it to the requested lifetime, then apply caller narrowing
    // caveats.
    let exp = now_unix().saturating_add(ttl_seconds);
    mac = mac.attenuate(Caveat::scalar(name::EXP, exp.to_string()));
    for (n, v) in &caveats {
        mac = mac.attenuate(Caveat::scalar(n.as_str(), v.as_str()));
    }
    eprintln!(
        "  appended exp={exp} + {} narrowing caveat(s); → POST {base_url}/v1/assume-role",
        caveats.len()
    );
    let body = build_request_body(request_src, role, ttl_seconds, now_unix())?;
    let (status, text) = post(&format!("{base_url}/v1/assume-role"), &mac, &seed, body).await?;
    if status != 200 {
        return Err(ClientError::Server { status, body: text });
    }
    eprintln!("  ← 200 — mint verified the chain + PoP and minted a scoped Tigris keypair:");
    Ok(text)
}

/// First value of caveat `name` in `m`, if present.
fn caveat_value<'a>(m: &'a Macaroon, name: &str) -> Option<&'a str> {
    m.caveats()
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.value.as_str())
}

/// Decode the held credential for `role` from `credentials/<role>`.
fn load_credential(dir: &Path, role: &str) -> Result<Macaroon, ClientError> {
    let path = dir.join(credential_path(role));
    let raw = read_text(&path, "run `mint client exchange --role <role>` first")?;
    Macaroon::decode(raw.trim()).map_err(|_| ClientError::BadFile("credential"))
}

/// `mint client credential list`: enumerate the per-role credentials
/// held under `credentials/`. Local-only — no network, no PoP.
pub fn credential_list(dir: &Path) -> Result<(), ClientError> {
    let cdir = dir.join(CREDENTIALS_DIR);
    let mut held: Vec<(String, Macaroon)> = match fs::read_dir(&cdir) {
        Ok(rd) => {
            let mut v = Vec::new();
            for entry in rd {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let role = entry.file_name().to_string_lossy().into_owned();
                let raw = fs::read_to_string(entry.path())?;
                let mac =
                    Macaroon::decode(raw.trim()).map_err(|_| ClientError::BadFile("credential"))?;
                v.push((role, mac));
            }
            v
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(ClientError::Io(e)),
    };
    if held.is_empty() {
        eprintln!(
            "no credentials held in {} — run `mint client exchange --role <role>`",
            cdir.display()
        );
        return Ok(());
    }
    held.sort_by(|a, b| a.0.cmp(&b.0));
    println!("{:<16} {:<28} {:>7}  SUB", "ROLE", "ROLE-CAVEAT", "CAVEATS");
    for (file_role, mac) in &held {
        // The filename is authoritative for *which* credential this is;
        // the `role` caveat is what the credential actually carries. A
        // mismatch is worth seeing, so show both rather than collapsing.
        let role_cav = caveat_value(mac, name::ROLE).unwrap_or("(none)");
        let sub = caveat_value(mac, name::SUB).unwrap_or("(none)");
        println!(
            "{:<16} {:<28} {:>7}  {sub}",
            file_role,
            role_cav,
            mac.caveats().len()
        );
    }
    Ok(())
}

/// `mint client credential inspect <role>`: narrate the held
/// credential's caveat chain (the same rendering `exchange` prints when
/// it receives it). Local-only — no network, no PoP.
pub fn credential_inspect(dir: &Path, role: &str) -> Result<(), ClientError> {
    let mac = load_credential(dir, role)?;
    eprintln!(
        "credential for role `{role}` ({}):",
        dir.join(credential_path(role)).display()
    );
    describe("credential (what you hold)", &mac);
    Ok(())
}

/// Convenience for callers that take a `--client-dir`.
pub fn client_dir(arg: Option<PathBuf>) -> PathBuf {
    arg.unwrap_or_else(|| PathBuf::from("mint_client"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keygen_writes_pair_and_is_no_clobber() {
        let d = tempfile::tempdir().unwrap();
        let (cnf, fp) = keygen(d.path(), false).unwrap();
        assert!(cnf.starts_with("ed25519:"));
        assert_eq!(fp.len(), 16); // 8 bytes hex
        assert!(d.path().join(KEY_FILE).exists());
        assert!(d.path().join(PUB_FILE).exists());
        // identity() round-trips the persisted key to the same cnf.
        assert_eq!(identity(d.path()).unwrap().0, cnf);
        // no-clobber unless forced
        assert!(matches!(
            keygen(d.path(), false),
            Err(ClientError::KeyExists(_))
        ));
        let (cnf2, _) = keygen(d.path(), true).unwrap();
        assert_ne!(cnf, cnf2, "force regenerates a distinct identity");
    }

    #[test]
    fn key_file_is_0600_hex_with_newline() {
        let d = tempfile::tempdir().unwrap();
        keygen(d.path(), false).unwrap();
        let raw = fs::read_to_string(d.path().join(KEY_FILE)).unwrap();
        assert!(raw.ends_with('\n'));
        assert_eq!(raw.trim().len(), 64);
        let mode = fs::metadata(d.path().join(KEY_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn bad_key_file_is_reported() {
        let d = tempfile::tempdir().unwrap();
        fs::create_dir_all(d.path()).unwrap();
        fs::write(d.path().join(KEY_FILE), "not-hex").unwrap();
        assert!(matches!(identity(d.path()), Err(ClientError::BadFile(_))));
    }

    #[test]
    fn bootstrap_arg_accepts_inline_or_file_and_rejects_neither() {
        let wire = crate::issuance::mint_bootstrap(&[1u8; 32], "mint", "nonce").encode();
        // inline: the value itself is the macaroon
        assert!(read_macaroon_arg(&wire).is_ok());
        // file path containing it
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("boot.txt");
        fs::write(&p, format!("{wire}\n")).unwrap();
        assert!(read_macaroon_arg(p.to_str().unwrap()).is_ok());
        // neither a macaroon nor a readable file → clear error, no panic
        assert!(matches!(
            read_macaroon_arg("/no/such/path-and-not-a-macaroon"),
            Err(ClientError::BadFile(_))
        ));
    }

    #[test]
    fn request_body_is_opaque_passthrough_with_client_owned_fields() {
        // No --request: just the client-owned conventional fields.
        let b = build_request_body(None, "read", 900, 1000).unwrap();
        let v: serde_json::Value = serde_json::from_str(&b).unwrap();
        assert_eq!(v["ts"], 1000);
        assert_eq!(v["role"], "read");
        assert_eq!(v["ttl_seconds"], 900);

        // Arbitrary fields (incl. an array) pass through verbatim;
        // client-owned keys win over anything the caller put there.
        let b = build_request_body(
            Some(r#"{"prefix":"demo/x","ancestors":["a","b"],"role":"EVIL","ts":1}"#),
            "read",
            900,
            1000,
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&b).unwrap();
        assert_eq!(v["prefix"], "demo/x");
        assert_eq!(v["ancestors"], serde_json::json!(["a", "b"]));
        assert_eq!(v["role"], "read", "client-owned, not caller's EVIL");
        assert_eq!(v["ts"], 1000, "client-owned, not caller's 1");

        // Non-object / invalid JSON fails closed.
        assert!(matches!(
            build_request_body(Some("[1,2]"), "r", 1, 1),
            Err(ClientError::BadRequest(_))
        ));
        assert!(matches!(
            build_request_body(Some("{not json"), "r", 1, 1),
            Err(ClientError::BadRequest(_))
        ));
    }

    #[test]
    fn caveat_parsing_is_vocabulary_agnostic() {
        let ok = parse_caveats(&[
            "elide:Volume=01VOL".into(),
            "Region=eu=west".into(), // only the first '=' splits
        ])
        .unwrap();
        assert_eq!(ok[0], ("elide:Volume".into(), "01VOL".into()));
        assert_eq!(ok[1], ("Region".into(), "eu=west".into()));
        for bad in ["novalue", "=novalue"] {
            assert!(matches!(
                parse_caveats(&[bad.to_string()]),
                Err(ClientError::BadCaveat(_))
            ));
        }
    }

    #[test]
    fn credential_list_and_inspect_are_local_and_fail_actionably() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path();

        // No credentials/ dir yet: list is a clean no-op (not an error),
        // inspect of an absent role points at the prerequisite command.
        assert!(credential_list(dir).is_ok());
        assert!(matches!(
            credential_inspect(dir, "write"),
            Err(ClientError::Missing { .. })
        ));

        // Persist a real minted credential at credentials/write.
        let cred =
            crate::issuance::mint_credential(&[7u8; 32], "mint", "coord-1", "ed25519:k", "write");
        save_macaroon(dir, &credential_path("write"), &cred.encode()).unwrap();
        assert!(credential_list(dir).is_ok());
        assert!(credential_inspect(dir, "write").is_ok());

        // A corrupt credential file is reported, not panicked on, by both.
        save_macaroon(dir, &credential_path("read"), &cred.encode()).unwrap();
        fs::write(dir.join(credential_path("read")), "not-a-macaroon").unwrap();
        assert!(matches!(
            credential_list(dir),
            Err(ClientError::BadFile("credential"))
        ));
        assert!(matches!(
            credential_inspect(dir, "read"),
            Err(ClientError::BadFile("credential"))
        ));
    }
}
