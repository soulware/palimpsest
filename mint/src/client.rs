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
//! `./mint_data`), overridable with `--client-dir`. The intermediate
//! and primary macaroons received from the server are persisted there
//! too, so the client is self-contained.

use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use ed25519_dalek::SigningKey;
use rand_core::{OsRng, RngCore};

use crate::caveat::{Caveat, name};
use crate::macaroon::Macaroon;
use crate::pop;
use crate::state::fingerprint;

const KEY_FILE: &str = "client.key";
const PUB_FILE: &str = "client.pub";
const INTERMEDIATE_FILE: &str = "intermediate";
const PRIMARY_FILE: &str = "primary";

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
    let raw = fs::read_to_string(dir.join(KEY_FILE))?;
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
    fs::create_dir_all(dir)?;
    fs::write(dir.join(file), b64)?;
    Ok(())
}

/// `mint client enroll`: attenuate the bootstrap macaroon with this
/// identity's `sub`/`cnf`, prove possession, receive + persist the
/// intermediate.
pub async fn enroll(
    dir: &Path,
    base_url: &str,
    bootstrap_src: &str,
    sub: &str,
) -> Result<(), ClientError> {
    let seed = load_seed(dir)?;
    let presented = read_macaroon_arg(bootstrap_src)?
        .attenuate(Caveat::scalar(name::SUB, sub))
        .attenuate(Caveat::scalar(name::CNF, pop::cnf_value(&seed)));
    let body = format!(r#"{{"ts":{}}}"#, now_unix());
    let (status, text) = post(&format!("{base_url}/v1/enroll"), &presented, &seed, body).await?;
    if status != 200 {
        return Err(ClientError::Server { status, body: text });
    }
    save_macaroon(dir, INTERMEDIATE_FILE, &json_field(&text, "intermediate")?)
}

/// `mint client exchange`: present the intermediate. `Ok(true)` =
/// primary written; `Ok(false)` = still awaiting operator approval
/// (HTTP 403, the one non-failure non-200) — the caller decides the
/// exit code / retry.
pub async fn exchange(dir: &Path, base_url: &str) -> Result<bool, ClientError> {
    let seed = load_seed(dir)?;
    let inter = Macaroon::decode(fs::read_to_string(dir.join(INTERMEDIATE_FILE))?.trim())
        .map_err(|_| ClientError::BadFile(INTERMEDIATE_FILE))?;
    let body = format!(r#"{{"ts":{}}}"#, now_unix());
    let (status, text) = post(
        &format!("{base_url}/v1/enroll-exchange"),
        &inter,
        &seed,
        body,
    )
    .await?;
    match status {
        200 => {
            save_macaroon(dir, PRIMARY_FILE, &json_field(&text, "primary")?)?;
            Ok(true)
        }
        403 => Ok(false),
        _ => Err(ClientError::Server { status, body: text }),
    }
}

/// `mint client assume-role`: attenuate the held primary (a bounding
/// `exp`, optional `elide:Volume`), exercise it. Returns the raw
/// keypair JSON to print.
pub async fn assume_role(
    dir: &Path,
    base_url: &str,
    role: &str,
    prefix: Option<&str>,
    volume: Option<&str>,
    ttl_seconds: u64,
) -> Result<String, ClientError> {
    let seed = load_seed(dir)?;
    let mut mac = Macaroon::decode(fs::read_to_string(dir.join(PRIMARY_FILE))?.trim())
        .map_err(|_| ClientError::BadFile(PRIMARY_FILE))?;
    // The primary does not expire; the role gate requires `exp`. Bound
    // it to the requested lifetime.
    let exp = now_unix().saturating_add(ttl_seconds);
    mac = mac.attenuate(Caveat::scalar(name::EXP, exp.to_string()));
    if let Some(v) = volume {
        mac = mac.attenuate(Caveat::scalar("elide:Volume", v));
    }
    let mut obj = serde_json::Map::new();
    obj.insert("ts".into(), now_unix().into());
    obj.insert("role".into(), role.into());
    obj.insert("ttl_seconds".into(), ttl_seconds.into());
    if let Some(p) = prefix {
        obj.insert("prefix".into(), p.into());
    }
    let body = serde_json::Value::Object(obj).to_string();
    let (status, text) = post(&format!("{base_url}/v1/assume-role"), &mac, &seed, body).await?;
    if status != 200 {
        return Err(ClientError::Server { status, body: text });
    }
    Ok(text)
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
}
