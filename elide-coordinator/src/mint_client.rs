//! mint credential-service client (`docs/design-mint.md`
//! § "Coordinator configuration").
//!
//! The coordinator holds a per-role capability macaroon under
//! `<data_dir>/credentials/<role>` (provisioned by enrollment — out of
//! scope here) and exercises it against mint's `assume-role`. Per
//! request it attenuates the stored macaroon with the bounding `exp`
//! and the per-volume `elide:Volume` caveat, proves possession with an
//! Ed25519 signature by `coordinator.key` over
//! `BLAKE3(macaroon-tail ‖ BLAKE3(request-body))`, and POSTs it.
//!
//! The macaroon wire format and PoP construction are reimplemented
//! here against the spec rather than shared: mint is a standalone
//! workspace with no `elide-*` dependency and Elide cannot depend on
//! it, the same deliberate duplication `mint/src/tigris.rs` makes.
//! The coordinator never mints or verifies — it only decodes,
//! attenuates (trailing-MAC only), and re-encodes — so the mint root
//! key never enters this path.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use tracing::{debug, warn};
use ulid::Ulid;

use elide_coordinator::config::MintConfig;
use elide_coordinator::identity::CoordinatorIdentity;

use crate::credential::{CredentialIssuer, Credentialer, IssuedCredentials};

const MAGIC: &[u8; 5] = b"mcrn1";
const NONCE_LEN: usize = 16;

const ROLE_VOLUME_RO: &str = "volume-ro";
const CAVEAT_EXP: &str = "exp";
const CAVEAT_VOLUME: &str = "elide:Volume";

/// Lifetime requested for a `volume-ro` credential. Matches the
/// documented `volume-ro` role default/max (`docs/design-mint.md`
/// § "Elide as customer": 30 days) and the prior in-process iam
/// default, so the `exp` we attenuate to never exceeds the role bound.
const VOLUME_RO_TTL_SECS: u64 = 30 * 24 * 60 * 60;

fn now_unix() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|e| io::Error::other(format!("system clock before unix epoch: {e}")))
}

/// One scalar `(name, value)` caveat. Mirrors `mint/src/caveat.rs`.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Caveat {
    name: String,
    value: String,
}

/// Canonical per-caveat encoding fed into the MAC chain; the same
/// bytes appear on the wire so a decoded macaroon re-MACs identically
/// (`mint/src/macaroon.rs`).
fn serialize_one(name: &str, value: &str) -> Vec<u8> {
    let n = name.as_bytes();
    let v = value.as_bytes();
    let mut out = Vec::with_capacity(n.len() + v.len() + 8);
    out.extend_from_slice(&(n.len() as u32).to_be_bytes());
    out.extend_from_slice(n);
    out.extend_from_slice(&(v.len() as u32).to_be_bytes());
    out.extend_from_slice(v);
    out
}

/// A decoded mint macaroon. The coordinator only ever decodes one mint
/// gave it, appends narrowing caveats, and re-encodes — it has no root
/// key, so it neither mints nor verifies.
struct WireMacaroon {
    nonce: [u8; NONCE_LEN],
    caveats: Vec<Caveat>,
    mac: [u8; 32],
}

impl WireMacaroon {
    fn decode(s: &str) -> io::Result<Self> {
        let buf = BASE64
            .decode(s.trim())
            .map_err(|_| io::Error::other("credential macaroon: base64 decode failed"))?;
        let mut pos = 0usize;
        let mut take = |n: usize| -> io::Result<&[u8]> {
            let end = pos
                .checked_add(n)
                .ok_or_else(|| io::Error::other("credential macaroon: truncated"))?;
            let slice = buf
                .get(pos..end)
                .ok_or_else(|| io::Error::other("credential macaroon: truncated"))?;
            pos = end;
            Ok(slice)
        };
        if take(MAGIC.len())? != MAGIC {
            return Err(io::Error::other("credential macaroon: bad magic"));
        }
        let nonce: [u8; NONCE_LEN] = take(NONCE_LEN)?
            .try_into()
            .map_err(|_| io::Error::other("credential macaroon: truncated"))?;
        let mac: [u8; 32] = take(32)?
            .try_into()
            .map_err(|_| io::Error::other("credential macaroon: truncated"))?;
        let count = u16::from_be_bytes(
            take(2)?
                .try_into()
                .map_err(|_| io::Error::other("credential macaroon: truncated"))?,
        );
        let mut caveats = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let name = read_string(&buf, &mut pos)?;
            let value = read_string(&buf, &mut pos)?;
            caveats.push(Caveat { name, value });
        }
        if pos != buf.len() {
            return Err(io::Error::other("credential macaroon: trailing bytes"));
        }
        Ok(Self {
            nonce,
            caveats,
            mac,
        })
    }

    /// Append `(name, value)`, extending the chain with only the
    /// trailing MAC. Caveats are AND-evaluated, so this can only
    /// restrict authority — the additive-restriction property that
    /// lets a non-root holder attenuate.
    fn attenuate(&mut self, name: &str, value: &str) {
        let step = serialize_one(name, value);
        self.mac = *blake3::keyed_hash(&self.mac, &step).as_bytes();
        self.caveats.push(Caveat {
            name: name.to_owned(),
            value: value.to_owned(),
        });
    }

    fn tail(&self) -> &[u8; 32] {
        &self.mac
    }

    fn encode(&self) -> String {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&self.nonce);
        buf.extend_from_slice(&self.mac);
        buf.extend_from_slice(&(self.caveats.len() as u16).to_be_bytes());
        for c in &self.caveats {
            buf.extend_from_slice(&serialize_one(&c.name, &c.value));
        }
        BASE64.encode(buf)
    }
}

fn read_string(buf: &[u8], pos: &mut usize) -> io::Result<String> {
    let lead = pos
        .checked_add(4)
        .ok_or_else(|| io::Error::other("credential macaroon: truncated"))?;
    let len_bytes: [u8; 4] = buf
        .get(*pos..lead)
        .ok_or_else(|| io::Error::other("credential macaroon: truncated"))?
        .try_into()
        .map_err(|_| io::Error::other("credential macaroon: truncated"))?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let end = lead
        .checked_add(len)
        .ok_or_else(|| io::Error::other("credential macaroon: truncated"))?;
    let bytes = buf
        .get(lead..end)
        .ok_or_else(|| io::Error::other("credential macaroon: truncated"))?;
    *pos = end;
    String::from_utf8(bytes.to_vec())
        .map_err(|_| io::Error::other("credential macaroon: caveat not utf-8"))
}

/// `BLAKE3(tail ‖ BLAKE3(body))` — the digest the PoP signature
/// covers. Body is hashed as the exact bytes sent (`mint/src/pop.rs`).
fn pop_digest(tail: &[u8; 32], body: &[u8]) -> [u8; 32] {
    let body_hash = blake3::hash(body);
    let mut h = blake3::Hasher::new();
    h.update(tail);
    h.update(body_hash.as_bytes());
    *h.finalize().as_bytes()
}

/// `unix:<path>` selects the UDS leg; anything else is the TCP base
/// URL. Scheme validation already happened in `MintConfig::validate`.
fn uds_socket(url: &str) -> Option<&str> {
    url.trim().strip_prefix("unix:")
}

async fn post(
    cfg_url: &str,
    connect_timeout: Duration,
    request_timeout: Duration,
    endpoint: &str,
    auth: &str,
    sig: &str,
    body: String,
) -> io::Result<(u16, String)> {
    match uds_socket(cfg_url) {
        Some(socket) => post_uds(socket, request_timeout, endpoint, auth, sig, body).await,
        None => {
            post_tcp(
                cfg_url,
                connect_timeout,
                request_timeout,
                endpoint,
                auth,
                sig,
                body,
            )
            .await
        }
    }
}

async fn post_tcp(
    base: &str,
    connect_timeout: Duration,
    request_timeout: Duration,
    endpoint: &str,
    auth: &str,
    sig: &str,
    body: String,
) -> io::Result<(u16, String)> {
    let client = reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .build()
        .map_err(|e| io::Error::other(format!("building mint http client: {e}")))?;
    let resp = client
        .post(format!("{}{endpoint}", base.trim_end_matches('/')))
        .header("authorization", auth)
        .header("x-mint-coord-pop", sig)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| io::Error::other(format!("mint request failed: {e}")))?;
    let status = resp.status().as_u16();
    let text = resp
        .text()
        .await
        .map_err(|e| io::Error::other(format!("reading mint response: {e}")))?;
    Ok((status, text))
}

/// HTTP-over-UDS leg — `reqwest` has no UDS support, so this drops to
/// `hyper` dialed through `hyperlocal`, the same split mint's
/// reference client makes (`docs/design-mint.md` § "Transport").
async fn post_uds(
    socket: &str,
    request_timeout: Duration,
    endpoint: &str,
    auth: &str,
    sig: &str,
    body: String,
) -> io::Result<(u16, String)> {
    use http_body_util::{BodyExt, Full};
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client: Client<_, Full<bytes::Bytes>> =
        Client::builder(TokioExecutor::new()).build(hyperlocal::UnixConnector);
    let uri: hyper::Uri = hyperlocal::Uri::new(socket, endpoint).into();
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header("authorization", auth)
        .header("x-mint-coord-pop", sig)
        .header("content-type", "application/json")
        .body(Full::new(bytes::Bytes::from(body)))
        .map_err(|e| io::Error::other(format!("building mint uds request: {e}")))?;
    let resp = tokio::time::timeout(request_timeout, client.request(req))
        .await
        .map_err(|_| io::Error::other("mint uds request timed out"))?
        .map_err(|e| io::Error::other(format!("mint uds request failed: {e}")))?;
    let status = resp.status().as_u16();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| io::Error::other(format!("reading mint uds response: {e}")))?
        .to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

fn json_str_field(body: &str, key: &str) -> io::Result<String> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get(key).and_then(|s| s.as_str()).map(str::to_owned))
        .ok_or_else(|| io::Error::other(format!("mint response missing `{key}` field")))
}

/// A configured mint endpoint plus the coordinator identity that
/// proves possession. Shared by every role the coordinator assumes —
/// `volume-ro` (vended to volumes, [`MintCredentialer`]) and the
/// `coord-*` roles (held by the coordinator, `crate::mint_stores`).
#[derive(Clone)]
pub struct MintEndpoint {
    url: String,
    connect_timeout: Duration,
    request_timeout: Duration,
    data_dir: PathBuf,
    identity: Arc<CoordinatorIdentity>,
}

impl MintEndpoint {
    pub fn new(cfg: &MintConfig, data_dir: PathBuf, identity: Arc<CoordinatorIdentity>) -> Self {
        Self {
            url: cfg.url.clone(),
            connect_timeout: cfg.connect_timeout,
            request_timeout: cfg.request_timeout,
            data_dir,
            identity,
        }
    }

    /// Load `credentials/<role>`, bound it to `ttl_secs` (`exp`
    /// caveat) plus any role-specific narrowing caveats, exercise it
    /// at `/v1/assume-role` with the `coordinator.key` PoP, and return
    /// the vended Tigris keypair. `extra_body` carries role-specific
    /// PoP-signed request fields (e.g. `volume-ro`'s `ancestors`).
    pub async fn assume_role(
        &self,
        role: &str,
        ttl_secs: u64,
        narrowing: &[(&str, &str)],
        extra_body: &[(&str, serde_json::Value)],
    ) -> io::Result<IssuedCredentials> {
        let cred_path = self.data_dir.join("credentials").join(role);
        let stored = std::fs::read_to_string(&cred_path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "reading {role} credential at {}: {e} (run enrollment for this role)",
                    cred_path.display()
                ),
            )
        })?;
        let mut mac = WireMacaroon::decode(&stored)?;

        let now = now_unix()?;
        let exp = now.saturating_add(ttl_secs);
        // The credential does not expire; the role gate requires `exp`.
        // Bound it, then apply any role-specific narrowing.
        mac.attenuate(CAVEAT_EXP, &exp.to_string());
        for (n, v) in narrowing {
            mac.attenuate(n, v);
        }

        // Build the exact body bytes once: they are both signed (via
        // BLAKE3(body)) and sent. Mint hashes the raw bytes before
        // parsing, so no canonicalization step may sit between.
        let mut obj = serde_json::Map::new();
        obj.insert("ts".into(), now.into());
        obj.insert("role".into(), role.into());
        obj.insert("ttl_seconds".into(), ttl_secs.into());
        for (k, v) in extra_body {
            obj.insert((*k).to_owned(), v.clone());
        }
        let body = serde_json::Value::Object(obj).to_string();

        let sig = BASE64.encode(self.identity.sign(&pop_digest(mac.tail(), body.as_bytes())));
        let auth = format!("Macaroon {}", mac.encode());

        let (status, text) = post(
            &self.url,
            self.connect_timeout,
            self.request_timeout,
            "/v1/assume-role",
            &auth,
            &sig,
            body,
        )
        .await?;

        if status != 200 {
            // mint's error model is deliberately coarse (401/400/503);
            // surface status + a short body for the operator log.
            let snippet: String = text.chars().take(200).collect();
            return Err(io::Error::other(format!(
                "mint assume-role for {role} returned {status}: {snippet}"
            )));
        }

        let access_key_id = json_str_field(&text, "access_key_id")?;
        let secret_access_key = json_str_field(&text, "secret_access_key")?;
        // mint may clamp to the role max; its `expiration` is
        // authoritative. If it is unparseable, the `exp` we attenuated
        // to is a valid upper bound — fall back to it rather than fail
        // a credential mint that otherwise succeeded.
        let expiry_unix = json_str_field(&text, "expiration")
            .ok()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s.trim()).ok())
            .map(|dt| dt.timestamp().max(0) as u64)
            .unwrap_or_else(|| {
                warn!("[coordinator] mint {role} expiration unparseable; using attenuated exp");
                exp
            });

        Ok(IssuedCredentials {
            access_key_id,
            secret_access_key,
            session_token: None,
            expiry_unix: Some(expiry_unix),
        })
    }
}

/// Per-volume RO credentialer backed by the external mint service.
/// Sees only ULIDs — ancestor resolution happens upstream in
/// [`MintCredentialIssuer`], the same split the in-process iam path
/// makes, so the seam is transport-agnostic.
pub struct MintCredentialer {
    endpoint: MintEndpoint,
}

impl MintCredentialer {
    pub fn new(cfg: &MintConfig, data_dir: PathBuf, identity: Arc<CoordinatorIdentity>) -> Self {
        Self {
            endpoint: MintEndpoint::new(cfg, data_dir, identity),
        }
    }
}

#[async_trait]
impl Credentialer for MintCredentialer {
    async fn provision_volume_ro(
        &self,
        vol_ulid: Ulid,
        ancestors: &[Ulid],
    ) -> io::Result<IssuedCredentials> {
        let ancestor_strs: Vec<String> = ancestors.iter().map(Ulid::to_string).collect();
        self.endpoint
            .assume_role(
                ROLE_VOLUME_RO,
                VOLUME_RO_TTL_SECS,
                &[(CAVEAT_VOLUME, &vol_ulid.to_string())],
                &[("ancestors", serde_json::json!(ancestor_strs))],
            )
            .await
    }

    async fn release_volume_ro(&self, vol_ulid: Ulid) {
        // Nothing to release. mint vends self-expiring ephemeral
        // keypairs; the IAM key lifecycle is mint's concern and the
        // coordinator holds no server-side handle to tear down. (The
        // in-process iam path deleted the key+policy because it minted
        // them itself — mint does not delegate that.)
        debug!("[coordinator] mint volume-ro release for {vol_ulid}: no-op (self-expiring)");
    }
}

/// `CredentialIssuer` wrapper that resolves the volume's ancestor
/// chain from local provenance and delegates to a [`Credentialer`].
/// Mirrors the in-process iam issuer so the inbound `credentials`
/// handshake is identical regardless of backend.
pub struct MintCredentialIssuer {
    credentialer: Arc<dyn Credentialer>,
    data_dir: PathBuf,
}

impl MintCredentialIssuer {
    pub fn new(credentialer: Arc<dyn Credentialer>, data_dir: PathBuf) -> Self {
        Self {
            credentialer,
            data_dir,
        }
    }
}

#[async_trait]
impl CredentialIssuer for MintCredentialIssuer {
    async fn issue(&self, volume_id: &str) -> io::Result<IssuedCredentials> {
        let vol_ulid = Ulid::from_string(volume_id)
            .map_err(|e| io::Error::other(format!("invalid vol_ulid: {e}")))?;
        let by_id_dir = self.data_dir.join("by_id");
        let fork_dir = by_id_dir.join(vol_ulid.to_string());
        let ancestors = elide_core::volume::lineage_ulids(&fork_dir, &by_id_dir)
            .map_err(|e| io::Error::other(format!("loading ancestor chain: {e}")))?;
        self.credentialer
            .provision_volume_ro(vol_ulid, &ancestors)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a wire macaroon the way `mint/src/macaroon.rs` does, so
    /// the coordinator's decode/attenuate/encode is checked against the
    /// real construction without depending on the mint crate.
    fn mint_like(root: &[u8; 32], nonce: [u8; NONCE_LEN], caveats: &[(&str, &str)]) -> String {
        const DOMAIN: &[u8] = b"mint-macaroon-v1";
        let mut seed_msg = Vec::new();
        seed_msg.extend_from_slice(DOMAIN);
        seed_msg.extend_from_slice(&nonce);
        let mut key = *blake3::keyed_hash(root, &seed_msg).as_bytes();
        for (n, v) in caveats {
            key = *blake3::keyed_hash(&key, &serialize_one(n, v)).as_bytes();
        }
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&nonce);
        buf.extend_from_slice(&key);
        buf.extend_from_slice(&(caveats.len() as u16).to_be_bytes());
        for (n, v) in caveats {
            buf.extend_from_slice(&serialize_one(n, v));
        }
        BASE64.encode(buf)
    }

    #[test]
    fn decode_then_encode_roundtrips() {
        let wire = mint_like(
            &[7u8; 32],
            [3u8; NONCE_LEN],
            &[("aud", "mint"), ("role", "volume-ro")],
        );
        let m = WireMacaroon::decode(&wire).expect("decode");
        assert_eq!(m.caveats.len(), 2);
        assert_eq!(m.encode(), wire);
    }

    #[test]
    fn attenuate_extends_chain_like_mint() {
        let root = [9u8; 32];
        let nonce = [1u8; NONCE_LEN];
        let base = mint_like(&root, nonce, &[("aud", "mint")]);
        let mut m = WireMacaroon::decode(&base).expect("decode");
        m.attenuate(CAVEAT_EXP, "1700000000");

        // The attenuated wire must equal a mint-side macaroon minted
        // with the same caveat appended — proves the trailing-MAC
        // extension is byte-identical.
        let expected = mint_like(&root, nonce, &[("aud", "mint"), ("exp", "1700000000")]);
        assert_eq!(m.encode(), expected);
        assert_eq!(m.caveats.len(), 2);
    }

    #[test]
    fn pop_digest_is_tail_then_body_hash() {
        let tail = [4u8; 32];
        let body = br#"{"ts":1,"role":"volume-ro"}"#;
        let bh = blake3::hash(body);
        let mut h = blake3::Hasher::new();
        h.update(&tail);
        h.update(bh.as_bytes());
        assert_eq!(pop_digest(&tail, body), *h.finalize().as_bytes());
    }

    #[test]
    fn decode_rejects_garbage_without_panicking() {
        assert!(WireMacaroon::decode("not base64!!!").is_err());
        assert!(WireMacaroon::decode(&BASE64.encode([0u8; 3])).is_err());
    }

    #[test]
    fn uds_scheme_detected() {
        assert_eq!(
            uds_socket("unix:mint/mint_data/mint.sock"),
            Some("mint/mint_data/mint.sock")
        );
        assert_eq!(uds_socket("https://mint.host:8085"), None);
    }
}
