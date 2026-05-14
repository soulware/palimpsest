// Typed macaroons for coordinator-issued credentials.
//
// Each token carries a 16-byte random nonce minted alongside the
// caveats. The nonce is mixed into the MAC seed so two tokens minted
// with identical caveats still have distinct MACs, and gives the token
// a stable identity for audit logging.
//
//     mac_seed = blake3_keyed(root_key, DOMAIN || nonce)
//     mac_i    = blake3_keyed(mac_{i-1}, serialize_one(c_i))
//
// The stored MAC is the final mac. Because each step's key is the
// previous step's MAC, any holder of the trailing MAC can append a
// caveat without knowing the root key — this is the "additive
// restriction" property. Removing a caveat is infeasible (would require
// inverting a keyed-BLAKE3 step). The DOMAIN tag also acts as a domain
// separator across uses of the coordinator's keyed-BLAKE3 surface.
//
// Verification is stateless: the coordinator replays the chain from the
// root key and constant-time-compares the final MAC.
//
// Wire format (a single hex line, fits the existing IPC line protocol):
//     v2.<16-byte nonce, hex>.<32-byte mac, hex>.<caveats blob, hex>
//
// Caveats blob (binary, hex-encoded for transport):
//     u8: count
//     repeated:
//       u8 tag
//       Volume   (tag 0): u8 len, N UTF-8 bytes
//       Scope    (tag 1): u8 (0 = credentials, 1 = fetch worker)
//       Pid      (tag 2): i32 BE
//       NotAfter (tag 3): u64 BE  (unix seconds)
//       Role     (tag 4): u8 (0 = operator)
//       Op       (tag 5): u8 (0 = remove)
//
// `Role` and `Op` are operator-token caveats; volume macaroons never
// carry them. See `docs/design-auth-model.md`.

use std::io;

use rand_core::{OsRng, RngCore};
use subtle::ConstantTimeEq;

const MAGIC: &str = "v2";
const DOMAIN: &[u8] = b"elide-macaroon-v2";
pub const NONCE_LEN: usize = 16;
const TAG_VOLUME: u8 = 0;
const TAG_SCOPE: u8 = 1;
const TAG_PID: u8 = 2;
const TAG_NOT_AFTER: u8 = 3;
const TAG_ROLE: u8 = 4;
const TAG_OP: u8 = 5;

const SCOPE_CREDENTIALS: u8 = 0;
const SCOPE_FETCH_WORKER: u8 = 1;

const ROLE_OPERATOR: u8 = 0;

const OP_REMOVE: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Issued to a registered volume daemon. Backs the
    /// `Request::Credentials` IPC for demand-fetch creds.
    Credentials,
    /// Issued to a coordinator-spawned `elide fetch-volume` worker.
    /// PID-bound to the worker via a `fetch.pid` file (not
    /// `volume.pid`, which is reserved for the volume daemon).
    /// Otherwise indistinguishable from a `Credentials`-scoped
    /// macaroon at the IPC layer — both grant short-lived S3 creds
    /// for the same volume — but separating the scope keeps a
    /// leaked fetch macaroon from being usable as if it were a
    /// volume-daemon credential.
    FetchWorker,
}

impl Scope {
    fn to_byte(self) -> u8 {
        match self {
            Self::Credentials => SCOPE_CREDENTIALS,
            Self::FetchWorker => SCOPE_FETCH_WORKER,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            SCOPE_CREDENTIALS => Some(Self::Credentials),
            SCOPE_FETCH_WORKER => Some(Self::FetchWorker),
            _ => None,
        }
    }
}

/// Distinguishes operator-issued tokens (human CLI users) from
/// volume-process tokens. Volume tokens carry [`Scope`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Operator macaroon: not PID-bound, gates destructive coordinator
    /// verbs. Minted by [`mint_operator`], attenuated per use by the
    /// CLI. See `docs/design-auth-model.md`.
    Operator,
}

impl Role {
    fn to_byte(self) -> u8 {
        match self {
            Self::Operator => ROLE_OPERATOR,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            ROLE_OPERATOR => Some(Self::Operator),
            _ => None,
        }
    }
}

/// Coordinator operations gated by an operator token. Exhaustive: the
/// dispatcher hands [`verify_operator`] the variant for the verb it is
/// about to execute, and the verifier requires the chain to carry the
/// matching [`Caveat::Op`]. New gated verbs slot in as new variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorOp {
    Remove,
}

impl OperatorOp {
    fn to_byte(self) -> u8 {
        match self {
            Self::Remove => OP_REMOVE,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            OP_REMOVE => Some(Self::Remove),
            _ => None,
        }
    }

    /// Lowercase verb name for logs and CLI integration.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Remove => "remove",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caveat {
    Volume(String),
    Scope(Scope),
    Pid(i32),
    NotAfter(u64),
    Role(Role),
    Op(OperatorOp),
}

#[derive(Debug, Clone)]
pub struct Macaroon {
    nonce: [u8; NONCE_LEN],
    caveats: Vec<Caveat>,
    mac: [u8; 32],
}

impl Macaroon {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn caveats(&self) -> &[Caveat] {
        &self.caveats
    }

    /// Hex form of the per-token random nonce. Stable identifier for
    /// audit logs; correlates the mint event with later use of the
    /// same token.
    pub fn nonce_hex(&self) -> String {
        encode_hex(&self.nonce)
    }

    pub fn volume(&self) -> Option<&str> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::Volume(v) => Some(v.as_str()),
            _ => None,
        })
    }

    pub fn scope(&self) -> Option<Scope> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::Scope(s) => Some(*s),
            _ => None,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn pid(&self) -> Option<i32> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::Pid(p) => Some(*p),
            _ => None,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn not_after(&self) -> Option<u64> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::NotAfter(t) => Some(*t),
            _ => None,
        })
    }

    /// First [`Caveat::Role`] in the chain, if any. Operator tokens
    /// always carry `Role::Operator`; volume tokens carry none.
    pub fn role(&self) -> Option<Role> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::Role(r) => Some(*r),
            _ => None,
        })
    }

    /// First [`Caveat::Op`] in the chain, if any. The CLI appends this
    /// at use time per the design in `docs/design-auth-model.md`.
    pub fn op(&self) -> Option<OperatorOp> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::Op(o) => Some(*o),
            _ => None,
        })
    }

    /// Smallest `NotAfter` in the chain, if any. AND-of-predicates
    /// evaluation means the narrowest bound always binds — useful for
    /// surfacing "when does this token effectively expire" in audit
    /// or diagnostic output. The per-caveat enforcement in
    /// [`check_caveats`] / [`check_operator_caveats`] already rejects
    /// any expired link, so this is purely a reporting helper.
    pub fn narrowest_not_after(&self) -> Option<u64> {
        self.caveats.iter().fold(None, |acc, c| match c {
            Caveat::NotAfter(t) => Some(acc.map_or(*t, |e: u64| e.min(*t))),
            _ => acc,
        })
    }

    pub fn encode(&self) -> String {
        let blob = serialize_caveats(&self.caveats);
        format!(
            "{MAGIC}.{}.{}.{}",
            encode_hex(&self.nonce),
            encode_hex(&self.mac),
            encode_hex(&blob),
        )
    }

    pub fn parse(s: &str) -> io::Result<Self> {
        let mut parts = s.splitn(4, '.');
        let magic = parts
            .next()
            .ok_or_else(|| io::Error::other("malformed macaroon"))?;
        if magic != MAGIC {
            return Err(io::Error::other(format!(
                "unsupported macaroon version: {magic}"
            )));
        }
        let nonce_hex = parts
            .next()
            .ok_or_else(|| io::Error::other("malformed macaroon"))?;
        let mac_hex = parts
            .next()
            .ok_or_else(|| io::Error::other("malformed macaroon"))?;
        let cav_hex = parts
            .next()
            .ok_or_else(|| io::Error::other("malformed macaroon"))?;
        let nonce = decode_hex_fixed::<NONCE_LEN>(nonce_hex)?;
        let mac = decode_hex_fixed::<32>(mac_hex)?;
        let blob = decode_hex(cav_hex)?;
        let caveats = deserialize_caveats(&blob)?;
        Ok(Self {
            nonce,
            caveats,
            mac,
        })
    }
}

/// Mint a macaroon. A 16-byte random nonce is generated, mixed into the
/// MAC seed alongside `root_key`, then each caveat is chained on by
/// keyed-BLAKE3 with the previous step's MAC as the next key.
pub fn mint(root_key: &[u8; 32], caveats: Vec<Caveat>) -> Macaroon {
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    let mac = chain_mac(root_key, &nonce, &caveats);
    Macaroon {
        nonce,
        caveats,
        mac,
    }
}

/// Constant-time MAC verification. The caller is still responsible for
/// checking individual caveat values against runtime context — see
/// [`check_caveats`].
pub fn verify(root_key: &[u8; 32], m: &Macaroon) -> bool {
    let expected = chain_mac(root_key, &m.nonce, &m.caveats);
    expected.ct_eq(&m.mac).into()
}

fn chain_mac(root_key: &[u8; 32], nonce: &[u8; NONCE_LEN], caveats: &[Caveat]) -> [u8; 32] {
    let mut seed_msg = Vec::with_capacity(DOMAIN.len() + NONCE_LEN);
    seed_msg.extend_from_slice(DOMAIN);
    seed_msg.extend_from_slice(nonce);
    let mut key = *blake3::keyed_hash(root_key, &seed_msg).as_bytes();
    for c in caveats {
        let step = serialize_one(c);
        key = *blake3::keyed_hash(&key, &step).as_bytes();
    }
    key
}

impl Macaroon {
    /// Attenuate by appending `c` to the caveat chain. Does not require
    /// the root key — only the holder's trailing MAC, which is enough to
    /// extend the chain. The resulting macaroon verifies against the
    /// same root key.
    ///
    /// Caveats are evaluated as an AND of predicates by the verifier, so
    /// adding a caveat can only restrict the token's authority.
    pub fn attenuate(mut self, c: Caveat) -> Macaroon {
        let step = serialize_one(&c);
        let new_mac = blake3::keyed_hash(&self.mac, &step);
        self.caveats.push(c);
        self.mac = *new_mac.as_bytes();
        self
    }
}

/// Runtime context for caveat evaluation. The caller fills in whatever
/// it knows about the request; each field is checked against every
/// caveat of the matching kind (AND semantics, so attenuation can only
/// restrict authority).
pub struct VerifyCtx<'a> {
    pub volume: &'a str,
    pub peer_pid: i32,
    pub now_unix: u64,
    pub accepted_scopes: &'a [Scope],
}

/// Mint an operator macaroon. The root token is coordinator-wide: no
/// `Volume` or `Op` caveats. The CLI narrows per use by appending
/// `Op(<verb>)`, `Volume(<target>)`, and a short `NotAfter` before
/// sending the token to the coordinator.
///
/// `expires_unix` is required (no indefinite operator tokens). The
/// per-token nonce that gives the token its audit identity is the
/// struct-level nonce minted inside [`mint`] — operator tokens do not
/// carry a separate nonce caveat. See `docs/design-auth-model.md`.
pub fn mint_operator(root_key: &[u8; 32], expires_unix: u64) -> Macaroon {
    mint(
        root_key,
        vec![Caveat::Role(Role::Operator), Caveat::NotAfter(expires_unix)],
    )
}

/// Runtime context for operator-token caveat evaluation. Operator
/// tokens carry no `Pid` or `Scope`, so they need a parallel context
/// to [`VerifyCtx`].
pub struct VerifyOperatorCtx<'a> {
    pub now_unix: u64,
    pub op: OperatorOp,
    pub op_volume: &'a str,
}

/// Reasons an operator token may be rejected. Coarse on the wire (the
/// IPC `Err` body collapses to a single string), but the typed enum is
/// kept locally so the audit log can record the specific reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorReject {
    Malformed,
    BadMac,
    WrongRole,
    Expired,
    WrongOp,
    VolumeMismatch,
    MissingVolume,
    MissingOp,
}

/// Evaluate every caveat against `ctx`. Returns `Ok(())` if all
/// predicates hold, otherwise the matching [`OperatorReject`]. Does
/// NOT verify the MAC — call [`verify`] first.
///
/// Operator tokens are an AND of: `Role::Operator`, every `NotAfter`
/// in the chain in the future, every `Op` equal to `ctx.op`, every
/// `Volume` equal to `ctx.op_volume`. The root token alone is
/// insufficient — `MissingVolume` / `MissingOp` reject any chain that
/// did not get narrowed at the CLI.
pub fn check_operator_caveats(
    m: &Macaroon,
    ctx: &VerifyOperatorCtx<'_>,
) -> Result<(), OperatorReject> {
    let mut saw_role = false;
    let mut saw_volume = false;
    let mut saw_op = false;
    for c in &m.caveats {
        match c {
            Caveat::Role(Role::Operator) => saw_role = true,
            Caveat::NotAfter(t) => {
                if ctx.now_unix >= *t {
                    return Err(OperatorReject::Expired);
                }
            }
            Caveat::Op(o) => {
                saw_op = true;
                if *o != ctx.op {
                    return Err(OperatorReject::WrongOp);
                }
            }
            Caveat::Volume(v) => {
                saw_volume = true;
                if v != ctx.op_volume {
                    return Err(OperatorReject::VolumeMismatch);
                }
            }
            // Volume-macaroon caveats have no place on an operator
            // token — fail closed.
            Caveat::Scope(_) | Caveat::Pid(_) => return Err(OperatorReject::WrongRole),
        }
    }
    if !saw_role {
        return Err(OperatorReject::WrongRole);
    }
    if !saw_op {
        return Err(OperatorReject::MissingOp);
    }
    if !saw_volume {
        return Err(OperatorReject::MissingVolume);
    }
    Ok(())
}

/// Top-level operator-token check. Parses the wire form, verifies the
/// MAC chain against `root_key`, then evaluates every caveat against
/// `ctx`. Returns the parsed macaroon on success (the caller uses its
/// `nonce_hex` for the audit log).
pub fn verify_operator(
    root_key: &[u8; 32],
    encoded: &str,
    ctx: &VerifyOperatorCtx<'_>,
) -> Result<Macaroon, OperatorReject> {
    let m = Macaroon::parse(encoded).map_err(|_| OperatorReject::Malformed)?;
    if !verify(root_key, &m) {
        return Err(OperatorReject::BadMac);
    }
    check_operator_caveats(&m, ctx)?;
    Ok(m)
}

/// Evaluate every caveat against `ctx`. Returns the first failure as a
/// short reason string, or `Ok(())` if all predicates hold. Does NOT
/// verify the MAC — call [`verify`] first.
pub fn check_caveats(m: &Macaroon, ctx: &VerifyCtx<'_>) -> Result<(), &'static str> {
    let mut saw_volume = false;
    let mut saw_scope = false;
    let mut saw_pid = false;
    for c in &m.caveats {
        match c {
            Caveat::Volume(v) => {
                saw_volume = true;
                if v != ctx.volume {
                    return Err("volume caveat does not match request");
                }
            }
            Caveat::Scope(s) => {
                saw_scope = true;
                if !ctx.accepted_scopes.contains(s) {
                    return Err("macaroon scope mismatch");
                }
            }
            Caveat::Pid(p) => {
                saw_pid = true;
                if *p != ctx.peer_pid {
                    return Err("peer pid does not match macaroon");
                }
            }
            Caveat::NotAfter(t) => {
                if ctx.now_unix >= *t {
                    return Err("macaroon expired");
                }
            }
            // Role/Op caveats belong to operator tokens; a volume
            // macaroon presented through this verifier is malformed
            // if it carries either.
            Caveat::Role(_) => return Err("role caveat not valid for volume macaroon"),
            Caveat::Op(_) => return Err("op caveat not valid for volume macaroon"),
        }
    }
    if !saw_volume {
        return Err("missing volume caveat");
    }
    if !saw_scope {
        return Err("missing scope caveat");
    }
    if !saw_pid {
        return Err("missing pid caveat");
    }
    Ok(())
}

fn serialize_caveats(caveats: &[Caveat]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(caveats.len() as u8);
    for c in caveats {
        write_one(c, &mut out);
    }
    out
}

fn serialize_one(c: &Caveat) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    write_one(c, &mut out);
    out
}

fn write_one(c: &Caveat, out: &mut Vec<u8>) {
    match c {
        Caveat::Volume(v) => {
            out.push(TAG_VOLUME);
            let bytes = v.as_bytes();
            debug_assert!(bytes.len() <= u8::MAX as usize);
            out.push(bytes.len() as u8);
            out.extend_from_slice(bytes);
        }
        Caveat::Scope(s) => {
            out.push(TAG_SCOPE);
            out.push(s.to_byte());
        }
        Caveat::Pid(p) => {
            out.push(TAG_PID);
            out.extend_from_slice(&p.to_be_bytes());
        }
        Caveat::NotAfter(t) => {
            out.push(TAG_NOT_AFTER);
            out.extend_from_slice(&t.to_be_bytes());
        }
        Caveat::Role(r) => {
            out.push(TAG_ROLE);
            out.push(r.to_byte());
        }
        Caveat::Op(o) => {
            out.push(TAG_OP);
            out.push(o.to_byte());
        }
    }
}

fn deserialize_caveats(blob: &[u8]) -> io::Result<Vec<Caveat>> {
    let mut cur = blob;
    let count = read_u8(&mut cur)?;
    let mut caveats = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let tag = read_u8(&mut cur)?;
        let c = match tag {
            TAG_VOLUME => {
                let len = read_u8(&mut cur)? as usize;
                let bytes = read_n(&mut cur, len)?;
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| io::Error::other("non-utf8 volume caveat"))?;
                Caveat::Volume(s.to_owned())
            }
            TAG_SCOPE => {
                let b = read_u8(&mut cur)?;
                Caveat::Scope(
                    Scope::from_byte(b)
                        .ok_or_else(|| io::Error::other(format!("unknown scope: {b}")))?,
                )
            }
            TAG_PID => {
                let bytes = read_n(&mut cur, 4)?;
                let mut a = [0u8; 4];
                a.copy_from_slice(bytes);
                Caveat::Pid(i32::from_be_bytes(a))
            }
            TAG_NOT_AFTER => {
                let bytes = read_n(&mut cur, 8)?;
                let mut a = [0u8; 8];
                a.copy_from_slice(bytes);
                Caveat::NotAfter(u64::from_be_bytes(a))
            }
            TAG_ROLE => {
                let b = read_u8(&mut cur)?;
                Caveat::Role(
                    Role::from_byte(b)
                        .ok_or_else(|| io::Error::other(format!("unknown role: {b}")))?,
                )
            }
            TAG_OP => {
                let b = read_u8(&mut cur)?;
                Caveat::Op(
                    OperatorOp::from_byte(b)
                        .ok_or_else(|| io::Error::other(format!("unknown op: {b}")))?,
                )
            }
            _ => return Err(io::Error::other(format!("unknown caveat tag: {tag}"))),
        };
        caveats.push(c);
    }
    if !cur.is_empty() {
        return Err(io::Error::other("trailing bytes in caveat blob"));
    }
    Ok(caveats)
}

fn read_u8(cur: &mut &[u8]) -> io::Result<u8> {
    if cur.is_empty() {
        return Err(io::Error::other("unexpected eof in caveat blob"));
    }
    let b = cur[0];
    *cur = &cur[1..];
    Ok(b)
}

fn read_n<'a>(cur: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if cur.len() < n {
        return Err(io::Error::other("unexpected eof in caveat blob"));
    }
    let r = &cur[..n];
    *cur = &cur[n..];
    Ok(r)
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_hex(s: &str) -> io::Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(io::Error::other("hex string has odd length"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| io::Error::other(format!("invalid hex at position {i}")))
        })
        .collect()
}

fn decode_hex_fixed<const N: usize>(s: &str) -> io::Result<[u8; N]> {
    let v = decode_hex(s)?;
    v.try_into()
        .map_err(|v: Vec<u8>| io::Error::other(format!("expected {N} bytes, got {}", v.len())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn sample_caveats() -> Vec<Caveat> {
        vec![
            Caveat::Volume("01JQAAAAAAAAAAAAAAAAAAAAAA".to_owned()),
            Caveat::Scope(Scope::Credentials),
            Caveat::Pid(12345),
        ]
    }

    #[test]
    fn mint_then_verify_roundtrip() {
        let m = mint(&key(), sample_caveats());
        assert!(verify(&key(), &m));
    }

    #[test]
    fn encode_then_parse_roundtrip() {
        let m = mint(&key(), sample_caveats());
        let s = m.encode();
        let parsed = Macaroon::parse(&s).unwrap();
        assert_eq!(parsed.nonce, m.nonce);
        assert_eq!(parsed.caveats, m.caveats);
        assert_eq!(parsed.mac, m.mac);
        assert!(verify(&key(), &parsed));
    }

    #[test]
    fn two_mints_with_identical_caveats_differ() {
        // The per-token random nonce is the whole point — even with
        // identical caveat sets, two mints must produce distinct MACs
        // and distinct nonces, so each token has its own audit identity.
        let a = mint(&key(), sample_caveats());
        let b = mint(&key(), sample_caveats());
        assert_ne!(a.nonce, b.nonce);
        assert_ne!(a.mac, b.mac);
        assert_eq!(a.nonce_hex().len(), NONCE_LEN * 2);
    }

    #[test]
    fn accessors_extract_caveat_values() {
        let m = mint(&key(), sample_caveats());
        assert_eq!(m.volume(), Some("01JQAAAAAAAAAAAAAAAAAAAAAA"));
        assert_eq!(m.scope(), Some(Scope::Credentials));
        assert_eq!(m.pid(), Some(12345));
        assert_eq!(m.not_after(), None);
    }

    #[test]
    fn tampered_mac_fails_verify() {
        let m = mint(&key(), sample_caveats());
        let mut s = m.encode();
        // Wire format: `v2.<nonce>.<mac>.<blob>` — flip a byte inside
        // the MAC section (between the 2nd and 3rd dot).
        let nonce_dot = s.find('.').unwrap();
        let mac_dot = nonce_dot + 1 + s[nonce_dot + 1..].find('.').unwrap();
        let pos = mac_dot + 2;
        let bytes = unsafe { s.as_bytes_mut() };
        bytes[pos] = if bytes[pos] == b'a' { b'b' } else { b'a' };
        let parsed = Macaroon::parse(&s).unwrap();
        assert!(!verify(&key(), &parsed));
    }

    #[test]
    fn tampered_caveat_fails_verify() {
        let m = mint(&key(), sample_caveats());
        let mut new_caveats = m.caveats().to_vec();
        // Mutate the pid — verify must reject because the MAC was over the
        // original caveat chain.
        for c in &mut new_caveats {
            if let Caveat::Pid(p) = c {
                *p = 99999;
            }
        }
        let forged = Macaroon {
            nonce: m.nonce,
            caveats: new_caveats,
            mac: m.mac,
        };
        assert!(!verify(&key(), &forged));
    }

    #[test]
    fn wrong_root_key_fails_verify() {
        let m = mint(&key(), sample_caveats());
        let mut other = key();
        other[0] ^= 0xFF;
        assert!(!verify(&other, &m));
    }

    #[test]
    fn parse_rejects_unknown_version() {
        let err = Macaroon::parse("v9.0011.00").unwrap_err();
        assert!(err.to_string().contains("version"));
    }

    #[test]
    fn parse_rejects_truncated_blob() {
        // Valid hex nonce + MAC, blob claims one Volume caveat but no length byte.
        let nonce = "00".repeat(NONCE_LEN);
        let mac = "00".repeat(32);
        let blob_hex = encode_hex(&[1u8, TAG_VOLUME]);
        let s = format!("v2.{nonce}.{mac}.{blob_hex}");
        assert!(Macaroon::parse(&s).is_err());
    }

    #[test]
    fn not_after_caveat_roundtrips() {
        let caveats = vec![
            Caveat::Volume("vol".to_owned()),
            Caveat::Scope(Scope::Credentials),
            Caveat::Pid(1),
            Caveat::NotAfter(1_700_000_000),
        ];
        let m = mint(&key(), caveats);
        let s = m.encode();
        let parsed = Macaroon::parse(&s).unwrap();
        assert_eq!(parsed.not_after(), Some(1_700_000_000));
        assert!(verify(&key(), &parsed));
    }

    fn ctx<'a>(volume: &'a str, pid: i32, now: u64) -> VerifyCtx<'a> {
        VerifyCtx {
            volume,
            peer_pid: pid,
            now_unix: now,
            accepted_scopes: &[Scope::Credentials, Scope::FetchWorker],
        }
    }

    #[test]
    fn check_caveats_accepts_well_formed_token() {
        let m = mint(&key(), sample_caveats());
        check_caveats(&m, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 12345, 1_000)).unwrap();
    }

    #[test]
    fn check_caveats_requires_each_kind() {
        // Missing volume.
        let m = mint(
            &key(),
            vec![Caveat::Scope(Scope::Credentials), Caveat::Pid(1)],
        );
        assert!(check_caveats(&m, &ctx("v", 1, 0)).is_err());
        // Missing scope.
        let m = mint(&key(), vec![Caveat::Volume("v".into()), Caveat::Pid(1)]);
        assert!(check_caveats(&m, &ctx("v", 1, 0)).is_err());
        // Missing pid.
        let m = mint(
            &key(),
            vec![
                Caveat::Volume("v".into()),
                Caveat::Scope(Scope::Credentials),
            ],
        );
        assert!(check_caveats(&m, &ctx("v", 1, 0)).is_err());
    }

    #[test]
    fn check_caveats_rejects_mismatched_pid_and_volume() {
        let m = mint(&key(), sample_caveats());
        assert!(check_caveats(&m, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 99999, 1_000)).is_err());
        assert!(check_caveats(&m, &ctx("different-vol", 12345, 1_000)).is_err());
    }

    #[test]
    fn check_caveats_rejects_expired() {
        let mut caveats = sample_caveats();
        caveats.push(Caveat::NotAfter(500));
        let m = mint(&key(), caveats);
        assert!(check_caveats(&m, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 12345, 1_000)).is_err());
        check_caveats(&m, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 12345, 100)).unwrap();
    }

    #[test]
    fn attenuate_extends_chain_and_verifies() {
        let m = mint(&key(), sample_caveats());
        let attenuated = m.clone().attenuate(Caveat::NotAfter(2_000));
        // MAC changed.
        assert_ne!(attenuated.mac, m.mac);
        // Still verifies against the same root key — the chain replays
        // correctly because the volume only knew the trailing MAC.
        assert!(verify(&key(), &attenuated));
        // And the attenuation is enforced.
        check_caveats(&attenuated, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 12345, 500)).unwrap();
        assert!(
            check_caveats(
                &attenuated,
                &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 12345, 2_001)
            )
            .is_err()
        );
    }

    #[test]
    fn attenuation_cannot_relax_existing_caveat() {
        // Original token expires at t=1000. Attacker tries to "attenuate"
        // by appending a *later* NotAfter to weaken the bound. The new
        // caveat passes the MAC chain (it really was appended), but the
        // AND-of-predicates verifier still enforces the original 1000.
        let mut caveats = sample_caveats();
        caveats.push(Caveat::NotAfter(1_000));
        let m = mint(&key(), caveats);
        let widened = m.attenuate(Caveat::NotAfter(10_000));
        assert!(verify(&key(), &widened));
        assert!(check_caveats(&widened, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 12345, 1_500)).is_err());
    }

    #[test]
    fn attenuation_cannot_change_volume_or_pid() {
        // Adding a contradicting Volume or Pid leaves the token unusable
        // by either side — the AND-check fails for any runtime context.
        let m = mint(&key(), sample_caveats());
        let pivoted = m.clone().attenuate(Caveat::Volume("other-vol".into()));
        assert!(verify(&key(), &pivoted));
        assert!(check_caveats(&pivoted, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 12345, 0)).is_err());
        assert!(check_caveats(&pivoted, &ctx("other-vol", 12345, 0)).is_err());

        let repidded = m.attenuate(Caveat::Pid(99));
        assert!(verify(&key(), &repidded));
        assert!(check_caveats(&repidded, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 12345, 0)).is_err());
        assert!(check_caveats(&repidded, &ctx("01JQAAAAAAAAAAAAAAAAAAAAAA", 99, 0)).is_err());
    }

    #[test]
    fn caveat_reordering_changes_mac() {
        // Chained MAC binds caveat order — reordering produces a
        // different MAC, so a hostile attempt to swap caveats post-mint
        // would fail verification.
        let a = mint(
            &key(),
            vec![
                Caveat::Volume("v".into()),
                Caveat::Scope(Scope::Credentials),
                Caveat::Pid(1),
            ],
        );
        let b = mint(
            &key(),
            vec![
                Caveat::Pid(1),
                Caveat::Scope(Scope::Credentials),
                Caveat::Volume("v".into()),
            ],
        );
        assert_ne!(a.mac, b.mac);
    }

    // ── operator tokens ───────────────────────────────────────────────

    const T_NOW: u64 = 1_750_000_000;
    const ROOT_EXPIRES: u64 = T_NOW + 30 * 86_400;
    const USE_EXPIRES: u64 = T_NOW + 60;

    fn op_ctx<'a>(op: OperatorOp, op_volume: &'a str, now: u64) -> VerifyOperatorCtx<'a> {
        VerifyOperatorCtx {
            now_unix: now,
            op,
            op_volume,
        }
    }

    fn attenuated_operator_token(
        expiry_root: u64,
        op: OperatorOp,
        vol: &str,
        expiry_use: u64,
    ) -> String {
        mint_operator(&key(), expiry_root)
            .attenuate(Caveat::Op(op))
            .attenuate(Caveat::Volume(vol.to_owned()))
            .attenuate(Caveat::NotAfter(expiry_use))
            .encode()
    }

    #[test]
    fn mint_operator_carries_role_and_expiry() {
        let m = mint_operator(&key(), ROOT_EXPIRES);
        assert_eq!(m.role(), Some(Role::Operator));
        assert_eq!(m.not_after(), Some(ROOT_EXPIRES));
        assert!(m.volume().is_none());
        assert!(m.op().is_none());
        assert!(verify(&key(), &m));
    }

    #[test]
    fn mint_operator_produces_unique_struct_nonces() {
        // The per-token random nonce on the struct gives each operator
        // token its own audit identity even if minted with identical
        // expiry. This is the only place "two operator tokens with the
        // same expires_unix" produces distinct identities; the rebase
        // dropped the caveat-level nonce so this property must hold
        // via the struct nonce alone.
        let a = mint_operator(&key(), ROOT_EXPIRES);
        let b = mint_operator(&key(), ROOT_EXPIRES);
        assert_ne!(a.nonce_hex(), b.nonce_hex());
    }

    #[test]
    fn verify_operator_happy_path_after_cli_attenuation() {
        let t = attenuated_operator_token(ROOT_EXPIRES, OperatorOp::Remove, "myvm", USE_EXPIRES);
        let m = verify_operator(&key(), &t, &op_ctx(OperatorOp::Remove, "myvm", T_NOW)).unwrap();
        assert_eq!(m.op(), Some(OperatorOp::Remove));
        assert_eq!(m.volume(), Some("myvm"));
    }

    #[test]
    fn verify_operator_rejects_wire_token_use_expired() {
        let t = attenuated_operator_token(ROOT_EXPIRES, OperatorOp::Remove, "myvm", T_NOW - 1);
        assert_eq!(
            verify_operator(&key(), &t, &op_ctx(OperatorOp::Remove, "myvm", T_NOW)).unwrap_err(),
            OperatorReject::Expired,
        );
    }

    #[test]
    fn verify_operator_rejects_when_root_expired_even_if_use_window_open() {
        // Root expiry is in the past; attenuation cannot widen authority.
        let t = attenuated_operator_token(T_NOW - 1, OperatorOp::Remove, "myvm", USE_EXPIRES);
        assert_eq!(
            verify_operator(&key(), &t, &op_ctx(OperatorOp::Remove, "myvm", T_NOW)).unwrap_err(),
            OperatorReject::Expired,
        );
    }

    #[test]
    fn verify_operator_rejects_bad_mac() {
        let mut t =
            attenuated_operator_token(ROOT_EXPIRES, OperatorOp::Remove, "myvm", USE_EXPIRES);
        // Flip a byte inside the MAC region: format is
        // `v2.<nonce>.<mac>.<blob>`. The MAC section starts after the
        // second dot.
        let nonce_dot = t.find('.').unwrap();
        let mac_dot = nonce_dot + 1 + t[nonce_dot + 1..].find('.').unwrap();
        let pos = mac_dot + 2;
        let bytes = unsafe { t.as_bytes_mut() };
        bytes[pos] = if bytes[pos] == b'a' { b'b' } else { b'a' };
        assert_eq!(
            verify_operator(&key(), &t, &op_ctx(OperatorOp::Remove, "myvm", T_NOW)).unwrap_err(),
            OperatorReject::BadMac,
        );
    }

    #[test]
    fn verify_operator_rejects_volume_macaroon_presented_as_operator() {
        // Volume-scoped token must never authenticate operator verbs.
        // The exact OperatorReject variant depends on chain order: the
        // matching-volume case fails at the Scope caveat (WrongRole);
        // the mismatching-volume case fails at the Volume caveat
        // (VolumeMismatch). Both reject, which is the property we need.
        let mismatching = mint(&key(), sample_caveats());
        assert_eq!(
            verify_operator(
                &key(),
                &mismatching.encode(),
                &op_ctx(OperatorOp::Remove, "myvm", T_NOW),
            )
            .unwrap_err(),
            OperatorReject::VolumeMismatch,
        );

        let matching = mint(
            &key(),
            vec![
                Caveat::Volume("myvm".to_owned()),
                Caveat::Scope(Scope::Credentials),
                Caveat::Pid(12345),
            ],
        );
        assert_eq!(
            verify_operator(
                &key(),
                &matching.encode(),
                &op_ctx(OperatorOp::Remove, "myvm", T_NOW),
            )
            .unwrap_err(),
            OperatorReject::WrongRole,
        );
    }

    #[test]
    fn verify_operator_rejects_wrong_op_in_chain() {
        // Wire token attenuated for Remove; verifier asked about a
        // different op. (Today only Remove exists, so the only way to
        // produce WrongOp via this path is via the CLI dispatcher
        // mis-routing — the test guards against that.) For now the
        // failure shape we can exercise is "no Op caveat at all":
        // returns MissingOp.
        let t = mint_operator(&key(), ROOT_EXPIRES)
            .attenuate(Caveat::Volume("myvm".to_owned()))
            .attenuate(Caveat::NotAfter(USE_EXPIRES))
            .encode();
        assert_eq!(
            verify_operator(&key(), &t, &op_ctx(OperatorOp::Remove, "myvm", T_NOW)).unwrap_err(),
            OperatorReject::MissingOp,
        );
    }

    #[test]
    fn verify_operator_rejects_volume_mismatch() {
        let t = attenuated_operator_token(ROOT_EXPIRES, OperatorOp::Remove, "myvm", USE_EXPIRES);
        assert_eq!(
            verify_operator(&key(), &t, &op_ctx(OperatorOp::Remove, "othervm", T_NOW)).unwrap_err(),
            OperatorReject::VolumeMismatch,
        );
    }

    #[test]
    fn verify_operator_rejects_missing_volume() {
        // CLI must always attenuate by volume — a chain without one
        // is rejected.
        let t = mint_operator(&key(), ROOT_EXPIRES)
            .attenuate(Caveat::Op(OperatorOp::Remove))
            .attenuate(Caveat::NotAfter(USE_EXPIRES))
            .encode();
        assert_eq!(
            verify_operator(&key(), &t, &op_ctx(OperatorOp::Remove, "myvm", T_NOW)).unwrap_err(),
            OperatorReject::MissingVolume,
        );
    }

    #[test]
    fn operator_attenuation_cannot_relax_root_expiry() {
        // Append a far-future NotAfter via attenuation. The original
        // tight root NotAfter remains in the chain and still binds
        // because evaluation is AND-of-predicates.
        let m = mint_operator(&key(), T_NOW - 1)
            .attenuate(Caveat::Op(OperatorOp::Remove))
            .attenuate(Caveat::Volume("myvm".to_owned()))
            .attenuate(Caveat::NotAfter(T_NOW + 86_400));
        assert!(verify(&key(), &m));
        assert_eq!(
            check_operator_caveats(&m, &op_ctx(OperatorOp::Remove, "myvm", T_NOW)),
            Err(OperatorReject::Expired),
        );
    }
}
