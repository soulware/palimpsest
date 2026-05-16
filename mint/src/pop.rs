//! Holder-of-key proof for the `elide:CoordKey` caveat
//! (`docs/design-mint.md` § *Coordinator bootstrap*, § *Authentication*;
//! open question #16).
//!
//! The primary macaroon is **key-bound, not a bearer**: mint honours an
//! `assume-role` request only when it carries a fresh Ed25519 signature,
//! by `coordinator.key`, over
//!
//! ```text
//! BLAKE3( macaroon-tail(32) ‖ ts_be(8) ‖ BLAKE3(raw-request-body) )
//! ```
//!
//! verified against the `ed25519:<pub>` sealed in `elide:CoordKey`. The
//! tail binds the proof to this exact attenuated macaroon; the body
//! hash binds it to this exact request (the body the policy renders
//! from); the timestamp, within a ±skew window, bounds replay (#16:
//! stateless `iat`-skew, no mint-issued nonce — DPoP's resolved
//! tradeoff; prior art RFC 7800 / RFC 9449).
//!
//! Resolution of `elide:CoordKey` goes through the tri-state
//! [`Resolved`]: `Absent` ⇒ the macaroon is a plain bearer (no PoP
//! required — generic non-Elide callers); `Value` ⇒ PoP **required**;
//! `Unsatisfiable` ⇒ **reject**. The last is the downgrade defence: a
//! holder can append caveats with only the trailing MAC, so an
//! appended contradictory `elide:CoordKey` must fail closed here, never
//! resolve to "absent" and silently drop the PoP requirement.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};

use crate::caveat::{Caveat, EffectiveCaveats, Resolved};

pub const COORD_KEY_CAVEAT: &str = "elide:CoordKey";
const ED25519_PREFIX: &str = "ed25519:";

/// Replay window on the proof timestamp (#16). Generous for a
/// prototype; a real deployment tunes this against clock skew.
pub const SKEW_SECONDS: u64 = 60;

/// Outcome of a successful PoP evaluation.
#[derive(Debug, PartialEq, Eq)]
pub enum PopOutcome {
    /// No `elide:CoordKey` — the macaroon is a plain bearer.
    NotKeyBound,
    /// `elide:CoordKey` present and the request proved possession.
    Verified,
}

/// Why a PoP evaluation was refused. The HTTP layer maps **every**
/// variant to an opaque `401` (don't help an attacker distinguish
/// causes — `docs/design-mint.md` § *Authentication*); the variant is
/// for the audit log / tests only.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PopReject {
    #[error("contradictory elide:CoordKey")]
    Unsatisfiable,
    #[error("malformed elide:CoordKey")]
    BadKey,
    #[error("proof header absent")]
    MissingProof,
    #[error("malformed proof")]
    BadProof,
    #[error("proof timestamp outside skew window")]
    Stale,
    #[error("signature verification failed")]
    BadSignature,
}

/// The request-side proof: the Ed25519 signature and the timestamp it
/// was signed at, parsed from the wire headers.
pub struct Proof {
    sig: [u8; 64],
    ts: u64,
}

impl Proof {
    /// Parse from `X-Mint-Coord-Pop` (base64 64-byte signature) and
    /// `X-Mint-Coord-Pop-Ts` (decimal unix seconds).
    pub fn from_parts(sig_b64: &str, ts: &str) -> Result<Proof, PopReject> {
        let raw = BASE64
            .decode(sig_b64.trim())
            .map_err(|_| PopReject::BadProof)?;
        let sig: [u8; 64] = raw.try_into().map_err(|_| PopReject::BadProof)?;
        let ts: u64 = ts.trim().parse().map_err(|_| PopReject::BadProof)?;
        Ok(Proof { sig, ts })
    }
}

fn parse_coord_key(value: &str) -> Result<[u8; 32], PopReject> {
    let b64 = value
        .strip_prefix(ED25519_PREFIX)
        .ok_or(PopReject::BadKey)?;
    let raw = BASE64.decode(b64.trim()).map_err(|_| PopReject::BadKey)?;
    raw.try_into().map_err(|_| PopReject::BadKey)
}

/// The signed digest: `BLAKE3(tail ‖ ts_be ‖ BLAKE3(body))`. The body
/// is hashed as the **exact bytes received** — the caller must pass the
/// raw request body before any parse/canonicalization (a canonical-form
/// mismatch is a signature-bypass footgun, #16).
fn digest(tail: &[u8; 32], ts: u64, body: &[u8]) -> [u8; 32] {
    let body_hash = blake3::hash(body);
    let mut h = blake3::Hasher::new();
    h.update(tail);
    h.update(&ts.to_be_bytes());
    h.update(body_hash.as_bytes());
    *h.finalize().as_bytes()
}

/// Evaluate the holder-of-key requirement for `caveats` against the
/// presented `proof`. `tail` is the presented macaroon's trailing MAC,
/// `body` the exact raw request body, `now_unix` the verifier's clock.
pub fn check(
    caveats: &[Caveat],
    tail: &[u8; 32],
    body: &[u8],
    proof: Option<Proof>,
    now_unix: u64,
) -> Result<PopOutcome, PopReject> {
    let key = match EffectiveCaveats::new(caveats).resolve(COORD_KEY_CAVEAT) {
        Resolved::Absent => return Ok(PopOutcome::NotKeyBound),
        Resolved::Unsatisfiable => return Err(PopReject::Unsatisfiable),
        Resolved::Value(k) => parse_coord_key(&k)?,
    };
    let proof = proof.ok_or(PopReject::MissingProof)?;
    if now_unix.abs_diff(proof.ts) > SKEW_SECONDS {
        return Err(PopReject::Stale);
    }
    let vk = VerifyingKey::from_bytes(&key).map_err(|_| PopReject::BadKey)?;
    let sig = Signature::from_bytes(&proof.sig);
    vk.verify_strict(&digest(tail, proof.ts, body), &sig)
        .map_err(|_| PopReject::BadSignature)?;
    Ok(PopOutcome::Verified)
}

/// The `elide:CoordKey` caveat value for an Ed25519 seed:
/// `ed25519:<base64 pubkey>`. This is the reference for what the
/// issuance path seals into the primary; a coordinator's identity key
/// seed produces the value mint must verify against.
pub fn coord_key_value(seed: &[u8; 32]) -> String {
    let vk = SigningKey::from_bytes(seed).verifying_key();
    format!("{ED25519_PREFIX}{}", BASE64.encode(vk.to_bytes()))
}

/// Reference client proof: sign `digest(tail, ts, body)` with the
/// coordinator key seed, returning the `(X-Mint-Coord-Pop,
/// X-Mint-Coord-Pop-Ts)` header values. This is exactly what a
/// coordinator does per `assume-role`; mint never calls it.
pub fn client_proof(seed: &[u8; 32], tail: &[u8; 32], body: &[u8], ts: u64) -> (String, String) {
    let sig = SigningKey::from_bytes(seed).sign(&digest(tail, ts, body));
    (BASE64.encode(sig.to_bytes()), ts.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer() -> ([u8; 32], String) {
        let seed = [7u8; 32];
        (seed, coord_key_value(&seed))
    }

    fn proof_for(seed: &[u8; 32], tail: &[u8; 32], body: &[u8], ts: u64) -> Proof {
        let (sig_b64, ts_s) = client_proof(seed, tail, body, ts);
        Proof::from_parts(&sig_b64, &ts_s).expect("well-formed proof")
    }

    const TAIL: [u8; 32] = [9u8; 32];

    #[test]
    fn absent_coordkey_is_bearer() {
        let cv = vec![Caveat::scalar("Audience", "mint")];
        assert_eq!(
            check(&cv, &TAIL, b"{}", None, 1000),
            Ok(PopOutcome::NotKeyBound)
        );
    }

    #[test]
    fn valid_proof_verifies() {
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let p = proof_for(&sk, &TAIL, b"{\"role\":\"x\"}", 1000);
        assert_eq!(
            check(&cv, &TAIL, b"{\"role\":\"x\"}", Some(p), 1000),
            Ok(PopOutcome::Verified)
        );
    }

    #[test]
    fn key_bound_without_proof_is_rejected() {
        let (_, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        assert_eq!(
            check(&cv, &TAIL, b"{}", None, 1000),
            Err(PopReject::MissingProof)
        );
    }

    #[test]
    fn tampered_body_fails() {
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let p = proof_for(&sk, &TAIL, b"{\"ancestors\":[\"A\"]}", 1000);
        // Same proof, different body → digest mismatch.
        assert_eq!(
            check(&cv, &TAIL, b"{\"ancestors\":[\"EVIL\"]}", Some(p), 1000),
            Err(PopReject::BadSignature)
        );
    }

    #[test]
    fn proof_bound_to_the_macaroon_tail() {
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let p = proof_for(&sk, &TAIL, b"{}", 1000);
        // A proof minted for TAIL must not verify against another tail.
        let other_tail = [1u8; 32];
        assert_eq!(
            check(&cv, &other_tail, b"{}", Some(p), 1000),
            Err(PopReject::BadSignature)
        );
    }

    #[test]
    fn stale_timestamp_rejected() {
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let p = proof_for(&sk, &TAIL, b"{}", 1000);
        assert_eq!(
            check(&cv, &TAIL, b"{}", Some(p), 1000 + SKEW_SECONDS + 1),
            Err(PopReject::Stale)
        );
    }

    #[test]
    fn wrong_key_fails() {
        let (_, key) = signer();
        let other_seed = [3u8; 32];
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let p = proof_for(&other_seed, &TAIL, b"{}", 1000);
        assert_eq!(
            check(&cv, &TAIL, b"{}", Some(p), 1000),
            Err(PopReject::BadSignature)
        );
    }

    #[test]
    fn contradictory_coordkey_fails_closed_not_bearer() {
        // The downgrade defence: an appended second, different
        // elide:CoordKey must reject — never resolve to Absent and
        // drop the PoP requirement (which would make a captured
        // primary a usable bearer).
        let (_, key) = signer();
        let cv = vec![
            Caveat::scalar(COORD_KEY_CAVEAT, key),
            Caveat::scalar(COORD_KEY_CAVEAT, "ed25519:AAAA"),
        ];
        assert_eq!(
            check(&cv, &TAIL, b"{}", None, 1000),
            Err(PopReject::Unsatisfiable)
        );
    }
}
