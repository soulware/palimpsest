//! Holder-of-key proof for the `elide:CoordKey` caveat
//! (`docs/design-mint.md` § *Coordinator bootstrap*, § *Authentication*;
//! open question #16).
//!
//! The primary macaroon is **key-bound, not a bearer**: mint honours an
//! `assume-role` request only when it carries a fresh Ed25519 signature,
//! by `coordinator.key`, over
//!
//! ```text
//! BLAKE3( macaroon-tail(32) ‖ BLAKE3(raw-request-body) )
//! ```
//!
//! verified against the `ed25519:<pub>` sealed in `elide:CoordKey`. The
//! tail binds the proof to this exact attenuated macaroon; the body
//! hash binds it to this exact request (the body the policy renders
//! from). Freshness is the `ts` field *inside* the body — already
//! covered by `BLAKE3(body)`, so there is no separate signed term and
//! no `X-Mint-Coord-Pop-Ts` header; within a ±skew window it bounds
//! replay (#16: stateless `iat`-skew, no mint-issued nonce — DPoP's
//! resolved tradeoff; prior art RFC 7800 / RFC 9449). Only the
//! detached signature stays a header (`X-Mint-Coord-Pop`): it cannot
//! live in the body it signs.
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

/// JSON body field carrying the per-request freshness timestamp (unix
/// seconds). It rides *in the body* — not a header — so it is already
/// covered by the PoP signature via `BLAKE3(body)`; no separate signed
/// term, no `X-Mint-Coord-Pop-Ts` header.
pub const TS_FIELD: &str = "ts";

/// The request-side proof: just the Ed25519 signature, from the
/// `X-Mint-Coord-Pop` header. Freshness (`ts`) is a body field, not
/// part of this struct — it is authenticated transitively by the
/// signature over the body.
pub struct Proof {
    sig: [u8; 64],
}

impl Proof {
    /// Parse from `X-Mint-Coord-Pop` (base64 64-byte signature).
    pub fn from_b64(sig_b64: &str) -> Result<Proof, PopReject> {
        let raw = BASE64
            .decode(sig_b64.trim())
            .map_err(|_| PopReject::BadProof)?;
        let sig: [u8; 64] = raw.try_into().map_err(|_| PopReject::BadProof)?;
        Ok(Proof { sig })
    }
}

/// Extract the freshness `ts` from the JSON body. Called **after** the
/// signature verifies, so the value is already authenticated (parsing
/// is not trusting; the trust comes from the verified signature over
/// these exact bytes).
fn body_ts(body: &[u8]) -> Result<u64, PopReject> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .as_ref()
        .and_then(|v| v.get(TS_FIELD))
        .and_then(|v| v.as_u64())
        .ok_or(PopReject::BadProof)
}

fn parse_coord_key(value: &str) -> Result<[u8; 32], PopReject> {
    let b64 = value
        .strip_prefix(ED25519_PREFIX)
        .ok_or(PopReject::BadKey)?;
    let raw = BASE64.decode(b64.trim()).map_err(|_| PopReject::BadKey)?;
    raw.try_into().map_err(|_| PopReject::BadKey)
}

/// The signed digest: `BLAKE3(tail ‖ BLAKE3(body))`. `ts` rides inside
/// `body` as a conventional field, so it is already covered by
/// `BLAKE3(body)` — no separate signed term. The body is hashed as the
/// **exact bytes received**: the caller must pass the raw request body
/// before any parse/canonicalization (a canonical-form mismatch is a
/// signature-bypass footgun, #16).
fn digest(tail: &[u8; 32], body: &[u8]) -> [u8; 32] {
    let body_hash = blake3::hash(body);
    let mut h = blake3::Hasher::new();
    h.update(tail);
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
    let vk = VerifyingKey::from_bytes(&key).map_err(|_| PopReject::BadKey)?;
    let sig = Signature::from_bytes(&proof.sig);
    vk.verify_strict(&digest(tail, body), &sig)
        .map_err(|_| PopReject::BadSignature)?;
    // ts is inside the body the signature just authenticated; read it
    // only now (parsing is not trusting — the trust is the verified
    // signature over these exact bytes).
    let ts = body_ts(body)?;
    if now_unix.abs_diff(ts) > SKEW_SECONDS {
        return Err(PopReject::Stale);
    }
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

/// Reference client signature: sign `digest(tail, body)` with the
/// coordinator key seed, returning the `X-Mint-Coord-Pop` header value.
/// The caller must have already embedded the freshness `ts` field in
/// `body` (it is covered by the signature via `BLAKE3(body)`). This is
/// exactly what a coordinator does per `assume-role`; mint never calls it.
pub fn client_signature(seed: &[u8; 32], tail: &[u8; 32], body: &[u8]) -> String {
    let sig = SigningKey::from_bytes(seed).sign(&digest(tail, body));
    BASE64.encode(sig.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer() -> ([u8; 32], String) {
        let seed = [7u8; 32];
        (seed, coord_key_value(&seed))
    }

    /// A request body carrying the freshness `ts` field plus optional
    /// extra JSON (e.g. `,"role":"x"`).
    fn body(ts: u64, extra: &str) -> Vec<u8> {
        format!("{{\"ts\":{ts}{extra}}}").into_bytes()
    }

    fn proof_for(seed: &[u8; 32], tail: &[u8; 32], body: &[u8]) -> Proof {
        Proof::from_b64(&client_signature(seed, tail, body)).expect("well-formed proof")
    }

    const TAIL: [u8; 32] = [9u8; 32];

    #[test]
    fn absent_coordkey_is_bearer() {
        let cv = vec![Caveat::scalar("Audience", "mint")];
        assert_eq!(
            check(&cv, &TAIL, &body(1000, ""), None, 1000),
            Ok(PopOutcome::NotKeyBound)
        );
    }

    #[test]
    fn valid_proof_verifies() {
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let b = body(1000, ",\"role\":\"x\"");
        let p = proof_for(&sk, &TAIL, &b);
        assert_eq!(
            check(&cv, &TAIL, &b, Some(p), 1000),
            Ok(PopOutcome::Verified)
        );
    }

    #[test]
    fn key_bound_without_proof_is_rejected() {
        let (_, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        assert_eq!(
            check(&cv, &TAIL, &body(1000, ""), None, 1000),
            Err(PopReject::MissingProof)
        );
    }

    #[test]
    fn tampered_body_fails() {
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let p = proof_for(&sk, &TAIL, &body(1000, ",\"ancestors\":[\"A\"]"));
        // Same proof, different body → digest mismatch (verified before
        // ts is even read).
        assert_eq!(
            check(
                &cv,
                &TAIL,
                &body(1000, ",\"ancestors\":[\"EVIL\"]"),
                Some(p),
                1000
            ),
            Err(PopReject::BadSignature)
        );
    }

    #[test]
    fn proof_bound_to_the_macaroon_tail() {
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let b = body(1000, "");
        let p = proof_for(&sk, &TAIL, &b);
        // A proof minted for TAIL must not verify against another tail.
        assert_eq!(
            check(&cv, &[1u8; 32], &b, Some(p), 1000),
            Err(PopReject::BadSignature)
        );
    }

    #[test]
    fn stale_timestamp_rejected() {
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let b = body(1000, "");
        let p = proof_for(&sk, &TAIL, &b);
        // Signature is valid; the in-body ts is outside the skew window.
        assert_eq!(
            check(&cv, &TAIL, &b, Some(p), 1000 + SKEW_SECONDS + 1),
            Err(PopReject::Stale)
        );
    }

    #[test]
    fn missing_ts_in_body_rejected_after_verify() {
        // A correctly-signed body that omits `ts`: signature verifies,
        // but freshness can't be established → reject (not minted).
        let (sk, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let b = br#"{"role":"x"}"#;
        let p = proof_for(&sk, &TAIL, b);
        assert_eq!(
            check(&cv, &TAIL, b, Some(p), 1000),
            Err(PopReject::BadProof)
        );
    }

    #[test]
    fn wrong_key_fails() {
        let (_, key) = signer();
        let cv = vec![Caveat::scalar(COORD_KEY_CAVEAT, key)];
        let b = body(1000, "");
        let p = proof_for(&[3u8; 32], &TAIL, &b);
        assert_eq!(
            check(&cv, &TAIL, &b, Some(p), 1000),
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
            check(&cv, &TAIL, &body(1000, ""), None, 1000),
            Err(PopReject::Unsatisfiable)
        );
    }
}
