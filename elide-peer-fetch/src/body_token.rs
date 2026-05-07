//! `BodyFetchToken` — volume-signed bearer token for the `.body` route.
//!
//! Body bytes are owned by the volume process (not the coordinator), so
//! the body-fetch token is signed with `volume.key` and verified against
//! `by_id/<vol_ulid>/volume.pub`. This is a separate token type from
//! [`crate::PeerFetchToken`] (the coordinator-signed token used by
//! `.idx` / `.prefetch` / `.manifest` / skeleton routes); the two
//! flavours have distinct domain tags, distinct wire encodings, and
//! distinct verify pipelines on the server side.
//!
//! See `docs/notes/design-peer-segment-fetch.md` § "Body fetch (deferred —
//! not v1)" for the v1.1 single-signer rationale.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use ulid::Ulid;

use crate::token::{TokenDecodeError, TokenVerifyError};

/// Domain-separation tag for body-fetch tokens. Distinct from the
/// coordinator-flavour [`crate::token::DOMAIN_TAG`] so a signature
/// minted for one flavour cannot be replayed against the other.
pub const BODY_DOMAIN_TAG: &[u8] = b"elide peer-fetch body v1\0";

/// Bearer token presented by a volume process to fetch body bytes from
/// a peer. Signed by the volume's `volume.key`; the holder of that key
/// is by definition the fork identified by `vol_ulid`.
///
/// The peer verifies by:
///   1. Checking `issued_at` against its own clock.
///   2. Looking up `by_id/<vol_ulid>/volume.pub` and verifying the
///      Ed25519 signature.
///   3. Walking the volume's signed ancestry (rooted at `vol_ulid`)
///      and confirming the URL's `vol_id` is in that set.
///
/// There is no `names/<name>` ownership step: the volume key proves
/// the signer *is* the fork, which is sufficient for read-only access
/// to the fork's own bytes and its lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyFetchToken {
    pub vol_ulid: Ulid,
    pub issued_at: u64,
    pub signature: [u8; 64],
}

impl BodyFetchToken {
    /// Convenience: current unix-seconds wall clock.
    pub fn now_unix_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Bytes signed by the issuing volume. Domain-tag-prefixed binary
    /// form of the named fields:
    ///
    /// ```text
    /// BODY_DOMAIN_TAG
    /// 16 bytes vol_ulid (big-endian, as per Ulid::to_bytes)
    /// u64-le issued_at
    /// ```
    pub fn signing_payload(vol_ulid: Ulid, issued_at: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BODY_DOMAIN_TAG.len() + 16 + 8);
        buf.extend_from_slice(BODY_DOMAIN_TAG);
        buf.extend_from_slice(&vol_ulid.to_bytes());
        buf.extend_from_slice(&issued_at.to_le_bytes());
        buf
    }

    /// Convenience: signing payload for `self`'s fields.
    pub fn signing_payload_self(&self) -> Vec<u8> {
        Self::signing_payload(self.vol_ulid, self.issued_at)
    }

    /// Encode the token in its wire form: vol_ulid bytes, issued_at,
    /// signature, base64-wrapped for HTTP header transport.
    pub fn encode(&self) -> String {
        let mut buf = Vec::with_capacity(16 + 8 + 64);
        buf.extend_from_slice(&self.vol_ulid.to_bytes());
        buf.extend_from_slice(&self.issued_at.to_le_bytes());
        buf.extend_from_slice(&self.signature);
        BASE64.encode(buf)
    }

    /// Decode a wire-form body-fetch token. Reverses [`Self::encode`];
    /// trailing bytes after the expected fields are rejected.
    pub fn decode(s: &str) -> Result<Self, TokenDecodeError> {
        let bytes = BASE64
            .decode(s)
            .map_err(|_| TokenDecodeError::InvalidBase64)?;
        if bytes.len() < 16 + 8 + 64 {
            return Err(TokenDecodeError::Truncated);
        }
        if bytes.len() > 16 + 8 + 64 {
            return Err(TokenDecodeError::TrailingBytes);
        }
        let mut ulid_buf = [0u8; 16];
        ulid_buf.copy_from_slice(&bytes[0..16]);
        let vol_ulid = Ulid::from_bytes(ulid_buf);
        let mut issued_buf = [0u8; 8];
        issued_buf.copy_from_slice(&bytes[16..24]);
        let issued_at = u64::from_le_bytes(issued_buf);
        let mut sig = [0u8; 64];
        sig.copy_from_slice(&bytes[24..88]);
        Ok(Self {
            vol_ulid,
            issued_at,
            signature: sig,
        })
    }

    /// Reject the token if `issued_at` differs from `now` by more than
    /// `window_secs` in either direction.
    pub fn check_freshness(&self, now: u64, window_secs: u64) -> Result<(), TokenVerifyError> {
        let skew = self.issued_at.abs_diff(now);
        if skew > window_secs {
            Err(TokenVerifyError::Stale { skew_secs: skew })
        } else {
            Ok(())
        }
    }

    /// Verify the Ed25519 signature against `key` (the publishing
    /// fork's `volume.pub`).
    pub fn verify_signature(&self, key: &VerifyingKey) -> Result<(), TokenVerifyError> {
        let payload = self.signing_payload_self();
        let sig = Signature::from_bytes(&self.signature);
        key.verify(&payload, &sig)
            .map_err(TokenVerifyError::BadSignature)
    }
}

/// Re-export for callers that want a single import for both token
/// types' freshness window.
pub use crate::token::DEFAULT_FRESHNESS_WINDOW_SECS as DEFAULT_BODY_FRESHNESS_WINDOW_SECS;

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use rand_core::OsRng;

    fn build(vol: Ulid, issued: u64, key: &SigningKey) -> BodyFetchToken {
        let payload = BodyFetchToken::signing_payload(vol, issued);
        let sig = key.sign(&payload).to_bytes();
        BodyFetchToken {
            vol_ulid: vol,
            issued_at: issued,
            signature: sig,
        }
    }

    #[test]
    fn round_trip_encode_decode() {
        let key = SigningKey::generate(&mut OsRng);
        let vol = Ulid::new();
        let tok = build(vol, 1_700_000_000, &key);
        let encoded = tok.encode();
        let decoded = BodyFetchToken::decode(&encoded).expect("decode");
        assert_eq!(decoded, tok);
    }

    #[test]
    fn signature_verifies_against_issuing_key() {
        let key = SigningKey::generate(&mut OsRng);
        let tok = build(Ulid::new(), 1_700_000_000, &key);
        tok.verify_signature(&key.verifying_key())
            .expect("signature verifies");
    }

    #[test]
    fn signature_fails_against_unrelated_key() {
        let k1 = SigningKey::generate(&mut OsRng);
        let k2 = SigningKey::generate(&mut OsRng);
        let tok = build(Ulid::new(), 1_700_000_000, &k1);
        let err = tok
            .verify_signature(&k2.verifying_key())
            .expect_err("unrelated key");
        assert!(matches!(err, TokenVerifyError::BadSignature(_)));
    }

    #[test]
    fn tampered_vol_ulid_invalidates_signature() {
        let key = SigningKey::generate(&mut OsRng);
        let mut tok = build(Ulid::new(), 1_700_000_000, &key);
        tok.vol_ulid = Ulid::new();
        let err = tok
            .verify_signature(&key.verifying_key())
            .expect_err("tampered ulid");
        assert!(matches!(err, TokenVerifyError::BadSignature(_)));
    }

    #[test]
    fn tampered_issued_at_invalidates_signature() {
        let key = SigningKey::generate(&mut OsRng);
        let mut tok = build(Ulid::new(), 1_700_000_000, &key);
        tok.issued_at += 1;
        let err = tok
            .verify_signature(&key.verifying_key())
            .expect_err("tampered issued_at");
        assert!(matches!(err, TokenVerifyError::BadSignature(_)));
    }

    #[test]
    fn freshness_accepts_within_window() {
        let key = SigningKey::generate(&mut OsRng);
        let tok = build(Ulid::new(), 1_700_000_000, &key);
        tok.check_freshness(1_700_000_010, DEFAULT_BODY_FRESHNESS_WINDOW_SECS)
            .expect("within window");
        tok.check_freshness(1_699_999_990, DEFAULT_BODY_FRESHNESS_WINDOW_SECS)
            .expect("past within window");
    }

    #[test]
    fn freshness_rejects_outside_window() {
        let key = SigningKey::generate(&mut OsRng);
        let tok = build(Ulid::new(), 1_700_000_000, &key);
        let err = tok
            .check_freshness(1_700_000_120, 60)
            .expect_err("future drift");
        assert!(matches!(err, TokenVerifyError::Stale { skew_secs } if skew_secs == 120));
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let key = SigningKey::generate(&mut OsRng);
        let tok = build(Ulid::new(), 1_700_000_000, &key);
        let encoded = tok.encode();
        let truncated = &encoded[..encoded.len() - 8];
        let err = BodyFetchToken::decode(truncated).expect_err("truncated");
        assert_eq!(err, TokenDecodeError::Truncated);
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let key = SigningKey::generate(&mut OsRng);
        let tok = build(Ulid::new(), 1_700_000_000, &key);
        let mut bytes = BASE64.decode(tok.encode()).unwrap();
        bytes.push(0xff);
        let encoded = BASE64.encode(&bytes);
        let err = BodyFetchToken::decode(&encoded).expect_err("trailing");
        assert_eq!(err, TokenDecodeError::TrailingBytes);
    }

    #[test]
    fn signing_payload_is_domain_tag_prefixed() {
        let payload = BodyFetchToken::signing_payload(Ulid::nil(), 0);
        assert!(payload.starts_with(BODY_DOMAIN_TAG));
    }

    #[test]
    fn body_domain_tag_differs_from_coordinator_tag() {
        assert_ne!(BODY_DOMAIN_TAG, crate::token::DOMAIN_TAG);
    }
}
