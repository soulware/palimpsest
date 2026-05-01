//! `PeerFetchToken` — bearer token used on the wire as
//! `Authorization: Bearer <base64(token)>` for peer-fetch requests.
//!
//! The token's signing payload is a fixed-domain-tag-prefixed,
//! length-prefixed binary form of the named fields. The on-wire form
//! is the same shape minus the domain tag, plus the 64-byte signature,
//! base64-encoded. The verifier reconstructs the signing payload by
//! re-prepending the domain tag.
//!
//! The domain tag (`DOMAIN_TAG`) ensures the same Ed25519 key cannot
//! produce a valid peer-fetch token signature on bytes that happen to
//! match a different artefact's canonical form (segments, event-log
//! entries) — the prepended tag forces the pre-images to differ.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{Signature, SignatureError, Verifier, VerifyingKey};

/// Domain-separation tag mixed into the signing payload. Different
/// from any other Ed25519 use of the coordinator key (segment
/// signing, event-log entries) so signatures cannot be replayed
/// across artefact types.
pub const DOMAIN_TAG: &[u8] = b"elide peer-fetch v1\0";

/// Default freshness window in seconds. A token whose `issued_at`
/// is more than `DEFAULT_FRESHNESS_WINDOW_SECS` away from the
/// verifier's clock is rejected as stale.
pub const DEFAULT_FRESHNESS_WINDOW_SECS: u64 = 60;

/// Bearer token presented by a fetching coordinator to a peer.
///
/// Carries the volume name being fetched on behalf of, the issuing
/// coordinator's id, an `issued_at` timestamp (unix seconds), and an
/// Ed25519 signature over the canonical signing payload.
///
/// The peer verifies by:
///   1. Checking `issued_at` against its own clock.
///   2. Looking up `coordinators/<coordinator_id>/coordinator.pub` and
///      verifying the signature.
///   3. Looking up `names/<volume_name>` and confirming the current
///      claimer's `coordinator_id` matches the token's `coordinator_id`.
///   4. Walking the volume's signed ancestry to build the authorised
///      S3 prefix set, then checking the requested path against it.
///
/// This crate covers steps 1 and 2; steps 3 and 4 are the peer-side
/// auth middleware that lives alongside the HTTP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerFetchToken {
    pub volume_name: String,
    pub coordinator_id: String,
    pub issued_at: u64,
    pub signature: [u8; 64],
}

/// Errors decoding the wire form back into a `PeerFetchToken`.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenDecodeError {
    /// Outer base64 wrapper did not parse.
    InvalidBase64,
    /// A length-prefixed field claimed more bytes than were present, or
    /// the trailing fixed-length fields (`issued_at`, signature) were
    /// short.
    Truncated,
    /// A length-prefixed string field contained non-UTF-8 bytes.
    InvalidUtf8,
    /// Bytes remained after parsing the expected fields.
    TrailingBytes,
}

impl std::fmt::Display for TokenDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBase64 => f.write_str("token base64 decode failed"),
            Self::Truncated => f.write_str("token wire form truncated"),
            Self::InvalidUtf8 => f.write_str("token field is not valid utf-8"),
            Self::TrailingBytes => f.write_str("token wire form has trailing bytes"),
        }
    }
}

impl std::error::Error for TokenDecodeError {}

/// Errors verifying a `PeerFetchToken` against a known verifying key.
#[derive(Debug)]
pub enum TokenVerifyError {
    /// `issued_at` is outside the freshness window — clock skew
    /// exceeds the configured tolerance in either direction.
    Stale {
        /// Magnitude of the skew in seconds (always non-negative).
        skew_secs: u64,
    },
    /// Ed25519 signature failed to verify against the supplied key.
    BadSignature(SignatureError),
}

impl std::fmt::Display for TokenVerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stale { skew_secs } => write!(f, "token clock skew too large: {skew_secs}s"),
            Self::BadSignature(e) => write!(f, "token signature verification failed: {e}"),
        }
    }
}

impl std::error::Error for TokenVerifyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stale { .. } => None,
            Self::BadSignature(e) => Some(e),
        }
    }
}

impl PeerFetchToken {
    /// Construct a token from its named fields plus a precomputed
    /// signature. Callers obtain the signature by signing
    /// [`PeerFetchToken::signing_payload`] with the coordinator's
    /// Ed25519 signing key (typically via `CoordinatorIdentity::sign`).
    pub fn new(
        volume_name: String,
        coordinator_id: String,
        issued_at: u64,
        signature: [u8; 64],
    ) -> Self {
        Self {
            volume_name,
            coordinator_id,
            issued_at,
            signature,
        }
    }

    /// Current unix-seconds wall clock, suitable for `issued_at`.
    /// Returns 0 if the system clock is before the unix epoch — the
    /// token will then fail freshness on any peer with a sane clock,
    /// which is the desired conservative behaviour.
    pub fn now_unix_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Bytes signed by the issuing coordinator. Domain-tag-prefixed
    /// length-prefixed binary form of the named fields:
    ///
    /// ```text
    /// DOMAIN_TAG
    /// u32-le volume_name_len | volume_name_bytes
    /// u32-le coordinator_id_len | coordinator_id_bytes
    /// u64-le issued_at
    /// ```
    pub fn signing_payload(volume_name: &str, coordinator_id: &str, issued_at: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            DOMAIN_TAG.len() + 4 + volume_name.len() + 4 + coordinator_id.len() + 8,
        );
        buf.extend_from_slice(DOMAIN_TAG);
        buf.extend_from_slice(&(volume_name.len() as u32).to_le_bytes());
        buf.extend_from_slice(volume_name.as_bytes());
        buf.extend_from_slice(&(coordinator_id.len() as u32).to_le_bytes());
        buf.extend_from_slice(coordinator_id.as_bytes());
        buf.extend_from_slice(&issued_at.to_le_bytes());
        buf
    }

    /// Convenience: signing payload for `self`'s fields.
    pub fn signing_payload_self(&self) -> Vec<u8> {
        Self::signing_payload(&self.volume_name, &self.coordinator_id, self.issued_at)
    }

    /// Encode the token in its wire form: same length-prefixed shape
    /// as the signing payload (without the domain tag), followed by
    /// the 64-byte signature, base64-wrapped for HTTP header transport.
    pub fn encode(&self) -> String {
        let mut buf =
            Vec::with_capacity(4 + self.volume_name.len() + 4 + self.coordinator_id.len() + 8 + 64);
        buf.extend_from_slice(&(self.volume_name.len() as u32).to_le_bytes());
        buf.extend_from_slice(self.volume_name.as_bytes());
        buf.extend_from_slice(&(self.coordinator_id.len() as u32).to_le_bytes());
        buf.extend_from_slice(self.coordinator_id.as_bytes());
        buf.extend_from_slice(&self.issued_at.to_le_bytes());
        buf.extend_from_slice(&self.signature);
        BASE64.encode(buf)
    }

    /// Decode a wire-form token. Reverses [`PeerFetchToken::encode`];
    /// trailing bytes after the expected fields are rejected.
    pub fn decode(s: &str) -> Result<Self, TokenDecodeError> {
        let bytes = BASE64
            .decode(s)
            .map_err(|_| TokenDecodeError::InvalidBase64)?;
        let mut cur: &[u8] = &bytes;

        let volume_name = read_lp_string(&mut cur)?;
        let coordinator_id = read_lp_string(&mut cur)?;
        let issued_at = read_u64_le(&mut cur)?;
        let signature = read_signature(&mut cur)?;

        if !cur.is_empty() {
            return Err(TokenDecodeError::TrailingBytes);
        }

        Ok(Self {
            volume_name,
            coordinator_id,
            issued_at,
            signature,
        })
    }

    /// Reject the token if `issued_at` differs from `now` by more
    /// than `window_secs` in either direction.
    pub fn check_freshness(&self, now: u64, window_secs: u64) -> Result<(), TokenVerifyError> {
        let skew = self.issued_at.abs_diff(now);
        if skew > window_secs {
            Err(TokenVerifyError::Stale { skew_secs: skew })
        } else {
            Ok(())
        }
    }

    /// Verify the Ed25519 signature against `key`. Does not check
    /// freshness — callers should pair this with
    /// [`check_freshness`] (and any S3-state checks the peer-side
    /// auth flow performs separately).
    pub fn verify_signature(&self, key: &VerifyingKey) -> Result<(), TokenVerifyError> {
        let payload = self.signing_payload_self();
        let sig = Signature::from_bytes(&self.signature);
        key.verify(&payload, &sig)
            .map_err(TokenVerifyError::BadSignature)
    }
}

fn read_lp_string(cur: &mut &[u8]) -> Result<String, TokenDecodeError> {
    let len = read_u32_le(cur)? as usize;
    if cur.len() < len {
        return Err(TokenDecodeError::Truncated);
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    String::from_utf8(head.to_vec()).map_err(|_| TokenDecodeError::InvalidUtf8)
}

fn read_u32_le(cur: &mut &[u8]) -> Result<u32, TokenDecodeError> {
    if cur.len() < 4 {
        return Err(TokenDecodeError::Truncated);
    }
    let (head, tail) = cur.split_at(4);
    *cur = tail;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(head);
    Ok(u32::from_le_bytes(buf))
}

fn read_u64_le(cur: &mut &[u8]) -> Result<u64, TokenDecodeError> {
    if cur.len() < 8 {
        return Err(TokenDecodeError::Truncated);
    }
    let (head, tail) = cur.split_at(8);
    *cur = tail;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(head);
    Ok(u64::from_le_bytes(buf))
}

fn read_signature(cur: &mut &[u8]) -> Result<[u8; 64], TokenDecodeError> {
    if cur.len() < 64 {
        return Err(TokenDecodeError::Truncated);
    }
    let (head, tail) = cur.split_at(64);
    *cur = tail;
    let mut buf = [0u8; 64];
    buf.copy_from_slice(head);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use rand_core::OsRng;

    fn fresh_keypair() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    fn sign_payload(key: &SigningKey, payload: &[u8]) -> [u8; 64] {
        key.sign(payload).to_bytes()
    }

    fn build(volume: &str, coord: &str, issued_at: u64, key: &SigningKey) -> PeerFetchToken {
        let payload = PeerFetchToken::signing_payload(volume, coord, issued_at);
        let sig = sign_payload(key, &payload);
        PeerFetchToken::new(volume.to_owned(), coord.to_owned(), issued_at, sig)
    }

    #[test]
    fn round_trip_encode_decode() {
        let key = fresh_keypair();
        let tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        let encoded = tok.encode();
        let decoded = PeerFetchToken::decode(&encoded).expect("decode");
        assert_eq!(decoded, tok);
    }

    #[test]
    fn signature_verifies_against_issuing_key() {
        let key = fresh_keypair();
        let tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        tok.verify_signature(&key.verifying_key())
            .expect("signature verifies");
    }

    #[test]
    fn signature_fails_against_unrelated_key() {
        let key1 = fresh_keypair();
        let key2 = fresh_keypair();
        let tok = build("vol-a", "coord-x", 1_700_000_000, &key1);
        let err = tok
            .verify_signature(&key2.verifying_key())
            .expect_err("signature should not verify under unrelated key");
        assert!(matches!(err, TokenVerifyError::BadSignature(_)));
    }

    #[test]
    fn tampered_volume_name_invalidates_signature() {
        let key = fresh_keypair();
        let mut tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        tok.volume_name = "vol-b".to_owned();
        let err = tok
            .verify_signature(&key.verifying_key())
            .expect_err("tampered field should fail signature check");
        assert!(matches!(err, TokenVerifyError::BadSignature(_)));
    }

    #[test]
    fn tampered_coordinator_id_invalidates_signature() {
        let key = fresh_keypair();
        let mut tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        tok.coordinator_id = "coord-y".to_owned();
        let err = tok
            .verify_signature(&key.verifying_key())
            .expect_err("tampered field should fail signature check");
        assert!(matches!(err, TokenVerifyError::BadSignature(_)));
    }

    #[test]
    fn tampered_issued_at_invalidates_signature() {
        let key = fresh_keypair();
        let mut tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        tok.issued_at += 1;
        let err = tok
            .verify_signature(&key.verifying_key())
            .expect_err("tampered field should fail signature check");
        assert!(matches!(err, TokenVerifyError::BadSignature(_)));
    }

    #[test]
    fn freshness_accepts_within_window() {
        let key = fresh_keypair();
        let tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        tok.check_freshness(1_700_000_010, DEFAULT_FRESHNESS_WINDOW_SECS)
            .expect("within +/-60s");
        tok.check_freshness(1_699_999_990, DEFAULT_FRESHNESS_WINDOW_SECS)
            .expect("within +/-60s in the past");
    }

    #[test]
    fn freshness_rejects_outside_window() {
        let key = fresh_keypair();
        let tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        let err = tok
            .check_freshness(1_700_000_120, 60)
            .expect_err("future drift > window");
        assert!(matches!(err, TokenVerifyError::Stale { skew_secs } if skew_secs == 120));
        let err = tok
            .check_freshness(1_699_999_800, 60)
            .expect_err("past drift > window");
        assert!(matches!(err, TokenVerifyError::Stale { skew_secs } if skew_secs == 200));
    }

    #[test]
    fn decode_rejects_invalid_base64() {
        let err = PeerFetchToken::decode("not!base64!").expect_err("bad base64");
        assert_eq!(err, TokenDecodeError::InvalidBase64);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let key = fresh_keypair();
        let tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        let encoded = tok.encode();
        // Drop the last 8 chars of base64 → roughly 6 bytes lopped off the
        // end, which sits inside the 64-byte signature suffix.
        let truncated = &encoded[..encoded.len() - 8];
        let err = PeerFetchToken::decode(truncated).expect_err("truncated");
        assert_eq!(err, TokenDecodeError::Truncated);
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let key = fresh_keypair();
        let tok = build("vol-a", "coord-x", 1_700_000_000, &key);
        let mut bytes = BASE64.decode(tok.encode()).unwrap();
        bytes.push(0xff);
        let encoded = BASE64.encode(&bytes);
        let err = PeerFetchToken::decode(&encoded).expect_err("trailing bytes");
        assert_eq!(err, TokenDecodeError::TrailingBytes);
    }

    #[test]
    fn signing_payload_includes_domain_tag() {
        let payload = PeerFetchToken::signing_payload("v", "c", 0);
        assert!(payload.starts_with(DOMAIN_TAG));
    }

    #[test]
    fn signing_payload_differs_when_any_field_changes() {
        let p1 = PeerFetchToken::signing_payload("v1", "c", 1);
        let p2 = PeerFetchToken::signing_payload("v2", "c", 1);
        let p3 = PeerFetchToken::signing_payload("v1", "c2", 1);
        let p4 = PeerFetchToken::signing_payload("v1", "c", 2);
        assert_ne!(p1, p2);
        assert_ne!(p1, p3);
        assert_ne!(p1, p4);
    }
}
