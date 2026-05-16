//! Primary-macaroon issuance (`docs/design-mint.md` § *Coordinator
//! bootstrap & macaroon lifecycle*).
//!
//! Issuance is two acts by two different principals at two different
//! moments, sharing only the `elide:CoordKey` through-line:
//!
//! 1. **Enrollment-token mint** — an operator, on the mint host, with
//!    the mint root ([`mint_enrollment_token`]). It is the "vouch for
//!    coordinator X" act: in the minimal self-hosted deployment there is
//!    no identity authority to discharge a third-party caveat against
//!    (design *Open questions* #15, deferred), so this admin-local
//!    operation *is* the trust anchor. The token is **expiring**
//!    (`NotAfter`) and **key-bound** (`elide:CoordKey` = the
//!    coordinator's public identity key).
//!
//! 2. **Enrollment exchange** — the coordinator, over HTTP, proving
//!    possession of `coordinator.key` ([`remint_primary`], driven by
//!    `POST /v1/enroll`). Mint re-mints **from its root** a primary
//!    carrying the *same* `Audience` + `elide:Coord` + `elide:CoordKey`,
//!    **stripped of `NotAfter`** — Fly.io's service-token pattern, only
//!    the root holder can re-mint. The exchange does not bind a key; it
//!    removes the expiry scaffolding around a binding already there, so
//!    a stolen enrollment token is inert (the thief lacks
//!    `coordinator.key`; PoP fails) and the primary does not expire (a
//!    file-only leak is already inert, a key compromise renews
//!    regardless).
//!
//! MAC verification, the holder-of-key PoP, and the `NotAfter` clock
//! check are the HTTP layer's job (they need the root, the request
//! body/proof, and the verifier's clock respectively). The functions
//! here are pure given an already-authenticated token.

use crate::caveat::{Caveat, EffectiveCaveats, Resolved};
use crate::macaroon::{self, Macaroon};
use crate::pop::{self, COORD_KEY_CAVEAT};

pub const AUDIENCE_CAVEAT: &str = "Audience";
pub const COORD_CAVEAT: &str = "elide:Coord";
pub const NOT_AFTER_CAVEAT: &str = "NotAfter";

/// Why issuance refused. The HTTP layer collapses every variant to the
/// same opaque `401` (the enrollment exchange shares `assume-role`'s
/// "don't help an attacker distinguish causes" model); the variant is
/// for the audit log and tests.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnrollError {
    /// `elide:CoordKey` value is not a well-formed Ed25519 pubkey.
    #[error("malformed elide:CoordKey")]
    BadCoordKey,
    /// The token's `Audience` is missing or != this mint's audience.
    #[error("audience mismatch")]
    AudienceMismatch,
    /// The token carries no `elide:Coord` (it is not coordinator-scoped).
    #[error("missing elide:Coord")]
    MissingCoord,
    /// The token carries no `elide:CoordKey` (it is a bearer token — an
    /// enrollment token must be key-bound or a captured copy enrols).
    #[error("missing elide:CoordKey")]
    MissingCoordKey,
    /// A binding caveat resolved `Unsatisfiable` (≥2 disagreeing
    /// occurrences — the append-a-contradictory-copy downgrade; fail
    /// closed, never read as absent).
    #[error("contradictory binding caveat")]
    Unsatisfiable,
}

/// Mint an enrollment token: the mint root attenuated with
/// `Audience` + `elide:Coord` + `elide:CoordKey` + `NotAfter`. Operator
/// act (2) — `coord_key_value` is the coordinator's *public* identity
/// (`ed25519:<b64>`), validated here so a typo is rejected now rather
/// than at the coordinator's first `assume-role`.
pub fn mint_enrollment_token(
    root: &[u8; 32],
    audience: &str,
    coord_ulid: &str,
    coord_key_value: &str,
    not_after_unix: u64,
) -> Result<Macaroon, EnrollError> {
    pop::validate_coord_key(coord_key_value).map_err(|_| EnrollError::BadCoordKey)?;
    Ok(macaroon::mint(
        root,
        vec![
            Caveat::scalar(AUDIENCE_CAVEAT, audience),
            Caveat::scalar(COORD_CAVEAT, coord_ulid),
            Caveat::scalar(COORD_KEY_CAVEAT, coord_key_value),
            Caveat::scalar(NOT_AFTER_CAVEAT, not_after_unix.to_string()),
        ],
    ))
}

/// Re-mint the primary from an authenticated enrollment `token`.
///
/// The caller (HTTP `/v1/enroll`) has already verified the token's MAC
/// against `root`, run the holder-of-key PoP, and confirmed `NotAfter`
/// is present and not past. This extracts the bound identity and
/// re-mints a fresh primary carrying exactly the `Audience`,
/// `elide:Coord` and `elide:CoordKey` triple, with `NotAfter` dropped so
/// the primary does not expire. The fresh nonce makes it an independent
/// chain, not an attenuation of the token (only the root holder can do
/// this).
pub fn remint_primary(
    root: &[u8; 32],
    expected_audience: &str,
    token: &Macaroon,
) -> Result<Macaroon, EnrollError> {
    let eff = EffectiveCaveats::new(token.caveats());

    match eff.resolve(AUDIENCE_CAVEAT) {
        Resolved::Value(a) if a == expected_audience => {}
        Resolved::Unsatisfiable => return Err(EnrollError::Unsatisfiable),
        _ => return Err(EnrollError::AudienceMismatch),
    }

    let coord = match eff.resolve(COORD_CAVEAT) {
        Resolved::Value(v) => v,
        Resolved::Unsatisfiable => return Err(EnrollError::Unsatisfiable),
        Resolved::Absent => return Err(EnrollError::MissingCoord),
    };

    let coord_key = match eff.resolve(COORD_KEY_CAVEAT) {
        Resolved::Value(v) => v,
        Resolved::Unsatisfiable => return Err(EnrollError::Unsatisfiable),
        Resolved::Absent => return Err(EnrollError::MissingCoordKey),
    };
    pop::validate_coord_key(&coord_key).map_err(|_| EnrollError::BadCoordKey)?;

    Ok(macaroon::mint(
        root,
        vec![
            Caveat::scalar(AUDIENCE_CAVEAT, expected_audience),
            Caveat::scalar(COORD_CAVEAT, coord),
            Caveat::scalar(COORD_KEY_CAVEAT, coord_key),
        ],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: [u8; 32] = [7u8; 32];
    const COORD: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    fn coord_key() -> String {
        pop::coord_key_value(&[3u8; 32])
    }

    #[test]
    fn enrollment_token_carries_the_four_caveats() {
        let t =
            mint_enrollment_token(&ROOT, "mint", COORD, &coord_key(), 1_700_000_000).expect("mint");
        assert!(t.verify(&ROOT));
        let eff = EffectiveCaveats::new(t.caveats());
        assert_eq!(eff.resolve(AUDIENCE_CAVEAT), Resolved::Value("mint".into()));
        assert_eq!(eff.resolve(COORD_CAVEAT), Resolved::Value(COORD.into()));
        assert_eq!(eff.resolve(COORD_KEY_CAVEAT), Resolved::Value(coord_key()));
        assert_eq!(eff.not_after(NOT_AFTER_CAVEAT), Some(1_700_000_000));
    }

    #[test]
    fn malformed_coord_pub_rejected_at_token_mint() {
        assert_eq!(
            mint_enrollment_token(&ROOT, "mint", COORD, "ed25519:not-base64!!", 1),
            Err(EnrollError::BadCoordKey)
        );
        assert_eq!(
            mint_enrollment_token(&ROOT, "mint", COORD, "rsa:whatever", 1),
            Err(EnrollError::BadCoordKey)
        );
    }

    #[test]
    fn primary_strips_not_after_and_preserves_binding() {
        let token =
            mint_enrollment_token(&ROOT, "mint", COORD, &coord_key(), 1_700_000_000).unwrap();
        let primary = remint_primary(&ROOT, "mint", &token).expect("remint");

        assert!(primary.verify(&ROOT));
        let eff = EffectiveCaveats::new(primary.caveats());
        // Same binding through-line ...
        assert_eq!(eff.resolve(COORD_CAVEAT), Resolved::Value(COORD.into()));
        assert_eq!(eff.resolve(COORD_KEY_CAVEAT), Resolved::Value(coord_key()));
        // ... NotAfter scaffolding removed: the primary does not expire.
        assert_eq!(eff.not_after(NOT_AFTER_CAVEAT), None);
        assert_eq!(primary.caveats().len(), 3);
    }

    #[test]
    fn primary_is_a_fresh_chain_not_an_attenuation() {
        // Re-mint from root → independent nonce, not token ‖ extra.
        let token =
            mint_enrollment_token(&ROOT, "mint", COORD, &coord_key(), 1_700_000_000).unwrap();
        let primary = remint_primary(&ROOT, "mint", &token).unwrap();
        assert_ne!(primary.nonce(), token.nonce());
        assert_ne!(primary.tail(), token.tail());
    }

    #[test]
    fn audience_mismatch_refused() {
        let token = mint_enrollment_token(&ROOT, "other", COORD, &coord_key(), 1).unwrap();
        assert_eq!(
            remint_primary(&ROOT, "mint", &token),
            Err(EnrollError::AudienceMismatch)
        );
    }

    #[test]
    fn bearer_enrollment_token_refused() {
        // No elide:CoordKey → a captured copy would enrol. Must reject;
        // an enrollment token has to be key-bound.
        let token = macaroon::mint(
            &ROOT,
            vec![
                Caveat::scalar(AUDIENCE_CAVEAT, "mint"),
                Caveat::scalar(COORD_CAVEAT, COORD),
                Caveat::scalar(NOT_AFTER_CAVEAT, "1"),
            ],
        );
        assert_eq!(
            remint_primary(&ROOT, "mint", &token),
            Err(EnrollError::MissingCoordKey)
        );
    }

    #[test]
    fn contradictory_binding_fails_closed() {
        // Appended second, disagreeing elide:Coord (only the trailing
        // MAC is needed to append). Must be Unsatisfiable, never read
        // as the first value.
        let token = mint_enrollment_token(&ROOT, "mint", COORD, &coord_key(), 1)
            .unwrap()
            .attenuate(Caveat::scalar(COORD_CAVEAT, "01EVIL"));
        assert_eq!(
            remint_primary(&ROOT, "mint", &token),
            Err(EnrollError::Unsatisfiable)
        );
    }

    #[test]
    fn missing_coord_refused() {
        let token = macaroon::mint(
            &ROOT,
            vec![
                Caveat::scalar(AUDIENCE_CAVEAT, "mint"),
                Caveat::scalar(COORD_KEY_CAVEAT, coord_key()),
            ],
        );
        assert_eq!(
            remint_primary(&ROOT, "mint", &token),
            Err(EnrollError::MissingCoord)
        );
    }
}
