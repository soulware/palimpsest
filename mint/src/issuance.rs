//! Macaroon issuance (`docs/design-mint.md` § *Coordinator bootstrap &
//! macaroon lifecycle*, § *Enrollment*).
//!
//! Every macaroon mint here is a fresh chain **from the root** (only
//! the root holder can mint; a coordinator can only attenuate). Three
//! mint points, each stamped with its own positively-required `op`:
//!
//! 1. [`mint_bootstrap`] — `op=enroll`, the reusable non-expiring
//!    participation gate. No principal identity; carries the current
//!    `bootstrap` nonce.
//! 2. [`mint_credential_ticket`] — `op=enroll-exchange`, short-lived,
//!    minted at `POST /v1/enroll` once the presented (coordinator-
//!    attenuated) bootstrap has verified and a pending record exists.
//!    Carries the self-asserted `sub`/`cnf` forward.
//! 3. [`mint_credential`] — `op=assume-role`, non-expiring, minted at
//!    `POST /v1/enroll-exchange` after operator approval. Same
//!    `sub`/`cnf`; no `exp`.
//!
//! MAC verification, the `op`/`aud`/`bootstrap` gates, the holder-of-key
//! PoP and the pending/approval lookup are the HTTP layer's job (they
//! need the root, config and the state store). The functions here are
//! pure given an already-authenticated macaroon.

use crate::caveat::{Caveat, EffectiveCaveats, Resolved, name, op};
use crate::macaroon::{self, Macaroon};
use crate::pop;

/// Why extracting the bound identity from a verified macaroon failed.
/// The HTTP layer collapses every variant to the same opaque `401`
/// (don't help an attacker distinguish causes); the variant is for the
/// audit log and tests.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnrollError {
    /// `cnf` is not a well-formed `ed25519:<b64 pub>`.
    #[error("malformed cnf")]
    BadCnf,
    /// No `sub` (the macaroon is not principal-scoped).
    #[error("missing sub")]
    MissingSub,
    /// No `cnf` (not key-bound — a bearer cannot enrol).
    #[error("missing cnf")]
    MissingCnf,
    /// A binding caveat resolved `Unsatisfiable` (≥2 disagreeing
    /// occurrences — the append-a-contradictory-copy downgrade; fail
    /// closed, never read as absent).
    #[error("contradictory binding caveat")]
    Unsatisfiable,
}

/// The reusable bootstrap macaroon: root attenuated with `op=enroll`,
/// `aud`, and the current `bootstrap` nonce. Non-expiring, carries no
/// principal identity — a pure participation gate, distributed
/// out-of-band and reusable for every enrolling client.
pub fn mint_bootstrap(root: &[u8; 32], audience: &str, bootstrap_nonce: &str) -> Macaroon {
    macaroon::mint(
        root,
        vec![
            Caveat::scalar(name::OP, op::ENROLL),
            Caveat::scalar(name::AUD, audience),
            Caveat::scalar(name::BOOTSTRAP, bootstrap_nonce),
        ],
    )
}

/// The short-lived credential ticket handed back from `POST /v1/enroll`:
/// `op=enroll-exchange`, `aud`, the self-asserted `sub`/`cnf`, and an
/// `exp`. (The third-party caveat for a configured identity authority
/// is deferred — design *Open questions* #15.)
pub fn mint_credential_ticket(
    root: &[u8; 32],
    audience: &str,
    sub: &str,
    cnf: &str,
    exp_unix: u64,
) -> Macaroon {
    macaroon::mint(
        root,
        vec![
            Caveat::scalar(name::OP, op::ENROLL_EXCHANGE),
            Caveat::scalar(name::AUD, audience),
            Caveat::scalar(name::SUB, sub),
            Caveat::scalar(name::CNF, cnf),
            Caveat::scalar(name::EXP, exp_unix.to_string()),
        ],
    )
}

/// The non-expiring credential, re-minted from root at a successful
/// exchange: `op=assume-role`, `aud`, the same `sub`/`cnf`, the
/// `role` it was authorized for, **no** `exp`. A fresh chain, not an
/// attenuation of the credential ticket (only the root holder can do
/// this). One credential carries exactly one role — a coordinator
/// exchanges once per role it needs (`docs/design-mint.md` §
/// *Coordinator bootstrap*).
pub fn mint_credential(
    root: &[u8; 32],
    audience: &str,
    sub: &str,
    cnf: &str,
    role: &str,
) -> Macaroon {
    macaroon::mint(
        root,
        vec![
            Caveat::scalar(name::OP, op::ASSUME_ROLE),
            Caveat::scalar(name::AUD, audience),
            Caveat::scalar(name::SUB, sub),
            Caveat::scalar(name::CNF, cnf),
            Caveat::scalar(name::ROLE, role),
        ],
    )
}

/// Extract the bound `(sub, cnf)` from an already-MAC-verified
/// macaroon, tri-state-safe: a contradictory copy of either fails
/// closed (never silently read as the first value), and `cnf` is
/// validated as a usable Ed25519 key here so a malformed one is
/// refused at enrollment rather than opaquely at first `assume-role`.
pub fn bound_identity(token: &Macaroon) -> Result<(String, String), EnrollError> {
    let eff = EffectiveCaveats::new(token.caveats());
    let sub = match eff.resolve(name::SUB) {
        Resolved::Value(v) => v,
        Resolved::Unsatisfiable => return Err(EnrollError::Unsatisfiable),
        Resolved::Absent => return Err(EnrollError::MissingSub),
    };
    let cnf = match eff.resolve(name::CNF) {
        Resolved::Value(v) => v,
        Resolved::Unsatisfiable => return Err(EnrollError::Unsatisfiable),
        Resolved::Absent => return Err(EnrollError::MissingCnf),
    };
    pop::validate_cnf(&cnf).map_err(|_| EnrollError::BadCnf)?;
    Ok((sub, cnf))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: [u8; 32] = [7u8; 32];
    const SUB: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    fn cnf() -> String {
        pop::cnf_value(&[3u8; 32])
    }

    #[test]
    fn bootstrap_is_enroll_op_no_identity() {
        let b = mint_bootstrap(&ROOT, "mint", "nonceXYZ");
        assert!(b.verify(&ROOT));
        let eff = EffectiveCaveats::new(b.caveats());
        assert_eq!(eff.resolve(name::OP), Resolved::Value(op::ENROLL.into()));
        assert_eq!(eff.resolve(name::AUD), Resolved::Value("mint".into()));
        assert_eq!(
            eff.resolve(name::BOOTSTRAP),
            Resolved::Value("nonceXYZ".into())
        );
        assert_eq!(eff.resolve(name::SUB), Resolved::Absent);
        assert_eq!(eff.resolve(name::CNF), Resolved::Absent);
    }

    #[test]
    fn ticket_then_credential_carry_identity_with_distinct_ops() {
        let ticket = mint_credential_ticket(&ROOT, "mint", SUB, &cnf(), 1_700_000_000);
        assert!(ticket.verify(&ROOT));
        let ie = EffectiveCaveats::new(ticket.caveats());
        assert_eq!(
            ie.resolve(name::OP),
            Resolved::Value(op::ENROLL_EXCHANGE.into())
        );
        assert_eq!(ie.not_after(name::EXP), Some(1_700_000_000));

        let cred = mint_credential(&ROOT, "mint", SUB, &cnf(), "volume-ro");
        assert!(cred.verify(&ROOT));
        let pe = EffectiveCaveats::new(cred.caveats());
        assert_eq!(
            pe.resolve(name::OP),
            Resolved::Value(op::ASSUME_ROLE.into())
        );
        assert_eq!(pe.resolve(name::SUB), Resolved::Value(SUB.into()));
        assert_eq!(pe.resolve(name::CNF), Resolved::Value(cnf()));
        assert_eq!(pe.resolve(name::ROLE), Resolved::Value("volume-ro".into()));
        // The credential does not expire.
        assert_eq!(pe.not_after(name::EXP), None);
        // Fresh chain, not an attenuation of the credential ticket.
        assert_ne!(cred.nonce(), ticket.nonce());
    }

    #[test]
    fn bound_identity_extracts_sub_and_cnf() {
        let m = macaroon::mint(
            &ROOT,
            vec![
                Caveat::scalar(name::SUB, SUB),
                Caveat::scalar(name::CNF, cnf()),
            ],
        );
        assert_eq!(bound_identity(&m), Ok((SUB.to_string(), cnf())));
    }

    #[test]
    fn missing_or_bad_identity_refused() {
        let no_cnf = macaroon::mint(&ROOT, vec![Caveat::scalar(name::SUB, SUB)]);
        assert_eq!(bound_identity(&no_cnf), Err(EnrollError::MissingCnf));

        let no_sub = macaroon::mint(&ROOT, vec![Caveat::scalar(name::CNF, cnf())]);
        assert_eq!(bound_identity(&no_sub), Err(EnrollError::MissingSub));

        let bad_cnf = macaroon::mint(
            &ROOT,
            vec![
                Caveat::scalar(name::SUB, SUB),
                Caveat::scalar(name::CNF, "ed25519:not-base64!!"),
            ],
        );
        assert_eq!(bound_identity(&bad_cnf), Err(EnrollError::BadCnf));
    }

    #[test]
    fn contradictory_binding_fails_closed() {
        // Appended second, disagreeing sub (only the trailing MAC is
        // needed to append). Must be Unsatisfiable, never the first value.
        let m = macaroon::mint(
            &ROOT,
            vec![
                Caveat::scalar(name::SUB, SUB),
                Caveat::scalar(name::CNF, cnf()),
            ],
        )
        .attenuate(Caveat::scalar(name::SUB, "01EVIL"));
        assert_eq!(bound_identity(&m), Err(EnrollError::Unsatisfiable));
    }
}
