//! Role gating and TTL computation (`docs/design-mint.md`
//! § *Role configuration*, § *TTL bounds*).
//!
//! Given a *verified* macaroon's caveats, a requested role, and a
//! requested TTL, decide whether the role may be assumed and for how
//! long. This module does **not** verify the MAC — that already
//! happened — it only evaluates caveat *values*.

use crate::caveat::{EffectiveCaveats, Resolved, name};
use crate::config::{Config, Role};

const AUDIENCE_CAVEAT: &str = name::AUD;
const NOT_AFTER_CAVEAT: &str = name::EXP;
const ROLE_CAVEAT: &str = name::ROLE;

/// Why an assume-role request was refused. Mapped to coarse HTTP
/// statuses by the caller; never surfaced verbatim to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Denied {
    /// Role name not in mint config.
    UnknownRole,
    /// `Audience` caveat missing or != configured audience.
    WrongAudience,
    /// A `Role` caveat is present and does not permit this role.
    RoleNotPermitted,
    /// A caveat named in the role's `required_caveats` is absent.
    MissingRequiredCaveat(String),
    /// A required caveat is present but its occurrences contradict
    /// (unsatisfiable) — fail closed, never treat as absent.
    UnsatisfiableCaveat(String),
    /// Macaroon carries no usable `NotAfter`, or it is already past.
    Expired,
    /// Requested TTL below the role's `min_ttl_seconds`.
    TtlTooShort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Granted {
    pub role: Role,
    /// Effective lifetime in seconds after clamping.
    pub ttl_seconds: u64,
}

/// Evaluate an assume-role request against config.
///
/// `requested_ttl` is the caller's `ttl_seconds` body field (already
/// defaulted to the role's `default_ttl_seconds` by the caller if the
/// field was absent). `now_unix` is the current time.
pub fn authorize(
    cfg: &Config,
    caveats: &[crate::caveat::Caveat],
    requested_role: &str,
    requested_ttl: u64,
    now_unix: u64,
) -> Result<Granted, Denied> {
    let role = cfg
        .roles
        .get(requested_role)
        .ok_or(Denied::UnknownRole)?
        .clone();

    let eff = EffectiveCaveats::new(caveats);

    // Audience: cross-service replay defence. Must resolve to a single
    // value equal to the configured name; absent or unsatisfiable both
    // fail closed.
    match eff.resolve(AUDIENCE_CAVEAT) {
        Resolved::Value(a) if a == cfg.audience => {}
        _ => return Err(Denied::WrongAudience),
    }

    // Optional Role caveat restricts which role is assumable (scalar —
    // a caller wanting a narrower role attenuates with a tighter one).
    // Absent = unrestricted; unsatisfiable = fail closed.
    match eff.resolve(ROLE_CAVEAT) {
        Resolved::Absent => {}
        Resolved::Value(s) if s == requested_role => {}
        Resolved::Value(_) | Resolved::Unsatisfiable => {
            return Err(Denied::RoleNotPermitted);
        }
    }

    // Required caveats: must be present *and* satisfiable. An
    // unsatisfiable required caveat is a distinct denial, never
    // collapsed to "missing".
    for req in &role.required_caveats {
        match eff.resolve(req) {
            Resolved::Value(_) => {}
            Resolved::Absent => return Err(Denied::MissingRequiredCaveat(req.clone())),
            Resolved::Unsatisfiable => {
                return Err(Denied::UnsatisfiableCaveat(req.clone()));
            }
        }
    }

    // TTL: granted = min(requested_or_default, role.max, NotAfter - now).
    let not_after = eff.not_after(NOT_AFTER_CAVEAT).ok_or(Denied::Expired)?;
    let remaining = not_after.checked_sub(now_unix).ok_or(Denied::Expired)?;
    if remaining == 0 {
        return Err(Denied::Expired);
    }
    if requested_ttl < role.min_ttl_seconds {
        return Err(Denied::TtlTooShort);
    }
    let ttl_seconds = requested_ttl.min(role.max_ttl_seconds).min(remaining);

    Ok(Granted { role, ttl_seconds })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caveat::Caveat;
    use crate::config::Config;

    fn cfg() -> Config {
        crate::config::parse_for_test(
            r#"
audience = "mint"
[tenant]
bucket = "b"
[[role]]
name = "volume-ro"
required_caveats = ["elide:Volume", "aud", "exp"]
min_ttl_seconds = 60
max_ttl_seconds = 1000
default_ttl_seconds = 800
policy_file = "volume-ro.json"
"#,
            &[("volume-ro.json", "{}")],
        )
        .expect("cfg")
    }

    fn good_caveats(not_after: u64) -> Vec<Caveat> {
        vec![
            Caveat::scalar(name::AUD, "mint"),
            Caveat::scalar("elide:Volume", "01ARZ"),
            Caveat::scalar(name::EXP, not_after.to_string()),
        ]
    }

    #[test]
    fn happy_path_clamps_to_max() {
        let g = authorize(&cfg(), &good_caveats(1_000_000), "volume-ro", 5000, 1000).unwrap();
        assert_eq!(g.ttl_seconds, 1000); // role max
    }

    #[test]
    fn ttl_capped_by_not_after() {
        let g = authorize(&cfg(), &good_caveats(1300), "volume-ro", 900, 1000).unwrap();
        assert_eq!(g.ttl_seconds, 300); // not_after - now
    }

    #[test]
    fn wrong_audience_denied() {
        let mut cv = good_caveats(1_000_000);
        cv[0] = Caveat::scalar(name::AUD, "other");
        assert_eq!(
            authorize(&cfg(), &cv, "volume-ro", 800, 1000),
            Err(Denied::WrongAudience)
        );
    }

    #[test]
    fn missing_required_caveat_denied() {
        let cv = vec![
            Caveat::scalar(name::AUD, "mint"),
            Caveat::scalar(name::EXP, "1000000"),
        ];
        assert_eq!(
            authorize(&cfg(), &cv, "volume-ro", 800, 1000),
            Err(Denied::MissingRequiredCaveat("elide:Volume".into()))
        );
    }

    #[test]
    fn expired_macaroon_denied() {
        assert_eq!(
            authorize(&cfg(), &good_caveats(500), "volume-ro", 800, 1000),
            Err(Denied::Expired)
        );
    }

    #[test]
    fn unknown_role_denied() {
        assert_eq!(
            authorize(&cfg(), &good_caveats(1_000_000), "nope", 800, 1000),
            Err(Denied::UnknownRole)
        );
    }

    #[test]
    fn role_caveat_restricts() {
        let mut cv = good_caveats(1_000_000);
        cv.push(Caveat::scalar(name::ROLE, "coord-names"));
        assert_eq!(
            authorize(&cfg(), &cv, "volume-ro", 800, 1000),
            Err(Denied::RoleNotPermitted)
        );
    }
}
