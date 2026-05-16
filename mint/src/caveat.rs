//! Named scalar caveats.
//!
//! The mint is caveat-vocabulary-agnostic (see `docs/design-mint.md`
//! § *Macaroon caveat conventions*): it does not hard-code which caveat
//! names are meaningful. A caveat is a `(name, value)` pair; **every
//! caveat is scalar**. There is no list-valued caveat type — the only
//! list-shaped input a role ever needed (the `volume-ro` ancestor set)
//! rides the PoP-signed request body as `request.ancestors`, not the
//! caveat chain (design-mint.md § *All caveats are scalar*). This keeps
//! the macaroon library to scalar caveats plus the holder-of-key
//! extension, with no chain whose effective value depends on
//! occurrence order.

use std::collections::BTreeSet;

/// A single named scalar caveat in a macaroon's chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caveat {
    pub name: String,
    pub value: String,
}

impl Caveat {
    pub fn scalar(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// The resolution of one caveat name against the chain under AND
/// (attenuation) semantics. A macaroon attenuates by *appending*, so N
/// occurrences of a name are AND-ed. The three outcomes are **not**
/// collapsible to `Option`: conflating "absent" with "present but
/// unsatisfiable" is a downgrade footgun — a gate keyed on the former
/// would skip for the latter, and a holder can append a contradictory
/// copy of a binding caveat using only the trailing MAC. Every
/// consumer must handle all three.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// No occurrence of this name — genuinely unconstrained.
    Absent,
    /// Present and satisfiable: every occurrence agreed on this value.
    Value(String),
    /// Present but ≥2 occurrences disagree: the AND is empty. Must
    /// deny in **every** consumer — never silently read as `Absent`.
    Unsatisfiable,
}

/// The effective view of a caveat chain. The one place "what does this
/// caveat mean" is decided, shared by the gate ([`crate::role`]), the
/// policy renderer ([`crate::template`]), and the holder-of-key check
/// ([`crate::pop`]). Every caveat is scalar: repeated occurrences must
/// agree (→ `Value`); ≥2 distinct → `Unsatisfiable`. `NotAfter` is
/// handled out of band ([`Self::not_after`], numeric minimum).
pub struct EffectiveCaveats<'a> {
    caveats: &'a [Caveat],
}

impl<'a> EffectiveCaveats<'a> {
    pub fn new(caveats: &'a [Caveat]) -> Self {
        Self { caveats }
    }

    /// Resolve `name` against the chain under AND semantics. The single
    /// definition of the caveat's effective meaning; tri-state so no
    /// consumer can collapse "absent" into "unsatisfiable" (see
    /// [`Resolved`]).
    pub fn resolve(&self, name: &str) -> Resolved {
        let mut occ = self
            .caveats
            .iter()
            .filter(|c| c.name == name)
            .map(|c| c.value.as_str());
        let Some(first) = occ.next() else {
            return Resolved::Absent;
        };
        if occ.all(|v| v == first) {
            Resolved::Value(first.to_string())
        } else {
            Resolved::Unsatisfiable
        }
    }

    /// Distinct caveat names in first-occurrence order.
    pub fn names(&self) -> Vec<&'a str> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for c in self.caveats {
            if seen.insert(c.name.as_str()) {
                out.push(c.name.as_str());
            }
        }
        out
    }

    /// Minimum `NotAfter` (unix seconds) across all `NotAfter` caveats,
    /// or `None` if the macaroon carries no parseable `NotAfter`. This
    /// is a numeric intersection (the minimum binds), distinct from the
    /// scalar-agreement resolution of [`Self::resolve`].
    pub fn not_after(&self, name: &str) -> Option<u64> {
        self.caveats
            .iter()
            .filter(|c| c.name == name)
            .filter_map(|c| c.value.parse::<u64>().ok())
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cv(pairs: &[(&str, &str)]) -> Vec<Caveat> {
        pairs.iter().map(|(n, v)| Caveat::scalar(*n, *v)).collect()
    }

    #[test]
    fn absent_when_no_occurrence() {
        let c = cv(&[("Audience", "mint")]);
        assert_eq!(
            EffectiveCaveats::new(&c).resolve("elide:Volume"),
            Resolved::Absent
        );
    }

    #[test]
    fn single_and_agreeing_occurrences_resolve_to_value() {
        let c = cv(&[("elide:Volume", "V1"), ("elide:Volume", "V1")]);
        assert_eq!(
            EffectiveCaveats::new(&c).resolve("elide:Volume"),
            Resolved::Value("V1".into())
        );
    }

    #[test]
    fn disagreeing_occurrences_are_unsatisfiable_not_absent() {
        // The downgrade footgun: an appended contradictory copy must
        // resolve to Unsatisfiable, never Absent.
        let c = cv(&[
            ("elide:CoordKey", "ed25519:A"),
            ("elide:CoordKey", "ed25519:B"),
        ]);
        assert_eq!(
            EffectiveCaveats::new(&c).resolve("elide:CoordKey"),
            Resolved::Unsatisfiable
        );
    }

    #[test]
    fn not_after_takes_the_minimum() {
        let c = cv(&[
            ("NotAfter", "5000"),
            ("NotAfter", "3000"),
            ("NotAfter", "9000"),
        ]);
        assert_eq!(EffectiveCaveats::new(&c).not_after("NotAfter"), Some(3000));
    }
}
