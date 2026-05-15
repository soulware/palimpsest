//! Generic named caveats.
//!
//! The mint is caveat-vocabulary-agnostic (see `docs/design-mint.md`
//! § *Macaroon caveat conventions*): it does not hard-code which caveat
//! names are meaningful. A caveat is a `(name, value)` pair where the
//! value is either a scalar string or a list of strings.
//!
//! This is the open-question-#6 generalisation of the elide v2 typed
//! macaroon: the existing coordinator format encodes a fixed enum of
//! typed caveats; here the name is free-form and exactly one list-valued
//! shape exists (the only macaroon-library extension the Elide role
//! inventory requires).

use std::collections::BTreeSet;

/// A caveat value: either a single string or an ordered list of strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaveatValue {
    Scalar(String),
    List(Vec<String>),
}

/// A single named caveat in a macaroon's chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caveat {
    pub name: String,
    pub value: CaveatValue,
}

impl Caveat {
    pub fn scalar(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: CaveatValue::Scalar(value.into()),
        }
    }

    pub fn list<I, S>(name: impl Into<String>, items: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            value: CaveatValue::List(items.into_iter().map(Into::into).collect()),
        }
    }
}

/// The effective view of a caveat chain after applying intersection
/// semantics: a macaroon attenuates by appending caveats, so multiple
/// occurrences of the same name **narrow** rather than override.
///
/// - Scalar caveats: every occurrence must carry the same value; a
///   disagreement is a contradiction the holder constructed and the
///   caveat resolves to "unsatisfiable" (treated as absent for gating,
///   which fails any role that requires it).
/// - List caveats: the effective value is the intersection across all
///   occurrences, preserving the order of the first occurrence. This is
///   the `elide:Ancestors` shape from the design doc.
/// - `NotAfter` is handled out of band (caller takes the minimum) since
///   it intersects numerically, not by set membership.
pub struct EffectiveCaveats<'a> {
    caveats: &'a [Caveat],
}

impl<'a> EffectiveCaveats<'a> {
    pub fn new(caveats: &'a [Caveat]) -> Self {
        Self { caveats }
    }

    /// True if any caveat with this name is present (used for the
    /// `required_caveats` gate, which only checks presence).
    pub fn contains(&self, name: &str) -> bool {
        self.caveats.iter().any(|c| c.name == name)
    }

    /// The effective scalar value for `name`, or `None` if absent or
    /// self-contradictory (two occurrences with differing values).
    pub fn scalar(&self, name: &str) -> Option<&'a str> {
        let mut found: Option<&str> = None;
        for c in self.caveats.iter().filter(|c| c.name == name) {
            let v = match &c.value {
                CaveatValue::Scalar(s) => s.as_str(),
                CaveatValue::List(_) => return None,
            };
            match found {
                None => found = Some(v),
                Some(prev) if prev == v => {}
                Some(_) => return None,
            }
        }
        found
    }

    /// The effective list value for `name`: intersection across every
    /// list-valued occurrence, in first-occurrence order. `None` if no
    /// list-valued occurrence exists.
    pub fn list(&self, name: &str) -> Option<Vec<String>> {
        let mut iter = self
            .caveats
            .iter()
            .filter(|c| c.name == name)
            .filter_map(|c| match &c.value {
                CaveatValue::List(items) => Some(items),
                CaveatValue::Scalar(_) => None,
            });
        let first = iter.next()?;
        let mut acc: Vec<String> = first.clone();
        for items in iter {
            let allow: BTreeSet<&String> = items.iter().collect();
            acc.retain(|x| allow.contains(x));
        }
        Some(acc)
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

    /// The single effective value for `name` after intersection
    /// semantics — the *one* definition of "what this caveat means",
    /// shared by the gate ([`crate::role`]) and the policy renderer
    /// ([`crate::template`]).
    ///
    /// - Any list-valued occurrence ⇒ `List` of the intersection.
    /// - Otherwise scalar: `Scalar` iff every occurrence agrees;
    ///   `None` if occurrences contradict (a token the holder
    ///   constructed to be unsatisfiable must not mint anything).
    pub fn effective(&self, name: &str) -> Option<CaveatValue> {
        let has_list = self
            .caveats
            .iter()
            .any(|c| c.name == name && matches!(c.value, CaveatValue::List(_)));
        if has_list {
            self.list(name).map(CaveatValue::List)
        } else {
            self.scalar(name)
                .map(|s| CaveatValue::Scalar(s.to_string()))
        }
    }

    /// Minimum `NotAfter` (unix seconds) across all `NotAfter` caveats,
    /// or `None` if the macaroon carries no parseable `NotAfter`.
    pub fn not_after(&self, name: &str) -> Option<u64> {
        self.caveats
            .iter()
            .filter(|c| c.name == name)
            .filter_map(|c| match &c.value {
                CaveatValue::Scalar(s) => s.parse::<u64>().ok(),
                CaveatValue::List(_) => None,
            })
            .min()
    }
}
