//! IPC `eligibility` classification for the `volume status --remote` verb.
//!
//! Captures the relationship between *this* coordinator and a foreign
//! `names/<name>` record: whether we own the name, another coordinator
//! holds it, the name is claimable, or it's a readonly handle. The
//! string form is the wire format embedded in the `volume_status_remote`
//! TOML reply; the typed form is the source of truth on both sides.

use elide_core::name_record::{NameRecord, NameState};
use serde::{Deserialize, Serialize};

/// This coordinator's eligibility relative to a foreign `NameRecord`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Eligibility {
    /// `state ∈ {Live, Stopped}` and `coordinator_id` matches our id.
    Owned,
    /// `state ∈ {Live, Stopped}` but held by another coordinator (or
    /// the record predates the coordinator-id plumbing and has no
    /// owner string at all).
    Foreign,
    /// `state == Released`. Any coordinator may `volume claim` it.
    ReleasedClaimable,
    /// `state == Readonly`. No exclusive owner; anyone may pull and serve.
    Readonly,
}

impl Eligibility {
    /// Classify this coordinator's eligibility against a name record.
    pub fn from_record(record: &NameRecord, our_coord_id: &str) -> Self {
        match record.state {
            NameState::Live | NameState::Stopped => match record.coordinator_id.as_deref() {
                Some(owner) if owner == our_coord_id => Self::Owned,
                _ => Self::Foreign,
            },
            NameState::Released => Self::ReleasedClaimable,
            NameState::Readonly => Self::Readonly,
        }
    }

    /// Wire-format string used in the `volume_status_remote` reply and
    /// rendered raw by the CLI's `volume status --remote`.
    pub fn wire_str(self) -> &'static str {
        match self {
            Self::Owned => "owned",
            Self::Foreign => "foreign",
            Self::ReleasedClaimable => "released-claimable",
            Self::Readonly => "readonly",
        }
    }
}

impl std::fmt::Display for Eligibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.wire_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn record(state: NameState, owner: Option<&str>) -> NameRecord {
        NameRecord {
            version: NameRecord::CURRENT_VERSION,
            vol_ulid: Ulid::nil(),
            coordinator_id: owner.map(str::to_owned),
            state,
            parent: None,
            claimed_at: None,
            hostname: None,
            handoff_snapshot: None,
        }
    }

    #[test]
    fn live_owned_by_us_is_owned() {
        let r = record(NameState::Live, Some("me"));
        assert_eq!(Eligibility::from_record(&r, "me"), Eligibility::Owned);
    }

    #[test]
    fn stopped_owned_by_us_is_owned() {
        let r = record(NameState::Stopped, Some("me"));
        assert_eq!(Eligibility::from_record(&r, "me"), Eligibility::Owned);
    }

    #[test]
    fn live_held_by_other_is_foreign() {
        let r = record(NameState::Live, Some("them"));
        assert_eq!(Eligibility::from_record(&r, "me"), Eligibility::Foreign);
    }

    #[test]
    fn stopped_held_by_other_is_foreign() {
        let r = record(NameState::Stopped, Some("them"));
        assert_eq!(Eligibility::from_record(&r, "me"), Eligibility::Foreign);
    }

    #[test]
    fn live_with_no_owner_is_foreign_for_safety() {
        // Phase 1 records have no `coordinator_id`. We classify as
        // Foreign rather than Owned so a host that did not write the
        // record can never claim ownership of it implicitly.
        let r = record(NameState::Live, None);
        assert_eq!(Eligibility::from_record(&r, "me"), Eligibility::Foreign);
    }

    #[test]
    fn released_is_claimable() {
        let r = record(NameState::Released, None);
        assert_eq!(
            Eligibility::from_record(&r, "me"),
            Eligibility::ReleasedClaimable
        );
    }

    #[test]
    fn readonly_classifies_as_readonly() {
        let r = record(NameState::Readonly, None);
        assert_eq!(Eligibility::from_record(&r, "me"), Eligibility::Readonly);
    }

    #[test]
    fn wire_str_matches_display() {
        for e in [
            Eligibility::Owned,
            Eligibility::Foreign,
            Eligibility::ReleasedClaimable,
            Eligibility::Readonly,
        ] {
            assert_eq!(format!("{e}"), e.wire_str());
        }
    }

    #[test]
    fn wire_str_values() {
        assert_eq!(Eligibility::Owned.wire_str(), "owned");
        assert_eq!(Eligibility::Foreign.wire_str(), "foreign");
        assert_eq!(
            Eligibility::ReleasedClaimable.wire_str(),
            "released-claimable"
        );
        assert_eq!(Eligibility::Readonly.wire_str(), "readonly");
    }
}
