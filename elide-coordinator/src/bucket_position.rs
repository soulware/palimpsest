//! Bucket-side classification of a `names/<name>` record relative to
//! this coordinator.
//!
//! Mirrors [`crate::volume_state::VolumeLifecycle`] for the on-disk
//! side: a typed enum that lifecycle verbs match on instead of each
//! one open-coding `match record.state` + `record.coordinator_id`
//! comparisons.
//!
//! Two entry points:
//!
//!   - [`OwnershipPosition::classify`] — pure, takes an already-read
//!     `NameRecord`. Used in tests and after the verb has already
//!     read the record for other reasons.
//!   - [`fetch_position`] — async, reads `names/<name>` from S3 and
//!     classifies. Returns the record + version alongside the
//!     position so verbs that need additional fields (`size`,
//!     `parent`, `claimed_at`, …) don't double-read.
//!
//! Wire compatibility: [`OwnershipPosition::to_eligibility`] produces
//! the `Eligibility` wire enum used by `volume_status_remote`.
//! `Eligibility` stays the wire-only surface; this richer type is for
//! verb dispatch.

use elide_core::name_record::{NameRecord, NameState};
use object_store::UpdateVersion;
use ulid::Ulid;

use crate::eligibility::Eligibility;
use crate::name_store::{NameStoreError, read_name_record};

/// This coordinator's relationship to a `names/<name>` record.
///
/// Variant precedence is determined by [`NameState`] and ownership:
///
///   - `Absent` — no record in the bucket.
///   - `OwnedByUs` — record exists, `state ∈ {Live, Stopped}`,
///     `coordinator_id` matches our id.
///   - `OwnedByOther` — record exists, `state ∈ {Live, Stopped}`,
///     `coordinator_id` is set to someone else (or unset — see note).
///   - `Released` — record exists, `state == Released`. No owner.
///     `handoff_snapshot` is the basis for the next claimant.
///   - `Readonly` — record exists, `state == Readonly`. No exclusive
///     owner.
///
/// Note: a `Live`/`Stopped` record with `coordinator_id = None`
/// (legacy / pre-ownership records) classifies as `OwnedByOther`
/// with `coord_id = "<unknown>"`. Matches [`Eligibility::Foreign`]'s
/// safety-first stance — a host that did not write the record
/// cannot claim implicit ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipPosition {
    /// No `names/<name>` record in the bucket.
    Absent,
    /// Owned by this coordinator. Both `Live` and `Stopped` are
    /// covered — both retain ownership. The `state` sub-field
    /// distinguishes them for verbs that care.
    OwnedByUs { vol_ulid: Ulid, state: OwnedState },
    /// Owned by another coordinator (or a legacy record with no
    /// owner field at all).
    OwnedByOther {
        vol_ulid: Ulid,
        coord_id: String,
        state: OwnedState,
    },
    /// Released. No owner. Any coordinator may `volume claim` to
    /// claim. `handoff_snapshot` is the basis the next claimant
    /// will fork from.
    Released {
        vol_ulid: Ulid,
        handoff_snapshot: Option<Ulid>,
    },
    /// Readonly handle (immutable content, e.g. imported OCI
    /// image). No exclusive owner; multiple coordinators may pull
    /// and serve the same name concurrently. Lifecycle verbs
    /// refuse this state.
    Readonly { vol_ulid: Ulid },
}

/// Sub-state of `OwnedByUs` / `OwnedByOther`. Both variants retain
/// ownership in the bucket sense; this distinguishes whether the
/// owning coordinator currently has an active daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedState {
    /// `NameState::Live` — owner has a daemon serving.
    Live,
    /// `NameState::Stopped` — owner has the name but daemon is
    /// down. Cross-host claim requires `--force`.
    Stopped,
}

/// Sentinel coord_id used when a `Live`/`Stopped` record has no
/// `coordinator_id` field. Matches the wire shape `volume_status`
/// already uses for the same case.
pub const UNKNOWN_COORD_ID: &str = "<unknown>";

impl OwnershipPosition {
    /// Classify a (possibly-absent) bucket record against this
    /// coordinator's id. Pure — no I/O.
    pub fn classify(record: Option<&NameRecord>, our_coord_id: &str) -> Self {
        let Some(rec) = record else {
            return Self::Absent;
        };
        match rec.state {
            NameState::Live | NameState::Stopped => {
                let state = match rec.state {
                    NameState::Live => OwnedState::Live,
                    NameState::Stopped => OwnedState::Stopped,
                    _ => unreachable!("matched Live | Stopped"),
                };
                match rec.coordinator_id.as_deref() {
                    Some(owner) if owner == our_coord_id => Self::OwnedByUs {
                        vol_ulid: rec.vol_ulid,
                        state,
                    },
                    Some(owner) => Self::OwnedByOther {
                        vol_ulid: rec.vol_ulid,
                        coord_id: owner.to_owned(),
                        state,
                    },
                    None => Self::OwnedByOther {
                        vol_ulid: rec.vol_ulid,
                        coord_id: UNKNOWN_COORD_ID.to_owned(),
                        state,
                    },
                }
            }
            NameState::Released => Self::Released {
                vol_ulid: rec.vol_ulid,
                handoff_snapshot: rec.handoff_snapshot,
            },
            NameState::Readonly => Self::Readonly {
                vol_ulid: rec.vol_ulid,
            },
        }
    }

    /// Project to the wire-format [`Eligibility`] used by the
    /// `volume_status_remote` IPC reply.
    pub fn to_eligibility(&self) -> Option<Eligibility> {
        match self {
            Self::Absent => None,
            Self::OwnedByUs { .. } => Some(Eligibility::Owned),
            Self::OwnedByOther { .. } => Some(Eligibility::Foreign),
            Self::Released { .. } => Some(Eligibility::ReleasedClaimable),
            Self::Readonly { .. } => Some(Eligibility::Readonly),
        }
    }

    /// Bucket `vol_ulid`, if a record exists.
    pub fn vol_ulid(&self) -> Option<Ulid> {
        match self {
            Self::Absent => None,
            Self::OwnedByUs { vol_ulid, .. }
            | Self::OwnedByOther { vol_ulid, .. }
            | Self::Released { vol_ulid, .. }
            | Self::Readonly { vol_ulid } => Some(*vol_ulid),
        }
    }

    /// True when this coordinator holds the name.
    pub fn is_ours(&self) -> bool {
        matches!(self, Self::OwnedByUs { .. })
    }

    /// True when the bucket record retains an owner (Live or
    /// Stopped) regardless of which coordinator. Used by code
    /// paths (e.g. breadcrumb maintenance) that care about
    /// "retains ownership" rather than "is ours".
    pub fn retains_ownership(&self) -> bool {
        matches!(self, Self::OwnedByUs { .. } | Self::OwnedByOther { .. })
    }
}

/// Fetch `names/<name>` and classify it against this coordinator
/// in one step. Returns the position alongside the raw record +
/// version — verbs that need additional fields (`size`, `parent`,
/// the `UpdateVersion` for a subsequent conditional write) take
/// them from there.
///
/// A missing record produces `(Absent, None)`; an S3 read error
/// surfaces as [`NameStoreError`].
pub async fn fetch_position(
    store: &dyn crate::stores::ReadStore,
    name: &str,
    our_coord_id: &str,
) -> Result<(OwnershipPosition, Option<(NameRecord, UpdateVersion)>), NameStoreError> {
    let record = read_name_record(store, name).await?;
    let position = OwnershipPosition::classify(record.as_ref().map(|(r, _)| r), our_coord_id);
    Ok((position, record))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(state: NameState, owner: Option<&str>, vol_ulid: Ulid) -> NameRecord {
        NameRecord {
            version: NameRecord::CURRENT_VERSION,
            vol_ulid,
            size: 4 * 1024 * 1024 * 1024,
            coordinator_id: owner.map(str::to_owned),
            state,
            parent: None,
            claimed_at: None,
            hostname: None,
            handoff_snapshot: None,
        }
    }

    #[test]
    fn absent_when_record_is_none() {
        assert_eq!(
            OwnershipPosition::classify(None, "me"),
            OwnershipPosition::Absent
        );
    }

    #[test]
    fn live_owned_by_us_classifies_as_owned_by_us_live() {
        let vol = Ulid::new();
        let r = record(NameState::Live, Some("me"), vol);
        assert_eq!(
            OwnershipPosition::classify(Some(&r), "me"),
            OwnershipPosition::OwnedByUs {
                vol_ulid: vol,
                state: OwnedState::Live
            }
        );
    }

    #[test]
    fn stopped_owned_by_us_classifies_as_owned_by_us_stopped() {
        let vol = Ulid::new();
        let r = record(NameState::Stopped, Some("me"), vol);
        assert_eq!(
            OwnershipPosition::classify(Some(&r), "me"),
            OwnershipPosition::OwnedByUs {
                vol_ulid: vol,
                state: OwnedState::Stopped
            }
        );
    }

    #[test]
    fn live_held_by_other_classifies_as_owned_by_other() {
        let vol = Ulid::new();
        let r = record(NameState::Live, Some("them"), vol);
        match OwnershipPosition::classify(Some(&r), "me") {
            OwnershipPosition::OwnedByOther {
                vol_ulid,
                coord_id,
                state,
            } => {
                assert_eq!(vol_ulid, vol);
                assert_eq!(coord_id, "them");
                assert_eq!(state, OwnedState::Live);
            }
            other => panic!("expected OwnedByOther, got {other:?}"),
        }
    }

    #[test]
    fn live_with_no_owner_classifies_as_other_unknown() {
        // Phase 1 records have no `coordinator_id`. Safety-first:
        // anyone-not-us is OwnedByOther. The sentinel `<unknown>`
        // surfaces in operator messages.
        let r = record(NameState::Live, None, Ulid::new());
        match OwnershipPosition::classify(Some(&r), "me") {
            OwnershipPosition::OwnedByOther { coord_id, .. } => {
                assert_eq!(coord_id, UNKNOWN_COORD_ID);
            }
            other => panic!("expected OwnedByOther <unknown>, got {other:?}"),
        }
    }

    #[test]
    fn released_classifies_as_released_with_handoff() {
        let vol = Ulid::new();
        let handoff = Ulid::new();
        let mut r = record(NameState::Released, None, vol);
        r.handoff_snapshot = Some(handoff);
        assert_eq!(
            OwnershipPosition::classify(Some(&r), "me"),
            OwnershipPosition::Released {
                vol_ulid: vol,
                handoff_snapshot: Some(handoff),
            }
        );
    }

    #[test]
    fn released_with_no_handoff_classifies_as_released_none() {
        let r = record(NameState::Released, None, Ulid::new());
        match OwnershipPosition::classify(Some(&r), "me") {
            OwnershipPosition::Released {
                handoff_snapshot, ..
            } => {
                assert!(handoff_snapshot.is_none());
            }
            other => panic!("expected Released, got {other:?}"),
        }
    }

    #[test]
    fn readonly_classifies_as_readonly() {
        let vol = Ulid::new();
        let r = record(NameState::Readonly, None, vol);
        assert_eq!(
            OwnershipPosition::classify(Some(&r), "me"),
            OwnershipPosition::Readonly { vol_ulid: vol }
        );
    }

    #[test]
    fn to_eligibility_round_trips_each_variant() {
        let vol = Ulid::new();
        assert_eq!(OwnershipPosition::Absent.to_eligibility(), None);
        assert_eq!(
            OwnershipPosition::OwnedByUs {
                vol_ulid: vol,
                state: OwnedState::Live
            }
            .to_eligibility(),
            Some(Eligibility::Owned)
        );
        assert_eq!(
            OwnershipPosition::OwnedByOther {
                vol_ulid: vol,
                coord_id: "them".into(),
                state: OwnedState::Stopped
            }
            .to_eligibility(),
            Some(Eligibility::Foreign)
        );
        assert_eq!(
            OwnershipPosition::Released {
                vol_ulid: vol,
                handoff_snapshot: None
            }
            .to_eligibility(),
            Some(Eligibility::ReleasedClaimable)
        );
        assert_eq!(
            OwnershipPosition::Readonly { vol_ulid: vol }.to_eligibility(),
            Some(Eligibility::Readonly)
        );
    }

    #[test]
    fn is_ours_only_true_for_owned_by_us() {
        let vol = Ulid::new();
        assert!(
            OwnershipPosition::OwnedByUs {
                vol_ulid: vol,
                state: OwnedState::Live
            }
            .is_ours()
        );
        assert!(!OwnershipPosition::Absent.is_ours());
        assert!(
            !OwnershipPosition::OwnedByOther {
                vol_ulid: vol,
                coord_id: "x".into(),
                state: OwnedState::Live
            }
            .is_ours()
        );
        assert!(
            !OwnershipPosition::Released {
                vol_ulid: vol,
                handoff_snapshot: None
            }
            .is_ours()
        );
        assert!(!OwnershipPosition::Readonly { vol_ulid: vol }.is_ours());
    }

    #[test]
    fn retains_ownership_covers_owned_arms_only() {
        let vol = Ulid::new();
        assert!(
            OwnershipPosition::OwnedByUs {
                vol_ulid: vol,
                state: OwnedState::Live
            }
            .retains_ownership()
        );
        assert!(
            OwnershipPosition::OwnedByOther {
                vol_ulid: vol,
                coord_id: "x".into(),
                state: OwnedState::Live
            }
            .retains_ownership()
        );
        assert!(!OwnershipPosition::Absent.retains_ownership());
        assert!(
            !OwnershipPosition::Released {
                vol_ulid: vol,
                handoff_snapshot: None
            }
            .retains_ownership()
        );
        assert!(!OwnershipPosition::Readonly { vol_ulid: vol }.retains_ownership());
    }

    #[test]
    fn vol_ulid_returned_for_every_present_variant() {
        let vol = Ulid::new();
        assert_eq!(OwnershipPosition::Absent.vol_ulid(), None);
        for p in [
            OwnershipPosition::OwnedByUs {
                vol_ulid: vol,
                state: OwnedState::Live,
            },
            OwnershipPosition::OwnedByOther {
                vol_ulid: vol,
                coord_id: "x".into(),
                state: OwnedState::Live,
            },
            OwnershipPosition::Released {
                vol_ulid: vol,
                handoff_snapshot: None,
            },
            OwnershipPosition::Readonly { vol_ulid: vol },
        ] {
            assert_eq!(p.vol_ulid(), Some(vol));
        }
    }
}
