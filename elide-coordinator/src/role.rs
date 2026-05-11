//! Role classification: this coordinator's relationship to a named
//! volume, derived purely from the bucket-side [`OwnershipPosition`].
//!
//! Role is the *coarsest* cut of the state space and the one verb
//! dispatch should anchor on. Three values:
//!
//!   - [`Role::None`] — no `names/<name>` record in the bucket.
//!     This coordinator has never encountered the name (or its
//!     record was deleted, which we don't currently do).
//!   - [`Role::Observer`] — record exists but we don't hold it. We
//!     can `fetch` / `pull` / `claim` (if Released) but cannot
//!     `start` / `stop` / `release`.
//!   - [`Role::Owner`] — record exists and `coordinator_id`
//!     matches us. We can run lifecycle verbs against it.
//!
//! Role is **purely bucket-driven**. Whether we happen to have
//! `cache/`, `idx/`, or `by_name/` entries locally is a separate
//! axis ([`crate::volume_state::VolumeLifecycle`]) and does not
//! affect role.
//!
//! ## Why a separate type from `OwnershipPosition`
//!
//! [`OwnershipPosition`] is the wire-grade classifier — it carries
//! `vol_ulid`, `coord_id`, `handoff_snapshot` payloads that some
//! verbs need. `Role` is the *dispatch* classifier — strips the
//! payloads we don't need at the gate so the variant set matches the
//! conceptual role model (three top-level states, Observer split by
//! kind, Owner split by Live/Stopped). Future PRs will let role-typed
//! verbs take a `Role` directly and refer to the full
//! `OwnershipPosition` when they need the payload.
//!
//! ## Role transitions
//!
//! | From                    | To                      | Verb                                  |
//! |-------------------------|-------------------------|---------------------------------------|
//! | `None`                  | `Owner { Stopped }`     | `create`                              |
//! | `None`                  | `Observer { * }`        | `fetch`, `pull`, `import`             |
//! | `Observer { Released }` | `Owner { Stopped }`     | `claim`                               |
//! | `Owner { Stopped }`     | `Owner { Live }`        | `start`                               |
//! | `Owner { Live }`        | `Owner { Stopped }`     | `stop`                                |
//! | `Owner { Stopped }`     | `Observer { Released }` | `release`                             |
//! | `Owner { * }`           | `Observer { Released }` | `release --force` (self)              |
//! | `Observer { Foreign }`  | `Observer { Released }` | `release --force` (other; the target  |
//! |                         |                         | host's role transitions too)          |
//!
//! All transitions are verb-driven except one external interrupt:
//! an external `release --force` from another coordinator flips us
//! from `Owner` → `Observer { Released }` involuntarily —
//! discovered lazily on the next bucket read.

use ulid::Ulid;

use crate::bucket_position::{OwnedState, OwnershipPosition};

/// Three top-level role values. Owner and Observer carry sub-state
/// payloads that verb dispatch frequently discriminates on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// No `names/<name>` record in the bucket. The coordinator
    /// has no role with respect to this name.
    None,
    /// A record exists; this coordinator is not the named owner.
    /// `kind` distinguishes the three observer sub-states for
    /// verb dispatch.
    Observer { kind: ObserverKind },
    /// A record exists; this coordinator is the named owner.
    /// `state` distinguishes whether a daemon is supposed to be
    /// running (`Live`) or parked (`Stopped`).
    Owner { state: OwnedState },
}

/// Sub-state of [`Role::Observer`]. Verb dispatch on observer
/// verbs (notably `claim`) discriminates on these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserverKind {
    /// Another coordinator holds the name (`OwnedByOther` in the
    /// bucket). `coord_id` is `"<unknown>"` for legacy records
    /// that have no `coordinator_id` field — see
    /// [`OwnershipPosition::OwnedByOther`].
    Foreign { coord_id: String },
    /// No owner; any coordinator may `claim`. `handoff` is the
    /// basis snapshot the next claimant forks from. `None` only
    /// for malformed records; well-formed Released records always
    /// carry a handoff.
    Released { handoff: Option<Ulid> },
    /// Readonly handle (e.g. an imported OCI image). No exclusive
    /// owner; multiple coordinators may `pull` and serve the same
    /// name concurrently.
    Readonly,
}

impl Role {
    /// Project an [`OwnershipPosition`] onto its role. Pure;
    /// allocates only the `Foreign.coord_id` String.
    pub fn from_position(position: &OwnershipPosition) -> Self {
        match position {
            OwnershipPosition::Absent => Self::None,
            OwnershipPosition::OwnedByUs { state, .. } => Self::Owner { state: *state },
            OwnershipPosition::OwnedByOther { coord_id, .. } => Self::Observer {
                kind: ObserverKind::Foreign {
                    coord_id: coord_id.clone(),
                },
            },
            OwnershipPosition::Released {
                handoff_snapshot, ..
            } => Self::Observer {
                kind: ObserverKind::Released {
                    handoff: *handoff_snapshot,
                },
            },
            OwnershipPosition::Readonly { .. } => Self::Observer {
                kind: ObserverKind::Readonly,
            },
        }
    }

    /// True when this coordinator holds the name in the bucket.
    pub fn is_owner(&self) -> bool {
        matches!(self, Self::Owner { .. })
    }

    /// True when a bucket record exists but we don't hold it.
    pub fn is_observer(&self) -> bool {
        matches!(self, Self::Observer { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos_owned_us(state: OwnedState) -> OwnershipPosition {
        OwnershipPosition::OwnedByUs {
            vol_ulid: Ulid::nil(),
            state,
        }
    }

    fn pos_owned_other(coord_id: &str) -> OwnershipPosition {
        OwnershipPosition::OwnedByOther {
            vol_ulid: Ulid::nil(),
            coord_id: coord_id.to_owned(),
            state: OwnedState::Live,
        }
    }

    fn pos_released(handoff: Option<Ulid>) -> OwnershipPosition {
        OwnershipPosition::Released {
            vol_ulid: Ulid::nil(),
            handoff_snapshot: handoff,
        }
    }

    fn pos_readonly() -> OwnershipPosition {
        OwnershipPosition::Readonly {
            vol_ulid: Ulid::nil(),
        }
    }

    #[test]
    fn absent_position_projects_to_role_none() {
        assert_eq!(Role::from_position(&OwnershipPosition::Absent), Role::None);
    }

    #[test]
    fn owned_by_us_projects_to_owner_with_state() {
        assert_eq!(
            Role::from_position(&pos_owned_us(OwnedState::Live)),
            Role::Owner {
                state: OwnedState::Live
            }
        );
        assert_eq!(
            Role::from_position(&pos_owned_us(OwnedState::Stopped)),
            Role::Owner {
                state: OwnedState::Stopped
            }
        );
    }

    #[test]
    fn owned_by_other_projects_to_observer_foreign() {
        match Role::from_position(&pos_owned_other("them")) {
            Role::Observer {
                kind: ObserverKind::Foreign { coord_id },
            } => {
                assert_eq!(coord_id, "them");
            }
            other => panic!("expected Observer Foreign, got {other:?}"),
        }
    }

    #[test]
    fn released_projects_to_observer_released_with_handoff() {
        let handoff = Ulid::new();
        match Role::from_position(&pos_released(Some(handoff))) {
            Role::Observer {
                kind: ObserverKind::Released { handoff: got },
            } => {
                assert_eq!(got, Some(handoff));
            }
            other => panic!("expected Observer Released, got {other:?}"),
        }
    }

    #[test]
    fn released_with_no_handoff_projects_to_observer_released_none() {
        match Role::from_position(&pos_released(None)) {
            Role::Observer {
                kind: ObserverKind::Released { handoff: None },
            } => {}
            other => panic!("expected Observer Released None, got {other:?}"),
        }
    }

    #[test]
    fn readonly_projects_to_observer_readonly() {
        assert_eq!(
            Role::from_position(&pos_readonly()),
            Role::Observer {
                kind: ObserverKind::Readonly
            }
        );
    }

    #[test]
    fn role_predicates_distinguish_the_two_role_arms() {
        let cases = [
            (Role::None, (false, false)),
            (
                Role::Observer {
                    kind: ObserverKind::Readonly,
                },
                (false, true),
            ),
            (
                Role::Owner {
                    state: OwnedState::Live,
                },
                (true, false),
            ),
        ];
        for (role, (owner, observer)) in cases {
            assert_eq!(role.is_owner(), owner, "is_owner({role:?})");
            assert_eq!(role.is_observer(), observer, "is_observer({role:?})");
        }
    }
}
