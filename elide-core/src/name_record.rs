//! `names/<name>` record format for portable live volumes.
//!
//! See `docs/design-portable-live-volume.md`. This record is stored at
//! `names/<name>` in the bucket and is the authoritative mapping from a
//! human-readable volume name to the currently-active fork, the
//! coordinator that owns it, and the lifecycle state.
//!
//! The record is mutated only via S3 conditional PUTs; conditional-write
//! atomicity on this single key carries name uniqueness, ownership
//! transfer, and the explicit-skip semantics of `--force`.
//!
//! # Wire format
//!
//! TOML, content-type `application/toml; charset=utf-8`. Optional fields
//! are omitted on serialise via `skip_serializing_if = "Option::is_none"`.
//!
//! ## Phase 1 writer output (`NameRecord::live_minimal(vol_ulid)`)
//!
//! ```toml
//! version = 1
//! vol_ulid = "01J0000000000000000000000V"
//! state = "live"
//! ```
//!
//! Three lines. `coordinator_id`, `parent`, `claimed_at`, `hostname`
//! are all `None` and skipped.
//!
//! ## Phase 2 fully-populated record (after lifecycle verbs land)
//!
//! ```toml
//! version = 1
//! vol_ulid = "01J0000000000000000000000V"
//! coordinator_id = "01ABCDEFGHJKMNPQRSTVWXYZ23"
//! state = "released"
//! parent = "01XYZ000000000000000000000/01SNP000000000000000000000"
//! claimed_at = "2026-04-27T12:34:56Z"
//! hostname = "host-a"
//! handoff_snapshot = "01HND0FF000000000000000000"
//! ```
//!
//! # Field semantics
//!
//! - `version` — schema version. Always `1` for this build. `from_toml`
//!   rejects unknown values; schema changes are fresh-bucket-only.
//! - `vol_ulid` — ULID of the fork currently bound to this name.
//!   Crockford-Base32; round-trips through `ulid::Ulid` directly via
//!   the `serde` feature on the `ulid` crate.
//! - `coordinator_id` — derived from the coordinator's Ed25519 public key
//!   via `blake3::derive_key("elide coordinator-id v1", &pub_bytes)`,
//!   formatted as a 26-char Crockford-Base32 ULID-shape (see
//!   `elide_coordinator::portable::format_coordinator_id`). Owner when
//!   `state ∈ {Live, Stopped}`; *intended claimer* when
//!   `state = Reserved`; cleared when `state ∈ {Released, Readonly}`.
//! - `state` — `"live"` | `"stopped"` | `"released"` | `"reserved"` |
//!   `"readonly"` on the wire (lowercase). See § "Five states, three
//!   intents" in the design doc.
//! - `parent` — `"<prev_vol_ulid>/<prev_snap_ulid>"`, the handoff
//!   snapshot the current fork was minted from. Present on forks born
//!   from a released ancestor; absent on root volumes.
//! - `claimed_at` — RFC3339, when the current claim episode began
//!   (set by `mark_initial` and `mark_claimed`; cleared by
//!   `mark_released` so a `Released` record carries no claim time).
//! - `hostname` — advisory only; never compared for ownership decisions.
//! - `handoff_snapshot` — ULID of the most recently published handoff
//!   snapshot. Set when state transitions to `Released`; the next
//!   coordinator claiming the name forks from
//!   `<vol_ulid>/<handoff_snapshot>`.

use std::fmt;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Best-effort hostname lookup for `NameRecord.hostname`.
///
/// Returns `None` if the system call fails or the result is not valid
/// UTF-8. Hostname is advisory metadata only; never compared for
/// ownership decisions.
pub fn current_hostname() -> Option<String> {
    nix::unistd::gethostname()
        .ok()
        .and_then(|h| h.into_string().ok())
}

/// Lifecycle state of a named volume.
///
/// See the design doc § "Five states, three intents" for the operator
/// model behind these values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NameState {
    /// Held by `coordinator_id`; daemon serving.
    #[default]
    Live,
    /// Held by `coordinator_id`; daemon down on this host. Other
    /// coordinators cannot claim without `--force`.
    Stopped,
    /// No current owner. Any coordinator may `volume start` to claim.
    /// `coordinator_id`, `claimed_at`, and `hostname` are cleared on
    /// release so the record's populated fields match the state.
    Released,
    /// Released to a specific coordinator (`volume release --to <X>`).
    /// `coordinator_id` carries the *intended claimer* (X), not a
    /// current owner; `claimed_at` and `hostname` stay empty until X
    /// claims via `mark_claimed`. Only X may transition this record
    /// to `Live`; other coordinators are refused before the
    /// conditional PUT, closing the post-release race window. The
    /// previous owner has already drained and published a handoff
    /// snapshot, exactly as for `Released`.
    Reserved,
    /// Name points at immutable content (e.g. an imported OCI image).
    /// No exclusive owner; multiple coordinators may pull and serve
    /// the same name concurrently. Lifecycle verbs (`stop` / `release`
    /// / `start`) all refuse this state. See design doc § "Readonly
    /// names".
    Readonly,
}

/// Record stored at `names/<name>` in the bucket.
///
/// All cross-host ownership transfer goes through conditional PUTs on
/// this record. Phase 1 of the portable-live-volume work establishes
/// the schema; Phase 2 wires the lifecycle verbs that mutate it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameRecord {
    /// Schema version. Bumped on fresh-bucket-only breaking changes.
    pub version: u32,

    /// ULID of the fork currently bound to this name.
    pub vol_ulid: Ulid,

    /// Coordinator currently holding the name (when `state` is `Live`
    /// or `Stopped`), or the most recent owner (when `state` is
    /// `Released`). `None` for records written by Phase 1 before the
    /// coordinator-id plumbing lands in Phase 2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator_id: Option<String>,

    /// Lifecycle state.
    #[serde(default)]
    pub state: NameState,

    /// Parent pin for forks minted by `volume start` against a
    /// released ancestor. Format: `<prev_vol_ulid>/<prev_snap_ulid>`.
    /// Absent on root volumes (those created by `volume create` not
    /// by `volume start`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,

    /// When the current claim episode began, RFC3339. Set by
    /// `mark_initial` and `mark_claimed`; cleared by `mark_released`.
    /// `None` for Phase 1 records (legacy) and for `Released` records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<String>,

    /// Hostname recorded at claim time. Advisory only — never
    /// compared for ownership decisions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    /// ULID of the most recently published handoff snapshot for this
    /// name. Set when state transitions to `Released` so the next
    /// claimant knows which snapshot to fork from. May also be set on
    /// `Live`/`Stopped` records as a reference to the last published
    /// release point. The next coordinator claiming a `Released` name
    /// forks from `<vol_ulid>/<handoff_snapshot>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff_snapshot: Option<Ulid>,
}

impl NameRecord {
    pub const CURRENT_VERSION: u32 = 1;

    /// Create a record naming a single fork as live, with no
    /// coordinator-specific metadata. Suitable for Phase 1 writers
    /// (e.g. `upload_volume_metadata`) that don't yet have access to
    /// `coordinator_id` or hostname.
    pub fn live_minimal(vol_ulid: Ulid) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            vol_ulid,
            coordinator_id: None,
            state: NameState::Live,
            parent: None,
            claimed_at: None,
            hostname: None,
            handoff_snapshot: None,
        }
    }

    /// Serialise as TOML.
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string(self)
    }

    /// Parse from TOML, rejecting unknown schema versions.
    pub fn from_toml(s: &str) -> Result<Self, ParseNameRecordError> {
        let record: NameRecord = toml::from_str(s).map_err(ParseNameRecordError::Toml)?;
        if record.version != Self::CURRENT_VERSION {
            return Err(ParseNameRecordError::UnsupportedVersion(record.version));
        }
        Ok(record)
    }
}

/// Errors from `NameRecord::from_toml`.
#[derive(Debug)]
pub enum ParseNameRecordError {
    /// The TOML body did not parse against the `NameRecord` schema.
    Toml(toml::de::Error),
    /// The record carries a `version` this build does not understand.
    /// Bucket schema changes are fresh-bucket-only.
    UnsupportedVersion(u32),
}

impl fmt::Display for ParseNameRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Toml(e) => write!(f, "{e}"),
            Self::UnsupportedVersion(v) => write!(
                f,
                "unsupported NameRecord version {v} (this build supports {})",
                NameRecord::CURRENT_VERSION,
            ),
        }
    }
}

impl std::error::Error for ParseNameRecordError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Toml(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ulid() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    #[test]
    fn round_trip_minimal() {
        let r = NameRecord::live_minimal(sample_ulid());
        let toml = r.to_toml().unwrap();
        let parsed = NameRecord::from_toml(&toml).unwrap();
        assert_eq!(parsed.version, NameRecord::CURRENT_VERSION);
        assert_eq!(parsed.vol_ulid, sample_ulid());
        assert_eq!(parsed.state, NameState::Live);
        assert!(parsed.coordinator_id.is_none());
        assert!(parsed.parent.is_none());
    }

    #[test]
    fn round_trip_full() {
        let snap = Ulid::from_string("01J1111111111111111111111V").unwrap();
        let r = NameRecord {
            version: NameRecord::CURRENT_VERSION,
            vol_ulid: sample_ulid(),
            coordinator_id: Some("01ABCDEFGHJKMNPQRSTVWXYZ23".to_string()),
            state: NameState::Released,
            parent: Some("01XYZ.../01ABC...".to_string()),
            claimed_at: Some("2026-04-27T12:34:56Z".to_string()),
            hostname: Some("host-a".to_string()),
            handoff_snapshot: Some(snap),
        };
        let toml = r.to_toml().unwrap();
        let parsed = NameRecord::from_toml(&toml).unwrap();
        assert_eq!(parsed.coordinator_id, r.coordinator_id);
        assert_eq!(parsed.state, NameState::Released);
        assert_eq!(parsed.parent, r.parent);
        assert_eq!(parsed.claimed_at, r.claimed_at);
        assert_eq!(parsed.hostname, r.hostname);
        assert_eq!(parsed.handoff_snapshot, Some(snap));
    }

    #[test]
    fn each_state_round_trips() {
        for state in [
            NameState::Live,
            NameState::Stopped,
            NameState::Released,
            NameState::Reserved,
            NameState::Readonly,
        ] {
            let r = NameRecord {
                state,
                ..NameRecord::live_minimal(sample_ulid())
            };
            let toml = r.to_toml().unwrap();
            let parsed = NameRecord::from_toml(&toml).unwrap();
            assert_eq!(parsed.state, state);
        }
    }

    #[test]
    fn rejects_unknown_version() {
        let toml = r#"
version = 999
vol_ulid = "01J0000000000000000000000V"
"#;
        let err = NameRecord::from_toml(toml).expect_err("unknown version must fail");
        assert!(matches!(err, ParseNameRecordError::UnsupportedVersion(999)));
    }

    #[test]
    fn rejects_malformed_toml() {
        let err =
            NameRecord::from_toml("this is not toml = = =").expect_err("malformed TOML must fail");
        assert!(matches!(err, ParseNameRecordError::Toml(_)));
    }

    #[test]
    fn rejects_missing_required_fields() {
        // `vol_ulid` is required.
        let toml = "version = 1\n";
        assert!(NameRecord::from_toml(toml).is_err());
    }

    #[test]
    fn lowercase_state_serialisation() {
        // `state` must be lowercase on the wire to keep TOML readable
        // and stable across Rust enum identifier conventions.
        let r = NameRecord {
            state: NameState::Released,
            ..NameRecord::live_minimal(sample_ulid())
        };
        let toml = r.to_toml().unwrap();
        assert!(
            toml.contains("state = \"released\""),
            "expected lowercase state, got: {toml}"
        );
    }

    #[test]
    fn reserved_serialises_lowercase() {
        let r = NameRecord {
            state: NameState::Reserved,
            ..NameRecord::live_minimal(sample_ulid())
        };
        let toml = r.to_toml().unwrap();
        assert!(
            toml.contains("state = \"reserved\""),
            "expected lowercase reserved, got: {toml}"
        );
    }
}
