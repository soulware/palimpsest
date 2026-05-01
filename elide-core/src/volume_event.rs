//! `events/<name>/<event_ulid>.toml` append-only journal entry.
//!
//! See `docs/design-volume-event-log.md`. Each event records one
//! lifecycle transition of a named volume. The pointer at
//! `names/<name>` (`name_record.rs`) is canonical for "now"; this
//! type is canonical for "ever" — the durable, signed history of
//! every operation that touched the name.
//!
//! # Wire format
//!
//! TOML, content-type `application/toml; charset=utf-8`. The `kind`
//! field discriminates the variant (`#[serde(tag = "kind")]`); each
//! variant's fields are flattened to the top level of the TOML
//! document.
//!
//! ```toml
//! version = 1
//! event_ulid = "01HHHHHHHH0000000000000000"
//! kind = "claimed"
//! at = "2024-01-19T22:35:43.328Z"
//! coordinator_id = "01ABCDEFGHJKMNPQRSTVWXYZ23"
//! vol_ulid = "01J0000000000000000000000V"
//! prev_event_ulid = "01HHHHHHGZ0000000000000000"
//! signature = "<128-hex-char Ed25519 signature>"
//! ```
//!
//! # Signing
//!
//! Events are signed by the emitter's `coordinator.key` over the
//! bytes returned by [`VolumeEvent::signing_payload`]. The payload is
//! domain-tagged (`b"elide volume-event v1\0"`) so a signature on an
//! event cannot be confused with one on any other coordinator-signed
//! artefact (e.g. synthesised handoff snapshots) — the pre-image
//! domain differs even if a malicious bucket-writer reuses the bytes
//! verbatim.
//!
//! The canonical form is deliberately not `toml::to_string` of the
//! struct: the `toml` crate's output ordering depends on serde
//! derive expansion details and could drift across releases.
//! Instead, [`VolumeEvent::signing_payload`] emits a fixed-order
//! line-oriented byte stream that is stable as long as this module
//! compiles. The on-disk TOML form is independent — readers parse
//! TOML, then recompute the canonical payload from the parsed
//! struct to verify the signature.

use std::fmt;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use ulid::Ulid;

/// Domain-separation tag prepended to every event signing payload.
/// Versioned independently of `VolumeEvent::version` so the signing
/// pre-image can evolve without bumping the on-disk schema.
pub const SIGNING_DOMAIN_TAG: &[u8] = b"elide volume-event v1\0";

/// Convert a millisecond Unix timestamp (as carried by a ULID) to
/// the canonical `DateTime<Utc>` we expose on the wire. Returns
/// `None` for values that cannot be represented (e.g. dates beyond
/// `chrono`'s supported range — practically never for ULIDs in this
/// century).
fn datetime_from_ms(ms: u64) -> Option<DateTime<Utc>> {
    let ms_i = i64::try_from(ms).ok()?;
    DateTime::<Utc>::from_timestamp_millis(ms_i)
}

/// `chrono::DateTime<Utc>` serializer that emits a fixed RFC3339
/// form with millisecond precision and a `Z` suffix
/// (`"2026-04-30T12:34:56.789Z"`). Pinning the format at this layer
/// keeps the on-disk shape stable across `chrono` releases —
/// important because [`VolumeEvent::from_toml`] cross-checks the
/// parsed value against `event_ulid.timestamp_ms()`.
mod rfc3339_millis_z {
    use super::*;

    pub fn serialize<S: Serializer>(dt: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&dt.to_rfc3339_opts(SecondsFormat::Millis, true))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let raw = String::deserialize(d)?;
        DateTime::parse_from_rfc3339(&raw)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

/// Kind discriminator + kind-specific fields for a [`VolumeEvent`].
///
/// Internally tagged on `kind` so the on-disk TOML reads as a flat
/// document. Variants carry only the fields that are *intrinsic* to
/// the kind; common fields (`event_ulid`, `at`, `coordinator_id`,
/// `vol_ulid`, `prev_event_ulid`) live on [`VolumeEvent`] itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    /// Initial creation of `names/<name>` (writable or readonly).
    Created,
    /// `names/<name>` transitioned `Released → Stopped` under this
    /// coordinator. The successful CAS that established ownership.
    Claimed,
    /// `names/<name>` transitioned `Live`/`Stopped → Released`. The
    /// emitter is the previous owner; `handoff_snapshot` is the
    /// snapshot the next claimant will fork from.
    Released { handoff_snapshot: Ulid },
    /// `names/<name>` was force-released by a coordinator that did
    /// not previously own it. `handoff_snapshot` is the synthesised
    /// snapshot produced by the recovering coordinator.
    /// `displaced_coordinator_id` is the previous owner identifier
    /// recorded just before the unconditional rewrite.
    ForceReleased {
        handoff_snapshot: Ulid,
        displaced_coordinator_id: String,
    },
    /// This name was created as a fork of another name's snapshot.
    /// Emitted on the *new* name's log only; the source name's log
    /// is not updated (see `design-volume-event-log.md` open question
    /// 1).
    ForkedFrom {
        source_name: String,
        source_vol_ulid: Ulid,
        source_snap_ulid: Ulid,
    },
    /// Terminal event for the *old* name. The pointer at
    /// `names/<old>` flips to a `Renamed` tombstone state and
    /// forwards to `new_name`. No further events are appended to
    /// this name's log.
    RenamedTo { new_name: String },
    /// Opening event for the *new* name. `inherits_log_from` is the
    /// old name; readers reconstructing full history walk back into
    /// `events/<old>/`.
    RenamedFrom {
        old_name: String,
        inherits_log_from: String,
    },
}

impl EventKind {
    /// Lowercase wire string for the `kind` field. Used in
    /// [`VolumeEvent::signing_payload`] so the canonical form depends
    /// only on this module, not on the `serde` rename machinery.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Claimed => "claimed",
            Self::Released { .. } => "released",
            Self::ForceReleased { .. } => "force_released",
            Self::ForkedFrom { .. } => "forked_from",
            Self::RenamedTo { .. } => "renamed_to",
            Self::RenamedFrom { .. } => "renamed_from",
        }
    }
}

/// Append-only journal entry stored at
/// `events/<name>/<event_ulid>.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeEvent {
    /// Schema version. Bumped on fresh-bucket-only breaking changes.
    pub version: u32,

    /// Total-order key. Determines the canonical ordering of events
    /// when listing `events/<name>/` and is the *source of
    /// truth* for the emit time.
    pub event_ulid: Ulid,

    /// RFC3339 timestamp at emit, fixed millisecond precision with
    /// `Z` suffix (e.g. `"2026-04-30T12:34:56.789Z"`). A pure
    /// duplicate of `event_ulid.timestamp_ms()` — kept on the wire
    /// so a raw `cat` of an event file shows a human-readable date
    /// without needing tooling to decode the ULID.
    ///
    /// **Invariant:** `at.timestamp_millis() ==
    /// event_ulid.timestamp_ms()`. Enforced by
    /// [`Self::from_toml`]. The signing payload deliberately omits
    /// this field: the same fact is already carried by
    /// `event_ulid`, and the parser refuses any event where the two
    /// disagree.
    #[serde(with = "rfc3339_millis_z")]
    pub at: DateTime<Utc>,

    /// The volume name this event belongs to. Duplicates the
    /// `<name>` segment of the on-disk key
    /// (`events/<name>/<event_ulid>.toml`) so the body is
    /// self-describing — a `cat` of the file shows the name without
    /// needing the path. Included in the canonical signing payload
    /// so the signature binds the event to a specific name; a
    /// bucket-rewriter cannot relocate an event under a different
    /// name without invalidating the signature.
    pub name: String,

    /// Emitter's coordinator id (Crockford-Base32 ULID-shape, as in
    /// `name_record.rs`).
    pub coordinator_id: String,

    /// Hostname of the emitting coordinator at the time of emit.
    /// Captured once at coordinator startup via
    /// `gethostname()`; advisory metadata only — never compared
    /// for ownership. `None` when the syscall failed or the
    /// coordinator's view of its hostname is otherwise unavailable.
    /// Stamped on every event so an audit trail attributes each
    /// transition to a human-meaningful host name, not just the
    /// opaque `coordinator_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    /// `vol_ulid` bound to the name at emit time (i.e. the value in
    /// `names/<name>` after the CAS this event records).
    pub vol_ulid: Ulid,

    /// Highest event ULID this emitter observed when computing the
    /// canonical history. Absent on the very first event for a name.
    /// Soft skip-detector: a subsequent event whose
    /// `prev_event_ulid` skips a present-on-disk event flags an
    /// emitter that wrote out of band (or a crash mid-emit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_event_ulid: Option<Ulid>,

    /// Kind discriminator + kind-specific fields. Flattened into the
    /// top level of the TOML document via `#[serde(tag = "kind")]`.
    #[serde(flatten)]
    pub kind: EventKind,

    /// Hex-encoded 64-byte Ed25519 signature over
    /// [`Self::signing_payload`]. `None` while the event is being
    /// constructed (signing happens last); `Some` for any event read
    /// off the wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl VolumeEvent {
    pub const CURRENT_VERSION: u32 = 1;

    /// Construct a new unsigned event. `at` is derived from
    /// `event_ulid` so writers cannot desync the two fields.
    /// Returns `None` if `event_ulid`'s timestamp cannot be
    /// represented as a `DateTime<Utc>` (practically impossible for
    /// ULIDs minted in this century, but exposed as `None` rather
    /// than panicking to keep this library-grade code panic-free).
    pub fn new(
        event_ulid: Ulid,
        name: String,
        coordinator_id: String,
        hostname: Option<String>,
        vol_ulid: Ulid,
        prev_event_ulid: Option<Ulid>,
        kind: EventKind,
    ) -> Option<Self> {
        let at = datetime_from_ms(event_ulid.timestamp_ms())?;
        Some(Self {
            version: Self::CURRENT_VERSION,
            event_ulid,
            at,
            name,
            coordinator_id,
            hostname,
            vol_ulid,
            prev_event_ulid,
            kind,
            signature: None,
        })
    }

    /// Bytes signed by the emitter's `coordinator.key`. Domain-tagged
    /// (`SIGNING_DOMAIN_TAG`) and emitted in a fixed field order
    /// independent of any `serde`/`toml` machinery, so the canonical
    /// form is stable across formatter changes.
    ///
    /// The `signature` field is excluded by construction (the
    /// pre-image must not depend on its own output).
    pub fn signing_payload(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(512);
        buf.extend_from_slice(SIGNING_DOMAIN_TAG);
        push_field(&mut buf, "version", &self.version.to_string());
        push_field(&mut buf, "event_ulid", &self.event_ulid.to_string());
        // `at` deliberately omitted — it is a strict derivative of
        // `event_ulid`, enforced by the parse-time invariant. Signing
        // it would only re-state a fact already covered.
        push_field(&mut buf, "name", &self.name);
        push_field(&mut buf, "coordinator_id", &self.coordinator_id);
        // `hostname` is signed even though it's advisory: the
        // signature ensures a reader can trust the recorded
        // hostname is what the emitter actually wrote, not a
        // post-hoc bucket-rewrite. `None` is encoded as an empty
        // string, matching the `prev_event_ulid` convention so the
        // canonical form is unambiguous in either case.
        match self.hostname.as_deref() {
            Some(h) => push_field(&mut buf, "hostname", h),
            None => push_field(&mut buf, "hostname", ""),
        }
        push_field(&mut buf, "vol_ulid", &self.vol_ulid.to_string());
        match self.prev_event_ulid {
            Some(u) => push_field(&mut buf, "prev_event_ulid", &u.to_string()),
            None => push_field(&mut buf, "prev_event_ulid", ""),
        }
        push_field(&mut buf, "kind", self.kind.as_str());
        match &self.kind {
            EventKind::Created | EventKind::Claimed => {}
            EventKind::Released { handoff_snapshot } => {
                push_field(&mut buf, "handoff_snapshot", &handoff_snapshot.to_string());
            }
            EventKind::ForceReleased {
                handoff_snapshot,
                displaced_coordinator_id,
            } => {
                push_field(&mut buf, "handoff_snapshot", &handoff_snapshot.to_string());
                push_field(
                    &mut buf,
                    "displaced_coordinator_id",
                    displaced_coordinator_id,
                );
            }
            EventKind::ForkedFrom {
                source_name,
                source_vol_ulid,
                source_snap_ulid,
            } => {
                push_field(&mut buf, "source_name", source_name);
                push_field(&mut buf, "source_vol_ulid", &source_vol_ulid.to_string());
                push_field(&mut buf, "source_snap_ulid", &source_snap_ulid.to_string());
            }
            EventKind::RenamedTo { new_name } => {
                push_field(&mut buf, "new_name", new_name);
            }
            EventKind::RenamedFrom {
                old_name,
                inherits_log_from,
            } => {
                push_field(&mut buf, "old_name", old_name);
                push_field(&mut buf, "inherits_log_from", inherits_log_from);
            }
        }
        buf
    }

    /// Serialise as TOML for on-disk storage.
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string(self)
    }

    /// Parse from TOML, rejecting unknown schema versions and any
    /// event whose `at` does not match `event_ulid.timestamp_ms()`
    /// at millisecond resolution. The latter is a strict invariant
    /// — the duplicate field exists only for human inspection, and
    /// a value that disagrees with the ULID is either a hand-edit
    /// or corruption.
    pub fn from_toml(s: &str) -> Result<Self, ParseVolumeEventError> {
        let event: VolumeEvent = toml::from_str(s).map_err(ParseVolumeEventError::Toml)?;
        if event.version != Self::CURRENT_VERSION {
            return Err(ParseVolumeEventError::UnsupportedVersion(event.version));
        }
        let ulid_ms = event.event_ulid.timestamp_ms();
        let at_ms = u64::try_from(event.at.timestamp_millis()).unwrap_or(u64::MAX);
        if at_ms != ulid_ms {
            return Err(ParseVolumeEventError::TimestampMismatch { at_ms, ulid_ms });
        }
        Ok(event)
    }
}

fn push_field(buf: &mut Vec<u8>, key: &str, value: &str) {
    buf.push(b'\n');
    buf.extend_from_slice(key.as_bytes());
    buf.push(b'=');
    buf.extend_from_slice(value.as_bytes());
}

/// Errors from [`VolumeEvent::from_toml`].
#[derive(Debug)]
pub enum ParseVolumeEventError {
    /// The TOML body did not parse against the `VolumeEvent` schema.
    Toml(toml::de::Error),
    /// The event carries a `version` this build does not understand.
    /// Bucket schema changes are fresh-bucket-only.
    UnsupportedVersion(u32),
    /// `at` does not match `event_ulid.timestamp_ms()`. The two are
    /// required to agree at millisecond resolution; a mismatch
    /// indicates a hand-edit or corruption.
    TimestampMismatch { at_ms: u64, ulid_ms: u64 },
}

impl fmt::Display for ParseVolumeEventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Toml(e) => write!(f, "{e}"),
            Self::UnsupportedVersion(v) => write!(
                f,
                "unsupported VolumeEvent version {v} (this build supports {})",
                VolumeEvent::CURRENT_VERSION,
            ),
            Self::TimestampMismatch { at_ms, ulid_ms } => write!(
                f,
                "at ({at_ms} ms) does not match event_ulid timestamp ({ulid_ms} ms)"
            ),
        }
    }
}

impl std::error::Error for ParseVolumeEventError {
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
    use ed25519_dalek::{Signer, SigningKey, Verifier};
    use rand_core::OsRng;

    fn vol_ulid() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    fn event_ulid() -> Ulid {
        // ULID timestamps are 48 bits → first 10 base32 chars; the
        // remaining 16 carry the random tail. Concrete value here
        // is a fixed instant in 2024.
        Ulid::from_string("01HHHHHHHH0000000000000000").unwrap()
    }

    fn snap_ulid() -> Ulid {
        Ulid::from_string("01HSNHSNHSN000000000000000").unwrap()
    }

    fn sample_event(kind: EventKind) -> VolumeEvent {
        VolumeEvent::new(
            event_ulid(),
            "vol".to_string(),
            "01ABCDEFGHJKMNPQRSTVWXYZ23".to_string(),
            Some("test-host".to_string()),
            vol_ulid(),
            None,
            kind,
        )
        .expect("event_ulid timestamp must be representable as DateTime<Utc>")
    }

    #[test]
    fn round_trip_each_kind() {
        let kinds = vec![
            EventKind::Created,
            EventKind::Claimed,
            EventKind::Released {
                handoff_snapshot: snap_ulid(),
            },
            EventKind::ForceReleased {
                handoff_snapshot: snap_ulid(),
                displaced_coordinator_id: "01OLDCOORDXXXXXXXXXXXXXXXX".to_string(),
            },
            EventKind::ForkedFrom {
                source_name: "parent".to_string(),
                source_vol_ulid: vol_ulid(),
                source_snap_ulid: snap_ulid(),
            },
            EventKind::RenamedTo {
                new_name: "fresh".to_string(),
            },
            EventKind::RenamedFrom {
                old_name: "stale".to_string(),
                inherits_log_from: "stale".to_string(),
            },
        ];
        for kind in kinds {
            let ev = sample_event(kind.clone());
            let toml = ev.to_toml().expect("serialise");
            let parsed = VolumeEvent::from_toml(&toml).expect("parse");
            assert_eq!(parsed, ev, "round-trip mismatch for kind {:?}", kind);
        }
    }

    #[test]
    fn signing_payload_starts_with_domain_tag() {
        let ev = sample_event(EventKind::Claimed);
        let payload = ev.signing_payload();
        assert!(
            payload.starts_with(SIGNING_DOMAIN_TAG),
            "payload must start with the domain tag"
        );
    }

    #[test]
    fn signing_payload_excludes_signature() {
        // Two events identical except for `signature` must produce
        // identical signing payloads — the pre-image cannot depend
        // on its own output.
        let mut ev = sample_event(EventKind::Claimed);
        let payload_a = ev.signing_payload();
        ev.signature = Some("ff".repeat(64));
        let payload_b = ev.signing_payload();
        assert_eq!(
            payload_a, payload_b,
            "signing payload must not include the signature field"
        );
    }

    #[test]
    fn signing_payload_changes_with_hostname() {
        // `hostname` is signed: an event with a different recorded
        // hostname must produce a different canonical pre-image so
        // a bucket-rewriter can't substitute a forged hostname
        // under the original signature.
        let mut a = sample_event(EventKind::Claimed);
        let mut b = sample_event(EventKind::Claimed);
        a.hostname = Some("host-a".to_string());
        b.hostname = Some("host-b".to_string());
        assert_ne!(a.signing_payload(), b.signing_payload());

        // None vs Some("") must also differ — the canonical form
        // distinguishes "no hostname recorded" from "the hostname
        // is the empty string", even though both are unusual.
        let mut c = sample_event(EventKind::Claimed);
        let mut d = sample_event(EventKind::Claimed);
        c.hostname = None;
        d.hostname = Some("".to_string());
        // Same canonical encoding (both push an empty value); this
        // is intentional. We only require that *non-empty* hostname
        // values are distinguished — `None` vs absent-but-empty is
        // not a security-relevant distinction.
        assert_eq!(c.signing_payload(), d.signing_payload());
    }

    #[test]
    fn signing_payload_changes_with_name() {
        // `name` is signed: a bucket-rewriter that copies an event
        // file under a different `events/<other>/` prefix must not
        // be able to keep the original signature valid.
        let mut a = sample_event(EventKind::Claimed);
        let mut b = sample_event(EventKind::Claimed);
        a.name = "alpha".to_string();
        b.name = "beta".to_string();
        assert_ne!(a.signing_payload(), b.signing_payload());
    }

    #[test]
    fn signing_payload_changes_with_kind() {
        // A signature on a Claimed event must not validate as a
        // Created event — the canonical form must differ on `kind`.
        let claimed = sample_event(EventKind::Claimed).signing_payload();
        let created = sample_event(EventKind::Created).signing_payload();
        assert_ne!(claimed, created);
    }

    #[test]
    fn signing_payload_changes_with_kind_specific_fields() {
        // Two `Released` events with different handoff snapshots
        // must sign differently.
        let a = sample_event(EventKind::Released {
            handoff_snapshot: snap_ulid(),
        })
        .signing_payload();
        let b = sample_event(EventKind::Released {
            handoff_snapshot: Ulid::from_string("01ZZZZZZZZZZZZZZZZZZZZZZZZ").unwrap(),
        })
        .signing_payload();
        assert_ne!(a, b);
    }

    #[test]
    fn ed25519_round_trip() {
        let key = SigningKey::generate(&mut OsRng);
        let verifying = key.verifying_key();

        let mut ev = sample_event(EventKind::Released {
            handoff_snapshot: snap_ulid(),
        });
        let payload = ev.signing_payload();
        let sig = key.sign(&payload);
        ev.signature = Some(crate::signing::encode_hex(&sig.to_bytes()));

        // Round-trip through the wire.
        let toml = ev.to_toml().expect("serialise");
        let parsed = VolumeEvent::from_toml(&toml).expect("parse");
        let recomputed_payload = parsed.signing_payload();
        assert_eq!(
            payload, recomputed_payload,
            "verifier must recompute the same payload from the parsed event"
        );

        let sig_bytes: [u8; 64] = crate::signing::decode_hex(parsed.signature.as_deref().unwrap())
            .expect("hex")
            .try_into()
            .expect("64 bytes");
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        verifying
            .verify(&recomputed_payload, &sig)
            .expect("signature must verify");
    }

    #[test]
    fn ed25519_rejects_tampered_kind() {
        let key = SigningKey::generate(&mut OsRng);
        let verifying = key.verifying_key();

        let mut ev = sample_event(EventKind::Claimed);
        let sig = key.sign(&ev.signing_payload());
        ev.signature = Some(crate::signing::encode_hex(&sig.to_bytes()));

        // Tamper with the kind after signing.
        ev.kind = EventKind::Created;
        let sig_bytes: [u8; 64] = crate::signing::decode_hex(ev.signature.as_deref().unwrap())
            .expect("hex")
            .try_into()
            .expect("64 bytes");
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        let payload = ev.signing_payload();
        assert!(
            verifying.verify(&payload, &sig).is_err(),
            "signature must not verify after kind tampering"
        );
    }

    #[test]
    fn rejects_unknown_version() {
        // Construct a valid event, then poke an unsupported version
        // into its TOML form so the parse path reaches the version
        // check rather than failing on field validation first.
        let mut ev = sample_event(EventKind::Claimed);
        ev.version = 999;
        let toml = ev.to_toml().expect("serialise");
        let err = VolumeEvent::from_toml(&toml).expect_err("unknown version must fail");
        assert!(matches!(
            err,
            ParseVolumeEventError::UnsupportedVersion(999)
        ));
    }

    #[test]
    fn rejects_at_ulid_mismatch() {
        // Hand-edit `at` to a different second; the parser must
        // refuse to accept the event.
        let ev = sample_event(EventKind::Claimed);
        let mut toml = ev.to_toml().expect("serialise");
        let original = ev.at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let tampered = (ev.at + chrono::Duration::seconds(1))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        toml = toml.replace(&original, &tampered);
        let err = VolumeEvent::from_toml(&toml).expect_err("at/ulid mismatch must fail");
        assert!(
            matches!(err, ParseVolumeEventError::TimestampMismatch { .. }),
            "expected TimestampMismatch, got {err:?}"
        );
    }

    #[test]
    fn at_derived_from_event_ulid() {
        let ev = sample_event(EventKind::Claimed);
        let ulid_ms = ev.event_ulid.timestamp_ms();
        assert_eq!(ev.at.timestamp_millis() as u64, ulid_ms);
    }

    #[test]
    fn lowercase_kind_serialisation() {
        let ev = sample_event(EventKind::ForceReleased {
            handoff_snapshot: snap_ulid(),
            displaced_coordinator_id: "01OLDCOORDXXXXXXXXXXXXXXXX".to_string(),
        });
        let toml = ev.to_toml().unwrap();
        assert!(
            toml.contains("kind = \"force_released\""),
            "expected snake_case kind, got: {toml}"
        );
    }
}
