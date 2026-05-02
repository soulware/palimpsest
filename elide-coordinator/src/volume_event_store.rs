//! Bucket-level read/write helpers for the per-name event log.
//!
//! Pairs with [`crate::name_store`] (which manages the
//! `names/<name>` pointer object). The event log lives under a
//! separate top-level prefix so the pointer key and the event
//! prefix never collide on the same parent — `aws s3 ls names/`
//! lists names and nothing else; `aws s3 ls events/` lists logs.
//! See `docs/design-volume-event-log.md`.
//!
//! Keys: `events/<name>/<event_ulid>.toml`. Each object is written
//! exactly once via `If-None-Match: *` — duplicate ULIDs would be
//! a programmer error, not a race.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutResult};
use tracing::{debug, warn};
use ulid::Ulid;

/// Concurrency cap for parallel event-body fetches inside
/// `list_events`. Sized to match the per-prefetch cap in
/// `crate::prefetch::PREFETCH_CONCURRENCY` — the same HTTP/2
/// multiplexing argument applies (commodity object stores tolerate
/// 8-way concurrency on a single connection comfortably).
const EVENT_FETCH_CONCURRENCY: usize = 8;

use elide_core::signing::{self, VerifyingKey};
use elide_core::volume_event::{EventKind, VolumeEvent};

use crate::identity::{self, CoordinatorIdentity};
use crate::ipc::{SignatureStatus, VolumeEventEntry};
use crate::portable::{ConditionalPutError, put_if_absent};

/// Errors from `volume_event_store` operations.
#[derive(Debug)]
pub enum VolumeEventStoreError {
    /// Failed to serialise the event as TOML.
    Serialise(toml::ser::Error),
    /// The underlying store reported an error.
    Store(object_store::Error),
    /// `If-None-Match: *` failed — an event with the same
    /// `event_ulid` already exists. This is a programmer error
    /// (caller minted a duplicate ULID), not a race.
    DuplicateEventUlid,
    /// `event_ulid.timestamp_ms()` cannot be represented as
    /// `DateTime<Utc>`. Practically impossible for ULIDs minted in
    /// this century.
    UnrepresentableTimestamp,
}

impl std::fmt::Display for VolumeEventStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialise(e) => write!(f, "serialising VolumeEvent: {e}"),
            Self::Store(e) => write!(f, "{e}"),
            Self::DuplicateEventUlid => write!(f, "duplicate event_ulid"),
            Self::UnrepresentableTimestamp => {
                write!(f, "event_ulid timestamp out of DateTime<Utc> range")
            }
        }
    }
}

impl std::error::Error for VolumeEventStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialise(e) => Some(e),
            Self::Store(e) => Some(e),
            _ => None,
        }
    }
}

impl From<object_store::Error> for VolumeEventStoreError {
    fn from(e: object_store::Error) -> Self {
        Self::Store(e)
    }
}

impl From<ConditionalPutError> for VolumeEventStoreError {
    fn from(e: ConditionalPutError) -> Self {
        match e {
            ConditionalPutError::PreconditionFailed => Self::DuplicateEventUlid,
            ConditionalPutError::Other(e) => Self::Store(e),
        }
    }
}

fn event_prefix(name: &str) -> StorePath {
    StorePath::from(format!("events/{name}/"))
}

fn event_key(name: &str, event_ulid: Ulid) -> StorePath {
    StorePath::from(format!("events/{name}/{event_ulid}.toml"))
}

/// Sign `event` in place using `identity`'s coordinator key. The
/// payload is the bytes returned by [`VolumeEvent::signing_payload`];
/// the resulting hex-encoded signature lands in `event.signature`.
fn sign_event(event: &mut VolumeEvent, identity: &CoordinatorIdentity) {
    let payload = event.signing_payload();
    let sig = identity.sign(&payload);
    event.signature = Some(signing::encode_hex(&sig));
}

/// PUT a fully-formed signed event at
/// `events/<name>/<event_ulid>.toml` using `If-None-Match: *`.
///
/// Refuses to write an unsigned event — the log invariant is that
/// every event on the wire is signed, so accepting an unsigned one
/// would silently break verification.
pub async fn append_event(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    event: &VolumeEvent,
) -> Result<PutResult, VolumeEventStoreError> {
    debug_assert!(
        event.signature.is_some(),
        "append_event called with unsigned event — call sign+append via emit_event"
    );

    let body = event.to_toml().map_err(VolumeEventStoreError::Serialise)?;
    let key = event_key(name, event.event_ulid);
    let started = std::time::Instant::now();
    let r = put_if_absent(store.as_ref(), &key, Bytes::from(body.into_bytes())).await?;
    debug!(
        "[volume_event_store] PUT-IF-ABSENT {key} kind={} ({:.2?})",
        event.kind.as_str(),
        started.elapsed()
    );
    Ok(r)
}

/// Return the highest `event_ulid` present under
/// `events/<name>/`, or `None` if the prefix is empty.
///
/// Listed objects whose filename does not parse as `<ulid>.toml`
/// are silently skipped — they aren't event records this code
/// emitted, and a stray file should not block a fresh emit.
pub async fn latest_event_ulid(
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> Result<Option<Ulid>, VolumeEventStoreError> {
    let prefix = event_prefix(name);
    let objects: Vec<_> = store.list(Some(&prefix)).try_collect().await?;

    let mut best: Option<Ulid> = None;
    for obj in objects {
        let Some(filename) = obj.location.filename() else {
            continue;
        };
        let Some(stem) = filename.strip_suffix(".toml") else {
            continue;
        };
        let Ok(ulid) = Ulid::from_string(stem) else {
            continue;
        };
        if best.is_none_or(|b| ulid > b) {
            best = Some(ulid);
        }
    }
    Ok(best)
}

/// Mint a fresh event, sign it with `identity`, and append it to
/// `events/<name>/`.
///
/// Steps:
///   1. Look up `prev_event_ulid` (best-effort — list failures fall
///      back to `None`, which produces a small audit gap rather
///      than blocking the emit).
///   2. Mint a fresh `event_ulid`. When a `prev_event_ulid` was
///      observed, seed a [`UlidMint`] with it so the new ULID is
///      strictly greater — this guarantees monotonic ordering across
///      rapid back-to-back emits within the same millisecond and
///      across emits from different coordinators (each
///      re-seeds from the latest S3 state). When no prior event
///      exists, mint via `Ulid::new()` directly.
///   3. Build the `VolumeEvent` with `at` derived from the ULID.
///   4. Sign with `identity.signing_key()` over the canonical
///      payload.
///   5. PUT under `If-None-Match: *`.
///
/// Returns the constructed signed event on success.
pub async fn emit_event(
    store: &Arc<dyn ObjectStore>,
    identity: &CoordinatorIdentity,
    name: &str,
    kind: EventKind,
    vol_ulid: Ulid,
) -> Result<VolumeEvent, VolumeEventStoreError> {
    let prev_event_ulid = match latest_event_ulid(store, name).await {
        Ok(p) => p,
        Err(e) => {
            warn!(
                "[volume_event_store] failed to list prior events for {name}: {e}; \
                 emitting with prev_event_ulid=None"
            );
            None
        }
    };

    // Seed a `UlidMint` from `prev_event_ulid` so the new event's
    // ULID is strictly greater than the highest known event in the
    // log. This is the architectural fix for the same-millisecond
    // ordering race: two `Ulid::new()` calls within one ms produce
    // ULIDs in random order, but `UlidMint::next()` advances the
    // random portion when the clock hasn't moved past `last`. Cross-
    // coordinator emits inherit the same property as long as each
    // emitter re-seeds from S3 — which `latest_event_ulid` above does.
    let event_ulid = match prev_event_ulid {
        Some(prev) => {
            let mut mint = elide_core::ulid_mint::UlidMint::new(prev);
            mint.next()
        }
        None => Ulid::new(),
    };
    let mut event = VolumeEvent::new(
        event_ulid,
        name.to_owned(),
        identity.coordinator_id_str().to_owned(),
        identity.hostname().map(str::to_owned),
        vol_ulid,
        prev_event_ulid,
        kind,
    )
    .ok_or(VolumeEventStoreError::UnrepresentableTimestamp)?;

    sign_event(&mut event, identity);
    append_event(store, name, &event).await?;
    Ok(event)
}

/// List every event under `events/<name>/`, parsed and
/// sorted ascending by `event_ulid`.
///
/// Listed objects whose filename does not parse as `<ulid>.toml`,
/// or whose body fails to parse as a [`VolumeEvent`], are dropped
/// with a `warn!` — a corrupt file should be visible in the log
/// but must not block the operator from inspecting the rest.
pub async fn list_events(
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> Result<Vec<VolumeEvent>, VolumeEventStoreError> {
    let prefix = event_prefix(name);
    let objects: Vec<_> = store.list(Some(&prefix)).try_collect().await?;

    // Sync filter: drop non-`<ulid>.toml` listings up front. Each
    // surviving entry's body GET is independent, so fetch them in
    // parallel under a small concurrency cap. Per-event errors are
    // warned and skipped (yields `None`) — matching the previous
    // sequential behaviour.
    let to_fetch: Vec<object_store::path::Path> = objects
        .into_iter()
        .filter_map(|obj| {
            let filename = obj.location.filename()?;
            let stem = filename.strip_suffix(".toml")?;
            if Ulid::from_string(stem).is_err() {
                return None;
            }
            Some(obj.location)
        })
        .collect();

    let mut events: Vec<VolumeEvent> =
        futures::stream::iter(to_fetch.into_iter().map(|location| {
            let store = store.clone();
            async move {
                let body = match store.get(&location).await {
                    Ok(g) => match g.bytes().await {
                        Ok(b) => b,
                        Err(e) => {
                            warn!("[volume_event_store] read {location}: {e}");
                            return None;
                        }
                    },
                    Err(e) => {
                        warn!("[volume_event_store] get {location}: {e}");
                        return None;
                    }
                };
                let text = match std::str::from_utf8(&body) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("[volume_event_store] {location}: not UTF-8: {e}");
                        return None;
                    }
                };
                match VolumeEvent::from_toml(text) {
                    Ok(event) => Some(event),
                    Err(e) => {
                        warn!("[volume_event_store] parse {location}: {e}");
                        None
                    }
                }
            }
        }))
        .buffer_unordered(EVENT_FETCH_CONCURRENCY)
        .filter_map(|opt| async move { opt })
        .collect()
        .await;

    events.sort_by_key(|e| e.event_ulid);
    Ok(events)
}

/// Verify `event.signature` against `verifying_key`. The pubkey is
/// assumed to already match the event's `coordinator_id` (the
/// caller resolves it via [`identity::fetch_coordinator_pub`],
/// which enforces the binding).
pub fn verify_event_signature(
    event: &VolumeEvent,
    verifying_key: &VerifyingKey,
) -> SignatureStatus {
    use ed25519_dalek::Verifier;

    let Some(sig_hex) = event.signature.as_deref() else {
        return SignatureStatus::Missing;
    };
    let sig_bytes = match signing::decode_hex(sig_hex) {
        Ok(b) => b,
        Err(e) => {
            return SignatureStatus::Invalid {
                reason: format!("signature hex decode: {e}"),
            };
        }
    };
    let sig_arr: [u8; 64] = match sig_bytes.as_slice().try_into() {
        Ok(a) => a,
        Err(_) => {
            return SignatureStatus::Invalid {
                reason: format!("signature wrong length ({}, want 64)", sig_bytes.len()),
            };
        }
    };
    let signature = ed25519_dalek::Signature::from_bytes(&sig_arr);
    match verifying_key.verify(&event.signing_payload(), &signature) {
        Ok(()) => SignatureStatus::Valid,
        Err(e) => SignatureStatus::Invalid {
            reason: format!("signature did not verify: {e}"),
        },
    }
}

/// Read every event under `events/<name>/` and pair each
/// with a [`SignatureStatus`].
///
/// Coordinator pubkeys are fetched once per unique `coordinator_id`
/// observed in the log and cached for the duration of the call.
/// Fetch failures collapse into `SignatureStatus::KeyUnavailable`
/// for every event signed by that coordinator — the per-event
/// status surfaces the failure rather than aborting the whole read.
pub async fn list_and_verify_events(
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> Result<Vec<VolumeEventEntry>, VolumeEventStoreError> {
    let events = list_events(store, name).await?;

    // Cache: Some(key) on success, None when fetch failed (with the
    // failure reason carried in `key_failures`).
    let mut keys: HashMap<String, Option<VerifyingKey>> = HashMap::new();
    let mut key_failures: HashMap<String, String> = HashMap::new();

    let mut entries = Vec::with_capacity(events.len());
    for event in events {
        let coord_id = event.coordinator_id.clone();
        if !keys.contains_key(&coord_id) {
            match identity::fetch_coordinator_pub(store.as_ref(), &coord_id).await {
                Ok(vk) => {
                    keys.insert(coord_id.clone(), Some(vk));
                }
                Err(e) => {
                    key_failures.insert(coord_id.clone(), format!("{e}"));
                    keys.insert(coord_id.clone(), None);
                }
            }
        }
        let status = match keys.get(&coord_id).and_then(|opt| opt.as_ref()) {
            Some(vk) => verify_event_signature(&event, vk),
            None => SignatureStatus::KeyUnavailable {
                reason: key_failures
                    .get(&coord_id)
                    .cloned()
                    .unwrap_or_else(|| "pubkey unavailable".to_string()),
            },
        };
        entries.push(VolumeEventEntry {
            event,
            signature_status: status,
        });
    }
    Ok(entries)
}

/// Best-effort companion to [`emit_event`]. Used by lifecycle call
/// sites that have just completed a successful CAS on
/// `names/<name>` and need to append the corresponding journal
/// entry — the entry must not block or fail the lifecycle
/// operation it accompanies, so any error is logged and discarded.
///
/// The cost of a missed event is a single-event gap in the log,
/// detectable later via `prev_event_ulid` skip — exactly the
/// "best-effort" contract documented in `design-volume-event-log.md`.
pub async fn emit_best_effort(
    store: &Arc<dyn ObjectStore>,
    identity: &CoordinatorIdentity,
    name: &str,
    kind: EventKind,
    vol_ulid: Ulid,
) {
    let kind_str = kind.as_str();
    if let Err(e) = emit_event(store, identity, name, kind, vol_ulid).await {
        warn!("[volume_event_store] failed to emit {kind_str} event for {name}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::CoordinatorIdentity;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn fresh_identity() -> (TempDir, CoordinatorIdentity) {
        let tmp = TempDir::new().expect("tmpdir");
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).expect("identity");
        (tmp, id)
    }

    fn vol_ulid() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    #[tokio::test]
    async fn emit_then_read_latest() {
        let s = store();
        let (_tmp, id) = fresh_identity();

        assert!(
            latest_event_ulid(&s, "vol").await.unwrap().is_none(),
            "empty prefix must return None"
        );

        let ev = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("first emit");
        assert!(ev.signature.is_some(), "emitted event must be signed");
        assert_eq!(ev.coordinator_id, id.coordinator_id_str());
        assert_eq!(ev.name, "vol", "emitted event must carry the volume name");

        let latest = latest_event_ulid(&s, "vol")
            .await
            .expect("list")
            .expect("one event present");
        assert_eq!(latest, ev.event_ulid);
    }

    #[tokio::test]
    async fn second_event_chains_via_prev_event_ulid() {
        let s = store();
        let (_tmp, id) = fresh_identity();

        let first = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("first");
        let second = emit_event(&s, &id, "vol", EventKind::Claimed, vol_ulid())
            .await
            .expect("second");

        assert_eq!(
            second.prev_event_ulid,
            Some(first.event_ulid),
            "second event must reference the first"
        );
    }

    /// Regression for a same-millisecond ULID race that took down CI:
    /// `Ulid::new()` is non-monotonic across two calls within one ms,
    /// so back-to-back `emit_event` calls could produce events in
    /// reverse ULID order — meaning `entries.last()` (which the
    /// peer-discovery flow uses) returns the *first*-emitted event,
    /// not the most recent.
    ///
    /// The fix lives in `emit_event`: seed a `UlidMint` with the
    /// observed `prev_event_ulid` so the new ULID is strictly greater
    /// than the highest event already in S3. This regression test
    /// fires emits as fast as possible on a single thread — without
    /// the mint, the failure shows up consistently inside the same
    /// millisecond.
    #[tokio::test]
    async fn back_to_back_emits_are_strictly_monotonic() {
        let s = store();
        let (_tmp, id) = fresh_identity();

        let mut last_ulid: Option<Ulid> = None;
        // 32 emits with no awaits between minting calls — many of
        // these will land within the same millisecond on a fast host
        // (CI runners reliably).
        for _ in 0..32 {
            let ev = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
                .await
                .expect("emit");
            if let Some(prev) = last_ulid {
                assert!(
                    ev.event_ulid > prev,
                    "event {} must be strictly greater than previous {}",
                    ev.event_ulid,
                    prev
                );
            }
            last_ulid = Some(ev.event_ulid);
        }
    }

    #[tokio::test]
    async fn emitted_event_round_trips_through_storage() {
        let s = store();
        let (_tmp, id) = fresh_identity();

        let ev = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("emit");

        // Read the object back and parse it.
        let key = event_key("vol", ev.event_ulid);
        let bytes = s.get(&key).await.unwrap().bytes().await.unwrap();
        let parsed = VolumeEvent::from_toml(std::str::from_utf8(&bytes).unwrap()).unwrap();
        assert_eq!(parsed, ev);
    }

    #[tokio::test]
    async fn list_events_returns_sorted_history() {
        let s = store();
        let (_tmp, id) = fresh_identity();

        let a = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("first");
        let b = emit_event(&s, &id, "vol", EventKind::Claimed, vol_ulid())
            .await
            .expect("second");

        // `emit_event` mints via `Ulid::new()`, which is monotonic
        // *across* milliseconds but not *within* one — two back-to-
        // back emits in the same ms can come out in either ULID
        // order (see CLAUDE.md "Monotonic ULIDs in tests"). The
        // invariant under test here is `list_events`' sort order,
        // so check both emitted ULIDs are present and the listing
        // is ascending, without assuming emit order matches ULID
        // order.
        let listed = list_events(&s, "vol").await.expect("list");
        assert_eq!(listed.len(), 2);
        assert!(
            listed[0].event_ulid < listed[1].event_ulid,
            "list_events must return events in ascending ULID order"
        );
        let listed_ulids: std::collections::HashSet<_> =
            listed.iter().map(|e| e.event_ulid).collect();
        assert!(listed_ulids.contains(&a.event_ulid));
        assert!(listed_ulids.contains(&b.event_ulid));
    }

    #[tokio::test]
    async fn list_events_skips_corrupt_files() {
        let s = store();
        let (_tmp, id) = fresh_identity();

        let good = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("good emit");

        // Inject a non-ULID-named file (must be silently ignored) and
        // a ULID-named file that fails to parse as a VolumeEvent (must
        // also be skipped, with a warn).
        s.put(
            &StorePath::from("events/vol/garbage.txt"),
            Bytes::from_static(b"hi").into(),
        )
        .await
        .expect("put garbage");
        let bogus_ulid = Ulid::from_string("01J9999999999999999999999X").unwrap();
        s.put(
            &event_key("vol", bogus_ulid),
            Bytes::from_static(b"not toml at all").into(),
        )
        .await
        .expect("put bogus");

        let listed = list_events(&s, "vol").await.expect("list");
        assert_eq!(listed.len(), 1, "only the parseable event survives");
        assert_eq!(listed[0].event_ulid, good.event_ulid);
    }

    #[tokio::test]
    async fn list_and_verify_marks_valid_when_pubkey_published() {
        let s = store();
        let (_tmp, id) = fresh_identity();
        id.publish_pub(s.as_ref()).await.expect("publish pub");

        emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("emit");

        let entries = list_and_verify_events(&s, "vol").await.expect("verify");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].signature_status, SignatureStatus::Valid);
    }

    #[tokio::test]
    async fn list_and_verify_reports_key_unavailable_without_published_pub() {
        let s = store();
        let (_tmp, id) = fresh_identity();
        // Deliberately do NOT publish_pub.

        emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("emit");

        let entries = list_and_verify_events(&s, "vol").await.expect("verify");
        assert_eq!(entries.len(), 1);
        assert!(matches!(
            entries[0].signature_status,
            SignatureStatus::KeyUnavailable { .. }
        ));
    }

    #[tokio::test]
    async fn list_and_verify_reports_invalid_for_tampered_event() {
        let s = store();
        let (_tmp, id) = fresh_identity();
        id.publish_pub(s.as_ref()).await.expect("publish pub");

        let original = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("emit");

        // Hand-tamper the on-disk event: keep the signature but flip
        // the kind. The event still parses; the signature should not
        // verify against the new payload.
        let key = event_key("vol", original.event_ulid);
        let mut tampered = original.clone();
        tampered.kind = EventKind::Claimed;
        let body = tampered.to_toml().expect("serialise");
        // PUT overwrites — InMemory has no conditional semantics for this.
        s.put(&key, Bytes::from(body.into_bytes()).into())
            .await
            .expect("overwrite");

        let entries = list_and_verify_events(&s, "vol").await.expect("verify");
        assert_eq!(entries.len(), 1);
        assert!(matches!(
            entries[0].signature_status,
            SignatureStatus::Invalid { .. }
        ));
    }

    #[tokio::test]
    async fn list_and_verify_caches_pubkey_per_coordinator() {
        // Two events from the same coordinator — pubkey fetch must
        // happen once. Sanity-check by publishing the pubkey, then
        // emitting two events and confirming both verify.
        let s = store();
        let (_tmp, id) = fresh_identity();
        id.publish_pub(s.as_ref()).await.expect("publish pub");

        emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("first");
        emit_event(&s, &id, "vol", EventKind::Claimed, vol_ulid())
            .await
            .expect("second");

        let entries = list_and_verify_events(&s, "vol").await.expect("verify");
        assert_eq!(entries.len(), 2);
        for e in &entries {
            assert_eq!(e.signature_status, SignatureStatus::Valid);
        }
    }
}
