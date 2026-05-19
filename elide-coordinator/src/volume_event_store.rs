//! Bucket-level read/write helpers for the per-name event log.
//!
//! Pairs with [`crate::name_store`] (which manages the
//! `names/<name>` pointer object). The event log lives under a
//! separate top-level prefix so the pointer key and the event
//! prefix never collide on the same parent — `aws s3 ls names/`
//! lists names and nothing else; `aws s3 ls events/` lists logs.
//! See `docs/design-volume-event-log.md`.
//!
//! Keys: `events/<name>/<event_ulid>`. Each object is written
//! exactly once via `If-None-Match: *` — duplicate ULIDs would be
//! a programmer error, not a race.

use std::collections::HashMap;
use std::sync::Arc;

use std::sync::LazyLock;
use std::sync::Mutex as StdMutex;

use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutResult, UpdateVersion};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, warn};
use ulid::Ulid;

use elide_core::signing::{self, VerifyingKey};
use elide_core::volume_event::{EventKind, VolumeEvent};

use crate::identity::{self, CoordinatorIdentity};
use crate::ipc::{SignatureStatus, VolumeEventEntry};
use crate::portable::{
    ConditionalPutError, MIME_TOML, put_if_absent_with_type, put_with_match_with_type,
};

/// Number of most-recent signed events carried inline in the
/// `events/<name>/HEAD` window. Tuning parameter, not pinned by the
/// design (`docs/list-elimination-plan.md` § *event-log spine*):
/// large enough that claim / peer-discovery / the default
/// `volume events` view are answered from the single HEAD GET without
/// any `prev_event_ulid` walk.
const HEAD_WINDOW: usize = 16;

/// Default `limit` for [`recent_events`] / `volume events` when the
/// caller doesn't specify one: the full HEAD window, answered in a
/// single GET with no `prev_event_ulid` walk.
pub const DEFAULT_EVENTS_LIMIT: usize = HEAD_WINDOW;

fn head_key(name: &str) -> StorePath {
    StorePath::from(format!("events/{name}/HEAD"))
}

/// Per-name in-process serialization of this coordinator's own emits
/// (the plan's "small Mutex map"). Cross-coordinator concurrency is
/// handled by the `names/<name>` ownership CAS upstream; this only
/// stops a coordinator's own concurrent tasks from racing each other's
/// HEAD read-modify-write (which would otherwise misfire the
/// `If-Match` displacement detector on a purely local race). One entry
/// per distinct name ever emitted — bounded by the coordinator's
/// volume count, same as the snapshot-lock registry.
static NAME_EMIT_LOCKS: LazyLock<StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn name_emit_lock(name: &str) -> Arc<AsyncMutex<()>> {
    let mut map = NAME_EMIT_LOCKS
        .lock()
        .expect("name-emit lock registry poisoned");
    map.entry(name.to_owned())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

/// The `events/<name>/HEAD` window: the last [`HEAD_WINDOW`] signed
/// events, newest-first (`events[0]` is the latest). HEAD is the
/// ordering authority for `emit` (so no LIST is needed) but **not**
/// the integrity authority — each entry is the same individually
/// signed `VolumeEvent` stored standalone, so a tampered entry still
/// fails the per-event signature check.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct EventHead {
    #[serde(default)]
    events: Vec<VolumeEvent>,
}

impl EventHead {
    /// Newest event, or `None` for an empty/just-created log.
    fn latest(&self) -> Option<&VolumeEvent> {
        self.events.first()
    }

    /// `[new] ++ self.events`, truncated to [`HEAD_WINDOW`].
    fn pushed(&self, new: VolumeEvent) -> EventHead {
        let mut events = Vec::with_capacity((self.events.len() + 1).min(HEAD_WINDOW));
        events.push(new);
        events.extend(self.events.iter().take(HEAD_WINDOW - 1).cloned());
        EventHead { events }
    }
}

/// GET `events/<name>/HEAD`. `Ok(None)` means the object is absent —
/// a genuinely empty log (first event for this name). A transient
/// store error is propagated, **not** mapped to `None`: under Option 3
/// `None` means "first event", so swallowing an error would set
/// `prev_event_ulid = None` on a name that has history and fork the
/// chain (`docs/list-elimination-plan.md`, Note A). The returned
/// [`UpdateVersion`] is the `If-Match` precondition for the rewrite.
async fn read_head(
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> Result<Option<(EventHead, UpdateVersion)>, VolumeEventStoreError> {
    let key = head_key(name);
    let got = match store.get(&key).await {
        Ok(g) => g,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(VolumeEventStoreError::Store(e)),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let bytes = got.bytes().await.map_err(VolumeEventStoreError::Store)?;
    let text = std::str::from_utf8(&bytes).map_err(|e| {
        VolumeEventStoreError::Store(object_store::Error::Generic {
            store: "events",
            source: format!("HEAD not utf-8: {e}").into(),
        })
    })?;
    let head: EventHead = toml::from_str(text).map_err(VolumeEventStoreError::ParseHead)?;
    Ok(Some((head, version)))
}

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
    /// `events/<name>/HEAD` did not parse as an [`EventHead`].
    ParseHead(toml::de::Error),
    /// The `If-Match` HEAD rewrite failed: `events/<name>/HEAD`
    /// changed under us. The only writer that can do that to a name
    /// we own is a concurrent `release --force` — i.e. **this
    /// coordinator has been displaced**. Caller must fail hard, not
    /// retry (`docs/list-elimination-plan.md` § *Single-writer*).
    Displaced,
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
            Self::ParseHead(e) => write!(f, "parsing events HEAD: {e}"),
            Self::Displaced => write!(
                f,
                "event-log HEAD changed under us — this coordinator has been displaced \
                 (concurrent release --force)"
            ),
        }
    }
}

impl std::error::Error for VolumeEventStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialise(e) => Some(e),
            Self::Store(e) => Some(e),
            Self::ParseHead(e) => Some(e),
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

fn event_key(name: &str, event_ulid: Ulid) -> StorePath {
    StorePath::from(format!("events/{name}/{event_ulid}"))
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
/// `events/<name>/<event_ulid>` using `If-None-Match: *`.
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
    let r = put_if_absent_with_type(
        store.as_ref(),
        &key,
        Bytes::from(body.into_bytes()),
        MIME_TOML,
    )
    .await?;
    debug!(
        "[volume_event_store] PUT-IF-ABSENT {key} kind={} ({:.2?})",
        event.kind.as_str(),
        started.elapsed()
    );
    Ok(r)
}

/// Write the `events/<name>/HEAD` window.
///
/// `is_force` (the `release --force` emit) writes **unconditionally**
/// — force is the override at this layer exactly as at `names/<name>`,
/// so it must never fail. Otherwise the write is a CAS: `If-Match`
/// the `expected` version (a normal emit) or `If-None-Match: *` when
/// the log was empty (`expected == None`). A precondition failure on
/// either path means HEAD changed under us — a concurrent `release
/// --force` displaced this coordinator → [`Displaced`], fail hard, no
/// retry.
///
/// [`Displaced`]: VolumeEventStoreError::Displaced
async fn write_head(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    head: &EventHead,
    expected: Option<UpdateVersion>,
    is_force: bool,
) -> Result<(), VolumeEventStoreError> {
    let body = Bytes::from(
        toml::to_string(head)
            .map_err(VolumeEventStoreError::Serialise)?
            .into_bytes(),
    );
    let key = head_key(name);
    if is_force {
        return store
            .put(&key, body.into())
            .await
            .map(|_| ())
            .map_err(VolumeEventStoreError::Store);
    }
    let displaced = |e| match e {
        ConditionalPutError::PreconditionFailed => VolumeEventStoreError::Displaced,
        ConditionalPutError::Other(e) => VolumeEventStoreError::Store(e),
    };
    match expected {
        Some(ver) => put_with_match_with_type(store.as_ref(), &key, body, ver, MIME_TOML)
            .await
            .map(|_| ())
            .map_err(displaced),
        None => put_if_absent_with_type(store.as_ref(), &key, body, MIME_TOML)
            .await
            .map(|_| ())
            .map_err(displaced),
    }
}

/// Mint a fresh event, sign it, and append it to `events/<name>/`,
/// keeping the `events/<name>/HEAD` window the ordering authority
/// (`docs/list-elimination-plan.md` § *event-log spine*).
///
/// Steps (Option 3 — HEAD before record):
///   1. Acquire the in-process per-name lock (intra-coordinator
///      serialization; cross-coordinator is the upstream
///      `names/<name>` CAS).
///   2. `read_head` → `prev = HEAD[0]`. A transient store error
///      **fails the emit** (it is *not* mapped to `None`; see Note A).
///   3. Mint a strictly-greater `event_ulid` via [`UlidMint`] seeded
///      from `prev` (unchanged ordering guarantee), build, sign.
///   4. Write the new HEAD window: `If-Match` the version read in (2)
///      for a normal emit; **unconditional** for the `release
///      --force` emit (`ForceReleased` — force is the override at the
///      event layer exactly as at `names/<name>`); `If-None-Match:*`
///      when the log is empty. An `If-Match` mismatch means this
///      coordinator was displaced → [`Displaced`], fail hard, no
///      retry.
///   5. PUT the immutable record under `If-None-Match: *`. A crash
///      between (4) and (5) leaves a benign phantom (full signed
///      record still inline in HEAD; readers 404-skip the standalone
///      object).
///
/// [`Displaced`]: VolumeEventStoreError::Displaced
pub async fn emit_event(
    store: &Arc<dyn ObjectStore>,
    identity: &CoordinatorIdentity,
    name: &str,
    kind: EventKind,
    vol_ulid: Ulid,
) -> Result<VolumeEvent, VolumeEventStoreError> {
    let lock = name_emit_lock(name);
    let _guard = lock.lock().await;

    // (2) HEAD is the ordering authority. Absent => first event;
    //     a transient error propagates (Note A — never prev=None).
    let head = read_head(store, name).await?;
    let (prev_head, expected) = match head {
        Some((h, ver)) => (Some(h), Some(ver)),
        None => (None, None),
    };
    let prev_event_ulid = prev_head
        .as_ref()
        .and_then(|h| h.latest())
        .map(|e| e.event_ulid);

    // (3) Strictly-greater ULID, seeded from prev (unchanged: the
    //     same-millisecond ordering fix). Build + sign.
    let event_ulid = match prev_event_ulid {
        Some(prev) => elide_core::ulid_mint::UlidMint::new(prev).next(),
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

    // (4) HEAD first. Force-release is the unconditional override at
    //     this layer just as at `names/<name>`; it must never fail.
    //     Any precondition failure on a normal emit is displacement.
    let new_head = prev_head.unwrap_or_default().pushed(event.clone());
    let is_force = matches!(event.kind, EventKind::ForceReleased { .. });
    write_head(store, name, &new_head, expected, is_force).await?;

    // (5) Immutable standalone record second (idempotent create).
    append_event(store, name, &event).await?;
    Ok(event)
}

/// Up to `limit` most-recent events for `name`, newest-first
/// (`[0]` is the latest). No LIST.
///
/// Served from the `events/<name>/HEAD` window in a single GET when
/// the window already holds `limit` events, or when the whole log
/// fits in the window. When `limit` exceeds a *full* window, the
/// oldest in-window event's `prev_event_ulid` back-link is walked one
/// immutable record at a time until `limit` is reached, the chain
/// roots out, or a link 404s — a crash phantom (HEAD names a record
/// whose standalone body was never written): stop and return what we
/// have. A corrupt/unreadable record on the chain truncates the walk
/// for the same reason (its back-link is gone) — logged, not fatal.
///
/// An absent HEAD is an empty log → empty vec.
pub async fn recent_events(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    limit: usize,
) -> Result<Vec<VolumeEvent>, VolumeEventStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let Some((head, _ver)) = read_head(store, name).await? else {
        return Ok(Vec::new());
    };
    let mut events = head.events;
    if events.len() >= limit {
        events.truncate(limit);
        return Ok(events);
    }
    // A non-full window holds the entire log (`pushed` only drops
    // events once it exceeds HEAD_WINDOW); nothing older to walk.
    if events.len() < HEAD_WINDOW {
        return Ok(events);
    }
    // Window full and more was asked for: walk the back-link chain
    // from the oldest in-window event.
    let mut prev = events.last().and_then(|e| e.prev_event_ulid);
    while events.len() < limit {
        let Some(p) = prev else { break };
        let key = event_key(name, p);
        let got = match store.get(&key).await {
            Ok(g) => g,
            Err(object_store::Error::NotFound { .. }) => {
                debug!("[volume_event_store] {key} missing (phantom back-link); stop walk");
                break;
            }
            Err(e) => return Err(VolumeEventStoreError::Store(e)),
        };
        let bytes = got.bytes().await.map_err(VolumeEventStoreError::Store)?;
        let event = match std::str::from_utf8(&bytes)
            .ok()
            .and_then(|t| VolumeEvent::from_toml(t).ok())
        {
            Some(ev) => ev,
            None => {
                debug!("[volume_event_store] {key} unreadable/corrupt; stop walk");
                break;
            }
        };
        prev = event.prev_event_ulid;
        events.push(event);
    }
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

/// Read the `limit` most-recent events for `name` (ascending
/// `event_ulid` order) and pair each with a [`SignatureStatus`].
///
/// Coordinator pubkeys are fetched once per unique `coordinator_id`
/// observed in the log and cached for the duration of the call.
/// Fetch failures collapse into `SignatureStatus::KeyUnavailable`
/// for every event signed by that coordinator — the per-event
/// status surfaces the failure rather than aborting the whole read.
pub async fn list_and_verify_events(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    limit: usize,
) -> Result<Vec<VolumeEventEntry>, VolumeEventStoreError> {
    // `recent_events` is newest-first; the history view is
    // chronological, so reverse to ascending `event_ulid`.
    let mut events = recent_events(store, name, limit).await?;
    events.reverse();

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
            recent_events(&s, "vol", DEFAULT_EVENTS_LIMIT)
                .await
                .unwrap()
                .is_empty(),
            "absent HEAD must read as an empty log"
        );

        let ev = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("first emit");
        assert!(ev.signature.is_some(), "emitted event must be signed");
        assert_eq!(ev.coordinator_id, id.coordinator_id_str());
        assert_eq!(ev.name, "vol", "emitted event must carry the volume name");

        let recent = recent_events(&s, "vol", DEFAULT_EVENTS_LIMIT)
            .await
            .expect("read HEAD");
        assert_eq!(recent.len(), 1);
        assert_eq!(
            recent[0].event_ulid, ev.event_ulid,
            "HEAD[0] is the just-emitted event"
        );
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
    async fn recent_events_newest_first_and_chronological() {
        let s = store();
        let (_tmp, id) = fresh_identity();
        id.publish_pub(s.as_ref()).await.expect("publish pub");

        let a = emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("first");
        let b = emit_event(&s, &id, "vol", EventKind::Claimed, vol_ulid())
            .await
            .expect("second");

        // `emit_event` mints strictly-monotonic ULIDs (UlidMint seeded
        // from the prev), so the order is deterministic: b > a, and
        // b back-links to a.
        assert!(b.event_ulid > a.event_ulid);
        assert_eq!(b.prev_event_ulid, Some(a.event_ulid));

        // `recent_events` is newest-first.
        let recent = recent_events(&s, "vol", DEFAULT_EVENTS_LIMIT)
            .await
            .expect("recent");
        assert_eq!(
            recent.iter().map(|e| e.event_ulid).collect::<Vec<_>>(),
            vec![b.event_ulid, a.event_ulid],
        );

        // `list_and_verify_events` reverses to chronological order.
        let listed = list_and_verify_events(&s, "vol", DEFAULT_EVENTS_LIMIT)
            .await
            .expect("verify");
        assert_eq!(
            listed
                .iter()
                .map(|e| e.event.event_ulid)
                .collect::<Vec<_>>(),
            vec![a.event_ulid, b.event_ulid],
        );
    }

    /// `limit` larger than a *full* HEAD window walks the
    /// `prev_event_ulid` chain through the standalone records, in
    /// strict newest→oldest order, and a missing back-link (crash
    /// phantom) truncates the walk at that point.
    #[tokio::test]
    async fn recent_events_walks_back_links_past_full_window() {
        let s = store();
        let (_tmp, id) = fresh_identity();

        let total = HEAD_WINDOW + 4;
        let mut emitted = Vec::with_capacity(total);
        for _ in 0..total {
            emitted.push(
                emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
                    .await
                    .expect("emit"),
            );
        }

        // Default limit (= window): exactly the newest HEAD_WINDOW,
        // strictly descending.
        let windowed = recent_events(&s, "vol", HEAD_WINDOW).await.expect("win");
        assert_eq!(windowed.len(), HEAD_WINDOW);
        assert!(
            windowed
                .windows(2)
                .all(|w| w[0].event_ulid > w[1].event_ulid)
        );
        assert_eq!(windowed[0].event_ulid, emitted[total - 1].event_ulid);

        // Larger limit: walk the chain past the window, recovering the
        // full history newest-first.
        let all = recent_events(&s, "vol", total).await.expect("all");
        assert_eq!(all.len(), total);
        assert!(all.windows(2).all(|w| w[0].event_ulid > w[1].event_ulid));
        assert_eq!(all[total - 1].event_ulid, emitted[0].event_ulid);

        // Delete the standalone record the walk reaches first off the
        // window (the event one older than the oldest in-window one).
        // That back-link now 404s → the walk stops; only the window
        // survives.
        let oldest_in_window = &all[HEAD_WINDOW - 1];
        let first_off_window = oldest_in_window
            .prev_event_ulid
            .expect("there is an older event");
        s.delete(&event_key("vol", first_off_window))
            .await
            .expect("delete record");

        let truncated = recent_events(&s, "vol", total).await.expect("truncated");
        assert_eq!(
            truncated.len(),
            HEAD_WINDOW,
            "a phantom back-link truncates the walk at the window edge"
        );
    }

    /// `write_head` on a normal emit is a CAS: a stale `expected`
    /// version maps a precondition failure to `Displaced` (the
    /// displacement detector). The force path is unconditional and
    /// succeeds against the same stale version.
    #[tokio::test]
    async fn write_head_cas_is_displaced_but_force_is_unconditional() {
        let s = store();
        let (_tmp, id) = fresh_identity();

        emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("seed");
        let (head_v1, ver_v1) = read_head(&s, "vol")
            .await
            .expect("read head")
            .expect("head present");

        // Out-of-band change bumps the etag (simulates a concurrent
        // `release --force` by another coordinator).
        write_head(&s, "vol", &head_v1, None, true)
            .await
            .expect("force overwrite");

        // Normal emit against the now-stale version → Displaced.
        let displaced = write_head(&s, "vol", &head_v1, Some(ver_v1.clone()), false).await;
        assert!(
            matches!(displaced, Err(VolumeEventStoreError::Displaced)),
            "stale If-Match must surface as Displaced, got {displaced:?}"
        );

        // Force write ignores the stale version entirely.
        write_head(&s, "vol", &head_v1, Some(ver_v1), true)
            .await
            .expect("force write must never fail on a version mismatch");
    }

    #[tokio::test]
    async fn list_and_verify_marks_valid_when_pubkey_published() {
        let s = store();
        let (_tmp, id) = fresh_identity();
        id.publish_pub(s.as_ref()).await.expect("publish pub");

        emit_event(&s, &id, "vol", EventKind::Created, vol_ulid())
            .await
            .expect("emit");

        let entries = list_and_verify_events(&s, "vol", DEFAULT_EVENTS_LIMIT)
            .await
            .expect("verify");
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

        let entries = list_and_verify_events(&s, "vol", DEFAULT_EVENTS_LIMIT)
            .await
            .expect("verify");
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

        // Hand-tamper the inline HEAD entry: keep the signature but
        // flip the kind. HEAD is the read source (no LIST), so the
        // per-event signature check must still catch the mutation
        // even though the standalone archival record is untouched.
        let mut tampered = original.clone();
        tampered.kind = EventKind::Claimed;
        let head = EventHead {
            events: vec![tampered],
        };
        let body = toml::to_string(&head).expect("serialise head");
        // PUT overwrites — InMemory has no conditional semantics for this.
        s.put(&head_key("vol"), Bytes::from(body.into_bytes()).into())
            .await
            .expect("overwrite head");

        let entries = list_and_verify_events(&s, "vol", DEFAULT_EVENTS_LIMIT)
            .await
            .expect("verify");
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

        let entries = list_and_verify_events(&s, "vol", DEFAULT_EVENTS_LIMIT)
            .await
            .expect("verify");
        assert_eq!(entries.len(), 2);
        for e in &entries {
            assert_eq!(e.signature_status, SignatureStatus::Valid);
        }
    }

    /// Property: under any interleaving of normal emits, force-release
    /// emits, and crash-injected emits (HEAD written, standalone
    /// record skipped — the Option-3 phantom), the readable log is
    /// always a single contiguous newest-first chain and the HEAD
    /// window is exactly its newest-N prefix.
    ///
    /// Covers the three P1 invariants from
    /// `docs/list-elimination-plan.md`: (1) window ≡ prefix of the
    /// prev-walk, (2) a crash phantom never makes a reader miss a real
    /// event (the event is inline in HEAD; the standalone body 404s),
    /// (3) force-release keeps `events/<name>/` a single chain.
    mod prop_event_log {
        use super::*;
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        enum Op {
            /// Normal `emit_event`: 0 → Created, else Claimed.
            Emit(u8),
            /// `release --force` emit from a second coordinator.
            EmitForce,
            /// Crash between HEAD write and record PUT: write HEAD,
            /// skip `append_event` → a phantom standalone record.
            EmitCrashed,
        }

        fn arb_op() -> impl Strategy<Value = Op> {
            prop_oneof![
                (0u8..2).prop_map(Op::Emit),
                Just(Op::EmitForce),
                Just(Op::EmitCrashed),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

            #[test]
            fn window_is_prefix_of_chain_under_crash_and_force(
                ops in prop::collection::vec(arb_op(), 1..40)
            ) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let s = store();
                    let (_ta, id_a) = fresh_identity();
                    let (_tb, id_b) = fresh_identity();
                    let name = "vol";
                    let v = vol_ulid();

                    for op in &ops {
                        match op {
                            Op::Emit(k) => {
                                let kind = if *k == 0 {
                                    EventKind::Created
                                } else {
                                    EventKind::Claimed
                                };
                                emit_event(&s, &id_a, name, kind, v)
                                    .await
                                    .expect("emit");
                            }
                            Op::EmitForce => {
                                emit_event(
                                    &s,
                                    &id_b,
                                    name,
                                    EventKind::ForceReleased {
                                        handoff_snapshot: Ulid::nil(),
                                        displaced_coordinator_id: id_a
                                            .coordinator_id_str()
                                            .to_owned(),
                                    },
                                    v,
                                )
                                .await
                                .expect("force emit");
                            }
                            Op::EmitCrashed => {
                                // Replicate emit_event's build+sign, write
                                // HEAD, then stop — the standalone record
                                // is never written (the phantom).
                                let head = read_head(&s, name).await.expect("read head");
                                let (prev_head, expected) = match head {
                                    Some((h, ver)) => (Some(h), Some(ver)),
                                    None => (None, None),
                                };
                                let prev_ulid = prev_head
                                    .as_ref()
                                    .and_then(|h| h.latest())
                                    .map(|e| e.event_ulid);
                                let event_ulid = match prev_ulid {
                                    Some(p) => {
                                        elide_core::ulid_mint::UlidMint::new(p).next()
                                    }
                                    None => Ulid::new(),
                                };
                                let mut ev = VolumeEvent::new(
                                    event_ulid,
                                    name.to_owned(),
                                    id_a.coordinator_id_str().to_owned(),
                                    id_a.hostname().map(str::to_owned),
                                    v,
                                    prev_ulid,
                                    EventKind::Created,
                                )
                                .expect("event");
                                sign_event(&mut ev, &id_a);
                                let new_head =
                                    prev_head.unwrap_or_default().pushed(ev.clone());
                                write_head(&s, name, &new_head, expected, false)
                                    .await
                                    .expect("crash write_head");
                                // Phantom: the standalone record is absent.
                                assert!(
                                    s.get(&event_key(name, ev.event_ulid)).await.is_err(),
                                    "crash op must leave no standalone record"
                                );
                            }
                        }
                    }

                    let window =
                        recent_events(&s, name, HEAD_WINDOW).await.expect("window");
                    let full =
                        recent_events(&s, name, usize::MAX).await.expect("full");

                    // (1) window is the newest-N prefix of the walk.
                    assert!(window.len() <= HEAD_WINDOW);
                    assert!(window.len() <= full.len());
                    for (w, f) in window.iter().zip(full.iter()) {
                        assert_eq!(w.event_ulid, f.event_ulid);
                    }

                    // (3) single contiguous chain: strictly descending,
                    //     every adjacent pair linked by prev_event_ulid.
                    //     (2) holds implicitly — a phantom in-window is
                    //     returned from HEAD, and the walk truncates
                    //     cleanly at a phantom past the window, so a
                    //     real event is never skipped over.
                    for pair in full.windows(2) {
                        assert!(
                            pair[0].event_ulid > pair[1].event_ulid,
                            "chain must be strictly descending"
                        );
                        assert_eq!(
                            pair[0].prev_event_ulid,
                            Some(pair[1].event_ulid),
                            "adjacent events must be back-linked"
                        );
                    }
                });
            }
        }
    }
}
