//! Discovery hook for the peer-fetch path: identify the previous
//! claimer of a volume from its signed event log so the prefetch tier
//! can warm from that host instead of S3.
//!
//! The handoff scenario this serves:
//!
//! 1. Coordinator A held volume V; cleanly released.
//! 2. Coordinator B is now running `volume claim` (or
//!    [`tasks::run_volume_tasks`] for an existing local fork).
//! 3. Before B's first prefetch tick fires, look up the latest event
//!    in `events/<name>/`. If it's a clean `Released` signed by A and
//!    A has published a peer-fetch endpoint, resolve and return it.
//!
//! Every failure path collapses to `None` — the prefetch loop falls
//! through to S3 silently. The discovery flow adds zero new state,
//! introduces no new artifacts, and is best-effort by design (per
//! `docs/notes/design-peer-segment-fetch.md` § "Discovery").
//!
//! Importantly, `force_released` is **not** a valid handoff signal:
//! the emitter of a `force_released` event is the *recovering*
//! coordinator, not the coordinator that previously held the volume's
//! cache warm. Attempting to peer-fetch from the recovering host
//! gives nothing; the only sensible fallback in that case is direct
//! S3, which is exactly what `None` selects.

use std::collections::HashMap;
use std::sync::Arc;

use elide_core::signing::VerifyingKey;
use elide_core::volume_event::{EventKind, VolumeEvent};
use elide_peer_fetch::PeerEndpoint;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::debug;
use ulid::Ulid;

use crate::identity;
use crate::ipc::SignatureStatus;
use crate::volume_event_store::verify_event_signature;

/// Result of discovery: who the previous claimer was, plus where to
/// reach them. Returned by [`discover_peer_for_claim`].
#[derive(Debug, Clone)]
pub struct DiscoveredPeer {
    /// Coordinator id of the previous (cleanly-released) claimer.
    pub coordinator_id: String,
    /// Reachable endpoint advertised by that coordinator at
    /// `coordinators/<id>/peer-endpoint.toml`.
    pub endpoint: PeerEndpoint,
}

/// Outcome of the backward walk inside [`discover_peer_for_claim`].
enum FindReleaserOutcome {
    /// Found a valid `Released` event; carry the releaser's
    /// coordinator id forward to endpoint resolution.
    Found(String),
    /// Walk terminated without a usable releaser — log empty,
    /// non-handoff event encountered, signature failure, fetch error,
    /// or pubkey unavailable. Caller returns `None`.
    Stop,
}

/// Walk `locations` (newest-first) one event at a time, fetching and
/// verifying each lazily, returning the coordinator id of the first
/// `Released` event found. Skips `Claimed` events; bails on anything
/// else.
///
/// `keys` is an in-out cache of `coordinator_id -> VerifyingKey`
/// shared across iterations — a cross-host handoff log typically
/// involves only two coordinators, so the cache amortises to one
/// pubkey GET per coord.
async fn find_releaser(
    store: &Arc<dyn ObjectStore>,
    volume_name: &str,
    locations: &[(Ulid, StorePath)],
    keys: &mut HashMap<String, VerifyingKey>,
) -> FindReleaserOutcome {
    for (_ulid, location) in locations {
        let body = match store.get(location).await {
            Ok(g) => match g.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    debug!("[peer-discovery {volume_name}] read {location}: {e}; skip peer");
                    return FindReleaserOutcome::Stop;
                }
            },
            Err(e) => {
                debug!("[peer-discovery {volume_name}] get {location}: {e}; skip peer");
                return FindReleaserOutcome::Stop;
            }
        };
        let text = match std::str::from_utf8(&body) {
            Ok(s) => s,
            Err(e) => {
                debug!("[peer-discovery {volume_name}] {location} not utf-8: {e}; skip peer");
                return FindReleaserOutcome::Stop;
            }
        };
        let event = match VolumeEvent::from_toml(text) {
            Ok(e) => e,
            Err(e) => {
                debug!("[peer-discovery {volume_name}] parse {location}: {e}; skip peer");
                return FindReleaserOutcome::Stop;
            }
        };

        // Resolve the verifying key for this event's signer (cached).
        let coord_id = event.coordinator_id.clone();
        let vk = if let Some(vk) = keys.get(&coord_id) {
            *vk
        } else {
            match identity::fetch_coordinator_pub(store.as_ref(), &coord_id).await {
                Ok(vk) => {
                    keys.insert(coord_id.clone(), vk);
                    vk
                }
                Err(e) => {
                    debug!(
                        "[peer-discovery {volume_name}] coord pubkey unavailable for \
                         {coord_id}: {e}; skip peer"
                    );
                    return FindReleaserOutcome::Stop;
                }
            }
        };

        match verify_event_signature(&event, &vk) {
            SignatureStatus::Valid => {}
            other => {
                debug!("[peer-discovery {volume_name}] event signature {other:?}; skip peer");
                return FindReleaserOutcome::Stop;
            }
        }

        match &event.kind {
            EventKind::Claimed => continue,
            EventKind::Released { .. } => return FindReleaserOutcome::Found(coord_id),
            other => {
                debug!(
                    "[peer-discovery {volume_name}] hit {} before Released; skip peer",
                    other.as_str()
                );
                return FindReleaserOutcome::Stop;
            }
        }
    }
    debug!("[peer-discovery {volume_name}] no Released event in log; skip peer");
    FindReleaserOutcome::Stop
}

/// Look up the previous claimer of `volume_name` via the volume event
/// log and resolve their peer-fetch endpoint. Returns `Some` only when
/// every step of the happy path succeeds; any failure (no events,
/// missing/invalid signature, non-`Released` latest event, no
/// published endpoint, store error) collapses to `None`.
///
/// The caller — typically the per-volume task at startup — uses the
/// returned peer as a one-shot warming hint for the next prefetch
/// tick. Subsequent prefetch ticks within the same volume task run
/// peer-less; this discovery is intentionally not persisted across
/// runs (the design doc explicitly puts persistent peer-fetch hints
/// out of scope for v1).
pub async fn discover_peer_for_claim(
    store: &Arc<dyn ObjectStore>,
    volume_name: &str,
) -> Option<DiscoveredPeer> {
    // List event-prefix locations once (one S3 LIST call). Filter +
    // sort *descending* by ULID so we visit newest first.
    let prefix = StorePath::from(format!("events/{volume_name}/"));
    let listed: Vec<_> = match store.list(Some(&prefix)).try_collect::<Vec<_>>().await {
        Ok(v) => v,
        Err(e) => {
            debug!("[peer-discovery {volume_name}] event log list failed; skip peer: {e}");
            return None;
        }
    };
    let mut locations: Vec<(Ulid, StorePath)> = listed
        .into_iter()
        .filter_map(|obj| {
            let filename = obj.location.filename()?;
            let stem = filename.strip_suffix(".toml")?;
            let ulid = Ulid::from_string(stem).ok()?;
            Some((ulid, obj.location))
        })
        .collect();
    locations.sort_by_key(|(ulid, _)| std::cmp::Reverse(*ulid));

    // Walk newest → oldest, fetching one event body at a time.
    // Short-circuits on every event kind except `Claimed`:
    //   - `Released`         → return the releaser's coordinator_id
    //   - `Claimed`          → skip past (it tells us nothing about
    //                          who held the cache warm); look further
    //   - anything else      → no clean handoff, return None
    //   - signature failure  → defensive bail, return None
    //
    // The typical case after a fresh claim is one or two GETs:
    //   - log tail `[…, Released-by-A, Claimed-by-B]` → 2 GETs
    //   - log tail `[…, Released-by-A]` (still unclaimed) → 1 GET
    //   - log tail ending in `ForceReleased`/`Created`/etc. → 1 GET
    // Pulling the full history was unnecessary — that's `list_events`
    // territory, kept for the CLI's `volume events` IPC.
    let mut keys: HashMap<String, VerifyingKey> = HashMap::new();
    let coord_id = match find_releaser(store, volume_name, &locations, &mut keys).await {
        FindReleaserOutcome::Found(coord_id) => coord_id,
        FindReleaserOutcome::Stop => return None,
    };

    let endpoint = match PeerEndpoint::fetch(store.as_ref(), &coord_id).await {
        Ok(Some(ep)) => ep,
        Ok(None) => {
            debug!(
                "[peer-discovery {volume_name}] previous claimer {coord_id} has no published \
                 peer-endpoint; skip peer"
            );
            return None;
        }
        Err(e) => {
            debug!(
                "[peer-discovery {volume_name}] resolving endpoint for {coord_id}: {e}; skip peer"
            );
            return None;
        }
    };

    debug!(
        "[peer-discovery {volume_name}] previous claimer {coord_id} reachable at {}",
        endpoint.url()
    );
    Some(DiscoveredPeer {
        coordinator_id: coord_id,
        endpoint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::CoordinatorIdentity;
    use crate::volume_event_store::emit_event;
    use elide_core::volume_event::EventKind;
    use object_store::memory::InMemory;
    use tempfile::TempDir;
    use ulid::Ulid;

    async fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// Build a coordinator identity in a fresh tempdir, publish its
    /// pubkey to the in-memory store at the canonical key, and return
    /// the identity.
    async fn make_coord(store: &Arc<dyn ObjectStore>) -> (CoordinatorIdentity, TempDir) {
        let dir = TempDir::new().unwrap();
        let ident = CoordinatorIdentity::load_or_generate(dir.path()).unwrap();
        ident.publish_pub(store.as_ref()).await.unwrap();
        (ident, dir)
    }

    #[tokio::test]
    async fn returns_none_when_event_log_is_empty() {
        let store = store().await;
        let result = discover_peer_for_claim(&store, "missing-vol").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_some_for_clean_released_with_endpoint() {
        let store = store().await;
        let (ident, _tmp) = make_coord(&store).await;
        let coord_id = ident.coordinator_id_str().to_owned();

        // Emit Created → Released, both signed by the same coord.
        let vol_ulid = Ulid::new();
        emit_event(&store, &ident, "vol", EventKind::Created, vol_ulid)
            .await
            .unwrap();
        emit_event(
            &store,
            &ident,
            "vol",
            EventKind::Released {
                handoff_snapshot: Ulid::new(),
            },
            vol_ulid,
        )
        .await
        .unwrap();

        // Publish the peer-endpoint for this coordinator.
        PeerEndpoint::new("10.0.0.42".to_owned(), 8443)
            .publish(store.as_ref(), &coord_id)
            .await
            .unwrap();

        let discovered = discover_peer_for_claim(&store, "vol")
            .await
            .expect("peer discovered");
        assert_eq!(discovered.coordinator_id, coord_id);
        assert_eq!(discovered.endpoint.url(), "http://10.0.0.42:8443");
    }

    /// Backward walk must short-circuit at the head: if the latest
    /// event is `Released`, neither older valid events nor older
    /// corrupt events should be fetched. Plants an unreadable event
    /// at the oldest position (lowest-ULID name) — if the walk
    /// reaches it, parsing fails and discovery returns `None`. The
    /// happy path here can't see that corruption because it stops
    /// at the head.
    #[tokio::test]
    async fn short_circuits_at_head_without_reading_older_events() {
        use bytes::Bytes;
        use object_store::path::Path as StorePath;

        let store = store().await;
        let (ident, _tmp) = make_coord(&store).await;
        let coord_id = ident.coordinator_id_str().to_owned();

        // Plant a deliberately-corrupt event at the lowest possible
        // ULID — guaranteed to sort to the back of the list. The
        // backward walk hits the newest entries first; if it
        // short-circuits, this is never fetched.
        let oldest_ulid = "01000000000000000000000000";
        store
            .put(
                &StorePath::from(format!("events/vol/{oldest_ulid}.toml")),
                Bytes::from_static(b"this is not valid toml at all").into(),
            )
            .await
            .unwrap();

        // Newest event: a clean Released. Should be the first thing
        // visited by the backward walk.
        let vol_ulid = Ulid::new();
        emit_event(
            &store,
            &ident,
            "vol",
            EventKind::Released {
                handoff_snapshot: Ulid::new(),
            },
            vol_ulid,
        )
        .await
        .unwrap();

        PeerEndpoint::new("10.0.0.42".to_owned(), 8443)
            .publish(store.as_ref(), &coord_id)
            .await
            .unwrap();

        let discovered = discover_peer_for_claim(&store, "vol")
            .await
            .expect("backward walk should short-circuit at the head's Released event");
        assert_eq!(discovered.coordinator_id, coord_id);
    }

    #[tokio::test]
    async fn returns_none_when_latest_event_is_not_released() {
        let store = store().await;
        let (ident, _tmp) = make_coord(&store).await;
        let vol_ulid = Ulid::new();
        emit_event(&store, &ident, "vol", EventKind::Created, vol_ulid)
            .await
            .unwrap();
        emit_event(&store, &ident, "vol", EventKind::Claimed, vol_ulid)
            .await
            .unwrap();
        // No Released event — latest is Claimed, which means the
        // volume is still owned. No peer-fetch handoff applies.

        let result = discover_peer_for_claim(&store, "vol").await;
        assert!(result.is_none());
    }

    /// Regression: when the foreign-claim flow emits its own `Claimed`
    /// event before discovery runs (as happens now that the rebind is
    /// folded into `fork_create_op`), the log tail looks like
    /// `[…, Released-by-A, Claimed-by-B]`. Discovery must walk back
    /// past the Claimed and resolve A as the releaser.
    #[tokio::test]
    async fn finds_releaser_when_claimed_already_emitted() {
        let store = store().await;
        let (a, _tmp_a) = make_coord(&store).await;
        let (b, _tmp_b) = make_coord(&store).await;
        let a_id = a.coordinator_id_str().to_owned();

        let vol_ulid = Ulid::new();
        emit_event(&store, &a, "vol", EventKind::Created, vol_ulid)
            .await
            .unwrap();
        emit_event(
            &store,
            &a,
            "vol",
            EventKind::Released {
                handoff_snapshot: Ulid::new(),
            },
            vol_ulid,
        )
        .await
        .unwrap();
        // B has already emitted its Claimed event by the time
        // discovery runs (this is the new ordering after the
        // rebind-into-fork-create refactor).
        emit_event(&store, &b, "vol", EventKind::Claimed, vol_ulid)
            .await
            .unwrap();

        // A advertises a peer-fetch endpoint.
        PeerEndpoint::new("10.0.0.42".to_owned(), 8443)
            .publish(store.as_ref(), &a_id)
            .await
            .unwrap();

        let discovered = discover_peer_for_claim(&store, "vol")
            .await
            .expect("discovery walks past B's Claimed and finds A's Release");
        assert_eq!(discovered.coordinator_id, a_id);
    }

    #[tokio::test]
    async fn returns_none_for_force_released() {
        let store = store().await;
        let (ident, _tmp) = make_coord(&store).await;
        let vol_ulid = Ulid::new();
        emit_event(&store, &ident, "vol", EventKind::Created, vol_ulid)
            .await
            .unwrap();
        // ForceReleased — the emitter is the recovering coordinator,
        // not the previous owner; peer-fetch must fall back to S3.
        emit_event(
            &store,
            &ident,
            "vol",
            EventKind::ForceReleased {
                handoff_snapshot: Ulid::new(),
                displaced_coordinator_id: "old-coord".to_owned(),
            },
            vol_ulid,
        )
        .await
        .unwrap();

        let result = discover_peer_for_claim(&store, "vol").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_endpoint_not_published() {
        let store = store().await;
        let (ident, _tmp) = make_coord(&store).await;
        let vol_ulid = Ulid::new();
        emit_event(&store, &ident, "vol", EventKind::Created, vol_ulid)
            .await
            .unwrap();
        emit_event(
            &store,
            &ident,
            "vol",
            EventKind::Released {
                handoff_snapshot: Ulid::new(),
            },
            vol_ulid,
        )
        .await
        .unwrap();
        // Skip publishing the peer-endpoint — coordinator either
        // didn't enable peer-fetch or hasn't started yet.

        let result = discover_peer_for_claim(&store, "vol").await;
        assert!(result.is_none());
    }
}
