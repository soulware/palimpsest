pub mod bins;
pub mod config;
pub mod control;
pub mod eligibility;
pub mod gc;
pub mod gc_cycle;
pub mod identity;
pub mod ipc;
pub mod lifecycle;
pub mod local_cond_store;
pub mod name_store;
pub mod park;
pub mod peer_discovery;
pub mod portable;
pub mod prefetch;
pub mod pull;
pub mod range_fetcher;
pub mod reaper;
pub mod recovery;
pub mod retention;
pub mod stores;
pub mod tasks;
pub mod upload;
pub mod volume_event_store;
pub mod volume_state;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use ulid::Ulid;

/// Registry of per-fork eviction channels.
///
/// Maps each fork directory path to its evict request sender.  The coordinator
/// daemon populates this on fork discovery; the inbound handler looks up the
/// sender to forward eviction requests into the fork's task loop.
pub type EvictRegistry =
    Arc<Mutex<HashMap<PathBuf, mpsc::Sender<(Option<String>, tasks::EvictReply)>>>>;

/// Registry of per-fork snapshot locks.
///
/// The outer `Mutex` guards the HashMap and is never held across `.await`.
/// The inner `AsyncMutex` is held by the snapshot inbound handler for the
/// full duration of a snapshot sequence (flush → inline drain → sign
/// manifest → upload) across multiple `.await`s, so it must be a tokio
/// mutex. The per-volume tick loop `try_lock`s the inner mutex before
/// running drain/GC/eviction and skips the volume for that tick if held.
///
/// The lock exists only to keep the coordinator's own background tick loop
/// off a volume mid-snapshot. Volume-actor commands are already serialised
/// through the actor channel, so intra-volume commands never race regardless
/// of this lock.
pub type SnapshotLockRegistry = Arc<Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>>;

/// Construct an empty `SnapshotLockRegistry`.
pub fn new_snapshot_lock_registry() -> SnapshotLockRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Get or create the per-fork snapshot lock for `fork_dir`.
pub fn snapshot_lock_for(
    registry: &SnapshotLockRegistry,
    fork_dir: &std::path::Path,
) -> Arc<AsyncMutex<()>> {
    let mut map = registry.lock().expect("snapshot lock registry poisoned");
    map.entry(fork_dir.to_owned())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

/// Per-fork prefetch state. `None` means prefetch is still running; `Some(Ok(()))`
/// means prefetch completed successfully; `Some(Err(_))` means prefetch failed and
/// the fork is not safe to open. Future-tense state lets late subscribers see the
/// terminal value via `Sender::subscribe()` without missing the publisher's send.
pub type PrefetchState = Option<Result<(), String>>;

/// Tracker exposing per-fork prefetch completion to the inbound IPC.
///
/// The tracker stores an `Arc<watch::Sender>` per ULID. Both the volume-
/// creating IPC handler (`fork_create_op` pre-registers before returning to
/// the CLI) and the daemon's discovery loop (which spawns
/// `run_volume_tasks`) obtain the same sender via [`register_prefetch_or_get`]
/// — whichever runs first inserts; the other gets the same handle. This
/// closes the race where the CLI's `await-prefetch` could hit the
/// "untracked → ok" path before the daemon registered the entry.
///
/// Subscribers obtain a fresh receiver via [`subscribe_prefetch`]
/// (`tx.subscribe()` under the lock).
///
/// The "task exited unexpectedly" signal is preserved by the per-fork task
/// removing its tracker entry on exit (via a Drop guard in
/// `run_volume_tasks`). When both the tracker's `Arc<Sender>` and the
/// task's local `Arc<Sender>` are dropped, the underlying watch channel
/// has no more senders, and pending subscribers' `changed().await`
/// returns `Err` — surfaced by the IPC as "task exited without
/// publishing a result".
pub type PrefetchTracker = Arc<Mutex<HashMap<Ulid, Arc<watch::Sender<PrefetchState>>>>>;

/// Construct an empty [`PrefetchTracker`].
pub fn new_prefetch_tracker() -> PrefetchTracker {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Idempotent: insert an entry for `vol_ulid` if absent, or return the
/// existing sender. Used both by `fork_create_op` (pre-registering on
/// volume creation, so the CLI's subsequent `await-prefetch` always finds
/// an entry) and by the daemon's first-time discovery — whichever path
/// runs first inserts; the other gets the same `Arc`.
pub fn register_prefetch_or_get(
    tracker: &PrefetchTracker,
    vol_ulid: Ulid,
) -> Arc<watch::Sender<PrefetchState>> {
    let mut guard = tracker.lock().expect("prefetch tracker poisoned");
    guard
        .entry(vol_ulid)
        .or_insert_with(|| {
            let (tx, _rx) = watch::channel(None);
            Arc::new(tx)
        })
        .clone()
}

/// Force-replace the entry for `vol_ulid`, returning the new sender. Used
/// on delete-and-recreate at the same ULID (inode-change rediscovery in
/// the daemon): any prior subscribers see the dropped previous sender via
/// `changed().await -> Err` and can retry.
pub fn replace_prefetch(
    tracker: &PrefetchTracker,
    vol_ulid: Ulid,
) -> Arc<watch::Sender<PrefetchState>> {
    let mut guard = tracker.lock().expect("prefetch tracker poisoned");
    let (tx, _rx) = watch::channel(None);
    let new = Arc::new(tx);
    guard.insert(vol_ulid, new.clone());
    new
}

/// Subscribe to the per-fork prefetch state. Returns `None` when the fork
/// is not tracked (caller treats this as "ready"), or a fresh receiver
/// pinned to the current state.
pub fn subscribe_prefetch(
    tracker: &PrefetchTracker,
    vol_ulid: &Ulid,
) -> Option<watch::Receiver<PrefetchState>> {
    tracker
        .lock()
        .expect("prefetch tracker poisoned")
        .get(vol_ulid)
        .map(|tx| tx.subscribe())
}

/// Remove the entry for `vol_ulid`. Called by the per-fork task on exit
/// (via a Drop guard in `run_volume_tasks`) so the tracker's
/// `Arc<Sender>` is released; combined with the task dropping its own
/// local `Arc<Sender>`, this leaves the watch channel with no senders
/// and unblocks pending subscribers with `changed() -> Err`.
pub fn unregister_prefetch(tracker: &PrefetchTracker, vol_ulid: &Ulid) {
    if let Ok(mut guard) = tracker.lock() {
        guard.remove(vol_ulid);
    }
}

#[cfg(test)]
mod prefetch_tracker_tests {
    use super::*;

    fn vol() -> Ulid {
        Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap()
    }

    /// `register_prefetch_or_get` is idempotent: repeated calls for the
    /// same ULID return the *same* underlying sender. This is the
    /// invariant `fork_create_op` (pre-register) and the daemon's
    /// discovery path (post-register) rely on to converge on a single
    /// channel without races.
    #[test]
    fn register_prefetch_or_get_returns_same_sender_on_second_call() {
        let tracker = new_prefetch_tracker();
        let v = vol();
        let tx1 = register_prefetch_or_get(&tracker, v);
        let tx2 = register_prefetch_or_get(&tracker, v);
        assert!(
            Arc::ptr_eq(&tx1, &tx2),
            "second call must return the same Arc<Sender>"
        );
        // Sanity: a subscriber sees publishes through *either* handle.
        let mut rx = subscribe_prefetch(&tracker, &v).expect("entry must be registered");
        tx1.send_replace(Some(Ok(())));
        assert_eq!(rx.borrow_and_update().clone(), Some(Ok(())));
    }

    /// `replace_prefetch` deliberately abandons the previous channel:
    /// any pre-existing subscriber sees `changed().await -> Err` because
    /// the previous Arc<Sender> drops when the tracker entry is replaced.
    /// This is the inode-change rediscovery path in the daemon.
    #[tokio::test]
    async fn replace_prefetch_drops_previous_channel_for_subscribers() {
        let tracker = new_prefetch_tracker();
        let v = vol();
        let _tx1 = register_prefetch_or_get(&tracker, v);
        let mut rx = subscribe_prefetch(&tracker, &v).expect("entry must be registered");

        // Force-replace; drop the previous local Arc to ensure the
        // tracker held the only other reference.
        let _tx2 = replace_prefetch(&tracker, v);
        drop(_tx1);

        // Old subscriber: previous channel has no senders → Err.
        let result = rx.changed().await;
        assert!(result.is_err(), "previous subscriber must see Err");
    }

    /// `unregister_prefetch` drops the tracker's `Arc<Sender>`. Combined
    /// with the per-fork task dropping its own local Arc<Sender> (the
    /// `prefetch_done` parameter), pending subscribers see Err — the
    /// "task exited unexpectedly" signal.
    #[tokio::test]
    async fn unregister_prefetch_with_dropped_local_unblocks_subscribers() {
        let tracker = new_prefetch_tracker();
        let v = vol();
        let tx = register_prefetch_or_get(&tracker, v);
        let mut rx = subscribe_prefetch(&tracker, &v).expect("entry must be registered");

        unregister_prefetch(&tracker, &v);
        drop(tx);

        let result = rx.changed().await;
        assert!(result.is_err(), "subscriber must see Err after exit");
        // After unregister, late subscribers find no entry.
        assert!(subscribe_prefetch(&tracker, &v).is_none());
    }
}
