pub mod config;
pub mod control;
pub mod gc;
pub mod identity;
pub mod lifecycle;
pub mod local_cond_store;
pub mod name_store;
pub mod portable;
pub mod prefetch;
pub mod pull;
pub mod range_fetcher;
pub mod reaper;
pub mod recovery;
pub mod retention;
pub mod tasks;
pub mod upload;

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
/// On volume discovery, the daemon calls [`register_prefetch`] to obtain a
/// `watch::Sender` for the per-fork task. The matching `Receiver` is stored
/// in the tracker, and `await-prefetch` subscribers clone it to read state.
///
/// Storing the Receiver (not the Sender) means the per-fork task's Sender is
/// the *only* live sender; when the task drops it, all in-flight subscribers'
/// `changed()` calls return `Err`, which the IPC surfaces as a clear "task
/// exited" error rather than hanging forever.
pub type PrefetchTracker = Arc<Mutex<HashMap<Ulid, watch::Receiver<PrefetchState>>>>;

/// Construct an empty [`PrefetchTracker`].
pub fn new_prefetch_tracker() -> PrefetchTracker {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Insert (or replace) the entry for `vol_ulid`, returning the sole sender
/// for the per-fork task to publish into. Replacing is the right behaviour on
/// a re-discovery (delete-and-recreate at the same ULID): old subscribers see
/// the dropped previous sender via `changed() -> Err` and can retry.
pub fn register_prefetch(
    tracker: &PrefetchTracker,
    vol_ulid: Ulid,
) -> watch::Sender<PrefetchState> {
    let (tx, rx) = watch::channel(None);
    tracker
        .lock()
        .expect("prefetch tracker poisoned")
        .insert(vol_ulid, rx);
    tx
}

/// Subscribe to the per-fork prefetch state. Returns `None` when the fork is
/// not tracked (caller treats this as "ready"), or a fresh receiver pinned to
/// the current state.
pub fn subscribe_prefetch(
    tracker: &PrefetchTracker,
    vol_ulid: &Ulid,
) -> Option<watch::Receiver<PrefetchState>> {
    tracker
        .lock()
        .expect("prefetch tracker poisoned")
        .get(vol_ulid)
        .cloned()
}
