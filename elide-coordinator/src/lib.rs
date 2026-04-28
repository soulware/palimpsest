pub mod config;
pub mod control;
pub mod gc;
pub mod lifecycle;
pub mod local_cond_store;
pub mod name_store;
pub mod portable;
pub mod prefetch;
pub mod pull;
pub mod range_fetcher;
pub mod reaper;
pub mod retention;
pub mod tasks;
pub mod upload;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, mpsc};

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
