pub mod config;
pub mod control;
pub mod gc;
pub mod prefetch;
pub mod range_fetcher;
pub mod tasks;
pub mod upload;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

/// Registry of per-fork eviction channels.
///
/// Maps each fork directory path to its evict request sender.  The coordinator
/// daemon populates this on fork discovery; the inbound handler looks up the
/// sender to forward eviction requests into the fork's task loop.
pub type EvictRegistry =
    Arc<Mutex<HashMap<PathBuf, mpsc::Sender<(Option<String>, tasks::EvictReply)>>>>;

/// Registry of per-fork snapshot locks.
///
/// Held by the coordinator snapshot inbound handler for the full duration of
/// a snapshot sequence (flush → inline drain → sign manifest → upload). The
/// per-volume tick loop `try_lock`s this before running drain/GC/eviction
/// for that fork and skips the volume for that tick if the lock is held.
///
/// The lock exists only to keep the coordinator's own background tick loop
/// off a volume mid-snapshot. Volume-actor commands are already serialised
/// through the actor channel, so intra-volume commands never race regardless
/// of this lock.
pub type SnapshotLockRegistry = Arc<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>>;

/// Construct an empty `SnapshotLockRegistry`.
pub fn new_snapshot_lock_registry() -> SnapshotLockRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Get or create the per-fork snapshot lock for `fork_dir`.
pub async fn snapshot_lock_for(
    registry: &SnapshotLockRegistry,
    fork_dir: &std::path::Path,
) -> Arc<Mutex<()>> {
    let mut map = registry.lock().await;
    map.entry(fork_dir.to_owned())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}
