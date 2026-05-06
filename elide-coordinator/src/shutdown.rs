//! Process-global shutdown signal.
//!
//! Mirrors [`crate::rescan`] but carries the `keep_volumes` flag set by
//! the IPC handler. The daemon's main loop awaits [`wait`] in its
//! `select!`; when triggered, it returns the flag so the teardown branch
//! knows whether to SIGTERM volume children or leave them running for a
//! rolling upgrade.

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

static SIGNAL: Notify = Notify::const_new();
static KEEP_VOLUMES: AtomicBool = AtomicBool::new(false);

/// Request shutdown. `keep_volumes = true` is the rolling-upgrade path:
/// the coordinator exits but does not signal its volume children, which
/// continue running detached (setsid). `keep_volumes = false` is the
/// full-teardown path that matches the existing Ctrl+C behaviour.
pub fn trigger(keep_volumes: bool) {
    KEEP_VOLUMES.store(keep_volumes, Ordering::SeqCst);
    SIGNAL.notify_one();
}

/// Block until [`trigger`] is called. Returns the `keep_volumes` flag
/// from the most recent trigger.
pub async fn wait() -> bool {
    SIGNAL.notified().await;
    KEEP_VOLUMES.load(Ordering::SeqCst)
}
