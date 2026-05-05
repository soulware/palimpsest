//! Process-global rescan signal.
//!
//! The daemon loop awaits [`wait`] to wake on demand; IPC handlers,
//! the import flow, and lifecycle helpers call [`trigger`] after work
//! the daemon should react to (volume created/started/released, import
//! finished, fork orchestrated).
//!
//! Implemented as a true compile-time `static` (not behind a
//! `OnceLock`/`Box::leak`) because [`Notify::const_new`] is a const-fn
//! and the type is internally synchronised — there is no setter to
//! forget and no init-ordering footgun.

use tokio::sync::Notify;

static SIGNAL: Notify = Notify::const_new();

/// Wake the daemon loop. Coalesces with any pending notification: a
/// burst of triggers between two `wait().await`s wakes the loop once.
pub fn trigger() {
    SIGNAL.notify_one();
}

/// Block until the next [`trigger`] call. Used by the daemon's main
/// loop in its `select!` arm.
pub async fn wait() {
    SIGNAL.notified().await;
}
