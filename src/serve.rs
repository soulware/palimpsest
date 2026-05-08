// IPC-only volume daemon and shared signal handling.
//
// `run_volume_ipc_only` opens the volume, starts the actor + control
// socket, and blocks until SIGTERM. Used when a volume is supervised
// for IPC purposes only — without ublk attached.
//
// The signal-handling helpers are also reused by the ublk transport's
// lifecycle (block-then-watch via signalfd). Keeping them here means
// the IPC-only path doesn't depend on the ublk module compiling.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "linux")]
use tracing::{error, warn};

/// Bound on volume-actor flush latency at shutdown. Matches ublk so
/// operators see the same cap across transports.
#[cfg(target_os = "linux")]
const SHUTDOWN_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

// Global flag set by SIGUSR1 handler; toggled by the volume daemon to
// emit diagnostic counters. Kept here rather than per-module so the
// signal disposition is installed once.
static PRINT_STATS: AtomicBool = AtomicBool::new(false);

extern "C" fn sigusr1_handler(_: libc::c_int) {
    PRINT_STATS.store(true, Ordering::Relaxed);
}

pub fn install_sigusr1_handler() {
    unsafe {
        libc::signal(
            libc::SIGUSR1,
            sigusr1_handler as *const () as libc::sighandler_t,
        );
    }
}

#[cfg(target_os = "linux")]
fn shutdown_sigset() -> nix::sys::signal::SigSet {
    let mut s = nix::sys::signal::SigSet::empty();
    s.add(nix::sys::signal::Signal::SIGINT);
    s.add(nix::sys::signal::Signal::SIGTERM);
    s.add(nix::sys::signal::Signal::SIGHUP);
    s
}

/// Block the shutdown signals on the calling thread before any other
/// thread is spawned. Inherited by every subsequent thread so only the
/// dedicated signal watcher reads them via signalfd.
pub fn block_shutdown_signals() -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        shutdown_sigset()
            .thread_block()
            .map_err(|e| io::Error::other(format!("block shutdown signals: {e}")))?;
    }
    Ok(())
}

/// Spawn a shutdown-signal watcher thread.
///
/// On signal: spawn a watchdog enforcing `SHUTDOWN_FLUSH_TIMEOUT`,
/// best-effort `client.flush()`, log peer-fetch counters, then
/// `_exit(0)`. signalfd is Linux-only; on other targets the spawned
/// thread is a stub that exits immediately.
pub fn spawn_signal_watcher(
    client: elide_core::actor::VolumeClient,
    peer_counters: Option<elide_peer_fetch::PeerFetchCountersHandle>,
) -> io::Result<std::thread::JoinHandle<()>> {
    #[cfg(target_os = "linux")]
    {
        let sfd = nix::sys::signalfd::SignalFd::new(&shutdown_sigset())
            .map_err(|e| io::Error::other(format!("signalfd: {e}")))?;
        std::thread::Builder::new()
            .name("volume-signal".into())
            .spawn(move || {
                match sfd.read_signal() {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        error!("volume-signal: signalfd read returned None on a blocking fd");
                        unsafe { libc::_exit(130) };
                    }
                    Err(e) => {
                        error!("volume-signal: signalfd read failed: {e}");
                        unsafe { libc::_exit(130) };
                    }
                }

                let _watchdog = std::thread::Builder::new()
                    .name("volume-shutdown-watchdog".into())
                    .spawn(|| {
                        std::thread::sleep(SHUTDOWN_FLUSH_TIMEOUT);
                        warn!(
                            "volume shutdown flush did not complete within {:?}; exiting anyway",
                            SHUTDOWN_FLUSH_TIMEOUT
                        );
                        unsafe { libc::_exit(0) };
                    });

                if let Err(e) = client.flush() {
                    warn!("volume shutdown flush: {e}");
                }
                crate::log_peer_fetch_counters_at_shutdown(peer_counters.as_ref());
                unsafe { libc::_exit(0) };
            })
            .map_err(io::Error::other)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (client, peer_counters);
        std::thread::Builder::new()
            .name("volume-signal-stub".into())
            .spawn(|| {})
            .map_err(io::Error::other)
    }
}

/// Open a volume and start the coordinator control socket; do not
/// attach a block-device transport. Blocks until SIGTERM (or the
/// process is otherwise killed).
///
/// Used when the coordinator supervises a volume for IPC purposes
/// only — no client is attached and no ublk device is requested.
pub fn run_volume_ipc_only(dir: &Path, fetch_inputs: crate::VolumeFetchInputs) -> io::Result<()> {
    block_shutdown_signals()?;
    install_sigusr1_handler();

    let by_id_dir = dir.parent().unwrap_or(dir);
    let mut volume = crate::volume_open::open_volume_with_retry(dir, by_id_dir)?;

    let mut peer_counters: Option<elide_peer_fetch::PeerFetchCountersHandle> = None;
    if let Some(build) = crate::build_volume_fetcher(dir, fetch_inputs)? {
        let fetcher = build.fetcher;
        let s3_store = build.s3_store;
        let arc_fetcher: Arc<dyn elide_core::segment::SegmentFetcher> = Arc::clone(&fetcher) as _;
        let fork_dirs = volume.fork_dirs();
        let (lba_map, extent_index) = volume.snapshot_maps();
        volume.set_fetcher(Arc::clone(&arc_fetcher));
        let fetcher_for_swap = Arc::clone(&fetcher);
        let body_prefetch_done = crate::body_prefetch::spawn(
            fork_dirs.clone(),
            Arc::clone(&arc_fetcher),
            Arc::clone(&extent_index),
            move || fetcher_for_swap.set_store(s3_store),
        );
        crate::full_warm::spawn(
            dir.to_path_buf(),
            fork_dirs,
            lba_map,
            extent_index,
            arc_fetcher,
            body_prefetch_done,
        );
        peer_counters = build.peer_counters;
    }

    let (actor, handle) = elide_core::actor::spawn(volume);
    let _actor_thread = std::thread::Builder::new()
        .name("volume-actor".into())
        .spawn(move || actor.run())
        .map_err(io::Error::other)?;
    let connected = Arc::new(AtomicBool::new(false));
    crate::control::start(dir, handle.clone(), connected)?;

    let _sig_watcher = spawn_signal_watcher(handle, peer_counters)?;

    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
