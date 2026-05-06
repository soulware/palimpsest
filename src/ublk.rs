//! ublk transport (Linux userspace block device).
//!
//! Step-2b: multi-queue with queue_depth > 1, async per-tag handler, backend
//! work offloaded to a per-queue worker pool. Completions ride on the queue's
//! own io_uring so there is no cross-thread waker problem.
//!
//! **Per-queue layout.**
//! - One libublk `UblkQueue` (owns the queue's io_uring).
//! - One `smol::LocalExecutor` running on the queue thread.
//! - `QUEUE_DEPTH` async tasks, one per tag, each owning an `IoBuf`, an
//!   eventfd, and a shared `AtomicI32` result slot.
//! - `WORKERS_PER_QUEUE` std::thread workers, each owning its own
//!   `VolumeReader`, draining `Job`s from a crossbeam channel.
//!
//! **I/O flow for one tag.**
//! 1. `submit_io_prep_cmd` → await first kernel request for this tag.
//! 2. Read `iod`, send a `Job` to the worker pool over the channel.
//! 3. Build a `PollAdd(eventfd)` SQE and submit it via
//!    `ublk_submit_sqe_async` — this enters the queue's *own* ring, so the
//!    completion is reaped by libublk's normal event loop.
//! 4. Worker dispatches to `VolumeReader`, stores the result in the shared
//!    atomic, and `write(eventfd, 1)`.
//! 5. Eventfd CQE fires on the queue's ring → `wait_and_handle_io_events`
//!    wakes the `PollAdd` future → task resumes, reads the atomic,
//!    drains the eventfd, submits `submit_io_commit_cmd`.
//!
//! The eventfd is the bridge: writing to it produces a CQE on *the queue
//! thread's* ring, so the smol executor never sleeps past a ready waker.
//! That was the defect in the earlier `blocking::unblock` attempt.
//!
//! **Shutdown-park.** The device is always added with
//! `UBLK_F_USER_RECOVERY | UBLK_F_USER_RECOVERY_REISSUE`. SIGTERM/SIGINT/
//! SIGHUP and crashes (SIGKILL, OOM, panic) are treated identically: the
//! daemon process exits without calling `STOP_DEV` or `DEL_DEV`. The
//! kernel's monitor work observes the io_uring fds close and, with
//! `UBLK_F_USER_RECOVERY` set, transitions the device LIVE → QUIESCED
//! (it does **not** transition there via STOP_DEV — that always goes to
//! DEAD). The kernel buffers in-flight I/O; mounts on `/dev/ublkb<N>`
//! stay valid; the sysfs entry and the `[ublk] dev_id` field in
//! `volume.toml` persist. Re-running `elide serve-volume --ublk` reads
//! the bound id from `volume.toml`, sees the existing
//! `/sys/class/ublk-char/ublkcN` entry, **verifies the kernel device's
//! `target_data` ULID stamp matches this volume**, then issues
//! `START_USER_RECOVERY`, reattaches fresh queue rings, and completes
//! with `END_USER_RECOVERY` — the kernel then reissues buffered I/O to
//! the new daemon. WAL idempotence + lowest-ULID-wins already handles
//! duplicate writes, so reissue is safe.
//!
//! Before exiting on a graceful signal the watcher fsyncs the WAL via
//! `VolumeClient::flush` (bounded by a watchdog timer) so any write the
//! actor accepted is durable. Anything still in flight is reissued by
//! the kernel on the next `START_USER_RECOVERY`.
//!
//! Deletion is an explicit verb: `elide ublk delete <id>`, or the
//! coordinator's startup reconciliation sweep (which deletes orphan
//! kernel devices and clears bindings whose sysfs entry has gone, e.g.
//! after a host reboot). See `docs/design-ublk-shutdown-park.md`.
//!
//! **Volume ↔ device binding.** Two sources of state co-operate:
//!   * `volume.toml` `[ublk] dev_id` — local hint, written by the
//!     daemon's `wait_hook` after a successful ADD. Tells the next
//!     serve which id to probe.
//!   * Kernel `target_data.elide.volume_ulid` — authoritative
//!     ownership stamp, set in `tgt_init` on every ADD. The verify-
//!     on-recover guard refuses to attach to any device whose stamp
//!     does not match this volume's ULID, even if `volume.toml` and
//!     the CLI both name that id. This is what makes "bind to the
//!     wrong device" impossible regardless of how the local hint
//!     drifts (operator-driven `del_dev` followed by someone else
//!     ADDing at the same id, etc.).
//!
//! On a subsequent serve: if `volume.toml` and `--ublk-id` disagree,
//! refuse; if the sysfs entry exists but its owner stamp doesn't name
//! this volume, refuse; otherwise route to RECOVER (bound + sysfs
//! present + owner verified) or ADD (no live device at the bound id).
//!
//! Zero-copy (`UBLK_F_AUTO_BUF_REG`) is a follow-up step. See
//! docs/design-ublk-transport.md.
//!
//! On non-Linux targets, and on Linux without the `ublk` cargo feature, this
//! module compiles to a stub that errors when the transport is invoked.

use std::io;
use std::path::Path;

/// Outcome of [`run_volume_ublk`]. `Config` failures are permanent under the
/// current host configuration (missing kernel module, missing privilege,
/// wrong kernel version, persistent dev-id mismatch). The supervisor signals
/// these to the coordinator via process exit code [`EXIT_CONFIG`] so it does
/// not respawn in a tight loop.
#[derive(Debug)]
pub enum UblkRunError {
    Config(String),
    Other(io::Error),
}

impl std::fmt::Display for UblkRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "{msg}"),
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

impl From<io::Error> for UblkRunError {
    fn from(e: io::Error) -> Self {
        Self::Other(e)
    }
}

/// Exit code used by the volume process to signal "permanent misconfiguration —
/// don't respawn me". Matches BSD `EX_CONFIG` from `sysexits.h`.
pub const EXIT_CONFIG: i32 = 78;

#[cfg(all(target_os = "linux", feature = "ublk"))]
mod imp {
    use std::io;
    use std::num::NonZeroUsize;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::path::Path;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::time::Duration;

    use crossbeam_channel::{Receiver, Sender, unbounded};
    use libublk::BufDesc;
    use libublk::UblkError;
    use libublk::UblkFlags;
    use libublk::UblkUringData;
    use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
    use libublk::helpers::IoBuf;
    use libublk::io::{UblkDev, UblkQueue};
    use libublk::uring_async::ublk_submit_sqe_async;

    use elide_core::actor::{VolumeClient, VolumeReader};

    /// Elide-side payload stamped into libublk's per-device JSON
    /// (`/run/ublksrvd/<id>.json`, field `target_data`). The kernel-held
    /// `volume_ulid` is the authoritative ownership stamp: the verify-
    /// on-recover path refuses to attach to a device whose stamp doesn't
    /// match this volume's ULID, regardless of what `volume.toml` says.
    ///
    /// Wrapped under the `"elide"` key so libublk (or other code that
    /// learns to write target_data) can add sibling fields without
    /// clashing.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct UblkTargetData {
        elide: UblkBinding,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct UblkBinding {
        /// Absolute path to the volume directory that owns this kernel device.
        volume_dir: String,
        /// ULID basename of `volume_dir`. Recorded explicitly so readers
        /// don't have to re-derive it from the path.
        volume_ulid: String,
    }

    impl UblkTargetData {
        /// Build the payload for the given volume directory under the
        /// `by_id/<ulid>/` layout. Falls back to an empty ULID string
        /// when the basename is non-UTF8 or the directory is unusual —
        /// the `volume_dir` field is still authoritative for lookup.
        fn for_volume(vol_dir: &Path) -> Self {
            let volume_ulid = vol_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_owned();
            Self {
                elide: UblkBinding {
                    volume_dir: vol_dir.display().to_string(),
                    volume_ulid,
                },
            }
        }

        /// Parse the payload from libublk's `target_data` JSON value.
        /// Returns `None` when the value is absent or doesn't carry the
        /// `"elide"` key — i.e. the device wasn't created by this
        /// codebase, or by an older version that didn't yet stamp it.
        fn from_target_json(value: &serde_json::Value) -> Option<Self> {
            serde_json::from_value(value.clone()).ok()
        }
    }

    const BLOCK: u64 = 4096;
    const LOGICAL_BS_SHIFT: u8 = 12;
    const PHYSICAL_BS_SHIFT: u8 = 12;
    const IO_MIN_SHIFT: u8 = 12;
    const IO_OPT_SHIFT: u8 = 12;

    /// Per-I/O buffer size. Caps the largest single request the kernel will
    /// issue to userspace. 1 MiB matches the step-1 spike and comfortably
    /// covers typical blk-mq dispatch sizes (max_sectors_kb is usually
    /// 512–1280).
    const IO_BUF_BYTES: u32 = 1 << 20;

    /// In-flight requests per queue. Each tag owns one async task + one
    /// IoBuf + one eventfd + one atomic result slot.
    const QUEUE_DEPTH: u16 = 64;

    /// Upper bound on queue count. With blk-mq one queue per CPU is ideal for
    /// locality; capped so tiny hosts do not pay for idle queues.
    const MAX_QUEUES: u16 = 4;

    /// Backend worker threads per queue. Each worker owns a `VolumeReader`
    /// and drains jobs from the queue's crossbeam channel. At depth 64 we
    /// bottleneck on either the actor mailbox (writes) or the reader's fd
    /// cache miss / S3 demand-fetch (reads); 8 workers is plenty for the
    /// latter and writes serialise at the actor anyway.
    const WORKERS_PER_QUEUE: usize = 8;

    const UBLK_IO_OP_READ: u32 = libublk::sys::UBLK_IO_OP_READ;
    const UBLK_IO_OP_WRITE: u32 = libublk::sys::UBLK_IO_OP_WRITE;
    const UBLK_IO_OP_FLUSH: u32 = libublk::sys::UBLK_IO_OP_FLUSH;
    const UBLK_IO_OP_DISCARD: u32 = libublk::sys::UBLK_IO_OP_DISCARD;
    const UBLK_IO_OP_WRITE_ZEROES: u32 = libublk::sys::UBLK_IO_OP_WRITE_ZEROES;

    /// Startup decision: what the serve should do given the persisted
    /// volume↔device binding, the CLI `--ublk-id`, and whether a kernel
    /// sysfs entry already exists.
    #[derive(Debug, PartialEq, Eq)]
    enum Route {
        /// No prior binding that matches a live kernel device. ADD with
        /// `target_id` (letting the kernel auto-allocate when `None`).
        Add { target_id: Option<i32> },
        /// Bound to an id whose sysfs entry is present AND whose
        /// `target_data` ownership stamp matches this volume's ULID.
        /// Safe to RECOVER — the kernel-buffered I/O queued under the
        /// last serve was destined for *us*.
        Recover { id: i32 },
        /// `--ublk-id` contradicts what `volume.toml` says this volume
        /// is bound to. Refusing is the whole point of persisting the
        /// binding.
        BoundMismatch { persisted: i32, cli: i32 },
        /// A sysfs entry exists at the requested id but its `target_data`
        /// stamp does not name this volume — either another volume's
        /// QUIESCED device, a non-elide device, or an unstamped device
        /// from an older codebase. Reissuing someone else's buffered
        /// writes into our WAL would be silent corruption; refuse.
        ForeignDevice { id: i32 },
    }

    /// `sysfs_has(id)` → kernel device entry exists.
    /// `owner_matches(id)` → kernel `target_data.elide.volume_ulid`
    /// equals our volume's ULID. Always called *after* `sysfs_has(id)`
    /// is true, so the implementation may assume the device exists.
    ///
    /// Returns `Recover` only when both predicates hold *and* the
    /// persisted binding agrees; an `owner_matches` failure converts
    /// any candidate id into `ForeignDevice`. This is the load-bearing
    /// safety net: the kernel stamp is authoritative ownership, so a
    /// stale `volume.toml` (from operator del_dev + someone-else ADD)
    /// can never lead us into the wrong device.
    fn plan_route(
        persisted: Option<i32>,
        cli: Option<i32>,
        sysfs_has: impl Fn(i32) -> bool,
        owner_matches: impl Fn(i32) -> bool,
    ) -> Route {
        if let (Some(p), Some(c)) = (persisted, cli)
            && p != c
        {
            return Route::BoundMismatch {
                persisted: p,
                cli: c,
            };
        }
        let Some(id) = persisted.or(cli) else {
            return Route::Add { target_id: None };
        };
        if !sysfs_has(id) {
            return Route::Add {
                target_id: Some(id),
            };
        }
        if !owner_matches(id) {
            return Route::ForeignDevice { id };
        }
        match persisted {
            Some(p) if p == id => Route::Recover { id },
            _ => Route::ForeignDevice { id },
        }
    }

    /// Classify a `UblkCtrlBuilder::build()` failure into permanent
    /// (`Config`, supervisor parks volume.stopped) vs transient (`Other`,
    /// supervisor retries with backoff). Keeps the underlying libublk
    /// error verbatim in the message so the operator sees the actual
    /// errno rather than a guess at what went wrong.
    ///
    /// Permanent: kernel module missing, daemon lacks CAP_SYS_ADMIN.
    /// Transient: EBUSY/EEXIST from a stale or mid-deleting device,
    /// the io_uring control ring failing to set up, anything else.
    fn classify_build_error(
        e: UblkError,
        target_id: Option<i32>,
        recovering: bool,
    ) -> super::UblkRunError {
        let phase = if recovering {
            match target_id {
                Some(id) => format!("RECOVER_DEV(id={id})"),
                None => "RECOVER_DEV".to_owned(),
            }
        } else {
            match target_id {
                Some(id) => format!("ADD_DEV(id={id})"),
                None => "ADD_DEV(auto-id)".to_owned(),
            }
        };

        // libublk's Display for UringIOError(i32) drops the errno; render
        // it ourselves so the operator sees the actual kernel response
        // (EBUSY, EEXIST, ENODEV, …) rather than the bare "io_uring IO
        // failure" string.
        let rendered = match &e {
            UblkError::UringIOError(neg_errno) => {
                let errno = -neg_errno;
                let name = errno_name(errno).unwrap_or("?");
                format!("io_uring IO failure (errno {errno} {name})")
            }
            other => other.to_string(),
        };
        let detail = format!("ublk {phase} failed: {rendered}");

        // EEXIST/ENOSPC against a bound id means the kernel says "id N is
        // already in use" — and id N can't change without operator action,
        // so retrying with the same id will never succeed. Surface this as
        // a Config error so the supervisor parks volume.stopped instead of
        // backoff-looping silently. The most common cause when sysfs is
        // empty is a leaked kernel idr slot from a previous device whose
        // last reference never dropped.
        let id_in_use = matches!(
            &e,
            UblkError::UringIOError(neg_errno) if matches!(-*neg_errno, libc::EEXIST | libc::ENOSPC)
        ) && target_id.is_some();

        let module_or_priv = match &e {
            UblkError::UringIOError(neg_errno) => {
                matches!(
                    -neg_errno,
                    libc::ENODEV | libc::EPERM | libc::EACCES | libc::ENOTTY
                )
            }
            UblkError::IOError(io_err) => matches!(
                io_err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
            ),
            _ => false,
        };

        if id_in_use {
            let id = target_id.expect("id_in_use implies target_id.is_some()");
            super::UblkRunError::Config(format!(
                "{detail} — ublk dev id {id} is already in use at the kernel level. \
                 If /sys/class/ublk-char/ublkc{id} exists, run `elide ublk delete {id}` \
                 (or pick a different --ublk-id). If sysfs is empty but the slot is \
                 still pinned, an earlier device leaked a kernel reference; \
                 reload the module (`rmmod ublk_drv && modprobe ublk_drv`) or reboot"
            ))
        } else if module_or_priv {
            super::UblkRunError::Config(format!(
                "{detail} — likely the ublk_drv kernel module is not loaded \
                 or the daemon lacks CAP_SYS_ADMIN (rerun under sudo)"
            ))
        } else {
            super::UblkRunError::Other(io::Error::other(detail))
        }
    }

    /// Map a small set of common errnos to their symbolic names for log
    /// readability. Returns `None` for codes we don't expect to see from
    /// ublk control ioctls so the caller can just print the raw number.
    fn errno_name(errno: i32) -> Option<&'static str> {
        let name = match errno {
            libc::EPERM => "EPERM",
            libc::ENOENT => "ENOENT",
            libc::EIO => "EIO",
            libc::EBADF => "EBADF",
            libc::EAGAIN => "EAGAIN",
            libc::ENOMEM => "ENOMEM",
            libc::EACCES => "EACCES",
            libc::EFAULT => "EFAULT",
            libc::EBUSY => "EBUSY",
            libc::EEXIST => "EEXIST",
            libc::ENODEV => "ENODEV",
            libc::EINVAL => "EINVAL",
            libc::ENFILE => "ENFILE",
            libc::EMFILE => "EMFILE",
            libc::ENOTTY => "ENOTTY",
            libc::ENOSPC => "ENOSPC",
            libc::EROFS => "EROFS",
            libc::EOPNOTSUPP => "EOPNOTSUPP",
            libc::ETIMEDOUT => "ETIMEDOUT",
            _ => return None,
        };
        Some(name)
    }

    /// Query a ublk device's current state via GET_DEV_INFO. Returns the
    /// raw `UBLK_S_DEV_*` value. Opens a fresh `UblkCtrl::new_simple`, so
    /// it is safe to call even when no daemon owns the device.
    fn probe_dev_state(id: i32) -> io::Result<u16> {
        let ctrl = UblkCtrl::new_simple(id)
            .map_err(|e| io::Error::other(format!("ublk open ctrl for dev {id}: {e}")))?;
        Ok(ctrl.dev_info().state)
    }

    /// The set of signals the ublk transport reacts to (process termination
    /// signals — SIGINT for Ctrl-C, SIGTERM for orderly stop, SIGHUP for
    /// terminal-loss). Centralised so the block-then-watch sequence cannot
    /// drift between the mask installed at startup and the one signalfd
    /// reads from.
    fn shutdown_sigset() -> nix::sys::signal::SigSet {
        let mut s = nix::sys::signal::SigSet::empty();
        s.add(nix::sys::signal::Signal::SIGINT);
        s.add(nix::sys::signal::Signal::SIGTERM);
        s.add(nix::sys::signal::Signal::SIGHUP);
        s
    }

    /// How long to give the volume actor to fsync the WAL on graceful
    /// shutdown before forcing the process to exit anyway. Bounded so a
    /// wedged actor cannot trap the operator.
    const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(3);

    /// Spawn the ublk-signal watcher thread.
    ///
    /// The caller MUST have already blocked the shutdown signals on the
    /// current thread before calling this, so that every subsequently-spawned
    /// thread inherits the block and only this watcher reads the signals
    /// (via `signalfd`).
    ///
    /// On signal the watcher does NOT call `kill_dev`. The kernel ublk
    /// state machine treats `STOP_DEV` as the explicit "tear it down"
    /// verb and transitions the device to `DEAD` regardless of
    /// `UBLK_F_USER_RECOVERY` — there is no "STOP_DEV but go to QUIESCED"
    /// path. To leave the device parked in `QUIESCED` (so the next serve
    /// can `START_USER_RECOVERY` it without re-adding) the daemon must
    /// simply exit: the kernel's monitor work observes the io_uring fds
    /// close and, with `UBLK_F_USER_RECOVERY` set on the device,
    /// transitions `LIVE` → `QUIESCED`.
    ///
    /// Sequence on first signal:
    ///   1. Spawn a watchdog thread that force-exits after
    ///      `SHUTDOWN_FLUSH_TIMEOUT` so a wedged actor cannot block exit.
    ///   2. Best-effort `client.flush()` (a durability barrier that fsyncs
    ///      the WAL and waits for any in-flight promote's old-WAL fsync).
    ///      Anything the actor accepted before this call is durable;
    ///      anything still in flight after it is reissued by the kernel
    ///      from its `UBLK_F_USER_RECOVERY_REISSUE` buffer on next serve.
    ///   3. `_exit(0)`. Queue threads, control socket, actor thread are
    ///      all killed; their fds are reaped by the kernel, the io_uring
    ///      fds close, and the kernel parks the device.
    ///
    /// We use `libc::_exit` (direct `exit_group` syscall) rather than
    /// `std::process::exit` because the daemon's stdio fds may point at
    /// a now-destroyed pty (e.g. when `elide coord run` was started
    /// under `sudo` and Ctrl-C'd: sudo's pty master is closed, the
    /// slave dentry is unlinked, but the volume — having been spawned
    /// before that — still holds fds onto it). `process::exit` calls
    /// `libc::exit` which runs glibc's `__cxa_finalize` and stdio
    /// flush/fclose; both can deadlock against a destroyed pty,
    /// stranding the daemon. `_exit` skips all of that and goes
    /// straight to the kernel.
    ///
    /// A second signal short-circuits to `_exit(130)` so an impatient
    /// operator is never trapped.
    fn spawn_ublk_signal_watcher(
        client: VolumeClient,
        peer_counters: Option<elide_peer_fetch::PeerFetchCountersHandle>,
    ) -> io::Result<std::thread::JoinHandle<()>> {
        let sfd = nix::sys::signalfd::SignalFd::new(&shutdown_sigset())
            .map_err(|e| io::Error::other(format!("signalfd: {e}")))?;
        std::thread::Builder::new()
            .name("ublk-signal".into())
            .spawn(move || {
                // Single-shot: block until any of the masked signals
                // arrives, then run the bounded flush and exit. The
                // watchdog spawned below caps total shutdown time at
                // SHUTDOWN_FLUSH_TIMEOUT, so a second signal isn't
                // needed to escape — and a second signal couldn't be
                // serviced inline anyway, since `client.flush()` blocks
                // this thread.
                match sfd.read_signal() {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        tracing::error!(
                            "ublk-signal: signalfd read returned None on a blocking fd"
                        );
                        // SAFETY: _exit is async-signal-safe and never returns.
                        unsafe { libc::_exit(130) };
                    }
                    Err(e) => {
                        tracing::error!("ublk-signal: signalfd read failed: {e}");
                        unsafe { libc::_exit(130) };
                    }
                }

                // Watchdog: if the flush stalls (actor wedged, disk
                // unresponsive), force-exit after a bounded wait so the
                // kernel can park the device on its own. Races the flush
                // below; whichever calls _exit first wins.
                let _watchdog = std::thread::Builder::new()
                    .name("ublk-shutdown-watchdog".into())
                    .spawn(|| {
                        std::thread::sleep(SHUTDOWN_FLUSH_TIMEOUT);
                        tracing::warn!(
                            "ublk shutdown flush did not complete within {:?}; \
                             exiting anyway — kernel will park the device on \
                             daemon-exit detection",
                            SHUTDOWN_FLUSH_TIMEOUT
                        );
                        unsafe { libc::_exit(0) };
                    });

                if let Err(e) = client.flush() {
                    tracing::warn!("ublk shutdown flush: {e}");
                }
                crate::log_peer_fetch_counters_at_shutdown(peer_counters.as_ref());
                unsafe { libc::_exit(0) };
            })
            .map_err(io::Error::other)
    }

    pub fn run_volume_ublk(
        dir: &Path,
        size_bytes: u64,
        fetch_inputs: crate::VolumeFetchInputs,
        dev_id: Option<i32>,
    ) -> Result<(), super::UblkRunError> {
        // Block the shutdown signals on the calling thread BEFORE any
        // other thread is spawned. The block is inherited by every
        // subsequent thread (volume-actor, control-server, ublk queue
        // threads, libublk's internal helpers). Only the dedicated
        // ublk-signal watcher reads them, via signalfd. This eliminates
        // the prior "ctrlc spawns its own dispatcher we don't control"
        // and "any thread might service a signal" hazards.
        shutdown_sigset()
            .thread_block()
            .map_err(|e| io::Error::other(format!("block shutdown signals: {e}")))?;

        let by_id_dir = dir.parent().unwrap_or(dir);
        // Coordinator prefetch races supervisor spawn on freshly-claimed
        // forks: ancestor `.idx` files may not yet be on disk when this
        // process starts. The retry helper absorbs the NotFound window;
        // anything else propagates. NBD uses the same helper.
        let mut volume = crate::volume_open::open_volume_with_retry(dir, by_id_dir)?;

        let mut peer_counters: Option<elide_peer_fetch::PeerFetchCountersHandle> = None;
        if let Some(build) = crate::build_volume_fetcher(dir, fetch_inputs)? {
            let fetcher = build.fetcher;
            let s3_store = build.s3_store;
            let arc_fetcher: Arc<dyn elide_core::segment::SegmentFetcher> =
                Arc::clone(&fetcher) as _;
            let fork_dirs = volume.fork_dirs();
            let (lba_map, extent_index) = volume.snapshot_maps();
            volume.set_fetcher(Arc::clone(&arc_fetcher));
            let fetcher_for_swap = Arc::clone(&fetcher);
            let body_prefetch_done = crate::body_prefetch::spawn(
                fork_dirs.clone(),
                Arc::clone(&arc_fetcher),
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
            tracing::info!("[demand-fetch enabled]");
        }

        let (actor, client) = elide_core::actor::spawn(volume);
        let _actor_thread = std::thread::Builder::new()
            .name("volume-actor".into())
            .spawn(move || actor.run())
            .map_err(io::Error::other)?;

        let connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        crate::control::start(dir, client.clone(), Arc::clone(&connected))?;

        let nr_queues = pick_nr_queues();

        // Spawn the signal watcher before any potentially-blocking ioctl
        // (in particular `start_user_recover`, which retries on EBUSY for
        // up to 30s). On signal the watcher fsyncs the WAL and exits the
        // process directly — STOP_DEV/del_dev are deliberately not used,
        // so the kernel's daemon-exit detection parks the device in
        // QUIESCED for the next serve to recover. See
        // `docs/design-ublk-shutdown-park.md`.
        let _sig_watcher = spawn_ublk_signal_watcher(client.clone(), peer_counters)?;

        let persisted_id = elide_core::config::VolumeConfig::bound_ublk_id(dir)
            .map_err(|e| io::Error::other(format!("reading bound ublk id: {e}")))?;
        let our_ulid = dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_owned();
        let owner_matches = |id: i32| device_owner_ulid(id).as_deref() == Some(our_ulid.as_str());
        let route = plan_route(persisted_id, dev_id, ublk_device_exists, owner_matches);
        let (target_id, recovering) = match route {
            Route::Add { target_id } => (target_id, false),
            Route::Recover { id } => {
                // Recovery is only valid from QUIESCED (or FAIL_IO). A DEAD
                // device cannot be recovered — the kernel never transitions
                // DEAD → QUIESCED, so `start_user_recover` would EBUSY-spin
                // for 30s and then fail. Probe state: if DEAD, delete the
                // stale entry and fall through to Add at the same id.
                match probe_dev_state(id) {
                    Ok(state) if state == libublk::sys::UBLK_S_DEV_DEAD as u16 => {
                        tracing::warn!(
                            "ublk dev {id} is DEAD (not recoverable); deleting stale entry and re-adding"
                        );
                        if let Ok(c) = UblkCtrl::new_simple(id)
                            && let Err(e) = c.del_dev()
                        {
                            tracing::warn!("ublk del_dev for stale DEAD dev {id}: {e}");
                        }
                        (Some(id), false)
                    }
                    Ok(_) => (Some(id), true),
                    Err(e) => {
                        tracing::warn!(
                            "ublk dev {id}: could not probe state ({e}); attempting recovery anyway"
                        );
                        (Some(id), true)
                    }
                }
            }
            Route::BoundMismatch { persisted, cli } => {
                return Err(super::UblkRunError::Config(format!(
                    "volume bound to ublk dev {persisted}; refusing to serve with --ublk-id {cli}. \
                     run `elide volume update --ublk-id {persisted} <name>` to align config, \
                     or `elide ublk delete {persisted}` to drop the binding"
                )));
            }
            Route::ForeignDevice { id } => {
                return Err(super::UblkRunError::Config(format!(
                    "ublk dev {id} exists but is not stamped with this volume's ULID. \
                     run `elide ublk delete {id}` to remove the foreign device, or use a different --ublk-id"
                )));
            }
        };

        if recovering {
            let id = target_id.expect("Recover route always sets target_id");
            let simple = UblkCtrl::new_simple(id).map_err(|e| {
                io::Error::other(format!("ublk open ctrl for recovery of dev {id}: {e}"))
            })?;
            simple
                .start_user_recover()
                .map_err(|e| io::Error::other(format!("ublk start_user_recover {id}: {e}")))?;
            tracing::info!("[ublk dev {id}: resuming from QUIESCED — reissuing buffered I/O]");
        }

        let dev_lifecycle = if recovering {
            UblkFlags::UBLK_DEV_F_RECOVER_DEV
        } else {
            UblkFlags::UBLK_DEV_F_ADD_DEV
        };
        let ctrl_flags = (libublk::sys::UBLK_F_USER_RECOVERY
            | libublk::sys::UBLK_F_USER_RECOVERY_REISSUE) as u64;

        // UblkCtrlBuilder::build() opens /dev/ublk-control and issues
        // ADD_DEV/RECOVER_DEV. Failures fall into two buckets:
        //   * permanent host config (no kernel module, no privilege) →
        //     return Config so the supervisor parks volume.stopped instead
        //     of respawning in a loop;
        //   * transient (EBUSY/EEXIST from a stale device, transient
        //     io_uring trouble, kernel still finalizing a prior delete) →
        //     return Other so the supervisor retries with backoff.
        // `classify_build_error` makes that split and preserves the
        // underlying libublk error verbatim in the message.
        let ctrl = Arc::new(
            UblkCtrlBuilder::default()
                .name("elide")
                .id(target_id.unwrap_or(-1))
                .nr_queues(nr_queues)
                .depth(QUEUE_DEPTH)
                .io_buf_bytes(IO_BUF_BYTES)
                .ctrl_flags(ctrl_flags)
                .dev_flags(dev_lifecycle)
                .build()
                .map_err(|e| classify_build_error(e, target_id, recovering))?,
        );

        // Stamp the per-device JSON (`/run/ublksrvd/<id>.json`) with the
        // owning volume directory and ULID. Two consumers: (1) operators
        // and the `elide ublk` CLI use it to correlate kernel devices
        // with elide volumes; (2) the verify-on-recover guard reads it
        // back to confirm that a sysfs entry at our bound id is actually
        // ours before reattaching.
        let target_data = UblkTargetData::for_volume(dir);
        let tgt_init = move |dev: &mut UblkDev| {
            set_params(dev, size_bytes);
            match serde_json::to_value(&target_data) {
                Ok(v) => dev.set_target_json(v),
                Err(e) => tracing::warn!("ublk target_data serialization failed: {e}"),
            }
            Ok(())
        };

        // VolumeClient is Send + Sync + Clone, so it satisfies run_target's
        // queue-handler bound directly. Each queue thread constructs a pool
        // of VolumeReaders (one per worker) from its VolumeClient clone.
        let q_handler = {
            let client = client.clone();
            move |qid, dev: &UblkDev| {
                q_fn(qid, dev, client.clone());
            }
        };

        // `wait_hook` runs once the kernel has transitioned the device to
        // LIVE. That is the earliest safe moment to record the binding
        // into `volume.toml`: the kernel has committed to this id. Under
        // shutdown-park the field persists across both clean and unclean
        // daemon exits — the next serve reads it to know which id to
        // probe + recover. Authoritative ownership still lives in the
        // kernel `target_data` stamp (set in `tgt_init` below); the
        // local hint can be stale without compromising safety.
        let binding_dir: std::path::PathBuf = dir.to_path_buf();
        let wait_hook = move |d_ctrl: &UblkCtrl| {
            d_ctrl.dump();
            let id = d_ctrl.dev_info().dev_id as i32;
            if let Err(e) = elide_core::config::VolumeConfig::set_bound_ublk_id(&binding_dir, id) {
                tracing::error!("ublk record binding for dev {id} failed: {e}");
            }
            tracing::info!("[ublk device ready: /dev/ublkb{id}]");
        };

        // Run the queue threads to completion. Under shutdown-park this
        // call only returns by an unusual route: the only way out is
        // the signal watcher's `process::exit` (which kills this thread
        // along with everything else), or libublk's own `stop_dev`
        // detecting that the kernel device went away from another
        // context. The `run_target` Result is propagated for the latter
        // case; the signal path never reaches here.
        ctrl.run_target(tgt_init, q_handler, wait_hook)
            .map_err(|e| io::Error::other(format!("ublk run_target: {e}")))?;
        Ok(())
    }

    fn pick_nr_queues() -> u16 {
        let cpus = std::thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(1);
        let clamped = cpus.min(MAX_QUEUES as usize).max(1);
        clamped as u16
    }

    /// Populate device params so the kernel issues only 4K-aligned I/O and
    /// advertises DISCARD / WRITE_ZEROES support to the blk-mq layer.
    fn set_params(dev: &mut UblkDev, size_bytes: u64) {
        let tgt = &mut dev.tgt;
        tgt.dev_size = size_bytes;
        tgt.params = libublk::sys::ublk_params {
            types: libublk::sys::UBLK_PARAM_TYPE_BASIC | libublk::sys::UBLK_PARAM_TYPE_DISCARD,
            basic: libublk::sys::ublk_param_basic {
                logical_bs_shift: LOGICAL_BS_SHIFT,
                physical_bs_shift: PHYSICAL_BS_SHIFT,
                io_opt_shift: IO_OPT_SHIFT,
                io_min_shift: IO_MIN_SHIFT,
                max_sectors: dev.dev_info.max_io_buf_bytes >> 9,
                dev_sectors: size_bytes >> 9,
                ..Default::default()
            },
            discard: libublk::sys::ublk_param_discard {
                discard_alignment: 0,
                discard_granularity: BLOCK as u32,
                max_discard_sectors: u32::MAX,
                max_write_zeroes_sectors: u32::MAX,
                max_discard_segments: 1,
                ..Default::default()
            },
            ..Default::default()
        };
    }

    /// One unit of backend work handed from a tag's async task to the
    /// per-queue worker pool.
    ///
    /// The buffer is identified by raw pointer + length because `IoBuf` is
    /// owned by the async task and we only need to view it from the worker
    /// for the duration of this single dispatch (between `submit_io_prep_cmd`
    /// and `submit_io_commit_cmd`, the kernel is not touching the buffer and
    /// no other task can access this tag's buffer). `buf_ptr` is `usize`
    /// rather than a pointer type so `Job` is trivially `Send`.
    struct Job {
        op: u32,
        off: u64,
        bytes: u32,
        buf_ptr: usize,
        buf_len: usize,
        eventfd: RawFd,
        result: Arc<AtomicI32>,
    }

    /// Per-queue entry point. Runs on a dedicated thread spawned by
    /// libublk's `run_target`. Owns the queue's io_uring, the worker pool,
    /// and the per-tag async tasks.
    fn q_fn(qid: u16, dev: &UblkDev, client: VolumeClient) {
        let queue = match UblkQueue::new(qid, dev) {
            Ok(q) => Rc::new(q),
            Err(e) => {
                tracing::error!("ublk queue {qid} setup failed: {e}");
                return;
            }
        };

        let (job_tx, job_rx) = unbounded::<Job>();
        let worker_handles: Vec<_> = (0..WORKERS_PER_QUEUE)
            .map(|widx| {
                let rx = job_rx.clone();
                let reader = client.reader();
                std::thread::Builder::new()
                    .name(format!("ublk-q{qid}-w{widx}"))
                    .spawn(move || worker_loop(rx, reader))
            })
            .filter_map(|r| match r {
                Ok(h) => Some(h),
                Err(e) => {
                    tracing::error!("ublk queue {qid} worker spawn failed: {e}");
                    None
                }
            })
            .collect();
        // Drop the local copy of the receiver so the channel closes once the
        // per-tag tasks drop their senders on shutdown.
        drop(job_rx);

        let executor = Rc::new(smol::LocalExecutor::new());
        let mut tasks = Vec::with_capacity(QUEUE_DEPTH as usize);
        for tag in 0..QUEUE_DEPTH {
            let q = queue.clone();
            let job_tx = job_tx.clone();
            tasks.push(executor.spawn(async move {
                match io_task(&q, tag, job_tx).await {
                    Ok(()) => {}
                    Err(UblkError::QueueIsDown) => {}
                    Err(e) => tracing::error!("ublk queue {qid} tag {tag} task error: {e}"),
                }
            }));
        }
        // Drop the primary sender: every task holds its own clone, so the
        // channel will close once the last tag exits.
        drop(job_tx);

        let exe = executor.clone();
        smol::block_on(executor.run(async move {
            let tick = || while exe.try_tick() {};
            let done = || tasks.iter().all(|t| t.is_finished());
            // 1-second idle timeout: short enough that shutdown (signalled
            // by QueueIsDown) propagates promptly even if the kernel side
            // has gone quiet; long enough to avoid unnecessary syscalls.
            if let Err(e) = libublk::wait_and_handle_io_events(&queue, Some(1), tick, done).await {
                tracing::error!("ublk queue {qid} event loop failed: {e}");
            }
        }));

        for h in worker_handles {
            if let Err(e) = h.join() {
                tracing::error!("ublk queue {qid} worker join failed: {e:?}");
            }
        }
    }

    /// Per-tag async task. Own the IoBuf, eventfd, and result slot for the
    /// full queue lifetime. Each iteration: receive one I/O command from the
    /// kernel (via prep/commit), hand it to a worker, await completion via
    /// the eventfd, commit the result back.
    async fn io_task(q: &UblkQueue<'_>, tag: u16, job_tx: Sender<Job>) -> Result<(), UblkError> {
        let buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);
        let efd = make_eventfd().map_err(UblkError::IOError)?;
        let result = Arc::new(AtomicI32::new(0));

        // Initial FETCH_REQ: result = 0 tells the kernel there is no prior
        // I/O to commit. The future resolves when the kernel hands us the
        // first request for this tag.
        q.submit_io_prep_cmd(tag, BufDesc::Slice(buf.as_slice()), 0, Some(&buf))
            .await?;

        loop {
            let iod = *q.get_iod(tag);
            let op = iod.op_flags & 0xff;
            let off = iod.start_sector << 9;
            let bytes = iod.nr_sectors << 9;

            let buf_slice = buf.as_slice();
            let res = if (bytes as usize) <= buf_slice.len() {
                result.store(0, Ordering::Relaxed);
                let job = Job {
                    op,
                    off,
                    bytes,
                    buf_ptr: buf_slice.as_ptr() as usize,
                    buf_len: buf_slice.len(),
                    eventfd: efd.as_raw_fd(),
                    result: Arc::clone(&result),
                };
                if job_tx.send(job).is_err() {
                    // Worker pool has gone away — treat as shutdown.
                    return Err(UblkError::QueueIsDown);
                }

                // Wait for the worker to signal completion on our eventfd.
                // The POLL_ADD CQE is reaped by libublk's event loop (same
                // ring as the prep/commit SQEs), so this `await` is woken
                // directly by the queue's own io_uring — no cross-thread
                // waker-vs-uring stall.
                let sqe = io_uring::opcode::PollAdd::new(
                    io_uring::types::Fd(efd.as_raw_fd()),
                    libc::POLLIN as u32,
                )
                .build();
                ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await?;

                // Drain the eventfd counter so the next POLL_ADD actually
                // waits for a fresh completion rather than firing immediately.
                drain_eventfd(efd.as_raw_fd());

                result.load(Ordering::Acquire)
            } else {
                -libc::EINVAL
            };

            q.submit_io_commit_cmd(tag, BufDesc::Slice(buf.as_slice()), res)
                .await?;
        }
    }

    /// Worker thread: owns a `VolumeReader`, drains `Job`s from the channel,
    /// performs the synchronous dispatch, stores the result, and writes to
    /// the eventfd so the queue thread's `PollAdd` future wakes.
    fn worker_loop(rx: Receiver<Job>, reader: VolumeReader) {
        while let Ok(job) = rx.recv() {
            // SAFETY: The async task has exclusive logical ownership of the
            // tag's IoBuf between `submit_io_prep_cmd` and
            // `submit_io_commit_cmd`. It has sent us the job and is awaiting
            // our eventfd notification; it will not touch the buffer until
            // we signal. The kernel also does not touch the buffer in that
            // window. So constructing a &mut [u8] here is sound.
            let slice: &mut [u8] =
                unsafe { std::slice::from_raw_parts_mut(job.buf_ptr as *mut u8, job.buf_len) };
            let res = if (job.bytes as usize) <= slice.len() {
                dispatch(
                    &reader,
                    job.op,
                    job.off,
                    job.bytes,
                    &mut slice[..job.bytes as usize],
                )
            } else {
                -libc::EINVAL
            };
            job.result.store(res, Ordering::Release);

            // Signal the tag's async task by incrementing the eventfd
            // counter. Any short write counts as an error worth logging but
            // does not require aborting the worker.
            let counter: u64 = 1;
            let n = unsafe {
                libc::write(
                    job.eventfd,
                    &counter as *const u64 as *const libc::c_void,
                    std::mem::size_of::<u64>(),
                )
            };
            if n != std::mem::size_of::<u64>() as isize {
                tracing::error!(
                    "ublk worker eventfd write returned {n} (expected 8): {}",
                    io::Error::last_os_error()
                );
            }
        }
    }

    fn make_eventfd() -> io::Result<OwnedFd> {
        // SAFETY: eventfd is a simple file-descriptor syscall. The returned
        // fd is wrapped in OwnedFd for automatic close-on-drop.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn drain_eventfd(fd: RawFd) {
        // The counter is monotonic; a single read resets it to 0. We don't
        // care about the value — only that the counter is cleared so the
        // next POLL_ADD blocks again.
        let mut buf = [0u8; 8];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::WouldBlock {
                tracing::error!("ublk eventfd drain read failed: {err}");
            }
        }
    }

    /// Translate one ublk I/O into a `VolumeReader` / `VolumeClient` call.
    /// Returns the kernel completion status: bytes on success, negative errno
    /// on failure.
    fn dispatch(reader: &VolumeReader, op: u32, offset: u64, length: u32, buf: &mut [u8]) -> i32 {
        // ublk SET_PARAMS pinned logical_bs_shift=12, so offset and length
        // are always 4K-aligned — no RMW path needed.
        debug_assert!(offset.is_multiple_of(BLOCK));
        debug_assert!((length as u64).is_multiple_of(BLOCK));

        let start_lba = offset / BLOCK;
        let lba_count = (length as u64 / BLOCK) as u32;

        match op {
            UBLK_IO_OP_READ => match reader.read(start_lba, lba_count) {
                Ok(data) => {
                    let len = data.len().min(length as usize);
                    buf[..len].copy_from_slice(&data[..len]);
                    len as i32
                }
                Err(e) => {
                    tracing::error!("[ublk read error offset={offset} len={length}: {e}]");
                    -libc::EIO
                }
            },
            UBLK_IO_OP_WRITE => {
                let data = buf[..length as usize].to_vec();
                match reader.write(start_lba, data) {
                    Ok(()) => length as i32,
                    Err(e) => {
                        tracing::error!("[ublk write error offset={offset} len={length}: {e}]");
                        -libc::EIO
                    }
                }
            }
            UBLK_IO_OP_FLUSH => match reader.flush() {
                Ok(()) => 0,
                Err(e) => {
                    tracing::error!("[ublk flush error: {e}]");
                    -libc::EIO
                }
            },
            UBLK_IO_OP_DISCARD => match reader.trim(start_lba, lba_count) {
                Ok(()) => length as i32,
                Err(e) => {
                    tracing::error!("[ublk discard error offset={offset} len={length}: {e}]");
                    -libc::EIO
                }
            },
            UBLK_IO_OP_WRITE_ZEROES => match reader.write_zeroes(start_lba, lba_count) {
                Ok(()) => length as i32,
                Err(e) => {
                    tracing::error!("[ublk write-zeroes error offset={offset} len={length}: {e}]");
                    -libc::EIO
                }
            },
            _ => -libc::EINVAL,
        }
    }

    pub fn list_devices() -> io::Result<()> {
        use libublk::ctrl::UblkCtrl;

        let ids = scan_dev_ids()?;
        if ids.is_empty() {
            println!("no ublk devices");
            return Ok(());
        }
        for id in ids {
            match UblkCtrl::new_simple(id) {
                Ok(ctrl) => ctrl.dump(),
                Err(e) => eprintln!("ublk{id}: failed to open ctrl: {e}"),
            }
        }
        Ok(())
    }

    pub fn delete_device(id: i32) -> io::Result<()> {
        use libublk::ctrl::UblkCtrl;

        let ctrl = UblkCtrl::new_simple(id)
            .map_err(|e| io::Error::other(format!("open ctrl for dev {id}: {e}")))?;

        // If the device was created by an elide volume daemon, target_data
        // tells us which volume owns it. Stop that daemon gracefully before
        // tearing down the kernel device — otherwise libublk's queue threads
        // race the del_dev and may leave a wedged process behind.
        let owner = ctrl
            .get_target_data_from_json()
            .as_ref()
            .and_then(UblkTargetData::from_target_json);
        if let Some(ref owner) = owner {
            stop_owning_daemon(id, Path::new(&owner.elide.volume_dir));
        }

        // kill_dev is the documented safe-from-anywhere stop; del_dev then
        // removes the kernel entry and libublk's json file.
        let _ = ctrl.kill_dev();
        ctrl.del_dev()
            .map_err(|e| io::Error::other(format!("del_dev {id}: {e}")))?;

        // Now that the kernel device is gone, clear the volume's bound
        // dev_id in volume.toml so the next serve does a fresh ADD
        // instead of trying to recover a device that no longer exists.
        // Keeps the [ublk] section: removing the device is a binding
        // change, not a transport-policy change.
        if let Some(owner) = owner {
            let vol_dir = Path::new(&owner.elide.volume_dir);
            if let Err(e) = elide_core::config::VolumeConfig::clear_bound_ublk_id(vol_dir) {
                eprintln!(
                    "warning: failed to clear bound dev_id in {}: {e} \
                     (next serve may try to recover dev {id})",
                    vol_dir.display()
                );
            }
            println!(
                "deleted ublk device {id} (volume {})",
                owner.elide.volume_ulid
            );
        } else {
            println!("deleted ublk device {id}");
        }
        Ok(())
    }

    /// SIGTERM the volume daemon recorded in `<vol_dir>/volume.pid` and
    /// poll until it exits or the bounded grace expires. Best-effort
    /// throughout: a missing/unparseable pid file or an unsignalable
    /// pid means there's no owning daemon to stop, which is fine.
    fn stop_owning_daemon(id: i32, vol_dir: &Path) {
        use elide_coordinator::volume_state::PID_FILE;
        use elide_core::process::pid_is_alive;

        let pid_path = vol_dir.join(PID_FILE);
        let Ok(text) = std::fs::read_to_string(&pid_path) else {
            return;
        };
        let Ok(pid) = text.trim().parse::<u32>() else {
            return;
        };
        if !pid_is_alive(pid) {
            return;
        }

        let Ok(raw) = i32::try_from(pid) else {
            return;
        };
        let nix_pid = nix::unistd::Pid::from_raw(raw);
        eprintln!("ublk{id}: signalling owning volume daemon (pid {pid}) to stop");
        if let Err(e) = nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGTERM) {
            eprintln!("warning: SIGTERM pid {pid}: {e} (proceeding with del_dev anyway)");
            return;
        }

        // Poll for exit. Bound the wait so a wedged daemon can't trap the
        // operator — del_dev will then race the live daemon, which is no
        // worse than the pre-target_data behaviour.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if !pid_is_alive(pid) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        eprintln!("warning: pid {pid} did not exit within 10s; proceeding with del_dev");
    }

    pub fn delete_all_devices() -> io::Result<()> {
        let ids = scan_dev_ids()?;
        if ids.is_empty() {
            println!("no ublk devices");
            return Ok(());
        }
        for id in ids {
            if let Err(e) = delete_device(id) {
                eprintln!("ublk{id}: {e}");
            }
        }
        Ok(())
    }

    /// True when `/sys/class/ublk-char/ublkc<id>` is present — i.e. the
    /// kernel still has a device registered under that id. Under
    /// shutdown-park the entry outlives every daemon exit (the kernel
    /// transitions to QUIESCED rather than tearing down), and is the
    /// signal for USER_RECOVERY_REISSUE to kick in on the next serve.
    fn ublk_device_exists(id: i32) -> bool {
        std::path::Path::new(&format!("/sys/class/ublk-char/ublkc{id}")).exists()
    }

    /// Read the kernel device's `target_data` stamp and return its
    /// `volume_ulid`, or `None` if the device cannot be opened, has no
    /// stamp, or the stamp does not parse. Used by the verify-on-recover
    /// guard to confirm we own the device before reattaching.
    fn device_owner_ulid(id: i32) -> Option<String> {
        let ctrl = UblkCtrl::new_simple(id).ok()?;
        let value = ctrl.get_target_data_from_json()?;
        UblkTargetData::from_target_json(&value).map(|td| td.elide.volume_ulid)
    }

    /// Scan `/sys/class/ublk-char` for `ublkcN` entries and return the ids.
    /// This mirrors what `libublk::ctrl::UblkCtrl::for_each_dev_id` does
    /// internally, but without its `Fn + Clone + 'static` closure bound that
    /// prevents borrowing mutable state.
    fn scan_dev_ids() -> io::Result<Vec<i32>> {
        let mut ids = Vec::new();
        let entries = match std::fs::read_dir("/sys/class/ublk-char") {
            Ok(d) => d,
            // Missing directory means no devices / module not loaded.
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(ids),
            Err(e) => return Err(e),
        };
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str()
                && let Some(rest) = name.strip_prefix("ublkc")
                && let Ok(id) = rest.parse::<i32>()
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        use elide_core::actor::{VolumeClient, VolumeReader};
        use elide_core::volume::Volume;

        static COUNTER: AtomicU64 = AtomicU64::new(0);

        fn temp_dir() -> std::path::PathBuf {
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let mut p = std::env::temp_dir();
            p.push(format!("elide-ublk-test-{}-{}", std::process::id(), n));
            std::fs::create_dir_all(&p).unwrap();
            elide_core::signing::generate_keypair(
                &p,
                elide_core::signing::VOLUME_KEY_FILE,
                elide_core::signing::VOLUME_PUB_FILE,
            )
            .unwrap();
            p
        }

        /// Build a live VolumeClient + VolumeReader backed by a scratch volume
        /// so tests can drive `dispatch` exactly as the per-queue worker does.
        /// Returns the temp dir, client (kept alive so the actor thread stays
        /// up), and reader. Drop order: reader → client → dir.
        fn spawn_volume() -> (std::path::PathBuf, VolumeClient, VolumeReader) {
            let dir = temp_dir();
            let volume = Volume::open(&dir, &dir).unwrap();
            let (actor, client) = elide_core::actor::spawn(volume);
            std::thread::Builder::new()
                .name("ublk-test-actor".into())
                .spawn(move || actor.run())
                .unwrap();
            let reader = client.reader();
            (dir, client, reader)
        }

        #[test]
        fn target_data_roundtrip_carries_volume_dir_and_ulid() {
            let dir = std::path::PathBuf::from("/elide_data/by_id/01ABCDEF");
            let payload = UblkTargetData::for_volume(&dir);
            assert_eq!(payload.elide.volume_ulid, "01ABCDEF");
            assert_eq!(payload.elide.volume_dir, "/elide_data/by_id/01ABCDEF");

            let value = serde_json::to_value(&payload).unwrap();
            let parsed = UblkTargetData::from_target_json(&value).expect("parses back");
            assert_eq!(parsed.elide.volume_ulid, "01ABCDEF");
            assert_eq!(parsed.elide.volume_dir, "/elide_data/by_id/01ABCDEF");
        }

        #[test]
        fn target_data_parse_returns_none_for_foreign_payload() {
            // A device created by something other than elide (or by an
            // older binary that didn't stamp target_data) lacks the
            // "elide" key. Parsing must not fall back to defaults — the
            // CLI uses None to mean "no owning daemon to stop".
            let foreign = serde_json::json!({ "other_target": { "x": 1 } });
            assert!(UblkTargetData::from_target_json(&foreign).is_none());
        }

        #[test]
        fn ublk_device_exists_false_for_absent_id() {
            // i32::MAX is vastly larger than any real device id the kernel
            // would allocate, so /sys/class/ublk-char/ublkc<MAX> never
            // exists — gives us a robust "missing" case that works on any
            // host, ublk loaded or not.
            assert!(!ublk_device_exists(i32::MAX));
        }

        // The owner predicate represents "kernel target_data ULID
        // matches our volume". Tests pass `|_| true` for "owner ok" and
        // `|_| false` for "stamp absent or someone else's".
        const OWNER_OK: fn(i32) -> bool = |_| true;
        const OWNER_FOREIGN: fn(i32) -> bool = |_| false;

        #[test]
        fn plan_route_add_when_no_prior_state() {
            assert_eq!(
                plan_route(None, None, |_| false, OWNER_OK),
                Route::Add { target_id: None }
            );
            assert_eq!(
                plan_route(None, Some(3), |_| false, OWNER_OK),
                Route::Add { target_id: Some(3) }
            );
        }

        #[test]
        fn plan_route_recover_on_bound_and_present_with_owner_match() {
            // Persisted == CLI, sysfs has it, our owner stamp: canonical
            // recover after a clean shutdown.
            assert_eq!(
                plan_route(Some(5), Some(5), |id| id == 5, OWNER_OK),
                Route::Recover { id: 5 }
            );
            // CLI omitted but persisted id is present: recover without
            // needing the operator to re-type the id.
            assert_eq!(
                plan_route(Some(5), None, |id| id == 5, OWNER_OK),
                Route::Recover { id: 5 }
            );
        }

        #[test]
        fn plan_route_recover_refuses_when_owner_stamp_doesnt_match() {
            // Sysfs has the persisted id, but the kernel device's
            // target_data names a different volume (e.g. operator
            // del_dev'd ours and someone else ADDed at this id). This
            // is the load-bearing safety net — never bind to the wrong
            // device.
            assert_eq!(
                plan_route(Some(5), Some(5), |id| id == 5, OWNER_FOREIGN),
                Route::ForeignDevice { id: 5 }
            );
        }

        #[test]
        fn plan_route_add_rebinds_to_persisted_when_sysfs_gone() {
            // volume.toml says "this volume is dev 5", but the kernel
            // entry is gone (e.g. operator ran `elide ublk delete 5`).
            // ADD with the persisted id so the volume reclaims its slot.
            // owner_matches isn't called because sysfs_has is false first.
            assert_eq!(
                plan_route(Some(5), None, |_| false, OWNER_OK),
                Route::Add { target_id: Some(5) }
            );
        }

        #[test]
        fn plan_route_bound_mismatch_refuses() {
            // The whole point of persisting the binding.
            assert_eq!(
                plan_route(Some(5), Some(7), |_| true, OWNER_OK),
                Route::BoundMismatch {
                    persisted: 5,
                    cli: 7,
                }
            );
            // Mismatch still refuses even if the CLI id has no sysfs
            // entry — catch the operator mistake early, don't let them
            // silently drift off the persisted binding.
            assert_eq!(
                plan_route(Some(5), Some(7), |_| false, OWNER_OK),
                Route::BoundMismatch {
                    persisted: 5,
                    cli: 7,
                }
            );
        }

        #[test]
        fn plan_route_foreign_device_refuses_no_persisted_binding() {
            // No persisted binding, CLI says id 5, ublkc5 exists — even
            // if its owner stamp happened to match us, we never ADDed it
            // so refuse on persisted-binding grounds (the sysfs entry
            // wasn't ours to begin with). Owner check is the second
            // gate; persisted-binding presence is the first.
            assert_eq!(
                plan_route(None, Some(5), |_| true, OWNER_OK),
                Route::ForeignDevice { id: 5 }
            );
            // Owner mismatch is also ForeignDevice (caught by the
            // owner_matches gate before the persisted check).
            assert_eq!(
                plan_route(None, Some(5), |_| true, OWNER_FOREIGN),
                Route::ForeignDevice { id: 5 }
            );
        }

        #[test]
        fn classify_build_error_transient_for_busy() {
            // EBUSY is the canonical "kernel still finalising a prior delete";
            // ENOENT/EAGAIN cover unrelated transient io_uring conditions.
            // EEXIST/ENOSPC are deliberately not in this set — they are
            // permanent for bound ids (see the dedicated test below) and
            // unreachable for auto-allocated ids (the kernel picks a free
            // slot, so collision is impossible).
            for errno in [libc::EBUSY, libc::ENOENT, libc::EAGAIN] {
                let err = classify_build_error(
                    UblkError::UringIOError(-errno),
                    Some(0),
                    /*recovering=*/ false,
                );
                assert!(
                    matches!(err, super::super::UblkRunError::Other(_)),
                    "errno {errno} should be transient (Other), got {err:?}"
                );
                let msg = err.to_string();
                assert!(
                    msg.contains(&format!("errno {errno}")),
                    "expected errno {errno} in message, got: {msg}"
                );
            }
        }

        #[test]
        fn classify_build_error_permanent_for_bound_id_in_use() {
            // EEXIST/ENOSPC against a bound id means the kernel idr slot is
            // taken. Retrying with the same id can never succeed — the
            // supervisor must park rather than backoff-loop silently.
            for errno in [libc::EEXIST, libc::ENOSPC] {
                let err = classify_build_error(
                    UblkError::UringIOError(-errno),
                    Some(7),
                    /*recovering=*/ false,
                );
                assert!(
                    matches!(err, super::super::UblkRunError::Config(_)),
                    "errno {errno} with bound id should be permanent (Config), got {err:?}"
                );
                let msg = err.to_string();
                assert!(
                    msg.contains("dev id 7 is already in use"),
                    "expected id-in-use hint mentioning id 7: {msg}"
                );
                assert!(
                    msg.contains("elide ublk delete 7"),
                    "expected operator hint mentioning the bound id: {msg}"
                );
            }
        }

        #[test]
        fn classify_build_error_transient_for_eexist_when_auto() {
            // Auto-allocated id (target_id=None) cannot legitimately EEXIST
            // — the kernel picks. If we somehow see it, treat as transient
            // rather than permanently parking the volume.
            let err = classify_build_error(
                UblkError::UringIOError(-libc::EEXIST),
                None,
                /*recovering=*/ false,
            );
            assert!(
                matches!(err, super::super::UblkRunError::Other(_)),
                "EEXIST with auto id should be transient (Other), got {err:?}"
            );
        }

        #[test]
        fn classify_build_error_permanent_for_priv_and_module() {
            for errno in [libc::ENODEV, libc::EPERM, libc::EACCES, libc::ENOTTY] {
                let err = classify_build_error(
                    UblkError::UringIOError(-errno),
                    None,
                    /*recovering=*/ false,
                );
                assert!(
                    matches!(err, super::super::UblkRunError::Config(_)),
                    "errno {errno} should be permanent (Config), got {err:?}"
                );
            }
        }

        #[test]
        fn classify_build_error_renders_phase_and_recovery() {
            let add = classify_build_error(UblkError::UringIOError(-libc::EBUSY), Some(2), false);
            assert!(
                add.to_string().contains("ADD_DEV(id=2)"),
                "expected ADD_DEV phase: {add}"
            );

            let recover =
                classify_build_error(UblkError::UringIOError(-libc::EBUSY), Some(2), true);
            assert!(
                recover.to_string().contains("RECOVER_DEV(id=2)"),
                "expected RECOVER_DEV phase: {recover}"
            );

            let auto = classify_build_error(UblkError::UringIOError(-libc::EBUSY), None, false);
            assert!(
                auto.to_string().contains("ADD_DEV(auto-id)"),
                "expected ADD_DEV(auto-id): {auto}"
            );
        }

        #[test]
        fn pick_nr_queues_clamped_to_max() {
            let n = pick_nr_queues();
            assert!(n >= 1);
            assert!(n <= MAX_QUEUES);
        }

        #[test]
        fn eventfd_roundtrip() {
            let efd = make_eventfd().unwrap();
            let counter: u64 = 1;
            let n = unsafe {
                libc::write(
                    efd.as_raw_fd(),
                    &counter as *const u64 as *const libc::c_void,
                    std::mem::size_of::<u64>(),
                )
            };
            assert_eq!(n, std::mem::size_of::<u64>() as isize);

            // First drain consumes the counter.
            drain_eventfd(efd.as_raw_fd());

            // Second drain would block (EAGAIN) because EFD_NONBLOCK is set;
            // drain_eventfd silently swallows WouldBlock, so this must not
            // panic or log.
            drain_eventfd(efd.as_raw_fd());
        }

        #[test]
        fn dispatch_unwritten_read_returns_zeros() {
            let (dir, client, reader) = spawn_volume();
            let mut buf = [0xabu8; BLOCK as usize];
            let res = dispatch(&reader, UBLK_IO_OP_READ, 0, BLOCK as u32, &mut buf);
            assert_eq!(res, BLOCK as i32);
            assert!(buf.iter().all(|&b| b == 0));
            drop(reader);
            client.shutdown();
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn dispatch_write_then_read_roundtrip() {
            let (dir, client, reader) = spawn_volume();
            let data: Vec<u8> = (0..BLOCK as u16).map(|i| (i & 0xff) as u8).collect();
            let mut wbuf = data.clone();
            let res = dispatch(&reader, UBLK_IO_OP_WRITE, 0, BLOCK as u32, &mut wbuf);
            assert_eq!(res, BLOCK as i32);

            let mut rbuf = vec![0u8; BLOCK as usize];
            let res = dispatch(&reader, UBLK_IO_OP_READ, 0, BLOCK as u32, &mut rbuf);
            assert_eq!(res, BLOCK as i32);
            assert_eq!(rbuf, data);

            drop(reader);
            client.shutdown();
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn dispatch_flush_returns_zero() {
            let (dir, client, reader) = spawn_volume();
            let mut buf = [0u8; 0];
            let res = dispatch(&reader, UBLK_IO_OP_FLUSH, 0, 0, &mut buf);
            assert_eq!(res, 0);
            drop(reader);
            client.shutdown();
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn dispatch_discard_clears_block() {
            let (dir, client, reader) = spawn_volume();
            let mut wbuf = vec![0xcdu8; BLOCK as usize];
            assert_eq!(
                dispatch(&reader, UBLK_IO_OP_WRITE, 0, BLOCK as u32, &mut wbuf),
                BLOCK as i32
            );

            let mut dbuf = [0u8; 0];
            let res = dispatch(&reader, UBLK_IO_OP_DISCARD, 0, BLOCK as u32, &mut dbuf);
            assert_eq!(res, BLOCK as i32);

            let mut rbuf = vec![0xffu8; BLOCK as usize];
            assert_eq!(
                dispatch(&reader, UBLK_IO_OP_READ, 0, BLOCK as u32, &mut rbuf),
                BLOCK as i32
            );
            assert!(rbuf.iter().all(|&b| b == 0));

            drop(reader);
            client.shutdown();
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn dispatch_write_zeroes_clears_block() {
            let (dir, client, reader) = spawn_volume();
            let mut wbuf = vec![0xcdu8; BLOCK as usize];
            assert_eq!(
                dispatch(&reader, UBLK_IO_OP_WRITE, 0, BLOCK as u32, &mut wbuf),
                BLOCK as i32
            );

            let mut zbuf = [0u8; 0];
            let res = dispatch(&reader, UBLK_IO_OP_WRITE_ZEROES, 0, BLOCK as u32, &mut zbuf);
            assert_eq!(res, BLOCK as i32);

            let mut rbuf = vec![0xffu8; BLOCK as usize];
            assert_eq!(
                dispatch(&reader, UBLK_IO_OP_READ, 0, BLOCK as u32, &mut rbuf),
                BLOCK as i32
            );
            assert!(rbuf.iter().all(|&b| b == 0));

            drop(reader);
            client.shutdown();
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn dispatch_unknown_op_returns_einval() {
            let (dir, client, reader) = spawn_volume();
            let mut buf = [0u8; BLOCK as usize];
            // 0xff is not any of the UBLK_IO_OP_* values we handle.
            let res = dispatch(&reader, 0xff, 0, BLOCK as u32, &mut buf);
            assert_eq!(res, -libc::EINVAL);
            drop(reader);
            client.shutdown();
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

#[cfg(not(all(target_os = "linux", feature = "ublk")))]
mod imp {
    use std::io;
    use std::path::Path;

    pub fn run_volume_ublk(
        _dir: &Path,
        _size_bytes: u64,
        _fetch_inputs: crate::VolumeFetchInputs,
        _dev_id: Option<i32>,
    ) -> Result<(), super::UblkRunError> {
        Err(super::UblkRunError::Config(stub_msg().to_owned()))
    }

    pub fn list_devices() -> io::Result<()> {
        Err(io::Error::other(stub_msg()))
    }

    pub fn delete_device(_id: i32) -> io::Result<()> {
        Err(io::Error::other(stub_msg()))
    }

    pub fn delete_all_devices() -> io::Result<()> {
        Err(io::Error::other(stub_msg()))
    }

    fn stub_msg() -> &'static str {
        "ublk transport requires Linux and the 'ublk' cargo feature"
    }
}

/// Serve a volume over ublk. Creates `/dev/ublkbN` and runs the I/O loop.
///
/// Step-2b: multi-queue (up to 4) at queue_depth = 64 with an async per-tag
/// handler and a per-queue worker pool. `dev_id = None` lets the kernel
/// auto-allocate. See docs/design-ublk-transport.md.
pub fn run_volume_ublk(
    dir: &Path,
    size_bytes: u64,
    fetch_inputs: crate::VolumeFetchInputs,
    dev_id: Option<i32>,
) -> Result<(), UblkRunError> {
    imp::run_volume_ublk(dir, size_bytes, fetch_inputs, dev_id)
}

/// List ublk devices known to the kernel (reads `/sys/class/ublk-char`).
pub fn list_devices() -> io::Result<()> {
    imp::list_devices()
}

/// Delete a single ublk device by id. Stops it first (safe even if already
/// stopped) and then removes the kernel entry and libublk's json file.
pub fn delete_device(id: i32) -> io::Result<()> {
    imp::delete_device(id)
}

/// Delete every ublk device found in `/sys/class/ublk-char`.
pub fn delete_all_devices() -> io::Result<()> {
    imp::delete_all_devices()
}
