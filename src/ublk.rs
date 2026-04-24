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
//! **Crash recovery.** The device is always added with
//! `UBLK_F_USER_RECOVERY | UBLK_F_USER_RECOVERY_REISSUE`. If the daemon
//! exits without a clean `STOP_DEV`/`DEL_DEV` (SIGKILL, OOM, panic), the
//! kernel transitions the device to QUIESCED and holds in-flight I/O.
//! Re-running `elide serve --ublk --ublk-id N` with the same id sees the
//! existing `/sys/class/ublk-char/ublkcN` entry, issues
//! `START_USER_RECOVERY`, reattaches fresh queue rings, and completes with
//! `END_USER_RECOVERY` — the kernel then reissues buffered I/O to the new
//! daemon. WAL idempotence + lowest-ULID-wins already handles duplicate
//! writes, so reissue is safe.
//!
//! **Volume ↔ device binding.** A sysfs `ublkcN` entry on its own does not
//! identify which volume the device was serving. To keep recovery from
//! reissuing one volume's buffered writes into a *different* volume's WAL,
//! each successful ADD records the kernel-assigned id in `<volume>/ublk.id`
//! (per-host runtime state, cleared on clean shutdown). On subsequent
//! serve: if `ublk.id` and `--ublk-id` disagree, refuse; if the sysfs
//! entry exists for an id the volume never bound to, refuse; otherwise
//! route to RECOVER (bound + sysfs present) or ADD (bound or absent).
//!
//! Zero-copy (`UBLK_F_AUTO_BUF_REG`) is a follow-up step. See
//! docs/design-ublk-transport.md.
//!
//! On non-Linux targets, and on Linux without the `ublk` cargo feature, this
//! module compiles to a stub that errors when the transport is invoked.

use std::io;
use std::path::Path;

#[cfg(all(target_os = "linux", feature = "ublk"))]
mod imp {
    use std::io;
    use std::num::NonZeroUsize;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::path::Path;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::{Arc, Mutex};

    use crossbeam_channel::{Receiver, Sender, unbounded};
    use libublk::BufDesc;
    use libublk::UblkError;
    use libublk::UblkFlags;
    use libublk::UblkUringData;
    use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder, ublk_init_ctrl_task_ring};
    use libublk::helpers::IoBuf;
    use libublk::io::{UblkDev, UblkQueue};
    use libublk::uring_async::ublk_submit_sqe_async;

    use elide_core::actor::{VolumeClient, VolumeReader};
    use elide_core::volume::Volume;

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
        /// Bound to an id whose sysfs entry is present — the kernel is
        /// holding a QUIESCED device for us. RECOVER.
        Recover { id: i32 },
        /// `--ublk-id` contradicts what `ublk.id` says this volume is
        /// bound to. Refusing is the whole point of persisting the binding.
        BoundMismatch { persisted: i32, cli: i32 },
        /// A sysfs entry exists for an id this volume never bound to —
        /// probably another volume's QUIESCED device, or a stale entry
        /// from a daemon that died before recording its binding. Reissuing
        /// someone else's buffered writes into this volume's WAL would be
        /// silent corruption; refuse.
        ForeignDevice { id: i32 },
    }

    fn plan_route(
        persisted: Option<i32>,
        cli: Option<i32>,
        sysfs_has: impl Fn(i32) -> bool,
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
        match persisted {
            Some(p) if p == id => Route::Recover { id },
            _ => Route::ForeignDevice { id },
        }
    }

    /// Query a ublk device's current state via GET_DEV_INFO. Returns the
    /// raw `UBLK_S_DEV_*` value. Opens a fresh `UblkCtrl::new_simple`, so
    /// it is safe to call even when no daemon owns the device.
    fn probe_dev_state(id: i32) -> io::Result<u16> {
        let ctrl = UblkCtrl::new_simple(id)
            .map_err(|e| io::Error::other(format!("ublk open ctrl for dev {id}: {e}")))?;
        Ok(ctrl.dev_info().state)
    }

    /// Install the SIGINT/SIGTERM/SIGHUP handler once at the top of
    /// `run_volume_ublk`. The handler looks at `slot`:
    ///
    /// - If `Some(ctrl)`: call `kill_dev` to unblock `run_target`'s
    ///   queue-thread join. The main thread then runs `del_dev` +
    ///   `clear_ublk_id` on the way out.
    /// - If `None`: there is no live device yet (startup, or recovery
    ///   probe), so the only useful thing the handler can do is exit the
    ///   process so the user is not stuck waiting on a blocking ioctl.
    ///
    /// `kill_dev` uses a thread-local libublk control ring, which the
    /// ctrlc signal thread has not initialized — set it up on first
    /// delivery before issuing the ioctl, otherwise `kill_dev` panics.
    fn install_signal_handler(slot: Arc<Mutex<Option<Arc<UblkCtrl>>>>) {
        // `let _`: another handler already installed (e.g. in tests) is
        // not fatal — the process already has a SIGINT disposition.
        let _ = ctrlc::set_handler(move || {
            if let Err(e) = ublk_init_ctrl_task_ring(|opt| {
                if opt.is_none() {
                    *opt = Some(
                        io_uring::IoUring::<io_uring::squeue::Entry128>::builder()
                            .build(32)
                            .map_err(libublk::UblkError::IOError)?,
                    );
                }
                Ok(())
            }) {
                tracing::error!("ublk signal-thread ctrl ring init failed: {e}");
                std::process::exit(130);
            }
            let guard = slot.lock().expect("sig_ctrl poisoned");
            match guard.as_ref() {
                Some(c) => {
                    if let Err(e) = c.kill_dev() {
                        tracing::error!("ublk kill_dev on signal failed: {e}");
                    }
                }
                None => {
                    // No live device: a blocking ioctl (e.g.
                    // start_user_recover) is in flight and cannot be
                    // interrupted cooperatively. Exit immediately so the
                    // user is never trapped.
                    std::process::exit(130);
                }
            }
        });
    }

    pub fn run_volume_ublk(
        dir: &Path,
        size_bytes: u64,
        fetch_config: Option<elide_fetch::FetchConfig>,
        dev_id: Option<i32>,
    ) -> io::Result<()> {
        let by_id_dir = dir.parent().unwrap_or(dir);
        let mut volume = Volume::open(dir, by_id_dir)?;

        if let Some(config) = fetch_config {
            let fetcher = elide_fetch::RemoteFetcher::new(&config, &volume.fork_dirs())?;
            volume.set_fetcher(Arc::new(fetcher));
            println!("[demand-fetch enabled]");
        }

        let (actor, client) = elide_core::actor::spawn(volume);
        let _actor_thread = std::thread::Builder::new()
            .name("volume-actor".into())
            .spawn(move || actor.run())
            .map_err(io::Error::other)?;

        let connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        crate::control::start(dir, client.clone(), Arc::clone(&connected))?;

        let nr_queues = pick_nr_queues();

        // Install the signal handler as early as possible, before any
        // potentially-blocking ioctl (in particular `start_user_recover`,
        // which retries on EBUSY for up to 30s). The handler consults a
        // shared slot: if a live ctrl is registered, it kill_dev's it so
        // `run_target` can unwind cleanly; if the slot is still empty
        // (startup, recovery probe, etc.) it exits the process directly
        // so the user is never trapped in an uninterruptible phase.
        let sig_ctrl: Arc<Mutex<Option<Arc<UblkCtrl>>>> = Arc::new(Mutex::new(None));
        install_signal_handler(Arc::clone(&sig_ctrl));

        let persisted_id = read_ublk_id(dir)?;
        let (target_id, recovering) = match plan_route(persisted_id, dev_id, ublk_device_exists) {
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
                return Err(io::Error::other(format!(
                    "volume bound to ublk dev {persisted}; refusing to serve with --ublk-id {cli}. \
                     pass --ublk-id {persisted}, or clear the binding after a clean shutdown"
                )));
            }
            Route::ForeignDevice { id } => {
                return Err(io::Error::other(format!(
                    "ublk dev {id} exists but this volume is not bound to it. \
                     run `elide ublk delete {id}` to remove the stale device, or use a different --ublk-id"
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
            println!("[ublk dev {id}: resuming from QUIESCED — reissuing buffered I/O]");
        }

        let dev_lifecycle = if recovering {
            UblkFlags::UBLK_DEV_F_RECOVER_DEV
        } else {
            UblkFlags::UBLK_DEV_F_ADD_DEV
        };
        let ctrl_flags = (libublk::sys::UBLK_F_USER_RECOVERY
            | libublk::sys::UBLK_F_USER_RECOVERY_REISSUE) as u64;

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
                .map_err(|e| io::Error::other(format!("ublk ctrl build: {e}")))?,
        );

        // Publish the live ctrl so the already-installed signal handler
        // can kill_dev it on SIGINT/SIGTERM/SIGHUP. kill_dev triggers
        // STOP_DEV, which unblocks run_target's queue-thread join.
        *sig_ctrl.lock().expect("sig_ctrl poisoned") = Some(Arc::clone(&ctrl));

        let tgt_init = move |dev: &mut UblkDev| {
            set_params(dev, size_bytes);
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
        // LIVE. That is the earliest safe moment to record the binding:
        // the kernel has committed to this id, and the file will only ever
        // be deleted on a clean shutdown below (or by the operator). An
        // unclean daemon exit leaves `ublk.id` in place, which is how the
        // next serve recognises the device is ours to recover.
        let binding_dir: std::path::PathBuf = dir.to_path_buf();
        let wait_hook = move |d_ctrl: &UblkCtrl| {
            d_ctrl.dump();
            let id = d_ctrl.dev_info().dev_id as i32;
            if let Err(e) = write_ublk_id(&binding_dir, id) {
                tracing::error!("ublk record binding for dev {id} failed: {e}");
            }
            println!("[ublk device ready: /dev/ublkb{id}]");
        };

        let run_result = ctrl
            .run_target(tgt_init, q_handler, wait_hook)
            .map_err(|e| io::Error::other(format!("ublk run_target: {e}")));

        // Clear the slot now that run_target has returned: any subsequent
        // signal should exit the process directly rather than kill_dev a
        // ctrl we are about to del_dev below.
        *sig_ctrl.lock().expect("sig_ctrl poisoned") = None;

        // Always attempt DEL_DEV so the kernel-side device does not linger
        // after the daemon exits. run_target's internal stop_dev only stops
        // the device; without del_dev the entry stays in /sys/class/ublk-char
        // and the dev_id cannot be reused. ENOENT is expected if the device
        // was already removed out-of-band.
        if let Err(e) = ctrl.del_dev() {
            tracing::debug!("ublk del_dev on shutdown returned: {e}");
        }

        // Clean shutdown: the device is gone, so the binding is no longer
        // meaningful. Clear the file so a subsequent serve starts fresh.
        // On crash this line is never reached — the file survives, which
        // is exactly what recovery needs.
        if let Err(e) = clear_ublk_id(dir) {
            tracing::error!("ublk clear binding failed: {e}");
        }

        run_result?;
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
        // kill_dev is the documented safe-from-anywhere stop; del_dev then
        // removes the kernel entry and libublk's json file.
        let _ = ctrl.kill_dev();
        ctrl.del_dev()
            .map_err(|e| io::Error::other(format!("del_dev {id}: {e}")))?;
        println!("deleted ublk device {id}");
        Ok(())
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
    /// kernel still has a device registered under that id. The entry
    /// outlives an unclean daemon exit because the kernel transitions to
    /// QUIESCED instead of tearing the device down, which is the signal
    /// for USER_RECOVERY_REISSUE to kick in on respawn.
    fn ublk_device_exists(id: i32) -> bool {
        std::path::Path::new(&format!("/sys/class/ublk-char/ublkc{id}")).exists()
    }

    const UBLK_ID_FILE: &str = "ublk.id";

    /// Read the persisted ublk dev id bound to this volume, if any. The
    /// file is a single line of decimal digits followed by an optional
    /// newline. A missing file means the volume is not currently bound to
    /// any device (fresh serve, or prior clean shutdown).
    fn read_ublk_id(dir: &Path) -> io::Result<Option<i32>> {
        let path = dir.join(UBLK_ID_FILE);
        let raw = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let trimmed = raw.trim();
        trimmed
            .parse::<i32>()
            .map(Some)
            .map_err(|e| io::Error::other(format!("parse {}: {e}", path.display())))
    }

    /// Atomically record the bound dev id. tmp-then-rename so a crashed
    /// write never leaves a half-written file that would fail the next
    /// parse. fsync is not required: the file is runtime state, rebuilt
    /// on every ADD; losing it across a power failure is harmless (next
    /// serve just does a fresh ADD with whatever the caller requests).
    fn write_ublk_id(dir: &Path, id: i32) -> io::Result<()> {
        let tmp = dir.join(format!("{UBLK_ID_FILE}.tmp"));
        let final_path = dir.join(UBLK_ID_FILE);
        std::fs::write(&tmp, format!("{id}\n"))?;
        std::fs::rename(&tmp, &final_path)
    }

    /// Drop the binding file. Absent file is fine — shutdown is idempotent.
    fn clear_ublk_id(dir: &Path) -> io::Result<()> {
        match std::fs::remove_file(dir.join(UBLK_ID_FILE)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
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
        fn ublk_device_exists_false_for_absent_id() {
            // i32::MAX is vastly larger than any real device id the kernel
            // would allocate, so /sys/class/ublk-char/ublkc<MAX> never
            // exists — gives us a robust "missing" case that works on any
            // host, ublk loaded or not.
            assert!(!ublk_device_exists(i32::MAX));
        }

        #[test]
        fn plan_route_add_when_no_prior_state() {
            assert_eq!(
                plan_route(None, None, |_| false),
                Route::Add { target_id: None }
            );
            assert_eq!(
                plan_route(None, Some(3), |_| false),
                Route::Add { target_id: Some(3) }
            );
        }

        #[test]
        fn plan_route_recover_on_bound_and_present() {
            // Persisted == CLI and sysfs has it: canonical recover case
            // after the daemon crashed.
            assert_eq!(
                plan_route(Some(5), Some(5), |id| id == 5),
                Route::Recover { id: 5 }
            );
            // CLI omitted but persisted id is present: recover without
            // needing the operator to re-type the id.
            assert_eq!(
                plan_route(Some(5), None, |id| id == 5),
                Route::Recover { id: 5 }
            );
        }

        #[test]
        fn plan_route_add_rebinds_to_persisted_when_sysfs_gone() {
            // ublk.id says "this volume is dev 5", but the kernel entry
            // is gone (e.g. operator ran `elide ublk delete 5`). ADD with
            // the persisted id so the volume reclaims its slot.
            assert_eq!(
                plan_route(Some(5), None, |_| false),
                Route::Add { target_id: Some(5) }
            );
        }

        #[test]
        fn plan_route_bound_mismatch_refuses() {
            // The whole point of persisting the binding.
            assert_eq!(
                plan_route(Some(5), Some(7), |_| true),
                Route::BoundMismatch {
                    persisted: 5,
                    cli: 7,
                }
            );
            // Mismatch still refuses even if the CLI id has no sysfs
            // entry — catch the operator mistake early, don't let them
            // silently drift off the persisted binding.
            assert_eq!(
                plan_route(Some(5), Some(7), |_| false),
                Route::BoundMismatch {
                    persisted: 5,
                    cli: 7,
                }
            );
        }

        #[test]
        fn plan_route_foreign_device_refuses() {
            // No persisted binding, CLI says id 5, and ublkc5 already
            // exists — probably another volume's QUIESCED device.
            // Reissuing someone else's writes into this volume's WAL
            // would corrupt; refuse.
            assert_eq!(
                plan_route(None, Some(5), |_| true),
                Route::ForeignDevice { id: 5 }
            );
        }

        #[test]
        fn ublk_id_roundtrip() {
            let dir = temp_dir();
            assert_eq!(read_ublk_id(&dir).unwrap(), None);

            write_ublk_id(&dir, 3).unwrap();
            assert_eq!(read_ublk_id(&dir).unwrap(), Some(3));

            // Overwrite atomically — later id wins.
            write_ublk_id(&dir, 7).unwrap();
            assert_eq!(read_ublk_id(&dir).unwrap(), Some(7));

            clear_ublk_id(&dir).unwrap();
            assert_eq!(read_ublk_id(&dir).unwrap(), None);

            // Clearing an already-absent binding is idempotent.
            clear_ublk_id(&dir).unwrap();

            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn ublk_id_rejects_garbage() {
            let dir = temp_dir();
            std::fs::write(dir.join(UBLK_ID_FILE), "not-a-number").unwrap();
            assert!(read_ublk_id(&dir).is_err());
            std::fs::remove_dir_all(dir).unwrap();
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
        _fetch_config: Option<elide_fetch::FetchConfig>,
        _dev_id: Option<i32>,
    ) -> io::Result<()> {
        Err(stub_err())
    }

    pub fn list_devices() -> io::Result<()> {
        Err(stub_err())
    }

    pub fn delete_device(_id: i32) -> io::Result<()> {
        Err(stub_err())
    }

    pub fn delete_all_devices() -> io::Result<()> {
        Err(stub_err())
    }

    fn stub_err() -> io::Error {
        io::Error::other("ublk transport requires Linux and the 'ublk' cargo feature")
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
    fetch_config: Option<elide_fetch::FetchConfig>,
    dev_id: Option<i32>,
) -> io::Result<()> {
    imp::run_volume_ublk(dir, size_bytes, fetch_config, dev_id)
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
