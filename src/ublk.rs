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
//! `UBLK_F_USER_RECOVERY_REISSUE` and zero-copy (`UBLK_F_AUTO_BUF_REG`) are
//! follow-up steps. See docs/design-ublk-transport.md.
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, Ordering};

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

        let ctrl = Arc::new(
            UblkCtrlBuilder::default()
                .name("elide")
                .id(dev_id.unwrap_or(-1))
                .nr_queues(nr_queues)
                .depth(QUEUE_DEPTH)
                .io_buf_bytes(IO_BUF_BYTES)
                .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
                .build()
                .map_err(|e| io::Error::other(format!("ublk ctrl build: {e}")))?,
        );

        // Break run_target out of its queue-thread join on SIGINT/SIGTERM/SIGHUP.
        // kill_dev() is the libublk-recommended safe path from outside the
        // target callbacks; it triggers STOP_DEV, which causes the queue
        // threads to exit and run_target to return, after which we issue a
        // DEL_DEV so the device does not accumulate across serve restarts.
        //
        // kill_dev uses the *calling thread's* thread-local control ring,
        // which ctrlc's signal thread has not initialized. Set it up on
        // first signal delivery before issuing the ioctl, otherwise
        // kill_dev() panics with "Control ring not initialized".
        //
        // Ignore an error from set_handler: another signal handler being
        // installed already (e.g. in test) should not fail the serve.
        {
            let ctrl_sig = Arc::clone(&ctrl);
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
                    return;
                }
                if let Err(e) = ctrl_sig.kill_dev() {
                    tracing::error!("ublk kill_dev on signal failed: {e}");
                }
            });
        }

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

        let wait_hook = move |d_ctrl: &UblkCtrl| {
            d_ctrl.dump();
            println!(
                "[ublk device ready: /dev/ublkb{}]",
                d_ctrl.dev_info().dev_id
            );
        };

        let run_result = ctrl
            .run_target(tgt_init, q_handler, wait_hook)
            .map_err(|e| io::Error::other(format!("ublk run_target: {e}")));

        // Always attempt DEL_DEV so the kernel-side device does not linger
        // after the daemon exits. run_target's internal stop_dev only stops
        // the device; without del_dev the entry stays in /sys/class/ublk-char
        // and the dev_id cannot be reused. ENOENT is expected if the device
        // was already removed out-of-band.
        if let Err(e) = ctrl.del_dev() {
            tracing::debug!("ublk del_dev on shutdown returned: {e}");
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
            let off = (iod.start_sector << 9) as u64;
            let bytes = (iod.nr_sectors << 9) as u32;

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
