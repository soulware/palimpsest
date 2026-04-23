//! ublk transport (Linux userspace block device).
//!
//! Step-1 spike: single queue, depth 1, no zero-copy, no user-recovery. The
//! handler is synchronous — one in-flight op per queue, so a blocking
//! `VolumeClient` call only stalls its own queue. Multi-queue, higher depth,
//! `spawn_blocking` offload, and `UBLK_F_USER_RECOVERY_REISSUE` are follow-up
//! steps. See docs/design-ublk-transport.md.
//!
//! On non-Linux targets, and on Linux without the `ublk` cargo feature, this
//! module compiles to a stub that errors when the transport is invoked.

use std::io;
use std::path::Path;

#[cfg(all(target_os = "linux", feature = "ublk"))]
mod imp {
    use std::io;
    use std::path::Path;
    use std::rc::Rc;
    use std::sync::Arc;

    use libublk::BufDesc;
    use libublk::UblkFlags;
    use libublk::UblkIORes;
    use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
    use libublk::io::{BufDescList, UblkDev, UblkIOCtx, UblkQueue};

    use elide_core::actor::{VolumeClient, VolumeReader};
    use elide_core::volume::Volume;

    const BLOCK: u64 = 4096;
    const LOGICAL_BS_SHIFT: u8 = 12;
    const PHYSICAL_BS_SHIFT: u8 = 12;
    const IO_MIN_SHIFT: u8 = 12;
    const IO_OPT_SHIFT: u8 = 12;

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

        let ctrl = UblkCtrlBuilder::default()
            .name("elide")
            .id(dev_id.unwrap_or(-1))
            .nr_queues(1)
            .depth(1)
            .io_buf_bytes(1 << 20)
            .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
            .build()
            .map_err(|e| io::Error::other(format!("ublk ctrl build: {e}")))?;

        let tgt_init = move |dev: &mut UblkDev| {
            set_params(dev, size_bytes);
            Ok(())
        };

        // VolumeClient is Send + Sync + Clone, so it satisfies run_target's
        // queue-handler bound directly. Each queue thread constructs its own
        // VolumeReader (Send, !Sync) to hold a per-thread file-descriptor
        // cache for reads.
        let q_handler = move |qid, dev: &UblkDev| {
            q_fn(qid, dev, client.reader());
        };

        let wait_hook = move |d_ctrl: &UblkCtrl| {
            d_ctrl.dump();
            println!(
                "[ublk device ready: /dev/ublkb{}]",
                d_ctrl.dev_info().dev_id
            );
        };

        ctrl.run_target(tgt_init, q_handler, wait_hook)
            .map_err(|e| io::Error::other(format!("ublk run_target: {e}")))?;

        Ok(())
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

    fn q_fn(qid: u16, dev: &UblkDev, reader: VolumeReader) {
        let bufs_rc = Rc::new(dev.alloc_queue_io_bufs());
        let bufs = bufs_rc.clone();

        let io_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
            let iod = q.get_iod(tag);
            let op = iod.op_flags & 0xff;
            let off = (iod.start_sector << 9) as u64;
            let bytes = (iod.nr_sectors << 9) as u32;

            let iob = &bufs[tag as usize];
            let reg_slice = iob.as_slice();
            // SAFETY: each tag owns a unique IoBuf allocation. The sync
            // handler is called serially per tag with no concurrent access,
            // and the kernel has already copied WRITE payload into the buffer
            // by the time the handler runs. libublk's own loop.rs example
            // does the same `as_ptr() as *mut u8` cast.
            let slice: &mut [u8] = unsafe {
                std::slice::from_raw_parts_mut(reg_slice.as_ptr() as *mut u8, reg_slice.len())
            };

            let res = if (bytes as usize) <= slice.len() {
                dispatch(&reader, op, off, bytes, &mut slice[..bytes as usize])
            } else {
                -libc::EINVAL
            };

            if let Err(e) = q.complete_io_cmd_unified(
                tag,
                BufDesc::Slice(reg_slice),
                Ok(UblkIORes::Result(res)),
            ) {
                tracing::error!("ublk complete_io_cmd_unified failed: {e}");
            }
        };

        let queue = match UblkQueue::new(qid, dev)
            .and_then(|q| q.submit_fetch_commands_unified(BufDescList::Slices(Some(&bufs_rc))))
        {
            Ok(q) => q,
            Err(e) => {
                tracing::error!("ublk queue setup failed: {e}");
                return;
            }
        };

        queue.wait_and_handle_io(io_handler);
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
        Err(io::Error::other(
            "ublk transport requires Linux and the 'ublk' cargo feature",
        ))
    }
}

/// Serve a volume over ublk. Creates `/dev/ublkbN` and runs the I/O loop.
///
/// Step-1 spike: single queue, depth 1. `dev_id = None` lets the kernel
/// auto-allocate. See docs/design-ublk-transport.md.
pub fn run_volume_ublk(
    dir: &Path,
    size_bytes: u64,
    fetch_config: Option<elide_fetch::FetchConfig>,
    dev_id: Option<i32>,
) -> io::Result<()> {
    imp::run_volume_ublk(dir, size_bytes, fetch_config, dev_id)
}
