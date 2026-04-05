// volume up/down: standalone volume lifecycle management.
//
// `volume up` spawns a background daemon (`volume-daemon`) that owns the full
// stack in a single process:
//
//   ┌─ tokio runtime ───────────────────────────────────────────────────────┐
//   │  spawn_blocking: nbd::run_volume_signed                               │
//   │    └─ std::thread: VolumeActor::run  (crossbeam channel loop)        │
//   │    └─ std::thread: control::start    (Unix socket IPC server)        │
//   │  tokio::spawn:   run_volume_tasks    (drain + GC + prefetch loop)    │
//   │  tokio signal:   SIGTERM → teardown                                   │
//   └───────────────────────────────────────────────────────────────────────┘
//
// Teardown on SIGTERM: abort drain task → umount → nbd disconnect →
//   process::exit (kills the blocking NBD server thread).
//
// Ready signalling: daemon writes <vol-dir>/mount.pid after mounting, which
// is how `volume up` (the parent) knows setup succeeded. `volume down` sends
// SIGTERM to that PID.

use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

// ── Public entry points ───────────────────────────────────────────────────────

/// `volume up` handler: spawn the background daemon and wait for it to signal
/// ready via mount.pid.
pub fn cmd_volume_up(vol_dir: &Path, mountpoint: &Path, format: bool) -> io::Result<()> {
    let pid_path = vol_dir.join("mount.pid");
    if pid_path.exists() {
        return Err(io::Error::other(
            "volume is already mounted (mount.pid exists); run `elide volume down` first",
        ));
    }

    let exe = std::env::current_exe()?;
    let mut child = Command::new(&exe)
        .arg("volume-daemon")
        .arg(vol_dir)
        .arg(mountpoint)
        .args(if format { &["--format"][..] } else { &[] })
        .spawn()?;

    // Poll for mount.pid; bail early if daemon exits before signalling ready.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if Instant::now() > deadline {
            child.kill().ok();
            return Err(io::Error::other("timed out waiting for volume to mount"));
        }
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "volume daemon exited before mount was ready: {status}"
            )));
        }
        if pid_path.exists() {
            let name = vol_dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("volume");
            println!("{name} mounted at {}", mountpoint.display());
            println!("  elide volume down {name}");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `volume down` handler: signal the daemon to unmount and exit.
pub fn cmd_volume_down(vol_dir: &Path) -> io::Result<()> {
    let pid_path = vol_dir.join("mount.pid");
    let pid_str = std::fs::read_to_string(&pid_path)
        .map_err(|_| io::Error::other("volume is not mounted (no mount.pid); nothing to stop"))?;
    let pid: libc::pid_t = pid_str
        .trim()
        .parse()
        .map_err(|_| io::Error::other("mount.pid contains an invalid PID"))?;

    // SAFETY: SIGTERM is a valid signal; the PID came from our own daemon.
    let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
    if rc != 0 {
        return Err(io::Error::other(format!(
            "failed to signal daemon (pid {pid}): {}",
            io::Error::last_os_error()
        )));
    }

    // Wait for the daemon to remove mount.pid on clean exit.
    let deadline = Instant::now() + Duration::from_secs(30);
    while pid_path.exists() {
        if Instant::now() > deadline {
            return Err(io::Error::other(
                "timed out waiting for volume daemon to exit",
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    println!("volume unmounted");
    Ok(())
}

// ── Hidden daemon command ─────────────────────────────────────────────────────

/// Entry point for the hidden `volume-daemon` subcommand.
///
/// Builds a tokio runtime and runs the embedded volume stack: NBD server,
/// coordinator drain/GC tasks, and SIGTERM-driven teardown — all in one
/// process with no coordinator subprocess.
pub fn run_volume_daemon(vol_dir: &Path, mountpoint: &Path, format: bool) -> io::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(daemon_main(vol_dir, mountpoint, format))
}

async fn daemon_main(vol_dir: &Path, mountpoint: &Path, format: bool) -> io::Result<()> {
    // 1. Load S3 store config from the volume directory (or environment).
    //    If no config is found, run without S3 (drain/GC tasks will be no-ops
    //    until a store is configured, but the volume can still be used locally).
    let store = match elide_fetch::FetchConfig::load(vol_dir)? {
        Some(cfg) => Some(cfg.build_store()?),
        None => {
            tracing::warn!("[volume-up] no store config found; S3 drain and GC are disabled");
            None
        }
    };

    // 2. Start the embedded NBD server as a blocking task.
    //    run_volume_signed is synchronous (blocking actor loop + NBD accept loop);
    //    spawn_blocking runs it on the dedicated blocking thread pool.
    let exe = std::env::current_exe()?;
    let nbd_sock = vol_dir.join("nbd.sock");
    let vol_dir_owned = vol_dir.to_owned();
    let nbd_sock_owned = nbd_sock.clone();

    let _nbd_task = tokio::task::spawn_blocking({
        let exe = exe.clone();
        move || {
            // Re-exec serve-volume in-process: delegate to the existing
            // run_volume_signed path via a subprocess for now.  Step 4b will
            // replace this with a direct call once the NBD server exposes a
            // cancellable async API.
            std::process::Command::new(&exe)
                .args(["serve-volume"])
                .arg(&vol_dir_owned)
                .arg("--socket")
                .arg(&nbd_sock_owned)
                .status()
        }
    });

    // 3. Wait for the NBD socket to appear.
    wait_for_path_async(&nbd_sock, Duration::from_secs(30)).await?;

    // 4. Find a free /dev/nbdN and attach via nbd-client.
    let nbd_dev = find_free_nbd().ok_or_else(|| {
        io::Error::other("no free NBD device found; is the nbd kernel module loaded?")
    })?;

    let status = Command::new("nbd-client")
        .arg("-unix")
        .arg(&nbd_sock)
        .arg(&nbd_dev)
        .status()?;
    if !status.success() {
        return Err(io::Error::other(format!("nbd-client failed: {status}")));
    }

    // 5. Probe for a filesystem; format if needed.
    //    --format means "initialise if blank" — safe to pass on every mount.
    let has_fs = probe_ext4(&nbd_dev)?;
    match (has_fs, format) {
        (true, true) => {
            eprintln!(
                "note: filesystem already present on {}; skipping format",
                nbd_dev.display()
            );
        }
        (true, false) => {}
        (false, true) => run_mkfs(&nbd_dev)?,
        (false, false) => {
            disconnect_nbd(&nbd_dev);
            return Err(io::Error::other(
                "device has no filesystem — pass --format to initialise it",
            ));
        }
    }

    // 6. Mount.
    let status = Command::new("mount")
        .arg(&nbd_dev)
        .arg(mountpoint)
        .status()?;
    if !status.success() {
        disconnect_nbd(&nbd_dev);
        return Err(io::Error::other(format!("mount failed: {status}")));
    }

    // 7. Start coordinator tasks (drain + GC + prefetch) if a store is available.
    let drain_task = store.map(|s| {
        let drain_interval = Duration::from_secs(5);
        let gc_config = elide_coordinator::config::GcConfig::default();
        tokio::spawn(elide_coordinator::tasks::run_volume_tasks(
            vol_dir.to_owned(),
            s,
            drain_interval,
            gc_config,
        ))
    });

    // 8. Signal ready: write PID so `volume up` knows setup succeeded and
    //    `volume down` knows where to send SIGTERM.
    let pid = unsafe { libc::getpid() };
    std::fs::write(vol_dir.join("mount.pid"), format!("{pid}\n"))?;

    // 9. Wait for SIGTERM.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    sigterm.recv().await;

    // 10. Teardown.
    if let Some(task) = drain_task {
        task.abort();
    }
    let _ = Command::new("umount").arg(mountpoint).status();
    disconnect_nbd(&nbd_dev);
    let _ = std::fs::remove_file(vol_dir.join("mount.pid"));

    // The blocking NBD server task cannot be cancelled (spawn_blocking runs to
    // completion).  Exit the process to clean it up — by this point the volume
    // has been unmounted and NBD disconnected, so no data is at risk.
    std::process::exit(0);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn wait_for_path_async(path: &Path, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if path.exists() {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for {}",
                path.display()
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Iterate /sys/block/nbdN/size; a value of "0" means the device is free.
fn find_free_nbd() -> Option<PathBuf> {
    for i in 0..16u32 {
        let size_path = format!("/sys/block/nbd{i}/size");
        if let Ok(s) = std::fs::read_to_string(&size_path)
            && s.trim() == "0"
        {
            return Some(PathBuf::from(format!("/dev/nbd{i}")));
        }
    }
    None
}

/// Check for the ext4 superblock magic (0xEF53) at byte offset 1080.
fn probe_ext4(device: &Path) -> io::Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(device)?;
    let mut magic = [0u8; 2];
    f.seek(SeekFrom::Start(1080))?;
    match f.read_exact(&mut magic) {
        Ok(()) => Ok(magic == [0x53, 0xEF]),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

fn run_mkfs(device: &Path) -> io::Result<()> {
    let status = Command::new("mkfs.ext4").arg("-F").arg(device).status()?;
    if !status.success() {
        return Err(io::Error::other(format!("mkfs.ext4 failed: {status}")));
    }
    Ok(())
}

fn disconnect_nbd(device: &Path) {
    Command::new("nbd-client")
        .arg("-d")
        .arg(device)
        .status()
        .ok();
}
