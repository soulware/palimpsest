// volume up/down: standalone volume lifecycle management.
//
// `volume up` spawns a background daemon that owns the full stack:
//   NBD server (elide serve-volume) → block device attachment (nbd-client)
//   → optional mkfs → mount → wait → SIGTERM → teardown
//
// The daemon writes <vol-dir>/mount.pid when the filesystem is ready, which
// is how `volume up` knows setup succeeded. `volume down` sends SIGTERM to
// that PID and waits for the file to be removed.
//
// Device detection: iterates /sys/block/nbdN/size; a value of "0" means free.
// Filesystem detection: reads the ext4 superblock magic at byte offset 1080.

use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_sigterm(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Spawn the volume daemon and wait for it to signal ready via mount.pid.
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

/// Send SIGTERM to the daemon and wait for it to finish teardown.
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
/// This runs as a background process owned by `volume up`. It lives until
/// SIGTERM, then tears down in reverse order: umount → nbd disconnect →
/// stop serve-volume → remove mount.pid.
pub fn run_volume_daemon(vol_dir: &Path, mountpoint: &Path, format: bool) -> io::Result<()> {
    // Register SIGTERM handler before doing any setup so we always clean up.
    // SAFETY: we only set an atomic bool inside the handler.
    unsafe {
        libc::signal(
            libc::SIGTERM,
            handle_sigterm as *const () as libc::sighandler_t,
        );
    }

    // 1. Start the NBD server.
    let exe = std::env::current_exe()?;
    let nbd_sock = vol_dir.join("nbd.sock");
    let mut serve_child = Command::new(&exe)
        .args(["serve-volume"])
        .arg(vol_dir)
        .arg("--socket")
        .arg(&nbd_sock)
        .spawn()?;

    // 2. Wait for the socket to appear.
    if let Err(e) = wait_for_path(&nbd_sock, Duration::from_secs(30)) {
        serve_child.kill().ok();
        return Err(io::Error::other(format!(
            "timed out waiting for NBD socket: {e}"
        )));
    }

    // 3. Find a free /dev/nbdN.
    let nbd_dev = match find_free_nbd() {
        Some(d) => d,
        None => {
            serve_child.kill().ok();
            return Err(io::Error::other(
                "no free NBD device found; is the nbd kernel module loaded?",
            ));
        }
    };

    // 4. Attach via nbd-client.
    let status = Command::new("nbd-client")
        .arg("-unix")
        .arg(&nbd_sock)
        .arg(&nbd_dev)
        .status()?;
    if !status.success() {
        serve_child.kill().ok();
        return Err(io::Error::other(format!("nbd-client failed: {status}")));
    }

    // 5. Probe for a filesystem; format if needed.
    //
    // --format means "initialise if blank" — it is safe to pass on every
    // invocation. It will never reformat a device that already has data.
    let has_fs = probe_ext4(&nbd_dev)?;
    match (has_fs, format) {
        (true, true) => {
            eprintln!(
                "note: filesystem already present on {}; skipping format",
                nbd_dev.display()
            );
        }
        (true, false) => {}
        (false, true) => {
            if let Err(e) = run_mkfs(&nbd_dev) {
                disconnect_nbd(&nbd_dev);
                serve_child.kill().ok();
                return Err(e);
            }
        }
        (false, false) => {
            disconnect_nbd(&nbd_dev);
            serve_child.kill().ok();
            return Err(io::Error::other(
                "device has no filesystem — pass --format to initialise it",
            ));
        }
    }

    // 6. Mount the device.
    let status = Command::new("mount")
        .arg(&nbd_dev)
        .arg(mountpoint)
        .status()?;
    if !status.success() {
        disconnect_nbd(&nbd_dev);
        serve_child.kill().ok();
        return Err(io::Error::other(format!("mount failed: {status}")));
    }

    // 7. Signal ready: write our PID so `volume up` knows setup succeeded
    //    and `volume down` knows where to send SIGTERM.
    let pid = unsafe { libc::getpid() };
    std::fs::write(vol_dir.join("mount.pid"), format!("{pid}\n"))?;

    // 8. Wait for SIGTERM.
    while !SHUTDOWN.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }

    // 9. Teardown (best-effort; log but don't bail on failures).
    let _ = Command::new("umount").arg(mountpoint).status();
    disconnect_nbd(&nbd_dev);
    serve_child.kill().ok();
    serve_child.wait().ok();
    let _ = std::fs::remove_file(vol_dir.join("mount.pid"));

    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn wait_for_path(path: &Path, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        if Instant::now() > deadline {
            return Err(io::Error::other("timeout"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
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
