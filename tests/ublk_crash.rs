//! Kernel-lane crash-injection tests for the ublk transport.
//!
//! These tests drive a real `/dev/ublkbN` device by spawning the elide
//! binary as a subprocess (the test process itself cannot be SIGKILL'd
//! without aborting the test), writing a pattern, signalling the daemon,
//! respawning, and reading the pattern back. They validate the two
//! complementary halves of `plan_route`:
//!
//! - `sigkill_recovery` — daemon dies uncleanly, kernel marks the device
//!   QUIESCED, `ublk.id` and the sysfs entry both survive, respawn takes
//!   `Route::Recover` and USER_RECOVERY_REISSUE reissues buffered I/O.
//!
//! - `sigterm_clean_restart` — daemon handles the signal, runs
//!   `kill_dev` → `del_dev` → `clear_ublk_id`, sysfs entry and binding
//!   file both disappear, respawn takes `Route::Add { target_id: None }`
//!   and acked writes are still durable in the WAL.
//!
//! These run only in the `ci-kernel` lane (real kernel + ublk_drv + root).

#![cfg(all(target_os = "linux", feature = "ublk"))]

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const BLOCK: usize = 4096;
const VOLUME_SIZE: &str = "64M";

/// Device ids chosen well above any realistic kernel auto-allocation so
/// the tests do not collide with anything the host allocated for itself.
/// Distinct ids per test let them run in parallel.
const DEV_ID_SIGKILL: i32 = 1001;
const DEV_ID_SIGTERM: i32 = 1002;

/// Runtime-overridable path to the elide binary. `env!("CARGO_BIN_EXE_elide")`
/// is a compile-time host path; when this binary is copied into the CI
/// guest over 9p the host path is not valid, so CI sets `ELIDE_BIN` to
/// the guest-side path.
fn elide_bin() -> PathBuf {
    if let Ok(p) = std::env::var("ELIDE_BIN") {
        return PathBuf::from(p);
    }
    PathBuf::from(env!("CARGO_BIN_EXE_elide"))
}

fn kernel_ready() -> bool {
    Path::new("/dev/ublk-control").exists()
}

fn sysfs_entry_exists(dev_id: i32) -> bool {
    Path::new(&format!("/sys/class/ublk-char/ublkc{dev_id}")).exists()
}

fn bdev_path(dev_id: i32) -> PathBuf {
    PathBuf::from(format!("/dev/ublkb{dev_id}"))
}

/// Create the minimum on-disk shape `Volume::open` expects: volume
/// keypair, provenance, and the three runtime sub-directories. The
/// `volume.toml` size is written on the first `serve-volume` invocation
/// via `resolve_volume_size`.
fn bootstrap_volume(dir: &Path) {
    std::fs::create_dir_all(dir.join("pending")).expect("mkdir pending");
    std::fs::create_dir_all(dir.join("index")).expect("mkdir index");
    std::fs::create_dir_all(dir.join("cache")).expect("mkdir cache");
    let key = elide_core::signing::generate_keypair(
        dir,
        elide_core::signing::VOLUME_KEY_FILE,
        elide_core::signing::VOLUME_PUB_FILE,
    )
    .expect("generate_keypair");
    elide_core::signing::write_provenance(
        dir,
        &key,
        elide_core::signing::VOLUME_PROVENANCE_FILE,
        &elide_core::signing::ProvenanceLineage::default(),
    )
    .expect("write_provenance");
}

/// Unique scratch dir per test run, created under `$TMPDIR` (or `/tmp`).
fn scratch_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    let p = base.join(format!("elide-ublk-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Spawn the daemon in `serve-volume` mode bound to `dev_id`. The
/// caller owns the `Child` and is responsible for signalling / reaping.
fn spawn_daemon(dir: &Path, dev_id: i32) -> Child {
    Command::new(elide_bin())
        .arg("serve-volume")
        .arg(dir)
        .arg("--size")
        .arg(VOLUME_SIZE)
        .arg("--ublk")
        .arg("--ublk-id")
        .arg(dev_id.to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn elide serve-volume")
}

/// Block until `/dev/ublkb<dev_id>` and its sysfs entry are both
/// present, or panic after 20s. Small settle after appearance gives the
/// kernel time to publish queue limits before we open the device.
fn wait_for_device(dev_id: i32) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let bdev = bdev_path(dev_id);
    while Instant::now() < deadline {
        if bdev.exists() && sysfs_entry_exists(dev_id) {
            thread::sleep(Duration::from_millis(200));
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("ublk device /dev/ublkb{dev_id} did not appear within 20s");
}

/// Block until the sysfs entry for `dev_id` disappears, or panic after
/// 10s. Used after SIGTERM to confirm the clean-shutdown path actually
/// ran `del_dev` (not just `kill_dev`).
fn wait_for_sysfs_gone(dev_id: i32) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if !sysfs_entry_exists(dev_id) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("/sys/class/ublk-char/ublkc{dev_id} still present 10s after signal");
}

/// Reap the child after signalling it, bounded by a deadline. If the
/// child does not exit within the deadline, panic — a hung daemon is a
/// regression, not a slow exit. SIGKILL the leaked process so the next
/// test run is not blocked on its lingering ublk device.
fn reap_within(child: &mut Child, expected: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                eprintln!("[test] daemon ({expected}) exited: {status:?}");
                return;
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let pid = child.id();
                    let _ = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
                    let _ = child.wait();
                    panic!(
                        "daemon (pid={pid}) did not exit within {timeout:?} after {expected}; \
                         SIGKILL'd to free kernel state — likely a shutdown deadlock"
                    );
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("try_wait on daemon after {expected}: {e}"),
        }
    }
}

/// Crash path: SIGKILL guarantees prompt exit. A short bound is fine.
fn reap_killed(child: &mut Child, expected: &str) {
    reap_within(child, expected, Duration::from_secs(2));
}

/// Clean shutdown path: more generous, but still bounded — anything
/// over a few seconds is the deadlock we are trying to detect.
fn reap_clean(child: &mut Child, expected: &str) {
    reap_within(child, expected, Duration::from_secs(5));
}

/// Send `sig` to `child` via raw `kill(2)`.
fn send_signal(child: &Child, sig: libc::c_int) {
    let pid = child.id() as libc::pid_t;
    let r = unsafe { libc::kill(pid, sig) };
    if r != 0 {
        panic!("kill({pid}, {sig}): {}", std::io::Error::last_os_error());
    }
}

/// Best-effort: if the daemon crashed without cleaning up the sysfs
/// entry (or the test itself panicked mid-cycle), delete the device so
/// subsequent test runs on the same machine are not wedged on a stale
/// QUIESCED entry. Ignores failure — cleanup is advisory.
fn force_delete_device(dev_id: i32) {
    let _ = Command::new(elide_bin())
        .args(["ublk", "delete"])
        .arg(dev_id.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

// ── Aligned-buffer helpers for O_DIRECT ───────────────────────────────

/// O_DIRECT requires buffer, length, and offset all aligned to the
/// device's logical block size. Our ublk device is 4K-logical, so we
/// use a 4K-aligned heap allocation.
struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
}

impl AlignedBuf {
    fn new(len: usize) -> Self {
        assert_eq!(len % BLOCK, 0, "AlignedBuf len must be a multiple of BLOCK");
        let layout = Layout::from_size_align(len, BLOCK).expect("valid layout");
        let ptr = unsafe { alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "alloc_zeroed failed");
        Self { ptr, len }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    fn fill_from(&mut self, src: &[u8]) {
        assert_eq!(src.len(), self.len);
        self.as_mut_slice().copy_from_slice(src);
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.len, BLOCK).expect("valid layout");
        unsafe { dealloc(self.ptr, layout) };
    }
}

fn open_direct(path: &Path, flags: libc::c_int) -> i32 {
    let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("cstring");
    let fd = unsafe { libc::open(c.as_ptr(), flags | libc::O_DIRECT, 0) };
    if fd < 0 {
        panic!(
            "open {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    fd
}

fn pwrite_all(fd: i32, buf: &[u8], mut offset: u64) {
    let mut written = 0usize;
    while written < buf.len() {
        let n = unsafe {
            libc::pwrite(
                fd,
                buf[written..].as_ptr() as *const libc::c_void,
                buf.len() - written,
                offset as libc::off_t,
            )
        };
        if n < 0 {
            panic!("pwrite: {}", std::io::Error::last_os_error());
        }
        if n == 0 {
            panic!(
                "pwrite returned 0 with {} bytes remaining",
                buf.len() - written
            );
        }
        written += n as usize;
        offset += n as u64;
    }
}

fn pread_all(fd: i32, buf: &mut [u8], mut offset: u64) {
    let mut read = 0usize;
    while read < buf.len() {
        let n = unsafe {
            libc::pread(
                fd,
                buf[read..].as_mut_ptr() as *mut libc::c_void,
                buf.len() - read,
                offset as libc::off_t,
            )
        };
        if n < 0 {
            panic!("pread: {}", std::io::Error::last_os_error());
        }
        if n == 0 {
            panic!(
                "pread returned 0 (EOF) with {} bytes remaining",
                buf.len() - read
            );
        }
        read += n as usize;
        offset += n as u64;
    }
}

fn write_pattern(dev_id: i32, offset: u64, data: &[u8]) {
    let fd = open_direct(&bdev_path(dev_id), libc::O_WRONLY | libc::O_SYNC);
    let mut aligned = AlignedBuf::new(data.len());
    aligned.fill_from(data);
    pwrite_all(fd, aligned.as_slice(), offset);
    // O_SYNC on the fd makes pwrite durable, but fsync the bdev for
    // good measure — the guarantee we want is "ack implies WAL commit".
    if unsafe { libc::fsync(fd) } != 0 {
        panic!("fsync: {}", std::io::Error::last_os_error());
    }
    unsafe { libc::close(fd) };
}

fn read_pattern(dev_id: i32, offset: u64, len: usize) -> Vec<u8> {
    let fd = open_direct(&bdev_path(dev_id), libc::O_RDONLY);
    let mut aligned = AlignedBuf::new(len);
    pread_all(fd, aligned.as_mut_slice(), offset);
    unsafe { libc::close(fd) };
    aligned.as_slice().to_vec()
}

// ── Test pattern ──────────────────────────────────────────────────────

/// Distinguishable 4K pattern: byte i = (i ^ seed) & 0xff. Makes
/// mismatches easy to spot when they happen.
fn pattern(seed: u8) -> Vec<u8> {
    (0..BLOCK).map(|i| (i as u8) ^ seed).collect()
}

// ── Tests ─────────────────────────────────────────────────────────────

#[test]
fn sigkill_recovery() {
    if !kernel_ready() {
        eprintln!("skip: /dev/ublk-control not present");
        return;
    }

    let dir = scratch_dir("sigkill");
    bootstrap_volume(&dir);
    force_delete_device(DEV_ID_SIGKILL);

    // Round 1: spawn, write pattern at block 0, wait for durability.
    let mut daemon = spawn_daemon(&dir, DEV_ID_SIGKILL);
    wait_for_device(DEV_ID_SIGKILL);

    let data = pattern(0xa5);
    write_pattern(DEV_ID_SIGKILL, 0, &data);

    // Kill the daemon uncleanly. Kernel sees the uring_cmd fds close,
    // moves the device to QUIESCED, and keeps /dev/ublkbN alive pending
    // a recovery attach. `ublk.id` on disk must survive.
    send_signal(&daemon, libc::SIGKILL);
    reap_killed(&mut daemon, "SIGKILL");

    assert!(
        sysfs_entry_exists(DEV_ID_SIGKILL),
        "sysfs entry should survive SIGKILL (device should be QUIESCED, not DELETED)"
    );
    assert!(
        dir.join("ublk.id").exists(),
        "volume <-> device binding file should survive SIGKILL"
    );

    // Round 2: respawn same volume, same id. plan_route sees
    // persisted=Some(id), sysfs=true => Route::Recover. The kernel
    // reissues any buffered I/O that was in flight at the crash.
    let mut daemon = spawn_daemon(&dir, DEV_ID_SIGKILL);
    wait_for_device(DEV_ID_SIGKILL);

    let read_back = read_pattern(DEV_ID_SIGKILL, 0, BLOCK);
    assert_eq!(
        read_back, data,
        "pattern at LBA 0 must survive SIGKILL + USER_RECOVERY_REISSUE"
    );

    // Clean shutdown of the recovered daemon.
    send_signal(&daemon, libc::SIGTERM);
    reap_clean(&mut daemon, "SIGTERM (cleanup)");
    wait_for_sysfs_gone(DEV_ID_SIGKILL);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sigterm_clean_restart() {
    if !kernel_ready() {
        eprintln!("skip: /dev/ublk-control not present");
        return;
    }

    let dir = scratch_dir("sigterm");
    bootstrap_volume(&dir);
    force_delete_device(DEV_ID_SIGTERM);

    // Round 1: spawn, write pattern, signal SIGTERM for a clean exit.
    let mut daemon = spawn_daemon(&dir, DEV_ID_SIGTERM);
    wait_for_device(DEV_ID_SIGTERM);

    let data = pattern(0x5a);
    write_pattern(DEV_ID_SIGTERM, 0, &data);

    send_signal(&daemon, libc::SIGTERM);
    reap_clean(&mut daemon, "SIGTERM");

    // Clean exit must tear down the kernel device and the on-disk
    // binding. If either survives, the daemon didn't reach the
    // post-`run_target` `del_dev` + `clear_ublk_id` path.
    wait_for_sysfs_gone(DEV_ID_SIGTERM);
    assert!(
        !dir.join("ublk.id").exists(),
        "ublk.id must be cleared on clean shutdown"
    );

    // Round 2: respawn — plan_route now sees persisted=None,
    // cli=Some(id), sysfs=false, so takes Route::Add { target_id:
    // Some(id) } (a fresh ADD, not recovery). Data must still read
    // back, because the WAL is the source of truth, not any in-memory
    // kernel state.
    let mut daemon = spawn_daemon(&dir, DEV_ID_SIGTERM);
    wait_for_device(DEV_ID_SIGTERM);

    let read_back = read_pattern(DEV_ID_SIGTERM, 0, BLOCK);
    assert_eq!(
        read_back, data,
        "acked writes must survive clean shutdown + fresh add"
    );

    send_signal(&daemon, libc::SIGTERM);
    reap_clean(&mut daemon, "SIGTERM (cleanup)");
    wait_for_sysfs_gone(DEV_ID_SIGTERM);

    let _ = std::fs::remove_dir_all(&dir);
}
