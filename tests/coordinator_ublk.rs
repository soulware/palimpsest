//! Coordinator-lane integration test: end-to-end coverage of the
//! coordinator driving a supervised volume over ublk, including a
//! coordinator restart that exercises Route::Recover.
//!
//! Mirrors the structure of `tests/coordinator_nbd.rs` but exercises the
//! kernel-backed transport, and goes further: after the first coordinator
//! exits cleanly (shutdown-park leaves the kernel device QUIESCED), a
//! fresh coordinator is spawned against the same data_dir. Its
//! supervisor re-adopts the existing fork; the daemon takes
//! Route::Recover and re-attaches to the still-live `/dev/ublkb<id>`.
//! Data written before the first shutdown reads back unchanged, and new
//! writes through the recovered device round-trip correctly.
//!
//! Covered operations (in order):
//!
//!   Phase 1 — fresh start under coordinator A
//!     1. Coordinator boot — `elide-coordinator serve --config <toml>`
//!        with a file-backed store (no S3) and a short scan interval.
//!     2. `elide volume create ... --ublk` + inbound `rescan`. The
//!        kernel auto-allocates a dev id; the test discovers it from
//!        `[ublk] dev_id` in `volume.toml` after the daemon's wait_hook
//!        writes it back.
//!     3. Supervisor adopts the fork: `volume.pid`, `control.sock`, and
//!        `/dev/ublkb<id>` (plus its sysfs entry and the `[ublk]
//!        dev_id` field in `volume.toml`) all appear.
//!     4. WRITE/READ over `/dev/ublkb<id>` via O_DIRECT pwrite/pread.
//!        Pattern round-trip validates the daemon is actually serving
//!        real I/O through the kernel device, not just owning the sysfs
//!        entry.
//!     5. `elide volume snapshot` — exercises the coordinator's inbound
//!        snapshot handler and the volume-side snapshot_manifest IPC,
//!        mid-service while the ublk device is live.
//!     6. SIGINT coord A → coordinator runs its defensive shutdown
//!        (PR #254): aborts its tasks, drains, exits — leaving the
//!        supervised volume running. The test then SIGTERMs the
//!        orphaned volume directly. Under shutdown-park
//!        (docs/design-ublk-shutdown-park.md) the daemon flushes and
//!        exits without calling STOP_DEV; the kernel parks the device
//!        in QUIESCED on daemon-exit detection. Post-shutdown
//!        invariants: the sysfs entry and the `[ublk] dev_id` field
//!        in `volume.toml` survive so the next serve takes
//!        Route::Recover. `volume.pid` is left as a
//!        stale file (the supervisor task was aborted before its
//!        remove_pid hook could run); the next coord's adoption
//!        cleans it up.
//!
//!   Phase 2 — restart + recovery under coordinator B
//!     7. Coordinator B boot — same `data_dir`, same store. The
//!        supervisor scans, finds the existing fork with no live
//!        `volume.pid`, and spawns `serve-volume`.
//!     8. Daemon takes Route::Recover (persisted=Some(id), sysfs=true).
//!        `volume.pid` reappears; the sysfs entry never went away.
//!     9. Read LBA 0 — pattern from phase 1 reads back. This is the
//!        durability proof: an acked write before coordinator A's
//!        SIGINT survives both the daemon SIGTERM (shutdown flush) and
//!        the kernel's recovery reattach under coordinator B.
//!    10. Write a second pattern at a non-zero offset, read back. The
//!        recovered device serves new I/O end-to-end.
//!    11. Re-read LBA 0 — original pattern still intact (no clobber by
//!        the new write).
//!    12. SIGINT coord B → defensive shutdown again, followed by
//!        SIGTERM to the volume to exercise the same shutdown-park
//!        path. Cleanup explicitly deletes the device.
//!
//! Distinct from `tests/ublk_crash.rs`: that test directly signals the
//! daemon to exercise crash/recovery; this test exercises the
//! coordinator-supervised orchestration path (CLI → coord IPC →
//! supervisor → daemon) and the cross-coordinator-process recovery
//! path that no other test covers today.
//!
//! Runs in the `ci-kernel` lane. Needs Linux + ublk_drv + root.

#![cfg(all(target_os = "linux", feature = "ublk"))]

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const SIGINT: i32 = 2;
const SIGTERM: i32 = 15;
const SIGKILL: i32 = 9;
const BLOCK: usize = 4096;
const VOLUME_SIZE: &str = "64M";

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

/// Poll `volume.toml` until `[ublk] dev_id` is populated and return it.
/// Used after `volume create --ublk` (no pin) to discover the kernel-
/// assigned id once the daemon's `wait_hook` has written it back.
fn wait_for_bound_id(fork_dir: &Path) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(Some(id)) = elide_core::config::VolumeConfig::bound_ublk_id(fork_dir) {
            return id;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "timed out waiting for [ublk] dev_id in {}",
        fork_dir.join("volume.toml").display()
    );
}

// ── Binary locations ────────────────────────────────────────────────────────

fn elide_bin() -> PathBuf {
    if let Ok(p) = std::env::var("ELIDE_BIN") {
        return PathBuf::from(p);
    }
    PathBuf::from(env!("CARGO_BIN_EXE_elide"))
}

fn coordinator_bin() -> PathBuf {
    if let Ok(p) = std::env::var("ELIDE_COORDINATOR_BIN") {
        return PathBuf::from(p);
    }
    let mut p = elide_bin();
    p.set_file_name("elide-coordinator");
    p
}

// ── Kernel-state probes ─────────────────────────────────────────────────────

fn kernel_ready() -> bool {
    Path::new("/dev/ublk-control").exists()
}

fn sysfs_entry_exists(dev_id: i32) -> bool {
    Path::new(&format!("/sys/class/ublk-char/ublkc{dev_id}")).exists()
}

fn bdev_path(dev_id: i32) -> PathBuf {
    PathBuf::from(format!("/dev/ublkb{dev_id}"))
}

/// Best-effort cleanup: remove any QUIESCED device left behind by a
/// previous run before we spawn the coordinator. Ignores failure.
fn force_delete_device(dev_id: i32) {
    let _ = Command::new(elide_bin())
        .args(["ublk", "delete"])
        .arg(dev_id.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn delete_and_wait_sysfs_gone(dev_id: i32) {
    force_delete_device(dev_id);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if !sysfs_entry_exists(dev_id) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("/sys/class/ublk-char/ublkc{dev_id} still present 10s after `elide ublk delete`");
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn wait_until(deadline: Duration, label: &str, mut check: impl FnMut() -> bool) {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if check() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out after {deadline:?} waiting for: {label}");
}

fn spawn_coordinator(config_path: &Path) -> Child {
    Command::new(coordinator_bin())
        .arg("serve")
        .arg("--config")
        .arg(config_path)
        .env("RUST_LOG", "info")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn elide-coordinator serve")
}

fn elide(data_dir: &Path, args: &[&str]) {
    let status = Command::new(elide_bin())
        .arg("--data-dir")
        .arg(data_dir)
        .args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .unwrap_or_else(|e| panic!("spawn `elide {}`: {e}", args.join(" ")));
    assert!(
        status.success(),
        "`elide {}` failed: {status:?}",
        args.join(" ")
    );
}

fn wait_with_timeout(
    child: &mut Child,
    timeout: Duration,
    label: &str,
) -> std::process::ExitStatus {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status,
            Ok(None) => {
                if start.elapsed() > timeout {
                    unsafe {
                        kill(child.id() as i32, SIGKILL);
                    }
                    let status = child.wait().expect("wait after SIGKILL");
                    panic!(
                        "{label} did not exit within {timeout:?}; killed; final status: {status:?}"
                    );
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("try_wait({label}): {e}"),
        }
    }
}

// ── Aligned-buffer helpers for O_DIRECT ─────────────────────────────────────
//
// Mirrors the helpers in `tests/ublk_crash.rs`. O_DIRECT requires
// buffer, length, and offset all aligned to the device's logical block
// size; our ublk device is 4K-logical.

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

// ── The test ────────────────────────────────────────────────────────────────

#[test]
fn coordinator_ublk_lifecycle() {
    if !kernel_ready() {
        eprintln!("skip: /dev/ublk-control not present");
        return;
    }

    // No pre-cleanup needed: the kernel auto-allocates a fresh dev id
    // per ADD, and each test run uses an isolated tempdir / data_dir
    // (so no `[ublk] dev_id` is carried over). Devices leaked by a
    // previous failed run on this host stay on whichever id the kernel
    // had assigned them; the new ADD picks an unused id.

    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let store_dir = tmp.path().join("store");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&store_dir).unwrap();

    let config_path = tmp.path().join("coordinator.toml");
    let config_toml = format!(
        "data_dir = {data:?}\n\
         elide_bin = {elide:?}\n\
         \n\
         [store]\n\
         local_path = {store:?}\n\
         \n\
         [supervisor]\n\
         drain_interval = \"1s\"\n\
         scan_interval = \"2s\"\n\
         \n\
         [gc]\n\
         interval = \"5s\"\n",
        data = data_dir,
        elide = elide_bin(),
        store = store_dir,
    );
    std::fs::write(&config_path, config_toml).expect("write coordinator.toml");

    // ── Phase 1: fresh start under coordinator A ────────────────────────────

    let mut coord_a = spawn_coordinator(&config_path);

    let inbound = data_dir.join("control.sock");
    wait_until(
        Duration::from_secs(15),
        "[A] coordinator inbound socket",
        || inbound.exists(),
    );

    // Create a fresh 64 MiB volume backed by ublk. The kernel auto-
    // allocates the dev id; we discover it after the daemon has ADDed
    // by reading `[ublk] dev_id` back from volume.toml.
    elide(
        &data_dir,
        &["volume", "create", "vtest", "--size", VOLUME_SIZE, "--ublk"],
    );

    let name_link = data_dir.join("by_name").join("vtest");
    wait_until(Duration::from_secs(10), "[A] by_name/vtest symlink", || {
        name_link.exists()
    });
    let fork_dir = std::fs::canonicalize(&name_link).expect("canonicalize by_name/vtest");

    // Wait for the supervisor to spawn the volume, the volume to bind
    // its control socket, and the kernel device + sysfs entry to
    // appear. The dev id is whatever the kernel assigned — discover
    // via `wait_for_bound_id` then resolve sysfs / bdev paths against
    // it. The bound id is sticky for the rest of this test (and would
    // be sticky across the coordinator restart in phase 2 too).
    let volume_pid = fork_dir.join("volume.pid");
    let volume_ctrl = fork_dir.join("control.sock");
    let dev_id = wait_for_bound_id(&fork_dir);
    let bdev = bdev_path(dev_id);
    wait_until(
        Duration::from_secs(30),
        "[A] supervised ublk volume up",
        || {
            volume_pid.exists()
                && volume_ctrl.exists()
                && bdev.exists()
                && sysfs_entry_exists(dev_id)
        },
    );
    // Small settle so the kernel has published queue limits before our
    // first open(2). Mirrors the same pattern in tests/ublk_crash.rs.
    thread::sleep(Duration::from_millis(200));

    // Capture the volume PID before shutdown so phase 2's PID file
    // appearance can be distinguished from a stale carryover.
    let pid_a = std::fs::read_to_string(&volume_pid)
        .expect("[A] read volume.pid")
        .trim()
        .parse::<i32>()
        .expect("[A] parse volume.pid");

    // Block-device round-trip: write a 4 KiB pattern at LBA 0, read back,
    // verify. fsync is implicit via O_SYNC + explicit fsync in
    // write_pattern. This pattern is the durability oracle for phase 2.
    let pattern_a: Vec<u8> = (0..BLOCK).map(|i| (i as u8) ^ 0x3c).collect();
    write_pattern(dev_id, 0, &pattern_a);
    let got = read_pattern(dev_id, 0, BLOCK);
    assert_eq!(got, pattern_a, "[A] ublk read-back pattern mismatch");

    // Coordinator-orchestrated snapshot, mid-service. Exercises:
    //   CLI → coord inbound → per-volume lock → volume IPC (flush,
    //   gc_checkpoint, snapshot_manifest) → store upload, while the
    //   ublk device is live and the daemon is serving I/O.
    elide(&data_dir, &["volume", "snapshot", "vtest"]);

    let snaps_dir = fork_dir.join("snapshots");
    wait_until(
        Duration::from_secs(5),
        "[A] snapshot manifest present",
        || {
            snaps_dir
                .read_dir()
                .map(|mut it| it.next().is_some())
                .unwrap_or(false)
        },
    );

    // SIGINT the coordinator. Under the defensive signal policy
    // (PR #254) coord runs the rolling-upgrade teardown: aborts its
    // tasks, drains, exits — the supervised volume is *not* signalled
    // and stays alive across the coord exit. To exercise the
    // shutdown-park path that phase 2's Route::Recover depends on,
    // we then SIGTERM the volume directly (mirroring what `elide
    // coord stop`'s fallback does).
    unsafe {
        let rc = kill(coord_a.id() as i32, SIGINT);
        assert_eq!(rc, 0, "[A] kill(SIGINT) to coordinator failed");
    }
    let status_a = wait_with_timeout(&mut coord_a, Duration::from_secs(20), "[A] coordinator");
    assert!(
        status_a.success(),
        "[A] coordinator exited non-zero: {status_a:?}"
    );

    // Defensive-policy invariant: the volume process is still alive
    // and `volume.pid` still points at it. Coord's shutdown does not
    // signal volume children. Failure here would mean a regression
    // back to the pre-PR-254 "coord SIGTERMs supervised processes"
    // behaviour.
    assert!(
        pid_alive(pid_a),
        "[A] volume process {pid_a} died under coord SIGINT — defensive policy regressed"
    );
    assert!(
        volume_pid.exists(),
        "[A] volume.pid removed under defensive shutdown — coord should not signal children"
    );

    // Now exercise the volume's own shutdown-park path: SIGTERM the
    // volume directly. The daemon flushes and exits without STOP_DEV;
    // the kernel parks the device in QUIESCED on daemon-exit
    // detection.
    unsafe {
        let rc = kill(pid_a, SIGTERM);
        assert_eq!(rc, 0, "[A] kill(SIGTERM) to volume failed");
    }
    wait_until(Duration::from_secs(20), "[A] volume process exit", || {
        !pid_alive(pid_a)
    });

    // Post-shutdown invariants:
    //
    //   1. The sysfs entry and the `[ublk] dev_id` field in volume.toml
    //      BOTH survive — shutdown-park leaves the device parked in
    //      QUIESCED and the binding hint intact, so the next serve takes
    //      Route::Recover.
    //   2. `volume.pid` is *not* checked here: under the defensive
    //      shutdown the supervisor task was aborted before any
    //      remove_pid hook could run, so the file is left as a stale
    //      reference to the now-dead pid. The next coord's supervisor
    //      removes it as part of adoption.
    //   3. The control socket is unlinked only by the volume's own
    //      accept-loop cleanup, which does not run under SIGTERM
    //      termination.
    assert!(
        sysfs_entry_exists(dev_id),
        "[A] sysfs entry should survive volume SIGTERM (shutdown-park)"
    );
    assert_eq!(
        elide_core::config::VolumeConfig::bound_ublk_id(&fork_dir)
            .ok()
            .flatten(),
        Some(dev_id),
        "[A] bound dev_id must persist for Route::Recover on next serve"
    );
    let _ = volume_ctrl;

    // ── Phase 2: restart + Route::Recover under coordinator B ───────────────

    let mut coord_b = spawn_coordinator(&config_path);

    wait_until(
        Duration::from_secs(15),
        "[B] coordinator inbound socket",
        || inbound.exists(),
    );

    // Wait for coordinator B's supervisor to scan the data_dir, find
    // the stale `volume.pid` left over from phase 1's defensive
    // shutdown, observe its pid as dead, remove it, and spawn a fresh
    // `serve-volume`. The daemon takes Route::Recover and re-attaches
    // to the still-live /dev/ublkb<id>. We require:
    //   - volume.pid contains a fresh, currently-alive PID different
    //     from phase 1's
    //   - sysfs entry was never deleted (Route::Recover, not Route::Add)
    //   - bdev is still the same /dev/ublkb<id>
    wait_until(
        Duration::from_secs(30),
        "[B] supervised ublk volume re-adopted",
        || {
            if !(volume_pid.exists() && volume_ctrl.exists() && bdev.exists()) {
                return false;
            }
            if !sysfs_entry_exists(dev_id) {
                return false;
            }
            // Distinguish "fresh PID file" from "stale carryover".
            // The supervisor removed the stale file and rewrote it
            // with the new spawn's PID; we additionally check the new
            // PID is different from phase 1's and is alive.
            std::fs::read_to_string(&volume_pid)
                .ok()
                .and_then(|s| s.trim().parse::<i32>().ok())
                .map(|pid_b| pid_b != pid_a && pid_alive(pid_b))
                .unwrap_or(false)
        },
    );
    thread::sleep(Duration::from_millis(200));

    // Durability proof: the pattern written under coordinator A reads
    // back identically after:
    //   (1) daemon SIGTERM under coord A (shutdown flush),
    //   (2) coord A exit (process boundary),
    //   (3) coord B startup + supervisor adoption,
    //   (4) daemon Route::Recover + USER_RECOVERY_REATTACH.
    let got = read_pattern(dev_id, 0, BLOCK);
    assert_eq!(
        got, pattern_a,
        "[B] LBA 0 must survive coordinator-driven shutdown + Route::Recover"
    );

    // The recovered device serves new I/O end-to-end. Pick a non-zero
    // offset both to exercise multi-LBA addressing and to verify it
    // does not clobber the previously written block at LBA 0.
    const OFFSET_B: u64 = (BLOCK as u64) * 100;
    let pattern_b: Vec<u8> = (0..BLOCK).map(|i| (i as u8) ^ 0xa7).collect();
    write_pattern(dev_id, OFFSET_B, &pattern_b);
    let got = read_pattern(dev_id, OFFSET_B, BLOCK);
    assert_eq!(
        got, pattern_b,
        "[B] write/read at offset {OFFSET_B} on recovered device"
    );

    // LBA 0 is still the phase-1 pattern.
    let got = read_pattern(dev_id, 0, BLOCK);
    assert_eq!(
        got, pattern_a,
        "[B] write at offset {OFFSET_B} must not clobber LBA 0"
    );

    // Read the recovered volume's PID before signalling coord B so we
    // can SIGTERM it directly after coord B exits (defensive shutdown
    // doesn't kill it).
    let pid_b: i32 = std::fs::read_to_string(&volume_pid)
        .expect("[B] read volume.pid")
        .trim()
        .parse()
        .expect("[B] parse volume.pid");

    // Same defensive-shutdown sequence as phase 1: SIGINT coord B,
    // wait for it to exit (volume left running), then SIGTERM the
    // volume to exercise its own shutdown-park.
    unsafe {
        let rc = kill(coord_b.id() as i32, SIGINT);
        assert_eq!(rc, 0, "[B] kill(SIGINT) to coordinator failed");
    }
    let status_b = wait_with_timeout(&mut coord_b, Duration::from_secs(20), "[B] coordinator");
    assert!(
        status_b.success(),
        "[B] coordinator exited non-zero: {status_b:?}"
    );
    assert!(
        pid_alive(pid_b),
        "[B] volume process {pid_b} died under coord SIGINT — defensive policy regressed"
    );
    unsafe {
        let rc = kill(pid_b, SIGTERM);
        assert_eq!(rc, 0, "[B] kill(SIGTERM) to volume failed");
    }
    wait_until(Duration::from_secs(20), "[B] volume process exit", || {
        !pid_alive(pid_b)
    });
    assert!(
        sysfs_entry_exists(dev_id),
        "[B] sysfs entry should survive volume SIGTERM (shutdown-park)"
    );
    assert_eq!(
        elide_core::config::VolumeConfig::bound_ublk_id(&fork_dir)
            .ok()
            .flatten(),
        Some(dev_id),
        "[B] bound dev_id must persist across the second shutdown too"
    );

    // Cleanup: explicit DEL_DEV so the next CI run starts from a clean
    // kernel state. Failures here would indicate a leaked device.
    delete_and_wait_sysfs_gone(dev_id);
}

/// Probe whether `pid` is alive without waiting on it. `kill(pid, 0)`
/// returns 0 when the process exists and we have permission; ESRCH
/// when it doesn't. EPERM (process exists but we can't signal it) is
/// also "alive".
fn pid_alive(pid: i32) -> bool {
    let rc = unsafe { kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    errno == libc::EPERM
}
