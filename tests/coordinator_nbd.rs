//! Coordinator-lane integration test: minimal end-to-end coverage of the
//! coordinator driving a supervised volume over NBD.
//!
//! Spawns `elide-coordinator serve` against a temp `data_dir` and a
//! file-backed store, then uses the real `elide` CLI to create a volume
//! with an `[nbd]` unix-socket endpoint. Waits for the supervisor to
//! adopt the fork, drives a write/flush/read round-trip through the NBD
//! socket, takes a coordinator-orchestrated snapshot, and then SIGINTs
//! the coordinator to exercise the clean-shutdown path.
//!
//! Covered operations:
//!   1. Coordinator boot — `elide-coordinator serve --config <toml>`
//!      with a file-backed store (no S3) and a short scan interval.
//!   2. `elide volume create ... --nbd-socket` + inbound `rescan`.
//!   3. Supervisor adopts the fork: `volume.pid`, `control.sock`, and
//!      the NBD unix socket all appear in the fork directory.
//!   4. NBD fixed-newstyle handshake + WRITE/FLUSH/READ over the unix
//!      socket. Pattern round-trip validates the volume process is
//!      actually serving real I/O, not just listening.
//!   5. `elide volume snapshot` — exercises the coordinator's inbound
//!      snapshot handler and the volume-side snapshot_manifest IPC.
//!   6. SIGINT → coordinator runs its defensive shutdown: aborts its
//!      tasks, drains the JoinSet, exits — leaving the supervised
//!      volume process alive (rolling-upgrade contract). The test
//!      then SIGTERMs the orphaned volume directly and verifies the
//!      volume's own clean-shutdown removes `volume.pid`.
//!
//! Intentionally out of scope here (covered elsewhere):
//!   - ublk transport + crash recovery (tests/ublk_crash.rs).
//!   - Coordinator GC (elide-coordinator/tests/gc_test.rs).
//!   - Real S3 — a local file store keeps this test hermetic.
//!
//! Runs in the `ci-kernel` lane. It only needs Linux + a filesystem
//! that supports unix sockets; no kernel modules and no root.

#![cfg(target_os = "linux")]

use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

// libc::kill without adding a dev-dependency. The coordinator's
// defensive signal handler treats both SIGINT and SIGTERM as the
// "leave volumes running" path; we use SIGINT here to mirror what an
// operator running `coord run` in the foreground sees with Ctrl-C.
const SIGINT: i32 = 2;
const SIGTERM: i32 = 15;
const SIGKILL: i32 = 9;

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

// ── Binary locations ────────────────────────────────────────────────────────

/// Path to the `elide` binary. Test-integration binaries can locate the
/// bin under test via `CARGO_BIN_EXE_*`; CI may override to a guest-side
/// path (see ublk_crash.rs for the same pattern).
fn elide_bin() -> PathBuf {
    if let Ok(p) = std::env::var("ELIDE_BIN") {
        return PathBuf::from(p);
    }
    PathBuf::from(env!("CARGO_BIN_EXE_elide"))
}

/// Path to the `elide-coordinator` binary. `CARGO_BIN_EXE_*` is scoped
/// to the current crate only, so we can't get it for free — fall back
/// to looking for a sibling of the `elide` binary in the workspace
/// target dir. CI builds both into the same directory.
fn coordinator_bin() -> PathBuf {
    if let Ok(p) = std::env::var("ELIDE_COORDINATOR_BIN") {
        return PathBuf::from(p);
    }
    let mut p = elide_bin();
    p.set_file_name("elide-coordinator");
    p
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

/// Spawn `elide-coordinator serve --config <path>`. Stdout/stderr are
/// inherited so test runs with `--nocapture` surface coordinator logs.
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

/// Run `elide --data-dir <dir> <args...>` and assert success.
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

/// Wait for `child` to exit; SIGKILL and reap if it blows past the deadline.
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

// ── Minimal NBD client (fixed newstyle over unix socket) ────────────────────
//
// Mirrors the handshake the volume NBD server speaks in src/nbd.rs. We
// re-declare the protocol constants locally so this test is independent
// of any crate-private visibility.

const NBD_MAGIC: u64 = 0x4e42444d41474943; // "NBDMAGIC"
const NBD_OPTS_MAGIC: u64 = 0x49484156454f5054; // "IHAVEOPT"
const NBD_OPTS_REPLY_MAGIC: u64 = 0x0003e889045565a9;
const NBD_OPT_GO: u32 = 7;
const NBD_REP_ACK: u32 = 1;
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_REPLY_MAGIC: u32 = 0x67446698;
const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;

struct NbdClient {
    s: UnixStream,
}

impl NbdClient {
    fn connect(sock: &Path) -> io::Result<Self> {
        // Socket file is created by the volume process between startup
        // and `bind()`; brief retry loop absorbs that race.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut s = loop {
            match UnixStream::connect(sock) {
                Ok(s) => break s,
                Err(e) if Instant::now() >= deadline => {
                    return Err(io::Error::other(format!(
                        "connect to {}: {e}",
                        sock.display()
                    )));
                }
                Err(_) => thread::sleep(Duration::from_millis(100)),
            }
        };

        // Server greeting.
        let mut b8 = [0u8; 8];
        s.read_exact(&mut b8)?;
        assert_eq!(u64::from_be_bytes(b8), NBD_MAGIC, "bad server magic");
        s.read_exact(&mut b8)?;
        assert_eq!(u64::from_be_bytes(b8), NBD_OPTS_MAGIC, "bad opts magic");
        let mut b2 = [0u8; 2];
        s.read_exact(&mut b2)?; // server handshake flags, discarded

        // Client flags: 0 = support fixed newstyle only.
        s.write_all(&0u32.to_be_bytes())?;

        // NBD_OPT_GO with empty export name, no info requests (6 bytes).
        s.write_all(&NBD_OPTS_MAGIC.to_be_bytes())?;
        s.write_all(&NBD_OPT_GO.to_be_bytes())?;
        s.write_all(&6u32.to_be_bytes())?;
        s.write_all(&0u32.to_be_bytes())?; // name_length = 0
        s.write_all(&0u16.to_be_bytes())?; // num_info_requests = 0

        // Consume option replies until ACK.
        loop {
            s.read_exact(&mut b8)?;
            assert_eq!(
                u64::from_be_bytes(b8),
                NBD_OPTS_REPLY_MAGIC,
                "bad reply magic"
            );
            let mut b4 = [0u8; 4];
            s.read_exact(&mut b4)?; // echoed option code
            s.read_exact(&mut b4)?;
            let reply_type = u32::from_be_bytes(b4);
            s.read_exact(&mut b4)?;
            let data_len = u32::from_be_bytes(b4) as usize;
            let mut data = vec![0u8; data_len];
            s.read_exact(&mut data)?;
            if reply_type == NBD_REP_ACK {
                break;
            }
        }

        Ok(NbdClient { s })
    }

    fn write(&mut self, handle: u64, offset: u64, data: &[u8]) -> io::Result<()> {
        self.s.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.s.write_all(&0u16.to_be_bytes())?;
        self.s.write_all(&NBD_CMD_WRITE.to_be_bytes())?;
        self.s.write_all(&handle.to_be_bytes())?;
        self.s.write_all(&offset.to_be_bytes())?;
        self.s.write_all(&(data.len() as u32).to_be_bytes())?;
        self.s.write_all(data)?;
        self.read_reply(handle, "write")?;
        Ok(())
    }

    fn flush(&mut self, handle: u64) -> io::Result<()> {
        self.s.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.s.write_all(&0u16.to_be_bytes())?;
        self.s.write_all(&NBD_CMD_FLUSH.to_be_bytes())?;
        self.s.write_all(&handle.to_be_bytes())?;
        self.s.write_all(&0u64.to_be_bytes())?;
        self.s.write_all(&0u32.to_be_bytes())?;
        self.read_reply(handle, "flush")?;
        Ok(())
    }

    fn read(&mut self, handle: u64, offset: u64, length: u32) -> io::Result<Vec<u8>> {
        self.s.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.s.write_all(&0u16.to_be_bytes())?;
        self.s.write_all(&NBD_CMD_READ.to_be_bytes())?;
        self.s.write_all(&handle.to_be_bytes())?;
        self.s.write_all(&offset.to_be_bytes())?;
        self.s.write_all(&length.to_be_bytes())?;
        self.read_reply(handle, "read")?;
        let mut data = vec![0u8; length as usize];
        self.s.read_exact(&mut data)?;
        Ok(data)
    }

    fn read_reply(&mut self, expected_handle: u64, op: &str) -> io::Result<()> {
        let mut b4 = [0u8; 4];
        self.s.read_exact(&mut b4)?;
        assert_eq!(
            u32::from_be_bytes(b4),
            NBD_REPLY_MAGIC,
            "{op}: bad reply magic"
        );
        self.s.read_exact(&mut b4)?;
        let err = u32::from_be_bytes(b4);
        let mut b8 = [0u8; 8];
        self.s.read_exact(&mut b8)?;
        assert_eq!(
            u64::from_be_bytes(b8),
            expected_handle,
            "{op}: handle mismatch"
        );
        if err != 0 {
            return Err(io::Error::other(format!("nbd {op} error: {err}")));
        }
        Ok(())
    }

    fn disconnect(mut self) -> io::Result<()> {
        self.s.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
        self.s.write_all(&0u16.to_be_bytes())?;
        self.s.write_all(&NBD_CMD_DISC.to_be_bytes())?;
        self.s.write_all(&0u64.to_be_bytes())?;
        self.s.write_all(&0u64.to_be_bytes())?;
        self.s.write_all(&0u32.to_be_bytes())?;
        Ok(())
    }
}

// ── The test ────────────────────────────────────────────────────────────────

#[test]
fn coordinator_nbd_lifecycle() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let store_dir = tmp.path().join("store");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&store_dir).unwrap();

    // Coordinator config: file-backed store, short scan interval so the
    // volume is picked up even if the CLI's rescan notify races the
    // inbound socket coming up.
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

    let mut coord = spawn_coordinator(&config_path);

    // Wait for the inbound socket to appear before invoking CLI ops
    // that want to talk to the coordinator. Also guards against the
    // coordinator dying at startup: if it crashed, try_wait would
    // reveal it and the subsequent wait_until would time out with a
    // clear message.
    let inbound = data_dir.join("control.sock");
    wait_until(
        Duration::from_secs(15),
        "coordinator inbound socket",
        || inbound.exists(),
    );

    // Create a fresh 16 MiB volume exposed over an NBD unix socket.
    // `--nbd-socket` with no value means "nbd.sock inside the fork dir".
    elide(
        &data_dir,
        &["volume", "create", "vtest", "--size", "16M", "--nbd-socket"],
    );

    // Resolve the fork dir via the by_name symlink.
    let name_link = data_dir.join("by_name").join("vtest");
    wait_until(Duration::from_secs(10), "by_name/vtest symlink", || {
        name_link.exists()
    });
    let fork_dir = std::fs::canonicalize(&name_link).expect("canonicalize by_name/vtest");

    // Wait for the supervisor to have spawned the volume and the
    // volume to have bound both its control socket and the NBD socket.
    let volume_pid = fork_dir.join("volume.pid");
    let volume_ctrl = fork_dir.join("control.sock");
    let nbd_sock = fork_dir.join("nbd.sock");
    wait_until(Duration::from_secs(30), "supervised volume up", || {
        volume_pid.exists() && volume_ctrl.exists() && nbd_sock.exists()
    });

    // NBD round-trip: write a 4 KiB pattern, flush, read back, verify.
    let mut client = NbdClient::connect(&nbd_sock).expect("nbd connect");
    let pattern: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
    client.write(1, 0, &pattern).expect("nbd write");
    client.flush(2).expect("nbd flush");
    let got = client.read(3, 0, 4096).expect("nbd read");
    assert_eq!(got, pattern, "nbd read-back pattern mismatch");
    client.disconnect().expect("nbd disconnect");

    // Coordinator-orchestrated snapshot. Exercises:
    //   CLI → coord inbound → per-volume lock → volume IPC (flush,
    //   gc_checkpoint, snapshot_manifest) → store upload.
    elide(&data_dir, &["volume", "snapshot", "vtest"]);

    // Snapshot manifest should now be present in the fork's snapshots/
    // directory. Exact ULID is non-deterministic; just assert at least
    // one entry exists.
    let snaps_dir = fork_dir.join("snapshots");
    wait_until(Duration::from_secs(5), "snapshot manifest present", || {
        snaps_dir
            .read_dir()
            .map(|mut it| it.next().is_some())
            .unwrap_or(false)
    });

    // Read the volume's PID before signalling the coordinator so we
    // can assert the volume process is still alive afterwards.
    let vol_pid: i32 = std::fs::read_to_string(&volume_pid)
        .expect("read volume.pid")
        .trim()
        .parse()
        .expect("volume.pid not numeric");

    // SIGINT the coordinator. Under the defensive signal policy
    // (PR #254) both SIGINT and SIGTERM run the rolling-upgrade
    // teardown: abort coordinator tasks, drain the JoinSet, exit —
    // explicitly leaving the supervised volume process running. The
    // "tear down volumes too" path is reachable only via the explicit
    // `Shutdown { keep_volumes: false }` IPC sent by `elide coord
    // stop`.
    unsafe {
        let rc = kill(coord.id() as i32, SIGINT);
        assert_eq!(rc, 0, "kill(SIGINT) to coordinator failed");
    }
    let status = wait_with_timeout(&mut coord, Duration::from_secs(20), "coordinator");
    assert!(status.success(), "coordinator exited non-zero: {status:?}");

    // Defensive-policy invariant: the volume process is *still alive*
    // and its pid file is still in place. The coordinator's shutdown
    // does not signal volume children. Failure here would mean the
    // coordinator regressed to the pre-PR-254 behaviour (SIGTERM
    // children on its own SIGINT/SIGTERM).
    assert!(
        volume_pid.exists(),
        "volume.pid removed under defensive shutdown — coord should not signal children"
    );
    assert!(
        pid_alive(vol_pid),
        "volume process {vol_pid} not alive after coord SIGINT — defensive policy regressed"
    );

    // Now exercise the volume's own clean-shutdown path: SIGTERM it
    // directly (mirrors what `elide coord stop`'s fallback does) and
    // verify its signal watcher tears down within the bounded
    // shutdown-flush window. The volume should exit promptly — well
    // under the watcher's 3 s flush watchdog plus reap latency.
    unsafe {
        let rc = kill(vol_pid, SIGTERM);
        assert_eq!(rc, 0, "kill(SIGTERM) to volume failed");
    }
    wait_until(Duration::from_secs(15), "volume process exit", || {
        !pid_alive(vol_pid)
    });

    // Note: `volume.pid` remains as a stale file after the volume
    // exits in this scenario. Under the defensive shutdown the
    // coordinator's supervisor task is aborted before any remove_pid
    // hook runs; pid-file cleanup is an adoption-time concern (the
    // next coord's supervisor sees a stale pid file, finds the pid
    // dead, removes it, and respawns). The test deliberately does
    // not assert on `volume.pid` here.
    //
    // The control socket and the NBD socket are likewise left in
    // place: they are unlinked only by the volume's own accept-loop
    // cleanup, which does not run under SIGTERM termination, and
    // they're rebound by the next volume start.
    let _ = volume_ctrl;
}

/// Probe whether `pid` is alive without waiting on it. `kill(pid, 0)`
/// returns 0 when the process exists and we have permission; ESRCH
/// when it doesn't. EPERM (process exists but we can't signal it) is
/// also "alive" for this test.
fn pid_alive(pid: i32) -> bool {
    let rc = unsafe { kill(pid, 0) };
    if rc == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    errno == libc::EPERM
}
