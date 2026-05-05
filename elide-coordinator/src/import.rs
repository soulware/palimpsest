// Import process supervision and job registry.
//
// The coordinator spawns `elide-import` as a short-lived child process for OCI
// volume imports. Two marker files are written to the fork directory:
//
//   volume.importing — ULID of the running import (one line); present while running or interrupted
//   import.pid  — PID of the running import process (one line)
//
// The coordinator creates volume.importing before spawning and removes both files
// when the process exits (success or failure). On coordinator startup,
// cleanup_stale_locks() kills any surviving import processes and removes stale
// lock files so forks are in a clean, resumable state.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use object_store::ObjectStore;
use tokio::io::AsyncBufReadExt;
use tracing::{info, warn};
use ulid::Ulid;

use elide_coordinator::lifecycle::{MarkInitialOutcome, mark_initial_readonly};
use elide_coordinator::volume_state::{IMPORTING_FILE, PID_FILE};

use crate::inbound::CoordinatorCore;

// ── Per-domain context ───────────────────────────────────────────────────────

/// Coordinator state needed by the import flow: the universal hot core
/// plus the in-flight import registry. Constructed via
/// [`crate::inbound::IpcContext::for_import`].
#[derive(Clone)]
pub(crate) struct ImportContext {
    pub core: CoordinatorCore,
    pub registry: ImportRegistry,
}

/// Hard cap on entries in the new volume's `extent_index` provenance field.
///
/// Each entry costs a full `.idx` file load at volume open time (and at
/// remote-pull prefetch time). Chained imports naturally grow the list:
/// importing `v2 --extents-from v1` copies v1's list and appends v1, so
/// a long chain can balloon. The ceiling is chosen to comfortably cover
/// typical release trains (a base image plus a handful of point releases)
/// without open-ended growth.
pub(crate) const MAX_EXTENT_INDEX_SOURCES: usize = 32;

/// Resolve a list of `--extents-from` source volume names into the flat
/// list of entries that will populate the new volume's `extent_index`
/// provenance field.
///
/// For each explicitly-named source the function:
///   1. Resolves the `by_name/<name>` symlink to a `by_id/<ulid>` dir
///   2. Reads the source's **signed provenance** (no unsigned file fallback)
///      and copies every `extent_index` entry into the inherited set
///   3. Records the source itself at its latest snapshot as an explicit entry
///
/// Eviction, when the total exceeds `MAX_EXTENT_INDEX_SOURCES`:
///   - **Explicit entries** (the direct `--extents-from` sources) are
///     sacred and kept in full. If explicit count alone exceeds the cap,
///     the function returns an error — silently dropping operator intent
///     is worse than a clean failure.
///   - **Inherited entries** fill the remaining slots. The first
///     inherited entry encountered (oldest-added in the source's list) is
///     always kept as the "base" position; the rest of the slots are
///     filled with the most recently added inherited entries. Middle
///     inherited entries are dropped with a warning.
///
/// Returns the final flat list (empty if `sources` is empty).
fn build_extent_index_entries(sources: &[String], data_dir: &Path) -> std::io::Result<Vec<String>> {
    if sources.is_empty() {
        return Ok(Vec::new());
    }

    // Phase 1: resolve every source, collecting its own explicit entry and
    // the inherited entries from its provenance. Order is preserved so that
    // "oldest inherited" is well-defined (first-appearing in the source's
    // flat list).
    let mut explicit: Vec<String> = Vec::new();
    let mut inherited: Vec<String> = Vec::new();
    let mut seen_ulids: std::collections::HashSet<String> = std::collections::HashSet::new();

    for source_name in sources {
        let by_name_link = data_dir.join("by_name").join(source_name);
        let source_dir = std::fs::canonicalize(&by_name_link).map_err(|e| {
            std::io::Error::other(format!(
                "extents-from volume '{source_name}' not found at {}: {e}",
                by_name_link.display()
            ))
        })?;
        let source_ulid_str = source_dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "extents-from volume '{source_name}' resolves to a non-utf8 path"
                ))
            })?;
        let source_ulid = Ulid::from_string(source_ulid_str).map_err(|e| {
            std::io::Error::other(format!(
                "extents-from volume '{source_name}' resolves to non-ulid dir name '{source_ulid_str}': {e}"
            ))
        })?;

        // Inherit entries from the source's signed provenance. Signature
        // verification guards against a tampered source dragging malicious
        // lineage claims into the new volume. Host/path match is NOT
        // required (the source may have been pulled from a different host).
        let source_lineage = elide_core::signing::read_lineage_verifying_signature(
            &source_dir,
            elide_core::signing::VOLUME_PUB_FILE,
            elide_core::signing::VOLUME_PROVENANCE_FILE,
        )
        .map_err(|e| {
            std::io::Error::other(format!(
                "extents-from volume '{source_name}' provenance read failed: {e}"
            ))
        })?;

        for entry in source_lineage.extent_index {
            let ulid_key = entry
                .split_once('/')
                .and_then(|(u, s)| {
                    Ulid::from_string(u).ok()?;
                    Ulid::from_string(s).ok()?;
                    Some(u.to_owned())
                })
                .ok_or_else(|| {
                    std::io::Error::other(format!(
                        "extents-from volume '{source_name}' has malformed extent_index entry: {entry}"
                    ))
                })?;
            if seen_ulids.insert(ulid_key) {
                inherited.push(entry);
            }
        }

        // Add the source itself as an explicit entry at its latest snapshot.
        let snapshot = elide_core::volume::latest_snapshot(&source_dir)?.ok_or_else(|| {
            std::io::Error::other(format!(
                "extents-from volume '{source_name}' has no snapshot; only imported/snapshotted volumes can contribute an extent index"
            ))
        })?;
        if seen_ulids.insert(source_ulid_str.to_owned()) {
            explicit.push(format!("{source_ulid}/{snapshot}"));
        }
    }

    // Phase 2: eviction. Explicit entries are sacred.
    if explicit.len() > MAX_EXTENT_INDEX_SOURCES {
        return Err(std::io::Error::other(format!(
            "--extents-from lists {} explicit sources, exceeding the hard cap of {}; reduce the number of direct sources",
            explicit.len(),
            MAX_EXTENT_INDEX_SOURCES
        )));
    }
    let slots_for_inherited = MAX_EXTENT_INDEX_SOURCES - explicit.len();

    let final_inherited: Vec<String> = if inherited.len() <= slots_for_inherited {
        inherited
    } else {
        // Keep-oldest-plus-most-recent: always preserve the first inherited
        // entry (the base, most likely to hold the largest reusable
        // footprint) and fill the remaining slots with the N most recently
        // added inherited entries. Middle entries are dropped.
        let dropped = inherited.len() - slots_for_inherited;
        warn!(
            "[import] extent-index inherited source count {} exceeds available slots {}; \
             dropping {} middle entries (keeping 1 oldest + {} most recent)",
            inherited.len(),
            slots_for_inherited,
            dropped,
            slots_for_inherited.saturating_sub(1)
        );
        let mut kept = Vec::with_capacity(slots_for_inherited);
        if slots_for_inherited > 0 {
            kept.push(inherited[0].clone());
            let tail_take = slots_for_inherited - 1;
            let tail_start = inherited.len() - tail_take;
            for entry in &inherited[tail_start..] {
                kept.push(entry.clone());
            }
        }
        kept
    };

    let mut result = explicit;
    result.extend(final_inherited);
    Ok(result)
}

/// Pidfile for the import subprocess (distinct from the volume daemon's
/// `volume.pid`). Local to this module since the supervisor never spawns
/// or adopts an import subprocess via this file.
const IMPORT_PID_FILE: &str = "import.pid";

#[derive(Clone, Debug)]
pub enum ImportState {
    Running,
    Done,
    Failed(String),
}

pub struct ImportJob {
    /// Fork directory being written by the import process.
    #[allow(dead_code)]
    pub fork_dir: PathBuf,
    /// PID of the running import process; useful for diagnostics.
    #[allow(dead_code)]
    pub pid: u32,
    lines: Mutex<Vec<String>>,
    pub state: RwLock<ImportState>,
}

impl ImportJob {
    fn new(fork_dir: PathBuf, pid: u32) -> Arc<Self> {
        Arc::new(Self {
            fork_dir,
            pid,
            lines: Mutex::new(Vec::new()),
            state: RwLock::new(ImportState::Running),
        })
    }

    fn append(&self, line: String) {
        self.lines
            .lock()
            .expect("import job lines poisoned")
            .push(line);
    }

    fn finish(&self, state: ImportState) {
        *self.state.write().expect("import job state poisoned") = state;
    }

    /// Return output lines starting from `offset`.
    pub fn read_from(&self, offset: usize) -> Vec<String> {
        self.lines.lock().expect("import job lines poisoned")[offset..].to_vec()
    }

    pub fn state(&self) -> ImportState {
        self.state
            .read()
            .expect("import job state poisoned")
            .clone()
    }
}

pub type ImportRegistry = Arc<Mutex<HashMap<String, Arc<ImportJob>>>>;

pub fn new_registry() -> ImportRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Caller-supplied parameters for `spawn_import`. Bundled so the request
/// fields stay together as a unit, separate from coordinator-side wiring.
pub struct ImportRequest<'a> {
    pub vol_name: &'a str,
    pub oci_ref: &'a str,
    pub extents_from: &'a [String],
}

/// Spawn an import process for `req.vol_name` using OCI image `req.oci_ref`.
///
/// Generates a ULID for the new volume, creates `<data_dir>/by_id/<ulid>/`,
/// writes `volume.name`, `volume.readonly`, and `volume.importing`, then spawns
/// `elide-import`. On success, creates the `<data_dir>/by_name/<vol_name>`
/// symlink. Returns the import job ULID.
pub async fn spawn_import(
    req: ImportRequest<'_>,
    elide_import_bin: &Path,
    store: Arc<dyn ObjectStore>,
    ctx: &ImportContext,
) -> std::io::Result<String> {
    let data_dir: &Path = &ctx.core.data_dir;
    let registry = &ctx.registry;
    let rescan_notify = ctx.core.rescan.clone();
    let identity = ctx.core.identity.clone();
    let ImportRequest {
        vol_name,
        oci_ref,
        extents_from,
    } = req;
    crate::inbound::validate_volume_name(vol_name).map_err(std::io::Error::other)?;

    let by_name_dir = data_dir.join("by_name");
    let symlink_path = by_name_dir.join(vol_name);

    // Reject if a volume with this name already exists.
    // Use is_symlink() || exists() so that a broken symlink (target deleted)
    // is still treated as "name in use" rather than silently overwritten.
    if symlink_path.is_symlink() || symlink_path.exists() {
        return Err(std::io::Error::other(format!(
            "volume already exists: {vol_name}"
        )));
    }

    // If --extents-from was given, resolve all sources up-front and
    // compute the flat extent-source list (applying the eviction rule).
    // Fail fast before creating any on-disk state for the new volume.
    let extent_sources = build_extent_index_entries(extents_from, data_dir)?;

    // Generate a stable ULID for this volume (= S3 prefix).
    let vol_ulid_value = Ulid::new();
    let vol_ulid = vol_ulid_value.to_string();
    let vol_dir = data_dir.join("by_id").join(&vol_ulid);

    // The bucket-side `names/<name>` claim is *deferred* until after
    // import completes: `NameRecord` requires a real `size`, and the
    // OCI image's logical size isn't known until `import_image` reads
    // it from the extracted ext4. The post-wait task at the end of
    // this function calls `mark_initial_readonly` with the final size.
    //
    // Cross-coordinator implication: two hosts racing to import the
    // same name will both download/extract the OCI image; the second
    // to call `mark_initial_readonly` sees `AlreadyExists` and rolls
    // back local state. Wasted CPU/bandwidth, no corruption.

    std::fs::create_dir_all(&by_name_dir)?;
    std::fs::create_dir_all(&vol_dir)?;
    // Wrap the rest of the local setup so any error rolls back local
    // artefacts. No bucket-side claim exists yet — `mark_initial_readonly`
    // runs only after the import process exits successfully.
    let setup: std::io::Result<String> = (|| {
        // Write volume.readonly immediately so a crashed import is never supervised
        // as a writable volume.
        std::fs::write(vol_dir.join("volume.readonly"), "")?;
        // Write the name into volume.toml (size is added later by elide-core once
        // the import completes).
        elide_core::config::VolumeConfig {
            name: Some(vol_name.to_owned()),
            ..Default::default()
        }
        .write(&vol_dir)?;

        // Write the import lock.
        let import_ulid = Ulid::new().to_string();
        std::fs::write(vol_dir.join(IMPORTING_FILE), &import_ulid)?;

        // Create the by_name symlink immediately so `import status/attach` can
        // resolve the volume before the import completes. Removed on failure.
        std::os::unix::fs::symlink(format!("../by_id/{vol_ulid}"), &symlink_path)?;
        Ok(import_ulid)
    })();
    let import_ulid = match setup {
        Ok(u) => u,
        Err(e) => {
            let _ = std::fs::remove_file(&symlink_path);
            let _ = std::fs::remove_dir_all(&vol_dir);
            return Err(e);
        }
    };

    let mut cmd = tokio::process::Command::new(elide_import_bin);
    cmd.arg(&vol_dir)
        .arg("--image")
        .arg(oci_ref)
        .stderr(Stdio::piped())
        .stdout(Stdio::null());

    // Pass each resolved extent-source entry to elide-import. It will sign
    // them into volume.provenance as part of setup_readonly_identity, and
    // walk provenance to rebuild the parent ExtentIndex for dedup during
    // the import block loop. No separate file is written — provenance is
    // the single signed source of truth for lineage.
    for entry in &extent_sources {
        cmd.arg("--extent-source").arg(entry);
    }

    // Place the child in a new session so it is not affected by the
    // coordinator's lifetime. pre_exec is unsafe because the callback runs
    // between fork() and exec() where only async-signal-safe functions may be
    // called. setsid() is async-signal-safe.
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(std::io::Error::from)
        });
    }

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let _ = std::fs::remove_file(&symlink_path);
            let _ = std::fs::remove_dir_all(&vol_dir);
            return Err(std::io::Error::other(format!(
                "failed to spawn {}: {e}",
                elide_import_bin.display()
            )));
        }
    };

    let pid = child.id().unwrap_or(0);
    if let Err(e) = std::fs::write(vol_dir.join(IMPORT_PID_FILE), pid.to_string()) {
        // Best-effort: kill the child since we won't be tracking it.
        let _ = child.start_kill();
        let _ = std::fs::remove_file(&symlink_path);
        let _ = std::fs::remove_dir_all(&vol_dir);
        return Err(e);
    }

    let job = ImportJob::new(vol_dir.clone(), pid);
    registry
        .lock()
        .expect("import registry poisoned")
        .insert(import_ulid.clone(), job.clone());

    // Watch for the import to enter the serve phase (control.sock appears) and
    // trigger an immediate rescan so the coordinator starts draining without
    // waiting up to supervisor.scan_interval.
    {
        let watch_dir = vol_dir.clone();
        let watch_lock = vol_dir.join(IMPORTING_FILE);
        tokio::spawn(async move {
            loop {
                if watch_dir.join("control.sock").exists() {
                    rescan_notify.notify_one();
                    break;
                }
                if !watch_lock.exists() {
                    break; // import finished or failed before entering serve phase
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    let import_ulid_clone = import_ulid.clone();
    let async_store = store.clone();
    let async_vol_name = vol_name.to_owned();
    let async_identity = identity.clone();
    let async_vol_dir = vol_dir.clone();
    tokio::spawn(async move {
        // Stream stderr into the job's output buffer.
        if let Some(stderr) = child.stderr.take() {
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                job.append(line);
            }
        }

        // Wait for the process to exit.
        let (mut final_state, mut failed) = match child.wait().await {
            Ok(s) if s.success() => {
                info!("[import {import_ulid_clone}] done");
                (ImportState::Done, false)
            }
            Ok(s) => {
                let msg = format!("exited with {s}");
                warn!("[import {import_ulid_clone}] failed: {msg}");
                (ImportState::Failed(msg), true)
            }
            Err(e) => {
                let msg = format!("wait error: {e}");
                warn!("[import {import_ulid_clone}] failed: {msg}");
                (ImportState::Failed(msg), true)
            }
        };

        // Post-success: claim the bucket-side `names/<name>` now that
        // the import has computed a real `size` and written it to
        // `volume.toml`. This is the bucket-uniqueness gate for
        // imports — two coordinators racing on the same name both
        // arrive here, the second sees `AlreadyExists`, and its
        // local artefacts are rolled back like any other failure.
        if !failed {
            let claim_outcome = (|| -> Result<(u64, MarkInitialOutcome), String> {
                let cfg = elide_core::config::VolumeConfig::read(&async_vol_dir)
                    .map_err(|e| format!("reading post-import volume.toml: {e}"))?;
                let size = cfg
                    .size
                    .ok_or_else(|| "post-import volume.toml missing size".to_owned())?;
                Ok((size, MarkInitialOutcome::Claimed))
            })();
            match claim_outcome {
                Ok((size, _)) => {
                    match mark_initial_readonly(&async_store, &async_vol_name, vol_ulid_value, size)
                        .await
                    {
                        Ok(MarkInitialOutcome::Claimed) => {
                            elide_coordinator::volume_event_store::emit_best_effort(
                                &async_store,
                                async_identity.as_ref(),
                                &async_vol_name,
                                elide_core::volume_event::EventKind::Created,
                                vol_ulid_value,
                            )
                            .await;
                        }
                        Ok(MarkInitialOutcome::AlreadyExists {
                            existing_vol_ulid,
                            existing_state,
                            existing_owner,
                        }) => {
                            let owner = existing_owner.as_deref().unwrap_or("<unowned>");
                            let msg = format!(
                                "name '{async_vol_name}' already exists in bucket \
                                 (vol_ulid={existing_vol_ulid}, state={existing_state:?}, \
                                 owner={owner})"
                            );
                            warn!("[import {import_ulid_clone}] {msg}");
                            final_state = ImportState::Failed(msg);
                            failed = true;
                        }
                        Err(e) => {
                            let msg = format!("claiming name in bucket: {e}");
                            warn!("[import {import_ulid_clone}] {msg}");
                            final_state = ImportState::Failed(msg);
                            failed = true;
                        }
                    }
                }
                Err(msg) => {
                    warn!("[import {import_ulid_clone}] {msg}");
                    final_state = ImportState::Failed(msg);
                    failed = true;
                }
            }
        }

        if failed {
            // Remove the by_name symlink so the name is not reserved by a
            // failed import (the vol_dir is left for post-mortem inspection).
            // No bucket claim to release: it was either never made (early
            // failure) or never landed (claim itself failed).
            let _ = std::fs::remove_file(&symlink_path);
        }

        job.finish(final_state);
        let _ = std::fs::remove_file(async_vol_dir.join(IMPORTING_FILE));
        let _ = std::fs::remove_file(async_vol_dir.join(IMPORT_PID_FILE));
    });

    info!("[import {import_ulid}] started pid {pid} for {vol_name} from {oci_ref}");
    Ok(import_ulid)
}

/// On coordinator startup, remove stale `volume.importing` files.
///
/// A lock is stale if no live process matches `import.pid`. If a process is
/// found alive, it is sent SIGTERM so the volume is in a clean state for retry.
pub fn cleanup_stale_locks(data_dir: &Path) {
    let by_id_dir = data_dir.join("by_id");
    let Ok(entries) = std::fs::read_dir(&by_id_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            cleanup_stale_lock_in(&path);
        }
    }
}

fn cleanup_stale_lock_in(dir: &Path) {
    let lock_path = dir.join(IMPORTING_FILE);
    if !lock_path.exists() {
        return;
    }

    let ulid = std::fs::read_to_string(&lock_path)
        .unwrap_or_default()
        .trim()
        .to_owned();

    // Check if the recorded import process is still alive.
    let pid = std::fs::read_to_string(dir.join(IMPORT_PID_FILE))
        .ok()
        .and_then(|t| t.trim().parse::<u32>().ok());

    if let Some(pid) = pid
        && is_alive(pid)
    {
        // If control.sock is present the import is in its serve phase and is
        // actively handling promote IPC from the coordinator.  Leave it running
        // — the coordinator will resume draining against it on the next tick.
        if dir.join("control.sock").exists() {
            return;
        }
        // Process is alive but no control.sock: still in write phase when the
        // coordinator restarted.  Send SIGTERM so the volume is in a clean
        // state for retry.
        sigterm(pid);
        warn!(
            "[import] killed stale import process pid={pid} in {} (ulid={ulid})",
            dir.display()
        );
    }

    warn!(
        "[import] removing stale volume.importing in {} (ulid={ulid})",
        dir.display()
    );
    let _ = std::fs::remove_file(&lock_path);
    let _ = std::fs::remove_file(dir.join(IMPORT_PID_FILE));
}

/// Send SIGTERM to the import process in `fork_dir`, if one is recorded.
/// Returns true if a signal was sent.
pub fn kill_import(fork_dir: &Path) -> bool {
    let Ok(text) = std::fs::read_to_string(fork_dir.join(IMPORT_PID_FILE)) else {
        return false;
    };
    let Ok(pid) = text.trim().parse::<u32>() else {
        return false;
    };
    if !is_alive(pid) {
        return false;
    }
    sigterm(pid);
    true
}

/// Send SIGTERM to the volume and import processes in `fork_dir`.
///
/// Returns the PIDs that were signalled so the caller can wait for them
/// to exit. Used for clean coordinator shutdown in foreground mode.
pub fn terminate_fork_processes(fork_dir: &Path) -> Vec<u32> {
    let mut pids = Vec::new();
    let label = fork_dir.display();

    if let Ok(text) = std::fs::read_to_string(fork_dir.join(PID_FILE))
        && let Ok(pid) = text.trim().parse::<u32>()
        && is_alive(pid)
    {
        sigterm(pid);
        info!("[coordinator] SIGTERM volume pid={pid} in {label}");
        pids.push(pid);
    }

    if let Ok(text) = std::fs::read_to_string(fork_dir.join(IMPORT_PID_FILE))
        && let Ok(pid) = text.trim().parse::<u32>()
        && is_alive(pid)
    {
        sigterm(pid);
        info!("[coordinator] SIGTERM import pid={pid} in {label}");
        pids.push(pid);
    }

    pids
}

/// Send SIGTERM to the volume and import processes in `vol_dir`, then wait
/// briefly for them to exit. Used by the `delete` operation.
pub fn kill_all_for_volume(vol_dir: &Path) {
    if let Ok(text) = std::fs::read_to_string(vol_dir.join(PID_FILE))
        && let Ok(pid) = text.trim().parse::<u32>()
        && is_alive(pid)
    {
        sigterm(pid);
        info!(
            "[import] sent SIGTERM to volume process pid={pid} in {}",
            vol_dir.display()
        );
    }
    if kill_import(vol_dir) {
        info!(
            "[import] sent SIGTERM to import process in {}",
            vol_dir.display()
        );
    }

    // Brief pause to allow processes to exit before we remove the directory.
    std::thread::sleep(Duration::from_millis(500));
}

use elide_core::process::pid_is_alive as is_alive;

fn sigterm(pid: u32) {
    if let Ok(raw) = i32::try_from(pid) {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(raw),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
}
