# Design: ublk shutdown leaves QUIESCED; deletion is explicit

**Status:** Implemented.

Implementation notes:

- `src/ublk.rs` `run_volume_ublk` — post-`run_target` cleanup no longer calls
  `del_dev`. The for-add ctrl is `mem::forget`-ed (option 1 in §Mechanism) to
  suppress libublk's auto-delete on Drop; the recover ctrl is dropped
  normally (its Drop is already a no-op). The `DEL_DEV_TIMEOUT` watchdog and
  the `clear_ublk_id` call on shutdown are removed.
- `elide-coordinator/src/ublk_sweep.rs` — startup reconciliation sweep that
  enumerates `/sys/class/ublk-char/`, walks `<data_dir>/by_id/*/ublk.id`,
  shells out to `elide ublk delete <id>` for orphan kernel devices (with a
  per-device 10s subprocess timeout), and removes binding files whose
  kernel device has gone (e.g. after a host reboot). Wired into `daemon::run`
  before the first scan tick.
- `elide ublk delete` (existing CLI) remains the explicit deletion verb.
- Open question §"Boot-time devices" is resolved: sweep step 4 clears
  `ublk.id` files whose sysfs entry no longer exists; the next serve takes
  `Route::Add { target_id: None }` and the kernel auto-allocates a fresh id.

## Problem

`elide serve-volume --ublk` currently treats process shutdown and ublk device deletion as the same operation: SIGTERM → `kill_dev` (STOP_DEV) → `run_target` returns → `del_dev`. The shutdown path is "stop and destroy."

The destroy half of that fails badly when something else holds a reference on the kernel block device:

- `del_dev` parks in `wait_event` until every reference on the `ublk_device` struct is released.
- udev/systemd transiently opens `/dev/ublkb<N>` when the device first appears; if the daemon shuts down before that fd closes, `del_dev` blocks.
- A live mount on `/dev/ublkb<N>` holds `bd_holders` on the gendisk indefinitely. `del_dev` cannot complete until `umount`.

PR #140 adds a 3-second timeout so the volume process can exit instead of wedging in the kernel, but the underlying conflation remains: every clean shutdown still attempts deletion, and deletion can stall arbitrarily on holders the operator may not even know about. After a timed-out shutdown the kernel state is "deleting, blocked" — neither cleanly recoverable nor cleanly absent — so a subsequent serve at the same id fails (`-EEXIST` from the still-allocated idr slot) until a `umount` releases the gendisk and the kernel finishes the deferred deletion.

## Observation

ublk's `UBLK_F_USER_RECOVERY` flag is designed for the opposite policy. When the daemon dies, the kernel transitions the device to QUIESCED, buffers in-flight I/O, and waits for a new daemon to attach via `START_USER_RECOVERY` + reissue. Mounts stay in place; from the guest's perspective I/O is paused, not lost.

Our current shutdown policy actively works against this: we use `USER_RECOVERY` to handle *crashes*, but on a clean shutdown we still tear the device down. Operators who restart the volume daemon while the filesystem is mounted hit the worst case for a feature that exists to make exactly that scenario safe.

## Proposal

Separate "daemon stops" from "device is destroyed."

- **Shutdown signal (SIGTERM/SIGINT/SIGHUP) leaves the device QUIESCED.** `kill_dev` (STOP_DEV) parks the device, queue threads exit, the daemon process exits, sysfs entry remains. `ublk.id` stays in place. Mount stays in place. Next serve at the same volume sees the QUIESCED device and takes the recovery path.
- **Deletion becomes an explicit verb.** `elide ublk delete <id>` (already exists as a CLI) is the supported path. The coordinator runs a startup-time reconciliation sweep that deletes any sysfs ublk device whose `ublk.id` does not match a currently-known volume directory.
- **Device unbind = volume removal.** The volume itself getting deleted is the trigger for cleaning up its ublk device, not the daemon getting signaled. Deleting a volume implies stopping its daemon, then `del_dev`'ing its bound id, then removing the volume directory. The order matters and is now explicit.

### Operations ordering, restated

| Sequence                              | Today (PR #140)                                  | Proposed                                  |
|---------------------------------------|--------------------------------------------------|-------------------------------------------|
| `umount → stop → start → mount`       | Clean: `del_dev` succeeds, fresh ADD             | Clean: device parked, recovery on start   |
| `stop → umount → start → mount`       | Recoverable: timeout, deferred del, fresh ADD    | Clean: device parked, recovery on start   |
| `stop → start → umount → mount`       | Broken: start fails until umount finishes del    | Clean: recovery succeeds while mounted    |
| `stop` then volume deleted            | Clean: del_dev ran during stop                   | Requires explicit `elide ublk delete`     |
| `stop` then daemon never restarted    | Clean: device gone                               | Stale device until coordinator sweep      |

The third sequence — start while mounted — is the one users actually want to "just work." Today it doesn't; under the proposal it does, because the kernel has been doing exactly the right thing all along and we were undoing it.

## Mechanism

### Suppressing libublk's auto-delete

`libublk-0.4.5/src/ctrl.rs` `Drop for UblkCtrl` calls `del_dev()` synchronously when `for_add_dev()` returns true (i.e. `UBLK_DEV_F_ADD_DEV` was set at construction). The RECOVER path already drops without auto-delete, but the ADD path's clean shutdown would auto-delete and we want to suppress that.

Three options, in increasing order of cleanliness:

1. **`mem::forget(ctrl)` after stop.** Skips Drop entirely, leaks the ctrl struct (open `/dev/ublk-control` fd, ~kilobyte heap allocation). Process is exiting anyway, so the leak is reaped by the kernel. Functionally fine; ugly and signals the wrong thing to readers.
2. **Reconstruct as a non-add ctrl before drop.** After `kill_dev`, drop the for-add ctrl deliberately into a new `UblkCtrl::new_simple(id)` that does not auto-delete. Has to be done carefully so the for-add Drop doesn't fire first.
3. **Upstream a `keep_alive` flag in libublk.** A builder option (`UBLK_DEV_F_NO_AUTO_DEL`?) that disables the Drop-time del. Cleanest, requires patching libublk and waiting for a release.

Recommendation: ship option 1 immediately under a clear comment ("intentional: leave QUIESCED for recovery; kernel reaps fd at exit"); pursue option 3 as a parallel upstream contribution.

### Reconciliation sweep

On coordinator startup, before the first scan tick:

1. Enumerate `/sys/class/ublk-char/ublkc*` to get the set of live ublk dev ids.
2. Walk `<data_dir>/by_id/*/ublk.id` to get the set of bound ids.
3. For each live id with no matching binding: log + `del_dev`. (Same `new_simple+del_dev` pattern as the existing `ublk delete` CLI.)
4. For each binding pointing at a non-existent live id: clear the file (the device was deleted out-of-band).

This sweep is idempotent and cheap. It runs once at coordinator startup; no need to run it on every tick.

### Bound timeout still applies

The `del_dev` paths that *do* run — the explicit `elide ublk delete <id>` command and the coordinator's reconciliation sweep — should still wrap the call in the bounded-thread + timeout pattern from PR #140. Mount-pinning still applies; we just hit it less often.

## Trade-offs

**Devices accumulate when volumes are deleted unsafely.** Today, daemon shutdown del's the device, so even `rm -rf <volume>` after a clean stop leaves no kernel state. Under the proposal, deleting the volume directory while the daemon is stopped strands the ublk device until the next coordinator startup sweep. Mitigation: documenting `elide volume delete` as the only supported deletion path, and having `elide volume delete` orchestrate the daemon stop + ublk delete + directory removal sequence.

**`ublk.id` is never cleared by the daemon.** Today a clean shutdown clears `ublk.id`; under the proposal it stays for recovery. The next serve sees it, finds a QUIESCED device in sysfs, takes `Route::Recover`. If the device is gone (operator did `elide ublk delete` between stops), the existing `sysfs_has(id)` check downgrades the route to `Route::Add { target_id: Some(id) }` and re-adds at the same id. Both branches already exist in `plan_route`.

**One more state for operators to reason about.** "Volume daemon stopped" is no longer the same as "ublk device gone." `elide volume status` should show both, and document the distinction. This is arguably *more* honest than today: the kernel has always had this distinction, we were just hiding it.

## Open questions

- **Coordinator-managed restarts.** The coordinator currently respawns crashed daemons. Should it also explicitly issue `del_dev` when a volume is removed from its supervised set, or is that always operator-driven via `elide volume delete`?
- **Unprivileged tier interaction.** With `UBLK_F_UNPRIVILEGED_DEV` (multi-tenant tier per `design-ublk-transport.md`), the per-tenant uid owns the device. Does the operator-run reconciliation sweep need additional permissions to delete a tenant's parked device? Likely yes — sweep belongs to a privileged context, not the per-tenant daemon.
- **Boot-time devices.** If the host reboots, the kernel forgets all ublk devices. `ublk.id` files on disk become stale. Sweep #4 (clear files pointing at non-existent ids) covers this; verify that a fresh serve from a stale `ublk.id` cleanly transitions to a new ADD without operator intervention.

## Non-goals

- Not changing `USER_RECOVERY_REISSUE` semantics. The recovery flow itself is unchanged; we're only changing whether shutdown unconditionally del_dev's.
- Not removing the PR #140 timeout. It's still the right safety net for the explicit-delete paths (CLI command, coordinator sweep) when udev or a stale mount holds a reference.
- Not introducing a new daemon-coordinator IPC. The shutdown path is local to `serve-volume`; the sweep is local to the coordinator.

## References

- `src/ublk.rs` — current shutdown path (`run_volume_ublk` post-`run_target` cleanup)
- `docs/design-ublk-transport.md` §"Crash recovery" — recovery design that this proposal makes routine instead of crash-only
- PR #140 — bounded `del_dev` + SIGKILL escalation (the immediate fix this builds on)
- libublk Drop: `libublk-0.4.5/src/ctrl.rs:993` (`force_sync = true`, `self.del()`)
- Kernel: `drivers/block/ublk_drv.c` `ublk_ctrl_del_dev` — `wait_event` for refs to drop
