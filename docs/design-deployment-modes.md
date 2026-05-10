# Deployment modes

**Status:** Proposed. No implementation. Captures the design discussion for the three operator-facing deployment shapes — `coord run`, `coord start`/`stop`, and systemd — and the internal abstraction that lets all three present the same volume / import / fetch semantics to operators while differing in how processes are launched and supervised.

## Problem

The coordinator today launches every long-running child (volume daemon, import worker, fetch worker) the same way: `Command::spawn` with `setsid`, plus a per-fork pidfile and an in-process supervisor loop. That model is uniform but blurs an important distinction:

- A **volume daemon** has live consumers (a VM via ublk) and must outlive a coordinator restart — bouncing the coordinator (rolling upgrade, panic, `systemctl restart`) must not drag the data plane with it.
- An **import worker** and a **fetch worker** are admin batch jobs with no live consumers. They are restartable and idempotent; tying their lifetime to the coordinator is the natural fit.

The volume-survival goal also turns out to be incompatible with shipping a default systemd unit: `KillMode=control-group` (the systemd default) sweeps every PID in the unit's cgroup on stop, regardless of `setsid`. cgroup membership is independent of session/process-group, so the current `setsid` is not load-bearing under systemd.

The obvious workaround — set `KillMode=process` on the coordinator's unit so only the coordinator's main PID gets signalled — is **explicitly discouraged by systemd**. From [`systemd.kill(5)`](https://www.freedesktop.org/software/systemd/man/latest/systemd.kill.html):

> Note that it is not recommended to set `KillMode=` to `process` or even `none`, as this allows processes to escape the service manager's lifecycle and resource management, and to remain running even while their service is considered stopped and is assumed to not consume any resources.

That guidance is correct: a coordinator unit with `KillMode=process` would leave volume daemons as orphans inside the unit's cgroup, accounting against a unit that systemd considers stopped. We want to *follow* the recommendation, not work around it.

The right shape is therefore: keep `KillMode=control-group` on the coordinator unit, and put each volume daemon in a *different* cgroup by making it its own systemd unit. Volumes live as transient units (`elide-vol-<vol_ulid>.service`); the coordinator's stop sweep only touches its own cgroup; volume units are unaffected and continue to be lifecycle-managed by systemd.

This document proposes three deployment modes, an internal launcher abstraction that keeps the operator surface uniform across them, and a list of decisions that need to be settled before the launcher refactor lands.

## Three modes

| Mode | Invocation | Volume daemon | Import / fetch worker | Volumes survive coord exit |
|---|---|---|---|---|
| **`coord run`** (foreground) | `elide coord run` blocks the terminal | direct fork + `setsid` | direct fork, no `setsid` | yes (own session) |
| **`coord start` / `stop`** (background) | `elide coord start` daemonises; `elide coord stop` SIGTERMs | direct fork + `setsid` | direct fork, no `setsid` | yes (own session) |
| **systemd** (production) | unit-managed coordinator; volumes are transient units | `StartTransientUnit` → `elide-vol-<vol_ulid>.service` | direct fork (child of coord) | yes (own cgroup) |

Modes 1 and 2 cover macOS and non-systemd Linux (containers, dev, CI). They are also the natural choice for kicking the tyres on Linux without writing unit files. Mode 3 is the production deployment.

Across all three modes the operator-facing CLI (`elide volume create`, `elide volume start`, `elide volume stop`, `elide volume fetch`, `elide volume list`, etc.) is identical and produces identical observable behaviour. Only the spawn and supervision mechanics differ.

## Operator-facing uniformity

What stays the same regardless of mode:

- All `elide volume *` verbs and flags.
- IPC surface (`Register`, `Credentials`, `volume_status`, `start_fetch`, etc.).
- On-disk markers in `<by_id>/<ulid>/`: `volume.pid`, `volume.stopped`, `volume.released`, `volume.fetched`, `volume.draining`, `volume.importing`, `import.pid`, `fetch.pid`.
- `volume.toml` config (including `[ublk]`).
- `serve-volume` binary and its arguments.
- Lifecycle classifier (`VolumeLifecycle::from_dir`).
- Macaroon / SO_PEERCRED handshake (uses `volume.pid` content; see "PID file ownership" below).

What differs per mode:

- How a volume daemon is launched and supervised (the launcher abstraction below).
- Where volume daemon stdout/stderr land by default (coordinator log relay vs journald; both are reachable through `elide volume logs <name>` regardless).
- The verb used to stop the coordinator itself (`coord stop` for modes 1/2, `systemctl stop elide` for mode 3 — these are the `coord` binary's own concern, not a `volume` verb).

## Internal mechanism: the volume launcher

A `VolumeLauncher` trait encapsulates the per-mode mechanics. The supervisor task no longer hard-codes `Command::spawn`; it owns *policy* (when to start, when to back off, when to mark `volume.stopped` after `EXIT_CONFIG`) and delegates *mechanism* to the launcher.

```rust
trait VolumeLauncher: Send + Sync {
    /// Start the volume daemon for `fork_dir`. Returns once the
    /// daemon is registered with whatever's supervising it; does not
    /// wait for the daemon to become ready (control.sock appearance,
    /// IPC register, etc.) — that is observed separately.
    async fn start(&self, fork_dir: &Path, env: &ChildEnv) -> Result<()>;

    /// Stop the volume daemon for `fork_dir`. Sends SIGTERM (or the
    /// systemd equivalent) and returns once the daemon has exited.
    async fn stop(&self, fork_dir: &Path, timeout: Duration) -> Result<()>;

    /// Observe the current run state. Used at coordinator startup to
    /// reconcile against existing daemons rather than respawning.
    async fn observe(&self, fork_dir: &Path) -> Result<VolumeRunState>;

    /// Wait for the volume daemon to exit. Used by the supervisor's
    /// restart loop. The implementation may poll, hold a `Child`
    /// handle, or subscribe to D-Bus signals.
    async fn await_exit(&self, fork_dir: &Path) -> ExitInfo;
}

enum VolumeRunState {
    Running { pid: u32 },
    Exited { exit_info: ExitInfo },
    NotStarted,
}
```

Two implementations:

- **`DirectLauncher`** — the existing supervisor mechanics, lifted into the trait. `start` is `Command::spawn` + `setsid` + write of `volume.pid`. `stop` SIGTERMs the recorded pid. `observe` reads `volume.pid` and checks liveness. `await_exit` holds the `Child` handle and `await`s its exit. Restart policy lives in the supervisor task that consumes the trait.

- **`SystemdLauncher`** — `start` calls `StartTransientUnit` for `elide-vol-<vol_ulid>.service`, with properties `Description=`, `KillMode=mixed`, `Restart=on-failure`, `RestartSec=`, `SuccessExitStatus=78` (for `EXIT_CONFIG`), `Environment=…` for `ChildEnv`, and `ExecStart=/usr/bin/elide serve-volume <fork_dir> [--ublk …]`. `stop` calls `StopUnit`. `observe` reads the unit's `ActiveState` + `MainPID`. `await_exit` subscribes to `JobRemoved` / `UnitRemoved` D-Bus signals and resolves when the volume's unit transitions to inactive. Restart policy lives in the unit's `Restart=` directive.

Initial implementation can use `systemd-run --quiet --collect --unit=… --property=… elide serve-volume …` shelled out. A direct D-Bus implementation (via `zbus`) is a follow-up that removes the binary dependency and improves error reporting; the trait surface does not change.

## Mode detection and selection

The coordinator selects a launcher at startup:

1. **Explicit override**: `--launcher=direct|systemd` flag (also exposed as `coordinator.toml` setting). Highest priority.
2. **Auto-detect**: use `SystemdLauncher` iff
   - `INVOCATION_ID` env var is set (we are running as a systemd unit), **and**
   - `/run/systemd/system` exists, **and**
   - the `org.freedesktop.systemd1` D-Bus name is reachable on the system bus.
3. **Fallback**: `DirectLauncher`.

The first two conditions distinguish "systemd is the host's init" from "systemd is around but we're a one-off `coord run` from a terminal." `coord run` and `coord start` always pick `DirectLauncher` regardless of host init system — they are the by-design escape hatches. Only `ExecStart` of the coordinator's systemd unit ends up with `INVOCATION_ID` set.

Detection happens once at `coord serve` startup. The chosen launcher is recorded in coordinator state (in-memory) and propagated to every supervisor task. Persisting the mode to disk would create stale-launcher hazards across coordinator restarts; re-detect each time instead.

## Re-adoption per mode

When a coordinator starts, it scans `<data_dir>/by_id/` for existing volume directories and reconciles each one against the launcher's view of "is this volume currently running."

- **`DirectLauncher`** — read `<vol_dir>/volume.pid`, check liveness via `pid_is_alive`, attach (poll until exit) or remove the stale file. Existing logic; lifted into `observe`.
- **`SystemdLauncher`** — `ListUnits` filtered by `elide-vol-*.service`. For each matching unit, read its `ActiveState`. For each volume in `by_id/` not in the active set (and not parked via `volume.stopped` / `volume.released` / `volume.fetched`), call `start`.

The existing `volume.pid` file remains the **macaroon SO_PEERCRED witness** in both modes (see next section). The systemd path additionally cross-checks the recorded pid against the unit's `MainPID` — divergence is a sign of corruption or a manually restarted unit.

## PID file ownership

Today the supervisor writes `volume.pid` externally after `Command::spawn` returns. With `SystemdLauncher`, the spawning is done by PID 1; the coordinator only learns the pid after the fact via `MainPID`. There are two reasonable fixes:

1. **Volume daemon writes its own `volume.pid`** on startup, regardless of who spawned it. The launcher contract becomes "ensure a volume daemon is running for this fork dir"; pidfile management moves into the daemon. Cleaner separation; one-time migration cost (any external code expecting the supervisor to write the file).

2. **Launcher writes `volume.pid`** after observing `MainPID`. Keeps current behaviour; adds a small race window where the daemon is up but the pidfile is not yet written, mirroring the existing spawn/register race. The macaroon retry loop already absorbs this.

**Proposed: option 1** — the volume daemon writes its own `volume.pid` and removes it on graceful exit. Removes a coupling between launcher implementations and pidfile mechanics; matches the way `import.pid` and `fetch.pid` are managed (the spawning side, but consistent in being "one writer per file"); and is structurally identical to how SaaS-like daemons usually self-report. Re-adoption still uses `volume.pid` (existing logic); the launcher only needs to ensure the daemon is running and observe its state.

## Stop semantics per mode

`elide volume stop <name>` does the same thing observably:

- **`DirectLauncher`** — read `volume.pid`, `kill(SIGTERM)`, wait for the daemon to exit (poll `pid_is_alive` until false or timeout), then continue. The supervisor's restart loop sees `volume.stopped` and parks.
- **`SystemdLauncher`** — write `volume.stopped` first (so the supervisor doesn't immediately restart), then call `StopUnit` with `JOB_MODE=replace`, then await `JobRemoved`. systemd handles SIGTERM → `TimeoutStopSec` → SIGKILL with the unit's configured timeouts.

Either way, the volume daemon receives SIGTERM, does its drain/cleanup, removes its own `volume.pid`, and exits.

## Logging per mode

Two surfaces:

- **`elide volume logs <name>`** — uniform across modes. Always reads from the IPC log relay (`log_relay.rs`). Mode 3 additionally has `journalctl -u elide-vol-<ulid>` available as a free observation surface; this is a *bonus*, not the primary path.
- **Stdout/stderr destinations** — modes 1/2 pipe through the coordinator's relay (existing). Mode 3 inherits the unit's stdout (journald by default).

For the relay to work in mode 3, the volume daemon needs to write log lines to a path the coordinator can also read — either via the existing IPC log channel (already in place) or via a sidecar file the coordinator tails. The IPC channel is the simpler choice; it works regardless of how stdout is captured.

## Restart policy mapping

The two launchers express the same policy in different vocabularies:

| Policy | DirectLauncher | SystemdLauncher (unit property) |
|---|---|---|
| Fast-failure backoff | `FAST_EXIT_THRESHOLD_SECS=5`, exponential up to `MAX_BACKOFF=60s` | `RestartSec=`, `StartLimitIntervalSec=`, `StartLimitBurst=` |
| `EXIT_CONFIG` (78) → don't restart | hard-coded check, write `volume.stopped` | `SuccessExitStatus=78` (treats 78 as success → no restart) |
| `volume.stopped` marker → don't start | supervisor consults marker before each spawn | coordinator simply does not call `StartTransientUnit` |
| `volume.draining` → no ublk | reads marker, omits `--ublk` arg | reads marker, omits `--ublk` arg in `ExecStart=` properties |

These should be pinned to the same numeric values across modes. A small policy table in code (constants used by both launchers) is the right shape.

## External-mutation hazards (mode 3 only)

In mode 3, an operator can run `systemctl stop elide-vol-<ulid>` directly. The coordinator's view (no `volume.stopped` marker) drifts from systemd's view (unit inactive). On the next supervisor tick the coordinator would call `StartTransientUnit` again, fighting the operator.

**Reconciler.** `SystemdLauncher` subscribes to the `JobRemoved` D-Bus signal at coordinator startup. When a `elide-vol-*.service` job is removed with result `done` and the unit transitions to inactive without an internal coordinator-driven stop in flight, the reconciler writes `volume.stopped` so the supervisor does not respawn.

The reverse case (operator runs `systemctl start elide-vol-<ulid>` while the coordinator thinks it's stopped) is rarer and lower-risk; the next coordinator scan picks it up via `observe`.

This reconciler is non-trivial code (D-Bus signal subscription, deduplication against coord-driven stops) but is contained inside `SystemdLauncher` and does not affect the trait surface.

## Unit file scope

Two unit files to ship:

- **`elide.service`** (or `elide-coord.service`) — coordinator unit. `Type=notify` (eventually; `Type=simple` for the first iteration), `ExecStart=/usr/bin/elide-coordinator serve`, `Restart=on-failure`, no `KillMode` override (defaults to `control-group`), runs as root for ublk.
- **No static unit file for volumes.** Volumes are transient units, created on the fly via `StartTransientUnit`. Properties are passed as part of the call. This is the standard approach for "supervisor that creates per-tenant units" patterns and matches how OCI runtimes like crun/runc create scope units.

The `elide.service` unit is the surface where `KillMode` and `INVOCATION_ID` propagation are guaranteed. The unit deliberately does **not** set `KillMode=process` — that would let volume daemons escape the unit's cgroup as orphans (the failure mode systemd's docs explicitly warn against) and is exactly the configuration this whole design is intended to avoid. Volume isolation comes from the per-volume transient unit, not from neutering the coordinator unit's kill semantics.

`coord run` and `coord start` are documented as **not** for production use even on systemd hosts — they bypass the unit and lose the cgroup-isolation property.

## When `coord stop` exists

`elide coord stop` is a modes-1/2 verb only. It SIGTERMs the running coordinator process, optionally `--stop-volumes` to terminate volume children too. Under systemd, the equivalent is `systemctl stop elide`; the coordinator's stop verb is not the right place to also stop the unit.

For uniformity we may still expose `coord stop` under systemd as a thin wrapper (`call StopUnit on elide.service`), but it's syntactic sugar at best — operators on a systemd host will reach for `systemctl` reflexively. **Proposed: omit `coord stop` from the systemd-mode CLI** rather than ship a confusing alias.

## Migration shape

Sequencing for the implementation, written down here so the launcher refactor doesn't try to do everything at once:

1. **`VolumeLauncher` trait + `DirectLauncher` impl.** No behavioural change; just lift the existing supervisor mechanics into the trait. Existing tests cover this.
2. **Volume daemon writes its own `volume.pid`.** Independent change; can land in either order with step 1.
3. **`SystemdLauncher` via `systemd-run` shell-out.** Initial implementation; gets us the cgroup-isolation property without a new dependency.
4. **Mode detection** (`INVOCATION_ID` + `/run/systemd/system` + D-Bus availability) and CLI override.
5. **`elide.service` unit file** in the package. Smoke-test the production deployment shape end-to-end.
6. **Reconciler for external mutation** (`JobRemoved` subscription). Adds robustness; non-blocking for the initial cut.
7. **Direct D-Bus implementation** of `SystemdLauncher` (replaces shell-out to `systemd-run`). Pure refactor under the trait.

Steps 1–2 are landable without committing to systemd at all; they're an internal cleanup that pays off across all three modes. Steps 3–5 are the systemd-mode delivery. Steps 6–7 are polish.

## What this is *not*

- **Not a change to the volume daemon's IPC, on-disk format, or self-containment property.** The coordinator's relationship to the volume is unchanged: it spawns it, it issues macaroons over IPC, it never sees a volume's I/O.
- **Not a multi-host story.** Per `docs/architecture.md`, coordinators on different hosts never talk to each other. This document scopes a *single-host* deployment, with three flavours.
- **Not a dependency on a specific systemd version** beyond what's standard on enterprise distributions (transient units have been stable since ~v229; `systemd-run --collect` since v236). The first cut targets a floor of v240+.
- **Not a replacement for `setsid` in modes 1/2.** Modes 1/2 keep `setsid` for volume daemons; that's load-bearing for "Ctrl-C the foreground coordinator and your VM keeps running."

## Open questions

- **`Type=notify` on the coordinator unit.** Worth doing eventually so systemd has visibility into "coordinator is up and ready." Not blocking for the first cut. Adds an `sd_notify` call on coordinator readiness.
- **Polkit / non-root coordinator.** `StartTransientUnit` against the system bus needs root or a polkit rule. Production coordinators run as root for ublk anyway. A polkit rule for the user-bus case (running coordinator under a regular user, no ublk) is a separate question we don't need to answer here.
- **Volume cgroup limits.** A nice property of mode 3 is per-volume `MemoryMax=`, `IOReadBandwidthMax=`, etc. Whether these become part of `volume.toml` (`[resource.memory_max] = "..."`) or stay operator-managed via `systemctl set-property` is open. Probably the latter for now — adding resource limits to `volume.toml` is its own design discussion.
- **macOS `coord start` daemonisation.** The current `coord start` likely does not double-fork properly on macOS (no `daemon(3)` portable equivalent we use). Need to verify; may need launchd integration as a sibling to systemd. Out of scope for this doc but worth flagging.
- **Mode-3 `volume.draining`.** Today the supervisor reads `volume.draining` and changes its `ExecStart` args (omits `--ublk`). Under systemd, we'd need to stop the existing transient unit, restart it with new properties, then transition out. The `ExecStart` re-write across a restart is straightforward; the question is whether the brief downtime during the property change is acceptable for the drain workflow. Probably yes — drain is itself a state transition operators expect to be controlled.
- **First-class `systemctl status` shape.** Operators will run `systemctl status elide-vol-<ulid>`. The default output should be informative — `Description=` should include the volume name, not just the ULID. Open question: do we look up the volume name at `StartTransientUnit` time and embed it in `Description=`, or keep the unit description ULID-only and require `elide volume list` for human-friendly names? Probably the former.
