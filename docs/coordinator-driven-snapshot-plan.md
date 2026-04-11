# Coordinator-driven snapshot — plan

Status: proposed, not started. Tackle in a fresh branch/PR after
`fork-from-remote` lands.

## Motivation

Snapshots currently run in-process in the CLI: `elide volume snapshot`
opens the volume, calls `Volume::snapshot()`, which flushes WAL to
`pending/` and writes `snapshots/<ulid>`. Drain, upload, and promote
happen later, asynchronously, on the coordinator's tick loop. The CLI
returns as soon as the marker is on disk.

Two problems this creates:

1. **"Snapshot complete" doesn't mean "durable in S3".** A user who
   snapshots and then deletes locally can lose data: the snapshot
   returned success, but `pending/` hadn't drained to S3 yet.

2. **No way to write a snapshot completeness manifest.** The
   fork-from-remote work needs a signed `.segments` file per snapshot
   listing every `index/<ulid>.idx` that belongs to the snapshot, so
   `Volume::open` can verify ancestor prefetch completeness offline.
   That manifest can only be written *after* every pre-snapshot segment
   is in `index/` — which today happens asynchronously, outside the
   snapshot call. Writing `.segments` in-process right now would either
   be empty (index/ hasn't populated yet) or incomplete (drain is in
   progress).

Both problems resolve by making snapshot a coordinator-orchestrated
sequence rather than a volume-process one-shot.

## Design

### Division of responsibility

- **Coordinator** owns the sequence: acquiring locks, suspending GC,
  running drain+upload+promote synchronously, triggering manifest
  signing, uploading the manifest, releasing locks.
- **Volume process** owns anything that touches the private signing
  key: flushing WAL, promoting segments, signing the snapshot
  manifest, writing the marker file.
- **CLI** is a dumb IPC client: sends `snapshot <name>` to the
  coordinator and waits for the response.

### Flow

1. CLI sends `snapshot <name>` to coordinator.
2. Coordinator resolves the volume, acquires a per-volume snapshot
   lock (blocks concurrent GC/drain ticks for this volume).
3. Coordinator sends `flush-wal` IPC to the volume process. Volume
   flushes WAL to `pending/` and returns the set of ULIDs it wrote
   (for logging only — the drain step doesn't need this list).
4. Coordinator drains the volume synchronously: for each file in
   `pending/`, upload to S3, then send `promote <ulid>` IPC so the
   volume extracts `index/<ulid>.idx` and moves the body to `cache/`.
   Reuses existing drain/upload/promote code, just runs it inline
   instead of waiting for a tick. Returns only when `pending/` is
   empty.
5. Coordinator picks `snap_ulid`: the max ULID in `index/`, or mints
   a fresh one if `index/` is empty.
6. Coordinator sends `sign-snapshot-manifest <snap_ulid>` IPC to the
   volume process. Volume:
   a. Reads the prior snapshot's `.segments` file (if any) as the
      base list.
   b. Enumerates `index/` for ULIDs in `(prev_snap, snap_ulid]`.
   c. Sorts, deduplicates, signs, writes
      `snapshots/<snap_ulid>.segments`.
   d. Writes `snapshots/<snap_ulid>` marker.
   e. Returns ok.
7. Coordinator uploads both files to S3 (existing snapshot upload
   path already walks `snapshots/` so adding a file type is trivial).
8. Coordinator releases the snapshot lock.
9. Coordinator returns ok to CLI.

If any step fails, the lock is released and the error propagates. The
marker file is written last, so a partial sequence leaves no snapshot
visible — consistent with existing semantics.

### Per-volume snapshot lock

A `Mutex<()>` per `fork_dir` in the coordinator's state. The existing
drain and GC tick handlers `try_lock` this before running their per-
volume work; if locked, they skip that volume for this tick. The
snapshot handler takes the lock for its full duration.

This is enough for correctness because drain and GC are the only
writers to `pending/` and `index/` from the coordinator side. The
volume process itself still writes to `pending/` for live WAL
flushes, but `flush-wal` + `sign-snapshot-manifest` are serialised
through the volume's actor channel, so there's no intra-volume race
either.

### `.segments` file format

Signed like `volume.provenance`: text body (one ULID per line, sorted)
+ signature. Verified with the volume's own `volume.pub`. Reuse
`signing::write_signed` / `read_signed` or whatever helper pattern
volume.provenance uses.

### New IPCs

- **Coordinator inbound**: `snapshot <name>` — orchestrates the
  sequence above.
- **Volume actor**: `sign-snapshot-manifest <snap_ulid>` — signs and
  writes the manifest and marker. Volume's existing `promote <ulid>`
  and `flush-wal` (or whichever command triggers WAL flush —
  confirm) are reused unchanged.

### Migration of existing callers

- `elide volume snapshot` CLI: switches from in-process
  `snapshot_volume()` to the new coordinator IPC.
- `create_fork`'s implicit snapshot: same — goes through the
  coordinator IPC. This means `volume fork --from <name>` now
  requires the coordinator to be running, which is already true for
  every other fork operation.
- Tests that currently call `Volume::snapshot()` in-process with no
  coordinator: use decomposed primitives directly
  (`flush_wal_to_pending` + manual marker write for tests that don't
  care about the manifest, or a test-only helper that runs a
  mini-drain loop for tests that do).

## Open questions

1. **Should GC pause during snapshot?** For correctness, probably
   not needed: GC preserves source ULID, so the enumerated `index/`
   at manifest-write time captures the correct state regardless of
   GC order. But a GC running concurrently with the drain loop adds
   noise; simpler to just hold the snapshot lock over GC too and
   revisit if it causes throughput problems.

2. **What if the volume process isn't running?** Snapshot should
   probably start it (via the supervisor) and then snapshot. Or
   error cleanly if the volume is marked stopped. Today snapshot
   opens in-process so this case doesn't arise.

3. **Timeout story.** Drain can take a long time on a large
   uncommitted-write batch. Is the CLI expected to block
   indefinitely, or is there a timeout? Probably block
   indefinitely with progress logging; users can Ctrl-C.

4. **`.segments` backfill for existing snapshots.** Pre-refactor
   snapshots won't have manifests. Per the no-backward-compat-by-
   default rule, they fail to open as fork ancestors after the
   verification check lands. Users migrate by taking a new
   snapshot on the source volume (which the new code writes a
   manifest for), or by regenerating the volume. Flag this
   prominently in the release notes for the PR.

## Out of scope for this refactor

- Generic "wait for all drains to finish" primitive beyond what
  snapshot itself needs.
- Parallel snapshots across multiple volumes (one snapshot lock at
  a time per volume is fine; cross-volume parallelism already works
  through the tick loop).
- Replacing the tick-based drain loop with a fully event-driven
  model. The tick loop stays; snapshot just runs its own inline
  drain alongside it.

## Dependency on fork-from-remote follow-ups

This refactor is the prerequisite for the `.segments` file (follow-up
#3 in `fork-from-remote-plan.md`), which is the prerequisite for the
`Volume::open` fail-fast guard (follow-up #4). Those three changes
should land as a single sequence once this refactor is in place.
