# Coordinator-driven snapshot — plan

Status: proposed, not started. Tackle in a fresh branch/PR after
`fork-from-remote` lands.

## Motivation

Snapshots currently run in-process in the CLI: `elide volume snapshot`
opens the volume, calls `Volume::snapshot()`, which flushes WAL to
`pending/` and writes `snapshots/<ulid>`. Drain, upload, and promote
happen later, asynchronously, on the coordinator's per-volume tick
loop (`elide-coordinator/src/tasks.rs`). The CLI returns as soon as
the marker is on disk.

Two problems this creates:

1. **"Snapshot complete" doesn't mean "durable in S3".** A user who
   snapshots and then deletes locally can lose data: the snapshot
   returned success, but `pending/` hadn't drained to S3 yet.

2. **No seam for writing a snapshot completeness manifest.** The
   fork-from-remote follow-ups need a signed `.manifest` file per
   snapshot listing every `index/<ulid>.idx` that belongs to it, so
   `Volume::open` can verify ancestor prefetch offline. That manifest
   can only be written *after* every pre-snapshot segment has landed
   in `index/` — which today happens on the tick loop, outside the
   snapshot call. Writing `.manifest` in-process right now would
   either be empty or incomplete.

Both problems resolve by making snapshot a coordinator-orchestrated
sequence rather than a volume-process one-shot.

## Design

### Guiding observation

Most of the plumbing already exists. We extend it; we do not invent a
new transport.

- The CLI already speaks to the coordinator over `coordinator.sock`
  (`elide-coordinator/src/inbound.rs`). Adding a `snapshot` verb is
  the same shape as `import`/`delete`/`evict`.
- The coordinator already speaks to each running volume process over
  `<fork_dir>/control.sock` (`elide-coordinator/src/control.rs` →
  `src/control.rs`), and already drives actor commands `flush`,
  `sweep_pending`, `promote`. Adding a `sign-snapshot-manifest` command
  is one more variant on `VolumeRequest` in `elide-core/src/actor.rs`.
- The volume actor channel already serialises all mutating commands,
  so "flush then sign-manifest then write marker" cannot interleave
  with writes or with each other without any new locking inside the
  volume.

The only thing the coordinator side needs that doesn't exist today is
a way to stop its **own tick loop** from touching a volume while a
snapshot is in flight on that volume. That is the snapshot lock — and
nothing more.

### Division of responsibility

- **CLI** is a dumb client: sends `snapshot <name>` to the coordinator
  and waits.
- **Coordinator** owns the sequence: acquires the per-volume snapshot
  lock, runs drain+upload+promote inline, tells the volume to sign
  the manifest, uploads the manifest, releases the lock.
- **Volume process** owns anything that touches the private signing
  key: flushing WAL, promoting segments, signing the snapshot
  manifest, writing the marker file. The key never leaves the volume
  process.

Note: snapshot is driven by the coordinator, not by the volume. The
volume is a passive executor of actor commands throughout.

### Flow

1. CLI sends `snapshot <name>` to the coordinator.
2. Coordinator resolves the volume and acquires its per-volume
   snapshot lock.
3. Coordinator sends `flush` to the volume. Volume flushes WAL into
   `pending/`.
4. Coordinator drains inline: for each file in `pending/`, upload to
   S3, then send `promote <ulid>`. Reuses the existing drain/upload/
   promote code from the tick loop — just runs it inline instead of
   waiting for the next tick. Returns only once `pending/` is empty.
5. Coordinator picks `snap_ulid`: the max ULID currently in `index/`,
   or mints a fresh one if `index/` is empty.
6. Coordinator sends `sign-snapshot-manifest <snap_ulid>` to the
   volume. Volume:
   a. Enumerates `index/` — every `.idx` ULID currently present in
      this volume's own index directory.
   b. Sorts, signs, writes `snapshots/<snap_ulid>.manifest`. This
      is a *full* manifest for this volume up to `snap_ulid`, not a
      delta over the previous snapshot (see below).
   c. Writes the `snapshots/<snap_ulid>` marker.
   d. Returns ok.
7. Coordinator uploads both files to S3. The existing snapshot-upload
   walker already iterates `snapshots/`, so adding a file type is a
   one-liner.
8. Coordinator releases the snapshot lock and returns ok to the CLI.

If any step fails, the lock is released and the error propagates. The
marker is written last, so a partial sequence leaves no snapshot
visible — consistent with existing semantics.

### Why not a "snapshot" WAL record?

Considered and rejected. A WAL record is the right tool when recovery
needs to replay a decision after a crash — but a snapshot isn't a
mutation of volume state. It's a durability fence (`pending/` empty,
`index/` populated) plus a signed manifest. The fence is observable
from directory state; there's nothing for WAL replay to reconstruct.
Adding a WAL record type would introduce a new recovery invariant
("partial snapshot in WAL → resume from where?") for no gain, and
would put the snapshot logic inside the volume process instead of the
coordinator, which is the opposite of what we want.

If we ever want crash-during-snapshot to be recoverable by the volume
alone, without coordinator involvement, we can revisit this. Today
the coordinator is already the supervisor, so "coordinator retries
the sequence on restart" is the natural recovery model.

### Per-volume snapshot lock

A `Mutex<()>` per `fork_dir` held in the coordinator's state. The
snapshot handler takes it for the full sequence. The tick loop
(`tasks.rs:172`) `try_lock`s it before running drain/GC/eviction for
that volume and skips that volume for this tick if it can't.

The lock exists **only** to keep the coordinator's own background
ticks off a volume that's mid-snapshot. Volume-actor commands are
already serialised through the actor channel in
`elide-core/src/actor.rs`, so `flush`, `promote`, and
`sign-snapshot-manifest` cannot interleave with each other or with
live writes regardless of the lock. We do not need a second layer of
mutual exclusion inside the volume process.

This also answers "should GC pause during snapshot": yes, trivially,
because GC runs on the tick loop and the tick loop respects the lock.

### `.manifest` file format

Signed like `volume.provenance`: text body (one ULID per line,
sorted) + detached signature. Verified with the volume's own
`volume.pub`. Reuse whichever `signing::` helper `volume.provenance`
already uses.

### Full manifest, not delta

Each `snapshots/<S>.manifest` lists **every** ULID in this volume's
`index/` as of snapshot `S`. It is not a delta over the previous
snapshot within the same volume.

Rationale: a fork's open-time verification walks one manifest per
volume in the ancestry chain, not one per snapshot per volume. If
manifests were deltas, verifying a fork of a mid-history snapshot
would require chaining through every prior snapshot on every
ancestor volume. Full manifests collapse that two-dimensional walk
into a one-dimensional one: walk the fork chain, read exactly one
`.manifest` per ancestor (the one named by the child's provenance),
union the ULIDs, check each file exists.

Size cost is negligible: ~26 bytes per ULID line, so 10k segments ≈
260 KB signed. Trivial next to the `.idx` files themselves.

At snapshot time the volume already has everything locally — it
just enumerates its own `index/` directory. No prior-snapshot read,
no ancestor walk during signing.

### Trust root

Each ancestor's `.manifest` is verified with that ancestor's
`volume.pub`. The trust chain is **chained through provenance**:
each child's `volume.provenance` embeds the parent's public key and
is signed by the child's key. Trusting the current volume's public
key transitively trusts every ancestor in the chain.

This works because **keys never rotate**. A volume's keypair is
minted once at creation and used for the life of the volume. If a
key needs to be replaced (compromise, policy change, etc.), the
operation is "fork the volume" — the fork gets a fresh keypair,
pins the old volume as its parent, and from that point forward the
old volume is read-only ancestor data. No in-place rotation, no
keyring, no revocation list.

Consequence worth noting: `volume.provenance` becomes the
authoritative record of the parent's pubkey at fork time. If the
parent volume's `volume.pub` on disk somehow differs from what the
child's provenance says it was, the child's provenance wins — that
is the whole point of embedding it.

### `volume.provenance` format changes

Two changes to the existing format in
`elide-core/src/signing.rs`:

1. **Add `parent_pubkey` field.** The child embeds the parent's
   Ed25519 public key (64 hex chars) under the child's signature.
   The open-time ancestor walk uses this as the trust anchor for
   verifying the parent's `volume.provenance` and
   `snapshots/<P>.manifest`, not the `volume.pub` file sitting in
   the parent's directory on disk.

2. **Change `parent` syntax from `<vol_ulid>/snapshots/<snap_ulid>`
   to `<vol_ulid>/<snap_ulid>`.** The `/snapshots/` infix was
   originally there to mirror the on-disk directory layout, but
   that's no longer the cleanest framing — the parent pointer is a
   logical `(volume, snapshot)` tuple, not a filesystem path. Drop
   the infix. The two-ULID form is unambiguous and parses more
   cleanly.

Both are breaking format changes. Per the no-backward-compat-by-
default rule, existing `.provenance` files become invalid and the
affected volumes need to be re-forked (or, for readonly imports,
re-imported). Acceptable because this lands as part of the same
release sequence as the `.manifest` manifest and the
`Volume::open` fail-fast guard — all three are already a breaking
event for ancestor data.

### Open-time ancestor walk

Sketch, for reference (belongs in the `Volume::open` guard that's
the third change in the landing sequence, not this refactor — but
the manifest shape has to support it):

1. Read own `volume.provenance` → `(parent_vol, parent_snap)` if
   present.
2. Read own `snapshots/<S>.manifest`, verify with own `volume.pub`.
3. Assert every listed ULID exists in `index/`.
4. If provenance present, read
   `readonly/<parent_vol>/snapshots/<parent_snap>.manifest`, verify
   with `readonly/<parent_vol>/volume.pub`, assert each listed ULID
   exists in `readonly/<parent_vol>/index/`.
5. Follow the parent's own provenance and recurse.
6. Terminate at a volume with no provenance.
7. Build the LBA map as the union of all layers, lowest-ULID-wins.

Any missing manifest, failed signature, or missing `.idx` fails
`Volume::open` — no demand-fetch fallback for ancestors.

### Concrete delta

- **New coordinator inbound verb**: `snapshot <volume> <name>` in
  `elide-coordinator/src/inbound.rs`, handled by a new
  `snapshot_volume` function that runs the sequence above.
- **New volume actor command**: `VolumeRequest::SignSnapshotManifest
  { snap_ulid, reply }` in `elide-core/src/actor.rs`, with a matching
  `snapshot-manifest <ulid>` line in `src/control.rs`'s dispatch and
  a thin wrapper in `elide-coordinator/src/control.rs`.
- **New state**: `HashMap<PathBuf, Arc<Mutex<()>>>` (or equivalent)
  in the coordinator for per-volume snapshot locks, consulted by
  both the snapshot handler and the tick loop.
- **Reused as-is**: `flush`, `sweep_pending`, `promote` actor
  commands; the drain/upload/promote code from `tasks.rs` (lifted
  into a reusable helper so the snapshot handler and the tick loop
  can both call it); the snapshot upload walker.
- **Removed**: the in-process `Volume::snapshot()` path as the CLI
  entry point. The primitives it's built from stay; only the
  top-level orchestration moves out.

### Migration of existing callers

- `elide volume snapshot` CLI: switches from in-process
  `snapshot_volume()` to the new coordinator IPC.
- `create_fork`'s implicit snapshot: same. This means
  `volume fork --from <name>` now requires the coordinator to be
  running, which is already true for every other fork operation.
- Tests that currently call `Volume::snapshot()` directly with no
  coordinator: use decomposed primitives (`flush_wal_to_pending` +
  manual marker write) for tests that don't care about the
  manifest, or a test-only helper that runs a mini-drain loop for
  tests that do.

## Open questions

1. **Volume process not running.** Snapshot should start it via the
   supervisor and then snapshot, or error cleanly if the volume is
   marked stopped. Today snapshot opens in-process so this case
   doesn't arise.

2. **Timeout story.** Drain can take a long time on a big
   uncommitted-write batch. Probably: block indefinitely with
   progress logging and let the user Ctrl-C. Revisit if we grow a
   non-interactive caller.

3. **`.manifest` backfill for existing snapshots.** Pre-refactor
   snapshots won't have manifests. Per the
   no-backward-compat-by-default rule, they will fail to open as
   fork ancestors once the `Volume::open` verification check lands.
   Users migrate by taking a fresh snapshot on the source volume.
   Flag this prominently in the release notes.

## Out of scope

- Generic "wait for all drains to finish" primitive beyond what
  snapshot itself needs.
- Cross-volume parallelism tricks — the tick loop already runs
  volumes concurrently, and snapshot locks are per-volume.
- Replacing the tick-based drain loop with an event-driven model.
  The tick loop stays; snapshot just runs its own inline drain
  alongside it.

## Dependency chain

This refactor is the prerequisite for the `.manifest` manifest
(fork-from-remote follow-up #3), which is the prerequisite for the
`Volume::open` fail-fast guard (follow-up #4). Those three changes
should land as a single sequence once this refactor is in place.
