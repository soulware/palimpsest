# Architecture

## Design principle: the volume is the primitive

A volume process is **self-contained and fully functional on its own** when all its data is present locally. Local storage (WAL + segments on NVMe) is a complete and correct deployment — not a degraded or temporary state. This must remain true as the system grows: nothing added to the coordinator should become a correctness dependency for the volume.

**Self-containment is a volume-level property, not a segment-level one.** A volume owns a set of segments plus an extent index that maps content hashes to their canonical locations. A segment containing DedupRef entries is not independently readable — resolving a DedupRef requires the extent index to locate the canonical Data extent, which may live in a different segment in the same volume or in an ancestor segment. The volume as a whole is self-contained because it owns everything the extent index can reach; individual segments are not.

**Caveat — demand-fetched volumes:** a volume that was started from a snapshot pulled from S3 (rather than built entirely from local writes) holds only the extents that have been accessed so far. Unaccessed extents still live in S3. Such a volume requires S3 reachability to serve reads for data it hasn't yet fetched; it is not fully self-sufficient until all referenced extents are local. This is intentional and expected — demand-fetch is a core feature, not a degraded state — but it means "self-contained" applies fully only to volumes that originated locally or have been fully warmed.

The coordinator and S3 are **strictly additive** for locally-originated volumes:
- Without coordinator: volumes run indefinitely on local storage; `pending/` accumulates but I/O is always correct
- With coordinator: GC reclaims space, S3 provides durability and capacity beyond local NVMe
- With coordinator + S3: full production deployment

This layering also means a single volume process can be started standalone for development, testing, or debugging with no service scaffolding required.

## Components

A single **Elide coordinator** runs on each host and manages all volumes. It forks one child process per volume — the process boundary is deliberate: a fault in one volume's I/O path cannot corrupt another, and the boundary forces the inter-component interface to be explicit and real (filesystem layout, IPC protocol, GC ownership) rather than loose in-process coupling.

**Coordinator (main process)** — spawns and supervises volume processes; owns all S3 mutations (upload, delete, segment GC rewrites); watches one or more configured volume root directories and discovers forks automatically; handles `prefetch-indexes` for forks cold-starting from S3.

**Volume process** (one per volume) — owns the ublk/NBD frontend for one volume; owns the WAL, `pending/` promotion, and `cache/` lifecycle for that volume; holds the live LBA map in memory; demand-fetches missing extents from S3 directly using read-only credentials. Does not communicate with other volume processes directly. Communicates with the coordinator via `control.sock` (Unix domain socket; see Control Socket Protocol below). Never requires the coordinator for correct I/O.

**S3 credential split:** the volume process requires only **read-only** S3 credentials (for demand-fetch). All S3 mutations — segment upload, segment delete, GC rewrites — are performed exclusively by the coordinator, which holds read-write credentials. This limits the blast radius if a volume host is compromised.

## Crate structure

The repository is a Cargo workspace with four crates:

```
elide-core/        — shared library: segment format, WAL, LBA map, extent index,
                     volume read/write, import_image(), and Ed25519 signing.
                     Deps: blake3, zstd, ulid, nix, ed25519-dalek, rand_core.
                     No async, no network. Usable standalone.

elide/             — volume process binary and user CLI: NBD server, analysis
                     tools (extents, inspect, ls), and volume management
                     subcommands including `volume import`. Adds:
                     clap, ext4-view, object_store, tokio (rt-multi-thread only).
                     The async runtime is used exclusively by the demand-fetch
                     path (ObjectStoreFetcher), which wraps block_on to satisfy
                     the sync SegmentFetcher interface. NBD I/O remains synchronous.

elide-import/      — OCI import binary: pulls public OCI images from a container
                     registry, extracts a rootfs, converts to ext4, and calls
                     elide_core::import::import_image to ingest. Adds: tokio,
                     oci-client, ocirender. Heavy async deps isolated here.

elide-coordinator/ — coordinator daemon: watches configured volume root
                     directories; discovers forks; supervises volume and
                     import processes; drains pending/ to S3; runs segment
                     GC; prefetches indexes for cold-start forks on
                     discovery. Adds: tokio, object_store (S3 and local
                     filesystem backends), nix (process supervision). Holds
                     read-write S3 credentials and OCI registry credentials;
                     volumes hold read-only S3 credentials only.
```

The split keeps the volume process binary lean and focused. The async HTTP stack
needed for OCI registry pulls belongs in tooling (`elide-import`), not in the
process that serves block I/O.

**Import model.** `elide volume import <name> <oci-ref>` is a user-facing CLI command that asks the coordinator to spawn `elide-import` as a supervised process. The coordinator creates the volume directory, writes an `import.lock` marker, spawns `elide-import`, and streams its output to attached clients. The import process runs in two phases: a **write phase** (segments written to `pending/`) followed by a **serve phase** (the import binds `control.sock` and handles `promote` IPC from the coordinator until `pending/` is empty). The coordinator removes `import.lock` when the process exits. The import produces a single readonly volume at `<data-dir>/<name>/` with no `wal/` directory. To get a writable copy, the user runs `elide volume fork <name> <new-name>` after the import completes. The import ULID returned by the coordinator is the handle for status polling and output streaming. `elide-import` remains a separate binary because of its heavy OCI/async dependencies; the `elide` CLI is the user-facing surface.

## Correctness foundations

Four mechanisms compose to guarantee that any read returns correct data, whether the segment body is present locally or must be demand-fetched from S3:

**Directory structure encodes lifecycle state.** `pending/`, `index/`, `cache/`, and `gc/` are not interchangeable storage tiers — they encode what is known about a segment at each point in its life. `pending/` means "written locally, not yet in S3". `index/<ulid>.idx` means "coordinator-confirmed in S3; LBA index permanently available". `cache/<ulid>.body` means "body bytes locally cached; safe to evict — S3 is authoritative". `gc/<ulid>.staged` means "coordinator compacted; volume has not yet applied". Bare `gc/<ulid>` means "volume-applied, volume-signed; coordinator uploading". The process controlling a volume directory is the sole writer of `index/` and `cache/`: the volume process for writable volumes, the import process for readonly volumes during its serve phase. In both cases the write is triggered by the coordinator's `promote <ulid>` IPC after confirmed S3 upload. `cache/<ulid>.{body,present}` is written alongside `index/<ulid>.idx`; the volume may also write `cache/` on demand-fetch. This makes the directory a machine-readable record of durability state, inspectable with standard tools without any binary decoding.

**ULIDs enforce total ordering.** Every segment has a ULID assigned at creation by the volume's own clock. ULIDs give a total order used in three places: (1) LBA map rebuild replays segments oldest-first, so the newest write for any LBA wins unambiguously; (2) fork ancestry walks stop at a ULID cutoff, preventing post-branch ancestor writes from leaking into derived volumes; (3) GC output ULIDs are derived as `max(inputs).increment()`, placing the output strictly after its inputs so concurrent writes always win in rebuild ordering. No external clock synchronisation is required — all ordering decisions are local to the volume's write history.

**Signatures enforce authorship integrity.** Only the process holding `volume.key` can produce valid segment signatures. The coordinator does not hold this key — it stages GC output at `gc/<ulid>.staged` with an ephemeral key; the volume re-signs before the coordinator uploads. Signature verification is enforced on every segment read from `pending/` and bare `gc/<ulid>` files. Together with the exclusive `volume.lock` flock, this makes the single-writer property cryptographically verifiable: not only is there at most one writer at a time, but any segment that exists in the directory was authored by that writer.

**The handoff protocol enforces GC correctness.** Coordinator GC compacts segments without taking the volume offline. The two-state self-describing handoff (`gc/<ulid>.staged` → bare `gc/<ulid>` → deleted via `finalize_gc_handoff`) ensures: no segment is deleted without the volume's explicit acknowledgment; no GC output is uploaded to S3 before the volume has re-signed it; and all crash/restart interleavings leave the system in a recoverable state. The new segment carries the sorted list of input ULIDs in its own header (`inputs_length` field), so the volume derives the apply set from the segment itself — no manifest sidecar to keep in sync. The safety invariants — no extent references a missing segment, no segment is removed while referenced — are verified across all interleavings by the TLA+ model in `specs/HandoffProtocol.tla` and the `gc_proptest` / `volume_proptest` suites.

**Together:** the directory structure ensures every `cache/` body is redundant with S3 (so eviction is always safe) and every `index/<ulid>.idx` guarantees the segment exists in S3, ULIDs ensure the LBA map is always reconstructable from whatever index and pending files are present, signatures ensure that what is reconstructed was written by the authorised writer, and the handoff protocol ensures GC transitions never break the first three properties. The result: any read can be served correctly from local storage, or — when bodies are absent from `cache/` — by demand-fetching from S3 with the guarantee that the index is accurate and the data is authentic.

## Directory layout

All state lives under a single root directory (`--data-dir`, default `./elide_data`). The structure mirrors the Linux `by-id` / `by-name` devicemapper convention:

- **`by_id/`** — canonical store; one ULID-named subdirectory per volume. The ULID is the volume's stable global identity, its S3 prefix, and the target of all `origin` ancestry links. The coordinator scans only `by_id/`.
- **`by_name/`** — pure symlink view; one symlink per volume pointing into `by_id/`. Maintained by the coordinator. Intended for human navigation and inspection.

```
elide_data/                           — single root (default --data-dir)
  control.sock                        — coordinator inbound socket (CLI connects here)
  coordinator.toml                    — coordinator configuration
  by_id/
    01JQAAAAAAA/                      — imported base (ULID = stable S3 prefix)
      volume.name                     — "ubuntu-22.04"
      volume.readonly                 — present = permanently readonly (imported/frozen)
      volume.size                     — volume size in bytes (plain text)
      volume.pub                      — Ed25519 public key (uploaded to S3)
      volume.provenance               — signed lineage (parent + extent_index); uploaded to S3
      manifest.toml                   — name, size, OCI source metadata
      pending/                        — segments awaiting S3 upload (volume-written)
      index/                          — volume-written LBA index files (01JQXXXXX.idx)
      cache/                          — volume-owned body cache (.body, .present); evictable
      snapshots/
        01JQXXXXX                     — branch point marker for derived volumes
      import.lock                     — present while import is running or interrupted
    01JQBBBBBBB/                      — writable volume forked from ubuntu-22.04
      volume.name                     — "server-1"
      volume.size
      volume.key                      — Ed25519 signing key (never uploaded; absent on readonly volumes)
      volume.pub
      volume.provenance               — signed: parent="01JQAAAAAAA/01JQXXXXX" + empty extent_index
      volume.pid                      — PID of running volume process
      wal/                            — present = live; write target
      pending/
      index/
      cache/
      snapshots/
      control.sock                    — volume process IPC socket (coordinator connects here)
    01JQCCCCCCC/
      volume.name                     — "server-2"
      volume.provenance               — signed: parent="01JQAAAAAAA/01JQXXXXX", extent_index empty
      ...
    01JQDDDDDDD/
      volume.name                     — "server-2-experiment"
      volume.provenance               — signed: parent="01JQCCCCCCC/<ulid>", extent_index empty
      ...
    01JQEEEEEEE/                      — readonly import layered on ubuntu-22.04
      volume.name                     — "ubuntu-22.04.1"
      volume.readonly
      volume.provenance               — signed: parent="", extent_index=["01JQAAAAAAA/01JQXXXXX"]
      ...
  by_name/
    ubuntu-22.04  ->  ../by_id/01JQAAAAAAA
    server-1      ->  ../by_id/01JQBBBBBBB
    server-2      ->  ../by_id/01JQCCCCCCC
    server-2-experiment  ->  ../by_id/01JQDDDDDDD
```

Lineage lives inside `volume.provenance`, **not** in standalone `volume.parent` / `volume.extent_index` files. Provenance is a single signed document recording both lineage relationships under one Ed25519 signature. Tampering with lineage is detectable with the volume's own public key, and the file format is extensible — adding new lineage fields in the future means extending the signed payload rather than dropping more unsigned files into the directory. `volume.provenance` is uploaded to S3 alongside `volume.pub` so that `remote pull` can materialise a signed, verifiable skeleton on another host.

Provenance file format:

```
parent: 01JQAAA…/01JQX…              # empty string if no fork parent
parent_pubkey: <64 hex chars>         # embedded parent verifying key (empty if no parent)
extent_index:
  01JQBBB…/01JQY…
  01JQCCC…/01JQZ…
sig: <hex-encoded 64-byte Ed25519 signature>
```

`parent:` is the fork ancestor — a read-path relationship. Fork children CoW against their parent at open time; `walk_ancestors(vol_dir, by_id_dir)` follows the chain from provenance `parent` fields to the root, and the result feeds **both** the LBA map rebuild and the extent index rebuild.

`extent_index:` is a flat list of snapshot references whose extents populate this volume's extent index **only**. Each entry names a snapshot whose hashes are available for dedup (and later delta compression source lookups) but whose data is **never** merged into the LBA map. The child is born with an empty LBA map and writes its own data; source segments are only fetched when a child write references a source hash via `DedupRef`. This means the `extent_index` field provides no read-path fall-through — unused source data is never visible from the child. `walk_extent_ancestors(vol_dir, by_id_dir)` reads the `extent_index` entries from provenance, dedupes by source directory, and returns the list in parallel with `walk_ancestors`. Only the fork chain feeds the LBA map rebuild.

The `extent_index` list is **flat because it is computed at import time, not resolved at attach time**. When a new volume is imported with `--extents-from X`, the coordinator reads `X`'s own provenance (verifying its signature), inherits every entry from `X`'s `extent_index` field, appends `X` itself at its latest snapshot, dedupes by source ULID, and signs the result into the new volume's provenance. Multiple `--extents-from` values contribute their already-flat lists in order. The total is capped at `MAX_EXTENT_INDEX_SOURCES = 32` to bound attach-time cost. When the expanded list exceeds the cap: **explicit sources** (passed directly via `--extents-from`) are sacred and kept in full — if the explicit count alone exceeds the cap, the import is rejected; **inherited entries** fill the remaining slots via "oldest + most recent" pruning, keeping the first-added entry (the base, likely the largest reusable pool) plus as many recently-added entries as fit. Middle inherited entries are dropped with a warning.

Walker integrity: `walk_ancestors` and `walk_extent_ancestors` both verify the signature of each volume's provenance before reading its lineage fields, using the volume's own `volume.pub`. The Ed25519 signature anchors lineage integrity against tampering.

**S3 path:** `by_id/<volume-ulid>/YYYYMMDD/<segment-ulid>` — the volume ULID is both the `by_id/` directory name and the S3 prefix. A volume moved to another host or renamed locally keeps the same S3 path. Additional per-volume S3 objects: `by_id/<volume-ulid>/manifest.toml`, `by_id/<volume-ulid>/volume.pub`, and `by_id/<volume-ulid>/volume.provenance`. Volume names are indexed at `names/<name>` (plain text ULID), enabling O(1) lookup and a single `LIST names/` to enumerate all named volumes.

**Name resolution:** the CLI accepts human-readable names in all commands. `by_name/<name>` is a symlink → O(1) resolution via `readlink`. Names must be unique within a `data_dir` — the CLI refuses to create a volume whose name would duplicate an existing `by_name/` entry. The uniqueness constraint is local only; different hosts sharing the same S3 bucket may assign different names to the same ULID.

**`by_name/` maintenance:** the coordinator creates the symlink when a volume is discovered, updates it on rename, and removes it on delete. On startup the coordinator reconciles `by_name/` against `by_id/`: removes stale symlinks (target ULID no longer exists), adds missing symlinks (volume in `by_id/` with no corresponding `by_name/` entry).

**Invariants:**
- `by_id/` entries are valid ULIDs — the coordinator skips anything else
- `by_name/` entries are symlinks only — no real directories
- `volume.name` is present in every volume; single non-empty line
- `volume.readonly` present → volume is permanently readonly; coordinator skips supervision; volume process refuses writable open
- `volume.pub` present in every volume — readonly volumes have only `volume.pub` (no `volume.key`); `serve-volume` verifies provenance against it on every open, writable or not
- `volume.key` present only on writable volumes; absent on readonly/imported volumes — `serve-volume` fails hard if it is missing and the volume is not readonly
- `volume.provenance` present in every volume — signed lineage (`parent`, `extent_index`) + Ed25519 signature; signed by the volume's private key at creation/import time and verified by `serve-volume` using `volume.pub` on every open. Uploaded to S3 so `remote pull` can rehydrate the lineage chain. Lineage is never stored outside provenance; there are no standalone `volume.parent` / `volume.extent_index` files.
- `wal/` present → volume is live (writable); exactly one process writes here (enforced by `volume.lock`)
- `parent` field set → volume is a fork; value is `<parent-ulid>/<snapshot-ulid>`; parent is merged into both the LBA map and the extent index
- `extent_index` field non-empty → volume lists a flat union of source snapshots; one `<source-ulid>/<snapshot-ulid>` per entry; each source is merged into the extent index only, **never** into the LBA map (no read-path fall-through, no data leak); bounded at `MAX_EXTENT_INDEX_SOURCES` entries
- `snapshots/<ulid>` is a plain marker file; ULID sorts after all segments present at snapshot time
- `manifest.toml` present on OCI-imported volumes and on volumes reconstructed via `remote pull`
- `import.lock` present while an import is in progress (write phase) or in serve phase (handling promote IPC) or was interrupted
- **`index/<ulid>.idx` present** means the volume has flushed segment `<ulid>` to `pending/` (or applied a GC handoff producing it). The volume writes `index/<ulid>.idx` at two points: (1) when flushing the WAL to `pending/<ulid>`; (2) when applying a GC handoff — writing `index/<new>.idx` from `gc/<new>` and deleting `index/<old>.idx` for each consumed input. `index/` is never written by the coordinator. `index/<ulid>.idx` files are never evicted — they are the permanent LBA index for all segments the volume has ever created or compacted.
- **`pending/<ulid>` absent ↔ segment `<ulid>` is confirmed in S3.** The volume deletes `pending/<ulid>` as part of responding to the coordinator's `promote` IPC, which the coordinator issues only after a confirmed S3 upload. If `pending/<ulid>` exists, the segment has not yet been confirmed in S3. `cache/<ulid>.body` and `cache/<ulid>.present` are volume-owned: written by the volume on `promote` response and on demand-fetch; may be evicted by the volume at any time; their absence means body bytes must be fetched from S3.

**Finding live volumes:** the coordinator scans `by_id/` for ULID-named subdirectories that have a `pending/` subdirectory (segments awaiting upload) or an `index/` subdirectory (already-uploaded segments). Readonly volumes (with `volume.readonly`) are included for drain if they have pending segments (import just completed), and for prefetch if `index/` is absent or empty (just pulled from the store, no segments indexed yet). Readonly volumes with a non-empty `index/` are already indexed and are skipped.

**Exclusive access:** a live volume holds an exclusive `flock` on `<vol-dir>/volume.lock` for the lifetime of its volume process. Attempting to open an already-locked volume fails immediately.

**Import lock:** `<vol-dir>/import.lock` (plain text, one line: the import job ULID) is present for the full lifetime of the import process: both the write phase (segments being written to `pending/`) and the serve phase (import handling `promote` IPC). The coordinator removes it when the process exits. A `control.sock` alongside `import.lock` signals that the import is in the serve phase and the coordinator may send IPC. Cleaned up on the next coordinator startup if stale. See *Import process lifecycle* below.

**Stopped marker:** `<vol-dir>/volume.stopped` is written by the coordinator when a volume is explicitly stopped via `volume stop` or `coordinator quiesce`. While present, the supervisor will not start or restart the volume process. Removed by `volume start`. Persists across coordinator restarts.

**Volume process state** (readable from the filesystem without running processes):

| Marker files present | State |
|---|---|
| `volume.pid` alive | running — volume process is serving I/O |
| `volume.stopped` | explicitly stopped — coordinator will not restart |
| `import.lock` (no `control.sock`) | import write phase or interrupted |
| `import.lock` + `control.sock` | import serve phase — coordinator may send IPC |
| `volume.readonly` | readonly — coordinator never supervises |
| none of the above | idle — coordinator will start the volume process |

**Volume ancestry:** a volume's `volume.parent` file names its parent ULID and the branch-point snapshot ULID. `walk_ancestors(vol_dir, by_id_dir)` follows this chain to the root (a volume with no `volume.parent` file), building an oldest-first list of ancestor layers. Segments in each ancestor are included only up to the branch-point ULID — post-branch writes to an ancestor are not visible in derived volumes.

```
VM
 │  block I/O (ublk / NBD)
 ▼
Volume process  (one per volume)                        [elide_data/by_id/<ulid>/]
 │  write path: buffer → extent boundary → hash → local dedup check → WAL append
 │  read path:  LBA → LBA map → extent index → segment body (pending/ · bare gc/<ulid> · cache/ · S3)
 │  owns: wal/, pending/, index/, cache/   shared: gc/
 │
 ├─ WAL  (wal/<ULID>)
 ├─ Pending segments  (pending/<ULID>)
 ├─ LBA index  (index/<ULID>.idx — written at flush + via promote_segment after GC)
 ├─ Body cache  (cache/<ULID>.{body,present})
 ├─ Live LBA map  (in memory, LBA → hash; merged from own + ancestor layers)
 └─ IPC  (control.sock — optional for I/O, used for coordination)
      │   flush · sweep_pending · repack · gc_checkpoint · promote <ulid>
      ▼
Coordinator (main process)                              [elide_data/]
 ├─ Volume supervisor    (spawn/re-adopt volume processes)
 ├─ Volume watcher       (scans by_id/; discovers new volumes)
 ├─ S3 uploader          (drains pending/ → S3 → promote IPC; volume deletes pending/)
 ├─ Segment GC           (reads index/ for LBA map; writes gc/ handoffs; promote IPC after upload)
 └─ prefetch-indexes     (downloads .idx files for cold-start forks)
```

## Coordinator/volume split: who writes what

The directory structure enforces a clean ownership boundary between the volume process and the coordinator.

### Volume process owns
- `wal/<ulid>` — active WAL file (WAL append path)
- `pending/<ulid>` — segment flushed from WAL; awaiting coordinator upload
- `index/<ulid>.idx` — LBA index (header + index section); written at WAL flush and via `promote_segment` after a GC handoff
- `cache/<ulid>.body` — body bytes; written on `promote` (after coordinator confirms S3 upload) and on demand-fetch
- `cache/<ulid>.present` — bitset of locally-present extents; written alongside `.body`
- `gc/<ulid>` (bare) — volume-applied GC output; re-signed copy of the staged body, awaiting coordinator upload

### Coordinator owns
- `gc/<ulid>.staged` — coordinator-staged GC output (ephemeral-signed); inputs ULIDs live in the segment header
- `elide_data/control.sock` — coordinator inbound socket
- `by_name/<name>` — symlinks into `by_id/`

### Coordinator reads
- `pending/<ulid>` — to upload to S3; volume deletes it as part of responding to `promote`
- `gc/<ulid>` (bare) — trigger for S3 upload; coordinator deletes via `finalize_gc_handoff` IPC after the upload + promote sequence completes

### Coordinator deletes
- (none directly under the volume tree) — every deletion is routed through the volume actor via IPC, so `gc/`, `index/`, and `pending/` mutations always serialise with the actor's idle-tick passes.

### Volume deletes
- `pending/<ulid>` — as part of responding to `promote` (after coordinator confirms S3 upload)
- `cache/<old>.body` + `cache/<old>.present` — when applying a GC handoff (input ULIDs read from the new segment's header) and on local eviction under disk pressure
- `gc/<ulid>` (bare) — on `finalize_gc_handoff` IPC after the coordinator confirms S3 upload + promote + S3 deletes

The coordinator never writes to `wal/`, `pending/`, `index/`, or `cache/`. The volume never writes to `gc/<ulid>.staged` or `by_name/`.

### Drain/upload flow (pending/ → S3 → cache/)

The volume writes `index/<ulid>.idx` at WAL flush time, so it already exists when the coordinator begins drain. The coordinator's job is to upload the body and notify the volume:

```
1. Upload pending/<ulid> to S3
2. IPC → volume: "promote <ulid>"
   Volume: copy body section  pending/<ulid> → cache/<ulid>.body
           write all-present  bitset         → cache/<ulid>.present
           delete pending/<ulid>
```

If the volume is not running, the coordinator leaves `pending/<ulid>` in place and retries on the next drain tick. The upload itself (step 1) proceeds regardless — it is idempotent — but promote is deferred until the volume is reachable. `pending/<ulid>` will accumulate temporarily; this is pure storage overhead, not a correctness issue, since `index/<ulid>.idx` is the permanent record.

If the coordinator crashes after step 1 but before step 3, the next drain pass re-uploads to S3 (idempotent PUT) and re-sends `promote` (idempotent — writing the same body to `cache/` a second time is harmless), then deletes `pending/<ulid>`.

The `index/<ulid>.idx` format is a complete segment header plus the index section (LBA → body offset mappings). The body section is stored separately in `cache/<ulid>.body`. This split keeps the LBA index small and permanently available for fast startup (no segment body download needed), while the body is independently evictable.

### GC cleanup ordering

Under the self-describing GC handoff protocol (see
`docs/design-gc-self-describing-handoff.md`) the new segment carries
its input ULID list in the segment header, so the volume never needs
to consult a manifest sidecar.

```
Volume (apply_gc_handoffs, when it sees gc/<new>.staged):
  1. Read gc/<new>.staged, walk each input's index/<input>.idx
  2. Update extent index in memory: repacks → new ulid; LBA-dead
     entries → remove
  3. Write a re-signed copy to gc/<new>.tmp
  4. Rename gc/<new>.tmp → gc/<new>          ← ATOMIC COMMIT POINT
  5. Remove gc/<new>.staged
  6. Delete cache/<input>.{body,present} for each input

Coordinator (apply_done_handoffs, seeing bare gc/<new>):
  7. Upload gc/<new> to S3
  8. IPC → volume promote_segment(<new>):
       - Write index/<new>.idx (from the staged body)
       - Write cache/<new>.body + cache/<new>.present
       - Delete index/<input>.idx for each input (read from header)
  9. Coordinator: delete old S3 objects
 10. IPC → volume finalize_gc_handoff(<new>):
       - Delete bare gc/<new>
```

**S3 confirmation signal:** `pending/<ulid>` absent (coordinator deleted it after upload) means the segment is in S3. `index/<ulid>.idx` is now always present from flush time onward, so it is no longer the S3 confirmation signal — it is simply the permanent LBA index.

**The old `index/<old>.idx`-before-S3-deletion ordering invariant is automatically satisfied:** `promote_segment` (step 8) deletes `index/<old>.idx` for each input as part of writing the new idx, which happens entirely before the coordinator begins the S3 deletion sequence at step 9. No explicit coordinator ordering is needed.

**Cache reads during the GC window:** between step 6 (cache/<old>.* deleted) and step 9 (old S3 objects deleted), a read for a consumed segment's LBA will miss `cache/` and demand-fetch from S3 — the old S3 objects are still present at this point, so the fetch succeeds. Critically, between step 4 and step 6 the extent index already points at the new segment, and the bare `gc/<new>` body is resolvable via `locate_segment_body`, so reads succeed without a fetch.

**Tombstone handoffs.** When the coordinator compacts a set of fully-dead segments, it still produces a real GC output via `write_gc_segment` — but with zero entries and a non-empty `inputs` list. `promote_segment` recognises zero-entry GC outputs and skips writing `index/<new>.idx` and `cache/<new>.body` entirely, only deleting the input idx files. The bare `gc/<new>` body is then deleted at step 10 the same way as a live output. This unifies the tombstone, removal-only, and live-compaction paths into one filename lifecycle.

## Coordinator lifecycle and shutdown behaviour

Volume processes are **detached** from the coordinator at spawn time (`setsid` / new session) so they are not in the coordinator's process group and are not automatically signalled when it exits. The coordinator runs in two modes with different shutdown semantics:

### Foreground mode (`elide-coordinator serve`, default)

Intended for development and testing. The coordinator and its volumes are treated as one logical unit.

On **Ctrl-C or SIGTERM**, the coordinator:
1. Sends SIGTERM to every supervised volume process (via `volume.pid`)
2. Sends SIGTERM to every running import process (via `import.pid`)
3. Waits briefly for all processes to exit
4. Exits itself

This gives a clean teardown: no orphaned NBD devices, no stale pid files, no lock files left behind. The user gets the behaviour they expect from a foreground process — stopping the coordinator stops everything.

### Daemon mode (`elide-coordinator serve --daemon`, planned)

Intended for production deployments managed by a service supervisor (systemd, launchd, etc.). The coordinator is a restartable supervisor; volumes are independent long-running services.

On **SIGTERM**, the coordinator exits immediately **without signalling volume or import processes**. Running volumes continue serving. The `setsid` at spawn time ensures they are not in the coordinator's session and receive no automatic signal.

On next coordinator start, running volumes are **re-adopted**: the coordinator scans for `volume.pid` files, confirms the processes are alive, and resumes supervision (drain, GC, inbound socket) without restarting them.

### Re-adoption on coordinator start

Regardless of mode, on startup the coordinator:
1. Calls `cleanup_stale_locks()` to remove any `import.lock` files left by a previous run (killing surviving import processes if found)
2. Scans all fork directories; for each `volume.pid` present: if the process is alive, re-adopt it; if dead, remove the stale pid file and start a fresh volume process

Re-adoption means the supervisor task polls the existing process until it exits naturally, then restarts it as normal. The volume's WAL and segments are untouched.

### Proposed: Volume stop/start and coordinator quiesce

These operations give explicit control over individual volumes or all volumes while the coordinator keeps running. They are the right tool for planned maintenance, controlled shutdown of a single VM, or draining a host before an upgrade.

**`volume stop <name>`** — stop a single volume:
1. Send SIGTERM to the volume process (via `volume.pid`)
2. Write `<vol-dir>/volume.stopped`
3. Supervisor sees the marker and does not restart

**`volume start <name>`** — start a previously stopped volume:
1. Remove `<vol-dir>/volume.stopped`
2. Supervisor picks it up on the next scan and starts the process normally

**`coordinator quiesce`** — stop all running volumes:
1. For each supervised fork: send SIGTERM, write `volume.stopped`
2. Coordinator keeps running; drain, GC, and inbound socket remain active
3. No volumes will be restarted until explicitly started again

**`coordinator resume`** — start all stopped volumes:
1. For each fork with `volume.stopped`: remove the marker
2. Supervisor picks them up on the next scan

The `volume.stopped` marker persists across coordinator restarts. A quiesced host stays quiesced even if the coordinator is restarted — this is intentional. Resumption is always an explicit act.

**Foreground Ctrl-C** does not write `volume.stopped`. There is no need — the coordinator is exiting anyway. On next coordinator start, volumes will be started normally (or re-adopted if still running).

**Relationship to `volume delete`:** `delete` does not set `volume.stopped` — it removes the directory entirely. `stop` is the right operation when you want the volume to remain but not serve I/O.

### IPC is optional

The coordinator skips compaction and GC steps gracefully if `control.sock` is absent (volume not running). Loss of the channel degrades background efficiency but never affects correctness or I/O availability — the volume never blocks on the coordinator.

## Control Socket Protocol

The volume process listens on `<vol-dir>/control.sock`. The coordinator connects, sends one request line, reads one response line, and closes the connection. Protocol is newline-delimited plain text.

**Request format:** `<op> [args...]\n`

**Response format:**
- `ok [values...]\n` — success, with optional return values
- `err <message>\n` — error; coordinator logs a warning and proceeds

**Supported operations:**

| Operation | Request | Response |
|-----------|---------|----------|
| Flush WAL | `flush` | `ok` |
| Sweep small pending segments | `sweep_pending` | `ok <segs> <new_segs> <bytes> <extents>` |
| Repack sparse pending segments | `repack <min_live_ratio>` | `ok <segs> <new_segs> <bytes> <extents>` |
| GC checkpoint | `gc_checkpoint` | `ok <repack_ulid> <sweep_ulid>` |
| Promote segment to cache | `promote <ulid>` | `ok` |

**`gc_checkpoint` detail:** flushes the WAL (so all in-flight writes are in `pending/` and visible to the coordinator), then mints two ULIDs 2ms apart using the volume's own clock, and returns them as `repack_ulid` and `sweep_ulid`. The coordinator uses these as the output segment ULIDs for its repack and sweep GC passes respectively. Using ULIDs from the volume's clock (not the coordinator's) is deliberate — it ensures the GC output ULIDs are always in the correct order relative to the volume's write history regardless of clock skew between hosts.

**`promote <ulid>` detail:** called by the coordinator after confirming a segment has been uploaded to S3 (drain path) or after uploading a GC output. The volume copies the body section from `pending/<ulid>` (drain path) or `gc/<ulid>` (GC path) into `cache/<ulid>.body` and writes an all-present bitset to `cache/<ulid>.present`. On the drain path the volume also deletes `pending/<ulid>` — the coordinator never deletes it. On the GC path the coordinator deletes `gc/<ulid>` after receiving `ok` (see GC cleanup ordering below). If the volume is not running, the coordinator defers promote: on the drain path it leaves `pending/<ulid>` in place and retries on the next tick; on the GC path it proceeds without populating the local cache.

**Compaction stats fields** (`sweep_pending` / `repack` response): `segs` = segments consumed, `new_segs` = segments produced, `bytes` = bytes freed, `extents` = extents removed.

The socket is absent when the volume is not running. All coordinator IPC calls treat a missing socket as a silent no-op and proceed with the remaining drain/GC steps that do not require volume cooperation.

## User-facing CLI surface

The `elide` binary serves a dual role: it is both the **user-facing CLI** and the **volume process binary** that the coordinator spawns. The `elide-coordinator` binary is the coordinator daemon only — it has no user-facing subcommands.

### `elide` CLI commands

Most commands operate directly on the volume directory and require no running coordinator. A small set need the coordinator for process or device management.

All user-facing commands accept a **volume name** (resolved via `by_name/<name>` symlink → O(1)) or a **volume ULID** (looked up directly in `by_id/`). Names must be unique within a `data_dir`; commands that create volumes refuse if a `by_name/` entry for that name already exists.

**Filesystem-direct (no coordinator required):**

| Command | What it does |
|---|---|
| `elide volume list` | Scan `data_dir` for ULID dirs; show name, ULID, state |
| `elide volume info <name\|ulid>` | Segment counts, WAL size, snapshot history, ancestry chain |
| `elide volume ls <name\|ulid> [path]` | Browse ext4 filesystem contents |
| `elide volume snapshot <name\|ulid>` | Write a snapshot marker file |
| `elide volume fork <src> <new-name>` | Create a new volume branched from latest snapshot of `<src>`; refuses if `<new-name>` already exists |
| `elide volume create <name> [--size N]` | Create a new empty volume (generates ULID dir, writes `volume.name`); rescan |
| `elide volume remote list` | `LIST names/` against the store; print all named volumes with ULID and size |
| `elide volume remote pull <name>` | Resolve name → ULID via `names/<name>`, download manifest, reconstruct local skeleton, trigger coordinator rescan; prefetch of segment indexes happens automatically on next coordinator tick |

`create` and `fork` generate a fresh ULID for the new volume directory. Both send a lightweight `rescan` to the coordinator after writing to disk. If the coordinator is not running, the rescan fails with a warning and the volume is discovered on the next startup or scan.

**Coordinator-required (process and device management):**

| Command | What it does |
|---|---|
| `elide volume status <name\|ulid>` | Is the volume process running? |
| `elide volume stop <name\|ulid>` | Stop the volume process and set `volume.stopped` |
| `elide volume start <name\|ulid>` | Clear `volume.stopped`; coordinator restarts the volume |
| `elide volume delete <name\|ulid>` | Stop all processes, then remove the volume directory |
| `elide volume import <name> <oci-ref>` | Ask coordinator to spawn an import; prints import job ULID |
| `elide volume import status <job-ulid>` | Poll import state: running / done / failed |
| `elide volume import attach <job-ulid>` | Stream import output until completion |
| `elide coordinator quiesce` | Stop all volumes (set `volume.stopped`); coordinator keeps running |
| `elide coordinator resume` | Clear `volume.stopped` on all volumes; coordinator restarts them |

**Internal (spawned by coordinator; not intended for direct use):**

```
elide serve-volume <vol-dir> [--bind addr] [--port N] [--readonly]
```

### `elide-coordinator` CLI commands

```
elide-coordinator serve [--config coordinator.toml]   # foreground mode: Ctrl-C terminates coordinator + all volumes
elide-coordinator serve --daemon                       # daemon mode: coordinator restartable independently of volumes
```

The `--daemon` flag changes shutdown semantics (see *Coordinator lifecycle and shutdown behaviour*); it does not currently detach the process. Process detachment (double-fork or systemd `Type=notify`) is deferred — for now, daemon mode is started via a service manager that handles detachment.

### Finding the coordinator socket

Both the coordinator and volume processes use `control.sock`; the directory encodes which process you're talking to:

```
elide_data/
  control.sock                         ← coordinator inbound (CLI talks here)
  by_id/<volume-ulid>/
    control.sock                       ← volume process control (coordinator talks here)
```

The `elide` CLI derives the coordinator socket path from `--data-dir`:

1. `--data-dir <path>` flag → `<path>/control.sock`
2. `ELIDE_DATA_DIR` environment variable → `<value>/control.sock`
3. `./elide_data/control.sock` (default)

## Coordinator inbound socket

The coordinator listens on `<data-dir>/control.sock` for commands from the `elide` CLI. Volume processes each listen on `<vol-dir>/control.sock` for commands from the coordinator. Same socket filename, different directory level — the path encodes what you're talking to.

Same text line protocol as the volume control socket: `<op> [args...]\n` → `ok [values...]\n` / `err <message>\n`.

**Implemented operations:**

| Operation | Request | Response |
|---|---|---|
| Trigger immediate fork discovery | `rescan` | `ok` |
| Query volume process state | `status <volume>` | `ok running` / `ok stopped` / `ok importing <ulid>` |
| Start OCI import | `import <name> <oci-ref>` | `ok <ulid>` |
| Poll import state | `import status <ulid>` | `ok running` / `ok done` / `err failed: <msg>` |
| Stream import output | `import attach <ulid>` | lines… then `ok done` or `err failed: <msg>` |
| Stop processes and remove volume | `delete <volume>` | `ok` |

`import` and `import status` follow the standard one-request / one-response model. `import attach` is the exception: the coordinator streams buffered and live output lines until the import completes, then sends a terminal `ok done\n` or `err failed: <message>\n` and closes the connection. If the import has already finished, the stored output is replayed immediately followed by the terminal line.

`rescan` is the lightweight hook used by `create` and `fork`. The coordinator runs a discovery pass immediately and starts supervising any new volumes found.

**Planned — volume stop/start and quiesce** (see *Volume stop/start and coordinator quiesce* above):

| Operation | Request | Response |
|---|---|---|
| Stop a single volume | `stop <volume>` | `ok` |
| Start a stopped volume | `start <volume>` | `ok` |
| Stop all volumes | `quiesce` | `ok` |
| Start all stopped volumes | `resume` | `ok` |

**Planned — S3 credential distribution** (see *S3 credential distribution via macaroons* below):

| Operation | Request | Response |
|---|---|---|
| Register volume process, receive macaroon | `register <volume>` | `ok <macaroon>` |
| Exchange macaroon for S3 credentials | `credentials <macaroon>` | `ok <key> <secret> <session-token> <expiry-unix>` |

The core isolation goal: **a compromised volume process must not be able to affect another volume's S3 data**. See *Isolation model* below for what this does and does not enforce.

## Proposed: Operator tokens

Operator tokens are macaroons minted by the coordinator for human operators (CLI usage). They are not PID-bound — identity is carried by the token itself, enabling audit logging and attenuation.

**Issuance:**

```
elide-coordinator token create [--expires 24h] [--volume <name>]
```

Prints a macaroon to stdout. The operator stores it in `~/.elide/operator-token` or passes it explicitly. The coordinator logs token creation with a unique nonce.

**Caveats on an operator token:**

| Caveat | Value | Purpose |
|---|---|---|
| `role` | `operator` | Distinguishes from volume tokens |
| `not-after` | `<expiry>` | Required; no indefinite operator tokens |
| `volume` | `<name>` | Optional; restrict to a specific volume |

**How the CLI uses it:**

The `elide` CLI locates the operator token in this order:
1. `--token <value>` flag
2. `ELIDE_OPERATOR_TOKEN` environment variable
3. `~/.elide/operator-token` file

It is presented with any coordinator mutation that requires one (currently: `delete`).

**Attenuation:** an operator can narrow their token before sharing it — for example, scoping it to a single volume or shortening the expiry — without involving the coordinator. The coordinator verifies all caveats on receipt.

**Audit log:** the coordinator logs every operator-token-authenticated operation with the token's nonce, the operation, and the timestamp. This provides a trail of who did what and when, traceable back to the `token create` event.

## Proposed: Isolation model

Volume processes on the same host share a uid and a filesystem. This has direct consequences for what the macaroon scheme can and cannot enforce.

**What macaroons do not enforce — local filesystem:** a compromised volume process can read or corrupt any other volume's local directory directly, without touching the coordinator. Macaroons provide no protection here. Proper local isolation requires OS-level mechanisms: separate uids per volume, Linux user namespaces, or running each volume in its own container. This is a separate layer and is not addressed by the current design.

**What macaroons do enforce — S3:** S3 credentials are scoped by IAM to a specific volume's prefix. This enforcement is external to Elide — AWS (or equivalent) rejects requests that exceed the credential's scope regardless of what the caller claims. The macaroon scheme ensures a volume process can only obtain credentials for its own volume. A compromised `myvm` process cannot request credentials for `othervm`, so it cannot read, write, or delete `othervm`'s S3 objects even with full local filesystem access.

**What macaroons provide for coordinator operations — defense-in-depth:** requiring a volume-scoped macaroon for coordinator mutations (e.g. `delete`) raises the bar slightly over bare socket access, and provides an audit trail. It does not prevent a compromised process from achieving the same effect via direct filesystem manipulation. The value here is auditability and protocol clarity, not a hard security boundary.

**Summary:**

| Resource | Isolation mechanism | Enforced by |
|---|---|---|
| S3 data | IAM credential scoping + macaroon gating | AWS + coordinator |
| Local filesystem | uid separation / namespacing | OS (not yet implemented) |
| Coordinator mutations | Macaroon + audit log | Coordinator (defense-in-depth) |

## Proposed: S3 credential distribution via macaroons

The coordinator holds the only copy of read-write S3 credentials. Volume processes receive **short-lived read-only credentials** for demand-fetch only, authenticated via macaroons.

### Macaroon model

The coordinator holds a **root key** (random bytes, generated at first start, stored alongside `coordinator.toml`). It uses this to mint per-fork macaroons — HMAC-SHA256 bearer tokens with a chain of typed caveats. Verification is stateless: the coordinator re-derives the expected HMAC from the root key and the caveat chain; no token storage is needed.

**Caveats on a volume macaroon:**

| Caveat | Value | Purpose |
|---|---|---|
| `volume` | `<volume-name>` | Binds token to this volume only |
| `scope` | `credentials` | Limits token to credential requests only |
| `pid` | `<process-pid>` | Locks token to the spawned process (see below) |

### Registration flow (how a volume gets its macaroon)

The PID is only known after the volume process is spawned, so the macaroon cannot be minted before spawn. Instead, the volume registers with the coordinator on startup:

1. Coordinator spawns volume process, records PID in `volume.pid`
2. Volume connects to `control.sock` and sends `register <volume> <fork>`
3. Coordinator reads `SO_PEERCRED` from the Unix socket connection → obtains peer PID
4. Coordinator cross-checks: is this PID the one recorded in `volume.pid` for `<volume>/<fork>`? If not, reject
5. Coordinator mints a macaroon with the caveats above (including `pid = <peer-pid>`) and responds with it
6. Volume stores the macaroon in memory for all subsequent `credentials` requests

### Credential exchange flow

When the volume needs S3 credentials (at startup or before expiry):

1. Volume sends `credentials <macaroon>` to `control.sock`
2. Coordinator verifies the HMAC chain (proves it minted this token)
3. Coordinator checks all caveats: volume/fork match, scope is `credentials`, `pid` matches `SO_PEERCRED` of the current connection
4. Coordinator issues short-lived read-only STS credentials (or equivalent) scoped to the volume's S3 prefix
5. Returns `ok <access-key> <secret-key> <session-token> <expiry-unix>`

The PID check on every request (step 3) means the macaroon is useless even if exfiltrated — it can only be presented from the original process. The HMAC chain means no volume can forge a token for a different volume.

### Token lifetime

The macaroon lives for the lifetime of the volume process. "Token dies when process is stopped" is the revocation model: when the coordinator stops a volume (on `delete` or coordinator shutdown), the PID is no longer live. Any subsequent `credentials` request with the old macaroon fails the `SO_PEERCRED` check — the PID either doesn't exist or belongs to a different process.

No revocation list is needed. This holds as long as the coordinator runs on the same host as the volumes. If the coordinator were ever moved off-host (not a current goal), the `SO_PEERCRED` check would be unavailable and explicit revocation would need to be designed.

### Attenuation

Because macaroons are additive-restriction-only, the volume can narrow its token before passing it to a subprocess (e.g. a future out-of-process demand-fetch helper):

```
original:   volume=myvm, scope=credentials, pid=1234
attenuated: volume=myvm, scope=credentials, pid=1234, not-after=<+5m>
```

The attenuated token is derived by the volume in-process — no coordinator round-trip. The coordinator verifies all caveats including the narrowed `not-after`.

### Standalone mode (no coordinator)

`serve-volume` accepts `--s3-access-key`, `--s3-secret-key`, `--s3-session-token` flags for direct credential passing. No macaroon is involved. This supports the fully standalone tier and is also useful for development.

### Implementation note

The caveat set is small and typed (`volume`, `fork`, `scope`, `pid`, optionally `not-after`). This is implementable in ~150–200 lines of HMAC-SHA256 chain code. Existing `macaroon` crates on crates.io should be evaluated before hand-rolling; if they use untyped opaque blobs (as noted in the fly.io design), a thin typed wrapper or a minimal hand-rolled implementation may be preferable for clarity.

## Import process lifecycle

OCI import is a potentially long-running operation. The coordinator supervises it as a short-lived child process, analogous to a volume process, with an explicit lifecycle marker on disk.

### Volume layout after import

`volume import <name> <oci-ref>` creates a single readonly volume at `<data_dir>/<name>/`:

- `pending/` holds the imported data until the coordinator drains it to S3
- After the coordinator drains: `index/` + `cache/` hold the segment index and body cache; `pending/` is empty
- No `wal/` — the volume is frozen after import completes
- `snapshots/<import-ulid>` marks the branch point for derived volumes
- `manifest.toml` records source metadata (OCI digest, arch)

To get a writable copy, the user runs `elide volume fork <name> <new-name>` after import completes. This is an explicit step, not automatic.

### `import.lock` and the two-phase lifecycle

When the coordinator begins an import it writes `<data_dir>/<name>/import.lock` containing a single line: the import ULID. This file is the source of truth for "an import is in progress or was interrupted here". The coordinator removes the file when the import process exits (whether success or failure).

The ULID in `import.lock` matches the ULID returned to the caller by `import <name> <oci-ref>`. This lets an operator correlate a lock file with coordinator logs or `import status` output.

The import process runs in two sequential phases:

**Write phase:** `elide-import` writes all segments directly into `pending/`. During this phase only `import.lock` is present (no `control.sock`). The coordinator sees this combination and skips drain entirely — segments are still being written and must not be uploaded mid-stream.

**Serve phase:** once all segments are written, the import process binds `control.sock` and enters a blocking loop that handles `promote <ulid>` IPC from the coordinator. Each promote causes the import to call `extract_idx` + `promote_to_cache` + remove `pending/<ulid>`. The import exits when `pending/` is empty, then removes `control.sock` before exiting.

The transition from write phase to serve phase is atomic from the coordinator's perspective: `control.sock` appears only when the import is fully ready to handle promote requests.

### Concurrency guard

The coordinator enforces mutual exclusion between volume processes and import processes on the same volume directory:

- Before spawning an import: check `volume.pid` — if a process is alive, refuse with `err volume already running`
- Before spawning a volume: check `import.lock` — if present, skip this volume (logged as a warning; no volume is started until the lock is cleared)
- The drain loop skips any volume directory that has `import.lock` present **and** no `control.sock` (write phase in progress). When both `import.lock` and `control.sock` are present (serve phase), the drain loop runs as normal — GC is naturally skipped because the import process does not respond to `gc_checkpoint` with valid ULIDs.

### Stale lock detection and cleanup

A crash (coordinator or import process) can leave `import.lock` behind with no matching live process. On every coordinator startup, the coordinator checks each `import.lock`:

1. Read the ULID from the file
2. If no import job with that ULID is tracked in memory, check whether any process wrote `import.pid` alongside the lock
3. If a live process is found **and** `control.sock` is present: the import is in serve phase and doing useful work — leave it running. The coordinator will begin draining against it on the next tick.
4. If a live process is found but no `control.sock`: the import was mid-write when the coordinator restarted — send SIGTERM (write phase must be restarted clean)
5. If no live process is found: remove the lock, log a warning including the ULID and volume path

The volume directory is left intact — it may contain partial segment data useful for debugging. After the stale lock is removed, the coordinator resumes normal supervision of the volume.

### `volume delete`

`elide volume delete <name>` is the recovery and cleanup command:

1. Send SIGTERM to any running volume process (via `volume.pid`)
2. Send SIGTERM to any running import process (via `import.pid`)
3. Remove stale `import.lock` if present (import process already dead)
4. Remove the volume directory tree (`<vol-dir>` and all contents)
5. Respond `ok`

After `volume delete`, `volume import <name> <oci-ref>` starts fresh with a clean directory.

### Output retention

The coordinator captures the import process's stdout and stderr. Output lines are buffered in memory for the lifetime of the coordinator (or until a configurable retention limit). `import attach` replays buffered output to any client that connects after the process has already exited, so a brief disconnect during a long import does not lose the log.

## Write Path

```
1. VM issues write for LBA range
2. Buffer contiguous writes; each non-contiguous LBA gap finalises an extent
3. For each extent:
   a. Hash extent content → extent_hash
   b. Check local extent index (own segments + all ancestor segments + other volumes) for extent_hash
      - Found  → write REF record to WAL (thin: no data payload; body stays in canonical segment)
      - Not found → write DATA record to WAL (fsync = durable)
4. Update live LBA map with new (start_LBA, length, extent_hash) entries
```

Durability is at the write log. S3 upload is asynchronous and not on the critical path.

**WAL ULID assignment.** The active WAL file is named `wal/<ULID>` where the ULID is assigned **at WAL creation**, not at flush time. This is a deliberate design choice: when the WAL is promoted to `pending/<ULID>`, the segment inherits the same ULID as the WAL file. If a crash occurs after `pending/<ULID>` is written but before the WAL file is deleted, recovery detects the situation by checking whether `pending/<ulid>` already exists alongside `wal/<ulid>` — if so, the WAL was already flushed and the stale WAL file is discarded. This linkage requires the ULID to be fixed at creation time; assigning it at flush time (as the lsvd reference implementation does) would eliminate the shared-name detection mechanism and require a separate marker or header field to handle the crash edge case.

The consequence of pre-assignment is that `sweep_pending` — which may run while a WAL is already open — cannot use `mint.next()` for its output ULID. `mint.next()` would produce a ULID higher than the current WAL's pre-assigned ULID; the sweep output would then sort after the WAL's eventual flush segment during rebuild, silently overwriting newer data with stale data. The fix: `sweep_pending` uses `max(candidate_ULIDs)` as its output ULID, which is always below the current WAL's ULID because all `pending/` candidates were created by prior WAL flushes. This also makes the causal ordering explicit — the sweep output ULID is derived from its inputs, not from the clock. The proptest suite found and verified this invariant; see [testing.md](testing.md).

**Dedup is local and opportunistic.** The write path checks the local extent index before writing data. If the extent already exists anywhere in the local tree (or, best-effort, in another volume on the same host), a REF record is written instead — no data is stored again. See the Dedup section below for full scope details.

**No delta compression locally.** Delta compression is computed at S3 upload time and exists in S3 only. Local segment bodies contain either the full extent data (DATA records) or no body bytes at all (REF and ZERO records). A REF record is resolved via the extent index to a canonical DATA record somewhere reachable — in the same volume (including the same segment), an ancestor, or another local volume under the same dedup scope. On S3 fetch, deltas are applied and the full extent is materialised locally before being cached and served to the VM.

## Read Path

```
1. VM reads LBA range
2. Look up LBA in live LBA map → extent_hash H
3. Check local segment bodies for H (own pending/ + bare gc/<ulid> + cache/<ulid>.body, then ancestor layers):
   - Hit in pending/ or bare gc/<ulid> → read directly from the full segment file; decompress if needed
   - Hit in cache/<ulid>.body → read from body-relative offset; .present bitset guards each extent; decompress if needed
   - Miss → look up H in extent index → (segment_id, body_offset, body_length)
4. Issue a byte-range GET to S3 covering a chunk of the segment body
   - The fetch unit is a contiguous byte range (e.g. 1MB-aligned chunk) that
     includes the needed extent(s) plus neighbours for spatial locality
   - The segment index section encodes body_offset + body_length per extent,
     so the chunk boundaries can be derived precisely
   - If a delta body is available and smaller, fetch from the delta instead;
     when multiple delta sources are available, prefer any source already
     cached locally, then prefer the earliest (lowest ULID) among uncached
     candidates — the earliest source maximises reuse because a single fetch
     warms a base that can decode every subsequent derived version
5. Write fetched bytes to cache/<ulid>.body; set bit in .present; decompress and return to VM
```

**DedupRef cold read.** If the missing extent is a `DedupRef`, its own segment holds no bytes for it — the extent index resolves `hash → (canonical_segment, body_offset, body_length)` and the byte-range GET is issued against the **canonical** segment, not the referring one. This is the same code path as any other cold extent fetch; the only difference is the target segment ULID. Thin DedupRef upload (see formats.md § Segment File Format) is safe because every canonical target is pinned by construction.

The kernel page cache sits above the block device and handles most hot reads. The local segment cache handles warm reads. S3 is the cold path.

**GC bare-name window — reading from `gc/`.** Between volume apply and coordinator upload, the new GC output body lives at bare `gc/<new-ulid>` (volume-signed). The in-memory extent index already points at `<new-ulid>` and `locate_segment_body` resolves bare files directly — no sidecar marker. The matching `.staged` (coordinator-staged, pre-apply) is naturally excluded because the bare-name match only accepts files with no extension. Once the coordinator's `apply_done_handoffs` finishes uploading and the volume's `finalize_gc_handoff` deletes the bare body, the read path falls through to `cache/<new>.body` (which `promote_segment` already populated).

**Local read path implementation.** The in-process read path resolves each LBA range as follows: LBA map BTreeMap range scan → per-extent HashMap lookup in the extent index → seek + read from the segment file. A single file-handle cache avoids repeated `open` syscalls for sequential extents within the same segment; a cache miss incurs one `open` plus O(ancestor_depth) `stat` calls to locate the segment across the directory tree. Compressed extents are decompressed in full on every read — there is no sub-extent decompression granularity (same model as lsvd). Both overheads are bounded in practice by the segment size limit and the pre-log coalescing block limit.

**Demand-fetch is at extent granularity, not segment granularity.** A segment file on S3 may contain hundreds of extents. Only the specific extent needed for a given read is fetched — the rest of the segment is never downloaded unless separately requested. This is the same design as the lab47/lsvd reference implementation, which issues byte-range `GetObject` requests for chunk-sized slices of segment bodies (1MB chunks, LRU-cached locally) and never downloads entire segment files. In practice, 93.9% of a 2.1GB Ubuntu cloud image is never read during a typical systemd boot — meaning 93.9% of S3 segment data is never fetched.

## LBA Map

The **LBA map** is the live in-memory data structure mapping logical block addresses to content. It is a sorted structure (B-tree or equivalent) keyed by `start_LBA`, where each entry holds `(start_lba, lba_length, extent_hash)`. It is updated on every write (new entries added, existing entries trimmed or replaced for overwrites) and is the authoritative source for read path lookups.

LBA ranges that have never been written are absent from the map and read as zeros. LBA ranges that have been explicitly zeroed (via TRIM or WRITE_ZEROES) are present in the map with the reserved `ZERO_HASH` sentinel (`[0x00; 32]`). The distinction matters for ancestry: an absent range falls through to the ancestor layer; a `ZERO_HASH` entry explicitly masks it. The read path checks for `ZERO_HASH` before doing any extent index lookup and returns zeros immediately.

**Contrast with lab47/lsvd:** the reference implementation calls this `lba2pba` and maps `LBA → segment+offset` (physical location). GC repacking must update it for every moved extent. Palimpsest maps `LBA → hash` — the logical layer. Physical location (`hash → segment+offset`) is a separate extent index. This two-level indirection means GC repacking updates only the extent index; the LBA map is never rewritten for GC.

**Layer merging:** a live volume's LBA map is the union of its own data and all ancestor layers. At startup, layers are merged oldest-first (root ancestor first, live node last), so later writes shadow earlier ones. This is the same model as the lsvd `lowers` array, encoded in the directory tree.

### LBA map persistence

The LBA map is optionally persisted to a local `lba.map` file on clean shutdown and used as a fast-start cache on restart.

**Freshness guard:** the file includes a BLAKE3 hash of the sorted list of all current local segment IDs (own + ancestors). On startup, if the guard matches the current segment list, the cached LBA map is loaded directly without scanning segment index sections. If the guard doesn't match (new segments were written, or ancestry changed), the LBA map is rebuilt from scratch.

**Rebuild procedure:**
1. Follow the `origin` chain from the fork to the root, collecting ancestor layers (oldest first)
2. For each ancestor fork: scan bare `gc/<ulid>` files (lowest priority), then `index/*.idx`, then `pending/` in ULID order, stopping at the branch-point ULID stored in the child's `origin`
3. For the current fork: scan all of bare `gc/<ulid>` files, `index/*.idx`, and `pending/` (all of them)
4. Replay the current WAL on top (WAL entries are the most recent writes)

Since segment index sections are the ground truth for segment contents, rebuilding the LBA map requires only index sections and the WAL — never the segment data bodies. A full startup rebuild for a volume with 100 segments across its ancestry is a scan of ~6–200KB per segment index section, not 3GB of segment bodies.

### Manifest format

"Manifest" refers specifically to the **serialised form** of the LBA map, written optionally at snapshot time or as a startup cache. It is a correctness-optional optimisation — the LBA map is always reconstructible from segment index sections. When a manifest exists and its freshness guard is valid, it allows startup without scanning any segment files.

When persisted, the format is a binary flat file:

**Header (84 bytes):**

| Offset | Size | Field        | Description                          |
|--------|------|--------------|--------------------------------------|
| 0      | 8    | magic        | `ELIDMAP\x01`                        |
| 8      | 32   | snapshot_id  | blake3 of all extent hashes in LBA order |
| 40     | 32   | parent_id    | snapshot_id of parent; zeros = root  |
| 72     | 4    | entry_count  | number of entries (u32 le)           |
| 76     | 8    | timestamp    | unix seconds (u64 le)                |

**Entries (44 bytes each, sorted by start_LBA):**

| Offset | Size | Field      | Description                          |
|--------|------|------------|--------------------------------------|
| 0      | 8    | start_lba  | first logical block address (u64 le) |
| 4      | 4    | length     | extent length in 4KB blocks (u32 le) |
| 12     | 32   | hash       | BLAKE3 extent hash                   |

One entry per extent. Unwritten LBA ranges have no entry (implicitly zero). Explicitly zeroed LBA ranges appear as normal 44-byte entries with `ZERO_HASH` as the hash field — these are required to mask ancestor data in the ancestry walk and must be serialised even though they carry no body bytes.

**Snapshot identity:** `snapshot_id = blake3(all extent hashes in LBA order)` — derived from the live LBA map, not from the file bytes. Identical volume state always produces the same snapshot_id regardless of when or where the manifest was serialised. The directory ancestry is the authoritative parent chain; `parent_id` in the manifest is a convenience field for S3 contexts where directory structure is not available.

### S3 cold start

When a volume is started on a host with no local data — pulled from a snapshot in S3 rather than built from local writes — there are no local segment files to scan and no `lba.map` cache. To reconstruct the LBA map, the volume would otherwise need to fetch the index section of every segment in the entire ancestry tree from S3: potentially hundreds of GETs before serving the first read.

The manifest eliminates this. At snapshot time, the manifest is uploaded to S3 at a well-known key:

```
s3://bucket/manifests/<snapshot-id>
```

Cold start from S3 then becomes:

```
1. Given a snapshot ID, fetch s3://bucket/manifests/<snapshot-id>  — one GET
2. Verify freshness guard; load LBA map from manifest entries
3. Begin serving reads via demand-fetch — no segment scanning required
```

The segment index sections remain the ground truth and are always sufficient to rebuild the LBA map if the manifest is absent or corrupt, but the manifest is the expected path for any cold start from S3. Writing the manifest to S3 at snapshot time is therefore a required part of the snapshot operation, not an optional optimisation.

## Extent Index

Maps `extent_hash → (segment_ULID, body_offset)`. Separate from the LBA map — the LBA map is purely logical (what data is at each LBA range), the extent index is physical (where that data lives on disk or in S3).

**Contrast with lab47/lsvd:** the reference implementation uses a single `lba2pba` map — a direct `LBA → segment+offset` (physical location) index. GC repacking requires updating this map for every moved extent. The LBA map + extent index split means GC can repack extents (changing their location) by updating only the extent index. The LBA map is never rewritten for GC.

The extent index covers the live node's own segments, all ancestor segments, and — on a best-effort basis — segments from other volumes stored under the same common root. At startup, the volume process scans the full common root directory tree, reading the index section of each segment file it finds. Ongoing updates use inotify (or periodic re-scan) to pick up new segments from other volumes as they are promoted. Because segment ULIDs are globally unique, `hash → ULID + body_offset` is sufficient to locate any extent; the on-disk path is derived at runtime from the ULID by searching the common root.

This is **purely local and coordinator-free**: the shared filesystem layout is the coordination mechanism. Cross-volume dedup is best-effort — a segment promoted by another volume after the last scan is a missed opportunity, not an error. Such duplicates are harmless and can be coalesced by GC.

## Dedup

**Exact dedup:** two extents with the same BLAKE3 hash are identical. Dedup is detected and applied **opportunistically on the write path**: before writing a DATA record to the WAL, the extent hash is checked against the local extent index. If a match is found, a REF record is written instead.

Dedup scope is **all volumes on the local host**. The extent index covers the current volume's own tree (own + ancestor segments) plus, on a best-effort basis, all other volumes stored under the same common root. No remote or cross-host dedup check is performed. Dedup quality is highest for snapshot-derived volumes (ancestor segments already contain most of the data) and lower for freshly provisioned volumes; cross-volume dedup raises quality for volumes that share a common base image even without a snapshot relationship.

**DedupRef entries are format-level thin.** When dedup fires, a REF record is written to the WAL with no body bytes. On promotion to a segment, the DedupRef index entry stores `stored_offset = 0` and `stored_length = 0` — it reserves no body space at all. The segment header's `body_length` is the sum of `stored_length` over DATA and INLINE entries only; DedupRef entries contribute zero. This layout is **unified across `pending/`, `cache/`, and S3**: the same segment bytes, the same body size, the same index entry semantics everywhere. There is no "local-disk vs S3" asymmetry; thin DedupRef delivers S3 storage savings directly, without a materialisation or compaction step.

- **WAL / `pending/`:** DedupRef entries are index-only. No body bytes, no body reservation, no sparse hole. Hash-dead DATA entries (LBA overwritten + hash no longer referenced by any live LBA in the volume) are hole-punched in place on `pending/<ULID>` by `redact_segment` at upload time, so deleted data does not leave the host. DedupRef entries are not touched by redact — they contribute no body bytes to begin with. Redact changes no offsets, no lengths, no signature, no file size; only the physical storage of dead DATA regions is freed.
- **S3:** the uploaded object is the local `pending/<ULID>` file as-is. `body_length` is already the thin sum; the S3 object is proportionally smaller than it would be with DedupRef reservation. DedupRef cold reads issue a byte-range GET against the **canonical** segment identified by the extent index, not the referring one.
- **`cache/` (local):** the three-file cache format (`.idx` + `.body` + `.present`) inherits the same thin layout. The `.body` file is sized to `body_length` — no DedupRef holes. The `.present` bitset has DedupRef bits unset (their body bytes are served from the canonical segment, not from this cache body).
- **Read path (local and cache miss):** the read path resolves the hash through the extent index to `(canonical_segment, body_offset, body_length)` and reads or fetches from the canonical segment. The DedupRef entry's own `stored_offset`/`stored_length` are never consulted — the read path goes through `extent_index.lookup(hash)`, which only returns canonical DATA locations.
- **Extent index:** required for dedup detection at write time and for serving all DedupRef reads. The extent index is a hard dependency of the read path, local and S3 alike. This is an explicit trade-off: segment-level self-containment is given up in exchange for a format that is thin everywhere.
- **GC:** DATA and DedupRef entries are treated with the same LBA-based liveness in `collect_stats`, but GC output construction preserves DedupRef as DedupRef rather than reclassifying to DATA. The coordinator never fetches body bytes for DedupRef entries (they have none). Repack handoff lines are emitted only for DATA entries; DedupRef entries pass through the GC output and resolve via the extent index, which is re-pointed at the new DATA location by the Repack line.

**Two invariants make thin DedupRef sound.**

**1. Pinning invariant.** Every live DedupRef targets a segment GC cannot rewrite or remove. This comes from two rules acting together:

- **Snapshot floor.** GC may not rewrite, compact, or remove any segment whose ULID is ≤ the latest snapshot ULID on that segment's owning volume.
- **First-snapshot pinning.** Any DedupRef written between snapshots targets a segment in the same live window (either a prior segment in this volume, or a segment in an already-pinned ancestor). When `snapshot()` advances the floor to the current max ULID, every segment written in that window — including every DedupRef target in this volume — becomes pinned atomically.

The invariant **"every DedupRef target ULID ≤ the snapshot floor on its owning volume"** holds at all snapshot boundaries by construction. It is enforced by a `debug_assert!` in `snapshot()` and a proptest invariant in the simulation model.

**2. Canonical-presence invariant.** For every live DedupRef hash H, `extent_index.lookup(H)` returns a DATA entry. The location of that DATA entry is irrelevant — it may be in any segment, local or S3-backed, and may be moved by GC via Repack — but it must exist. This is what makes the DedupRef resolvable at all.

The canonical-presence invariant is maintained by GC's liveness rule: a DATA entry is kept alive if **any** LBA in the volume references its hash, including LBAs that reference the hash via a DedupRef. This is load-bearing on `lba_map::live_hashes()` being sourced from the LBA map directly (every `(lba → hash)` entry contributes, with no kind discrimination between DATA-sourced and DedupRef-sourced LBAs). See the worked examples below.

An unsnapshotted volume may hold DedupRefs whose targets are in the same live window and not yet pinned — but such a volume has no S3 presence for those segments either, so the thin-upload question does not arise until the first snapshot, at which point the invariant is re-established.

### Worked examples: two DedupRefs with the same hash in one segment

Two edge cases worth making explicit, since they test both invariants simultaneously.

**Variant (a) — both DedupRefs to an external canonical.** A segment S contains two index entries with the same hash H, both DedupRef. The canonical DATA for H lives in some other segment S_canon (an ancestor segment, a prior segment in this volume, or a segment in another volume under the same dedup scope).

- Format: both entries have `stored_offset = 0, stored_length = 0`. Neither contributes to S's `body_length`.
- LBA map rebuild: both entries contribute `(lba → H)`. Two distinct LBAs both resolve to H.
- Extent index rebuild: both are skipped (DedupRef never enters the extent index). The extent index maps `H → S_canon` from S_canon's own scan.
- Reads of either LBA → `extent_index.lookup(H) → S_canon` → serve from S_canon's body. Correct.
- GC of S: both DedupRef entries kept alive iff their LBAs are live. DedupRef passes through to the GC output unchanged. `H`'s canonical stays in S_canon (or is Repacked there by a separate GC pass).

**Variant (b) — one DATA + one DedupRef to the sibling DATA, same segment.** A segment S contains a DATA entry for H at LBA1 and a DedupRef for H at LBA2. The DATA entry in S is the canonical for H.

- Format: the DATA entry has `stored_offset = X, stored_length = L, body_length += L`. The DedupRef has `stored_offset = 0, stored_length = 0, body_length += 0`.
- LBA map rebuild: both contribute: `LBA1 → H` and `LBA2 → H`.
- Extent index rebuild: DATA inserts `H → {segment=S, body_offset=X, body_length=L}`. DedupRef is skipped (order-independent).
- Reads of LBA1 and LBA2 both resolve through `extent_index.lookup(H)` back into S at offset X. Correct.
- GC of S, with LBA1 overwritten (now maps to some other hash H'): the DATA at LBA1 has `lba_live = false` but `extent_live = true` (it is canonical for H) and `live_hashes.contains(H) = true` (because LBA2 still references H via the DedupRef). The DATA is therefore kept alive and carried into the GC output — the sibling DedupRef at LBA2 is what keeps it alive.

**Why `live_hashes()` must be sourced from the LBA map, not from extent_index keys.** Variant (b) only works because `lba_map::live_hashes()` is `self.inner.values().map(|e| e.hash).collect()` — every hash in the LBA map contributes, whether the LBA was populated via a DATA write or a DedupRef write. If `live_hashes()` were ever "optimised" to a DATA-only filter, the DATA at LBA1 in variant (b) would become invisible to GC (not in live_hashes, not lba_live) and would be dropped as dead. The sibling DedupRef at LBA2 would then lose its canonical, violating the canonical-presence invariant. The function's name is worth hardening (`lba_referenced_hashes()` is more precise and hostile to this misreading) and the behaviour is worth a regression test.

**Delta compression** is a separate concern from dedup. See [design-delta-compression.md](design-delta-compression.md) for the full sequencing; the summary below covers the format interaction with GC, the extent index, and `lba_referenced_hashes`.

**Source selection** is filemap path-matching across imported snapshots linked by `extent_index` provenance lineage. For a changed file in a child import, the natural delta source is the same path in the parent import. Filemap-based delta, computed in-process in `elide-import` after pending segments are written but before the volume is promoted, is the single production delta path.

**Thin delta entries.** `EntryKind::Delta` mirrors the format-level shape of thin DedupRef: `stored_offset = 0`, `stored_length = 0`, no body bytes anywhere. The entry's content is materialised by fetching a delta blob from the segment's delta body section and decompressing it against a source extent body located via `extent_index.lookup(source_hash)`. Multiple delta options per entry act as hints; the reader picks the first whose source resolves (earliest-source preference).

- **Delta source must be a DATA entry** — never another Delta. No delta-of-delta chains.
- **Unified across `pending/`, `cache/`, and S3**, same as DedupRef. The delta body section is preserved on disk and in S3; Delta entries contribute nothing to `body_length`.
- **Read path:** `extent_index.lookup(source_hash)` → fetch source body → fetch delta blob → zstd-dict decompress. If no option's source resolves, the fetch fails; there is no full-body fallback in the same segment (the price of thin delta).

**Two invariants make thin Delta sound**, mirroring the DedupRef invariants:

1. **Pinning invariant.** Every live Delta's source extent lives in a segment GC cannot rewrite or remove. Cross-import delta sources come from `extent_index` provenance lineage, which names only fully-snapshotted ancestors (enforced at import time). Combined with the snapshot floor + first-snapshot pinning rules, the source segment is GC-stable for the lifetime of any Delta referencing it.
2. **Canonical-presence invariant.** For every live Delta with source hash H, `extent_index.lookup(H)` returns a DATA entry. Maintained by the liveness-rule extension below.

**`lba_referenced_hashes()` extension.** The set is extended to include delta source hashes for every live Delta LBA. With this folding, GC's existing rule — "keep DATA alive iff any live LBA references its hash" — automatically covers delta sources with no new code path. The rule's load-bearing property (variant (b) DedupRef correctness) is unchanged; the set simply contains more hashes per live LBA in the Delta case.

**GC under Delta entries.** GC carries Delta entries through `compact_candidates_inner` unchanged, same pattern as DedupRef: a match arm that debug-asserts `stored_offset == 0, stored_length == 0`, copies the entry through to the output, and skips `fetch_live_bodies`. The delta blob stays in its source segment's delta body section across GC runs — GC does not move delta blobs between segments. Repack handoff lines are emitted only for DATA entries.

Historical note: an earlier design considered delta compression as purely S3-side, with local segments carrying full extents and the coordinator computing deltas fresh at upload. Thin delta collapses this — the producer writes the delta entry thin in-process during import, so local and S3 layouts stay unified (the same "no local/S3 asymmetry" property that thin DedupRef establishes).

Delta compression is compelling for point-release image updates; not worth the complexity for cross-version (major version) updates where content is genuinely different throughout.

**Sparse** is an alternative S3-only reduction technique that operates at block (4KB) granularity rather than byte granularity, and requires no diff library. At S3 upload time, a newly-promoted extent is compared block-by-block against the ancestor's blocks for the same LBA range. Unchanged blocks are not uploaded — they already exist in the ancestor S3 segment and are inherited implicitly via the layer merge. Only changed blocks are uploaded, as one or more small extents in the live leaf's S3 segment.

No explicit descriptor for this is needed. The LBA map encodes it naturally: the live leaf's S3 manifest contains entries only for changed block LBA ranges; any LBA range absent from the live leaf falls through to the ancestor during layer merge. The ancestor blocks are already there.

**Local/S3 divergence under sparse:** the local `pending/<ULID>` segment still holds the full extent (e.g. H_new covering all 256 blocks). The S3 object for the same segment contains only the changed block extents. Local reads are served directly from the full local copy; S3 reads reconstruct via layer merge. The local LBA map and the S3 manifest therefore differ — the local LBA map has one entry covering the full LBA range, the S3 manifest has one small entry per changed block. This divergence is correct by design: the local segment is a complete, self-contained store; the S3 object is a sparse representation of the same data.

Because the S3 object under sparse is substantially different from the local file (not merely the local file with a delta body appended), the coordinator must build the S3 object fresh rather than streaming the local file with additions. See [formats.md](formats.md) for the upload path.

**GC under sparse** is simpler than under delta compression. There are no delta dependency chains: each changed block is an independent extent. GC of the live leaf removes only the live leaf's own extents; the ancestor blocks are in frozen ancestor segments, which are structurally immutable while any live descendant exists. No "materialise before removing source" logic is needed.

**Cross-host dedup caveat:** H_new (the full extent hash) is never registered in the S3 extent index, because H_new is never uploaded. If another host holds H_new locally and attempts a cross-host dedup lookup in S3, it will not find it and will re-upload. This is a missed dedup opportunity, not a correctness failure.

### Delta compression vs sparse

Both techniques are S3-only and both require a snapshot ancestor to be present (source blocks must be in a frozen segment to be safe from GC). The key trade-offs:

| | Delta compression | Sparse |
|---|---|---|
| Minimum stored size per change | bytes actually different | 4KB per changed block |
| Sub-block changes (e.g. 1 byte in 4KB) | efficient — stores ~tens of bytes | wastes up to 4KB |
| Implementation | diff library required | block hash comparison only |
| S3 read path | apply diff to source; one source extent | layer merge; multi-source but no CPU diff |
| GC | dependency chain tracking; materialise if source removed | none — no chains |
| S3 object construction | local file + appended delta body | fresh build; diverges from local file |
| Local/S3 divergence | header/index only (delta body appended) | index differs (changed-block extents only) |

Sparse is simpler to implement and has cleaner GC semantics; delta compression is more storage-efficient for sub-block changes. For the common VM image workload — file-level overwrites where changed 4KB blocks are genuinely different — the 4KB floor is not a meaningful constraint and sparse may be the right default. For database-style workloads with byte-level random updates, delta compression captures savings that sparse cannot.

The two are not mutually exclusive: sparse could be applied first (skip unchanged blocks entirely), and delta compression applied to the remaining changed blocks. Whether the added complexity is worth it depends on the change distribution of the target workload.

**Sparse gives the client fetch-strategy flexibility.** Because sparse data is raw bytes at known offsets in S3 objects, a client has a choice of how to reconstruct an extent:

- *Simple*: fetch the full ancestor extent and overlay the live leaf's changed blocks on top. Two byte-range GETs, no algorithm. A client unaware of the sparse strategy can do this correctly just by following the layer merge.
- *Precise*: compute exactly which LBA sub-ranges come from the ancestor vs the live leaf; issue byte-range GETs only for those ranges. Avoids fetching ancestor bytes that will be overwritten by the live leaf.

The client picks based on economics — bandwidth, request latency, cache state — and neither strategy requires anything beyond "read bytes at offset X, length Y."

Delta compression collapses this flexibility: reconstruction always requires fetching both source and delta, then applying a CPU transform. The data is not directly addressable. Sparse also composes cleanly with the boot-hint repacking optimisation: repacked segments co-locate extents contiguously for efficient byte-range fetches, and sparse preserves that property since all data remains raw bytes at fixed offsets. Delta compression complicates repacking because moving a source extent can invalidate dependent deltas.

## Named Forks and Volume Addressing

### Concepts

| Term | Definition |
|------|------------|
| **Volume** | A ULID-named directory directly under `data_dir`. Every volume is a peer. Its stable global identity is its ULID (the directory name and S3 prefix); its human-readable name is stored in `volume.name`. |
| **Fork** | A volume created from another volume's snapshot via `volume fork`. Structurally identical to any other volume; the parent relationship is recorded in an `origin` file using ULID paths. "Fork" describes lineage, not location. |
| **Snapshot** | A marker file (`snapshots/<ulid>`) recording a point in a volume's committed segment sequence. The ULID gives the position: all segments with ULID ≤ the snapshot ULID are part of that snapshot. The latest snapshot ULID also serves as the **compaction floor** — segments at or below it are frozen and will never be compacted. The file content is empty or an optional human-readable label. |
| **Imported volume** | A readonly volume populated by `volume import`. Marked with `volume.readonly`. No `wal/` — frozen after import completes. The user runs `volume fork <name> <new-name>` to get a writable copy. |
| **Export** | A squash-and-detach operation that produces a new self-contained volume with no ancestry dependencies. |

### Ancestry walk

To rebuild the LBA map for a volume:
1. Follow the `origin` chain to the root volume (no `origin` file). Each `origin` file contains `<parent-ulid>/snapshots/<snapshot-ulid>` — the parent is resolved as `by_id_dir/<parent-ulid>`. Using ULIDs in `origin` means the chain is stable across renames and host moves. `walk_ancestors(vol_dir, by_id_dir)` performs this traversal.
2. From the root outward, for each ancestor in the chain: apply bare `gc/<ulid>` files (lowest priority), then `index/*.idx`, then `pending/`, in ULID order within each group, stopping at (and including) the branch-point ULID recorded in the child's `origin`. Snapshot markers are not scanned during replay.
3. Apply the current volume's own bare `gc/<ulid>` files, `index/*.idx`, and `pending/` in ULID order within each group (all of them, no cutoff).
4. **Priority within each rebuild pass:** bare `gc/<ulid>` files are applied first (lowest priority — GC-compacted data, ULID < the WAL flush that followed), then `index/*.idx` (committed segments promoted from pending/), then `pending/` (highest priority — in-flight WAL flushes, always the most recent write for any LBA). Bodies with `.staged` siblings are coordinator-staged and not yet volume-re-signed; they are excluded by `collect_gc_applied_segment_files`.
5. Replay the WAL.

The per-ancestor ULID cutoff is what prevents a concurrently-written ancestor from leaking newer data into the derived volume's view.

### Single-writer invariant

**Each volume directory has exactly one process that writes new segments into it.** For writable volumes, the volume process (holding `volume.key`) is the sole writer of `pending/`, `index/`, and `cache/` — `index/<ulid>.idx` and `cache/<ulid>.{body,present}` are written by the volume in response to the coordinator's `promote <ulid>` IPC, which the coordinator sends only after confirmed S3 upload. For readonly/imported volumes, the import process is the sole writer of `pending/` during the write phase and writes `index/` + `cache/` during its serve phase, also in response to `promote` IPC. The coordinator writes only to `gc/` as a staging area; the volume re-signs the coordinator's staged output in-place within `gc/`, then the coordinator uploads it to S3 and triggers `promote` IPC so the volume writes the resulting `index/` and `cache/` files. Crucially, the coordinator derives the output ULID from the volume's existing write history (`max(input ULIDs).increment()`) rather than from its own wall clock. The coordinator does not author an independent position in the volume's timeline — it extends the sequence by one step from where the volume left off.

This invariant is what makes ULID total-order sufficient for all correctness guarantees in rebuild, GC, and ancestor cutoff:

- **Rebuild:** processing segments in ULID order is unambiguous — there is no external writer that could inject a segment with an arbitrary timestamp into the middle of the sequence.
- **GC ULID assignment:** `max(inputs).increment()` is safe because any write that occurs *during* compaction comes from the one writer, gets a timestamp from the current wall clock, and is therefore far ahead of the old `max(inputs)` timestamp (which has already passed through the drain pipeline). No locking is required.
- **Ancestor cutoff:** the branch-point ULID is a stable boundary because the ancestor's writer cannot insert segments retroactively below it.

The single-writer property and crash safety are enforced through ULID ordering and signing verification. Only the volume process holding `volume.key` can produce valid segment signatures; any unsigned or incorrectly-signed segment will be rejected at read time.

**Signing trust boundary:** signature verification is enforced on all segments in `pending/` and bare `gc/<ulid>` files. Every such file was either:
- Written directly by the volume process itself at WAL promotion time (signed with `volume.key`), or
- Produced by the self-describing GC handoff protocol: the coordinator creates compacted segments at `gc/<ulid>.staged` (signed with an ephemeral key) with the input ULID list embedded in the segment header. The volume reads `.staged`, derives the apply set from the inputs field, writes a re-signed copy to `gc/<ulid>.tmp`, and renames it to bare `gc/<ulid>` (the atomic commit point). Only after the bare file appears does the coordinator upload it to S3 and trigger `promote_segment` IPC, which writes the resulting `index/` + `cache/` files. The bare file is deleted at the end via `finalize_gc_handoff` IPC. This ensures S3 always receives the volume-signed version. Note that `cache/` body files are coordinator-written (not volume-signed) — see the known gap below.

This protocol keeps the private key (`volume.key`) on the volume host and prevents the coordinator from forging new data.

**Known gap — demand-fetched segments:** segments in `cache/` (pulled from S3 on demand) are not currently verified against `volume.pub`. The fetch path uses `read_segment_index` (unverified) rather than `read_and_verify_segment_index`. As a result, a tampered S3 object would not be detected when it enters the local read path. This gap pre-dates the mandatory-signing work; extending verification to the fetch path is deferred and tracked as a known gap. The intended fix is to verify content integrity (hash checks) and re-sign with `volume.key` when a fetched segment is promoted into the local cache.

See [formats.md](formats.md) — *Fork ownership and signing*.

The ULID monotonicity invariant and crash-recovery correctness are verified by property-based tests using proptest. See [testing.md](testing.md) for the simulation model, the two properties tested, and a concrete bug these tests found and fixed.

### Concurrency model

`Volume` is intentionally **single-writer with no internal locking**. All mutations (WAL append, LBA map update, flush, compaction) are serialized by the caller. The serialization point is made explicit at the integration layer rather than hidden inside the struct.

The intended integration pattern is **actor + snapshot**:

**`VolumeActor`** owns a `Volume` exclusively and processes requests from a `crossbeam-channel` bounded channel sequentially. It is the sole thread that mutates the fork. After every `write()` call, it publishes a new `ReadSnapshot` via an `ArcSwap`.

**`VolumeHandle`** is the shareable client handle — `Clone + Send`. It holds:
- A `crossbeam_channel::Sender<VolumeRequest>` to the actor
- An `Arc<ArcSwap<ReadSnapshot>>` for the lock-free read path
- A per-handle file-descriptor cache (`RefCell<Option<(String, File)>>`) so sequential reads hitting the same segment avoid repeated `open` syscalls. Each clone gets a fresh empty cache — handles are not `Sync` and are intended for exclusive use by one thread.
- `last_flush_gen: Cell<u64>` — tracks the last snapshot generation whose offsets populated the fd cache. Compared against `ReadSnapshot::flush_gen` on every read.

**`ReadSnapshot`** is an immutable view sufficient to serve any read. It holds:
- `Arc<LbaMap>` and `Arc<ExtentIndex>` — the actor stores its live maps as `Arc`s; publishing a snapshot is an `Arc::clone()` — O(1) unless a reader is still holding the previous version, in which case the next write triggers a copy-on-write clone via `Arc::make_mut`. In practice reads complete in microseconds, so the refcount is almost always 1.
- `flush_gen: u64` — a promotion counter incremented by the actor after every WAL promotion. Handles compare this against a cached value before each read; if it changed they evict their file-descriptor cache before proceeding. Embedding the counter inside the snapshot means a handle always sees a consistent pair: the post-promote extent index offsets and the corresponding generation arrive together in a single `ArcSwap::load()`. There is no window in which a handle could observe new offsets without knowing to evict its cache, or vice versa.

**Request flow:**
- `Write`, `Flush`, `SweepPending` — sent through the channel with an attached `crossbeam_channel::Sender` for the response. The actor processes them in arrival order and replies when done.
- `Read` — the calling thread loads the current snapshot via `ArcSwap::load()` and resolves the request entirely on that thread. No channel round-trip; no contention with the actor.

**Read-your-writes:** the snapshot is published after every `write()` call, before the response is sent back to the caller. Any read issued after a write has returned will see that write, regardless of whether it has been flushed to a `pending/` segment — the same guarantee a physical disk provides.

**WAL promotion (decoupled from writes):** `Volume::write()` only appends to the WAL and updates the in-memory maps — it never touches the segment layer. The actor is responsible for promoting the WAL to a `pending/` segment via two mechanisms:

1. **Threshold-triggered:** after sending the write reply, the actor checks `Volume::needs_promote()`. If the WAL has reached the 32 MiB soft cap, it calls `flush_wal()` immediately — before processing the next queued message. The write caller is already unblocked; the cost is borne by the next message in the queue.

2. **Idle-flush tick:** the actor run loop uses `crossbeam_channel::tick(10s)` alongside the request channel. When the tick fires and the WAL is non-empty, `flush_wal()` is called. This ensures data is promoted even under low or zero write load. The interval is 10 seconds (chosen for observability during development; tightening it later is a one-line change).

Background promotes that fail (I/O error, disk full) are logged and do not crash the actor — the data is safe in the WAL. The next explicit `Flush` or threshold-triggered promote will surface the error.

**Why `crossbeam-channel`:** the actor loop and NBD/ublk handlers are synchronous threads; `crossbeam-channel` is a natural fit. When ublk integration uses io_uring, ublk queue threads remain synchronous callers — they block on the `Sender` and the actor thread owns the `Receiver`. If a fully async actor is ever needed, `crossbeam-channel` bridges cleanly into async runtimes via `block_on`.

**Why this enables ublk:** ublk supports multiple queues, each driven by a separate thread. Each queue thread holds a cloned `VolumeHandle`. Reads fan out across queue threads with no contention; writes and flushes serialise through the actor. No `Mutex<Volume>` is needed anywhere.

**Current state (NBD):** the NBD server is single-threaded (one TCP connection). It uses a `VolumeHandle` through a single thread — the concurrency benefit is not yet exercised, but the structure is correct for ublk when that integration is added.

**NBD protocol coverage:** the server implements the fixed newstyle handshake and the following transmission-phase commands:

| Command | Code | Status | Notes |
|---------|------|--------|-------|
| `NBD_CMD_READ` | 0 | Implemented | demand-fetches extents on miss |
| `NBD_CMD_WRITE` | 1 | Implemented | writes to WAL via actor |
| `NBD_CMD_DISC` | 2 | Implemented | clean shutdown |
| `NBD_CMD_FLUSH` | 3 | Implemented | promotes WAL to pending segment |
| `NBD_CMD_TRIM` | 4 | Implemented | zero-extent path; see note below |
| `NBD_CMD_WRITE_ZEROES` | 6 | Implemented | zero-extent path; same as TRIM |
| `NBD_CMD_BLOCK_STATUS` | 7 | Not implemented | allows clients to query allocated vs sparse regions |
| `NBD_CMD_RESIZE` | 8 | Not implemented | live resize; not needed without dynamic sizing |

Transmission flags advertised in the server's handshake:

| Flag | Status | Notes |
|------|--------|-------|
| `NBD_FLAG_HAS_FLAGS` | Advertised | always set per spec |
| `NBD_FLAG_SEND_FLUSH` | Advertised | client may send `NBD_CMD_FLUSH` |
| `NBD_FLAG_READ_ONLY` | Advertised (conditional) | set when `--readonly` is passed |
| `NBD_FLAG_SEND_TRIM` | Advertised | client may send `NBD_CMD_TRIM` |
| `NBD_FLAG_SEND_WRITE_ZEROES` | Advertised | client may send `NBD_CMD_WRITE_ZEROES` |
| `NBD_FLAG_SEND_FUA` | Not advertised | force-unit-access per write not implemented |
| `NBD_FLAG_CAN_MULTI_CONN` | Not advertised | single connection only |

Protocol options not negotiated during the option haggling phase: `NBD_OPT_STRUCTURED_REPLY`, `NBD_OPT_STARTTLS`.

**TRIM and WRITE_ZEROES — zero-extent path:** `NBD_CMD_TRIM` and `NBD_CMD_WRITE_ZEROES` are both implemented via zero extents (see [formats.md](formats.md) — *ZERO record*). When a filesystem (e.g. ext4) frees blocks, it issues TRIM; `NBD_CMD_WRITE_ZEROES` is the write-command equivalent that explicitly requires the range to read back as zeros. Both routes to the same `Volume::write_zeroes()` call, which appends a single ZERO WAL record covering the entire range and inserts a `ZERO_HASH` entry into the LBA map.

Zero extents have no data payload — no hashing, no compression, no body bytes. A whole-device TRIM on a 20 GB volume writes a single ~40-byte WAL record. This is a significant improvement over the earlier approach of writing actual zero-filled extents via the normal write path (which required hashing the full volume worth of zero bytes).

Zero extents in the LBA map explicitly override ancestor data: if a descendant volume trims a range that had data in the ancestor, the zero entry masks it — the read path returns zeros for that range without falling through to the ancestor. This is the key semantic difference from an unwritten LBA range.

The read path checks `hash == ZERO_HASH` before doing any extent index lookup; if the LBA maps to ZERO_HASH, `lba_length × 4096` zero bytes are returned directly.

Sub-block-aligned TRIM and WRITE_ZEROES ranges are rounded inward to fully-covered 4096-byte blocks (ext4 only operates on block boundaries in practice, so this edge case is theoretical). The `NO_HOLE` flag in `WRITE_ZEROES` requests — which asks the server not to punch a hole but to store durable zeros — is satisfied by design: zero extents are WAL-durable and read back as zeros unconditionally.

**GC and zero extents:** zero entries have no body bytes, so they contribute 0 to a segment's stored-data size. However, they carry semantic weight: a live zero entry masks ancestor data. GC cannot use hash-based liveness for zero entries (ZERO_HASH is shared across all zero extents). Instead, a zero entry at `[X, X+N)` is live if the current LBA map maps any part of that range to ZERO_HASH; if it has been fully overwritten with real data, the zero entry is dead and is dropped during compaction. GC may also merge adjacent zero entries into a single entry to reduce index size.

**Share-nothing coordination:** the coordinator and volume share a filesystem layout and a ULID total order, but nothing else — no shared memory, no locks, no clock synchronisation, no protocol negotiation for normal operation. The coordinator reads the fork's on-disk state, extends its timeline by one step, and the volume applies or ignores the result at its own pace. The only real coordination is the `.staged` → bare → deleted handoff, and even that is asynchronous and crash-safe: if the volume never processes a `.staged` file, the worst case is a space leak, not inconsistency. The filesystem directory structure is the entire coordination mechanism — inspectable with standard tools, recoverable without special tooling, and correct by construction from ULID ordering alone.

The volume actor processes `gc/<ulid>.staged` files on its idle tick. For each file it: reads the staged body, walks each input's `index/<input>.idx` to derive the extent-index updates (entries present in input idx and absent from the new segment are removes; entries carried into the new segment update the extent location), updates the in-memory extent index, writes a re-signed copy to `gc/<ulid>.tmp`, renames `<ulid>.tmp → <ulid>` (the atomic commit point), removes `<ulid>.staged`, and evicts `cache/<input>.{body,present}` for each input. All steps are idempotent: re-deriving the action set produces the same updates, re-deleting absent files is harmless, and re-signing produces byte-identical output. No `flush_gen` bump is needed — GC moves data between segment files only, so body offsets remain absolute.

The coordinator processes bare `gc/<ulid>` files at the start of each GC tick (`apply_done_handoffs`). For each it: uploads the body to S3; sends `promote <new-ulid>` IPC to the volume (which writes `index/<new>.idx`, `cache/<new>.{body,present}`, and deletes `index/<input>.idx` for each input — read from the new segment's header); deletes the old S3 objects; sends `finalize_gc_handoff <new-ulid>` IPC to the volume, which deletes the bare body. The coordinator never touches `index/` or `cache/` directly — every mutation under the volume tree goes through the volume actor, serialised with the actor's idle-tick passes. S3 404 on delete is treated as success (idempotent across coordinator crashes).

**`index/` and `cache/` are fully volume-owned.** The coordinator never reads or writes `index/`. The coordinator never writes or deletes `cache/`. The volume is the sole authority on its LBA index and body cache lifecycle. Demand-fetch, eviction, and GC cache cleanup all work correctly whether or not the coordinator is running.

**Race tolerance:** LBA map and extent index rebuild (`Volume::open`) collect `index/*.idx` paths and then read each one. If the coordinator deletes an index file during a GC cleanup pass between path collection and the read, the rebuild skips the missing file with a warning rather than failing. This is always correct: the new compacted segment (with a higher ULID) has its own `index/<new>.idx` and its entries will overwrite whatever the deleted file would have contributed. The same tolerance is applied in `repack()` when it scans `index/`.

**The `.staged` file is written atomically** (`<ulid>.staged.tmp` then rename) to prevent a coordinator crash from leaving a partial handoff. Crash recovery is content-resolved: stale `.tmp` and `.staged.tmp` files are swept on the next apply pass, and a partially re-signed `<ulid>.tmp` from a volume-side crash is regenerated byte-identically because the apply path is deterministic (see `docs/design-gc-self-describing-handoff.md` for the full crash table).

**Tombstone and removal-only handoffs collapse into one shape.** When all extents in GC candidates are truly extent-dead, or when only extent-index references need cleanup with no live LBAs, the coordinator still writes a real GC output via `write_gc_segment` — a zero-entry segment with a non-empty `inputs` list. `promote_segment` recognises zero-entry GC outputs and skips writing `index/<new>.idx` and `cache/<new>.body` entirely, only deleting the input idx files. The bare `gc/<new>` body is then deleted via `finalize_gc_handoff` the same way as a live output. Direct coordinator deletion without staging a handoff is never safe: the coordinator's liveness view is built from on-disk index files and may be behind the volume's in-memory WAL.

### Operations

Implemented:

```
elide serve-volume <vol-dir> [--readonly]      # serve a volume over NBD

elide snapshot-volume <vol-dir>                # checkpoint a volume; stays live

elide fork-volume <vol-dir> <new-vol-dir>      # create a new volume branched from the
                                                # latest snapshot of <vol-dir>

elide inspect-volume <vol-dir>                 # human-readable summary
```

Not yet implemented:

```
elide list-snapshots <vol-dir>
elide export-volume <src-vol-dir> <new-vol-dir>
```

Import is handled by `elide volume import <name> <oci-ref>`, which asks the coordinator to spawn the separate `elide-import` binary as a supervised short-lived process. See *Import process lifecycle* above.

**Snapshot procedure:** snapshot is a coordinator-orchestrated sequence, not an in-process volume call. The CLI (`elide volume snapshot`) sends `snapshot <name>` to the coordinator, which acquires the per-volume snapshot lock (mutual exclusion against drain/GC/eviction on that volume's tick) and runs: (1) `flush` IPC to the volume — WAL becomes a segment in `pending/`; (2) inline drain — upload each `pending/` segment to S3 and `promote` it so the volume writes `index/<ulid>.idx` and `cache/<ulid>.{body,present}`; (3) pick `snap_ulid` as the max ULID in `index/`; (4) `sign_snapshot_manifest` IPC to the volume — the volume enumerates its own `index/`, writes a signed `snapshots/<snap_ulid>.manifest` listing every segment ULID, then writes the `snapshots/<snap_ulid>` marker; (4b) regenerate `snapshots/<snap_ulid>.filemap` by walking ext4 metadata at the sealed snapshot via `BlockReader::open_snapshot` (no body reads — hashes come from the LBA map), non-ext4 volumes and parse failures skip cleanly; (5) upload manifest, marker, and filemap to S3; release the lock. The marker is written last by the volume, so a partial sequence leaves no visible snapshot. Keys never leave the volume process — signing stays inside the actor. If no new segments have been committed since the latest snapshot, the operation is idempotent and returns the existing ULID.

**Import procedure:** the import path writes data directly into `<vol-dir>/pending/`, bypassing the WAL entirely, since there is no ongoing VM I/O. At the end of import, a snapshot marker `snapshots/<import-ulid>` is written; this ULID matches the last segment written. It serves as the branch point for any volumes forked from this one. The `volume.size` marker and `manifest.toml` (OCI source metadata) are written into the volume directory.

The import process then enters its serve phase: it binds `control.sock` and handles `promote <ulid>` IPC from the coordinator. Each promote call causes the import to write `index/<ulid>.idx` and `cache/<ulid>.{body,present}`, then remove `pending/<ulid>`. This keeps the same ownership boundary as writable volumes: the process that controls the directory performs the `pending/ → index/ + cache/` transition in response to coordinator IPC. The import exits when `pending/` is empty.

**S3 upload for volume metadata:** at import, fork, and create time, two objects are written to the store eagerly: `names/<name>` (contains the ULID, plain text) and `by_id/<ulid>/manifest.toml` (name, size, origin, source metadata). Snapshot markers are uploaded as empty objects at `by_id/<ulid>/snapshots/YYYYMMDD/<snapshot-ulid>` after each `volume snapshot` and at the end of import. Together these allow any host to reconstruct the full volume ancestry skeleton with O(depth) GETs before segment index prefetch begins. See *S3 object layout* in `docs/formats.md` for the full key structure.

**Implicit snapshot rule:** `fork-volume` and `export-volume` always take an implicit snapshot of the source volume. If a snapshot already exists at the tip, `fork-volume` uses the latest existing snapshot marker rather than creating a duplicate.

### Base directory defaulting

A single directory governs all state:

- **`--data-dir`** (env `ELIDE_DATA_DIR`, default `./elide_data`) — everything: `by_id/`, `by_name/`, `control.sock`, `coordinator.toml`. Used by the volume process, coordinator, and CLI.

Commands that accept `<vol-dir>` resolve volume names via `<data-dir>/by_name/<name>`.

### Compaction

Compaction reclaims space in a fork by rewriting segments that contain a high proportion of overwritten (dead) data. The compaction algorithm:

1. Compute the live hash set from the fork's current LBA map.
2. Determine the **compaction floor** = max ULID across all files in `snapshots/` (none if no snapshots exist).
3. For each segment in `pending/` and `index/` (via `index/*.idx`) with ULID **> floor**:
   - Count total and live bytes (DATA entries only; DEDUP_REF entries have no body bytes).
   - If `live_bytes / total_bytes ≥ min_live_ratio`, skip.
   - Otherwise: read live entries' bodies (from `pending/<ulid>` or `cache/<ulid>.body`), write a new denser segment to `pending/<new-ulid>`, update the extent index, delete the old segment from `pending/` (or coordinate deletion of the old `index/<ulid>.idx` and `cache/<ulid>.*` via the coordinator).
4. Segments with ULID **≤ floor** are never touched — they are frozen by the latest snapshot.

The floor ensures segments readable by child forks are never modified or deleted. Any fork that branched from this fork at snapshot ULID S uses ancestor segments with ULID ≤ S ≤ floor. Repacked segments always receive new (higher) ULIDs, landing above the floor — no existing child fork's ancestry walk will include them.

`pending/` and `index/` encode S3 upload status, not GC eligibility. The compaction floor applies to both. In practice, a snapshot is always taken against a segment that is still in `pending/` (the WAL flush lands there; promotion to `index/` + `cache/` happens asynchronously at S3 upload). The ULID comparison is directory-agnostic: a segment's ULID is assigned at creation and remains stable across the `pending/` → `index/` transition, so the floor check remains correct regardless of which directory the segment currently lives in.

The `repack` CLI command triggers repacking with a configurable `--min-live-ratio` threshold (default 0.7).

### Open questions

1. **Rollback within a fork.** Not yet designed. Two candidate approaches: (a) discard segments and WAL above target snapshot ULID in-place; (b) fork from the target snapshot and rename.

### `fetch_extent` durability ordering

The demand-fetch path in `elide-fetch::fetch_one_extent` maintains the invariant "a set present bit implies the body bytes are durable". To enforce it, the fetch writes `cache/<id>.body` and then publishes the present bit in strict order:

1. `write_all` body bytes at `batch_body_start` into `cache/<id>.body`.
2. `sync_data()` the `.body` file handle — fsync barrier before any bit is published.
3. If the `.body` file was newly created by this fetch, fsync `cache/` so the directory entry itself is durable.
4. Build the updated present bitset and write it via `segment::write_file_atomic` (tmp + `sync_data` + rename + parent-dir fsync).

The reverse reordering — present bit durable, body bytes still in the page cache — would surface after a crash as a silent read of zeros, because the LBA map would treat the extent as cached and skip refetching. The fsync at step 2 is the correctness barrier; without it, the filesystem is free to commit `.present` first regardless of syscall order. Routing `.present` through `write_file_atomic` additionally prevents a torn bitset write from losing or inventing bits.

The opposite failure — body bytes durable, present bit not yet published — is harmless: the next read redundantly refetches and rewrites the same bytes idempotently.
