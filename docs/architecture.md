# Architecture

## Design principle: the volume is the primitive

A volume process is **self-contained and fully functional on its own** when all its data is present locally. Local storage (WAL + segments on NVMe) is a complete and correct deployment — not a degraded or temporary state. This must remain true as the system grows: nothing added to the coordinator should become a correctness dependency for the volume.

**Caveat — demand-fetched volumes:** a volume that was started from a snapshot pulled from S3 (rather than built entirely from local writes) holds only the extents that have been accessed so far. Unaccessed extents still live in S3. Such a volume requires S3 reachability to serve reads for data it hasn't yet fetched; it is not fully self-sufficient until all referenced extents are local. This is intentional and expected — demand-fetch is a core feature, not a degraded state — but it means "self-contained" applies fully only to volumes that originated locally or have been fully warmed.

The coordinator and S3 are **strictly additive** for locally-originated volumes:
- Without coordinator: volumes run indefinitely on local storage; `pending/` accumulates but I/O is always correct
- With coordinator: GC reclaims space, S3 provides durability and capacity beyond local NVMe
- With coordinator + S3: full production deployment

This layering also means a single volume process can be started standalone for development, testing, or debugging with no service scaffolding required.

## Components

A single **Elide coordinator** runs on each host and manages all volumes. It forks one child process per volume — the process boundary is deliberate: a fault in one volume's I/O path cannot corrupt another, and the boundary forces the inter-component interface to be explicit and real (filesystem layout, IPC protocol, GC ownership) rather than loose in-process coupling.

**Coordinator (main process)** — spawns and supervises volume processes; owns all S3 mutations (upload, delete, segment GC rewrites); watches one or more configured volume root directories and discovers forks automatically; handles `prefetch-indexes` for forks cold-starting from S3.

**Volume process** (one per volume) — owns the ublk/NBD frontend for one volume; owns the WAL and pending promotion for that volume; holds the live LBA map in memory; runs background `pending/` compaction and `fetched/` promotion in the idle arm. Does not communicate with other volume processes directly. Communicates with the coordinator via a defined IPC boundary (TBD — Unix socket or similar). Never requires the coordinator for correct I/O.

**S3 credential split:** the volume process requires only **read-only** S3 credentials (for demand-fetch). All S3 mutations — segment upload, segment delete, GC rewrites — are performed exclusively by the coordinator, which holds read-write credentials. This limits the blast radius if a volume host is compromised.

Proposed: this split is the design target. Currently the volume holds full S3 credentials for demand-fetch (via `object_store` in `elide/src/fetcher.rs`). Read-only credential enforcement is deferred until the coordinator daemon is built out.

## Crate structure

The repository is a Cargo workspace with four crates:

```
elide-core/        — shared library: segment format, WAL, LBA map, extent index,
                     volume read/write, import_image(), and Ed25519 signing.
                     Deps: blake3, zstd, ulid, nix, ed25519-dalek, rand_core.
                     No async, no network. Usable standalone.

elide/             — volume process binary: NBD server, analysis tools (extents,
                     inspect, ls), and the import-volume CLI subcommand. Adds:
                     clap, ext4-view, object_store, tokio (rt-multi-thread only).
                     The async runtime is used exclusively by the demand-fetch
                     path (ObjectStoreFetcher), which wraps block_on to satisfy
                     the sync SegmentFetcher interface. NBD I/O remains synchronous.

elide-import/      — OCI import binary: pulls public OCI images from a container
                     registry, extracts a rootfs, converts to ext4, and calls
                     elide_core::import::import_image to ingest. Adds: tokio,
                     oci-client, ocirender. Heavy async deps isolated here.

elide-coordinator/ — coordinator daemon: watches configured volume root
                     directories; discovers forks; drains pending/ to S3;
                     runs segment GC; prefetches indexes for cold forks.
                     Adds: tokio, object_store (S3 and local filesystem
                     backends), nix (process supervision). Holds read-write
                     S3 credentials; volume holds read-only credentials only.
```

The split keeps the volume process binary lean and focused. The async HTTP stack
needed for OCI registry pulls belongs in tooling (`elide-import`), not in the
process that serves block I/O. As the coordinator is built out, OCI import
functionality will likely migrate there, with `elide-import` becoming a thin CLI
wrapper around coordinator APIs.

## Directory layout

All volume state lives under a shared root directory on a dedicated local NVMe mount. A **volume** is a named directory containing metadata files and a `forks/` subdirectory. Named forks live exclusively under `forks/` — this keeps fork directories cleanly separated from volume-level metadata. Each fork is a named subdirectory under `forks/` that maintains its own WAL, segments, and snapshot history. A fork with `wal/` present is live (writable); a fork without `wal/` is frozen or not yet started.

**Writable volume:**

```
/var/lib/elide/volumes/
  myvm/                          — volume "myvm"
    size                         — volume size in bytes (plain text)
    forks/
      default/                   — default fork (live)
        wal/                     — present = live; write target
        pending/                 — segments awaiting promotion
        segments/                — committed segment files
        snapshots/
          <ulid-1>               — marker file (empty, or optional name)
          <ulid-2>
      dev/                       — named fork branched from default
        wal/
        pending/
        segments/
        origin                   — text: "forks/<fork-name>/snapshots/<ulid>" (branch point)
        snapshots/
          <ulid-3>
  service.sock                   — Unix socket at a stable, known path
```

**Readonly volume** (a template; no forks until explicitly forked):

```
  ubuntu-22.04/
    meta.toml                    — source metadata (OCI digest, arch, readonly = true)
    size
    base/                        — frozen base populated by import (no wal/)
      segments/
      snapshots/
        <import-ulid>
    forks/                       — absent on fresh import; created on first fork
      server-1/                  — named fork branched from base (live)
        wal/
        pending/
        segments/
        origin                   — "base/snapshots/<import-ulid>"
        snapshots/
```

The readonly volume's `base/` directory lives directly under the volume root (not under `forks/`) and holds the frozen segments and snapshot markers from the import. It is not a user fork and is not writable. All user-created forks live under `forks/`. `forks/` is absent on a freshly-imported readonly volume and is created automatically when the first fork is taken.

**Invariants:**
- `wal/` present → fork is live; exactly one process writes here (enforced by `volume.lock`)
- `wal/` absent → fork is frozen or not yet started; cannot be served writably
- `origin` is present only on forks branched from another fork; its value is a path relative to the volume root: `base/snapshots/<ulid>` for forks from the import base, or `forks/<name>/snapshots/<ulid>` for forks from a user fork
- `snapshots/<ulid>` is a plain marker file; the ULID sorts after all segments present at snapshot time, giving a stable branch point
- `meta.toml` at the volume root records source metadata; `readonly = true` in this file marks the volume as a template — it cannot be served writably and requires an explicit fork before use
- `base/` at the volume root is the frozen import data; it is not a user fork and never has a `wal/`
- `forks/` is the exclusive home for named user forks; no fork directories appear directly in the volume root except `base/`
- `children/` is not used; forks are named siblings, not anonymous `children/<ulid>/` descendants

**Finding live forks:** scan `forks/` for subdirectories containing `wal/`.

**Exclusive access:** a live fork holds an exclusive `flock` on `<fork-dir>/volume.lock` for the lifetime of its volume process. Attempting to open an already-locked fork fails immediately.

**Fork ancestry:** a fork's `origin` file names its parent fork and the branch-point snapshot ULID. For forks in `forks/`, `walk_ancestors` follows this chain to the root (`base/`), building an oldest-first list of ancestor layers. Segments in each ancestor fork are included only up to the branch-point ULID — post-branch writes to an ancestor fork are not visible in derived forks.

```
VM
 │  block I/O (ublk / NBD)
 ▼
Volume process  (one per volume)
 │  write path: buffer → extent boundary → hash → local dedup check → WAL append
 │  read path:  LBA → LBA map → extent index → segment file (pending/ · segments/ · fetched/ · S3)
 │
 ├─ WAL  (wal/<ULID>)
 ├─ Pending segments  (pending/<ULID>)
 ├─ Live LBA map  (in memory, LBA → hash; merged from own + ancestor layers)
 └─ IPC  (service.sock — optional for I/O, used for coordination)
      │
      ▼
Coordinator (main process)
 ├─ Volume supervisor    (spawn/re-adopt volume processes)
 ├─ Fork watcher         (inotify on configured root dirs; discovers new forks/volumes)
 ├─ S3 uploader          (drains pending/ → S3; async, not on write critical path)
 ├─ Segment GC           (coordinator-driven; reads LBA map from indexes; writes gc/ result files)
 └─ prefetch-indexes     (downloads .idx files for cold-start forks)
```

## Coordinator restartability

Volume processes are **detached** from the coordinator at spawn time (`setsid` / new session) so they are not in the coordinator's process group and are not signalled when it exits. The coordinator can be stopped, upgraded, or restarted without interrupting running volumes.

**Re-adoption on coordinator start:** when the coordinator starts, it scans for `wal/` directories and checks whether each has a running process (via a `volume.pid` file alongside `wal/`). Volumes with a live process are re-adopted. Volumes with no running process are started fresh and recover from their WAL as normal.

**IPC is reconnectable:** volume processes handle `service.sock` disappearing and attempt reconnection when it reappears. The IPC channel carries coordination traffic only (GC notifications, S3 upload confirmations) — loss of the channel degrades background efficiency but never affects correctness or I/O availability.

## Write Path

```
1. VM issues write for LBA range
2. Buffer contiguous writes; each non-contiguous LBA gap finalises an extent
3. For each extent:
   a. Hash extent content → extent_hash
   b. Check local extent index (own segments + all ancestor segments + other volumes) for extent_hash
      - Found  → write REF record to WAL (no data payload)
      - Not found → write DATA record to WAL (fsync = durable)
4. Update live LBA map with new (start_LBA, length, extent_hash) entries
```

Durability is at the write log. S3 upload is asynchronous and not on the critical path.

**WAL ULID assignment.** The active WAL file is named `wal/<ULID>` where the ULID is assigned **at WAL creation**, not at flush time. This is a deliberate design choice: when the WAL is promoted to `pending/<ULID>`, the segment inherits the same ULID as the WAL file. If a crash occurs after `pending/<ULID>` is written but before the WAL file is deleted, recovery detects the situation by checking whether `pending/<ulid>` already exists alongside `wal/<ulid>` — if so, the WAL was already flushed and the stale WAL file is discarded. This linkage requires the ULID to be fixed at creation time; assigning it at flush time (as the lsvd reference implementation does) would eliminate the shared-name detection mechanism and require a separate marker or header field to handle the crash edge case.

The consequence of pre-assignment is that `compact_pending` — which may run while a WAL is already open — cannot use `mint.next()` for its output ULID. `mint.next()` would produce a ULID higher than the current WAL's pre-assigned ULID; the compaction output would then sort after the WAL's eventual flush segment during rebuild, silently overwriting newer data with stale compacted data. The fix: `compact_pending` uses `max(candidate_ULIDs)` as its output ULID, which is always below the current WAL's ULID because all `pending/` candidates were created by prior WAL flushes. This also makes the causal ordering explicit — the compaction output ULID is derived from its inputs, not from the clock. The proptest suite found and verified this invariant; see [testing.md](testing.md).

**Dedup is local and opportunistic.** The write path checks the local extent index before writing data. If the extent already exists anywhere in the local tree (or, best-effort, in another volume on the same host), a REF record is written instead — no data is stored again. See the Dedup section below for full scope details.

**No delta compression locally.** Delta compression is computed at S3 upload time and exists in S3 only. Local segment bodies contain either the full extent data (DATA records) or nothing (REF records, where the data already lives in an ancestor segment and is not duplicated). On S3 fetch, deltas are applied and the full extent is materialised locally before being cached and served to the VM.

## Read Path

```
1. VM reads LBA range
2. Look up LBA in live LBA map → extent_hash H
3. Check local segments (own pending/ + segments/, then ancestor segments/) for H
   - Hit  → return data (decompress if FLAG_COMPRESSED)
   - Miss → check fetched/<ulid>.body (body-relative offsets; .present bitset guards each extent)
     - Hit  → read from body file; decompress if needed
     - Miss → look up H in extent index → (segment_id, body_offset, body_length)
4. Issue a byte-range GET to S3 covering a chunk of the segment body
   - The fetch unit is a contiguous byte range (e.g. 1MB-aligned chunk) that
     includes the needed extent(s) plus neighbours for spatial locality
   - The segment index section encodes body_offset + body_length per extent,
     so the chunk boundaries can be derived precisely
   - If a delta body is available and smaller, fetch from the delta instead
5. Write fetched bytes to fetched/<ulid>.body; set bit in .present; decompress and return to VM
```

The kernel page cache sits above the block device and handles most hot reads. The local segment cache handles warm reads. S3 is the cold path.

**Local read path implementation.** The in-process read path resolves each LBA range as follows: LBA map BTreeMap range scan → per-extent HashMap lookup in the extent index → seek + read from the segment file. A single file-handle cache avoids repeated `open` syscalls for sequential extents within the same segment; a cache miss incurs one `open` plus O(ancestor_depth) `stat` calls to locate the segment across the directory tree. Compressed extents are decompressed in full on every read — there is no sub-extent decompression granularity (same model as lsvd). Both overheads are bounded in practice by the segment size limit and the pre-log coalescing block limit.

**Demand-fetch is at extent granularity, not segment granularity.** A segment file on S3 may contain hundreds of extents. Only the specific extent needed for a given read is fetched — the rest of the segment is never downloaded unless separately requested. This is the same design as the lab47/lsvd reference implementation, which issues byte-range `GetObject` requests for chunk-sized slices of segment bodies (1MB chunks, LRU-cached locally) and never downloads entire segment files. In practice, 93.9% of a 2.1GB Ubuntu cloud image is never read during a typical systemd boot — meaning 93.9% of S3 segment data is never fetched.

## LBA Map

The **LBA map** is the live in-memory data structure mapping logical block addresses to content. It is a sorted structure (B-tree or equivalent) keyed by `start_LBA`, where each entry holds `(start_lba, lba_length, extent_hash)`. It is updated on every write (new entries added, existing entries trimmed or replaced for overwrites) and is the authoritative source for read path lookups.

**Contrast with lab47/lsvd:** the reference implementation calls this `lba2pba` and maps `LBA → segment+offset` (physical location). GC repacking must update it for every moved extent. Palimpsest maps `LBA → hash` — the logical layer. Physical location (`hash → segment+offset`) is a separate extent index. This two-level indirection means GC repacking updates only the extent index; the LBA map is never rewritten for GC.

**Layer merging:** a live volume's LBA map is the union of its own data and all ancestor layers. At startup, layers are merged oldest-first (root ancestor first, live node last), so later writes shadow earlier ones. This is the same model as the lsvd `lowers` array, encoded in the directory tree.

### LBA map persistence

The LBA map is optionally persisted to a local `lba.map` file on clean shutdown and used as a fast-start cache on restart.

**Freshness guard:** the file includes a BLAKE3 hash of the sorted list of all current local segment IDs (own + ancestors). On startup, if the guard matches the current segment list, the cached LBA map is loaded directly without scanning segment index sections. If the guard doesn't match (new segments were written, or ancestry changed), the LBA map is rebuilt from scratch.

**Rebuild procedure:**
1. Follow the `origin` chain from the fork to the root, collecting ancestor layers (oldest first)
2. For each ancestor fork: scan `segments/` and `pending/` in ULID order, stopping at the branch-point ULID stored in the child's `origin`
3. For the current fork: scan all of `segments/` and `pending/`
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

One entry per extent. Unwritten LBA ranges have no entry (implicitly zero).

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

**Exact dedup:** two extents with the same BLAKE3 hash are identical. Dedup is detected and applied **opportunistically on the write path**: before writing a DATA record to the WAL, the extent hash is checked against the local extent index. If a match is found, a REF record is written instead — no data payload, just a reference to the existing extent.

Dedup scope is **all volumes on the local host**. The extent index covers the current volume's own tree (own + ancestor segments) plus, on a best-effort basis, all other volumes stored under the same common root. No remote or cross-host dedup check is performed. Dedup quality is highest for snapshot-derived volumes (ancestor segments already contain most of the data) and lower for freshly provisioned volumes; cross-volume dedup raises quality for volumes that share a common base image even without a snapshot relationship.

**Delta compression** is a separate concern from dedup and is **S3-only**. Local segment bodies never contain delta records — an entry in a local segment is either a full extent (DATA record, data present in body) or a reference (REF record, no data in this segment's body, data lives in an ancestor segment). At S3 upload time, extents that differ only slightly from extents in ancestor segments are stored as deltas in the delta body section of the S3 segment file (see [formats.md](formats.md)). The benefit is reduced S3 fetch size, not local storage cost. On fetch, the delta is applied and the full extent is materialised locally before being cached and served.

**Delta source selection** is trivial at the extent level: the natural reference for a changed file is the same-path file in the parent snapshot. The snapshot parent chain gives direct access to the prior version of each extent.

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
| **Volume** | A named collection of forks stored under a common base directory. Identified by name (e.g. `myvm`). |
| **Fork** | A named, independently live line of work within a volume. A fork has its own WAL, pending segments, and checkpoint history. Names are unique within a volume. User forks live under `forks/`. |
| **Snapshot** | A marker file (`snapshots/<ulid>`) recording a point in a fork's committed segment sequence. The ULID gives the position: all segments with ULID ≤ the snapshot ULID are part of that snapshot. The latest snapshot ULID also serves as the **compaction floor** — segments at or below it are frozen and will never be compacted. The file content is empty or an optional human-readable name. |
| **Readonly volume** | A volume with `readonly = true` in its `meta.toml`. It is a template: its `base/` directory holds the frozen data populated by import (no `wal/`). It cannot be served directly. Named forks are created under `forks/` and are fully writable. `forks/` is absent until the first fork is taken. |
| **Export** | A squash-and-detach operation that produces a new self-contained volume from a fork, with no ancestry dependencies. |

### Ancestry walk

To rebuild the LBA map for a fork:
1. Follow the `origin` chain to the root fork (no `origin` file).
2. From the root fork outward, for each ancestor fork in the chain: scan both `segments/` and `pending/` in ULID order (merged), stopping at (and including) the branch-point ULID recorded in the child fork's `origin`. Snapshot markers are not scanned during replay.
3. Apply the current fork's own `segments/` and `pending/` in ULID order (all of them).
4. Replay the WAL.

The per-ancestor ULID cutoff is what prevents a concurrently-written ancestor fork from leaking newer data into the derived fork's view.

### Single-writer invariant

**Each fork directory has exactly one process that writes new segments into it.** The volume process that holds `fork.key` is the sole writer of `pending/` and `segments/` for that fork. The coordinator writes only to `gc/` and promotes results into `segments/` after S3 confirmation. Crucially, it derives the output ULID from the fork's existing write history (`max(input ULIDs).increment()`) rather than from its own wall clock. The coordinator does not author an independent position in the fork's timeline — it extends the sequence by one step from where the volume left off.

This invariant is what makes ULID total-order sufficient for all correctness guarantees in rebuild, GC, and ancestor cutoff:

- **Rebuild:** processing segments in ULID order is unambiguous — there is no external writer that could inject a segment with an arbitrary timestamp into the middle of the sequence.
- **GC ULID assignment:** `max(inputs).increment()` is safe because any write that occurs *during* compaction comes from the one writer, gets a timestamp from the current wall clock, and is therefore far ahead of the old `max(inputs)` timestamp (which has already passed through the drain pipeline). No locking is required.
- **Ancestor cutoff:** the branch-point ULID is a stable boundary because the ancestor's writer cannot insert segments retroactively below it.

The single-writer property is enforced by the signing key: only the host holding `fork.key` can produce valid segment signatures. An attempt to inject a segment from another host is detected at demand-fetch verification time. See [formats.md](formats.md) — *Fork ownership and signing*.

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
- `Write`, `Flush`, `CompactPending` — sent through the channel with an attached `crossbeam_channel::Sender` for the response. The actor processes them in arrival order and replies when done.
- `Read` — the calling thread loads the current snapshot via `ArcSwap::load()` and resolves the request entirely on that thread. No channel round-trip; no contention with the actor.

**Read-your-writes:** the snapshot is published after every `write()` call, before the response is sent back to the caller. Any read issued after a write has returned will see that write, regardless of whether it has been flushed to a `pending/` segment — the same guarantee a physical disk provides.

**WAL promotion (decoupled from writes):** `Volume::write()` only appends to the WAL and updates the in-memory maps — it never touches the segment layer. The actor is responsible for promoting the WAL to a `pending/` segment via two mechanisms:

1. **Threshold-triggered:** after sending the write reply, the actor checks `Volume::needs_promote()`. If the WAL has reached the 32 MiB soft cap, it calls `flush_wal()` immediately — before processing the next queued message. The write caller is already unblocked; the cost is borne by the next message in the queue.

2. **Idle-flush tick:** the actor run loop uses `crossbeam_channel::tick(10s)` alongside the request channel. When the tick fires and the WAL is non-empty, `flush_wal()` is called. This ensures data is promoted even under low or zero write load. The interval is 10 seconds (chosen for observability during development; tightening it later is a one-line change).

Background promotes that fail (I/O error, disk full) are logged and do not crash the actor — the data is safe in the WAL. The next explicit `Flush` or threshold-triggered promote will surface the error.

**Why `crossbeam-channel`:** the actor loop and NBD/ublk handlers are synchronous threads; `crossbeam-channel` is a natural fit. When ublk integration uses io_uring, ublk queue threads remain synchronous callers — they block on the `Sender` and the actor thread owns the `Receiver`. If a fully async actor is ever needed, `crossbeam-channel` bridges cleanly into async runtimes via `block_on`.

**Why this enables ublk:** ublk supports multiple queues, each driven by a separate thread. Each queue thread holds a cloned `VolumeHandle`. Reads fan out across queue threads with no contention; writes and flushes serialise through the actor. No `Mutex<Volume>` is needed anywhere.

**Current state (NBD):** the NBD server is single-threaded (one TCP connection). It uses a `VolumeHandle` through a single thread — the concurrency benefit is not yet exercised, but the structure is correct for ublk when that integration is added.

**Share-nothing coordination:** the coordinator and volume share a filesystem layout and a ULID total order, but nothing else — no shared memory, no locks, no clock synchronisation, no protocol negotiation for normal operation. The coordinator reads the fork's on-disk state, extends its timeline by one step, and the volume applies or ignores the result at its own pace. The only real coordination is the `.pending` → `.applied` handoff, and even that is asynchronous and crash-safe: if the volume never processes it, the worst case is a space leak, not inconsistency. The filesystem directory structure is the entire coordination mechanism — inspectable with standard tools, recoverable without special tooling, and correct by construction from ULID ordering alone.

### Operations

Implemented:

```
elide serve-volume <vol-dir|fork-dir> [--readonly]
                                                # serve a fork over NBD; accepts vol-dir
                                                # (uses forks/default) or fork-dir directly

elide snapshot-volume <vol-dir|fork-dir>       # checkpoint a fork; fork stays live

elide fork-volume <vol-dir> <fork-name> [--from <source-fork>]
                                                # create forks/<fork-name> branched from the
                                                # latest snapshot of source fork (default: "base")

elide list-forks <vol-dir|fork-dir>            # list all named forks under forks/

elide inspect-volume <vol-dir|fork-dir>        # human-readable summary; accepts either form
```

Not yet implemented:

```
elide create-readonly-volume <vol-dir> --size <size>
elide list-snapshots <vol-dir> [--fork <name>]
elide export-volume <vol-dir> <fork-name> <new-vol-dir>
```

Import is handled by the separate `elide-import` binary (OCI pull → ext4 → volume ingest). There is no `elide import-volume` subcommand.

**Snapshot procedure:** `snapshot-volume` flushes the WAL (producing a segment in `pending/` if there are unflushed writes), then writes a new `snapshots/<ulid>` marker file. The snapshot ULID matches the ULID of the last committed segment, making the branch point self-describing. If no new segments have been committed since the latest existing snapshot, the operation is idempotent — it returns the existing snapshot ULID without writing a new marker. The fork remains live; no directory structure changes.

**Import procedure for readonly volumes:** the import path writes data directly into `base/segments/`, bypassing the WAL entirely, since there is no ongoing VM I/O. At the end of import, a snapshot marker `base/snapshots/<import-ulid>` is written; this ULID matches the last segment written. It is used as the branch point by all forks created from this volume. The `size` marker and `meta.toml` (containing `readonly = true` plus OCI source metadata) are written at the volume root. No `forks/` directory is created — it is created automatically when the first fork is taken via `fork-volume`.

**S3 upload for snapshots:** snapshot marker files and `origin` files must also be uploaded to S3 so the fork tree structure is visible to other hosts. These are small and should be uploaded eagerly.

**Implicit snapshot rule:** `fork-volume` and `export-volume` always take an implicit snapshot of the source fork. For a readonly volume's `base/` directory (which already has a snapshot from import), a new snapshot is not needed — `fork-volume` uses the latest existing snapshot marker from `base/snapshots/`.

### Base directory defaulting

`<base>` defaults to `ELIDE_VOLUMES_DIR` if set, otherwise `~/.local/share/elide/volumes`. Commands that accept `<vol-dir>` use this default unless an explicit path is given.

### Compaction

Compaction reclaims space in a fork by rewriting segments that contain a high proportion of overwritten (dead) data. The compaction algorithm:

1. Compute the live hash set from the fork's current LBA map.
2. Determine the **compaction floor** = max ULID across all files in `snapshots/` (none if no snapshots exist).
3. For each segment in `pending/` and `segments/` with ULID **> floor**:
   - Count total and live bytes (DATA entries only; DEDUP_REF entries have no body bytes).
   - If `live_bytes / total_bytes ≥ min_live_ratio`, skip.
   - Otherwise: read live entries' bodies, write a new denser segment to `pending/<new-ulid>`, update the extent index, delete the old segment.
4. Segments with ULID **≤ floor** are never touched — they are frozen by the latest snapshot.

The floor ensures segments readable by child forks are never modified or deleted. Any fork that branched from this fork at snapshot ULID S uses ancestor segments with ULID ≤ S ≤ floor. Repacked segments always receive new (higher) ULIDs, landing above the floor — no existing child fork's ancestry walk will include them.

`pending/` and `segments/` encode S3 upload status, not GC eligibility. The compaction floor applies to both. In practice, a snapshot is always taken against a segment that is still in `pending/` (the WAL flush lands there; promotion to `segments/` happens asynchronously at S3 upload). The ULID comparison is directory-agnostic: a frozen segment retains its ULID when promoted, so the floor check remains correct regardless of which directory the segment currently lives in.

The `compact-volume` CLI command triggers compaction with a configurable `--min-live-ratio` threshold (default 0.7).

### Open questions

1. **Rollback within a fork.** Not yet designed. Two candidate approaches: (a) discard segments and WAL above target snapshot ULID in-place; (b) fork from the target snapshot and rename.
