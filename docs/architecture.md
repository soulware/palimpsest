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

A single **palimpsest coordinator** runs on each host and manages all volumes. It forks one child process per volume — the process boundary is deliberate: a fault in one volume's I/O path cannot corrupt another, and the boundary forces the inter-component interface to be explicit and real (filesystem layout, IPC protocol, GC ownership) rather than loose in-process coupling.

**Coordinator (main process)** — spawns and supervises volume processes; owns GC (runs as a coordinator-level task with access to all volumes' on-disk state); handles S3 upload/download.

**Volume process** (one per volume) — owns the ublk/NBD frontend for one volume; owns the WAL and pending promotion for that volume; holds the live LBA map in memory. Does not communicate with other volume processes directly. Communicates with the coordinator via a defined IPC boundary (TBD — Unix socket or similar). Never requires the coordinator for correct I/O.

## Directory layout

All volume state lives under a shared root directory on a dedicated local NVMe mount. **The directory tree is the snapshot tree**: each node is a volume state at a point in time. A node containing `wal/` is a live (writable) leaf. A node without `wal/` is frozen (read-only). The parent chain is the directory ancestry — no manifest is needed to traverse it.

```
/var/lib/palimpsest/
  volumes/
    <volume-id>/                  — root node of a volume tree
      segments/                   — frozen after first snapshot
      <snap-ulid>/                — child node (snapshot or fork)
        segments/                 — frozen after next snapshot
        <snap-ulid>/              — grandchild node
          segments/
          wal/                    — live leaf: this is the current write target
          pending/
        <fork-ulid>/              — another live fork from the same parent
          segments/
          wal/
          pending/
  service.sock                    — Unix socket at a stable, known path
```

**Invariants:**
- `wal/` present → live leaf; the volume process writes here
- `wal/` absent → frozen; contents are immutable
- `pending/` always accompanies `wal/`
- All ancestor nodes of a live leaf are frozen and shared across all sibling forks; GC must not modify them

**Finding live volumes:** scan for directories containing `wal/`. Each such directory is an independently running volume process.

**Finding a volume's ancestry:** walk up the directory tree from the live leaf to the root. Each parent directory is a frozen snapshot layer; its `segments/` contribute to the LBA map via layer merging (ancestors first, descendants shadow).

```
VM
 │  block I/O (ublk / NBD)
 ▼
Volume process  (one per volume)
 │  write path: buffer → extent boundary → hash → local dedup check → WAL append
 │  read path:  LBA → LBA map → extent index → segment file (local or S3)
 │
 ├─ WAL  (wal/<ULID>)
 ├─ Pending segments  (pending/<ULID>)
 ├─ Live LBA map  (in memory, LBA → hash; merged from own + ancestor layers)
 └─ IPC  (service.sock — optional for I/O, used for coordination)
      │
      ▼
Coordinator (main process)
 ├─ Volume supervisor  (spawn/re-adopt volume processes)
 ├─ GC / segment packer  (compacts live leaf segments; never touches frozen ancestors)
 └─ S3 uploader  (async, not on write critical path)
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

**Dedup is local and opportunistic.** The write path checks the local extent index before writing data. If the extent already exists anywhere in the local tree (or, best-effort, in another volume on the same host), a REF record is written instead — no data is stored again. See the Dedup section below for full scope details.

**No delta compression locally.** Delta compression is computed at S3 upload time and exists in S3 only. Local segment bodies contain either the full extent data (DATA records) or nothing (REF records, where the data already lives in an ancestor segment and is not duplicated). On S3 fetch, deltas are applied and the full extent is materialised locally before being cached and served to the VM.

## Read Path

```
1. VM reads LBA range
2. Look up LBA in live LBA map → extent_hash H
3. Check local segments (own pending/ + segments/, then ancestor segments/) for H
   - Hit  → return data
   - Miss → look up H in extent index → (segment_id, body_offset, body_length)
4. Issue a byte-range GET to S3 covering a chunk of the segment body
   - The fetch unit is a contiguous byte range (e.g. 1MB-aligned chunk) that
     includes the needed extent(s) plus neighbours for spatial locality
   - The segment index section encodes body_offset + body_length per extent,
     so the chunk boundaries can be derived precisely
   - If a delta body is available and smaller, fetch from the delta instead
5. Cache the fetched chunk; decompress and return the needed extent(s) to VM
```

The kernel page cache sits above the block device and handles most hot reads. The local segment cache handles warm reads. S3 is the cold path.

**Demand-fetch is at extent granularity, not segment granularity.** A segment file on S3 may contain hundreds of extents. Only the specific extent needed for a given read is fetched — the rest of the segment is never downloaded unless separately requested. This is the same design as the lab47/lsvd reference implementation, which issues byte-range `GetObject` requests for chunk-sized slices of segment bodies (1MB chunks, LRU-cached locally) and never downloads entire segment files. In practice, 93.9% of a 2.1GB Ubuntu cloud image is never read during a typical systemd boot — meaning 93.9% of S3 segment data is never fetched.

## LBA Map

The **LBA map** is the live in-memory data structure mapping logical block addresses to content. It is a sorted structure (B-tree or equivalent) keyed by `start_LBA`, where each entry holds `(start_lba, lba_length, extent_hash)`. It is updated on every write (new entries added, existing entries trimmed or replaced for overwrites) and is the authoritative source for read path lookups.

**Contrast with lab47/lsvd:** the reference implementation calls this `lba2pba` and maps `LBA → segment+offset` (physical location). GC repacking must update it for every moved extent. Palimpsest maps `LBA → hash` — the logical layer. Physical location (`hash → segment+offset`) is a separate extent index. This two-level indirection means GC repacking updates only the extent index; the LBA map is never rewritten for GC.

**Layer merging:** a live volume's LBA map is the union of its own data and all ancestor layers. At startup, layers are merged oldest-first (root ancestor first, live node last), so later writes shadow earlier ones. This is the same model as the lsvd `lowers` array, encoded in the directory tree.

### LBA map persistence

The LBA map is optionally persisted to a local `lba.map` file on clean shutdown and used as a fast-start cache on restart.

**Freshness guard:** the file includes a BLAKE3 hash of the sorted list of all current local segment IDs (own + ancestors). On startup, if the guard matches the current segment list, the cached LBA map is loaded directly without scanning segment index sections. If the guard doesn't match (new segments were written, or ancestry changed), the LBA map is rebuilt from scratch.

**Rebuild procedure:**
1. Walk the directory tree from the root ancestor to the live node
2. For each node, scan its `segments/` and `pending/` segment files
3. Read each segment's index section; apply LBA entries to the map (later layers take precedence for any overlapping LBA range)
4. Replay the current WAL on top (WAL entries are the most recent writes)

Since segment index sections are the ground truth for segment contents, rebuilding the LBA map requires only index sections and the WAL — never the segment data bodies. A full startup rebuild for a volume with 100 segments across its ancestry is a scan of ~6–200KB per segment index section, not 3GB of segment bodies.

### Manifest format

"Manifest" refers specifically to the **serialised form** of the LBA map, written optionally at snapshot time or as a startup cache. It is a correctness-optional optimisation — the LBA map is always reconstructible from segment index sections. When a manifest exists and its freshness guard is valid, it allows startup without scanning any segment files.

When persisted, the format is a binary flat file:

**Header (84 bytes):**

| Offset | Size | Field        | Description                          |
|--------|------|--------------|--------------------------------------|
| 0      | 8    | magic        | `PLMPST\x00\x02`                     |
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

## Snapshots

A snapshot freezes the current live node and starts a new live child. Snapshots serve two purposes: **checkpointing** (a rollback point for the same ongoing volume) and **forking** (launching a new independent volume from a known state). Both use the same mechanism.

**Taking a snapshot:**

```
1. Create <snap-ulid>/ as a child of the current live node
2. Create <snap-ulid>/wal/, <snap-ulid>/pending/, <snap-ulid>/segments/
3. Redirect new writes to the child immediately (live volume continues uninterrupted)
4. Background: flush any remaining WAL data to segments/ in the current (now-freezing) node
5. Background: remove wal/ and pending/ from the current node when flush completes
   → node is now frozen; directory contains only segments/
```

Steps 1–3 are the only blocking part and are instantaneous. Steps 4–5 are background and do not block I/O.

**Forking** (two VMs from the same snapshot point): once a node is frozen, create multiple children. Each child is an independent live volume that inherits the parent's data via the directory ancestry.

```
volumes/<base-id>/
  segments/                 ← frozen, shared by both forks
  <fork-a-ulid>/            ← VM A
    wal/
    pending/
    segments/
  <fork-b-ulid>/            ← VM B
    wal/
    pending/
    segments/
```

**Rollback:** delete the live leaf (and any of its descendants if needed), then re-create `wal/` and `pending/` in the target ancestor. The ancestor's segments are untouched.

**Checkpoint semantics (linear history):**

```
Before snapshot:          After snapshot:
volumes/<base>/           volumes/<base>/
  segments/                 segments/         ← frozen
  wal/               →      <snap-1>/
  pending/                    wal/            ← live continues here
                              pending/
                              segments/
```

**The directory tree is the source of truth.** No manifest file is required to understand the snapshot relationships or to reconstruct the LBA map. A manifest may be written as an optional startup optimisation, but its absence never affects correctness.

**GC interaction:** see [operations.md](operations.md). GC operates only on live leaf nodes; frozen ancestors are structurally immutable.

**Migration and disaster recovery** share the snapshot code path: start a volume from a snapshot manifest on a new host. One operation, multiple use cases.
