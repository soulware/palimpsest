# Nydus Snapshotter Notes

[containerd/nydus-snapshotter](https://github.com/containerd/nydus-snapshotter) — a lazy-loading container image snapshotter built on the RAFS chunk-addressed filesystem format.

## What it solves

Nydus eliminates the "pull entire image before starting container" bottleneck. Containers start while the image is only partially downloaded; chunks are fetched on demand via HTTP range requests as the workload accesses them.

## Integration with containerd

Nydus is an **external snapshotter plugin** — it runs as a separate process and communicates with containerd via gRPC, implementing the `snapshots.Snapshotter` interface. Containerd calls `Prepare`, `Commit`, `Mounts`, `Remove`, etc. over a Unix socket. Snapshot metadata is stored in BoltDB (`metadata.db`).

## RAFS format: metadata/data separation

The core enabling idea is that image metadata and image data are stored separately:

- **Bootstrap** (`image.boot`): the complete inode/directory tree with chunk references, ~128KB, downloaded first. Enables immediate file enumeration with no data yet present.
- **Data blobs**: actual file contents split into fixed-size chunks (64KB–1MB, configurable), fetched on demand and cached locally.

The bootstrap-first approach means you can stat and list files immediately; data arrives lazily as it's accessed.

## On-disk layout

```
root/
├── metadata.db                    # BoltDB: snapshot metadata, parentage, labels
└── snapshots/<id>/
    ├── fs/
    │   └── image.boot             # RAFS bootstrap (metadata only, no file data)
    └── work/                      # overlayfs work dir (active snapshots only)

cache-dir/
├── <blob-id>                      # Cached chunk data
├── <blob-id>.chunk_map            # Bitmap: which chunks have been fetched
└── <blob-id>.blob.meta            # Chunk table (offsets, digests)
```

## Mounting

A `nydusd` userspace daemon handles the actual mounts. Two modes:

- **Fusedev**: nydusd presents a FUSE filesystem — most compatible, works on any kernel
- **Fscache/EROFS**: uses Linux kernel fscache + mounts EROFS — more efficient, avoids userspace context switches on every read

The snapshotter spawns/connects to nydusd via HTTP API (`api.sock`), then returns overlayfs `lowerdir` chains to containerd. One nydusd can serve multiple RAFS instances (shared mode) or one per snapshot (dedicated mode).

## Lazy fetch and chunk cache

When a container reads a file and the chunk isn't cached:

1. FUSE handler in nydusd intercepts the read
2. Checks `<blob-id>.chunk_map` bitmap — chunk absent
3. Issues HTTP range request to registry for that chunk
4. Writes to `cache-dir/<blob-id>`, marks chunk present in bitmap
5. Returns data to container

The `.chunk_map` bitmap is the local cache index — it tracks which of the blob's chunks are present without reading the data files.

## Deduplication

Content-addressed at the chunk level. Identical chunks across different layers share the same blob ID and thus the same cache entry. Multiple RAFS instances reference the same blob files; blobs are reference-counted and only deleted when no RAFS instance references them.

## Snapshot model

Standard linear OCI layer chain. `KindActive` (writable, has overlayfs upper) → `Commit()` → `KindCommitted` (immutable). Parent chains build overlayfs `lowerdir` stacks. No branching — strictly linear ancestry per OCI spec.

## The NRI optimizer plugin and boot hints

Nydus has a two-plugin system for learning and applying file access patterns — directly analogous to Elide's boot hints concept.

### Collection: `optimizer-nri-plugin`

Hooks into container `StartContainer`/`StopContainer` NRI events. On start, spawns a Rust binary (`optimizer-server`) that:

- Joins the container's mount and PID namespace
- Attaches **fanotify** (`FAN_OPEN`, `FAN_ACCESS`, `FAN_OPEN_EXEC`) to monitor all file access
- Streams events as JSON with `{path, size, elapsed_µs}` — the elapsed timestamp captures *when* during startup each file was first accessed, giving an ordered sequence not just a set
- Writes deduplicated paths to `/opt/nri/optimizer/results/<repo>/<image>:<tag>`

### Embedding: `nydusify convert --prefetch-patterns`

The collected file list is fed into the image build tool (`nydusify`) which embeds the prefetch hints into the RAFS bootstrap at image build time. This is the primary path — hints travel with the image, not as separate runtime config.

### Consumption: `prefetchfiles-nri-plugin`

Hooks into `RunPodSandbox`. Reads a pod annotation `containerd.io/nydus-prefetch`, sends the file list to the snapshotter's system controller via HTTP PUT. When nydusd is launched, the list is passed as `--prefetch-files`, driving prefetch ordering.

### Full lifecycle

```
1. Run container with workload (profiling run)
2. fanotify records ordered file access sequence → results/<image>
3. Developer feeds list into: nydusify convert --prefetch-patterns <file>
4. New Nydus image built with hints embedded in bootstrap
5. On future container start, nydusd uses hints to drive prefetch ordering
```

The profiling run is a **deliberate separate step** — you run a workload, observe it, encode the observations into the artifact. This is cleaner than speculative runtime learning.

## Failure modes under network unavailability

If nydusd cannot fetch a chunk (network down, registry unavailable):

- nydusd retries with backoff internally before returning an error upward
- Eventually returns EIO to the FUSE/fscache layer
- For a rootfs during boot: EIO on a critical file (shared library, kernel module, init binary) causes a kernel panic or process crash — the boot fails

The stall case (network hung, not refused) is worse: the FUSE read blocks indefinitely, the process waiting on the file hangs, and the system appears frozen with no visible error.

**Mitigations Nydus provides:**
- `prefetch_all: true` — download the entire blob before allowing the container to start; defeats lazy loading but ensures safe boot
- `fs_prefetch.threads_count` — parallel workers to make eager prefetch fast
- NRI optimizer hints — narrow the eager fetch to just the boot-critical file set, avoiding full download while still ensuring safety

For rootfs boot scenarios, Nydus's practical recommendation is to prefetch aggressively enough that the container is effectively ready before init runs. The latency win comes from parallel prefetch during startup, not true on-demand access of uncached data.

## Comparison to Elide

| Aspect | Nydus | Elide |
|---|---|---|
| Domain | Container image layers | VM block device (NBD) |
| Format | RAFS (chunk-addressed FS) | Log-structured segments (block-addressed) |
| Metadata separation | Bootstrap + data blobs | LBA map in WAL + segment data |
| Lazy fetch | HTTP range requests per chunk | Demand-fetch from S3 (planned) |
| Dedup | Content-addressed chunks | Content-addressed extents |
| Snapshot model | Linear OCI layer chain | Branching tree (`children/` dir, named forks) |
| Mount mechanism | FUSE or EROFS via `nydusd` | NBD via `nbd-server` |
| Layer writability | Active snapshot = overlayfs upper | Volume is fully writable (COW on fork) |
| Chunk granularity | 64KB–1MB configurable | 4KB blocks (NBD sector granularity) |
| Boot hints | File paths + elapsed time, embedded in bootstrap | LBA ranges, stored in volume metadata (planned) |
| Hint collection | fanotify in container namespace | NBD read log or filesystem-level tracing (planned) |

The core ideas converge: both separate metadata from data, both use content addressing for dedup, both want lazy remote fetch, both have a boot-hints mechanism. The main differences are domain (filesystem vs block device) and snapshot model (linear OCI chain vs branching tree). Nydus has no concept of mutable forks or branching history.

The elapsed-time ordering in the NRI optimizer hints is worth adopting for Elide's boot hints — an ordered LBA sequence (by first-access time during boot) is more useful than an unordered set, since it lets the prefetch queue be prioritised by boot criticality.
