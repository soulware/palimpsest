# Design: skip no-op writes

**Status:** Implemented (single-tier).

## Problem

`Volume::write()` (`elide-core/src/volume.rs`) treats every inbound block uniformly: hash, dedup-check, append WAL record, update LBA map. For a block whose content already lives at that LBA, the path still writes a ~50-byte DedupRef record and pays the durability barrier. ext4 journal replay after an unclean mount, metadata rewrites landing on identical values, and page-cache double-flushes all generate these no-op writes.

## Skip check

After computing `blake3::hash(data)` (which the write path needs anyway for cross-LBA dedup, the WAL header, and the LBA map insert), check the LBA map for an exact match before falling through to the normal write path.

```rust
let hash = blake3::hash(data);

if self.lbamap.has_full_match(lba, lba_length, &hash) {
    self.noop_stats.skipped_writes += 1;
    self.noop_stats.skipped_bytes += data.len() as u64;
    return Ok(());
}
```

`lbamap.has_full_match(lba, lba_length, &hash)` returns `true` iff the LBA map has an entry keyed at exactly `lba` with matching length, matching hash, and `payload_block_offset == 0`. A single `BTreeMap::get` plus three field comparisons. **Zero file I/O.**

Correctness rests on BLAKE3 collision resistance: hash equality implies byte equality. The check does not need to read the body, check `.present`, or know whether the bytes are local. It fires for every `body_source` — `Local`, `Cached(present)`, `Cached(absent)`, S3-only — without touching the body file or the network.

## Why no byte-compare tier

An earlier version of this design included a second tier: when the hash check missed, walk the LBA map over the incoming range, verify every overlapping extent's body was on host, and byte-compare the existing content against the incoming data. The intent was to catch fragmented matches — e.g. two adjacent 4 KiB writes whose concatenation equals an 8 KiB incoming write — for which the hash check cannot fire by construction.

We removed it. The measurement that decided the question, taken with `dd if=/dev/urandom of=... bs=4M count=32` twice over a fresh ext4-on-NBD volume:

- Tier-2 read **~690 MiB** to save **~50 MiB** of write work.
- Almost all of those 50 MiB savings were ext4 metadata blocks (journal frames, group descriptors, unchanged bitmaps) — the hash check already caught those. Tier 2's marginal contribution on the random-data payload was ~zero.

The cost was real for a benefit the hash check already delivered, and it ran synchronously on the `VolumeActor` thread — extending write latency on the same thread that processes writes. It also forced an API split: a parallel `write_internal` entry point existed solely to bypass tier 2 for extent-reclamation rewrites, where the incoming bytes equal the existing bytes by construction and tier 2 would have silently dropped the rewrite. Removing tier 2 collapses that split.

If a future workload makes fragmented-match no-ops worth catching, reintroducing tier 2 with a shape gate (only fire when the LBA map's overlap is multi-extent — never on the dominant single-extent miss case) and a streaming early-exit compare would cap the cost. But we want measured demand before paying the complexity again.

## Coverage

| scenario                                       | skipped? |
| ---------------------------------------------- | -------- |
| Whole-extent match, any `body_source`          | yes      |
| Different content at the same LBA range        | no       |
| Unwritten range                                | no       |
| Fragmented match (incoming spans many extents) | no       |
| All-zeros write over a `ZERO_HASH` extent      | no       |

The last row is a behaviour change from the two-tier design: `blake3::hash([0; N])` is not the `ZERO_HASH` sentinel, so the hash check misses and the write commits as a normal (highly compressible) DATA record. In practice this case is rare — userspace zero-fills usually arrive as `NBD_CMD_TRIM` or `NBD_CMD_WRITE_ZEROES`, both of which bypass `write` entirely via `Volume::trim` / `Volume::write_zeroes`.

## Correctness

**No body I/O on the write path.** The check reads only the in-memory LBA map.

**Fork layering.** `open_read_state()` builds a flat `LbaMap` by walking the ancestor chain oldest-first. Parent entries are flattened into `self.lbamap` at open time; lookup does no chain walking. A child fork's LBA map already surfaces inherited parent mappings, so a no-op write against an inherited LBA matches unconditionally.

**Snapshots.** A skipped write produces no local segment entry. The snapshot's view of that LBA is inherited from whatever existing entry already covered it — exactly right, since the content is unchanged.

**Durability.** `NBD_CMD_FLUSH` routes through `VolumeHandle::flush → Volume::flush_wal`, an entirely separate path from `Volume::write`. A skip only means *this* write call adds no new WAL bytes; previously-appended WAL data still becomes durable on the next flush. The skip does not change the flush contract.

**Hash collisions.** Same BLAKE3 collision-resistance assumption already baked into the existing extent-index dedup path. No new trust.

**Internal rewrites.** Extent reclamation (`design-extent-reclamation.md`) reuses the write pipeline for rewrites whose incoming bytes equal the existing bytes by construction. The hash check is *correct* for those: re-running reclamation over an already-merged range is supposed to be a no-op, and a hash match expresses exactly that. Reclamation calls a `write_with_hash` entry point that takes a precomputed hash (avoiding a redundant blake3 pass) and returns whether the write committed; both paths run the same skip check.

## Counters

Two counters on `Volume`, exposed via `VolumeHandle::noop_stats()` and printed on NBD client disconnect:

- `skipped_writes` — `write()` calls short-circuited by the hash check.
- `skipped_bytes` — bytes the skip avoided writing to the WAL.

## Comparison: lsvd

lab47/lsvd has no write-time content dedup at all. `disk.go:681` (`WriteExtent`) buffers every incoming write unconditionally; `segment.go:538` computes entropy and compression stats but no content hash. Duplicate LBA writes overwrite previous LBA map entries and stale extents are reclaimed later by GC.

Elide already diverges by doing write-time content dedup (the REF-record path). This skip extends that mechanism: the same hash that drives REF emission also gates a free LBA-map shortcut for true no-ops.

## Non-goals

- Any skip that would trigger a demand fetch.
- Any change to `write_zeroes` / `trim`, which already bypass hashing.
- Any change to the REF-record path for cross-LBA dedup.
- Per-block hashes stored in the LBA map or segment entries (rejected as too expensive for the expected hit rate).
- Body byte-compares on the write path (see "Why no byte-compare tier" above).
