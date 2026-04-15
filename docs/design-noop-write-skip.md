# Design: skip no-op writes

**Status:** Implemented.

## Problem

`Volume::write()` (`elide-core/src/volume.rs:519`) treats every inbound block uniformly: hash, dedup-check, append WAL record, update LBA map. For a block whose content already lives at that LBA, the path still writes a ~50 byte DedupRef record and pays the durability barrier. ext4 journal replay after an unclean mount, metadata rewrites landing on identical values, page-cache double-flushes, and zero-over-zero writes during `mkfs`/`fstrim` all generate these no-op writes.

## Two-tier skip

After computing `blake3::hash(data)`, try two checks before falling through to the normal write path. Both return `Ok(())` immediately on hit, leaving the LBA map and segment tree untouched.

```rust
let hash = blake3::hash(data);

// Tier 1 — hash compare. Pure LBA map lookup, zero body I/O.
if self.lbamap.has_full_match(lba, lba_length, &hash) {
    self.noop_stats.skipped_writes += 1;
    self.noop_stats.skipped_bytes += data.len() as u64;
    return Ok(());
}

// Tier 2 — opportunistic byte compare. Only fires when every overlapping
// body is already on host (Local or Cached-present).
if let Some(existing) = self.try_read_local(lba, lba_length)?
    && existing == data
{
    self.noop_stats.skipped_writes += 1;
    self.noop_stats.skipped_bytes += data.len() as u64;
    return Ok(());
}
```

### Tier 1 — hash compare

`lbamap.has_full_match(lba, lba_length, &hash)` returns `true` iff the LBA map has an entry keyed at exactly `lba` with matching length, matching hash, and `payload_block_offset == 0`. A single `BTreeMap::get` plus three field comparisons. **Zero file I/O.**

Correctness rests on BLAKE3 collision resistance: hash equality implies byte equality. We do not need to read the body, check `.present`, or know whether the bytes are local. Tier 1 fires for every `body_source` — `Local`, `Cached(present)`, `Cached(absent)`, S3-only — without touching the body file or the network.

This is the dominant case in practice. Most no-op writes (ext4 journal replay, page-cache double-flush, metadata rewrites that land on identical values) overwrite a previously-written LBA with the same content. The LBA map already records that, and tier 1 catches it for the cost of one map lookup.

### Tier 2 — opportunistic byte compare

Tier 1 cannot fire when the LBA map covers the incoming range with **multiple entries** — the new write's hash is over the whole range, and none of the per-fragment hashes match. The classic case: two adjacent 4 KiB writes at LBAs L and L+1 with different content, followed by an 8 KiB write at L whose content is exactly the concatenation. Hash-compare misses; byte-compare catches it.

`try_read_local` walks `extents_in_range`, verifies every covered block is **on host** (not just indexed), and reads the existing bytes via the normal `read_extents` path:

- `Local` extent → on host (WAL, `pending/`, bare `gc/<ulid>` files).
- `Cached(n)` extent → on host iff `cache/<id>.present` has bit `n` set, in either the current fork's cache or any ancestor's cache.
- Unwritten gap, missing extent index entry, delta-only entry, or `Cached(n)` with the present bit clear → bail.

Bailing falls through silently to the normal write path. Tier 2 never triggers a demand fetch.

The byte read is cheap when warm (the page cache holds recently-written segments), but the cost is real for cold data. Tier 2 is intentionally narrow: it only adds value for fragmented matches, and only when the bytes are already on host.

## Coverage matrix

| scenario                                    | tier 1 | tier 2 |
| ------------------------------------------- | ------ | ------ |
| Whole-extent match, Local                   | hit    | -      |
| Whole-extent match, Cached present          | hit    | -      |
| Whole-extent match, Cached absent / S3-only | hit    | -      |
| Fragmented match, Local                     | -      | hit    |
| Fragmented match, Cached all present        | -      | hit    |
| Fragmented match, any extent absent         | -      | bail   |
| Different content                           | -      | miss   |
| Unwritten range                             | -      | bail   |

The only no-op case nothing catches is a fragmented match where at least one extent's body is absent — and that case is what we explicitly want to decline, because catching it would require a demand fetch.

## Correctness

**No body fetch, ever.** Tier 1 reads only the in-memory LBA map. Tier 2 gates on `is_extent_on_host` before calling `read_extents`, and `read_extents` uses the existing `find_segment_in_dirs` path which only invokes the fetcher if no local copy is found — by then we have already verified locality.

**Fork layering.** `open_read_state()` (`volume.rs:2880`) builds a flat `LbaMap` via `lbamap::rebuild_segments()` that walks the ancestor chain oldest-first. Parent entries are flattened into `self.lbamap` at open time; lookup does no chain walking. A child fork's LBA map already surfaces inherited parent mappings, so a no-op write against an inherited LBA hits tier 1 unconditionally and tier 2 if any ancestor still has the body cached present.

**Snapshots.** A skipped write produces no local segment entry. The snapshot's view of that LBA is inherited from whatever existing entry already covered it — exactly right, since the content is unchanged.

**Durability.** The NBD layer routes `NBD_CMD_FLUSH` through `VolumeHandle::flush → Volume::flush_wal`, an entirely separate path from `Volume::write`. A skip only means *this* write call adds no new WAL bytes; previously-appended WAL data still becomes durable on the next flush. The skip does not change the flush contract.

**Hash collisions.** Tier 1 relies on BLAKE3 collision resistance, the same assumption already baked into the existing extent-index dedup path. No new trust.

## Counters

Three counters on `Volume`, exposed via `VolumeHandle::noop_stats()` and printed on NBD client disconnect:

- `skipped_writes` — `write()` calls short-circuited (tier 1 + tier 2 combined).
- `skipped_bytes` — bytes the skip avoided writing to the WAL.
- `check_reads_bytes` — bytes read by tier 2's byte compare (hit + miss). Always `0` if only tier 1 fires; non-zero only when tier 2 ran.

The disconnect line prints all three after every NBD session, so any mounted workload surfaces the hit rate without extra tooling.

## Comparison: lsvd

lab47/lsvd has no write-time content dedup at all. `disk.go:681` (`WriteExtent`) buffers every incoming write unconditionally; `segment.go:538` computes entropy and compression stats but no content hash. Duplicate LBA writes overwrite previous LBA map entries and stale extents are reclaimed later by GC.

Elide already diverges by doing write-time content dedup (the REF-record path). This proposal extends that mechanism: the same hash that drives REF emission now also gates a free LBA-map shortcut for true no-ops, and a supplemental byte compare picks up the fragmented cases that pure hash-compare misses.

## Non-goals

- Any skip that would trigger a demand fetch.
- Any change to `write_zeroes` / `trim`, which already bypass hashing.
- Any change to the REF-record path for cross-LBA dedup.
- Per-block hashes stored in the LBA map or segment entries (rejected as too expensive for the expected hit rate).

## Scope: client-intent writes only

The two tiers above are a correct optimisation for writes whose *caller's intent* is to change observable LBA content — i.e. normal NBD client writes, where a same-bytes-already-present outcome is genuinely redundant work.

They are **not** correct for internal rewrite paths whose caller's intent is to change the *representation* while preserving observable content. The motivating case is extent reclamation (see `design-extent-reclamation.md`): the volume reads live bytes from a fragmented or partially-dead payload and writes them back as fresh compact entries. By construction the incoming content equals the existing bytes at those LBAs, so tier 2 would classify the write as a no-op and silently drop it — defeating the whole operation.

Tier 1 is safe for internal rewrites. It only fires when a map entry *already binds this exact hash directly to these LBAs with `payload_block_offset == 0`*, which is precisely the post-rematerialisation steady state. Running reclamation a second time over an already-merged range therefore hits tier 1 and skips correctly, giving idempotent convergence for free without any termination bookkeeping.

Internal rewrite paths should use a write entry point that runs tier 1 but bypasses tier 2. The split encodes the distinction between the two legitimate motivations for a write:

- **Client-origin write** — caller wanted observable bytes to change; both tiers fire.
- **Internal-origin write** — caller wanted the representation to change even though observable bytes are unchanged; only tier 1 fires.

This split is intentional design surface, not an edge case: it is the mechanism that lets reclamation reuse the full write pipeline (compression, dedup lookup, WAL append, LBA map update) with a single switch.
