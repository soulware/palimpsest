# GC bucket unification

Collapse the GC selector's "smalls + one filler" tier model into a single
bin-packing pass that can emit multiple output buckets per tick, mirroring
the pending-side unification that landed in #297. Status: proposal.

## Motivation

After #297 the pending-side compaction is one bin-packing pass with N→N
outputs. The coordinator-driven GC selector
(`elide-coordinator/src/gc.rs:325-460`) is still on the pre-unification
shape:

|              | Selection                                     | Output count | Notes                              |
|--------------|-----------------------------------------------|--------------|------------------------------------|
| **Tier 0**   | tombstone-like (`live_lba_bytes == 0`)        | folded free  | contributes to `inputs`, no body   |
| **Tier 1**   | smalls (`live_lba_bytes ≤ 16 MiB`)            | shared       | greedy ascending into one bucket   |
| **Tier 2**   | sparse-large filler (density < threshold)     | one per pass | tops up remaining headroom         |

`gc_fork` emits exactly one output segment per tick. The "one filler" rule
exists for the same reason sweep had it before #297: when there is only
one output bucket, a narrow size-based candidate set leaves the bucket
under-filled, so a single larger candidate is grafted on to top it up.

This shape has the same problems #297 fixed for pending:

- **Throughput is bucket-rate-limited.** GC ticks at `gc_interval`
  (default 10 s) and reclaims at most one 32 MiB live bucket per tick.
  A volume with many small dead-bearing segments takes many ticks to
  drain even though all the work could pack into independent buckets.
  The historical reason for the per-tick cap was that body fetch
  happened on the coordinator and was assumed to be S3 GETs. Under
  plan-handoff (#…) the volume materialises bodies via `BlockReader`,
  which is **cache-first** — a hit on `cache/<ulid>.body` (gated on
  `cache/<ulid>.present` bits) skips S3 entirely. Self-written and
  recently-demand-fetched segments are the common cache-hit case, so
  the assumed cost model is now too pessimistic.
- **Dense-mid-sized-with-dead is invisible.** A segment with
  `live_lba_bytes > 16 MiB` AND `density ≥ density_threshold` AND some
  dead bytes is neither small nor a sparse-large filler candidate, so
  GC never touches it. The dead bytes accumulate until enough writes
  drop the density below threshold or the segment shrinks under the
  small cap. Bin-packing makes this case a free rider in any bucket
  with leftover headroom.
- **The filler slot is artificial.** Today's filler picks "lowest
  density, ties broken by largest live bytes." That's a workaround for
  emitting one output: with N independent buckets every sparse segment
  is a primary candidate.

## What's already in place

Critically, the *handoff* side of the protocol already supports multiple
outputs per tick — only the selector is single-bucket. Specifically:

- The volume's `apply_gc_handoffs`
  (`elide-core/src/volume/mod.rs:905`) walks every `<ulid>.plan` file in
  `gc/` and applies them sequentially in ULID order. Multiple plans
  emitted in one tick apply as a batch on the volume's next idle arm.
- `apply_done_handoffs` (`elide-coordinator/src/gc.rs:481`) walks every
  bare `gc/<ulid>` and finalises each (upload → promote → S3 delete →
  finalize). Already N-aware.
- `gc_checkpoint` mints two ULIDs (`u_gc`, `u_flush`) and pre-flushes
  the WAL. The same pre-mint pattern with N+1 ULIDs (proposed below)
  trivially generalises — `UlidMint::next()` is monotonic in a `u128`
  counter, so reserving more is free.
- The "only one outstanding pass per fork" gate is "no `.plan` or bare
  `gc/<ulid>` exists." Multiple plans landing in one tick are still one
  pass; the gate stays correct.

So this proposal touches the selector and the checkpoint ULID count,
not the handoff plumbing.

## Proposal

Replace the tier model with one bin-pack:

1. **Select** every eligible segment using the existing rules:
   - tombstone (`live_entries.is_empty() && removed_hashes.is_empty()`),
   - small (`live_lba_bytes ≤ SWEEP_SMALL_THRESHOLD`), or
   - sparse (`density < density_threshold && dead_lba_bytes > 0 &&
     has_data_content`),
   excluding snapshot-floor and partial-LBA-death-Delta deferred
   segments (no change here), then **filter to cache-resident
   candidates** (see below).
2. **Bin-pack** candidates into output buckets sized to
   `SWEEP_LIVE_CAP` (32 MiB live) and `SWEEP_ENTRY_CAP` (8192 entries).
   First-fit-decreasing on `live_lba_bytes`. Tombstones fold into the
   first bucket (or any bucket — they cost nothing). A bucket with
   exactly one input that is not tombstone-bearing and not sparse is
   a no-op rewrite and is dropped (the existing skip-check rule).
3. **Cap** the number of buckets per tick at `max_buckets_per_tick`
   (new config knob, default `4`; the cache-first body resolver makes
   the multi-bucket case cheap for the common workload). Excess
   candidates wait for the next tick.
4. **Mint** `max_buckets_per_tick + 1` ULIDs at `gc_checkpoint`: one
   per potential bucket, plus `u_flush` (kept above all bucket ULIDs).
   Unused ULIDs cost nothing; the mint is a `u128` counter.
5. **Emit** one `gc/<u_bucket_i>.plan` per filled bucket via tmp+rename.

Per-tick reclamation throughput becomes
`max_buckets_per_tick × SWEEP_LIVE_CAP` of cache-resident input,
bounded above by what's actually local on the volume. Raising the
cap is a deliberate tuning decision; lowering candidate residency
self-throttles GC without operator action.

### Cache-residency candidate filter

A candidate is eligible only when its body is fully resolvable
without an S3 GET — i.e. `cache/<ulid>.body` exists *and* every live
entry's bit is set in `cache/<ulid>.present`. The same check the
pre-plan-handoff `fetch_live_bodies` performed, applied at selection
time instead of fetch time.

This makes GC throughput track cache residency rather than total
segment count, which has three useful properties:

- **Lazy volumes still GC.** Volumes that never fully prefetch (e.g.
  `lazy` config) keep reclaiming their own writes. Locally-authored
  segments are fully cache-resident at promote time (`promote_segment`
  copies the body from `pending/` and sets every `present` bit), so
  every recently-written-then-partially-overwritten segment is an
  eligible candidate even on a volume whose ancestor cache is cold.
- **Cold-restart doesn't burst S3 GETs.** After coordinator restart
  or claim handoff, prefetch warms `.idx` synchronously but warms
  bodies lazily (peer hints + demand-fetch). The candidate filter
  defers GC of cold-cache segments to whenever they actually get
  read, instead of paying the GET cost just so GC can rewrite them.
- **No over-fetching for GC's sake.** If a segment is dead or sparse
  but not in cache, it costs less to leave it on S3 (paid once, in
  retention) than to fetch it locally only to rewrite and re-upload.
  The retention-window economics already amortise the leak.

Partial cache hits (some entries present, others not) count as
**not resident**: the bin-packer wouldn't know which subset to admit
without separate per-entry accounting, and the simpler all-or-nothing
rule is safe (it just defers, never corrupts).

## Algorithm

Proposed `select_buckets`:

```
select_buckets(eligible, config, u_buckets):
  partition eligible into:
    tombstones — live_entries.is_empty() && removed_hashes.is_empty()
    smalls     — live_lba_bytes ≤ SWEEP_SMALL_THRESHOLD
    sparse_lg  — density < threshold && dead_lba_bytes > 0
                                     && has_data_content
                                     && live_lba_bytes > SWEEP_SMALL_THRESHOLD
    (dense_lg dropped — same as today)

  candidates = smalls ∪ sparse_lg
  sort candidates descending by live_lba_bytes (FFD)

  buckets: Vec<Bucket> = []
  for c in candidates:
    placed = false
    for b in buckets:
      if b.fits(c.live_lba_bytes, c.estimated_output_entries()):
        b.push(c); placed = true; break
    if !placed && buckets.len() < max_buckets_per_tick:
      buckets.push(Bucket::new().push(c))

  // Tombstones fold for free into the first bucket. If there are no
  // candidate buckets, tombstones alone form a bucket — they emit a
  // zero-entry handoff ("tombstone handoff") that lets the inputs be
  // safely deleted.
  if !tombstones.is_empty():
    if buckets.is_empty():
      buckets.push(Bucket::tombstone_only(tombstones))
    else:
      buckets[0].extend(tombstones)

  // Drop trivial no-ops (existing skip rule, applied per-bucket).
  buckets.retain(|b| {
    has_dead = b.any(s -> s.live_entries.is_empty()
                       && s.removed_hashes.is_empty());
    has_sparse = b.any(s -> s.density() < threshold);
    has_dead || has_sparse || b.len() ≥ 2
  })

  // Sort each bucket back into ULID order before plan emission so
  // compact_segments' rebuild-order semantics still hold.
  for b in buckets:
    b.sort_by_ulid()

  assign u_buckets[i] to buckets[i]
  return buckets
```

`gc_checkpoint` becomes:

```
gc_checkpoint(max_buckets):
  u_buckets = (0..max_buckets).map(|_| mint.next()).collect()
  u_flush   = mint.next()
  flush_wal_to_pending_as(u_flush)
  return (u_buckets, u_flush)
```

The ordering invariant `u_bucket_i < u_flush < next_wal_ulid` holds by
construction: bucket ULIDs are minted before `u_flush`, and the WAL
ULID after the checkpoint is necessarily `> u_flush` because the mint
is monotonic. Same as today, generalised to N.

## Apply path

Already supports N plans per tick (see *What's already in place*). One
sequencing detail to call out:

- `apply_gc_handoffs` walks plans in ULID order. Plans emitted from the
  same checkpoint are independent (their input sets are disjoint by
  bin-packing) so any apply order is safe. The ULID-sorted walk is a
  determinism property, not a correctness one.

The retention-marker write in `apply_done_handoffs` happens per bare
file as today; one marker per output, listing that output's input set.

## Config

New field:

```rust
pub struct GcConfig {
    pub density_threshold: f64,
    pub interval: Duration,
    pub retention_window: Duration,
    /// Maximum number of output buckets emitted per GC tick. Raising
    /// this multiplies the per-tick rewrite throughput by the same
    /// factor and, more importantly, the retention-window peak by the
    /// same factor. Default 4: the body resolver is cache-first under
    /// plan-handoff, so multi-bucket ticks are cheap for the common
    /// (recently-written) workload, and 4 lets a small-segment backlog
    /// drain in one tick without inflating retention peak unduly.
    #[serde(default = "default_max_buckets_per_tick")]
    pub max_buckets_per_tick: usize,
}
```

Default `1`: behaviour-preserving. The user opts into more aggressive
packing.

## Tradeoffs

**Pro**
- Closes the dense-mid-sized-with-dead packing hole (same hole #297
  closed for pending).
- Reclamation throughput scales with `max_buckets_per_tick` instead of
  being hard-capped at one bucket per `gc_interval`.
- Drops the "filler" tier and its lowest-density-tie-break-by-largest
  selection logic — sparse segments become ordinary candidates.
- One selection rule, easier to reason about and test.

**Con**
- Per-bucket PUT to S3 for each emitted output. Modest — bounded by
  `max_buckets × SWEEP_LIVE_CAP` of compressed live bytes — but real,
  and not amortised the way local rewrites are on pending.
- Retention peak grows linearly with bucket count. Operators raising
  `max_buckets_per_tick` need to consider
  `live_data + post_compaction_outputs + (C × T)` from
  `docs/operations.md:210` accordingly. This is the strongest reason
  to keep the cap configurable rather than unbounded.
- Bin-packing logic is one branch deeper than the pending case
  because GC also has to honour `density_threshold` (kept) — pending
  could drop its density gate, GC cannot, because rewriting a dense
  large segment with no dead bytes still costs a PUT and a retention
  slot for no reclamation.
- Cache-resident-only selection means a sparse-but-cold segment
  doesn't get reclaimed until something else (read, eviction, retention
  reaper) touches it. This is a deliberate tradeoff — GET-then-PUT
  for GC's sake is worse than paying retention for a known-dead
  segment — but it does mean reclamation isn't strictly monotone in
  wall-clock time for cold inputs.

## Decisions

- **Keep `density_threshold` as the rewrite gate.** Pending dropped
  it because local rewrites are cheap; GC rewrites cost S3 bandwidth.
  Dense-large segments without enough dead bytes still don't get
  rewritten just for consolidation, even under bin-packing.
- **Bucket-level skip-check, not pass-level.** Each emitted bucket
  must justify itself (≥1 tombstone, ≥1 sparse, or ≥2 inputs). Today's
  pass-level check carries over per-bucket; a tick that produces zero
  worthwhile buckets emits nothing.
- **`max_buckets_per_tick` default 4.** The body resolver is
  cache-first under plan-handoff, so the original "one bucket per
  tick" rate-limit (which assumed coordinator-side S3 GETs) is no
  longer the binding constraint for the common workload. Default of
  4 lets a backlog of small dead-bearing segments drain in one tick
  without picking a number that pushes retention peak to 4× live data
  by default. Operators can raise or lower from there.
- **N+1 ULIDs at checkpoint.** `max_buckets_per_tick` bucket ULIDs +
  `u_flush`. Unused ULIDs are free.
- **Plan filenames stay `gc/<ulid>.plan`.** No protocol change to the
  filename or file content; the only difference is "more than one of
  these can appear per tick."
- **Cache-residency filter is mandatory, not opt-in.** GC never issues
  S3 GETs purely to enable a rewrite. A sparse-but-cold input waits
  for organic warming (read, peer-fetch hint) or retention. This
  matches the lazy-volume design — laziness must extend to GC for it
  to mean anything.

## Open

- **Per-bucket parallelism.** Once N plans land in one tick the
  coordinator's plan-materialisation and the volume's apply could run
  in parallel. Out of scope for this proposal; revisit after the
  selector lands.
- **Adaptive `max_buckets_per_tick`.** Tie the per-tick cap to a
  bandwidth budget rather than a count. Cleaner separation of the
  rate-limit from the bucket structure, but requires telemetry the
  coordinator does not collect today. Out of scope.
- **Cross-tick filler heuristic.** When a tick fills `N-1` buckets and
  the last bucket has under-filled headroom, should it greedily admit
  a dense-mid-sized-with-dead segment that wouldn't otherwise be
  eligible? Symmetric with today's "one filler per pass" but at the
  bucket level. Default no — keep the eligibility rules clean.

## Out of scope

- The `gc_checkpoint` IPC shape changes (single `Ulid` reply →
  `(Vec<Ulid>, Ulid)` or similar). Mechanical, called out for
  awareness.
- The retention-window economics. Tuned via `retention_window` and
  `density_threshold` independently of this change.
- Pending-side compaction (already unified in #297).
- Snapshot-floor handling (unchanged).

## Sketch of the diff

Approximate, for sizing:
- `select_bucket` (one bucket) → `select_buckets` (N): replace
  Tier-1/Tier-2 logic with FFD bin-pack. ~+80, −40.
- `gc_fork` returns `Vec<PackedBucket>`; emit one plan per. ~+30.
- `gc_checkpoint` IPC: mint N+1 ULIDs, return `Vec<Ulid> + Ulid`.
  Volume side: small loop over `mint.next()`. ~+30.
- `gc_cycle::run_gc_pass`: pass `max_buckets` from config to
  `gc_checkpoint`, log per-bucket compact lines instead of one. ~+10.
- `GcConfig`: add `max_buckets_per_tick` with default. ~+10.
- Tests: existing single-bucket tests run with default `1` unchanged.
  Add a multi-bucket test that exercises FFD packing. ~+150.
- Doc updates: `docs/operations.md` GC section, this file referenced
  from `README.md`.

Net likely modest positive — the new bucketer is straightforward FFD
and the existing skip-check / partial-LBA-death deferral logic
transfers verbatim.
