# Design: coordinator→volume GC plan handoff

**Status:** in progress (April 2026).

Builds on:
- [`design-gc-self-describing-handoff.md`](design-gc-self-describing-handoff.md)
  — the self-describing bare-`gc/<ulid>` body protocol.
- [`design-gc-partial-death-compaction.md`](design-gc-partial-death-compaction.md)
  — per-entry classifier (alive / canonical / partial-death / dead).

## Problem

The coordinator today materialises compacted segments end-to-end: it classifies
each entry, fetches every required body (local cache, S3 range-GET, S3 full-GET,
delta blob from the segment file, delta sidecar), applies deltas to reconstruct
composite bodies for partial-death Delta entries, and writes a signed
`gc/<ulid>.staged` segment. Three async coordinator helpers — `fetch_live_bodies`,
`resolve_body_by_hash`, `read_delta_blob` — re-implement body resolution that
`BlockReader` already owns:

- Ancestor-fork search (multiple `search_dirs`): BlockReader walks them;
  coordinator helpers consult a single `fork_dir`.
- Per-segment volume ownership (which volume's cache owns which segment):
  BlockReader's `Fetcher` knows; coordinator assumes a single `volume_id`.
- Demand-fetch + S3 fallback: BlockReader delegates to `SegmentFetcher`;
  coordinator hand-rolls cache/present-bit/range-GET logic in parallel.
- Delta reconstruction: BlockReader's `read_delta_block` picks a resolvable
  `source_hash`, reads the base body, applies the delta; coordinator reimplements
  the same three steps across `resolve_body_by_hash` + `read_delta_blob` +
  `elide_core::delta_compute::apply_delta`.

The coordinator's re-implementation lags: ancestor forks are invisible to it,
which surfaces as a correctness gap in cross-fork GC (flagged in PR #79). As
`BlockReader` grows, the gap widens.

## Design

The coordinator emits a plaintext **plan** (`gc/<new-ulid>.plan`) instead of a
signed staged segment. The volume consumes the plan, materialises bodies via its
own `BlockReader`, assembles the output segment, signs it, and commits via the
same tmp→bare rename that already exists.

### Coordinator responsibilities

- Run `collect_stats` exactly as today — classify every input-segment entry.
- Emit `gc/<new-ulid>.plan.tmp`, fsync, rename to `gc/<new-ulid>.plan`. No body
  fetching, no segment writing, no ephemeral signer.
- On the next GC tick, process bare `gc/<new-ulid>` the same way as today:
  upload to S3, promote IPC, delete old S3 objects, finalize IPC.

### Volume responsibilities

- Idle-tick handoff sweep grows a new step: for each `<ulid>.plan`, construct a
  `BlockReader` (already ancestor-aware / fetcher-aware), walk the plan outputs,
  resolve bodies via the reader, assemble `SegmentEntry`s, call `write_gc_segment`
  with the volume's own signer, rename `<ulid>.tmp` → bare `<ulid>`, remove
  `<ulid>.plan`.
- Per-input extent-index derivation stays exactly the same as the self-describing
  handoff: walk each input's `.idx`, compute moves + removals, commit after the
  tmp→bare rename.

### Body-resolution single source of truth

All body fetching funnels through `BlockReader`:

- **Data / Inline passthrough**: stored (possibly compressed) bytes read from
  `cache/<input>.body` via the reader's existing `find_segment_file` +
  `body_seek` primitives. Delta passthrough keeps the delta blob via
  `read_delta_blob`.
- **DedupRef partial-death**: `read_extent_body(&hash)` resolves ancestor-aware,
  decompressed.
- **Delta partial-death**: `read_extent_body(&base_hash)` + `read_delta_blob` +
  `apply_delta`. The volume re-picks the first resolvable `source_hash` from the
  entry's `delta_options` against its own extent index — which is the merged,
  ancestor-aware index, so it always sees at least what the coordinator saw.
- **Inline section**: read from the input `.idx`'s inline section (already
  local).

## Plan file format

Plaintext, tab-separated, line-oriented. Each record translates directly
into one output instruction — there is no structured framing and no
"header" block. The new segment's ULID comes from the filename
(`<ulid>.plan`), and the inputs list is derived from the set of
`<input-ulid>` values referenced by the records. First non-blank /
non-comment line is a version tag (`v1`).

```text
v1
keep        <input-ulid>  <entry-idx>
zero_split  <input-ulid>  <entry-idx>  <start-lba>  <lba-length>
canonical   <input-ulid>  <entry-idx>
run         <input-ulid>  <entry-idx>  <payload-block-offset>  <start-lba>  <lba-length>
drop        <input-ulid>
```

**Record meanings** (volume-side interpretation):

- `keep`: emit the input entry unchanged (fully-alive Data / Inline /
  DedupRef / Zero / Delta). Data/Inline bodies are copied stored-as-stored;
  Delta blobs are copied through into the output's delta body section and
  the option offsets rewritten.
- `zero_split`: emit a fresh Zero entry at the given `(start_lba,
  lba_length)`, discarding the input entry's original span. Used when a
  multi-LBA Zero has been partially overwritten and `collect_stats` split
  it into surviving sub-runs.
- `canonical`: demote the input entry to `CanonicalData` /
  `CanonicalInline`. Used for fully-LBA-dead-hash-live entries **and** for
  partial-death entries whose composite hash is externally referenced —
  both shapes emit exactly one canonical record.
- `run`: emit a fresh `Data` entry for one surviving sub-run of a
  partial-death entry. The volume resolves the composite body from
  `(input, entry_idx)`, slices `lba_length` blocks starting at
  `payload_block_offset * 4096`, and hashes the slice for the new entry.
  Multiple `run` records for the same `(input, entry_idx)` share the
  composite body (cached during materialise).
- `drop`: marks an input as consumed without emitting any output. The
  apply path walks the input's `.idx` and evicts every body-owning hash
  from the extent index (subject to stale-liveness). Used for fully-dead
  inputs in the dead pre-pass and for mixed batches where one input has
  no live entries.

A partial-death Data entry with an external-reference canonical and two
surviving sub-runs is encoded as one `canonical` + two `run` records:

```text
canonical   01HAAAA...  3
run         01HAAAA...  3  0  100  2
run         01HAAAA...  3  3  103  1
```

Without the external reference, the canonical is omitted — just two `run`
records.

Removals (hash-level extent-index evictions) are not listed explicitly —
the volume derives them at apply time by diffing each input's `.idx`
against the materialised output entries, same mechanism the self-
describing handoff used today.

## Signing

The plan itself is unsigned — it's plaintext sitting in a local directory
written by a trusted coordinator process. The output segment is signed by the
volume directly (no more ephemeral coordinator signer, no re-sign step).

## lbamap rebuild after apply

Partial-death sub-run expansion introduces **new** hashes at LBAs that
previously held the composite hash — e.g., a sub-run re-emission transitions
LBA 5 from `H_composite` to `H_subrun`. The on-disk state is self-consistent
after the bare-name rename, but the volume's in-memory `lbamap` still holds
the pre-apply view: LBA 5 → `H_composite`.

Left unfixed, the next GC pass's stale-liveness check (which scans
`self.lbamap.lba_referenced_hashes()`) sees `H_composite` as "live", marks
the input entry for removal as stale-cancel-worthy, and aborts the apply in
a loop that only breaks when the affected LBA is overwritten externally.

The commit path therefore rebuilds `self.lbamap` from the ancestor chain +
self directory after the tmp → bare rename (same code path `Volume::open`
uses, plus a WAL-tail replay). This keeps the in-memory view aligned with
what a fresh restart would produce. The rebuild is O(total segments); for
the expected GC cadence this is acceptable, and if it becomes a tail-latency
problem we can switch to a per-entry diff that updates only the affected
LBAs.

This is a real-bug fix — the pre-existing `.staged` apply path had the same
issue and was relying on the lbamap LBA happening to be overwritten by
unrelated writes before the stale_cancel would fire. Two proptest regression
entries for exactly this shape (`3b1f9cb…`, `ff79e9a0…`) now exercise the
rebuild path end-to-end.

## Lifecycle and crash recovery

The filename states for one compaction:

```
gc/<ulid>.plan.tmp   ← coordinator mid-write (swept by coordinator on startup)
gc/<ulid>.plan       ← coordinator committed; volume hasn't materialised
gc/<ulid>.tmp        ← volume mid-write (swept by volume on startup)
gc/<ulid>            ← volume applied, awaiting S3 upload
(deleted)            ← coordinator finalised
```

| # | Crash site | On-disk | Recovery |
|---|---|---|---|
| 1 | coord mid-write | `.plan.tmp` | sweep at coord startup |
| 2 | coord wrote `.plan`, volume hasn't applied | `.plan` | volume's next tick picks it up |
| 3 | volume mid-apply (tmp not written) | `.plan` | re-run — plan read is deterministic |
| 4 | volume mid-write of `.tmp` | `.plan` + `.tmp` | sweep `.tmp`, re-run |
| 5 | volume post-rename, pre-plan-remove | `.plan` + bare | bare wins; remove `.plan` |
| 6 | volume post-cleanup | bare | coord picks it up |
| 7 | coord post-upload, pre-delete | bare + S3 | idempotent on next tick |

Re-apply is deterministic: plan-read is pure, body resolution via BlockReader
is pure given disk state, output segment bytes are byte-identical across runs
given the same plan + same disk state.

## Defer semantics

The coordinator today defers partial-death Delta entries whose `source_hash`es
don't resolve via its local extent index. Under the plan model the check moves
to materialise time:

- Coordinator emits a `partial` for a Delta entry only if at least one
  `source_hash` resolves via its local index (unchanged from today).
- If the volume fails to resolve the base body at materialise time
  (BlockReader returns `NotFound`), it removes the `.plan` and leaves the
  input segments intact. The coordinator retries next pass, when a later
  write may have re-established a source.

The volume's merged (ancestor-aware) extent index is a superset of the
coordinator's fork-local index, so the volume's resolvability check is
weakly monotone in the coordinator's favour: the volume always resolves
when the coordinator resolves. The one failure mode — concurrent churn
vanishing a hash between plan emission and materialisation — is handled by
the same retry loop.

## Code deletions

With this change the coordinator no longer needs:

- `fetch_live_bodies`, `try_fetch_live_bodies_local` — body fetching.
- `resolve_body_by_hash`, `read_extent_body`, `read_extent_body_local`,
  `read_extent_body_cached` — hash-based body resolution.
- `read_delta_blob` — delta body reads.
- `expand_partial_death` — moves to the volume, reusing BlockReader.
- `generate_ephemeral_signer` + the ephemeral-key signing path — volume
  signs directly.
- The re-sign branch of `apply_staged_handoff` (volume→volume re-sign with
  same key) collapses into the plan-driven materialise path.

Associated tests (`fetch_live_bodies_*` unit tests in `gc.rs`) are removed;
coverage moves to:

- `gc_plan` round-trip unit tests (already landed).
- Volume materialise tests that drive `BlockReader` against hand-built
  plans under various shapes.
- The existing `gc_delta_partial_death_compaction` deterministic test,
  repointed to assert plan emission + materialised output.
- `gc_oracle` and `MultiLbaDedupRefOverwrite` proptest coverage,
  re-pointed to the plan-based path.

## Open questions

1. **Plan emission site.** The coordinator's `compact_segments` currently
   does fetch + expand + write in one function. Under the plan model we
   need a `plan_segments` producer that runs `collect_stats` → `GcPlan` and
   writes `gc/<ulid>.plan`. It can reuse the current classification code
   without modification.

2. **Zero-input defensives.** The plan must have at least one `input` record
   to be valid (the output segment's `inputs` field is non-empty, same as
   today). Empty-input plans are rejected at parse time.

3. **Delta blob copy.** `keep`-style Delta passthrough must carry the delta
   blob across into the output's delta body section. The volume builds the
   output delta body by concatenating each kept Delta's blob in plan order.
   Partial-death Delta produces a fresh `Data` entry (plus optional canonical
   full body), no delta blob — consistent with today's behaviour.
