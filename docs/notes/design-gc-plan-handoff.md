---
status: landed
related: [design-gc-self-describing-handoff.md, design-gc-partial-death-compaction.md]
landed_in: ../formats.md
---

# Coordinator → volume GC plan handoff

The coordinator emits a plaintext **plan** (`gc/<new-ulid>.plan`) instead of a signed staged segment. The volume consumes the plan, materialises bodies via its own `BlockReader`, assembles the output segment, signs it, and commits via the same tmp→bare rename.

## Why

The coordinator was materialising compacted segments end-to-end, with three async helpers (`fetch_live_bodies`, `resolve_body_by_hash`, `read_delta_blob`) re-implementing body resolution that `BlockReader` already owns:

- Ancestor-fork search across multiple `search_dirs` — `BlockReader` walks them; coordinator helpers consulted a single `fork_dir`.
- Per-segment volume ownership — `BlockReader`'s `Fetcher` knows; coordinator assumed a single `volume_id`.
- Demand-fetch + S3 fallback — `BlockReader` delegates; coordinator hand-rolled cache/present-bit/range-GET in parallel.
- Delta reconstruction — `BlockReader::read_delta_block` resolves; coordinator reimplemented across three fns.

The coordinator's reimplementation lagged: ancestor forks were invisible, surfacing as a cross-fork GC correctness gap (PR #79). Plan handoff makes the volume's `BlockReader` the single source of truth and deletes ~6 coordinator helpers + the ephemeral-signer path.

## Plan format

Plaintext, tab-separated, line-oriented. The new segment's ULID comes from the filename; inputs are derived from `<input-ulid>` references. First non-blank line is `v1`.

```text
v1
keep        <input-ulid>  <entry-idx>
zero_split  <input-ulid>  <entry-idx>  <start-lba>  <lba-length>
canonical   <input-ulid>  <entry-idx>
run         <input-ulid>  <entry-idx>  <payload-block-offset>  <start-lba>  <lba-length>
drop        <input-ulid>
```

- `keep` — emit input entry unchanged. Data/Inline copy stored-as-stored; Delta blobs copy through and option offsets get rewritten.
- `zero_split` — emit a fresh Zero entry at the given range, discarding the input's original span (multi-LBA Zero with surviving sub-runs).
- `canonical` — demote the input entry to `CanonicalData`/`CanonicalInline`. Used both for fully-LBA-dead-hash-live entries **and** for partial-death entries whose composite hash is externally referenced.
- `run` — emit a fresh `Data` entry for one surviving sub-run of a partial-death entry. The volume slices the composite body (cached during materialise) at `payload_block_offset * 4096` and re-hashes.
- `drop` — input consumed without emitting output. Apply walks the input's `.idx` and evicts every body-owning hash from the extent index (subject to stale-liveness).

A partial-death Data entry with external-reference canonical and two surviving sub-runs is one `canonical` + two `run` records. Removals (hash-level evictions) aren't listed explicitly — the volume derives them at apply time by diffing each input's `.idx` against the materialised output, same as the self-describing handoff predecessor.

## lbamap rebuild after apply

Partial-death sub-run expansion introduces **new** hashes at LBAs that previously held the composite hash. On-disk state is self-consistent after the bare-name rename, but the volume's in-memory `lbamap` still holds the pre-apply view. Left unfixed, the next GC pass's stale-liveness check sees `H_composite` as live and aborts the apply in a loop that only breaks on external overwrite.

The commit path therefore rebuilds `self.lbamap` from the ancestor chain + self directory after the tmp→bare rename — same code path `Volume::open` uses, plus a WAL-tail replay. O(total segments); acceptable at expected GC cadence.

This was a real-bug fix — the pre-existing `.staged` apply path had the same issue and was relying on accidental external overwrites. Two proptest regressions (`3b1f9cb…`, `ff79e9a0…`) exercise the rebuild path end-to-end.

## Lifecycle

```
gc/<ulid>.plan.tmp   ← coordinator mid-write (swept on startup)
gc/<ulid>.plan       ← coordinator committed; volume hasn't materialised
gc/<ulid>.tmp        ← volume mid-write (swept on startup)
gc/<ulid>            ← volume applied, awaiting S3 upload
(deleted)            ← coordinator finalised
```

Re-apply is deterministic: plan-read is pure; body resolution via `BlockReader` is pure given disk state; output segment bytes are byte-identical across runs given the same plan + disk state.

## Code shape

The plan-handoff landing deleted these coordinator-side helpers (all reimplementations of `BlockReader` capabilities):

- `fetch_live_bodies`, `try_fetch_live_bodies_local` — body fetching.
- `resolve_body_by_hash`, `read_extent_body`, `read_extent_body_local`, `read_extent_body_cached` — hash-based body resolution.
- `read_delta_blob` — delta body reads.
- `expand_partial_death` — moved to the volume, reusing `BlockReader`.
- `generate_ephemeral_signer` + the ephemeral-key signing path — volume signs directly.

Body resolution funnels through `BlockReader`: Data/Inline passthrough via `find_segment_file` + `body_seek`; DedupRef via `read_extent_body(&hash)` (ancestor-aware, decompressed); Delta via `read_extent_body(&base_hash)` + `read_delta_blob` + `apply_delta` with the volume re-picking the first resolvable `source_hash` from the entry's `delta_options` against its merged (ancestor-aware) extent index.

## Defer semantics

Delta partial-death whose `source_hash`es don't resolve are deferred at materialise time, not plan-emit time: the coordinator emits a plan if at least one option resolves via its local index; the volume fails-and-removes if `BlockReader` returns `NotFound`, leaving inputs intact for the next tick. The volume's merged (ancestor-aware) extent index is a superset of the coordinator's fork-local index, so the volume always resolves when the coordinator resolves — except under concurrent churn vanishing a hash, which the retry loop handles.
