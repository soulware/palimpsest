# Design: self-describing GC handoff (no manifest sidecar)

Status: **proposed**

Date: 2026-04-15

---

## Context

GC handoff today uses two files per compacted segment in `gc/`:

- `gc/<ulid>` — the body (binary segment file, coordinator-written).
- `gc/<ulid>.{pending,applied,done}` — a plaintext manifest of
  `repack <hash> <old_ulid> <new_ulid> <new_offset>` / `remove <hash>
  <old_ulid>` / `dead <old_ulid>` lines, whose suffix encodes the
  handoff lifecycle.

The manifest is the coordinator's precomputed digest telling the
volume how to update its extent index. Its suffix is also doing
double duty as the "body is safe to read" gate — `find_segment_in_dirs`
and `locate_segment_body` accept a `gc/<ulid>` read only when
`gc/<ulid>.applied` exists alongside it.

This couples two logically-distinct things:

1. **The body's readability state** ("has the volume re-signed this?").
2. **The handoff protocol state** ("who needs to act next in the
   coordinator↔volume handshake?").

Reads are forced through (2) to answer (1). A second read site
(`delta_compute::read_source_extent`) was silently missing the gate
until recently — see `refactor(segment): unify segment body lookup`
commit.

## Goal

Make the body file's name the single source of truth for readability:

```
gc/<ulid>.staged    ← coordinator staged, not yet applied, NOT readable
gc/<ulid>           ← volume applied + re-signed, readable
(deleted after upload)
```

Rule: **a bare-name segment in `gc/` is always safe to read.**

`locate_segment_body` collapses to one `exists()` call, the `gc/`
branch is symmetric with `pending/` and `wal/`, and there is no
sidecar state to get out of sync with the body.

## Key move: derive the handoff from the segment itself

To eliminate the manifest entirely, the new segment must carry enough
information for the volume to reconstruct the apply actions from its
own contents. The minimum new field is a list of **input ulids** —
the old segments that were consumed to produce this GC output.

With that field plus the new segment's entry table, the volume
computes the apply set itself at restart/apply time:

```text
for each input_ulid in gc_segment.inputs:
    old_entries = read_segment_index(input_ulid)
    for each entry in old_entries:
        if entry.hash in gc_segment.entries:   # repacked
            update extent_index: hash → (new_ulid, new_offset)
        else:                                  # superseded
            maybe remove extent_index entry (lowest-ULID-wins still applies)
```

The old segments are still on disk at apply time (the coordinator
does not delete them until after the volume acknowledges). Their
`.idx` files are small and already loaded into the rebuild path, so
the extra work is bounded.

This replaces the plaintext manifest format with a typed field inside
the already-signed segment header — no separate file, no separate
format evolution, no parse step.

## Lifecycle

### Normal path

1. **Coordinator**: compacts old segments → writes
   `gc/<ulid>.staged.tmp`, fsyncs, renames to
   `gc/<ulid>.staged` (body with the new `inputs` field in the
   header, coordinator-signed pending volume re-sign).
2. **Volume** (inside `apply_gc_handoffs`):
   - Reads `gc/<ulid>.staged` entries + inputs list.
   - Reads each input's index, computes repack/remove actions.
   - Updates extent index in memory.
   - Writes a re-signed copy to `gc/<ulid>.tmp` (volume key over
     the same header+index bytes — deterministic).
   - `rename(gc/<ulid>.tmp, gc/<ulid>)` ← **atomic commit point**:
     the bare name appears.
   - `remove(gc/<ulid>.staged)` — cleanup (idempotent).
3. **Coordinator**: polls for bare-name presence, uploads to S3,
   writes `index/<ulid>.idx`, deletes `gc/<ulid>`.

### Crash recovery

The only filename states on disk are `.staged.tmp`, `.staged`,
`.tmp`, and bare. Every other distinction (which key signed the
header, whether S3 has the object) is resolved from content and is
idempotent under re-execution.

| # | When crash happens | On-disk state | Recovery |
|---|---|---|---|
| 1 | coordinator mid-write (before staged rename) | `.staged.tmp` | sweep stale tmps at startup |
| 2 | coordinator wrote `.staged`; volume hasn't applied | `.staged` (coordinator-signed) | volume's next apply tick picks it up |
| 3 | volume mid-apply, re-signed file not yet written | `.staged` | re-run apply — idempotent (extent index is rebuilt from disk on boot anyway) |
| 4 | volume mid-write of `.tmp` | `.staged` + partial `.tmp` | sweep stale `.tmp`, re-run apply |
| 5 | volume post-rename, pre-remove-staged | `.staged` + bare | bare wins; remove `.staged` |
| 6 | volume post-cleanup | bare | coordinator picks it up |
| 7 | coordinator post-upload, pre-delete | bare + S3 object | idempotent re-upload + delete on next tick |
| 8 | steady state | (gone) | — |

Why this works without intermediate filename states:

- **Re-apply is deterministic.** Inputs list + input `.idx` files +
  new segment's entries produce the same apply set on every run.
  Extent index updates are from fixed content, not clock or PRNG.
- **Re-sign is deterministic.** The hash input is
  `header[0..36] || index_bytes`, both fixed on disk. A torn or
  partial `.tmp` is discarded; a fresh `.tmp` regenerates byte-
  identical output.
- **Rename is the only commit.** No in-place mutation of
  `.staged`. Either `.tmp` was renamed to bare or it wasn't; there
  is no "half-committed" state in between.

Recovery rules (volume startup, walking `gc/`):

- `.staged.tmp` or `<ulid>.tmp` → remove (stale from crash).
- `.staged` only → re-run apply.
- `.staged` + bare → bare wins; remove `.staged`.
- bare only → already applied, no volume action; extent index
  rebuild picks it up.
- `.staged` with a vanished input `.idx` → warn-and-skip, same as
  today.

Recovery rules (coordinator startup, walking `gc/`):

- `.staged.tmp` → remove (stale from crash).
- `.staged` → wait; not the coordinator's turn.
- bare → queue for upload if not already uploaded (idempotent by
  S3 object existence or a coordinator-local cursor).

The current `.applied` and `.done` states disappear entirely.
"Applied" is "file is bare-named"; "done" is "file deleted after
upload succeeds."

## What changes

### Segment format (`elide-core/src/segment.rs`)

New segment format version bump (magic `ELIDSEG\x05` or
equivalent). Header extension: an `inputs_length` field and an
`inputs` section in the index region listing input ULIDs (16 bytes
each). Most segments have zero inputs; GC outputs have N > 0.

`read_segment_index` / `read_and_verify_segment_index` gain an
`inputs: Vec<Ulid>` return field. `write_segment` gains an
`inputs: &[Ulid]` parameter (defaulted to empty for normal WAL
promotes).

Per project policy (no-compat-by-default): no migration path. Old
segments in existing volumes get regenerated by user action — which
is acceptable since volumes are reproducible from their source data.

### GC handoff (`elide-core/src/gc.rs`)

- Delete `GcHandoffState`, `GcHandoff`, handoff file line parser.
- Delete manifest read/write helpers.
- The entire `gc.rs` module may collapse to a small set of ULID
  parsing helpers, or be removed.

### Volume apply (`elide-core/src/volume.rs::apply_gc_handoffs`)

- Walk `gc/` for `.staged` entries (not `.pending` files).
- For each: read the `.staged` body, extract `inputs`, compute the
  apply set from input/new segment diffs, update extent index,
  write a volume-signed copy to `<ulid>.tmp`, rename
  `<ulid>.tmp → <ulid>`, remove `<ulid>.staged`.
- Commit point is the tmp→bare rename. `.staged` is never mutated
  in place — re-apply on crash regenerates a byte-identical `.tmp`.
- Crash recovery: see the table in "Crash recovery" above.
  Summary: stale `.tmp` or `.staged.tmp` → remove; `.staged` alone
  → re-run apply; `.staged` + bare → bare wins.

### Coordinator GC (`elide-coordinator/src/gc.rs`)

- Compaction writes `gc/<ulid>.staged` and includes the input
  ULID list in the segment header.
- Instead of waiting for `.applied`, watch for bare-name
  appearance (or poll).
- Upload + cleanup on bare-name.
- The `.done` state, TTL-based retention of done files, etc.
  all disappear.

### Read path (`elide-core/src/segment.rs::locate_segment_body`)

- `gc/` branch becomes `if gc_dir.join(&sid).exists() { return Some((path, FullSegment)); }`
- No more `.applied` sidecar check.

### Rebuild (`elide-core/src/extentindex.rs`)

- `collect_gc_applied_segment_files` renamed to reflect that
  we're picking up bare-named GC bodies.
- Selector changes from "file has no extension AND `.applied`
  exists" to "file has no extension" (ULID-named).

### Tests

- `elide-core/tests/gc_index_test.rs` — handoff file shape changes.
- `elide-core/tests/common/mod.rs` — GC test helpers that build
  manifest files need to write inputs into segment headers instead.
- `elide-core/tests/gc_ordering_test.rs` — same.
- `elide-coordinator/tests/gc_test.rs`, `gc_proptest.rs` — adjust
  staging filename and commit protocol.
- `elide-core/src/segment.rs` tests — new
  `locate_segment_body` case: bare `gc/<ulid>` resolves without
  any sidecar.

### Docs

- `docs/formats.md` — segment format bump, new header field.
- `docs/operations.md` — GC handoff protocol section rewritten.
- `docs/architecture.md` — GC handoff diagram update if there is one.
- `docs/design-gc-ulid-ordering.md` — reference the new handoff
  filename shape (the ULID ordering invariant is unchanged).

## Open questions

1. **Input ULID list size bound.** Is there a practical cap on how
   many input segments can feed a single GC output? If so the
   header can use a fixed-width field; if not it needs a
   length-prefixed variable section. The compaction policy
   currently bounds inputs implicitly — worth confirming the
   worst case before picking a format.

2. **Handoff `remove` semantics without a manifest.** The current
   `remove <hash> <old_ulid>` line tells the volume to drop an
   extent index entry when the old segment is going away. Under
   derive-at-apply-time, the equivalent is: "hash was in old
   segment's index, is not in new segment's entries, and the
   extent index currently points at the old segment." The
   lowest-ULID-wins rule (extent index PR #23) means we should
   only drop the entry if the old segment is the one the index
   points at. This is the same logic, just derived on the volume
   side instead of prescribed by the coordinator.

3. **Zero-input GC outputs ("pure rewrite").** Does any current GC
   path produce a new segment with no inputs? Expect not (a GC
   output without inputs is just a normal segment), but worth
   checking so the volume can treat `inputs.is_empty()` as "not
   a GC segment, no apply work needed."

4. **Dead-input GC outputs.** The current `dead <old_ulid>` line
   covers old segments that had nothing live. Under derive-at-apply,
   a dead input is one whose entries are all absent from the new
   segment — the diff produces a pure-remove action set, which
   is fine.

5. **Interaction with delta body section.** GC compaction today
   copies body bytes verbatim. If a GC output carries delta
   entries, the delta body section needs to be copied through
   unchanged — double-check that the inputs-based diff logic
   handles Delta entries symmetrically with Data/Inline.

## Investment

Medium. Scope-bounded (segment format + GC protocol + tests) but
touches a delicate area with existing proptest coverage. Recommend
a dedicated branch. Build order:

1. Add `inputs` field to segment format + tests, with empty-input
   default. No behaviour change yet.
2. Coordinator: write `inputs` on GC output.
3. Volume: derive-at-apply path side-by-side with the existing
   manifest path, behind a feature check; both paths must agree.
4. Flip over: coordinator writes `.staged` instead of manifest,
   volume commits by renaming body. Manifest code deleted.
5. Read path: drop the `.applied` gate in `locate_segment_body`.

Steps 1–3 can land incrementally on main. Step 4 is a single
atomic switch. Step 5 is mechanical.

## Why this is worth doing

- **Removes a coupling** between readability state and handoff
  protocol state that has already caused at least one quiet bug
  (`delta_compute::read_source_extent` missing the `.applied`
  gate before the recent refactor).
- **Makes `ls gc/` tell the truth**: bare name = done, suffixed
  name = in flight. Matches how `pending/` already works.
- **Segments become self-describing about their GC derivation**,
  which is a property we'll want later for any post-hoc
  verification or auditing of GC history.
- **Deletes code**: `gc.rs` handoff format, state machine,
  manifest parser, `.pending/.applied/.done` lifecycle — all
  gone. The replacement lives inside the segment format, which
  we already have verification infrastructure for.
