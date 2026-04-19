---- MODULE HandoffProtocol ----
(*
  TLA+ model of the Elide self-describing GC handoff protocol.

  TODO: STALE — this spec describes the pre-plan-handoff protocol.
  The implementation has since moved to a plan-based handoff:
    - coordinator writes `gc/<ulid>.plan` (plaintext declarative records),
      not a `.staged` signed segment
    - volume materialises bodies from the plan (resolving DedupRef
      composites via extent_index) then signs and commits
    - partial-death sub-runs introduce fresh hashes in the output
    - `apply_plan_apply_result` performs a full lbamap rebuild from disk
  The "bare supersedes inputs" invariant originally found by this spec
  still holds (see `segment::discover_fork_segments` + its
  `collect_superseded_inputs` helper), but the state machine and
  actions below need rewriting to match `gc_plan.rs`, `gc_apply.rs`,
  and `volume::apply_plan_apply_result` before any new TLC runs.

  BACKGROUND
  ----------
  The coordinator compacts segments and hands the result to the volume via a
  two-state filename lifecycle on disk:

      gc/<ulid>.staged   coordinator wrote the new segment with an ephemeral
                         key; the segment header carries the sorted list of
                         input ULIDs (the `inputs_length` field). Volume has
                         not yet applied.
      gc/<ulid>          (bare name) volume re-signed the body and renamed it
                         from <ulid>.tmp; this rename is the atomic commit
                         point. Coordinator has not yet uploaded.
      (deleted)          coordinator finished apply_done_handoffs (upload to
                         S3 → promote IPC → delete old S3 → finalize IPC),
                         and the volume's finalize_gc_handoff handler removed
                         the bare body.

  No manifest sidecar exists. The volume's apply path reads the new segment's
  inputs field, walks each input's index/<input>.idx, and derives the
  extent-index updates by diffing the input entries against the new segment's
  entries.

  Step 1 — Coordinator stages:
    Writes gc/<ulid>.staged.tmp with the compacted segment (carries the input
    ULID list in its header), renames to gc/<ulid>.staged. Tombstone and
    removal-only handoffs produce real segments too — zero entries with a
    non-empty inputs list. Always atomic (tmp+rename).

  Step 2 — Volume applies (apply_staged_handoff):
    a. Reads gc/<ulid>.staged, walks each input's index/<input>.idx, and
       diffs to derive the extent-index updates. Carried hashes → "gc";
       LBA-dead hashes → "gone".
    b. Updates the in-memory extent index in one synchronous call.
    c. Writes a re-signed copy to gc/<ulid>.tmp (volume key).
    d. Renames gc/<ulid>.tmp → gc/<ulid> ← ATOMIC COMMIT POINT.
    e. Removes gc/<ulid>.staged.
    f. Evicts cache/<input>.{body,present} for each input. By this point the
       extent index no longer references any old hash that the new segment
       doesn't carry, so no concurrent reader can resolve via the old cache.

    Steps b–f are observable as one atomic transition: the actor publishes a
    fresh ReadSnapshot only after apply_gc_handoffs returns, so partial
    in-memory state is never visible to readers. The TLA+ model collapses
    them into a single VolumeFinishApply action.

  Step 3 — Coordinator cleans up (apply_done_handoffs):

    Step 3a — Upload the bare body to S3 (idempotent PUT). Tombstone bodies
              are uploaded too; the zero-entry segment is harmless.

    Step 3b — Promote IPC: the volume runs promote_segment, which writes
              index/<new>.idx and cache/<new>.{body,present} (skipped for
              tombstones — zero-entry segments produce no idx/cache), and
              deletes index/<input>.idx for each input segment (read from
              the new segment's inputs header field).

              ORDERING INVARIANT: index/<old>.idx is deleted here, BEFORE
              the old S3 object is deleted in Step 3d. This upholds:
                index/<ulid>.idx present  ↔  segment guaranteed to be in S3

    Step 3c — Delete old S3 objects for each input (404 = success).

    Step 3d — Finalize IPC: the volume's finalize_gc_handoff handler deletes
              the bare gc/<new> body. Routed through the actor for the
              same reason as the cache evict in Step 2 — single writer on
              gc/. Must happen after the S3 deletes; a crash between the
              S3 delete and the finalize would otherwise leak old S3
              objects forever (the bare body is the retry trigger for
              apply_done_handoffs, so deleting it before finishing the S3
              cleanup would lose the redo).

    Steps 3a–3d run inside one apply_done_handoffs call per bare file.
    Every sub-step is idempotent across coordinator crashes:
      * Re-PUT to S3 is idempotent.
      * promote_segment short-circuits when cache/<new>.body exists.
      * 404 on S3 delete is success.
      * finalize_gc_handoff is a no-op when the bare file is gone.

  TWO-STATE LIFECYCLE
  -------------------
  The previous protocol had four filename states (`bare gc/<ulid>` + `.pending`
  + `.applied` + `.done`). Under the self-describing protocol, only two
  filename states exist on disk: `.staged` and bare. Crash recovery is
  content-resolved:

    * `.staged.tmp` or `<ulid>.tmp`     → stale, swept on the next apply pass
    * `.staged` alone                   → re-run apply (deterministic, byte-
                                          identical output regardless of how
                                          many times apply restarts)
    * `.staged` + bare                  → bare wins; remove `.staged`
    * bare alone                        → already applied; coordinator's
                                          turn

  TOMBSTONE HANDOFFS
  ------------------
  When the coordinator's liveness analysis finds a segment with no live
  extents and no extent index references, it still produces a real GC
  output via write_gc_segment — but with zero entries and a non-empty
  inputs list. The volume applies this exactly like a normal handoff;
  the extent-index update is a no-op (no entries) but the bare file
  appears as the acknowledgment that the inputs are safe to delete.
  promote_segment recognises zero-entry GC outputs and skips writing
  index/<new>.idx + cache/<new>.body, only deleting the input idx files.
  The bare body is then deleted via finalize_gc_handoff the same way as
  a live output.

  RESTART-SAFETY (formerly Bug E)
  -------------------------------
  After a volume restart, the in-memory extent index is rebuilt from on-disk
  state. The new design is structurally safe:

    * If handoff is still .staged (no bare file): rebuild sees only the input
      .idx files; extent[h] = "old". The volume's next apply tick re-runs
      apply_staged_handoff and produces the bare file.

    * If handoff has reached bare (volume committed): rebuild scans gc/ for
      bare files via collect_gc_applied_segment_files. Bare files feed the
      extent index at LOWER priority than index/*.idx. So:
        - If old_idx_present (coordinator promote_segment hasn't run yet):
          rebuild reads the new segment's index from the bare body but the
          old .idx still wins → extent[h] = "old". Reads still resolve
          through the old segment (which is still in S3). The next
          coordinator tick runs CoordPromote, which writes index/<new>.idx
          and deletes index/<old>.idx. The next rebuild then sees the new
          state.
        - If ~old_idx_present (CoordPromote ran): index/<new>.idx exists,
          old .idx is gone, the bare file or new .idx wins → extent[h] = "gc"
          (carried) or extent[h] = "gone" (removed).

    * If handoff = cleaned (all done): bare body and old .idx are gone,
      index/<new>.idx is the canonical source. extent[h] = "gc" / "gone".

  At no point during this rebuild can extent[h] reference a missing segment.
  Bug E (the in-memory extent index going stale on restart and pointing at
  a freshly-deleted old segment) is impossible because old_present can only
  be FALSE after CoordFinalize fires, which itself is gated on
  ~old_idx_present (CoordPromote completed). By that time, index/<new>.idx
  is on disk and the rebuild always lands at "gc"/"gone".

  WHY RE-SIGN?
  ------------
  The coordinator does not hold the volume's private signing key. It stages
  the compacted segment with an ephemeral key. The volume reads the body,
  derives the action set, writes a re-signed copy to <ulid>.tmp, and renames
  to bare. The bare file's existence implies it is volume-signed by
  construction (no separate gc_seg_signed bookkeeping needed). After upload
  S3 always receives the volume-signed version.

  WHAT WE CHECK
  -------------
  TLC exhaustively explores every reachable state — all crash/restart
  interleavings, concurrent writes, and the per-step ordering constraints —
  and verifies five safety invariants and one liveness property:

    NoSegmentNotFound     — the extent index never references a missing segment
    NoLostData            — segments are only removed when no extent points to them
    OldOnlyDeletedAfterApplied
                          — the old segment is absent only after the volume has
                            applied (handoff ∈ {"bare", "cleaned"})
    OldIdxOnlyPresentWhenSegmentPresent
                          — index/<old>.idx is absent whenever the old S3 object
                            is absent; no dangling index entries after GC cleanup
    CacheOnlyAfterUpload  — cache/<new> is populated only after the bare commit
                            and the S3 upload (handoff ∈ {"bare", "cleaned"} ∧
                            gc_s3_uploaded)

    EventuallyDone        — under fair scheduling AND a bounded number of
                            crashes (see CONSTANT MaxCrashes), every handoff
                            eventually reaches handoff = "cleaned"

  HOW TO READ THIS
  ----------------
  TLA+ describes a system as:
    - VARIABLES: the current state
    - Actions: relations between the current and next state (written with ')
    - UNCHANGED: shorthand for "these variables are the same in the next state"
    - Init: the initial state predicate
    - Next: the disjunction of all possible actions (one fires per step)
    - Spec: Init ∧ □[Next]_vars  (always, Next fires or no variable changes)

  TLC checks that every state reachable from Init satisfies the INVARIANTS.

  INSTANTIATION EXAMPLES
  ----------------------
  Standard repack handoff (carried + removed):
    Carried <- {h1, h2},  Removed <- {h3},  Dead <- FALSE

  Removal-only handoff (no live extents to carry, but extent-index references
  to clean up):
    Carried <- {},         Removed <- {h1},  Dead <- FALSE

  Tombstone handoff (all-dead segment, no hashes at all):
    Carried <- {},         Removed <- {},    Dead <- TRUE
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Carried,    \* set of hashes whose extents are moved to GCOutput (repack entries)
  Removed,    \* set of hashes whose extent entries are deleted    (remove entries)
  Dead,       \* TRUE iff this is a tombstone handoff (all-dead input).
              \* For standard handoffs instantiate with FALSE.
              \* For tombstone handoffs instantiate with TRUE; Carried and Removed must be {}.
  MaxCrashes  \* Bound on the total number of CoordCrash + VolumeCrash events.
              \* TLC needs this to model bounded-crash liveness: under unbounded
              \* crashes, the adversary can interleave VolumeCrash and CoordCrash
              \* such that the two processes are never simultaneously up at the
              \* moment CoordPromote is enabled, so EventuallyDone fails as a
              \* matter of fairness theory rather than a real bug. Bounding
              \* crashes lets TLC explore all sequences up to MaxCrashes and
              \* then forces the system into a quiescent phase where SF on the
              \* progress actions guarantees handoff = "cleaned" is reached.
              \* MaxCrashes = 3 is enough to cover the interesting interleavings
              \* (including 2 vol + 1 coord, 1 vol + 2 coord, and the cycle TLC
              \* identified) without blowing up the state space.

\* Pairwise disjoint; at least one must contribute to the handoff.
ASSUME Carried \cap Removed = {}
ASSUME Dead => (Carried = {} /\ Removed = {})
ASSUME Carried \cup Removed # {} \/ Dead
ASSUME MaxCrashes \in Nat

VARIABLES
  handoff,        \* on-disk state of the GC handoff:
                  \*   "absent"   — nothing in gc/ for this output
                  \*   "staged"   — gc/<ulid>.staged exists; volume must apply
                  \*   "bare"     — bare gc/<ulid> exists; coordinator must finish cleanup
                  \*   "cleaned"  — apply_done_handoffs finished; bare body deleted

  extent,         \* map: hash → what the extent index currently says
                  \*   "old"  — still points to the original old segment (pre-GC)
                  \*   "gc"   — updated to point to the new bare body (Carried only)
                  \*   "gone" — entry removed from extent index (Removed only)
                  \*   "new"  — a concurrent write superseded this entry
                  \* For tombstone handoffs Hashes = {} so this map is empty.

  old_present,    \* TRUE iff the old (input) segment S3 object is still present.
                  \* Set FALSE only by CoordFinalize.

  old_idx_present, \* TRUE iff index/<old>.idx exists on disk.
                   \* Invariant: old_idx_present => old_present.
                   \* Cleared by CoordPromote (the promote_segment IPC) — production
                   \* defers the index/<old>.idx delete to promote_segment so that
                   \* the writes/deletes in promote_segment all happen inside a
                   \* single actor message, serialised with apply_gc_handoffs.

  bare_present,   \* TRUE iff bare gc/<ulid> exists on disk (volume-signed).
                  \* Set TRUE by VolumeFinishApply (the rename .tmp → bare commit).
                  \* Set FALSE by CoordFinalize (the finalize_gc_handoff IPC, which
                  \* runs inside the volume actor for single-writer-on-gc/ reasons).

  gc_s3_uploaded, \* TRUE iff coordinator has confirmed the bare body is uploaded to S3.
                  \* Set TRUE by CoordUploadGc. Reset to FALSE on coordinator crash
                  \* (no durable local record; coordinator re-uploads idempotently).

  new_seg_present, \* TRUE iff cache/<new>.body (volume-written) is present.
                   \* Set TRUE by CoordPromote (after the volume responds to the
                   \* promote IPC). Stays FALSE for tombstone handoffs (Carried = {})
                   \* because promote_segment recognises zero-entry outputs and
                   \* skips writing the cache body.

  old_cache_present,  \* TRUE iff cache/<old>.body and cache/<old>.present exist.
                      \* Initially TRUE (the input segment was cached locally before GC).
                      \* Set FALSE by VolumeFinishApply: eviction happens at the
                      \* commit point (rename .tmp → bare). By that instant the
                      \* in-memory extent index has been updated and the next
                      \* published ReadSnapshot will reflect it, so no concurrent
                      \* reader can still need cache/<old>.

  coord_up,       \* TRUE iff the coordinator is currently running
  vol_up,         \* TRUE iff the volume is currently running
  crashes_remaining  \* CoordCrash and VolumeCrash decrement this; they are
                     \* disabled at zero. See the rationale on the CONSTANT
                     \* MaxCrashes above.

vars == <<handoff, extent, old_present, old_idx_present, bare_present, gc_s3_uploaded, new_seg_present, old_cache_present, coord_up, vol_up, crashes_remaining>>

\* ---------------------------------------------------------------------------
\* Type correctness (checked as an invariant, useful for debugging the model)
\* ---------------------------------------------------------------------------

Hashes == Carried \cup Removed

TypeOK ==
  /\ handoff          \in {"absent", "staged", "bare", "cleaned"}
  /\ extent           \in [Hashes -> {"old", "gc", "gone", "new"}]
  /\ old_present      \in BOOLEAN
  /\ old_idx_present  \in BOOLEAN
  /\ bare_present     \in BOOLEAN
  /\ gc_s3_uploaded   \in BOOLEAN
  /\ new_seg_present  \in BOOLEAN
  /\ old_cache_present \in BOOLEAN
  /\ coord_up         \in BOOLEAN
  /\ vol_up           \in BOOLEAN
  /\ crashes_remaining \in 0..MaxCrashes

\* ---------------------------------------------------------------------------
\* Initial state
\* ---------------------------------------------------------------------------

Init ==
  /\ handoff          = "absent"
  /\ extent           = [h \in Hashes |-> "old"]   \* all entries pre-GC
  /\ old_present      = TRUE                        \* old segment exists in S3
  /\ old_idx_present  = TRUE                        \* index/<old>.idx exists (S3-confirmed)
  /\ bare_present     = FALSE                       \* no bare body yet
  /\ gc_s3_uploaded   = FALSE                       \* not yet uploaded
  /\ new_seg_present  = FALSE                       \* cache/<new>.body not yet written
  /\ old_cache_present = TRUE                       \* input segment was previously cached
  /\ coord_up         = TRUE
  /\ vol_up           = TRUE
  /\ crashes_remaining = MaxCrashes

\* ---------------------------------------------------------------------------
\* Coordinator actions
\* ---------------------------------------------------------------------------

(*
  Step 1: Coordinator writes gc/<ulid>.staged.tmp with the compacted segment
  (carrying the input ULID list in its header) and renames atomically to
  gc/<ulid>.staged. Modelled as a single atomic step.

  No bare body exists yet; the .staged file's existence is implicit in
  handoff = "staged".

  Tombstone and removal-only handoffs go through the same path — the
  segment is real but has zero entries with a non-empty inputs list.
*)
CoordWriteStaged ==
  /\ coord_up
  /\ handoff = "absent"
  /\ handoff' = "staged"
  /\ UNCHANGED <<extent, old_present, old_idx_present, bare_present, gc_s3_uploaded, new_seg_present, old_cache_present, coord_up, vol_up, crashes_remaining>>

(*
  Step 3a: Coordinator uploads the volume-signed bare body to S3.

  Idempotent: re-PUT is fine. gc_s3_uploaded = TRUE is the durable confirmation.

  Fires for every handoff including tombstones — the zero-entry body is
  uploaded too. It is harmless on S3 (nothing reads it) and gets deleted
  by CoordFinalize.

  No vol_up requirement: S3 upload does not involve the volume.
*)
CoordUploadGc ==
  /\ coord_up
  /\ handoff = "bare"
  /\ bare_present
  /\ ~gc_s3_uploaded
  /\ gc_s3_uploaded' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, bare_present, new_seg_present, old_cache_present, coord_up, vol_up, crashes_remaining>>

(*
  Step 3b: Coordinator sends promote_segment IPC. The volume's handler:

    * Writes index/<new>.idx and cache/<new>.{body,present} from the bare body
      (skipped for tombstones — zero-entry segments produce no idx/cache).
    * Deletes index/<input>.idx for each input segment, reading the input ULID
      list from the bare body's inputs header field.

  Both effects happen inside a single actor message handler so they are
  serialised with apply_gc_handoffs.

  Requires vol_up: this is an IPC round-trip. If the volume is down the
  coordinator retries on the next tick.

  Modelled as one atomic step: in production the body of promote_segment runs
  start-to-end on a single actor message.

  For tombstone / removal-only handoffs (Carried = {}) new_seg_present stays
  FALSE; only old_idx_present is cleared.
*)
CoordPromote ==
  /\ coord_up
  /\ vol_up
  /\ handoff = "bare"
  /\ gc_s3_uploaded
  /\ old_idx_present                  \* idempotency: only the first call clears
  /\ new_seg_present' = (Carried # {})
  /\ old_idx_present' = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, bare_present, gc_s3_uploaded, old_cache_present, coord_up, vol_up, crashes_remaining>>

(*
  Step 3c + 3d: Coordinator deletes the old S3 objects, then sends
  finalize_gc_handoff IPC. The volume's handler removes the bare body and
  the handoff is complete.

  Modelled as one atomic step. The two fs/network operations are split in
  production (delete loop, then IPC) but a coordinator crash between them
  re-runs apply_done_handoffs on the next tick: the bare body is still on
  disk, S3 deletes are idempotent (404 = success), and the finalize IPC is
  a no-op if the bare file is already gone.

  Precondition ~old_idx_present enforces the ordering: the old idx must be
  gone (CoordPromote completed) before the old S3 object is deleted. This
  upholds OldIdxOnlyPresentWhenSegmentPresent in all interleavings.

  Precondition new_seg_present \/ Carried = {}: promote_segment must have
  run. For tombstones the cache body is never written so the disjunction
  collapses to TRUE (Carried = {}), letting CoordFinalize fire after
  CoordPromote alone.

  Requires vol_up: finalize_gc_handoff is an IPC.
*)
CoordFinalize ==
  /\ coord_up
  /\ vol_up
  /\ handoff = "bare"
  /\ ~old_idx_present                            \* CoordPromote ran
  /\ (new_seg_present \/ Carried = {})           \* CoordPromote ran
  /\ old_present' = FALSE                        \* delete old S3 objects
  /\ bare_present' = FALSE                       \* finalize_gc_handoff IPC
  /\ handoff' = "cleaned"
  /\ UNCHANGED <<extent, old_idx_present, gc_s3_uploaded, new_seg_present, old_cache_present, coord_up, vol_up, crashes_remaining>>

CoordCrash ==
  /\ coord_up
  /\ crashes_remaining > 0
  /\ coord_up' = FALSE
  /\ gc_s3_uploaded' = FALSE   \* no durable local record; re-upload idempotently
  /\ crashes_remaining' = crashes_remaining - 1
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, bare_present, new_seg_present, old_cache_present, vol_up>>

\* On restart the coordinator re-reads gc/ and resumes at whatever state the
\* file is in. No state reset needed because the file state is persistent.
CoordRestart ==
  /\ ~coord_up
  /\ coord_up' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, bare_present, gc_s3_uploaded, new_seg_present, old_cache_present, vol_up, crashes_remaining>>

\* ---------------------------------------------------------------------------
\* Volume actions
\* ---------------------------------------------------------------------------

(*
  Step 2: Volume's apply_staged_handoff. Atomic from the actor's perspective:
  the message handler runs start-to-end on a single message and the next
  ReadSnapshot is published only after it returns. Effects:

    a. Read .staged body, walk each input's index/<input>.idx, derive updates.
    b. Update extent index (per-entry guard: skip if a concurrent write has
       superseded the hash).
    c. Write a re-signed copy to <ulid>.tmp.
    d. Rename .tmp → bare (commit point).
    e. Remove .staged.
    f. Evict cache/<input>.{body,present} for each input.

  In TLA+ all of this is one transition: extent index applied, bare_present
  set TRUE, old_cache_present set FALSE, handoff transitioned to "bare".

  No separate VolumeReSigns action: the bare file's existence implies it is
  volume-signed by construction. There is no observable in-between state
  where the body in gc/ is volume-signed but the bare name doesn't yet exist
  (the rename IS the commit).

  No per-entry VolumeApplyCarried/VolumeApplyRemoved actions: the apply path
  is a single synchronous function call. Per-entry partial in-memory state is
  not observable to readers (the snapshot publish happens after apply returns)
  and is lost on crash (the extent index is rebuilt from disk on restart).
  TLC's per-step exploration on the OTHER actions (CoordCrash, VolumeCrash,
  NewerWrite) is sufficient to cover the relevant interleavings.

  Tombstone handoffs: Hashes = {} so the extent update is a no-op. The bare
  file is still produced (zero entries with a non-empty inputs list) and is
  the volume's acknowledgment that the input is safe to delete.
*)
VolumeFinishApply ==
  /\ vol_up
  /\ handoff = "staged"
  \* Apply per-entry guards: only update entries still pointing at "old".
  /\ extent' = [h \in Hashes |->
                  IF extent[h] = "old"
                  THEN IF h \in Carried THEN "gc" ELSE "gone"
                  ELSE extent[h]]
  /\ handoff'           = "bare"
  /\ bare_present'      = TRUE              \* rename .tmp → bare
  /\ old_cache_present' = FALSE             \* evict input cache files at commit
  /\ UNCHANGED <<old_present, old_idx_present, gc_s3_uploaded, new_seg_present, coord_up, vol_up, crashes_remaining>>

VolumeCrash ==
  /\ vol_up
  /\ crashes_remaining > 0
  /\ vol_up' = FALSE
  /\ crashes_remaining' = crashes_remaining - 1
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, bare_present, gc_s3_uploaded, new_seg_present, old_cache_present, coord_up>>

(*
  On restart the volume rebuilds its in-memory extent index from on-disk
  state via extentindex::rebuild (elide-core/src/extentindex.rs). The pass
  order is:

    1. paths loop: pending/<ulid> + bare gc/<ulid> files, fed to
       insert_if_absent. sort_for_rebuild puts bare gc/ entries first so
       they win over pending/ entries when the same hash appears in both.
    2. cache_paths loop: index/*.idx files, fed to insert_if_absent.

  Because both loops use insert_if_absent (first-write-wins), an entry
  inserted by the bare gc/<ulid> file in step 1 makes the matching entry
  in old/new index/*.idx in step 2 a no-op.

  Critical filter: before the cache_paths loop, the rebuild reads the
  `inputs` field of every bare gc/<ulid> file in this fork and skips
  index/<input>.idx for each declared input. Without this filter, a
  rebuild during the bare phase would re-introduce Removed entries from
  the old idx file that VolumeFinishApply had explicitly removed from
  the in-memory index — a regression discovered by this TLA+ model and
  pinned by gc_staged_crash_in_bare_phase_drops_removed_extents in
  volume.rs.

  Decision tree for extent[h] after rebuild:

    * extent[h] = "new" (concurrent write): always preserved.

    * h ∈ Carried (the new GC output carries this hash):
        - bare_present: step 1 inserts h → new ulid → extent[h] = "gc".
          (The cache_paths filter then drops every input idx; the new
          idx, if it exists, is also processed but already-present.)
        - ~bare_present /\ new_seg_present: bare body was deleted by
          CoordFinalize but CoordPromote left index/<new>.idx on disk.
          The cache_paths loop processes index/<new>.idx → extent[h] = "gc".
          (Old idx is also gone — CoordPromote cleared it.)
        - ~bare_present /\ ~new_seg_present: handoff is still "absent"
          or "staged"; no bare body, no new idx. cache_paths processes
          index/<old>.idx → extent[h] = "old".

    * h ∈ Removed (the new GC output does NOT carry this hash):
        - bare_present: bare body's inputs filter drops the input idx
          files; cache_paths skips them. New idx doesn't carry h either.
          Nothing inserts h → extent[h] = "gone".
        - ~bare_present /\ old_idx_present: handoff is "absent" or
          "staged"; no filter applies. cache_paths processes
          index/<old>.idx → extent[h] = "old".
        - ~bare_present /\ ~old_idx_present: post-CoordFinalize. Old idx
          gone, new idx doesn't carry h, nothing inserts → "gone".

  This is structurally safe: by the time old_present can become FALSE
  (only in CoordFinalize, which is gated on ~old_idx_present and sets
  bare_present = FALSE), every Carried h has been re-routed to the new
  segment and every Removed h has been turned into "gone". No restart
  can leave extent[h] = "old" pointing at a deleted segment.
*)
VolumeRestart ==
  /\ ~vol_up
  /\ vol_up' = TRUE
  /\ extent' = [h \in Hashes |->
                  IF extent[h] = "new" THEN "new"
                  ELSE IF h \in Carried THEN
                    IF bare_present \/ new_seg_present THEN "gc"
                    ELSE "old"
                  ELSE \* h \in Removed
                    IF bare_present THEN "gone"
                    ELSE IF old_idx_present THEN "old"
                    ELSE "gone"]
  /\ UNCHANGED <<handoff, old_present, old_idx_present, bare_present, gc_s3_uploaded, new_seg_present, old_cache_present, coord_up, crashes_remaining>>

\* ---------------------------------------------------------------------------
\* Environment: concurrent writes
\* ---------------------------------------------------------------------------

(*
  A write to an LBA covered by a hash in Hashes arrives at any time — before,
  during, or after the handoff. This models the coordinator's snapshot being
  stale by the time the volume applies.

  A newer write supersedes:
    - "old": write arrived after the GC snapshot but before the volume
             applied the entry. VolumeFinishApply's per-entry guard skips
             "new" entries, so the carried position is correctly preserved.
    - "gc":  write arrived after the volume applied the entry. The extent
             index now points to the write's own segment, not the GC
             output. The carried extent in the bare body becomes orphaned
             (space leak, not corruption: it is never read).

  We do not model superseding "gone" or "new" — those hashes are no
  longer owned by the old segment.

  For tombstone handoffs Hashes = {} so NewerWrite never fires. This is
  correct: a fully-dead segment has no hashes anyone could write to.
*)
NewerWrite(h) ==
  /\ extent[h] \in {"old", "gc"}
  /\ extent' = [extent EXCEPT ![h] = "new"]
  /\ UNCHANGED <<handoff, old_present, old_idx_present, bare_present, gc_s3_uploaded, new_seg_present, old_cache_present, coord_up, vol_up, crashes_remaining>>

\* ---------------------------------------------------------------------------
\* Specification
\* ---------------------------------------------------------------------------

Next ==
  \/ CoordWriteStaged
  \/ CoordUploadGc
  \/ CoordPromote
  \/ CoordFinalize
  \/ CoordCrash
  \/ CoordRestart
  \/ VolumeFinishApply
  \/ VolumeCrash
  \/ VolumeRestart
  \/ \E h \in Hashes : NewerWrite(h)

(*
  Fairness conditions for liveness.

  WF_vars(Restart): restart actions are continuously enabled while an actor
  is down; weak fairness is enough to bring them back.

  SF_vars(progress): progress actions require the actor to be UP. Crashes
  can interrupt them, so they are never CONTINUOUSLY enabled in a trace
  with unbounded crashes — weak fairness would never fire. Strong fairness
  fires whenever an action is enabled INFINITELY OFTEN, which is exactly
  what the restart loop guarantees.

  Crashes and NewerWrite are unconstrained — they are adversarial.
*)
Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(CoordRestart)
  /\ WF_vars(VolumeRestart)
  /\ SF_vars(CoordWriteStaged)
  /\ SF_vars(CoordUploadGc)
  /\ SF_vars(CoordPromote)
  /\ SF_vars(CoordFinalize)
  /\ SF_vars(VolumeFinishApply)

\* ---------------------------------------------------------------------------
\* Safety invariants
\* ---------------------------------------------------------------------------

(*
  Core safety: the extent index never holds a reference to a segment that is
  not present.

  - extent = "old"  references the original input segment (old_present)
  - extent = "gc"   references the new GC output, which may be in two places:
                      * bare gc/<ulid>:           bare_present
                      * cache/<ulid>.body:        new_seg_present
                    The body must be in at least one of these locations.
  - extent = "gone" means the entry was deleted (no reference)
  - extent = "new"  means a concurrent write's segment is in use
                    (always present by assumption — writes land in pending/)

  For tombstone handoffs Hashes = {} so both quantifiers are vacuously true.
*)
NoSegmentNotFound ==
  /\ \A h \in Hashes  : extent[h] = "old" => old_present
  /\ \A h \in Carried : extent[h] = "gc"  => bare_present \/ new_seg_present

(*
  Segments are only removed when nothing in the extent index points to them
  any more. Equivalently: if a segment is absent, no extent references it.

  For tombstone handoffs Hashes = {} so both quantifiers are vacuously true.
*)
NoLostData ==
  /\ (~old_present => \A h \in Hashes : extent[h] # "old")
  /\ (\A h \in Carried :
        ~(bare_present \/ new_seg_present) => extent[h] # "gc")

(*
  Coordinator deletion invariant: the old segment is absent only after the
  volume has acknowledged the handoff (handoff has reached "bare" or beyond).

    "The coordinator never deletes a segment without the volume's
     acknowledgment via the handoff protocol."

  Under the new protocol the volume's acknowledgment is the rename
  .tmp → bare in VolumeFinishApply. Once handoff = "bare", the volume has
  applied the extent index updates and committed them on disk. The
  coordinator can then proceed with cleanup safely.

  This invariant is the binding constraint for tombstone handoffs (where
  NoSegmentNotFound and NoLostData are vacuously true).
*)
OldOnlyDeletedAfterApplied ==
  ~old_present => handoff = "cleaned"

(*
  Index file ordering: index/<old>.idx may only exist while the old S3
  object exists.

    old_idx_present => old_present

  This is the structural fix for the historical "segment not found in any
  ancestor" bug: the rebuild from index/*.idx must never produce an entry
  pointing to a segment that has been deleted from S3.

  CoordFinalize is gated on ~old_idx_present, so old_present can only
  become FALSE after CoordPromote (which clears old_idx_present) has run.
*)
OldIdxOnlyPresentWhenSegmentPresent ==
  old_idx_present => old_present

(*
  cache/<new>.body is populated only after the volume's commit. CoordPromote
  requires gc_s3_uploaded and handoff = "bare" before writing the cache body,
  so the body cannot appear in any earlier handoff state.

  The invariant does NOT mention gc_s3_uploaded directly: that variable is
  reset to FALSE on coordinator crash (no durable local record), but the
  cache body's presence on disk is durable evidence that the upload did
  happen at some earlier point — re-asserting gc_s3_uploaded after a crash
  would falsify a perfectly valid state.
*)
CacheOnlyAfterUpload ==
  new_seg_present => handoff \in {"bare", "cleaned"}

\* ---------------------------------------------------------------------------
\* Liveness property
\* ---------------------------------------------------------------------------

(*
  The handoff eventually completes.

  Holds under the WF/SF fairness conditions in Spec above. As before, this
  does NOT require liveness under unbounded crashes — only under fair
  scheduling, where every progress action eventually fires in some
  restart window.
*)
EventuallyDone == <>(handoff = "cleaned")
====
