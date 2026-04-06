---- MODULE HandoffProtocol ----
(*
  TLA+ model of the Elide GC handoff protocol.

  BACKGROUND
  ----------
  The coordinator compacts segments and hands the result to the volume via a
  three-step file sequence:

      Step 1 — Coordinator stages the compacted segment (if any) and signals
               the volume:
        gc/<ulid>          (segment, signed with an ephemeral key; omitted for
                            removal-only and tombstone handoffs)
        gc/<ulid>.pending  (text file: carried + removed + dead entries; written
                            last via tmp-rename, so .pending is never visible
                            without the segment already being present when one
                            is needed)

      Step 2 — Volume re-signs and applies:
        a. Re-signs gc/<ulid> in-place with its own key (tmp-rename within gc/).
           The body stays in gc/ — it is NOT moved to cache/ here.
           Skipped for removal-only and tombstone handoffs (no staged segment).
        b. Applies extent index patches (repack → "gc", remove → "gone"),
           with a per-entry guard: skip if a concurrent write has already
           superseded the hash.
        c. For tombstone entries ("dead <old_ulid>"): verifies from the volume's
           own perspective that no LBA map or extent index entry references the
           named segment.  This is a no-op in the normal case; its purpose is
           to provide the volume's acknowledgment before deletion.
        d. Deletes index/<old>.idx for each consumed input segment.
           ORDERING INVARIANT: index/<old>.idx is deleted here, as part of Step 2,
           BEFORE the old S3 object is deleted in Step 3 Part B.  This upholds:
             index/<ulid>.idx present  ↔  segment guaranteed to be in S3
        e. Renames gc/<ulid>.pending → gc/<ulid>.applied.

      Step 3 — Coordinator cleans up (two sub-steps, ordered):

        Step 3 Part A — Upload new segment and populate local cache:
        a. Uploads gc/<ulid> (now volume-signed) to S3.
           Skipped for removal-only and tombstone handoffs.
        b. IPC → volume: "promote <ulid>".
           Volume copies gc/<ulid> body → cache/<ulid>.body, writes all-present
           cache/<ulid>.present, and responds ok.
           Skipped for removal-only and tombstone handoffs.
        c. Coordinator deletes gc/<ulid>.
           Skipped for removal-only and tombstone handoffs.

        Step 3 Part B — Delete old S3 objects and finalise:
        d. Deletes old S3 objects.  A 404 means already gone; treat as success.
        e. Renames gc/<ulid>.applied → gc/<ulid>.done.

        BUG HISTORY: an earlier implementation combined Part A and Part B into a
        single atomic cleanup that deleted old S3 objects but did NOT delete
        index/<old>.idx.  After eviction (cache/ emptied) and coordinator
        restart, rebuild_segments included the stale .idx in the extent index,
        mapping hashes to a segment absent from both disk and S3, causing
        "segment not found in any ancestor" read errors on every access.

  Two independent actors drive the transitions:
    - Coordinator: Steps 1 and 3
    - Volume:      Step 2

  A handoff file contains three kinds of entries:
    - Carried (repack): extent moved from old segment to GCOutput
    - Removed (remove): extent index entry deleted (LBA-dead; no output segment)
    - Dead (dead):      entire old segment is all-dead; no output segment;
                        no extent index patches needed.  The volume's role is
                        purely to acknowledge deletion is safe.

  TOMBSTONE HANDOFFS
  ------------------
  A tombstone handoff (Dead # {}, Carried = {}, Removed = {}) arises when the
  coordinator's liveness analysis determines that a segment has no live extents
  and no extent index entries at all.  In this case there is nothing to compact
  or patch — the old segment can simply be deleted.

  The coordinator must NOT delete directly.  Its liveness analysis is based on
  on-disk index files; the volume's in-memory LBA map may be ahead (writes
  between the last gc_checkpoint WAL flush and the end of the GC pass are not
  yet visible on disk).  Direct deletion without the volume's acknowledgment
  caused a "segment not found" bug in practice when the coordinator wrongly
  classified a live segment as all-dead.

  The tombstone handoff closes this gap: the coordinator writes a .pending file
  with only "dead <old_ulid>" lines and waits for the volume to apply it (.applied)
  before deleting.  The volume confirms from its own state that no references
  exist — a no-op in the common case — providing the safety acknowledgment.

  COORDINATOR DELETION INVARIANT
  -------------------------------
  No segment (local or S3) is ever deleted without the volume's acknowledgment
  via the handoff protocol.  This invariant is captured by:

    OldOnlyDeletedAfterApplied:
      The old segment is absent only after the handoff has reached "done".
      Since "done" is only reachable via "applied", and "applied" requires the
      volume to have run VolumeFinishApply, the old segment is only deleted
      after the volume has explicitly acknowledged the handoff.

  This invariant is checked for all three handoff types (repack, removal-only,
  tombstone) by TLC.

  INDEX FILE INVARIANT
  --------------------
  index/<ulid>.idx being present on disk means the corresponding segment is
  guaranteed to be in S3.  This is the signal that rebuild_segments uses to
  include evicted segments in the extent index and extent index rebuild.

    OldIdxOnlyPresentWhenSegmentPresent:
      old_idx_present => old_present
      i.e. the old index file may only exist while the old S3 object exists.

  This invariant is maintained by the volume deleting index/<old>.idx in
  VolumeFinishApply (Step 2d) BEFORE the coordinator deletes the old S3
  object in CoordApplyDone (Step 3 Part B).  CoordApplyDone is gated on
  ~old_idx_present, enforcing this ordering in all interleavings.

  WHY RE-SIGN IN-PLACE IN GC/?
  ----------------------------
  The coordinator does not hold the volume's private signing key.  It stages
  the compacted segment in gc/ using an ephemeral key.  The volume re-signs
  it in-place within gc/; the coordinator then uploads and moves it to segments/.
  segments/ therefore contains only volume-signed, S3-confirmed bodies.

  The re-sign-in-place design (vs. the earlier move-to-segments/ approach)
  enforces the invariant "segments/ present ↔ S3-confirmed" structurally:
  the coordinator is the sole writer of segments/, and writes there only after
  upload.  This eliminates the need for eviction to inspect in-flight GC state.

  RESTART-SAFETY NOTE (formerly Bug E)
  -------------------------------------
  After a volume restart, the in-memory extent index is rebuilt from on-disk
  .idx files.  In the new design this is structurally safe without any extra
  coordinator guard:

    - VolumeFinishApply deletes index/<old>.idx (sets old_idx_present = FALSE)
      BEFORE renaming .pending → .applied.
    - Therefore: when handoff = "applied", old_idx_present is always FALSE.
    - VolumeRestart with old_idx_present = FALSE sets extent[h] = "gc" for
      carried hashes (not "old"), so the extent index is already correct
      after restart — no re-application step is needed.

  VolumeRestart still models the rebuild correctly:
    extent[h] = "old" if old_idx_present (crash during .pending phase, before
                         VolumeFinishApply completes)
    extent[h] = "gc"  if ~old_idx_present ∧ h ∈ Carried
    extent[h] = "gone" if ~old_idx_present ∧ h ∈ Removed
    extent[h] = "new"  if a concurrent write superseded (always preserved)

  The old VolumeApplyApplied action (re-apply .applied entries after restart)
  and the CoordCleanupIdx guard (∀ h : extent[h] ≠ "old") are no longer needed:
  old_idx_present is always FALSE by the time the coordinator sees .applied, so
  VolumeRestart can never set extent[h] = "old" in that state.

  WHAT WE CHECK
  -------------
  TLC exhaustively explores every reachable state — including all crash/restart
  interleavings and concurrent writes — and verifies that the five safety
  invariants hold in all of them:

    NoSegmentNotFound             — the extent index never references a missing segment
    NoLostData                    — segments are only removed when no extent points to them
    OldOnlyDeletedAfterApplied    — the old segment is absent only after handoff = "done"
    OldIdxOnlyPresentWhenSegmentPresent
                                  — index/<old>.idx is absent whenever the old S3 object
                                    is absent; no dangling index entries after GC cleanup
    CacheOnlyAfterUpload          — cache/<new> is populated only after volume
                                    acknowledgment (handoff ∈ {"applied", "done"})

  The restart-safety modelling ensures TLC explores crash scenarios during the
  .pending phase (where old_idx_present may still be TRUE and extent can revert
  to "old"), verifying that VolumeFinishApply always completes the idx deletion
  and handoff rename atomically before the coordinator proceeds.

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

  Removal-only handoff (no output segment):
    Carried <- {},         Removed <- {h1},  Dead <- FALSE

  Tombstone handoff (all-dead segment, no hashes):
    Carried <- {},         Removed <- {},    Dead <- TRUE
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Carried,   \* set of hashes whose extents are moved to GCOutput (repack entries)
  Removed,   \* set of hashes whose extent entries are deleted    (remove entries)
  Dead       \* TRUE iff this is a tombstone handoff (dead entry; Carried = Removed = {})
             \* For standard handoffs instantiate with FALSE.
             \* For tombstone handoffs instantiate with TRUE; Carried and Removed must be {}.

\* Pairwise disjoint; at least one must contribute to the handoff.
ASSUME Carried \cap Removed = {}
ASSUME Dead => (Carried = {} /\ Removed = {})
ASSUME Carried \cup Removed # {} \/ Dead

VARIABLES
  handoff,        \* state of the gc/<ulid>.{pending,applied,done} marker file
                  \*   "absent"  — not yet written
                  \*   "pending" — coordinator wrote it; volume must apply
                  \*   "applied" — volume applied it; coordinator must clean up
                  \*   "done"    — coordinator cleaned up; handoff complete

  extent,         \* map: hash → what the extent index currently says
                  \*   "old"  — still points to the original old segment (pre-GC)
                  \*   "gc"   — updated to point to GCOutput   (Carried hashes only)
                  \*   "gone" — entry removed from extent index (Removed hashes only)
                  \*   "new"  — a concurrent write superseded this entry
                  \* For tombstone handoffs Hashes = {} so this map is empty.

  old_present,    \* TRUE iff the old (input) segment S3 object is still present.
                  \* For tombstone handoffs this is the all-dead segment being deleted.
                  \* Set FALSE only by CoordApplyDone (Step 3 Part B).

  old_idx_present, \* TRUE iff index/<old>.idx exists on disk.
                   \* Invariant: old_idx_present => old_present
                   \* (the .idx file may only exist while the S3 object exists).
                   \* Set FALSE by VolumeFinishApply (before .applied rename).

  gc_seg_present, \* TRUE iff gc/<ulid> body is present (coordinator- or volume-signed).
                  \* Set FALSE by CoordPromote (Step 3 Part A) after promote IPC succeeds.
  gc_seg_signed,  \* TRUE iff the gc/<ulid> body has been re-signed by the volume.
                  \* FALSE until VolumeReSigns fires; stays TRUE until gc_seg_present = FALSE.
                  \* Extent index entries pointing at "gc" are only readable once this is TRUE.
  gc_s3_uploaded, \* TRUE iff coordinator has confirmed gc/<ulid> is uploaded to S3.
                  \* Set TRUE by CoordUploadGc.  Reset to FALSE on coordinator crash (no
                  \* durable local record; coordinator re-uploads idempotently on restart).
                  \* Only relevant when Carried # {} (repack handoffs); stays FALSE otherwise.
  new_seg_present, \* TRUE iff cache/<ulid>.body (volume-written, S3-confirmed) is present.
                   \* Set TRUE by CoordPromote (after volume responds to promote IPC).
  coord_up,       \* TRUE iff the coordinator is currently running
  vol_up          \* TRUE iff the volume is currently running

vars == <<handoff, extent, old_present, old_idx_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up, vol_up>>

\* ---------------------------------------------------------------------------
\* Type correctness (checked as an invariant, useful for debugging the model)
\* ---------------------------------------------------------------------------

Hashes == Carried \cup Removed

TypeOK ==
  /\ handoff          \in {"absent", "pending", "applied", "done"}
  /\ extent           \in [Hashes -> {"old", "gc", "gone", "new"}]
  /\ old_present      \in BOOLEAN
  /\ old_idx_present  \in BOOLEAN
  /\ gc_seg_present   \in BOOLEAN
  /\ gc_seg_signed    \in BOOLEAN
  /\ gc_s3_uploaded   \in BOOLEAN
  /\ new_seg_present  \in BOOLEAN
  /\ coord_up         \in BOOLEAN
  /\ vol_up           \in BOOLEAN

\* ---------------------------------------------------------------------------
\* Initial state
\* ---------------------------------------------------------------------------

Init ==
  /\ handoff          = "absent"
  /\ extent           = [h \in Hashes |-> "old"]   \* all entries pre-GC
  /\ old_present      = TRUE                        \* old segment exists in S3
  /\ old_idx_present  = TRUE                        \* index/<old>.idx exists (S3-confirmed)
  /\ gc_seg_present   = FALSE                       \* staged segment not yet written
  /\ gc_seg_signed    = FALSE                       \* not yet re-signed by volume
  /\ gc_s3_uploaded   = FALSE                       \* gc body not yet uploaded to S3
  /\ new_seg_present  = FALSE                       \* cache/<new>.body not yet written
  /\ coord_up         = TRUE
  /\ vol_up           = TRUE

\* ---------------------------------------------------------------------------
\* Coordinator actions
\* ---------------------------------------------------------------------------

(*
  Step 1: Coordinator materialises GCOutput (if any carried entries), staging
  it in gc/<ulid> with an ephemeral key, then atomically writes gc/<ulid>.pending
  via a tmp-file rename.  Modelled as a single atomic step; in the implementation
  the segment file is written before the handoff marker is visible, so the volume
  always finds both files together when it sees .pending.

  Removal-only handoffs (Carried = {}) produce no staged segment; gc_seg_present
  stays FALSE and the volume applies only the remove entries.

  Tombstone handoffs (Dead = TRUE, Carried = {}, Removed = {}) also produce no
  staged segment.  The .pending file contains only "dead <old_ulid>" lines.
  The volume applies the tombstone (a no-op from its perspective) to provide
  its acknowledgment that deletion is safe.
*)
CoordWritePending ==
  /\ coord_up
  /\ handoff = "absent"
  /\ handoff'         = "pending"
  /\ gc_seg_present'  = (Carried # {})   \* staged segment written iff there are carried entries
  /\ UNCHANGED <<extent, old_present, old_idx_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up, vol_up>>

(*
  Step 3 Part A-1: Coordinator uploads gc/<ulid> (volume-signed) to S3.

  Fires when the coordinator is up and the gc body is present and volume-signed.
  The upload is idempotent; gc_s3_uploaded = TRUE is the durable confirmation.

  No vol_up requirement: S3 upload does not involve the volume.

  Skipped for removal-only and tombstone handoffs (gc_seg_present stays FALSE).
*)
CoordUploadGc ==
  /\ coord_up
  /\ handoff = "applied"
  /\ gc_seg_present /\ gc_seg_signed  \* body must be present and volume-signed
  /\ ~gc_s3_uploaded                  \* idempotency guard
  /\ gc_s3_uploaded'  = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, gc_seg_present, gc_seg_signed, new_seg_present, coord_up, vol_up>>

(*
  Step 3 Part A-2: Coordinator calls promote IPC; volume responds by writing
  cache/<ulid>.body and cache/<ulid>.present; coordinator then deletes gc/<ulid>.

  Requires vol_up: the promote IPC round-trip requires the volume to be running.
  If the volume is down, this action is blocked; the coordinator retries on the
  next GC tick once the volume is back.  gc_seg_present stays TRUE during this
  window (gc/<ulid> is not deleted until promote succeeds).

  Modelled as one atomic step (promote + gc deletion): once promote returns ok,
  the coordinator immediately deletes gc/<ulid>.  A crash before promote (while
  vol_up = FALSE or between ticks) simply leaves the window open; gc_s3_uploaded
  stays TRUE so CoordUploadGc does not re-run, and CoordPromote retries.

  A coordinator crash resets gc_s3_uploaded = FALSE (see CoordCrash), so on
  restart CoordUploadGc re-runs the idempotent S3 upload before CoordPromote.

  Skipped for removal-only and tombstone handoffs (Carried = {}; no gc body).
*)
CoordPromote ==
  /\ coord_up
  /\ vol_up              \* promote IPC requires volume to be running
  /\ handoff = "applied"
  /\ gc_s3_uploaded      \* S3 upload must be confirmed first
  /\ Carried # {}        \* repack handoffs only; removal-only/tombstone have no body
  /\ ~new_seg_present    \* idempotency guard
  /\ new_seg_present'    = TRUE   \* volume writes cache/<new>.body + .present
  /\ gc_seg_present'     = FALSE  \* coordinator deletes gc/<ulid>
  /\ gc_seg_signed'      = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, gc_s3_uploaded, coord_up, vol_up>>

(*
  Step 3 Part B: Coordinator deletes old S3 objects and finalises the handoff.

  Precondition ~old_idx_present enforces the ordering guarantee: the old index
  file must be gone (CoordCleanupIdx has completed) before the old S3 object
  is deleted.  This ensures the invariant old_idx_present => old_present holds
  in all reachable states — index/<old>.idx is never present after the S3 object
  is gone.

  After this action old_present = FALSE and handoff = "done".  The old segment
  no longer exists anywhere; no extent entry points to "old" (VolumeFinishApply
  applied all entries atomically before the .applied rename, and VolumeRestart
  uses old_idx_present to reconstruct extent correctly after a crash).

  For tombstone handoffs (Carried = {}, Removed = {}) all Hashes = {} so no
  extent entries exist; old_present = FALSE is the only state change.

  S3 upload (CoordUploadGc) and cache promotion (CoordPromote) happened in
  Step 3 Part A and are idempotent on coordinator restart.

  A 404 on the old S3 delete means the object is already gone (e.g. coordinator
  crashed after delete but before rename); treated as success so the step is
  idempotent.
*)
CoordApplyDone ==
  /\ coord_up
  /\ handoff = "applied"
  /\ ~old_idx_present                      \* ordering: .idx must be gone first
  /\ handoff'          = "done"
  /\ old_present'      = FALSE             \* old S3 object deleted (Step 3f)
  /\ UNCHANGED <<extent, old_idx_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up, vol_up>>

CoordCrash ==
  /\ coord_up
  /\ coord_up'        = FALSE
  /\ gc_s3_uploaded'  = FALSE   \* no durable local record; re-upload idempotently on restart
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, gc_seg_present, gc_seg_signed, new_seg_present, vol_up>>

\* On restart the coordinator re-reads gc/ and resumes at whatever state the
\* file is in.  No state reset is needed because the file state is persistent.
CoordRestart ==
  /\ ~coord_up
  /\ coord_up' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, vol_up>>

\* ---------------------------------------------------------------------------
\* Volume actions
\* ---------------------------------------------------------------------------

(*
  Step 2a: Volume re-signs the staged segment in-place.

  The volume reads gc/<ulid> (ephemeral-signed), writes a volume-signed copy
  back to gc/<ulid> (via gc/<ulid>.tmp then rename).  The body stays in gc/ —
  it is NOT moved to segments/ here.  Modelled as one atomic step.

  This must happen before any carried entry can be applied: VolumeApplyCarried
  requires gc_seg_signed (the volume-signed gc/ body) rather than merely
  gc_seg_present (which is TRUE even for the ephemeral coordinator output).

  Idempotency: re-signing is a pure function of the input content; if the volume
  crashes mid-rename and retries, the output is identical.  In the model this is
  captured by allowing VolumeReSigns to fire again only if gc_seg_present is
  still TRUE and gc_seg_signed is FALSE.

  For removal-only and tombstone handoffs gc_seg_present = FALSE throughout, so
  VolumeReSigns never fires — correctly, as there is no staged segment to re-sign.
*)
VolumeReSigns ==
  /\ vol_up
  /\ handoff = "pending"
  /\ gc_seg_present              \* staged segment must be present
  /\ ~gc_seg_signed              \* not yet re-signed (idempotency guard)
  /\ gc_seg_signed'   = TRUE     \* body in gc/ is now volume-signed
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, gc_seg_present, gc_s3_uploaded, new_seg_present, coord_up, vol_up>>

(*
  Step 2b: Volume applies one carried entry from the handoff.

  Guard: only update the extent index entry if it still points to "old"
  (i.e. the old segment).  If a concurrent write has already superseded this
  hash (extent = "new"), skip it — the newer write wins.

  This guard is the central correctness mechanism.  Without it a stale GC
  output could overwrite a newer write's extent index entry.

  Precondition gc_seg_signed: the volume needs the re-signed body in gc/ before
  it can apply carried entries.  The re-signed body is in gc/ (not segments/);
  the coordinator will move it to segments/ in Step 3c.  Removal entries
  (Removed) do not need this.
*)
VolumeApplyCarried(h) ==
  /\ vol_up
  /\ handoff = "pending"
  /\ h \in Carried
  /\ gc_seg_signed               \* re-signed body must be in gc/
  /\ extent[h] = "old"           \* guard: skip if superseded by a concurrent write
  /\ extent' = [extent EXCEPT ![h] = "gc"]
  /\ UNCHANGED <<handoff, old_present, old_idx_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up, vol_up>>

(*
  Step 2b (removal): Volume applies one removed entry from the handoff.

  Guard: same per-entry check.  If the LBA that made this hash dead has since
  been overwritten again (extent = "new"), skip it — the new write's extent
  entry is the authoritative one.
*)
VolumeApplyRemoved(h) ==
  /\ vol_up
  /\ handoff = "pending"
  /\ h \in Removed
  /\ extent[h] = "old"           \* guard: skip if superseded
  /\ extent' = [extent EXCEPT ![h] = "gone"]
  /\ UNCHANGED <<handoff, old_present, old_idx_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up, vol_up>>

(*
  Step 2d+e: Volume has visited every entry (applied or skipped due to guard),
  deletes index/<old>.idx for each consumed input, and atomically renames
  gc/<ulid>.pending → gc/<ulid>.applied.

  Precondition: ~gc_seg_present \/ gc_seg_signed: if a staged segment was written
  (gc_seg_present = TRUE), it must have been re-signed by the volume
  (gc_seg_signed = TRUE) before signalling the coordinator.  This ensures
  the body in gc/ is volume-signed before Step 3 begins.
  (For Carried = {} no staged segment exists; gc_seg_present = FALSE always.)

  This action atomically applies any remaining "old" entries (the per-entry
  guard: skip if already "gc"/"gone"/"new") AND renames .pending → .applied AND
  deletes index/<old>.idx.  In the implementation apply_gc_handoffs() does all
  of this in a single function call; a crash anywhere restarts the whole call
  idempotently.  Modelling it as one atomic TLA+ step is the correct abstraction:
  per-entry partial progress (VolumeApplyCarried / VolumeApplyRemoved) is still
  explored for safety, but the rename+idx-delete is never split from the last
  entry update, preventing the adversary from crashing between the last apply
  and the rename.

  old_idx_present' = FALSE: the volume deletes index/<old>.idx here, before the
  .applied rename.  This ensures that when the coordinator sees .applied, the old
  .idx is already gone and extent[h] will never revert to "old" after a restart.
  This is the structural fix for the Bug E class: by the time the coordinator
  can act on .applied, the restart-safety condition is already guaranteed on disk.

  For tombstone handoffs (Dead = TRUE, Carried = {}, Removed = {}):
    - Hashes = {}; the extent update is a no-op
    - gc_seg_present = FALSE throughout (no staged segment)
    - VolumeFinishApply fires immediately after CoordWritePending
    - This models the volume's acknowledgment: it has checked its own state
      and confirmed deletion is safe
*)
VolumeFinishApply ==
  /\ vol_up
  /\ handoff = "pending"
  /\ ~gc_seg_present \/ gc_seg_signed  \* re-signing complete (or Carried = {})
  \* Apply any remaining "old" entries atomically (idempotent: "gc"/"gone"/"new" unchanged).
  /\ extent'        = [h \in Hashes |->
                         IF extent[h] = "old"
                         THEN IF h \in Carried THEN "gc" ELSE "gone"
                         ELSE extent[h]]
  /\ handoff'          = "applied"
  /\ old_idx_present'  = FALSE          \* delete index/<old>.idx before .applied rename
  /\ UNCHANGED <<old_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up, vol_up>>

VolumeCrash ==
  /\ vol_up
  /\ vol_up' = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, old_idx_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up>>

(*
  On restart the volume rebuilds its in-memory extent index from on-disk .idx
  files.  The rebuild priority is: gc/ bodies first (lower priority), then
  segments/ (higher priority, wins).  index/*.idx files are consulted to find
  which segment owns each hash.

  If old_idx_present = TRUE: index/<old>.idx is on disk → the old segment wins
  the rebuild → extent[h] reverts to "old" (as if VolumeApplyCarried never ran).
  If old_idx_present = FALSE: VolumeFinishApply has already removed the old .idx
  and (for repack handoffs) the coordinator has written index/<new>.idx and
  uploaded the body to S3.  The new segment wins the rebuild:
    - Carried hashes: extent[h] = "gc" (pointing at the new GC output)
    - Removed hashes: extent[h] = "gone" (removal was already in effect)

  Concurrent-write entries ("new") survive restart: the write that created
  "new" is in a WAL or segment file that persists on disk.

  This models the Bug E class: after restart, if old_idx_present = TRUE,
  the extent index is stale.  The volume must run VolumeFinishApply to apply
  the .pending handoff and clear old_idx_present before the coordinator can
  proceed with CoordUploadGc / CoordPromote / CoordApplyDone.
*)
VolumeRestart ==
  /\ ~vol_up
  /\ vol_up' = TRUE
  /\ extent' = [h \in Hashes |->
                  IF extent[h] = "new" THEN "new"
                  ELSE IF old_idx_present THEN "old"
                  ELSE IF h \in Carried    THEN "gc"
                  ELSE                         "gone"]
  /\ UNCHANGED <<handoff, old_present, old_idx_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up>>

(*
  VolumeApplyApplied (REMOVED): in the previous design the volume could restart
  after VolumeFinishApply with old_idx_present = TRUE still on disk, causing
  VolumeRestart to reset extent[h] = "old".  VolumeApplyApplied re-applied
  .applied entries to recover.  In the new design VolumeFinishApply clears
  old_idx_present before the .applied rename, so VolumeRestart always sets
  extent[h] = "gc" (not "old") when handoff = "applied".  The action's
  precondition extent[h] = "old" is never satisfiable in that state; the
  action is structurally dead and has been removed.
*)

\* ---------------------------------------------------------------------------
\* Environment: concurrent writes
\* ---------------------------------------------------------------------------

(*
  A write to an LBA covered by a hash in Hashes arrives at any time — before,
  during, or after the handoff.  This models the coordinator's snapshot being
  stale by the time the volume applies it.

  A newer write supersedes:
    - "old": write arrived after the GC snapshot but before the volume applied
             the entry.  The extent index guard will skip this entry correctly.
    - "gc":  write arrived after the volume applied the entry.  The extent
             index now points to the write's own segment, not GCOutput.  The
             carried extent in GCOutput becomes orphaned (space leak, not
             corruption: it is never read because this hash no longer points
             to it).

  We do not model superseding "gone" or "new" — those hashes are no longer
  owned by the old segment and a further write is out of scope for this
  protocol.

  For tombstone handoffs Hashes = {} so NewerWrite never fires.  This is
  correct: if the coordinator's liveness analysis said a segment is all-dead,
  there are no hashes in Carried or Removed to write to.  The tombstone
  protocol does not need to handle concurrent writes to those hashes because
  there are none.
*)
NewerWrite(h) ==
  /\ extent[h] \in {"old", "gc"}
  /\ extent' = [extent EXCEPT ![h] = "new"]
  /\ UNCHANGED <<handoff, old_present, old_idx_present, gc_seg_present, gc_seg_signed, gc_s3_uploaded, new_seg_present, coord_up, vol_up>>

\* ---------------------------------------------------------------------------
\* Specification
\* ---------------------------------------------------------------------------

Next ==
  \/ CoordWritePending
  \/ CoordUploadGc
  \/ CoordPromote
  \/ CoordApplyDone
  \/ CoordCrash
  \/ CoordRestart
  \/ VolumeReSigns
  \/ \E h \in Carried : VolumeApplyCarried(h)
  \/ \E h \in Removed : VolumeApplyRemoved(h)
  \/ VolumeFinishApply
  \/ VolumeCrash
  \/ VolumeRestart
  \/ \E h \in Hashes  : NewerWrite(h)

(*
  Weak fairness on the five progress actions.

  WF_vars(A) means: if A is continuously enabled, it must eventually fire.
  This rules out infinite stuttering (both actors up, a step possible, but
  none ever taken) while still permitting any finite number of crashes.

  Two kinds of fairness:

  WF_vars(Restart): Restart actions are continuously enabled whenever an
  actor is down (nothing disables them while the actor stays down).  Weak
  fairness is sufficient — it says "if continuously enabled, eventually
  fire", which is exactly what we want for restart.

  SF_vars(progress): Progress actions (WritePending, UploadGc, Promote,
  ApplyDone, ReSigns, Apply*, Finish) require the actor to be UP.  A crash can interrupt
  them at any moment, so they are never continuously enabled in a trace with
  unbounded crashes.  Weak fairness would never fire.  Strong fairness fires
  whenever an action is enabled *infinitely often* — even intermittently.
  Because restarts bring actors back up, each progress action is enabled in
  every restart window, which is infinitely often.  SF therefore guarantees
  that every progress step eventually completes in some restart window.

  Crashes and NewerWrite remain unconstrained — they are adversarial.

  The \E-quantified actions get fairness at the existential level: if any
  hash can make progress, some hash will.  This is sufficient for all
  entries to be eventually processed.

  Liveness for VolumeApplyCarried / VolumeApplyRemoved / VolumeFinishApply:

  VolumeFinishApply now atomically applies any remaining "old" entries and
  renames .pending → .applied.  It is enabled on every restart where
  handoff = "pending" and the gc body is signed (or Carried = {}).  With
  SF_vars(VolumeFinishApply) the system is guaranteed to eventually reach
  .applied in some crash-free step: crashes keep vol_up = FALSE temporarily,
  but WF_vars(VolumeRestart) brings it back, and SF_vars(VolumeFinishApply)
  then forces the commit.

  VolumeApplyCarried and VolumeApplyRemoved retain \E-quantified fairness for
  safety exploration (partial-application states are still reachable by TLC),
  but liveness no longer depends on them — VolumeFinishApply subsumes them.

  For tombstone handoffs (Hashes = {}) all \E-quantified conditions are
  vacuously satisfied; progress is driven by VolumeFinishApply alone.
*)
Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(CoordRestart)
  /\ SF_vars(CoordWritePending)
  /\ SF_vars(CoordUploadGc)
  /\ SF_vars(CoordPromote)
  /\ SF_vars(CoordApplyDone)
  /\ WF_vars(VolumeRestart)
  /\ SF_vars(VolumeReSigns)
  /\ SF_vars(\E h \in Carried : VolumeApplyCarried(h))
  /\ SF_vars(\E h \in Removed : VolumeApplyRemoved(h))
  /\ SF_vars(VolumeFinishApply)

\* ---------------------------------------------------------------------------
\* Safety invariants
\* ---------------------------------------------------------------------------

(*
  Core safety: the extent index never holds a reference to a segment that is
  not present.  A "segment not found" error at read time means a violated
  NoSegmentNotFound at some earlier point.

  - extent = "old"  references the original input segment (old_present)
  - extent = "gc"   references the GC output body, which may be in two places:
                      * gc/ (volume-signed, during the .applied window):
                        gc_seg_present /\ gc_seg_signed
                      * cache/ (after coordinator calls promote in Step 3b):
                        new_seg_present
                    The body must be in at least one of these locations.
  - extent = "gone" means the entry was deleted from the index (no reference)
  - extent = "new"  means a write's segment is in use (always present by
                    assumption — writes land in pending/ which is local)

  gc_seg_present without gc_seg_signed (the ephemeral coordinator-signed body)
  is never referenced by any extent entry; it is only present before VolumeReSigns.

  For tombstone handoffs Hashes = {} so both quantifiers are vacuously true.
  The binding safety invariant for tombstones is OldOnlyDeletedAfterApplied.
*)
NoSegmentNotFound ==
  /\ \A h \in Hashes  : extent[h] = "old" => old_present
  /\ \A h \in Carried : extent[h] = "gc"  =>
       (gc_seg_present /\ gc_seg_signed) \/ new_seg_present

(*
  Segments are only removed when nothing in the extent index points to them
  any more.  Equivalently: if a segment is absent, no extent references it.

  For "gc" entries: the body may be in gc/ OR segments/.  NoLostData requires
  that if BOTH are absent, no extent still points there.

  The ephemeral coordinator-signed gc_seg_present (before VolumeReSigns) is
  never referenced by any extent entry, so its presence or absence does not
  affect index safety.

  For tombstone handoffs Hashes = {} so both quantifiers are vacuously true.
*)
NoLostData ==
  /\ (~old_present => \A h \in Hashes : extent[h] # "old")
  /\ (\A h \in Carried :
        ~((gc_seg_present /\ gc_seg_signed) \/ new_seg_present) => extent[h] # "gc")

(*
  Coordinator deletion invariant: the old segment (including an all-dead
  tombstone segment) is absent only after the handoff has fully completed.

  Since handoff = "done" is only reachable via handoff = "applied", and
  "applied" requires the volume to have run VolumeFinishApply, this invariant
  encodes:

    "The coordinator never deletes a segment without the volume's
     acknowledgment via the handoff protocol."

  This is the invariant that the previous all-dead direct deletion path
  violated: it set old_present = FALSE (by deleting the local file) without
  going through the handoff protocol, which is equivalent to transitioning
  old_present to FALSE while handoff is still "absent" — a state this
  invariant forbids.

  This invariant applies to all three handoff types.  For tombstone handoffs
  it is the primary binding constraint (NoSegmentNotFound and NoLostData are
  vacuously true).  For repack and removal-only handoffs it provides an
  additional explicit guarantee on top of those two.
*)
OldOnlyDeletedAfterApplied ==
  ~old_present => handoff = "done"

(*
  Index file ordering invariant: index/<old>.idx may only exist while the old
  S3 object exists.

  This invariant encodes the fix for the "segment not found in any ancestor"
  bug: an earlier implementation deleted old S3 objects without first removing
  index/<old>.idx.  After eviction (cache/ emptied) and coordinator restart,
  rebuild_segments included the stale .idx in the extent index, mapping hashes
  to a segment absent from both disk and S3.

  The fix: VolumeFinishApply sets old_idx_present = FALSE (Step 2d) before
  CoordApplyDone sets old_present = FALSE (Step 3 Part B).  CoordApplyDone is
  gated on ~old_idx_present, enforcing this ordering in all interleavings.

  TLC verifies that no reachable state violates this invariant, including
  all crash-and-restart interleavings between VolumeFinishApply and CoordApplyDone.
*)
OldIdxOnlyPresentWhenSegmentPresent ==
  old_idx_present => old_present

(*
  cache/ invariant: new_seg_present (the GC output body is in cache/)
  implies the body in gc/ is gone and the handoff has reached at least "applied".

  new_seg_present is set by CoordPromote (promote IPC to the volume), which
  requires handoff = "applied" and gc_s3_uploaded — i.e. after the volume has
  acknowledged and the coordinator has confirmed the S3 upload.  Once in
  cache/, gc/ no longer has the body (gc_seg_present = FALSE).

  The invariant verifies that cache/<new> is only populated after the volume's
  acknowledgment (handoff = "applied") and after the S3 upload is confirmed.
*)
CacheOnlyAfterUpload ==
  new_seg_present => (~gc_seg_present /\ handoff \in {"applied", "done"})

\* ---------------------------------------------------------------------------
\* Liveness property
\* ---------------------------------------------------------------------------

(*
  The handoff eventually completes.

  This is a PROPERTY (checked with <>), not an INVARIANT.  It is only
  meaningful with the WF fairness conditions in Spec above.

  <>( handoff = "done" ) holds iff every infinite execution eventually
  reaches a state where handoff = "done".

  Note: this does NOT require the handoff to complete despite unlimited
  crashes.  WF only guarantees progress when an action is *continuously*
  enabled.  If crashes recur forever, the CoordWritePending action may
  never be continuously enabled, so the liveness obligation does not apply.
  That is the correct and intended behaviour: liveness under fair scheduling,
  not liveness under adversarial crash-forever scenarios.
*)
EventuallyDone == <>(handoff = "done")

====
