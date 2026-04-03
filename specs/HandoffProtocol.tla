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
           The body stays in gc/ — it is NOT moved to segments/ here.
           Skipped for removal-only and tombstone handoffs (no staged segment).
        b. Applies extent index patches (repack → "gc", remove → "gone"),
           with a per-entry guard: skip if a concurrent write has already
           superseded the hash.
        c. For tombstone entries ("dead <old_ulid>"): verifies from the volume's
           own perspective that no LBA map or extent index entry references the
           named segment.  This is a no-op in the normal case; its purpose is
           to provide the volume's acknowledgment before deletion.
        d. Renames gc/<ulid>.pending → gc/<ulid>.applied.

      Step 3 — Coordinator cleans up:
        a. Uploads gc/<ulid> (now volume-signed) to S3.
           Skipped for removal-only and tombstone handoffs.
        b. Writes index/<ulid>.idx (S3-confirmation marker).
           Skipped for removal-only and tombstone handoffs.
        c. Moves gc/<ulid> → segments/<ulid>.  This is the point at which
           segments/ reflects that the segment is S3-confirmed.
           Skipped for removal-only and tombstone handoffs.
        d. Deletes old S3 objects and old local segment files.
        e. Renames gc/<ulid>.applied → gc/<ulid>.done.

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

  WHAT WE CHECK
  -------------
  TLC exhaustively explores every reachable state — including all crash/restart
  interleavings and concurrent writes — and verifies that the three safety
  invariants hold in all of them:

    NoSegmentNotFound         — the extent index never references a missing segment
    NoLostData                — segments are only removed when no extent points to them
    OldOnlyDeletedAfterApplied — the old segment is absent only after handoff = "done"

  The third invariant directly encodes the coordinator deletion invariant: no
  deletion without the volume's acknowledgment.  The first two cover correctness
  of extent index state.  All three apply to all three handoff types; for
  tombstone handoffs (Carried = {}, Removed = {}) the first two are vacuously
  true (Hashes = {}) and the third is the binding constraint.

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
  handoff,       \* state of the gc/<ulid>.{pending,applied,done} marker file
                 \*   "absent"  — not yet written
                 \*   "pending" — coordinator wrote it; volume must apply
                 \*   "applied" — volume applied it; coordinator must clean up
                 \*   "done"    — coordinator cleaned up; handoff complete

  extent,        \* map: hash → what the extent index currently says
                 \*   "old"  — still points to the original old segment (pre-GC)
                 \*   "gc"   — updated to point to GCOutput   (Carried hashes only)
                 \*   "gone" — entry removed from extent index (Removed hashes only)
                 \*   "new"  — a concurrent write superseded this entry
                 \* For tombstone handoffs Hashes = {} so this map is empty.

  old_present,    \* TRUE iff the old (input) segment file is still present.
                  \* For tombstone handoffs this is the all-dead segment being deleted.
  gc_seg_present, \* TRUE iff gc/<ulid> body is present (coordinator- or volume-signed).
                  \* Set FALSE only when the coordinator moves it to segments/ in Step 3c.
  gc_seg_signed,  \* TRUE iff the gc/<ulid> body has been re-signed by the volume.
                  \* FALSE until VolumeReSigns fires; stays TRUE until gc_seg_present = FALSE.
                  \* Extent index entries pointing at "gc" are only readable once this is TRUE.
  new_seg_present, \* TRUE iff segments/<ulid> (volume-signed, S3-confirmed) is present.
                   \* Set only by CoordApplyDone (the gc/ → segments/ move).
  coord_up,      \* TRUE iff the coordinator is currently running
  vol_up         \* TRUE iff the volume is currently running

vars == <<handoff, extent, old_present, gc_seg_present, gc_seg_signed, new_seg_present, coord_up, vol_up>>

\* ---------------------------------------------------------------------------
\* Type correctness (checked as an invariant, useful for debugging the model)
\* ---------------------------------------------------------------------------

Hashes == Carried \cup Removed

TypeOK ==
  /\ handoff          \in {"absent", "pending", "applied", "done"}
  /\ extent           \in [Hashes -> {"old", "gc", "gone", "new"}]
  /\ old_present      \in BOOLEAN
  /\ gc_seg_present   \in BOOLEAN
  /\ gc_seg_signed    \in BOOLEAN
  /\ new_seg_present  \in BOOLEAN
  /\ coord_up         \in BOOLEAN
  /\ vol_up           \in BOOLEAN

\* ---------------------------------------------------------------------------
\* Initial state
\* ---------------------------------------------------------------------------

Init ==
  /\ handoff          = "absent"
  /\ extent           = [h \in Hashes |-> "old"]   \* all entries pre-GC
  /\ old_present      = TRUE                        \* old segment exists
  /\ gc_seg_present   = FALSE                       \* staged segment not yet written
  /\ gc_seg_signed    = FALSE                       \* not yet re-signed by volume
  /\ new_seg_present  = FALSE                       \* GC output not yet in segments/
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
  /\ UNCHANGED <<extent, old_present, gc_seg_signed, new_seg_present, coord_up, vol_up>>

(*
  Step 3: Coordinator sees .applied.  Uploads gc/<ulid> (now volume-signed) to
  S3 if a new segment was produced, writes index/<ulid>.idx, moves gc/<ulid>
  → segments/<ulid>, deletes old S3 objects and old local segment files, then
  renames .applied → .done.  Modelled as one atomic step because each S3 DELETE
  is idempotent (404 = success), so a crash mid-cleanup is safe: on restart the
  coordinator retries from .applied and the net effect is the same.

  By the time handoff = "applied" the volume has processed every entry
  (VolumeFinishApply's precondition guarantees it), so no extent points to
  "old" any more.  It is safe to remove the old segment.

  For tombstone handoffs the volume's apply was a no-op (no extent patches),
  but the .applied marker still serves as the acknowledgment that the volume
  has confirmed no live references exist from its perspective.

  S3 upload state is not modelled (no S3 variable); the upload happens before
  the move and is safe to retry.

  The gc/ → segments/ move (step 3c) is what sets new_seg_present.  Until this
  step, extents in the "gc" state reference a body still in gc/ (accessible via
  the read-path gc/ fallback when segments/<ulid> is absent).  After this step,
  the body is in segments/ and the normal read path applies.
*)
CoordApplyDone ==
  /\ coord_up
  /\ handoff = "applied"
  /\ handoff'          = "done"
  /\ old_present'      = FALSE
  /\ gc_seg_present'   = FALSE          \* gc/ body moved to segments/
  /\ gc_seg_signed'    = FALSE          \* reset: body no longer in gc/
  /\ new_seg_present'  = (Carried # {}) \* segments/<ulid> written iff a body was produced
  /\ UNCHANGED <<extent, coord_up, vol_up>>

CoordCrash ==
  /\ coord_up
  /\ coord_up' = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, gc_seg_signed, new_seg_present, vol_up>>

\* On restart the coordinator re-reads gc/ and resumes at whatever state the
\* file is in.  No state reset is needed because the file state is persistent.
CoordRestart ==
  /\ ~coord_up
  /\ coord_up' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, gc_seg_signed, new_seg_present, vol_up>>

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
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, new_seg_present, coord_up, vol_up>>

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
  /\ UNCHANGED <<handoff, old_present, gc_seg_present, gc_seg_signed, new_seg_present, coord_up, vol_up>>

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
  /\ UNCHANGED <<handoff, old_present, gc_seg_present, gc_seg_signed, new_seg_present, coord_up, vol_up>>

(*
  Step 2d: Volume has visited every entry (applied or skipped due to guard) and
  atomically renames gc/<ulid>.pending → gc/<ulid>.applied.

  Preconditions:
    - Every carried entry is now "gc" (applied) or "new" (guard fired, skipped)
    - Every removed entry is now "gone" (applied) or "new" (guard fired, skipped)
    - ~gc_seg_present \/ gc_seg_signed: if a staged segment was written
      (gc_seg_present = TRUE), it must have been re-signed by the volume
      (gc_seg_signed = TRUE) before signalling the coordinator.  This ensures
      the body in gc/ is volume-signed before Step 3 begins.
      (For Carried = {} no staged segment exists; gc_seg_present = FALSE always.)

  For tombstone handoffs (Dead = TRUE, Carried = {}, Removed = {}):
    - Both universals are vacuously true (Hashes = {})
    - gc_seg_present = FALSE throughout (no staged segment)
    - VolumeFinishApply fires immediately after CoordWritePending
    - This models the volume's acknowledgment: it has checked its own state
      and confirmed deletion is safe

  If the volume crashes before this rename, the .pending file is still there
  on restart: the volume re-applies from scratch.  Per-entry application is
  idempotent because the guard (extent[h] = "old") does not fire for entries
  already set to "gc" or "gone" in a previous partial run.
*)
VolumeFinishApply ==
  /\ vol_up
  /\ handoff = "pending"
  /\ ~gc_seg_present \/ gc_seg_signed  \* re-signing complete (or Carried = {})
  /\ \A h \in Carried : extent[h] \in {"gc",   "new"}
  /\ \A h \in Removed : extent[h] \in {"gone", "new"}
  /\ handoff' = "applied"
  /\ UNCHANGED <<extent, old_present, gc_seg_present, gc_seg_signed, new_seg_present, coord_up, vol_up>>

VolumeCrash ==
  /\ vol_up
  /\ vol_up' = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, gc_seg_signed, new_seg_present, coord_up>>

VolumeRestart ==
  /\ ~vol_up
  /\ vol_up' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, gc_seg_signed, new_seg_present, coord_up>>

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
  /\ UNCHANGED <<handoff, old_present, gc_seg_present, gc_seg_signed, new_seg_present, coord_up, vol_up>>

\* ---------------------------------------------------------------------------
\* Specification
\* ---------------------------------------------------------------------------

Next ==
  \/ CoordWritePending
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

  SF_vars(progress): Progress actions (WritePending, ReSigns, ApplyDone,
  Apply*, Finish) require the actor to be UP.  A crash can interrupt them at
  any moment, so they are never continuously enabled in a trace with unbounded
  crashes.  Weak fairness would never fire.  Strong fairness fires whenever
  an action is enabled *infinitely often* — even intermittently.  Because
  restarts bring actors back up, each progress action is enabled in every
  restart window, which is infinitely often.  SF therefore guarantees that
  every progress step eventually completes in some restart window.

  Crashes and NewerWrite remain unconstrained — they are adversarial.

  The \E-quantified actions get fairness at the existential level: if any
  hash can make progress, some hash will.  This is sufficient for all
  entries to be eventually processed.

  For tombstone handoffs (Hashes = {}) the \E-quantified fairness conditions
  are vacuously satisfied; progress is driven by VolumeFinishApply alone.
*)
Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(CoordRestart)
  /\ SF_vars(CoordWritePending)
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
                      * segments/ (after coordinator moves it in Step 3c):
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
  segments/ invariant: new_seg_present (the GC output body is in segments/)
  implies the body in gc/ is gone and the handoff has reached "done".

  This encodes the structural enforcement of "segments/ present ↔ S3-confirmed":
  new_seg_present is only set TRUE by CoordApplyDone, which fires only when
  handoff = "applied" — i.e. after the volume has acknowledged and the
  coordinator has completed upload.  Once in segments/, gc/ no longer has the
  body (gc_seg_present = FALSE).

  This invariant is trivially true in the old design (new_seg_present was set
  by VolumeReSigns before upload).  Here it verifies that the new design
  correctly ties segments/ presence to coordinator completion.
*)
SegmentsOnlyAfterUpload ==
  new_seg_present => (~gc_seg_present /\ handoff = "done")

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
