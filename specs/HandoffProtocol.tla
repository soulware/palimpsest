---- MODULE HandoffProtocol ----
(*
  TLA+ model of the Elide GC handoff protocol.

  BACKGROUND
  ----------
  The coordinator compacts segments and hands the result to the volume via a
  three-step file sequence:

      Step 1 — Coordinator stages the compacted segment and signals the volume:
        gc/<ulid>          (segment, signed with an ephemeral key)
        gc/<ulid>.pending  (text file: carried + removed entries; written last
                            via tmp-rename, so .pending is never visible without
                            the segment already being present)

      Step 2 — Volume re-signs and applies:
        a. Re-signs gc/<ulid> with its own key, writes segments/<ulid>, deletes
           gc/<ulid>.
        b. Applies extent index patches (carried → "gc", removed → "gone"),
           with a per-entry guard: skip if a concurrent write has already
           superseded the hash.
        c. Renames gc/<ulid>.pending → gc/<ulid>.applied.

      Step 3 — Coordinator cleans up:
        a. Uploads segments/<ulid> (the volume-signed version) to S3.
        b. Deletes old S3 objects and old local segment files.
        c. Renames gc/<ulid>.applied → gc/<ulid>.done.

  Two independent actors drive the transitions:
    - Coordinator: Steps 1 and 3
    - Volume:      Step 2

  A handoff file contains two kinds of entries (by hash):
    - Carried (4-field): extent moved from old segment to GCOutput
    - Removed (2-field): extent index entry deleted (LBA-dead; no output segment)

  WHY TWO SEGMENT FILES?
  ----------------------
  The coordinator does not hold the volume's private signing key.  It stages
  the compacted segment in gc/ using an ephemeral key.  The volume re-signs
  it with its own key before moving it into segments/, so that segments/ always
  contains only volume-signed data.  extentindex::rebuild therefore never needs
  to special-case in-transit coordinator output.

  WHAT WE CHECK
  -------------
  TLC exhaustively explores every reachable state — including all crash/restart
  interleavings and concurrent writes — and verifies that the two safety
  invariants hold in all of them:

    NoSegmentNotFound  — the extent index never references a missing segment
    NoLostData         — segments are only removed when no extent points to them

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
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Carried,   \* set of hashes whose extents are moved to GCOutput (4-field entries)
  Removed    \* set of hashes whose extent entries are deleted   (2-field entries)

\* The two sets must be disjoint; at least one must be non-empty.
ASSUME Carried \cap Removed = {}
ASSUME Carried \cup Removed # {}

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

  old_present,   \* TRUE iff the old (input) segment file is still present
  gc_seg_present, \* TRUE iff gc/<ulid> (ephemeral-signed staged segment) is present
  new_seg_present, \* TRUE iff segments/<ulid> (volume-signed GC output) is present
  coord_up,      \* TRUE iff the coordinator is currently running
  vol_up         \* TRUE iff the volume is currently running

vars == <<handoff, extent, old_present, gc_seg_present, new_seg_present, coord_up, vol_up>>

\* ---------------------------------------------------------------------------
\* Type correctness (checked as an invariant, useful for debugging the model)
\* ---------------------------------------------------------------------------

Hashes == Carried \cup Removed

TypeOK ==
  /\ handoff         \in {"absent", "pending", "applied", "done"}
  /\ extent          \in [Hashes -> {"old", "gc", "gone", "new"}]
  /\ old_present     \in BOOLEAN
  /\ gc_seg_present  \in BOOLEAN
  /\ new_seg_present \in BOOLEAN
  /\ coord_up        \in BOOLEAN
  /\ vol_up          \in BOOLEAN

\* ---------------------------------------------------------------------------
\* Initial state
\* ---------------------------------------------------------------------------

Init ==
  /\ handoff         = "absent"
  /\ extent          = [h \in Hashes |-> "old"]   \* all entries pre-GC
  /\ old_present     = TRUE                         \* old segment exists
  /\ gc_seg_present  = FALSE                        \* staged segment not yet written
  /\ new_seg_present = FALSE                        \* GC output not yet in segments/
  /\ coord_up        = TRUE
  /\ vol_up          = TRUE

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
  stays FALSE and the volume applies only the 2-field removal entries.
*)
CoordWritePending ==
  /\ coord_up
  /\ handoff = "absent"
  /\ handoff'         = "pending"
  /\ gc_seg_present'  = (Carried # {})   \* staged segment written iff there are carried entries
  /\ UNCHANGED <<extent, old_present, new_seg_present, coord_up, vol_up>>

(*
  Step 3: Coordinator sees .applied.  Uploads segments/<ulid> (the volume-signed
  version) to S3, deletes old S3 objects and old local segment files, then
  renames .applied → .done.  Modelled as one atomic step because each S3 DELETE
  is idempotent (404 = success), so a crash mid-cleanup is safe: on restart the
  coordinator retries from .applied and the net effect is the same.

  By the time handoff = "applied" the volume has processed every entry
  (VolumeFinishApply's precondition guarantees it), so no extent points to
  "old" any more.  It is safe to remove the old segment.

  S3 upload state is not modelled (no S3 variable); the upload happens before
  deletion and is safe to retry.  new_seg_present stays TRUE — extents in the
  "gc" state still point to segments/<ulid>, which persists locally beyond the
  scope of this protocol.
*)
CoordApplyDone ==
  /\ coord_up
  /\ handoff = "applied"
  /\ handoff'     = "done"
  /\ old_present' = FALSE
  /\ UNCHANGED <<extent, gc_seg_present, new_seg_present, coord_up, vol_up>>

CoordCrash ==
  /\ coord_up
  /\ coord_up' = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, new_seg_present, vol_up>>

\* On restart the coordinator re-reads gc/ and resumes at whatever state the
\* file is in.  No state reset is needed because the file state is persistent.
CoordRestart ==
  /\ ~coord_up
  /\ coord_up' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, new_seg_present, vol_up>>

\* ---------------------------------------------------------------------------
\* Volume actions
\* ---------------------------------------------------------------------------

(*
  Step 2a: Volume re-signs the staged segment.

  The volume reads gc/<ulid> (ephemeral-signed), writes a volume-signed copy to
  segments/<ulid>, then deletes gc/<ulid>.  Modelled as one atomic step.

  This must happen before any carried entry can be applied: VolumeApplyCarried
  requires new_seg_present (the volume-signed output) rather than gc_seg_present
  (the ephemeral coordinator output).

  Idempotency: if the volume crashes after writing segments/<ulid> but before
  deleting gc/<ulid>, both files are present on restart.  The next call
  re-signs, overwriting segments/<ulid> with identical content (idempotent),
  and deletes gc/<ulid>.  In the model this is captured by allowing VolumeReSigns
  to fire again only if gc_seg_present is still TRUE.  After a crash-and-restart
  where segments/<ulid> already exists, the volume will find gc/<ulid> still
  present (gc_seg_present = TRUE) and re-sign idempotently, then proceed.
*)
VolumeReSigns ==
  /\ vol_up
  /\ handoff = "pending"
  /\ gc_seg_present              \* staged segment must be present
  /\ gc_seg_present'  = FALSE    \* gc/<ulid> deleted after re-sign
  /\ new_seg_present' = TRUE     \* segments/<ulid> written
  /\ UNCHANGED <<handoff, extent, old_present, coord_up, vol_up>>

(*
  Step 2b: Volume applies one carried entry from the handoff.

  Guard: only update the extent index entry if it still points to "old"
  (i.e. the old segment).  If a concurrent write has already superseded this
  hash (extent = "new"), skip it — the newer write wins.

  This guard is the central correctness mechanism.  Without it a stale GC
  output could overwrite a newer write's extent index entry.

  Precondition new_seg_present: the volume needs the re-signed segment in
  segments/ before it can apply carried entries.  Removal-only entries
  (Removed) do not need this.
*)
VolumeApplyCarried(h) ==
  /\ vol_up
  /\ handoff = "pending"
  /\ h \in Carried
  /\ new_seg_present             \* re-signed segment must be in segments/
  /\ extent[h] = "old"           \* guard: skip if superseded by a concurrent write
  /\ extent' = [extent EXCEPT ![h] = "gc"]
  /\ UNCHANGED <<handoff, old_present, gc_seg_present, new_seg_present, coord_up, vol_up>>

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
  /\ UNCHANGED <<handoff, old_present, gc_seg_present, new_seg_present, coord_up, vol_up>>

(*
  Step 2c: Volume has visited every entry (applied or skipped due to guard) and
  atomically renames gc/<ulid>.pending → gc/<ulid>.applied.

  Preconditions:
    - Every carried entry is now "gc" (applied) or "new" (guard fired, skipped)
    - Every removed entry is now "gone" (applied) or "new" (guard fired, skipped)
    - gc_seg_present = FALSE: the staged segment has been consumed by VolumeReSigns
      (or Carried = {}, so no staged segment was written).  This ensures
      segments/<ulid> is always the canonical location before signalling the
      coordinator to clean up old files.

  If the volume crashes before this rename, the .pending file is still there
  on restart: the volume re-applies from scratch.  Per-entry application is
  idempotent because the guard (extent[h] = "old") does not fire for entries
  already set to "gc" or "gone" in a previous partial run.
*)
VolumeFinishApply ==
  /\ vol_up
  /\ handoff = "pending"
  /\ ~gc_seg_present             \* re-signing complete (or Carried = {} → always FALSE)
  /\ \A h \in Carried : extent[h] \in {"gc",   "new"}
  /\ \A h \in Removed : extent[h] \in {"gone", "new"}
  /\ handoff' = "applied"
  /\ UNCHANGED <<extent, old_present, gc_seg_present, new_seg_present, coord_up, vol_up>>

VolumeCrash ==
  /\ vol_up
  /\ vol_up' = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, new_seg_present, coord_up>>

VolumeRestart ==
  /\ ~vol_up
  /\ vol_up' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, gc_seg_present, new_seg_present, coord_up>>

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
*)
NewerWrite(h) ==
  /\ extent[h] \in {"old", "gc"}
  /\ extent' = [extent EXCEPT ![h] = "new"]
  /\ UNCHANGED <<handoff, old_present, gc_seg_present, new_seg_present, coord_up, vol_up>>

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
  - extent = "gc"   references the volume-signed GC output (new_seg_present)
  - extent = "gone" means the entry was deleted from the index (no reference)
  - extent = "new"  means a write's segment is in use (always present by
                    assumption — writes land in pending/ which is local)

  gc_seg_present (the ephemeral staged segment in gc/) is never referenced by
  any extent entry; it is only a staging area for the re-signing step.
*)
NoSegmentNotFound ==
  /\ \A h \in Hashes  : extent[h] = "old"  => old_present
  /\ \A h \in Carried : extent[h] = "gc"   => new_seg_present

(*
  Segments are only removed when nothing in the extent index points to them
  any more.  Equivalently: if a segment is absent, no extent references it.

  The ephemeral gc_seg_present segment is excluded: it is never referenced by
  any extent entry, so its presence or absence does not affect index safety.
*)
NoLostData ==
  /\ (~old_present     => \A h \in Hashes  : extent[h] # "old")
  /\ (~new_seg_present => \A h \in Carried : extent[h] # "gc")

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
