---- MODULE HandoffProtocol ----
(*
  TLA+ model of the Elide GC handoff protocol.

  BACKGROUND
  ----------
  The coordinator compacts segments and hands the result to the volume via a
  three-state file in gc/:

      absent  →  <ulid>.pending  →  <ulid>.applied  →  <ulid>.done

  Two independent actors drive the transitions:
    - Coordinator: writes .pending, later cleans up .applied → .done
    - Volume:      detects .pending, patches its extent index, renames → .applied

  A handoff file contains two kinds of entries (by hash):
    - Carried (4-field): extent moved from old segment to GCOutput
    - Removed (2-field): extent index entry deleted (LBA-dead; no output segment)

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
  handoff,     \* state of the gc/<ulid> file
               \*   "absent"  — not yet written
               \*   "pending" — coordinator wrote it; volume must apply
               \*   "applied" — volume applied it; coordinator must clean up
               \*   "done"    — coordinator cleaned up; handoff complete

  extent,      \* map: hash → what the extent index currently says
               \*   "old"  — still points to the original old segment (pre-GC)
               \*   "gc"   — updated to point to GCOutput   (Carried hashes only)
               \*   "gone" — entry removed from extent index (Removed hashes only)
               \*   "new"  — a concurrent write superseded this entry

  old_present, \* TRUE iff the old (input) segment file is still present
  gc_present,  \* TRUE iff the GCOutput segment file is present
  coord_up,    \* TRUE iff the coordinator is currently running
  vol_up       \* TRUE iff the volume is currently running

vars == <<handoff, extent, old_present, gc_present, coord_up, vol_up>>

\* ---------------------------------------------------------------------------
\* Type correctness (checked as an invariant, useful for debugging the model)
\* ---------------------------------------------------------------------------

Hashes == Carried \cup Removed

TypeOK ==
  /\ handoff     \in {"absent", "pending", "applied", "done"}
  /\ extent      \in [Hashes -> {"old", "gc", "gone", "new"}]
  /\ old_present \in BOOLEAN
  /\ gc_present  \in BOOLEAN
  /\ coord_up    \in BOOLEAN
  /\ vol_up      \in BOOLEAN

\* ---------------------------------------------------------------------------
\* Initial state
\* ---------------------------------------------------------------------------

Init ==
  /\ handoff     = "absent"
  /\ extent      = [h \in Hashes |-> "old"]   \* all entries pre-GC
  /\ old_present = TRUE                         \* old segment exists
  /\ gc_present  = FALSE                        \* GCOutput not yet written
  /\ coord_up    = TRUE
  /\ vol_up      = TRUE

\* ---------------------------------------------------------------------------
\* Coordinator actions
\* ---------------------------------------------------------------------------

(*
  Coordinator materialises GCOutput (if any carried entries) and atomically
  writes gc/<ulid>.pending via a tmp-file rename.  In the model this is a
  single atomic step; in the implementation the GCOutput segment is written
  first (so it is present before the handoff file is visible).

  Removal-only handoffs (Carried = {}) produce no GCOutput; gc_present stays
  FALSE and the volume applies only the 2-field removal entries.
*)
CoordWritePending ==
  /\ coord_up
  /\ handoff = "absent"
  /\ handoff'     = "pending"
  /\ gc_present'  = (Carried # {})   \* GCOutput written iff there are carried entries
  /\ UNCHANGED <<extent, old_present, coord_up, vol_up>>

(*
  Coordinator sees .applied, deletes the old input segment(s) from S3/local,
  and renames .applied → .done.  Modelled as one atomic step because each
  S3 DELETE is idempotent (404 = success), so a crash mid-cleanup is safe:
  on restart the coordinator retries from .applied and the net effect is the
  same.
*)
CoordApplyDone ==
  /\ coord_up
  /\ handoff = "applied"
  \* By the time handoff = "applied" the volume has processed every entry
  \* (VolumeFinishApply's precondition guarantees it), so no extent points
  \* to "old" any more.  It is safe to remove the old segment.
  /\ handoff'     = "done"
  /\ old_present' = FALSE
  /\ UNCHANGED <<extent, gc_present, coord_up, vol_up>>

CoordCrash ==
  /\ coord_up
  /\ coord_up' = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, gc_present, vol_up>>

\* On restart the coordinator re-reads gc/ and resumes at whatever state the
\* file is in.  No state reset is needed because the file state is persistent.
CoordRestart ==
  /\ ~coord_up
  /\ coord_up' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, gc_present, vol_up>>

\* ---------------------------------------------------------------------------
\* Volume actions
\* ---------------------------------------------------------------------------

(*
  Volume applies one carried entry from the handoff.

  Guard: only update the extent index entry if it still points to "old"
  (i.e. the old segment).  If a concurrent write has already superseded this
  hash (extent = "new"), skip it — the newer write wins.

  This guard is the central correctness mechanism.  Without it a stale GC
  output could overwrite a newer write's extent index entry.

  Precondition gc_present: the volume needs the new segment file accessible
  (locally or via demand-fetch from S3) before it can apply carried entries.
  Removal-only entries (Removed) do not need GCOutput.
*)
VolumeApplyCarried(h) ==
  /\ vol_up
  /\ handoff = "pending"
  /\ h \in Carried
  /\ gc_present             \* new segment must be reachable
  /\ extent[h] = "old"      \* guard: skip if superseded by a concurrent write
  /\ extent' = [extent EXCEPT ![h] = "gc"]
  /\ UNCHANGED <<handoff, old_present, gc_present, coord_up, vol_up>>

(*
  Volume applies one removed entry from the handoff.

  Guard: same per-entry check.  If the LBA that made this hash dead has since
  been overwritten again (extent = "new"), skip it — the new write's extent
  entry is the authoritative one.
*)
VolumeApplyRemoved(h) ==
  /\ vol_up
  /\ handoff = "pending"
  /\ h \in Removed
  /\ extent[h] = "old"      \* guard: skip if superseded
  /\ extent' = [extent EXCEPT ![h] = "gone"]
  /\ UNCHANGED <<handoff, old_present, gc_present, coord_up, vol_up>>

(*
  Volume has visited every entry (applied or skipped due to guard) and
  atomically renames gc/<ulid>.pending → gc/<ulid>.applied.

  Preconditions:
    - Every carried entry is now "gc" (applied) or "new" (guard fired, skipped)
    - Every removed entry is now "gone" (applied) or "new" (guard fired, skipped)

  If the volume crashes before this rename, the .pending file is still there
  on restart: the volume re-applies from scratch.  Per-entry application is
  idempotent because the guard (extent[h] = "old") does not fire for entries
  already set to "gc" or "gone" in a previous partial run.
*)
VolumeFinishApply ==
  /\ vol_up
  /\ handoff = "pending"
  /\ \A h \in Carried : extent[h] \in {"gc",   "new"}
  /\ \A h \in Removed : extent[h] \in {"gone", "new"}
  /\ handoff' = "applied"
  /\ UNCHANGED <<extent, old_present, gc_present, coord_up, vol_up>>

VolumeCrash ==
  /\ vol_up
  /\ vol_up' = FALSE
  /\ UNCHANGED <<handoff, extent, old_present, gc_present, coord_up>>

VolumeRestart ==
  /\ ~vol_up
  /\ vol_up' = TRUE
  /\ UNCHANGED <<handoff, extent, old_present, gc_present, coord_up>>

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
  /\ UNCHANGED <<handoff, old_present, gc_present, coord_up, vol_up>>

\* ---------------------------------------------------------------------------
\* Specification
\* ---------------------------------------------------------------------------

Next ==
  \/ CoordWritePending
  \/ CoordApplyDone
  \/ CoordCrash
  \/ CoordRestart
  \/ \E h \in Carried : VolumeApplyCarried(h)
  \/ \E h \in Removed : VolumeApplyRemoved(h)
  \/ VolumeFinishApply
  \/ VolumeCrash
  \/ VolumeRestart
  \/ \E h \in Hashes  : NewerWrite(h)

Spec == Init /\ [][Next]_vars

\* ---------------------------------------------------------------------------
\* Safety invariants
\* ---------------------------------------------------------------------------

(*
  Core safety: the extent index never holds a reference to a segment that is
  not present.  A "segment not found" error at read time means a violated
  NoSegmentNotFound at some earlier point.

  "gone" and "new" are safe: "gone" means the entry was deleted from the
  index (no reference); "new" means a write's segment is in use (always
  present by assumption — writes land in pending/ which is local).
*)
NoSegmentNotFound ==
  /\ \A h \in Hashes  : extent[h] = "old"  => old_present
  /\ \A h \in Carried : extent[h] = "gc"   => gc_present

(*
  Segments are only removed when nothing in the extent index points to them
  any more.  Equivalently: if a segment is absent, no extent references it.
*)
NoLostData ==
  /\ (~old_present => \A h \in Hashes  : extent[h] # "old")
  /\ (~gc_present  => \A h \in Carried : extent[h] # "gc")

====
