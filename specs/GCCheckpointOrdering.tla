---- MODULE GCCheckpointOrdering ----
(*
  TLA+ model of the ULID ordering invariant established by gc_checkpoint's
  three-ULID pre-mint pattern.

  THE PROBLEM
  -----------
  After a crash, the volume rebuilds its LBA map by replaying all segment
  files in ascending ULID order (highest ULID = most recent = wins for each
  LBA).  For this to produce the correct result, any WAL segment flushed by
  gc_checkpoint must have a ULID strictly greater than the GC repack output
  ULID.

  THE BUG (pre-fix behaviour)
  ---------------------------
  Before the fix, gc_checkpoint:
    1. Minted GC output ULIDs  (u_repack, u_sweep)
    2. Flushed the WAL under its *original* open ULID — the ULID assigned
       when the WAL was first created, before GC ran, always below u_repack.

  After a crash, rebuild replayed:
    [wal_open_ulid]  "new" data  (WAL segment: lower ULID, applied first)
    [u_repack]       "old" data  (GC output:   higher ULID, applied last)

  The GC output won, shadowing newer writes with stale content.

  THE FIX (three-ULID pre-mint)
  -----------------------------
  gc_checkpoint now pre-mints three ULIDs atomically before any I/O:

    u_repack = mint        (GC repack output)
    u_sweep  = mint + 1   (GC sweep pass)
    u_flush    = mint + 2   (WAL flush at checkpoint time)

  The WAL is flushed to disk under u_flush, NOT under its original open ULID.
  No new WAL is eagerly opened: the post-checkpoint WAL is lazy — the next
  write calls mint.next() and opens a WAL at that fresh ULID. Because mint
  is strictly monotonic, that ULID is always > u_flush (> u_sweep > u_repack),
  so the new-WAL-above-GC-output invariant holds without pre-reservation.

  Ordering enforced:

    wal_open_ulid  <  u_repack  <  u_sweep  <  u_flush  <  next_write_wal_ulid

  After a crash, rebuild replays:
    [u_repack]  "old" data  (GC output: lower ULID, applied first)
    [u_flush]     "new" data  (WAL segment: higher ULID, applied last → wins)

  Crash-recovery LBA state is correct.

  NOTE: this fix only covers segments that the WAL flush at gc_checkpoint
  time produces.  Any segments already sitting in pending/ before
  gc_checkpoint runs also have ULIDs below u_repack.  The coordinator
  enforces a separate invariant — drain pending/ to completion before
  calling gc_checkpoint — ensuring no pre-existing pending segment is
  present when GC runs.  That tick-ordering invariant is enforced in the
  coordinator daemon (drain_ok guard) and asserted in gc_fork
  (debug_assert on pending segment ULIDs).

  WHAT TLC CHECKS
  ---------------
  This module models one complete gc_checkpoint cycle with one LBA.
  TLC exhaustively explores all paths — write-before-checkpoint and no
  write — and verifies three invariants:

    UlidMonotonicity        — u_repack < u_sweep < u_flush
    WalFlushAboveRepack     — u_flush > u_repack (structural ordering)
    RebuildProducesCorrectLba — if a write arrived before checkpoint,
                                crash-recovery rebuilds to "new", not "old"
*)
EXTENDS Naturals

VARIABLES
  mint,           \* monotonic counter: models the ULID generator
  wal_open_ulid,  \* ULID assigned when the WAL was opened (before GC)
  u_repack,       \* ULID pre-minted for the GC repack output segment
  u_sweep,        \* ULID pre-minted for the GC sweep pass
  u_flush,          \* ULID pre-minted for the WAL flush at checkpoint time
  wal_has_write,  \* TRUE iff a write to LBA 0 arrived before gc_checkpoint
  phase,          \* "init" | "wal_open" | "checkpoint" | "crashed" | "rebuilt"
  lba0            \* value of LBA 0 after crash-recovery rebuild:
                  \*   "none"  — not yet computed (pre-crash)
                  \*   "old"   — GC repack output value (stale if wal_has_write)
                  \*   "new"   — recent write value (correct)

vars == <<mint, wal_open_ulid, u_repack, u_sweep, u_flush, wal_has_write, phase, lba0>>

\* ---------------------------------------------------------------------------
\* Type correctness
\* ---------------------------------------------------------------------------

TypeOK ==
  /\ mint          \in Nat
  /\ wal_open_ulid \in Nat
  /\ u_repack      \in Nat
  /\ u_sweep       \in Nat
  /\ u_flush         \in Nat
  /\ wal_has_write \in BOOLEAN
  /\ phase         \in {"init", "wal_open", "checkpoint", "crashed", "rebuilt"}
  /\ lba0          \in {"none", "old", "new"}

\* ---------------------------------------------------------------------------
\* Initial state
\* ---------------------------------------------------------------------------

Init ==
  /\ mint          = 1    \* counter starts at 1; 0 is the sentinel "not yet minted"
  /\ wal_open_ulid = 0
  /\ u_repack      = 0
  /\ u_sweep       = 0
  /\ u_flush         = 0
  /\ wal_has_write = FALSE
  /\ phase         = "init"
  /\ lba0          = "none"

\* ---------------------------------------------------------------------------
\* Actions
\* ---------------------------------------------------------------------------

(*
  The volume opens a new WAL.  The WAL gets the next available ULID from
  the mint counter.  This happens at startup or when the previous WAL is
  sealed — always before gc_checkpoint runs, so wal_open_ulid < u_repack.
*)
OpenWal ==
  /\ phase         = "init"
  /\ wal_open_ulid' = mint
  /\ mint'          = mint + 1
  /\ phase'         = "wal_open"
  /\ UNCHANGED <<u_repack, u_sweep, u_flush, wal_has_write, lba0>>

(*
  A write to LBA 0 is appended to the WAL before gc_checkpoint runs.

  Non-deterministic: TLC explores both paths (write fires / does not fire)
  to verify the invariant holds in both cases.  The guard ~wal_has_write
  ensures this fires at most once; a single write is sufficient to expose
  any ordering violation.
*)
WriteToWal ==
  /\ phase         = "wal_open"
  /\ ~wal_has_write
  /\ wal_has_write' = TRUE
  /\ UNCHANGED <<mint, wal_open_ulid, u_repack, u_sweep, u_flush, phase, lba0>>

(*
  gc_checkpoint runs: pre-mint u_repack, u_sweep, u_flush in sequence, then
  flush the WAL under u_flush.

  THE CRITICAL DESIGN DECISION: all three ULIDs are minted BEFORE any I/O.
  This is the "pre-mint" pattern.  Because u_flush is minted strictly after
  u_repack (u_flush = mint + 2, u_repack = mint), the WAL segment written to
  disk under u_flush will always sort above the GC repack output in any
  subsequent crash-recovery replay.

  For the purpose of this model, the entire operation is one atomic step:
  the ordering guarantee is established by the minting sequence, which is
  determined before any disk writes begin.  Crashes during the I/O phase
  are handled by the coordinator's idempotent retry logic (not relevant to
  the ordering invariant).
*)
GcCheckpoint ==
  /\ phase     = "wal_open"
  /\ u_repack' = mint
  /\ u_sweep'  = mint + 1
  /\ u_flush'    = mint + 2
  /\ mint'     = mint + 3    \* new WAL (not modelled) gets mint + 3
  /\ phase'    = "checkpoint"
  /\ UNCHANGED <<wal_open_ulid, wal_has_write, lba0>>

(*
  The system crashes after gc_checkpoint has run.  At this point the
  following segments are on disk:

    pending/<u_repack>   GC repack output: contributes "old" to LBA 0
    segments/<u_flush>     WAL segment:      contributes "new" to LBA 0
                                           iff wal_has_write = TRUE

  The coordinator may not yet have drained pending/ or completed the
  handoff — that is fine; crash-recovery reads both pending/ and segments/
  and replays them in ULID order.
*)
Crash ==
  /\ phase  = "checkpoint"
  /\ phase' = "crashed"
  /\ UNCHANGED <<mint, wal_open_ulid, u_repack, u_sweep, u_flush, wal_has_write, lba0>>

(*
  Crash-recovery rebuild: collect all segment files, sort by ULID ascending,
  replay in order.  The last entry for each LBA wins.

  Segments present on disk after gc_checkpoint:
    u_repack  — GC repack output; contains the old value for LBA 0
    u_flush     — WAL flush;        contains "new" iff wal_has_write

  Replay order (ascending ULID = chronological):
    1. Apply u_repack entry: lba0 = "old"
    2. Apply u_flush entry (if wal_has_write): lba0 = "new"

  Step 2 only overwrites step 1 if u_flush > u_repack.  WalFlushAboveRepack
  guarantees this.  The Rebuild action models the rebuild outcome explicitly
  to make the dependency on ULID ordering visible to TLC.
*)
Rebuild ==
  /\ phase  = "crashed"
  /\ lba0'  = IF wal_has_write
              THEN IF u_flush > u_repack THEN "new" ELSE "old"
              ELSE "old"
  /\ phase' = "rebuilt"
  /\ UNCHANGED <<mint, wal_open_ulid, u_repack, u_sweep, u_flush, wal_has_write>>

\* ---------------------------------------------------------------------------
\* Specification
\* ---------------------------------------------------------------------------

Next ==
  \/ OpenWal
  \/ WriteToWal
  \/ GcCheckpoint
  \/ Crash
  \/ Rebuild

Spec == Init /\ [][Next]_vars

\* ---------------------------------------------------------------------------
\* Safety invariants
\* ---------------------------------------------------------------------------

(*
  The three pre-minted ULIDs are strictly ordered.

  This holds from the "checkpoint" phase onwards.  It is a consequence of
  the sequential mint: u_repack = mint, u_sweep = mint+1, u_flush = mint+2
  at the moment GcCheckpoint fires.
*)
UlidMonotonicity ==
  phase \in {"checkpoint", "crashed", "rebuilt"} =>
    /\ u_repack < u_sweep
    /\ u_sweep  < u_flush

(*
  The WAL segment ULID is strictly above the GC repack ULID.

  This is the structural ordering guarantee: any crash-recovery replay in
  ascending ULID order will apply u_repack (old data) before u_flush (new
  data), so the WAL's write wins for any LBA it covers.

  Violated by the pre-fix implementation: there, the WAL was flushed under
  wal_open_ulid, which is always below u_repack (minted before GC ran).
  A model instantiated with u_flush = wal_open_ulid would fail this invariant
  in every trace where a write arrived before GcCheckpoint.
*)
WalFlushAboveRepack ==
  phase \in {"checkpoint", "crashed", "rebuilt"} =>
    u_flush > u_repack

(*
  After crash-recovery rebuild: if a write to LBA 0 arrived before
  gc_checkpoint, the rebuild must produce "new" (the write's value), not
  "old" (the stale GC repack output).

  This is the end-to-end correctness property.  It follows from:
    1. WalFlushAboveRepack:  u_flush > u_repack
    2. Rebuild replays in ascending order → u_repack applied before u_flush
    3. u_flush's "new" value overwrites u_repack's "old" value for LBA 0
*)
RebuildProducesCorrectLba ==
  (phase = "rebuilt" /\ wal_has_write) => lba0 = "new"

====
