# Coordinator-side mint enrollment — implementation plan

Implements the coordinator's half of mint enrollment: the thing that
actually writes `<data_dir>/credentials/<role>`. The mint server
(`/v1/enroll`, `/v1/enroll-exchange`, operator approve) and the generic
reference client already exist and are conformance-tested
(`mint/tests/enroll.rs`). `elide-coordinator/src/mint_client.rs` already
has the steady-state `assume-role` half and explicitly defers
provisioning ("provisions the `credentials/<role>` files — out of scope
here"). This plan fills that gap.

Authority: `docs/design-mint.md` § *Enrollment*, § *Coordinator
configuration*, § *Coordinator store architecture*.

## Shape: one blocking operator command + a hard startup gate

Enrollment is **not** a daemon reconciler. It is a single operator-run
command that performs the whole `A → (wait for B) → C` sequence, and
the daemon **refuses to start** (when `[mint]` is configured) until it
has completed successfully.

- **A — `POST /v1/enroll`.** The command attenuates the
  operator-supplied bootstrap macaroon with `sub=<coord-ulid>` and
  `cnf=ed25519:<coordinator.pub>`, PoP-signs the body (`{ts}`) with
  `coordinator.key`, and receives the short-lived credential ticket. It
  prints the `cnf` fingerprint and the exact `mint enroll approve
  <coord-ulid>` line the operator runs out of band.
- **wait for B — operator approval.** The command blocks, polling
  `/v1/enroll-exchange`. `403` ⇒ not approved yet ⇒ backoff and retry.
  The operator runs `mint enroll approve <coord-ulid>` on the mint host
  (existing code, not ours), matching the printed fingerprint through a
  trusted side channel first.
- **C — exchange fan-out.** Once approved, the command exchanges the
  ticket once per role in the canonical inventory (`coord-base`,
  `coord-writer`, `coord-data`, `volume-ro`) — body `{ts, role}`, same
  PoP — and writes each re-minted credential to
  `credentials/<role>` (mode `0600`). The pending record is not
  consumed per exchange (multi-use until the ticket `exp`), so one
  approval covers all four.

On success: four files under `credentials/`. Exit `0`.

### Startup gate

In `main.rs`, in the existing `if let Some(mint_cfg) = &config.mint`
branch (right after `mint_cfg.validate()?`, before constructing
`MintScopedStores`): assert every role file in the canonical inventory
exists under `<data_dir>/credentials/` and decodes as a macaroon.
Missing or undecodable ⇒ `bail!` with a message naming the missing
role(s) and pointing at `elide coord enroll`. The `[mint]`-absent
branch (shared-key `PassthroughStores`) is unchanged — no gate.

This makes the doc's "not exercisable end-to-end until enrollment
provisions the files" a hard precondition rather than a latent runtime
failure on first S3 touch.

## Decisions

1. **Single blocking command, not split enroll/exchange.** The
   reference client splits them across CLI invocations and therefore
   persists `credential.ticket` to disk. The coordinator collapses the
   sequence into one process, so **the ticket lives in memory for the
   command's duration and never touches disk**. `credentials/<role>` is
   the only durable enrollment artifact. Reasoned deviation from the
   reference client; consistent with keeping on-disk state inspectable
   and minimal.

2. **Self-healing the ticket-expiry race.** Because the single command
   holds the bootstrap macaroon for its whole duration, if the ticket
   `exp` passes during the wait-for-approval (operator slow), the
   command transparently re-runs A for a fresh ticket and continues.
   The split hybrid could not do this (no bootstrap on disk). This is
   the main argument for the single-command shape over a daemon
   reconciler. After ticket expiry the mint-side pending record is GC'd
   and needs fresh approval — the command surfaces that it is
   re-enrolling so the operator knows a re-approve is required.

3. **Bootstrap is operator-supplied, never config.** `<mac|file|->`
   argument mirroring `mint client enroll`; not a `[mint]` key. The
   bootstrap is reusable and non-expiring — parking it in
   `coordinator.toml` on every host is the surface we explicitly avoid.
   `MintConfig` stays as-is (`url` + timeouts only).

4. **Canonical role inventory consolidated.** The four role-name
   constants are currently split: `ROLE_COORD_BASE/_WRITER/_DATA` in
   `mint_stores.rs`, `ROLE_VOLUME_RO` in `mint_client.rs`. Introduce one
   `pub const COORD_ENROLL_ROLES: &[&str]` (single source of truth) used
   by the exchange fan-out **and** the startup gate **and** referenced
   by the existing stores, so the three can never drift. `coord-data`
   is per-volume only at `assume-role` time (the `elide:Volume`
   narrowing caveat) — enrollment still produces exactly one
   `credentials/coord-data`.

5. **All-or-nothing per run, idempotent per file.** If C partially
   completes and the ticket then expires, decision (2) re-enrolls and
   continues only the missing roles (per-file presence check). Re-running
   `elide coord enroll` after a full success is a no-op-ish: A is
   idempotent for identical `(sub, pub)` (fresh ticket, same record);
   already-present credentials are permanent and left untouched unless
   `--force`.

## Code layout

- **New `elide-coordinator/src/enroll.rs`.** Enroll/exchange are
  one-shot provisioning; `assume-role` is steady-state — different
  lifecycles, shared primitives. Reuses, from `mint_client.rs` (made
  `pub(crate)` as needed): `WireMacaroon` (decode/attenuate/encode),
  `pop_digest`, `post` (TCP + UDS), `now_unix`, the JSON field helpers.
  No new crypto or transport.
- **`mint_client.rs`** — add `pub(crate)` to the shared primitives;
  add `COORD_ENROLL_ROLES`; otherwise untouched.
- **`main.rs`** — new `Command::Enroll`; startup-gate check in the
  `[mint]` serve branch.
- **`config.rs`** — unchanged (documented decision 3).

### CLI surface

```
elide coord enroll [--data-dir <dir>] <bootstrap-macaroon | file | ->
                    [--timeout <humantime>] [--force]
```

- positional bootstrap source: inline macaroon, a file path, or `-`
  for stdin (same resolution as `mint client enroll`).
- `--timeout`: overall wait-for-approval bound (default e.g. `30m`);
  on timeout, exit non-zero with the resume instruction (re-run is safe).
- `--force`: re-exchange and overwrite existing `credentials/<role>`
  (default: keep present files, only fill missing).
- Loads `CoordinatorIdentity::load_or_generate(data_dir)` for
  `sub`/`cnf`/PoP. Reads `[mint] url` + timeouts from the resolved
  config (same config plumbing as `serve`).

## Edge cases

- **403 forever** (operator never approves): bounded by `--timeout`;
  clear message, idempotent re-run.
- **401 at enroll**: bad/stale bootstrap or wrong `op` — fail fast with
  the mint error snippet (mint's error model is deliberately coarse).
- **401 at exchange**: ticket expired — decision (2) re-enrolls
  automatically; if that also 401s, the bootstrap itself is
  stale/rotated (`mint bootstrap rotate`) — fail with that diagnosis.
- **pub/sub conflict** (mint surfaces a different-key anomaly to the
  operator): exchange keeps returning non-200; the command reports the
  mint snippet so the operator can `mint enroll list` and reconcile.
- **`[mint]` configured, `coord enroll` never run**: startup gate
  `bail!`s — the daemon does not come up half-credentialed.

## Testing

- Unit (in `enroll.rs`, mirroring `mint_client.rs` tests): bootstrap
  attenuation chain (`sub`/`cnf` appended in order), PoP digest over
  `{ts}` / `{ts, role}`, ticket-expiry → re-enroll branch with a fake
  `post`.
- Integration against the real mint binary: extend the pattern in
  `mint/tests/enroll.rs` / the coordinator side — spin mint on a UDS,
  run the enroll command with `--timeout` short, drive
  `mint enroll approve`, assert all four `credentials/<role>` files
  appear and decode, and that a second run is idempotent.
- Startup gate: `serve` with `[mint]` and an empty `credentials/`
  fails with the expected message; with all four present, proceeds.

## Prototype validation

Walked the full sequence end-to-end with the `mint` binary alone
(server + reference client + operator subcommands) over a UDS, against
a 4-role config (`coord-base`, `coord-writer`, `coord-data`,
`volume-ro`), fake minter. Findings that shaped the plan above:

1. **The split CLI is the wrong operator surface — confirms the
   single-command decision.** End-to-end is `client keygen` → `client
   enroll` → *(out-of-band approve)* → `client exchange` **once per
   role**: six client invocations plus the operator step, with the
   caller expected to know all four role names and loop. The reference
   client's per-step granularity exists for conformance/generality; the
   coordinator wants the macro. `elide coord enroll` doing A +
   approval-poll + the role fan-out internally is the right collapse.

2. **Exit codes are a clean tri-state for the internal wait loop.**
   `enroll` → `0` on 200; `exchange` → `0` on 200, **`2` on 403**
   (awaiting approval), `1` on transport/other. So the wait loop is
   `0`=done / `2`=keep polling / else=hard fail — decision-table clean.
   Caveat learned the hard way: a pipe to a formatter masks the child
   status (`$?` catches the formatter); the driver must read the
   process exit directly.

3. **Ticket is genuinely multi-use until `exp`.** One approval, then
   four role exchanges *and* an idempotent re-exchange all returned
   `0` and wrote/overwrote `credentials/<role>`. Confirms the design
   doc and validates both the fan-out and the partial-completion
   resume (re-run only fills missing roles).

4. **The ticket-expiry race is real and short.** The prototype ticket
   `exp` was ~10 min out. An operator slow to approve will blow it,
   and the split-CLI shape then needs a *manual* re-enroll **and**
   re-approve (the pending record is GC'd at `exp`). This concretely
   justifies plan decision (2): the single command, holding the
   bootstrap for its whole duration, re-enrolls transparently — the
   reference client structurally cannot.

5. **Operator side needs no work.** `enroll list` shows the `cnf`
   fingerprint for out-of-band comparison; `approve` prints it again
   and gates on interactive y/N with `--yes` for automation. That half
   is done; this plan is purely the coordinator-side macro.

## Out of scope

- The steady-state Tigris-keypair cache / proactive refresh
  (`assume-role` side, already designed in § *Coordinator store
  architecture*) — credentials themselves never expire, so enrollment
  is converge-once and has no refresh cadence.
- `mint bootstrap rotate` handling beyond surfacing the diagnosis.
- The future domain-typed S3 layer (§ *Coordinator store architecture*,
  explicitly separate work).
