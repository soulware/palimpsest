# Elide

## Rust Code Quality Rules

These rules apply to all Rust code in this project. Follow them without needing to be reminded.

**No panicking code in library paths.**
- No `.unwrap()` or `.expect()` outside of tests and `main.rs`. Propagate errors with `?`.
- `.expect()` is only acceptable in tests, `main.rs` entry points, and for invariants that are genuinely impossible to violate — in that case, add a comment explaining why.
- No `panic!`, `unreachable!`, `todo!`, or `unimplemented!` in library code unless behind a `#[cfg(test)]` or clearly guarded.

**Avoid unnecessary data copies.**
- Prefer borrowing (`&[u8]`, `&str`) over cloning when the lifetime allows it.
- Avoid `.to_vec()`, `.clone()`, or `Vec::new()` allocations on hot paths unless genuinely necessary.
- If a function only reads data, take a slice not an owned value.
- Avoid allocating a `Vec` for a small, fixed-size header — use a stack buffer (`[u8; N]`) instead.
- When you already own an allocation, pass it by value rather than borrowing it only to copy it again. The pattern `some_option.as_deref().unwrap_or(fallback)` followed by `.to_vec()` is a sign you should use `some_option.unwrap_or_else(|| fallback.to_vec())` and pass ownership directly.

**Error handling.**
- Use `io::Error::other(msg)` not `io::Error::new(io::ErrorKind::Other, msg)`.
- Match on `error.kind()` when distinguishing error cases — don't swallow errors with a catch-all `Err(_)`.
- Error messages should be lowercase and not end with punctuation (Rust convention).

**Code quality.**
- Run `cargo fmt` before committing (enforced by pre-commit hook). Clippy with `-D warnings` is enforced in CI, not locally.
- Fix all clippy warnings — don't accumulate `#[allow(...)]` suppressions unless there is a deliberate, documented reason.
- Prefer `div_ceil()` over manual ceiling-division idioms.
- Use const-generic `read_fixed::<N>` helpers rather than `.try_into().expect(...)` for fixed-size slice conversions.

**Prefer crates over hand-rolled implementations.**
- Before implementing a non-trivial algorithm or format (e.g. ULID, UUID, base64, checksums), check whether a well-known crate exists and discuss with the user before deciding to roll it by hand.
- Hand-rolling is sometimes the right call (zero-dep binary, custom constraints), but the choice should be explicit, not a default.

**Parse, don't validate: use typed parsers when reading string representations.**
- When reading a string from an external source (filename, file content, CLI arg) that represents a typed value, always parse it through the type's own parser rather than using the raw string directly.
- This validates the value at the boundary and produces a canonical string if re-serialised (e.g. `Ulid::from_string(s)?.to_string()`, not `s.to_owned()`).
- The same applies to any structured string: paths, hashes, addresses, IDs.

**Monotonic ULIDs in tests.**
- When a test mints two or more ULIDs that must be ordered (e.g. a parent segment and a later delta segment), use `elide_core::ulid_mint::UlidMint` — seed with `Ulid::nil()` and call `.next()` per ULID.
- Two back-to-back `Ulid::new()` calls in the same millisecond produce ULIDs in random order — a flake source that has bitten CI more than once.
- Independent test IDs (distinct volume dirs, unrelated segment IDs in uniqueness tests) can still use `Ulid::new()` directly.

## Design principles

**No backward compatibility by default.**
- When a change would break existing on-disk data or behaviour, surface the tradeoff explicitly and discuss it — don't silently add a legacy/optional path to avoid the conversation.
- The default answer is often "break it": data can be regenerated, tooling can migrate it, and optional paths add permanent complexity.
- A compatibility path may sometimes be warranted, but it is never free: it adds code complexity, and — critically — it creates execution paths where important operations are skipped, making the system harder to reason about. That cost must be justified explicitly.
- If backward compatibility is genuinely needed, it should be an explicit, reasoned decision, not a reflex.

**No optional paths for correctness properties.**
- If a property must hold (e.g. every segment is signed), enforce it unconditionally — no fallback mode, no warn-and-continue.
- An optional path for a correctness invariant means the invariant doesn't actually hold.

## Documentation

Two tiers, separated by audience:

**Synthesis docs at `docs/` top level — current truth, kept tight.** README links these.

- `docs/overview.md` — problem statement, key concepts, operation modes, empirical findings
- `docs/findings.md` — empirical measurements: dedup rates, demand-fetch patterns, delta compression data, write amplification
- `docs/architecture.md` — system architecture, directory layout, write/read paths, LBA map, extent index, dedup, snapshots
- `docs/formats.md` — WAL format, segment file format, S3 retrieval strategies
- `docs/operations.md` — GC, repacking, boot hints, filesystem metadata awareness
- `docs/testing.md` — property-based tests: ULID monotonicity invariant, crash-recovery oracle, simulation model
- `docs/integrations.md` — Docker, Firecracker, Cloud Hypervisor, Kubernetes integration targets

**Working record at `docs/notes/` — LLM-targeted, indexed in [`docs/notes/INDEX.md`](docs/notes/INDEX.md).**

- Design discussions (`design-*.md`), implementation plans (`*-plan.md`), dated status snapshots (`status-YYYY-MM-DD.md`), prior-art notes (`reference-*.md`).
- New designs and plans go here, not at `docs/` top level. README does **not** index this directory.
- Consult `docs/notes/INDEX.md` before proposing changes in areas that have prior design discussion.

### Rules for synthesis docs

These apply to `docs/architecture.md`, `docs/formats.md`, `docs/operations.md`, `docs/testing.md`, `docs/findings.md`, `docs/overview.md`, `docs/integrations.md`.

**Edit, don't append.** When a PR touches a synthesis doc, the default is to edit existing prose — replace stale text, tighten, relocate detail to `docs/notes/`. Pure additions need explicit justification; the failure mode is unbounded accretion.

**Detail belongs in `docs/notes/`, not in synthesis docs.** Synthesis docs explain *what* the system does and *why*, at summary level. Mechanical detail, algorithmic walkthroughs, and decision rationale belong in a `design-*.md` under `docs/notes/`. If a section is growing past summary level, move detail out and link to it.

**Retire-on-land.** When a feature lands and updates a synthesis doc, the corresponding `design-*.md` gets a `landed: true` (and `landed_in: <synthesis-section>`) frontmatter line and stops being maintained. The synthesis doc is canonical; the note becomes history. Two sources of truth is the failure mode.

**Soft length budgets.** Targets, not hard limits — exceeding triggers a trim or split, not a block:

- `docs/architecture.md` — 800 lines
- `docs/formats.md` — 600 lines
- `docs/operations.md` — 500 lines
- `docs/testing.md`, `docs/findings.md`, `docs/integrations.md`, `docs/overview.md` — 300 lines each

If a doc has grown past its budget, the next PR touching it should trim or split before adding.

### Rules for `docs/notes/`

**One new doc per substantial PR is fine.** That's the point of the directory — keep the written record. New `design-*.md` and `*-plan.md` are encouraged for non-trivial work. Status docs (`status-YYYY-MM-DD.md`) are point-in-time snapshots: never edit one after its date; write a new one or update synthesis docs.

**Frontmatter convention** (see `docs/notes/INDEX.md` for the schema). Add to new notes; existing notes will be backfilled lazily.

**Don't index `docs/notes/` from `README.md`.** The directory is for Claude. Update `docs/notes/INDEX.md` when adding a new note.

## References

- `refs/lsvd-paper.pdf` — local copy of "Beating the I/O Bottleneck: A Case for Log-Structured Virtual Disks" (EuroSys 2022)
- `refs/lsvd/` — local clone of lab47/lsvd, Evan Phoenix's Go reference implementation
- [composefs/composefs-rs](https://github.com/composefs/composefs-rs) — Rust composefs implementation
