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
- Run `cargo fmt` and `cargo clippy -- -D warnings` before committing (enforced by pre-commit hook).
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

## Documentation

Design documentation is indexed in `README.md` and lives in `docs/`.

- `docs/overview.md` — problem statement, key concepts, operation modes, empirical findings
- `docs/findings.md` — empirical measurements: dedup rates, demand-fetch patterns, delta compression data, write amplification
- `docs/architecture.md` — system architecture, directory layout, write/read paths, LBA map, extent index, dedup, snapshots
- `docs/formats.md` — WAL format, segment file format, S3 retrieval strategies
- `docs/operations.md` — GC, repacking, boot hints, filesystem metadata awareness
- `docs/testing.md` — property-based tests: ULID monotonicity invariant, crash-recovery oracle, simulation model
- `docs/reference.md` — lsvd reference comparison, implementation notes, open questions

## References

- `refs/lsvd-paper.pdf` — local copy of "Beating the I/O Bottleneck: A Case for Log-Structured Virtual Disks" (EuroSys 2022)
- `refs/lsvd/` — local clone of lab47/lsvd, Evan Phoenix's Go reference implementation
- [composefs/composefs-rs](https://github.com/composefs/composefs-rs) — Rust composefs implementation
