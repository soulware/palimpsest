# Palimpsest

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

**Error handling.**
- Use `io::Error::other(msg)` not `io::Error::new(io::ErrorKind::Other, msg)`.
- Match on `error.kind()` when distinguishing error cases — don't swallow errors with a catch-all `Err(_)`.
- Error messages should be lowercase and not end with punctuation (Rust convention).

**Code quality.**
- Run `cargo fmt` and `cargo clippy -- -D warnings` before committing (enforced by pre-commit hook).
- Fix all clippy warnings — don't accumulate `#[allow(...)]` suppressions unless there is a deliberate, documented reason.
- Prefer `div_ceil()` over manual ceiling-division idioms.
- Use const-generic `read_fixed::<N>` helpers rather than `.try_into().expect(...)` for fixed-size slice conversions.

## References

- [Beating the I/O Bottleneck: A Case for Log-Structured Virtual Disks](https://dl.acm.org/doi/pdf/10.1145/3492321.3524271) — the original LSVD paper
- [lab47/lsvd](https://github.com/lab47/lsvd) — Evan Phoenix's Go reference implementation
- [composefs/composefs-rs](https://github.com/composefs/composefs-rs) — Rust composefs implementation
