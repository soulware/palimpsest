# Fork from remote — plan

Branch: `fork-from-remote`

## Goal

`elide volume fork --from <vol_ulid>/<snap_ulid> <new_name>` should work whether the ancestor is local or remote. Having the ancestor local is an optimization; by construction it is a readonly fork source.

## Model

- **Writable local volumes** live in `by_id/<vol_ulid>/`, symlinked from `by_name/<name>`.
- **Pulled readonly ancestors** live in a separate tree `readonly/<vol_ulid>/`. They have `volume.pub` but no private key. They are never supervised, never serve NBD, and only exist as fork sources.
- Each volume (writable or readonly ancestor) keeps its own `index/` signed with its own keypair. No cross-volume `.idx` mixing.
- Segment bodies are still demand-fetched from `elide_store/`. Only `.idx` files and skeleton metadata are materialized at pull time.
- Addressing is explicit `<vol_ulid>/<snap_ulid>` for phase 1. Human-readable names/tags are a later layer.

## Why the layout split matters

The `.idx` signature bug we hit (`invalid signature` on a prefetched ancestor index) is a direct consequence of the current prefetch path dumping every ancestor's `.idx` into the child's `index/` directory. `rebuild_segments` then tries to verify all of them against the child's `volume.pub`, which fails because each `.idx` was signed by its origin volume's private key. Putting each ancestor in its own `readonly/<vol_ulid>/` with its own `volume.pub` makes the per-volume signing invariant hold naturally.

Key references:
- Signature check: `elide-core/src/segment.rs:789`
- Sign-on-write: `elide-core/src/segment.rs:494-500`
- Prefetch writes ancestor `.idx` into child dir: `elide-coordinator/src/prefetch.rs:146`
- Rebuild uses `self.verifying_key` for all of `index/`: `elide-core/src/volume.rs:668`
- Supervisor gate on `volume.readonly`: `elide-coordinator/src/daemon.rs:142`
- Import sets `volume.readonly` at spawn: `elide-coordinator/src/import.rs:292`
- Remote pull (to be rewritten): `src/main.rs:1247` (currently conditional — already edited on branch to be unconditional, but will move under the layout refactor)

## Status

Phases 1a, 1b, 1c, and 2 are implemented on this branch. See
`git log fork-from-remote ^main` for the commits. Key tests:
- `walk_ancestors_crosses_into_readonly_tree` (elide-core)
- `discover_volumes_scans_readonly_tree` (elide-coordinator)
- `prefetch_indexes_writes_readonly_ancestor_idx` (elide-coordinator)
- `fork_volume_at_pins_explicit_snapshot_without_requiring_local_marker` (elide-core)

## Phases

### Phase 0 — confirmed cause, no code
Signature bug is explained by the layout issue. No separate fix needed; it falls out of phase 1.

### Phase 1a — layout refactor
- Introduce `readonly/<vol_ulid>/` as the target tree for pulled ancestors.
- `remote pull` writes there instead of `by_id/`.
- Coordinator discovery scans `readonly/` but does not supervise entries in it.
- `walk_ancestors` (and any parent-ULID lookup) checks both `by_id/` and `readonly/`.
- `remote pull` input changes to `<vol_ulid>/<snap_ulid>` and walks the ancestor chain, pulling each ancestor skeleton.
- Fail hard on ULID vs name collisions.

### Phase 1b — prefetch fix
- Each ancestor's `.idx` files land in `readonly/<ancestor_ulid>/index/`, not in the child's `index/`.
- Prefetch walks the chain and populates each ancestor directory independently.

### Phase 1c — rebuild_segments
- A volume verifies only its own `index/` against `self.verifying_key`.
- Ancestor indexes are read from each ancestor's own directory and verified with that ancestor's `volume.pub`.
- Likely mostly falls out of 1a/1b once the directory split is in place; audit the read path to confirm.

### Phase 2 — `volume fork --from <vol_ulid>/<snap_ulid>`
- Resolve parent in `by_id/` first, then `readonly/`.
- If not found in either, pull the chain on demand (phase 1 logic), then proceed with local fork.
- Fork writes provenance pinning the explicit snapshot ULID.
- `remote pull` remains as a user-facing command for "I want the ancestor locally without forking yet."

### Deferred / later
- Human-readable snapshot tags (`snap.tag = "pre-upgrade"`) as an alias layer over `<vol_ulid>/<snap_ulid>`.
- Name-based remote resolution (currently explicit ULIDs only).
- `volume delete` refusing or cascading when a fork parent has live children (separate bug — see `elide-coordinator/src/inbound.rs:417`).
- Cleanup of the now-redundant `volume.readonly` marker (directory placement is the real discriminator; marker can stay for belt-and-braces).

## Open questions

- Does `readonly/` need its own GC/eviction story, or is it just "pull once, keep forever until user removes"? Probably the latter for phase 1.
- Where does `volume.parent` vs `volume.provenance` fit in the readonly tree? Confirm both are written during pull.
- When fork-from-remote pulls an ancestor chain, what happens if a mid-chain ancestor is already present locally in `by_id/` (writable)? Probably use the local copy and stop walking up from there — but confirm the extent index rebuild is happy mixing sources.
