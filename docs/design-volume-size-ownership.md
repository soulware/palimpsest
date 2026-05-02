# Volume size lives with the live owner

**Status:** Implemented.

`size` was previously the only mutable, load-bearing field carried by `manifest.toml`, and it had no cryptographic anchor — it sat in an unsigned TOML file on S3, trusted via S3 IAM alone. It now lives on the `names/<name>` claim record (already authoritative for current ownership, already CAS-protected, already chained into the signed event log). Ancestors carry no size on disk at all.

## The shape

- `size: u64` is a required field on `NameRecord` (the TOML at `names/<name>`); schema bumped to `version = 2`.
- `manifest.toml` no longer carries `size` (and is now gone entirely — see [design-manifest-toml-removal.md](design-manifest-toml-removal.md)). Pulled-ancestor skeletons fetch `volume.pub + volume.provenance` only — no `volume.toml`.
- The single owner of `names/<name>` — the coordinator currently holding the claim — is the sole writer of `size`. Updates are CAS-mutations on the same record that already serialises ownership, state, and handoff snapshot.
- Local `volume.toml.size` is a *cache* of the authoritative `names/<name>.size`, hydrated at create / fork / claim / orphan-resume time, the same way `name` is already cached locally.

## Why this works

`size` has the same shape as `name`: mutable, identity-ish, single-writer, and already gated by the CAS-on-`names/<name>` mechanism. The current asymmetry — name in `names/<name>`, size in `manifest.toml` — exists by accident, not by design. Putting them together collapses the trust model:

| Concern | Today | Proposed |
|---|---|---|
| Ownership | `names/<name>.coordinator_id` | unchanged |
| State | `names/<name>.state` | unchanged |
| Capacity | `manifest.toml.size` (unsigned, S3-IAM-only) | `names/<name>.size` (CAS, signed in event log) |
| Authoritative writer | implicit | explicit: holder of `names/<name>` |

## Why ancestors don't need it

Tracing actual size readers (`src/lib.rs:104` → ublk/nbd `nr_sectors`; `inbound.rs:2061` fork inheritance; `filemap.rs:165` ext4 scan; `upload.rs:461` re-publish; `import.rs:259` initial write), every path that consumes `size` operates on a *live* volume — the current fork being served, the source of a fork operation, or the import in progress. Ancestors are read-only segment containers; their data is reached through a child's LBA map, and the child's *own* size determines what the guest sees. There is no read path that consults an ancestor's `size`.

So ancestors simply stop carrying it. Pulled skeletons drop from `volume.pub + volume.provenance + manifest.toml` to `volume.pub + volume.provenance`. The peer-fetch trust gap closes (every skeleton file is now either signed or a public key), and the bootstrap pull is one fewer S3 GET per ancestor.

## Resize semantics

Resize becomes a metadata operation against the claim record, not a fork:

1. Coordinator computes the new size (validate: shrink requires no LBAs ≥ new_size carry data; grow is unconditional).
2. CAS on `names/<name>` bumps `size`; emits a signed `Resized { new_size }` entry in `events/<name>/`.
3. Coordinator (or volume process) calls `UBLK_U_CMD_UPDATE_SIZE` (Linux 6.16+) on the running ublk device — `set_capacity_and_notify()` updates the gendisk capacity live, no I/O interruption.
4. Local `volume.toml.size` cache is updated.

NBD has no equivalent live-update; resize against an NBD-served volume requires reconnect (acceptable: NBD is the simpler-deployment transport, not the primary one).

## What happens to `manifest.toml`

Once `size` moves out, the remaining fields are all derivable or non-load-bearing:

- `name` — already not consumed (pull explicitly drops it in favour of `names/<name>`).
- `origin` — redundant with `volume.provenance.parent`, which is signed.
- `source` (OCI digest/arch) — bookkeeping only, never consumed by code.
- `readonly` — implied by `volume.readonly` marker presence and `volume.key` absence.

Dropped entirely — see [design-manifest-toml-removal.md](design-manifest-toml-removal.md). OCI source migrates to a signed `oci_source` block on `volume.provenance`; the other three fields go away with no replacement.

## Migration

Per project convention (no backwards-compat by default): the schema bump from `version = 1` → `version = 2` is fresh-bucket-only. `NameRecord::from_toml` rejects unknown versions, so coordinators upgrading against a populated bucket will refuse to parse pre-existing records. Buckets must be re-created.

For readonly imports, the bucket-side `mark_initial_readonly` claim is now deferred from `spawn_import` to post-`import_image` completion (the size isn't known until ext4 extraction). Two coordinators racing on the same name both download/extract; the second to claim sees `AlreadyExists` and rolls back local state. Wasted CPU/bandwidth, no corruption.

## Tradeoffs

- **Pro:** size joins the same trust root as name/ownership/state. No more unsigned correctness-relevant field.
- **Pro:** peer-fetch can extend to the skeleton without further design — both remaining files are signed/anchored.
- **Pro:** resize is a CAS, not a fork — no new vol_ulids per resize, no ancestor chain growth, no name rebind needed.
- **Con:** ublk online resize requires Linux 6.16+. Older kernels need the older fall-back (stop + del + re-add) — but that's true of any resize implementation, with or without this proposal.
- **Con:** an ancestor pulled by a host that *only* needs it as a fork source carries no size locally. Diagnostic tools that walk ancestors and want sizes must either look up `names/<ancestor-name>` (only works if the ancestor is currently named) or accept that ancestor size isn't recoverable. No production code path does this.
- **Con:** readonly-import bucket claim is deferred until after the OCI image is extracted, opening a small race window for concurrent same-name imports on different hosts. Both downloads complete; one wins the claim, the other rolls back. Operator error to begin with — and the alternative (claim up-front, sentinel size) was a worse fit for the trust model.
