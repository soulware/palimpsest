---
status: landed
related: [design-manifest-toml-removal.md, design-portable-live-volume.md]
landed_in: ../formats.md
---

# Volume size lives with the live owner

`size` previously sat in `manifest.toml` — unsigned, S3-IAM-trusted, decoupled from the CAS-protected `names/<name>` claim record that already serialised ownership and state. It now lives on `NameRecord` as a required field (schema `version = 2`), CAS-protected and chained into the signed event log.

Pulled-ancestor skeletons drop from `volume.pub + volume.provenance + manifest.toml` to just `volume.pub + volume.provenance` — every remaining skeleton file is now signed or a public key.

## Why this works

`size` has the same shape as `name`: mutable, single-writer, already gated by CAS on `names/<name>`. Putting them together collapses the trust model — the holder of the claim is the sole writer of capacity, just as it is the sole writer of ownership and state.

Ancestors don't need size. Every consumer operates on a *live* volume — the current fork being served, the source of a fork operation, or the import in progress:

- `src/lib.rs` — ublk/nbd `nr_sectors`
- `inbound.rs` — fork inheritance
- `filemap.rs` — ext4 scan
- `upload.rs` — re-publish
- `import.rs` — initial write

Pulled-skeleton ancestors are read-only segment containers; their data is reached through a child's LBA map, and the child's *own* size determines what the guest sees.

## Resize

Resize is a metadata operation: CAS-bump `names/<name>.size`, emit a signed `Resized { new_size }` event, then `UBLK_U_CMD_UPDATE_SIZE` (Linux 6.16+) for live capacity update. NBD has no live-update equivalent — resize against an NBD-served volume requires reconnect.

Local `volume.toml.size` is a cache, hydrated at create/fork/claim/resume time the same way `name` already is.

## Tradeoffs

- Size joins the same trust root as name/ownership/state — no more unsigned correctness-relevant field.
- Resize is a CAS, not a fork — no new ULIDs per resize, no ancestor chain growth.
- Readonly-import bucket claim is deferred until after OCI extraction (size unknown earlier); concurrent same-name imports race, the loser rolls back. Operator error to begin with.
