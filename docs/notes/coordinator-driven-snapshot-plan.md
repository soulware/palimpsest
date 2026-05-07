# Coordinator-driven snapshot — design rationale

Status: landed. Shipped in #42 (coordinator-driven sequence + signed
ancestor manifests) and extended in #46 (snapshot-time filemap).

For the current flow and code layout see `docs/architecture.md` and
`elide-coordinator/src/inbound.rs`'s `snapshot_volume`. This doc only
keeps the design *reasoning* behind decisions that aren't obvious from
reading the code.

## Why the coordinator owns the sequence

Snapshot used to run in-process in the CLI: `Volume::snapshot()` wrote
a marker and returned, with drain/upload happening later on the
coordinator's tick loop. Two problems forced the move:

1. **"Snapshot complete" didn't mean "durable in S3".** A user who
   snapshotted and then deleted locally could lose data — the marker
   was on disk but `pending/` hadn't drained.
2. **No seam for a post-drain completeness manifest.** The signed
   `.manifest` has to list every `index/<ulid>.idx` belonging to the
   snapshot, which only exists after drain+promote — outside the
   volume's snapshot call as it stood.

Making the coordinator drive the sequence resolves both: it holds a
per-volume snapshot lock, runs drain/upload/promote inline, then asks
the volume to sign the manifest and write the marker. The volume
process still owns anything touching the private signing key.

## Why not a "snapshot" WAL record

Considered and rejected. A WAL record is the right tool when recovery
needs to replay a decision after a crash — but a snapshot isn't a
mutation of volume state. It's a durability fence (`pending/` empty,
`index/` populated) plus a signed manifest. The fence is observable
from directory state; there is nothing for WAL replay to reconstruct.
A WAL record would introduce a new recovery invariant ("partial
snapshot in WAL → resume from where?") for no gain, and would push the
orchestration back into the volume process — the opposite of what we
want. Coordinator-retries-on-restart is the recovery model.

## Why `.manifest` is full, not a delta

Each `snapshots/<S>.manifest` lists **every** ULID in this volume's
`index/` as of snapshot `S`. It is not a delta over the previous
snapshot within the same volume.

A fork's open-time verification walks one manifest per volume in the
ancestry chain, not one per snapshot per volume. If manifests were
deltas, verifying a fork of a mid-history snapshot would require
chaining through every prior snapshot on every ancestor. Full
manifests collapse that two-dimensional walk into a one-dimensional
one: walk the fork chain, read exactly one `.manifest` per ancestor,
union the ULIDs, check each file exists.

Size cost is negligible: ~26 bytes per ULID line, so 10k segments ≈
260 KB signed. Trivial next to the `.idx` files themselves.

## Trust root: provenance chain, not on-disk `volume.pub`

Each ancestor's `.manifest` is verified with that ancestor's public
key, but the key is taken from the **child's signed provenance**, not
from the ancestor's `volume.pub` file on disk. Each `volume.provenance`
embeds `parent_pubkey` under the child's signature, so trusting the
current volume transitively trusts every ancestor.

This works because **keys never rotate**. A volume's keypair is minted
once at creation and used for the life of the volume. "Rotation" means
forking: the fork gets a fresh keypair, pins the old volume as its
parent, and from that point the old volume is read-only ancestor data.
No in-place rotation, no keyring, no revocation list.

Consequence: if an ancestor's on-disk `volume.pub` ever disagrees with
what the child's provenance says it was, the provenance wins. That is
the whole point of embedding it.
