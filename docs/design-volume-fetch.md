# Volume fetch

**Status:** Proposed. No implementation. Captures the design discussion for a `volume fetch <name>` verb that warms a foreign volume's local cache without claiming it.

## Problem

Today, the only ways to pull a remote volume's bytes onto a host are:

- `volume create --from <name>` — mints a writable fork, does pull the segments, but creates a new local volume.
- `volume claim` (after a release) — takes ownership and starts demand-fetching from a cold cache on first reads.

Neither covers "I want this remote volume's bytes on my host, ready, but I'm not claiming it." The motivating use case is **pre-positioning for a future claim**: if a host that may need to claim a volume warms its cache in advance against a recent snapshot, the eventual claim only has to fetch segments newer than that snapshot — the claim-time gap is bounded by snapshot cadence, not by the volume's full size.

## Verb

```
volume fetch <name>
```

Resolves `<name>` against `names/<name>` (the existing remote name-record path), pulls the ancestor chain, and warms the local cache against the volume's most recent published snapshot. Fails if the remote has no snapshot — see "Basis" below. Does not mint a fork, claim ownership, or start a daemon.

## Basis: most recent existing snapshot

`volume fetch` operates against the volume's most recent **owner-signed** snapshot, not a synthesised "now" snapshot.

This is a deliberate choice. A signed snapshot's manifest is the authoritative enumeration of which segments belong to the volume at that point in time, signed by the owner under the volume's `volume.pub`. Using it as the basis means:

- The fetch set is exactly what the manifest enumerates — no LIST round-trips, no need to invent which post-snapshot segments to include.
- Signature verification follows the existing path (`verify_manifest` against `volume.pub`).
- We don't introduce phantom ancestors: there is no synthesised manifest signed by a non-owning coordinator.

The implication is that **snapshot cadence is the current owner's responsibility**. If the owner snapshots rarely, `volume fetch` will warm against an older basis, and a later claim will have a larger gap to demand-fetch. If the owner snapshots frequently, the gap is small. This is the right place for the policy to live: the owner controls writes, so the owner controls how stale the latest published basis can be. A foreign host can't usefully synthesise around that.

If no snapshot exists, `volume fetch` fails. The synthesised-now path already exists for the recovery use case (`volume release --force`'s `mint_and_publish_synthesised_snapshot`) and for the readonly-fork-base use case (`force_snapshot_now_op` behind `volume create --from --force-snapshot`); extending it to `volume fetch` would conflate hydration with attestation and introduce a phantom ancestor under a non-owning key. Those are separate problems with separate verbs.

## State

A fetched volume is a **distinct lifecycle state**, not an opportunistic readonly ancestor. The on-disk shape (index + cache, no `volume.key`, no fork) overlaps with the readonly-ancestor layout, but the semantics differ enough that conflating the two would cause real problems:

- **Lifecycle.** A readonly ancestor is pinned by a descendant fork — deleting it would orphan the fork. A `Fetched` volume has no descendant; it is its own root, pinned by nothing, and is freely removable. Conflating the two means either forbidding cleanup of fetched copies (wrong) or allowing cleanup of pinned ancestors (corruption).
- **Refresh.** A readonly ancestor is immutable: its content is whatever was pulled when its descendant was minted. A `Fetched` volume is *expected* to be re-fetched as the owner publishes new snapshots; the basis snapshot ULID is part of its state and advances over time.
- **Listing.** `volume list` should show fetched volumes — a user that ran `volume fetch nginx-prod` expects to see it in the inventory, with a marker that it's foreign and pre-positioned. A bare ancestor in `by_id/` shouldn't surface there.
- **Claim interaction.** When a fetched volume's name later becomes claimable (owner releases), `volume claim <name>` should detect the local fetched copy and reuse it as the basis for the new fork — same role as the in-place reclaim path uses today, but starting from `Fetched` rather than from `Stopped` ownership.

Sketch of the state surface (details to be settled):

- A `Fetched` state distinct from `Live` / `Stopped` / `Released` / `Readonly-ancestor`.
- An on-disk marker that makes the state inspectable via `ls` (per the project's "directories encode lifecycle state" rule). Likely a `volume.fetched` marker file in `by_id/<vol_ulid>/`, recording at minimum the basis `snap_ulid`, the owner's coordinator id at fetch time, and the fetch timestamp.
- A `by_name/<name>` symlink for fetched volumes, parallel to the owned layout, so `<name>` is the user-facing handle for fetch / refetch / remove. Resolution rules need to spell out what happens if the same name is later claimed locally (the claim presumably retargets the symlink at the freshly-minted fork's `by_id/` and demotes the prior fetched copy to a readonly ancestor of that fork).
- `volume list` surfaces the `Fetched` state with the basis snapshot ULID and last fetch time.
- `volume remove <name>` works on a `Fetched` volume the same way it works on a stopped owned one — it's local-only state.

## Pipeline

The implementation reuses existing primitives end-to-end:

1. **Resolve.** `<name>` → `vol_ulid` via `names/<name>`.
2. **Find basis.** `latest_snapshot_op(vol_ulid)` (already in `fork.rs:546`). Fail with a clear error if `None`.
3. **Pull ancestor chain.** Walk parents via `pull_readonly_op`, mirroring `Fork::pull_chain` (`fork.rs:300`). Stop when an ancestor is already local. No peer-fetch tier (the existing fork path's reasoning applies — no `names/<name>` rebind to anchor against).
4. **Pull and verify the manifest.** Fetch `by_id/<vol_ulid>/snapshots/<snap>.manifest`, verify under the ancestor's `volume.pub`, and write `index/<seg>.idx` for each entry the manifest enumerates. This pulls only the segments the owner attested at snapshot time — by definition the right set.
5. **Open readonly.** `Volume::open` against `by_id/<vol_ulid>/` in readonly mode (`volume/readonly.rs:47`). The existing open path rebuilds `lba_map` and `extent_index` from the now-populated `index/`.
6. **Warm bodies.** `full_warm::spawn` with an S3-direct fetcher. The existing pool walks `lba_map.lba_referenced_hashes() ∩ ExtentIndex`, skips `BodySource::Local`, filters per-segment `.present` bitsets, and pulls every cold live entry from S3 in worker threads.

No new daemon mode, no new IPC verb on the volume side, no new fetch primitive. The coordinator drives steps 1–5 directly and hands the assembled state to `full_warm::spawn` for step 6.

## Properties

**Incremental.** Re-running `volume fetch <name>` later, after the owner has published a newer snapshot, naturally pulls only the delta:

- Step 4 writes any new `.idx` entries the new manifest references; existing ones are unchanged.
- Step 5 rebuilds `lba_map`/`extent_index` against the now-larger segment set.
- Step 6's `full_warm` enumerates the new live extents, finds the previously-fetched segments already present (via `cache/<id>.present`), and only fetches the segments that are new to this snapshot.

GC nuance: segments superseded by GC between snapshots disappear from the new manifest. Their cache bodies remain locally as unreferenced entries — harmless, just unreclaimed space, and standard cache eviction handles them. The live readonly view is always derived from the current manifest.

**No ownership change.** The volume's `names/<name>` record is unaffected. The local layout under `by_id/<vol_ulid>/` shares its on-disk shape with a readonly ancestor (index + cache, no `volume.key`, no fork) but is tagged with the `Fetched` state marker (see "State" above). A subsequent `volume claim <name>` (after the owner releases) detects the fetched copy and reuses it as the in-place basis for the freshly-minted fork.

**Bounded claim-time gap.** Post-snapshot segments — those promoted to S3 after the manifest was signed — are not fetched by `volume fetch`. They are exactly the set a later claim has to pull. With a recent snapshot basis, this set is bounded by the owner's write rate × snapshot cadence.

**No new trust assumption.** Every byte fetched is verified under the owner's signing key, exactly as the existing readonly-fork path does. The fetch is opaque to the live owner; no S3 writes occur.

## What `volume fetch` is not

- **Not a fork.** `volume create --from <name>` mints a writable fork descending from the latest snapshot. `volume fetch` does not — it leaves the local layout in the readonly-ancestor shape. A later `volume create --from <name>` benefits from the warm cache but is a separate verb.
- **Not a claim.** Claiming requires the bucket-side state machine (`Released` → CAS rebind). `volume fetch` is read-only against the bucket.
- **Not a snapshot.** No new snapshot or manifest is written, locally or remotely. The owner remains the sole publisher.
- **Not a "now" hydrate.** Post-snapshot segments are not fetched. This is intentional; see "Basis" above.

## Open questions

- **Concurrency with the owner.** The owner may publish a new snapshot, GC, or repack while `volume fetch` is running. The basis manifest pins us to a frozen segment set: GC's snapshot-floor rule means every segment referenced by a published snapshot is reaper-stable until that snapshot is itself retired, so a concurrent GC pass cannot delete a segment under a basis we hold. A mid-run owner publishing a *newer* snapshot is also benign — we stay on the manifest we captured; the new one is picked up on the next `volume fetch` run. The only real concurrency hazard is the owner retiring our basis snapshot itself (e.g. publishing a newer snapshot and then GC-collecting the old one's segments). That window is bounded by the owner's snapshot retention policy, and has the same shape as a stale-snapshot fork would.
- **Cancellation / progress.** `full_warm::spawn` runs detached. For an interactive `volume fetch` invocation, the CLI probably wants to block on completion and report progress. Whether to expose `full_warm` progress via the existing IPC envelope or add a thin progress channel is open.
- **Eviction interaction.** A host that runs `volume fetch` against many volumes will accumulate cache bodies. The existing eviction policy operates per-volume; cross-volume cache pressure on a hydration-heavy host hasn't been characterised.
- **Naming.** `volume fetch` reads cleanly against the existing `volume create --from` / `volume claim` / `volume pull` (internal) vocabulary. If "fetch" is too generic, alternatives include `volume warm` and `volume preload`. Settled here as `fetch` for parity with how the verb is described in user-facing docs ("fetch the remote volume").
- **Worker auth model.** The fetch worker resolves config via the unauthenticated `GetStoreConfig` IPC and reads `AWS_*` from env vars set by the coordinator at spawn time. It does *not* go through the volume daemon's PID-bound macaroon handshake — `register_volume` is keyed off `volume.pid`, which the worker doesn't own (and writing one would mis-classify the volume's lifecycle as `Running`). Trust derives from the coordinator-spawned process relationship rather than from a cryptographic binding. This is fine today (`SharedKeyPassthrough` issues non-expiring shared creds; warm passes are bounded), but two cases would force a revisit: short-lived per-volume IAM creds that could expire mid-warm (would need a refresh path — likely a fetch-worker-specific re-issue IPC, or FD-passing at spawn), and any future deployment where defence-in-depth against rogue local processes claiming to be fetch workers becomes load-bearing.
