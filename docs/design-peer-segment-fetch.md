# Peer segment fetch

**Status:** Exploration. No implementation. This doc captures the design discussion to date and the decisions that have landed; several pieces are intentionally left open.

**v1 scope: `.idx` only.** The first iteration fetches segment index files (`.idx`) from peers; body bytes are explicitly out of scope and continue to demand-fetch directly from S3. The `.idx` path is the simpler problem (small files, set known ahead of time, full-file semantics — no range/partial-coverage logic) and exercises the entire stack: discovery, auth, transport, signature verification, S3 fallback. Whether to extend to body fetch is a downstream decision informed by what v1 surfaces — peer hit rate, latency improvement vs direct S3, operational cost. The body-fetch design is sketched below for context but is **not** v1.

## Problem

Multiple Elide coordinators run on a LAN. Today every fetch path miss goes directly to S3. Two scenarios make this avoidable:

1. **Image pull at scale.** N hosts independently demand-fetch the same volume; each pays S3 egress for the same bytes. A peer that already has those bytes warm in `cache/` could serve them over LAN at a fraction of the cost and latency.
2. **Volume release → claim across hosts.** A volume migrates from host A to host B (release on A, claim on B). A's `cache/` *is* the working set B is about to demand-fetch; pulling it from A saves the S3 round-trip on every read until B's cache catches up.

Both reduce to "the bytes already exist on the LAN; don't pay S3 to copy them again."

The handoff case has stronger structural alignment with the existing fetch path (the peer is *known* — recorded in `names/<name>`) and is the more compelling driver. Image pull at scale is a second use case that the same primitive happens to serve.

## Framing: peer is a cache tier, not a coordination mechanism

S3 remains the source of truth. Peer fetch sits as an opportunistic tier in front of every existing S3 GET in the fetch path:

```
local check → peer (best-effort, short timeout) → S3
```

A peer miss, timeout, or unreachable peer collapses to today's behaviour. The peer never affects correctness, never holds cross-host state, never participates in a multi-host protocol beyond serving bytes it happens to have. This keeps the design consistent with the per-host coordinator model — cross-host coordination still rendezvous through S3; there is no coordinator-to-coordinator state machine.

## What's served

A volume's local post-upload layout is per-segment (see `docs/formats.md`):

- `index/<ulid>.idx` — header + index + inline; always retained
- `cache/<ulid>.body` — sparse body file; body-relative offsets
- `cache/<ulid>.present` — per-entry presence bitmap

The peer exposes these directly. A peer with `index/<ulid>.idx` locally can serve it; a peer with `cache/<ulid>.body` covered for a given range can serve that range.

`pending/<ulid>` is **not** peer-eligible. Pending segments are not yet on S3, so a peer fetch that fell through to S3 fallback would fail. Only S3-promoted segments are served.

The peer exposes per-segment data, not per-volume bundles. The same segment ULID is reachable via any volume that includes it in its ancestor chain, so per-segment is the natural unit; B's local state already encodes which of those segments it has, and B drives the fetch loop.

## Wire shape: GETs that mirror S3 paths

The peer's URL space is the same as S3's:

```
GET /by_id/<fork>/segments/<date>/<ulid>
[Range: bytes=<a>-<b>]
Authorization: Bearer <prefix-scoped token>
```

The caller asks the peer for the same path it would ask S3 for. On a hit, the peer returns 200/206 with bytes; on a miss, 404. The caller falls back to S3 for the same path. The peer is a literal pull-through cache, addressed identically to the upstream.

Concurrency comes from HTTP/2 multiplexing — many parallel GETs on a single kept-alive connection. No batch endpoint; the multiplexing layer handles fan-out.

The peer's local layout (`.idx` + sparse `.body` + `.present`) is not a single file like the S3 object. The endpoint synthesises the requested byte range from the local triplet: bytes in the index/inline section come from `.idx`, body-section bytes come from `.body` at the matching body-relative offset (subject to `.present`).

### Index fetch (v1)

The `.idx` files are small (~10–50 KB), and B's cold-start prefetch already enumerates the full set of ULIDs it needs by listing each ancestor's S3 prefix. The request set is fully known before fetching:

```
For each reachable segment ULID not already on disk:
  GET <peer>/by_id/<fork>/segments/<date>/<ulid>
  Range: bytes=0-<index_section_end>
  - 200/206 → write to index/<ulid>.idx; verify signature
  - 404      → fall back to S3 with the same path and Range
```

The index section ends at `100 + index_length + inline_length`, which is a fixed offset readable from the segment header — same range used by the S3 path's index-only retrieval (`docs/formats.md`).

A "miss" on the peer is a `stat` of `<vol_dir>/<fork>/index/<ulid>.idx`: no I/O on miss; one read per hit.

Signature verification still happens caller-side per `.idx`, exactly as on the S3 path. Bytes from a peer are not trusted past byte level — a tampering peer is caught at verification and the fetch retries from S3.

#### What v1 should surface

The point of shipping `.idx`-only first is to learn whether the peer tier is worth extending. Useful signals:

- **Hit rate** at the peer, both for handoff (one specific peer) and image-pull (any peer with the bytes). Below some threshold the entire mechanism isn't paying for itself.
- **Latency improvement** vs direct S3 for the cold-start prefetch phase, where `.idx` fetch dominates wall-clock time.
- **Operational cost.** Did discovery work? Did auth survive real claim/release/force-release sequences? Did fallback to S3 collapse cleanly on every error path?

If those signals are weak, body fetch is unlikely to justify its complexity. If they're strong, body fetch is the natural extension and is sketched below.

### Body fetch (deferred — not v1)

Once `.idx` fetch is validated, the body-fetch extension uses the same peer-then-S3 tier with `Range:` covering the body-section bytes the read needs:

```
GET <peer>/by_id/<fork>/segments/<date>/<ulid>
Range: bytes=<a>-<b>
```

Peer response semantics:

- **200 / 206** with the requested range, if fully covered in `cache/<ulid>.body`
- **206** with the *maximal contiguous prefix* of the range it has, if partially covered (uses `Content-Range: bytes <a>-<a'>/<total>`); caller picks up the rest from S3
- **404** if no body file or no bits covered in the requested range
- **416** if the range is invalid

No multi-range responses. A peer with gaps in the middle of the requested range serves only the prefix; S3 gets the rest, even if that means a small over-fetch. Multi-range handling is not worth the complexity for the expected win.

Signature verification on body bytes is per-extent against the signed `.idx`, same as on the S3 path.

The body-fetch path is genuinely more complex than `.idx` (range arithmetic, partial-coverage semantics, larger transfers, more failure modes during streaming). Validating `.idx` first ensures we only pay that complexity if v1 has demonstrated peer fetch is actually paying off.

## Auth: claim-derived bearer tokens, verified against S3

Per `docs/architecture.md` (§ S3 credential distribution via macaroons), each volume holds a short-lived read-only S3 credential scoped via IAM to its own S3 prefix and its ancestor prefixes. The peer **mirrors this scoping**: whatever a volume can read from S3, it can read from the peer; nothing more.

The trust root is the existing S3 state — `names/<name>` records the current claimer; per-fork keys (`volume.key` / `volume.pub`) and the signed `volume.provenance` chain establish ancestry. No separate credential service or pre-shared coordinator secret is required for v1; the peer verifies tokens against S3 alone.

### Token shape

**The fetching process signs.** For v1 (`.idx`-only), the prefetch path runs in the coordinator process, so the coordinator signs tokens with `coordinator.key`. If body fetch is added later it would run on the volume's read path in the volume process and sign with `volume.key`. Either way, peer-side verification looks up the appropriate pubkey in S3 (`coordinators/<id>/coordinator.pub` or `by_id/<fork>/volume.pub`) and the same prefix-membership check applies.

For v1:

```
PeerFetchToken {
  volume_name:    String,    // which names/<name> to look up
  coordinator_id: String,    // which coordinators/<id>/coordinator.pub to verify against
  issued_at:      u64,       // unix-seconds; bounds replay window
  signature:      [u8; 64],  // Ed25519 over the above, using coordinator.key
}
```

Carried in `Authorization: Bearer <base64(token)>`.

### Peer verification (v1)

1. Decode the token; check `issued_at` is within a freshness window (e.g. ±60 s).
2. Read `coordinators/<coordinator_id>/coordinator.pub` from S3. Verify the token signature against it. Mismatch → 401.
3. ETag-conditional GET `names/<volume_name>` from S3. Confirm the current claim record's `coordinator_id` matches the token's `coordinator_id`. Mismatch → 401.
4. Resolve the fork: `names/<volume_name>` → `vol_ulid`. Read `by_id/<vol_ulid>/volume.provenance` from S3. Walk the signed ancestor chain to produce the authorised prefix set: `by_id/<vol_ulid>/`, plus each ancestor's `by_id/<ancestor_ulid>/`.
5. For each requested path, check prefix membership. Out of scope → 404 (no leak about presence).

The coordinator-key model has two operational benefits over per-volume signing:

- **No IPC for token minting.** The coordinator has its own key in memory; signing is in-process. Per-volume signing would require a coord → volume IPC round-trip per token (or per token-cache-miss).
- **One token per coord can fan out across multiple volumes.** A single coordinator can re-use the same auth pattern to fetch indexes for any volume it claims, since the auth check is "is the requesting coord the current claimer?" — naturally true for every volume the coord owns.

**v1 does not cache the auth lookups.** Each peer request triggers fresh S3 GETs for steps 2–4. The cost is bounded:

- Most peer-fetch sessions open one HTTP/2 connection and fire N requests on it; the auth lookups run concurrently with serving local files, so the marginal wall-clock cost across the batch is roughly one S3 round-trip total, not N.
- Compared to the alternative — N missing `.idx` retrieved directly from S3 — even uncached peer auth is a clear win (N peer-RTTs plus ~1 amortised S3-RTT vs. N S3-RTTs).
- ETag / `If-None-Match` on the `names/<name>` GET trims bandwidth on subsequent lookups within a session; the `names/<name>` object is small enough that this is bonus.

The benefit of skipping the cache is that **the auth fence coincides exactly with the S3 CAS**: the moment a new claim lands, the peer's next request sees it. No TTL gap, no stale-claim window.

If auth lookups become a measurable bottleneck (e.g. very high request rates from many short-lived connections), the natural optimisation is **per-connection memoisation**: peer holds one auth result per `(connection, volume_name)` for that connection's lifetime. Connection close → fresh lookup. Simpler and tighter than a time-bounded TTL.

### Why this works

- **Trust root is S3.** `names/<name>` is the canonical claim record; signatures chain back to keys already established in S3. Same trust boundary the rest of Elide uses.
- **No shared secrets between hosts.** A and B do not need to pre-establish trust. A learns about B's fork from S3 the first time B asks; ETag-conditional GETs make subsequent checks cheap.
- **Handoff fence and auth fence coincide.** When B successfully CAS-claims `names/<name>`, A's next read of that key sees B as claimer. The S3 CAS that defines "B is now the holder" is also the moment B becomes peer-authorised. No separate "tell A about B" step.
- **Force-release fence and auth fence coincide.** This is the load-bearing property of the no-cache design. `release --force` exists to fence a split-brain old host by flipping `names/<name>` via CAS (see `docs/design-force-release-fencing.md`). Because every peer request re-validates `names/<name>` against S3 in the same round-trip as the fetch, the moment the CAS lands, the old fork's tokens stop authorising — no TTL gap, no stale-claim window during which a fenced-out host can still pull bytes from peers. Any time-bounded cache would have created exactly the gap `--force` is trying to close.
- **The "Released" interregnum is naturally safe.** Between `release --force` and the new `claim`, `names/<name>` records no current claimer, so no token's `coordinator_id` can match. Peer fetch is dormant during that window; clients fall through to S3 (which is the original behaviour anyway).
- **Image-pull case fits naturally.** Each fetching host's own fork has its own claim on its own name; the ancestor prefixes are reachable through that fork's signed ancestry. Same auth code path as handoff.
- **No new S3 artifacts.** Uses `names/<name>`, `volume.pub`, `volume.provenance` — all of which already exist with the right semantics.

### Caveats

- **Replay window.** `issued_at` ±60 s allows replay within that window. Strictly fine for read-only requests against signed bytes (worst case: replayed token retrieves bytes the original holder was already entitled to). Could tighten with a peer-issued challenge; probably overkill for v1.
- **Token issuer is the volume process directly.** The volume holds `volume.key` and signs the token itself. The coordinator does not need to be in the request path. If a coordinator-mediated path is wanted later for audit, the scheme still works — same key, same verification.
- **Future: macaroons / credential service.** When the credential service direction (`docs/architecture.md` § S3 credential distribution via macaroons; `project_credential_service_scaling`) is built out, peer auth can switch to macaroons with third-party caveats verified against the issuer. Wire shape (Bearer token, prefix membership check) is unchanged; only the token format and verification step change.

## Discovery: which peer to ask

| Use case | Discovery |
|---|---|
| Volume release/claim handoff | Look at the immediately previous event in `events/<name>/`. If it's a clean `released`, use the emitting coordinator as peer; otherwise fall back to S3. |
| Image pull at scale | TBD. No central peer registry exists today. Options include mDNS on the LAN, a `peers/<host>.json` rendezvous file in S3, or static config. Deferred until handoff lands. |

### Handoff discovery flow

`names/<name>` cannot help: in the `released` state the schema explicitly clears `coordinator_id`, `claimed_at`, and `hostname` (per `docs/design-portable-live-volume.md`). The previous owner is recorded in the volume event log (`docs/design-volume-event-log.md`), specifically the `released` event for that name.

**Peer-fetch discovery piggy-backs on the claim flow at zero extra cost.** A coordinator running `volume claim` already has to find the latest event in `events/<name>/` so it can populate `prev_event_ulid` on its own `claimed` event. By the time the claim CAS lands, that event is already in B's hands.

The claim sequence:

1. B reads `names/<name>` → sees `released`.
2. B locates the latest event in `events/<name>/` — needed for `prev_event_ulid` regardless of peer-fetch.
3. B does the conditional PUT on `names/<name>` → flips to `live`.
4. B emits its own `claimed` event referencing the prior event.

Peer-fetch then inspects the same event B already has:

- If `kind == "released"` and the signature verifies against `coordinators/<event.coordinator_id>/coordinator.pub`: the prior owner is identified. Resolve their endpoint via `coordinators/<event.coordinator_id>/peer-endpoint.toml` (proposed; see below) and use as peer.
- **Anything else** (`force_released`, missing event, signature failure, missing endpoint, unreachable peer, …): skip peer; fetch direct from S3.

The "anything-other-than-clean-released → S3" rule keeps the logic boring: one happy path, and every failure mode at every layer collapses to the same fallback.

For `force_released` specifically, the event's emitter is the *recovering* coordinator (the one that ran `--force`), not the prior owner. The whole point of `--force` is "the prior owner is gone and not coming back" — so even though the event identifies a coordinator, that coordinator has no relationship to whichever host had the volume's cache warm. Direct S3 is the only sensible fallback.

`prev_event_ulid` on each event chains backward through history; useful for verifying that no concurrent event was missed but not for *finding* the head of the log. Whatever mechanism the event-log design adopts for find-latest (LIST + sort by default, with `events/<name>/HEAD` pointer or inverted-ULID keys as future optimisations) is shared between claim and peer-fetch.

### Coordinator endpoint registry (proposed)

The handoff discovery step assumes a way to resolve `coordinator_id` → reachable endpoint. The natural slot is sibling to the already-proposed `coordinators/<coordinator_id>/coordinator.pub`:

```
coordinators/<coordinator_id>/coordinator.pub        — already proposed
coordinators/<coordinator_id>/peer-endpoint.toml     — new
```

`peer-endpoint.toml` records `host`, `port`, and any TLS hints. A coordinator updates its own endpoint at startup; absence or unreachability flows naturally into the "fall back to S3" branch above.

Embedding the endpoint in the `released` event itself is an alternative but ties endpoint information to volume history records and goes stale if the coordinator's address changes. The registry keeps the event log a record of *what happened*, separate from *where to reach a host today*.

## Out of scope for v1

- **Release-time hint artifact.** An earlier draft proposed writing a per-segment populated-bitmap object to S3 at release time so B could drive proactive prefetch. Dropped — adds a new artifact and a write-path step for an optimisation we can revisit once the on-demand peer-then-S3 path is real.
- **Cache retention policy on the source host.** A's `cache/<ulid>.{body,present}` may be evicted while B is still fetching. Fallback to S3 covers correctness; handoff transfer rate degrades if A evicts during the window. Smarter retention (release-grace window, refcount across volumes) is a future improvement.
- **Cache-bitmap query endpoint.** Without a release hint, B doesn't know what A has populated until B asks. A bitmap-query endpoint would let B plan ahead; not needed for the basic peer-then-S3 tier to work.
- **Multi-source peer fanout.** "Fetch from any of these N peers, take whoever responds first." Useful for image pull at scale; defer.
- **Multi-tenant peer.** A single peer process serving multiple buckets under different cred scopes. Out of scope until per-host coordinators serve more than one bucket.

## Open questions

1. **Hedging vs strict fall-through.** Should B try the peer first and fall back only on miss/timeout, or hedge-fire S3 after a short delay? Hedging has the best worst-case latency at the cost of duplicated request volume when the peer is slow. Probably not v1.
2. **Maximal-prefix on partial body coverage.** The proposal returns the longest contiguous covered prefix of the requested range. Worth empirical confirmation that this beats the alternatives (404 the whole thing; serve the largest covered sub-range regardless of position) for typical cache shapes.
3. **Discovery for image pull.** Out of scope until handoff lands, but worth a sketch eventually so the wire doesn't paint us into a corner.
