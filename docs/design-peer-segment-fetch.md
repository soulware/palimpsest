# Peer segment fetch

**Status:** Exploration. No implementation. This doc captures the design discussion to date and the decisions that have landed; several pieces are intentionally left open.

**v1 scope: `.idx` + `.prefetch` only.** The first iteration fetches segment index files (`.idx`) and *prefetch hints* (`.prefetch`) from peers; body bytes are explicitly out of scope and continue to demand-fetch directly from S3. Both are small, full-file fetches (no range arithmetic), known up-front from ancestor walk, and exercise the entire stack: discovery, auth, transport, signature verification, S3 fallback. `.prefetch` is an opaque hint — "these are byte ranges worth warming" — used to drive background Range-GETs from S3 after claim. The peer happens to synthesise it from its local `.present` bitmap, but the wire resource is intentionally a different name from the on-disk file: clients consume it as advice, never confuse it with authoritative cache state, and the peer is free to evolve the encoding later. Whether to extend to peer body fetch is a downstream decision informed by what v1 surfaces. The body-fetch design is sketched below for context but is **not** v1.

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
- `cache/<ulid>.present` — per-entry presence bitmap (local cache state, authoritative for *this* host)

v1 wire resources:

- `<ulid>.idx` — served verbatim from `index/<ulid>.idx`
- `<ulid>.prefetch` — *prefetch hint*, derived from the peer's local `.present` (v1 returns its bytes as-is, but the format is the wire's, not the on-disk file's, and is free to evolve)
- `<snap>.manifest` — signed handoff manifest from `snapshots/<snap>.manifest`. The manifest's existence under `snapshots/` (locally and in S3) is the snapshot's existence — there is no separate empty-marker file. Caller verifies signature using the volume's `volume.pub`, same as on the S3 path.
Filemaps (`snapshots/<snap>.filemap`) are deliberately **not** part of the peer-fetch surface. They're consumed only by the import path's cross-volume dedup (`elide volume import --extents-from`, via `delta_compute`), which reads them from local disk and tolerates absence by skipping that source. Operators wanting cross-volume dedup against a remote source run `elide volume generate-filemap <source>` locally to build one from the source volume's ext4 layout. Filemaps are also intentionally absent for handoff snapshots produced by `release` (no ext4 layout to derive from). Exposing them on the peer-fetch wire would invite speculative prefetch on every claim against an artifact with no claim-path consumer.
- `volume.pub` — per-fork Ed25519 verifying key from `by_id/<vol_id>/volume.pub`; pulled by the requester's ancestor-skeleton walk before any segment fetch can verify a signature for that fork
- `volume.provenance` — per-fork signed lineage from `by_id/<vol_id>/volume.provenance`; the requester verifies the signature against the pubkey it trusts (embedded in the child's `ParentRef` for fork-chain ancestors, or the just-pulled `volume.pub` for extent-index ancestors)
- `.body` — deferred; not in v1

The skeleton-class routes (`volume.pub`, `volume.provenance`, `<snap>.manifest`) close out the last category of S3 traffic on the claim path. With `manifest.toml` removed, the per-fork S3 footprint is exactly these three artifact families plus `segments/` — all of which are now peer-eligible. All three skeleton-class routes share an auth-mode relaxation that lets a claim-time chain walk authenticate before the requester's own `volume.provenance` is published; see "Route auth modes" below.

The wire-level `.prefetch` name is deliberate: it tells the client "this is advice about what to warm," not "this is authoritative cache state." The new host's own `cache/<ulid>.present` is built from its own fetches, not copied from the peer.

`pending/<ulid>` is **not** peer-eligible. Pending segments are not yet on S3, so a peer fetch that fell through to S3 fallback would fail. Only S3-promoted segments are served.

The peer exposes per-segment data, not per-volume bundles. The same segment ULID is reachable via any volume that includes it in its ancestor chain, so per-segment is the natural unit; B's local state already encodes which of those segments it has, and B drives the fetch loop.

## Wire shape: per-segment GETs keyed by `(vol_id, ulid)`

The peer's URL space is intentionally narrower than S3's. S3 paths embed `by_id/<fork>/segments/<date>/<ulid>` because the date partition is an S3-side optimisation; the peer has its own local layout (`index/<ulid>.idx`, `cache/<ulid>.{body,present}`) and doesn't need the S3 prefix to find files. Stripping the noise gives:

```
GET /v1/<vol_id>/<ulid>.idx
GET /v1/<vol_id>/<ulid>.prefetch
GET /v1/<vol_id>/<snap>.manifest
GET /v1/<vol_id>/volume.pub
GET /v1/<vol_id>/volume.provenance
Authorization: Bearer <token>
```

`vol_id` is the fork that owns the segment, snapshot, or skeleton file (the *home* volume, which may be an ancestor of the volume the requesting coordinator currently claims). The second URL component is a *segment* ULID for `.idx` / `.prefetch`, a *snapshot* ULID for `.manifest`, and a literal filename for `volume.pub` / `volume.provenance`. The lineage check (step 4 in the verify pipeline below) only runs for payload routes (`.idx` / `.prefetch`); skeleton-class routes skip it — see "Route auth modes" — and which-flavour membership falls out of step 5 (local file exists). The `/v1/` prefix reserves room for protocol evolution.

For the snapshot route, the coordinator's claim-time prefetch can also skip the S3 LIST entirely. The manifest name is deterministic from the branch-point snapshot ULID, so a known-branch prefetch issues a single peer GET and falls back to a keyed S3 GET (using the canonical `by_id/<vol>/snapshots/<YYYYMMDD>/<snap>.manifest` path) on a peer miss. The S3 LIST stays in the listed-path branch (no known branch, e.g. when prefetching a fork's own accumulated snapshots) and as a fallback for any peer-less callers.

On a hit, the peer returns 200 with bytes; on a miss (file not present locally), 404; on auth failure, 401 or 403 (see auth pipeline below). The caller falls back to S3 for any non-200 response. The peer is a pull-through cache; semantics are unchanged from the previous "mirror S3 paths" sketch — only the URL shape is simpler.

**No batch endpoint.** Concurrency comes from HTTP/2 multiplexing — many parallel GETs on one kept-alive connection. The multiplexing layer handles fan-out, partial failures stay per-request (each GET succeeds or 404s independently), and a future body-fetch route slots in as `GET /v1/<vol_id>/<ulid>.body` with a `Range:` header — same shape.

### `.prefetch` is a hint, not authoritative state

The peer's local `.present` is per-host state: it reflects what *that host* has populated locally. Without something like it, the new coordinator only knows which LBAs are *mappable* (everything in the extent index after walking ancestor `.idx`s) — which for typical workloads is an order of magnitude larger than the working set actually touched (~2 GB image vs. ~130 MB boot working set per `findings.md`).

The wire `.prefetch` resource exposes that information as advice. v1 returns the peer's `.present` bytes verbatim because they happen to encode exactly the right set, but the wire name is deliberately different to keep three distinct things from collapsing:

| | What it is | Trusted? |
|---|---|---|
| Peer's local `cache/<ulid>.present` | Authoritative cache state for the peer | By the peer, for its own reads |
| Wire `<ulid>.prefetch` response | Advice: "these byte ranges are worth warming" | No |
| New host's local `cache/<ulid>.present` | Authoritative cache state for the new host, built from its own fetches | By the new host, for its own reads |

It's not a perfect signal:

- Whole-segment fetches mark more bits than the guest touched.
- Body-cache eviction can clear bits even though the guest accessed them earlier.
- Speculative prefetch can mark bits the guest never read.

But it is workload-shaped, free (already exists in a usable form on the peer), and a strict superset of "guest accesses that survived in cache" — i.e. it captures the warm working set. v1 uses it solely to drive **background byte-range prefetch from S3** after `volume claim`: parse the hint, issue Range-GETs to S3 for the indicated bytes, populate the new host's `cache/<ulid>.body`. The new host's `cache/<ulid>.present` is then built from the bits it actually fetched — never from the wire response.

If the peer has evicted body cache (so its `.present` is all-zeros or absent), the `.prefetch` response is empty or 404; the hint degrades to "demand-fetch only" — same as no peer at all. That's a graceful floor, not a failure.

### Index + prefetch-hint fetch (v1)

The `.idx` files are small (~10–50 KB) and `.prefetch` responses are smaller still (one bit per chunk on the wire). B's cold-start prefetch already enumerates the full set of ULIDs it needs by walking each ancestor's lineage. The request set is fully known before fetching:

```
For each reachable segment ULID not already on disk, in parallel on one HTTP/2 connection:
  GET /v1/<vol_id>/<ulid>.idx
    - 200 → write to index/<ulid>.idx; verify signature
    - 404 → fall back to S3
  GET /v1/<vol_id>/<ulid>.prefetch
    - 200 → hold response in memory as warming hint; enqueue background Range-GETs to S3 for the indicated bytes
    - 404 → no warming hint for this segment (peer has no body cache); demand-fetch only
```

Both are full-file fetches — `.idx` is the whole on-disk file; `.prefetch` is the whole hint payload (which v1 happens to derive from the peer's `.present` verbatim). A "miss" on the peer is a `stat` of the relevant local file; no I/O on miss, one read per hit.

Signature verification still happens caller-side per `.idx`, exactly as on the S3 path. Bytes from a peer are not trusted past byte level — a tampering peer is caught at verification and the fetch retries from S3. `.prefetch` is never trusted as authoritative; it's a hint that drives subsequent S3 Range-GETs whose results *are* verified, and the new host's local `.present` reflects only those verified fetches.

#### What v1 should surface

The point of shipping `.idx`-only first is to learn whether the peer tier is worth extending. Useful signals:

- **Hit rate** at the peer, both for handoff (one specific peer) and image-pull (any peer with the bytes). Below some threshold the entire mechanism isn't paying for itself.
- **Latency improvement** vs direct S3 for the cold-start prefetch phase, where `.idx` fetch dominates wall-clock time.
- **Operational cost.** Did discovery work? Did auth survive real claim/release/force-release sequences? Did fallback to S3 collapse cleanly on every error path?

If those signals are weak, body fetch is unlikely to justify its complexity. If they're strong, body fetch is the natural extension and is sketched below.

### Body fetch (deferred — not v1)

Once `.idx` + `.prefetch` fetch is validated, the body-fetch extension uses the same peer-then-S3 tier with `Range:` covering the body-section bytes the read needs:

```
GET /v1/<vol_id>/<ulid>.body
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

The peer's verify pipeline is five checks, each tied to a property the auth model must establish:

| # | Check | Source | What it proves |
|---|---|---|---|
| 1 | Decode + freshness | token bytes, local clock | replay defence (±60 s window) |
| 2 | Ed25519 signature valid against `coordinator.pub` | `coordinators/<token.coordinator_id>/coordinator.pub` from S3 | which coordinator is requesting |
| 3 | `names/<token.volume_name>.coordinator_id == token.coordinator_id` | `names/<token.volume_name>` from S3 (ETag-conditional) | the requesting coordinator currently owns/claims this volume |
| 4 | URL `vol_id` ∈ ancestry(`token.volume_name`) | walk `volume.provenance` chain from S3 starting at `names/<volume_name>.vol_ulid` | `vol_id` is in the requesting volume's signed lineage |
| 5 | `index/<ulid>.idx` (or `cache/<ulid>.present`) exists locally under `vol_id` | local fs | `ulid` is a segment of `vol_id` (implicit — falls out as 404) |

Failures map to status codes: 1–3 fail → 401 (bad credentials); 4 fails → 403 (out of authorised lineage); 5 fails → 404. Distinguishing 403 from 404 is fine here — there is no information leak (the requester has the lineage walk in hand and could derive the answer themselves).

#### Route auth modes

Step 4 (lineage walk) only runs for **payload routes**: `.idx`, `.prefetch`. The **skeleton-class routes** — `volume.pub`, `volume.provenance`, `<snap>.manifest` — declare `RouteAuthMode::SkeletonsOnly` and skip step 4. They still run steps 1–3 (token integrity + freshness + signature + `names/<volume>` ownership) and step 5 (local file exists).

The reason is that step 4 is *intent scoping* — "this requester actually has business with the URL's `vol_id`" — not confidentiality. Within the bucket trust boundary every authenticated coordinator already has S3 read access to every key, so the lineage gate isn't keeping anything secret; it's filtering against accidental over-fetch. The skeleton-class artifacts are signed bytes the caller verifies before trusting (Ed25519 verifying key, signed lineage record, signed snapshot manifest), so peer tampering is detected even without the gate.

Relaxing those routes is what makes claim-time chain walks work end-to-end. The new fork's `volume.provenance` isn't published until `finalize_claim_fork` — and that finalisation needs the verified parent, which needs `skip_empty_intermediates` to read each ancestor's signed manifest, which under `LineageGated` would itself need the new fork's provenance. Without `SkeletonsOnly` the loop would never start; the chain walk would 401 every request and silently fall through to S3 on the very path peer-fetch was added to accelerate.

DoS pressure on the now-broader skeleton surface is the responsibility of the [`RateLimiter`] hook, which is consulted before the cache regardless of mode.

The coordinator-key model has two operational benefits over per-volume signing:

- **No IPC for token minting.** The coordinator has its own key in memory; signing is in-process. Per-volume signing would require a coord → volume IPC round-trip per token (or per token-cache-miss).
- **One token per coord can fan out across multiple volumes.** A single coordinator can re-use the same auth pattern to fetch indexes for any volume it claims, since the auth check is "is the requesting coord the current claimer?" — naturally true for every volume the coord owns.

#### Caching profile

Each check has its own staleness shape, and v1 ships with all three caches active:

- **`coordinator.pub`** (check 2) — immutable per `coordinator_id`. Cache forever; rotate is a separate operational event.
- **`names/<volume_name>`** (check 3) — ETag-conditional GET. The peer holds `(NameRecord, ETag)` per volume name; every cache-miss request fires `If-None-Match: <etag>`. A 304 confirms the cached value (no body transferred); a 200 ships the new value and the cache updates. The auth fence coincides exactly with the S3 CAS that defines current ownership.
- **`ancestry(volume_name)`** (check 4) — derived from `volume.provenance`, which is **immutable once the volume exists**. Forking creates new volumes; it does not alter the ancestry of existing ones. Cache forever per `volume_name`, never invalidate.
- **Local file existence** (check 5) — `stat`, basically free.

On top of those per-check caches, the resolved [`Authorized`] outcome is also memoised, keyed on the bearer-token bytes plus URL `vol_id`, with a lifetime equal to the **token's residual freshness window**. Within that window, repeat requests with the same token + `vol_id` skip checks 3 and 4 entirely — no S3 round-trip even for the conditional `names/<name>` GET. A refreshed token (any coordinator re-mints in steady state every freshness-window interval) is a fresh cache miss and re-runs the full pipeline.

So a steady-state prefetch session looks like:

- **First request:** Ed25519 verify + 1 GET (`coordinator.pub`, ~once per coordinator ever) + 1 GET (`names/<name>`, full body) + N GETs (`volume.provenance` ancestry walk, once per volume ever) + local stat.
- **Subsequent requests on same token, different `vol_id`:** Ed25519 verify + cache hits for everything except lineage check (which uses cached ancestry) + local stat.
- **Subsequent requests on same token, same `vol_id`:** Ed25519 verify + resolved-Authorized cache hit + local stat. Zero S3 round-trips.
- **Token refresh:** like first request, but `coordinator.pub` and ancestry stay cached, so it's just one ETag-conditional `names/<name>` (likely 304) + local stat.

#### Auth-fence properties under caching

- **`coordinator.pub`** rotation: not modelled in v1 — keys never rotate; if compromised, the resolution is to mint a new coordinator. Cache-forever has no fence to violate.
- **`ancestry`**: provenance is immutable, so cache-forever has no fence to violate.
- **`names/<volume_name>`**: ETag revalidation makes the fence coincide with the S3 CAS — `release --force` flips the record, the next conditional GET returns 200 with the new value, the auth check fails on the next request that misses the resolved-Authorized cache.
- **Resolved-Authorized cache**: the only layer that introduces a fence gap. A force-released coordinator's already-issued token can serve cached requests for up to the token's residual freshness — at most `DEFAULT_FRESHNESS_WINDOW_SECS` (60 s in v1). This is bounded, scoped to read-only operations against bytes the holder already had access to, and incurs no write-fence violation (writes are gated by S3 IAM, which `release --force` revokes independently). The trade is acceptable in exchange for skipping per-request S3 round-trips during normal prefetch sessions.

#### Why no time-bounded cache for `names/<name>`

The `names/<name>` cache uses ETag-conditional revalidation, **not** a wall-clock TTL. The reason is that ETag revalidation keeps the auth fence coincident with the S3 CAS at the cost of one small round-trip per cache miss; a TTL would introduce an additional fence gap on top of the resolved-Authorized cache without any further perf benefit.

The resolved-Authorized cache *is* a time-bounded cache, but its bound is the token's freshness — not an arbitrary clock window — so it lines up naturally with the existing replay-window guarantee.

#### Anti-abuse properties

The auth model puts a verifiable `coordinator_id` on every request. That gives the peer a strong identity surface for behaviour-based defences without inventing new mechanism:

- **Per-coordinator request budgets.** A coordinator that requests more `(vol_id, ulid)` pairs than its claimed volumes' lineages contain is misbehaving — even if every individual request is auth-valid. Rate-limiting and temporary blacklisting key off `coordinator_id` directly.
- **Targeted blacklist.** A misbehaving or compromised coordinator can be denied at the peer without affecting any other coordinator on the LAN; the blacklist is a small set of `coordinator_id`s, locally configured.
- **Thundering-herd containment.** Only the current claimer of `volume_name` can issue valid tokens for it, and only against `volume_name`'s ancestry — so a single rogue actor can't trigger N peers to fetch arbitrary segment data on its behalf.

These are not v1 features (the simple version of v1 has no rate-limit at all), but the auth shape leaves them as cheap drop-ins later. They do not require any wire-format change.

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
- **Coordinator key compromise.** Anyone with `coordinator.key` can claim and operate volumes for that coordinator anyway; peer fetch does not widen this blast radius. Rotation is the same event as today.

### Future: converge on the credential-service format

The principal direction for any future evolution of the auth surface is **wire alignment with the eventual S3-cred service** (`docs/architecture.md` § S3 credential distribution via macaroons; `project_credential_service_scaling`). When that service exists, volumes will hold a single short-lived signed credential — likely a public-key macaroon or an equivalent caveat-based format — that conveys "you may read these S3 prefixes until time T." The natural move is to use exactly the same credential as the peer-fetch bearer: one library, one verification path, one mental model.

Concretely this means the v1 bearer-token format is **not** the long-term shape. The current token is deliberately minimal so the migration cost is bounded by the things that won't change:

- **Trust root** — verification against `coordinators/<id>/coordinator.pub` from S3 is the same in either format.
- **Lineage check** — moves from "peer walks ancestry" to "credential carries the prefix list as caveats," but the semantics are identical.
- **Freshness** — bounded validity moves from a custom `issued_at` field to a macaroon `time < T` caveat, but both express the same thing.
- **Token lifetime** is short (60 s), so the migration is cheap in operational terms — no on-disk persistence to deal with, no long-lived sessions to wait out. Old clients stop minting old tokens; new clients use the new format; peers accept both during a brief window.

The v1 bearer token therefore exists primarily to *defer* the format choice until the credential service is being designed, when the choice can be made jointly across both surfaces. Picking a format now would commit the credential service's design too, and there's no payoff in doing that before the service's other constraints (centralised vs federated issuance, third-party caveat structure, revocation strategy) are settled.

Until then, the v1 `PeerFetchToken` semantics are intentionally a strict subset of what a public-key macaroon could express:

| Bearer-token field | Macaroon equivalent |
|---|---|
| `coordinator_id`     | identifier (issuer) |
| `volume_name`        | first-party caveat: `volume = <name>` |
| `issued_at`          | first-party caveat: `time < <issued_at + window>` |
| signature            | macaroon Ed25519 root signature |
| (peer-side ancestry) | first-party caveat list: `prefix in [...]` (carried in credential, not derived on peer) |

So the v1 verification logic is the macaroon verification logic minus caveats the v1 token doesn't yet carry. The migration is "stop deriving caveats locally, start trusting them from the credential."

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

- **Peer body fetch.** Body bytes still go direct to S3 in v1. The `.prefetch` warming hint drives those S3 Range-GETs after claim, but the bytes themselves don't traverse the peer.
- **Release-time hint artifact.** An earlier draft proposed writing a per-segment populated-bitmap object to S3 at release time so B could drive proactive prefetch. Dropped in favour of fetching `.prefetch` directly from the peer at claim time — same hint quality, no new S3 artifact, no write-path step.
- **Cache retention policy on the source host.** A's `cache/<ulid>.{body,present}` may be evicted while B is still fetching. The peer's `.prefetch` response degrades gracefully (404 when the underlying state is gone; warming hint absent for that segment). Smarter retention (release-grace window, refcount across volumes) is a future improvement.
- **Per-coordinator rate-limiting / blacklist.** The auth model exposes `coordinator_id` on every request, so behaviour-based defences are cheap drop-ins later. v1 ships with no rate-limit; the LAN-only deployment model and the lineage check together bound the abuse surface enough to defer.
- **Multi-source peer fanout.** "Fetch from any of these N peers, take whoever responds first." Useful for image pull at scale; defer.
- **Multi-tenant peer.** A single peer process serving multiple buckets under different cred scopes. Out of scope until per-host coordinators serve more than one bucket.

## Open questions

1. **Hedging vs strict fall-through.** Should B try the peer first and fall back only on miss/timeout, or hedge-fire S3 after a short delay? Hedging has the best worst-case latency at the cost of duplicated request volume when the peer is slow. Probably not v1.
2. **Maximal-prefix on partial body coverage.** When body fetch lands, the proposal returns the longest contiguous covered prefix of the requested range. Worth empirical confirmation that this beats the alternatives (404 the whole thing; serve the largest covered sub-range regardless of position) for typical cache shapes.
3. **Discovery for image pull.** Out of scope until handoff lands, but worth a sketch eventually so the wire doesn't paint us into a corner.
4. **`.prefetch` payload shape on the wire.** v1 returns the peer's on-disk `.present` bytes as-is. A future protocol bump could compress, summarise (RLE of populated extents), or restrict to only the LBAs the requesting volume actually maps in its extent index. The payload is small enough today that none of this is likely to matter, but the wire/file decoupling means we can change the encoding without renaming the resource.
