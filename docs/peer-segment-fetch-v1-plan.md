# Peer segment fetch — v1 implementation plan

Plan for landing the `.idx`-only first iteration of peer fetch. Design lives in [`design-peer-segment-fetch.md`](design-peer-segment-fetch.md); this doc sequences the work.

## Scope

**In:** Opportunistic peer-fetch tier in front of S3 for `.idx` files only. Coordinator-driven (matches existing `prefetch_indexes` flow). Single-peer discovery from the volume event log. Bearer-token auth signed by `coordinator.key`, verified entirely against S3.

**Out:** Body-byte fetch, image-pull discovery (mDNS / shared registry), TLS, multi-source peer fanout, source-host cache retention policy, release-time hint artifact. See the design doc for rationale.

## Settled decisions

- **Crate:** new `elide-peer-fetch` crate. Clean isolation; the surface (HTTP server, HTTP client, token type) is self-contained and shouldn't bloat `elide-coordinator`.
- **Token signing:** `coordinator.key`. The fetching process signs; for `.idx` that's the coordinator.
- **Transport:** plain HTTP/2 for v1. TLS deferred.
- **Coordinator config:** the peer-fetch port is **optional**. Absence ⇒ no peer-fetch server starts and the prefetch path skips the peer tier entirely. v1 ships off-by-default; opt-in per coordinator config.
- **Endpoint registry:** `coordinators/<coordinator_id>/peer-endpoint.toml`, sibling to the existing `coordinator.pub`. Written at coordinator startup when peer-fetch is configured; absent otherwise.

## Existing infrastructure (no work)

- `CoordinatorIdentity` (load/generate keypair, sign, publish/fetch `coordinator.pub`).
- Volume event log (`append_event`, `latest_event_ulid`, `list_and_verify_events`).
- `prefetch_indexes` in `elide-coordinator/src/prefetch.rs` — natural integration point.
- `volume.pub`, `volume.provenance`, signed ancestor walk.
- `names/<name>` schema with `coordinator_id` of current claimer.

## Work items

### 1. `elide-peer-fetch` crate scaffold

New workspace crate. Public surface:

- `PeerFetchToken` — struct with canonical signing payload; `sign(&CoordinatorIdentity)` and `verify(&VerifyingKey)`.
- `PeerFetchClient` — HTTP/2 client wrapper with token caching (~60 s validity).
- `PeerFetchServer` — HTTP/2 server with route handler and auth middleware.
- `PeerEndpoint` — endpoint-registry record (`peer-endpoint.toml`) with `read`/`write` against an `ObjectStore`.

Dependencies: `hyper` (HTTP/2), `ed25519-dalek` (already in tree via `elide-core`), `object_store`, `serde`/`toml`.

### 2. Endpoint registry

- `PeerEndpoint::write_to_store` — coordinator publishes `coordinators/<id>/peer-endpoint.toml` at startup.
- `PeerEndpoint::fetch_from_store` — read another coord's endpoint by id; returns `None` cleanly on absence.

Coordinator startup (in `elide-coordinator`): when peer-fetch is configured, call `write_to_store` after the existing `publish_pub` step.

### 3. Token type

`PeerFetchToken { volume_name, coordinator_id, issued_at, signature }`. Canonical signing payload: domain tag `"elide peer-fetch v1\0"` + sorted-key serialisation of the non-signature fields. Base64 encoding for `Authorization: Bearer …`.

Tests: round-trip sign/verify; tampered payload fails; expired `issued_at` rejected.

### 4. Peer-fetch HTTP server

Single route: `GET /by_id/<fork>/segments/<date>/<ulid>` with `Range:` support.

Server steps per request:
1. **Auth** (middleware): see item 5.
2. **Path → local file:** `<data_dir>/by_id/<fork>/index/<ulid>.idx`. Stat-only on miss → 404. Out-of-scope path (auth rejected) → 404.
3. **Range slice:** reject ranges outside the index section (`bytes=0-<header.index_section_end>`); for v1 we only serve `.idx` content. Body-section ranges → 404.
4. **Stream response:** open file, send the requested byte range with `Content-Range`.

Bind to the configured peer port; only start the server task if the port is configured.

### 5. Auth middleware (peer side)

For each request:

1. Extract bearer token from `Authorization` header. Decode + verify shape.
2. Check `issued_at` is within ±60 s of `now`.
3. Fetch `coordinators/<token.coordinator_id>/coordinator.pub` from S3 (cached per connection is fine; fresh per request is also fine for v1). Verify token signature.
4. ETag-conditional GET `names/<token.volume_name>` from S3. Confirm `name_record.coordinator_id == token.coordinator_id`. Mismatch → 401.
5. Resolve fork: `name_record.vol_ulid`. Read `by_id/<vol_ulid>/volume.provenance` (signature-verified against `volume.pub`). Walk ancestors → authorised prefix set: `by_id/<vol_ulid>/`, plus each ancestor's `by_id/<ancestor_id>/`.
6. Check requested URL path against prefix set. Out of scope → 404.

No persistent cache for v1 (per the design doc rationale around `release --force` fencing). Per-connection memoisation is the natural future optimisation if needed.

### 6. Peer-fetch client (caller side)

`PeerFetchClient::fetch_index_range(peer_endpoint, fork, ulid, range, token) -> Result<Option<Bytes>>`

- `Some(bytes)` on 200/206.
- `None` on 404 or any error (treated identically — caller falls through to S3).
- HTTP/2 connection pool keyed by peer endpoint; reuse across requests in the same prefetch run.
- Token cached by the client for ~60 s, re-minted on demand.

### 7. Discovery hook in claim flow

After the existing claim CAS in `volume claim` succeeds, the handler already loads the latest event in `events/<name>/` for the new `claimed` event's `prev_event_ulid`. Branch on it:

- `kind == "released"` + signature verifies + endpoint resolves → record `(coordinator_id, peer-endpoint)` against this volume's prefetch context.
- Anything else → no peer.

The "peer for this volume's prefetch" hint is held alongside the volume's other registration state, consumed once by the next prefetch tick, and discarded afterwards. (No persistent cross-tick state — fresh prefetches after the initial claim go peer-less.)

### 8. Prefetch integration

In `elide-coordinator/src/prefetch.rs`:

- Extend `prefetch_indexes` to take an optional peer-fetch context (`Option<(PeerFetchClient, PeerEndpoint, CoordinatorIdentity)>`).
- For each missing `.idx`, attempt peer fetch first (range = bytes 0..index_section_end). On `Some(bytes)`, verify signature, write to `index/<ulid>.idx`, done. On `None` or verification failure, fall through to the existing S3 path.
- Existing call sites (`tasks.rs:177`, `inbound.rs:1712`) pass `None` initially; the claim-discovery hook (item 7) passes a populated context for the volume just claimed.

### 9. Tests

- **Unit (`elide-peer-fetch`):** token round-trip; auth middleware happy/sad paths against a mock object store; path-prefix membership; Range parsing.
- **Integration:** spin two coordinators against a shared local object store. Coord A holds `vol-X` with hydrated `index/`. Coord B claims `vol-X` (after A releases); B's prefetch tick uses the claim hint and fetches `.idx` from A. Verify file count matches expectation, contents byte-identical to S3.
- **Failure modes:**
  - A's peer-fetch port disabled → B falls back to S3 cleanly.
  - A's coord crashed (endpoint unreachable) → fallback.
  - `force_released` instead of clean `released` → B skips peer, fetches from S3.
  - Token replay outside `issued_at` window → 401.
  - Caller asserts a `volume_name` they don't currently claim → 401.
- **Counters:** per-prefetch-run hit/miss/error counts; logged at info on completion. These are the signal for whether to extend to body fetch.

## Sequencing

1. **Item 1** (crate scaffold) and **item 3** (token type) first — small, no I/O, straightforward to test.
2. **Item 2** (endpoint registry) — short, no external dependencies.
3. **Items 4 + 5** (server + auth) together — testable with a mock object store before any client exists.
4. **Item 6** (client) — testable against the server from item 4.
5. **Items 7 + 8** (discovery + prefetch wire-up) — depends on the rest being usable.
6. **Item 9** (tests) — alongside each item; the integration test caps the work.

## Out of scope for v1 (re-stated, for clarity)

- Body-byte fetch.
- Image-pull discovery beyond "the previous releaser of this name".
- TLS / mTLS.
- Persistent peer-fetch hints across prefetch runs.
- Per-connection or time-bounded auth caching.
- Multi-tenant peer (peer serving multiple buckets under different scopes).

## Decision criteria for extending to body fetch

The point of shipping `.idx`-only first is to learn whether the mechanism is worth the additional complexity of body fetch. Look for:

- **Peer hit rate** for handoff specifically — is it reliably high when the predecessor coordinator is alive and reachable?
- **Latency improvement** for cold-claim prefetch — measurable reduction in time-to-first-read after `volume claim`.
- **Operational behaviour** through real `release` / `claim` / `release --force` sequences — does the auth fence hold cleanly under `--force`? Are there discovery races that surfaced?

If those are weak, body fetch likely isn't worth it. If they're strong, the body-fetch design (sketched in `design-peer-segment-fetch.md`) becomes the natural extension.
