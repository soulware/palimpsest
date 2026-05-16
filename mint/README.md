# mint (prototype)

Macaroon-authenticated scoped-credential vending for Tigris. Tracks
[`docs/design-mint.md`](../docs/design-mint.md).

This is an **initial implementation to feel out the shape** — a runnable
vertical slice, not v1. It lives in the elide workspace during the
design phase and is deliberately free of `elide-*` dependencies; it is
destined to become a standalone OSS project.

## What works

The full request path runs end to end:

```
Authorization: Macaroon <b64>  ->  verify MAC against trust root
  ->  elide:CoordKey PoP: Ed25519 over BLAKE3(tail ‖ BLAKE3(body))
  ->  parse {role, ttl_seconds, ...}  ->  audience / Role /
  required-caveat gate  ->  TTL clamp (min(req, role.max, NotAfter-now))
  ->  render IAM policy from caveats + PoP-verified request.*
  ->  mint keypair
  ->  {access_key_id, secret_access_key, expiration}  +  audit line
```

- `caveat` / `macaroon` — named **scalar** caveats, chained-BLAKE3 MAC,
  base64 wire. Same construction as the elide coordinator's v2
  macaroon. There is no list-valued caveat type: `EffectiveCaveats`
  resolves a name tri-state — `Absent` / `Value` / `Unsatisfiable` —
  and ≥2 disagreeing occurrences are `Unsatisfiable` (fail closed,
  never silently `Absent`; this is the append-a-contradictory-copy
  downgrade defence). Not wire-compatible with the elide v2 typed
  format (by design).
- `pop` — the `elide:CoordKey` holder-of-key gate. A request bearing a
  key-bound macaroon must carry `X-Mint-Coord-Pop` (Ed25519 sig); the
  signature covers `tail ‖ BLAKE3(raw-body)`, binding it to the exact
  macaroon and the exact body. Freshness is a `ts` field *in the body*
  (unix seconds), already covered by the body hash — no `…-Ts` header;
  verified, then ±skew-checked (design OQ#16). No `elide:CoordKey` ⇒
  plain bearer (no PoP); contradictory ⇒ rejected. `client_signature`
  / `coord_key_value` are the reference client surface (what a
  coordinator does per request).
- `config` — TOML: audience, single trust root, tenant, roles with TTL
  bounds and policy templates. Validated on load. The Tigris admin
  credential is **not** in the TOML — it is read from `AWS_ACCESS_KEY_ID`
  / `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`), the same
  convention as the elide coordinator.
- `role` — required-caveat gate (present *and* satisfiable),
  `Audience`/`Role` checks, TTL clamp.
- `template` — handlebars rendering of the IAM policy from four
  provenance-distinct classes: `caveat.*` (MAC-bound), `request.*`
  (PoP-verified body, e.g. `request.ancestors`), `tenant.*` (config),
  `system.*` (mint-computed). Strict mode — a referenced-but-absent or
  unsatisfiable input fails the render closed.
- `iam` — `KeypairMinter` trait. Prototype ships `FakeMinter`
  (deterministic, records calls). A real Tigris minter wrapping
  `elide-tigris-iam` slots in unchanged.
- `audit` — one JSON line per call; secrets never logged (key id only).
- `http` — axum `POST /v1/assume-role` + `GET /healthz`, coarse
  401/400/503 error model, `X-Request-Id`.

## Run it

```sh
cargo run -p mint -- mint/examples/mint.toml 127.0.0.1:8085
```

Keypair minting is **faked** — no real Tigris call is made; the audit
log goes to stdout.

## Not yet (deliberately out of scope for this slice)

Real Tigris IAM client, TLS, multi-root / trust-root rotation,
multi-tenancy, `ListRoles`/`GetRole`, rate-limit smoothing, refresh
tokens. See `docs/design-mint.md` § *Open questions*.

**Issuance / enrollment:** mint is the *issuer and verifier* of the
primary macaroon (root never distributed). The verifier side and the
`elide:CoordKey` PoP are built; the *issuance* side is not — there is
no `elide coord register` / enrollment exchange and no third-party
caveat discharge (`docs/design-mint.md` open questions #13–#15). The
prototype reads `trust_root_hex` straight from the TOML purely so the
slice runs; that TOML root is a shortcut, not the intended provisioning
(open question #14).

**PoP wire:** the ±skew freshness anchor and the signed payload
(`tail ‖ BLAKE3(body)`, `ts` in-body) are settled in the design
(OQ#16); the encoding (`X-Mint-Coord-Pop` base64 Ed25519, body `ts`
unix seconds, 60s skew) is an implementation choice in `src/pop.rs`,
not a fixed protocol.
