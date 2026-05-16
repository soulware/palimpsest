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
  ->  parse {role, ttl_seconds}  ->  audience / Role / required-caveat
  gate  ->  TTL clamp (min(req, role.max, NotAfter-now))
  ->  render IAM policy template from caveats  ->  mint keypair
  ->  {access_key_id, secret_access_key, expiration}  +  audit line
```

- `caveat` / `macaroon` — generic **named** caveats (scalar **or**
  list-valued), chained-BLAKE3 MAC, base64 wire. Same construction as
  the elide coordinator's v2 macaroon, generalised per design open
  question #6. Not wire-compatible with the elide v2 typed format (by
  design — this prototypes the target generalised format).
- `config` — TOML: audience, single trust root, tenant, roles with TTL
  bounds and policy templates. Validated on load. The Tigris admin
  credential is **not** in the TOML — it is read from `AWS_ACCESS_KEY_ID`
  / `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`), the same
  convention as the elide coordinator.
- `role` — required-caveat gate, `Audience`/`Role` checks, TTL clamp.
- `template` — handlebars rendering of the IAM policy from
  `tenant.*` / `caveat.*` / `system.*`.
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

**Issuer model:** the design now has mint as *issuer and verifier* of
the primary macaroon (root never distributed), with delegation via
third-party caveats — see `docs/design-mint.md` § *Trust model* and
open questions #13–#15. The prototype predates that: it has no issuance
path and reads a `trust_root_hex` straight from the TOML purely so the
slice runs. That TOML root is a prototype shortcut, not the intended
provisioning (open question #14); issuance and third-party discharge
are unbuilt.

## Doc-reconciliation finding

`docs/design-mint.md` writes namespaced caveats as the data path
`{{caveat.elide:Volume}}`. Handlebars path segments cannot contain `:`,
so caveats are instead reached through a registered **`caveat` lookup
helper**: `{{caveat "elide:Volume"}}` and
`{{#each (caveat "elide:Ancestors")}}`. This keeps the doc's `:`
namespace convention unchanged (no issuer-side rename) and tightens the
"no policy DSL" property — the only template surface is `{{tenant.*}}`
/ `{{system.*}}` plain paths, the `caveat` helper, and `{{#each}}`.
The design doc should adopt the helper form for caveat references when
it next moves.

The helper resolves names against the **effective** caveat set
(`EffectiveCaveats::effective`): list caveats are intersected across
all chain occurrences, repeated scalars must agree, and a
self-contradictory scalar is treated as absent (template fails closed).
The minted policy therefore reflects exactly the authority the gate
evaluated — never a broader last-occurrence view.
