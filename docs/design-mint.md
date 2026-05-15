# mint: macaroon-authenticated credential vending for Tigris

## Status

**Proposed. Initial draft.** Supersedes `design-elide-mint.md` (PR #354).

The project name is TBD — "mint" is the working name in this draft. This will
become a **separate OSS project** distinct from Elide; the design doc lives in
`elide/docs/` during the design phase and will move to the project's own repo
once the shape is settled. Elide is the driving customer, but the design is
deliberately general-purpose for any Tigris consumer that needs scoped,
short-lived credential vending.

This doc builds on the macaroon construction in
[`design-auth-model.md`](design-auth-model.md) and replaces the on-host
sidecar shape proposed in `design-elide-mint.md`. The IAM-key inventory in
[`design-iam-key-model.md`](design-iam-key-model.md) collapses under this
design — see *Elide as customer* below.

## Why

Tigris has no STS — no native way to vend short-lived, narrowly-scoped
credentials. Consumers that want fine-grained access scoping today have to
either share a long-lived broadly-scoped credential across many actors, or
hold an admin-class credential locally and call `CreateAccessKey` themselves.

Both options are unacceptable for Elide and likely for many other Tigris
consumers:

- **Long-lived shared credential.** Defeats per-volume isolation; a single
  leaked key compromises every volume served from that bucket.
- **Local admin credential.** Tigris admin keys are org-global root (see
  `design-iam-key-model.md` § *Tigris admin keys are org-global root*). A
  compromise of any host holding admin yields full control of every bucket in
  the org. This is an unacceptable trust model for a multi-host fleet.

`mint` solves this by being a **standalone STS-shaped service for Tigris**:
holds the admin credential off-host, accepts macaroon-authenticated requests
from clients, mints scoped Tigris keypairs against pre-configured roles, and
returns them. Clients never see the admin credential; the credential plane is
strictly hierarchical.

The closest analogue in AWS terms is `AssumeRoleWithWebIdentity` plus session
tags — except the identity token is a macaroon (not a JWT), the variable
binding happens at issuance (because Tigris has no request-time variable
resolver), and the result is a real Tigris AccessKey/SecretKey pair (not a
signed session token, because Tigris has no session-token endpoint).

## Topology

```
   ┌──────────┐                  ┌──────┐                  ┌────────┐
   │ caller   │ ──── HTTPS ────▶ │ mint │ ─── Tigris IAM ▶ │ Tigris │
   │          │   macaroon-      │      │   admin creds    │        │
   │          │   authenticated  │      │                  │        │
   │          │ ◀── keypair ──── │      │ ◀── keypair ──── │        │
   └──────────┘                  └──────┘                  └────────┘
        │                                                       ▲
        │                  S3 data plane                        │
        └───── uses returned keypair against Tigris ────────────┘
```

The caller (e.g. an Elide coordinator) holds a macaroon issued by an authority
the mint trusts. It calls `mint`'s HTTP API, presenting the macaroon and a role
name. `mint` verifies the macaroon, looks up the role, renders the role's
policy template with values drawn from macaroon caveats, calls Tigris IAM to
mint a keypair under that policy, and returns the keypair to the caller. The
caller then uses the keypair directly against Tigris's S3 endpoint.

`mint` is **never** in the data path. It is consulted only at credential
issuance and refresh.

## Trust model

### Layers

```
caller ↔ mint:       macaroon authentication (HTTP + macaroon-as-bearer)
mint  ↔ Tigris IAM:  admin credential (held by mint, never disclosed)
caller ↔ Tigris S3:  the freshly-minted scoped keypair
```

The admin credential lives and dies inside the mint process. It never reaches
the caller. The macaroon root key lives inside the mint as well (for verifying
caller-presented macaroons) — but mint is *not* an issuer; some other authority
mints the macaroons, mint only verifies them.

### Mint configuration

Each mint instance is configured with:

1. **One or more macaroon trust roots** — the symmetric keys mint will accept
   as valid macaroon-signing authorities. Multi-root supports multiple issuers
   federating against the same mint (out of scope for v1; v1 supports a single
   trust root).
2. **One Tigris admin credential** (per backend), held in memory.
3. **A set of role definitions** — see *Role configuration* below.
4. **Tenant metadata** — bucket name(s), per-tenant settings. v1 is
   single-tenant per instance; multi-tenancy is a v2 question.

Configuration is static (file-backed) in v1; a config-management API is a
future direction.

### Admin credential custody — deployment shapes

The same mint code supports three deployment shapes:

1. **Self-hosted.** Operator runs the mint on a machine they trust (typically
   not the same host as any volume daemon). Configures the admin credential
   directly. Full control; no third-party dependency. The canonical OSS
   deployment.
2. **Central custodial** (Elide-managed offering). Elide runs a hosted mint
   instance; the operator's admin credential is held by Elide. Easier setup,
   meaningful trust handoff. Customer interacts via the closed-source web
   console.
3. **Central proxy** (Elide-managed, customer-key offering). Elide runs the
   mint, but the admin credential it uses is one the customer provisioned and
   vended to Elide central. Customer can rotate/revoke at any time
   independently. Compliance-oriented deployments choose this.

(2) and (3) differ only in whose Tigris account the admin credential is
issued against — the mint software is identical.

## Protocol

### Endpoint

```
POST /v1/assume-role
Host: <mint-instance>
Authorization: Macaroon <base64-encoded macaroon>
Content-Type: application/json

{
  "role": "volume-ro",
  "ttl_seconds": 3600
}
```

Response:

```
200 OK
Content-Type: application/json

{
  "access_key_id": "tid_...",
  "secret_access_key": "...",
  "expiration": "2026-05-15T14:30:00Z"
}
```

### Authentication

The `Authorization` header carries a single macaroon, base64-encoded. The mint
verifies its chain MAC against the configured trust root(s) (see
`design-auth-model.md` for the construction).

If verification fails — bad signature, unknown root, malformed encoding — the
mint returns `401 Unauthorized` with no further detail (don't help an attacker
distinguish "wrong key" from "tampered caveats").

### Request body

The request body specifies the **exercise of authority** — what the caller is
asking for right now within the bounds the macaroon attests to. v1 fields:

- `role` (required): role name from the mint's configuration.
- `ttl_seconds` (optional): requested credential lifetime. Must be within
  the role's `min_ttl_seconds`..`max_ttl_seconds` and must not exceed the
  macaroon's `NotAfter` caveat. Defaults to the role's `default_ttl_seconds`.

Future fields (Option 3 from design discussion — not in v1):

- `ancestors` (optional): subset of the macaroon's `Ancestors` caveat to
  include in the policy. Useful when a caller wants narrower-than-maximum
  authority for a specific operation. v1 always uses the full caveat list.

### Response

On success: the freshly-minted Tigris keypair plus its absolute expiration.

On role mismatch (caller asks for role not in config, or caveats don't
satisfy role requirements): `400 Bad Request` with a generic error.

On Tigris-side failure (rate limit, quota, admin credential rejection):
`503 Service Unavailable` with an error code indicating retry-ability.

Error model is deliberately coarse; see *Open questions*.

## Role configuration

### Schema

Roles are declared in a TOML config file (loaded at mint startup). Each role
has:

```toml
[[role]]
name = "volume-ro"
required_caveats = ["Volume", "Ancestors"]
min_ttl_seconds = 60
max_ttl_seconds = 604800     # 7 days
default_ttl_seconds = 86400  # 1 day

policy = """
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "ReadVolumeAndAncestors",
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": [
      "arn:aws:s3:::{{tenant.bucket}}/by_id/{{caveat.Volume}}/*"
      {{#each caveat.Ancestors}},
      "arn:aws:s3:::{{tenant.bucket}}/by_id/{{this}}/*"
      {{/each}}
    ],
    "Condition": {
      "DateLessThan": {"aws:CurrentTime": "{{system.expiry_iso8601}}"}
    }
  }]
}
"""
```

### Templating

The mint substitutes three classes of variable in the policy template at
issuance time:

- `{{tenant.X}}` — values from the mint's tenant configuration (bucket name,
  etc.). Server-side, never caller-controlled.
- `{{caveat.X}}` — values from the verified macaroon's caveats. Scalar
  caveats render directly; list-valued caveats are iterated with the
  `{{#each ...}}{{/each}}` construct.
- `{{system.X}}` — values computed by the mint at request time. v1 set:
  `system.expiry_iso8601` (the issued credential's expiry, derived from the
  requested or default TTL).

The mint **does not** ship a general-purpose policy DSL. Conditional blocks,
arithmetic, value transformations, and dynamic resource construction beyond
straight substitution are deliberately out of scope. Roles requiring more
expressive policies should be split into multiple roles.

### Required caveats

`required_caveats` declares which caveats the macaroon **must** carry for the
role to be assumed. If the macaroon lacks any required caveat, the request
fails before policy rendering. This catches malformed or wrong-audience
macaroons cleanly.

### TTL bounds

`min_ttl_seconds` / `max_ttl_seconds` / `default_ttl_seconds` bound the
credential's lifetime. The granted TTL is:

```
granted_ttl = min(
    requested_ttl_or_default,
    max_ttl_seconds,
    macaroon.NotAfter - now  // can't outlive the macaroon
)
```

`min_ttl_seconds` exists to reject silly requests (e.g. `ttl_seconds: 1`).

## Macaroon caveat conventions

The mint is **caveat-vocabulary-agnostic** — it doesn't hard-code which
caveat names are meaningful. Role configs reference whatever caveats they
need by name, and the macaroon issuer is responsible for putting the right
caveats in.

That said, several caveats are **conventional** across uses:

### Standard caveats

- **`Audience`** (string, scalar). Names the service the macaroon is intended
  for. Prevents a macaroon scoped for one service (e.g. coord-internal IPC)
  from being replayed at a different service (e.g. mint). Mint config
  declares its own audience name (e.g. `"mint"`) and rejects macaroons whose
  `Audience` caveat doesn't match.
- **`NotAfter`** (uint64 unix seconds, scalar, intersecting). Standard
  expiry. Multiple `NotAfter` caveats narrow to the minimum.
- **`Role`** (string, scalar — or list-valued if subsetting is wanted).
  Restricts which roles this macaroon can assume. If absent, any role the
  mint config exposes is reachable.

### Namespacing

Caveats other than the well-known standards above are conventionally prefixed
to indicate their issuer or domain:

- `elide:Volume`
- `elide:Ancestors`
- `elide:Coord`

This avoids collisions between issuers. Role templates reference caveats by
their full namespaced name: `{{caveat.elide:Volume}}`.

### List-valued caveats with intersection semantics

A list-valued caveat (e.g. `elide:Ancestors=[A,B,C]`) attenuates by
intersection: attaching another `elide:Ancestors=[A,B]` to the chain produces
an effective value of `[A,B]`. This preserves the "caveats only narrow"
invariant.

The macaroon library must understand list-valued caveats natively; see
`design-auth-model.md` for the encoding (TBD addition).

### Caveat field inventory (Elide)

The complete caveat vocabulary the Elide roles draw on. A caveat serves
one or both of two purposes: it **gates** authorization (listed in a
role's `required_caveats`) and/or it **feeds** the policy template
(`{{caveat.X}}` substitution). Some only gate.

| Caveat | Type | Scalar/List | Issuer | Purpose |
|---|---|---|---|---|
| `Audience` | string | scalar | macaroon issuer | Gate only — must equal `mint`. Cross-service replay defense. |
| `NotAfter` | uint64 (unix s) | scalar, intersecting | issuer | Gate — caps granted TTL (`min(req, role.max, NotAfter−now)`). |
| `Role` | string | scalar or list | issuer | Gate only — restricts assumable roles. Optional. |
| `elide:Coord` | string (coord-ulid) | scalar | coordinator identity | Gate on all `coord-*`. Templated only in the deferred one-time-publish split. |
| `elide:Volume` | string (vol-ulid) | scalar | coordinator | Gate **and** template — `by_id/{{caveat.elide:Volume}}/*`. |
| `elide:Ancestors` | list of vol-ulids | **list**, intersecting | coordinator | Gate **and** template — `{{#each}}` over ancestor ARNs. |

Per-role gate matrix (template substitutions are listed in each role's
definition below):

| Role | `Audience` | `NotAfter` | `elide:Coord` | `elide:Volume` | `elide:Ancestors` |
|---|---|---|---|---|---|
| `coord-data` | ● | ● | ● | ● | |
| `coord-names` | ● | ● | ● | | |
| `coord-events` | ● | ● | ● | | |
| `coord-identity` | ● | ● | ● | | |
| `coord-list` | ● | ● | ● | | |
| `coord-base` | ● | ● | ● | | |
| `volume-ro` | ● | ● | | ● | ● |

Non-caveat template inputs (the other two substitution classes, listed
here so the issuer's surface is unambiguous):

- `{{tenant.X}}` — server-side config; Elide uses `tenant.bucket`. Never
  caller-controlled.
- `{{system.X}}` — mint-computed at issuance; Elide uses
  `system.expiry_iso8601`.

Notes:

- **Exactly one list-valued field exists** (`elide:Ancestors`). Every
  other caveat is scalar. The list-valued caveat type (open question #6)
  is the only macaroon-library extension this inventory requires.
- **`elide:Coord` templates only in `coord-identity`**
  (`coordinators/{{caveat.elide:Coord}}/*`, own-prefix write). Every
  other `coord-*` role uses it as a gate only; their policies use
  prefix wildcards (`names/*`, `coordinators/*`, `events/*`).
- **`coord-base` is the read-only baseline every coordinator holds**, and
  the only credential the LAN/internet-exposed peer-fetch verifier holds.
  Coordinator-wide read of `names/*` / `coordinators/*` / `events/*`,
  gated by `elide:Coord` like the other `coord-*` roles.

## Elide as customer: role inventory

Elide's existing four-key model (`design-iam-key-model.md` § *Key classes*)
does **not** collapse to a single coordinator-wide writer under this design.
The monolithic `coord-writer` is split two ways:

- **By purpose** (Split A): the five-statement writer policy fragments into
  one role per top-level prefix, since they differ sharply in cadence, blast
  radius, and which IAM-layer invariant they must preserve.
- **By volume** (Split B): the `by_id/` data writer becomes *per-volume*,
  assumed with an `elide:Volume` caveat and cached coordinator-side per
  vol_ulid. This reopens `design-iam-key-model.md` § *Per-volume scoping for
  writes (rejected)* — see *Why Split B is viable now* below.

Orthogonally, `coord-base` is the read-only control-plane baseline every
coordinator holds (it is not a fragment of the writer policy).

### TTL principle

Mint does no active key deletion (§ *Cleanup*): a key lives until its
`DateLessThan` expiry. **TTL is therefore the maximum revocation latency.**
Two consequences shape every TTL below:

- Write/delete capability earns a *tighter* TTL than read-only — a leaked
  write key is strictly worse than a leaked read key for the same scope.
- Coordinator-held keys can take short TTLs: the coordinator is a
  long-running process that refreshes proactively on a timer, and writes
  buffer in the WAL if a refresh briefly stalls. The data-plane-held
  `volume-ro` cannot — a refresh stall there stalls guest I/O — so it trades
  a longer revocation window for refresh robustness, justified by it being
  the narrowest scope in the system.

### `coord-data` (Split B — per-volume)

Per-volume `by_id/` writer. Assumed by the coordinator the first time it
writes a given volume within a TTL window; the returned keypair is cached
in memory keyed by vol_ulid and re-assumed on miss/expiry. Structurally
identical to `volume-ro` but with write actions and a single (non-ancestor)
prefix.

- **Required caveats:** `elide:Coord`, `elide:Volume`, `Audience=mint`,
  `NotAfter`
- **TTL:** 24h default. Not on the hot write path (cache holds the key for
  the window; WAL absorbs a brief refresh stall), and 24h bounds the
  write/delete revocation window on a single volume.
- **Policy:** `s3:GetObject`/`s3:PutObject`/`s3:DeleteObject` on
  `arn:aws:s3:::{{tenant.bucket}}/by_id/{{caveat.elide:Volume}}/*`, single
  volume only.

GC and the reaper cross volume boundaries (read ancestor/input prefixes,
delete a consumed prefix). GC *input reads* compose by assuming `volume-ro`
for the inputs alongside `coord-data` for the output volume rather than
widening `coord-data`'s policy. (Reaper delete of a volume's own prefix is
covered by `coord-data` on that volume.)

### `coord-names` (Split A)

Coordinator-wide. Name claim / rename / force-release / rollback.

- **Required caveats:** `elide:Coord`, `Audience=mint`, `NotAfter`
- **TTL:** 1h. Control-plane, infrequent; refresh-on-demand is cheap.
- **Policy:** `s3:GetObject`/`s3:PutObject`/`s3:DeleteObject` on
  `arn:aws:s3:::{{tenant.bucket}}/names/*`.

### `coord-events` (Split A)

Coordinator-wide. Event-journal appends and reads.

- **Required caveats:** `elide:Coord`, `Audience=mint`, `NotAfter`
- **TTL:** 1h.
- **Policy:** `s3:GetObject`/`s3:PutObject` (**no** `s3:DeleteObject`) on
  `arn:aws:s3:::{{tenant.bucket}}/events/*`. The append-only invariant is
  enforced here at the IAM layer — no role in the inventory holds delete on
  `events/`.

### `coord-identity` (Split A — own-prefix only)

Writes this coordinator's own identity records: `coordinator.pub` and
`peer-endpoint.toml`. Scoped to **its own** `coordinators/<ulid>/`
prefix via `elide:Coord` templating — it cannot touch any other
coordinator's records. Peer identity/endpoint *reads* are not here;
they are covered by the read-only `coord-base` baseline.

- **Required caveats:** `elide:Coord`, `Audience=mint`, `NotAfter`
- **TTL:** 6h.
- **Policy:** `s3:GetObject`/`s3:PutObject` (**no** `s3:DeleteObject`) on
  `arn:aws:s3:::{{tenant.bucket}}/coordinators/{{caveat.elide:Coord}}/*`.
  Coordinator-identity immutability is enforced here — no role holds
  delete on `coordinators/`. A leaked `coord-identity` key can rewrite
  only its own coordinator's identity, not impersonate another.

### `coord-list` (Split A)

Coordinator-wide bucket enumeration: `volume list --remote` (LIST
`names/`), snapshot enumeration when the branch point is unknown (LIST
`by_id/<vol>/snapshots/`), event-log find-latest / peer-discovery (LIST
`events/<name>/`).

`s3:ListBucket` is irreducibly bucket-global on Tigris. AWS scopes it
to a prefix only via the `s3:prefix` condition key; Tigris supports
**no string condition keys** — only `IpAddress`/`NotIpAddress` and the
`Date*` family ([Tigris IAM policy support][tigris-iam-policies]). So
`coord-list` cannot be prefix-scoped or folded into per-volume
`coord-data`; it is the one structurally un-scopable role. Mitigation
is temporal only: short TTL, assumed on demand while enumerating.

- **Required caveats:** `elide:Coord`, `Audience=mint`, `NotAfter`
- **TTL:** 6h.
- **Policy:** `s3:ListBucket` on `arn:aws:s3:::{{tenant.bucket}}` (bucket
  resource, no object statement).

It only exposes object *keys* (ULIDs, names, coord ids), never object
contents. Eliminating LIST dependence — `events/<name>/HEAD` pointers,
deterministic manifest keys, a maintained `names` index — would shrink
or remove `coord-list`; tracked as open question #12.

### `volume-ro`

Per-volume, held by the volume process (vended via the macaroon handshake).
Narrowest scope in the system and the most refresh-sensitive holder.

- **Required caveats:** `elide:Volume`, `elide:Ancestors`, `Audience=mint`,
  `NotAfter`
- **TTL:** 30 days. Long deliberately: read-only, single volume + fixed
  ancestor list, held by long-running data-plane processes whose refresh
  path stalls guest I/O on failure. The 30d revocation window is the
  accepted cost of that refresh robustness, bounded by the minimal blast
  radius (read one volume's lineage).
- **Policy:** the per-volume RO shape, exact ARNs for self + each ancestor.

### Why Split B is viable now

`design-iam-key-model.md` § *Per-volume scoping for writes (rejected)*
rejected per-volume writer keys on two grounds. The mint redesign changes
one of them:

- *Confused-deputy enforcement is "modest"* — unchanged. Volume-identity
  correctness still lives in `volume_event_store` / claim records / the
  directory structure; per-volume IAM is still a redundant belt.
- *Operational cost* (N persisted policies, `ListPolicies` reconciliation,
  orphan reaping, refresh churn) — **dissolved**. Mint keys are short-lived,
  vended on demand, never persisted, expired by `DateLessThan`. No
  reconciliation, no orphans.

Per-volume **attribution** is obtained for free regardless of Split B —
every `AssumeRole` already logs the `elide:Volume` caveat (§ *Audit log*).
Split B's *additional* value over a coordinator-wide `coord-data` is
purely per-volume IAM *enforcement* (the "modest" confused-deputy catch).
The remaining cost is `AssumeRole` call volume: ~one mint round-trip per
active volume per TTL window per coordinator, gated by Tigris IAM rate
limits (*Open questions* #9). The 24h TTL is the primary knob: longer →
fewer mints, larger leaked-key window.

### `coord-base` (Split A — coordinator-wide, read-only baseline)

The baseline read-only credential every coordinator holds. Covers the
control-plane public state a coordinator reads as a matter of course:
name resolution and claim verification, peer-coordinator identity and
endpoint resolution, event-log and peer-discovery reads.

- **Required caveats:** `elide:Coord`, `Audience=mint`, `NotAfter` — the
  same coordinator-wide gate as the other `coord-*` roles.
- **TTL:** short (1h), like the other coordinator-held roles.
- **Policy:** `s3:GetObject` only, on `names/*`, `coordinators/*`, and
  `events/*`:

```
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "ControlPlaneReadOnly",
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": [
      "arn:aws:s3:::{{tenant.bucket}}/names/*",
      "arn:aws:s3:::{{tenant.bucket}}/coordinators/*",
      "arn:aws:s3:::{{tenant.bucket}}/events/*"
    ]
  }]
}
```

**Invariant: `coord-base` is read-only and `by_id/`-free.** This is what
makes it safe to be the *only* credential held by the LAN/internet-
exposed peer-fetch HTTP verifier: a compromise of the exposed surface
can neither mutate state nor read segment bodies
(`design-iam-key-model.md` § *IAM-layer invariants*). The write-capable
`coord-names` / `coord-identity` / `coord-events` roles stay separate
and are held only by the non-exposed mutation paths. `coord-base` must
never accrete a write action or any `by_id/` read; doing so silently
breaks exposed-surface containment.

The peer-fetch verifier needs no dedicated role and no `by_id/` access:
it uses `coord-base` for the gap-free fence (per-request ETag-
conditional `names/<name>` read, coincident with the `release --force`
S3 CAS) and the requester-pubkey check (`coordinators/<B>/
coordinator.pub`), and verifies lineage against the serving peer's
**own local** signed `volume.provenance` chain — see
`design-peer-segment-fetch.md` § *Peer verification* check 4.

The `ephemeral-fetch` key class from the prior model collapses into
`volume-ro` with a shorter TTL request. Operationally distinguishable via
audit log; same role config.

## Operational

### Deployment

The mint is a single static binary with one HTTPS listener and an outbound
Tigris IAM client. Reasonable hardware: small (single-core CPU, minimal
memory). Throughput-bounded by Tigris IAM API rate limits, not by mint
itself.

Standard production deployment: behind a TLS-terminating reverse proxy or
serving TLS directly, with the admin credential delivered via systemd
`LoadCredential=` or equivalent secrets-management.

### Audit log

Every `AssumeRole` call produces an audit entry. Minimal field set:

- `timestamp`
- `request_id` (uuid, surfaced to caller in `X-Request-Id`)
- `caller_address` (IP, for forensics)
- `macaroon_nonce` (per-token nonce from the macaroon)
- `macaroon_caveats` (sanitised — names + values, never secrets)
- `role`
- `granted_ttl_seconds`
- `outcome` (`granted` / `denied:<reason>` / `tigris_error:<code>`)
- `tigris_access_key_id` (if granted)

Audit log is local (file-based) in v1. Shipping to external sinks is an
operational concern, not a mint concern.

### Failure modes

- **Tigris IAM rate limit hit.** Mint returns `503` with `Retry-After`.
  Callers retry with backoff. Mint may internally smooth bursts via a token
  bucket if rate-limit pain emerges.
- **Tigris admin credential rejected.** Mint returns `503` and logs loudly;
  manual operator intervention required to refresh the admin credential.
- **Macaroon trust root rotation.** TBD — see *Open questions*.

### Cleanup

Tigris keypairs minted by the mint have `DateLessThan` policies, so they
expire automatically. Mint does **not** track issued keypairs to delete them
explicitly — that would require holding per-keypair state and trying to call
`DeleteAccessKey` on expiry, which is failure-prone and doesn't improve
security (the policy expiry already enforces the bound).

For operational visibility, the audit log records every issuance; operators
can correlate Tigris-side access key activity with mint audit entries.

## Open questions

These are genuinely unsettled — flagging them rather than committing
prematurely.

1. **Project name.** "mint" is the working name; not committed. Candidates:
   `tigris-mint`, `macaroon-iam-broker`, a fresh name. Decision needed before
   the project moves to its own repo.
2. **Multi-tenancy shape.** v1 is single-tenant-per-instance. Whether v2
   should support multi-tenant per instance (each tenant with its own trust
   root, admin credential, role set) or stay single-tenant with per-tenant
   deployments is open. Multi-tenant per instance is more useful for
   centralised offerings; single-tenant is structurally simpler.
3. **Trust root rotation.** Static trust roots fit v1, but rotation needs a
   story. Options: hot-reload on SIGHUP, dual-key acceptance during overlap,
   explicit rotation endpoint. Probably defer to v2.
4. **Peer-fetch scope — settled.** There is no dedicated peer-fetch
   role; the verifier uses `coord-base` (read-only `names/*` /
   `coordinators/*` / `events/*`). Lineage is verified by the serving
   peer against its own *local* signed `volume.provenance`, not via S3.
   The force-release fence is gap-free via the per-request ETag-
   conditional `names/<name>` read (fence coincident with the S3 CAS).
5. **Mid-path wildcard verification.** Not on the v1 critical path:
   `coord-data` uses a single-volume *trailing* wildcard
   (`by_id/{{caveat.elide:Volume}}/*`), `volume-ro` uses exact ancestor
   ARNs, and `coord-base` touches no `by_id/` at all — none need mid-path
   `*`. It is only a constraint on a future role wanting
   `by_id/*/<something>` shape. Empirical test still worth running once,
   but does not block the current inventory.
6. **Caveat library schema.** List-valued caveats with intersection
   semantics are required; `design-auth-model.md` documents only scalar
   caveats today. Needs extending — minor work, but the encoding needs to
   round-trip cleanly.
7. **HTTP API surface beyond `AssumeRole`.** Likely additions: `ListRoles`
   (caller discovers what's available), `GetRole` (caller introspects role
   requirements), health endpoint. None blocking for v1; design once
   real callers ask.
8. **Caller-side credential refresh.** Should mint return a refresh token,
   or should callers just re-call `AssumeRole` on expiry? STS does the
   latter; same answer probably right here. Worth being explicit.
9. **Tigris IAM rate-limit headroom — gates Split B.** This is no longer a
   "defer unless workload demands" item: per-volume `coord-data` (Split B)
   makes `AssumeRole` volume scale with active volumes — roughly one mint
   round-trip per active volume per TTL window per coordinator, each one a
   Tigris `CreatePolicy`+`CreateAccessKey`+`AttachUserPolicy` sequence.
   Tigris publishes no IAM rate limit. The 24h `coord-data` TTL is the
   primary knob (longer → fewer mints, larger leaked-key window); mint-side
   per-root rate limiting / burst smoothing may also be needed. Measuring
   Tigris IAM headroom at realistic volume counts is the gate before Split B
   is committed to implementation.
10. **What lives in the mint vs in the closed-source web console.** The
    mint is the credential plane. The web console handles user identity
    (SSO), org/tenant management, key custody UX, audit visualisation, and
    multi-coordinator dashboarding. The exact API boundary between them
    (does the console talk to mint over the same `/v1/assume-role`, or via
    a privileged management interface?) is TBD.
11. **GC / reaper cross-volume composition under per-volume `coord-data`.**
    `coord-data` is scoped to a single volume's `by_id/<vol>/*`. GC reads
    input/ancestor prefixes that belong to *other* volumes and the reaper
    deletes a fully-consumed volume's prefix. The sketched answer (GC input
    reads via a separately-assumed `volume-ro`; the output write and the
    reaper's own-prefix delete via `coord-data` on the target volume) is
    stated in the role inventory but not fully specified — the exact set of
    roles a GC pass assumes, and whether the reaper's delete wants its own
    narrower role, is open.
12. **Eliminate `coord-list`.** It is the one structurally un-scopable
    role (Tigris `ListBucket` is bucket-global; no string conditions to
    prefix-scope it). Replacing the LIST paths with `events/<name>/HEAD`
    pointers, deterministic manifest keys, and a maintained `names`
    index — ideas already floated in `design-volume-event-log.md` and
    `design-peer-segment-fetch.md` for performance — would shrink it to
    just `volume list --remote`, or remove it entirely. Not blocking;
    the temporal mitigation (short TTL, on-demand) holds until then.

## Future directions

These do not affect v1 but are anticipated extensions worth designing
around:

- **Third-party caveats** (from `design-auth-model.md` § *Future
  directions*). The mint's macaroon-verification path becomes the natural
  place to handle discharge bundles when third-party caveats are introduced.
  No protocol change needed beyond accepting discharge macaroons in the
  request; the chained-MAC construction already accommodates them.
- **Backend-agnostic roles.** The role config language doesn't assume Tigris
  specifically — it's IAM-policy-template-shaped. Other backends (native
  AWS, S3-compatibles with IAM) could be plugged in by swapping the
  Tigris-IAM-API client for an equivalent. Worth deciding before v1 whether
  to design the role config explicitly backend-agnostic or to keep it
  Tigris-specific and refactor later.
- **Federation across mint instances.** Multi-root trust support enables
  federation: one mint trusts macaroons issued by another's authority.
  Allows mint instances to chain (e.g. a regional mint trusting a global
  identity mint).
- **Replacing template rendering with request-time variables.** If Tigris
  ever ships request-time variable resolution (`${session.X}` in policies),
  the mint could store policies once with variables and resolve at request
  time rather than rendering per issuance. The role config schema would
  not need to change; only the renderer.
- **List-roles authorisation discovery.** Beyond a flat `ListRoles`,
  callers may want "which roles can this specific macaroon assume." The
  macaroon's caveats determine eligibility; computing the answer requires
  walking each role's required-caveats list. Cheap to compute, useful for
  UX in the web console.

## References

- [`design-auth-model.md`](design-auth-model.md) — macaroon construction
  shared with this design.
- [`design-iam-key-model.md`](design-iam-key-model.md) — Elide's IAM key
  inventory; the monolithic writer splits per-purpose and per-volume under
  this design (Split A + Split B).
- AWS STS docs: [`AssumeRoleWithWebIdentity`][assume-role-web-identity],
  [session tags][session-tags] — the closest AWS analogue for the
  identity-token-to-scoped-credential flow.
- Tigris IAM docs:
  [policy support](https://www.tigrisdata.com/docs/iam/policies/),
  [supported actions](https://www.tigrisdata.com/docs/iam/policies/supported-actions/).

[assume-role-web-identity]: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
[session-tags]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html
[tigris-iam-policies]: https://www.tigrisdata.com/docs/iam/policies/
