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

The caller (e.g. an Elide coordinator) holds a macaroon **issued by the
mint itself** — minted once at enrollment, then attenuated by
the caller per request. The macaroon is a pure *capability* (which
roles this key-bound principal may assume, until when); the per-request
*exercise* parameters (role, TTL, and any role-specific scoping data
such as the ancestor set) travel in the request **body**, which is
covered by the caller's proof-of-possession signature (§ *Coordinator
bootstrap*). The caller calls `mint`'s HTTP API, presenting the
(attenuated) macaroon, the PoP-signed body, and any discharge
macaroons. `mint` verifies the macaroon against its own root and any
third-party caveats, verifies the PoP signature over the body against
the macaroon's `cnf`, looks up the role, renders the role's
policy template from the verified caveats and the PoP-verified body,
calls Tigris IAM to mint a keypair under that policy, and returns the
keypair to the caller. The caller then uses the keypair directly
against Tigris's S3 endpoint.

`mint` is **never** in the data path. It is consulted only at credential
issuance and refresh.

## Trust model

### Layers

```
caller ↔ mint:       capability macaroon (MAC, mint root) + per-request
                     Ed25519 PoP over macaroon-tail ‖ body (ts in body)
mint  ↔ Tigris IAM:  admin credential (held by mint, never disclosed)
caller ↔ Tigris S3:  the freshly-minted scoped keypair
```

**mint is both issuer and verifier of the primary macaroon.** The
symmetric macaroon root key lives and dies inside the mint and is never
distributed: mint mints a caller's primary once (at the enrollment
exchange — § *Coordinator bootstrap*), and verifies the attenuated
macaroon presented on every `assume-role`. Issuer and verifier being the same process is what
removes any root-distribution problem — there is no separate authority
to share the root with, and no "configure mint to trust the
coordinator's root" step.

The caller (e.g. a coordinator) is therefore **neither a macaroon issuer
nor a root holder**. It holds a macaroon and may only *attenuate* it
(append narrowing caveats — `exp`, a specific `elide:Volume`),
which needs the trailing MAC, never the root. A compromised caller can
only narrow authority it was already granted; it cannot forge authority
for another coordinator or volume.

Delegation to a *separate* authority — proving the caller's identity,
org membership, or SSO authentication — is **not** modelled as that
authority issuing the macaroon. It is a **third-party caveat**: mint
stamps "valid only if discharged by `<identity authority>` attesting
predicate P", and verifies the discharge against a key it shares with
that authority. The identity plane (who is this caller) and the
credential plane (what Tigris scope do they get) stay separate; the
managed login service discharges the caveat (a discharge authority, not
an issuer — the "login" is that discharge, not the registration verb).
See *Open questions* and *Future directions*.

The admin credential likewise lives and dies inside the mint process and
never reaches the caller.

### Mint configuration

Each mint instance is configured with:

1. **Its own root key** — the single symmetric key mint uses to
   *both* mint and verify primary macaroons (the "root key" of the
   Macaroons paper). It never leaves the process and is never shared
   with a caller or any other authority. It is a 32-byte CSPRNG key
   generated on first start at `<data_dir>/root_key` (64 hex chars,
   mode 0600), loaded thereafter — never a config field, mirroring the
   elide coordinator's `coordinator.key`.
   Symmetric, so there is no public half. (v1 is single-root; multi-root
   for federating issuers is out of scope.) The current **`bootstrap`**
   is persisted alongside the root under the same custody — it must
   survive restart so the distributed bootstrap macaroon stays valid;
   only `mint bootstrap rotate` changes it.
2. **Zero or more third-party discharge keys** — one symmetric key per
   identity/discharge authority mint trusts to satisfy a third-party
   caveat. Absent in the minimal self-hosted deployment (no third-party
   caveat); present when an identity authority such as the managed login
   service is in use.
3. **One Tigris admin credential** (per backend), held in memory. It is
   read from the standard AWS environment variables
   (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, optionally
   `AWS_SESSION_TOKEN`) — the same convention the elide coordinator uses
   for its IAM-mode admin credential — **not** from the config file. The
   credential is a secret delivered by the environment (systemd
   `LoadCredential=`, a secrets manager); keeping it out of the TOML
   keeps secrets and role definitions on separate management planes.
4. **A set of role definitions** — see *Role configuration* below.
5. **Tenant metadata** — bucket name(s), per-tenant settings. v1 is
   single-tenant per instance; multi-tenancy is a v2 question.

Role definitions, audience, and tenant metadata are static and
file-backed. The root key and admin credential are secrets and are
not plaintext TOML fields — the admin credential comes from the AWS
environment; the root key is generated on first start at
`<data_dir>/root_key` (64 hex chars, 0600) and loaded thereafter.

#### On-disk layout

A mint instance is named by its config file: `--config <path>`,
defaulting to `./mint.toml`. The config declares two optional
directories, mirroring the elide coordinator's `data_dir`
(`coordinator.toml`):

- **`data_dir`** (default `mint_data`) — persisted state under the same
  custody as the macaroon root: the current `bootstrap` value and the
  transient pending-enrollment table.
- **`roles_dir`** (default `mint_roles`) — role *policy templates*, one
  file per role (see *Role configuration*).

Both follow the coordinator's resolution rule: a relative value
(including the default) is resolved against the current working
directory, not the config file's parent; an absolute path is used
verbatim. Unlike the coordinator, mint has **no `--data-dir` override
flag** — a mint instance is fully described by its config file, so
running two instances is purely `mint.toml` + `mint2.toml` with distinct
`data_dir` values (and, if desired, a shared `roles_dir`). The override
flag would be unused surface; its absence is a decision, not an
oversight.

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

## Coordinator bootstrap & macaroon lifecycle

The **primary macaroon** is the mint root attenuated to exactly one
coordinator identity: `op=assume-role`, `aud=mint`,
`sub=<coord-ulid>`, `cnf=ed25519:<coordinator.pub>`,
no `exp`. It *is* that coordinator's identity within the credential
plane; every credential the coordinator vends is a further attenuation
of it. A coordinator holds it long-lived — persisted in `data_dir`
(mode 0600, alongside the identity key), loaded on every start, reused
across restarts. Per request and per managed volume it appends narrowing
caveats (`elide:Volume`, a tighter `exp`) before calling
`assume-role`; the stored macaroon is never sent unattenuated. **The
primary does not expire**: once PoP-bound a file-only leak is inert (the
thief lacks `coordinator.key`) and a key compromise renews regardless,
so there is no re-issuance cadence. The identity key is not rotated: a
new key is a new coordinator — new `coord-ulid`, new enrollment.

`sub` and `cnf` are partitioning caveats. A
coordinator self-asserts them only inside the enrollment exchange; a
primary carrying them exists only because mint re-minted it from root
after the operator vouched for the pairing (below). A coordinator can
never append them to an existing macaroon to widen authority — a
contradictory copy is unsatisfiable and fails closed.

The primary is **bound to the coordinator's Ed25519 identity** by the
`cnf` first-party holder-of-key caveat. `assume-role` honours
the macaroon only when the request carries a fresh Ed25519 signature, by
`coordinator.key`, over `BLAKE3(presented-macaroon-tail ‖
BLAKE3(request-body))` — the tail binds the proof to this exact
capability macaroon (role/`exp`/`elide:Volume`), the body hash to
this exact request (role, TTL, scoping data such as the ancestor set).
Freshness is a `ts` field **inside the body** (unix seconds, ±skew
window) — already covered by `BLAKE3(request-body)`, so no separate
signed term and no header. The persisted file alone is therefore inert:
the only secret is the identity key the coordinator already protects
(name-claims, provenance, peer-fetch), and mint keeps no per-coordinator
registry once the primary is issued — the pairing rides the token.

### Enrollment

Enrollment binds a coordinator's self-asserted
`sub`/`cnf` to an operator-verified key, once, and
exchanges it for the non-expiring primary.

**Bootstrap macaroon.** At first start mint draws a random nonce — the
`bootstrap` value — persists it (single current value, same custody as
the root), and emits the bootstrap macaroon: the root attenuated with
`op=enroll`, `aud=mint`, `bootstrap=<current>`. It is
non-expiring, carries no coordinator identity, and is a pure
participation gate. It is distributed out-of-band and is reusable for
every coordinator that enrols against this mint.

**(1) `POST /v1/enroll`.** The client attenuates the bootstrap macaroon
with `sub=<own id>` (Elide: the coordinator ULID) and
`cnf=ed25519:<own pub>` and presents it with a PoP over the body, by
the private half of `cnf` (the `assume-role` PoP machinery). Mint
verifies the chain against its root, `op=enroll`, `bootstrap`=current,
and the PoP against the appended `cnf`; records a **pending enrollment**
keyed by `sub` — `(sub, pub, bootstrap, first-seen ts, peer ip)`; and
returns an **intermediate macaroon** minted fresh from root: short
`exp`, `op=enroll-exchange`, the same `sub`/`cnf`, plus a third-party
caveat when an identity authority is configured. A retried request with
an identical `(sub, pub)` is idempotent (fresh intermediate, same
record); a second request for the same `sub` with a different pub is a
conflict that surfaces to the operator and never auto-resolves; a pub
seen on a different `sub` is anomalous (a new key is a new principal)
and surfaced. Unapproved records age out on a bound ≥ the intermediate
`exp`, keeping the table transient rather than a registry.

**(2) Operator approval.** `mint enroll approve <sub>` prints the
pending record's `cnf` fingerprint and requires an interactive y/N
confirmation (default no); the operator confirms only after matching it,
through a trusted side channel, against what the client reports
(`mint client fingerprint`). That interactive confirmation **is** the
trust anchor binding `sub` to the rightful key in the minimal
deployment — it gates approval rather than trailing it; the third-party
caveat is an additive upgrade, not a replacement. `--yes` skips the
prompt for automation (the operator then asserts the out-of-band check
happened).

**(3) `POST /v1/enroll-exchange`.** Before the intermediate expires the
coordinator presents it with a `coordinator.key` PoP, discharging any
third-party caveat. Mint verifies the chain, `op=enroll-exchange`,
the PoP, and the discharge; requires the pending record **approved**;
**re-mints from root** the primary (`op=assume-role`, the same
`sub`/`cnf`, `aud=mint`, no `exp`, no
third-party caveat); and **consumes** the pending+approval record. A
later re-enrollment of the same coordinator is a fresh pending request
needing fresh approval — mint holds no standing per-coordinator state
once the primary is issued.

**Rotation.** `mint bootstrap rotate` draws a new random
`bootstrap`, persists it, emits a fresh bootstrap macaroon, and
drops every pending record whose `bootstrap` is not the new value.
Outstanding primaries are unaffected — they carry no `bootstrap`
and were re-minted from root. Restart preserves the nonce; only explicit
rotation cancels in-flight enrollments.

Refresh cadences, distinct, in increasing trust cost:

- **Tigris keypair** — re-call `assume-role` with the held macaroon
  (*Open questions* #8).
- **Volume Tigris keypair** — the coordinator attenuates its primary
  into `volume-ro` and calls `assume-role`, then vends the resulting
  keypair to the volume over the local handshake. On demand per fetch
  episode for non-lazy volumes; kept warm and refreshed proactively for
  lazy ones (the `coord-data` cache pattern). The volume holds no
  macaroon; the keypair `DateLessThan` is the only lifetime here.
- **Discharge macaroon** — when a third-party caveat is present, fetched
  from the identity authority on its own shorter cadence.

The primary itself has no refresh cadence — it does not expire (see
above); it is minted once at enrollment.

## Protocol

### `assume-role`

```
POST /v1/assume-role
Host: <mint-instance>
Authorization: Macaroon <base64-encoded macaroon>
X-Mint-Coord-Pop: <base64 Ed25519 signature>
Content-Type: application/json

{
  "ts": 1747000000,
  "role": "volume-ro",
  "ttl_seconds": 3600,
  "ancestors": ["01ARZ...", "01BXY..."]
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

### Enrollment endpoints

```
POST /v1/enroll              # bootstrap (+sub/cnf) + PoP
                             # → 200 intermediate macaroon (base64)

POST /v1/enroll-exchange     # intermediate + PoP + discharge bundle
                             # → 200 primary macaroon (base64)
```

Both carry the macaroon in `Authorization: Macaroon <base64>` and the
PoP in `X-Mint-Coord-Pop` exactly as `assume-role` (the body is the PoP
freshness `ts` and nothing else). `/v1/enroll` always returns `200` with
an intermediate even for a brand-new `(ulid, pub)` — the pending record
is created and approval is out-of-band; the coordinator polls
`/v1/enroll-exchange`, which returns `403` until the operator has
approved and `200` with the primary once it has. Failures are the same
opaque `401` as `assume-role`.

### Authentication

Authentication is identical across all three `op`s — `enroll`,
`enroll-exchange`, `assume-role`. The `Authorization` header carries the
presented macaroon, base64-encoded — the coordinator-attenuated
bootstrap at `/v1/enroll`, the intermediate at `/v1/enroll-exchange`,
the attenuated primary at `/v1/assume-role`; any discharge macaroons for
third-party caveats accompany it (bundle wire format per *Open
questions* #15; discharges apply to the intermediate and primary, never
the bootstrap). The mint verifies the presented macaroon's chain MAC
against its own macaroon root, and each discharge against the relevant
third-party key (see `design-auth-model.md` for the construction).

The request also carries the proof-of-possession the macaroon's
`cnf` caveat requires: `X-Mint-Coord-Pop` is the base64
Ed25519 signature, by `coordinator.key`, over `BLAKE3(macaroon-tail ‖
BLAKE3(request-body))`. Every Elide-path token is key-bound — the
coordinator appends `cnf` when it attenuates the bootstrap,
and mint carries it through the intermediate and primary — so PoP is
required on all three operations. The body it covers differs by
operation: at `/v1/enroll` and `/v1/enroll-exchange` it is just the
freshness `ts`; at `/v1/assume-role` it is the full exercise body (§
*Request body*). The freshness timestamp is **not** a header — it is a
`ts` field (unix seconds) *inside the body*, already covered by
`BLAKE3(request-body)`, so it needs no separate signed term. Only the
detached signature is a header (it cannot live in the body it signs).
The mint recomputes the digest over the **exact raw body bytes it
received** (hashed before parsing — no JSON canonicalization, which is
itself a footgun) and the presented macaroon's tail, verifies the
signature against the sealed `cnf`, and **then** reads `ts`
from the now-authenticated body and rejects it if outside the skew
window. Only after the signature verifies does any `request.*` body
field — `ts` included — become a trusted input. A macaroon that carries
no `cnf` is a plain bearer and no PoP is required: this
applies only to a non-Elide caller at `/v1/assume-role`; the Elide
enrollment path always seals one, so `enroll`/`enroll-exchange` are
never bearer.

If verification fails — bad MAC, unknown root, malformed encoding,
wrong/absent `op` for the endpoint, stale `bootstrap`,
missing or bad PoP when `cnf` is present — the mint returns
`401 Unauthorized` with no further detail (don't help an attacker
distinguish "wrong key" from "tampered caveats" from "bad PoP"). The
sole non-`401` authorization outcome is `/v1/enroll-exchange` returning
`403` for a not-yet-approved pending record — an awaited state, not an
auth failure.

### Request body

This section is the `/v1/assume-role` body; the `enroll`/`enroll-exchange`
bodies carry only `ts`. The request body specifies the **exercise of
authority** — what the caller is asking for right now within the bounds
the macaroon attests to. The
whole body is covered by the PoP signature (§ *Authentication*), so every
field is vouched for by `coordinator.key` and bound to this exact
macaroon and moment. Mint is **body-field-agnostic** in the same way it
is caveat-vocabulary-agnostic: it does not hard-code which fields are
meaningful. It parses the verified body into the `request.*` template
namespace; a role's policy template is the only thing that decides which
fields matter, by referencing them (strict mode — a template referencing
an absent `request.X` fails closed). Conventional fields:

- `ts` (required when the macaroon is key-bound): the PoP freshness
  timestamp, unix seconds. Carried here, not in a header, so it is
  covered by the signature over the body; mint rejects it outside the
  ±skew window. Absent/garbled on a key-bound request ⇒ `401`.
- `role` (required): role name from the mint's configuration.
- `ttl_seconds` (optional): requested credential lifetime. Must be within
  the role's `min_ttl_seconds`..`max_ttl_seconds` and must not exceed the
  macaroon's `exp` caveat. Defaults to the role's `default_ttl_seconds`.
- `ancestors` (role-specific): the ancestor vol-ulid set the
  `volume-ro` policy expands into per-ancestor ARNs. It is **not** a
  caveat: the coordinator computes the honest lineage from signed
  provenance and asserts it here, authenticated by the PoP rather than
  the MAC chain. Mint neither knows nor requires this field except
  through the role template that names it.

### Response

On success: the freshly-minted Tigris keypair plus its absolute expiration.

On role mismatch (caller asks for role not in config, or caveats don't
satisfy role requirements): `400 Bad Request` with a generic error.

On Tigris-side failure (rate limit, quota, admin credential rejection):
`503 Service Unavailable` with an error code indicating retry-ability.

Error model is deliberately coarse; see *Open questions*.

## Role configuration

### Schema

Roles are declared in the TOML config file (loaded at mint startup).
Each role's *metadata* stays in `mint.toml`; its *policy template* lives
in its own file under `roles_dir`, named `<name>.json` by default:

```toml
[[role]]
name = "volume-ro"
required_caveats = ["elide:Volume"]
min_ttl_seconds = 60
max_ttl_seconds = 604800     # 7 days
default_ttl_seconds = 86400  # 1 day
# template: <roles_dir>/volume-ro.json (the default; no policy_file needed)
```

```jsonc
// <roles_dir>/volume-ro.json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "ReadVolumeAndAncestors",
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": [
      "arn:aws:s3:::{{tenant.bucket}}/by_id/{{caveat "elide:Volume"}}/*"
      {{#each request.ancestors}},
      "arn:aws:s3:::{{tenant.bucket}}/by_id/{{this}}/*"
      {{/each}}
    ],
    "Condition": {
      "DateLessThan": {"aws:CurrentTime": "{{system.expiry_iso8601}}"}
    }
  }]
}
```

The template filename defaults to `<name>.json`; an optional
`policy_file` on a role overrides it (for a non-`.json` name, or to
point two roles at one shared template). Whether derived or explicit,
the filename is resolved against `roles_dir` and must be a single normal
path component — parsed, not substring-checked: `Path::new` of it must
yield exactly one `Component::Normal`. That rejects path separators,
absolute paths, `.`, `..`, parent traversal, and the empty string in one
predicate, so neither a role name nor a `policy_file` can reach outside
the roles directory. Because the default derives from `name`, an unsafe
role name (one containing a separator or `..`) is rejected by the same
check — distinctly diagnosed (`BadDerivedPolicyName`) from a bad
explicit `policy_file` (`BadPolicyFileName`), pointing the operator at
the actual fix. The guarantee is name-level: a symlink *inside*
`roles_dir` is still followed, but `roles_dir` shares `mint.toml`'s
custody, so its contents are the operator's own, not an external-input
boundary. The role inventory — names, required caveats, TTL bounds —
stays visible at a glance in one `mint.toml`; only the multi-line
handlebars-over-JSON template, which is awkward to lint and diff inside a
TOML triple-quoted string, moves to a per-role file. The policy is
mandatory: a role whose template file is absent is a config error
(`ReadPolicyFile`); there is no inline form.

### Templating

The mint substitutes four classes of variable in the policy template at
issuance time, each with an explicit, distinct trust provenance:

- `{{tenant.X}}` — values from the mint's tenant configuration (bucket
  name, etc.), as a plain path. Server-side, never caller-controlled.
- `{{caveat "X"}}` — the verified macaroon's caveat named `X` (MAC-bound,
  rooted in the mint's macaroon root), resolved through a built-in
  `caveat` lookup helper that takes the caveat name as a string argument.
  All caveats are scalar and render directly (`{{caveat "elide:Volume"}}`).
  The helper form (not a `{{caveat.X}}` path) is required because
  namespaced caveat names contain `:`, which is not a legal template path
  segment; it also keeps the caveat surface to a single named lookup
  rather than arbitrary data-graph traversal.
- `{{request.X}}` — fields from the PoP-verified request body (bound to
  `coordinator.key`, this macaroon's tail, and this moment — §
  *Authentication*). Available **only** after the PoP signature is
  verified. Scalars render directly; arrays iterate as
  `{{#each request.ancestors}}…{{/each}}`. This is the channel for
  honest-but-unverified scoping data the coordinator computes (e.g. the
  ancestor lineage): mint transmits it into the policy, the PoP
  authenticates *who* asserted it, mint never validates the value.
- `{{system.X}}` — values computed by the mint at request time, as a
  plain path. v1 set: `system.expiry_iso8601` (the issued credential's
  expiry, derived from the requested or default TTL).

The `caveat` helper resolves names against the chain under AND
semantics: a scalar caveat repeated across attenuations must agree on a
single value; two disagreeing occurrences are an unsatisfiable
restriction the holder constructed and resolve to a hard failure —
**never** silently to "absent" (that would let a holder, who can append
caveats with only the trailing MAC, neutralise a binding caveat by
appending a contradictory copy). A reference to a caveat the macaroon
does not carry, or one that is unsatisfiable, is a hard render failure:
the request is refused, never minted with a missing or downgraded
substitution. `{{request.X}}` is likewise strict — an absent field a
template references fails the render closed.

The mint **does not** ship a general-purpose policy DSL. The entire
template surface is `{{tenant.*}}` / `{{system.*}}` plain paths, the
`caveat` scalar lookup helper, `{{request.*}}` fields, and `{{#each}}`
over a `request.*` array. Conditional blocks, arithmetic, value
transformations, and dynamic resource construction beyond straight
substitution are deliberately out of scope. Roles requiring more
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
    macaroon.exp - now  // can't outlive the macaroon
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

Names split by provenance. **Borrowed** caveats reuse a registered
claim verbatim — the abbreviation *is* the standard, so a consumer who
knows JWT knows the semantics with no lookup. **Coined** caveats name a
mint-specific concept with no registered equivalent; they are readable
lowercase words, deliberately *not* in the registered-claim style, so a
reader does not hunt for them in an RFC.

Borrowed (RFC 7519 / RFC 7800):

- **`aud`** (string, scalar; RFC 7519). Names the service the macaroon
  is intended for. Prevents a macaroon scoped for one service (e.g.
  coord-internal IPC) from being replayed at another (e.g. mint). Mint
  config declares its own audience (e.g. `"mint"`) and rejects macaroons
  whose `aud` doesn't match.
- **`exp`** (uint64 unix seconds, scalar; RFC 7519). Standard expiry.
  Multiple `exp` caveats narrow to the minimum — a numeric
  intersection, not a list.
- **`sub`** (string, scalar; RFC 7519). The opaque principal the
  credential is about and bound to. Mint treats it as opaque: it keys
  the pending table on it and the operator approves it. The Elide
  instantiation puts a coordinator ULID here; a role policy may template
  it (`{{caveat "sub"}}`). Coordinator-self-asserted in enrollment;
  survives into a primary only via the re-mint-from-root after operator
  approval.
- **`cnf`** (string, scalar; RFC 7800). The holder-of-key the request
  must prove possession of — scalar-encoded (`ed25519:<pub>`), **not**
  the JWT `cnf` JSON object. Every `assume-role` (and enrollment)
  request carries a fresh Ed25519 signature by `coordinator.key` over
  `tail ‖ BLAKE3(body)`, verified against this key. Makes the primary
  key-bound, not a bearer.

Coined (mint-specific; no registered equivalent):

- **`op`** (string, scalar). Partitions a token to one mint operation:
  `enroll`, `enroll-exchange`, or `assume-role`. Mint stamps it at every
  point it mints (bootstrap, intermediate, primary) and each endpoint
  **positively requires** its own value — no endpoint tests for absence.
  Immutable by construction: a coordinator can only append, and a
  contradictory copy is unsatisfiable.
- **`role`** (string, scalar). Restricts which role this macaroon can
  assume. If absent, any role the mint config exposes is reachable.
- **`bootstrap`** (string, scalar). Carried only by the bootstrap
  macaroon. Mint stores one current random nonce (persisted, same
  custody as the root) and rejects any bootstrap whose `bootstrap` value
  ≠ current. `mint bootstrap rotate` draws a new nonce; equality only,
  no ordering.

### Namespacing

The standard caveats above (`aud`/`exp`/`sub`/`cnf`/`op`/`role`/
`bootstrap`) are un-namespaced — they are the mint mechanism, common to
every consumer. Consumer-specific caveats are conventionally prefixed to
indicate their issuer or domain. Elide's is:

- `elide:Volume`

This avoids collisions between issuers. Role templates reference such
caveats by their full namespaced name through the `caveat` helper:
`{{caveat "elide:Volume"}}`. The string-argument form is what makes the
`:` separator usable in a template at all. (`sub` is the principal even
for Elide — there is no `elide:`-prefixed coordinator caveat; the
coordinator ULID is simply the `sub` value.)

### All caveats are scalar

There are no list-valued caveats. Every caveat is a scalar capability
predicate that attenuates by AND (repeated occurrences must agree;
`exp` narrows to the numeric minimum). The only list-shaped input
a role ever needed — the ancestor set for `volume-ro` — is **not** a
caveat: it rides the PoP-signed request body as `request.ancestors`
(§ *Request body*, § *Templating*). This keeps the macaroon library to
scalar caveats plus the holder-of-key extension; no list-valued caveat
type, no intersection semantics, no chain whose effective value depends
on occurrence order.

### Partitioning vs. narrowing caveats

Caveats split into two kinds by where their value originates:

- **Partitioning** — `op`, `bootstrap`, `sub`,
  `cnf`. Identify what the token is for and bind the
  principal. `op`/`bootstrap` are mint-stamped at each mint
  point; `sub`/`cnf` are coordinator-self-asserted
  inside enrollment and survive into a primary only via the
  re-mint-from-root that follows operator approval (see *Coordinator
  bootstrap*). A caller never alters any of them — an appended
  contradictory copy is unsatisfiable and fails closed, never silently
  dropped.
- **Narrowing** — `elide:Volume`, `exp`. Coordinator-appended,
  restricting an existing grant to one volume / expiry for attribution
  and per-credential blast-radius reduction. Volume ownership across
  coordinators is established by the name-claim and body-token lineage;
  `elide:Volume` scopes a coordinator's own credential within authority
  it already holds.

The honest-but-unverified lineage data (the ancestor set) is neither:
it is not a capability the macaroon attests, it is a per-request
assertion the coordinator computes from signed provenance and the PoP
authenticates. It therefore belongs in the signed body, not the caveat
chain — see *Request body*.

### Caveat field inventory (Elide)

The complete caveat vocabulary the Elide roles draw on. A caveat serves
one or both of two purposes: it **gates** authorization (listed in a
role's `required_caveats`) and/or it **feeds** the policy template
(`{{caveat "X"}}` substitution). Some only gate.

| Caveat | Type | Scalar/List | Issuer | Purpose |
|---|---|---|---|---|
| `aud` | string | scalar | macaroon issuer | Gate only — must equal `mint`. Cross-service replay defense. |
| `op` | string | scalar | mint, at each mint point | Gate only — endpoint partition (`enroll` / `enroll-exchange` / `assume-role`); each endpoint positively requires its value. |
| `bootstrap` | string | scalar | mint, on first start / rotate | Gate only — bootstrap macaroon must carry the current value. |
| `exp` | uint64 (unix s) | scalar | issuer | Gate — caps granted TTL (`min(req, role.max, exp−now)`); multiple narrow to the minimum. |
| `role` | string | scalar | issuer | Gate only — restricts the assumable role. Optional. |
| `sub` | string (opaque; Elide: coord-ulid) | scalar | coordinator-self-asserted in enrollment; survives into a primary only via re-mint-from-root after operator approval | Gate on all `coord-*`; defines the primary macaroon. Templated as `{{caveat "sub"}}` in `coord-identity`. |
| `cnf` | string (`ed25519:<pub>`, scalar-encoded) | scalar | coordinator-self-asserted alongside `sub` | First-party proof-of-possession — every `assume-role` request must carry a fresh Ed25519 signature by `coordinator.key` over `tail ‖ BLAKE3(body)` (freshness `ts` rides in the body), verified against this key. Makes the primary key-bound (not a bearer) and authenticates the request body. |
| `elide:Volume` | string (vol-ulid) | scalar | coordinator (narrowing) | Gate **and** template — `by_id/{{caveat "elide:Volume"}}/*`. |

The ancestor set is **not** in this table — it is not a caveat. It is
`request.ancestors` in the PoP-signed body (§ *Request body*).

Per-role gate matrix (template substitutions are listed in each role's
definition below):

| Role | `aud` | `exp` | `sub` | `elide:Volume` |
|---|---|---|---|---|
| `coord-data` | ● | ● | ● | ● |
| `coord-names` | ● | ● | ● | |
| `coord-events` | ● | ● | ● | |
| `coord-identity` | ● | ● | ● | |
| `coord-list` | ● | ● | ● | |
| `coord-base` | ● | ● | ● | |
| `volume-ro` | ● | ● | | ● |

Non-caveat template inputs (the other three substitution classes,
listed here so the issuer's surface is unambiguous):

- `{{tenant.X}}` — server-side config; Elide uses `tenant.bucket`. Never
  caller-controlled.
- `{{request.X}}` — PoP-verified request body; Elide uses
  `request.ancestors` (the `volume-ro` ancestor lineage). Vouched for
  by `coordinator.key`, never validated by mint.
- `{{system.X}}` — mint-computed at issuance; Elide uses
  `system.expiry_iso8601`.

Notes:

- **Every caveat is scalar.** The one macaroon-library extension this
  inventory requires is the first-party holder-of-key caveat for
  `cnf` (#16). No list-valued caveat type is needed (#6
  resolved): the only list-shaped input, the ancestor set, is
  `request.ancestors` in the PoP-signed body, not a caveat.
- **`sub` templates only in `coord-identity`**
  (`coordinators/{{caveat "sub"}}/*`, own-prefix write). Every
  other `coord-*` role uses it as a gate only; their policies use
  prefix wildcards (`names/*`, `coordinators/*`, `events/*`).
- **`coord-base` is the read-only baseline every coordinator holds**, and
  the only credential the LAN/internet-exposed peer-fetch verifier holds.
  Coordinator-wide read of `names/*` / `coordinators/*` / `events/*`,
  gated by `sub` like the other `coord-*` roles.

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
  buffer in the WAL if a refresh briefly stalls. `volume-ro` is also
  coordinator-assumed (the volume holds only the resulting Tigris
  keypair); for a lazy volume the coordinator keeps that keypair warm so
  a cache-miss demand-fetch never waits on `assume-role`. The wider
  read-only window is justified by it being the narrowest scope in the
  system.

### `coord-data` (Split B — per-volume)

Per-volume `by_id/` writer. Assumed by the coordinator the first time it
writes a given volume within a TTL window; the returned keypair is cached
in memory keyed by vol_ulid and re-assumed on miss/expiry. Structurally
identical to `volume-ro` but with write actions and a single (non-ancestor)
prefix.

- **Required caveats:** `sub`, `elide:Volume`, `aud=mint`,
  `exp`
- **TTL:** 24h default. Not on the hot write path (cache holds the key for
  the window; WAL absorbs a brief refresh stall), and 24h bounds the
  write/delete revocation window on a single volume.
- **Policy:** `s3:GetObject`/`s3:PutObject`/`s3:DeleteObject` on
  `arn:aws:s3:::{{tenant.bucket}}/by_id/{{caveat "elide:Volume"}}/*`, single
  volume only.

GC and the reaper cross volume boundaries (read ancestor/input prefixes,
delete a consumed prefix). GC *input reads* compose by assuming `volume-ro`
for the inputs alongside `coord-data` for the output volume rather than
widening `coord-data`'s policy. (Reaper delete of a volume's own prefix is
covered by `coord-data` on that volume.)

### `coord-names` (Split A)

Coordinator-wide. Name claim / rename / force-release / rollback.

- **Required caveats:** `sub`, `aud=mint`, `exp`
- **TTL:** 1h. Control-plane, infrequent; refresh-on-demand is cheap.
- **Policy:** `s3:GetObject`/`s3:PutObject`/`s3:DeleteObject` on
  `arn:aws:s3:::{{tenant.bucket}}/names/*`.

### `coord-events` (Split A)

Coordinator-wide. Event-journal appends and reads.

- **Required caveats:** `sub`, `aud=mint`, `exp`
- **TTL:** 1h.
- **Policy:** `s3:GetObject`/`s3:PutObject` (**no** `s3:DeleteObject`) on
  `arn:aws:s3:::{{tenant.bucket}}/events/*`. The append-only invariant is
  enforced here at the IAM layer — no role in the inventory holds delete on
  `events/`.

### `coord-identity` (Split A — own-prefix only)

Writes this coordinator's own identity records: `coordinator.pub` and
`peer-endpoint.toml`. Scoped to **its own** `coordinators/<ulid>/`
prefix via `sub` templating — it cannot touch any other
coordinator's records. Peer identity/endpoint *reads* are not here;
they are covered by the read-only `coord-base` baseline.

- **Required caveats:** `sub`, `aud=mint`, `exp`
- **TTL:** 6h.
- **Policy:** `s3:GetObject`/`s3:PutObject` (**no** `s3:DeleteObject`) on
  `arn:aws:s3:::{{tenant.bucket}}/coordinators/{{caveat "sub"}}/*`.
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

- **Required caveats:** `sub`, `aud=mint`, `exp`
- **TTL:** 6h.
- **Policy:** `s3:ListBucket` on `arn:aws:s3:::{{tenant.bucket}}` (bucket
  resource, no object statement).

It only exposes object *keys* (ULIDs, names, coord ids), never object
contents. Eliminating LIST dependence — `events/<name>/HEAD` pointers,
deterministic manifest keys, a maintained `names` index — would shrink
or remove `coord-list`; tracked as open question #12.

### `volume-ro`

Per-volume read of one volume's lineage. **Assumed by the coordinator**,
not the volume: the coordinator attenuates its primary (`elide:Volume`,
`exp`), puts the honest ancestor lineage in the request body as
`request.ancestors`, calls `assume-role` with its `coordinator.key` PoP
(which signs the body), and vends the resulting **Tigris keypair** to
the volume process over the local handshake. The volume holds only that
keypair — it never holds a macaroon and never calls mint, so the
coordinator is the only principal that authenticates to mint. Used only
when the volume reads S3 itself: hydration, or the S3 fallback when
peer-fetch is unavailable. Peer-fetch proper does not use it — that path
is the Ed25519 `PeerFetchToken` against a peer's local bytes
(`design-peer-segment-fetch.md`).

- **Required caveats:** `elide:Volume`, `aud=mint`, `exp`
- **Required body:** `request.ancestors` (PoP-signed; the role template
  references it, so an absent value fails the render closed)
- **Keypair freshness — split by volume mode:**
  - *Non-lazy (default):* the coordinator assumes on demand. A hydrated
    volume serves from local cache and touches S3 only in bounded fetch
    episodes; a refresh stall there does not stall guest I/O, so the
    coordinator assumes a fresh keypair per episode (one local
    attenuation + one `assume-role`).
  - *Lazy:* cache-miss demand-fetch is synchronous to guest I/O, so the
    coordinator keeps a warm keypair cached per `vol_ulid` and refreshes
    it proactively (the `coord-data` cache pattern), handing the volume a
    still-valid keypair off the hot path. Revocation window is the
    keypair `DateLessThan`, bounded by the minimal blast radius (read one
    volume's lineage).
- **Policy:** the per-volume RO shape — exact ARN for self
  (`{{caveat "elide:Volume"}}`) plus one per `request.ancestors` entry.

### Why Split B is viable now

`design-iam-key-model.md` § *Per-volume scoping for writes (rejected)*
rejected per-volume writer keys on two grounds. The mint redesign changes
one of them:

- *Confused-deputy enforcement is "modest"* — unchanged. `elide:Volume`
  is a narrowing caveat (see *Partitioning vs. narrowing caveats*);
  per-volume IAM remains a redundant belt over the name-claim lineage.
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

- **Required caveats:** `sub`, `aud=mint`, `exp` — the
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
serving TLS directly, with the admin credential delivered into the
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment via systemd
`LoadCredential=` or equivalent secrets-management.

### Reference client & demo

The full flow is exercisable end-to-end from the `mint` binary alone —
no `elide-*` dependency. The same binary carries the server, the
operator subcommands, and a **reference client** that plays the
coordinator's half generically (it also doubles as the conformance
harness `tests/enroll.rs` exercises).

Operator / server:

```
mint serve <cfg> [bind]            # HTTP service
mint bootstrap [rotate]            # print current bootstrap macaroon / rotate the nonce
mint enroll list                   # pending: sub, cnf fingerprint, peer ip, age
mint enroll approve <sub>          # approve a pending record
```

Reference client (the ed25519 keypair is created out-of-band; the
client only reads it; `--id` is the opaque `sub`):

```
mint client keygen                                       # → client.key/.pub
mint client enroll       --id <sub> <macaroon|file|->    # → intermediate
mint client exchange                                     # 403 until approved → primary
mint client assume-role  --request '{"prefix":"x"}' <role>   # → Tigris keypair
```

A worked `examples/` script chains them: `serve` (background) →
`client enroll` → operator `enroll approve` → `client exchange` →
`client assume-role`, printing the returned Tigris keypair.

**Backend.** `serve --tigris` selects the real Tigris IAM minter — a
self-contained AWS IAM Query-API client (`CreateAccessKey` →
`CreatePolicy` → `AttachUserPolicy`, SigV4-signed against
`https://iam.storage.dev`, overridable via `MINT_IAM_ENDPOINT`), ported
into `mint/` rather than shared with `elide-tigris-iam` so the crate
keeps zero `elide-*` deps. It hard-errors at startup without a Tigris
admin credential in the `AWS_*` environment, so a misconfiguration
fails fast rather than at the first request. Without `--tigris`,
`serve` wires the deterministic fake minter (no account needed).
Consequence for CI: the `bootstrap` / `enroll` / `enroll-exchange` legs
and the fake-minter `assume-role` are hermetic and run anywhere; the
real-Tigris `assume-role` end-to-end is VM-only.

**Demo role config** is a minimal `read` / `write` pair over a single
`{{request.prefix}}` (shipped as `examples/mint-demo.toml`) — distinct from
the full Elide role inventory below; it exists only to exercise the
issuance path.

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
- **Macaroon-root rotation.** TBD — see *Open questions* #3 / #14.

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
3. **Macaroon-root rotation.** A single static mint-held root fits v1,
   but rotation needs a story: rotating it invalidates every
   outstanding macaroon (mint is the issuer, so a re-issue sweep is
   possible but not free). Options: dual-key acceptance during an
   overlap window, a re-issue-on-rotate flow. Tied to #14. Probably
   defer to v2.
4. **Peer-fetch scope — settled.** There is no dedicated peer-fetch
   role; the verifier uses `coord-base` (read-only `names/*` /
   `coordinators/*` / `events/*`). Lineage is verified by the serving
   peer against its own *local* signed `volume.provenance`, not via S3.
   The force-release fence is gap-free via the per-request ETag-
   conditional `names/<name>` read (fence coincident with the S3 CAS).
5. **Mid-path wildcard verification.** Not on the v1 critical path:
   `coord-data` uses a single-volume *trailing* wildcard
   (`by_id/{{caveat "elide:Volume"}}/*`), `volume-ro` uses exact ancestor
   ARNs, and `coord-base` touches no `by_id/` at all — none need mid-path
   `*`. It is only a constraint on a future role wanting
   `by_id/*/<something>` shape. Empirical test still worth running once,
   but does not block the current inventory.
6. **Caveat library schema — resolved.** No list-valued caveat is
   needed. The only list-shaped input (the `volume-ro` ancestor set)
   rides the PoP-signed request body as `request.ancestors`, not the
   caveat chain. All caveats are scalar; the only macaroon-library
   extension over `design-auth-model.md`'s scalar caveats is the
   holder-of-key caveat (#16). This also removes the occurrence-order
   /effective-vs-last hazard a list caveat would carry.
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
13. **Enrollment surface — settled.** See *Coordinator bootstrap* §
    *Enrollment*: reusable non-expiring bootstrap macaroon → coordinator
    self-asserts `sub`/`cnf` and `POST /v1/enroll`
    creates a pending record → operator approves a displayed pubkey
    fingerprint → `POST /v1/enroll-exchange` re-mints the primary from
    root and consumes the record. `bootstrap` is the rotation knob;
    `op` partitions the three endpoints.
14. **Root-key durability.** *Resolved:* mint generates the root key
    on first start and persists it at `<data_dir>/root_key` (64 hex
    chars, 0600), like the coordinator's identity key; the `bootstrap`
    shares this custody. The accepted consequence is that losing
    `data_dir` invalidates every outstanding macaroon — recovery is
    re-bootstrap + re-enroll, not state restore. Whether `data_dir`
    warrants backup/replication, and how the root rotates, remain open
    and are tied to rotation (#3).
15. **Third-party-caveat construction.** Delegation to an identity
    authority is a third-party caveat (mint shares a symmetric key per
    discharge authority; the caveat carries a verification key encrypted
    to that authority; the holder presents discharge macaroons).
    `design-auth-model.md` documents only scalar first-party caveats
    today; the third-party construction and its discharge-bundle wire
    format on the intermediate at `/v1/enroll-exchange` need specifying.
    The anchor split is settled: the minimal self-hosted deployment is
    anchored by operator approval of a displayed fingerprint (§
    *Enrollment*); the third-party caveat mint stamps on the
    intermediate is the additive central-service upgrade, not a
    replacement. Open within this question: because the primary does not
    expire, periodic re-attestation of a coordinator (e.g. a managed
    customer who left) is enforced at the discharge layer or by refusing
    re-enrollment — which, is unsettled.
16. **PoP caveat wire detail.** `cnf` is decided (first-party
    holder-of-key; primary is key-bound, not a bearer; the signed
    payload is `BLAKE3(presented-macaroon-tail ‖ BLAKE3(request-body))`
    so the proof also authenticates the body — see *Coordinator
    bootstrap*, *Authentication*). The body hash is over the **exact
    raw bytes received**, hashed before parsing — no JSON
    canonicalization (a canonicalization mismatch is a signature-bypass
    footgun). Freshness is a **±skew window** on a `ts` field carried
    *in the body* (not a header — it is already covered by the body
    hash, so no separate signed term and one fewer header); stateless,
    no mint-issued nonce (DPoP's `iat`-skew anchor; prior art: RFC 7800
    `cnf` PoP key, RFC 9449 DPoP). Tail-binding pins the proof to the
    exact macaroon, body-hash binding to the exact request, the in-body
    `ts` + skew window bounds replay. The signature stays a header
    (`X-Mint-Coord-Pop`) — it cannot live in the body it signs; folding
    it in as a structural envelope would reintroduce a framing/
    canonicalization boundary. What remains is only the encoding
    (working draft: `X-Mint-Coord-Pop` base64 Ed25519 signature, body
    `ts` unix seconds, skew bound) — an implementation detail, not a
    design fork.

## Future directions

These do not affect v1 but are anticipated extensions worth designing
around:

- **Third-party caveats.** No longer purely a future direction — they
  are the mechanism for delegation to an identity authority under the
  issuer-and-verifier model (*Trust model*; *Open questions* #15). The
  mint's verification path handles discharge bundles; the chained-MAC
  construction accommodates them with no change beyond accepting
  discharge macaroons in the request. What remains future is the
  concrete construction and wire format, tracked as #15.
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
