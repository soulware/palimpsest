# Auth model: operator tokens, isolation

This doc captures the coordinator's destructive-verb auth surface in one
place: how human operators authenticate the CLI's destructive verbs, what
the coordinator's audit log records, and what the scheme does and does not
enforce given the same-host trust model.

The underlying macaroon construction — chained keyed-BLAKE3 MAC, per-token
struct-level nonce, AND-of-predicates caveat evaluation — is shared with
volume macaroons and is documented in
[`architecture.md`](architecture.md#proposed-s3-credential-distribution-via-macaroons).
This doc layers the operator-token-specific surface on top of that
foundation:

- **Volume macaroons** — minted on `register`, PID-bound, scope-bound. Used
  by volume processes to request short-lived read-only S3 credentials.
  Implemented. Construction and registration flow live in `architecture.md`.
- **Operator tokens** — minted on `elide token create` (IPC), not
  PID-bound, attenuated per use by the CLI to the narrowest volume/expiry
  needed. Today they gate a single proof-of-concept verb. The settled
  direction — operator tokens authorise the coordinator's *S3 write*
  credential acquisition, not a hand-enumerated verb list — is in
  *Proposed: operator tokens gate S3 writes, not verbs* below.
- **Isolation model** — the surrounding context that explains what either
  scheme can enforce on a shared-uid host.

## Operator tokens

Operator tokens are coordinator-wide macaroons issued to human operators.
The gating mechanism below is a **proof of concept**: it currently wires
exactly one verb, `remove`. `remove` is a poor exemplar — it deletes only
the local cache directory and is fully reversible by re-pulling from S3,
so it neither loses data nor demonstrates the property the token is meant
to enforce. The PoC should move to `claim` / `release`, which actually
mutate shared S3 state (the `names/<name>` ownership record). The settled
direction supersedes per-verb gating entirely — see *Proposed: operator
tokens gate S3 writes, not verbs* below.

### Issuance

```
elide token create [--expires 30d]
```

This is an IPC verb (`Request::MintOperatorToken`) against `control.sock`.
The coordinator mints with its in-memory root key and returns the encoded
macaroon plus the per-token nonce (hex) and expiry; the CLI prints the
token to stdout, upserts it into `~/.elide/tokens.toml`, and logs the
nonce/expiry plus the file path to stderr.

`~/.elide/tokens.toml` is a per-user file (mode 0600 under `$HOME`) with
one entry per coordinator, keyed by that coordinator's canonical
data_dir:

```toml
[[coordinator]]
data-dir = "/srv/elide-a"
operator-token = "MDAxMG..."

[[coordinator]]
data-dir = "/home/op/elide_data"
operator-token = "MDAxNm..."
```

Keeping the trust boundary per-user while keying by data_dir lets
several coordinators run on one host without sharing a token. `token
create` rewrites only the addressed coordinator's entry; the others
survive the upsert. Gated verbs resolve the token in precedence order:
`--token`, then `ELIDE_OPERATOR_TOKEN`, then the entry whose `data-dir`
matches the canonical data_dir the CLI used to reach the socket.

`elide token list` prints one row per entry — data_dir, token nonce,
and expiry — decoding the stored macaroon for the nonce and narrowest
`NotAfter` without contacting any coordinator. `elide token remove
<nonce>` deletes a single entry, selected by the nonce from that
listing rather than by path, so a stale entry can be cleaned up after
its coordinator's data_dir is gone.

The mint endpoint is ungated beyond socket reachability. The trust floor
for "can mint an operator token" is "can reach the coordinator's unix
socket," which is the same floor as "can perform every other coordinator
operation." There is no separate gate to add here without moving the trust
boundary, and that move requires off-host transport, which is out of scope.

`--expires` defaults to 30 days. The default is configurable down for
tests; there is no indefinite-lifetime option.

### Caveats

The minted root token carries:

| Caveat | Value | Purpose |
|---|---|---|
| `Role` | `Operator` | Distinguishes from volume tokens |
| `NotAfter` | mint + `--expires` | Required; bounded lifetime |

It does **not** carry a `Volume` or `Op` caveat — the root token is
coordinator-wide and verb-agnostic. Volume and op scoping happen per use,
via attenuation.

Each minted token also carries a per-token 16-byte random struct-level
nonce (generated inside `mint`; not a caveat). The nonce is mixed into the
MAC seed so two tokens minted with identical caveats still have distinct
MACs and gives each token a stable hex identifier for audit logging — see
*Audit log* below.

### CLI-side attenuation per use

Each destructive CLI verb appends caveats before sending the token to the
coordinator. Attenuation narrows by three axes: operation, volume, expiry.

```
stored:     Role=Operator, NotAfter=<+30d>           (nonce on the struct)
on the wire (elide volume remove myvm):
            Role=Operator, NotAfter=<+30d>,
            Op=Remove, Volume=myvm, NotAfter=<now+60s>
```

The attenuation is performed entirely in the CLI — no coordinator
round-trip — by calling `Macaroon::attenuate` three times against the
stored token's trailing MAC. AND-of-predicates evaluation in the verifier
means appending a *looser* `NotAfter` cannot widen authority; the original
30-day bound is still in the chain and still checked.

The wire token is therefore single-operation, single-volume,
very-short-lived, and useless to anyone who intercepts it after the fact.
The persistent stored token never leaves the operator's machine in
narrowed form.

### Typed operation surface

The `Op` caveat is typed, not a free string. The coordinator-side enum
enumerates every gated verb:

```rust
pub enum OperatorOp {
    Remove,
    // PoC only. Do not add variants — the per-verb model is
    // superseded; see *Proposed: operator tokens gate S3 writes,
    // not verbs*. The one pre-mint change is moving the PoC hook
    // to `claim` / `release`.
}
```

The dispatcher hands the verifier the `OperatorOp` it is about to execute
(`verify_operator(..., OperatorOp::Remove, target_volume)`). The verifier
requires the chain to carry the matching `Op` caveat. Unknown op-bytes on
the wire → `OperatorReject::Malformed` (fail closed).

Two consequences worth calling out:

- **Exhaustiveness.** Adding a new gated verb is "add an enum variant and
  a dispatch arm." A new verb cannot accidentally inherit authority from
  an existing operator token, because operator tokens are minted as
  `Op = ∅` and only the CLI's attenuation step adds the op caveat for the
  specific verb being invoked.
- **The op caveat must match the entry-point IPC verb,** not any
  sub-operation a handler dispatches internally. Today every gated verb
  is a single dispatch and this is moot, but if a future verb fans out
  into authenticated sub-calls, the design choice is either to
  re-attenuate per sub-call (more macaroon-like) or to document that the
  entry-point caveat is what matters.

### Verifier shape

Operator tokens have no `Pid` or `Scope`, so they don't fit the volume
`VerifyCtx` from `architecture.md`. The macaroon module exposes a
parallel verifier:

```rust
pub struct VerifyOperatorCtx<'a> {
    pub now_unix: u64,
    pub op: OperatorOp,
    pub op_volume: &'a str,
}

pub fn check_operator_caveats(
    m: &Macaroon,
    ctx: &VerifyOperatorCtx<'_>,
) -> Result<(), OperatorReject> { /* AND-of-predicates over Role / Op / Volume / NotAfter */ }
```

Top-level `verify_operator` is `parse` → `verify` (MAC, shared with volume
macaroons) → `check_operator_caveats`. Rejection reasons (`Malformed`,
`BadMac`, `WrongRole`, `Expired`, `WrongOp`, `VolumeMismatch`,
`MissingVolume`, `MissingOp`) are exposed as a typed `OperatorReject` enum
so callers can log without leaking variant-level detail to the wire — the
IPC `Err` body is the coarse string `"operator token rejected (..)"`.

### Audit log

The coordinator logs every operator-token event under
`target = "operator_token::authn"`:

- `event = "mint"` — on `Request::MintOperatorToken`. Fields:
  `nonce` (the struct-level hex), `expires_unix`.
- `event = "verify"` — on a successful gated verb. Fields:
  `op` (`OperatorOp::as_str`), `volume`, `nonce`.
- `event = "reject"` — on any rejection. Fields: `op`, `volume`,
  `reason` (`OperatorReject` variant).

The `nonce` field uses the same name as the volume-macaroon
`creds::issuance` log target — one audit-id concept across both token
kinds, so a single grep correlates mint → use without bookkeeping the two
schemes separately.

Rejection reasons are intentionally coarse on the wire — finer detail
would help an attacker probe token state — but the full
`OperatorReject` variant is logged locally for operator debugging.

## Isolation model

Volume processes on the same host share a uid and a filesystem. This has
direct consequences for what the macaroon scheme can and cannot enforce.

**What macaroons do not enforce — local filesystem.** A compromised volume
process can read or corrupt any other volume's local directory directly,
without touching the coordinator. Macaroons provide no protection here.
Proper local isolation requires OS-level mechanisms: separate uids per
volume, Linux user namespaces, or running each volume in its own
container. This is a separate layer and is not addressed by the current
design.

**What macaroons do enforce — S3.** S3 credentials are scoped by IAM to a
specific volume's prefix. This enforcement is external to Elide — AWS (or
equivalent) rejects requests that exceed the credential's scope regardless
of what the caller claims. The macaroon scheme ensures a volume process
can only obtain credentials for its own volume. A compromised `myvm`
process cannot request credentials for `othervm`, so it cannot read,
write, or delete `othervm`'s S3 objects even with full local filesystem
access.

**What operator tokens provide — audit + ceremony, not access control.**
Requiring an operator token for coordinator mutations raises the bar
slightly over bare socket access, and provides an audit trail. It does
not prevent a compromised local process from achieving the same effect
via direct filesystem manipulation (`rm -rf` on the volume dir achieves
`remove` without going through the coordinator). The value is
auditability, forced ceremony for destructive verbs, and per-request
attenuation — not a hard security boundary against a local attacker.

**Summary:**

| Resource | Isolation mechanism | Enforced by |
|---|---|---|
| S3 data | IAM credential scoping + macaroon gating | AWS + coordinator |
| Local filesystem | uid separation / namespacing | OS (not yet implemented) |
| Coordinator mutations | Operator token + audit log | Coordinator (defense-in-depth) |

## Proposed: operator tokens gate S3 writes, not verbs

**Status: proposed. Not yet implemented. The section above describes the
current PoC; this section describes the settled direction and the one
binding question that the [`mint`](design-mint.md) cutover must answer.**

### The principle

The original intent of operator tokens was never "gate destructive
verbs." It was: **any operation that mutates S3 state must be
authorised.** `remove` was a proof-of-concept hook, not the model. Three
framings were considered and rejected as the organising axis:

- *Destructive verbs* — `remove`'s default form is a reversible local
  cache drop; the destructive/reversible line does not fall on verb
  boundaries.
- *`--force` flags* — narrows the gate to irreversibility escape
  hatches, but says nothing about the routine S3 writes that are the
  actual point.
- *Ownership ops only* — closer (`claim` / `release` do write shared S3
  state), which is why they are the right *PoC*, but still an
  enumeration, not the principle.

The principle is read-vs-write **against S3**: read paths are an
unauthorised baseline; every S3 mutation requires operator
authorisation.

### Why this cannot be expressed today, and becomes structural under mint

Today the coordinator is *both* the macaroon issuer and the holder of
IAM admin that writes S3. Enforcing "every S3 mutation is authorised"
in that architecture means intercepting every code path that touches the
bucket (breadcrumb writes, snapshot uploads, `names/` flips, IAM
teardown) and bolting a token check onto each — a leaky enumeration and
exactly the "optional path for a correctness property" this project
rejects. There is no chokepoint, which is why `remove` could only ever
be a PoC.

`mint` (see [`design-mint.md`](design-mint.md)) creates the chokepoint.
Once mint is split out, the coordinator cannot write S3 with ambient
admin creds: to mutate it must call `mint /v1/assume-role` with a
macaroon and obtain a write-capable keypair (`coord-data`, `coord-names`,
the Split-A writer roles). Reads need only `coord-base`, the read-only
baseline every coordinator already holds. "Every S3 mutation is
authorised" then holds *architecturally* — enforced by IAM at the single
point write credentials are acquired — rather than by scattered
in-coordinator checks.

### The binding open question

`design-mint.md` deliberately leaves the issuer abstract: *"mint is not
an issuer; some other authority mints the macaroons."* The operator
token's role reduces to one question — **what authority issues the
coordinator's write-role macaroon, and where does the human
authorisation enter that chain?** Three shapes are consistent with both
docs; the choice is deferred to the mint cutover:

- **(a) The operator token *is* the write-role macaroon.** `elide token
  create` mints with `Role=coord-data|coord-names|…`; the coordinator
  presents it to mint per write window. Reads use the coordinator's own
  `coord-base`. Most direct reading of the principle.
- **(b) Operator token as a third-party-caveat discharge.** Coordinator
  self-issues a read/identity baseline; write roles additionally require
  a discharge proving a human authorised this window. This is the
  *third-party caveats for authentication* future direction below.
- **(c) Two mint trust roots.** A coordinator-held root for
  `coord-base` / identity; an operator-held root for write roles. Mint's
  multi-root config (`design-mint.md` § *Mint configuration*) is built
  for this.

### Until then

Do not extend `OperatorOp` with more verbs — wiring additional verbs
into the PoC entrenches the per-verb model the mint cutover dissolves.
The only PoC change worth making before mint is moving the hook from
`remove` (misleading: local-cache-only) to `claim` / `release` (genuine
shared-S3-state mutations).

## Open questions

- **Bootstrap.** First-ever `elide token create` against a fresh
  coordinator has no offline escape hatch (there is no
  `elide-coordinator token create` subcommand under this design). If the
  coordinator socket is unreachable, there is no way to mint. Likely fine
  — destructive verbs are coordinator-mediated anyway — but worth noting.
- **Token rotation UX.** No `revoke` command. A leaked token is mitigated
  by its `NotAfter` and by re-keying the root (which invalidates all
  tokens, including volume macaroons). Whether root rotation needs a
  dedicated verb or can stay manual is open.

## Future directions

These do not affect the design above; they describe extensions that slot
in cleanly when the threat model or deployment shape warrants them.

- **Third-party caveats for authentication.** The model above is
  authorisation-only: possession of an operator token is treated as
  operator identity. A future extension adds *third-party caveats* — a
  caveat that says "valid only if the bearer also presents a discharge
  macaroon from `<auth_service>` attesting predicate P." This adds a real
  authentication step (SSO, webauthn, whatever the auth service does)
  tied to each token use, with the discharge's lifetime acting as the
  session length. The chained-MAC construction already accommodates this
  — third-party caveats are just another `Caveat` variant whose body is
  `(location_uri, caveat_id, vid_key)`. None of the existing `Op` /
  `Volume` / `NotAfter` surface needs to change. This is the mechanism
  behind option (b) in *Proposed: operator tokens gate S3 writes, not
  verbs*.
- **Root key in a separate signing process.** Today the coordinator
  holds the root key in memory. Splitting it into a standalone signing
  service reduces blast radius (coordinator compromise can no longer
  forge across the fleet), gives mint operations an independent audit
  boundary, and enables TPM/HSM backing. Verify is hot — every
  operator-token IPC and every volume `credentials` request — so the
  likely shape is per-coordinator derived keys (signing service issues
  an HKDF-derived sub-key the coordinator uses to verify locally) rather
  than RPC-on-verify. Mint is rare enough to comfortably stay RPC. Worth
  doing when there is more than one coordinator host, or when the
  coordinator's trust level is bounded below the key's.
