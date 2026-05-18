# mint (prototype)

Macaroon-authenticated scoped-credential vending for Tigris. Tracks
[`docs/design-mint.md`](../docs/design-mint.md).

This is an implementation tracking the settled design — a runnable
vertical slice, not v1. It lives in the elide workspace during the
design phase and is deliberately free of `elide-*` dependencies; it is
destined to become a standalone OSS project.

## Caveat vocabulary

Borrowed verbatim from the RFCs (`docs/design-mint.md` § *Standard
caveats*): `aud` (RFC 7519), `exp` (RFC 7519), `sub` (RFC 7519 — the
opaque principal; Elide puts a coordinator ULID here), `cnf` (RFC 7800
holder-of-key, scalar-encoded `ed25519:<pub>`). Coined, mint-specific:
`op` (endpoint partition — positively required at every endpoint, never
absence-tested), `role`, `bootstrap` (the rotation nonce). Elide's only
namespaced caveat is `elide:Volume`.

## Flow

**Enrollment** (`docs/design-mint.md` § *Enrollment*):

```
mint bootstrap             -> reusable non-expiring bootstrap macaroon
                              (op=enroll, aud, current bootstrap nonce)
client attenuates sub+cnf, PoP
  POST /v1/enroll          -> pending record (keyed by sub) + short
                              intermediate (op=enroll-exchange)
operator: mint enroll list / approve <sub>   (verify cnf fingerprint
                              out of band — the client prints its own)
  POST /v1/enroll-exchange -> 403 until approved, then re-mint the
                              non-expiring primary from root
                              (op=assume-role); pending record consumed
```

**Vending**: the client attenuates the held primary (`exp`,
`elide:Volume`, …) and `POST /v1/assume-role` + PoP → role gate →
policy render → Tigris keypair.

`mint bootstrap rotate` draws a new nonce and cancels in-flight
enrollments; outstanding primaries are unaffected.

## Modules

- `caveat` / `macaroon` — named **scalar** caveats, chained-BLAKE3 MAC,
  base64 wire. `EffectiveCaveats` resolves a name tri-state — `Absent`
  / `Value` / `Unsatisfiable` — ≥2 disagreeing occurrences are
  `Unsatisfiable` (fail closed, the append-a-contradictory-copy
  defence). `caveat::name` / `caveat::op` are the canonical constants.
- `pop` — the `cnf` holder-of-key gate. Ed25519 over
  `tail ‖ BLAKE3(raw-body)`; freshness `ts` rides in the body. Required
  on all three operations in the Elide path.
- `issuance` — `mint_bootstrap` / `mint_intermediate` / `mint_primary`
  (each a fresh chain from root) + `bound_identity`.
- `state` — persisted bootstrap nonce + transient pending table, a
  directory of files (`bootstrap`, `pending/<sub>.json`,
  `approved/<sub>`) so the lifecycle is `ls`-inspectable. Idempotent
  same-`(sub,pub)`, conflict on a different key, GC of stale unapproved,
  consume-on-exchange.
- `config` — TOML: audience, `data_dir`, `roles_dir`, tenant, role
  metadata. The macaroon root key is not config — `state::Store`
  generates `<data_dir>/root_key` (64 hex, 0600) on first start.
  Each role's policy template is a separate file
  under `roles_dir`, `<name>.json` by default (`policy_file` overrides);
  derived or explicit, it must be a single normal path component.
  Admin credential from `AWS_*`, never the TOML.
- `role` / `template` / `audit` / `http` — role gate, handlebars policy
  render, JSON audit line, axum endpoints.
- `iam` — `KeypairMinter` trait; `FakeMinter` for tests.

## Build

mint is its own Cargo workspace, independent of the elide workspace it
is nested in (Standalone-OSS-to-be). Build, test, and lint it from this
directory — `cargo …` run from the elide root deliberately does not see
it:

```sh
cd mint
cargo build && cargo test
```

## Run it

clap CLI (`--config` defaults to `mint.toml`). Server + operator:

```sh
mint serve   --config mint-demo.toml [--tigris]      # --tigris = real Tigris IAM; else fake minter
mint bootstrap --config mint-demo.toml               # print the bootstrap macaroon
mint enroll list    --config mint-demo.toml
mint enroll approve --config mint-demo.toml <sub>    # shows the fingerprint, interactive y/N; --yes for automation
```

Client (the coordinator's half; identity under `./mint_client`):

```sh
mint client keygen
mint client fingerprint                                  # operator compares this during `enroll approve`
mint client enroll      --id <sub> <macaroon|file|->     # bootstrap is the final positional arg
mint client exchange                                     # exit 2 until approved, then saves the primary
mint client assume-role --request '{"prefix":"demo/x"}' [--caveat N=V] <role>
                                                         # request body is opaque pass-through
```

Without `--tigris`, `serve` wires the deterministic fake minter (warns
loudly) so the whole flow runs hermetically; `--tigris` calls real
Tigris IAM and requires `AWS_*` admin creds.

## Out of scope

TLS, multi-root / root rotation, multi-tenancy, `ListRoles`/`GetRole`,
third-party-caveat discharge for a central identity authority
(`docs/design-mint.md` § *Open questions* #14/#15). The root key is
generated at `<data_dir>/root_key` on first start; backup/replication
of `data_dir` and root rotation remain open (OQ#14, tied to #3).
