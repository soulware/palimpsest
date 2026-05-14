# elide-mint: credential minter process

## Status

Proposed. Extracts the admin-key-holding role out of the coordinator into a
sibling process. Builds on the macaroon construction in
[`design-auth-model.md`](design-auth-model.md) and the key inventory in
[`design-iam-key-model.md`](design-iam-key-model.md) — neither of those
designs change; this one redistributes which process holds which key.

## Why

`design-iam-key-model.md` § *Admin key containment* lists three structural
options for shrinking the admin key's spatial scope, with the per-host
credentialer sidecar being the least disruptive. `elide-mint` is that
sidecar made concrete:

- The admin key is held by a separate process. A coordinator-process
  compromise no longer yields the admin key directly.
- The minting surface (CreateAccessKey, CreatePolicy, AttachUserPolicy,
  DateLessThan refresh) is concentrated in one binary with a small,
  auditable surface area.
- The coordinator's runtime environment no longer carries `AWS_*` admin
  credentials at all, so accidental exposure paths (logs, core dumps,
  child processes) close at the process boundary.

The motivating constraint is Tigris-specific (admin keys are org-global
root — see `design-iam-key-model.md` § *Tigris admin keys are org-global
root*), but the topology is useful on any backend where the admin
credential is meaningfully more privileged than the data-plane credential.

## Process topology

Two long-running processes, siblings under a supervisor:

- **`elide-mint`** — holds the admin credential (`AWS_*` env), serves a
  local Unix socket. Mints and refreshes the key classes enumerated in
  `design-iam-key-model.md` (writer, peer-fetch, per-volume RO, ephemeral
  fetch).
- **`elide-coord`** — the coordinator as it exists today, minus the
  admin-key-holder role. Holds the writer key it received from mint, and
  the per-volume RO keys it has minted on behalf of volumes. No `AWS_*`
  admin env vars.

A single supervisor (`elide coord run`) manages both children in *both*
deployments. The only difference between standalone and systemd is who
launches the supervisor — a human terminal or systemd's
`ExecStart=` — not what the supervisor does.

Mint and coord move as one. Mint has no workload independent of coord
(no other client to serve), and coord cannot function without mint
(every IAM operation routes through it). There is no scenario in which
one runs without the other, so they share a single lifecycle and a
single systemd unit.

### The supervisor

`elide coord run`:

1. Reads `AWS_*` admin credentials from its own env (or from a
   systemd-delivered credential path; see *Credential delivery* below).
2. Generates a 32-byte random mint-RPC token in memory.
3. Spawns `elide-mint`, passing the admin credential and the RPC token
   via inherited file descriptors. Waits for mint to signal "socket
   ready."
4. Spawns `elide-coord` with admin env vars `unsetenv`'d, passing only
   the RPC token via inherited fd. Coord asserts on startup that the
   admin env vars are absent and refuses to start otherwise.
5. Propagates SIGINT/SIGTERM to both children. On Ctrl-C: signal both,
   wait for both to exit, then exit itself.
6. Exits when either child exits (fail-fast).

Foreground by design. In standalone mode `elide coord run` blocks and
Ctrl-C is the intended shutdown path. Under systemd it runs as
`Type=simple` (or `Type=notify`, see below) with systemd handling
daemonisation, restarts, and journaling.

Volumes are unaffected by coord/mint restart cycles. They hold their
currently-issued macaroons and per-volume RO keys; a restart only
matters when they next need to refresh credentials.

### Prior art

The "one outer unit, internal supervisor that runs role-specific child
binaries" pattern is well-established. The closest analogue is
**Postfix**: one `postfix.service`, which runs `postfix start`, which
launches `master(8)` — Postfix's own supervisor — which in turn
launches and re-launches a dozen separate daemons (`smtpd`, `qmgr`,
`cleanup`, `pickup`, `local`, …), each a distinct binary with its own
role and privilege boundary. systemd sees one unit, one main PID; the
internal process tree is Postfix's concern.

Other examples in the same family: PostgreSQL (`postmaster` supervises
`autovacuum`, `bgwriter`, `checkpointer`, `wal writer`, plus a backend
per connection), nginx and Apache (master + worker pool), OpenSSH (sshd
master + per-session forks). All run under one systemd unit with an
internal supervisor managing the process tree.

systemd does carry a documented preference for "one process per unit,"
but the project supports `Type=forking` and `Type=simple` foreground
supervisors precisely for cases like this. The reason to prefer
one-process-per-unit is to let systemd handle restarts and
dependencies — which buys nothing when restart and dependencies are
intrinsically coupled (as they are between mint and coord).

### systemd unit

```ini
# elide-coord.service
[Unit]
Description=Elide coordinator (mint + coord)

[Service]
Type=notify
LoadCredential=admin-creds:/etc/elide/admin-creds
ExecStart=/usr/bin/elide coord run
Restart=on-failure
KillMode=control-group              # SIGTERM whole cgroup on stop
TimeoutStopSec=30
```

`Type=notify` lets the supervisor `sd_notify(READY=1)` once both
children have signalled ready, so `systemctl start elide-coord` blocks
until the service is actually serving. `Type=simple` is acceptable if
sd_notify is inconvenient; the tradeoff is that systemd will report the
unit "active" the instant the supervisor starts, before children are
ready.

Admin credentials reach the supervisor via `LoadCredential=`, which
delivers them to a per-unit, unit-private path readable only by the
unit's user. The supervisor reads them and passes them to mint only;
coord never receives them.

`KillMode=control-group` ensures `systemctl stop elide-coord` sends
SIGTERM to the whole cgroup — supervisor, mint, coord — for clean
shutdown of the entire tree.

### Operator surface

| Verb | Standalone | systemd |
|---|---|---|
| Start in foreground | `elide coord run` (blocks) | n/a |
| Start as daemon | `elide coord start` | `systemctl start elide-coord` |
| Stop | `elide coord stop` (signals supervisor pidfile) | `systemctl stop elide-coord` |
| Status | `elide coord status` | `systemctl status elide-coord` |
| Logs | stdout/stderr | `journalctl -u elide-coord` |

`elide coord start|stop|status` autodetects systemd and shells out to
`systemctl` if the unit exists; otherwise it manages a pidfile directly.

## Trust

The credential plane is strictly hierarchical:

```
volume ↔ coord       (IPC, macaroon-authenticated — unchanged)
coord  ↔ mint        (Unix socket, peer-cred + startup-shared secret)
mint   ↔ Tigris      (admin credential — never leaves mint)
```

Volumes never speak to mint. Mint never speaks to a volume process. The
admin credential lives and dies inside the mint process. The macaroon
construction in `design-auth-model.md` (symmetric chained MAC) stays
inside coord — mint is never given the macaroon root key, never verifies
macaroons, and is not aware of the macaroon scheme at all.

### volume ↔ coord (unchanged)

As today. Volume processes hold coord-issued macaroons (per
`architecture.md`) and present them to coord when requesting credentials
for S3 access. Coord verifies macaroons with its in-memory root key and
decides authorisation. The mint split does not alter this path.

### coord ↔ mint

Three defence layers protect mint's RPC surface — important because
anything that successfully calls `mint_writer_key()` effectively
exercises admin authority over Tigris.

**Layer 1 — Filesystem permissions.** Mint listens on
`<data_dir>/control/mint.sock`. Socket owner = elide uid, mode `0600`,
parent directory mode `0700`. Anything not running as the elide uid is
rejected at `connect()`.

**Layer 2 — Peer-credentials uid check.** On accept, mint reads
`SO_PEERCRED` (Linux) / `LOCAL_PEERCRED` (macOS) and rejects connections
whose peer uid ≠ mint's own uid. Defence in depth against future
namespace or permission misconfiguration.

**Layer 3 — Startup-shared RPC token.** The supervisor generates a
32-byte random token at startup and delivers it to *both* mint and
coord. Coord presents the token as the first frame on every connection
to mint; mint verifies before serving any verb. The token never reaches
volume processes or the CLI.

Token delivery differs by deployment but the verification rule is
identical:

- **`elide coord run`**: supervisor generates the token in memory,
  passes it to each child via an inherited file descriptor (preferred)
  or a one-shot env var that the child reads and immediately
  `unsetenv`s. Coord scrubs the token from its env before spawning any
  volume process, in the same pass that scrubs `AWS_*`.
- **systemd**: a pre-generated token file (owned root, mode `0400`) is
  delivered to each unit via `LoadCredential=mint-rpc-token:/etc/elide/mint-rpc-token`.
  Each unit reads it via the systemd credential path; the file is never
  readable by the elide uid directly.

This third layer is what keeps a same-uid volume process out of mint.
Filesystem permissions and peer-cred uid both pass for any elide-uid
process; without the token, a connecting volume gets its first frame
rejected and the socket closed.

### Residual threats

- **Ptrace of coord by a same-uid process** yields the token. Mitigation
  is OS-layer (`kernel.yama.ptrace_scope ≥ 1`, default on most distros).
  Worth documenting in `operations.md`; not enforceable from Elide.
- **Compromised coord** has the token and can ask mint to mint any key.
  Same blast radius as "coord is the trust root for credential
  decisions today," which we already accept. Coord cannot escalate to
  admin; the admin credential never leaves mint.
- **Compromised mint** holds the admin credential and can do anything
  Tigris IAM allows. This is the surface the split exists to *shrink*,
  by making mint a small auditable binary rather than the full
  coordinator.

### Mint's own outward auth

Mint speaks IAM/S3 to Tigris using its admin credential. It is the only
Elide process that does so. Coord's S3 client is constructed with the
writer key mint returned at coord startup, not the admin key.

## Mint RPC surface

Mint exposes a small, typed verb set over its Unix socket. All requests
authenticate via the startup token; all responses include either an
access key (id + secret + optional session token) or a structured error.

| Verb | Caller | Purpose |
|---|---|---|
| `mint_writer_key(coord_ulid)` | coord at startup | Returns the persistent writer key for this coordinator. Mints + persists if not yet created; returns existing otherwise. |
| `mint_peerfetch_key(coord_ulid)` | coord on peer-fetch enable | Mints (or returns existing) peer-fetch key. |
| `mint_ro_key(coord_ulid, vol_ulid, ancestors, ttl)` | coord on `volume create` / `volume fork` | Mints per-volume RO key with policy scoped to `[vol_ulid, ancestors…]` and `DateLessThan = now + ttl`. |
| `mint_fetch_key(coord_ulid, vol_ulid, ancestors, ttl)` | coord on `start_fetch` | Mints ephemeral fetch key (shorter ttl than RO). |
| `refresh_ro_key(coord_ulid, vol_ulid)` | coord on `DateLessThan` refresh | Performs the create-new-policy → attach → detach-old → delete-old dance for an existing volume's RO key. |
| `delete_ro_key(coord_ulid, vol_ulid)` | coord on `volume delete` | Detaches and deletes the per-volume RO policy and access key. |
| `delete_fetch_key(coord_ulid, vol_ulid)` | coord on fetch-worker exit | Detaches and deletes the ephemeral fetch policy and access key. |
| `list_policies(coord_ulid)` | coord at startup for reconciliation | Returns all policy names starting with `elide-<coord_ulid>-`. Coord cross-references with its local registry and reaps orphans via `delete_*`. |

No macaroon-shaped surface. No verbs that issue tokens, sign anything,
or mutate coord-side state. Mint is purely a typed wrapper over Tigris
IAM operations, scoped to the coord_ulid presented on each call.

`coord_ulid` is in every call so that a future "mint serves more than
one coord" deployment (e.g. the central credentialer service from
`design-iam-key-model.md` § *Admin key containment*) can authorise
per-coord scopes without changing the RPC surface. In the per-host OSS
shape, mint serves exactly one coord_ulid (the one whose supervisor
spawned it) and rejects calls bearing any other coord_ulid.

## Credential delivery and env hygiene

Two secrets enter the supervisor at startup: the Tigris admin
credential and (generated inside the supervisor) the mint-RPC token.
Each must reach exactly one downstream process and no further.

**Admin credentials (`AWS_*`).**

- Standalone: supervisor reads them from its own env. Before
  `execvp("elide-coord", ...)`, supervisor `unsetenv`s
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
  (and any other admin env per
  [`project_iam_env_var_convention`](../README.md)).
- systemd: supervisor reads them from the
  `LoadCredential=admin-creds:...` path. They are never in the unit's
  env at all; coord cannot see them regardless of scrubbing.
- Mint receives them via inherited fd from the supervisor (not env), so
  there is no env entry to leak even within the mint process.
- Coord asserts on startup that admin env vars are absent. Defence in
  depth against future regressions or misconfigurations.

**Mint-RPC token.**

- Generated by the supervisor in memory. Never read from env or disk.
- Delivered to both children via inherited file descriptor — each child
  reads its end, then closes the fd. The token never appears in any
  process's env or filesystem.
- Coord holds the token in memory and scrubs any inherited
  `ELIDE_MINT_RPC_TOKEN*` env vars (defence in depth) before spawning
  volume processes.
- Volume processes have no token and no other path to mint; an attempt
  to connect to `mint.sock` will fail at the first frame.

## Lifecycle

Mint and coord share a single lifecycle in both deployments — they are
managed by the same supervisor process running under one systemd unit
(or one terminal session, standalone).

| Event | Behaviour |
|---|---|
| Cold start | Supervisor reads creds → spawns mint → waits for "socket ready" → spawns coord (scrubbed) → `sd_notify(READY=1)` if `Type=notify` |
| Either child exits | Supervisor signals the other child, waits for clean exit, then exits itself (fail-fast) |
| Supervisor exits under systemd | `Restart=on-failure` brings the unit back; `KillMode=control-group` ensures any straggling child is reaped on stop |
| Ctrl-C in standalone foreground | Supervisor forwards SIGINT to both children; both shut down cleanly; supervisor exits |
| `systemctl stop elide-coord` | SIGTERM to whole cgroup; supervisor catches, propagates to children, exits |
| Volume processes | Unaffected by mint/coord restarts until next credential refresh |

## Cross-references

- `design-iam-key-model.md` — defines *which* keys mint manages and what
  policies attach to them. This doc does not change that inventory.
- `design-auth-model.md` — defines the macaroon construction coord uses
  for volume macaroons and operator tokens. Mint is not involved in
  macaroon issuance or verification.
- `project_iam_env_var_convention` (memory) — the `AWS_*` env-var
  convention coord currently reads. After this design, mint reads it;
  coord does not.

## Open questions

- **`ptrace_scope` hardening guidance.** A same-uid attacker with
  `CAP_SYS_PTRACE` or permissive `kernel.yama.ptrace_scope` can read the
  mint-RPC token from coord's memory. Document a recommended baseline
  (`ptrace_scope ≥ 1`) in `operations.md`; whether to enforce-or-warn
  at coord startup is a separate decision.

## Future directions

These do not affect the mint split design; they describe extensions
that compose with it.

- **External key management for volume encryption.** Volume encryption
  keys live in an external service (login service / KMS), not in coord
  and not in mint. `elide login` populates `~/.elide` with user
  identity material; `elide volume start <name>` uses that material
  to fetch the volume's encryption key directly from the login service
  and pass it to the volume process at start time. Coord and mint
  never see the encryption key. The macaroon hook for this is a
  third-party caveat on the volume-start authorisation requiring a
  fresh discharge from the login service — useful as a
  freshness/revocation gate, not as a key-transport mechanism. The
  actual key bytes are released by the login service to the
  authenticated user, not encoded in the macaroon.
- **Mint key rotation.** Today's `design-iam-key-model.md` rotation
  flow (create-new-policy → attach → detach-old → delete-old) is
  unchanged; it just moves into mint. Operator-driven admin-key
  rotation gains a clean home (mint reload / mint restart) without
  needing to touch coord.
- **Multi-host containment.** The OSS shape keeps mint per-host. The
  commercial credentialer-service variant from
  `design-iam-key-model.md` § *Central credentialer service* is the
  logical extension when fleets want admin off most hosts. The mint
  RPC surface (`mint_*`, `refresh_ro_key`, `delete_*`, `list_policies`)
  is the same shape that service would expose; the `coord_ulid`
  parameter already present on every verb is the differentiator. The
  only differences are transport (cross-host TLS instead of local Unix
  socket) and authentication (mutual TLS or an RPC token issued at
  coord onboarding, replacing the supervisor-shared startup token).
- **Macaroon root isolation.** The same supervisor pattern could host
  a third sibling process holding the macaroon root key, mirroring the
  mint split for the other privileged secret. We're not doing this in
  the OSS single-host shape: a compromised coord can forge macaroons
  regardless of where the root lives (verify is hot and must stay
  local; the symmetric MAC scheme means "can verify" ⇒ "can sign"), so
  the split gains nothing without per-coord HKDF-derived subkeys
  spanning multiple hosts. Trigger condition and the derived-subkey
  shape are recorded in `design-auth-model.md` § *Future directions* —
  *Root key in a separate signing process*. If/when adopted, it
  should be a *separate* sibling under the supervisor, not folded
  into mint — mint's narrow IAM-API surface and cold-path profile
  should not absorb the hot macaroon verify path.
