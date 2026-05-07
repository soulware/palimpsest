---
status: landed
related: [design-isolation-model.md, design-operator-tokens.md, design-portable-live-volume.md]
landed_in: ../architecture.md#credential-distribution
---

# Credential distribution via macaroons

The coordinator holds the only copy of read-write S3 credentials. Volume processes receive **short-lived read-only credentials** for demand-fetch, authenticated by macaroons minted at startup.

## Macaroon model

The coordinator holds a **root key** (32 random bytes, generated at first start, stored at `<data_dir>/coordinator.root_key` mode 0600). It mints per-volume macaroons — keyed-BLAKE3 bearer tokens with a chain of typed caveats. Verification is stateless: the coordinator re-derives the expected MAC from the root key and the caveats; no token storage is needed.

Caveats are typed (Rust enum `Caveat` in `elide-coordinator/src/macaroon.rs`):

| Caveat | Value | Purpose |
|---|---|---|
| `volume` | `<volume-name>` | Binds token to this volume only |
| `scope` | `Credentials` | Limits token to credential requests |
| `pid` | `<process-pid>` | Locks token to the spawned process |
| `not-after` | `<unix-seconds>` | Optional expiry (used for attenuated tokens) |

Wire format is a single hex line over the existing IPC line protocol: `v1.<32-byte mac, hex>.<caveat blob, hex>`. The MAC is `blake3::keyed_hash(root_key, caveats_blob)`.

## Registration flow

The PID is only known after the volume is spawned, so the macaroon cannot be minted before spawn. The volume registers on startup:

1. Coordinator spawns the volume process and records PID in `volume.pid`.
2. Volume connects to `control.sock` and sends `{"verb":"register","volume_ulid":"<ulid>"}`.
3. Coordinator reads peer credentials from the Unix socket → obtains peer PID.
4. Coordinator cross-checks: is this PID the one recorded in `volume.pid` for `<volume>`? If not, replies `Envelope::Err { kind: "forbidden" }`.
5. Coordinator mints a macaroon with the caveats above (including `pid = <peer-pid>`) and replies with it.
6. Volume holds the macaroon in memory for all subsequent `credentials` requests.

The supervisor writes `volume.pid` immediately after `Command::spawn()` returns, but a fast-starting volume can reach step 2 before that write completes. The volume retries `register` a handful of times on `forbidden` to absorb that window.

## Credential exchange flow

When the volume needs S3 credentials (at startup, after eviction, or before expiry):

1. Volume sends `{"verb":"credentials","macaroon":"…"}` to `control.sock`.
2. Coordinator verifies the HMAC chain (proves it minted this token).
3. Coordinator checks all caveats: volume matches, scope is `Credentials`, `pid` matches `SO_PEERCRED` of the current connection.
4. Coordinator delegates to the configured `CredentialIssuer` (see *Issuer backends* below).
5. Replies with `{access_key, secret_key, session_token, expiry_unix}`.

The PID check on every request (step 3) means the macaroon is useless even if exfiltrated — it can only be presented from the original process. The HMAC chain means no volume can forge a token for a different volume.

## Issuer backends

`CredentialIssuer` is the trait used by the inbound `credentials` handler to turn an authenticated request into an S3 access triple (`elide-coordinator/src/credential.rs`). The macaroon handshake runs identically for every backend — only the credentials returned in step 5 change.

**`SharedKeyPassthrough` — landed.** Returns the coordinator's own configured access key for every volume, with no per-volume IAM scoping. Logs a downgrade warning at startup. This is the minimum-viable issuer and is equivalent to the previous `get-store-creds` behaviour reached through the macaroon handshake.

**Per-volume scoped issuers — future.** Slot in behind the same trait without changing the handshake:

| Backend | Mechanism |
|---|---|
| AWS S3 | STS `AssumeRole` with a session policy narrowing `s3:GetObject` to `arn:aws:s3:::<bucket>/by_id/<volume-ulid>/*`. Session 15 min – 12 h. |
| S3-compatible with STS (MinIO, Ceph RGW/STS) | `AssumeRole` against the backend's STS endpoint with the same session policy shape. |
| Tigris (no STS, IAM-policied keys) | `CreateAccessKey` per volume with a policy granting `s3:GetObject` on the volume's prefix and a `DateLessThan` condition for time-bounding; `DeleteAccessKey` on rotation. Coordinator's admin key needs `iam:CreateAccessKey` / `iam:DeleteAccessKey` / `iam:PutAccessKeyPolicy` — strictly stronger than today's S3 RW key, guarded accordingly. |

Tigris-native is the per-volume upgrade path on Tigris; the shared-key downgrade row remains valid for single-tenant dev.

**Local filesystem.** No-op issuer used in tests and single-host deployments — returns a sentinel the volume's fetcher treats as "no auth needed".

## Token lifetime and revocation

The macaroon lives for the lifetime of the volume process. Revocation is implicit: when the coordinator stops a volume, the PID is no longer live, and any subsequent `credentials` request with the old macaroon fails the `SO_PEERCRED` check. No revocation list is needed, given coordinator and volumes share a host.

If the coordinator is ever moved off-host (not a current goal), `SO_PEERCRED` would be unavailable and explicit revocation would need to be designed.

## Attenuation

Macaroons are additive-restriction-only, so the volume can narrow its token before passing to a subprocess (e.g. an out-of-process demand-fetch helper):

```
original:   volume=myvm, scope=credentials, pid=1234
attenuated: volume=myvm, scope=credentials, pid=1234, not-after=<+5m>
```

The attenuated token is derived in-process — no coordinator round-trip. The coordinator verifies all caveats including the narrowed `not-after`.

## Refresh and clock skew

The fetcher holds a `CredentialProvider` that re-issues `credentials` when the remaining lifetime drops below 10% or 60 s, whichever is greater. Refresh is lazy — driven by the next fetch request, not a background timer — so an idle volume holds stale credentials until next use.

A fetch that receives HTTP 403 retries once after forcing a credential refresh. This absorbs clock skew between coordinator, volume, and the backend's signing check. A second 403 propagates as a fetch error. In-flight fetches under the old credentials are not cancelled on refresh — they either succeed under the old signature or fail and retry with the new one.

## Standalone mode (no coordinator)

`serve-volume` accepts `--s3-access-key`, `--s3-secret-key`, `--s3-session-token` for direct credential passing. No macaroon is involved. This supports the fully standalone tier and is also useful for development.
