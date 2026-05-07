---
status: descriptive
related: [design-credential-macaroons.md, design-operator-tokens.md]
---

# Isolation model

Volume processes on the same host share a uid and a filesystem. This bounds what the macaroon scheme does and does not protect against.

**Local filesystem — not enforced.** A compromised volume process can read or corrupt any other volume's local directory directly, without touching the coordinator. Macaroons provide no protection here. Proper local isolation requires OS-level mechanisms: separate uids per volume, Linux user namespaces, or running each volume in its own container. This is a separate layer and is not addressed by the current design.

**S3 — enforced (when per-volume issuer is configured).** S3 credentials can be scoped by IAM to a specific volume's prefix. The enforcement is external to Elide — AWS (or equivalent) rejects requests that exceed the credential's scope regardless of what the caller claims. The macaroon scheme ensures a volume process can only obtain credentials for its own volume, so a compromised `myvm` cannot request credentials for `othervm`. With the default `SharedKeyPassthrough` issuer this is a downgrade — every volume gets the same shared read-only key — but the handshake still binds the requester to a volume and refuses cross-volume requests at the coordinator. See [design-credential-macaroons.md](design-credential-macaroons.md) for the issuer surface.

**Coordinator mutations — defense-in-depth.** Requiring a volume-scoped macaroon for coordinator mutations raises the bar slightly over bare socket access, and provides an audit trail. It does not prevent a compromised process from achieving the same effect via direct filesystem manipulation. The value here is auditability and protocol clarity, not a hard security boundary.

**Summary:**

| Resource | Isolation mechanism | Enforced by |
|---|---|---|
| S3 data | IAM credential scoping + macaroon gating | Object store + coordinator |
| Local filesystem | uid separation / namespacing | OS (not yet implemented) |
| Coordinator mutations | Macaroon + audit log | Coordinator (defense-in-depth) |
