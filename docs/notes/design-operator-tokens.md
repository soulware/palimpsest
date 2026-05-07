---
status: proposed
related: [design-credential-macaroons.md, design-isolation-model.md]
---

# Operator tokens

Operator tokens are macaroons minted by the coordinator for human operators (CLI usage). They are not PID-bound — identity is carried by the token itself, enabling audit logging and attenuation.

**Issuance:**

```
elide-coordinator token create [--expires 24h] [--volume <name>]
```

Prints a macaroon to stdout. The operator stores it in `~/.elide/operator-token` or passes it explicitly. The coordinator logs token creation with a unique nonce.

**Caveats on an operator token:**

| Caveat | Value | Purpose |
|---|---|---|
| `role` | `operator` | Distinguishes from volume tokens |
| `not-after` | `<expiry>` | Required; no indefinite operator tokens |
| `volume` | `<name>` | Optional; restrict to a specific volume |

**How the CLI uses it:**

The `elide` CLI locates the operator token in this order:
1. `--token <value>` flag
2. `ELIDE_OPERATOR_TOKEN` environment variable
3. `~/.elide/operator-token` file

It is presented with any coordinator mutation that requires one (currently: `delete`).

**Attenuation:** an operator can narrow their token before sharing it — for example, scoping it to a single volume or shortening the expiry — without involving the coordinator. The coordinator verifies all caveats on receipt.

**Audit log:** the coordinator logs every operator-token-authenticated operation with the token's nonce, the operation, and the timestamp. This provides a trail of who did what and when, traceable back to the `token create` event.
