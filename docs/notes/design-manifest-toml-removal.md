# Drop `manifest.toml` entirely

**Status:** Implemented.

Follow-up to [design-volume-size-ownership.md](design-volume-size-ownership.md). With `size` relocated to `NameRecord`, every remaining field on `manifest.toml` was either redundant with an existing surface or migrated onto a signed one, so the file is gone.

## What's left on `manifest.toml`

| Field | Fate |
|---|---|
| `name` | Drop. Reverse lookup is `names/<name>`; nothing reads `name` from `manifest.toml`. |
| `readonly` | Drop. Implied by the `volume.readonly` marker locally and by `NameRecord.state` in the bucket. |
| `origin` | Drop. Redundant with the signed `volume.provenance.parent`. |
| `source` (OCI image / digest / arch) | Move to `volume.provenance` as a new optional `oci_source` field on `ProvenanceLineage`. |

## `oci_source` belongs in `volume.provenance`

`source` is the only `manifest.toml` field that carries information not already represented elsewhere. The natural home is `volume.provenance`:

- Same trust level as the rest of provenance. "This volume was built from `foo@sha256:…`" is a static lineage claim, and `volume.provenance` is the existing signed home for static lineage facts (`parent`, `extent_index`). Today the OCI source claim has no cryptographic anchor at all.
- Already pulled on every cross-host skeleton fetch (one of the two GETs after the size move). Operator inspection — `elide volume inspect` showing the OCI label — keeps working without a new S3 surface.
- `ProvenanceLineage` already mixes optional fields with "present iff this kind of root" semantics (`parent` for forks, `extent_index` for `--extents-from` imports). `oci_source` slots in alongside them as "present iff OCI-imported root".

### Inheritance

`oci_source` lives only on the import root. Forks do not inherit it: the field describes how a *root* was built, not how a child branched, and a fork's `parent` chain already points back to the importing root for tools that want to walk up. `elide volume inspect` showing the OCI label on a fork costs one provenance read of the import root if we want it; it is otherwise unsupported.

### Shape

```rust
pub struct OciSource {
    pub image: String,   // e.g. "docker.io/library/ubuntu:24.04"
    pub digest: String,  // sha256:... of the resolved platform manifest
    pub arch: String,    // "amd64" / "arm64" / ...
}

pub struct ProvenanceLineage {
    pub parent: Option<ParentRef>,
    pub extent_index: Vec<String>,
    pub oci_source: Option<OciSource>,  // present iff OCI-imported root
}
```

Signing-input encoding follows the same convention as `parent_manifest_pubkey`: stable line ordering, lines absent when the field is `None`, so a root-without-OCI-source signs to the same input it does today.

## Migration

Same story as the size relocation: the signing input changes, so existing signatures don't validate. Fresh-bucket-only. No back-compat shim, no fallback path that reads `manifest.toml` on parse failure.

## Cleanup punch list

**Code**

- `elide-coordinator/src/upload.rs`: drop `Manifest` / `ManifestSource` structs, `upload_manifest()`, the call site in `upload_volume_metadata`, the `uploaded/manifest.toml` sentinel handling, and the manifest-shape tests (`drain_pending_uploads_manifest_…`, the no-rewrite test, etc.).
- `elide-import/src/main.rs`: have the importer populate `oci_source` on the volume's `ProvenanceLineage` directly. Drop the local `meta.toml` write that today exists only to feed the uploader.
- `elide-core/src/signing.rs`: extend `ProvenanceLineage` with `oci_source`, extend the canonical signing-input encoding, extend the parser. New round-trip test covering the `oci_source = Some(...)` case.
- `elide-coordinator/src/inbound.rs`: stale comment ("Mirrors the CLI's `pull_one_readonly`: downloads `manifest.toml`") and timing-log line ("manifest …").
- `elide-coordinator/src/retention.rs`: the `"by_id/.../manifest.toml"` example in the wrong-shape test fixture; swap to a still-real key like `volume.pub`.

**Docs**

- `docs/architecture.md`: directory layout box, "S3 upload for volume metadata" para, "Import procedure" para.
- `docs/formats.md`: S3 object layout section.
- Scrub `manifest.toml` mentions in `docs/overview.md`, `docs/findings.md`, `docs/operations.md`, `docs/design-replica-model.md`, `docs/design-consistency-surface.md`, `docs/design-gc-self-describing-handoff.md`, `docs/quickstart-tigris.md`.
- `docs/design-volume-size-ownership.md`: flip the "could be dropped entirely as a follow-up" line to "dropped — see [design-manifest-toml-removal.md]" once this lands.

## Tradeoffs

- **Pro:** every per-volume metadata file in S3 is now signed or a public key. No unsigned correctness-or-provenance-relevant field anywhere in the per-volume skeleton.
- **Pro:** one fewer S3 surface to author, upload, gate, and reason about. Removes the `uploaded/manifest.toml` sentinel — content-equality gate that only ever applied to this one file.
- **Pro:** OCI provenance becomes verifiable. A volume claiming to be `foo@sha256:abc` either signs it or it doesn't.
- **Con:** OCI label on a forked volume needs an extra provenance walk to display. Today it's one local read of `manifest.toml`. Acceptable — `volume inspect` is not a hot path.
