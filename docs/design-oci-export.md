# OCI Export & containerd Snapshotter (exploration)

Status: future exploration — not on the current roadmap. This doc captures the design thinking so it can be picked up without re-deriving.

`elide import` turns an OCI image into a volume. The reverse direction — publishing a volume *as* an OCI image — is interesting for two reasons: interop with existing container tooling, and the possibility of a containerd snapshotter that preserves Elide's demand-fetch and block-level dedup all the way to the container runtime.

## Squashed OCI export (interop escape hatch)

Walk the volume's ext4, emit a tar stream as a single squashed layer, synthesize an OCI manifest/config, write as `oci-archive` or push via skopeo.

Single-layer is a deliberate choice, not a compromise: Elide's block-level dedup already does the job that OCI layer sharing exists to do, so intermediate layers buy nothing and squashing gives consumers a cleaner artifact — no layer cache thrash, no secrets surviving in shadowed lowers, smaller manifests, faster pulls.

The consequence is that a vanilla docker/podman consumer loses demand-fetch: they pull the whole tar before running. Acceptable as an interop escape hatch, but it's not the interesting path — it throws away Elide's execution-side advantages the moment bytes leave Elide in tar form.

## Dual publish via OCI referrers

The same volume can be published twice to the same registry tag using the OCI referrers API:

- **Primary**: squashed OCI image, for vanilla container runtimes.
- **Referrer artifact**: Elide segment bundle, for Elide-aware consumers.

Vanilla consumers see a normal image and never notice the referrer. Elide-aware consumers discover the segment artifact via the referrers endpoint and pull segments directly — full demand-fetch, block dedup, delta updates. Same registry, same tag, two views selected by consumer capability.

The storage cost at the publish point is roughly `squashed tar + segment bundle`, but the segment side is delta-compressed against prior versions (see `findings.md`), so in practice much less than 2×.

## elide-snapshotter (the interesting path)

Nydus and SOCI exist to let containerd do lazy on-demand pulls against an OCI-shaped artifact (see `reference-nydus.md`). Elide is in a structurally better position to do the same thing because its native abstraction is already a lazy block device — no FUSE daemon emulating a filesystem on top of tar required.

### Architecture sketch

```
OCI registry
  └─ manifest (media type: application/vnd.elide.volume.v1)
      └─ segment artifacts

containerd
  └─ image resolver routes custom media type → elide-snapshotter (gRPC)
      └─ Prepare: fork base volume, expose via ublk
          └─ Mounts: ext4 /dev/ublkbN /rootfs
              └─ container runs; reads trigger demand-fetch; writes land in fork
      └─ Remove: drop fork
```

1. **Image identification.** Custom OCI media type in the manifest (`application/vnd.elide.volume.v1`). Containerd's resolver config routes anything with that media type to elide-snapshotter instead of the overlayfs snapshotter — exactly how nydus-snapshotter is wired.
2. **Pull.** The snapshotter recognizes the media type and pulls Elide segments (OCI registries accept arbitrary blob types), not tar layers. First boot already has demand-fetch — no "import" step, no pre-materialization.
3. **Prepare.** For each container, the snapshotter creates a writable replica (branched from the base volume's latest snapshot via `volume create --from`) and exposes it as a block device. Returns mount options: `ext4 /dev/ublkbN /rootfs`. "Fork" below refers to this per-container replica as a lineage concept, not a CLI command.
4. **Run.** Container runs with that rootfs. Reads trigger Elide demand-fetch; writes go to the fork. Block-level dedup is automatic across every image on the host — two containers sharing glibc share actual disk blocks even if they come from unrelated base images.
5. **Remove.** Snapshotter drops the fork. Base volume is untouched.

### Why this is cleaner than nydus/SOCI

- **No FUSE.** Present a real ext4 on a real block device; the kernel's own ext4 driver does file lookups. Nydus has to emulate a filesystem layer on top of tar chunks.
- **CoW per container is a volume-level replica.** No overlayfs stack to assemble, no whiteout semantics to re-derive.
- **Block dedup works across all images on the host**, not just those sharing an OCI base layer. Nydus dedup is chunk-level but scoped to the OCI layer chain.
- **Delta updates.** "New image version" = a few MB of changed segments pulled on first read, not a whole new layer blob. Findings put this at ~94% savings for point releases, ~86% cross-major.

### Hard parts

- **Snapshotter plugin is Go.** Containerd's plugin interface is Go-native. Write a thin Go shim that talks to the Elide volume daemon over a local socket rather than embedding Rust behind CGo. nydus-snapshotter is the right shape to copy.
- **Block device scale.** NBD caps at ~16 devices by default and burns a kernel slot per running container. ublk (Linux 5.19+) is the scalable answer and is already on the integrations sequencing table for other reasons — this path would ride on top of it rather than justify it alone.
- **Registry publish of segments.** Needs a stable on-registry layout for the segment artifact and a defined media type. Same prerequisite as dual publish, so the two paths share it.
- **Host-side runtime.** The volume runtime has to run as a system service on the container host, not just in dev VMs. Packaging/ops lift more than a design problem.

## Performance comparison

One-time costs (Elide pays, OCI doesn't):

- **Import**: OCI pull + ext4 extraction + segment packing. O(image size), disk-bound. Paid once per image version.
- **Export**: ext4 walk + tar emit. Same order of magnitude. Only paid if interop is actually needed.

Runtime (every boot — Elide wins big):

- **OCI multi-layer**: must fetch all blobs before container start (unless using a lazy-pull snapshotter), then overlayfs stacks N layers on every lookup. Cold start ≈ full image pull time.
- **Elide**: boot via block device with demand-fetch. Findings measure ~6% of a 2.1GB image touched for a full systemd boot (~130MB). Cold start is seconds, network-bound on only the bytes actually read.

Incremental updates (Elide wins bigger):

- **OCI point-release**: new layers, re-pull changed layers whole. Dedup is layer-granularity.
- **Elide**: delta fetch at block level. 94% savings on point releases, 86% cross-major.

Cross-image storage:

- **OCI**: shared base layers only; anything above diverges.
- **Elide**: block-level dedup across unrelated images. Two distros with the same glibc share blocks even without a common base layer.

Break-even for the import overhead is probably 1–2 boots; after that Elide is strictly ahead. OCI optimizes *distribution* (layer cache, parallel pull); Elide optimizes *execution* (demand-fetch, block dedup, delta updates). Import/export is the tax for living in both worlds.

## Reframing

"Export to OCI" stops being a lossy one-way conversion and becomes "publish the same volume in two formats, let the consumer pick based on what they can do." Vanilla docker sees a normal squashed image; containerd + elide-snapshotter sees a demand-fetched block-deduped volume. Same registry, same tag, same publish workflow.

The scope is comparable to nydus-snapshotter itself, so the question is less "can we" and more "when does this become the priority vs. finishing the standalone volume story first." Prerequisites (ublk, coordinator network API, registry publish of segments) overlap substantially with work already sequenced in `integrations.md`.

## Related docs

- `integrations.md` — Docker, Firecracker, CH, K8s integration targets and sequencing; ublk prerequisite.
- `reference-nydus.md` — nydus-snapshotter design, RAFS format, NRI boot-hint pipeline.
- `findings.md` — empirical demand-fetch ratios, delta compression measurements.
