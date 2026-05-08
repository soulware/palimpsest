# Integration Targets

Elide is designed to serve block volumes to a range of compute targets: containers,
microVMs, and eventually Kubernetes. Each target requires a different integration
layer, but they all share the same storage core.

## Layered architecture

```
┌──────────────────────────────────────────────────────┐
│                  Integration layer                    │
│  Docker          Firecracker   Cloud Hypervisor  K8s │
│  volume plugin   hotplug API   vhost-user-blk   CSI  │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│             Block device exposure                     │
│             ublk (/dev/ublkb<N>)                     │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│             Coordinator + network API                 │
│             (prerequisite for all multi-node targets) │
└────────────────────────┬─────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────┐
│             Volume storage core                       │
│             WAL, segments, GC, forks                 │
└──────────────────────────────────────────────────────┘
```

The integration layers are thin. The CSI node plugin and the Firecracker
integration do essentially the same thing — attach a volume and expose it as a
block device to a workload — over different APIs.

## Common prerequisites

- **ublk backend** — Elide exposes each volume as a `/dev/ublkb<N>` device via
  the kernel ublk driver (`CONFIG_BLK_DEV_UBLK`, Linux 6.0+; 6.5+ for
  unprivileged mode). The device looks like a regular block device to every
  integration target.
- **Coordinator network API** — the coordinator currently speaks a Unix socket
  line protocol. A network-accessible API (HTTP/gRPC) is required for Docker
  volume plugin, Firecracker orchestration, and CSI.

## Docker

**Rootfs:** OCI images imported via `elide import` are already usable as Docker
images — this is the primary use case and requires no block device integration.
The reverse direction (publishing a volume *as* an OCI image, and a containerd
snapshotter that preserves demand-fetch end-to-end) is captured as future
exploration in [`design-oci-export.md`](design-oci-export.md).

**Data volumes (current):**
```
Elide (ublk) → /dev/ublkb0 → mount → /mnt/vol
docker run -v /mnt/vol:/data ...
```
A bind mount from a host-mounted volume into the container. Works with any
container runtime. Host-side only; the container sees a regular directory.

**Data volumes (future — Docker volume plugin):**

Elide implements the Docker volume plugin API (a JSON protocol over a Unix
socket). Users then do:
```
docker volume create --driver elide myvolume
docker run -v myvolume:/data ...
```
Docker calls the plugin to mount/unmount; Elide handles the ublk attach and
mount internally. Requires the coordinator network API.

## Firecracker

Firecracker does not expose vsock or vhost-user — it is intentionally minimal.
All block device management is host-side.

**Rootfs:**
```
Elide ublk → /dev/ublkb0
      ↓
Firecracker boot drive config
      ↓
VM boots from /dev/ublkb0 as rootfs
```

**Data volumes (hotplug):**
```
Elide ublk → /dev/ublkb1
      ↓
Firecracker PATCH /drives API
      ↓
VM sees new virtio-blk device → mount inside VM
```
The coordinator calls the Firecracker management API to hotplug the device.
Requires the coordinator network API so the coordinator knows which Firecracker
instance each volume is attached to.

**Control plane (vsock — future):**

Firecracker supports virtio-vsock. If the guest needs to self-request volume
attachments (without a host-side orchestrator knowing upfront), vsock provides
a clean control channel:
```
VM: "attach volume X" → AF_VSOCK → host Elide coordinator
Coordinator: attach volume → hotplug virtio-blk → VM sees new device
```
vsock is a *control plane* mechanism only. The data path is still virtio-blk
via hotplug. This is worth pursuing if VMs need to declare their own storage
requirements at runtime rather than having the host orchestrate everything.

## Cloud Hypervisor

For basic use cases (rootfs, data volume hotplug), Cloud Hypervisor is
effectively identical to Firecracker — virtio-blk hotplug via the management
API, host-side ublk. Treat them the same until vhost-user is a priority.

**vhost-user-blk (future):**

Cloud Hypervisor supports the vhost-user protocol. Elide can act as a
vhost-user-blk backend directly, removing the VMM from the data path:
```
VM → vhost-user shared memory rings → Elide vhost-user-blk backend
```
Combined with ublk on the host-storage side, this gives the shortest I/O path:
VM shared memory → Elide → io_uring → storage.

This requires implementing the vhost-user-blk backend protocol in Elide — a
meaningful piece of work, but a natural fit alongside ublk.

## In-VM (self-contained)

A hosted VM — a cloud instance, a dev machine, a CI runner — already has a
local filesystem and can run arbitrary userspace processes. Elide can run
entirely inside such a VM, with no hypervisor cooperation and no host-side
tooling. The VM self-serves its own volumes: creating them, writing to them,
forking snapshots, and uploading segments to S3 for durability.

```
┌─ VM ──────────────────────────────────────────┐
│  elide coordinator + volume serve             │
│         ↓  ublk                               │
│  /dev/ublkb0                                  │
│         ↓                                     │
│  mkfs / mount → workload reads & writes       │
│         ↓                                     │
│  segments on VM local disk                    │
│         ↓  (when S3 is wired up)              │
│  upload to S3 → durable across VM restarts    │
└───────────────────────────────────────────────┘
```

Elide spawns the ublk device itself; `/dev/ublkb0` appears as a regular block
device. The in-VM case is identical to the host-side case.

**What this unlocks:**

- **Durable scratch volumes.** VM instance storage is ephemeral; S3-backed Elide
  volumes survive VM termination and can be re-attached to a replacement instance.
- **Cheap snapshots.** Fork a volume before a risky operation; roll back by
  discarding the fork. No filesystem-level snapshot support required from the
  hypervisor.
- **Multi-volume isolation.** Multiple independent volumes inside one VM, each
  with its own write history and fork tree, without needing multiple block
  devices from the hypervisor.
- **Self-service from inside the VM.** No host-side agent, no privileged
  sidecar, no hypervisor API. The workload manages its own storage lifecycle via
  the coordinator.

**Constraints:**

- Requires Linux 6.0+ with `CONFIG_BLK_DEV_UBLK` (6.5+ for unprivileged mode).
- `CAP_SYS_ADMIN` is needed to attach the block device. This is standard for
  any block-device operation and is available to root inside most VMs.
- The coordinator and volume serve run as persistent processes in the VM; a
  process supervisor (systemd, s6) is recommended for production use.

## Kubernetes (deferred)

CSI (Container Storage Interface) is the standard for Kubernetes storage. A CSI
driver has two components:

- **Controller plugin** (Deployment) — cluster-wide volume lifecycle, talks to
  the Elide coordinator network API.
- **Node plugin** (DaemonSet) — per-node attach/mount, runs ublk locally.

| CSI call | Elide action |
|---|---|
| `CreateVolume` | Coordinator creates a new volume |
| `DeleteVolume` | Coordinator deletes volume |
| `CreateSnapshot` | Coordinator creates a fork — direct mapping |
| `NodeStageVolume` | ublk → block device → mount to staging path |
| `NodePublishVolume` | Bind mount staging path → pod volume path |

Access mode mapping:

| K8s access mode | Elide model |
|---|---|
| `ReadWriteOnce` (RWO) | Mutable fork, single node |
| `ReadOnlyMany` (ROX) | Readonly fork, multiple nodes |
| `ReadWriteMany` (RWX) | Not supported (single-writer) |

Elide's fork model maps directly to CSI snapshots — `CreateSnapshot` is
essentially free given the existing data model.

The node plugin needs `CAP_SYS_ADMIN` and a Linux 6.5+ kernel with
`CONFIG_BLK_DEV_UBLK` for unprivileged ublk.

## Sequencing

| Step | Work | Unblocks |
|---|---|---|
| 1 | Coordinator network API | Docker volume plugin, Firecracker orchestration, CSI |
| 2 | Firecracker integration | Firecracker rootfs + data volumes via hotplug |
| 3 | Docker volume plugin | Docker data volumes with lifecycle management |
| 4 | Cloud Hypervisor / vhost-user-blk | CH data volumes, shortest I/O path |
| 5 | CSI driver | Kubernetes |
