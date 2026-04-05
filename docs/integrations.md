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
│             nbd (now) → ublk (target)                │
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

Two pieces of infrastructure unblock all integration targets:

- **Unix socket NBD** (done) — binds an `UnixListener` instead of TCP; avoids
  port allocation for local testing and host-side tooling (qemu-nbd, nbd-client
  on the host). See `--nbd-socket`.
- **ublk backend** — replaces nbd-client with a direct io_uring block device.
  Eliminates the kernel nbd module and socket-per-I/O overhead. `/dev/ublkb0`
  looks like a regular block device to every integration target.
- **Coordinator network API** — the coordinator currently speaks a Unix socket
  line protocol. A network-accessible API (HTTP/gRPC) is required for Docker
  volume plugin, Firecracker orchestration, and CSI.

## Docker

**Rootfs:** OCI images imported via `elide import` are already usable as Docker
images — this is the primary use case and requires no block device integration.

**Data volumes (current):**
```
Elide (nbd/ublk) → /dev/nbd0 → mount → /mnt/vol
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
Docker calls the plugin to mount/unmount; Elide handles the nbd/ublk attach and
mount internally. Requires the coordinator network API.

## Firecracker

Firecracker does not expose vsock or vhost-user — it is intentionally minimal.
All block device management is host-side. The VM never speaks NBD.

**Rootfs:**
```
Elide NBD server  ←—— Unix socket (host-local)
      ↓
nbd-client (host) → /dev/nbd0
      ↓
Firecracker boot drive config
      ↓
VM boots from /dev/nbd0 as rootfs
```
The Unix socket NBD path (`--nbd-socket`) is ideal here: no port allocation,
host-local only. When ublk is available, `/dev/ublkb0` replaces `/dev/nbd0`
with lower overhead.

**Data volumes (hotplug):**
```
Elide → nbd-client/ublk → /dev/nbd1
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
API, host-side nbd/ublk. Treat them the same until vhost-user is a priority.

**vhost-user-blk (future):**

Cloud Hypervisor supports the vhost-user protocol. Elide can act as a
vhost-user-blk backend directly, removing the VMM from the data path:
```
VM → vhost-user shared memory rings → Elide vhost-user-blk backend
```
Combined with ublk on the host-storage side, this gives the shortest I/O path:
VM shared memory → Elide → io_uring → storage. No VMM process, no kernel nbd
module in the hot path.

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
│         ↓  unix socket NBD (now)              │
│         ↓  ublk (target)                      │
│  /dev/nbd0 or /dev/ublkb0                     │
│         ↓                                     │
│  mkfs / mount → workload reads & writes       │
│         ↓                                     │
│  segments on VM local disk                    │
│         ↓  (when S3 is wired up)              │
│  upload to S3 → durable across VM restarts    │
└───────────────────────────────────────────────┘
```

**Current path (NBD unix socket):**
```
elide volume serve --nbd-socket ./vol/nbd.sock
nbd-client -unix ./vol/nbd.sock /dev/nbd0
mount /dev/nbd0 /mnt/data
```
Requires the `nbd` kernel module. Present in most general-purpose Linux
kernels; may be absent in stripped-down microVM images.

**Target path (ublk):**

ublk replaces `nbd-client` with a direct io_uring block device. No kernel
module required beyond the ublk driver (Linux 5.19+). Elide spawns the ublk
device itself; `/dev/ublkb0` appears as a regular block device. The in-VM case
is identical to the host-side case — ublk work done for any target immediately
benefits this one.

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

- Requires `nbd` kernel module (today) or Linux 5.19+ (ublk target).
- `CAP_SYS_ADMIN` is needed to attach the block device (`nbd-client` or ublk).
  This is standard for any block-device operation and is available to root
  inside most VMs.
- The coordinator and volume serve run as persistent processes in the VM; a
  process supervisor (systemd, s6) is recommended for production use.

## Kubernetes (deferred)

CSI (Container Storage Interface) is the standard for Kubernetes storage. A CSI
driver has two components:

- **Controller plugin** (Deployment) — cluster-wide volume lifecycle, talks to
  the Elide coordinator network API.
- **Node plugin** (DaemonSet) — per-node attach/mount, runs nbd/ublk locally.

| CSI call | Elide action |
|---|---|
| `CreateVolume` | Coordinator creates a new volume |
| `DeleteVolume` | Coordinator deletes volume |
| `CreateSnapshot` | Coordinator creates a fork — direct mapping |
| `NodeStageVolume` | ublk/nbd → block device → mount to staging path |
| `NodePublishVolume` | Bind mount staging path → pod volume path |

Access mode mapping:

| K8s access mode | Elide model |
|---|---|
| `ReadWriteOnce` (RWO) | Mutable fork, single node |
| `ReadOnlyMany` (ROX) | Readonly fork, multiple nodes |
| `ReadWriteMany` (RWX) | Not supported (single-writer) |

Elide's fork model maps directly to CSI snapshots — `CreateSnapshot` is
essentially free given the existing data model.

The node plugin needs `CAP_SYS_ADMIN` and either the nbd kernel module or a
Linux 5.19+ kernel for ublk.

## Sequencing

| Step | Work | Unblocks |
|---|---|---|
| 1 | Unix socket NBD (`--nbd-socket`) | Local testing, Firecracker rootfs without port allocation, in-VM self-contained |
| 2 | Coordinator network API | Docker volume plugin, Firecracker orchestration, CSI |
| 3 | ublk backend | All targets — lower latency, no nbd-client process; in-VM path: no nbd module required |
| 4 | Firecracker integration | Firecracker rootfs + data volumes via hotplug |
| 5 | Docker volume plugin | Docker data volumes with lifecycle management |
| 6 | Cloud Hypervisor / vhost-user-blk | CH data volumes, shortest I/O path |
| 7 | CSI driver | Kubernetes |
