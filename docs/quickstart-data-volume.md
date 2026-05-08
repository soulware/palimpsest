# Quickstart: Empty Data Volume

Create a blank writable volume, mount it from a Linux VM, write data, and let the coordinator drain segments to the store automatically.

## Prerequisites

- Rust toolchain (`cargo`)
- A Linux VM with `CONFIG_BLK_DEV_UBLK`. The repo ships [`elide-dev.yaml`](../elide-dev.yaml), a [Lima](https://lima-vm.io/) config that provisions Ubuntu 24.04 with the `ublk_drv` module pre-loaded.

```sh
limactl start --name=elide-dev ./elide-dev.yaml   # first time only
limactl start elide-dev                            # subsequent boots
```

## Build and start the coordinator

```sh
cargo build -p elide -p elide-coordinator
./target/debug/elide-coordinator serve   # leave running in a separate terminal
```

With no config file: volume state in `elide_data/`, local store in `elide_store/`.

## Create the volume

```sh
./target/debug/elide volume create --size 1G --ublk data-vol
```

`--ublk` writes a `[ublk]` section into `volume.toml` so the coordinator's
supervisor will attach a kernel block device on first start. The kernel
auto-allocates a device id; the chosen id is sticky across restarts.

## Mount from the VM

The kernel exposes the volume as `/dev/ublkbN` on the host where the
coordinator runs. To mount inside Lima, share the host device through the
VM (or run the coordinator inside the VM directly).

```sh
limactl shell elide-dev sudo mkfs.ext4 /dev/ublkb0
limactl shell elide-dev sudo mount /dev/ublkb0 /mnt
```

Format with `mkfs.ext4` on first use only; subsequent mounts skip this step.

## Write data

```sh
limactl shell elide-dev \
    sudo bash -c 'dd if=/dev/urandom of=/mnt/bigfile bs=1M count=80 && sync'
```

The WAL flushes to `pending/` once it exceeds 32 MiB; 80 MiB produces two or three segments.

## Segments drain automatically

The coordinator uploads `pending/` segments to the store on each drain tick (default: every 5 seconds). No manual step required. Check progress:

```sh
./target/debug/elide volume info data-vol
```

After drain, segments are uploaded to `elide_store/` and promoted: the volume writes `index/<ulid>.idx` (permanent LBA index) and `cache/<ulid>.body` (evictable body), then removes the `pending/` file.

## Volume directory layout

```
elide_data/by_id/<ulid>/
  wal/
    <ulid>          — active WAL (unflushed remainder between drain ticks)
  pending/          — empty between ticks; segments here are uploading
  index/
    <ulid>.idx      — LBA index section; written at flush; permanent (survives eviction)
  cache/
    <ulid>.body     — segment body; evictable once committed to store
  volume.toml       — name, size, [ublk] dev_id binding
  volume.key        — Ed25519 signing key (never uploaded)
  volume.pub        — Ed25519 public key
  volume.provenance — signed lineage (parent + extent_index); uploaded to S3
  volume.pid        — PID of running volume process
  control.sock      — volume IPC socket
  volume.lock       — exclusive lock held while running
```

## Disconnect

```sh
limactl shell elide-dev sudo umount /mnt
./target/debug/elide volume stop data-vol
```

`volume stop` flushes the WAL, halts the daemon, and detaches the ublk
device. `volume start data-vol` brings it back up.

## Troubleshooting

**Stale lock file** (if the volume process crashed and the coordinator has not yet restarted it):

```sh
rm elide_data/by_name/data-vol/volume.lock
```

The coordinator will restart the volume on the next scan.
