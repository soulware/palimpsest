# Quickstart: Empty Data Volume

Create a blank writable volume, mount it from a Linux VM, write data, and let the coordinator drain segments to the store automatically.

## Prerequisites

- Rust toolchain (`cargo`)
- A Linux VM with `nbd-client` available (Multipass is convenient; any VM with host network access works)

```sh
multipass launch --name elide-test   # if you don't have one already
```

## Build and start the coordinator

```sh
cargo build -p elide -p elide-coordinator
./target/debug/elide-coordinator serve   # leave running in a separate terminal
```

With no config file: volume state in `elide_data/`, local store in `elide_store/`.

## Create the volume

```sh
./target/debug/elide volume create --size 1G data-vol
```

## Enable NBD

Write the desired port to `nbd.port`. The coordinator reads this at spawn time and passes `--port` to the volume process:

```sh
echo 10809 > elide_data/by_name/data-vol/nbd.port
./target/debug/elide volume status data-vol   # wait until "running"
```

## Connect from the VM

Find the host IP from inside the VM (the default gateway), then connect and format:

```sh
HOST_IP=$(multipass exec elide-test -- ip route show default | awk '{print $3}')
# typically 192.168.64.1 or 192.168.2.1 depending on the Multipass backend

multipass exec elide-test -- sudo nbd-client -b 4096 $HOST_IP 10809 /dev/nbd0
multipass exec elide-test -- sudo mkfs.ext4 /dev/nbd0
multipass exec elide-test -- sudo mount /dev/nbd0 /mnt
```

Format with `mkfs.ext4` on first use only; subsequent mounts skip this step.

## Write data

```sh
multipass exec elide-test -- \
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
  volume.name       — "data-vol"
  volume.size       — "1073741824"
  volume.key        — Ed25519 signing key (never uploaded)
  volume.pub        — Ed25519 public key
  volume.provenance — signed lineage (parent + extent_index); uploaded to S3
  volume.pid        — PID of running volume process
  nbd.port          — "10809"
  control.sock      — volume IPC socket
  volume.lock       — exclusive lock held while running
```

## Disconnect

```sh
multipass exec elide-test -- sudo umount /mnt
multipass exec elide-test -- sudo nbd-client -d /dev/nbd0
```

The coordinator keeps the volume process running after disconnect. Reconnect with `nbd-client` at any time.

## Troubleshooting

**`nbd-client -d` leaves the device in a bad state:**

```sh
multipass exec elide-test -- sudo rmmod nbd
multipass exec elide-test -- sudo modprobe nbd
```

**Stale lock file** (if the volume process crashed and the coordinator has not yet restarted it):

```sh
rm elide_data/by_name/data-vol/volume.lock
```

The coordinator will restart the volume on the next scan.
