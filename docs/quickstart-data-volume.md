# Quickstart: Data Volume with Multipass

Create an empty writable volume, mount it from a Linux VM, write data, and
upload the resulting segments to an object store.

This is distinct from the OCI rootfs workflow: there is no base image to import,
no fork step, and the volume starts empty. A Multipass VM is used as a
convenient Linux environment with `nbd-client` available.

## Prerequisites

- Rust toolchain (`cargo`)
- [Multipass](https://multipass.run/) with a running Ubuntu VM

```sh
# Create a VM if you don't have one
multipass launch --name elide-test
```

## Build

```sh
cargo build -p elide -p elide-coordinator
```

## Create and serve the volume

`serve-volume` creates the directory structure on first use when `--size` is
provided. Pick a size appropriate for your test data.

```sh
./target/debug/elide serve-volume /tmp/elide-test/data-vol default \
    --size 1G --bind 0.0.0.0
```

Leave this running in a separate terminal. The volume is at
`/tmp/elide-test/data-vol/forks/default/`.

## Connect from the VM

Find the host IP from inside the VM (the default gateway):

```sh
multipass exec elide-test -- \
    ip route show default | awk '{print $3}'
# typically 192.168.2.1 or 192.168.64.1 depending on the Multipass backend
```

Connect `nbd-client`, format, and mount:

```sh
multipass exec elide-test -- sudo nbd-client <host-ip> 10809 /dev/nbd0
multipass exec elide-test -- sudo mkfs.ext4 /dev/nbd0
multipass exec elide-test -- sudo mount /dev/nbd0 /mnt
```

## Write data

Writes go to the WAL immediately. The WAL flushes to a segment in `pending/`
automatically once it exceeds 32 MiB — write enough to cross that threshold at
least once:

```sh
multipass exec elide-test -- \
    sudo bash -c 'dd if=/dev/urandom of=/mnt/bigfile bs=1M count=80 && sync'
```

80 MiB crosses the 32 MiB threshold twice, producing two segments in `pending/`
with the remainder still in `wal/`.

After the write completes the volume directory looks like:

```
/tmp/elide-test/data-vol/forks/default/
  wal/
    <ulid>          — remainder not yet flushed (~16 MiB)
  pending/
    <ulid>          — ready for upload (32 MiB)
    <ulid>          — ready for upload (32 MiB)
  segments/         — empty until drain-pending runs
  volume.lock
```

## Unmount and disconnect

```sh
multipass exec elide-test -- sudo umount /mnt
multipass exec elide-test -- sudo nbd-client -d /dev/nbd0
```

Then stop `serve-volume` (Ctrl-C). The WAL and any unflushed data are safe —
`Volume::open` replays the WAL on next open.

## Upload pending segments

Use `drain-pending` to upload everything in `pending/` and commit each segment
to `segments/` on success. Each segment is handled independently.

**Against a local directory (no server needed):**

```sh
./target/debug/elide-coordinator --local /tmp/elide-store \
    drain-pending /tmp/elide-test/data-vol/forks/default
# 2 uploaded, 0 failed
```

Objects land at `/tmp/elide-store/data-vol/default/YYYYMMDD/<ulid>`.

**Against a real S3-compatible store (MinIO, Tigris, AWS S3):**

```sh
export ELIDE_S3_BUCKET=elide
export AWS_ENDPOINT_URL=http://localhost:9000   # omit for real AWS S3
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

./target/debug/elide-coordinator \
    drain-pending /tmp/elide-test/data-vol/forks/default
```

## Verify

After a successful drain:

```
/tmp/elide-test/data-vol/forks/default/
  wal/
    <ulid>          — unflushed remainder (unchanged)
  pending/          — empty
  segments/
    <ulid>          — committed after upload
    <ulid>          — committed after upload
```

The remaining WAL data will flush to `pending/` on the next write session that
pushes the WAL back over 32 MiB. Run `drain-pending` again after that to upload
the new segments.

## Troubleshooting

**`nbd-client -d` leaves the device in a bad state:**

```sh
multipass exec elide-test -- sudo rmmod nbd
multipass exec elide-test -- sudo modprobe nbd
```

**Volume won't open (stale lock):** if `serve-volume` crashed, remove the lock:

```sh
rm /tmp/elide-test/data-vol/forks/default/volume.lock
```

**Want to start fresh:** delete the volume directory and re-run `serve-volume`:

```sh
rm -rf /tmp/elide-test/data-vol
```
