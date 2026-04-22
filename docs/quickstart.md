# Quickstart

Import an OCI image, fork it, and serve it over NBD.

## Prerequisites

- Rust toolchain (`cargo`)
- `mke2fs` from e2fsprogs (macOS: `brew install e2fsprogs`)
- `nbd-client` for connecting a block device (Linux only; on macOS use QEMU direct kernel boot — see [vm-boot.md](vm-boot.md))

## Build

```sh
cargo build -p elide -p elide-import -p elide-coordinator
```

Binaries land in `target/debug/`.

## Start the coordinator

The coordinator supervises volume processes, drains segments to the store, and handles imports. Run it in a dedicated terminal from the project root:

```sh
./target/debug/elide-coordinator serve
```

With no config file it defaults to `elide_data/` for volume state and `elide_store/` for local object storage — no setup needed for local development.

## Import an OCI image

```sh
./target/debug/elide volume import start ubuntu-22.04 ubuntu:22.04
# prints an import job ULID, e.g.: 01JQA3NDEKTSV4RRFFQ69G5FAV
```

Stream the import log until completion:

```sh
./target/debug/elide volume import attach 01JQA3NDEKTSV4RRFFQ69G5FAV
```

Or poll the state:

```sh
./target/debug/elide volume import status 01JQA3NDEKTSV4RRFFQ69G5FAV
# ubuntu-22.04: done
```

On Apple Silicon, `elide-import` auto-selects `arm64`.

## Check volumes

```sh
./target/debug/elide volume list
# ubuntu-22.04  01JQA3...  readonly
```

```sh
./target/debug/elide volume info ubuntu-22.04
```

## Browse the filesystem (without mounting)

```sh
./target/debug/elide volume ls ubuntu-22.04 /etc
```

## Branch a writable replica for a VM

Create a writable replica branched from the imported base:

```sh
./target/debug/elide volume create vm1 --from ubuntu-22.04
```

`--from` accepts a volume name (resolved locally or against the remote
store), a bare volume ULID, or an explicit `<vol_ulid>/<snap_ulid>` pin.
The explicit-pin form is forward-compatible — see
[design-replica-model.md](design-replica-model.md).

## Serve over NBD

By default the coordinator runs volumes in IPC-only mode (no NBD listener). To expose a volume over NBD, write the desired port to `nbd.port` in the volume directory. The supervisor reads this file at spawn time:

```sh
echo 10809 > elide_data/by_name/vm1/nbd.port
```

Since `vm1` was just created, the coordinator will start it fresh on the next scan and pick up the port. Check status:

```sh
./target/debug/elide volume status vm1
# vm1: running
```

## Connect with nbd-client

```sh
sudo nbd-client -b 4096 127.0.0.1 10809 /dev/nbd0
sudo mount /dev/nbd0 /mnt
```

`-b 4096` sets the NBD block size to 4 KiB, matching the volume's LBA size. The default (512 bytes) causes every write to be smaller than one LBA block, which defeats compression and dedup at the block level.

Or boot directly with QEMU — see [vm-boot.md](vm-boot.md).

## Take a snapshot

```sh
./target/debug/elide volume snapshot vm1
# prints the snapshot ULID
```

`vm1/snapshots/<ulid>` is now a branch point — `elide volume create vm2 --from vm1` will branch a new writable replica from the latest snapshot.

## Clean up

```sh
./target/debug/elide volume delete vm1
./target/debug/elide volume delete ubuntu-22.04
```
