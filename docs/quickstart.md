# Quickstart

Import an OCI image, fork it, and serve it over ublk.

## Prerequisites

- Rust toolchain (`cargo`)
- `mke2fs` from e2fsprogs (macOS: `brew install e2fsprogs`)
- Linux with `CONFIG_BLK_DEV_UBLK` for the block device (on macOS use QEMU direct kernel boot — see [vm-boot.md](vm-boot.md))

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

## Branch a writable replica for a VM

Create a writable replica branched from the imported base:

```sh
./target/debug/elide volume create vm1 --from ubuntu-22.04
```

`--from` accepts a volume name (resolved locally or against the remote
store), a bare volume ULID, or an explicit `<vol_ulid>/<snap_ulid>` pin.
The explicit-pin form is forward-compatible — see
[design-replica-model.md](design-replica-model.md).

## Serve the volume

By default the coordinator runs volumes in IPC-only mode (no host-visible
block device). Attach the ublk transport to expose `/dev/ublkbN`:

```sh
sudo modprobe ublk_drv     # one-time kernel module load
./target/debug/elide volume update vm1 --ublk
```

This writes `[ublk]` to `volume.toml`; the kernel auto-allocates a device id
on first start (recorded in `volume.toml` for crash recovery). Then:

```sh
sudo mount /dev/ublkb0 /mnt
```

Or boot directly with QEMU — see [vm-boot.md](vm-boot.md).

## Take a snapshot

```sh
./target/debug/elide volume snapshot vm1
# prints the snapshot ULID
```

`vm1/snapshots/<ulid>` is now a branch point — `elide volume create vm2 --from vm1` will branch a new writable replica from the latest snapshot.

## Clean up

```sh
./target/debug/elide volume stop vm1
./target/debug/elide volume remove vm1
./target/debug/elide volume stop ubuntu-22.04
./target/debug/elide volume remove ubuntu-22.04
```
