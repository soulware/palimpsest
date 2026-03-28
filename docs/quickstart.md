# Quickstart

Import an OCI image, fork it, and serve it over NBD.

## Prerequisites

- Rust toolchain (`cargo`)
- `mke2fs` from e2fsprogs (macOS: `brew install e2fsprogs`)

## Build

```sh
cargo build -p elide -p elide-import
```

Binaries land in `target/debug/`.

## Import an OCI image

```sh
./target/debug/elide-import ubuntu:22.04 /tmp/elide-test/ubuntu-22.04
```

This pulls the image, builds an ext4 rootfs, and writes a readonly base volume. On Apple Silicon it auto-selects `arm64`; use `--arch amd64` to override.

The result:

```
/tmp/elide-test/ubuntu-22.04/
  meta.toml          — OCI source, digest, arch; readonly = true
  size               — volume size in bytes
  base/
    segments/        — imported data
    snapshots/
      <ulid>         — branch point for forks
```

## Fork

Create a writable fork from the base:

```sh
./target/debug/elide fork-volume /tmp/elide-test/ubuntu-22.04 vm1
```

The fork lands at `forks/vm1/` and its `origin` file points to `base/snapshots/<ulid>`.

## Serve over NBD

```sh
./target/debug/elide serve-volume /tmp/elide-test/ubuntu-22.04 vm1
```

Binds to `127.0.0.1:10809`. Leave it running in a separate terminal.

To bind on all interfaces (for VM access): `--bind 0.0.0.0`

## Connect with nbd-client

```sh
sudo nbd-client 127.0.0.1 10809 /dev/nbd0
sudo mount /dev/nbd0 /mnt
```

Or boot directly with QEMU — see [vm-boot.md](vm-boot.md).

## Import a raw ext4 image directly

If you already have an ext4 image (e.g. extracted from a cloud image), use `--from-file` to skip the OCI pull:

```sh
./target/debug/elide-import --from-file ubuntu-22.04.ext4 /tmp/elide-test/ubuntu-22.04
```

## Other useful commands

```sh
# Human-readable summary of the volume layout
./target/debug/elide inspect-volume /tmp/elide-test/ubuntu-22.04

# List forks
./target/debug/elide list-forks /tmp/elide-test/ubuntu-22.04

# Browse the ext4 filesystem without mounting
./target/debug/elide ls-volume /tmp/elide-test/ubuntu-22.04 vm1 /etc

# Snapshot a live fork (idempotent if no new writes)
./target/debug/elide snapshot-volume /tmp/elide-test/ubuntu-22.04 vm1

# Fork from a user fork instead of base
./target/debug/elide fork-volume /tmp/elide-test/ubuntu-22.04 vm2 --from vm1
```

