# VM Boot from an Elide Volume

How to boot a VM using an OCI-imported Elide volume as its root filesystem, using QEMU direct kernel boot.

## Overview

Elide volumes are raw ext4 block devices served over NBD. QEMU can boot a VM directly against one without a bootloader on the disk: you supply the kernel and initrd separately, and the kernel mounts the volume as its root. This is the same model Firecracker uses in production.

## Kernel Requirements

**Use the Ubuntu cloud kernel, not a Firecracker reference kernel.**

The Firecracker reference kernels published on S3 (`spec.ccfc.min`) are compiled for Firecracker's machine type, which has a different GIC address, memory map, and serial driver configuration than QEMU's `-M virt` machine. Specifically:

- `CONFIG_SERIAL_AMBA_PL011` is not set — the Firecracker kernel has no PL011 driver, so it produces no output on QEMU's virt UART
- The physical memory base address differs — the Firecracker kernel's ELF load addresses are incompatible with QEMU virt's RAM layout
- GIC version and base address differ

A Firecracker kernel loaded with QEMU's `-kernel` flag will hang silently. No earlycon output, no panic, nothing.

The Ubuntu cloud kernel (`vmlinuz` + `initrd.img`, extractable from any Ubuntu cloud image) is built for QEMU's virt machine and works correctly. The `initrd.img` bundles `virtio-blk` and `ext4` modules, so it can mount an Elide volume as root without any modules on the rootfs itself.

## Prerequisites

- QEMU: `brew install qemu`
- `cargo-binutils` and the LLVM tools bundled with Rust (used for kernel image manipulation):
  ```sh
  rustup component add llvm-tools
  cargo install cargo-binutils
  ```
- A kernel and initrd extracted from an Ubuntu cloud image. The `extract-boot` subcommand does this:
  ```sh
  elide extract-boot ubuntu-22.04.img boot-arm64/
  ```

## Serving the Volume

```sh
elide serve-volume /path/to/volume <fork-name>
```

For example, if you forked the volume to create a fork named `vm1`:

```sh
elide serve-volume /path/to/ubuntu-22.04 vm1
```

This starts an NBD server on `127.0.0.1:10809`. Leave it running in a separate terminal.

## Booting with QEMU

```sh
qemu-system-aarch64 \
  -M virt \
  -cpu host -accel hvf \
  -m 1G \
  -kernel boot-arm64/vmlinuz \
  -initrd boot-arm64/initrd.img \
  -append "root=/dev/vda rw console=ttyAMA0 init=/sbin/init" \
  -drive file=nbd://127.0.0.1:10809,format=raw,if=virtio \
  -nographic
```

On Apple Silicon, `-cpu host -accel hvf` uses the hardware hypervisor. See the known issues section below if QEMU crashes immediately.

To exit QEMU: `Ctrl-A X` (only works with `-nographic`).

## Docker Image Limitations

OCI images imported with `elide import-volume` are Docker container images. They do not include an init system (systemd, sysvinit, etc.) — Docker provides init at the container level. As a result, the kernel cannot exec `/sbin/init` after mounting root and drops to a minimal shell.

To get a usable shell immediately, pass `init=/bin/bash`:

```sh
-append "root=/dev/vda rw console=ttyAMA0 init=/bin/bash"
```

To get a fully booting system with systemd, install it into the volume first:

```sh
# From within the shell after booting
apt-get update && apt-get install -y systemd
```

Then reboot with `init=/sbin/init` (which will be a symlink to systemd after installation).

Alternatively, import a full Ubuntu cloud image instead of a Docker image — cloud images ship with systemd and are designed to boot.

## Known Issues

**HVF assertion crash on Apple Silicon (`Assertion failed: (isv), function hvf_handle_exception`):**
QEMU's HVF backend crashes on certain ARM exception types not handled by QEMU 10.x's `hvf_handle_exception`. Fall back to TCG (software emulation):

```sh
-cpu cortex-a57 -accel tcg
```

TCG is slower but correct. This is a QEMU bug, not a kernel or volume issue.

**Firecracker `vmlinux` ELF cannot be used directly with QEMU's `-kernel`:**
The Firecracker kernel is distributed as an ELF binary (`vmlinux`). QEMU's aarch64 `-kernel` loader expects a flat `Image` binary. If you need to extract one from the ELF, use `rust-objcopy` (GNU `objcopy` on macOS is x86-only and will reject aarch64 ELFs):

```sh
rust-objcopy -O binary boot-arm64/vmlinux boot-arm64/Image
```

However, even with the correct binary format, the Firecracker kernel will not boot under QEMU's `-M virt` due to the machine type incompatibility described above. This step is a dead end — use the Ubuntu cloud kernel.

**No output with custom chardev setup:**
Using `-chardev stdio` with `-display none -serial none` works but disables QEMU's `Ctrl-A X` escape sequence. Prefer `-nographic` for interactive use — it multiplexes the monitor and serial on stdio and keeps the escape sequence working.
