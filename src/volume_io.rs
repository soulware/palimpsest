//! Block device abstraction shared by transport layers (NBD today, ublk next).
//!
//! The trait takes byte-granular offsets and handles the 4 KiB LBA rounding /
//! read-modify-write logic internally, so each transport only needs to frame
//! the wire protocol.

use std::io;

use elide_core::actor::VolumeHandle;
use elide_core::volume::{NoopSkipStats, ReadonlyVolume};

pub const LBA: u64 = 4096;

/// A block-device-shaped view over a volume.
///
/// Implementors: [`VolumeHandle`] (read/write) and [`ReadonlyVolume`]
/// (read-only; write ops return `PermissionDenied`).
pub trait BlockIO {
    /// Fill `buf` with the bytes at `offset`. `buf.len()` is the byte length.
    fn read_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;

    /// Write `buf` at `offset`. Handles sub-LBA alignment via read-modify-write.
    /// Takes ownership of `buf` to avoid a copy on the aligned fast path.
    fn write_at(&self, offset: u64, buf: Vec<u8>) -> io::Result<()>;

    /// Discard (zero) the LBAs fully covered by `[offset, offset+length)`.
    /// Sub-LBA edges are no-ops — the filesystem will rewrite those blocks
    /// before reading them.
    fn write_zeroes_at(&self, offset: u64, length: u64) -> io::Result<()>;

    /// Flush any buffered writes durably.
    fn flush(&self) -> io::Result<()>;

    fn is_readonly(&self) -> bool;

    /// No-op write skip counters, if the backend tracks them.
    fn noop_stats(&self) -> Option<NoopSkipStats> {
        None
    }
}

fn read_blocks_into<F>(offset: u64, buf: &mut [u8], read: F) -> io::Result<()>
where
    F: FnOnce(u64, u32) -> io::Result<Vec<u8>>,
{
    let length = buf.len();
    if length == 0 {
        return Ok(());
    }
    let start_lba = offset / LBA;
    let end_lba = (offset + length as u64).div_ceil(LBA);
    let lba_count = (end_lba - start_lba) as u32;
    let blocks = read(start_lba, lba_count)?;
    let skip = (offset % LBA) as usize;
    debug_assert!(skip + length <= blocks.len());
    buf.copy_from_slice(&blocks[skip..skip + length]);
    Ok(())
}

impl BlockIO for VolumeHandle {
    fn read_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        read_blocks_into(offset, buf, |lba, count| self.read(lba, count))
    }

    fn write_at(&self, offset: u64, buf: Vec<u8>) -> io::Result<()> {
        let length = buf.len();
        if length == 0 {
            return Ok(());
        }
        let start_lba = offset / LBA;
        let end_lba = (offset + length as u64).div_ceil(LBA);
        let lba_count = (end_lba - start_lba) as u32;
        let skip = (offset % LBA) as usize;
        if skip == 0 && (length as u64).is_multiple_of(LBA) {
            self.write(start_lba, buf)
        } else {
            let mut blocks = self.read(start_lba, lba_count)?;
            debug_assert!(skip + length <= blocks.len());
            blocks[skip..skip + length].copy_from_slice(&buf);
            self.write(start_lba, blocks)
        }
    }

    fn write_zeroes_at(&self, offset: u64, length: u64) -> io::Result<()> {
        let start_lba = offset.div_ceil(LBA);
        let end_lba = (offset + length) / LBA;
        if end_lba <= start_lba {
            return Ok(());
        }
        let lba_count = (end_lba - start_lba) as u32;
        self.write_zeroes(start_lba, lba_count)
    }

    fn flush(&self) -> io::Result<()> {
        VolumeHandle::flush(self)
    }

    fn is_readonly(&self) -> bool {
        false
    }

    fn noop_stats(&self) -> Option<NoopSkipStats> {
        VolumeHandle::noop_stats(self).ok()
    }
}

impl BlockIO for ReadonlyVolume {
    fn read_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        read_blocks_into(offset, buf, |lba, count| self.read(lba, count))
    }

    fn write_at(&self, _offset: u64, _buf: Vec<u8>) -> io::Result<()> {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }

    fn write_zeroes_at(&self, _offset: u64, _length: u64) -> io::Result<()> {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }

    fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    fn is_readonly(&self) -> bool {
        true
    }
}
