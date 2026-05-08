//! lz4 compression gating used by the write/promote paths.
//!
//! Pulled out of `volume/mod.rs` for legibility.

/// Minimum compression ratio required to store compressed data (1.5×).
///
/// If the compressed payload is not at least 1/3 smaller than the original,
/// the compression overhead is not worth it and the raw data is stored instead.
pub(in crate::volume) const MIN_COMPRESSION_RATIO_NUM: usize = 3;
pub(in crate::volume) const MIN_COMPRESSION_RATIO_DEN: usize = 2;

/// Attempt lz4 compression on `data`.
///
/// Returns `Some(compressed_bytes)` if the compression ratio meets the
/// minimum threshold (1.5×); `None` to store raw. Incompressible data (high
/// entropy, already-compressed payloads) fails the ratio check naturally —
/// lz4 itself decides faster than a precomputed entropy gate would.
pub(crate) fn maybe_compress(data: &[u8]) -> Option<Vec<u8>> {
    let compressed = lz4_flex::compress_prepend_size(data);
    if compressed.len() * MIN_COMPRESSION_RATIO_NUM / MIN_COMPRESSION_RATIO_DEN >= data.len() {
        return None;
    }
    Some(compressed)
}
