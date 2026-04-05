// Elide library: module declarations and utilities shared across the binary
// and its tests.

pub mod control;
pub mod coordinator_client;
pub mod extents;
pub mod inspect;
pub mod inspect_files;
pub mod ls;
pub mod nbd;

use std::io;
use std::path::{Path, PathBuf};

/// Resolve a volume name to its directory via `<data_dir>/by_name/<name>`.
///
/// The path is returned as-is; the OS follows the symlink transparently.
pub fn resolve_volume_dir(data_dir: &Path, name: &str) -> PathBuf {
    data_dir.join("by_name").join(name)
}

/// Volume names must be non-empty, contain only `[a-zA-Z0-9._-]`, and not
/// be reserved by the `import` subcommand (`status`, `attach`).
pub fn validate_volume_name(name: &str) -> io::Result<()> {
    if name.is_empty() {
        return Err(io::Error::other("volume name must not be empty"));
    }
    if let Some(c) = name
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_' && *c != '.')
    {
        return Err(io::Error::other(format!(
            "invalid character {c:?} in volume name {name:?}: only [a-zA-Z0-9._-] allowed"
        )));
    }
    if matches!(name, "status" | "attach") {
        return Err(io::Error::other(format!("'{name}' is a reserved name")));
    }
    Ok(())
}

/// Parse a human-readable size string: plain bytes, or with suffix K/M/G/T (base-2).
///
/// Accepts both bare suffixes (`4G`) and SI-style (`4GB`).
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, shift) = if let Some(rest) = s.strip_suffix('T').or_else(|| s.strip_suffix("TB")) {
        (rest, 40)
    } else if let Some(rest) = s.strip_suffix('G').or_else(|| s.strip_suffix("GB")) {
        (rest, 30)
    } else if let Some(rest) = s.strip_suffix('M').or_else(|| s.strip_suffix("MB")) {
        (rest, 20)
    } else if let Some(rest) = s.strip_suffix('K').or_else(|| s.strip_suffix("KB")) {
        (rest, 10)
    } else {
        (s, 0)
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid size: {s}"))?;
    Ok(n << shift)
}

/// Read the volume size from `<dir>/volume.size`, or create it from `size_arg` if absent.
pub fn resolve_volume_size(dir: &Path, size_arg: Option<&str>) -> io::Result<u64> {
    let size_file = dir.join("volume.size");
    if size_file.exists() {
        let s = std::fs::read_to_string(&size_file)?;
        s.trim()
            .parse::<u64>()
            .map_err(|e| io::Error::other(format!("bad size file: {e}")))
    } else {
        let s = size_arg.ok_or_else(|| {
            io::Error::other("volume size required on first use: pass --size (e.g. --size 4G)")
        })?;
        let bytes = parse_size(s).map_err(|e| io::Error::other(format!("bad --size: {e}")))?;
        if bytes == 0 {
            return Err(io::Error::other("volume size must be non-zero"));
        }
        std::fs::create_dir_all(dir)?;
        std::fs::write(&size_file, bytes.to_string())?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── validate_volume_name ──────────────────────────────────────────────────

    #[test]
    fn valid_names_accepted() {
        for name in &["foo", "my-vol", "vol_1", "ubuntu-22.04", "a.b.c"] {
            assert!(
                validate_volume_name(name).is_ok(),
                "expected ok for {name:?}"
            );
        }
    }

    #[test]
    fn empty_name_rejected() {
        assert!(validate_volume_name("").is_err());
    }

    #[test]
    fn colon_rejected() {
        let err = validate_volume_name("ubuntu:22.04").unwrap_err();
        assert!(err.to_string().contains(':'));
    }

    #[test]
    fn slash_rejected() {
        assert!(validate_volume_name("foo/bar").is_err());
    }

    #[test]
    fn reserved_names_rejected() {
        for name in &["status", "attach"] {
            let err = validate_volume_name(name).unwrap_err();
            assert!(
                err.to_string().contains("reserved"),
                "expected 'reserved' in error for {name:?}, got: {err}"
            );
        }
    }

    // ── parse_size ────────────────────────────────────────────────────────────

    #[test]
    fn parse_size_bytes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }

    #[test]
    fn parse_size_suffixes() {
        assert_eq!(parse_size("4K").unwrap(), 4 * 1024);
        assert_eq!(parse_size("4KB").unwrap(), 4 * 1024);
        assert_eq!(parse_size("2M").unwrap(), 2 << 20);
        assert_eq!(parse_size("2MB").unwrap(), 2 << 20);
        assert_eq!(parse_size("1G").unwrap(), 1u64 << 30);
        assert_eq!(parse_size("1GB").unwrap(), 1u64 << 30);
        assert_eq!(parse_size("1T").unwrap(), 1u64 << 40);
        assert_eq!(parse_size("1TB").unwrap(), 1u64 << 40);
    }

    #[test]
    fn parse_size_trims_whitespace() {
        assert_eq!(parse_size("  8G  ").unwrap(), 8u64 << 30);
    }

    #[test]
    fn parse_size_invalid() {
        assert!(parse_size("abc").is_err());
        assert!(parse_size("").is_err());
        assert!(parse_size("4X").is_err());
    }
}
