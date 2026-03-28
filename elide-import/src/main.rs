// elide-import: import a readonly Elide volume from an OCI image or a raw ext4 file.
//
// OCI pipeline (default):
//   1. Pull the image manifest (resolving multi-platform indexes to linux/<arch>)
//   2. Download all layer blobs concurrently to a temp directory
//   3. Merge layers into a rootfs directory via ocirender's StreamingPacker
//   4. Create an ext4 disk image from the rootfs (mke2fs or genext2fs)
//   5. Import the ext4 image into an Elide volume via elide_core::import
//
// Raw ext4 (--from-file):
//   1. Import the ext4 image directly into an Elide volume via elide_core::import

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use clap::Parser;
use elide_signing::{BASE_KEY_FILE, BASE_ORIGIN_FILE, BASE_PUB_FILE};
use oci_client::manifest::{OciImageManifest, OciManifest};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use oci_spec::image::{Arch, Os};
use ocirender::{ImageSpec, LayerMeta, StreamingPacker};
use serde::Serialize;
use tempfile::TempDir;

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(about = "Import a readonly Elide volume from an OCI image or a raw ext4 file")]
struct Args {
    /// Path to the volume directory to create (e.g. volumes/ubuntu-22.04)
    vol_dir: String,

    /// OCI image reference (e.g. ubuntu:22.04, ghcr.io/org/image:tag).
    /// Mutually exclusive with --from-file.
    #[arg(long, conflicts_with = "from_file")]
    image: Option<String>,

    /// Import a raw ext4 image directly, skipping OCI pull.
    /// Mutually exclusive with --image.
    #[arg(long, value_name = "PATH", conflicts_with = "image")]
    from_file: Option<PathBuf>,

    /// Disk image size (e.g. 4G, 2048M). Auto-sized from unpacked rootfs if omitted.
    /// Ignored when using --from-file (size is read from the image).
    #[arg(long)]
    size: Option<String>,

    /// Target CPU architecture (e.g. amd64, arm64). Defaults to host architecture.
    /// Ignored when using --from-file.
    #[arg(long)]
    arch: Option<String>,

    /// Save the intermediate flat ext4 image to this path (for boot-trace analysis).
    /// Only valid with --image; ignored with --from-file (the file is already flat).
    #[arg(long, value_name = "PATH")]
    save_flat: Option<PathBuf>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    run(args).await
}

async fn run(args: Args) -> anyhow::Result<()> {
    let vol_dir = Path::new(&args.vol_dir);
    match (args.image, args.from_file) {
        (Some(image), None) => {
            run_oci(
                &image,
                vol_dir,
                args.size.as_deref(),
                args.arch.as_deref(),
                args.save_flat.as_deref(),
            )
            .await?;
        }
        (None, Some(ext4_path)) => {
            run_from_file(&ext4_path, vol_dir)?;
        }
        _ => {
            bail!("provide --image <ref> or --from-file <path>");
        }
    }
    Ok(())
}

fn run_from_file(ext4_path: &Path, vol_dir: &Path) -> anyhow::Result<()> {
    eprintln!(
        "Importing {} into {}...",
        ext4_path.display(),
        vol_dir.display()
    );
    std::fs::create_dir_all(vol_dir).context("create volume directory")?;
    let key = elide_signing::generate_keypair(vol_dir, BASE_KEY_FILE, BASE_PUB_FILE)
        .context("generate base keypair")?;
    elide_signing::write_origin(vol_dir, &key, BASE_ORIGIN_FILE).context("write base.origin")?;
    let signer =
        elide_signing::load_signer(vol_dir, BASE_KEY_FILE).context("load base signing key")?;
    let mut last_pct = u64::MAX;
    elide_core::import::import_image(ext4_path, vol_dir, Some(&*signer), |done, total| {
        let pct = done * 100 / total;
        if pct != last_pct {
            last_pct = pct;
            eprint!("\r  {pct}%");
        }
        if done == total {
            eprintln!();
        }
    })?;
    write_meta(vol_dir, &ext4_path.display().to_string(), "", "")?;
    eprintln!("Done. Volume ready at {}", vol_dir.display());
    Ok(())
}

async fn run_oci(
    image: &str,
    vol_dir: &Path,
    size: Option<&str>,
    arch: Option<&str>,
    save_flat: Option<&Path>,
) -> anyhow::Result<()> {
    let target_arch = arch.map(parse_arch).unwrap_or_else(host_arch);

    // 1. Pull manifest
    eprintln!("Pulling manifest for {image}...");
    let reference: Reference = image
        .parse()
        .with_context(|| format!("invalid image reference: {image}"))?;
    let client = Arc::new(Client::new(Default::default()));
    let (manifest, initial_digest) = client
        .pull_manifest(&reference, &RegistryAuth::Anonymous)
        .await
        .context("failed to pull manifest")?;

    // 2. Resolve to a single-platform image manifest
    let (image_manifest, digest) =
        resolve_image_manifest(&client, &reference, manifest, initial_digest, &target_arch).await?;
    let n_layers = image_manifest.layers.len();
    eprintln!(
        "Image has {n_layers} layer(s), total compressed ~{} MiB",
        image_manifest.layers.iter().map(|l| l.size).sum::<i64>() >> 20
    );

    // 3. Download all layer blobs concurrently
    let tmp = TempDir::new().context("create temp dir")?;
    eprintln!("Downloading layers...");
    let blob_paths = download_layers(&client, &reference, &image_manifest, tmp.path()).await?;

    // 4. Merge layers into rootfs dir via ocirender
    eprintln!("Merging layers...");
    let rootfs_dir = tmp.path().join("rootfs");
    tokio::fs::create_dir_all(&rootfs_dir)
        .await
        .context("create rootfs dir")?;
    merge_layers(image_manifest, blob_paths, &rootfs_dir).await?;

    // 5. Determine ext4 image size
    let unpacked_bytes = measure_dir_bytes(&rootfs_dir).context("measure rootfs")?;
    eprintln!(
        "Rootfs unpacked: {:.1} GiB",
        unpacked_bytes as f64 / (1 << 30) as f64
    );
    let ext4_size = match size {
        Some(s) => parse_size(s).with_context(|| format!("invalid --size: {s}"))?,
        None => {
            let s = auto_size(unpacked_bytes);
            eprintln!(
                "Auto-sized ext4 image: {:.1} GiB",
                s as f64 / (1 << 30) as f64
            );
            s
        }
    };

    // 6. Create ext4 image
    let ext4_path = tmp.path().join("rootfs.ext4");
    eprintln!("Creating ext4 image...");
    create_ext4(&rootfs_dir, &ext4_path, ext4_size)?;

    // 7. Import into Elide volume
    eprintln!("Importing into {}...", vol_dir.display());
    std::fs::create_dir_all(vol_dir).context("create volume directory")?;
    let key = elide_signing::generate_keypair(vol_dir, BASE_KEY_FILE, BASE_PUB_FILE)
        .context("generate base keypair")?;
    elide_signing::write_origin(vol_dir, &key, BASE_ORIGIN_FILE).context("write base.origin")?;
    let signer =
        elide_signing::load_signer(vol_dir, BASE_KEY_FILE).context("load base signing key")?;
    let mut last_pct = u64::MAX;
    elide_core::import::import_image(&ext4_path, vol_dir, Some(&*signer), |done, total| {
        let pct = done * 100 / total;
        if pct != last_pct {
            last_pct = pct;
            eprint!("\r  {pct}%");
        }
        if done == total {
            eprintln!();
        }
    })?;

    // 8. Optionally save flat ext4 for boot-trace analysis
    if let Some(dst) = save_flat {
        save_flat_image(&ext4_path, dst)?;
        eprintln!(
            "Flat ext4 saved to {} ({:.1} GiB)",
            dst.display(),
            std::fs::metadata(dst)?.len() as f64 / (1 << 30) as f64
        );
    }

    // 9. Write volume metadata
    write_meta(vol_dir, image, &digest, &target_arch.to_string())?;

    eprintln!("Done. Volume ready at {}", vol_dir.display());
    Ok(())
}

// ── Manifest resolution ───────────────────────────────────────────────────────

/// Resolve an `OciManifest` to a single-platform `OciImageManifest`.
///
/// If the manifest is already a single-image manifest, it is returned as-is.
/// If it is an image index, the entry matching `linux/<target_arch>` is
/// selected and its manifest is pulled by digest.
/// Returns `(manifest, digest)` where `digest` is the sha256 digest of the
/// resolved platform manifest — the canonical identifier for this specific image.
async fn resolve_image_manifest(
    client: &Client,
    reference: &Reference,
    manifest: OciManifest,
    digest: String,
    target_arch: &Arch,
) -> anyhow::Result<(OciImageManifest, String)> {
    match manifest {
        OciManifest::Image(m) => Ok((m, digest)),
        OciManifest::ImageIndex(index) => {
            let entry = index
                .manifests
                .iter()
                .find(|e| {
                    e.platform
                        .as_ref()
                        .is_some_and(|p| &p.architecture == target_arch && p.os == Os::Linux)
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no linux/{target_arch:?} image found in index; \
                         use --arch to specify a different target"
                    )
                })?;

            // The entry digest is the digest of the platform-specific manifest.
            let platform_digest = entry.digest.clone();

            let digest_ref_str = format!(
                "{}/{}@{}",
                reference.registry(),
                reference.repository(),
                platform_digest
            );
            let digest_ref: Reference = digest_ref_str.parse().with_context(|| {
                format!("failed to construct digest reference: {digest_ref_str}")
            })?;

            let (platform_manifest, _) = client
                .pull_manifest(&digest_ref, &RegistryAuth::Anonymous)
                .await
                .context("failed to pull platform manifest")?;

            match platform_manifest {
                OciManifest::Image(m) => Ok((m, platform_digest)),
                OciManifest::ImageIndex(_) => bail!("nested image index is not supported"),
            }
        }
    }
}

// ── Layer download ────────────────────────────────────────────────────────────

/// Download all layer blobs concurrently to `tmp_dir/layer_<N>.blob`.
///
/// Returns a vec of blob paths in manifest order (index 0 = oldest layer).
async fn download_layers(
    client: &Arc<Client>,
    reference: &Reference,
    manifest: &OciImageManifest,
    tmp_dir: &Path,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut handles = Vec::with_capacity(manifest.layers.len());

    for (i, layer) in manifest.layers.iter().enumerate() {
        let client = client.clone();
        let reference = reference.clone();
        let blob_path = tmp_dir.join(format!("layer_{i}.blob"));
        let layer = layer.clone();

        handles.push(tokio::spawn(async move {
            eprintln!("  layer {i}: {} MiB", layer.size >> 20);
            let file = tokio::fs::File::create(&blob_path)
                .await
                .with_context(|| format!("create blob file for layer {i}"))?;
            client
                .pull_blob(&reference, &layer, file)
                .await
                .with_context(|| format!("download layer {i}"))?;
            anyhow::Ok((i, blob_path))
        }));
    }

    let mut paths = vec![PathBuf::new(); manifest.layers.len()];
    for handle in handles {
        let (i, path) = handle.await.context("layer download task panicked")??;
        paths[i] = path;
    }
    Ok(paths)
}

// ── Layer merge ───────────────────────────────────────────────────────────────

/// Merge all layer blobs into `rootfs_dir` using ocirender's `StreamingPacker`.
async fn merge_layers(
    manifest: OciImageManifest,
    blob_paths: Vec<PathBuf>,
    rootfs_dir: &Path,
) -> anyhow::Result<()> {
    let metas: Vec<LayerMeta> = manifest
        .layers
        .iter()
        .enumerate()
        .map(|(i, l)| LayerMeta {
            index: i,
            media_type: l.media_type.clone(),
        })
        .collect();

    let packer = StreamingPacker::new(
        metas,
        ImageSpec::Dir {
            path: rootfs_dir.to_path_buf(),
        },
        None,
    );

    for (i, path) in blob_paths.into_iter().enumerate() {
        packer
            .notify_layer_ready(i, path)
            .await
            .with_context(|| format!("notify layer {i} ready"))?;
    }
    packer.finish().await.context("merge layers")?;
    Ok(())
}

// ── ext4 creation ─────────────────────────────────────────────────────────────

/// Create an ext4 image at `output` from `rootfs_dir`.
///
/// Tries `mke2fs -d` first (e2fsprogs ≥ 1.43, 2016); falls back to
/// `genext2fs`. Errors clearly if neither is available.
fn create_ext4(rootfs_dir: &Path, output: &Path, size_bytes: u64) -> anyhow::Result<()> {
    let rootfs = rootfs_dir
        .to_str()
        .context("rootfs path is not valid UTF-8")?;
    let out = output.to_str().context("output path is not valid UTF-8")?;

    if try_mke2fs(rootfs, out, size_bytes).is_ok() {
        return Ok(());
    }

    try_genext2fs(rootfs, out, size_bytes).or_else(|e| {
        bail!("failed to create ext4 image — install mke2fs (e2fsprogs) or genext2fs: {e}")
    })
}

fn try_mke2fs(rootfs: &str, output: &str, size_bytes: u64) -> anyhow::Result<()> {
    // Size argument: mke2fs accepts "<n>K"
    let size_k = format!("{}K", size_bytes / 1024);
    // On macOS, e2fsprogs is keg-only and not on PATH; probe the Homebrew keg path as a fallback.
    let cmd = if std::process::Command::new("mke2fs")
        .arg("--version")
        .output()
        .is_ok()
    {
        "mke2fs".to_owned()
    } else if cfg!(target_os = "macos") {
        let keg = "/opt/homebrew/opt/e2fsprogs/sbin/mke2fs";
        if std::path::Path::new(keg).exists() {
            keg.to_owned()
        } else {
            bail!("mke2fs not found (install e2fsprogs via brew)");
        }
    } else {
        bail!("mke2fs not found");
    };
    let status = std::process::Command::new(&cmd)
        .args(["-t", "ext4", "-d", rootfs, output, &size_k])
        .status()
        .with_context(|| format!("run {cmd}"))?;
    if !status.success() {
        bail!("mke2fs exited with {status}");
    }
    Ok(())
}

fn try_genext2fs(rootfs: &str, output: &str, size_bytes: u64) -> anyhow::Result<()> {
    // genext2fs uses 1 KiB blocks; -b specifies the number of blocks
    let blocks = size_bytes / 1024;
    let status = std::process::Command::new("genext2fs")
        .args(["-b", &blocks.to_string(), "-d", rootfs, output])
        .status()
        .context("run genext2fs")?;
    if !status.success() {
        bail!("genext2fs exited with {status}");
    }
    Ok(())
}

// ── Flat image export ─────────────────────────────────────────────────────────

/// Move or copy the flat ext4 image to `dst`.
///
/// Tries a rename first (free if on the same filesystem); falls back to a full
/// copy if the source and destination are on different filesystems.
fn save_flat_image(src: &Path, dst: &Path) -> anyhow::Result<()> {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent).context("create output directory")?;
    }
    if std::fs::rename(src, dst).is_err() {
        std::fs::copy(src, dst).with_context(|| format!("copy flat ext4 to {}", dst.display()))?;
    }
    Ok(())
}

// ── Sizing helpers ────────────────────────────────────────────────────────────

/// Sum the bytes of all regular files under `dir` (non-recursive symlinks skipped).
fn measure_dir_bytes(dir: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            if ft.is_file() {
                total += entry.metadata()?.len();
            } else if ft.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(total)
}

/// Compute an automatic ext4 image size from the unpacked rootfs byte count.
///
/// Uses 2× as an overhead multiplier (ext4 journal ~128 MiB, inode tables,
/// fragmentation), with a floor of 1 GiB, rounded up to the next 512 MiB.
fn auto_size(unpacked_bytes: u64) -> u64 {
    const FLOOR: u64 = 1 << 30; // 1 GiB
    const ROUND: u64 = 512 << 20; // 512 MiB
    let with_overhead = (unpacked_bytes * 2).max(FLOOR);
    (with_overhead + ROUND - 1) / ROUND * ROUND
}

/// Parse a human-readable size string (e.g. "4G", "2048M", "1073741824").
fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    let (num, shift) = if let Some(r) = s.strip_suffix('T').or_else(|| s.strip_suffix("TB")) {
        (r, 40)
    } else if let Some(r) = s.strip_suffix('G').or_else(|| s.strip_suffix("GB")) {
        (r, 30)
    } else if let Some(r) = s.strip_suffix('M').or_else(|| s.strip_suffix("MB")) {
        (r, 20)
    } else if let Some(r) = s.strip_suffix('K').or_else(|| s.strip_suffix("KB")) {
        (r, 10)
    } else {
        (s, 0)
    };
    let n: u64 = num
        .trim()
        .parse()
        .with_context(|| format!("invalid size value: {num}"))?;
    Ok(n << shift)
}

// ── Volume metadata ───────────────────────────────────────────────────────────

#[derive(Serialize)]
struct VolumeMeta<'a> {
    readonly: bool,
    source: &'a str,
    digest: &'a str,
    arch: &'a str,
}

/// Write `meta.toml` to the volume root with OCI image provenance information.
fn write_meta(vol_dir: &Path, source: &str, digest: &str, arch: &str) -> anyhow::Result<()> {
    let meta = VolumeMeta {
        readonly: true,
        source,
        digest,
        arch,
    };
    let content = toml::to_string(&meta).context("serialize meta.toml")?;
    std::fs::write(vol_dir.join("meta.toml"), content).context("write meta.toml")
}

// ── Architecture helpers ──────────────────────────────────────────────────────

fn host_arch() -> Arch {
    match std::env::consts::ARCH {
        "x86_64" => Arch::Amd64,
        "aarch64" => Arch::ARM64,
        other => Arch::Other(other.to_string()),
    }
}

/// Map user-supplied architecture strings to OCI `Arch` values.
///
/// Accepts both OCI names ("amd64") and Go/Rust aliases ("x86_64", "aarch64").
fn parse_arch(s: &str) -> Arch {
    match s {
        "amd64" | "x86_64" => Arch::Amd64,
        "arm64" | "aarch64" => Arch::ARM64,
        "arm" | "armv7" => Arch::ARM,
        "386" | "i386" | "i686" => Arch::i386,
        "ppc64le" => Arch::PowerPC64le,
        "s390x" => Arch::s390x,
        "riscv64" => Arch::RISCV64,
        other => Arch::Other(other.to_string()),
    }
}
