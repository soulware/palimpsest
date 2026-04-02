use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tracing::{error, warn};

use elide_core::volume::{ReadonlyVolume, Volume};

use crate::extents::{LocatedExtent, locate_extents};

// Handshake magic
const NBD_MAGIC: u64 = 0x4e42444d41474943; // "NBDMAGIC"
const NBD_OPTS_MAGIC: u64 = 0x49484156454f5054; // "IHAVEOPT"
const NBD_OPTS_REPLY_MAGIC: u64 = 0x0003e889045565a9;

// Server handshake flags
const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1;

// Option codes
const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_LIST: u32 = 3;
const NBD_OPT_INFO: u32 = 6;
const NBD_OPT_GO: u32 = 7;

// Option reply types
const NBD_REP_ACK: u32 = 1;
const NBD_REP_SERVER: u32 = 2;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = (1 << 31) | 1;

// Info types
const NBD_INFO_EXPORT: u16 = 0;
const NBD_INFO_BLOCK_SIZE: u16 = 3;

// Transmission magic
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_REPLY_MAGIC: u32 = 0x67446698;

// Transmission commands
const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;

// Transmission flags
const NBD_FLAG_HAS_FLAGS: u16 = 1;
const NBD_FLAG_READ_ONLY: u16 = 2;
const NBD_FLAG_SEND_FLUSH: u16 = 4;

// COW granularity — 4KB blocks
const COW_BLOCK: u64 = 4096;

// Global flag set by SIGUSR1 handler
static PRINT_STATS: AtomicBool = AtomicBool::new(false);

extern "C" fn sigusr1_handler(_: libc::c_int) {
    PRINT_STATS.store(true, Ordering::Relaxed);
}

fn install_sigusr1_handler() {
    unsafe {
        libc::signal(
            libc::SIGUSR1,
            sigusr1_handler as *const () as libc::sighandler_t,
        );
    }
}

fn report_path() -> String {
    format!("/tmp/elide-{}.report", std::process::id())
}

// --- extent index ---

struct ExtentIndex {
    extents: Vec<LocatedExtent>,
    total_bytes: u64,
}

impl ExtentIndex {
    fn build(image_path: &str) -> io::Result<Self> {
        let extents = locate_extents(Path::new(image_path))?;
        let total_bytes = extents.iter().map(|e| e.byte_count).sum();
        Ok(Self {
            extents,
            total_bytes,
        })
    }

    // Indices of extents overlapping [offset, offset+length).
    // Valid because non-overlapping extents sorted by start are also sorted by end.
    fn overlapping(&self, offset: u64, length: u64) -> std::ops::Range<usize> {
        let end = offset + length;
        let start_idx = self
            .extents
            .partition_point(|e| e.start_byte + e.byte_count <= offset);
        let end_idx = self.extents.partition_point(|e| e.start_byte < end);
        start_idx..end_idx
    }
}

// --- reporting ---

struct ReadStats {
    accessed: HashSet<usize>, // extent indices
    unmapped_reads: u64,      // reads that hit no extent (metadata, journal, etc.)
}

fn print_report(stats: &ReadStats, index: &ExtentIndex, image_size: u64) {
    let n = stats.accessed.len();
    let total = index.extents.len();
    let accessed_bytes: u64 = stats
        .accessed
        .iter()
        .map(|&i| index.extents[i].byte_count)
        .sum();
    let lines = format!(
        "\n=== Extent Report ===\n  Extents accessed:  {} / {} ({:.1}%)\n  Data accessed:     {:.1} MB / {:.1} MB ({:.1}%)\n  Unmapped reads:    {} (metadata/journal)\n",
        n,
        total,
        100.0 * n as f64 / total.max(1) as f64,
        accessed_bytes as f64 / (1024.0 * 1024.0),
        index.total_bytes as f64 / (1024.0 * 1024.0),
        100.0 * accessed_bytes as f64 / index.total_bytes.max(1) as f64,
        stats.unmapped_reads,
    );
    let _ = image_size; // kept for context in callers
    print!("{}", lines);
    let _ = std::fs::write(report_path(), &lines);
}

pub fn run(image_path: &str, port: u16, save_trace: Option<&str>) -> io::Result<()> {
    let image_size = std::fs::metadata(image_path)?.len();

    print!("Building extent index for {} ...", image_path);
    let index = ExtentIndex::build(image_path)?;
    println!(
        " {} extents ({:.1} MB file data)",
        index.extents.len(),
        index.total_bytes as f64 / (1024.0 * 1024.0)
    );

    let stats: Arc<Mutex<ReadStats>> = Arc::new(Mutex::new(ReadStats {
        accessed: HashSet::new(),
        unmapped_reads: 0,
    }));

    install_sigusr1_handler();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;
    println!("NBD server on 127.0.0.1:{}", port);
    println!(
        "Image: {} ({:.1} MB)",
        image_path,
        image_size as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Connect QEMU with: -drive file=nbd://127.0.0.1:{},format=raw,if=virtio",
        port
    );
    println!(
        "Send SIGUSR1 (kill -USR1 {}) for a mid-boot report",
        std::process::id()
    );
    println!("Report file:  {}", report_path());
    if let Some(t) = save_trace {
        println!("Boot trace:   {} (written on disconnect)", t);
    }
    println!("Waiting for connection...\n");

    if let Some(stream) = listener.incoming().next() {
        let stream = stream?;
        println!("[connected: {}]", stream.peer_addr()?);

        let result = handle_connection(stream, image_path, image_size, &index, Arc::clone(&stats));

        let stats = stats.lock().unwrap();
        println!("\n=== Boot Read Report ===");
        let n = stats.accessed.len();
        let total = index.extents.len();
        let accessed_bytes: u64 = stats
            .accessed
            .iter()
            .map(|&i| index.extents[i].byte_count)
            .sum();
        println!(
            "  Extents accessed:  {} / {} ({:.1}%)",
            n,
            total,
            100.0 * n as f64 / total.max(1) as f64
        );
        println!(
            "  Data accessed:     {:.1} MB / {:.1} MB ({:.1}%)",
            accessed_bytes as f64 / (1024.0 * 1024.0),
            index.total_bytes as f64 / (1024.0 * 1024.0),
            100.0 * accessed_bytes as f64 / index.total_bytes.max(1) as f64
        );
        println!(
            "  Unmapped reads:    {} (metadata/journal)",
            stats.unmapped_reads
        );

        if let Some(trace_path) = save_trace {
            let entries: Vec<crate::extents::TraceEntry> = stats
                .accessed
                .iter()
                .map(|&i| {
                    let e = &index.extents[i];
                    crate::extents::TraceEntry {
                        hash: e.hash,
                        start_byte: e.start_byte,
                        byte_count: e.byte_count,
                    }
                })
                .collect();
            match crate::extents::save_trace(std::path::Path::new(trace_path), &entries) {
                Ok(()) => println!(
                    "  Boot trace:        {} ({} extents)",
                    trace_path,
                    entries.len()
                ),
                Err(e) => error!("  Boot trace error:  {}", e),
            }
        }

        if let Err(e) = result {
            warn!("[connection error: {}]", e);
        } else {
            println!("[disconnected]");
        }
    }

    Ok(())
}

fn handle_connection(
    mut s: TcpStream,
    image_path: &str,
    image_size: u64,
    index: &ExtentIndex,
    stats: Arc<Mutex<ReadStats>>,
) -> io::Result<()> {
    // --- Newstyle handshake ---
    s.write_all(&NBD_MAGIC.to_be_bytes())?;
    s.write_all(&NBD_OPTS_MAGIC.to_be_bytes())?;
    s.write_all(&NBD_FLAG_FIXED_NEWSTYLE.to_be_bytes())?;

    let _client_flags = read_u32(&mut s)?;

    let tx_flags: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;

    // --- Options loop ---
    loop {
        let magic = read_u64(&mut s)?;
        if magic != NBD_OPTS_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad option magic",
            ));
        }
        let option = read_u32(&mut s)?;
        let len = read_u32(&mut s)? as usize;
        let mut data = vec![0u8; len];
        s.read_exact(&mut data)?;

        match option {
            NBD_OPT_EXPORT_NAME => {
                // Legacy path: send export info directly, no option reply wrapper
                s.write_all(&image_size.to_be_bytes())?;
                s.write_all(&tx_flags.to_be_bytes())?;
                s.write_all(&[0u8; 124])?; // padding (zeroes)
                break;
            }
            NBD_OPT_GO | NBD_OPT_INFO => {
                // Send NBD_REP_INFO with export size and flags
                let mut info = Vec::new();
                info.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                info.extend_from_slice(&image_size.to_be_bytes());
                info.extend_from_slice(&tx_flags.to_be_bytes());
                opt_reply(&mut s, option, NBD_REP_INFO, &info)?;
                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
                if option == NBD_OPT_GO {
                    break;
                }
            }
            NBD_OPT_LIST => {
                // Report one anonymous export
                let mut entry = Vec::new();
                entry.extend_from_slice(&0u32.to_be_bytes()); // name length = 0
                opt_reply(&mut s, option, NBD_REP_SERVER, &entry)?;
                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
            }
            NBD_OPT_ABORT => {
                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
                return Ok(());
            }
            _ => {
                opt_reply(&mut s, option, NBD_REP_ERR_UNSUP, &[])?;
            }
        }
    }

    // --- Transmission ---
    let mut file = File::open(image_path)?;
    let mut cow: HashMap<u64, Vec<u8>> = HashMap::new(); // block_idx → 4KB block
    let mut reads: u64 = 0;
    let mut writes: u64 = 0;

    // Wake up periodically so we can check for SIGUSR1 even when the VM is idle
    s.set_read_timeout(Some(std::time::Duration::from_millis(200)))?;

    loop {
        let magic = match read_u32(&mut s) {
            Ok(m) => m,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                if PRINT_STATS.swap(false, Ordering::Relaxed) {
                    let st = stats.lock().unwrap();
                    print_report(&st, index, image_size);
                }
                continue;
            }
            Err(e) => return Err(e),
        };
        if magic != NBD_REQUEST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad request magic",
            ));
        }
        let _flags = read_u16(&mut s)?;
        let cmd = read_u16(&mut s)?;
        let handle = read_u64(&mut s)?;
        let offset = read_u64(&mut s)?;
        let length = read_u32(&mut s)? as usize;

        match cmd {
            NBD_CMD_READ => {
                reads += 1;
                let mut buf = vec![0u8; length];

                // Read from base file
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut buf)?;

                // Overlay COW blocks
                cow_read_overlay(&cow, offset, &mut buf);

                // Map this read back to extents
                {
                    let mut st = stats.lock().unwrap();
                    let range = index.overlapping(offset, length as u64);
                    if range.is_empty() {
                        st.unmapped_reads += 1;
                    } else {
                        for i in range {
                            st.accessed.insert(i);
                        }
                    }
                }

                tx_reply(&mut s, 0, handle)?;
                s.write_all(&buf)?;
            }

            NBD_CMD_WRITE => {
                writes += 1;
                let mut buf = vec![0u8; length];
                s.read_exact(&mut buf)?;
                cow_write(&mut cow, &mut file, image_size, offset, &buf)?;
                tx_reply(&mut s, 0, handle)?;
            }

            NBD_CMD_DISC => {
                println!("[reads: {}, writes: {}]", reads, writes);
                break;
            }

            NBD_CMD_FLUSH => {
                tx_reply(&mut s, 0, handle)?;
            }

            _ => {
                tx_reply(&mut s, 22, handle)?;
            }
        }

        // Check for SIGUSR1 — print report without interrupting
        if PRINT_STATS.swap(false, Ordering::Relaxed) {
            let st = stats.lock().unwrap();
            print_report(&st, index, image_size);
        }
    }

    Ok(())
}

// Apply any COW blocks over buf (which was read from the base file at `offset`)
fn cow_read_overlay(cow: &HashMap<u64, Vec<u8>>, offset: u64, buf: &mut [u8]) {
    let length = buf.len() as u64;
    let first_block = offset / COW_BLOCK;
    let last_block = (offset + length - 1) / COW_BLOCK;

    for block_idx in first_block..=last_block {
        if let Some(block) = cow.get(&block_idx) {
            let block_start = block_idx * COW_BLOCK;
            let isect_start = offset.max(block_start);
            let isect_end = (offset + length).min(block_start + COW_BLOCK);
            if isect_start < isect_end {
                let dst = (isect_start - offset) as usize;
                let src = (isect_start - block_start) as usize;
                let len = (isect_end - isect_start) as usize;
                buf[dst..dst + len].copy_from_slice(&block[src..src + len]);
            }
        }
    }
}

// Write buf at offset into the COW layer, initialising blocks from the base file as needed
fn cow_write(
    cow: &mut HashMap<u64, Vec<u8>>,
    file: &mut File,
    image_size: u64,
    offset: u64,
    buf: &[u8],
) -> io::Result<()> {
    let length = buf.len() as u64;
    let first_block = offset / COW_BLOCK;
    let last_block = (offset + length - 1) / COW_BLOCK;

    for block_idx in first_block..=last_block {
        let block = cow.entry(block_idx).or_insert_with(|| {
            let mut b = vec![0u8; COW_BLOCK as usize];
            let block_start = block_idx * COW_BLOCK;
            if block_start < image_size {
                let read_len = COW_BLOCK.min(image_size - block_start) as usize;
                let _ = file.seek(SeekFrom::Start(block_start));
                let _ = file.read(&mut b[..read_len]);
            }
            b
        });

        let block_start = block_idx * COW_BLOCK;
        let isect_start = offset.max(block_start);
        let isect_end = (offset + length).min(block_start + COW_BLOCK);
        if isect_start < isect_end {
            let dst = (isect_start - block_start) as usize;
            let src = (isect_start - offset) as usize;
            let len = (isect_end - isect_start) as usize;
            block[dst..dst + len].copy_from_slice(&buf[src..src + len]);
        }
    }

    Ok(())
}

// --- Volume NBD server ---
#[allow(dead_code)]
pub fn run_volume(dir: &Path, size_bytes: u64, bind: &str, port: u16) -> io::Result<()> {
    let listener = TcpListener::bind(format!("{}:{}", bind, port))?;
    let addr = listener.local_addr()?;
    println!("NBD volume server on {}", addr);
    println!(
        "Volume: {} ({:.1} MB)",
        dir.display(),
        size_bytes as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Connect with: sudo nbd-client {} {} -N export /dev/nbdX",
        addr.ip(),
        addr.port()
    );
    println!("Waiting for connection...\n");
    serve_volume_listener(dir, size_bytes, listener, None, None)
}

/// Serve a fork over NBD with a signing key attached.
///
/// Segments promoted during this session will be signed with `signer`.
/// If `fetch_config` is provided, missing segments are fetched from remote
/// storage on demand and cached in `segments/`.
/// If `port` is `None`, no NBD server is started — the volume runs for
/// coordinator IPC only (control socket).
pub fn run_volume_signed(
    dir: &Path,
    size_bytes: u64,
    bind: &str,
    port: Option<u16>,
    signer: std::sync::Arc<dyn elide_core::segment::SegmentSigner>,
    fetch_config: Option<elide_fetch::FetchConfig>,
) -> io::Result<()> {
    let Some(port) = port else {
        return run_volume_ipc_only(dir, size_bytes, Some(signer), fetch_config);
    };
    let listener = TcpListener::bind(format!("{}:{}", bind, port))?;
    let addr = listener.local_addr()?;
    println!("NBD volume server on {}", addr);
    println!(
        "Volume: {} ({:.1} MB)",
        dir.display(),
        size_bytes as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Connect with: sudo nbd-client {} {} -N export /dev/nbdX",
        addr.ip(),
        addr.port()
    );
    println!("Waiting for connection...\n");
    serve_volume_listener(dir, size_bytes, listener, Some(signer), fetch_config)
}

/// Serve a fork as a read-only NBD device.
///
/// The fork is opened without acquiring a write lock or creating a WAL. The
/// NBD device is advertised as read-only; write commands return EPERM.
/// Requires `--readonly` to be passed explicitly so the intent is unambiguous.
/// If `fetch_config` is provided, missing segments are fetched on demand.
pub fn run_volume_readonly(
    dir: &Path,
    size_bytes: u64,
    bind: &str,
    port: Option<u16>,
    fetch_config: Option<elide_fetch::FetchConfig>,
) -> io::Result<()> {
    let Some(port) = port else {
        return run_volume_ipc_only(dir, size_bytes, None, fetch_config);
    };
    let listener = TcpListener::bind(format!("{}:{}", bind, port))?;
    let addr = listener.local_addr()?;
    println!("NBD readonly volume server on {}", addr);
    println!(
        "Volume: {} ({:.1} MB)  [read-only]",
        dir.display(),
        size_bytes as f64 / (1024.0 * 1024.0)
    );
    println!(
        "Connect with: sudo nbd-client {} {} -N export /dev/nbdX",
        addr.ip(),
        addr.port()
    );
    println!("Waiting for connection...\n");

    install_sigusr1_handler();
    let by_id_dir = dir.parent().unwrap_or(dir);
    let mut volume = ReadonlyVolume::open(dir, by_id_dir)?;

    if let Some(config) = fetch_config {
        let volume_ids = elide_fetch::ancestry_chain(&volume.fork_dirs())?;
        let fetcher = elide_fetch::ObjectStoreFetcher::new(&config, volume_ids)?;
        volume.set_fetcher(std::sync::Arc::new(fetcher));
        println!("[demand-fetch enabled]");
    }

    for stream in listener.incoming() {
        let stream = stream?;
        println!("[connected: {}]", stream.peer_addr()?);
        let result = handle_readonly_connection(stream, &volume, size_bytes);
        match result {
            Ok(()) => println!("[disconnected]"),
            Err(e) => warn!("[connection error: {}]", e),
        }
        println!("Waiting for connection...\n");
    }
    Ok(())
}

fn handle_readonly_connection(
    mut s: TcpStream,
    volume: &ReadonlyVolume,
    volume_size: u64,
) -> io::Result<()> {
    // Newstyle handshake
    s.write_all(&NBD_MAGIC.to_be_bytes())?;
    s.write_all(&NBD_OPTS_MAGIC.to_be_bytes())?;
    s.write_all(&NBD_FLAG_FIXED_NEWSTYLE.to_be_bytes())?;

    let _client_flags = read_u32(&mut s)?;

    let tx_flags: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_READ_ONLY;

    loop {
        let magic = read_u64(&mut s)?;
        if magic != NBD_OPTS_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad option magic",
            ));
        }
        let option = read_u32(&mut s)?;
        let len = read_u32(&mut s)? as usize;
        let mut data = vec![0u8; len];
        s.read_exact(&mut data)?;

        match option {
            NBD_OPT_EXPORT_NAME => {
                s.write_all(&volume_size.to_be_bytes())?;
                s.write_all(&tx_flags.to_be_bytes())?;
                s.write_all(&[0u8; 124])?;
                break;
            }
            NBD_OPT_GO | NBD_OPT_INFO => {
                let mut info = Vec::new();
                info.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                info.extend_from_slice(&volume_size.to_be_bytes());
                info.extend_from_slice(&tx_flags.to_be_bytes());
                opt_reply(&mut s, option, NBD_REP_INFO, &info)?;

                let mut bsz = Vec::new();
                bsz.extend_from_slice(&NBD_INFO_BLOCK_SIZE.to_be_bytes());
                bsz.extend_from_slice(&512u32.to_be_bytes());
                bsz.extend_from_slice(&4096u32.to_be_bytes());
                bsz.extend_from_slice(&(4u32 * 1024 * 1024).to_be_bytes());
                opt_reply(&mut s, option, NBD_REP_INFO, &bsz)?;

                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
                if option == NBD_OPT_GO {
                    break;
                }
            }
            NBD_OPT_LIST => {
                let mut entry = Vec::new();
                entry.extend_from_slice(&0u32.to_be_bytes());
                opt_reply(&mut s, option, NBD_REP_SERVER, &entry)?;
                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
            }
            NBD_OPT_ABORT => {
                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
                return Ok(());
            }
            _ => {
                opt_reply(&mut s, option, NBD_REP_ERR_UNSUP, &[])?;
            }
        }
    }

    let mut reads: u64 = 0;
    s.set_read_timeout(Some(std::time::Duration::from_millis(200)))?;

    loop {
        let magic = match read_u32(&mut s) {
            Ok(m) => m,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => return Err(e),
        };
        if magic != NBD_REQUEST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad request magic",
            ));
        }
        let _flags = read_u16(&mut s)?;
        let cmd = read_u16(&mut s)?;
        let handle = read_u64(&mut s)?;
        let offset = read_u64(&mut s)?;
        let length = read_u32(&mut s)? as usize;

        match cmd {
            NBD_CMD_READ => {
                reads += 1;
                let start_lba = offset / 4096;
                let end_lba = (offset + length as u64).div_ceil(4096);
                let lba_count = (end_lba - start_lba) as u32;
                match volume.read(start_lba, lba_count) {
                    Ok(blocks) => {
                        let skip = (offset % 4096) as usize;
                        // skip + length <= lba_count * 4096 = blocks.len() by construction.
                        debug_assert!(skip + length <= blocks.len());
                        tx_reply(&mut s, 0, handle)?;
                        s.write_all(&blocks[skip..skip + length])?;
                    }
                    Err(e) => {
                        error!("[read error offset={} len={}: {}]", offset, length, e);
                        tx_reply(&mut s, 5, handle)?; // EIO
                    }
                }
            }
            NBD_CMD_WRITE => {
                // Drain the write payload before responding.
                let mut buf = vec![0u8; length];
                s.read_exact(&mut buf)?;
                tx_reply(&mut s, 1, handle)?; // EPERM — device is read-only
            }
            NBD_CMD_DISC => {
                println!("[reads: {}]", reads);
                break;
            }
            NBD_CMD_FLUSH => {
                tx_reply(&mut s, 0, handle)?; // flush is a no-op on readonly
            }
            _ => {
                tx_reply(&mut s, 22, handle)?; // EINVAL
            }
        }
    }
    Ok(())
}

/// Readonly variant of `serve_volume_listener`. Opens the fork with
/// `ReadonlyVolume` (no WAL, no lock) and serves it read-only.
/// Separated from `run_volume_readonly` so tests can bind port 0.
#[cfg(test)]
fn serve_readonly_volume_listener(
    dir: &Path,
    size_bytes: u64,
    listener: TcpListener,
) -> io::Result<()> {
    let volume = ReadonlyVolume::open(dir, dir)?;
    for stream in listener.incoming() {
        let stream = stream?;
        let result = handle_readonly_connection(stream, &volume, size_bytes);
        if let Err(e) = result {
            warn!("[readonly connection error: {}]", e);
        }
    }
    Ok(())
}

/// Open a volume and start the coordinator control socket, but do not bind an
/// NBD server. Blocks until SIGTERM (or the process is otherwise killed).
///
/// Used when the coordinator supervises a volume for IPC purposes only — no
/// VM is connected and no NBD port is needed.
fn run_volume_ipc_only(
    dir: &Path,
    _size_bytes: u64,
    _signer: Option<std::sync::Arc<dyn elide_core::segment::SegmentSigner>>,
    fetch_config: Option<elide_fetch::FetchConfig>,
) -> io::Result<()> {
    install_sigusr1_handler();

    let by_id_dir = dir.parent().unwrap_or(dir);
    let mut volume = Volume::open(dir, by_id_dir)?;

    if let Some(config) = fetch_config {
        let volume_ids = elide_fetch::ancestry_chain(&volume.fork_dirs())?;
        let fetcher = elide_fetch::ObjectStoreFetcher::new(&config, volume_ids)?;
        volume.set_fetcher(std::sync::Arc::new(fetcher));
    }

    let (_actor, handle) = elide_core::actor::spawn(volume);
    let _actor_thread = std::thread::Builder::new()
        .name("volume-actor".into())
        .spawn(move || _actor.run())
        .map_err(io::Error::other)?;
    crate::control::start(dir, handle)?;

    // Block until SIGTERM terminates the process.
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

/// Serve NBD connections on an already-bound listener, looping until the
/// listener is closed. The volume is opened once and reused across connections
/// so the in-memory LBA map is preserved between reconnects.
/// Separated from `run_volume` so tests can bind port 0 and learn the port.
fn serve_volume_listener(
    dir: &Path,
    size_bytes: u64,
    listener: TcpListener,
    _signer: Option<std::sync::Arc<dyn elide_core::segment::SegmentSigner>>,
    fetch_config: Option<elide_fetch::FetchConfig>,
) -> io::Result<()> {
    install_sigusr1_handler();

    let by_id_dir = dir.parent().unwrap_or(dir);
    let mut volume = Volume::open(dir, by_id_dir)?;

    if let Some(config) = fetch_config {
        let volume_ids = elide_fetch::ancestry_chain(&volume.fork_dirs())?;
        let fetcher = elide_fetch::ObjectStoreFetcher::new(&config, volume_ids)?;
        volume.set_fetcher(std::sync::Arc::new(fetcher));
        println!("[demand-fetch enabled]");
    }

    let (actor, handle) = elide_core::actor::spawn(volume);
    let _actor_thread = std::thread::Builder::new()
        .name("volume-actor".into())
        .spawn(move || actor.run())
        .map_err(io::Error::other)?;
    crate::control::start(dir, handle.clone())?;

    for stream in listener.incoming() {
        let stream = stream?;
        println!("[connected: {}]", stream.peer_addr()?);

        let result = handle_volume_connection(stream, &handle, size_bytes);

        match result {
            Ok(()) => println!("[disconnected]"),
            Err(e) => warn!("[connection error: {}]", e),
        }

        println!("Waiting for connection...\n");
    }

    Ok(())
}

fn handle_volume_connection(
    mut s: TcpStream,
    volume: &elide_core::actor::VolumeHandle,
    volume_size: u64,
) -> io::Result<()> {
    // Newstyle handshake
    s.write_all(&NBD_MAGIC.to_be_bytes())?;
    s.write_all(&NBD_OPTS_MAGIC.to_be_bytes())?;
    s.write_all(&NBD_FLAG_FIXED_NEWSTYLE.to_be_bytes())?;

    let _client_flags = read_u32(&mut s)?;

    let tx_flags: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;

    // Options loop
    loop {
        let magic = read_u64(&mut s)?;
        if magic != NBD_OPTS_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad option magic",
            ));
        }
        let option = read_u32(&mut s)?;
        let len = read_u32(&mut s)? as usize;
        let mut data = vec![0u8; len];
        s.read_exact(&mut data)?;

        match option {
            NBD_OPT_EXPORT_NAME => {
                s.write_all(&volume_size.to_be_bytes())?;
                s.write_all(&tx_flags.to_be_bytes())?;
                s.write_all(&[0u8; 124])?;
                break;
            }
            NBD_OPT_GO | NBD_OPT_INFO => {
                // Export info
                let mut info = Vec::new();
                info.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                info.extend_from_slice(&volume_size.to_be_bytes());
                info.extend_from_slice(&tx_flags.to_be_bytes());
                opt_reply(&mut s, option, NBD_REP_INFO, &info)?;

                // Block size: minimum=512, preferred=4096, maximum=4MB.
                // We handle sub-4096 writes internally via read-modify-write.
                let mut bsz = Vec::new();
                bsz.extend_from_slice(&NBD_INFO_BLOCK_SIZE.to_be_bytes());
                bsz.extend_from_slice(&512u32.to_be_bytes());
                bsz.extend_from_slice(&4096u32.to_be_bytes());
                bsz.extend_from_slice(&(4u32 * 1024 * 1024).to_be_bytes());
                opt_reply(&mut s, option, NBD_REP_INFO, &bsz)?;

                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
                if option == NBD_OPT_GO {
                    break;
                }
            }
            NBD_OPT_LIST => {
                let mut entry = Vec::new();
                entry.extend_from_slice(&0u32.to_be_bytes());
                opt_reply(&mut s, option, NBD_REP_SERVER, &entry)?;
                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
            }
            NBD_OPT_ABORT => {
                opt_reply(&mut s, option, NBD_REP_ACK, &[])?;
                return Ok(());
            }
            _ => {
                opt_reply(&mut s, option, NBD_REP_ERR_UNSUP, &[])?;
            }
        }
    }

    // Transmission loop
    let mut reads: u64 = 0;
    let mut writes: u64 = 0;

    loop {
        let magic = read_u32(&mut s)?;
        if magic != NBD_REQUEST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad request magic",
            ));
        }
        let _flags = read_u16(&mut s)?;
        let cmd = read_u16(&mut s)?;
        let handle = read_u64(&mut s)?;
        let offset = read_u64(&mut s)?;
        let length = read_u32(&mut s)? as usize;

        match cmd {
            NBD_CMD_READ => {
                reads += 1;
                let start_lba = offset / 4096;
                let end_lba = (offset + length as u64).div_ceil(4096);
                let lba_count = (end_lba - start_lba) as u32;
                match volume.read(start_lba, lba_count) {
                    Ok(blocks) => {
                        let skip = (offset % 4096) as usize;
                        // skip + length <= lba_count * 4096 = blocks.len() by construction.
                        debug_assert!(skip + length <= blocks.len());
                        tx_reply(&mut s, 0, handle)?;
                        s.write_all(&blocks[skip..skip + length])?;
                    }
                    Err(e) => {
                        error!("[read error offset={} len={}: {}]", offset, length, e);
                        tx_reply(&mut s, 5, handle)?; // EIO — no data follows on error
                    }
                }
            }

            NBD_CMD_WRITE => {
                writes += 1;
                let mut buf = vec![0u8; length];
                s.read_exact(&mut buf)?;
                let start_lba = offset / 4096;
                let end_lba = (offset + length as u64).div_ceil(4096);
                let lba_count = (end_lba - start_lba) as u32;
                let skip = (offset % 4096) as usize;
                let result = if skip == 0 && length.is_multiple_of(4096) {
                    // Already block-aligned — write directly.
                    volume.write(start_lba, buf)
                } else {
                    // Sub-block write: read covering blocks, patch, write back.
                    volume.read(start_lba, lba_count).and_then(|mut blocks| {
                        // skip + length <= lba_count * 4096 = blocks.len() by construction.
                        debug_assert!(skip + length <= blocks.len());
                        blocks[skip..skip + length].copy_from_slice(&buf);
                        volume.write(start_lba, blocks)
                    })
                };
                match result {
                    Ok(()) => {
                        tx_reply(&mut s, 0, handle)?;
                    }
                    Err(e) => {
                        error!("[write error offset={} len={}: {}]", offset, length, e);
                        tx_reply(&mut s, 5, handle)?; // EIO
                    }
                }
            }

            NBD_CMD_DISC => {
                println!("[reads: {}, writes: {}]", reads, writes);
                break;
            }

            NBD_CMD_FLUSH => match volume.flush() {
                Ok(()) => tx_reply(&mut s, 0, handle)?,
                Err(e) => {
                    error!("[fsync error: {}]", e);
                    tx_reply(&mut s, 5, handle)?; // EIO
                }
            },

            _ => {
                tx_reply(&mut s, 22, handle)?; // EINVAL
            }
        }
    }

    Ok(())
}

// --- Wire helpers ---

fn opt_reply(s: &mut TcpStream, option: u32, reply_type: u32, data: &[u8]) -> io::Result<()> {
    s.write_all(&NBD_OPTS_REPLY_MAGIC.to_be_bytes())?;
    s.write_all(&option.to_be_bytes())?;
    s.write_all(&reply_type.to_be_bytes())?;
    s.write_all(&(data.len() as u32).to_be_bytes())?;
    s.write_all(data)?;
    Ok(())
}

fn tx_reply(s: &mut TcpStream, error: u32, handle: u64) -> io::Result<()> {
    s.write_all(&NBD_REPLY_MAGIC.to_be_bytes())?;
    s.write_all(&error.to_be_bytes())?;
    s.write_all(&handle.to_be_bytes())?;
    Ok(())
}

fn read_u16(s: &mut TcpStream) -> io::Result<u16> {
    let mut b = [0u8; 2];
    s.read_exact(&mut b)?;
    Ok(u16::from_be_bytes(b))
}

fn read_u32(s: &mut TcpStream) -> io::Result<u32> {
    let mut b = [0u8; 4];
    s.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}

fn read_u64(s: &mut TcpStream) -> io::Result<u64> {
    let mut b = [0u8; 8];
    s.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("elide-nbd-test-{}-{}", std::process::id(), n));
        p
    }

    // Bind on port 0, spawn the server, return the port.
    // The listener is already bound before the thread starts, so the client can
    // connect immediately — the OS queues the connection until accept() is called.
    fn start_server(dir: &std::path::Path, size_bytes: u64) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let dir = dir.to_path_buf();
        std::thread::spawn(move || {
            serve_volume_listener(&dir, size_bytes, listener, None, None).ok();
        });
        port
    }

    // --- NBD client helpers ---

    struct NbdClient {
        s: TcpStream,
    }

    impl NbdClient {
        /// Connect and complete the newstyle NBD handshake via NBD_OPT_GO.
        fn connect(port: u16) -> io::Result<Self> {
            let mut s = TcpStream::connect(format!("127.0.0.1:{}", port))?;

            // Server greeting
            let mut buf = [0u8; 8];
            s.read_exact(&mut buf)?;
            assert_eq!(u64::from_be_bytes(buf), NBD_MAGIC, "bad server magic");

            s.read_exact(&mut buf)?;
            assert_eq!(u64::from_be_bytes(buf), NBD_OPTS_MAGIC, "bad opts magic");

            let mut flags = [0u8; 2];
            s.read_exact(&mut flags)?; // server flags — just consume

            // Client flags (0 = support fixed newstyle only)
            s.write_all(&0u32.to_be_bytes())?;

            // Send NBD_OPT_GO with empty export name, no info requests:
            //   u32 name_length=0, u16 num_info_requests=0  (6 bytes)
            s.write_all(&NBD_OPTS_MAGIC.to_be_bytes())?;
            s.write_all(&NBD_OPT_GO.to_be_bytes())?;
            s.write_all(&6u32.to_be_bytes())?; // data length
            s.write_all(&0u32.to_be_bytes())?; // name_length = 0
            s.write_all(&0u16.to_be_bytes())?; // num_info_requests = 0

            // Consume option replies until NBD_REP_ACK
            loop {
                let mut b8 = [0u8; 8];
                s.read_exact(&mut b8)?;
                assert_eq!(
                    u64::from_be_bytes(b8),
                    NBD_OPTS_REPLY_MAGIC,
                    "bad reply magic"
                );
                let mut b4 = [0u8; 4];
                s.read_exact(&mut b4)?; // echoed option code
                s.read_exact(&mut b4)?;
                let reply_type = u32::from_be_bytes(b4);
                s.read_exact(&mut b4)?;
                let data_len = u32::from_be_bytes(b4) as usize;
                let mut data = vec![0u8; data_len];
                s.read_exact(&mut data)?;

                if reply_type == NBD_REP_ACK {
                    break;
                }
            }

            Ok(NbdClient { s })
        }

        fn read(&mut self, handle: u64, offset: u64, length: u32) -> io::Result<Vec<u8>> {
            self.s.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
            self.s.write_all(&0u16.to_be_bytes())?; // flags
            self.s.write_all(&NBD_CMD_READ.to_be_bytes())?;
            self.s.write_all(&handle.to_be_bytes())?;
            self.s.write_all(&offset.to_be_bytes())?;
            self.s.write_all(&length.to_be_bytes())?;

            let mut b4 = [0u8; 4];
            self.s.read_exact(&mut b4)?;
            assert_eq!(u32::from_be_bytes(b4), NBD_REPLY_MAGIC, "bad reply magic");
            self.s.read_exact(&mut b4)?;
            let error = u32::from_be_bytes(b4);
            let mut b8 = [0u8; 8];
            self.s.read_exact(&mut b8)?;
            assert_eq!(u64::from_be_bytes(b8), handle, "handle mismatch");

            if error != 0 {
                return Err(io::Error::other(format!("NBD read error {}", error)));
            }
            let mut data = vec![0u8; length as usize];
            self.s.read_exact(&mut data)?;
            Ok(data)
        }

        fn write(&mut self, handle: u64, offset: u64, data: &[u8]) -> io::Result<()> {
            self.s.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
            self.s.write_all(&0u16.to_be_bytes())?;
            self.s.write_all(&NBD_CMD_WRITE.to_be_bytes())?;
            self.s.write_all(&handle.to_be_bytes())?;
            self.s.write_all(&offset.to_be_bytes())?;
            self.s.write_all(&(data.len() as u32).to_be_bytes())?;
            self.s.write_all(data)?;

            let mut b4 = [0u8; 4];
            self.s.read_exact(&mut b4)?;
            assert_eq!(u32::from_be_bytes(b4), NBD_REPLY_MAGIC, "bad reply magic");
            self.s.read_exact(&mut b4)?;
            let error = u32::from_be_bytes(b4);
            let mut b8 = [0u8; 8];
            self.s.read_exact(&mut b8)?;
            assert_eq!(u64::from_be_bytes(b8), handle, "handle mismatch");

            if error != 0 {
                return Err(io::Error::other(format!("NBD write error {}", error)));
            }
            Ok(())
        }

        fn flush(&mut self, handle: u64) -> io::Result<()> {
            self.s.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
            self.s.write_all(&0u16.to_be_bytes())?;
            self.s.write_all(&NBD_CMD_FLUSH.to_be_bytes())?;
            self.s.write_all(&handle.to_be_bytes())?;
            self.s.write_all(&0u64.to_be_bytes())?; // offset
            self.s.write_all(&0u32.to_be_bytes())?; // length

            let mut b4 = [0u8; 4];
            self.s.read_exact(&mut b4)?;
            assert_eq!(u32::from_be_bytes(b4), NBD_REPLY_MAGIC, "bad reply magic");
            self.s.read_exact(&mut b4)?;
            let error = u32::from_be_bytes(b4);
            let mut b8 = [0u8; 8];
            self.s.read_exact(&mut b8)?;
            assert_eq!(u64::from_be_bytes(b8), handle, "handle mismatch");

            if error != 0 {
                return Err(io::Error::other(format!("NBD flush error {}", error)));
            }
            Ok(())
        }

        fn disconnect(mut self) -> io::Result<()> {
            self.s.write_all(&NBD_REQUEST_MAGIC.to_be_bytes())?;
            self.s.write_all(&0u16.to_be_bytes())?;
            self.s.write_all(&NBD_CMD_DISC.to_be_bytes())?;
            self.s.write_all(&0u64.to_be_bytes())?; // handle
            self.s.write_all(&0u64.to_be_bytes())?; // offset
            self.s.write_all(&0u32.to_be_bytes())?; // length
            Ok(())
        }
    }

    fn start_readonly_server(dir: &std::path::Path, size_bytes: u64) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let dir = dir.to_path_buf();
        std::thread::spawn(move || {
            serve_readonly_volume_listener(&dir, size_bytes, listener).ok();
        });
        port
    }

    // --- Tests ---

    #[test]
    fn unwritten_blocks_read_as_zeros() {
        let dir = temp_dir();
        let port = start_server(&dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();
        let buf = c.read(1, 0, 4096).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn write_then_read_roundtrip() {
        let dir = temp_dir();
        let port = start_server(&dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();

        let data: Vec<u8> = (0..4096u16).map(|i| (i & 0xff) as u8).collect();
        c.write(1, 0, &data).unwrap();
        let back = c.read(2, 0, 4096).unwrap();
        assert_eq!(back, data);

        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn multi_block_write_and_partial_read() {
        let dir = temp_dir();
        let port = start_server(&dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();

        // Write 3 blocks starting at LBA 2 (offset 8192)
        let data: Vec<u8> = (0..12288u16).map(|i| (i & 0xff) as u8).collect();
        c.write(1, 8192, &data).unwrap();

        // Read them back as a unit
        let back = c.read(2, 8192, 12288).unwrap();
        assert_eq!(back, data);

        // LBA 0 and 1 should still be zero
        let zero = c.read(3, 0, 8192).unwrap();
        assert!(zero.iter().all(|&b| b == 0));

        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn flush_succeeds() {
        let dir = temp_dir();
        let port = start_server(&dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();
        c.flush(1).unwrap();
        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn overwrite_block_returns_new_data() {
        let dir = temp_dir();
        let port = start_server(&dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();

        let first = vec![0xaau8; 4096];
        let second = vec![0xbbu8; 4096];
        c.write(1, 0, &first).unwrap();
        c.write(2, 0, &second).unwrap();
        let back = c.read(3, 0, 4096).unwrap();
        assert_eq!(back, second);

        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sub_block_write_read_roundtrip() {
        // Simulate the kernel using 512-byte sectors: write 1024 bytes at
        // offset 1024 (the ext4 superblock location), then read it back.
        let dir = temp_dir();
        let port = start_server(&dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();

        // Write 1024 bytes at byte offset 1024 (not 4096-aligned).
        let payload: Vec<u8> = (0..1024u16).map(|i| (i & 0xff) as u8).collect();
        c.write(1, 1024, &payload).unwrap();

        // Read back the same 1024 bytes.
        let back = c.read(2, 1024, 1024).unwrap();
        assert_eq!(back, payload);

        // Bytes before and after should be zero (unwritten).
        let before = c.read(3, 0, 1024).unwrap();
        assert!(before.iter().all(|&b| b == 0));
        let after = c.read(4, 2048, 2048).unwrap();
        assert!(after.iter().all(|&b| b == 0));

        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sub_block_write_preserves_neighbours() {
        // Write a full block, then overwrite part of it with a sub-block write.
        // The untouched bytes in the block should be preserved.
        let dir = temp_dir();
        let port = start_server(&dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();

        // Fill block 0 with 0xaa.
        c.write(1, 0, &vec![0xaau8; 4096]).unwrap();

        // Overwrite bytes 512..1024 with 0xbb.
        c.write(2, 512, &vec![0xbbu8; 512]).unwrap();

        // Read the whole block back.
        let back = c.read(3, 0, 4096).unwrap();
        assert!(back[..512].iter().all(|&b| b == 0xaa));
        assert!(back[512..1024].iter().all(|&b| b == 0xbb));
        assert!(back[1024..].iter().all(|&b| b == 0xaa));

        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn write_incompressible_data_roundtrip() {
        // All existing NBD tests use repetitive (compressible) data.
        // This test exercises the uncompressed path through the full NBD stack.
        let dir = temp_dir();
        let port = start_server(&dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();

        // Build a high-entropy block: LCG with coprime multiplier gives all 256
        // values exactly 16 times in 4096 bytes (entropy = 8 bits/byte).
        let data: Vec<u8> = (0..4096u16)
            .map(|i| (i as u8).wrapping_mul(0x6D).wrapping_add(0xA7))
            .collect();

        c.write(1, 0, &data).unwrap();
        let back = c.read(1, 0, 4096).unwrap();
        assert_eq!(back, data);

        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    // --- Readonly NBD tests ---

    #[test]
    fn readonly_nbd_read_returns_data() {
        let dir = temp_dir();
        let fork_dir = dir.join("default");

        let data: Vec<u8> = (0..4096u32).map(|i| (i & 0xFF) as u8).collect();

        // Populate the fork via a writable Volume, promote to segment, then drop.
        {
            let mut vol = elide_core::volume::Volume::open(&fork_dir, &fork_dir).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
        }

        let port = start_readonly_server(&fork_dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();
        let back = c.read(1, 0, 4096).unwrap();
        assert_eq!(back, data);

        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn readonly_nbd_write_returns_eperm() {
        let dir = temp_dir();
        let fork_dir = dir.join("default");

        // Create an empty fork with no WAL so ReadonlyVolume::open works.
        std::fs::create_dir_all(fork_dir.join("segments")).unwrap();
        std::fs::create_dir_all(fork_dir.join("pending")).unwrap();

        let port = start_readonly_server(&fork_dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();

        let result = c.write(1, 0, &vec![0xABu8; 4096]);
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("NBD write error 1"),
            "expected EPERM (error 1), got: {err_msg}"
        );

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn readonly_nbd_unwritten_blocks_read_as_zeros() {
        let dir = temp_dir();
        let fork_dir = dir.join("default");
        std::fs::create_dir_all(fork_dir.join("segments")).unwrap();
        std::fs::create_dir_all(fork_dir.join("pending")).unwrap();

        let port = start_readonly_server(&fork_dir, 4 * 1024 * 1024);
        let mut c = NbdClient::connect(port).unwrap();
        let buf = c.read(1, 0, 4096).unwrap();
        assert_eq!(buf, vec![0u8; 4096]);

        c.disconnect().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}
