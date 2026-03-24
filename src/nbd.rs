use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

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
        libc::signal(libc::SIGUSR1, sigusr1_handler as *const () as libc::sighandler_t);
    }
}

fn report_path() -> String {
    format!("/tmp/palimpsest-{}.report", std::process::id())
}

fn print_report(accessed: &HashSet<u64>, total_chunks: u64, chunk_size: usize, image_size: u64) {
    let n = accessed.len();
    let lines = format!(
        "\n=== Chunk Report ===\n  Chunks read: {} / {} ({:.1}%)\n  Data read:   {:.1} MB / {:.1} MB\n",
        n, total_chunks,
        100.0 * n as f64 / total_chunks as f64,
        n as f64 * chunk_size as f64 / (1024.0 * 1024.0),
        image_size as f64 / (1024.0 * 1024.0),
    );
    print!("{}", lines);
    let _ = std::fs::write(report_path(), &lines);
}

pub fn run(image_path: &str, port: u16, chunk_size: usize) -> io::Result<()> {
    let image_size = std::fs::metadata(image_path)?.len();
    let total_chunks = (image_size + chunk_size as u64 - 1) / chunk_size as u64;
    let accessed: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

    install_sigusr1_handler();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;
    println!("NBD server on 127.0.0.1:{}", port);
    println!("Image: {} ({:.1} MB, {} chunks)", image_path, image_size as f64 / (1024.0 * 1024.0), total_chunks);
    println!("Connect QEMU with: -drive file=nbd://127.0.0.1:{},format=raw,if=virtio", port);
    println!("Send SIGUSR1 (kill -USR1 {}) for a mid-boot report", std::process::id());
    println!("Report file:  {}", report_path());
    println!("Waiting for connection...\n");

    for stream in listener.incoming() {
        let stream = stream?;
        println!("[connected: {}]", stream.peer_addr()?);

        let result = handle_connection(stream, image_path, image_size, chunk_size, total_chunks, Arc::clone(&accessed));

        let accessed = accessed.lock().unwrap();
        println!("\n=== Boot Read Report ===");
        println!("  Chunks read from base: {} / {} ({:.1}%)",
            accessed.len(), total_chunks,
            100.0 * accessed.len() as f64 / total_chunks as f64);
        println!("  Data read (approx):    {:.1} MB / {:.1} MB",
            accessed.len() as f64 * chunk_size as f64 / (1024.0 * 1024.0),
            image_size as f64 / (1024.0 * 1024.0));

        if let Err(e) = result {
            eprintln!("[connection error: {}]", e);
        } else {
            println!("[disconnected]");
        }

        // Accept another connection (e.g. VM reboot) or break here
        break;
    }

    Ok(())
}

fn handle_connection(
    mut s: TcpStream,
    image_path: &str,
    image_size: u64,
    chunk_size: usize,
    total_chunks: u64,
    accessed: Arc<Mutex<HashSet<u64>>>,
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
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad option magic"));
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
            Err(e) if e.kind() == io::ErrorKind::WouldBlock
                   || e.kind() == io::ErrorKind::TimedOut => {
                if PRINT_STATS.swap(false, Ordering::Relaxed) {
                    let acc = accessed.lock().unwrap();
                    print_report(&acc, total_chunks, chunk_size, image_size);
                }
                continue;
            }
            Err(e) => return Err(e),
        };
        if magic != NBD_REQUEST_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad request magic"));
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

                // Track which base chunks were accessed
                {
                    let mut acc = accessed.lock().unwrap();
                    let first = offset / chunk_size as u64;
                    let last = (offset + length as u64 - 1) / chunk_size as u64;
                    for idx in first..=last {
                        acc.insert(idx);
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
            let acc = accessed.lock().unwrap();
            print_report(&acc, total_chunks, chunk_size, image_size);
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
fn cow_write(cow: &mut HashMap<u64, Vec<u8>>, file: &mut File, image_size: u64, offset: u64, buf: &[u8]) -> io::Result<()> {
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
