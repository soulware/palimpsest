use std::path::Path;

use clap::{Parser, Subcommand};
use ext4_view::{Ext4, Ext4Error, PathBuf as Ext4PathBuf};

mod extents;
mod nbd;

/// Analyse ext4 disk images for dedup and delta compression potential.
#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scan an image for file extents and analyse dedup + delta compression potential
    Extents {
        image1: String,
        image2: Option<String>,
        #[arg(long, default_value_t = 3)]
        level: i32,
    },
    /// Serve a raw ext4 image over NBD, tracking which blocks are read
    Serve {
        image: String,
        #[arg(long, default_value_t = 10809)]
        port: u16,
    },
    /// Extract kernel and initrd from an ext4 image's /boot directory
    ExtractBoot {
        image: String,
        #[arg(long, default_value = ".")]
        out_dir: String,
    },
}

fn main() {
    let args = Args::parse();

    match args.command {
        Command::Extents { image1, image2, level } => {
            extents::run(Path::new(&image1), image2.as_deref().map(Path::new), level)
                .expect("extents failed");
        }

        Command::Serve { image, port } => {
            nbd::run(&image, port).expect("NBD server error");
        }

        Command::ExtractBoot { image, out_dir } => {
            extract_boot(Path::new(&image), Path::new(&out_dir)).expect("extract-boot failed");
        }
    }
}

fn extract_boot(image: &Path, out_dir: &Path) -> Result<(), Ext4Error> {
    let fs = Ext4::load_from_path(image)?;
    std::fs::create_dir_all(out_dir).ok();

    for name in &["vmlinuz", "initrd.img"] {
        let path_str = format!("/boot/{}", name);
        let src = Ext4PathBuf::new(&path_str);
        match fs.read(&src) {
            Ok(data) => {
                let dst = out_dir.join(name);
                std::fs::write(&dst, &data).expect("write failed");
                println!("Extracted /boot/{} → {} ({:.1} MB)", name, dst.display(), data.len() as f64 / (1024.0 * 1024.0));
            }
            Err(e) => eprintln!("Could not read /boot/{}: {}", name, e),
        }
    }

    Ok(())
}
