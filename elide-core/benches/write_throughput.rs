//! Benchmark: end-to-end `VolumeClient::write` throughput on a populated
//! volume, single- and multi-threaded.
//!
//! Background: the write hot path was reshaped over PRs #305, #306,
//! #309 — hash + lz4 hoisted off the volume mutex, signal-then-block on
//! a full mailbox, `extent_index` switched to an `imbl` HAMT so
//! `Arc::make_mut` is path-copy.  This bench runs the whole stack —
//! `VolumeClient::write` → mutex → WAL pwrite → snapshot publish — on
//! a real `Volume` with the index pre-populated to a representative
//! size, so we can see where write latency actually lands now.
//!
//! What to look for:
//!   * `single/incompressible_4k/N`     — per-write latency as a
//!     function of index size; should be ~flat with the HAMT.
//!   * `multi/incompressible_4k/threadsT/N` — aggregate throughput
//!     across `T` writer threads on a populated volume.  T > 1 is the
//!     scenario the hash + lz4 hoist (#305) was aimed at: real ublk
//!     traffic has 8 worker threads per queue, all hashing in parallel
//!     and serialising only on the WAL append.
//!
//! Numbers depend on local disk speed (the WAL pwrite is real I/O), so
//! treat absolute latencies as a snapshot of *this* host rather than a
//! portable target.  Relative comparisons across N and across T are
//! the value.
//!
//! Run with:
//!   cargo bench -p elide-core --bench write_throughput

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use elide_core::actor::{VolumeClient, spawn};
use elide_core::volume::Volume;

const BLOCK_SIZE: usize = 4096;

fn write_test_keypair(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let key = elide_core::signing::generate_keypair(
        dir,
        elide_core::signing::VOLUME_KEY_FILE,
        elide_core::signing::VOLUME_PUB_FILE,
    )
    .unwrap();
    elide_core::signing::write_provenance(
        dir,
        &key,
        elide_core::signing::VOLUME_PROVENANCE_FILE,
        &elide_core::signing::ProvenanceLineage::default(),
    )
    .unwrap();
}

/// Generate `BLOCK_SIZE` of pseudo-random (incompressible) bytes derived
/// deterministically from `seed`.  Each `seed` produces a distinct block,
/// so seeded LBAs hash to distinct content — no dedup-REF, no no-op skip.
fn incompressible_block(seed: u64) -> [u8; BLOCK_SIZE] {
    let mut buf = [0u8; BLOCK_SIZE];
    // Simple LCG, same constants as the existing compression bench.
    let mut state: u64 = 0xdeadbeef_cafebabe_u64.wrapping_mul(seed.wrapping_add(1));
    for byte in buf.iter_mut() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *byte = (state >> 33) as u8;
    }
    buf
}

/// Live volume in a tempdir, ready for benching.  The actor thread runs
/// detached — process exit cleans it up; we don't need clean shutdown
/// inside a benchmark.
struct VolumeFixture {
    handle: VolumeClient,
    _dir: tempfile::TempDir,
}

impl VolumeFixture {
    /// Spin up a fresh volume, pre-populate it with `n` distinct 4 KiB
    /// extents (incompressible content, distinct LBAs), drain to
    /// `index/` + `cache/` so the WAL is empty going into the bench.
    /// LBAs `0..n` are now occupied; the bench writes start at `n`.
    fn populated(n: usize) -> Self {
        let dir = tempfile::TempDir::new().unwrap();
        write_test_keypair(dir.path());
        let vol = Volume::open(dir.path(), dir.path()).unwrap();
        let (actor, handle) = spawn(vol);
        std::thread::Builder::new()
            .name("bench-actor".into())
            .spawn(move || actor.run())
            .unwrap();

        for i in 0..n as u64 {
            let buf = incompressible_block(i);
            handle.write(i, &buf).unwrap();
        }
        // Promote any pending WAL contents so the bench starts with a
        // fresh, empty WAL — the per-iter timings shouldn't include
        // the cost of the populate-phase WAL rolling underneath them.
        handle.promote_wal().unwrap();

        Self { handle, _dir: dir }
    }
}

fn bench_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("single");
    group.throughput(Throughput::Bytes(BLOCK_SIZE as u64));
    group.sample_size(50);

    for &n in &[0usize, 1_000, 10_000, 100_000] {
        let fixture = VolumeFixture::populated(n);
        // LBA counter survives across iters so each iter gets a
        // distinct (lba, hash) pair — never a no-op skip, never a
        // dedup hit.
        let next_lba = AtomicU64::new(n as u64);

        group.bench_with_input(BenchmarkId::new("incompressible_4k", n), &n, |b, _| {
            b.iter_batched(
                || {
                    let lba = next_lba.fetch_add(1, Ordering::Relaxed);
                    (lba, incompressible_block(lba))
                },
                |(lba, buf)| {
                    fixture.handle.write(lba, &buf).unwrap();
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_multi_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi");
    group.sample_size(20);

    /// How many writes each spawned thread issues per Criterion iter.
    /// Big enough that thread-spawn overhead is amortised below per-iter
    /// noise; small enough that a Criterion sample doesn't blow up the
    /// volume directory.
    const WRITES_PER_THREAD: u64 = 256;

    for &threads in &[1usize, 4, 8] {
        for &n in &[10_000usize, 100_000] {
            let fixture = Arc::new(VolumeFixture::populated(n));
            let next_lba = Arc::new(AtomicU64::new(n as u64));
            let total_bytes = (threads as u64) * WRITES_PER_THREAD * BLOCK_SIZE as u64;
            group.throughput(Throughput::Bytes(total_bytes));

            group.bench_function(
                BenchmarkId::new(format!("incompressible_4k/threads{threads}"), n),
                |b| {
                    b.iter(|| {
                        std::thread::scope(|s| {
                            for _ in 0..threads {
                                let f = Arc::clone(&fixture);
                                let counter = Arc::clone(&next_lba);
                                s.spawn(move || {
                                    for _ in 0..WRITES_PER_THREAD {
                                        let lba = counter.fetch_add(1, Ordering::Relaxed);
                                        let buf = incompressible_block(lba);
                                        f.handle.write(lba, &buf).unwrap();
                                    }
                                });
                            }
                        });
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_single_thread, bench_multi_thread);
criterion_main!(benches);
