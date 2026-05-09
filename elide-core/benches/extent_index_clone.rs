//! Benchmark: cost of `Arc::make_mut` on `extent_index` per write.
//!
//! Background: `ExtentIndex.inner` is a `std::HashMap<blake3::Hash,
//! ExtentLocation>`.  After every write the actor publishes a fresh
//! `ReadSnapshot` via `ArcSwap`, which keeps the previous `Arc` alive.
//! The next non-dedup write calls `Arc::make_mut(&mut self.extent_index)`,
//! which deep-clones the entire `HashMap` because the strong count is
//! ≥ 2.  Cost scales with the number of indexed extents, not the write
//! size.
//!
//! This benchmark quantifies that cost at realistic index sizes and
//! contrasts it with the persistent-map (`imbl::GenericHashMap` /
//! HAMT) shape we'd switch to if the clone proves dominant.  It also
//! measures lookup throughput on both shapes, since `imbl` lookups are
//! O(log n) and reads vastly outnumber writes — a regression there
//! could cancel any write-side win.
//!
//! Run with:
//!   cargo bench -p elide-core --bench extent_index_clone
//!
//! What to look for:
//!   * `clone_then_insert/std/N` — the production cost.  Linear in N.
//!   * `clone_then_insert/imbl/N` — persistent-map alternative.  ~log N.
//!   * `lookup/std/N` vs `lookup/imbl/N` — read-side regression of the
//!     HAMT switch.

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use imbl::GenericHashMap;
use imbl::shared_ptr::DefaultSharedPtr;
use ulid::Ulid;

use elide_core::blake3_id_hasher::Blake3IdBuildHasher;
use elide_core::extentindex::{BodySource, ExtentLocation};

type StdMap = HashMap<blake3::Hash, ExtentLocation, Blake3IdBuildHasher>;
type ImblMap = GenericHashMap<blake3::Hash, ExtentLocation, Blake3IdBuildHasher, DefaultSharedPtr>;

/// Generate `n` blake3 hashes (all `inline_data: None`, the typical case)
/// and a parallel set of `n` distinct hashes used for "absent" lookups
/// and as fresh insert keys per benchmark iteration.
fn make_keys(n: usize) -> (Vec<blake3::Hash>, Vec<blake3::Hash>) {
    let present: Vec<blake3::Hash> = (0..n as u32)
        .map(|i| blake3::hash(&i.to_le_bytes()))
        .collect();
    let absent: Vec<blake3::Hash> = (n as u32..(n as u32 * 2))
        .map(|i| blake3::hash(&i.to_le_bytes()))
        .collect();
    (present, absent)
}

fn dummy_location(seed: u32) -> ExtentLocation {
    ExtentLocation {
        segment_id: Ulid::from_parts(0, seed as u128),
        body_offset: 0,
        body_length: 4096,
        compressed: false,
        body_source: BodySource::Local,
        body_section_start: 0,
        inline_data: None,
    }
}

fn build_std_map(keys: &[blake3::Hash]) -> StdMap {
    let mut m: StdMap = StdMap::with_hasher(Blake3IdBuildHasher);
    m.reserve(keys.len());
    for (i, k) in keys.iter().enumerate() {
        m.insert(*k, dummy_location(i as u32));
    }
    m
}

fn build_imbl_map(keys: &[blake3::Hash]) -> ImblMap {
    let mut m = ImblMap::with_hasher(Blake3IdBuildHasher);
    for (i, k) in keys.iter().enumerate() {
        m.insert(*k, dummy_location(i as u32));
    }
    m
}

/// Models the production hot-path mutation: an `Arc<Map>` whose strong
/// count is ≥ 2 (because `ArcSwap` is holding the previous publish),
/// and a write that calls `Arc::make_mut` then inserts one new entry.
///
/// The benchmark holds two `Arc`s outside the timed region; per iter we
/// clone one of them (cheap refcount bump) so `make_mut` sees `sc > 1`
/// and is forced to deep-clone.  The cloned-out result is dropped at
/// the end of the iter, leaving the held-aside `Arc` unchanged for the
/// next iter.
fn bench_clone_then_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("clone_then_insert");
    group.sample_size(20);
    group.throughput(Throughput::Elements(1));

    for &n in &[1_024usize, 10_240, 102_400, 1_048_576] {
        let (present, absent) = make_keys(n);

        // std::HashMap variant — production shape.
        let std_arc = Arc::new(build_std_map(&present));
        let _std_holder = Arc::clone(&std_arc);
        group.bench_with_input(BenchmarkId::new("std", n), &n, |b, _| {
            let mut i = 0usize;
            b.iter_batched(
                || (Arc::clone(&std_arc), absent[i % n]),
                |(mut a, key)| {
                    Arc::make_mut(&mut a).insert(key, dummy_location(0));
                    std::hint::black_box(a);
                },
                BatchSize::SmallInput,
            );
            i = i.wrapping_add(1);
            std::hint::black_box(i);
        });
        drop(_std_holder);
        drop(std_arc);

        // imbl persistent-HAMT variant — proposed alternative.
        let imbl_arc = Arc::new(build_imbl_map(&present));
        let _imbl_holder = Arc::clone(&imbl_arc);
        group.bench_with_input(BenchmarkId::new("imbl", n), &n, |b, _| {
            let mut i = 0usize;
            b.iter_batched(
                || (Arc::clone(&imbl_arc), absent[i % n]),
                |(mut a, key)| {
                    Arc::make_mut(&mut a).insert(key, dummy_location(0));
                    std::hint::black_box(a);
                },
                BatchSize::SmallInput,
            );
            i = i.wrapping_add(1);
            std::hint::black_box(i);
        });
        drop(_imbl_holder);
        drop(imbl_arc);
    }

    group.finish();
}

/// Lookup throughput on a steady-state map.  One present + one absent
/// per iter so we capture the hit and miss costs that the dedup-REF
/// check (`extent_index.lookup(&hash).is_some()`) and the read path
/// (`extent_index.lookup(&er.hash)`) both pay.
fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup");
    group.throughput(Throughput::Elements(2));

    for &n in &[1_024usize, 10_240, 102_400, 1_048_576] {
        let (present, absent) = make_keys(n);

        let std_map = build_std_map(&present);
        group.bench_with_input(BenchmarkId::new("std", n), &n, |b, _| {
            let mut i = 0usize;
            b.iter(|| {
                let p = std::hint::black_box(&present[i % n]);
                let a = std::hint::black_box(&absent[i % n]);
                i = i.wrapping_add(1);
                std::hint::black_box(std_map.get(p));
                std::hint::black_box(std_map.get(a));
            });
        });

        let imbl_map = build_imbl_map(&present);
        group.bench_with_input(BenchmarkId::new("imbl", n), &n, |b, _| {
            let mut i = 0usize;
            b.iter(|| {
                let p = std::hint::black_box(&present[i % n]);
                let a = std::hint::black_box(&absent[i % n]);
                i = i.wrapping_add(1);
                std::hint::black_box(imbl_map.get(p));
                std::hint::black_box(imbl_map.get(a));
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_clone_then_insert, bench_lookup);
criterion_main!(benches);
