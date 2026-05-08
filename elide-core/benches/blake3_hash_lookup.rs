//! Benchmark: HashMap<blake3::Hash, V> lookup with the default SipHash hasher
//! versus the identity-style hasher in `crate::blake3_id_hasher`.
//!
//! Demonstrates the rationale for `Blake3IdBuildHasher` in ExtentIndex.
//! `blake3::Hash` keys are already cryptographically uniform; routing them
//! through SipHash-1-3 mixes 32 bytes per lookup for no distinguishability
//! gain. The XOR-fold hasher just reduces the bytes to 8 and returns them.
//!
//! Run with:
//!   cargo bench -p elide-core --bench blake3_hash_lookup

use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use elide_core::blake3_id_hasher::Blake3HashMap;

/// Generate `n` distinct `blake3::Hash` values, plus an interleaved
/// `present`/`absent` lookup pattern so the benchmark covers both hits and
/// misses (the latter still pays the hash even though there's no entry).
fn make_keys(n: usize) -> (Vec<blake3::Hash>, Vec<blake3::Hash>) {
    let present: Vec<blake3::Hash> = (0..n as u32)
        .map(|i| blake3::hash(&i.to_le_bytes()))
        .collect();
    let absent: Vec<blake3::Hash> = (n as u32..(n as u32 * 2))
        .map(|i| blake3::hash(&i.to_le_bytes()))
        .collect();
    (present, absent)
}

fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("blake3_hash_map_lookup");
    // Per-iteration: one present + one absent lookup (2 ops). We size
    // throughput in keys looked up so /sec scales sensibly.
    group.throughput(Throughput::Elements(2));

    for &n in &[64usize, 1024, 16384] {
        let (present, absent) = make_keys(n);

        // SipHash (std default).
        let sip_map: HashMap<blake3::Hash, u32> = present
            .iter()
            .enumerate()
            .map(|(i, h)| (*h, i as u32))
            .collect();
        group.bench_with_input(BenchmarkId::new("siphash", n), &n, |b, _| {
            let mut i = 0usize;
            b.iter(|| {
                let p = std::hint::black_box(&present[i % n]);
                let a = std::hint::black_box(&absent[i % n]);
                i = i.wrapping_add(1);
                std::hint::black_box(sip_map.get(p));
                std::hint::black_box(sip_map.get(a));
            });
        });

        // Blake3IdHasher (XOR-fold of the 32-byte payload).
        let mut id_map: Blake3HashMap<u32> = Blake3HashMap::default();
        for (i, h) in present.iter().enumerate() {
            id_map.insert(*h, i as u32);
        }
        group.bench_with_input(BenchmarkId::new("blake3_id", n), &n, |b, _| {
            let mut i = 0usize;
            b.iter(|| {
                let p = std::hint::black_box(&present[i % n]);
                let a = std::hint::black_box(&absent[i % n]);
                i = i.wrapping_add(1);
                std::hint::black_box(id_map.get(p));
                std::hint::black_box(id_map.get(a));
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_lookup);
criterion_main!(benches);
