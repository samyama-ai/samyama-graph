//! ns per f32 distance, scalar against the SIMD kernels the index uses (#1976).
//!
//! The dimensions are the ones the issue names: 96 (small learned
//! embeddings), 768 (BERT-class text models) and 1536 (OpenAI's
//! `text-embedding-3-small` / `ada-002`). Each group times the scalar kernel
//! and, where the CPU has one, the SIMD kernel on the same pair of vectors,
//! so the two columns are directly comparable. The SIMD rows are named after
//! the kernel set (`avx2+fma` or `neon`); a CPU without either has only the
//! scalar rows.
//!
//!   cargo bench --bench vector_distance

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use samyama::vector::distance::{scalar, simd};

fn vector(len: usize, seed: u64) -> Vec<f32> {
    let mut s = seed;
    (0..len)
        .map(|_| {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
        })
        .collect()
}

fn bench(c: &mut Criterion) {
    let simd = simd::kernels();
    for (metric, which) in [("dot", 0), ("l2_sq", 1), ("cosine_parts", 2)] {
        let mut group = c.benchmark_group(format!("distance/{metric}"));
        for d in [96usize, 768, 1536] {
            let a = vector(d, 1);
            let b = vector(d, 2);
            group.bench_with_input(BenchmarkId::new("scalar", d), &d, |bch, _| match which {
                0 => bch.iter(|| scalar::dot(black_box(&a), black_box(&b))),
                1 => bch.iter(|| scalar::l2_sq(black_box(&a), black_box(&b))),
                _ => bch.iter(|| scalar::cosine_parts(black_box(&a), black_box(&b))),
            });
            if let Some((name, dot, l2_sq, cosine_parts)) = simd {
                group.bench_with_input(BenchmarkId::new(name, d), &d, |bch, _| match which {
                    0 => bch.iter(|| dot(black_box(&a), black_box(&b))),
                    1 => bch.iter(|| l2_sq(black_box(&a), black_box(&b))),
                    _ => bch.iter(|| cosine_parts(black_box(&a), black_box(&b))),
                });
            }
        }
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
