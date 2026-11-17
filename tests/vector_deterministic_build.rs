//! The same vectors, added in the same order, build the same index (#2120).
//!
//! `hnsw_rs` 0.3.4 seeded its layer generator from the OS, so every
//! `VectorIndex` drew its layers afresh and two builds over the same input
//! were two different graphs. Every vector test that asserts a recall, a
//! ranking or a round trip was therefore asserting it about a random graph,
//! and four of them failed now and then on unmodified `dev` (#2120).
//!
//! 0.3.5 seeds that generator with a constant. Insertion here is sequential,
//! and search draws nothing at random, so an index is now a function of its
//! input. These tests pin that: if a later `hnsw_rs` goes back to an OS seed,
//! or the index starts inserting in parallel, they fail here, by name, rather
//! than as a recall test that turns red once in twenty runs.

use samyama::graph::types::NodeId;
use samyama::vector::index::{DistanceMetric, Quantization, VectorIndex};

const DIM: usize = 32;
// Above the 128-vector exact-search threshold, so search goes through the
// graph and the answer depends on how it was built.
const N: usize = 600;
const QUERIES: usize = 50;
const K: usize = 10;

fn lcg(seed: &mut u64) -> f32 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((*seed >> 33) as f32 / (1u64 << 31) as f32) - 1.0
}

fn corpus(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut seed = seed;
    (0..n)
        .map(|_| (0..DIM).map(|_| lcg(&mut seed)).collect())
        .collect()
}

fn build(vectors: &[Vec<f32>], metric: DistanceMetric, q: Quantization) -> VectorIndex {
    let mut index = VectorIndex::with_quantization(DIM, metric, q);
    for (i, v) in vectors.iter().enumerate() {
        index.add(NodeId(i as u64), v).expect("add");
    }
    index
}

/// Every answer, ids and scores, for a fixed query set. Search width is left
/// at its default and kept narrow on purpose: a wide search can hide two
/// different graphs behind the same answers.
fn answers(index: &VectorIndex) -> Vec<Vec<(u64, u32)>> {
    let queries = corpus(QUERIES, 7);
    queries
        .iter()
        .map(|q| {
            index
                .search(q, K)
                .expect("search")
                .into_iter()
                .map(|(id, score)| (id.0, score.to_bits()))
                .collect()
        })
        .collect()
}

fn assert_two_builds_agree(metric: DistanceMetric, q: Quantization) {
    let vectors = corpus(N, 42);
    let first = answers(&build(&vectors, metric, q));
    let second = answers(&build(&vectors, metric, q));
    for (i, (a, b)) in first.iter().zip(&second).enumerate() {
        assert_eq!(
            a, b,
            "query {i}: two builds over the same {N} vectors answered differently \
             ({metric:?}, {q:?}). The index is random again; see #2120."
        );
    }
}

#[test]
fn two_cosine_builds_answer_identically() {
    assert_two_builds_agree(DistanceMetric::Cosine, Quantization::None);
}

#[test]
fn two_l2_builds_answer_identically() {
    assert_two_builds_agree(DistanceMetric::L2, Quantization::None);
}

#[test]
fn two_quantized_builds_answer_identically() {
    assert_two_builds_agree(DistanceMetric::Cosine, Quantization::Fp16);
}
