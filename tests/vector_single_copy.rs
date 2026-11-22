//! A vector index holds each vector once: in its HNSW graph (#1973).
//!
//! Until #1973 the index kept a second copy of every vector beside the graph,
//! for persistence and for the exact paths (the brute-force fallback, the
//! exact-match merge of #1498, the candidate scan of #445). Those paths now
//! read the graph's own points. What these tests pin is that they still
//! answer exactly what they answered when they read the copy:
//!
//! - an exact scan ranks every current vector by its true distance, ties by
//!   insertion order, whatever order the graph keeps its points in;
//! - a stored vector queried with itself is found at distance zero;
//! - a replaced vector answers with its new value, the old one never, across
//!   the rebuild a fifth of dead entries triggers (#1968);
//! - a dump writes, and a load reads back, the current vectors only;
//! - `vector_bytes()` counts one copy.
//!
//! The expected answers are computed here, from the test's own vectors, with
//! `DistanceMetric::distance` -- not read back from the index under test.

use samyama::graph::types::NodeId;
use samyama::vector::index::{DistanceMetric, Quantization, VectorIndex};

const DIM: usize = 16;
// Above the 128-vector exact-search threshold, so `search` walks the graph.
const N: usize = 600;
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

/// The `k` nearest of `vectors` (node id = index + 1) by exact distance, ties
/// by node id, which is insertion order here.
fn expected(metric: DistanceMetric, vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<u64> {
    let mut scored: Vec<(u64, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u64 + 1, metric.distance(query, v)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

fn ids(results: &[(NodeId, f32)]) -> Vec<u64> {
    results.iter().map(|(id, _)| id.as_u64()).collect()
}

fn build(vectors: &[Vec<f32>], metric: DistanceMetric) -> VectorIndex {
    let mut index = VectorIndex::new(DIM, metric);
    for (i, v) in vectors.iter().enumerate() {
        index.add(NodeId::new(i as u64 + 1), v).unwrap();
    }
    index
}

#[test]
fn exact_scan_ranks_every_vector_by_its_true_distance() {
    for metric in [
        DistanceMetric::Cosine,
        DistanceMetric::L2,
        DistanceMetric::InnerProduct,
    ] {
        let vectors = corpus(N, 7);
        let index = build(&vectors, metric);
        for query in corpus(20, 99) {
            let got = index.search_exact_where(&query, K, &|_| true).unwrap();
            assert_eq!(
                ids(&got),
                expected(metric, &vectors, &query, K),
                "{metric:?}"
            );
        }
    }
}

#[test]
fn exact_scan_breaks_ties_by_insertion_order() {
    // Every vector the same: all distances tie, so the answer is decided by
    // the tie order alone. The graph keeps its points by layer, not by
    // insertion, so this fails if the scan reports the graph's order.
    let same = vec![vec![0.5_f32; DIM]; N];
    let index = build(&same, DistanceMetric::L2);
    let got = index.search_exact_where(&same[0], K, &|_| true).unwrap();
    assert_eq!(ids(&got), (1..=K as u64).collect::<Vec<_>>());
}

#[test]
fn a_stored_vector_queried_with_itself_is_found_at_distance_zero() {
    let vectors = corpus(N, 11);
    let index = build(&vectors, DistanceMetric::L2);
    for (i, v) in vectors.iter().enumerate().step_by(37) {
        let got = index.search(v, K).unwrap();
        assert_eq!(got[0].0.as_u64(), i as u64 + 1);
        assert_eq!(got[0].1, 0.0);
    }
}

#[test]
fn a_replaced_vector_answers_with_its_new_value_across_a_rebuild() {
    let mut vectors = corpus(N, 13);
    let mut index = build(&vectors, DistanceMetric::L2);
    // A third of the nodes get new vectors: enough dead entries that the
    // index rebuilds itself from the current ones at least once (#1968).
    let fresh = corpus(N / 3, 17);
    for (i, v) in fresh.iter().enumerate() {
        let node = i * 3;
        index.add(NodeId::new(node as u64 + 1), v).unwrap();
        vectors[node] = v.clone();
    }
    assert_eq!(index.len(), N);
    for query in corpus(10, 19) {
        let got = index.search_exact_where(&query, K, &|_| true).unwrap();
        assert_eq!(ids(&got), expected(DistanceMetric::L2, &vectors, &query, K));
    }
    // The new value is found exactly; the old one no longer names its node.
    let got = index.search(&fresh[0], 1).unwrap();
    assert_eq!((got[0].0.as_u64(), got[0].1), (1, 0.0));
}

#[test]
fn a_dump_round_trips_the_current_vectors() {
    let mut vectors = corpus(N, 23);
    let mut index = build(&vectors, DistanceMetric::Cosine);
    // A few replaced, below the rebuild threshold, so the dump has dead
    // entries in the graph to leave out.
    for node in [0usize, 5, 50] {
        let v = corpus(1, node as u64 + 1000).remove(0);
        index.add(NodeId::new(node as u64 + 1), &v).unwrap();
        vectors[node] = v;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vectors.hnsw");
    index.dump(&path).unwrap();
    let back = VectorIndex::load(&path, DIM, DistanceMetric::Cosine).unwrap();
    assert_eq!(back.len(), N);
    for query in corpus(10, 29) {
        let got = back.search_exact_where(&query, K, &|_| true).unwrap();
        assert_eq!(
            ids(&got),
            expected(DistanceMetric::Cosine, &vectors, &query, K)
        );
    }
}

#[test]
fn vector_bytes_counts_one_copy() {
    let vectors = corpus(N, 31);
    let f32_index = build(&vectors, DistanceMetric::L2);
    assert_eq!(f32_index.vector_bytes(), N * DIM * 4);

    let mut f16_index = VectorIndex::with_quantization(DIM, DistanceMetric::L2, Quantization::Fp16);
    for (i, v) in vectors.iter().enumerate() {
        f16_index.add(NodeId::new(i as u64 + 1), v).unwrap();
    }
    assert_eq!(f16_index.vector_bytes(), N * DIM * 2);
}
