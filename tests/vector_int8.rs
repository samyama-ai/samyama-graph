//! int8 scalar quantization with an exact rerank (#1974).
//!
//! An `int8` index holds one signed byte per dimension and one f32 scale per
//! vector, in the HNSW graph and in the copy kept for persistence: about a
//! quarter of f32's bytes. The graph is built and searched on the codes. A
//! search through the graph store asks the index for
//! `k * Quantization::rerank_factor()` candidates and ranks them again with the
//! f32 vector the node holds, so the order and the distances it reports are
//! the unquantized ones over that candidate set.

use std::collections::HashSet;

use samyama::graph::types::NodeId;
use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::vector::index::{DistanceMetric, Quantization, VectorIndex};

const DIM: usize = 64;
const N: usize = 2_000;
const K: usize = 10;
const QUERIES: usize = 100;

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

fn store_with(
    vectors: &[Vec<f32>],
    metric: DistanceMetric,
    q: Quantization,
) -> (GraphStore, Vec<NodeId>) {
    let mut store = GraphStore::new();
    store
        .create_vector_index_named(None, "Doc", "v", DIM, metric, q)
        .expect("index");
    let ids = vectors
        .iter()
        .map(|v| {
            let props: PropertyMap = [("v".to_string(), PropertyValue::Vector(v.clone()))]
                .into_iter()
                .collect();
            store.create_node_with_properties("default", vec![Label::new("Doc")], props)
        })
        .collect();
    (store, ids)
}

/// Recall@K of the store's search against an exact scan, over the same
/// queries for every index.
fn recall(store: &GraphStore, ids: &[NodeId], vectors: &[Vec<f32>], metric: DistanceMetric) -> f64 {
    let queries = corpus(QUERIES, 7);
    let mut hit = 0usize;
    for q in &queries {
        let mut exact: Vec<(NodeId, f32)> = ids
            .iter()
            .zip(vectors)
            .map(|(id, v)| (*id, metric.distance(q, v)))
            .collect();
        exact.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let truth: HashSet<NodeId> = exact.iter().take(K).map(|(id, _)| *id).collect();
        let got = store.vector_search("Doc", "v", q, K).expect("search");
        assert_eq!(got.len(), K, "a short answer");
        hit += got.iter().filter(|(id, _)| truth.contains(id)).count();
    }
    hit as f64 / (QUERIES * K) as f64
}

#[test]
fn int8_holds_about_a_quarter_of_the_bytes() {
    let vectors = corpus(500, 42);
    let mut full = VectorIndex::new(DIM, DistanceMetric::Cosine);
    let mut int8 = VectorIndex::with_quantization(DIM, DistanceMetric::Cosine, Quantization::Int8);
    for (i, v) in vectors.iter().enumerate() {
        full.add(NodeId(i as u64), v).expect("add");
        int8.add(NodeId(i as u64), v).expect("add");
    }
    let ratio = int8.vector_bytes() as f64 / full.vector_bytes() as f64;
    // One byte a dimension and a four-byte scale per vector, against four
    // bytes a dimension: (64 + 4) / 256.
    assert!(
        ratio <= 0.3,
        "int8 holds {ratio:.3} of f32's vector bytes; #1974 asks for at most 0.3"
    );
    assert!(int8.resident_bytes() < full.resident_bytes());
}

#[test]
fn int8_with_rerank_recalls_as_well_as_f32() {
    for metric in [DistanceMetric::Cosine, DistanceMetric::L2] {
        let vectors = corpus(N, 42);
        let (full, ids) = store_with(&vectors, metric, Quantization::None);
        let reference = recall(&full, &ids, &vectors, metric);
        let (int8, ids) = store_with(&vectors, metric, Quantization::Int8);
        let got = recall(&int8, &ids, &vectors, metric);
        assert!(
            reference > 0.85,
            "{metric:?}: the f32 reference itself is broken: {reference:.3}"
        );
        assert!(
            got >= reference - 0.02,
            "{metric:?}: int8 with rerank recall@{K} {got:.3}, f32 {reference:.3}"
        );
    }
}

#[test]
fn reranked_distances_are_the_unquantized_ones() {
    let vectors = corpus(N, 42);
    let metric = DistanceMetric::Cosine;
    let (store, ids) = store_with(&vectors, metric, Quantization::Int8);
    for q in corpus(20, 9) {
        let got = store.vector_search("Doc", "v", &q, K).expect("search");
        for w in got.windows(2) {
            assert!(w[0].1 <= w[1].1, "not nearest first: {got:?}");
        }
        for (id, d) in &got {
            let i = ids.iter().position(|x| x == id).expect("a known node");
            assert_eq!(
                *d,
                metric.distance(&q, &vectors[i]),
                "node {}: the reported distance is not the f32 one",
                id.0
            );
        }
    }
}

#[test]
fn an_int8_index_finds_a_vector_it_holds() {
    // Small, so the exact-scan path answers; it quantizes the query too.
    let vectors = corpus(40, 3);
    let mut index = VectorIndex::with_quantization(DIM, DistanceMetric::Cosine, Quantization::Int8);
    for (i, v) in vectors.iter().enumerate() {
        index.add(NodeId(i as u64), v).expect("add");
    }
    for (i, v) in vectors.iter().enumerate() {
        let got = index.search(v, 1).expect("search");
        assert_eq!(got[0].0, NodeId(i as u64), "vector {i} did not find itself");
    }
}

#[test]
fn an_int8_index_round_trips_through_the_f32_file() {
    // The file is f32 whatever the index holds, so a reload re-quantizes the
    // dequantized values. Small, so the exact-scan path answers and the check
    // is of the values rather than of a graph.
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = dir.path().join("v.idx");
    let vectors = corpus(40, 5);
    let mut index = VectorIndex::with_quantization(DIM, DistanceMetric::L2, Quantization::Int8);
    for (i, v) in vectors.iter().enumerate() {
        index.add(NodeId(i as u64), v).expect("add");
    }
    index.dump(&path).expect("dump");
    let back =
        VectorIndex::load_with_quantization(&path, DIM, DistanceMetric::L2, Quantization::Int8)
            .expect("load");
    assert_eq!(back.len(), index.len());
    assert_eq!(back.quantization(), Quantization::Int8);
    for (i, v) in vectors.iter().enumerate() {
        let got = back.search(v, 1).expect("search");
        assert_eq!(got[0].0, NodeId(i as u64), "vector {i} after reload");
    }
}

#[test]
fn a_zero_vector_is_held_without_dividing_by_zero() {
    let mut index = VectorIndex::with_quantization(4, DistanceMetric::L2, Quantization::Int8);
    index.add(NodeId(1), &vec![0.0; 4]).expect("add");
    index
        .add(NodeId(2), &vec![1.0, 0.0, 0.0, 0.0])
        .expect("add");
    let got = index.search(&[0.0; 4], 1).expect("search");
    assert_eq!(got[0].0, NodeId(1));
    assert_eq!(got[0].1, 0.0);
}
