//! The HNSW index finds most of the true nearest neighbours (#1328-adjacent, RUSTSEC-2026-0300).
//!
//! `hnsw_rs` went 0.2.1 -> 0.3.4 to clear RUSTSEC-2026-0300, a use-after-free
//! in `skiplist::SkipList::clear` reachable when an element's `Drop` panics.
//! 0.3.4 does not depend on `skiplist` at all, so the advisory leaves the tree
//! rather than moving to a patched version.
//!
//! That is a **major** bump of the library the vector index is built on, and an
//! approximate index is exactly the kind of thing that can keep compiling,
//! keep passing its unit tests, and quietly return worse neighbours. Nothing
//! here measured that, so nothing would have noticed.
//!
//! Measured before the bump landed, three runs each against exact cosine:
//!
//! ```text
//! 0.2.1   0.9780  0.9720  0.9730
//! 0.3.4   0.9660  0.9840  0.9820
//! ```
//!
//! The ranges overlap, so recall is unchanged within the index's own
//! run-to-run variation -- HNSW assigns layers at random and two indexes over
//! the same vectors do not agree exactly.
//!
//! # Why the floor is 0.90 and not 0.97
//!
//! The spread above is the reason. A floor set at the observed median would
//! fail on a good day, and the usual repair for a flaky test is to weaken it
//! until it cannot fail at all. 0.90 sits below every sample here and far above
//! the ~0.5 that a broken graph or a mis-scaled distance produces, so it
//! catches the failure it exists for without catching the weather.

use samyama::graph::types::NodeId;
use samyama::vector::index::{DistanceMetric, VectorIndex};
use std::collections::HashSet;

const N: usize = 2000;
const DIM: usize = 64;
const K: usize = 10;
const QUERIES: usize = 100;
const FLOOR: f64 = 0.90;

/// A fixed generator, so the corpus is the same on every run and the only
/// thing that varies is the index's own randomness.
fn lcg(seed: &mut u64) -> f32 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    ((*seed >> 33) as f32 / (1u64 << 31) as f32) - 1.0
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    dot / (na.sqrt() * nb.sqrt() + 1e-12)
}

#[test]
fn hnsw_recall_stays_above_the_floor() {
    let mut seed = 42u64;
    let vectors: Vec<Vec<f32>> = (0..N)
        .map(|_| (0..DIM).map(|_| lcg(&mut seed)).collect())
        .collect();

    let mut index = VectorIndex::new(DIM, DistanceMetric::Cosine);
    for (i, v) in vectors.iter().enumerate() {
        index.add(NodeId(i as u64), v).expect("add");
    }

    let mut qseed = 7u64;
    let (mut hit, mut total) = (0usize, 0usize);
    for _ in 0..QUERIES {
        let q: Vec<f32> = (0..DIM).map(|_| lcg(&mut qseed)).collect();

        // Exact top-K by brute force. The index is compared against the answer,
        // not against its previous self: a reference the index also produces
        // would agree with it however wrong both were.
        let mut exact: Vec<(usize, f32)> =
            vectors.iter().enumerate().map(|(i, v)| (i, cosine(&q, v))).collect();
        exact.sort_by(|a, b| b.1.partial_cmp(&a.1).expect("no NaNs in a fixed corpus"));
        let truth: HashSet<usize> = exact.iter().take(K).map(|(i, _)| *i).collect();

        let got = index.search(&q, K).expect("search");
        assert_eq!(got.len(), K, "the index returned {} of {K} asked for", got.len());
        hit += got.iter().filter(|(id, _)| truth.contains(&(id.0 as usize))).count();
        total += K;
    }

    let recall = hit as f64 / total as f64;
    assert!(
        recall >= FLOOR,
        "recall@{K} over {QUERIES} queries fell to {recall:.4} ({hit}/{total}), below the \
         {FLOOR} floor. Measured 0.966-0.984 on hnsw_rs 0.3.4 and 0.972-0.978 on 0.2.1, so \
         this is a real drop rather than the index's usual variation."
    );
}

#[test]
fn a_vector_in_the_index_is_its_own_nearest_neighbour() {
    // The control. Recall is a ratio, and a ratio can look healthy while the
    // index is answering a subtly different question; an exact-match query has
    // one right answer and no tolerance.
    let mut seed = 99u64;
    let vectors: Vec<Vec<f32>> = (0..200)
        .map(|_| (0..DIM).map(|_| lcg(&mut seed)).collect())
        .collect();
    let mut index = VectorIndex::new(DIM, DistanceMetric::Cosine);
    for (i, v) in vectors.iter().enumerate() {
        index.add(NodeId(i as u64), v).expect("add");
    }
    for (i, v) in vectors.iter().enumerate().take(25) {
        let got = index.search(v, 1).expect("search");
        assert_eq!(
            got[0].0,
            NodeId(i as u64),
            "querying with vector {i} returned {:?}",
            got[0].0
        );
    }
}
