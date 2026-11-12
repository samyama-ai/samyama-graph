//! A vector index under churn answers as well as one built fresh (#1972).
//!
//! Deleting a node, or setting its vector to a new value, leaves its old entry
//! in the HNSW graph until a fifth of the graph is dead and the index is
//! rebuilt (#1968). Until then every search walks through those dead entries.
//! Since #1972 the walk skips them as results rather than fetching extra
//! answers and dropping the dead ones afterwards, so they cost neither result
//! slots nor search width.
//!
//! The index is sized above the point where a polluted graph is answered by a
//! linear scan instead, so these searches go through the graph.

use std::collections::{HashMap, HashSet};

use samyama::graph::types::NodeId;
use samyama::vector::index::{DistanceMetric, VectorIndex};

const DIM: usize = 16;
// Above 2 * m * ef (2 * 16 * 64) live vectors, where a search on an index
// with dead entries stops using the graph.
const N: u64 = 3_000;
const CYCLES: usize = 10;
// A tenth of the index churns each cycle: half deleted and replaced by new
// nodes, half set to a new vector in place.
const CHURN: u64 = N / 10;
const QUERIES: usize = 100;
const K: usize = 10;
// How far below a fresh build's recall the churned index may fall.
const SLACK: f64 = 0.02;

fn lcg(seed: &mut u64) -> f32 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((*seed >> 33) as f32 / (1u64 << 31) as f32) - 1.0
}

fn vector(seed: &mut u64) -> Vec<f32> {
    (0..DIM).map(|_| lcg(seed)).collect()
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// The true `K` nearest live nodes, by linear scan.
fn truth(live: &HashMap<u64, Vec<f32>>, q: &[f32]) -> HashSet<u64> {
    let mut all: Vec<(u64, f32)> = live.iter().map(|(id, v)| (*id, l2(v, q))).collect();
    all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
    all.into_iter().take(K).map(|(id, _)| id).collect()
}

fn recall(index: &VectorIndex, live: &HashMap<u64, Vec<f32>>, queries: &[Vec<f32>]) -> f64 {
    let mut hit = 0usize;
    for q in queries {
        let want = truth(live, q);
        let got = index.search(q, K).expect("search");
        for (id, _) in &got {
            assert!(
                live.contains_key(&id.0),
                "search returned node {}, which is no longer in the index",
                id.0
            );
        }
        hit += got.iter().filter(|(id, _)| want.contains(&id.0)).count();
    }
    hit as f64 / (queries.len() * K) as f64
}

fn fresh(live: &HashMap<u64, Vec<f32>>) -> VectorIndex {
    let mut ids: Vec<&u64> = live.keys().collect();
    ids.sort();
    let mut index = VectorIndex::new(DIM, DistanceMetric::L2);
    for id in ids {
        index.add(NodeId(*id), &live[id]).expect("add");
    }
    index
}

#[test]
fn churned_index_recalls_within_two_points_of_a_fresh_build() {
    let mut seed = 42u64;
    let mut index = VectorIndex::new(DIM, DistanceMetric::L2);
    let mut live: HashMap<u64, Vec<f32>> = HashMap::new();
    for id in 0..N {
        let v = vector(&mut seed);
        index.add(NodeId(id), &v).expect("add");
        live.insert(id, v);
    }
    let mut qseed = 7u64;
    let queries: Vec<Vec<f32>> = (0..QUERIES).map(|_| vector(&mut qseed)).collect();

    let mut next_id = N;
    let mut measured_with_dead = 0;
    for cycle in 1..=CYCLES {
        let mut ids: Vec<u64> = live.keys().copied().collect();
        ids.sort();
        // A different, deterministic tenth each cycle.
        let picked: Vec<u64> = ids
            .iter()
            .copied()
            .skip(cycle * 7 % 10)
            .step_by(10)
            .take(CHURN as usize)
            .collect();
        for (i, id) in picked.into_iter().enumerate() {
            if i % 2 == 0 {
                assert!(index.remove(NodeId(id)), "node {id} was in the index");
                live.remove(&id);
                let v = vector(&mut seed);
                index.add(NodeId(next_id), &v).expect("add");
                live.insert(next_id, v);
                next_id += 1;
            } else {
                let v = vector(&mut seed);
                index.add(NodeId(id), &v).expect("set");
                live.insert(id, v);
            }
        }
        assert_eq!(index.len(), live.len(), "cycle {cycle}: live count");

        // Dead entries are never answered, at every cycle.
        let churned = recall(&index, &live, &queries);

        // Compare against a fresh build where the graph still holds dead
        // entries; right after a rebuild the two are the same thing.
        if index.superseded() > 0 && cycle >= CYCLES - 2 {
            measured_with_dead += 1;
            let baseline = recall(&fresh(&live), &live, &queries);
            assert!(
                churned >= baseline - SLACK,
                "cycle {cycle}: recall@{K} {churned:.3} with {} dead entries in the graph, \
                 against {baseline:.3} for a fresh build of the same {} vectors",
                index.superseded(),
                live.len()
            );
        }
    }
    assert!(
        measured_with_dead > 0,
        "no comparison ran with dead entries in the graph; the churn schedule \
         no longer leaves any, so this test is not testing #1972"
    );
}

#[test]
fn a_deleted_vector_is_not_found_by_its_own_value() {
    let mut seed = 11u64;
    let mut index = VectorIndex::new(DIM, DistanceMetric::L2);
    let mut vectors = Vec::new();
    for id in 0..N {
        let v = vector(&mut seed);
        index.add(NodeId(id), &v).expect("add");
        vectors.push(v);
    }
    // Fewer than a fifth, so no rebuild clears them.
    let gone: Vec<u64> = (0..N).step_by(20).collect();
    for id in &gone {
        assert!(index.remove(NodeId(*id)));
    }
    assert!(index.superseded() > 0, "a rebuild cleared the dead entries");
    for id in &gone {
        let hits = index.search(&vectors[*id as usize], K).expect("search");
        assert_eq!(hits.len(), K, "a short answer from a {N}-vector index");
        assert!(
            hits.iter().all(|(hit, _)| hit.0 != *id),
            "deleted node {id} was returned for its own vector"
        );
    }
}
