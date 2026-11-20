//! Checkpoint, diff and rollback cost (#2093, Samyama Smriti Phase 0).
//!
//! The Smriti spec's hypothesis is that a checkpoint, a turn's worth of
//! writes (50 mutations) and a rollback together cost under 5 ms on a
//! working-memory-sized graph, and that the cost follows the mutations, not
//! the graph. This measures both: the same 50 mutations against synthetic
//! stores of increasing size. A flat line across sizes is the O(mutations)
//! claim; the absolute figure is only meaningful next to the calibration line
//! printed first (#529).
//!
//! `cargo bench --bench checkpoint_rollback_benchmark`

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use std::hint::black_box;
use std::time::{Duration, Instant};

#[path = "common/bench_setup.rs"]
mod bench_setup;

const SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];
const MUTATIONS: usize = 50;
const ROUNDS: usize = 200;
const T: &str = "default";

/// A store of `nodes` nodes with two properties each and about two
/// relationships per node.
fn synthetic(nodes: usize) -> (GraphStore, Vec<NodeId>) {
    let mut store = GraphStore::new();
    let mut ids = Vec::with_capacity(nodes);
    for i in 0..nodes {
        let id = store.create_node(if i % 2 == 0 { "Fact" } else { "Entity" });
        store
            .set_node_property(T, id, "id", PropertyValue::Integer(i as i64))
            .unwrap();
        store
            .set_node_property(T, id, "score", PropertyValue::Float(i as f64 / 7.0))
            .unwrap();
        ids.push(id);
    }
    for i in 0..nodes {
        for hop in [1, 7] {
            let e = store
                .create_edge(ids[i], ids[(i + hop) % nodes], "RELATES")
                .unwrap();
            if hop == 1 {
                store
                    .set_edge_property(e, "w", PropertyValue::Integer(1))
                    .unwrap();
            }
        }
    }
    (store, ids)
}

/// A turn's writes: new facts, updated properties, new and dropped
/// relationships, and a forgotten node, spread over the graph.
fn a_turn(store: &mut GraphStore, ids: &[NodeId], round: usize) {
    let n = ids.len();
    let pick = |k: usize| ids[(round * 7919 + k * 104_729) % n];
    for k in 0..MUTATIONS {
        match k % 5 {
            0 => {
                let id = store.create_node("Fact");
                store
                    .set_node_property(T, id, "id", PropertyValue::Integer(-(k as i64)))
                    .unwrap();
            }
            1 | 2 => store
                .set_node_property(T, pick(k), "score", PropertyValue::Float(k as f64))
                .unwrap(),
            3 => {
                store.create_edge(pick(k), pick(k + 1), "RELATES").unwrap();
            }
            _ => {
                let victim = pick(k);
                if k == 4 {
                    store.delete_node(T, victim).unwrap();
                } else {
                    store.remove_node_property(victim, "id");
                }
            }
        }
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    println!("checkpoint + {MUTATIONS} mutations + rollback, {ROUNDS} rounds per size");
    println!(
        "{:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "nodes", "p50", "p99", "rollback", "diff p50"
    );
    for &size in &SIZES {
        let (mut store, ids) = synthetic(size);
        let mut total = Vec::with_capacity(ROUNDS);
        let mut rollback_only = Vec::with_capacity(ROUNDS);
        let mut diff_only = Vec::with_capacity(ROUNDS);
        for round in 0..ROUNDS {
            let started = Instant::now();
            let v = store.checkpoint().unwrap();
            a_turn(&mut store, &ids, round);
            let diffed = Instant::now();
            black_box(store.diff_since(v).unwrap());
            diff_only.push(diffed.elapsed());
            let rolling = Instant::now();
            let stats = store.rollback_to(v).unwrap();
            rollback_only.push(rolling.elapsed());
            total.push(started.elapsed() - diff_only[round]);
            assert!(stats.failures.is_empty(), "{:?}", stats.failures);
            black_box(stats);
        }
        total.sort();
        rollback_only.sort();
        diff_only.sort();
        let ms = |d: Duration| format!("{:.3} ms", d.as_secs_f64() * 1000.0);
        println!(
            "{:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
            size,
            ms(percentile(&total, 0.5)),
            ms(percentile(&total, 0.99)),
            ms(percentile(&rollback_only, 0.5)),
            ms(percentile(&diff_only, 0.5)),
        );
    }

    bench_setup::report_drift(calibration);
}
