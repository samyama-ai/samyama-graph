//! Allocation-count gates for the hot read and ingest paths (#2207, part 2).
//!
//! A per-row allocation added to a hot loop -- an unintended `.clone()`, a
//! `format!` per row -- does not show reliably in a wall-clock benchmark on a
//! shared CI runner, where timing moves 20-30% run to run. An **allocation
//! count** does not move at all: measured twice, every read path here gave the
//! same count to the call. So these gates are exact and cannot flake.
//!
//! Each path is measured at two sizes and gated on its **slope**, allocations
//! per row: fixed per-query work (parse cache, plan, result header) cancels
//! out, and a regression that adds one allocation per row adds 1.0 to the
//! slope. The ceilings are today's slope + 0.5, so such a regression always
//! fails and nothing smaller than half an allocation per row can drift in.
//!
//! **When a ceiling fails,** read the message: it names the path, the slope
//! and the ceiling. If the extra allocation is the point of the change, lower
//! cost elsewhere or raise the ceiling in the same commit and say why -- the
//! same rule as the clippy ratchet. If it is not, it is the regression this
//! file exists to catch. Lower a ceiling when a change makes a path cheaper.
//!
//! Measured on `dev` 8ab8e804 + #2271, debug build (what CI runs):
//!
//! | path | slope | ceiling |
//! |---|---:|---:|
//! | index range scan, `RETURN p.i` | 4.01 / row | 4.5 |
//! | 1-hop expand from an anchor, `RETURN b.i` | 2.00 / row | 2.5 |
//! | `UNWIND … CREATE (:Q {i: i})` | 26.0 / node | 26.5 |
//! | indexed point lookup (one row) | 162 calls | 170 |
//!
//! Its own binary, because the counting allocator is global, and its tests
//! one at a time, because the count is process-wide.

#[path = "../benches/common/peak_alloc.rs"]
mod peak_alloc;

use std::sync::Mutex;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::{BoundParams, QueryEngine};

static SERIAL: Mutex<()> = Mutex::new(());

const RANGE_SCAN_PER_ROW: f64 = 4.5;
const EXPAND_PER_ROW: f64 = 2.5;
const INGEST_PER_NODE: f64 = 26.5;
const POINT_LOOKUP_CALLS: usize = 170;

fn n(v: i64) -> BoundParams {
    BoundParams::from([("n".to_string(), PropertyValue::Integer(v))])
}

/// Allocation calls made by `f`, run once to warm and then once counted.
fn calls(mut f: impl FnMut()) -> usize {
    f();
    let start = peak_alloc::calls();
    f();
    peak_alloc::calls() - start
}

fn slope(small: usize, large: usize, rows_small: usize, rows_large: usize) -> f64 {
    (large as f64 - small as f64) / (rows_large - rows_small) as f64
}

/// 5,000 indexed `:P` nodes, and two hubs with 100 and 1,000 out-edges.
fn graph() -> GraphStore {
    let e = QueryEngine::new();
    let mut g = GraphStore::new();
    e.execute_mut("UNWIND range(0, 4999) AS i CREATE (:P {i: i, name: 'n' + toString(i)})",
                  &mut g, "default").unwrap();
    e.execute_mut("CREATE INDEX ON :P(i)", &mut g, "default").unwrap();
    for (hub, k) in [("H100", 100), ("H1000", 1000)] {
        e.execute_mut(&format!("CREATE (:{hub})"), &mut g, "default").unwrap();
        e.execute_mut(&format!("MATCH (h:{hub}), (p:P) WHERE p.i < {k} CREATE (h)-[:R]->(p)"),
                      &mut g, "default").unwrap();
    }
    g
}

#[test]
fn an_indexed_range_scan_allocates_a_fixed_amount_per_row() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (g, e) = (graph(), QueryEngine::new());
    let q = "MATCH (p:P) WHERE p.i >= 0 AND p.i < $n RETURN p.i AS i";
    let run = |k| calls(|| assert_eq!(e.execute_with_params_within(q, &g, &n(k), None).unwrap().records.len(), k as usize));
    let s = slope(run(100), run(1000), 100, 1000);
    assert!(s <= RANGE_SCAN_PER_ROW, "range scan: {s:.2} allocations/row, ceiling {RANGE_SCAN_PER_ROW}");
}

#[test]
fn an_expand_from_an_anchor_allocates_a_fixed_amount_per_row() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (g, e) = (graph(), QueryEngine::new());
    let run = |hub: &str, k: usize| {
        let q = format!("MATCH (h:{hub})-[:R]->(b) RETURN b.i AS i");
        calls(|| assert_eq!(e.execute_with_params_within(&q, &g, &BoundParams::new(), None).unwrap().records.len(), k))
    };
    let s = slope(run("H100", 100), run("H1000", 1000), 100, 1000);
    assert!(s <= EXPAND_PER_ROW, "1-hop expand: {s:.2} allocations/row, ceiling {EXPAND_PER_ROW}");
}

#[test]
fn ingest_allocates_a_fixed_amount_per_node() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let e = QueryEngine::new();
    let q = "UNWIND range(1, $n) AS i CREATE (:Q {i: i})";
    // A fresh store per run, so each one starts from the same empty state.
    let run = |k| calls(|| {
        let mut g = GraphStore::new();
        e.execute_mut_with_params_within(q, &mut g, "default", &n(k), None).unwrap();
        assert_eq!(g.node_count(), k as usize);
    });
    let s = slope(run(100), run(1000), 100, 1000);
    assert!(s <= INGEST_PER_NODE, "ingest: {s:.2} allocations/node, ceiling {INGEST_PER_NODE}");
}

#[test]
fn an_indexed_point_lookup_stays_within_its_allocation_count() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (g, e) = (graph(), QueryEngine::new());
    let c = calls(|| {
        assert_eq!(e.execute_with_params_within("MATCH (p:P {i: $n}) RETURN p.name AS n", &g, &n(1234), None)
            .unwrap().records.len(), 1)
    });
    assert!(c <= POINT_LOOKUP_CALLS, "point lookup: {c} allocations, ceiling {POINT_LOOKUP_CALLS}");
}

/// The counts the gates rest on are exact: the same path, measured twice,
/// allocates the same number of times. A gate on a number that moved would
/// be a flaky test.
#[test]
fn the_counts_do_not_move_between_runs() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (g, e) = (graph(), QueryEngine::new());
    let q = "MATCH (p:P) WHERE p.i >= 0 AND p.i < $n RETURN p.i AS i";
    let a = calls(|| { e.execute_with_params_within(q, &g, &n(500), None).unwrap(); });
    let b = calls(|| { e.execute_with_params_within(q, &g, &n(500), None).unwrap(); });
    assert_eq!(a, b);
}
