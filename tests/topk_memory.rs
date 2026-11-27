//! `ORDER BY ... LIMIT k` holds about k rows, not its whole input (#2275).
//!
//! The bounded sort used to pull its input 65,536 rows at a time and trim
//! only after each batch, so a top-10 over 100,000 rows held tens of
//! thousands of projected rows and peaked at 60% of sorting all of them
//! (`docs/QUERY-MEMORY.md`, finding 4). It now feeds rows into the top-k as
//! they arrive and asks for batches no larger than its working size.
//!
//! Peaks are asserted as ratios against a baseline measured in the same
//! process, never as bytes: an absolute figure encodes the allocator, the
//! row layout and the build profile of the run that wrote it.
//!
//! Its own binary, because the allocator is global, and its tests one at a
//! time, because the counts are process-wide.

#[path = "../benches/common/peak_alloc.rs"]
mod peak_alloc;

use std::sync::Mutex;

use peak_alloc::measure;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::{BoundParams, QueryEngine};

static SERIAL: Mutex<()> = Mutex::new(());

const TOP: &str = "MATCH (a:P) RETURN a.name AS n ORDER BY n DESC LIMIT 10";
const ALL: &str = "MATCH (a:P) RETURN a.name AS n ORDER BY n DESC";

/// `nodes` `:P` nodes with a string `name`. Built through the store, not
/// Cypher, so the build is not what is measured.
fn graph(nodes: usize) -> GraphStore {
    let mut g = GraphStore::new();
    for i in 0..nodes {
        let id = g.create_node("P");
        g.set_node_property(
            "default",
            id,
            "name",
            PropertyValue::String(format!("name-{i:08}")),
        )
        .unwrap();
    }
    g
}

/// The peak of one run of `q`, after a warm-up run that fills the parse cache.
fn peak(e: &QueryEngine, g: &GraphStore, q: &str, rows: usize) -> isize {
    let _ = e.execute_with_params_within(q, g, &BoundParams::new(), None);
    let (cost, out) = measure(|| e.execute_with_params_within(q, g, &BoundParams::new(), None));
    assert_eq!(out, Ok(rows), "{q}");
    assert!(cost.peak_bytes > 0, "{q}: the measurement saw nothing");
    cost.peak_bytes
}

#[test]
fn a_top_k_peaks_at_a_small_fraction_of_a_full_sort() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // More rows than one 65,536-row batch, as in the benchmark.
    let n = 100_000;
    let g = graph(n);
    let e = QueryEngine::new();
    let all = peak(&e, &g, ALL, n);
    let top = peak(&e, &g, TOP, 10);
    // Measured before the change: 0.60 (35.4 MB against 59.2 MB).
    assert!(
        top * 5 < all,
        "top-10 peaked at {top} bytes, {:.2} of the full sort's {all}",
        top as f64 / all as f64
    );
}

#[test]
fn a_top_k_does_not_grow_with_the_rows_it_discards() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let e = QueryEngine::new();
    // Both sizes fit in one of the old 65,536-row batches, so before the
    // change the larger held three times the rows of the smaller.
    let small = peak(&e, &graph(20_000), TOP, 10);
    let large = peak(&e, &graph(60_000), TOP, 10);
    // What still grows is the scan's list of node ids, 8 bytes per node.
    assert!(
        large * 2 < small * 3,
        "3x the rows cost {:.2}x the peak ({small} -> {large} bytes)",
        large as f64 / small as f64
    );
}
