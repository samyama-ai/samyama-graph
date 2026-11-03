//! A long algorithm stops at the query deadline instead of running to the end
//! (ALGO-12, #1693).
//!
//! An algorithm runs inside one call, and nothing inside that call looked at the
//! deadline: `algo.pageRank` built its graph view and ran every iteration
//! however long they took, and the query reported `Query timed out after N
//! rows` only once all of it was done. Measured on 50,000 nodes and 500,000
//! edges (debug build): about 1.15 s whether the deadline was 0 ms, 200 ms or
//! 700 ms.
//!
//! Now the view build and the PageRank iterations ask the deadline as they go,
//! and a query past it fails with how far the algorithm got.
//!
//! Nothing here asserts how long anything took, so a slow or fast CI host
//! cannot make it flaky. The deadlines are chosen relative to the run itself,
//! and the tests assert what comes back: which step stopped, and that it was
//! before the end.

use samyama::graph::GraphStore;
use samyama::query::{BoundParams, QueryEngine};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Nodes in the test graph, each with `EDGES_PER_NODE` outgoing edges to
/// pseudo-random targets: large enough that building the view takes many
/// deadline checks (one per 4,096 nodes), small enough to build in seconds.
const NODES: u64 = 20_000;
const EDGES_PER_NODE: usize = 10;

/// Built once and shared: the tests only read it, and building it is most of
/// their time in a debug build.
fn graph() -> &'static GraphStore {
    static GRAPH: OnceLock<GraphStore> = OnceLock::new();
    GRAPH.get_or_init(build)
}

fn build() -> GraphStore {
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..NODES).map(|_| s.create_node("P")).collect();
    // xorshift: the same graph on every run, no dependency on a seedable RNG.
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for &from in &ids {
        for _ in 0..EDGES_PER_NODE {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            s.create_edge(from, ids[(x % NODES) as usize], "L").unwrap();
        }
    }
    s
}

fn run(engine: &QueryEngine, store: &GraphStore, q: &str, limit: Option<Duration>) -> Result<usize, String> {
    engine
        .execute_with_params_within(q, store, &BoundParams::new(), limit)
        .map(|b| b.records.len())
        .map_err(|e| e.to_string())
}

const PAGE_RANK: &str = "CALL algo.pageRank('P', 'L', {iterations: 50}) YIELD node, score RETURN node, score";

/// `<what> stopped ... after <done> of <total> <unit>`, as numbers.
fn stopped_at(msg: &str) -> (usize, usize) {
    let after = msg.split(" after ").nth(1).unwrap_or_else(|| panic!("no 'after' in: {msg}"));
    let mut words = after.split_whitespace();
    let done = words.next().unwrap().parse().unwrap();
    assert_eq!(words.next(), Some("of"), "{msg}");
    let total = words.next().unwrap().parse().unwrap();
    (done, total)
}

/// A deadline that has already passed when the query starts: the algorithm
/// does no work at all. Before the fix it built the whole view and ran every
/// iteration first.
#[test]
fn a_page_rank_out_of_time_before_it_starts_does_no_work() {
    let store = graph();
    let engine = QueryEngine::new();
    let err = run(&engine, store, PAGE_RANK, Some(Duration::ZERO)).unwrap_err();
    assert!(err.contains("Query timed out: algo.pageRank stopped building its graph view"), "{err}");
    assert_eq!(stopped_at(&err), (0, NODES as usize));

    // The deadline is the query's, not a latch on the engine.
    assert_eq!(run(&engine, store, "RETURN 1 AS one", None), Ok(1));
    assert_eq!(run(&engine, store, PAGE_RANK, None), Ok(NODES as usize), "the same call, with time, answers");
}

/// A deadline that falls part-way through. It is set to a quarter of the time
/// the same call takes without one, measured here, so it lands inside the run
/// on any host. The old behaviour -- finish, then report `Query timed out after
/// N rows` -- fails this test by its message.
#[test]
fn a_page_rank_that_runs_out_of_time_part_way_stops_there() {
    let store = graph();
    let engine = QueryEngine::new();
    let started = Instant::now();
    assert_eq!(run(&engine, store, PAGE_RANK, None), Ok(NODES as usize));
    let full = started.elapsed();

    let err = run(&engine, store, PAGE_RANK, Some(full / 4)).unwrap_err();
    assert!(
        err.contains("Query timed out: algo.pageRank stopped"),
        "the run was not stopped part-way (the full run took {full:?}): {err}"
    );
    let (done, total) = stopped_at(&err);
    assert!(done < total, "stopped after {done} of {total}: that is the end, not part-way");
}

/// Every algorithm builds its view through the same place, so the deadline
/// stops the others there too, not only PageRank. Only the view: their own
/// steps are not checked yet. `algo.louvain` would make the point, and is left
/// out because its full run, the control, takes about 19 s here in a debug
/// build -- which is the next thing to make stop.
#[test]
fn other_algorithms_stop_building_their_view_at_the_deadline() {
    let store = graph();
    let engine = QueryEngine::new();
    for (call, q) in [
        ("algo.wcc", "CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n"),
        ("algo.cdlp", "CALL algo.cdlp({maxIterations: 5}) YIELD node RETURN count(*) AS n"),
    ] {
        let err = run(&engine, store, q, Some(Duration::ZERO)).unwrap_err();
        assert!(
            err.contains(&format!("Query timed out: {call} stopped building its graph view")),
            "{call}: {err}"
        );
        assert_eq!(run(&engine, store, q, None), Ok(1), "{call} with time answers");
    }
}
