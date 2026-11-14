//! `/metrics` counts every query the engine answers or refuses (#2080).
//!
//! Two kinds slipped past `samyama_queries_total` and
//! `samyama_queries_failed_total`: a read answered from the result cache
//! returned before anything was counted, and a statement that did not parse
//! failed before the timer started. So a busy cache made the server look idle
//! and a client sending broken Cypher made it look healthy.
//!
//! One test in its own binary, because the counters are process-wide and a
//! second test running beside it would move them.

use samyama::graph::GraphStore;
use samyama::query::metrics::snapshot;
use samyama::query::{BoundParams, QueryEngine};

/// (total, failed, latency observations) moved by `f`.
fn moved(f: impl FnOnce()) -> (u64, u64, u64) {
    let before = snapshot();
    f();
    let after = snapshot();
    (
        after.total - before.total,
        after.failed - before.failed,
        after.count - before.count,
    )
}

#[test]
fn cache_hits_and_parse_errors_are_counted_once_each() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut("CREATE (:Person {name: 'a'})", &mut store, "default")
        .unwrap();

    // A miss, then a hit: two queries answered, two latencies observed.
    let read = "MATCH (n:Person) RETURN n.name AS name";
    let (served, cached) = (
        engine.execute_cached(read, &store).unwrap().1,
        engine.execute_cached(read, &store).unwrap().1,
    );
    assert!(!served && cached, "the second read should be a cache hit");
    let again = moved(|| {
        assert!(engine.execute_cached(read, &store).unwrap().1);
    });
    assert_eq!(again, (1, 0, 1), "a cache hit was not counted as a query answered");

    // A statement that does not parse, on every path that runs one.
    let broken = "MATCH (n:Person RETURN n";
    assert_eq!(
        moved(|| assert!(engine.execute(broken, &store).is_err())),
        (1, 1, 1),
        "read path"
    );
    assert_eq!(
        moved(|| assert!(engine.execute_cached(broken, &store).is_err())),
        (1, 1, 1),
        "cached read path: counted once, not once per layer"
    );
    assert_eq!(
        moved(|| assert!(engine.execute_mut(broken, &mut store, "default").is_err())),
        (1, 1, 1),
        "write path"
    );
    assert_eq!(
        moved(|| {
            let mut sink = |_: &[String], _: Vec<samyama::query::executor::Record>| Ok(());
            assert!(engine
                .execute_streaming_with_params(broken, &store, &BoundParams::new(), 10, &mut sink)
                .is_err());
        }),
        (1, 1, 1),
        "streaming path"
    );

    // Classifying a statement is not running it, so it counts nothing; the
    // run that follows is the one that does.
    assert_eq!(
        moved(|| assert!(engine.statement_is_write(broken).is_err())),
        (0, 0, 0),
        "statement_is_write counted a query"
    );
}
