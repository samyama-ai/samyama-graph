//! `SKIP` and `LIMIT` trim the result set, not the side effects (#866).
//!
//! ```text
//! CREATE (n:N {num: 42}) RETURN n LIMIT 0   -> 0 rows, and the node exists
//! ```
//!
//! Two defects in sequence. The bare-`CREATE`-plus-`RETURN` planner path never
//! applied `ORDER BY`/`SKIP`/`LIMIT` at all — it works when the create has
//! input rows, which is a different path. And applying them *lazily* skipped
//! the write, because `LimitOperator(0)` returns without pulling.
//!
//! So both halves are asserted everywhere below: **the row count and the side
//! effect**. Checking only the row count is what the first fix passed while
//! creating nothing.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

/// Runs `cypher` against a fresh store; returns (rows returned, nodes existing).
fn run(cypher: &str) -> (usize, usize) {
    let mut store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let rows = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"))
        .records
        .len();
    let count = parse_query("MATCH (n) RETURN n")
        .ok()
        .and_then(|p| QueryExecutor::new(&store).execute(&p).ok())
        .map(|b| b.records.len())
        .expect("count runs");
    (rows, count)
}

/// The TCK's four `Create6` shapes.
#[test]
fn a_row_limit_does_not_cancel_the_write() {
    assert_eq!(run("CREATE (n:N {num: 42}) RETURN n LIMIT 0"), (0, 1));
    assert_eq!(run("CREATE (n:N {num: 42}) RETURN n SKIP 1"), (0, 1));
    // Two nodes, one relationship.
    assert_eq!(run("CREATE ()-[r:R]->() RETURN r LIMIT 0"), (0, 2));
    assert_eq!(run("CREATE ()-[r:R]->() RETURN r SKIP 1"), (0, 2));
}

/// The clause still trims when it should, and a create with input rows — which
/// always worked, via a different planner path — is undisturbed.
#[test]
fn a_row_limit_still_trims() {
    assert_eq!(run("CREATE (n:N {num: 42}) RETURN n LIMIT 1"), (1, 1));
    assert_eq!(run("UNWIND [1, 2, 3] AS x CREATE (n:N {num: x}) RETURN n LIMIT 2"), (2, 3));
    assert_eq!(run("UNWIND [1, 2, 3] AS x CREATE (n:N {num: x}) RETURN n SKIP 2"), (1, 3));
}

/// **`ORDER BY` above a write.** This raised "requires mutable store access",
/// because `SortOperator` had no `next_mut` — the defect class #649 declared
/// closed on `SKIP` and `LIMIT`.
#[test]
fn a_sort_above_a_write_runs() {
    assert_eq!(run("CREATE (n:N {num: 42}) RETURN n ORDER BY n.num LIMIT 0"), (0, 1));
    assert_eq!(run("CREATE (n:N {num: 42}) RETURN n ORDER BY n.num"), (1, 1));
    assert_eq!(
        run("UNWIND [3, 1, 2] AS x CREATE (n:N {num: x}) RETURN n ORDER BY n.num LIMIT 2"),
        (2, 3)
    );
}

/// **A filter above a write** had the same gap.
#[test]
fn a_filter_above_a_write_runs() {
    assert_eq!(
        run("UNWIND [1, 2, 3] AS x CREATE (n:N {num: x}) WITH n WHERE n.num > 1 RETURN n"),
        (2, 3)
    );
}

/// Reads are untouched: a limit on a read must still stop early rather than
/// draining, which is the whole point of one.
#[test]
fn reads_are_unaffected() {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (:A), (:A), (:A)").expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    for (cypher, want) in [
        ("MATCH (n) RETURN n LIMIT 0", 0),
        ("MATCH (n) RETURN n LIMIT 2", 2),
        ("MATCH (n) RETURN n SKIP 2", 1),
        ("MATCH (n) RETURN n", 3),
    ] {
        let p = parse_query(cypher).expect("parses");
        let rows = QueryExecutor::new(&store).execute(&p).expect("runs").records.len();
        assert_eq!(rows, want, "{cypher}");
    }
}
