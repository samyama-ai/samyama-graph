//! A query may not end with `WITH` (#2358). It used to run, and
//! `MATCH (m) WITH m.city AS city, count(m) AS n ORDER BY n DESC LIMIT 1`
//! answered with a column `m` the `WITH` never kept. Pinned both ways: every
//! form that ends with a WITH is refused with Neo4j's message, and every form
//! that continues after one -- RETURN, a write, a CALL, another reading
//! clause then RETURN -- still runs.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    QueryEngine::new()
        .execute_mut("UNWIND range(1, 6) AS i CREATE (:M {i: i, city: CASE WHEN i < 4 THEN 'A' ELSE 'B' END})", &mut s, "default")
        .unwrap();
    s
}

#[test]
fn a_query_that_ends_with_with_is_refused() {
    let s = store();
    let e = QueryEngine::new();
    for q in [
        "MATCH (m:M) WITH m.city AS city, count(m) AS n ORDER BY n DESC LIMIT 1",
        "MATCH (m:M) WITH m",
        "MATCH (m:M) WITH m WHERE m.i > 2",
        "MATCH (m:M) WITH m.i AS i SKIP 1 LIMIT 2",
        "MATCH (m:M) WITH m WITH m.i AS i",
        "UNWIND [1, 2] AS x WITH x",
        "MATCH (m:M) WITH m MATCH (m)--(o) WITH o",
        "WITH 1 AS x",
        "EXPLAIN MATCH (m:M) WITH m",
    ] {
        let err = e.execute(q, &s).err().unwrap_or_else(|| panic!("ran: {q}")).to_string();
        assert!(err.contains("Query cannot conclude with WITH"), "{q}\n{err}");
    }
}

#[test]
fn a_query_that_continues_after_with_still_runs() {
    let mut s = store();
    let e = QueryEngine::new();
    for (q, rows) in [
        ("MATCH (m:M) WITH m.city AS city, count(m) AS n ORDER BY n DESC LIMIT 1 RETURN city, n", 1),
        ("MATCH (m:M) WITH m WHERE m.i > 4 RETURN m.i AS i", 2),
        ("MATCH (m:M) WITH m WITH m.i AS i RETURN i", 6),
        ("MATCH (m:M) WITH m MATCH (m) RETURN count(m) AS n", 1),
        ("UNWIND [1, 2] AS x WITH x UNWIND [3] AS y RETURN x, y", 2),
        ("WITH 1 AS x RETURN x", 1),
        ("MATCH (m:M) WITH m ORDER BY m.i RETURN m.i AS i LIMIT 1", 1),
    ] {
        let b = e.execute(q, &s).unwrap_or_else(|err| panic!("refused: {q}\n{err}"));
        assert_eq!(b.records.len(), rows, "{q}");
    }
    for q in [
        "MATCH (m:M) WITH m WHERE m.i = 1 SET m.seen = true",
        "MATCH (m:M) WITH m WHERE m.i = 2 DETACH DELETE m",
        "MATCH (m:M) WITH m.city AS c MERGE (:City {name: c})",
        "WITH 1 AS x CREATE (:X {x: x})",
        "MATCH (m:M) WITH m FOREACH (_ IN [1] | SET m.f = 1)",
    ] {
        e.execute_mut(q, &mut s, "default").unwrap_or_else(|err| panic!("refused: {q}\n{err}"));
    }
}
