//! No query shape scans a label it could have anchored (#584).
//!
//! #584 was a `shortestPath` target endpoint that scanned every node in its
//! label while the start was pinned — 329× on a 1,200-node chain — and it hid
//! behind a wall-clock assertion for a whole PR. The bug was not the operator;
//! it was a planner branch that never consulted an anchorable predicate.
//!
//! That is a **class**, so this enumerates shapes rather than testing one. Each
//! is planned with `EXPLAIN` and checked for a `NodeScan` where a predicate
//! pins the variable.
//!
//! ## What "anchored" means per shape
//!
//! Not simply "one anchor per pinned variable". A variable reached by an
//! *expand* is discovered by traversal, so the far end of `(a)-[:KNOWS]->(b)`
//! is neither anchorable nor worth anchoring: expanding from a pinned `a` to
//! its neighbours is cheap, and filtering `b` afterwards is right. The shapes
//! where **both** ends must be anchored are those joined by a cartesian
//! product — two disjoint patterns, and `shortestPath`, which searches between
//! two independently located endpoints.
//!
//! Getting that distinction wrong in either direction makes the test useless:
//! too strict and it fails on correct plans, too loose and it passes #584.
//!
//! ## A case deliberately not asserted
//!
//! `MATCH (a)-[:KNOWS*1..3]->(b) WHERE id(a) = … AND id(b) = …` expands to
//! depth 3 and filters the target afterwards, so the constraint does no work.
//! Measured on a 20,000-node graph of degree 20: **2.4 ms with the target
//! pinned against 2.3 ms with it free** — the constraint is indeed ignored, and
//! exploiting it would save nothing worth the change. (`shortestPath` answers
//! the same question in 9.6 ms, so this is already the fast path.) Recorded
//! here so the next reader does not re-derive it.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let ids: Vec<_> = (0..200i64)
        .map(|i| {
            let id = store.create_node("N");
            let _ = store.set_node_property("default", id, "seq".to_string(), PropertyValue::Integer(i));
            id
        })
        .collect();
    for w in ids.windows(2) {
        store.create_edge(w[0], w[1], "KNOWS").unwrap();
    }
    for (i, &a) in ids.iter().enumerate() {
        store.create_edge(a, ids[(i * 7 + 3) % ids.len()], "LIKES").unwrap();
    }
    let create = parse_query("CREATE INDEX ON :N(seq)").unwrap();
    let mut mutating = MutQueryExecutor::new(&mut store, "default".to_string());
    let _ = mutating.execute(&create);
    store
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}"))
        .unwrap_or_else(|e| panic!("{cypher}: parse {e:?}"));
    let batch = QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: exec {e:?}"));
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => {
            t.lines().take_while(|l| !l.starts_with("---")).collect::<Vec<_>>().join("\n")
        }
        other => panic!("{other:?}"),
    }
}

/// Every shape, with the number of anchors its plan should contain.
const SHAPES: &[(&str, &str, usize)] = &[
    // A scan the predicate can replace outright.
    ("id() equality", "MATCH (n:N) WHERE id(n) = 5 RETURN n", 1),
    ("id() IN a list", "MATCH (n:N) WHERE id(n) IN [1,2,3] RETURN n", 1),
    ("indexed property", "MATCH (n:N) WHERE n.seq = 5 RETURN n", 1),
    ("inline property", "MATCH (n:N {seq: 5}) RETURN n", 1),
    // One end anchored, the other reached by expansion — one anchor is correct.
    ("expand from an anchored start", "MATCH (a:N)-[:KNOWS]->(b:N) WHERE id(a) = 5 RETURN b", 1),
    ("expand anchored at the far end", "MATCH (a:N)-[:KNOWS]->(b:N) WHERE id(b) = 5 RETURN a", 1),
    ("both ends constrained, one hop", "MATCH (a:N)-[:KNOWS]->(b:N) WHERE a.seq = 1 AND b.seq = 2 RETURN a, b", 1),
    ("three-node path, ends anchored", "MATCH (a:N)-[:KNOWS]->(m:N)-[:KNOWS]->(b:N) WHERE id(a) = 1 AND id(b) = 3 RETURN m", 1),
    ("a second label", "MATCH (a:N)-[:LIKES]->(b:N) WHERE a.seq = 1 RETURN b", 1),
    ("var-length from an anchor", "MATCH (a:N)-[:KNOWS*1..3]->(b:N) WHERE id(a) = 1 RETURN b", 1),
    ("anchored inside EXISTS", "MATCH (a:N) WHERE id(a) = 1 AND EXISTS { MATCH (b:N) WHERE id(b) = 6 } RETURN a", 1),
    // Joined by a cartesian product — both ends must be anchored. This is
    // where #584 lived.
    ("shortestPath, both by id", "MATCH p = shortestPath((a:N)-[:KNOWS*]-(b:N)) WHERE id(a) = 1 AND id(b) = 6 RETURN p", 2),
    ("shortestPath, both by index", "MATCH p = shortestPath((a:N)-[:KNOWS*]-(b:N)) WHERE a.seq = 1 AND b.seq = 6 RETURN p", 2),
    ("allShortestPaths, both by id", "MATCH p = allShortestPaths((a:N)-[:KNOWS*]-(b:N)) WHERE id(a) = 1 AND id(b) = 6 RETURN p", 2),
    ("two disjoint patterns, by id", "MATCH (a:N), (b:N) WHERE id(a) = 1 AND id(b) = 6 RETURN a, b", 2),
    ("two disjoint patterns, by index", "MATCH (a:N), (b:N) WHERE a.seq = 1 AND b.seq = 6 RETURN a, b", 2),
    ("OPTIONAL MATCH, both anchored", "MATCH (a:N) WHERE id(a) = 1 OPTIONAL MATCH (b:N) WHERE id(b) = 6 RETURN a, b", 2),
    ("WITH between two anchored matches", "MATCH (a:N) WHERE id(a) = 1 WITH a MATCH (b:N) WHERE id(b) = 6 RETURN a, b", 2),
];

#[test]
fn no_shape_scans_a_label_it_could_anchor() {
    let store = fixture();
    let mut offenders = Vec::new();

    for (label, cypher, expected) in SHAPES {
        let text = plan(&store, cypher);
        let scans = text.matches("NodeScan").count();
        let anchors = text.matches("NodeById").count() + text.matches("IndexScan").count();
        if scans != 0 || anchors < *expected {
            offenders.push(format!(
                "{label}: {scans} NodeScan, {anchors} anchors (expected {expected})\n  {cypher}\n{}",
                text.lines().map(|l| format!("    {l}")).collect::<Vec<_>>().join("\n")
            ));
        }
    }

    assert!(
        offenders.is_empty(),
        "{} shape(s) scan where a predicate could pin:\n\n{}",
        offenders.len(),
        offenders.join("\n\n")
    );
}

#[test]
fn the_shapes_all_still_answer() {
    // A plan-shape assertion cannot tell an anchored plan from an anchored plan
    // that returns nothing. Every shape above is run, and every one of them is
    // written to match at least one row on this fixture.
    let store = fixture();
    for (label, cypher, _) in SHAPES {
        let query = parse_query(cypher).unwrap_or_else(|e| panic!("{label}: {e:?}"));
        let batch = QueryExecutor::new(&store)
            .execute(&query)
            .unwrap_or_else(|e| panic!("{label}: {e:?}"));
        assert!(!batch.records.is_empty(), "{label} returned nothing:\n  {cypher}");
    }
}
