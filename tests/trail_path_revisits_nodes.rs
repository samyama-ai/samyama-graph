//! A trail that revisits a node keeps its whole walk (#976).
//!
//! ```cypher
//! MATCH topRoute = (:Start)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*3..3]-(:End)
//! RETURN topRoute
//! ```
//!
//! returned the right **number** of rows and a two-hop path where the answer
//! is four hops.
//!
//! The trail was flattened into a `parent` map keyed by node and rebuilt from
//! it. That cannot represent a walk that **revisits a node**: every later
//! visit overwrites the earlier one, so reconstruction follows the wrong edges
//! back and stops early.
//!
//! Undirected walks over a small graph revisit constantly, which is why this
//! shows on `*3..3` undirected and not on a directed chain — and why the row
//! count stayed right while the paths did not.
//!
//! `path` was already the walk, in order. There was never a reason to go
//! through a lossy intermediate.

use samyama::graph::{GraphStore, Label};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// The openCypher `Match6` [14] fixture: a star around `mid`.
fn star() -> GraphStore {
    let mut store = GraphStore::new();
    let db1 = store.create_node_with_labels([Label::new("Start")]);
    let db2 = store.create_node_with_labels([Label::new("End")]);
    let mid = store.create_node("");
    let other = store.create_node("");
    for t in [db1, db2, db2, other, other] {
        store.create_edge(mid, t, "CONNECTED_TO").unwrap();
    }
    store
}

fn shapes(store: &GraphStore, cypher: &str, col: &str) -> Vec<(usize, usize)> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut out: Vec<(usize, usize)> = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .iter()
        .map(|r| match r.get(col) {
            Some(Value::Path { nodes, edges }) => (nodes.len(), edges.len()),
            other => panic!("{cypher}: {other:?}"),
        })
        .collect();
    out.sort();
    out
}

#[test]
fn an_undirected_trail_that_revisits_a_node_keeps_every_hop() {
    let store = star();
    let got = shapes(
        &store,
        "MATCH topRoute = (:Start)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*3..3]-(:End) \
         RETURN topRoute",
        "topRoute",
    );
    assert_eq!(got.len(), 4, "four routes");
    // One fixed hop plus three variable ones: five nodes, four relationships.
    assert!(got.iter().all(|&s| s == (5, 4)), "{got:?}");
}

#[test]
fn every_path_is_internally_consistent() {
    let store = star();
    for (n, e) in shapes(
        &store,
        "MATCH p = (:Start)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*3..3]-(:End) RETURN p",
        "p",
    ) {
        assert_eq!(n, e + 1, "{n} nodes, {e} relationships");
    }
}

#[test]
fn length_agrees_with_the_path() {
    // A shortened path used to report a shortened length too — consistent and
    // wrong — so the count of relationships has to be checked against the
    // pattern, not against the path itself.
    let store = star();
    let q = parse_query(
        "MATCH p = (:Start)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*3..3]-(:End) \
         RETURN length(p) AS len",
    )
    .unwrap();
    for rec in &QueryExecutor::new(&store).execute(&q).unwrap().records {
        assert!(
            format!("{:?}", rec.get("len")).contains("Integer(4)"),
            "{:?}",
            rec.get("len")
        );
    }
}

#[test]
fn a_directed_chain_is_unchanged() {
    // The shape the parent map always handled: no node is visited twice.
    let mut store = GraphStore::new();
    let a = store.create_node_with_labels([Label::new("A")]);
    let b = store.create_node("");
    let c = store.create_node_with_labels([Label::new("C")]);
    store.create_edge(a, b, "R").unwrap();
    store.create_edge(b, c, "R").unwrap();
    assert_eq!(
        shapes(&store, "MATCH p = (:A)-[:R*2..2]->(:C) RETURN p", "p"),
        vec![(3, 2)]
    );
}

#[test]
fn the_relationship_list_matches_the_path() {
    let store = star();
    let q = parse_query(
        "MATCH p = (:Start)<-[:CONNECTED_TO]-()-[r:CONNECTED_TO*3..3]-(:End) \
         RETURN size(r) AS n",
    )
    .unwrap();
    for rec in &QueryExecutor::new(&store).execute(&q).unwrap().records {
        assert!(
            format!("{:?}", rec.get("n")).contains("Integer(3)"),
            "the variable-length segment walked three: {:?}",
            rec.get("n")
        );
    }
}
