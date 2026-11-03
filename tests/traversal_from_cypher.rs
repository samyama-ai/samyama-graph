//! Topological sort, cycle detection, bridges, articulation points (ALGO-01).
//!
//! Four structural questions from one depth-first walk. The first two are the
//! same question — a directed graph has a topological order **exactly when**
//! it has no cycle — and the last two are the same property seen from an edge
//! and from a node.

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph(n: usize, edges: &[(usize, usize)]) -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let x = s.create_node_with_labels([Label::new("N")]);
        s.set_node_property("default", x, "name", PropertyValue::String(format!("n{i}"))).unwrap();
        ids.push(x);
    }
    for &(a, b) in edges {
        s.create_edge(ids[a], ids[b], "R").unwrap();
    }
    (s, ids)
}

fn names(store: &GraphStore, cypher: &str, col: &str) -> Vec<String> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    r.records.iter().map(|rec| match rec.get(col) {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        other => format!("{other:?}"),
    }).collect()
}

fn fails(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(cypher).unwrap();
    format!("{:?}", QueryExecutor::new(store).execute(&q).unwrap_err())
}

#[test]
fn a_dag_sorts_into_dependency_order() {
    // 0 -> 1 -> 3, 0 -> 2 -> 3, 3 -> 4
    let (s, _) = graph(5, &[(0,1),(0,2),(1,3),(2,3),(3,4)]);
    let order = names(&s,
        "CALL algo.topologicalSort() YIELD node, position RETURN node.name AS name", "name");
    assert_eq!(order.len(), 5);
    let pos = |n: &str| order.iter().position(|x| x == n).unwrap();
    // Every edge must point forward. That is the property, not a fixed order.
    for (a, b) in [("n0","n1"),("n0","n2"),("n1","n3"),("n2","n3"),("n3","n4")] {
        assert!(pos(a) < pos(b), "{a} must precede {b}: {order:?}");
    }
}

#[test]
fn a_cycle_is_an_error_not_a_partial_order() {
    // Returning the part that *could* be sorted would be a plausible answer to
    // a question with no answer, and a build system acting on it would run
    // steps out of order.
    let (s, _) = graph(3, &[(0,1),(1,2),(2,0)]);
    let e = fails(&s, "CALL algo.topologicalSort() YIELD node RETURN node");
    assert!(e.contains("cycle"), "{e}");
    assert!(e.contains("findCycle"), "the error should name the way to see it: {e}");
}

#[test]
fn find_cycle_returns_the_witness_not_a_boolean() {
    let (s, _) = graph(4, &[(0,1),(1,2),(2,0),(2,3)]);
    let c = names(&s, "CALL algo.findCycle() YIELD node, position RETURN node.name AS name", "name");
    assert_eq!(c.len(), 3, "the cycle is 0-1-2: {c:?}");
    assert!(c.contains(&"n3".to_string()) == false, "n3 is not on the cycle: {c:?}");
}

#[test]
fn an_acyclic_graph_yields_no_cycle_rows() {
    let (s, _) = graph(3, &[(0,1),(1,2)]);
    assert!(names(&s, "CALL algo.findCycle() YIELD node RETURN node.name AS name", "name").is_empty());
}

#[test]
fn the_bridge_of_a_barbell_is_the_link_between_the_halves() {
    // Two triangles joined by one edge.
    let (s, _) = graph(6, &[(0,1),(1,2),(2,0),(2,3),(3,4),(4,5),(5,3)]);
    let q = "CALL algo.bridges() YIELD source, target \
             RETURN source.name AS name, target.name AS t";
    let src = names(&s, q, "name");
    let tgt = names(&s, q, "t");
    assert_eq!(src.len(), 1, "exactly one bridge: {src:?}");
    let pair = (src[0].as_str(), tgt[0].as_str());
    assert!(pair == ("n2", "n3") || pair == ("n3", "n2"), "{pair:?}");
}

#[test]
fn a_cycle_has_no_bridges_and_no_articulation_points() {
    let (s, _) = graph(5, &[(0,1),(1,2),(2,3),(3,4),(4,0)]);
    assert!(names(&s, "CALL algo.bridges() YIELD source RETURN source.name AS name", "name").is_empty());
    assert!(names(&s, "CALL algo.articulationPoints() YIELD node RETURN node.name AS name", "name").is_empty());
}

#[test]
fn every_edge_of_a_tree_is_a_bridge() {
    let (s, _) = graph(7, &[(0,1),(0,2),(1,3),(1,4),(2,5),(2,6)]);
    let b = names(&s, "CALL algo.bridges() YIELD source RETURN source.name AS name", "name");
    assert_eq!(b.len(), 6, "a tree of 7 nodes has 6 edges, all bridges: {b:?}");
    let a = names(&s, "CALL algo.articulationPoints() YIELD node RETURN node.name AS name", "name");
    assert_eq!(a.len(), 3, "the three interior nodes: {a:?}");
}

#[test]
fn the_root_of_a_second_component_is_not_an_articulation_point() {
    // The bug a two-component graph found: a DFS root is an articulation
    // point only with more than one child, and identifying roots by
    // "discovery time zero" names only the *first* one. Removing n4 leaves
    // n5 in a smaller component, not a disconnected graph.
    let (s, _) = graph(6, &[(0,1),(1,2),(2,0),(0,3),(4,5)]);
    let a = names(&s, "CALL algo.articulationPoints() YIELD node RETURN node.name AS name", "name");
    assert_eq!(a, vec!["n0"], "only n0 cuts the graph: {a:?}");
}

#[test]
fn the_two_cycle_spellings_agree() {
    let (s, _) = graph(3, &[(0,1),(1,2),(2,0)]);
    let a = names(&s, "CALL algo.findCycle() YIELD node RETURN node.name AS name", "name");
    let b = names(&s, "CALL algo.cycleDetection() YIELD node RETURN node.name AS name", "name");
    assert_eq!(a, b);
}
