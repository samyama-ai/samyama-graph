//! Eccentricity, diameter, radius, and degree mixing (ALGO-01).
//!
//! The first three are one computation read three ways: a node's eccentricity
//! is its distance to the furthest node it can reach, the diameter is the
//! largest of those, the radius the smallest. Separate entry points because a
//! caller wants a column, a scalar, and a scalar.
//!
//! The disconnected case is where implementations differ and where the wrong
//! answer looks most plausible: a diameter computed over the largest component
//! is a real-looking number that is simply smaller than the truth.

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
    for &(a, b) in edges { s.create_edge(ids[a], ids[b], "R").unwrap(); }
    (s, ids)
}

fn run(store: &GraphStore, cypher: &str) -> Vec<Vec<String>> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store).execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let cols = r.columns.clone();
    r.records.iter()
        .map(|rec| cols.iter().map(|c| format!("{:?}", rec.get(c))).collect())
        .collect()
}

fn fails(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(cypher).unwrap();
    format!("{:?}", QueryExecutor::new(store).execute(&q).unwrap_err())
}

/// A path of five: diameter 4, radius 2.
fn path5() -> GraphStore { graph(5, &[(0,1),(1,2),(2,3),(3,4)]).0 }

#[test]
fn a_path_has_the_diameter_and_radius_its_shape_implies() {
    let s = path5();
    let d = run(&s, "CALL algo.diameter() YIELD diameter RETURN diameter");
    assert!(d[0][0].contains("Integer(4)"), "{d:?}");
    let r = run(&s, "CALL algo.radius() YIELD radius RETURN radius");
    assert!(r[0][0].contains("Integer(2)"), "{r:?}");
}

#[test]
fn eccentricity_is_a_column_and_the_ends_are_furthest() {
    let s = path5();
    let e = run(&s, "CALL algo.eccentricity() YIELD node, eccentricity \
                     RETURN node.name AS n, eccentricity AS e");
    assert_eq!(e.len(), 5);
    // Sorted highest first, so the two ends lead with 4 and the middle is 2.
    assert!(e[0][1].contains("Integer(4)"), "{e:?}");
    assert!(e[4][1].contains("Integer(2)"), "{e:?}");
}

#[test]
fn a_disconnected_graph_has_no_diameter_rather_than_a_smaller_one() {
    // The failure worth guarding: the diameter of the largest component is a
    // real-looking number, and a caller sizing a traversal budget from it
    // would under-provision.
    let (s, _) = graph(5, &[(0,1),(1,2),(3,4)]);
    for algo in ["diameter", "radius"] {
        let e = fails(&s, &format!("CALL algo.{algo}() YIELD {algo} RETURN {algo}"));
        assert!(e.contains("not connected"), "{algo}: {e}");
        assert!(e.contains("algo.wcc"), "the error should say what to do next: {e}");
    }
}

#[test]
fn a_node_that_cannot_reach_everything_has_a_null_eccentricity_not_a_missing_row() {
    let (s, _) = graph(5, &[(0,1),(1,2),(3,4)]);
    let e = run(&s, "CALL algo.eccentricity() YIELD node, eccentricity \
                     RETURN node.name AS n, eccentricity AS e");
    assert_eq!(e.len(), 5, "every node keeps a row: {e:?}");
    assert!(e.iter().all(|r| r[1].contains("Null")), "all are unreachable-from-some: {e:?}");
}

#[test]
fn a_star_is_disassortative_and_a_cycle_has_no_assortativity_at_all() {
    // A star: the hub has degree 4 and every leaf degree 1, so every edge
    // joins a high degree to a low one -- perfectly disassortative, -1.
    let (star, _) = graph(5, &[(0,1),(0,2),(0,3),(0,4)]);
    let a = run(&star, "CALL algo.degreeAssortativity() YIELD assortativity RETURN assortativity");
    assert!(a[0][0].contains("-1.0"), "a star should be -1.0: {a:?}");

    // A cycle: every node has degree 2, so the correlation is 0/0. Reporting
    // 0.0 would claim "no assortativity" about a perfectly regular graph.
    let (cycle, _) = graph(5, &[(0,1),(1,2),(2,3),(3,4),(4,0)]);
    let e = fails(&cycle, "CALL algo.degreeAssortativity() YIELD assortativity RETURN assortativity");
    assert!(e.contains("undefined"), "{e}");
}

#[test]
fn average_neighbour_degree_sees_the_hub_from_the_leaves() {
    let (s, _) = graph(5, &[(0,1),(0,2),(0,3),(0,4)]);
    let a = run(&s, "CALL algo.averageNeighborDegree() YIELD node, score \
                     RETURN node.name AS n, score");
    assert_eq!(a.len(), 5);
    // A leaf's only neighbour is the hub, degree 4. The hub's neighbours are
    // four leaves, degree 1. So the leaves lead at 4.0 and the hub is last.
    assert!(a[0][1].contains("4.0"), "{a:?}");
    assert!(a[4][0].contains("n0") && a[4][1].contains("1.0"), "{a:?}");
}

#[test]
fn both_spellings_of_average_neighbour_degree_agree() {
    let (s, _) = graph(4, &[(0,1),(1,2),(2,3)]);
    assert_eq!(
        run(&s, "CALL algo.averageNeighborDegree() YIELD node, score RETURN node.name AS n, score"),
        run(&s, "CALL algo.averageNeighbourDegree() YIELD node, score RETURN node.name AS n, score"),
    );
}
