//! A variable-length segment enumerates *paths*, not shortest distances.
//!
//! `VarLengthExpandOperator` runs a BFS with a node-visited set, so every
//! reachable node is emitted exactly once, at its shortest depth. openCypher
//! specifies something different: `-[:R*m..n]-` matches every path whose
//! relationships are distinct, so a node reachable by two different routes
//! inside the bound produces two rows, and a node whose *only* route long
//! enough to satisfy `m` is not the shortest one is missing entirely.
//!
//! The scope is the same one #684 established for fixed-length patterns:
//! relationships may not repeat within a clause, nodes may.
//!
//! Expectations below are openCypher's, matching Neo4j 5 on the same graphs,
//! and three of the four fail today. They are `#[ignore]`d against #710 rather
//! than deleted or weakened: the expected values are the specification's, and a
//! test that asserts the current behaviour would have to be rewritten by
//! whoever fixes it, which is the opposite of useful.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn names(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| match r.get("n") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => panic!("expected string n, got {other:?}"),
        })
        .collect();
    got.sort();
    got
}

/// A triangle: a-b, b-c, a-c, all `:R`, all undirected in the pattern.
fn triangle() -> GraphStore {
    let mut store = GraphStore::new();
    for n in ["a", "b", "c"] {
        run(&mut store, &format!("CREATE (:N {{name: \"{n}\"}})"));
    }
    for (x, y) in [("a", "b"), ("b", "c"), ("a", "c")] {
        run(
            &mut store,
            &format!(
                "MATCH (x:N {{name:\"{x}\"}}), (y:N {{name:\"{y}\"}}) CREATE (x)-[:R]->(y)"
            ),
        );
    }
    store
}

/// Two routes to the same node inside the bound are two rows, not one.
///
/// From `a` over `*1..2`: `b` (via a-b), `c` (via a-c), `c` (via a-b-c) and
/// `b` (via a-c-b). Four rows. A shortest-path BFS emits two.
#[test]
#[ignore = "#710: the operator walks shortest paths, not paths"]
fn a_node_reachable_two_ways_within_the_bound_yields_two_rows() {
    let store = triangle();
    assert_eq!(
        names(&store, "MATCH (a:N {name:\"a\"})-[:R*1..2]-(x) RETURN x.name AS n"),
        vec!["b", "b", "c", "c"],
    );
}

/// A lower bound does not mean "shortest distance is at least m".
///
/// `*2..2` from `a` matches a-b-c and a-c-b, so `b` and `c` each appear once.
/// A BFS that marks both visited at depth 1 and never revisits them returns
/// nothing at all.
#[test]
#[ignore = "#710: the operator walks shortest paths, not paths"]
fn a_lower_bound_still_matches_a_longer_route_to_a_near_node() {
    let store = triangle();
    assert_eq!(
        names(&store, "MATCH (a:N {name:\"a\"})-[:R*2..2]-(x) RETURN x.name AS n"),
        vec!["b", "c"],
    );
}

/// The relationship-uniqueness rule #684 established applies here too.
///
/// This one passes, but by accident rather than by rule: the node-visited set
/// stops the walk back to `a`, not any check on the edge. Kept unignored so a
/// fix for #710 is required to preserve it.
///
/// One edge, `a-[:R]-b`. Walking out and back is not a two-hop path, so
/// `*2..2` from `a` matches nothing.
#[test]
fn one_edge_cannot_be_walked_out_and_back_to_make_two_hops() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:N {name: \"a\"})");
    run(&mut store, "CREATE (:N {name: \"b\"})");
    run(&mut store, "MATCH (x:N {name:\"a\"}), (y:N {name:\"b\"}) CREATE (x)-[:R]->(y)");
    assert_eq!(
        names(&store, "MATCH (a:N {name:\"a\"})-[:R*2..2]-(x) RETURN x.name AS n"),
        Vec::<String>::new(),
    );
}

/// An edge already consumed earlier in the same clause is not available to a
/// var-length segment that follows it.
///
/// One edge `a-b`. `(a)-[:R]-(y)-[:R*1..1]-(z)` must bind `y=b` using the only
/// edge there is, leaving the var-length segment with nothing — 0 rows.
/// Without cross-operator edge tracking the segment walks the same edge back
/// to `a` and returns one.
#[test]
#[ignore = "#710: the operator walks shortest paths, not paths"]
fn a_var_length_segment_may_not_reuse_an_edge_the_clause_already_walked() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:N {name: \"a\"})");
    run(&mut store, "CREATE (:N {name: \"b\"})");
    run(&mut store, "MATCH (x:N {name:\"a\"}), (y:N {name:\"b\"}) CREATE (x)-[:R]->(y)");
    assert_eq!(
        names(
            &store,
            "MATCH (a:N {name:\"a\"})-[:R]-(y)-[:R*1..1]-(z) RETURN z.name AS n"
        ),
        Vec::<String>::new(),
    );
}
