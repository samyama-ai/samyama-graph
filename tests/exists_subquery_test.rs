//! Regression tests for EXISTS / NOT EXISTS subquery evaluation.
//!
//! These cover the defect where an exclusion filter combined with a traversal
//! silently returned an empty result set: the subquery matcher ignored the
//! outer query's binding for the subquery's end variable, so
//! `NOT EXISTS { MATCH (a)-[:KNOWS]-(b) }` was evaluated as "a has no KNOWS
//! edge at all" rather than "a and b are not connected". In a graph where the
//! anchor has any relationship of that type, that is false for every candidate
//! row, so the whole result set is eliminated with no error.
//!
//! The same matcher also ignored edge direction, never chained multi-segment
//! subquery patterns, and dropped the subquery's own WHERE clause whenever the
//! pattern had at least one relationship. Each of those has a test here.

use samyama::{GraphStore, QueryEngine};

/// Fixed test graph. All KNOWS edges are directed as written:
///
/// ```text
///   A --> B --> C
///   |     |
///   |     +--> D --> E
///   +--> C
/// ```
///
/// Undirected neighbours: A{B,C}  B{A,C,D}  C{A,B}  D{B,E}  E{D}
///
/// Ages: A=30 B=25 C=35 D=40 E=28
fn setup() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    for (name, age) in [("A", 30), ("B", 25), ("C", 35), ("D", 40), ("E", 28)] {
        let q = format!("CREATE (n:Person {{name: '{}', age: {}}})", name, age);
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    for (from, to) in [("A", "B"), ("B", "C"), ("B", "D"), ("A", "C"), ("D", "E")] {
        let q = format!(
            "MATCH (a:Person {{name: '{}'}}), (b:Person {{name: '{}'}}) CREATE (a)-[:KNOWS]->(b)",
            from, to
        );
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    store
}

/// Collect a single string column, sorted, so assertions are order-independent.
fn names(store: &GraphStore, query: &str, col: &str) -> Vec<String> {
    let engine = QueryEngine::new();
    let result = engine.execute(query, store).unwrap();
    let mut out: Vec<String> = result
        .records
        .iter()
        .map(|r| {
            r.get(col)
                .unwrap_or_else(|| panic!("missing column {}", col))
                .as_property()
                .unwrap()
                .as_string()
                .unwrap()
                .to_string()
        })
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// The reported defect: traversal + exclusion filter returned zero rows.
// ---------------------------------------------------------------------------

/// 2-hop traversal with a NOT EXISTS exclusion, the shape that regressed to 0 rows.
///
/// Note: this engine treats `*N` as "nodes at shortest-path distance N", not as
/// "endpoints of any N-hop walk". From A that makes `*2` == {D}. D is not a
/// direct neighbour of A, so it survives the exclusion.
#[test]
fn two_hop_traversal_with_exclusion_filter_returns_rows() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (a:Person {name: 'A'})-[:KNOWS*2]-(s:Person)
         WHERE s.name <> 'A' AND NOT EXISTS { MATCH (a)-[:KNOWS]-(s) }
         RETURN DISTINCT s.name AS name",
        "name",
    );
    assert_eq!(got, vec!["D"], "2-hop + NOT EXISTS must not be empty");
}

/// Positive and negative forms must partition the same candidate set.
/// `*1..2` from A reaches {B, C, D}; A's direct neighbours are B and C.
#[test]
fn exists_and_not_exists_partition_the_candidate_set() {
    let store = setup();
    let included = names(
        &store,
        "MATCH (a:Person {name: 'A'})-[:KNOWS*1..2]-(s:Person)
         WHERE s.name <> 'A' AND EXISTS { MATCH (a)-[:KNOWS]-(s) }
         RETURN DISTINCT s.name AS name",
        "name",
    );
    assert_eq!(included, vec!["B", "C"]);

    let excluded = names(
        &store,
        "MATCH (a:Person {name: 'A'})-[:KNOWS*1..2]-(s:Person)
         WHERE s.name <> 'A' AND NOT EXISTS { MATCH (a)-[:KNOWS]-(s) }
         RETURN DISTINCT s.name AS name",
        "name",
    );
    assert_eq!(excluded, vec!["D"]);
}

/// 1-hop traversal + exclusion. A's neighbours are B and C; B knows D, C does not.
#[test]
fn one_hop_traversal_with_exclusion_filter() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (a:Person {name: 'A'})-[:KNOWS]-(f:Person)
         WHERE NOT EXISTS { MATCH (f)-[:KNOWS]-(x:Person {name: 'D'}) }
         RETURN DISTINCT f.name AS name",
        "name",
    );
    assert_eq!(got, vec!["C"]);
}

/// 3-hop traversal + exclusion, to cover depth beyond the reported case.
/// Shortest-path distance 3 from A is {E}; E is not a direct neighbour of A.
#[test]
fn three_hop_traversal_with_exclusion_filter() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (a:Person {name: 'A'})-[:KNOWS*3]-(s:Person)
         WHERE s.name <> 'A' AND NOT EXISTS { MATCH (a)-[:KNOWS]-(s) }
         RETURN DISTINCT s.name AS name",
        "name",
    );
    assert_eq!(got, vec!["E"]);
}

/// Fixed-length 2-hop chain written out explicitly, rather than `*2`. This is a
/// different execution path from variable-length expansion and must agree.
#[test]
fn fixed_length_two_hop_chain_with_exclusion_filter() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (a:Person {name: 'A'})-[:KNOWS]-(m:Person)-[:KNOWS]-(s:Person)
         WHERE s.name <> 'A' AND NOT EXISTS { MATCH (a)-[:KNOWS]-(s) }
         RETURN DISTINCT s.name AS name",
        "name",
    );
    assert_eq!(got, vec!["D"]);
}

// ---------------------------------------------------------------------------
// Traversal without a filter, and filter without a traversal, stay correct.
// ---------------------------------------------------------------------------

#[test]
fn two_hop_traversal_without_filter_is_unchanged() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (a:Person {name: 'A'})-[:KNOWS*1..2]-(s:Person)
         WHERE s.name <> 'A'
         RETURN DISTINCT s.name AS name",
        "name",
    );
    assert_eq!(got, vec!["B", "C", "D"]);
}

#[test]
fn exclusion_filter_without_traversal_is_unchanged() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (p:Person)
         WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(o:Person) }
         RETURN p.name AS name",
        "name",
    );
    // Only C and E have no outgoing KNOWS edge.
    assert_eq!(got, vec!["C", "E"]);
}

// ---------------------------------------------------------------------------
// Adjacent defects in the same matcher.
// ---------------------------------------------------------------------------

/// Direction must be honoured. B has an incoming KNOWS from A but not from C,
/// even though B has outgoing edges to both C and D.
#[test]
fn subquery_honours_edge_direction() {
    let store = setup();

    let from_a = names(
        &store,
        "MATCH (p:Person {name: 'B'})
         WHERE EXISTS { MATCH (p)<-[:KNOWS]-(o:Person {name: 'A'}) }
         RETURN p.name AS name",
        "name",
    );
    assert_eq!(from_a, vec!["B"], "A -> B exists, so incoming from A holds");

    let from_c = names(
        &store,
        "MATCH (p:Person {name: 'B'})
         WHERE EXISTS { MATCH (p)<-[:KNOWS]-(o:Person {name: 'C'}) }
         RETURN p.name AS name",
        "name",
    );
    assert!(
        from_c.is_empty(),
        "B -> C is outgoing; there is no incoming edge from C, got {:?}",
        from_c
    );
}

/// A multi-segment subquery pattern must be walked as a chain, not evaluated as
/// independent single hops from the anchor.
///
/// Two outgoing hops exist only from A (A->B->C) and B (B->D->E).
#[test]
fn subquery_chains_multiple_segments() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (p:Person)
         WHERE EXISTS { MATCH (p)-[:KNOWS]->(m:Person)-[:KNOWS]->(t:Person) }
         RETURN p.name AS name",
        "name",
    );
    assert_eq!(got, vec!["A", "B"]);
}

/// The subquery's own WHERE clause must be applied when the pattern has
/// relationships. A->C(35) and B->C(35)/B->D(40) qualify; D->E(28) does not.
#[test]
fn subquery_where_clause_applies_with_relationships() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (p:Person)
         WHERE EXISTS { MATCH (p)-[:KNOWS]->(o:Person) WHERE o.age > 30 }
         RETURN p.name AS name",
        "name",
    );
    assert_eq!(got, vec!["A", "B"]);
}

/// Inline property constraints on the subquery's end node must be applied.
#[test]
fn subquery_applies_end_node_properties() {
    let store = setup();
    let got = names(
        &store,
        "MATCH (p:Person)
         WHERE EXISTS { MATCH (p)-[:KNOWS]->(o:Person {name: 'C'}) }
         RETURN p.name AS name",
        "name",
    );
    assert_eq!(got, vec!["A", "B"]);
}
