//! `EXISTS { (a)-[:R]->(b) }` with `b` already bound must not enumerate every
//! neighbour of `a` before noticing (#681).
//!
//! The walker expanded all neighbours, cloned the binding record for each, and
//! only checked the pin one recursion level down. For LDBC BI-11 the pinned end
//! is a Tag's other side — ~250 nodes per tag, over ~1.19M outer rows.
//!
//! These tests pin the *semantics*, which the optimisation must not change:
//! filtering the expansion to the pinned node has to give the same answers as
//! enumerating and rejecting. The cases that would break a careless version are
//! a pinned node that is a neighbour, one that is not, one reachable only by
//! the wrong direction, and one reachable only by the wrong edge type.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    match out.records.first().and_then(|r| r.get("c")) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("expected integer c, got {other:?}"),
    }
}

/// a -[:R]-> hub <-[:R]- b, plus decoys hanging off the hub so a naive walker
/// has something to enumerate, and c which shares no hub with a.
fn seeded() -> GraphStore {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:N {name: 'a'})");
    run(&mut store, "CREATE (:N {name: 'b'})");
    run(&mut store, "CREATE (:N {name: 'c'})");
    run(&mut store, "CREATE (:H {name: 'hub'})");
    run(&mut store, "CREATE (:N {name: 'd1'})");
    run(&mut store, "CREATE (:N {name: 'd2'})");
    run(&mut store, "MATCH (a:N {name:'a'}), (h:H) CREATE (a)-[:R]->(h)");
    run(&mut store, "MATCH (b:N {name:'b'}), (h:H) CREATE (b)-[:R]->(h)");
    run(&mut store, "MATCH (d:N {name:'d1'}), (h:H) CREATE (d)-[:R]->(h)");
    run(&mut store, "MATCH (d:N {name:'d2'}), (h:H) CREATE (d)-[:R]->(h)");
    // c attaches by a different edge type, so it must not count as sharing.
    run(&mut store, "CREATE (:H {name: 'hub2'})");
    run(&mut store, "MATCH (c:N {name:'c'}), (h:H {name:'hub2'}) CREATE (c)-[:OTHER]->(h)");
    store
}

#[test]
fn a_pinned_endpoint_that_is_reachable_matches() {
    let store = seeded();
    // BI-11's exact shape: both ends bound, meeting at a shared middle node.
    assert_eq!(
        count(&store,
            "MATCH (a:N {name:'a'}), (b:N {name:'b'}) \
             WHERE EXISTS { MATCH (a)-[:R]->(h:H)<-[:R]-(b) } RETURN count(*) AS c"),
        1
    );
}

#[test]
fn a_pinned_endpoint_that_is_not_reachable_does_not_match() {
    let store = seeded();
    // c hangs off a different hub by a different edge type.
    assert_eq!(
        count(&store,
            "MATCH (a:N {name:'a'}), (c:N {name:'c'}) \
             WHERE EXISTS { MATCH (a)-[:R]->(h:H)<-[:R]-(c) } RETURN count(*) AS c"),
        0
    );
}

#[test]
fn the_negation_is_the_complement() {
    // BI-11 is NOT EXISTS, so the inverse has to hold on the same fixture.
    let store = seeded();
    assert_eq!(
        count(&store,
            "MATCH (a:N {name:'a'}), (b:N {name:'b'}) \
             WHERE NOT EXISTS { MATCH (a)-[:R]->(h:H)<-[:R]-(b) } RETURN count(*) AS c"),
        0
    );
    assert_eq!(
        count(&store,
            "MATCH (a:N {name:'a'}), (c:N {name:'c'}) \
             WHERE NOT EXISTS { MATCH (a)-[:R]->(h:H)<-[:R]-(c) } RETURN count(*) AS c"),
        1
    );
}

#[test]
fn direction_is_still_honoured_for_a_pinned_endpoint() {
    // b reaches the hub by an outgoing edge; demanding an incoming one from the
    // hub's perspective must fail. A filter that ignores direction would pass.
    let store = seeded();
    assert_eq!(
        count(&store,
            "MATCH (a:N {name:'a'}), (b:N {name:'b'}) \
             WHERE EXISTS { MATCH (a)-[:R]->(h:H)-[:R]->(b) } RETURN count(*) AS c"),
        0
    );
}

#[test]
fn edge_type_is_still_honoured_for_a_pinned_endpoint() {
    let store = seeded();
    assert_eq!(
        count(&store,
            "MATCH (a:N {name:'a'}), (b:N {name:'b'}) \
             WHERE EXISTS { MATCH (a)-[:R]->(h:H)<-[:OTHER]-(b) } RETURN count(*) AS c"),
        0
    );
}
