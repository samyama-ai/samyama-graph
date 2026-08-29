//! A variable-length relationship list is in pattern order (#933).
//!
//! ```cypher
//! CREATE (a:A)-[:REL {num: 1}]->(b:B)-[:REL {num: 2}]->(e:End)
//! MATCH (a)-[r:REL*2..2]->(b:End) RETURN r
//! ```
//!
//! answered `[{num: 2}, {num: 1}]`. Two correct relationships in the wrong
//! order, from a query that reported success — so `r[0]` was the last hop.
//!
//! The planner may anchor a variable-length segment at whichever end is more
//! selective and walk back along it: `(a)-[:R*1..2]->(b)` read from `b` is
//! `(b)<-[:R*1..2]-(a)`, and the *pairs* are identical. That reversal is worth
//! a great deal — it is why LDBC IC6 starts at the tag that selects seven
//! rather than expanding 400,257 rows. What is not identical is the **order**
//! of what the walk collects.
//!
//! The pattern here has a label on the far end and none on the near one, which
//! is exactly what makes anchor selection flip it. Tests that only ever walk
//! forwards never see this.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `(a:A)-[:REL {num: 1}]->(b:B)-[:REL {num: 2}]->(e:End)`
fn chain() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let e = store.create_node("End");
    for (src, dst, num) in [(a, b, 1i64), (b, e, 2)] {
        let eid = store.create_edge(src, dst, "REL").unwrap();
        let _ = store.set_edge_property_sparse(eid, "num", PropertyValue::Integer(num));
    }
    store
}

/// The `num` of each relationship in the list bound to `col`, in order.
fn nums(store: &GraphStore, cypher: &str, col: &str) -> Vec<i64> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let rec = batch.records.first().unwrap_or_else(|| panic!("{cypher}: no rows"));
    let items = match rec.get(col) {
        Some(Value::List(items)) => items.clone(),
        other => panic!("{cypher}: {other:?}"),
    };
    items
        .iter()
        .map(|v| {
            let id = match v {
                Value::Edge(id, _) | Value::EdgeRef(id, ..) => *id,
                other => panic!("{other:?}"),
            };
            let edge = store.get_edge(id).expect("edge in store");
            match edge.properties.get("num") {
                Some(PropertyValue::Integer(n)) => *n,
                other => panic!("{other:?}"),
            }
        })
        .collect()
}

#[test]
fn the_list_reads_in_the_pattern_direction_when_the_far_end_is_anchored() {
    // `b:End` is labelled and the near end is not, so the planner anchors
    // there and walks backwards. The list must still read 1, 2.
    let store = chain();
    assert_eq!(nums(&store, "MATCH (a)-[r:REL*2..2]->(b:End) RETURN r", "r"), vec![1, 2]);
}

#[test]
fn the_list_reads_in_the_pattern_direction_when_the_near_end_is_anchored() {
    // The same walk in the direction it is written. This one always worked,
    // and is asserted so a fix cannot trade one direction for the other.
    let store = chain();
    assert_eq!(nums(&store, "MATCH (a:A)-[r:REL*2..2]->(b) RETURN r", "r"), vec![1, 2]);
}

#[test]
fn a_named_path_carries_its_relationships_in_the_same_order() {
    let store = chain();
    assert_eq!(
        nums(&store, "MATCH p = (a)-[:REL*2..2]->(b:End) RETURN relationships(p) AS r", "r"),
        vec![1, 2]
    );
}

#[test]
fn the_path_nodes_run_from_the_start_of_the_pattern() {
    let store = chain();
    let query = parse_query("MATCH p = (a)-[:REL*2..2]->(b:End) RETURN nodes(p) AS n").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let labels: Vec<String> = match batch.records[0].get("n") {
        Some(Value::List(items)) => items
            .iter()
            .map(|v| {
                let id = match v {
                    Value::Node(id, _) | Value::NodeRef(id) => *id,
                    other => panic!("{other:?}"),
                };
                let node = store.get_node(id).unwrap();
                let mut ls: Vec<String> = node.labels.iter().map(|l| l.as_str().to_string()).collect();
                ls.sort();
                ls.join(":")
            })
            .collect(),
        other => panic!("{other:?}"),
    };
    assert_eq!(labels, vec!["A", "B", "End"]);
}

#[test]
fn a_single_hop_is_unaffected() {
    let store = chain();
    assert_eq!(nums(&store, "MATCH (a)-[r:REL*1..1]->(b:B) RETURN r", "r"), vec![1]);
}

#[test]
fn the_pairs_are_the_same_whichever_end_is_anchored() {
    // The reversal is only about ordering. If it ever changed *which* rows
    // come back, that would be a far worse bug than the one being fixed.
    let store = chain();
    let count = |q: &str| {
        let query = parse_query(q).unwrap();
        QueryExecutor::new(&store).execute(&query).unwrap().records.len()
    };
    assert_eq!(count("MATCH (a)-[r:REL*2..2]->(b:End) RETURN r"), 1);
    assert_eq!(count("MATCH (a:A)-[r:REL*2..2]->(b) RETURN r"), 1);
    assert_eq!(count("MATCH (a)-[r:REL*1..2]->(b) RETURN r"), 3);
}
