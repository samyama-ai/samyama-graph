//! `WHERE id(n) = …` anchors the scan instead of filtering one (#538).
//!
//! `id()` is unique by construction, so this needs no statistics and no cost
//! model — which is what made it worth handling separately from the
//! index-selection path. Before, `MATCH (n) WHERE id(n) = 5` scanned the whole
//! label and filtered, and `shortestPath(…) WHERE id(a) = 1 AND id(b) = …` ran
//! ~1000× slower than the same query written with inline properties.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph(n: i64) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..n {
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(i));
    }
    store
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("parse");
    match QueryExecutor::new(store).execute(&query).unwrap().records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => {
            t.lines().take_while(|l| !l.starts_with("---")).collect::<Vec<_>>().join("\n")
        }
        other => panic!("{other:?}"),
    }
}

fn values(store: &GraphStore, cypher: &str) -> Vec<i64> {
    let query = parse_query(cypher).expect("parse");
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap()
        .records
        .iter()
        .map(|r| match r.get("v") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn an_id_equality_anchors_the_scan() {
    let store = graph(1000);
    let text = plan(&store, "MATCH (n:N) WHERE id(n) = 5 RETURN n.v AS v");
    assert!(text.contains("NodeById"), "{text}");
    assert!(!text.contains("NodeScan"), "the label scan should be gone:\n{text}");
}

#[test]
fn it_works_without_a_label_too() {
    let store = graph(1000);
    let text = plan(&store, "MATCH (n) WHERE id(n) = 5 RETURN n.v AS v");
    assert!(text.contains("NodeById"), "{text}");
}

#[test]
fn the_answer_is_unchanged() {
    let store = graph(1000);
    assert_eq!(values(&store, "MATCH (n:N) WHERE id(n) = 5 RETURN n.v AS v").len(), 1);
    // Ids start at 1, and `v` counts from 0, so id 5 holds v = 4.
    assert_eq!(values(&store, "MATCH (n:N) WHERE id(n) = 5 RETURN n.v AS v"), vec![4]);
}

#[test]
fn a_label_that_does_not_match_returns_nothing() {
    // The hazard of scanning by id: it bypasses the label index, so the
    // pattern's labels have to be checked per node or `(n:Other)` would match
    // whatever node holds that id.
    let mut store = graph(10);
    let other = store.create_node("Other");
    let _ = store.set_node_property("default", other, "v".to_string(), PropertyValue::Integer(99));

    let other_id = other.as_u64() as i64;
    assert_eq!(
        values(&store, &format!("MATCH (n:Other) WHERE id(n) = {other_id} RETURN n.v AS v")),
        vec![99]
    );
    assert!(
        values(&store, &format!("MATCH (n:N) WHERE id(n) = {other_id} RETURN n.v AS v")).is_empty(),
        "an :N pattern must not match an :Other node by id"
    );
}

#[test]
fn an_id_that_does_not_exist_returns_nothing() {
    let store = graph(10);
    assert!(values(&store, "MATCH (n:N) WHERE id(n) = 99999 RETURN n.v AS v").is_empty());
}

#[test]
fn a_negative_id_returns_nothing_rather_than_wrapping() {
    // `as u64` on a negative literal wraps to a very large positive id, which
    // matches nothing but does so after a full scan.
    let store = graph(10);
    assert!(values(&store, "MATCH (n:N) WHERE id(n) = -1 RETURN n.v AS v").is_empty());
}

#[test]
fn the_literal_may_be_on_either_side() {
    let store = graph(100);
    let text = plan(&store, "MATCH (n:N) WHERE 5 = id(n) RETURN n.v AS v");
    assert!(text.contains("NodeById"), "{text}");
    assert_eq!(values(&store, "MATCH (n:N) WHERE 5 = id(n) RETURN n.v AS v"), vec![4]);
}

#[test]
fn an_id_list_anchors_on_every_element() {
    let store = graph(1000);
    let text = plan(&store, "MATCH (n:N) WHERE id(n) IN [3, 5, 7] RETURN n.v AS v");
    assert!(text.contains("NodeById"), "{text}");
    let mut got = values(&store, "MATCH (n:N) WHERE id(n) IN [3, 5, 7] RETURN n.v AS v");
    got.sort();
    assert_eq!(got, vec![2, 4, 6]);
}

#[test]
fn a_point_lookup_does_not_scale_with_the_graph() {
    // The property this is really about: the cost should not depend on how
    // many nodes exist.
    let store = graph(200_000);
    let started = std::time::Instant::now();
    let rows = values(&store, "MATCH (n:N) WHERE id(n) = 5 RETURN n.v AS v");
    let elapsed = started.elapsed();

    assert_eq!(rows, vec![4]);
    assert!(
        elapsed < std::time::Duration::from_millis(5),
        "a point lookup on 200,000 nodes took {elapsed:?} — it is still scanning"
    );
}

#[test]
fn both_endpoints_of_a_path_can_be_pinned_by_id() {
    // #538's original shape.
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..2000).map(|_| store.create_node("N")).collect();
    for w in ids.windows(2) {
        store.create_edge(w[0], w[1], "KNOWS").unwrap();
    }
    let (a, b) = (ids[0].as_u64(), ids[5].as_u64());

    let cypher = format!(
        "MATCH p = shortestPath((a:N)-[:KNOWS*]-(b:N)) WHERE id(a) = {a} AND id(b) = {b} \
         RETURN length(p) AS v"
    );
    let started = std::time::Instant::now();
    let query = parse_query(&cypher).unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(batch.records.len(), 1, "the chain connects them");
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "took {elapsed:?} — the endpoints are not being pinned"
    );
}
