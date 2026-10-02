//! A `shortestPath` clause whose endpoints an earlier MATCH bound searches
//! from those rows, not from every pair in the label (#1633).
//!
//! The clause used to be planned on its own -- a scan of each endpoint's label
//! crossed with the other, one search per pair -- and hash-joined back to the
//! rows that had already pinned both ends. The WHERE form of an anchored
//! shortest path took 28 s and 8 GB at 1,000 nodes; the inline-property form
//! of the same query took 57 us.

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// A chain of `n` `:Account` nodes, `id` 0..n, linked by `:TRANSFER`.
fn chain(n: i64) -> GraphStore {
    chain_ids(n).0
}

fn chain_ids(n: i64) -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let mut ids: Vec<NodeId> = Vec::new();
    for i in 0..n {
        let id = s.create_node_with_labels([Label::new("Account")]);
        s.set_node_property("default", id, "id", PropertyValue::Integer(i))
            .unwrap();
        ids.push(id);
    }
    for w in ids.windows(2) {
        s.create_edge(w[0], w[1], "TRANSFER").unwrap();
    }
    let idx = parse_query("CREATE INDEX ON :Account(id)").unwrap();
    MutQueryExecutor::new(&mut s, "default".to_string())
        .execute(&idx)
        .unwrap();
    (s, ids)
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("parse");
    match QueryExecutor::new(store).execute(&query).unwrap().records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t
            .lines()
            .take_while(|l| !l.starts_with("---"))
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("{other:?}"),
    }
}

/// Column `l` of every row, sorted, as integers (null as -1).
fn lengths(store: &GraphStore, cypher: &str) -> Vec<i64> {
    let query = parse_query(cypher).expect("parse");
    let mut out: Vec<i64> = QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e}"))
        .records
        .iter()
        .map(|r| match r.get("l") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            Some(Value::Null | Value::Property(PropertyValue::Null)) => -1,
            other => panic!("{other:?}"),
        })
        .collect();
    out.sort();
    out
}

const WHERE_FORM: &str = "MATCH (a:Account), (b:Account) WHERE a.id = 5 AND b.id = 40 \
     MATCH p = shortestPath((a)-[:TRANSFER*]-(b)) RETURN length(p) AS l";

#[test]
fn the_search_runs_over_the_bound_rows_without_rescanning_the_label() {
    let store = chain(200);
    let text = plan(&store, WHERE_FORM);
    assert!(text.contains("ShortestPath"), "{text}");
    assert!(
        !text.contains("NodeScan"),
        "a bound endpoint was scanned again:\n{text}"
    );
    assert!(
        !text.contains("Join"),
        "the search was planned apart and joined back:\n{text}"
    );
    assert_eq!(lengths(&store, WHERE_FORM), vec![35]);
}

/// The plan assertion above is the check; this is the consequence, as a ratio
/// against the inline-property form in the same process, so it does not
/// depend on the machine or the build profile. The re-scan ran one search per
/// pair -- 40,000 of them at this size -- and the bound plan runs one.
#[test]
fn the_where_form_costs_what_the_inline_form_costs() {
    let store = chain(200);
    let inline = "MATCH p = shortestPath((a:Account {id: 5})-[:TRANSFER*]-(b:Account {id: 40})) \
                  RETURN length(p) AS l";
    let time = |q: &str| {
        let query = parse_query(q).unwrap();
        let t = std::time::Instant::now();
        for _ in 0..20 {
            QueryExecutor::new(&store).execute(&query).unwrap();
        }
        t.elapsed()
    };
    time(inline);
    let base = time(inline);
    let bound = time(WHERE_FORM);
    assert!(
        bound < base * 20,
        "WHERE form {bound:?} against inline {base:?}: the search is not running once"
    );
}

#[test]
fn every_incoming_row_gets_its_own_search() {
    let store = chain(50);
    assert_eq!(
        lengths(
            &store,
            "MATCH (a:Account), (b:Account) WHERE a.id IN [0, 10, 20] AND b.id = 30 \
             MATCH p = shortestPath((a)-[:TRANSFER*]->(b)) RETURN length(p) AS l"
        ),
        vec![10, 20, 30]
    );
    // Direction is honoured: nothing reaches 0 going forwards.
    assert_eq!(
        lengths(
            &store,
            "MATCH (a:Account), (b:Account) WHERE a.id = 30 AND b.id = 0 \
             MATCH p = shortestPath((a)-[:TRANSFER*]->(b)) RETURN length(p) AS l"
        ),
        Vec::<i64>::new()
    );
}

#[test]
fn a_bound_start_and_an_unbound_target_scan_only_the_target() {
    let (mut store, ids) = chain_ids(20);
    let other = store.create_node_with_labels([Label::new("Other")]);
    store.create_edge(ids[0], other, "TRANSFER").unwrap();
    let q = "MATCH (a:Account) WHERE a.id = 3 \
             MATCH p = shortestPath((a)-[:TRANSFER*]-(b:Other)) RETURN length(p) AS l";
    let text = plan(&store, q);
    assert!(!text.contains("Join"), "{text}");
    assert_eq!(
        text.matches("NodeScan").count(),
        1,
        "only the target is scanned:\n{text}"
    );
    // id 0 is three hops from id 3, and :Other one more.
    assert_eq!(lengths(&store, q), vec![4]);
}

#[test]
fn labels_and_properties_on_a_bound_endpoint_still_constrain_it() {
    let store = chain(20);
    for (q, want) in [
        (
            "MATCH (a), (b) WHERE a.id = 2 AND b.id = 9 \
          MATCH p = shortestPath((a:Account)-[:TRANSFER*]-(b:Account)) RETURN length(p) AS l",
            vec![7],
        ),
        (
            "MATCH (a), (b) WHERE a.id = 2 AND b.id = 9 \
          MATCH p = shortestPath((a:Missing)-[:TRANSFER*]-(b)) RETURN length(p) AS l",
            vec![],
        ),
        (
            "MATCH (a), (b) WHERE a.id = 2 AND b.id = 9 \
          MATCH p = shortestPath((a)-[:TRANSFER*]-(b {id: 8})) RETURN length(p) AS l",
            vec![],
        ),
        (
            "MATCH (a), (b) WHERE a.id = 2 AND b.id = 9 \
          MATCH p = shortestPath((a {id: 2})-[:TRANSFER*]-(b {id: 9})) RETURN length(p) AS l",
            vec![7],
        ),
    ] {
        assert_eq!(lengths(&store, q), want, "{q}");
    }
}

#[test]
fn the_clause_where_applies_to_the_path() {
    let store = chain(20);
    let base = "MATCH (a:Account), (b:Account) WHERE a.id = 2 AND b.id = 9 \
                MATCH p = shortestPath((a)-[:TRANSFER*]-(b))";
    assert_eq!(
        lengths(
            &store,
            &format!("{base} WHERE length(p) > 5 RETURN length(p) AS l")
        ),
        vec![7]
    );
    assert_eq!(
        lengths(
            &store,
            &format!("{base} WHERE length(p) > 7 RETURN length(p) AS l")
        ),
        Vec::<i64>::new()
    );
}

#[test]
fn all_shortest_paths_returns_each_one() {
    // A diamond: two shortest paths from s to t.
    let mut store = GraphStore::new();
    let mk = |s: &mut GraphStore, v: i64| {
        let id = s.create_node_with_labels([Label::new("D")]);
        s.set_node_property("default", id, "id", PropertyValue::Integer(v))
            .unwrap();
        id
    };
    let (s, x, y, t) = (
        mk(&mut store, 0),
        mk(&mut store, 1),
        mk(&mut store, 2),
        mk(&mut store, 3),
    );
    for (u, v) in [(s, x), (s, y), (x, t), (y, t)] {
        store.create_edge(u, v, "E").unwrap();
    }
    let q = "MATCH (a:D), (b:D) WHERE a.id = 0 AND b.id = 3 \
             MATCH p = allShortestPaths((a)-[:E*]->(b)) RETURN length(p) AS l";
    assert!(!plan(&store, q).contains("Join"));
    assert_eq!(lengths(&store, q), vec![2, 2]);
}

#[test]
fn after_a_with_the_endpoints_are_still_bound() {
    let store = chain(30);
    let q = "MATCH (a:Account), (b:Account) WHERE a.id = 1 AND b.id = 21 WITH a, b \
             MATCH p = shortestPath((a)-[:TRANSFER*]-(b)) RETURN length(p) AS l";
    let text = plan(&store, q);
    assert!(!text.contains("NodeScan"), "{text}");
    assert_eq!(lengths(&store, q), vec![20]);
}

#[test]
fn a_null_endpoint_has_no_path_and_is_not_an_error() {
    let store = chain(10);
    let q = "MATCH (a:Account) WHERE a.id = 1 OPTIONAL MATCH (a)-[:NOPE]->(b) \
             MATCH p = shortestPath((a)-[:TRANSFER*]-(b)) RETURN length(p) AS l";
    assert_eq!(lengths(&store, q), Vec::<i64>::new());
}
