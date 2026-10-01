//! A node that gains or loses a label after creation is indexed under it (#1590).
//!
//! `add_label_to_node` indexed the node's properties under the new label from
//! the row copy, which has been empty since #1188, so it indexed nothing: a
//! property-index lookup on the new label missed the node, and a unique
//! constraint on the new label neither saw its key nor refused a duplicate.
//! `remove_label_from_node` left the label's entries behind.

use samyama::graph::{GraphError, GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

const T: &str = "default";

fn try_run(store: &mut GraphStore, cypher: &str) -> Result<(), String> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut mutating = MutQueryExecutor::new(store, T.to_string());
    mutating
        .execute(&query)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn run(store: &mut GraphStore, cypher: &str) {
    try_run(store, cypher).unwrap_or_else(|e| panic!("{cypher}: {e}"));
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records.first().and_then(|r| r.get("c")) {
        Some(Value::Property(PropertyValue::Integer(c))) => *c,
        other => panic!("{cypher}: {other:?}"),
    }
}

/// The nodes the plain property index holds for `:label(property) = value`.
fn indexed(store: &GraphStore, label: &str, property: &str, value: i64) -> Vec<NodeId> {
    let index = store
        .property_index
        .get_index(&Label::new(label), property)
        .expect("index exists");
    let mut ids = index.read().unwrap().get(&PropertyValue::Integer(value));
    ids.sort();
    ids
}

fn holders(store: &GraphStore, label: &str, property: &str, value: i64) -> Vec<NodeId> {
    let mut ids = store
        .property_index
        .unique_constraint_holders(&Label::new(label), property, &PropertyValue::Integer(value))
        .expect("constraint exists");
    ids.sort();
    ids
}

fn has_label(store: &GraphStore, id: NodeId, label: &str) -> bool {
    store
        .get_node(id)
        .unwrap()
        .labels
        .contains(&Label::new(label))
}

#[test]
fn a_node_that_gains_a_label_is_found_through_its_index() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE INDEX ON :M(p)");
    run(&mut s, "CREATE (:N {p: 5})");
    run(&mut s, "MATCH (n:N) SET n:M");

    assert_eq!(rows(&s, "MATCH (n:M {p: 5}) RETURN n"), 1);
    assert_eq!(rows(&s, "MATCH (n:M) WHERE n.p = 5 RETURN n"), 1);
    assert_eq!(rows(&s, "MATCH (n:M) RETURN n.p"), 1);
}

#[test]
fn gaining_a_constrained_label_with_a_taken_key_is_refused() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE CONSTRAINT ON (n:U) ASSERT n.k IS UNIQUE");
    run(&mut s, "CREATE (:U {k: 1})");
    run(&mut s, "CREATE (:X {k: 1})");
    let x = s.get_nodes_by_label(&Label::new("X"))[0].id;
    let version = s.get_node(x).unwrap().version;

    let err = try_run(&mut s, "MATCH (n:X) SET n:U").expect_err("duplicate key admitted");
    assert!(err.contains("already has value"), "{err}");

    // Refused, and nothing of it is left behind.
    assert!(!has_label(&s, x, "U"));
    assert_eq!(s.get_node(x).unwrap().version, version);
    assert_eq!(rows(&s, "MATCH (n:U {k: 1}) RETURN n"), 1);
    assert_eq!(rows(&s, "MATCH (n:U) RETURN n"), 1);
    assert!(!holders(&s, "U", "k", 1).contains(&x));
    assert!(!indexed(&s, "U", "k", 1).contains(&x));
}

#[test]
fn the_store_call_refuses_with_a_constraint_violation() {
    let mut s = GraphStore::new();
    s.create_unique_constraint(&Label::new("U"), "k").unwrap();
    let u = s.create_node("U");
    s.set_node_property(T, u, "k", 1i64).unwrap();
    let x = s.create_node("X");
    s.set_node_property(T, x, "k", 1i64).unwrap();

    match s.add_label_to_node(T, x, "U") {
        Err(GraphError::ConstraintViolation(msg)) => {
            assert!(msg.contains(":U(k) already has value"), "{msg}")
        }
        other => panic!("expected a ConstraintViolation, got {other:?}"),
    }
    assert!(!has_label(&s, x, "U"));
    assert_eq!(s.get_nodes_by_label(&Label::new("U")).len(), 1);
}

#[test]
fn a_merge_that_adds_a_taken_constrained_label_is_refused() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE CONSTRAINT ON (n:U) ASSERT n.k IS UNIQUE");
    run(&mut s, "CREATE (:U {k: 1})");
    run(&mut s, "CREATE (:X {k: 1})");

    let err = try_run(&mut s, "MERGE (n:X {k: 1}) ON MATCH SET n:U")
        .expect_err("MERGE admitted a duplicate key");
    assert!(err.contains("already has value"), "{err}");
    assert_eq!(rows(&s, "MATCH (n:U) RETURN n"), 1);
}

#[test]
fn a_key_is_not_a_duplicate_of_itself() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE CONSTRAINT ON (n:U) ASSERT n.k IS UNIQUE");
    run(&mut s, "CREATE (:U {k: 1})");
    // Setting a label the node already has, and one with no constraint.
    run(&mut s, "MATCH (n:U) SET n:U, n:V");
    assert_eq!(rows(&s, "MATCH (n:U:V {k: 1}) RETURN n"), 1);
}

#[test]
fn a_node_that_gains_a_constrained_label_is_found_by_its_key() {
    let mut s = GraphStore::new();
    s.create_unique_constraint(&Label::new("U"), "k").unwrap();
    let x = s.create_node("X");
    s.set_node_property(T, x, "k", 7i64).unwrap();
    s.add_label_to_node(T, x, "U").unwrap();

    assert_eq!(
        s.find_node_by_unique(&Label::new("U"), "k", &PropertyValue::Integer(7))
            .unwrap(),
        Some(x)
    );
    // And it now takes the key: another :U may not.
    let y = s.create_node("U");
    assert!(matches!(
        s.set_node_property(T, y, "k", 7i64),
        Err(GraphError::ConstraintViolation(_))
    ));
}

#[test]
fn gaining_then_losing_a_label_leaves_no_entry() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE INDEX ON :M(p)");
    run(&mut s, "CREATE CONSTRAINT ON (n:U) ASSERT n.k IS UNIQUE");
    run(&mut s, "CREATE (:N {p: 5, k: 1})");
    run(&mut s, "MATCH (n:N) SET n:M, n:U");
    assert_eq!(rows(&s, "MATCH (n:M {p: 5}) RETURN n"), 1);
    assert_eq!(rows(&s, "MATCH (n:U {k: 1}) RETURN n"), 1);

    run(&mut s, "MATCH (n:N) REMOVE n:M, n:U");
    assert_eq!(rows(&s, "MATCH (n:M {p: 5}) RETURN n"), 0);
    assert_eq!(rows(&s, "MATCH (n:U {k: 1}) RETURN n"), 0);
    assert!(indexed(&s, "M", "p", 5).is_empty());
    assert!(indexed(&s, "U", "k", 1).is_empty());
    assert!(holders(&s, "U", "k", 1).is_empty());
    assert_eq!(
        s.find_node_by_unique(&Label::new("U"), "k", &PropertyValue::Integer(1))
            .unwrap(),
        None
    );

    // The key is free: a new node may take it.
    run(&mut s, "CREATE (:U {k: 1})");
    assert_eq!(rows(&s, "MATCH (n:U {k: 1}) RETURN n"), 1);
}

#[test]
fn a_rolled_back_label_add_leaves_the_indexes_as_they_were() {
    let mut s = GraphStore::new();
    s.create_property_index(&Label::new("M"), "p");
    s.create_unique_constraint(&Label::new("U"), "k").unwrap();
    let n = s.create_node("N");
    s.set_node_property(T, n, "p", 5i64).unwrap();
    s.set_node_property(T, n, "k", 1i64).unwrap();

    s.begin_session_transaction().unwrap();
    s.add_label_to_node(T, n, "M").unwrap();
    s.add_label_to_node(T, n, "U").unwrap();
    assert_eq!(indexed(&s, "M", "p", 5), vec![n]);
    assert_eq!(holders(&s, "U", "k", 1), vec![n]);
    s.rollback_session_transaction().unwrap();

    assert!(!has_label(&s, n, "M"));
    assert!(!has_label(&s, n, "U"));
    assert!(indexed(&s, "M", "p", 5).is_empty());
    assert!(indexed(&s, "U", "k", 1).is_empty());
    assert!(holders(&s, "U", "k", 1).is_empty());
    assert_eq!(rows(&s, "MATCH (n:M {p: 5}) RETURN n"), 0);
    // The key it briefly held is free again.
    let other = s.create_node("U");
    s.set_node_property(T, other, "k", 1i64).unwrap();
}

#[test]
fn a_rolled_back_label_removal_puts_the_entries_back() {
    let mut s = GraphStore::new();
    s.create_unique_constraint(&Label::new("U"), "k").unwrap();
    let n = s.create_node("U");
    s.set_node_property(T, n, "k", 1i64).unwrap();

    s.begin_session_transaction().unwrap();
    s.remove_label_from_node(n, &Label::new("U")).unwrap();
    assert!(holders(&s, "U", "k", 1).is_empty());
    // The freed key is taken by a node the same transaction creates; rolling
    // back must still give the label, and the key, back to `n`.
    let m = s.create_node("U");
    s.set_node_property(T, m, "k", 1i64).unwrap();
    s.rollback_session_transaction().unwrap();

    assert!(has_label(&s, n, "U"));
    assert!(s.get_node(m).is_none());
    assert_eq!(holders(&s, "U", "k", 1), vec![n]);
    assert_eq!(indexed(&s, "U", "k", 1), vec![n]);
    assert_eq!(
        s.find_node_by_unique(&Label::new("U"), "k", &PropertyValue::Integer(1))
            .unwrap(),
        Some(n)
    );
}

/// The index-backed answer agrees with a scan over a random sequence of label
/// changes. A small deterministic generator, so a failure reproduces.
#[test]
fn index_and_scan_agree_across_random_label_changes() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE INDEX ON :M(p)");
    for i in 0..12 {
        run(&mut s, &format!("CREATE (:N {{i: {i}, p: {}}})", i % 4));
    }
    let mut state: u64 = 0x1590_1590;
    let mut next = |bound: u64| {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) % bound
    };
    for step in 0..200 {
        let i = next(12);
        let op = if next(2) == 0 { "SET" } else { "REMOVE" };
        run(&mut s, &format!("MATCH (n:N {{i: {i}}}) {op} n:M"));
        if next(5) == 0 {
            // A property write in between, so values move under the label too.
            let p = next(4);
            run(&mut s, &format!("MATCH (n:N {{i: {i}}}) SET n.p = {p}"));
        }
        for p in 0..4 {
            let by_index = count(&s, &format!("MATCH (n:M {{p: {p}}}) RETURN count(n) AS c"));
            // `+ 0` keeps the equality off the index: a filter over a label scan.
            let by_scan = count(
                &s,
                &format!("MATCH (n:M) WHERE n.p + 0 = {p} RETURN count(n) AS c"),
            );
            let truth = s
                .get_nodes_by_label(&Label::new("M"))
                .iter()
                .filter(|n| s.node_property(n.id, "p") == Some(PropertyValue::Integer(p as i64)))
                .count() as i64;
            assert_eq!(by_scan, truth, "step {step}, p = {p}: scan");
            assert_eq!(by_index, truth, "step {step}, p = {p}: index");
            assert_eq!(
                indexed(&s, "M", "p", p as i64).len() as i64,
                truth,
                "step {step}"
            );
        }
    }
}
