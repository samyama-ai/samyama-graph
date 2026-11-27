//! A repeated `CREATE` of a key held under a unique constraint declared
//! `OPTIONS {onDuplicate: 'update'}` updates the node holding it (#2176).
//!
//! The issue: a client that times out does not know whether its write
//! committed, and retrying a `CREATE` either made a second node or, under a
//! unique constraint, was refused. The second write should be an update of the
//! first, as a new MVCC version, and never a duplicate. The key is the
//! constraint's property, declared in the schema, so it holds for every client
//! and not only for one that sends an `Idempotency-Key`.
//!
//! Opt-in: a constraint without the option refuses the duplicate exactly as
//! before, which is what openCypher specifies.

use samyama::graph::{GraphStore, IsolationLevel};
use samyama::persistence::PersistenceManager;
use samyama::query::QueryEngine;

const T: &str = "default";

fn run(store: &mut GraphStore, q: &str) {
    QueryEngine::new()
        .execute_mut(q, store, T)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn try_run(store: &mut GraphStore, q: &str) -> Result<(), String> {
    QueryEngine::new()
        .execute_mut(q, store, T)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// The values of the query's columns, one string per row.
fn rows(store: &GraphStore, q: &str) -> Vec<String> {
    let batch = QueryEngine::new()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    batch
        .records
        .iter()
        .map(|r| {
            batch
                .columns
                .iter()
                .map(|c| format!("{:?}", r.get(c).unwrap()))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

fn count(store: &GraphStore, label: &str) -> usize {
    QueryEngine::new()
        .execute(&format!("MATCH (n:{label}) RETURN n"), store)
        .unwrap()
        .records
        .len()
}

fn upsert_store() -> GraphStore {
    let mut s = GraphStore::new();
    run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.id IS UNIQUE OPTIONS {onDuplicate: 'update'}",
    );
    s
}

#[test]
fn a_repeated_create_updates_the_node_instead_of_duplicating_it() {
    let mut s = upsert_store();
    run(&mut s, "CREATE (:Order {id: 1, total: 10})");
    run(&mut s, "CREATE (:Order {id: 1, total: 20, status: 'paid'})");

    assert_eq!(count(&s, "Order"), 1);
    assert_eq!(
        rows(&s, "MATCH (o:Order {id: 1}) RETURN o.total, o.status"),
        vec!["Property(Integer(20)) Property(String(\"paid\"))"]
    );
}

#[test]
fn the_update_is_a_new_version_and_a_reader_already_open_sees_the_old_one() {
    let mut s = upsert_store();
    run(&mut s, "CREATE (:Order {id: 1, total: 10})");
    let id = s
        .find_node_by_unique(&"Order".into(), "id", &1.into())
        .unwrap()
        .unwrap();
    let first_version = s.get_node(id).unwrap().version;

    // A snapshot opened before the retry. Old versions are kept for as long
    // as a reader needs them and collected after, as for any other write.
    let reader = s.begin_transaction(IsolationLevel::SnapshotIsolation);
    run(&mut s, "CREATE (:Order {id: 1, total: 20})");

    let seen = s
        .get_node_for_txn(reader, id)
        .expect("reader sees the node");
    assert_eq!(
        seen.get_property("total").and_then(|v| v.as_integer()),
        Some(10)
    );
    assert_eq!(
        s.node_property(id, "total").and_then(|v| v.as_integer()),
        Some(20)
    );
    assert!(s.get_node(id).unwrap().version > first_version);
}

#[test]
fn the_created_variable_is_bound_to_the_node_that_was_updated() {
    let mut s = upsert_store();
    run(&mut s, "CREATE (:Order {id: 1, total: 10})");
    let first = rows(&s, "MATCH (o:Order) RETURN id(o)");

    let batch = QueryEngine::new()
        .execute_mut(
            "CREATE (o:Order {id: 1, total: 30}) RETURN id(o), o.total",
            &mut s,
            T,
        )
        .unwrap();
    let got: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            format!(
                "{:?} {:?}",
                r.get("id(o)").unwrap(),
                r.get("o.total").unwrap()
            )
        })
        .collect();
    assert_eq!(got, vec![format!("{} Property(Integer(30))", first[0])]);
}

#[test]
fn properties_the_create_does_not_mention_are_kept_and_labels_added() {
    let mut s = upsert_store();
    run(&mut s, "CREATE (:Order {id: 1, total: 10, note: 'gift'})");
    run(&mut s, "CREATE (:Order:Priority {id: 1, total: 11})");

    assert_eq!(count(&s, "Order"), 1);
    assert_eq!(count(&s, "Priority"), 1);
    assert_eq!(
        rows(&s, "MATCH (o:Order {id: 1}) RETURN o.total, o.note"),
        vec!["Property(Integer(11)) Property(String(\"gift\"))"]
    );
}

#[test]
fn without_the_option_a_duplicate_is_still_refused() {
    let mut s = GraphStore::new();
    run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.id IS UNIQUE",
    );
    run(&mut s, "CREATE (:Order {id: 1, total: 10})");
    let err = try_run(&mut s, "CREATE (:Order {id: 1, total: 20})").unwrap_err();
    assert!(err.contains("already has value"), "{err}");
    assert_eq!(count(&s, "Order"), 1);
    assert_eq!(
        rows(&s, "MATCH (o:Order) RETURN o.total"),
        vec!["Property(Integer(10))"]
    );
}

#[test]
fn on_duplicate_error_turns_the_update_back_off() {
    let mut s = upsert_store();
    run(&mut s, "CREATE (:Order {id: 1})");
    run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.id IS UNIQUE OPTIONS {onDuplicate: 'error'}",
    );
    assert!(try_run(&mut s, "CREATE (:Order {id: 1})").is_err());

    // Restating the constraint without OPTIONS leaves its mode as it is.
    run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.id IS UNIQUE OPTIONS {onDuplicate: 'update'}",
    );
    run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.id IS UNIQUE",
    );
    run(&mut s, "CREATE (:Order {id: 1, total: 5})");
    assert_eq!(count(&s, "Order"), 1);
}

#[test]
fn the_legacy_form_takes_the_option_too() {
    let mut s = GraphStore::new();
    run(
        &mut s,
        "CREATE CONSTRAINT ON (o:Order) ASSERT o.id IS UNIQUE OPTIONS {onDuplicate: 'update'}",
    );
    run(&mut s, "CREATE (:Order {id: 1})");
    run(&mut s, "CREATE (:Order {id: 1})");
    assert_eq!(count(&s, "Order"), 1);
}

#[test]
fn an_unknown_option_or_value_is_refused() {
    let mut s = GraphStore::new();
    let err = try_run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.id IS UNIQUE OPTIONS {onDuplicte: 'update'}",
    )
    .unwrap_err();
    assert!(err.contains("unknown option `onDuplicte`"), "{err}");

    let err = try_run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.id IS UNIQUE OPTIONS {onDuplicate: 'merge'}",
    )
    .unwrap_err();
    assert!(err.contains("'error' or 'update'"), "{err}");
}

#[test]
fn keys_held_by_two_different_nodes_are_refused_not_merged() {
    let mut s = upsert_store();
    run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.ref IS UNIQUE OPTIONS {onDuplicate: 'update'}",
    );
    run(&mut s, "CREATE (:Order {id: 1, ref: 'a', total: 1})");
    run(&mut s, "CREATE (:Order {id: 2, ref: 'b', total: 2})");

    let err = try_run(&mut s, "CREATE (:Order {id: 1, ref: 'b', total: 9})").unwrap_err();
    assert!(err.contains("does not merge two"), "{err}");
    assert_eq!(count(&s, "Order"), 2);
    assert_eq!(
        rows(&s, "MATCH (o:Order) RETURN o.id, o.total ORDER BY o.id"),
        vec![
            "Property(Integer(1)) Property(Integer(1))",
            "Property(Integer(2)) Property(Integer(2))"
        ]
    );
}

#[test]
fn an_update_that_would_break_another_constraint_changes_nothing() {
    let mut s = upsert_store();
    // `email` is unique in the default mode: taking another order's email is
    // still a violation, and the update is refused before any property is set.
    run(
        &mut s,
        "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.email IS UNIQUE",
    );
    run(&mut s, "CREATE (:Order {id: 1, email: 'a@x', total: 1})");
    run(&mut s, "CREATE (:Order {id: 2, email: 'b@x', total: 2})");

    let err = try_run(&mut s, "CREATE (:Order {id: 1, total: 99, email: 'b@x'})").unwrap_err();
    assert!(err.contains("already has value"), "{err}");
    assert_eq!(
        rows(&s, "MATCH (o:Order {id: 1}) RETURN o.total, o.email"),
        vec!["Property(Integer(1)) Property(String(\"a@x\"))"]
    );
}

#[test]
fn unwind_rows_with_a_repeated_key_make_one_node() {
    let mut s = upsert_store();
    run(
        &mut s,
        "UNWIND [{id: 1, t: 1}, {id: 2, t: 2}, {id: 1, t: 3}] AS r \
         CREATE (:Order {id: r.id, total: r.t})",
    );
    assert_eq!(count(&s, "Order"), 2);
    assert_eq!(
        rows(&s, "MATCH (o:Order) RETURN o.id, o.total ORDER BY o.id"),
        vec![
            "Property(Integer(1)) Property(Integer(3))",
            "Property(Integer(2)) Property(Integer(2))"
        ]
    );
}

#[test]
fn a_relationship_created_with_the_node_attaches_to_the_updated_one() {
    let mut s = upsert_store();
    run(&mut s, "CREATE (:Order {id: 1})");
    run(
        &mut s,
        "CREATE (:Order {id: 1, total: 5})-[:FOR]->(:Customer {name: 'c'})",
    );

    assert_eq!(count(&s, "Order"), 1);
    assert_eq!(
        rows(
            &s,
            "MATCH (o:Order)-[:FOR]->(c:Customer) RETURN o.id, o.total, c.name"
        ),
        vec!["Property(Integer(1)) Property(Integer(5)) Property(String(\"c\"))"]
    );
}

#[test]
fn the_mode_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let pm = PersistenceManager::new(dir.path()).unwrap();
        pm.tenants()
            .create_tenant(T.to_string(), T.to_string(), None)
            .ok();
        let mut s = GraphStore::new();
        s.enable_write_log();
        for q in [
            "CREATE CONSTRAINT FOR (o:Order) REQUIRE o.id IS UNIQUE OPTIONS {onDuplicate: 'update'}",
            "CREATE (:Order {id: 1, total: 10})",
        ] {
            run(&mut s, q);
            let muts = s.take_write_log();
            pm.apply_mutations(T, &s, &muts).unwrap();
        }
        pm.checkpoint().unwrap();
    }

    let pm = PersistenceManager::new(dir.path()).unwrap();
    let mut s = GraphStore::new();
    s.enable_write_log();
    pm.recover_into(T, &mut s).expect("recover");

    run(&mut s, "CREATE (:Order {id: 1, total: 20})");
    let muts = s.take_write_log();
    pm.apply_mutations(T, &s, &muts).unwrap();
    assert_eq!(count(&s, "Order"), 1);
    assert_eq!(
        rows(&s, "MATCH (o:Order) RETURN o.total"),
        vec!["Property(Integer(20))"]
    );
}

#[test]
fn a_graph_without_upsert_constraints_creates_as_before() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE (:Order {id: 1})");
    run(&mut s, "CREATE (:Order {id: 1})");
    assert_eq!(count(&s, "Order"), 2);
}
