//! What survives a restart (#1094).
//!
//! Durability used to be read off the *result* of a write query: the persist loop in
//! `src/protocol/command.rs` walked the returned bindings for `Node`/`Edge` values. That
//! made it a property of the `RETURN` clause — `CREATE (:Person)` with nothing returned
//! reached no storage, and `DELETE` had no row to describe itself with at all — and the
//! HTTP path did not persist anything under any clause. Every write returned success.
//!
//! These tests drive the write log instead, and each one ends in a real `recover()`, so
//! what they assert is what a restart would actually find.

use samyama::graph::GraphStore;
use samyama::persistence::PersistenceManager;
use samyama::query::QueryEngine;

const T: &str = "default";

struct Db {
    _dir: tempfile::TempDir,
    pm: PersistenceManager,
    engine: QueryEngine,
    store: GraphStore,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let pm = PersistenceManager::new(dir.path()).unwrap();
        pm.tenants()
            .create_tenant(T.to_string(), T.to_string(), None)
            .ok();
        let mut store = GraphStore::new();
        store.enable_write_log();
        Db { _dir: dir, pm, engine: QueryEngine::new(), store }
    }

    /// One statement, persisted the way the server persists it.
    fn write(&mut self, q: &str) {
        self.engine.execute_mut(q, &mut self.store, T).expect(q);
        let muts = self.store.take_write_log();
        self.pm.apply_mutations(T, &self.store, &muts).unwrap();
    }

    /// What a restart would load.
    fn restart(&self) -> (Vec<samyama::graph::Node>, Vec<samyama::graph::Edge>) {
        self.pm.checkpoint().unwrap();
        self.pm.recover(T).unwrap()
    }
}

#[test]
fn a_create_survives_whether_or_not_it_returns_the_node() {
    let mut db = Db::new();
    db.write("CREATE (p:Person {name: \"ada\"}) RETURN p");
    db.write("CREATE (p:Person {name: \"grace\"})");

    let (nodes, _) = db.restart();
    let mut names: Vec<String> = nodes
        .iter()
        .map(|n| n.properties.get("name").unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names.len(), 2, "both creates reached storage");
    assert!(names[0].contains("ada") && names[1].contains("grace"));
}

#[test]
fn a_set_survives_without_a_return_clause() {
    let mut db = Db::new();
    db.write("CREATE (p:Person {name: \"ada\"})");
    db.write("MATCH (p:Person) SET p.name = \"grace\"");

    let (nodes, _) = db.restart();
    assert_eq!(nodes.len(), 1);
    let name = nodes[0].properties.get("name").unwrap().to_string();
    assert!(name.contains("grace"), "stored node still says {name}");
}

#[test]
fn a_delete_is_not_resurrected_by_the_restart() {
    let mut db = Db::new();
    db.write("CREATE (p:Person {name: \"ada\"})");
    assert_eq!(db.restart().0.len(), 1);

    db.write("MATCH (p:Person) DELETE p");
    let (nodes, _) = db.restart();
    assert!(nodes.is_empty(), "the deleted node came back: {nodes:?}");
}

#[test]
fn an_edge_and_its_properties_survive() {
    let mut db = Db::new();
    db.write(
        "CREATE (a:Person {name: \"ada\"})-[:KNOWS {since: 1843}]->(b:Person {name: \"grace\"})",
    );

    let (nodes, edges) = db.restart();
    assert_eq!(nodes.len(), 2);
    assert_eq!(edges.len(), 1, "the edge reached storage");
    assert_eq!(edges[0].edge_type.as_str(), "KNOWS");
    assert!(
        edges[0].properties.get("since").unwrap().to_string().contains("1843"),
        "edge properties: {:?}",
        edges[0].properties
    );
}

#[test]
fn deleting_a_node_persists_the_deletion_of_its_edges() {
    let mut db = Db::new();
    db.write("CREATE (a:Person {name: \"ada\"})-[:KNOWS]->(b:Person {name: \"grace\"})");
    assert_eq!(db.restart().1.len(), 1);

    db.write("MATCH (a:Person {name: \"ada\"}) DETACH DELETE a");
    let (nodes, edges) = db.restart();
    assert_eq!(nodes.len(), 1, "only ada was deleted");
    assert!(
        edges.is_empty(),
        "the cascade deleted the edge in memory but left it on disk: {edges:?}"
    );
}

#[test]
fn a_node_created_and_deleted_in_the_same_session_leaves_nothing() {
    let mut db = Db::new();
    db.write("CREATE (p:Person {name: \"ada\"})");
    db.write("MATCH (p:Person) DELETE p");
    db.write("CREATE (p:Person {name: \"grace\"})");

    let (nodes, _) = db.restart();
    assert_eq!(nodes.len(), 1);
    assert!(nodes[0].properties.get("name").unwrap().to_string().contains("grace"));
}

#[test]
fn repeated_writes_to_one_node_do_not_inflate_the_tenant_count() {
    // `persist_create_node` increments the tenant's node usage every time it is called,
    // so an upsert path that re-persists on each SET would report a graph several times
    // larger than it is, and would hit a quota that was never reached.
    let mut db = Db::new();
    db.write("CREATE (p:Person {name: \"ada\"})");
    for i in 0..5 {
        db.write(&format!("MATCH (p:Person) SET p.age = {i}"));
    }

    let usage = db.pm.tenants().get_usage(T).unwrap();
    assert_eq!(usage.node_count, 1, "five SETs on one node counted as {} nodes", usage.node_count);
    assert_eq!(db.restart().0.len(), 1);
}

#[test]
fn a_failed_statement_does_not_persist_its_partial_log() {
    let mut db = Db::new();
    db.write("CREATE (p:Person {name: \"ada\"})");

    // Fails at execution, after the CREATE has already touched the store.
    let _ = db
        .engine
        .execute_mut("CREATE (q:Person {name: \"eve\"}) RETURN nosuchfn(q)", &mut db.store, T);
    let muts = db.store.take_write_log();
    // The server drops the log rather than applying it.
    drop(muts);

    let (nodes, _) = db.restart();
    assert_eq!(nodes.len(), 1, "only the committed statement is on disk");
}
