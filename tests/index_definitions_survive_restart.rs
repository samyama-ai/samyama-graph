//! Index definitions must come back with the rows (#1477).
//!
//! The rows survived a restart and the three index catalogs did not. Nothing in
//! `src/persistence/` wrote or read an index catalog, so `SHOW INDEXES` was empty
//! on a server started against a data directory that had three indexes in it, and
//! the three structures then failed differently:
//!
//! | structure | after restart |
//! |---|---|
//! | BTREE    | the predicate replans as `Filter` over `NodeScan` — right answer, wrong plan |
//! | FULLTEXT | `no full-text index named 'ftidx'` |
//! | VECTOR   | an empty result set, no error — indistinguishable from "nothing matched" |
//!
//! Every assertion here goes through a surface a user has: `SHOW INDEXES`, an
//! `EXPLAIN` plan, and the two index procedures. None of them reads a private
//! field, because a restored definition with nothing in it passes a field check
//! and still answers every query with zero rows.
//!
//! The restart is a real one: the `PersistenceManager` is dropped and reopened on
//! the same directory, and the `GraphStore` is thrown away and rebuilt.

use samyama::graph::GraphStore;
use samyama::persistence::PersistenceManager;
use samyama::query::QueryEngine;

const T: &str = "default";

struct Server {
    dir: tempfile::TempDir,
    pm: Option<PersistenceManager>,
    store: GraphStore,
}

impl Server {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let pm = PersistenceManager::new(dir.path()).unwrap();
        pm.tenants()
            .create_tenant(T.to_string(), T.to_string(), None)
            .ok();
        let mut store = GraphStore::new();
        store.enable_write_log();
        Server { dir, pm: Some(pm), store }
    }

    fn pm(&self) -> &PersistenceManager {
        self.pm.as_ref().unwrap()
    }

    /// One statement, persisted the way the server persists it.
    fn write(&mut self, q: &str) {
        QueryEngine::new()
            .execute_mut(q, &mut self.store, T)
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        let muts = self.store.take_write_log();
        self.pm
            .as_ref()
            .unwrap()
            .apply_mutations(T, &self.store, &muts)
            .unwrap_or_else(|e| panic!("persist {q}: {e}"));
    }

    /// Stop the process and start it again on the same `--data-path`.
    fn restart(&mut self) {
        self.pm.as_ref().unwrap().checkpoint().unwrap();
        // Dropped before the reopen: RocksDB holds an exclusive lock on the
        // directory, so a restart that did not drop it would not be one.
        self.pm = None;
        let pm = PersistenceManager::new(self.dir.path()).unwrap();
        let mut store = GraphStore::new();
        store.enable_write_log();
        pm.recover_into(T, &mut store).expect("recover");
        self.store = store;
        self.pm = Some(pm);
    }

    fn rows(&self, q: &str) -> Vec<String> {
        let batch = QueryEngine::new()
            .execute(q, &self.store)
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        batch
            .records
            .iter()
            .map(|r| {
                batch
                    .columns
                    .iter()
                    .map(|c| format!("{:?}", r.get(c)))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    }

    fn try_rows(&self, q: &str) -> Result<usize, String> {
        QueryEngine::new()
            .execute(q, &self.store)
            .map(|b| b.records.len())
            .map_err(|e| e.to_string())
    }

    fn plan(&self, q: &str) -> String {
        let batch = QueryEngine::new()
            .execute(&format!("EXPLAIN {q}"), &self.store)
            .expect("explain");
        format!("{:?}", batch.records[0].get("plan")).replace("\\n", "\n")
    }

    fn index_kinds(&self) -> Vec<String> {
        self.rows("SHOW INDEXES")
    }
}

/// Rows first, then the three index definitions — the order the issue reproduces.
fn seeded() -> Server {
    let mut s = Server::new();
    s.write(
        "CREATE (:P {id: 1, body: 'quick brown fox', emb: [0.1, 0.1, 0.1, 0.1]}) \
         CREATE (:P {id: 2, body: 'lazy dog sleeps', emb: [0.9, 0.9, 0.9, 0.9]}) \
         CREATE (:P {id: 3, body: 'a quick answer', emb: [0.2, 0.2, 0.2, 0.2]})",
    );
    s.write("CREATE INDEX ON :P(id)");
    s.write("CREATE FULLTEXT INDEX ftidx FOR (d:P) ON (d.body)");
    s.write("CREATE VECTOR INDEX vidx FOR (v:P) ON (v.emb) OPTIONS {dimensions: 4}");
    s
}

#[test]
fn show_indexes_lists_all_three_after_a_restart() {
    let mut s = seeded();
    let before = s.index_kinds();
    assert_eq!(before.len(), 3, "three indexes before the restart: {before:?}");

    s.restart();

    let after = s.index_kinds();
    assert_eq!(
        after, before,
        "SHOW INDEXES must read the same after a restart as before one"
    );
    let joined = after.join("\n");
    assert!(joined.contains("BTREE"), "no BTREE row: {joined}");
    assert!(joined.contains("FULLTEXT[ftidx]"), "no FULLTEXT row: {joined}");
    assert!(joined.contains("VECTOR"), "no VECTOR row: {joined}");
}

#[test]
fn the_rows_are_still_there_so_the_definitions_are_the_only_thing_at_stake() {
    let mut s = seeded();
    s.restart();
    assert_eq!(s.rows("MATCH (p:P) RETURN count(p)"), vec!["Some(Property(Integer(3)))".to_string()]);
}

#[test]
fn a_btree_predicate_still_plans_as_an_index_scan_after_a_restart() {
    let mut s = seeded();
    assert!(
        s.plan("MATCH (p:P {id: 1}) RETURN p").contains("IndexScan"),
        "the index is not being used before the restart, so this test proves nothing"
    );

    s.restart();

    let plan = s.plan("MATCH (p:P {id: 1}) RETURN p");
    assert!(
        plan.contains("IndexScan"),
        "the predicate replanned as a scan, so the definition did not come back:\n{plan}"
    );
    assert_eq!(
        s.rows("MATCH (p:P {id: 1}) RETURN p.id"),
        vec!["Some(Property(Integer(1)))".to_string()],
        "a restored index that answers with nothing is worse than no index"
    );
}

#[test]
fn full_text_search_answers_by_name_after_a_restart() {
    let mut s = seeded();
    s.restart();

    let hits = s
        .try_rows("CALL db.index.fulltext.queryNodes('ftidx', 'quick') YIELD node RETURN node")
        .unwrap_or_else(|e| panic!("the index name did not survive the restart: {e}"));
    assert_eq!(hits, 2, "both 'quick' documents must come back from the restored index");
}

#[test]
fn a_vector_search_returns_its_rows_after_a_restart() {
    // The dangerous case. A definition restored empty turns `200 []` into a
    // plausible-looking empty index, so this asserts rows, not declaration.
    let mut s = seeded();
    s.restart();

    let by_key = s
        .try_rows(
            "CALL db.index.vector.queryNodes('P', 'emb', [0.1, 0.1, 0.1, 0.1], 3) \
             YIELD node, score RETURN node",
        )
        .expect("vector search must run");
    assert_eq!(by_key, 3, "the restored vector index is declared but empty");

    let by_name = s
        .try_rows(
            "CALL db.index.vector.queryNodes('vidx', 3, [0.1, 0.1, 0.1, 0.1]) \
             YIELD node, score RETURN node",
        )
        .unwrap_or_else(|e| panic!("the vector index name did not survive the restart: {e}"));
    assert_eq!(by_name, 3, "resolving by name found the index but not its contents");
}

#[test]
fn a_dropped_index_does_not_come_back() {
    // The catalog is a snapshot of what exists, not a log of what was created.
    // A log would resurrect every index anyone ever dropped.
    let mut s = seeded();
    s.write("DROP INDEX ON :P(id)");
    s.write("DROP FULLTEXT INDEX ftidx");

    s.restart();

    let after = s.index_kinds().join("\n");
    assert!(!after.contains("BTREE"), "the dropped BTREE index came back: {after}");
    assert!(!after.contains("FULLTEXT"), "the dropped full-text index came back: {after}");
    assert!(after.contains("VECTOR"), "the index that was not dropped is gone: {after}");
}

#[test]
fn definitions_restored_before_the_rows_are_not_silently_half_built() {
    // The order chosen is rows first, definitions rebuilt from them. The other
    // order is only correct if every restored index is maintained as the rows
    // arrive, and `insert_recovered_node` maintains none of the three. This
    // asserts that the wrong order fails loudly-by-emptiness rather than
    // half-working, so that nobody reorders `recover_into` and finds the tests
    // still green.
    let s = seeded();
    let catalog = {
        s.pm().checkpoint().unwrap();
        s.pm().load_index_catalog(T).expect("catalog")
    };
    let (nodes, edges) = s.pm().recover(T).unwrap();
    assert_eq!(nodes.len(), 3);

    let mut store = GraphStore::new();
    store.restore_index_catalog(&catalog);
    for n in nodes {
        store.insert_recovered_node(n);
    }
    for e in edges {
        store.insert_recovered_edge(e).unwrap();
    }

    let engine = QueryEngine::new();
    let ft = engine
        .execute(
            "CALL db.index.fulltext.queryNodes('ftidx', 'quick') YIELD node RETURN node",
            &store,
        )
        .expect("the definition is there, so the call resolves")
        .records
        .len();
    assert_eq!(
        ft, 0,
        "declaring before the rows arrive produced a populated index; if row \
         recovery now maintains the indexes, `recover_into` may declare first — \
         until then this emptiness is the reason it must not"
    );
}

#[test]
fn a_unique_constraint_still_refuses_a_duplicate_after_a_restart() {
    // The only one of the four whose loss changes an *answer* rather than a
    // plan: an unpersisted constraint does not make the next write slower, it
    // lets a duplicate in. `SHOW CONSTRAINTS` is the surface, and the write it
    // must refuse is the proof that it was populated and not just declared.
    let mut s = Server::new();
    s.write("CREATE (:U {email: 'a@x'})");
    s.write("CREATE CONSTRAINT FOR (u:U) REQUIRE u.email IS UNIQUE");
    assert_eq!(s.rows("SHOW CONSTRAINTS").len(), 1);

    s.restart();

    assert_eq!(
        s.rows("SHOW CONSTRAINTS").len(),
        1,
        "the constraint is not listed after the restart"
    );
    let err = QueryEngine::new()
        .execute_mut("CREATE (:U {email: 'a@x'})", &mut s.store, T)
        .err();
    assert!(
        err.is_some(),
        "the duplicate was accepted: the constraint came back declared but empty"
    );
    s.write("CREATE (:U {email: 'b@x'})");
}
