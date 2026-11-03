//! A per-row MERGE must see what earlier rows of the same statement wrote —
//! with an index on the merge property and without one (#1467).
//!
//! The invariant, not the counts: the same statement must build the same graph
//! either way. Index maintenance used to be asynchronous whenever the store had
//! an event subscriber, which every server has (`main.rs`), so MERGE's index
//! lookup read a snapshot with none of the statement's own writes in it. Every
//! row missed and created: 500 CSV rows made 1000 nodes where 50 were right,
//! and no error was raised.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::csv_source::set_import_root;
use samyama::query::QueryEngine;
use std::io::Write;

const T: &str = "default";

/// The graph as comparable data: every `:N` node's `id`, and every edge as the
/// `id` pair of its endpoints. Sorted, so the answer does not depend on the
/// order the rows happened to create things in.
fn shape(store: &GraphStore) -> (Vec<i64>, Vec<(i64, i64)>) {
    let id_of = |n: samyama::graph::types::NodeId| match store.node_property(n, "id") {
        Some(PropertyValue::Integer(i)) => i,
        other => panic!("node {n:?} has no integer id: {other:?}"),
    };
    let mut nodes: Vec<i64> = store
        .get_nodes_by_label(&"N".into())
        .iter()
        .map(|n| id_of(n.id))
        .collect();
    nodes.sort_unstable();
    let mut edges: Vec<(i64, i64)> = store
        .all_edges()
        .iter()
        .map(|e| (id_of(e.source), id_of(e.target)))
        .collect();
    edges.sort_unstable();
    (nodes, edges)
}

/// Run `query` on a fresh store, with or without an index on `:N(id)`.
///
/// The store is built with an event subscriber and the receiver is held but
/// never drained. That is the server's configuration and the worst case of the
/// race it used to create: if any index a query reads back is maintained off
/// this thread, this store never sees the update.
fn run(query: &str, with_index: bool) -> (Vec<i64>, Vec<(i64, i64)>) {
    let (mut store, _rx) = GraphStore::with_async_indexing();
    let engine = QueryEngine::new();
    if with_index {
        engine
            .execute_mut("CREATE INDEX ON :N(id)", &mut store, T)
            .expect("create index");
    }
    engine.execute_mut(query, &mut store, T).expect("run query");
    shape(&store)
}

#[test]
fn load_csv_merge_builds_the_same_graph_with_and_without_an_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("e.csv");
    let mut body = String::from("a,b\n");
    for i in 0..500 {
        body.push_str(&format!("{},{}\n", i % 50, (i + 1) % 50));
    }
    write!(std::fs::File::create(&path).unwrap(), "{body}").unwrap();
    set_import_root(Some(dir.path())).unwrap();

    let query = format!(
        "LOAD CSV WITH HEADERS FROM 'file://{}' AS row \
         MERGE (a:N {{id: toInteger(row.a)}}) \
         MERGE (b:N {{id: toInteger(row.b)}}) \
         MERGE (a)-[:R]->(b)",
        path.display()
    );

    let without = run(&query, false);
    let with = run(&query, true);
    set_import_root(None).unwrap();

    // Sizes first, so a failure prints four numbers rather than a thousand ids.
    assert_eq!(
        (with.0.len(), with.1.len()),
        (without.0.len(), without.1.len()),
        "an index on the merge property changed the size of the graph the same load builds"
    );
    assert_eq!(with, without, "same sizes, different graph");
}

#[test]
fn unwind_merge_builds_the_same_graph_with_and_without_an_index() {
    // The same fault, no CSV involved: any statement whose MERGE runs per row.
    let query = "UNWIND range(0, 499) AS x MERGE (:N {id: x % 50})";
    let without = run(query, false);
    let with = run(query, true);
    assert_eq!(
        with.0.len(),
        without.0.len(),
        "an index on the merge property changed how many nodes the same UNWIND builds"
    );
    assert_eq!(with, without, "same node count, different ids");
}

#[test]
fn a_write_is_visible_through_its_index_to_the_next_statement_on_the_same_store() {
    // The read side of the same defect: the index-backed MATCH plan answered
    // from an index the write had not reached, and returned nothing at all.
    let (mut store, _rx) = GraphStore::with_async_indexing();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE INDEX ON :N(id)", &mut store, T)
        .unwrap();
    engine
        .execute_mut("CREATE (:N {id: 1})", &mut store, T)
        .unwrap();
    let via_index = engine
        .execute("MATCH (n:N {id: 1}) RETURN count(n)", &store)
        .unwrap();
    let via_scan = engine
        .execute("MATCH (n:N) RETURN count(n)", &store)
        .unwrap();
    assert_eq!(
        format!("{:?}", via_index.records[0].get("count(n)")),
        format!("{:?}", via_scan.records[0].get("count(n)")),
        "the indexed lookup and the scan disagree about a node that exists"
    );
}

/// The same load with the server's exact wiring: the real background indexer,
/// running over the store's own index handles, on a second thread.
///
/// `persistence::start_indexer` takes `vector_index` and `property_index` off
/// the store and spawns `start_background_indexer_with_store`; this is that,
/// minus the tenant registry's contents. Two worker threads, so the loop can
/// genuinely make progress while the statement runs.
///
/// Measured, so it is not mistaken for the guard: this one **passed** against
/// the code before the fix, on this host, in a debug build. The indexer kept up
/// there. It is here to show the fix is right with the real loop running beside
/// it — a double-applied insert or a removed-then-reinserted value would show
/// up here — and the two tests above are what actually fail when the fix is
/// reverted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwind_merge_is_correct_with_the_real_background_indexer_running() {
    use std::sync::Arc;

    let (mut store, rx) = GraphStore::with_async_indexing();
    let vector_index = Arc::clone(&store.vector_index);
    let property_index = Arc::clone(&store.property_index);
    let tenants = Arc::new(samyama::persistence::TenantManager::new());
    tokio::spawn(async move {
        GraphStore::start_background_indexer(rx, vector_index, property_index, tenants).await
    });

    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE INDEX ON :N(id)", &mut store, T)
        .unwrap();
    engine
        .execute_mut(
            "UNWIND range(0, 499) AS x MERGE (:N {id: x % 50})",
            &mut store,
            T,
        )
        .unwrap();

    let (nodes, _) = shape(&store);
    let distinct: std::collections::BTreeSet<i64> = nodes.iter().copied().collect();
    assert_eq!(
        nodes.len(),
        distinct.len(),
        "MERGE created {} nodes for {} distinct ids",
        nodes.len(),
        distinct.len()
    );
    assert_eq!(distinct.len(), 50);
}

/// MERGE is not the only reader. An index-backed `MATCH` later in the *same*
/// statement read the same stale index and answered 0 for a node the statement
/// had just created — measured on a persistent server before the fix:
///
/// ```text
/// CREATE INDEX ON :S(id)
/// CREATE (:S {id: 42}) WITH 1 AS x MATCH (n:S {id: 42}) RETURN count(n)  -> 0
/// ```
#[test]
fn a_match_later_in_the_statement_sees_what_the_statement_created() {
    let (mut store, _rx) = GraphStore::with_async_indexing();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE INDEX ON :N(id)", &mut store, T)
        .unwrap();
    let out = engine
        .execute_mut(
            "CREATE (:N {id: 42}) WITH 1 AS x MATCH (n:N {id: 42}) RETURN count(n)",
            &mut store,
            T,
        )
        .unwrap();
    let count = out.records[0]
        .get("count(n)")
        .and_then(|v| v.as_property())
        .and_then(|p| p.as_integer())
        .expect("count(n) is an integer");
    assert_eq!(
        count, 1,
        "the indexed MATCH did not see the node its own statement created"
    );
}

/// Full-text is maintained on the write path too, and for a blunter reason than
/// MERGE's: on a store with a subscriber nothing else maintained it at all.
///
/// Measured on the v1.9.0 release binary before this change, persistent server:
///
/// ```text
/// CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)
/// CREATE (:Doc {body: 'quick brown fox jumps'})
/// CALL db.index.fulltext.queryNodes('docs', 'brown') YIELD node RETURN count(node)  -> 0
/// MATCH (d:Doc) RETURN count(d)                                                     -> 1
/// ```
///
/// The channel never carried full-text events, so the only maintainer was the
/// no-subscriber branch — tests. A search returned nothing and said nothing.
#[test]
fn the_fulltext_index_is_maintained_on_a_store_that_has_a_subscriber() {
    let (mut store, _rx) = GraphStore::with_async_indexing();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)", &mut store, T)
        .unwrap();
    engine
        .execute_mut("CREATE (:Doc {body: 'quick brown fox jumps'})", &mut store, T)
        .unwrap();
    let hits = engine
        .execute(
            "CALL db.index.fulltext.queryNodes('docs', 'brown') YIELD node RETURN count(node)",
            &store,
        )
        .unwrap();
    let n = hits.records[0]
        .get("count(node)")
        .and_then(|v| v.as_property())
        .and_then(|p| p.as_integer())
        .expect("count(node) is an integer");
    assert_eq!(n, 1, "the full-text index did not see a document that was written");
}

/// The vector index stays on the channel, and this is what says it still arrives.
///
/// The split this test guards: the write path maintains the indexes a statement
/// reads back (property, full-text); the HNSW index is maintained by the
/// background loop, because an HNSW insert is a graph descent with distance
/// computations at every level and nothing reads that index back mid-statement.
/// If someone moves it onto the write path again, the ingest measurement in the
/// PR body is the thing to re-run; if someone removes it from the loop, this
/// test fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_vector_written_through_the_channel_reaches_the_index() {
    use std::sync::Arc;
    use samyama::graph::PropertyValue;

    let (mut store, rx) = GraphStore::with_async_indexing();
    let vector_index = Arc::clone(&store.vector_index);
    let property_index = Arc::clone(&store.property_index);
    let tenants = Arc::new(samyama::persistence::TenantManager::new());
    vector_index
        .create_index("Doc", "embedding", 4, samyama::vector::DistanceMetric::Cosine)
        .expect("create vector index");
    {
        let (vi, pi, tm) = (Arc::clone(&vector_index), property_index, Arc::clone(&tenants));
        tokio::spawn(async move { GraphStore::start_background_indexer(rx, vi, pi, tm).await });
    }

    let id = store.create_node("Doc");
    store
        .set_node_property(
            T,
            id,
            "embedding".to_string(),
            PropertyValue::Array(vec![
                PropertyValue::Float(1.0),
                PropertyValue::Float(0.0),
                PropertyValue::Float(0.0),
                PropertyValue::Float(0.0),
            ]),
        )
        .expect("set embedding");

    // The loop is asynchronous by design; poll rather than sleep a fixed time.
    let mut hits = Vec::new();
    for _ in 0..100 {
        hits = vector_index
            .search("Doc", "embedding", &[1.0f32, 0.0, 0.0, 0.0], 1)
            .expect("search");
        if !hits.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(hits.len(), 1, "the vector never reached the index");
}
