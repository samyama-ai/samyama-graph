//! The background indexer does not need a `PersistenceManager` (#1469).
//!
//! `main.rs` arms `index_sender` for every server, but started the consumer
//! only under persistence. On `--ephemeral` every index event went to a channel
//! with nobody on the other end: the HNSW insert never happened, auto-embed and
//! the agentic trigger never fired, and the events accumulated for the life of
//! the process.
//!
//! The symptom was the bad kind. `/api/vector-search` answered `200` with no
//! rows for vectors that were in the graph — "nothing is similar", not "there
//! is no index". A label scan found the nodes; the vector index had never heard
//! of them.
//!
//! What this test establishes is the claim the fix rests on: the indexer's
//! dependencies are the store's two index handles and a `TenantManager`, none
//! of which involve persistence. `PersistenceManager::start_indexer` is a thin
//! wrapper that supplies its own manager.
//!
//! It does not cover `main.rs`'s branch — that is wiring, and its evidence is a
//! four-arm run against both binaries:
//!
//!     old  persistent  vector=1     old  ephemeral  vector=0
//!     new  persistent  vector=1     new  ephemeral  vector=1

use std::sync::Arc;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::persistence::TenantManager;
use samyama::vector::DistanceMetric;

#[tokio::test]
async fn a_vector_written_with_no_persistence_reaches_the_index() {
    let (mut graph, rx) = GraphStore::with_async_indexing();
    graph
        .vector_index
        .create_index("Doc", "embedding", 3, DistanceMetric::L2)
        .expect("index creation");

    let store = Arc::new(tokio::sync::RwLock::new(graph));
    let tenants = Arc::new(TenantManager::new());

    // Exactly what `main.rs` now does when `persistence` is `None`.
    let (vector_index, property_index) = {
        let g = store.read().await;
        (Arc::clone(&g.vector_index), Arc::clone(&g.property_index))
    };
    let spawned = Arc::clone(&store);
    tokio::spawn(async move {
        GraphStore::start_background_indexer_with_store(
            rx,
            vector_index,
            property_index,
            tenants,
            Some(spawned),
        )
        .await;
    });

    {
        let mut g = store.write().await;
        let id = g.create_node("Doc");
        // `set_node_property`, not `get_node_mut(..).set_property(..)`. The
        // latter hands out a raw `&mut Node` and emits no `IndexEvent`, so a
        // test written that way exercises a path no server takes and fails
        // whether or not the indexer is running — which is the same mistake
        // that let #1467 and #1469 ship.
        g.set_node_property(
            "default",
            id,
            "embedding",
            PropertyValue::Array(vec![
                PropertyValue::Float(1.0),
                PropertyValue::Float(0.0),
                PropertyValue::Float(0.0),
            ]),
        )
        .expect("set_node_property");
    }

    // The indexer is asynchronous by design — poll rather than sleep a fixed
    // amount, so the test is not a race dressed as a duration.
    let mut hits = 0;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let g = store.read().await;
        if let Ok(found) = g.vector_index.search("Doc", "embedding", &[1.0, 0.0, 0.0], 5) {
            hits = found.len();
            if hits > 0 {
                break;
            }
        }
    }

    assert!(
        hits > 0,
        "a vector written to a store with no PersistenceManager never reached the \
         index: this is #1469, and it is why --ephemeral answered 200 with no rows"
    );
}
