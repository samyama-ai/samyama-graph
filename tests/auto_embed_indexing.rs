//! Auto-embed must put its vectors in the index a user actually queries (#310).
//!
//! The pipeline generated embeddings correctly — the reporter's Ollama received thousands
//! of requests — but indexed each one under the *source* text property (`Person.headline`).
//! Users create their vector index on the property the embedding is called
//! (`Person.embedding`), so every vector went into an index nobody queried, and
//! `add_vector` returned `Ok(())` for a missing index, so nothing ever said so.
//!
//! Uses the `Mock` provider, so no network and no model are involved.

use std::collections::HashMap;
use std::sync::Arc;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::index::manager::IndexManager;
use samyama::persistence::tenant::{AutoEmbedConfig, LLMProvider};
use samyama::persistence::TenantManager;
use samyama::vector::{DistanceMetric, VectorIndexManager};

/// The Mock provider emits 64-dimensional vectors.
const MOCK_DIM: usize = 64;

fn embed_config(source_property: &str) -> AutoEmbedConfig {
    AutoEmbedConfig {
        provider: LLMProvider::Mock,
        embedding_model: "mock".to_string(),
        api_key: None,
        api_base_url: None,
        chunk_size: 512,
        chunk_overlap: 0,
        vector_dimension: MOCK_DIM,
        embedding_policies: HashMap::from([(
            "Person".to_string(),
            vec![source_property.to_string()],
        )]),
        embedding_property: "embedding".to_string(),
    }
}

#[tokio::test]
async fn auto_embed_indexes_under_the_target_property() {
    let tenants = Arc::new(TenantManager::new());
    tenants
        .update_embed_config("default", Some(embed_config("headline")))
        .expect("set embed config");

    let vector_index = Arc::new(VectorIndexManager::new());
    let property_index = Arc::new(IndexManager::new());
    // the index a user would create: on the embedding property, not the text it came from
    vector_index
        .create_index("Person", "embedding", MOCK_DIM, DistanceMetric::Cosine)
        .expect("create index");

    let (mut store, rx) = GraphStore::with_async_indexing();
    let (vi, pi, tm) = (
        Arc::clone(&vector_index),
        Arc::clone(&property_index),
        Arc::clone(&tenants),
    );
    tokio::spawn(async move { GraphStore::start_background_indexer(rx, vi, pi, tm).await });

    let id = store.create_node("Person");
    store
        .set_node_property(
            "default",
            id,
            "headline".to_string(),
            PropertyValue::String("a graph database engineer".to_string()),
        )
        .expect("set property");

    // auto-embed runs on a spawned task
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    let hits = vector_index
        .search("Person", "embedding", &vec![0.1f32; MOCK_DIM], 5)
        .expect("search");
    assert_eq!(
        hits.len(),
        1,
        "the generated embedding should be findable in the index that was created for it"
    );

    // and it must not have gone into an index keyed by the source text property
    let source_hits = vector_index
        .search("Person", "headline", &vec![0.1f32; MOCK_DIM], 5)
        .expect("search");
    assert!(source_hits.is_empty(), "nothing should be indexed under the source property");
}

#[tokio::test]
async fn a_property_outside_the_policy_is_not_embedded() {
    // The policy names which properties are embedding sources; anything else must be left
    // alone, or every string write would hit the provider.
    let tenants = Arc::new(TenantManager::new());
    tenants
        .update_embed_config("default", Some(embed_config("headline")))
        .expect("set embed config");

    let vector_index = Arc::new(VectorIndexManager::new());
    vector_index
        .create_index("Person", "embedding", MOCK_DIM, DistanceMetric::Cosine)
        .expect("create index");

    let (mut store, rx) = GraphStore::with_async_indexing();
    let (vi, pi, tm) = (
        Arc::clone(&vector_index),
        Arc::new(IndexManager::new()),
        Arc::clone(&tenants),
    );
    tokio::spawn(async move { GraphStore::start_background_indexer(rx, vi, pi, tm).await });

    let id = store.create_node("Person");
    store
        .set_node_property(
            "default",
            id,
            "nickname".to_string(), // not in the policy
            PropertyValue::String("nobody".to_string()),
        )
        .expect("set property");

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    let hits = vector_index
        .search("Person", "embedding", &vec![0.1f32; MOCK_DIM], 5)
        .expect("search");
    assert!(hits.is_empty(), "only policy properties should be embedded");
}

/// The vector has to survive a rebuild, and only a stored property does (#310).
///
/// `rebuild_vector_index` rebuilds every index from node properties. It runs on snapshot
/// import, on reindex, and implicitly on every restart. An auto-embedded vector that
/// lived only in the in-memory index was therefore dropped by all three, and vector
/// search went back to answering zero rows with a success — the same silence the
/// wrong-index bug produced, reached from the other side.
///
/// Written against the shared-handle wiring the server uses, because that is the only
/// wiring in which auto-embed can reach the store to write the property at all.
#[tokio::test]
async fn an_auto_embedded_vector_survives_a_rebuild() {
    let tenants = Arc::new(TenantManager::new());
    tenants
        .update_embed_config("default", Some(embed_config("headline")))
        .expect("set embed config");

    let (store, rx) = GraphStore::with_async_indexing();
    // the store's own index, not a side one: a rebuild only touches the store's.
    let vector_index = Arc::clone(&store.vector_index);
    vector_index
        .create_index("Person", "embedding", MOCK_DIM, DistanceMetric::Cosine)
        .expect("create index");

    let store = Arc::new(tokio::sync::RwLock::new(store));
    let (vi, pi, tm, sh) = (
        Arc::clone(&vector_index),
        Arc::new(IndexManager::new()),
        Arc::clone(&tenants),
        Arc::clone(&store),
    );
    tokio::spawn(async move {
        GraphStore::start_background_indexer_with_store(rx, vi, pi, tm, Some(sh)).await
    });

    let id = {
        let mut guard = store.write().await;
        let id = guard.create_node("Person");
        guard
            .set_node_property(
                "default",
                id,
                "headline".to_string(),
                PropertyValue::String("a graph database engineer".to_string()),
            )
            .expect("set property");
        id
    };

    // auto-embed runs on a spawned task, and the write-back is a second hop through it
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    let before = vector_index
        .search("Person", "embedding", &vec![0.1f32; MOCK_DIM], 5)
        .expect("search");
    assert_eq!(before.len(), 1, "the embedding should be indexed to begin with");

    // the property is what a rebuild reads, so it has to be on the node
    {
        let guard = store.read().await;
        let stored = guard
            .get_node(id)
            .and_then(|n| n.get_property("embedding").cloned());
        assert!(
            stored.is_some_and(|v| v.to_vector().is_some_and(|v| v.len() == MOCK_DIM)),
            "the embedding should be stored on the node, not only in the index"
        );
    }

    {
        let mut guard = store.write().await;
        guard.rebuild_vector_index();
    }

    let after = vector_index
        .search("Person", "embedding", &vec![0.1f32; MOCK_DIM], 5)
        .expect("search");
    assert_eq!(
        after.len(),
        1,
        "a rebuild must not lose an auto-embedded vector — it is the restart path"
    );
}
