//! The SDK's `pca` reads node properties column-first (samyama-graph#1022).
//!
//! `EmbeddedClient::pca` built its feature matrix with `node.get_property`,
//! which reads a node's row copy only. A node restored from a snapshot keeps
//! its properties in columns and has no row copy, so on an imported graph every
//! feature was 0.0 and PCA explained no variance at all -- a result, not an
//! error.

use std::sync::Arc;

use samyama::algo::PcaConfig;
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama_sdk::algo::AlgorithmClient;
use samyama_sdk::EmbeddedClient;
use tokio::sync::RwLock;

fn features(i: usize) -> [i64; 2] {
    [i as i64, 3 * i as i64 + (i % 4) as i64]
}

fn row_store() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..20 {
        let n = store.create_node("P");
        for (k, v) in ["a", "b"].iter().zip(features(i)) {
            store.set_node_property("default", n, *k, v).unwrap();
        }
    }
    store
}

/// The same values held only in columns, as a snapshot import leaves them.
fn column_store() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..20 {
        let n = store.create_node_stub(Label::new("P"));
        for (k, v) in ["a", "b"].iter().zip(features(i)) {
            store.set_column_property(n, k, PropertyValue::Integer(v));
        }
    }
    store
}

#[tokio::test]
async fn pca_over_column_only_nodes_matches_pca_over_rows() {
    let rows = EmbeddedClient::with_store(Arc::new(RwLock::new(row_store())));
    let cols = EmbeddedClient::with_store(Arc::new(RwLock::new(column_store())));
    let a = rows.pca(Some("P"), &["a", "b"], PcaConfig::default()).await;
    let b = cols.pca(Some("P"), &["a", "b"], PcaConfig::default()).await;

    assert_eq!(b.n_samples, 20);
    assert!(
        b.explained_variance.iter().any(|v| *v > 1e-6),
        "column-only features read as zeros: explained variance {:?}",
        b.explained_variance
    );
    assert_eq!(a.explained_variance.len(), b.explained_variance.len());
    for (x, y) in a.explained_variance.iter().zip(&b.explained_variance) {
        assert!((x - y).abs() < 1e-9, "explained variance differs: {:?} vs {:?}", a.explained_variance, b.explained_variance);
    }
    for (x, y) in a.mean.iter().zip(&b.mean) {
        assert!((x - y).abs() < 1e-9, "feature means differ: {:?} vs {:?}", a.mean, b.mean);
    }
}
