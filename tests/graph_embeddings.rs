//! FastRP and node2vec graph embeddings, callable from Cypher (ML-06).
//!
//! ML-06 asks for "graph embeddings -- node2vec, FastRP, spectral; stored as
//! vector properties, refreshable, versioned" with an H1 target of node2vec
//! AND FastRP (spectral is out of scope here). Before this file, neither was
//! callable and neither appeared in the engine source: `randomWalk` is a
//! *step* of node2vec rather than the embedding, and `pca` reduces vectors
//! that already exist rather than producing them from graph structure.
//!
//! Both algorithms need write access -- like `algo.or.solve`, they store
//! their result on the node rather than only streaming it -- so every test
//! here goes through `QueryEngine::execute_mut`, and a read-only call is
//! itself something to check for (`algo.pca` and friends stream only;
//! writing an embedding and never persisting it would silently drop the
//! "stored as vector properties" half of ML-06).

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;

/// Two disjoint 5-cliques, `A0..A4` and `B0..B4`, no edges between them.
/// FastRP and node2vec both aggregate over graph structure, so a genuine
/// embedding should place same-clique nodes closer together than
/// different-clique nodes; a broken one (e.g. random noise with no
/// structure) would not reliably do that.
fn two_cliques() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let mut names = String::new();
    for prefix in ["A", "B"] {
        for i in 0..5 {
            if !names.is_empty() {
                names.push_str(", ");
            }
            names.push_str(&format!("({prefix}{i}:Node {{name: '{prefix}{i}'}})"));
        }
    }
    engine
        .execute_mut(&format!("CREATE {names}"), &mut store, "default")
        .unwrap();
    for prefix in ["A", "B"] {
        for i in 0..5 {
            for j in 0..5 {
                if i != j {
                    engine
                        .execute_mut(
                            &format!(
                                "MATCH (a:Node {{name: '{prefix}{i}'}}), (b:Node {{name: '{prefix}{j}'}}) \
                                 CREATE (a)-[:LINK]->(b)"
                            ),
                            &mut store,
                            "default",
                        )
                        .unwrap();
                }
            }
        }
    }
    (store, engine)
}

/// Reads through `store.node_property`, not `Node::get_property` -- the
/// column store is the source of truth for a property set after creation
/// (ADR-012 late materialization), and a `Node`'s own field can be stale or
/// empty for exactly the properties this test needs to check (`name`, set at
/// CREATE time, and `embedding`, set by the algorithm under test).
fn embedding_of(store: &GraphStore, name: &str) -> Vec<f64> {
    let id = store
        .get_nodes_by_label(&Label::new("Node"))
        .into_iter()
        .map(|n| n.id)
        .find(|&id| {
            store.node_property(id, "name") == Some(PropertyValue::String(name.to_string()))
        })
        .unwrap_or_else(|| panic!("node {name} not found"));
    match store.node_property(id, "embedding") {
        Some(PropertyValue::Array(items)) => items
            .iter()
            .map(|v| match v {
                PropertyValue::Float(f) => *f,
                other => panic!("embedding entry was not a float: {other:?}"),
            })
            .collect(),
        other => panic!("node {name} has no `embedding` array property, got {other:?}"),
    }
}

fn dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f64>()
        .sqrt()
}

// ---------- FastRP ----------

#[test]
fn fastrp_is_callable_and_writes_a_vector_property_of_the_requested_dimension() {
    let (mut store, engine) = two_cliques();
    let result = engine
        .execute_mut(
            "CALL algo.fastRP('Node', {embeddingDimension: 8, seed: 1}) YIELD node, embedding RETURN node, embedding",
            &mut store,
            "default",
        )
        .expect("algo.fastRP must be callable");
    assert_eq!(result.records.len(), 10, "one row per node");

    let a0 = embedding_of(&store, "A0");
    assert_eq!(
        a0.len(),
        8,
        "written property must have the requested dimension"
    );
}

#[test]
fn fastrp_write_property_is_configurable() {
    let (mut store, engine) = two_cliques();
    engine
        .execute_mut(
            "CALL algo.fastRP('Node', {embeddingDimension: 4, seed: 1, writeProperty: 'fastrpVec'}) \
             YIELD node, embedding RETURN node",
            &mut store,
            "default",
        )
        .unwrap();
    let id = store
        .get_nodes_by_label(&Label::new("Node"))
        .into_iter()
        .map(|n| n.id)
        .find(|&id| {
            store.node_property(id, "name") == Some(PropertyValue::String("A0".to_string()))
        })
        .unwrap();
    assert!(matches!(
        store.node_property(id, "fastrpVec"),
        Some(PropertyValue::Array(_))
    ));
    assert!(
        store.node_property(id, "embedding").is_none(),
        "must not also write the default name"
    );
}

#[test]
fn fastrp_same_seed_reproduces_different_seed_diverges() {
    let (mut store1, engine1) = two_cliques();
    engine1
        .execute_mut(
            "CALL algo.fastRP('Node', {embeddingDimension: 6, seed: 5}) YIELD node, embedding RETURN node",
            &mut store1,
            "default",
        )
        .unwrap();
    let (mut store2, engine2) = two_cliques();
    engine2
        .execute_mut(
            "CALL algo.fastRP('Node', {embeddingDimension: 6, seed: 5}) YIELD node, embedding RETURN node",
            &mut store2,
            "default",
        )
        .unwrap();
    assert_eq!(
        embedding_of(&store1, "A0"),
        embedding_of(&store2, "A0"),
        "same seed on the same graph must reproduce"
    );

    let (mut store3, engine3) = two_cliques();
    engine3
        .execute_mut(
            "CALL algo.fastRP('Node', {embeddingDimension: 6, seed: 6}) YIELD node, embedding RETURN node",
            &mut store3,
            "default",
        )
        .unwrap();
    assert_ne!(
        embedding_of(&store1, "A0"),
        embedding_of(&store3, "A0"),
        "a different seed must not reproduce the same embedding"
    );
}

#[test]
fn fastrp_places_same_clique_nodes_closer_than_different_cliques() {
    let (mut store, engine) = two_cliques();
    engine
        .execute_mut(
            "CALL algo.fastRP('Node', {embeddingDimension: 16, seed: 42}) YIELD node, embedding RETURN node",
            &mut store,
            "default",
        )
        .unwrap();
    let within = dist(&embedding_of(&store, "A0"), &embedding_of(&store, "A1"));
    let across = dist(&embedding_of(&store, "A0"), &embedding_of(&store, "B0"));
    assert!(
        within < across,
        "within-clique {within} should be less than across-clique {across}"
    );
}

#[test]
fn fastrp_is_refused_on_the_read_only_path() {
    let (store, _engine) = two_cliques();
    let err = samyama::query::executor::QueryExecutor::new(&store)
        .execute(&samyama::query::parser::parse_query(
            "CALL algo.fastRP('Node', {embeddingDimension: 4}) YIELD node, embedding RETURN node",
        ).unwrap())
        .expect_err("fastRP writes and must be refused without write access");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("write"),
        "message should say why: {msg}"
    );
}

#[test]
fn fastrp_rejects_an_unknown_config_key() {
    let (mut store, engine) = two_cliques();
    let err = engine
        .execute_mut(
            "CALL algo.fastRP('Node', {embeddingDimension: 4, mutateProperty: 'x'}) YIELD node, embedding RETURN node",
            &mut store,
            "default",
        )
        .expect_err("an unread config key must be refused, not silently ignored");
    assert!(err.to_string().contains("mutateProperty"));
}

// ---------- node2vec ----------

#[test]
fn node2vec_is_callable_and_writes_a_vector_property_of_the_requested_dimension() {
    let (mut store, engine) = two_cliques();
    let result = engine
        .execute_mut(
            "CALL algo.node2vec('Node', {embeddingDimension: 8, walksPerNode: 4, walkLength: 6, seed: 1}) \
             YIELD node, embedding RETURN node, embedding",
            &mut store,
            "default",
        )
        .expect("algo.node2vec must be callable");
    assert_eq!(result.records.len(), 10);
    assert_eq!(embedding_of(&store, "A0").len(), 8);
}

#[test]
fn node2vec_same_seed_reproduces_different_seed_diverges() {
    let (mut store1, engine1) = two_cliques();
    engine1
        .execute_mut(
            "CALL algo.node2vec('Node', {embeddingDimension: 6, walksPerNode: 4, walkLength: 6, seed: 5}) \
             YIELD node, embedding RETURN node",
            &mut store1,
            "default",
        )
        .unwrap();
    let (mut store2, engine2) = two_cliques();
    engine2
        .execute_mut(
            "CALL algo.node2vec('Node', {embeddingDimension: 6, walksPerNode: 4, walkLength: 6, seed: 5}) \
             YIELD node, embedding RETURN node",
            &mut store2,
            "default",
        )
        .unwrap();
    assert_eq!(embedding_of(&store1, "A0"), embedding_of(&store2, "A0"));

    let (mut store3, engine3) = two_cliques();
    engine3
        .execute_mut(
            "CALL algo.node2vec('Node', {embeddingDimension: 6, walksPerNode: 4, walkLength: 6, seed: 6}) \
             YIELD node, embedding RETURN node",
            &mut store3,
            "default",
        )
        .unwrap();
    assert_ne!(embedding_of(&store1, "A0"), embedding_of(&store3, "A0"));
}

#[test]
fn node2vec_places_same_clique_nodes_closer_than_different_cliques() {
    let (mut store, engine) = two_cliques();
    engine
        .execute_mut(
            "CALL algo.node2vec('Node', {embeddingDimension: 16, walksPerNode: 8, walkLength: 10, \
             windowSize: 4, seed: 42}) YIELD node, embedding RETURN node",
            &mut store,
            "default",
        )
        .unwrap();
    let within = dist(&embedding_of(&store, "A0"), &embedding_of(&store, "A1"));
    let across = dist(&embedding_of(&store, "A0"), &embedding_of(&store, "B0"));
    assert!(
        within < across,
        "within-clique {within} should be less than across-clique {across}"
    );
}

#[test]
fn node2vec_is_refused_on_the_read_only_path() {
    let (store, _engine) = two_cliques();
    let err = samyama::query::executor::QueryExecutor::new(&store)
        .execute(&samyama::query::parser::parse_query(
            "CALL algo.node2vec('Node', {embeddingDimension: 4}) YIELD node, embedding RETURN node",
        ).unwrap())
        .expect_err("node2vec writes and must be refused without write access");
    assert!(err.to_string().to_lowercase().contains("write"));
}

#[test]
fn node2vec_accepts_p_and_q() {
    let (mut store, engine) = two_cliques();
    engine
        .execute_mut(
            "CALL algo.node2vec('Node', {embeddingDimension: 4, returnFactor: 0.5, inOutFactor: 2.0, \
             walksPerNode: 3, walkLength: 5, seed: 1}) YIELD node, embedding RETURN node",
            &mut store,
            "default",
        )
        .expect("p/q config keys must be accepted");
}

#[test]
fn node2vec_rejects_an_unknown_config_key() {
    let (mut store, engine) = two_cliques();
    let err = engine
        .execute_mut(
            "CALL algo.node2vec('Node', {embeddingDimension: 4, writeProperty2: 'x'}) YIELD node, embedding RETURN node",
            &mut store,
            "default",
        )
        .expect_err("an unread config key must be refused, not silently ignored");
    assert!(err.to_string().contains("writeProperty2"));
}

// ---------- routing sanity ----------

#[test]
fn both_names_are_case_and_namespace_insensitive() {
    let (mut store, engine) = two_cliques();
    for q in [
        "CALL fastRP('Node', {embeddingDimension: 3}) YIELD node RETURN node",
        "CALL algo.fastrp('Node', {embeddingDimension: 3}) YIELD node RETURN node",
        "CALL samyama.fastRP('Node', {embeddingDimension: 3}) YIELD node RETURN node",
        "CALL node2vec('Node', {embeddingDimension: 3, walksPerNode: 2, walkLength: 4}) YIELD node RETURN node",
        "CALL algo.NODE2VEC('Node', {embeddingDimension: 3, walksPerNode: 2, walkLength: 4}) YIELD node RETURN node",
    ] {
        engine.execute_mut(q, &mut store, "default").unwrap_or_else(|e| panic!("{q} failed: {e}"));
    }
}
