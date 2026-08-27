//! An entity inside a list or map must arrive as an entity, not as Rust `Debug`.
//!
//! `record_batch_to_query_result` converts each returned value to
//! `serde_json::Value`, and gives `Value::Node` and `Value::Edge` a proper
//! shape — `{id, labels, properties}`. For `Value::List` and `Value::Map` it
//! instead does `format!("{v:?}")` on every element, so an entity *inside* a
//! collection comes back as the Debug rendering of the internal enum:
//!
//! ```text
//! collect(n)        -> ["NodeRef(NodeId(1))", "NodeRef(NodeId(2))"]
//! relationships(p)  -> ["EdgeRef(EdgeId(1), NodeId(1), NodeId(2), EdgeType(\"R\"))"]
//! ```
//!
//! Labels and properties are gone, and the escaped quote is embedded in a JSON
//! string. Scalars are unaffected, which is what makes this easy to miss:
//! `collect(n.name)` is correct, `collect(n)` is not — and `collect`, `nodes`
//! and `relationships` are everyday Cypher.

use samyama_sdk::{EmbeddedClient, SamyamaClient};

async fn one_value(setup: &[&str], cypher: &str) -> serde_json::Value {
    let client = EmbeddedClient::new();
    for s in setup {
        client.query("default", s).await.expect("setup should run");
    }
    let result = client.query("default", cypher).await.expect("query should run");
    let row = result.records.first().expect("one row").clone();
    row.into_iter().next().expect("one column")
}

const TWO_NODES: &[&str] = &["CREATE (:P {n: 1})-[:R {w: 2}]->(:P {n: 2})"];

#[tokio::test]
async fn collected_nodes_keep_their_labels_and_properties() {
    let v = one_value(TWO_NODES, "MATCH (n:P) RETURN collect(n) AS xs").await;
    let items = v.as_array().expect("a JSON array");
    assert_eq!(items.len(), 2);
    for item in items {
        assert!(
            item.is_object(),
            "a collected node must be an object with id/labels/properties, got {item}"
        );
        assert_eq!(item["labels"], serde_json::json!(["P"]), "labels lost: {item}");
        assert!(item["properties"]["n"].is_number(), "properties lost: {item}");
    }
}

#[tokio::test]
async fn nodes_of_a_path_are_entities() {
    let v = one_value(TWO_NODES, "MATCH p=(:P)-[:R]->(:P) RETURN nodes(p) AS xs").await;
    let items = v.as_array().expect("a JSON array");
    assert_eq!(items.len(), 2);
    assert!(items.iter().all(|i| i.is_object()), "got {v}");
}

#[tokio::test]
async fn relationships_of_a_path_are_entities() {
    let v = one_value(TWO_NODES, "MATCH p=(:P)-[:R]->(:P) RETURN relationships(p) AS xs").await;
    let items = v.as_array().expect("a JSON array");
    assert_eq!(items.len(), 1);
    assert!(items[0].is_object(), "a relationship must not be a Debug string: {v}");
    assert_eq!(items[0]["type"], serde_json::json!("R"), "edge type lost: {v}");
}

#[tokio::test]
async fn a_map_value_that_is_a_node_is_an_entity() {
    let v = one_value(TWO_NODES, "MATCH (n:P {n: 1}) RETURN {v: n} AS m").await;
    assert!(v["v"].is_object(), "a node in a map must not be a Debug string: {v}");
}

/// The half that already worked, pinned so a fix does not break it.
#[tokio::test]
async fn collected_scalars_are_still_plain_values() {
    let v = one_value(TWO_NODES, "MATCH (n:P) RETURN collect(n.n) AS xs").await;
    assert_eq!(v, serde_json::json!([1, 2]));
}

#[tokio::test]
async fn a_literal_list_is_still_plain_values() {
    let v = one_value(&[], "RETURN [1, 2, 3] AS xs").await;
    assert_eq!(v, serde_json::json!([1, 2, 3]));
}
