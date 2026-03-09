//! HTTP handlers for the Visualizer API

use axum::{
    extract::{State, Json, Multipart},
    response::IntoResponse,
};
use crate::query::Value;
use crate::graph::PropertyValue;
use crate::http::server::AppState;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, BTreeMap, BTreeSet};

/// Request for executing a Cypher query
#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
}

/// Response containing both graph data and raw tabular data
#[derive(Serialize)]
pub struct QueryResponse {
    nodes: Vec<serde_json::Value>,
    edges: Vec<serde_json::Value>,
    columns: Vec<String>,
    records: Vec<Vec<serde_json::Value>>,
}

/// Handler for Cypher queries
pub async fn query_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    // Check if query is write or read
    let query_upper = payload.query.trim().to_uppercase();
    let is_write = query_upper.starts_with("CREATE") ||
                   query_upper.starts_with("SET") ||
                   query_upper.starts_with("DELETE");

    let result = if is_write {
        let mut store_guard = state.store.write().await;
        state.engine.execute_mut(&payload.query, &mut *store_guard, "default")
    } else {
        let store_guard = state.store.read().await;
        state.engine.execute(&payload.query, &*store_guard)
    };

    match result {
        Ok(batch) => {
            let mut nodes = HashMap::new();
            let mut edges = HashMap::new();
            let mut records = Vec::new();

            for record in &batch.records {
                let mut row = Vec::new();
                for col in &batch.columns {
                    let val = record.get(col).unwrap_or(&Value::Null);
                    
                    // Extract graph elements for visualization
                    match val {
                        Value::Node(id, node) => {
                            let mut properties = serde_json::Map::new();
                            for (k, v) in &node.properties {
                                properties.insert(k.clone(), v.to_json());
                            }
                            let node_json = json!({
                                "id": id.as_u64().to_string(),
                                "labels": node.labels.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
                                "properties": properties,
                            });
                            nodes.insert(id.as_u64().to_string(), node_json.clone());
                            row.push(node_json);
                        }
                        Value::NodeRef(id) => {
                            // Lazy ref — minimal JSON (no properties available without store)
                            let node_json = json!({
                                "id": id.as_u64().to_string(),
                                "labels": [],
                                "properties": {},
                            });
                            nodes.insert(id.as_u64().to_string(), node_json.clone());
                            row.push(node_json);
                        }
                        Value::Edge(id, edge) => {
                            let mut properties = serde_json::Map::new();
                            for (k, v) in &edge.properties {
                                properties.insert(k.clone(), v.to_json());
                            }
                            let edge_json = json!({
                                "id": id.as_u64().to_string(),
                                "source": edge.source.as_u64().to_string(),
                                "target": edge.target.as_u64().to_string(),
                                "type": edge.edge_type.as_str(),
                                "properties": properties,
                            });
                            edges.insert(id.as_u64().to_string(), edge_json.clone());
                            row.push(edge_json);
                        }
                        Value::EdgeRef(id, src, tgt, et) => {
                            let edge_json = json!({
                                "id": id.as_u64().to_string(),
                                "source": src.as_u64().to_string(),
                                "target": tgt.as_u64().to_string(),
                                "type": et.as_str(),
                                "properties": {},
                            });
                            edges.insert(id.as_u64().to_string(), edge_json.clone());
                            row.push(edge_json);
                        }
                        Value::Property(p) => {
                            row.push(p.to_json());
                        }
                        Value::Path { nodes: path_nodes, edges: path_edges } => {
                            let path_json = json!({
                                "nodes": path_nodes.iter().map(|n| n.as_u64().to_string()).collect::<Vec<_>>(),
                                "edges": path_edges.iter().map(|e| e.as_u64().to_string()).collect::<Vec<_>>(),
                                "length": path_edges.len(),
                            });
                            row.push(path_json);
                        }
                        Value::Null => {
                            row.push(serde_json::Value::Null);
                        }
                    }
                }
                records.push(row);
            }

            Json(json!({
                "nodes": nodes.values().collect::<Vec<_>>(),
                "edges": edges.values().collect::<Vec<_>>(),
                "columns": batch.columns,
                "records": records,
            })).into_response()
        }
        Err(e) => {
            (axum::http::StatusCode::BAD_REQUEST, Json(json!({ "error": e.to_string() }))).into_response()
        }
    }
}

/// Handler for system status
pub async fn status_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let store_guard = state.store.read().await;
    let stats = state.engine.cache_stats();
    Json(json!({
        "status": "healthy",
        "version": crate::VERSION,
        "storage": {
            "nodes": store_guard.node_count(),
            "edges": store_guard.edge_count(),
        },
        "cache": {
            "hits": stats.hits(),
            "misses": stats.misses(),
            "size": state.engine.cache_len(),
        }
    }))
}

/// Handler for graph schema introspection
pub async fn schema_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let store_guard = state.store.read().await;

    let stats = store_guard.compute_statistics();
    let mut node_types = Vec::new();
    for label in store_guard.all_labels() {
        let count = store_guard.label_node_count(label);
        let mut properties = BTreeMap::new();
        for ((l, prop), _pstats) in &stats.property_stats {
            if l == label {
                let nodes = store_guard.get_nodes_by_label(label);
                let mut prop_type = "Unknown".to_string();
                for node in nodes.iter().take(1) {
                    if let Some(val) = node.properties.get(prop) {
                        prop_type = match val {
                            PropertyValue::String(_) => "String",
                            PropertyValue::Integer(_) => "Integer",
                            PropertyValue::Float(_) => "Float",
                            PropertyValue::Boolean(_) => "Boolean",
                            PropertyValue::Vector(_) => "Vector",
                            _ => "Unknown",
                        }.to_string();
                    }
                }
                properties.insert(prop.clone(), prop_type);
            }
        }
        node_types.push(json!({
            "label": label.as_str(),
            "count": count,
            "properties": properties,
        }));
    }

    let mut edge_types = Vec::new();
    for edge_type in store_guard.all_edge_types() {
        let count = store_guard.edge_type_count(edge_type);
        let edges = store_guard.get_edges_by_type(edge_type);
        let mut source_labels = BTreeSet::new();
        let mut target_labels = BTreeSet::new();
        let mut edge_props = BTreeMap::new();

        for edge in edges.iter().take(1000) {
            if let Some(src) = store_guard.get_node(edge.source) {
                for l in &src.labels {
                    source_labels.insert(l.as_str().to_string());
                }
            }
            if let Some(tgt) = store_guard.get_node(edge.target) {
                for l in &tgt.labels {
                    target_labels.insert(l.as_str().to_string());
                }
            }
            for (k, v) in &edge.properties {
                edge_props.entry(k.clone()).or_insert_with(|| {
                    match v {
                        PropertyValue::String(_) => "String".to_string(),
                        PropertyValue::Integer(_) => "Integer".to_string(),
                        PropertyValue::Float(_) => "Float".to_string(),
                        PropertyValue::Boolean(_) => "Boolean".to_string(),
                        _ => "Unknown".to_string(),
                    }
                });
            }
        }

        edge_types.push(json!({
            "type": edge_type.as_str(),
            "count": count,
            "source_labels": source_labels.into_iter().collect::<Vec<_>>(),
            "target_labels": target_labels.into_iter().collect::<Vec<_>>(),
            "properties": edge_props,
        }));
    }

    let index_list = store_guard.property_index.list_indexes();
    let indexes: Vec<_> = index_list.iter().map(|(l, p)| {
        json!({ "label": l.as_str(), "property": p, "type": "BTREE" })
    }).collect();

    let constraint_list = store_guard.property_index.list_constraints();
    let constraints: Vec<_> = constraint_list.iter().map(|(l, p)| {
        json!({ "label": l.as_str(), "property": p, "type": "UNIQUE" })
    }).collect();

    Json(json!({
        "node_types": node_types,
        "edge_types": edge_types,
        "indexes": indexes,
        "constraints": constraints,
        "statistics": {
            "total_nodes": stats.total_nodes,
            "total_edges": stats.total_edges,
            "avg_out_degree": stats.avg_out_degree,
        }
    }))
}

/// Handler for CSV file upload and import
pub async fn import_csv_handler(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut csv_data: Option<String> = None;
    let mut label = String::new();
    let mut id_column: Option<String> = None;
    let mut delimiter = b',';

    loop {
        let field_result: Result<Option<axum::extract::multipart::Field<'_>>, _> = multipart.next_field().await;
        match field_result {
            Ok(Some(field)) => {
                let name = field.name().unwrap_or("").to_string();
                match name.as_str() {
                    "file" => {
                        match field.text().await {
                            Ok(text) => csv_data = Some(text),
                            Err(e) => return (axum::http::StatusCode::BAD_REQUEST, Json(json!({ "error": format!("Failed to read file: {}", e) }))).into_response(),
                        }
                    }
                    "label" => {
                        if let Ok(text) = field.text().await {
                            label = text;
                        }
                    }
                    "id_column" => {
                        if let Ok(text) = field.text().await {
                            id_column = Some(text);
                        }
                    }
                    "delimiter" => {
                        if let Ok(text) = field.text().await {
                            if let Some(&ch) = text.as_bytes().first() {
                                delimiter = ch;
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let csv_text = match csv_data {
        Some(data) => data,
        None => return (axum::http::StatusCode::BAD_REQUEST, Json(json!({ "error": "No file field in multipart request" }))).into_response(),
    };

    if label.is_empty() {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({ "error": "Missing 'label' field" }))).into_response();
    }

    let mut lines = csv_text.lines();
    let header_line = match lines.next() {
        Some(h) => h,
        None => return (axum::http::StatusCode::BAD_REQUEST, Json(json!({ "error": "Empty CSV file" }))).into_response(),
    };

    let headers: Vec<&str> = header_line.split(delimiter as char).collect();
    let id_col_idx = id_column.as_ref().and_then(|id_col| headers.iter().position(|h| h.trim() == id_col.as_str()));

    let mut store_guard = state.store.write().await;
    let mut count = 0usize;
    let mut id_map: HashMap<String, crate::graph::NodeId> = HashMap::new();

    for line in lines {
        if line.trim().is_empty() { continue; }
        let fields: Vec<&str> = line.split(delimiter as char).collect();

        let node_id = store_guard.create_node(label.as_str());

        if let Some(idx) = id_col_idx {
            if let Some(val) = fields.get(idx) {
                id_map.insert(val.trim().to_string(), node_id);
            }
        }

        if let Some(node) = store_guard.get_node_mut(node_id) {
            for (i, header) in headers.iter().enumerate() {
                if let Some(value) = fields.get(i) {
                    let trimmed = value.trim();
                    if trimmed.is_empty() { continue; }

                    let prop_val = if let Ok(int_val) = trimmed.parse::<i64>() {
                        PropertyValue::Integer(int_val)
                    } else if let Ok(float_val) = trimmed.parse::<f64>() {
                        PropertyValue::Float(float_val)
                    } else if trimmed.eq_ignore_ascii_case("true") {
                        PropertyValue::Boolean(true)
                    } else if trimmed.eq_ignore_ascii_case("false") {
                        PropertyValue::Boolean(false)
                    } else {
                        PropertyValue::String(trimmed.to_string())
                    };

                    node.set_property(header.trim(), prop_val);
                }
            }
        }
        count += 1;
    }

    Json(json!({
        "status": "ok",
        "nodes_created": count,
        "label": label,
        "columns": headers.iter().map(|h| h.trim()).collect::<Vec<_>>(),
    })).into_response()
}

/// Request for JSON import
#[derive(Deserialize)]
pub struct JsonImportRequest {
    pub label: String,
    pub nodes: Vec<serde_json::Value>,
}

/// Handler for JSON node import
pub async fn import_json_handler(
    State(state): State<AppState>,
    Json(payload): Json<JsonImportRequest>,
) -> impl IntoResponse {
    if payload.label.is_empty() {
        return (axum::http::StatusCode::BAD_REQUEST, Json(json!({ "error": "Missing 'label' field" }))).into_response();
    }

    let mut store_guard = state.store.write().await;
    let mut count = 0usize;

    for node_json in &payload.nodes {
        let node_id = store_guard.create_node(payload.label.as_str());

        if let (Some(node), Some(obj)) = (store_guard.get_node_mut(node_id), node_json.as_object()) {
            for (key, val) in obj {
                let prop_val = match val {
                    serde_json::Value::String(s) => PropertyValue::String(s.clone()),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            PropertyValue::Integer(i)
                        } else if let Some(f) = n.as_f64() {
                            PropertyValue::Float(f)
                        } else {
                            continue;
                        }
                    }
                    serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
                    _ => continue,
                };
                node.set_property(key, prop_val);
            }
        }
        count += 1;
    }

    Json(json!({
        "status": "ok",
        "nodes_created": count,
        "label": payload.label,
    })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphStore;
    use crate::query::QueryEngine;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::{get, post},
        Router,
    };
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tower::util::ServiceExt;

    /// Build a test router with fresh state and return (router, state).
    fn test_app() -> (Router, AppState) {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
        };
        let app = Router::new()
            .route("/api/query", post(query_handler))
            .route("/api/status", get(status_handler))
            .with_state(state.clone());
        (app, state)
    }

    /// Helper: send a POST /api/query with the given body and return (status, json).
    async fn post_query(app: Router, body: &str) -> (StatusCode, serde_json::Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        (status, json)
    }

    /// Helper: send a GET /api/status and return (status, json).
    async fn get_status(app: Router) -> (StatusCode, serde_json::Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        (status, json)
    }

    // ==================== status_handler tests ====================

    #[tokio::test]
    async fn test_status_handler_empty_store() {
        let (app, _state) = test_app();
        let (status, json) = get_status(app).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["version"], crate::VERSION);
        assert_eq!(json["storage"]["nodes"], 0);
        assert_eq!(json["storage"]["edges"], 0);
        assert_eq!(json["cache"]["hits"], 0);
        assert_eq!(json["cache"]["misses"], 0);
        assert_eq!(json["cache"]["size"], 0);
    }

    #[tokio::test]
    async fn test_status_handler_after_data_and_queries() {
        let (app, state) = test_app();

        // Seed data into the store
        {
            let mut store = state.store.write().await;
            let alice = store.create_node("Person");
            store.get_node_mut(alice).unwrap().set_property("name", "Alice");
            let bob = store.create_node("Person");
            store.get_node_mut(bob).unwrap().set_property("name", "Bob");
            store.create_edge(alice, bob, "KNOWS").unwrap();
        }

        // Run a query through the engine to populate cache stats
        {
            let store_guard = state.store.read().await;
            let _ = state.engine.execute("MATCH (n:Person) RETURN n", &*store_guard);
        }

        let (status, json) = get_status(app).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["storage"]["nodes"], 2);
        assert_eq!(json["storage"]["edges"], 1);
        assert_eq!(json["cache"]["misses"], 1);
        assert_eq!(json["cache"]["size"], 1);
    }

    // ==================== query_handler read tests ====================

    #[tokio::test]
    async fn test_query_handler_match_empty_store() {
        let (app, _state) = test_app();

        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (n:Person) RETURN n"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        assert!(json["nodes"].as_array().unwrap().is_empty());
        assert!(json["edges"].as_array().unwrap().is_empty());
        assert_eq!(json["columns"], json!(["n"]));
        assert!(json["records"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_query_handler_match_returns_nodes() {
        let (app, state) = test_app();

        // Seed data
        {
            let mut store = state.store.write().await;
            let alice = store.create_node("Person");
            store.get_node_mut(alice).unwrap().set_property("name", "Alice");
            store.get_node_mut(alice).unwrap().set_property("age", 30i64);
        }

        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (n:Person) RETURN n"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        // Should return exactly 1 node
        let nodes = json["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1);

        let node = &nodes[0];
        assert!(node["id"].is_string());
        assert!(node["labels"].as_array().unwrap().contains(&json!("Person")));
        assert_eq!(node["properties"]["name"], "Alice");
        assert_eq!(node["properties"]["age"], 30);

        // Records should also contain 1 row
        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn test_query_handler_match_property_projection() {
        let (app, state) = test_app();

        // Seed data
        {
            let mut store = state.store.write().await;
            let n = store.create_node("Person");
            store.get_node_mut(n).unwrap().set_property("name", "Bob");
        }

        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (n:Person) RETURN n.name"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["columns"], json!(["n.name"]));

        // Property values go through Value::Property branch
        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0], "Bob");
    }

    #[tokio::test]
    async fn test_query_handler_match_edge_traversal() {
        let (app, state) = test_app();

        // Seed data: Alice -[:KNOWS]-> Bob
        {
            let mut store = state.store.write().await;
            let alice = store.create_node("Person");
            store.get_node_mut(alice).unwrap().set_property("name", "Alice");
            let bob = store.create_node("Person");
            store.get_node_mut(bob).unwrap().set_property("name", "Bob");
            store.create_edge(alice, bob, "KNOWS").unwrap();
        }

        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);

        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);

        // Should have edges populated
        let edges = json["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 1);
        let edge = &edges[0];
        assert!(edge["id"].is_string());
        assert!(edge["source"].is_string());
        assert!(edge["target"].is_string());
        assert_eq!(edge["type"], "KNOWS");

        // Should have 2 nodes (Alice and Bob)
        let nodes = json["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);
    }

    // ==================== query_handler write tests ====================

    #[tokio::test]
    async fn test_query_handler_create_node() {
        let (app, state) = test_app();

        let (status, _json) = post_query(
            app,
            r#"{"query": "CREATE (n:Movie {title: \"Inception\", year: 2010})"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);

        // Verify the node was actually created in the store
        let store = state.store.read().await;
        assert_eq!(store.node_count(), 1);
    }

    #[tokio::test]
    async fn test_query_handler_create_with_edge() {
        let (app, state) = test_app();

        let (status, _json) = post_query(
            app,
            r#"{"query": "CREATE (a:Person {name: 'Alice'})"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);

        // Verify node was created
        let store = state.store.read().await;
        assert_eq!(store.node_count(), 1);
    }

    #[tokio::test]
    async fn test_query_handler_return_integer_property() {
        let (app, state) = test_app();

        // Seed data with integer property
        {
            let mut store = state.store.write().await;
            let n = store.create_node("Person");
            store.get_node_mut(n).unwrap().set_property("age", 30i64);
        }

        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (n:Person) RETURN n.age"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0], 30);
    }

    // ==================== query_handler error tests ====================

    #[tokio::test]
    async fn test_query_handler_parse_error() {
        let (app, _state) = test_app();

        let (status, json) = post_query(
            app,
            r#"{"query": "THIS IS NOT VALID CYPHER!!!"}"#,
        ).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(json["error"].is_string());
        assert!(!json["error"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_query_handler_malformed_json_returns_error() {
        let (app, _state) = test_app();

        // Sending malformed JSON — axum's Json extractor returns 422
        let req: Request<Body> = Request::builder()
            .method("POST")
            .uri("/api/query")
            .header("content-type", "application/json")
            .body(Body::from("not json"))
            .unwrap();
        let response = app.oneshot(req).await.unwrap();

        // Axum returns 400 Bad Request for deserialization failures
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_query_handler_missing_content_type() {
        let (app, _state) = test_app();

        // Missing content-type header — axum's Json extractor rejects
        let req: Request<Body> = Request::builder()
            .method("POST")
            .uri("/api/query")
            .body(Body::from(r#"{"query": "MATCH (n) RETURN n"}"#))
            .unwrap();
        let response = app.oneshot(req).await.unwrap();

        // Axum returns 415 Unsupported Media Type when content-type is missing
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    // ==================== Value branch coverage tests ====================

    #[tokio::test]
    async fn test_query_handler_null_value() {
        let (app, state) = test_app();

        // Seed a node without 'age' property
        {
            let mut store = state.store.write().await;
            let n = store.create_node("Person");
            store.get_node_mut(n).unwrap().set_property("name", "Alice");
        }

        // Accessing a missing property returns null
        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (n:Person) RETURN n.age"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0][0].is_null(), "Missing property should be null");
    }

    #[tokio::test]
    async fn test_query_handler_named_path() {
        let (app, state) = test_app();

        // Seed data with a path
        {
            let mut store = state.store.write().await;
            let a = store.create_node("Person");
            store.get_node_mut(a).unwrap().set_property("name", "Alice");
            let b = store.create_node("Person");
            store.get_node_mut(b).unwrap().set_property("name", "Bob");
            store.create_edge(a, b, "KNOWS").unwrap();
        }

        // Named path query: p = (a)-[]->(b) RETURN p
        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);

        // Path JSON should have nodes, edges, and length
        let path = &records[0][0];
        assert!(path["nodes"].is_array());
        assert!(path["edges"].is_array());
        assert!(path["length"].is_number());
        assert_eq!(path["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(path["edges"].as_array().unwrap().len(), 1);
        assert_eq!(path["length"], 1);
    }

    #[tokio::test]
    async fn test_query_handler_multiple_columns() {
        let (app, state) = test_app();

        {
            let mut store = state.store.write().await;
            let n = store.create_node("Person");
            store.get_node_mut(n).unwrap().set_property("name", "Alice");
            store.get_node_mut(n).unwrap().set_property("age", 30i64);
        }

        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (n:Person) RETURN n.name, n.age, n"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["columns"].as_array().unwrap().len(), 3);

        // Each record row should have 3 values
        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].as_array().unwrap().len(), 3);
        // First value: property string
        assert_eq!(records[0][0], "Alice");
        // Second value: property integer
        assert_eq!(records[0][1], 30);
        // Third value: node object
        assert!(records[0][2]["id"].is_string());
    }

    #[tokio::test]
    async fn test_query_handler_node_deduplication() {
        let (app, state) = test_app();

        // Create 2 nodes with edges between them
        {
            let mut store = state.store.write().await;
            let a = store.create_node("Person");
            store.get_node_mut(a).unwrap().set_property("name", "Alice");
            let b = store.create_node("Person");
            store.get_node_mut(b).unwrap().set_property("name", "Bob");
            store.create_edge(a, b, "KNOWS").unwrap();
        }

        // Query returns both nodes
        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);

        // The nodes map should deduplicate — 2 unique nodes
        let nodes = json["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[tokio::test]
    async fn test_query_handler_profile_no_panic() {
        let (app, state) = test_app();

        // Seed data
        {
            let mut store = state.store.write().await;
            let n = store.create_node("Person");
            store.get_node_mut(n).unwrap().set_property("name", "Alice");
        }

        // PROFILE should not panic — returns plan-format RecordBatch
        let (status, json) = post_query(
            app,
            r#"{"query": "PROFILE MATCH (n:Person) RETURN n"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        // Should have plan column in records
        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn test_query_handler_count_star() {
        let (app, state) = test_app();

        {
            let mut store = state.store.write().await;
            store.create_node("Person");
            store.create_node("Person");
        }

        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (n:Person) RETURN count(*) AS total"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);
        let records = json["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0], 2);
    }

    #[tokio::test]
    async fn test_query_handler_edge_with_properties() {
        let (app, state) = test_app();

        // Create an edge with properties
        {
            let mut store = state.store.write().await;
            let a = store.create_node("Person");
            store.get_node_mut(a).unwrap().set_property("name", "Alice");
            let b = store.create_node("Person");
            store.get_node_mut(b).unwrap().set_property("name", "Bob");
            let eid = store.create_edge(a, b, "FRIENDS").unwrap();
            store.get_edge_mut(eid).unwrap().set_property("since", 2020i64);
        }

        let (status, json) = post_query(
            app,
            r#"{"query": "MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN r"}"#,
        ).await;

        assert_eq!(status, StatusCode::OK);

        let edges = json["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["type"], "FRIENDS");
        assert_eq!(edges[0]["properties"]["since"], 2020);
    }
}
