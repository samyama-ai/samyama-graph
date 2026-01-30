//! HTTP handlers for the Visualizer API

use axum::{
    extract::{State, Json},
    response::IntoResponse,
};
use crate::graph::GraphStore;
use crate::query::{QueryEngine, Value};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

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
    State(store): State<Arc<RwLock<GraphStore>>>,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    let engine = QueryEngine::new();
    
    // Check if query is write or read
    let query_upper = payload.query.trim().to_uppercase();
    let is_write = query_upper.starts_with("CREATE") || 
                   query_upper.starts_with("SET") || 
                   query_upper.starts_with("DELETE");

    let result = if is_write {
        let mut store_guard = store.write().await;
        engine.execute_mut(&payload.query, &mut *store_guard, "default")
    } else {
        let store_guard = store.read().await;
        engine.execute(&payload.query, &*store_guard)
    };

    match result {
        Ok(batch) => {
            let mut nodes = HashMap::new();
            let mut edges = HashMap::new();
            let mut records = Vec::new();

            for record in &batch.records {
                let mut row = Vec::new();
                for col in &batch.columns {
                    let val = record.get(col).unwrap();
                    
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
                        Value::Property(p) => {
                            row.push(p.to_json());
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
    State(store): State<Arc<RwLock<GraphStore>>>,
) -> impl IntoResponse {
    let store_guard = store.read().await;
    Json(json!({
        "status": "healthy",
        "version": crate::VERSION,
        "storage": {
            "nodes": store_guard.node_count(),
            "edges": store_guard.edge_count(),
        }
    }))
}
