use crate::agent::{AgentError, AgentResult, Tool};
use crate::graph::GraphStore;
use crate::query::QueryEngine;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cypher read-only query tool. Executes the `query` arg against the
/// provided graph store and returns `{records: [[...]], headers: [...]}`.
/// Unlike WebSearchTool below this is not a stub — it wires straight
/// to the same QueryEngine the RESP/HTTP layers use.
pub struct CypherTool {
    engine: Arc<QueryEngine>,
    store: Arc<RwLock<GraphStore>>,
    tenant: String,
}

impl CypherTool {
    pub fn new(engine: Arc<QueryEngine>, store: Arc<RwLock<GraphStore>>) -> Self {
        Self { engine, store, tenant: "default".to_string() }
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = tenant.into();
        self
    }
}

#[async_trait]
impl Tool for CypherTool {
    fn name(&self) -> &str { "cypher" }
    fn description(&self) -> &str {
        "Run a read-only Cypher query against the graph and return matching records."
    }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "query": { "type": "string", "description": "Cypher MATCH/RETURN text" }
            },
            "required": ["query"]
        })
    }
    async fn execute(&self, args: Value) -> AgentResult<Value> {
        let query = args.get("query").and_then(|v| v.as_str()).ok_or_else(|| {
            AgentError::ToolError("missing 'query' parameter".into())
        })?;
        let store = self.store.read().await;
        let batch = self
            .engine
            .execute(query, &*store)
            .map_err(|e| AgentError::ToolError(format!("cypher: {e}")))?;
        let records: Vec<Vec<Value>> = batch
            .records
            .iter()
            .map(|r| {
                batch
                    .columns
                    .iter()
                    .map(|col| r.get(col).map(value_to_json).unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        Ok(json!({ "headers": batch.columns, "records": records }))
    }
}

fn value_to_json(v: &crate::query::executor::record::Value) -> Value {
    use crate::query::executor::record::Value as V;
    match v {
        V::Null => Value::Null,
        V::Property(p) => prop_to_json(p),
        V::Node(id, _) | V::NodeRef(id) => json!({ "node_id": id.as_u64() }),
        V::Edge(id, _) | V::EdgeRef(id, _, _, _) => json!({ "edge_id": id.as_u64() }),
        V::Path { nodes, edges } => json!({
            "nodes": nodes.iter().map(|n| n.as_u64()).collect::<Vec<_>>(),
            "edges": edges.iter().map(|e| e.as_u64()).collect::<Vec<_>>(),
        }),
    }
}

fn prop_to_json(p: &crate::graph::PropertyValue) -> Value {
    use crate::graph::PropertyValue as P;
    match p {
        P::String(s) => json!(s),
        P::Integer(i) => json!(i),
        P::Float(f) => json!(f),
        P::Boolean(b) => json!(b),
        P::DateTime(ts) => json!(ts),
        P::Null => Value::Null,
        P::Array(a) => Value::Array(a.iter().map(prop_to_json).collect()),
        P::Map(m) => Value::Object(m.iter().map(|(k, v)| (k.clone(), prop_to_json(v))).collect()),
        P::Vector(v) => json!(v),
        P::Duration { months, days, seconds, nanos } => {
            json!({"months": months, "days": days, "seconds": seconds, "nanos": nanos})
        }
    }
}

pub struct WebSearchTool {
    api_key: String, // Google Custom Search API Key (or SerpApi)
    client: Client,
}

impl WebSearchTool {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Tool for WebSearchTool {
    fn name(&self) -> &str {
        "web_search"
    }

    fn description(&self) -> &str {
        "Search the web for information using Google Custom Search."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query"
                }
            },
            "required": ["query"]
        })
    }

    async fn execute(&self, args: Value) -> AgentResult<Value> {
        let query = args.get("query")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentError::ToolError("Missing 'query' parameter".to_string()))?;

        // Mock implementation for demo/prototype to avoid needing another real API key immediately
        // In production, call: https://www.googleapis.com/customsearch/v1?key={}&cx={}&q={}
        
        println!("Available for search: {}", query);
        
        // Return dummy data
        Ok(json!({
            "results": [
                { "title": "Samyama Graph Database", "snippet": "Samyama is a high-performance distributed graph database..." },
                { "title": "Graph Database - Wikipedia", "snippet": "A graph database is a database that uses graph structures..." }
            ]
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_web_search_tool_name() {
        let tool = WebSearchTool::new("test-key".to_string());
        assert_eq!(tool.name(), "web_search");
    }

    #[test]
    fn test_web_search_tool_description() {
        let tool = WebSearchTool::new("test-key".to_string());
        assert!(!tool.description().is_empty());
    }

    #[test]
    fn test_web_search_tool_parameters() {
        let tool = WebSearchTool::new("test-key".to_string());
        let params = tool.parameters();
        assert_eq!(params["type"], "object");
        assert!(params["properties"]["query"].is_object());
    }

    #[tokio::test]
    async fn test_web_search_tool_execute() {
        let tool = WebSearchTool::new("test-key".to_string());
        let args = json!({"query": "graph database"});
        let result = tool.execute(args).await;
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(value["results"].is_array());
    }

    #[tokio::test]
    async fn test_web_search_tool_missing_query() {
        let tool = WebSearchTool::new("test-key".to_string());
        let args = json!({});
        let result = tool.execute(args).await;
        assert!(result.is_err());
    }
}
