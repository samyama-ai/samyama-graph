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
        V::List(items) => Value::Array(items.iter().map(value_to_json).collect()),
        V::Map(entries) => {
            Value::Object(entries.iter().map(|(k, v)| (k.clone(), value_to_json(v))).collect())
        }
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
        // Rendered, not decomposed: this feeds an LLM tool response, where
        // "2015-07-21" is usable and {"days": 16637} is not (#689).
        P::Date(_) | P::LocalTime(_) | P::Time { .. }
        | P::LocalDateTime { .. } | P::ZonedDateTime { .. } => json!(p.to_cypher_string()),
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

    // ---------------------------------------------------------- CypherTool

    use crate::graph::{EdgeId, EdgeType, PropertyValue as P};
    use crate::query::executor::record::Value as V;
    use std::collections::{BTreeMap, HashMap};

    fn cypher_tool(store: GraphStore) -> CypherTool {
        CypherTool::new(Arc::new(QueryEngine::new()), Arc::new(RwLock::new(store)))
    }

    fn people() -> GraphStore {
        let mut s = GraphStore::new();
        let a = s.create_node("Person");
        s.set_node_property("default", a, "name", "Alice").unwrap();
        s.set_node_property("default", a, "age", 30i64).unwrap();
        let b = s.create_node("Person");
        s.set_node_property("default", b, "name", "Bob").unwrap();
        s
    }

    #[test]
    fn cypher_tool_metadata() {
        let tool = cypher_tool(GraphStore::new()).with_tenant("t1");
        assert_eq!(tool.name(), "cypher");
        assert_eq!(tool.tenant, "t1");
        assert!(tool.description().contains("read-only Cypher"));
        let p = tool.parameters();
        assert_eq!(p["required"], json!(["query"]));
        assert_eq!(p["properties"]["query"]["type"], "string");
    }

    #[tokio::test]
    async fn cypher_tool_returns_headers_and_rows_in_column_order() {
        let tool = cypher_tool(people());
        let out = tool
            .execute(json!({"query": "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name"}))
            .await
            .unwrap();
        assert_eq!(out["headers"], json!(["name", "age"]));
        assert_eq!(out["records"], json!([["Alice", 30], ["Bob", null]]));
    }

    #[tokio::test]
    async fn cypher_tool_renders_nodes_as_ids() {
        let tool = cypher_tool(people());
        let out = tool
            .execute(json!({"query": "MATCH (p:Person {name: 'Alice'}) RETURN p"}))
            .await
            .unwrap();
        let rec = &out["records"][0][0];
        assert!(rec["node_id"].is_u64(), "{out}");
    }

    #[tokio::test]
    async fn cypher_tool_missing_or_non_string_query_is_a_tool_error() {
        let tool = cypher_tool(GraphStore::new());
        for args in [json!({}), json!({"query": 5})] {
            let err = tool.execute(args).await.unwrap_err();
            assert!(matches!(err, AgentError::ToolError(ref m) if m.contains("missing 'query'")), "{err:?}");
        }
    }

    #[tokio::test]
    async fn cypher_tool_reports_query_errors() {
        let tool = cypher_tool(GraphStore::new());
        let err = tool.execute(json!({"query": "THIS IS NOT CYPHER"})).await.unwrap_err();
        assert!(matches!(err, AgentError::ToolError(ref m) if m.starts_with("cypher: ")), "{err:?}");
    }

    #[test]
    fn value_to_json_covers_every_value_shape() {
        let mut s = GraphStore::new();
        let a = s.create_node("A");
        let b = s.create_node("B");
        let e = s.create_edge(a, b, "R").unwrap();
        let edge = s.get_edge(e).unwrap();
        let node = s.node_materialized(a).unwrap();

        assert_eq!(value_to_json(&V::Null), Value::Null);
        assert_eq!(value_to_json(&V::Property(P::Integer(4))), json!(4));
        assert_eq!(
            value_to_json(&V::List(vec![V::Property(P::Boolean(true)), V::Null])),
            json!([true, null])
        );
        let mut m = BTreeMap::new();
        m.insert("k".to_string(), V::Property(P::String("v".into())));
        assert_eq!(value_to_json(&V::Map(m)), json!({"k": "v"}));
        assert_eq!(value_to_json(&V::Node(a, Box::new(node))), json!({"node_id": a.as_u64()}));
        assert_eq!(value_to_json(&V::NodeRef(b)), json!({"node_id": b.as_u64()}));
        assert_eq!(value_to_json(&V::Edge(e, Box::new(edge))), json!({"edge_id": e.as_u64()}));
        assert_eq!(
            value_to_json(&V::EdgeRef(EdgeId::new(9), a, b, EdgeType::new("R"))),
            json!({"edge_id": 9})
        );
        assert_eq!(
            value_to_json(&V::Path { nodes: vec![a, b], edges: vec![e] }),
            json!({"nodes": [a.as_u64(), b.as_u64()], "edges": [e.as_u64()]})
        );
    }

    #[test]
    fn prop_to_json_covers_every_property_shape() {
        assert_eq!(prop_to_json(&P::String("s".into())), json!("s"));
        assert_eq!(prop_to_json(&P::Float(1.5)), json!(1.5));
        assert_eq!(prop_to_json(&P::Boolean(false)), json!(false));
        assert_eq!(prop_to_json(&P::DateTime(1234)), json!(1234));
        assert_eq!(prop_to_json(&P::Date(16637)), json!("2015-07-21"));
        assert_eq!(prop_to_json(&P::Null), Value::Null);
        assert_eq!(
            prop_to_json(&P::Array(vec![P::Integer(1), P::String("x".into())])),
            json!([1, "x"])
        );
        let mut m = HashMap::new();
        m.insert("a".to_string(), P::Integer(1));
        assert_eq!(prop_to_json(&P::Map(m)), json!({"a": 1}));
        assert_eq!(prop_to_json(&P::Vector(vec![0.5, 1.0])), json!([0.5, 1.0]));
        assert_eq!(
            prop_to_json(&P::Duration { months: 1, days: 2, seconds: 3, nanos: 4 }),
            json!({"months": 1, "days": 2, "seconds": 3, "nanos": 4})
        );
        // Temporal values are rendered as their Cypher text, not decomposed.
        for p in [
            P::LocalTime(3_600_000_000_000),
            P::Time { nanos: 0, offset_seconds: 3600 },
            P::LocalDateTime { secs: 0, nanos: 0 },
            P::ZonedDateTime { secs: 0, nanos: 0, offset_seconds: 0, zone: None },
        ] {
            assert_eq!(prop_to_json(&p), json!(p.to_cypher_string()));
            assert!(prop_to_json(&p).is_string());
        }
    }
}
