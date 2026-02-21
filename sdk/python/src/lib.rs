//! Python bindings for the Samyama Graph Database SDK
//!
//! Exposes SamyamaClient with both embedded and remote modes to Python.

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;
use samyama_sdk::{
    EmbeddedClient, RemoteClient, SamyamaClient as SamyamaClientTrait,
    QueryResult as SdkQueryResult,
};
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Create a shared tokio runtime for all async operations
fn get_runtime() -> &'static Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new().expect("Failed to create tokio runtime")
    })
}

/// Query result returned from Cypher queries
#[pyclass]
struct QueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    records_json: Vec<Vec<serde_json::Value>>,
    nodes_json: Vec<serde_json::Value>,
    edges_json: Vec<serde_json::Value>,
}

#[pymethods]
impl QueryResult {
    fn __len__(&self) -> usize {
        self.records_json.len()
    }

    fn __repr__(&self) -> String {
        format!("QueryResult(columns={:?}, records={})", self.columns, self.records_json.len())
    }

    #[getter]
    fn records(&self, py: Python<'_>) -> PyResult<PyObject> {
        json_to_py(py, &serde_json::Value::Array(
            self.records_json.iter().map(|row| serde_json::Value::Array(row.clone())).collect()
        ))
    }

    #[getter]
    fn nodes(&self, py: Python<'_>) -> PyResult<PyObject> {
        json_to_py(py, &serde_json::Value::Array(self.nodes_json.clone()))
    }

    #[getter]
    fn edges(&self, py: Python<'_>) -> PyResult<PyObject> {
        json_to_py(py, &serde_json::Value::Array(self.edges_json.clone()))
    }
}

/// Server status information
#[pyclass]
#[derive(Clone)]
struct ServerStatus {
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    version: String,
    #[pyo3(get)]
    nodes: u64,
    #[pyo3(get)]
    edges: u64,
}

#[pymethods]
impl ServerStatus {
    fn __repr__(&self) -> String {
        format!(
            "ServerStatus(status='{}', version='{}', nodes={}, edges={})",
            self.status, self.version, self.nodes, self.edges
        )
    }
}

/// Convert SDK QueryResult to Python QueryResult
fn convert_query_result(result: SdkQueryResult) -> PyResult<QueryResult> {
    let nodes_json: Vec<serde_json::Value> = result.nodes.iter().map(|n| {
        serde_json::json!({
            "id": n.id,
            "labels": n.labels,
            "properties": n.properties,
        })
    }).collect();

    let edges_json: Vec<serde_json::Value> = result.edges.iter().map(|e| {
        serde_json::json!({
            "id": e.id,
            "source": e.source,
            "target": e.target,
            "type": e.edge_type,
            "properties": e.properties,
        })
    }).collect();

    Ok(QueryResult {
        columns: result.columns,
        records_json: result.records,
        nodes_json,
        edges_json,
    })
}

/// Convert a serde_json::Value to a Python object
fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.to_object(py)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.to_object(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.to_object(py))
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.to_object(py)),
        serde_json::Value::Array(arr) => {
            let list: Vec<PyObject> = arr.iter()
                .map(|v| json_to_py(py, v))
                .collect::<PyResult<_>>()?;
            Ok(list.to_object(py))
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.to_object(py))
        }
    }
}

/// Internal enum to hold either embedded or remote client
enum ClientInner {
    Embedded(EmbeddedClient),
    Remote(RemoteClient),
}

/// Python client for the Samyama Graph Database.
///
/// Create with `SamyamaClient.embedded()` for in-process mode
/// or `SamyamaClient.connect(url)` for remote mode.
#[pyclass]
struct SamyamaClient {
    inner: Arc<ClientInner>,
}

#[pymethods]
impl SamyamaClient {
    /// Create an in-process embedded client (no server needed)
    #[staticmethod]
    fn embedded() -> PyResult<Self> {
        Ok(SamyamaClient {
            inner: Arc::new(ClientInner::Embedded(EmbeddedClient::new())),
        })
    }

    /// Connect to a running Samyama server via HTTP
    #[staticmethod]
    fn connect(url: &str) -> PyResult<Self> {
        Ok(SamyamaClient {
            inner: Arc::new(ClientInner::Remote(RemoteClient::new(url))),
        })
    }

    /// Execute a Cypher query
    #[pyo3(signature = (cypher, graph="default"))]
    fn query(&self, cypher: &str, graph: &str) -> PyResult<QueryResult> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.query(graph, cypher)),
            ClientInner::Remote(c) => rt.block_on(c.query(graph, cypher)),
        };
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Execute a read-only Cypher query
    #[pyo3(signature = (cypher, graph="default"))]
    fn query_readonly(&self, cypher: &str, graph: &str) -> PyResult<QueryResult> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.query_readonly(graph, cypher)),
            ClientInner::Remote(c) => rt.block_on(c.query_readonly(graph, cypher)),
        };
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Get server status
    fn status(&self) -> PyResult<ServerStatus> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.status()),
            ClientInner::Remote(c) => rt.block_on(c.status()),
        };
        match result {
            Ok(s) => Ok(ServerStatus {
                status: s.status,
                version: s.version,
                nodes: s.storage.nodes,
                edges: s.storage.edges,
            }),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Ping the server
    fn ping(&self) -> PyResult<String> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.ping()),
            ClientInner::Remote(c) => rt.block_on(c.ping()),
        };
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Delete a graph
    #[pyo3(signature = (graph="default"))]
    fn delete_graph(&self, graph: &str) -> PyResult<()> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.delete_graph(graph)),
            ClientInner::Remote(c) => rt.block_on(c.delete_graph(graph)),
        };
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// List graphs
    fn list_graphs(&self) -> PyResult<Vec<String>> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.list_graphs()),
            ClientInner::Remote(c) => rt.block_on(c.list_graphs()),
        };
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        match &*self.inner {
            ClientInner::Embedded(_) => "SamyamaClient(mode='embedded')".to_string(),
            ClientInner::Remote(_) => "SamyamaClient(mode='remote')".to_string(),
        }
    }
}

/// Python module for the Samyama Graph Database
#[pymodule]
fn samyama(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SamyamaClient>()?;
    m.add_class::<QueryResult>()?;
    m.add_class::<ServerStatus>()?;
    Ok(())
}
