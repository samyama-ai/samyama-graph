//! Python bindings for the Samyama Graph Database SDK
//!
//! Exposes SamyamaClient with both embedded and remote modes to Python.

use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::PyDict;
use samyama_sdk::{
    ConnectionConfig, EmbeddedClient, NlqSettings, RemoteClient, RowStream, SamyamaClient as SamyamaClientTrait,
    QueryResult as SdkQueryResult,
    AlgorithmClient, PageRankConfig, PcaConfig,
    VectorClient, DistanceMetric, NodeId,
};
use std::collections::HashMap;
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
    fn records(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &serde_json::Value::Array(
            self.records_json.iter().map(|row| serde_json::Value::Array(row.clone())).collect()
        ))
    }

    #[getter]
    fn nodes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &serde_json::Value::Array(self.nodes_json.clone()))
    }

    #[getter]
    fn edges(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &serde_json::Value::Array(self.edges_json.clone()))
    }
}

/// The rows of a streamed query, one dict per row keyed by column (#1632).
///
/// Iterating pulls rows from the server as they are needed; nothing is
/// buffered whole. Stopping early -- `break`, `close()`, leaving a `with`
/// block, or dropping the iterator -- closes the connection, and the server
/// stops producing rows. A server error, or a body that ends without its
/// trailer, raises `RuntimeError` from the iteration.
#[pyclass]
struct QueryStream {
    #[pyo3(get)]
    columns: Vec<String>,
    inner: Option<StreamSource>,
}

enum StreamSource {
    Remote(RowStream),
    /// Embedded mode: the engine is in-process, so there is no connection to
    /// stream over; the rows are the query's result, iterated.
    Buffered(std::vec::IntoIter<Vec<serde_json::Value>>),
}

#[pymethods]
impl QueryStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let row = match slf.inner.as_mut() {
            None => return Ok(None),
            Some(StreamSource::Buffered(rows)) => rows.next(),
            Some(StreamSource::Remote(stream)) => {
                let next = py.detach(|| get_runtime().block_on(stream.next_row()));
                match next {
                    Ok(row) => row,
                    Err(e) => {
                        slf.inner = None;
                        return Err(PyRuntimeError::new_err(e.to_string()));
                    }
                }
            }
        };
        let Some(row) = row else {
            slf.inner = None;
            return Ok(None);
        };
        let dict = PyDict::new(py);
        for (col, value) in slf.columns.iter().zip(row.iter()) {
            dict.set_item(col, json_to_py(py, value)?)?;
        }
        Ok(Some(dict.into_any().unbind()))
    }

    /// Stop reading: closes the connection, so the server stops producing.
    fn close(&mut self) {
        self.inner = None;
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc: Option<Py<PyAny>>,
        _tb: Option<Py<PyAny>>,
    ) -> bool {
        self.inner = None;
        false
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
fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => b.into_py_any(py),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_py_any(py)
            } else if let Some(f) = n.as_f64() {
                f.into_py_any(py)
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => s.into_py_any(py),
        serde_json::Value::Array(arr) => {
            let list: Vec<Py<PyAny>> = arr.iter()
                .map(|v| json_to_py(py, v))
                .collect::<PyResult<_>>()?;
            list.into_py_any(py)
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            dict.into_py_any(py)
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

impl SamyamaClient {
    fn require_embedded(&self) -> PyResult<&EmbeddedClient> {
        match &*self.inner {
            ClientInner::Embedded(c) => Ok(c),
            ClientInner::Remote(_) => Err(PyRuntimeError::new_err(
                "Algorithm methods are only available in embedded mode. Use SamyamaClient.embedded()."
            )),
        }
    }

    /// The embedded client, for a snapshot method; a remote client is refused
    /// with where to go instead (#2092).
    fn embedded_for_snapshot(&self, method: &str) -> PyResult<&EmbeddedClient> {
        match &*self.inner {
            ClientInner::Embedded(c) => Ok(c),
            ClientInner::Remote(_) => Err(PyRuntimeError::new_err(format!(
                "{method} reads or writes a file in this process, so it is only available in \
                 embedded mode (SamyamaClient.embedded() or SamyamaClient.open_snapshot(path)). \
                 A server imports and exports snapshots itself: \
                 POST /api/snapshot/import and POST /api/snapshot/export."
            ))),
        }
    }

    /// Import `path` into `client`, as `import_snapshot` does.
    fn import_into(
        py: Python<'_>,
        client: &EmbeddedClient,
        path: &str,
        dedup_keys: Option<Vec<String>>,
        allow_conflicts: bool,
    ) -> PyResult<Py<PyAny>> {
        if allow_conflicts && dedup_keys.is_none() {
            // Conflicts are between nodes a dedup key merges; with no keys,
            // nothing merges and the flag would be accepted and ignored.
            return Err(PyValueError::new_err(
                "allow_conflicts only applies with dedup_keys: without keys nothing is merged",
            ));
        }
        let path = std::path::PathBuf::from(path);
        let started = std::time::Instant::now();
        // The SDK's error is not `Send`, so it is turned into text before it
        // leaves the detached section.
        let stats = py
            .detach(|| {
                let rt = get_runtime();
                match &dedup_keys {
                    None => rt.block_on(client.import_snapshot("default", &path)),
                    Some(keys) => {
                        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
                        rt.block_on(client.import_snapshot_dedup_options(
                            "default", &path, &keys, allow_conflicts,
                        ))
                    }
                }
                .map_err(|e| e.to_string())
            })
            .map_err(|e| {
                PyRuntimeError::new_err(format!("import_snapshot({}): {e}", path.display()))
            })?;
        let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
        // The keys of the server's `POST /api/snapshot/import` response, so a
        // caller reads one shape whichever way the snapshot came in.
        let report = serde_json::json!({
            "nodes_imported": stats.node_count,
            "edges_imported": stats.edge_count,
            "nodes_merged": stats.merged_count,
            "merges_refused_conflicting": stats.merges_refused_conflicting,
            "edges_deduplicated": stats.edges_deduplicated,
            "parallel_edges_kept": stats.parallel_edges_kept,
            "merge_groups": stats.merge_groups,
            "largest_merge_group": stats.largest_merge_group,
            "merges_by_key": stats.merges_by_key.iter()
                .map(|(k, n)| serde_json::json!({"key": k, "merged": n}))
                .collect::<Vec<_>>(),
            "property_conflicts_by_key": stats.property_conflicts_by_key.iter()
                .map(|(k, n)| serde_json::json!({"key": k, "conflicts": n}))
                .collect::<Vec<_>>(),
            "property_conflict_samples": stats.property_conflict_samples.iter()
                .map(|c| serde_json::json!({"key": c.key, "kept": c.kept, "discarded": c.discarded}))
                .collect::<Vec<_>>(),
            "dedup_keys_unused": stats.dedup_keys_unused.iter()
                .map(|u| serde_json::json!({"key": u.key, "incoming_nodes_with_key": u.incoming_nodes_with_key}))
                .collect::<Vec<_>>(),
            "labels": stats.labels,
            "edge_types": stats.edge_types,
            "indexes_restored": stats.indexes.map(|r| serde_json::json!({
                "property": r.property,
                "unique": r.unique,
                "fulltext": r.fulltext,
                "vector": r.vector,
                "failed": r.failed,
                "conflicts": stats.index_conflicts,
            })),
            "duration_ms": duration_ms,
        });
        json_to_py(py, &report)
    }
}

/// This build serves one graph. The embedded client's snapshot methods have no
/// graph of their own to select, so another name is refused rather than
/// accepted and ignored -- the same rule `query` follows (#366).
fn require_default_graph(method: &str, graph: &str) -> PyResult<()> {
    if graph == "default" {
        Ok(())
    } else {
        Err(PyValueError::new_err(format!(
            "{method}: this build serves a single graph, \"default\"; got graph=\"{graph}\""
        )))
    }
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

    /// Connect to a running Samyama server via HTTP.
    ///
    /// `timeout_seconds` bounds a whole request and `connect_timeout_seconds`
    /// just the TCP connect; `max_retries` retries a timeout or connection
    /// failure with exponential backoff starting at `retry_base_delay_ms`.
    ///
    /// The defaults are the point of the arguments existing. This used to build
    /// a client with no request timeout, so a server that accepted the
    /// connection and then stopped talking hung the caller until the process
    /// was killed -- with no way for a notebook, a web request or an agent loop
    /// to recover (samyama-graph#1326). Pass `timeout_seconds=None` to restore
    /// the old behaviour deliberately.
    #[staticmethod]
    #[pyo3(signature = (url, timeout_seconds=30.0, connect_timeout_seconds=5.0,
                        max_retries=2, retry_base_delay_ms=100))]
    fn connect(
        url: &str,
        timeout_seconds: Option<f64>,
        connect_timeout_seconds: Option<f64>,
        max_retries: u32,
        retry_base_delay_ms: u64,
    ) -> PyResult<Self> {
        let secs = |v: Option<f64>| -> PyResult<Option<std::time::Duration>> {
            match v {
                None => Ok(None),
                Some(s) if s > 0.0 && s.is_finite() => {
                    Ok(Some(std::time::Duration::from_secs_f64(s)))
                }
                Some(s) => Err(PyRuntimeError::new_err(format!(
                    "timeout must be a positive number of seconds, or None for no \
                     timeout; got {s}"
                ))),
            }
        };
        let config = ConnectionConfig {
            timeout: secs(timeout_seconds)?,
            connect_timeout: secs(connect_timeout_seconds)?,
            max_retries,
            retry_base_delay: std::time::Duration::from_millis(retry_base_delay_ms),
            ..Default::default()
        };
        Ok(SamyamaClient {
            inner: Arc::new(ClientInner::Remote(RemoteClient::with_config(url, config))),
        })
    }

    /// Execute a Cypher query.
    ///
    /// `graph` selects the target graph. This build serves a single graph, `"default"`,
    /// and the server rejects any other name. It previously accepted and ignored the
    /// argument, so datasets written under different graph names silently merged into one
    /// (#366). To keep datasets apart here, run a separate instance per dataset.
    #[pyo3(signature = (cypher, graph="default"))]
    fn query(&self, py: Python<'_>, cypher: &str, graph: &str) -> PyResult<QueryResult> {
        let result = py.detach(|| {
            let rt = get_runtime();
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.query(graph, cypher)),
                ClientInner::Remote(c) => rt.block_on(c.query(graph, cypher)),
            }
        });
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Execute a Cypher query under an idempotency key the caller chose (#2176).
    ///
    /// Every `query` against a server is already sent with a key of its own,
    /// so the client's own retries cannot apply a write twice. That key is new
    /// per call, though: if the call raises, calling again is a second write.
    /// A caller that wants to try again after an error that does not say
    /// whether the write landed (a timeout, a dropped connection) picks the
    /// key itself and sends it again: a write that committed under it answers
    /// with its first result and does not run again, for as long as the server
    /// keeps the key (`SAMYAMA_IDEMPOTENCY_TTL_SECS`, 24 h by default). The
    /// same key with a different statement is refused.
    ///
    /// Visible ASCII, no spaces, at most 255 bytes. Refused on an embedded
    /// client, which has no reply to lose and no server to keep the key.
    #[pyo3(signature = (cypher, key, graph="default"))]
    fn query_with_idempotency_key(
        &self,
        py: Python<'_>,
        cypher: &str,
        key: &str,
        graph: &str,
    ) -> PyResult<QueryResult> {
        let result = py.detach(|| {
            let rt = get_runtime();
            match &*self.inner {
                ClientInner::Embedded(_) => Err(samyama_sdk::SamyamaError::QueryError(
                    "an idempotency key needs a server to keep it; an embedded client \
                     has no reply to lose, so call query instead"
                        .to_string(),
                )),
                ClientInner::Remote(c) => {
                    rt.block_on(c.query_with_idempotency_key(graph, cypher, key))
                }
            }
        });
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Execute a read-only Cypher query.
    ///
    /// See `query` for what `graph` does -- and does not -- select.
    #[pyo3(signature = (cypher, graph="default"))]
    fn query_readonly(&self, py: Python<'_>, cypher: &str, graph: &str) -> PyResult<QueryResult> {
        let result = py.detach(|| {
            let rt = get_runtime();
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.query_readonly(graph, cypher)),
                ClientInner::Remote(c) => rt.block_on(c.query_readonly(graph, cypher)),
            }
        });
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Run a read query and iterate its rows as the server produces them,
    /// one dict per row (#1632).
    ///
    /// ```python
    /// for row in client.query_stream("MATCH (n:Person) RETURN n.name AS name"):
    ///     print(row["name"])
    /// ```
    ///
    /// Remote mode streams with backpressure over `application/x-ndjson`; a
    /// write is refused before it runs. Embedded mode has no connection to
    /// stream over and iterates the read query's result.
    #[pyo3(signature = (cypher, graph="default"))]
    fn query_stream(&self, py: Python<'_>, cypher: &str, graph: &str) -> PyResult<QueryStream> {
        match &*self.inner {
            ClientInner::Remote(c) => {
                let stream = py
                    .detach(|| get_runtime().block_on(c.query_stream(graph, cypher)))
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(QueryStream {
                    columns: stream.columns().to_vec(),
                    inner: Some(StreamSource::Remote(stream)),
                })
            }
            ClientInner::Embedded(c) => {
                let result = py
                    .detach(|| get_runtime().block_on(c.query_readonly(graph, cypher)))
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(QueryStream {
                    columns: result.columns,
                    inner: Some(StreamSource::Buffered(result.records.into_iter())),
                })
            }
        }
    }

    /// Translate a natural-language question into Cypher.
    ///
    /// Returns the generated query; it does not run it. Pass the result to
    /// `query_readonly` for results. A generated query that writes is
    /// refused, and that refusal, like an unconfigured provider, raises
    /// `RuntimeError`.
    ///
    /// Embedded mode translates in-process (#2092): the model is shown this
    /// graph's schema, and the provider comes from `NLQ_PROVIDER`,
    /// `NLQ_MODEL`, the provider's own key variable (`OPENAI_API_KEY`,
    /// `GEMINI_API_KEY`) and `NLQ_API_BASE_URL`. `provider`, `model`,
    /// `api_key` and `api_base_url` take the place of those for this call.
    ///
    /// Remote mode asks the server (`POST /api/nlq`), which chooses its own
    /// provider, so passing any of the four to a remote client raises
    /// `ValueError` rather than being ignored.
    #[pyo3(signature = (question, provider=None, model=None, api_key=None, api_base_url=None))]
    fn nlq(
        &self,
        py: Python<'_>,
        question: &str,
        provider: Option<String>,
        model: Option<String>,
        api_key: Option<String>,
        api_base_url: Option<String>,
    ) -> PyResult<String> {
        let settings = NlqSettings { provider, model, api_key, api_base_url };
        let result = match &*self.inner {
            ClientInner::Remote(c) => {
                let given: Vec<&str> = [
                    ("provider", settings.provider.is_some()),
                    ("model", settings.model.is_some()),
                    ("api_key", settings.api_key.is_some()),
                    ("api_base_url", settings.api_base_url.is_some()),
                ]
                .into_iter()
                .filter_map(|(name, set)| set.then_some(name))
                .collect();
                if !given.is_empty() {
                    return Err(PyValueError::new_err(format!(
                        "nlq settings only apply in embedded mode (got: {}). A server \
                         chooses its own NLQ provider (NLQ_PROVIDER on the server, or the \
                         tenant's nlq_config).",
                        given.join(", ")
                    )));
                }
                py.detach(|| get_runtime().block_on(c.nlq(question)))
            }
            ClientInner::Embedded(c) => {
                py.detach(|| get_runtime().block_on(c.nlq_with(question, &settings)))
            }
        };
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Get server status
    fn status(&self, py: Python<'_>) -> PyResult<ServerStatus> {
        let result = py.detach(|| {
            let rt = get_runtime();
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.status()),
                ClientInner::Remote(c) => rt.block_on(c.status()),
            }
        });
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
    fn ping(&self, py: Python<'_>) -> PyResult<String> {
        let result = py.detach(|| {
            let rt = get_runtime();
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.ping()),
                ClientInner::Remote(c) => rt.block_on(c.ping()),
            }
        });
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Delete a graph
    #[pyo3(signature = (graph="default"))]
    fn delete_graph(&self, py: Python<'_>, graph: &str) -> PyResult<()> {
        let result = py.detach(|| {
            let rt = get_runtime();
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.delete_graph(graph)),
                ClientInner::Remote(c) => rt.block_on(c.delete_graph(graph)),
            }
        });
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// List graphs
    fn list_graphs(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let result = py.detach(|| {
            let rt = get_runtime();
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.list_graphs()),
                ClientInner::Remote(c) => rt.block_on(c.list_graphs()),
            }
        });
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    // ========================================================================
    // Snapshots (embedded mode only, #2092)
    // ========================================================================

    /// An embedded client holding the `.sgsnap` snapshot at `path`, ready to
    /// query: `SamyamaClient.embedded()` followed by `import_snapshot(path)`.
    ///
    /// ```python
    /// client = SamyamaClient.open_snapshot("pubmed.sgsnap")
    /// client.query_readonly("MATCH (a:Article) RETURN count(a)")
    /// ```
    ///
    /// No server and no network socket: the graph is in this process.
    /// `dedup_keys` and `allow_conflicts` are as for `import_snapshot`.
    #[staticmethod]
    #[pyo3(signature = (path, dedup_keys=None, allow_conflicts=false))]
    fn open_snapshot(
        py: Python<'_>,
        path: &str,
        dedup_keys: Option<Vec<String>>,
        allow_conflicts: bool,
    ) -> PyResult<Self> {
        let client = EmbeddedClient::new();
        Self::import_into(py, &client, path, dedup_keys, allow_conflicts)?;
        Ok(SamyamaClient {
            inner: Arc::new(ClientInner::Embedded(client)),
        })
    }

    /// Import the `.sgsnap` snapshot at `path` into this client's graph, and
    /// return what happened as a dict.
    ///
    /// The snapshot is **added** to what the graph already holds. With
    /// `dedup_keys` (node properties such as `["drugbank_id"]`), an incoming
    /// node that shares a key value with a node already there is merged into
    /// it instead of added; `allow_conflicts=True` lets a merge go ahead when
    /// another of the keys differs.
    ///
    /// The dict has the keys of the server's `POST /api/snapshot/import`
    /// response -- `nodes_imported`, `edges_imported`, `nodes_merged`,
    /// `merge_groups`, `merges_by_key`, `dedup_keys_unused`, `labels`,
    /// `edge_types`, `indexes_restored` and the rest -- plus `duration_ms`.
    /// A file that is missing or is not a snapshot raises `RuntimeError`, and
    /// leaves the graph as it was.
    #[pyo3(signature = (path, graph="default", dedup_keys=None, allow_conflicts=false))]
    fn import_snapshot(
        &self,
        py: Python<'_>,
        path: &str,
        graph: &str,
        dedup_keys: Option<Vec<String>>,
        allow_conflicts: bool,
    ) -> PyResult<Py<PyAny>> {
        require_default_graph("import_snapshot", graph)?;
        let client = self.embedded_for_snapshot("import_snapshot")?;
        Self::import_into(py, client, path, dedup_keys, allow_conflicts)
    }

    /// Write this client's graph to a `.sgsnap` snapshot at `path`, and return
    /// what was written as a dict: `nodes_exported`, `edges_exported`,
    /// `bytes_written`, `labels`, `edge_types`, `duration_ms`, and `dropped`
    /// -- what the format does not carry, one `{what, count, detail}` per
    /// kind, empty when nothing was lost. An existing file at `path` is
    /// replaced.
    #[pyo3(signature = (path, graph="default"))]
    fn export_snapshot(&self, py: Python<'_>, path: &str, graph: &str) -> PyResult<Py<PyAny>> {
        require_default_graph("export_snapshot", graph)?;
        let client = self.embedded_for_snapshot("export_snapshot")?;
        let path = std::path::PathBuf::from(path);
        let started = std::time::Instant::now();
        let stats = py
            .detach(|| {
                get_runtime()
                    .block_on(client.export_snapshot("default", &path))
                    .map_err(|e| e.to_string())
            })
            .map_err(|e| {
                PyRuntimeError::new_err(format!("export_snapshot({}): {e}", path.display()))
            })?;
        let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
        let report = serde_json::json!({
            "nodes_exported": stats.node_count,
            "edges_exported": stats.edge_count,
            "bytes_written": stats.bytes_written,
            "labels": stats.labels,
            "edge_types": stats.edge_types,
            "dropped": stats.dropped.iter()
                .map(|d| serde_json::json!({"what": d.what, "count": d.count, "detail": d.detail}))
                .collect::<Vec<_>>(),
            "duration_ms": duration_ms,
        });
        json_to_py(py, &report)
    }

    // ========================================================================
    // Algorithm methods (embedded mode only)
    // ========================================================================

    /// Run PageRank on the graph.
    /// Returns dict mapping node_id -> score.
    #[pyo3(signature = (label=None, edge_type=None, damping=0.85, iterations=20, tolerance=1e-6))]
    fn page_rank(
        &self,
        py: Python<'_>,
        label: Option<&str>,
        edge_type: Option<&str>,
        damping: f64,
        iterations: usize,
        tolerance: f64,
    ) -> PyResult<Py<PyAny>> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let config = PageRankConfig {
            damping_factor: damping,
            iterations,
            tolerance,
            ..Default::default()
        };
        let scores: HashMap<u64, f64> = py.detach(|| rt.block_on(client.page_rank(config, label, edge_type)));
        let dict = PyDict::new(py);
        for (k, v) in &scores {
            dict.set_item(k, v)?;
        }
        dict.into_py_any(py)
    }

    /// Detect weakly connected components.
    /// Returns dict with 'components' (dict of component_id -> list of node IDs) and 'component_count'.
    #[pyo3(signature = (label=None, edge_type=None))]
    fn wcc(&self, py: Python<'_>, label: Option<&str>, edge_type: Option<&str>) -> PyResult<Py<PyAny>> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let result = py.detach(|| rt.block_on(client.weakly_connected_components(label, edge_type)));
        let dict = PyDict::new(py);
        let component_count = result.components.len();
        let components_dict = PyDict::new(py);
        for (k, v) in &result.components {
            components_dict.set_item(k, v.into_py_any(py)?)?;
        }
        dict.set_item("components", components_dict)?;
        dict.set_item("component_count", component_count)?;
        dict.into_py_any(py)
    }

    /// Detect strongly connected components.
    /// Returns dict with 'components' and 'component_count'.
    #[pyo3(signature = (label=None, edge_type=None))]
    fn scc(&self, py: Python<'_>, label: Option<&str>, edge_type: Option<&str>) -> PyResult<Py<PyAny>> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let result = py.detach(|| rt.block_on(client.strongly_connected_components(label, edge_type)));
        let dict = PyDict::new(py);
        let component_count = result.components.len();
        let components_dict = PyDict::new(py);
        for (k, v) in &result.components {
            components_dict.set_item(k, v.into_py_any(py)?)?;
        }
        dict.set_item("components", components_dict)?;
        dict.set_item("component_count", component_count)?;
        dict.into_py_any(py)
    }

    /// Breadth-first search from source to target.
    /// Returns dict with 'path' (list of node IDs) and 'distance', or None if no path.
    #[pyo3(signature = (source, target, label=None, edge_type=None))]
    fn bfs(
        &self,
        py: Python<'_>,
        source: u64,
        target: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let result = py.detach(|| rt.block_on(client.bfs(source, target, label, edge_type)));
        match result {
            Some(path) => {
                let dict = PyDict::new(py);
                dict.set_item("path", path.path.into_py_any(py)?)?;
                dict.set_item("cost", path.cost)?;
                dict.into_py_any(py)
            }
            None => Ok(py.None()),
        }
    }

    /// Dijkstra's shortest path from source to target (weighted).
    /// Returns dict with 'path' and 'distance', or None if no path.
    #[pyo3(signature = (source, target, label=None, edge_type=None, weight_property=None))]
    fn dijkstra(
        &self,
        py: Python<'_>,
        source: u64,
        target: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
        weight_property: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let result = py.detach(|| rt.block_on(client.dijkstra(source, target, label, edge_type, weight_property)));
        match result {
            Ok(Some(path)) => {
                let dict = PyDict::new(py);
                dict.set_item("path", path.path.into_py_any(py)?)?;
                dict.set_item("cost", path.cost)?;
                dict.into_py_any(py)
            }
            Ok(None) => Ok(py.None()),
            // A refusal is not "no path". #1303 was exactly this conflation on
            // the Rust side: a graph holding a cheap negative-weight route was
            // reported as having no route at all, and the caller could not
            // tell. Mapping Err to None here would reintroduce that bug for
            // every Python caller, and silently.
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Run PCA on node numeric properties.
    /// Returns dict with 'components', 'explained_variance', 'explained_variance_ratio',
    /// 'mean', 'std_dev', 'n_samples', 'n_features'.
    #[pyo3(signature = (properties, label=None, n_components=2))]
    fn pca(
        &self,
        py: Python<'_>,
        properties: Vec<String>,
        label: Option<&str>,
        n_components: usize,
    ) -> PyResult<Py<PyAny>> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let config = PcaConfig {
            n_components,
            ..PcaConfig::default()
        };
        let props_refs: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
        let result = py.detach(|| rt.block_on(client.pca(label, &props_refs, config)));
        let dict = PyDict::new(py);
        // Convert components (Vec<Vec<f64>>) to list of lists
        let components: Vec<Vec<f64>> = result.components;
        dict.set_item("components", components.into_py_any(py)?)?;
        dict.set_item("explained_variance", result.explained_variance.into_py_any(py)?)?;
        dict.set_item("explained_variance_ratio", result.explained_variance_ratio.into_py_any(py)?)?;
        dict.set_item("mean", result.mean.into_py_any(py)?)?;
        dict.set_item("std_dev", result.std_dev.into_py_any(py)?)?;
        dict.set_item("n_samples", result.n_samples)?;
        dict.set_item("n_features", result.n_features)?;
        dict.into_py_any(py)
    }

    /// Count triangles in the graph.
    #[pyo3(signature = (label=None, edge_type=None))]
    fn triangle_count(
        &self,
        py: Python<'_>,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> PyResult<usize> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        Ok(py.detach(|| rt.block_on(client.count_triangles(label, edge_type))))
    }

    // ========================================================================
    // Vector methods (embedded mode only)
    // ========================================================================

    /// Create a vector index for a label/property.
    /// metric: "cosine", "l2", or "dot"
    #[pyo3(signature = (label, property, dimensions, metric="cosine"))]
    fn create_vector_index(
        &self,
        py: Python<'_>,
        label: &str,
        property: &str,
        dimensions: usize,
        metric: &str,
    ) -> PyResult<()> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let dist = match metric {
            "l2" => DistanceMetric::L2,
            "dot" | "inner_product" => DistanceMetric::InnerProduct,
            _ => DistanceMetric::Cosine,
        };
        py.detach(|| rt.block_on(client.create_vector_index(label, property, dimensions, dist)))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Add a vector for a node in a vector index.
    fn add_vector(
        &self,
        py: Python<'_>,
        label: &str,
        property: &str,
        node_id: u64,
        vector: Vec<f32>,
    ) -> PyResult<()> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        py.detach(|| rt.block_on(client.add_vector(label, property, NodeId(node_id), &vector)))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Search for k nearest neighbors. Returns list of (node_id, distance) tuples.
    #[pyo3(signature = (label, property, query_vector, k=10))]
    fn vector_search(
        &self,
        py: Python<'_>,
        label: &str,
        property: &str,
        query_vector: Vec<f32>,
        k: usize,
    ) -> PyResult<Vec<(u64, f32)>> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let results = py.detach(|| rt.block_on(client.vector_search(label, property, &query_vector, k)))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(results.into_iter().map(|(nid, dist)| (nid.0, dist)).collect())
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
    m.add_class::<QueryStream>()?;
    Ok(())
}
