//! Coverage tests for the SDK: the embedded client's result conversion,
//! snapshot and factory helpers, the algorithm and vector extension traits,
//! and the remote client's error handling against a local mock server.

use std::collections::HashMap;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::*;
use samyama::algo::CdlpConfig;

async fn client_with(setup: &[&str]) -> EmbeddedClient {
    let c = EmbeddedClient::new();
    for q in setup {
        c.query("default", q).await.unwrap();
    }
    c
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("samyama_sdk_cov_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

// ─────────────────────────────────────────────────────────────── models

#[test]
fn query_result_len_and_is_empty_follow_the_records() {
    let mut r = QueryResult {
        nodes: vec![],
        edges: vec![],
        columns: vec!["x".into()],
        records: vec![],
    };
    assert!(r.is_empty());
    assert_eq!(r.len(), 0);
    r.records.push(vec![serde_json::json!(1)]);
    r.records.push(vec![serde_json::json!(2)]);
    assert!(!r.is_empty());
    assert_eq!(r.len(), 2);
}

#[test]
fn errors_render_with_their_category() {
    let cases = [
        (SamyamaError::QueryError("q".into()), "Query error: q"),
        (
            SamyamaError::ConnectionError("c".into()),
            "Connection error: c",
        ),
        (SamyamaError::ProtocolError("p".into()), "Protocol error: p"),
        (SamyamaError::VectorError("v".into()), "Vector error: v"),
        (
            SamyamaError::AlgorithmError("a".into()),
            "Algorithm error: a",
        ),
        (
            SamyamaError::PersistenceError("s".into()),
            "Persistence error: s",
        ),
    ];
    for (e, want) in cases {
        assert_eq!(e.to_string(), want);
    }
    let io: SamyamaError = std::io::Error::new(std::io::ErrorKind::Other, "disk").into();
    assert_eq!(io.to_string(), "I/O error: disk");
    let json: SamyamaError = serde_json::from_str::<serde_json::Value>("{")
        .unwrap_err()
        .into();
    assert!(json.to_string().starts_with("Serialization error:"));
}

// ───────────────────────────────────────────────────── embedded results

#[tokio::test]
async fn relationships_and_paths_convert_to_json_entities() {
    let c = client_with(&[r#"CREATE (a:P {name: "A"})-[:R {w: 3}]->(b:P {name: "B"})"#]).await;
    let r = c
        .query_readonly("default", "MATCH p = (a:P)-[r:R]->(b:P) RETURN p, r, a")
        .await
        .unwrap();
    assert_eq!(r.len(), 1);
    let row = &r.records[0];
    let path = &row[0];
    assert_eq!(path["length"], 1);
    assert_eq!(path["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(path["edges"].as_array().unwrap().len(), 1);
    assert_eq!(row[1]["type"], "R");
    assert_eq!(r.edges.len(), 1);
    assert_eq!(r.edges[0].edge_type, "R");
    assert_eq!(row[2]["labels"], serde_json::json!(["P"]));
    assert_eq!(row[2]["properties"]["name"], "A");
    assert!(r
        .nodes
        .iter()
        .any(|n| n.properties.get("name") == Some(&serde_json::json!("A"))));
}

#[tokio::test]
async fn a_read_query_that_fails_to_parse_is_a_query_error() {
    let c = EmbeddedClient::new();
    let e = c.query("default", "MATCH (n RETURN n").await.unwrap_err();
    assert!(matches!(e, SamyamaError::QueryError(_)), "{e:?}");
}

#[tokio::test]
async fn a_deleted_node_reference_renders_as_a_bare_id() {
    // The row holds a reference to a node the same statement removed; there
    // is nothing left to materialise, so the entity carries only its id.
    let c = client_with(&[r#"CREATE (:T {name: "gone"})"#]).await;
    let r = c
        .query("default", "MATCH (n:T) DELETE n RETURN n")
        .await
        .unwrap();
    assert_eq!(r.len(), 1);
    let v = &r.records[0][0];
    assert!(v["id"].as_str().is_some(), "{v}");
    assert_eq!(v["labels"], serde_json::json!([]));
    assert_eq!(v["properties"], serde_json::json!({}));
    assert_eq!(r.nodes.len(), 1);
    assert_eq!(c.status().await.unwrap().storage.nodes, 0);
}

#[tokio::test]
async fn snapshot_export_and_import_round_trip() {
    let dir = temp_dir("snap");
    let path = dir.join("g.sgsnap");
    let src = client_with(&[
        r#"CREATE (a:Country {iso_code: "IN", name: "India"})"#,
        r#"CREATE (b:Country {iso_code: "FR", name: "France"})"#,
    ])
    .await;
    let stats = src.export_snapshot("default", &path).await.unwrap();
    assert_eq!(stats.node_count, 2);

    let dst = EmbeddedClient::new();
    let imported = dst.import_snapshot("default", &path).await.unwrap();
    assert_eq!(imported.node_count, 2);
    assert_eq!(dst.status().await.unwrap().storage.nodes, 2);

    // Importing the same snapshot again with dedup on iso_code adds nothing.
    dst.import_snapshot_dedup("default", &path, &["iso_code"])
        .await
        .unwrap();
    let r = dst
        .query_readonly("default", "MATCH (c:Country) RETURN c.iso_code")
        .await
        .unwrap();
    assert_eq!(r.len(), 2);

    // Missing files and unwritable paths are errors, not panics.
    assert!(dst
        .import_snapshot("default", &dir.join("nope"))
        .await
        .is_err());
    assert!(dst
        .import_snapshot_dedup("default", &dir.join("nope"), &[])
        .await
        .is_err());
    assert!(src
        .export_snapshot("default", &dir.join("no").join("x"))
        .await
        .is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn factory_helpers_build_working_components() {
    let c = EmbeddedClient::new();
    let nlq = c
        .nlq_pipeline(NLQConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock".into(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        })
        .expect("mock pipeline");
    let cypher = nlq.text_to_cypher("anything", "schema").await.unwrap();
    assert!(cypher.starts_with("MATCH"), "{cypher}");

    let _agent = c.agent_runtime(AgentConfig {
        enabled: true,
        provider: LLMProvider::Mock,
        model: "mock".into(),
        api_key: None,
        api_base_url: None,
        system_prompt: None,
        tools: vec![],
        policies: HashMap::new(),
    });

    let dir = temp_dir("pm");
    let pm = c.persistence_manager(&dir);
    assert!(pm.is_ok(), "{:?}", pm.err());
    drop(pm);
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────── algorithms

/// a -> b -> c -> a (a triangle), c -> d, all KNOWS with a weight.
async fn algo_client() -> (EmbeddedClient, HashMap<String, u64>) {
    let c = client_with(&[
        r#"CREATE (:N {name: "a", x: 1, y: 2.0})"#,
        r#"CREATE (:N {name: "b", x: 2, y: 4.0})"#,
        r#"CREATE (:N {name: "c", x: 3, y: 6.5})"#,
        r#"CREATE (:N {name: "d", x: 4, y: 8.0})"#,
        r#"MATCH (a:N {name: "a"}), (b:N {name: "b"}) CREATE (a)-[:KNOWS {w: 1.0}]->(b)"#,
        r#"MATCH (a:N {name: "b"}), (b:N {name: "c"}) CREATE (a)-[:KNOWS {w: 2.0}]->(b)"#,
        r#"MATCH (a:N {name: "c"}), (b:N {name: "a"}) CREATE (a)-[:KNOWS {w: 5.0}]->(b)"#,
        r#"MATCH (a:N {name: "c"}), (b:N {name: "d"}) CREATE (a)-[:KNOWS {w: 1.0}]->(b)"#,
    ])
    .await;
    let mut ids = HashMap::new();
    {
        let store = c.store_read().await;
        for n in store.all_nodes() {
            if let Some(PropertyValue::String(s)) = store.node_property(n.id, "name") {
                ids.insert(s.clone(), n.id.as_u64());
            }
        }
    }
    (c, ids)
}

#[tokio::test]
async fn algorithm_client_wraps_each_algorithm() {
    let (c, id) = algo_client().await;
    let (a, b, cc, d) = (id["a"], id["b"], id["c"], id["d"]);

    let view = c.build_view(Some("N"), Some("KNOWS"), Some("w")).await;
    assert_eq!(view.node_count, 4);
    assert!(view.weights.is_some());

    let scc = c
        .strongly_connected_components(Some("N"), Some("KNOWS"))
        .await;
    assert_eq!(scc.components.len(), 2);
    assert_eq!(scc.node_component[&a], scc.node_component[&cc]);
    assert_ne!(scc.node_component[&a], scc.node_component[&d]);

    let p = c
        .dijkstra(a, d, Some("N"), Some("KNOWS"), Some("w"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p.path, vec![a, b, cc, d]);
    assert_eq!(p.cost, 4.0);

    let flow = c
        .edmonds_karp(a, d, Some("N"), Some("KNOWS"))
        .await
        .unwrap();
    assert_eq!(flow.max_flow, 1.0);

    let mst = c.prim_mst(Some("N"), Some("KNOWS"), Some("w")).await;
    assert_eq!(mst.edges.len(), 3);
    assert_eq!(mst.total_weight, 4.0);

    assert_eq!(c.count_triangles(Some("N"), Some("KNOWS")).await, 1);

    let all = c
        .bfs_all_shortest_paths(a, d, Some("N"), Some("KNOWS"))
        .await;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].path, vec![a, b, cc, d]);

    let labels = c
        .cdlp(CdlpConfig::default(), Some("N"), Some("KNOWS"))
        .await
        .labels;
    assert_eq!(labels.len(), 4);

    let lcc = c
        .local_clustering_coefficient(Some("N"), Some("KNOWS"))
        .await;
    assert_eq!(lcc.coefficients[&a], 1.0);
    assert_eq!(lcc.coefficients[&d], 0.0);
}

#[tokio::test]
async fn pca_over_every_node_and_over_an_empty_label() {
    let (c, _) = algo_client().await;
    let r = c.pca(None, &["x", "y", "name"], PcaConfig::default()).await;
    assert_eq!(r.n_samples, 4);
    assert_eq!(r.n_features, 3);
    // x and y are nearly collinear; the non-numeric column reads as 0.0.
    assert!(
        r.explained_variance_ratio[0] > 0.95,
        "{:?}",
        r.explained_variance_ratio
    );
    assert_eq!(r.mean[2], 0.0);

    let empty = c
        .pca(Some("Nobody"), &["x", "y"], PcaConfig::default())
        .await;
    assert_eq!(empty.n_samples, 0);
    assert_eq!(empty.n_features, 2);
    assert!(empty.components.is_empty());
    assert_eq!(empty.mean, vec![0.0, 0.0]);
    assert_eq!(empty.std_dev, vec![1.0, 1.0]);
}

// ─────────────────────────────────────────────────────────────── vectors

#[tokio::test]
async fn vector_dimension_mismatches_are_vector_errors() {
    let c = EmbeddedClient::new();
    c.create_vector_index("Doc", "emb", 3, DistanceMetric::Cosine)
        .await
        .unwrap();
    let err = c
        .add_vector("Doc", "emb", NodeId::new(1), &[1.0, 0.0])
        .await
        .unwrap_err();
    assert!(matches!(err, SamyamaError::VectorError(_)), "{err:?}");
    let err = c
        .vector_search("Doc", "emb", &[1.0, 0.0], 3)
        .await
        .unwrap_err();
    assert!(matches!(err, SamyamaError::VectorError(_)), "{err:?}");
    // A search on an index that does not exist finds nothing.
    assert!(c
        .vector_search("Nope", "emb", &[1.0], 3)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[ignore = "bug: VectorIndexManager::add_vector returns Ok(()) for a missing index though its comment (#310) says it must now error"]
async fn adding_a_vector_to_a_missing_index_is_an_error() {
    let c = EmbeddedClient::new();
    let err = c
        .add_vector("Doc", "emb", NodeId::new(1), &[1.0, 0.0])
        .await
        .unwrap_err();
    assert!(matches!(err, SamyamaError::VectorError(_)), "{err:?}");
}

// ─────────────────────────────────────────────────────── remote client

/// Serve each connection with the next canned `(status line, body)`.
async fn canned(responses: Vec<(&'static str, &'static str)>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        for (status, body) in responses {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0u8; 8192];
            let _ = sock.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
    });
    format!("http://{addr}/")
}

#[tokio::test]
async fn remote_client_without_timeouts_keeps_its_config() {
    let cfg = ConnectionConfig {
        timeout: None,
        connect_timeout: None,
        pool_idle_timeout: None,
        pool_max_idle_per_host: 1,
        max_retries: 0,
        retry_base_delay: Duration::from_millis(1),
    };
    let r = RemoteClient::with_config("http://127.0.0.1:1/", cfg);
    assert!(r.config().timeout.is_none());
    assert_eq!(r.config().max_retries, 0);
    assert_eq!(r.list_graphs().await.unwrap(), vec!["default"]);
}

#[tokio::test]
async fn remote_query_and_delete_graph_parse_the_server_answer() {
    let body = r#"{"nodes":[],"edges":[],"columns":["x"],"records":[[1]]}"#;
    let url = canned(vec![("200 OK", body), ("200 OK", body), ("200 OK", body)]).await;
    let r = RemoteClient::new(&url);
    let q = r.query("default", "RETURN 1 AS x").await.unwrap();
    assert_eq!(q.columns, vec!["x"]);
    assert_eq!(q.records, vec![vec![serde_json::json!(1)]]);
    let q = r.query_readonly("default", "RETURN 1 AS x").await.unwrap();
    assert_eq!(q.len(), 1);
    r.delete_graph("default").await.unwrap();
}

#[tokio::test]
async fn remote_errors_carry_the_servers_message_or_a_fallback() {
    let url = canned(vec![
        ("400 Bad Request", r#"{"error":"syntax"}"#),
        ("500 Internal Server Error", "<html>oops</html>"),
        ("400 Bad Request", r#"{"message":"no error key"}"#),
    ])
    .await;
    let r = RemoteClient::new(&url);
    let e = r.query("default", "X").await.unwrap_err();
    assert_eq!(e.to_string(), "Query error: syntax");
    let e = r.query("default", "X").await.unwrap_err();
    assert_eq!(e.to_string(), "Query error: Unknown error");
    let e = r.query("default", "X").await.unwrap_err();
    assert_eq!(e.to_string(), "Query error: Unknown error");
}

#[tokio::test]
async fn remote_status_and_ping() {
    let healthy = r#"{"status":"healthy","version":"9.9.9","storage":{"nodes":1,"edges":0}}"#;
    let sick = r#"{"status":"degraded","version":"9.9.9","storage":{"nodes":1,"edges":0}}"#;
    let url = canned(vec![
        ("200 OK", healthy),
        ("200 OK", healthy),
        ("200 OK", sick),
        ("503 Service Unavailable", "{}"),
    ])
    .await;
    let r = RemoteClient::new(&url);
    let s = r.status().await.unwrap();
    assert_eq!(s.version, "9.9.9");
    assert_eq!(s.storage.nodes, 1);
    assert_eq!(r.ping().await.unwrap(), "PONG");
    let e = r.ping().await.unwrap_err();
    assert_eq!(
        e.to_string(),
        "Connection error: Server unhealthy: degraded"
    );
    let e = r.status().await.unwrap_err();
    assert!(
        e.to_string().contains("Status endpoint returned 503"),
        "{e}"
    );
}

#[tokio::test]
async fn remote_nlq_error_without_a_json_body_is_unknown() {
    let url = canned(vec![("502 Bad Gateway", "gateway")]).await;
    let e = RemoteClient::new(&url).nlq("q").await.unwrap_err();
    assert_eq!(e.to_string(), "Query error: Unknown error");
}

#[tokio::test]
async fn a_refused_connection_is_retried_then_reported() {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let cfg = ConnectionConfig {
        max_retries: 2,
        retry_base_delay: Duration::from_millis(5),
        ..ConnectionConfig::default()
    };
    let r = RemoteClient::with_config(&format!("http://127.0.0.1:{port}"), cfg);
    let e = r.query("default", "RETURN 1").await.unwrap_err();
    assert!(matches!(e, SamyamaError::HttpError(_)), "{e:?}");
}
