//! Named subgraphs: persisted, read-only views over the tenant graph (#337).
//!
//! A subgraph is addressed the way a graph is -- its name in `GRAPH.QUERY`,
//! or as `graph` in `POST /api/query` -- and the engine confines the query to
//! it: the query text is run as written. So every check here compares what a
//! scoped query returns with what the whole graph has, and an out-of-scope
//! node has to stay out however the query reaches for it: by scan, by label,
//! by id, through one hop or several.
//!
//! The graph: two trials, two drugs, a patient and a site.
//!
//! ```text
//! (t1:Trial Recruiting)-[:TESTS]->(d1:Drug)-[:SIMILAR]->(d2:Drug)<-[:TESTS]-(t2:Trial Completed)
//! (t1)-[:ENROLLS]->(p1:Patient)-[:TAKES]->(d1)
//! (s1:Site)-[:HOSTS]->(t1)
//! ```

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::persistence::{PersistenceManager, TenantManager};
use samyama::protocol::{CommandHandler, RespValue};
use tokio::sync::RwLock;
use tower::ServiceExt;

const GRAPH: &str = "CREATE (t1:Trial {name: 't1', status: 'Recruiting'}), \
     (t2:Trial {name: 't2', status: 'Completed'}), \
     (d1:Drug {name: 'd1'}), (d2:Drug {name: 'd2'}), \
     (p1:Patient {name: 'p1'}), (s1:Site {name: 's1'}), \
     (t1)-[:TESTS]->(d1), (t2)-[:TESTS]->(d2), (d1)-[:SIMILAR]->(d2), \
     (t1)-[:ENROLLS]->(p1), (p1)-[:TAKES]->(d1), (s1)-[:HOSTS]->(t1)";

/// Trials and drugs, and three relationship types. `TAKES` is in scope by
/// type, but its only relationship starts at a patient, who is not.
const TRIALS: &[&str] = &[
    "GRAPH.SUBGRAPH.CREATE",
    "trials",
    "LABELS",
    "Trial,Drug",
    "EDGETYPES",
    "TESTS,SIMILAR,TAKES",
];

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(
        parts
            .iter()
            .map(|p| RespValue::BulkString(Some(p.as_bytes().to_vec())))
            .collect(),
    )
}

fn text(v: &RespValue) -> String {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        RespValue::SimpleString(s) => s.clone(),
        RespValue::Integer(i) => i.to_string(),
        RespValue::Array(items) => items.iter().map(text).collect::<Vec<_>>().join(","),
        other => format!("{other:?}"),
    }
}

fn error(v: &RespValue) -> Option<&str> {
    match v {
        RespValue::Error(e) => Some(e),
        _ => None,
    }
}

/// The data rows of a GRAPH.QUERY reply, each cell as text.
fn rows(reply: &RespValue) -> Vec<Vec<String>> {
    match reply {
        RespValue::Array(r) => r
            .iter()
            .skip(1)
            .map(|row| match row {
                RespValue::Array(cells) => cells.iter().map(text).collect(),
                other => vec![text(other)],
            })
            .collect(),
        other => panic!("not a result set: {other:?}"),
    }
}

/// The one column of a GRAPH.QUERY reply, sorted.
fn column(reply: &RespValue) -> Vec<String> {
    let mut out: Vec<String> = rows(reply).into_iter().map(|r| r.join(" ")).collect();
    out.sort();
    out
}

struct Server {
    dir: tempfile::TempDir,
    pm: Option<Arc<PersistenceManager>>,
    handler: CommandHandler,
    store: Arc<RwLock<GraphStore>>,
}

impl Server {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let pm = Arc::new(PersistenceManager::new(dir.path()).unwrap());
        pm.tenants()
            .create_tenant("default".into(), "default".into(), None)
            .ok();
        Server {
            handler: CommandHandler::new(Some(Arc::clone(&pm))),
            store: Arc::new(RwLock::new(GraphStore::new())),
            pm: Some(pm),
            dir,
        }
    }

    async fn with_data() -> Self {
        let s = Self::new();
        let r = s.query("default", GRAPH).await;
        assert!(error(&r).is_none(), "{r:?}");
        s
    }

    async fn send(&self, parts: &[&str]) -> RespValue {
        self.handler
            .handle_command(&cmd(parts), &self.store, None)
            .await
    }

    async fn query(&self, graph: &str, q: &str) -> RespValue {
        self.send(&["GRAPH.QUERY", graph, q]).await
    }

    async fn ok(&self, graph: &str, q: &str) -> Vec<String> {
        let r = self.query(graph, q).await;
        assert!(error(&r).is_none(), "{graph}: {q}: {r:?}");
        column(&r)
    }

    /// Stop the process and start it again on the same data directory.
    fn restart(&mut self) {
        let pm = self.pm.take().unwrap();
        pm.checkpoint().unwrap();
        // RocksDB holds an exclusive lock on the directory: the old manager
        // and the handler holding it go before the reopen.
        self.handler = CommandHandler::new(None);
        drop(pm);
        let pm = Arc::new(PersistenceManager::new(self.dir.path()).unwrap());
        let mut store = GraphStore::new();
        pm.recover_into("default", &mut store).expect("recover");
        self.store = Arc::new(RwLock::new(store));
        self.handler = CommandHandler::new(Some(Arc::clone(&pm)));
        self.pm = Some(pm);
    }
}

#[tokio::test]
async fn a_subgraph_is_created_listed_described_and_dropped() {
    let s = Server::with_data().await;
    assert_eq!(text(&s.send(TRIALS).await), "OK");
    assert_eq!(text(&s.send(&["GRAPH.SUBGRAPH.LIST"]).await), "trials");

    let info = text(&s.send(&["GRAPH.SUBGRAPH.INFO", "trials"]).await);
    // Four nodes; TESTS twice and SIMILAR once. TAKES and the other two types
    // touch a node outside the view.
    assert!(info.contains("nodes,4,relationships,3"), "{info}");
    assert!(info.contains(r#""labels":["Drug","Trial"]"#), "{info}");

    let again = s.send(TRIALS).await;
    assert!(
        error(&again).is_some_and(|e| e.contains("already exists")),
        "{again:?}"
    );

    assert_eq!(
        s.send(&["GRAPH.SUBGRAPH.DROP", "trials"]).await,
        RespValue::Integer(1)
    );
    assert_eq!(
        s.send(&["GRAPH.SUBGRAPH.LIST"]).await,
        RespValue::Array(vec![])
    );
    let gone = s.send(&["GRAPH.SUBGRAPH.DROP", "trials"]).await;
    assert!(
        error(&gone).is_some_and(|e| e.contains("no named subgraph 'trials'")),
        "{gone:?}"
    );

    // The data the view showed is untouched by the drop.
    assert_eq!(s.ok("default", "MATCH (n) RETURN count(n)").await, ["6"]);
}

#[tokio::test]
async fn a_definition_that_cannot_be_honoured_is_refused() {
    let s = Server::with_data().await;
    for (parts, says) in [
        (
            &["GRAPH.SUBGRAPH.CREATE", "default", "LABELS", "Trial"][..],
            "default",
        ),
        (
            &["GRAPH.SUBGRAPH.CREATE", "x y", "LABELS", "Trial"][..],
            "name",
        ),
        (&["GRAPH.SUBGRAPH.CREATE", "nolabels"][..], "label"),
        (
            &[
                "GRAPH.SUBGRAPH.CREATE",
                "p",
                "LABELS",
                "Trial",
                "WHERE",
                "Drug",
                "name = 'd1'",
            ][..],
            "label",
        ),
        (
            &[
                "GRAPH.SUBGRAPH.CREATE",
                "p",
                "LABELS",
                "Trial",
                "WHERE",
                "rand() > 0.5",
            ][..],
            "rand",
        ),
        (
            &[
                "GRAPH.SUBGRAPH.CREATE",
                "p",
                "LABELS",
                "Trial",
                "WHERE",
                "name = $x",
            ][..],
            "parameter",
        ),
    ] {
        let r = s.send(parts).await;
        let e = error(&r).unwrap_or_else(|| panic!("{parts:?} was accepted: {r:?}"));
        assert!(
            e.to_lowercase().contains(&says.to_lowercase()),
            "{parts:?}: {e}"
        );
    }
    assert_eq!(
        s.send(&["GRAPH.SUBGRAPH.LIST"]).await,
        RespValue::Array(vec![])
    );
}

#[tokio::test]
async fn a_scoped_query_sees_only_the_views_nodes() {
    let s = Server::with_data().await;
    s.send(TRIALS).await;

    assert_eq!(
        s.ok("trials", "MATCH (n) RETURN n.name").await,
        ["d1", "d2", "t1", "t2"]
    );
    // Counts are answered from the view, not from the graph's label counts.
    assert_eq!(s.ok("trials", "MATCH (n) RETURN count(n)").await, ["4"]);
    assert_eq!(
        s.ok("trials", "MATCH (n:Drug) RETURN count(*)").await,
        ["2"]
    );
    // A label outside the view matches nothing, by scan or by property.
    assert!(s
        .ok("trials", "MATCH (n:Patient) RETURN n.name")
        .await
        .is_empty());
    assert!(s
        .ok("trials", "MATCH (n {name: 'p1'}) RETURN n.name")
        .await
        .is_empty());
    assert_eq!(
        s.ok("trials", "MATCH (n:Patient) RETURN count(n)").await,
        ["0"]
    );
    // Nor by id.
    let p1 = s
        .ok("default", "MATCH (n:Patient) RETURN id(n)")
        .await
        .remove(0);
    assert!(s
        .ok(
            "trials",
            &format!("MATCH (n) WHERE id(n) = {p1} RETURN n.name")
        )
        .await
        .is_empty());
}

#[tokio::test]
async fn an_index_lookup_in_a_subgraph_is_confined_too() {
    let s = Server::with_data().await;
    s.ok("default", "CREATE INDEX FOR (n:Patient) ON (n.name)")
        .await;
    s.ok("default", "CREATE INDEX FOR (n:Drug) ON (n.name)")
        .await;
    s.send(TRIALS).await;
    assert!(s
        .ok("trials", "MATCH (n:Patient {name: 'p1'}) RETURN n.name")
        .await
        .is_empty());
    assert_eq!(
        s.ok("trials", "MATCH (n:Drug {name: 'd1'}) RETURN n.name")
            .await,
        ["d1"]
    );
}

#[tokio::test]
async fn traversal_does_not_leave_the_view() {
    let s = Server::with_data().await;
    s.send(TRIALS).await;

    // One hop: t1 also ENROLLS p1 and is HOSTED by s1; neither is in the view.
    assert_eq!(
        s.ok("trials", "MATCH (:Trial {name: 't1'})--(x) RETURN x.name")
            .await,
        ["d1"]
    );
    assert_eq!(
        s.ok("default", "MATCH (:Trial {name: 't1'})--(x) RETURN x.name")
            .await,
        ["d1", "p1", "s1"]
    );
    // TAKES is in scope by type, but its relationship starts outside the view.
    assert!(s
        .ok("trials", "MATCH (d:Drug)<-[:TAKES]-(p) RETURN p.name")
        .await
        .is_empty());
    // A type outside the view, from a node inside it.
    assert!(s
        .ok("trials", "MATCH (:Trial)-[:ENROLLS]->(p) RETURN p.name")
        .await
        .is_empty());

    // Several hops: from t1 the whole graph reaches p1 and s1; the view does not.
    assert_eq!(
        s.ok(
            "trials",
            "MATCH (:Trial {name: 't1'})-[*1..4]-(x) RETURN DISTINCT x.name"
        )
        .await,
        ["d1", "d2", "t2"],
    );
    assert!(s
        .ok(
            "default",
            "MATCH (:Trial {name: 't1'})-[*1..4]-(x) RETURN DISTINCT x.name"
        )
        .await
        .contains(&"p1".to_string()));
    // A path that exists only through an out-of-scope node: t1 -> p1 -> d1 is
    // not a second way to d1.
    assert_eq!(
        s.ok(
            "trials",
            "MATCH (:Trial {name: 't1'})-[*2..2]->(x) RETURN x.name"
        )
        .await,
        ["d2"],
    );

    // Every relationship in the view, against the same question asked of the
    // whole graph with the scope written out by hand.
    let scoped = s
        .ok(
            "trials",
            "MATCH (a)-[r]->(b) RETURN a.name, type(r), b.name",
        )
        .await;
    let by_hand = s
        .ok(
            "default",
            "MATCH (a)-[r]->(b) WHERE (a:Trial OR a:Drug) AND (b:Trial OR b:Drug) \
             AND type(r) IN ['TESTS', 'SIMILAR', 'TAKES'] RETURN a.name, type(r), b.name",
        )
        .await;
    assert_eq!(scoped, by_hand);
    assert_eq!(scoped.len(), 3, "{scoped:?}");
    assert_eq!(
        s.ok("trials", "MATCH ()-[r]->() RETURN count(r)").await,
        ["3"]
    );
}

#[tokio::test]
async fn a_property_predicate_narrows_one_label() {
    let s = Server::with_data().await;
    let r = s
        .send(&[
            "GRAPH.SUBGRAPH.CREATE",
            "recruiting",
            "LABELS",
            "Trial,Drug",
            "EDGETYPES",
            "TESTS",
            "WHERE",
            "Trial",
            "status = 'Recruiting'",
        ])
        .await;
    assert!(error(&r).is_none(), "{r:?}");

    assert_eq!(
        s.ok("recruiting", "MATCH (t:Trial) RETURN t.name").await,
        ["t1"]
    );
    // Drug has no predicate: both drugs are in the view.
    assert_eq!(
        s.ok("recruiting", "MATCH (d:Drug) RETURN d.name").await,
        ["d1", "d2"]
    );
    // The relationship from t2 goes with t2.
    assert_eq!(
        s.ok("recruiting", "MATCH (t)-[:TESTS]->(d) RETURN d.name")
            .await,
        ["d1"]
    );
    assert_eq!(
        s.ok(
            "recruiting",
            "MATCH (d:Drug {name: 'd2'})<--(t) RETURN t.name"
        )
        .await,
        Vec::<String>::new()
    );

    // The predicate follows the data: a trial that starts recruiting joins.
    s.ok(
        "default",
        "MATCH (t:Trial {name: 't2'}) SET t.status = 'Recruiting'",
    )
    .await;
    assert_eq!(
        s.ok("recruiting", "MATCH (t:Trial) RETURN t.name").await,
        ["t1", "t2"]
    );

    // `n.status` reads the same as a bare `status`.
    let r = s
        .send(&[
            "GRAPH.SUBGRAPH.CREATE",
            "done",
            "LABELS",
            "Trial",
            "WHERE",
            "n.status = 'Completed'",
        ])
        .await;
    assert!(error(&r).is_none(), "{r:?}");
    assert!(s.ok("done", "MATCH (t) RETURN t.name").await.is_empty());
}

#[tokio::test]
async fn writes_and_unconfinable_forms_are_refused_not_run_unscoped() {
    let s = Server::with_data().await;
    s.send(TRIALS).await;
    for q in [
        "CREATE (:Trial {name: 't3'})",
        "MATCH (n:Drug) SET n.x = 1",
        "MATCH (n) DETACH DELETE n",
    ] {
        let r = s.query("trials", q).await;
        assert!(
            error(&r)
                .is_some_and(|e| e.contains("Writes through named subgraphs are not supported")),
            "{q}: {r:?}"
        );
    }
    assert_eq!(s.ok("default", "MATCH (n) RETURN count(n)").await, ["6"]);

    // A pattern predicate is evaluated against the store from inside an
    // expression; until it can be confined it is refused, not run against
    // the whole graph.
    let r = s
        .query(
            "trials",
            "MATCH (d:Drug) WHERE (d)<-[:TAKES]-() RETURN d.name",
        )
        .await;
    assert!(
        error(&r).is_some_and(|e| e.contains("named subgraph")),
        "{r:?}"
    );
}

#[tokio::test]
async fn an_unknown_name_is_a_clear_error() {
    let s = Server::with_data().await;
    let r = s.query("nope", "MATCH (n) RETURN n").await;
    let e = error(&r).unwrap_or_else(|| panic!("ran: {r:?}"));
    assert!(
        e.contains("'nope' does not exist") && e.contains("named subgraph"),
        "{e}"
    );
    let r = s.send(&["GRAPH.RO_QUERY", "nope", "RETURN 1"]).await;
    assert!(
        error(&r).is_some_and(|e| e.contains("named subgraph")),
        "{r:?}"
    );
    let r = s.send(&["GRAPH.SUBGRAPH.INFO", "nope"]).await;
    assert!(
        error(&r).is_some_and(|e| e.contains("no named subgraph 'nope'")),
        "{r:?}"
    );
}

#[tokio::test]
async fn definitions_survive_a_restart() {
    let mut s = Server::with_data().await;
    s.send(TRIALS).await;
    s.send(&[
        "GRAPH.SUBGRAPH.CREATE",
        "recruiting",
        "LABELS",
        "Trial",
        "WHERE",
        "status = 'Recruiting'",
    ])
    .await;
    s.send(&["GRAPH.SUBGRAPH.CREATE", "gone", "LABELS", "Site"])
        .await;
    assert_eq!(
        s.send(&["GRAPH.SUBGRAPH.DROP", "gone"]).await,
        RespValue::Integer(1)
    );

    s.restart();

    assert_eq!(
        text(&s.send(&["GRAPH.SUBGRAPH.LIST"]).await),
        "recruiting,trials"
    );
    assert_eq!(
        s.ok("trials", "MATCH (:Trial {name: 't1'})--(x) RETURN x.name")
            .await,
        ["d1"]
    );
    assert_eq!(s.ok("recruiting", "MATCH (t) RETURN t.name").await, ["t1"]);
    let r = s.query("gone", "MATCH (n) RETURN n").await;
    assert!(error(&r).is_some(), "a dropped subgraph came back: {r:?}");
}

async fn http(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Body::from(b.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test]
async fn over_http_the_schema_and_queries_are_scoped() {
    let s = Server::with_data().await;
    let app = HttpServer::new(Arc::clone(&s.store), 0)
        .with_tenant_manager(Arc::new(TenantManager::new()))
        .router();

    let def = serde_json::json!({
        "name": "trials",
        "labels": ["Trial", "Drug"],
        "edge_types": ["TESTS", "SIMILAR"],
        "where": {"Trial": "status = 'Recruiting'"},
    });
    let (status, body) = http(&app, Method::POST, "/api/subgraphs", Some(def.clone())).await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["nodes"], 3, "{body}");
    let (status, _) = http(&app, Method::POST, "/api/subgraphs", Some(def)).await;
    assert_eq!(status, 409);

    let (status, body) = http(&app, Method::GET, "/api/schema?subgraph=trials", None).await;
    assert_eq!(status, 200, "{body}");
    let labels: Vec<&str> = body["node_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["label"].as_str().unwrap())
        .collect();
    assert_eq!(labels, ["Drug", "Trial"]);
    let types: Vec<&str> = body["edge_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["type"].as_str().unwrap())
        .collect();
    assert_eq!(types, ["SIMILAR", "TESTS"]);
    let shown = body.to_string();
    for outside in ["Patient", "Site", "ENROLLS", "TAKES", "HOSTS"] {
        assert!(
            !shown.contains(outside),
            "{outside} in the scoped schema: {shown}"
        );
    }
    assert_eq!(body["statistics"]["total_nodes"], 3);
    assert_eq!(body["statistics"]["total_edges"], 2);
    let trial = &body["node_types"][1];
    assert_eq!(trial["count"], 1, "{trial}");

    // The whole graph's schema is unchanged.
    let (_, whole) = http(&app, Method::GET, "/api/schema", None).await;
    assert!(whole.to_string().contains("Patient"));

    let q = serde_json::json!({"query": "MATCH (n) RETURN n.name AS name ORDER BY name", "graph": "trials"});
    let (status, body) = http(&app, Method::POST, "/api/query", Some(q)).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["records"],
        serde_json::json!([["d1"], ["d2"], ["t1"]]),
        "{body}"
    );

    let w = serde_json::json!({"query": "CREATE (:Trial)", "graph": "trials"});
    let (status, body) = http(&app, Method::POST, "/api/query", Some(w)).await;
    assert_eq!(status, 400, "{body}");

    let (status, body) = http(&app, Method::GET, "/api/subgraphs", None).await;
    assert_eq!((status, body["count"].clone()), (200, serde_json::json!(1)));
    let (status, _) = http(&app, Method::DELETE, "/api/subgraphs/trials", None).await;
    assert_eq!(status, 200);
    for (method, uri) in [
        (Method::GET, "/api/subgraphs/trials"),
        (Method::DELETE, "/api/subgraphs/trials"),
        (Method::GET, "/api/schema?subgraph=trials"),
    ] {
        let (status, body) = http(&app, method.clone(), uri, None).await;
        assert_eq!(status, 404, "{method} {uri}: {body}");
    }
    let q = serde_json::json!({"query": "MATCH (n) RETURN n", "graph": "trials"});
    let (status, body) = http(&app, Method::POST, "/api/query", Some(q)).await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("named subgraph"),
        "{body}"
    );
}
