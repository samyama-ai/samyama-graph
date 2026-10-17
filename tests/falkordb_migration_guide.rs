//! Pins what `docs/MIGRATING-FROM-FALKORDB.md` says about the RESP surface and
//! about the Samyama form of each FalkorDB idiom (DX-13, #1787).
//!
//! Every row of the guide's protocol table and every replacement it recommends
//! was checked by hand against a running server. These tests make that check
//! repeatable, so a change that makes the guide wrong fails here instead of in a
//! user's migration.

use samyama::graph::GraphStore;
use samyama::protocol::{CommandHandler, RespValue};
use std::sync::Arc;
use tokio::sync::RwLock;

/// The only graph name this build serves.
const GRAPH: &str = "default";

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(
        parts
            .iter()
            .map(|p| RespValue::BulkString(Some(p.as_bytes().to_vec())))
            .collect(),
    )
}

struct Server {
    handler: CommandHandler,
    store: Arc<RwLock<GraphStore>>,
}

impl Server {
    fn new() -> Self {
        Server {
            handler: CommandHandler::new(None),
            store: Arc::new(RwLock::new(GraphStore::new())),
        }
    }

    async fn send(&self, parts: &[&str]) -> RespValue {
        self.handler
            .handle_command(&cmd(parts), &self.store, None)
            .await
    }

    async fn query(&self, q: &str) -> RespValue {
        self.send(&["GRAPH.QUERY", GRAPH, q]).await
    }

    /// Runs `q` and panics with the server's message if it was refused.
    async fn ok(&self, q: &str) -> RespValue {
        let r = self.query(q).await;
        assert!(
            error_text(&r).is_none(),
            "the guide says this runs, but it was refused: {q}\n  {r:?}"
        );
        r
    }
}

fn error_text(v: &RespValue) -> Option<&str> {
    match v {
        RespValue::Error(e) => Some(e.as_str()),
        _ => None,
    }
}

fn text(v: &RespValue) -> String {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        RespValue::SimpleString(s) => s.clone(),
        RespValue::Integer(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

fn rows(v: &RespValue) -> &[RespValue] {
    match v {
        RespValue::Array(items) => items,
        other => panic!("expected an array reply, got {other:?}"),
    }
}

#[tokio::test]
async fn only_the_default_graph_is_served() {
    let s = Server::new();
    let r = s.send(&["GRAPH.QUERY", "social", "RETURN 1"]).await;
    let e = error_text(&r).expect("a FalkorDB graph key other than 'default' must be refused");
    assert!(
        e.contains("'default'"),
        "the refusal should name the graph that exists: {e}"
    );
    assert_eq!(
        rows(&s.send(&["GRAPH.LIST"]).await)
            .iter()
            .map(text)
            .collect::<Vec<_>>(),
        vec![GRAPH]
    );
}

#[tokio::test]
async fn the_read_reply_is_flat_header_then_rows() {
    let s = Server::new();
    s.ok("CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})")
        .await;
    let r = s.ok("MATCH (p:Person) RETURN p.name ORDER BY p.name").await;
    let items = rows(&r);
    assert_eq!(
        items.len(),
        3,
        "header plus two inline rows, with no statistics element: {r:?}"
    );
    assert_eq!(text(&rows(&items[0])[0]), "p.name");
    assert_eq!(text(&rows(&items[1])[0]), "Alice");
}

#[tokio::test]
async fn compact_and_the_cypher_prefix_are_refused_and_trailing_pairs_bind() {
    let s = Server::new();
    s.ok("CREATE (:Person {name: 'Alice', age: 30})").await;

    let compact = s
        .send(&[
            "GRAPH.QUERY",
            GRAPH,
            "MATCH (p:Person) RETURN p.name",
            "--compact",
        ])
        .await;
    assert!(
        error_text(&compact).is_some_and(|e| e.contains("key/value pairs")),
        "{compact:?}"
    );

    let prefix = s
        .query("CYPHER name='Alice' MATCH (p:Person {name: $name}) RETURN p.age")
        .await;
    assert!(
        error_text(&prefix).is_some(),
        "the FalkorDB CYPHER prefix should be a syntax error: {prefix:?}"
    );

    let pairs = s
        .send(&[
            "GRAPH.QUERY",
            GRAPH,
            "MATCH (p:Person {name: $name}) RETURN p.age",
            "name",
            "Alice",
        ])
        .await;
    assert_eq!(text(&rows(&rows(&pairs)[1])[0]), "30", "{pairs:?}");

    let list = s
        .send(&[
            "GRAPH.QUERY",
            GRAPH,
            "UNWIND $rows AS r RETURN r.n",
            "rows",
            r#"[{"n":1},{"n":2}]"#,
        ])
        .await;
    assert_eq!(
        rows(&list).len(),
        3,
        "a JSON parameter should bind as a list of maps: {list:?}"
    );
}

#[tokio::test]
async fn falkordb_only_commands_are_unknown_and_explain_works_as_a_prefix() {
    let s = Server::new();
    for c in [
        "GRAPH.EXPLAIN",
        "GRAPH.PROFILE",
        "GRAPH.CONFIG",
        "GRAPH.CONSTRAINT",
        "GRAPH.SLOWLOG",
        "GRAPH.COPY",
        "GRAPH.INFO",
        "GRAPH.MEMORY",
    ] {
        let r = s.send(&[c, GRAPH]).await;
        assert!(
            error_text(&r).is_some_and(|e| e.contains("unknown command")),
            "{c}: {r:?}"
        );
    }
    let plan = s.ok("EXPLAIN MATCH (n) RETURN n").await;
    assert_eq!(text(&rows(&rows(&plan)[0])[0]), "plan");
}

#[tokio::test]
async fn ro_query_refuses_a_write() {
    let s = Server::new();
    let r = s.send(&["GRAPH.RO_QUERY", GRAPH, "CREATE (:X)"]).await;
    assert!(error_text(&r).is_some(), "{r:?}");
}

#[tokio::test]
async fn the_recommended_replacements_run() {
    let s = Server::new();
    s.ok("CREATE (a:Person {name: 'Alice', embedding: [0.1, 0.2, 0.3]})-[:KNOWS]->(b:Person {name: 'Bob', embedding: [0.9, 0.1, 0.0]}), (:Movie {title: 'The Matrix'})").await;

    // Unchanged from FalkorDB.
    s.ok("CREATE INDEX ON :Person(name)").await;
    s.ok("DROP INDEX ON :Person(name)").await;
    s.ok("CALL algo.pageRank('Person', 'KNOWS') YIELD node, score RETURN node.name, score")
        .await;

    // Full-text, by index name.
    s.ok("CREATE FULLTEXT INDEX movie_title FOR (m:Movie) ON EACH [m.title]")
        .await;
    let hit = s.ok("CALL db.index.fulltext.queryNodes('movie_title', 'Matrix') YIELD node, score RETURN node.title").await;
    assert_eq!(rows(&hit).len(), 2, "one header and one hit: {hit:?}");
    s.ok("DROP FULLTEXT INDEX movie_title").await;
    s.ok("SHOW INDEXES").await;

    // Vectors: renamed options, plain lists, and the vector before k.
    let falkor_options = s.query("CREATE VECTOR INDEX FOR (p:Person) ON (p.embedding) OPTIONS {dimension: 3, similarityFunction: 'euclidean'}").await;
    assert!(
        error_text(&falkor_options).is_some_and(|e| e.contains("dimensions")),
        "{falkor_options:?}"
    );
    s.ok("CREATE VECTOR INDEX person_emb FOR (p:Person) ON (p.embedding) OPTIONS {dimensions: 3, similarity: 'cosine'}").await;
    let knn = s.ok("CALL db.index.vector.queryNodes('Person', 'embedding', [0.1, 0.2, 0.3], 1) YIELD node, score RETURN node.name").await;
    assert_eq!(text(&rows(&rows(&knn)[1])[0]), "Alice");

    // Paths, degrees, types, joins.
    s.ok("MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person {name: 'Bob'})) RETURN length(p)").await;
    s.ok("MATCH (a:Person) RETURN a.name, size([(a)<--() | 1]) AS indegree, size([(a)-->() | 1]) AS outdegree").await;
    s.ok("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(n) RETURN DISTINCT n")
        .await;
    s.ok(
        "CALL algo.wcc({label: 'Person'}) YIELD node, componentId RETURN componentId, count(node)",
    )
    .await;
    s.ok("CALL algo.betweenness({label: 'Person', edgeType: 'KNOWS'}) YIELD node, score RETURN node.name, score").await;
    let vt = s.ok("RETURN valueType(1)").await;
    assert_eq!(text(&rows(&rows(&vt)[1])[0]), "INTEGER");
    let joined = s.ok("RETURN reduce(s = '', x IN ['a', 'b'] | s + CASE WHEN s = '' THEN '' ELSE '-' END + x)").await;
    assert_eq!(text(&rows(&rows(&joined)[1])[0]), "a-b");
    let date = s
        .ok("RETURN date(datetime({epochSeconds: 1704153600}))")
        .await;
    assert_eq!(text(&rows(&rows(&date)[1])[0]), "2024-01-02");
}

#[tokio::test]
async fn the_documented_import_route_keeps_ids_and_edges() {
    let s = Server::new();
    s.ok("CREATE INDEX FOR (n:Person) ON (n.falkor_id)").await;
    let nodes =
        r#"[{"id":0,"props":{"name":"Alice","age":30}},{"id":1,"props":{"name":"Bob","age":25}}]"#;
    let edges = r#"[{"src":0,"dst":1,"props":{"since":2020}}]"#;
    let n = s
        .send(&[
            "GRAPH.QUERY",
            GRAPH,
            "UNWIND $rows AS row CREATE (n:Person) SET n += row.props, n.falkor_id = row.id",
            "rows",
            nodes,
        ])
        .await;
    assert!(error_text(&n).is_none(), "{n:?}");
    let e = s.send(&["GRAPH.QUERY", GRAPH, "UNWIND $rows AS row MATCH (a:Person {falkor_id: row.src}), (b:Person {falkor_id: row.dst}) CREATE (a)-[r:KNOWS]->(b) SET r += row.props", "rows", edges]).await;
    assert!(error_text(&e).is_none(), "{e:?}");
    let r = s
        .ok("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, a.falkor_id, r.since, b.name")
        .await;
    let row: Vec<String> = rows(&rows(&r)[1]).iter().map(text).collect();
    assert_eq!(
        row,
        vec!["Alice", "0", "2020", "Bob"],
        "the migrated edge and its endpoints' source ids"
    );
}
