//! `CREATE p = (a)-[:R]->(b)` binds `p` (#876).
//!
//! ```text
//! CREATE p = (a {num: 1}) RETURN p    VariableNotFound("p")
//! ```
//!
//! A query that parses, writes, and then cannot name what it just made. The
//! parser has always captured `path_variable` — `MATCH p = …` and
//! `CREATE p = …` produce the same AST field — and the write operators never
//! bound it.
//!
//! Anonymous positions need a handle for the path to reference. `CREATE`
//! already mints synthetic names for anonymous nodes, to wire edges; this
//! extends the same treatment to an anonymous **relationship** inside a named
//! path, which had no reason to need one before.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `(node count, relationship count)` of the path bound to `p`.
fn path_shape(cypher: &str) -> Option<(usize, usize)> {
    let mut store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("p")) {
        Some(Value::Path { nodes, edges }) => Some((nodes.len(), edges.len())),
        _ => None,
    }
}

/// Every shape, including the anonymous relationship that needed a synthetic
/// handle.
#[test]
fn create_binds_a_named_path() {
    assert_eq!(path_shape("CREATE p = (a {num: 1}) RETURN p"), Some((1, 0)));
    assert_eq!(path_shape("CREATE p = (a)-[:R]->(b) RETURN p"), Some((2, 1)));
    assert_eq!(path_shape("CREATE p = (a)-[r:R]->(b) RETURN p"), Some((2, 1)));
    assert_eq!(path_shape("CREATE p = (a)-[r:R]->(b)-[:S]->(c) RETURN p"), Some((3, 2)));
    assert_eq!(path_shape("CREATE p = (a)<-[:R]-(b) RETURN p"), Some((2, 1)));
}

/// A bare `MERGE` binds it too.
#[test]
fn merge_binds_a_named_path() {
    assert_eq!(path_shape("MERGE p = (a {num: 1}) RETURN p"), Some((1, 0)));
}

/// An unnamed pattern is unaffected — the binder must not invent a path where
/// none was asked for.
#[test]
fn an_unnamed_pattern_binds_nothing() {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a)-[:R]->(b) RETURN a").expect("parses");
    let batch = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs");
    let rec = batch.records.first().expect("one row");
    assert!(rec.get("p").is_none(), "bound `p` for a pattern that names no path");
    assert!(rec.get("a").is_some(), "the node variable is still bound");
}

/// The write still happens, and happens once — binding a path must not change
/// what was created.
#[test]
fn the_write_is_unchanged() {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE p = (a)-[:R]->(b) RETURN p").expect("parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs");
    let count = |cypher: &str| {
        let p = parse_query(cypher).expect("parses");
        samyama::query::executor::QueryExecutor::new(&store)
            .execute(&p)
            .expect("runs")
            .records
            .len()
    };
    assert_eq!(count("MATCH (n) RETURN n"), 2);
    assert_eq!(count("MATCH ()-[r]->() RETURN r"), 1);
}
