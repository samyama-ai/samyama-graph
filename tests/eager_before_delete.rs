//! A write does not un-produce rows the read already matched (#899).
//!
//! ```cypher
//! CREATE ()-[:R]->();
//! MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*) AS c   -- returned 1, not 2
//! ```
//!
//! An undirected pattern over one relationship matches twice, once in each
//! orientation — and `MATCH (a)-[r]-(b) RETURN a, b` really does return two
//! rows. Add the delete and the count drops to 1: the first row's delete
//! removed the edge, and the lazy expansion re-read adjacency to produce the
//! second row and found nothing there.
//!
//! The read is what decides how many rows there are. Cypher settles this with
//! an eager barrier — the read is fully materialised before the write touches
//! anything — and the engine had one, used only for `SKIP`/`LIMIT`.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::graph::PropertyValue;

fn setup(cypher: &str) -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query(cypher).expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

fn count_of(store: &mut GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).expect("parses");
    let batch = MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("runs");
    match batch.records.first().and_then(|r| r.get("c")) {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("expected an integer count, got {other:?}"),
    }
}

#[test]
fn an_undirected_expand_still_counts_both_orientations() {
    let mut store = setup("CREATE ()-[:R]->()");
    assert_eq!(
        count_of(&mut store, "MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*) AS c"),
        2
    );
}

/// The same shape through a variable-length pattern.
#[test]
fn a_variable_length_expand_counts_every_path_it_matched() {
    let mut store = setup("CREATE (a)-[:R]->(b)-[:R]->(c)");
    let n = count_of(
        &mut store,
        "MATCH (a)-[r*]-(b) DETACH DELETE a, b RETURN count(*) AS c",
    );
    assert!(n > 2, "every matched path counts, got {n}");
}

/// The count is the read's, so it does not change when the write does.
#[test]
fn the_count_matches_the_read_without_the_delete() {
    let mut read_only = setup("CREATE ()-[:R]->()");
    let q = parse_query("MATCH (a)-[r]-(b) RETURN count(*) AS c").expect("parses");
    let read = QueryExecutor::new(&read_only)
        .execute(&q)
        .expect("runs")
        .records
        .first()
        .and_then(|r| r.get("c"))
        .cloned();
    assert!(matches!(read, Some(Value::Property(PropertyValue::Integer(2)))), "{read:?}");

    assert_eq!(
        count_of(&mut read_only, "MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*) AS c"),
        2,
        "adding the DELETE must not change the count"
    );
}

/// And the delete still deletes.
#[test]
fn everything_matched_is_still_deleted() {
    let mut store = setup("CREATE ()-[:R]->(), ()-[:R]->()");
    let q = parse_query("MATCH (a)-[r]->(b) DELETE r, a, b").expect("parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs");
    let remaining = parse_query("MATCH (n) RETURN n").expect("parses");
    assert_eq!(
        QueryExecutor::new(&store).execute(&remaining).expect("runs").records.len(),
        0
    );
}
