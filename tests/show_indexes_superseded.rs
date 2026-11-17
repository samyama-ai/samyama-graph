//! `SHOW INDEXES` says how many dead entries a vector index carries (#1972).
//!
//! Deleting a node, or giving it a new vector, leaves the old entry in the
//! HNSW graph until a fifth of the graph is dead and the index rebuilds itself
//! (#1968). Every search walks those entries. The count was exported only as a
//! Prometheus gauge; the statement that lists indexes did not show it, so an
//! operator looking at one index could not tell how far it was from a rebuild.
//!
//! The two columns are appended after `label`, `property` and `type`, so a
//! client reading those by position is unaffected, and are null for the index
//! kinds that delete in place.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

/// `(type, superseded, supersededRatio)` per row of `SHOW INDEXES`.
fn shown(s: &GraphStore) -> Vec<(String, Option<i64>, Option<f64>)> {
    let query = parse_query("SHOW INDEXES").unwrap();
    let batch = QueryExecutor::new(s).execute(&query).unwrap();
    assert_eq!(
        batch.columns,
        vec!["label", "property", "type", "superseded", "supersededRatio"]
    );
    batch
        .records
        .iter()
        .map(|r| {
            let kind = match r.get("type") {
                Some(Value::Property(PropertyValue::String(t))) => t.clone(),
                other => panic!("type: {other:?}"),
            };
            let dead = match r.get("superseded") {
                Some(Value::Property(PropertyValue::Integer(n))) => Some(*n),
                Some(Value::Property(PropertyValue::Null)) => None,
                other => panic!("superseded: {other:?}"),
            };
            let ratio = match r.get("supersededRatio") {
                Some(Value::Property(PropertyValue::Float(f))) => Some(*f),
                Some(Value::Property(PropertyValue::Null)) => None,
                other => panic!("supersededRatio: {other:?}"),
            };
            (kind, dead, ratio)
        })
        .collect()
}

fn vector_row(s: &GraphStore) -> (i64, f64) {
    let rows: Vec<_> = shown(s)
        .into_iter()
        .filter(|(t, _, _)| t.starts_with("VECTOR"))
        .collect();
    assert_eq!(rows.len(), 1);
    (
        rows[0].1.expect("a vector row has a count"),
        rows[0].2.expect("and a ratio"),
    )
}

fn docs(n: usize) -> GraphStore {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE VECTOR INDEX FOR (n:Doc) ON (n.v) OPTIONS {dimensions: 2, similarity: 'l2'}",
    );
    write(&mut s, "CREATE INDEX FOR (n:Doc) ON (n.id)");
    for i in 0..n {
        write(
            &mut s,
            &format!("CREATE (:Doc {{id: {i}, v: [{i}.0, 1.0]}})"),
        );
    }
    s
}

#[test]
fn a_fresh_index_shows_no_dead_entries_and_other_kinds_show_null() {
    let s = docs(100);
    for (kind, dead, ratio) in shown(&s) {
        if kind.starts_with("VECTOR") {
            assert_eq!((dead, ratio), (Some(0), Some(0.0)), "{kind}");
        } else {
            assert_eq!((dead, ratio), (None, None), "{kind}");
        }
    }
}

#[test]
fn deletes_and_replaced_vectors_are_counted_until_the_rebuild() {
    let mut s = docs(100);
    // Five deleted, five given a new vector: ten dead entries in a graph of
    // 105, below the fifth that triggers a rebuild.
    write(&mut s, "MATCH (n:Doc) WHERE n.id < 5 DETACH DELETE n");
    write(
        &mut s,
        "MATCH (n:Doc) WHERE n.id >= 10 AND n.id < 15 SET n.v = [0.0, -1.0]",
    );
    let (dead, ratio) = vector_row(&s);
    assert_eq!(dead, 10);
    assert!((ratio - 10.0 / 105.0).abs() < 1e-12, "{ratio}");

    // Enough further deletes to cross a fifth: the index rebuilds from its
    // live entries, so the count drops back and the ratio stays below 0.2.
    write(
        &mut s,
        "MATCH (n:Doc) WHERE n.id >= 20 AND n.id < 40 DETACH DELETE n",
    );
    let (dead, ratio) = vector_row(&s);
    assert!(dead < 21, "rebuilt: {dead}");
    assert!(ratio < 0.2, "{ratio}");
    let index = s.vector_index.get_index("Doc", "v").unwrap();
    let index = index.read().unwrap();
    assert_eq!(index.len(), 75);
    assert_eq!(dead as usize, index.superseded());
}
