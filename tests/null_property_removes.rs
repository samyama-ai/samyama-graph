//! Setting a property to null removes it (#952).
//!
//! Cypher has no stored null. `CREATE ({missing: null})` creates a node with
//! no `missing` key at all, so `keys(n)` must not report one.
//!
//! The engine already knew this on **one** path: `SET n.b = null` removed the
//! property correctly, while `CREATE ({b: null})` stored a `Null` and
//! `'b' IN keys(n)` answered true. Two paths to the same state that disagreed
//! — and `properties(n)` handed back a map containing an explicit `Null`,
//! which is not a value Cypher can produce any other way.
//!
//! Fixed at the store rather than at each writer: there are twenty-odd call
//! sites across CREATE, MERGE, SET, FOREACH and the algorithm writers, and
//! fixing them one at a time is how the next one gets missed.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
}

/// `keys(n)` for the single node, sorted.
fn keys(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).unwrap();
    let batch = QueryExecutor::new(store).execute(&q).unwrap();
    let mut out = match batch.records[0].get("k") {
        Some(Value::Property(PropertyValue::Array(items))) => items
            .iter()
            .map(|p| match p {
                PropertyValue::String(s) => s.clone(),
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>(),
        other => panic!("{other:?}"),
    };
    out.sort();
    out
}

#[test]
fn create_does_not_store_a_null_property() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE ({exists: 42, missing: null})");
    assert_eq!(keys(&store, "MATCH (n) RETURN keys(n) AS k"), vec!["exists"]);
}

#[test]
fn properties_does_not_report_it_either() {
    // The map used to come back containing an explicit `Null`, which Cypher
    // cannot produce any other way.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE ({exists: 42, missing: null})");
    let q = parse_query("MATCH (n) RETURN properties(n) AS k").unwrap();
    match QueryExecutor::new(&store).execute(&q).unwrap().records[0].get("k") {
        Some(Value::Property(PropertyValue::Map(m))) => {
            assert_eq!(m.len(), 1, "{m:?}");
            assert!(m.contains_key("exists"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn membership_in_keys_answers_false() {
    // The scenario as written: three questions, one about a null property.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE ({exists: 42, missing: null})");
    let q = parse_query(
        "MATCH (n) RETURN 'exists' IN keys(n) AS a, 'missing' IN keys(n) AS b, \
         'missingToo' IN keys(n) AS c",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    let got: Vec<bool> = ["a", "b", "c"]
        .iter()
        .map(|c| match batch.records[0].get(c) {
            Some(Value::Property(PropertyValue::Boolean(v))) => *v,
            other => panic!("{c}: {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![true, false, false]);
}

#[test]
fn set_to_null_still_removes() {
    // The path that was already right, pinned so the store-level change did
    // not disturb it.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE ({a: 1, b: 2})");
    run(&mut store, "MATCH (n) SET n.b = null");
    assert_eq!(keys(&store, "MATCH (n) RETURN keys(n) AS k"), vec!["a"]);
}

#[test]
fn a_relationship_property_behaves_the_same() {
    // Relationships go through a different setter, which is why fixing the
    // node path alone would have left half the bug in place.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE ()-[:R {kept: 1, dropped: null}]->()");
    assert_eq!(
        keys(&store, "MATCH ()-[r]->() RETURN keys(r) AS k"),
        vec!["kept"]
    );
}

#[test]
fn merge_does_not_store_one_either() {
    let mut store = GraphStore::new();
    run(&mut store, "MERGE ({id: 1, absent: null})");
    assert_eq!(keys(&store, "MATCH (n) RETURN keys(n) AS k"), vec!["id"]);
}

#[test]
fn ordinary_properties_are_untouched() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE ({a: 1, b: 'two', c: true, d: 3.5})");
    assert_eq!(
        keys(&store, "MATCH (n) RETURN keys(n) AS k"),
        vec!["a", "b", "c", "d"]
    );
}

#[test]
fn setting_null_over_an_existing_value_removes_it() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE ({a: 1, b: 2})");
    run(&mut store, "MATCH (n) SET n.a = null, n.b = 3");
    assert_eq!(keys(&store, "MATCH (n) RETURN keys(n) AS k"), vec!["b"]);
}
