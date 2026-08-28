//! Two equal lists are equal (#925).
//!
//! `PartialEq for Value` matched every variant except `List` and `Map`, which
//! fell through to `_ => false`. So `[] == []` was false, `[1,2] == [1,2]` was
//! false, and `a == a` was false for a type that also implements `Eq`.
//!
//! Nothing errored. `GROUP BY` over a list never merged two groups and
//! `DISTINCT` never removed a duplicate, so a query returned one row too many
//! with every row individually correct — which no caller can detect.
//!
//! The hash is what hid it: `Hash for Value` *did* handle both variants, so
//! two equal lists landed in the same bucket, were compared, found unequal,
//! and stored twice. A missing hash would have been loud.
//!
//! These tests exercise the behaviour through queries rather than through
//! `==` directly, because the defect was only visible in grouping and
//! deduplication and a unit test on the operator would not have run either.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

/// A chain a-b-c-d, so `p = (a)-[*0..3]->(x)` yields four paths.
fn chain() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("Root");
    let b = store.create_node("N");
    let c = store.create_node("N");
    let d = store.create_node("N");
    store.create_edge(a, b, "R").unwrap();
    store.create_edge(b, c, "R").unwrap();
    store.create_edge(c, d, "R").unwrap();
    store
}

#[test]
fn two_empty_lists_group_as_one() {
    let store = chain();
    // The zero-length path has `relationships(p) = []`; the one-hop path has
    // one relationship, whose `tail` is also `[]`. Two paths, one group.
    // Before the fix this answered 2.
    assert_eq!(
        rows(&store, "MATCH p = (:Root)-[*0..1]->(x) \
                      WITH tail(relationships(p)) AS rs, count(*) AS c RETURN rs, c"),
        1
    );
}

#[test]
fn equal_lists_of_relationships_group_as_one() {
    let store = chain();
    // Four paths of length 0..3; their tails are [], [], [r2] and [r2, r3] —
    // three distinct groups.
    assert_eq!(
        rows(&store, "MATCH p = (:Root)-[*0..3]->(x) \
                      WITH tail(relationships(p)) AS rs, count(*) AS c RETURN rs, c"),
        3
    );
}

#[test]
fn distinct_removes_a_duplicate_list() {
    let store = chain();
    assert_eq!(
        rows(&store, "MATCH p = (:Root)-[*0..1]->(x) \
                      RETURN DISTINCT tail(relationships(p)) AS rs"),
        1
    );
}

#[test]
fn distinct_keeps_lists_that_differ() {
    let store = chain();
    assert_eq!(
        rows(&store, "UNWIND [[1, 2], [1, 2], [2, 1], []] AS l RETURN DISTINCT l"),
        3
    );
}

#[test]
fn distinct_deduplicates_maps() {
    let store = chain();
    assert_eq!(
        rows(&store, "UNWIND [{a: 1}, {a: 1}, {a: 2}] AS m RETURN DISTINCT m"),
        2
    );
}

#[test]
fn a_map_is_not_equal_to_a_map_with_an_extra_key() {
    let store = chain();
    assert_eq!(
        rows(&store, "UNWIND [{a: 1}, {a: 1, b: 2}] AS m RETURN DISTINCT m"),
        2
    );
}

/// The two spellings of one list must group together, or a query that reads
/// one list from a property and builds another in the query splits them.
#[test]
fn a_stored_list_and_a_built_list_are_the_same_list() {
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    let _ = store.set_node_property(
        "default",
        n,
        "xs".to_string(),
        PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]),
    );
    assert_eq!(
        rows(&store, "MATCH (n:N) UNWIND [n.xs, [1, 2]] AS l RETURN DISTINCT l"),
        1
    );
}

/// Equality is only half of it: a HashMap consults the hash first, so the two
/// spellings must also hash alike or grouping loses entries it holds.
#[test]
fn the_two_spellings_hash_alike() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn h(v: &Value) -> u64 {
        let mut s = DefaultHasher::new();
        v.hash(&mut s);
        s.finish()
    }

    let built = Value::List(vec![
        Value::Property(PropertyValue::Integer(1)),
        Value::Property(PropertyValue::Integer(2)),
    ]);
    let stored = Value::Property(PropertyValue::Array(vec![
        PropertyValue::Integer(1),
        PropertyValue::Integer(2),
    ]));
    assert_eq!(built, stored, "equal");
    assert_eq!(h(&built), h(&stored), "and therefore hashed alike");

    // A different list must not be forced to collide just to satisfy the above.
    let other = Value::List(vec![Value::Property(PropertyValue::Integer(1))]);
    assert_ne!(built, other);
}

/// `Eq` promises reflexivity, and lists were the counter-example.
#[test]
fn a_list_equals_itself() {
    let empty = Value::List(vec![]);
    assert_eq!(empty, empty.clone());
    let one = Value::List(vec![Value::Property(PropertyValue::String("x".into()))]);
    assert_eq!(one, one.clone());
}
