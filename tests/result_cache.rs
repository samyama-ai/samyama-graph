//! A cached answer must be indistinguishable from a computed one (#1153).
//!
//! The cache is keyed on `(normalized query, bound params, graph epoch)`. The
//! epoch is what makes it correct: every write to the store bumps it, so an
//! entry from before a write is unreachable rather than stale.
//!
//! These tests are the gate the issue asks for. The important one is
//! `cached_and_uncached_agree_on_every_query`: same rows, same order, for a
//! corpus run both ways. A cache that reorders an unordered result is a
//! metamorphic violation, not a performance win.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::{QueryEngine, RecordBatch};

/// Deterministic rendering of a result, for comparison.
///
/// Includes column names and row order. Comparing only row *sets* would let a
/// reordering cache pass, which is exactly what must not happen.
fn render(batch: &RecordBatch) -> String {
    let mut out = batch.columns.join("|");
    for rec in &batch.records {
        out.push('\n');
        let mut cells: Vec<String> = Vec::new();
        for col in &batch.columns {
            cells.push(format!("{:?}", rec.get(col)));
        }
        out.push_str(&cells.join("|"));
    }
    out
}

fn seeded_store() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..25i64 {
        let mut props = samyama::graph::PropertyMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i));
        props.insert("name".to_string(), PropertyValue::String(format!("p{i:02}")));
        props.insert("age".to_string(), PropertyValue::Integer(20 + (i % 7)));
        store.create_node_with_properties("default", vec![Label::new("Person")], props);
    }
    let people: Vec<_> = store.get_nodes_by_label(&Label::new("Person"))
        .iter().map(|n| n.id).collect();
    for w in people.windows(2) {
        let _ = store.create_edge(w[0], w[1], "KNOWS");
    }
    store
}

const CORPUS: &[&str] = &[
    "MATCH (p:Person) RETURN p.name ORDER BY p.name",
    "MATCH (p:Person) WHERE p.age > 22 RETURN p.name, p.age ORDER BY p.name",
    "MATCH (p:Person) RETURN count(p) AS n",
    "MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN p.name, q.name ORDER BY p.name, q.name",
    "MATCH (p:Person) RETURN p.age, count(*) AS n ORDER BY p.age",
    // No ORDER BY on purpose: if the cache changed row order for an unordered
    // query, only a case like this would catch it.
    "MATCH (p:Person) WHERE p.age = 21 RETURN p.name",
];

#[test]
fn cached_and_uncached_agree_on_every_query() {
    let store = seeded_store();
    let engine = QueryEngine::new();

    for q in CORPUS {
        let plain = render(&engine.execute(q, &store).expect(q));

        let (first, was_cached) = engine.execute_cached(q, &store).expect(q);
        assert!(!was_cached, "first call for {q:?} reported a hit on an empty cache");
        assert_eq!(render(&first), plain, "cached path disagreed on first run: {q}");

        let (second, was_cached) = engine.execute_cached(q, &store).expect(q);
        assert!(was_cached, "second call for {q:?} missed; the cache never hit");
        assert_eq!(
            render(&second), plain,
            "a cached answer differed from a computed one, rows or order: {q}"
        );
    }
}

#[test]
fn a_write_makes_every_entry_unreachable() {
    let mut store = seeded_store();
    let engine = QueryEngine::new();
    let q = "MATCH (p:Person) RETURN count(p) AS n";

    let (_, cached) = engine.execute_cached(q, &store).unwrap();
    assert!(!cached);
    let (_, cached) = engine.execute_cached(q, &store).unwrap();
    assert!(cached, "warm-up did not populate the cache");
    let before = render(&engine.execute_cached(q, &store).unwrap().0);

    store.create_node_with_labels([Label::new("Person")]);

    let (after_batch, cached) = engine.execute_cached(q, &store).unwrap();
    assert!(!cached, "a write did not invalidate: the old count was served again");
    let after = render(&after_batch);
    assert_ne!(before, after, "count did not change after adding a Person");
}

/// The case the statistics cache is right to ignore and this one must not: a
/// property write that changes no count at all.
#[test]
fn a_property_write_that_changes_no_count_still_invalidates() {
    let mut store = seeded_store();
    let engine = QueryEngine::new();
    let q = "MATCH (p:Person) WHERE p.id = 0 RETURN p.name";

    let (_, _) = engine.execute_cached(q, &store).unwrap();
    let (warm, cached) = engine.execute_cached(q, &store).unwrap();
    assert!(cached);
    let before = render(&warm);

    let target = store.get_nodes_by_label(&Label::new("Person"))
        .iter().find(|n| n.properties.get("id") == Some(&PropertyValue::Integer(0)))
        .map(|n| n.id)
        .expect("seeded Person with id 0");
    store.set_node_property("default", target, "name", PropertyValue::String("renamed".into()))
        .expect("rename");

    let (after_batch, cached) = engine.execute_cached(q, &store).unwrap();
    assert!(!cached, "a property write served a stale name from the cache");
    assert_ne!(before, render(&after_batch), "the rename is not visible");
}

#[test]
fn the_plain_execute_path_never_touches_the_cache() {
    let store = seeded_store();
    let engine = QueryEngine::new();
    let q = "MATCH (p:Person) RETURN count(p) AS n";

    for _ in 0..5 {
        let _ = engine.execute(q, &store).unwrap();
    }
    assert_eq!(
        engine.result_cache_len(), 0,
        "execute() populated the result cache; a benchmark calling it would \
         measure the cache instead of the engine"
    );
    assert_eq!(engine.result_cache_stats().hits(), 0);
}

/// The gap that motivated the epoch work (#1153).
///
/// `set_column_property` writes a columnar property that queries read and
/// changes no count, so it does not touch the statistics cache. Keying the
/// result cache on the statistics hook alone would serve the old value here
/// forever. This is the test that distinguishes the two.
#[test]
fn a_columnar_property_write_invalidates_too() {
    let mut store = seeded_store();
    let engine = QueryEngine::new();
    let q = "MATCH (p:Person) WHERE p.id = 3 RETURN p.name";

    let (_, _) = engine.execute_cached(q, &store).unwrap();
    let (warm, cached) = engine.execute_cached(q, &store).unwrap();
    assert!(cached, "warm-up did not populate the cache");
    let before = render(&warm);

    let target = store.get_nodes_by_label(&Label::new("Person"))
        .iter().find(|n| n.properties.get("id") == Some(&PropertyValue::Integer(3)))
        .map(|n| n.id)
        .expect("seeded Person with id 3");
    store.set_column_property(target, "name", PropertyValue::String("columnar".into()));

    let (after, cached) = engine.execute_cached(q, &store).unwrap();
    assert!(
        !cached,
        "a columnar property write was served from the cache; the epoch did not \
         move, which is what keying on invalidate_statistics_cache() alone would do"
    );
    assert_ne!(before, render(&after), "the columnar write is not visible to the query");
}
