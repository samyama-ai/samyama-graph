//! MERGE must match a node that exists, whichever store holds its properties.
//!
//! Snapshot import writes property values to the columnar store and leaves the
//! row `Node.properties` empty. MERGE's candidate filter read the row only, so
//! on any restored graph it matched nothing and took the create branch: the
//! first MERGE of an existing entity silently made a second one.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::{QueryEngine, Value};

fn count(engine: &QueryEngine, store: &GraphStore, q: &str) -> i64 {
    let b = engine.execute(q, store).expect(q);
    match b.records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(c))) => *c,
        other => panic!("{q}: unexpected {other:?}"),
    }
}

/// A store populated by a snapshot round trip, so values live in the columns.
fn restored(setup: &str) -> GraphStore {
    let engine = QueryEngine::new();
    let mut src = GraphStore::new();
    engine.execute_mut(setup, &mut src, "default").expect(setup);
    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(&src, &mut buf).expect("export");
    let mut dst = GraphStore::new();
    samyama::snapshot::import_tenant(&mut dst, &buf[..]).expect("import");
    dst
}

#[test]
fn merge_matches_an_existing_node_after_a_snapshot_round_trip() {
    let engine = QueryEngine::new();
    let mut store = restored(r#"CREATE (:P {k: 1, name: "x"})"#);

    // Precondition that makes the test mean something: the values really are
    // columnar-only here. If import starts filling the row copy, this test
    // would pass without exercising the fix.
    let row_len = store
        .get_nodes_by_label(&samyama::Label::new("P"))
        .first()
        .map(|n| n.properties.len())
        .expect("one P node");
    assert_eq!(row_len, 0, "import filled the row copy; this test no longer covers the bug");

    engine.execute_mut("MERGE (n:P {k: 1}) RETURN n", &mut store, "default").unwrap();
    assert_eq!(
        count(&engine, &store, "MATCH (n:P) RETURN count(n) AS c"), 1,
        "MERGE created a duplicate of a node that exists"
    );
}

/// The path form of MERGE filters its endpoint candidates through the same check.
///
/// Differential, not hand-written: openCypher MERGE matches or creates the
/// *whole* pattern, so a path MERGE with no existing relationship creates new
/// endpoints even when matching nodes exist. Asserting "no new :A" there would
/// encode a rule Cypher does not have. What must hold is that a restored store
/// answers exactly as the store it was restored from.
#[test]
fn merge_on_a_path_behaves_the_same_after_a_snapshot_round_trip() {
    const SETUP: &str = r#"CREATE (:A {k: 1})-[:R]->(:B {k: 2})"#;
    const MERGE: &str = "MERGE (a:A {k: 1})-[:R]->(b:B {k: 2}) RETURN a, b";
    let counts = |store: &GraphStore| -> (i64, i64, i64) {
        let e = QueryEngine::new();
        (
            count(&e, store, "MATCH (n:A) RETURN count(n) AS c"),
            count(&e, store, "MATCH (n:B) RETURN count(n) AS c"),
            count(&e, store, "MATCH (:A)-[r:R]->(:B) RETURN count(r) AS c"),
        )
    };
    let engine = QueryEngine::new();

    let mut original = GraphStore::new();
    engine.execute_mut(SETUP, &mut original, "default").unwrap();
    engine.execute_mut(MERGE, &mut original, "default").unwrap();

    let mut store = restored(SETUP);
    engine.execute_mut(MERGE, &mut store, "default").unwrap();

    assert_eq!(counts(&original), (1, 1, 1), "control: the path existed, so MERGE should match it");
    assert_eq!(
        counts(&store), counts(&original),
        "a restored store answered a path MERGE differently from its source"
    );
}

/// A property that does not match must still create: the fix must not turn
/// MERGE into "match any node with the label".
#[test]
fn merge_still_creates_when_the_value_differs() {
    let engine = QueryEngine::new();
    let mut store = restored(r#"CREATE (:P {k: 1})"#);
    engine.execute_mut("MERGE (n:P {k: 2}) RETURN n", &mut store, "default").unwrap();
    assert_eq!(count(&engine, &store, "MATCH (n:P) RETURN count(n) AS c"), 2);
}
