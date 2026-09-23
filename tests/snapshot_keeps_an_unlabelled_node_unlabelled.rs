//! A node with no labels survives a `.sgsnap` round trip with no labels.
//!
//! It did not. `import_tenant` defaulted the "first label" to `""` when the
//! snapshot carried none and then created the node **with** that label, so an
//! unlabelled node came back labelled with the empty string:
//!
//! ```text
//! MATCH (n) WHERE size(labels(n)) = 0 RETURN count(n)
//!   before the round trip: 1
//!   after:                 0
//! ```
//!
//! Neo4j allows an unlabelled node, `CREATE (n)` makes one, and the importer
//! for `apoc.export.json` deliberately invents no label for one. The snapshot
//! path was the odd one out.
//!
//! Found by `examples/snapshot_portability`, whose whole job is to compare the
//! graph a query sees on either side of a snapshot — on its first run, against
//! a fixture that happened to contain an unlabelled node.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

fn round_trip(src: &GraphStore) -> GraphStore {
    let mut buf: Vec<u8> = Vec::new();
    samyama::snapshot::export_tenant(src, &mut buf).expect("export");
    let mut dst = GraphStore::new();
    samyama::snapshot::import_tenant(&mut dst, &buf[..]).expect("import");
    dst
}

fn count(store: &GraphStore, q: &str) -> i64 {
    let batch = QueryEngine::new()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let v = batch
        .records
        .first()
        .and_then(|r| r.values().next().cloned())
        .unwrap_or_else(|| panic!("{q}: no rows"));
    match v {
        samyama::query::executor::record::Value::Property(PropertyValue::Integer(n)) => n,
        other => panic!("{q}: expected an integer, got {other:?}"),
    }
}

fn graph(cypher: &str) -> GraphStore {
    let mut g = GraphStore::new();
    QueryEngine::new()
        .execute_mut(cypher, &mut g, "default")
        .unwrap_or_else(|e| panic!("{cypher}: {e}"));
    g
}

#[test]
fn an_unlabelled_node_is_still_unlabelled_after_a_round_trip() {
    let src = graph("CREATE (:Person {n: 'a'}) CREATE ({n: 'unlabelled'})");
    let dst = round_trip(&src);

    let q = "MATCH (n) WHERE size(labels(n)) = 0 RETURN count(n) AS c";
    assert_eq!(count(&src, q), 1, "the fixture itself is wrong");
    assert_eq!(
        count(&dst, q),
        1,
        "the unlabelled node gained a label across the snapshot"
    );
}

#[test]
fn no_empty_string_label_is_invented() {
    // The specific symptom, pinned separately: the count test above would also
    // pass if the round trip dropped the node entirely.
    let src = graph("CREATE (:Person {n: 'a'}) CREATE ({n: 'unlabelled'})");
    let dst = round_trip(&src);

    assert_eq!(dst.all_nodes().len(), 2, "a node went missing");
    let labels: Vec<Vec<String>> = dst
        .all_nodes()
        .iter()
        .map(|n| {
            let mut v: Vec<String> = n.labels.iter().map(|l| l.as_str().to_string()).collect();
            v.sort();
            v
        })
        .collect();
    assert!(
        !labels.iter().any(|ls| ls.iter().any(|l| l.is_empty())),
        "an empty-string label was invented: {labels:?}"
    );
    assert!(
        labels.contains(&Vec::<String>::new()),
        "no node came back unlabelled: {labels:?}"
    );
}

#[test]
fn labelled_nodes_are_unaffected() {
    // The fix branches on "no labels", so the ordinary path needs a guard too:
    // a change that dropped the *first* label from every node would satisfy
    // both tests above.
    let src = graph(
        "CREATE (:Person:Employee {n: 'a'}) CREATE (:Company {n: 'c'}) CREATE ({n: 'u'})",
    );
    let dst = round_trip(&src);

    for q in [
        "MATCH (n:Person) RETURN count(n) AS c",
        "MATCH (n:Employee) RETURN count(n) AS c",
        "MATCH (n:Company) RETURN count(n) AS c",
    ] {
        assert_eq!(count(&src, q), count(&dst, q), "{q}");
    }
    assert_eq!(
        count(&dst, "MATCH (n:Person:Employee) RETURN count(n) AS c"),
        1,
        "the second label was lost"
    );
}

#[test]
fn an_unlabelled_node_keeps_its_properties() {
    // An unlabelled node now takes a different creation path from a labelled
    // one, and the properties travel separately from the labels.
    let src = graph("CREATE ({name: 'solo', n: 7, ok: true})");
    let dst = round_trip(&src);

    assert_eq!(
        count(&dst, "MATCH (n) WHERE n.name = 'solo' AND n.n = 7 RETURN count(n) AS c"),
        1,
        "the unlabelled node lost its properties"
    );
}
