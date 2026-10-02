//! A write the store refuses is refused end to end, and a label change reaches
//! every structure keyed by label (#1603, #1604, #1605).
//!
//! The three issues share one shape: the store already said no, or already
//! knew the node changed, and some caller or side structure did not hear it.

use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::query::QueryEngine;
use samyama::vector::DistanceMetric;

fn constrained() -> (GraphStore, QueryEngine) {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    e.execute_mut(
        "CREATE CONSTRAINT FOR (n:U) REQUIRE n.k IS UNIQUE",
        &mut s,
        "default",
    )
    .unwrap();
    (s, e)
}

fn props(pairs: &[(&str, PropertyValue)]) -> PropertyMap {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn count(e: &QueryEngine, s: &GraphStore, q: &str) -> i64 {
    let b = e.execute(q, s).unwrap();
    match b.records[0].get(&b.columns[0]) {
        Some(samyama::query::Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{q}: {other:?}"),
    }
}

// ───────────────────────────────────────────────────────────── #1603

#[test]
fn the_checked_create_refuses_a_duplicate_key_and_leaves_the_store_unchanged() {
    let (mut s, e) = constrained();
    let first = s
        .try_create_node_with_properties(
            "default",
            vec![Label::new("U")],
            props(&[("k", PropertyValue::Integer(1))]),
        )
        .expect("the first holder is admitted");
    let before = s.node_count();

    let err = s
        .try_create_node_with_properties(
            "default",
            vec![Label::new("U"), Label::new("Extra")],
            props(&[
                ("k", PropertyValue::Integer(1)),
                ("x", PropertyValue::Integer(9)),
            ]),
        )
        .expect_err("a second holder of :U(k)=1 must be refused");
    assert!(err.to_string().contains(":U(k)"), "{err}");
    assert_eq!(
        s.node_count(),
        before,
        "a refused node leaves no node behind"
    );
    assert_eq!(
        count(&e, &s, "MATCH (n:Extra) RETURN count(n)"),
        0,
        "nor a label entry"
    );
    assert_eq!(
        s.find_node_by_unique(&Label::new("U"), "k", &PropertyValue::Integer(1))
            .unwrap(),
        Some(first),
        "the key is still held by exactly the first node"
    );

    // A different value, a null, and a label without the constraint all pass.
    s.try_create_node_with_properties(
        "default",
        vec![Label::new("U")],
        props(&[("k", PropertyValue::Integer(2))]),
    )
    .unwrap();
    s.try_create_node_with_properties(
        "default",
        vec![Label::new("V")],
        props(&[("k", PropertyValue::Integer(1))]),
    )
    .unwrap();
}

#[test]
fn a_key_freed_by_a_delete_can_be_taken_by_the_checked_create() {
    let (mut s, e) = constrained();
    s.try_create_node_with_properties(
        "default",
        vec![Label::new("U")],
        props(&[("k", PropertyValue::Integer(1))]),
    )
    .unwrap();
    e.execute_mut("MATCH (n:U {k: 1}) DELETE n", &mut s, "default")
        .unwrap();
    s.try_create_node_with_properties(
        "default",
        vec![Label::new("U")],
        props(&[("k", PropertyValue::Integer(1))]),
    )
    .expect("the deleted node no longer holds the key");
}

/// Pinned, not endorsed: the unchecked form cannot refuse, and says so in its
/// documentation. If it ever starts checking, this test should change with it.
#[test]
fn the_unchecked_create_is_documented_as_not_enforcing_the_constraint() {
    let (mut s, _) = constrained();
    for _ in 0..2 {
        s.create_node_with_properties(
            "default",
            vec![Label::new("U")],
            props(&[("k", PropertyValue::Integer(1))]),
        );
    }
    assert!(
        s.find_node_by_unique(&Label::new("U"), "k", &PropertyValue::Integer(1))
            .is_err(),
        "two holders, reported as a violation by the lookup"
    );
}

// ───────────────────────────────────────────────────────────── #1604

#[test]
fn merge_on_match_set_that_would_duplicate_a_unique_key_fails_and_changes_nothing() {
    let (mut s, e) = constrained();
    e.execute_mut(
        "CREATE (:U {id: 1, k: 10}), (:U {id: 2, k: 20})",
        &mut s,
        "default",
    )
    .unwrap();
    let err = e
        .execute_mut(
            "MERGE (n:U {id: 1}) ON MATCH SET n.k = 20 RETURN n",
            &mut s,
            "default",
        )
        .expect_err("k = 20 is held by the other node");
    assert!(err.to_string().contains(":U(k)"), "{err}");
    assert_eq!(
        count(&e, &s, "MATCH (n:U {id: 1, k: 10}) RETURN count(n)"),
        1
    );
}

#[test]
fn merge_on_create_set_that_would_duplicate_a_unique_key_leaves_no_node() {
    let (mut s, e) = constrained();
    e.execute_mut("CREATE (:U {id: 1, k: 10})", &mut s, "default")
        .unwrap();
    let err = e
        .execute_mut(
            "MERGE (n:U {id: 2}) ON CREATE SET n.k = 10 RETURN n",
            &mut s,
            "default",
        )
        .expect_err("k = 10 is held");
    assert!(err.to_string().contains(":U(k)"), "{err}");
    assert_eq!(
        count(&e, &s, "MATCH (n:U) RETURN count(n)"),
        1,
        "the failed statement is undone whole, the created node with it"
    );
}

#[test]
fn a_refused_relationship_property_write_in_merge_fails_the_statement() {
    let (mut s, e) = constrained();
    e.execute_mut("CREATE (:A {id: 1}), (:B {id: 2})", &mut s, "default")
        .unwrap();
    // A list of maps is not a storable property; the store refuses it.
    let err = e
        .execute_mut(
            "MATCH (a:A), (b:B) MERGE (a)-[r:R]->(b) ON CREATE SET r.p = [{x: 1}] RETURN r",
            &mut s,
            "default",
        )
        .expect_err("the refused write must fail the statement");
    assert!(err.to_string().contains("InvalidPropertyType"), "{err}");
    assert_eq!(count(&e, &s, "MATCH ()-[r:R]->() RETURN count(r)"), 0);

    e.execute_mut("MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)", &mut s, "default")
        .unwrap();
    let err = e
        .execute_mut(
            "MATCH (a:A), (b:B) MERGE (a)-[r:R]->(b) ON MATCH SET r.p = [{x: 1}] RETURN r",
            &mut s,
            "default",
        )
        .expect_err("ON MATCH too");
    assert!(err.to_string().contains("InvalidPropertyType"), "{err}");
}

#[test]
fn a_snapshot_whose_merged_label_would_duplicate_a_key_is_refused_and_undone() {
    // The target already holds :U(k)=1 on node A. The snapshot's node merges,
    // by `name`, into B, and would make B a second :U holding k=1.
    let (mut target, e) = constrained();
    e.execute_mut(
        "CREATE (:U {name: 'a', k: 1}), (:P {name: 'b', k: 1})",
        &mut target,
        "default",
    )
    .unwrap();

    let mut source = GraphStore::new();
    source.create_node_with_properties(
        "default",
        vec![Label::new("P"), Label::new("U")],
        props(&[
            ("name", PropertyValue::String("b".into())),
            ("k", PropertyValue::Integer(1)),
            ("extra", PropertyValue::String("from the snapshot".into())),
        ]),
    );
    let mut bytes = Vec::new();
    samyama::snapshot::export_tenant(&source, &mut bytes).unwrap();

    let nodes_before = target.node_count();
    let err = samyama::snapshot::import_tenant_with_dedup(&mut target, &bytes[..], &["name"])
        .expect_err("the label would admit a duplicate key, so the import is refused");
    assert!(err.to_string().contains("label :U refused"), "{err}");
    assert_eq!(target.node_count(), nodes_before);
    assert_eq!(
        count(&e, &target, "MATCH (n:U) RETURN count(n)"),
        1,
        "B did not gain :U"
    );
    assert_eq!(
        count(
            &e,
            &target,
            "MATCH (n:P {name: 'b'}) WHERE n.extra IS NULL RETURN count(n)"
        ),
        1,
        "and the property the merge added to B is undone"
    );
}

// ───────────────────────────────────────────────────────────── #1605

#[test]
fn a_node_that_lost_the_label_is_not_returned_by_that_labels_vector_index() {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    s.create_vector_index("Doc", "v", 2, DistanceMetric::Cosine)
        .unwrap();
    let mut ids = Vec::new();
    for (i, v) in [[1.0f32, 0.0], [0.9, 0.1], [0.0, 1.0]].iter().enumerate() {
        ids.push(s.create_node_with_properties(
            "default",
            vec![Label::new("Doc")],
            props(&[
                ("i", PropertyValue::Integer(i as i64)),
                ("v", PropertyValue::Vector(v.to_vec())),
            ]),
        ));
    }
    let nearest = s.vector_search("Doc", "v", &[1.0, 0.0], 1).unwrap();
    assert_eq!(nearest[0].0, ids[0]);

    e.execute_mut("MATCH (n:Doc {i: 0}) REMOVE n:Doc", &mut s, "default")
        .unwrap();
    let after = s.vector_search("Doc", "v", &[1.0, 0.0], 2).unwrap();
    let returned: Vec<_> = after.iter().map(|(id, _)| *id).collect();
    assert!(!returned.contains(&ids[0]), "{returned:?}");
    assert_eq!(
        after.len(),
        2,
        "k results still, from the nodes that remain"
    );
    assert!(!s
        .vector_search_all(&[1.0, 0.0], 3)
        .unwrap()
        .iter()
        .any(|(id, _)| *id == ids[0]));

    // Regaining the label brings it back, once.
    e.execute_mut("MATCH (n {i: 0}) SET n:Doc", &mut s, "default")
        .unwrap();
    let back = s.vector_search("Doc", "v", &[1.0, 0.0], 3).unwrap();
    assert_eq!(back[0].0, ids[0]);
    assert_eq!(back.iter().filter(|(id, _)| *id == ids[0]).count(), 1);

    // A deleted node is not returned either.
    e.execute_mut("MATCH (n:Doc {i: 0}) DETACH DELETE n", &mut s, "default")
        .unwrap();
    assert!(!s
        .vector_search("Doc", "v", &[1.0, 0.0], 3)
        .unwrap()
        .iter()
        .any(|(id, _)| *id == ids[0]));
}

#[test]
fn catalog_label_counts_equal_a_label_scan_after_label_churn() {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    for i in 0..20 {
        e.execute_mut(&format!("CREATE (:A {{i: {i}}})"), &mut s, "default")
            .unwrap();
    }
    // A deterministic pseudo-random sequence of SET and REMOVE, including
    // adding a label a node already has.
    let mut x: u64 = 0x9e3779b97f4a7c15;
    for _ in 0..300 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let i = x % 20;
        let label = ["A", "B", "C"][((x >> 8) % 3) as usize];
        let q = if (x >> 16) % 2 == 0 {
            format!("MATCH (n {{i: {i}}}) SET n:{label}")
        } else {
            format!("MATCH (n {{i: {i}}}) REMOVE n:{label}")
        };
        e.execute_mut(&q, &mut s, "default").unwrap();
    }
    for label in ["A", "B", "C"] {
        let scanned = count(&e, &s, &format!("MATCH (n:{label}) RETURN count(n)"));
        let counted = s
            .catalog()
            .label_counts
            .get(&Label::new(label))
            .copied()
            .unwrap_or(0);
        assert_eq!(counted as i64, scanned, ":{label}");
    }
}
