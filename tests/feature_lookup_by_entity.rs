//! Fetching a materialized feature vector by entity key (ML-13, #1768).
//!
//! ```cypher
//! CALL feature.lookup(42, ['spend', 'orders']) YIELD node, features, values
//! MATCH (c:Customer) RETURN c.name, feature.value('spend', c)
//! MATCH (c:Customer) RETURN c.name, feature.vector(['spend', 'orders'], c)
//! ```
//!
//! `feature.lookup`, `feature.serve` and friends used to be refused as unknown
//! procedures. These read the values `feature.materialize` (ML-07, #1742)
//! kept; they compute nothing. This file checks that the vector comes back in
//! the order asked for, that the per-row functions agree with `feature.get`,
//! that an entity with no value reads null while a typo is an error, and that
//! a deleted node's value is never handed to the node that reuses its id.

use std::collections::BTreeMap;

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::{QueryEngine, RecordBatch};

const T: &str = "default";

/// `c1` placed orders of 10, 20 and 30; `c2` one of 5; `c3` none.
fn fixture() -> (GraphStore, QueryEngine) {
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    for (name, amounts) in [("c1", vec![10, 20, 30]), ("c2", vec![5]), ("c3", vec![])] {
        let c = s.create_node_with_labels([Label::new("Customer")]);
        s.set_node_property(T, c, "name", PropertyValue::String(name.into()))
            .unwrap();
        for amount in amounts {
            let o = s.create_node_with_labels([Label::new("Order")]);
            s.set_node_property(T, o, "amount", PropertyValue::Integer(amount))
                .unwrap();
            s.create_edge(c, o, "PLACED").unwrap();
        }
    }
    for q in [
        "CALL feature.define('spend', {label: 'Customer', edgeType: 'PLACED', \
         aggregate: 'sum', property: 'amount'})",
        "CALL feature.define('orders', {label: 'Customer', edgeType: 'PLACED', \
         aggregate: 'count'})",
        "CALL feature.materialize('spend')",
        "CALL feature.materialize('orders')",
    ] {
        engine
            .execute_mut(q, &mut s, T)
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    (s, engine)
}

fn int(i: i64) -> PropertyValue {
    PropertyValue::Integer(i)
}

fn cell(r: &RecordBatch, row: usize, col: &str) -> PropertyValue {
    match r.records[row].get(col) {
        Some(v) => v.as_property().cloned().unwrap_or(PropertyValue::Null),
        None => panic!("no column {col}"),
    }
}

fn list(r: &RecordBatch, row: usize, col: &str) -> Vec<PropertyValue> {
    match r.records[row].get(col) {
        Some(samyama::query::executor::Value::List(items)) => items
            .iter()
            .map(|v| v.as_property().cloned().unwrap_or(PropertyValue::Null))
            .collect(),
        Some(v) => match v.as_property() {
            Some(PropertyValue::Array(items)) => items.clone(),
            other => panic!("{col} was {other:?}"),
        },
        None => panic!("no column {col}"),
    }
}

fn id_of(s: &GraphStore, engine: &QueryEngine, name: &str) -> i64 {
    let r = engine
        .execute(
            &format!("MATCH (c:Customer {{name: '{name}'}}) RETURN id(c) AS id"),
            s,
        )
        .unwrap();
    match cell(&r, 0, "id") {
        PropertyValue::Integer(i) => i,
        other => panic!("id was {other:?}"),
    }
}

fn err(s: &GraphStore, engine: &QueryEngine, q: &str) -> String {
    engine.execute(q, s).map(|_| ()).expect_err(q).to_string()
}

#[test]
fn lookup_returns_the_feature_vector_in_the_order_named() {
    let (s, engine) = fixture();
    let c1 = id_of(&s, &engine, "c1");
    for (names, expect) in [
        ("['spend', 'orders']", vec![int(60), int(3)]),
        ("['orders', 'spend']", vec![int(3), int(60)]),
        ("'spend'", vec![int(60)]),
    ] {
        let q = format!(
            "CALL feature.lookup({c1}, {names}) YIELD node, features, values \
             RETURN node.name AS name, features, values"
        );
        let r = engine
            .execute(&q, &s)
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        assert_eq!(r.records.len(), 1, "{q}");
        assert_eq!(cell(&r, 0, "name"), PropertyValue::String("c1".into()));
        assert_eq!(list(&r, 0, "values"), expect, "{q}");
        assert_eq!(list(&r, 0, "features").len(), expect.len());
    }
}

#[test]
fn lookup_takes_the_entity_as_a_parameter() {
    // How a serving client calls it: the key is bound, not spliced in.
    let (s, engine) = fixture();
    let c2 = id_of(&s, &engine, "c2");
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), int(c2));
    let r = engine
        .execute_with_params(
            "CALL feature.lookup($id, ['spend', 'orders']) YIELD values RETURN values",
            &s,
            &params,
        )
        .unwrap();
    assert_eq!(list(&r, 0, "values"), vec![int(5), int(1)]);
}

#[test]
fn the_per_row_functions_agree_with_feature_get() {
    let (s, engine) = fixture();
    let by_get: BTreeMap<String, PropertyValue> = {
        let r = engine
            .execute(
                "CALL feature.get('spend') YIELD node, value RETURN node.name AS name, value",
                &s,
            )
            .unwrap();
        (0..r.records.len())
            .map(|i| match cell(&r, i, "name") {
                PropertyValue::String(n) => (n, cell(&r, i, "value")),
                other => panic!("{other:?}"),
            })
            .collect()
    };
    let r = engine
        .execute(
            "MATCH (c:Customer) RETURN c.name AS name, feature.value('spend', c) AS value, \
             feature.vector(['spend', 'orders'], c) AS vec ORDER BY name",
            &s,
        )
        .unwrap();
    assert_eq!(r.records.len(), 3);
    let expected_vec = [
        vec![int(60), int(3)],
        vec![int(5), int(1)],
        vec![int(0), int(0)],
    ];
    for (i, want) in expected_vec.iter().enumerate() {
        let PropertyValue::String(name) = cell(&r, i, "name") else {
            panic!()
        };
        assert_eq!(cell(&r, i, "value"), by_get[&name], "{name}");
        assert_eq!(&list(&r, i, "vec"), want, "{name}");
    }
}

#[test]
fn the_function_takes_a_node_id_as_well_as_a_node() {
    let (s, engine) = fixture();
    let c1 = id_of(&s, &engine, "c1");
    let r = engine
        .execute(&format!("RETURN feature.value('spend', {c1}) AS v"), &s)
        .unwrap();
    assert_eq!(cell(&r, 0, "v"), int(60));
}

#[test]
fn an_entity_without_a_value_reads_null_and_a_missing_node_gives_no_row() {
    let (s, engine) = fixture();
    // An Order is not a Customer, so `spend` has no value for it.
    let r = engine
        .execute(
            "MATCH (o:Order) RETURN feature.value('spend', o) AS v LIMIT 1",
            &s,
        )
        .unwrap();
    assert_eq!(cell(&r, 0, "v"), PropertyValue::Null);
    // OPTIONAL MATCH that matched nothing: null in, null out.
    let r = engine
        .execute(
            "OPTIONAL MATCH (x:Nobody) RETURN feature.value('spend', x) AS v",
            &s,
        )
        .unwrap();
    assert_eq!(cell(&r, 0, "v"), PropertyValue::Null);
    // No node with that id at all: no row, as MATCH on the id would give.
    let r = engine
        .execute(
            "CALL feature.lookup(999999, ['spend']) YIELD values RETURN values",
            &s,
        )
        .unwrap();
    assert!(r.records.is_empty(), "{:?}", r.records);
}

#[test]
fn a_deleted_nodes_value_is_not_given_to_the_node_that_reuses_its_id() {
    let (mut s, engine) = fixture();
    let c1 = id_of(&s, &engine, "c1");
    engine
        .execute_mut("MATCH (c:Customer {name: 'c1'}) DETACH DELETE c", &mut s, T)
        .unwrap();
    engine
        .execute_mut("CREATE (:Customer {name: 'newcomer'})", &mut s, T)
        .unwrap();
    // The premise: the store handed the deleted id to the new node.
    assert_eq!(
        id_of(&s, &engine, "newcomer"),
        c1,
        "the store did not reuse the id, so this test no longer tests reuse"
    );

    let r = engine
        .execute(
            "MATCH (c:Customer {name: 'newcomer'}) RETURN feature.value('spend', c) AS v, \
             feature.vector(['spend', 'orders'], c) AS vec",
            &s,
        )
        .unwrap();
    assert_eq!(
        cell(&r, 0, "v"),
        PropertyValue::Null,
        "c1's 60 leaked onto the newcomer"
    );
    assert_eq!(
        list(&r, 0, "vec"),
        vec![PropertyValue::Null, PropertyValue::Null]
    );
    let r = engine
        .execute(
            &format!("CALL feature.lookup({c1}, ['spend']) YIELD values RETURN values"),
            &s,
        )
        .unwrap();
    assert_eq!(list(&r, 0, "values"), vec![PropertyValue::Null]);
    let r = engine
        .execute(
            "CALL feature.get('spend') YIELD node RETURN node.name AS name ORDER BY name",
            &s,
        )
        .unwrap();
    let names: Vec<_> = (0..r.records.len()).map(|i| cell(&r, i, "name")).collect();
    assert_eq!(
        names,
        vec![
            PropertyValue::String("c2".into()),
            PropertyValue::String("c3".into())
        ],
        "feature.get still listed the newcomer under c1's value"
    );

    // Materializing again computes the newcomer's own value.
    engine
        .execute_mut("CALL feature.materialize('spend')", &mut s, T)
        .unwrap();
    let r = engine
        .execute(
            "MATCH (c:Customer {name: 'newcomer'}) RETURN feature.value('spend', c) AS v",
            &s,
        )
        .unwrap();
    assert_eq!(cell(&r, 0, "v"), int(0));
}

#[test]
fn a_bad_lookup_is_refused_with_what_to_pass() {
    let (mut s, engine) = fixture();
    engine
        .execute_mut(
            "CALL feature.define('fresh', {label: 'Customer', aggregate: 'count'})",
            &mut s,
            T,
        )
        .unwrap();
    let c1 = id_of(&s, &engine, "c1");
    for (q, expect) in [
        (
            format!("RETURN feature.value('spnd', {c1}) AS v"),
            "no feature named `spnd`",
        ),
        (
            format!("RETURN feature.value('fresh', {c1}) AS v"),
            "has not been materialized",
        ),
        (
            format!("RETURN feature.value(['spend', 'orders'], {c1}) AS v"),
            "feature.vector",
        ),
        (
            "RETURN feature.value('spend', 'c1') AS v".to_string(),
            "node or its integer id",
        ),
        (
            format!("RETURN feature.vector([], {c1}) AS v"),
            "non-empty list",
        ),
        (
            format!("CALL feature.lookup({c1}, ['spend', 'nope']) YIELD values RETURN values"),
            "no feature named `nope`",
        ),
        (
            "CALL feature.lookup(['spend']) YIELD values RETURN values".to_string(),
            "takes (entity, names)",
        ),
        (
            "CALL feature.lookup(1.5, ['spend']) YIELD values RETURN values".to_string(),
            "node or its integer id",
        ),
    ] {
        let e = err(&s, &engine, &q);
        assert!(e.contains(expect), "{q}: expected `{expect}` in: {e}");
    }
    // A typo is an error even for a node that does not exist: it must not
    // read as "no value".
    let e = err(
        &s,
        &engine,
        "CALL feature.lookup(999999, ['spnd']) YIELD values RETURN values",
    );
    assert!(e.contains("no feature named `spnd`"), "{e}");
}

#[test]
fn lookup_is_a_read() {
    let engine = QueryEngine::new();
    for q in [
        "CALL feature.lookup(1, ['spend']) YIELD values RETURN values",
        "MATCH (c) RETURN feature.value('spend', c)",
        "MATCH (c) RETURN feature.vector(['spend'], c)",
    ] {
        assert!(
            !engine.statement_is_write(q).unwrap(),
            "{q} reads; it must not take the write lock"
        );
    }
}

#[test]
fn a_cached_function_read_is_not_served_after_a_new_materialization() {
    let (mut s, engine) = fixture();
    let q = "MATCH (c:Customer {name: 'c2'}) RETURN feature.value('spend', c) AS v";
    assert_eq!(
        cell(&engine.execute_cached(q, &s).unwrap().0, 0, "v"),
        int(5)
    );
    assert!(
        engine.execute_cached(q, &s).unwrap().1,
        "second read should hit"
    );
    // Add an order through the Rust API, then materialize through the
    // registry: neither goes through the Cypher write path.
    let c2 = NodeId::new(id_of(&s, &engine, "c2") as u64);
    let o = s.create_node_with_labels([Label::new("Order")]);
    s.set_node_property(T, o, "amount", int(100)).unwrap();
    s.create_edge(c2, o, "PLACED").unwrap();
    s.features.materialize(&s, "spend", None).unwrap();
    let (r, hit) = engine.execute_cached(q, &s).unwrap();
    assert!(!hit, "served from before the materialization");
    assert_eq!(cell(&r, 0, "v"), int(105));
}
