//! Declarative feature definitions, materialized from Cypher (ML-07, #1742).
//!
//! ```cypher
//! CALL feature.define('spend', {label: 'Customer', edgeType: 'PLACED',
//!                               targetLabel: 'Order', aggregate: 'sum',
//!                               property: 'amount', timeProperty: 'ts'})
//! CALL feature.materialize('spend', {asOf: 200})
//! CALL feature.get('spend') YIELD node, value
//! ```
//!
//! `CALL feature.define(...)` used to be refused as an unknown procedure.
//! This file checks what `CH-ML-FEATURE` asks for -- define a feature,
//! materialize it, read it back -- and that a point-in-time materialization
//! ignores writes that come after `asOf`. It also checks the write routing,
//! the result cache, and that every bad option is refused with what to pass.

use std::collections::BTreeMap;

use samyama::graph::{EdgeId, GraphStore, Label, NodeId, PropertyValue};
use samyama::query::QueryEngine;

const T: &str = "default";

/// Three customers:
///
/// - `c1` placed orders of 10 (ts 100), 20 (ts 200) and 30 (ts 300), and a
///   `PLACED` edge to a `Gift` of 999 that `targetLabel: 'Order'` leaves out;
/// - `c2` placed an order of 5 (ts 150), an order of 7 whose edge has **no**
///   `ts`, and `RETURNED` an order of 1000 that `edgeType` leaves out;
/// - `c3` placed nothing.
fn fixture() -> (GraphStore, BTreeMap<&'static str, NodeId>) {
    let mut s = GraphStore::new();
    let mut ids = BTreeMap::new();
    for name in ["c1", "c2", "c3"] {
        let id = s.create_node_with_labels([Label::new("Customer")]);
        s.set_node_property(T, id, "name", PropertyValue::String(name.into()))
            .unwrap();
        ids.insert(name, id);
    }
    place(&mut s, ids["c1"], "Order", 10, Some(100), "PLACED");
    place(&mut s, ids["c1"], "Order", 20, Some(200), "PLACED");
    place(&mut s, ids["c1"], "Order", 30, Some(300), "PLACED");
    place(&mut s, ids["c1"], "Gift", 999, Some(100), "PLACED");
    place(&mut s, ids["c2"], "Order", 5, Some(150), "PLACED");
    place(&mut s, ids["c2"], "Order", 7, None, "PLACED");
    place(&mut s, ids["c2"], "Order", 1000, Some(150), "RETURNED");
    (s, ids)
}

fn place(
    s: &mut GraphStore,
    customer: NodeId,
    label: &str,
    amount: i64,
    ts: Option<i64>,
    rel: &str,
) -> EdgeId {
    let order = s.create_node_with_labels([Label::new(label)]);
    s.set_node_property(T, order, "amount", PropertyValue::Integer(amount))
        .unwrap();
    let e = s.create_edge(customer, order, rel).unwrap();
    if let Some(ts) = ts {
        s.set_edge_property(e, "ts", PropertyValue::Integer(ts))
            .unwrap();
    }
    e
}

fn write(engine: &QueryEngine, s: &mut GraphStore, q: &str) -> Result<(), String> {
    engine
        .execute_mut(q, s, T)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn ok(engine: &QueryEngine, s: &mut GraphStore, q: &str) {
    write(engine, s, q).unwrap_or_else(|e| panic!("{q}: {e}"));
}

/// `feature.get` as `name -> value`, read through the read path.
fn get(engine: &QueryEngine, s: &GraphStore, feature: &str) -> BTreeMap<String, PropertyValue> {
    let q =
        format!("CALL feature.get('{feature}') YIELD node, value RETURN node.name AS name, value");
    let r = engine.execute(&q, s).unwrap_or_else(|e| panic!("{q}: {e}"));
    rows(&r)
}

fn rows(r: &samyama::query::RecordBatch) -> BTreeMap<String, PropertyValue> {
    r.records
        .iter()
        .map(|rec| {
            let name = match rec.get("name").and_then(|v| v.as_property()) {
                Some(PropertyValue::String(s)) => s.clone(),
                other => panic!("name was {other:?}"),
            };
            // A null cell can come back as `Value::Null` or as a null property.
            let value = rec
                .get("value")
                .and_then(|v| v.as_property())
                .cloned()
                .unwrap_or(PropertyValue::Null);
            (name, value)
        })
        .collect()
}

fn int(i: i64) -> PropertyValue {
    PropertyValue::Integer(i)
}

const SPEND: &str = "CALL feature.define('spend', {label: 'Customer', edgeType: 'PLACED', \
    targetLabel: 'Order', aggregate: 'sum', property: 'amount', timeProperty: 'ts'})";

#[test]
fn a_feature_is_defined_materialized_and_read_back() {
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    ok(&engine, &mut s, SPEND);
    ok(&engine, &mut s, "CALL feature.materialize('spend')");
    let got = get(&engine, &s, "spend");
    // c1: 10 + 20 + 30, the Gift excluded by targetLabel.
    // c2: 5 + 7, the RETURNED 1000 excluded by edgeType.
    // c3: no orders, and sum() of nothing is 0.
    assert_eq!(got["c1"], int(60));
    assert_eq!(got["c2"], int(12));
    assert_eq!(got["c3"], int(0));
    assert_eq!(
        got.len(),
        3,
        "one row per Customer, including the one with no match"
    );
}

#[test]
fn materialize_reports_the_entities_it_computed() {
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    ok(&engine, &mut s, SPEND);
    let r = engine
        .execute_mut(
            "CALL feature.materialize('spend', {asOf: 200}) YIELD name, entities, asOf \
             RETURN name, entities, asOf",
            &mut s,
            T,
        )
        .unwrap();
    assert_eq!(r.records.len(), 1);
    let rec = &r.records[0];
    assert_eq!(
        rec.get("entities").and_then(|v| v.as_property()),
        Some(&int(3))
    );
    assert_eq!(
        rec.get("asOf").and_then(|v| v.as_property()),
        Some(&int(200))
    );
}

#[test]
fn a_point_in_time_materialization_ignores_later_events() {
    let (mut s, ids) = fixture();
    let engine = QueryEngine::new();
    ok(&engine, &mut s, SPEND);
    ok(
        &engine,
        &mut s,
        "CALL feature.materialize('spend', {asOf: 200})",
    );
    let before = get(&engine, &s, "spend");
    // c1: 10 (ts 100) + 20 (ts 200, at asOf counts); 30 (ts 300) is after it.
    // c2: 5 (ts 150); the 7 has no ts, so it cannot be shown to precede asOf.
    assert_eq!(before["c1"], int(30));
    assert_eq!(before["c2"], int(5));
    assert_eq!(before["c3"], int(0));

    // Writes after asOf: a later order for c1 and a first one for c3.
    place(&mut s, ids["c1"], "Order", 500, Some(400), "PLACED");
    place(&mut s, ids["c3"], "Order", 50, Some(250), "PLACED");
    ok(
        &engine,
        &mut s,
        "CALL feature.materialize('spend', {asOf: 200})",
    );
    assert_eq!(
        get(&engine, &s, "spend"),
        before,
        "materializing as of 200 again must not see events stamped after 200"
    );

    // The same writes are seen once asOf is past them, and without asOf.
    ok(
        &engine,
        &mut s,
        "CALL feature.materialize('spend', {asOf: 400})",
    );
    let later = get(&engine, &s, "spend");
    assert_eq!(later["c1"], int(10 + 20 + 30 + 500));
    assert_eq!(later["c3"], int(50));
    ok(&engine, &mut s, "CALL feature.materialize('spend')");
    assert_eq!(
        get(&engine, &s, "spend")["c2"],
        int(12),
        "no asOf counts the undated order"
    );
}

#[test]
fn time_can_be_read_from_the_neighbour() {
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    let c = s.create_node_with_labels([Label::new("Customer")]);
    s.set_node_property(T, c, "name", PropertyValue::String("c".into()))
        .unwrap();
    for (amount, day) in [(1, 10), (2, 20), (4, 30)] {
        let o = s.create_node_with_labels([Label::new("Order")]);
        s.set_node_property(T, o, "amount", int(amount)).unwrap();
        s.set_node_property(T, o, "day", int(day)).unwrap();
        s.create_edge(c, o, "PLACED").unwrap();
    }
    ok(
        &engine,
        &mut s,
        "CALL feature.define('f', {label: 'Customer', aggregate: 'sum', property: 'amount', \
         timeProperty: 'day', timeOn: 'node'})",
    );
    ok(&engine, &mut s, "CALL feature.materialize('f', {asOf: 20})");
    assert_eq!(get(&engine, &s, "f")["c"], int(3));
}

#[test]
fn every_aggregate_matches_what_cypher_computes() {
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    for agg in ["count", "sum", "avg", "min", "max"] {
        let property = if agg == "count" {
            ""
        } else {
            ", property: 'amount'"
        };
        ok(
            &engine,
            &mut s,
            &format!(
                "CALL feature.define('{agg}', {{label: 'Customer', edgeType: 'PLACED', \
                 targetLabel: 'Order', aggregate: '{agg}'{property}}})"
            ),
        );
        ok(
            &engine,
            &mut s,
            &format!("CALL feature.materialize('{agg}')"),
        );
        let ours = get(&engine, &s, agg);

        // The same question asked as a plain Cypher aggregation.
        let expr = if agg == "count" {
            "count(o)".to_string()
        } else {
            format!("{agg}(o.amount)")
        };
        let q = format!(
            "MATCH (c:Customer) OPTIONAL MATCH (c)-[:PLACED]->(o:Order) \
             RETURN c.name AS name, {expr} AS value"
        );
        let cypher = rows(&engine.execute(&q, &s).unwrap());
        assert_eq!(ours, cypher, "feature `{agg}` disagrees with `{q}`");
    }
}

#[test]
fn direction_in_aggregates_over_incoming_relationships() {
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    ok(
        &engine,
        &mut s,
        "CALL feature.define('buyers', {label: 'Order', edgeType: 'PLACED', direction: 'in', \
         aggregate: 'count'})",
    );
    let r = engine
        .execute_mut(
            "CALL feature.materialize('buyers') YIELD entities RETURN entities",
            &mut s,
            T,
        )
        .unwrap();
    // Six Order nodes: five PLACED and one RETURNED. The Gift is not an Order.
    assert_eq!(
        r.records[0].get("entities").and_then(|v| v.as_property()),
        Some(&int(6))
    );
    let q = "CALL feature.get('buyers') YIELD value RETURN value, count(*) AS n";
    let r = engine.execute(q, &s).unwrap();
    let mut by_value: Vec<(PropertyValue, PropertyValue)> = r
        .records
        .iter()
        .map(|rec| {
            (
                rec.get("value")
                    .and_then(|v| v.as_property())
                    .cloned()
                    .unwrap(),
                rec.get("n").and_then(|v| v.as_property()).cloned().unwrap(),
            )
        })
        .collect();
    by_value.sort_by(|a, b| a.0.cmp(&b.0));
    // Five orders have one PLACED buyer; the RETURNED one has none.
    assert_eq!(by_value, vec![(int(0), int(1)), (int(1), int(5))]);
}

#[test]
fn the_calls_that_change_the_registry_take_the_write_path() {
    let engine = QueryEngine::new();
    for q in [
        "CALL feature.define('f', {label: 'A', aggregate: 'count'})",
        "CALL feature.materialize('f')",
        "CALL feature.drop('f')",
    ] {
        assert!(
            engine.statement_is_write(q).unwrap(),
            "{q} must route to the write executor"
        );
    }
    for q in [
        "CALL feature.get('f') YIELD node RETURN node",
        "CALL feature.list()",
    ] {
        assert!(
            !engine.statement_is_write(q).unwrap(),
            "{q} reads; it must not take the write lock"
        );
    }

    // A read executor handed a define directly refuses rather than writing.
    let s = GraphStore::new();
    let err = engine
        .execute(
            "CALL feature.define('f', {label: 'A', aggregate: 'count'})",
            &s,
        )
        .unwrap_err()
        .to_string();
    assert!(
        s.features.list(0).is_empty(),
        "the read path defined a feature: {err}"
    );
}

#[test]
fn a_cached_read_is_not_served_after_a_new_materialization() {
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    ok(&engine, &mut s, SPEND);
    ok(
        &engine,
        &mut s,
        "CALL feature.materialize('spend', {asOf: 200})",
    );
    let q = "CALL feature.get('spend') YIELD node, value RETURN node.name AS name, value";
    let (first, _) = engine.execute_cached(q, &s).unwrap();
    let (again, hit) = engine.execute_cached(q, &s).unwrap();
    assert!(hit, "the second identical read should be a cache hit");
    assert_eq!(rows(&first), rows(&again));

    // Materializing again changes the answer without changing any data
    // the epoch tracks; the cache must not serve the old one.
    ok(
        &engine,
        &mut s,
        "CALL feature.materialize('spend', {asOf: 400})",
    );
    let (after, hit) = engine.execute_cached(q, &s).unwrap();
    assert!(
        !hit,
        "a read after a new materialization was served from the cache"
    );
    assert_eq!(rows(&after)["c1"], int(60));
}

#[test]
fn a_cached_read_is_not_served_after_materializing_through_the_rust_api() {
    // Through Cypher the write path already moves the store's epoch, so the
    // test above passes on the epoch alone. The registry is also public Rust
    // API, and a materialization made there moves no epoch: only the feature
    // generation in the cache key keeps the old answer from being served.
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    ok(&engine, &mut s, SPEND);
    ok(
        &engine,
        &mut s,
        "CALL feature.materialize('spend', {asOf: 200})",
    );
    let q = "CALL feature.get('spend') YIELD node, value RETURN node.name AS name, value";
    let (first, _) = engine.execute_cached(q, &s).unwrap();
    assert_eq!(rows(&first)["c1"], int(30));
    let epoch = s.epoch();
    s.features.materialize(&s, "spend", Some(int(400))).unwrap();
    assert_eq!(
        s.epoch(),
        epoch,
        "the premise: no data changed, so the epoch did not move"
    );
    let (after, hit) = engine.execute_cached(q, &s).unwrap();
    assert!(
        !hit,
        "a read after a new materialization was served from the cache"
    );
    assert_eq!(rows(&after)["c1"], int(60));
}

#[test]
fn list_reports_each_feature_and_whether_it_is_stale() {
    let (mut s, ids) = fixture();
    let engine = QueryEngine::new();
    ok(&engine, &mut s, SPEND);
    let list = |s: &GraphStore| {
        let r = engine
            .execute(
                "CALL feature.list() YIELD name, pattern, entities, stale \
                 RETURN name, pattern, entities, stale",
                s,
            )
            .unwrap();
        assert_eq!(r.records.len(), 1);
        let rec = &r.records[0];
        let cell = |k: &str| rec.get(k).and_then(|v| v.as_property()).cloned();
        (cell("pattern"), cell("entities"), cell("stale"))
    };
    let (pattern, entities, stale) = list(&s);
    assert_eq!(
        pattern,
        Some(PropertyValue::String(
            "(n:Customer)-[:PLACED]->(m:Order) -> sum(m.amount)".into()
        ))
    );
    assert_eq!(
        entities, None,
        "never materialized, so no entity count: {entities:?}"
    );
    assert_eq!(stale, Some(PropertyValue::Boolean(true)));

    ok(&engine, &mut s, "CALL feature.materialize('spend')");
    assert_eq!(list(&s).1, Some(int(3)));
    assert_eq!(list(&s).2, Some(PropertyValue::Boolean(false)));

    place(&mut s, ids["c3"], "Order", 1, Some(1), "PLACED");
    assert_eq!(
        list(&s).2,
        Some(PropertyValue::Boolean(true)),
        "a write since must read as stale"
    );
}

#[test]
fn a_redefinition_discards_the_old_values_and_drop_removes_the_feature() {
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    ok(&engine, &mut s, SPEND);
    ok(&engine, &mut s, "CALL feature.materialize('spend')");
    let r = engine
        .execute_mut(
            "CALL feature.define('spend', {label: 'Customer', aggregate: 'count'}) \
             YIELD replaced RETURN replaced",
            &mut s,
            T,
        )
        .unwrap();
    assert_eq!(
        r.records[0].get("replaced").and_then(|v| v.as_property()),
        Some(&PropertyValue::Boolean(true))
    );
    let err = engine
        .execute("CALL feature.get('spend') YIELD node RETURN node", &s)
        .unwrap_err()
        .to_string();
    assert!(err.contains("has not been materialized"), "{err}");

    ok(&engine, &mut s, "CALL feature.drop('spend')");
    let listed = engine
        .execute("CALL feature.list()", &s)
        .map(|r| r.records.len());
    assert_eq!(listed.unwrap(), 0);
    let err = write(&engine, &mut s, "CALL feature.materialize('spend')").unwrap_err();
    assert!(err.contains("no feature named `spend`"), "{err}");
}

#[test]
fn a_bad_definition_is_refused_with_what_to_pass() {
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    for (config, expect) in [
        ("{aggregate: 'count'}", "`label` is required"),
        ("{label: 'Customer'}", "`aggregate` is required"),
        (
            "{label: 'Customer', aggregate: 'median', property: 'x'}",
            "unknown aggregate `median`",
        ),
        ("{label: 'Customer', aggregate: 'sum'}", "needs `property`"),
        (
            "{label: 'Customer', aggregate: 'count', property: 'amount'}",
            "reads no property",
        ),
        (
            "{label: 'Customer', aggregate: 'count', direction: 'up'}",
            "unknown direction `up`",
        ),
        (
            "{label: 'Customer', aggregate: 'count', timeOn: 'node'}",
            "there is no `timeProperty`",
        ),
        (
            "{label: 'Customer', aggregate: 'count', window: 3}",
            "unknown key `window`",
        ),
    ] {
        let q = format!("CALL feature.define('f', {config})");
        let err = write(&engine, &mut s, &q).expect_err(&q);
        assert!(err.contains(expect), "{q}: expected `{expect}` in: {err}");
    }
    assert!(
        s.features.list(0).is_empty(),
        "a refused definition was kept"
    );

    let err = write(
        &engine,
        &mut s,
        "CALL feature.define('', {label: 'A', aggregate: 'count'})",
    )
    .unwrap_err();
    assert!(err.contains("non-empty"), "{err}");
}

#[test]
fn a_bad_materialization_is_refused_with_what_to_pass() {
    let (mut s, _) = fixture();
    let engine = QueryEngine::new();
    ok(&engine, &mut s, SPEND);
    ok(
        &engine,
        &mut s,
        "CALL feature.define('n', {label: 'Customer', aggregate: 'count'})",
    );

    let err = write(&engine, &mut s, "CALL feature.materialize('n', {asOf: 5})").unwrap_err();
    assert!(err.contains("without `timeProperty`"), "{err}");

    // `ts` is an integer; a string asOf is refused rather than compared.
    let err = write(
        &engine,
        &mut s,
        "CALL feature.materialize('spend', {asOf: '2026-01-01'})",
    )
    .unwrap_err();
    assert!(err.contains("cannot be compared"), "{err}");

    let err = write(
        &engine,
        &mut s,
        "CALL feature.materialize('spend', {as_of: 5})",
    )
    .unwrap_err();
    assert!(err.contains("unknown key `as_of`"), "{err}");

    let err = write(&engine, &mut s, "CALL feature.materialize('nope')").unwrap_err();
    assert!(err.contains("no feature named `nope`"), "{err}");

    // A non-numeric property is refused for sum, naming the problem.
    let c = s.create_node_with_labels([Label::new("Customer")]);
    let o = s.create_node_with_labels([Label::new("Order")]);
    s.set_node_property(T, o, "amount", PropertyValue::String("ten".into()))
        .unwrap();
    s.create_edge(c, o, "PLACED").unwrap();
    let err = write(&engine, &mut s, "CALL feature.materialize('spend')").unwrap_err();
    assert!(err.contains("needs numbers"), "{err}");
}

#[test]
fn a_misspelt_feature_procedure_suggests_the_right_one() {
    let s = GraphStore::new();
    let err = QueryEngine::new()
        .execute("CALL feature.lst()", &s)
        .unwrap_err()
        .to_string();
    assert!(err.contains("Did you mean `feature.list`"), "{err}");
}
