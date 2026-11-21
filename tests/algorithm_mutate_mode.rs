//! Mutate mode: an algorithm keeps its result in a graph projection (ALGO-06, #2263).
//!
//! ```cypher
//! CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS
//! CALL algo.pageRank({graph: 'social', mutateProperty: 'rank'}) YIELD node, score
//! CALL gds.pageRank.mutate('social', {mutateProperty: 'rank'}) YIELD nodePropertiesWritten
//! CALL algo.graph.nodeProperty('social', 'rank') YIELD node, value
//! ```
//!
//! ALGO-06 asks for three modes. `stream` and `write` (`writeProperty`, #1683)
//! existed; `mutate` was refused because "a projection here cannot hold a
//! written property yet". The property that matters: mutate keeps exactly the
//! values the stream returns, keeps them **only** in the projection (the
//! store's nodes are untouched), and a refresh or drop discards them with the
//! view they were computed on.

use std::collections::BTreeMap;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{Record, Value};
use samyama::query::QueryEngine;

const TENANT: &str = "default";

/// Two triangles of people joined by a bridge, and a robot outside the
/// projection.
const GRAPH: &str = "CREATE \
     (a:Person {name: 'a'}), (b:Person {name: 'b'}), (c:Person {name: 'c'}), \
     (d:Person {name: 'd'}), (e:Person {name: 'e'}), (f:Person {name: 'f'}), \
     (r:Robot {name: 'r'}), \
     (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
     (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
     (c)-[:KNOWS]->(d), (r)-[:OWNS]->(a)";

const PROJECT: &str = "CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS";

fn graph() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in [GRAPH, PROJECT] {
        engine
            .execute_mut(q, &mut store, TENANT)
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    (store, engine)
}

fn run(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> Result<Vec<Record>, String> {
    engine
        .execute_mut(q, store, TENANT)
        .map(|r| r.records)
        .map_err(|e| e.to_string())
}

fn ok(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> Vec<Record> {
    run(store, engine, q).unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn refused(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> String {
    run(store, engine, q).expect_err(&format!("{q} was accepted"))
}

fn cell(r: &Record, column: &str) -> PropertyValue {
    r.get(column)
        .and_then(|v| v.as_property())
        .cloned()
        .unwrap_or(PropertyValue::Null)
}

fn node_id(r: &Record) -> u64 {
    match r.get("node") {
        Some(Value::Node(id, _)) | Some(Value::NodeRef(id)) => id.as_u64(),
        other => panic!("node was {other:?}"),
    }
}

/// `node id -> column` for a set of rows.
fn by_node(rows: &[Record], column: &str) -> BTreeMap<u64, PropertyValue> {
    rows.iter().map(|r| (node_id(r), cell(r, column))).collect()
}

fn read_back(
    store: &mut GraphStore,
    engine: &QueryEngine,
    property: &str,
) -> BTreeMap<u64, PropertyValue> {
    let q = format!(
        "CALL algo.graph.nodeProperty('social', '{property}') YIELD node, value RETURN node, value"
    );
    by_node(&ok(store, engine, &q), "value")
}

#[test]
fn mutate_keeps_exactly_what_the_stream_returns() {
    let (mut s, engine) = graph();
    let streamed = by_node(
        &ok(
            &mut s,
            &engine,
            "CALL algo.pageRank({graph: 'social'}) YIELD node, score RETURN node, score",
        ),
        "score",
    );
    assert_eq!(streamed.len(), 6, "six people in the projection");

    let mutated = ok(
        &mut s,
        &engine,
        "CALL algo.pageRank({graph: 'social', mutateProperty: 'rank'}) YIELD node, score \
         RETURN node, score",
    );
    // The call still streams its rows, as writeProperty does.
    assert_eq!(by_node(&mutated, "score"), streamed);
    // And the projection now holds the same values.
    assert_eq!(read_back(&mut s, &engine, "rank"), streamed);
}

#[test]
fn mutate_does_not_touch_the_nodes_in_the_store() {
    let (mut s, engine) = graph();
    ok(
        &mut s,
        &engine,
        "CALL algo.pageRank({graph: 'social', mutateProperty: 'rank'})",
    );
    let rows = ok(
        &mut s,
        &engine,
        "MATCH (n) WHERE n.rank IS NOT NULL RETURN count(n) AS n",
    );
    assert_eq!(
        cell(&rows[0], "n"),
        PropertyValue::Integer(0),
        "mutate wrote to the store"
    );

    // writeProperty, by contrast, does write the nodes: the two modes differ.
    ok(
        &mut s,
        &engine,
        "CALL algo.pageRank({graph: 'social', writeProperty: 'rank'})",
    );
    let rows = ok(
        &mut s,
        &engine,
        "MATCH (n) WHERE n.rank IS NOT NULL RETURN count(n) AS n",
    );
    assert_eq!(cell(&rows[0], "n"), PropertyValue::Integer(6));
}

#[test]
fn the_gds_spelling_mutates_and_answers_with_a_summary_row() {
    let (mut s, engine) = graph();
    let rows = ok(
        &mut s,
        &engine,
        "CALL gds.pageRank.mutate('social', {mutateProperty: 'pr'}) \
         YIELD nodePropertiesWritten, mutateMillis RETURN nodePropertiesWritten, mutateMillis",
    );
    assert_eq!(rows.len(), 1, "GDS's mutate answers with one summary row");
    assert_eq!(
        cell(&rows[0], "nodePropertiesWritten"),
        PropertyValue::Integer(6)
    );
    assert!(matches!(cell(&rows[0], "mutateMillis"), PropertyValue::Integer(ms) if ms >= 0));

    let streamed = by_node(
        &ok(
            &mut s,
            &engine,
            "CALL algo.pageRank({graph: 'social'}) YIELD node, score RETURN node, score",
        ),
        "score",
    );
    assert_eq!(read_back(&mut s, &engine, "pr"), streamed);
}

#[test]
fn every_one_value_per_node_algorithm_can_mutate() {
    // The algorithms writeProperty accepts are the ones mutate accepts; each
    // keeps one value per node of the projection.
    let (mut s, engine) = graph();
    for (algo, column) in [
        ("degree", "score"),
        ("wcc", "componentId"),
        ("louvain", "communityId"),
        ("betweenness", "score"),
    ] {
        let q = format!(
            "CALL algo.{algo}({{graph: 'social'}}) YIELD node, {column} RETURN node, {column}"
        );
        let streamed = match run(&mut s, &engine, &q) {
            Ok(rows) => by_node(&rows, column),
            // A column name differing from the guess is a test problem, so
            // say which.
            Err(e) => panic!("{q}: {e}"),
        };
        ok(
            &mut s,
            &engine,
            &format!("CALL algo.{algo}({{graph: 'social', mutateProperty: '{algo}_out'}})"),
        );
        assert_eq!(
            read_back(&mut s, &engine, &format!("{algo}_out")),
            streamed,
            "{algo}"
        );
    }
}

#[test]
fn show_lists_what_was_mutated_and_refresh_or_drop_discards_it() {
    let (mut s, engine) = graph();
    let listed = |s: &mut GraphStore| -> Vec<PropertyValue> {
        let rows = ok(s, &engine, "SHOW GRAPH PROJECTIONS");
        match rows[0].get("node_properties") {
            Some(Value::List(items)) => items
                .iter()
                .map(|v| v.as_property().cloned().unwrap())
                .collect(),
            other => panic!("node_properties was {other:?}"),
        }
    };
    assert!(listed(&mut s).is_empty());
    ok(
        &mut s,
        &engine,
        "CALL algo.pageRank({graph: 'social', mutateProperty: 'rank'})",
    );
    ok(
        &mut s,
        &engine,
        "CALL algo.degree({graph: 'social', mutateProperty: 'deg'})",
    );
    assert_eq!(
        listed(&mut s),
        vec![
            PropertyValue::String("deg".into()),
            PropertyValue::String("rank".into())
        ]
    );

    // Refresh rebuilds the view; values computed on the old one go with it.
    ok(&mut s, &engine, "REFRESH GRAPH PROJECTION social");
    assert!(
        listed(&mut s).is_empty(),
        "refresh kept values computed on the old view"
    );
    let err = refused(
        &mut s,
        &engine,
        "CALL algo.graph.nodeProperty('social', 'rank') YIELD node RETURN node",
    );
    assert!(err.contains("has no node property `rank`"), "{err}");

    ok(
        &mut s,
        &engine,
        "CALL algo.pageRank({graph: 'social', mutateProperty: 'rank'})",
    );
    ok(&mut s, &engine, "DROP GRAPH PROJECTION social");
    ok(&mut s, &engine, PROJECT);
    assert!(
        listed(&mut s).is_empty(),
        "a re-created projection inherited the old values"
    );
}

#[test]
fn mutating_a_property_again_replaces_it() {
    let (mut s, engine) = graph();
    ok(
        &mut s,
        &engine,
        "CALL algo.pageRank({graph: 'social', mutateProperty: 'x'})",
    );
    ok(
        &mut s,
        &engine,
        "CALL algo.degree({graph: 'social', mutateProperty: 'x'})",
    );
    let degree = by_node(
        &ok(
            &mut s,
            &engine,
            "CALL algo.degree({graph: 'social'}) YIELD node, score RETURN node, score",
        ),
        "score",
    );
    assert_eq!(read_back(&mut s, &engine, "x"), degree);
}

#[test]
fn mutate_takes_the_write_path_and_the_read_back_does_not() {
    let engine = QueryEngine::new();
    for q in [
        "CALL algo.pageRank({graph: 'social', mutateProperty: 'rank'})",
        "CALL gds.pageRank.mutate('social', {mutateProperty: 'rank'})",
    ] {
        assert!(
            engine.statement_is_write(q).unwrap(),
            "{q} must route to the write executor"
        );
    }
    for q in [
        "CALL algo.pageRank({graph: 'social'}) YIELD node RETURN node",
        "CALL algo.graph.nodeProperty('social', 'rank') YIELD node, value RETURN node, value",
    ] {
        assert!(!engine.statement_is_write(q).unwrap(), "{q} reads");
    }

    // A read executor handed a mutate refuses rather than changing anything.
    let (mut s, engine) = graph();
    let err = engine
        .execute(
            "CALL algo.pageRank({graph: 'social', mutateProperty: 'rank'})",
            &s,
        )
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(!err.is_empty());
    let rows = ok(&mut s, &engine, "SHOW GRAPH PROJECTIONS");
    assert!(
        matches!(rows[0].get("node_properties"), Some(Value::List(items)) if items.is_empty()),
        "the read path mutated the projection: {err}"
    );
}

#[test]
fn the_read_back_is_cached_only_until_the_next_mutate() {
    let (mut s, engine) = graph();
    ok(
        &mut s,
        &engine,
        "CALL algo.pageRank({graph: 'social', mutateProperty: 'x'})",
    );
    let q = "CALL algo.graph.nodeProperty('social', 'x') YIELD node, value RETURN node, value";
    let (first, _) = engine.execute_cached(q, &s).unwrap();
    assert!(
        engine.execute_cached(q, &s).unwrap().1,
        "the second read should hit"
    );
    ok(
        &mut s,
        &engine,
        "CALL algo.degree({graph: 'social', mutateProperty: 'x'})",
    );
    let (after, hit) = engine.execute_cached(q, &s).unwrap();
    assert!(!hit, "a read after a mutate was served from the cache");
    assert_ne!(
        by_node(&first.records, "value"),
        by_node(&after.records, "value")
    );
}

#[test]
fn what_mutate_cannot_honour_is_refused_with_what_to_pass() {
    let (mut s, engine) = graph();
    for (q, expect) in [
        (
            "CALL algo.pageRank({mutateProperty: 'rank'})",
            "no projection is named",
        ),
        (
            "CALL algo.pageRank({graph: 'nope', mutateProperty: 'rank'})",
            "nope",
        ),
        (
            "CALL algo.pageRank({graph: 'social', mutateProperty: 'rank', writeProperty: 'rank'})",
            "pick one",
        ),
        (
            "CALL algo.pageRank({graph: 'social', mutateProperty: ''})",
            "non-empty name",
        ),
        (
            "CALL algo.pageRank({graph: 'social', mutateProperty: 3})",
            "non-empty name",
        ),
        (
            "CALL algo.bridges({graph: 'social', mutateProperty: 'b'})",
            "one value per node",
        ),
        // These write the store by design; the key must not be ignored.
        (
            "CALL algo.fastRP({graph: 'social', mutateProperty: 'e'})",
            "writes its result to the nodes",
        ),
        (
            "CALL gds.pageRank.mutate('social', {})",
            "needs `mutateProperty`",
        ),
        (
            "CALL gds.pageRank.mutate({mutateProperty: 'x'})",
            "takes (graphName",
        ),
        (
            "CALL gds.pageRank.mutate('social', {mutateProperty: 'x', graph: 'social'})",
            "first argument",
        ),
        (
            "CALL gds.pageRank.stream({graph: 'social', mutateProperty: 'x'})",
            "stream mode does not mutate",
        ),
        (
            "CALL algo.graph.nodeProperty('social') YIELD node RETURN node",
            "takes (graph, property)",
        ),
        (
            "CALL algo.graph.nodeProperty('nope', 'x') YIELD node RETURN node",
            "no graph projection named `nope`",
        ),
        (
            "CALL algo.graph.nodeProperty('social', 'never') YIELD node RETURN node",
            "none has been written",
        ),
    ] {
        let err = refused(&mut s, &engine, q);
        assert!(err.contains(expect), "{q}: expected `{expect}` in: {err}");
    }
    let rows = ok(&mut s, &engine, "SHOW GRAPH PROJECTIONS");
    assert!(
        matches!(rows[0].get("node_properties"), Some(Value::List(items)) if items.is_empty()),
        "a refused mutate kept something"
    );
}
