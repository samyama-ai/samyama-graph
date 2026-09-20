//! Every temporal primitive returns the walk behind its answer (ALGO-15).
//!
//! ALGO-15 asks for causal primitives that return a **supporting path**. Three
//! of the four returned a node and a time, or a count and an onset — all
//! useful, and none of them answers *why*, which is the whole difference
//! between "service X broke at 12:04" and "X broke at 12:04 because A reached
//! B reached X".
//!
//! The reconstruction was already there: `earliest_arrival` records
//! `parent_edge` for every node it reaches, and only `temporalShortestPath`
//! read it.
//!
//! # What is checked, and why it is not "a path came back"
//!
//! A path column that returns *some* walk is worse than none, because it looks
//! like evidence. Each test below checks the path against the row it explains:
//!
//! - it starts at the source and ends at the node the row is about;
//! - it is the walk that produced the arrival the row reports, not merely a
//!   walk that exists — the fixture has two routes to D arriving at different
//!   times, so naming the wrong one is detectable.
//!
//! The corresponding properties over the times — never decreasing, last edge
//! equal to the arrival — are checked in `temporal.rs`'s own tests, where the
//! `TemporalPath` is in hand rather than rendered into a column.

use samyama::graph::GraphStore;
use samyama::query::executor::record::Value;
use samyama::graph::PropertyValue;
use samyama::query::QueryEngine;

/// A -> B -> D and A -> C -> D, where the two routes arrive at different
/// times, so "which walk" is a real question with a checkable answer.
fn store() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in [
        "CREATE (:S {name: 'A'}), (:S {name: 'B'}), (:S {name: 'C'}), (:S {name: 'D'}), (:S {name: 'E'})",
        "MATCH (a:S {name:'A'}), (b:S {name:'B'}) CREATE (a)-[:CALLS {t: 10}]->(b)",
        "MATCH (a:S {name:'A'}), (c:S {name:'C'}) CREATE (a)-[:CALLS {t: 20}]->(c)",
        "MATCH (b:S {name:'B'}), (d:S {name:'D'}) CREATE (b)-[:CALLS {t: 30}]->(d)",
        "MATCH (c:S {name:'C'}), (d:S {name:'D'}) CREATE (c)-[:CALLS {t: 40}]->(d)",
        // Unreachable in time from A: fires before A's first edge.
        "MATCH (d:S {name:'D'}), (e:S {name:'E'}) CREATE (d)-[:CALLS {t: 5}]->(e)",
    ] {
        engine
            .execute_mut(q, &mut store, "default")
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    store
}

fn id_of(store: &GraphStore, name: &str) -> i64 {
    let b = QueryEngine::new()
        .execute(
            &format!("MATCH (n:S {{name: '{name}'}}) RETURN id(n) AS id"),
            store,
        )
        .expect("id");
    match &b.records[0].bindings()[0].1 {
        Value::Property(PropertyValue::Integer(i)) => *i,
        other => panic!("id came back as {other:?}"),
    }
}

/// Every row as a map from column name to its debug form.
fn rows(store: &GraphStore, query: &str) -> Vec<Vec<(String, String)>> {
    QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .records
        .iter()
        .map(|r| {
            r.bindings()
                .iter()
                .map(|(k, v)| (k.to_string(), format!("{v:?}")))
                .collect()
        })
        .collect()
}

fn column<'a>(row: &'a [(String, String)], name: &str) -> &'a str {
    row.iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
        .unwrap_or_else(|| panic!("no column {name} in {row:?}"))
}

/// The id inside a `node` column's debug form.
///
/// Parsed from `NodeId(n)` rather than searched for as a substring: a node's
/// debug form carries `created_at` and `updated_at`, so every digit of a
/// millisecond timestamp is in there and `contains("4")` matched a row about a
/// different node. The first version of this file did exactly that and
/// reported the implementation as broken when it was not.
fn node_id(cell: &str) -> i64 {
    let at = cell.find("NodeId(").expect("a node column");
    let rest = &cell[at + "NodeId(".len()..];
    let end = rest.find(')').expect("closing paren");
    rest[..end].parse().expect("an integer node id")
}

/// The node ids inside a `path` column's debug form, in order.
fn path_ids(cell: &str) -> Vec<i64> {
    let mut out = Vec::new();
    let mut rest = cell;
    while let Some(i) = rest.find("Integer(") {
        rest = &rest[i + "Integer(".len()..];
        let end = rest.find(')').expect("closing paren");
        out.push(rest[..end].parse::<i64>().expect("an integer"));
        rest = &rest[end..];
    }
    out
}

#[test]
fn temporal_reachability_returns_the_walk_it_took() {
    let store = store();
    let a = id_of(&store, "A");
    let d = id_of(&store, "D");
    let b = id_of(&store, "B");

    let rows = rows(
        &store,
        &format!(
            "CALL algo.temporalReachability({a}, {{timeProperty: 't'}}) \
             YIELD node, time, path RETURN node, time, path"
        ),
    );
    assert!(!rows.is_empty(), "A reaches something");

    let to_d = rows
        .iter()
        .find(|r| node_id(column(r, "node")) == d)
        .unwrap_or_else(|| panic!("D should be reachable: {rows:?}"));

    // The earliest route to D is A -> B -> D, arriving at 30. The other route
    // exists and arrives at 40, so a path that named C would be a walk that
    // does not explain the time in its own row.
    assert_eq!(
        path_ids(column(to_d, "path")),
        vec![a, b, d],
        "the path must be the walk that produced the reported arrival"
    );
    assert!(
        column(to_d, "time").contains("30"),
        "{}",
        column(to_d, "time")
    );
}

#[test]
fn every_reachability_path_starts_at_the_source_and_ends_at_its_row() {
    // The general check. A path column that returns some walk is worse than
    // none, because it looks like evidence.
    let store = store();
    let a = id_of(&store, "A");
    let rows = rows(
        &store,
        &format!(
            "CALL algo.temporalReachability({a}, {{timeProperty: 't'}}) \
             YIELD node, time, path RETURN node, time, path"
        ),
    );
    assert!(rows.len() >= 3, "A reaches B, C and D: {rows:?}");
    for row in &rows {
        let ids = path_ids(column(row, "path"));
        assert!(ids.len() >= 2, "a reached node needs at least one hop: {row:?}");
        assert_eq!(ids[0], a, "the walk must start at the source: {row:?}");
        assert_eq!(
            node_id(column(row, "node")),
            ids[ids.len() - 1],
            "the walk must end at the node the row is about: {row:?}"
        );
    }
}

#[test]
fn propagation_ranking_carries_the_same_walk() {
    // `propagationRanking` is the same traversal under a different name, so
    // the path must be the same one. A row that ranked without explaining was
    // the original complaint.
    let store = store();
    let a = id_of(&store, "A");
    let plain = rows(
        &store,
        &format!(
            "CALL algo.temporalReachability({a}, {{timeProperty: 't'}}) \
             YIELD node, path RETURN node, path"
        ),
    );
    let ranked = rows(
        &store,
        &format!(
            "CALL algo.propagationRanking({a}, {{timeProperty: 't'}}) \
             YIELD node, rank, path RETURN node, rank, path"
        ),
    );
    assert_eq!(plain.len(), ranked.len());
    for (p, r) in plain.iter().zip(ranked.iter()) {
        assert_eq!(column(p, "node"), column(r, "node"));
        assert_eq!(
            path_ids(column(p, "path")),
            path_ids(column(r, "path")),
            "the two names of one traversal disagree about the walk"
        );
    }
}

#[test]
fn symptom_explanation_shows_the_walk_that_binds_its_onset() {
    // A candidate may explain several symptoms by several walks and a row can
    // show one. It shows the binding one, so the path and the onset describe
    // the same journey.
    let store = store();
    let a = id_of(&store, "A");
    let d = id_of(&store, "D");

    let rows = rows(
        &store,
        &format!(
            "CALL algo.symptomExplanation([[{d}, 35]], {{timeProperty: 't'}}) \
             YIELD node, explains, onset, path RETURN node, explains, onset, path"
        ),
    );
    assert!(!rows.is_empty(), "something should explain D at 35");

    let from_a = rows
        .iter()
        .find(|r| node_id(column(r, "node")) == a)
        .unwrap_or_else(|| panic!("A should explain D: {rows:?}"));
    let ids = path_ids(column(from_a, "path"));
    assert_eq!(
        ids.first(),
        Some(&a),
        "the walk must start at the candidate cause: {from_a:?}"
    );
    assert_eq!(
        ids.last(),
        Some(&d),
        "the walk must end at the symptom it explains: {from_a:?}"
    );
}

#[test]
fn a_node_reached_by_no_walk_produces_no_row_at_all() {
    // The half that stops "always return a path" from being the fix. E is
    // downstream of D by an edge that fires at 5, before A's first edge, so it
    // is not reachable in time and must not appear — with or without a path.
    let store = store();
    let a = id_of(&store, "A");
    let e = id_of(&store, "E");
    let rows = rows(
        &store,
        &format!(
            "CALL algo.temporalReachability({a}, {{timeProperty: 't'}}) \
             YIELD node, path RETURN node, path"
        ),
    );
    assert!(
        !rows.iter().any(|r| node_id(column(r, "node")) == e),
        "E is not reachable in time and must not be reported: {rows:?}"
    );
}

#[test]
fn the_times_come_back_beside_the_path() {
    // Without these the path is a claim a caller has to take on trust. With
    // them they can check it against their own edges, which is the difference
    // between evidence and an assertion.
    let store = store();
    let a = id_of(&store, "A");
    let d = id_of(&store, "D");
    let rows = rows(
        &store,
        &format!(
            "CALL algo.temporalReachability({a}, {{timeProperty: 't'}}) \
             YIELD node, time, path, times RETURN node, time, path, times"
        ),
    );
    let to_d = rows
        .iter()
        .find(|r| node_id(column(r, "node")) == d)
        .expect("D");
    let times = path_ids(column(to_d, "times"));
    assert_eq!(times, vec![10, 30], "the walk A -> B -> D fires at 10 then 30");
    assert_eq!(
        times.len() + 1,
        path_ids(column(to_d, "path")).len(),
        "one time per edge, so one fewer than the nodes"
    );
}

#[test]
fn the_path_column_did_not_displace_the_columns_that_were_there() {
    // A YIELD that names only the old columns must still work: adding a column
    // is not licence to break the callers who do not want it.
    let store = store();
    let a = id_of(&store, "A");
    for q in [
        format!("CALL algo.temporalReachability({a}, {{timeProperty: 't'}}) YIELD node, time RETURN node, time"),
        format!("CALL algo.propagationRanking({a}, {{timeProperty: 't'}}) YIELD node, rank RETURN node, rank"),
    ] {
        assert!(!rows(&store, &q).is_empty(), "{q} returned nothing");
    }
}
