//! Grouping on identity, and the merge that keeps it equivalent (#521).
//!
//! `RETURN forum.id, forum.title, count(*)` keys on two properties of one
//! node. Resolving them per row is 1.68M property resolutions — with a string
//! clone among them — to distinguish 96,862 groups the node id already
//! distinguishes. So the operator now groups on the node and resolves the key
//! once per group.
//!
//! That is **finer**, not equivalent: two different nodes may carry the same
//! key tuple, and Cypher says they are one group. So there is a merge step,
//! and these tests are mostly about the merge. A merge that is missing, or
//! wrong for one aggregate, shows up only when two nodes collide on the key —
//! which no LDBC query does, and no benchmark would catch.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn prop(store: &mut GraphStore, id: samyama::graph::NodeId, k: &str, v: PropertyValue) {
    let _ = store.set_node_property("default", id, k.to_string(), v);
}

/// `teams` Team nodes, each with `members` Member nodes pointing at it.
/// `title` is deliberately a function of the caller's choosing, so a test can
/// make two distinct teams share one.
fn teams(spec: &[(&str, i64, usize)]) -> GraphStore {
    let mut store = GraphStore::new();
    for (title, score, members) in spec {
        let t = store.create_node("Team");
        prop(&mut store, t, "title", PropertyValue::String(title.to_string()));
        prop(&mut store, t, "score", PropertyValue::Integer(*score));
        for i in 0..*members {
            let m = store.create_node("Member");
            prop(&mut store, m, "v", PropertyValue::Integer(i as i64));
            store.create_edge(m, t, "IN").unwrap();
        }
    }
    store
}

/// Asserts the query actually reaches `AggregateOperator`.
///
/// Worth its own helper because of a trap that cost real time: the planner
/// rewrites `RETURN t.x, count(m)` over an expand into `AdjacencyCountAggregate`,
/// which reads degrees off the adjacency index and never builds a group at all.
/// Eight of the first ten tests here passed with the merge step deleted, because
/// eight of them were exercising that rewrite instead of the operator they name.
/// A second aggregate, or any aggregate that is not a plain count, defeats it.
fn assert_hash_aggregate(store: &GraphStore, cypher: &str) {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("EXPLAIN should run");
    let text = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    };
    assert!(
        text.contains("Aggregate (group_by="),
        "this query does not reach AggregateOperator, so it proves nothing about it:\n{text}"
    );
}

/// Rows as sorted `(column, rendered value)` pairs, so comparisons do not
/// depend on the hash-map order the aggregate emits groups in.
fn rows(store: &GraphStore, cypher: &str) -> Vec<Vec<String>> {
    assert_hash_aggregate(store, cypher);
    let query = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("query should run");
    let mut out: Vec<Vec<String>> = batch
        .records
        .iter()
        .map(|r| {
            let mut cells: Vec<String> =
                r.bindings().iter().map(|(k, v)| format!("{k}={v:?}")).collect();
            cells.sort();
            cells
        })
        .collect();
    out.sort();
    out
}

#[test]
fn two_nodes_sharing_a_key_tuple_are_one_group() {
    // The whole reason the merge exists. Two distinct Teams, same title and
    // score, 3 and 4 members. Grouping on identity gives 2 groups of 3 and 4;
    // Cypher wants 1 group of 7.
    let store = teams(&[("Ops", 10, 3), ("Ops", 10, 4)]);
    let got = rows(
        &store,
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS title, t.score AS score, count(m) AS c, sum(m.v) AS s",
    );
    assert_eq!(got.len(), 1, "the two teams share a key tuple: {got:?}");
    assert!(got[0].iter().any(|c| c.contains("Integer(7)")), "{got:?}");
}

#[test]
fn distinct_key_tuples_stay_distinct() {
    let store = teams(&[("Ops", 10, 3), ("Ops", 11, 4)]);
    let got = rows(
        &store,
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS title, t.score AS score, count(m) AS c, sum(m.v) AS s",
    );
    assert_eq!(got.len(), 2, "the scores differ: {got:?}");
}

#[test]
fn a_single_key_on_a_property_also_merges() {
    // One key is the same argument: `t.title` is a property of `t`, so this
    // takes the identity path too, and two teams share the title.
    let store = teams(&[("Ops", 10, 3), ("Ops", 11, 4), ("Dev", 1, 5)]);
    let got = rows(&store, "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS title, count(m) AS c, sum(m.v) AS s");
    assert_eq!(got.len(), 2, "Ops and Dev: {got:?}");
    let ops = got.iter().find(|r| r.iter().any(|c| c.contains("Ops"))).unwrap();
    assert!(ops.iter().any(|c| c.contains("Integer(7)")), "3 + 4 = 7: {ops:?}");
}

/// Every aggregate has to survive a merge, and each merges differently. A
/// missing arm shows up only on a key collision, so each gets a collision.
#[test]
fn every_aggregate_survives_a_merge() {
    // Two teams, same key, members with values 0,1,2 and 0,1,2,3.
    let store = teams(&[("Ops", 10, 3), ("Ops", 10, 4)]);
    let one = teams(&[("Ops", 10, 7)]); // no collision, but not the same values

    for (cypher, expected) in [
        ("count(m)", "Integer(7)"),
        ("sum(m.v)", "Integer(9)"),   // (0+1+2) + (0+1+2+3)
        ("min(m.v)", "Integer(0)"),
        ("max(m.v)", "Integer(3)"),
        ("avg(m.v)", "Float(1.2857142857142858)"), // 9 / 7
        ("count(DISTINCT m.v)", "Integer(4)"),     // {0,1,2} ∪ {0,1,2,3}
    ] {
        let q = format!("MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS ti, t.score AS s, {cypher} AS a, collect(m.v) AS keep");
        let got = rows(&store, &q);
        assert_eq!(got.len(), 1, "{cypher}: {got:?}");
        assert!(
            got[0].iter().any(|c| c.contains(expected)),
            "{cypher} should be {expected}, got {got:?}"
        );
    }

    // collect() merges by concatenation, so check the multiset rather than the
    // order, which Cypher does not specify.
    let got = rows(
        &store,
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS ti, t.score AS s, collect(m.v) AS a",
    );
    assert_eq!(got.len(), 1, "{got:?}");
    let rendered = got[0].join(" ");
    for want in ["Integer(0)", "Integer(1)", "Integer(2)", "Integer(3)"] {
        assert!(rendered.contains(want), "collect lost {want}: {rendered}");
    }
    assert_eq!(rendered.matches("Integer(0)").count(), 2, "both zeroes: {rendered}");

    let _ = one;
}

#[test]
fn count_of_a_property_still_counts_non_null_values() {
    // The `all_simple_count` guard (#358), now shared by three paths. Half the
    // members have no `w`, so `count(m.w)` must not equal `count(m)`.
    let mut store = GraphStore::new();
    let t = store.create_node("Team");
    prop(&mut store, t, "title", PropertyValue::String("Ops".into()));
    prop(&mut store, t, "score", PropertyValue::Integer(1));
    for i in 0..10 {
        let m = store.create_node("Member");
        store.create_edge(m, t, "IN").unwrap();
        if i % 2 == 0 {
            prop(&mut store, m, "w", PropertyValue::Integer(i));
        }
    }
    let got = rows(
        &store,
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS ti, t.score AS s, count(m.w) AS a",
    );
    assert_eq!(got.len(), 1);
    assert!(got[0].iter().any(|c| c.contains("Integer(5)")), "half have w: {got:?}");
}

#[test]
fn a_key_spanning_two_variables_takes_the_general_path() {
    // `identity_group_variable` must decline here: the keys are properties of
    // two different nodes, so no single identity groups them.
    let store = teams(&[("Ops", 10, 3), ("Dev", 20, 2)]);
    let got = rows(
        &store,
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS ti, m.v AS mv, count(m) AS c, sum(m.v) AS s",
    );
    // 3 members with v 0,1,2 in Ops and 2 with v 0,1 in Dev: 5 groups.
    assert_eq!(got.len(), 5, "{got:?}");
}

#[test]
fn a_null_property_groups_with_the_other_nulls() {
    // Teams with no title: the key tuple is (Null, score). Identity keeps them
    // apart, and the merge has to bring them back together where the scores
    // agree — including that Null equals Null as a group key, which is not how
    // Null compares elsewhere.
    let mut store = GraphStore::new();
    for _ in 0..2 {
        let t = store.create_node("Team");
        prop(&mut store, t, "score", PropertyValue::Integer(5));
        for _ in 0..3 {
            let m = store.create_node("Member");
            store.create_edge(m, t, "IN").unwrap();
        }
    }
    let got = rows(
        &store,
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS ti, t.score AS s, count(m) AS c, collect(m) AS ms",
    );
    assert_eq!(got.len(), 1, "both untitled teams group together: {got:?}");
    assert!(got[0].iter().any(|c| c.contains("Integer(6)")), "{got:?}");
}

#[test]
fn the_identity_path_agrees_with_the_general_path_on_a_wide_graph() {
    // A differential check. `t.title, t.score` takes the identity path;
    // `t.title, m.team_score` is the same partition expressed across two
    // variables, so it takes the general path. Same groups, same counts.
    let mut store = GraphStore::new();
    for g in 0..200 {
        let t = store.create_node("Team");
        // 200 teams over 50 titles, so four teams share each title, and the
        // merge runs 150 times.
        prop(&mut store, t, "title", PropertyValue::String(format!("t{}", g % 50)));
        prop(&mut store, t, "score", PropertyValue::Integer((g % 50) as i64));
        for i in 0..(g % 7 + 1) {
            let m = store.create_node("Member");
            prop(&mut store, m, "v", PropertyValue::Integer(i as i64));
            prop(&mut store, m, "team_score", PropertyValue::Integer((g % 50) as i64));
            store.create_edge(m, t, "IN").unwrap();
        }
    }

    let identity = rows(
        &store,
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS a, t.score AS b, count(m) AS c, sum(m.v) AS d",
    );
    let general = rows(
        &store,
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS a, m.team_score AS b, count(m) AS c, sum(m.v) AS d",
    );
    assert_eq!(identity.len(), 50, "50 distinct titles: {}", identity.len());
    assert_eq!(identity, general, "the two paths disagree");
}

#[test]
fn grouping_cost_does_not_scale_with_the_string_key() {
    // The regression this is guarding: a long string key used to be resolved
    // and cloned once per input row. Now it is resolved once per group, so a
    // 400-character title should cost about what a short one does.
    fn build(title_len: usize) -> GraphStore {
        let mut store = GraphStore::new();
        for g in 0..500 {
            let t = store.create_node("Team");
            prop(&mut store, t, "title", PropertyValue::String(format!("{g}").repeat(title_len)));
            prop(&mut store, t, "score", PropertyValue::Integer(g as i64));
            for i in 0..200 {
                let m = store.create_node("Member");
                prop(&mut store, m, "v", PropertyValue::Integer(i));
                store.create_edge(m, t, "IN").unwrap();
            }
        }
        store
    }
    let cypher =
        "MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS a, t.score AS b, sum(m.v) AS c";

    let time = |store: &GraphStore| {
        let q = parse_query(cypher).unwrap();
        let _ = QueryExecutor::new(store).execute(&q).unwrap();
        let started = std::time::Instant::now();
        let out = QueryExecutor::new(store).execute(&q).unwrap();
        assert_eq!(out.records.len(), 500);
        started.elapsed().as_secs_f64()
    };

    let short = time(&build(1));
    let long = time(&build(200));
    assert!(
        long < short * 4.0,
        "a 200x longer key cost {:.1}x more ({short:.4}s -> {long:.4}s) — it is being cloned per row",
        long / short
    );
}

#[test]
fn explain_is_unchanged_by_the_grouping_strategy() {
    // The plan is the contract; which of three fold paths runs is not part of
    // it. Asserted so a future reader does not go looking for a `GroupBy` node
    // that was never there.
    let store = teams(&[("Ops", 10, 3)]);
    let query =
        parse_query("EXPLAIN MATCH (t:Team)<-[:IN]-(m:Member) RETURN t.title AS a, t.score AS b, sum(m.v) AS c")
            .unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let text = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    };
    assert!(text.contains("Aggregate (group_by="), "{text}");
}
