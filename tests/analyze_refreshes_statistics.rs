//! `ANALYZE` recomputes the planner's statistics and says what it did (LANG-13).
//!
//! The planner estimates cardinality from `GraphStatistics`, which the store
//! builds lazily and caches. `ANALYZE` recomputes them and reports what they
//! were computed over.
//!
//! # What these cases cannot prove, stated rather than implied
//!
//! Deleting the `invalidate_statistics_cache` call from the operator fails
//! **none** of these tests, and that is not a gap in them. Every mutating path
//! on `GraphStore` already invalidates — including the bulk-load stubs a
//! snapshot import uses — so no public API can produce a populated-but-wrong
//! cache for a test to catch. The invalidation is insurance against a future
//! write path that forgets; it is not a fix for a reachable bug, and it is
//! documented that way in the operator.
//!
//! What these cases do pin is the part that is observable: the reported numbers
//! against a fixture whose contents the test controls, and `cache_was_stale`,
//! which is hardcode-proof — pinning it either way fails one of the two
//! sequence cases below.
//!
//! # What these cases are careful about
//!
//! A statement that only invalidates a cache is almost impossible to test
//! through its effect: the next plan recomputes either way, so "it worked" and
//! "it did nothing" produce the same graph. That is why `ANALYZE` returns a
//! row, and why the row carries `cache_was_stale` — the one field that differs
//! between a call that replaced a cached set and a call that populated an
//! empty one.
//!
//! So the assertions here are on the reported numbers against a graph whose
//! contents the test controls, plus the two-call sequence where the second
//! call must report a cache the first one filled.

use samyama::graph::{EdgeType, GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

const TENANT: &str = "default";

/// Three `:Person`, two `:Company`, two `KNOWS` and one `WORKS_AT`.
fn seed() -> GraphStore {
    let mut store = GraphStore::new();
    let mut people = Vec::new();
    for name in ["alice", "bob", "carol"] {
        let id = store.create_node(Label::new("Person"));
        store
            .set_node_property(TENANT, id, "name", PropertyValue::String(name.into()))
            .expect("set name");
        people.push(id);
    }
    let mut companies = Vec::new();
    for name in ["acme", "globex"] {
        let id = store.create_node(Label::new("Company"));
        store
            .set_node_property(TENANT, id, "name", PropertyValue::String(name.into()))
            .expect("set name");
        companies.push(id);
    }
    store
        .create_edge(people[0], people[1], EdgeType::new("KNOWS"))
        .expect("edge");
    store
        .create_edge(people[1], people[2], EdgeType::new("KNOWS"))
        .expect("edge");
    store
        .create_edge(people[0], companies[0], EdgeType::new("WORKS_AT"))
        .expect("edge");
    store
}

fn analyze(store: &GraphStore) -> std::collections::HashMap<String, Value> {
    let q = parse_query("ANALYZE").expect("ANALYZE must parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("ANALYZE must run");
    assert_eq!(batch.records.len(), 1, "ANALYZE returns exactly one row");
    let rec = &batch.records[0];
    ["nodes", "edges", "labels", "edge_types", "cache_was_stale"]
        .iter()
        .map(|k| {
            (
                (*k).to_string(),
                rec.get(k)
                    .unwrap_or_else(|| panic!("ANALYZE must return a `{k}` column"))
                    .clone(),
            )
        })
        .collect()
}

fn int_of(v: &Value) -> i64 {
    match v {
        Value::Property(PropertyValue::Integer(i)) => *i,
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn bool_of(v: &Value) -> bool {
    match v {
        Value::Property(PropertyValue::Boolean(b)) => *b,
        other => panic!("expected a boolean, got {other:?}"),
    }
}

#[test]
fn analyze_parses_and_returns_a_row() {
    let store = seed();
    let row = analyze(&store);
    assert_eq!(row.len(), 5);
}

#[test]
fn the_reported_numbers_describe_the_graph_it_analysed() {
    // The point of the row: a reader can see whether the statistics describe
    // the graph they think they do. Numbers that did not come from this graph
    // would still look plausible, so they are checked against a fixture whose
    // contents the test set.
    let store = seed();
    let row = analyze(&store);
    assert_eq!(int_of(&row["nodes"]), 5, "3 Person + 2 Company");
    assert_eq!(int_of(&row["edges"]), 3, "2 KNOWS + 1 WORKS_AT");
    assert_eq!(int_of(&row["labels"]), 2, "Person and Company");
    assert_eq!(int_of(&row["edge_types"]), 2, "KNOWS and WORKS_AT");
}

#[test]
fn the_first_analyze_on_a_cold_store_reports_no_cached_statistics() {
    // `cache_was_stale` is the field that distinguishes "replaced a cached
    // set" from "filled an empty cache". On a store nothing has planned
    // against, there is nothing to replace.
    let store = GraphStore::new();
    let row = analyze(&store);
    assert!(
        !bool_of(&row["cache_was_stale"]),
        "nothing has been cached yet, so ANALYZE cannot have replaced anything"
    );
}

#[test]
fn a_second_analyze_reports_the_cache_the_first_one_filled() {
    // The sequence that proves the field varies. If `cache_was_stale` were
    // hardcoded either way, one of these two cases would fail.
    let store = seed();
    let first = analyze(&store);
    assert!(!bool_of(&first["cache_was_stale"]));
    let second = analyze(&store);
    assert!(
        bool_of(&second["cache_was_stale"]),
        "the first ANALYZE left a cached set; the second must report replacing it"
    );
}

#[test]
fn analyze_sees_a_graph_that_changed_under_it() {
    // Note what this does and does not show. `create_node` invalidates the
    // cache itself, so this passes with or without ANALYZE's own invalidation.
    // It pins that the reported numbers track the graph, which is the row's
    // job -- not that the invalidation is load-bearing, which nothing here can
    // show and the module docs say so.
    let mut store = seed();
    let before = analyze(&store);
    assert_eq!(int_of(&before["nodes"]), 5);

    for _ in 0..4 {
        store.create_node(Label::new("Person"));
    }

    let after = analyze(&store);
    assert_eq!(
        int_of(&after["nodes"]),
        9,
        "ANALYZE must report the graph as it is now, not as it was cached"
    );
}

#[test]
fn analyze_is_a_read_and_runs_on_the_read_executor() {
    // It mutates a cache, not the graph. Requiring write access would put it
    // out of reach of the person diagnosing a slow read, which is who asks for
    // it. That this test uses `QueryExecutor` at all is the assertion; the
    // explicit check is that the plan does not claim to be a write.
    let store = seed();
    let q = parse_query("ANALYZE").expect("parse");
    let planner = samyama::query::executor::planner::QueryPlanner::new();
    let plan = planner.plan(&q, &store).expect("plan");
    assert!(!plan.is_write, "ANALYZE does not modify the graph");
}

#[test]
fn analyze_is_case_insensitive_like_every_other_keyword() {
    for form in ["ANALYZE", "analyze", "Analyze", "  ANALYZE  "] {
        assert!(
            parse_query(form).is_ok(),
            "{form:?} should parse the same as any other keyword"
        );
    }
}
