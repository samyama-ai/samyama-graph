//! `USING INDEX` and `USING SCAN` decide where a pattern starts and how
//! (LANG-13, #1675).
//!
//! Both parsed as errors before this. A hint is **mandatory**, as in Neo4j: one
//! the planner cannot honour -- no such index, no predicate for it, a variable
//! no pattern binds -- is an error naming the hint. A dropped hint reads
//! exactly like one that worked, which is the one thing a hint exists to rule
//! out. The answer never changes: a hint chooses the plan, and the predicates
//! still filter.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// 50 `:P` with `id` 0..50 and `grp` = id % 5, each `KNOWS` the next; an index
/// on both properties.
fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let mut prev = None;
    for i in 0..50 {
        let n = s.create_node_with_labels([Label::new("P")]);
        s.set_node_property("default", n, "id", PropertyValue::Integer(i)).unwrap();
        s.set_node_property("default", n, "grp", PropertyValue::Integer(i % 5)).unwrap();
        if let Some(p) = prev {
            s.create_edge(p, n, "KNOWS").unwrap();
        }
        prev = Some(n);
    }
    for q in ["CREATE INDEX ON :P(id)", "CREATE INDEX ON :P(grp)"] {
        MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(q).unwrap()).unwrap();
    }
    s
}

fn plan(s: &GraphStore, q: &str) -> Result<String, String> {
    let query = parse_query(&format!("EXPLAIN {q}")).map_err(|e| e.to_string())?;
    let batch = QueryExecutor::new(s).execute(&query).map_err(|e| e.to_string())?;
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => Ok(t.split("--- Row budget").next().unwrap().to_string()),
        other => panic!("{other:?}"),
    }
}

fn ids(s: &GraphStore, q: &str) -> Vec<i64> {
    let batch = QueryExecutor::new(s).execute(&parse_query(q).unwrap()).unwrap_or_else(|e| panic!("{q}: {e}"));
    let mut v: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("{other:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn using_scan_replaces_the_index_the_planner_would_choose() {
    let s = store();
    let q = "MATCH (p:P) WHERE p.id = 7 RETURN p.id AS id";
    assert!(plan(&s, q).unwrap().contains("IndexScan"), "control: {}", plan(&s, q).unwrap());
    let hinted = "MATCH (p:P) USING SCAN p:P WHERE p.id = 7 RETURN p.id AS id";
    let text = plan(&s, hinted).unwrap();
    assert!(text.contains("NodeScan") && !text.contains("IndexScan"), "{text}");
    assert_eq!(ids(&s, hinted), ids(&s, q));
}

#[test]
fn using_index_picks_the_named_index_over_the_other_one() {
    let s = store();
    let hinted = "MATCH (p:P) USING INDEX p:P(grp) WHERE p.id = 7 AND p.grp = 2 RETURN p.id AS id";
    let text = plan(&s, hinted).unwrap();
    assert!(text.contains("P.grp"), "the scan is not on grp: {text}");
    assert_eq!(ids(&s, hinted), vec![7]);
}

#[test]
fn a_hint_moves_the_start_of_a_path() {
    let s = store();
    // Written from `a`; the hint starts it at `b`, through b's index.
    let q = "MATCH (a:P)-[:KNOWS]->(b:P) USING INDEX b:P(id) WHERE b.id = 10 RETURN a.id AS id";
    let text = plan(&s, q).unwrap();
    assert!(text.contains("IndexScan (var=b"), "{text}");
    assert_eq!(ids(&s, q), vec![9]);
}

#[test]
fn a_hint_that_cannot_be_honoured_is_an_error_naming_it() {
    let s = store();
    for (q, needle) in [
        ("MATCH (p:P) USING INDEX p:P(nope) WHERE p.nope = 1 RETURN p", "no such index"),
        ("MATCH (p:P) USING INDEX p:P(id) RETURN p", "needs a predicate"),
        ("MATCH (p:P) USING SCAN q:P RETURN p", "no MATCH pattern binds"),
        ("MATCH (p:P) USING SCAN p:Q RETURN p", "does not give `p` that label"),
    ] {
        let err = plan(&s, q).expect_err(q);
        assert!(err.contains(needle), "{q}: {err}");
    }
}

#[test]
fn using_join_is_accepted() {
    let s = store();
    let q = "MATCH (a:P)-[:KNOWS]->(b:P), (b)-[:KNOWS]->(c:P) USING JOIN ON b WHERE a.id = 1 RETURN c.id AS id";
    assert_eq!(ids(&s, q), vec![3]);
}
