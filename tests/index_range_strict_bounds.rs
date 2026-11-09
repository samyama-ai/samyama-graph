//! A strict range over an indexed property must not return the bound itself.
//!
//! `IndexScanOperator`'s loose bounds read the scan's own bound inclusively and
//! across the `Integer`/`Float` tie, so a `>` scan's range starts at the bound.
//! That is sound only while the range conjunct survives as a filter to re-check
//! it, which is what `with_loose_bounds` is promised.
//!
//! The promise used to be kept by re-applying unapplied WHERE conjuncts at the
//! end of `plan_inner`. Two shapes bypass that: a query with a `WITH` skips the
//! re-application outright, and an optional-scoped conjunct is routed to a join
//! that does not apply it -- so `p.id > 100 WITH p` answered 100 (#2107).
//!
//! So these tests pin the strict bound through both of those shapes as well as
//! the plain one, and pin the inclusive bound keeping both spellings of its
//! number, which is what the loose bounds exist for.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

/// The `id` values a query returned, sorted, as f64 so Integer and Float
/// spellings of the same number compare equal.
fn ids(s: &GraphStore, q: &str) -> Vec<f64> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let mut out: Vec<f64> = batch
        .records
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i as f64,
            Some(Value::Property(PropertyValue::Float(f))) => *f,
            other => panic!("expected a number, got {other:?}"),
        })
        .collect();
    out.sort_by(|a, b| a.partial_cmp(b).unwrap());
    out
}

/// 99, 100 and 101 as integers, plus 100.0 as a float: the tie the loose
/// bounds exist for sits exactly on the bound these tests probe.
fn indexed_store() -> GraphStore {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:P {id: 99})");
    write(&mut s, "CREATE (:P {id: 100})");
    write(&mut s, "CREATE (:P {id: 100.0})");
    write(&mut s, "CREATE (:P {id: 101})");
    write(&mut s, "CREATE INDEX ON :P(id)");
    s
}

/// The same four nodes, each with an outgoing edge, so the planner's
/// path-anchoring site chooses the scan instead.
fn indexed_store_with_path() -> GraphStore {
    let mut s = indexed_store();
    write(
        &mut s,
        "MATCH (p:P) CREATE (p)-[:HAS]->(:Tag {name: 'x'})",
    );
    s
}

#[test]
fn a_strict_greater_than_excludes_its_own_bound() {
    let s = indexed_store();
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id > 100 RETURN p.id AS id"),
        vec![101.0],
        "`> 100` must not return 100, in either spelling"
    );
}

#[test]
fn a_strict_less_than_excludes_its_own_bound() {
    let s = indexed_store();
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id < 100 RETURN p.id AS id"),
        vec![99.0],
        "`< 100` must not return 100, in either spelling"
    );
}

#[test]
fn a_strict_bound_written_as_a_float_also_excludes_the_integer() {
    let s = indexed_store();
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id > 100.0 RETURN p.id AS id"),
        vec![101.0],
        "`> 100.0` must exclude Integer(100) as well as Float(100.0)"
    );
}

#[test]
fn an_inclusive_bound_still_keeps_both_spellings() {
    // What the loose bounds were added for: `PropertyValue::cmp` orders
    // `Integer(n)` just below `Float(n)`, so an exact bound on one variant cuts
    // the other off. Fixing the strict case must not undo this.
    let s = indexed_store();
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id >= 100 RETURN p.id AS id"),
        vec![100.0, 100.0, 101.0],
        "`>= 100` keeps Integer(100) and Float(100.0)"
    );
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id <= 100 RETURN p.id AS id"),
        vec![99.0, 100.0, 100.0],
        "`<= 100` keeps Integer(100) and Float(100.0)"
    );
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id >= 100.0 RETURN p.id AS id"),
        vec![100.0, 100.0, 101.0],
        "a float bound keeps the integer spelling"
    );
}

#[test]
fn a_bounded_range_excludes_a_strict_end() {
    let s = indexed_store();
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id > 100 AND p.id <= 101 RETURN p.id AS id"),
        vec![101.0],
        "the second conjunct supplies the other end; the strict end stays strict"
    );
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id >= 99 AND p.id < 100 RETURN p.id AS id"),
        vec![99.0],
        "a strict upper end excludes both spellings of 100"
    );
}

#[test]
fn the_path_anchoring_planner_site_is_strict_too() {
    // The planner has two places that anchor on an indexed property and they
    // decide independently, so a fix to one leaves the other wrong.
    let s = indexed_store_with_path();
    assert_eq!(
        ids(&s, "MATCH (p:P)-[:HAS]->(:Tag) WHERE p.id > 100 RETURN p.id AS id"),
        vec![101.0],
        "`> 100` must not return 100 when the scan anchors a path"
    );
    assert_eq!(
        ids(&s, "MATCH (p:P)-[:HAS]->(:Tag) WHERE p.id >= 100 RETURN p.id AS id"),
        vec![100.0, 100.0, 101.0],
        "and the inclusive case still keeps both spellings"
    );
}

#[test]
fn a_with_does_not_drop_the_scan_s_own_bound() {
    // `plan_inner` skips its re-application of unapplied WHERE conjuncts when
    // the query has a `WITH`, on the reasoning that the match planners already
    // pushed them down. A conjunct handed to a loose `IndexScan` is pushed
    // nowhere: the scan reports no `filter_predicate` for a range.
    let s = indexed_store();
    for query in [
        "MATCH (p:P) WHERE p.id > 100 WITH p RETURN p.id AS id",
        "MATCH (p:P) WHERE p.id > 100 WITH p AS q RETURN q.id AS id",
        "MATCH (p:P) WHERE p.id > 100 WITH p, 1 AS k RETURN p.id AS id",
        "MATCH (p:P) WHERE p.id > 100 WITH p ORDER BY p.id RETURN p.id AS id",
    ] {
        assert_eq!(ids(&s, query), vec![101.0], "{query}");
    }
}

#[test]
fn an_optional_match_does_not_drop_the_scan_s_own_bound() {
    // The conjunct is scoped to the optional side and excluded from the
    // top-level filter so it can be. The join then did not apply it either.
    let s = indexed_store();
    assert_eq!(
        ids(&s, "OPTIONAL MATCH (p:P) WHERE p.id > 100 RETURN p.id AS id"),
        vec![101.0],
    );
    let s = indexed_store_with_path();
    assert_eq!(
        ids(
            &s,
            "OPTIONAL MATCH (p:P)-[:HAS]->(:Tag) WHERE p.id > 100 RETURN p.id AS id"
        ),
        vec![101.0],
    );
}

#[test]
fn a_with_keeps_both_ends_of_a_bounded_range() {
    // Both ends, because which conjunct the scan consumes is the planner's
    // choice, not the query's: here it anchors on `> 99` and bounds the other
    // end with `< 101`, so before the fix it was the *lower* bound that leaked
    // and 99 came back.
    let s = indexed_store();
    assert_eq!(
        ids(&s, "MATCH (p:P) WHERE p.id > 99 AND p.id < 101 WITH p RETURN p.id AS id"),
        vec![100.0, 100.0],
        "both spellings of 100 lie strictly inside (99, 101)"
    );
}

#[test]
fn an_equality_scan_still_answers_its_own_predicate() {
    // The `=` conjunct is still consumed: the scan emits only matching rows and
    // reports the predicate as applied, so re-filtering it is pure cost
    // (#1380). Keeping ranges must not have swept equality in with them.
    let s = indexed_store();
    assert_eq!(ids(&s, "MATCH (p:P) WHERE p.id = 101 RETURN p.id AS id"), vec![101.0]);
    assert_eq!(ids(&s, "MATCH (p:P) WHERE p.id = 101 WITH p RETURN p.id AS id"), vec![101.0]);

    let query = parse_query("EXPLAIN MATCH (p:P) WHERE p.id = 101 RETURN p.id AS id").unwrap();
    let batch = QueryExecutor::new(&s).execute(&query).unwrap();
    let plan = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(p))) => p.clone(),
        other => panic!("expected a plan, got {other:?}"),
    };
    assert!(
        !plan.lines().any(|l| l.contains("Filter") && l.contains("p.id")),
        "an equality scan needs no filter above it:\n{plan}"
    );
}
