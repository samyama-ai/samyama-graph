//! `OPTIONAL MATCH` pushed onto the pipeline still emits the row that misses (#726).
//!
//! An optional clause hanging off a bound variable used to be planned
//! standalone and left-outer-joined, which meant a full label scan of the far
//! end: 422 ms at SF1 where the equivalent `EXISTS` costs 0.02. It is now an
//! expand that null-fills a source row matching nothing.
//!
//! That null-filled row is the entire semantics of `OPTIONAL MATCH`, so these
//! tests are mostly about the miss rather than the hit — and about the cases
//! where the pushdown must decline, because a wrong answer here looks like a
//! smaller result rather than an error.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

/// Rows as `name=value` strings, sorted, so a bag comparison reads clearly.
fn rows(store: &GraphStore, cypher: &str, cols: &[&str]) -> Vec<String> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| {
            cols.iter()
                .map(|c| match r.get(c) {
                    Some(Value::Property(PropertyValue::String(s))) => format!("{c}={s}"),
                    Some(Value::Property(PropertyValue::Boolean(b))) => format!("{c}={b}"),
                    Some(Value::Property(PropertyValue::Integer(i))) => format!("{c}={i}"),
                    Some(Value::Null) | Some(Value::Property(PropertyValue::Null)) | None => {
                        format!("{c}=null")
                    }
                    other => format!("{c}={other:?}"),
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    got.sort();
    got
}

/// `a` and `b` know each other; `c` knows nobody. All three wrote a post.
fn people() -> GraphStore {
    let mut s = GraphStore::new();
    for n in ["a", "b", "c"] {
        run(&mut s, &format!("CREATE (:Person {{name: \"{n}\"}})"));
        run(
            &mut s,
            &format!("CREATE (:Post {{title: \"p{n}\", author: \"{n}\"}})"),
        );
    }
    run(
        &mut s,
        "MATCH (x:Person {name:\"a\"}), (y:Person {name:\"b\"}) CREATE (x)-[:KNOWS {since: 2020}]->(y)",
    );
    for n in ["a", "b", "c"] {
        run(
            &mut s,
            &format!(
                "MATCH (p:Post {{title:\"p{n}\"}}), (q:Person {{name:\"{n}\"}}) \
                 CREATE (p)-[:HAS_CREATOR]->(q)"
            ),
        );
    }
    s
}

/// The miss still produces a row. This is the whole point.
#[test]
fn a_source_row_that_matches_nothing_still_emits_with_nulls() {
    let s = people();
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) \
             RETURN p.name AS p, f.name AS f",
            &["p", "f"],
        ),
        vec!["p=a f=b", "p=b f=null", "p=c f=null"],
    );
}

/// The far end already bound — the shape from IS7, and the one that cost
/// 422 ms. Both endpoints are known, so the expand closes onto `f` rather than
/// scanning the label, and the pairs that do not know each other still emit.
#[test]
fn a_bound_far_end_is_matched_not_rescanned_and_misses_still_emit() {
    let s = people();
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person {name:\"a\"}), (f:Person) \
             OPTIONAL MATCH (p)-[k:KNOWS]-(f) \
             RETURN f.name AS f, (k IS NOT NULL) AS knows",
            &["f", "knows"],
        ),
        vec!["f=a knows=false", "f=b knows=true", "f=c knows=false"],
    );
}

/// An edge variable bound on the hit is readable; on the miss it is null.
#[test]
fn the_edge_variable_is_bound_on_a_hit_and_null_on_a_miss() {
    let s = people();
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person) OPTIONAL MATCH (p)-[k:KNOWS]->(:Person) \
             RETURN p.name AS p, k.since AS since",
            &["p", "since"],
        ),
        vec!["p=a since=2020", "p=b since=null", "p=c since=null"],
    );
}

/// A label on the far end is part of the pattern, so failing it is a miss —
/// a null row — not a dropped row.
#[test]
fn a_far_end_label_that_excludes_everything_is_a_miss_not_a_drop() {
    let s = people();
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Nonexistent) \
             RETURN p.name AS p, f.name AS f",
            &["p", "f"],
        ),
        vec!["p=a f=null", "p=b f=null", "p=c f=null"],
    );
}

/// An inline property on the far end has to filter *inside* the expand. As a
/// filter above it, the null row would be deleted and the miss would vanish.
#[test]
fn an_inline_property_on_the_far_end_still_leaves_a_null_row() {
    let s = people();
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person {name: \"zzz\"}) \
             RETURN p.name AS p, f.name AS f",
            &["p", "f"],
        ),
        vec!["p=a f=null", "p=b f=null", "p=c f=null"],
    );
}

/// A `WHERE` on the optional clause belongs to the optional pattern: a row
/// failing it still emits nulls. The pushdown declines this shape and the join
/// handles it; the answer is what matters.
#[test]
fn a_where_on_the_optional_clause_still_leaves_a_null_row() {
    let s = people();
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person) OPTIONAL MATCH (p)-[k:KNOWS]->(f:Person) \
             WHERE k.since > 3000 RETURN p.name AS p, f.name AS f",
            &["p", "f"],
        ),
        vec!["p=a f=null", "p=b f=null", "p=c f=null"],
    );
}

/// Two hops: a source matching the first and not the second owes one null row,
/// not one per partial match. The pushdown declines multi-segment paths for
/// exactly this reason.
#[test]
fn a_two_segment_optional_path_emits_one_null_row_per_source() {
    let s = people();
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(:Person)-[:KNOWS]->(g:Person) \
             RETURN p.name AS p, g.name AS g",
            &["p", "g"],
        ),
        vec!["p=a g=null", "p=b g=null", "p=c g=null"],
    );
}

/// Several matches produce several rows, and no extra null row alongside them.
#[test]
fn several_matches_produce_several_rows_and_no_null() {
    let mut s = people();
    run(
        &mut s,
        "MATCH (x:Person {name:\"a\"}), (y:Person {name:\"c\"}) CREATE (x)-[:KNOWS]->(y)",
    );
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person {name:\"a\"}) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) \
             RETURN p.name AS p, f.name AS f",
            &["p", "f"],
        ),
        vec!["p=a f=b", "p=a f=c"],
    );
}
