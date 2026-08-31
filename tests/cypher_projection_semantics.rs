//! openCypher conformance suite for the **result-shaping stage**: projection, DISTINCT,
//! implicit grouping keys, aggregation and ORDER BY.
//!
//! ## Why this file exists
//!
//! Six issues in `comp:query-engine` were open at once, all of the same kind — the engine
//! returning *plausible but wrong* rows with no error: `RETURN DISTINCT` not deduplicating
//! (#311), `count(var)` returning one row per implicit group instead of a scalar (#301),
//! `ORDER BY sum(x)` not sorting (#345), inline properties inside `EXISTS { }` never
//! matching (#346). Those are not four unrelated slips; they are what happens when a stage
//! has no systematic semantics tests and is only exercised incidentally by feature tests
//! that assert on one column at a time.
//!
//! So this suite asserts the *semantics* rather than any particular plan: one fixture,
//! many small questions with arithmetically obvious answers. A query planner is free to
//! answer them however it likes — with a specialized operator, a cache, or a full scan —
//! but it must answer them the way openCypher says.
//!
//! ## Conventions
//!
//! - Every expected value is derivable by hand from the fixture below; no golden files.
//! - Row order is only asserted where the query asks for an order.
//! - Tests for behaviour that is *known broken and not fixed here* are `#[ignore]`d with
//!   the issue number, so the semantics are recorded and the test turns into the
//!   regression net the day someone fixes it. Run them with `cargo test -- --ignored`.

use std::collections::BTreeMap;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------
//
//  Person   dept    salary   city    KNOWS →
//  ------   ------  ------   -----   ---------------------------
//  Alice    Eng      100     NYC     Bob, Carol
//  Bob      Eng      200     NYC     Alice
//  Carol    Eng      300     LON     Alice
//  Dave     Sales    150     NYC     Eve
//  Eve      Sales    250     LON     Dave
//  Frank    (none)  (none)   LON     (none)
//
//  6 people, 3 distinct cities → 2 (NYC, LON), 2 distinct depts + one absent,
//  salary total 1000 over 5 people, 6 KNOWS edges.

fn fixture() -> GraphStore {
    let mut s = GraphStore::new();
    let people: [(&str, Option<&str>, Option<i64>, &str); 6] = [
        ("Alice", Some("Eng"), Some(100), "NYC"),
        ("Bob", Some("Eng"), Some(200), "NYC"),
        ("Carol", Some("Eng"), Some(300), "LON"),
        ("Dave", Some("Sales"), Some(150), "NYC"),
        ("Eve", Some("Sales"), Some(250), "LON"),
        ("Frank", None, None, "LON"),
    ];
    let mut ids = BTreeMap::new();
    for (name, dept, salary, city) in people {
        let id = s.create_node("Person");
        s.set_column_property(id, "name", PropertyValue::String(name.into()));
        s.set_column_property(id, "city", PropertyValue::String(city.into()));
        if let Some(d) = dept {
            s.set_column_property(id, "dept", PropertyValue::String(d.into()));
        }
        if let Some(v) = salary {
            s.set_column_property(id, "salary", PropertyValue::Integer(v));
        }
        ids.insert(name, id);
    }
    for (from, to) in [
        ("Alice", "Bob"),
        ("Alice", "Carol"),
        ("Bob", "Alice"),
        ("Carol", "Alice"),
        ("Dave", "Eve"),
        ("Eve", "Dave"),
    ] {
        s.create_edge(ids[from], ids[to], "KNOWS").unwrap();
    }
    s
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Render a value in a form that is stable to compare and readable when it fails.
fn render(v: &Value) -> String {
    match v {
        Value::Property(PropertyValue::String(s)) => s.clone(),
        Value::Property(PropertyValue::Integer(i)) => i.to_string(),
        Value::Property(PropertyValue::Float(f)) => format!("{f:.4}"),
        Value::Property(PropertyValue::Boolean(b)) => b.to_string(),
        Value::Property(PropertyValue::Null) | Value::Null => "NULL".to_string(),
        Value::Property(PropertyValue::Array(a)) => {
            let mut parts: Vec<String> = a
                .iter()
                .map(|p| render(&Value::Property(p.clone())))
                .collect();
            parts.sort();
            format!("[{}]", parts.join(","))
        }
        Value::Node(id, _) | Value::NodeRef(id) => format!("node:{}", id.as_u64()),
        Value::Edge(id, _) | Value::EdgeRef(id, ..) => format!("edge:{}", id.as_u64()),
        other => format!("{other:?}"),
    }
}

/// Rows in engine order, each row rendered as `col=value` sorted by column.
fn rows(store: &GraphStore, q: &str) -> Vec<String> {
    let engine = QueryEngine::new();
    let batch = engine
        .execute(q, store)
        .unwrap_or_else(|e| panic!("query failed: {q}\n  {e}"));
    batch
        .records
        .iter()
        .map(|r| {
            let mut cells: Vec<String> = r
                .bindings()
                .iter()
                .map(|(k, v)| format!("{k}={}", render(v)))
                .collect();
            cells.sort();
            cells.join(" ")
        })
        .collect()
}

/// Rows as an order-independent multiset, for queries with no ORDER BY.
fn bag(store: &GraphStore, q: &str) -> Vec<String> {
    let mut r = rows(store, q);
    r.sort();
    r
}

/// The single scalar a query must return, failing loudly on any other shape.
///
/// The shape check is the point: `count(x)` returning three rows of the right *number* is
/// exactly the failure mode of #301, and an assertion that only looked at `records[0]`
/// would have passed straight through it.
fn scalar(store: &GraphStore, q: &str) -> String {
    let r = rows(store, q);
    assert_eq!(
        r.len(),
        1,
        "expected exactly one row from an ungrouped aggregate, got {}: {q}\n  {r:?}",
        r.len()
    );
    r[0].clone()
}

// ---------------------------------------------------------------------------
// Projection basics
// ---------------------------------------------------------------------------

#[test]
fn projects_a_property_once_per_matched_node() {
    assert_eq!(
        rows(&fixture(), "MATCH (p:Person) RETURN p.city AS c").len(),
        6
    );
}

#[test]
fn projects_a_missing_property_as_null_not_as_a_dropped_row() {
    let s = fixture();
    let r = bag(&s, "MATCH (p:Person) RETURN p.name AS n, p.dept AS d");
    assert_eq!(r.len(), 6, "Frank must still produce a row");
    assert!(r.contains(&"d=NULL n=Frank".to_string()), "{r:?}");
}

#[test]
fn an_alias_renames_the_column_and_nothing_else() {
    let s = fixture();
    let aliased = bag(
        &s,
        "MATCH (p:Person) WHERE p.name = \"Alice\" RETURN p.salary AS pay",
    );
    assert_eq!(aliased, vec!["pay=100"]);
}

// ---------------------------------------------------------------------------
// DISTINCT  (#311)
// ---------------------------------------------------------------------------

#[test]
fn distinct_deduplicates_a_projected_property() {
    // 6 people live in 2 distinct cities.
    let s = fixture();
    assert_eq!(
        bag(&s, "MATCH (p:Person) RETURN DISTINCT p.city AS c"),
        vec!["c=LON", "c=NYC"]
    );
}

#[test]
fn distinct_deduplicates_across_a_join_that_multiplies_rows() {
    // 6 KNOWS edges, but only 4 distinct people are ever a target, in 2 cities.
    let s = fixture();
    assert_eq!(
        bag(
            &s,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b.city AS c"
        ),
        vec!["c=LON", "c=NYC"]
    );
    assert_eq!(
        bag(
            &s,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b.name AS n"
        ),
        vec!["n=Alice", "n=Bob", "n=Carol", "n=Dave", "n=Eve"]
    );
}

#[test]
fn distinct_deduplicates_whole_nodes() {
    // Alice is the target of two KNOWS edges; DISTINCT must return her once.
    let s = fixture();
    let r = bag(
        &s,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b",
    );
    assert_eq!(r.len(), 5, "5 distinct people are known by someone: {r:?}");
}

#[test]
fn distinct_applies_to_the_whole_row_not_to_each_column() {
    // (dept, city) pairs: Eng/NYC twice, Eng/LON, Sales/NYC, Sales/LON, NULL/LON → 5.
    let s = fixture();
    let r = bag(
        &s,
        "MATCH (p:Person) RETURN DISTINCT p.dept AS d, p.city AS c",
    );
    assert_eq!(r.len(), 5, "{r:?}");
    assert!(r.contains(&"c=NYC d=Eng".to_string()), "{r:?}");
    assert!(
        r.contains(&"c=LON d=NULL".to_string()),
        "Frank's null dept is a value: {r:?}"
    );
}

#[test]
fn distinct_treats_null_as_equal_to_null_for_deduplication() {
    // openCypher: DISTINCT groups NULLs together even though NULL = NULL is unknown.
    let s = fixture();
    let r = bag(&s, "MATCH (p:Person) RETURN DISTINCT p.dept AS d");
    assert_eq!(r, vec!["d=Eng", "d=NULL", "d=Sales"], "{r:?}");
}

#[test]
fn distinct_on_already_unique_rows_changes_nothing() {
    let s = fixture();
    assert_eq!(
        bag(&s, "MATCH (p:Person) RETURN DISTINCT p.name AS n").len(),
        6
    );
}

#[test]
fn distinct_composes_with_order_by_at_least_as_far_as_deduplicating() {
    // The dedup half works today; the ordering half is blocked on #356. Asserting the
    // multiset here keeps the DISTINCT coverage live instead of parking it behind an
    // unrelated bug.
    let s = fixture();
    assert_eq!(
        bag(
            &s,
            "MATCH (p:Person) RETURN DISTINCT p.city AS c ORDER BY c"
        ),
        vec!["c=LON", "c=NYC"]
    );
}

#[test]
fn distinct_composes_with_order_by() {
    let s = fixture();
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person) RETURN DISTINCT p.city AS c ORDER BY c"
        ),
        vec!["c=LON", "c=NYC"]
    );
}

#[test]
fn distinct_composes_with_limit() {
    // LIMIT applies after deduplication, so 2 distinct cities LIMIT 5 gives 2 rows —
    // not 5 rows of duplicates.
    let s = fixture();
    assert_eq!(
        rows(&s, "MATCH (p:Person) RETURN DISTINCT p.city AS c LIMIT 5").len(),
        2
    );
}

// ---------------------------------------------------------------------------
// Implicit grouping keys and aggregation  (#301)
// ---------------------------------------------------------------------------

#[test]
fn a_return_containing_only_aggregates_produces_exactly_one_row() {
    let s = fixture();
    assert_eq!(scalar(&s, "MATCH (p:Person) RETURN count(p) AS n"), "n=6");
    assert_eq!(scalar(&s, "MATCH (p:Person) RETURN count(*) AS n"), "n=6");
    assert_eq!(
        scalar(&s, "MATCH (p:Person) RETURN sum(p.salary) AS v"),
        "v=1000"
    );
}

#[test]
fn counting_one_endpoint_of_a_pattern_is_still_a_single_scalar() {
    // The #301 shape. 6 KNOWS edges, so count over either endpoint is 6 — one row.
    // Returning one row per `a` with its out-degree is the bug.
    let s = fixture();
    assert_eq!(
        scalar(
            &s,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(b) AS n"
        ),
        "n=6"
    );
    assert_eq!(
        scalar(
            &s,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(a) AS n"
        ),
        "n=6"
    );
    assert_eq!(
        scalar(
            &s,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS n"
        ),
        "n=6"
    );
}

#[test]
fn count_distinct_over_an_endpoint_is_a_single_scalar() {
    // 6 edges, 5 distinct targets (Alice appears twice).
    let s = fixture();
    assert_eq!(
        scalar(
            &s,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(DISTINCT b) AS n"
        ),
        "n=5"
    );
}

#[test]
fn a_non_aggregate_item_becomes_the_grouping_key() {
    // Eng 3, Sales 2, no dept 1.
    let s = fixture();
    let r = bag(&s, "MATCH (p:Person) RETURN p.dept AS d, count(p) AS n");
    assert_eq!(r, vec!["d=Eng n=3", "d=NULL n=1", "d=Sales n=2"], "{r:?}");
}

#[test]
fn grouping_over_a_pattern_counts_edges_per_group() {
    // KNOWS targets by city: NYC targets are Alice(x2) and Bob = 3; LON are Carol, Dave...
    // by target city: Alice NYC x2, Bob NYC x1, Carol LON x1, Dave NYC x1, Eve LON x1
    //   → NYC 4, LON 2
    let s = fixture();
    let r = bag(
        &s,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.city AS c, count(a) AS n",
    );
    assert_eq!(r, vec!["c=LON n=2", "c=NYC n=4"], "{r:?}");
}

#[test]
fn two_grouping_keys_group_on_the_pair() {
    let s = fixture();
    let r = bag(
        &s,
        "MATCH (p:Person) RETURN p.dept AS d, p.city AS c, count(p) AS n",
    );
    assert_eq!(r.len(), 5, "five distinct (dept, city) pairs: {r:?}");
}

#[test]
fn several_aggregates_share_one_ungrouped_row() {
    // Shape and the three aggregates that handle nulls correctly. `min` is split out
    // below because it is broken for a reason unrelated to grouping (#357).
    let s = fixture();
    let r = rows(
        &s,
        "MATCH (p:Person) RETURN count(p) AS c, sum(p.salary) AS s, max(p.salary) AS mx",
    );
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(r[0], "c=6 mx=300 s=1000");
}

#[test]
fn count_of_a_node_counts_rows() {
    let s = fixture();
    assert_eq!(scalar(&s, "MATCH (p:Person) RETURN count(p) AS n"), "n=6");
    assert_eq!(scalar(&s, "MATCH (p:Person) RETURN count(*) AS n"), "n=6");
}

#[test]
fn count_of_a_property_skips_nulls() {
    let s = fixture();
    assert_eq!(
        scalar(&s, "MATCH (p:Person) RETURN count(p.salary) AS n"),
        "n=5"
    );
    // collect() skips nulls too — render() sorts the array so this is order-independent
    assert_eq!(
        rows(&s, "MATCH (p:Person) RETURN collect(p.salary) AS a"),
        vec!["a=[100,150,200,250,300]"]
    );
}

#[test]
fn sum_and_max_ignore_nulls() {
    let s = fixture();
    assert_eq!(
        scalar(&s, "MATCH (p:Person) RETURN sum(p.salary) AS v"),
        "v=1000"
    );
    assert_eq!(
        scalar(&s, "MATCH (p:Person) RETURN max(p.salary) AS v"),
        "v=300"
    );
    // avg divides by the 5 non-null salaries, not by 6 rows
    assert_eq!(
        scalar(&s, "MATCH (p:Person) RETURN avg(p.salary) AS v"),
        "v=200.0000"
    );
}

#[test]
fn min_ignores_nulls() {
    let s = fixture();
    assert_eq!(
        scalar(&s, "MATCH (p:Person) RETURN min(p.salary) AS v"),
        "v=100"
    );
}

#[test]
fn a_filtered_empty_match_still_yields_one_row_for_count_and_zero_for_a_bare_projection() {
    let s = fixture();
    assert_eq!(
        scalar(
            &s,
            "MATCH (p:Person) WHERE p.name = \"Nobody\" RETURN count(p) AS n"
        ),
        "n=0"
    );
    assert_eq!(
        rows(
            &s,
            "MATCH (p:Person) WHERE p.name = \"Nobody\" RETURN p.name AS n"
        )
        .len(),
        0
    );
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

#[test]
fn order_by_a_property_expression_sorts() {
    // The spelling that works today, kept live so a regression here is caught even while
    // the alias form (#356) is parked.
    let s = fixture();
    let r = rows(
        &s,
        "MATCH (p:Person) RETURN p.name AS n, p.salary AS v ORDER BY p.salary DESC",
    );
    // openCypher orders NULL as the greatest value, so DESC leads with it (#369).
    assert_eq!(r[0], "n=Frank v=NULL", "nulls first on DESC: {r:?}");
    assert_eq!(r[1], "n=Carol v=300", "then the highest salary: {r:?}");
}

#[test]
fn order_by_a_projected_alias_sorts() {
    let s = fixture();
    let r = rows(
        &s,
        "MATCH (p:Person) RETURN p.name AS n, p.salary AS v ORDER BY v DESC",
    );
    assert_eq!(r[0], "n=Frank v=NULL", "nulls first on DESC (#369): {r:?}");
    assert_eq!(r[1], "n=Carol v=300", "then the highest salary: {r:?}");
    assert_eq!(r[r.len() - 1], "n=Alice v=100", "lowest last: {r:?}");
}

#[test]
fn order_by_an_aggregate_alias_sorts_groups() {
    let s = fixture();
    let r = rows(
        &s,
        "MATCH (p:Person) RETURN p.dept AS d, count(p) AS n ORDER BY n DESC",
    );
    assert_eq!(r[0], "d=Eng n=3", "{r:?}");
}

#[test]
fn order_by_with_limit_returns_the_true_top_k() {
    let s = fixture();
    // DESC order is NULL, 300, 250, 200, 150, 100 — null is the greatest value (#369),
    // so the true top 2 includes it. Surprising, but it is what openCypher specifies.
    let r = rows(
        &s,
        "MATCH (p:Person) RETURN p.name AS n, p.salary AS v ORDER BY v DESC LIMIT 2",
    );
    assert_eq!(r, vec!["n=Frank v=NULL", "n=Carol v=300"], "{r:?}");
    // ASC has no such wrinkle: the smallest real value leads.
    let asc = rows(
        &s,
        "MATCH (p:Person) RETURN p.name AS n, p.salary AS v ORDER BY v ASC LIMIT 2",
    );
    assert_eq!(asc, vec!["n=Alice v=100", "n=Dave v=150"], "{asc:?}");
}

#[test]
fn order_by_a_repeated_aggregate_expression_sorts_like_its_alias() {
    let s = fixture();
    let by_expr = rows(
        &s,
        "MATCH (p:Person) RETURN p.dept AS d, sum(p.salary) AS v ORDER BY sum(p.salary) DESC",
    );
    let by_alias = rows(
        &s,
        "MATCH (p:Person) RETURN p.dept AS d, sum(p.salary) AS v ORDER BY v DESC",
    );
    assert_eq!(by_expr, by_alias);
}

// ---------------------------------------------------------------------------
// Neighbouring semantics this stage depends on — recorded, not fixed here
// ---------------------------------------------------------------------------

#[test]
fn exists_subquery_applies_inline_property_constraints() {
    let s = fixture();
    assert_eq!(
        scalar(
            &s,
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(o:Person {name: \"Alice\"}) } RETURN count(p) AS n"
        ),
        "n=2",
        "Bob and Carol both know Alice"
    );
}

#[test]
fn not_binds_looser_than_string_comparison() {
    let s = fixture();
    assert_eq!(
        scalar(
            &s,
            "MATCH (p:Person) WHERE NOT p.name STARTS WITH \"A\" RETURN count(p) AS n"
        ),
        "n=5"
    );
}

#[test]
fn is_not_null_matches_only_nodes_carrying_the_property() {
    // #312 claims this over-matches on mixed-schema data; it holds on this fixture, so the
    // assertion stays as a guard while that issue is narrowed.
    let s = fixture();
    assert_eq!(
        scalar(
            &s,
            "MATCH (p:Person) WHERE p.dept IS NOT NULL RETURN count(p) AS n"
        ),
        "n=5"
    );
    assert_eq!(
        scalar(
            &s,
            "MATCH (p:Person) WHERE p.dept IS NULL RETURN count(p) AS n"
        ),
        "n=1"
    );
}

#[test]
fn order_by_resolves_the_same_way_on_specialized_plans() {
    // The adjacency-count-aggregate plan (ADR-017) projects and then sorts, so an ORDER BY
    // written as a property or a repeated aggregate has to be translated to the emitted
    // alias. This shape gets a different physical plan from the generic aggregate above,
    // and it regressed independently — hence its own test rather than trust by proximity.
    let s = fixture();
    // KNOWS targets per city: NYC 4, LON 2.
    let by_alias = rows(
        &s,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.city AS c, count(a) AS n ORDER BY n DESC",
    );
    let by_aggregate = rows(
        &s,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.city AS c, count(a) AS n ORDER BY count(a) DESC",
    );
    assert_eq!(by_alias, vec!["c=NYC n=4", "c=LON n=2"], "{by_alias:?}");
    assert_eq!(by_aggregate, by_alias, "both spellings must agree");

    // ...and ordering by the grouping property itself, which is neither the alias nor an
    // aggregate.
    let by_group = rows(
        &s,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.city AS c, count(a) AS n ORDER BY b.city DESC",
    );
    assert_eq!(by_group, vec!["c=NYC n=4", "c=LON n=2"], "{by_group:?}");
}

#[test]
fn every_sort_key_is_honoured_not_just_the_first() {
    // #362 also reported "only the first sort key is honoured". It is the same defect:
    // a key that does not resolve evaluates to null for every row, so it contributes
    // nothing to the comparison and the rows appear ordered by the remaining keys only.
    // Once aliases resolve, secondary keys work — asserted here in all three spellings
    // so the symptom cannot come back through a different door.
    let s = fixture();
    let expected = vec![
        "c=LON n=Carol",
        "c=LON n=Eve",
        "c=LON n=Frank",
        "c=NYC n=Alice",
        "c=NYC n=Bob",
        "c=NYC n=Dave",
    ];
    for query in [
        "MATCH (p:Person) RETURN p.city AS c, p.name AS n ORDER BY p.city ASC, p.name ASC",
        "MATCH (p:Person) RETURN p.city AS c, p.name AS n ORDER BY c ASC, n ASC",
        "MATCH (p:Person) RETURN p.city AS c, p.name AS n ORDER BY c ASC, p.name ASC",
    ] {
        assert_eq!(rows(&s, query), expected, "sorting by two keys: {query}");
    }

    // and the secondary key's own direction is respected
    let desc_second = rows(
        &s,
        "MATCH (p:Person) RETURN p.city AS c, p.name AS n ORDER BY c ASC, n DESC",
    );
    assert_eq!(desc_second[0], "c=LON n=Frank", "{desc_second:?}");
}

#[test]
fn order_by_with_limit_on_a_grouped_aggregate_returns_the_true_top_k() {
    let s = fixture();
    let r = rows(
        &s,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.city AS c, count(a) AS n ORDER BY count(a) DESC LIMIT 1",
    );
    assert_eq!(r, vec!["c=NYC n=4"], "{r:?}");
}

#[test]
fn null_ordering_follows_opencypher() {
    // openCypher orders NULL as greater than every value, so it lands last on ASC and
    // first on DESC. The engine currently does the opposite.
    let s = fixture();
    let asc = rows(
        &s,
        "MATCH (p:Person) RETURN p.name AS n, p.salary AS v ORDER BY v ASC",
    );
    assert_eq!(
        asc[asc.len() - 1],
        "n=Frank v=NULL",
        "nulls last on ASC: {asc:?}"
    );
    let desc = rows(
        &s,
        "MATCH (p:Person) RETURN p.name AS n, p.salary AS v ORDER BY v DESC",
    );
    assert_eq!(desc[0], "n=Frank v=NULL", "nulls first on DESC: {desc:?}");
}

// ---------------------------------------------------------------------------
// Multi-clause joins (#360)
// ---------------------------------------------------------------------------

#[test]
fn a_second_match_joins_on_every_shared_variable() {
    // Two MATCH clauses sharing *two* variables. Joining on only one of them leaves the
    // other uncorrelated and yields a cartesian product across it — and because the join
    // key came from a HashSet intersection, which one was enforced varied between runs of
    // the same query on the same data.
    let mut s = GraphStore::new();
    let mut mk = |s: &mut GraphStore, label: &str, id: &str| {
        let n = s.create_node(label);
        s.set_column_property(n, "id", PropertyValue::String(id.into()));
        n
    };
    let board = mk(&mut s, "Board", "B1");
    let ma = mk(&mut s, "Model", "MA");
    let mb = mk(&mut s, "Model", "MB");
    for (vid, precision, model) in [
        ("MA32", "fp32", ma),
        ("MA8", "int8", ma),
        ("MB32", "fp32", mb),
        ("MB8", "int8", mb),
    ] {
        let v = mk(&mut s, "Variant", vid);
        s.set_column_property(v, "precision", PropertyValue::String(precision.into()));
        s.create_edge(v, model, "VARIANT_OF").unwrap();
        let d = s.create_node("Deploy");
        s.create_edge(d, v, "OF").unwrap();
        s.create_edge(d, board, "ON").unwrap();
    }

    // Each model's fp32 variant paired with *its own* int8 variant: 2 rows, not 4.
    let r = bag(
        &s,
        "MATCH (b:Board)<-[:ON]-(d1:Deploy)-[:OF]->(v1:Variant)-[:VARIANT_OF]->(m:Model) \
         WHERE v1.precision = \"fp32\" \
         MATCH (b)<-[:ON]-(d2:Deploy)-[:OF]->(v2:Variant)-[:VARIANT_OF]->(m) \
         WHERE v2.precision = \"int8\" \
         RETURN m.id AS model, v1.id AS fp32, v2.id AS int8",
    );
    assert_eq!(
        r,
        vec![
            "fp32=MA32 int8=MA8 model=MA",
            "fp32=MB32 int8=MB8 model=MB",
        ],
        "a variant must pair only with its own model's other variant: {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Deletion hygiene (#364)
// ---------------------------------------------------------------------------

#[test]
fn a_deleted_nodes_property_does_not_reappear_on_its_successor() {
    // Node ids are recycled through a free list, so the next CREATE gets the deleted
    // node's slot. If deletion leaves the columnar row behind, the new node inherits the
    // old value for any property it does not itself set — deleted data reappearing on new
    // data, with nothing in the query to hint at it.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE (:Ghost {id: \"a\", secret: \"LEAKED\"})",
            &mut s,
            "default",
        )
        .unwrap();
    engine
        .execute_mut("MATCH (n:Ghost) DETACH DELETE n", &mut s, "default")
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (n:Ghost) RETURN count(n) AS n"), "n=0");

    // the successor sets `id` but never `secret`
    engine
        .execute_mut("CREATE (:Ghost {id: \"b\"})", &mut s, "default")
        .unwrap();
    let r = bag(&s, "MATCH (n:Ghost) RETURN n.id AS id, n.secret AS secret");
    assert_eq!(r, vec!["id=b secret=NULL"], "{r:?}");

    // and again after a global wipe
    engine
        .execute_mut("MATCH (n) DETACH DELETE n", &mut s, "default")
        .unwrap();
    engine
        .execute_mut("CREATE (:Ghost {id: \"c\"})", &mut s, "default")
        .unwrap();
    let r = bag(&s, "MATCH (n:Ghost) RETURN n.id AS id, n.secret AS secret");
    assert_eq!(r, vec!["id=c secret=NULL"], "{r:?}");
}

// ---------------------------------------------------------------------------
// Three-valued logic in WHERE (#398)
// ---------------------------------------------------------------------------

#[test]
fn comparisons_against_a_missing_property_are_unknown_not_true() {
    // Cypher has three truth values. A comparison where either side is null is *unknown*,
    // and WHERE keeps only rows that are definitely true. The dangerous case is `<>`:
    // read as two-valued boolean logic, "the property is not 1" looks true for a row that
    // has no such property at all, so the filter kept every row instead of none — a
    // silently inverted predicate, the worst kind of wrong answer because it looks like a
    // successful query returning data.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    for id in ["a", "b", "c"] {
        engine
            .execute_mut(&format!("CREATE (:Blank {{id: \"{id}\"}})"), &mut s, "default")
            .unwrap();
    }

    for pred in [
        "n.missing <> 1",
        "n.missing = 1",
        "n.missing > 0",
        "n.missing < 0",
        "n.missing >= 0",
        "n.missing <= 0",
        "n.missing = null",
        "n.missing <> null",
    ] {
        assert_eq!(
            scalar(
                &s,
                &format!("MATCH (n:Blank) WHERE {pred} RETURN count(n) AS n")
            ),
            "n=0",
            "`WHERE {pred}` must match nothing: unknown is not true"
        );
    }

    // IS NULL / IS NOT NULL are the operators that *can* see nullness, and still do.
    assert_eq!(
        scalar(
            &s,
            "MATCH (n:Blank) WHERE n.missing IS NULL RETURN count(n) AS n"
        ),
        "n=3"
    );
    assert_eq!(
        scalar(
            &s,
            "MATCH (n:Blank) WHERE n.missing IS NOT NULL RETURN count(n) AS n"
        ),
        "n=0"
    );

    // A present property still compares normally — the null rule must not swallow real
    // predicates.
    assert_eq!(
        scalar(&s, "MATCH (n:Blank) WHERE n.id <> \"a\" RETURN count(n) AS n"),
        "n=2"
    );
    assert_eq!(
        scalar(&s, "MATCH (n:Blank) WHERE n.id = \"a\" RETURN count(n) AS n"),
        "n=1"
    );
}

// ---------------------------------------------------------------------------
// CREATE builds the pattern it was given (#400)
// ---------------------------------------------------------------------------

fn edge_count(s: &GraphStore) -> String {
    scalar(s, "MATCH ()-[r]->() RETURN count(r) AS n")
}

#[test]
fn create_wires_edges_even_when_endpoints_are_anonymous() {
    // Edges were wired by variable name, so an endpoint written as `(:Label)` had no name
    // to wire to and the relationship was dropped. The nodes were still created and no
    // error was raised, so a bulk load of `CREATE (:A {..})-[:R]->(:B {..})` produced a
    // graph with every node and not one edge -- the shape most load scripts are written in.
    let engine = QueryEngine::new();

    for pattern in [
        "CREATE (a:A {id: 1})-[:R]->(b:B {id: 2})", // both named (always worked)
        "CREATE (a:A {id: 1})-[:R]->(:B {id: 2})",  // tail anonymous
        "CREATE (:A {id: 1})-[:R]->(b:B {id: 2})",  // head anonymous
        "CREATE (:A {id: 1})-[:R]->(:B {id: 2})",   // both anonymous
        "CREATE (:A {id: 1})-[r:R]->(:B {id: 2})",  // named rel, anonymous endpoints
    ] {
        let mut s = GraphStore::new();
        engine.execute_mut(pattern, &mut s, "default").unwrap();
        assert_eq!(edge_count(&s), "n=1", "no edge created by: {pattern}");
        assert_eq!(scalar(&s, "MATCH (n) RETURN count(n) AS n"), "n=2", "{pattern}");
    }

    // Multi-segment paths wire every segment, named or not.
    for pattern in [
        "CREATE (a:A)-[:R]->(b:B)-[:R2]->(c:C)",
        "CREATE (:A)-[:R]->(:B)-[:R2]->(:C)",
        "CREATE (a:A)-[:R]->(:B)-[:R2]->(c:C)",
    ] {
        let mut s = GraphStore::new();
        engine.execute_mut(pattern, &mut s, "default").unwrap();
        assert_eq!(edge_count(&s), "n=2", "wrong edge count for: {pattern}");
    }
}

#[test]
fn create_honours_the_direction_the_pattern_was_written_in() {
    // `plan_create_only` wired source -> target in written order and never consulted the
    // segment's direction, so a `<-` pattern stored an edge pointing the *opposite* way.
    // Every query against it then silently returns nothing, or the wrong endpoint.
    let engine = QueryEngine::new();

    let mut s = GraphStore::new();
    engine
        .execute_mut("CREATE (a:A {id: 1})<-[:R]-(b:B {id: 2})", &mut s, "default")
        .unwrap();
    assert_eq!(edge_count(&s), "n=1");
    assert_eq!(
        scalar(&s, "MATCH (b:B)-[:R]->(a:A) RETURN count(*) AS n"),
        "n=1",
        "`(a)<-[:R]-(b)` must store b -> a"
    );
    assert_eq!(
        scalar(&s, "MATCH (a:A)-[:R]->(b:B) RETURN count(*) AS n"),
        "n=0",
        "the edge must not also point a -> b"
    );

    // Same, with anonymous endpoints.
    let mut s = GraphStore::new();
    engine
        .execute_mut("CREATE (:A {id: 1})<-[:R]-(:B {id: 2})", &mut s, "default")
        .unwrap();
    assert_eq!(
        scalar(&s, "MATCH (b:B)-[:R]->(a:A) RETURN count(*) AS n"),
        "n=1"
    );

    // And the forward form is unchanged.
    let mut s = GraphStore::new();
    engine
        .execute_mut("CREATE (a:A {id: 1})-[:R]->(b:B {id: 2})", &mut s, "default")
        .unwrap();
    assert_eq!(
        scalar(&s, "MATCH (a:A)-[:R]->(b:B) RETURN count(*) AS n"),
        "n=1"
    );
}

// ---------------------------------------------------------------------------
// MATCH ... CREATE builds new nodes, and MATCH/WHERE groups chain (#305, #402)
// ---------------------------------------------------------------------------

#[test]
fn match_create_creates_the_unbound_nodes_in_its_pattern() {
    // `MATCH (p) CREATE (p)-[:R]->(c:C {..})` wired edges only between variables the MATCH
    // had already bound; a node appearing for the first time in the CREATE pattern was
    // dropped, taking its edge with it. The statement reported success and changed nothing,
    // which is how the fixture in this very file's sibling tests came to be silently empty.
    let engine = QueryEngine::new();

    for (pattern, want_dir) in [
        ("MATCH (p:P) CREATE (p)-[:R]->(c:C {id: 1})", "forward"),
        ("MATCH (p:P) CREATE (p)-[:R]->(:C {id: 1})", "forward"), // anonymous new node
        ("MATCH (p:P) CREATE (p)<-[:R]-(c:C {id: 1})", "reverse"),
    ] {
        let mut s = GraphStore::new();
        engine
            .execute_mut("CREATE (:P {id: 0})", &mut s, "default")
            .unwrap();
        engine.execute_mut(pattern, &mut s, "default").unwrap();

        assert_eq!(scalar(&s, "MATCH (n) RETURN count(n) AS n"), "n=2", "{pattern}");
        assert_eq!(edge_count(&s), "n=1", "{pattern}");
        let fwd = scalar(&s, "MATCH (:P)-[:R]->(:C) RETURN count(*) AS n");
        let rev = scalar(&s, "MATCH (:C)-[:R]->(:P) RETURN count(*) AS n");
        match want_dir {
            "forward" => assert_eq!((fwd.as_str(), rev.as_str()), ("n=1", "n=0"), "{pattern}"),
            _ => assert_eq!((fwd.as_str(), rev.as_str()), ("n=0", "n=1"), "{pattern}"),
        }
    }

    // CREATE runs once per matched row.
    let mut s = GraphStore::new();
    for id in 0..3 {
        engine
            .execute_mut(&format!("CREATE (:P {{id: {id}}})"), &mut s, "default")
            .unwrap();
    }
    engine
        .execute_mut("MATCH (p:P) CREATE (p)-[:R]->(:C {tag: 1})", &mut s, "default")
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (c:C) RETURN count(c) AS n"), "n=3");
    assert_eq!(edge_count(&s), "n=3");
}

#[test]
fn match_where_groups_chain_beyond_two() {
    // The grammar hand-unrolled exactly two `MATCH+ WHERE?` groups before the first WITH,
    // so a third MATCH after a second WHERE was a *parse error* — the multi-hop cohort
    // shape that analytic queries are written in. Asserting semantics, not just parsing:
    // each added clause must actually narrow the result.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    // p1 has all three attachments, p2 two, p3 one.
    for (p, drug, lab) in [("p1", true, true), ("p2", true, false), ("p3", false, false)] {
        engine
            .execute_mut(&format!("CREATE (:P {{n: \"{p}\"}})"), &mut s, "default")
            .unwrap();
        engine
            .execute_mut(
                &format!("MATCH (p:P {{n: \"{p}\"}}) CREATE (p)-[:HAS_CONDITION]->(:C {{x: \"Diabetes\"}})"),
                &mut s, "default",
            )
            .unwrap();
        if drug {
            engine.execute_mut(
                &format!("MATCH (p:P {{n: \"{p}\"}}) CREATE (p)-[:PRESCRIBED]->(:D {{y: \"Metformin\"}})"),
                &mut s, "default").unwrap();
        }
        if lab {
            engine.execute_mut(
                &format!("MATCH (p:P {{n: \"{p}\"}}) CREATE (p)-[:MEASURED]->(:M {{z: \"HbA1c\"}})"),
                &mut s, "default").unwrap();
        }
    }

    let base = "MATCH (p:P)-[:HAS_CONDITION]->(c:C) WHERE c.x CONTAINS \"Diabetes\" \
                MATCH (p)-[:PRESCRIBED]->(d:D) WHERE d.y CONTAINS \"Metformin\"";
    assert_eq!(scalar(&s, &format!("{base} RETURN count(p) AS n")), "n=2");
    // third group: parses *and* narrows
    assert_eq!(
        scalar(&s, &format!("{base} MATCH (p)-[:MEASURED]->(m:M) RETURN count(p) AS n")),
        "n=1"
    );
    assert_eq!(
        scalar(&s, &format!("{base} MATCH (p)-[:MEASURED]->(m:M) WHERE m.z = \"HbA1c\" RETURN count(p) AS n")),
        "n=1"
    );
    // a predicate in the third group must be able to exclude everything — proving it is
    // applied rather than parsed and discarded
    assert_eq!(
        scalar(&s, &format!("{base} MATCH (p)-[:MEASURED]->(m:M) WHERE m.z = \"NOPE\" RETURN count(p) AS n")),
        "n=0"
    );
    // and an earlier group's predicate is still ANDed in, not overwritten
    assert_eq!(
        scalar(&s, &"MATCH (p:P)-[:HAS_CONDITION]->(c:C) WHERE c.x CONTAINS \"Diabetes\" \
                     MATCH (p)-[:PRESCRIBED]->(d:D) WHERE d.y = \"NOPE\" \
                     MATCH (p)-[:MEASURED]->(m:M) RETURN count(p) AS n".to_string()),
        "n=0"
    );
}

// ---------------------------------------------------------------------------
// MERGE over a relationship pattern (#306)
// ---------------------------------------------------------------------------

#[test]
fn merge_on_a_relationship_pattern_creates_and_then_matches_the_whole_pattern() {
    // MERGE only ever inspected the first node of its pattern and ignored the segments, so
    // a relationship MERGE created no edge and reported success. openCypher treats a MERGE
    // pattern as all-or-nothing: match the whole pattern, else create the whole pattern.
    let engine = QueryEngine::new();

    let mut s = GraphStore::new();
    for run in 1..=3 {
        engine
            .execute_mut(
                "MERGE (a:X {k: 1})-[:R]->(b:Y {k: 2}) RETURN a.k AS k",
                &mut s,
                "default",
            )
            .unwrap();
        // idempotent: the second and third runs must match, not create again
        assert_eq!(scalar(&s, "MATCH (n) RETURN count(n) AS n"), "n=2", "run {run}");
        assert_eq!(edge_count(&s), "n=1", "run {run}");
    }

    // direction comes from the pattern
    let mut s = GraphStore::new();
    engine
        .execute_mut("MERGE (a:X {k: 1})<-[:R]-(b:Y {k: 2}) RETURN a.k AS k", &mut s, "default")
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (:Y)-[:R]->(:X) RETURN count(*) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (:X)-[:R]->(:Y) RETURN count(*) AS n"), "n=0");

    // multi-segment paths, also idempotent
    let mut s = GraphStore::new();
    for _ in 0..2 {
        engine
            .execute_mut(
                "MERGE (a:X {k: 1})-[:R]->(b:Y {k: 2})-[:R2]->(c:Z {k: 3}) RETURN a.k AS k",
                &mut s, "default",
            )
            .unwrap();
    }
    assert_eq!(scalar(&s, "MATCH (n) RETURN count(n) AS n"), "n=3");
    assert_eq!(edge_count(&s), "n=2");

    // a pattern differing only in edge type is a *different* pattern, so it is created
    let mut s = GraphStore::new();
    engine.execute_mut("MERGE (a:X {k: 1})-[:R]->(b:Y {k: 2}) RETURN a.k AS k", &mut s, "default").unwrap();
    engine.execute_mut("MERGE (a:X {k: 1})-[:OTHER]->(b:Y {k: 2}) RETURN a.k AS k", &mut s, "default").unwrap();
    assert_eq!(edge_count(&s), "n=2");
}

#[test]
fn merge_between_already_bound_nodes_reuses_them() {
    // The idiomatic way to add an edge between *existing* nodes is to bind them first.
    // This form reuses the matched nodes rather than creating fresh ones, and is the
    // reason a standalone MERGE creating new nodes is not a bug but the documented
    // openCypher behaviour.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine.execute_mut("CREATE (:Repro {k: 10})", &mut s, "default").unwrap();
    engine.execute_mut("CREATE (:Repro {k: 11})", &mut s, "default").unwrap();

    for _ in 0..2 {
        engine
            .execute_mut(
                "MATCH (a:Repro {k: 10}), (b:Repro {k: 11}) MERGE (a)-[:R]->(b) RETURN a.k AS k",
                &mut s, "default",
            )
            .unwrap();
        assert_eq!(scalar(&s, "MATCH (n) RETURN count(n) AS n"), "n=2", "must not duplicate nodes");
        assert_eq!(edge_count(&s), "n=1", "must not duplicate the edge");
    }
}

#[test]
fn merge_on_create_and_on_match_fire_on_the_right_branch() {
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut(
            "MERGE (a:X {k: 1})-[:R]->(b:Y {k: 2}) ON CREATE SET a.tag = \"created\" RETURN a.k AS k",
            &mut s, "default",
        )
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (a:X) RETURN a.tag AS tag"), "tag=created");

    engine
        .execute_mut(
            "MERGE (a:X {k: 1})-[:R]->(b:Y {k: 2}) ON MATCH SET a.tag = \"matched\" RETURN a.k AS k",
            &mut s, "default",
        )
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (a:X) RETURN a.tag AS tag"), "tag=matched");
    assert_eq!(scalar(&s, "MATCH (n) RETURN count(n) AS n"), "n=2", "the second MERGE matched");
}

// ---------------------------------------------------------------------------
// Pattern predicates, modern constraint syntax, and unique enforcement (#367)
// ---------------------------------------------------------------------------

#[test]
fn a_relationship_pattern_can_be_used_as_a_predicate() {
    // `WHERE (:Acc)-[:SUPPORTS]->(o)` is the natural way to write "has a ...", and its
    // negation is the natural way to write "has no ..." — the whole point of coverage and
    // gap analysis. Both were parse errors, forcing the OPTIONAL MATCH + count = 0 idiom.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine.execute_mut("CREATE (:Op {name: \"op1\"})", &mut s, "default").unwrap();
    engine.execute_mut("CREATE (:Op {name: \"op2\"})", &mut s, "default").unwrap();
    engine
        .execute_mut(
            "MATCH (o:Op {name: \"op1\"}) CREATE (:Acc {name: \"a1\"})-[:SUPPORTS]->(o)",
            &mut s, "default",
        )
        .unwrap();

    assert_eq!(
        bag(&s, "MATCH (o:Op) WHERE (:Acc)-[:SUPPORTS]->(o) RETURN o.name AS name"),
        vec!["name=op1"]
    );
    assert_eq!(
        bag(&s, "MATCH (o:Op) WHERE NOT (:Acc)-[:SUPPORTS]->(o) RETURN o.name AS name"),
        vec!["name=op2"]
    );
    // must agree with the EXISTS { } form it desugars to
    assert_eq!(
        bag(&s, "MATCH (o:Op) WHERE EXISTS { MATCH (:Acc)-[:SUPPORTS]->(o) } RETURN o.name AS name"),
        vec!["name=op1"]
    );

    // Parenthesised expressions must not be mistaken for patterns.
    let mut s2 = GraphStore::new();
    engine.execute_mut("CREATE (:N {a: 1, b: 2})", &mut s2, "default").unwrap();
    assert_eq!(scalar(&s2, "MATCH (n:N) WHERE (n.a + n.b) = 3 RETURN count(n) AS n"), "n=1");
    assert_eq!(scalar(&s2, "MATCH (n:N) RETURN (n.a * (n.b + 1)) AS v"), "v=3");
}

#[test]
fn unique_constraints_accept_modern_syntax_and_are_actually_enforced() {
    // The registry, the per-constraint index and `check_unique_constraint` all existed but
    // nothing on the write path called them, so a constraint was accepted, listed by
    // SHOW CONSTRAINTS, and enforced nothing — a double load silently produced duplicates
    // while appearing to be protected against exactly that.
    let engine = QueryEngine::new();

    // all three spellings register the same constraint
    for stmt in [
        "CREATE CONSTRAINT ON (n:Kernel) ASSERT n.id IS UNIQUE",
        "CREATE CONSTRAINT kid IF NOT EXISTS FOR (n:Kernel) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT FOR (n:Kernel) REQUIRE n.id IS UNIQUE",
    ] {
        let mut s = GraphStore::new();
        engine.execute_mut(stmt, &mut s, "default").unwrap();
        let shown = bag(&s, "SHOW CONSTRAINTS");
        assert_eq!(shown.len(), 1, "{stmt}");
        assert!(shown[0].contains("Kernel"), "{stmt}: {shown:?}");
        assert!(shown[0].contains("id"), "{stmt}: {shown:?}");
    }

    let mut s = GraphStore::new();
    engine
        .execute_mut("CREATE CONSTRAINT FOR (n:Kernel) REQUIRE n.id IS UNIQUE", &mut s, "default")
        .unwrap();
    engine.execute_mut("CREATE (:Kernel {id: 1})", &mut s, "default").unwrap();

    // the duplicate is rejected ...
    assert!(engine.execute_mut("CREATE (:Kernel {id: 1})", &mut s, "default").is_err());
    // ... and leaves nothing behind: a rejected statement must not half-apply
    assert_eq!(scalar(&s, "MATCH (k:Kernel) RETURN count(k) AS n"), "n=1");

    // a distinct value is fine
    engine.execute_mut("CREATE (:Kernel {id: 2})", &mut s, "default").unwrap();
    assert_eq!(scalar(&s, "MATCH (k:Kernel) RETURN count(k) AS n"), "n=2");

    // re-setting a node's own value is not a violation; taking another's is
    engine
        .execute_mut("MATCH (k:Kernel {id: 2}) SET k.id = 2 RETURN k.id AS i", &mut s, "default")
        .unwrap();
    assert!(engine
        .execute_mut("MATCH (k:Kernel {id: 2}) SET k.id = 1 RETURN k.id AS i", &mut s, "default")
        .is_err());

    // graphs with no constraints are unaffected
    let mut s2 = GraphStore::new();
    engine.execute_mut("CREATE (:Free {id: 1})", &mut s2, "default").unwrap();
    engine.execute_mut("CREATE (:Free {id: 1})", &mut s2, "default").unwrap();
    assert_eq!(scalar(&s2, "MATCH (f:Free) RETURN count(f) AS n"), "n=2");

    // creating a constraint over pre-existing duplicates still fails
    let mut s3 = GraphStore::new();
    engine.execute_mut("CREATE (:K {id: 1})", &mut s3, "default").unwrap();
    engine.execute_mut("CREATE (:K {id: 1})", &mut s3, "default").unwrap();
    assert!(engine
        .execute_mut("CREATE CONSTRAINT FOR (n:K) REQUIRE n.id IS UNIQUE", &mut s3, "default")
        .is_err());
}

// ---------------------------------------------------------------------------
// UNWIND as a leading clause (#307)
// ---------------------------------------------------------------------------

#[test]
fn unwind_can_lead_a_statement() {
    // UNWIND was already allowed after a MATCH or WITH, but a *leading* one had no rule to
    // match, so `UNWIND [...] AS x ...` failed at column 1 — the shape batch and
    // parameterized writes are written in.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine.execute_mut("CREATE (:P {name: \"a\", n: 1})", &mut s, "default").unwrap();
    engine.execute_mut("CREATE (:P {name: \"b\", n: 2})", &mut s, "default").unwrap();

    assert_eq!(bag(&s, "UNWIND [1, 2, 3] AS x RETURN x").len(), 3);
    assert_eq!(bag(&s, "UNWIND [] AS x RETURN x").len(), 0);
    assert_eq!(bag(&s, "UNWIND [1, 2, 3] AS x RETURN x LIMIT 2").len(), 2);
    assert_eq!(
        bag(&s, "UNWIND [\"a\", \"b\"] AS s RETURN s"),
        vec!["s=a", "s=b"]
    );
    assert_eq!(scalar(&s, "UNWIND [1, 2, 3] AS x RETURN count(x) AS n"), "n=3");

    // A leading UNWIND feeding a MATCH: the predicate references the unwound variable, so
    // the Unwind must be planned *below* the filter. It was previously pushed above it,
    // and the query died with "Variable not found: x".
    assert_eq!(
        bag(&s, "UNWIND [1, 2] AS x MATCH (p:P) WHERE p.n = x RETURN p.name AS name").len(),
        2
    );
    assert_eq!(
        bag(&s, "UNWIND [1] AS x MATCH (p:P) WHERE p.n = x RETURN p.name AS name"),
        vec!["name=a"]
    );
    // ... and a value matching nothing yields nothing, so the predicate is really applied
    assert_eq!(
        bag(&s, "UNWIND [99] AS x MATCH (p:P) WHERE p.n = x RETURN p.name AS name").len(),
        0
    );

    // A trailing UNWIND still cross-products, unchanged.
    assert_eq!(
        bag(&s, "MATCH (p:P) UNWIND [1, 2] AS x RETURN p.name AS name, x").len(),
        4
    );
    // and one fed from an aggregate still works
    assert_eq!(
        bag(&s, "MATCH (p:P) WITH collect(p.n) AS ns UNWIND ns AS x RETURN x").len(),
        2
    );
}

// ---------------------------------------------------------------------------
// String escapes, bare SET after MERGE, WHERE after YIELD (#308, #309, #348)
// ---------------------------------------------------------------------------

#[test]
fn string_literals_support_escape_sequences() {
    // The literal ran to the first matching quote with no escape handling, so a string
    // containing its own quote character could not be written at all, and `\n` produced a
    // backslash followed by an `n` rather than a newline.
    let s = GraphStore::new();

    assert_eq!(scalar(&s, r#"RETURN "it\"s" AS v"#), "v=it\"s");
    assert_eq!(scalar(&s, r#"RETURN 'it\'s' AS v"#), "v=it's");
    assert_eq!(scalar(&s, r#"RETURN "a\nb" AS v"#), "v=a\nb");
    assert_eq!(scalar(&s, r#"RETURN "a\tb" AS v"#), "v=a\tb");
    assert_eq!(scalar(&s, r#"RETURN "back\\slash" AS v"#), "v=back\\slash");
    assert_eq!(scalar(&s, r#"RETURN "ABC" AS v"#), "v=ABC");
    // a quote of the *other* kind needs no escape
    assert_eq!(scalar(&s, r#"RETURN "single 'inside'" AS v"#), "v=single 'inside'");
    // unescaped strings are untouched
    assert_eq!(scalar(&s, r#"RETURN "plain" AS v"#), "v=plain");

    // round-trips through storage, and an escaped literal matches what it stored
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(r#"CREATE (:S {v: "say \"hi\""})"#, &mut s, "default")
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (n:S) RETURN n.v AS v"), "v=say \"hi\"");
    assert_eq!(
        scalar(&s, r#"MATCH (n:S) WHERE n.v = "say \"hi\"" RETURN count(n) AS n"#),
        "n=1"
    );
}

#[test]
fn a_bare_set_after_merge_applies_on_both_branches() {
    // Only ON CREATE SET / ON MATCH SET were accepted. A bare SET -- which applies whichever
    // branch MERGE took -- was a parse error, and once parsing was fixed it was still
    // dropped during planning, leaving the property unset with no error.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();

    // create branch
    engine
        .execute_mut("MERGE (m:M {k: 1}) SET m.seen = 1", &mut s, "default")
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (m:M) RETURN m.seen AS v"), "v=1");

    // match branch: the SET must still apply, and must not create a second node
    engine
        .execute_mut("MERGE (m:M {k: 1}) SET m.seen = 2", &mut s, "default")
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (m:M) RETURN m.seen AS v"), "v=2");
    assert_eq!(scalar(&s, "MATCH (m:M) RETURN count(m) AS n"), "n=1");
}

#[test]
fn where_can_filter_yielded_variables_directly() {
    // WHERE only existed inside the MATCH sub-rule, so filtering a CALL's output required
    // an intervening MATCH. Note the intermediate state was worse than the parse error:
    // once the grammar accepted it, the predicate was parsed and silently discarded, so
    // every row came back regardless.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    for label in ["P", "Q", "R"] {
        engine
            .execute_mut(&format!("CREATE (:{label} {{n: 1}})"), &mut s, "default")
            .unwrap();
    }

    assert_eq!(bag(&s, "CALL db.labels() YIELD label RETURN label").len(), 3);
    assert_eq!(
        bag(&s, "CALL db.labels() YIELD label WHERE label = \"P\" RETURN label"),
        vec!["label=P"]
    );
    // a predicate matching nothing must return nothing -- otherwise the filter is being
    // ignored rather than applied
    assert_eq!(
        bag(&s, "CALL db.labels() YIELD label WHERE label = \"ZZZ\" RETURN label").len(),
        0
    );
    assert_eq!(
        bag(&s, "CALL db.labels() YIELD label WHERE label <> \"P\" RETURN label").len(),
        2
    );
}

// ---------------------------------------------------------------------------
// Multi-hop projection keeps columns aligned to their node (#338)
// ---------------------------------------------------------------------------

#[test]
fn projecting_several_properties_across_a_relationship_keeps_them_on_one_node() {
    // Reported as `a.key` and `a.email` in the same row coming from *different* nodes, with
    // nodes that had no such relationship appearing as endpoints. Both are invisible in a
    // row count, so this asserts the pairing itself: key `k{i}` must carry email `e{i}@x`,
    // odd-numbered nodes must carry no email, and only linked nodes may appear at all.
    //
    // Sized so the planner behaves as it would on real data rather than taking a
    // small-input path -- the original report noted the shape was correct on a toy graph.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();

    const N: usize = 300;
    const LINKED: usize = 160; // keys k0..k159 are linked in pairs; the rest are isolated
    for i in 0..N {
        let q = if i % 2 == 0 {
            format!("CREATE (:Person {{key: \"k{i}\", email: \"e{i}@x\", name: \"n{i}\"}})")
        } else {
            format!("CREATE (:Person {{key: \"k{i}\", name: \"n{i}\"}})")
        };
        engine.execute_mut(&q, &mut s, "default").unwrap();
    }
    for i in (0..LINKED).step_by(2) {
        engine
            .execute_mut(
                &format!(
                    "MATCH (a:Person {{key: \"k{i}\"}}), (b:Person {{key: \"k{}\"}}) \
                     CREATE (a)-[:SAME_AS]->(b)",
                    i + 1
                ),
                &mut s, "default",
            )
            .unwrap();
    }

    // control: an isolated node has no email and no relationship
    assert_eq!(scalar(&s, "MATCH (p:Person {key: \"k299\"}) RETURN p.email AS e"), "e=NULL");
    assert_eq!(
        scalar(&s, "MATCH (p:Person {key: \"k298\"})-[:SAME_AS]-(x) RETURN count(x) AS n"),
        "n=0"
    );

    for q in [
        "MATCH (a)-[:SAME_AS]-(b) RETURN a.key AS akey, a.email AS aemail",
        "MATCH (a:Person)-[:SAME_AS]-(b:Person) RETURN a.key AS akey, a.email AS aemail",
        // projection order must not matter
        "MATCH (a)-[:SAME_AS]-(b) RETURN a.email AS aemail, a.key AS akey",
    ] {
        let r = rows(&s, q);
        assert_eq!(r.len(), LINKED, "{q}");
        for row in &r {
            // rows render as "akey=k12  aemail=e12@x" (cells sorted by column name)
            let key = row
                .split_whitespace()
                .find_map(|c| c.strip_prefix("akey="))
                .unwrap_or_else(|| panic!("no akey in {row:?}"));
            let i: usize = key[1..].parse().unwrap();
            assert!(i < LINKED, "phantom endpoint: {key} has no SAME_AS edge ({q})");
            let expected = if i % 2 == 0 {
                format!("aemail=e{i}@x")
            } else {
                "aemail=NULL".to_string()
            };
            assert!(
                row.contains(&expected),
                "column misalignment: {row:?} should carry {expected} ({q})"
            );
        }
    }

    // both endpoints projected at once
    let r = rows(
        &s,
        "MATCH (a)-[:SAME_AS]->(b) RETURN a.key AS akey, a.email AS aemail, b.key AS bkey",
    );
    assert_eq!(r.len(), LINKED / 2);
    for row in &r {
        let akey = row.split_whitespace().find_map(|c| c.strip_prefix("akey=")).unwrap();
        let bkey = row.split_whitespace().find_map(|c| c.strip_prefix("bkey=")).unwrap();
        let (ai, bi): (usize, usize) = (akey[1..].parse().unwrap(), bkey[1..].parse().unwrap());
        assert_eq!(bi, ai + 1, "endpoints paired wrongly: {row:?}");
        assert!(row.contains(&format!("aemail=e{ai}@x")), "{row:?}");
    }
}

// ---------------------------------------------------------------------------
// Properties survive a snapshot round-trip (#333)
// ---------------------------------------------------------------------------

#[test]
fn keys_and_properties_see_columnar_values_after_a_snapshot_import() {
    // Properties live in row storage *and* in the columnar store, and a snapshot import
    // populates only the latter. `keys()`, `properties()` and whole-node RETURN read the
    // row map directly, so an imported node reported having no properties at all — while
    // `n.name` returned a value, because scalar access goes through the columnar store.
    //
    // Reported (#333) as specific to nodes carrying embeddings, but it is not: the
    // embedding merely happened to be the one property that survived in row storage, which
    // made embedded nodes look singled out. A node with no embedding is equally affected.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE (:Player {name: \"Alice\", player_id: \"P-1\", age: 30})",
            &mut s, "default",
        )
        .unwrap();
    engine
        .execute_mut("CREATE (:Team {name: \"Reds\", city: \"X\"})", &mut s, "default")
        .unwrap();
    engine
        .execute_mut("MATCH (p:Player) SET p.embedding = [0.1, 0.2, 0.3]", &mut s, "default")
        .unwrap();

    let mut buf: Vec<u8> = Vec::new();
    samyama::snapshot::export_tenant(&s, &mut buf).expect("export");
    let mut imported = GraphStore::new();
    samyama::snapshot::import_tenant(&mut imported, &buf[..]).expect("import");

    // scalar access always worked; it is the aggregate views that did not
    assert_eq!(scalar(&imported, "MATCH (p:Player) RETURN p.name AS v"), "v=Alice");

    let keys = scalar(&imported, "MATCH (p:Player) RETURN keys(p) AS v");
    for expected in ["name", "player_id", "age", "embedding"] {
        assert!(keys.contains(expected), "keys() lost {expected}: {keys}");
    }

    let props = scalar(&imported, "MATCH (p:Player) RETURN properties(p) AS v");
    for expected in ["name", "player_id", "age"] {
        assert!(props.contains(expected), "properties() lost {expected}: {props}");
    }

    // the node without an embedding must be just as complete
    let team_keys = scalar(&imported, "MATCH (t:Team) RETURN keys(t) AS v");
    for expected in ["name", "city"] {
        assert!(team_keys.contains(expected), "keys() lost {expected}: {team_keys}");
    }

    // and nothing regressed for a graph that was never exported
    let fresh_keys = scalar(&s, "MATCH (t:Team) RETURN keys(t) AS v");
    for expected in ["name", "city"] {
        assert!(fresh_keys.contains(expected), "{fresh_keys}");
    }
}

// ---------------------------------------------------------------------------
// List literals keep their element types (#409)
// ---------------------------------------------------------------------------

#[test]
fn integer_list_literals_stay_integers() {
    // Any all-numeric list was stored as `Vector`, the f32 embedding type, so `[1, 2, 3]`
    // came back as 1.0, 2.0, 3.0 -- decimals for data that had none. A list is only taken
    // to be a vector when it actually contains a float.
    let s = GraphStore::new();

    assert_eq!(bag(&s, "UNWIND [1, 2, 3] AS x RETURN x"), vec!["x=1", "x=2", "x=3"]);
    assert_eq!(bag(&s, "UNWIND [10, 20] AS x RETURN x + 1 AS y"), vec!["y=11", "y=21"]);

    // a float anywhere still makes it a vector, so embeddings are unaffected
    let v = scalar(&s, "RETURN [0.1, 0.2] AS v");
    assert!(v.contains('.'), "float list should keep its decimals: {v}");

    // mixed types stay a plain list
    let mixed = scalar(&s, "RETURN [1, \"a\"] AS v");
    assert!(mixed.contains('a'), "{mixed}");
}

#[test]
fn an_embedding_written_with_whole_numbers_is_still_indexable() {
    // Consequence of the above: `[1, 0, 0]` is now a list of integers rather than a
    // `Vector`, so the vector paths must accept a numeric array or such an embedding would
    // silently stop being indexed -- trading one silent wrong answer for another.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:D {name: \"a\", emb: [1, 0, 0]})", &mut s, "default")
        .unwrap();
    engine
        .execute_mut("CREATE (:D {name: \"b\", emb: [0, 1, 0]})", &mut s, "default")
        .unwrap();

    // unnamed form: the optional index name used to swallow `FOR`, so this was rejected
    engine
        .execute_mut("CREATE VECTOR INDEX FOR (d:D) ON (d.emb)", &mut s, "default")
        .unwrap();
    s.rebuild_vector_index();
    assert_eq!(scalar(&s, "MATCH (d:D) RETURN count(d) AS n"), "n=2");

    // named form still works and keeps the name it was given
    let mut s2 = GraphStore::new();
    engine
        .execute_mut("CREATE (:D {emb: [0.5, 0.5]})", &mut s2, "default")
        .unwrap();
    engine
        .execute_mut("CREATE VECTOR INDEX myidx FOR (d:D) ON (d.emb)", &mut s2, "default")
        .unwrap();
}

// ---------------------------------------------------------------------------
// CREATE property values may be expressions (#408)
// ---------------------------------------------------------------------------

#[test]
fn create_can_derive_property_values_from_bound_variables() {
    // Pattern property values had to be literals, so a node could only ever be created with
    // constants: `CREATE (:Q {n: p.n})` was a parse error while `SET p.m = p.n` was fine.
    // That ruled out copy/derive writes and, with UNWIND, batch writes entirely.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:P {n: 1, name: \"a\"})", &mut s, "default")
        .unwrap();

    engine.execute_mut("MATCH (p:P) CREATE (:Q {n: p.n})", &mut s, "default").unwrap();
    assert_eq!(scalar(&s, "MATCH (q:Q) RETURN q.n AS v"), "v=1");

    engine.execute_mut("MATCH (p:P) CREATE (:R {n: p.n + 10})", &mut s, "default").unwrap();
    assert_eq!(scalar(&s, "MATCH (r:R) RETURN r.n AS v"), "v=11");

    engine.execute_mut("MATCH (p:P) CREATE (:S {label: p.name})", &mut s, "default").unwrap();
    assert_eq!(scalar(&s, "MATCH (x:S) RETURN x.label AS v"), "v=a");

    // literals are unaffected
    engine.execute_mut("MATCH (p:P) CREATE (:T {n: 5})", &mut s, "default").unwrap();
    assert_eq!(scalar(&s, "MATCH (t:T) RETURN t.n AS v"), "v=5");
}

#[test]
fn unwind_drives_a_batch_create() {
    // The batch-write idiom from #307. It needs both a leading UNWIND *and* a non-literal
    // property value; with either missing it runs once with nothing bound.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut("UNWIND [1, 2, 3] AS x CREATE (:N {id: x})", &mut s, "default")
        .unwrap();

    assert_eq!(scalar(&s, "MATCH (n:N) RETURN count(n) AS n"), "n=3");
    let mut ids = bag(&s, "MATCH (n:N) RETURN n.id AS id");
    ids.sort();
    assert_eq!(ids, vec!["id=1", "id=2", "id=3"]);

    // one row per unwound value, each carrying its own value — not three copies of one
    engine
        .execute_mut("UNWIND [7, 8] AS x CREATE (:M {id: x, doubled: x * 2})", &mut s, "default")
        .unwrap();
    let mut rows = bag(&s, "MATCH (m:M) RETURN m.id AS id, m.doubled AS doubled");
    rows.sort();
    assert_eq!(rows, vec!["doubled=14 id=7", "doubled=16 id=8"]);
}

#[test]
fn a_constant_expression_is_allowed_but_an_unbound_variable_is_refused() {
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();

    // nothing bound, but a constant needs nothing bound
    engine.execute_mut("CREATE (:C {n: 1 + 2})", &mut s, "default").unwrap();
    assert_eq!(scalar(&s, "MATCH (c:C) RETURN c.n AS v"), "v=3");

    // a variable that is not bound must be an error, not a silent null — storing nothing
    // for a property without saying so is the failure this whole change removes
    let err = engine
        .execute_mut("CREATE (:D {n: nosuch.x})", &mut s, "default")
        .expect_err("should not silently store null");
    assert!(format!("{err}").contains("not bound"), "{err}");
    assert_eq!(scalar(&s, "MATCH (d:D) RETURN count(d) AS n"), "n=0");
}

#[test]
fn match_refuses_a_non_literal_property_value_and_merge_evaluates_it() {
    // The danger both halves guard against is the same: accepting the pattern
    // and dropping the constraint. `MATCH (p:P {n: x})` would then return
    // *every* `:P` — a working-looking query returning too much.
    //
    // MATCH still refuses, because it still does not evaluate these. MERGE no
    // longer needs to: #642 resolves the property against the row and uses the
    // result for the match and the creation alike, which is what makes
    // `UNWIND $rows AS row MERGE (n {id: row.id})` an upsert rather than a
    // node factory. This test used to assert both refused; it now asserts each
    // does the right thing, which is no longer the same thing.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine.execute_mut("CREATE (:P {n: 1})", &mut s, "default").unwrap();
    engine.execute_mut("CREATE (:P {n: 2})", &mut s, "default").unwrap();

    let err = engine
        .execute("UNWIND [1] AS x MATCH (p:P {n: x}) RETURN p.n AS v", &s)
        .expect_err("must not silently match everything");
    assert!(format!("{err}").contains("WHERE"), "should name the workaround: {err}");

    // the WHERE form it points at does work
    assert_eq!(
        bag(&s, "UNWIND [1] AS x MATCH (p:P) WHERE p.n = x RETURN p.n AS v"),
        vec!["v=1"]
    );

    // MERGE keys on the row: one `:M` per distinct `p.n`, and running it again
    // adds nothing.
    engine.execute_mut("MATCH (p:P) MERGE (:M {n: p.n})", &mut s, "default").unwrap();
    assert_eq!(scalar(&s, "MATCH (m:M) RETURN count(m) AS n"), "n=2");
    engine.execute_mut("MATCH (p:P) MERGE (:M {n: p.n})", &mut s, "default").unwrap();
    assert_eq!(
        scalar(&s, "MATCH (m:M) RETURN count(m) AS n"),
        "n=2",
        "a second run must find the nodes the first one wrote"
    );
}

// ---------------------------------------------------------------------------
// YIELD variables are in scope for a following MATCH's WHERE (#429)
// ---------------------------------------------------------------------------

#[test]
fn a_yielded_variable_can_be_referenced_by_a_later_where() {
    // `CALL ... YIELD x` binds x in an operator that sits *above* the match pipeline, so a
    // predicate mentioning x was being assigned to a MATCH and evaluated underneath the
    // operator that binds it -- "Variable not found", even though the same variable
    // projects fine in RETURN. That asymmetry is what made it confusing: the join was
    // already happening, only the filter was placed on the wrong side of it.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    for i in 0..5 {
        engine
            .execute_mut(&format!("CREATE (:Term {{code: \"T{i}\"}})"), &mut s, "default")
            .unwrap();
    }

    // db.labels() yields exactly one row here: "Term"
    assert_eq!(bag(&s, "CALL db.labels() YIELD label RETURN label"), vec!["label=Term"]);

    // The predicate must *filter*, not merely parse. `t.code` is T0..T4 and `label` is
    // "Term", so this is never true -- a dropped predicate would give 5.
    assert_eq!(
        scalar(
            &s,
            "CALL db.labels() YIELD label MATCH (t:Term) WHERE t.code = label RETURN count(t) AS n"
        ),
        "n=0"
    );

    // always true -> every row survives
    assert_eq!(
        scalar(
            &s,
            "CALL db.labels() YIELD label MATCH (t:Term) WHERE label = \"Term\" RETURN count(t) AS n"
        ),
        "n=5"
    );

    // a predicate spanning both sides of the join
    assert_eq!(
        scalar(
            &s,
            "CALL db.labels() YIELD label MATCH (t:Term) \
             WHERE label = \"Term\" AND t.code = \"T2\" RETURN count(t) AS n"
        ),
        "n=1"
    );
    assert_eq!(
        scalar(
            &s,
            "CALL db.labels() YIELD label MATCH (t:Term) \
             WHERE label = \"Nope\" AND t.code = \"T2\" RETURN count(t) AS n"
        ),
        "n=0"
    );

    // a match-only predicate must still be pushed down to the MATCH, not deferred
    assert_eq!(
        scalar(
            &s,
            "CALL db.labels() YIELD label MATCH (t:Term) WHERE t.code = \"T1\" RETURN count(t) AS n"
        ),
        "n=1"
    );

    // and the previously-working shapes are unchanged
    assert_eq!(
        scalar(&s, "CALL db.labels() YIELD label MATCH (t:Term) RETURN count(t) AS n"),
        "n=5"
    );
    assert_eq!(
        scalar(&s, "CALL db.labels() YIELD label WHERE label = \"Term\" RETURN count(*) AS n"),
        "n=1"
    );
}

// ---------------------------------------------------------------------------
// Two long-standing semantic traps, pinned
// ---------------------------------------------------------------------------

#[test]
fn single_quoted_strings_work_wherever_double_quoted_ones_do() {
    // "double-quoted strings required in places" was a recorded trap. Both quote styles are
    // valid openCypher and the engine now accepts either; this pins that, because a
    // regression would be silent — a single-quoted literal that fails to parse looks like a
    // typo in the caller's query rather than an engine limitation.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:P {name: \"alice\", age: 30})", &mut s, "default")
        .unwrap();
    engine
        .execute_mut("CREATE (:P {name: 'bob', age: 40})", &mut s, "default")
        .unwrap();

    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.name = 'alice' RETURN count(p) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P {name: 'alice'}) RETURN count(p) AS n"), "n=1");
    assert_eq!(
        scalar(&s, "MATCH (p:P) WHERE p.name STARTS WITH 'al' RETURN count(p) AS n"),
        "n=1"
    );
    assert_eq!(scalar(&s, "RETURN 'hello' AS v"), "v=hello");
    // the single-quoted CREATE above must have stored a real value
    assert_eq!(scalar(&s, "MATCH (p:P {name: 'bob'}) RETURN p.age AS v"), "v=40");
}

#[test]
fn integers_and_floats_compare_across_types() {
    // "float-vs-int WHERE silently returns empty" was a recorded trap, and silent is the
    // operative word: the query succeeds and returns nothing, which reads as "no such data"
    // rather than "your literal had the wrong type". Both directions are checked — an int
    // property against a float literal and a float property against an int literal — since
    // fixing one and not the other would leave half the trap in place.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:P {name: \"alice\", age: 30, score: 9.5})", &mut s, "default")
        .unwrap();
    engine
        .execute_mut("CREATE (:P {name: \"bob\", age: 40, score: 8.0})", &mut s, "default")
        .unwrap();

    // integer property
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.age = 30 RETURN count(p) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.age = 30.0 RETURN count(p) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.age > 35.5 RETURN count(p) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.age < 35 RETURN count(p) AS n"), "n=1");

    // float property
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.score = 8 RETURN count(p) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.score = 8.0 RETURN count(p) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.score > 9 RETURN count(p) AS n"), "n=1");

    // and a comparison that should genuinely match nothing still does
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.age = 99 RETURN count(p) AS n"), "n=0");
    assert_eq!(scalar(&s, "MATCH (p:P) WHERE p.score > 100.0 RETURN count(p) AS n"), "n=0");
}

#[test]
fn counting_with_an_inline_property_filter_counts_only_the_matching_rows() {
    // `MATCH (p:P {name: "alice"}) RETURN count(p)` returned the count of *every* :P. The
    // O(1) label-count shortcut fires for a single unadorned count over one label, and it
    // checked `where_clause.is_none()` but never looked at the pattern's inline properties
    // — which are a filter the label metadata knows nothing about.
    //
    // The giveaway was that the same pattern behaved differently by projection: `RETURN p`
    // gave the one correct row, `RETURN count(p)` gave three, and min/max/sum were correct
    // because only count has the shortcut.
    let mut s = GraphStore::new();
    let engine = QueryEngine::new();
    for (name, age) in [("alice", 30), ("bob", 40), ("carol", 50)] {
        engine
            .execute_mut(
                &format!("CREATE (:P {{name: \"{name}\", age: {age}}})"),
                &mut s, "default",
            )
            .unwrap();
    }

    // the filtered forms
    assert_eq!(scalar(&s, "MATCH (p:P {name: \"alice\"}) RETURN count(p) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P {name: \"alice\"}) RETURN count(*) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P {age: 30}) RETURN count(p) AS n"), "n=1");
    assert_eq!(scalar(&s, "MATCH (p:P {name: \"nobody\"}) RETURN count(p) AS n"), "n=0");

    // consistency across projections of the *same* pattern — the property that was violated
    assert_eq!(bag(&s, "MATCH (p:P {name: \"alice\"}) RETURN p.name AS v"), vec!["v=alice"]);
    assert_eq!(scalar(&s, "MATCH (p:P {name: \"alice\"}) RETURN min(p.age) AS n"), "n=30");
    assert_eq!(scalar(&s, "MATCH (p:P {name: \"alice\"}) RETURN sum(p.age) AS n"), "n=30");

    // and the shortcut must still apply when the pattern really is unadorned
    assert_eq!(scalar(&s, "MATCH (p:P) RETURN count(p) AS n"), "n=3");
    assert_eq!(scalar(&s, "MATCH (p:P) RETURN count(*) AS n"), "n=3");
}

// ---------------------------------------------------------------------------
// Chained subscripting (#453)
//
// `term` in the grammar allowed at most one `index_op`, and placed it *after*
// `postfix_op`. That made every chained subscript a parse error -- for lists as
// well as maps -- and made `xs[0] IS NULL`, the idiomatic missing-element test,
// unparseable too. The rule is now `primary ~ index_op* ~ postfix_op?`.
// ---------------------------------------------------------------------------

/// A map-valued property, the shape that has no dot-notation path access (#452).
fn nested_map_fixture() -> GraphStore {
    let mut s = GraphStore::new();
    QueryEngine::new()
        .execute_mut("CREATE (:D {meta: {c: {d: 9}}})", &mut s, "default")
        .unwrap();
    s
}

#[test]
fn chained_list_index() {
    assert_eq!(scalar(&GraphStore::new(), "RETURN [[1,2],[3,4]][0][1] AS v"), "v=2");
}

#[test]
fn chained_index_three_deep() {
    assert_eq!(scalar(&GraphStore::new(), "RETURN [[[5]]][0][0][0] AS v"), "v=5");
}

#[test]
fn chained_list_then_map_key() {
    assert_eq!(scalar(&GraphStore::new(), "RETURN [{a:1}][0][\"a\"] AS v"), "v=1");
}

#[test]
fn chained_map_keys_on_stored_property() {
    assert_eq!(
        scalar(&nested_map_fixture(), "MATCH (d:D) RETURN d.meta[\"c\"][\"d\"] AS v"),
        "v=9"
    );
}

#[test]
fn chained_map_key_in_where() {
    assert_eq!(
        scalar(
            &nested_map_fixture(),
            "MATCH (d:D) WHERE d.meta[\"c\"][\"d\"] = 9 RETURN count(d) AS v"
        ),
        "v=1"
    );
}

#[test]
fn index_then_slice_composes() {
    assert_eq!(
        scalar(&GraphStore::new(), "RETURN [[1,2,3],[4]][0][0..2] AS v"),
        "v=[1,2]"
    );
}

#[test]
fn is_null_applies_to_indexed_element_not_container() {
    // The out-of-range element is null even though the list itself is not.
    assert_eq!(scalar(&GraphStore::new(), "RETURN [1,2][5] IS NULL AS v"), "v=true");
    assert_eq!(scalar(&GraphStore::new(), "RETURN [1,2][0] IS NULL AS v"), "v=false");
}

#[test]
fn is_null_on_missing_map_key() {
    assert_eq!(scalar(&GraphStore::new(), "RETURN {a:1}[\"z\"] IS NULL AS v"), "v=true");
    assert_eq!(
        scalar(&GraphStore::new(), "RETURN {a:1}[\"a\"] IS NOT NULL AS v"),
        "v=true"
    );
}

#[test]
fn single_subscript_and_slice_still_work() {
    assert_eq!(scalar(&GraphStore::new(), "RETURN [1,2][0] AS v"), "v=1");
    assert_eq!(scalar(&GraphStore::new(), "RETURN [1,2,3][0..2] AS v"), "v=[1,2]");
}

// ---------------------------------------------------------------------------
// Null propagation through arithmetic (#457)
//
// Comparison and logical operators already returned null for a null operand;
// arithmetic raised a type error instead. The damaging case was not the
// literal but `p.a + p.missing` -- a property absent on some nodes is the
// ordinary state of a property graph, and it aborted the entire query.
// ---------------------------------------------------------------------------

#[test]
fn arithmetic_with_null_yields_null_not_an_error() {
    let s = GraphStore::new();
    for q in [
        "RETURN 1 + null AS v",
        "RETURN null + 1 AS v",
        "RETURN 1 - null AS v",
        "RETURN 1 * null AS v",
        "RETURN 1 / null AS v",
        "RETURN 1 % null AS v",
        "RETURN \"a\" + null AS v",
        "RETURN -null AS v",
    ] {
        assert_eq!(scalar(&s, q), "v=NULL", "{q}");
    }
}

#[test]
fn a_missing_property_nulls_only_its_own_row() {
    let e = QueryEngine::new();
    let mut s = GraphStore::new();
    e.execute_mut("CREATE (:P {a: 1})", &mut s, "default").unwrap();
    e.execute_mut("CREATE (:P {a: 2, b: 10})", &mut s, "default").unwrap();

    // The row lacking `b` is null; the row that has it still computes. Before
    // the fix this raised a type error and neither row came back.
    assert_eq!(bag(&s, "MATCH (p:P) RETURN p.a + p.b AS v"), vec!["v=12", "v=NULL"]);
}

#[test]
fn narrowing_null_did_not_disable_the_type_check() {
    let e = QueryEngine::new();
    let s = GraphStore::new();
    // Genuinely non-numeric, non-null operands must still be rejected.
    assert!(e.execute("RETURN \"a\" - 1 AS v", &s).is_err());
    // And division by zero is still an error, not a null.
    assert!(e.execute("RETURN 1 / 0 AS v", &s).is_err());
}

#[test]
fn ordinary_arithmetic_is_unaffected() {
    let s = GraphStore::new();
    assert_eq!(scalar(&s, "RETURN 1 + 2 AS v"), "v=3");
    assert_eq!(scalar(&s, "RETURN \"a\" + \"b\" AS v"), "v=ab");
    assert_eq!(scalar(&s, "RETURN 7 % 3 AS v"), "v=1");
}

// ---------------------------------------------------------------------------
// CALL {} subqueries (#458)
//
// The subquery was parsed into its own Query and then never executed -- it
// only appeared as a "bail out of this optimisation" flag in the detectors --
// so its columns were never bound and `CALL { RETURN 1 AS n } RETURN n` died
// with `Variable not found: n`, blaming the user's variable name for an
// engine gap.
// ---------------------------------------------------------------------------

fn three_p_nodes() -> GraphStore {
    let e = QueryEngine::new();
    let mut s = GraphStore::new();
    for n in [1, 2, 2] {
        e.execute_mut(&format!("CREATE (:P {{n: {n}}})"), &mut s, "default").unwrap();
    }
    s
}

#[test]
fn call_subquery_exports_its_columns() {
    // No graph access at all -- the simplest case, which also failed before.
    assert_eq!(scalar(&GraphStore::new(), "CALL { RETURN 1 AS n } RETURN n"), "n=1");
}

#[test]
fn call_subquery_over_a_match_exports_every_row() {
    assert_eq!(
        bag(&three_p_nodes(), "CALL { MATCH (p:P) RETURN p.n AS n } RETURN n"),
        vec!["n=1", "n=2", "n=2"]
    );
}

#[test]
fn bare_call_subquery_yields_its_own_rows() {
    // `CALL { ... }` with nothing after it is the subquery's result.
    assert_eq!(
        bag(&three_p_nodes(), "CALL { MATCH (p:P) RETURN p.n AS n }").len(),
        3
    );
}

#[test]
fn call_subquery_respects_outer_distinct_and_where() {
    assert_eq!(
        bag(&three_p_nodes(), "CALL { MATCH (p:P) RETURN p.n AS n } RETURN DISTINCT n"),
        vec!["n=1", "n=2"]
    );
    assert_eq!(
        bag(&three_p_nodes(), "CALL { MATCH (p:P) RETURN p.n AS n } WHERE n > 1 RETURN n"),
        vec!["n=2", "n=2"]
    );
}

#[test]
fn call_subquery_can_aggregate_inside() {
    assert_eq!(
        scalar(&three_p_nodes(), "CALL { MATCH (p:P) RETURN count(p) AS c } RETURN c"),
        "c=3"
    );
}

#[test]
fn unsupported_call_shapes_fail_loudly_rather_than_partially() {
    let e = QueryEngine::new();
    let s = three_p_nodes();

    // A subquery followed by more pattern matching needs a real join; refusing
    // is correct, silently dropping the MATCH would not be.
    let err = e
        .execute("CALL { MATCH (p:P) RETURN p.n AS n } MATCH (q:P) RETURN n", &s)
        .unwrap_err()
        .to_string();
    assert!(err.contains("not supported"), "{err}");
}

#[test]
fn a_write_inside_a_call_subquery_is_refused_and_writes_nothing() {
    let e = QueryEngine::new();
    let mut s = GraphStore::new();
    let err = e
        .execute_mut("CALL { CREATE (:X {a: 1}) }", &mut s, "default")
        .unwrap_err()
        .to_string();
    assert!(err.contains("CALL {} subquery"), "{err}");
    assert_eq!(s.node_count(), 0, "a refused subquery must not write");
}

#[test]
fn call_subquery_works_on_the_write_path_too() {
    // HTTP and RESP route every statement through the mutable executor, so a
    // fix that only landed on the read path would be invisible to them.
    let e = QueryEngine::new();
    let mut s = three_p_nodes();
    let batch = e
        .execute_mut("CALL { MATCH (p:P) RETURN p.n AS n } RETURN n", &mut s, "default")
        .unwrap();
    assert_eq!(batch.records.len(), 3);
}

// ---------------------------------------------------------------------------
// Map dot access and keys() over maps (#452)
//
// Map properties stored and round-tripped intact to arbitrary depth, but the
// only way in was `m["k"]`. Dot notation was a parse error and keys() rejected
// maps outright, so a map's fields could not even be enumerated.
// ---------------------------------------------------------------------------

fn map_property_fixture() -> GraphStore {
    let mut s = GraphStore::new();
    QueryEngine::new()
        .execute_mut(
            "CREATE (:D {meta: {a: 1, c: {d: 9}}, plain: 5})",
            &mut s,
            "default",
        )
        .unwrap();
    s
}

#[test]
fn dot_access_reaches_one_level_into_a_map() {
    assert_eq!(scalar(&map_property_fixture(), "MATCH (d:D) RETURN d.meta.a AS v"), "v=1");
}

#[test]
fn dot_access_chains_arbitrarily_deep() {
    assert_eq!(scalar(&map_property_fixture(), "MATCH (d:D) RETURN d.meta.c.d AS v"), "v=9");
}

#[test]
fn dot_access_works_in_a_predicate() {
    let s = map_property_fixture();
    assert_eq!(scalar(&s, "MATCH (d:D) WHERE d.meta.a = 1 RETURN count(d) AS v"), "v=1");
    assert_eq!(scalar(&s, "MATCH (d:D) WHERE d.meta.c.d = 9 RETURN count(d) AS v"), "v=1");
}

#[test]
fn dot_and_bracket_access_are_the_same_path() {
    // Both spellings desugar to Expression::Index, so they cannot drift apart.
    let s = map_property_fixture();
    assert_eq!(
        scalar(&s, "MATCH (d:D) RETURN d.meta.c[\"d\"] AS v"),
        scalar(&s, "MATCH (d:D) RETURN d.meta[\"c\"][\"d\"] AS v")
    );
}

#[test]
fn a_missing_map_key_is_null_not_an_error() {
    assert_eq!(scalar(&map_property_fixture(), "MATCH (d:D) RETURN d.meta.nope AS v"), "v=NULL");
}

#[test]
fn plain_property_access_is_unaffected() {
    assert_eq!(scalar(&map_property_fixture(), "MATCH (d:D) RETURN d.plain AS v"), "v=5");
}

#[test]
fn keys_enumerates_a_map_and_still_a_node() {
    let s = map_property_fixture();
    assert_eq!(bag(&s, "MATCH (d:D) RETURN keys(d.meta) AS v"), vec!["v=[a,c]"]);
    assert_eq!(bag(&s, "MATCH (d:D) RETURN keys(d) AS v"), vec!["v=[meta,plain]"]);
}

#[test]
fn writing_through_a_map_path_is_still_rejected() {
    // Reads gained dot access; writes did not, because writing into a map is
    // not implemented. Accepting the syntax and dropping the write would be
    // worse than refusing it.
    let e = QueryEngine::new();
    let mut s = map_property_fixture();
    assert!(e.execute_mut("MATCH (d:D) SET d.meta.a = 5", &mut s, "default").is_err());
    assert!(e.execute_mut("MATCH (d:D) REMOVE d.meta.a", &mut s, "default").is_err());
    // Ordinary single-segment SET/REMOVE keep working.
    assert!(e.execute_mut("MATCH (d:D) SET d.plain = 6", &mut s, "default").is_ok());
    assert!(e.execute_mut("MATCH (d:D) REMOVE d.plain", &mut s, "default").is_ok());
}

// ---------------------------------------------------------------------------
// split() (#437 probe finding)
// ---------------------------------------------------------------------------

#[test]
fn split_on_a_single_character() {
    assert_eq!(scalar(&GraphStore::new(), "RETURN split(\"a,b,c\", \",\") AS v"), "v=[a,b,c]");
}

#[test]
fn split_on_a_multi_character_delimiter() {
    assert_eq!(scalar(&GraphStore::new(), "RETURN split(\"a::b\", \"::\") AS v"), "v=[a,b]");
}

#[test]
fn split_that_matches_nothing_returns_the_whole_string() {
    assert_eq!(scalar(&GraphStore::new(), "RETURN split(\"abc\", \",\") AS v"), "v=[abc]");
}

#[test]
fn split_on_an_empty_delimiter_yields_characters() {
    // Cypher splits into single characters here. Rust's split("") would also
    // emit an empty string at each end, which is why this is special-cased.
    assert_eq!(scalar(&GraphStore::new(), "RETURN split(\"abc\", \"\") AS v"), "v=[a,b,c]");
}

#[test]
fn split_keeps_the_empty_field_a_trailing_delimiter_creates() {
    // "a," is two fields, the second empty -- dropping it would lose information.
    assert_eq!(scalar(&GraphStore::new(), "RETURN size(split(\"a,\", \",\")) AS v"), "v=2");
}

#[test]
fn split_composes_with_size_and_indexing() {
    let s = GraphStore::new();
    assert_eq!(scalar(&s, "RETURN size(split(\"a,b,c\", \",\")) AS v"), "v=3");
    assert_eq!(scalar(&s, "RETURN split(\"a,b,c\", \",\")[1] AS v"), "v=b");
}

#[test]
fn split_with_the_wrong_arity_says_so() {
    let err = QueryEngine::new()
        .execute("RETURN split(\"a\") AS v", &GraphStore::new())
        .unwrap_err()
        .to_string();
    assert!(err.contains("split() requires 2 arguments"), "{err}");
}

// ---------------------------------------------------------------------------
// Unknown-algorithm errors name what exists (#437 probe finding)
//
// `Unknown algorithm: algo.bfs` gave the caller nothing: the name they wanted
// is not guessable from it, and the procedures do not share an argument shape
// either, so even the right name failed on the first attempt.
// ---------------------------------------------------------------------------

fn algo_error(q: &str) -> String {
    QueryEngine::new()
        .execute(q, &GraphStore::new())
        .unwrap_err()
        .to_string()
}

#[test]
fn unknown_algorithm_lists_the_available_ones_with_their_arguments() {
    let err = algo_error("CALL algo.nonsense() YIELD nodeId RETURN count(*) AS v");
    assert!(err.contains("shortestPath(source, target)"), "{err}");
    assert!(err.contains("weightedPath(source, target, weightProperty)"), "{err}");
    assert!(err.contains("pageRank({config})"), "{err}");
}

#[test]
fn common_wrong_names_are_redirected_to_the_right_procedure() {
    assert!(algo_error("CALL algo.bfs() YIELD nodeId RETURN count(*) AS v")
        .contains("use algo.shortestPath"));
    assert!(algo_error("CALL algo.dijkstra() YIELD nodeId RETURN count(*) AS v")
        .contains("use algo.weightedPath"));
    // `algo.louvain` used to be redirected to cdlp because it did not exist.
    // It does now, so the redirect was removed and asserting it would pin the
    // engine to a gap that has been closed. `bfs` and `dijkstra` above are
    // different: those are *deliberately* routed to shortestPath and
    // weightedPath and the redirect is the permanent answer.
    assert!(
        QueryEngine::new()
            .execute("CALL algo.louvain() YIELD communityId RETURN count(*) AS v", &GraphStore::new())
            .is_ok(),
        "louvain is implemented and must not be redirected",
    );
}

// ---------------------------------------------------------------------------
// FOREACH CREATE binds the loop variable (#467)
//
// The CREATE branch read only `path.start.properties` -- the already-literal
// map. A property whose value is an *expression*, which includes the loop
// variable itself, lives in `property_exprs` and was never evaluated. So
// `CREATE (:T {i: i})` created the node and silently dropped `i`: the right
// number of nodes, none of the data, and a successful-looking statement.
// ---------------------------------------------------------------------------

fn foreach_store() -> GraphStore {
    let mut s = GraphStore::new();
    QueryEngine::new()
        .execute_mut("CREATE (:P {n: 0})", &mut s, "default")
        .unwrap();
    s
}

#[test]
fn foreach_create_stores_the_loop_variable() {
    let e = QueryEngine::new();
    let mut s = foreach_store();
    e.execute_mut(
        "MATCH (p:P) FOREACH (i IN [7,8] | CREATE (:T {i: i, lit: 99}))",
        &mut s,
        "default",
    )
    .unwrap();

    // The property must exist as a key, not merely read back as null -- before
    // the fix it was never created at all.
    assert_eq!(bag(&s, "MATCH (t:T) RETURN t.i AS v"), vec!["v=7", "v=8"]);
    assert_eq!(bag(&s, "MATCH (t:T) RETURN t.lit AS v"), vec!["v=99", "v=99"]);
}

#[test]
fn foreach_create_handles_the_canonical_tag_case() {
    // The shape from the docs, and the one that silently produced nameless
    // nodes: right count, no data, no error.
    let e = QueryEngine::new();
    let mut s = foreach_store();
    e.execute_mut(
        "MATCH (p:P) FOREACH (tag IN [\"a\",\"b\"] | CREATE (:Tag {name: tag}))",
        &mut s,
        "default",
    )
    .unwrap();
    assert_eq!(bag(&s, "MATCH (t:Tag) RETURN t.name AS v"), vec!["v=a", "v=b"]);
}

#[test]
fn foreach_create_evaluates_expressions_over_the_loop_variable() {
    let e = QueryEngine::new();
    let mut s = foreach_store();
    e.execute_mut(
        "MATCH (p:P) FOREACH (i IN [1,2] | CREATE (:Calc {v: i * 10}))",
        &mut s,
        "default",
    )
    .unwrap();
    assert_eq!(bag(&s, "MATCH (c:Calc) RETURN c.v AS v"), vec!["v=10", "v=20"]);
}

#[test]
fn foreach_set_still_binds_the_loop_variable() {
    // SET was always correct; pin it so the CREATE fix cannot regress it.
    let e = QueryEngine::new();
    let mut s = foreach_store();
    e.execute_mut("MATCH (p:P) FOREACH (i IN [5] | SET p.n = i)", &mut s, "default").unwrap();
    assert_eq!(scalar(&s, "MATCH (p:P) RETURN p.n AS v"), "v=5");
}

#[test]
fn foreach_create_of_a_relationship_is_refused_not_silently_orphaned() {
    // Only the start node was ever created, so this produced a stray node
    // instead of an edge. Refusing is the honest answer.
    let e = QueryEngine::new();
    let mut s = foreach_store();
    let before = s.node_count();
    let err = e
        .execute_mut("MATCH (p:P) FOREACH (i IN [1] | CREATE (p)-[:R]->(:X))", &mut s, "default")
        .unwrap_err()
        .to_string();
    assert!(err.contains("relationship pattern inside FOREACH"), "{err}");
    assert_eq!(s.node_count(), before, "a refused FOREACH must not leave nodes behind");
}

#[test]
fn foreach_over_an_empty_list_is_a_no_op() {
    let e = QueryEngine::new();
    let mut s = foreach_store();
    let before = s.node_count();
    e.execute_mut("MATCH (p:P) FOREACH (i IN [] | CREATE (:Empty {i: i}))", &mut s, "default")
        .unwrap();
    assert_eq!(s.node_count(), before);
}

// ---------------------------------------------------------------------------
// Leading FOREACH (#465)
//
// `foreach_clause` existed only as a trailing clause of match_stmt/unwind_stmt,
// so a FOREACH with nothing before it was a parse error at 1:1. It has no
// pattern to drive it, so it runs against one empty row -- the same way a bare
// RETURN does.
// ---------------------------------------------------------------------------

#[test]
fn leading_foreach_creates_with_the_loop_variable_bound() {
    let e = QueryEngine::new();
    let mut s = GraphStore::new();
    e.execute_mut("FOREACH (i IN [1,2,3] | CREATE (:L {i: i}))", &mut s, "default").unwrap();
    assert_eq!(bag(&s, "MATCH (l:L) RETURN l.i AS v"), vec!["v=1", "v=2", "v=3"]);
}

#[test]
fn leading_foreach_over_an_empty_list_is_a_no_op() {
    let e = QueryEngine::new();
    let mut s = GraphStore::new();
    e.execute_mut("FOREACH (i IN [] | CREATE (:Z {i: i}))", &mut s, "default").unwrap();
    assert_eq!(s.node_count(), 0);
}

#[test]
fn leading_foreach_is_a_write_and_the_read_executor_refuses_it() {
    // The plan is marked is_write, so a read-only executor rejects it rather
    // than running it and discarding the effects.
    let e = QueryEngine::new();
    let s = GraphStore::new();
    assert!(e.execute("FOREACH (i IN [1] | CREATE (:RO {i: i}))", &s).is_err());
}

#[test]
fn trailing_foreach_is_unaffected_by_the_leading_form() {
    let e = QueryEngine::new();
    let mut s = GraphStore::new();
    e.execute_mut("CREATE (:P {n: 0})", &mut s, "default").unwrap();
    e.execute_mut("MATCH (p:P) FOREACH (i IN [9] | CREATE (:M2 {i: i}))", &mut s, "default")
        .unwrap();
    assert_eq!(scalar(&s, "MATCH (m:M2) RETURN m.i AS v"), "v=9");
}
