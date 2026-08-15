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
