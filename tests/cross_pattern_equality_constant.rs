//! A cross-pattern equality plus a constant pins both patterns (samyama-graph#1813).
//!
//! ```cypher
//! MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease)
//! MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease)
//! WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'MONDO:0007254'
//! RETURN DISTINCT v.rs_id, t.symbol LIMIT 50
//! ```
//!
//! The two MATCH clauses share no variable, so they plan as a
//! `CartesianProduct` and the `WHERE` filters it. At 328.2M nodes / 1.44B edges
//! that is refused after 13.8 s by the 50M per-operator row budget.
//!
//! `d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'MONDO:0007254'` pins **both**
//! sides to one value by transitivity, and `:Disease(mondo_id)` is
//! index-backed. The planner now derives `d2.mondo_id = 'MONDO:0007254'` and
//! hands it to the second clause, which turns a scan into a point lookup. The
//! original conjuncts are all still applied, so a derived predicate can only
//! change an answer if `=` is not transitive — which is what the fixtures with
//! nulls, duplicates and mixed types are here to check.
//!
//! Every answer test compares the two plans: the same query is run against a
//! store **with** the index (where the derivation pays) and **without** it
//! (where it cannot), and the rows must be identical. A shape the planner
//! declines gets a test asserting both that no `IndexScan` appears *and* that
//! the rows are right.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&p)
        .unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

/// Every returned column of every row, stringified and sorted. Sorted because
/// neither plan promises an order without `ORDER BY`, and the question here is
/// the multiset of rows, not its sequence.
fn rows(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s)
        .execute(&p)
        .unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut v: Vec<String> = out
        .records
        .iter()
        .map(|r| {
            let mut cells: Vec<String> = out
                .columns
                .iter()
                .map(|c| match r.get(c) {
                    Some(Value::Property(PropertyValue::String(x))) => x.clone(),
                    Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
                    Some(Value::Property(PropertyValue::Float(f))) => format!("{f:?}"),
                    Some(Value::Null)
                    | Some(Value::Property(PropertyValue::Null))
                    | None => "NULL".to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            cells.iter_mut().for_each(|c| *c = c.replace('|', "\\|"));
            cells.join("|")
        })
        .collect();
    v.sort();
    v
}

/// `rows`, for a query that writes. The store is consumed in place.
fn rows_mut(s: &mut GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = MutQueryExecutor::new(s, "default".into())
        .execute(&p)
        .unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut v: Vec<String> = out
        .records
        .iter()
        .map(|r| {
            out.columns
                .iter()
                .map(|c| match r.get(c) {
                    Some(Value::Property(PropertyValue::String(x))) => x.clone(),
                    Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
                    Some(Value::Null) | Some(Value::Property(PropertyValue::Null)) | None => {
                        "NULL".to_string()
                    }
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    v.sort();
    v
}

fn plan(s: &GraphStore, q: &str) -> String {
    let p = parse_query(&format!("EXPLAIN {q}")).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s)
        .execute(&p)
        .unwrap_or_else(|e| panic!("`{q}`: {e}"));
    match out.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("no plan: {other:?}"),
    }
}

/// How many `IndexScan`s the plan uses, and on which variables.
fn index_scans(text: &str) -> Vec<String> {
    text.lines()
        .filter(|l| l.contains("IndexScan"))
        .map(|l| l.trim().to_string())
        .collect()
}

/// The OM08 shape in miniature: `d` diseases, each with `per` variants and
/// `per` targets. Returned with and without the index on `:Disease(mondo_id)`.
fn federation(d: i64, per: i64) -> (GraphStore, GraphStore) {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        write(
            s,
            &format!("UNWIND range(1, {d}) AS i CREATE (:Disease {{mondo_id: 'MONDO:' + toString(i), name: 'd' + toString(i)}})"),
        );
        write(
            s,
            &format!(
                "UNWIND range(1, {d}) AS i UNWIND range(1, {per}) AS j \
                 MATCH (dz:Disease {{mondo_id: 'MONDO:' + toString(i)}}) \
                 CREATE (:Variant {{rs_id: 'rs' + toString(i) + '_' + toString(j)}})-[:ASSOCIATED_WITH_DISEASE]->(dz)"
            ),
        );
        write(
            s,
            &format!(
                "UNWIND range(1, {d}) AS i UNWIND range(1, {per}) AS j \
                 MATCH (dz:Disease {{mondo_id: 'MONDO:' + toString(i)}}) \
                 CREATE (:Target {{symbol: 'T' + toString(i) + '_' + toString(j)}})-[:ASSOCIATED_WITH_DISEASE]->(dz)"
            ),
        );
    }
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");
    (indexed, plain)
}

const OM08: &str = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
     MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
     WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'MONDO:3' \
     RETURN DISTINCT v.rs_id AS v, t.symbol AS t";

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

/// The constant reaches **both** patterns, so both `:Disease` lookups are
/// index seeks. Before this change only `d1` was pinned and `d2` was a full
/// `:Disease` scan feeding a cartesian product.
#[test]
fn both_sides_of_the_equality_become_index_lookups() {
    let (indexed, _) = federation(4, 2);
    let text = plan(&indexed, OM08);
    let scans = index_scans(&text);
    assert_eq!(
        scans.len(),
        2,
        "expected an IndexScan for d1 and one for d2, got {scans:?}\n{text}"
    );
    assert!(
        scans.iter().any(|s| s.contains("var=d1")) && scans.iter().any(|s| s.contains("var=d2")),
        "both d1 and d2 must be pinned: {scans:?}\n{text}"
    );
    for s in &scans {
        assert!(
            s.contains("Disease.mondo_id = String(\"MONDO:3\")"),
            "the propagated constant must be the literal: {s}\n{text}"
        );
    }
}

/// The propagation is an *addition*. Nothing is taken away, so the original
/// cross-pattern equality is still applied — otherwise correctness would
/// depend on the derivation being exactly right rather than merely sound.
#[test]
fn the_original_equality_is_still_applied() {
    let (indexed, _) = federation(4, 2);
    let text = plan(&indexed, OM08);
    assert!(
        text.contains("d1") && text.contains("d2"),
        "both pattern variables must survive:\n{text}"
    );
    let filters: Vec<&str> = text.lines().filter(|l| l.contains("Filter")).collect();
    assert!(
        filters.iter().any(|l| l.contains("mondo_id")),
        "the cross-pattern equality must still be applied as a filter: {filters:?}\n{text}"
    );
}

// ---------------------------------------------------------------------------
// The answers
// ---------------------------------------------------------------------------

/// The query the issue is about. 2 variants x 2 targets on `MONDO:3`.
#[test]
fn om08_answers_and_agrees_with_the_unindexed_plan() {
    let (indexed, plain) = federation(4, 2);
    let want = vec![
        "rs3_1|T3_1".to_string(),
        "rs3_1|T3_2".to_string(),
        "rs3_2|T3_1".to_string(),
        "rs3_2|T3_2".to_string(),
    ];
    assert_eq!(rows(&indexed, OM08), want, "indexed");
    assert_eq!(rows(&plain, OM08), want, "unindexed");
}

/// Without `DISTINCT` the multiplicity has to match too: a join and a filtered
/// cartesian product agree on the set of rows but not automatically on how
/// many times each appears.
#[test]
fn duplicate_rows_survive_in_the_same_number() {
    let mut s = GraphStore::new();
    // Two diseases carrying the *same* mondo_id, so each (v, t) pair is found
    // through two d1 bindings and two d2 bindings: four duplicates per pair.
    write(&mut s, "CREATE (:Disease {mondo_id: 'X'}), (:Disease {mondo_id: 'X'}), (:Disease {mondo_id: 'Y'})");
    write(&mut s, "MATCH (d:Disease {mondo_id: 'X'}) CREATE (:Variant {rs_id: 'r1'})-[:A]->(d)");
    write(&mut s, "MATCH (d:Disease {mondo_id: 'X'}) CREATE (:Target {symbol: 't1'})-[:A]->(d)");
    let mut indexed = GraphStore::new();
    write(&mut indexed, "CREATE (:Disease {mondo_id: 'X'}), (:Disease {mondo_id: 'X'}), (:Disease {mondo_id: 'Y'})");
    write(&mut indexed, "MATCH (d:Disease {mondo_id: 'X'}) CREATE (:Variant {rs_id: 'r1'})-[:A]->(d)");
    write(&mut indexed, "MATCH (d:Disease {mondo_id: 'X'}) CREATE (:Target {symbol: 't1'})-[:A]->(d)");
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");

    let q = "MATCH (v:Variant)-[:A]->(d1:Disease) MATCH (t:Target)-[:A]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'X' \
             RETURN v.rs_id AS v, t.symbol AS t";
    // 2 variants (one per :Disease{X}) x 2 targets x ... every (d1, d2) pair of
    // the two X diseases: whatever the number is, both plans must agree.
    let a = rows(&indexed, q);
    let b = rows(&s, q);
    assert_eq!(a, b, "indexed and unindexed plans disagree on duplicates");
    assert!(!a.is_empty(), "the fixture produced no rows");

    let distinct = format!("MATCH (v:Variant)-[:A]->(d1:Disease) MATCH (t:Target)-[:A]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'X' \
             RETURN DISTINCT v.rs_id AS v, t.symbol AS t");
    assert_eq!(rows(&indexed, &distinct), rows(&s, &distinct), "DISTINCT disagrees");
    assert_eq!(rows(&indexed, &distinct), vec!["r1|t1".to_string()], "DISTINCT collapses to one pair");
}

/// A value no node carries: both plans return nothing, and neither errors.
#[test]
fn an_empty_result_is_empty_in_both_plans() {
    let (indexed, plain) = federation(3, 2);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'MONDO:nope' \
             RETURN v.rs_id AS v, t.symbol AS t";
    assert!(rows(&indexed, q).is_empty(), "indexed");
    assert!(rows(&plain, q).is_empty(), "unindexed");
}

/// A null property on one side. `null = null` is null in Cypher, so a disease
/// with no `mondo_id` matches nothing, with or without the derived predicate.
#[test]
fn a_null_property_matches_nothing_in_either_plan() {
    let build = |s: &mut GraphStore| {
        write(s, "CREATE (:Disease {mondo_id: 'X'}), (:Disease {name: 'no-id'})");
        write(s, "MATCH (d:Disease) CREATE (:Variant {rs_id: 'r'})-[:A]->(d)");
        write(s, "MATCH (d:Disease) CREATE (:Target {symbol: 't'})-[:A]->(d)");
    };
    let (mut indexed, mut plain) = (GraphStore::new(), GraphStore::new());
    build(&mut indexed);
    build(&mut plain);
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");

    // The constant is on the nullable side.
    let q = "MATCH (v:Variant)-[:A]->(d1:Disease) MATCH (t:Target)-[:A]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'X' \
             RETURN v.rs_id AS v, t.symbol AS t";
    assert_eq!(rows(&indexed, q), rows(&plain, q), "nullable property: plans disagree");
    assert_eq!(rows(&indexed, q), vec!["r|t".to_string()]);

    // No constant at all: nothing to propagate, and the null rows must still
    // drop out rather than matching each other.
    let no_const = "MATCH (v:Variant)-[:A]->(d1:Disease) MATCH (t:Target)-[:A]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    assert_eq!(rows(&indexed, no_const), rows(&plain, no_const), "no constant: plans disagree");
    assert_eq!(rows(&indexed, no_const).len(), 1, "only the non-null disease pairs with itself");
}

/// Mixed types across the equality. `d1.k = 5` with `d2.k` a string must not
/// start matching, and an integer against a float must answer whatever the
/// engine's `=` already answers — the unindexed plan is the oracle.
#[test]
fn mixed_types_answer_the_same_with_and_without_the_derivation() {
    let build = |s: &mut GraphStore| {
        write(s, "CREATE (:D {k: 5}), (:D {k: '5'}), (:D {k: 5.0}), (:D {k: true})");
        write(s, "MATCH (d:D) CREATE (:V {n: 'v' + toString(d.k)})-[:A]->(d)");
        write(s, "MATCH (d:D) CREATE (:T {n: 't' + toString(d.k)})-[:A]->(d)");
    };
    let (mut indexed, mut plain) = (GraphStore::new(), GraphStore::new());
    build(&mut indexed);
    build(&mut plain);
    write(&mut indexed, "CREATE INDEX ON :D(k)");

    for lit in ["5", "'5'", "5.0", "true"] {
        let q = format!(
            "MATCH (v:V)-[:A]->(d1:D) MATCH (t:T)-[:A]->(d2:D) \
             WHERE d1.k = d2.k AND d1.k = {lit} RETURN v.n AS v, t.n AS t"
        );
        assert_eq!(
            rows(&indexed, &q),
            rows(&plain, &q),
            "literal {lit}: the derived predicate changed the answer\n{}",
            plan(&indexed, &q)
        );
    }
}

/// The constant written on the *other* side of the equality chain, and the
/// equality written with its operands the other way round.
#[test]
fn either_operand_order_propagates() {
    let (indexed, plain) = federation(4, 2);
    let want = vec![
        "rs3_1|T3_1".to_string(),
        "rs3_1|T3_2".to_string(),
        "rs3_2|T3_1".to_string(),
        "rs3_2|T3_2".to_string(),
    ];
    for q in [
        // constant pins d2 instead of d1
        "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
         MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
         WHERE d1.mondo_id = d2.mondo_id AND d2.mondo_id = 'MONDO:3' \
         RETURN DISTINCT v.rs_id AS v, t.symbol AS t",
        // equality reversed
        "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
         MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
         WHERE d2.mondo_id = d1.mondo_id AND d1.mondo_id = 'MONDO:3' \
         RETURN DISTINCT v.rs_id AS v, t.symbol AS t",
        // literal on the left
        "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
         MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
         WHERE d1.mondo_id = d2.mondo_id AND 'MONDO:3' = d1.mondo_id \
         RETURN DISTINCT v.rs_id AS v, t.symbol AS t",
    ] {
        assert_eq!(rows(&indexed, q), want, "indexed: {q}");
        assert_eq!(rows(&plain, q), want, "unindexed: {q}");
        let scans = index_scans(&plan(&indexed, q));
        assert_eq!(scans.len(), 2, "both sides must be pinned: {scans:?} for {q}");
    }
}

/// A three-link chain: `d1.k = d2.k AND d2.k = d3.k AND d3.k = 'X'` pins all
/// three.
#[test]
fn a_chain_of_three_propagates_transitively() {
    let (indexed, plain) = federation(4, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             MATCH (d3:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d2.mondo_id = d3.mondo_id \
               AND d3.mondo_id = 'MONDO:2' \
             RETURN DISTINCT v.rs_id AS v, t.symbol AS t";
    assert_eq!(rows(&indexed, q), rows(&plain, q), "chain: plans disagree");
    assert_eq!(rows(&indexed, q), vec!["rs2_1|T2_1".to_string()]);

    // `d1` and `d2` are seeks on the propagated constant. `d3` is reached by
    // the pre-existing correlated index lookup (`try_correlated_index_lookup`,
    // #1219) because `d2` is bound by then — one probe per row, and the
    // propagation is what made the rows few. Either way, no `:Disease` is
    // scanned.
    let text = plan(&indexed, q);
    let scans = index_scans(&text);
    assert_eq!(scans.len(), 2, "d1 and d2 must be seeks: {scans:?}\n{text}");
    assert!(
        text.contains("CorrelatedIndexLookup (d3"),
        "d3 must be probed, not scanned:\n{text}"
    );
    assert!(
        !text.contains("NodeScan") || !text.contains("Disease"),
        "no :Disease may be scanned:\n{text}"
    );
}

// ---------------------------------------------------------------------------
// Declined shapes: no derivation, and the rows are still right
// ---------------------------------------------------------------------------

/// `a.x = b.x OR a.x = 'X'` is not a conjunction, so neither equality is a
/// propagation source: a row may satisfy the second disjunct without the
/// first.
#[test]
fn declines_an_equality_under_or() {
    let (indexed, plain) = federation(3, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id OR d1.mondo_id = 'MONDO:1' \
             RETURN v.rs_id AS v, t.symbol AS t";
    let scans = index_scans(&plan(&indexed, q));
    assert!(scans.is_empty(), "nothing may be pinned under OR: {scans:?}");
    assert_eq!(rows(&indexed, q), rows(&plain, q), "OR: plans disagree");
    // 3 matching pairs (d1 = d2) plus the whole d1 = MONDO:1 row set.
    assert!(rows(&indexed, q).len() > 3, "the OR must widen the result");
}

/// `NOT (a.x = b.x)` is not an equality the chain may use.
#[test]
fn declines_an_equality_under_not() {
    let (indexed, plain) = federation(3, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE NOT (d1.mondo_id = d2.mondo_id) AND d1.mondo_id = 'MONDO:1' \
             RETURN v.rs_id AS v, t.symbol AS t";
    let scans = index_scans(&plan(&indexed, q));
    assert!(
        !scans.iter().any(|s| s.contains("var=d2")),
        "d2 must not be pinned under NOT: {scans:?}"
    );
    assert_eq!(rows(&indexed, q), rows(&plain, q), "NOT: plans disagree");
    // d1 is MONDO:1, d2 is anything else: 2 other diseases, one target each.
    assert_eq!(rows(&indexed, q).len(), 2);
}

/// An inequality is not an equality. `d1.k > d2.k` links nothing.
#[test]
fn declines_a_non_equality_comparison() {
    let (indexed, plain) = federation(3, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id > d2.mondo_id AND d1.mondo_id = 'MONDO:3' \
             RETURN v.rs_id AS v, t.symbol AS t";
    let scans = index_scans(&plan(&indexed, q));
    assert!(
        !scans.iter().any(|s| s.contains("var=d2")),
        "d2 must not be pinned by an inequality: {scans:?}"
    );
    assert_eq!(rows(&indexed, q), rows(&plain, q), "inequality: plans disagree");
}

/// Two different constants in one chain is a contradiction. The engine must
/// answer nothing, and it must do so without the planner inventing a third
/// constant for the free side.
#[test]
fn declines_a_chain_with_two_different_constants() {
    let (indexed, plain) = federation(4, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'MONDO:1' \
               AND d2.mondo_id = 'MONDO:2' \
             RETURN v.rs_id AS v, t.symbol AS t";
    assert_eq!(rows(&indexed, q), rows(&plain, q), "contradiction: plans disagree");
    assert!(rows(&indexed, q).is_empty(), "a contradiction answers nothing");
}

/// A property of a variable bound by `OPTIONAL MATCH` must not acquire a
/// mandatory filter: the optional side has to null-fill, and a derived
/// predicate on a null property would delete exactly the row the clause exists
/// to keep.
#[test]
fn declines_propagating_into_an_optional_match() {
    let build = |s: &mut GraphStore| {
        write(s, "CREATE (:Disease {mondo_id: 'X'})");
        write(s, "MATCH (d:Disease) CREATE (:Variant {rs_id: 'r'})-[:A]->(d)");
        // No :Target at all, so the OPTIONAL MATCH must null-fill.
        write(s, "CREATE (:Other {mondo_id: 'X'})");
    };
    let (mut indexed, mut plain) = (GraphStore::new(), GraphStore::new());
    build(&mut indexed);
    build(&mut plain);
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");

    let q = "MATCH (v:Variant)-[:A]->(d1:Disease) \
             OPTIONAL MATCH (t:Target)-[:A]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'X' \
             RETURN v.rs_id AS v, t.symbol AS t";
    assert_eq!(rows(&indexed, q), rows(&plain, q), "OPTIONAL MATCH: plans disagree");
    assert_eq!(
        rows(&indexed, q),
        vec!["r|NULL".to_string()],
        "the optional side must null-fill, not vanish"
    );
    let scans = index_scans(&plan(&indexed, q));
    assert!(
        !scans.iter().any(|s| s.contains("var=d2")),
        "d2 is optional-bound and must not be pinned: {scans:?}"
    );
}

/// An equality between two properties of the *same* pattern is not a
/// cross-pattern join, but the constant still propagates along it — and that
/// is sound, because both terms are filtered in the same clause.
#[test]
fn a_same_pattern_equality_propagates_within_the_clause() {
    let build = |s: &mut GraphStore| {
        write(s, "CREATE (:P {x: 1, y: 1}), (:P {x: 1, y: 2}), (:P {x: 2, y: 2})");
    };
    let (mut indexed, mut plain) = (GraphStore::new(), GraphStore::new());
    build(&mut indexed);
    build(&mut plain);
    write(&mut indexed, "CREATE INDEX ON :P(y)");

    let q = "MATCH (a:P) WHERE a.x = a.y AND a.x = 1 RETURN a.x AS x, a.y AS y";
    assert_eq!(rows(&indexed, q), rows(&plain, q), "same-pattern: plans disagree");
    assert_eq!(rows(&indexed, q), vec!["1|1".to_string()]);
    // Only `y` is indexed, and only the derivation puts a literal next to it.
    let scans = index_scans(&plan(&indexed, q));
    assert_eq!(
        scans.len(),
        1,
        "the derived `a.y = 1` must reach the index on :P(y): {scans:?}\n{}",
        plan(&indexed, q)
    );
}

/// An equality whose free side is an expression, not a bare property, is not a
/// term the chain can pin.
#[test]
fn declines_an_expression_operand() {
    let (indexed, plain) = federation(3, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = toUpper(d2.mondo_id) AND d1.mondo_id = 'MONDO:1' \
             RETURN v.rs_id AS v, t.symbol AS t";
    let scans = index_scans(&plan(&indexed, q));
    assert!(
        !scans.iter().any(|s| s.contains("var=d2")),
        "a function result is not a pinnable term: {scans:?}"
    );
    assert_eq!(rows(&indexed, q), rows(&plain, q), "expression operand: plans disagree");
}

/// Different properties on the two sides: `d1.mondo_id = d2.name` pins
/// `d2.name`, not `d2.mondo_id`. The derivation must follow the property, not
/// just the variable.
#[test]
fn propagates_to_the_named_property_not_the_variable() {
    let (indexed, plain) = federation(4, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.name AND d1.mondo_id = 'd2' \
             RETURN v.rs_id AS v, t.symbol AS t";
    assert_eq!(rows(&indexed, q), rows(&plain, q), "named property: plans disagree");
    // `d2.name` is 'd2' on the second disease; `d1.mondo_id = 'd2'` matches no
    // disease, so the answer is empty — and both plans must say so.
    assert!(rows(&indexed, q).is_empty());
}

// ---------------------------------------------------------------------------
// Shape parity (tests/ast_shape_parity.rs)
// ---------------------------------------------------------------------------

/// The same query, planned through the by-kind fields and through the clause
/// pipeline, must return the same rows.
///
/// `Query` carries two representations and a planner rule written against one
/// silently does nothing for the other — the repeat failure mode
/// `tests/ast_shape_parity.rs` exists for. A leading `CREATE` is what forces
/// the pipeline representation.
///
/// **The plan is deliberately not compared.** In the pipeline, MATCH clauses
/// that follow a write go through the inline arm at `planner.rs:7441`, which
/// passes `None` for the where_clause: that arm does no per-match predicate
/// pushdown at all, so *even an explicitly written* `d1.mondo_id = 'MONDO:3'`
/// gets no index there. The gap is older than this change and wider than it —
/// `no_predicate_pushdown_in_the_pipeline_inline_arm` below pins it so the
/// claim is measured rather than assumed. What this test asserts is the part
/// that must hold either way: the answer.
#[test]
fn the_derivation_reaches_both_ast_shapes() {
    let by_kind = OM08.to_string();
    let pipeline = format!("CREATE (:Seed) WITH 1 AS one {OM08}");

    assert!(
        parse_query(&by_kind).unwrap().clauses.is_empty(),
        "the by-kind sample no longer parses by-kind"
    );
    assert!(
        !parse_query(&pipeline).unwrap().clauses.is_empty(),
        "the pipeline sample no longer reaches the clause pipeline: `{pipeline}`"
    );

    let want = vec![
        "rs3_1|T3_1".to_string(),
        "rs3_1|T3_2".to_string(),
        "rs3_2|T3_1".to_string(),
        "rs3_2|T3_2".to_string(),
    ];
    let (indexed, plain) = federation(4, 2);
    assert_eq!(rows(&indexed, &by_kind), want, "by-kind, indexed");
    assert_eq!(rows(&plain, &by_kind), want, "by-kind, unindexed");

    // The pipeline query writes, so it needs the mutating executor. Its
    // projection is the same two columns; the leading CREATE contributes no row
    // of its own.
    for (what, s) in [("indexed", federation(4, 2).0), ("unindexed", federation(4, 2).1)] {
        let mut s = s;
        let got = rows_mut(&mut s, &pipeline);
        assert_eq!(
            got, want,
            "pipeline, {what}: the shapes disagree — by-kind answered {want:?}"
        );
    }
}

/// The pipeline's inline MATCH arm does no predicate pushdown (`planner.rs`
/// passes `None` for the where_clause). Pinned here because the parity test
/// above relies on the claim: if this ever starts producing an `IndexScan`,
/// the parity test should compare plans too.
#[test]
fn no_predicate_pushdown_in_the_pipeline_inline_arm() {
    let indexed = federation(4, 2).0;
    // One MATCH, one explicitly written indexed equality — nothing for this
    // change to derive. In the by-kind shape it is a seek.
    let direct = "MATCH (d:Disease) WHERE d.mondo_id = 'MONDO:3' RETURN d.name AS n";
    assert!(
        !index_scans(&plan(&indexed, direct)).is_empty(),
        "the by-kind shape must still seek:\n{}",
        plan(&indexed, direct)
    );
    let piped = format!("CREATE (:Seed) WITH 1 AS one {direct}");
    assert!(
        !parse_query(&piped).unwrap().clauses.is_empty(),
        "the sample no longer reaches the pipeline"
    );
    assert!(
        index_scans(&plan(&indexed, &piped)).is_empty(),
        "the pipeline inline arm has gained predicate pushdown — the parity \
         test above can now compare plans:\n{}",
        plan(&indexed, &piped)
    );
}

// ---------------------------------------------------------------------------
// The refusal the issue reports
// ---------------------------------------------------------------------------

/// The query answers under a row budget the cross product blows (#1813).
///
/// This is the issue's symptom, in miniature and deterministic. 200 diseases
/// with 20 variants and 20 targets each: 4,000 `:Variant` and 4,000 `:Target`
/// edges, so the unpinned cartesian product is 16,000,000 rows. With both
/// `:Disease` lookups pinned to one value it is 20 x 20 = 400.
///
/// A 1,000,000-row budget therefore separates the two plans: the old one is
/// refused with `RowBudgetExceeded`, the new one answers. No wall-clock
/// assertion — the budget is a row count, which is the thing the plan changed.
#[test]
fn the_query_answers_under_a_budget_the_cross_product_blows() {
    let mut s = GraphStore::new();
    write(&mut s, "UNWIND range(1, 200) AS i CREATE (:Disease {mondo_id: 'MONDO:' + toString(i)})");
    write(&mut s, "CREATE INDEX ON :Disease(mondo_id)");
    write(
        &mut s,
        "UNWIND range(1, 200) AS i UNWIND range(1, 20) AS j \
         MATCH (dz:Disease {mondo_id: 'MONDO:' + toString(i)}) \
         CREATE (:Variant {rs_id: 'rs' + toString(i) + '_' + toString(j)})-[:ASSOCIATED_WITH_DISEASE]->(dz)",
    );
    write(
        &mut s,
        "UNWIND range(1, 200) AS i UNWIND range(1, 20) AS j \
         MATCH (dz:Disease {mondo_id: 'MONDO:' + toString(i)}) \
         CREATE (:Target {symbol: 'T' + toString(i) + '_' + toString(j)})-[:ASSOCIATED_WITH_DISEASE]->(dz)",
    );

    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'MONDO:77' \
             RETURN DISTINCT v.rs_id AS v, t.symbol AS t";
    let p = parse_query(q).unwrap();
    let out = QueryExecutor::new(&s)
        .with_row_budget(1_000_000)
        .execute(&p)
        .unwrap_or_else(|e| {
            panic!("refused under a 1M-row budget — the cross product is unpinned: {e}")
        });
    assert_eq!(out.records.len(), 400, "20 variants x 20 targets on MONDO:77");

    // Not vacuous: without the pinning this *is* refused. The same query with
    // the constant removed has no constant to propagate, and must still be
    // refused by the budget.
    let unpinnable = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id \
             RETURN DISTINCT v.rs_id AS v, t.symbol AS t";
    let p = parse_query(unpinnable).unwrap();
    let err = QueryExecutor::new(&s)
        .with_row_budget(1_000_000)
        .execute(&p)
        .expect_err("16M cartesian rows must still exceed a 1M budget");
    assert!(
        format!("{err}").contains("RowBudgetExceeded") || format!("{err:?}").contains("RowBudget"),
        "expected a row-budget refusal, got: {err}"
    );
}
