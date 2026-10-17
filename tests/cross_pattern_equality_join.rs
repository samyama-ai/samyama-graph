//! What a join keyed on an expression must answer (samyama-graph#1822).
//!
//! ```cypher
//! MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease)
//! MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease)
//! WHERE d1.mondo_id = d2.mondo_id
//! RETURN DISTINCT v.rs_id, t.symbol
//! ```
//!
//! No constant, so #1813's transitive propagation has nothing to propagate: the
//! equality is a join condition between two patterns and the plan is
//! `Filter(CartesianProduct(...))`. `JoinOperator` keys on **variable names**
//! and the planner reaches for it only when `known_vars ∩ clause_vars` is
//! non-empty; `d1.mondo_id = d2.mondo_id` shares no variable, so the
//! intersection is empty and the product is the plan.
//!
//! These tests are the correctness bar, not a characterisation of the defect.
//! They pass today against the filtered product, and a join that replaces it
//! must still pass every one — because unlike #1813, which *added* a predicate
//! and left the plan standing, a join **replaces** the plan, so the filtered
//! product is the oracle it has to agree with.
//!
//! Each answer test therefore runs the query twice, against a store with the
//! index on `:Disease(mondo_id)` and one without, and requires identical rows.
//! That is the differential #1813 used, and it keeps working here: whatever the
//! planner does with the indexed store, the unindexed one has no seek available
//! and must agree anyway.
//!
//! The hazards worth naming, from #1822's own bar:
//!
//!   - **Multiplicity.** A hash join that emits one row per matching pair must
//!     emit exactly as many as the filter kept, with and without `DISTINCT`.
//!   - **Nulls.** Cypher `null = null` is null, not true. A join keyed on the
//!     value must not pair two missing properties, where a variable-identity
//!     join never faces the question because a bound variable is never null.
//!   - **Mixed types.** `5` and `5.0` compare equal under the engine's `=`.
//!     Hashing them to different buckets silently loses a pair the filter keeps,
//!     which is the failure a hash join invites and a product cannot have.

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
/// neither plan promises an order without `ORDER BY`; the question is the
/// multiset, not the sequence.
fn rows(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s)
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
                    Some(Value::Property(PropertyValue::Float(f))) => format!("{f:?}"),
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

/// Run against both stores and require the same rows. The returned value is
/// that agreed multiset, so a caller asserts on it once.
fn agreed(indexed: &GraphStore, plain: &GraphStore, q: &str) -> Vec<String> {
    let a = rows(indexed, q);
    let b = rows(plain, q);
    assert_eq!(
        a, b,
        "the indexed and unindexed plans disagree, so one of them is wrong:\n\
         indexed: {a:?}\nplain:   {b:?}\nplan was:\n{}",
        plan(indexed, q)
    );
    a
}

/// The OM08 shape in miniature: `d` diseases, each with `per` variants and
/// `per` targets. With and without the index on `:Disease(mondo_id)`.
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

/// The #1822 shape: the equality alone, no constant.
const OM08_NO_CONSTANT: &str =
    "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
     MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
     WHERE d1.mondo_id = d2.mondo_id \
     RETURN DISTINCT v.rs_id AS v, t.symbol AS t";

// ---------------------------------------------------------------------------
// Where the plan stands today
// ---------------------------------------------------------------------------

/// Today there is no join: the equality shares no variable, so the plan is a
/// product with a filter over it.
///
/// This is the only test here that asserts the *current* shape, and it is
/// expected to change when #1822 lands — at which point the answer tests below
/// are what prove the replacement is faithful. It is written to say which of
/// the two it saw rather than merely failing.
#[test]
fn the_equality_alone_plans_as_a_product_today() {
    let (indexed, _) = federation(3, 2);
    let text = plan(&indexed, OM08_NO_CONSTANT);
    let product = text.contains("CartesianProduct");
    let joined = text.contains("Join");
    assert!(
        product || joined,
        "expected either the product or a join, got neither:\n{text}"
    );
    assert!(
        product,
        "a join now plans this shape -- #1822 has landed, so update this test \
         and keep the answer tests below as the bar:\n{text}"
    );
}

// ---------------------------------------------------------------------------
// The bar: every one of these must hold before and after
// ---------------------------------------------------------------------------

/// The answer itself. Each disease contributes `per × per` variant/target pairs,
/// and `DISTINCT` does not reduce them because every pair is distinct.
#[test]
fn the_equality_answers_and_both_plans_agree() {
    let (indexed, plain) = federation(3, 2);
    let got = agreed(&indexed, &plain, OM08_NO_CONSTANT);

    // 3 diseases × 2 variants × 2 targets = 12 pairs, all distinct.
    assert_eq!(got.len(), 12, "got {got:?}");
    assert!(got.contains(&"rs1_1|T1_1".to_string()));
    assert!(got.contains(&"rs3_2|T3_2".to_string()));
    // Never pairs across diseases: the equality is doing real work.
    assert!(
        !got.iter().any(|r| {
            let (v, t) = r.split_once('|').expect("pair");
            v.trim_start_matches("rs").split('_').next() != t.trim_start_matches('T').split('_').next()
        }),
        "a pair crossed diseases, so the join condition was not applied: {got:?}"
    );
}

/// Multiplicity without `DISTINCT`. A join must emit exactly as many rows as
/// the filter kept, which is the count `DISTINCT` was hiding.
#[test]
fn multiplicity_without_distinct_matches_the_filtered_product() {
    let (indexed, plain) = federation(3, 2);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    assert_eq!(got.len(), 12, "one row per surviving pair: {got:?}");
}

/// Duplicate join values multiply rows, and the count is the product of the
/// two sides per value — the arithmetic a hash join is most likely to get
/// wrong by emitting one row per key instead of one per pair.
#[test]
fn duplicate_join_values_multiply_rows_on_both_sides() {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        // Two diseases share 'SHARED': the left pattern reaches it twice and the
        // right three times, so the pair count for that value is 2 × 3.
        write(s, "CREATE (:Disease {mondo_id: 'SHARED', name: 'a'})");
        write(s, "CREATE (:Disease {mondo_id: 'SHARED', name: 'b'})");
        write(s, "MATCH (d:Disease {name: 'a'}) CREATE (:Variant {rs_id: 'v1'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'b'}) CREATE (:Variant {rs_id: 'v2'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'a'}) CREATE (:Target {symbol: 't1'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'a'}) CREATE (:Target {symbol: 't2'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'b'}) CREATE (:Target {symbol: 't3'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
    }
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");

    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    // 2 variants × 3 targets, all on the one shared value.
    assert_eq!(got.len(), 6, "every pair of the shared value: {got:?}");
}

/// `null = null` is null, not true, so two missing properties must not pair.
///
/// This is the case a variable-identity join never meets, because a bound
/// variable is never null. A join keyed on the value meets it immediately.
#[test]
fn two_missing_properties_do_not_pair() {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        // Neither disease carries mondo_id.
        write(s, "CREATE (:Disease {name: 'left'})");
        write(s, "CREATE (:Disease {name: 'right'})");
        write(s, "MATCH (d:Disease {name: 'left'}) CREATE (:Variant {rs_id: 'v1'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'right'}) CREATE (:Target {symbol: 't1'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
    }
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");

    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    assert!(
        got.is_empty(),
        "null = null is null, so nothing may pair: {got:?}"
    );
}

/// One side null, the other present: still no pair, and the present side is not
/// dropped from any other pairing it has.
#[test]
fn a_null_on_one_side_pairs_with_nothing() {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        write(s, "CREATE (:Disease {mondo_id: 'M1', name: 'has'})");
        write(s, "CREATE (:Disease {name: 'hasnt'})");
        // A variant on the valued disease and one on the null disease.
        write(s, "MATCH (d:Disease {name: 'has'}) CREATE (:Variant {rs_id: 'v_has'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'hasnt'}) CREATE (:Variant {rs_id: 'v_null'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'has'}) CREATE (:Target {symbol: 't_has'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'hasnt'}) CREATE (:Target {symbol: 't_null'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
    }
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");

    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    assert_eq!(
        got,
        vec!["v_has|t_has".to_string()],
        "only the valued pair survives: {got:?}"
    );
}

/// `5` and `5.0` are equal under the engine's `=`, so they must pair.
///
/// The named hazard: a hash join bucketing an integer and a float separately
/// silently loses a pair the filter keeps. A product cannot have this bug, so
/// this test is worth nothing today and everything after the change.
#[test]
fn an_integer_and_a_float_that_compare_equal_still_pair() {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        write(s, "CREATE (:Disease {code: 5, name: 'int'})");
        write(s, "CREATE (:Disease {code: 5.0, name: 'float'})");
        write(s, "MATCH (d:Disease {name: 'int'}) CREATE (:Variant {rs_id: 'v_int'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'float'}) CREATE (:Target {symbol: 't_float'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
    }
    write(&mut indexed, "CREATE INDEX ON :Disease(code)");

    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.code = d2.code \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    assert_eq!(
        got,
        vec!["v_int|t_float".to_string()],
        "5 = 5.0 under the engine's `=`, so the pair must survive: {got:?}"
    );
}

/// An empty side yields nothing, rather than the whole other side.
#[test]
fn an_empty_side_yields_no_rows() {
    let (indexed, plain) = federation(3, 2);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id AND d1.mondo_id = 'MONDO:nonexistent' \
             RETURN v.rs_id AS v, t.symbol AS t";
    assert!(agreed(&indexed, &plain, q).is_empty());
}

/// No value in common: the join condition excludes every pair.
#[test]
fn disjoint_values_pair_nothing() {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        write(s, "CREATE (:Disease {mondo_id: 'LEFT', name: 'l'})");
        write(s, "CREATE (:Disease {mondo_id: 'RIGHT', name: 'r'})");
        write(s, "MATCH (d:Disease {name: 'l'}) CREATE (:Variant {rs_id: 'v1'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'r'}) CREATE (:Target {symbol: 't1'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
    }
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");

    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    assert!(agreed(&indexed, &plain, q).is_empty());
}

/// `OPTIONAL MATCH`: a row failing the join condition must null-fill, not
/// vanish. A join that replaces the product must not turn the outer side into
/// an inner one.
#[test]
fn an_optional_match_null_fills_rather_than_dropping_the_row() {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        write(s, "CREATE (:Disease {mondo_id: 'M1', name: 'matched'})");
        write(s, "CREATE (:Disease {mondo_id: 'M2', name: 'unmatched'})");
        write(s, "MATCH (d:Disease {name: 'matched'}) CREATE (:Variant {rs_id: 'v_m'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        write(s, "MATCH (d:Disease {name: 'unmatched'}) CREATE (:Variant {rs_id: 'v_u'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
        // Only M1 has a target, so v_u's optional side finds nothing.
        write(s, "MATCH (d:Disease {name: 'matched'}) CREATE (:Target {symbol: 't_m'})-[:ASSOCIATED_WITH_DISEASE]->(d)");
    }
    write(&mut indexed, "CREATE INDEX ON :Disease(mondo_id)");

    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             WITH v, d1 \
             OPTIONAL MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    assert_eq!(
        got,
        vec!["v_m|t_m".to_string(), "v_u|NULL".to_string()],
        "the unmatched variant must survive with a null target: {got:?}"
    );
}

// ---------------------------------------------------------------------------
// Declines: shapes a join must not claim
// ---------------------------------------------------------------------------

/// Under `OR` the equality is not a join condition: a row may qualify without
/// it. Whatever the plan, the answer must include the rows the disjunction
/// admits.
#[test]
fn an_equality_under_or_still_answers_the_disjunction() {
    let (indexed, plain) = federation(2, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id = d2.mondo_id OR d2.mondo_id = 'MONDO:1' \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    // Pairs where the ids match: (1,1) and (2,2). Plus every pair whose right
    // side is MONDO:1, which adds (2,1). So three.
    assert_eq!(got.len(), 3, "the disjunction admits three pairs: {got:?}");
}

/// Negated, the equality excludes the matching pairs rather than selecting
/// them.
#[test]
fn a_negated_equality_selects_the_complement() {
    let (indexed, plain) = federation(2, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE NOT d1.mondo_id = d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    // 2 × 2 pairs, minus the 2 where the ids agree.
    assert_eq!(got.len(), 2, "only the mismatched pairs: {got:?}");
}

/// A non-equality comparison is not a hash-join condition. The rows must still
/// be right.
#[test]
fn a_non_equality_comparison_still_answers() {
    let (indexed, plain) = federation(3, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             MATCH (t:Target)-[:ASSOCIATED_WITH_DISEASE]->(d2:Disease) \
             WHERE d1.mondo_id < d2.mondo_id \
             RETURN v.rs_id AS v, t.symbol AS t";
    let got = agreed(&indexed, &plain, q);
    // MONDO:1 < MONDO:2, MONDO:1 < MONDO:3, MONDO:2 < MONDO:3 -- three ordered pairs.
    assert_eq!(got.len(), 3, "the strictly-ordered pairs: {got:?}");
}

/// An equality inside one pattern is not a cross-pattern join condition.
#[test]
fn an_equality_within_one_pattern_is_not_a_join() {
    let (indexed, plain) = federation(2, 1);
    let q = "MATCH (v:Variant)-[:ASSOCIATED_WITH_DISEASE]->(d1:Disease) \
             WHERE d1.mondo_id = d1.mondo_id \
             RETURN v.rs_id AS v";
    let got = agreed(&indexed, &plain, q);
    assert_eq!(got.len(), 2, "a reflexive equality excludes nothing: {got:?}");
}
