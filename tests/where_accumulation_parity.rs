//! A `WHERE` must filter identically whichever path plans it (#1823).
//!
//! `Query` carries two representations — the by-kind fields and the clause
//! pipeline — and `tests/ast_shape_parity.rs:14-27` catalogues eight fixes that
//! were one bug: a rule written against one shape, silently doing nothing for
//! the other. #301, #303, #305, #1810 and #1823 are all that shape. This file
//! asks the question for `WHERE` handling itself rather than for one query.
//!
//! ## The defect
//!
//! `plan_clause_pipeline` rebuilds the leading run of *reading* clauses as a
//! by-kind `Query` and hands it to the established planner. That fold
//! **assigned** the `WHERE`:
//!
//! ```text
//! prefix.where_clause = Some(w.clone())
//! ```
//!
//! The parser **ANDs** multiple `WHERE`s into one chain. So two `WHERE`s in the
//! leading run kept the last and dropped the first, with no error and nothing
//! in the plan to say a predicate went missing.
//!
//! Two things the fold also lost, both found here:
//!
//! * `optional_where` was never populated, so a `WHERE` written after an
//!   `OPTIONAL MATCH` became a **required** filter on the whole row. That
//!   deletes the null-extended rows the optional match exists to produce —
//!   the same conversion-to-required defect as #1231 and #1557.
//!
//! ## Reaching the fold
//!
//! #1823 says the reproduction needs a write *before* the MATCHes. It does not:
//! a query that opens with a write has `split == 0`, the prefix fold is skipped
//! entirely and each `WHERE` becomes its own `FilterOperator` — correct, if
//! unpushed. The fold is reached when a *leading* reading run is followed by a
//! write, which is a query the by-kind grammar cannot express:
//!
//! ```cypher
//! MATCH (a:P) WHERE a.n = 'a' MATCH (b:P) WHERE b.n = 'b'
//! CREATE (:Marker) WITH a.n AS u, b.n AS v RETURN u, v
//! ```
//!
//! `the_prefix_fold_is_the_path_under_test` pins that, because a parity test
//! that reaches neither the fold nor the other shape cannot fail.
//!
//! ## Verified against the bug
//!
//! Before the fix, `a_dropped_where_returns_rows_the_predicate_excludes`
//! returned 3 rows where the by-kind shape returns 1, and
//! `a_where_after_an_optional_match_stays_optional` returned 0 where the
//! by-kind shape returns 1. Both are recorded in the test bodies.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::ast::Clause;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Three `:P` nodes named a, b, c. Small enough that a dropped predicate is a
/// row count anyone can check by hand.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:P {n: 'a'}), (:P {n: 'b'}), (:P {n: 'c'})");
    store
}

fn write(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string()).execute(&q).expect("query should run");
}

fn cell(record: &samyama::query::executor::Record, key: &str) -> String {
    match record.get(key) {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

/// `u|v` per row, sorted. The pipeline samples write, so they need the
/// mutating executor; the by-kind samples are read-only, and running both
/// through `MutQueryExecutor` would compare one executor against itself.
fn rows_by_kind(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    let mut out: Vec<String> =
        batch.records.iter().map(|r| format!("{}|{}", cell(r, "u"), cell(r, "v"))).collect();
    out.sort();
    out
}

fn rows_pipeline(cypher: &str) -> Vec<String> {
    let mut store = fixture();
    let q = parse_query(cypher).expect("query should parse");
    let batch = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("query should run");
    let mut out: Vec<String> =
        batch.records.iter().map(|r| format!("{}|{}", cell(r, "u"), cell(r, "v"))).collect();
    out.sort();
    out
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(&format!("EXPLAIN {cypher}")).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("EXPLAIN should run");
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => {
            t.lines().take_while(|l| !l.starts_with("---")).collect::<Vec<_>>().join("\n")
        }
        other => panic!("expected a plan string, got {other:?}"),
    }
}

/// Every predicate the plan applies, as the plan states it, sorted and
/// de-indented. This is the "same predicate set" assertion: a dropped `WHERE`
/// removes an entry, and a predicate pushed into a scan on one shape and left
/// in a `Filter` on the other is a difference this will show rather than hide.
fn predicates(plan_text: &str) -> Vec<String> {
    let mut out: Vec<String> = plan_text
        .lines()
        .map(|l| l.trim_start_matches([' ', '+', '-', '|']).trim())
        .filter(|l| l.contains("predicate=") || l.starts_with("Filter"))
        .map(|l| {
            // Keep only the predicate text: the scan's var/label detail differs
            // between a FilteredNodeScan and an IndexScan and is not the subject.
            match l.find("predicate=") {
                Some(i) => l[i + "predicate=".len()..].trim_end_matches(')').to_string(),
                None => l.to_string(),
            }
        })
        .collect();
    out.sort();
    out
}

/// A query's by-kind form, and the same reading prefix followed by a write.
///
/// The write is what the by-kind grammar cannot express, so it is what routes
/// the query into the clause pipeline. `CREATE (:Marker)` adds a node per row
/// and changes no column, so the two forms must return the same `u, v` rows.
fn pair(reading_prefix: &str, projection: &str) -> (String, String) {
    (
        format!("{reading_prefix} RETURN {projection}"),
        format!("{reading_prefix} CREATE (:Marker) WITH {projection} RETURN u, v"),
    )
}

/// The same pair with `SET` as the write instead of `CREATE`, because **EXPLAIN
/// cannot see through a `CREATE`**.
///
/// `MatchCreateEdgeOperator` does not implement `describe`, so the default
/// `name: "Unknown"` with no children is printed and the plan stops there:
///
/// ```text
/// Project (u, v)
/// +- WithBarrier (items=[a.n AS u, b.n AS v])
///    +- Unknown
/// ```
///
/// Any plan-text assertion over a pipeline query whose write is a `CREATE` is
/// therefore vacuous — it is reading an empty tree, not an absence of
/// operators. PR #1821's `no_predicate_pushdown_in_the_pipeline_inline_arm`
/// asserts `index_scans(...).is_empty()` over exactly such a plan; its
/// conclusion is right (that arm passes `None` for the where_clause) but the
/// check cannot distinguish it from a plan it failed to print. Filed separately
/// rather than fixed here.
///
/// `SetProperty` does implement `describe`, so the fold's output is visible
/// below it. `SET a.t = 1` touches a property no sample projects.
fn plan_pair(reading_prefix: &str, projection: &str) -> (String, String) {
    (
        format!("{reading_prefix} RETURN {projection}"),
        format!("{reading_prefix} SET a.t = 1 WITH {projection} RETURN u, v"),
    )
}

// ---------------------------------------------------------------------------
// The path under test
// ---------------------------------------------------------------------------

/// The pipeline sample must reach the **prefix fold**, not the inline arm.
///
/// Two distinct things had to hold and neither is obvious:
///
/// 1. the by-kind sample parses by-kind (`clauses` empty), so the pair is not
///    comparing the pipeline against itself — the trap `ast_shape_parity.rs`
///    records falling into;
/// 2. the pipeline sample's first clause is a **reading** clause, so the fold's
///    `split` is greater than zero and the fold actually runs. A query that
///    opens with `CREATE` has `split == 0`: the fold is skipped and the test
///    would pass against the unfixed planner.
#[test]
fn the_prefix_fold_is_the_path_under_test() {
    let (by_kind, pipeline) =
        pair("MATCH (a:P) WHERE a.n = 'a' MATCH (b:P) WHERE b.n = 'b'", "a.n AS u, b.n AS v");

    assert!(
        parse_query(&by_kind).unwrap().clauses.is_empty(),
        "the by-kind sample no longer parses by-kind: `{by_kind}`"
    );

    let piped = parse_query(&pipeline).unwrap();
    assert!(!piped.clauses.is_empty(), "the pipeline sample no longer reaches the pipeline");
    assert!(
        matches!(piped.clauses.first(), Some(Clause::Match(_))),
        "the pipeline sample must open with a reading clause or the prefix fold \
         is skipped (split == 0), got {:?}",
        piped.clauses.first().map(std::mem::discriminant)
    );
    let leading_wheres = piped
        .clauses
        .iter()
        .take_while(|c| {
            matches!(c, Clause::Match(_) | Clause::Where(_) | Clause::Unwind(_))
        })
        .filter(|c| matches!(c, Clause::Where(_)))
        .count();
    assert_eq!(leading_wheres, 2, "the leading reading run must carry two WHEREs: {pipeline}");

    // #1823's own suggested shape does *not* reach the fold. Recorded so the
    // next reader does not spend the time twice.
    let write_first = parse_query(
        "CREATE (:Seed) WITH 1 AS one MATCH (a:P) WHERE a.n = 'a' \
         MATCH (b:P) WHERE b.n = 'b' RETURN a.n AS u, b.n AS v",
    )
    .unwrap();
    assert!(
        matches!(write_first.clauses.first(), Some(Clause::Create(_))),
        "a write-first query opens with CREATE, so split == 0 and the fold is skipped"
    );
}

// ---------------------------------------------------------------------------
// The wrong answer
// ---------------------------------------------------------------------------

/// The reproduction: the first `WHERE` was dropped and the query returned rows
/// it excluded.
///
/// `a.n = 'a'` pins `a` to one node, so the answer is one row. Before the fix
/// the pipeline returned **three** — `a|b`, `b|b`, `c|b` — because only
/// `b.n = 'b'` survived the fold. Nothing errored and the plan said
/// `NodeScan (var=a)` with no predicate at all.
#[test]
fn a_dropped_where_returns_rows_the_predicate_excludes() {
    let (by_kind, pipeline) =
        pair("MATCH (a:P) WHERE a.n = 'a' MATCH (b:P) WHERE b.n = 'b'", "a.n AS u, b.n AS v");
    let store = fixture();

    assert_eq!(rows_by_kind(&store, &by_kind), vec!["a|b".to_string()], "by-kind baseline");
    assert_eq!(
        rows_pipeline(&pipeline),
        vec!["a|b".to_string()],
        "the pipeline dropped `a.n = 'a'` — it returned a, b and c for `a`"
    );
}

/// The write really happened, so the rows above are not a read that skipped it.
///
/// One `:Marker` per row. Three markers instead of one is the dropped
/// predicate seen from the other side.
#[test]
fn the_dropped_predicate_also_changed_what_was_written() {
    let mut store = fixture();
    let (_, pipeline) =
        pair("MATCH (a:P) WHERE a.n = 'a' MATCH (b:P) WHERE b.n = 'b'", "a.n AS u, b.n AS v");
    let q = parse_query(&pipeline).unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string()).execute(&q).unwrap();

    let markers = rows_by_kind(&store, "MATCH (m:Marker) RETURN 'm' AS u, 'm' AS v").len();
    assert_eq!(markers, 1, "one row, one marker — three means `a` was unfiltered");
}

// ---------------------------------------------------------------------------
// Parity over WHERE handling itself
// ---------------------------------------------------------------------------

/// One, two and three `WHERE`s: same rows and same predicate set on both
/// shapes.
///
/// Three matters separately from two. Accumulation that replaced the chain
/// instead of extending it would pass at two and fail at three, and a fold that
/// kept only the *first* rather than the last would pass the two-WHERE row
/// check for the query above by accident.
#[test]
fn one_two_and_three_wheres_agree_across_the_shapes() {
    let store = fixture();
    let cases: Vec<(&str, &str, Vec<&str>)> = vec![
        ("MATCH (a:P) WHERE a.n = 'a' MATCH (b:P)", "a.n AS u, b.n AS v", vec!["a|a", "a|b", "a|c"]),
        (
            "MATCH (a:P) WHERE a.n = 'a' MATCH (b:P) WHERE b.n = 'b'",
            "a.n AS u, b.n AS v",
            vec!["a|b"],
        ),
        (
            "MATCH (a:P) WHERE a.n = 'a' MATCH (b:P) WHERE b.n <> 'a' WHERE b.n <> 'c'",
            "a.n AS u, b.n AS v",
            vec!["a|b"],
        ),
    ];

    for (prefix, projection, want) in cases {
        let (by_kind, pipeline) = pair(prefix, projection);
        let want: Vec<String> = want.into_iter().map(str::to_string).collect();

        assert_eq!(rows_by_kind(&store, &by_kind), want, "by-kind rows: `{by_kind}`");
        assert_eq!(rows_pipeline(&pipeline), want, "pipeline rows: `{pipeline}`");

        let (_, visible) = plan_pair(prefix, projection);
        assert_eq!(rows_pipeline(&visible), want, "pipeline rows (SET form): `{visible}`");
        assert_eq!(
            predicates(&plan(&store, &by_kind)),
            predicates(&plan(&store, &visible)),
            "the shapes apply different predicates for `{prefix}`\n\
             by-kind:\n{}\npipeline:\n{}",
            plan(&store, &by_kind),
            plan(&store, &visible),
        );
    }
}

/// The AND chain nests the way the parser nests it.
///
/// `a AND b AND c` means the same whichever way it associates, but code in this
/// repo pattern-matches on the chain's **shape** — `find_index_predicate` walks
/// it, and `flatten_and_predicates` is only called where someone remembered to.
/// A right-nested chain where the parser produces a left-nested one is a
/// difference waiting to matter, so it is asserted rather than assumed.
///
/// Cross-variable predicates are used deliberately: they cannot be pushed into
/// a scan, so the whole chain is printed in one `Filter` line and the nesting
/// is visible in the plan text.
#[test]
fn the_and_chain_nests_as_the_parser_nests() {
    let store = fixture();
    let (by_kind, pipeline) = plan_pair(
        "MATCH (a:P) MATCH (b:P) WHERE a.n < b.n MATCH (c:P) WHERE b.n < c.n WHERE a.n <> c.n",
        "a.n AS u, c.n AS v",
    );

    assert_eq!(
        predicates(&plan(&store, &by_kind)),
        predicates(&plan(&store, &pipeline)),
        "the AND chain is printed differently, so it is nested differently\n\
         by-kind:\n{}\npipeline:\n{}",
        plan(&store, &by_kind),
        plan(&store, &pipeline),
    );
    assert_eq!(rows_by_kind(&store, &by_kind), rows_pipeline(&pipeline), "rows: `{pipeline}`");
    assert_eq!(rows_pipeline(&pipeline), vec!["a|c".to_string()], "a < b < c, three nodes");
}

// ---------------------------------------------------------------------------
// OPTIONAL MATCH: the predicate must not be promoted to required
// ---------------------------------------------------------------------------

/// A `WHERE` belonging to an `OPTIONAL MATCH` must not be ANDed into the
/// required filter.
///
/// ANDing it converts the optional match into a required one: the null-extended
/// row the optional match exists to produce fails the predicate and is deleted.
/// That is the defect PR #1821's `count(DISTINCT …)` work hit, and #1231 and
/// #1557 before it.
///
/// `:Q` has no nodes, so the correct answer is one row with `v` null. Before the
/// fix the pipeline returned **zero** rows: the fold never populated
/// `optional_where`, so `b.n = 'zzz'` was applied to the joined row.
///
/// Note this fails with a **single** `WHERE` in the run, so it is a second
/// defect in the same fold rather than a consequence of the overwrite.
#[test]
fn a_where_after_an_optional_match_stays_optional() {
    let store = fixture();
    let (by_kind, pipeline) = pair(
        "MATCH (a:P) WHERE a.n = 'a' OPTIONAL MATCH (b:Q) WHERE b.n = 'zzz'",
        "a.n AS u, b.n AS v",
    );

    assert_eq!(
        rows_by_kind(&store, &by_kind),
        vec!["a|NULL".to_string()],
        "by-kind baseline: the optional match finds nothing and keeps its nulls"
    );
    assert_eq!(
        rows_pipeline(&pipeline),
        vec!["a|NULL".to_string()],
        "the pipeline promoted the optional match's WHERE to a required filter \
         and deleted the null row"
    );
}

/// The same, with a required `WHERE` after the optional one.
///
/// This is the case the accumulation fix could get wrong in the other
/// direction: ANDing everything into one chain is right for the required
/// predicates and wrong for the optional one, and both appear here. The
/// required predicate must still filter, the optional one must still not.
#[test]
fn an_optional_where_and_a_required_where_in_one_run() {
    let store = fixture();
    let (by_kind, pipeline) = plan_pair(
        "MATCH (a:P) OPTIONAL MATCH (b:Q) WHERE b.n = 'zzz' MATCH (c:P) WHERE c.n = 'c'",
        "a.n AS u, c.n AS v",
    );

    let want: Vec<String> = vec!["a|c".to_string(), "b|c".to_string(), "c|c".to_string()];
    assert_eq!(rows_by_kind(&store, &by_kind), want, "by-kind baseline: `{by_kind}`");
    assert_eq!(
        rows_pipeline(&pipeline),
        want,
        "the optional predicate must not filter and `c.n = 'c'` must: `{pipeline}`"
    );
    assert_eq!(
        predicates(&plan(&store, &by_kind)),
        predicates(&plan(&store, &pipeline)),
        "by-kind:\n{}\npipeline:\n{}",
        plan(&store, &by_kind),
        plan(&store, &pipeline),
    );
}

/// An `OPTIONAL MATCH` whose predicate *is* satisfiable still joins.
///
/// Without this the optional tests above would pass against a planner that
/// simply ignored the predicate, which is the opposite error and equally wrong.
#[test]
fn an_optional_predicate_that_matches_still_joins() {
    let store = fixture();
    let (by_kind, pipeline) = pair(
        "MATCH (a:P) WHERE a.n = 'a' OPTIONAL MATCH (b:P) WHERE b.n = 'c'",
        "a.n AS u, b.n AS v",
    );

    assert_eq!(rows_by_kind(&store, &by_kind), vec!["a|c".to_string()], "by-kind baseline");
    assert_eq!(
        rows_pipeline(&pipeline),
        vec!["a|c".to_string()],
        "the optional match does find a row, and the predicate selects it"
    );
}
