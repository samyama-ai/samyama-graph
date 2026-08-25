//! The same query must behave the same whichever AST shape it parses into.
//!
//! `Query` carries two representations. A query may parse into the **clause
//! pipeline** (`clauses` populated, by-kind fields empty) or into the
//! **by-kind fields** (`match_clauses`, `with_clause`, `extra_with_stages`,
//! `clauses` empty). Which one you get depends on the query's shape, and
//! nothing in the type system says a rule must handle both.
//!
//! Eight fixes this session were the same bug: a rule written against one
//! representation, silently doing nothing for queries that parsed into the
//! other.
//!
//! | | what was missed |
//! |---|---|
//! | #710 | `RETURN DISTINCT` invisible; every var-length query enumerated |
//! | #766 | variable-kind conflicts unchecked in the by-kind shape |
//! | #772 | selection maps arrived as `Value::Map`, the arm matched `PropertyValue::Map` |
//! | #777 | `ORDER BY` scope check measured **+0** on its first attempt |
//! | #779 | boolean-operand check reached one shape |
//! | #785 | a post-`WITH` `UNWIND` lost its position |
//! | #791 | property-access targets |
//! | #795 | value-as-pattern, in two copies, both narrow |
//!
//! Each was found by noticing a fix had measured zero. This asks the question
//! directly instead: for a set of queries whose *answers* are known, does the
//! engine agree with itself across the shapes?
//!
//! It cannot enumerate the whole language. What it can do is fail the moment a
//! validation rule or planner path is added to one representation and not the
//! other, for the shapes below — which is how every one of the eight would
//! have been caught at the time.
//!
//! ## Which shape a query gets
//!
//! `parse_query` tries the by-kind grammar **first** and falls back to the
//! pipeline only when that fails. So the by-kind fields are the common path and
//! the pipeline is the rare one — reached by queries the by-kind grammar cannot
//! express, chiefly **write-then-read**: `CREATE … WITH … MATCH …`.
//!
//! I had this backwards while writing this file. Fifteen ordinary queries —
//! `MATCH (n) WITH n RETURN n` among them — all parse **by-kind**, so the first
//! version of these pairs compared that representation against itself and would
//! have passed no matter what. `the_two_representations_are_both_reachable`
//! caught it, which is the whole reason it is here: a parity test that
//! accidentally tests one side twice cannot fail.
//!
//! ## Verified against the bug it exists for
//!
//! Disabling the pipeline branch of `all_expressions` — the shape-branching
//! walk that feeds the boolean-operand check — makes
//! `validation_rules_reach_both_representations` fail and name the rule and
//! both shapes.
//!
//! The *first* negative check I ran passed, which is worth recording: I had
//! disabled a loop rather than the `if !query.clauses.is_empty()` branch, so
//! both sides still reached the same validator and the pairs agreed for a
//! reason unrelated to shape coverage. A parity test can be vacuous in two
//! ways — testing one shape twice, or exercising a rule that does not branch on
//! shape at all — and only running it against a real defect distinguishes them.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// Which representation a query parsed into.
fn shape(cypher: &str) -> &'static str {
    match parse_query(cypher) {
        Ok(q) if !q.clauses.is_empty() => "pipeline",
        Ok(_) => "by-kind",
        Err(_) => "unparsed",
    }
}

fn accepted(cypher: &str) -> bool {
    parse_query(cypher).is_ok()
}

fn rows(cypher: &str) -> Result<usize, String> {
    let store = GraphStore::new();
    let q = parse_query(cypher).map_err(|e| format!("parse: {e:?}"))?;
    QueryExecutor::new(&store)
        .execute(&q)
        .map(|b| b.records.len())
        .map_err(|e| format!("exec: {e:?}"))
}

/// Both shapes are actually reachable, so the pairs below are not two spellings
/// of the same parse.
///
/// Without this the whole file could pass while testing one representation
/// twice — the "test that cannot fail" shape, one level up.
#[test]
fn the_two_representations_are_both_reachable() {
    let pipeline = shape("MATCH (n) CREATE (m) WITH m MATCH (o) RETURN o");
    let by_kind = shape("MATCH (n) WITH n RETURN n");
    assert_eq!(pipeline, "pipeline", "the pipeline sample no longer reaches the pipeline");
    assert_eq!(by_kind, "by-kind", "the by-kind sample no longer reaches the by-kind fields");
    assert!(
        pipeline != by_kind,
        "both samples parsed as `{pipeline}` — this file is testing one shape twice"
    );
}

/// A validation rule must reject in both shapes, or in neither.
///
/// Each pair is the same violation written two ways. A rule wired into one
/// representation only makes one side of a pair accept.
#[test]
fn validation_rules_reach_both_representations() {
    let pairs: &[(&str, &str, &str)] = &[
        (
            "value used as a pattern (#795)",
            "WITH 123 AS n MATCH (n) RETURN n",
            "CREATE (x) WITH 123 AS n MATCH (n) RETURN n",
        ),
        (
            "non-boolean operand (#779)",
            "MATCH (n) WHERE 123 AND true RETURN n",
            "CREATE (x) WITH x MATCH (n) WHERE 123 AND true RETURN n",
        ),
        (
            "property access on a non-map (#791)",
            "WITH 123 AS m RETURN m.num",
            "CREATE (x) WITH 123 AS m RETURN m.num",
        ),
        (
            "ORDER BY out of scope (#777)",
            "MATCH (a), (c) WITH a WITH a ORDER BY c RETURN a",
            "CREATE (x) WITH x MATCH (a), (c) WITH a WITH a ORDER BY c RETURN a",
        ),
    ];
    for (what, one, two) in pairs {
        assert_eq!(
            accepted(one),
            accepted(two),
            "{what}: `{one}` parsed as {} and `{two}` as {} — the rule reaches one shape only",
            shape(one),
            shape(two),
        );
    }
}

/// The planner must produce the same row count in both shapes.
///
/// `UNWIND … WITH … UNWIND` (#785) is the case that motivated this: the
/// pipeline handled it and the by-kind path dropped the second `UNWIND`
/// entirely, so one spelling answered and the other raised
/// `VariableNotFound`.
#[test]
fn the_planner_agrees_across_representations() {
    let pairs: &[(&str, &str, &str)] = &[
        (
            "UNWIND after WITH (#785)",
            "WITH [1,2] AS b UNWIND b AS c RETURN c",
            "UNWIND [9] AS z WITH [1,2] AS b UNWIND b AS c RETURN c",
        ),
        (
            "aggregating WITH then UNWIND",
            "WITH [1,2,3] AS xs UNWIND xs AS y RETURN y",
            "UNWIND [1,2,3] AS a WITH collect(a) AS xs UNWIND xs AS y RETURN y",
        ),
    ];
    for (what, one, two) in pairs {
        let (a, b) = (rows(one), rows(two));
        assert!(a.is_ok(), "{what}: `{one}` -> {a:?}");
        assert!(b.is_ok(), "{what}: `{two}` -> {b:?}");
        assert_eq!(
            a.unwrap(),
            b.unwrap(),
            "{what}: `{one}` ({}) and `{two}` ({}) disagree",
            shape(one),
            shape(two),
        );
    }
}
