//! Every error class DX-06 measures says how to fix the query (DX-06, #1781).
//!
//! The issue counted 11 of 17 fault classes with no suggested repair. Each is
//! triggered here through the engine, as a client would, and must carry its
//! code, its cause and a repair; the ones with a token to point at carry a
//! span too.

use samyama::compat::Refusal;
use samyama::graph::GraphStore;
use samyama::query::executor::planner::KNOWN_DB_PROCEDURES;
use samyama::query::QueryEngine;

const TENANT: &str = "default";
/// The span annotation `QueryEngine` appends: ` --> line:column`.
const SPAN: &str = "-->";

/// The error a query raises, as a client sees it.
fn error(query: &str) -> String {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:P {name: 'a', n: 1})", &mut store, TENANT)
        .expect("setup");
    engine
        .execute_mut(query, &mut store, TENANT)
        .map(|_| ())
        .expect_err(&format!("`{query}` should fail"))
        .to_string()
}

fn assert_says(query: &str, parts: &[&str]) {
    let err = error(query);
    for part in parts {
        assert!(err.contains(part), "`{query}`: missing {part:?} in: {err}");
    }
}

#[test]
fn divide_and_modulo_by_zero_suggest_a_guard() {
    assert_says(
        "RETURN 1 / 0",
        &[
            "RuntimeError",
            "Division by zero",
            "CASE WHEN d = 0 THEN null ELSE n / d END",
            SPAN,
        ],
    );
    assert_says("RETURN 1 % 0", &["Modulo by zero", "CASE WHEN d = 0", SPAN]);
}

#[test]
fn a_duplicate_column_suggests_a_distinct_alias() {
    assert_says(
        "RETURN 1 AS a, 2 AS a",
        &["ClauseConflict", "`a`", "AS a_2", SPAN],
    );
}

#[test]
fn a_missing_index_points_to_show_indexes() {
    assert_says(
        "DROP INDEX ON :P(nothing)",
        &["does not exist", "SHOW INDEXES", SPAN],
    );
}

#[test]
fn a_non_integer_list_index_suggests_to_integer() {
    assert_says(
        "RETURN [1, 2]['x']",
        &["TypeError", "must be an integer", "toInteger(i)"],
    );
}

#[test]
fn a_property_on_a_scalar_says_what_to_use_instead() {
    assert_says(
        "WITH 1 AS x RETURN x.name",
        &[
            "VariableTypeError",
            "`x`",
            "use `x` itself, or bind it",
            SPAN,
        ],
    );
}

#[test]
fn an_operand_type_error_names_both_types_and_the_conversions() {
    assert_says(
        "RETURN 1 + {a: 1}",
        &[
            "TypeError",
            "`+`",
            "Integer and Map",
            "toInteger(), toFloat() or toString()",
            SPAN,
        ],
    );
    for (query, op) in [
        ("RETURN 1 - 'a'", "`-`"),
        ("RETURN 2 * 'a'", "`*`"),
        ("RETURN 2 / 'a'", "`/`"),
        ("RETURN 2 % 'a'", "`%`"),
    ] {
        assert_says(
            query,
            &["TypeError", op, "Integer and String", "toInteger()", SPAN],
        );
    }
}

#[test]
fn an_unbound_variable_suggests_a_bound_one_and_how_to_bind() {
    assert_says(
        "MATCH (person:P) RETURN persn.name",
        &[
            "VariableNotBound",
            "`persn`",
            "Did you mean `person`?",
            "MATCH, UNWIND or WITH",
            SPAN,
        ],
    );
    // Nothing close in scope: no guess, still the advice.
    let err = error("RETURN zzz");
    assert!(!err.contains("Did you mean"), "{err}");
    assert!(err.contains("MATCH, UNWIND or WITH"), "{err}");
}

#[test]
fn an_undefined_parameter_says_how_to_supply_it_on_each_surface() {
    assert_says(
        "RETURN $limit",
        &[
            "`$limit`",
            "key `limit` in the `params` object",
            "`limit <value>`",
            "GRAPH.QUERY",
            SPAN,
        ],
    );
}

#[test]
fn an_unknown_function_suggests_the_known_one() {
    assert_says(
        "RETURN lenght('abc')",
        &["UnknownFunction", "Did you mean `length()`?", SPAN],
    );
    let err = error("RETURN frobnicate(1)");
    assert!(!err.contains("Did you mean"), "{err}");
    assert!(err.contains("Check its spelling"), "{err}");
    assert!(
        !err.contains("Checked at compile time"),
        "developer rationale stays in the code: {err}"
    );
}

#[test]
fn an_unknown_procedure_suggests_the_known_one_or_lists_them() {
    assert_says(
        "CALL db.lables()",
        &["Procedure.NotFound", "Did you mean `db.labels`?"],
    );
    assert_says("CALL nothing.like.it()", &["db.labels", "`algo.<name>()`"]);
}

#[test]
fn wrong_arity_shows_how_to_call_it() {
    assert_says(
        "RETURN toUpper()",
        &["ArgumentError", "takes at least 1 argument", "`toUpper(a)`"],
    );
}

#[test]
fn every_listed_procedure_is_one_the_planner_dispatches() {
    // The list is what "did you mean" offers, so each must be real: whatever
    // else a call to it says (missing arguments, say), it is not "unknown".
    for name in KNOWN_DB_PROCEDURES {
        let mut store = GraphStore::new();
        let result = QueryEngine::new().execute_mut(&format!("CALL {name}()"), &mut store, TENANT);
        if let Err(e) = result {
            assert!(!e.to_string().contains("Unknown procedure"), "{name}: {e}");
        }
    }
}

#[test]
fn a_repair_names_nothing_the_migration_report_groups_by() {
    // `compat` groups refusals by their text with quoted names blanked, so a
    // repair must quote every name it mentions, or each name becomes its own
    // cause and the report lists instances instead of causes.
    let cause = |query: &str| {
        Refusal {
            code: String::new(),
            detail: error(query),
        }
        .cause()
    };
    for (one, other) in [
        ("RETURN $a", "RETURN $limit"),
        ("RETURN lenght('x')", "RETURN toLowr('x')"),
        (
            "MATCH (person:P) RETURN persn",
            "MATCH (thing:P) RETURN thng",
        ),
        ("CALL db.lables()", "CALL db.labls()"),
        ("RETURN 1 AS a, 2 AS a", "RETURN 1 AS b, 2 AS b"),
        ("RETURN toUpper()", "RETURN toLower()"),
    ] {
        assert_eq!(cause(one), cause(other), "`{one}` and `{other}`");
    }
}
