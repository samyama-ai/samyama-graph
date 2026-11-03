//! `AND` / `OR` / `XOR` need boolean operands (#779).
//!
//! ```text
//! RETURN 123 AND true      -> SyntaxError: InvalidArgumentType
//! ```
//!
//! 26 TCK scenarios across Boolean1, Boolean2 and Boolean3, and we answered
//! every one.
//!
//! The whole difficulty is **what can be known at compile time**. Cypher raises
//! this at *compile* time, so it can only apply to operands whose type is
//! visible in the text. `n.prop AND true` is legal to write — the type is not
//! known until the row arrives — and rejecting it would break ordinary queries,
//! which `validate.rs` opens by calling the worse failure. Most of this file is
//! therefore the accept side.

use samyama::query::parser::parse_query;

fn rejected(q: &str) -> bool {
    parse_query(q).is_err()
}
fn accepted(q: &str) -> bool {
    parse_query(q).is_ok()
}

/// Every literal type the TCK pairs with a boolean.
#[test]
fn a_literal_non_boolean_operand_is_refused() {
    for expr in [
        "123 AND true",
        "123.4 AND false",
        "123.4 AND null",
        "'foo' AND true",
        "[] AND false",
        "[true] AND false",
        "[null] AND null",
        "{} AND true",
        "{x: []} AND true",
    ] {
        assert!(rejected(&format!("RETURN {expr}")), "should be refused: {expr}");
    }
}

/// All three operators, and either side.
#[test]
fn all_three_operators_and_both_sides_are_checked() {
    for expr in [
        "123 AND true", "true AND 123",
        "123 OR true",  "true OR 123",
        "123 XOR true", "true XOR 123",
    ] {
        assert!(rejected(&format!("RETURN {expr}")), "should be refused: {expr}");
    }
}

/// **`null` is allowed.** It is the unknown boolean, not a wrong type —
/// `123.4 AND null` fails on the `123.4`, not on the `null`.
#[test]
fn null_is_a_valid_boolean_operand() {
    assert!(accepted("RETURN null AND null"));
    assert!(accepted("RETURN true AND null"));
    assert!(accepted("RETURN null OR false"));
}

/// **A value whose type is only known at run time is fine.**
///
/// This is the accept side that matters. `n.prop` might hold a boolean; Cypher
/// finds out when the row arrives and reports a runtime error then. Rejecting
/// it at compile time would break ordinary queries.
#[test]
fn a_runtime_typed_operand_is_not_a_compile_time_error() {
    for q in [
        "MATCH (n) RETURN n.flag AND true",
        "MATCH (n) WHERE n.a AND n.b RETURN n",
        "MATCH (n) RETURN n.a OR n.b XOR n.c",
        "UNWIND [true, false] AS x RETURN x AND true",
        "MATCH (n) WHERE n.age > 3 AND n.name = 'x' RETURN n",
        "WITH true AS t RETURN t AND false",
    ] {
        assert!(accepted(q), "must still be accepted: {q}");
    }
}

/// Nested inside other expressions, both directions.
#[test]
fn the_check_reaches_nested_expressions() {
    assert!(rejected("MATCH (n) WHERE (123 AND true) RETURN n"));
    assert!(rejected("RETURN CASE WHEN 1 AND true THEN 1 ELSE 2 END"));
    // ...without disturbing the same shapes with legal operands.
    assert!(accepted("MATCH (n) WHERE (n.a AND true) RETURN n"));
    assert!(accepted("RETURN CASE WHEN true AND false THEN 1 ELSE 2 END"));
}

/// Comparisons produce booleans, so they are legal operands even though their
/// operands are not.
#[test]
fn comparisons_are_boolean_and_stay_legal() {
    assert!(accepted("RETURN 1 < 2 AND 3 > 2"));
    assert!(accepted("MATCH (n) WHERE n.x = 1 AND n.y = 2 RETURN n"));
}
