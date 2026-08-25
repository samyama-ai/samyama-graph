//! Property access on something that cannot carry properties (#791).
//!
//! ```text
//! WITH 123 AS nonMap RETURN nonMap.num     -> TypeError: InvalidArgumentType
//! ```
//!
//! We answered `null`, which is the recurring failure in this codebase: a
//! wrong answer indistinguishable from a legitimate absent property.
//!
//! The TCK asks for this **at compile time**, and that bound is what makes the
//! rule safe. Only a variable a projection binds to a *literal* of a non-map
//! type can be checked; `n.name` where `n` comes from the graph is untouched,
//! because the type is not knowable from the text.
//!
//! Property access is the most common expression in Cypher, so over-rejecting
//! here would be severe. Most of this file is the accept side.

use samyama::query::parser::parse_query;

fn rejected(q: &str) -> bool {
    parse_query(q).is_err()
}
fn accepted(q: &str) -> bool {
    parse_query(q).is_ok()
}

/// Every non-map literal type.
#[test]
fn property_access_on_a_non_map_literal_is_refused() {
    for lit in ["123", "42.45", "true", "'string'", "[1, 2]"] {
        assert!(
            rejected(&format!("WITH {lit} AS nonMap RETURN nonMap.num")),
            "{lit}.num should be refused"
        );
    }
}

/// A map literal *can* carry properties.
#[test]
fn a_map_literal_is_fine() {
    assert!(accepted("WITH {num: 1} AS m RETURN m.num"));
    assert!(accepted("WITH {} AS m RETURN m.num"));
}

/// **Anything read at run time is untouched.**
///
/// This is the accept side that matters. `n` might be a node, a map, or
/// anything else; the type is not knowable from the text, and Cypher reports a
/// runtime error if it turns out wrong. Rejecting these would break essentially
/// every query.
#[test]
fn run_time_typed_values_are_not_compile_time_errors() {
    for q in [
        "MATCH (n) RETURN n.name",
        "MATCH (n) WHERE n.age > 3 RETURN n",
        "MATCH (a)-[r]->(b) RETURN r.weight",
        "MATCH (n) WITH n AS m RETURN m.name",
        "UNWIND [{a: 1}, {a: 2}] AS m RETURN m.a",
        "MATCH (n) WITH n.data AS d RETURN d.inner",
        "WITH $param AS p RETURN p.field",
    ] {
        assert!(accepted(q), "must still be accepted: {q}");
    }
}

/// **Re-binding the name clears the judgement.**
///
/// `WITH 1 AS x` then `WITH {a: 2} AS x` leaves `x` a map. Carrying the first
/// binding forward would reject a valid query — the failure mode this rule is
/// most likely to have.
#[test]
fn rebinding_a_name_clears_it() {
    assert!(accepted("WITH 1 AS x WITH {a: 2} AS x RETURN x.a"));
    assert!(accepted("MATCH (n) WITH 1 AS x WITH n AS x RETURN x.name"));
    // ...and the reverse still catches it.
    assert!(rejected("WITH {a: 1} AS x WITH 2 AS x RETURN x.a"));
}

/// The name survives an intermediate projection that passes it through.
#[test]
fn the_judgement_follows_the_name_through_a_projection() {
    assert!(rejected("WITH 123 AS x WITH x RETURN x.num"));
}

/// The message names the variable and its type, rather than reporting that
/// something somewhere was wrong.
#[test]
fn the_message_names_the_variable() {
    let e = parse_query("WITH 123 AS nonMap RETURN nonMap.num").expect_err("refused");
    let msg = format!("{e:?}");
    assert!(msg.contains("nonMap"), "{msg}");
    assert!(msg.contains("integer"), "{msg}");
}
