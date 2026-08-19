//! `IN` has three answers, not two.
//!
//! ```text
//! null IN [null]      null    -- nothing can be known about it
//! 4 IN [1, null, 3]   null    -- the null might have been the 4
//! 1 IN [1, null]      true    -- a definite match wins regardless
//! null IN []          false   -- nothing to compare with at all
//! ```
//!
//! `PartialEq` on `PropertyValue` is derived, so `Null == Null` is `true`:
//! the first answered `true` and the second answered `false`. Neither is an
//! error a caller would notice — they are values it will branch on, which
//! makes this worse than a refusal (#647).
//!
//! The two cases that catch people out pull in opposite directions, and both
//! are asserted here: an **empty** list is `false` however null the left side
//! is, because no comparison is required; and a list that merely *contains*
//! something null is unknown, even when neither side is itself null
//! (`[null] IN [null]`).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn result_of(cypher: &str) -> Value {
    let q = parse_query(cypher).expect("query should parse");
    let out = QueryExecutor::new(&GraphStore::new())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    out.records[0].get("res").cloned().expect("column res")
}

fn assert_null(cypher: &str) {
    let got = result_of(cypher);
    assert!(
        matches!(got, Value::Null | Value::Property(PropertyValue::Null)),
        "`{cypher}` should be null, got {got:?}"
    );
}

fn assert_bool(cypher: &str, expected: bool) {
    assert_eq!(
        result_of(cypher),
        Value::Property(PropertyValue::Boolean(expected)),
        "for `{cypher}`"
    );
}

#[test]
fn an_undecidable_membership_is_null() {
    assert_null("RETURN null IN [null] AS res");
    assert_null("RETURN null IN [1] AS res");
    assert_null("RETURN 4 IN [1, null, 3] AS res");
}

#[test]
fn a_definite_match_wins_over_a_null_elsewhere_in_the_list() {
    // The ordering that matters: the null must not turn a found match into
    // "unknown". Cypher answers the question it can answer.
    assert_bool("RETURN 1 IN [1, null] AS res", true);
    assert_bool("RETURN 1 IN [null, 1] AS res", true);
}

#[test]
fn an_empty_list_is_false_however_null_the_left_side_is() {
    // Pulls the opposite way from the rule above: with nothing to compare
    // against, there is nothing undecidable.
    assert_bool("RETURN null IN [] AS res", false);
    assert_bool("RETURN [] IN [] AS res", false);
    assert_bool("RETURN 1 IN [] AS res", false);
}

#[test]
fn a_value_that_merely_contains_null_is_undecidable_too() {
    // Neither side *is* null here, so a shallow check answers `false`.
    assert_null("RETURN [null] IN [null] AS res");
    // Same length, one undecidable position, nothing else to settle it.
    assert_null("RETURN [1, 2] IN [[1, null]] AS res");
}

#[test]
fn a_null_only_matters_if_the_comparison_has_to_look_at_it() {
    // The first version of this fix asked "does either side contain a null"
    // and made all four of these unknown. They are `false`: a length mismatch,
    // or a definite difference at any other position, settles the comparison
    // without ever reaching the null.
    assert_bool("RETURN [1] IN [[1, null]] AS res", false);
    assert_bool("RETURN [1, 2] IN [[null, 'foo']] AS res", false);
    assert_bool("RETURN [1, 2] IN [1, [1, 2, null]] AS res", false);
    assert_bool(
        "RETURN [[1, 2], [3, 4]] IN [5, [[1, 2], [3, 4], null]] AS res",
        false,
    );
}

#[test]
fn a_null_free_list_still_answers_true_or_false() {
    assert_bool("RETURN 4 IN [1, 3] AS res", false);
    assert_bool("RETURN 1 IN [1, 2] AS res", true);
    assert_bool("RETURN 'a' IN ['a', 'b'] AS res", true);
    assert_bool("RETURN 'c' IN ['a', 'b'] AS res", false);
    // Cross-type numeric comparison is unaffected by any of this.
    assert_bool("RETURN 1.0 IN [1] AS res", true);
    // A list element compared against a list value.
    assert_bool("RETURN [1] IN [[1], 2] AS res", true);
}
