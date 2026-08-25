//! Comparing values that *contain* null is three-valued (#783).
//!
//! The engine already treated a null **operand** as unknown. A null *inside* a
//! list is different and was answering `false`:
//!
//! ```text
//! RETURN [1] = [null]     expected null, got false
//! ```
//!
//! The rule is not "any null makes it null". A definitive difference wins over
//! an unknown one, because two lists that differ in length or in a known
//! element are unequal whatever the nulls say. Getting that backwards turns
//! `[1, null] = [2, 3]` from `false` into `null`, which is just as wrong and
//! much harder to notice.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn eval(cypher: &str) -> PropertyValue {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        Some(Value::Null) | None => PropertyValue::Null,
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

fn is_null(cypher: &str) -> bool {
    matches!(eval(cypher), PropertyValue::Null)
}
fn is(cypher: &str, want: bool) -> bool {
    eval(cypher) == PropertyValue::Boolean(want)
}

/// A null inside a list makes the comparison unknown.
#[test]
fn a_null_element_makes_the_comparison_unknown() {
    assert!(is_null("RETURN [1] = [null] AS r"));
    assert!(is_null("RETURN [null] = [1] AS r"));
    assert!(is_null("RETURN [null] = [null] AS r"));
    assert!(is_null("RETURN [1, null] = [1, 2] AS r"));
    assert!(is_null("RETURN [[1], [null]] = [[1], [2]] AS r"));
}

/// **A definitive difference beats an unknown one.**
///
/// This is the half that is easy to get wrong in the other direction. If any
/// pair of elements is known to differ, the lists are unequal whatever the
/// nulls elsewhere say — returning null here would be just as wrong and far
/// harder to spot, because null is the answer you expect when nulls appear.
#[test]
fn a_known_difference_settles_it_despite_nulls() {
    assert!(is("RETURN [1, null] = [2, 3] AS r", false));
    assert!(is("RETURN [null, 1] = [null, 2] AS r", false));
}

/// A length difference is definitive: no element can rescue it.
#[test]
fn a_length_difference_is_false_not_null() {
    assert!(is("RETURN [1] = [1, null] AS r", false));
    assert!(is("RETURN [null] = [] AS r", false));
    assert!(is("RETURN [null, null] = [null] AS r", false));
}

/// Lists with no nulls are unaffected.
#[test]
fn ordinary_list_comparison_is_undisturbed() {
    assert!(is("RETURN [1, 2] = [1, 2] AS r", true));
    assert!(is("RETURN [1, 2] = [2, 1] AS r", false));
    assert!(is("RETURN [] = [] AS r", true));
    assert!(is("RETURN ['a'] = ['a'] AS r", true));
    assert!(is("RETURN [[1, 2]] = [[1, 2]] AS r", true));
}

/// `<>` is the negation, and negating unknown is still unknown.
#[test]
fn inequality_negates_a_known_answer_and_propagates_an_unknown_one() {
    assert!(is("RETURN [1, 2] <> [1, 2] AS r", false));
    assert!(is("RETURN [1, 2] <> [2, 1] AS r", true));
    assert!(is_null("RETURN [1] <> [null] AS r"));
}

/// Maps follow the same rule, keys included.
#[test]
fn maps_compare_the_same_way() {
    assert!(is_null("RETURN {a: 1, b: null} = {a: 1, b: 2} AS r"));
    assert!(is("RETURN {a: 1, b: null} = {a: 2, b: 3} AS r", false));
    // A differing key set is definitive.
    assert!(is("RETURN {a: null} = {b: null} AS r", false));
    assert!(is("RETURN {a: 1} = {a: 1} AS r", true));
}

/// Scalar nulls still behave as before — this change must not disturb the
/// existing three-valued guard.
#[test]
fn scalar_null_comparison_is_unchanged() {
    assert!(is_null("RETURN null = 1 AS r"));
    assert!(is_null("RETURN 1 = null AS r"));
    assert!(is_null("RETURN null = null AS r"));
    assert!(is("RETURN 1 = 1 AS r", true));
    assert!(is("RETURN 1 = 2 AS r", false));
}
