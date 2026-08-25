//! `all`/`any`/`none`/`single` are three-valued (#826).
//!
//! Tracking only a true-count makes a null predicate result indistinguishable
//! from a `false` one, so the third truth value never reaches the caller — who
//! gets a **boolean they will branch on**, not an error and not a null they
//! might check.
//!
//! `single` is the worst of the four: it does not weaken to a wrong-but-cautious
//! answer, it flips to the *opposite certainty*. `single(x IN [2, null] WHERE
//! x = 2)` returned `true`, asserting exactly one match, when there might be two.
//!
//! The tables below are `Quantifier1`–`Quantifier4` scenario 10, transcribed
//! whole. The four quantifiers disagree on the same input — `[0, null]` with
//! `x = 2` is null, false, null and null respectively — so a test covering one
//! of them says nothing about the others.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `Some(bool)` for a definite answer, `None` for null.
fn quantify(q: &str, list: &str, pred: &str) -> Option<bool> {
    let store = GraphStore::new();
    let cypher = format!("RETURN {q}(x IN {list} WHERE {pred}) AS r");
    let parsed = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&parsed)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(PropertyValue::Boolean(b))) => Some(*b),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

/// One TCK table. `None` in the expectation means the scenario says `null`.
fn check(q: &str, rows: &[(&str, &str, Option<bool>)]) {
    let wrong: Vec<String> = rows
        .iter()
        .filter_map(|(list, pred, want)| {
            let got = quantify(q, list, pred);
            (got != *want).then(|| {
                let show = |v: &Option<bool>| match v {
                    Some(b) => b.to_string(),
                    None => "null".to_string(),
                };
                format!("  {q}(x IN {list} WHERE {pred}) -> {}, want {}", show(&got), show(want))
            })
        })
        .collect();
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

const LISTS: [(&str, &str); 8] = [
    ("[null]", "x = 2"),
    ("[null, null]", "x = 2"),
    ("[0, null]", "x = 2"),
    ("[2, null]", "x = 2"),
    ("[null, 2]", "x = 2"),
    ("[34, 0, null, 5, 900]", "x < 10"),
    ("[34, 10, null, 15, 900]", "x < 10"),
    ("[4, 0, null, -15, 9]", "x < 10"),
];

fn table(q: &str, expected: [Option<bool>; 8]) {
    let rows: Vec<(&str, &str, Option<bool>)> = LISTS
        .iter()
        .zip(expected)
        .map(|((l, p), want)| (*l, *p, want))
        .collect();
    check(q, &rows);
}

/// A single `false` settles it; otherwise a null leaves it undecided.
#[test]
fn all_is_three_valued() {
    use Some as S;
    table("all", [None, None, S(false), None, None, S(false), S(false), None]);
}

/// A single `true` settles it.
#[test]
fn any_is_three_valued() {
    use Some as S;
    table("any", [None, None, None, S(true), S(true), S(true), None, S(true)]);
}

/// A single `true` settles it — to `false`.
#[test]
fn none_is_three_valued() {
    use Some as S;
    table("none", [None, None, None, S(false), S(false), S(false), None, S(false)]);
}

/// **Two** trues settle it, which no other quantifier does: they rule out
/// "exactly one" regardless of what the unknown elements hold. One true does
/// not, because an unknown could be a second.
#[test]
fn single_is_settled_by_a_count_not_by_one_element() {
    use Some as S;
    table("single", [None, None, None, None, None, S(false), None, S(false)]);
}

/// Without nulls the four keep their ordinary two-valued answers, including on
/// the empty list, where they differ from each other.
#[test]
fn null_free_lists_are_unaffected() {
    for (q, empty, some, one) in [
        ("all", true, false, true),
        ("any", false, true, true),
        ("none", true, false, false),
        ("single", false, false, true),
    ] {
        assert_eq!(quantify(q, "[]", "x = 2"), Some(empty), "{q} on []");
        // Two matches: `single` must say false where `all` and `any` say true.
        assert_eq!(quantify(q, "[2, 2, 3]", "x = 2"), Some(some), "{q} on [2, 2, 3]");
        assert_eq!(quantify(q, "[2]", "x = 2"), Some(one), "{q} on [2]");
    }
}
