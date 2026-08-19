//! `EXPLAIN` renders a predicate that reparses to the same predicate (#541).
//!
//! It did not. LDBC IC3's
//! `A AND B AND (place.name = X OR place.name = Y)` printed as
//! `A AND B AND place.name = X OR place.name = Y`, which by precedence reads
//! `(A AND B AND C) OR D` — a different predicate, and one that returns every
//! Venezuelan row regardless of the date window.
//!
//! The engine was running the right thing; only the rendering was wrong. That
//! is not a small distinction to leave standing: `EXPLAIN` and `PROFILE` exist
//! so a person can reason about a plan (#517), and per Axiom 4 an agent is
//! meant to consume plans as data. A rendering that changes the meaning of the
//! predicate defeats both, and the issue records an hour lost to writing a P0
//! report about a bug that was not there.
//!
//! The strong form of the property is checked by `a_rendered_predicate_reparses`:
//! rather than asserting on particular strings, it takes the rendered predicate,
//! parses it again, and asserts the two evaluate to the same rows. A renderer
//! that is merely *different* passes; one that changes the meaning cannot.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn plan_of(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("EXPLAIN should run");
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

/// Rows chosen so the two readings of an unparenthesised predicate differ.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    for (d, c) in [(5i64, "X"), (50, "X"), (50, "Y"), (500, "Y")] {
        let id = store.create_node("R");
        let _ = store.set_node_property("default", id, "d".to_string(), PropertyValue::Integer(d));
        let _ = store.set_node_property(
            "default",
            id,
            "c".to_string(),
            PropertyValue::String(c.to_string()),
        );
    }
    store
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let query = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store).execute(&query).expect("query should run").records.len()
}

#[test]
fn a_grouped_or_keeps_its_parentheses() {
    let store = fixture();
    let text = plan_of(
        &store,
        "MATCH (r:R) WHERE r.d >= 10 AND r.d < 100 AND (r.c = \"X\" OR r.c = \"Y\") RETURN r",
    );
    assert!(
        text.contains("(r.c = String(\"X\") OR r.c = String(\"Y\"))"),
        "the OR must stay bracketed:\n{text}"
    );
}

#[test]
fn the_two_forms_still_differ_in_the_answer() {
    // The premise of the issue: the parser was always right. If this ever fails
    // the bug is real and much worse than a rendering problem.
    let store = fixture();
    assert_eq!(
        count(&store, "MATCH (r:R) WHERE r.d >= 10 AND r.d < 100 AND (r.c = \"X\" OR r.c = \"Y\") RETURN r"),
        2
    );
    assert_eq!(
        count(&store, "MATCH (r:R) WHERE r.d >= 10 AND r.d < 100 AND r.c = \"X\" OR r.c = \"Y\" RETURN r"),
        3
    );
}

#[test]
fn brackets_are_not_added_where_they_are_not_needed() {
    // Bracketing everything would be correct and unreadable. A chain of ANDs at
    // one precedence level should print flat.
    let store = fixture();
    let text = plan_of(&store, "MATCH (r:R) WHERE r.d >= 10 AND r.d < 100 AND r.c = \"X\" RETURN r");
    assert!(!text.contains("(("), "over-bracketed:\n{text}");
    assert!(
        text.contains("r.d >= Integer(10) AND r.d < Integer(100) AND r.c = String(\"X\")"),
        "a flat AND chain should print flat:\n{text}"
    );
}

#[test]
fn a_looser_operator_under_a_tighter_one_is_bracketed() {
    let store = fixture();
    let text = plan_of(&store, "MATCH (r:R) WHERE (r.d + 1) * 2 > 10 RETURN r");
    assert!(text.contains("(r.d + Integer(1)) * Integer(2)"), "{text}");
}

#[test]
fn right_association_is_preserved() {
    // `a - (b - c)` must keep its brackets; `(a - b) - c` does not need them.
    let store = fixture();
    let nested_right = plan_of(&store, "MATCH (r:R) WHERE r.d - (r.d - 1) > 0 RETURN r");
    assert!(nested_right.contains("r.d - (r.d - Integer(1))"), "{nested_right}");

    let nested_left = plan_of(&store, "MATCH (r:R) WHERE (r.d - 1) - 1 > 0 RETURN r");
    assert!(
        nested_left.contains("r.d - Integer(1) - Integer(1)"),
        "a left-associated chain needs no brackets:\n{nested_left}"
    );
}

#[test]
fn not_brackets_a_binary_operand() {
    let store = fixture();
    let text = plan_of(&store, "MATCH (r:R) WHERE NOT (r.d > 10 AND r.c = \"X\") RETURN r");
    assert!(
        text.contains("NOT (r.d > Integer(10) AND r.c = String(\"X\"))"),
        "NOT over a binary operand must bracket it:\n{text}"
    );
}

/// The property, rather than any particular rendering.
#[test]
fn a_rendered_predicate_reparses_to_the_same_predicate() {
    let store = fixture();
    let cases = [
        "r.d >= 10 AND r.d < 100 AND (r.c = \"X\" OR r.c = \"Y\")",
        "r.d >= 10 AND r.d < 100 AND r.c = \"X\" OR r.c = \"Y\"",
        "(r.d > 1 OR r.d > 2) AND r.c = \"X\"",
        "NOT (r.d > 10 AND r.c = \"X\")",
        "(r.d + 1) * 2 > 10",
        "r.d - (r.d - 1) > 0",
        "r.d > 1 XOR r.c = \"X\"",
        "(r.d > 1 XOR r.c = \"X\") AND r.d < 500",
        "r.d ^ 2 > 100",
    ];

    for predicate in cases {
        let original = format!("MATCH (r:R) WHERE {predicate} RETURN r");
        let plan = plan_of(&store, &original);

        // Pull the rendered predicate back out of the Filter line.
        let rendered = plan
            .lines()
            .find_map(|l| {
                let i = l.find("Filter (")? + "Filter (".len();
                let body = &l[i..];
                body.rfind(')').map(|j| body[..j].to_string())
            })
            .unwrap_or_else(|| panic!("no Filter line for {predicate}:\n{plan}"));

        let round_tripped = format!("MATCH (r:R) WHERE {rendered} RETURN r");
        // The rendering prints literals as `Integer(10)` / `String("X")`, which
        // is not Cypher — normalise before reparsing. That is a separate
        // readability question; the meaning is what this test is about.
        let cypher_ish = round_tripped
            .replace("Integer(", "(")
            .replace("String(", "(")
            .replace("Float(", "(");

        let reparsed = match parse_query(&cypher_ish) {
            Ok(q) => q,
            Err(e) => panic!("rendered predicate does not reparse: {rendered}\n  {e:?}"),
        };
        let got = QueryExecutor::new(&store).execute(&reparsed).expect("should run").records.len();
        let want = count(&store, &original);
        assert_eq!(
            got, want,
            "rendering changed the meaning of `{predicate}`\n  rendered: {rendered}"
        );
    }
}
