//! `CALL db.schema.forLLM(token_budget)` -- AI-05.
//!
//! The requirement asks for a compact, token-budgeted schema description --
//! labels, types, cardinalities, property distributions, sample values and
//! example queries -- retrievable in **one call**. What the engine had was four
//! procedures returning names, so a caller assembled the description with one
//! query per label per property and a model given the names alone guessed at
//! the rest.
//!
//! Two properties matter more than the formatting and are what these tests are
//! mostly about:
//!
//! * the answer is **ordered by size**, so a budget is spent on the parts of
//!   the graph a question is likely to be about; and
//! * a truncated answer **says it is truncated**, in the text and in a
//!   `complete` column, naming what was dropped. Silent truncation is the
//!   defect the rest of this surface had.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

fn call(store: &mut GraphStore, q: &str) -> (String, i64, bool) {
    let engine = QueryEngine::new();
    let batch = engine.execute_mut(q, store, "default").expect("query");
    assert_eq!(batch.records.len(), 1, "one row, one schema");
    let mut text = String::new();
    let mut tokens = 0i64;
    let mut complete = false;
    for (name, v) in batch.records[0].bindings() {
        match (&name[..], v) {
            ("schema", Value::Property(PropertyValue::String(s))) => text = s.clone(),
            ("estimated_tokens", Value::Property(PropertyValue::Integer(n))) => tokens = *n,
            ("complete", Value::Property(PropertyValue::Boolean(b))) => complete = *b,
            _ => {}
        }
    }
    (text, tokens, complete)
}

/// Two labels of very different size, a relationship between them, and a
/// property that most `Person`s do not carry.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "UNWIND range(1, 50) AS i CREATE (:Person {name: 'p' + toString(i)})",
            &mut store,
            "default",
        )
        .expect("people");
    engine
        .execute_mut(
            "CREATE (:Company {name: 'Acme', industry: 'tech'})",
            &mut store,
            "default",
        )
        .expect("company");
    engine
        .execute_mut(
            "MATCH (p:Person), (c:Company) WITH p, c LIMIT 20 CREATE (p)-[:WORKS_AT]->(c)",
            &mut store,
            "default",
        )
        .expect("edges");
    store
}

#[test]
fn one_call_returns_labels_counts_relationships_and_an_example() {
    let mut store = fixture();
    let (text, tokens, complete) = call(&mut store, "CALL db.schema.forLLM()");
    assert!(complete, "nothing here should need truncating:\n{text}");
    assert!(tokens > 0);
    for needle in [
        "(:Person) 50",
        "(:Company) 1",
        "(:Person)-[:WORKS_AT]->(:Company) 20",
        "name:String",
        "MATCH (a:Person)-[:WORKS_AT]->(b:Company)",
    ] {
        assert!(text.contains(needle), "missing `{needle}` from:\n{text}");
    }
}

#[test]
fn the_largest_label_comes_first() {
    // A budget is spent top-down, so the order decides what survives it.
    let mut store = fixture();
    let (text, _, _) = call(&mut store, "CALL db.schema.forLLM()");
    let person = text.find("(:Person)").expect("Person");
    let company = text.find("(:Company)").expect("Company");
    assert!(
        person < company,
        "50 Person nodes and 1 Company, and Company is listed first:\n{text}"
    );
}

#[test]
fn a_small_budget_truncates_and_says_so() {
    let mut store = fixture();
    let (text, tokens, complete) = call(&mut store, "CALL db.schema.forLLM(60)");
    assert!(!complete, "60 tokens cannot hold this schema:\n{text}");
    assert!(
        text.contains("truncated to fit a 60-token budget"),
        "truncation is not stated in the text:\n{text}"
    );
    assert!(
        text.contains("of 2 labels"),
        "the notice does not say how much was dropped:\n{text}"
    );
    // The notice is allowed to overshoot the budget -- it is reserved for --
    // but the schema itself must have stopped near it.
    assert!(
        tokens < 160,
        "asked for 60 tokens and got {tokens}:\n{text}"
    );
}

#[test]
fn a_budget_that_fits_everything_reports_complete() {
    let mut store = fixture();
    let (text, _, complete) = call(&mut store, "CALL db.schema.forLLM(4000)");
    assert!(complete, "{text}");
    assert!(!text.contains("truncated"), "{text}");
}

#[test]
fn property_distributions_are_reported_not_just_names() {
    // AI-05 asks for distributions and sample values: the point is that a model
    // can see a property is mostly null *before* writing a query that filters
    // on it.
    let mut store = fixture();
    let (text, _, _) = call(&mut store, "CALL db.schema.forLLM()");
    assert!(text.contains("null="), "no null fraction:\n{text}");
    assert!(text.contains("distinct="), "no distinct count:\n{text}");
    assert!(text.contains("e.g."), "no sample values:\n{text}");
}

#[test]
fn the_same_graph_renders_the_same_text() {
    // A schema that reorders between calls cannot be cached or diffed.
    let mut store = fixture();
    let (a, _, _) = call(&mut store, "CALL db.schema.forLLM()");
    let (b, _, _) = call(&mut store, "CALL db.schema.forLLM()");
    assert_eq!(a, b);
}

#[test]
fn a_budget_too_small_to_answer_is_refused_rather_than_answered_emptily() {
    let mut store = fixture();
    let engine = QueryEngine::new();
    let err = engine
        .execute_mut("CALL db.schema.forLLM(5)", &mut store, "default")
        .expect_err("5 tokens is not a budget");
    assert!(
        format!("{err}").contains("minimum"),
        "the error does not say what the minimum is: {err}"
    );
}

#[test]
fn a_non_integer_budget_is_refused_rather_than_ignored() {
    // An ignored argument returns a default-sized answer under a call that
    // asked for another size, which the caller discovers as an overflow.
    let mut store = fixture();
    let engine = QueryEngine::new();
    let err = engine
        .execute_mut("CALL db.schema.forLLM('big')", &mut store, "default")
        .expect_err("a string is not a token budget");
    assert!(format!("{err}").contains("token budget"), "{err}");
}
