//! Lists of floats behave as lists, and `IN` coerces numbers (#605, #606).
//!
//! A list literal whose numeric elements include a float parses as
//! `PropertyValue::Vector` — the embedding type — because the type is inferred
//! from the values, and nothing in `[1.0, 2.0]` distinguishes an embedding from
//! a list of numbers. Every list operation then failed, and **three failed
//! silently**: indexing returned null, a list comprehension returned `[]`, and
//! `reduce` returned its seed. Those read as true negatives.
//!
//! The inference is not removed here, and the reason is worth stating: the
//! `Vector` *type* is what marks a property as an embedding for the vector
//! index, so making literals `Array` would stop `{embedding: [0.1, 0.2]}` being
//! indexed. Declaring an embedding some other way is a design question, left on
//! #605. What changes is that anything expecting a list accepts a `Vector` as
//! one.
//!
//! Separately, `IN` compared with `PartialEq`, so `7.0 IN [7, 99]` was false
//! while `p.score = 7` matched a float 7.0 — `IN` disagreed with `=` about
//! whether an integer and a float can be equal.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn one(cypher: &str) -> PropertyValue {
    let store = GraphStore::new();
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        _ => PropertyValue::Null,
    }
}

fn floats(cypher: &str) -> Vec<f64> {
    match one(cypher) {
        PropertyValue::Array(items) => items
            .iter()
            .map(|p| match p {
                PropertyValue::Float(f) => *f,
                PropertyValue::Integer(i) => *i as f64,
                other => panic!("{other:?}"),
            })
            .collect(),
        PropertyValue::Vector(v) => v.iter().map(|f| *f as f64).collect(),
        other => panic!("{cypher}: {other:?}"),
    }
}

// ------------------------------------------------------- the silent three

#[test]
fn indexing_a_float_list_returns_the_element() {
    assert_eq!(one("RETURN [1.0, 2.0][0] AS r"), PropertyValue::Float(1.0));
    assert_eq!(one("RETURN [1.0, 2.0][1] AS r"), PropertyValue::Float(2.0));
    assert_eq!(one("RETURN [1.0, 2.0][-1] AS r"), PropertyValue::Float(2.0));
    assert_eq!(one("RETURN [1.0, 2.0][9] AS r"), PropertyValue::Null, "out of range is null");
}

#[test]
fn a_comprehension_over_a_float_list_filters_rather_than_emptying() {
    assert_eq!(floats("RETURN [x IN [1.0, 2.0, 3.0] WHERE x > 1.5] AS r"), vec![2.0, 3.0]);
    assert_eq!(floats("RETURN [x IN [1.0, 2.0] | x * 2] AS r"), vec![2.0, 4.0]);
}

#[test]
fn reduce_over_a_float_list_accumulates_rather_than_returning_the_seed() {
    assert_eq!(one("RETURN reduce(s = 0.0, x IN [1.0, 2.0] | s + x) AS r"), PropertyValue::Float(3.0));
}

// ------------------------------------------------------- the loud ones

#[test]
fn the_list_functions_accept_a_float_list() {
    assert_eq!(one("RETURN size([1.0, 2.0]) AS r"), PropertyValue::Integer(2));
    assert_eq!(one("RETURN head([1.0, 2.0]) AS r"), PropertyValue::Float(1.0));
    assert_eq!(one("RETURN last([1.0, 2.0]) AS r"), PropertyValue::Float(2.0));
    assert_eq!(floats("RETURN reverse([1.0, 2.0]) AS r"), vec![2.0, 1.0]);
}

#[test]
fn predicate_functions_accept_a_float_list() {
    assert_eq!(one("RETURN all(x IN [1.0, 2.0] WHERE x > 0) AS r"), PropertyValue::Boolean(true));
    assert_eq!(one("RETURN any(x IN [1.0, 2.0] WHERE x > 1.5) AS r"), PropertyValue::Boolean(true));
    assert_eq!(one("RETURN none(x IN [1.0, 2.0] WHERE x > 9) AS r"), PropertyValue::Boolean(true));
}

// ------------------------------------------------------- IN coercion

#[test]
fn in_matches_a_float_against_an_integer_list() {
    assert_eq!(one("RETURN 7.0 IN [7, 99] AS r"), PropertyValue::Boolean(true));
    assert_eq!(one("RETURN 7 IN [7.0, 99.0] AS r"), PropertyValue::Boolean(true));
    assert_eq!(one("RETURN 7.5 IN [7, 99] AS r"), PropertyValue::Boolean(false));
}

#[test]
fn in_agrees_with_equality() {
    // The inconsistency that made this a surprise: `=` coerced and `IN` did not.
    let mut store = GraphStore::new();
    let id = store.create_node("P");
    let _ = store.set_node_property("default", id, "score".to_string(), PropertyValue::Float(7.0));

    let count = |cypher: &str| -> i64 {
        let query = parse_query(cypher).unwrap();
        let batch = QueryExecutor::new(&store).execute(&query).unwrap();
        match batch.records[0].get("r") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(count("MATCH (p:P) WHERE p.score = 7 RETURN count(p) AS r"), 1);
    assert_eq!(
        count("MATCH (p:P) WHERE p.score IN [7, 99] RETURN count(p) AS r"),
        1,
        "IN must agree with ="
    );
}

#[test]
fn in_still_works_on_ordinary_lists() {
    assert_eq!(one("RETURN 1 IN [1, 2] AS r"), PropertyValue::Boolean(true));
    assert_eq!(one("RETURN 3 IN [1, 2] AS r"), PropertyValue::Boolean(false));
    assert_eq!(one("RETURN \"a\" IN [\"a\", \"b\"] AS r"), PropertyValue::Boolean(true));
    assert_eq!(one("RETURN \"a\" IN [1, 2] AS r"), PropertyValue::Boolean(false));
}

// ------------------------------------------------------- #606

#[test]
fn to_integer_yields_null_rather_than_failing() {
    assert_eq!(one("RETURN toInteger(\"abc\") AS r"), PropertyValue::Null);
    assert_eq!(one("RETURN toFloat(\"abc\") AS r"), PropertyValue::Null);
    assert_eq!(one("RETURN toInteger(null) AS r"), PropertyValue::Null);
    assert_eq!(one("RETURN toFloat(null) AS r"), PropertyValue::Null);
}

#[test]
fn to_integer_still_converts_what_it_can() {
    assert_eq!(one("RETURN toInteger(\"42\") AS r"), PropertyValue::Integer(42));
    assert_eq!(one("RETURN toInteger(3.9) AS r"), PropertyValue::Integer(3), "truncates");
    assert_eq!(one("RETURN toInteger(7) AS r"), PropertyValue::Integer(7));
    assert_eq!(one("RETURN toFloat(\"1.5\") AS r"), PropertyValue::Float(1.5));
    assert_eq!(one("RETURN toFloat(3) AS r"), PropertyValue::Float(3.0));
}

#[test]
fn the_filtering_idiom_to_integer_exists_for_now_works() {
    // The query the error made impossible: parse a list of inputs and keep the
    // ones that are numbers.
    match one("RETURN [x IN [\"1\", \"nope\", \"3\"] | toInteger(x)] AS r") {
        PropertyValue::Array(items) => {
            assert_eq!(
                items,
                vec![
                    PropertyValue::Integer(1),
                    PropertyValue::Null,
                    PropertyValue::Integer(3)
                ],
                "one element per input, with the unparseable one null rather than \
                 the whole query failing"
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(one("RETURN toInteger(\"nope\") IS NULL AS r"), PropertyValue::Boolean(true));
}

// ------------------------------------------------------- unchanged

#[test]
fn an_integer_list_is_still_an_array() {
    // The `Vector` inference only fires when a float is present, and integer
    // lists must be unaffected.
    match one("RETURN [1, 2] AS r") {
        PropertyValue::Array(items) => assert_eq!(items.len(), 2),
        other => panic!("an all-integer list should stay an Array, got {other:?}"),
    }
}

#[test]
fn a_vector_property_is_still_a_vector() {
    // The behaviour the inference exists for: a stored embedding keeps its type,
    // which is what marks it for the vector index.
    let mut store = GraphStore::new();
    let id = store.create_node("Doc");
    let _ = store.set_node_property(
        "default",
        id,
        "embedding".to_string(),
        PropertyValue::Vector(vec![0.1, 0.2, 0.3]),
    );
    assert!(matches!(
        store.node_columns.get_property(id.as_u64() as usize, "embedding"),
        PropertyValue::Vector(_)
    ));
    // And it reads as a list too, which is the point of the change.
    let query = parse_query("MATCH (d:Doc) RETURN size(d.embedding) AS r").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert_eq!(
        batch.records[0].get("r"),
        Some(&Value::Property(PropertyValue::Integer(3)))
    );
}
