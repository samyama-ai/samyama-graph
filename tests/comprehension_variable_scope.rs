//! A WHERE predicate's variables are found inside comprehensions too (#948).
//!
//! ```cypher
//! MATCH (n)-->(b)
//! WHERE n.name IN [x IN labels(b) | toLower(x)]
//! RETURN b
//! ```
//!
//! raised `VariableNotFound("b")`.
//!
//! `Planner::collect_expression_variables` decides *where* a WHERE conjunct
//! can be evaluated. It handled `Variable`, `Property`, `Binary`, `Unary`,
//! `Function` and `ExistsSubquery`, then `_ => {}`. So a predicate whose only
//! variables live inside a comprehension reported **no** variables, looked
//! constant, and was pushed to the initial scan — before the expansion that
//! binds `b`.
//!
//! That is the bug the `ExistsSubquery` arm was added for, left in place for
//! every other compound expression. Its comment already gives the rule:
//! over-approximating only defers a filter to a later, still-correct point,
//! while under-approximating evaluates it too early.
//!
//! `VariableNotFound` is the loud failure. The quiet one is a predicate that
//! happens to reference a variable bound early enough to evaluate, but whose
//! meaning depends on a later binding — it filters on the wrong thing and
//! returns a plausible number of rows.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// `(a:A {name: 'c'})-[:T]->(:B)` and `-[:T]->(:C)`.
fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let _ = store.set_node_property("default", a, "name".to_string(),
                                    PropertyValue::String("c".into()));
    let _ = store.set_node_property("default", a, "n".to_string(), PropertyValue::Integer(2));
    for label in ["B", "C"] {
        let t = store.create_node_with_labels([Label::new(label)]);
        let _ = store.set_node_property("default", t, "n".to_string(), PropertyValue::Integer(2));
        store.create_edge(a, t, "T").unwrap();
    }
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

#[test]
fn a_list_comprehension_in_a_where_sees_the_expanded_variable() {
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n)-->(b) WHERE n.name IN [x IN labels(b) | toLower(x)] RETURN b"),
        1
    );
}

#[test]
fn a_quantifier_in_a_where_sees_it_too() {
    // `all`/`any`/`none`/`single` are `PredicateFunction`, a separate variant
    // that fell into the same catch-all.
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n)-->(b) WHERE any(x IN labels(b) WHERE x = 'B') RETURN b"),
        1
    );
    assert_eq!(
        rows(&store, "MATCH (n)-->(b) WHERE none(x IN labels(b) WHERE x = 'B') RETURN b"),
        1
    );
}

#[test]
fn a_case_expression_in_a_where_sees_it_too() {
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n)-->(b) WHERE (CASE WHEN b:B THEN 1 ELSE 0 END) = 1 RETURN b"),
        1
    );
}

#[test]
fn a_list_literal_in_a_where_sees_it_too() {
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n)-->(b) WHERE size([b, n]) = 2 RETURN b"),
        2
    );
}

#[test]
fn reduce_in_a_where_sees_it_too() {
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n)-->(b) WHERE reduce(acc = 0, x IN labels(b) | acc + 1) = 1 RETURN b"),
        2
    );
}

#[test]
fn the_comprehensions_own_variable_is_not_an_outer_dependency() {
    // `x` is bound by the comprehension. Treating it as a dependency would
    // defer the predicate past every point that could apply it, so a query
    // referencing nothing else must still run.
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n) WHERE any(x IN [1, 2] WHERE x = 1) RETURN n"),
        3
    );
    assert_eq!(
        rows(&store, "MATCH (n) WHERE size([x IN [1, 2, 3] | x * 2]) = 3 RETURN n"),
        3
    );
}

#[test]
fn an_ordinary_pushed_predicate_still_filters() {
    // The half that must not regress: a predicate naming only the scanned
    // variable is still pushed down, and still narrows.
    let store = graph();
    assert_eq!(rows(&store, "MATCH (n) WHERE n.name = 'c' RETURN n"), 1);
    assert_eq!(rows(&store, "MATCH (n)-->(b) WHERE n.name = 'c' RETURN b"), 2);
    assert_eq!(rows(&store, "MATCH (n)-->(b) WHERE n.name = 'zzz' RETURN b"), 0);
}
