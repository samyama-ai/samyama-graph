//! Unit tests for `*` expansion in `RETURN *` / `WITH *`, in both AST shapes.

use super::*;
use crate::query::ast::{CreateClause, ForeachClause, ReturnClause};
use crate::query::parser::parse_query;

fn return_columns(q: &Query) -> Vec<String> {
    q.return_clause
        .as_ref()
        .expect("a RETURN")
        .items
        .iter()
        .enumerate()
        .map(|(i, item)| item.column_name(i))
        .collect()
}

fn star_item() -> ReturnItem {
    ReturnItem {
        expression: Expression::Variable(STAR_ITEM.to_string()),
        alias: None,
        source_text: None,
    }
}

fn var_item(v: &str) -> ReturnItem {
    ReturnItem {
        expression: Expression::Variable(v.to_string()),
        alias: None,
        source_text: None,
    }
}

#[test]
fn return_star_lists_every_pattern_variable_in_written_order() {
    let q = parse_query("MATCH p = (a)-[r]->(b) RETURN *").unwrap();
    assert_eq!(return_columns(&q), vec!["p", "a", "r", "b"]);
}

#[test]
fn return_star_does_not_duplicate_an_explicit_column() {
    let q = parse_query("MATCH (a)-[r]->(b) RETURN *, a").unwrap();
    assert_eq!(return_columns(&q), vec!["r", "b", "a"]);
    let q = parse_query("MATCH (a)-[r]->(b) RETURN *, a.x AS r").unwrap();
    assert_eq!(return_columns(&q), vec!["a", "b", "r"]);
}

#[test]
fn return_star_after_create_returns_the_created_variables() {
    let q = parse_query("CREATE (n:P)-[e:T]->(m:P) RETURN *").unwrap();
    assert_eq!(return_columns(&q), vec!["n", "e", "m"]);
    let q = parse_query("CREATE p = (n:P)-[:T]->(m:P) RETURN *").unwrap();
    assert_eq!(return_columns(&q), vec!["p", "n", "m"]);
}

#[test]
fn an_unaliased_expression_beside_a_star_is_not_an_explicit_name() {
    let q = parse_query("MATCH (a) RETURN *, a.x").unwrap();
    assert_eq!(return_columns(&q), vec!["a", "a.x"]);
}

#[test]
fn with_star_passes_scope_through() {
    let q = parse_query("MATCH (a) WITH * MATCH (a)-->(b) RETURN *").unwrap();
    assert_eq!(return_columns(&q), vec!["a", "b"]);
}

#[test]
fn pipeline_star_sees_create_merge_unwind_bindings() {
    let q =
        parse_query("CREATE (a:A) WITH * MERGE (b:B) WITH * UNWIND [1, 2] AS x RETURN *").unwrap();
    assert!(
        q.needs_clause_pipeline,
        "expected the clause pipeline shape"
    );
    assert_eq!(return_columns(&q), vec!["a", "b", "x"]);
}

#[test]
fn pipeline_star_sees_load_csv_and_call_yield_bindings() {
    let q = parse_query("CREATE (n) WITH * LOAD CSV FROM 'file:///x.csv' AS row RETURN *").unwrap();
    assert!(q.needs_clause_pipeline);
    assert_eq!(return_columns(&q), vec!["n", "row"]);

    let q = parse_query("CREATE (n) WITH * LOAD PARQUET FROM 'file:///x.parquet' AS row RETURN *")
        .unwrap();
    assert!(q.needs_clause_pipeline);
    assert_eq!(return_columns(&q), vec!["n", "row"]);

    let q = parse_query("CREATE (n) WITH * CALL db.labels() YIELD label AS l RETURN *").unwrap();
    assert!(q.needs_clause_pipeline);
    assert_eq!(return_columns(&q), vec!["n", "l"]);

    let q = parse_query("CREATE (n) WITH * CALL db.labels() YIELD label RETURN *").unwrap();
    assert_eq!(return_columns(&q), vec!["n", "label"]);
}

#[test]
fn pipeline_with_narrows_the_scope_a_later_star_sees() {
    let q = parse_query("CREATE (a), (b) WITH a CREATE (c) RETURN *").unwrap();
    assert!(q.needs_clause_pipeline);
    assert_eq!(return_columns(&q), vec!["a", "c"]);
}

#[test]
fn expand_into_without_a_star_is_a_no_op() {
    let mut items = vec![var_item("a")];
    assert!(!expand_into(
        &mut items,
        &["a".to_string(), "b".to_string()]
    ));
    assert_eq!(items.len(), 1);
}

#[test]
fn expand_into_reports_a_star_that_expanded_to_nothing() {
    let mut items = vec![star_item()];
    assert!(expand_into(&mut items, &[]));
    assert!(items.is_empty());
    // With something written alongside it, the result is not empty.
    let mut items = vec![star_item(), var_item("z")];
    assert!(!expand_into(&mut items, &[]));
    assert_eq!(items.len(), 1);
}

#[test]
fn pipeline_pass_sets_the_empty_flag_only_for_return() {
    // `WITH *` over nothing is legal; `RETURN *` over nothing is flagged.
    let mut clauses = vec![
        Clause::With(WithClause {
            items: vec![star_item()],
            ..parse_query("WITH 1 AS x RETURN x")
                .unwrap()
                .with_clause
                .unwrap()
        }),
        Clause::Return(ReturnClause {
            items: vec![star_item()],
            ..parse_query("RETURN 1").unwrap().return_clause.unwrap()
        }),
    ];
    assert!(expand_stars_pipeline(&mut clauses));

    let mut only_with = vec![Clause::With(WithClause {
        items: vec![star_item()],
        ..parse_query("WITH 1 AS x RETURN x")
            .unwrap()
            .with_clause
            .unwrap()
    })];
    assert!(!expand_stars_pipeline(&mut only_with));
}

#[test]
fn pipeline_pass_ignores_foreach_and_non_binding_clauses() {
    let mut clauses = vec![
        Clause::Create(CreateClause {
            pattern: parse_query("CREATE (a)")
                .unwrap()
                .create_clause
                .unwrap()
                .pattern,
        }),
        Clause::Foreach(ForeachClause {
            variable: "i".into(),
            expression: Expression::Literal(crate::graph::PropertyValue::Null),
            body: vec![],
        }),
        Clause::Return(ReturnClause {
            items: vec![star_item()],
            ..parse_query("RETURN 1").unwrap().return_clause.unwrap()
        }),
    ];
    assert!(!expand_stars_pipeline(&mut clauses));
    match &clauses[2] {
        Clause::Return(rc) => {
            let names: Vec<String> = rc.items.iter().map(|i| i.column_name(0)).collect();
            // FOREACH's own variable does not leak into the outer scope.
            assert_eq!(names, vec!["a"]);
        }
        other => panic!("expected RETURN, got {other:?}"),
    }
}
