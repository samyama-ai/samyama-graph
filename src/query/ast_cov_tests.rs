//! Additional unit tests for AST helpers: clause names, path modes, operator
//! symbols, result column naming and write classification.

use super::*;
use crate::query::parser::parse_query;

fn empty_pattern() -> Pattern {
    Pattern { paths: vec![] }
}

fn call(name: &str) -> CallClause {
    CallClause {
        procedure_name: name.to_string(),
        arguments: vec![],
        yield_items: vec![],
    }
}

#[test]
fn clause_kind_names_match_and_optional_match_apart() {
    let m = Clause::Match(MatchClause {
        pattern: empty_pattern(),
        optional: false,
    });
    let om = Clause::Match(MatchClause {
        pattern: empty_pattern(),
        optional: true,
    });
    assert_eq!(m.kind(), "MATCH");
    assert_eq!(om.kind(), "OPTIONAL MATCH");
    assert!(!m.is_write());
}

#[test]
fn clause_kind_for_create_foreach_and_call() {
    let c = Clause::Create(CreateClause {
        pattern: empty_pattern(),
    });
    assert_eq!(c.kind(), "CREATE");
    assert!(c.is_write());
    let f = Clause::Foreach(ForeachClause {
        variable: "x".into(),
        expression: Expression::Literal(PropertyValue::Null),
        body: vec![],
    });
    assert_eq!(f.kind(), "FOREACH");
    assert!(f.is_write());
    let k = Clause::Call(call("db.labels"));
    assert_eq!(k.kind(), "CALL");
    assert!(!k.is_write());
}

#[test]
fn path_restrictor_names_and_prefix_closure() {
    assert_eq!(PathRestrictor::default(), PathRestrictor::Trail);
    assert_eq!(PathRestrictor::Walk.as_str(), "WALK");
    assert_eq!(PathRestrictor::Trail.as_str(), "TRAIL");
    assert_eq!(PathRestrictor::Acyclic.as_str(), "ACYCLIC");
    assert_eq!(PathRestrictor::Simple.as_str(), "SIMPLE");
    assert!(PathRestrictor::Walk.is_prefix_closed());
    assert!(PathRestrictor::Trail.is_prefix_closed());
    assert!(PathRestrictor::Acyclic.is_prefix_closed());
    assert!(!PathRestrictor::Simple.is_prefix_closed());
}

#[test]
fn path_selector_names_and_predicates() {
    assert_eq!(PathSelector::default(), PathSelector::All);
    let all = [
        (PathSelector::All, "ALL", false, false),
        (PathSelector::Any, "ANY", false, true),
        (PathSelector::AllShortest, "ALL SHORTEST", true, false),
        (PathSelector::AnyShortest, "ANY SHORTEST", true, true),
    ];
    for (sel, name, shortest, single) in all {
        assert_eq!(sel.as_str(), name);
        assert_eq!(sel.is_shortest(), shortest, "{name}");
        assert_eq!(sel.is_single(), single, "{name}");
    }
}

#[test]
fn binary_operator_symbols_are_what_a_user_types() {
    let cases = [
        (BinaryOp::Eq, "="),
        (BinaryOp::Ne, "<>"),
        (BinaryOp::Lt, "<"),
        (BinaryOp::Le, "<="),
        (BinaryOp::Gt, ">"),
        (BinaryOp::Ge, ">="),
        (BinaryOp::And, "AND"),
        (BinaryOp::Or, "OR"),
        (BinaryOp::Add, "+"),
        (BinaryOp::Sub, "-"),
        (BinaryOp::Mul, "*"),
        (BinaryOp::Div, "/"),
        (BinaryOp::Pow, "^"),
        (BinaryOp::Xor, "XOR"),
        (BinaryOp::Mod, "%"),
        (BinaryOp::StartsWith, "STARTS WITH"),
        (BinaryOp::EndsWith, "ENDS WITH"),
        (BinaryOp::Contains, "CONTAINS"),
        (BinaryOp::In, "IN"),
        (BinaryOp::RegexMatch, "=~"),
    ];
    for (op, sym) in cases {
        assert_eq!(op.symbol(), sym);
    }
}

fn item(expression: Expression, alias: Option<&str>, text: Option<&str>) -> ReturnItem {
    ReturnItem {
        expression,
        alias: alias.map(str::to_string),
        source_text: text.map(str::to_string),
    }
}

#[test]
fn column_name_prefers_alias_then_source_text_then_shape() {
    let var = Expression::Variable("n".into());
    assert_eq!(item(var.clone(), Some("a"), Some("n")).column_name(0), "a");
    assert_eq!(item(var.clone(), None, Some("n ")).column_name(0), "n ");
    assert_eq!(item(var, None, None).column_name(3), "n");
    let prop = Expression::Property {
        variable: "n".into(),
        property: "name".into(),
    };
    assert_eq!(item(prop, None, None).column_name(0), "n.name");
    let lit = Expression::Literal(PropertyValue::Integer(1));
    assert_eq!(item(lit, None, None).column_name(2), "col_2");
}

#[test]
fn write_clause_names_the_first_write_in_either_ast_shape() {
    let mut q = Query::new();
    assert_eq!(q.write_clause(), None);
    q.clauses.push(Clause::Match(MatchClause {
        pattern: empty_pattern(),
        optional: false,
    }));
    q.clauses.push(Clause::Create(CreateClause {
        pattern: empty_pattern(),
    }));
    assert_eq!(q.write_clause(), Some("CREATE"));

    let legacy = |src: &str| parse_query(src).unwrap().write_clause();
    assert_eq!(legacy("CREATE (n:P)"), Some("CREATE"));
    assert_eq!(legacy("MATCH (n) DELETE n"), Some("DELETE"));
    assert_eq!(legacy("MATCH (n) SET n.x = 1"), Some("SET"));
    assert_eq!(legacy("MATCH (n) REMOVE n.x"), Some("REMOVE"));
    assert_eq!(legacy("MATCH (n) RETURN n"), None);
}

#[test]
fn legacy_write_fields_are_found_when_set_directly() {
    let parsed = parse_query("MATCH (n) DELETE n").unwrap();
    let mut q = Query::new();
    q.delete_clause = parsed.delete_clause.clone();
    assert_eq!(q.write_clause(), Some("DELETE"));
    assert!(q.is_write());

    let parsed = parse_query("MATCH (n) SET n.x = 1").unwrap();
    let mut q = Query::new();
    q.set_clauses = parsed.set_clauses.clone();
    assert_eq!(q.write_clause(), Some("SET"));
    assert!(q.is_write());

    let parsed = parse_query("MATCH (n) REMOVE n.x").unwrap();
    let mut q = Query::new();
    q.remove_clauses = parsed.remove_clauses.clone();
    assert_eq!(q.write_clause(), Some("REMOVE"));
    assert!(q.is_write());
}

#[test]
fn schema_changes_are_writes() {
    let mut q = Query::new();
    assert!(!q.is_write());
    q.drop_hierarchy_index = Some("h".into());
    assert!(q.is_write());

    let mut q = Query::new();
    q.rebuild_hierarchy_index = Some("h".into());
    assert!(q.is_write());

    for src in [
        "CREATE INDEX ON :Person(name)",
        "DROP INDEX ON :Person(name)",
        "CREATE HIERARCHY INDEX h ON ()-[:IS_A]->()",
    ] {
        let q = parse_query(src).unwrap();
        assert!(q.is_write(), "{src} should be a write");
    }
}

#[test]
fn a_mutating_procedure_call_is_a_write_in_either_ast_shape() {
    let mut legacy = Query::new();
    legacy.call_clause = Some(call("algo.or.solve"));
    assert!(legacy.is_write());

    let mut read = Query::new();
    read.call_clause = Some(call("db.labels"));
    assert!(!read.is_write());

    let mut pipeline = Query::new();
    pipeline.clauses.push(Clause::Call(call("algo.or.solve")));
    assert!(pipeline.is_write());

    let mut pipeline_read = Query::new();
    pipeline_read.clauses.push(Clause::Call(call("db.labels")));
    pipeline_read.clauses.push(Clause::Match(MatchClause {
        pattern: empty_pattern(),
        optional: false,
    }));
    assert!(!pipeline_read.is_write());
}

#[test]
fn a_write_inside_a_union_or_subquery_makes_the_whole_query_a_write() {
    let write = parse_query("CREATE (n:P)").unwrap();
    let read = parse_query("MATCH (n) RETURN n").unwrap();

    let mut q = Query::new();
    q.union_queries.push((read.clone(), false));
    assert!(!q.is_write());
    q.union_queries.push((write.clone(), true));
    assert!(q.is_write());

    let mut q = Query::new();
    q.correlated_call = Some(CorrelatedCall {
        imports: None,
        body: Box::new(read.clone()),
    });
    assert!(!q.is_write());
    q.correlated_call = Some(CorrelatedCall {
        imports: Some(vec!["n".into()]),
        body: Box::new(write.clone()),
    });
    assert!(q.is_write());

    let mut q = Query::new();
    q.call_subquery = Some(Box::new(read));
    assert!(!q.is_write());
    q.call_subquery = Some(Box::new(write));
    assert!(q.is_write());
}

#[test]
fn query_default_is_an_empty_read() {
    let q = Query::default();
    assert!(q.is_read_only());
    assert!(!q.is_write());
    assert_eq!(q.write_clause(), None);
}
