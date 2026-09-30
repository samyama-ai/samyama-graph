//! Additional unit tests for the Cypher parser: error codes, the clause
//! pipeline fallback, literals, row counts and the DDL statements.

use super::*;
use crate::query::ast::Clause;
use crate::query::error_code as codes;

fn ok(q: &str) -> Query {
    parse_query(q).unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn err(q: &str) -> ParseError {
    match parse_query(q) {
        Ok(parsed) => panic!("{q} should not parse, got {parsed:?}"),
        Err(e) => e,
    }
}

fn first_return_literal(q: &str) -> PropertyValue {
    match &ok(q).return_clause.expect("RETURN").items[0].expression {
        Expression::Literal(v) => v.clone(),
        other => panic!("{q}: expected a literal, got {other:?}"),
    }
}

fn kinds(q: &Query) -> Vec<&'static str> {
    q.clauses.iter().map(Clause::kind).collect()
}

// ---------------------------------------------------------------------------
// ParseError
// ---------------------------------------------------------------------------

#[test]
fn every_parse_error_carries_a_stable_code() {
    let pest = err("MATCH ((");
    assert!(matches!(pest, ParseError::PestError(_)));
    assert_eq!(pest.code(), codes::SYNTAX);
    assert!(!pest.is_semantic());
    assert!(pest
        .to_string()
        .starts_with(&format!("[{}] Parse error", codes::SYNTAX)));

    let semantic = ParseError::SemanticError("x".into());
    assert_eq!(semantic.code(), codes::SEMANTIC);
    assert!(semantic.is_semantic());
    assert_eq!(
        semantic.to_string(),
        format!("[{}] Semantic error: x", codes::SEMANTIC)
    );

    let unsupported = ParseError::UnsupportedFeature("y".into());
    assert_eq!(unsupported.code(), codes::UNSUPPORTED);
    assert_eq!(
        unsupported.to_string(),
        format!("[{}] Unsupported feature: y", codes::UNSUPPORTED)
    );

    let coded = ParseError::Coded {
        code: codes::INVALID_LITERAL,
        message: "m".into(),
    };
    assert_eq!(coded.code(), codes::INVALID_LITERAL);
    assert_eq!(coded.to_string(), format!("[{}] m", codes::INVALID_LITERAL));
}

// ---------------------------------------------------------------------------
// Clause pipeline fallback
// ---------------------------------------------------------------------------

#[test]
fn pipeline_records_every_clause_in_written_order() {
    let q = ok("CREATE (a:A) WITH a MATCH (b:B) WHERE b.x = 1 \
                UNWIND [1] AS i SET b.y = i REMOVE b.z RETURN b");
    assert!(q.needs_clause_pipeline);
    assert_eq!(
        kinds(&q),
        vec!["CREATE", "WITH", "MATCH", "WHERE", "UNWIND", "SET", "REMOVE", "RETURN"]
    );
    assert!(q.return_clause.is_some(), "the RETURN is mirrored");
}

#[test]
fn pipeline_accepts_merge_delete_optional_match_and_load_csv() {
    let q = ok("CREATE (a:A) WITH a MERGE (m:M) WITH m OPTIONAL MATCH (m)-->(x) DELETE x");
    assert!(q.needs_clause_pipeline);
    assert_eq!(
        kinds(&q),
        vec![
            "CREATE",
            "WITH",
            "MERGE",
            "WITH",
            "OPTIONAL MATCH",
            "DELETE"
        ]
    );
    let q = ok("CREATE (a) WITH a LOAD CSV WITH HEADERS FROM 'file:///x.csv' AS row RETURN row");
    assert!(kinds(&q).contains(&"LOAD CSV"));
    match q.clauses.iter().find(|c| matches!(c, Clause::LoadCsv(_))) {
        Some(Clause::LoadCsv(l)) => {
            assert_eq!(l.variable, "row");
            assert!(l.with_headers);
        }
        other => panic!("expected LOAD CSV, got {other:?}"),
    }
}

#[test]
fn pipeline_call_and_row_counts() {
    let q = ok(
        "CREATE (a) WITH a CALL db.labels() YIELD label RETURN label \
                ORDER BY label SKIP 1 LIMIT 2",
    );
    assert!(q.needs_clause_pipeline);
    assert!(kinds(&q).contains(&"CALL"));
    assert!(q.order_by.is_some());
    assert_eq!(q.skip, Some(1));
    assert_eq!(q.limit, Some(2));
}

#[test]
fn pipeline_explain_and_profile() {
    let q = ok("EXPLAIN CREATE (a) WITH a CREATE (b) RETURN b");
    assert!(q.needs_clause_pipeline);
    assert!(q.explain);
    assert!(!q.profile);
    let q = ok("PROFILE CREATE (a) WITH a CREATE (b) RETURN b");
    assert!(q.profile);
}

#[test]
fn pipeline_refuses_a_clause_it_cannot_lower() {
    let e = err("CREATE (a:A) WITH a FOREACH (i IN [1, 2] | SET a.n = i) WITH a RETURN a");
    assert!(e.is_semantic());
    assert!(
        e.to_string()
            .contains("`FOREACH` is not yet supported in this clause position"),
        "{e}"
    );
}

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

#[test]
fn integer_literals_in_every_radix() {
    assert_eq!(
        first_return_literal("RETURN 0x1A AS x"),
        PropertyValue::Integer(26)
    );
    assert_eq!(
        first_return_literal("RETURN 0X1a AS x"),
        PropertyValue::Integer(26)
    );
    assert_eq!(
        first_return_literal("RETURN 0o17 AS x"),
        PropertyValue::Integer(15)
    );
    assert_eq!(parse_integer_literal("-0x10").unwrap(), -16);
    assert_eq!(
        parse_integer_literal(" -9223372036854775808 ").unwrap(),
        i64::MIN
    );
    assert_eq!(parse_integer_literal("0O7").unwrap(), 7);
}

#[test]
fn out_of_range_integer_literals_are_refused_not_crashes() {
    for text in [
        "9223372036854775808",
        "0xFFFFFFFFFFFFFFFFFF",
        "99999999999999999999999999999999999999999",
    ] {
        match parse_integer_literal(text) {
            Err(ParseError::Coded { code, message }) => {
                assert_eq!(code, codes::INVALID_LITERAL);
                assert!(
                    message.contains("integer literal out of range"),
                    "{message}"
                );
            }
            other => panic!("{text}: expected a coded error, got {other:?}"),
        }
    }
    let e = err("RETURN 9223372036854775808 AS x");
    assert_eq!(e.code(), codes::INVALID_LITERAL);
}

#[test]
fn string_escapes_decode() {
    assert_eq!(
        first_return_literal(r#"RETURN 'a\tb\nc\rd\0e\bf\fg\\h\'i\"j' AS s"#),
        PropertyValue::String("a\tb\nc\rd\0e\u{0008}f\u{000C}g\\h'i\"j".into())
    );
    assert_eq!(
        first_return_literal(r#"RETURN "Aé" AS s"#),
        PropertyValue::String("Aé".into())
    );
    // An unknown escape keeps the character.
    assert_eq!(
        first_return_literal(r#"RETURN '\q' AS s"#),
        PropertyValue::String("q".into())
    );
}

#[test]
fn a_malformed_unicode_escape_is_an_invalid_literal() {
    for q in [r#"RETURN '\u12' AS s"#, r#"RETURN '\uZZZZ' AS s"#] {
        let e = err(q);
        assert_eq!(e.code(), codes::INVALID_LITERAL, "{q}: {e}");
        assert!(e.to_string().contains("InvalidUnicodeLiteral"), "{e}");
    }
}

// ---------------------------------------------------------------------------
// SKIP / LIMIT
// ---------------------------------------------------------------------------

#[test]
fn row_counts_may_be_constant_expressions() {
    let q = ok("MATCH (n) RETURN n LIMIT toInteger(ceil(1.7))");
    assert_eq!(q.limit, Some(2));
    let q = ok("MATCH (n) RETURN n SKIP 2.0 LIMIT 3");
    assert_eq!(q.skip, Some(2));
    assert_eq!(q.limit, Some(3));
}

#[test]
fn a_parameter_row_count_is_deferred() {
    let q = ok("MATCH (n) RETURN n SKIP $s LIMIT $l");
    assert_eq!(q.skip, None);
    assert!(matches!(q.deferred_skip, Some(Expression::Parameter(_))));
    assert!(matches!(q.deferred_limit, Some(Expression::Parameter(_))));
    // A parameter nested in an expression is deferred too.
    let q = ok("MATCH (n) RETURN n LIMIT $l + 1");
    assert!(q.deferred_limit.is_some());
}

#[test]
fn bad_row_counts_are_refused_with_the_reason() {
    let e = err("MATCH (n) RETURN n LIMIT 1.5");
    assert!(e.to_string().contains("non-negative whole number"), "{e}");
    let e = err("MATCH (n) RETURN n LIMIT n.x");
    assert!(e.to_string().contains("must not depend on the rows"), "{e}");
    let e = err("MATCH (n) RETURN n LIMIT -1");
    assert!(e.to_string().contains("LIMIT"), "{e}");
    assert!(matches!(
        parse_count_literal("-3"),
        Err(ParseError::SemanticError(m)) if m.contains("must not be negative")
    ));
}

#[test]
fn row_counts_on_return_and_with_return_statements() {
    let q = ok("RETURN 1 AS x ORDER BY x SKIP 0 LIMIT 1");
    assert_eq!((q.skip, q.limit), (Some(0), Some(1)));
    assert!(q.order_by.is_some());
    let q = ok("WITH 1 AS x RETURN x ORDER BY x SKIP 1 LIMIT 5");
    assert_eq!((q.skip, q.limit), (Some(1), Some(5)));
    assert!(q.with_clause.is_some());
    assert!(q.order_by.is_some());
}

// ---------------------------------------------------------------------------
// DDL and administrative statements
// ---------------------------------------------------------------------------

#[test]
fn show_and_analyze_statements_set_their_flags() {
    assert!(ok("SHOW INDEXES").show_indexes);
    assert!(ok("SHOW INDEX").show_indexes);
    assert!(ok("ANALYZE").analyze);
    assert!(ok("SHOW HIERARCHY INDEXES").show_hierarchy_indexes);
    assert!(ok("SHOW CONSTRAINTS").show_constraints);
}

#[test]
fn hierarchy_index_ddl() {
    let q = ok("CREATE HIERARCHY INDEX h ON ()<-[:HAS|:PART]-() MEASURE Trial.enrollment AGGREGATE sum, max");
    let c = q.create_hierarchy_index_clause.unwrap();
    assert_eq!(c.name, "h");
    assert_eq!(c.edge_types, vec!["HAS".to_string(), "PART".to_string()]);
    assert!(c.reverse);
    assert_eq!(c.measure_label.as_deref(), Some("Trial"));
    assert_eq!(c.measure_property.as_deref(), Some("enrollment"));
    assert_eq!(c.aggregates, vec!["sum".to_string(), "max".to_string()]);

    let q = ok("CREATE HIERARCHY INDEX g ON ()-[:IS_A]->()");
    let c = q.create_hierarchy_index_clause.unwrap();
    assert!(!c.reverse);
    assert_eq!(c.measure_property, None);
    assert!(c.aggregates.is_empty());

    assert_eq!(
        ok("DROP HIERARCHY INDEX h").drop_hierarchy_index.as_deref(),
        Some("h")
    );
    assert_eq!(
        ok("REBUILD HIERARCHY INDEX `h x`")
            .rebuild_hierarchy_index
            .as_deref(),
        Some("h x")
    );
}

#[test]
fn property_index_ddl() {
    let q = ok("CREATE INDEX ON :Person(name, age)");
    let c = q.create_index_clause.unwrap();
    assert_eq!(c.label.as_str(), "Person");
    assert_eq!(c.property, "name");
    assert_eq!(c.additional_properties, vec!["age".to_string()]);

    let q = ok("DROP INDEX ON :Person(name)");
    let d = q.drop_index_clause.unwrap();
    assert_eq!((d.label.as_str(), d.property.as_str()), ("Person", "name"));
}

#[test]
fn fulltext_index_ddl() {
    let q = ok("CREATE FULLTEXT INDEX docs FOR (n:Doc) ON EACH [n.title, n.body]");
    let c = q.create_fulltext_index_clause.unwrap();
    assert_eq!(c.index_name, "docs");
    assert_eq!(c.label.as_str(), "Doc");
    assert_eq!(
        c.property_keys,
        vec!["title".to_string(), "body".to_string()]
    );
    let q = ok("CREATE FULLTEXT INDEX one FOR (n:Doc) ON (n.title)");
    assert_eq!(
        q.create_fulltext_index_clause.unwrap().property_keys,
        vec!["title".to_string()]
    );
    let q = ok("DROP FULLTEXT INDEX docs");
    assert_eq!(q.drop_fulltext_index_clause.unwrap().index_name, "docs");
}

#[test]
fn constraint_ddl_in_both_spellings() {
    for q in [
        "CREATE CONSTRAINT ON (p:Person) ASSERT p.email IS UNIQUE",
        "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE",
        "CREATE CONSTRAINT uniq_email IF NOT EXISTS FOR (p:Person) REQUIRE p.email IS UNIQUE",
    ] {
        let c = ok(q)
            .create_constraint_clause
            .unwrap_or_else(|| panic!("{q}"));
        assert_eq!(c.variable, "p", "{q}");
        assert_eq!(c.label.as_str(), "Person", "{q}");
        assert_eq!(c.property, "email", "{q}");
    }
}

#[test]
fn vector_index_ddl_and_its_options() {
    let q = ok("CREATE VECTOR INDEX emb FOR (n:Doc) ON (n.vec) \
                OPTIONS {dimensions: 4, similarity: 'l2', quantization: 'fp16'}");
    let c = q.create_vector_index_clause.unwrap();
    assert_eq!(c.index_name.as_deref(), Some("emb"));
    assert_eq!(c.label.as_str(), "Doc");
    assert_eq!(c.property_key, "vec");
    assert_eq!(c.dimensions, 4);
    assert_eq!(c.similarity, "l2");
    assert_eq!(c.quantization, Quantization::Fp16);

    let q = ok("CREATE VECTOR INDEX ON :Doc(vec)");
    let c = q.create_vector_index_clause.unwrap();
    assert_eq!(c.index_name, None);
    assert_eq!(c.dimensions, 1536);
    assert_eq!(c.similarity, "cosine");
}

#[test]
fn vector_index_options_are_checked() {
    for (q, frag) in [
        (
            "CREATE VECTOR INDEX ON :Doc(vec) OPTIONS {dimension: 4}",
            "unknown option `dimension`",
        ),
        (
            "CREATE VECTOR INDEX ON :Doc(vec) OPTIONS {dimensions: 0}",
            "must be a positive integer",
        ),
        (
            "CREATE VECTOR INDEX ON :Doc(vec) OPTIONS {dimensions: 'x'}",
            "must be a positive integer",
        ),
        (
            "CREATE VECTOR INDEX ON :Doc(vec) OPTIONS {quantization: 'fp8'}",
            "unknown `quantization`",
        ),
        (
            "CREATE VECTOR INDEX ON :Doc(vec) OPTIONS {quantization: 8}",
            "`quantization` must be a string",
        ),
        (
            "CREATE VECTOR INDEX ON :Doc(vec) OPTIONS {similarity: 1}",
            "`similarity` must be a string",
        ),
    ] {
        let e = err(q);
        assert!(e.to_string().contains(frag), "{q}: {e}");
    }
}

// ---------------------------------------------------------------------------
// CALL and FOREACH statements
// ---------------------------------------------------------------------------

#[test]
fn call_with_yield_where_and_trailing_clauses() {
    let q =
        ok("CALL db.labels() YIELD label AS l WHERE l <> 'X' RETURN l ORDER BY l SKIP 1 LIMIT 3");
    let call = q.call_clause.as_ref().unwrap();
    assert_eq!(call.procedure_name, "db.labels");
    assert_eq!(call.yield_items[0].name, "label");
    assert_eq!(call.yield_items[0].alias.as_deref(), Some("l"));
    assert!(q.where_clause.is_some());
    assert!(q.order_by.is_some());
    assert_eq!((q.skip, q.limit), (Some(1), Some(3)));
}

#[test]
fn call_followed_by_a_match() {
    let q = ok("CALL db.labels() YIELD label MATCH (n) WHERE n.x = label RETURN n");
    assert_eq!(q.match_clauses.len(), 1);
    assert!(q.where_clause.is_some());
    assert_eq!(q.call_clause.unwrap().yield_items[0].alias, None);
}

#[test]
fn call_subquery_parses_its_body() {
    let q = ok("CALL { MATCH (n) RETURN n } RETURN n");
    let body = q.call_subquery.expect("a subquery");
    assert_eq!(body.match_clauses.len(), 1);
    assert!(body.return_clause.is_some());
}

#[test]
fn a_leading_foreach_is_the_whole_statement() {
    let q = ok("FOREACH (i IN [1, 2] | CREATE (:N {v: i}))");
    let f = q.foreach_clause.expect("FOREACH");
    assert_eq!(f.variable, "i");
    assert_eq!(f.body.len(), 1);
}

// ---------------------------------------------------------------------------
// Match-statement clause shapes
// ---------------------------------------------------------------------------

#[test]
fn each_with_stage_keeps_its_own_unwind() {
    let q = ok("MATCH (a) WITH a UNWIND [1] AS x WITH a, x UNWIND [2] AS y RETURN a, x, y");
    assert_eq!(q.extra_with_stages.len(), 1);
    let (_, unwind, _, _) = &q.extra_with_stages[0];
    assert_eq!(unwind.as_ref().map(|u| u.variable.as_str()), Some("x"));
    assert_eq!(q.post_with_unwind_clauses.len(), 1);
    assert_eq!(q.post_with_unwind_clauses[0].variable, "y");
}

#[test]
fn repeated_leading_unwinds_queue_behind_the_first() {
    let q = ok("UNWIND [1] AS x UNWIND [2] AS y RETURN x, y");
    assert_eq!(q.unwind_clause.as_ref().unwrap().variable, "x");
    assert_eq!(q.extra_unwind_clauses.len(), 1);
    assert_eq!(q.extra_unwind_clauses[0].variable, "y");
    assert!(q.unwind_leading);
}

#[test]
fn correlated_call_records_its_imports() {
    let q = ok("MATCH (a) CALL { WITH a RETURN a.x AS y } RETURN y");
    let c = q.correlated_call.expect("correlated CALL");
    assert_eq!(c.imports, Some(vec!["a".to_string()]));
    assert!(c.body.return_clause.is_some());

    let q = ok("MATCH (a) CALL { WITH * RETURN a.x AS y } RETURN y");
    assert_eq!(q.correlated_call.unwrap().imports, None);
}

#[test]
fn match_then_create_and_detach_delete() {
    let q = ok("MATCH (a) CREATE (a)-[:T]->(b:B)");
    assert_eq!(q.create_clause.unwrap().pattern.paths.len(), 1);
    let q = ok("MATCH (a) DETACH DELETE a");
    assert!(q.delete_clause.unwrap().detach);
    let q = ok("MATCH (a) DELETE a");
    assert!(!q.delete_clause.unwrap().detach);
}

#[test]
fn create_statement_with_row_counts() {
    let q = ok("CREATE (n:P) RETURN n ORDER BY n SKIP 0 LIMIT 1");
    assert!(q.create_clause.is_some());
    assert!(q.order_by.is_some());
    assert_eq!((q.skip, q.limit), (Some(0), Some(1)));
}

#[test]
fn with_clause_carries_its_own_filter_and_row_counts() {
    let q = ok("MATCH (n) WITH DISTINCT n ORDER BY n.x SKIP 1 LIMIT 2 WHERE n.x > 1 RETURN n");
    let w = q.with_clause.unwrap();
    assert!(w.distinct);
    assert!(w.where_clause.is_some());
    assert!(w.order_by.is_some());
    assert_eq!((w.skip, w.limit), (Some(1), Some(2)));
}

#[test]
fn set_items_of_every_form() {
    let q = ok("MATCH (n) SET n.a = 1, n = {b: 2}, n += {c: 3}, n:L1:L2");
    let s = &q.set_clauses[0];
    assert_eq!(s.items.len(), 1);
    assert_eq!(s.entity_items.len(), 2);
    assert!(!s.entity_items[0].merge);
    assert!(s.entity_items[1].merge);
    assert_eq!(s.label_items.len(), 1);
    assert_eq!(s.label_items[0].labels.len(), 2);
}

#[test]
fn merge_statement_with_every_action() {
    let q = ok("MERGE (n:P {k: 1}) \
                ON CREATE SET n.a = 1, n += {b: 2}, n:New \
                ON MATCH SET n.c = 3, n = {k: 1}, n:Seen \
                SET n.d = 4 REMOVE n.e RETURN n");
    let m = q.merge_clause.as_ref().unwrap();
    assert_eq!(m.on_create_set.len(), 1);
    assert_eq!(m.on_create_entity_set.len(), 1);
    assert_eq!(m.on_create_labels.len(), 1);
    assert_eq!(m.on_match_set.len(), 1);
    assert_eq!(m.on_match_entity_set.len(), 1);
    assert_eq!(m.on_match_labels.len(), 1);
    assert_eq!(q.set_clauses.len(), 1);
    assert_eq!(q.remove_clauses.len(), 1);
    assert!(q.return_clause.is_some());
}

#[test]
fn inline_merge_with_every_action() {
    let q = ok("MATCH (a) MERGE (a)-[:T]->(b:B) \
                ON CREATE SET b.x = 1, b += {y: 2}, b:C \
                ON MATCH SET b.z = 1, b = {z: 2}, b:D RETURN b");
    let m = q.merge_clause.as_ref().unwrap();
    assert_eq!(m.on_create_set.len(), 1);
    assert_eq!(m.on_create_entity_set.len(), 1);
    assert_eq!(m.on_create_labels.len(), 1);
    assert_eq!(m.on_match_set.len(), 1);
    assert_eq!(m.on_match_entity_set.len(), 1);
    assert_eq!(m.on_match_labels.len(), 1);
}

#[test]
fn load_csv_field_terminator() {
    let q = ok("LOAD CSV with headers FROM 'file:///x.csv' AS row FIELDTERMINATOR ';' RETURN row");
    let l = q.load_csv_clause.unwrap();
    assert!(l.with_headers);
    assert_eq!(l.field_terminator, Some(';'));
    assert_eq!(l.variable, "row");
    let q = ok("LOAD CSV FROM 'file:///x.csv' AS row RETURN row");
    let l = q.load_csv_clause.unwrap();
    assert!(!l.with_headers);
    assert_eq!(l.field_terminator, None);
    let e = err("LOAD CSV FROM 'file:///x.csv' AS row FIELDTERMINATOR 'ab' RETURN row");
    assert!(
        e.to_string()
            .contains("FIELDTERMINATOR must be a single character"),
        "{e}"
    );
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

/// The expression of the only RETURN item, after a leading MATCH/WITH `prefix`.
fn ret_expr(q: &str) -> Expression {
    ok(q).return_clause.expect("RETURN").items[0]
        .expression
        .clone()
}

fn bin(e: &Expression) -> (&Expression, &BinaryOp, &Expression) {
    match e {
        Expression::Binary { left, op, right } => (left, op, right),
        other => panic!("expected a binary expression, got {other:?}"),
    }
}

#[test]
fn a_chained_comparison_expands_to_a_conjunction() {
    let q = ok("MATCH (n) WHERE 1 < n.x <= 3 RETURN n");
    let pred = q.where_clause.unwrap().predicate;
    let (l, op, r) = bin(&pred);
    assert_eq!(*op, BinaryOp::And);
    assert_eq!(*bin(l).1, BinaryOp::Lt);
    assert_eq!(*bin(r).1, BinaryOp::Le);
    // The middle operand appears on both sides.
    assert_eq!(bin(l).2, bin(r).0);
}

#[test]
fn a_chained_comparison_mixed_with_other_operators_is_refused() {
    let e = err("MATCH (n) WHERE 1 < n.x < 3 AND true RETURN n");
    assert!(e.to_string().contains("chained comparison"), "{e}");
}

#[test]
fn every_operator_spelling_maps_to_its_op() {
    for (src, op) in [
        ("1 = 2", BinaryOp::Eq),
        ("1 <> 2", BinaryOp::Ne),
        ("1 != 2", BinaryOp::Ne),
        ("1 < 2", BinaryOp::Lt),
        ("1 <= 2", BinaryOp::Le),
        ("1 > 2", BinaryOp::Gt),
        ("1 >= 2", BinaryOp::Ge),
        ("1 + 2", BinaryOp::Add),
        ("1 - 2", BinaryOp::Sub),
        ("1 * 2", BinaryOp::Mul),
        ("1 / 2", BinaryOp::Div),
        ("1 % 2", BinaryOp::Mod),
        ("1 ^ 2", BinaryOp::Pow),
        ("'a' STARTS WITH 'b'", BinaryOp::StartsWith),
        ("'a' ends with 'b'", BinaryOp::EndsWith),
        ("'a' CONTAINS 'b'", BinaryOp::Contains),
        ("1 IN [1]", BinaryOp::In),
        ("'a' =~ 'b'", BinaryOp::RegexMatch),
        ("true XOR false", BinaryOp::Xor),
        ("true OR false", BinaryOp::Or),
    ] {
        let e = ret_expr(&format!("RETURN {src} AS x"));
        assert_eq!(*bin(&e).1, op, "{src}");
    }
    assert!(matches!(
        parse_op_str("<=>"),
        Err(ParseError::SemanticError(_))
    ));
}

#[test]
fn negated_integer_literals_fold() {
    assert_eq!(
        ret_expr("RETURN -0x10 AS x"),
        Expression::Literal(PropertyValue::Integer(-16))
    );
    assert_eq!(
        ret_expr("RETURN -9223372036854775808 AS x"),
        Expression::Literal(PropertyValue::Integer(i64::MIN))
    );
    // A negated non-literal stays a unary minus.
    assert!(matches!(
        ret_expr("WITH 1 AS y RETURN -y AS x"),
        Expression::Unary {
            op: UnaryOp::Minus,
            ..
        }
    ));
}

#[test]
fn subscripts_slices_and_member_access() {
    assert!(matches!(
        ret_expr("WITH [1, 2, 3] AS xs RETURN xs[1..] AS x"),
        Expression::ListSlice {
            start: Some(_),
            end: None,
            ..
        }
    ));
    assert!(matches!(
        ret_expr("WITH [1, 2, 3] AS xs RETURN xs[..2] AS x"),
        Expression::ListSlice {
            start: None,
            end: Some(_),
            ..
        }
    ));
    assert!(matches!(
        ret_expr("WITH [[1]] AS xs RETURN xs[0][0] AS x"),
        Expression::Index { .. }
    ));
    match ret_expr("WITH {a: {b: 1}} AS m RETURN head([m]).a AS x") {
        Expression::Index { index, .. } => assert_eq!(
            *index,
            Expression::Literal(PropertyValue::String("a".into()))
        ),
        other => panic!("expected member access, got {other:?}"),
    }
    match ret_expr("MATCH (d) RETURN d.meta.a.b AS x") {
        Expression::Index { expr, .. } => assert!(matches!(*expr, Expression::Index { .. })),
        other => panic!("expected nested access, got {other:?}"),
    }
}

#[test]
fn postfix_label_checks_and_null_tests() {
    match ret_expr("MATCH (n) RETURN n:A:B AS x") {
        Expression::Function { name, args, .. } => {
            assert_eq!(name, "hasLabels");
            assert_eq!(
                args[1],
                Expression::Literal(PropertyValue::Array(vec![
                    PropertyValue::String("A".into()),
                    PropertyValue::String("B".into())
                ]))
            );
        }
        other => panic!("expected hasLabels, got {other:?}"),
    }
    assert!(matches!(
        ret_expr("MATCH (n) RETURN n.x IS NOT NULL AS x"),
        Expression::Unary {
            op: UnaryOp::IsNotNull,
            ..
        }
    ));
    assert!(matches!(
        ret_expr("MATCH (n) RETURN n.x IS NULL AS x"),
        Expression::Unary {
            op: UnaryOp::IsNull,
            ..
        }
    ));
    assert!(matches!(
        ret_expr("RETURN NOT NOT true AS x"),
        Expression::Unary {
            op: UnaryOp::Not,
            ..
        }
    ));
}

#[test]
fn map_projections() {
    match ret_expr("MATCH (n) WITH n, 1 AS other RETURN n {.name, k: 1, other} AS m") {
        Expression::Case {
            else_result: Some(body),
            when_clauses,
            ..
        } => {
            assert_eq!(when_clauses.len(), 1);
            match *body {
                Expression::MapExpr(entries) => {
                    let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
                    assert_eq!(keys, vec!["name", "k", "other"]);
                }
                other => panic!("expected a map, got {other:?}"),
            }
        }
        other => panic!("expected a null-guarded map, got {other:?}"),
    }
    match ret_expr("MATCH (n) RETURN n {.*} AS m") {
        Expression::Case {
            else_result: Some(body),
            ..
        } => {
            assert!(matches!(*body, Expression::Function { ref name, .. } if name == "properties"));
        }
        other => panic!("expected properties(n), got {other:?}"),
    }
    let e = err("MATCH (n) RETURN n {.*, .name} AS m");
    assert!(matches!(e, ParseError::UnsupportedFeature(_)), "{e}");
}

#[test]
fn map_literals_with_expression_values_and_quoted_keys() {
    match ret_expr("WITH 1 AS x RETURN {a: x, 'b c': x + 1} AS m") {
        Expression::MapExpr(entries) => {
            let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(keys, vec!["a", "b c"]);
        }
        other => panic!("expected a map expression, got {other:?}"),
    }
}

#[test]
fn count_and_exists_subqueries() {
    let q = ok("MATCH (a) RETURN COUNT { (a)-->(b) WHERE b.x > 1 } AS c");
    match &q.return_clause.unwrap().items[0].expression {
        Expression::ExistsSubquery {
            count,
            where_clause,
            ..
        } => {
            assert!(*count);
            assert!(where_clause.is_some());
        }
        other => panic!("expected COUNT {{}}, got {other:?}"),
    }
    let q = ok("MATCH (a) WHERE EXISTS { MATCH (a)-->(b) RETURN b } RETURN a");
    match q.where_clause.unwrap().predicate {
        Expression::ExistsSubquery { bare_pattern, .. } => {
            assert!(!bare_pattern);
        }
        other => panic!("expected EXISTS {{}}, got {other:?}"),
    }
    let e = err("MATCH (a) WHERE EXISTS { RETURN 1 } RETURN a");
    assert!(e.to_string().contains("needs a MATCH"), "{e}");
    // `exists(pattern)` is the explicit existence test, which may be projected.
    match ret_expr("MATCH (n) RETURN exists((n)-->()) AS e") {
        Expression::ExistsSubquery { bare_pattern, .. } => assert!(!bare_pattern),
        other => panic!("expected an existence test, got {other:?}"),
    }
}

#[test]
fn list_comprehension_forms() {
    match ret_expr("RETURN [x IN [1, 2] WHERE x > 1] AS l") {
        Expression::ListComprehension {
            filter, map_expr, ..
        } => {
            assert!(filter.is_some());
            assert_eq!(*map_expr, Expression::Variable("x".into()));
        }
        other => panic!("{other:?}"),
    }
    match ret_expr("RETURN [x IN [1, 2] | x * 2] AS l") {
        Expression::ListComprehension {
            filter, map_expr, ..
        } => {
            assert!(filter.is_none());
            assert!(matches!(*map_expr, Expression::Binary { .. }));
        }
        other => panic!("{other:?}"),
    }
    match ret_expr("RETURN [x IN [1, 2] WHERE x > 1 | x * 2] AS l") {
        Expression::ListComprehension {
            filter, map_expr, ..
        } => {
            assert!(filter.is_some());
            assert!(matches!(*map_expr, Expression::Binary { .. }));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn predicate_functions_reduce_and_pattern_comprehensions() {
    match ret_expr("RETURN any(x IN [1, 2] WHERE x > 1) AS b") {
        Expression::PredicateFunction { name, variable, .. } => {
            assert_eq!(name, "any");
            assert_eq!(variable, "x");
        }
        other => panic!("{other:?}"),
    }
    match ret_expr("RETURN reduce(acc = 0, x IN [1, 2] | acc + x) AS s") {
        Expression::Reduce {
            accumulator,
            variable,
            ..
        } => {
            assert_eq!(accumulator, "acc");
            assert_eq!(variable, "x");
        }
        other => panic!("{other:?}"),
    }
    match ret_expr("MATCH (a) RETURN [p = (a)-->(b) WHERE b.x > 0 | p] AS ps") {
        Expression::PatternComprehension {
            pattern, filter, ..
        } => {
            assert_eq!(pattern.paths[0].path_variable.as_deref(), Some("p"));
            assert!(filter.is_some());
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn foreach_bodies_of_every_kind() {
    let q = ok("MATCH (n) WITH collect(n) AS ns \
                FOREACH (x IN ns | SET x.a = 1 REMOVE x.b MERGE (:M {v: 1}) \
                FOREACH (y IN [1] | CREATE (:C)) DETACH DELETE x)");
    let f = q.foreach_clause.expect("FOREACH");
    use crate::query::ast::ForeachBody;
    let kinds: Vec<&str> = f
        .body
        .iter()
        .map(|b| match b {
            ForeachBody::Set(_) => "set",
            ForeachBody::Remove(_) => "remove",
            ForeachBody::Delete(_) => "delete",
            ForeachBody::Create(_) => "create",
            ForeachBody::Merge(_) => "merge",
            ForeachBody::Foreach(_) => "foreach",
        })
        .collect();
    assert_eq!(kinds, vec!["set", "remove", "merge", "foreach", "delete"]);
}

#[test]
fn backticked_variables_and_properties_are_unescaped() {
    match ret_expr("MATCH (`my node`) RETURN `my node`.`the name` AS x") {
        Expression::Property { variable, property } => {
            assert_eq!(variable, "my node");
            assert_eq!(property, "the name");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_optional_match_where_is_split_into_its_conjuncts() {
    let q = ok("MATCH (a) OPTIONAL MATCH (a)-->(b) WHERE b.x = 1 AND b.y = 2 RETURN a, b");
    assert_eq!(q.optional_where.len(), 2);
    assert!(q.match_clauses[1].optional);
}

#[test]
fn a_single_quoted_unicode_escape_decodes() {
    assert_eq!(
        first_return_literal(r"RETURN 'A' AS s"),
        PropertyValue::String("A".into())
    );
}

#[test]
fn a_query_nested_past_the_limit_is_refused_before_parsing() {
    let depth = MAX_NESTING_DEPTH + 8;
    let q = format!("RETURN {}1{} AS x", "(".repeat(depth), ")".repeat(depth));
    let e = err(&q);
    assert_eq!(e.code(), codes::SYNTAX);
    assert!(
        e.to_string()
            .contains(&format!("query nests brackets {depth} deep")),
        "{e}"
    );
}
