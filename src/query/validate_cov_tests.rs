//! Additional unit tests for semantic validation: every error's code and
//! message, and the query shapes that raise each of them.

use super::*;
use crate::query::error_code as c;
use crate::query::parser::{parse_query, ParseError};

/// Every variant, the code it must carry and a fragment of its message.
fn catalogue() -> Vec<(ValidationError, &'static str, &'static str)> {
    use ValidationError as V;
    vec![
        (
            V::DuplicateColumn("a".into()),
            c::CLAUSE_CONFLICT,
            "same name are not supported: `a`",
        ),
        (
            V::UnionColumnMismatch {
                left: vec!["a".into()],
                right: vec!["b".into()],
            },
            c::CLAUSE_CONFLICT,
            "[\"a\"] vs [\"b\"]",
        ),
        (
            V::MixedUnionAndUnionAll,
            c::CLAUSE_CONFLICT,
            "Cannot mix UNION and UNION ALL",
        ),
        (
            V::CreateOnBoundVariable("n".into()),
            c::INVALID_WRITE_PATTERN,
            "Variable `n` already declared",
        ),
        (
            V::CreateRelationshipWithoutType,
            c::INVALID_WRITE_PATTERN,
            "Exactly one relationship type",
        ),
        (
            V::CreateUndirectedRelationship,
            c::INVALID_WRITE_PATTERN,
            "Only directed relationships",
        ),
        (
            V::CreateVariableLengthRelationship,
            c::INVALID_WRITE_PATTERN,
            "cannot be created",
        ),
        (
            V::UnboundedWalk,
            c::INVALID_PATTERN,
            "an unbounded WALK under ALL",
        ),
        (
            V::CreateOnBoundRelationship("r".into()),
            c::INVALID_WRITE_PATTERN,
            "cannot rebind a relationship",
        ),
        (
            V::MergeRelationshipWithoutType,
            c::INVALID_WRITE_PATTERN,
            "MERGE requires exactly one relationship type",
        ),
        (
            V::MergeVariableLengthRelationship,
            c::INVALID_WRITE_PATTERN,
            "variable-length relationship",
        ),
        (
            V::MergeOnBoundVariable("n".into()),
            c::INVALID_WRITE_PATTERN,
            "on `n`, which is already",
        ),
        (
            V::MergeRelationshipWithNullProperty("p".into()),
            c::INVALID_WRITE_PATTERN,
            "null property (`p`)",
        ),
        (
            V::VariableTypeConflict("xs".into()),
            c::VARIABLE_KIND,
            "`xs` is bound to a collection",
        ),
        (
            V::NonBooleanOperand("an integer"),
            c::TYPE_MISMATCH,
            "this one is an integer",
        ),
        (
            V::PropertyAccessOnNonMap {
                name: "x".into(),
                what: "an integer",
            },
            c::VARIABLE_KIND,
            "`x` is an integer, so it has no properties",
        ),
        (
            V::UnboundPatternVariable("b".into()),
            c::VARIABLE_NOT_BOUND,
            "`b` is introduced by a pattern",
        ),
        (
            V::VariableKindConflict {
                name: "a".into(),
                first: "a node",
                second: "a relationship",
            },
            c::VARIABLE_KIND,
            "`a` is bound to a node and then to a relationship",
        ),
        (V::PatternInSetValue, c::CLAUSE_CONFLICT, "right of SET"),
        (
            V::SizeOfNonCollection("a path"),
            c::VARIABLE_KIND,
            "not a path; use length()",
        ),
        (
            V::PatternInProjection("RETURN"),
            c::CLAUSE_CONFLICT,
            "cannot be projected by RETURN",
        ),
        (
            V::ExistsSubqueryPosition,
            c::CLAUSE_CONFLICT,
            "EXISTS { } subquery with a full body",
        ),
        (
            V::InvalidDeleteTarget("got 1".into()),
            c::VARIABLE_KIND,
            "DELETE takes a node, relationship or path; got 1",
        ),
        (
            V::OrderByUndefinedVariable("n".into()),
            c::VARIABLE_NOT_BOUND,
            "`n` is not available to ORDER BY",
        ),
        (
            V::UnboundVariable("m".into()),
            c::VARIABLE_NOT_BOUND,
            "`m` is not bound",
        ),
        (V::UnaliasedWithItem, c::CLAUSE_CONFLICT, "needs an alias"),
        (
            V::UnknownFunction("nope".into()),
            c::UNKNOWN_FUNCTION,
            "UnknownFunction: `nope`",
        ),
        (
            V::MergeNullProperty("k".into()),
            c::INVALID_WRITE_PATTERN,
            "`k: null` matches nothing",
        ),
        (
            V::InvalidPredicatePattern,
            c::CLAUSE_CONFLICT,
            "a bare node pattern is not a predicate",
        ),
        (
            V::PropertyOnPath("p".into()),
            c::VARIABLE_KIND,
            "`p` is a path",
        ),
        (
            V::NoVariablesInScope,
            c::VARIABLE_NOT_BOUND,
            "NoVariablesInScope",
        ),
        (
            V::RelationshipUniquenessViolation("r".into()),
            c::CLAUSE_CONFLICT,
            "`r` appears twice",
        ),
        (
            V::AmbiguousGroupingExpression("`n`".into()),
            c::AGGREGATE_MISUSE,
            "`n` appears inside an expression",
        ),
        (
            V::AggregateNotAllowed("in WHERE"),
            c::AGGREGATE_MISUSE,
            "not allowed in WHERE",
        ),
        (
            V::FunctionArgumentKind("labels", "a node", "a path"),
            c::BAD_ARGUMENT,
            "`labels()` takes a node, and was given a path",
        ),
        (
            V::AmbiguousAggregationExpression,
            c::AGGREGATE_MISUSE,
            "ambiguous whether the expression",
        ),
        (
            V::InvalidAggregation,
            c::AGGREGATE_MISUSE,
            "ORDER BY introduces an aggregate",
        ),
    ]
}

#[test]
fn every_validation_error_has_a_code_and_explains_itself() {
    for (err, code, fragment) in catalogue() {
        assert_eq!(err.code(), code, "{err:?}");
        let text = err.to_string();
        assert!(text.contains(fragment), "{err:?}: {text}");
    }
}

/// Parse `q` and return the code and message of the rejection.
fn rejected(q: &str) -> (String, String) {
    match parse_query(q) {
        Ok(_) => panic!("expected {q:?} to be rejected"),
        Err(ParseError::Coded { code, message }) => (code.to_string(), message),
        Err(other) => (other.code().to_string(), other.to_string()),
    }
}

macro_rules! rejects {
    ($($name:ident: $q:expr => $code:expr, $frag:expr;)*) => {
        $(
            #[test]
            fn $name() {
                let (code, msg) = rejected($q);
                assert!(msg.contains($frag), "{}: {msg}", $q);
                assert_eq!(code, $code, "{}: {msg}", $q);
            }
        )*
    };
}

rejects! {
    duplicate_columns: "RETURN 1 AS a, 2 AS a" => c::CLAUSE_CONFLICT, "`a`";
    union_columns_differ: "RETURN 1 AS a UNION RETURN 2 AS b" => c::CLAUSE_CONFLICT, "same column names";
    union_and_union_all_mixed: "RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a"
        => c::CLAUSE_CONFLICT, "Cannot mix";
    create_on_bound_node: "MATCH (n) CREATE (n:Extra)" => c::INVALID_WRITE_PATTERN, "`n` already declared";
    create_untyped_relationship: "CREATE (a)-[:T]->(b), (a)-[]->(b)"
        => c::INVALID_WRITE_PATTERN, "Exactly one relationship type";
    create_undirected_relationship: "CREATE (a)-[:T]-(b)" => c::INVALID_WRITE_PATTERN, "directed";
    create_varlength_relationship: "CREATE (a)-[:T*2]->(b)" => c::INVALID_WRITE_PATTERN, "cannot be created";
    create_on_bound_relationship: "MATCH ()-[r:T]->() CREATE ()-[r:T]->()"
        => c::INVALID_WRITE_PATTERN, "`r` already declared";
    merge_untyped_relationship: "MERGE (a)-[]->(b)" => c::INVALID_WRITE_PATTERN, "MERGE requires exactly one";
    merge_varlength_relationship: "MERGE (a)-[:T*2]->(b)" => c::INVALID_WRITE_PATTERN, "variable-length";
    merge_relationship_null_property: "MERGE (a)-[:T {w: null}]->(b)"
        => c::INVALID_WRITE_PATTERN, "`w: null` matches nothing";
    merge_node_null_property: "MERGE (n:P {k: null})" => c::INVALID_WRITE_PATTERN, "`k: null`";
    collection_used_as_node: "WITH [1] AS xs MATCH (xs)-->() RETURN xs" => c::VARIABLE_KIND, "collection";
    non_boolean_and_operand: "RETURN 1 AND true AS x" => c::TYPE_MISMATCH, "boolean operands";
    property_on_integer: "WITH 1 AS x RETURN x.prop AS p" => c::VARIABLE_KIND, "no properties";
    node_then_relationship: "MATCH (a)-[a]->() RETURN a" => c::VARIABLE_KIND, "`a` is bound to";
    pattern_as_set_value: "MATCH (a) SET a.x = (a)-->()" => c::CLAUSE_CONFLICT, "SET";
    pattern_in_return: "MATCH (a) RETURN (a)-->() AS p" => c::CLAUSE_CONFLICT, "predicate, not a value";
    unaliased_with_expression: "MATCH (n) WITH n.x RETURN 1 AS one" => c::CLAUSE_CONFLICT, "alias";
    unknown_function: "RETURN nosuchfn(1) AS x" => c::UNKNOWN_FUNCTION, "`nosuchfn`";
    bare_node_predicate: "MATCH (n) WHERE (n) RETURN n" => c::CLAUSE_CONFLICT, "bare node pattern";
    property_on_path: "MATCH p = ()-->() RETURN p.x AS x" => c::VARIABLE_KIND, "is a path";
    star_over_nothing: "MATCH () RETURN *" => c::VARIABLE_NOT_BOUND, "NoVariablesInScope";
    one_relationship_twice: "MATCH (a)-[r]->(b)-[r]->(c) RETURN a" => c::CLAUSE_CONFLICT, "`r` appears twice";
    aggregate_in_where: "MATCH (n) WHERE count(n) > 1 RETURN n" => c::AGGREGATE_MISUSE, "not allowed";
    labels_of_a_path: "MATCH p = ()-->() RETURN labels(p) AS l" => c::BAD_ARGUMENT, "`labels()` takes a node";
    type_of_a_node: "MATCH (n) RETURN type(n) AS t" => c::BAD_ARGUMENT, "`type()` takes a relationship";
    length_of_a_node: "MATCH (n) RETURN length(n) AS l" => c::BAD_ARGUMENT, "`length()` takes a path";
    size_of_a_path: "MATCH p = ()-->() RETURN size(p) AS s" => c::VARIABLE_KIND, "size()";
    order_by_aggregate_not_projected: "MATCH (n) RETURN n ORDER BY count(n)"
        => c::AGGREGATE_MISUSE, "ORDER BY introduces an aggregate";
    order_by_hidden_variable_after_aggregation: "MATCH (n) RETURN count(n) AS c ORDER BY n.x"
        => c::VARIABLE_NOT_BOUND, "`n` is not available to ORDER BY";
    order_by_hidden_variable_after_distinct: "MATCH (n), (m) RETURN DISTINCT n.x AS x ORDER BY m.y"
        => c::VARIABLE_NOT_BOUND, "`m` is not available to ORDER BY";
    order_by_grouped_variable_mixed_with_aggregate:
        "MATCH (n) RETURN n.x AS k, count(*) AS c ORDER BY n.y + count(*)"
        => c::AGGREGATE_MISUSE, "ambiguous";
    unbound_variable: "MATCH (n) RETURN m" => c::VARIABLE_NOT_BOUND, "`m` is not bound";
    unbound_in_where: "MATCH (n) WHERE m.x = 1 RETURN n" => c::VARIABLE_NOT_BOUND, "`m`";
    unbounded_walk: "MATCH WALK (a)-[*]->(b) RETURN a" => c::INVALID_PATTERN, "unbounded WALK";
}

/// Queries that are fine and must stay fine: the other side of each check.
#[test]
fn well_formed_queries_pass_validation() {
    for q in [
        "MATCH WALK (a)-[*1..3]->(b) RETURN a",
        "MATCH (n) RETURN count(n) AS c ORDER BY c",
        "MATCH (n) RETURN n.x AS x, count(*) AS c ORDER BY x, c",
        "MATCH (n) RETURN DISTINCT n.x AS x ORDER BY x",
        "MATCH (n) RETURN n ORDER BY n.x",
        "MATCH (n) WITH n.x AS x RETURN x",
        "MATCH p = ()-->() RETURN length(p) AS l, nodes(p) AS ns",
        "MATCH ()-[r]->() RETURN type(r) AS t",
        "MATCH (n) RETURN labels(n) AS l",
        "MERGE (a:A {k: 1})-[:T]->(b:B)",
        "UNWIND [1, 2] AS x RETURN x UNION RETURN 3 AS x",
        "MATCH (a) WHERE (a)-->() RETURN a",
        "WITH {k: 1} AS m RETURN m.k AS k",
        // Binders in every shape the scope analysis walks.
        "CREATE (a) WITH a LOAD CSV FROM 'file:///x.csv' AS row RETURN row",
        "CREATE (a) WITH a CALL db.labels() YIELD label AS l RETURN l",
        "CREATE (a) WITH a CALL db.labels() YIELD label RETURN label",
        "CREATE (a) WITH a MERGE (b:B) WITH a, b UNWIND [1] AS x RETURN a, b, x",
        "MATCH (a) CALL { WITH a RETURN a.x AS y } RETURN y",
        "MATCH (a) CALL { WITH a MATCH (a)-->(b) RETURN b } RETURN b",
        "CALL { WITH 1 AS x RETURN x } RETURN x",
        "CALL { UNWIND [1] AS x RETURN x } RETURN x",
        "MATCH (n) WITH collect(n) AS ns FOREACH (m IN ns | SET m.v = 1)",
        "MATCH (a)-[r]->(b) WITH a, r MATCH (a)-[s]->(c) WHERE s <> r RETURN c",
    ] {
        assert!(parse_query(q).is_ok(), "{q}: {:?}", parse_query(q).err());
    }
}
