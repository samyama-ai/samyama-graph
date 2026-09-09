//! Catalog parameters are bound, typed and used (#1156).
//!
//! The risk is that a "templatized, parameterized query" is filled by string
//! substitution, which makes a published `.sgqueries` file a Cypher-injection
//! surface that ships with the data. The parser has had real parameters all
//! along (`$name`), so the safe form was always available and the unsafe one is
//! merely easier to write.
//!
//! Two of the risks the issue lists turned out to be unreachable in this
//! engine, and that is recorded here as a test rather than as a claim:
//! `LIMIT $k` and `*1..$n` are both refused, the first by a semantic check and
//! the second by the grammar.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::snapshot::verify::{
    build_catalog, referenced_params, validate_entry_shape, verify, ParamSpec, QuerySpec,
};
use serde_json::json;

fn seeded() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..10i64 {
        let mut p = samyama::graph::PropertyMap::new();
        p.insert("age".into(), PropertyValue::Integer(20 + i));
        p.insert("name".into(), PropertyValue::String(format!("p{i}")));
        store.create_node_with_properties("default", vec![Label::new("P")], p);
    }
    store
}

fn param(name: &str, kind: &str, sample: serde_json::Value) -> ParamSpec {
    ParamSpec { name: name.into(), kind: kind.into(), sample, enum_values: None }
}

#[test]
fn a_bound_parameter_round_trips_through_build_and_verify() {
    let store = seeded();
    let q = vec![QuerySpec {
        id: "by_age".into(),
        cypher: "MATCH (n:P) WHERE n.age = $age RETURN n.name".into(),
        unanswerable: false,
        params: vec![param("age", "int", json!(25))],
    }];
    let catalog = build_catalog(&store, &q, &[]).expect("build");
    assert_eq!(catalog.entries[0].rows, 1);
    assert!(verify(&store, &catalog).expect("verify").is_ok());
}

/// The reachable type hazard, confirmed by measurement: a string parameter
/// compared to an integer property returns zero rows and **no error**. The
/// caller reads "no results" where the truth is "wrong type".
///
/// The zero-row rule catches it at build time, which is the point of having
/// that rule apply to parameterized entries too.
#[test]
fn a_type_mismatch_is_refused_at_build_rather_than_returning_nothing() {
    let store = seeded();
    let q = vec![QuerySpec {
        id: "by_age".into(),
        cypher: "MATCH (n:P) WHERE n.age = $age RETURN n.name".into(),
        unanswerable: false,
        // "25" as a string against an integer property.
        params: vec![param("age", "string", json!("25"))],
    }];
    let err = build_catalog(&store, &q, &[]).expect_err("a silent zero-row match must be refused");
    assert!(err.contains("0 rows"), "{err}");
    assert!(err.contains("type mismatch"), "the error should name the likely cause: {err}");
}

/// Numeric coercion does work, so the issue's stated worst case -- a float
/// where the property holds an int -- is not silent through the bound path.
#[test]
fn a_float_parameter_matches_an_integer_property() {
    let store = seeded();
    let q = vec![QuerySpec {
        id: "by_age".into(),
        cypher: "MATCH (n:P) WHERE n.age = $age RETURN n.name".into(),
        unanswerable: false,
        params: vec![param("age", "float", json!(25.0))],
    }];
    let catalog = build_catalog(&store, &q, &[]).expect("float should coerce to int");
    assert_eq!(catalog.entries[0].rows, 1);
}

#[test]
fn an_interpolated_template_is_refused() {
    for cypher in [
        "MATCH (n:P) WHERE n.name = '{{name}}' RETURN n",
        "MATCH (n:P) WHERE n.name = '${name}' RETURN n",
        "MATCH (n:P) WHERE n.age = %d RETURN n",
        "MATCH (n:P) WHERE n.name = '#{name}' RETURN n",
    ] {
        let err = validate_entry_shape("t", cypher, &[]).expect_err(cypher);
        assert!(err.contains("injection"), "{err}");
    }
}

/// A map literal is not interpolation, or every ordinary query would be refused.
#[test]
fn a_map_literal_is_not_mistaken_for_a_template() {
    validate_entry_shape("t", "MATCH (n:P {name: 'p1'}) RETURN n", &[]).expect("map literal");
    validate_entry_shape("t", "MATCH (n:P) RETURN {a: n.name}", &[]).expect("map projection");
}

#[test]
fn an_undeclared_parameter_is_refused() {
    let err = validate_entry_shape("t", "MATCH (n:P) WHERE n.age = $age RETURN n", &[])
        .expect_err("undeclared parameter");
    assert!(err.contains("$age"), "{err}");
    assert!(err.contains("declares no type"), "{err}");
}

/// A spec that binds nothing gives false assurance about what is checked.
#[test]
fn a_declared_parameter_the_query_never_uses_is_refused() {
    let err = validate_entry_shape(
        "t", "MATCH (n:P) RETURN n.name", &[param("age", "int", json!(1))],
    ).expect_err("unused parameter");
    assert!(err.contains("never uses"), "{err}");
}

#[test]
fn a_mis_declared_type_is_refused() {
    let err = validate_entry_shape(
        "t", "MATCH (n:P) WHERE n.age = $age RETURN n",
        &[param("age", "int", json!("not a number"))],
    ).expect_err("type/sample mismatch");
    assert!(err.contains("declared type"), "{err}");
}

/// The hostile-value corpus (DoD 5). Every value is bound, so none of it is
/// parsed as Cypher: the assertion is that the store is unchanged and no extra
/// clause took effect.
#[test]
fn hostile_parameter_values_cannot_add_a_clause_or_write() {
    let store = seeded();
    let before_nodes = store.node_count();

    const HOSTILE: &[&str] = &[
        "' OR 1=1 --",
        "x' RETURN n; MATCH (m) DETACH DELETE m; //",
        "'} ) DETACH DELETE n //",
        "\\\" OR true",
        "p1' UNION MATCH (s) RETURN s //",
        "$injected",
        "{{name}}",
        "'; CREATE (:Backdoor); //",
    ];

    for v in HOSTILE {
        let q = vec![QuerySpec {
            id: "hostile".into(),
            cypher: "MATCH (n:P) WHERE n.name = $name RETURN n.name".into(),
            unanswerable: true,
            params: vec![param("name", "string", json!(v))],
        }];
        // The value is bound, so it is compared as a string and matches nothing.
        // What must not happen is that any of it becomes syntax.
        let catalog = build_catalog(&store, &q, &[]).expect(v);
        assert_eq!(catalog.entries[0].rows, 0, "hostile value matched a node: {v}");
    }

    assert_eq!(
        store.node_count(), before_nodes,
        "the store changed while running hostile values through a read query"
    );
}

/// Recorded as a test because it is the reason DoD 3's two named risks are not
/// implemented: neither is expressible.
#[test]
fn a_parameter_cannot_reach_a_limit_or_a_var_length_bound() {
    assert!(
        samyama::query::parse_query("MATCH (n:P) RETURN n.name LIMIT $k").is_err(),
        "LIMIT $k parsed; an unbounded LIMIT slot would then be reachable"
    );
    assert!(
        samyama::query::parse_query("MATCH (a)-[:R*1..$n]->(b) RETURN a").is_err(),
        "*1..$n parsed; an unbounded var-length exponent would then be reachable"
    );
}

#[test]
fn parameter_extraction_finds_each_name_once() {
    assert_eq!(
        referenced_params("MATCH (n) WHERE n.a = $x AND n.b = $y OR n.c = $x RETURN n"),
        vec!["x".to_string(), "y".to_string()]
    );
    assert!(referenced_params("MATCH (n) RETURN n").is_empty());
}
