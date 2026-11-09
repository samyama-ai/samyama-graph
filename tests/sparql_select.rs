//! SPARQL SELECT and ASK answer from the RDF store (#2100, part 1).
//!
//! Until this, `SparqlEngine::query` returned an empty result for every query,
//! so "no matches" and "not implemented" looked the same. These tests pin exact
//! answers, and that anything not evaluated yet is an error.

use samyama::rdf::{Literal, NamedNode, RdfStore, RdfTerm, Triple};
use samyama::sparql::{SparqlEngine, SparqlResults};

const EX: &str = "http://example.org/";
const XSD_INT: &str = "http://www.w3.org/2001/XMLSchema#integer";

fn iri(s: &str) -> NamedNode {
    NamedNode::new(&format!("{EX}{s}")).unwrap()
}

fn store() -> RdfStore {
    let mut st = RdfStore::new();
    let mut add = |s: &str, p: &str, o: samyama::rdf::RdfObject| {
        st.insert(Triple::new(iri(s).into(), iri(p).into(), o))
            .unwrap();
    };
    let int =
        |n: i64| Literal::new_typed_literal(n.to_string(), NamedNode::new(XSD_INT).unwrap()).into();
    let text = |t: &str| Literal::new_simple_literal(t).into();
    add("alice", "name", text("Alice"));
    add("alice", "age", int(30));
    add("alice", "knows", iri("bob").into());
    add("alice", "knows", iri("carol").into());
    add("bob", "name", text("Bob"));
    add("bob", "age", int(25));
    add("bob", "knows", iri("carol").into());
    add("carol", "name", text("Carol"));
    add("carol", "age", int(41));
    add("carol", "email", text("carol@example.org"));
    add("dan", "knows", iri("dan").into());
    st
}

fn engine() -> SparqlEngine {
    SparqlEngine::new(store())
}

/// One column of a SELECT, as plain text in result order. `-` is unbound.
fn column(r: &SparqlResults, var: &str) -> Vec<String> {
    let SparqlResults::Bindings { solutions, .. } = r else {
        panic!("not bindings: {r:?}")
    };
    solutions
        .iter()
        .map(|s| match s.get(var) {
            Some(RdfTerm::Literal(l)) => l.value().to_string(),
            Some(RdfTerm::NamedNode(n)) => n.as_str().trim_start_matches(EX).to_string(),
            Some(RdfTerm::BlankNode(b)) => format!("_:{}", b.as_str()),
            None => "-".to_string(),
        })
        .collect()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn q(body: &str) -> String {
    format!("PREFIX ex: <{EX}> {body}")
}

#[test]
fn a_join_across_two_triples_returns_exactly_the_matches() {
    let r = engine()
        .query(&q(
            "SELECT ?n WHERE { ex:alice ex:knows ?p . ?p ex:name ?n }",
        ))
        .unwrap();
    assert_eq!(sorted(column(&r, "n")), ["Bob", "Carol"]);
    let SparqlResults::Bindings { variables, .. } = &r else {
        unreachable!()
    };
    assert_eq!(variables, &["n"]);
}

#[test]
fn filter_order_and_limit_work_on_numbers() {
    let r = engine()
        .query(&q(
            "SELECT ?n ?a WHERE { ?p ex:name ?n ; ex:age ?a FILTER(?a > 26) } ORDER BY DESC(?a)",
        ))
        .unwrap();
    assert_eq!(column(&r, "n"), ["Carol", "Alice"]);
    let r = engine()
        .query(&q(
            "SELECT ?n WHERE { ?p ex:name ?n ; ex:age ?a } ORDER BY ?a LIMIT 2 OFFSET 1",
        ))
        .unwrap();
    assert_eq!(column(&r, "n"), ["Alice", "Carol"]);
    // 30 as a typed integer equals the literal 30 written in the query.
    let r = engine()
        .query(&q(
            "SELECT ?n WHERE { ?p ex:name ?n ; ex:age ?a FILTER(?a = 30) }",
        ))
        .unwrap();
    assert_eq!(column(&r, "n"), ["Alice"]);
}

#[test]
fn optional_keeps_rows_without_a_match() {
    let r = engine()
        .query(&q(
            "SELECT ?n ?e WHERE { ?p ex:name ?n OPTIONAL { ?p ex:email ?e } } ORDER BY ?n",
        ))
        .unwrap();
    assert_eq!(column(&r, "n"), ["Alice", "Bob", "Carol"]);
    assert_eq!(column(&r, "e"), ["-", "-", "carol@example.org"]);
    let r = engine()
        .query(&q(
            "SELECT ?n WHERE { ?p ex:name ?n OPTIONAL { ?p ex:email ?e } FILTER(!BOUND(?e)) }",
        ))
        .unwrap();
    assert_eq!(sorted(column(&r, "n")), ["Alice", "Bob"]);
}

#[test]
fn union_distinct_and_a_repeated_variable() {
    let r = engine()
        .query(&q(
            "SELECT DISTINCT ?x WHERE { { ?x ex:knows ex:carol } UNION { ?x ex:email ?e } }",
        ))
        .unwrap();
    assert_eq!(sorted(column(&r, "x")), ["alice", "bob", "carol"]);
    // alice knows two people, so she appears twice without DISTINCT.
    let all = engine()
        .query(&q("SELECT ?x WHERE { ?x ex:knows ?y }"))
        .unwrap();
    assert_eq!(sorted(column(&all, "x")), ["alice", "alice", "bob", "dan"]);
    let once = engine()
        .query(&q("SELECT DISTINCT ?x WHERE { ?x ex:knows ?y }"))
        .unwrap();
    assert_eq!(sorted(column(&once, "x")), ["alice", "bob", "dan"]);
    // `?x ex:knows ?x` is only dan, who knows himself.
    let r = engine()
        .query(&q("SELECT ?x WHERE { ?x ex:knows ?x }"))
        .unwrap();
    assert_eq!(column(&r, "x"), ["dan"]);
}

#[test]
fn ask_answers_true_and_false() {
    let e = engine();
    assert!(matches!(
        e.query(&q("ASK { ex:bob ex:knows ex:carol }")).unwrap(),
        SparqlResults::Boolean(true)
    ));
    assert!(matches!(
        e.query(&q("ASK { ex:carol ex:knows ex:bob }")).unwrap(),
        SparqlResults::Boolean(false)
    ));
}

#[test]
fn what_is_not_evaluated_yet_is_an_error_not_an_empty_answer() {
    let e = engine();
    for unsupported in [
        "SELECT (COUNT(?p) AS ?c) WHERE { ?p ex:name ?n }",
        "SELECT ?p WHERE { ?p ex:knows+ ex:carol }",
        "SELECT ?x WHERE { ?p ex:age ?a BIND(?a + 1 AS ?x) }",
        "CONSTRUCT { ?p ex:named ?n } WHERE { ?p ex:name ?n }",
        "SELECT ?n WHERE { ?p ex:name ?n FILTER(STRSTARTS(?n, \"A\")) }",
    ] {
        let err = e.query(&q(unsupported)).expect_err(unsupported);
        assert!(
            err.to_string().contains("not") || err.to_string().contains("Unsupported"),
            "{unsupported}: {err}"
        );
    }
    assert!(e
        .query("SELEC ?x")
        .unwrap_err()
        .to_string()
        .starts_with("Parse error"));
}
