//! `CALL rdf.sparql(query [, base]) YIELD row` (#2100, part 3b): SPARQL from
//! inside Cypher, answering what `POST /api/sparql` answers.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::http::server::HttpServer;
use samyama::query::QueryEngine;
use tokio::sync::RwLock;
use tower::ServiceExt;

const B: &str = "https://samyama.ai/rdf/";

fn graph() -> GraphStore {
    let mut g = GraphStore::new();
    QueryEngine::new()
        .execute_mut(
            "CREATE (a:Person {name: 'Alice', age: 30, score: 2.5, active: true}), \
                    (b:Person {name: 'Bob'}), (a)-[:KNOWS]->(b)",
            &mut g,
            "default",
        )
        .unwrap();
    g
}

/// The `row` maps a CALL yields, each rendered sorted so order does not matter.
fn rows(g: &GraphStore, cypher: &str) -> Vec<std::collections::BTreeMap<String, PropertyValue>> {
    let batch = QueryEngine::new().execute(cypher, g).unwrap_or_else(|e| panic!("{cypher}: {e}"));
    batch
        .records
        .iter()
        .map(|r| match r.get("row").and_then(|v| v.as_property()) {
            Some(PropertyValue::Map(m)) => m.clone().into_iter().collect(),
            other => panic!("row is not a map: {other:?}"),
        })
        .collect()
}

fn call(q: &str) -> String {
    format!("CALL rdf.sparql('{}') YIELD row RETURN row", q.replace('\'', "\\'"))
}

#[test]
fn literals_come_back_as_the_cypher_values_they_encode() {
    let g = graph();
    let r = rows(&g, &call(&format!(
        "SELECT ?p ?name ?age ?score ?active WHERE {{ ?p <{B}prop/name> \"Alice\" ; <{B}prop/name> ?name ; \
         <{B}prop/age> ?age ; <{B}prop/score> ?score ; <{B}prop/active> ?active }}"
    )));
    assert_eq!(r.len(), 1, "{r:?}");
    let row = &r[0];
    assert_eq!(row["name"], PropertyValue::String("Alice".into()));
    assert_eq!(row["age"], PropertyValue::Integer(30), "an integer stays an integer");
    assert_eq!(row["score"], PropertyValue::Float(2.5));
    assert_eq!(row["active"], PropertyValue::Boolean(true));
    match &row["p"] {
        PropertyValue::String(iri) => assert!(iri.starts_with(&format!("{B}node/")), "{iri}"),
        other => panic!("an IRI comes back as its string: {other:?}"),
    }
}

#[test]
fn an_unbound_variable_reads_null_and_the_rows_can_be_filtered() {
    let g = graph();
    let q = format!("SELECT ?n ?a WHERE {{ ?x <{B}prop/name> ?n OPTIONAL {{ ?x <{B}prop/age> ?a }} }}");
    let cypher = format!(
        "CALL rdf.sparql('{q}') YIELD row WITH row WHERE row.a IS NULL RETURN row.n AS n"
    );
    let batch = QueryEngine::new().execute(&cypher, &g).unwrap();
    let names: Vec<String> = batch
        .records
        .iter()
        .map(|r| format!("{:?}", r.get("n").unwrap()))
        .collect();
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].contains("Bob"), "{names:?}");
}

#[test]
fn ask_yields_one_row_with_the_result() {
    let g = graph();
    for (edge, want) in [("KNOWS", true), ("HATES", false)] {
        let r = rows(&g, &call(&format!("ASK {{ ?x <{B}rel/{edge}> ?y }}")));
        assert_eq!(r.len(), 1);
        assert_eq!(r[0]["result"], PropertyValue::Boolean(want), "{edge}");
    }
}

#[test]
fn a_base_of_ones_own_is_honoured() {
    let g = graph();
    let other = "http://example.org/g/";
    let q = format!("SELECT ?n WHERE {{ ?p <{other}prop/name> ?n }}");
    let with = format!("CALL rdf.sparql('{q}', '{other}') YIELD row RETURN row");
    assert_eq!(rows(&g, &with).len(), 2);
    assert_eq!(rows(&g, &call(&q)).len(), 0, "under the default base nothing has that predicate");
}

#[test]
fn what_is_not_answered_is_an_error_not_an_empty_result() {
    let g = graph();
    let engine = QueryEngine::new();
    for (cypher, why) in [
        (call("SELECT ?x WHERE { ?x "), "a SPARQL parse error"),
        (call("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), "CONSTRUCT"),
        (call("INSERT DATA { <a:s> <a:p> <a:o> }"), "an UPDATE"),
        ("CALL rdf.sparql() YIELD row RETURN row".to_string(), "no query"),
        ("CALL rdf.sparql(1) YIELD row RETURN row".to_string(), "a query that is not a string"),
        ("CALL rdf.sparql('ASK {}', 'b', 'c') YIELD row RETURN row".to_string(), "three arguments"),
    ] {
        assert!(engine.execute(&cypher, &g).is_err(), "{why} came back as rows: {cypher}");
    }
}

/// The same queries over HTTP and from Cypher give the same values.
#[tokio::test]
async fn cypher_and_http_answer_the_same() {
    let store = Arc::new(RwLock::new(graph()));
    let app = HttpServer::new(Arc::clone(&store), 0).router();
    let queries = [
        format!("SELECT ?n WHERE {{ ?p a <{B}class/Person> ; <{B}prop/name> ?n }}"),
        format!("SELECT ?a ?b WHERE {{ ?x <{B}rel/KNOWS> ?y . ?x <{B}prop/name> ?a . ?y <{B}prop/name> ?b }}"),
        format!("SELECT ?age WHERE {{ ?p <{B}prop/age> ?age }}"),
    ];
    for q in queries {
        let res = app
            .clone()
            .oneshot(
                Request::post("/api/sparql")
                    .header("content-type", "application/sparql-query")
                    .body(Body::from(q.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let doc: serde_json::Value =
            serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
        let http: BTreeSet<String> = doc["results"]["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| {
                let mut kv: Vec<String> = b
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| format!("{k}={}", v["value"].as_str().unwrap()))
                    .collect();
                kv.sort();
                kv.join(" ")
            })
            .collect();
        let g = store.read().await;
        let cypher: BTreeSet<String> = rows(&g, &call(&q))
            .into_iter()
            .map(|m| {
                m.into_iter()
                    .map(|(k, v)| match v {
                        PropertyValue::String(s) => format!("{k}={s}"),
                        PropertyValue::Integer(i) => format!("{k}={i}"),
                        other => format!("{k}={other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        assert!(!http.is_empty(), "{q}");
        assert_eq!(cypher, http, "{q}");
    }
}
