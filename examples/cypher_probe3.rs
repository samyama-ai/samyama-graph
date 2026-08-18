//! A third sweep: MERGE, FOREACH, UNION, OPTIONAL MATCH, CALL, subqueries.
//!
//! Sweep 1 (`cypher_probe.rs`) covered scalar expressions and found five gaps
//! plus a nondeterminism bug. Sweep 2 (`cypher_probe2.rs`) covered writes,
//! paths, temporal and aggregate corners and found three *silent wrong
//! answers*. Both now pass 100%, so this covers the clause-level surface
//! neither touched.
//!
//! Same method: every case states its own expected answer, and wrong answers
//! are reported separately from errors — one is a hazard, the other a gap.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn render(v: Option<&Value>) -> String {
    match v {
        Some(Value::Property(p)) => match p {
            PropertyValue::Integer(n) => n.to_string(),
            PropertyValue::Float(f) => format!("{f}"),
            PropertyValue::String(s) => format!("\"{s}\""),
            PropertyValue::Boolean(b) => b.to_string(),
            PropertyValue::Null => "null".into(),
            PropertyValue::Array(a) => format!(
                "[{}]",
                a.iter().map(|x| match x {
                    PropertyValue::Integer(n) => n.to_string(),
                    PropertyValue::String(s) => format!("\"{s}\""),
                    other => format!("{other:?}"),
                }).collect::<Vec<_>>().join(", ")
            ),
            other => format!("{other:?}"),
        },
        Some(Value::Null) | None => "null".into(),
        Some(Value::NodeRef(_)) | Some(Value::Node(..)) => "<node>".into(),
        Some(Value::EdgeRef(..)) | Some(Value::Edge(..)) => "<edge>".into(),
        Some(Value::Path { nodes, edges }) => format!("<path {} {}>", nodes.len(), edges.len()),
    }
}

/// Ada -knows-> Bob -knows-> Cy. Cy has no age; Ada has no email.
fn fresh() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let _ = store.set_node_property("default", a, "name", PropertyValue::String("Ada".into()));
    let _ = store.set_node_property("default", a, "age", PropertyValue::Integer(36));
    let b = store.create_node("P");
    let _ = store.set_node_property("default", b, "name", PropertyValue::String("Bob".into()));
    let _ = store.set_node_property("default", b, "age", PropertyValue::Integer(41));
    let c = store.create_node("P");
    let _ = store.set_node_property("default", c, "name", PropertyValue::String("Cy".into()));
    store.create_edge(a, b, "KNOWS").unwrap();
    store.create_edge(b, c, "KNOWS").unwrap();
    store
}

fn run(setup: &[&str], read: &str) -> Result<String, String> {
    let mut store = fresh();
    for stmt in setup {
        let q = parse_query(stmt).map_err(|e| format!("parse `{stmt}`: {e:?}"))?;
        let mut m = MutQueryExecutor::new(&mut store, "default".to_string());
        m.execute(&q).map_err(|e| format!("exec `{stmt}`: {e:?}"))?;
    }
    let q = parse_query(read).map_err(|e| format!("parse: {e:?}"))?;
    let batch = QueryExecutor::new(&store).execute(&q).map_err(|e| format!("exec: {e:?}"))?;
    Ok(batch.records.first().map(|r| render(r.get("r"))).unwrap_or("<no rows>".into()))
}

fn main() {
    let cases: &[(&str, &[&str], &str, &str)] = &[
        // ---- OPTIONAL MATCH
        ("OPTIONAL MATCH keeps unmatched rows", &[], "MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN count(p) AS r", "3"),
        ("OPTIONAL MATCH yields null", &[], "MATCH (p:P) WHERE p.name = \"Cy\" OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN f.name AS r", "null"),
        ("OPTIONAL MATCH that matches", &[], "MATCH (p:P) WHERE p.name = \"Ada\" OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN f.name AS r", "\"Bob\""),
        ("count over an optional null", &[], "MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN count(f) AS r", "2"),
        // ---- MERGE
        ("MERGE ON CREATE fires", &["MERGE (p:P {name: \"Zed\"}) ON CREATE SET p.made = 1"], "MATCH (p:P) WHERE p.name = \"Zed\" RETURN p.made AS r", "1"),
        ("MERGE ON MATCH fires", &["MERGE (p:P {name: \"Ada\"}) ON MATCH SET p.seen = 1"], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.seen AS r", "1"),
        ("MERGE ON CREATE skipped on match", &["MERGE (p:P {name: \"Ada\"}) ON CREATE SET p.made = 1"], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.made AS r", "null"),
        ("MERGE ON MATCH skipped on create", &["MERGE (p:P {name: \"Zed\"}) ON MATCH SET p.seen = 1"], "MATCH (p:P) WHERE p.name = \"Zed\" RETURN p.seen AS r", "null"),
        ("MERGE twice creates once", &["MERGE (p:P {name: \"Zed\"})", "MERGE (p:P {name: \"Zed\"})"], "MATCH (p:P) RETURN count(p) AS r", "4"),
        ("MERGE a relationship", &["MATCH (a:P), (c:P) WHERE a.name = \"Ada\" AND c.name = \"Cy\" MERGE (a)-[:LIKES]->(c)"], "MATCH ()-[e:LIKES]->() RETURN count(e) AS r", "1"),
        ("MERGE a relationship twice", &["MATCH (a:P), (c:P) WHERE a.name = \"Ada\" AND c.name = \"Cy\" MERGE (a)-[:LIKES]->(c)", "MATCH (a:P), (c:P) WHERE a.name = \"Ada\" AND c.name = \"Cy\" MERGE (a)-[:LIKES]->(c)"], "MATCH ()-[e:LIKES]->() RETURN count(e) AS r", "1"),
        // ---- UNION
        ("UNION deduplicates", &[], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.name AS r UNION MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.name AS r", "\"Ada\""),
        ("UNION ALL keeps duplicates", &[], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.name AS r UNION ALL MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.name AS r", "\"Ada\""),
        // ---- FOREACH
        ("FOREACH sets over a list", &["MATCH (p:P) WHERE p.name = \"Ada\" FOREACH (x IN [1] | SET p.touched = x)"], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.touched AS r", "1"),
        // ---- WITH / aggregation pipelines
        ("WITH then filter on an aggregate", &[], "MATCH (p:P)-[:KNOWS]->(f:P) WITH p, count(f) AS n WHERE n > 0 RETURN count(p) AS r", "2"),
        ("WITH DISTINCT", &[], "MATCH (p:P)-[:KNOWS]->(f:P) WITH DISTINCT f RETURN count(f) AS r", "2"),
        ("WITH ORDER BY LIMIT then match", &[], "MATCH (p:P) WITH p ORDER BY p.name LIMIT 1 RETURN p.name AS r", "\"Ada\""),
        // ---- EXISTS subquery
        ("EXISTS positive", &[], "MATCH (p:P) WHERE EXISTS { MATCH (p)-[:KNOWS]->() } RETURN count(p) AS r", "2"),
        ("NOT EXISTS", &[], "MATCH (p:P) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->() } RETURN count(p) AS r", "1"),
        // ---- pattern shapes
        ("undirected matches both ways", &[], "MATCH (p:P)-[:KNOWS]-(f:P) RETURN count(f) AS r", "4"),
        ("variable length 1..2", &[], "MATCH (a:P)-[:KNOWS*1..2]->(x:P) WHERE a.name = \"Ada\" RETURN count(x) AS r", "2"),
        ("zero-length allowed", &[], "MATCH (a:P)-[:KNOWS*0..1]->(x:P) WHERE a.name = \"Ada\" RETURN count(x) AS r", "2"),
        ("a self-referencing pattern", &[], "MATCH (a:P)-[:KNOWS]->(b:P)-[:KNOWS]->(c:P) RETURN count(c) AS r", "1"),
        ("anonymous nodes", &[], "MATCH (:P)-[:KNOWS]->(:P) RETURN count(*) AS r", "2"),
        // ---- DELETE semantics
        ("DELETE a node with edges errors or detaches", &["MATCH (p:P) WHERE p.name = \"Cy\" DELETE p"], "MATCH (p:P) RETURN count(p) AS r", "2"),
        // ---- CALL
        ("CALL pageRank YIELD", &[], "CALL algo.pageRank() YIELD nodeId, score RETURN count(score) AS r", "3"),
    ];

    let mut wrong = Vec::new();
    let mut errored = Vec::new();
    for (label, setup, read, expected) in cases {
        match run(setup, read) {
            Err(e) => errored.push((label.to_string(), read.to_string(), e)),
            Ok(got) if got != *expected => {
                wrong.push((label.to_string(), read.to_string(), expected.to_string(), got))
            }
            Ok(_) => {}
        }
    }
    println!("{} cases: {} ok, {} wrong answer, {} errored",
        cases.len(), cases.len()-wrong.len()-errored.len(), wrong.len(), errored.len());
    if !wrong.is_empty() {
        println!("\n=== WRONG ANSWER (parses, runs, returns the wrong thing) ===");
        for (l,q,e,g) in &wrong { println!("  {l}\n     {q}\n     expected {e}, got {g}"); }
    }
    if !errored.is_empty() {
        println!("\n=== ERRORED (at least it says so) ===");
        for (l,q,e) in &errored { println!("  {l}\n     {q}\n     {e}"); }
    }
}
