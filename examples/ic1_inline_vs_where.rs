//! Does an inline property on the far side of a var-length pattern drop rows?
//!
//! `(p)-[:KNOWS*1..3]-(f:Person {firstName: "Rare"})` returned 0 on a graph
//! where the expansion reaches essentially every person and ten of them are
//! named "Rare". The same predicate written as `WHERE` is a different planner
//! path -- the shortestPath work showed the two are not equally served -- so
//! ask both, plus the reachability question on its own.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn scalar(s: &GraphStore, q: &str) -> String {
    let p = parse_query(q).expect("parse");
    let b = QueryExecutor::new(s).execute(&p).expect("execute");
    b.records.first().and_then(|r| r.values().next().cloned())
        .map(|v| format!("{v:?}")).unwrap_or_else(|| "<no rows>".into())
}

fn main() {
    let n = 60_000i64;
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let p = s.create_node_with_labels([Label::new("Person")]);
        s.set_node_property("default", p, "id", PropertyValue::Integer(i)).unwrap();
        s.set_node_property("default", p, "firstName", PropertyValue::String(
            if i % 6000 == 7 { "Rare".into() } else { format!("Common{}", i % 17) })).unwrap();
        ids.push(p);
    }
    for i in 0..ids.len() {
        for d in 1..=40usize {
            let _ = s.create_edge(ids[i], ids[(i * 7 + d * 977) % ids.len()], "KNOWS");
        }
    }
    for stmt in ["CREATE INDEX ON :Person(id)", "CREATE INDEX ON :Person(firstName)"] {
        let q = parse_query(stmt).unwrap();
        MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();
    }

    let rare = scalar(&s, "MATCH (f:Person {firstName: \"Rare\"}) RETURN count(f) AS n");
    let reached = scalar(&s, "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person) RETURN count(DISTINCT f) AS n");
    let inline = scalar(&s, "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person {firstName: \"Rare\"}) RETURN count(DISTINCT f) AS n");
    let where_ = scalar(&s, "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person) WHERE f.firstName = \"Rare\" RETURN count(DISTINCT f) AS n");

    println!("people named Rare in the graph : {rare}");
    println!("distinct people within 3 hops  : {reached}");
    println!("  ...named Rare, inline pattern: {inline}");
    println!("  ...named Rare, WHERE clause  : {where_}");
    println!("\ninline and WHERE agree: {}", inline == where_);
}
