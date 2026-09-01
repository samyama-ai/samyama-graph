//! Where does IC1's time go: choosing the plan, or walking the edges?
//!
//! IC1 is `(p:Person {id: X})-[:KNOWS*1..3]-(f:Person {firstName: "Y"})`. Both
//! ends carry an equality on an indexed property, so adding a `firstName` index
//! looked like it should help. At SF10 it did not move IC1 at all. This asks
//! why, at a KNOWS degree where three hops actually explode -- at low degree
//! the whole query costs microseconds and the question cannot be asked.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;
use std::time::{Duration, Instant};

fn run(s: &GraphStore, q: &str) -> (usize, Duration) {
    let p = parse_query(q).expect("parse");
    let t = Instant::now();
    let b = QueryExecutor::new(s).execute(&p).expect("execute");
    (b.records.len(), t.elapsed())
}

fn anchor_of(s: &GraphStore, q: &str) -> String {
    let p = parse_query(&format!("EXPLAIN {q}")).expect("parse");
    let b = QueryExecutor::new(s).execute(&p).expect("execute");
    let plan = b.records.first().and_then(|r| r.get("plan")).map(|v| format!("{v:?}")).unwrap_or_default();
    plan.replace("\\n", "\n").lines().rev().find(|l| l.contains("Scan"))
        .map(|l| l.trim().trim_matches(|c| c == '"' || c == ' ').to_string())
        .unwrap_or_else(|| "<no scan>".into())
}

fn main() {
    let n = 60_000i64;
    let degree = 40usize; // LDBC SNB Person-KNOWS is this order; 6 was not enough to explode
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
        for d in 1..=degree {
            let _ = s.create_edge(ids[i], ids[(i * 7 + d * 977) % ids.len()], "KNOWS");
        }
    }
    for stmt in ["CREATE INDEX ON :Person(id)", "CREATE INDEX ON :Person(firstName)"] {
        let q = parse_query(stmt).unwrap();
        MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();
    }
    println!("{n} Person, {} KNOWS (degree {degree}), id and firstName both indexed\n", n as usize * degree);

    // The whole query, and the same expansion with the name predicate removed.
    // If they cost the same, the name side is not where the time goes and no
    // index on it can help.
    let ic1 = "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person {firstName: \"Rare\"}) RETURN f.id AS id";
    let expand_only = "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person) RETURN f.id AS id";

    for (name, q) in [("IC1 (name-filtered)", ic1), ("expansion alone", expand_only)] {
        let _ = run(&s, q);
        let (r, d) = run(&s, q);
        println!("{name:<22} {:>10.2?}  rows={r:<9} anchor: {}", d, anchor_of(&s, q));
    }
    let (rows_ic1, t_ic1) = run(&s, ic1);
    let (rows_exp, t_exp) = run(&s, expand_only);
    let share = t_exp.as_secs_f64() / t_ic1.as_secs_f64() * 100.0;
    println!("\nthe expansion is {share:.0}% of IC1, and it produces {rows_exp} rows to keep {rows_ic1}.");
    println!("throughput: {:.1}M expanded rows/s",
        rows_exp as f64 / t_exp.as_secs_f64() / 1e6);
}
