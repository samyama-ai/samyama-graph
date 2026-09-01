//! `RETURN DISTINCT` decides whether a var-length segment enumerates paths.
//!
//! #1054 claimed IC1's expansion emits 505,660 path rows to reach 5,218
//! endpoints and proposed collapsing it. That number came from a probe that
//! ran the pattern with a bare `RETURN`:
//!
//!     MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person {firstName: "..."}) RETURN f.id
//!
//! The benchmark's IC1 is `RETURN DISTINCT`, and `multiplicity_is_observable`
//! already routes a DISTINCT-terminated query with no aggregate to the BFS walk
//! instead of enumerating trails (#710). The probe and the query were not the
//! same question, and the proposed optimisation was already in the tree.
//!
//! This example exists so that stops being something anyone has to rediscover.
//! It asserts the gap rather than printing it: if `multiplicity_is_observable`
//! ever stops recognising this shape, IC1 silently starts enumerating again and
//! the only symptom is a benchmark getting slower.
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

/// Median of `n` runs, after one warm-up.
fn median(s: &GraphStore, q: &str, n: usize) -> (usize, Duration) {
    let _ = run(s, q);
    let mut rows = 0;
    let mut ds = Vec::new();
    for _ in 0..n {
        let (r, d) = run(s, q);
        rows = r;
        ds.push(d);
    }
    ds.sort();
    (rows, ds[ds.len() / 2])
}

fn main() {
    // Degree 40 so three hops actually fan out. An earlier version of this
    // probe used degree 6, where three hops reach 216 nodes and both shapes
    // cost microseconds — it would have shown no difference at all.
    let n = 60_000i64;
    let degree = 40usize;
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let p = s.create_node_with_labels([Label::new("Person")]);
        s.set_node_property("default", p, "id", PropertyValue::Integer(i)).unwrap();
        s.set_node_property("default", p, "firstName", PropertyValue::String(
            if i % 97 == 7 { "Rare".into() } else { format!("Common{}", i % 17) })).unwrap();
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
    println!("{n} Person, {} KNOWS (degree {degree})\n", n as usize * degree);

    let bare = "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person {firstName: \"Rare\"}) \
                WHERE f.id <> 0 RETURN f.id AS id";
    let distinct = "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person {firstName: \"Rare\"}) \
                    WHERE f.id <> 0 RETURN DISTINCT f.id AS id";

    let (bare_rows, bare_t) = median(&s, bare, 5);
    let (dist_rows, dist_t) = median(&s, distinct, 5);
    println!("{:<38} {:>9} rows  {:>10.2?}", "RETURN (enumerates trails)", bare_rows, bare_t);
    println!("{:<38} {:>9} rows  {:>10.2?}", "RETURN DISTINCT (BFS, as IC1)", dist_rows, dist_t);

    // The set of answers must be identical; only multiplicity differs. If the
    // two walks disagree on *which* nodes are reachable that is a wrong answer,
    // and it matters more than any timing here.
    let (p, q) = (parse_query(bare).unwrap(), parse_query(distinct).unwrap());
    let mut a: Vec<String> = QueryExecutor::new(&s).execute(&p).unwrap().records.iter()
        .filter_map(|r| r.get("id").map(|v| format!("{v:?}"))).collect();
    a.sort();
    a.dedup();
    let mut b: Vec<String> = QueryExecutor::new(&s).execute(&q).unwrap().records.iter()
        .filter_map(|r| r.get("id").map(|v| format!("{v:?}"))).collect();
    b.sort();
    assert_eq!(a, b, "the two walks disagree on which nodes are reachable");
    println!("\nboth walks agree on all {} distinct answers", b.len());

    assert!(dist_rows < bare_rows,
        "DISTINCT returned {dist_rows} rows and the bare form {bare_rows}: the \
         pattern is not fanning out, so this probe cannot test what it claims");

    // The point of the example. A generous bound: the measured gap is ~30x, and
    // asserting 3x fails loudly if the BFS path stops being taken while leaving
    // room for a slower machine.
    let speedup = bare_t.as_secs_f64() / dist_t.as_secs_f64();
    println!("DISTINCT is {speedup:.1}x faster — it takes the BFS walk, not trail enumeration");
    assert!(speedup > 3.0,
        "DISTINCT was only {speedup:.1}x faster than the enumerating form; \
         `multiplicity_is_observable` has probably stopped recognising this shape, \
         and IC1 is enumerating trails again (#710, #1054)");
}
