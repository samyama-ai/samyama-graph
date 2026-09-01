//! How much of a var-length expansion is walking, and how much is emitting?
//!
//! IC1 profiles at 91.3% in `VarLengthExpand`, which emits 9,118 rows for a
//! `firstName` filter above it to cut to 2. The walk has to visit those nodes;
//! it does not have to build a record for each one. `emit_ok` already prunes on
//! target *labels* before buffering, so pointing the pattern at a label nothing
//! carries traverses identically and emits nothing — the difference between the
//! two is the cost of emission.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;
use std::time::{Duration, Instant};

fn median(s: &GraphStore, q: &str, n: usize) -> (usize, Duration) {
    let p = parse_query(q).expect("parse");
    let _ = QueryExecutor::new(s).execute(&p);
    let mut rows = 0;
    let mut ds = Vec::new();
    for _ in 0..n {
        let t = Instant::now();
        let b = QueryExecutor::new(s).execute(&p).expect("execute");
        rows = b.records.len();
        ds.push(t.elapsed());
    }
    ds.sort();
    (rows, ds[ds.len() / 2])
}

fn main() {
    let n = 60_000i64;
    let degree = 20usize; // SF1's KNOWS degree is ~21
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let p = s.create_node_with_labels([Label::new("Person")]);
        s.set_node_property("default", p, "id", PropertyValue::Integer(i)).unwrap();
        s.set_node_property("default", p, "firstName", PropertyValue::String(
            if i % 4000 == 7 { "Zeljko".into() } else { format!("Common{}", i % 900) })).unwrap();
        ids.push(p);
    }
    for i in 0..ids.len() {
        for d in 1..=degree {
            let _ = s.create_edge(ids[i], ids[(i * 7 + d * 977) % ids.len()], "KNOWS");
        }
    }
    let q = parse_query("CREATE INDEX ON :Person(id)").unwrap();
    MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();

    // Same traversal in every case; only what is emitted differs.
    let emit_all = "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person) RETURN count(DISTINCT f) AS n";
    let emit_none = "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:NoSuchLabel) RETURN count(DISTINCT f) AS n";
    let ic1 = "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person {firstName: \"Zeljko\"}) \
               RETURN count(DISTINCT f) AS n";

    let (_, t_all) = median(&s, emit_all, 7);
    let (_, t_none) = median(&s, emit_none, 7);
    let (_, t_ic1) = median(&s, ic1, 7);

    // How many nodes the walk actually reaches, for context.
    let reached = {
        let p = parse_query(emit_all).unwrap();
        let b = QueryExecutor::new(&s).execute(&p).unwrap();
        b.records.first().and_then(|r| r.values().next().cloned())
            .map(|v| format!("{v:?}")).unwrap_or_default()
    };

    println!("{n} Person, {} KNOWS (degree {degree}); 3-hop reaches {reached}\n",
             n as usize * degree);
    println!("{:<44} {:>10.2?}", "walk + emit every endpoint", t_all);
    println!("{:<44} {:>10.2?}", "walk only (label matches nothing)", t_none);
    println!("{:<44} {:>10.2?}", "walk + emit, name filtered above (IC1)", t_ic1);

    let emit_share = 1.0 - t_none.as_secs_f64() / t_all.as_secs_f64();
    println!("\nemission is {:.0}% of the operator; the walk itself is {:.0}%",
             emit_share * 100.0, (1.0 - emit_share) * 100.0);
    println!("IC1 pays the emission cost in full and keeps almost none of it — the");
    println!("`firstName` predicate is inline on the target pattern, so the operator");
    println!("could prune before buffering the way `emit_ok` already does for labels.");
}
