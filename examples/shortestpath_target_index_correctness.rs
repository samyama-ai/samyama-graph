//! An indexed target scan must return the same rows as the scan it replaced.
//!
//! Narrowing a scan is only a win if it narrows to the right set. The cases
//! that matter are the ones where the index answers *part* of the question:
//! a second inline property the index does not cover, a label the index is
//! not on, and a value that is not in the index at all. In each the filter
//! above must still decide, and the answer must equal the unindexed answer.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn build(indexed: bool) -> GraphStore {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..300i64 {
        let n = s.create_node_with_labels([Label::new("Account")]);
        s.set_node_property("default", n, "id", PropertyValue::Integer(i)).unwrap();
        // Two accounts share `id`-adjacent values but differ on `kind`, so a
        // query naming both properties has a wrong answer available to it.
        s.set_node_property("default", n, "kind",
            PropertyValue::String(if i % 2 == 0 { "even" } else { "odd" }.into())).unwrap();
        ids.push(n);
    }
    for i in 0..(ids.len() - 1) {
        let _ = s.create_edge(ids[i], ids[i + 1], "TRANSFER");
    }
    if indexed {
        let q = parse_query("CREATE INDEX ON :Account(id)").unwrap();
        MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();
    }
    s
}

fn run(s: &GraphStore, q: &str) -> Option<usize> {
    let parsed = parse_query(q).ok()?;
    QueryExecutor::new(s).execute(&parsed).ok().map(|b| b.records.len())
}

fn main() {
    let plain = build(false);
    let indexed = build(true);
    let cases = [
        ("single indexed property",
         "MATCH p = shortestPath((a:Account {id: 3})-[:TRANSFER*]-(b:Account {id: 40})) RETURN length(p) AS l"),
        ("second property the index does not cover, matching",
         "MATCH p = shortestPath((a:Account {id: 3})-[:TRANSFER*]-(b:Account {id: 40, kind: \"even\"})) RETURN length(p) AS l"),
        ("second property the index does not cover, NOT matching",
         "MATCH p = shortestPath((a:Account {id: 3})-[:TRANSFER*]-(b:Account {id: 40, kind: \"odd\"})) RETURN length(p) AS l"),
        ("target value absent from the graph",
         "MATCH p = shortestPath((a:Account {id: 3})-[:TRANSFER*]-(b:Account {id: 9999})) RETURN length(p) AS l"),
        ("label with no index on it",
         "MATCH p = shortestPath((a:Account {id: 3})-[:TRANSFER*]-(b:Account {kind: \"odd\"})) RETURN length(p) AS l"),
        ("directed",
         "MATCH p = shortestPath((a:Account {id: 3})-[:TRANSFER*]->(b:Account {id: 40})) RETURN length(p) AS l"),
        ("directed backwards, unreachable",
         "MATCH p = shortestPath((a:Account {id: 40})-[:TRANSFER*]->(b:Account {id: 3})) RETURN length(p) AS l"),
    ];
    let mut bad = 0;
    for (label, q) in cases {
        let (u, i) = (run(&plain, q), run(&indexed, q));
        let ok = u == i;
        if !ok { bad += 1; }
        println!("{:<52} unindexed={:?} indexed={:?}  {}", label, u, i, if ok { "ok" } else { "MISMATCH" });
    }
    assert_eq!(bad, 0, "{bad} cases where the index changed the answer");
    println!("\nOK: the indexed target scan returns what the unindexed scan returned");
}
