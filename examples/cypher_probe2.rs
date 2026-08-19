//! A second sweep: mutations, paths, temporal, and aggregate edge cases.
//!
//! `examples/cypher_probe.rs` covers scalar expressions and found five gaps
//! plus a nondeterminism bug (#577, #578). This covers the surface it does not:
//! writes, path functions, date handling, and the corners of aggregation —
//! looking for the same class, a construct that parses, runs, and returns the
//! wrong thing without erroring.
//!
//! Each case states its own expected answer. Wrong answers and errors are
//! reported separately, because one is a hazard and the other a gap.

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
                    PropertyValue::Float(f) => format!("{f}"),
                    PropertyValue::Boolean(b) => b.to_string(),
                    PropertyValue::Null => "null".into(),
                    other => format!("{other:?}"),
                }).collect::<Vec<_>>().join(", ")
            ),
            other => format!("{other:?}"),
        },
        Some(Value::Null) | None => "null".into(),
        Some(Value::NodeRef(_)) | Some(Value::Node(..)) => "<node>".into(),
        Some(Value::EdgeRef(..)) | Some(Value::Edge(..)) => "<edge>".into(),
        Some(Value::Path { nodes, edges }) => format!("<path {} nodes {} edges>", nodes.len(), edges.len()),
    }
}

/// A small graph rebuilt per case, so a mutation cannot leak into the next.
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
    let e = store.create_edge(a, b, "KNOWS").unwrap();
    let _ = store.set_edge_property(e, "since", PropertyValue::Integer(2020));
    store.create_edge(b, c, "KNOWS").unwrap();
    store
}

/// Runs a script of write statements, then one read, returning column `r`.
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
    // (label, setup statements, read query, expected `r`)
    let cases: &[(&str, &[&str], &str, &str)] = &[
        // ---- writes
        ("CREATE a node", &["CREATE (:P {name: \"Dee\", age: 1})"], "MATCH (p:P) RETURN count(p) AS r", "4"),
        ("CREATE reads back", &["CREATE (:P {name: \"Dee\"})"], "MATCH (p:P) WHERE p.name = \"Dee\" RETURN p.name AS r", "\"Dee\""),
        ("SET a property", &["MATCH (p:P) WHERE p.name = \"Ada\" SET p.age = 99"], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.age AS r", "99"),
        ("SET a new property", &["MATCH (p:P) WHERE p.name = \"Ada\" SET p.city = \"Pune\""], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.city AS r", "\"Pune\""),
        ("SET to null removes", &["MATCH (p:P) WHERE p.name = \"Ada\" SET p.age = null"], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.age AS r", "null"),
        ("REMOVE a property", &["MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age"], "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.age AS r", "null"),
        ("DELETE a node", &["MATCH (p:P) WHERE p.name = \"Cy\" DELETE p"], "MATCH (p:P) RETURN count(p) AS r", "2"),
        ("DETACH DELETE", &["MATCH (p:P) WHERE p.name = \"Bob\" DETACH DELETE p"], "MATCH (p:P) RETURN count(p) AS r", "2"),
        ("DETACH DELETE drops edges", &["MATCH (p:P) WHERE p.name = \"Bob\" DETACH DELETE p"], "MATCH ()-[e:KNOWS]->() RETURN count(e) AS r", "0"),
        ("MERGE matches existing", &["MERGE (p:P {name: \"Ada\"})"], "MATCH (p:P) RETURN count(p) AS r", "3"),
        ("MERGE creates missing", &["MERGE (p:P {name: \"Zed\"})"], "MATCH (p:P) RETURN count(p) AS r", "4"),
        ("CREATE an edge", &["MATCH (a:P), (b:P) WHERE a.name = \"Ada\" AND b.name = \"Cy\" CREATE (a)-[:LIKES]->(b)"], "MATCH ()-[e:LIKES]->() RETURN count(e) AS r", "1"),
        ("SET an edge property", &["MATCH ()-[e:KNOWS]->() WHERE e.since = 2020 SET e.since = 2021"], "MATCH ()-[e:KNOWS]->() WHERE e.since = 2021 RETURN count(e) AS r", "1"),
        ("SET a label", &["MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin"], "MATCH (p:Admin) RETURN count(p) AS r", "1"),
        ("REMOVE a label", &["MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin", "MATCH (p:Admin) REMOVE p:Admin"], "MATCH (p:Admin) RETURN count(p) AS r", "0"),
        // ---- paths
        ("nodes(p)", &[], "MATCH p = (a:P)-[:KNOWS]->(b:P) WHERE a.name = \"Ada\" RETURN size(nodes(p)) AS r", "2"),
        ("relationships(p)", &[], "MATCH p = (a:P)-[:KNOWS]->(b:P) WHERE a.name = \"Ada\" RETURN size(relationships(p)) AS r", "1"),
        ("length of a 2-hop path", &[], "MATCH p = (a:P)-[:KNOWS*2]->(c:P) WHERE a.name = \"Ada\" RETURN length(p) AS r", "2"),
        ("a path value round-trips", &[], "MATCH p = (a:P)-[:KNOWS]->(b:P) WHERE a.name = \"Ada\" RETURN p AS r", "<path 2 nodes 1 edges>"),
        // ---- aggregation corners
        ("count over no rows", &[], "MATCH (p:Nonexistent) RETURN count(p) AS r", "0"),
        ("sum over no rows", &[], "MATCH (p:Nonexistent) RETURN sum(p.age) AS r", "0"),
        ("avg over no rows", &[], "MATCH (p:Nonexistent) RETURN avg(p.age) AS r", "null"),
        ("min over no rows", &[], "MATCH (p:Nonexistent) RETURN min(p.age) AS r", "null"),
        ("collect over no rows", &[], "MATCH (p:Nonexistent) RETURN collect(p.age) AS r", "[]"),
        ("count ignores nulls", &[], "MATCH (p:P) RETURN count(p.age) AS r", "2"),
        ("count(*) does not", &[], "MATCH (p:P) RETURN count(*) AS r", "3"),
        ("avg ignores nulls", &[], "MATCH (p:P) RETURN avg(p.age) AS r", "38.5"),
        ("collect drops nulls", &[], "MATCH (p:P) RETURN size(collect(p.age)) AS r", "2"),
        ("count DISTINCT over nodes", &[], "MATCH (a:P)-[:KNOWS]->(b:P) RETURN count(DISTINCT a) AS r", "2"),
        ("aggregate with no group key", &[], "MATCH (p:P) RETURN max(p.age) AS r", "41"),
        // ---- DISTINCT / ordering
        ("RETURN DISTINCT", &[], "MATCH (p:P) RETURN count(DISTINCT p.age) AS r", "2"),
        // collect() drops nulls by design (#358), so Cy's absent age is absent here
        // too. Asserted as the documented behaviour rather than as a surprise.
        ("collect drops the null, ordered", &[], "MATCH (p:P) RETURN collect(p.age) AS r ORDER BY p.age ASC", "[36, 41]"),
        ("SKIP and LIMIT", &[], "MATCH (p:P) RETURN p.name AS r ORDER BY p.name SKIP 1 LIMIT 1", "\"Bob\""),
        // ---- temporal
        ("datetime from millis", &[], "WITH datetime({epochMillis: 1700000000000}) AS d RETURN d.year AS r", "2023"),
        ("duration component", &[], "WITH duration({days: 3}) AS d RETURN d.days AS r", "3"),
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

    println!(
        "{} cases: {} ok, {} wrong answer, {} errored",
        cases.len(),
        cases.len() - wrong.len() - errored.len(),
        wrong.len(),
        errored.len()
    );
    if !wrong.is_empty() {
        println!("\n=== WRONG ANSWER (parses, runs, returns the wrong thing) ===");
        for (l, q, e, g) in &wrong {
            println!("  {l}\n     {q}\n     expected {e}, got {g}");
        }
    }
    if !errored.is_empty() {
        println!("\n=== ERRORED (at least it says so) ===");
        for (l, q, e) in &errored {
            println!("  {l}\n     {q}\n     {e}");
        }
    }
}
