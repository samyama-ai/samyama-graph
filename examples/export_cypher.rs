//! Emit the whole graph as Cypher `CREATE` statements (INT-07).
//!
//! The exit path that needs no agreement from anybody: a text file of standard
//! Cypher that Neo4j, Memgraph, FalkorDB or another Samyama can read. `.sgsnap`
//! is ours and only we load it; Parquet and Arrow carry a *result set* rather
//! than a graph, so the topology has to be reassembled by hand on the far side.
//!
//! **Node identity is the whole problem.** A `CREATE` cannot refer to a node it
//! did not create in the same statement, so the edges need a way to find their
//! endpoints. This writes a `_sgid` property carrying the original id, indexes
//! it, matches on it, and then -- because it is our artefact and not the user's
//! data -- offers to remove it at the end. The alternative, one enormous
//! statement with every node bound to a variable, stops parsing somewhere in
//! the low thousands of nodes.
//!
//! ```text
//! cargo run --release --example export_cypher -- --out graph.cypher
//! ```

use std::collections::BTreeMap;
use std::io::Write;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

/// The property holding the original node id while the import runs.
pub const ID_PROPERTY: &str = "_sgid";

/// How many `CREATE`s go in one statement.
///
/// Batched because one statement per node is slow on every engine, and one
/// statement for the whole graph exceeds what parsers accept.
const BATCH: usize = 500;

fn literal(v: &PropertyValue) -> String {
    match v {
        PropertyValue::String(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")),
        PropertyValue::Integer(i) => i.to_string(),
        PropertyValue::Float(f) => {
            // `1` would be read back as an integer and change the property's
            // type across the move, which is a silent schema change.
            if f.fract() == 0.0 { format!("{f:.1}") } else { f.to_string() }
        }
        PropertyValue::Boolean(b) => b.to_string(),
        PropertyValue::Array(a) => {
            format!("[{}]", a.iter().map(literal).collect::<Vec<_>>().join(", "))
        }
        PropertyValue::Map(m) => {
            let mut keys: Vec<_> = m.keys().collect();
            keys.sort();
            format!(
                "{{{}}}",
                keys.iter()
                    .map(|k| format!("{k}: {}", literal(&m[*k])))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        // Temporal values go out as their Cypher constructor rather than as a
        // number: a date written as an integer arrives as an integer.
        PropertyValue::Date(_)
        | PropertyValue::LocalTime(_)
        | PropertyValue::Time { .. }
        | PropertyValue::LocalDateTime { .. }
        | PropertyValue::ZonedDateTime { .. }
        | PropertyValue::DateTime(_) => format!("'{v}'"),
        PropertyValue::Duration { .. } => format!("duration('{v}')"),
        PropertyValue::Null => "null".to_string(),
        PropertyValue::Vector(v) => format!(
            "[{}]",
            v.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(", ")
        ),
    }
}

fn props_literal(props: &BTreeMap<String, PropertyValue>) -> String {
    props
        .iter()
        .map(|(k, v)| format!("{k}: {}", literal(v)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Write `store` as a Cypher script.
pub fn write_cypher(store: &GraphStore, out: &mut impl Write) -> std::io::Result<(usize, usize)> {
    writeln!(out, "// Samyama graph export. See docs/LEAVING-SAMYAMA.md.")?;
    writeln!(
        out,
        "// `{ID_PROPERTY}` carries the original node id so the relationships below can find\n\
         // their endpoints. The last statement removes it."
    )?;
    writeln!(out, "CREATE INDEX ON :_Imported({ID_PROPERTY});")?;

    let nodes = store.all_nodes();
    let mut written_nodes = 0usize;
    for chunk in nodes.chunks(BATCH) {
        let mut stmt = String::from("CREATE\n");
        for (i, node) in chunk.iter().enumerate() {
            // Empty labels filtered out: `GraphStore::create_node("")` makes a
            // node whose label set holds one empty string, and emitting it
            // produced `(::_Imported)`, which does not parse. Found by the
            // round-trip test, not by reading the code.
            let mut labels: Vec<&str> = node
                .labels
                .iter()
                .map(|l| l.as_str())
                .filter(|l| !l.is_empty())
                .collect();
            labels.sort_unstable();
            // `_Imported` so the id index above has something to attach to even
            // for a node with no labels of its own.
            let label_text = if labels.is_empty() {
                ":_Imported".to_string()
            } else {
                format!(":{}:_Imported", labels.join(":"))
            };
            let mut props: BTreeMap<String, PropertyValue> =
                store.node_properties_merged(node.id).into_iter().collect();
            props.insert(
                ID_PROPERTY.to_string(),
                PropertyValue::Integer(node.id.as_u64() as i64),
            );
            stmt.push_str(&format!("  ({label_text} {{{}}})", props_literal(&props)));
            stmt.push_str(if i + 1 == chunk.len() { "\n" } else { ",\n" });
            written_nodes += 1;
        }
        writeln!(out, "{stmt};")?;
    }

    // Relationships, one `MATCH ... CREATE` per edge. Not batched: a batched
    // form needs every endpoint bound in one statement, and a batch whose
    // endpoints repeat would match the same pair more than once.
    let mut written_edges = 0usize;
    for edge in store.all_edges() {
        let props = store.edge_properties_merged(edge.id);
        let prop_text = if props.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", props_literal(&props.into_iter().collect()))
        };
        writeln!(
            out,
            "MATCH (a:_Imported {{{ID_PROPERTY}: {}}}), (b:_Imported {{{ID_PROPERTY}: {}}}) \
             CREATE (a)-[:{}{prop_text}]->(b);",
            edge.source.as_u64(),
            edge.target.as_u64(),
            edge.edge_type.as_str(),
        )?;
        written_edges += 1;
    }

    writeln!(
        out,
        "\n// Our scaffolding, not your data. Drop it once the import is done.\n\
         MATCH (n:_Imported) REMOVE n:_Imported REMOVE n.{ID_PROPERTY};"
    )?;
    Ok((written_nodes, written_edges))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out_path = args
        .iter()
        .position(|a| a == "--out")
        .and_then(|i| args.get(i + 1))
        .cloned();

    // A demonstration graph, so running the example with no arguments shows
    // what it produces rather than failing on an empty store.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in [
        "CREATE (:Person {name: 'Alice', age: 30})",
        "CREATE (:Person {name: 'Bob', age: 25})",
        "CREATE (:Company {name: 'Acme'})",
        "MATCH (a:Person {name: 'Alice'}), (c:Company) CREATE (a)-[:WORKS_AT {since: 2019}]->(c)",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    ] {
        engine.execute_mut(q, &mut store, "default").expect(q);
    }

    match out_path {
        Some(p) => {
            let mut f = std::fs::File::create(&p).expect("create");
            let (n, e) = write_cypher(&store, &mut f).expect("write");
            eprintln!("wrote {n} nodes and {e} relationships to {p}");
        }
        None => {
            let mut buf = Vec::new();
            let (n, e) = write_cypher(&store, &mut buf).expect("write");
            print!("{}", String::from_utf8_lossy(&buf));
            eprintln!("\n{n} nodes, {e} relationships");
        }
    }
}
