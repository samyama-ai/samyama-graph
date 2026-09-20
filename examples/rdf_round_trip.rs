//! Does an RDF round trip actually move data? (NDS-15)
//!
//! The conformance suite used to answer this by grepping the bodies of
//! `map_to_graph` and `sync_to_rdf` for `TODO` and `NotImplemented`. That was
//! the right check while they were stubs, and it stops being one the moment
//! those bodies delegate somewhere: a one-line call to a function that does
//! nothing passes a grep for "does nothing".
//!
//! So this runs the round trip and reports what survived. A build that broke
//! the mapping would produce a graph that does not match, not a body that
//! still reads as implemented.
//!
//!     cargo run --release --example rdf_round_trip -- --json out.json
//!
//! Exit code is non-zero when the round trip loses something it should not,
//! so the example is usable as a gate on its own.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;
use samyama::rdf::{GraphToRdfMapper, RdfStore, RdfToGraphMapper, IRI_PROPERTY};

const BASE: &str = "https://example.org/";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let mut source = GraphStore::new();
    let engine = QueryEngine::new();
    for q in [
        "CREATE (:Person {name: 'Alice', age: 30, active: true, score: 1.5})",
        "CREATE (:Person {name: 'Bob', age: 41, active: false, score: 2.5})",
        "CREATE (:Company {name: 'Acme'})",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2019}]->(b)",
        "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT]->(c)",
    ] {
        engine
            .execute_mut(q, &mut source, "default")
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }

    let mut rdf = RdfStore::new();
    let report = GraphToRdfMapper::new(BASE)
        .sync_to_rdf(&source, &mut rdf)
        .expect("export");

    let mut out = GraphStore::new();
    RdfToGraphMapper::new(BASE)
        .map_to_graph(&rdf, &mut out)
        .expect("import");

    // Second trip: the IRIs the first one invented must be reused, not
    // reinvented. That is the half of NDS-15 that says "IRIs preserved".
    let mut rdf2 = RdfStore::new();
    GraphToRdfMapper::new(BASE)
        .sync_to_rdf(&out, &mut rdf2)
        .expect("re-export");
    let mut out2 = GraphStore::new();
    RdfToGraphMapper::new(BASE)
        .map_to_graph(&rdf2, &mut out2)
        .expect("re-import");

    let nodes_in = source.all_nodes().len();
    let nodes_out = out.all_nodes().len();
    let edges_in = source.edge_count();
    let edges_out = out.edge_count();
    let iris_first = iris(&out);
    let iris_second = iris(&out2);
    let types_kept = typed_properties_survive(&out);
    let iris_stable = !iris_first.is_empty() && iris_first == iris_second;

    let ok = nodes_in == nodes_out && edges_in == edges_out && types_kept && iris_stable;

    println!("RDF round trip (NDS-15)");
    println!("  triples written      {}", report.triples_written);
    println!("  duplicates collapsed {}", report.duplicates_collapsed);
    println!("  nodes    {nodes_in} -> {nodes_out}");
    println!("  edges    {edges_in} -> {edges_out}");
    println!("  property types kept  {types_kept}");
    println!("  IRIs stable on a second trip  {iris_stable}");
    println!("  {}", if ok { "round trip preserved the graph" } else { "ROUND TRIP LOST SOMETHING" });

    if let Some(path) = json_out {
        let json = format!(
            "{{\n  \"round_trip_preserves_graph\": {ok},\n  \"nodes_in\": {nodes_in},\n  \
             \"nodes_out\": {nodes_out},\n  \"edges_in\": {edges_in},\n  \"edges_out\": {edges_out},\n  \
             \"triples_written\": {},\n  \"duplicates_collapsed\": {},\n  \
             \"property_types_preserved\": {types_kept},\n  \"iris_stable\": {iris_stable}\n}}\n",
            report.triples_written, report.duplicates_collapsed
        );
        std::fs::write(&path, json).expect("write json");
        eprintln!("[rdf] wrote {path}");
    }

    if !ok {
        std::process::exit(1);
    }
}

fn iris(store: &GraphStore) -> Vec<String> {
    let mut v: Vec<String> = store
        .all_nodes()
        .into_iter()
        .filter_map(|n| match store.node_properties_merged(n.id).get(IRI_PROPERTY) {
            Some(PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

/// An integer that came back an integer, and a float that came back a float.
///
/// Untyped literals would make every value a string and the node counts above
/// would still match, so counting nodes is not enough to call this a round trip.
fn typed_properties_survive(store: &GraphStore) -> bool {
    store.all_nodes().into_iter().any(|n| {
        let p = store.node_properties_merged(n.id);
        p.get("name") == Some(&PropertyValue::String("Alice".into()))
            && p.get("age") == Some(&PropertyValue::Integer(30))
            && p.get("active") == Some(&PropertyValue::Boolean(true))
            && p.get("score") == Some(&PropertyValue::Float(1.5))
    })
}
