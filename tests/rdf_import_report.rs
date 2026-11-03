//! Importing RDF reports what it skipped (#1906).
//!
//! `RdfToGraphMapper::map_to_graph` reads back the RDF Samyama writes: only
//! `rdf:type`, `{base}prop/…` and `{base}rel/…` are mapped. Every other
//! predicate -- all of SKOS, RDFS and OWL -- was skipped, and the call
//! returned `Ok(())`. Importing a real vocabulary gave nodes with no
//! properties and no edges, reported as success.
//!
//! The import now returns an `RdfImportReport` that counts the loss. What is
//! imported does not change here; the richer mapping is #1910.

use samyama::graph::GraphStore;
use samyama::rdf::{RdfFormat, RdfParser, RdfStore, RdfToGraphMapper};

const BASE: &str = "https://example.org/";

/// Ten triples: three concepts, SKOS labels and hierarchy (skipped), and two
/// literals on Samyama's own property namespace that lose something.
const SKOS: &str = r#"
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix c:    <https://example.org/concept/> .
@prefix p:    <https://example.org/prop/> .

c:db      a skos:Concept ; skos:prefLabel "Databases"@en ;        skos:broader c:cs .
c:graphdb a skos:Concept ; skos:prefLabel "Graph databases"@en ;  skos:broader c:db .
c:cs      a skos:Concept ; skos:prefLabel "Computer science"@en ;
          p:label "Informatik"@de ;
          p:created "2012-01-01"^^xsd:date .
"#;

fn import(turtle: &str) -> (GraphStore, samyama::rdf::RdfImportReport) {
    let mut rdf = RdfStore::new();
    for triple in RdfParser::parse(turtle, RdfFormat::Turtle).expect("the fixture parses") {
        rdf.insert(triple).expect("insert");
    }
    let mut graph = GraphStore::new();
    let report = RdfToGraphMapper::new(BASE)
        .map_to_graph(&rdf, &mut graph)
        .expect("import");
    (graph, report)
}

#[test]
fn skos_predicates_are_counted_as_skipped() {
    let (graph, report) = import(SKOS);
    assert_eq!(report.triples_read, 10);
    assert_eq!(report.predicates_skipped.get("http://www.w3.org/2004/02/skos/core#prefLabel"), Some(&3));
    assert_eq!(report.predicates_skipped.get("http://www.w3.org/2004/02/skos/core#broader"), Some(&2));
    assert_eq!(report.predicates_skipped.len(), 2, "{:?}", report.predicates_skipped);
    assert!(!report.lossless());

    // What did arrive, counted by the import and matching the graph.
    assert_eq!(report.nodes_created, 3);
    assert_eq!(graph.node_count(), 3);
    assert_eq!(report.labels_applied, 3);
    assert_eq!(report.edges_created, 0, "skos:broader is not mapped to an edge yet (#1910)");
    assert_eq!(graph.edge_count(), 0);
    assert_eq!(report.properties_set, 2);
}

#[test]
fn a_language_tag_and_an_unread_datatype_are_counted() {
    let (_, report) = import(SKOS);
    assert_eq!(report.language_tags_dropped, 1, "\"Informatik\"@de lost its tag");
    assert_eq!(report.literals_typed_as_string, 1, "xsd:date arrived as a string");
}

#[test]
fn the_counts_add_up() {
    // Every triple is accounted for exactly once: a label, a property, an
    // edge, or a skip. A count that missed a branch would leave a gap here.
    let (_, report) = import(SKOS);
    let skipped: u64 = report.predicates_skipped.values().sum();
    assert_eq!(
        report.labels_applied + report.properties_set + report.edges_created + skipped as usize,
        report.triples_read
    );
}

#[test]
fn samyama_s_own_namespace_imports_losslessly() {
    let turtle = r#"
@prefix c: <https://example.org/class/> .
@prefix p: <https://example.org/prop/> .
@prefix r: <https://example.org/rel/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<https://example.org/n/1> a c:Person ; p:name "Ada" ; p:born "1815"^^xsd:integer ; r:KNOWS <https://example.org/n/2> .
<https://example.org/n/2> a c:Person ; p:name "Alan" ; p:active "true"^^xsd:boolean .
"#;
    let (graph, report) = import(turtle);
    assert!(report.lossless(), "{report:?}");
    assert_eq!(report.edges_created, 1);
    assert_eq!(graph.edge_count(), 1);
    assert_eq!(report.properties_set, 4);
}
