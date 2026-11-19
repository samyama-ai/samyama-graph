//! SPARQL over the property graph where it is, with no copy (#2100, part 2).
//!
//! The rule under test: for the same graph and the same question, reading the
//! graph as RDF in place (`GraphAsRdf`) gives exactly what exporting it to an
//! `RdfStore` and asking that gives. Checked first one triple pattern at a
//! time, every shape, then as whole SPARQL queries.

use std::collections::BTreeSet;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::rdf::{
    GraphAsRdf, GraphToRdfMapper, MappingConfig, NamedNode, RdfObject, RdfStore, RdfSubject,
    Triple, TriplePattern, TripleSource, IRI_PROPERTY,
};
use samyama::sparql::{SparqlEngine, SparqlResults};

const BASE: &str = "http://example.org/g/";

fn config() -> MappingConfig {
    MappingConfig::new(BASE)
}

/// A graph with the parts the mapping treats specially: several labels, a
/// label needing escaping, typed properties, a node with its own IRI, two
/// parallel edges (one triple in RDF), an edge with properties (reified), a
/// self-loop and a node with nothing on it.
fn graph() -> GraphStore {
    let mut g = GraphStore::new();
    let t = "default";
    let alice = g.create_node("Person");
    g.add_label_to_node(t, alice, "Employee").unwrap();
    g.set_node_property(t, alice, "name", "Alice").unwrap();
    g.set_node_property(t, alice, "age", 30i64).unwrap();
    let bob = g.create_node("Person");
    g.set_node_property(t, bob, "name", "Bob").unwrap();
    g.set_node_property(t, bob, "score", 2.5f64).unwrap();
    g.set_node_property(t, bob, "active", true).unwrap();
    let acme = g.create_node("Company Name");
    g.set_node_property(t, acme, "name", "Acme & Co").unwrap();
    g.set_node_property(t, acme, IRI_PROPERTY, "http://other.org/acme").unwrap();
    let lone = g.create_node("Thing");
    let _ = lone;
    g.create_edge(alice, bob, "KNOWS").unwrap();
    g.create_edge(alice, bob, "KNOWS").unwrap(); // parallel: one triple
    let works = g.create_edge(alice, acme, "WORKS_AT").unwrap();
    g.set_edge_property(works, "since", 2019i64).unwrap();
    g.create_edge(bob, bob, "LIKES").unwrap(); // self-loop
    g
}

fn exported(g: &GraphStore) -> RdfStore {
    let mut rdf = RdfStore::new();
    GraphToRdfMapper::with_config(config()).sync_to_rdf(g, &mut rdf).unwrap();
    rdf
}

fn set(v: Vec<Triple>) -> BTreeSet<String> {
    v.into_iter().map(|t| format!("{t:?}")).collect()
}

/// Every bound/unbound combination of every exported triple, plus terms the
/// graph does not have: what the copy answers, the view answers.
#[test]
fn every_triple_pattern_matches_what_the_exported_copy_matches() {
    let g = graph();
    let copy = exported(&g);
    let view = GraphAsRdf::new(&g, config());
    let triples: Vec<Triple> = copy.iter().cloned().collect();
    assert!(triples.len() >= 15, "the fixture should exercise the mapping: {}", triples.len());

    let mut patterns = vec![TriplePattern::new(None, None, None)];
    for t in &triples {
        for mask in 1..8u8 {
            patterns.push(TriplePattern::new(
                (mask & 1 != 0).then(|| t.subject.clone()),
                (mask & 2 != 0).then(|| t.predicate.clone()),
                (mask & 4 != 0).then(|| t.object.clone()),
            ));
        }
    }
    // Names the graph does not use, in every position the planner narrows on.
    let missing = |s: &str| NamedNode::new(&format!("{BASE}{s}")).unwrap();
    for absent in ["node/999", "statement/999", "class/Nope", "rel/NOPE", "prop/nope", "node/x"] {
        patterns.push(TriplePattern::new(Some(RdfSubject::NamedNode(missing(absent))), None, None));
        patterns.push(TriplePattern::new(None, None, Some(RdfObject::NamedNode(missing(absent)))));
        patterns.push(TriplePattern::new(None, Some(missing(absent).into()), None));
    }

    for p in &patterns {
        let want = set(copy.matching(p).unwrap());
        let got = set(view.matching(p).unwrap());
        assert_eq!(got, want, "pattern {p:?}");
    }
}

#[test]
fn a_parallel_edge_is_one_triple_as_in_the_copy() {
    let g = graph();
    let view = GraphAsRdf::new(&g, config());
    let knows = NamedNode::new(&format!("{BASE}rel/KNOWS")).unwrap();
    let got = view.matching(&TriplePattern::new(None, Some(knows.into()), None)).unwrap();
    assert_eq!(got.len(), 1, "{got:?}");
}

#[test]
fn a_node_with_its_own_iri_is_found_by_it_and_not_by_its_number() {
    let g = graph();
    let view = GraphAsRdf::new(&g, config());
    let own = NamedNode::new("http://other.org/acme").unwrap();
    let by_own = view.matching(&TriplePattern::new(Some(RdfSubject::NamedNode(own)), None, None)).unwrap();
    assert!(!by_own.is_empty());
    // Acme's numbered IRI, from its real id: ids are the store's to assign.
    let acme = g.get_nodes_by_label(&"Company Name".into())[0].id.as_u64();
    let numbered = NamedNode::new(&format!("{BASE}node/{acme}")).unwrap();
    let by_number =
        view.matching(&TriplePattern::new(Some(RdfSubject::NamedNode(numbered)), None, None)).unwrap();
    assert!(by_number.is_empty(), "{by_number:?}");
}

// ── whole queries ──────────────────────────────────────────────────────────

fn rows(r: SparqlResults) -> BTreeSet<String> {
    match r {
        SparqlResults::Bindings { variables, solutions } => solutions
            .iter()
            .map(|s| {
                variables
                    .iter()
                    .map(|v| format!("{v}={:?}", s.get(v)))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect(),
        SparqlResults::Boolean(b) => BTreeSet::from([b.to_string()]),
        other => panic!("unexpected result {other:?}"),
    }
}

const QUERIES: &[&str] = &[
    "SELECT ?s WHERE { ?s a <http://example.org/g/class/Person> }",
    "SELECT ?s WHERE { ?s a <http://example.org/g/class/Company%20Name> }",
    "SELECT ?n ?a WHERE { ?p <http://example.org/g/prop/name> ?n . OPTIONAL { ?p <http://example.org/g/prop/age> ?a } }",
    "SELECT ?a ?b WHERE { ?a <http://example.org/g/rel/KNOWS> ?b }",
    "SELECT ?who ?n WHERE { ?who <http://example.org/g/rel/WORKS_AT> ?c . ?c <http://example.org/g/prop/name> ?n }",
    "SELECT ?st ?since WHERE { ?st <http://www.w3.org/1999/02/22-rdf-syntax-ns#object> <http://other.org/acme> . ?st <http://example.org/g/prop/since> ?since }",
    "SELECT ?p ?o WHERE { <ALICE> ?p ?o }",
    "SELECT ?s ?p WHERE { ?s ?p <BOB> }",
    "SELECT DISTINCT ?p WHERE { ?s ?p ?o } ORDER BY ?p",
    "SELECT ?n WHERE { ?x <http://example.org/g/prop/age> ?age . ?x <http://example.org/g/prop/name> ?n FILTER(?age > 18) }",
    "SELECT ?x WHERE { { ?x a <http://example.org/g/class/Thing> } UNION { ?x a <http://example.org/g/class/Employee> } }",
    "ASK { ?x <http://example.org/g/rel/LIKES> ?x }",
    "ASK { ?x <http://example.org/g/rel/HATES> ?y }",
];

#[test]
fn sparql_over_the_graph_answers_as_sparql_over_its_export() {
    let g = graph();
    let copy = SparqlEngine::new(exported(&g));
    let view = SparqlEngine::over_graph(&g, config());
    let iri_of = |name: &str| {
        let id = g.all_nodes().into_iter()
            .find(|n| g.node_properties_merged(n.id).get("name") == Some(&PropertyValue::String(name.into())))
            .unwrap().id.as_u64();
        format!("{BASE}node/{id}")
    };
    let (alice, bob) = (iri_of("Alice"), iri_of("Bob"));
    for q in QUERIES {
        let q = &q.replace("ALICE", &alice).replace("BOB", &bob);
        let want = rows(copy.query(q).unwrap_or_else(|e| panic!("copy: {q}: {e}")));
        let got = rows(view.query(q).unwrap_or_else(|e| panic!("view: {q}: {e}")));
        assert_eq!(got, want, "{q}");
        assert!(!want.is_empty(), "a query with no answer proves nothing: {q}");
    }
}

/// The point of reading in place: a change to the graph is in the next
/// answer, with nothing re-exported. And reading changes nothing.
#[test]
fn the_next_query_sees_the_graph_as_it_is_now() {
    let mut g = graph();
    let q = "SELECT ?s WHERE { ?s a <http://example.org/g/class/Person> }";
    let before = rows(SparqlEngine::over_graph(&g, config()).query(q).unwrap());
    let (nodes, edges) = (g.node_count(), g.edge_count());
    let _ = rows(SparqlEngine::over_graph(&g, config()).query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }").unwrap());
    assert_eq!((g.node_count(), g.edge_count()), (nodes, edges), "reading changed the graph");

    let carol = g.create_node("Person");
    g.set_node_property("default", carol, "name", PropertyValue::String("Carol".into())).unwrap();
    let after = rows(SparqlEngine::over_graph(&g, config()).query(q).unwrap());
    assert_eq!(after.len(), before.len() + 1, "{after:?}");
}
