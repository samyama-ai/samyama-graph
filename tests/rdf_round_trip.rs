//! Property graph → RDF → property graph, with the IRIs intact (#1362, NDS-15).
//!
//! NDS-15 asks for round-trip import/export with IRIs preserved. Each direction
//! alone is easy to get right and easy to check; the round trip is neither, so
//! these tests are written from the composition rather than from either half.
//!
//! The mapping used to return `NotImplemented` from all four functions — which
//! was itself a fix, because before that it returned `Ok(())` and an empty
//! graph, so an import reported success and did nothing.
//!
//! What is deliberately *not* asserted: that two edges of the same type between
//! the same pair survive. RDF has no parallel edges and they collapse into one
//! triple. The test below pins that collapse so it is a documented loss rather
//! than a surprise.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;
use samyama::rdf::{GraphToRdfMapper, RdfStore, RdfToGraphMapper, IRI_PROPERTY};

const BASE: &str = "https://example.org/";

fn graph_from(statements: &[&str]) -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in statements {
        engine
            .execute_mut(q, &mut store, "default")
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    store
}

/// Export, then import into a fresh graph.
fn round_trip(source: &GraphStore) -> GraphStore {
    let mut rdf = RdfStore::new();
    GraphToRdfMapper::new(BASE)
        .sync_to_rdf(source, &mut rdf)
        .expect("export");
    let mut out = GraphStore::new();
    RdfToGraphMapper::new(BASE)
        .map_to_graph(&rdf, &mut out)
        .expect("import");
    out
}

/// Every node as `(sorted labels, sorted non-IRI properties)`, sorted — so two
/// graphs can be compared without depending on node ids, which the round trip
/// does not claim to preserve.
fn shape(store: &GraphStore) -> Vec<(Vec<String>, Vec<(String, PropertyValue)>)> {
    let mut rows: Vec<_> = store
        .all_nodes()
        .into_iter()
        .map(|n| {
            let mut labels: Vec<String> =
                n.labels.iter().map(|l| l.as_str().to_string()).collect();
            labels.sort();
            // Merged, not `n.properties`: the row map is only half the store,
            // and an imported node's properties live on the columnar side
            // (#554). Comparing row maps made two identical graphs look
            // different and, worse, made an empty export look correct.
            let mut props: Vec<(String, PropertyValue)> = store
                .node_properties_merged(n.id)
                .into_iter()
                .filter(|(k, _)| k.as_str() != IRI_PROPERTY)
                .collect();
            props.sort_by(|a, b| a.0.cmp(&b.0));
            (labels, props)
        })
        .collect();
    rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    rows
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

const SOCIAL: &[&str] = &[
    "CREATE (:Person {name: 'Alice', age: 30, active: true, score: 1.0})",
    "CREATE (:Person {name: 'Bob', age: 41, active: false, score: 2.5})",
    "CREATE (:Company {name: 'Acme'})",
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT]->(c)",
];

#[test]
fn a_graph_survives_the_round_trip() {
    let source = graph_from(SOCIAL);
    let out = round_trip(&source);
    assert_eq!(
        shape(&source),
        shape(&out),
        "labels or properties changed across the round trip"
    );
    assert_eq!(
        source.edge_count(),
        out.edge_count(),
        "edge count changed across the round trip"
    );
}

#[test]
fn a_property_keeps_its_type_and_not_just_its_text() {
    // Untyped literals would make every value a string, and the round trip
    // would return a graph where age is "30". That is the difference between
    // a round trip and a lossy one, and a shape comparison alone would still
    // pass if both sides were strings — so this asserts the types directly.
    let out = round_trip(&graph_from(SOCIAL));
    let alice = out
        .all_nodes()
        .into_iter()
        .find(|n| {
            out.node_properties_merged(n.id).get("name")
                == Some(&PropertyValue::String("Alice".into()))
        })
        .expect("Alice");
    let alice = out.node_properties_merged(alice.id);
    assert_eq!(alice.get("age"), Some(&PropertyValue::Integer(30)));
    assert_eq!(alice.get("active"), Some(&PropertyValue::Boolean(true)));
    // 1.0 under Display is "1", which comes back an Integer. The float has to
    // be written with a form that keeps the point.
    assert_eq!(
        alice.get("score"),
        Some(&PropertyValue::Float(1.0)),
        "a whole float came back as something else"
    );
}

#[test]
fn iris_are_preserved_across_a_second_round_trip() {
    // The requirement's actual claim. The first trip invents IRIs from node
    // ids; the second must reuse them rather than invent new ones, or anything
    // that referred to the first set stops resolving.
    let first = round_trip(&graph_from(SOCIAL));
    let second = round_trip(&first);
    assert_eq!(iris(&first), iris(&second), "the IRIs moved");
    assert!(
        iris(&first).iter().all(|i| i.starts_with(BASE)),
        "{:?}",
        iris(&first)
    );
}

#[test]
fn an_imported_iri_is_kept_rather_than_regenerated() {
    // Someone else's IRIs, not ours. These must come back out unchanged: a
    // round trip that renamed every resource to `{base}node/{id}` would pass
    // every other test here.
    let turtle_like = [
        ("https://dbpedia.org/resource/Ada_Lovelace", "Person", "Ada"),
        ("https://dbpedia.org/resource/Alan_Turing", "Person", "Alan"),
    ];
    let mut rdf = RdfStore::new();
    {
        use samyama::rdf::{Literal, NamedNode, RdfObject, RdfPredicate, RdfSubject, Triple};
        for (iri, class, name) in turtle_like {
            let s = RdfSubject::NamedNode(NamedNode::new(iri).unwrap());
            rdf.insert(Triple::new(
                s.clone(),
                RdfPredicate::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap(),
                RdfObject::NamedNode(NamedNode::new(&format!("{BASE}class/{class}")).unwrap()),
            ))
            .unwrap();
            rdf.insert(Triple::new(
                s,
                RdfPredicate::new(&format!("{BASE}prop/name")).unwrap(),
                RdfObject::Literal(Literal::new_simple_literal(name)),
            ))
            .unwrap();
        }
    }
    let mut graph = GraphStore::new();
    RdfToGraphMapper::new(BASE)
        .map_to_graph(&rdf, &mut graph)
        .expect("import");

    assert_eq!(
        iris(&graph),
        vec![
            "https://dbpedia.org/resource/Ada_Lovelace".to_string(),
            "https://dbpedia.org/resource/Alan_Turing".to_string(),
        ]
    );

    let mut back = RdfStore::new();
    GraphToRdfMapper::new(BASE)
        .sync_to_rdf(&graph, &mut back)
        .expect("export");
    let subjects: Vec<String> = back
        .iter()
        .map(|t| t.subject.to_string())
        .filter(|s| s.contains("dbpedia"))
        .collect();
    assert!(
        !subjects.is_empty(),
        "the foreign IRIs were not used as subjects on the way out"
    );
}

#[test]
fn a_label_with_a_space_does_not_break_the_iri() {
    // Labels and property keys are user-authored. Left raw they are not valid
    // IRI segments, so the mapping refused graphs it could perfectly well
    // represent — and this is not an exotic case: an RDF import's labels come
    // from the local name of an `rdf:type` IRI, which is arbitrary text.
    //
    // Built through the store rather than Cypher because a backticked label is
    // itself a parse error (#1373) — which is the same gap seen from the other
    // side: a graph this path can hold cannot be queried by label.
    let mut source = GraphStore::new();
    let id = source.create_node("Research Paper");
    source
        .set_node_property("default", id, "first author", "Ada")
        .unwrap();
    source
        .set_node_property("default", id, "year of publication", 1843i64)
        .unwrap();
    let out = round_trip(&source);
    assert_eq!(shape(&source), shape(&out));
    assert_eq!(
        out.all_nodes()[0]
            .labels
            .iter()
            .map(|l| l.as_str())
            .collect::<Vec<_>>(),
        vec!["Research Paper"],
        "the label came back percent-encoded"
    );
}

#[test]
fn edge_properties_survive_by_reification() {
    let source = graph_from(&[
        "CREATE (:P {n: 'a'}), (:P {n: 'b'})",
        "MATCH (a:P {n: 'a'}), (b:P {n: 'b'}) CREATE (a)-[:RATED {stars: 5}]->(b)",
    ]);
    let mut rdf = RdfStore::new();
    GraphToRdfMapper::new(BASE)
        .sync_to_rdf(&source, &mut rdf)
        .expect("export");
    let text: Vec<String> = rdf.iter().map(|t| t.to_string()).collect();
    assert!(
        text.iter().any(|t| t.contains("rdf-syntax-ns#Statement")),
        "an edge with properties was not reified: {text:?}"
    );
    assert!(
        text.iter().any(|t| t.contains("prop/stars")),
        "the edge property is missing: {text:?}"
    );
}

#[test]
fn parallel_edges_collapse_and_that_is_the_documented_loss() {
    // RDF has no parallel edges. Two plain `KNOWS` between the same pair are
    // one triple, so the round trip returns one edge. Pinned rather than
    // wished away: a loss nobody wrote down is a loss somebody discovers in
    // production.
    let source = graph_from(&[
        "CREATE (:P {n: 'a'}), (:P {n: 'b'})",
        "MATCH (a:P {n: 'a'}), (b:P {n: 'b'}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:P {n: 'a'}), (b:P {n: 'b'}) CREATE (a)-[:KNOWS]->(b)",
    ]);
    assert_eq!(source.edge_count(), 2);
    assert_eq!(
        round_trip(&source).edge_count(),
        1,
        "if this now says 2, parallel edges survive and the docs are out of date"
    );

    // And the loss is reported, not silent. Before the report existed the
    // store's `DuplicateTriple` propagated and this graph **failed to export
    // at all** — the mapping refusing data it was supposed to be lossy about,
    // which is the worst of the three available behaviours.
    let mut rdf = RdfStore::new();
    let report = GraphToRdfMapper::new(BASE)
        .sync_to_rdf(&source, &mut rdf)
        .expect("a graph with parallel edges must still export");
    assert_eq!(report.duplicates_collapsed, 1, "{report:?}");
    assert!(report.triples_written > 0, "{report:?}");
}

#[test]
fn an_edge_property_survives_the_round_trip() {
    // Export wrote the reification and import ignored it, so an edge property
    // survived being written and not being read. A test that only checked the
    // RDF side — as the one above does — would have passed throughout.
    let source = graph_from(&[
        "CREATE (:P {n: 'a'}), (:P {n: 'b'})",
        "MATCH (a:P {n: 'a'}), (b:P {n: 'b'}) CREATE (a)-[:RATED {stars: 5, note: 'good'}]->(b)",
    ]);
    let out = round_trip(&source);
    let edges = out.all_edges();
    assert_eq!(edges.len(), 1);
    let props = out.edge_properties_merged(edges[0].id);
    assert_eq!(props.get("stars"), Some(&PropertyValue::Integer(5)));
    assert_eq!(
        props.get("note"),
        Some(&PropertyValue::String("good".into()))
    );
}

#[test]
fn an_edge_without_properties_gains_none() {
    // The other direction: a plain edge must not pick up reification leftovers
    // from a neighbouring statement.
    let out = round_trip(&graph_from(SOCIAL));
    for edge in out.all_edges() {
        assert!(
            out.edge_properties_merged(edge.id).is_empty(),
            "a plain edge came back with properties: {:?}",
            out.edge_properties_merged(edge.id)
        );
    }
}

#[test]
fn a_graph_with_nothing_duplicated_reports_no_collapse() {
    // The half that keeps the counter honest: a report that always said 1
    // would pass the assertion above.
    let mut rdf = RdfStore::new();
    let report = GraphToRdfMapper::new(BASE)
        .sync_to_rdf(&graph_from(SOCIAL), &mut rdf)
        .expect("export");
    assert_eq!(report.duplicates_collapsed, 0, "{report:?}");
    assert_eq!(report.triples_written, rdf.len(), "{report:?}");
}

#[test]
fn an_empty_graph_round_trips_to_an_empty_graph() {
    // The case that used to "pass" when the mapping returned Ok and did
    // nothing: an empty result was indistinguishable from a no-op. It is worth
    // keeping precisely because it is the one a broken implementation gets
    // right.
    let out = round_trip(&GraphStore::new());
    assert_eq!(out.all_nodes().len(), 0);
    assert_eq!(out.edge_count(), 0);
}

#[test]
fn a_node_with_no_label_still_comes_back() {
    // An RDF resource with no rdf:type has no label to carry, and a node
    // without a label cannot be found again. It gets `Resource`, and the point
    // of the test is that it exists at all rather than being dropped.
    use samyama::rdf::{Literal, NamedNode, RdfObject, RdfPredicate, RdfSubject, Triple};
    let mut rdf = RdfStore::new();
    rdf.insert(Triple::new(
        RdfSubject::NamedNode(NamedNode::new("https://example.org/thing/1").unwrap()),
        RdfPredicate::new(&format!("{BASE}prop/name")).unwrap(),
        RdfObject::Literal(Literal::new_simple_literal("nameless")),
    ))
    .unwrap();
    let mut graph = GraphStore::new();
    RdfToGraphMapper::new(BASE)
        .map_to_graph(&rdf, &mut graph)
        .expect("import");
    assert_eq!(graph.all_nodes().len(), 1);
    assert_eq!(
        graph.node_properties_merged(graph.all_nodes()[0].id).get("name"),
        Some(&PropertyValue::String("nameless".into()))
    );
}
