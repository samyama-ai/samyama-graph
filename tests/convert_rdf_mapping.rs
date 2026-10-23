//! The RDF mapping file and its presets (#1910).
//!
//! One test per decision the mapping makes, each on a fixture of a few
//! triples: node id, labels, literal keys, edges, multi-valued properties,
//! language tags, typed literals, blank nodes, merge, reverse and base IRI.
//! Then the presets, the `samyama` preset against the engine's mapper, and the
//! file's own errors.

use std::collections::BTreeSet;

use samyama::convert::rdf_mapping::{Mapping, MappingFileError, RdfMapReport};
use samyama::graph::{GraphStore, PropertyValue};
use samyama::rdf::{RdfFormat, RdfParser, RdfStore, IRI_PROPERTY};

const PREFIXES: &str = r#"
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/> .
@prefix other: <http://other.org/ns#> .
"#;

fn store(turtle: &str) -> RdfStore {
    let mut s = RdfStore::new();
    for t in RdfParser::parse(&format!("{PREFIXES}{turtle}"), RdfFormat::Turtle).unwrap() {
        let _ = s.insert(t);
    }
    s
}

fn map(yaml: &str, turtle: &str) -> (GraphStore, RdfMapReport) {
    let mapping = Mapping::from_yaml(yaml).unwrap();
    let mut g = GraphStore::new();
    let report = mapping.apply(&store(turtle), &mut g).unwrap();
    (g, report)
}

fn node(g: &GraphStore, iri: &str) -> samyama::graph::NodeId {
    g.all_nodes()
        .iter()
        .find(|n| {
            g.node_properties_merged(n.id).get(IRI_PROPERTY)
                == Some(&PropertyValue::String(iri.into()))
        })
        .map(|n| n.id)
        .unwrap_or_else(|| panic!("no node for {iri}"))
}

fn prop(g: &GraphStore, iri: &str, key: &str) -> Option<PropertyValue> {
    g.node_properties_merged(node(g, iri)).get(key).cloned()
}

fn labels(g: &GraphStore, iri: &str) -> BTreeSet<String> {
    let id = node(g, iri);
    g.get_node(id)
        .unwrap()
        .labels
        .iter()
        .map(|l| l.as_str().to_string())
        .collect()
}

fn iri_of(g: &GraphStore, id: samyama::graph::NodeId) -> String {
    match g.node_properties_merged(id).get(IRI_PROPERTY) {
        Some(PropertyValue::String(s)) => s.clone(),
        other => panic!("{other:?}"),
    }
}

/// Every edge as `(source IRI, type, target IRI)`.
fn edges(g: &GraphStore) -> BTreeSet<(String, String, String)> {
    g.all_edges()
        .iter()
        .map(|e| {
            (
                iri_of(g, e.source),
                e.edge_type.as_str().to_string(),
                iri_of(g, e.target),
            )
        })
        .collect()
}

fn s(v: &str) -> PropertyValue {
    PropertyValue::String(v.into())
}

#[test]
fn a_node_is_identified_by_its_iri() {
    let (g, r) = map("", "ex:a ex:name \"A\" .");
    assert_eq!(r.nodes_created, 1);
    assert_eq!(prop(&g, "http://example.org/a", "name"), Some(s("A")));
}

#[test]
fn labels_come_from_rdf_type_by_local_name_table_or_style() {
    let fixture = "ex:a rdf:type ex:Person . ex:b rdf:type skos:Concept . ex:c ex:name \"c\" .";
    let (g, _) = map("", fixture);
    assert_eq!(
        labels(&g, "http://example.org/a"),
        ["Person".to_string()].into()
    );
    assert_eq!(
        labels(&g, "http://example.org/b"),
        ["Concept".to_string()].into()
    );
    // Untyped: the placeholder the engine's mapper also gives.
    assert_eq!(
        labels(&g, "http://example.org/c"),
        ["Resource".to_string()].into()
    );

    let (g, _) = map(
        "prefixes: { ex: \"http://example.org/\" }\nlabels: { \"ex:Person\": Human }",
        fixture,
    );
    assert_eq!(
        labels(&g, "http://example.org/a"),
        ["Human".to_string()].into()
    );

    let (g, _) = map("label_style: prefixed", fixture);
    assert_eq!(
        labels(&g, "http://example.org/b"),
        ["skos_Concept".to_string()].into()
    );

    let (g, _) = map("label_style: iri", fixture);
    assert_eq!(
        labels(&g, "http://example.org/a"),
        ["http://example.org/Person".to_string()].into()
    );
}

#[test]
fn a_label_clash_between_namespaces_falls_back_to_prefixed_and_is_reported() {
    let (g, r) = map(
        "prefixes: { ex: \"http://example.org/\", other: \"http://other.org/ns#\" }",
        "ex:a rdf:type ex:Concept . ex:b rdf:type other:Concept .",
    );
    assert_eq!(
        labels(&g, "http://example.org/a"),
        ["ex_Concept".to_string()].into()
    );
    assert_eq!(
        labels(&g, "http://example.org/b"),
        ["other_Concept".to_string()].into()
    );
    assert_eq!(r.name_clashes.len(), 1, "{:?}", r.name_clashes);
    assert!(r.name_clashes[0].starts_with("Concept:"));
}

#[test]
fn a_literal_is_a_property_keyed_by_local_name_or_table_with_clashes_prefixed() {
    let (g, _) = map("", "ex:a skos:prefLabel \"Graph\" .");
    assert_eq!(
        prop(&g, "http://example.org/a", "prefLabel"),
        Some(s("Graph"))
    );

    let (g, _) = map(
        "properties: { \"skos:prefLabel\": title }",
        "ex:a skos:prefLabel \"Graph\" .",
    );
    assert_eq!(prop(&g, "http://example.org/a", "title"), Some(s("Graph")));

    let (g, r) = map(
        "prefixes: { ex: \"http://example.org/\", other: \"http://other.org/ns#\" }",
        "ex:a ex:label \"one\" ; other:label \"two\" .",
    );
    assert_eq!(prop(&g, "http://example.org/a", "ex_label"), Some(s("one")));
    assert_eq!(
        prop(&g, "http://example.org/a", "other_label"),
        Some(s("two"))
    );
    assert_eq!(r.name_clashes.len(), 1);
}

#[test]
fn an_iri_object_is_an_edge_upper_cased_or_from_the_table_and_reversible() {
    let fixture = "ex:a rdfs:subClassOf ex:b . ex:a skos:narrower ex:c .";
    let (g, r) = map("", fixture);
    assert_eq!(r.edges_created, 2);
    let e = edges(&g);
    assert!(e.contains(&(
        "http://example.org/a".into(),
        "SUB_CLASS_OF".into(),
        "http://example.org/b".into()
    )));
    assert!(e.contains(&(
        "http://example.org/a".into(),
        "NARROWER".into(),
        "http://example.org/c".into()
    )));

    let (g, _) = map(
        "edges: { \"skos:narrower\": { type: BROADER, reverse: true }, \"rdfs:subClassOf\": ISA }",
        fixture,
    );
    let e = edges(&g);
    assert!(e.contains(&(
        "http://example.org/c".into(),
        "BROADER".into(),
        "http://example.org/a".into()
    )));
    assert!(e.contains(&(
        "http://example.org/a".into(),
        "ISA".into(),
        "http://example.org/b".into()
    )));
}

#[test]
fn a_multi_valued_property_is_a_list_the_first_value_or_an_error() {
    let fixture = "ex:a skos:altLabel \"x\" , \"y\" .";
    let (g, _) = map("", fixture);
    match prop(&g, "http://example.org/a", "altLabel") {
        Some(PropertyValue::Array(v)) => {
            let mut v: Vec<_> = v.iter().map(|x| x.to_string()).collect();
            v.sort();
            assert_eq!(v.len(), 2);
        }
        other => panic!("{other:?}"),
    }

    let (g, r) = map("multi_valued: first", fixture);
    assert!(matches!(
        prop(&g, "http://example.org/a", "altLabel"),
        Some(PropertyValue::String(_))
    ));
    assert_eq!(r.extra_values_dropped, 1);
    assert!(!r.lossless());

    let mapping = Mapping::from_yaml("multi_valued: error").unwrap();
    let err = mapping
        .apply(&store(fixture), &mut GraphStore::new())
        .unwrap_err();
    assert!(
        matches!(err, MappingFileError::MultiValued { ref key, .. } if key == "altLabel"),
        "{err}"
    );
}

#[test]
fn language_tags_go_to_suffixed_keys_a_map_or_are_dropped() {
    let fixture = "ex:a skos:prefLabel \"Graph\"@en , \"Graph-de\"@de .";
    let (g, r) = map("", fixture);
    assert_eq!(
        prop(&g, "http://example.org/a", "prefLabel"),
        Some(s("Graph"))
    );
    assert_eq!(
        prop(&g, "http://example.org/a", "prefLabel@de"),
        Some(s("Graph-de"))
    );
    assert_eq!(r.languages_suffixed, 1);
    assert!(r.lossless());

    let (g, _) = map("languages: { prefer: de }", fixture);
    assert_eq!(
        prop(&g, "http://example.org/a", "prefLabel"),
        Some(s("Graph-de"))
    );
    assert_eq!(
        prop(&g, "http://example.org/a", "prefLabel@en"),
        Some(s("Graph"))
    );

    let (g, _) = map("languages: { others: map }", fixture);
    match prop(&g, "http://example.org/a", "prefLabel") {
        Some(PropertyValue::Map(m)) => {
            assert_eq!(m.get("en"), Some(&s("Graph")));
            assert_eq!(m.get("de"), Some(&s("Graph-de")));
        }
        other => panic!("{other:?}"),
    }

    let (g, r) = map("languages: { others: drop }", fixture);
    assert_eq!(
        prop(&g, "http://example.org/a", "prefLabel"),
        Some(s("Graph"))
    );
    assert_eq!(prop(&g, "http://example.org/a", "prefLabel@de"), None);
    assert_eq!(r.languages_dropped, 1);
    assert!(!r.lossless());
}

#[test]
fn typed_literals_become_typed_values_and_what_cannot_is_reported() {
    let fixture = r#"ex:a ex:n "42"^^xsd:integer ; ex:f "2.5"^^xsd:double ;
        ex:b "true"^^xsd:boolean ; ex:d "1.10"^^xsd:decimal ;
        ex:day "2024-02-29"^^xsd:date ;
        ex:local "2024-02-29T10:30:00"^^xsd:dateTime ;
        ex:zoned "2024-02-29T10:30:00+05:30"^^xsd:dateTime ;
        ex:odd "x"^^ex:customType ; ex:bad "nope"^^xsd:integer ."#;
    let (g, r) = map("", fixture);
    let a = "http://example.org/a";
    assert_eq!(prop(&g, a, "n"), Some(PropertyValue::Integer(42)));
    assert_eq!(prop(&g, a, "f"), Some(PropertyValue::Float(2.5)));
    assert_eq!(prop(&g, a, "b"), Some(PropertyValue::Boolean(true)));
    assert_eq!(prop(&g, a, "d"), Some(PropertyValue::Float(1.1)));
    // 2024-02-29 is day 19782 since the epoch.
    assert_eq!(prop(&g, a, "day"), Some(PropertyValue::Date(19782)));
    let local_secs = 19782 * 86_400 + 10 * 3600 + 30 * 60;
    assert_eq!(
        prop(&g, a, "local"),
        Some(PropertyValue::LocalDateTime {
            secs: local_secs,
            nanos: 0
        })
    );
    assert_eq!(
        prop(&g, a, "zoned"),
        Some(PropertyValue::ZonedDateTime {
            secs: local_secs - 19_800,
            nanos: 0,
            offset_seconds: 19_800,
            zone: None
        })
    );
    assert_eq!(prop(&g, a, "odd"), Some(s("x")));
    assert_eq!(prop(&g, a, "bad"), Some(s("nope")));
    assert_eq!(r.decimals_as_float, 1);
    assert_eq!(r.literals_as_string, 2);

    let (g, r) = map("typed_literals: false", fixture);
    assert_eq!(prop(&g, a, "n"), Some(s("42")));
    assert_eq!(r.literals_as_string, 0);
}

#[test]
fn a_blank_node_is_a_node_a_map_or_dropped() {
    let fixture = "ex:a ex:address [ ex:city \"Pune\" ; ex:zip \"411001\" ] .";

    let (g, r) = map("", fixture);
    assert_eq!(r.blank_nodes_as_nodes, 1);
    let e = edges(&g);
    assert_eq!(e.len(), 1);
    let (_, ty, target) = e.into_iter().next().unwrap();
    assert_eq!(ty, "ADDRESS");
    assert!(
        target.starts_with("https://samyama.ai/rdf/blank/"),
        "{target}"
    );
    assert_eq!(prop(&g, &target, "city"), Some(s("Pune")));

    let (g, r) = map("blank_nodes: inline", fixture);
    assert_eq!(r.blank_nodes_inlined, 1);
    assert_eq!(g.all_nodes().len(), 1);
    match prop(&g, "http://example.org/a", "address") {
        Some(PropertyValue::Map(m)) => {
            assert_eq!(m.get("city"), Some(&s("Pune")));
            assert_eq!(m.get("zip"), Some(&s("411001")));
        }
        other => panic!("{other:?}"),
    }
    assert!(r.lossless());

    // `ex:a` is still a node; only the blank node and its triples go.
    let (g, r) = map("blank_nodes: drop", fixture);
    assert_eq!(g.all_nodes().len(), 1);
    assert_eq!(prop(&g, "http://example.org/a", "address"), None);
    assert_eq!(r.blank_node_triples_dropped, 3);
    assert!(!r.lossless());
}

#[test]
fn base_names_the_iri_given_to_blank_nodes() {
    let (g, _) = map(
        "base: \"https://dl.acm.org/ccs2012/\"",
        "ex:a ex:part [ ex:name \"p\" ] .",
    );
    let (_, _, target) = edges(&g).into_iter().next().unwrap();
    assert!(
        target.starts_with("https://dl.acm.org/ccs2012/blank/"),
        "{target}"
    );
}

#[test]
fn merge_folds_linked_subjects_into_one_node_keeping_the_others_as_aliases() {
    let yaml = "prefixes: { ex: \"http://example.org/\" }\n\
                merge: { predicate: \"ex:preferred\", into: object, aliases_property: altIri }";
    let fixture = "ex:graphdb ex:preferred ex:graph_database . ex:gdb ex:preferred ex:graphdb .
        ex:graph_database ex:name \"graph database\" .
        ex:graphdb ex:broader ex:databases . ex:gdb ex:broader ex:databases .";
    let (g, r) = map(yaml, fixture);
    // graphdb -> graph_database and gdb -> graphdb -> graph_database.
    assert_eq!(r.aliases_merged, 2);
    assert_eq!(g.all_nodes().len(), 2, "graph_database and databases");
    assert_eq!(
        prop(&g, "http://example.org/graph_database", "altIri"),
        Some(PropertyValue::Array(vec![
            s("http://example.org/gdb"),
            s("http://example.org/graphdb")
        ]))
    );
    // Both aliases said `broader databases`: one edge, the repeat counted.
    assert_eq!(
        edges(&g),
        [(
            "http://example.org/graph_database".to_string(),
            "BROADER".to_string(),
            "http://example.org/databases".to_string()
        )]
        .into()
    );
    assert_eq!(r.duplicate_edges_collapsed, 1);
}

#[test]
fn drop_lists_predicates_that_are_not_stored_and_reports_them() {
    let (g, r) = map(
        "drop: [ \"skos:inScheme\" ]",
        "ex:a skos:inScheme ex:scheme ; skos:prefLabel \"A\" .",
    );
    assert_eq!(g.all_nodes().len(), 1);
    assert_eq!(
        r.predicates_dropped
            .get("http://www.w3.org/2004/02/skos/core#inScheme"),
        Some(&1)
    );
    assert!(r
        .dropped()
        .iter()
        .any(|d| d.what == "rdf_predicates_dropped"));
}

#[test]
fn the_skos_preset_maps_concepts_labels_and_the_broader_tree() {
    let fixture = r#"ex:scheme rdf:type skos:ConceptScheme .
        ex:db rdf:type skos:Concept ; skos:prefLabel "Databases"@en ; skos:inScheme ex:scheme .
        ex:graph rdf:type skos:Concept ; skos:prefLabel "Graph databases"@en ;
            skos:altLabel "GDB" ; skos:broader ex:db ; skos:related ex:rdf .
        ex:rdf rdf:type skos:Concept ; skos:narrower ex:triples .
        ex:triples rdf:type skos:Concept ."#;
    let (g, r) = map("preset: skos", fixture);
    assert_eq!(
        labels(&g, "http://example.org/db"),
        ["Concept".to_string()].into()
    );
    assert_eq!(
        prop(&g, "http://example.org/graph", "prefLabel"),
        Some(s("Graph databases"))
    );
    assert_eq!(
        prop(&g, "http://example.org/graph", "altLabel"),
        Some(s("GDB"))
    );
    let e = edges(&g);
    let ex = |x: &str| format!("http://example.org/{x}");
    assert!(e.contains(&(ex("graph"), "BROADER".into(), ex("db"))));
    assert!(e.contains(&(ex("graph"), "RELATED".into(), ex("rdf"))));
    // `narrower` is BROADER the other way round.
    assert!(e.contains(&(ex("triples"), "BROADER".into(), ex("rdf"))));
    assert_eq!(
        r.predicates_dropped
            .get("http://www.w3.org/2004/02/skos/core#inScheme"),
        Some(&1)
    );
}

#[test]
fn the_owl_preset_maps_classes_properties_and_inlines_restrictions() {
    let fixture = r#"ex:Animal rdf:type owl:Class ; rdfs:label "Animal" .
        ex:Dog rdf:type owl:Class ; rdfs:subClassOf ex:Animal ;
            rdfs:subClassOf [ rdf:type owl:Restriction ; owl:onProperty ex:hasOwner ;
                              owl:someValuesFrom ex:Person ] .
        ex:hasOwner rdf:type owl:ObjectProperty ; rdfs:domain ex:Dog ; rdfs:range ex:Person .
        ex:Person rdf:type owl:Class ."#;
    let (g, r) = map("preset: owl", fixture);
    let ex = |x: &str| format!("http://example.org/{x}");
    assert_eq!(labels(&g, &ex("Dog")), ["Class".to_string()].into());
    assert_eq!(labels(&g, &ex("hasOwner")), ["Property".to_string()].into());
    assert_eq!(prop(&g, &ex("Animal"), "label"), Some(s("Animal")));
    let e = edges(&g);
    assert!(e.contains(&(ex("Dog"), "SUBCLASS_OF".into(), ex("Animal"))));
    assert!(e.contains(&(ex("hasOwner"), "DOMAIN".into(), ex("Dog"))));
    assert!(e.contains(&(ex("hasOwner"), "RANGE".into(), ex("Person"))));
    assert_eq!(r.blank_nodes_inlined, 1);
    // The restriction is a map on Dog under the subClassOf key.
    let restriction = prop(&g, &ex("Dog"), "SUBCLASS_OF").or(prop(&g, &ex("Dog"), "subClassOf"));
    match restriction {
        Some(PropertyValue::Map(m)) => {
            assert_eq!(m.get("onProperty"), Some(&s("http://example.org/hasOwner")));
        }
        other => panic!("{other:?}"),
    }
}

/// The `samyama` preset is the engine's mapper, so reading Samyama's own RDF
/// export gives the same graph through either.
#[test]
fn the_samyama_preset_reproduces_the_engine_mapper() {
    let mut original = GraphStore::new();
    let a = original.create_node("Person");
    original
        .set_node_property("default", a, "name", "Ada")
        .unwrap();
    original
        .set_node_property("default", a, "born", 1815i64)
        .unwrap();
    let b = original.create_node("Engine");
    original.create_edge(a, b, "DESIGNED").unwrap();
    let mut rdf = RdfStore::new();
    samyama::rdf::GraphToRdfMapper::new("https://samyama.ai/rdf/")
        .sync_to_rdf(&original, &mut rdf)
        .unwrap();

    let mut via_engine = GraphStore::new();
    let engine = samyama::rdf::RdfToGraphMapper::new("https://samyama.ai/rdf/")
        .map_to_graph(&rdf, &mut via_engine)
        .unwrap();
    let mut via_preset = GraphStore::new();
    let preset = Mapping::preset("samyama")
        .unwrap()
        .apply(&rdf, &mut via_preset)
        .unwrap();

    let shape = |g: &GraphStore| {
        let mut nodes: Vec<String> = g
            .all_nodes()
            .iter()
            .map(|n| {
                let mut l: Vec<_> = n.labels.iter().map(|l| l.as_str().to_string()).collect();
                l.sort();
                let mut p: Vec<_> = g
                    .node_properties_merged(n.id)
                    .into_iter()
                    .map(|(k, v)| format!("{k}={v:?}"))
                    .collect();
                p.sort();
                format!("{l:?} {p:?}")
            })
            .collect();
        nodes.sort();
        (nodes, edges(g))
    };
    assert_eq!(shape(&via_preset), shape(&via_engine));
    assert_eq!(preset.nodes_created, engine.nodes_created);
    assert_eq!(preset.edges_created, engine.edges_created);
    assert!(preset.lossless());
}

#[test]
fn the_samyama_preset_takes_only_base() {
    assert!(Mapping::from_yaml("preset: samyama\nbase: \"https://x.org/\"").is_ok());
    let err = Mapping::from_yaml("preset: samyama\nmulti_valued: first").unwrap_err();
    assert!(
        matches!(err, MappingFileError::SamyamaOverrides(_)),
        "{err}"
    );
}

#[test]
fn the_resolved_mapping_expands_prefixes_for_the_snapshot_header() {
    let m = Mapping::from_yaml(
        "preset: skos\nprefixes: { cso: \"http://cso.kmi.open.ac.uk/schema/cso#\" }\n\
         labels: { \"cso:Topic\": Concept }",
    )
    .unwrap();
    let json = m.to_json();
    assert_eq!(json["preset"], "skos");
    assert_eq!(
        json["labels"]["http://cso.kmi.open.ac.uk/schema/cso#Topic"],
        "Concept"
    );
    assert_eq!(
        json["edges"]["http://www.w3.org/2004/02/skos/core#narrower"]["reverse"],
        true
    );
    assert!(json
        .to_string()
        .contains("http://www.w3.org/2004/02/skos/core#inScheme"));
}

#[test]
fn a_bad_mapping_file_is_refused_with_a_reason() {
    assert!(matches!(
        Mapping::from_yaml("preset: nope"),
        Err(MappingFileError::UnknownPreset(_))
    ));
    assert!(matches!(
        Mapping::from_yaml("labels: { \"nope:Thing\": Thing }"),
        Err(MappingFileError::UnknownPrefix(_))
    ));
    assert!(matches!(
        Mapping::from_yaml("lables: {}"),
        Err(MappingFileError::Yaml(_))
    ));
}

#[test]
fn every_preset_and_the_cso_example_resolve() {
    for p in samyama::convert::rdf_mapping::PRESETS {
        Mapping::preset(p).unwrap_or_else(|e| panic!("{p}: {e}"));
    }
    let cso = Mapping::from_path(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/convert/presets/examples/cso.yaml"
    )))
    .unwrap();
    let rule = cso.merge.as_ref().unwrap();
    assert_eq!(
        rule.predicate,
        "http://cso.kmi.open.ac.uk/schema/cso#preferentialEquivalent"
    );
    assert!(
        cso.edges["http://cso.kmi.open.ac.uk/schema/cso#superTopicOf"].reverse,
        "superTopicOf is BROADER the other way round"
    );
}

/// A language map keeps every value: several in one language go through
/// `multi_valued`, and untagged ones sit under `@none` instead of being
/// replaced by the map (review on #1952).
#[test]
fn a_language_map_keeps_repeated_and_untagged_values() {
    let fixture = r#"ex:a skos:prefLabel "Graph"@en , "Graphs"@en , "plain" , "Graphe"@fr ."#;
    let (g, r) = map("languages: { others: map }", fixture);
    match prop(&g, "http://example.org/a", "prefLabel") {
        Some(PropertyValue::Map(m)) => {
            match m.get("en") {
                Some(PropertyValue::Array(v)) => assert_eq!(v.len(), 2, "{v:?}"),
                other => panic!("en: {other:?}"),
            }
            assert_eq!(m.get("fr"), Some(&s("Graphe")));
            assert_eq!(m.get("@none"), Some(&s("plain")));
        }
        other => panic!("{other:?}"),
    }
    assert!(r.lossless(), "{:?}", r.dropped());

    let mapping = Mapping::from_yaml("languages: { others: map }\nmulti_valued: error").unwrap();
    let err = mapping
        .apply(&store(fixture), &mut GraphStore::new())
        .unwrap_err();
    assert!(
        matches!(err, MappingFileError::MultiValued { ref key, .. } if key == "prefLabel@en"),
        "{err}"
    );
}

/// An aliases key that a predicate also fills on the same node is refused,
/// not resolved by one silently replacing the other (review on #1952).
#[test]
fn an_aliases_key_a_predicate_also_fills_is_refused() {
    let yaml = "prefixes: { ex: \"http://example.org/\" }\n\
                merge: { predicate: \"ex:preferred\", into: object, aliases_property: altLabel }";
    let fixture = r#"ex:gdb ex:preferred ex:graph . ex:graph skos:altLabel "graph store" ."#;
    let err = Mapping::from_yaml(yaml)
        .unwrap()
        .apply(&store(fixture), &mut GraphStore::new())
        .unwrap_err();
    assert!(
        matches!(err, MappingFileError::AliasesConflict { ref property, .. } if property == "altLabel"),
        "{err}"
    );

    // A distinct key keeps both.
    let yaml = "prefixes: { ex: \"http://example.org/\" }\n\
                merge: { predicate: \"ex:preferred\", into: object, aliases_property: aliases }";
    let (g, _) = map(yaml, fixture);
    assert_eq!(
        prop(&g, "http://example.org/graph", "altLabel"),
        Some(s("graph store"))
    );
    assert_eq!(
        prop(&g, "http://example.org/graph", "aliases"),
        Some(PropertyValue::Array(vec![s("http://example.org/gdb")]))
    );
}

/// An inlined blank node follows the same rules as a node: `drop`, language
/// tags and `multi_valued` (review on #1952).
#[test]
fn an_inlined_blank_node_follows_drop_languages_and_multi_valued() {
    let fixture =
        r#"ex:a ex:part [ skos:inScheme ex:s ; skos:prefLabel "x"@en , "y"@de ; ex:n 1 , 2 ] ."#;
    let (g, r) = map("blank_nodes: inline\ndrop: [ \"skos:inScheme\" ]", fixture);
    match prop(&g, "http://example.org/a", "part") {
        Some(PropertyValue::Map(m)) => {
            assert!(!m.contains_key("inScheme"), "{m:?}");
            assert_eq!(m.get("prefLabel"), Some(&s("x")));
            assert_eq!(m.get("prefLabel@de"), Some(&s("y")));
            assert!(
                matches!(m.get("n"), Some(PropertyValue::Array(v)) if v.len() == 2),
                "{m:?}"
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        r.predicates_dropped
            .get("http://www.w3.org/2004/02/skos/core#inScheme"),
        Some(&1)
    );

    let mapping = Mapping::from_yaml("blank_nodes: inline\nmulti_valued: error").unwrap();
    let err = mapping
        .apply(&store(fixture), &mut GraphStore::new())
        .unwrap_err();
    assert!(matches!(err, MappingFileError::MultiValued { .. }), "{err}");
}

/// A blank node two parents share is inlined under both; one that refers
/// back to itself is a cycle, written as null there and reported (review on
/// #1952).
#[test]
fn a_shared_blank_node_is_inlined_twice_and_a_cycle_is_reported() {
    let shared = "ex:a ex:left _:c ; ex:right _:c . _:c ex:v \"1\" .";
    let (g, r) = map("blank_nodes: inline", shared);
    for side in ["left", "right"] {
        match prop(&g, "http://example.org/a", side) {
            Some(PropertyValue::Map(m)) => assert_eq!(m.get("v"), Some(&s("1")), "{side}"),
            other => panic!("{side}: {other:?}"),
        }
    }
    assert_eq!(r.blank_node_cycles, 0);

    let cyclic = "ex:a ex:p _:x . _:x ex:q _:y . _:y ex:r _:x .";
    let (_, r) = map("blank_nodes: inline", cyclic);
    assert_eq!(r.blank_node_cycles, 1);
    assert!(r
        .dropped()
        .iter()
        .any(|d| d.what == "rdf_blank_node_cycles"));
}
