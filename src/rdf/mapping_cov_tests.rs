//! Unit tests for each mapping decision, both directions.

use super::*;
use crate::graph::{Label, NodeId};
use crate::rdf::mapping::{GraphToRdfMapper, RdfToGraphMapper};
use crate::rdf::BlankNode;

const BASE: &str = "https://ex.org/";

fn cfg() -> MappingConfig {
    MappingConfig::new(BASE)
}

fn nn(s: &str) -> NamedNode {
    NamedNode::new(s).unwrap()
}

fn t(s: &str, p: &str, o: RdfObject) -> Triple {
    Triple::new(
        RdfSubject::NamedNode(nn(s)),
        RdfPredicate::new(p).unwrap(),
        o,
    )
}

fn lit(v: &str) -> RdfObject {
    RdfObject::Literal(Literal::new_simple_literal(v))
}

fn strings(ts: &[Triple]) -> Vec<String> {
    ts.iter().map(|t| t.to_string()).collect()
}

// ------------------------------------------------------------ segments

#[test]
fn escape_and_decode_segments_round_trip() {
    assert_eq!(escape_segment("Plain-Name_1.x~"), "Plain-Name_1.x~");
    assert_eq!(escape_segment("a b/c"), "a%20b%2Fc");
    assert_eq!(escape_segment("é"), "%C3%A9");
    for s in ["a b/c", "é ü", "100%"] {
        assert_eq!(decode_segment(&escape_segment(s)), s);
    }
}

#[test]
fn decode_segment_keeps_malformed_escapes_literally() {
    assert_eq!(decode_segment("%zz"), "%zz");
    assert_eq!(decode_segment("a%4"), "a%4", "truncated escape at the end");
    assert_eq!(decode_segment("%41%"), "A%");
    // Decodes to bytes that are not UTF-8: the input is kept as written.
    assert_eq!(decode_segment("%FF%FEx"), "%FF%FEx");
}

// ------------------------------------------------------------ literals

#[test]
fn literal_for_types_scalars_and_json_encodes_the_rest() {
    let int = literal_for(&PropertyValue::Integer(30)).unwrap();
    assert_eq!(
        (int.value(), int.datatype().as_str()),
        ("30", "http://www.w3.org/2001/XMLSchema#integer")
    );
    let f = literal_for(&PropertyValue::Float(1.0)).unwrap();
    assert_eq!(
        (f.value(), f.datatype().as_str()),
        ("1.0", "http://www.w3.org/2001/XMLSchema#double")
    );
    let b = literal_for(&PropertyValue::Boolean(true)).unwrap();
    assert_eq!(
        b.datatype().as_str(),
        "http://www.w3.org/2001/XMLSchema#boolean"
    );
    let s = literal_for(&PropertyValue::String("x".into())).unwrap();
    assert_eq!(
        s.datatype().as_str(),
        "http://www.w3.org/2001/XMLSchema#string"
    );
    let arr = PropertyValue::Array(vec![PropertyValue::Integer(1)]);
    let j = literal_for(&arr).unwrap();
    assert_eq!(
        j.datatype().as_str(),
        "https://samyama.ai/rdf/PropertyValue"
    );

    for v in [
        PropertyValue::Integer(-4),
        PropertyValue::Float(2.5),
        PropertyValue::Boolean(false),
        PropertyValue::String("s".into()),
        arr,
        PropertyValue::Date(3),
    ] {
        assert_eq!(value_for(&literal_for(&v).unwrap()), v);
    }
}

#[test]
fn value_for_falls_back_to_text_when_it_cannot_parse() {
    let typed = |v: &str, t: &str| Literal::new_typed_literal(v, nn(t));
    let xsd = "http://www.w3.org/2001/XMLSchema#";
    for t in ["integer", "double", "boolean"] {
        assert_eq!(
            value_for(&typed("oops", &format!("{xsd}{t}"))),
            PropertyValue::String("oops".into()),
            "{t}"
        );
    }
    assert_eq!(
        value_for(&typed("{bad json", "https://samyama.ai/rdf/PropertyValue")),
        PropertyValue::String("{bad json".into())
    );
    assert_eq!(
        value_for(&typed("2020-01-01", &format!("{xsd}date"))),
        PropertyValue::String("2020-01-01".into())
    );
    let lang = Literal::new_language_tagged_literal("chat", "fr").unwrap();
    assert_eq!(value_for(&lang), PropertyValue::String("chat".into()));
}

// ------------------------------------------------------------ export

#[test]
fn node_iri_prefers_the_stored_iri_and_refuses_an_invalid_one() {
    let mut g = GraphStore::new();
    let plain = g.create_node("A");
    let kept = g.create_node("A");
    g.set_node_property("default", kept, IRI_PROPERTY, "http://orig/x")
        .unwrap();
    let bad = g.create_node("A");
    g.set_node_property("default", bad, IRI_PROPERTY, "not an iri")
        .unwrap();

    let n = |id| g.get_node(id).unwrap();
    assert_eq!(
        node_iri(&cfg(), &g, n(plain)).unwrap().as_str(),
        format!("{BASE}node/{}", plain.as_u64())
    );
    assert_eq!(
        node_iri(&cfg(), &g, n(kept)).unwrap().as_str(),
        "http://orig/x"
    );
    assert!(
        matches!(node_iri(&cfg(), &g, n(bad)), Err(MappingError::InvalidIri(ref s)) if s == "not an iri")
    );
}

#[test]
fn map_node_sorts_labels_and_properties_and_hides_the_iri_property() {
    let mut g = GraphStore::new();
    let id = g.create_node_with_labels([Label::new("Zed Label"), Label::new("Alpha")]);
    g.set_node_property("default", id, "b key", 2i64).unwrap();
    g.set_node_property("default", id, "a", "x").unwrap();
    g.set_node_property("default", id, IRI_PROPERTY, "http://orig/n")
        .unwrap();
    let triples = map_node(&cfg(), &g, g.get_node(id).unwrap()).unwrap();
    let text = strings(&triples);
    assert_eq!(triples.len(), 4, "{text:?}");
    assert!(text[0].contains("class/Alpha"), "{text:?}");
    assert!(text[1].contains("class/Zed%20Label"), "{text:?}");
    assert!(text[2].contains("prop/a>"), "{text:?}");
    assert!(text[3].contains("prop/b%20key>"), "{text:?}");
    assert!(text.iter().all(|t| t.starts_with("<http://orig/n>")));
}

#[test]
fn map_edge_reifies_properties_only_when_asked_and_present() {
    let mut g = GraphStore::new();
    let a = g.create_node("A");
    let b = g.create_node("B");
    let bare = g.create_edge(a, b, "KNOWS").unwrap();
    let rich = g.create_edge(a, b, "RATED IT").unwrap();
    g.set_edge_property(rich, "stars", 5i64).unwrap();
    g.set_edge_property(rich, "note", "ok").unwrap();

    let edge = |id| g.get_edge(id).unwrap();
    assert_eq!(map_edge(&cfg(), &g, &edge(bare)).unwrap().len(), 1);

    let triples = map_edge(&cfg(), &g, &edge(rich)).unwrap();
    let text = strings(&triples);
    assert_eq!(
        triples.len(),
        7,
        "base + 4 statement triples + 2 props: {text:?}"
    );
    assert!(text[0].contains("rel/RATED%20IT"));
    let stmt = format!("<{BASE}statement/{}>", rich.as_u64());
    assert!(text[1..].iter().all(|t| t.starts_with(&stmt)), "{text:?}");
    assert!(
        text[5].contains("prop/note") && text[6].contains("prop/stars"),
        "{text:?}"
    );

    let mut no_reif = cfg();
    no_reif.use_reification = false;
    assert_eq!(map_edge(&no_reif, &g, &edge(rich)).unwrap().len(), 1);
}

#[test]
fn map_edge_with_a_missing_endpoint_is_an_error() {
    let mut g = GraphStore::new();
    let a = g.create_node("A");
    let b = g.create_node("B");
    let e = g.create_edge(a, b, "R").unwrap();
    let edge = g.get_edge(e).unwrap();
    let err = map_edge(&cfg(), &GraphStore::new(), &edge).unwrap_err();
    assert!(err.to_string().contains("is not in the graph"), "{err}");
}

#[test]
fn sync_counts_written_and_collapsed_triples() {
    let mut g = GraphStore::new();
    let a = g.create_node("A");
    let b = g.create_node("B");
    g.create_edge(a, b, "R").unwrap();
    g.create_edge(a, b, "R").unwrap(); // parallel: collapses
    let mut rdf = RdfStore::new();
    let rep = GraphToRdfMapper::with_config(cfg())
        .sync_to_rdf(&g, &mut rdf)
        .unwrap();
    assert_eq!(
        rep,
        SyncReport {
            triples_written: 3,
            duplicates_collapsed: 1
        }
    );
    assert_eq!(rdf.len(), 3);
}

#[test]
fn sync_propagates_mapping_errors() {
    let mut g = GraphStore::new();
    let n = g.create_node("A");
    g.set_node_property("default", n, IRI_PROPERTY, "bad iri")
        .unwrap();
    let mut rdf = RdfStore::new();
    assert!(matches!(
        sync_to_rdf(&cfg(), &g, &mut rdf),
        Err(MappingError::InvalidIri(_))
    ));
}

// ------------------------------------------------------------ import

fn rdf_of(triples: Vec<Triple>) -> RdfStore {
    let mut s = RdfStore::new();
    for t in triples {
        s.insert(t).unwrap();
    }
    s
}

fn by_iri(g: &GraphStore, iri: &str) -> NodeId {
    g.all_nodes()
        .into_iter()
        .find(|n| {
            g.node_properties_merged(n.id).get(IRI_PROPERTY)
                == Some(&PropertyValue::String(iri.into()))
        })
        .map(|n| n.id)
        .unwrap_or_else(|| panic!("no node for {iri}"))
}

fn labels(g: &GraphStore, id: NodeId) -> Vec<String> {
    let mut l: Vec<String> = g
        .get_node(id)
        .unwrap()
        .labels
        .iter()
        .map(|l| l.as_str().to_string())
        .collect();
    l.sort();
    l
}

#[test]
fn import_labels_properties_and_edges() {
    let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let rdf = rdf_of(vec![
        t(
            "http://x/a",
            rdf_type,
            RdfObject::NamedNode(nn(&format!("{BASE}class/Big%20Cat"))),
        ),
        t(
            "http://x/a",
            rdf_type,
            RdfObject::NamedNode(nn("http://schema.org/Thing")),
        ),
        t("http://x/a", rdf_type, lit("not a class")),
        t("http://x/a", &format!("{BASE}prop/full%20name"), lit("Ann")),
        t(
            "http://x/a",
            &format!("{BASE}prop/ignored"),
            RdfObject::NamedNode(nn("http://x/iri")),
        ),
        t(
            "http://x/a",
            &format!("{BASE}rel/LIKES"),
            RdfObject::NamedNode(nn("http://x/b")),
        ),
        t("http://x/a", &format!("{BASE}rel/SKIP"), lit("not a node")),
        t("http://x/a", "http://other/pred", lit("foreign predicate")),
        // A relationship to a statement resource, which is never a node.
        t(
            "http://x/a",
            &format!("{BASE}rel/META"),
            RdfObject::NamedNode(nn(&format!("{BASE}statement/9"))),
        ),
        Triple::new(
            RdfSubject::BlankNode(BlankNode::from_str("typed").unwrap()),
            RdfPredicate::new(rdf_type).unwrap(),
            RdfObject::NamedNode(nn("http://schema.org/Thing")),
        ),
        Triple::new(
            RdfSubject::BlankNode(BlankNode::from_str("bn").unwrap()),
            RdfPredicate::new(&format!("{BASE}prop/x")).unwrap(),
            lit("blank subjects are skipped"),
        ),
    ]);
    let mut g = GraphStore::new();
    RdfToGraphMapper::new(BASE)
        .map_to_graph(&rdf, &mut g)
        .unwrap();

    assert_eq!(g.node_count(), 2, "a and the edge target b");
    let a = by_iri(&g, "http://x/a");
    let b = by_iri(&g, "http://x/b");
    assert_eq!(
        labels(&g, a),
        vec!["Big Cat".to_string(), "http://schema.org/Thing".to_string()]
    );
    assert_eq!(
        labels(&g, b),
        vec!["Resource".to_string()],
        "untyped nodes get the placeholder"
    );
    let props = g.node_properties_merged(a);
    assert_eq!(
        props.get("full name"),
        Some(&PropertyValue::String("Ann".into()))
    );
    assert!(props.get("ignored").is_none());
    let out = g.get_outgoing_edges(a);
    assert_eq!(out.len(), 1);
    assert_eq!((out[0].edge_type.as_str(), out[0].target), ("LIKES", b));
}

#[test]
fn import_restores_reified_edge_properties_and_ignores_broken_statements() {
    let rdf_ns = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    let stmt = |n: &str| format!("{BASE}statement/{n}");
    let rel = format!("{BASE}rel/R");
    let node = |x: &str| RdfObject::NamedNode(nn(&format!("http://x/{x}")));
    let mut triples = vec![
        t("http://x/a", &rel, node("b")),
        // A complete statement.
        t(&stmt("1"), &format!("{rdf_ns}subject"), node("a")),
        t(
            &stmt("1"),
            &format!("{rdf_ns}predicate"),
            RdfObject::NamedNode(nn(&rel)),
        ),
        t(&stmt("1"), &format!("{rdf_ns}object"), node("b")),
        t(
            &stmt("1"),
            &format!("{BASE}prop/w%20t"),
            RdfObject::Literal(literal_for(&PropertyValue::Integer(7)).unwrap()),
        ),
        t(&stmt("1"), "http://other/p", lit("not ours")),
        t(
            &stmt("1"),
            &format!("{rdf_ns}type"),
            RdfObject::NamedNode(nn(&format!("{rdf_ns}Statement"))),
        ),
        // Missing its object.
        t(&stmt("2"), &format!("{rdf_ns}subject"), node("a")),
        t(
            &stmt("2"),
            &format!("{rdf_ns}predicate"),
            RdfObject::NamedNode(nn(&rel)),
        ),
        t(&stmt("2"), &format!("{BASE}prop/lost"), lit("x")),
        // Names an endpoint that is not a node.
        t(&stmt("3"), &format!("{rdf_ns}subject"), node("a")),
        t(
            &stmt("3"),
            &format!("{rdf_ns}predicate"),
            RdfObject::NamedNode(nn(&rel)),
        ),
        t(&stmt("3"), &format!("{rdf_ns}object"), node("nowhere")),
        t(&stmt("3"), &format!("{BASE}prop/lost"), lit("x")),
        // A predicate that is not one of our relationships.
        t(&stmt("4"), &format!("{rdf_ns}subject"), node("a")),
        t(
            &stmt("4"),
            &format!("{rdf_ns}predicate"),
            RdfObject::NamedNode(nn("http://other/rel")),
        ),
        t(&stmt("4"), &format!("{rdf_ns}object"), node("b")),
        t(&stmt("4"), &format!("{BASE}prop/lost"), lit("x")),
        // An edge type with no such edge between the pair.
        t(&stmt("5"), &format!("{rdf_ns}subject"), node("b")),
        t(
            &stmt("5"),
            &format!("{rdf_ns}predicate"),
            RdfObject::NamedNode(nn(&rel)),
        ),
        t(&stmt("5"), &format!("{rdf_ns}object"), node("a")),
        t(&stmt("5"), &format!("{BASE}prop/lost"), lit("x")),
    ];
    triples.push(t("http://x/b", &format!("{BASE}prop/k"), lit("v")));
    let rdf = rdf_of(triples);
    let mut g = GraphStore::new();
    map_to_graph(&cfg(), &rdf, &mut g).unwrap();

    assert_eq!(g.node_count(), 2, "statement resources are not nodes");
    let a = by_iri(&g, "http://x/a");
    let edges = g.get_outgoing_edges(a);
    assert_eq!(edges.len(), 1);
    let props = g.edge_properties_merged(edges[0].id);
    assert_eq!(props.get("w t"), Some(&PropertyValue::Integer(7)));
    assert!(props.get("lost").is_none(), "{props:?}");
}

#[test]
fn round_trip_preserves_iris_labels_and_edge_properties() {
    let mut g = GraphStore::new();
    let a = g.create_node("Person");
    g.set_node_property("default", a, "age", 30i64).unwrap();
    let b = g.create_node("Person");
    let e = g.create_edge(a, b, "KNOWS").unwrap();
    g.set_edge_property(e, "since", 2020i64).unwrap();

    let mut rdf = RdfStore::new();
    GraphToRdfMapper::new(BASE)
        .sync_to_rdf(&g, &mut rdf)
        .unwrap();
    let mut back = GraphStore::new();
    RdfToGraphMapper::new(BASE)
        .map_to_graph(&rdf, &mut back)
        .unwrap();

    let a2 = by_iri(&back, &format!("{BASE}node/{}", a.as_u64()));
    assert_eq!(labels(&back, a2), vec!["Person".to_string()]);
    assert_eq!(
        back.node_properties_merged(a2).get("age"),
        Some(&PropertyValue::Integer(30))
    );
    let out = back.get_outgoing_edges(a2);
    assert_eq!(out.len(), 1);
    assert_eq!(
        back.edge_properties_merged(out[0].id).get("since"),
        Some(&PropertyValue::Integer(2020))
    );

    // Exporting the import again reproduces the same subject IRIs.
    let mut rdf2 = RdfStore::new();
    GraphToRdfMapper::new(BASE)
        .sync_to_rdf(&back, &mut rdf2)
        .unwrap();
    let subjects = |r: &RdfStore| {
        let mut v: Vec<String> = r
            .iter()
            .map(|t| t.subject.to_string())
            .filter(|s| s.contains("/node/"))
            .collect();
        v.sort();
        v.dedup();
        v
    };
    assert_eq!(subjects(&rdf), subjects(&rdf2));
}

#[test]
fn mapper_wrappers_and_error_messages() {
    let mut g = GraphStore::new();
    let a = g.create_node("A");
    let b = g.create_node("B");
    let e = g.create_edge(a, b, "R").unwrap();
    let m = GraphToRdfMapper::new(BASE);
    assert_eq!(m.map_edge(&g, &g.get_edge(e).unwrap()).unwrap().len(), 1);

    let c = cfg();
    assert!(c.use_reification && !c.preserve_blank_nodes);
    assert_eq!(MappingError::MissingBaseIri.to_string(), "Missing base IRI");
    assert_eq!(
        MappingError::InvalidIri("x".into()).to_string(),
        "Invalid IRI: x"
    );
    assert_eq!(
        MappingError::UnsupportedPropertyType("y".into()).to_string(),
        "Unsupported property type: y"
    );
    assert!(MappingError::NotImplemented("f")
        .to_string()
        .starts_with("f is not implemented"));
}
