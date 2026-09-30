//! Every term shape through every format, plus the file and error paths.

use super::*;
use crate::rdf::{BlankNode, Literal, NamedNode, RdfObject, RdfPredicate, RdfSubject};
use std::collections::HashSet;

const XSD_INT: &str = "http://www.w3.org/2001/XMLSchema#integer";

fn nn(s: &str) -> NamedNode {
    NamedNode::new(s).unwrap()
}

fn pred(s: &str) -> RdfPredicate {
    RdfPredicate::new(s).unwrap()
}

/// One triple of each object shape, with a blank-node subject in the mix.
fn all_shapes() -> Vec<Triple> {
    let s = RdfSubject::NamedNode(nn("http://example.org/s"));
    let b = RdfSubject::BlankNode(BlankNode::from_str("b1").unwrap());
    vec![
        Triple::new(
            s.clone(),
            pred("http://example.org/knows"),
            RdfObject::NamedNode(nn("http://example.org/o")),
        ),
        Triple::new(
            s.clone(),
            pred("http://example.org/friend"),
            RdfObject::BlankNode(BlankNode::from_str("b1").unwrap()),
        ),
        Triple::new(
            s.clone(),
            pred("http://example.org/name"),
            Literal::new_simple_literal("Ann").into(),
        ),
        Triple::new(
            s.clone(),
            pred("http://example.org/label"),
            Literal::new_language_tagged_literal("chat", "fr")
                .unwrap()
                .into(),
        ),
        Triple::new(
            s,
            pred("http://example.org/age"),
            Literal::new_typed_literal("42", nn(XSD_INT)).into(),
        ),
        Triple::new(
            b,
            pred("http://example.org/name"),
            Literal::new_simple_literal("Bee").into(),
        ),
    ]
}

/// Blank node labels are not preserved by a round trip, so compare with them
/// normalised.
fn normalised(ts: &[Triple]) -> HashSet<String> {
    ts.iter()
        .map(|t| {
            let s = if t.subject.is_blank_node() {
                "_:b".to_string()
            } else {
                t.subject.to_string()
            };
            let o = if t.object.is_blank_node() {
                "_:b".to_string()
            } else {
                t.object.to_string()
            };
            format!("{s} {} {o}", t.predicate)
        })
        .collect()
}

#[test]
fn turtle_ntriples_and_rdfxml_round_trip_every_term_shape() {
    let triples = all_shapes();
    for format in [RdfFormat::Turtle, RdfFormat::NTriples, RdfFormat::RdfXml] {
        let text = RdfSerializer::serialize(&triples, format).unwrap();
        let back =
            RdfParser::parse(&text, format).unwrap_or_else(|e| panic!("{format:?}: {e}\n{text}"));
        assert_eq!(
            normalised(&back),
            normalised(&triples),
            "{format:?}\n{text}"
        );
        let lit = back
            .iter()
            .find(|t| t.predicate.as_named_node().as_str().ends_with("label"))
            .unwrap();
        match &lit.object {
            RdfObject::Literal(l) => assert_eq!(l.language(), Some("fr"), "{format:?}"),
            other => panic!("{other:?}"),
        }
        let age = back
            .iter()
            .find(|t| t.predicate.as_named_node().as_str().ends_with("age"))
            .unwrap();
        match &age.object {
            RdfObject::Literal(l) => assert_eq!(l.datatype().as_str(), XSD_INT, "{format:?}"),
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn jsonld_serializes_every_term_shape_and_refuses_to_parse() {
    let text = RdfSerializer::serialize(&all_shapes(), RdfFormat::JsonLd).unwrap();
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    let nodes = v.as_array().unwrap();
    assert_eq!(nodes.len(), 2, "grouped by subject: {text}");
    let s = nodes
        .iter()
        .find(|n| n["@id"] == "http://example.org/s")
        .unwrap();
    assert_eq!(
        s["http://example.org/knows"][0]["@id"],
        "http://example.org/o"
    );
    assert_eq!(s["http://example.org/friend"][0]["@id"], "_:b1");
    assert_eq!(
        s["http://example.org/name"][0],
        serde_json::json!({"@value": "Ann"})
    );
    assert_eq!(
        s["http://example.org/label"][0],
        serde_json::json!({"@value": "chat", "@language": "fr"})
    );
    assert_eq!(
        s["http://example.org/age"][0],
        serde_json::json!({"@value": "42", "@type": XSD_INT})
    );
    let b = nodes
        .iter()
        .find(|n| n["@id"] != "http://example.org/s")
        .unwrap();
    assert_eq!(b["@id"], "_:b1");

    let err = RdfParser::parse("[]", RdfFormat::JsonLd).unwrap_err();
    assert!(
        matches!(err, ParseError::Parse(ref m) if m.contains("JSON-LD")),
        "{err:?}"
    );
}

#[test]
fn malformed_input_is_a_parse_error_in_each_format() {
    for (format, bad) in [
        (RdfFormat::Turtle, "<http://a> <http://b> ."),
        (RdfFormat::NTriples, "not a triple"),
        (
            RdfFormat::RdfXml,
            "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"><unclosed",
        ),
    ] {
        let err = RdfParser::parse(bad, format).unwrap_err();
        assert!(matches!(err, ParseError::Parse(_)), "{format:?}: {err:?}");
    }
}

#[test]
fn empty_input_parses_to_no_triples() {
    assert!(RdfParser::parse("", RdfFormat::Turtle).unwrap().is_empty());
    assert!(RdfParser::parse("", RdfFormat::NTriples)
        .unwrap()
        .is_empty());
    assert_eq!(
        RdfSerializer::serialize(&[], RdfFormat::JsonLd).unwrap(),
        "[]"
    );
}

#[test]
fn format_from_extension_is_case_insensitive_and_knows_aliases() {
    use std::path::Path;
    assert_eq!(
        RdfFormat::from_extension(Path::new("a.TTL")),
        Some(RdfFormat::Turtle)
    );
    assert_eq!(
        RdfFormat::from_extension(Path::new("a.xml")),
        Some(RdfFormat::RdfXml)
    );
    assert_eq!(
        RdfFormat::from_extension(Path::new("a.json")),
        Some(RdfFormat::JsonLd)
    );
    assert_eq!(RdfFormat::from_extension(Path::new("a.csv")), None);
    assert_eq!(RdfFormat::from_extension(Path::new("noext")), None);
}

#[test]
fn files_round_trip_with_explicit_or_inferred_format() {
    let dir = tempfile::tempdir().unwrap();
    let triples = all_shapes();
    let ttl = dir.path().join("g.ttl");
    RdfSerializer::serialize_file(&triples, &ttl, RdfFormat::Turtle).unwrap();
    assert_eq!(
        normalised(&RdfParser::parse_file(&ttl, None).unwrap()),
        normalised(&triples)
    );

    let odd = dir.path().join("g.data");
    RdfSerializer::serialize_file(&triples, &odd, RdfFormat::NTriples).unwrap();
    assert_eq!(
        RdfParser::parse_file(&odd, Some(RdfFormat::NTriples))
            .unwrap()
            .len(),
        triples.len()
    );
    let err = RdfParser::parse_file(&odd, None).unwrap_err();
    assert!(
        matches!(err, ParseError::Parse(ref m) if m.contains("determine format")),
        "{err:?}"
    );

    let missing = dir.path().join("missing.nt");
    assert!(matches!(
        RdfParser::parse_file(&missing, None).unwrap_err(),
        ParseError::Io(_)
    ));
    let bad_dir = dir.path().join("no/such/dir/g.nt");
    assert!(matches!(
        RdfSerializer::serialize_file(&triples, &bad_dir, RdfFormat::NTriples).unwrap_err(),
        SerializeError::Io(_)
    ));
}

#[test]
fn serialize_store_writes_what_the_store_holds() {
    let mut store = RdfStore::new();
    for t in all_shapes() {
        store.insert(t).unwrap();
    }
    let text = RdfSerializer::serialize_store(&store, RdfFormat::NTriples).unwrap();
    let back = RdfParser::parse(&text, RdfFormat::NTriples).unwrap();
    assert_eq!(normalised(&back), normalised(&all_shapes()));
}

#[test]
fn error_messages() {
    assert_eq!(ParseError::Parse("x".into()).to_string(), "Parse error: x");
    assert_eq!(
        ParseError::UnsupportedFormat(RdfFormat::JsonLd).to_string(),
        "Unsupported format: JsonLd"
    );
    assert_eq!(
        SerializeError::Serialize("y".into()).to_string(),
        "Serialization error: y"
    );
    assert_eq!(
        SerializeError::UnsupportedFormat(RdfFormat::Turtle).to_string(),
        "Unsupported format: Turtle"
    );
    let io = std::io::Error::other("disk");
    assert_eq!(ParseError::from(io).to_string(), "IO error: disk");
}
