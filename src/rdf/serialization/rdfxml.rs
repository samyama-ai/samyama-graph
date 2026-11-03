//! RDF/XML format implementation

use crate::rdf::{
    Triple, RdfStore, NamedNode, BlankNode, Literal, RdfSubject, RdfPredicate, RdfObject
};
use super::{ParseResult, SerializeResult, ParseError, SerializeError};
use rio_api::parser::TriplesParser;
use rio_api::formatter::TriplesFormatter;
use rio_xml::{RdfXmlParser, RdfXmlFormatter};
use std::io::{BufReader, Cursor};

/// RDF/XML parser
pub struct RdfXmlParserWrapper;

impl RdfXmlParserWrapper {
    /// Parse RDF/XML string to Triples
    pub fn parse(input: &str) -> ParseResult<Vec<Triple>> {
        let cursor = Cursor::new(input);
        let mut reader = BufReader::new(cursor);
        // Base IRI is optional but often needed for RDF/XML
        let mut parser = RdfXmlParser::new(&mut reader, None);
        
        let mut triples = Vec::new();
        
        let res: Result<(), rio_xml::RdfXmlError> = parser.parse_all(&mut |t| {
            let subject = convert_subject(t.subject).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            let predicate = convert_predicate(t.predicate).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            let object = convert_object(t.object).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            
            triples.push(Triple::new(subject, predicate, object));
            Ok(())
        });

        match res {
            Ok(_) => Ok(triples),
            Err(e) => Err(ParseError::Parse(e.to_string())),
        }
    }
}

/// RDF/XML serializer
pub struct RdfXmlSerializerWrapper;

impl RdfXmlSerializerWrapper {
    /// Serialize Triples to RDF/XML string
    pub fn serialize(triples: &[Triple]) -> SerializeResult<String> {
        let mut output = Vec::new();
        let mut formatter = RdfXmlFormatter::new(&mut output)
            .map_err(|e| SerializeError::Io(e))?;

        for triple in triples {
            let s_node;
            let s_blank;
            let subject = match &triple.subject {
                RdfSubject::NamedNode(n) => {
                    s_node = rio_api::model::NamedNode { iri: n.as_str() };
                    rio_api::model::Subject::NamedNode(s_node)
                }
                RdfSubject::BlankNode(b) => {
                    s_blank = rio_api::model::BlankNode { id: b.as_str() };
                    rio_api::model::Subject::BlankNode(s_blank)
                }
            };

            let p_node = rio_api::model::NamedNode { iri: triple.predicate.as_named_node().as_str() };
            
            let o_node;
            let o_blank;
            let o_dt_node;
            let object = match &triple.object {
                RdfObject::NamedNode(n) => {
                    o_node = rio_api::model::NamedNode { iri: n.as_str() };
                    rio_api::model::Term::NamedNode(o_node)
                },
                RdfObject::BlankNode(b) => {
                    o_blank = rio_api::model::BlankNode { id: b.as_str() };
                    rio_api::model::Term::BlankNode(o_blank)
                },
                RdfObject::Literal(l) => {
                    if let Some(lang) = l.language() {
                        rio_api::model::Term::Literal(rio_api::model::Literal::LanguageTaggedString { 
                            value: l.value(), 
                            language: lang 
                        })
                    } else {
                        let datatype_iri = l.datatype();
                        if datatype_iri.as_str() == "http://www.w3.org/2001/XMLSchema#string" {
                             rio_api::model::Term::Literal(rio_api::model::Literal::Simple { 
                                value: l.value()
                            })
                        } else {
                            o_dt_node = datatype_iri;
                            rio_api::model::Term::Literal(rio_api::model::Literal::Typed { 
                                value: l.value(), 
                                datatype: rio_api::model::NamedNode { iri: o_dt_node.as_str() } 
                            })
                        }
                    }
                },
            };
            
            let rio_triple = rio_api::model::Triple {
                subject,
                predicate: p_node,
                object,
            };
            
            formatter.format(&rio_triple)
                .map_err(|e| SerializeError::Serialize(e.to_string()))?;
        }
        
        formatter.finish()
            .map_err(|e| SerializeError::Serialize(e.to_string()))?;
            
        String::from_utf8(output)
            .map_err(|e| SerializeError::Serialize(e.to_string()))
    }
}

// Reuse conversion logic 

fn convert_subject(s: rio_api::model::Subject) -> Result<RdfSubject, ParseError> {
    match s {
        rio_api::model::Subject::NamedNode(n) => {
            Ok(RdfSubject::NamedNode(NamedNode::new(n.iri).map_err(|e| ParseError::Parse(e.to_string()))?))
        },
        rio_api::model::Subject::BlankNode(b) => {
            Ok(RdfSubject::BlankNode(BlankNode::from_str(b.id).map_err(|e| ParseError::Parse(e.to_string()))?))
        },
        _ => Err(ParseError::Parse("Unsupported subject type".to_string())),
    }
}

fn convert_predicate(p: rio_api::model::NamedNode) -> Result<RdfPredicate, ParseError> {
    Ok(RdfPredicate::new(p.iri).map_err(|e| ParseError::Parse(e.to_string()))?)
}

fn convert_object(o: rio_api::model::Term) -> Result<RdfObject, ParseError> {
    match o {
        rio_api::model::Term::NamedNode(n) => {
            Ok(RdfObject::NamedNode(NamedNode::new(n.iri).map_err(|e| ParseError::Parse(e.to_string()))?))
        },
        rio_api::model::Term::BlankNode(b) => {
            Ok(RdfObject::BlankNode(BlankNode::from_str(b.id).map_err(|e| ParseError::Parse(e.to_string()))?))
        },
        rio_api::model::Term::Literal(l) => {
            match l {
                rio_api::model::Literal::Simple { value } => {
                    Ok(RdfObject::Literal(Literal::new_simple_literal(value)))
                },
                rio_api::model::Literal::LanguageTaggedString { value, language } => {
                    Ok(RdfObject::Literal(
                        Literal::new_language_tagged_literal(value, language)
                            .map_err(|e| ParseError::Parse(e.to_string()))?
                    ))
                },
                rio_api::model::Literal::Typed { value, datatype } => {
                    let dt = NamedNode::new(datatype.iri)
                        .map_err(|e| ParseError::Parse(e.to_string()))?;
                    Ok(RdfObject::Literal(Literal::new_typed_literal(value, dt)))
                }
            }
        },
        _ => Err(ParseError::Parse("Unsupported object type".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdfxml_roundtrip() {
        let input = r#"
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
            <rdf:Description rdf:about="http://example.org/a">
                <b xmlns="http://example.org/">c</b>
            </rdf:Description>
        </rdf:RDF>"#;
        
        let triples = RdfXmlParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);
        
        let output = RdfXmlSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("rdf:RDF"));
    }

    #[test]
    fn test_rdfxml_serialize_empty() {
        let output = RdfXmlSerializerWrapper::serialize(&[]).unwrap();
        assert!(output.contains("rdf:RDF") || output.is_empty());
    }

    #[test]
    fn test_rdfxml_roundtrip_multiple() {
        let input = r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/a">
                <ex:p1>v1</ex:p1>
                <ex:p2>v2</ex:p2>
            </rdf:Description>
        </rdf:RDF>"#;
        let triples = RdfXmlParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 2);
    }

    // ========== Additional RDF/XML Coverage Tests ==========

    #[test]
    fn test_rdfxml_parse_invalid_xml() {
        let input = r#"<not valid rdf at all"#;
        let result = RdfXmlParserWrapper::parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_rdfxml_parse_empty_rdf() {
        let input = r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
        </rdf:RDF>"#;
        let triples = RdfXmlParserWrapper::parse(input).unwrap();
        assert!(triples.is_empty());
    }

    #[test]
    fn test_rdfxml_with_named_node_object() {
        let input = r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/alice">
                <ex:knows rdf:resource="http://example.org/bob"/>
            </rdf:Description>
        </rdf:RDF>"#;
        let triples = RdfXmlParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].object {
            RdfObject::NamedNode(n) => assert_eq!(n.as_str(), "http://example.org/bob"),
            _ => panic!("Expected NamedNode object"),
        }
    }

    #[test]
    fn test_rdfxml_with_typed_literal() {
        let input = r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema#">
            <rdf:Description rdf:about="http://example.org/alice">
                <ex:age rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">30</ex:age>
            </rdf:Description>
        </rdf:RDF>"#;
        let triples = RdfXmlParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "30");
                assert_eq!(l.datatype().as_str(), "http://www.w3.org/2001/XMLSchema#integer");
            }
            _ => panic!("Expected Literal object"),
        }
    }

    #[test]
    fn test_rdfxml_with_language_tagged_literal() {
        let input = r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/alice">
                <ex:name xml:lang="en">Alice</ex:name>
            </rdf:Description>
        </rdf:RDF>"#;
        let triples = RdfXmlParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "Alice");
                assert_eq!(l.language(), Some("en"));
            }
            _ => panic!("Expected Literal object"),
        }
    }

    #[test]
    fn test_rdfxml_serialize_named_node_object() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/alice").unwrap());
        let predicate = RdfPredicate::new("http://example.org/knows").unwrap();
        let object = RdfObject::NamedNode(NamedNode::new("http://example.org/bob").unwrap());

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = RdfXmlSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("http://example.org/alice"));
        assert!(output.contains("http://example.org/bob"));
    }

    #[test]
    fn test_rdfxml_serialize_typed_literal() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/alice").unwrap());
        let predicate = RdfPredicate::new("http://example.org/age").unwrap();
        let dt = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
        let object = RdfObject::Literal(Literal::new_typed_literal("30", dt));

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = RdfXmlSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("30"));
    }

    #[test]
    fn test_rdfxml_serialize_language_tagged_literal() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/alice").unwrap());
        let predicate = RdfPredicate::new("http://example.org/name").unwrap();
        let object = RdfObject::Literal(
            Literal::new_language_tagged_literal("Alice", "en").unwrap()
        );

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = RdfXmlSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("Alice"));
    }

    #[test]
    fn test_rdfxml_serialize_blank_node_subject() {
        let subject = RdfSubject::BlankNode(BlankNode::from_str("b0").unwrap());
        let predicate = RdfPredicate::new("http://example.org/name").unwrap();
        let object = RdfObject::Literal(Literal::new_simple_literal("Test"));

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = RdfXmlSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("rdf:RDF") || !output.is_empty());
    }

    #[test]
    fn test_rdfxml_serialize_blank_node_object() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/a").unwrap());
        let predicate = RdfPredicate::new("http://example.org/ref").unwrap();
        let object = RdfObject::BlankNode(BlankNode::from_str("b1").unwrap());

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = RdfXmlSerializerWrapper::serialize(&triples).unwrap();
        assert!(!output.is_empty());
    }

    #[test]
    fn test_rdfxml_roundtrip_multiple_subjects() {
        let input = r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/alice">
                <ex:name>Alice</ex:name>
            </rdf:Description>
            <rdf:Description rdf:about="http://example.org/bob">
                <ex:name>Bob</ex:name>
            </rdf:Description>
        </rdf:RDF>"#;
        let triples = RdfXmlParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 2);

        let output = RdfXmlSerializerWrapper::serialize(&triples).unwrap();
        let reparsed = RdfXmlParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 2);
    }

    #[test]
    fn test_rdfxml_serialize_simple_literal() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/a").unwrap());
        let predicate = RdfPredicate::new("http://example.org/val").unwrap();
        let object = RdfObject::Literal(Literal::new_simple_literal("hello world"));

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = RdfXmlSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("hello world"));

        // Roundtrip
        let reparsed = RdfXmlParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 1);
    }

    #[test]
    fn test_rdfxml_with_rdf_type() {
        let input = r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/">
            <rdf:Description rdf:about="http://example.org/alice">
                <rdf:type rdf:resource="http://example.org/Person"/>
            </rdf:Description>
        </rdf:RDF>"#;
        let triples = RdfXmlParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);
        assert!(matches!(&triples[0].object, RdfObject::NamedNode(_)));
    }
}