//! N-Triples format implementation

use crate::rdf::{
    Triple, RdfStore, NamedNode, BlankNode, Literal, RdfSubject, RdfPredicate, RdfObject
};
use super::{ParseResult, SerializeResult, ParseError, SerializeError};
use rio_api::parser::TriplesParser;
use rio_api::formatter::TriplesFormatter;
use rio_turtle::{NTriplesParser, NTriplesFormatter};
use std::io::{BufReader, Cursor};

/// N-Triples parser
pub struct NTriplesParserWrapper;

impl NTriplesParserWrapper {
    /// Parse N-Triples string to Triples
    pub fn parse(input: &str) -> ParseResult<Vec<Triple>> {
        let cursor = Cursor::new(input);
        let mut reader = BufReader::new(cursor);
        let mut parser = NTriplesParser::new(&mut reader);
        
        let mut triples = Vec::new();
        
        let res: Result<(), rio_turtle::TurtleError> = parser.parse_all(&mut |t| {
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

/// N-Triples serializer
pub struct NTriplesSerializerWrapper;

impl NTriplesSerializerWrapper {
    /// Serialize Triples to N-Triples string
    pub fn serialize(triples: &[Triple]) -> SerializeResult<String> {
        let mut output = Vec::new();
        let mut formatter = NTriplesFormatter::new(&mut output);

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

// Helpers (Same as turtle.rs)

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
    fn test_ntriples_roundtrip() {
        let input = r#"<http://example.org/a> <http://example.org/b> "c" ."#;
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("<http://example.org/a>"));
    }

    #[test]
    fn test_ntriples_multiple() {
        let input = concat!(
            "<http://example.org/s1> <http://example.org/p1> \"v1\" .\n",
            "<http://example.org/s2> <http://example.org/p2> \"v2\" .\n"
        );
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 2);

        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("<http://example.org/s1>"));
        assert!(output.contains("<http://example.org/s2>"));
    }

    #[test]
    fn test_ntriples_serialize_empty() {
        let output = NTriplesSerializerWrapper::serialize(&[]).unwrap();
        assert!(output.is_empty() || !output.contains("<http://"));
    }

    // ========== Additional N-Triples Coverage Tests ==========

    #[test]
    fn test_ntriples_parse_invalid() {
        let input = "this is not valid ntriples";
        let result = NTriplesParserWrapper::parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_ntriples_parse_empty() {
        let input = "";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert!(triples.is_empty());
    }

    #[test]
    fn test_ntriples_parse_with_comments() {
        let input = concat!(
            "# This is a comment\n",
            "<http://example.org/a> <http://example.org/b> \"c\" .\n",
            "# Another comment\n"
        );
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);
    }

    #[test]
    fn test_ntriples_parse_named_node_object() {
        let input = "<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].object {
            RdfObject::NamedNode(n) => assert_eq!(n.as_str(), "http://example.org/bob"),
            _ => panic!("Expected NamedNode object"),
        }
    }

    #[test]
    fn test_ntriples_parse_typed_literal() {
        let input = "<http://example.org/alice> <http://example.org/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
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
    fn test_ntriples_parse_language_tagged_literal() {
        let input = "<http://example.org/alice> <http://example.org/name> \"Alice\"@en .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
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
    fn test_ntriples_parse_blank_node_subject() {
        let input = "_:b0 <http://example.org/name> \"Test\" .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].subject {
            RdfSubject::BlankNode(_) => {}
            _ => panic!("Expected BlankNode subject"),
        }
    }

    #[test]
    fn test_ntriples_parse_blank_node_object() {
        let input = "<http://example.org/a> <http://example.org/ref> _:b1 .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].object {
            RdfObject::BlankNode(_) => {}
            _ => panic!("Expected BlankNode object"),
        }
    }

    #[test]
    fn test_ntriples_serialize_named_node_object() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/alice").unwrap());
        let predicate = RdfPredicate::new("http://example.org/knows").unwrap();
        let object = RdfObject::NamedNode(NamedNode::new("http://example.org/bob").unwrap());

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("<http://example.org/alice>"));
        assert!(output.contains("<http://example.org/bob>"));
    }

    #[test]
    fn test_ntriples_serialize_typed_literal() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/alice").unwrap());
        let predicate = RdfPredicate::new("http://example.org/age").unwrap();
        let dt = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
        let object = RdfObject::Literal(Literal::new_typed_literal("30", dt));

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("\"30\""));
        assert!(output.contains("XMLSchema#integer"));
    }

    #[test]
    fn test_ntriples_serialize_language_tagged() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/alice").unwrap());
        let predicate = RdfPredicate::new("http://example.org/name").unwrap();
        let object = RdfObject::Literal(
            Literal::new_language_tagged_literal("Alice", "en").unwrap()
        );

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("\"Alice\"@en"));
    }

    #[test]
    fn test_ntriples_serialize_blank_node_subject() {
        let subject = RdfSubject::BlankNode(BlankNode::from_str("b0").unwrap());
        let predicate = RdfPredicate::new("http://example.org/name").unwrap();
        let object = RdfObject::Literal(Literal::new_simple_literal("Test"));

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("_:b0"));
    }

    #[test]
    fn test_ntriples_serialize_blank_node_object() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/a").unwrap());
        let predicate = RdfPredicate::new("http://example.org/ref").unwrap();
        let object = RdfObject::BlankNode(BlankNode::from_str("b1").unwrap());

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("_:b1"));
    }

    #[test]
    fn test_ntriples_roundtrip_typed_literal() {
        let input = "<http://example.org/alice> <http://example.org/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        let reparsed = NTriplesParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 1);

        match &reparsed[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "30");
                assert_eq!(l.datatype().as_str(), "http://www.w3.org/2001/XMLSchema#integer");
            }
            _ => panic!("Expected Literal object after roundtrip"),
        }
    }

    #[test]
    fn test_ntriples_roundtrip_language_tagged() {
        let input = "<http://example.org/alice> <http://example.org/name> \"Alice\"@en .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        let reparsed = NTriplesParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 1);

        match &reparsed[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "Alice");
                assert_eq!(l.language(), Some("en"));
            }
            _ => panic!("Expected Literal object after roundtrip"),
        }
    }

    #[test]
    fn test_ntriples_roundtrip_blank_nodes() {
        let subject = RdfSubject::BlankNode(BlankNode::from_str("b0").unwrap());
        let predicate = RdfPredicate::new("http://example.org/p").unwrap();
        let object = RdfObject::BlankNode(BlankNode::from_str("b1").unwrap());

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        let reparsed = NTriplesParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 1);

        assert!(matches!(&reparsed[0].subject, RdfSubject::BlankNode(_)));
        assert!(matches!(&reparsed[0].object, RdfObject::BlankNode(_)));
    }

    #[test]
    fn test_ntriples_roundtrip_named_node_object() {
        let input = "<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        let reparsed = NTriplesParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 1);

        match &reparsed[0].object {
            RdfObject::NamedNode(n) => assert_eq!(n.as_str(), "http://example.org/bob"),
            _ => panic!("Expected NamedNode object after roundtrip"),
        }
    }

    #[test]
    fn test_ntriples_serialize_many() {
        let mut triples = Vec::new();
        for i in 0..10 {
            let iri = format!("http://example.org/node{}", i);
            let subject = RdfSubject::NamedNode(
                NamedNode::new(&iri).unwrap()
            );
            let predicate = RdfPredicate::new("http://example.org/value").unwrap();
            let object = RdfObject::Literal(Literal::new_simple_literal(format!("val{}", i)));
            triples.push(Triple::new(subject, predicate, object));
        }

        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        let reparsed = NTriplesParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 10);
    }

    #[test]
    fn test_ntriples_parse_simple_literal() {
        let input = "<http://example.org/a> <http://example.org/b> \"simple string\" .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "simple string");
                // Simple literals have xsd:string datatype
                assert_eq!(l.datatype().as_str(), "http://www.w3.org/2001/XMLSchema#string");
                assert!(l.language().is_none());
            }
            _ => panic!("Expected Literal object"),
        }
    }

    // ========== Additional N-Triples Coverage Tests ==========

    #[test]
    fn test_ntriples_parse_multiple_blank_nodes() {
        let input = concat!(
            "_:b0 <http://example.org/p1> _:b1 .\n",
            "_:b1 <http://example.org/p2> \"value\" .\n",
        );
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 2);

        assert!(matches!(&triples[0].subject, RdfSubject::BlankNode(_)));
        assert!(matches!(&triples[0].object, RdfObject::BlankNode(_)));
        assert!(matches!(&triples[1].subject, RdfSubject::BlankNode(_)));
        assert!(matches!(&triples[1].object, RdfObject::Literal(_)));
    }

    #[test]
    fn test_ntriples_serialize_multiple_types() {
        let triples = vec![
            // Named node to named node
            Triple::new(
                RdfSubject::NamedNode(NamedNode::new("http://example.org/alice").unwrap()),
                RdfPredicate::new("http://example.org/knows").unwrap(),
                RdfObject::NamedNode(NamedNode::new("http://example.org/bob").unwrap()),
            ),
            // Named node to simple literal
            Triple::new(
                RdfSubject::NamedNode(NamedNode::new("http://example.org/alice").unwrap()),
                RdfPredicate::new("http://example.org/name").unwrap(),
                RdfObject::Literal(Literal::new_simple_literal("Alice")),
            ),
            // Blank node subject
            Triple::new(
                RdfSubject::BlankNode(BlankNode::from_str("anon1").unwrap()),
                RdfPredicate::new("http://example.org/value").unwrap(),
                RdfObject::Literal(Literal::new_simple_literal("test")),
            ),
        ];

        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("<http://example.org/alice>"));
        assert!(output.contains("<http://example.org/bob>"));
        assert!(output.contains("_:anon1"));
        assert!(output.contains("\"Alice\""));
    }

    #[test]
    fn test_ntriples_roundtrip_multiple() {
        let input = concat!(
            "<http://example.org/s1> <http://example.org/p1> \"v1\" .\n",
            "<http://example.org/s2> <http://example.org/p2> <http://example.org/o2> .\n",
            "<http://example.org/s3> <http://example.org/p3> \"v3\"@fr .\n",
        );

        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 3);

        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        let reparsed = NTriplesParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 3);
    }

    #[test]
    fn test_ntriples_parse_xsd_boolean() {
        let input = "<http://example.org/a> <http://example.org/active> \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "true");
                assert_eq!(l.datatype().as_str(), "http://www.w3.org/2001/XMLSchema#boolean");
            }
            _ => panic!("Expected typed literal"),
        }
    }

    #[test]
    fn test_ntriples_parse_xsd_double() {
        let input = "<http://example.org/a> <http://example.org/score> \"3.14\"^^<http://www.w3.org/2001/XMLSchema#double> .\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);

        match &triples[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "3.14");
                assert_eq!(l.datatype().as_str(), "http://www.w3.org/2001/XMLSchema#double");
            }
            _ => panic!("Expected typed literal"),
        }
    }

    #[test]
    fn test_ntriples_serialize_xsd_boolean_roundtrip() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/a").unwrap());
        let predicate = RdfPredicate::new("http://example.org/active").unwrap();
        let dt = NamedNode::new("http://www.w3.org/2001/XMLSchema#boolean").unwrap();
        let object = RdfObject::Literal(Literal::new_typed_literal("true", dt));

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("\"true\""));
        assert!(output.contains("XMLSchema#boolean"));

        let reparsed = NTriplesParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 1);
        match &reparsed[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "true");
            }
            _ => panic!("Expected Literal"),
        }
    }

    #[test]
    fn test_ntriples_parse_multiple_language_tags() {
        let input = concat!(
            "<http://example.org/a> <http://example.org/label> \"Hello\"@en .\n",
            "<http://example.org/a> <http://example.org/label> \"Bonjour\"@fr .\n",
            "<http://example.org/a> <http://example.org/label> \"Hola\"@es .\n",
        );

        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 3);

        let languages: Vec<&str> = triples.iter().map(|t| {
            match &t.object {
                RdfObject::Literal(l) => l.language().unwrap(),
                _ => panic!("Expected literal"),
            }
        }).collect();

        assert!(languages.contains(&"en"));
        assert!(languages.contains(&"fr"));
        assert!(languages.contains(&"es"));
    }

    #[test]
    fn test_ntriples_parse_whitespace_only() {
        let input = "   \n  \n\n";
        let triples = NTriplesParserWrapper::parse(input).unwrap();
        assert!(triples.is_empty());
    }

    #[test]
    fn test_ntriples_serialize_simple_literal_roundtrip() {
        let subject = RdfSubject::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let predicate = RdfPredicate::new("http://example.org/p").unwrap();
        let object = RdfObject::Literal(Literal::new_simple_literal("hello world"));

        let triples = vec![Triple::new(subject, predicate, object)];
        let output = NTriplesSerializerWrapper::serialize(&triples).unwrap();

        let reparsed = NTriplesParserWrapper::parse(&output).unwrap();
        assert_eq!(reparsed.len(), 1);
        match &reparsed[0].object {
            RdfObject::Literal(l) => {
                assert_eq!(l.value(), "hello world");
                assert!(l.language().is_none());
            }
            _ => panic!("Expected Literal"),
        }
    }
}