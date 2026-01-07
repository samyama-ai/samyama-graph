//! Turtle format implementation

use crate::rdf::{
    Triple, RdfStore, NamedNode, BlankNode, Literal, RdfSubject, RdfPredicate, RdfObject
};
use super::{ParseResult, SerializeResult, ParseError, SerializeError};
use rio_api::parser::TriplesParser;
use rio_api::formatter::TriplesFormatter;
use rio_turtle::{TurtleParser, TurtleFormatter};
use std::io::{BufReader, Cursor};

/// Turtle parser
pub struct TurtleParserWrapper;

impl TurtleParserWrapper {
    /// Parse Turtle string to Triples
    pub fn parse(input: &str) -> ParseResult<Vec<Triple>> {
        let cursor = Cursor::new(input);
        let mut reader = BufReader::new(cursor);
        let mut parser = TurtleParser::new(&mut reader, None);
        
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

/// Turtle serializer
pub struct TurtleSerializerWrapper;

impl TurtleSerializerWrapper {
    /// Serialize Triples to Turtle string
    pub fn serialize(triples: &[Triple]) -> SerializeResult<String> {
        let mut output = Vec::new();
        let mut formatter = TurtleFormatter::new(&mut output);

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
    fn test_turtle_roundtrip() {
        let input = r#"<http://example.org/a> <http://example.org/b> "c" ."#;
        let triples = TurtleParserWrapper::parse(input).unwrap();
        assert_eq!(triples.len(), 1);
        let output = TurtleSerializerWrapper::serialize(&triples).unwrap();
        assert!(output.contains("http://example.org/a"));
    }
}