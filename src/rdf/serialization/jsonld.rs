//! JSON-LD format implementation (Basic)

use crate::rdf::{
    Triple, RdfObject
};
use super::{ParseResult, SerializeResult, ParseError, SerializeError};
use serde_json::{json, Value};
use std::collections::HashMap;

/// JSON-LD parser
pub struct JsonLdParserWrapper;

impl JsonLdParserWrapper {
    /// Parse JSON-LD string to Triples
    pub fn parse(_input: &str) -> ParseResult<Vec<Triple>> {
        // Full JSON-LD parsing requires a complex processor (expansion/compaction).
        // Without a dedicated crate like json-ld or sophia_jsonld, this is non-trivial.
        // For now, we return an error indicating it's not yet implemented.
        Err(ParseError::Parse("JSON-LD parsing not yet supported without external crate".to_string()))
    }
}

/// JSON-LD serializer
pub struct JsonLdSerializerWrapper;

impl JsonLdSerializerWrapper {
    /// Serialize Triples to JSON-LD string
    ///
    /// This implements a basic "expanded" JSON-LD serialization.
    pub fn serialize(triples: &[Triple]) -> SerializeResult<String> {
        // Group by subject
        let mut map: HashMap<String, HashMap<String, Vec<Value>>> = HashMap::new();

        for triple in triples {
            let s_str = triple.subject.to_string();
            // Basic cleanup: remove < > if named node, keep _: if blank
            let s_key = if triple.subject.is_named_node() {
                 triple.subject.to_string().trim_matches(|c| c == '<' || c == '>').to_string()
            } else {
                triple.subject.to_string()
            };

            let p_key = triple.predicate.to_string().trim_matches(|c| c == '<' || c == '>').to_string();

            let o_val = match &triple.object {
                RdfObject::NamedNode(n) => {
                    json!({ "@id": n.as_str() })
                },
                RdfObject::BlankNode(b) => {
                    json!({ "@id": format!("_:{}", b.as_str()) })
                },
                RdfObject::Literal(l) => {
                    if let Some(lang) = l.language() {
                         json!({ "@value": l.value(), "@language": lang })
                    } else {
                        let dt = l.datatype();
                        if dt.as_str() == "http://www.w3.org/2001/XMLSchema#string" {
                            json!({ "@value": l.value() })
                        } else {
                            json!({ "@value": l.value(), "@type": dt.as_str() })
                        }
                    }
                }
            };

            map.entry(s_key)
                .or_default()
                .entry(p_key)
                .or_default()
                .push(o_val);
        }

        let mut output = Vec::new();
        for (subject, props) in map {
            let mut node = json!({ "@id": subject });
            for (pred, objs) in props {
                node.as_object_mut().unwrap().insert(pred, json!(objs));
            }
            output.push(node);
        }

        serde_json::to_string_pretty(&output)
            .map_err(|e| SerializeError::Serialize(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdf::{NamedNode, RdfPredicate, Literal};

    #[test]
    fn test_jsonld_serialization() {
        let subject = NamedNode::new("http://example.org/alice").unwrap();
        let predicate = RdfPredicate::new("http://xmlns.com/foaf/0.1/name").unwrap();
        let object = Literal::new_simple_literal("Alice");

        let triple = Triple::new(
            subject.into(),
            predicate,
            object.into(),
        );

        let json = JsonLdSerializerWrapper::serialize(&[triple]).unwrap();
        assert!(json.contains("@id"));
        assert!(json.contains("http://example.org/alice"));
        assert!(json.contains("Alice"));
    }
}
