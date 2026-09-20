//! The bodies behind `mapping.rs` (#1362, NDS-15).
//!
//! Kept beside the API rather than inside it so the mapping decisions have
//! somewhere to be written down. Every one of them is a choice about what
//! survives a round trip, and the requirement is a *round trip*: NDS-15 asks
//! for import and export with IRIs preserved, which is a stronger thing than
//! either direction alone.
//!
//! # The IRI problem
//!
//! A property graph node has an integer id. An RDF resource has an IRI. Export
//! can always invent one — `{base}node/{id}` — but then importing RDF and
//! exporting it again produces different IRIs from the ones that came in, and
//! anything that referred to the originals no longer resolves. So an imported
//! resource keeps its IRI in a reserved property, [`IRI_PROPERTY`], and export
//! prefers it over the generated form. That makes the round trip an identity
//! on IRIs, which is the part of NDS-15 that is easy to claim and easy to get
//! wrong.
//!
//! The reserved name is visible in the graph on purpose. A hidden field would
//! not survive `.sgsnap` export, Cypher `SET`, or a user who copies a node —
//! and an invisible thing that silently changes IRIs is worse than a visible
//! one someone can read.
//!
//! # What maps to what
//!
//! | Property graph | RDF |
//! |---|---|
//! | node | a subject IRI |
//! | label | `rdf:type {base}class/{Label}` |
//! | node property | `{base}prop/{key}` → typed literal |
//! | edge | `{source} {base}rel/{TYPE} {target}` |
//! | edge property | reified: an `rdf:Statement` carrying subject/predicate/object, read back onto the edge on import |
//!
//! # What does not round-trip, and is not pretended to
//!
//! - **Edge identity.** Two edges of the same type between the same pair are
//!   one triple unless they carry properties and reification is on. RDF has no
//!   parallel edges, so collapsing them is the honest reading — pinned in
//!   `tests/rdf_round_trip.rs` so it is a documented loss rather than a
//!   surprise.
//! - **Float precision.** `xsd:double` is written with `{:?}`, which round-trips
//!   an `f64` exactly in Rust's formatting, but an external consumer may parse
//!   it more loosely. That is RDF's problem to the extent it is anyone's.
//! - **Vectors and arrays.** Serialised as a JSON literal rather than an RDF
//!   list. An RDF list is five triples per element and nothing we import would
//!   read it back as a vector.

use std::collections::HashMap;

use super::mapping::{MappingError, MappingResult, MappingConfig};
use super::{Literal, NamedNode, RdfObject, RdfPredicate, RdfStore, RdfSubject, Triple};
use crate::graph::{Edge, GraphStore, Node, NodeId, PropertyValue};

/// Where an imported resource's original IRI is kept.
///
/// Read on export, written on import. A node without it is exported at the
/// generated `{base}node/{id}`, which is right for a graph that was never RDF.
pub const IRI_PROPERTY: &str = "__rdf_iri";

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_STATEMENT: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement";
const RDF_SUBJECT: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#subject";
const RDF_PREDICATE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate";
const RDF_OBJECT: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#object";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn iri(s: &str) -> MappingResult<NamedNode> {
    NamedNode::new(s).map_err(|_| MappingError::InvalidIri(s.to_string()))
}

/// Percent-encode the characters that cannot appear in an IRI path segment.
///
/// Labels and property keys are user-authored and routinely contain spaces.
/// Leaving them raw produced an `InvalidIri` for perfectly ordinary graphs, so
/// the mapping refused data it could represent.
fn escape_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(c),
            _ => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).as_bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
        }
    }
    out
}

/// The IRI for a node: the one it came in with, or a generated one.
///
/// Reads the **merged** properties, not `node.properties`. The row map is only
/// half the store: a property set through `set_node_property` — which is what
/// import does, and what a snapshot restore does — lands in the columnar side
/// and leaves the row map empty (#554). Reading the row alone reported that
/// every imported node had no properties at all, so the export of a graph that
/// had just been imported was a set of bare subjects.
pub fn node_iri(config: &MappingConfig, graph: &GraphStore, node: &Node) -> MappingResult<NamedNode> {
    if let Some(PropertyValue::String(existing)) = graph.node_properties_merged(node.id).get(IRI_PROPERTY) {
        return iri(existing);
    }
    iri(&format!("{}node/{}", config.base_iri, node.id.as_u64()))
}

fn node_iri_by_id(config: &MappingConfig, graph: &GraphStore, id: NodeId) -> MappingResult<NamedNode> {
    match graph.get_node(id) {
        Some(node) => node_iri(config, graph, node),
        // An edge endpoint that is not in the store is a torn graph, not
        // something to paper over with a plausible IRI.
        None => Err(MappingError::InvalidIri(format!(
            "edge endpoint {} is not in the graph",
            id.as_u64()
        ))),
    }
}

/// A property value as an RDF literal, with the XSD datatype that matches.
///
/// Typed rather than plain: a plain literal makes every integer a string, and
/// the round trip then returns a graph where `age` is `"30"`. Preserving the
/// type is the difference between a round trip and a lossy one.
pub fn literal_for(value: &PropertyValue) -> MappingResult<Literal> {
    let typed = |v: String, t: &str| -> MappingResult<Literal> {
        Ok(Literal::new_typed_literal(v, iri(&format!("{XSD}{t}"))?))
    };
    match value {
        PropertyValue::String(s) => Ok(Literal::new_simple_literal(s.clone())),
        PropertyValue::Integer(i) => typed(i.to_string(), "integer"),
        // `{:?}` rather than `{}`: `1.0` formats as "1" under Display and comes
        // back an integer, so the round trip changed the type of every whole
        // float in the graph.
        PropertyValue::Float(f) => typed(format!("{f:?}"), "double"),
        PropertyValue::Boolean(b) => typed(b.to_string(), "boolean"),
        other => {
            // Everything else — temporals, arrays, maps, vectors — goes as a
            // JSON literal. Named so a reader knows it is ours and not a
            // standard datatype they should recognise.
            let json = serde_json::to_string(other)
                .map_err(|e| MappingError::UnsupportedPropertyType(e.to_string()))?;
            Ok(Literal::new_typed_literal(
                json,
                iri("https://samyama.ai/rdf/PropertyValue")?,
            ))
        }
    }
}

/// Read a literal back, undoing [`literal_for`].
pub fn value_for(lit: &Literal) -> PropertyValue {
    let datatype = lit.datatype();
    let datatype = datatype.as_str();
    let text = lit.value().to_string();
    match Some(datatype) {
        Some(d) if d == format!("{XSD}integer") => text
            .parse::<i64>()
            .map(PropertyValue::Integer)
            .unwrap_or_else(|_| PropertyValue::String(text.clone())),
        Some(d) if d == format!("{XSD}double") => text
            .parse::<f64>()
            .map(PropertyValue::Float)
            .unwrap_or_else(|_| PropertyValue::String(text.clone())),
        Some(d) if d == format!("{XSD}boolean") => text
            .parse::<bool>()
            .map(PropertyValue::Boolean)
            .unwrap_or_else(|_| PropertyValue::String(text.clone())),
        Some("https://samyama.ai/rdf/PropertyValue") => serde_json::from_str(&text)
            // A value we wrote and cannot read is a bug in this pair of
            // functions; keeping the text loses less than panicking.
            .unwrap_or_else(|_| PropertyValue::String(text.clone())),
        _ => PropertyValue::String(text.clone()),
    }
}

/// Triples for one node: its labels and its properties.
pub fn map_node(config: &MappingConfig, graph: &GraphStore, node: &Node) -> MappingResult<Vec<Triple>> {
    let subject = RdfSubject::NamedNode(node_iri(config, graph, node)?);
    let properties = graph.node_properties_merged(node.id);
    let rdf_type = RdfPredicate::new(RDF_TYPE).map_err(|_| MappingError::InvalidIri(RDF_TYPE.into()))?;
    let mut triples = Vec::new();

    // Sorted: `labels` is a HashSet and `properties` a HashMap, so without this
    // the same node serialises in a different order every run and no output can
    // be diffed or recorded (#1364 is the same lesson one layer down).
    let mut labels: Vec<&str> = node.labels.iter().map(|l| l.as_str()).collect();
    labels.sort_unstable();
    for label in labels {
        triples.push(Triple::new(
            subject.clone(),
            rdf_type.clone(),
            RdfObject::NamedNode(iri(&format!(
                "{}class/{}",
                config.base_iri,
                escape_segment(label)
            ))?),
        ));
    }

    let mut keys: Vec<&String> = properties.keys().collect();
    keys.sort();
    for key in keys {
        if key == IRI_PROPERTY {
            // The IRI is the subject. Emitting it as a property as well would
            // make every round trip add a triple.
            continue;
        }
        let value = &properties[key];
        triples.push(Triple::new(
            subject.clone(),
            RdfPredicate::new(&format!("{}prop/{}", config.base_iri, escape_segment(key)))
                .map_err(|_| MappingError::InvalidIri(key.clone()))?,
            RdfObject::Literal(literal_for(value)?),
        ));
    }
    Ok(triples)
}

/// Triples for one edge, reifying its properties when it has any.
pub fn map_edge(config: &MappingConfig, graph: &GraphStore, edge: &Edge) -> MappingResult<Vec<Triple>> {
    let source = node_iri_by_id(config, graph, edge.source)?;
    let target = node_iri_by_id(config, graph, edge.target)?;
    let predicate = RdfPredicate::new(&format!(
        "{}rel/{}",
        config.base_iri,
        escape_segment(edge.edge_type.as_str())
    ))
    .map_err(|_| MappingError::InvalidIri(edge.edge_type.as_str().to_string()))?;

    let mut triples = vec![Triple::new(
        RdfSubject::NamedNode(source.clone()),
        predicate.clone(),
        RdfObject::NamedNode(target.clone()),
    )];

    // Merged, for the same reason as the node's (#554).
    let properties = graph.edge_properties_merged(edge.id);
    if !config.use_reification || properties.is_empty() {
        return Ok(triples);
    }

    // Reification: a resource standing for the statement, carrying the
    // properties. Keyed by edge id so two parallel edges reify separately even
    // though their base triple is the same one.
    let statement = iri(&format!("{}statement/{}", config.base_iri, edge.id.as_u64()))?;
    let s = RdfSubject::NamedNode(statement);
    let p = |x: &str| RdfPredicate::new(x).map_err(|_| MappingError::InvalidIri(x.to_string()));
    triples.push(Triple::new(s.clone(), p(RDF_TYPE)?, RdfObject::NamedNode(iri(RDF_STATEMENT)?)));
    triples.push(Triple::new(s.clone(), p(RDF_SUBJECT)?, RdfObject::NamedNode(source)));
    triples.push(Triple::new(
        s.clone(),
        p(RDF_PREDICATE)?,
        RdfObject::NamedNode(iri(&format!(
            "{}rel/{}",
            config.base_iri,
            escape_segment(edge.edge_type.as_str())
        ))?),
    ));
    triples.push(Triple::new(s.clone(), p(RDF_OBJECT)?, RdfObject::NamedNode(target)));

    let mut keys: Vec<&String> = properties.keys().collect();
    keys.sort();
    for key in keys {
        triples.push(Triple::new(
            s.clone(),
            RdfPredicate::new(&format!("{}prop/{}", config.base_iri, escape_segment(key)))
                .map_err(|_| MappingError::InvalidIri(key.clone()))?,
            RdfObject::Literal(literal_for(&properties[key])?),
        ));
    }
    Ok(triples)
}

/// What an export wrote, and what it could not keep.
///
/// A count rather than a silence. The first version returned `()` and
/// propagated the store's `DuplicateTriple`, so a graph with two `KNOWS`
/// between the same pair **failed to export at all** — the mapping refused
/// data it was supposed to be lossy about. Collapsing is the right answer for
/// RDF, which has no parallel edges; refusing was not, and neither is
/// collapsing quietly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SyncReport {
    /// Triples actually added to the store.
    pub triples_written: usize,
    /// Triples that were already present, so the graph said something twice
    /// and RDF can only say it once. Almost always parallel edges.
    pub duplicates_collapsed: usize,
}

/// Write the whole graph into an RDF store.
pub fn sync_to_rdf(
    config: &MappingConfig,
    graph: &GraphStore,
    rdf: &mut RdfStore,
) -> MappingResult<SyncReport> {
    let mut report = SyncReport::default();
    let mut add = |rdf: &mut RdfStore, triple: Triple, report: &mut SyncReport| {
        // A duplicate is the collapse, not a failure. Any other store error
        // still propagates.
        match rdf.insert(triple) {
            Ok(()) => {
                report.triples_written += 1;
                Ok(())
            }
            Err(super::RdfStoreError::DuplicateTriple) => {
                report.duplicates_collapsed += 1;
                Ok(())
            }
            Err(e) => Err(MappingError::UnsupportedPropertyType(e.to_string())),
        }
    };
    for node in graph.all_nodes() {
        for triple in map_node(config, graph, node)? {
            add(rdf, triple, &mut report)?;
        }
    }
    for edge in graph.all_edges() {
        for triple in map_edge(config, graph, &edge)? {
            add(rdf, triple, &mut report)?;
        }
    }
    Ok(report)
}

/// Read an RDF store into a property graph.
///
/// Two passes. The first creates a node per subject IRI so an edge can be
/// written the moment it is seen; the second fills in labels, properties and
/// edges. One pass would have had to buffer every forward reference, which is
/// the same work with somewhere else to get it wrong.
pub fn map_to_graph(config: &MappingConfig, rdf: &RdfStore, graph: &mut GraphStore) -> MappingResult<()> {
    let class_prefix = format!("{}class/", config.base_iri);
    let prop_prefix = format!("{}prop/", config.base_iri);
    let rel_prefix = format!("{}rel/", config.base_iri);

    // Pass 1: every IRI that appears as a subject, or as the object of a
    // relationship, becomes a node.
    let mut ids: HashMap<String, NodeId> = HashMap::new();
    let mut subjects: Vec<String> = Vec::new();
    for triple in rdf.iter() {
        if let RdfSubject::NamedNode(n) = &triple.subject {
            let s = n.as_str().to_string();
            if !subjects.contains(&s) {
                subjects.push(s);
            }
        }
        if triple.predicate.as_named_node().as_str().starts_with(&rel_prefix) {
            if let RdfObject::NamedNode(n) = &triple.object {
                let s = n.as_str().to_string();
                if !subjects.contains(&s) {
                    subjects.push(s);
                }
            }
        }
    }
    // Sorted so a re-import assigns the same node ids to the same IRIs. Without
    // it the store's iteration order decides, and two imports of one file
    // produce graphs that differ in every id.
    subjects.sort();

    // Which subjects carry an `rdf:type`, so the placeholder label is only
    // given to the ones that have nothing else. Adding `Resource` to every node
    // and the real label on top left every round-tripped node carrying a label
    // it did not start with, so export -> import was not an identity on labels.
    let mut typed: Vec<&str> = rdf
        .iter()
        .filter(|t| t.predicate.as_named_node().as_str() == RDF_TYPE)
        .filter_map(|t| match &t.subject {
            RdfSubject::NamedNode(n) => Some(n.as_str()),
            _ => None,
        })
        .collect();
    typed.sort_unstable();
    typed.dedup();

    for s in &subjects {
        // Statement resources are reification bookkeeping, not nodes.
        if s.starts_with(&format!("{}statement/", config.base_iri)) {
            continue;
        }
        // A label is how a node is found again, so one that names no class
        // still needs something. `Resource` is that, and only that.
        let id = if typed.binary_search(&s.as_str()).is_ok() {
            graph.create_node_with_labels(std::iter::empty())
        } else {
            graph.create_node("Resource")
        };
        graph
            .set_node_property("default", id, IRI_PROPERTY, s.clone())
            .map_err(|e| MappingError::UnsupportedPropertyType(e.to_string()))?;
        ids.insert(s.clone(), id);
    }

    // Pass 2.
    for triple in rdf.iter() {
        let RdfSubject::NamedNode(subject) = &triple.subject else {
            continue;
        };
        let Some(&node_id) = ids.get(subject.as_str()) else {
            continue;
        };
        let predicate = triple.predicate.as_named_node().as_str().to_string();

        if predicate == RDF_TYPE {
            if let RdfObject::NamedNode(class) = &triple.object {
                let name = class
                    .as_str()
                    .strip_prefix(&class_prefix)
                    .map(decode_segment)
                    // A type IRI from someone else's vocabulary is still a
                    // label; using the whole IRI keeps it distinguishable from
                    // one of ours and keeps the round trip honest.
                    .unwrap_or_else(|| class.as_str().to_string());
                graph
                    .add_label_to_node("default", node_id, name)
                    .map_err(|e| MappingError::UnsupportedPropertyType(e.to_string()))?;
            }
        } else if let Some(key) = predicate.strip_prefix(&prop_prefix) {
            if let RdfObject::Literal(lit) = &triple.object {
                graph
                    .set_node_property("default", node_id, decode_segment(key), value_for(lit))
                    .map_err(|e| MappingError::UnsupportedPropertyType(e.to_string()))?;
            }
        } else if let Some(edge_type) = predicate.strip_prefix(&rel_prefix) {
            if let RdfObject::NamedNode(target) = &triple.object {
                if let Some(&target_id) = ids.get(target.as_str()) {
                    graph
                        .create_edge(node_id, target_id, decode_segment(edge_type))
                        .map_err(|e| MappingError::UnsupportedPropertyType(e.to_string()))?;
                }
            }
        }
    }

    // Pass 3: reified edge properties.
    //
    // Without this the export writes them and the import drops them, so an
    // edge property survives being written and not being read — which is the
    // shape of a round trip that looks like it works. Left until last because
    // it needs the edges pass 2 created.
    restore_reified_edge_properties(config, rdf, graph, &ids, &rel_prefix, &prop_prefix)?;
    Ok(())
}

/// Read `rdf:Statement` resources back onto the edges they describe.
fn restore_reified_edge_properties(
    config: &MappingConfig,
    rdf: &RdfStore,
    graph: &mut GraphStore,
    ids: &HashMap<String, NodeId>,
    rel_prefix: &str,
    prop_prefix: &str,
) -> MappingResult<()> {
    let statement_prefix = format!("{}statement/", config.base_iri);

    // Gather each statement's subject, predicate, object and properties before
    // touching the graph: the triples arrive in no particular order and the
    // endpoints are needed before the properties can be placed.
    #[derive(Default)]
    struct Reified {
        subject: Option<String>,
        predicate: Option<String>,
        object: Option<String>,
        properties: Vec<(String, PropertyValue)>,
    }
    let mut statements: HashMap<String, Reified> = HashMap::new();
    for triple in rdf.iter() {
        let RdfSubject::NamedNode(s) = &triple.subject else {
            continue;
        };
        if !s.as_str().starts_with(&statement_prefix) {
            continue;
        }
        let entry = statements.entry(s.as_str().to_string()).or_default();
        let predicate = triple.predicate.as_named_node().as_str();
        match (predicate, &triple.object) {
            (RDF_SUBJECT, RdfObject::NamedNode(n)) => entry.subject = Some(n.as_str().to_string()),
            (RDF_PREDICATE, RdfObject::NamedNode(n)) => {
                entry.predicate = Some(n.as_str().to_string())
            }
            (RDF_OBJECT, RdfObject::NamedNode(n)) => entry.object = Some(n.as_str().to_string()),
            (p, RdfObject::Literal(lit)) => {
                if let Some(key) = p.strip_prefix(prop_prefix) {
                    entry.properties.push((decode_segment(key), value_for(lit)));
                }
            }
            _ => {}
        }
    }

    for reified in statements.values() {
        let (Some(s), Some(p), Some(o)) = (&reified.subject, &reified.predicate, &reified.object)
        else {
            // A statement missing one of its three parts describes nothing.
            continue;
        };
        let (Some(&source), Some(&target)) = (ids.get(s), ids.get(o)) else {
            continue;
        };
        let Some(edge_type) = p.strip_prefix(rel_prefix).map(decode_segment) else {
            continue;
        };
        let candidates = graph.edges_between(source, target, Some(&edge_type.clone().into()));
        // Parallel edges of the same type collapsed on the way out, so there is
        // at most one to put these back on. Taking the first is exact when the
        // export was lossless and is the only available answer when it was not.
        let Some(&edge_id) = candidates.first() else {
            continue;
        };
        for (key, value) in &reified.properties {
            graph
                .set_edge_property(edge_id, key.clone(), value.clone())
                .map_err(|e| MappingError::UnsupportedPropertyType(e.to_string()))?;
        }
    }
    Ok(())
}

/// Undo [`escape_segment`].
fn decode_segment(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}
