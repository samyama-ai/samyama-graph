//! How an RDF graph becomes a property graph: a mapping file with presets
//! (#1910).
//!
//! RDF has no single correct property-graph form. The engine's
//! `RdfToGraphMapper` hard-codes one -- Samyama's own namespace maps, and every
//! other predicate is dropped and counted (#1906). This module makes each of
//! those choices explicit, in a YAML file that starts from a preset:
//!
//! | Decision | Options | `generic` default |
//! |---|---|---|
//! | Node id | the IRI, kept in `__rdf_iri` | same |
//! | Label from `rdf:type` | `labels` table, else `label_style`: `local`, `prefixed`, `iri` | local name; a clash between namespaces falls back to prefixed and is reported |
//! | Literal object | property: `properties` table, else `key_style` | local name; clash → prefixed |
//! | IRI object | edge: `edges` table (`type`, `reverse`), else the local name upper-cased | `BROADER`, `SUB_CLASS_OF`, … |
//! | Multi-valued property | `multi_valued`: `list`, `first`, `error` | list |
//! | Language tags | `languages.prefer` and `languages.others`: `suffix`, `map`, `drop` | `key` takes `en` or the untagged value; others go to `key@<lang>` |
//! | Typed literals | `typed_literals` | on: `xsd:date` → Date, `xsd:dateTime` → LocalDateTime / ZonedDateTime, `xsd:decimal` → Float (reported) |
//! | Blank nodes | `blank_nodes`: `node`, `inline`, `drop` | node, at `{base}blank/<id>` |
//! | Merge | `merge`: subjects linked by a predicate become one node, the others its aliases | off |
//! | Reverse | per edge in `edges` | off |
//! | Base IRI | `base` | `https://samyama.ai/rdf/` |
//!
//! The `samyama` preset is the engine's mapper itself, so Samyama → RDF →
//! Samyama stays an identity by construction rather than by a second
//! implementation agreeing with the first.
//!
//! A resolved [`Mapping`] -- preset plus overrides, every prefix expanded --
//! serializes with [`Mapping::to_json`], which is what goes into the
//! `.sgsnap` header's `sources[].mapping` (#1911).
//!
//! Whatever the mapping cannot carry is counted in [`RdfMapReport`], never
//! dropped quietly; [`RdfMapReport::dropped`] puts it in the converter's loss
//! report.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::graph::{GraphStore, NodeId, PropertyValue};
use crate::rdf::{Literal, RdfObject, RdfStore, RdfSubject, Triple, IRI_PROPERTY};
use crate::snapshot::format::Dropped;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// The key an inlined blank node's `rdf:type` goes under. `@` keeps it apart
/// from any key a predicate's local name can produce, so an ordinary
/// predicate named `type` cannot be taken for the class (#1954 review).
pub const INLINE_CLASS_KEY: &str = "@type";

/// How deep inlined blank nodes may nest. Past it the read is refused rather
/// than recursing until the stack overflows (#1952 review).
pub const MAX_INLINE_DEPTH: usize = 64;

/// How many triples inlining may read across every copy of a shared blank
/// node. A blank node reached through several paths is copied into each map,
/// so a small layered graph can expand exponentially (#1952 review).
pub const MAX_INLINE_VALUES: usize = 1_000_000;

/// The presets, by name.
pub const PRESETS: [&str; 4] = ["generic", "skos", "owl", "samyama"];

fn preset_text(name: &str) -> Option<&'static str> {
    Some(match name {
        "generic" => include_str!("presets/generic.yaml"),
        "skos" => include_str!("presets/skos.yaml"),
        "owl" => include_str!("presets/owl.yaml"),
        "samyama" => include_str!("presets/samyama.yaml"),
        _ => return None,
    })
}

/// Prefixes every mapping knows without declaring them.
const WELL_KNOWN_PREFIXES: [(&str, &str); 8] = [
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("xsd", XSD),
    ("dcterms", "http://purl.org/dc/terms/"),
    ("foaf", "http://xmlns.com/foaf/0.1/"),
    ("schema", "https://schema.org/"),
];

/// How a name is made from an IRI when no table names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NameStyle {
    /// The part after the last `#`, `/` or `:` -- `Concept`.
    Local,
    /// A known prefix and the local name -- `skos_Concept`.
    Prefixed,
    /// The whole IRI.
    Iri,
}

/// What a predicate with more than one value on a subject becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiValued {
    /// A list, in the order the triples were read.
    List,
    /// The first value read; the rest are counted as dropped.
    First,
    /// Refuse the conversion, naming the subject and the property.
    Error,
}

/// Where a literal in a language other than the preferred one goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtherLanguages {
    /// Its own property, `key@de`.
    Suffix,
    /// `key` becomes a map from language tag to value, every language in it.
    Map,
    /// Not stored; counted.
    Drop,
}

/// What a blank node becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlankNodes {
    /// A node, given the IRI `{base}blank/<id>`.
    Node,
    /// A map property on whatever points at it, its own triples the entries.
    Inline,
    /// Not stored; every triple touching it is counted.
    Drop,
}

/// Which end of a merge predicate survives as the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MergeInto {
    Subject,
    /// `a pred b` merges `a` into `b`, as CSO's `preferentialEquivalent`
    /// points at the preferred topic.
    #[default]
    Object,
}

/// `languages:` as written in a file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LanguagesFile {
    pub prefer: Option<String>,
    pub others: Option<OtherLanguages>,
}

/// One `edges:` entry: a type, or a type with `reverse`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EdgeRuleFile {
    Type(String),
    Full {
        #[serde(rename = "type")]
        edge_type: String,
        #[serde(default)]
        reverse: bool,
    },
}

/// `merge:` as written in a file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeFile {
    pub predicate: String,
    #[serde(default)]
    pub into: MergeInto,
    pub aliases_property: Option<String>,
}

/// A mapping file as written: every field optional, overlaid on its preset.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MappingFile {
    pub preset: Option<String>,
    pub base: Option<String>,
    #[serde(default)]
    pub prefixes: BTreeMap<String, String>,
    pub label_style: Option<NameStyle>,
    pub key_style: Option<NameStyle>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default)]
    pub edges: BTreeMap<String, EdgeRuleFile>,
    #[serde(default)]
    pub drop: Vec<String>,
    pub multi_valued: Option<MultiValued>,
    pub languages: Option<LanguagesFile>,
    pub typed_literals: Option<bool>,
    pub blank_nodes: Option<BlankNodes>,
    pub merge: Option<MergeFile>,
}

impl MappingFile {
    /// Lay `top` over `self`: a field `top` sets wins, tables merge entry by
    /// entry with `top`'s entries winning.
    fn overlay(&mut self, top: MappingFile) {
        if top.preset.is_some() {
            self.preset = top.preset;
        }
        if top.base.is_some() {
            self.base = top.base;
        }
        self.prefixes.extend(top.prefixes);
        if top.label_style.is_some() {
            self.label_style = top.label_style;
        }
        if top.key_style.is_some() {
            self.key_style = top.key_style;
        }
        self.labels.extend(top.labels);
        self.properties.extend(top.properties);
        self.edges.extend(top.edges);
        self.drop.extend(top.drop);
        if top.multi_valued.is_some() {
            self.multi_valued = top.multi_valued;
        }
        if let Some(l) = top.languages {
            let mine = self.languages.get_or_insert_with(Default::default);
            if l.prefer.is_some() {
                mine.prefer = l.prefer;
            }
            if l.others.is_some() {
                mine.others = l.others;
            }
        }
        if top.typed_literals.is_some() {
            self.typed_literals = top.typed_literals;
        }
        if top.blank_nodes.is_some() {
            self.blank_nodes = top.blank_nodes;
        }
        if top.merge.is_some() {
            self.merge = top.merge;
        }
    }
}

/// An edge rule with its predicate expanded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EdgeRule {
    #[serde(rename = "type")]
    pub edge_type: String,
    pub reverse: bool,
}

/// A merge rule with its predicate expanded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MergeRule {
    pub predicate: String,
    pub into: MergeInto,
    pub aliases_property: String,
}

/// A mapping resolved from a preset and a file: every decision made, every
/// prefixed name expanded to a full IRI. This is what [`Mapping::to_json`]
/// records in the snapshot header.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Mapping {
    pub preset: String,
    pub base: String,
    pub prefixes: BTreeMap<String, String>,
    pub label_style: NameStyle,
    pub key_style: NameStyle,
    pub labels: BTreeMap<String, String>,
    pub properties: BTreeMap<String, String>,
    pub edges: BTreeMap<String, EdgeRule>,
    pub drop: BTreeSet<String>,
    pub multi_valued: MultiValued,
    pub prefer_language: String,
    pub other_languages: OtherLanguages,
    pub typed_literals: bool,
    pub blank_nodes: BlankNodes,
    pub merge: Option<MergeRule>,
}

/// Why a mapping could not be built or applied.
#[derive(Debug, thiserror::Error)]
pub enum MappingFileError {
    #[error("mapping file is not valid YAML for a mapping: {0}")]
    Yaml(String),
    #[error("unknown preset {0:?}; the presets are generic, skos, owl and samyama")]
    UnknownPreset(String),
    #[error("{0:?} uses a prefix the mapping does not declare; add it under `prefixes`")]
    UnknownPrefix(String),
    #[error(
        "the samyama preset reproduces RdfToGraphMapper exactly and takes only `base`, not {0}"
    )]
    SamyamaOverrides(String),
    #[error("{subject} has more than one {key:?} and `multi_valued: error` was set")]
    MultiValued { subject: String, key: String },
    #[error(
        "{subject} has its own {property:?} values, but `aliases_property` reserves that key \
         for merged aliases; name a key no predicate maps to"
    )]
    AliasesConflict { subject: String, property: String },
    #[error("{0:?} is reserved for each node's own IRI or an inlined class and cannot be a mapping's key")]
    ReservedKey(String),
    #[error("{0}")]
    InlineTooLarge(String),
    #[error("could not read the mapping file {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("{0}")]
    Engine(String),
}

fn parse_file(text: &str) -> Result<MappingFile, MappingFileError> {
    if text.trim().is_empty() {
        return Ok(MappingFile::default());
    }
    serde_yaml::from_str(text).map_err(|e| MappingFileError::Yaml(e.to_string()))
}

impl Mapping {
    /// A preset as it stands, with no overrides.
    pub fn preset(name: &str) -> Result<Mapping, MappingFileError> {
        Mapping::resolve(MappingFile {
            preset: Some(name.to_string()),
            ..MappingFile::default()
        })
    }

    /// A mapping file's text, resolved against its preset (`generic` when it
    /// names none).
    pub fn from_yaml(text: &str) -> Result<Mapping, MappingFileError> {
        Mapping::resolve(parse_file(text)?)
    }

    /// [`from_yaml`](Self::from_yaml) on the file at `path`.
    pub fn from_path(path: &Path) -> Result<Mapping, MappingFileError> {
        let text = std::fs::read_to_string(path).map_err(|source| MappingFileError::Io {
            path: path.into(),
            source,
        })?;
        Mapping::from_yaml(&text)
    }

    /// Resolve a file against its preset.
    pub fn resolve(file: MappingFile) -> Result<Mapping, MappingFileError> {
        let preset = file.preset.clone().unwrap_or_else(|| "generic".to_string());
        let preset_yaml =
            preset_text(&preset).ok_or_else(|| MappingFileError::UnknownPreset(preset.clone()))?;
        if preset == "samyama" {
            // Anything but `base` would make it a different mapper from the
            // engine's, and then it no longer reproduces it.
            let rest = MappingFile {
                preset: None,
                base: None,
                ..file.clone()
            };
            if rest != MappingFile::default() {
                let set = serde_yaml::to_string(&rest).unwrap_or_default();
                return Err(MappingFileError::SamyamaOverrides(set.trim().to_string()));
            }
        }

        let mut merged = parse_file(preset_text("generic").unwrap_or_default())?;
        if preset != "generic" {
            merged.overlay(parse_file(preset_yaml)?);
        }
        merged.overlay(file);

        let mut prefixes: BTreeMap<String, String> = WELL_KNOWN_PREFIXES
            .iter()
            .map(|(p, ns)| (p.to_string(), ns.to_string()))
            .collect();
        prefixes.extend(merged.prefixes);
        let expand = |name: &str| expand(name, &prefixes);

        let labels = merged
            .labels
            .iter()
            .map(|(k, v)| Ok((expand(k)?, v.clone())))
            .collect::<Result<_, MappingFileError>>()?;
        let properties = merged
            .properties
            .iter()
            .map(|(k, v)| Ok((expand(k)?, v.clone())))
            .collect::<Result<_, MappingFileError>>()?;
        let edges = merged
            .edges
            .iter()
            .map(|(k, v)| {
                let rule = match v {
                    EdgeRuleFile::Type(t) => EdgeRule {
                        edge_type: t.clone(),
                        reverse: false,
                    },
                    EdgeRuleFile::Full { edge_type, reverse } => EdgeRule {
                        edge_type: edge_type.clone(),
                        reverse: *reverse,
                    },
                };
                Ok((expand(k)?, rule))
            })
            .collect::<Result<_, MappingFileError>>()?;
        let drop = merged
            .drop
            .iter()
            .map(|d| expand(d))
            .collect::<Result<_, MappingFileError>>()?;
        let merge = match &merged.merge {
            Some(m) => Some(MergeRule {
                predicate: expand(&m.predicate)?,
                into: m.into,
                aliases_property: m
                    .aliases_property
                    .clone()
                    .unwrap_or_else(|| "aliases".to_string()),
            }),
            None => None,
        };
        // `__rdf_iri` holds each node's own IRI and `@type` an inlined class;
        // a mapping that sends values to either would overwrite them (#1952
        // review).
        let reserved = |k: &str| k == IRI_PROPERTY || k == INLINE_CLASS_KEY;
        if let Some(key) = merged.properties.values().find(|k| reserved(k)) {
            return Err(MappingFileError::ReservedKey(key.clone()));
        }
        if let Some(m) = &merge {
            if reserved(&m.aliases_property) {
                return Err(MappingFileError::ReservedKey(m.aliases_property.clone()));
            }
        }
        let languages = merged.languages.unwrap_or_default();

        Ok(Mapping {
            preset,
            base: merged
                .base
                .unwrap_or_else(|| "https://samyama.ai/rdf/".to_string()),
            label_style: merged.label_style.unwrap_or(NameStyle::Local),
            key_style: merged.key_style.unwrap_or(NameStyle::Local),
            labels,
            properties,
            edges,
            drop,
            multi_valued: merged.multi_valued.unwrap_or(MultiValued::List),
            prefer_language: languages.prefer.unwrap_or_else(|| "en".to_string()),
            other_languages: languages.others.unwrap_or(OtherLanguages::Suffix),
            typed_literals: merged.typed_literals.unwrap_or(true),
            blank_nodes: merged.blank_nodes.unwrap_or(BlankNodes::Node),
            merge,
            prefixes,
        })
    }

    /// The resolved mapping as JSON, for the `.sgsnap` header's
    /// `sources[].mapping` (#1911).
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Read `rdf` into `graph` under this mapping.
    pub fn apply(
        &self,
        rdf: &RdfStore,
        graph: &mut GraphStore,
    ) -> Result<RdfMapReport, MappingFileError> {
        if self.preset == "samyama" {
            let r = crate::rdf::RdfToGraphMapper::new(self.base.clone())
                .map_to_graph(rdf, graph)
                .map_err(|e| MappingFileError::Engine(e.to_string()))?;
            return Ok(RdfMapReport {
                triples_read: r.triples_read,
                nodes_created: r.nodes_created,
                edges_created: r.edges_created,
                labels_applied: r.labels_applied,
                properties_set: r.properties_set,
                predicates_dropped: r.predicates_skipped,
                languages_dropped: r.language_tags_dropped,
                literals_as_string: r.literals_typed_as_string,
                ..RdfMapReport::default()
            });
        }
        Applier::new(self, rdf).run(graph)
    }
}

/// `prefix:local` to a full IRI; a full IRI as it is.
fn expand(name: &str, prefixes: &BTreeMap<String, String>) -> Result<String, MappingFileError> {
    if name.contains("://") || name.starts_with("urn:") {
        return Ok(name.to_string());
    }
    match name.split_once(':') {
        Some((p, local)) => match prefixes.get(p) {
            Some(ns) => Ok(format!("{ns}{local}")),
            None => Err(MappingFileError::UnknownPrefix(name.to_string())),
        },
        None => Err(MappingFileError::UnknownPrefix(name.to_string())),
    }
}

/// The part of an IRI after its last `#`, `/` or `:`.
fn local_name(iri: &str) -> &str {
    let cut = iri.rfind(['#', '/', ':']).map(|i| i + 1).unwrap_or(0);
    match &iri[cut..] {
        "" => iri,
        local => local,
    }
}

/// `subClassOf` → `SUB_CLASS_OF`, `skos_broader` → `SKOS_BROADER`.
fn upper_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let mut prev_lower = false;
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            if c.is_ascii_uppercase() && prev_lower {
                out.push('_');
            }
            prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
            out.push(c.to_ascii_uppercase());
        } else {
            if !out.ends_with('_') && !out.is_empty() {
                out.push('_');
            }
            prev_lower = false;
        }
    }
    out.trim_end_matches('_').to_string()
}

/// What an RDF read under a mapping made, and what it could not keep.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RdfMapReport {
    pub triples_read: usize,
    pub nodes_created: usize,
    pub edges_created: usize,
    pub labels_applied: usize,
    pub properties_set: usize,
    /// Predicate IRI -> triples not stored: listed under `drop`, or with an
    /// object of a kind the mapping cannot use for that predicate.
    pub predicates_dropped: BTreeMap<String, u64>,
    /// Local names used by more than one namespace, so each was written with
    /// its prefix instead: `"Concept: http://a/Concept, http://b/Concept"`.
    pub name_clashes: Vec<String>,
    /// Literals in another language written to `key@<lang>`.
    pub languages_suffixed: usize,
    /// Literals in another language not stored (`others: drop`).
    pub languages_dropped: usize,
    /// `xsd:decimal` values stored as Float, which may round them.
    pub decimals_as_float: usize,
    /// Literals stored as their text: a datatype the mapping does not read,
    /// or text that did not parse as its datatype.
    pub literals_as_string: usize,
    pub blank_nodes_as_nodes: usize,
    pub blank_nodes_inlined: usize,
    /// Triples touching a blank node, not stored (`blank_nodes: drop`, or a
    /// blank node under `inline` that nothing points at).
    pub blank_node_triples_dropped: usize,
    /// Subjects merged into another node and kept as its aliases.
    pub aliases_merged: usize,
    /// Values beyond the first under `multi_valued: first`.
    pub extra_values_dropped: usize,
    /// Edges that a merge made identical to one already there.
    pub duplicate_edges_collapsed: usize,
    /// Blank nodes reached again inside their own inlined map -- a cycle --
    /// written as null there (`blank_nodes: inline`).
    #[serde(default)]
    pub blank_node_cycles: usize,
}

impl RdfMapReport {
    /// What did not arrive, for the converter's loss report. Renaming on a
    /// clash, merging with aliases recorded, inlining a blank node and
    /// suffixing a language keep the data, so they are not listed.
    pub fn dropped(&self) -> Vec<Dropped> {
        let mut out = Vec::new();
        let mut push = |what: &str, count: u64, detail: String| {
            if count > 0 {
                out.push(Dropped {
                    what: what.to_string(),
                    count,
                    detail,
                });
            }
        };
        push(
            "rdf_predicates_dropped",
            self.predicates_dropped.values().sum(),
            format!(
                "Triples whose predicate the mapping drops or cannot use with that object: {}.",
                self.predicates_dropped
                    .iter()
                    .map(|(p, n)| format!("{p} ({n})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
        push(
            "rdf_languages_dropped",
            self.languages_dropped as u64,
            "Literals in a language other than the preferred one were not stored.".to_string(),
        );
        push(
            "rdf_decimals_as_float",
            self.decimals_as_float as u64,
            "xsd:decimal values are stored as Float, which can round them.".to_string(),
        );
        push(
            "rdf_literals_as_string",
            self.literals_as_string as u64,
            "Literals of a datatype the mapping does not read, or that did not parse as \
             their datatype, are stored as their text."
                .to_string(),
        );
        push(
            "rdf_blank_node_triples_dropped",
            self.blank_node_triples_dropped as u64,
            "Triples touching a blank node the mapping does not keep were not stored.".to_string(),
        );
        push(
            "rdf_extra_values_dropped",
            self.extra_values_dropped as u64,
            "Values beyond the first of a multi-valued property were not stored \
             (`multi_valued: first`)."
                .to_string(),
        );
        push(
            "rdf_duplicate_edges_collapsed",
            self.duplicate_edges_collapsed as u64,
            "Edges a merge made identical to another were stored once.".to_string(),
        );
        push(
            "rdf_blank_node_cycles",
            self.blank_node_cycles as u64,
            "Blank nodes that refer back to themselves cannot be inlined as a map; the \
             repeat is written as null."
                .to_string(),
        );
        out
    }

    /// True when [`dropped`](Self::dropped) is empty.
    pub fn lossless(&self) -> bool {
        self.dropped().is_empty()
    }

    fn drop_predicate(&mut self, predicate: &str) {
        *self
            .predicates_dropped
            .entry(predicate.to_string())
            .or_insert(0) += 1;
    }
}

/// A subject or object position, as the mapper sees it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Term {
    Iri(String),
    Blank(String),
}

fn subject_term(s: &RdfSubject) -> Term {
    match s {
        RdfSubject::NamedNode(n) => Term::Iri(n.as_str().to_string()),
        RdfSubject::BlankNode(b) => Term::Blank(b.as_str().to_string()),
    }
}

/// One mapping run over one store.
struct Applier<'a> {
    m: &'a Mapping,
    triples: Vec<&'a Triple>,
    report: RdfMapReport,
    /// Merged IRI -> the IRI it merged into, before path compression.
    merged_into: HashMap<String, String>,
    label_names: HashMap<String, String>,
    key_names: HashMap<String, String>,
    edge_names: HashMap<String, String>,
    /// Triples read into inlined maps so far, across every copy, for the
    /// expansion bound.
    inline_values: usize,
}

impl<'a> Applier<'a> {
    fn new(m: &'a Mapping, rdf: &'a RdfStore) -> Self {
        Applier {
            m,
            triples: rdf.iter().collect(),
            report: RdfMapReport::default(),
            merged_into: HashMap::new(),
            label_names: HashMap::new(),
            key_names: HashMap::new(),
            edge_names: HashMap::new(),
            inline_values: 0,
        }
    }

    fn canonical(&self, iri: &str) -> String {
        let mut cur = iri;
        // Bounded by the number of merges, so a cycle cannot hang this.
        for _ in 0..=self.merged_into.len() {
            match self.merged_into.get(cur) {
                Some(next) if next != cur => cur = next,
                _ => break,
            }
        }
        cur.to_string()
    }

    fn canonical_term(&self, t: &Term) -> Term {
        match t {
            Term::Iri(i) => Term::Iri(self.canonical(i)),
            blank => blank.clone(),
        }
    }

    /// `prefix_local` for an IRI under a known namespace, else the IRI.
    fn prefixed(&self, iri: &str) -> String {
        self.m
            .prefixes
            .iter()
            .filter(|(_, ns)| iri.starts_with(ns.as_str()) && iri.len() > ns.len())
            .max_by_key(|(_, ns)| ns.len())
            .map(|(p, ns)| format!("{p}_{}", &iri[ns.len()..]))
            .unwrap_or_else(|| iri.to_string())
    }

    /// Names for every IRI in `iris` under `style`, falling back to the
    /// prefixed form where two namespaces share a local name.
    fn names(&mut self, iris: BTreeSet<String>, style: NameStyle) -> HashMap<String, String> {
        let mut by_local: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
        for iri in &iris {
            by_local.entry(local_name(iri)).or_default().push(iri);
        }
        let mut out = HashMap::new();
        for (local, group) in by_local {
            let clash = style == NameStyle::Local && group.len() > 1;
            if clash {
                self.report.name_clashes.push(format!(
                    "{local}: {}",
                    group
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            for iri in group {
                let name = match (style, clash) {
                    (NameStyle::Local, false) => local.to_string(),
                    (NameStyle::Iri, _) => iri.clone(),
                    _ => self.prefixed(iri),
                };
                out.insert(iri.clone(), name);
            }
        }
        out
    }

    fn run(mut self, graph: &mut GraphStore) -> Result<RdfMapReport, MappingFileError> {
        let m = self.m;
        self.report.triples_read = self.triples.len();

        // Merges first: every later step talks about canonical IRIs.
        if let Some(rule) = &m.merge {
            for t in &self.triples {
                if t.predicate.as_named_node().as_str() != rule.predicate {
                    continue;
                }
                let (RdfSubject::NamedNode(s), RdfObject::NamedNode(o)) = (&t.subject, &t.object)
                else {
                    continue;
                };
                let (from, into) = match rule.into {
                    MergeInto::Object => (s.as_str(), o.as_str()),
                    MergeInto::Subject => (o.as_str(), s.as_str()),
                };
                let (from, into) = (self.canonical(from), self.canonical(into));
                if from != into {
                    self.merged_into.insert(from, into);
                }
            }
        }

        // Which predicates and classes are used how, so names are chosen once
        // with every clash in view.
        let mut classes = BTreeSet::new();
        let mut literal_preds = BTreeSet::new();
        let mut edge_preds = BTreeSet::new();
        for t in &self.triples {
            let p = t.predicate.as_named_node().as_str();
            if m.drop.contains(p) || m.merge.as_ref().is_some_and(|r| r.predicate == p) {
                continue;
            }
            if p == RDF_TYPE {
                if let RdfObject::NamedNode(c) = &t.object {
                    if !m.labels.contains_key(c.as_str()) {
                        classes.insert(c.as_str().to_string());
                    }
                }
                continue;
            }
            // Inside an inlined blank node every predicate is a map key, an IRI
            // object included, so it is named as a key -- under key_style and
            // with clashes resolved -- not as an edge type (#1952 review).
            let inline_subject = matches!(t.subject, RdfSubject::BlankNode(_))
                && m.blank_nodes == BlankNodes::Inline;
            let as_edge = !inline_subject
                && match &t.object {
                    RdfObject::Literal(_) => false,
                    RdfObject::NamedNode(_) => !m.properties.contains_key(p),
                    RdfObject::BlankNode(_) => m.blank_nodes == BlankNodes::Node,
                };
            if as_edge {
                if !m.edges.contains_key(p) {
                    edge_preds.insert(p.to_string());
                }
            } else if !m.properties.contains_key(p) {
                literal_preds.insert(p.to_string());
            }
        }
        self.label_names = self.names(classes, m.label_style);
        self.key_names = self.names(literal_preds, m.key_style);
        self.edge_names = self
            .names(edge_preds, m.key_style)
            .into_iter()
            .map(|(iri, name)| (iri, upper_snake(&name)))
            .collect();
        // Normalization can collapse distinct names (fooBar and foo_bar).
        // Refuse before creating nodes rather than silently merging predicates.
        let mut normalized = BTreeMap::new();
        for (iri, name) in &self.edge_names {
            if let Some(previous) = normalized.insert(name, iri) {
                return Err(MappingFileError::Engine(format!(
                    "edge name {name:?} is shared by {previous:?} and {iri:?}; assign distinct explicit edge types"
                )));
            }
        }

        // Blank nodes' own triples, for inlining.
        let mut blank_triples: HashMap<String, Vec<&Triple>> = HashMap::new();
        for t in &self.triples {
            if let RdfSubject::BlankNode(b) = &t.subject {
                blank_triples
                    .entry(b.as_str().to_string())
                    .or_default()
                    .push(t);
            }
        }
        // Blank nodes an inlined map will actually contain: those a named
        // subject points at through a kept predicate, and what they point at
        // in turn. Being pointed at by another blank node is not enough -- a
        // component of blank nodes no named subject reaches is never inlined,
        // and its triples are reported as dropped (#1952 review).
        let kept =
            |p: &str| !m.drop.contains(p) && !m.merge.as_ref().is_some_and(|r| r.predicate == p);
        let mut reachable: BTreeSet<String> = BTreeSet::new();
        let mut frontier: Vec<String> = self
            .triples
            .iter()
            .filter(|t| matches!(t.subject, RdfSubject::NamedNode(_)))
            .filter(|t| kept(t.predicate.as_named_node().as_str()))
            .filter_map(|t| match &t.object {
                RdfObject::BlankNode(b) => Some(b.as_str().to_string()),
                _ => None,
            })
            .collect();
        while let Some(b) = frontier.pop() {
            if !reachable.insert(b.clone()) {
                continue;
            }
            for t in blank_triples.get(&b).into_iter().flatten() {
                if let RdfObject::BlankNode(next) = &t.object {
                    if kept(t.predicate.as_named_node().as_str()) {
                        frontier.push(next.as_str().to_string());
                    }
                }
            }
        }

        // Nodes: every subject, and every object an edge will point at.
        let mut terms: BTreeSet<Term> = BTreeSet::new();
        for t in &self.triples {
            let p = t.predicate.as_named_node().as_str();
            let subject = subject_term(&t.subject);
            if matches!(subject, Term::Blank(_)) && m.blank_nodes != BlankNodes::Node {
                continue;
            }
            if m.drop.contains(p) && !matches!(subject, Term::Iri(_)) {
                continue;
            }
            terms.insert(self.canonical_term(&subject));
            if p == RDF_TYPE || m.drop.contains(p) {
                continue;
            }
            match &t.object {
                RdfObject::NamedNode(o) if !m.properties.contains_key(p) => {
                    terms.insert(Term::Iri(self.canonical(o.as_str())));
                }
                RdfObject::BlankNode(b) if m.blank_nodes == BlankNodes::Node => {
                    terms.insert(Term::Blank(b.as_str().to_string()));
                }
                _ => {}
            }
        }
        let mut ids: HashMap<Term, NodeId> = HashMap::new();
        for term in &terms {
            let id = graph.create_node_with_labels(std::iter::empty());
            let iri = match term {
                Term::Iri(i) => i.clone(),
                Term::Blank(b) => {
                    self.report.blank_nodes_as_nodes += 1;
                    format!("{}blank/{b}", m.base)
                }
            };
            set_node(graph, id, IRI_PROPERTY, PropertyValue::String(iri))?;
            ids.insert(term.clone(), id);
            self.report.nodes_created += 1;
        }

        // Aliases of merged nodes. Written after the properties, and refused if
        // a predicate also maps to the same key, rather than one silently
        // replacing the other (review on #1952).
        let mut alias_lists: BTreeMap<NodeId, Vec<String>> = BTreeMap::new();
        if m.merge.is_some() {
            let mut aliases: BTreeMap<String, Vec<String>> = BTreeMap::new();
            let merged: Vec<String> = self.merged_into.keys().cloned().collect();
            for from in merged {
                aliases.entry(self.canonical(&from)).or_default().push(from);
            }
            for (into, mut list) in aliases {
                let Some(&id) = ids.get(&Term::Iri(into)) else {
                    continue;
                };
                list.sort();
                self.report.aliases_merged += list.len();
                alias_lists.insert(id, list);
            }
        }

        // Labels, properties and edges.
        let mut values: Vec<((NodeId, String), Vec<PropertyValue>)> = Vec::new();
        let mut value_index: HashMap<(NodeId, String), usize> = HashMap::new();
        let mut lang_maps: BTreeMap<(NodeId, String), BTreeMap<String, Vec<PropertyValue>>> =
            BTreeMap::new();
        let mut edges_seen: BTreeSet<(NodeId, NodeId, String)> = BTreeSet::new();
        let mut labelled: BTreeSet<NodeId> = BTreeSet::new();
        for t in self.triples.clone() {
            let p = t.predicate.as_named_node().as_str();
            let subject = subject_term(&t.subject);
            let object_is_blank = matches!(t.object, RdfObject::BlankNode(_));
            if matches!(subject, Term::Blank(_)) || object_is_blank {
                match m.blank_nodes {
                    BlankNodes::Drop => {
                        self.report.blank_node_triples_dropped += 1;
                        continue;
                    }
                    BlankNodes::Inline => {
                        if let Term::Blank(b) = &subject {
                            // Read through its parent's inlined map; counted
                            // when no named subject reaches it.
                            if !reachable.contains(b) {
                                self.report.blank_node_triples_dropped += 1;
                            }
                            continue;
                        }
                    }
                    BlankNodes::Node => {}
                }
            }
            if m.drop.contains(p) {
                self.report.drop_predicate(p);
                continue;
            }
            if m.merge.as_ref().is_some_and(|r| r.predicate == p) {
                // The merge pass takes a named subject and a named object; any
                // other merge-predicate triple it could not represent is
                // dropped and reported, not skipped as if merged (#1952
                // review).
                let merged = matches!(
                    (&t.subject, &t.object),
                    (RdfSubject::NamedNode(_), RdfObject::NamedNode(_))
                );
                if !merged {
                    self.report.drop_predicate(p);
                }
                continue;
            }
            let Some(&node) = ids.get(&self.canonical_term(&subject)) else {
                self.report.drop_predicate(p);
                continue;
            };

            if p == RDF_TYPE {
                let RdfObject::NamedNode(class) = &t.object else {
                    self.report.drop_predicate(p);
                    continue;
                };
                let label = m
                    .labels
                    .get(class.as_str())
                    .or_else(|| self.label_names.get(class.as_str()))
                    .cloned()
                    .unwrap_or_else(|| class.as_str().to_string());
                graph
                    .add_label_to_node("default", node, label)
                    .map_err(|e| MappingFileError::Engine(e.to_string()))?;
                labelled.insert(node);
                self.report.labels_applied += 1;
                continue;
            }

            match &t.object {
                RdfObject::Literal(lit) => {
                    if m.edges.contains_key(p) {
                        self.report.drop_predicate(p);
                        continue;
                    }
                    let key = self.key_for(p);
                    if !self.usable_key(&key, p) {
                        continue;
                    }
                    match lit.language() {
                        Some(lang) if m.other_languages == OtherLanguages::Map => {
                            lang_maps
                                .entry((node, key))
                                .or_default()
                                .entry(lang.to_string())
                                .or_default()
                                .push(string(lit));
                        }
                        Some(lang) if !lang.eq_ignore_ascii_case(&m.prefer_language) => {
                            match m.other_languages {
                                OtherLanguages::Suffix => {
                                    self.report.languages_suffixed += 1;
                                    let k = format!("{key}@{lang}");
                                    push_value(&mut values, &mut value_index, node, k, string(lit));
                                }
                                _ => self.report.languages_dropped += 1,
                            }
                        }
                        _ => {
                            let v = self.literal_value(lit);
                            push_value(&mut values, &mut value_index, node, key, v);
                        }
                    }
                }
                RdfObject::NamedNode(o) if m.properties.contains_key(p) => {
                    let key = self.key_for(p);
                    if !self.usable_key(&key, p) {
                        continue;
                    }
                    let v = PropertyValue::String(o.as_str().to_string());
                    push_value(&mut values, &mut value_index, node, key, v);
                }
                RdfObject::NamedNode(o) => {
                    let target = ids.get(&Term::Iri(self.canonical(o.as_str()))).copied();
                    self.edge(graph, &mut edges_seen, node, target, p)?;
                }
                RdfObject::BlankNode(b) => match m.blank_nodes {
                    BlankNodes::Node => {
                        let target = ids.get(&Term::Blank(b.as_str().to_string())).copied();
                        self.edge(graph, &mut edges_seen, node, target, p)?;
                    }
                    _ => {
                        let key = self.key_for(p);
                        if !self.usable_key(&key, p) {
                            continue;
                        }
                        let mut stack = BTreeSet::new();
                        let v = self.inline(b.as_str(), &blank_triples, &mut stack)?;
                        self.report.blank_nodes_inlined += 1;
                        push_value(&mut values, &mut value_index, node, key, v);
                    }
                },
            }
        }

        // Multi-valued properties, then language maps. A key that is a language
        // map on a node keeps that node's untagged values inside the map, under
        // `@none` (as JSON-LD names them), instead of the map replacing them;
        // every language's values go through the same multi-value rule
        // (review on #1952).
        for ((node, key), vs) in values {
            if let Some(map) = lang_maps.get_mut(&(node, key.clone())) {
                map.entry("@none".to_string()).or_default().extend(vs);
                continue;
            }
            // Reserved on every node while merging is on, not only on nodes
            // that acquired aliases: a value under this key would otherwise be
            // read back as a merge alias (#1954 review).
            if let Some(rule) = m.merge.as_ref().filter(|r| r.aliases_property == key) {
                return Err(MappingFileError::AliasesConflict {
                    subject: subject_of(graph, node),
                    property: rule.aliases_property.clone(),
                });
            }
            let value = self.collapse(vs, || subject_of(graph, node), &key)?;
            set_node(graph, node, &key, value)?;
            self.report.properties_set += 1;
        }
        for ((node, key), langs) in lang_maps {
            // The same reservation as the values above: a language map under
            // the aliases key would be replaced by the alias list, or read
            // back as aliases (#1952 review).
            if let Some(rule) = m.merge.as_ref().filter(|r| r.aliases_property == key) {
                return Err(MappingFileError::AliasesConflict {
                    subject: subject_of(graph, node),
                    property: rule.aliases_property.clone(),
                });
            }
            let mut map = HashMap::new();
            for (lang, vs) in langs {
                let v = self.collapse(vs, || subject_of(graph, node), &format!("{key}@{lang}"))?;
                map.insert(lang, v);
            }
            set_node(graph, node, &key, PropertyValue::Map(map))?;
            self.report.properties_set += 1;
        }
        if let Some(rule) = &m.merge {
            for (node, list) in alias_lists {
                let values = list.into_iter().map(PropertyValue::String).collect();
                set_node(
                    graph,
                    node,
                    &rule.aliases_property,
                    PropertyValue::Array(values),
                )?;
            }
        }

        // A node with no `rdf:type` still needs a label to be found by, as the
        // engine's mapper gives it.
        for &id in ids.values() {
            if !labelled.contains(&id) {
                graph
                    .add_label_to_node("default", id, "Resource")
                    .map_err(|e| MappingFileError::Engine(e.to_string()))?;
            }
        }
        Ok(self.report)
    }

    /// Whether `key` may hold a predicate's values. `__rdf_iri` holds the
    /// node's own IRI and `@type` an inlined class, so a predicate that maps
    /// to either would overwrite them; its triple is dropped and reported
    /// instead (#1952 review).
    fn usable_key(&mut self, key: &str, predicate: &str) -> bool {
        if key == IRI_PROPERTY || key == INLINE_CLASS_KEY {
            self.report.drop_predicate(predicate);
            return false;
        }
        true
    }

    fn key_for(&self, predicate: &str) -> String {
        self.m
            .properties
            .get(predicate)
            .or_else(|| self.key_names.get(predicate))
            .cloned()
            .unwrap_or_else(|| local_name(predicate).to_string())
    }

    fn edge(
        &mut self,
        graph: &mut GraphStore,
        seen: &mut BTreeSet<(NodeId, NodeId, String)>,
        node: NodeId,
        target: Option<NodeId>,
        predicate: &str,
    ) -> Result<(), MappingFileError> {
        let Some(target) = target else {
            self.report.drop_predicate(predicate);
            return Ok(());
        };
        let (edge_type, reverse) = match self.m.edges.get(predicate) {
            Some(rule) => (rule.edge_type.clone(), rule.reverse),
            None => (
                self.edge_names
                    .get(predicate)
                    .cloned()
                    .unwrap_or_else(|| upper_snake(local_name(predicate))),
                false,
            ),
        };
        let (from, to) = if reverse {
            (target, node)
        } else {
            (node, target)
        };
        if !seen.insert((from, to, edge_type.clone())) {
            self.report.duplicate_edges_collapsed += 1;
            return Ok(());
        }
        graph
            .create_edge(from, to, edge_type)
            .map_err(|e| MappingFileError::Engine(e.to_string()))?;
        self.report.edges_created += 1;
        Ok(())
    }

    /// One key's values under `multi_valued`: one value as itself, several as
    /// a list, the first, or an error naming the subject.
    fn collapse(
        &mut self,
        mut vs: Vec<PropertyValue>,
        subject: impl FnOnce() -> String,
        key: &str,
    ) -> Result<PropertyValue, MappingFileError> {
        if vs.len() == 1 {
            return Ok(vs.pop().unwrap_or(PropertyValue::Null));
        }
        match self.m.multi_valued {
            MultiValued::List => Ok(PropertyValue::Array(vs)),
            MultiValued::First => {
                self.report.extra_values_dropped += vs.len() - 1;
                Ok(vs.swap_remove(0))
            }
            MultiValued::Error => Err(MappingFileError::MultiValued {
                subject: subject(),
                key: key.to_string(),
            }),
        }
    }

    /// A blank node's triples as a map, nested blank nodes as nested maps,
    /// under the same rules as a node's own triples: `drop`, language tags
    /// and `multi_valued` (review on #1952).
    ///
    /// `stack` holds the blank nodes on the current path only, so a blank node
    /// two parents share is inlined under each of them; one that refers back
    /// to itself is a cycle, written as null there and counted.
    fn inline(
        &mut self,
        blank: &str,
        triples: &HashMap<String, Vec<&Triple>>,
        stack: &mut BTreeSet<String>,
    ) -> Result<PropertyValue, MappingFileError> {
        if !stack.insert(blank.to_string()) {
            self.report.blank_node_cycles += 1;
            return Ok(PropertyValue::Null);
        }
        if stack.len() > MAX_INLINE_DEPTH {
            return Err(MappingFileError::InlineTooLarge(format!(
                "blank nodes nest more than {MAX_INLINE_DEPTH} deep, past what inlining \
                 holds as nested maps. Use `blank_nodes: node`"
            )));
        }
        let m = self.m;
        let mut entries: Vec<(String, Vec<PropertyValue>)> = Vec::new();
        let mut index: HashMap<String, usize> = HashMap::new();
        let mut langs: BTreeMap<String, BTreeMap<String, Vec<PropertyValue>>> = BTreeMap::new();
        let mut push = |entries: &mut Vec<(String, Vec<PropertyValue>)>, key: String, v| match index
            .get(&key)
        {
            Some(&i) => entries[i].1.push(v),
            None => {
                index.insert(key.clone(), entries.len());
                entries.push((key, vec![v]));
            }
        };
        for t in triples.get(blank).into_iter().flatten() {
            let p = t.predicate.as_named_node().as_str();
            if m.drop.contains(p) {
                self.report.drop_predicate(p);
                continue;
            }
            if m.merge.as_ref().is_some_and(|r| r.predicate == p) {
                // A blank node is never merged, so its merge-predicate
                // triples cannot be represented (#1952 review).
                self.report.drop_predicate(p);
                continue;
            }
            self.inline_values += 1;
            if self.inline_values > MAX_INLINE_VALUES {
                return Err(MappingFileError::InlineTooLarge(format!(
                    "inlining blank nodes would expand to more than {MAX_INLINE_VALUES} values; \
                     blank nodes shared through several paths are copied into each map. Use \
                     `blank_nodes: node`"
                )));
            }
            let key = if p == RDF_TYPE {
                INLINE_CLASS_KEY.to_string()
            } else {
                let key = self.key_for(p);
                if !self.usable_key(&key, p) {
                    continue;
                }
                key
            };
            match &t.object {
                RdfObject::Literal(lit) => match lit.language() {
                    Some(lang) if m.other_languages == OtherLanguages::Map => {
                        langs
                            .entry(key)
                            .or_default()
                            .entry(lang.to_string())
                            .or_default()
                            .push(string(lit));
                    }
                    Some(lang) if !lang.eq_ignore_ascii_case(&m.prefer_language) => {
                        match m.other_languages {
                            OtherLanguages::Suffix => {
                                self.report.languages_suffixed += 1;
                                push(&mut entries, format!("{key}@{lang}"), string(lit));
                            }
                            _ => self.report.languages_dropped += 1,
                        }
                    }
                    _ => {
                        let v = self.literal_value(lit);
                        push(&mut entries, key, v);
                    }
                },
                RdfObject::NamedNode(n) => push(
                    &mut entries,
                    key,
                    PropertyValue::String(n.as_str().to_string()),
                ),
                RdfObject::BlankNode(b) => {
                    let v = self.inline(b.as_str(), triples, stack)?;
                    push(&mut entries, key, v);
                }
            }
        }
        stack.remove(blank);

        let subject = || format!("blank node _:{blank}");
        let mut map = HashMap::new();
        for (key, vs) in entries {
            if let Some(l) = langs.get_mut(&key) {
                l.entry("@none".to_string()).or_default().extend(vs);
                continue;
            }
            let v = self.collapse(vs, subject, &key)?;
            map.insert(key, v);
        }
        for (key, by_lang) in langs {
            let mut lm = HashMap::new();
            for (lang, vs) in by_lang {
                let v = self.collapse(vs, subject, &format!("{key}@{lang}"))?;
                lm.insert(lang, v);
            }
            map.insert(key, PropertyValue::Map(lm));
        }
        Ok(PropertyValue::Map(map))
    }

    /// A literal as the property value its datatype names.
    fn literal_value(&mut self, lit: &Literal) -> PropertyValue {
        if !self.m.typed_literals || lit.language().is_some() {
            return string(lit);
        }
        let text = lit.value().trim();
        let datatype = lit.datatype();
        let Some(local) = datatype.as_str().strip_prefix(XSD) else {
            if datatype.as_str() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString" {
                return string(lit);
            }
            self.report.literals_as_string += 1;
            return string(lit);
        };
        let parsed = match local {
            "string" | "normalizedString" | "token" | "anyURI" | "language" | "Name" | "NCName" => {
                return string(lit)
            }
            "integer" | "int" | "long" | "short" | "byte" | "nonNegativeInteger"
            | "positiveInteger" | "negativeInteger" | "nonPositiveInteger" | "unsignedLong"
            | "unsignedInt" | "unsignedShort" | "unsignedByte" => {
                text.parse::<i64>().ok().map(PropertyValue::Integer)
            }
            "double" | "float" => text.parse::<f64>().ok().map(PropertyValue::Float),
            "decimal" => {
                let v = text.parse::<f64>().ok().map(PropertyValue::Float);
                if v.is_some() {
                    self.report.decimals_as_float += 1;
                }
                v
            }
            "boolean" => match text {
                "true" | "1" => Some(PropertyValue::Boolean(true)),
                "false" | "0" => Some(PropertyValue::Boolean(false)),
                _ => None,
            },
            "date" => crate::query::executor::temporal::parse_iso_date(text)
                .ok()
                .map(PropertyValue::Date),
            "dateTime" => date_time(text),
            _ => None,
        };
        parsed.unwrap_or_else(|| {
            self.report.literals_as_string += 1;
            string(lit)
        })
    }
}

/// A node's IRI, for an error that names it.
fn subject_of(graph: &GraphStore, node: NodeId) -> String {
    match graph.node_properties_merged(node).get(IRI_PROPERTY) {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => format!("node {}", node.as_u64()),
    }
}

fn string(lit: &Literal) -> PropertyValue {
    PropertyValue::String(lit.value().to_string())
}

/// `xsd:dateTime` text as a LocalDateTime, or a ZonedDateTime when it carries
/// an offset or `Z`.
fn date_time(text: &str) -> Option<PropertyValue> {
    use crate::query::executor::temporal::{parse_iso_date, parse_time_parts};
    let (date, time) = text.split_once('T')?;
    let days = parse_iso_date(date).ok()? as i64;
    let (nanos_of_day, offset) = parse_time_parts(time).ok()?;
    let local_secs = days * 86_400 + nanos_of_day.div_euclid(1_000_000_000);
    let nanos = nanos_of_day.rem_euclid(1_000_000_000) as u32;
    Some(match offset {
        Some(offset_seconds) => PropertyValue::ZonedDateTime {
            secs: local_secs - offset_seconds as i64,
            nanos,
            offset_seconds,
            zone: None,
        },
        None => PropertyValue::LocalDateTime {
            secs: local_secs,
            nanos,
        },
    })
}

fn push_value(
    values: &mut Vec<((NodeId, String), Vec<PropertyValue>)>,
    index: &mut HashMap<(NodeId, String), usize>,
    node: NodeId,
    key: String,
    value: PropertyValue,
) {
    let slot = (node, key);
    match index.get(&slot) {
        Some(&i) => values[i].1.push(value),
        None => {
            index.insert(slot.clone(), values.len());
            values.push((slot, vec![value]));
        }
    }
}

fn set_node(
    graph: &mut GraphStore,
    id: NodeId,
    key: &str,
    value: PropertyValue,
) -> Result<(), MappingFileError> {
    graph
        .set_node_property("default", id, key, value)
        .map_err(|e| MappingFileError::Engine(e.to_string()))
}
