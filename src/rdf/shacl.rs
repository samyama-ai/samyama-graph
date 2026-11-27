//! SHACL shape validation over the graph read as RDF (#2100, part 3).
//!
//! `CALL rdf.shacl.validate(shapes [, baseIri]) YIELD focus_node, result_path,
//! severity, source_constraint, message` checks the graph against shapes
//! written in Turtle and returns one row per violation; no rows means the graph
//! conforms. The graph is read through [`GraphAsRdf`](super::GraphAsRdf), so a
//! shape sees the triples `POST /api/sparql` and the RDF export see: a label is
//! `rdf:type <base>class/<Label>`, a property is `<base>prop/<key>`, a
//! relationship is `<base>rel/<TYPE>`.
//!
//! # What is checked
//!
//! SHACL Core, the parts a property graph exercises:
//!
//! - targets: `sh:targetClass` (with `rdfs:subClassOf` in the data),
//!   `sh:targetNode`, `sh:targetSubjectsOf`, `sh:targetObjectsOf`;
//! - paths: a predicate, or `[ sh:inversePath p ]`;
//! - constraints: `sh:minCount`, `sh:maxCount`, `sh:datatype`, `sh:class`,
//!   `sh:nodeKind`, `sh:minInclusive`, `sh:maxInclusive`, `sh:minExclusive`,
//!   `sh:maxExclusive`, `sh:minLength`, `sh:maxLength`, `sh:pattern` (with
//!   `sh:flags`), `sh:in`, `sh:hasValue`;
//! - `sh:severity`, `sh:message`, `sh:deactivated`.
//!
//! Anything else in the `sh:` namespace that constrains (`sh:node`, `sh:and`,
//! `sh:closed`, a sequence path, …) is **refused**, naming it, rather than
//! skipped: a skipped constraint reports a graph as conforming when nobody
//! checked it. Descriptive terms (`sh:name`, `sh:description`, `sh:order`,
//! `sh:group`, `sh:defaultValue`) do not constrain and are read past.

use std::collections::{HashMap, HashSet};

use regex::Regex;

use super::{Literal, RdfFormat, RdfObject, RdfParser, RdfPredicate, RdfSubject, TripleSource, TriplePattern};

const SH: &str = "http://www.w3.org/ns/shacl#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// One violation: the row `rdf.shacl.validate` yields.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationResult {
    /// The node that broke the rule: an IRI, `_:id`, or a literal's text.
    pub focus_node: String,
    /// The property path checked, `^<iri>` for an inverse path; `None` for a
    /// constraint on the focus node itself.
    pub result_path: Option<String>,
    /// `Violation` (the default), `Warning` or `Info`.
    pub severity: String,
    /// The SHACL constraint component, e.g. `sh:MinCountConstraintComponent`.
    pub source_constraint: String,
    /// The shape's `sh:message`, or a description of what failed.
    pub message: String,
}

#[derive(Debug, Clone)]
enum Path {
    Predicate(String),
    Inverse(String),
}

impl Path {
    fn display(&self) -> String {
        match self {
            Path::Predicate(p) => p.clone(),
            Path::Inverse(p) => format!("^{p}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum NodeKind {
    Iri,
    BlankNode,
    Literal,
    BlankNodeOrIri,
    BlankNodeOrLiteral,
    IriOrLiteral,
}

#[derive(Debug, Clone, Copy)]
enum Bound {
    MinInclusive,
    MaxInclusive,
    MinExclusive,
    MaxExclusive,
}

#[derive(Debug, Clone)]
enum Constraint {
    MinCount(usize),
    MaxCount(usize),
    Datatype(String),
    Class(String),
    NodeKind(NodeKind),
    Range(Bound, Literal),
    MinLength(usize),
    MaxLength(usize),
    Pattern(Regex),
    In(Vec<RdfObject>),
    HasValue(RdfObject),
}

#[derive(Debug, Clone)]
enum Target {
    Class(String),
    Node(RdfObject),
    SubjectsOf(String),
    ObjectsOf(String),
}

#[derive(Debug, Clone)]
struct Shape {
    path: Option<Path>,
    targets: Vec<Target>,
    constraints: Vec<Constraint>,
    properties: Vec<Shape>,
    severity: String,
    message: Option<String>,
    deactivated: bool,
}

/// The shapes graph, indexed by subject.
struct ShapesGraph {
    about: HashMap<RdfSubject, Vec<(String, RdfObject)>>,
}

impl ShapesGraph {
    fn values<'a>(&'a self, s: &RdfSubject, p: &str) -> impl Iterator<Item = &'a RdfObject> + 'a {
        let p = p.to_string();
        self.about.get(s).into_iter().flatten().filter(move |(q, _)| *q == p).map(|(_, o)| o)
    }

    /// An RDF list (`( a b c )` in Turtle) as its members.
    fn list(&self, head: &RdfObject) -> Result<Vec<RdfObject>, String> {
        let mut out = Vec::new();
        let mut at = head.clone();
        let mut seen = HashSet::new();
        loop {
            if matches!(&at, RdfObject::NamedNode(n) if n.as_str() == RDF_NIL) {
                return Ok(out);
            }
            let s = subject_of(&at).ok_or("sh:in must be an RDF list, e.g. ( \"a\" \"b\" )")?;
            if !seen.insert(s.clone()) {
                return Err("sh:in: the RDF list loops".into());
            }
            out.push(self.values(&s, RDF_FIRST).next().ok_or("sh:in: a list cell has no rdf:first")?.clone());
            at = self.values(&s, RDF_REST).next().ok_or("sh:in: a list cell has no rdf:rest")?.clone();
        }
    }
}

fn subject_of(o: &RdfObject) -> Option<RdfSubject> {
    match o {
        RdfObject::NamedNode(n) => Some(RdfSubject::NamedNode(n.clone())),
        RdfObject::BlankNode(b) => Some(RdfSubject::BlankNode(b.clone())),
        RdfObject::Literal(_) => None,
    }
}

fn object_of(s: &RdfSubject) -> RdfObject {
    match s {
        RdfSubject::NamedNode(n) => RdfObject::NamedNode(n.clone()),
        RdfSubject::BlankNode(b) => RdfObject::BlankNode(b.clone()),
    }
}

/// A term as text: an IRI as itself, a blank node as `_:id`, a literal as its
/// lexical form.
fn text(o: &RdfObject) -> String {
    match o {
        RdfObject::NamedNode(n) => n.as_str().to_string(),
        RdfObject::BlankNode(b) => format!("_:{}", b.as_str()),
        RdfObject::Literal(l) => l.value().to_string(),
    }
}

fn shape_name(s: &RdfSubject) -> String {
    text(&object_of(s))
}

fn iri_of(o: &RdfObject, what: &str) -> Result<String, String> {
    match o {
        RdfObject::NamedNode(n) => Ok(n.as_str().to_string()),
        other => Err(format!("{what} must be an IRI, got {}", text(other))),
    }
}

fn count_of(o: &RdfObject, what: &str) -> Result<usize, String> {
    match o {
        RdfObject::Literal(l) => l.value().parse().map_err(|_| format!("{what} must be a non-negative integer, got {}", l.value())),
        other => Err(format!("{what} must be a non-negative integer, got {}", text(other))),
    }
}

fn literal_of(o: &RdfObject, what: &str) -> Result<Literal, String> {
    match o {
        RdfObject::Literal(l) => Ok(l.clone()),
        other => Err(format!("{what} must be a literal, got {}", text(other))),
    }
}

/// Parse the shapes graph and every shape that has a target.
fn read_shapes(turtle: &str) -> Result<Vec<Shape>, String> {
    let triples = RdfParser::parse(turtle, RdfFormat::Turtle).map_err(|e| format!("the shapes do not parse as Turtle: {e}"))?;
    let mut about: HashMap<RdfSubject, Vec<(String, RdfObject)>> = HashMap::new();
    for t in triples {
        about.entry(t.subject).or_default().push((t.predicate.as_named_node().as_str().to_string(), t.object));
    }
    let graph = ShapesGraph { about };

    let typed = |s: &RdfSubject, class: &str| {
        graph.values(s, RDF_TYPE).any(|o| matches!(o, RdfObject::NamedNode(n) if n.as_str() == class))
    };
    let is_shape = |s: &RdfSubject| typed(s, &format!("{SH}NodeShape")) || typed(s, &format!("{SH}PropertyShape"));
    let has_target = |s: &RdfSubject| {
        graph.about[s].iter().any(|(p, _)| p.strip_prefix(SH).is_some_and(|l| l.starts_with("target")))
            || (is_shape(s) && typed(s, RDFS_CLASS))
    };

    // Sorted so the rows come out in the same order on every run.
    let mut subjects: Vec<&RdfSubject> = graph.about.keys().collect();
    subjects.sort_by_key(|s| shape_name(s));
    if !subjects.iter().any(|s| is_shape(s) || has_target(s)) {
        return Err(format!(
            "no SHACL shapes found: nothing is an sh:NodeShape or sh:PropertyShape, and nothing has an \
             sh:target*. Is the sh: prefix bound to <{SH}>?"
        ));
    }
    // A shape with no target validates nothing (SHACL 2.1.3), so only shapes
    // with one are read from the top; the rest are reached through sh:property.
    subjects
        .into_iter()
        .filter(|s| has_target(s))
        .map(|s| read_shape(&graph, s, &mut Vec::new()))
        .collect()
}

fn read_shape(graph: &ShapesGraph, s: &RdfSubject, stack: &mut Vec<RdfSubject>) -> Result<Shape, String> {
    let name = shape_name(s);
    if stack.contains(s) {
        return Err(format!("shape {name} contains itself through sh:property"));
    }
    stack.push(s.clone());
    let mut shape = Shape {
        path: None,
        targets: Vec::new(),
        constraints: Vec::new(),
        properties: Vec::new(),
        severity: "Violation".into(),
        message: None,
        deactivated: false,
    };
    let at = |e: String| format!("shape {name}: {e}");
    if graph.values(s, RDF_TYPE).any(|o| matches!(o, RdfObject::NamedNode(n) if n.as_str() == RDFS_CLASS)) {
        // An implicit class target: a shape that is also a class (SHACL 2.1.3.3).
        shape.targets.push(Target::Class(name.clone()));
    }
    let mut pattern: Option<String> = None;
    let mut flags = String::new();
    for (p, o) in graph.about.get(s).into_iter().flatten() {
        let Some(local) = p.strip_prefix(SH) else { continue };
        let what = format!("sh:{local}");
        match local {
            "targetClass" => shape.targets.push(Target::Class(iri_of(o, &what).map_err(at)?)),
            "targetNode" => shape.targets.push(Target::Node(o.clone())),
            "targetSubjectsOf" => shape.targets.push(Target::SubjectsOf(iri_of(o, &what).map_err(at)?)),
            "targetObjectsOf" => shape.targets.push(Target::ObjectsOf(iri_of(o, &what).map_err(at)?)),
            "path" => shape.path = Some(read_path(graph, o).map_err(at)?),
            "property" => {
                let child = subject_of(o).ok_or_else(|| at("sh:property must name a shape".into()))?;
                shape.properties.push(read_shape(graph, &child, stack)?);
            }
            "minCount" => shape.constraints.push(Constraint::MinCount(count_of(o, &what).map_err(at)?)),
            "maxCount" => shape.constraints.push(Constraint::MaxCount(count_of(o, &what).map_err(at)?)),
            "minLength" => shape.constraints.push(Constraint::MinLength(count_of(o, &what).map_err(at)?)),
            "maxLength" => shape.constraints.push(Constraint::MaxLength(count_of(o, &what).map_err(at)?)),
            "datatype" => shape.constraints.push(Constraint::Datatype(iri_of(o, &what).map_err(at)?)),
            "class" => shape.constraints.push(Constraint::Class(iri_of(o, &what).map_err(at)?)),
            "nodeKind" => {
                let kind = match iri_of(o, &what).map_err(at)?.strip_prefix(SH) {
                    Some("IRI") => NodeKind::Iri,
                    Some("BlankNode") => NodeKind::BlankNode,
                    Some("Literal") => NodeKind::Literal,
                    Some("BlankNodeOrIRI") => NodeKind::BlankNodeOrIri,
                    Some("BlankNodeOrLiteral") => NodeKind::BlankNodeOrLiteral,
                    Some("IRIOrLiteral") => NodeKind::IriOrLiteral,
                    _ => return Err(at(format!("sh:nodeKind {} is not a SHACL node kind", text(o)))),
                };
                shape.constraints.push(Constraint::NodeKind(kind));
            }
            "minInclusive" | "maxInclusive" | "minExclusive" | "maxExclusive" => {
                let bound = match local {
                    "minInclusive" => Bound::MinInclusive,
                    "maxInclusive" => Bound::MaxInclusive,
                    "minExclusive" => Bound::MinExclusive,
                    _ => Bound::MaxExclusive,
                };
                shape.constraints.push(Constraint::Range(bound, literal_of(o, &what).map_err(at)?));
            }
            "pattern" => pattern = Some(literal_of(o, &what).map_err(at)?.value().to_string()),
            "flags" => flags = literal_of(o, &what).map_err(at)?.value().to_string(),
            "in" => shape.constraints.push(Constraint::In(graph.list(o).map_err(at)?)),
            "hasValue" => shape.constraints.push(Constraint::HasValue(o.clone())),
            "severity" => {
                let iri = iri_of(o, &what).map_err(at)?;
                shape.severity = match iri.strip_prefix(SH) {
                    Some(level @ ("Violation" | "Warning" | "Info")) => level.to_string(),
                    _ => iri,
                };
            }
            "message" => {
                if shape.message.is_none() {
                    shape.message = Some(literal_of(o, &what).map_err(at)?.value().to_string());
                }
            }
            "deactivated" => shape.deactivated = matches!(o, RdfObject::Literal(l) if l.value() == "true" || l.value() == "1"),
            // Descriptive: they say what a shape is, not what it requires.
            "name" | "description" | "order" | "group" | "defaultValue" => {}
            other => {
                return Err(at(format!(
                    "sh:{other} is not supported by rdf.shacl.validate yet. Supported: targets, a predicate \
                     or sh:inversePath path, sh:minCount, sh:maxCount, sh:datatype, sh:class, sh:nodeKind, \
                     sh:min/maxInclusive, sh:min/maxExclusive, sh:min/maxLength, sh:pattern, sh:in, sh:hasValue"
                )))
            }
        }
    }
    if let Some(p) = pattern {
        let mut inline = String::new();
        for f in flags.chars() {
            match f {
                'i' | 'm' | 's' | 'x' => inline.push(f),
                _ => return Err(at(format!("sh:flags \"{flags}\": only i, m, s and x are supported"))),
            }
        }
        let source = if inline.is_empty() { p.clone() } else { format!("(?{inline}){p}") };
        let re = Regex::new(&source).map_err(|e| at(format!("sh:pattern \"{p}\" is not a valid regex: {e}")))?;
        shape.constraints.push(Constraint::Pattern(re));
    }
    if shape.path.is_none() && graph.values(s, RDF_TYPE).any(|o| matches!(o, RdfObject::NamedNode(n) if n.as_str() == format!("{SH}PropertyShape"))) {
        return Err(at("an sh:PropertyShape needs an sh:path".into()));
    }
    if shape.path.is_none() && stack.len() > 1 {
        return Err(at("a shape under sh:property needs an sh:path".into()));
    }
    if shape.path.is_some() && !shape.properties.is_empty() {
        return Err(at("sh:property inside a property shape is not supported yet".into()));
    }
    stack.pop();
    Ok(shape)
}

fn read_path(graph: &ShapesGraph, o: &RdfObject) -> Result<Path, String> {
    match o {
        RdfObject::NamedNode(n) => Ok(Path::Predicate(n.as_str().to_string())),
        RdfObject::BlankNode(b) => {
            let s = RdfSubject::BlankNode(b.clone());
            let inverse: Vec<_> = graph.values(&s, &format!("{SH}inversePath")).collect();
            match inverse.as_slice() {
                [RdfObject::NamedNode(p)] => Ok(Path::Inverse(p.as_str().to_string())),
                _ => Err("only a predicate path or [ sh:inversePath <predicate> ] is supported yet \
                          (sequence, alternative and repetition paths are not)"
                    .into()),
            }
        }
        RdfObject::Literal(l) => Err(format!("sh:path must be an IRI, got \"{}\"", l.value())),
    }
}

/// Check the data against the shapes in `shapes_turtle`.
///
/// `Err` is a shapes graph that cannot be read, or one that uses SHACL this
/// does not check; `Ok(vec![])` is a graph that conforms.
pub fn validate(shapes_turtle: &str, data: &dyn TripleSource) -> Result<Vec<ValidationResult>, String> {
    let shapes = read_shapes(shapes_turtle)?;
    let data = Data { source: data };
    let mut out = Vec::new();
    for shape in shapes.iter().filter(|s| !s.deactivated) {
        for focus in data.focus_nodes(&shape.targets)? {
            check(&data, shape, &focus, &mut out)?;
        }
    }
    Ok(out)
}

struct Data<'a> {
    source: &'a dyn TripleSource,
}

impl Data<'_> {
    fn matching(&self, s: Option<RdfSubject>, p: &str, o: Option<RdfObject>) -> Result<Vec<super::Triple>, String> {
        let p = RdfPredicate::new(p).map_err(|e| format!("{p} is not an IRI: {e}"))?;
        self.source.matching(&TriplePattern::new(s, Some(p), o))
    }

    fn iri(iri: &str) -> Result<RdfObject, String> {
        super::NamedNode::new(iri).map(RdfObject::NamedNode).map_err(|e| format!("{iri} is not an IRI: {e}"))
    }

    /// `class` and every class under it by `rdfs:subClassOf` in the data.
    fn class_and_below(&self, class: &str) -> Result<HashSet<String>, String> {
        let mut found = HashSet::from([class.to_string()]);
        let mut todo = vec![class.to_string()];
        while let Some(c) = todo.pop() {
            for t in self.matching(None, RDFS_SUBCLASS_OF, Some(Self::iri(&c)?))? {
                let sub = shape_name(&t.subject);
                if found.insert(sub.clone()) {
                    todo.push(sub);
                }
            }
        }
        Ok(found)
    }

    fn focus_nodes(&self, targets: &[Target]) -> Result<Vec<RdfObject>, String> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut add = |o: RdfObject| {
            if seen.insert(o.clone()) {
                out.push(o);
            }
        };
        for target in targets {
            match target {
                Target::Class(c) => {
                    let mut classes: Vec<String> = self.class_and_below(c)?.into_iter().collect();
                    classes.sort();
                    for class in classes {
                        for t in self.matching(None, RDF_TYPE, Some(Self::iri(&class)?))? {
                            add(object_of(&t.subject));
                        }
                    }
                }
                Target::Node(n) => add(n.clone()),
                Target::SubjectsOf(p) => self.matching(None, p, None)?.into_iter().for_each(|t| add(object_of(&t.subject))),
                Target::ObjectsOf(p) => self.matching(None, p, None)?.into_iter().for_each(|t| add(t.object)),
            }
        }
        Ok(out)
    }

    fn values(&self, focus: &RdfObject, path: &Path) -> Result<Vec<RdfObject>, String> {
        let mut out = Vec::new();
        match path {
            Path::Predicate(p) => {
                // A literal has no properties.
                if let Some(s) = subject_of(focus) {
                    out.extend(self.matching(Some(s), p, None)?.into_iter().map(|t| t.object));
                }
            }
            Path::Inverse(p) => {
                out.extend(self.matching(None, p, Some(focus.clone()))?.into_iter().map(|t| object_of(&t.subject)));
            }
        }
        Ok(out)
    }

    fn is_instance(&self, value: &RdfObject, class: &str) -> Result<bool, String> {
        let Some(s) = subject_of(value) else { return Ok(false) };
        let classes = self.class_and_below(class)?;
        Ok(self.matching(Some(s), RDF_TYPE, None)?.iter().any(|t| classes.contains(&text(&t.object))))
    }
}

fn check(data: &Data, shape: &Shape, focus: &RdfObject, out: &mut Vec<ValidationResult>) -> Result<(), String> {
    let values = match &shape.path {
        Some(path) => data.values(focus, path)?,
        None => vec![focus.clone()],
    };
    let mut report = |component: &str, default: String| {
        out.push(ValidationResult {
            focus_node: text(focus),
            result_path: shape.path.as_ref().map(Path::display),
            severity: shape.severity.clone(),
            source_constraint: format!("sh:{component}ConstraintComponent"),
            message: shape.message.clone().unwrap_or(default),
        });
    };
    for c in &shape.constraints {
        match c {
            Constraint::MinCount(n) if values.len() < *n => {
                report("MinCount", format!("{} value(s), at least {n} required", values.len()))
            }
            Constraint::MaxCount(n) if values.len() > *n => {
                report("MaxCount", format!("{} value(s), at most {n} allowed", values.len()))
            }
            Constraint::HasValue(v) if !values.contains(v) => report("HasValue", format!("missing the value {}", text(v))),
            Constraint::MinCount(_) | Constraint::MaxCount(_) | Constraint::HasValue(_) => {}
            _ => {
                for v in &values {
                    if let Some((component, why)) = fails(data, c, v)? {
                        report(component, why);
                    }
                }
            }
        }
    }
    for property in shape.properties.iter().filter(|p| !p.deactivated) {
        check(data, property, focus, out)?;
    }
    Ok(())
}

/// Why `v` fails `c`, if it does: the component name and a description.
fn fails(data: &Data, c: &Constraint, v: &RdfObject) -> Result<Option<(&'static str, String)>, String> {
    let shown = text(v);
    Ok(match c {
        Constraint::Datatype(d) => {
            let ok = matches!(v, RdfObject::Literal(l) if l.datatype().as_str() == d && well_formed(l));
            (!ok).then(|| ("Datatype", format!("{shown} is not a valid {}", short(d))))
        }
        Constraint::Class(class) => {
            (!data.is_instance(v, class)?).then(|| ("Class", format!("{shown} is not an instance of {class}")))
        }
        Constraint::NodeKind(kind) => {
            let ok = matches!(
                (kind, v),
                (NodeKind::Iri | NodeKind::BlankNodeOrIri | NodeKind::IriOrLiteral, RdfObject::NamedNode(_))
                    | (NodeKind::BlankNode | NodeKind::BlankNodeOrIri | NodeKind::BlankNodeOrLiteral, RdfObject::BlankNode(_))
                    | (NodeKind::Literal | NodeKind::BlankNodeOrLiteral | NodeKind::IriOrLiteral, RdfObject::Literal(_))
            );
            (!ok).then(|| ("NodeKind", format!("{shown} is not of node kind {kind:?}")))
        }
        Constraint::Range(bound, limit) => {
            let order = match v {
                RdfObject::Literal(l) => compare(l, limit),
                _ => None,
            };
            let (component, ok, word) = match bound {
                Bound::MinInclusive => ("MinInclusive", order.is_some_and(|o| o.is_ge()), ">="),
                Bound::MaxInclusive => ("MaxInclusive", order.is_some_and(|o| o.is_le()), "<="),
                Bound::MinExclusive => ("MinExclusive", order.is_some_and(|o| o.is_gt()), ">"),
                Bound::MaxExclusive => ("MaxExclusive", order.is_some_and(|o| o.is_lt()), "<"),
            };
            (!ok).then(|| (component, format!("{shown} is not {word} {}", limit.value())))
        }
        Constraint::MinLength(n) | Constraint::MaxLength(n) => {
            let min = matches!(c, Constraint::MinLength(_));
            let ok = match v {
                RdfObject::BlankNode(_) => false,
                _ => {
                    let len = shown.chars().count();
                    if min { len >= *n } else { len <= *n }
                }
            };
            let (component, word) = if min { ("MinLength", "at least") } else { ("MaxLength", "at most") };
            (!ok).then(|| (component, format!("{shown} is not {word} {n} characters long")))
        }
        Constraint::Pattern(re) => {
            let ok = !matches!(v, RdfObject::BlankNode(_)) && re.is_match(&shown);
            (!ok).then(|| ("Pattern", format!("{shown} does not match {}", re.as_str())))
        }
        Constraint::In(allowed) => (!allowed.contains(v)).then(|| ("In", format!("{shown} is not one of the allowed values"))),
        Constraint::MinCount(_) | Constraint::MaxCount(_) | Constraint::HasValue(_) => None,
    })
}

fn short(iri: &str) -> String {
    iri.strip_prefix(XSD).map(|l| format!("xsd:{l}")).unwrap_or_else(|| iri.to_string())
}

const NUMERIC: &[&str] = &[
    "integer", "decimal", "double", "float", "int", "long", "short", "byte", "nonNegativeInteger",
    "positiveInteger", "negativeInteger", "nonPositiveInteger", "unsignedInt", "unsignedLong",
    "unsignedShort", "unsignedByte",
];

fn is_numeric(l: &Literal) -> bool {
    l.datatype().as_str().strip_prefix(XSD).is_some_and(|d| NUMERIC.contains(&d))
}

/// Whether a literal's text is a value of its datatype, for the datatypes the
/// graph writes; other datatypes are taken as written.
fn well_formed(l: &Literal) -> bool {
    let d = l.datatype();
    match d.as_str().strip_prefix(XSD) {
        Some("integer") => l.value().parse::<i128>().is_ok(),
        Some("double" | "float" | "decimal") => l.value().parse::<f64>().is_ok(),
        Some("boolean") => matches!(l.value(), "true" | "false" | "1" | "0"),
        _ => true,
    }
}

/// `a` against `b`: numerically when both are numbers, by text when both have
/// the same datatype (ISO dates order that way), else not comparable.
fn compare(a: &Literal, b: &Literal) -> Option<std::cmp::Ordering> {
    if is_numeric(a) && is_numeric(b) {
        let (x, y) = (a.value().parse::<f64>().ok()?, b.value().parse::<f64>().ok()?);
        return x.partial_cmp(&y);
    }
    (a.datatype() == b.datatype() && a.language() == b.language()).then(|| a.value().cmp(b.value()))
}

#[cfg(test)]
mod tests {
    //! Data a property graph never produces (`rdfs:subClassOf`, an ill-formed
    //! literal), read from an RDF store instead of the graph.
    use super::*;
    use crate::rdf::RdfStore;

    fn store(turtle: &str) -> RdfStore {
        let mut s = RdfStore::new();
        for t in RdfParser::parse(turtle, RdfFormat::Turtle).unwrap() {
            s.insert(t).unwrap();
        }
        s
    }

    const P: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . \
                     @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . @prefix e: <http://e/> . ";

    fn focus(shapes: &str, data: &str) -> Vec<String> {
        let data = store(&format!("{P}{data}"));
        let mut v: Vec<String> =
            validate(&format!("{P}{shapes}"), &data).unwrap().into_iter().map(|r| r.focus_node).collect();
        v.sort();
        v
    }

    #[test]
    fn a_target_class_reaches_instances_of_its_subclasses() {
        let data = "e:Student rdfs:subClassOf e:Person . e:PhD rdfs:subClassOf e:Student . \
                    e:a a e:Person . e:b a e:Student . e:c a e:PhD . e:d a e:Thing .";
        let shapes = "e:S sh:targetClass e:Person ; sh:property [ sh:path e:name ; sh:minCount 1 ] .";
        assert_eq!(focus(shapes, data), ["http://e/a", "http://e/b", "http://e/c"]);
        let class = "e:S sh:targetNode e:x ; sh:property [ sh:path e:knows ; sh:class e:Person ] .";
        assert_eq!(focus(class, &format!("{data} e:x e:knows e:c , e:d .")), ["http://e/x"]);
    }

    #[test]
    fn a_literal_of_the_right_datatype_but_ill_formed_fails_sh_datatype() {
        let data = "e:a e:age \"12\"^^xsd:integer . e:b e:age \"twelve\"^^xsd:integer .";
        let shapes = "e:S sh:targetSubjectsOf e:age ; sh:property [ sh:path e:age ; sh:datatype xsd:integer ] .";
        assert_eq!(focus(shapes, data), ["http://e/b"]);
    }
}
