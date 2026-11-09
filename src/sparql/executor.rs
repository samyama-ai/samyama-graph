//! SPARQL query executor: evaluates a parsed query's algebra over an [`RdfStore`].
//!
//! Covers what most SELECT and ASK queries use: basic graph patterns, joins,
//! `OPTIONAL`, `UNION`, `FILTER` (comparisons, `&&`, `||`, `!`, `BOUND`),
//! `ORDER BY`, `DISTINCT`, `LIMIT` and `OFFSET`. Anything else is refused with
//! [`ExecutionError::Unsupported`], never answered with an empty result: an
//! empty answer to a question the engine did not understand reads as "no
//! matches", which is the wrong thing to tell a caller.

use super::results::QuerySolution;
use crate::rdf::{
    Literal, NamedNode, RdfObject, RdfPredicate, RdfStore, RdfSubject, RdfTerm, TriplePattern,
};
use spargebra::algebra::{Expression, GraphPattern, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern};
use std::cmp::Ordering;
use std::collections::HashSet;
use thiserror::Error;

/// Execution errors
#[derive(Error, Debug)]
pub enum ExecutionError {
    /// Query error
    #[error("Query error: {0}")]
    Query(String),

    /// Type mismatch
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    /// A construct this executor does not evaluate yet.
    #[error("Unsupported SPARQL feature: {0}")]
    Unsupported(String),
}

type Solutions = Vec<QuerySolution>;
type Result<T> = std::result::Result<T, ExecutionError>;

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// SPARQL query executor
pub struct SparqlExecutor {
    store: RdfStore,
}

impl SparqlExecutor {
    /// Create a new executor
    pub fn new(store: RdfStore) -> Self {
        Self { store }
    }

    /// Every solution of `pattern`, in the order the algebra defines.
    pub fn evaluate(&self, pattern: &GraphPattern) -> Result<Solutions> {
        match pattern {
            GraphPattern::Bgp { patterns } => {
                let mut sols = vec![QuerySolution::new()];
                for tp in patterns {
                    sols = self.match_triple(tp, sols)?;
                }
                Ok(sols)
            }
            GraphPattern::Join { left, right } => {
                Ok(join(&self.evaluate(left)?, &self.evaluate(right)?))
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let right = self.evaluate(right)?;
                let mut out = Vec::new();
                for l in self.evaluate(left)? {
                    let mut matched = false;
                    for r in &right {
                        if let Some(m) = merge(&l, r) {
                            let keep = match expression {
                                Some(e) => ebv(e, &m)? == Some(true),
                                None => true,
                            };
                            if keep {
                                matched = true;
                                out.push(m);
                            }
                        }
                    }
                    if !matched {
                        out.push(l);
                    }
                }
                Ok(out)
            }
            GraphPattern::Union { left, right } => {
                let mut out = self.evaluate(left)?;
                out.extend(self.evaluate(right)?);
                Ok(out)
            }
            GraphPattern::Filter { expr, inner } => {
                let mut out = Vec::new();
                for s in self.evaluate(inner)? {
                    if ebv(expr, &s)? == Some(true) {
                        out.push(s);
                    }
                }
                Ok(out)
            }
            GraphPattern::Project { inner, variables } => Ok(self
                .evaluate(inner)?
                .into_iter()
                .map(|s| {
                    let mut p = QuerySolution::new();
                    for v in variables {
                        if let Some(t) = s.get(v.as_str()) {
                            p.bind(v.as_str().to_string(), t.clone());
                        }
                    }
                    p
                })
                .collect()),
            GraphPattern::Distinct { inner } | GraphPattern::Reduced { inner } => {
                let mut seen = HashSet::new();
                Ok(self
                    .evaluate(inner)?
                    .into_iter()
                    .filter(|s| {
                        let mut key: Vec<_> = s.bindings.iter().collect();
                        key.sort_by(|a, b| a.0.cmp(b.0));
                        seen.insert(format!("{key:?}"))
                    })
                    .collect())
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                let rows = self.evaluate(inner)?.into_iter().skip(*start);
                Ok(match length {
                    Some(n) => rows.take(*n).collect(),
                    None => rows.collect(),
                })
            }
            GraphPattern::OrderBy { inner, expression } => {
                let mut rows = Vec::new();
                for s in self.evaluate(inner)? {
                    let mut keys = Vec::new();
                    for o in expression {
                        let (e, desc) = match o {
                            OrderExpression::Asc(e) => (e, false),
                            OrderExpression::Desc(e) => (e, true),
                        };
                        keys.push((value(e, &s)?, desc));
                    }
                    rows.push((keys, s));
                }
                rows.sort_by(|(a, _), (b, _)| {
                    a.iter()
                        .zip(b)
                        .map(|((x, desc), (y, _))| {
                            let o = order(x.as_ref(), y.as_ref());
                            if *desc {
                                o.reverse()
                            } else {
                                o
                            }
                        })
                        .find(|o| *o != Ordering::Equal)
                        .unwrap_or(Ordering::Equal)
                });
                Ok(rows.into_iter().map(|(_, s)| s).collect())
            }
            other => Err(ExecutionError::Unsupported(pattern_name(other).to_string())),
        }
    }

    /// Extend each solution with every triple matching `tp` under its bindings.
    fn match_triple(
        &self,
        tp: &spargebra::term::TriplePattern,
        sols: Solutions,
    ) -> Result<Solutions> {
        let mut out = Vec::new();
        for s in sols {
            let subject = match bound(&tp.subject, &s)? {
                Some(RdfTerm::NamedNode(n)) => Some(RdfSubject::NamedNode(n)),
                Some(RdfTerm::BlankNode(b)) => Some(RdfSubject::BlankNode(b)),
                Some(RdfTerm::Literal(_)) => continue, // a literal is never a subject
                None => None,
            };
            let predicate = match &tp.predicate {
                NamedNodePattern::NamedNode(n) => {
                    Some(RdfPredicate::from(NamedNode::from(n.clone())))
                }
                NamedNodePattern::Variable(v) => match s.get(v.as_str()) {
                    Some(RdfTerm::NamedNode(n)) => Some(RdfPredicate::from(n.clone())),
                    Some(_) => continue,
                    None => None,
                },
            };
            let object = bound(&tp.object, &s)?.map(|t| match t {
                RdfTerm::NamedNode(n) => RdfObject::NamedNode(n),
                RdfTerm::BlankNode(b) => RdfObject::BlankNode(b),
                RdfTerm::Literal(l) => RdfObject::Literal(l),
            });
            for t in self
                .store
                .query(&TriplePattern::new(subject, predicate, object))
            {
                let mut m = s.clone();
                let ok = bind(&mut m, &tp.subject, t.subject.into())
                    && bind_predicate(&mut m, &tp.predicate, t.predicate)
                    && bind(&mut m, &tp.object, t.object.into());
                if ok {
                    out.push(m);
                }
            }
        }
        Ok(out)
    }
}

/// The variable a pattern position binds. A blank node in a query is a
/// variable that is not projected, so it gets a name no `?var` can have.
fn var_name(p: &TermPattern) -> Option<String> {
    match p {
        TermPattern::Variable(v) => Some(v.as_str().to_string()),
        TermPattern::BlankNode(b) => Some(format!("_:{}", b.as_str())),
        _ => None,
    }
}

/// The term at a pattern position under `s`, or `None` if it is still free.
fn bound(p: &TermPattern, s: &QuerySolution) -> Result<Option<RdfTerm>> {
    if let Some(v) = var_name(p) {
        return Ok(s.get(&v).cloned());
    }
    match p {
        TermPattern::NamedNode(n) => Ok(Some(RdfTerm::NamedNode(NamedNode::from(n.clone())))),
        TermPattern::Literal(l) => Ok(Some(RdfTerm::Literal(Literal::from(l.clone())))),
        _ => Err(ExecutionError::Unsupported(
            "RDF-star triple patterns".into(),
        )),
    }
}

/// Bind a free variable, or check an already-bound one agrees (`?x :p ?x`).
fn bind(s: &mut QuerySolution, p: &TermPattern, t: RdfTerm) -> bool {
    match var_name(p) {
        Some(v) => match s.get(&v) {
            Some(existing) => *existing == t,
            None => {
                s.bind(v, t);
                true
            }
        },
        None => true,
    }
}

fn bind_predicate(s: &mut QuerySolution, p: &NamedNodePattern, t: RdfPredicate) -> bool {
    match p {
        NamedNodePattern::Variable(v) => bind(
            s,
            &TermPattern::Variable(v.clone()),
            RdfTerm::NamedNode(t.into()),
        ),
        NamedNodePattern::NamedNode(_) => true,
    }
}

/// Two solutions combined, if they agree on every variable they share.
fn merge(a: &QuerySolution, b: &QuerySolution) -> Option<QuerySolution> {
    let mut m = a.clone();
    for (k, v) in &b.bindings {
        match m.get(k) {
            Some(x) if x != v => return None,
            Some(_) => {}
            None => m.bind(k.clone(), v.clone()),
        }
    }
    Some(m)
}

fn join(left: &Solutions, right: &Solutions) -> Solutions {
    left.iter()
        .flat_map(|l| right.iter().filter_map(move |r| merge(l, r)))
        .collect()
}

fn number(t: &RdfTerm) -> Option<f64> {
    let RdfTerm::Literal(l) = t else { return None };
    let dt = l.datatype();
    let local = dt.as_str().strip_prefix(XSD)?;
    let numeric = matches!(
        local,
        "integer"
            | "decimal"
            | "double"
            | "float"
            | "int"
            | "long"
            | "short"
            | "byte"
            | "nonNegativeInteger"
            | "positiveInteger"
            | "negativeInteger"
            | "nonPositiveInteger"
            | "unsignedInt"
            | "unsignedLong"
            | "unsignedShort"
            | "unsignedByte"
    );
    if numeric {
        l.value().parse().ok()
    } else {
        None
    }
}

/// A plain or `xsd:string` literal's text: the strings SPARQL compares.
fn string(t: &RdfTerm) -> Option<&str> {
    let RdfTerm::Literal(l) = t else { return None };
    (l.language().is_none() && l.datatype().as_str() == format!("{XSD}string")).then(|| l.value())
}

/// SPARQL value comparison. `None` is a type error, which a FILTER treats as false.
fn compare(a: &RdfTerm, b: &RdfTerm) -> Option<Ordering> {
    if let (Some(x), Some(y)) = (number(a), number(b)) {
        return x.partial_cmp(&y);
    }
    if let (Some(x), Some(y)) = (string(a), string(b)) {
        return Some(x.cmp(y));
    }
    None
}

/// ORDER BY's total order: unbound first, then by value, then by text.
fn order(a: Option<&RdfTerm>, b: Option<&RdfTerm>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Less,
        (_, None) => Ordering::Greater,
        (Some(x), Some(y)) => compare(x, y).unwrap_or_else(|| text(x).cmp(&text(y))),
    }
}

fn text(t: &RdfTerm) -> String {
    match t {
        RdfTerm::NamedNode(n) => n.as_str().to_string(),
        RdfTerm::BlankNode(b) => b.as_str().to_string(),
        RdfTerm::Literal(l) => l.value().to_string(),
    }
}

/// An expression's value under `s`; `None` when unbound or an error.
fn value(e: &Expression, s: &QuerySolution) -> Result<Option<RdfTerm>> {
    Ok(match e {
        Expression::Variable(v) => s.get(v.as_str()).cloned(),
        Expression::NamedNode(n) => Some(RdfTerm::NamedNode(NamedNode::from(n.clone()))),
        Expression::Literal(l) => Some(RdfTerm::Literal(Literal::from(l.clone()))),
        _ => ebv(e, s)?.map(|b| {
            RdfTerm::Literal(Literal::new_typed_literal(
                b.to_string(),
                NamedNode::new(&format!("{XSD}boolean")).expect("xsd:boolean is an IRI"),
            ))
        }),
    })
}

/// Effective boolean value; `None` is SPARQL's error, which FILTER drops.
fn ebv(e: &Expression, s: &QuerySolution) -> Result<Option<bool>> {
    let cmp =
        |a: &Expression, b: &Expression, want: fn(Ordering) -> bool| -> Result<Option<bool>> {
            Ok(match (value(a, s)?, value(b, s)?) {
                (Some(x), Some(y)) => compare(&x, &y).map(want),
                _ => None,
            })
        };
    match e {
        Expression::And(a, b) => Ok(match (ebv(a, s)?, ebv(b, s)?) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        }),
        Expression::Or(a, b) => Ok(match (ebv(a, s)?, ebv(b, s)?) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }),
        Expression::Not(a) => Ok(ebv(a, s)?.map(|b| !b)),
        Expression::Bound(v) => Ok(Some(s.get(v.as_str()).is_some())),
        Expression::Equal(a, b) => Ok(match (value(a, s)?, value(b, s)?) {
            (Some(x), Some(y)) => Some(compare(&x, &y).map_or(x == y, |o| o == Ordering::Equal)),
            _ => None,
        }),
        Expression::SameTerm(a, b) => Ok(match (value(a, s)?, value(b, s)?) {
            (Some(x), Some(y)) => Some(x == y),
            _ => None,
        }),
        Expression::Greater(a, b) => cmp(a, b, |o| o == Ordering::Greater),
        Expression::GreaterOrEqual(a, b) => cmp(a, b, |o| o != Ordering::Less),
        Expression::Less(a, b) => cmp(a, b, |o| o == Ordering::Less),
        Expression::LessOrEqual(a, b) => cmp(a, b, |o| o != Ordering::Greater),
        Expression::Variable(_) | Expression::Literal(_) | Expression::NamedNode(_) => {
            Ok(value(e, s)?.and_then(|t| {
                if let Some(n) = number(&t) {
                    return Some(n != 0.0 && !n.is_nan());
                }
                if let Some(x) = string(&t) {
                    return Some(!x.is_empty());
                }
                match &t {
                    RdfTerm::Literal(l) if l.datatype().as_str() == format!("{XSD}boolean") => {
                        Some(l.value() == "true" || l.value() == "1")
                    }
                    _ => None,
                }
            }))
        }
        other => Err(ExecutionError::Unsupported(format!("expression {other}"))),
    }
}

fn pattern_name(p: &GraphPattern) -> &'static str {
    match p {
        GraphPattern::Path { .. } => "property paths",
        GraphPattern::Graph { .. } => "GRAPH",
        GraphPattern::Extend { .. } => "BIND and computed SELECT expressions",
        GraphPattern::Minus { .. } => "MINUS",
        GraphPattern::Values { .. } => "VALUES",
        GraphPattern::Group { .. } => "GROUP BY and aggregates",
        GraphPattern::Service { .. } => "SERVICE",
        _ => "this graph pattern",
    }
}
