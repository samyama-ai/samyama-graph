//! Similarity graphs from a vector index (#1979): `vector.knnGraph` and
//! `vector.similarityJoin`.
//!
//! A vector database can answer "what is near this vector". A graph database
//! can also keep the answer: turn each node's nearest neighbours into
//! relationships and run graph algorithms -- communities, PageRank, entity
//! resolution -- over them.
//!
//! ```cypher
//! CALL vector.knnGraph('Doc', 'embedding', {k: 10, threshold: 0.8})
//! YIELD node1, node2, similarity
//!
//! CALL vector.knnGraph('Doc', 'embedding', {k: 10, write: true, relType: 'SIMILAR_TO'})
//!
//! CALL vector.similarityJoin('Person', 'embedding', 'Author', 'embedding', {threshold: 0.9})
//! ```
//!
//! Both stream `(node1, node2, similarity)` rows by default. `write: true`
//! stores each row as a `node1 -[relType {scoreProperty: similarity}]-> node2`
//! relationship instead, and needs write access to do it.
//!
//! What they are and are not:
//!
//! - The neighbours come from the HNSW index on `(label, property)` (on the
//!   second label for a join), so they are as approximate as the index:
//!   `ef` widens the search for recall, exactly as `queryNodes`' `{ef}` does.
//!   A small index (128 nodes or fewer) is searched exactly.
//! - `similarity` is the index metric's Neo4j score
//!   ([`crate::vector::DistanceMetric::score`]), the same number
//!   `queryNodes` yields as `score`, so a `threshold` means the same thing
//!   in both.
//! - The result is the **directed** k-NN graph: each source node's k nearest.
//!   Nearness is symmetric but "among my k nearest" is not, so A -> B can
//!   exist without B -> A. When both do, both are written.
//! - A threshold join is capped at `k` matches per source node. Every pair
//!   over a threshold is a range search, which HNSW does not answer; a large
//!   `k` approaches it.
//! - Running a write again does not duplicate: an existing relationship of
//!   `relType` between the same two nodes keeps its identity and has its
//!   score refreshed. Relationships from an earlier run that no longer make
//!   the cut are left as they are.

use std::collections::HashMap;

use rayon::prelude::*;

use crate::graph::{EdgeType, GraphStore, Label, NodeId, PropertyValue};
use crate::query::ast::Expression;
use crate::query::executor::operator::{eval_expression, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};
use crate::vector::DistanceMetric;

/// The two procedures, by the name a caller writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorProcedure {
    /// `vector.knnGraph(label, property, {config})`
    KnnGraph,
    /// `vector.similarityJoin(labelA, propertyA, labelB, propertyB, {config})`
    SimilarityJoin,
}

impl VectorProcedure {
    /// Which procedure `name` calls, case-insensitively, or `None`.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "vector.knngraph" => Some(Self::KnnGraph),
            "vector.similarityjoin" => Some(Self::SimilarityJoin),
            _ => None,
        }
    }

    /// The spelling used in messages.
    pub fn name(self) -> &'static str {
        match self {
            Self::KnnGraph => "vector.knnGraph",
            Self::SimilarityJoin => "vector.similarityJoin",
        }
    }

    /// How many positional (label, property) arguments precede the config.
    fn positional(self) -> usize {
        match self {
            Self::KnnGraph => 2,
            Self::SimilarityJoin => 4,
        }
    }
}

/// The config keys both procedures read.
const CONFIG_KEYS: &[&str] = &["k", "threshold", "ef", "write", "relType", "scoreProperty"];

/// The columns both procedures yield.
const COLUMNS: &[&str] = &["node1", "node2", "similarity"];

/// Does this `CALL` write? `Query::is_write()` asks, so the statement reaches
/// the write executor.
///
/// Any `write` other than a literal `false` counts, including a parameter or
/// a value of the wrong type: the write path then either writes or refuses
/// with the reason, where a read executor could only say it cannot write.
pub fn call_writes(name: &str, args: &[Expression]) -> bool {
    VectorProcedure::from_name(name).is_some()
        && args.iter().any(|arg| match arg {
            Expression::Literal(PropertyValue::Map(m)) => m
                .get("write")
                .is_some_and(|w| *w != PropertyValue::Boolean(false)),
            Expression::MapExpr(entries) => entries.iter().any(|(k, v)| {
                k == "write" && !matches!(v, Expression::Literal(PropertyValue::Boolean(false)))
            }),
            _ => false,
        })
}

/// What a call asked for, after its arguments are evaluated.
#[derive(Clone, Debug)]
struct Request {
    source: (String, String),
    target: (String, String),
    k: usize,
    threshold: Option<f64>,
    ef: Option<usize>,
    write: bool,
    rel_type: String,
    score_property: String,
}

/// `vector.knnGraph` / `vector.similarityJoin` as a physical operator.
pub struct VectorSimilarityGraphOperator {
    procedure: VectorProcedure,
    args: Vec<Expression>,
    /// Output variable per column, after `YIELD ... AS`.
    columns: Vec<(&'static str, String)>,
    writes: bool,
    rows: Option<std::vec::IntoIter<(NodeId, NodeId, f64)>>,
}

impl VectorSimilarityGraphOperator {
    /// Built by the planner. `yields` are `(column, alias)` pairs; an empty
    /// list yields every column under its own name.
    pub fn new(
        procedure: VectorProcedure,
        args: Vec<Expression>,
        yields: &[(String, Option<String>)],
    ) -> ExecutionResult<Self> {
        let columns = if yields.is_empty() {
            COLUMNS.iter().map(|c| (*c, c.to_string())).collect()
        } else {
            let mut columns = Vec::with_capacity(yields.len());
            for (name, alias) in yields {
                let Some(column) = COLUMNS.iter().find(|c| **c == name.as_str()) else {
                    return Err(ExecutionError::PlanningError(format!(
                        "{} yields {}; it has no `{name}` column",
                        procedure.name(),
                        COLUMNS.join(", ")
                    )));
                };
                columns.push((*column, alias.clone().unwrap_or_else(|| name.clone())));
            }
            columns
        };
        let writes = call_writes(procedure.name(), &args);
        Ok(Self {
            procedure,
            args,
            columns,
            writes,
            rows: None,
        })
    }

    fn request(&self, store: &GraphStore) -> ExecutionResult<Request> {
        let call = self.procedure.name();
        let positional = self.procedure.positional();
        if self.args.len() < positional || self.args.len() > positional + 1 {
            return Err(ExecutionError::bad_argument(match self.procedure {
                VectorProcedure::KnnGraph => {
                    format!("{call} takes (label, property) and an optional config map")
                }
                VectorProcedure::SimilarityJoin => format!(
                    "{call} takes (labelA, propertyA, labelB, propertyB) and an optional config map"
                ),
            }));
        }
        let empty = Record::new();
        let mut names = Vec::with_capacity(positional);
        for (i, arg) in self.args[..positional].iter().enumerate() {
            match eval_expression(arg, &empty, store)? {
                Value::Property(PropertyValue::String(s)) if !s.trim().is_empty() => names.push(s),
                other => return Err(ExecutionError::bad_argument(format!(
                    "{call}: argument {} must be a non-empty label or property name, got {other:?}",
                    i + 1
                ))),
            }
        }
        let config = match self.args.get(positional) {
            None => HashMap::new(),
            Some(arg) => match eval_expression(arg, &empty, store)? {
                Value::Property(PropertyValue::Map(m)) => m,
                Value::Null | Value::Property(PropertyValue::Null) => HashMap::new(),
                other => {
                    return Err(ExecutionError::bad_argument(format!(
                        "{call}: the last argument must be a config map, got {other:?}"
                    )))
                }
            },
        };
        let mut unknown: Vec<&String> = config
            .keys()
            .filter(|k| !CONFIG_KEYS.contains(&k.as_str()))
            .collect();
        unknown.sort();
        if let Some(first) = unknown.first() {
            return Err(ExecutionError::bad_argument(format!(
                "{call}: unknown config key `{first}`. It reads: {}",
                CONFIG_KEYS.join(", ")
            )));
        }

        let positive = |key: &str, default: Option<usize>| -> ExecutionResult<Option<usize>> {
            match config.get(key) {
                None | Some(PropertyValue::Null) => Ok(default),
                Some(PropertyValue::Integer(n)) if *n >= 1 => Ok(Some(*n as usize)),
                Some(other) => Err(ExecutionError::bad_argument(format!(
                    "{call}: `{key}` must be a positive integer, got {other:?}"
                ))),
            }
        };
        let k = positive("k", Some(10))?.unwrap_or(10);
        let ef = positive("ef", None)?;
        let threshold = match config.get("threshold") {
            None | Some(PropertyValue::Null) => None,
            Some(PropertyValue::Float(f)) if f.is_finite() => Some(*f),
            Some(PropertyValue::Integer(i)) => Some(*i as f64),
            Some(other) => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call}: `threshold` must be a number, got {other:?}"
                )))
            }
        };
        let write = match config.get("write") {
            None => false,
            Some(PropertyValue::Boolean(b)) => *b,
            Some(other) => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call}: `write` must be a boolean, got {other:?}"
                )))
            }
        };
        let name = |key: &str, default: &str| -> ExecutionResult<String> {
            match config.get(key) {
                None => Ok(default.to_string()),
                Some(_) if !write => Err(ExecutionError::bad_argument(format!(
                    "{call}: `{key}` names what a write stores, and this call does not write. \
                     Add `write: true`, or drop `{key}`."
                ))),
                Some(PropertyValue::String(s)) if !s.trim().is_empty() => Ok(s.clone()),
                Some(other) => Err(ExecutionError::bad_argument(format!(
                    "{call}: `{key}` must be a non-empty name, got {other:?}"
                ))),
            }
        };
        let rel_type = name("relType", "SIMILAR_TO")?;
        let score_property = name("scoreProperty", "score")?;

        let source = (names[0].clone(), names[1].clone());
        let target = if positional == 4 {
            (names[2].clone(), names[3].clone())
        } else {
            source.clone()
        };
        Ok(Request {
            source,
            target,
            k,
            threshold,
            ef,
            write,
            rel_type,
            score_property,
        })
    }

    /// The rows, in source-node order and nearest-first within a source.
    fn compute(
        store: &GraphStore,
        call: &str,
        req: &Request,
    ) -> ExecutionResult<Vec<(NodeId, NodeId, f64)>> {
        let (target_label, target_property) = (&req.target.0, &req.target.1);
        let no_index = || {
            ExecutionError::RuntimeError(format!(
                "{call}: no vector index on :{target_label}({target_property}). \
                 CREATE VECTOR INDEX FOR (n:{target_label}) ON (n.{target_property}) \
                 OPTIONS {{dimensions: <d>}} creates one."
            ))
        };
        let metric: DistanceMetric = store
            .vector_index
            .metric(target_label, target_property)
            .ok_or_else(no_index)?;
        let dimensions = store
            .vector_index
            .get_index(target_label, target_property)
            .ok_or_else(no_index)?
            .read()
            .map_err(|_| {
                ExecutionError::RuntimeError(format!("{call}: vector index lock poisoned"))
            })?
            .dimensions();

        // Sources: every node with the label and a vector of the index's
        // width. A node without one has no neighbours to report; one of
        // another width cannot be compared, and is skipped rather than
        // failing the whole graph.
        let mut sources: Vec<(NodeId, Vec<f32>)> = store
            .get_nodes_by_label(&Label::new(req.source.0.as_str()))
            .into_iter()
            .filter_map(|n| {
                let v = store.node_property(n.id, &req.source.1)?.to_vector()?;
                (v.len() == dimensions).then_some((n.id, v))
            })
            .collect();
        sources.sort_by_key(|(id, _)| *id);

        let per_source: Vec<ExecutionResult<Vec<(NodeId, NodeId, f64)>>> = sources
            .par_iter()
            .map(|(src, v)| {
                let src = *src;
                let hits = store
                    .vector_search_where(target_label, target_property, v, req.k, req.ef, |id| {
                        id != src
                    })
                    .map_err(|e| ExecutionError::RuntimeError(format!("{call}: {e}")))?;
                let mut rows: Vec<(NodeId, NodeId, f64)> = hits
                    .into_iter()
                    .map(|(dst, d)| (src, dst, metric.score(d)))
                    .filter(|(_, _, s)| req.threshold.is_none_or(|t| *s >= t))
                    .collect();
                // Nearest first, ties by node id, so the order is reproducible.
                rows.sort_by(|a, b| b.2.total_cmp(&a.2).then(a.1.cmp(&b.1)));
                Ok(rows)
            })
            .collect();
        let mut rows = Vec::new();
        for r in per_source {
            rows.extend(r?);
        }
        Ok(rows)
    }

    /// Store each row as a relationship: refresh the score on an existing
    /// one of the same type between the same two nodes, create it otherwise.
    fn write(
        store: &mut GraphStore,
        call: &str,
        req: &Request,
        rows: &[(NodeId, NodeId, f64)],
    ) -> ExecutionResult<()> {
        let rel = EdgeType::new(req.rel_type.as_str());
        let mut i = 0;
        while i < rows.len() {
            let src = rows[i].0;
            let existing: HashMap<NodeId, crate::graph::EdgeId> = store
                .get_outgoing_edges(src)
                .into_iter()
                .filter(|e| e.edge_type == rel)
                .map(|e| (e.target, e.id))
                .collect();
            while i < rows.len() && rows[i].0 == src {
                let (_, dst, score) = rows[i];
                let edge = match existing.get(&dst) {
                    Some(id) => *id,
                    None => store.create_edge(src, dst, rel.clone()).map_err(|e| {
                        ExecutionError::RuntimeError(format!(
                            "{call}: failed to create a relationship: {e}"
                        ))
                    })?,
                };
                store
                    .set_edge_property(
                        edge,
                        req.score_property.clone(),
                        PropertyValue::Float(score),
                    )
                    .map_err(|e| {
                        ExecutionError::RuntimeError(format!(
                            "{call}: failed to write `{}`: {e}",
                            req.score_property
                        ))
                    })?;
                i += 1;
            }
        }
        Ok(())
    }

    fn row(&self, (a, b, s): (NodeId, NodeId, f64)) -> Record {
        let mut r = Record::new();
        for (column, var) in &self.columns {
            let value = match *column {
                "node1" => Value::NodeRef(a),
                "node2" => Value::NodeRef(b),
                _ => Value::Property(PropertyValue::Float(s)),
            };
            r.bind(var.clone(), value);
        }
        r
    }
}

impl PhysicalOperator for VectorSimilarityGraphOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            let call = self.procedure.name();
            let req = self.request(store)?;
            if req.write {
                return Err(ExecutionError::RuntimeError(format!(
                    "{call} with `write: true` requires write access (MutQueryExecutor): \
                     it creates `{}` relationships",
                    req.rel_type
                )));
            }
            self.rows = Some(Self::compute(store, call, &req)?.into_iter());
        }
        Ok(self
            .rows
            .as_mut()
            .and_then(|r| r.next())
            .map(|row| self.row(row)))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            let call = self.procedure.name();
            let req = self.request(store)?;
            let rows = Self::compute(store, call, &req)?;
            if req.write {
                Self::write(store, call, &req, &rows)?;
            }
            self.rows = Some(rows.into_iter());
        }
        Ok(self
            .rows
            .as_mut()
            .and_then(|r| r.next())
            .map(|row| self.row(row)))
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn is_mutating(&self) -> bool {
        self.writes
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: match self.procedure {
                VectorProcedure::KnnGraph => "VectorKnnGraph",
                VectorProcedure::SimilarityJoin => "VectorSimilarityJoin",
            }
            .to_string(),
            details: if self.writes {
                "write".to_string()
            } else {
                "stream".to_string()
            },
            children: Vec::new(),
        }
    }
}
