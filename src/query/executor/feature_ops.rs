//! The `feature.*` procedures (ML-07, #1742): declarative feature definitions
//! and their batch materialization. What a feature is, and what point-in-time
//! means here, is in [`crate::graph::feature`].
//!
//! | Call | Yields | Writes |
//! |---|---|---|
//! | `feature.define(name, {config})` | `name, pattern, aggregate, replaced` | the registry |
//! | `feature.materialize(name, {asOf}?)` | `name, entities, asOf` | the registry |
//! | `feature.get(name)` | `node, value, asOf` | no |
//! | `feature.list()` | `name, pattern, aggregate, timeProperty, entities, asOf, stale` | no |
//! | `feature.drop(name)` | `name` | the registry |
//!
//! The three that change the registry take the write path, as `CREATE GRAPH
//! PROJECTION` does: they keep or free memory for later queries, which a
//! read-only caller should not be able to do.

use std::collections::HashMap;

use crate::graph::feature::{
    Aggregate, Direction, FeatureDefinition, FeatureError, FeatureInfo, TimeOn,
};
use crate::graph::{GraphStore, PropertyValue};
use crate::query::ast::Expression;
use crate::query::executor::operator::{eval_expression, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// The procedures, by the name a caller writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeatureProcedure {
    Define,
    Materialize,
    Get,
    List,
    Drop,
}

/// The keys `feature.define` reads.
pub const DEFINE_KEYS: &[&str] = &[
    "label",
    "edgeType",
    "direction",
    "targetLabel",
    "aggregate",
    "property",
    "timeProperty",
    "timeOn",
];

/// The keys `feature.materialize` reads.
pub const MATERIALIZE_KEYS: &[&str] = &["asOf"];

impl FeatureProcedure {
    /// Which procedure `name` calls, case-insensitively, or `None`.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "feature.define" => Some(Self::Define),
            "feature.materialize" | "feature.materialise" => Some(Self::Materialize),
            "feature.get" => Some(Self::Get),
            "feature.list" => Some(Self::List),
            "feature.drop" => Some(Self::Drop),
            _ => None,
        }
    }

    /// Every spelling a caller can use, for "did you mean".
    pub const NAMES: &'static [&'static str] = &[
        "feature.define",
        "feature.materialize",
        "feature.get",
        "feature.list",
        "feature.drop",
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Define => "feature.define",
            Self::Materialize => "feature.materialize",
            Self::Get => "feature.get",
            Self::List => "feature.list",
            Self::Drop => "feature.drop",
        }
    }

    /// Whether it changes the registry, and so needs the write path.
    pub fn writes(self) -> bool {
        matches!(self, Self::Define | Self::Materialize | Self::Drop)
    }

    pub fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Define => &["name", "pattern", "aggregate", "replaced"],
            Self::Materialize => &["name", "entities", "asOf"],
            Self::Get => &["node", "value", "asOf"],
            Self::List => &[
                "name",
                "pattern",
                "aggregate",
                "timeProperty",
                "entities",
                "asOf",
                "stale",
            ],
            Self::Drop => &["name"],
        }
    }
}

/// Does this `CALL` change the feature registry? `Query::is_write()` asks.
pub fn call_writes(name: &str) -> bool {
    FeatureProcedure::from_name(name).is_some_and(FeatureProcedure::writes)
}

/// One `feature.*` call as a physical operator.
pub struct FeatureOperator {
    procedure: FeatureProcedure,
    args: Vec<Expression>,
    /// Output variable per column, after `YIELD ... AS`.
    columns: Vec<(&'static str, String)>,
    rows: Option<std::vec::IntoIter<Record>>,
}

fn refused(e: FeatureError) -> ExecutionError {
    ExecutionError::bad_argument(e.to_string())
}

fn string(s: impl Into<String>) -> Value {
    Value::Property(PropertyValue::String(s.into()))
}

fn int(n: usize) -> Value {
    Value::Property(PropertyValue::Integer(i64::try_from(n).unwrap_or(i64::MAX)))
}

fn opt(v: Option<PropertyValue>) -> Value {
    Value::Property(v.unwrap_or(PropertyValue::Null))
}

impl FeatureOperator {
    /// Built by the planner. `yields` are `(column, alias)` pairs; an empty
    /// list yields every column under its own name.
    pub fn new(
        procedure: FeatureProcedure,
        args: Vec<Expression>,
        yields: &[(String, Option<String>)],
    ) -> ExecutionResult<Self> {
        let all = procedure.columns();
        let columns = if yields.is_empty() {
            all.iter().map(|c| (*c, c.to_string())).collect()
        } else {
            let mut columns = Vec::with_capacity(yields.len());
            for (name, alias) in yields {
                let Some(column) = all.iter().find(|c| **c == name.as_str()) else {
                    return Err(ExecutionError::PlanningError(format!(
                        "{} yields {}; it has no `{name}` column",
                        procedure.name(),
                        all.join(", ")
                    )));
                };
                columns.push((*column, alias.clone().unwrap_or_else(|| name.clone())));
            }
            columns
        };
        Ok(Self {
            procedure,
            args,
            columns,
            rows: None,
        })
    }

    /// The arguments, evaluated: `(positional strings, config map)`.
    fn arguments(
        &self,
        store: &GraphStore,
        positional: usize,
        keys: &[&str],
    ) -> ExecutionResult<(Vec<String>, HashMap<String, PropertyValue>)> {
        let call = self.procedure.name();
        let takes_config = !keys.is_empty();
        let max = positional + usize::from(takes_config);
        if self.args.len() < positional || self.args.len() > max {
            let shape = match (positional, takes_config) {
                (0, _) => "no arguments".to_string(),
                (_, false) => "(name)".to_string(),
                (_, true) => format!("(name) and an optional map of {}", keys.join(", ")),
            };
            return Err(ExecutionError::bad_argument(format!(
                "{call} takes {shape}; got {} argument(s)",
                self.args.len()
            )));
        }
        let empty = Record::new();
        let mut names = Vec::with_capacity(positional);
        for arg in &self.args[..positional] {
            match eval_expression(arg, &empty, store)? {
                Value::Property(PropertyValue::String(s)) if !s.trim().is_empty() => names.push(s),
                other => {
                    return Err(ExecutionError::bad_argument(format!(
                        "{call}: the feature name must be a non-empty string, got {other:?}"
                    )))
                }
            }
        }
        let config = match self.args.get(positional) {
            None => HashMap::new(),
            Some(arg) => match eval_expression(arg, &empty, store)? {
                Value::Property(PropertyValue::Map(m)) => m,
                Value::Null | Value::Property(PropertyValue::Null) => HashMap::new(),
                other => {
                    return Err(ExecutionError::bad_argument(format!(
                        "{call}: the last argument must be a map, got {other:?}"
                    )))
                }
            },
        };
        let mut unknown: Vec<&String> = config
            .keys()
            .filter(|k| !keys.contains(&k.as_str()))
            .collect();
        unknown.sort();
        if let Some(first) = unknown.first() {
            return Err(ExecutionError::bad_argument(format!(
                "{call}: unknown key `{first}`. It reads: {}",
                keys.join(", ")
            )));
        }
        Ok((names, config))
    }

    /// `feature.define`'s config, checked.
    fn definition(
        call: &str,
        config: &HashMap<String, PropertyValue>,
    ) -> ExecutionResult<FeatureDefinition> {
        let text = |key: &str| -> ExecutionResult<Option<String>> {
            match config.get(key) {
                None | Some(PropertyValue::Null) => Ok(None),
                Some(PropertyValue::String(s)) if !s.trim().is_empty() => Ok(Some(s.clone())),
                Some(other) => Err(ExecutionError::bad_argument(format!(
                    "{call}: `{key}` must be a non-empty string, got {other:?}"
                ))),
            }
        };
        let one_of = |key: &str, names: &[&str]| -> ExecutionResult<Option<String>> {
            text(key).map_err(|_| {
                ExecutionError::bad_argument(format!(
                    "{call}: `{key}` must be one of {}",
                    names.join(", ")
                ))
            })
        };
        let label = text("label")?.ok_or_else(|| {
            ExecutionError::bad_argument(format!(
                "{call}: `label` is required: it names the nodes that get a value"
            ))
        })?;
        let aggregate = match one_of("aggregate", Aggregate::NAMES)? {
            None => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call}: `aggregate` is required; use one of {}",
                    Aggregate::NAMES.join(", ")
                )))
            }
            Some(a) => Aggregate::from_name(&a).ok_or_else(|| {
                ExecutionError::bad_argument(format!(
                    "{call}: unknown aggregate `{a}`; use one of {}",
                    Aggregate::NAMES.join(", ")
                ))
            })?,
        };
        let direction = match one_of("direction", Direction::NAMES)? {
            None => Direction::Out,
            Some(d) => Direction::from_name(&d).ok_or_else(|| {
                ExecutionError::bad_argument(format!(
                    "{call}: unknown direction `{d}`; use one of {}",
                    Direction::NAMES.join(", ")
                ))
            })?,
        };
        let time_property = text("timeProperty")?;
        let time_on = match one_of("timeOn", TimeOn::NAMES)? {
            None => TimeOn::Relationship,
            Some(_) if time_property.is_none() => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call}: `timeOn` says where `timeProperty` is read, and there is no \
                     `timeProperty`; add one, or drop `timeOn`"
                )))
            }
            Some(t) => TimeOn::from_name(&t).ok_or_else(|| {
                ExecutionError::bad_argument(format!(
                    "{call}: unknown timeOn `{t}`; use one of {}",
                    TimeOn::NAMES.join(", ")
                ))
            })?,
        };
        let property = text("property")?;
        match (aggregate, &property) {
            (Aggregate::Count, Some(_)) => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call}: `count` counts matches and reads no property; drop `property`, \
                     or use sum, avg, min or max"
                )))
            }
            (Aggregate::Count, None) | (_, Some(_)) => {}
            (agg, None) => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call}: `{}` needs `property`, the neighbour property it aggregates",
                    agg.name()
                )))
            }
        }
        Ok(FeatureDefinition {
            label,
            edge_type: text("edgeType")?,
            direction,
            target_label: text("targetLabel")?,
            aggregate,
            property,
            time_property,
            time_on,
        })
    }

    fn row(&self, cells: &[(&str, Value)]) -> Record {
        let mut record = Record::new();
        for (column, alias) in &self.columns {
            if let Some((_, v)) = cells.iter().find(|(c, _)| c == column) {
                record.bind(alias.clone(), v.clone());
            }
        }
        record
    }

    fn info_row(&self, info: FeatureInfo) -> Record {
        self.row(&[
            ("name", string(info.name)),
            ("pattern", string(info.definition.pattern())),
            ("aggregate", string(info.definition.aggregate.name())),
            (
                "timeProperty",
                opt(info.definition.time_property.map(PropertyValue::String)),
            ),
            ("entities", info.entities.map_or(Value::Null, int)),
            ("asOf", opt(info.as_of)),
            ("stale", Value::Property(PropertyValue::Boolean(info.stale))),
        ])
    }

    fn compute(&self, store: &GraphStore) -> ExecutionResult<Vec<Record>> {
        let call = self.procedure.name();
        let registry = std::sync::Arc::clone(&store.features);
        Ok(match self.procedure {
            FeatureProcedure::Define => {
                let (names, config) = self.arguments(store, 1, DEFINE_KEYS)?;
                let definition = Self::definition(call, &config)?;
                let pattern = definition.pattern();
                let aggregate = definition.aggregate.name();
                let replaced = registry.define(&names[0], definition).map_err(refused)?;
                vec![self.row(&[
                    ("name", string(names[0].clone())),
                    ("pattern", string(pattern)),
                    ("aggregate", string(aggregate)),
                    (
                        "replaced",
                        Value::Property(PropertyValue::Boolean(replaced)),
                    ),
                ])]
            }
            FeatureProcedure::Materialize => {
                let (names, config) = self.arguments(store, 1, MATERIALIZE_KEYS)?;
                let as_of = config.get("asOf").filter(|v| !v.is_null()).cloned();
                let m = registry
                    .materialize(store, &names[0], as_of)
                    .map_err(refused)?;
                vec![self.row(&[
                    ("name", string(names[0].clone())),
                    ("entities", int(m.values.len())),
                    ("asOf", opt(m.as_of.clone())),
                ])]
            }
            FeatureProcedure::Get => {
                let (names, _) = self.arguments(store, 1, &[])?;
                let Some(m) = registry.values(&names[0]).map_err(refused)? else {
                    return Err(ExecutionError::bad_argument(format!(
                        "{call}: feature `{}` has not been materialized; CALL \
                         feature.materialize('{}') first",
                        names[0], names[0]
                    )));
                };
                let as_of = opt(m.as_of.clone());
                m.values
                    .iter()
                    // A node deleted since the materialization has no row:
                    // there is nothing to bind `node` to.
                    .filter_map(|(id, value)| {
                        let node = store.get_node(*id)?;
                        Some(self.row(&[
                            ("node", Value::Node(*id, Box::new(node.clone()))),
                            ("value", Value::Property(value.clone())),
                            ("asOf", as_of.clone()),
                        ]))
                    })
                    .collect()
            }
            FeatureProcedure::List => {
                self.arguments(store, 0, &[])?;
                registry
                    .list(store.data_changes())
                    .into_iter()
                    .map(|info| self.info_row(info))
                    .collect()
            }
            FeatureProcedure::Drop => {
                let (names, _) = self.arguments(store, 1, &[])?;
                registry.drop_feature(&names[0]).map_err(refused)?;
                vec![self.row(&[("name", string(names[0].clone()))])]
            }
        })
    }
}

impl PhysicalOperator for FeatureOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.procedure.writes() {
            // `Query::is_write` sends these to the write executor; reaching
            // here means a read executor was built for one directly.
            return Err(ExecutionError::write_in_read(format!(
                "{} keeps or frees feature values for later queries, so it needs the write path",
                self.procedure.name()
            )));
        }
        if self.rows.is_none() {
            self.rows = Some(self.compute(store)?.into_iter());
        }
        Ok(self.rows.as_mut().and_then(Iterator::next))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            self.rows = Some(self.compute(store)?.into_iter());
        }
        Ok(self.rows.as_mut().and_then(Iterator::next))
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn is_mutating(&self) -> bool {
        self.procedure.writes()
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Feature".to_string(),
            details: self.procedure.name().to_string(),
            children: Vec::new(),
        }
    }
}
