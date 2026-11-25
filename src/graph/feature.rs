//! Declarative feature definitions and their materialization (ML-07, #1742).
//!
//! A model trained on a graph wants one number per entity: how many orders a
//! customer placed, what they spent on average, the largest transfer an
//! account sent. Written by hand that is a Cypher aggregation per feature,
//! re-typed for every training run and easy to get subtly different each time.
//! A **feature definition** names it once and the engine computes it:
//!
//! ```cypher
//! CALL feature.define('orders_90d', {
//!   label: 'Customer', edgeType: 'PLACED', direction: 'out', targetLabel: 'Order',
//!   aggregate: 'sum', property: 'amount', timeProperty: 'ts'
//! })
//! CALL feature.materialize('orders_90d', {asOf: 1700000000})
//! CALL feature.get('orders_90d') YIELD node, value
//! ```
//!
//! # What a definition is, exactly
//!
//! A one-hop pattern and an aggregate over it:
//! `(n:label)-[:edgeType]-(m:targetLabel)` in the given direction, and
//! `count`, `sum`, `avg`, `min` or `max` of `m.property` per `n`. Every node
//! with `label` gets a value, including one with no match: `count` and `sum`
//! are then 0, and `avg`, `min` and `max` are null, as Cypher's aggregates of
//! no rows are. A neighbour without the property is skipped, as Cypher's
//! aggregates skip null; `count` counts matches, like `count(m)`.
//!
//! It is a pattern rather than a free Cypher string on purpose. The engine has
//! to know which value is the event's time to make a point-in-time read
//! correct (below), and a string it can only run cannot tell it that.
//!
//! # Point in time
//!
//! With `timeProperty`, `materialize` takes an optional `asOf`. A match counts
//! only when its time is at or before `asOf`, so a training row built for a
//! past moment does not see what happened after it -- the leakage a feature
//! computed "as of now" would carry into training. The time is read from the
//! relationship (`timeOn: 'relationship'`, the default: the event is the edge)
//! or from the neighbour (`timeOn: 'node'`). A match **without** a time is
//! left out of an `asOf` read: it cannot be shown to precede `asOf`, and
//! counting it is how a later event leaks in. A time of a different type from
//! `asOf` is refused rather than compared.
//!
//! What `asOf` does not do: it filters the matches by their time property; it
//! does not rewind the property values themselves. An `amount` edited after
//! `asOf` is read as it is now.
//!
//! # Batch, not incremental; in memory, not on disk
//!
//! `materialize` recomputes the whole feature, and its result is kept beside
//! the definition until the next one. [`FeatureInfo::stale`] says when the
//! data may have changed since. Incremental refresh is not here yet. As with
//! graph projections, definitions and values live in memory only: a restart
//! forgets them and a snapshot does not carry them.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::graph::{GraphStore, Label, NodeId, PropertyValue};

/// The aggregate a feature computes over its matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregate {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl Aggregate {
    /// The spellings `feature.define` accepts.
    pub const NAMES: &'static [&'static str] = &["count", "sum", "avg", "min", "max"];

    /// Which aggregate `name` spells, case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "count" => Some(Self::Count),
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

/// Which way the relationship points, from the entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

impl Direction {
    pub const NAMES: &'static [&'static str] = &["out", "in", "both"];

    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "out" | "outgoing" => Some(Self::Out),
            "in" | "incoming" => Some(Self::In),
            "both" | "any" => Some(Self::Both),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Out => "out",
            Self::In => "in",
            Self::Both => "both",
        }
    }
}

/// Where a match's time is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeOn {
    Relationship,
    Node,
}

impl TimeOn {
    pub const NAMES: &'static [&'static str] = &["relationship", "node"];

    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "relationship" | "edge" => Some(Self::Relationship),
            "node" => Some(Self::Node),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Relationship => "relationship",
            Self::Node => "node",
        }
    }
}

/// A feature: the pattern it aggregates over and the aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureDefinition {
    /// The entities that get a value.
    pub label: String,
    /// Only relationships of this type; `None` for any.
    pub edge_type: Option<String>,
    pub direction: Direction,
    /// Only neighbours with this label; `None` for any.
    pub target_label: Option<String>,
    pub aggregate: Aggregate,
    /// The neighbour property aggregated; `None` only for `count`.
    pub property: Option<String>,
    /// The property holding a match's time, which makes `asOf` possible.
    pub time_property: Option<String>,
    pub time_on: TimeOn,
}

impl FeatureDefinition {
    /// The pattern as Cypher would write it, for the reader.
    pub fn pattern(&self) -> String {
        let rel = match &self.edge_type {
            Some(t) => format!("[:{t}]"),
            None => String::new(),
        };
        let target = match &self.target_label {
            Some(l) => format!("(m:{l})"),
            None => "(m)".to_string(),
        };
        let (left, right) = match self.direction {
            Direction::Out => ("-", "->"),
            Direction::In => ("<-", "-"),
            Direction::Both => ("-", "-"),
        };
        let value = match (&self.aggregate, &self.property) {
            (Aggregate::Count, _) => "count(m)".to_string(),
            (agg, Some(p)) => format!("{}(m.{p})", agg.name()),
            (agg, None) => format!("{}(m)", agg.name()),
        };
        format!("(n:{}){left}{rel}{right}{target} -> {value}", self.label)
    }
}

/// The values of one materialization, and what it was computed against.
#[derive(Debug, Clone)]
pub struct Materialization {
    /// One value per entity, in node id order.
    pub values: Vec<(NodeId, PropertyValue)>,
    /// The `asOf` it was computed for; `None` for "everything so far".
    pub as_of: Option<PropertyValue>,
    /// `GraphStore::data_changes` when it was computed.
    pub at_change: u64,
}

impl Materialization {
    /// `node`'s value: a binary search, since `values` is in node id order.
    /// `None` when the node is not one of the entities.
    pub fn value_of(&self, node: NodeId) -> Option<&PropertyValue> {
        self.values
            .binary_search_by_key(&node.as_u64(), |(id, _)| id.as_u64())
            .ok()
            .map(|i| &self.values[i].1)
    }
}

/// One feature: its definition, and its latest materialization if any.
#[derive(Debug, Clone)]
pub struct Feature {
    pub name: String,
    pub definition: FeatureDefinition,
    pub materialized: Option<Arc<Materialization>>,
    /// Entities of the materialization deleted since it was computed.
    ///
    /// The store reuses a deleted node's id, so without this the next node
    /// to take the id would be answered with the deleted one's value. Cleared
    /// by the next materialization, which no longer contains them.
    pub forgotten: HashSet<NodeId>,
}

/// What `feature.list` reports for one feature.
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureInfo {
    pub name: String,
    pub definition: FeatureDefinition,
    /// Entities in the latest materialization; `None` if never materialized.
    pub entities: Option<usize>,
    pub as_of: Option<PropertyValue>,
    /// Whether the data may have changed since the latest materialization.
    /// Never false after a change; true for a feature never materialized.
    pub stale: bool,
}

/// Why a feature call was refused.
#[derive(Debug, Clone, PartialEq)]
pub enum FeatureError {
    /// A name that no feature has.
    NotFound(String),
    /// A feature defined and never materialized.
    NotMaterialized(String),
    EmptyName,
    /// `asOf` on a feature defined without `timeProperty`.
    AsOfWithoutTime(String),
    /// A value the aggregate cannot take, or a time that cannot be compared.
    BadValue(String),
}

impl fmt::Display for FeatureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FeatureError::NotFound(name) => write!(
                f,
                "there is no feature named `{name}`; CALL feature.list() lists them"
            ),
            FeatureError::NotMaterialized(name) => write!(
                f,
                "feature `{name}` has not been materialized; CALL \
                 feature.materialize('{name}') first"
            ),
            FeatureError::EmptyName => f.write_str("a feature needs a non-empty name"),
            FeatureError::AsOfWithoutTime(name) => write!(
                f,
                "feature `{name}` was defined without `timeProperty`, so there is no time \
                 to compare `asOf` with; define it again with `timeProperty`, or drop `asOf`"
            ),
            FeatureError::BadValue(why) => f.write_str(why),
        }
    }
}

impl std::error::Error for FeatureError {}

/// The features of one store.
#[derive(Default)]
pub struct FeatureRegistry {
    entries: RwLock<BTreeMap<String, Feature>>,
    /// Moves on with every define, materialize and drop. The query result
    /// cache keys on it: `feature.get` must not be answered from before the
    /// latest materialization. No data changed, so the data epoch does not move.
    generation: AtomicU64,
}

impl fmt::Debug for FeatureRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeatureRegistry")
            .field("names", &self.read().keys().collect::<Vec<_>>())
            .field("generation", &self.generation())
            .finish()
    }
}

impl FeatureRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// See `GraphProjections::read` on why a poisoned lock is recovered: the
    /// map only changes by a single insert or remove.
    fn read(&self) -> RwLockReadGuard<'_, BTreeMap<String, Feature>> {
        self.entries.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Feature>> {
        self.entries.write().unwrap_or_else(|e| e.into_inner())
    }

    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Define `name`, replacing any earlier definition and its values: values
    /// computed by another definition are not this feature's. Returns whether
    /// one was replaced.
    pub fn define(&self, name: &str, definition: FeatureDefinition) -> Result<bool, FeatureError> {
        let name = checked_name(name)?;
        let replaced = self
            .write()
            .insert(
                name.clone(),
                Feature {
                    name,
                    definition,
                    materialized: None,
                    forgotten: HashSet::new(),
                },
            )
            .is_some();
        self.bump();
        Ok(replaced)
    }

    /// Compute `name` over the store as it is now, keep the result, and
    /// return it.
    pub fn materialize(
        &self,
        store: &GraphStore,
        name: &str,
        as_of: Option<PropertyValue>,
    ) -> Result<Arc<Materialization>, FeatureError> {
        let definition = self.definition(name)?;
        if as_of.is_some() && definition.time_property.is_none() {
            return Err(FeatureError::AsOfWithoutTime(name.to_string()));
        }
        let at_change = store.data_changes();
        let values = compute(store, &definition, as_of.as_ref())?;
        let materialization = Arc::new(Materialization {
            values,
            as_of,
            at_change,
        });
        {
            let mut entries = self.write();
            match entries.get_mut(name) {
                // Redefined while it was computing: these values belong to
                // the old definition, so they are not stored under the new one.
                Some(feature) if feature.definition == definition => {
                    feature.materialized = Some(Arc::clone(&materialization));
                    feature.forgotten.clear();
                }
                Some(_) => {
                    return Err(FeatureError::BadValue(format!(
                        "feature `{name}` was redefined while it was being materialized; \
                         materialize it again"
                    )))
                }
                None => return Err(FeatureError::NotFound(name.to_string())),
            }
        }
        self.bump();
        Ok(materialization)
    }

    /// The latest materialization of `name`, and the entities deleted since.
    pub fn values(
        &self,
        name: &str,
    ) -> Result<(Arc<Materialization>, HashSet<NodeId>), FeatureError> {
        let entries = self.read();
        let feature = entries
            .get(name)
            .ok_or_else(|| FeatureError::NotFound(name.to_string()))?;
        let m = feature
            .materialized
            .clone()
            .ok_or_else(|| FeatureError::NotMaterialized(name.to_string()))?;
        Ok((m, feature.forgotten.clone()))
    }

    /// Each named feature's latest value for `node`, in the order named: the
    /// feature vector of one entity, read under one lock.
    ///
    /// `None` for a feature `node` is not an entity of, or was deleted from
    /// since it was materialized. An unknown or unmaterialized name is an
    /// error rather than a null: a typo would otherwise read as "no value".
    pub fn lookup(
        &self,
        names: &[String],
        node: NodeId,
    ) -> Result<Vec<Option<PropertyValue>>, FeatureError> {
        let entries = self.read();
        names
            .iter()
            .map(|name| {
                let feature = entries
                    .get(name)
                    .ok_or_else(|| FeatureError::NotFound(name.clone()))?;
                let m = feature
                    .materialized
                    .as_ref()
                    .ok_or_else(|| FeatureError::NotMaterialized(name.clone()))?;
                if feature.forgotten.contains(&node) {
                    return Ok(None);
                }
                Ok(m.value_of(node).cloned())
            })
            .collect()
    }

    /// `node` was deleted: no materialization answers for it again, so the
    /// node that next takes its id is not given its values.
    ///
    /// Called by `GraphStore::delete_node`. Cheap when there are no features,
    /// which is the common case: one read lock and no write.
    pub fn forget_node(&self, node: NodeId) {
        let holds_it = self.read().values().any(|f| {
            f.materialized
                .as_ref()
                .is_some_and(|m| m.value_of(node).is_some())
        });
        if !holds_it {
            return;
        }
        for feature in self.write().values_mut() {
            if feature
                .materialized
                .as_ref()
                .is_some_and(|m| m.value_of(node).is_some())
            {
                feature.forgotten.insert(node);
            }
        }
        self.bump();
    }

    pub fn definition(&self, name: &str) -> Result<FeatureDefinition, FeatureError> {
        self.read()
            .get(name)
            .map(|f| f.definition.clone())
            .ok_or_else(|| FeatureError::NotFound(name.to_string()))
    }

    pub fn drop_feature(&self, name: &str) -> Result<(), FeatureError> {
        if self.write().remove(name).is_none() {
            return Err(FeatureError::NotFound(name.to_string()));
        }
        self.bump();
        Ok(())
    }

    pub fn list(&self, current_change: u64) -> Vec<FeatureInfo> {
        self.read()
            .values()
            .map(|f| FeatureInfo {
                name: f.name.clone(),
                definition: f.definition.clone(),
                entities: f.materialized.as_ref().map(|m| m.values.len()),
                as_of: f.materialized.as_ref().and_then(|m| m.as_of.clone()),
                stale: f
                    .materialized
                    .as_ref()
                    .is_none_or(|m| m.at_change != current_change),
            })
            .collect()
    }
}

fn checked_name(name: &str) -> Result<String, FeatureError> {
    if name.trim().is_empty() {
        return Err(FeatureError::EmptyName);
    }
    Ok(name.to_string())
}

/// A number as `f64`. `PropertyValue::as_float` is `Some` for a `Float`
/// only, so an integer needs its own arm.
fn number(v: &PropertyValue) -> Option<f64> {
    match v {
        PropertyValue::Integer(i) => Some(*i as f64),
        PropertyValue::Float(f) => Some(*f),
        _ => None,
    }
}

/// How a match's time compares with `asOf`, or why it cannot.
///
/// Numbers compare with numbers, and each temporal type with itself. Anything
/// else is refused: ordering an integer against a date would answer a
/// question nobody asked.
fn time_order(time: &PropertyValue, as_of: &PropertyValue) -> Option<std::cmp::Ordering> {
    use PropertyValue::*;
    match (time, as_of) {
        (Integer(a), Integer(b)) => Some(a.cmp(b)),
        (Integer(_) | Float(_), Integer(_) | Float(_)) => {
            number(time)?.partial_cmp(&number(as_of)?)
        }
        (DateTime(_), DateTime(_))
        | (Date(_), Date(_))
        | (LocalTime(_), LocalTime(_))
        | (Time { .. }, Time { .. })
        | (LocalDateTime { .. }, LocalDateTime { .. })
        | (ZonedDateTime { .. }, ZonedDateTime { .. }) => Some(time.cmp(as_of)),
        _ => None,
    }
}

/// Running state of one entity's aggregate.
#[derive(Default)]
struct Acc {
    matches: i64,
    seen: i64,
    int_sum: i64,
    float_sum: f64,
    any_float: bool,
    overflowed: bool,
    min: Option<f64>,
    max: Option<f64>,
    /// The min/max as the value it was, so an integer stays an integer.
    min_value: Option<PropertyValue>,
    max_value: Option<PropertyValue>,
}

impl Acc {
    fn add(&mut self, v: &PropertyValue) {
        let f = number(v).unwrap_or(0.0);
        self.seen += 1;
        match v {
            PropertyValue::Integer(i) => match self.int_sum.checked_add(*i) {
                Some(s) => self.int_sum = s,
                None => self.overflowed = true,
            },
            _ => self.any_float = true,
        }
        self.float_sum += f;
        if self.min.is_none_or(|m| f < m) {
            self.min = Some(f);
            self.min_value = Some(v.clone());
        }
        if self.max.is_none_or(|m| f > m) {
            self.max = Some(f);
            self.max_value = Some(v.clone());
        }
    }

    fn finish(self, aggregate: Aggregate) -> PropertyValue {
        match aggregate {
            Aggregate::Count => PropertyValue::Integer(self.matches),
            Aggregate::Sum if self.any_float || self.overflowed => {
                PropertyValue::Float(self.float_sum)
            }
            Aggregate::Sum => PropertyValue::Integer(self.int_sum),
            Aggregate::Avg if self.seen == 0 => PropertyValue::Null,
            Aggregate::Avg => PropertyValue::Float(self.float_sum / self.seen as f64),
            Aggregate::Min => self.min_value.unwrap_or(PropertyValue::Null),
            Aggregate::Max => self.max_value.unwrap_or(PropertyValue::Null),
        }
    }
}

/// The feature's value for every entity, in node id order.
pub fn compute(
    store: &GraphStore,
    def: &FeatureDefinition,
    as_of: Option<&PropertyValue>,
) -> Result<Vec<(NodeId, PropertyValue)>, FeatureError> {
    let label = Label::new(def.label.clone());
    let target_label = def.target_label.as_ref().map(|l| Label::new(l.clone()));
    let mut entities: Vec<NodeId> = store
        .nodes_with_label(&label)
        .map(|s| s.iter().copied().collect())
        .unwrap_or_default();
    entities.sort_by_key(|id| id.as_u64());

    let mut out = Vec::with_capacity(entities.len());
    for entity in entities {
        let mut matches = Vec::new();
        if matches!(def.direction, Direction::Out | Direction::Both) {
            for (eid, _, target, et) in store.get_outgoing_edge_targets_owned(entity) {
                matches.push((eid, target, et));
            }
        }
        if matches!(def.direction, Direction::In | Direction::Both) {
            for (eid, source, _, et) in store.get_incoming_edge_sources_owned(entity) {
                // A self-loop is one match, not two, when both directions count.
                if def.direction == Direction::Both && source == entity {
                    continue;
                }
                matches.push((eid, source, et));
            }
        }

        let mut acc = Acc::default();
        for (eid, other, et) in matches {
            if def.edge_type.as_deref().is_some_and(|t| et.as_str() != t) {
                continue;
            }
            if let Some(tl) = &target_label {
                if !store.get_node(other).is_some_and(|n| n.has_label(tl)) {
                    continue;
                }
            }
            if let (Some(as_of), Some(tp)) = (as_of, &def.time_property) {
                let time = match def.time_on {
                    TimeOn::Relationship => store.edge_property(eid, tp),
                    TimeOn::Node => store.node_property(other, tp),
                };
                let Some(time) = time.filter(|t| !t.is_null()) else {
                    // No time: it cannot be shown to precede `asOf`.
                    continue;
                };
                match time_order(&time, as_of) {
                    Some(std::cmp::Ordering::Greater) => continue,
                    Some(_) => {}
                    None => {
                        return Err(FeatureError::BadValue(format!(
                            "`{tp}` on a match of node {} is {time:?}, which cannot be \
                             compared with asOf {as_of:?}; give asOf the same type as `{tp}`",
                            entity.as_u64()
                        )))
                    }
                }
            }
            acc.matches += 1;
            if def.aggregate == Aggregate::Count {
                continue;
            }
            let Some(property) = &def.property else {
                continue;
            };
            match store.node_property(other, property) {
                None | Some(PropertyValue::Null) => {}
                Some(v @ (PropertyValue::Integer(_) | PropertyValue::Float(_))) => acc.add(&v),
                Some(other_value) => {
                    return Err(FeatureError::BadValue(format!(
                        "{}(m.{property}) needs numbers, and a neighbour of node {} has \
                         {other_value:?}",
                        def.aggregate.name(),
                        entity.as_u64()
                    )))
                }
            }
        }
        out.push((entity, acc.finish(def.aggregate)));
    }
    Ok(out)
}
