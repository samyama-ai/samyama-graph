//! The `agent.memory.*` procedures (#2099, Samyama Smriti Phase 1). What is
//! stored and how facts are superseded is in [`crate::agent::smriti`]; the
//! decision traces are [`crate::agent::memory`].
//!
//! | Call | Yields | Writes |
//! |---|---|---|
//! | `agent.memory.remember(session_id, episode, entities, relations, embedding)` | `episode_id, superseded_edges` | yes |
//! | `agent.memory.recall(query_text, query_embedding, session_filter, temporal_as_of, k, max_hops)` | `node, score, memory_tier, provenance_episode` | no |
//! | `agent.memory.record_decision(session_id, parent_step_id, action, rationale, evidence_node_ids, outcome)` | `decision_node_id, trace_id` | yes |
//! | `agent.memory.inspect_trace(trace_id)` | `step_order, decision_node, retrieved_context_nodes, tool_calls, causal_edges` | no |
//!
//! Arguments after the first may be left out or passed as `null`.
//!
//! - `episode` is the content as a string, or a map with `content` and any of
//!   `agent_id`, `user_id`, `namespace`, `observed_at`.
//! - `entities` is a list of names, or of maps with `name` and any of `kind`
//!   and `id` (an existing node, or its id, that the name stands for).
//! - `relations` is a list of maps with `subject`, `relation`, `object` and
//!   any of `single_valued`, `valid_from`, `confidence`.
//! - `superseded_edges` lists the ids of the facts the call closed.
//! - `record_decision` takes an optional seventh argument, `trace_id`, for a
//!   first step in a named trace; a step with a parent joins the parent's.
//! - Times are epoch milliseconds or a temporal value.
//!
//! The two that write are routed to the write path by `Query::is_write`, and
//! run inside the statement's transaction: the RESP and HTTP paths persist
//! that under the statement's tenant, so a memory write reaches disk with the
//! rest of the statement or not at all.

use std::collections::BTreeMap;

use crate::agent::memory::{self, DecisionInput};
use crate::agent::smriti::{
    self, EntityInput, EpisodeInput, RecallQuery, RelationInput, RememberInput,
};
use crate::graph::{GraphStore, NodeId, PropertyValue};
use crate::query::ast::Expression;
use crate::query::executor::operator::{eval_expression, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// The procedures, by the name a caller writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryProcedure {
    Remember,
    Recall,
    RecordDecision,
    InspectTrace,
}

impl MemoryProcedure {
    /// Which procedure `name` calls, case-insensitively, or `None`.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "agent.memory.remember" => Some(Self::Remember),
            "agent.memory.recall" => Some(Self::Recall),
            "agent.memory.record_decision" | "agent.memory.recorddecision" => {
                Some(Self::RecordDecision)
            }
            "agent.memory.inspect_trace" | "agent.memory.inspecttrace" => Some(Self::InspectTrace),
            _ => None,
        }
    }

    /// Every name, for "did you mean".
    pub const NAMES: &'static [&'static str] = &[
        "agent.memory.remember",
        "agent.memory.recall",
        "agent.memory.record_decision",
        "agent.memory.inspect_trace",
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Remember => "agent.memory.remember",
            Self::Recall => "agent.memory.recall",
            Self::RecordDecision => "agent.memory.record_decision",
            Self::InspectTrace => "agent.memory.inspect_trace",
        }
    }

    /// Whether it writes, and so needs the write path.
    pub fn writes(self) -> bool {
        matches!(self, Self::Remember | Self::RecordDecision)
    }

    pub fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Remember => &["episode_id", "superseded_edges"],
            Self::Recall => &["node", "score", "memory_tier", "provenance_episode"],
            Self::RecordDecision => &["decision_node_id", "trace_id"],
            Self::InspectTrace => &[
                "step_order",
                "decision_node",
                "retrieved_context_nodes",
                "tool_calls",
                "causal_edges",
            ],
        }
    }

    /// The most arguments it takes.
    fn max_args(self) -> usize {
        match self {
            Self::Remember => 5,
            Self::Recall => 6,
            Self::RecordDecision => 7,
            Self::InspectTrace => 1,
        }
    }
}

/// Does this `CALL` write agent memory? `Query::is_write()` asks.
pub fn call_writes(name: &str) -> bool {
    MemoryProcedure::from_name(name).is_some_and(MemoryProcedure::writes)
}

/// One `agent.memory.*` call as a physical operator.
pub struct MemoryOperator {
    procedure: MemoryProcedure,
    args: Vec<Expression>,
    /// Output variable per column, after `YIELD ... AS`.
    columns: Vec<(&'static str, String)>,
    rows: Option<std::vec::IntoIter<Record>>,
}

fn int(n: i64) -> Value {
    Value::Property(PropertyValue::Integer(n))
}

fn id_value(id: NodeId) -> Value {
    int(i64::try_from(id.as_u64()).unwrap_or(i64::MAX))
}

fn string(s: impl Into<String>) -> Value {
    Value::Property(PropertyValue::String(s.into()))
}

fn node(store: &GraphStore, id: NodeId) -> Value {
    match store.get_node(id) {
        Some(n) => Value::Node(id, Box::new(n.clone())),
        None => Value::Null,
    }
}

/// An evaluated argument, read as the shapes the procedures accept.
struct Arg<'a> {
    call: &'static str,
    what: &'a str,
    value: Value,
}

impl Arg<'_> {
    fn bad(&self, expected: &str) -> ExecutionError {
        ExecutionError::bad_argument(format!(
            "{}: {} must be {expected}, got {:?}",
            self.call, self.what, self.value
        ))
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }

    fn string(&self) -> ExecutionResult<Option<String>> {
        match &self.value {
            v if v.is_null() => Ok(None),
            Value::Property(PropertyValue::String(s)) => Ok(Some(s.clone())),
            _ => Err(self.bad("a string")),
        }
    }

    fn required_string(&self) -> ExecutionResult<String> {
        match self.string()? {
            Some(s) if !s.trim().is_empty() => Ok(s),
            _ => Err(self.bad("a non-empty string")),
        }
    }

    fn bool(&self) -> ExecutionResult<Option<bool>> {
        match &self.value {
            v if v.is_null() => Ok(None),
            Value::Property(PropertyValue::Boolean(b)) => Ok(Some(*b)),
            _ => Err(self.bad("a boolean")),
        }
    }

    fn float(&self) -> ExecutionResult<Option<f64>> {
        match &self.value {
            v if v.is_null() => Ok(None),
            Value::Property(PropertyValue::Float(f)) => Ok(Some(*f)),
            Value::Property(PropertyValue::Integer(i)) => Ok(Some(*i as f64)),
            _ => Err(self.bad("a number")),
        }
    }

    fn non_negative(&self) -> ExecutionResult<Option<usize>> {
        match &self.value {
            v if v.is_null() => Ok(None),
            Value::Property(PropertyValue::Integer(i)) if *i >= 0 => {
                Ok(Some(usize::try_from(*i).unwrap_or(usize::MAX)))
            }
            _ => Err(self.bad("a non-negative integer")),
        }
    }

    /// Epoch milliseconds, from an integer or a temporal value.
    fn millis(&self) -> ExecutionResult<Option<i64>> {
        match &self.value {
            v if v.is_null() => Ok(None),
            Value::Property(PropertyValue::Integer(i)) => Ok(Some(*i)),
            Value::Property(p) => p
                .as_epoch_millis()
                .map(Some)
                .ok_or_else(|| self.bad("epoch milliseconds or a temporal value")),
            _ => Err(self.bad("epoch milliseconds or a temporal value")),
        }
    }

    /// A node, or a node id.
    fn node_id(&self) -> ExecutionResult<Option<NodeId>> {
        match &self.value {
            v if v.is_null() => Ok(None),
            Value::Property(PropertyValue::Integer(i)) if *i >= 0 => {
                Ok(Some(NodeId::new(*i as u64)))
            }
            v => v
                .node_id()
                .map(Some)
                .ok_or_else(|| self.bad("a node or a node id")),
        }
    }

    fn list(&self) -> ExecutionResult<Vec<Value>> {
        match &self.value {
            v if v.is_null() => Ok(Vec::new()),
            Value::List(items) => Ok(items.clone()),
            Value::Property(PropertyValue::Array(items)) => {
                Ok(items.iter().cloned().map(Value::Property).collect())
            }
            _ => Err(self.bad("a list")),
        }
    }

    fn vector(&self) -> ExecutionResult<Option<Vec<f32>>> {
        if self.is_null() {
            return Ok(None);
        }
        if let Value::Property(PropertyValue::Vector(v)) = &self.value {
            return Ok(Some(v.clone()));
        }
        let items = self.list().map_err(|_| self.bad("a list of numbers"))?;
        items
            .iter()
            .map(|item| match item {
                Value::Property(PropertyValue::Float(f)) => Ok(*f as f32),
                Value::Property(PropertyValue::Integer(i)) => Ok(*i as f32),
                _ => Err(self.bad("a list of numbers")),
            })
            .collect::<ExecutionResult<Vec<f32>>>()
            .map(Some)
    }

    /// A map's entries, refusing keys not in `keys`.
    fn map(&self, keys: &[&str]) -> ExecutionResult<BTreeMap<String, Value>> {
        let entries: BTreeMap<String, Value> = match &self.value {
            Value::Map(m) => m.clone(),
            Value::Property(PropertyValue::Map(m)) => m
                .iter()
                .map(|(k, v)| (k.clone(), Value::Property(v.clone())))
                .collect(),
            _ => return Err(self.bad("a map")),
        };
        if let Some(unknown) = entries.keys().find(|k| !keys.contains(&k.as_str())) {
            return Err(ExecutionError::bad_argument(format!(
                "{}: {} has an unknown key `{unknown}`. It reads: {}",
                self.call,
                self.what,
                keys.join(", ")
            )));
        }
        Ok(entries)
    }
}

const EPISODE_KEYS: &[&str] = &["content", "agent_id", "user_id", "namespace", "observed_at"];
const ENTITY_KEYS: &[&str] = &["name", "kind", "id"];
const RELATION_KEYS: &[&str] = &[
    "subject",
    "relation",
    "object",
    "single_valued",
    "valid_from",
    "confidence",
];

impl MemoryOperator {
    /// Built by the planner. `yields` are `(column, alias)` pairs; an empty
    /// list yields every column under its own name.
    pub fn new(
        procedure: MemoryProcedure,
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
        if args.len() > procedure.max_args() {
            return Err(ExecutionError::bad_argument(format!(
                "{} takes at most {} arguments; got {}",
                procedure.name(),
                procedure.max_args(),
                args.len()
            )));
        }
        Ok(Self {
            procedure,
            args,
            columns,
            rows: None,
        })
    }

    /// The arguments, evaluated, with a `null` for each one left out.
    fn arguments<'n>(
        &self,
        store: &GraphStore,
        names: &[&'n str],
    ) -> ExecutionResult<Vec<Arg<'n>>> {
        let empty = Record::new();
        let call = self.procedure.name();
        names
            .iter()
            .enumerate()
            .map(|(i, what)| {
                let value = match self.args.get(i) {
                    Some(arg) => eval_expression(arg, &empty, store)?,
                    None => Value::Null,
                };
                Ok(Arg { call, what, value })
            })
            .collect()
    }

    fn row(&self, cells: Vec<(&str, Value)>) -> Record {
        let mut record = Record::new();
        for (column, alias) in &self.columns {
            if let Some((_, v)) = cells.iter().find(|(c, _)| c == column) {
                record.bind(alias.clone(), v.clone());
            }
        }
        record
    }

    fn remember_input(&self, store: &GraphStore) -> ExecutionResult<RememberInput> {
        let call = self.procedure.name();
        let a = self.arguments(
            store,
            &[
                "session_id",
                "episode",
                "entities",
                "relations",
                "embedding",
            ],
        )?;
        let mut episode = EpisodeInput {
            session_id: a[0].required_string()?,
            ..EpisodeInput::default()
        };
        match &a[1].value {
            Value::Property(PropertyValue::String(s)) => episode.content = s.clone(),
            v if v.is_null() => return Err(a[1].bad("a string or a map with `content`")),
            _ => {
                let entries = a[1].map(EPISODE_KEYS)?;
                let field = |key: &'static str| Arg {
                    call,
                    what: key,
                    value: entries.get(key).cloned().unwrap_or(Value::Null),
                };
                episode.content = field("content").required_string()?;
                episode.agent_id = field("agent_id").string()?;
                episode.user_id = field("user_id").string()?;
                episode.namespace = field("namespace").string()?;
                episode.observed_at = field("observed_at").millis()?;
            }
        }

        let mut entities = Vec::new();
        for value in a[2].list()? {
            let item = Arg {
                call,
                what: "an entity",
                value,
            };
            if let Value::Property(PropertyValue::String(name)) = &item.value {
                entities.push(EntityInput {
                    name: name.clone(),
                    ..EntityInput::default()
                });
                continue;
            }
            let entries = item.map(ENTITY_KEYS)?;
            let field = |key: &'static str| Arg {
                call,
                what: key,
                value: entries.get(key).cloned().unwrap_or(Value::Null),
            };
            entities.push(EntityInput {
                name: field("name").required_string()?,
                kind: field("kind").string()?,
                node_id: field("id").node_id()?,
            });
        }

        let mut relations = Vec::new();
        for value in a[3].list()? {
            let entries = Arg {
                call,
                what: "a relation",
                value,
            }
            .map(RELATION_KEYS)?;
            let field = |key: &'static str| Arg {
                call,
                what: key,
                value: entries.get(key).cloned().unwrap_or(Value::Null),
            };
            relations.push(RelationInput {
                subject: field("subject").required_string()?,
                relation: field("relation").required_string()?,
                object: field("object").required_string()?,
                single_valued: field("single_valued").bool()?.unwrap_or(false),
                valid_from: field("valid_from").millis()?,
                confidence: field("confidence").float()?,
            });
        }

        Ok(RememberInput {
            episode,
            entities,
            relations,
            embedding: a[4].vector()?,
        })
    }

    fn recall_query(&self, store: &GraphStore) -> ExecutionResult<RecallQuery> {
        let a = self.arguments(
            store,
            &[
                "query_text",
                "query_embedding",
                "session_filter",
                "temporal_as_of",
                "k",
                "max_hops",
            ],
        )?;
        let sessions = match &a[2].value {
            v if v.is_null() => None,
            Value::Property(PropertyValue::String(s)) => Some(vec![s.clone()]),
            _ => Some(
                a[2].list()?
                    .into_iter()
                    .map(|v| match v {
                        Value::Property(PropertyValue::String(s)) => Ok(s),
                        _ => Err(a[2].bad("a session id or a list of them")),
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?,
            ),
        };
        let defaults = RecallQuery::default();
        Ok(RecallQuery {
            text: a[0].string()?,
            embedding: a[1].vector()?,
            sessions,
            as_of: a[3].millis()?,
            k: a[4].non_negative()?.unwrap_or(defaults.k),
            max_hops: a[5].non_negative()?.unwrap_or(defaults.max_hops),
        })
    }

    fn compute(&self, store: &GraphStore) -> ExecutionResult<Vec<Record>> {
        let call = self.procedure.name();
        let refused = |e: String| ExecutionError::bad_argument(format!("{call}: {e}"));
        match self.procedure {
            MemoryProcedure::Recall => {
                let query = self.recall_query(store)?;
                let found = smriti::recall(store, &query).map_err(refused)?;
                Ok(found
                    .into_iter()
                    .map(|r| {
                        self.row(vec![
                            ("node", node(store, r.node_id)),
                            ("score", Value::Property(PropertyValue::Float(r.score))),
                            ("memory_tier", string(r.tier.name())),
                            (
                                "provenance_episode",
                                r.provenance_episode
                                    .map_or(Value::Null, |id| node(store, id)),
                            ),
                        ])
                    })
                    .collect())
            }
            MemoryProcedure::InspectTrace => {
                let a = self.arguments(store, &["trace_id"])?;
                let trace = memory::inspect_trace(store, &a[0].required_string()?);
                Ok(trace
                    .steps
                    .iter()
                    .map(|step| {
                        let causal_edges = step
                            .parent_step_id
                            .map(|parent| {
                                let edge: BTreeMap<String, Value> = [
                                    ("type".to_string(), string("CAUSED_BY")),
                                    ("from".to_string(), id_value(step.node_id)),
                                    ("to".to_string(), id_value(parent)),
                                ]
                                .into();
                                Value::Map(edge)
                            })
                            .into_iter()
                            .collect();
                        let tool_call: BTreeMap<String, Value> = [
                            ("action".to_string(), string(step.action.clone())),
                            ("outcome".to_string(), string(step.outcome.clone())),
                        ]
                        .into();
                        self.row(vec![
                            (
                                "step_order",
                                int(i64::try_from(step.step_order).unwrap_or(i64::MAX)),
                            ),
                            ("decision_node", node(store, step.node_id)),
                            (
                                "retrieved_context_nodes",
                                Value::List(
                                    step.evidence_node_ids
                                        .iter()
                                        .map(|&id| node(store, id))
                                        .collect(),
                                ),
                            ),
                            ("tool_calls", Value::List(vec![Value::Map(tool_call)])),
                            ("causal_edges", Value::List(causal_edges)),
                        ])
                    })
                    .collect())
            }
            MemoryProcedure::Remember | MemoryProcedure::RecordDecision => {
                Err(ExecutionError::write_in_read(format!(
                    "{call} writes agent memory, so it needs the write path"
                )))
            }
        }
    }

    fn compute_mut(&self, store: &mut GraphStore, tenant: &str) -> ExecutionResult<Vec<Record>> {
        let call = self.procedure.name();
        let refused = |e: String| ExecutionError::bad_argument(format!("{call}: {e}"));
        match self.procedure {
            MemoryProcedure::Remember => {
                let input = self.remember_input(store)?;
                let done = smriti::remember_in(store, tenant, &input, smriti::now_millis())
                    .map_err(refused)?;
                Ok(vec![self.row(vec![
                    ("episode_id", id_value(done.episode_id)),
                    (
                        "superseded_edges",
                        Value::List(done.superseded.into_iter().map(id_value).collect()),
                    ),
                ])])
            }
            MemoryProcedure::RecordDecision => {
                let a = self.arguments(
                    store,
                    &[
                        "session_id",
                        "parent_step_id",
                        "action",
                        "rationale",
                        "evidence_node_ids",
                        "outcome",
                        "trace_id",
                    ],
                )?;
                let session_id = a[0].required_string()?;
                let parent_step_id = a[1].node_id()?;
                let action = a[2].required_string()?;
                let rationale = a[3].string()?.unwrap_or_default();
                let mut evidence = Vec::new();
                for value in a[4].list()? {
                    let item = Arg {
                        call,
                        what: "an evidence node",
                        value,
                    };
                    match item.node_id()? {
                        Some(id) => evidence.push(id),
                        None => return Err(item.bad("a node or a node id")),
                    }
                }
                let outcome = a[5].string()?.unwrap_or_default();
                let named_trace = a[6].string()?;
                let trace_id = match parent_step_id {
                    Some(parent) => {
                        let parent_trace =
                            memory::decision_trace_id(store, parent).ok_or_else(|| {
                                refused(format!("parent decision {parent} does not exist"))
                            })?;
                        if named_trace.as_ref().is_some_and(|t| *t != parent_trace) {
                            return Err(refused(
                                "the parent decision belongs to another trace".into(),
                            ));
                        }
                        Some(parent_trace)
                    }
                    None => named_trace,
                };
                let step = memory::record_decision_in(
                    store,
                    tenant,
                    DecisionInput {
                        session_id: &session_id,
                        trace_id: trace_id.as_deref(),
                        parent_step_id,
                        action: &action,
                        rationale: &rationale,
                        evidence_node_ids: &evidence,
                        outcome: &outcome,
                    },
                )
                .map_err(refused)?;
                Ok(vec![self.row(vec![
                    ("decision_node_id", id_value(step.node_id)),
                    ("trace_id", string(step.trace_id)),
                ])])
            }
            MemoryProcedure::Recall | MemoryProcedure::InspectTrace => self.compute(store),
        }
    }
}

impl PhysicalOperator for MemoryOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            self.rows = Some(self.compute(store)?.into_iter());
        }
        Ok(self.rows.as_mut().and_then(Iterator::next))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            self.rows = Some(self.compute_mut(store, tenant_id)?.into_iter());
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
            name: "AgentMemory".to_string(),
            details: self.procedure.name().to_string(),
            children: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_route_and_only_the_writers_write() {
        for name in MemoryProcedure::NAMES {
            let p = MemoryProcedure::from_name(name).unwrap();
            assert_eq!(p.name(), *name);
        }
        assert!(call_writes("agent.memory.remember"));
        assert!(call_writes("AGENT.MEMORY.RECORD_DECISION"));
        assert!(!call_writes("agent.memory.recall"));
        assert!(!call_writes("agent.memory.inspect_trace"));
        assert!(!call_writes("db.labels"));
    }

    #[test]
    fn an_unknown_yield_column_is_refused_at_planning() {
        let err = MemoryOperator::new(
            MemoryProcedure::Recall,
            vec![],
            &[("nodes".to_string(), None)],
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("no `nodes` column"), "{err}");
    }
}
