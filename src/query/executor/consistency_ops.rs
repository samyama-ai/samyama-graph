//! `eval.checkConsistency`: contradictions within a graph, one row each
//! (EVAL-08, #1737).
//!
//! ```cypher
//! CALL eval.checkConsistency({
//!   label: 'Person',
//!   exclusive: [['deathDate', 'alive']],
//!   temporal: [['born', 'died']],
//!   cardinality: [{type: 'BORN_IN', max: 1}],
//!   unique: ['passport'],
//!   required: ['name']
//! })
//! YIELD class, violation, nodes, relationships
//! ```
//!
//! # The rules are the caller's
//!
//! A property graph has no schema that says two properties exclude each
//! other, that a relationship type is a hierarchy, or that a person is born
//! in one place. Those are facts about a domain, so the caller states them
//! and the procedure checks the graph against them. Each key of the map is a
//! class of rule, and every key is optional:
//!
//! | key | rule | class reported |
//! |---|---|---|
//! | `exclusive` | `[[a, b], ...]`: no node holds both properties | `mutually_exclusive_properties` |
//! | `exclusiveLabels` | `[[A, B], ...]`: no node carries both labels | `mutually_exclusive_labels` |
//! | `acyclic` | `[T, ...]`: relationships of type `T` form no cycle | `cycle_in_acyclic_relationship` |
//! | `temporal` | `[[start, end], ...]`: `end` is not before `start` | `temporal_impossibility` |
//! | `cardinality` | `[{type, direction, min, max}, ...]`: a node has between `min` and `max` such relationships | `cardinality_violation` |
//! | `unique` | `[p, ...]`: no two nodes share a value of `p` | `duplicate_value` |
//! | `required` | `[p, ...]`: every node has `p` | `missing_required_property` |
//!
//! `label` limits every check to nodes with that label; for `acyclic`, to
//! relationships whose two ends both carry it. Without it every node is in
//! scope.
//!
//! # What a row is
//!
//! | yield | |
//! |---|---|
//! | `class` | one of the classes above, for a caller to branch on |
//! | `violation` | what is wrong, in a sentence naming the rule and the values |
//! | `nodes` | the nodes involved, as nodes |
//! | `relationships` | the relationships involved (a cycle's, a cardinality's), as relationships |
//!
//! No rows means no rule is broken. An empty map, or no argument, checks
//! nothing and returns no rows.
//!
//! # Comparisons are Cypher's
//!
//! `temporal` compares as `end < start` would in a query: a date against a
//! date, a number against a number. Two values Cypher cannot order (a date
//! and a string) are not reported, because the rule cannot be decided for
//! them; a missing value is not reported either, since `required` is the
//! rule for that. `unique` groups values as `=` does, so `1` and `1.0` are
//! the same value.
//!
//! # Deterministic
//!
//! Rows come in the order of the table above, then rule by rule as given,
//! then by node id, so the same graph and rules give the same rows.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::graph::{EdgeId, EdgeType, GraphStore, Label, Node, NodeId, PropertyValue};
use crate::query::executor::operator::{cypher_ordering, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// The procedure's name.
pub const NAME: &str = "eval.checkConsistency";

/// The columns, in the order a call without `YIELD` binds them.
pub const COLUMNS: [&str; 4] = ["class", "violation", "nodes", "relationships"];

/// The keys the rule map accepts.
pub const KEYS: [&str; 8] = [
    "label",
    "exclusive",
    "exclusiveLabels",
    "acyclic",
    "temporal",
    "cardinality",
    "unique",
    "required",
];

/// Which way a cardinality rule counts relationships.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

/// One cardinality rule: a node has between `min` and `max` relationships of
/// `rel_type`, counted in `direction`.
#[derive(Debug, Clone, PartialEq)]
pub struct Cardinality {
    pub rel_type: String,
    pub direction: Direction,
    pub min: Option<u64>,
    pub max: Option<u64>,
}

/// The rules of one call, read from its map.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rules {
    pub label: Option<String>,
    pub exclusive: Vec<(String, String)>,
    pub exclusive_labels: Vec<(String, String)>,
    pub acyclic: Vec<String>,
    pub temporal: Vec<(String, String)>,
    pub cardinality: Vec<Cardinality>,
    pub unique: Vec<String>,
    pub required: Vec<String>,
}

/// One broken rule.
#[derive(Debug, Clone, PartialEq)]
pub struct Violation {
    pub class: &'static str,
    pub violation: String,
    pub nodes: Vec<NodeId>,
    pub relationships: Vec<EdgeId>,
}

fn bad(message: impl Into<String>) -> ExecutionError {
    ExecutionError::bad_argument(format!("{NAME}: {}", message.into()))
}

fn string_of(key: &str, v: &PropertyValue) -> ExecutionResult<String> {
    match v {
        PropertyValue::String(s) if !s.is_empty() => Ok(s.clone()),
        other => Err(bad(format!(
            "`{key}` takes names as strings; got {}",
            describe(other)
        ))),
    }
}

fn list_of<'a>(key: &str, v: &'a PropertyValue) -> ExecutionResult<&'a [PropertyValue]> {
    match v {
        PropertyValue::Array(items) => Ok(items),
        other => Err(bad(format!(
            "`{key}` takes a list; got {}",
            describe(other)
        ))),
    }
}

fn names(key: &str, v: &PropertyValue) -> ExecutionResult<Vec<String>> {
    list_of(key, v)?.iter().map(|i| string_of(key, i)).collect()
}

fn pairs(key: &str, v: &PropertyValue, example: &str) -> ExecutionResult<Vec<(String, String)>> {
    list_of(key, v)?
        .iter()
        .map(|item| match item {
            PropertyValue::Array(pair) if pair.len() == 2 => {
                Ok((string_of(key, &pair[0])?, string_of(key, &pair[1])?))
            }
            other => Err(bad(format!(
                "`{key}` takes pairs, e.g. `{key}: [{example}]`; got {}",
                describe(other)
            ))),
        })
        .collect()
}

fn count_of(key: &str, field: &str, v: &PropertyValue) -> ExecutionResult<u64> {
    match v {
        PropertyValue::Integer(n) if *n >= 0 => Ok(*n as u64),
        other => Err(bad(format!(
            "`{key}`'s `{field}` takes a non-negative integer; got {}",
            describe(other)
        ))),
    }
}

fn cardinalities(v: &PropertyValue) -> ExecutionResult<Vec<Cardinality>> {
    const FIELDS: [&str; 4] = ["type", "direction", "min", "max"];
    list_of("cardinality", v)?
        .iter()
        .map(|item| {
            let PropertyValue::Map(m) = item else {
                return Err(bad(format!(
                    "`cardinality` takes maps, e.g. `cardinality: [{{type: 'BORN_IN', max: 1}}]`; got {}",
                    describe(item)
                )));
            };
            if let Some(k) = m.keys().filter(|k| !FIELDS.contains(&k.as_str())).min() {
                return Err(bad(format!(
                    "`cardinality` has no `{k}`; its fields are {}",
                    FIELDS.join(", ")
                )));
            }
            let rel_type = match m.get("type") {
                Some(t) => string_of("cardinality", t)?,
                None => return Err(bad("`cardinality` needs a `type`")),
            };
            let direction = match m.get("direction") {
                None => Direction::Out,
                Some(PropertyValue::String(d)) => match d.to_ascii_uppercase().as_str() {
                    "OUT" | "OUTGOING" => Direction::Out,
                    "IN" | "INCOMING" => Direction::In,
                    "BOTH" => Direction::Both,
                    _ => return Err(bad(format!("`cardinality`'s `direction` is OUT, IN or BOTH; got `{d}`"))),
                },
                Some(other) => {
                    return Err(bad(format!(
                        "`cardinality`'s `direction` is OUT, IN or BOTH; got {}",
                        describe(other)
                    )))
                }
            };
            let min = m.get("min").map(|v| count_of("cardinality", "min", v)).transpose()?;
            let max = m.get("max").map(|v| count_of("cardinality", "max", v)).transpose()?;
            match (min, max) {
                (None, None) => return Err(bad("`cardinality` needs a `min`, a `max` or both")),
                (Some(lo), Some(hi)) if lo > hi => {
                    return Err(bad(format!("`cardinality`'s `min` ({lo}) is above its `max` ({hi})")))
                }
                _ => {}
            }
            Ok(Cardinality { rel_type, direction, min, max })
        })
        .collect()
}

/// Ids for a sentence: the first [`LISTED`], then how many more. The `nodes`
/// column carries every one; a 20,000-node cycle needs no 20,000-id sentence.
fn id_list(ids: &[NodeId]) -> String {
    let shown: Vec<String> = ids
        .iter()
        .take(LISTED)
        .map(|n| n.as_u64().to_string())
        .collect();
    match ids.len().saturating_sub(LISTED) {
        0 => shown.join(", "),
        more => format!("{} and {more} more", shown.join(", ")),
    }
}

/// How many node ids a `violation` sentence names.
pub const LISTED: usize = 10;

/// A value as a reader would write it, for an error or a violation.
fn describe(v: &PropertyValue) -> String {
    match v {
        PropertyValue::String(s) => format!("'{s}'"),
        PropertyValue::Null => "null".to_string(),
        other => format!("{other}"),
    }
}

impl Rules {
    /// The rules in `config`, or why they cannot be read. `Null` is no rules.
    pub fn from_config(config: &PropertyValue) -> ExecutionResult<Self> {
        let map: &HashMap<String, PropertyValue> = match config {
            PropertyValue::Null => return Ok(Self::default()),
            PropertyValue::Map(m) => m,
            other => {
                return Err(bad(format!(
                    "takes one map of rules, e.g. `{{label: 'Person', temporal: [['born', 'died']]}}`; got {}",
                    describe(other)
                )))
            }
        };
        let mut unknown: Vec<&String> =
            map.keys().filter(|k| !KEYS.contains(&k.as_str())).collect();
        unknown.sort();
        if let Some(k) = unknown.first() {
            let hint = match crate::query::suggest::closest(k, KEYS) {
                Some(near) => format!("Did you mean `{near}`? "),
                None => String::new(),
            };
            return Err(bad(format!(
                "there is no rule `{k}`. {hint}The rules are {}.",
                KEYS.join(", ")
            )));
        }
        let mut rules = Self::default();
        for (key, value) in map {
            if matches!(value, PropertyValue::Null) {
                continue;
            }
            match key.as_str() {
                "label" => rules.label = Some(string_of("label", value)?),
                "exclusive" => {
                    rules.exclusive = pairs("exclusive", value, "['alive', 'deathDate']")?
                }
                "exclusiveLabels" => {
                    rules.exclusive_labels = pairs("exclusiveLabels", value, "['Alive', 'Dead']")?
                }
                "acyclic" => rules.acyclic = names("acyclic", value)?,
                "temporal" => rules.temporal = pairs("temporal", value, "['start', 'end']")?,
                "cardinality" => rules.cardinality = cardinalities(value)?,
                "unique" => rules.unique = names("unique", value)?,
                "required" => rules.required = names("required", value)?,
                _ => unreachable!("checked against KEYS above"),
            }
        }
        Ok(rules)
    }
}

/// The nodes in scope, in id order.
fn scope<'a>(store: &'a GraphStore, label: Option<&str>) -> Vec<&'a Node> {
    let mut nodes = match label {
        Some(l) => store.get_nodes_by_label(&Label::new(l)),
        None => store.all_nodes(),
    };
    nodes.sort_by_key(|n| n.id);
    nodes
}

/// A node's value for `key`, read as a query reads it (columns first), or
/// `None` when it has none.
fn present(store: &GraphStore, node: &Node, key: &str) -> Option<PropertyValue> {
    store
        .node_property(node.id, key)
        .filter(|v| !matches!(v, PropertyValue::Null))
}

/// Every broken rule, in the order the module documents.
pub fn check(store: &GraphStore, rules: &Rules) -> Vec<Violation> {
    let nodes = scope(store, rules.label.as_deref());
    let mut out = Vec::new();

    for (a, b) in &rules.exclusive {
        for n in &nodes {
            if let (Some(va), Some(vb)) = (present(store, n, a), present(store, n, b)) {
                out.push(Violation {
                    class: "mutually_exclusive_properties",
                    violation: format!(
                        "node {} has both `{a}` ({}) and `{b}` ({}), which exclude each other",
                        n.id.as_u64(),
                        describe(&va),
                        describe(&vb)
                    ),
                    nodes: vec![n.id],
                    relationships: vec![],
                });
            }
        }
    }

    for (a, b) in &rules.exclusive_labels {
        let (la, lb) = (Label::new(a.as_str()), Label::new(b.as_str()));
        for n in &nodes {
            if n.has_label(&la) && n.has_label(&lb) {
                out.push(Violation {
                    class: "mutually_exclusive_labels",
                    violation: format!(
                        "node {} carries both `:{a}` and `:{b}`, which exclude each other",
                        n.id.as_u64()
                    ),
                    nodes: vec![n.id],
                    relationships: vec![],
                });
            }
        }
    }

    for rel_type in &rules.acyclic {
        out.extend(cycles(store, &nodes, rel_type));
    }

    for (start, end) in &rules.temporal {
        for n in &nodes {
            let (Some(s), Some(e)) = (present(store, n, start), present(store, n, end)) else {
                continue;
            };
            if cypher_ordering(&e, &s) == Some(std::cmp::Ordering::Less) {
                out.push(Violation {
                    class: "temporal_impossibility",
                    violation: format!(
                        "node {} has `{end}` ({}) before `{start}` ({})",
                        n.id.as_u64(),
                        describe(&e),
                        describe(&s)
                    ),
                    nodes: vec![n.id],
                    relationships: vec![],
                });
            }
        }
    }

    for rule in &rules.cardinality {
        let rel_type = EdgeType::new(rule.rel_type.as_str());
        for n in &nodes {
            let mut edges: Vec<EdgeId> = Vec::new();
            if matches!(rule.direction, Direction::Out | Direction::Both) {
                edges.extend(
                    store
                        .get_outgoing_edges(n.id)
                        .into_iter()
                        .filter(|e| e.edge_type == rel_type)
                        .map(|e| e.id),
                );
            }
            if matches!(rule.direction, Direction::In | Direction::Both) {
                edges.extend(
                    store
                        .get_incoming_edges(n.id)
                        .into_iter()
                        .filter(|e| e.edge_type == rel_type)
                        .map(|e| e.id),
                );
            }
            edges.sort();
            edges.dedup(); // a self-loop is both outgoing and incoming
            let count = edges.len() as u64;
            let below = rule.min.is_some_and(|lo| count < lo);
            let above = rule.max.is_some_and(|hi| count > hi);
            if below || above {
                let arrow = match rule.direction {
                    Direction::Out => format!("-[:{}]->", rule.rel_type),
                    Direction::In => format!("<-[:{}]-", rule.rel_type),
                    Direction::Both => format!("-[:{}]-", rule.rel_type),
                };
                let bound = match (rule.min, rule.max) {
                    (Some(lo), Some(hi)) if lo == hi => format!("exactly {lo}"),
                    (Some(lo), Some(hi)) => format!("between {lo} and {hi}"),
                    (Some(lo), None) => format!("at least {lo}"),
                    (None, Some(hi)) => format!("at most {hi}"),
                    (None, None) => unreachable!("refused when the rules were read"),
                };
                out.push(Violation {
                    class: "cardinality_violation",
                    violation: format!(
                        "node {} has {count} `{arrow}` relationships; the rule allows {bound}",
                        n.id.as_u64()
                    ),
                    nodes: vec![n.id],
                    relationships: edges,
                });
            }
        }
    }

    for key in &rules.unique {
        let mut held: Vec<(PropertyValue, NodeId)> = nodes
            .iter()
            .filter_map(|n| present(store, n, key).map(|v| (v, n.id)))
            .collect();
        // `PropertyValue`'s total order puts values `=` calls equal next to
        // each other (it orders numbers by value across integer and float),
        // so a run of equal neighbours is one shared value.
        held.sort_by(|(va, na), (vb, nb)| va.cmp(vb).then(na.cmp(nb)));
        let mut i = 0;
        while i < held.len() {
            let mut j = i + 1;
            while j < held.len()
                && cypher_ordering(&held[i].0, &held[j].0) == Some(std::cmp::Ordering::Equal)
            {
                j += 1;
            }
            if j - i > 1 {
                let mut ids: Vec<NodeId> = held[i..j].iter().map(|(_, n)| *n).collect();
                ids.sort();
                out.push(Violation {
                    class: "duplicate_value",
                    violation: format!(
                        "{} nodes share `{key}` = {}: {}",
                        ids.len(),
                        describe(&held[i].0),
                        id_list(&ids)
                    ),
                    nodes: ids,
                    relationships: vec![],
                });
            }
            i = j;
        }
    }

    for key in &rules.required {
        for n in &nodes {
            if present(store, n, key).is_none() {
                out.push(Violation {
                    class: "missing_required_property",
                    violation: format!("node {} has no `{key}`", n.id.as_u64()),
                    nodes: vec![n.id],
                    relationships: vec![],
                });
            }
        }
    }

    out
}

/// One row per cycle of `rel_type` among `nodes`: each strongly connected
/// component with more than one node, or a node with a self-loop.
fn cycles(store: &GraphStore, nodes: &[&Node], rel_type: &str) -> Vec<Violation> {
    let rel = EdgeType::new(rel_type);
    let in_scope: BTreeSet<NodeId> = nodes.iter().map(|n| n.id).collect();
    // Adjacency over the scope, as (target, edge), in id order.
    let mut adj: BTreeMap<NodeId, Vec<(NodeId, EdgeId)>> = BTreeMap::new();
    for &n in &in_scope {
        let mut out: Vec<(NodeId, EdgeId)> = store
            .get_outgoing_edges(n)
            .into_iter()
            .filter(|e| e.edge_type == rel && in_scope.contains(&e.target))
            .map(|e| (e.target, e.id))
            .collect();
        out.sort();
        adj.insert(n, out);
    }

    // Tarjan's strongly connected components, iteratively: a deep hierarchy
    // must not overflow the stack.
    let mut index: HashMap<NodeId, usize> = HashMap::new();
    let mut low: HashMap<NodeId, usize> = HashMap::new();
    let mut on_stack: BTreeSet<NodeId> = BTreeSet::new();
    let mut stack: Vec<NodeId> = Vec::new();
    let mut components: Vec<Vec<NodeId>> = Vec::new();
    let mut next = 0usize;
    for &root in &in_scope {
        if index.contains_key(&root) {
            continue;
        }
        let mut work: Vec<(NodeId, usize)> = vec![(root, 0)];
        index.insert(root, next);
        low.insert(root, next);
        next += 1;
        stack.push(root);
        on_stack.insert(root);
        while let Some(&mut (v, ref mut i)) = work.last_mut() {
            let succ = &adj[&v];
            if *i < succ.len() {
                let (w, _) = succ[*i];
                *i += 1;
                if let std::collections::hash_map::Entry::Vacant(slot) = index.entry(w) {
                    slot.insert(next);
                    low.insert(w, next);
                    next += 1;
                    stack.push(w);
                    on_stack.insert(w);
                    work.push((w, 0));
                } else if on_stack.contains(&w) {
                    let lw = index[&w];
                    let lv = low.get_mut(&v).expect("visited");
                    *lv = (*lv).min(lw);
                }
            } else {
                work.pop();
                if let Some(&(parent, _)) = work.last() {
                    let lv = low[&v];
                    let lp = low.get_mut(&parent).expect("visited");
                    *lp = (*lp).min(lv);
                }
                if low[&v] == index[&v] {
                    let mut component = Vec::new();
                    loop {
                        let w = stack.pop().expect("on the stack");
                        on_stack.remove(&w);
                        component.push(w);
                        if w == v {
                            break;
                        }
                    }
                    components.push(component);
                }
            }
        }
    }

    let mut out: Vec<Violation> = components
        .into_iter()
        .filter_map(|mut component| {
            component.sort();
            let members: BTreeSet<NodeId> = component.iter().copied().collect();
            let mut edges: Vec<EdgeId> = component
                .iter()
                .flat_map(|n| adj[n].iter().filter(|(t, _)| members.contains(t)).map(|(_, e)| *e))
                .collect();
            edges.sort();
            // One node is a cycle only when it points at itself.
            if component.len() == 1 && edges.is_empty() {
                return None;
            }
            let ids = id_list(&component);
            Some(Violation {
                class: "cycle_in_acyclic_relationship",
                violation: if component.len() == 1 {
                    format!("node {ids} has a `:{rel_type}` relationship to itself, so `:{rel_type}` is not acyclic")
                } else {
                    format!(
                        "`:{rel_type}` forms a cycle through {} nodes: {ids}",
                        component.len()
                    )
                },
                nodes: component,
                relationships: edges,
            })
        })
        .collect();
    out.sort_by_key(|v| v.nodes.first().copied());
    out
}

/// `CALL eval.checkConsistency(rules)`.
pub struct ConsistencyCheckOperator {
    rules: Rules,
    yields: Vec<(&'static str, String)>,
    rows: Option<std::vec::IntoIter<Violation>>,
}

impl ConsistencyCheckOperator {
    /// `yields` are `(name, alias)`; none yields every column under its own name.
    pub fn new(rules: Rules, yields: &[(String, Option<String>)]) -> ExecutionResult<Self> {
        let yields = if yields.is_empty() {
            COLUMNS.iter().map(|c| (*c, c.to_string())).collect()
        } else {
            yields
                .iter()
                .map(
                    |(name, alias)| match COLUMNS.iter().find(|c| **c == name.as_str()) {
                        Some(c) => Ok((*c, alias.clone().unwrap_or_else(|| name.clone()))),
                        None => Err(bad(format!(
                            "yields {}; there is no `{name}`",
                            COLUMNS.join(", ")
                        ))),
                    },
                )
                .collect::<ExecutionResult<Vec<_>>>()?
        };
        Ok(Self {
            rules,
            yields,
            rows: None,
        })
    }
}

impl PhysicalOperator for ConsistencyCheckOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            self.rows = Some(check(store, &self.rules).into_iter());
        }
        let Some(v) = self.rows.as_mut().and_then(Iterator::next) else {
            return Ok(None);
        };
        let mut record = Record::new();
        for (column, var) in &self.yields {
            let value = match *column {
                "class" => Value::Property(PropertyValue::String(v.class.to_string())),
                "violation" => Value::Property(PropertyValue::String(v.violation.clone())),
                "nodes" => Value::List(v.nodes.iter().map(|n| Value::NodeRef(*n)).collect()),
                "relationships" => Value::List(
                    v.relationships
                        .iter()
                        .filter_map(|e| {
                            store.get_edge(*e).map(|edge| {
                                Value::EdgeRef(
                                    edge.id,
                                    edge.source,
                                    edge.target,
                                    edge.edge_type.clone(),
                                )
                            })
                        })
                        .collect(),
                ),
                _ => unreachable!("yields are checked against COLUMNS"),
            };
            record.bind(var.clone(), value);
        }
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CheckConsistency".to_string(),
            details: NAME.to_string(),
            children: Vec::new(),
        }
    }
}
