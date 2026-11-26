//! Named subgraphs: persisted, virtual views over one tenant's graph (#337).
//!
//! A subgraph is a name for a slice of the graph that is already there. It
//! copies nothing: it is a set of node labels, a set of relationship types and,
//! per label, an optional property predicate. Membership is decided when a
//! query reads, so a node written after the definition is in the view the
//! moment it matches.
//!
//! # Membership
//!
//! * A **node** is in the view when at least one of its labels is in the view
//!   and, if that label carries a predicate, the predicate is true for the
//!   node. A node with two in-scope labels needs only one of them to admit it.
//! * A **relationship** is in the view when its type is in the view **and**
//!   both its endpoints are (the induced-edge rule). A traversal therefore
//!   cannot leave the view through a relationship whose far end is outside it.
//!
//! # Predicates
//!
//! A predicate is the text of a Cypher boolean expression over the node's
//! properties: `status = 'Recruiting'`, `year >= 2020 AND NOT archived`.
//! A bare name is a property of the node; `n.status` is accepted as a
//! spelling of the same thing. Only expressions that read the one node are
//! allowed -- no subqueries, pattern comprehensions or parameters -- so a
//! predicate cannot reach past the node it is deciding.
//!
//! A predicate that does not evaluate to `true` (false, null, or an error such
//! as comparing a string with a number) excludes the node. A view fails closed.
//!
//! See `docs/ADR/ADR-042-named-subgraphs.md`.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::{EdgeId, EdgeType, GraphStore, Label, NodeId};
use crate::query::ast::Expression;

/// The longest name a subgraph may have.
pub const MAX_NAME_LEN: usize = 64;

/// The error a write addressed to a named subgraph gets.
pub const WRITE_REFUSED: &str = "Writes through named subgraphs are not supported. \
     Run the write against the tenant graph.";

/// What a user declares. The form stored in the index catalog.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubgraphDefinition {
    pub name: String,
    /// Node labels in the view, sorted and without duplicates once compiled.
    pub labels: Vec<String>,
    /// Relationship types in the view, sorted and without duplicates. Empty
    /// means the view has nodes and no relationships.
    #[serde(default)]
    pub edge_types: Vec<String>,
    /// Label -> predicate source text.
    #[serde(default, rename = "where")]
    pub predicates: BTreeMap<String, String>,
}

/// A definition checked and compiled for use by the engine.
#[derive(Debug, Clone)]
pub struct SubgraphScope {
    definition: SubgraphDefinition,
    labels: Vec<Label>,
    label_set: HashSet<Label>,
    edge_types: HashSet<EdgeType>,
    predicates: HashMap<Label, Expression>,
}

/// The variable a compiled predicate reads the node through. Not a name a
/// user can write: it starts with two underscores and the predicate's own
/// variables are rewritten onto it.
const NODE_VAR: &str = "__subgraph_node";

/// Is `name` usable as a subgraph name?
pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("a subgraph name must not be empty".to_string());
    }
    if name.len() > MAX_NAME_LEN {
        return Err(format!(
            "a subgraph name is at most {MAX_NAME_LEN} characters"
        ));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap_or('_');
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "invalid subgraph name '{name}': use letters, digits, '_' and '-', starting with a letter or '_'"
        ));
    }
    if name.eq_ignore_ascii_case("default") {
        return Err("'default' names the tenant graph itself and cannot be a subgraph".to_string());
    }
    Ok(())
}

fn validate_identifier(kind: &str, s: &str) -> Result<(), String> {
    if s.trim().is_empty() {
        return Err(format!("a {kind} must not be empty"));
    }
    if s != s.trim() {
        return Err(format!("{kind} '{s}' has leading or trailing whitespace"));
    }
    Ok(())
}

/// Parse a predicate's text and rewrite it onto [`NODE_VAR`].
pub fn compile_predicate(label: &str, text: &str) -> Result<Expression, String> {
    let bad = |why: &str| format!("invalid predicate for :{label} (`{text}`): {why}");
    if text.trim().is_empty() {
        return Err(bad("it is empty"));
    }
    // A statement with nothing else in it, so the predicate's text cannot
    // close the expression and smuggle in a clause: anything beyond the one
    // RETURN item makes the parsed query differ from the reference below.
    //
    // Parsed without the semantic checks: a bare `status` is a variable no
    // clause binds, which `validate` refuses. It is a property here, and the
    // rewritten expression is validated below, once it reads the node.
    let wrapped = format!("RETURN ({text}) AS __p");
    let mut parsed =
        crate::query::parser::parse_query_unvalidated(&wrapped).map_err(|e| bad(&e.to_string()))?;
    let mut reference = crate::query::parser::parse_query_unvalidated("RETURN 1 AS __p")
        .map_err(|e| bad(&e.to_string()))?;
    let ret = parsed
        .return_clause
        .take()
        .ok_or_else(|| bad("not an expression"))?;
    reference.return_clause = None;
    if format!("{parsed:?}") != format!("{reference:?}")
        || ret.distinct
        || ret.items.len() != 1
        || ret.items[0].alias.as_deref() != Some("__p")
    {
        return Err(bad("it must be a single expression"));
    }
    let expr = ret
        .items
        .into_iter()
        .next()
        .map(|i| i.expression)
        .ok_or_else(|| bad("not an expression"))?;
    let expr = rewrite(expr).map_err(|why| bad(&why))?;

    // The checks skipped above -- unknown functions, misplaced aggregates --
    // run on the rewritten predicate, in a query that binds the node.
    let mut check = crate::query::parse_query(&format!("MATCH ({NODE_VAR}) RETURN 1 AS __p"))
        .map_err(|e| bad(&e.to_string()))?;
    if let Some(item) = check
        .return_clause
        .as_mut()
        .and_then(|r| r.items.first_mut())
    {
        item.expression = expr.clone();
    }
    crate::query::validate::validate(&check).map_err(|e| bad(&e.to_string()))?;
    Ok(expr)
}

/// Rewrite property reads onto the node variable, refusing anything that
/// reads more than the node.
fn rewrite(expr: Expression) -> Result<Expression, String> {
    let r = |e: Box<Expression>| rewrite(*e).map(Box::new);
    Ok(match expr {
        Expression::Literal(v) => Expression::Literal(v),
        // A bare name is a property of the node.
        Expression::Variable(property) => Expression::Property {
            variable: NODE_VAR.to_string(),
            property,
        },
        Expression::Property { variable, property } if variable == "n" => Expression::Property {
            variable: NODE_VAR.to_string(),
            property,
        },
        Expression::Property { variable, .. } => {
            return Err(format!(
                "`{variable}` is not the node; write the property bare (`status`) or as `n.status`"
            ))
        }
        Expression::Binary { left, op, right } => Expression::Binary {
            left: r(left)?,
            op,
            right: r(right)?,
        },
        Expression::Unary { op, expr } => Expression::Unary { op, expr: r(expr)? },
        Expression::Function {
            name,
            args,
            distinct,
        } => {
            let lower = name.to_ascii_lowercase();
            // Functions that read more than the node's own properties, or
            // that read an index the view does not restrict.
            const REFUSED: &[&str] = &[
                "subsumes",
                "hierarchy_rollup",
                "hierarchy_lca",
                "labels",
                "id",
                "elementid",
                "keys",
                "properties",
                "nodes",
                "relationships",
                "degree",
                "exists",
                "count",
                "sum",
                "avg",
                "min",
                "max",
                "collect",
                "stdev",
                "stdevp",
                "percentilecont",
                "percentiledisc",
                "randomuuid",
                "rand",
            ];
            if REFUSED.contains(&lower.as_str()) || distinct {
                return Err(format!(
                    "function `{name}` is not allowed in a subgraph predicate"
                ));
            }
            Expression::Function {
                name,
                args: args.into_iter().map(rewrite).collect::<Result<_, _>>()?,
                distinct,
            }
        }
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => Expression::Case {
            operand: operand.map(r).transpose()?,
            when_clauses: when_clauses
                .into_iter()
                .map(|(w, t)| Ok::<_, String>((rewrite(w)?, rewrite(t)?)))
                .collect::<Result<_, _>>()?,
            else_result: else_result.map(r).transpose()?,
        },
        Expression::Index { expr, index } => Expression::Index {
            expr: r(expr)?,
            index: r(index)?,
        },
        Expression::ListExpr(items) => {
            Expression::ListExpr(items.into_iter().map(rewrite).collect::<Result<_, _>>()?)
        }
        Expression::MapExpr(items) => Expression::MapExpr(
            items
                .into_iter()
                .map(|(k, v)| Ok::<_, String>((k, rewrite(v)?)))
                .collect::<Result<_, _>>()?,
        ),
        Expression::Parameter(p) => {
            return Err(format!("parameter `${p}` is not allowed; write the value"))
        }
        _ => {
            return Err(
                "only comparisons, boolean logic, literals, lists, CASE and scalar functions \
                 over the node's properties are allowed"
                    .to_string(),
            )
        }
    })
}

impl SubgraphScope {
    /// Check a definition and compile its predicates.
    pub fn compile(mut definition: SubgraphDefinition) -> Result<Self, String> {
        validate_name(&definition.name)?;
        if definition.labels.is_empty() {
            return Err(format!(
                "subgraph '{}' must name at least one label",
                definition.name
            ));
        }
        for l in &definition.labels {
            validate_identifier("label", l)?;
        }
        for t in &definition.edge_types {
            validate_identifier("relationship type", t)?;
        }
        definition.labels.sort();
        definition.labels.dedup();
        definition.edge_types.sort();
        definition.edge_types.dedup();

        let mut predicates = HashMap::new();
        for (label, text) in &definition.predicates {
            if !definition.labels.iter().any(|l| l == label) {
                return Err(format!(
                    "subgraph '{}' has a predicate for :{label}, which is not one of its labels ({})",
                    definition.name,
                    definition.labels.join(", ")
                ));
            }
            predicates.insert(Label::new(label), compile_predicate(label, text)?);
        }
        let labels: Vec<Label> = definition.labels.iter().map(Label::new).collect();
        Ok(Self {
            label_set: labels.iter().cloned().collect(),
            labels,
            edge_types: definition.edge_types.iter().map(EdgeType::new).collect(),
            predicates,
            definition,
        })
    }

    pub fn name(&self) -> &str {
        &self.definition.name
    }

    pub fn definition(&self) -> &SubgraphDefinition {
        &self.definition
    }

    /// The view's labels, sorted by name.
    pub fn labels(&self) -> &[Label] {
        &self.labels
    }

    pub fn has_label(&self, label: &Label) -> bool {
        self.label_set.contains(label)
    }

    pub fn has_edge_type(&self, edge_type: &EdgeType) -> bool {
        self.edge_types.contains(edge_type)
    }

    /// Does `label` carry a predicate? A label without one admits every node
    /// it is on.
    pub fn has_predicate(&self, label: &Label) -> bool {
        self.predicates.contains_key(label)
    }

    /// Is the node in the view?
    pub fn contains_node(&self, store: &GraphStore, id: NodeId) -> bool {
        let Some(node) = store.get_node(id) else {
            return false;
        };
        node.labels.iter().any(|l| self.label_admits(store, l, id))
    }

    /// Is the node in the view through `label`?
    pub fn label_admits(&self, store: &GraphStore, label: &Label, id: NodeId) -> bool {
        if !self.label_set.contains(label) {
            return false;
        }
        match self.predicates.get(label) {
            None => true,
            Some(pred) => {
                let mut record = crate::query::executor::Record::new();
                record.bind(
                    NODE_VAR.to_string(),
                    crate::query::executor::Value::NodeRef(id),
                );
                matches!(
                    crate::query::executor::operator::eval_expression(pred, &record, store),
                    Ok(crate::query::executor::Value::Property(
                        crate::graph::PropertyValue::Boolean(true)
                    ))
                )
            }
        }
    }

    /// Is the relationship in the view? Its type, and both its endpoints.
    pub fn contains_edge(&self, store: &GraphStore, id: EdgeId) -> bool {
        let Some(t) = store.get_edge_type(id) else {
            return false;
        };
        if !self.edge_types.contains(&t) {
            return false;
        }
        let Some((s, d)) = store.get_edge_endpoints(id) else {
            return false;
        };
        self.contains_node(store, s) && self.contains_node(store, d)
    }

    /// [`Self::contains_edge`] when the endpoints are already known, and the
    /// near one already known to be in the view.
    pub fn contains_edge_to(&self, store: &GraphStore, id: EdgeId, far: NodeId) -> bool {
        match store.get_edge_type(id) {
            Some(t) if self.edge_types.contains(&t) => self.contains_node(store, far),
            _ => false,
        }
    }
}

/// Per-label and per-type counts restricted to a view, with the label
/// patterns its relationships connect.
#[derive(Debug, Clone, Default)]
pub struct SubgraphSchema {
    /// Label -> nodes in the view through that label.
    pub labels: BTreeMap<String, usize>,
    /// Type -> (relationships in the view, source labels, target labels).
    pub edge_types: BTreeMap<
        String,
        (
            usize,
            std::collections::BTreeSet<String>,
            std::collections::BTreeSet<String>,
        ),
    >,
    pub total_nodes: usize,
    pub total_edges: usize,
}

impl GraphStore {
    /// Every node in the view, ascending by id.
    pub fn subgraph_node_ids(&self, scope: &SubgraphScope) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = scope
            .labels()
            .iter()
            .flat_map(|l| self.node_ids_by_label(l, None))
            .collect();
        ids.sort_unstable_by_key(|id| id.as_u64());
        ids.dedup();
        ids.retain(|id| scope.contains_node(self, *id));
        ids
    }

    /// Counts and patterns restricted to the view. Exact: every member is
    /// visited, which costs a pass over the view's labels and types.
    pub fn subgraph_schema(&self, scope: &SubgraphScope) -> SubgraphSchema {
        let mut out = SubgraphSchema::default();
        let members: HashSet<NodeId> = self.subgraph_node_ids(scope).into_iter().collect();
        out.total_nodes = members.len();
        // A node counts under each of its labels that admits it.
        let in_labels = |id: NodeId| -> Vec<String> {
            self.get_node(id)
                .map(|n| {
                    let mut v: Vec<String> = n
                        .labels
                        .iter()
                        .filter(|l| scope.label_admits(self, l, id))
                        .map(|l| l.as_str().to_string())
                        .collect();
                    v.sort();
                    v
                })
                .unwrap_or_default()
        };
        for label in scope.labels() {
            out.labels.insert(label.as_str().to_string(), 0);
        }
        for id in &members {
            for l in in_labels(*id) {
                *out.labels.entry(l).or_default() += 1;
            }
        }
        for t in &scope.definition().edge_types {
            let et = EdgeType::new(t);
            let entry = out.edge_types.entry(t.clone()).or_default();
            for edge in self.get_edges_by_type(&et) {
                if members.contains(&edge.source) && members.contains(&edge.target) {
                    entry.0 += 1;
                    entry.1.extend(in_labels(edge.source));
                    entry.2.extend(in_labels(edge.target));
                }
            }
            out.total_edges += entry.0;
        }
        out
    }

    /// [`GraphStore::schema_summary`] restricted to a view: the NLQ prompt's
    /// schema when a question is asked of a named subgraph. Names no label
    /// and no relationship type outside it.
    pub fn schema_summary_for(&self, scope: &SubgraphScope) -> String {
        let schema = self.subgraph_schema(scope);
        let mut summary = String::new();
        summary.push_str("Node Labels:\n");
        for (label, count) in &schema.labels {
            summary.push_str(&format!("  :{label} ({count} nodes)\n"));
        }
        summary.push_str("\nRelationship Patterns:\n");
        for (t, (count, sources, targets)) in &schema.edge_types {
            if *count == 0 {
                continue;
            }
            for s in sources {
                for d in targets {
                    summary.push_str(&format!("  ({s})-[:{t}]->({d}) ({count} edges)\n"));
                }
            }
        }
        summary.push_str("\nKey Properties:\n");
        for label in scope.labels() {
            // The lowest-id member under this label.
            let first = self
                .node_ids_by_label(label, None)
                .into_iter()
                .filter(|id| scope.label_admits(self, label, *id))
                .min_by_key(|id| id.as_u64());
            if let Some(first) = first {
                let mut keys: Vec<String> = self.node_properties_full(first).into_keys().collect();
                keys.sort();
                keys.truncate(5);
                if !keys.is_empty() {
                    summary.push_str(&format!(
                        "  :{} has properties: {}\n",
                        label.as_str(),
                        keys.join(", ")
                    ));
                }
            }
        }
        summary
    }
}
