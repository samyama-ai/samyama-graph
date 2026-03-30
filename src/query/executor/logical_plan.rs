//! Logical plan IR for the graph-native query planner (ADR-015)
//!
//! Separates plan structure from physical operators. Direction reversal is a
//! logical-level decision made before physical operator selection.

use std::collections::{HashMap, HashSet};
use crate::graph::types::{Label, EdgeType};
use crate::query::ast::{MatchClause, Expression};

/// Direction of an Expand operation in the logical plan
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandDirection {
    /// Follow edges in the stored direction (outgoing)
    Forward,
    /// Follow edges against the stored direction (incoming)
    Reverse,
}

/// A logical plan node — describes what to do, not how to do it
#[derive(Debug, Clone)]
pub enum LogicalPlanNode {
    /// Scan all nodes with a given label
    LabelScan {
        variable: String,
        label: Option<Label>,
    },
    /// Start from specific node IDs (e.g., from index lookup)
    IndexLookup {
        variable: String,
        label: Label,
        property: String,
        value: Expression,
    },
    /// Expand from a bound node to discover new neighbors
    Expand {
        input: Box<LogicalPlanNode>,
        source_var: String,
        target_var: String,
        edge_var: Option<String>,
        edge_types: Vec<EdgeType>,
        direction: ExpandDirection,
    },
    /// Check if an edge exists between two already-bound nodes
    ExpandInto {
        input: Box<LogicalPlanNode>,
        source_var: String,
        target_var: String,
        edge_types: Vec<EdgeType>,
        edge_var: Option<String>,
    },
    /// Apply a filter predicate
    Filter {
        input: Box<LogicalPlanNode>,
        predicate: Expression,
    },
    /// Join two sub-plans on shared variables
    Join {
        left: Box<LogicalPlanNode>,
        right: Box<LogicalPlanNode>,
        join_keys: Vec<String>,
    },
    /// Cartesian product of two sub-plans
    CartesianProduct {
        left: Box<LogicalPlanNode>,
        right: Box<LogicalPlanNode>,
    },
}

impl LogicalPlanNode {
    /// Get all variables bound by this plan node and its inputs
    pub fn bound_variables(&self) -> HashSet<String> {
        match self {
            LogicalPlanNode::LabelScan { variable, .. } => {
                let mut s = HashSet::new();
                s.insert(variable.clone());
                s
            }
            LogicalPlanNode::IndexLookup { variable, .. } => {
                let mut s = HashSet::new();
                s.insert(variable.clone());
                s
            }
            LogicalPlanNode::Expand { input, source_var, target_var, edge_var, .. } => {
                let mut s = input.bound_variables();
                s.insert(source_var.clone());
                s.insert(target_var.clone());
                if let Some(ev) = edge_var {
                    s.insert(ev.clone());
                }
                s
            }
            LogicalPlanNode::ExpandInto { input, source_var, target_var, edge_var, .. } => {
                let mut s = input.bound_variables();
                s.insert(source_var.clone());
                s.insert(target_var.clone());
                if let Some(ev) = edge_var {
                    s.insert(ev.clone());
                }
                s
            }
            LogicalPlanNode::Filter { input, .. } => input.bound_variables(),
            LogicalPlanNode::Join { left, right, .. } => {
                let mut s = left.bound_variables();
                s.extend(right.bound_variables());
                s
            }
            LogicalPlanNode::CartesianProduct { left, right } => {
                let mut s = left.bound_variables();
                s.extend(right.bound_variables());
                s
            }
        }
    }

    /// Format as indented plan text (same format as physical EXPLAIN output)
    pub fn display_plan(&self, indent: usize) -> String {
        let prefix = "   ".repeat(indent);
        let connector = if indent > 0 { "+- " } else { "" };
        match self {
            LogicalPlanNode::LabelScan { variable, label } => {
                let lbl = label.as_ref().map(|l| format!("\"{}\"", l.as_str())).unwrap_or_else(|| "*".to_string());
                format!("{}{}NodeScan (var={}, labels=[{}])", prefix, connector, variable, lbl)
            }
            LogicalPlanNode::IndexLookup { variable, label, property, .. } => {
                format!("{}{}IndexScan (var={}, label=\"{}\", prop={})", prefix, connector, variable, label.as_str(), property)
            }
            LogicalPlanNode::Expand { input, source_var, target_var, edge_types, direction, .. } => {
                let dir_str = match direction {
                    ExpandDirection::Forward => format!("({})-[:{}]->({})", source_var, Self::fmt_etypes(edge_types), target_var),
                    ExpandDirection::Reverse => format!("({})<-[:{}]-({})", source_var, Self::fmt_etypes(edge_types), target_var),
                };
                let child = input.display_plan(indent + 1);
                format!("{}{}Expand ({})\n{}", prefix, connector, dir_str, child)
            }
            LogicalPlanNode::ExpandInto { input, source_var, target_var, edge_types, .. } => {
                let types = Self::fmt_etypes(edge_types);
                let child = input.display_plan(indent + 1);
                format!("{}{}ExpandInto ({}<-[:{}]->{})\n{}", prefix, connector, source_var, types, target_var, child)
            }
            LogicalPlanNode::Filter { input, predicate } => {
                let child = input.display_plan(indent + 1);
                format!("{}{}Filter ({:?})\n{}", prefix, connector, predicate, child)
            }
            LogicalPlanNode::Join { left, right, join_keys, .. } => {
                let l = left.display_plan(indent + 1);
                let r = right.display_plan(indent + 1);
                format!("{}{}Join (on=[{}])\n{}\n{}", prefix, connector, join_keys.join(", "), l, r)
            }
            LogicalPlanNode::CartesianProduct { left, right } => {
                let l = left.display_plan(indent + 1);
                let r = right.display_plan(indent + 1);
                format!("{}{}CartesianProduct\n{}\n{}", prefix, connector, l, r)
            }
        }
    }

    fn fmt_etypes(edge_types: &[EdgeType]) -> String {
        if edge_types.is_empty() {
            "*".to_string()
        } else {
            edge_types.iter().map(|e| e.as_str()).collect::<Vec<_>>().join("|")
        }
    }
}

// ============================================================
// PatternGraph: parse MATCH clause into a graph topology
// ============================================================

/// A node in the pattern graph
#[derive(Debug, Clone)]
pub struct PatternNode {
    pub variable: String,
    pub labels: Vec<Label>,
}

/// An edge in the pattern graph
#[derive(Debug, Clone)]
pub struct PatternEdge {
    /// Source variable (always the arrow's tail in the stored graph)
    pub source_var: String,
    /// Target variable (always the arrow's head in the stored graph)
    pub target_var: String,
    pub edge_var: Option<String>,
    pub edge_types: Vec<EdgeType>,
    /// Direction as written in the AST (Outgoing = -[]->, Incoming = <-[]--, Both = -[]-)
    pub ast_direction: AstDirection,
}

/// Direction as parsed from the Cypher AST
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AstDirection {
    Outgoing,   // (a)-[]->(b)
    Incoming,   // (a)<-[]-(b)
    Both,       // (a)-[]-(b)
}

/// A pattern graph extracted from a MATCH clause
#[derive(Debug, Clone)]
pub struct PatternGraph {
    pub nodes: HashMap<String, PatternNode>,
    pub edges: Vec<PatternEdge>,
}

impl PatternGraph {
    /// Build a PatternGraph from a MATCH clause
    pub fn from_match_clause(clause: &MatchClause) -> Self {
        let mut nodes = HashMap::new();
        let mut edges = Vec::new();

        for path_pattern in &clause.pattern.paths {
            // Process the start node
            let start_var = path_pattern.start.variable.clone().unwrap_or_default();
            if !start_var.is_empty() {
                let labels: Vec<Label> = path_pattern.start.labels.iter().cloned().collect();
                nodes.entry(start_var.clone()).or_insert_with(|| PatternNode {
                    variable: start_var.clone(),
                    labels,
                });
            }

            // Process segments (edge + node pairs)
            let mut prev_var = start_var;
            for segment in &path_pattern.segments {
                let next_var = segment.node.variable.clone().unwrap_or_default();
                if !next_var.is_empty() {
                    let labels: Vec<Label> = segment.node.labels.iter().cloned().collect();
                    nodes.entry(next_var.clone()).or_insert_with(|| PatternNode {
                        variable: next_var.clone(),
                        labels,
                    });
                }

                let edge_types: Vec<EdgeType> = segment.edge.types.iter().cloned().collect();

                let ast_dir = match segment.edge.direction {
                    crate::query::ast::Direction::Outgoing => AstDirection::Outgoing,
                    crate::query::ast::Direction::Incoming => AstDirection::Incoming,
                    crate::query::ast::Direction::Both => AstDirection::Both,
                };

                // Normalize: for Incoming, swap source/target so source is always the arrow's tail
                let (source, target) = match ast_dir {
                    AstDirection::Incoming => (next_var.clone(), prev_var.clone()),
                    _ => (prev_var.clone(), next_var.clone()),
                };

                edges.push(PatternEdge {
                    source_var: source,
                    target_var: target,
                    edge_var: segment.edge.variable.clone(),
                    edge_types,
                    ast_direction: ast_dir,
                });

                prev_var = next_var;
            }
        }

        PatternGraph { nodes, edges }
    }

    /// Get all neighbor edges for a node variable
    pub fn neighbors(&self, variable: &str) -> Vec<(usize, &PatternEdge)> {
        self.edges.iter().enumerate()
            .filter(|(_, e)| e.source_var == variable || e.target_var == variable)
            .collect()
    }

    /// Get neighbor edges that connect to unvisited nodes
    pub fn unvisited_neighbors(&self, variable: &str, visited: &HashSet<String>) -> Vec<(usize, &PatternEdge)> {
        self.neighbors(variable)
            .into_iter()
            .filter(|(_, e)| {
                let other = if e.source_var == variable { &e.target_var } else { &e.source_var };
                !visited.contains(other)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::*;

    fn make_match_clause(paths: Vec<PathPattern>) -> MatchClause {
        MatchClause {
            pattern: Pattern { paths },
            optional: false,
        }
    }

    fn make_path(start_var: &str, start_labels: Vec<Label>, segments: Vec<PathSegment>) -> PathPattern {
        PathPattern {
            path_variable: None,
            path_type: PathType::Normal,
            start: NodePattern {
                variable: Some(start_var.to_string()),
                labels: start_labels,
                properties: None,
            },
            segments,
        }
    }

    fn make_segment(edge_var: Option<&str>, edge_types: Vec<EdgeType>, dir: Direction, node_var: &str, node_labels: Vec<Label>) -> PathSegment {
        PathSegment {
            edge: EdgePattern {
                variable: edge_var.map(|s| s.to_string()),
                types: edge_types,
                direction: dir,
                length: None,
                properties: None,
            },
            node: NodePattern {
                variable: Some(node_var.to_string()),
                labels: node_labels,
                properties: None,
            },
        }
    }

    #[test]
    fn test_bound_variables_label_scan() {
        let plan = LogicalPlanNode::LabelScan {
            variable: "n".to_string(),
            label: Some(Label::new("Person")),
        };
        let vars = plan.bound_variables();
        assert_eq!(vars.len(), 1);
        assert!(vars.contains("n"));
    }

    #[test]
    fn test_bound_variables_expand() {
        let plan = LogicalPlanNode::Expand {
            input: Box::new(LogicalPlanNode::LabelScan {
                variable: "a".to_string(),
                label: Some(Label::new("Person")),
            }),
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_var: Some("r".to_string()),
            edge_types: vec![EdgeType::new("KNOWS")],
            direction: ExpandDirection::Forward,
        };
        let vars = plan.bound_variables();
        assert_eq!(vars.len(), 3);
        assert!(vars.contains("a"));
        assert!(vars.contains("b"));
        assert!(vars.contains("r"));
    }

    #[test]
    fn test_bound_variables_expand_into() {
        let inner = LogicalPlanNode::Join {
            left: Box::new(LogicalPlanNode::LabelScan { variable: "a".to_string(), label: Some(Label::new("Person")) }),
            right: Box::new(LogicalPlanNode::LabelScan { variable: "b".to_string(), label: Some(Label::new("Person")) }),
            join_keys: vec![],
        };
        let plan = LogicalPlanNode::ExpandInto {
            input: Box::new(inner),
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_types: vec![],
            edge_var: None,
        };
        let vars = plan.bound_variables();
        assert!(vars.contains("a"));
        assert!(vars.contains("b"));
    }

    #[test]
    fn test_pattern_graph_from_match_simple() {
        // MATCH (a:Person)-[:KNOWS]->(b:Person)
        let clause = make_match_clause(vec![make_path(
            "a", vec![Label::new("Person")],
            vec![make_segment(Some("r"), vec![EdgeType::new("KNOWS")], Direction::Outgoing, "b", vec![Label::new("Person")])],
        )]);

        let pg = PatternGraph::from_match_clause(&clause);
        assert_eq!(pg.nodes.len(), 2);
        assert!(pg.nodes.contains_key("a"));
        assert!(pg.nodes.contains_key("b"));
        assert_eq!(pg.edges.len(), 1);
        assert_eq!(pg.edges[0].source_var, "a");
        assert_eq!(pg.edges[0].target_var, "b");
        assert_eq!(pg.edges[0].ast_direction, AstDirection::Outgoing);
    }

    #[test]
    fn test_pattern_graph_incoming_direction() {
        // MATCH (a:Person)<-[:FOLLOWS]-(b:Person)
        let clause = make_match_clause(vec![make_path(
            "a", vec![Label::new("Person")],
            vec![make_segment(None, vec![EdgeType::new("FOLLOWS")], Direction::Incoming, "b", vec![Label::new("Person")])],
        )]);

        let pg = PatternGraph::from_match_clause(&clause);
        assert_eq!(pg.edges[0].source_var, "b"); // normalized: source is the arrow's tail
        assert_eq!(pg.edges[0].target_var, "a");
        assert_eq!(pg.edges[0].ast_direction, AstDirection::Incoming);
    }

    #[test]
    fn test_pattern_graph_chain() {
        // MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c)
        let clause = make_match_clause(vec![make_path(
            "a", vec![],
            vec![
                make_segment(None, vec![EdgeType::new("KNOWS")], Direction::Outgoing, "b", vec![]),
                make_segment(None, vec![EdgeType::new("WORKS_AT")], Direction::Outgoing, "c", vec![]),
            ],
        )]);

        let pg = PatternGraph::from_match_clause(&clause);
        assert_eq!(pg.nodes.len(), 3);
        assert_eq!(pg.edges.len(), 2);

        // b has 2 neighbors (a and c)
        let b_neighbors = pg.neighbors("b");
        assert_eq!(b_neighbors.len(), 2);

        // unvisited neighbors of b when a is already visited
        let mut visited = HashSet::new();
        visited.insert("a".to_string());
        visited.insert("b".to_string());
        let unvisited = pg.unvisited_neighbors("b", &visited);
        assert_eq!(unvisited.len(), 1);
    }

    #[test]
    fn test_expand_direction_equality() {
        assert_eq!(ExpandDirection::Forward, ExpandDirection::Forward);
        assert_ne!(ExpandDirection::Forward, ExpandDirection::Reverse);
    }
}
