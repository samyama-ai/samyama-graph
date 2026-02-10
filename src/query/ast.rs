//! Abstract Syntax Tree for OpenCypher queries
//!
//! Implements REQ-CYPHER-001 through REQ-CYPHER-009

use crate::graph::{EdgeType, Label, PropertyValue};
use std::collections::HashMap;

/// Complete Cypher query representation
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// MATCH clauses
    pub match_clauses: Vec<MatchClause>,
    /// WHERE clause (optional)
    pub where_clause: Option<WhereClause>,
    /// RETURN clause (optional)
    pub return_clause: Option<ReturnClause>,
    /// CREATE clause (optional)
    pub create_clause: Option<CreateClause>,
    /// ORDER BY clause (optional)
    pub order_by: Option<OrderByClause>,
    /// LIMIT clause (optional)
    pub limit: Option<usize>,
    /// SKIP clause (optional)
    pub skip: Option<usize>,
    /// CALL clause (optional)
    pub call_clause: Option<CallClause>,
    /// DELETE clause (optional)
    pub delete_clause: Option<DeleteClause>,
    /// SET clauses
    pub set_clauses: Vec<SetClause>,
    /// REMOVE clauses
    pub remove_clauses: Vec<RemoveClause>,
    /// WITH clause (optional)
    pub with_clause: Option<WithClause>,
    /// CREATE VECTOR INDEX clause (optional)
    pub create_vector_index_clause: Option<CreateVectorIndexClause>,
    /// CREATE INDEX clause (optional)
    pub create_index_clause: Option<CreateIndexClause>,
    /// UNWIND clause (optional)
    pub unwind_clause: Option<UnwindClause>,
    /// MERGE clause (optional)
    pub merge_clause: Option<MergeClause>,
    /// UNION queries (chained via UNION/UNION ALL)
    pub union_queries: Vec<(Query, bool)>, // (query, is_union_all)
    /// EXPLAIN clause (optional)
    pub explain: bool,
}

/// CREATE VECTOR INDEX clause
#[derive(Debug, Clone, PartialEq)]
pub struct CreateVectorIndexClause {
    pub index_name: Option<String>,
    pub label: Label,
    pub property_key: String,
    pub dimensions: usize,
    pub similarity: String, // 'cosine', 'l2', etc.
}

/// CREATE INDEX clause
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexClause {
    pub label: Label,
    pub property: String,
}

/// CALL clause: CALL db.index.vector.queryNodes('Person', 'embedding', [...], 10) YIELD node, score
#[derive(Debug, Clone, PartialEq)]
pub struct CallClause {
    /// Procedure name (e.g., "db.index.vector.queryNodes")
    pub procedure_name: String,
    /// Procedure arguments
    pub arguments: Vec<Expression>,
    /// YIELD items
    pub yield_items: Vec<YieldItem>,
}

/// YIELD item: node AS n, score
#[derive(Debug, Clone, PartialEq)]
pub struct YieldItem {
    /// Name of the yielded variable
    pub name: String,
    /// Alias (optional)
    pub alias: Option<String>,
}

/// MATCH clause: MATCH (n:Person)-[:KNOWS]->(m)
#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    /// Pattern to match
    pub pattern: Pattern,
    /// Whether this is an optional match
    pub optional: bool,
}

/// Graph pattern
#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    /// Path patterns in this clause
    pub paths: Vec<PathPattern>,
}

/// Path pattern: (n:Person)-[:KNOWS*1..3]->(m:Person)
#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    /// Start node
    pub start: NodePattern,
    /// Edges and nodes
    pub segments: Vec<PathSegment>,
}

/// Segment of a path (edge + node)
#[derive(Debug, Clone, PartialEq)]
pub struct PathSegment {
    /// Edge pattern
    pub edge: EdgePattern,
    /// Target node pattern
    pub node: NodePattern,
}

/// Node pattern: (n:Person:Employee {name: "Alice"})
#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    /// Variable name (e.g., "n")
    pub variable: Option<String>,
    /// Labels (e.g., ["Person", "Employee"])
    pub labels: Vec<Label>,
    /// Property constraints
    pub properties: Option<HashMap<String, PropertyValue>>,
}

/// Edge pattern: -[:KNOWS|FOLLOWS*1..5]->
#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    /// Variable name (e.g., "r")
    pub variable: Option<String>,
    /// Edge types (e.g., ["KNOWS", "FOLLOWS"])
    pub types: Vec<EdgeType>,
    /// Direction
    pub direction: Direction,
    /// Variable length pattern
    pub length: Option<LengthPattern>,
    /// Property constraints
    pub properties: Option<HashMap<String, PropertyValue>>,
}

/// Edge direction
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Direction {
    /// Outgoing: ->
    Outgoing,
    /// Incoming: <-
    Incoming,
    /// Both: -
    Both,
}

/// Variable length pattern: *1..5 or * or *3
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LengthPattern {
    /// Minimum length (None = 1)
    pub min: Option<usize>,
    /// Maximum length (None = unbounded)
    pub max: Option<usize>,
}

/// WHERE clause with predicates
#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    /// Root predicate expression
    pub predicate: Expression,
}

/// Expression in WHERE or RETURN
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Property access: n.name
    Property {
        /// Variable name (e.g., "n")
        variable: String,
        /// Property name (e.g., "name")
        property: String,
    },
    /// Literal value
    Literal(PropertyValue),
    /// Binary operation
    Binary {
        /// Left operand
        left: Box<Expression>,
        /// Binary operator
        op: BinaryOp,
        /// Right operand
        right: Box<Expression>,
    },
    /// Unary operation
    Unary {
        /// Unary operator
        op: UnaryOp,
        /// Operand expression
        expr: Box<Expression>,
    },
    /// Function call
    Function {
        /// Function name
        name: String,
        /// Function arguments
        args: Vec<Expression>,
    },
    /// Variable reference
    Variable(String),
    /// CASE expression
    Case {
        /// Optional operand for simple CASE
        operand: Option<Box<Expression>>,
        /// WHEN condition THEN result pairs
        when_clauses: Vec<(Expression, Expression)>,
        /// ELSE default
        else_result: Option<Box<Expression>>,
    },
    /// List/map indexing: expr[index]
    Index {
        /// Expression being indexed
        expr: Box<Expression>,
        /// Index expression
        index: Box<Expression>,
    },
}

/// Binary operators
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp {
    /// Equal to (=)
    Eq,
    /// Not equal to (<>)
    Ne,
    /// Less than (<)
    Lt,
    /// Less than or equal to (<=)
    Le,
    /// Greater than (>)
    Gt,
    /// Greater than or equal to (>=)
    Ge,
    /// Logical AND
    And,
    /// Logical OR
    Or,
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
    Div,
    /// Modulo (%)
    Mod,
    /// String starts with
    StartsWith,
    /// String ends with
    EndsWith,
    /// String contains
    Contains,
    /// IN list membership
    In,
    /// Regex match (=~)
    RegexMatch,
}

/// Unary operators
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnaryOp {
    /// Logical NOT
    Not,
    /// Numeric negation (-)
    Minus,
    /// IS NULL check
    IsNull,
    /// IS NOT NULL check
    IsNotNull,
}

/// RETURN clause
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    /// Items to return
    pub items: Vec<ReturnItem>,
    /// Whether to return distinct results
    pub distinct: bool,
}

/// Return item: n, n.name AS name, count(n)
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    /// Expression to return
    pub expression: Expression,
    /// Alias (optional)
    pub alias: Option<String>,
}

/// CREATE clause
#[derive(Debug, Clone, PartialEq)]
pub struct CreateClause {
    /// Pattern to create
    pub pattern: Pattern,
}

/// DELETE clause
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteClause {
    /// Expressions to delete (variables referencing nodes/edges)
    pub expressions: Vec<Expression>,
    /// Whether DETACH DELETE (also removes relationships)
    pub detach: bool,
}

/// SET clause
#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    /// Items to set
    pub items: Vec<SetItem>,
}

/// SET item: n.name = "Alice"
#[derive(Debug, Clone, PartialEq)]
pub struct SetItem {
    /// Variable name
    pub variable: String,
    /// Property name
    pub property: String,
    /// Value expression
    pub value: Expression,
}

/// REMOVE clause
#[derive(Debug, Clone, PartialEq)]
pub struct RemoveClause {
    /// Items to remove
    pub items: Vec<RemoveItem>,
}

/// REMOVE item: n.name or n:Label
#[derive(Debug, Clone, PartialEq)]
pub enum RemoveItem {
    Property { variable: String, property: String },
    Label { variable: String, label: Label },
}

/// UNWIND clause: UNWIND [1,2,3] AS x
#[derive(Debug, Clone, PartialEq)]
pub struct UnwindClause {
    /// Expression to unwind (must evaluate to a list)
    pub expression: Expression,
    /// Variable name for each element
    pub variable: String,
}

/// MERGE clause: MERGE (n:Person {name: "Alice"}) ON CREATE SET ... ON MATCH SET ...
#[derive(Debug, Clone, PartialEq)]
pub struct MergeClause {
    /// Pattern to merge
    pub pattern: Pattern,
    /// ON CREATE SET items
    pub on_create_set: Vec<SetItem>,
    /// ON MATCH SET items
    pub on_match_set: Vec<SetItem>,
}

/// WITH clause
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    /// Items to pass through
    pub items: Vec<ReturnItem>,
    /// Whether DISTINCT
    pub distinct: bool,
    /// WHERE filter after WITH
    pub where_clause: Option<WhereClause>,
    /// ORDER BY within WITH
    pub order_by: Option<OrderByClause>,
    /// SKIP within WITH
    pub skip: Option<usize>,
    /// LIMIT within WITH
    pub limit: Option<usize>,
}

/// ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    /// Items to order by
    pub items: Vec<OrderByItem>,
}

/// Order by item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    /// Expression to order by
    pub expression: Expression,
    /// Order direction
    pub ascending: bool,
}

impl Query {
    /// Create a new empty query
    pub fn new() -> Self {
        Self {
            match_clauses: Vec::new(),
            where_clause: None,
            return_clause: None,
            create_clause: None,
            order_by: None,
            limit: None,
            skip: None,
            call_clause: None,
            delete_clause: None,
            set_clauses: Vec::new(),
            remove_clauses: Vec::new(),
            with_clause: None,
            create_vector_index_clause: None,
            create_index_clause: None,
            unwind_clause: None,
            merge_clause: None,
            union_queries: Vec::new(),
            explain: false,
        }
    }

    /// Check if this is a read-only query
    pub fn is_read_only(&self) -> bool {
        self.create_clause.is_none()
    }
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_pattern_creation() {
        let pattern = NodePattern {
            variable: Some("n".to_string()),
            labels: vec![Label::new("Person")],
            properties: None,
        };
        assert_eq!(pattern.variable, Some("n".to_string()));
        assert_eq!(pattern.labels.len(), 1);
    }

    #[test]
    fn test_edge_direction() {
        assert_ne!(Direction::Outgoing, Direction::Incoming);
        assert_ne!(Direction::Outgoing, Direction::Both);
    }

    #[test]
    fn test_query_is_read_only() {
        let mut query = Query::new();
        assert!(query.is_read_only());

        query.create_clause = Some(CreateClause {
            pattern: Pattern { paths: vec![] },
        });
        assert!(!query.is_read_only());
    }

    #[test]
    fn test_expression_types() {
        let prop = Expression::Property {
            variable: "n".to_string(),
            property: "name".to_string(),
        };
        assert!(matches!(prop, Expression::Property { .. }));

        let lit = Expression::Literal(PropertyValue::String("test".to_string()));
        assert!(matches!(lit, Expression::Literal(_)));
    }
}
