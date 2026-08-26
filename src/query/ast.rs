//! # Abstract Syntax Tree (AST) for OpenCypher Queries
//!
//! An **Abstract Syntax Tree** is a tree-shaped data structure that represents the
//! *semantic structure* of source code. It is "abstract" because it discards syntactic
//! noise that matters to the parser but not to the compiler -- parentheses for grouping,
//! semicolons, whitespace, and comments are all gone. What remains is a clean hierarchy
//! of *language constructs*: clauses, patterns, expressions, and literals.
//!
//! ## Cypher AST and Graph Patterns
//!
//! Cypher's pattern syntax maps naturally to a tree:
//!
//! ```text
//!   (a:Person)-[:KNOWS]->(b:Person)
//!        |          |          |
//!   PatternNode  PatternEdge  PatternNode
//!        \__________|__________/
//!              PatternPath
//!                   |
//!              MatchClause
//!                   |
//!                 Query   (root of the tree)
//! ```
//!
//! The [`Query`] struct is the **root node** of every AST. Its fields are all optional
//! because Cypher allows many clause combinations: `CREATE` without `MATCH`, `MATCH`
//! without `RETURN`, standalone `MERGE`, and so on. The planner inspects which fields
//! are populated to decide what execution plan to build.
//!
//! ## Recursive Types and `Box<T>` in Rust
//!
//! ASTs are inherently recursive -- an `Expression` can contain sub-expressions
//! (`1 + (2 * 3)`), and a `Query` can contain sub-queries (`CALL { ... }`). Rust
//! requires every type to have a known size at compile time, but a recursive type like
//! `Expression { left: Expression, right: Expression }` would be infinitely large.
//!
//! The solution is **`Box<T>`** -- a heap-allocated pointer with a fixed size (8 bytes
//! on 64-bit systems). By writing `Box<Expression>` instead of `Expression`, we break
//! the infinite-size cycle. The AST nodes in this module use `Box` extensively:
//! `Box<Expression>` in binary operations, `Box<Query>` in CALL subqueries, and
//! `Box<WhereClause>` in EXISTS subqueries (the `Option<Box<WhereClause>>` pattern
//! also breaks a recursive type cycle between `Expression` and `WhereClause`).

use crate::graph::{EdgeType, Label, PropertyValue};
use std::collections::HashMap;

/// The root AST node representing a complete Cypher query.
///
/// Every parsed Cypher statement produces exactly one `Query`. Its fields are grouped
/// into four logical categories:
///
/// 1. **Pattern matching**: `match_clauses`, `where_clause`, `post_with_where_clause` --
///    these define *what* to find in the graph (nodes, edges, paths, filters).
///
/// 2. **Mutations**: `create_clause`, `delete_clause`, `set_clauses`, `remove_clauses`,
///    `merge_clause`, `foreach_clause` -- these modify graph state. When any of these
///    are present, the planner sets `ExecutionPlan::is_write = true`, which routes
///    execution through `MutQueryExecutor` (requiring `&mut GraphStore`).
///
/// 3. **Projection and ordering**: `return_clause`, `order_by`, `skip`, `limit`,
///    `with_clause` -- these shape the output (column selection, sorting, pagination).
///    `WITH` acts as a pipeline barrier: it materializes intermediate results and
///    introduces a new scope, similar to a subquery in SQL.
///
/// 4. **Schema and introspection**: `create_index_clause`, `drop_index_clause`,
///    `create_constraint_clause`, `show_indexes`, `show_constraints`, `explain`,
///    `profile` -- these are DDL/DML operations and diagnostic flags.
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
    /// CALL subquery (optional)
    pub call_subquery: Option<Box<Query>>,
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
    /// DROP INDEX clause (optional)
    pub drop_index_clause: Option<DropIndexClause>,
    /// CREATE CONSTRAINT clause (optional)
    pub create_constraint_clause: Option<CreateConstraintClause>,
    /// CREATE HIERARCHY INDEX clause (optional) — ADR-035
    pub create_hierarchy_index_clause: Option<CreateHierarchyIndexClause>,
    /// DROP HIERARCHY INDEX name (optional)
    pub drop_hierarchy_index: Option<String>,
    /// REBUILD HIERARCHY INDEX name (optional)
    pub rebuild_hierarchy_index: Option<String>,
    /// SHOW HIERARCHY INDEXES flag
    pub show_hierarchy_indexes: bool,
    /// SHOW INDEXES flag
    pub show_indexes: bool,
    /// SHOW CONSTRAINTS flag
    pub show_constraints: bool,
    /// PROFILE flag
    pub profile: bool,
    /// Query parameters
    pub params: HashMap<String, PropertyValue>,
    /// FOREACH clause (optional)
    pub foreach_clause: Option<ForeachClause>,
    /// UNWIND clause (optional)
    pub unwind_clause: Option<UnwindClause>,
    /// Every clause of the query, in the order it was written.
    ///
    /// The fields above describe a query by *kind* — all the MATCHes here, the
    /// CREATE there — which is enough only while the grammar fixes one legal
    /// order. Cypher does not: a write may sit before a `WITH`, and two writes
    /// may be separated by a projection, and neither has anywhere to live in a
    /// shape-based representation. `CREATE (a) WITH a CREATE (b)` has two
    /// creates on opposite sides of a barrier and one `Option<CreateClause>`.
    ///
    /// Populated for every query. The planner uses it only when the legacy
    /// fields cannot express the query (`needs_clause_pipeline`), so the
    /// established paths are untouched and this grows into them rather than
    /// replacing them in one step.
    pub clauses: Vec<Clause>,
    /// The clause order is not expressible in the fields above, so the planner
    /// must walk `clauses` instead.
    pub needs_clause_pipeline: bool,
    /// Further `UNWIND`s written directly after the first, in order.
    ///
    /// Each is a cross product with everything before it. Kept beside
    /// `unwind_clause` rather than replacing it with a `Vec` because the
    /// single-UNWIND field is read in a dozen places that only ever care
    /// about the first one.
    pub extra_unwind_clauses: Vec<UnwindClause>,
    /// `UNWIND`s that appear **after** the last `WITH`.
    ///
    /// Kept apart from `extra_unwind_clauses` because position is the whole
    /// point: the planner applies the leading run before the WITH barrier, and
    /// applying a post-WITH unwind there reads a variable the WITH has not
    /// projected yet. `UNWIND [1,2] AS a WITH [3,4] AS b UNWIND b AS c` failed
    /// with `VariableNotFound("b")` for exactly that reason (#785).
    pub post_with_unwind_clauses: Vec<UnwindClause>,
    /// Whether the UNWIND *led* the statement (`UNWIND ... MATCH ...`) rather than
    /// following the match. The AST keeps a single `unwind_clause` with no position, but
    /// the two orders plan differently: a leading UNWIND must bind its variable before the
    /// WHERE runs, or a predicate referencing it fails with "Variable not found". A
    /// trailing UNWIND deliberately stays after the filter so the cross product is built
    /// from the already-narrowed rows.
    pub unwind_leading: bool,
    /// MERGE clause (optional)
    pub merge_clause: Option<MergeClause>,
    /// UNION queries (chained via UNION/UNION ALL)
    pub union_queries: Vec<(Query, bool)>, // (query, is_union_all)
    /// EXPLAIN clause (optional)
    pub explain: bool,
    /// Index into match_clauses where WITH clause splits pre-WITH from post-WITH.
    /// match_clauses[..split] belong to pre-WITH, match_clauses[split..] to post-WITH.
    pub with_split_index: Option<usize>,
    /// Post-WITH WHERE clause (second WHERE after WITH ... MATCH ... WHERE ...)
    pub post_with_where_clause: Option<WhereClause>,
    /// Additional WITH stages (for multi-WITH queries like WITH ... MATCH ... WITH ... RETURN)
    /// Each stage: (with_clause, unwind_clause, post_match_clauses, post_where_clause)
    pub extra_with_stages: Vec<(WithClause, Option<UnwindClause>, Vec<MatchClause>, Option<WhereClause>)>,
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
    /// Additional properties for composite indexes
    pub additional_properties: Vec<String>,
}

/// CREATE HIERARCHY INDEX clause (ADR-035).
///
/// `CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE d.units AGGREGATE sum, max`
///
/// The relationship pattern names the **covering relation**, oriented `child -> parent`.
/// A reversed arrow (`()<-[:HAS_CHILD]-()`) declares the same hierarchy stored the other
/// way round.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateHierarchyIndexClause {
    /// Index name.
    pub name: String,
    /// Edge types forming the covering relation.
    pub edge_types: Vec<String>,
    /// True when the stored edges point parent -> child.
    pub reverse: bool,
    /// Optional label qualifier on the measure.
    pub measure_label: Option<String>,
    /// Optional measure property.
    pub measure_property: Option<String>,
    /// Requested roll-up monoids (`sum`, `count`, `min`, `max`).
    pub aggregates: Vec<String>,
}

/// DROP INDEX clause
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexClause {
    pub label: Label,
    pub property: String,
}

/// Unique constraint clause
#[derive(Debug, Clone, PartialEq)]
pub struct CreateConstraintClause {
    pub variable: String,
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

/// MATCH clause: `MATCH (n:Person)-[:KNOWS]->(m)`
#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    /// Pattern to match
    pub pattern: Pattern,
    /// Whether this is an optional match
    pub optional: bool,
}

/// One clause of a query, as written.
///
/// A flat, ordered alternative to the by-kind fields on [`Query`]. Cypher is a
/// sequence of reading, writing and projecting clauses with few ordering
/// constraints; this is that sequence.
#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    /// `MATCH` / `OPTIONAL MATCH` — the clause carries its own `optional` flag.
    Match(MatchClause),
    /// A `WHERE` attached to the reading clause before it.
    Where(WhereClause),
    Unwind(UnwindClause),
    With(WithClause),
    Create(CreateClause),
    Merge(MergeClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
    Foreach(ForeachClause),
    Call(CallClause),
    Return(ReturnClause),
}

impl Clause {
    /// Whether this clause writes to the graph.
    ///
    /// The distinction the grammar used to encode positionally: writes were
    /// only allowed after the last projection.
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Clause::Create(_)
                | Clause::Merge(_)
                | Clause::Set(_)
                | Clause::Remove(_)
                | Clause::Delete(_)
                | Clause::Foreach(_)
        )
    }

    /// A short name, for plan descriptions and error messages.
    pub fn kind(&self) -> &'static str {
        match self {
            Clause::Match(m) if m.optional => "OPTIONAL MATCH",
            Clause::Match(_) => "MATCH",
            Clause::Where(_) => "WHERE",
            Clause::Unwind(_) => "UNWIND",
            Clause::With(_) => "WITH",
            Clause::Create(_) => "CREATE",
            Clause::Merge(_) => "MERGE",
            Clause::Set(_) => "SET",
            Clause::Remove(_) => "REMOVE",
            Clause::Delete(_) => "DELETE",
            Clause::Foreach(_) => "FOREACH",
            Clause::Call(_) => "CALL",
            Clause::Return(_) => "RETURN",
        }
    }
}

/// Graph pattern
#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    /// Path patterns in this clause
    pub paths: Vec<PathPattern>,
}

/// Path type for path patterns (normal, shortest, allShortest)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathType {
    Normal,
    Shortest,
    AllShortest,
}

/// Path pattern: `(n:Person)-[:KNOWS*1..3]->(m:Person)`
#[derive(Debug, Clone, PartialEq)]
pub struct PathPattern {
    /// Named path variable (e.g., `p` in `p = (a)-\[:KNOWS\]->(b)`)
    pub path_variable: Option<String>,
    /// Path type (Normal, Shortest, AllShortest)
    pub path_type: PathType,
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
    /// Property values that are not literals, e.g. `{n: p.n}` or `{id: row.id}`.
    ///
    /// Kept separate from `properties` so the literal case -- the overwhelming majority,
    /// and the only one usable for an index lookup -- keeps its concrete `PropertyValue`
    /// and every existing consumer is unaffected. Only CREATE and MERGE evaluate these,
    /// once per row; a MATCH pattern carrying one is rejected in planning rather than
    /// silently ignoring the constraint.
    pub property_exprs: Option<HashMap<String, Expression>>,
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
    /// Property values that are not literals, e.g. `{n: p.n}` or `{id: row.id}`.
    ///
    /// Kept separate from `properties` so the literal case -- the overwhelming majority,
    /// and the only one usable for an index lookup -- keeps its concrete `PropertyValue`
    /// and every existing consumer is unaffected. Only CREATE and MERGE evaluate these,
    /// once per row; a MATCH pattern carrying one is rejected in planning rather than
    /// silently ignoring the constraint.
    pub property_exprs: Option<HashMap<String, Expression>>,
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
        /// Whether DISTINCT modifier is present (e.g., count(DISTINCT x))
        distinct: bool,
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
    /// List/map indexing: `expr[index]`
    Index {
        /// Expression being indexed
        expr: Box<Expression>,
        /// Index expression
        index: Box<Expression>,
    },
    /// List slicing: expr[start..end]
    ListSlice {
        /// Expression being sliced
        expr: Box<Expression>,
        /// Optional start index (inclusive)
        start: Option<Box<Expression>>,
        /// Optional end index (exclusive)
        end: Option<Box<Expression>>,
    },
    /// EXISTS { MATCH pattern WHERE condition }
    ExistsSubquery {
        /// Pattern to match
        pattern: Pattern,
        /// Optional WHERE predicate
        where_clause: Option<Box<WhereClause>>,
        /// True when this came from a **bare pattern predicate** —
        /// `WHERE (n)-[r]->(a)` — rather than from `EXISTS { ... }`.
        ///
        /// The two desugar to the same node because they evaluate identically,
        /// but one rule separates them: a bare predicate may **not** introduce
        /// variables, while `EXISTS` may. Without this flag the check cannot
        /// tell them apart, and applying it to both rejects every
        /// `EXISTS { MATCH (n)-->(m) ... }` — which is what happened (#798).
        bare_pattern: bool,
    },
    /// List comprehension: [x IN list WHERE cond | expr]
    ListComprehension {
        /// Variable name
        variable: String,
        /// List expression
        list_expr: Box<Expression>,
        /// Optional filter predicate
        filter: Option<Box<Expression>>,
        /// Mapping expression
        map_expr: Box<Expression>,
    },
    /// Predicate function: all(x IN list WHERE pred), any(...), none(...), single(...)
    PredicateFunction {
        /// Function name (all, any, none, single)
        name: String,
        /// Iterator variable
        variable: String,
        /// List expression
        list_expr: Box<Expression>,
        /// Predicate expression
        predicate: Box<Expression>,
    },
    /// reduce(acc = init, x IN list | expr)
    Reduce {
        /// Accumulator variable name
        accumulator: String,
        /// Initial value expression
        init: Box<Expression>,
        /// Iterator variable name
        variable: String,
        /// List expression
        list_expr: Box<Expression>,
        /// Body expression
        expression: Box<Expression>,
    },
    /// Pattern comprehension: `[(a)-[:REL]->(b) WHERE cond | expr]`
    PatternComprehension {
        /// Pattern to match
        pattern: Pattern,
        /// Optional filter predicate
        filter: Option<Box<Expression>>,
        /// Projection expression
        projection: Box<Expression>,
    },
    /// Named path reference (for Value::Path)
    PathVariable(String),
    /// Query parameter reference ($name)
    Parameter(String),
    /// `[a, f(b), 1]` — a list literal whose elements are expressions.
    ///
    /// Distinct from `Literal(PropertyValue::Array(..))`, which can only hold
    /// literals. The grammar tries the literal form first, so this variant
    /// appears only for collections the literal form cannot express (#654).
    ListExpr(Vec<Expression>),
    /// `{k: a, j: f(b)}` — a map literal whose values are expressions.
    MapExpr(Vec<(String, Expression)>),
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
    /// Exponentiation (^). Right-associative and binds tighter than `*`.
    Pow,
    /// Exclusive or (XOR). Sits between OR and AND, as Cypher specifies.
    Xor,
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

/// The variable name standing for `RETURN *` / `WITH *` between parsing and
/// star expansion.
///
/// A sentinel rather than a new `Expression` variant because `*` is not a
/// value and never survives into a plan: `expand_stars` replaces it with the
/// variables in scope immediately after parsing, so nothing downstream can
/// encounter it. `*` is not a legal identifier in Cypher, so the sentinel
/// cannot collide with a user variable.
pub const STAR_ITEM: &str = "*";

/// Return item: n, n.name AS name, count(n)
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    /// Expression to return
    pub expression: Expression,
    /// Alias (optional)
    pub alias: Option<String>,
    /// The expression exactly as it was written, when it came from source.
    ///
    /// Cypher names an unaliased result column after its own text, so
    /// `RETURN count(*)` produces a column called `count(*)`. The planner used
    /// to reconstruct a name from the AST and fall back to `col_0`, which is
    /// not a name any client can ask for -- and it dropped the `*`, naming the
    /// column `count()` (#635).
    pub source_text: Option<String>,
}

impl ReturnItem {
    /// The result column this item produces.
    ///
    /// One place, because the planner had this decision written out in ten
    /// and they did not agree. `index` only matters for an expression with no
    /// recorded text, which now means one the engine built itself.
    pub fn column_name(&self, index: usize) -> String {
        if let Some(alias) = &self.alias {
            return alias.clone();
        }
        if let Some(text) = &self.source_text {
            return text.clone();
        }
        match &self.expression {
            Expression::Variable(v) => v.clone(),
            Expression::Property { variable, property } => format!("{variable}.{property}"),
            _ => format!("col_{index}"),
        }
    }
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
    /// Labels to add: `SET n:Admin`, `SET n:A:B`.
    ///
    /// Kept beside `items` rather than folded into it because `SetItem` is a
    /// struct read by field in several places, and an enum would touch all of
    /// them for no gain. `RemoveItem` is an enum because it always was.
    pub label_items: Vec<SetLabelItem>,
    /// Whole-entity assignment: `SET n = {…}` and `SET n += {…}`.
    pub entity_items: Vec<SetEntityItem>,
}

/// `SET n = <expr>` (replace every property) or `SET n += <expr>` (merge).
///
/// The right-hand side evaluates to a map, or to another node or relationship
/// whose properties are copied.
#[derive(Debug, Clone, PartialEq)]
pub struct SetEntityItem {
    /// Variable naming the entity being assigned to.
    pub variable: String,
    /// `true` for `+=` (merge), `false` for `=` (replace).
    pub merge: bool,
    /// The map, or entity, to take properties from.
    pub value: Expression,
}

/// SET item adding labels: `n:Admin`, `n:A:B`
#[derive(Debug, Clone, PartialEq)]
pub struct SetLabelItem {
    /// Variable name
    pub variable: String,
    /// Labels to add
    pub labels: Vec<Label>,
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

/// FOREACH clause: FOREACH (x IN list | SET x.prop = val)
#[derive(Debug, Clone, PartialEq)]
pub struct ForeachClause {
    /// Variable name for each element
    pub variable: String,
    /// List expression to iterate
    pub expression: Expression,
    /// SET items to apply for each element
    pub set_clauses: Vec<SetClause>,
    /// CREATE clauses to apply for each element
    pub create_clauses: Vec<CreateClause>,
}

/// UNWIND clause: `UNWIND [1,2,3] AS x`
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
    /// Labels added by `ON CREATE SET n:Label`.
    ///
    /// Separate from `on_create_set` for the same reason `SetClause` keeps
    /// `label_items` apart from `items`: the property form is read by field in
    /// several places and an enum would touch all of them.
    pub on_create_labels: Vec<SetLabelItem>,
    /// Labels added by `ON MATCH SET n:Label`.
    pub on_match_labels: Vec<SetLabelItem>,
    /// Whole-entity assignment in `ON CREATE SET n = {…}` / `n += {…}`.
    ///
    /// The grammar has always parsed these -- `set_entry` includes
    /// `set_entity_item` -- but `parse_merge_clause` matched only `set_item`
    /// and `set_label_item`, so they fell through its `match` and the clause
    /// the user wrote was **silently discarded** (#874).
    pub on_create_entity_set: Vec<SetEntityItem>,
    /// The same for `ON MATCH SET`.
    pub on_match_entity_set: Vec<SetEntityItem>,
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
            call_subquery: None,
            delete_clause: None,
            set_clauses: Vec::new(),
            remove_clauses: Vec::new(),
            with_clause: None,
            create_vector_index_clause: None,
            create_index_clause: None,
            drop_index_clause: None,
            create_constraint_clause: None,
            create_hierarchy_index_clause: None,
            drop_hierarchy_index: None,
            rebuild_hierarchy_index: None,
            show_hierarchy_indexes: false,
            show_indexes: false,
            show_constraints: false,
            profile: false,
            params: HashMap::new(),
            foreach_clause: None,
            unwind_clause: None,
            clauses: Vec::new(),
            needs_clause_pipeline: false,
            extra_unwind_clauses: Vec::new(),
            post_with_unwind_clauses: Vec::new(),
            unwind_leading: false,
            merge_clause: None,
            union_queries: Vec::new(),
            explain: false,
            with_split_index: None,
            post_with_where_clause: None,
            extra_with_stages: Vec::new(),
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
            property_exprs: None,
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
