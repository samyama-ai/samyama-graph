//! Semantic checks the grammar cannot express (openCypher TCK negative cases).
//!
//! A parser that accepts everything is not lenient, it is wrong: the TCK has
//! 55 scenarios whose entire assertion is *"this query must be rejected"*, and
//! an engine that runs them anyway gives an answer to a question that was
//! never well-formed. Two of those answers are actively dangerous —
//! `RETURN a AS x, b AS x` silently drops a column, and `CREATE (a:Foo)` over
//! a variable already bound by `MATCH` reads as "add a label" when Cypher
//! says it is an error.
//!
//! Only rules that are **unambiguous and local** live here. Scope analysis
//! ("is this variable defined?") is deliberately absent: getting it slightly
//! wrong would reject valid queries, which is a far worse failure than
//! accepting an invalid one, and this engine's own benchmarks and loaders
//! would be the first casualties.

use std::collections::HashSet;

use crate::query::ast::{Expression, OrderByClause, Query, ReturnItem};

/// Why a query was rejected. Carries the offending name so the message can
/// say which one rather than that something, somewhere, was wrong.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    DuplicateColumn(String),
    UnionColumnMismatch { left: Vec<String>, right: Vec<String> },
    MixedUnionAndUnionAll,
    CreateOnBoundVariable(String),
    CreateRelationshipWithoutType,
    CreateUndirectedRelationship,
    CreateVariableLengthRelationship,
    CreateOnBoundRelationship(String),
    MergeRelationshipWithoutType,
    MergeVariableLengthRelationship,
    MergeOnBoundVariable(String),
    MergeRelationshipWithNullProperty(String),
    VariableTypeConflict(String),
    /// A boolean operator given an operand that cannot be a boolean.
    NonBooleanOperand(&'static str),
    /// Property access on a value that cannot carry properties.
    PropertyAccessOnNonMap { name: String, what: &'static str },
    /// A pattern predicate naming a variable nothing has bound.
    UnboundPatternVariable(String),
    /// One variable bound to two different kinds of entity.
    VariableKindConflict {
        name: String,
        first: &'static str,
        second: &'static str,
    },
    PatternInSetValue,
    /// `size()` applied to a pattern or a path (#843).
    SizeOfNonCollection(&'static str),
    /// A bare pattern used where a value is expected (#880).
    PatternInProjection(&'static str),
    /// `DELETE` applied to something that is not an entity (#887).
    InvalidDeleteTarget(String),
    /// An ORDER BY naming something the projection did not keep.
    OrderByUndefinedVariable(String),
    /// A name used in an expression that nothing in the query binds.
    UnboundVariable(String),
    /// A `WITH` item that is not a bare variable and has no alias.
    UnaliasedWithItem,
    /// A call to a function this engine does not implement.
    UnknownFunction(String),
    /// A projection mixes an aggregate with an expression that is not a
    /// grouping key. See [`validate_aggregation_is_unambiguous`].
    ///
    /// Distinct from [`Self::AmbiguousAggregationExpression`], which is the
    /// same openCypher error name raised for the narrower ORDER BY case; both
    /// render as `AmbiguousAggregationExpression`, which is what the TCK
    /// matches on.
    AmbiguousGroupingExpression(String),
    /// An aggregate where aggregation is not allowed.
    AggregateNotAllowed(&'static str),
    /// A function applied to the wrong kind of entity: (function, wanted, got).
    FunctionArgumentKind(&'static str, &'static str, &'static str),
    /// An ORDER BY item that mixes an aggregate with a grouping expression.
    AmbiguousAggregationExpression,
    /// An ORDER BY that aggregates when the projection does not.
    InvalidAggregation,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnboundVariable(v) => write!(
                f,
                "`{v}` is not bound: nothing in this query defines it"
            ),
            Self::UnaliasedWithItem => write!(
                f,
                "every WITH item that is not a bare variable needs an alias: nothing \
                 downstream can name the column otherwise"
            ),
            Self::FunctionArgumentKind(func, wanted, got) => write!(
                f,
                "`{func}()` takes {wanted}, and was given {got}"
            ),
            Self::UnknownFunction(name) => write!(
                f,
                "UnknownFunction: `{name}` is not a function this engine implements. \
                 Checked at compile time on purpose: as a run-time error it only \
                 fired on a row that reached the call, so the same query over an \
                 empty graph returned no rows and reported success"
            ),
            Self::AmbiguousGroupingExpression(what) => write!(
                f,
                "AmbiguousAggregationExpression: {what} appears inside an expression \
                 that also aggregates, but is not itself a grouping key. Cypher \
                 groups by the projection's non-aggregating items, so there is no \
                 group for this to be evaluated over -- project it as its own item, \
                 or move it past a WITH"
            ),
            Self::AggregateNotAllowed(where_) => write!(
                f,
                "an aggregate function is not allowed {where_}"
            ),
            Self::OrderByUndefinedVariable(v) => write!(
                f,
                "`{v}` is not available to ORDER BY: the projection aggregates or is \
                 DISTINCT, so only its own columns survive. Project `{v}` if the sort \
                 needs it."
            ),
            Self::AmbiguousAggregationExpression => write!(
                f,
                "an ORDER BY item that contains an aggregate may not also contain a \
                 grouping expression — it is ambiguous whether the expression means the \
                 grouped value or the value inside the aggregate. Sort by the projected \
                 alias instead."
            ),
            Self::InvalidAggregation => write!(
                f,
                "ORDER BY introduces an aggregate that the projection does not have. \
                 Aggregate in the RETURN or WITH, then sort by its alias."
            ),
            Self::PatternInSetValue => write!(
                f,
                "a relationship pattern cannot be used as a value on the right of SET; \
                 patterns belong in MATCH, WHERE or a pattern comprehension"
            ),
            Self::VariableKindConflict { name, first, second } => write!(
                f,
                "`{name}` is bound to {first} and then to {second} in the same scope. A \
                 variable names one entity, and an entity is a node, a relationship or a \
                 path -- not two of them. Rename one of the two."
            ),
            Self::UnboundPatternVariable(name) => write!(
                f,
                "`{name}` is introduced by a pattern used as a predicate, which tests \
                 whether a pattern exists rather than binding anything. Bind `{name}` \
                 in a MATCH first, or use EXISTS {{ ... }}, which may introduce names."
            ),
            Self::PropertyAccessOnNonMap { name, what } => write!(
                f,
                "`{name}` is {what}, so it has no properties to read. Only a map, \
                 a node or a relationship does."
            ),
            Self::NonBooleanOperand(what) => write!(
                f,
                "AND, OR and XOR need boolean operands, and this one is {what}. \
                 A value whose type is only known at run time is fine here; a \
                 literal of the wrong type is not."
            ),
            Self::SizeOfNonCollection(what) => write!(
                f,
                "size() takes a list or a string, not {what}; \
                 use length() for a path"
            ),
            Self::PatternInProjection(where_) => write!(
                f,
                "a pattern is a predicate, not a value; it cannot be projected by {where_}. \
                 Use `EXISTS {{ ... }}` for a boolean, or a pattern comprehension for a list"
            ),
            Self::InvalidDeleteTarget(what) => write!(
                f,
                "DELETE takes a node, relationship or path; {what}"
            ),
            Self::VariableTypeConflict(name) => write!(
                f,
                "`{name}` is bound to a collection and cannot be used as a node or \
                 relationship in a pattern"
            ),
            Self::MergeRelationshipWithoutType => write!(
                f,
                "MERGE requires exactly one relationship type: `MERGE (a)-->(b)` does not say \
                 what kind of edge to create"
            ),
            Self::MergeVariableLengthRelationship => write!(
                f,
                "MERGE cannot use a variable-length relationship: it does not say what the \
                 intermediate nodes are"
            ),
            Self::MergeOnBoundVariable(name) => write!(
                f,
                "MERGE cannot impose new labels or properties on `{name}`, which is already \
                 bound; use SET instead"
            ),
            Self::MergeRelationshipWithNullProperty(key) => write!(
                f,
                "MERGE cannot create a relationship with a null property (`{key}`): the pattern \
                 would never match what it creates"
            ),
            Self::DuplicateColumn(name) => write!(
                f,
                "Multiple result columns with the same name are not supported: `{name}`"
            ),
            Self::UnionColumnMismatch { left, right } => write!(
                f,
                "All sub queries in an UNION must have the same column names: {left:?} vs {right:?}"
            ),
            Self::MixedUnionAndUnionAll => write!(
                f,
                "Cannot mix UNION and UNION ALL in the same query"
            ),
            Self::CreateOnBoundVariable(name) => write!(
                f,
                "Variable `{name}` already declared; CREATE cannot add labels or properties to it"
            ),
            Self::CreateRelationshipWithoutType => write!(
                f,
                "Exactly one relationship type must be specified for CREATE"
            ),
            Self::CreateUndirectedRelationship => write!(
                f,
                "Only directed relationships are supported in CREATE"
            ),
            Self::CreateVariableLengthRelationship => write!(
                f,
                "Variable length relationships cannot be created"
            ),
            Self::CreateOnBoundRelationship(name) => write!(
                f,
                "Variable `{name}` already declared; CREATE cannot rebind a relationship"
            ),
        }
    }
}

/// The column name a return item produces, if it has one that can collide.
///
/// An unaliased expression like `count(*)` has a generated name that Cypher
/// does not treat as a user-visible column for collision purposes, so only
/// aliases and bare variables are considered.
fn column_name(item: &crate::query::ast::ReturnItem) -> Option<String> {
    if let Some(alias) = &item.alias {
        return Some(alias.clone());
    }
    match &item.expression {
        Expression::Variable(v) => Some(v.clone()),
        _ => None,
    }
}

fn columns(query: &Query) -> Vec<String> {
    query
        .return_clause
        .as_ref()
        .map(|rc| rc.items.iter().filter_map(column_name).collect())
        .unwrap_or_default()
}

/// Variables a MATCH clause binds — the ones a later CREATE must not redeclare.
fn pattern_variables(pattern: &crate::query::ast::Pattern, out: &mut HashSet<String>) {
    for path in &pattern.paths {
        if let Some(v) = &path.start.variable {
            out.insert(v.clone());
        }
        for seg in &path.segments {
            if let Some(v) = &seg.edge.variable {
                out.insert(v.clone());
            }
            if let Some(v) = &seg.node.variable {
                out.insert(v.clone());
            }
        }
    }
}

/// Which write clause a pattern came from. The two differ: an undirected
/// `MERGE (a)-[:R]-(b)` is legal Cypher and an undirected CREATE is not.
#[derive(Clone, Copy, PartialEq)]
enum WriteKind {
    Create,
    Merge,
}

/// A `WITH` re-scopes: only names it projects *by name* stay bound on the far
/// side, so a name it drops is free again and a later CREATE/MERGE may reuse it.
///
/// This mirrors `carry_kinds_through_with` for the bound-variable checks.
/// Without it, `MATCH (a)-[r]->(b) WITH a CREATE (r:X)` was rejected because `r`
/// looked already-bound, even though the WITH does not carry it through — a
/// valid query refused for no reason the user can act on (#764).
///
/// Only a bare `WITH v` (optionally `AS alias`) keeps a name bound as an entity.
/// `WITH count(r) AS r` recomputes the name into a scalar that is no longer the
/// original entity, so it does not carry forward here — reusing that name in a
/// CREATE is legal, and any genuine type conflict is `validate_variable_kinds`'
/// concern, not this one.
fn carry_names_through_with(
    bound: &HashSet<String>,
    wc: &crate::query::ast::WithClause,
) -> HashSet<String> {
    let mut next = HashSet::new();
    for item in &wc.items {
        if let Expression::Variable(v) = &item.expression {
            if bound.contains(v) {
                next.insert(item.alias.clone().unwrap_or_else(|| v.clone()));
            }
        }
    }
    next
}

/// Every write pattern in the order it was written, each paired with the
/// variables in scope *before* it.
///
/// A query reaches the validator in one of two shapes: the by-kind fields
/// (`match_clauses`, `create_clause`, `merge_clause`), or an ordered
/// `clauses` list when the clause-sequence path parsed it. Reading only the
/// first shape is how these checks came to be inert for half the queries in
/// the language — `CREATE (a:Foo) MERGE (a)-[r:KNOWS]->(a:Bar)` has no
/// `create_clause` and no `merge_clause`, so every rule below simply did not
/// run and the query was accepted. The scope has to be tracked in written
/// order anyway, so both shapes are flattened into one sequence here.
fn write_patterns(query: &Query) -> Vec<(WriteKind, &crate::query::ast::Pattern, HashSet<String>)> {
    use crate::query::ast::Clause;
    let mut out = Vec::new();
    let mut bound: HashSet<String> = HashSet::new();

    if query.needs_clause_pipeline {
        for clause in &query.clauses {
            match clause {
                Clause::Match(mc) => pattern_variables(&mc.pattern, &mut bound),
                Clause::Unwind(uc) => {
                    bound.insert(uc.variable.clone());
                }
                Clause::Create(cc) => {
                    out.push((WriteKind::Create, &cc.pattern, bound.clone()));
                    pattern_variables(&cc.pattern, &mut bound);
                }
                Clause::Merge(mc) => {
                    out.push((WriteKind::Merge, &mc.pattern, bound.clone()));
                    pattern_variables(&mc.pattern, &mut bound);
                }
                // A WITH re-scopes: only the names it projects survive it, so a
                // name it drops is free for a later CREATE/MERGE to reuse. Not
                // applying this boundary rejected valid queries like
                // `MATCH (a)-[r]->(b) WITH a CREATE (r:X)` (#764).
                Clause::With(wc) => bound = carry_names_through_with(&bound, wc),
                _ => {}
            }
        }
        return out;
    }

    // Pre-WITH matches bind first; a WITH then re-scopes the set to only what
    // it projects, so a CREATE/MERGE after the WITH may reuse a name the WITH
    // dropped (#764). `with_split_index` marks where the leading WITH cuts the
    // match list.
    let upto = query.with_split_index.unwrap_or(query.match_clauses.len());
    for mc in query.match_clauses.iter().take(upto) {
        pattern_variables(&mc.pattern, &mut bound);
    }
    if let Some(wc) = &query.with_clause {
        bound = carry_names_through_with(&bound, wc);
    }
    for mc in query.match_clauses.iter().skip(upto) {
        pattern_variables(&mc.pattern, &mut bound);
    }
    if let Some(create) = &query.create_clause {
        out.push((WriteKind::Create, &create.pattern, bound.clone()));
        pattern_variables(&create.pattern, &mut bound);
    }
    if let Some(merge) = &query.merge_clause {
        out.push((WriteKind::Merge, &merge.pattern, bound.clone()));
    }
    out
}


/// The aggregate functions, by the name the parser produces.
const AGGREGATE_NAMES: &[&str] = &[
    "count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp",
    "percentilecont", "percentiledisc",
];

fn is_aggregate_call(expr: &Expression) -> bool {
    matches!(expr, Expression::Function { name, .. }
        if AGGREGATE_NAMES.contains(&name.to_lowercase().as_str()))
}

/// Does this expression contain an aggregate anywhere inside it?
///
/// *Anywhere*, which it did not do: `walk_children` applies its closure to the
/// **immediate** children only, so this saw one level. `count(a) > 10` was
/// found and `a.n > 1 AND count(a) > 10` was not -- the same aggregate, one
/// conjunction deeper.
fn contains_aggregate(expr: &Expression) -> bool {
    is_aggregate_call(expr) || child_expressions(expr).into_iter().any(contains_aggregate)
}

/// Variables named outside any aggregate call within `expr`.
///
/// `avg(person.age) + $p` names none: `person` sits inside the aggregate and
/// `$p` is a parameter. `me.age + count(*)` names `me`, which is the case the
/// TCK rejects.
fn vars_outside_aggregates(expr: &Expression, out: &mut Vec<String>) {
    match expr {
        e if is_aggregate_call(e) => {}
        Expression::Variable(v) => out.push(v.clone()),
        Expression::Property { variable, .. } => out.push(variable.clone()),
        other => walk_children(other, &mut |e| vars_outside_aggregates(e, out)),
    }
}

/// Apply `f` to the immediate sub-expressions of `expr`.
fn walk_children(expr: &Expression, f: &mut impl FnMut(&Expression)) {
    match expr {
        Expression::Binary { left, right, .. } => {
            f(left);
            f(right);
        }
        Expression::Unary { expr: inner, .. } => f(inner),
        Expression::Function { args, .. } => args.iter().for_each(|a| f(a)),
        Expression::Index { expr: inner, index } => {
            f(inner);
            f(index);
        }
        Expression::ListExpr(items) => items.iter().for_each(|e| f(e)),
        Expression::MapExpr(pairs) => pairs.iter().for_each(|(_, e)| f(e)),
        _ => {}
    }
}

/// What an ORDER BY may reference once the projection has aggregated or
/// de-duplicated the row.
///
/// Such a projection *replaces* the row, so a sort key has to be expressible in
/// terms of what survived. openCypher makes anything else a compile-time error;
/// we sorted on whatever the variable still happened to hold.
///
/// The rule has two halves, and both are needed — an earlier version enforced
/// only the second and rejected two scenarios the TCK requires to work:
///
/// 1. **Every aggregate in the ORDER BY must match one the projection
///    computed.** `RETURN me.age AS age, count(you.age) AS cnt ORDER BY
///    me.age + count(you.age)` is fine because `count(you.age)` was projected;
///    `WITH mod, min(sum) ORDER BY sum(sum)` is not, because `sum(sum)` was
///    never computed and `sum` no longer exists to compute it from.
///
/// 2. **Every variable or property outside an aggregate must be a projected
///    grouping key or an alias.** `me.age` qualifies when the projection says
///    `me.age AS age`. It does not when the projection says
///    `me.age + you.age` — the compound is projected, its parts are not, and
///    which one the sort means is ambiguous.
///
/// Constants and parameters are always fine, which keeps
/// `ORDER BY $age + avg(p.age) - 1000` legal.
fn validate_order_by_scope(
    items: &[ReturnItem],
    distinct: bool,
    order_by: Option<&OrderByClause>,
) -> Result<(), ValidationError> {
    let Some(order_by) = order_by else {
        return Ok(());
    };
    let projection_aggregates = items.iter().any(|i| contains_aggregate(&i.expression));
    if !projection_aggregates && !distinct {
        for item in &order_by.items {
            if contains_aggregate(&item.expression) {
                return Err(ValidationError::InvalidAggregation);
            }
        }
        return Ok(());
    }

    let aliases: HashSet<String> = items
        .iter()
        .enumerate()
        .filter_map(|(i, it)| column_name_at(it, i))
        .collect();
    // What the projection computed: grouping keys, and the aggregates themselves.
    let mut grouping: Vec<&Expression> = Vec::new();
    let mut projected_aggs: Vec<&Expression> = Vec::new();
    for it in items {
        if contains_aggregate(&it.expression) {
            collect_aggregates(&it.expression, &mut projected_aggs);
        } else {
            grouping.push(&it.expression);
        }
    }
    let grouping_vars: HashSet<String> = {
        let mut v = Vec::new();
        for g in &grouping {
            vars_outside_aggregates(g, &mut v);
        }
        v.into_iter().collect()
    };

    for item in &order_by.items {
        let expr = &item.expression;

        // (0) The whole sort key restates a projected grouping expression —
        // `WITH a.num2 % 3 AS m ... ORDER BY a.num2 % 3`. That is the same
        // value under a different spelling, and it is the commoner of the two
        // spellings. Checked against the *item*, not against its leaves: an
        // earlier version compared only leaves and rejected this.
        //
        // Deliberately before the aggregate branch and not inside it: a
        // grouping expression buried in a sort key that also aggregates stays
        // ambiguous, which is what separates this from the rejected case.
        if grouping.iter().any(|g| *g == expr) {
            continue;
        }

        // (1) aggregates in the sort key must be ones the projection computed
        let mut sort_aggs: Vec<&Expression> = Vec::new();
        collect_aggregates(expr, &mut sort_aggs);
        for a in &sort_aggs {
            if !projected_aggs.iter().any(|p| *p == *a) {
                let mut inner = Vec::new();
                collect_all_vars(a, &mut inner);
                let unknown = inner.into_iter().find(|v| !aliases.contains(v));
                return Err(match unknown {
                    Some(v) => ValidationError::OrderByUndefinedVariable(v),
                    None => ValidationError::InvalidAggregation,
                });
            }
        }

        // (2) everything outside an aggregate must be projected
        let mut outside = Vec::new();
        collect_leaf_exprs_outside_aggregates(expr, &mut outside);
        for leaf in outside {
            if let Expression::Variable(v) = leaf {
                if aliases.contains(v) {
                    continue;
                }
            }
            if let Some(name) = simple_column_name(leaf) {
                if aliases.contains(&name) {
                    continue;
                }
            }
            if grouping.iter().any(|g| *g == leaf) {
                continue;
            }
            // `RETURN DISTINCT a ORDER BY a.name` — projecting the *entity*
            // keeps its properties reachable, because `a` itself survives.
            // That is what separates it from `RETURN DISTINCT a.name ORDER BY
            // a.age`, where only the one property survived and `a` is gone.
            if let Expression::Property { variable, .. } = leaf {
                let entity_projected = aliases.contains(variable)
                    || grouping
                        .iter()
                        .any(|g| matches!(g, Expression::Variable(v) if v == variable));
                if entity_projected {
                    continue;
                }
            }
            let named = match leaf {
                Expression::Variable(v) => v.clone(),
                Expression::Property { variable, .. } => variable.clone(),
                _ => continue,
            };
            // Projected as part of a compound the sort re-states: which of the
            // two the sort means is ambiguous. Never projected at all: it is
            // simply not there.
            return Err(if grouping_vars.contains(&named) {
                ValidationError::AmbiguousAggregationExpression
            } else {
                ValidationError::OrderByUndefinedVariable(named)
            });
        }
    }
    Ok(())
}

/// Every aggregate call inside `expr`, outermost first.
fn collect_aggregates<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    if is_aggregate_call(expr) {
        out.push(expr);
        return;
    }
    walk_children_ref(expr, &mut |e| collect_aggregates(e, out));
}

/// Variable/property leaves that are *not* inside an aggregate call.
fn collect_leaf_exprs_outside_aggregates<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    match expr {
        e if is_aggregate_call(e) => {}
        Expression::Variable(_) | Expression::Property { .. } => out.push(expr),
        other => walk_children_ref(other, &mut |e| collect_leaf_exprs_outside_aggregates(e, out)),
    }
}

/// Every variable named anywhere in `expr`, aggregates included.
fn collect_all_vars(expr: &Expression, out: &mut Vec<String>) {
    match expr {
        Expression::Variable(v) => out.push(v.clone()),
        Expression::Property { variable, .. } => out.push(variable.clone()),
        other => walk_children_ref(other, &mut |e| collect_all_vars(e, out)),
    }
}

/// `me.age` -> "me.age", `x` -> "x"; anything compound has no simple name.
fn simple_column_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Variable(v) => Some(v.clone()),
        Expression::Property { variable, property } => Some(format!("{variable}.{property}")),
        _ => None,
    }
}

fn walk_children_ref<'a>(expr: &'a Expression, f: &mut impl FnMut(&'a Expression)) {
    match expr {
        Expression::Binary { left, right, .. } => {
            f(left);
            f(right);
        }
        Expression::Unary { expr: inner, .. } => f(inner),
        Expression::Function { args, .. } => args.iter().for_each(|a| f(a)),
        Expression::Index { expr: inner, index } => {
            f(inner);
            f(index);
        }
        Expression::ListExpr(items) => items.iter().for_each(|e| f(e)),
        Expression::MapExpr(pairs) => pairs.iter().for_each(|(_, e)| f(e)),
        _ => {}
    }
}

/// The column a return item produces, by alias or by written form.
fn column_name_at(item: &ReturnItem, _idx: usize) -> Option<String> {
    item.alias.clone().or_else(|| match &item.expression {
        Expression::Variable(v) => Some(v.clone()),
        Expression::Property { variable, property } => Some(format!("{variable}.{property}")),
        _ => None,
    })
}


/// What a pattern binds a variable to. A variable may be exactly one of these
/// for as long as it stays in scope.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EntityKind {
    Node,
    Relationship,
    Path,
}

impl EntityKind {
    fn noun(self) -> &'static str {
        match self {
            Self::Node => "a node",
            Self::Relationship => "a relationship",
            Self::Path => "a path",
        }
    }
}

/// Record `name` as binding `kind`, or report the clash.
fn note_kind(
    seen: &mut std::collections::HashMap<String, EntityKind>,
    name: &Option<String>,
    kind: EntityKind,
) -> Result<(), ValidationError> {
    let Some(name) = name else { return Ok(()) };
    match seen.get(name) {
        Some(prev) if *prev != kind => Err(ValidationError::VariableKindConflict {
            name: name.clone(),
            first: prev.noun(),
            second: kind.noun(),
        }),
        _ => {
            seen.insert(name.clone(), kind);
            Ok(())
        }
    }
}

/// Collect the kinds one pattern binds.
fn note_pattern_kinds(
    seen: &mut std::collections::HashMap<String, EntityKind>,
    pattern: &crate::query::ast::Pattern,
) -> Result<(), ValidationError> {
    for path in &pattern.paths {
        note_kind(seen, &path.path_variable, EntityKind::Path)?;
        note_kind(seen, &path.start.variable, EntityKind::Node)?;
        for seg in &path.segments {
            note_kind(seen, &seg.edge.variable, EntityKind::Relationship)?;
            note_kind(seen, &seg.node.variable, EntityKind::Node)?;
        }
    }
    Ok(())
}

/// A `WITH` opens a new scope. Variables it passes through *by name* keep the
/// kind they had; anything it computes is a fresh value whose kind this check
/// no longer claims to know, so it is dropped rather than guessed.
fn carry_kinds_through_with(
    seen: &std::collections::HashMap<String, EntityKind>,
    wc: &crate::query::ast::WithClause,
) -> std::collections::HashMap<String, EntityKind> {
    let mut next = std::collections::HashMap::new();
    for item in &wc.items {
        if let Expression::Variable(v) = &item.expression {
            if let Some(kind) = seen.get(v) {
                // `WITH r AS x` carries the relationship under a new name.
                next.insert(item.alias.clone().unwrap_or_else(|| v.clone()), *kind);
            }
        }
    }
    next
}

/// One variable may not be a node here and a relationship there.
///
/// openCypher binds a variable to an entity, and the entity has a kind: a node,
/// a relationship, or a path. `MATCH (r), ()-[r]-()` asks for `r` to be both,
/// which has no answer, and the TCK asserts a failure for every arrangement of
/// it — same pattern, same clause, preceding clause, all three kinds against
/// each other. **227 scenarios**, all of this one rule, and we returned rows
/// for every one of them.
///
/// Rows, not an error: the second binding simply overwrote the first, so the
/// query "succeeded" with an answer to a question that was never well-formed.
///
/// Only pattern bindings are examined. A `WITH` that recomputes a name is left
/// alone -- see `carry_kinds_through_with` -- because the cost of being wrong
/// here is rejecting a valid query, which is worse than accepting an invalid
/// one (the rule this module opens with).
fn validate_variable_kinds(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;
    let mut seen: std::collections::HashMap<String, EntityKind> = std::collections::HashMap::new();

    if !query.clauses.is_empty() {
        for clause in &query.clauses {
            match clause {
                Clause::Match(mc) => note_pattern_kinds(&mut seen, &mc.pattern)?,
                Clause::Create(cc) => note_pattern_kinds(&mut seen, &cc.pattern)?,
                Clause::Merge(mc) => note_pattern_kinds(&mut seen, &mc.pattern)?,
                Clause::With(wc) => seen = carry_kinds_through_with(&seen, wc),
                _ => {}
            }
        }
        return Ok(());
    }

    // The by-kind representation. A query parsed into these fields has an empty
    // `clauses`, so both shapes have to be walked or the rule holds for half
    // the queries -- the same split that cost #710 a day.
    let upto = query.with_split_index.unwrap_or(query.match_clauses.len());
    for mc in query.match_clauses.iter().take(upto) {
        note_pattern_kinds(&mut seen, &mc.pattern)?;
    }
    if let Some(wc) = &query.with_clause {
        seen = carry_kinds_through_with(&seen, wc);
        for mc in query.match_clauses.iter().skip(upto) {
            note_pattern_kinds(&mut seen, &mc.pattern)?;
        }
    }
    for (wc, _, matches, _) in &query.extra_with_stages {
        seen = carry_kinds_through_with(&seen, wc);
        for mc in matches {
            note_pattern_kinds(&mut seen, &mc.pattern)?;
        }
    }
    // CREATE and MERGE come *after* the WITH stages, so their kinds must be
    // noted after the scope resets -- walking them first reads them against a
    // scope they never see. `MATCH (a)-[r]->(b) WITH a CREATE (r:X)` is legal:
    // `r` is not carried through the WITH, so it is unbound and the CREATE
    // makes a fresh node. Checked in the old order, that is Relationship vs
    // Node and a **valid query gets rejected** -- the failure this module's
    // opening comment warns is the worse one. Caught in review, not by a test,
    // so `a_write_after_with_may_reuse_a_dropped_name` now pins it.
    if let Some(cc) = &query.create_clause {
        note_pattern_kinds(&mut seen, &cc.pattern)?;
    }
    if let Some(mc) = &query.merge_clause {
        note_pattern_kinds(&mut seen, &mc.pattern)?;
    }
    Ok(())
}


/// Variables a pattern binds.
fn pattern_vars(pattern: &crate::query::ast::Pattern, out: &mut HashSet<String>) {
    for path in &pattern.paths {
        if let Some(v) = &path.path_variable { out.insert(v.clone()); }
        if let Some(v) = &path.start.variable { out.insert(v.clone()); }
        for seg in &path.segments {
            if let Some(v) = &seg.edge.variable { out.insert(v.clone()); }
            if let Some(v) = &seg.node.variable { out.insert(v.clone()); }
        }
    }
}

/// The names a projection makes available downstream.
fn projected_names(items: &[ReturnItem]) -> HashSet<String> {
    let mut out = HashSet::new();
    for item in items {
        if let Some(a) = &item.alias {
            out.insert(a.clone());
        } else if let Expression::Variable(v) = &item.expression {
            out.insert(v.clone());
        }
    }
    out
}

/// Every variable an expression reads.
fn expression_vars(e: &Expression, out: &mut HashSet<String>) {
    use Expression::*;
    match e {
        Variable(v) => { out.insert(v.clone()); }
        Property { variable, .. } => { out.insert(variable.clone()); }
        Binary { left, right, .. } => { expression_vars(left, out); expression_vars(right, out); }
        Unary { expr, .. } => expression_vars(expr, out),
        Function { args, .. } => for a in args { expression_vars(a, out) },
        Index { expr, index } => { expression_vars(expr, out); expression_vars(index, out); }
        Case { operand, when_clauses, else_result } => {
            if let Some(o) = operand { expression_vars(o, out); }
            for (w, t) in when_clauses { expression_vars(w, out); expression_vars(t, out); }
            if let Some(e) = else_result { expression_vars(e, out); }
        }
        _ => {}
    }
}

/// `ORDER BY` may not name a variable that is not in scope.
///
/// ```text
/// MATCH (a:A), (b:B), (c:C)
/// WITH a, b
/// WITH a ORDER BY c        <-- c was dropped two clauses ago
/// RETURN a
/// ```
///
/// 40 TCK scenarios across WithOrderBy1 and WithOrderBy3 assert a
/// `SyntaxError` here and we answered them, sorting by a column that does not
/// exist.
///
/// **Deliberately conservative**, because this module opens by saying scope
/// analysis is absent precisely so that valid queries are not rejected. Two
/// choices keep it safe:
///
/// * the allowed set is the projection **plus the scope that preceded it**, not
///   the projection alone. `MATCH (n) RETURN n.name ORDER BY n.age` is legal —
///   `n` is in scope even though the projected column is `n.name` — and a
///   stricter rule would reject it.
/// * anything the walk cannot account for leaves the scope *empty*, and an
///   empty scope checks nothing. A query shape this does not model is passed
///   through rather than guessed at.
fn validate_order_by_in_scope(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;
    let mut scope: HashSet<String> = HashSet::new();
    let mut seen_any_binding = false;

    // The by-kind shape. A multi-WITH query parses into `with_clause` plus
    // `extra_with_stages` and leaves `clauses` empty, so declining it here
    // would leave the rule checking nothing at all -- which is exactly what
    // the first version of this did, and it measured +0. The two-representation
    // split has now cost four separate fixes; assume neither shape.
    if query.clauses.is_empty() {
        // The by-kind shape, and its WITH order is **not** the obvious one:
        // `extra_with_stages` holds the *earlier* WITHs in order and
        // `with_clause` holds the **last** one. Walking `with_clause` first --
        // which is what it looks like it should be -- checks the final ORDER BY
        // against the scope of the first clause and silently finds nothing.
        // Verified against a three-WITH query rather than assumed.
        let upto = query.with_split_index.unwrap_or(query.match_clauses.len());
        for mc in query.match_clauses.iter().take(upto) {
            pattern_vars(&mc.pattern, &mut scope);
            seen_any_binding = true;
        }
        for (wc, _, matches, _) in &query.extra_with_stages {
            let projected = projected_names(&wc.items);
            if let Some(ob) = &wc.order_by {
                check_order_by(ob, &projected, &scope, seen_any_binding)?;
            }
            scope = projected;
            // A WITH projection *is* exact scope knowledge -- more exact than a
            // pattern, in fact -- so it satisfies the guard. Requiring a
            // pattern binding first silently skipped every query of the form
            // `WITH 1 AS a ... ORDER BY c`, which is 30 of the 40 scenarios
            // this rule exists for.
            seen_any_binding = true;
            for mc in matches {
                pattern_vars(&mc.pattern, &mut scope);
            }
        }
        if let Some(wc) = &query.with_clause {
            let projected = projected_names(&wc.items);
            if let Some(ob) = &wc.order_by {
                check_order_by(ob, &projected, &scope, seen_any_binding)?;
            }
            scope = projected;
            for mc in query.match_clauses.iter().skip(upto) {
                pattern_vars(&mc.pattern, &mut scope);
            }
        }
        if let Some(rc) = &query.return_clause {
            let projected = projected_names(&rc.items);
            if let Some(ob) = &query.order_by {
                check_order_by(ob, &projected, &scope, seen_any_binding)?;
            }
        }
        return Ok(());
    }

    for clause in &query.clauses {
        match clause {
            Clause::Match(mc) => { pattern_vars(&mc.pattern, &mut scope); seen_any_binding = true; }
            Clause::Create(cc) => { pattern_vars(&cc.pattern, &mut scope); seen_any_binding = true; }
            Clause::Merge(mc) => { pattern_vars(&mc.pattern, &mut scope); seen_any_binding = true; }
            Clause::Unwind(u) => { scope.insert(u.variable.clone()); seen_any_binding = true; }
            Clause::With(wc) => {
                let projected = projected_names(&wc.items);
                if let Some(ob) = &wc.order_by {
                    check_order_by(ob, &projected, &scope, seen_any_binding)?;
                }
                scope = projected;
                seen_any_binding = true;
            }
            Clause::Return(rc) => {
                let projected = projected_names(&rc.items);
                if let Some(ob) = &query.order_by {
                    check_order_by(ob, &projected, &scope, seen_any_binding)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn check_order_by(
    ob: &OrderByClause,
    projected: &HashSet<String>,
    scope: &HashSet<String>,
    seen_any_binding: bool,
) -> Result<(), ValidationError> {
    // Nothing was bound anywhere the walk could see, so there is no scope to
    // check against. Saying "undefined" here would reject `RETURN 1 ORDER BY 1`
    // and anything else this walk does not model.
    if !seen_any_binding {
        return Ok(());
    }
    for item in &ob.items {
        let mut used = HashSet::new();
        expression_vars(&item.expression, &mut used);
        for v in used {
            if !projected.contains(&v) && !scope.contains(&v) {
                return Err(ValidationError::OrderByUndefinedVariable(v));
            }
        }
    }
    Ok(())
}


/// `AND` / `OR` / `XOR` require boolean operands, and a literal that is not one
/// is a compile-time error.
///
/// ```text
/// RETURN 123 AND true      -> SyntaxError: InvalidArgumentType
/// ```
///
/// 26 TCK scenarios across Boolean1, Boolean2 and Boolean4. We answered every
/// one of them.
///
/// **Only statically-known operands are checked.** `n.prop AND true` is legal
/// to *write* — the type is unknown until the row arrives, and Cypher reports
/// a runtime error then, not a compile-time one. Checking anything whose type
/// is not known from the text would reject valid queries, which this module
/// opens by naming as the worse failure.
///
/// `null` is allowed: it is the unknown boolean, and `123.4 AND null` fails on
/// the `123.4`.
fn validate_boolean_operands(query: &Query) -> Result<(), ValidationError> {
    fn statically_non_boolean(e: &Expression) -> Option<&'static str> {
        match e {
            Expression::Literal(p) => match p {
                crate::graph::PropertyValue::Boolean(_) | crate::graph::PropertyValue::Null => None,
                crate::graph::PropertyValue::Integer(_) => Some("an integer"),
                crate::graph::PropertyValue::Float(_) => Some("a float"),
                crate::graph::PropertyValue::String(_) => Some("a string"),
                crate::graph::PropertyValue::Array(_) => Some("a list"),
                crate::graph::PropertyValue::Map(_) => Some("a map"),
                _ => None,
            },
            // A list or map *literal* is known to be a list or map whatever is
            // inside it -- `[true]` is a list, not a boolean.
            Expression::ListExpr(_) => Some("a list"),
            Expression::MapExpr(_) => Some("a map"),
            _ => None,
        }
    }

    fn walk(e: &Expression) -> Result<(), ValidationError> {
        if let Expression::Binary { left, op, right } = e {
            if matches!(op, crate::query::ast::BinaryOp::And
                          | crate::query::ast::BinaryOp::Or
                          | crate::query::ast::BinaryOp::Xor)
            {
                for side in [left, right] {
                    if let Some(what) = statically_non_boolean(side) {
                        return Err(ValidationError::NonBooleanOperand(what));
                    }
                }
            }
        }
        for child in child_expressions(e) {
            walk(child)?;
        }
        Ok(())
    }

    for e in all_expressions(query) {
        walk(e)?;
    }
    Ok(())
}

/// The sub-expressions of an expression, for a generic walk.
fn child_expressions(e: &Expression) -> Vec<&Expression> {
    use Expression::*;
    match e {
        Binary { left, right, .. } => vec![left, right],
        Unary { expr, .. } => vec![expr],
        Function { args, .. } => args.iter().collect(),
        Index { expr, index } => vec![expr, index],
        Case { operand, when_clauses, else_result } => {
            let mut v: Vec<&Expression> = Vec::new();
            if let Some(o) = operand { v.push(o); }
            for (w, t) in when_clauses { v.push(w); v.push(t); }
            if let Some(x) = else_result { v.push(x); }
            v
        }
        ListExpr(items) => items.iter().collect(),
        MapExpr(entries) => entries.iter().map(|(_, v)| v).collect(),
        ListSlice { expr, start, end } => {
            let mut v = vec![expr.as_ref()];
            if let Some(s) = start { v.push(s); }
            if let Some(e) = end { v.push(e); }
            v
        }
        _ => Vec::new(),
    }
}

/// Every expression a query evaluates, for a generic walk.
///
/// Both AST shapes, because a query parsed into one leaves the other empty --
/// the split that has now cost six separate fixes.
fn all_expressions(query: &Query) -> Vec<&Expression> {
    use crate::query::ast::Clause;
    let mut out: Vec<&Expression> = Vec::new();
    fn push_items<'a>(items: &'a [ReturnItem], out: &mut Vec<&'a Expression>) {
        for i in items {
            out.push(&i.expression);
        }
    }
    if !query.clauses.is_empty() {
        for c in &query.clauses {
            match c {
                Clause::Return(r) => push_items(&r.items, &mut out),
                Clause::With(w) => push_items(&w.items, &mut out),
                Clause::Where(w) => out.push(&w.predicate),
                Clause::Unwind(u) => out.push(&u.expression),
                _ => {}
            }
        }
    }
    if let Some(r) = &query.return_clause {
        push_items(&r.items, &mut out);
    }
    if let Some(w) = &query.with_clause {
        push_items(&w.items, &mut out);
    }
    for (w, _, _, _) in &query.extra_with_stages {
        push_items(&w.items, &mut out);
    }
    if let Some(w) = &query.where_clause {
        out.push(&w.predicate);
    }
    out
}


/// Property access on something that cannot have properties (#791).
///
/// ```text
/// WITH 123 AS nonMap RETURN nonMap.num     -> TypeError: InvalidArgumentType
/// ```
///
/// The TCK asks for this **at compile time**, which bounds what can be
/// checked: only a variable whose value is a literal of a non-map type, taken
/// from the projection that introduced it. `n.name` where `n` is a node, or a
/// map, or anything read from the graph, is untouched — the type is not known
/// from the text, and rejecting it would break every ordinary query.
///
/// So this fires on `WITH <literal> AS x ... x.prop` and nothing else. Narrow
/// on purpose: `validate.rs` opens by naming over-rejection as the worse
/// failure, and property access is the single most common expression in Cypher.
fn validate_property_access_targets(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;

    /// Literal types that cannot carry properties. A map can; a node,
    /// relationship or anything read at run time is not visible here.
    fn non_map_literal(e: &Expression) -> Option<&'static str> {
        match e {
            Expression::Literal(p) => match p {
                crate::graph::PropertyValue::Integer(_) => Some("an integer"),
                crate::graph::PropertyValue::Float(_) => Some("a float"),
                crate::graph::PropertyValue::String(_) => Some("a string"),
                crate::graph::PropertyValue::Boolean(_) => Some("a boolean"),
                crate::graph::PropertyValue::Array(_) => Some("a list"),
                _ => None,
            },
            Expression::ListExpr(_) => Some("a list"),
            _ => None,
        }
    }

    // Names a projection binds to a literal of a non-map type.
    let mut bad: std::collections::HashMap<String, &'static str> =
        std::collections::HashMap::new();
    let mut note = |items: &[ReturnItem], bad: &mut std::collections::HashMap<String, &'static str>| {
        for item in items {
            let Some(alias) = &item.alias else { continue };
            match non_map_literal(&item.expression) {
                Some(what) => { bad.insert(alias.clone(), what); }
                // Re-binding the name to anything else clears it.
                None => { bad.remove(alias); }
            }
        }
    };

    let mut check = |e: &Expression, bad: &std::collections::HashMap<String, &'static str>|
        -> Result<(), ValidationError> {
        if let Expression::Property { variable, .. } = e {
            if let Some(what) = bad.get(variable) {
                return Err(ValidationError::PropertyAccessOnNonMap {
                    name: variable.clone(),
                    what,
                });
            }
        }
        Ok(())
    };

    if !query.clauses.is_empty() {
        for clause in &query.clauses {
            match clause {
                Clause::With(w) => {
                    for item in &w.items { check(&item.expression, &bad)?; }
                    note(&w.items, &mut bad);
                }
                Clause::Return(r) => {
                    for item in &r.items { check(&item.expression, &bad)?; }
                }
                Clause::Where(w) => check(&w.predicate, &bad)?,
                _ => {}
            }
        }
        return Ok(());
    }

    // The by-kind shape: earlier WITHs first, then the last, then RETURN.
    for (wc, _, _, _) in &query.extra_with_stages {
        for item in &wc.items { check(&item.expression, &bad)?; }
        note(&wc.items, &mut bad);
    }
    if let Some(wc) = &query.with_clause {
        for item in &wc.items { check(&item.expression, &bad)?; }
        note(&wc.items, &mut bad);
    }
    if let Some(w) = &query.where_clause {
        check(&w.predicate, &bad)?;
    }
    if let Some(rc) = &query.return_clause {
        for item in &rc.items { check(&item.expression, &bad)?; }
    }
    Ok(())
}


/// Whether a projection binds this name to something that **cannot** be a node,
/// relationship or path — so using it as a pattern is a type conflict.
///
/// ```text
/// WITH 123 AS n MATCH (n) RETURN n     -> SyntaxError: VariableTypeConflict
/// ```
///
/// Was lists and maps only, which covered `WITH [n] AS users MATCH (users)`
/// (#654) and missed every scalar — 16 TCK scenarios across Match1 [11]/[13]
/// and Match6 [25], covering `true`, `123`, `123.4`, `'foo'` and `null` as
/// node, relationship and path variables.
///
/// Literals only, and deliberately: `WITH n.prop AS x MATCH (x)` cannot be
/// judged from the text, and `validate.rs` opens by naming over-rejection as
/// the worse failure. Two copies of this test existed and both were narrow;
/// they now share one function, because the next widening should not have to
/// find both.
fn not_an_entity(e: &Expression) -> bool {
    matches!(
        e,
        Expression::ListExpr(_)
            | Expression::MapExpr(_)
            | Expression::Literal(
                crate::graph::PropertyValue::Array(_)
                    | crate::graph::PropertyValue::Map(_)
                    | crate::graph::PropertyValue::Integer(_)
                    | crate::graph::PropertyValue::Float(_)
                    | crate::graph::PropertyValue::String(_)
                    | crate::graph::PropertyValue::Boolean(_)
            )
    )
}


/// A pattern predicate in `WHERE` may not **introduce** variables (#798).
///
/// ```text
/// MATCH (n) WHERE (n)-[r]->(a) RETURN n
///   -> SyntaxError: UndefinedVariable      -- r and a are bound nowhere
/// ```
///
/// The pattern is a *test*, not a match: it asks whether an edge exists, and
/// `r` and `a` have no meaning outside it. openCypher requires them to be
/// already bound; `EXISTS { ... }` is the form that may introduce names.
///
/// 15 scenarios in `Pattern1` [10]. We answered every one, silently binding
/// variables that go nowhere.
///
/// Anonymous positions are fine — `(n)-[]->()` introduces nothing — which is
/// why the check is on *named* variables rather than on pattern complexity.
fn validate_pattern_predicate_vars(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;

    /// Names a pattern binds, ignoring anonymous positions.
    fn pattern_named(pattern: &crate::query::ast::Pattern) -> HashSet<String> {
        let mut out = HashSet::new();
        pattern_vars(pattern, &mut out);
        out
    }

    /// Walk an expression for pattern predicates, checking each against scope.
    fn walk(
        e: &Expression,
        bound: &HashSet<String>,
    ) -> Result<(), ValidationError> {
        // Only a *bare* pattern predicate is restricted. `EXISTS { ... }`
        // desugars to the same node and may introduce names -- that is the
        // whole difference between the two spellings, and checking both
        // rejects every `EXISTS { MATCH (n)-->(m) }` (#798).
        if let Expression::ExistsSubquery { pattern, bare_pattern: true, .. } = e {
            for name in pattern_named(pattern) {
                if !bound.contains(&name) {
                    return Err(ValidationError::UnboundPatternVariable(name));
                }
            }
        }
        for child in child_expressions(e) {
            walk(child, bound)?;
        }
        Ok(())
    }

    // What the reading clauses have bound by the time the WHERE runs.
    let mut bound: HashSet<String> = HashSet::new();

    if !query.clauses.is_empty() {
        for clause in &query.clauses {
            match clause {
                Clause::Match(mc) => pattern_vars(&mc.pattern, &mut bound),
                Clause::Create(cc) => pattern_vars(&cc.pattern, &mut bound),
                Clause::Merge(mc) => pattern_vars(&mc.pattern, &mut bound),
                Clause::Unwind(u) => { bound.insert(u.variable.clone()); }
                Clause::With(w) => bound = projected_names(&w.items),
                Clause::Where(w) => walk(&w.predicate, &bound)?,
                _ => {}
            }
        }
        return Ok(());
    }

    let upto = query.with_split_index.unwrap_or(query.match_clauses.len());
    for mc in query.match_clauses.iter().take(upto) {
        pattern_vars(&mc.pattern, &mut bound);
    }
    if let Some(u) = &query.unwind_clause {
        bound.insert(u.variable.clone());
    }
    for u in &query.extra_unwind_clauses {
        bound.insert(u.variable.clone());
    }
    if let Some(w) = &query.where_clause {
        walk(&w.predicate, &bound)?;
    }
    // Post-WITH stages get the projection's scope plus their own matches.
    for (wc, uw, matches, wh) in &query.extra_with_stages {
        bound = projected_names(&wc.items);
        if let Some(u) = uw { bound.insert(u.variable.clone()); }
        for mc in matches { pattern_vars(&mc.pattern, &mut bound); }
        if let Some(w) = wh { walk(&w.predicate, &bound)?; }
    }
    if let Some(wc) = &query.with_clause {
        bound = projected_names(&wc.items);
        for mc in query.match_clauses.iter().skip(upto) {
            pattern_vars(&mc.pattern, &mut bound);
        }
        if let Some(w) = &query.post_with_where_clause {
            walk(&w.predicate, &bound)?;
        }
    }
    Ok(())
}

/// `size()` takes a list or a string, and nothing else (#843).
///
/// ```text
/// MATCH (a), (b), (c) RETURN size((a)-[:REL]->(b))
/// MATCH p = (a)-[*]->(b) RETURN size(p)
/// ```
///
/// Both must fail **at compile time**, and the reason the TCK insists on that
/// rather than accepting a runtime error is visible in these very scenarios:
/// they run against an empty graph, so `MATCH (a), (b), (c)` binds nothing, the
/// argument is never evaluated, and a runtime check never fires. The query
/// "succeeds" with zero rows.
///
/// The engine did raise a `TypeError` -- but only when the pattern matched
/// something. A probe against an empty store showed the error and a probe with
/// data showed it too; what neither showed is that the *scenario* never reaches
/// either, because the failure has to happen before any row exists.
///
/// `size()` on a path is a separate spelling error rather than a type error:
/// `length()` is the function for a path, and `size()` accepted one silently.
fn validate_size_arguments(query: &Query) -> Result<(), ValidationError> {
    // Every name bound as a path, from both AST representations.
    let mut paths: HashSet<String> = HashSet::new();
    let mut note = |pattern: &crate::query::ast::Pattern| {
        for path in &pattern.paths {
            if let Some(v) = &path.path_variable {
                paths.insert(v.clone());
            }
        }
    };
    for mc in &query.match_clauses {
        note(&mc.pattern);
    }
    for clause in &query.clauses {
        if let crate::query::ast::Clause::Match(mc) = clause {
            note(&mc.pattern);
        }
    }

    fn walk(e: &Expression, paths: &HashSet<String>) -> Result<(), ValidationError> {
        if let Expression::Function { name, args, .. } = e {
            if name.eq_ignore_ascii_case("size") {
                for arg in args {
                    match arg {
                        // A bare pattern. `EXISTS { ... }` desugars to the same
                        // node, so the flag is what separates them -- widening
                        // this to both would reject `size(...)` nowhere and
                        // `EXISTS` everywhere, which is how #798 went wrong.
                        Expression::ExistsSubquery { bare_pattern: true, .. } => {
                            return Err(ValidationError::SizeOfNonCollection("a pattern"));
                        }
                        Expression::Variable(v) if paths.contains(v) => {
                            return Err(ValidationError::SizeOfNonCollection("a path"));
                        }
                        _ => {}
                    }
                }
            }
        }
        for child in child_expressions(e) {
            walk(child, paths)?;
        }
        Ok(())
    }

    for e in all_expressions(query) {
        walk(e, &paths)?;
    }
    Ok(())
}

/// A bare pattern cannot be **projected** (#880).
///
/// ```text
/// MATCH (n) RETURN (n)-[]->()
/// MATCH (n) WITH (n)-[]->() AS x RETURN x
/// ```
///
/// Both must fail at compile time. A pattern in that position is a predicate
/// written where a value belongs, and the engine happily evaluated it as one
/// and projected the boolean -- an answer to a question nobody asked.
///
/// Only the **top level** of a projection item is checked, and only a *bare*
/// pattern. `EXISTS { ... }` desugars to the same AST node and is legal
/// anywhere (`bare_pattern` is what separates them, as #798 established); a
/// pattern comprehension is a different node; and a bare pattern nested inside
/// a list comprehension's own `WHERE` is a predicate in a predicate position.
/// Walking the whole tree would reject all three, and over-rejecting a valid
/// query is the worse failure.
fn validate_pattern_projections(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;

    fn check(items: &[crate::query::ast::ReturnItem], what: &'static str) -> Result<(), ValidationError> {
        for item in items {
            if matches!(item.expression, Expression::ExistsSubquery { bare_pattern: true, .. }) {
                return Err(ValidationError::PatternInProjection(what));
            }
        }
        Ok(())
    }

    if let Some(rc) = &query.return_clause {
        check(&rc.items, "RETURN")?;
    }
    if let Some(wc) = &query.with_clause {
        check(&wc.items, "WITH")?;
    }
    for (wc, _, _, _) in &query.extra_with_stages {
        check(&wc.items, "WITH")?;
    }
    for clause in &query.clauses {
        match clause {
            Clause::Return(rc) => check(&rc.items, "RETURN")?,
            Clause::With(wc) => check(&wc.items, "WITH")?,
            _ => {}
        }
    }
    Ok(())
}

/// `DELETE` takes a node, a relationship or a path (#887).
///
/// ```text
/// MATCH (n) DELETE n:Person     -- a boolean predicate
/// MATCH (a) DELETE x            -- nothing named x
/// MATCH () DELETE 1 + 1         -- a number
/// MATCH (n) DELETE n.prop       -- a property, not the node
/// ```
///
/// All four ran and deleted nothing, reporting success. Deleting nothing is a
/// legitimate outcome — `MATCH (n:Nope) DELETE n` deletes nothing too — so a
/// caller cannot tell "there was nothing to delete" from "I did not understand
/// what you asked me to delete".
///
/// Only the shapes that **cannot** be an entity are refused: a literal,
/// arithmetic, a property access, a label predicate (`n:Label` parses to
/// `hasLabels`), and a variable nothing binds. A function call is left alone,
/// because Cypher does allow an expression that resolves to an entity and
/// deciding that statically is a different job — over-rejecting a valid
/// `DELETE` is much worse than under-rejecting an invalid one.
fn validate_delete_targets(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;

    /// Names anything in the query binds, from either AST representation.
    fn bound_names(query: &Query) -> HashSet<String> {
        let mut out = HashSet::new();
        let mut note = |p: &crate::query::ast::Pattern| pattern_vars(p, &mut out);
        for mc in &query.match_clauses {
            note(&mc.pattern);
        }
        if let Some(cc) = &query.create_clause {
            note(&cc.pattern);
        }
        if let Some(mc) = &query.merge_clause {
            note(&mc.pattern);
        }
        if let Some(u) = &query.unwind_clause {
            out.insert(u.variable.clone());
        }
        for u in &query.extra_unwind_clauses {
            out.insert(u.variable.clone());
        }
        for u in &query.post_with_unwind_clauses {
            out.insert(u.variable.clone());
        }
        if let Some(wc) = &query.with_clause {
            out.extend(projected_names(&wc.items));
        }
        for (wc, uw, mcs, _) in &query.extra_with_stages {
            out.extend(projected_names(&wc.items));
            if let Some(u) = uw {
                out.insert(u.variable.clone());
            }
            for mc in mcs {
                pattern_vars(&mc.pattern, &mut out);
            }
        }
        for clause in &query.clauses {
            match clause {
                Clause::Match(mc) => pattern_vars(&mc.pattern, &mut out),
                Clause::Create(cc) => pattern_vars(&cc.pattern, &mut out),
                Clause::Merge(mc) => pattern_vars(&mc.pattern, &mut out),
                Clause::Unwind(u) => {
                    out.insert(u.variable.clone());
                }
                Clause::With(w) => out.extend(projected_names(&w.items)),
                _ => {}
            }
        }
        out
    }

    let mut targets: Vec<&Expression> = Vec::new();
    if let Some(dc) = &query.delete_clause {
        targets.extend(dc.expressions.iter());
    }
    for clause in &query.clauses {
        if let Clause::Delete(dc) = clause {
            targets.extend(dc.expressions.iter());
        }
    }
    if targets.is_empty() {
        return Ok(());
    }

    let bound = bound_names(query);
    for expr in targets {
        let complaint = match expr {
            Expression::Variable(v) if !bound.contains(v) => {
                Some(format!("nothing in this query binds `{v}`"))
            }
            Expression::Variable(_) => None,
            Expression::Literal(_) => Some("a literal is not one".to_string()),
            Expression::Binary { .. } | Expression::Unary { .. } => {
                Some("an arithmetic or boolean expression is not one".to_string())
            }
            // **Not** a property access. `WITH {key: u} AS nodes DELETE nodes.key`
            // is valid Cypher -- a map field can hold an entity -- and
            // rejecting it cost two scenarios that had been passing. Whether a
            // property access yields an entity is a runtime question, and
            // guessing it statically is the over-rejection this check is
            // scoped to avoid.
            // `n:Label` parses to a `hasLabels` call, and is a *predicate*.
            Expression::Function { name, .. } if name.eq_ignore_ascii_case("hasLabels") => {
                Some("`n:Label` is a label test; use REMOVE to drop a label".to_string())
            }
            _ => None,
        };
        if let Some(what) = complaint {
            return Err(ValidationError::InvalidDeleteTarget(what));
        }
    }
    Ok(())
}

/// A name used in an expression must be bound by something in the query.
///
/// ```text
/// MATCH () RETURN foo                          -> SyntaxError: UndefinedVariable
/// MATCH (s) WHERE s.name = undefinedVariable   -> SyntaxError: UndefinedVariable
/// MERGE (n) ON CREATE SET x.num = 1            -> SyntaxError: UndefinedVariable
/// ```
///
/// Every one of these ran and returned zero rows, or set a property on
/// nothing, and reported success. A typo in a variable name is the single most
/// ordinary mistake there is, and the engine answered it with silence.
///
/// **Deliberately coarse**: the question asked is "does *anything* in this
/// query bind this name", not "is it in scope *here*". Real scope analysis
/// would also catch a name dropped by an intervening `WITH`, but it has to be
/// exactly right about `WITH`, `FOREACH`, comprehension binders, `CALL …
/// YIELD` and both `Query` representations before it can be trusted -- and
/// `validate.rs` opens by naming over-rejection as the worse failure. A false
/// `SyntaxError` breaks a working query; a missed one leaves today's
/// behaviour. `validate_order_by_in_scope` already covers the narrowing case
/// where it is cheap to be certain.
///
/// Anything that binds anywhere counts as binding everywhere here, which is
/// why comprehension variables and `FOREACH` loop variables are collected up
/// front rather than tracked positionally.
fn validate_variables_are_bound(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::{Clause, RemoveItem, SetClause};

    let mut bound: HashSet<String> = HashSet::new();

    /// Like `pattern_variables`, plus the path variable.
    ///
    /// `pattern_variables` exists to answer "what may a later CREATE not
    /// redeclare", and a path name is not that. Here it is: `MATCH p = (a)-->(b)
    /// RETURN p` binds `p`, and borrowing the other collector rejected every
    /// named-path query in the suite.
    fn pattern_and_path_variables(
        pattern: &crate::query::ast::Pattern,
        out: &mut HashSet<String>,
    ) {
        pattern_variables(pattern, out);
        for path in &pattern.paths {
            if let Some(v) = &path.path_variable {
                out.insert(v.clone());
            }
        }
    }

    /// Names any expression *introduces*, at any depth.
    fn binders(e: &Expression, out: &mut HashSet<String>) {
        match e {
            Expression::ListComprehension { variable, .. }
            | Expression::PredicateFunction { variable, .. } => {
                out.insert(variable.clone());
            }
            Expression::Reduce { accumulator, variable, .. } => {
                out.insert(accumulator.clone());
                out.insert(variable.clone());
            }
            Expression::PatternComprehension { pattern, .. } => pattern_variables(pattern, out),
            Expression::ExistsSubquery { pattern, .. } => pattern_variables(pattern, out),
            _ => {}
        }
        for child in child_expressions(e) {
            binders(child, out);
        }
    }

    fn note_set(sc: &SetClause, bound: &mut HashSet<String>) {
        for item in &sc.items {
            binders(&item.value, bound);
        }
        for item in &sc.entity_items {
            binders(&item.value, bound);
        }
    }

    fn note_clause_binders(query: &Query, bound: &mut HashSet<String>) {
        for mc in &query.match_clauses {
            pattern_and_path_variables(&mc.pattern, bound);
        }
        if let Some(c) = &query.create_clause {
            pattern_and_path_variables(&c.pattern, bound);
        }
        if let Some(m) = &query.merge_clause {
            pattern_and_path_variables(&m.pattern, bound);
        }
        for u in query
            .unwind_clause
            .iter()
            .chain(query.extra_unwind_clauses.iter())
            .chain(query.post_with_unwind_clauses.iter())
        {
            bound.insert(u.variable.clone());
            binders(&u.expression, bound);
        }
        if let Some(f) = &query.foreach_clause {
            bound.insert(f.variable.clone());
            for c in &f.create_clauses {
                pattern_and_path_variables(&c.pattern, bound);
            }
            for sc in &f.set_clauses {
                note_set(sc, bound);
            }
        }
        if let Some(call) = &query.call_clause {
            for y in &call.yield_items {
                bound.insert(y.alias.clone().unwrap_or_else(|| y.name.clone()));
            }
        }
        for c in &query.clauses {
            match c {
                Clause::Match(mc) => pattern_and_path_variables(&mc.pattern, bound),
                Clause::Create(cc) => pattern_and_path_variables(&cc.pattern, bound),
                Clause::Merge(mc) => pattern_and_path_variables(&mc.pattern, bound),
                Clause::Unwind(u) => {
                    bound.insert(u.variable.clone());
                    binders(&u.expression, bound);
                }
                Clause::Foreach(f) => {
                    bound.insert(f.variable.clone());
                    for cc in &f.create_clauses {
                        pattern_and_path_variables(&cc.pattern, bound);
                    }
                    for sc in &f.set_clauses {
                        note_set(sc, bound);
                    }
                }
                Clause::Call(call) => {
                    for y in &call.yield_items {
                        bound.insert(y.alias.clone().unwrap_or_else(|| y.name.clone()));
                    }
                }
                Clause::Set(sc) => note_set(sc, bound),
                _ => {}
            }
        }
        for sc in &query.set_clauses {
            note_set(sc, bound);
        }
    }

    note_clause_binders(query, &mut bound);

    // A `CALL { … }` subquery exports the columns its RETURN names, and binds
    // everything it binds internally. Recursing rather than duplicating the
    // walk: a subquery is a `Query`.
    if let Some(sub) = &query.call_subquery {
        note_clause_binders(sub, &mut bound);
        for items in sub
            .return_clause
            .iter()
            .map(|r| &r.items)
            .chain(sub.with_clause.iter().map(|w| &w.items))
        {
            for item in items {
                if let Some(a) = &item.alias {
                    bound.insert(a.clone());
                } else if let Expression::Variable(v) = &item.expression {
                    bound.insert(v.clone());
                }
                binders(&item.expression, &mut bound);
            }
        }
        for c in &sub.clauses {
            let items = match c {
                Clause::Return(r) => &r.items,
                Clause::With(w) => &w.items,
                _ => continue,
            };
            for item in items {
                if let Some(a) = &item.alias {
                    bound.insert(a.clone());
                } else if let Expression::Variable(v) = &item.expression {
                    bound.insert(v.clone());
                }
                binders(&item.expression, &mut bound);
            }
        }
    }

    // Projections bind their aliases -- and a bare `RETURN n` keeps `n`.
    let mut note_items = |items: &[ReturnItem], bound: &mut HashSet<String>| {
        for item in items {
            if let Some(a) = &item.alias {
                bound.insert(a.clone());
            }
            binders(&item.expression, bound);
        }
    };
    if let Some(w) = &query.with_clause {
        note_items(&w.items, &mut bound);
    }
    for (w, u, post_matches, post_where) in &query.extra_with_stages {
        note_items(&w.items, &mut bound);
        if let Some(u) = u {
            bound.insert(u.variable.clone());
            binders(&u.expression, &mut bound);
        }
        // A MATCH written after a WITH binds too. Skipping these rejected
        // `MATCH (m) WITH m MATCH (a)-->(m) WITH m, count(a) AS cnt RETURN cnt`
        // -- an ordinary two-stage aggregation, and one the TCK does not
        // cover but the engine's own suite does.
        for mc in post_matches {
            pattern_and_path_variables(&mc.pattern, &mut bound);
        }
        if let Some(w) = post_where {
            binders(&w.predicate, &mut bound);
        }
    }
    if let Some(r) = &query.return_clause {
        note_items(&r.items, &mut bound);
    }
    for c in &query.clauses {
        match c {
            Clause::With(w) => note_items(&w.items, &mut bound),
            Clause::Return(r) => note_items(&r.items, &mut bound),
            _ => {}
        }
    }
    // Binders inside predicates and inline property expressions count too.
    for e in all_expressions(query) {
        binders(e, &mut bound);
    }

    // ---- everything referenced, checked against that set.
    let mut used: Vec<String> = Vec::new();
    let mut note_expr = |e: &Expression, used: &mut Vec<String>| collect_all_vars(e, used);

    for e in all_expressions(query) {
        note_expr(e, &mut used);
    }
    let mut note_set_use = |sc: &SetClause, used: &mut Vec<String>| {
        for item in &sc.items {
            used.push(item.variable.clone());
            collect_all_vars(&item.value, used);
        }
        for item in &sc.entity_items {
            used.push(item.variable.clone());
            collect_all_vars(&item.value, used);
        }
        for item in &sc.label_items {
            used.push(item.variable.clone());
        }
    };
    for sc in &query.set_clauses {
        note_set_use(sc, &mut used);
    }
    for rc in &query.remove_clauses {
        for item in &rc.items {
            match item {
                RemoveItem::Property { variable, .. } => used.push(variable.clone()),
                RemoveItem::Label { variable, .. } => used.push(variable.clone()),
            }
        }
    }
    let mut note_merge_use = |mc: &crate::query::ast::MergeClause, used: &mut Vec<String>| {
        for item in mc.on_create_set.iter().chain(mc.on_match_set.iter()) {
            used.push(item.variable.clone());
            collect_all_vars(&item.value, used);
        }
        for item in mc
            .on_create_entity_set
            .iter()
            .chain(mc.on_match_entity_set.iter())
        {
            used.push(item.variable.clone());
            collect_all_vars(&item.value, used);
        }
    };
    if let Some(mc) = &query.merge_clause {
        note_merge_use(mc, &mut used);
    }
    for c in &query.clauses {
        match c {
            Clause::Set(sc) => note_set_use(sc, &mut used),
            Clause::Merge(mc) => note_merge_use(mc, &mut used),
            Clause::Delete(dc) => {
                for e in &dc.expressions {
                    collect_all_vars(e, &mut used);
                }
            }
            _ => {}
        }
    }
    if let Some(dc) = &query.delete_clause {
        for e in &dc.expressions {
            collect_all_vars(e, &mut used);
        }
    }
    // Inline property expressions in write patterns: `CREATE (b {name: missing})`.
    for pattern in pattern_property_expressions(query) {
        collect_all_vars(pattern, &mut used);
    }

    for name in used {
        if !bound.contains(&name) {
            return Err(ValidationError::UnboundVariable(name));
        }
    }
    Ok(())
}

/// Every expression written inside a pattern's inline property map.
///
/// `CREATE (b {name: missing})` hides a reference to `missing` where no
/// expression walker looks: it is a property map on a pattern node, not an
/// expression of the clause.
fn pattern_property_expressions(query: &Query) -> Vec<&Expression> {
    use crate::query::ast::Clause;
    let mut out: Vec<&Expression> = Vec::new();

    fn from_pattern<'a>(pattern: &'a crate::query::ast::Pattern, out: &mut Vec<&'a Expression>) {
        for path in &pattern.paths {
            let mut nodes = vec![&path.start];
            for seg in &path.segments {
                nodes.push(&seg.node);
                if let Some(pe) = &seg.edge.property_exprs {
                    out.extend(pe.values());
                }
            }
            for np in nodes {
                if let Some(pe) = &np.property_exprs {
                    out.extend(pe.values());
                }
            }
        }
    }

    for mc in &query.match_clauses {
        from_pattern(&mc.pattern, &mut out);
    }
    if let Some(c) = &query.create_clause {
        from_pattern(&c.pattern, &mut out);
    }
    if let Some(m) = &query.merge_clause {
        from_pattern(&m.pattern, &mut out);
    }
    for c in &query.clauses {
        match c {
            Clause::Match(mc) => from_pattern(&mc.pattern, &mut out),
            Clause::Create(cc) => from_pattern(&cc.pattern, &mut out),
            Clause::Merge(mc) => from_pattern(&mc.pattern, &mut out),
            _ => {}
        }
    }
    out
}

/// A `WITH` item that is not a bare variable must be aliased (#897).
///
/// ```text
/// MATCH (a) WITH a, count(*) RETURN a     -> SyntaxError: NoExpressionAlias
/// ```
///
/// `WITH` re-scopes: what it projects is all that exists downstream, and a
/// column nobody can name is a column nobody can use. `RETURN` is different --
/// it is the end of the query, and `RETURN count(*)` names its column after
/// the text the user wrote.
fn validate_with_items_aliased(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;

    fn check(items: &[ReturnItem]) -> Result<(), ValidationError> {
        for item in items {
            if item.alias.is_none() && !matches!(item.expression, Expression::Variable(_)) {
                return Err(ValidationError::UnaliasedWithItem);
            }
        }
        Ok(())
    }

    if let Some(w) = &query.with_clause {
        check(&w.items)?;
    }
    for (w, ..) in &query.extra_with_stages {
        check(&w.items)?;
    }
    for c in &query.clauses {
        if let Clause::With(w) = c {
            check(&w.items)?;
        }
    }
    Ok(())
}

/// A function call must name a function this engine implements (#947).
///
/// ```text
/// MATCH (a) RETURN foo(a)   -> UnknownFunction
/// ```
///
/// It used to **succeed with zero rows**. A misspelled `lenght(x)` or
/// `toLowerCase(s)` produced an empty result set from a query that reported
/// success, and the reader concluded something about their data. Empty is also
/// the answer that survives review: a wrong number gets questioned, an empty
/// result looks like a legitimately empty match.
///
/// At compile time rather than at run time, because the run-time error only
/// fires on a row that reaches the expression — over an empty graph the call
/// never ran and the query "succeeded". A compile-time check does not depend
/// on the data.
///
/// Aggregates are dispatched by the planner rather than by `eval_function`, so
/// they are checked against `AGGREGATE_NAMES` instead. `all`/`any`/`none`/
/// `single`, `reduce` and the comprehensions are their own AST nodes and never
/// reach here at all.
fn validate_function_names(query: &Query) -> Result<(), ValidationError> {
    fn check(e: &Expression) -> Result<(), ValidationError> {
        if let Expression::Function { name, .. } = e {
            let lower = name.to_lowercase();
            let known = crate::query::executor::operator::is_known_function(&lower)
                || AGGREGATE_NAMES.contains(&lower.as_str())
                // The parser lowers postfix `:A:B` and a few other forms into
                // synthetic calls that are not user-writable names.
                || lower.starts_with("__");
            if !known {
                return Err(ValidationError::UnknownFunction(name.clone()));
            }
        }
        for child in child_expressions(e) {
            check(child)?;
        }
        Ok(())
    }

    for e in all_expressions(query) {
        check(e)?;
    }
    Ok(())
}

/// A projection that aggregates may only combine the aggregate with a
/// **grouping key** (#930).
///
/// ```text
/// WITH me.age + count(you.age) AS agg                 -> AmbiguousAggregationExpression
/// RETURN me.age + you.age, me.age + you.age + count(*) -> AmbiguousAggregationExpression
/// ```
///
/// Cypher forms its groups from the projection's *non-aggregating* items, so
/// in `me.age + count(you.age)` there is no group `me.age` could be evaluated
/// over -- the rows have already been folded. It is not that the answer is
/// hard to define; it is that two readings (the first row's `me.age`, or one
/// group per distinct `me.age`) are equally defensible, which is what
/// "ambiguous" means here.
///
/// We answered these with **zero rows and no error**, which is the worst of
/// the three options: the query looks like it ran and found nothing.
///
/// The check is deliberately narrow. A whole projected item is a grouping key,
/// so `RETURN n, n.age + count(*)` is left alone: `n` is projected, and
/// rejecting it would fail queries Cypher accepts. Only a variable that is
/// referenced inside an aggregating expression and is *not* projected on its
/// own is reported -- a wider rule cost 126 valid scenarios the last time this
/// file guessed.
fn validate_aggregation_is_unambiguous(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;

    /// Variables referenced by the parts of `e` that do **not** aggregate.
    fn free_variables_outside_aggregates(e: &Expression, out: &mut HashSet<String>) {
        if is_aggregate_call(e) {
            // Inside an aggregate is exactly where a non-grouping variable is
            // allowed: `count(you.age)` is the point of the query.
            return;
        }
        match e {
            Expression::Variable(v) => {
                out.insert(v.clone());
            }
            // `n.age` carries its variable as a `String` field, not as a child
            // expression, so a walk over `child_expressions` alone never sees
            // it -- and the whole rule silently matched nothing, because every
            // scenario it targets is written with property access.
            Expression::Property { variable, .. } => {
                out.insert(variable.clone());
            }
            _ => {}
        }
        for child in child_expressions(e) {
            free_variables_outside_aggregates(child, out);
        }
    }

    fn check(items: &[ReturnItem]) -> Result<(), ValidationError> {
        if !items.iter().any(|i| contains_aggregate(&i.expression)) {
            return Ok(());
        }
        // A whole item that does not aggregate is a grouping key, and so is
        // the variable it is, when it is just a variable.
        let mut keys: HashSet<String> = HashSet::new();
        for item in items {
            if contains_aggregate(&item.expression) {
                continue;
            }
            match &item.expression {
                Expression::Variable(v) => {
                    keys.insert(v.clone());
                }
                // `WITH n.age, count(*)` groups by `n.age`, and referring to
                // `n.age` inside the aggregating item is then unambiguous.
                // Recorded under the variable rather than the property because
                // grouping by `n.age` fixes `n.age` and nothing else about `n`
                // -- and this rule only reports variables.
                Expression::Property { variable, .. } => {
                    keys.insert(variable.clone());
                }
                _ => {}
            }
        }
        for item in items {
            if !contains_aggregate(&item.expression) || is_aggregate_call(&item.expression) {
                continue;
            }
            let mut used = HashSet::new();
            free_variables_outside_aggregates(&item.expression, &mut used);
            let mut ungrouped: Vec<&String> =
                used.iter().filter(|v| !keys.contains(*v)).collect();
            if let Some(v) = ungrouped.pop() {
                // Sorted so the message is the same on every run; a HashSet
                // would name a different variable each time and make the
                // failure look intermittent.
                ungrouped.push(v);
                ungrouped.sort();
                return Err(ValidationError::AmbiguousGroupingExpression(format!(
                    "`{}`",
                    ungrouped[0]
                )));
            }
        }
        Ok(())
    }

    if let Some(r) = &query.return_clause {
        check(&r.items)?;
    }
    if let Some(w) = &query.with_clause {
        check(&w.items)?;
    }
    for (w, ..) in &query.extra_with_stages {
        check(&w.items)?;
    }
    for c in &query.clauses {
        match c {
            Clause::With(w) => check(&w.items)?,
            Clause::Return(r) => check(&r.items)?,
            _ => {}
        }
    }
    Ok(())
}

/// Aggregation is not allowed in a `WHERE`, nor inside a comprehension (#897).
///
/// ```text
/// MATCH (a) WHERE count(a) > 10 RETURN a         -> SyntaxError
/// MATCH (n) RETURN [x IN [1, 2] | count(*)]      -> SyntaxError
/// ```
///
/// An aggregate is computed over a group of rows, and a `WHERE` runs on one
/// row at a time -- the filter would have to consume the rows it is filtering.
/// The `HAVING` shape Cypher does have is `WITH … count(*) AS c … WHERE c > 1`,
/// which filters on the *alias*, not on the aggregate, and is untouched here.
///
/// A comprehension is the same argument one level down: its body runs per list
/// element, and `count(*)` over a list element has no group to count.
fn validate_aggregate_placement(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;

    fn inside_comprehension(e: &Expression) -> Result<(), ValidationError> {
        let body: Vec<&Expression> = match e {
            Expression::ListComprehension { map_expr, filter, .. } => {
                let mut v: Vec<&Expression> = vec![map_expr.as_ref()];
                v.extend(filter.iter().map(|b| b.as_ref()));
                v
            }
            Expression::PatternComprehension { projection, filter, .. } => {
                let mut v: Vec<&Expression> = vec![projection.as_ref()];
                v.extend(filter.iter().map(|b| b.as_ref()));
                v
            }
            _ => Vec::new(),
        };
        for b in body {
            if contains_aggregate(b) {
                return Err(ValidationError::AggregateNotAllowed(
                    "inside a comprehension: its body runs once per element, \
                     which is not a group",
                ));
            }
        }
        for child in child_expressions(e) {
            inside_comprehension(child)?;
        }
        Ok(())
    }

    for e in all_expressions(query) {
        inside_comprehension(e)?;
    }

    let mut where_predicates: Vec<&Expression> = Vec::new();
    if let Some(w) = &query.where_clause {
        where_predicates.push(&w.predicate);
    }
    for (_, _, _, post_where) in &query.extra_with_stages {
        if let Some(w) = post_where {
            where_predicates.push(&w.predicate);
        }
    }
    for c in &query.clauses {
        if let Clause::Where(w) = c {
            where_predicates.push(&w.predicate);
        }
    }
    for p in where_predicates {
        if contains_aggregate(p) {
            return Err(ValidationError::AggregateNotAllowed(
                "in WHERE: it filters one row at a time, and an aggregate needs \
                 the group. Aggregate in a WITH and filter on the alias",
            ));
        }
    }
    Ok(())
}

/// A function applied to the wrong kind of entity (#901).
///
/// ```text
/// MATCH (r) RETURN type(r)     -> SyntaxError: InvalidArgumentType
/// MATCH (n) RETURN length(n)   -> SyntaxError: InvalidArgumentType
/// ```
///
/// We returned a null column. `type()` asks a relationship for its type and a
/// node does not have one; `length()` asks a path how long it is. The TCK wants
/// these **at compile time**, which is only possible because the pattern says
/// what kind each variable is -- the same `EntityKind` map that
/// `validate_variable_kinds` builds.
///
/// Only a variable whose kind the pattern fixes is checked. An expression, a
/// parameter, or a name a `WITH` recomputed has no kind here and is left alone:
/// rejecting a valid query is the worse failure, and `carry_kinds_through_with`
/// already refuses to guess for exactly that reason.
fn validate_function_argument_kinds(query: &Query) -> Result<(), ValidationError> {
    use crate::query::ast::Clause;

    /// Functions whose single argument must be one kind of entity.
    const FIXED: &[(&str, EntityKind)] = &[
        ("type", EntityKind::Relationship),
        ("startnode", EntityKind::Relationship),
        ("endnode", EntityKind::Relationship),
        ("labels", EntityKind::Node),
        ("length", EntityKind::Path),
        ("nodes", EntityKind::Path),
        ("relationships", EntityKind::Path),
    ];

    fn walk(
        e: &Expression,
        kinds: &std::collections::HashMap<String, EntityKind>,
    ) -> Result<(), ValidationError> {
        if let Expression::Function { name, args, .. } = e {
            let lowered = name.to_lowercase();
            if let Some((func, wanted)) = FIXED.iter().find(|(n, _)| *n == lowered) {
                if let Some(Expression::Variable(v) | Expression::PathVariable(v)) = args.first() {
                    if let Some(got) = kinds.get(v) {
                        if got != wanted {
                            return Err(ValidationError::FunctionArgumentKind(
                                func,
                                wanted.noun(),
                                got.noun(),
                            ));
                        }
                    }
                }
            }
        }
        for child in child_expressions(e) {
            walk(child, kinds)?;
        }
        Ok(())
    }

    // The kinds every pattern in the query fixes. Scope narrowing is not
    // modelled here: a name a WITH drops is simply absent from a later
    // pattern's map too, and a name it carries keeps its kind. The coarse
    // version cannot produce a *wrong* kind, only miss one.
    let mut kinds: std::collections::HashMap<String, EntityKind> =
        std::collections::HashMap::new();
    let mut note = |pattern: &crate::query::ast::Pattern,
                    kinds: &mut std::collections::HashMap<String, EntityKind>| {
        // Kind *clashes* are `validate_variable_kinds`'s job; ignored here so
        // one rule reports them.
        let _ = note_pattern_kinds(kinds, pattern);
    };
    for mc in &query.match_clauses {
        note(&mc.pattern, &mut kinds);
    }
    if let Some(cc) = &query.create_clause {
        note(&cc.pattern, &mut kinds);
    }
    if let Some(mc) = &query.merge_clause {
        note(&mc.pattern, &mut kinds);
    }
    for (_, _, matches, _) in &query.extra_with_stages {
        for mc in matches {
            note(&mc.pattern, &mut kinds);
        }
    }
    for c in &query.clauses {
        match c {
            Clause::Match(mc) => note(&mc.pattern, &mut kinds),
            Clause::Create(cc) => note(&cc.pattern, &mut kinds),
            Clause::Merge(mc) => note(&mc.pattern, &mut kinds),
            _ => {}
        }
    }

    for e in all_expressions(query) {
        walk(e, &kinds)?;
    }
    Ok(())
}

pub fn validate(query: &Query) -> Result<(), ValidationError> {
    validate_variables_are_bound(query)?;

    validate_with_items_aliased(query)?;

    validate_aggregate_placement(query)?;
    validate_aggregation_is_unambiguous(query)?;
    validate_function_names(query)?;

    validate_function_argument_kinds(query)?;

    validate_delete_targets(query)?;

    validate_pattern_projections(query)?;

    validate_size_arguments(query)?;

    validate_pattern_predicate_vars(query)?;

    validate_property_access_targets(query)?;

    validate_boolean_operands(query)?;

    validate_order_by_in_scope(query)?;

    validate_variable_kinds(query)?;

    // ---- ORDER BY scope after an aggregating or DISTINCT projection.
    if let Some(rc) = &query.return_clause {
        validate_order_by_scope(&rc.items, rc.distinct, query.order_by.as_ref())?;
    }
    if let Some(wc) = &query.with_clause {
        validate_order_by_scope(&wc.items, wc.distinct, wc.order_by.as_ref())?;
    }

    // ---- Duplicate result columns.
    //
    // `RETURN a AS x, b AS x` cannot be answered: one of the two columns has
    // to win, and whichever it is, the caller silently loses data.
    let cols = columns(query);
    let mut seen: HashSet<&str> = HashSet::new();
    for c in &cols {
        if !seen.insert(c.as_str()) {
            return Err(ValidationError::DuplicateColumn(c.clone()));
        }
    }
    if let Some(wc) = &query.with_clause {
        let mut seen: HashSet<String> = HashSet::new();
        for item in &wc.items {
            if let Some(name) = column_name(item) {
                if !seen.insert(name.clone()) {
                    return Err(ValidationError::DuplicateColumn(name));
                }
            }
        }
    }

    // ---- UNION: same columns on both sides, and one flavour throughout.
    if !query.union_queries.is_empty() {
        let flavours: HashSet<bool> = query.union_queries.iter().map(|(_, all)| *all).collect();
        if flavours.len() > 1 {
            return Err(ValidationError::MixedUnionAndUnionAll);
        }
        for (sub, _) in &query.union_queries {
            let right = columns(sub);
            // An empty column list means the sub-query had no RETURN this
            // check can read; leaving it alone keeps the rule to what it can
            // actually see.
            if !cols.is_empty() && !right.is_empty() && cols != right {
                return Err(ValidationError::UnionColumnMismatch {
                    left: cols.clone(),
                    right,
                });
            }
            validate(sub)?;
        }
    }

    // ---- Writes over a variable something already bound, and edges that do
    // not say what they are.
    //
    // `MATCH (a) CREATE (a:Foo)` looks like "add a label" and is not: Cypher
    // requires SET for that, and the CREATE form is an error. A *bare*
    // re-mention -- `MATCH (a), (b) CREATE (a)-[:R]->(b)` -- is how you write
    // an edge between matched nodes and stays legal. MERGE says the same for
    // its own patterns (TCK Merge5 [22]).
    //
    // A relationship being written has to say exactly what it is. These are
    // ambiguous rather than merely unsupported: `CREATE (a)-->(b)` does not
    // say what kind of edge to make, `CREATE (a)-[:R]-(b)` does not say which
    // way it points, and `CREATE (a)-[:R*2]->(b)` does not say what the
    // intermediate node is. Cypher rejects all three, and accepting them means
    // inventing an answer.
    //
    // Note this is only in CREATE and MERGE. The same patterns are perfectly
    // good in MATCH, where they mean "any type", "either direction" and "two
    // hops" -- which is why the check lives here and not in the grammar. The
    // one place the two kinds differ is direction: `MERGE (a)-[:R]-(b)` is
    // legal and creates an outgoing edge, so undirected is refused for CREATE
    // only.
    for (kind, pattern, bound) in write_patterns(query) {
        let merge = kind == WriteKind::Merge;
        for path in &pattern.paths {
            for seg in &path.segments {
                if seg.edge.length.is_some() {
                    return Err(if merge {
                        ValidationError::MergeVariableLengthRelationship
                    } else {
                        ValidationError::CreateVariableLengthRelationship
                    });
                }
                if seg.edge.types.len() != 1 {
                    return Err(if merge {
                        ValidationError::MergeRelationshipWithoutType
                    } else {
                        ValidationError::CreateRelationshipWithoutType
                    });
                }
                if !merge && matches!(seg.edge.direction, crate::query::ast::Direction::Both) {
                    return Err(ValidationError::CreateUndirectedRelationship);
                }
                // A null property on a written edge cannot be matched back:
                // `MERGE (a)-[r:X {num: null}]->(b)` would create an edge the
                // same pattern does not find, so a second run creates another.
                if merge {
                    if let Some(props) = &seg.edge.properties {
                        let mut nulls: Vec<&String> = props
                            .iter()
                            .filter(|(_, v)| matches!(v, crate::graph::PropertyValue::Null))
                            .map(|(k, _)| k)
                            .collect();
                        nulls.sort();
                        if let Some(key) = nulls.first() {
                            return Err(ValidationError::MergeRelationshipWithNullProperty(
                                (*key).clone(),
                            ));
                        }
                    }
                }
                if let Some(v) = &seg.edge.variable {
                    if bound.contains(v) {
                        return Err(if merge {
                            ValidationError::MergeOnBoundVariable(v.clone())
                        } else {
                            ValidationError::CreateOnBoundRelationship(v.clone())
                        });
                    }
                }
            }
        }
        // Tracked *within* the clause as well as before it. `CREATE (n:Foo)-[:T1]->(),
        // (n:Bar)-[:T2]->()` binds `n` in its first path and re-labels it in its
        // second, and a bound set computed only before the clause cannot see that.
        let mut bound = bound;
        for path in &pattern.paths {
            // A bare re-mention is how an edge between existing nodes is
            // written -- `CREATE (a)-[:R]->(b)`, and the `CREATE (a), (b),
            // (a)-[:R]->(b)` idiom every TCK fixture uses. A *standalone* one
            // is not: `MATCH (a) CREATE (a)` re-creates a node that already
            // exists and wires nothing, which Cypher rejects (#663).
            if path.segments.is_empty() {
                if let Some(v) = &path.start.variable {
                    if bound.contains(v) {
                        return Err(if merge {
                            ValidationError::MergeOnBoundVariable(v.clone())
                        } else {
                            ValidationError::CreateOnBoundVariable(v.clone())
                        });
                    }
                }
            }
            {
                let bound = &bound;
                let mut check = |np: &crate::query::ast::NodePattern| -> Result<(), ValidationError> {
                    if let Some(v) = &np.variable {
                        let adds_something = !np.labels.is_empty()
                            || np.properties.as_ref().is_some_and(|p| !p.is_empty())
                            || np.property_exprs.as_ref().is_some_and(|p| !p.is_empty());
                        if bound.contains(v) && adds_something {
                            return Err(if merge {
                                ValidationError::MergeOnBoundVariable(v.clone())
                            } else {
                                ValidationError::CreateOnBoundVariable(v.clone())
                            });
                        }
                    }
                    Ok(())
                };
                check(&path.start)?;
                for seg in &path.segments {
                    check(&seg.node)?;
                }
            }
            // Only now do this path's own variables count as bound, so a later
            // path in the same clause sees them.
            if let Some(v) = &path.start.variable {
                bound.insert(v.clone());
            }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable {
                    bound.insert(v.clone());
                }
                if let Some(v) = &seg.edge.variable {
                    bound.insert(v.clone());
                }
            }
        }
    }

    // ---- A relationship pattern used as a *value* in SET.
    //
    // `SET n.prop = head(nodes(head((n)-[:REL]->()))).foo` is a compile-time
    // error in Cypher: a pattern is a predicate or a comprehension source, not
    // a thing you can store. This was unreachable until `f(x).prop` parsed
    // (#673), so the TCK scenario asserting the error passed for an unrelated
    // reason — making the parse work removed the accident, and the rule then
    // has to be real.
    //
    // A pattern desugars to `ExistsSubquery`, so the check is structural
    // rather than textual, and it looks *anywhere* in the value expression
    // because the offending pattern is usually nested inside function calls.
    {
        fn holds_pattern(e: &Expression) -> bool {
            match e {
                Expression::ExistsSubquery { .. } | Expression::PatternComprehension { .. } => true,
                Expression::Binary { left, right, .. } => holds_pattern(left) || holds_pattern(right),
                Expression::Unary { expr, .. } => holds_pattern(expr),
                Expression::Function { args, .. } => args.iter().any(holds_pattern),
                Expression::Index { expr, index } => holds_pattern(expr) || holds_pattern(index),
                Expression::ListExpr(items) => items.iter().any(holds_pattern),
                Expression::MapExpr(entries) => entries.iter().any(|(_, v)| holds_pattern(v)),
                _ => false,
            }
        }
        fn set_values_of(sc: &crate::query::ast::SetClause) -> Vec<&Expression> {
            let mut out: Vec<&Expression> = sc.items.iter().map(|i| &i.value).collect();
            out.extend(sc.entity_items.iter().map(|i| &i.value));
            out
        }
        let mut set_values: Vec<&Expression> = Vec::new();
        for sc in &query.set_clauses {
            set_values.extend(set_values_of(sc));
        }
        for clause in &query.clauses {
            if let crate::query::ast::Clause::Set(sc) = clause {
                set_values.extend(set_values_of(sc));
            }
        }
        if set_values.into_iter().any(holds_pattern) {
            return Err(ValidationError::PatternInSetValue);
        }
    }

    // ---- A pattern variable that is already bound to a collection.
    //
    // `WITH [n] AS users MATCH (users)-->(m)` is a compile-time error in
    // Cypher: `users` is a list, and a list is not a node. Until collection
    // literals could hold expressions this was unreachable — `[n]` did not
    // parse, so the query failed for an unrelated reason and the TCK scenario
    // asserting the error passed by accident. Making the literal work removed
    // the accident, which is the honest moment to implement the rule (#654).
    {
        use crate::query::ast::Clause;
        let mut collections: HashSet<String> = HashSet::new();
        for clause in &query.clauses {
            match clause {
                Clause::With(wc) => {
                    for item in &wc.items {
                        let Some(alias) = item.alias.as_ref() else { continue };
                        let is_collection = not_an_entity(&item.expression);
                        // Re-aliasing something else to the same name clears it.
                        if is_collection {
                            collections.insert(alias.clone());
                        } else {
                            collections.remove(alias);
                        }
                    }
                }
                Clause::Match(mc) => {
                    for path in &mc.pattern.paths {
                        let mut check = |v: &Option<String>| -> Result<(), ValidationError> {
                            if let Some(name) = v {
                                if collections.contains(name) {
                                    return Err(ValidationError::VariableTypeConflict(name.clone()));
                                }
                            }
                            Ok(())
                        };
                        // The *path* variable too: `WITH 123 AS p MATCH p = ()-[]-()`
                        // is the same conflict and was walked past, because
                        // this loop only visited the nodes and edges (#795).
                        check(&path.path_variable)?;
                        check(&path.start.variable)?;
                        for seg in &path.segments {
                            check(&seg.node.variable)?;
                            check(&seg.edge.variable)?;
                        }
                    }
                }
                _ => {}
            }
        }

        // The by-kind shape says the same thing differently: `with_clause`
        // plus the matches after `with_split_index`, then each extra WITH
        // stage with the matches that follow it. Checking only the pipeline
        // shape missed this query entirely, because `MATCH … WITH … MATCH …
        // RETURN` is a shape the established rules already accept.
        let mut collections: HashSet<String> = HashSet::new();
        let mut note = |wc: &crate::query::ast::WithClause,
                        collections: &mut HashSet<String>| {
            for item in &wc.items {
                let Some(alias) = item.alias.as_ref() else { continue };
                let is_collection = not_an_entity(&item.expression);
                if is_collection {
                    collections.insert(alias.clone());
                } else {
                    collections.remove(alias);
                }
            }
        };
        let check_patterns = |mcs: &[crate::query::ast::MatchClause],
                              collections: &HashSet<String>|
         -> Result<(), ValidationError> {
            for mc in mcs {
                for path in &mc.pattern.paths {
                    let mut check = |v: &Option<String>| -> Result<(), ValidationError> {
                        if let Some(name) = v {
                            if collections.contains(name) {
                                return Err(ValidationError::VariableTypeConflict(name.clone()));
                            }
                        }
                        Ok(())
                    };
                    check(&path.path_variable)?;
                    check(&path.start.variable)?;
                    for seg in &path.segments {
                        check(&seg.node.variable)?;
                        check(&seg.edge.variable)?;
                    }
                }
            }
            Ok(())
        };
        if let Some(wc) = &query.with_clause {
            note(wc, &mut collections);
            let from = query.with_split_index.unwrap_or(query.match_clauses.len());
            if from <= query.match_clauses.len() {
                check_patterns(&query.match_clauses[from..], &collections)?;
            }
        }
        for (wc, _, matches, _) in &query.extra_with_stages {
            note(wc, &mut collections);
            check_patterns(matches, &collections)?;
        }
    }

    Ok(())
}
