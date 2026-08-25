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
    /// One variable bound to two different kinds of entity.
    VariableKindConflict {
        name: String,
        first: &'static str,
        second: &'static str,
    },
    PatternInSetValue,
    /// An ORDER BY naming something the projection did not keep.
    OrderByUndefinedVariable(String),
    /// An ORDER BY item that mixes an aggregate with a grouping expression.
    AmbiguousAggregationExpression,
    /// An ORDER BY that aggregates when the projection does not.
    InvalidAggregation,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
            Self::NonBooleanOperand(what) => write!(
                f,
                "AND, OR and XOR need boolean operands, and this one is {what}. \
                 A value whose type is only known at run time is fine here; a \
                 literal of the wrong type is not."
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
                // A WITH re-projects rather than binds new pattern variables,
                // and its aliases are what survive it. Anything this does not
                // model can only *shrink* the bound set, which risks accepting
                // an invalid query rather than rejecting a valid one -- the
                // trade this module states up front.
                _ => {}
            }
        }
        return out;
    }

    for mc in &query.match_clauses {
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
fn contains_aggregate(expr: &Expression) -> bool {
    if is_aggregate_call(expr) {
        return true;
    }
    let mut found = false;
    walk_children(expr, &mut |e| {
        if is_aggregate_call(e) {
            found = true;
        }
    });
    found
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

pub fn validate(query: &Query) -> Result<(), ValidationError> {
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
                        let is_collection = matches!(
                            &item.expression,
                            Expression::ListExpr(_)
                                | Expression::MapExpr(_)
                                | Expression::Literal(crate::graph::PropertyValue::Array(_))
                                | Expression::Literal(crate::graph::PropertyValue::Map(_))
                        );
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
                let is_collection = matches!(
                    &item.expression,
                    Expression::ListExpr(_)
                        | Expression::MapExpr(_)
                        | Expression::Literal(crate::graph::PropertyValue::Array(_))
                        | Expression::Literal(crate::graph::PropertyValue::Map(_))
                );
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
