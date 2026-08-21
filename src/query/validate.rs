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

pub fn validate(query: &Query) -> Result<(), ValidationError> {
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
