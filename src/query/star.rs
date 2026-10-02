//! Expansion of `RETURN *` and `WITH *` (TCK: `Return*`, `With*`, `Match*`).
//!
//! `*` means "every variable currently in scope". Scope is decidable from the
//! AST alone — a variable enters scope when a pattern, `UNWIND` or `CALL …
//! YIELD` binds it, and a `WITH` **replaces** scope with exactly the items it
//! projects. So the star is expanded here, immediately after parsing, and no
//! part of the planner or executor ever sees one.
//!
//! Doing it as a separate pass rather than inside the planner matters because
//! the planner has a dozen branches that each build their own projection list;
//! teaching every one of them about `*` would mean twelve chances to get scope
//! wrong, and the branch nobody exercised would be the one that silently
//! dropped a column.
//!
//! Ordering is insertion order — the order the variables were bound — which is
//! what Neo4j and Memgraph produce and what the TCK's ordered scenarios
//! expect. Deduplicated, because `MATCH (a)-->(b), (b)-->(c)` binds `b` twice.

use crate::query::ast::{
    Clause, Expression, MatchClause, Pattern, Query, ReturnItem, UnwindClause, WithClause,
    STAR_ITEM,
};

/// Whether an item is the `*` sentinel.
fn is_star(item: &ReturnItem) -> bool {
    matches!(&item.expression, Expression::Variable(v) if v == STAR_ITEM) && item.alias.is_none()
}

/// Whether a list of items contains one.
fn has_star(items: &[ReturnItem]) -> bool {
    items.iter().any(is_star)
}

/// Push `name` if it is not already present. Scope is a set, but an ordered
/// one.
fn push_unique(scope: &mut Vec<String>, name: &str) {
    if !scope.iter().any(|s| s == name) {
        scope.push(name.to_string());
    }
}

/// Every variable a MATCH pattern binds, in the order it is written.
///
/// Edge variables count: `MATCH (a)-[r]->(b) RETURN *` returns `r` too. Missing
/// them is the easy mistake here, and it is invisible until a scenario returns
/// two columns instead of three.
pub(crate) fn bind_match(scope: &mut Vec<String>, clauses: &[MatchClause]) {
    for mc in clauses {
        for path in &mc.pattern.paths {
            if let Some(v) = &path.path_variable {
                push_unique(scope, v);
            }
            if let Some(v) = &path.start.variable {
                push_unique(scope, v);
            }
            for seg in &path.segments {
                if let Some(v) = &seg.edge.variable {
                    push_unique(scope, v);
                }
                if let Some(v) = &seg.node.variable {
                    push_unique(scope, v);
                }
            }
        }
    }
}

fn bind_unwind(scope: &mut Vec<String>, unwind: Option<&UnwindClause>) {
    if let Some(u) = unwind {
        push_unique(scope, &u.variable);
    }
}

/// The names a WITH projects — its aliases, or the expression itself when it
/// is a bare variable. Anything else without an alias cannot be referred to
/// later, so it does not enter scope.
fn with_output(items: &[ReturnItem]) -> Vec<String> {
    let mut out = Vec::new();
    for item in items {
        if let Some(alias) = &item.alias {
            push_unique(&mut out, alias);
        } else if let Expression::Variable(v) = &item.expression {
            push_unique(&mut out, v);
        }
    }
    out
}

/// Replace the `*` item in `items` with one item per name in `scope`,
/// preserving any other items written alongside it.
/// Returns true when a star was expanded and produced no columns at all.
fn expand_into(items: &mut Vec<ReturnItem>, scope: &[String]) -> bool {
    if !has_star(items) {
        return false;
    }

    // Names the query projects explicitly, wherever they appear relative to
    // the star. `RETURN *, n` must not yield two `n` columns, and checking
    // only the items already emitted misses that case entirely — the star
    // comes first, so at that point nothing has been emitted yet. Collected up
    // front instead.
    let explicit: Vec<String> = items
        .iter()
        .filter(|i| !is_star(i))
        .filter_map(|i| match (&i.alias, &i.expression) {
            (Some(alias), _) => Some(alias.clone()),
            (None, Expression::Variable(v)) => Some(v.clone()),
            _ => None,
        })
        .collect();

    let mut out: Vec<ReturnItem> = Vec::with_capacity(items.len() + scope.len());
    for item in items.drain(..) {
        if is_star(&item) {
            for name in scope {
                if explicit.iter().any(|e| e == name) {
                    continue;
                }
                out.push(ReturnItem {
                    expression: Expression::Variable(name.clone()),
                    alias: None,
                    // `RETURN *` names each column after the variable it
                    // expands to, which `column_name` derives from the
                    // expression.
                    source_text: None,
                });
            }
        } else {
            out.push(item);
        }
    }
    let empty = out.is_empty();
    *items = out;
    empty
}

/// Every variable a pattern binds, in written order.
///
/// `CREATE` and `MERGE` bind exactly like `MATCH` does, so this is the one
/// implementation all three use.
fn bind_pattern(scope: &mut Vec<String>, pattern: &Pattern) {
    for path in &pattern.paths {
        if let Some(v) = &path.path_variable {
            push_unique(scope, v);
        }
        if let Some(v) = &path.start.variable {
            push_unique(scope, v);
        }
        for seg in &path.segments {
            if let Some(v) = &seg.edge.variable {
                push_unique(scope, v);
            }
            if let Some(v) = &seg.node.variable {
                push_unique(scope, v);
            }
        }
    }
}

/// Expand the stars in a clause-pipeline query.
///
/// `Query` has two shapes -- the by-kind fields and this pipeline -- and this
/// pass only ever walked the first. In the pipeline a `WITH *` kept a literal
/// variable named `*`, so it projected nothing, every binding was dropped, and
/// a later clause naming one of them treated it as new:
///
/// ```cypher
/// CREATE (a) WITH * CREATE (b) CREATE (a)<-[:T]-(b)   -- created three nodes
/// ```
///
/// The star is the reason to walk in order: scope is what has been bound so
/// far, and a `WITH` replaces it (#892).
fn expand_stars_pipeline(clauses: &mut [Clause], outer: &[String], hidden: &[String]) -> bool {
    let mut scope: Vec<String> = outer.to_vec();
    let mut empty_star = false;
    for clause in clauses.iter_mut() {
        match clause {
            Clause::Match(mc) => bind_match(&mut scope, std::slice::from_ref(mc)),
            Clause::Create(cc) => bind_pattern(&mut scope, &cc.pattern),
            Clause::Merge(mc) => bind_pattern(&mut scope, &mc.pattern),
            Clause::Foreach(_) => {
                // FOREACH binds only inside its own body.
            }
            Clause::Unwind(u) => push_unique(&mut scope, &u.variable),
            Clause::LoadCsv(l) => push_unique(&mut scope, &l.variable),
            Clause::LoadParquet(l) => push_unique(&mut scope, &l.variable),
            Clause::Call(call) => {
                for item in &call.yield_items {
                    push_unique(&mut scope, item.alias.as_ref().unwrap_or(&item.name));
                }
            }
            Clause::With(wc) => {
                // A `WITH *` that projects nothing is legal --
                // `MATCH () CREATE () WITH * CREATE ()` is a TCK scenario that
                // must pass. Only `RETURN *` with nothing in scope is the
                // error, so the flag is set at RETURN sites only.
                let _ = expand_into(&mut wc.items, &scope);
                scope = with_output(&wc.items);
            }
            Clause::Return(rc) => {
                empty_star |= expand_into(&mut rc.items, &returnable(&scope, hidden))
            }
            Clause::Where(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_) => {}
        }
    }
    empty_star
}

/// Expand every `*` in `query`, in place.
///
/// Walks the query in execution order so that each star sees the scope that
/// actually reaches it, including through `WITH` stages that narrow it.
pub fn expand_stars(query: &mut Query) {
    expand_stars_in(query, &[], &[]);
}

/// The names a `RETURN *` may produce: everything in scope but `hidden`.
fn returnable(scope: &[String], hidden: &[String]) -> Vec<String> {
    scope
        .iter()
        .filter(|n| !hidden.contains(n))
        .cloned()
        .collect()
}

/// [`expand_stars`] for a query that starts with `outer` already in scope: the
/// body of a correlated `CALL { WITH a ... }`, which sees what it imports
/// (#1606). Its `RETURN *` leaves out `hidden` -- the imports -- since a
/// subquery may not return a name the outer query already binds, and Neo4j
/// refuses the explicit form of exactly that.
fn expand_stars_in(query: &mut Query, outer: &[String], hidden: &[String]) {
    // Each UNION branch is a query of its own, with its own scope: nothing
    // either side binds is visible to the other.
    for (branch, _) in query.union_queries.iter_mut() {
        expand_stars_in(branch, outer, hidden);
    }
    // Both representations, always. The parser fills `clauses` even when the
    // by-kind fields can express the query, and mirrors the RETURN into
    // `return_clause` -- so expanding only one left a literal `*` in the other
    // for whatever reads it next.
    if !query.clauses.is_empty() {
        query.star_expanded_to_nothing |= expand_stars_pipeline(&mut query.clauses, outer, hidden);
    }
    if query.needs_clause_pipeline {
        // The parser mirrors the pipeline's RETURN into `return_clause` before
        // this pass runs, so expanding one leaves the other holding a literal
        // `*`. Re-mirrored rather than expanded twice: one of them has to be
        // the copy, and the pipeline is the original.
        if let Some(Clause::Return(rc)) = query
            .clauses
            .iter()
            .rev()
            .find(|c| matches!(c, Clause::Return(_)))
        {
            query.return_clause = Some(rc.clone());
        }
        return;
    }

    let mut scope: Vec<String> = outer.to_vec();

    // Only the matches *before* the first WITH are in scope when that WITH is
    // evaluated; the rest are added after it narrows scope.
    let pre_with = query
        .with_split_index
        .unwrap_or(query.match_clauses.len())
        .min(query.match_clauses.len());
    bind_match(&mut scope, &query.match_clauses[..pre_with]);
    bind_unwind(&mut scope, query.unwind_clause.as_ref());
    for u in &query.extra_unwind_clauses {
        push_unique(&mut scope, &u.variable);
    }
    if let Some(l) = &query.load_csv_clause {
        push_unique(&mut scope, &l.variable);
    }
    if let Some(l) = &query.load_parquet_clause {
        push_unique(&mut scope, &l.variable);
    }
    if let Some(call) = &query.call_clause {
        for item in &call.yield_items {
            push_unique(&mut scope, item.alias.as_ref().unwrap_or(&item.name));
        }
    }
    if let Some(sub) = query.call_subquery.as_mut() {
        // An uncorrelated `CALL { ... }` sees nothing from outside, so its
        // stars expand against its own scope alone (#1606); the columns it
        // returns then join the outer scope.
        expand_stars_in(sub, &[], &[]);
        if let Some(rc) = &sub.return_clause {
            for name in with_output(&rc.items) {
                push_unique(&mut scope, &name);
            }
        }
    }
    if let Some(cc) = query.correlated_call.as_mut() {
        // A star in the body sees what the leading `WITH` imports -- every
        // variable in scope here, for `WITH *` -- plus what the body binds
        // (#1606). Expanded before the body's columns are read below, so a
        // `RETURN *` in it exposes real names to the outer query.
        let imported = cc.imports.clone().unwrap_or_else(|| scope.clone());
        expand_stars_in(&mut cc.body, &imported, &imported);
        // `CALL { WITH a ... RETURN b }` adds the columns its body returns.
        // The grammar admits it only before the first WITH.
        if let Some(rc) = &cc.body.return_clause {
            for name in with_output(&rc.items) {
                push_unique(&mut scope, &name);
            }
        }
    }
    if let Some(create) = &query.create_clause {
        // `CREATE (n) RETURN *` returns the created node.
        bind_pattern(&mut scope, &create.pattern);
    }

    // A WITH narrows scope to what it projects, so its own `*` is expanded
    // against the scope that reaches it, and everything after sees only its
    // output.
    let mut empty_star = false;
    let apply_with = |wc: &mut WithClause, scope: &mut Vec<String>| {
        let _ = expand_into(&mut wc.items, scope);
        *scope = with_output(&wc.items);
    };

    if query.with_clause.is_none() {
        // No WITH: every match is in scope, including any the split index
        // would have excluded.
        bind_match(&mut scope, &query.match_clauses);
    }

    // The WITHs in written order. `extra_with_stages` holds the *earlier*
    // ones, each with the clauses that follow it, and `with_clause` holds the
    // **last**. Expanding `with_clause` first gave the last `WITH *` the scope
    // of the first WITH, so a variable bound by a MATCH between them was
    // dropped: `MATCH (a) WITH a MATCH (a)-->(b) WITH * RETURN b` failed with
    // "Variable not found: b" (#1591).
    for (wc, unwind, post_matches, _) in query.extra_with_stages.iter_mut() {
        apply_with(wc, &mut scope);
        bind_unwind(&mut scope, unwind.as_ref());
        bind_match(&mut scope, post_matches);
    }

    if let Some(wc) = query.with_clause.as_mut() {
        apply_with(wc, &mut scope);
        for u in &query.post_with_unwind_clauses {
            push_unique(&mut scope, &u.variable);
        }
        // Matches written after the last WITH re-bind into the narrowed scope.
        // The AST keeps them in the one list and records the boundary in
        // `with_split_index`, so the tail is what follows the WITH.
        let split = query.with_split_index.unwrap_or(query.match_clauses.len());
        if split < query.match_clauses.len() {
            bind_match(&mut scope, &query.match_clauses[split..]);
        }
    }

    if let Some(rc) = query.return_clause.as_mut() {
        empty_star |= expand_into(&mut rc.items, &returnable(&scope, hidden));
    }
    query.star_expanded_to_nothing |= empty_star;
}

#[cfg(test)]
#[path = "star_cov_tests.rs"]
mod cov_tests;
