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
    Expression, MatchClause, Query, ReturnItem, UnwindClause, WithClause, STAR_ITEM,
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
fn bind_match(scope: &mut Vec<String>, clauses: &[MatchClause]) {
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
fn expand_into(items: &mut Vec<ReturnItem>, scope: &[String]) {
    if !has_star(items) {
        return;
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
    *items = out;
}

/// Expand every `*` in `query`, in place.
///
/// Walks the query in execution order so that each star sees the scope that
/// actually reaches it, including through `WITH` stages that narrow it.
pub fn expand_stars(query: &mut Query) {
    let mut scope: Vec<String> = Vec::new();

    // Only the matches *before* the first WITH are in scope when that WITH is
    // evaluated; the rest are added after it narrows scope.
    let pre_with = query
        .with_split_index
        .unwrap_or(query.match_clauses.len())
        .min(query.match_clauses.len());
    bind_match(&mut scope, &query.match_clauses[..pre_with]);
    bind_unwind(&mut scope, query.unwind_clause.as_ref());
    if let Some(call) = &query.call_clause {
        for item in &call.yield_items {
            push_unique(&mut scope, item.alias.as_ref().unwrap_or(&item.name));
        }
    }
    if let Some(create) = &query.create_clause {
        // `CREATE (n) RETURN *` returns the created node.
        for path in &create.pattern.paths {
            if let Some(v) = &path.start.variable {
                push_unique(&mut scope, v);
            }
            for seg in &path.segments {
                if let Some(v) = &seg.edge.variable {
                    push_unique(&mut scope, v);
                }
                if let Some(v) = &seg.node.variable {
                    push_unique(&mut scope, v);
                }
            }
        }
    }

    // A WITH narrows scope to what it projects, so its own `*` is expanded
    // against the scope that reaches it, and everything after sees only its
    // output.
    let mut apply_with = |wc: &mut WithClause, scope: &mut Vec<String>| {
        expand_into(&mut wc.items, scope);
        *scope = with_output(&wc.items);
    };

    if query.with_clause.is_none() {
        // No WITH: every match is in scope, including any the split index
        // would have excluded.
        bind_match(&mut scope, &query.match_clauses);
    }

    if let Some(wc) = query.with_clause.as_mut() {
        apply_with(wc, &mut scope);
        // Matches written after the WITH re-bind into the narrowed scope. The
        // AST keeps every MATCH in one list and records the boundary in
        // `with_split_index`, so the tail is what follows the WITH.
        let split = query.with_split_index.unwrap_or(query.match_clauses.len());
        if split < query.match_clauses.len() {
            bind_match(&mut scope, &query.match_clauses[split..]);
        }
    }

    for (wc, unwind, post_matches, _) in query.extra_with_stages.iter_mut() {
        apply_with(wc, &mut scope);
        bind_unwind(&mut scope, unwind.as_ref());
        bind_match(&mut scope, post_matches);
    }

    if let Some(rc) = query.return_clause.as_mut() {
        expand_into(&mut rc.items, &scope);
    }
}
