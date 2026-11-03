//! A projection may combine an aggregate only with a grouping key (#930).
//!
//! ```cypher
//! WITH me.age + count(you.age) AS agg                    -- ran, 0 rows
//! RETURN me.age + you.age, me.age + you.age + count(*)   -- ran, 0 rows
//! ```
//!
//! openCypher raises `SyntaxError: AmbiguousAggregationExpression` at compile
//! time. We returned **zero rows and no error**, which is the worst of the
//! three outcomes: the query looks like it ran and found nothing.
//!
//! Cypher forms its groups from the projection's non-aggregating items, so in
//! `me.age + count(you.age)` there is no group for `me.age` to be evaluated
//! over. The error name is exact — two readings are equally defensible (the
//! first row's `me.age`, or one group per distinct `me.age`), so the language
//! refuses to pick rather than picking badly.
//!
//! Half of these tests are the queries that must keep working. A rule like
//! this is far more dangerous when it over-rejects than when it under-rejects:
//! the previous attempt at an undefined-variable rule in this file failed 126
//! valid scenarios on its first run.

use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

fn accepted(cypher: &str) -> bool {
    parse_query(cypher).is_ok()
}

#[test]
fn a_property_outside_the_aggregate_is_refused() {
    assert!(refused(
        "MATCH (me:Person)--(you:Person) WITH me.age + count(you.age) AS agg RETURN *"
    ));
}

#[test]
fn a_compound_grouping_key_is_still_ambiguous() {
    // `me.age + you.age` *is* projected, and it is still not a grouping key:
    // a compound expression restated inside an aggregating item is not
    // something Cypher can group by.
    assert!(refused(
        "MATCH (me:Person)--(you:Person) \
         WITH me.age + you.age AS grp, me.age + you.age + count(*) AS agg RETURN *"
    ));
    assert!(refused(
        "MATCH (me:Person)--(you:Person) RETURN me.age + you.age, me.age + you.age + count(*)"
    ));
}

#[test]
fn a_bare_aggregate_is_fine() {
    assert!(accepted("MATCH (n) RETURN count(*)"));
    assert!(accepted("MATCH (n) RETURN count(n.age)"));
    assert!(accepted("MATCH (n) WITH count(*) AS c RETURN c"));
}

#[test]
fn grouping_by_a_projected_variable_is_fine() {
    // `n` is a whole projected item, so it is a grouping key and referring to
    // it inside the aggregating item is unambiguous.
    assert!(accepted("MATCH (n) RETURN n, count(*)"));
    assert!(accepted("MATCH (n) RETURN n, n.age + count(*)"));
    assert!(accepted("MATCH (n) WITH n, count(*) AS c RETURN n, c"));
}

#[test]
fn grouping_by_a_projected_property_is_fine() {
    assert!(accepted("MATCH (n) RETURN n.age, n.age + count(*)"));
    assert!(accepted("MATCH (n) WITH n.age AS a, count(*) AS c RETURN a, c"));
}

#[test]
fn a_projection_that_does_not_aggregate_is_untouched() {
    // The rule must not fire at all without an aggregate, or every ordinary
    // projection becomes a candidate for rejection.
    assert!(accepted("MATCH (me)--(you) RETURN me.age + you.age"));
    assert!(accepted("MATCH (me)--(you) WITH me.age + you.age AS s RETURN s"));
}

#[test]
fn variables_inside_the_aggregate_need_no_grouping() {
    // Inside an aggregate is exactly where a non-grouping variable belongs:
    // `count(you.age)` is the point of the query.
    assert!(accepted("MATCH (me)--(you) RETURN me, count(you.age)"));
    assert!(accepted("MATCH (me)--(you) RETURN me, collect(you)"));
}

#[test]
fn multiple_aggregates_over_one_key_are_fine() {
    assert!(accepted("MATCH (n) RETURN n.age, count(*), sum(n.score), max(n.score)"));
}

#[test]
fn a_literal_beside_an_aggregate_is_fine() {
    // No variable, so nothing to group by and nothing ambiguous.
    assert!(accepted("MATCH (n) RETURN 1 + count(*)"));
    assert!(accepted("MATCH (n) RETURN count(*) * 2"));
}
