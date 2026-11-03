//! Where an aggregate may and may not be written (#897).
//!
//! ```cypher
//! MATCH (a) WHERE count(a) > 10 RETURN a       -- ran, returned nothing
//! MATCH (n) RETURN [x IN [1, 2] | count(*)]    -- ran
//! MATCH (a) WITH a, count(*) RETURN a          -- ran, column unnameable
//! ```
//!
//! An aggregate is computed over a *group of rows*. A `WHERE` runs on one row
//! at a time, so the filter would have to consume the rows it is filtering; a
//! comprehension body runs once per list element, which is not a group either.
//!
//! The `HAVING` shape Cypher does have filters on the **alias** — `WITH …
//! count(*) AS c … WHERE c > 1` — not on the aggregate, and is untouched.

use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

#[test]
fn an_aggregate_in_where_is_refused() {
    assert!(refused("MATCH (a) WHERE count(a) > 10 RETURN a"));
    assert!(refused("MATCH (a) WHERE sum(a.n) > 10 RETURN a"));
    assert!(
        refused("MATCH (a) WHERE a.n > 1 AND count(a) > 10 RETURN a"),
        "nested inside a conjunction"
    );
}

/// The HAVING shape is the one Cypher has, and it must keep working.
#[test]
fn filtering_on_an_aggregate_alias_is_allowed() {
    for cypher in [
        "MATCH (a) WITH a, count(*) AS c WHERE c > 1 RETURN a",
        "MATCH (a) WITH count(*) AS c RETURN c",
        "MATCH (a) RETURN count(*) AS c",
        "MATCH (a) RETURN count(*)",
        "MATCH (a) WITH a.n AS n, collect(a) AS as WHERE size(as) > 1 RETURN n",
    ] {
        assert!(!refused(cypher), "wrongly refused `{cypher}`");
    }
}

#[test]
fn an_aggregate_inside_a_comprehension_is_refused() {
    assert!(refused("MATCH (n) RETURN [x IN [1, 2, 3] | count(*)]"));
    assert!(refused("MATCH (n) RETURN [x IN [1, 2] WHERE count(*) > 1 | x]"));
}

/// An aggregate *over* a comprehension is a different thing and is fine.
#[test]
fn an_aggregate_over_a_comprehension_is_allowed() {
    for cypher in [
        "MATCH (n) RETURN collect([x IN [1, 2] | x + 1]) AS c",
        "MATCH (n) RETURN size([x IN [1, 2] | x]) AS s",
    ] {
        assert!(!refused(cypher), "wrongly refused `{cypher}`");
    }
}

/// `WITH` re-scopes, so a column nobody can name is a column nobody can use.
/// `RETURN` is the end of the query and names its column after the text.
#[test]
fn a_with_item_that_is_not_a_variable_needs_an_alias() {
    assert!(refused("MATCH (a) WITH a, count(*) RETURN a"));
    assert!(refused("MATCH (a) WITH a.name RETURN a"));
    assert!(refused("MATCH (a) WITH 1 + 1 RETURN a"));

    assert!(!refused("MATCH (a) WITH a RETURN a"), "a bare variable needs none");
    assert!(!refused("MATCH (a) WITH a, count(*) AS c RETURN a, c"));
    assert!(!refused("MATCH (a) RETURN a.name"), "RETURN is not WITH");
}
