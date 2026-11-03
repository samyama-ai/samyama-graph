//! What an ORDER BY may reference after an aggregating or DISTINCT projection.
//!
//! A projection that aggregates or de-duplicates *replaces* the row. Variables
//! that fed it are no longer in scope, so an ORDER BY naming them is asking for
//! something that no longer exists. openCypher makes this a compile-time error;
//! we answered the query instead, silently sorting on whatever the variable
//! still happened to hold.
//!
//! The rule has three edges, all taken from the TCK rather than inferred:
//!
//! ```text
//! RETURN DISTINCT a.name ORDER BY a.age              error - a is gone after DISTINCT
//! RETURN count(y.age) AS agg ORDER BY m.age+count(..) error - m was never projected
//! RETURN m.age+y.age, count(*) ORDER BY m.age+y.age+count(*)
//!                                                     error - a grouping key mixed into
//!                                                     an aggregating sort item
//! RETURN avg(p.age) AS a ORDER BY $age+avg(p.age)-1000   LEGAL - only constants and
//!                                                     parameters outside the aggregate
//! ```
//!
//! The legal cases are the point of this file. Rejecting too much is worse than
//! the defect: the defect sorts oddly, an over-strict rule refuses queries that
//! work today.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn seeded() -> GraphStore {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:Person {name: 'A', age: 13, num1: 1, num2: 4})");
    run(&mut store, "CREATE (:Person {name: 'B', age: 12, num1: 5, num2: 2})");
    store
}

/// Rejected at parse time, or at execution — either is a refusal.
fn assert_rejected(store: &GraphStore, cypher: &str) {
    match parse_query(cypher) {
        Err(_) => {}
        Ok(q) => {
            if QueryExecutor::new(store).execute(&q).is_ok() {
                panic!("`{cypher}` should be rejected, but it succeeded");
            }
        }
    }
}

fn assert_accepted(store: &GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run, not be rejected: {e}"));
}

#[test]
fn distinct_removes_the_variable_from_order_by_scope() {
    // ReturnOrderBy2 [13].
    let store = seeded();
    assert_rejected(&store, "MATCH (a:Person) RETURN DISTINCT a.name ORDER BY a.age");
}

#[test]
fn an_unprojected_variable_inside_an_aggregating_sort_item_is_rejected() {
    // ReturnOrderBy6 [4] / WithOrderBy4 [19]: `me` never reaches the projection.
    let store = seeded();
    assert_rejected(
        &store,
        "MATCH (me:Person)--(you:Person) RETURN count(you.age) AS agg \
         ORDER BY me.age + count(you.age)",
    );
    assert_rejected(
        &store,
        "MATCH (me:Person)--(you:Person) WITH count(you.age) AS agg \
         ORDER BY me.age + count(you.age) RETURN agg",
    );
}

#[test]
fn a_projected_expression_mixed_into_an_aggregating_sort_item_is_rejected() {
    // ReturnOrderBy6 [5] / WithOrderBy4 [20]. `me.age + you.age` *is* projected,
    // and still may not sit inside a sort item that also aggregates.
    let store = seeded();
    assert_rejected(
        &store,
        "MATCH (me:Person)--(you:Person) RETURN me.age + you.age, count(*) AS cnt \
         ORDER BY me.age + you.age + count(*)",
    );
}

#[test]
fn an_aggregation_in_order_by_without_one_in_the_projection_is_rejected() {
    // ReturnOrderBy2 [14].
    let store = seeded();
    assert_rejected(&store, "MATCH (n:Person) RETURN n.num1 ORDER BY max(n.num2)");
}

#[test]
fn constants_and_parameters_beside_an_aggregate_stay_legal() {
    // ReturnOrderBy6 [1] — the case an over-strict rule breaks.
    let store = seeded();
    assert_accepted(
        &store,
        "MATCH (person:Person) RETURN avg(person.age) AS avgAge ORDER BY 38 + avg(person.age) - 1000",
    );
}

#[test]
fn ordinary_order_by_is_untouched() {
    // No aggregation and no DISTINCT: ORDER BY may name anything in scope.
    let store = seeded();
    assert_accepted(&store, "MATCH (n:Person) RETURN n.name ORDER BY n.age");
    assert_accepted(&store, "MATCH (n:Person) RETURN n.name AS nm ORDER BY nm");
    assert_accepted(&store, "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC");
}

#[test]
fn sorting_by_a_grouping_key_or_an_alias_stays_legal() {
    // The common aggregate shapes. These must keep working.
    let store = seeded();
    assert_accepted(&store, "MATCH (n:Person) RETURN n.name, count(*) AS c ORDER BY n.name");
    assert_accepted(&store, "MATCH (n:Person) RETURN n.name, count(*) AS c ORDER BY c");
    assert_accepted(&store, "MATCH (n:Person) RETURN count(*) AS c ORDER BY c");
    assert_accepted(&store, "MATCH (n:Person) WITH n.name AS nm, count(*) AS c ORDER BY nm RETURN nm, c");
}

#[test]
fn a_projected_property_inside_an_aggregating_sort_item_stays_legal() {
    // ReturnOrderBy6 [3] and WithOrderBy4 [18]. My first version of this rule
    // rejected both — it treated any variable outside the aggregate as an
    // error, when a *projected grouping key* there is fine. The difference
    // from the rejected case is whether the leaf itself was projected
    // (`me.age AS age`) or only some compound containing it (`me.age+you.age`).
    let store = seeded();
    assert_accepted(
        &store,
        "MATCH (me:Person)--(you:Person) RETURN me.age AS age, count(you.age) AS cnt \
         ORDER BY me.age + count(you.age)",
    );
    assert_accepted(
        &store,
        "MATCH (me:Person)--(you:Person) WITH me.age AS age, count(you.age) AS cnt \
         ORDER BY me.age + count(you.age) RETURN age, cnt",
    );
}

#[test]
fn an_aggregate_the_projection_did_not_compute_is_rejected() {
    // WithOrderBy4 [13]/[14]: `sum(sum)` was never projected, and `sum` does
    // not survive the second WITH, so there is nothing to compute it from.
    let store = seeded();
    assert_rejected(
        &store,
        "MATCH (a:Person) WITH a, a.num1 + a.num2 AS sum \
         WITH a.num2 % 3 AS mod, min(sum) AS mn ORDER BY sum(sum) RETURN mod, mn",
    );
}

#[test]
fn distinct_on_a_whole_entity_keeps_its_properties_sortable() {
    // ReturnOrderBy2 [4], [5], [10] — three scenarios my first two attempts at
    // this rule both broke. Projecting the entity keeps `a` alive, so `a.name`
    // is reachable; projecting only `a.name` does not, which is the rejected
    // case above. The distinction is what is projected, not that DISTINCT
    // appeared.
    let store = seeded();
    assert_accepted(&store, "MATCH (a:Person) RETURN DISTINCT a ORDER BY a.name");
    assert_accepted(&store, "MATCH (a:Person) RETURN DISTINCT a ORDER BY a.age DESC");
    assert_accepted(&store, "MATCH (a:Person) WITH DISTINCT a ORDER BY a.name RETURN a.name");
}

#[test]
fn restating_a_grouping_expression_as_the_sort_key_stays_legal() {
    // `ORDER BY a.num2 % 3` where the projection says `a.num2 % 3 AS m` is the
    // same value spelled out rather than aliased, and is the commoner spelling.
    //
    // Caught by `result_determinism.rs`, not by this file or by the TCK diff —
    // the third legal case these rules broke. Comparing grouping keys against
    // the sort item's *leaves* is not the same as comparing against the item.
    let store = seeded();
    assert_accepted(
        &store,
        "MATCH (a:Person) WITH a.num2 % 3 AS m, sum(a.num1) AS s ORDER BY a.num2 % 3 RETURN m, s",
    );
    assert_accepted(
        &store,
        "MATCH (a:Person) RETURN a.num2 % 3 AS m, sum(a.num1) AS s ORDER BY a.num2 % 3",
    );
}
