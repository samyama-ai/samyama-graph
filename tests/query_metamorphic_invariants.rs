//! Two metamorphic relations over ordinary queries (LANG-05).
//!
//! A metamorphic relation compares **two of the engine's own answers** across a
//! transformation that must not change them. That is what makes it worth
//! having: an ordinary test asserts an answer we chose, and #1140 survived our
//! tests because they encoded the same assumption the bug did. A relation
//! cannot encode our assumptions, because neither side is written down.
//!
//! # 1. An index must not change the answer
//!
//! Declaring an index is a statement about *speed*. If it changes a result,
//! the index is answering a different question from the one that was asked.
//!
//! This is the general form of #1343, which was found by hand: the hierarchy
//! index answered a subsumption question — set-shaped — in place of a
//! variable-length pattern, which is defined over paths, so a multi-parent
//! subtree summed to 225 with the index and 235 without. The fix was narrow.
//! This relation is the check that would have found it, and will find the next
//! one over any index the engine grows.
//!
//! # 2. A property in the pattern is the same as a property in `WHERE`
//!
//! `MATCH (n:L {k: 1})` and `MATCH (n:L) WHERE n.k = 1` are the same question
//! written two ways.
//!
//! **Without an index they plan identically** — the inline form is normalised
//! into the predicate form — so on a bare store this relation compares a plan
//! with itself and checks nothing. The guard below is what found that, and it
//! is kept because it is the only thing standing between this relation and a
//! check that cannot fail.
//!
//! With an index declared they diverge: the inline form becomes a bare
//! `IndexScan`, the `WHERE` form an `IndexScan` under a `Filter` that
//! re-evaluates what the index already answered (#1380). Two code paths, one
//! required answer, and #593 and the WHERE re-application bug were both in
//! that machinery.
//!
//! Neither relation is a special case of the six already counted, which are
//! about quantifier intervals, round trips and the result cache.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

/// A graph with enough shape for the corpus to say something: two labels,
/// properties of three types, a multi-parent hierarchy, and a node that is
/// reachable two ways.
const FIXTURE: &[&str] = &[
    "CREATE (:Person {name: 'Alice', age: 30, active: true})",
    "CREATE (:Person {name: 'Bob', age: 41, active: false})",
    "CREATE (:Person {name: 'Carol', age: 30, active: true})",
    "CREATE (:Company {name: 'Acme', founded: 1999})",
    "CREATE (:T {code: 'R', units: 0}), (:T {code: 'A', units: 10})",
    "CREATE (:T {code: 'B', units: 20}), (:T {code: 'L', units: 5})",
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2019}]->(b)",
    "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:KNOWS {since: 2021}]->(c)",
    "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT]->(c)",
    "MATCH (a:T {code: 'A'}), (r:T {code: 'R'}) CREATE (a)-[:MAPS_TO]->(r)",
    "MATCH (b:T {code: 'B'}), (r:T {code: 'R'}) CREATE (b)-[:MAPS_TO]->(r)",
    "MATCH (l:T {code: 'L'}), (a:T {code: 'A'}) CREATE (l)-[:MAPS_TO]->(a)",
    "MATCH (l:T {code: 'L'}), (b:T {code: 'B'}) CREATE (l)-[:MAPS_TO]->(b)",
];

/// Read-only queries, spanning the shapes an index is most likely to disturb.
const CORPUS: &[&str] = &[
    "MATCH (p:Person) RETURN p.name AS a, p.age AS b",
    "MATCH (p:Person {age: 30}) RETURN p.name AS a",
    "MATCH (p:Person) WHERE p.age = 30 RETURN p.name AS a",
    "MATCH (p:Person) WHERE p.age > 30 RETURN count(p) AS a",
    "MATCH (p:Person) RETURN p.age AS a, count(*) AS b",
    "MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN p.name AS a, q.name AS b",
    "MATCH (p:Person)-[r:KNOWS]->(q) WHERE r.since > 2020 RETURN q.name AS a",
    "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name AS a",
    "MATCH (d)-[:MAPS_TO*0..]->(r:T {code: 'R'}) RETURN sum(d.units) AS a",
    "MATCH (d)-[:MAPS_TO*0..]->(r:T {code: 'A'}) RETURN sum(d.units) AS a",
    "MATCH (d)-[:MAPS_TO*0..]->(r:T {code: 'R'}) RETURN count(d) AS a",
    "MATCH (d)-[:MAPS_TO*1..2]->(r:T {code: 'R'}) RETURN count(d) AS a",
    "MATCH (t:T) RETURN t.code AS a ORDER BY a",
    "MATCH (p:Person) RETURN max(p.age) AS a, min(p.age) AS b, avg(p.age) AS c",
    "MATCH (p:Person) WHERE p.active RETURN p.name AS a",
];

/// Index declarations. Each must leave every answer above untouched.
const INDEXES: &[&str] = &[
    "CREATE INDEX ON :Person(age)",
    "CREATE INDEX ON :Person(name)",
    "CREATE INDEX ON :T(code)",
    "CREATE HIERARCHY INDEX h ON ()-[:MAPS_TO]->() MEASURE units AGGREGATE sum, count",
];

fn build(statements: &[&str]) -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in statements {
        engine
            .execute_mut(q, &mut store, "default")
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    store
}

/// An answer as a **multiset**: sorted rows, so an ordering difference is not
/// reported as a semantic one. Order without `ORDER BY` is not guaranteed and
/// asserting it here would make this relation fail for the wrong reason.
fn answer(store: &GraphStore, query: &str) -> Vec<String> {
    let batch = QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            r.bindings()
                .iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn declaring_an_index_does_not_change_any_answer() {
    let plain = build(FIXTURE);

    let mut indexed = build(FIXTURE);
    let engine = QueryEngine::new();
    for ddl in INDEXES {
        engine
            .execute_mut(ddl, &mut indexed, "default")
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }

    let mut differing = Vec::new();
    for q in CORPUS {
        let before = answer(&plain, q);
        let after = answer(&indexed, q);
        if before != after {
            differing.push(format!("{q}\n      without: {before:?}\n      with:    {after:?}"));
        }
    }
    assert!(
        differing.is_empty(),
        "{} quer(ies) answered differently once an index existed. An index is a claim \
         about speed; a different answer means it is answering a different question \
         (#1343 was this, found by hand):\n  {}",
        differing.len(),
        differing.join("\n  ")
    );
}

#[test]
fn the_corpus_actually_exercises_the_indexes() {
    // The half that stops the relation above from holding vacuously. If every
    // query returned no rows, or if no index were ever consulted, the two
    // answers would agree for reasons that say nothing.
    let store = build(FIXTURE);
    let non_empty = CORPUS
        .iter()
        .filter(|q| !answer(&store, q).is_empty())
        .count();
    assert_eq!(
        non_empty,
        CORPUS.len(),
        "every corpus query must return rows, or the comparison is between two empty sets"
    );

    // And at least one query must plan differently with the indexes present —
    // otherwise the relation is comparing the same plan with itself.
    let mut indexed = build(FIXTURE);
    let engine = QueryEngine::new();
    for ddl in INDEXES {
        engine.execute_mut(ddl, &mut indexed, "default").unwrap();
    }
    let changed = CORPUS
        .iter()
        .filter(|q| plan_of(q, &store) != plan_of(q, &indexed))
        .count();
    assert!(
        changed > 0,
        "no query in the corpus plans differently with the indexes declared, so the \
         relation is comparing a plan with itself"
    );
}

#[test]
fn a_pattern_property_equals_the_same_predicate_in_where() {
    // Two spellings of one question. **Without an index the planner
    // normalises them into the same plan**, so on a bare store this relation
    // compares a plan with itself and checks nothing — which the guard below
    // is what found.
    //
    // With an index they diverge, and that is where the relation earns its
    // keep: the inline form becomes a bare `IndexScan`, the `WHERE` form an
    // `IndexScan` under a `Filter`. Two code paths, one required answer.
    let store = indexed_store();
    let pairs = [
        (
            "MATCH (p:Person {age: 30}) RETURN p.name AS a",
            "MATCH (p:Person) WHERE p.age = 30 RETURN p.name AS a",
        ),
        (
            "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(q) RETURN q.name AS a",
            "MATCH (p:Person)-[:KNOWS]->(q) WHERE p.name = 'Alice' RETURN q.name AS a",
        ),
        (
            "MATCH (p:Person {active: true}) RETURN count(p) AS a",
            "MATCH (p:Person) WHERE p.active = true RETURN count(p) AS a",
        ),
        (
            "MATCH (p:Person)-[r:KNOWS {since: 2019}]->(q) RETURN q.name AS a",
            "MATCH (p:Person)-[r:KNOWS]->(q) WHERE r.since = 2019 RETURN q.name AS a",
        ),
        (
            "MATCH (c:Company {founded: 1999}) RETURN c.name AS a",
            "MATCH (c:Company) WHERE c.founded = 1999 RETURN c.name AS a",
        ),
    ];
    for (inline, predicate) in pairs {
        let a = answer(&store, inline);
        let b = answer(&store, predicate);
        assert_eq!(
            a, b,
            "the same question written two ways answered differently:\n  {inline}\n  {predicate}"
        );
        assert!(
            !a.is_empty(),
            "both spellings returned nothing, so they agree about nothing: {inline}"
        );
    }
}

#[test]
fn the_relation_compares_two_plans_and_not_one() {
    // The guard that keeps the relation above from being a check that cannot
    // fail, and it has done its job twice now.
    //
    // First: on a store with no index the two spellings plan **identically**,
    // so the original version of the relation compared a plan with itself.
    let bare = build(FIXTURE);
    assert_eq!(
        plan_of("MATCH (p:Person {age: 30}) RETURN p.name AS a", &bare),
        plan_of("MATCH (p:Person) WHERE p.age = 30 RETURN p.name AS a", &bare),
        "the planner used to normalise these into one plan without an index; if that \
         has changed, this relation is stronger than its comment claims"
    );

    // Second: samyama-graph#1380 made the *indexed* simple-equality pair
    // converge too. The `WHERE` form used to keep a `Filter` re-checking the
    // predicate its `IndexScan` had already answered; now the conjunct the
    // index consumed is dropped, and the two spellings plan alike. That is
    // the fix working -- and it removes the divergence this guard relied on,
    // so the guard moves to a pair that still diverges rather than being
    // deleted.
    let store = indexed_store();
    let inline = plan_of("MATCH (p:Person {age: 30}) RETURN p.name AS a", &store);
    let predicate = plan_of("MATCH (p:Person) WHERE p.age = 30 RETURN p.name AS a", &store);
    assert_eq!(
        inline, predicate,
        "#1380 made these converge; if they have diverged again the redundant \
         Filter is back:\n  inline: {inline}\n  where:  {predicate}"
    );
    assert!(
        inline.contains("IndexScan") && !inline.contains("Filter"),
        "both forms should be a bare index lookup: {inline}"
    );

    // The pair that keeps the relation honest: a predicate the index answers
    // only in part. The scan narrows on `age` and the `city` conjunct still
    // has to be evaluated, so the two spellings reach the answer differently
    // and comparing their answers is comparing two things.
    let partial_inline = plan_of(
        "MATCH (p:Person {age: 30, city: 'Springfield'}) RETURN p.name AS a",
        &store,
    );
    let partial_where = plan_of(
        "MATCH (p:Person) WHERE p.age = 30 AND p.city = 'Springfield' RETURN p.name AS a",
        &store,
    );
    assert!(
        partial_where.contains("IndexScan") && partial_where.contains("Filter"),
        "a conjunct the index cannot answer must survive as a Filter: {partial_where}"
    );
    assert!(
        !partial_where.contains("p.age ="),
        "the conjunct the IndexScan answered is still being re-checked: {partial_where}"
    );
    let _ = partial_inline;

    // And a range, which #1380 deliberately does not touch: a scan that
    // narrows is not a scan that decides.
    let ranged = plan_of("MATCH (p:Person) WHERE p.age > 20 RETURN p.name AS a", &store);
    assert!(
        ranged.contains("IndexScan") && ranged.contains("Filter"),
        "a range scan must keep its filter: {ranged}"
    );
}

/// The fixture with every index declared.
fn indexed_store() -> GraphStore {
    let mut store = build(FIXTURE);
    let engine = QueryEngine::new();
    for ddl in INDEXES {
        engine
            .execute_mut(ddl, &mut store, "default")
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    store
}

fn plan_of(query: &str, store: &GraphStore) -> String {
    let parsed = samyama::query::parse_query(query).expect("parse");
    let planner = samyama::query::executor::planner::QueryPlanner::new();
    planner
        .plan(&parsed, store)
        .expect("plan")
        .root
        .describe()
        .format(0)
}
