//! Full-text search through Cypher: DDL, procedure, composition (NDS-06, NDS-07).
//!
//! Everything here goes through the query engine rather than calling the index
//! directly. The index has its own unit tests; what these ask is whether a
//! user can reach it — the DDL parses, the index is populated, the procedure
//! binds `node` and `score`, and the result composes with a MATCH.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

fn engine() -> QueryEngine {
    QueryEngine::new()
}

fn write(store: &mut GraphStore, q: &str) {
    engine()
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn rows(store: &GraphStore, q: &str) -> Vec<Vec<(String, String)>> {
    let batch = engine()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    batch
        .records
        .iter()
        .map(|r| {
            batch
                .columns
                .iter()
                .map(|c| (c.clone(), format!("{:?}", r.get(c))))
                .collect()
        })
        .collect()
}

fn count(store: &GraphStore, q: &str) -> i64 {
    let batch = engine()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let first = batch
        .records
        .first()
        .map(|r| r.values().next().cloned())
        .unwrap_or(None);
    match first {
        Some(samyama::query::executor::record::Value::Property(PropertyValue::Integer(n))) => n,
        other => panic!("{q}: expected an integer, got {other:?}"),
    }
}

/// Three documents, one of them not matching, plus an outgoing edge for the
/// composition test.
fn corpus() -> GraphStore {
    let mut g = GraphStore::new();
    write(
        &mut g,
        "CREATE (a:Doc {title: 'a', body: 'graph databases store relationships'}) \
         CREATE (b:Doc {title: 'b', body: 'a graph is a set of nodes and edges'}) \
         CREATE (c:Doc {title: 'c', body: 'relational tables and joins'}) \
         CREATE (t:Tag {name: 'theory'}) \
         CREATE (b)-[:TAGGED]->(t)",
    );
    g
}

#[test]
fn the_ddl_creates_an_index_and_backfills_what_is_already_there() {
    // The normal order for a load is data first, DDL second. An index that
    // registers without populating returns nothing for every search, with no
    // error to say why — which is indistinguishable from a corpus that does
    // not contain the word.
    let mut g = corpus();
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");

    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'graph') YIELD node RETURN count(node)"
        ),
        2,
        "both graph documents must be found on a corpus loaded before the DDL"
    );
}

#[test]
fn the_score_comes_back_and_ranks() {
    let mut g = corpus();
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");

    // Read the value, not its Debug rendering: parsing `Some(Property(Float(..)))`
    // back out of a string is a test that breaks when the formatting changes
    // and says nothing about the score.
    let batch = engine()
        .execute(
            "CALL db.index.fulltext.queryNodes('docs', 'graph relationships') \
             YIELD node, score RETURN node, score",
            &g,
        )
        .unwrap();
    assert_eq!(batch.records.len(), 2);
    let scores: Vec<f64> = batch
        .records
        .iter()
        .map(|r| match r.get("score") {
            Some(samyama::query::executor::record::Value::Property(PropertyValue::Float(f))) => *f,
            other => panic!("score must be a float, got {other:?}"),
        })
        .collect();
    let r = &scores;
    assert!(scores[0] > 0.0, "a matching document scored zero: {r:?}");
    assert!(
        scores[0] >= scores[1],
        "results must come back ranked: {scores:?}"
    );
}

#[test]
fn the_search_finds_words_contains_would_miss() {
    // The reason this index exists. `CONTAINS 'relationship'` misses
    // "relationships"; `CONTAINS 'graph'` additionally matches "polygraph".
    let mut g = corpus();
    write(
        &mut g,
        "CREATE (:Doc {title: 'd', body: 'a polygraph is not a graph'})",
    );
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");

    assert_eq!(
        count(
            &g,
            "MATCH (d:Doc) WHERE d.body CONTAINS 'relationship' RETURN count(d)"
        ),
        1,
        "CONTAINS does substring matching, so it finds 'relationships' too"
    );
    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'relationship') \
             YIELD node RETURN count(node)"
        ),
        1,
        "the stemmer must find the plural from the singular"
    );

    // And the difference that matters the other way: ranking. `polygraph`
    // contains `graph` as a substring but is a different word.
    let ids = rows(
        &g,
        "CALL db.index.fulltext.queryNodes('docs', 'graph') YIELD node RETURN node",
    );
    assert_eq!(
        ids.len(),
        3,
        "three documents hold the word `graph`; `polygraph` is not one of them: {ids:?}"
    );
}

#[test]
fn a_search_composes_with_a_traversal() {
    // NDS-07: two structures in one query. The CALL binds `node`, and the
    // MATCH that follows expands from it.
    let mut g = corpus();
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");

    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'graph') YIELD node \
             MATCH (node)-[]->(m) RETURN count(m)"
        ),
        1,
        "only document b has an outgoing edge, and it is one of the two matches"
    );
}

#[test]
fn an_updated_property_stops_matching_its_old_words() {
    // Maintenance. A stale inverted index reports nothing — it returns a
    // document that no longer holds the word, and the caller has no way to
    // tell that from a correct hit.
    let mut g = corpus();
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");
    write(
        &mut g,
        "MATCH (d:Doc {title: 'a'}) SET d.body = 'entirely different subject matter'",
    );

    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'databases') \
             YIELD node RETURN count(node)"
        ),
        0,
        "the old text is still indexed"
    );
    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'subject') YIELD node RETURN count(node)"
        ),
        1,
        "the new text was not indexed"
    );
}

#[test]
fn a_node_created_after_the_ddl_is_indexed() {
    // The other order: DDL first, data second. Both have to work, and they go
    // through different code — the backfill and the write hook.
    let mut g = GraphStore::new();
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");
    write(
        &mut g,
        "CREATE (:Doc {body: 'added after the index existed'})",
    );

    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'existed') YIELD node RETURN count(node)"
        ),
        1
    );
}

#[test]
fn a_deleted_node_stops_matching() {
    // Found by re-reading the write paths rather than by a failing test:
    // `on_node_deleted` existed and nothing called it, so a deleted node kept
    // its terms and a search returned a node that was gone. A caller cannot
    // tell that from a correct hit.
    let mut g = corpus();
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");
    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'graph') YIELD node RETURN count(node)"
        ),
        2
    );

    write(&mut g, "MATCH (d:Doc {title: 'a'}) DELETE d");
    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'databases') \
             YIELD node RETURN count(node)"
        ),
        0,
        "the deleted node is still in the index"
    );
    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'graph') YIELD node RETURN count(node)"
        ),
        1,
        "the surviving match must still be found"
    );
}

#[test]
fn a_phrase_query_requires_adjacency() {
    let mut g = GraphStore::new();
    write(
        &mut g,
        "CREATE (:Doc {body: 'the shortest path between nodes'}) \
         CREATE (:Doc {body: 'the shortest route along another path'})",
    );
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");

    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', '\\\"shortest path\\\"') \
             YIELD node RETURN count(node)"
        ),
        1,
        "both documents hold both words; only one holds the phrase"
    );
}

#[test]
fn a_misspelt_index_name_is_an_error_not_an_empty_result() {
    // An empty result and a typo look identical to a caller, and the typo is
    // far commoner. Failing at planning time says which it was.
    let mut g = corpus();
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");

    let err = engine()
        .execute(
            "CALL db.index.fulltext.queryNodes('doc', 'graph') YIELD node RETURN node",
            &g,
        )
        .expect_err("a name that does not exist must be refused");
    let msg = format!("{err}");
    assert!(
        msg.contains("doc") && msg.contains("docs"),
        "the error must name what was asked for and what exists: {msg}"
    );
}

#[test]
fn dropping_an_index_that_is_not_there_is_an_error() {
    // Same reason: the commonest cause is a typo, and succeeding silently
    // leaves the real index in place and the caller believing otherwise.
    let mut g = corpus();
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");
    assert!(engine()
        .execute_mut("DROP FULLTEXT INDEX nosuch", &mut g, "default")
        .is_err());

    write(&mut g, "DROP FULLTEXT INDEX docs");
    assert!(
        engine()
            .execute(
                "CALL db.index.fulltext.queryNodes('docs', 'graph') YIELD node RETURN node",
                &g
            )
            .is_err(),
        "the dropped index must be gone"
    );
}

#[test]
fn only_the_indexed_label_and_property_are_searched() {
    // An index that quietly covered every string property would answer more
    // than it was asked and look better while doing it.
    let mut g = GraphStore::new();
    write(
        &mut g,
        "CREATE (:Doc {body: 'alpha', title: 'beta'}) CREATE (:Other {body: 'alpha'})",
    );
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");

    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'alpha') YIELD node RETURN count(node)"
        ),
        1,
        "the :Other node must not be indexed"
    );
    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'beta') YIELD node RETURN count(node)"
        ),
        0,
        "the title property is not in this index"
    );
}

#[test]
fn the_limit_is_a_third_argument_and_defaults() {
    let mut g = GraphStore::new();
    for i in 0..10 {
        write(
            &mut g,
            &format!("CREATE (:Doc {{body: 'common word {i}'}})"),
        );
    }
    write(&mut g, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON (d.body)");

    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'common') YIELD node RETURN count(node)"
        ),
        10,
        "the default limit must not cut a ten-document corpus short"
    );
    assert_eq!(
        count(
            &g,
            "CALL db.index.fulltext.queryNodes('docs', 'common', 3) YIELD node RETURN count(node)"
        ),
        3
    );
}
