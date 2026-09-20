//! A label or relationship type may be written in backticks (#1373).
//!
//! `escaped_name` was in the grammar and only `property_key` used it, so this
//! worked:
//!
//! ```text
//! CREATE (n:N {`first author`: 'Ada'})    -- OK
//! ```
//!
//! and this did not:
//!
//! ```text
//! CREATE (:`Research Paper` {name: 'x'})  -- parse error
//! MATCH ()-[r:`WORKS WITH`]->() RETURN r  -- parse error
//! ```
//!
//! Backticks are how openCypher writes an identifier that is not an ASCII
//! word, and labels with spaces are ordinary in imported data. They are what an
//! RDF import produces, because an `rdf:type` IRI's local name is arbitrary
//! text — so a graph the RDF path can hold could not be queried by label
//! through the Cypher path (#1362).
//!
//! The asymmetry was also the surprising kind: a user who finds backticks work
//! on property keys reasonably concludes they work.
//!
//! **Variables are still not covered.** ``MATCH (`my node`:N)`` remains a parse
//! error; there are twenty-five read sites for `Rule::variable` against eight
//! for labels and types, and a missed one is silent rather than loud — see the
//! sweep below for why that matters. #1373 stays open for it.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn store() -> GraphStore {
    GraphStore::new()
}

fn write(store: &mut GraphStore, query: &str) {
    QueryEngine::new()
        .execute_mut(query, store, "default")
        .unwrap_or_else(|e| panic!("{query}: {e}"));
}

fn scalars(store: &GraphStore, query: &str) -> Vec<String> {
    QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .records
        .iter()
        .map(|r| format!("{:?}", r.bindings()[0].1))
        .collect()
}

#[test]
fn a_label_with_a_space_can_be_created_and_matched() {
    let mut s = store();
    write(&mut s, "CREATE (:`Research Paper` {title: 'On Computable Numbers'})");
    assert_eq!(
        scalars(&s, "MATCH (n:`Research Paper`) RETURN n.title AS t").len(),
        1,
        "the label could not be matched back"
    );
}

#[test]
fn a_relationship_type_with_a_space_works_too() {
    let mut s = store();
    write(&mut s, "CREATE (:N {n: 1}), (:N {n: 2})");
    write(
        &mut s,
        "MATCH (a:N {n: 1}), (b:N {n: 2}) CREATE (a)-[:`WORKS WITH` {since: 2020}]->(b)",
    );
    assert_eq!(
        scalars(&s, "MATCH ()-[r:`WORKS WITH`]->() RETURN r.since AS s").len(),
        1
    );
}

#[test]
fn the_backticks_do_not_survive_into_the_name() {
    // The #847 failure mode, one layer up: a name left with its delimiters
    // round-trips through the engine perfectly well and simply becomes a label
    // nobody can find under the name they wrote. `labels()` and `type()` are
    // where that would show.
    let mut s = store();
    write(&mut s, "CREATE (:`Research Paper`)");
    write(&mut s, "CREATE (:N {n: 1}), (:N {n: 2})");
    write(
        &mut s,
        "MATCH (a:N {n: 1}), (b:N {n: 2}) CREATE (a)-[:`WORKS WITH`]->(b)",
    );
    let labels = scalars(&s, "MATCH (n:`Research Paper`) RETURN labels(n) AS l");
    assert!(labels[0].contains("Research Paper"), "{labels:?}");
    assert!(!labels[0].contains('`'), "backticks leaked into the label: {labels:?}");
    let types = scalars(&s, "MATCH ()-[r:`WORKS WITH`]->() RETURN type(r) AS t");
    assert!(!types[0].contains('`'), "backticks leaked into the type: {types:?}");
}

#[test]
fn a_backticked_name_and_a_plain_one_are_the_same_name() {
    // The question a delimiter always raises. If they were different, a user
    // could write the same label two ways and get two labels, which is worse
    // than the parse error this replaced.
    let mut s = store();
    write(&mut s, "CREATE (:Person {name: 'Ada'})");
    assert_eq!(
        scalars(&s, "MATCH (n:`Person`) RETURN n.name AS n").len(),
        1,
        "a backticked spelling of a plain label did not match it"
    );
    write(&mut s, "CREATE (:`Person` {name: 'Alan'})");
    assert_eq!(
        scalars(&s, "MATCH (n:Person) RETURN n.name AS n").len(),
        2,
        "a label created with backticks is a different label"
    );
}

#[test]
fn no_read_site_leaves_a_backtick_in_a_name() {
    // The sweep, and the reason for it: a missed unescape site is **silent**.
    // The name is stored with its delimiters, every query that also writes it
    // with delimiters keeps working, and the only symptom is a label nobody
    // can find under the name they wrote. So the check is over every shape
    // that reads a label or a type, not over one of them.
    let mut s = store();
    write(&mut s, "CREATE (:`A B` {k: 1}), (:`A B` {k: 2}), (:`C D` {k: 3})");
    write(
        &mut s,
        "MATCH (a:`A B` {k: 1}), (b:`C D` {k: 3}) CREATE (a)-[:`E F`]->(b)",
    );

    let shapes = [
        "MATCH (n:`A B`) RETURN labels(n) AS x",
        "MATCH (n) WHERE n:`A B` RETURN labels(n) AS x",
        "MATCH (n:`A B`)-[r:`E F`]->(m:`C D`) RETURN type(r) AS x",
        "MATCH (n:`A B`) WITH n RETURN labels(n) AS x",
        "MATCH (n:`A B`) RETURN DISTINCT labels(n) AS x",
        "MATCH (n) WHERE n:`A B` OR n:`C D` RETURN labels(n) AS x",
        "MATCH (n:`A B`) RETURN labels(n) AS x ORDER BY n.k",
        "OPTIONAL MATCH (n:`A B`) RETURN labels(n) AS x",
        "MATCH (n:`A B`) UNWIND labels(n) AS x RETURN x",
        "MATCH ()-[r:`E F`]-() RETURN type(r) AS x",
    ];
    for q in shapes {
        let rows = scalars(&s, q);
        assert!(!rows.is_empty(), "{q} matched nothing");
        for row in &rows {
            assert!(
                !row.contains('`'),
                "a backtick survived into the result of `{q}`: {row}"
            );
            assert!(
                row.contains("A B") || row.contains("C D") || row.contains("E F"),
                "`{q}` returned something unexpected: {row}"
            );
        }
    }
}

#[test]
fn a_doubled_backtick_is_one_backtick() {
    // openCypher's escape for a literal backtick inside a delimited name.
    // `unescape_name` already did this for property keys; the point is that
    // labels go through the same function rather than a second copy of it.
    let mut s = store();
    write(&mut s, "CREATE (:`od``d` {k: 1})");
    let labels = scalars(&s, "MATCH (n:`od``d`) RETURN labels(n) AS l");
    assert_eq!(labels.len(), 1, "the doubled backtick did not match back");
    assert!(labels[0].contains("od`d"), "{labels:?}");
}

#[test]
fn an_ordinary_label_is_untouched() {
    // The half that keeps the grammar change honest: an alternation that
    // accidentally preferred `escaped_name` would break every query in the
    // suite, but a subtler mistake might only break the plain path here.
    let mut s = store();
    write(&mut s, "CREATE (:Person {name: 'Ada'})-[:KNOWS]->(:Person {name: 'Alan'})");
    assert_eq!(scalars(&s, "MATCH (n:Person) RETURN n.name AS n").len(), 2);
    assert_eq!(
        scalars(&s, "MATCH ()-[r:KNOWS]->() RETURN type(r) AS t"),
        vec!["Property(String(\"KNOWS\"))".to_string()]
    );
}

#[test]
fn an_unterminated_backtick_is_still_a_parse_error() {
    // Accepting a name is not the same as accepting anything. An open
    // delimiter that swallowed the rest of the query would turn a typo into a
    // query that runs.
    let mut s = store();
    let err = QueryEngine::new()
        .execute_mut("CREATE (:`Research Paper {k: 1})", &mut s, "default")
        .expect_err("an unterminated backtick must not parse");
    assert!(
        err.to_string().to_lowercase().contains("parse")
            || err.to_string().to_lowercase().contains("syntax"),
        "{err}"
    );
}
