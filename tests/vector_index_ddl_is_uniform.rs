//! `CREATE VECTOR INDEX` accepts both spellings of the same statement.
//!
//! Every other index DDL here is written `ON :Label(prop)` — `CREATE INDEX`,
//! `DROP INDEX`. The vector index took only Neo4j 5's `FOR (n:L) ON (n.prop)`,
//! so the one statement a migrating user was most likely to write by analogy
//! with the others was a syntax error. The capability was always there; only
//! the spelling was missing.
//!
//! These tests ask for the *effect*, not the parse. A grammar alternative that
//! parses and then builds nothing is the failure mode worth guarding against:
//! an empty index reports success and returns no rows, which is
//! indistinguishable from a graph with no matching data.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

/// Four Doc nodes with 4-dimension embeddings.
fn store_with_embeddings() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..4 {
        let id = store.create_node("Doc");
        let f = i as f32 / 10.0;
        if let Some(n) = store.get_node_mut(id) {
            n.set_property("title", PropertyValue::String(format!("doc{i}")));
            n.set_property("emb", PropertyValue::Vector(vec![f, f, f, f]));
        }
    }
    store
}

fn searchable_nodes(store: &GraphStore) -> usize {
    let engine = QueryEngine::new();
    engine
        .execute(
            "CALL db.index.vector.queryNodes(\"Doc\", \"emb\", [0.1, 0.1, 0.1, 0.1], 4) \
             YIELD node, score RETURN node, score",
            store,
        )
        .expect("vector search must run")
        .records
        .len()
}

#[test]
fn the_plain_on_form_builds_a_searchable_index() {
    let mut store = store_with_embeddings();
    QueryEngine::new()
        .execute_mut(
            "CREATE VECTOR INDEX ON :Doc(emb) OPTIONS {dimensions: 4}",
            &mut store,
            "default",
        )
        .expect("`ON :Label(prop)` is the spelling every other index DDL uses");

    assert_eq!(
        searchable_nodes(&store),
        4,
        "the plain form must build the same index as the FOR form, not an empty one"
    );
}

#[test]
fn the_plain_on_form_takes_a_name() {
    // The name is optional in both spellings, and `index_name` must not swallow
    // the `ON` keyword when it is omitted — the same trap `FOR` fell into.
    let mut store = store_with_embeddings();
    QueryEngine::new()
        .execute_mut(
            "CREATE VECTOR INDEX docvec ON :Doc(emb) OPTIONS {dimensions: 4}",
            &mut store,
            "default",
        )
        .expect("a named index in the plain form must parse");

    assert_eq!(searchable_nodes(&store), 4);
}

#[test]
fn both_spellings_describe_the_same_index() {
    // Break-test for the two tests above: if the plain form quietly built
    // something else — a different property, a different dimension count — both
    // would still pass, because both only ask that *some* index answers. This
    // asks that SHOW INDEXES reports the same row for either spelling.
    let engine = QueryEngine::new();
    let describe = |ddl: &str| {
        let mut store = store_with_embeddings();
        engine.execute_mut(ddl, &mut store, "default").unwrap();
        let rows = engine.execute("SHOW INDEXES", &store).unwrap();
        let mut described: Vec<String> = rows
            .records
            .iter()
            .map(|r| format!("{:?}", r.values().collect::<Vec<_>>()))
            .collect();
        described.sort();
        described
    };

    let from_for = describe("CREATE VECTOR INDEX v FOR (d:Doc) ON (d.emb) OPTIONS {dimensions: 4}");
    let from_on = describe("CREATE VECTOR INDEX v ON :Doc(emb) OPTIONS {dimensions: 4}");

    assert!(
        !from_for.is_empty(),
        "SHOW INDEXES reported nothing for either form, so this comparison proves nothing"
    );
    assert_eq!(
        from_for, from_on,
        "the two spellings must produce the same index, not merely both produce one"
    );
}

#[test]
fn show_indexes_lists_the_vector_index() {
    // Found by the assertion in `both_spellings_describe_the_same_index`:
    // SHOW INDEXES listed only BTREE property indexes, so a vector index that
    // had been created looked exactly like one that had not. The only other way
    // to tell was to search it, and an empty search result is also what a
    // correct index over no matching data returns.
    let mut store = store_with_embeddings();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE VECTOR INDEX FOR (d:Doc) ON (d.emb) OPTIONS {dimensions: 4}",
            &mut store,
            "default",
        )
        .unwrap();

    let rows = engine.execute("SHOW INDEXES", &store).unwrap();
    let listed: Vec<String> = rows
        .records
        .iter()
        .map(|r| format!("{:?}", r.values().collect::<Vec<_>>()))
        .collect();

    assert!(
        listed
            .iter()
            .any(|r| r.contains("VECTOR") && r.contains("emb")),
        "SHOW INDEXES must list the vector index; got {listed:?}"
    );
}

#[test]
fn the_options_still_apply_in_the_plain_form() {
    // `dimensions` is the option a caller is most likely to get wrong, and
    // silently defaulting it to 1536 is #474: the mismatch surfaces later
    // against the caller's vector, which is the wrong thing to blame. A form
    // that parsed the options and dropped them would pass every test above.
    let mut store = store_with_embeddings();
    let err = QueryEngine::new()
        .execute_mut(
            "CREATE VECTOR INDEX ON :Doc(emb) OPTIONS {dimension: 4}",
            &mut store,
            "default",
        )
        .expect_err("`dimension` is not an accepted option in either spelling");
    // Not just "the message mentions `dimension`": before the plain form was
    // accepted, this query failed with a *parse* error that quoted the query
    // text back, which contains the word. The assertion passed for the wrong
    // reason. Ask for the option check specifically.
    let err = format!("{err}");
    assert!(
        err.contains("unknown option") && err.contains("Accepted options"),
        "the option check must run in the plain form, not be pre-empted by a parse error; got: {err}"
    );
}
