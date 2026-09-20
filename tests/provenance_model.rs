//! A row knows where it came from, and an export can act on it
//! (TRUST-03, ML-09, EVAL-10).
//!
//! The provenance lives in properties, so these tests write it the way a user
//! would — through Cypher — rather than through a back door the rest of the
//! engine does not have.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::provenance::{
    self, derivation_path, redistributable, screen, ExportPolicy, Redistributable,
};
use samyama::query::QueryEngine;

fn run(store: &mut GraphStore, q: &str) {
    QueryEngine::new()
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

/// A node's string property, unquoted. `Display` for `PropertyValue::String`
/// adds quotes -- it renders Cypher literals -- so comparing `to_string()`
/// against a bare name never matches.
fn string_prop(store: &GraphStore, id: NodeId, key: &str) -> Option<String> {
    match store.node_properties_merged(id).get(key) {
        Some(PropertyValue::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn node_named(store: &GraphStore, key: &str, value: &str) -> NodeId {
    store
        .all_nodes()
        .iter()
        .find(|n| string_prop(store, n.id, key).as_deref() == Some(value))
        .unwrap_or_else(|| panic!("no node with {key} = {value}"))
        .id
}

fn ids(store: &GraphStore) -> Vec<NodeId> {
    store.all_nodes().iter().map(|n| n.id).collect()
}

#[test]
fn provenance_written_in_cypher_is_read_back_by_the_model() {
    // The property is set through the executor, which writes to the columnar
    // side. Reading `node.properties` instead of the merged view is #554, and
    // it would make every fact here look unprovenanced.
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE (:Fact {name: 'f1', __source_uri: 'https://wikidata.org', \
         __source_version: 'dump-2026-08-01', __retrieved_at: '2026-08-02T10:00:00Z', \
         __license: 'CC0-1.0', __redistributable: true})",
    );

    let id = ids(&store)[0];
    let p = provenance::of(&store, id);
    assert_eq!(p.source_uri.as_deref(), Some("https://wikidata.org"));
    assert_eq!(p.source_version.as_deref(), Some("dump-2026-08-01"));
    assert_eq!(p.license.as_deref(), Some("CC0-1.0"));
    assert!(p.is_reproducible());
    assert_eq!(redistributable(&store, id), Redistributable::Yes);
}

#[test]
fn an_unmarked_row_is_unknown_not_permitted() {
    // The decision this whole module turns on. An existing graph has no
    // provenance on any row, and reading that silence as permission would mark
    // a whole database redistributable because nobody said otherwise.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:Fact {name: 'old'})");
    let id = ids(&store)[0];

    assert_eq!(redistributable(&store, id), Redistributable::Unknown);
    assert!(provenance::of(&store, id).is_empty());
}

#[test]
fn a_string_false_is_not_read_as_unknown() {
    // CSV, JSON and RDF all deliver booleans as text. A row that says "false"
    // and is read as unknown leaves under `WithholdForbidden` — a licence
    // breach caused entirely by the type the importer happened to use.
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE (:Fact {name: 'a', __redistributable: 'false'})",
    );
    run(
        &mut store,
        "CREATE (:Fact {name: 'b', __redistributable: 'TRUE'})",
    );
    run(
        &mut store,
        "CREATE (:Fact {name: 'c', __redistributable: 'maybe'})",
    );

    let by_name =
        |n: &str| -> Redistributable { redistributable(&store, node_named(&store, "name", n)) };
    assert_eq!(by_name("a"), Redistributable::No);
    assert_eq!(by_name("b"), Redistributable::Yes);
    assert_eq!(
        by_name("c"),
        Redistributable::Unknown,
        "an unrecognised word must not become a yes"
    );
}

#[test]
fn the_three_policies_differ_exactly_on_the_unmarked_row() {
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE (:F {n: 'yes', __redistributable: true}) \
         CREATE (:F {n: 'no', __redistributable: false}) \
         CREATE (:F {n: 'unknown'})",
    );
    let all = ids(&store);

    let (kept, report) = screen(&store, &all, ExportPolicy::CountOnly);
    assert_eq!(
        kept.len(),
        3,
        "the default must not change what an export emits"
    );
    assert_eq!(report.withheld, 0);
    assert_eq!(report.marked_redistributable, 1);
    assert_eq!(report.marked_not_redistributable, 1);
    assert_eq!(report.unmarked, 1);

    let (kept, report) = screen(&store, &all, ExportPolicy::WithholdForbidden);
    assert_eq!(kept.len(), 2, "the unmarked row still leaves");
    assert_eq!(report.withheld, 1);

    let (kept, report) = screen(&store, &all, ExportPolicy::RequirePermission);
    assert_eq!(
        kept.len(),
        1,
        "only the explicit yes: this is the one policy that is a guarantee"
    );
    assert_eq!(report.withheld, 2);
}

#[test]
fn the_licence_report_lists_the_licences_it_saw() {
    // ML-09: an exported row carries its source and licence. Which licences
    // are in play is the first question anyone redistributing a graph has, and
    // a count does not answer it.
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE (:F {__license: 'CC0-1.0', __source_uri: 'a', __source_version: '1'}) \
         CREATE (:F {__license: 'CC-BY-SA-4.0', __source_uri: 'b'}) \
         CREATE (:F {__license: 'CC0-1.0'})",
    );
    let (_, report) = screen(&store, &ids(&store), ExportPolicy::CountOnly);

    assert_eq!(
        report.licenses,
        vec!["CC-BY-SA-4.0".to_string(), "CC0-1.0".to_string()],
        "distinct and sorted, so two reports of one graph agree"
    );
    assert_eq!(
        report.reproducible, 1,
        "a source URI without a version names no dump anyone can fetch"
    );
}

#[test]
fn the_derivation_path_returns_every_step_with_its_provenance() {
    // EVAL-10. The derived fact records nothing of its own; the chain is how
    // anyone finds out what it rests on.
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE (d:Derived {n: 'avg_income'}) \
         CREATE (s1:Source {n: 'census', __source_uri: 'https://census.gov', \
                            __source_version: '2026', __license: 'CC0-1.0', \
                            __redistributable: true}) \
         CREATE (s2:Source {n: 'survey', __source_uri: 'https://survey.example', \
                            __source_version: 'v3', __license: 'proprietary', \
                            __redistributable: false}) \
         CREATE (d)-[:DERIVED_FROM]->(s1) \
         CREATE (d)-[:DERIVED_FROM]->(s2)",
    );

    let derived = store
        .all_nodes()
        .iter()
        .find(|n| n.labels.iter().any(|l| l.as_str() == "Derived"))
        .unwrap()
        .id;

    let path = derivation_path(&store, derived);
    assert_eq!(path.len(), 3, "the fact and both of its sources");
    assert_eq!(path[0].depth, 0);
    assert!(
        path[0].provenance.is_empty(),
        "a step that recorded nothing is still a step; omitting it shortens the chain"
    );
    assert_eq!(path[0].redistributable, Redistributable::Unknown);

    let sources: Vec<&samyama::provenance::Derivation> =
        path.iter().filter(|d| d.depth == 1).collect();
    assert_eq!(sources.len(), 2);
    assert!(sources
        .iter()
        .any(|s| s.provenance.license.as_deref() == Some("proprietary")
            && s.redistributable == Redistributable::No));
    assert!(sources
        .iter()
        .any(|s| s.provenance.source_version.as_deref() == Some("2026")));
}

#[test]
fn a_cycle_in_the_derivation_terminates() {
    // A derivation graph is a claim made by whoever built it, and nothing
    // stops them claiming a cycle. Without the visited set this hangs, which
    // is the worst way for a governance query to fail.
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE (a:F {n: 'a'}) CREATE (b:F {n: 'b'}) \
         CREATE (a)-[:DERIVED_FROM]->(b) CREATE (b)-[:DERIVED_FROM]->(a)",
    );
    let start = ids(&store)[0];
    let path = derivation_path(&store, start);
    assert_eq!(path.len(), 2, "each node once: {path:?}");
}

#[test]
fn a_node_that_derives_from_nothing_is_its_own_whole_path() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:F {n: 'root', __license: 'CC0-1.0'})");
    let path = derivation_path(&store, ids(&store)[0]);
    assert_eq!(path.len(), 1);
    assert_eq!(path[0].depth, 0);
    assert_eq!(path[0].provenance.license.as_deref(), Some("CC0-1.0"));
}

#[test]
fn only_derived_from_edges_are_followed() {
    // A provenance chain that walked every edge would return the whole
    // connected component and call it a derivation — an answer that is always
    // available and never true.
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE (a:F {n: 'a'}) CREATE (b:F {n: 'b'}) CREATE (c:F {n: 'c'}) \
         CREATE (a)-[:DERIVED_FROM]->(b) CREATE (a)-[:MENTIONS]->(c)",
    );
    let a = node_named(&store, "n", "a");
    let path = derivation_path(&store, a);
    assert_eq!(path.len(), 2, "b is derived from; c is merely mentioned");
}

#[test]
fn the_reserved_keys_are_the_ones_the_model_reads() {
    // `KEYS` exists so an export can strip or carry the set. If it drifted
    // from what `of()` reads, an export would strip four of five and leave a
    // stray `__license` in a file it thought it had cleaned.
    let mut store = GraphStore::new();
    run(
        &mut store,
        "CREATE (:F {__source_uri: 'a', __source_version: 'b', \
         __retrieved_at: 'c', __license: 'd', __redistributable: true})",
    );
    let props = store.node_properties_merged(ids(&store)[0]);
    for k in provenance::KEYS {
        assert!(props.contains_key(k), "{k} is reserved but never written");
    }
    let p = provenance::of(&store, ids(&store)[0]);
    assert_eq!(p.source_uri.as_deref(), Some("a"));
    assert_eq!(p.source_version.as_deref(), Some("b"));
    assert_eq!(p.retrieved_at.as_deref(), Some("c"));
    assert_eq!(p.license.as_deref(), Some("d"));
}
