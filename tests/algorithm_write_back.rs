//! `writeProperty` stores an algorithm's result on each node (ALGO-06, #1683).
//!
//! `CALL algo.pageRank({writeProperty: 'rank'})` used to be refused as an
//! unknown config key; before #1316 it was accepted and ignored, which looked
//! like success. These tests hold the write mode to what it promises:
//!
//! - every algorithm in `AlgorithmOperator::WRITE_BACK`, under every alias,
//!   stores exactly the value it streams, on exactly the nodes it streams;
//! - the rows it streams are the ones a plain call streams;
//! - the call is a write: routed to the write executor, refused by the read
//!   one, journalled for persistence, and undone whole when a write fails;
//! - a request it cannot honour is refused before anything is written.

use std::collections::BTreeMap;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::operator::AlgorithmOperator;
use samyama::query::executor::{QueryExecutor, Record};
use samyama::query::{parse_query, QueryEngine};

const TENANT: &str = "default";
const LABEL: &str = "N";
const WRITE: &str = "wb";

/// A directed 3-cycle with a tail, and a separate edge: two weak components,
/// a strongly connected one, nodes no path reaches, and a node with no
/// eccentricity. Every algorithm in the table has something to say about it.
const GRAPH: &str = "CREATE (n0:N {name: 'n0'}), (n1:N {name: 'n1'}), (n2:N {name: 'n2'}), \
     (n3:N {name: 'n3'}), (n4:N {name: 'n4'}), (n5:N {name: 'n5'}), (n6:N {name: 'n6'}), \
     (n0)-[:E]->(n1), (n1)-[:E]->(n2), (n2)-[:E]->(n0), (n2)-[:E]->(n3), (n3)-[:E]->(n4), \
     (n5)-[:E]->(n6)";

/// The same shape with the 3-cycle made a 4-cycle: `bipartite` refuses an odd
/// cycle, rightly, so it is given a graph that has two sides.
const BIPARTITE_GRAPH: &str =
    "CREATE (n0:N {name: 'n0'}), (n1:N {name: 'n1'}), (n2:N {name: 'n2'}), \
     (n3:N {name: 'n3'}), (n4:N {name: 'n4'}), (n5:N {name: 'n5'}), (n6:N {name: 'n6'}), \
     (n0)-[:E]->(n1), (n1)-[:E]->(n2), (n2)-[:E]->(n3), (n3)-[:E]->(n0), (n3)-[:E]->(n4), \
     (n5)-[:E]->(n6)";

/// A directed 5-cycle: every node reaches every other, so every node has an
/// eccentricity. On `GRAPH` none does, which would test only the null case.
const CONNECTED_GRAPH: &str =
    "CREATE (n0:N {name: 'n0'}), (n1:N {name: 'n1'}), (n2:N {name: 'n2'}), \
     (n3:N {name: 'n3'}), (n4:N {name: 'n4'}), \
     (n0)-[:E]->(n1), (n1)-[:E]->(n2), (n2)-[:E]->(n3), (n3)-[:E]->(n4), (n4)-[:E]->(n0)";

fn graph() -> (GraphStore, QueryEngine) {
    graph_from(GRAPH)
}

fn graph_from(create: &str) -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(create, &mut store, TENANT)
        .expect("build the graph");
    (store, engine)
}

/// The fixture each algorithm runs on.
fn graph_for(name: &str) -> (GraphStore, QueryEngine) {
    if name.starts_with("bipartite") {
        graph_from(BIPARTITE_GRAPH)
    } else if name == "eccentricity" {
        graph_from(CONNECTED_GRAPH)
    } else {
        graph()
    }
}

/// The id of the node named `name`.
fn id_of(store: &GraphStore, engine: &QueryEngine, name: &str) -> i64 {
    let r = engine
        .execute(
            &format!("MATCH (n:{LABEL} {{name: '{name}'}}) RETURN id(n) AS id"),
            store,
        )
        .expect("find a node");
    match prop(&r.records[0], "id") {
        PropertyValue::Integer(i) => i,
        other => panic!("id is {other:?}"),
    }
}

fn prop(record: &Record, column: &str) -> PropertyValue {
    record
        .get(column)
        .and_then(|v| v.as_property())
        .cloned()
        .unwrap_or(PropertyValue::Null)
}

/// The arguments before the config map, for the algorithms that need them.
fn leading_args(canonical: &str, store: &GraphStore, engine: &QueryEngine) -> String {
    let source = id_of(store, engine, "n0");
    match canonical {
        "bellmanford" => format!("{source}, "),
        "personalizedpagerank" | "personalisedpagerank" => format!("[{source}], "),
        // `strategy` is required, and a second config map is merged into the first.
        "split" | "traintestsplit" => "{strategy: 'random'}, ".to_string(),
        _ => String::new(),
    }
}

/// `CALL algo.<name>(<leading>{<config>})`, returning each row's node id and
/// `column` value.
fn call(
    store: &mut GraphStore,
    engine: &QueryEngine,
    name: &str,
    config: &str,
    column: &str,
) -> Result<BTreeMap<i64, PropertyValue>, String> {
    let lead = leading_args(name, store, engine);
    let q = format!(
        "CALL algo.{name}({lead}{{{config}}}) YIELD node, {column} \
         RETURN id(node) AS id, {column} AS v"
    );
    let r = engine
        .execute_mut(&q, store, TENANT)
        .map_err(|e| e.to_string())?;
    let mut rows = BTreeMap::new();
    for rec in &r.records {
        let PropertyValue::Integer(id) = prop(rec, "id") else {
            panic!("{q}: no id");
        };
        assert!(
            rows.insert(id, prop(rec, "v")).is_none(),
            "{q}: node {id} twice"
        );
    }
    Ok(rows)
}

/// Each node's `property`, absent ones as null.
fn stored(
    store: &GraphStore,
    engine: &QueryEngine,
    property: &str,
) -> BTreeMap<i64, PropertyValue> {
    let r = engine
        .execute(
            &format!("MATCH (n:{LABEL}) RETURN id(n) AS id, n.{property} AS v"),
            store,
        )
        .expect("read back");
    r.records
        .iter()
        .map(|rec| match prop(rec, "id") {
            PropertyValue::Integer(id) => (id, prop(rec, "v")),
            other => panic!("id is {other:?}"),
        })
        .collect()
}

#[test]
fn every_write_back_algorithm_stores_exactly_what_it_streams() {
    let mut checked = 0;
    for (names, column) in AlgorithmOperator::WRITE_BACK {
        for name in *names {
            let (mut store, engine) = graph_for(name);
            let streamed = call(&mut store, &engine, name, "", column)
                .unwrap_or_else(|e| panic!("{name}: the plain call fails: {e}"));
            let written_rows = call(
                &mut store,
                &engine,
                name,
                &format!("writeProperty: '{WRITE}'"),
                column,
            )
            .unwrap_or_else(|e| panic!("{name}: the write call fails: {e}"));
            assert_eq!(written_rows, streamed, "{name}: writing changed the rows");
            // An empty result would pass every comparison below and prove
            // nothing, so each algorithm must have written something.
            assert!(
                streamed.values().any(|v| !v.is_null()),
                "{name}: the fixture gives it nothing to write"
            );

            for (id, value) in stored(&store, &engine, WRITE) {
                let expected = streamed.get(&id).cloned().unwrap_or(PropertyValue::Null);
                assert_eq!(
                    value, expected,
                    "{name}: node {id} holds {value:?}, but the call streamed {expected:?}"
                );
            }
            checked += 1;
        }
    }
    // Every alias is a separate dispatch arm, and each was run.
    assert!(checked >= AlgorithmOperator::WRITE_BACK.len(), "{checked}");
}

#[test]
fn a_write_back_names_the_property_it_was_asked_for_and_no_other() {
    let (mut store, engine) = graph();
    call(
        &mut store,
        &engine,
        "pageRank",
        "writeProperty: 'rank'",
        "score",
    )
    .unwrap();
    assert!(stored(&store, &engine, "rank")
        .values()
        .all(|v| !v.is_null()));
    assert!(stored(&store, &engine, WRITE).values().all(|v| v.is_null()));
    // The algorithm's other config still applies.
    let fewer = call(
        &mut store,
        &engine,
        "pageRank",
        "iterations: 1, writeProperty: 'rank1'",
        "score",
    )
    .unwrap();
    assert_eq!(stored(&store, &engine, "rank1"), fewer);
}

#[test]
fn the_yielded_node_carries_the_written_property() {
    let (mut store, engine) = graph();
    let r = engine
        .execute_mut(
            "CALL algo.wcc({writeProperty: 'component'}) YIELD node, componentId \
             RETURN node.component AS stored, componentId",
            &mut store,
            TENANT,
        )
        .unwrap();
    assert!(!r.records.is_empty());
    for rec in &r.records {
        assert_eq!(prop(rec, "stored"), prop(rec, "componentId"));
    }
}

#[test]
fn the_yielded_node_value_itself_holds_the_written_property() {
    // `RETURN node.x` reads the store; `RETURN node` hands back the node value
    // the row carries, which was read before the write.
    let (mut store, engine) = graph();
    let r = engine
        .execute_mut(
            "CALL algo.pageRank({writeProperty: 'rank'}) YIELD node, score RETURN node, score",
            &mut store,
            TENANT,
        )
        .unwrap();
    assert!(!r.records.is_empty());
    for rec in &r.records {
        let node = match rec.get("node") {
            Some(samyama::query::executor::record::Value::Node(_, n)) => n,
            other => panic!("node is {other:?}"),
        };
        assert_eq!(node.properties.get("rank"), Some(&prop(rec, "score")));
    }
}

#[test]
fn a_label_filter_writes_only_the_nodes_it_scores() {
    let (mut store, engine) = graph();
    engine
        .execute_mut("CREATE (:Other {name: 'x'})", &mut store, TENANT)
        .unwrap();
    engine
        .execute_mut(
            "CALL algo.pageRank('N', {writeProperty: 'rank'})",
            &mut store,
            TENANT,
        )
        .unwrap();
    let r = engine
        .execute("MATCH (o:Other) RETURN o.rank AS rank", &store)
        .unwrap();
    assert!(
        prop(&r.records[0], "rank").is_null(),
        "a node outside the label was written"
    );
}

#[test]
fn a_second_run_overwrites_and_a_null_result_removes_a_stale_value() {
    let (mut store, engine) = graph();
    // A stale value on every node, as an earlier run would leave.
    engine
        .execute_mut(
            &format!("MATCH (n:{LABEL}) SET n.ecc = -1"),
            &mut store,
            TENANT,
        )
        .unwrap();
    let streamed = call(
        &mut store,
        &engine,
        "eccentricity",
        "writeProperty: 'ecc'",
        "eccentricity",
    )
    .unwrap();
    assert!(
        streamed.values().any(|v| v.is_null()),
        "the fixture needs a node with no eccentricity"
    );
    assert_eq!(stored(&store, &engine, "ecc"), streamed);
}

// ---------- it is a write ----------

const PAGERANK_WRITE: &str =
    "CALL algo.pageRank({writeProperty: 'rank'}) YIELD node RETURN count(*) AS n";

#[test]
fn a_write_back_call_is_classified_as_a_write_in_both_query_shapes() {
    let engine = QueryEngine::new();
    for q in [
        PAGERANK_WRITE,
        "CALL algo.pageRank({writeProperty: 'rank'})",
        // A CALL after another clause is parsed into the other AST shape.
        "WITH 1 AS x CALL algo.wcc({writeProperty: 'c'}) YIELD node RETURN count(*) AS n",
    ] {
        assert!(engine.statement_is_write(q).unwrap(), "{q} is not a write");
        assert!(parse_query(q).unwrap().is_write(), "{q}");
    }
    for q in [
        "CALL algo.pageRank() YIELD node RETURN count(*) AS n",
        "CALL algo.pageRank({iterations: 5}) YIELD node RETURN count(*) AS n",
    ] {
        assert!(!engine.statement_is_write(q).unwrap(), "{q} became a write");
    }
}

#[test]
fn the_read_executor_refuses_a_write_back_and_writes_nothing() {
    let (store, _) = graph();
    let query = parse_query(PAGERANK_WRITE).unwrap();
    let err = QueryExecutor::new(&store)
        .execute(&query)
        .expect_err("a write-back ran on the read path")
        .to_string();
    assert!(err.contains("requires write access"), "{err}");
    assert!(
        err.contains("rank"),
        "the refusal names the property: {err}"
    );
}

#[test]
fn a_write_back_is_journalled_for_persistence() {
    let (mut store, engine) = graph();
    let nodes = stored(&store, &engine, WRITE).len();
    store.enable_write_log();
    engine
        .execute_mut(PAGERANK_WRITE, &mut store, TENANT)
        .unwrap();
    let log = store.take_write_log();
    // One upsert per node written: what the server hands to the WAL.
    let upserts = log
        .iter()
        .filter(|m| format!("{m:?}").starts_with("NodeUpserted"))
        .count();
    assert_eq!(upserts, nodes, "{log:?}");
}

#[test]
fn a_write_the_store_refuses_undoes_the_whole_statement() {
    let (mut store, engine) = graph();
    // Every node in a component shares its componentId, so a unique
    // constraint on the written property must refuse the second one.
    engine
        .execute_mut(
            &format!("CREATE CONSTRAINT FOR (n:{LABEL}) REQUIRE n.component IS UNIQUE"),
            &mut store,
            TENANT,
        )
        .unwrap();
    let err = call(
        &mut store,
        &engine,
        "wcc",
        "writeProperty: 'component'",
        "componentId",
    )
    .expect_err("a duplicate under a unique constraint was written");
    assert!(err.contains("component"), "{err}");
    assert!(
        stored(&store, &engine, "component")
            .values()
            .all(|v| v.is_null()),
        "the nodes written before the refusal kept their values"
    );
}

// ---------- refusals ----------

#[test]
fn an_algorithm_with_no_value_per_node_refuses_write_property() {
    let (mut store, engine) = graph();
    let a = id_of(&store, &engine, "n0");
    let b = id_of(&store, &engine, "n4");
    for q in [
        format!("CALL algo.shortestPath({a}, {b}, {{writeProperty: 'p'}})"),
        "CALL algo.triangleCount({writeProperty: 'p'})".to_string(),
        "CALL algo.jaccard({writeProperty: 'p'})".to_string(),
        "CALL algo.hits({writeProperty: 'p'})".to_string(),
        "CALL algo.diameter({writeProperty: 'p'})".to_string(),
        "CALL algo.mst({writeProperty: 'p'})".to_string(),
        "CALL algo.voteRank({writeProperty: 'p'})".to_string(),
    ] {
        let err = engine
            .execute_mut(&q, &mut store, TENANT)
            .expect_err(&format!("{q} accepted writeProperty"))
            .to_string();
        assert!(
            err.contains("does not support `writeProperty`"),
            "{q}: {err}"
        );
    }
    assert!(stored(&store, &engine, "p").values().all(|v| v.is_null()));
}

#[test]
fn a_write_property_that_is_not_a_name_is_refused() {
    let (mut store, engine) = graph();
    for bad in ["''", "'   '", "5", "null", "['a']"] {
        let q = format!("CALL algo.pageRank({{writeProperty: {bad}}})");
        let err = engine
            .execute_mut(&q, &mut store, TENANT)
            .expect_err(&format!("{q} was accepted"))
            .to_string();
        assert!(err.contains("non-empty property name"), "{q}: {err}");
    }
}

#[test]
fn a_gds_stream_call_does_not_write() {
    let (mut store, engine) = graph();
    let err = engine
        .execute_mut(
            "CALL gds.pageRank.stream({writeProperty: 'rank'})",
            &mut store,
            TENANT,
        )
        .expect_err("a stream call wrote")
        .to_string();
    assert!(err.contains("stream"), "{err}");
    assert!(stored(&store, &engine, "rank")
        .values()
        .all(|v| v.is_null()));
}

#[test]
fn gds_write_mode_points_to_write_property() {
    let (mut store, engine) = graph();
    let err = engine
        .execute_mut(
            "CALL gds.pageRank.write({writeProperty: 'rank'})",
            &mut store,
            TENANT,
        )
        .expect_err("gds write mode is still not aliased")
        .to_string();
    assert!(err.contains("writeProperty"), "{err}");
    assert!(err.contains("algo.pagerank"), "{err}");
}

#[test]
fn mutate_is_refused_with_the_reason() {
    let (mut store, engine) = graph();
    let err = engine
        .execute_mut(
            "CALL algo.pageRank({mutateProperty: 'rank'})",
            &mut store,
            TENANT,
        )
        .expect_err("mutate ran")
        .to_string();
    assert!(err.contains("projection"), "{err}");
}

// ---------- docs ----------

/// The conventions document's write-back table, and the section it is in.
const DOC: &str = "docs/ALGORITHM-CONVENTIONS.md";
const DOC_SECTION: &str = "## Write-back";
const DOC_NAME_COLUMN: usize = 2;

#[test]
fn the_conventions_document_lists_every_write_back_algorithm() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(DOC);
    let doc = std::fs::read_to_string(&path).expect("read the conventions document");
    let start = doc
        .find(DOC_SECTION)
        .expect("the document has a write-back section");
    let section = &doc[start + DOC_SECTION.len()..];
    let section = &section[..section.find("\n## ").unwrap_or(section.len())];
    let listed: std::collections::BTreeSet<String> = section
        .lines()
        .filter(|l| l.starts_with('|'))
        .filter_map(|l| l.split('|').nth(DOC_NAME_COLUMN))
        .flat_map(|cell| cell.split(','))
        .map(|name| {
            name.chars()
                .filter(|c| c.is_alphanumeric())
                .flat_map(char::to_lowercase)
                .collect()
        })
        .collect();
    for (names, _) in AlgorithmOperator::WRITE_BACK {
        assert!(
            names.iter().any(|n| listed.contains(*n)),
            "{DOC}'s write-back table does not list {names:?}"
        );
    }
}
