//! Named graph projections, end to end through Cypher (ALGO-05, #1681).
//!
//! The property that matters most: an algorithm run on a projection returns
//! exactly the rows the same algorithm returns on the graph the projection was
//! built from. A projection that changed an answer would be a cache that lies.
//! The rest: the statements and what they report, the refusals, staleness and
//! refresh, read-only routing, memory accounting, and the result cache.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Record, Value};
use samyama::query::{parse_query, QueryEngine};

const TENANT: &str = "default";

/// Two people-and-knows triangles joined by a bridge, a cost on every KNOWS,
/// and one robot with a non-KNOWS edge that a filtered projection must leave out.
const GRAPH: &str = "CREATE \
     (a:Person {name: 'a'}), (b:Person {name: 'b'}), (c:Person {name: 'c'}), \
     (d:Person {name: 'd'}), (e:Person {name: 'e'}), (f:Person {name: 'f'}), \
     (r:Robot {name: 'r'}), \
     (a)-[:KNOWS {cost: 1.0}]->(b), (b)-[:KNOWS {cost: 2.0}]->(c), (c)-[:KNOWS {cost: 1.5}]->(a), \
     (d)-[:KNOWS {cost: 3.0}]->(e), (e)-[:KNOWS {cost: 1.0}]->(f), (f)-[:KNOWS {cost: 2.5}]->(d), \
     (c)-[:KNOWS {cost: 4.0}]->(d), (r)-[:OWNS]->(a)";

fn graph() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(GRAPH, &mut store, TENANT)
        .expect("build the graph");
    (store, engine)
}

fn run(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> Result<Vec<Record>, String> {
    engine
        .execute_mut(q, store, TENANT)
        .map(|r| r.records)
        .map_err(|e| e.to_string())
}

fn ok(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> Vec<Record> {
    run(store, engine, q).unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn refused(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> String {
    run(store, engine, q).expect_err(&format!("{q} was accepted"))
}

fn cell(r: &Record, column: &str) -> PropertyValue {
    r.get(column)
        .and_then(|v| v.as_property())
        .cloned()
        .unwrap_or(PropertyValue::Null)
}

fn int(r: &Record, column: &str) -> i64 {
    match cell(r, column) {
        PropertyValue::Integer(i) => i,
        other => panic!("`{column}` is {other:?}"),
    }
}

/// Rows as comparable text, in a stable order. Nodes compare by id, so a row
/// is equal when it names the same nodes with the same values.
fn rows_as_text(rows: &[Record]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|r| {
            let mut cells: Vec<String> = r
                .bindings()
                .iter()
                .map(|(k, v)| {
                    let shown = match v {
                        Value::Node(id, _) | Value::NodeRef(id) => format!("node#{}", id.as_u64()),
                        other => format!("{other:?}"),
                    };
                    format!("{k}={shown}")
                })
                .collect();
            cells.sort();
            cells.join(",")
        })
        .collect();
    out.sort();
    out
}

// ---------- the statements ----------

#[test]
fn create_show_refresh_and_drop_through_cypher() {
    let (mut s, e) = graph();
    let created = ok(
        &mut s,
        &e,
        "CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS WEIGHT cost",
    );
    assert_eq!(created.len(), 1);
    assert_eq!(
        cell(&created[0], "status"),
        PropertyValue::String("created".into())
    );
    assert_eq!(
        (int(&created[0], "nodes"), int(&created[0], "relationships")),
        (6, 7)
    );
    assert_eq!(
        cell(&created[0], "node_label"),
        PropertyValue::String("Person".into())
    );
    assert_eq!(
        cell(&created[0], "relationship_type"),
        PropertyValue::String("KNOWS".into())
    );
    assert_eq!(
        cell(&created[0], "weight_property"),
        PropertyValue::String("cost".into())
    );
    assert!(int(&created[0], "bytes") > 0);

    ok(&mut s, &e, "CREATE GRAPH PROJECTION everything");
    let shown = ok(&mut s, &e, "SHOW GRAPH PROJECTIONS");
    let names: Vec<PropertyValue> = shown.iter().map(|r| cell(r, "name")).collect();
    assert_eq!(
        names,
        [
            PropertyValue::String("everything".into()),
            PropertyValue::String("social".into())
        ]
    );
    assert_eq!(int(&shown[0], "nodes"), 7, "no filter: every node");

    let refreshed = ok(&mut s, &e, "REFRESH GRAPH PROJECTION social");
    assert_eq!(
        cell(&refreshed[0], "status"),
        PropertyValue::String("refreshed".into())
    );

    assert!(ok(&mut s, &e, "DROP GRAPH PROJECTION social").is_empty());
    assert_eq!(ok(&mut s, &e, "SHOW GRAPH PROJECTIONS").len(), 1);
}

#[test]
fn keywords_are_case_insensitive_and_names_can_be_escaped() {
    let (mut s, e) = graph();
    ok(
        &mut s,
        &e,
        "create graph projection `my graph` nodes :Person",
    );
    let shown = ok(&mut s, &e, "show graph projection");
    assert_eq!(
        cell(&shown[0], "name"),
        PropertyValue::String("my graph".into())
    );
    ok(
        &mut s,
        &e,
        "CALL algo.pageRank({graph: 'my graph'}) YIELD node RETURN count(*) AS n",
    );
    ok(&mut s, &e, "drop graph projection `my graph`");
}

#[test]
fn if_not_exists_and_if_exists() {
    let (mut s, e) = graph();
    ok(&mut s, &e, "CREATE GRAPH PROJECTION p NODES :Person");
    let err = refused(&mut s, &e, "CREATE GRAPH PROJECTION p");
    assert!(err.contains("already exists"), "{err}");
    let again = ok(&mut s, &e, "CREATE GRAPH PROJECTION p IF NOT EXISTS");
    assert_eq!(
        cell(&again[0], "status"),
        PropertyValue::String("already exists".into())
    );
    assert_eq!(
        cell(&again[0], "node_label"),
        PropertyValue::String("Person".into()),
        "the one already there"
    );

    ok(&mut s, &e, "DROP GRAPH PROJECTION p");
    let err = refused(&mut s, &e, "DROP GRAPH PROJECTION p");
    assert!(err.contains("no graph projection named `p`"), "{err}");
    ok(&mut s, &e, "DROP GRAPH PROJECTION p IF EXISTS");
    let err = refused(&mut s, &e, "REFRESH GRAPH PROJECTION p");
    assert!(err.contains("SHOW GRAPH PROJECTIONS"), "{err}");
}

// ---------- algorithms on a projection ----------

/// Each call, with `{config}` where the projection's config map goes and
/// `{plain}` the config a plain call needs to see the same graph. The whole
/// graph, so every algorithm sees the same view either way.
const SAME_ANSWER: &[(&str, &str)] = &[
    (
        "CALL algo.pageRank({config}) YIELD node, score RETURN node, score",
        "",
    ),
    (
        "CALL algo.wcc({config}) YIELD node, componentId RETURN node, componentId",
        "",
    ),
    (
        "CALL algo.scc({config}) YIELD node, componentId RETURN node, componentId",
        "",
    ),
    (
        "CALL algo.cdlp({config}) YIELD node, communityId RETURN node, communityId",
        "",
    ),
    (
        "CALL algo.lcc({config}) YIELD node, coefficient RETURN node, coefficient",
        "",
    ),
    (
        "CALL algo.triangleCount({config}) YIELD triangles RETURN triangles",
        "",
    ),
    (
        "CALL algo.degree({config}) YIELD node, score RETURN node, score",
        "",
    ),
    (
        "CALL algo.betweenness({config}) YIELD node, score RETURN node, score",
        "",
    ),
    (
        "CALL algo.closeness({config}) YIELD node, score RETURN node, score",
        "",
    ),
    (
        "CALL algo.kcore({config}) YIELD node, score RETURN node, score",
        "",
    ),
    (
        "CALL algo.louvain({config}) YIELD node, communityId RETURN node, communityId",
        "",
    ),
    (
        "CALL algo.katz({config}) YIELD node, score RETURN node, score",
        "",
    ),
    (
        "CALL algo.articleRank({config}) YIELD node, score RETURN node, score",
        "",
    ),
    (
        "CALL algo.hits({config}) YIELD node, hub, authority RETURN node, hub, authority",
        "",
    ),
    (
        "CALL algo.jaccard({config}) YIELD node1, node2, score RETURN node1, node2, score",
        "",
    ),
    (
        "CALL algo.colouring({config}) YIELD node, colour RETURN node, colour",
        "",
    ),
    (
        "CALL algo.diameter({config}) YIELD diameter RETURN diameter",
        "",
    ),
    (
        "CALL algo.bridges({config}) YIELD source, target RETURN source, target",
        "",
    ),
];

#[test]
fn every_algorithm_gives_the_same_answer_on_a_projection_of_the_whole_graph() {
    let (mut s, e) = graph();
    ok(&mut s, &e, "CREATE GRAPH PROJECTION everything");
    for (template, plain) in SAME_ANSWER {
        let direct = template.replace("{config}", if plain.is_empty() { "" } else { plain });
        let projected = template.replace("{config}", "{graph: 'everything'}");
        let want = rows_as_text(&ok(&mut s, &e, &direct));
        let got = rows_as_text(&ok(&mut s, &e, &projected));
        assert!(!want.is_empty(), "{direct}: no rows to compare");
        assert_eq!(got, want, "{projected} differs from {direct}");
    }
}

#[test]
fn a_filtered_projection_answers_as_the_filtered_call_does() {
    let (mut s, e) = graph();
    ok(
        &mut s,
        &e,
        "CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS",
    );
    for (direct, projected) in [
        (
            "CALL algo.pageRank('Person', 'KNOWS') YIELD node, score RETURN node, score",
            "CALL algo.pageRank({graph: 'social'}) YIELD node, score RETURN node, score",
        ),
        (
            "CALL algo.degree({label: 'Person', edgeType: 'KNOWS'}) YIELD node, score RETURN node, score",
            "CALL algo.degree({graph: 'social'}) YIELD node, score RETURN node, score",
        ),
        (
            "CALL algo.louvain({label: 'Person', edgeType: 'KNOWS'}) YIELD node, communityId RETURN node, communityId",
            "CALL algo.louvain({graph: 'social'}) YIELD node, communityId RETURN node, communityId",
        ),
    ] {
        let want = rows_as_text(&ok(&mut s, &e, direct));
        assert_eq!(rows_as_text(&ok(&mut s, &e, projected)), want, "{projected}");
        assert_eq!(want.len(), 6, "{direct}: the robot is left out");
    }
}

#[test]
fn a_weighted_projection_carries_its_weights() {
    // `mst` returns a summary row (total weight) and then one row per edge;
    // every column of every row is compared.
    let (mut s, e) = graph();
    ok(&mut s, &e, "CREATE GRAPH PROJECTION costs WEIGHT cost");
    let direct = rows_as_text(&ok(&mut s, &e, "CALL algo.mst('cost')"));
    let projected = rows_as_text(&ok(&mut s, &e, "CALL algo.mst({graph: 'costs'})"));
    assert!(
        direct.iter().any(|row| row.contains("total_weight")),
        "{direct:?}"
    );
    assert_eq!(projected, direct);
}

#[test]
fn a_filter_given_with_graph_is_refused_not_ignored() {
    let (mut s, e) = graph();
    ok(
        &mut s,
        &e,
        "CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS",
    );
    for q in [
        "CALL algo.degree({graph: 'social', label: 'Robot'})",
        "CALL algo.degree({graph: 'social', edgeType: 'OWNS'})",
        "CALL algo.pageRank('Robot', {graph: 'social'})",
        "CALL algo.mst('cost', {graph: 'social'})",
    ] {
        let err = refused(&mut s, &e, q);
        assert!(err.contains("already decides"), "{q}: {err}");
    }
}

#[test]
fn an_algorithm_that_cannot_use_a_projection_says_so() {
    let (mut s, e) = graph();
    ok(&mut s, &e, "CREATE GRAPH PROJECTION everything");
    for q in [
        "CALL algo.temporalReachability(1, {graph: 'everything'})",
        "CALL algo.pca({graph: 'everything'})",
        "CALL algo.or.solve({graph: 'everything'})",
    ] {
        let err = refused(&mut s, &e, q);
        assert!(
            err.contains("cannot run on a graph projection"),
            "{q}: {err}"
        );
    }
}

#[test]
fn an_unknown_or_malformed_graph_is_refused() {
    let (mut s, e) = graph();
    let err = refused(&mut s, &e, "CALL algo.pageRank({graph: 'nope'})");
    assert!(err.contains("no graph projection named `nope`"), "{err}");
    for bad in ["''", "5", "null", "['a']"] {
        let err = refused(&mut s, &e, &format!("CALL algo.pageRank({{graph: {bad}}})"));
        assert!(
            err.contains("must be a graph projection's name"),
            "{bad}: {err}"
        );
    }
}

#[test]
fn write_back_from_a_projection_writes_the_store() {
    let (mut s, e) = graph();
    ok(
        &mut s,
        &e,
        "CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS",
    );
    ok(
        &mut s,
        &e,
        "CALL algo.wcc({graph: 'social', writeProperty: 'component'})",
    );
    let rows = ok(
        &mut s,
        &e,
        "MATCH (n:Person) WHERE n.component IS NOT NULL RETURN count(n) AS n",
    );
    assert_eq!(int(&rows[0], "n"), 6);
    let robot = ok(&mut s, &e, "MATCH (n:Robot) RETURN n.component AS c");
    assert_eq!(
        cell(&robot[0], "c"),
        PropertyValue::Null,
        "outside the projection"
    );
}

// ---------- a snapshot, refreshed on request ----------

#[test]
fn a_projection_is_a_snapshot_until_refreshed() {
    let (mut s, e) = graph();
    ok(
        &mut s,
        &e,
        "CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS",
    );
    let count = |s: &mut GraphStore| {
        ok(
            s,
            &e,
            "CALL algo.pageRank({graph: 'social'}) YIELD node RETURN count(*) AS n",
        )
        .first()
        .map(|r| int(r, "n"))
        .unwrap()
    };
    assert_eq!(count(&mut s), 6);
    ok(&mut s, &e, "CREATE (:Person {name: 'g'})");
    assert_eq!(count(&mut s), 6, "writes do not reach it");
    let shown = ok(&mut s, &e, "SHOW GRAPH PROJECTIONS");
    assert_eq!(cell(&shown[0], "stale"), PropertyValue::Boolean(true));
    ok(&mut s, &e, "REFRESH GRAPH PROJECTION social");
    assert_eq!(count(&mut s), 7);
    let shown = ok(&mut s, &e, "SHOW GRAPH PROJECTIONS");
    assert_eq!(cell(&shown[0], "stale"), PropertyValue::Boolean(false));
}

#[test]
fn a_node_deleted_after_the_build_does_not_break_the_algorithm() {
    let (mut s, e) = graph();
    ok(
        &mut s,
        &e,
        "CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS",
    );
    ok(&mut s, &e, "MATCH (n:Person {name: 'f'}) DETACH DELETE n");
    // The projection still has six nodes; the deleted one has no node to return.
    let rows = ok(
        &mut s,
        &e,
        "CALL algo.pageRank({graph: 'social'}) YIELD node RETURN count(*) AS n",
    );
    assert!(int(&rows[0], "n") <= 6);
}

// ---------- read-only routing ----------

#[test]
fn only_show_is_a_read() {
    let e = QueryEngine::new();
    for q in [
        "CREATE GRAPH PROJECTION p",
        "REFRESH GRAPH PROJECTION p",
        "DROP GRAPH PROJECTION p",
        "DROP GRAPH PROJECTION p IF EXISTS",
    ] {
        assert!(
            e.statement_is_write(q).unwrap(),
            "{q} must need the write path"
        );
    }
    assert!(!e.statement_is_write("SHOW GRAPH PROJECTIONS").unwrap());
    // Running an algorithm on one only reads.
    assert!(!e
        .statement_is_write("CALL algo.pageRank({graph: 'p'}) YIELD node RETURN count(*)")
        .unwrap());
}

#[test]
fn the_read_executor_lists_and_runs_but_does_not_create() {
    let (mut s, e) = graph();
    ok(&mut s, &e, "CREATE GRAPH PROJECTION social");
    let read = |q: &str, s: &GraphStore| {
        QueryExecutor::new(s)
            .execute(&parse_query(q).unwrap())
            .map(|r| r.records.len())
            .map_err(|e| e.to_string())
    };
    assert_eq!(read("SHOW GRAPH PROJECTIONS", &s), Ok(1));
    assert!(read(
        "CALL algo.pageRank({graph: 'social'}) YIELD node RETURN node",
        &s
    )
    .is_ok());
    let err = read("CREATE GRAPH PROJECTION other", &s).unwrap_err();
    assert!(err.contains("read-only") || err.contains("write"), "{err}");
    assert!(s.graph_projections.get("other").is_none());
}

// ---------- memory ----------

#[test]
fn a_projection_is_accounted_in_index_memory_and_grows_with_the_graph() {
    let (mut s, e) = graph();
    ok(&mut s, &e, "CREATE GRAPH PROJECTION small NODES :Robot");
    ok(&mut s, &e, "CREATE GRAPH PROJECTION large");
    let memory = s.index_memory();
    let bytes = |name: &str| {
        memory
            .iter()
            .find(|m| m.kind == "graph_projection" && m.name.as_deref() == Some(name))
            .map(|m| m.bytes)
            .unwrap_or_else(|| panic!("{name} is not in the memory report"))
    };
    assert!(
        bytes("large") > bytes("small"),
        "{} vs {}",
        bytes("large"),
        bytes("small")
    );
    ok(&mut s, &e, "DROP GRAPH PROJECTION large");
    assert!(
        s.index_memory()
            .iter()
            .all(|m| m.name.as_deref() != Some("large")),
        "dropped, so released"
    );
}

// ---------- the result cache ----------

#[test]
fn a_cached_answer_is_not_served_after_the_projection_changes() {
    let (mut s, e) = graph();
    let q = "CALL algo.pageRank({graph: 'p'}) YIELD node RETURN count(*) AS n";
    let count = |s: &GraphStore| {
        let (batch, _) = e.execute_cached(q, s).expect(q);
        int(&batch.records[0], "n")
    };
    ok(&mut s, &e, "CREATE GRAPH PROJECTION p NODES :Robot");
    assert_eq!(count(&s), 1);
    let (_, hit) = e.execute_cached(q, &s).unwrap();
    assert!(hit, "the second call is served from the cache");
    // Same data; only the projection changes. (The epoch itself moves with
    // every write statement, data or not, so it is not what is compared.)
    let data = s.data_changes();
    ok(&mut s, &e, "DROP GRAPH PROJECTION p");
    ok(&mut s, &e, "CREATE GRAPH PROJECTION p NODES :Person");
    assert_eq!(s.data_changes(), data, "no data changed");
    assert_eq!(
        count(&s),
        6,
        "the answer from the old projection was served"
    );
}
