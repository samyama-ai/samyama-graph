//! The EXPLAIN tree must not stop before the plan does (#1826).
//!
//! `PhysicalOperator::describe` has a default — `name: "Unknown"`, no details
//! and **no children** — so an operator that does not implement it truncates
//! the plan tree at itself. Everything it reads from becomes invisible, and
//! every assertion of the form "the plan does not contain X" over such a tree
//! is satisfied by the empty tree whatever the planner did: a check that cannot
//! fail.
//!
//! Eleven operators took that default, all of them on the write and DDL paths
//! (`CREATE`, `MERGE`, `FOREACH`, index DDL, `CALL <algo>`), so the shape that
//! reaches the clause pipeline — `CREATE … WITH … MATCH …` — was exactly the
//! shape whose plan could not be read. `MATCH (a:P) … CREATE (:Marker) WITH …`
//! printed three lines and stopped at `Unknown`.
//!
//! These tests assert two things that together close the class: that no write
//! or DDL query prints an `Unknown` node, and that the tree below the write
//! operator reaches the scans. The second is the one that makes a negative
//! assertion elsewhere non-vacuous — a populated tree is what gives
//! `!plan.contains("IndexScan")` something to be false about.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{undescribed_plan_lines, OperatorDescription, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Two `:P` nodes, `n` = "a" and "b", so a two-MATCH pipeline query has one
/// row per arm.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    for n in ["a", "b"] {
        let id = store.create_node("P");
        let _ = store.set_node_property(
            "default",
            id,
            "n".to_string(),
            PropertyValue::String(n.to_string()),
        );
    }
    store
}

/// The plan tree, without the trailing statistics block.
fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("EXPLAIN should run");
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => {
            t.lines().take_while(|l| !l.starts_with("---")).collect::<Vec<_>>().join("\n")
        }
        other => panic!("expected a plan string, got {other:?}"),
    }
}

/// Every write and DDL shape whose operator took the `describe` default.
///
/// One query per operator: `CreateNode`, `CreateNodesAndEdges`,
/// `MatchCreateEdge` (both arms — a bound-to-bound edge and a pattern node the
/// MATCH did not bind), `Merge` (leaf and piped), `MatchMergeEdge`, `Foreach`,
/// `CreateIndex`, `CreateVectorIndex`, `DropFullTextIndex`, `Algorithm`.
const WRITES: [&str; 12] = [
    "CREATE (n:Person {name: 'x', age: 3}) RETURN n",
    "CREATE (a:A)-[r:R {w: 2}]->(b:B) RETURN r",
    "MATCH (a:P), (b:P) CREATE (a)-[r:KNOWS]->(b) RETURN r",
    "MATCH (a:P) CREATE (a)-[:HAS]->(c:C {k: 1}) RETURN c",
    "MERGE (n:P {n: 'a'}) ON CREATE SET n.c = 1 ON MATCH SET n.m = 2 RETURN n",
    "MATCH (a:P) WITH a UNWIND [1, 2] AS x MERGE (:N {v: x}) RETURN x",
    "MATCH (a:P), (b:P) MERGE (a)-[r:R]-(b) RETURN r",
    "MATCH (a:P) FOREACH (x IN [1, 2] | SET a.v = x)",
    "CREATE INDEX ON :P(n)",
    "CREATE VECTOR INDEX vi FOR (n:P) ON (n.emb) OPTIONS {dimensions: 4, similarity: 'cosine'}",
    "DROP FULLTEXT INDEX ft",
    "CALL algo.pagerank()",
];

#[test]
fn no_write_or_ddl_query_prints_an_undescribed_operator() {
    let store = fixture();
    for cypher in WRITES {
        let text = plan(&store, cypher);
        assert!(
            undescribed_plan_lines(&text).is_empty(),
            "{cypher} plans an operator with no describe:\n{text}"
        );
        // An operator that printed nothing at all would also pass the check
        // above. The plan is at least one named line.
        assert!(!text.trim().is_empty(), "{cypher} planned an empty tree");
    }
}

#[test]
fn the_write_operator_in_a_pipeline_keeps_its_input_in_the_tree() {
    // The query from #1826: `CREATE … WITH … MATCH …` is the shape that reaches
    // the clause pipeline, and the create operator sits between the projection
    // and both scans. Before the fix the tree was three lines ending `Unknown`.
    let store = fixture();
    let text = plan(
        &store,
        "MATCH (a:P) WHERE a.n = 'a' MATCH (b:P) WHERE b.n = 'b' \
         CREATE (:Marker) WITH a.n AS u, b.n AS v RETURN u, v",
    );
    assert!(undescribed_plan_lines(&text).is_empty(), "{text}");
    assert!(text.contains("MatchCreateEdge"), "{text}");
    // What the truncation hid: the join and both of its scans.
    assert!(text.contains("CartesianProduct"), "{text}");
    assert_eq!(
        text.matches("Scan (var=").count(),
        2,
        "both MATCH arms should be visible below the create:\n{text}"
    );
    // Depth, not just presence: the scans are below the create operator, which
    // is what makes an assertion about them an assertion about this plan.
    let create = text.find("MatchCreateEdge").expect("create operator");
    assert!(text[create..].contains("CartesianProduct"), "{text}");
}

#[test]
fn a_write_pipeline_plan_can_be_asserted_about_negatively() {
    // The pattern PR #1821's `no_predicate_pushdown_in_the_pipeline_inline_arm`
    // uses: "this plan has no IndexScan". Over a truncated tree that holds for
    // free. Guarding it with `undescribed_plan_lines` plus a populated-tree
    // check is what gives it something to be false about.
    let store = fixture();
    let text = plan(
        &store,
        "MATCH (a:P) WHERE a.n = 'a' CREATE (:Marker) WITH a.n AS u RETURN u",
    );
    assert!(undescribed_plan_lines(&text).is_empty(), "{text}");
    assert!(text.contains("Scan (var=a"), "the scan must be visible:\n{text}");
    assert!(!text.contains("IndexScan"), "there is no index on :P(n):\n{text}");
}

#[test]
fn the_detector_names_an_undescribed_operator_and_its_path() {
    // The guard the tests above lean on, checked against a tree that has what
    // it looks for -- otherwise the guard itself is the thing that cannot fail.
    let unknown = OperatorDescription {
        name: "Unknown".to_string(),
        details: String::new(),
        children: Vec::new(),
    };
    let tree = OperatorDescription {
        name: "Project".to_string(),
        details: "u".to_string(),
        children: vec![unknown],
    };
    assert_eq!(tree.unknown_nodes(), vec!["Project -> Unknown".to_string()]);
    assert_eq!(undescribed_plan_lines(&tree.format(0)), vec!["+- Unknown".to_string()]);
    // And it does not cry wolf on a tree with none.
    let described = OperatorDescription {
        name: "Project".to_string(),
        details: "u".to_string(),
        children: vec![OperatorDescription {
            name: "NodeScan".to_string(),
            details: "var=a".to_string(),
            children: Vec::new(),
        }],
    };
    assert!(described.unknown_nodes().is_empty());
    assert!(undescribed_plan_lines(&described.format(0)).is_empty());
}
