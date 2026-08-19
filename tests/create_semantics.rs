//! `CREATE` binds a variable once, and adjacent `CREATE` clauses are one create.
//!
//! Both defects here were found by running the openCypher TCK: 197 scenarios
//! were being **skipped** with "setup did not parse", and the reason was that
//! every TCK fixture is written as a run of `CREATE` clauses. Fixing the
//! parsing exposed the second, worse defect underneath it.
//!
//! ```cypher
//! CREATE (a), (b), (a)-[:R]->(b)
//! ```
//!
//! created **four** nodes. The third path re-registered `a` and `b` for
//! creation instead of reusing the ones bound a moment earlier, so the query
//! succeeded, the edge was correct, and the graph quietly had twice the nodes
//! it should. Nothing in the result signalled it — you have to count.
//!
//! This is the shape most fixtures and loaders are written in, which is what
//! makes it worth its own file.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("query should run");
}

fn scalar(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    match batch.records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn nodes(store: &GraphStore) -> i64 {
    scalar(store, "MATCH (n) RETURN count(n) AS c")
}

fn edges(store: &GraphStore) -> i64 {
    scalar(store, "MATCH ()-->() RETURN count(*) AS c")
}

#[test]
fn a_variable_reused_inside_one_create_refers_to_the_same_node() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (a), (b), (a)-[:R]->(b)");
    assert_eq!(nodes(&store), 2, "two nodes, not four");
    assert_eq!(edges(&store), 1);
}

#[test]
fn the_reused_node_keeps_the_labels_it_was_created_with() {
    // Reusing must not mean "create it again with the second mention's
    // labels", nor drop the first mention's.
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (a:A), (b:B), (a)-[:R]->(b)");
    assert_eq!(nodes(&store), 2);
    assert_eq!(scalar(&store, "MATCH (:A) RETURN count(*) AS c"), 1);
    assert_eq!(scalar(&store, "MATCH (:B) RETURN count(*) AS c"), 1);
    assert_eq!(scalar(&store, "MATCH (:A)-[:R]->(:B) RETURN count(*) AS c"), 1);
}

#[test]
fn adjacent_create_clauses_mean_the_same_as_one_comma_separated_create() {
    // The equivalence the parser relies on when it merges them.
    let mut separate = GraphStore::new();
    write(&mut separate, "CREATE (a) CREATE (b) CREATE (a)-[:R]->(b)");

    let mut combined = GraphStore::new();
    write(&mut combined, "CREATE (a), (b), (a)-[:R]->(b)");

    assert_eq!(nodes(&separate), nodes(&combined));
    assert_eq!(edges(&separate), edges(&combined));
    assert_eq!(nodes(&separate), 2);
    assert_eq!(edges(&separate), 1);
}

#[test]
fn a_run_of_create_clauses_building_a_small_graph_lands_the_right_counts() {
    // The fixture shape: one hub, several spokes, written as repeated
    // clauses exactly as the TCK writes them.
    let mut store = GraphStore::new();
    write(
        &mut store,
        "CREATE (hub:Hub {n: 'h'}) \
         CREATE (hub)-[:E]->(:Leaf {n: 'a'}) \
         CREATE (hub)-[:E]->(:Leaf {n: 'b'}) \
         CREATE (hub)-[:E]->(:Leaf {n: 'c'})",
    );
    assert_eq!(nodes(&store), 4, "one hub and three leaves");
    assert_eq!(edges(&store), 3);
    assert_eq!(scalar(&store, "MATCH (:Hub) RETURN count(*) AS c"), 1, "the hub is created once");
}

#[test]
fn a_variable_bound_by_match_is_not_recreated() {
    // The pre-existing rule this change had to preserve: MATCH-bound
    // variables were already reused, and the fix must not double-create them
    // or stop reusing them.
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:Person {n: 'p'})");
    write(&mut store, "MATCH (p:Person) CREATE (p)-[:OWNS]->(:Thing)");
    assert_eq!(nodes(&store), 2);
    assert_eq!(edges(&store), 1);
    assert_eq!(scalar(&store, "MATCH (:Person) RETURN count(*) AS c"), 1);
}

#[test]
fn anonymous_nodes_are_still_created_once_each() {
    // Anonymous nodes have no variable to compare, so each mention is a
    // distinct node. Deduplicating them would be the opposite bug.
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (), (), ()");
    assert_eq!(nodes(&store), 3);
}

#[test]
fn properties_on_the_first_mention_survive() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (a {v: 7}), (b), (a)-[:R]->(b)");
    assert_eq!(nodes(&store), 2);
    assert_eq!(scalar(&store, "MATCH (n) WHERE n.v = 7 RETURN count(*) AS c"), 1);
}

/// Every label on every node in the store, sorted, one entry per node.
fn label_sets(store: &GraphStore) -> Vec<Vec<String>> {
    (0..16u64)
        .map(samyama::graph::types::NodeId::from)
        .filter_map(|id| store.get_node(id))
        .map(|n| {
            let mut labels: Vec<String> = n.labels.iter().map(|l| l.as_str().to_string()).collect();
            labels.sort();
            labels
        })
        .collect()
}

#[test]
fn a_node_carries_exactly_the_labels_that_were_written() {
    // `create_node` takes one label and always inserts it, so every caller
    // building a node from a pattern had to invent one when the pattern had
    // none: CREATE passed `""` and MERGE passed the string `"Node"`. Both went
    // into the label index and the catalog, so an unlabelled node reported a
    // label, and `MATCH (n:Node)` matched nodes nobody had labelled (#625).
    for (cypher, expected) in [
        ("CREATE ({id: 0})", vec![Vec::<String>::new()]),
        ("MERGE ({id: 2})", vec![Vec::new()]),
        ("CREATE ()-[:R]->()", vec![Vec::new(), Vec::new()]),
        ("UNWIND [1, 2] AS x CREATE ({v: x})", vec![Vec::new(), Vec::new()]),
        ("CREATE (:A {id: 1})", vec![vec!["A".to_string()]]),
        ("CREATE (:A:B)", vec![vec!["A".to_string(), "B".to_string()]]),
        ("MERGE (:A:B)", vec![vec!["A".to_string(), "B".to_string()]]),
    ] {
        let mut store = GraphStore::new();
        write(&mut store, cypher);
        assert_eq!(label_sets(&store), expected, "labels after `{cypher}`");
    }
}

#[test]
fn an_unlabelled_node_is_not_findable_by_an_invented_label() {
    // The label index and the catalog are the reason this matters beyond
    // rendering: a phantom label is a phantom class with a real count, and the
    // anchor-choice cost model reads those counts.
    let mut store = GraphStore::new();
    write(&mut store, "CREATE ({id: 0})");
    write(&mut store, "MERGE ({id: 2})");

    let found = |cypher: &str| -> usize {
        let q = parse_query(cypher).expect("query should parse");
        QueryExecutor::new(&store).execute(&q).unwrap().records.len()
    };
    assert_eq!(found("MATCH (n) RETURN n"), 2, "both nodes exist");
    assert_eq!(found("MATCH (n:Node) RETURN n"), 0, "`Node` was never written");

    // The catalog is where a phantom label does real damage: it becomes a
    // class with a count, and the anchor-choice cost model reads those counts.
    for phantom in ["Node", ""] {
        assert_eq!(
            store.catalog().estimate_label_scan(&samyama::graph::types::Label::new(phantom)),
            0.0,
            "the catalog should not carry a `{phantom}` class"
        );
    }
}
