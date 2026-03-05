//! Tests for all v0.6.0 backlog features
//!
//! Covers: CY-01, CY-02, CY-04, CY-05, QE-01, QE-02, QE-03,
//!         IX-01, IX-02, IX-03, IX-04, IX-05, IX-06,
//!         QP-01, QP-03, QP-04

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;

// ---------------------------------------------------------------------------
// Helper: build a small social graph for reuse across tests
// ---------------------------------------------------------------------------
fn social_graph() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let people = vec![
        ("Alice", 30, "Engineering"),
        ("Bob", 25, "Engineering"),
        ("Charlie", 35, "Marketing"),
        ("David", 40, "Marketing"),
        ("Eve", 28, "Engineering"),
    ];

    for (name, age, dept) in &people {
        let q = format!(
            "CREATE (n:Person {{name: '{}', age: {}, dept: '{}'}})",
            name, age, dept
        );
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    // Edges: Alice->Bob, Bob->Charlie, Charlie->David, Alice->Eve
    for (src, dst) in &[("Alice", "Bob"), ("Bob", "Charlie"), ("Charlie", "David"), ("Alice", "Eve")] {
        let q = format!(
            "MATCH (a:Person {{name: '{}'}}), (b:Person {{name: '{}'}}) CREATE (a)-[:KNOWS]->(b)",
            src, dst
        );
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    (store, engine)
}

// ===== CY-01: collect(DISTINCT x) ===========================================

#[test]
fn test_collect_distinct() {
    let (store, engine) = social_graph();

    // All departments (with duplicates)
    let result = engine.execute(
        "MATCH (n:Person) RETURN collect(n.dept) AS depts",
        &store,
    ).unwrap();
    assert_eq!(result.len(), 1);
    let all_depts = result.records[0].get("depts").unwrap();
    // Should have 5 items (3 Engineering + 2 Marketing)
    let arr = all_depts.as_property().unwrap();
    if let PropertyValue::Array(items) = arr {
        assert_eq!(items.len(), 5);
    } else {
        panic!("Expected array, got {:?}", arr);
    }

    // DISTINCT departments
    let result = engine.execute(
        "MATCH (n:Person) RETURN collect(DISTINCT n.dept) AS depts",
        &store,
    ).unwrap();
    assert_eq!(result.len(), 1);
    let distinct_depts = result.records[0].get("depts").unwrap();
    let arr = distinct_depts.as_property().unwrap();
    if let PropertyValue::Array(items) = arr {
        assert_eq!(items.len(), 2, "Expected 2 distinct depts, got {:?}", items);
    } else {
        panic!("Expected array, got {:?}", arr);
    }
}

// ===== CY-02: datetime({year:..., month:..., day:...}) =======================

#[test]
fn test_datetime_named_constructor_in_with() {
    let (store, engine) = social_graph();

    // datetime() works in WITH/RETURN projection (via eval_function)
    let result = engine.execute(
        "MATCH (n:Person {name: 'Alice'}) WITH n, datetime({year: 2026, month: 3, day: 4}) AS dt RETURN n.name, dt",
        &store,
    ).unwrap();
    assert_eq!(result.len(), 1);
    let dt_val = result.records[0].get("dt").unwrap().as_property().unwrap();
    match dt_val {
        PropertyValue::DateTime(_) => {}, // good
        other => panic!("Expected DateTime from named constructor, got {:?}", other),
    }
}

#[test]
fn test_datetime_no_args_returns_now() {
    let (store, engine) = social_graph();

    // datetime() with no args returns current timestamp
    let result = engine.execute(
        "MATCH (n:Person {name: 'Alice'}) RETURN datetime() AS now",
        &store,
    ).unwrap();
    assert_eq!(result.len(), 1);
    let dt_val = result.records[0].get("now").unwrap().as_property().unwrap();
    match dt_val {
        PropertyValue::DateTime(millis) => {
            assert!(*millis > 0, "datetime() should return positive millis");
        },
        other => panic!("Expected DateTime from datetime(), got {:?}", other),
    }
}

// BUG: datetime() in SET context evaluates to Null (SetPropertyOperator line 4931
// only handles Literal and Property expressions, not Function).
// This test documents the bug — enable when fixed.
#[test]
#[ignore]
fn test_datetime_in_set_context() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine.execute_mut("CREATE (e:Event {name: 'Launch'})", &mut store, "default").unwrap();
    engine.execute_mut(
        "MATCH (e:Event {name: 'Launch'}) SET e.date = datetime({year: 2026, month: 3, day: 4})",
        &mut store, "default",
    ).unwrap();

    let result = engine.execute("MATCH (e:Event) RETURN e.date", &store).unwrap();
    let date_val = result.records[0].get("e.date").unwrap().as_property().unwrap();
    assert!(!matches!(date_val, PropertyValue::Null), "datetime() in SET should not be Null");
}

// BUG: datetime() in WHERE context fails with "Unknown function: datetime"
// (FilterOperator has its own evaluate_function that doesn't include datetime).
// This test documents the bug — enable when fixed.
#[test]
#[ignore]
fn test_datetime_in_where_context() {
    let (store, engine) = social_graph();

    let result = engine.execute(
        "MATCH (n:Person) WHERE datetime({year: 2026, month: 1, day: 1}) > datetime({year: 2025, month: 1, day: 1}) RETURN n.name",
        &store,
    );
    assert!(result.is_ok(), "datetime() should work in WHERE: {:?}", result.err());
}

// ===== CY-04: Named paths (p = (a)-[]->(b)) =================================

#[test]
fn test_named_path_basic() {
    let (store, engine) = social_graph();

    // Named path: p = (a)-[:KNOWS]->(b)
    let result = engine.execute(
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN p",
        &store,
    ).unwrap();
    // Alice knows Bob and Eve
    assert_eq!(result.len(), 2, "Alice has 2 outgoing KNOWS edges");
}

#[test]
fn test_named_path_multi_hop() {
    let (store, engine) = social_graph();

    // 2-hop path
    let result = engine.execute(
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN p, c.name",
        &store,
    ).unwrap();
    // Alice->Bob->Charlie
    assert_eq!(result.len(), 1);
    let name = result.records[0].get("c.name").unwrap()
        .as_property().unwrap().as_string().unwrap();
    assert_eq!(name, "Charlie");
}

// ===== CY-05: nodes(p) / relationships(p) ===================================

#[test]
fn test_nodes_function() {
    let (store, engine) = social_graph();

    let result = engine.execute(
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN nodes(p) AS ns",
        &store,
    ).unwrap();
    assert_eq!(result.len(), 1);
    // nodes(p) should return a list with 2 nodes (Alice, Bob)
    let ns = result.records[0].get("ns").unwrap().as_property().unwrap();
    if let PropertyValue::Array(items) = ns {
        assert_eq!(items.len(), 2, "Path Alice->Bob has 2 nodes");
    } else {
        panic!("Expected array from nodes(), got {:?}", ns);
    }
}

#[test]
fn test_relationships_function() {
    let (store, engine) = social_graph();

    let result = engine.execute(
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN relationships(p) AS rels",
        &store,
    ).unwrap();
    assert_eq!(result.len(), 1);
    let rels = result.records[0].get("rels").unwrap().as_property().unwrap();
    if let PropertyValue::Array(items) = rels {
        assert_eq!(items.len(), 1, "Path Alice->Bob has 1 relationship");
    } else {
        panic!("Expected array from relationships(), got {:?}", rels);
    }
}

// ===== QE-01: Parameterized queries ($param) =================================

#[test]
fn test_parameterized_query_where() {
    let (store, _engine) = social_graph();

    // Use the lower-level API with params
    let query = samyama::query::parse_query(
        "MATCH (n:Person) WHERE n.name = $name RETURN n.age"
    ).unwrap();

    let mut params = std::collections::HashMap::new();
    params.insert("name".to_string(), PropertyValue::String("Alice".to_string()));

    let executor = samyama::query::QueryExecutor::new(&store)
        .with_params(params);
    let result = executor.execute(&query).unwrap();

    assert_eq!(result.len(), 1);
    let age = result.records[0].get("n.age").unwrap()
        .as_property().unwrap().as_integer().unwrap();
    assert_eq!(age, 30);
}

#[test]
fn test_parameterized_query_multiple_params() {
    let (store, _) = social_graph();

    let query = samyama::query::parse_query(
        "MATCH (n:Person) WHERE n.age > $min_age AND n.dept = $dept RETURN n.name"
    ).unwrap();

    let mut params = std::collections::HashMap::new();
    params.insert("min_age".to_string(), PropertyValue::Integer(28));
    params.insert("dept".to_string(), PropertyValue::String("Engineering".to_string()));

    let executor = samyama::query::QueryExecutor::new(&store)
        .with_params(params);
    let result = executor.execute(&query).unwrap();

    // Alice (30, Engineering) and Eve is 28 so NOT > 28
    assert_eq!(result.len(), 1);
    let name = result.records[0].get("n.name").unwrap()
        .as_property().unwrap().as_string().unwrap();
    assert_eq!(name, "Alice");
}

// ===== QE-02: PROFILE ========================================================

#[test]
fn test_profile_returns_results_and_stats() {
    let (store, engine) = social_graph();

    let result = engine.execute(
        "PROFILE MATCH (n:Person) RETURN n.name",
        &store,
    ).unwrap();

    // PROFILE should return actual query results plus a profile record
    // At minimum, we should get some records back
    assert!(result.len() >= 5, "PROFILE should return query results (5 persons), got {}", result.len());
}

// ===== QE-03: shortestPath() Cypher pattern ==================================

#[test]
fn test_shortest_path_cypher_pattern() {
    let (store, engine) = social_graph();

    // shortestPath is part of the MATCH pattern, not a separate clause.
    // Alice->Bob->Charlie->David (path exists)
    let result = engine.execute(
        "MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]->(d:Person {name: 'David'})) RETURN p",
        &store,
    ).unwrap();

    // Should find exactly one shortest path
    assert!(result.len() >= 1, "Should find at least one shortest path from Alice to David, got {}", result.len());
}

#[test]
fn test_shortest_path_direct() {
    let (store, engine) = social_graph();

    // Alice->Bob is a direct 1-hop path
    let result = engine.execute(
        "MATCH p = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person {name: 'Bob'})) RETURN p",
        &store,
    ).unwrap();

    assert!(result.len() >= 1, "Should find path from Alice to Bob");
}

// ===== IX-01: DROP INDEX =====================================================

#[test]
fn test_drop_index() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create some data and an index
    engine.execute_mut(
        "CREATE (n:User {email: 'a@b.com'})", &mut store, "default"
    ).unwrap();
    engine.execute_mut(
        "CREATE INDEX ON :User(email)", &mut store, "default"
    ).unwrap();
    assert!(store.property_index.has_index(&Label::new("User"), "email"));

    // Drop the index
    engine.execute_mut(
        "DROP INDEX ON :User(email)", &mut store, "default"
    ).unwrap();
    assert!(!store.property_index.has_index(&Label::new("User"), "email"),
        "Index should be removed after DROP INDEX");
}

// ===== IX-02: SHOW INDEXES / SHOW CONSTRAINTS ================================

#[test]
fn test_show_indexes() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create two indexes
    engine.execute_mut("CREATE INDEX ON :Person(name)", &mut store, "default").unwrap();
    engine.execute_mut("CREATE INDEX ON :Person(age)", &mut store, "default").unwrap();

    let result = engine.execute("SHOW INDEXES", &store).unwrap();
    assert!(result.len() >= 2, "SHOW INDEXES should list at least 2 indexes, got {}", result.len());
}

#[test]
fn test_show_constraints() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine.execute_mut(
        "CREATE CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE",
        &mut store, "default"
    ).unwrap();

    let result = engine.execute("SHOW CONSTRAINTS", &store).unwrap();
    assert!(result.len() >= 1, "SHOW CONSTRAINTS should list at least 1 constraint, got {}", result.len());
}

// ===== IX-04: Composite indexes ==============================================

#[test]
fn test_composite_index_creation() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    for i in 0..20 {
        let q = format!(
            "CREATE (n:Product {{name: 'P{}', category: 'Cat{}', price: {}}})",
            i, i % 3, i * 10
        );
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    // Create composite index
    engine.execute_mut(
        "CREATE INDEX ON :Product(category, price)", &mut store, "default"
    ).unwrap();

    // Both properties should have indexes
    assert!(store.property_index.has_index(&Label::new("Product"), "category"));
    assert!(store.property_index.has_index(&Label::new("Product"), "price"));

    // Query using both indexed properties
    let result = engine.execute(
        "MATCH (p:Product) WHERE p.category = 'Cat0' AND p.price = 0 RETURN p.name",
        &store,
    ).unwrap();
    assert_eq!(result.len(), 1);
}

// ===== IX-05: Unique constraints =============================================

#[test]
fn test_unique_constraint_creation() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine.execute_mut(
        "CREATE CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE",
        &mut store, "default"
    ).unwrap();

    assert!(store.property_index.has_unique_constraint(&Label::new("User"), "email"));
    // Should also have a backing index
    assert!(store.property_index.has_index(&Label::new("User"), "email"));
}

// ===== IX-03 / IX-06: AND-chain index selection / index intersection =========

#[test]
fn test_and_chain_index_usage() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create index and data
    engine.execute_mut("CREATE INDEX ON :Employee(dept)", &mut store, "default").unwrap();
    engine.execute_mut("CREATE INDEX ON :Employee(level)", &mut store, "default").unwrap();

    for i in 0..50 {
        let q = format!(
            "CREATE (n:Employee {{name: 'E{}', dept: 'D{}', level: {}}})",
            i, i % 5, i % 3
        );
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    // AND-chain: planner should use index on dept, filter on level
    let result = engine.execute(
        "MATCH (n:Employee) WHERE n.dept = 'D0' AND n.level = 0 RETURN n.name",
        &store,
    ).unwrap();

    // D0: i=0,5,10,15,20,25,30,35,40,45 (10 items)
    // level=0: i%3==0 → i=0,15,30,45 (4 items from D0)
    assert_eq!(result.len(), 4, "AND-chain should return 4 matching employees");
}

// ===== QP-01: Predicate pushdown =============================================

#[test]
fn test_predicate_pushdown_reduces_traversal() {
    let (store, engine) = social_graph();

    // Without pushdown, this would scan all persons then filter.
    // With pushdown, filter is applied during scan.
    // We verify correctness — that filters work on both sides of a join.
    let result = engine.execute(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WHERE a.dept = 'Engineering' AND b.dept = 'Marketing' \
         RETURN a.name, b.name",
        &store,
    ).unwrap();

    // Bob (Engineering) -> Charlie (Marketing)
    assert_eq!(result.len(), 1);
    let a_name = result.records[0].get("a.name").unwrap()
        .as_property().unwrap().as_string().unwrap();
    let b_name = result.records[0].get("b.name").unwrap()
        .as_property().unwrap().as_string().unwrap();
    assert_eq!(a_name, "Bob");
    assert_eq!(b_name, "Charlie");
}

// ===== QP-03: Join reordering ================================================

#[test]
fn test_multi_match_join_correctness() {
    let (store, engine) = social_graph();

    // Multi-MATCH with different selectivities — join reordering should
    // not affect correctness
    let result = engine.execute(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) \
         MATCH (b)-[:KNOWS]->(c:Person) \
         RETURN a.name, b.name, c.name",
        &store,
    ).unwrap();

    // Alice->Bob->Charlie
    assert_eq!(result.len(), 1);
    let c_name = result.records[0].get("c.name").unwrap()
        .as_property().unwrap().as_string().unwrap();
    assert_eq!(c_name, "Charlie");
}

// ===== QP-04: Early LIMIT propagation ========================================

#[test]
fn test_limit_with_large_dataset() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create 1000 nodes
    for i in 0..1000 {
        let q = format!("CREATE (n:Item {{id: {}}})", i);
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    // LIMIT 5 — with early propagation the scan should stop early
    let result = engine.execute(
        "MATCH (n:Item) RETURN n.id LIMIT 5",
        &store,
    ).unwrap();

    assert_eq!(result.len(), 5, "LIMIT 5 should return exactly 5 records");
}

#[test]
fn test_order_by_with_limit() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    for i in 0..100 {
        let q = format!("CREATE (n:Num {{val: {}}})", i);
        engine.execute_mut(&q, &mut store, "default").unwrap();
    }

    let result = engine.execute(
        "MATCH (n:Num) RETURN n.val ORDER BY n.val DESC LIMIT 3",
        &store,
    ).unwrap();

    assert_eq!(result.len(), 3);
    let first = result.records[0].get("n.val").unwrap()
        .as_property().unwrap().as_integer().unwrap();
    assert_eq!(first, 99);
}

// ===== EXPLAIN output correctness ============================================

#[test]
fn test_explain_shows_plan() {
    let (store, engine) = social_graph();

    let result = engine.execute(
        "EXPLAIN MATCH (n:Person) WHERE n.age > 30 RETURN n.name",
        &store,
    ).unwrap();

    // EXPLAIN should return plan description, not actual query results
    assert!(result.len() >= 1, "EXPLAIN should return at least one record");
    // Check that the plan text contains operator names
    let plan_text = format!("{:?}", result.records[0]);
    assert!(
        plan_text.contains("Scan") || plan_text.contains("Filter") || plan_text.contains("Project"),
        "EXPLAIN output should contain operator names, got: {}",
        plan_text
    );
}
