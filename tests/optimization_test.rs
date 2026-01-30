use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;

#[test]
fn test_optimization_solver_integration() {
    // 1. Setup Graph
    let mut store = GraphStore::new();
    let tenant_id = "default".to_string();

    // Create 10 Resource nodes with cost 10.0
    for i in 0..10 {
        let node_id = store.create_node("Resource");
        store.set_node_property(&tenant_id, node_id, "cost".to_string(), PropertyValue::Float(10.0)).unwrap();
        store.set_node_property(&tenant_id, node_id, "allocation".to_string(), PropertyValue::Float(0.0)).unwrap();
    }

    // 2. Prepare Optimization Query
    // We want to minimize allocation * cost. 
    // Without constraints, it should drive allocation to 'min' (0.0).
    // Let's set min=5.0 to force it to 5.0.
    let query_str = r#"
        CALL algo.or.solve({
            algorithm: 'GWO',
            label: 'Resource',
            property: 'allocation',
            min: 5.0,
            max: 100.0,
            cost_property: 'cost',
            population_size: 20,
            max_iterations: 50
        }) YIELD fitness, algorithm
    "#;

    let query = parse_query(query_str).expect("Failed to parse query");

    // 3. Execute
    let mut executor = MutQueryExecutor::new(&mut store, tenant_id.clone());
    let result = executor.execute(&query).expect("Execution failed");

    // 4. Verify Result Records
    assert_eq!(result.records.len(), 1);
    let fitness = result.records[0].get("fitness").unwrap().as_property().unwrap().as_float().unwrap();
    println!("Optimization Fitness: {}", fitness);
    
    // Expected fitness: 10 nodes * 5.0 allocation * 10.0 cost = 500.0
    // Jaya should converge close to this.
    assert!(fitness >= 500.0 && fitness < 505.0, "Fitness should be close to 500.0 (Global Opt), got {}", fitness);

    // 5. Verify Graph Updates
    // Check if properties were actually updated in the store
    let nodes = store.get_nodes_by_label(&samyama::graph::Label::new("Resource"));
    for node in nodes {
        let allocation = node.get_property("allocation").unwrap().as_float().unwrap();
        assert!(allocation >= 4.99, "Allocation should be >= 5.0, got {}", allocation);
        // It might not be exactly 5.0 due to stochastic nature, but should be close.
    }
}
