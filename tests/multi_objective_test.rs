use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;

#[test]
fn test_multi_objective_optimization() {
    let mut store = GraphStore::new();
    let tenant_id = "default".to_string();

    // Create 10 nodes with two conflicting costs
    // n.cost1: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    // n.cost2: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    for i in 1..=10 {
        let node_id = store.create_node("Task");
        store.set_node_property(&tenant_id, node_id, "cost1".to_string(), PropertyValue::Float(i as f64)).unwrap();
        store.set_node_property(&tenant_id, node_id, "cost2".to_string(), PropertyValue::Float((11 - i) as f64)).unwrap();
        store.set_node_property(&tenant_id, node_id, "priority".to_string(), PropertyValue::Float(0.0)).unwrap();
    }

    // Query: Multi-Objective minimize sum(priority * cost1) AND sum(priority * cost2)
    let query_str = r#"
        CALL algo.or.solve({
            algorithm: 'NSGA2',
            label: 'Task',
            property: 'priority',
            cost_properties: ['cost1', 'cost2'],
            min: 0.0,
            max: 1.0,
            population_size: 20,
            max_iterations: 50
        }) YIELD fitness, algorithm, front_size
    "#;

    let query = parse_query(query_str).expect("Failed to parse query");
    let mut executor = MutQueryExecutor::new(&mut store, tenant_id.clone());
    let result = executor.execute(&query).expect("Execution failed");

    assert_eq!(result.records.len(), 1);
    let record = &result.records[0];
    
    // fitness should be an array of 2 values
    let fitness = record.get("fitness").unwrap().as_property().unwrap().as_array().unwrap();
    assert_eq!(fitness.len(), 2);
    
    let front_size = record.get("front_size").unwrap().as_property().unwrap().as_integer().unwrap();
    println!("Pareto Front Size: {}", front_size);
    assert!(front_size > 0);
}
