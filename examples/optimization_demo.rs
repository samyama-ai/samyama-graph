use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;
use std::time::Instant;

fn main() {
    println!("🏭 Samyama Optimization Engine Demo: Smart Manufacturing");
    println!("======================================================");
    
    // 1. Setup Graph
    let mut store = GraphStore::new();
    let tenant_id = "demo_tenant".to_string();
    
    println!("-> Initializing Plant Topology...");
    
    // Create 10 Machines with varying costs
    // Machine 0-4: Efficient (Cost 10.0)
    // Machine 5-9: Expensive (Cost 50.0)
    for i in 0..10 {
        let node_id = store.create_node("Machine");
        let cost = if i < 5 { 10.0 } else { 50.0 };
        
        store.set_node_property(&tenant_id, node_id, "id".to_string(), PropertyValue::Integer(i as i64)).unwrap();
        store.set_node_property(&tenant_id, node_id, "unit_cost".to_string(), PropertyValue::Float(cost)).unwrap();
        // Initial production set to 0
        store.set_node_property(&tenant_id, node_id, "production".to_string(), PropertyValue::Float(0.0)).unwrap();
    }
    
    // 2. Define Problem
    // Demand: 500 units.
    // Constraints:
    // - Each machine max capacity: 100 units.
    // - Total production >= 500 units.
    // Objective: Minimize Total Cost.
    
    println!("\n📋 Problem Definition:");
    println!("   - Nodes: 10 Machines");
    println!("   - Variable: 'production' (0 to 100)");
    println!("   - Constraint: Total Production >= 500.0");
    println!("   - Objective: Minimize sum(production * unit_cost)");
    println!("   - Solver: Cuckoo Search (Nature-Inspired Metaheuristic)");

    let query_str = r#" 
        CALL algo.or.solve({
            algorithm: 'Cuckoo',
            label: 'Machine',
            property: 'production',
            min: 0.0,
            max: 100.0,
            min_total: 500.0,
            cost_property: 'unit_cost',
            population_size: 50,
            max_iterations: 200
        }) YIELD fitness, algorithm, iterations
    "#;

    println!("\n🚀 Running Optimization via Cypher...");
    let start = Instant::now();
    
    let query = parse_query(query_str).expect("Failed to parse query");
    let mut executor = MutQueryExecutor::new(&mut store, tenant_id.clone());
    let result = executor.execute(&query).expect("Execution failed");
    
    let duration = start.elapsed();
    
    // 3. Analyze Results
    let fitness = result.records[0].get("fitness").unwrap().as_property().unwrap().as_float().unwrap();
    
    println!("\n✅ Optimization Complete in {:.2?}", duration);
    println!("   - Best Fitness (Cost): {:.2}", fitness);
    
    println!("\n📊 Updated Machine Schedule:");
    println!("   | ID | Cost/Unit | Production | Total Cost |");
    println!("   |----|-----------|------------|------------|");
    
    let nodes = store.get_nodes_by_label(&samyama::graph::Label::new("Machine"));
    let mut total_production = 0.0;
    let mut efficient_production = 0.0;
    let mut expensive_production = 0.0;
    
    let mut nodes_vec: Vec<_> = nodes.iter().collect();
    nodes_vec.sort_by_key(|n| n.get_property("id").unwrap().as_integer().unwrap());

    for node in nodes_vec {
        let id = node.get_property("id").unwrap().as_integer().unwrap();
        let cost = node.get_property("unit_cost").unwrap().as_float().unwrap();
        let prod = node.get_property("production").unwrap().as_float().unwrap();
        let total = prod * cost;
        
        println!("   | {:<2} | ${:<8.2} | {:<10.2} | ${:<10.2} |", id, cost, prod, total);
        
        total_production += prod;
        if cost < 20.0 {
            efficient_production += prod;
        } else {
            expensive_production += prod;
        }
    }
    
    println!("\n📈 Summary:");
    println!("   - Total Production: {:.2} (Target: 500.0)", total_production);
    println!("   - Efficient Machines Utilized: {:.2} / 500.0 (Capacity)", efficient_production);
    println!("   - Expensive Machines Utilized: {:.2} / 500.0 (Capacity)", expensive_production);
    
    // Validation
    if total_production >= 499.0 {
        println!("\nResult: SUCCESS (Demand Met)");
    } else {
        println!("\nResult: FAILURE (Demand Not Met)");
    }
    
    // Optimal Strategy: Maximize efficient machines (5 * 100 = 500). Expensive should be 0.
    // Total Cost should be 500 * 10 = 5000.
    if efficient_production > 450.0 && expensive_production < 50.0 {
        println!("Strategy: OPTIMAL (Prioritized efficient machines)");
    } else {
        println!("Strategy: SUB-OPTIMAL (Used expensive machines)");
    }
}
