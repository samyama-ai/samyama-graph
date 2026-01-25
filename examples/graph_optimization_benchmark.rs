use samyama::graph::{GraphStore, Label, EdgeType, PropertyValue, NodeId, EdgeId, PropertyMap};
use samyama_optimization::algorithms::JayaSolver;
use samyama_optimization::common::{Problem, SolverConfig};
use ndarray::Array1;
use rand::Rng;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// A Problem definition that wraps a reference to the Graph Store.
struct HospitalGraphProblem {
    graph: Arc<RwLock<GraphStore>>,
    dept_ids: Vec<NodeId>,
    resource_ids: Vec<NodeId>,
    edge_ids: Vec<EdgeId>, 
    budget: f64,
}

// Safety: GraphStore is protected by RwLock.
unsafe impl Sync for HospitalGraphProblem {}
unsafe impl Send for HospitalGraphProblem {}

impl Problem for HospitalGraphProblem {
    fn dim(&self) -> usize {
        self.edge_ids.len()
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        let dim = self.dim();
        // Min 1 unit, Max 100 units per resource
        (Array1::from_elem(dim, 1.0), Array1::from_elem(dim, 100.0))
    }

    fn objective(&self, variables: &Array1<f64>) -> f64 {
        let mut total_wait_time = 0.0;
        let num_resources_per_dept = self.resource_ids.len();

        let store = self.graph.read().unwrap();
        
        for (i, &dept_id) in self.dept_ids.iter().enumerate() {
            let dept_node = store.get_node(dept_id).expect("Node missing");
            let demand = match dept_node.properties.get("demand").unwrap() {
                PropertyValue::Float(v) => *v,
                _ => 10.0,
            };

            let mut capacity = 0.0;
            for j in 0..num_resources_per_dept {
                let var_idx = i * num_resources_per_dept + j;
                let quantity = variables[var_idx];
                
                let res_id = self.resource_ids[j];
                let res_node = store.get_node(res_id).expect("Res missing");
                let efficiency = match res_node.properties.get("efficiency").unwrap() {
                    PropertyValue::Float(v) => *v,
                    _ => 1.0,
                };

                capacity += quantity * efficiency;
            }

            if capacity < 1.0 { capacity = 1.0; }
            total_wait_time += demand / capacity;
        }

        total_wait_time
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        let mut total_cost = 0.0;
        let num_resources_per_dept = self.resource_ids.len();
        
        let store = self.graph.read().unwrap();

        for j in 0..num_resources_per_dept {
            let res_id = self.resource_ids[j];
            let res_node = store.get_node(res_id).unwrap();
            let cost = match res_node.properties.get("cost").unwrap() {
                PropertyValue::Float(v) => *v,
                _ => 0.0,
            };

            for i in 0..self.dept_ids.len() {
                let var_idx = i * num_resources_per_dept + j;
                total_cost += variables[var_idx] * cost;
            }
        }

        if total_cost > self.budget {
            (total_cost - self.budget).powi(2)
        } else {
            0.0
        }
    }
}

fn main() {
    let num_departments = 50;
    let num_resources = 20;
    let total_vars = num_departments * num_resources;
    let budget = 1_000_000.0;

    println!("🏥 High-Scale Graph Optimization Benchmark");
    println!("========================================");
    println!("Departments: {}", num_departments);
    println!("Resource Types: {}", num_resources);
    println!("Total Decision Variables: {}", total_vars);
    println!("Target: Optimize allocation in a LIVE Graph Database");

    // 1. Initialize Graph
    println!("\n[1/4] Building Graph...");
    let start_build = Instant::now();
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let mut rng = rand::thread_rng();

    let mut dept_ids: Vec<NodeId> = Vec::with_capacity(num_departments);
    let mut res_ids: Vec<NodeId> = Vec::with_capacity(num_resources);
    let mut edge_ids: Vec<EdgeId> = Vec::with_capacity(total_vars);

    {
        let mut store_write = store.write().unwrap();
        for r in 0..num_resources {
            let mut props = PropertyMap::new();
            props.insert("name".to_string(), PropertyValue::String(format!("Resource_{}", r)));
            props.insert("cost".to_string(), PropertyValue::Float(rng.gen_range(50.0..500.0)));
            props.insert("efficiency".to_string(), PropertyValue::Float(rng.gen_range(0.5..2.0)));
        let id = store_write.create_node_with_properties("default", vec![Label::new("Resource")], props);
...
        let d_id = store_write.create_node_with_properties("default", vec![Label::new("Department")], props);
            res_ids.push(id);
        }

        for d in 0..num_departments {
            let mut props = PropertyMap::new();
            props.insert("name".to_string(), PropertyValue::String(format!("Dept_{}", d)));
            props.insert("demand".to_string(), PropertyValue::Float(rng.gen_range(100.0..1000.0)));
            let d_id = store_write.create_node_with_properties(vec![Label::new("Department")], props);
            dept_ids.push(d_id);

            for r_id in &res_ids {
                let mut edge_props = PropertyMap::new();
                edge_props.insert("quantity".to_string(), PropertyValue::Float(0.0));
                let e_id = store_write.create_edge_with_properties(d_id, *r_id, EdgeType::new("ALLOCATED"), edge_props).unwrap();
                edge_ids.push(e_id);
            }
        }
    }
    println!("Graph built in {:.2?} ({} Nodes, {} Edges)", start_build.elapsed(), num_departments + num_resources, total_vars);

    // 2. Setup Optimization Problem
    println!("\n[2/4] Initializing Solver...");
    let problem = HospitalGraphProblem {
        graph: store.clone(),
        dept_ids,
        resource_ids: res_ids,
        edge_ids: edge_ids.clone(),
        budget,
    };

    let config = SolverConfig {
        population_size: 100,
        max_iterations: 200, 
    };
    let solver = JayaSolver::new(config);

    // 3. Run Optimization
    println!("\n[3/4] Optimizing {} variables...", total_vars);
    let start_solve = Instant::now();
    let result = solver.solve(&problem);
    let solve_time = start_solve.elapsed();
    
    println!("Optimization converged in {:.2?}", solve_time);
    println!("Best Objective Score: {:.4}", result.best_fitness);
    println!("Throughput: {:.0} evals/sec", (100 * 200) as f64 / solve_time.as_secs_f64());

    // 4. Write-Back to Graph
    println!("\n[4/4] Writing results back to Graph...");
    let start_write = Instant::now();
    let mut updated_count = 0;
    
    {
        let mut store_write = store.write().unwrap();
        for (i, &val) in result.best_variables.iter().enumerate() {
            let edge_id = edge_ids[i];
            
            if let Some(edge) = store_write.get_edge_mut(edge_id) {
                edge.properties.insert("quantity".to_string(), PropertyValue::Float(val));
                updated_count += 1;
            }
        }
    }
    println!("Updated {} edges in {:.2?}", updated_count, start_write.elapsed());

    println!("\n✨ SUCCESS: In-Database Optimization Complete!");
}
