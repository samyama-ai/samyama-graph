use samyama::graph::{GraphStore, Label, EdgeType, PropertyValue, NodeId, EdgeId, PropertyMap};
use samyama_optimization::algorithms::{JayaSolver, RaoSolver, RaoVariant, FireflySolver, CuckooSolver, GWOSolver, GASolver, SASolver, BatSolver, ABCSolver, HSSolver, FPASolver};
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
            res_ids.push(id);
        }

        for d in 0..num_departments {
            let mut props = PropertyMap::new();
            props.insert("name".to_string(), PropertyValue::String(format!("Dept_{}", d)));
            props.insert("demand".to_string(), PropertyValue::Float(rng.gen_range(100.0..1000.0)));
            let d_id = store_write.create_node_with_properties("default", vec![Label::new("Department")], props);
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

    // 2. Define Algorithms
    let algorithms = vec!["Jaya", "Rao3", "Firefly", "Cuckoo", "GWO", "GA", "SA", "Bat", "ABC", "HS", "FPA"];
    let mut results = Vec::new();

    let config = SolverConfig {
        population_size: 50,
        max_iterations: 100, // Reduced iterations for quicker benchmark of multiple algos
    };

    println!("\n[2/4] Benchmarking Algorithms...");
    println!("----------------------------------------------------------------");
    println!("| Algorithm | Time (s) | Best Fitness | Evals/Sec |");
    println!("|-----------|----------|--------------|-----------|");

    for &algo_name in &algorithms {
        let problem = HospitalGraphProblem {
            graph: store.clone(),
            dept_ids: dept_ids.clone(),
            resource_ids: res_ids.clone(),
            edge_ids: edge_ids.clone(),
            budget,
        };

        let start_solve = Instant::now();
        
        let result = match algo_name {
            "Jaya" => JayaSolver::new(config.clone()).solve(&problem),
            "Rao3" => RaoSolver::new(config.clone(), RaoVariant::Rao3).solve(&problem),
            "Firefly" => FireflySolver::new(config.clone()).solve(&problem),
            "Cuckoo" => CuckooSolver::new(config.clone()).solve(&problem),
            "GWO" => GWOSolver::new(config.clone()).solve(&problem),
            "GA" => GASolver::new(config.clone()).solve(&problem),
            "SA" => SASolver::new(config.clone()).solve(&problem),
            "Bat" => BatSolver::new(config.clone()).solve(&problem),
            "ABC" => ABCSolver::new(config.clone()).solve(&problem),
            "HS" => HSSolver::new(config.clone()).solve(&problem),
            "FPA" => FPASolver::new(config.clone()).solve(&problem),
            _ => panic!("Unknown algorithm"),
        };
        
        let solve_time = start_solve.elapsed();
        let evals_sec = (config.population_size * config.max_iterations) as f64 / solve_time.as_secs_f64();
        
        println!("| {:<9} | {:<8.2} | {:<12.4} | {:<9.0} |", 
            algo_name, solve_time.as_secs_f64(), result.best_fitness, evals_sec);
            
        results.push((algo_name, solve_time, result.best_fitness));
    }
    println!("----------------------------------------------------------------");

    // 3. Write-Back to Graph (using the result from the last algorithm, or best?)
    // For benchmark demo, we just skip write-back or do it once.
    println!("\n[3/4] Benchmark Complete.");
}
