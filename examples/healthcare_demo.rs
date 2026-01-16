use samyama_optimization::algorithms::JayaSolver;
use samyama_optimization::common::{Problem, SolverConfig};
use ndarray::Array1;
use rand::Rng;

/// Healthcare Resource Allocation Problem
struct HealthcareProblem {
    num_departments: usize,
    num_resources: usize,
    demand: Array1<f64>,
    costs: Array1<f64>,
    budget: f64,
}

impl HealthcareProblem {
    fn new(departments: usize, resources: usize, budget: f64) -> Self {
        let mut rng = rand::thread_rng();
        
        // Random demand per department (patients/hour)
        let demand = Array1::from_iter((0..departments).map(|_| rng.gen_range(10.0..100.0)));
        
        // Random cost per resource type (e.g., Doctor=$100, Nurse=$40, Bed=$10)
        let costs = Array1::from_iter((0..resources).map(|_| rng.gen_range(10.0..200.0)));

        Self {
            num_departments: departments,
            num_resources: resources,
            demand,
            costs,
            budget,
        }
    }
}

impl Problem for HealthcareProblem {
    fn dim(&self) -> usize {
        self.num_departments * self.num_resources
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        // Minimum 1 resource, Maximum 50 resources per slot
        let dim = self.dim();
        (Array1::from_elem(dim, 1.0), Array1::from_elem(dim, 50.0))
    }

    fn objective(&self, variables: &Array1<f64>) -> f64 {
        // Variables are flattened [Dept1_Res1, Dept1_Res2, ..., Dept2_Res1, ...]
        let mut total_wait_time = 0.0;

        for d in 0..self.num_departments {
            let offset = d * self.num_resources;
            
            // Calculate capacity for this department based on resources
            // Simplified model: Capacity = sum(resource_quantity * resource_efficiency)
            // Assuming resource_efficiency is correlated with cost for simplicity here
            let mut capacity = 0.0;
            for r in 0..self.num_resources {
                let qty = variables[offset + r];
                let efficiency = self.costs[r] / 10.0; // Higher cost = higher efficiency
                capacity += qty * efficiency;
            }

            // Avoid division by zero
            if capacity < 1.0 { capacity = 1.0; }

            // Wait time ~ Demand / Capacity
            total_wait_time += self.demand[d] / capacity;
        }

        total_wait_time
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        // Constraint: Total Cost <= Budget
        let mut total_cost = 0.0;
        for d in 0..self.num_departments {
            for r in 0..self.num_resources {
                let idx = d * self.num_resources + r;
                total_cost += variables[idx] * self.costs[r];
            }
        }

        if total_cost > self.budget {
            // Quadratic penalty for exceeding budget
            (total_cost - self.budget).powi(2)
        } else {
            0.0
        }
    }
}

fn main() {
    println!("🏥 Samyama Healthcare Optimization Demo");
    println!("=======================================");

    let departments = 5; // ER, ICU, Surgery, General, Pediatrics
    let resources = 3;   // Doctors, Nurses, Equipment
    let budget = 50000.0; // Total hourly budget

    let problem = HealthcareProblem::new(departments, resources, budget);
    
    println!("Departments: {}", departments);
    println!("Resources per Dept: {}", resources);
    println!("Total Budget: ${:.2}", budget);
    println!("Problem Dimensions: {}", problem.dim());

    let config = SolverConfig {
        population_size: 100,
        max_iterations: 1000,
    };

    println!("\n🚀 Running Jaya Algorithm...");
    let start = std::time::Instant::now();
    
    let solver = JayaSolver::new(config);
    let result = solver.solve(&problem);

    let duration = start.elapsed();

    println!("✅ Optimization Complete in {:.2?}", duration);
    println!("🏆 Best Fitness (Min Wait Score): {:.4}", result.best_fitness);
    
    println!("\nOptimal Allocation (First Department):");
    println!("Doctors: {:.1}", result.best_variables[0]);
    println!("Nurses:  {:.1}", result.best_variables[1]);
    println!("Equip:   {:.1}", result.best_variables[2]);

    // Verify constraints
    let penalty = problem.penalty(&result.best_variables);
    if penalty > 0.0 {
        println!("⚠️ Constraints violated! Penalty: {}", penalty);
    } else {
        println!("✨ All constraints satisfied (Within Budget).");
    }
}
