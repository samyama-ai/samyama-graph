use samyama_optimization::algorithms::*;
use samyama_optimization::common::*;
use ndarray::{Array1, array};

struct SphereProblem;

impl Problem for SphereProblem {
    fn objective(&self, variables: &Array1<f64>) -> f64 {
        variables.iter().map(|&x| x * x).sum()
    }

    fn dim(&self) -> usize { 2 }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (array![-10.0, -10.0], array![10.0, 10.0])
    }
}

#[test]
fn test_jaya_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = JayaSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.01, "Jaya failed: fitness {}", result.best_fitness);
}

#[test]
fn test_qojaya_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = QOJayaSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.01, "QOJaya failed: fitness {}", result.best_fitness);
}

#[test]
fn test_itlbo_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = ITLBOSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.01, "ITLBO failed: fitness {}", result.best_fitness);
}

#[test]
fn test_rao3_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 100, max_iterations: 1000 };
    let solver = RaoSolver::new(config, RaoVariant::Rao3);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 1.0, "Rao3 failed: fitness {}", result.best_fitness);
}

#[test]
fn test_tlbo_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = TLBOSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.01, "TLBO failed: fitness {}", result.best_fitness);
}

#[test]
fn test_bmr_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = BMRSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.1, "BMR failed: fitness {}", result.best_fitness);
}

#[test]
fn test_bwr_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = BWRSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.1, "BWR failed: fitness {}", result.best_fitness);
}

#[test]
fn test_pso_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = PSOSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.01, "PSO failed: fitness {}", result.best_fitness);
}

#[test]
fn test_de_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = DESolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 0.01, "DE failed: fitness {}", result.best_fitness);
}

#[test]
fn test_gotlbo_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = GOTLBOSolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 0.1, "GOTLBO failed: fitness {}", result.best_fitness);
}

#[test]
fn test_firefly_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = FireflySolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "Firefly failed: fitness {}", result.best_fitness);
}

#[test]
fn test_cuckoo_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = CuckooSolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "Cuckoo failed: fitness {}", result.best_fitness);
}

#[test]
fn test_gwo_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = GWOSolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 0.01, "GWO failed: fitness {}", result.best_fitness);
}

#[test]
fn test_ga_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = GASolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "GA failed: fitness {}", result.best_fitness);
}

#[test]
fn test_sa_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = SASolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "SA failed: fitness {}", result.best_fitness);
}

#[test]
fn test_bat_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = BatSolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "Bat failed: fitness {}", result.best_fitness);
}

#[test]
fn test_abc_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = ABCSolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "ABC failed: fitness {}", result.best_fitness);
}

#[test]
fn test_gsa_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = GSASolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "GSA failed: fitness {}", result.best_fitness);
}

#[test]
fn test_hs_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = HSSolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "HS failed: fitness {}", result.best_fitness);
}

#[test]
fn test_fpa_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = FPASolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 1.0, "FPA failed: fitness {}", result.best_fitness);
}

// --- Multi-Objective Tests ---

struct BiObjectiveProblem;

impl MultiObjectiveProblem for BiObjectiveProblem {
    fn objectives(&self, variables: &Array1<f64>) -> Vec<f64> {
        // Simple bi-objective: f1 = x0^2, f2 = (x1-1)^2
        // Pareto front: any tradeoff between minimizing x0^2 and (x1-1)^2
        let f1 = variables[0] * variables[0];
        let f2 = (variables[1] - 1.0) * (variables[1] - 1.0);
        vec![f1, f2]
    }

    fn dim(&self) -> usize { 2 }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (array![0.0, 0.0], array![1.0, 1.0])
    }

    fn num_objectives(&self) -> usize { 2 }
}

#[test]
fn test_nsga2_biobjective() {
    let problem = BiObjectiveProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 100 };
    let solver = NSGA2Solver::new(config);
    let result = solver.solve(&problem);

    assert!(!result.pareto_front.is_empty(), "NSGA-II should produce a non-empty Pareto front");
    // All Pareto front members should be rank 0
    for ind in &result.pareto_front {
        assert_eq!(ind.rank, 0, "Pareto front members should have rank 0");
    }
    // Objectives should be non-negative (ZDT1 range)
    for ind in &result.pareto_front {
        assert!(ind.fitness[0] >= 0.0 && ind.fitness[0] <= 1.0, "f1 out of range: {}", ind.fitness[0]);
        assert!(ind.fitness[1] >= 0.0, "f2 should be non-negative: {}", ind.fitness[1]);
    }
}

#[test]
fn test_motlbo_biobjective() {
    let problem = BiObjectiveProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 100 };
    let solver = MOTLBOSolver::new(config);
    let result = solver.solve(&problem);

    assert!(!result.pareto_front.is_empty(), "MOTLBO should produce a non-empty Pareto front");
    for ind in &result.pareto_front {
        assert_eq!(ind.rank, 0, "Pareto front members should have rank 0");
    }
}

// --- Convergence / History Tests ---

#[test]
fn test_solver_history_decreasing() {
    // Verify that best fitness generally decreases over iterations
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 200 };
    let solver = JayaSolver::new(config);
    let result = solver.solve(&problem);

    assert_eq!(result.history.len(), 200, "History should have one entry per iteration");
    // First value should be >= last value (minimization)
    assert!(result.history.first().unwrap() >= result.history.last().unwrap(),
        "Fitness should generally decrease: first={}, last={}",
        result.history.first().unwrap(), result.history.last().unwrap());
}

#[test]
fn test_result_variables_in_bounds() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 100 };
    let solver = PSOSolver::new(config);
    let result = solver.solve(&problem);

    let (lower, upper) = problem.bounds();
    for i in 0..problem.dim() {
        assert!(result.best_variables[i] >= lower[i] && result.best_variables[i] <= upper[i],
            "Variable {} out of bounds: {} not in [{}, {}]",
            i, result.best_variables[i], lower[i], upper[i]);
    }
}

// --- SimpleProblem Test ---

#[test]
fn test_simple_problem_closure() {
    // Verify that SimpleProblem works with a closure-based objective
    let problem = SimpleProblem {
        objective_func: |x: &Array1<f64>| x.iter().map(|&v| v * v).sum(),
        dim: 3,
        lower: array![-5.0, -5.0, -5.0],
        upper: array![5.0, 5.0, 5.0],
    };

    let config = SolverConfig { population_size: 30, max_iterations: 200 };
    let solver = DESolver::new(config);
    let result = solver.solve(&problem);

    assert!(result.best_fitness < 0.1, "SimpleProblem with DE failed: fitness {}", result.best_fitness);
    assert_eq!(result.best_variables.len(), 3);
}

// --- Higher-dimensional Test ---

struct Rastrigin10D;

impl Problem for Rastrigin10D {
    fn objective(&self, variables: &Array1<f64>) -> f64 {
        let n = variables.len() as f64;
        10.0 * n + variables.iter().map(|&x| x * x - 10.0 * (2.0 * std::f64::consts::PI * x).cos()).sum::<f64>()
    }

    fn dim(&self) -> usize { 10 }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::from_elem(10, -5.12), Array1::from_elem(10, 5.12))
    }
}

#[test]
fn test_de_rastrigin_10d() {
    let problem = Rastrigin10D;
    let config = SolverConfig { population_size: 100, max_iterations: 1000 };
    let solver = DESolver::new(config);
    let result = solver.solve(&problem);

    // Rastrigin is multimodal, just verify it found something reasonable
    assert!(result.best_fitness < 50.0, "DE on Rastrigin-10D: fitness {} too high", result.best_fitness);
}

// --- Constrained Problem Test ---

struct ConstrainedSphere;

impl Problem for ConstrainedSphere {
    fn objective(&self, variables: &Array1<f64>) -> f64 {
        variables.iter().map(|&x| x * x).sum()
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        // Constraint: x0 + x1 >= 1
        let sum = variables[0] + variables[1];
        if sum < 1.0 {
            1000.0 * (1.0 - sum).powi(2)
        } else {
            0.0
        }
    }

    fn dim(&self) -> usize { 2 }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (array![-10.0, -10.0], array![10.0, 10.0])
    }
}

#[test]
fn test_constrained_problem() {
    let problem = ConstrainedSphere;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = JayaSolver::new(config);
    let result = solver.solve(&problem);

    // The constrained optimum is at x0 + x1 = 1, x0 = x1 = 0.5, fitness = 0.5
    assert!(result.best_variables[0] + result.best_variables[1] >= 0.9,
        "Constraint violated: x0+x1 = {}", result.best_variables[0] + result.best_variables[1]);
}
