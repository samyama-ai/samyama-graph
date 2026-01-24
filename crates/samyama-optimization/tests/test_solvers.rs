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
