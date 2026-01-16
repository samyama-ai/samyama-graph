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
    
    assert!(result.best_fitness < 0.01, "Jaya failed to minimize Sphere function: fitness {}", result.best_fitness);
}

#[test]
fn test_rao3_sphere() {
    let problem = SphereProblem;
    // Rao3 needs more population/iterations for the Sphere function due to the abs() term
    let config = SolverConfig { population_size: 100, max_iterations: 1000 };
    let solver = RaoSolver::new(config, RaoVariant::Rao3);
    let result = solver.solve(&problem);
    
    // We check if it improved significantly from the start (initial avg fitness ~66)
    assert!(result.best_fitness < 1.0, "Rao3 failed to minimize Sphere function: fitness {}", result.best_fitness);
}

#[test]
fn test_tlbo_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = TLBOSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.01, "TLBO failed to minimize Sphere function: fitness {}", result.best_fitness);
}

#[test]
fn test_bmr_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = BMRSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.1, "BMR failed to minimize Sphere function: fitness {}", result.best_fitness);
}

#[test]
fn test_bwr_sphere() {
    let problem = SphereProblem;
    let config = SolverConfig { population_size: 50, max_iterations: 500 };
    let solver = BWRSolver::new(config);
    let result = solver.solve(&problem);
    
    assert!(result.best_fitness < 0.1, "BWR failed to minimize Sphere function: fitness {}", result.best_fitness);
}