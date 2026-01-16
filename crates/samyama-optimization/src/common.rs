use ndarray::Array1;
use serde::{Deserialize, Serialize};

/// Represents a candidate solution in the optimization space.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Individual {
    pub variables: Array1<f64>,
    pub fitness: f64,
}

impl Individual {
    pub fn new(variables: Array1<f64>, fitness: f64) -> Self {
        Self { variables, fitness }
    }
}

/// Defines the optimization problem.
pub trait Problem: Send + Sync {
    /// The objective function to minimize.
    fn objective(&self, variables: &Array1<f64>) -> f64;
    
    /// Optional constraints. Returns a penalty score (0 if all satisfied).
    fn penalty(&self, _variables: &Array1<f64>) -> f64 {
        0.0
    }

    /// Combined fitness (objective + penalty).
    fn fitness(&self, variables: &Array1<f64>) -> f64 {
        self.objective(variables) + self.penalty(variables)
    }

    /// Number of variables.
    fn dim(&self) -> usize;

    /// Lower and upper bounds for each variable.
    fn bounds(&self) -> (Array1<f64>, Array1<f64>);
}

/// Configuration for the solver.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SolverConfig {
    pub population_size: usize,
    pub max_iterations: usize,
}

impl Default for SolverConfig {
    fn default() -> Self {
        Self {
            population_size: 50,
            max_iterations: 100,
        }
    }
}

/// The result of an optimization run.
#[derive(Debug, Serialize, Deserialize)]
pub struct OptimizationResult {
    pub best_variables: Array1<f64>,
    pub best_fitness: f64,
    pub history: Vec<f64>,
}

/// A simple problem defined by a closure.
pub struct SimpleProblem<F> 
where F: Fn(&Array1<f64>) -> f64 + Send + Sync
{
    pub objective_func: F,
    pub dim: usize,
    pub lower: Array1<f64>,
    pub upper: Array1<f64>,
}

impl<F> Problem for SimpleProblem<F> 
where F: Fn(&Array1<f64>) -> f64 + Send + Sync
{
    fn objective(&self, variables: &Array1<f64>) -> f64 {
        (self.objective_func)(variables)
    }

    fn dim(&self) -> usize { self.dim }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (self.lower.clone(), self.upper.clone())
    }
}
