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

/// Represents a candidate solution in a multi-objective space.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiObjectiveIndividual {
    pub variables: Array1<f64>,
    pub fitness: Vec<f64>,
    pub constraint_violation: f64,
    pub rank: usize,
    pub crowding_distance: f64,
}

impl MultiObjectiveIndividual {
    pub fn new(variables: Array1<f64>, fitness: Vec<f64>, constraint_violation: f64) -> Self {
        Self { 
            variables, 
            fitness, 
            constraint_violation,
            rank: 0, 
            crowding_distance: 0.0 
        }
    }
}

/// Defines a multi-objective optimization problem.
pub trait MultiObjectiveProblem: Send + Sync {
    /// Multiple objective functions to minimize.
    fn objectives(&self, variables: &Array1<f64>) -> Vec<f64>;
    
    /// Optional constraints. Returns a vector of penalties.
    fn penalties(&self, _variables: &Array1<f64>) -> Vec<f64> {
        vec![]
    }

    /// Number of variables.
    fn dim(&self) -> usize;

    /// Lower and upper bounds for each variable.
    fn bounds(&self) -> (Array1<f64>, Array1<f64>);
    
    /// Number of objectives.
    fn num_objectives(&self) -> usize;
}

/// The result of a multi-objective optimization run (Pareto Front).
#[derive(Debug, Serialize, Deserialize)]
pub struct MultiObjectiveResult {
    pub pareto_front: Vec<MultiObjectiveIndividual>,
    pub history: Vec<f64>, // e.g., hypervolume or min of first objective
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

/// Reproducible randomness for the solvers.
///
/// Every solver drew from `thread_rng()`, so no run of this crate could be
/// re-derived -- not by a test, and not by anyone reproducing a published
/// result. Seeding is opt-in via `Solver::with_seed(u64)`; with no seed the
/// behaviour is unchanged (entropy), so existing callers see no difference.
///
/// [`child_rng`] is the part that matters for the parallel solvers. Seeding
/// only the outer RNG and letting rayon workers call `thread_rng()` would
/// produce runs that *look* reproducible and are not, which is worse than
/// admitting they are random: the seed would be recorded alongside results it
/// cannot regenerate. Deriving each element's stream from (seed, iteration,
/// index) instead makes the result independent of how work is scheduled across
/// threads, which is the property reproducibility actually requires.
pub mod rng {
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    /// Odd 64-bit constants from SplitMix64, used to decorrelate the two
    /// coordinates so that (iter 1, index 2) and (iter 2, index 1) do not
    /// collide onto the same stream.
    const ITER_ODD: u64 = 0x9E37_79B9_7F4A_7C15;
    const INDEX_ODD: u64 = 0xBF58_476D_1CE4_E5B9;

    /// The solver's own RNG: seeded if a seed was given, entropy otherwise.
    pub fn solver_rng(seed: Option<u64>) -> StdRng {
        match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        }
    }

    /// A per-element RNG for work inside a parallel iterator.
    ///
    /// Deterministic in (seed, iteration, index) and therefore independent of
    /// thread scheduling. Unseeded, it falls back to entropy exactly as before.
    pub fn child_rng(seed: Option<u64>, iteration: usize, index: usize) -> StdRng {
        match seed {
            Some(s) => StdRng::seed_from_u64(
                s ^ (iteration as u64).wrapping_mul(ITER_ODD)
                  ^ (index as u64).wrapping_mul(INDEX_ODD),
            ),
            None => StdRng::from_entropy(),
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
