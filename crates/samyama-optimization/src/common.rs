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
///
/// This is a black-box model: an objective function, an optional scalar penalty,
/// a dimension and box bounds. There is no coefficient row, constraint matrix,
/// constraint senses or integrality here, so a problem defined through this
/// trait cannot be written as an MPS or LP file. That export (OPT-14) needs a
/// separate structured linear/MILP representation first; see the crate README.
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

    /// Whether the run's budget is spent. Every solver asks at the top of each
    /// iteration and stops when this is true. Always false unless the problem
    /// is wrapped in a [`crate::budget::Budgeted`].
    fn budget_exhausted(&self) -> bool {
        false
    }
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

/// `problem.fitness(x)`, with a NaN ranked as the worst possible value (#1634).
///
/// Every solver here minimises, and every one of them compares fitness with
/// `<`, `>` or a sort. A NaN breaks all three: a sort that unwraps
/// `partial_cmp` panics, `<` never lets a NaN incumbent be replaced, and
/// `f64::total_cmp` orders the negative NaN that `0.0 / 0.0` produces on x86
/// *before* every number -- so a minimiser would return it as the best
/// solution. Mapping it to `+inf` here, at the one place every solver
/// evaluates, makes it lose every comparison in every algorithm at once.
pub fn evaluate<P: Problem + ?Sized>(problem: &P, variables: &Array1<f64>) -> f64 {
    worst_if_nan(problem.fitness(variables))
}

/// `problem.objectives(x)`, each NaN objective ranked worst, as [`evaluate`].
pub fn evaluate_objectives<P: MultiObjectiveProblem + ?Sized>(
    problem: &P,
    variables: &Array1<f64>,
) -> Vec<f64> {
    problem
        .objectives(variables)
        .into_iter()
        .map(worst_if_nan)
        .collect()
}

/// A NaN as `+inf`, the worst value under minimisation; anything else as is.
pub fn worst_if_nan(v: f64) -> f64 {
    if v.is_nan() {
        f64::INFINITY
    } else {
        v
    }
}

/// Ascending order for minimisation with a NaN last (worst), never a panic.
///
/// For a sort that may still meet a NaN -- a fitness built outside
/// [`evaluate`], or a ratio such as a crowding distance computed from
/// infinite objectives. `partial_cmp(..).unwrap()` panics on it, and
/// `total_cmp` puts a negative NaN first.
pub fn ascending_nan_last(a: f64, b: f64) -> std::cmp::Ordering {
    a.partial_cmp(&b)
        .unwrap_or_else(|| a.is_nan().cmp(&b.is_nan()))
}

/// Descending order with a NaN last: the mirror rule, for crowding distance,
/// where larger is better and an undefined distance is the least preferred.
pub fn descending_nan_last(a: f64, b: f64) -> std::cmp::Ordering {
    b.partial_cmp(&a)
        .unwrap_or_else(|| a.is_nan().cmp(&b.is_nan()))
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

    /// Whether the run's budget is spent; see [`Problem::budget_exhausted`].
    fn budget_exhausted(&self) -> bool {
        false
    }
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

/// The penalty a solution carries when it satisfies every constraint: the
/// value [`Problem::penalty`] is documented to return in that case.
pub const NO_CONSTRAINT_VIOLATION: f64 = 0.0;

/// Milliseconds in a second, for reporting a `Duration` in milliseconds.
const MILLIS_PER_SEC: f64 = 1_000.0;

/// Whether a solution with this constraint penalty is feasible (OPT-09, #1776).
///
/// `fitness` is objective plus penalty, so a solver returns its best *fitness*
/// whether or not that solution obeys the constraints: a budget it cannot meet
/// comes back as a large number, not as a refusal. This is the field that says
/// which of the two happened.
///
/// Strict on purpose: any violation, however small, is infeasible, because a
/// tolerance here would be a number the caller never chose. A NaN penalty is
/// infeasible too -- a constraint that could not be evaluated was not met.
pub fn is_feasible(penalty: f64) -> bool {
    penalty <= NO_CONSTRAINT_VIOLATION
}

/// How far an objective value is from a best known one (OPT-09, #1776).
///
/// Absolute, `objective - best_known`, in the objective's own units. Every
/// solver here minimises, so a positive gap is how much worse the answer is,
/// and a negative gap means it beat the value supplied as best known. Absolute
/// rather than relative because the commonest best known value, an optimum of
/// zero, would make a relative gap a division by zero.
///
/// None of the solvers can prove a bound, so there is no gap to report unless
/// the caller supplies the best known value; callers report `null` then, never
/// a guess.
pub fn gap_to_best_known(objective: f64, best_known: f64) -> f64 {
    objective - best_known
}

/// A duration in milliseconds, keeping the sub-millisecond part.
///
/// Fractional, because a small solve finishes in well under a millisecond and
/// rounding it to 0 would read as "not measured" (#1798).
pub fn duration_ms(elapsed: std::time::Duration) -> f64 {
    elapsed.as_secs_f64() * MILLIS_PER_SEC
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A penalty strictly above zero, however small: one ULP of violation.
    const TINY_VIOLATION: f64 = f64::MIN_POSITIVE;

    #[test]
    fn no_penalty_is_feasible() {
        assert!(is_feasible(NO_CONSTRAINT_VIOLATION));
    }

    #[test]
    fn any_positive_penalty_is_infeasible() {
        assert!(!is_feasible(TINY_VIOLATION));
        assert!(!is_feasible(f64::INFINITY));
    }

    #[test]
    fn a_nan_penalty_is_infeasible() {
        assert!(!is_feasible(f64::NAN));
    }

    #[test]
    fn gap_is_objective_minus_best_known() {
        let (objective, best_known) = (12.5, 10.0);
        assert_eq!(
            gap_to_best_known(objective, best_known),
            objective - best_known
        );
    }

    #[test]
    fn a_solution_that_beats_the_best_known_has_a_negative_gap() {
        let best_known = 10.0;
        let better = best_known - 1.0;
        assert!(gap_to_best_known(better, best_known) < 0.0);
    }

    #[test]
    fn reaching_the_best_known_is_a_zero_gap() {
        let best_known = 0.0;
        assert_eq!(gap_to_best_known(best_known, best_known), 0.0);
    }

    #[test]
    fn duration_ms_keeps_the_sub_millisecond_part() {
        assert_eq!(duration_ms(Duration::ZERO), 0.0);
        assert_eq!(duration_ms(Duration::from_micros(1_500)), 1.5);
        assert_eq!(duration_ms(Duration::from_micros(250)), 0.25);
        assert_eq!(duration_ms(Duration::from_secs(2)), 2_000.0);
    }
}
