use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct DESolver {
    pub config: SolverConfig,
    pub f: f64,  // Scaling factor (default 0.5)
    pub cr: f64, // Crossover probability (default 0.9)
}

impl DESolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { 
            config,
            f: 0.5,
            cr: 0.9,
        }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        let mut population: Vec<Individual> = (0..self.config.population_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.fitness(&vars);
                Individual::new(vars, fitness)
            })
            .collect();

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!("DE Solver: Iteration {}/{}", iter, self.config.max_iterations);
            }
            let best_idx = self.find_best(&population);
            history.push(population[best_idx].fitness);

            // Create new generation
            // Read-only access to old population for mutation
            let old_pop = population.clone();

            population = population
                .into_par_iter()
                .enumerate()
                .map(|(i, mut target)| {
                    let mut local_rng = thread_rng();
                    
                    // Pick a, b, c distinct from i
                    let mut idxs = [0; 3];
                    for k in 0..3 {
                        loop {
                            let r = local_rng.gen_range(0..old_pop.len());
                            if r != i && !idxs[0..k].contains(&r) {
                                idxs[k] = r;
                                break;
                            }
                        }
                    }
                    
                    let a = &old_pop[idxs[0]];
                    let b = &old_pop[idxs[1]];
                    let c = &old_pop[idxs[2]];

                    // Mutation + Crossover
                    let mut trial_vars = Array1::zeros(dim);
                    let r_idx = local_rng.gen_range(0..dim); // Ensure at least one parameter changes

                    for j in 0..dim {
                        if local_rng.gen::<f64>() < self.cr || j == r_idx {
                            let val = a.variables[j] + self.f * (b.variables[j] - c.variables[j]);
                            trial_vars[j] = val.clamp(lower[j], upper[j]);
                        } else {
                            trial_vars[j] = target.variables[j];
                        }
                    }

                    // Selection
                    let trial_fitness = problem.fitness(&trial_vars);
                    if trial_fitness < target.fitness {
                        target.variables = trial_vars;
                        target.fitness = trial_fitness;
                    }
                    target
                })
                .collect();
        }

        let best_idx = self.find_best(&population);
        let final_best = &population[best_idx];

        OptimizationResult {
            best_variables: final_best.variables.clone(),
            best_fitness: final_best.fitness,
            history,
        }
    }

    fn find_best(&self, population: &[Individual]) -> usize {
        let mut best_idx = 0;
        for (i, ind) in population.iter().enumerate() {
            if ind.fitness < population[best_idx].fitness {
                best_idx = i;
            }
        }
        best_idx
    }
}
