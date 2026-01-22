use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct QOJayaSolver {
    pub config: SolverConfig,
}

impl QOJayaSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        // Initialize population
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

        // Apply Quasi-Oppositional Based Learning (QOBL) to initial population
        // Generate QO population and pick best N
        let mut qo_population: Vec<Individual> = population.par_iter().map(|ind| {
            let mut new_vars = Array1::zeros(dim);
            let mut local_rng = thread_rng();
            
            for j in 0..dim {
                // Center point c = (a+b)/2
                let c = (lower[j] + upper[j]) / 2.0;
                // Opposite point xo = a + b - x
                let xo = lower[j] + upper[j] - ind.variables[j];
                
                // Quasi-opposite point xqo is rand(c, xo)
                let xqo = if c < xo {
                    local_rng.gen_range(c..xo)
                } else {
                    local_rng.gen_range(xo..c)
                };
                
                new_vars[j] = xqo.clamp(lower[j], upper[j]);
            }
            
            let fitness = problem.fitness(&new_vars);
            Individual::new(new_vars, fitness)
        }).collect();
        
        population.append(&mut qo_population);
        population.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
        population.truncate(self.config.population_size);

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!("QOJaya Solver: Iteration {}/{}", iter, self.config.max_iterations);
            }
            let (best_idx, worst_idx) = self.find_best_worst(&population);
            let best_vars = population[best_idx].variables.clone();
            let worst_vars = population[worst_idx].variables.clone();
            let best_fitness = population[best_idx].fitness;

            history.push(best_fitness);

            // Jaya Update + QOBL
            population = population
                .into_par_iter()
                .map(|mut ind| {
                    let mut local_rng = thread_rng();
                    let mut new_vars = Array1::zeros(dim);

                    let r1: f64 = local_rng.gen();
                    let r2: f64 = local_rng.gen();

                    // 1. Jaya Update
                    for j in 0..dim {
                        let val = ind.variables[j] 
                            + r1 * (best_vars[j] - ind.variables[j].abs()) 
                            - r2 * (worst_vars[j] - ind.variables[j].abs());
                        new_vars[j] = val.clamp(lower[j], upper[j]);
                    }

                    let jaya_fitness = problem.fitness(&new_vars);
                    if jaya_fitness < ind.fitness {
                        ind.variables = new_vars.clone();
                        ind.fitness = jaya_fitness;
                    }

                    // 2. QOBL on the updated individual
                    // Only apply with some probability (jumping rate), e.g., 0.05?
                    // The standard QOJaya applies it to the whole population occasionally or per individual.
                    // We'll apply it per individual.
                    
                    let mut qo_vars = Array1::zeros(dim);
                    for j in 0..dim {
                        let c = (lower[j] + upper[j]) / 2.0;
                        let xo = lower[j] + upper[j] - ind.variables[j];
                        let xqo = if c < xo {
                            local_rng.gen_range(c..xo)
                        } else {
                            local_rng.gen_range(xo..c)
                        };
                        qo_vars[j] = xqo.clamp(lower[j], upper[j]);
                    }
                    
                    let qo_fitness = problem.fitness(&qo_vars);
                    if qo_fitness < ind.fitness {
                        ind.variables = qo_vars;
                        ind.fitness = qo_fitness;
                    }

                    ind
                })
                .collect();
        }

        let (final_best_idx, _) = self.find_best_worst(&population);
        let final_best = &population[final_best_idx];

        OptimizationResult {
            best_variables: final_best.variables.clone(),
            best_fitness: final_best.fitness,
            history,
        }
    }

    fn find_best_worst(&self, population: &[Individual]) -> (usize, usize) {
        let mut best_idx = 0;
        let mut worst_idx = 0;
        for (i, ind) in population.iter().enumerate() {
            if ind.fitness < population[best_idx].fitness {
                best_idx = i;
            }
            if ind.fitness > population[worst_idx].fitness {
                worst_idx = i;
            }
        }
        (best_idx, worst_idx)
    }
}
