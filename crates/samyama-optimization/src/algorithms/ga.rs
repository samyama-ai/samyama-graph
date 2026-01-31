use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct GASolver {
    pub config: SolverConfig,
    pub crossover_rate: f64,
    pub mutation_rate: f64,
}

impl GASolver {
    pub fn new(config: SolverConfig) -> Self {
        Self {
            config,
            crossover_rate: 0.8,
            mutation_rate: 0.1,
        }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        // 1. Initialize Population
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

        for _iter in 0..self.config.max_iterations {
            // Find best for history
            let mut best_idx = 0;
            for i in 1..population.len() {
                if population[i].fitness < population[best_idx].fitness {
                    best_idx = i;
                }
            }
            history.push(population[best_idx].fitness);

            // 2. Evolution
            let mut new_population = Vec::with_capacity(self.config.population_size);
            
            // Elitism: carry over the best
            new_population.push(population[best_idx].clone());

            while new_population.len() < self.config.population_size {
                // Selection (Tournament)
                let p1 = self.select(&population);
                let p2 = self.select(&population);

                // Crossover
                let (mut c1_vars, mut c2_vars) = if rng.gen::<f64>() < self.crossover_rate {
                    self.crossover(&p1.variables, &p2.variables)
                } else {
                    (p1.variables.clone(), p2.variables.clone())
                };

                // Mutation
                self.mutate(&mut c1_vars, &lower, &upper);
                self.mutate(&mut c2_vars, &lower, &upper);

                // Add to new population
                let f1 = problem.fitness(&c1_vars);
                new_population.push(Individual::new(c1_vars, f1));
                
                if new_population.len() < self.config.population_size {
                    let f2 = problem.fitness(&c2_vars);
                    new_population.push(Individual::new(c2_vars, f2));
                }
            }

            population = new_population;
        }

        let mut best_idx = 0;
        for i in 1..population.len() {
            if population[i].fitness < population[best_idx].fitness {
                best_idx = i;
            }
        }

        OptimizationResult {
            best_variables: population[best_idx].variables.clone(),
            best_fitness: population[best_idx].fitness,
            history,
        }
    }

    fn select<'a>(&self, population: &'a [Individual]) -> &'a Individual {
        let mut rng = thread_rng();
        let i1 = rng.gen_range(0..population.len());
        let i2 = rng.gen_range(0..population.len());
        
        if population[i1].fitness < population[i2].fitness {
            &population[i1]
        } else {
            &population[i2]
        }
    }

    fn crossover(&self, p1: &Array1<f64>, p2: &Array1<f64>) -> (Array1<f64>, Array1<f64>) {
        let mut rng = thread_rng();
        let dim = p1.len();
        let mut c1 = p1.clone();
        let mut c2 = p2.clone();

        // Uniform crossover
        for i in 0..dim {
            if rng.gen_bool(0.5) {
                std::mem::swap(&mut c1[i], &mut c2[i]);
            }
        }
        (c1, c2)
    }

    fn mutate(&self, vars: &mut Array1<f64>, lower: &Array1<f64>, upper: &Array1<f64>) {
        let mut rng = thread_rng();
        let dim = vars.len();

        for i in 0..dim {
            if rng.gen::<f64>() < self.mutation_rate {
                // Small Gaussian mutation or random reset?
                // Let's use Gaussian mutation for continuous space
                let range = upper[i] - lower[i];
                let delta = rand_distr::Normal::new(0.0, range * 0.1).unwrap().sample(&mut rng);
                vars[i] = (vars[i] + delta).clamp(lower[i], upper[i]);
            }
        }
    }
}
