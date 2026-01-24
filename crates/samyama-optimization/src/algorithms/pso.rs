use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct PSOSolver {
    pub config: SolverConfig,
    pub w: f64,  // Inertia weight
    pub c1: f64, // Cognitive weight (pbest)
    pub c2: f64, // Social weight (gbest)
}

impl PSOSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { 
            config,
            w: 0.7,
            c1: 1.5,
            c2: 1.5,
        }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        // Initialize population (swarm)
        let mut swarm: Vec<Individual> = (0..self.config.population_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.fitness(&vars);
                Individual::new(vars, fitness)
            })
            .collect();

        // Initialize velocities
        let mut velocities: Vec<Array1<f64>> = (0..self.config.population_size)
            .map(|_| Array1::zeros(dim))
            .collect();

        // Initialize personal bests (pbest)
        let mut pbests = swarm.clone();

        // Initialize global best (gbest)
        let mut gbest_idx = self.find_best(&swarm);
        let mut gbest = swarm[gbest_idx].clone();

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!("PSO Solver: Iteration {}/{}", iter, self.config.max_iterations);
            }
            
            history.push(gbest.fitness);

            // Update swarm
            // Note: In parallel, we need to collect updates then apply? 
            // Or we can update particle i using its own pbest and the *current* gbest (read-only).
            // Updating velocities requires mutable access to velocities[i].
            // Updating positions requires mutable access to swarm[i].
            
            // We'll compute new state in parallel and then replace.
            let results: Vec<(Individual, Array1<f64>, Individual)> = swarm.par_iter().zip(velocities.par_iter()).zip(pbests.par_iter())
                .map(|((particle, velocity), pbest)| {
                    let mut local_rng = thread_rng();
                    let mut new_vel = Array1::zeros(dim);
                    let mut new_vars = Array1::zeros(dim);

                    for j in 0..dim {
                        let r1: f64 = local_rng.gen();
                        let r2: f64 = local_rng.gen();
                        
                        let v = self.w * velocity[j] 
                              + self.c1 * r1 * (pbest.variables[j] - particle.variables[j])
                              + self.c2 * r2 * (gbest.variables[j] - particle.variables[j]);
                        
                        new_vel[j] = v;
                        new_vars[j] = (particle.variables[j] + v).clamp(lower[j], upper[j]);
                    }

                    let new_fitness = problem.fitness(&new_vars);
                    let new_ind = Individual::new(new_vars, new_fitness);
                    
                    let new_pbest = if new_fitness < pbest.fitness {
                        new_ind.clone()
                    } else {
                        pbest.clone()
                    };

                    (new_ind, new_vel, new_pbest)
                })
                .collect();

            // Unpack results
            for (i, (new_ind, new_vel, new_pbest)) in results.into_iter().enumerate() {
                swarm[i] = new_ind;
                velocities[i] = new_vel;
                pbests[i] = new_pbest;
                
                if swarm[i].fitness < gbest.fitness {
                    gbest = swarm[i].clone();
                }
            }
        }

        OptimizationResult {
            best_variables: gbest.variables.clone(),
            best_fitness: gbest.fitness,
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
