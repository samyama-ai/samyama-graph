use crate::common::{MultiObjectiveIndividual, MultiObjectiveProblem, MultiObjectiveResult, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct MOTLBOSolver {
    pub config: SolverConfig,
}

impl MOTLBOSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config }
    }

    pub fn solve<P: MultiObjectiveProblem>(&self, problem: &P) -> MultiObjectiveResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let pop_size = self.config.population_size;

        // 1. Initialize Population
        let mut population: Vec<MultiObjectiveIndividual> = (0..pop_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.objectives(&vars);
                MultiObjectiveIndividual::new(vars, fitness)
            })
            .collect();

        self.evaluate_population(&mut population);

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for _iter in 0..self.config.max_iterations {
            let mut offspring = Vec::with_capacity(pop_size * 2);

            // Teacher Phase
            let teacher_idx = self.select_teacher(&population);
            let teacher_vars = population[teacher_idx].variables.clone();
            let mean_vars = self.calculate_mean(&population, dim);

            for ind in &population {
                let mut local_rng = thread_rng();
                let tf: f64 = local_rng.gen_range(1..3) as f64;
                let mut new_vars = Array1::zeros(dim);
                for j in 0..dim {
                    let r: f64 = local_rng.gen();
                    new_vars[j] = (ind.variables[j] + r * (teacher_vars[j] - tf * mean_vars[j])).clamp(lower[j], upper[j]);
                }
                offspring.push(MultiObjectiveIndividual::new(new_vars.clone(), problem.objectives(&new_vars)));
            }

            // Learner Phase
            for i in 0..pop_size {
                let mut local_rng = thread_rng();
                let mut j;
                loop {
                    j = local_rng.gen_range(0..pop_size);
                    if j != i { break; }
                }

                let mut new_vars = Array1::zeros(dim);
                let dominates_i_j = self.dominates(&population[i].fitness, &population[j].fitness);
                let dominates_j_i = self.dominates(&population[j].fitness, &population[i].fitness);

                for k in 0..dim {
                    let r: f64 = local_rng.gen();
                    if dominates_i_j {
                        new_vars[k] = (population[i].variables[k] + r * (population[i].variables[k] - population[j].variables[k])).clamp(lower[k], upper[k]);
                    } else if dominates_j_i {
                        new_vars[k] = (population[i].variables[k] + r * (population[j].variables[k] - population[i].variables[k])).clamp(lower[k], upper[k]);
                    } else {
                        // Random movement if non-dominated
                        new_vars[k] = population[i].variables[k];
                    }
                }
                offspring.push(MultiObjectiveIndividual::new(new_vars.clone(), problem.objectives(&new_vars)));
            }

            // Merge and Rank
            let mut combined = population;
            combined.extend(offspring);
            self.evaluate_population(&mut combined);

            // Select Best N
            combined.sort_by(|a, b| {
                if a.rank != b.rank {
                    a.rank.cmp(&b.rank)
                } else {
                    b.crowding_distance.partial_cmp(&a.crowding_distance).unwrap()
                }
            });
            
            combined.truncate(pop_size);
            population = combined;
            
            history.push(population[0].fitness[0]);
        }

        MultiObjectiveResult {
            pareto_front: population.into_iter().filter(|ind| ind.rank == 0).collect(),
            history,
        }
    }

    fn select_teacher(&self, population: &[MultiObjectiveIndividual]) -> usize {
        // Teacher is chosen from the first rank (best non-dominated front)
        let first_rank: Vec<usize> = population.iter().enumerate()
            .filter(|(_, ind)| ind.rank == 0)
            .map(|(i, _)| i)
            .collect();
        
        let mut rng = thread_rng();
        *first_rank.choose(&mut rng).unwrap_or(&0)
    }

    fn calculate_mean(&self, population: &[MultiObjectiveIndividual], dim: usize) -> Array1<f64> {
        let mut mean = Array1::zeros(dim);
        for ind in population {
            mean += &ind.variables;
        }
        mean / (population.len() as f64)
    }

    // Reuse NSGA-II sorting logic
    fn evaluate_population(&self, population: &mut [MultiObjectiveIndividual]) {
        self.non_dominated_sort(population);
        let mut rank = 0;
        loop {
            let indices: Vec<usize> = population.iter().enumerate()
                .filter(|(_, ind)| ind.rank == rank)
                .map(|(i, _)| i)
                .collect();
            if indices.is_empty() { break; }
            self.calculate_crowding_distance(population, &indices);
            rank += 1;
        }
    }

    fn non_dominated_sort(&self, population: &mut [MultiObjectiveIndividual]) {
        let n = population.len();
        let mut dominance_counts = vec![0; n];
        let mut dominated_sets = vec![Vec::new(); n];
        let mut fronts = vec![Vec::new()];

        for i in 0..n {
            for j in 0..n {
                if i == j { continue; }
                if self.dominates(&population[i].fitness, &population[j].fitness) {
                    dominated_sets[i].push(j);
                } else if self.dominates(&population[j].fitness, &population[i].fitness) {
                    dominance_counts[i] += 1;
                }
            }
            if dominance_counts[i] == 0 {
                population[i].rank = 0;
                fronts[0].push(i);
            }
        }

        let mut i = 0;
        while !fronts[i].is_empty() {
            let mut next_front = Vec::new();
            for &p in &fronts[i] {
                for &q in &dominated_sets[p] {
                    dominance_counts[q] -= 1;
                    if dominance_counts[q] == 0 {
                        population[q].rank = i + 1;
                        next_front.push(q);
                    }
                }
            }
            i += 1;
            fronts.push(next_front);
        }
    }

    fn dominates(&self, f1: &[f64], f2: &[f64]) -> bool {
        let mut better = false;
        for i in 0..f1.len() {
            if f1[i] > f2[i] { return false; }
            if f1[i] < f2[i] { better = true; }
        }
        better
    }

    fn calculate_crowding_distance(&self, population: &mut [MultiObjectiveIndividual], indices: &[usize]) {
        let num_objectives = population[0].fitness.len();
        for &idx in indices {
            population[idx].crowding_distance = 0.0;
        }

        for m in 0..num_objectives {
            let mut sorted_indices = indices.to_vec();
            sorted_indices.sort_by(|&a, &b| population[a].fitness[m].partial_cmp(&population[b].fitness[m]).unwrap());
            
            let min_val = population[*sorted_indices.first().unwrap()].fitness[m];
            let max_val = population[*sorted_indices.last().unwrap()].fitness[m];
            let range = max_val - min_val;

            population[*sorted_indices.first().unwrap()].crowding_distance = f64::INFINITY;
            population[*sorted_indices.last().unwrap()].crowding_distance = f64::INFINITY;

            if range > 1e-9 {
                for i in 1..(sorted_indices.len() - 1) {
                    let prev = population[sorted_indices[i-1]].fitness[m];
                    let next = population[sorted_indices[i+1]].fitness[m];
                    population[sorted_indices[i]].crowding_distance += (next - prev) / range;
                }
            }
        }
    }
}
