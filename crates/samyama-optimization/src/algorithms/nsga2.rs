use crate::common::{MultiObjectiveIndividual, MultiObjectiveProblem, MultiObjectiveResult, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct NSGA2Solver {
    pub config: SolverConfig,
    pub mutation_rate: f64,
}

impl NSGA2Solver {
    pub fn new(config: SolverConfig) -> Self {
        Self { 
            config,
            mutation_rate: 0.1,
        }
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
            // 2. Create Offspring (Crossover + Mutation)
            let mut offspring = Vec::with_capacity(pop_size);
            while offspring.len() < pop_size {
                let p1 = self.tournament_select(&population);
                let p2 = self.tournament_select(&population);
                
                let (mut c1_vars, mut c2_vars) = self.crossover(&p1.variables, &p2.variables);
                self.mutate(&mut c1_vars, &lower, &upper);
                self.mutate(&mut c2_vars, &lower, &upper);
                
                offspring.push(MultiObjectiveIndividual::new(c1_vars.clone(), problem.objectives(&c1_vars)));
                if offspring.len() < pop_size {
                    offspring.push(MultiObjectiveIndividual::new(c2_vars.clone(), problem.objectives(&c2_vars)));
                }
            }

            // 3. Merge Population and Offspring (2N)
            let mut combined = population;
            combined.extend(offspring);

            // 4. Non-dominated Sort + Crowding Distance
            self.evaluate_population(&mut combined);

            // 5. Select Best N
            combined.sort_by(|a, b| {
                if a.rank != b.rank {
                    a.rank.cmp(&b.rank)
                } else {
                    // Larger crowding distance is better
                    b.crowding_distance.partial_cmp(&a.crowding_distance).unwrap()
                }
            });
            
            combined.truncate(pop_size);
            population = combined;
            
            history.push(population[0].fitness[0]); // Track first objective of best-ranked
        }

        MultiObjectiveResult {
            pareto_front: population.into_iter().filter(|ind| ind.rank == 0).collect(),
            history,
        }
    }

    fn evaluate_population(&self, population: &mut [MultiObjectiveIndividual]) {
        // Fast Non-dominated Sort
        self.non_dominated_sort(population);
        
        // Crowding Distance per rank
        let mut rank = 0;
        loop {
            let current_rank_indices: Vec<usize> = population.iter().enumerate()
                .filter(|(_, ind)| ind.rank == rank)
                .map(|(i, _)| i)
                .collect();
            
            if current_rank_indices.is_empty() { break; }
            
            self.calculate_crowding_distance(population, &current_rank_indices);
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

    fn tournament_select<'a>(&self, population: &'a [MultiObjectiveIndividual]) -> &'a MultiObjectiveIndividual {
        let mut rng = thread_rng();
        let i1 = rng.gen_range(0..population.len());
        let i2 = rng.gen_range(0..population.len());
        
        let p1 = &population[i1];
        let p2 = &population[i2];

        if p1.rank < p2.rank { p1 }
        else if p2.rank < p1.rank { p2 }
        else if p1.crowding_distance > p2.crowding_distance { p1 }
        else { p2 }
    }

    fn crossover(&self, p1: &Array1<f64>, p2: &Array1<f64>) -> (Array1<f64>, Array1<f64>) {
        let mut rng = thread_rng();
        let dim = p1.len();
        let mut c1 = p1.clone();
        let mut c2 = p2.clone();

        // BLX-alpha crossover or similar for continuous
        for i in 0..dim {
            if rng.gen_bool(0.5) {
                let alpha = 0.5;
                let min = p1[i].min(p2[i]);
                let max = p1[i].max(p2[i]);
                let range = max - min;
                
                let lower = min - alpha * range;
                let upper = max + alpha * range;
                
                if (upper - lower).abs() > 1e-9 {
                    c1[i] = rng.gen_range(lower..upper);
                    c2[i] = rng.gen_range(lower..upper);
                }
            }
        }
        (c1, c2)
    }

    fn mutate(&self, vars: &mut Array1<f64>, lower: &Array1<f64>, upper: &Array1<f64>) {
        let mut rng = thread_rng();
        for i in 0..vars.len() {
            if rng.gen::<f64>() < self.mutation_rate {
                let range = upper[i] - lower[i];
                vars[i] = (vars[i] + (rng.gen::<f64>() - 0.5) * range * 0.1).clamp(lower[i], upper[i]);
            }
        }
    }
}
